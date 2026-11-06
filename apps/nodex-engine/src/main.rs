mod chart_service;
mod token_details;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use launch_detector::{detect_pump_fun_launch, CreateInstruction};
use nodex_core::{config::EngineConfig, TokenRegistry};
use nodex_solana::{
    decode_pump_fun_create, decode_pump_swap_activity_logs, decode_pump_swap_pool_account,
    derive_pump_swap_pool_address, run_logs_stream, start_market_monitor, MarketMonitor,
    PumpSwapActivityEvent, PumpSwapPoolAccount, SolanaRpc, StreamStatus, PUMPSWAP_PROGRAM_ID,
    WRAPPED_SOL_MINT,
};
use nodex_trading::paper_wallet::{PaperWallet, LAMPORTS_PER_SOL};
use nodex_trading::volume::{RollingTradeVolume, TradeSide, WindowVolumeMetrics};
use serde::Serialize;
use serde_json::json;
use solana_keypair::Keypair;
use solana_signer::Signer;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{broadcast, Mutex, RwLock};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct AppState {
    events: broadcast::Sender<serde_json::Value>,
    stream_status: Arc<RwLock<StreamStatus>>,
    pumpswap_status: Arc<RwLock<StreamStatus>>,
    wallet: Arc<RwLock<PaperWallet>>,
    rpc: Option<SolanaRpc>,
    database: sqlx::SqlitePool,
    control: Arc<RwLock<EngineControl>>,
    admin_api_key: Option<String>,
    ws_tickets: Arc<Mutex<HashMap<String, Instant>>>,
    token_registry: TokenRegistry,
    profile: Arc<RwLock<serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize)]
struct EngineControl {
    paused: bool,
    mode: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum ClientCommand {
    Pause,
    Resume,
    Start,
    SetMode { mode: String },
    Status,
    SubscribeToken { mint: String },
    SetChartTimeframe { mint: String, timeframe: String },
    UnsubscribeToken { mint: String },
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
    mode: &'static str,
    service: &'static str,
}

fn volume_window_json(metrics: WindowVolumeMetrics) -> serde_json::Value {
    json!({
        "buy_volume_raw": metrics.buy_volume_raw.to_string(),
        "sell_volume_raw": metrics.sell_volume_raw.to_string(),
        "total_volume_raw": metrics.total_volume_raw.to_string(),
        "buy_pressure_pct": metrics.buy_pressure_pct,
        "buy_trade_count": metrics.buy_trade_count,
        "sell_trade_count": metrics.sell_trade_count
    })
}

fn start_pumpswap_activity_processor(
    mut receiver: broadcast::Receiver<serde_json::Value>,
    events: broadcast::Sender<serde_json::Value>,
    control: Arc<RwLock<EngineControl>>,
    rpc: Option<SolanaRpc>,
) {
    let seen_signatures = Arc::new(Mutex::new(HashSet::<String>::new()));
    tokio::spawn(async move {
        let mut resolved_pools = HashMap::<String, PumpSwapPoolAccount>::new();
        let mut pool_retry_after = HashMap::<String, i64>::new();
        let mut pool_volumes = HashMap::<String, RollingTradeVolume>::new();
        loop {
            let notification = match receiver.recv().await {
                Ok(value) => value,
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    let _ = events.send(json!({"type":"diagnostic","data":{
                        "severity":"warning","component":"pumpswap_activity_processor",
                        "message":"PumpSwap activity consumer lagged; a bounded notification gap may exist.",
                        "dropped_notifications":count,
                        "received_at":chrono::Utc::now().to_rfc3339()
                    }}));
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            };

            if notification
                .get("method")
                .and_then(serde_json::Value::as_str)
                != Some("logsNotification")
                || notification
                    .get("nodex_program_id")
                    .and_then(serde_json::Value::as_str)
                    != Some(PUMPSWAP_PROGRAM_ID)
                || control.read().await.paused
            {
                continue;
            }

            let Some(value) = notification.pointer("/params/result/value") else {
                continue;
            };
            if !value.get("err").is_some_and(serde_json::Value::is_null) {
                continue;
            }
            let Some(signature) = value
                .get("signature")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
            else {
                continue;
            };
            {
                let mut seen = seen_signatures.lock().await;
                if !seen.insert(signature.clone()) {
                    continue;
                }
                if seen.len() > 20_000 {
                    seen.clear();
                    seen.insert(signature.clone());
                }
            }

            let Some(logs) = value.get("logs").and_then(serde_json::Value::as_array) else {
                continue;
            };
            let logs: Vec<String> = logs
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_owned)
                .collect();
            let slot = notification
                .pointer("/params/result/context/slot")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default();

            match decode_pump_swap_activity_logs(&logs, PUMPSWAP_PROGRAM_ID) {
                Ok(decoded) => {
                    for (event_index, event) in decoded.into_iter().enumerate() {
                        let (kind, timestamp, pool, user, base_amount, quote_amount) = match event {
                            PumpSwapActivityEvent::Buy {
                                timestamp,
                                pool,
                                user,
                                base_amount,
                                quote_amount,
                            } => ("buy", timestamp, pool, user, base_amount, quote_amount),
                            PumpSwapActivityEvent::Sell {
                                timestamp,
                                pool,
                                user,
                                base_amount,
                                quote_amount,
                            } => ("sell", timestamp, pool, user, base_amount, quote_amount),
                            PumpSwapActivityEvent::Deposit {
                                timestamp,
                                pool,
                                user,
                                base_amount,
                                quote_amount,
                            } => ("deposit", timestamp, pool, user, base_amount, quote_amount),
                            PumpSwapActivityEvent::Withdraw {
                                timestamp,
                                pool,
                                user,
                                base_amount,
                                quote_amount,
                            } => ("withdraw", timestamp, pool, user, base_amount, quote_amount),
                        };
                        let pool_details = if let Some(cached) = resolved_pools.get(&pool) {
                            Some(cached.clone())
                        } else if let Some(rpc_client) = rpc.as_ref() {
                            let now = chrono::Utc::now().timestamp_millis();
                            let retry_allowed = pool_retry_after
                                .get(&pool)
                                .is_none_or(|retry_at| now >= *retry_at);
                            if retry_allowed {
                                pool_retry_after.insert(pool.clone(), now.saturating_add(10_000));
                                match rpc_client.get_account_data(&pool).await {
                                    Ok(Some((owner, raw, _))) if owner == PUMPSWAP_PROGRAM_ID => {
                                        match decode_pump_swap_pool_account(&raw) {
                                            Ok(decoded) => {
                                                match derive_pump_swap_pool_address(&decoded) {
                                                    Ok(derived) if derived == pool => {
                                                        resolved_pools
                                                            .insert(pool.clone(), decoded.clone());
                                                        pool_retry_after.remove(&pool);
                                                        Some(decoded)
                                                    }
                                                    _ => None,
                                                }
                                            }
                                            Err(_) => None,
                                        }
                                    }
                                    _ => None,
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        let pair_status = if pool_details.is_some() {
                            "resolved"
                        } else {
                            "unresolved"
                        };
                        let quote_mint = pool_details
                            .as_ref()
                            .map(|details| details.quote_mint.as_str());
                        let quote_unit = if quote_mint == Some(WRAPPED_SOL_MINT) {
                            "lamports"
                        } else {
                            "raw_quote_asset_units"
                        };
                        let _ = events.send(json!({"type":"pumpswap_activity","data":{
                            "venue":"pump_swap",
                            "event_type":kind,
                            "event_index":event_index,
                            "signature":signature,
                            "slot":slot,
                            "timestamp":timestamp,
                            "pool_address":pool,
                            "pool_pair_status":pair_status,
                            "base_mint":pool_details.as_ref().map(|details| details.base_mint.clone()),
                            "mint_address":pool_details.as_ref().map(|details| details.base_mint.clone()),
                            "quote_mint":pool_details.as_ref().map(|details| details.quote_mint.clone()),
                            "pool_creator":pool_details.as_ref().map(|details| details.creator.clone()),
                            "user_address":user,
                            "base_amount_raw":base_amount.to_string(),
                            "quote_amount_raw":quote_amount.to_string(),
                            "quote_amount_unit":quote_unit,
                            "received_at":chrono::Utc::now().to_rfc3339()
                        }}));
                        if pool_details.is_some() {
                            let side = match kind {
                                "buy" => Some(TradeSide::Buy),
                                "sell" => Some(TradeSide::Sell),
                                _ => None,
                            };
                            if let Some(side) = side {
                                let now_ms = chrono::Utc::now().timestamp_millis();
                                let event_time_ms = timestamp.saturating_mul(1_000);
                                let volume = pool_volumes.entry(pool.clone()).or_default();
                                if volume.record(event_time_ms, side, quote_amount, now_ms) {
                                    let window_30s = volume.metrics(now_ms, 30);
                                    let window_1m = volume.metrics(now_ms, 60);
                                    let window_5m = volume.metrics(now_ms, 300);
                                    let _ = events.send(json!({
                                        "type":"pumpswap_volume_update",
                                        "data":{
                                            "venue":"pump_swap",
                                            "pool_address":pool,
                                            "base_mint":pool_details.as_ref().map(|details| details.base_mint.clone()),
                                            "mint_address":pool_details.as_ref().map(|details| details.base_mint.clone()),
                                            "quote_mint":pool_details.as_ref().map(|details| details.quote_mint.clone()),
                                            "quote_amount_unit":quote_unit,
                                            "window_30s":volume_window_json(window_30s),
                                            "window_1m":volume_window_json(window_1m),
                                            "window_5m":volume_window_json(window_5m),
                                            "calculation":"quote-volume-weighted-buy-pressure",
                                            "received_at":chrono::Utc::now().to_rfc3339()
                                        }
                                    }));
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    let _ = events.send(json!({"type":"diagnostic","data":{
                        "severity":"warning","component":"pumpswap_activity_decoder",
                        "message":error.to_string(),"signature":signature,"slot":slot,
                        "received_at":chrono::Utc::now().to_rfc3339()
                    }}));
                }
            }
        }
    });
}

/// Projects independent worker events into the canonical shared token state.
/// Workers remain decoupled: they publish domain events and this projector updates
/// only the matching domain of the token record.
fn start_token_state_projector(
    mut receiver: broadcast::Receiver<serde_json::Value>,
    events: broadcast::Sender<serde_json::Value>,
    database: sqlx::SqlitePool,
    registry: TokenRegistry,
    profile: Arc<RwLock<serde_json::Value>>,
) {
    tokio::spawn(async move {
        loop {
            let event = match receiver.recv().await {
                Ok(event) => event,
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    tracing::warn!(
                        dropped_events = count,
                        "Token state projector lagged; latest state may need reconciliation"
                    );
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            };
            let Some(kind) = event.get("type").and_then(serde_json::Value::as_str) else {
                continue;
            };
            let Some(data) = event.get("data").cloned() else {
                continue;
            };
            let Some(mint) = data.get("mint_address").and_then(serde_json::Value::as_str) else {
                continue;
            };
            if kind == "token_rejected_cleanup" {
                if let Err(error) = nodex_persistence::delete_token_state(&database, mint).await {
                    tracing::error!(%error, mint_address=mint, "Could not remove rejected token state");
                }
                if let Err(error) = nodex_persistence::delete_token_launches(&database, mint).await
                {
                    tracing::error!(%error, mint_address=mint, "Could not remove rejected token launch records");
                }
                registry.remove(mint);
                let _ = events.send(json!({"type":"token_removed","data":{
                    "mint_address":mint,
                    "reason":data.get("reason").cloned().unwrap_or(serde_json::Value::Null),
                    "removed_at":chrono::Utc::now().to_rfc3339()
                }}));
                continue;
            }
            if matches!(kind, "market_price_update" | "pumpswap_volume_update") {
                let approved = registry.get(mint).is_some_and(|state| {
                    state
                        .data
                        .pointer("/security/decision")
                        .and_then(serde_json::Value::as_str)
                        == Some("APPROVED")
                });
                if !approved {
                    continue;
                }
            }
            let domain_patch = match kind {
                "token_scan_started" => json!({"security":{"status":"scanning"}}),
                "token_scan_progress" => json!({"security":{
                    "status":"scanning",
                    "progress":data.get("progress").cloned().unwrap_or(serde_json::Value::Null),
                    "stage":data.get("stage").cloned().unwrap_or(serde_json::Value::Null),
                    "message":data.get("message").cloned().unwrap_or(serde_json::Value::Null)
                }}),
                "token_scan_update" => json!({"security":data}),
                "creator_history_update" => {
                    json!({"security":{"creator_history":data.get("history").cloned().unwrap_or(serde_json::Value::Null)}})
                }
                "market_price_update" => json!({"market":data}),
                "pumpswap_volume_update" => json!({"statistics":{"pumpswap_volume":data}}),
                _ => continue,
            };
            match registry.upsert_patch(mint, domain_patch) {
                Ok(snapshot) => {
                    if let Err(error) =
                        nodex_persistence::save_token_state(&database, &snapshot).await
                    {
                        tracing::error!(%error, mint_address = mint, "Could not persist projected token state");
                        continue;
                    }
                    if kind == "market_price_update" {
                        let event_timestamp_ms = data
                            .get("timestamp_ms")
                            .or_else(|| data.get("block_time_ms"))
                            .and_then(serde_json::Value::as_i64)
                            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
                        let _ = events.send(json!({
                            "type":"token_market_update",
                            "data":{
                                "mint_address":mint,
                                "state_version":snapshot.version,
                                "event_timestamp_ms":event_timestamp_ms,
                                "market_patch":data.clone()
                            }
                        }));
                    } else if kind == "pumpswap_volume_update" {
                        let _ = events.send(json!({
                            "type":"token_statistics_update",
                            "data":{
                                "mint_address":mint,
                                "state_version":snapshot.version,
                                "statistics_patch":data.clone()
                            }
                        }));
                    } else {
                        let _ = events.send(json!({"type":"token_state_updated","data":snapshot}));
                    }
                    if kind == "market_price_update" {
                        if let Some(price_sol) =
                            data.get("price_sol").and_then(serde_json::Value::as_f64)
                        {
                            match nodex_persistence::load_portfolio_positions(&database).await {
                                Ok(mut positions) => {
                                    for position in positions.iter_mut().filter(|position| {
                                        position
                                            .get("mint_address")
                                            .and_then(serde_json::Value::as_str)
                                            == Some(mint)
                                            && position
                                                .get("status")
                                                .and_then(serde_json::Value::as_str)
                                                == Some("open")
                                    }) {
                                        let quantity = position
                                            .get("token_quantity")
                                            .or_else(|| position.get("quantity"))
                                            .and_then(serde_json::Value::as_f64)
                                            .unwrap_or(0.0);
                                        let cost_basis = position
                                            .get("cost_basis_sol")
                                            .or_else(|| position.get("invested_sol"))
                                            .and_then(serde_json::Value::as_f64)
                                            .unwrap_or(0.0);
                                        let current_value = quantity * price_sol;
                                        let pnl = current_value - cost_basis;
                                        if let Some(object) = position.as_object_mut() {
                                            object.insert(
                                                "current_price_sol".into(),
                                                json!(price_sol),
                                            );
                                            object.insert(
                                                "current_value_sol".into(),
                                                json!(current_value),
                                            );
                                            object.insert("unrealized_pnl_sol".into(), json!(pnl));
                                            object.insert(
                                                "unrealized_pnl_pct".into(),
                                                json!(if cost_basis > 0.0 {
                                                    pnl / cost_basis * 100.0
                                                } else {
                                                    0.0
                                                }),
                                            );
                                            object.insert(
                                                "marked_at".into(),
                                                json!(chrono::Utc::now().to_rfc3339()),
                                            );
                                        }
                                        let position_id = position
                                            .get("position_id")
                                            .and_then(serde_json::Value::as_str)
                                            .unwrap_or(mint);
                                        if let Err(error) =
                                            nodex_persistence::save_portfolio_position(
                                                &database,
                                                position_id,
                                                mint,
                                                "open",
                                                position,
                                            )
                                            .await
                                        {
                                            tracing::error!(%error, mint_address = mint, position_id, "Could not persist marked portfolio position");
                                        } else {
                                            let _ = events.send(json!({
                                                "type":"position_updated",
                                                "data":position.clone()
                                            }));
                                        }
                                    }
                                    let refreshed =
                                        match nodex_persistence::load_portfolio_positions(&database)
                                            .await
                                        {
                                            Ok(items) => items,
                                            Err(error) => {
                                                tracing::error!(%error, "Could not reload marked portfolio positions");
                                                Vec::new()
                                            }
                                        };
                                    let open: Vec<serde_json::Value> = refreshed
                                        .iter()
                                        .filter(|item| {
                                            item.get("status").and_then(serde_json::Value::as_str)
                                                == Some("open")
                                        })
                                        .cloned()
                                        .collect();
                                    let sum = |items: &[serde_json::Value], field: &str| -> f64 {
                                        items
                                            .iter()
                                            .filter_map(|item| {
                                                item.get(field).and_then(serde_json::Value::as_f64)
                                            })
                                            .sum()
                                    };
                                    let position_count = open.len();
                                    let portfolio = json!({
                                        "total_value_sol": sum(&open, "current_value_sol"),
                                        "total_invested_sol": sum(&open, "cost_basis_sol"),
                                        "unrealized_pnl_sol": sum(&open, "unrealized_pnl_sol"),
                                        "realized_pnl_sol": sum(&refreshed, "realized_pnl_sol"),
                                        "positions": open,
                                        "position_count": position_count
                                    });
                                    let mut current_profile = profile.write().await;
                                    if let Some(object) = current_profile.as_object_mut() {
                                        object.insert("portfolio".into(), portfolio.clone());
                                    }
                                    let profile_snapshot = current_profile.clone();
                                    drop(current_profile);
                                    if let Err(error) = nodex_persistence::save_profile(
                                        &database,
                                        &profile_snapshot,
                                    )
                                    .await
                                    {
                                        tracing::error!(%error, "Could not persist recalculated profile portfolio");
                                    } else {
                                        let _ = events.send(json!({
                                            "type":"portfolio_summary_updated",
                                            "data":{
                                                "total_value_sol":portfolio["total_value_sol"],
                                                "total_invested_sol":portfolio["total_invested_sol"],
                                                "unrealized_pnl_sol":portfolio["unrealized_pnl_sol"],
                                                "realized_pnl_sol":portfolio["realized_pnl_sol"],
                                                "position_count":portfolio["position_count"],
                                                "updated_at":chrono::Utc::now().to_rfc3339()
                                            }
                                        }));
                                    }
                                }
                                Err(error) => {
                                    tracing::error!(%error, "Could not load portfolio positions for market mark")
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    tracing::error!(%error, mint_address = mint, "Could not update canonical token state")
                }
            }
        }
    });
}

#[allow(clippy::too_many_arguments)]
fn start_launch_processor(
    mut receiver: broadcast::Receiver<serde_json::Value>,
    events: broadcast::Sender<serde_json::Value>,
    rpc: SolanaRpc,
    database: sqlx::SqlitePool,
    pump_fun_program_id: String,
    control: Arc<RwLock<EngineControl>>,
    market_monitor: Option<MarketMonitor>,
    token_registry: TokenRegistry,
) {
    const LAUNCH_QUEUE_CAPACITY: usize = 256;
    const LAUNCH_WORKER_COUNT: usize = 4;
    let seen_signatures = Arc::new(Mutex::new(HashSet::<String>::new()));
    let (work_tx, work_rx) = tokio::sync::mpsc::channel::<(String, u64)>(LAUNCH_QUEUE_CAPACITY);
    let work_rx = Arc::new(Mutex::new(work_rx));

    for _ in 0..LAUNCH_WORKER_COUNT {
        let work_rx = work_rx.clone();
        let seen_signatures = seen_signatures.clone();
        let rpc = rpc.clone();
        let events = events.clone();
        let database = database.clone();
        let program_id = pump_fun_program_id.clone();
        let control = control.clone();
        let market_monitor = market_monitor.clone();
        let token_registry = token_registry.clone();
        tokio::spawn(async move {
            loop {
                let work = { work_rx.lock().await.recv().await };
                let Some((signature, slot_hint)) = work else {
                    break;
                };
                if control.read().await.paused {
                    continue;
                }
                let mut transaction = None;
                let mut last_error = None;
                for attempt in 0..4 {
                    match rpc.get_transaction(&signature).await {
                        Ok(Some(value)) => {
                            transaction = Some(value);
                            break;
                        }
                        Ok(None) => {
                            if attempt < 3 {
                                tokio::time::sleep(std::time::Duration::from_millis(
                                    300 * (attempt + 1),
                                ))
                                .await;
                            }
                        }
                        Err(error) => {
                            last_error = Some(error.to_string());
                            if attempt < 3 {
                                tokio::time::sleep(std::time::Duration::from_millis(
                                    300 * (attempt + 1),
                                ))
                                .await;
                            }
                        }
                    }
                }
                let Some(transaction) = transaction else {
                    let message = last_error.unwrap_or_else(|| {
                        "RPC returned null for this confirmed transaction after retries".to_string()
                    });
                    let _ = events.send(json!({"type":"diagnostic","data":{
                        "severity":"warning","component":"solana_get_transaction",
                        "message":message,"signature":signature,"slot_hint":slot_hint,
                        "retry_exhausted":true,"received_at":chrono::Utc::now().to_rfc3339()
                    }}));
                    // Allow reconnect/backfill to retry transactions that RPC could not return.
                    seen_signatures.lock().await.remove(&signature);
                    continue;
                };
                let decoded = match decode_pump_fun_create(&transaction, &signature, &program_id) {
                    Ok(Some(decoded)) => decoded,
                    Ok(None) => continue,
                    Err(error) => {
                        let _ = events.send(json!({"type":"diagnostic","data":{
                            "severity":"error","component":"pump_fun_decoder",
                            "message":error.to_string(),"signature":signature,
                            "received_at":chrono::Utc::now().to_rfc3339()
                        }}));
                        continue;
                    }
                };
                let launch = detect_pump_fun_launch(CreateInstruction {
                    program_id: program_id.clone(),
                    configured_pump_fun_program_id: program_id.clone(),
                    mint_address: decoded.mint_address,
                    creator_address: Some(decoded.creator_address.clone()),
                    signature: signature.clone(),
                    slot: if decoded.slot == 0 {
                        slot_hint
                    } else {
                        decoded.slot
                    },
                });
                let Some(launch) = launch else { continue };
                match nodex_persistence::save_launch(&database, &launch).await {
                    Ok(true) => {
                        let mint_address = launch.mint_address.clone();
                        let creator_address = launch.creator_address.clone();
                        // Register the token immediately. Scanner and market workers
                        // can now update independent domains of the same canonical record.
                        let initial_state = token_registry.upsert_patch(
                            &mint_address,
                            json!({
                                "identity": {"mint_address": mint_address.clone()},
                                "discovery": {
                                    "source": "pump_fun",
                                    "creator_address": creator_address.clone(),
                                    "signature": launch.signature.clone(),
                                    "slot": launch.slot,
                                    "detected_at": launch.detected_at.clone()
                                },
                                "security": {"status": "pending"},
                                "market": {"status": "starting"}
                            }),
                        );
                        match initial_state {
                            Ok(snapshot) => {
                                if let Err(error) =
                                    nodex_persistence::save_token_state(&database, &snapshot).await
                                {
                                    let _ = events.send(json!({"type":"diagnostic","data":{
                                        "severity":"error","component":"token_state_persistence",
                                        "message":error.to_string(),"mint_address":mint_address.clone(),
                                        "received_at":chrono::Utc::now().to_rfc3339()
                                    }}));
                                }
                                let _ = events
                                    .send(json!({"type":"token_state_updated","data":snapshot}));
                            }
                            Err(error) => {
                                let _ = events.send(json!({"type":"diagnostic","data":{
                                    "severity":"error","component":"token_registry",
                                    "message":error,"mint_address":mint_address.clone(),
                                    "received_at":chrono::Utc::now().to_rfc3339()
                                }}));
                            }
                        }
                        let _ = events.send(json!({"type":"launch_detected","data":launch}));
                        let _ = events.send(json!({"type":"token_scan_started","data":{
                            "mint_address":mint_address,
                            "status":"scanning",
                            "received_at":chrono::Utc::now().to_rfc3339()
                        }}));
                        let scan_rpc = rpc.clone();
                        let scan_events = events.clone();
                        let scan_mint = mint_address.clone();
                        let approved_monitor = market_monitor.clone();
                        let approved_program_id = program_id.clone();
                        let approved_registry = token_registry.clone();
                        let approved_database = database.clone();
                        tokio::spawn(async move {
                            let progress_events = scan_events.clone();
                            let progress_mint = scan_mint.clone();
                            match scan_rpc
                                .scan_token_security_with_progress(
                                    &scan_mint,
                                    move |stage, progress, message| {
                                        let _ = progress_events.send(
                                            json!({"type":"token_scan_progress","data":{
                                                "mint_address":progress_mint,
                                                "stage":stage,
                                                "progress":progress,
                                                "message":message,
                                                "received_at":chrono::Utc::now().to_rfc3339()
                                            }}),
                                        );
                                    },
                                )
                                .await
                            {
                                Ok(report) => {
                                    let decision = report.decision.clone();
                                    let _ = scan_events.send(json!({"type":"token_scan_update","data":report.clone()}));
                                    match decision.as_str() {
                                        "APPROVED" => {
                                            let approved_state = approved_registry.upsert_patch(
                                                &scan_mint,
                                                json!({"security":report}),
                                            );
                                            let approval_persisted = match approved_state {
                                                Ok(snapshot) => match nodex_persistence::save_token_state(&approved_database, &snapshot).await {
                                                    Ok(()) => true,
                                                    Err(error) => {
                                                        let _ = scan_events.send(json!({"type":"diagnostic","data":{
                                                            "severity":"error","component":"token_approval_persistence",
                                                            "message":error.to_string(),"mint_address":scan_mint
                                                        }}));
                                                        false
                                                    }
                                                },
                                                Err(error) => {
                                                    let _ = scan_events.send(json!({"type":"diagnostic","data":{
                                                        "severity":"error","component":"token_approval_registry",
                                                        "message":error,"mint_address":scan_mint
                                                    }}));
                                                    false
                                                }
                                            };
                                            if approval_persisted {
                                            if let Some(monitor) = approved_monitor.as_ref() {
                                                match monitor
                                                    .subscribe_pump_fun_bonding_curve(
                                                        &scan_mint,
                                                        &approved_program_id,
                                                    )
                                                    .await
                                                {
                                                    Ok(account_address) => {
                                                        let _ = scan_events.send(json!({"type":"market_subscription_requested","data":{
                                                            "mint_address":scan_mint,
                                                            "account_address":account_address,
                                                            "venue":"pump_fun_bonding_curve",
                                                            "status":"pending",
                                                            "received_at":chrono::Utc::now().to_rfc3339()
                                                        }}));
                                                    }
                                                    Err(error) => {
                                                        let _ = scan_events.send(json!({"type":"diagnostic","data":{
                                                            "severity":"warning","component":"market_monitor_registration",
                                                            "message":error.to_string(),"mint_address":scan_mint,
                                                            "received_at":chrono::Utc::now().to_rfc3339()
                                                        }}));
                                                    }
                                                }
                                            }
                                            }
                                        }
                                        "REJECTED" => {
                                            let _ = scan_events.send(json!({"type":"token_rejected_cleanup","data":{
                                                "mint_address":scan_mint,
                                                "reason":"Security scanner rejected this token.",
                                                "received_at":chrono::Utc::now().to_rfc3339()
                                            }}));
                                        }
                                        _ => {
                                            let _ = scan_events.send(json!({"type":"token_approval_pending","data":{
                                                "mint_address":scan_mint,
                                                "decision":decision,
                                                "reason":"Token remains isolated until an explicit APPROVED decision is produced."
                                            }}));
                                        }
                                    }
                                }
                                Err(error) => {
                                    let _ = scan_events.send(json!({"type":"diagnostic","data":{
                                        "severity":"error","component":"token_security_scanner",
                                        "message":error.to_string(),"mint_address":scan_mint,
                                        "received_at":chrono::Utc::now().to_rfc3339()
                                    }}));
                                    let _ = scan_events.send(json!({"type":"token_scan_update","data":{
                                        "mint_address":scan_mint,"decision":"UNVERIFIED",
                                        "decision_reason":"Scanner failed; token remains isolated and no market or metadata service may use it.",
                                        "status":"unverified","received_at":chrono::Utc::now().to_rfc3339()
                                    }}));
                                }
                            }
                        });


                    }
                    Err(error) => {
                        tracing::warn!(%error, "Could not synchronize Solana wallet balance")
                    }
                }
            }
        });
    }

    let wallet = match nodex_persistence::load_paper_wallet(&db).await? {
        Some(wallet) => wallet,
        None => {
            let wallet = PaperWallet::new(config.paper_starting_balance_lamports);
            nodex_persistence::save_paper_wallet(&db, &wallet).await?;
            wallet
        }
    };

    // Keep canonical token state synchronized from independent worker events.
    start_token_state_projector(
        events.subscribe(),
        events.clone(),
        db.clone(),
        token_registry.clone(),
        profile_state.clone(),
    );
    // Candles are generated only for tokens whose security decision is APPROVED.
    chart_service::start_chart_service(
        events.subscribe(),
        events.clone(),
        db.clone(),
        token_registry.clone(),
    );
    let stream_status = Arc::new(RwLock::new(StreamStatus::default()));
    let rpc = config.solana_http_url.clone().map(SolanaRpc::new);
    let control = Arc::new(RwLock::new(EngineControl {
        paused: false,
        mode: "paper".into(),
    }));

    // Keep launch detection and market monitoring on separate subscriptions.
    // The market monitor multiplexes all watched bonding curves over one WebSocket.
    let market_monitor = config
        .solana_ws_url
        .clone()
        .map(|ws_url| start_market_monitor(ws_url, events.clone()));

    // Decode Pump.fun creation transactions from confirmed logs notifications.
    if let Some(launch_rpc) = rpc.clone() {
        start_launch_processor(
            events.subscribe(),
            events.clone(),
            launch_rpc,
            db.clone(),
            config.pump_fun_program_id.clone(),
            control.clone(),
            market_monitor.clone(),
            token_registry.clone(),
        );
    } else {
        let _ = events.send(json!({"type":"diagnostic","data":{
            "severity":"warning","component":"solana_http_rpc",
            "message":"Token transaction decoding is disabled because SOLANA_RPC_HTTP_URL / HELIUS_API_KEY is not configured.",
            "received_at":chrono::Utc::now().to_rfc3339()
        }}));
    }

    // PumpSwap activity uses its own program-level subscription. The provider's
    // mentions filter accepts one program address per subscription.
    start_pumpswap_activity_processor(
        events.subscribe(),
        events.clone(),
        control.clone(),
        rpc.clone(),
    );

    let pumpswap_status = Arc::new(RwLock::new(StreamStatus::default()));
    if let Some(ws_url) = config.solana_ws_url.clone() {
        let sender = events.clone();
        let status = pumpswap_status.clone();
        let recovery_rpc = rpc.clone();
        tokio::spawn(async move {
            run_logs_stream(
                ws_url,
                PUMPSWAP_PROGRAM_ID.to_owned(),
                sender,
                status,
                recovery_rpc,
            )
            .await;
        });
    }

    // Persist raw upstream log notifications independently of browser subscribers.
    // Batch writes avoid one SQLite transaction/fsync per Solana notification.
    let mut persistence_receiver = events.subscribe();
    let persistence_db = db.clone();
    tokio::spawn(async move {
        let mut pending = Vec::<serde_json::Value>::with_capacity(100);
        let mut flush_tick = tokio::time::interval(std::time::Duration::from_millis(250));
        flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        flush_tick.tick().await;
        loop {
            tokio::select! {
                event = persistence_receiver.recv() => match event {
                    Ok(event) => {
                        if event.get("method").and_then(serde_json::Value::as_str)
                            == Some("logsNotification")
                        {
                            pending.push(event);
                            if pending.len() == 100 {
                                match nodex_persistence::save_raw_solana_events(&persistence_db, &pending).await {
                                    Ok(()) => pending.clear(),
                                    Err(error) => tracing::error!(%error, batch_size = pending.len(), "Could not persist raw Solana event batch"),
                                }
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        tracing::warn!(
                            dropped_notifications = count,
                            pending_notifications = pending.len(),
                            "Raw Solana event persistence subscriber lagged; recovered notifications may be backfilled after reconnect"
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                _ = flush_tick.tick() => {
                    if !pending.is_empty() {
                        match nodex_persistence::save_raw_solana_events(&persistence_db, &pending).await {
                            Ok(()) => pending.clear(),
                            Err(error) => tracing::error!(%error, batch_size = pending.len(), "Could not persist raw Solana event batch"),
                        }
                    }
                }
            }
        }
    });

    if let Some(ws_url) = config.solana_ws_url.clone() {
        let sender = events.clone();
        let status = stream_status.clone();
        let program_id = config.pump_fun_program_id.clone();
        let recovery_rpc = rpc.clone();
        tokio::spawn(async move {
            run_logs_stream(ws_url, program_id, sender, status, recovery_rpc).await;
        });
    } else {
        tracing::warn!("Helius not configured; set HELIUS_API_KEY or SOLANA_RPC_WS_URL");
    }

    let state = AppState {
        events,
        stream_status,
        pumpswap_status,
        wallet: Arc::new(RwLock::new(wallet)),
        rpc,
        database: db.clone(),
        control,
        admin_api_key: std::env::var("NODEX_ADMIN_API_KEY")
            .ok()
            .filter(|value| !value.trim().is_empty()),
        ws_tickets: Arc::new(Mutex::new(HashMap::new())),
        token_registry,
        profile: profile_state,
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/engine/status", get(engine_status))
        .route("/api/v1/paper-wallet", get(paper_wallet))
        .route("/api/v1/profile", get(get_profile))
        .route("/api/v1/tokens/recent", get(recent_launches))
        .route("/api/v1/tokens/:mint/state", get(token_state))
        .route("/api/v1/events/recent", get(recent_events))
        .route(
            "/api/v1/admin/maintenance/clear-history",
            post(clear_token_and_event_history),
        )
        .route("/api/v1/tokens/:mint/scan", get(scan_token))
        .route("/api/v1/fees/token-account-rent", get(token_account_rent))
        .route("/api/v1/ws-ticket", post(issue_ws_ticket))
        .route("/api/v1/ws", get(ws_handler))
        .with_state(state)
        // The personal dashboard is hosted on a different origin. The REST API
        // currently exposes read-only GET routes, so permit browser reads without
        // enabling credentialed cross-origin access.
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http());

    let address: SocketAddr = config.bind_addr.parse()?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!(%address, mode = "paper", "NodeX engine API listening");
    axum::serve(listener, app).await?;
    Ok(())
}
