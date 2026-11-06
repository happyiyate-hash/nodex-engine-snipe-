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
                                                &database, position_id, mint, "open", position,
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
                        if let Some(monitor) = market_monitor.as_ref() {
                            match monitor
                                .subscribe_pump_fun_bonding_curve(&mint_address, &program_id)
                                .await
                            {
                                Ok(account_address) => {
                                    let _ = events.send(
                                        json!({"type":"market_subscription_requested","data":{
                                            "mint_address":mint_address,
                                            "account_address":account_address,
                                            "venue":"pump_fun_bonding_curve",
                                            "status":"pending",
                                            "received_at":chrono::Utc::now().to_rfc3339()
                                        }}),
                                    );
                                }
                                Err(error) => {
                                    let _ = events.send(json!({"type":"diagnostic","data":{
                                        "severity":"warning",
                                        "component":"market_monitor_registration",
                                        "message":error.to_string(),
                                        "mint_address":mint_address,
                                        "received_at":chrono::Utc::now().to_rfc3339()
                                    }}));
                                }
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
                                    let _ = scan_events
                                        .send(json!({"type":"token_scan_update","data":report}));
                                }
                                Err(error) => {
                                    let _ = scan_events.send(json!({"type":"diagnostic","data":{
                                        "severity":"error","component":"token_security_scanner",
                                        "message":error.to_string(),"mint_address":scan_mint,
                                        "received_at":chrono::Utc::now().to_rfc3339()
                                    }}));
                                    let _ = scan_events.send(json!({"type":"token_scan_update","data":{
                                        "mint_address":scan_mint,"decision":"UNVERIFIED",
                                        "decision_reason":"Scanner failed; no automatic paper buy is permitted.",
                                        "status":"unverified","received_at":chrono::Utc::now().to_rfc3339()
                                    }}));
                                }
                            }
                        });

                        if let Some(creator) = creator_address {
                            let history_rpc = rpc.clone();
                            let history_events = events.clone();
                            let history_mint = mint_address.clone();
                            let history_signature = signature.clone();
                            let history_program = program_id.clone();
                            tokio::spawn(async move {
                                let _ = history_events.send(
                                    json!({"type":"creator_history_started","data":{
                                        "mint_address":history_mint,
                                        "creator_address":creator,
                                        "status":"scanning",
                                        "received_at":chrono::Utc::now().to_rfc3339()
                                    }}),
                                );
                                match history_rpc
                                    .scan_creator_history(
                                        &creator,
                                        &history_signature,
                                        &history_program,
                                    )
                                    .await
                                {
                                    Ok(history) => {
                                        let _ = history_events.send(
                                            json!({"type":"creator_history_update","data":{
                                                "mint_address":history_mint,
                                                "history":history,
                                                "received_at":chrono::Utc::now().to_rfc3339()
                                            }}),
                                        );
                                    }
                                    Err(error) => {
                                        let _ = history_events.send(json!({"type":"diagnostic","data":{
                                            "severity":"warning","component":"creator_history_scanner",
                                            "message":error.to_string(),"mint_address":history_mint,
                                            "creator_address":creator,
                                            "received_at":chrono::Utc::now().to_rfc3339()
                                        }}));
                                        let _ = history_events.send(json!({"type":"creator_history_update","data":{
                                            "mint_address":history_mint,
                                            "history":{
                                                "creator_address":creator,
                                                "status":"unverified",
                                                "early_selling":"UNVERIFIED",
                                                "sample_is_partial":true,
                                                "caveat":"Creator history RPC failed; no conclusion can be drawn."
                                            },
                                            "received_at":chrono::Utc::now().to_rfc3339()
                                        }}));
                                    }
                                }
                            });
                        }
                    }
                    Ok(false) => {}
                    Err(error) => {
                        let _ = events.send(json!({"type":"diagnostic","data":{
                            "severity":"error","component":"launch_persistence",
                            "message":error.to_string(),"signature":signature,
                            "received_at":chrono::Utc::now().to_rfc3339()
                        }}));
                    }
                }
            }
        });
    }

    tokio::spawn(async move {
        loop {
            let event = match receiver.recv().await {
                Ok(event) => event,
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    let _ = events.send(json!({"type":"diagnostic","data":{
                        "severity":"warning","component":"launch_processor",
                        "message":"Transaction notification consumer lagged; some notifications may need RPC backfill.",
                        "dropped_notifications":count,"received_at":chrono::Utc::now().to_rfc3339()
                    }}));
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            };
            if event.get("method").and_then(serde_json::Value::as_str) != Some("logsNotification")
                || event
                    .get("nodex_program_id")
                    .and_then(serde_json::Value::as_str)
                    != Some(pump_fun_program_id.as_str())
            {
                continue;
            }
            if control.read().await.paused {
                continue;
            }
            let Some(value) = event.pointer("/params/result/value") else {
                continue;
            };
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
            let slot_hint = event
                .pointer("/params/result/context/slot")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default();
            if let Err(error) = work_tx.try_send((signature.clone(), slot_hint)) {
                let (reason, retryable) = match error {
                    tokio::sync::mpsc::error::TrySendError::Full(_) => ("queue_full", true),
                    tokio::sync::mpsc::error::TrySendError::Closed(_) => ("queue_closed", true),
                };
                if retryable {
                    seen_signatures.lock().await.remove(&signature);
                }
                let _ = events.send(json!({"type":"diagnostic","data":{
                    "severity":"warning","component":"launch_processor_queue",
                    "message":"Launch transaction was not admitted to the bounded worker queue; it remains eligible for a later backfill/re-delivery.",
                    "reason":reason,"signature":signature,"slot_hint":slot_hint,
                    "queue_capacity":LAUNCH_QUEUE_CAPACITY,
                    "received_at":chrono::Utc::now().to_rfc3339()
                }}));
                if reason == "queue_closed" {
                    break;
                }
            }
        }
    });
}
async fn health() -> Json<Health> {
    Json(Health {
        status: "ok",
        mode: "paper",
        service: "nodex-engine",
    })
}

async fn engine_status(State(state): State<AppState>) -> Json<serde_json::Value> {
    let stream = state.stream_status.read().await.clone();
    let pumpswap_stream = state.pumpswap_status.read().await.clone();
    let control = state.control.read().await.clone();
    Json(json!({
        "mode": control.mode,
        "paused": control.paused,
        "live_execution_enabled": false,
        "live_execution_reason": "Live signing and transaction submission are not implemented; the backend refuses live mode.",
        "solana_stream": stream,
        "solana_streams": {
            "pump_fun": stream,
            "pumpswap": pumpswap_stream
        }
    }))
}

/// Temporary maintenance endpoint. Deletes historical launch rows and raw
/// upstream notification history only; live subscriptions and monitor state stay running.
async fn clear_token_and_event_history(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let configured_key = state
        .admin_api_key
        .as_deref()
        .filter(|key| !key.is_empty())
        .ok_or((
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "Maintenance endpoint is disabled: set NODEX_ADMIN_API_KEY on the backend first."
                .to_string(),
        ))?;
    let supplied_key = headers
        .get("x-nodex-admin-key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if supplied_key.is_empty() || supplied_key != configured_key {
        return Err((
            axum::http::StatusCode::UNAUTHORIZED,
            "Invalid or missing NodeX admin key.".to_string(),
        ));
    }

    let (deleted_token_records, deleted_raw_event_records) =
        nodex_persistence::clear_token_and_event_history(&state.database)
            .await
            .map_err(|error| {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("History cleanup failed: {error}"),
                )
            })?;

    tracing::warn!(
        deleted_token_records,
        deleted_raw_event_records,
        "NodeX historical token and raw event records cleared through maintenance API"
    );

    Ok(Json(json!({
        "status": "ok",
        "deleted_token_records": deleted_token_records,
        "deleted_raw_event_records": deleted_raw_event_records,
        "preserved": [
            "live token discovery and market subscriptions",
            "paper wallet and ledger",
            "database schema and configuration",
            "source code, API keys, and environment configuration"
        ]
    })))
}

async fn token_state(
    State(state): State<AppState>,
    Path(mint): Path<String>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    if let Some(snapshot) = state.token_registry.get(&mint) {
        return Ok(Json(
            serde_json::to_value(snapshot).unwrap_or_else(|_| json!({})),
        ));
    }
    match nodex_persistence::get_token_state(&state.database, &mint).await {
        Ok(Some(snapshot)) => Ok(Json(
            serde_json::to_value(snapshot).unwrap_or_else(|_| json!({})),
        )),
        Ok(None) => Err((
            axum::http::StatusCode::NOT_FOUND,
            "Token state not found".to_string(),
        )),
        Err(error) => Err((
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            error.to_string(),
        )),
    }
}

async fn recent_launches(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Vec<serde_json::Value>>, (axum::http::StatusCode, String)> {
    let limit = query
        .get("limit")
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(100);
    let launches = nodex_persistence::list_recent_launches(&state.database, limit)
        .await
        .map_err(|error| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                error.to_string(),
            )
        })?;
    Ok(Json(launches))
}

async fn recent_events(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Vec<serde_json::Value>>, (axum::http::StatusCode, String)> {
    let limit = query
        .get("limit")
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(200);
    let events = nodex_persistence::list_recent_events(&state.database, limit)
        .await
        .map_err(|error| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                error.to_string(),
            )
        })?;
    Ok(Json(events))
}

async fn scan_token(
    State(state): State<AppState>,
    Path(mint): Path<String>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let rpc = state.rpc.ok_or((
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "Solana HTTP RPC is not configured".to_string(),
    ))?;
    let report = rpc.scan_token_security(&mint).await.map_err(|error| {
        (
            axum::http::StatusCode::BAD_GATEWAY,
            format!("Token security scan failed: {error}"),
        )
    })?;
    Ok(Json(
        serde_json::to_value(report).unwrap_or_else(|_| json!({})),
    ))
}

async fn paper_wallet(State(state): State<AppState>) -> Json<PaperWallet> {
    Json(state.wallet.read().await.clone())
}

/// Returns one assembled profile snapshot. Wallet SOL is the on-chain balance;
/// paper-trading funds and positions remain separate accounting domains.
async fn get_profile(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(state.profile.read().await.clone())
}

async fn token_account_rent(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let rpc = state.rpc.ok_or((
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "Solana HTTP RPC is not configured".to_string(),
    ))?;
    let rent = rpc
        .token_account_rent_lamports()
        .await
        .map_err(|error| (axum::http::StatusCode::BAD_GATEWAY, error.to_string()))?;
    Ok(Json(json!({
        "account_size_bytes": 165,
        "rent_lamports": rent,
        "rent_sol": rent as f64 / LAMPORTS_PER_SOL as f64,
        "source": "solana_getMinimumBalanceForRentExemption"
    })))
}

fn is_frontend_event(value: &serde_json::Value) -> bool {
    matches!(
        value.get("type").and_then(serde_json::Value::as_str),
        Some("connection_status")
            | Some("engine_state")
            | Some("launch_detected")
            | Some("token_scan_started")
            | Some("token_scan_update")
            | Some("token_scan_progress")
            | Some("token_state_updated")
            | Some("token_market_update")
            | Some("chart_candle_update")
            | Some("chart_candle_closed")
            | Some("token_chart_snapshot")
            | Some("token_detail_status")
            | Some("token_statistics_update")
            | Some("profile_updated")
            | Some("wallet_balance_updated")
            | Some("position_updated")
            | Some("portfolio_summary_updated")
            | Some("market_price_update")
            | Some("paper_buy_executed")
            | Some("paper_buy_failed")
            | Some("strategy_rejected_token")
            | Some("diagnostic")
            | Some("provider_status")
            | Some("market_account_update")
            | Some("market_curve_completed")
            | Some("market_monitor_status")
            | Some("market_subscription_requested")
            | Some("market_subscription_active")
            | Some("creator_history_started")
            | Some("creator_history_update")
            | Some("stream_warning")
            | Some("reconnect_recovery_status")
            | Some("pumpswap_activity")
            | Some("pumpswap_volume_update")
            | Some("command_rejected")
    )
}

async fn issue_ws_ticket(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let expected = state
        .admin_api_key
        .as_deref()
        .filter(|key| !key.is_empty())
        .ok_or((
            StatusCode::SERVICE_UNAVAILABLE,
            "WebSocket ticket authentication is not configured.".to_string(),
        ))?;
    let provided = headers
        .get("x-nodex-admin-key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if provided.is_empty() || provided != expected {
        return Err((
            StatusCode::UNAUTHORIZED,
            "Invalid or missing NodeX operator key.".to_string(),
        ));
    }

    let ticket = uuid::Uuid::new_v4().to_string();
    let expires_at = Instant::now() + Duration::from_secs(30);
    let mut tickets = state.ws_tickets.lock().await;
    tickets.retain(|_, expiry| *expiry > Instant::now());
    tickets.insert(ticket.clone(), expires_at);
    Ok(Json(json!({
        "ticket": ticket,
        "expires_in_seconds": 30,
        "single_use": true
    })))
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let Some(ticket) = query.get("ticket").filter(|value| !value.is_empty()) else {
        return (
            StatusCode::UNAUTHORIZED,
            "Missing WebSocket ticket.".to_string(),
        )
            .into_response();
    };
    let valid = {
        let mut tickets = state.ws_tickets.lock().await;
        tickets
            .remove(ticket)
            .is_some_and(|expiry| expiry > Instant::now())
    };
    if !valid {
        return (
            StatusCode::UNAUTHORIZED,
            "Invalid, expired, or already-used WebSocket ticket.".to_string(),
        )
            .into_response();
    }
    ws.on_upgrade(move |socket| client_socket(socket, state))
        .into_response()
}

async fn client_socket(socket: WebSocket, state: AppState) {
    static NEXT_EVENT_ID: AtomicU64 = AtomicU64::new(1);
    let (mut sender, mut receiver_socket) = socket.split();
    let mut receiver = state.events.subscribe();
    let mut subscribed_tokens = HashSet::<String>::new();
    let status = state.stream_status.read().await.clone();
    let pumpswap_status = state.pumpswap_status.read().await.clone();
    let control = state.control.read().await.clone();
    let initial = json!({"type":"connection_status","data":{"solana_stream":status,"solana_streams":{"pump_fun":status,"pumpswap":pumpswap_status},"engine":control,"capabilities":{"paper_trading":true,"live_trading":false,"token_scanning":state.rpc.is_some() && status.connected,"token_security_scanning":state.rpc.is_some() && status.connected,"transaction_decoding":"pump_fun_create_and_create_v2","transaction_decoding_enabled":state.rpc.is_some() && status.connected}}}).to_string();
    if sender.send(Message::Text(initial)).await.is_err() {
        return;
    }
    // Replay a bounded launch snapshot so a browser reconnect does not start empty.
    match nodex_persistence::list_recent_launches(&state.database, 100).await {
        Ok(launches) => {
            let snapshot = json!({"type":"launch_snapshot","data":{
                "launches":launches,
                "received_at":chrono::Utc::now().to_rfc3339()
            }})
            .to_string();
            if sender.send(Message::Text(snapshot)).await.is_err() {
                return;
            }
        }
        Err(error) => {
            tracing::warn!(%error, "Could not load recent launches for WebSocket snapshot")
        }
    }
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(20));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;
    let mut pending_dropped_events = 0_u64;
    let mut last_lag_warning = std::time::Instant::now() - std::time::Duration::from_secs(5);
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                let heartbeat_message = json!({
                    "type":"heartbeat",
                    "data":{"server_timestamp":chrono::Utc::now().to_rfc3339()}
                }).to_string();
                if sender.send(Message::Text(heartbeat_message)).await.is_err() {
                    break;
                }
            }
            event = receiver.recv() => match event {
                Ok(mut value) => {
                    let event_type = value.get("type").and_then(serde_json::Value::as_str).unwrap_or_default();

                    // Full token snapshots are delivered only to clients that explicitly
                    // subscribed to that mint. They are never broadcast to every browser.
                    if event_type == "token_state_updated" || event_type == "token_market_update" || event_type == "chart_candle_update" || event_type == "chart_candle_closed" || event_type == "token_statistics_update" {
                        let mint = value.pointer("/data/mint_address")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default();
                        if !subscribed_tokens.contains(mint) {
                            continue;
                        }
                    } else if event_type == "market_price_update" {
                        // The global stream receives only a compact market tick.
                        let Some(data) = value.get("data").cloned() else { continue };
                        let mint = data.get("mint_address").and_then(serde_json::Value::as_str).unwrap_or_default();
                        if mint.is_empty() { continue; }
                        let snapshot = state.token_registry.get(mint);
                        let identity = snapshot.as_ref().and_then(|item| item.data.get("identity"));
                        value = json!({
                            "type":"market_tick",
                            "data":{
                                "mint_address":mint,
                                "name":identity.and_then(|v| v.get("name")).cloned().unwrap_or(serde_json::Value::Null),
                                "symbol":identity.and_then(|v| v.get("symbol")).cloned().unwrap_or(serde_json::Value::Null),
                                "price_sol":data.get("price_sol").cloned().unwrap_or(serde_json::Value::Null),
                                "price_change_1m_pct":data.get("price_change_1m_pct").cloned().unwrap_or(serde_json::Value::Null),
                                "price_change_5m_pct":data.get("price_change_5m_pct").cloned().unwrap_or(serde_json::Value::Null),
                                "market_cap_sol":data.get("market_cap_sol").cloned().unwrap_or(serde_json::Value::Null),
                                "timestamp":data.get("received_at").cloned().unwrap_or(serde_json::Value::Null),
                                "version":snapshot.as_ref().map(|item| item.version)
                            }
                        });
                    } else if !is_frontend_event(&value) {
                        continue;
                    }
                    let paused = state.control.read().await.paused;
                    let is_market_stream = value.get("method").and_then(|v| v.as_str()) == Some("logsNotification");
                    if paused && is_market_stream {
                        continue;
                    }
                    if let Some(object) = value.as_object_mut() {
                        let now_ms = chrono::Utc::now().timestamp_millis();
                        object.entry("event_id").or_insert_with(|| json!(format!("nodex-{now_ms}-{}", NEXT_EVENT_ID.fetch_add(1, Ordering::Relaxed))));
                        object.entry("server_timestamp").or_insert_with(|| json!(now_ms));
                    }
                    if sender.send(Message::Text(value.to_string())).await.is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    pending_dropped_events = pending_dropped_events.saturating_add(count);
                    if last_lag_warning.elapsed() >= std::time::Duration::from_secs(5) {
                        let now_ms = chrono::Utc::now().timestamp_millis();
                        let warning = json!({
                            "type": "stream_warning",
                            "event_id": format!("nodex-stream-gap-{now_ms}-{}", NEXT_EVENT_ID.fetch_add(1, Ordering::Relaxed)),
                            "server_timestamp": now_ms,
                            "data": {
                                "severity":"warning",
                                "component":"backend_websocket_broadcaster",
                                "message":"The browser event subscriber fell behind the backend broadcast stream.",
                                "dropped_events": pending_dropped_events,
                                "aggregation_window_seconds": 5,
                                "received_at": chrono::Utc::now().to_rfc3339()
                            }
                        }).to_string();
                        pending_dropped_events = 0;
                        last_lag_warning = std::time::Instant::now();
                        if sender.send(Message::Text(warning)).await.is_err() {
                            break;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
            incoming = receiver_socket.next() => match incoming {
                Some(Ok(Message::Ping(payload))) => {
                    if sender.send(Message::Pong(payload)).await.is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Text(text))) => {
                    let result = match serde_json::from_str::<ClientCommand>(&text) {
                        Ok(ClientCommand::Pause) => {
                            state.control.write().await.paused = true;
                            Some(json!({"type":"engine_state","data":{"paused":true,"mode":"paper"}}))
                        }
                        Ok(ClientCommand::Resume) | Ok(ClientCommand::Start) => {
                            state.control.write().await.paused = false;
                            Some(json!({"type":"engine_state","data":{"paused":false,"mode":"paper"}}))
                        }
                        Ok(ClientCommand::SetMode { mode }) if mode == "paper" => {
                            state.control.write().await.mode = "paper".into();
                            Some(json!({"type":"engine_state","data":{"paused":state.control.read().await.paused,"mode":"paper"}}))
                        }
                        Ok(ClientCommand::SetMode { mode }) => Some(json!({"type":"command_rejected","data":{"command":"set_mode","requested_mode":mode,"reason":"Live mode is not implemented or enabled. Paper mode remains active.","live_execution_enabled":false}})),
                        Ok(ClientCommand::Status) => {
                            let control = state.control.read().await.clone();
                            Some(json!({"type":"engine_state","data":control}))
                        }
                        Ok(ClientCommand::SubscribeToken { mint }) => {
                            let mint = mint.trim().to_owned();
                            if mint.is_empty() || mint.len() > 64 {
                                Some(json!({"type":"command_rejected","data":{"command":"subscribe_token","mint":mint,"reason":"A valid token mint address is required."}}))
                            } else {
                                subscribed_tokens.insert(mint.clone());
                                let snapshot = match state.token_registry.get(&mint) {
                                    Some(snapshot) => Some(snapshot),
                                    None => match nodex_persistence::get_token_state(&state.database, &mint).await {
                                        Ok(snapshot) => snapshot,
                                        Err(error) => {
                                            tracing::warn!(%error, mint_address = mint, "Could not load token snapshot for WebSocket subscription");
                                            None
                                        }
                                    }
                                };
                                match snapshot {
                                    Some(snapshot) => {
                                        let positions = match nodex_persistence::load_portfolio_positions(&state.database).await {
                                            Ok(items) => items,
                                            Err(error) => {
                                                tracing::warn!(%error, mint_address = mint, "Could not load positions for token detail snapshot");
                                                Vec::new()
                                            }
                                        };
                                        let mut details = token_details::build_token_detail_snapshot(
                                            &mint,
                                            Some(&snapshot),
                                            &positions,
                                        );
                                        let candles = match nodex_persistence::load_chart_candles(
                                            &state.database, &mint, "5m", 500,
                                        ).await {
                                            Ok(items) => items,
                                            Err(error) => {
                                                tracing::warn!(%error, mint_address = mint, "Could not load persisted chart candles");
                                                Vec::new()
                                            }
                                        };
                                        if let Some(chart) = details.get_mut("chart").and_then(serde_json::Value::as_object_mut) {
                                            let has_candles = !candles.is_empty();
                                            chart.insert("timeframe".into(), json!("5m"));
                                            chart.insert("candles".into(), json!(candles));
                                            chart.insert("status".into(), json!(if has_candles { "ready" } else { "warming_up" }));
                                        }
                                        Some(json!({
                                            "type":"token_detail_snapshot",
                                            "data":{
                                                "subscription":"active",
                                                "mint_address":mint,
                                                "snapshot":details
                                            }
                                        }))
                                    }
                                    None => {
                                        subscribed_tokens.remove(&mint);
                                        Some(json!({"type":"command_rejected","data":{"command":"subscribe_token","mint":mint,"reason":"Token state was not found."}}))
                                    }
                                }
                            }
                        }
                        Ok(ClientCommand::SetChartTimeframe { mint, timeframe }) => {
                            let mint = mint.trim().to_owned();
                            let allowed = ["1m", "5m", "15m", "1h", "24h"];
                            if !subscribed_tokens.contains(&mint) || !allowed.contains(&timeframe.as_str()) {
                                Some(json!({"type":"command_rejected","data":{
                                    "command":"set_chart_timeframe","mint_address":mint,
                                    "timeframe":timeframe,
                                    "reason":"Subscribe to the token first and use one of: 1m, 5m, 15m, 1h, 24h."
                                }}))
                            } else {
                                match nodex_persistence::load_chart_candles(&state.database, &mint, &timeframe, 500).await {
                                    Ok(candles) => Some(json!({"type":"token_chart_snapshot","data":{
                                        "mint_address":mint,"timeframe":timeframe,"candles":candles,
                                        "status":if candles.is_empty() {"warming_up"} else {"ready"}
                                    }})),
                                    Err(error) => {
                                        tracing::warn!(%error, mint_address=mint, timeframe, "Could not load requested chart timeframe");
                                        Some(json!({"type":"token_detail_status","data":{
                                            "mint_address":mint,"domain":"chart","status":"unavailable",
                                            "timeframe":timeframe
                                        }}))
                                    }
                                }
                            }
                        }
                        Ok(ClientCommand::UnsubscribeToken { mint }) => {
                            let mint = mint.trim().to_owned();
                            subscribed_tokens.remove(&mint);
                            Some(json!({"type":"token_unsubscribed","data":{"mint_address":mint}}))
                        }
                        Err(error) => Some(json!({"type":"diagnostic","data":{"severity":"error","component":"websocket_command","message":error.to_string(),"received_at":chrono::Utc::now().to_rfc3339()}})),
                    };
                    if let Some(value) = result {
                        if sender.send(Message::Text(value.to_string())).await.is_err() { break; }
                    }
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                _ => {}
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = EngineConfig::from_env();
    let db = nodex_persistence::connect(&config.database_url).await?;
    let token_registry = TokenRegistry::new();
    for snapshot in nodex_persistence::load_token_states(&db).await? {
        token_registry
            .restore(snapshot)
            .map_err(|error| anyhow::anyhow!(error))?;
    }
    tracing::info!(
        restored_tokens = token_registry.len(),
        "Restored canonical token state registry"
    );
    // The public wallet identity is derived from the configured private key.
    // The secret is parsed only in memory and is never persisted or returned by the API.
    let wallet_keypair = std::env::var("NODEX_WALLET_PRIVATE_KEY")
        .ok()
        .and_then(|raw| {
            let trimmed = raw.trim();
            let bytes = if trimmed.starts_with('[') {
                serde_json::from_str::<Vec<u8>>(trimmed).ok()
            } else {
                bs58::decode(trimmed).into_vec().ok()
            }?;
            Keypair::try_from(bytes.as_slice()).ok()
        });
    let wallet_address = wallet_keypair
        .as_ref()
        .map(|keypair| keypair.pubkey().to_string());
    if wallet_address.is_none() {
        tracing::warn!("NODEX_WALLET_PRIVATE_KEY is missing or invalid; profile wallet address and live SOL balance are unavailable");
    }
    let mut profile = nodex_persistence::load_profile(&db).await?.unwrap_or_else(|| json!({
        "profile": {
            "wallet_address": wallet_address,
            "name": std::env::var("NODEX_PROFILE_NAME").unwrap_or_else(|_| "NodeX".into()),
            "symbol": "SOL",
            "profile_url": "",
            "profile_picture_url": ""
        },
        "wallet": {"sol_balance": 0.0, "last_updated": null},
        "portfolio": {"total_value_sol": 0.0, "unrealized_pnl_sol": 0.0, "realized_pnl_sol": 0.0, "positions": []},
        "existing_wallet": {"sol_balance": 0.0, "last_synced": null}
    }));
    if let Some(profile_obj) = profile
        .get_mut("profile")
        .and_then(serde_json::Value::as_object_mut)
    {
        profile_obj.insert("wallet_address".into(), json!(wallet_address));
    }
    nodex_persistence::save_profile(&db, &profile).await?;
    let profile_state = Arc::new(RwLock::new(profile));
    let (events, _) = broadcast::channel(8192);
    chart_service::start_chart_service(events.subscribe(), events.clone(), db.clone());
    if let (Some(rpc_client), Some(address)) = (
        config.solana_http_url.clone().map(SolanaRpc::new),
        wallet_address.clone(),
    ) {
        let profile_for_sync = profile_state.clone();
        let database_for_sync = db.clone();
        let events_for_sync = events.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(15));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                match rpc_client.get_balance_lamports(&address).await {
                    Ok(lamports) => {
                        let balance_sol = lamports as f64 / LAMPORTS_PER_SOL as f64;
                        let mut current = profile_for_sync.write().await;
                        if let Some(wallet) = current
                            .get_mut("wallet")
                            .and_then(serde_json::Value::as_object_mut)
                        {
                            wallet.insert("sol_balance".into(), json!(balance_sol));
                            wallet.insert(
                                "last_updated".into(),
                                json!(chrono::Utc::now().to_rfc3339()),
                            );
                        }
                        if let Some(existing) = current
                            .get_mut("existing_wallet")
                            .and_then(serde_json::Value::as_object_mut)
                        {
                            existing.insert("sol_balance".into(), json!(balance_sol));
                            existing.insert(
                                "last_synced".into(),
                                json!(chrono::Utc::now().to_rfc3339()),
                            );
                        }
                        let snapshot = current.clone();
                        drop(current);
                        if let Err(error) =
                            nodex_persistence::save_profile(&database_for_sync, &snapshot).await
                        {
                            tracing::error!(%error, "Could not persist synchronized wallet profile");
                        } else {
                            let _ = events_for_sync.send(json!({
                                "type":"wallet_balance_updated",
                                "data":{
                                    "sol_balance":balance_sol,
                                    "last_synced":snapshot.pointer("/existing_wallet/last_synced").cloned().unwrap_or(serde_json::Value::Null)
                                }
                            }));
                        }
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
