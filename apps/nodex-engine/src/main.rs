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
    decode_pump_fun_create, run_logs_stream, start_market_monitor, MarketMonitor, SolanaRpc,
    StreamStatus,
};
use nodex_trading::paper_wallet::{PaperWallet, LAMPORTS_PER_SOL};
use serde::{Deserialize, Serialize};
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
use tokio::sync::{broadcast, watch, Mutex, RwLock};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing_subscriber::EnvFilter;

// Temporary operator key for the current paper-trading deployment.
const NODEX_ADMIN_API_KEY: &str = "n80JHBorD0oFUhPQtpZrwEV4K4o9EyLw";

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
    trading_settings: Arc<RwLock<TradingSettings>>,
    runtime_enabled: watch::Sender<bool>,
    market_monitor: Arc<RwLock<Option<MarketMonitor>>>,
    solana_ws_urls: Vec<String>,
    ws_tickets: Arc<Mutex<HashMap<String, Instant>>>,
    token_registry: TokenRegistry,
    profile: Arc<RwLock<serde_json::Value>>,
    portfolio_positions: Arc<RwLock<Vec<serde_json::Value>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
struct TradingSettings {
    min_liquidity_usd: f64,
    max_entry_price_usd: f64,
    max_investment_per_token_sol: f64,
    max_investment_per_token_usd: f64,
    max_simultaneous_positions: u32,
    min_safety_score: u8,
    max_slippage_pct: f64,
    stop_loss_pct: f64,
    take_profit_pct: f64,
    min_5m_buy_volume_usd: f64,
    max_top10_holder_concentration_pct: f64,
    auto_buy_enabled: bool,
    trailing_stop_enabled: bool,
    trailing_stop_callback_pct: f64,
    blacklist_creators_with_prior_rugs: bool,
}

impl Default for TradingSettings {
    fn default() -> Self {
        Self {
            min_liquidity_usd: 5_000.0,
            max_entry_price_usd: 0.0,
            max_investment_per_token_sol: 0.1,
            max_investment_per_token_usd: 0.0,
            max_simultaneous_positions: 3,
            min_safety_score: 80,
            max_slippage_pct: 10.0,
            stop_loss_pct: 20.0,
            take_profit_pct: 50.0,
            min_5m_buy_volume_usd: 1_000.0,
            max_top10_holder_concentration_pct: 35.0,
            auto_buy_enabled: false,
            trailing_stop_enabled: false,
            trailing_stop_callback_pct: 10.0,
            blacklist_creators_with_prior_rugs: true,
        }
    }
}

impl TradingSettings {
    fn validate(&self) -> Result<(), String> {
        let finite_non_negative = [
            ("min_liquidity_usd", self.min_liquidity_usd),
            ("max_entry_price_usd", self.max_entry_price_usd),
            (
                "max_investment_per_token_sol",
                self.max_investment_per_token_sol,
            ),
            (
                "max_investment_per_token_usd",
                self.max_investment_per_token_usd,
            ),
            ("max_slippage_pct", self.max_slippage_pct),
            ("stop_loss_pct", self.stop_loss_pct),
            ("take_profit_pct", self.take_profit_pct),
            ("min_5m_buy_volume_usd", self.min_5m_buy_volume_usd),
            (
                "max_top10_holder_concentration_pct",
                self.max_top10_holder_concentration_pct,
            ),
            (
                "trailing_stop_callback_pct",
                self.trailing_stop_callback_pct,
            ),
        ];
        for (name, value) in finite_non_negative {
            if !value.is_finite() || value < 0.0 {
                return Err(format!("{name} must be a finite, non-negative number."));
            }
        }
        if self.stop_loss_pct > 100.0 || self.trailing_stop_callback_pct > 100.0 {
            return Err(
                "Stop-loss and trailing-stop percentages must be between 0 and 100.".into(),
            );
        }
        if self.take_profit_pct > 1_000.0 {
            return Err("take_profit_pct must not exceed 1000.".into());
        }
        if self.max_slippage_pct > 100.0 || self.max_top10_holder_concentration_pct > 100.0 {
            return Err("Slippage and top-10 concentration must be between 0 and 100.".into());
        }
        if self.min_safety_score > 100 {
            return Err("min_safety_score must be between 0 and 100.".into());
        }
        if self.max_investment_per_token_sol <= 0.0 && self.max_investment_per_token_usd <= 0.0 {
            return Err("Set a positive per-token paper position size in SOL or USD.".into());
        }
        if self.max_simultaneous_positions == 0 || self.max_simultaneous_positions > 1_000 {
            return Err("max_simultaneous_positions must be between 1 and 1000.".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
struct TradingModeAvailability {
    paper: bool,
    real: bool,
}

#[derive(Debug, Clone, Serialize)]
struct EngineControl {
    paused: bool,
    mode: String,
    available_modes: TradingModeAvailability,
    real_trading_available: bool,
    token_discovery_enabled: bool,
    new_entries_enabled: bool,
    position_monitoring_enabled: bool,
}

#[derive(Debug, Deserialize)]
struct EngineControlRequest {
    command: String,
    mode: Option<String>,
    token_discovery_enabled: Option<bool>,
    new_entries_enabled: Option<bool>,
    position_monitoring_enabled: Option<bool>,
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

/// Projects independent worker events into the canonical shared token state.
/// Workers remain decoupled: they publish domain events and this projector updates
/// only the matching domain of the token record.
fn start_token_state_projector(
    mut receiver: broadcast::Receiver<serde_json::Value>,
    events: broadcast::Sender<serde_json::Value>,
    database: sqlx::SqlitePool,
    registry: TokenRegistry,
    profile: Arc<RwLock<serde_json::Value>>,
    portfolio_positions: Arc<RwLock<Vec<serde_json::Value>>>,
    control: Arc<RwLock<EngineControl>>,
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
                    // Price ticks are runtime state, not durable history. Persist only
                    // non-price state transitions; Analytics owns chart history separately.
                    if kind != "market_price_update" {
                        if let Err(error) =
                            nodex_persistence::save_token_state(&database, &snapshot).await
                        {
                            tracing::error!(%error, mint_address = mint, "Could not persist projected token state");
                            continue;
                        }
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
                    } else {
                        let _ = events.send(json!({"type":"token_state_updated","data":snapshot}));
                    }
                    if kind == "market_price_update"
                        && control.read().await.position_monitoring_enabled
                    {
                        if let Some(price_sol) =
                            data.get("price_sol").and_then(serde_json::Value::as_f64)
                        {
                            // Mark open positions in memory. Never read/write SQLite for every tick.
                            let mut positions = portfolio_positions.write().await;
                            for position in positions.iter_mut().filter(|position| {
                                position.get("mint_address").and_then(serde_json::Value::as_str) == Some(mint)
                                    && position.get("status").and_then(serde_json::Value::as_str) == Some("open")
                            }) {
                                let quantity = position.get("token_quantity")
                                    .or_else(|| position.get("quantity"))
                                    .and_then(serde_json::Value::as_f64).unwrap_or(0.0);
                                let cost_basis = position.get("cost_basis_sol")
                                    .or_else(|| position.get("invested_sol"))
                                    .and_then(serde_json::Value::as_f64).unwrap_or(0.0);
                                let current_value = quantity * price_sol;
                                let pnl = current_value - cost_basis;
                                if let Some(object) = position.as_object_mut() {
                                    object.insert("current_price_sol".into(), json!(price_sol));
                                    object.insert("current_value_sol".into(), json!(current_value));
                                    object.insert("unrealized_pnl_sol".into(), json!(pnl));
                                    object.insert("unrealized_pnl_pct".into(), json!(
                                        if cost_basis > 0.0 { pnl / cost_basis * 100.0 } else { 0.0 }
                                    ));
                                    object.insert("marked_at".into(), json!(chrono::Utc::now().to_rfc3339()));
                                }
                                let _ = events.send(json!({"type":"position_updated","data":position.clone()}));
                            }
                            let open: Vec<serde_json::Value> = positions.iter()
                                .filter(|item| item.get("status").and_then(serde_json::Value::as_str) == Some("open"))
                                .cloned().collect();
                            let sum = |items: &[serde_json::Value], field: &str| -> f64 {
                                items.iter().filter_map(|item| item.get(field).and_then(serde_json::Value::as_f64)).sum()
                            };
                            let portfolio = json!({
                                "total_value_sol": sum(&open, "current_value_sol"),
                                "total_invested_sol": sum(&open, "cost_basis_sol"),
                                "unrealized_pnl_sol": sum(&open, "unrealized_pnl_sol"),
                                "realized_pnl_sol": sum(&positions, "realized_pnl_sol"),
                                "positions": open,
                                "position_count": open.len()
                            });
                            drop(positions);
                            let mut current_profile = profile.write().await;
                            if let Some(object) = current_profile.as_object_mut() {
                                object.insert("portfolio".into(), portfolio.clone());
                            }
                            let profile_snapshot = current_profile.clone();
                            drop(current_profile);
                            let _ = events.send(json!({"type":"profile_updated","data":profile_snapshot}));
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
    market_monitor: Arc<RwLock<Option<MarketMonitor>>>,
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
                            let scan_result = match tokio::time::timeout(
                                Duration::from_secs(45),
                                scan_rpc.scan_token_security_with_progress(
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
                                ),
                            ).await {
                                Ok(Ok(report)) => Ok(report),
                                Ok(Err(error)) => Err(error.to_string()),
                                Err(_) => Err("Security scan timed out after 45 seconds".to_string()),
                            };
                            match scan_result {
                                Ok(mut report) => {
                                    // Metadata is an independent operation: it must never keep
                                    // the security scan in a scanning state.
                                    if report.metadata.name.is_none() {
                                        let _ = scan_events.send(json!({"type":"token_metadata_started","data":{
                                            "mint_address":scan_mint,
                                            "provider":"metaplex",
                                            "received_at":chrono::Utc::now().to_rfc3339()
                                        }}));
                                        match tokio::time::timeout(
                                            Duration::from_secs(8),
                                            scan_rpc.resolve_metaplex_metadata(&scan_mint),
                                        ).await {
                                            Ok(Ok(Some(metadata))) => {
                                                report.metadata = metadata;
                                            }
                                            Ok(Ok(None)) => {
                                                let _ = scan_events.send(json!({"type":"token_metadata_failed","data":{
                                                    "mint_address":scan_mint,
                                                    "provider":"metaplex",
                                                    "reason":"No Metaplex metadata account was found.",
                                                    "status":"not_found",
                                                    "received_at":chrono::Utc::now().to_rfc3339()
                                                }}));
                                            }
                                            Ok(Err(error)) => {
                                                let message = error.to_string();
                                                tracing::warn!(%message, mint_address=%scan_mint, "Metaplex metadata lookup failed");
                                                let _ = scan_events.send(json!({"type":"token_metadata_failed","data":{
                                                    "mint_address":scan_mint,
                                                    "provider":"metaplex",
                                                    "reason":message,
                                                    "status":"failed",
                                                    "received_at":chrono::Utc::now().to_rfc3339()
                                                }}));
                                                let _ = scan_events.send(json!({"type":"diagnostic","data":{
                                                    "severity":"warning","component":"token_metadata_resolver",
                                                    "message":message,"mint_address":scan_mint,
                                                    "received_at":chrono::Utc::now().to_rfc3339()
                                                }}));
                                            }
                                            Err(_) => {
                                                let message = "Metaplex metadata lookup timed out after 8 seconds".to_string();
                                                let _ = scan_events.send(json!({"type":"token_metadata_failed","data":{
                                                    "mint_address":scan_mint,
                                                    "provider":"metaplex",
                                                    "reason":message,
                                                    "status":"timeout",
                                                    "received_at":chrono::Utc::now().to_rfc3339()
                                                }}));
                                                let _ = scan_events.send(json!({"type":"diagnostic","data":{
                                                    "severity":"warning","component":"token_metadata_resolver",
                                                    "message":message,"mint_address":scan_mint,
                                                    "received_at":chrono::Utc::now().to_rfc3339()
                                                }}));
                                            }
                                        }
                                    }
                                    let mut identity_patch = serde_json::Map::new();
                                    if let Some(name) = report.metadata.name.as_ref() {
                                        identity_patch.insert("name".into(), json!(name));
                                    }
                                    if let Some(symbol) = report.metadata.symbol.as_ref() {
                                        identity_patch.insert("symbol".into(), json!(symbol));
                                    }
                                    if let Some(uri) = report.metadata.uri.as_ref() {
                                        identity_patch.insert("metadata_uri".into(), json!(uri));
                                    }
                                    if let Some(decimals) = report.decimals {
                                        identity_patch.insert("decimals".into(), json!(decimals));
                                    }
                                    if let Some(supply) = report.supply_raw.as_ref() {
                                        identity_patch
                                            .insert("total_supply_raw".into(), json!(supply));
                                    }
                                    if !identity_patch.is_empty() {
                                        match approved_registry.upsert_patch(
                                            &scan_mint,
                                            json!({"identity":identity_patch}),
                                        ) {
                                            Ok(snapshot) => {
                                                if let Err(error) =
                                                    nodex_persistence::save_token_state(
                                                        &approved_database,
                                                        &snapshot,
                                                    )
                                                    .await
                                                {
                                                    tracing::warn!(%error, mint_address=%scan_mint, "Could not persist resolved token identity");
                                                }
                                                let _ = scan_events.send(json!({
                                                    "type":"token_metadata_updated",
                                                    "data":{
                                                        "mint_address":scan_mint,
                                                        "name":report.metadata.name,
                                                        "symbol":report.metadata.symbol,
                                                        "metadata_uri":report.metadata.uri,
                                                        "decimals":report.decimals,
                                                        "total_supply_raw":report.supply_raw,
                                                        "source":report.metadata.source,
                                                        "status":report.metadata.status
                                                    }
                                                }));
                                            }
                                            Err(error) => {
                                                tracing::warn!(%error, mint_address=%scan_mint, "Could not update canonical token identity")
                                            }
                                        }
                                    }
                                    let decision = report.decision.clone();
                                    let mut terminal_status = match decision.as_str() {
                                        "REJECTED" => "rejected",
                                        "APPROVED" => "completed",
                                        _ => "unverified",
                                    };
                                    match decision.as_str() {
                                        "APPROVED" => {
                                            let approved_state = approved_registry.upsert_patch(
                                                &scan_mint,
                                                json!({"security":report.clone()}),
                                            );
                                            let approval_persisted = match approved_state {
                                                Ok(snapshot) => {
                                                    match nodex_persistence::save_token_state(
                                                        &approved_database,
                                                        &snapshot,
                                                    )
                                                    .await
                                                    {
                                                        Ok(()) => true,
                                                        Err(error) => {
                                                            let _ = scan_events.send(json!({"type":"diagnostic","data":{
                                                                "severity":"error","component":"token_approval_persistence",
                                                                "message":error.to_string(),"mint_address":scan_mint
                                                            }}));
                                                            false
                                                        }
                                                    }
                                                }
                                                Err(error) => {
                                                    let _ = scan_events.send(json!({"type":"diagnostic","data":{
                                                        "severity":"error","component":"token_approval_registry",
                                                        "message":error,"mint_address":scan_mint
                                                    }}));
                                                    false
                                                }
                                            };
                                            if approval_persisted {
                                                // Publish APPROVED only after the canonical state is durable,
                                                // so a frontend subscription cannot race the approval gate.
                                                let _ = scan_events.send(json!({
                                                    "type":"token_scan_update",
                                                    "data":report.clone()
                                                }));
                                            } else {
                                                terminal_status = "unverified";
                                                approved_registry.remove(&scan_mint);
                                                let _ = scan_events.send(json!({
                                                    "type":"token_approval_pending",
                                                    "data":{
                                                        "mint_address":scan_mint,
                                                        "decision":"UNVERIFIED",
                                                        "reason":"Security checks passed, but the approved token state could not be persisted. Activation is blocked."
                                                    }
                                                }));
                                            }
                                        }
                                        "REJECTED" => {
                                            let rejected_state = approved_registry.upsert_patch(
                                                &scan_mint,
                                                json!({"security":report.clone()}),
                                            );
                                            if let Ok(snapshot) = rejected_state {
                                                if let Err(error) =
                                                    nodex_persistence::save_token_state(
                                                        &approved_database,
                                                        &snapshot,
                                                    )
                                                    .await
                                                {
                                                    let _ = scan_events.send(json!({"type":"diagnostic","data":{
                                                        "severity":"error","component":"token_scan_persistence",
                                                        "message":error.to_string(),"mint_address":scan_mint
                                                    }}));
                                                }
                                            }
                                            let _ = scan_events.send(json!({
                                                "type":"token_scan_update",
                                                "data":report.clone()
                                            }));
                                            let _ = scan_events.send(
                                                json!({"type":"token_approval_pending","data":{
                                                    "mint_address":scan_mint,
                                                    "decision":"REJECTED",
                                                    "reason":report.decision_reason
                                                }}),
                                            );
                                        }
                                        _ => {
                                            let _ = scan_events.send(json!({
                                                "type":"token_scan_update",
                                                "data":report.clone()
                                            }));
                                            let _ = scan_events.send(json!({"type":"token_approval_pending","data":{
                                                "mint_address":scan_mint,
                                                "decision":decision,
                                                "reason":"Token remains isolated until an explicit APPROVED decision is produced."
                                            }}));
                                        }
                                    }
                                    let _ = scan_events.send(json!({"type":"token_scan_completed","data":{
                                        "mint_address":scan_mint,
                                        "decision":decision,
                                        "status":terminal_status,
                                        "progress":100,
                                        "message":"Security scan finished.",
                                        "received_at":chrono::Utc::now().to_rfc3339()
                                    }}));
                                }
                                Err(error) => {
                                    let _ = scan_events.send(json!({"type":"diagnostic","data":{
                                        "severity":"error","component":"token_security_scanner",
                                        "message":error,"mint_address":scan_mint,
                                        "received_at":chrono::Utc::now().to_rfc3339()
                                    }}));
                                    let _ = scan_events.send(json!({"type":"token_scan_failed","data":{
                                        "mint_address":scan_mint,
                                        "decision":"UNVERIFIED",
                                        "decision_reason":error,
                                        "status":"failed",
                                        "received_at":chrono::Utc::now().to_rfc3339()
                                    }}));
                                    let _ = scan_events.send(json!({"type":"token_scan_update","data":{
                                        "mint_address":scan_mint,"decision":"UNVERIFIED",
                                        "decision_reason":error,
                                        "status":"unverified","received_at":chrono::Utc::now().to_rfc3339()
                                    }}));
                                }
                            }
                            // Market observation is independent from trade approval.
                            // Security decisions gate execution, never token visibility or price collection.
                            if let Some(monitor) = approved_monitor.read().await.clone() {
                                match monitor
                                    .subscribe_pump_fun_bonding_curve(
                                        &scan_mint,
                                        &approved_program_id,
                                    )
                                    .await
                                {
                                    Ok(account_address) => {
                                        let _ = scan_events.send(
                                            json!({"type":"market_subscription_requested","data":{
                                                "mint_address":scan_mint,
                                                "account_address":account_address,
                                                "venue":"pump_fun_bonding_curve",
                                                "status":"pending",
                                                "received_at":chrono::Utc::now().to_rfc3339()
                                            }}),
                                        );
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
                        });
                    }
                    Ok(false) => {
                        // Duplicate launch signature: it has already been persisted.
                    }
                    Err(error) => {
                        let _ = events.send(json!({"type":"diagnostic","data":{
                            "severity":"error",
                            "component":"launch_persistence",
                            "message":error.to_string(),
                            "signature":signature,
                            "received_at":chrono::Utc::now().to_rfc3339()
                        }}));
                    }
                }
            }
        });
    }

    // The producer owns the Solana notification stream. It validates the program tag,
    // extracts the confirmed transaction signature/slot, deduplicates, then hands work
    // to the bounded decoder queue. RPC decoding stays in the worker pool above.
    tokio::spawn(async move {
        loop {
            let notification = match receiver.recv().await {
                Ok(value) => value,
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    let _ = events.send(json!({"type":"diagnostic","data":{
                        "severity":"warning",
                        "component":"pump_fun_launch_queue",
                        "message":"Launch notification consumer lagged; stream backfill may replay missed signatures.",
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
                    != Some(pump_fun_program_id.as_str())
                || {
                    let current = control.read().await;
                    current.paused || !current.token_discovery_enabled
                }
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
                .filter(|signature| !signature.is_empty())
                .map(str::to_owned)
            else {
                continue;
            };
            let slot = notification
                .pointer("/params/result/context/slot")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default();

            {
                let mut seen = seen_signatures.lock().await;
                if !seen.insert(signature.clone()) {
                    continue;
                }
                if seen.len() > 20_000 {
                    // Keep this bounded without retaining an unbounded process-lifetime set.
                    seen.clear();
                    seen.insert(signature.clone());
                }
            }

            if let Err(error) = work_tx.try_send((signature.clone(), slot)) {
                // Do not permanently suppress a signature that could not enter the queue.
                seen_signatures.lock().await.remove(&signature);
                let _ = events.send(json!({"type":"diagnostic","data":{
                    "severity":"warning",
                    "component":"pump_fun_launch_queue",
                    "message":format!("Could not enqueue launch transaction: {error}"),
                    "signature":signature,
                    "queue_capacity":LAUNCH_QUEUE_CAPACITY,
                    "received_at":chrono::Utc::now().to_rfc3339()
                }}));
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

fn verify_operator_key(state: &AppState, headers: &HeaderMap) -> Result<(), (StatusCode, String)> {
    let expected = state
        .admin_api_key
        .as_deref()
        .filter(|key| !key.is_empty())
        .ok_or((
            StatusCode::SERVICE_UNAVAILABLE,
            "Operator API authentication is unavailable.".to_string(),
        ))?;
    let provided = headers
        .get("x-nodex-admin-key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if provided.is_empty() || provided != expected {
        return Err((
            StatusCode::UNAUTHORIZED,
            "Invalid or missing NodeX operator key.".into(),
        ));
    }
    Ok(())
}

async fn get_trading_config(State(state): State<AppState>) -> Json<serde_json::Value> {
    let settings = state.trading_settings.read().await.clone();
    Json(json!({
        "config": settings,
        "mode": "paper",
        "available_modes": { "paper": true, "real": false },
        "real_trading_available": false,
        "updated_at": chrono::Utc::now().to_rfc3339()
    }))
}

async fn update_trading_config(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(settings): Json<TradingSettings>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    verify_operator_key(&state, &headers)?;
    settings
        .validate()
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let serialized = serde_json::to_string(&settings)
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    sqlx::query(
        "INSERT INTO nodex_runtime_config (config_key, config_json, updated_at) VALUES ('trading', ?, ?) ON CONFLICT(config_key) DO UPDATE SET config_json = excluded.config_json, updated_at = excluded.updated_at"
    )
    .bind(serialized)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(&state.database)
    .await
    .map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Could not persist trading configuration: {error}"),
        )
    })?;
    *state.trading_settings.write().await = settings.clone();
    let _ = state
        .events
        .send(json!({"type":"trading_config_updated","data":settings}));
    Ok(Json(json!({"status":"ok","config":settings})))
}

async fn get_engine_control(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(json!(state.control.read().await.clone()))
}

async fn update_engine_control(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<EngineControlRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    verify_operator_key(&state, &headers)?;
    let mut control = state.control.write().await;
    match request.command.as_str() {
        "start" | "resume" => {
            control.paused = false;
            control.token_discovery_enabled = true;
        }
        "pause" | "stop" => {
            control.paused = true;
            control.token_discovery_enabled = false;
        }
        "configure" => {}
        "set_mode" => match request.mode.as_deref() {
            Some("paper") => control.mode = "paper".into(),
            Some("real") => {
                return Err((
                    StatusCode::CONFLICT,
                    "Real trading is not available. Paper mode remains active.".into(),
                ));
            }
            _ => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "set_mode requires mode to be either 'paper' or 'real'.".into(),
                ));
            }
        },
        other => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("Unsupported engine command: {other}"),
            ))
        }
    }
    if let Some(enabled) = request.token_discovery_enabled {
        control.token_discovery_enabled = enabled;
    }
    if let Some(enabled) = request.new_entries_enabled {
        control.new_entries_enabled = enabled;
    }
    if let Some(enabled) = request.position_monitoring_enabled {
        control.position_monitoring_enabled = enabled;
    }
    let snapshot = control.clone();
    drop(control);
    state.runtime_enabled.send_replace(!snapshot.paused);
    if !snapshot.paused {
        ensure_market_monitor(&state).await;
    }
    let _ = state
        .events
        .send(json!({"type":"engine_state","data":snapshot}));
    Ok(Json(json!({"status":"ok","engine":snapshot})))
}

async fn engine_status(State(state): State<AppState>) -> Json<serde_json::Value> {
    let stream = state.stream_status.read().await.clone();
    let pumpswap_stream = state.pumpswap_status.read().await.clone();
    let control = state.control.read().await.clone();
    let trading_settings = state.trading_settings.read().await.clone();
    Json(json!({
        "mode": control.mode,
        "paused": control.paused,
        "token_discovery_enabled": control.token_discovery_enabled,
        "new_entries_enabled": control.new_entries_enabled,
        "position_monitoring_enabled": control.position_monitoring_enabled,
        "trading_config": trading_settings,
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
            "Maintenance endpoint authentication is unavailable.".to_string(),
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

async fn build_approved_token_detail_snapshot(
    state: &AppState,
    mint: &str,
) -> Result<Option<serde_json::Value>, String> {
    // Detail snapshots are read-only market/identity data. Approval is a
    // separate safety gate for execution and must not hide discovered tokens.
    let Some(token_state) = state.token_registry.get(mint) else {
        return Ok(None);
    };

    let positions = nodex_persistence::load_portfolio_positions(&state.database)
        .await
        .map_err(|error| error.to_string())?;
    let mut detail =
        token_details::build_token_detail_snapshot(mint, Some(&token_state), &positions);

    let chart_candles = match nodex_persistence::load_chart_candles(
        &state.database,
        mint,
        "5m",
        500,
    )
    .await
    {
        Ok(candles) => candles,
        Err(error) => {
            tracing::warn!(%error, mint_address = mint, "Could not load persisted token chart candles");
            Vec::new()
        }
    };
    detail["chart"] = json!({
        "status": if chart_candles.is_empty() { "unavailable" } else { "available" },
        "timeframe": "5m",
        "candles": chart_candles,
        "message": if chart_candles.is_empty() {
            "Historical candle data is not available yet"
        } else {
            ""
        }
    });
    Ok(Some(detail))
}

async fn token_state(
    State(state): State<AppState>,
    Path(mint): Path<String>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    match build_approved_token_detail_snapshot(&state, &mint).await {
        Ok(Some(snapshot)) => Ok(Json(snapshot)),
        Ok(None) => Err((
            axum::http::StatusCode::NOT_FOUND,
            "Approved token state not found".to_string(),
        )),
        Err(error) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, error)),
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
    let positions = state.portfolio_positions.read().await.clone();
    let mut profile = state.profile.read().await.clone();
    let open_positions: Vec<serde_json::Value> = positions.iter()
        .filter(|position| position.get("status").and_then(serde_json::Value::as_str) == Some("open"))
        .cloned().collect();
    let sum_field = |items: &[serde_json::Value], field: &str| -> f64 {
        items.iter().filter_map(|item| item.get(field).and_then(serde_json::Value::as_f64)).sum()
    };
    let portfolio = json!({
        "total_value_sol": sum_field(&open_positions, "current_value_sol"),
        "total_invested_sol": sum_field(&open_positions, "cost_basis_sol"),
        "unrealized_pnl_sol": sum_field(&open_positions, "unrealized_pnl_sol"),
        "realized_pnl_sol": sum_field(&positions, "realized_pnl_sol"),
        "positions": open_positions,
        "position_count": open_positions.len()
    });
    if let Some(object) = profile.as_object_mut() {
        object.insert("portfolio".into(), portfolio);
    }
    Json(profile)
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

// Only curated application events are allowed onto the browser WebSocket.
// Raw Solana notifications, provider status chatter, diagnostics, and pool-level
// activity remain internal to the engine and its processors.
fn is_token_detail_event(event_type: &str) -> bool {
    matches!(
        event_type,
        "token_market_update"
            | "token_statistics_update"
            | "chart_candle_update"
            | "chart_candle_closed"
            | "position_updated"
    )
}

fn is_frontend_event(value: &serde_json::Value) -> bool {
    matches!(
        value.get("type").and_then(serde_json::Value::as_str),
        Some("connection_status")
            | Some("engine_state")
            | Some("trading_config_updated")
            | Some("launch_detected")
            | Some("token_removed")
            | Some("token_approval_pending")
            | Some("token_scan_started")
            | Some("token_scan_update")
            | Some("token_scan_progress")
            | Some("token_scan_completed")
            | Some("token_scan_failed")
            | Some("token_metadata_started")
            | Some("token_metadata_failed")
            | Some("token_metadata_updated")
            | Some("token_state_updated")
            | Some("profile_updated")
            | Some("market_price_update")
            | Some("token_market_update")
            | Some("token_statistics_update")
            | Some("chart_candle_update")
            | Some("chart_candle_closed")
            | Some("position_updated")
            | Some("portfolio_summary_updated")
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
                    if event_type == "token_state_updated" || is_token_detail_event(event_type) {
                        let mint = value.pointer("/data/mint_address")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default();
                        if !subscribed_tokens.contains(mint) {
                            continue;
                        }
                    } else if event_type == "market_price_update" {
                        // The global stream receives only compact ticks for approved tokens.
                        let Some(data) = value.get("data").cloned() else { continue };
                        let mint = data.get("mint_address").and_then(serde_json::Value::as_str).unwrap_or_default();
                        if mint.is_empty() {
                            continue;
                        }
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
                            let mut control = state.control.write().await;
                            control.paused = true;
                            control.token_discovery_enabled = false;
                            let snapshot = control.clone();
                            drop(control);
                            state.runtime_enabled.send_replace(false);
                            Some(json!({"type":"engine_state","data":snapshot}))
                        }
                        Ok(ClientCommand::Resume) | Ok(ClientCommand::Start) => {
                            let mut control = state.control.write().await;
                            control.paused = false;
                            control.token_discovery_enabled = true;
                            let snapshot = control.clone();
                            drop(control);
                            state.runtime_enabled.send_replace(true);
                            ensure_market_monitor(&state).await;
                            Some(json!({"type":"engine_state","data":snapshot}))
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
                                match build_approved_token_detail_snapshot(&state, &mint).await {
                                    Ok(Some(snapshot)) => {
                                        subscribed_tokens.insert(mint.clone());
                                        Some(json!({
                                            "type":"token_detail_snapshot",
                                            "data":{"subscription":"active","mint_address":mint,"snapshot":snapshot}
                                        }))
                                    }
                                    Ok(None) => {
                                        subscribed_tokens.remove(&mint);
                                        Some(json!({"type":"command_rejected","data":{
                                            "command":"subscribe_token",
                                            "mint":mint,
                                            "reason":"Token is not present in the approved token registry."
                                        }}))
                                    }
                                    Err(error) => {
                                        subscribed_tokens.remove(&mint);
                                        Some(json!({"type":"command_rejected","data":{
                                            "command":"subscribe_token",
                                            "mint":mint,
                                            "reason":format!("Could not assemble token snapshot: {error}")
                                        }}))
                                    }
                                }
                            }
                        }
                        Ok(ClientCommand::SetChartTimeframe { mint, timeframe }) => {
                            let mint = mint.trim().to_owned();
                            let timeframe = timeframe.trim().to_owned();
                            let valid_timeframe = matches!(
                                timeframe.as_str(),
                                "1m" | "5m" | "15m" | "1h" | "24h"
                            );
                            if !valid_timeframe || !subscribed_tokens.contains(&mint) {
                                Some(json!({"type":"command_rejected","data":{
                                    "command":"set_chart_timeframe",
                                    "mint_address":mint,
                                    "timeframe":timeframe,
                                    "reason":"Subscribe to a discovered token and request a supported chart timeframe."
                                }}))
                            } else {
                                match nodex_persistence::load_chart_candles(
                                    &state.database,
                                    &mint,
                                    &timeframe,
                                    2_000,
                                ).await {
                                    Ok(candles) => Some(json!({
                                        "type":"chart_history_snapshot",
                                        "data":{
                                            "mint_address":mint,
                                            "timeframe":timeframe,
                                            "candles":candles,
                                            "received_at":chrono::Utc::now().to_rfc3339()
                                        }
                                    })),
                                    Err(error) => Some(json!({"type":"command_rejected","data":{
                                        "command":"set_chart_timeframe",
                                        "mint_address":mint,
                                        "timeframe":timeframe,
                                        "reason":format!("Could not load chart history: {error}")
                                    }}))
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

async fn ensure_market_monitor(state: &AppState) {
    let mut monitor = state.market_monitor.write().await;
    if monitor.is_none() && !state.solana_ws_urls.is_empty() {
        *monitor = Some(start_market_monitor(
            state.solana_ws_urls.clone(),
            state.events.clone(),
        ));
    }
}

fn spawn_controlled_log_stream(
    ws_urls: Vec<String>,
    program_id: String,
    events: broadcast::Sender<serde_json::Value>,
    status: Arc<RwLock<StreamStatus>>,
    recovery_rpc: Option<SolanaRpc>,
    mut runtime_enabled: watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        loop {
            while !*runtime_enabled.borrow() {
                if runtime_enabled.changed().await.is_err() {
                    return;
                }
            }
            let stream = run_logs_stream(
                ws_urls.clone(),
                program_id.clone(),
                events.clone(),
                status.clone(),
                recovery_rpc.clone(),
            );
            tokio::pin!(stream);
            tokio::select! {
                _ = &mut stream => {}
                changed = runtime_enabled.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    if !*runtime_enabled.borrow() {
                        *status.write().await = StreamStatus::default();
                    }
                }
            }
        }
    });
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
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS nodex_runtime_config (config_key TEXT PRIMARY KEY, config_json TEXT NOT NULL, updated_at TEXT NOT NULL)"
    )
    .execute(&db)
    .await?;
    let stored_trading_config: Option<String> = sqlx::query_scalar(
        "SELECT config_json FROM nodex_runtime_config WHERE config_key = 'trading'",
    )
    .fetch_optional(&db)
    .await?;
    let trading_settings = match stored_trading_config {
        Some(raw) => serde_json::from_str::<TradingSettings>(&raw)
            .ok()
            .filter(|settings| settings.validate().is_ok())
            .unwrap_or_default(),
        None => TradingSettings::default(),
    };
    let trading_settings_json = serde_json::to_string(&trading_settings)?;
    sqlx::query(
        "INSERT INTO nodex_runtime_config (config_key, config_json, updated_at) VALUES ('trading', ?, ?) ON CONFLICT(config_key) DO NOTHING"
    )
    .bind(trading_settings_json)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(&db)
    .await?;
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
    let portfolio_positions = Arc::new(RwLock::new(
        nodex_persistence::load_portfolio_positions(&db).await?,
    ));
    let (events, _) = broadcast::channel(8192);
    if let (Some(rpc_client), Some(address)) = (
        (!config.solana_http_urls.is_empty())
            .then(|| SolanaRpc::new_with_urls(config.solana_http_urls.clone())),
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
                            let _ = events_for_sync
                                .send(json!({"type":"profile_updated","data":snapshot}));
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

    // The process starts online but idle. An explicit operator Start/Resume command
    // is required before launch processors accept discovery work.
    let control = Arc::new(RwLock::new(EngineControl {
        paused: true,
        mode: "paper".into(),
        available_modes: TradingModeAvailability {
            paper: true,
            real: false,
        },
        real_trading_available: false,
        token_discovery_enabled: false,
        new_entries_enabled: false,
        position_monitoring_enabled: true,
    }));

    // Keep canonical token state synchronized from independent worker events.
    start_token_state_projector(
        events.subscribe(),
        events.clone(),
        db.clone(),
        token_registry.clone(),
        profile_state.clone(),
        portfolio_positions.clone(),
        control.clone(),
    );
    let stream_status = Arc::new(RwLock::new(StreamStatus::default()));
    let rpc = (!config.solana_http_urls.is_empty())
        .then(|| SolanaRpc::new_with_urls(config.solana_http_urls.clone()));
    // Keep launch detection and market monitoring on separate subscriptions.
    // The market monitor multiplexes all watched bonding curves over one WebSocket.
    let market_monitor = Arc::new(RwLock::new(None));
    let (runtime_enabled, runtime_receiver) = watch::channel(false);

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

    // PumpSwap activity/volume analytics are intentionally outside the slim core engine.
    // Keep the compatibility status field idle until the separate analytics service owns it.
    let pumpswap_status = Arc::new(RwLock::new(StreamStatus::default()));

    // Raw upstream JSON notifications are transient and are not written to SQLite.

    if !config.solana_ws_urls.is_empty() {
        spawn_controlled_log_stream(
            config.solana_ws_urls.clone(),
            config.pump_fun_program_id.clone(),
            events.clone(),
            stream_status.clone(),
            rpc.clone(),
            runtime_receiver.clone(),
        );
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
        admin_api_key: Some(NODEX_ADMIN_API_KEY.to_owned()),
        trading_settings: Arc::new(RwLock::new(trading_settings)),
        runtime_enabled,
        market_monitor,
        solana_ws_urls: config.solana_ws_urls.clone(),
        ws_tickets: Arc::new(Mutex::new(HashMap::new())),
        token_registry,
        profile: profile_state,
        portfolio_positions,
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/engine/status", get(engine_status))
        .route(
            "/api/v1/engine/control",
            get(get_engine_control).post(update_engine_control),
        )
        .route(
            "/api/v1/config/trading",
            get(get_trading_config).put(update_trading_config),
        )
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
