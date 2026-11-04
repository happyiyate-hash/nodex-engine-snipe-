use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use launch_detector::{detect_pump_fun_launch, CreateInstruction};
use nodex_core::config::EngineConfig;
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
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};
use tokio::sync::{broadcast, Mutex, RwLock, Semaphore};
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct AppState {
    events: broadcast::Sender<serde_json::Value>,
    stream_status: Arc<RwLock<StreamStatus>>,
    wallet: Arc<RwLock<PaperWallet>>,
    rpc: Option<SolanaRpc>,
    database: sqlx::SqlitePool,
    control: Arc<RwLock<EngineControl>>,
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

fn start_launch_processor(
    mut receiver: broadcast::Receiver<serde_json::Value>,
    events: broadcast::Sender<serde_json::Value>,
    rpc: SolanaRpc,
    database: sqlx::SqlitePool,
    pump_fun_program_id: String,
    control: Arc<RwLock<EngineControl>>,
    market_monitor: Option<MarketMonitor>,
) {
    let seen_signatures = Arc::new(Mutex::new(HashSet::<String>::new()));
    let concurrency = Arc::new(Semaphore::new(4));
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
            let permit = match concurrency.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => break,
            };
            let rpc = rpc.clone();
            let events = events.clone();
            let database = database.clone();
            let program_id = pump_fun_program_id.clone();
            let control = control.clone();
            let market_monitor = market_monitor.clone();
            tokio::spawn(async move {
                if control.read().await.paused {
                    return;
                }
                let _permit = permit;
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
                    return;
                };
                let decoded = match decode_pump_fun_create(&transaction, &signature, &program_id) {
                    Ok(Some(decoded)) => decoded,
                    Ok(None) => return,
                    Err(error) => {
                        let _ = events.send(json!({"type":"diagnostic","data":{
                            "severity":"error","component":"pump_fun_decoder",
                            "message":error.to_string(),"signature":signature,
                            "received_at":chrono::Utc::now().to_rfc3339()
                        }}));
                        return;
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
                let Some(launch) = launch else { return };
                match nodex_persistence::save_launch(&database, &launch).await {
                    Ok(true) => {
                        let mint_address = launch.mint_address.clone();
                        let creator_address = launch.creator_address.clone();
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
                        if let Some(creator) = creator_address {
                            let _ = events.send(json!({"type":"creator_history_started","data":{"mint_address":mint_address,"creator_address":creator,"status":"scanning","received_at":chrono::Utc::now().to_rfc3339()}}));
                            match rpc
                                .scan_creator_history(&creator, &signature, &program_id)
                                .await
                            {
                                Ok(history) => {
                                    let _ = events.send(json!({"type":"creator_history_update","data":{"mint_address":mint_address,"history":history,"received_at":chrono::Utc::now().to_rfc3339()}}));
                                }
                                Err(error) => {
                                    let _ = events.send(json!({"type":"diagnostic","data":{"severity":"warning","component":"creator_history_scanner","message":error.to_string(),"mint_address":mint_address,"creator_address":creator,"received_at":chrono::Utc::now().to_rfc3339()}}));
                                    let _ = events.send(json!({"type":"creator_history_update","data":{"mint_address":mint_address,"history":{"creator_address":creator,"status":"unverified","early_selling":"UNVERIFIED","sample_is_partial":true,"caveat":"Creator history RPC failed; no conclusion can be drawn."},"received_at":chrono::Utc::now().to_rfc3339()}}));
                                }
                            }
                        }
                        match rpc.scan_token_security(&mint_address).await {
                            Ok(report) => {
                                let _ =
                                    events.send(json!({"type":"token_scan_update","data":report}));
                            }
                            Err(error) => {
                                let _ = events.send(json!({"type":"diagnostic","data":{
                                    "severity":"error","component":"token_security_scanner",
                                    "message":error.to_string(),"mint_address":mint_address,
                                    "received_at":chrono::Utc::now().to_rfc3339()
                                }}));
                                let _ = events.send(json!({"type":"token_scan_update","data":{
                                    "mint_address":mint_address,"decision":"UNVERIFIED",
                                    "decision_reason":"Scanner RPC failed; no automatic buy is permitted.",
                                    "status":"unverified","received_at":chrono::Utc::now().to_rfc3339()
                                }}));
                            }
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
            });
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
    let control = state.control.read().await.clone();
    Json(json!({
        "mode": control.mode,
        "paused": control.paused,
        "live_execution_enabled": false,
        "live_execution_reason": "Live signing and transaction submission are not implemented; the backend refuses live mode.",
        "solana_stream": stream,
    }))
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

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| client_socket(socket, state))
}

async fn client_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver_socket) = socket.split();
    let mut receiver = state.events.subscribe();
    let status = state.stream_status.read().await.clone();
    let control = state.control.read().await.clone();
    let initial = json!({"type":"connection_status","data":{"solana_stream":status,"engine":control,"capabilities":{"paper_trading":true,"live_trading":false,"token_scanning":state.rpc.is_some() && status.connected,"token_security_scanning":false,"transaction_decoding":"pump_fun_create_and_create_v2","transaction_decoding_enabled":state.rpc.is_some() && status.connected}}}).to_string();
    if sender.send(Message::Text(initial)).await.is_err() {
        return;
    }
    loop {
        tokio::select! {
            event = receiver.recv() => match event {
                Ok(value) => {
                    let paused = state.control.read().await.paused;
                    let is_market_stream = value.get("method").and_then(|v| v.as_str()) == Some("logsNotification");
                    if paused && is_market_stream {
                        continue;
                    }
                    if sender.send(Message::Text(value.to_string())).await.is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    let warning = json!({
                        "type": "stream_warning",
                        "data": {"dropped_events": count}
                    }).to_string();
                    if sender.send(Message::Text(warning)).await.is_err() {
                        break;
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
    let wallet = match nodex_persistence::load_paper_wallet(&db).await? {
        Some(wallet) => wallet,
        None => {
            let wallet = PaperWallet::new(config.paper_starting_balance_lamports);
            nodex_persistence::save_paper_wallet(&db, &wallet).await?;
            wallet
        }
    };

    let (events, _) = broadcast::channel(2048);
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

    if let Some(ws_url) = config.solana_ws_url.clone() {
        let sender = events.clone();
        let status = Arc::new(RwLock::new(StreamStatus::default()));
        tokio::spawn(async move {
            run_logs_stream(ws_url, PUMPSWAP_PROGRAM_ID.to_owned(), sender, status).await;
        });
    }

    // Persist raw upstream log notifications independently of browser subscribers.
    let mut persistence_receiver = events.subscribe();
    let persistence_db = db.clone();
    tokio::spawn(async move {
        while let Ok(event) = persistence_receiver.recv().await {
            if let Err(error) =
                nodex_persistence::save_raw_solana_event(&persistence_db, &event).await
            {
                tracing::error!(%error, "Could not persist raw Solana event");
            }
        }
    });

    if let Some(ws_url) = config.solana_ws_url.clone() {
        let sender = events.clone();
        let status = stream_status.clone();
        let program_id = config.pump_fun_program_id.clone();
        tokio::spawn(async move {
            run_logs_stream(ws_url, program_id, sender, status).await;
        });
    } else {
        tracing::warn!("Helius not configured; set HELIUS_API_KEY or SOLANA_RPC_WS_URL");
    }

    let state = AppState {
        events,
        stream_status,
        wallet: Arc::new(RwLock::new(wallet)),
        rpc,
        database: db.clone(),
        control,
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/engine/status", get(engine_status))
        .route("/api/v1/paper-wallet", get(paper_wallet))
        .route("/api/v1/tokens/recent", get(recent_launches))
        .route("/api/v1/tokens/:mint/scan", get(scan_token))
        .route("/api/v1/fees/token-account-rent", get(token_account_rent))
        .route("/api/v1/ws", get(ws_handler))
        .with_state(state)
        .layer(TraceLayer::new_for_http());

    let address: SocketAddr = config.bind_addr.parse()?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!(%address, mode = "paper", "NodeX engine API listening");
    axum::serve(listener, app).await?;
    Ok(())
}
