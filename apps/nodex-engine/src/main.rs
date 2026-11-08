mod storage;
mod token_details;
mod websocket;

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
use tokio::sync::{broadcast, mpsc, watch, Mutex, RwLock};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct AppState {
    events: broadcast::Sender<serde_json::Value>,
    gateway: websocket::Gateway,
    database_changes: tokio::sync::mpsc::Sender<serde_json::Value>,
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
#[allow(clippy::too_many_arguments)]
fn start_token_state_projector(
    mut receiver: broadcast::Receiver<serde_json::Value>,
    events: broadcast::Sender<serde_json::Value>,
    database: sqlx::SqlitePool,
    registry: TokenRegistry,
    profile: Arc<RwLock<serde_json::Value>>,
    portfolio_positions: Arc<RwLock<Vec<serde_json::Value>>>,
    control: Arc<RwLock<EngineControl>>,
    database_changes: mpsc::Sender<serde_json::Value>,
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
            if kind == "token_diagnostic" {
                let mut diagnostics = registry
                    .get(mint)
                    .and_then(|snapshot| {
                        snapshot
                            .data
                            .get("diagnostics")
                            .and_then(serde_json::Value::as_array)
                            .cloned()
                    })
                    .unwrap_or_default();
                diagnostics.push(data.clone());
                if diagnostics.len() > 50 {
                    diagnostics.drain(0..diagnostics.len() - 50);
                }
                match registry.upsert_patch(mint, json!({"diagnostics":diagnostics})) {
                    Ok(snapshot) => {
                        if let Err(error) =
                            nodex_persistence::save_token_state(&database, &snapshot).await
                        {
                            tracing::error!(%error,mint_address=mint,"Could not persist token diagnostic");
                        } else {
                            let _ = database_changes
                                .send(json!({"type":"token_state_updated","data":snapshot}))
                                .await;
                        }
                    }
                    Err(error) => {
                        tracing::error!(%error,mint_address=mint,"Could not append token diagnostic to canonical state")
                    }
                }
                continue;
            }
            // The approved-token persistence path already commits the final scan report.
            // Never let the projector persist rejected/unverified reports a second time.
            if kind == "token_scan_update" {
                continue;
            }
            let transient_scan_event = matches!(kind, "token_scan_started" | "token_scan_progress");
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
                    // Scan progress is transient runtime state. Do not write it to SQLite.
                    if transient_scan_event {
                        continue;
                    }
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
                        // The SQLite reader receives this only after save_token_state succeeded.
                        let _ = database_changes
                            .send(json!({"type":"token_state_updated","data":snapshot}))
                            .await;
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
                                position
                                    .get("mint_address")
                                    .and_then(serde_json::Value::as_str)
                                    == Some(mint)
                                    && position.get("status").and_then(serde_json::Value::as_str)
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
                                    object.insert("current_price_sol".into(), json!(price_sol));
                                    object.insert("current_value_sol".into(), json!(current_value));
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
                                let _ = events.send(
                                    json!({"type":"position_updated","data":position.clone()}),
                                );
                            }
                            let open: Vec<serde_json::Value> = positions
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
                                "realized_pnl_sol": sum(&positions, "realized_pnl_sol"),
                                "positions": open,
                                "position_count": position_count
                            });
                            drop(positions);
                            let mut current_profile = profile.write().await;
                            if let Some(object) = current_profile.as_object_mut() {
                                object.insert("portfolio".into(), portfolio.clone());
                            }
                            let profile_snapshot = current_profile.clone();
                            drop(current_profile);
                            let _ = events
                                .send(json!({"type":"profile_updated","data":profile_snapshot}));
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
#[derive(Debug, Clone)]
struct DiscoveryMetadata {
    name: Option<String>,
    symbol: Option<String>,
    decimals: Option<u8>,
    metadata_uri: Option<String>,
    logo_url: Option<String>,
    status: &'static str,
    errors: Vec<serde_json::Value>,
}

impl DiscoveryMetadata {
    fn partial() -> Self {
        Self {
            name: None,
            symbol: None,
            decimals: None,
            metadata_uri: None,
            logo_url: None,
            status: "unavailable",
            errors: Vec::new(),
        }
    }
}

fn normalize_image_url(candidate: &str) -> Option<String> {
    let candidate = candidate.trim();
    let normalized = if let Some(path) = candidate.strip_prefix("ipfs://") {
        format!("https://ipfs.io/ipfs/{}", path.trim_start_matches("ipfs/"))
    } else if let Some(path) = candidate.strip_prefix("ar://") {
        format!("https://arweave.net/{}", path.trim_start_matches('/'))
    } else {
        candidate.to_owned()
    };
    let url = reqwest::Url::parse(&normalized).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();
    let unsafe_host = host.parse::<std::net::IpAddr>().is_ok()
        || host == "localhost"
        || host.ends_with(".localhost")
        || host.ends_with(".local");
    if url.scheme() != "https" || unsafe_host || !url.username().is_empty() || url.password().is_some() {
        return None;
    }
    Some(url.to_string())
}

fn provider_error_details(message: &str) -> serde_json::Value {
    const PREFIX: &str = "NODEX_PROVIDER_ERROR_JSON:";
    if let Some((_, payload)) = message.split_once(PREFIX) {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(payload.trim()) {
            return value;
        }
    }
    json!({"kind":"application_error","message":message})
}
fn token_diagnostic(
    mint: &str,
    stage: &str,
    provider: &str,
    message: impl Into<String>,
    raw_response: serde_json::Value,
) -> serde_json::Value {
    json!({"mint_address":mint,"stage":stage,"provider":provider,"severity":"error","message":message.into(),"raw_response":raw_response,"occurred_at":chrono::Utc::now().to_rfc3339()})
}

/// Resolve the token's display identity before publishing new_token_detected.
/// Missing optional fields never block discovery. Off-chain metadata is fetched only
/// from HTTPS URLs and redirects are disabled to reduce SSRF and untrusted redirect risk.
async fn resolve_discovery_metadata(
    rpc: &SolanaRpc,
    mint: &str,
    activity_events: &broadcast::Sender<serde_json::Value>,
) -> anyhow::Result<DiscoveryMetadata> {
    let mut result = DiscoveryMetadata::partial();
    match rpc.get_account_data(mint).await {
        Ok(Some((owner, data, _))) if data.len() >= 82 => {
            const SPL_TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
            const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnB9J3q6k7";
            if owner == SPL_TOKEN || owner == TOKEN_2022 {
                result.decimals = Some(data[44]);
            } else {
                result.errors.push(token_diagnostic(
                    mint,
                    "mint_account_metadata",
                    "solana_rpc",
                    "Mint account is owned by an unsupported token program.",
                    json!({"owner":owner,"account_data_length":data.len()}),
                ));
            }
        }
        Ok(Some((owner, data, _))) => result.errors.push(token_diagnostic(
            mint,
            "mint_account_metadata",
            "solana_rpc",
            "Mint account data is shorter than the expected SPL mint layout.",
            json!({"owner":owner,"account_data_length":data.len(),"minimum_expected":82}),
        )),
        Ok(None) => result.errors.push(token_diagnostic(
            mint,
            "mint_account_metadata",
            "solana_rpc",
            "The mint account was not found by the RPC provider.",
            json!({"result":null}),
        )),
        Err(error) => result.errors.push(token_diagnostic(
            mint,
            "mint_account_metadata",
            "solana_rpc",
            error.to_string(),
            provider_error_details(&format!("{error:#}")),
        )),
    }
    // Preserve canonical on-chain metadata as the primary source. Helius DAS fills
    // missing display fields and supplies indexed image links when available.
    match rpc.resolve_token_metadata(mint).await {
        Ok(metadata) => {
            let metadata_json = serde_json::to_value(&metadata)
                .unwrap_or_else(|_| json!({"status":metadata.status}));
            let metadata_caveat = metadata.caveat.clone();
            let metadata_status = metadata.status;
            result.name = metadata.name;
            result.symbol = metadata.symbol;
            result.metadata_uri = metadata.uri;
            if metadata_status == "unavailable" || metadata_status == "unverified" {
                result.errors.push(token_diagnostic(
                    mint,
                    "on_chain_metadata",
                    "solana_rpc",
                    metadata_caveat,
                    metadata_json,
                ));
            }
        }
        Err(error) => result.errors.push(token_diagnostic(
            mint,
            "on_chain_metadata",
            "solana_rpc",
            error.to_string(),
            provider_error_details(&format!("{error:#}")),
        )),
    }
    match rpc.get_asset(mint).await {
        Ok(asset) => {
            let content = asset.get("content").unwrap_or(&serde_json::Value::Null);
            let metadata = content.get("metadata").unwrap_or(&serde_json::Value::Null);
            let take_string = |value: Option<&serde_json::Value>| {
                value
                    .and_then(serde_json::Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned)
            };
            if result.name.is_none() {
                result.name = take_string(metadata.get("name"));
            }
            if result.symbol.is_none() {
                result.symbol = take_string(metadata.get("symbol"));
            }
            if result.metadata_uri.is_none() {
                result.metadata_uri = take_string(content.get("json_uri"));
            }
            if result.logo_url.is_none() {
                result.logo_url = take_string(content.pointer("/links/image"));
            }
            if result.logo_url.is_none() {
                result.logo_url = content
                    .get("files")
                    .and_then(serde_json::Value::as_array)
                    .and_then(|files| {
                        files.iter().find(|file| {
                            file.get("mime")
                                .and_then(serde_json::Value::as_str)
                                .is_some_and(|mime| mime.starts_with("image/"))
                        })
                    })
                    .and_then(|file| take_string(file.get("uri")));
            }
        }
        Err(error) => result.errors.push(token_diagnostic(
            mint,
            "helius_das_metadata",
            "helius",
            error.to_string(),
            provider_error_details(&format!("{error:#}")),
        )),
    }

    if let Some(uri) = result.metadata_uri.as_deref() {
        let fetch_uri = if let Some(path) = uri.strip_prefix("ipfs://") {
            format!("https://ipfs.io/ipfs/{}", path.trim_start_matches("ipfs/"))
        } else if let Some(path) = uri.strip_prefix("ar://") {
            format!("https://arweave.net/{}", path.trim_start_matches('/'))
        } else {
            uri.to_owned()
        };
        match reqwest::Url::parse(&fetch_uri) {
            Ok(url) => {
                let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
                let unsafe_host = host.parse::<std::net::IpAddr>().is_ok()
                    || host == "localhost"
                    || host.ends_with(".localhost")
                    || host.ends_with(".local");
                if url.scheme() != "https" || unsafe_host {
                    result.errors.push(token_diagnostic(
                        mint,
                        "off_chain_metadata",
                        "metadata_uri",
                        "Metadata URI was blocked because it is not a permitted public HTTPS URL.",
                        json!({"uri_scheme":url.scheme(),"host":host,"blocked":true}),
                    ));
                } else {
                    match reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).timeout(Duration::from_secs(5)).build(){
                        Err(error)=>result.errors.push(token_diagnostic(mint,"off_chain_metadata","metadata_uri",error.to_string(),json!({"kind":"client_configuration_error","message":error.to_string()}))),
                        Ok(client)=>{
                            let activity_id = uuid::Uuid::new_v4().to_string();
                            let request_started = std::time::Instant::now();
                            let _ = activity_events.send(json!({"type":"engine_activity","data":{
                                "id":activity_id,"correlation_id":activity_id,"timestamp":chrono::Utc::now().to_rfc3339(),
                                "direction":"request","provider":"token_metadata_uri","operation":"GET",
                                "token_mint":mint,"endpoint":host,"request":{"method":"GET","url":format!("{}://{}{}",url.scheme(),host,url.path())}
                            }}));
                            match client.get(url).header(reqwest::header::ACCEPT,"application/json").send().await{
                            Err(error)=>{
                                let _ = activity_events.send(json!({"type":"engine_activity","data":{
                                    "id":format!("{activity_id}-response"),"correlation_id":activity_id,"timestamp":chrono::Utc::now().to_rfc3339(),
                                    "direction":"response","provider":"token_metadata_uri","operation":"GET","token_mint":mint,
                                    "endpoint":host,"duration_ms":request_started.elapsed().as_millis(),"severity":"error","error":error.to_string()
                                }}));
                                result.errors.push(token_diagnostic(mint,"off_chain_metadata","metadata_uri",error.to_string(),json!({"kind":"transport_error","message":error.to_string()})));
                            },
                            Ok(mut response)=>{
                                let status=response.status();let content_length=response.content_length();let mut bytes=Vec::new();
                                let mut too_large=content_length.is_some_and(|length|length>262_144);
                                while !too_large{match response.chunk().await{
                                    Ok(Some(chunk))=>{if bytes.len().saturating_add(chunk.len())>262_144{too_large=true;break;}bytes.extend_from_slice(&chunk);}
                                    Ok(None)=>break,
                                    Err(error)=>{result.errors.push(token_diagnostic(mint,"off_chain_metadata","metadata_uri",error.to_string(),json!({"kind":"response_read_error","http_status":status.as_u16(),"message":error.to_string()})));break;}
                                }}
                                let response_preview = String::from_utf8_lossy(&bytes);
                                let response_json = serde_json::from_slice::<serde_json::Value>(&bytes)
                                    .unwrap_or_else(|_| json!({"raw_text":response_preview.chars().take(32_768).collect::<String>()}));
                                let response_text = serde_json::to_string(&response_json).unwrap_or_default();
                                let response_truncated = response_text.len() > 32_768 || too_large;
                                let _ = activity_events.send(json!({"type":"engine_activity","data":{
                                    "id":format!("{activity_id}-response"),"correlation_id":activity_id,"timestamp":chrono::Utc::now().to_rfc3339(),
                                    "direction":"response","provider":"token_metadata_uri","operation":"GET","token_mint":mint,
                                    "endpoint":host,"duration_ms":request_started.elapsed().as_millis(),"http_status":status.as_u16(),
                                    "severity":if status.is_success() && !too_large {"info"} else {"error"},
                                    "body_truncated":response_truncated,
                                    "response":if response_truncated {json!({"truncated":true,"preview":response_text.chars().take(32_768).collect::<String>()})} else {response_json}
                                }}));
                                if too_large{result.errors.push(token_diagnostic(mint,"off_chain_metadata","metadata_uri","Metadata response exceeded the 256 KiB safety limit.",json!({"content_length":content_length,"received_bytes":bytes.len(),"maximum_bytes":262144})));}
                                else if !status.is_success(){
                                    let body=String::from_utf8_lossy(&bytes);let raw=serde_json::from_str::<serde_json::Value>(&body).unwrap_or_else(|_|json!({"raw_text":body.chars().take(65_536).collect::<String>()}));
                                    result.errors.push(token_diagnostic(mint,"off_chain_metadata","metadata_uri",format!("Metadata provider returned HTTP {status}."),json!({"http_status":status.as_u16(),"response":raw})));
                                }else{match serde_json::from_slice::<serde_json::Value>(&bytes){
                                    Ok(value)=>{
                                        result.logo_url=value.get("image").or_else(||value.get("image_url")).and_then(serde_json::Value::as_str).map(str::trim).filter(|v|!v.is_empty()).map(str::to_owned);
                                        if result.name.is_none(){result.name=value.get("name").and_then(serde_json::Value::as_str).map(str::trim).filter(|v|!v.is_empty()).map(str::to_owned);}
                                        if result.symbol.is_none(){result.symbol=value.get("symbol").and_then(serde_json::Value::as_str).map(str::trim).filter(|v|!v.is_empty()).map(str::to_owned);}
                                    }
                                    Err(error)=>{let body=String::from_utf8_lossy(&bytes);result.errors.push(token_diagnostic(mint,"off_chain_metadata","metadata_uri",format!("Metadata response was not valid JSON: {error}"),json!({"kind":"invalid_json","response_text":body.chars().take(65_536).collect::<String>()})));}
                                }}
                            }
                            }
                        }
                    }
                }
            }
            Err(error) => result.errors.push(token_diagnostic(
                mint,
                "off_chain_metadata",
                "metadata_uri",
                error.to_string(),
                json!({"kind":"invalid_metadata_uri","message":error.to_string()}),
            )),
        }
    }
    // DexScreener is a last-resort display fallback. Only accept a pair whose
    // base token address exactly matches this mint; never borrow another token's identity.
    if result.name.is_none() || result.symbol.is_none() || result.logo_url.is_none() {
        let activity_id = uuid::Uuid::new_v4().to_string();
        let started = Instant::now();
        let endpoint = "api.dexscreener.com";
        let request_url = format!("https://api.dexscreener.com/latest/dex/tokens/{mint}");
        let _ = activity_events.send(json!({"type":"engine_activity","data":{
            "id":activity_id,"correlation_id":activity_id,"timestamp":chrono::Utc::now().to_rfc3339(),
            "direction":"request","provider":"dexscreener","endpoint":endpoint,
            "operation":"GET /latest/dex/tokens/{tokenAddress}","token_mint":mint,
            "request":{"method":"GET","path":format!("/latest/dex/tokens/{mint}")}
        }}));
        match reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
        {
            Err(error) => result.errors.push(token_diagnostic(
                mint,
                "dexscreener_metadata",
                "dexscreener",
                error.to_string(),
                json!({"kind":"client_configuration_error","message":error.to_string()}),
            )),
            Ok(client) => match client
                .get(&request_url)
                .header(reqwest::header::ACCEPT, "application/json")
                .send()
                .await
            {
                Err(error) => {
                    let _ = activity_events.send(json!({"type":"engine_activity","data":{
                        "id":format!("{activity_id}-response"),"correlation_id":activity_id,
                        "timestamp":chrono::Utc::now().to_rfc3339(),"direction":"response",
                        "provider":"dexscreener","endpoint":endpoint,
                        "operation":"GET /latest/dex/tokens/{tokenAddress}","token_mint":mint,
                        "duration_ms":started.elapsed().as_millis(),"severity":"error","error":error.to_string()
                    }}));
                    result.errors.push(token_diagnostic(
                        mint,
                        "dexscreener_metadata",
                        "dexscreener",
                        error.to_string(),
                        json!({"kind":"transport_error","message":error.to_string()}),
                    ));
                }
                Ok(response) => {
                    let status = response.status();
                    match response.bytes().await {
                        Err(error) => result.errors.push(token_diagnostic(
                            mint,"dexscreener_metadata","dexscreener",error.to_string(),
                            json!({"kind":"response_read_error","http_status":status.as_u16(),"message":error.to_string()}),
                        )),
                        Ok(bytes) if bytes.len() > 524_288 => result.errors.push(token_diagnostic(
                            mint,"dexscreener_metadata","dexscreener",
                            "DexScreener response exceeded the 512 KiB safety limit.",
                            json!({"response_bytes":bytes.len(),"maximum_bytes":524288}),
                        )),
                        Ok(bytes) => {
                            let value = serde_json::from_slice::<serde_json::Value>(&bytes)
                                .unwrap_or_else(|_| json!({"invalid_json":true}));
                            let body_text = serde_json::to_string(&value).unwrap_or_default();
                            let truncated = body_text.len() > 32_768;
                            let _ = activity_events.send(json!({"type":"engine_activity","data":{
                                "id":format!("{activity_id}-response"),"correlation_id":activity_id,
                                "timestamp":chrono::Utc::now().to_rfc3339(),"direction":"response",
                                "provider":"dexscreener","endpoint":endpoint,
                                "operation":"GET /latest/dex/tokens/{tokenAddress}","token_mint":mint,
                                "duration_ms":started.elapsed().as_millis(),"http_status":status.as_u16(),
                                "severity":if status.is_success() {"info"} else {"error"},
                                "body_truncated":truncated,
                                "response":if truncated {json!({"truncated":true,"preview":body_text.chars().take(32_768).collect::<String>()})} else {value.clone()}
                            }}));
                            if !status.is_success() {
                                result.errors.push(token_diagnostic(
                                    mint,"dexscreener_metadata","dexscreener",
                                    format!("DexScreener returned HTTP {status}."),
                                    json!({"http_status":status.as_u16(),"response":value}),
                                ));
                            } else if let Some(pairs) = value.get("pairs").and_then(serde_json::Value::as_array) {
                                let pair = pairs.iter().find(|pair|
                                    pair.pointer("/baseToken/address").and_then(serde_json::Value::as_str)
                                        == Some(mint)
                                );
                                if let Some(pair) = pair {
                                    let token = pair.get("baseToken").unwrap_or(&serde_json::Value::Null);
                                    let take = |value: Option<&serde_json::Value>| value
                                        .and_then(serde_json::Value::as_str).map(str::trim)
                                        .filter(|value| !value.is_empty()).map(str::to_owned);
                                    if result.name.is_none() { result.name = take(token.get("name")); }
                                    if result.symbol.is_none() { result.symbol = take(token.get("symbol")); }
                                    if result.logo_url.is_none() {
                                        result.logo_url = take(pair.pointer("/info/imageUrl"))
                                            .or_else(|| take(pair.pointer("/info/header")));
                                    }
                                } else {
                                    result.errors.push(token_diagnostic(
                                        mint,"dexscreener_metadata","dexscreener",
                                        "No indexed pair had this mint as its base token; identity fallback was not applied.",
                                        json!({"pair_count":pairs.len(),"mint_match":false}),
                                    ));
                                }
                            }
                        }
                    }
                }
            },
        }
    }

    if let Some(candidate) = result.logo_url.clone() {
        result.logo_url = normalize_image_url(&candidate);
        if result.logo_url.is_none() {
            result.errors.push(token_diagnostic(
                mint,
                "logo_url_validation",
                "metadata",
                "The token image URL was rejected because it was not a public HTTPS URL.",
                json!({"blocked":true}),
            ));
        }
    }

    result.status = if result.name.is_some()
        && result.symbol.is_some()
        && result.decimals.is_some()
        && result.logo_url.is_some()
    {
        "complete"
    } else if result.name.is_some()
        || result.symbol.is_some()
        || result.decimals.is_some()
        || result.metadata_uri.is_some()
        || result.logo_url.is_some()
    {
        "partial"
    } else {
        "unavailable"
    };
    Ok(result)
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
    // Prevent concurrent discovery/metadata/scan pipelines for the same mint.
    let inflight_mints = Arc::new(Mutex::new(HashSet::<String>::new()));
    // Bound concurrent background authority monitors independently from launch workers.
    let authority_monitor_slots = Arc::new(tokio::sync::Semaphore::new(24));
    let (work_tx, work_rx) = tokio::sync::mpsc::channel::<(String, u64)>(LAUNCH_QUEUE_CAPACITY);
    let work_rx = Arc::new(Mutex::new(work_rx));

    for _ in 0..LAUNCH_WORKER_COUNT {
        let work_rx = work_rx.clone();
        let seen_signatures = seen_signatures.clone();
        let rpc = rpc.clone();
        let events = events.clone();
        let program_id = pump_fun_program_id.clone();
        let control = control.clone();
        let market_monitor = market_monitor.clone();
        let token_registry = token_registry.clone();
        let inflight_mints = inflight_mints.clone();
        let authority_monitor_slots = authority_monitor_slots.clone();
        let database = database.clone();
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
                let mint_address = launch.mint_address.clone();
                {
                    let mut inflight = inflight_mints.lock().await;
                    if !inflight.insert(mint_address.clone()) {
                        continue;
                    }
                }
                // A previously approved token is already durable; do not restart its lifecycle.
                if token_registry.get(&mint_address).is_some() {
                    inflight_mints.lock().await.remove(&mint_address);
                    continue;
                }

                // Resolve identity before announcing discovery or starting security analysis.
                let metadata_result = tokio::time::timeout(
                    Duration::from_secs(10),
                    resolve_discovery_metadata(&rpc, &mint_address, &events),
                )
                .await;
                let metadata = match metadata_result {
                    Ok(Ok(metadata)) => metadata,
                    Ok(Err(error)) => {
                        tracing::warn!(%error,mint_address=%mint_address,"Initial token metadata resolution failed; continuing with partial discovery");
                        let mut partial = DiscoveryMetadata::partial();
                        partial.errors.push(token_diagnostic(
                            &mint_address,
                            "metadata_resolution",
                            "solana_rpc",
                            error.to_string(),
                            provider_error_details(&format!("{error:#}")),
                        ));
                        partial
                    }
                    Err(_) => {
                        let mut partial = DiscoveryMetadata::partial();
                        let message =
                            "Initial token metadata resolution timed out after 10 seconds.";
                        tracing::warn!(mint_address=%mint_address,"{message}");
                        partial.errors.push(token_diagnostic(
                            &mint_address,
                            "metadata_resolution",
                            "solana_rpc",
                            message,
                            json!({"kind":"timeout","timeout_seconds":10}),
                        ));
                        partial
                    }
                };

                let creator_address = launch.creator_address.clone();
                let creator_address_for_scan = creator_address.clone();
                let launch_signature_for_scan = launch.signature.clone();
                let mut identity = serde_json::Map::new();
                identity.insert("mint_address".into(), json!(mint_address));
                if let Some(value) = metadata.name.as_ref() {
                    identity.insert("name".into(), json!(value));
                }
                if let Some(value) = metadata.symbol.as_ref() {
                    identity.insert("symbol".into(), json!(value));
                }
                if let Some(value) = metadata.decimals {
                    identity.insert("decimals".into(), json!(value));
                }
                if let Some(value) = metadata.metadata_uri.as_ref() {
                    identity.insert("metadata_uri".into(), json!(value));
                }
                if let Some(value) = metadata.logo_url.as_ref() {
                    identity.insert("logo_url".into(), json!(value));
                }

                let initial_state = token_registry.upsert_patch(
                    &mint_address,
                    json!({
                        "identity": identity,
                        "discovery": {
                            "source": "pump_fun",
                            "creator_address": creator_address,
                            "signature": launch.signature,
                            "slot": launch.slot,
                            "detected_at": launch.detected_at,
                            "metadata_status": metadata.status
                        },
                        "security": {"status": "pending"},
                        "market": {"status": "not_started"}
                    }),
                );
                match initial_state {
                    Ok(snapshot) => {
                        // This is intentionally runtime-only. No launch or token row is written yet.
                        let _ = events.send(json!({
                            "type":"new_token_detected",
                            "data":{
                                "mint_address":mint_address,
                                "name":metadata.name,
                                "symbol":metadata.symbol,
                                "decimals":metadata.decimals,
                                "logo_url":metadata.logo_url,
                                "metadata_uri":metadata.metadata_uri,
                                "metadata_status":metadata.status,
                                "scan_status":"pending",
                                "discovery":snapshot.data.get("discovery").cloned().unwrap_or(serde_json::Value::Null)
                            }
                        }));
                        let mut legacy_launch =
                            serde_json::to_value(&launch).unwrap_or_else(|_| json!({}));
                        if let Some(object) = legacy_launch.as_object_mut() {
                            object.insert("name".into(), json!(metadata.name));
                            object.insert("symbol".into(), json!(metadata.symbol));
                            object.insert("decimals".into(), json!(metadata.decimals));
                            object.insert("logo_url".into(), json!(metadata.logo_url));
                            object.insert("metadata_uri".into(), json!(metadata.metadata_uri));
                            object.insert("metadata_status".into(), json!(metadata.status));
                        }
                        // Keep the legacy event enriched so existing clients also receive
                        // metadata before the scanner begins.
                        let _ = events.send(json!({"type":"launch_detected","data":legacy_launch}));
                        for diagnostic in &metadata.errors {
                            let _ =
                                events.send(json!({"type":"token_diagnostic","data":diagnostic}));
                        }
                        let _ = events.send(json!({"type":"token_scan_started","data":{
                            "mint_address":mint_address,
                            "status":"scanning",
                            "received_at":chrono::Utc::now().to_rfc3339()
                        }}));
                    }
                    Err(error) => {
                        tracing::error!(%error, mint_address=%mint_address, "Could not create in-memory pending token state");
                        inflight_mints.lock().await.remove(&mint_address);
                        continue;
                    }
                }

                let scan_rpc = rpc.clone();
                let scan_events = events.clone();
                let scan_mint = mint_address.clone();
                let approved_monitor = market_monitor.clone();
                let approved_program_id = program_id.clone();
                let approved_registry = token_registry.clone();
                let approved_database = database.clone();
                let inflight_mints_for_scan = inflight_mints.clone();
                let authority_monitor_slots_for_scan = authority_monitor_slots.clone();
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
                    )
                    .await
                    {
                        Ok(Ok(report)) => Ok(report),
                        Ok(Err(error)) => Err(error.to_string()),
                        Err(_) => Err("Security scan timed out after 45 seconds".to_string()),
                    };
                    match scan_result {
                        Ok(mut report) => {
                            // Keep the pre-scan identity authoritative when the scanner has no better value.
                            if report.metadata.name.is_none() {
                                report.metadata.name = metadata.name.clone();
                            }
                            if report.metadata.symbol.is_none() {
                                report.metadata.symbol = metadata.symbol.clone();
                            }
                            if report.metadata.uri.is_none() {
                                report.metadata.uri = metadata.metadata_uri.clone();
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
                                identity_patch.insert("total_supply_raw".into(), json!(supply));
                            }
                            if let Some(logo) = metadata.logo_url.as_ref() {
                                identity_patch.insert("logo_url".into(), json!(logo));
                            }
                            if !identity_patch.is_empty() {
                                let _ = approved_registry
                                    .upsert_patch(&scan_mint, json!({"identity":identity_patch}));
                            }

                            // Creator history is evidence, not a standalone verdict. A bounded sample
                            // can reveal prior launches, but cannot prove wallet clusters or early selling.
                            let _ = scan_events.send(json!({"type":"token_scan_progress","data":{
                                "mint_address":scan_mint,
                                "stage":"creator_history",
                                "progress":72,
                                "message":"Reviewing recent creator activity",
                                "received_at":chrono::Utc::now().to_rfc3339()
                            }}));
                            match tokio::time::timeout(
                                Duration::from_secs(15),
                                scan_rpc.scan_creator_history(
                                    creator_address_for_scan.as_deref().unwrap_or_default(),
                                    &launch_signature_for_scan,
                                    &approved_program_id,
                                ),
                            )
                            .await
                            {
                                Ok(Ok(creator_report)) => {
                                    let status = creator_report.status.clone();
                                    let detail = format!(
                                        "Sampled {} creator signatures and resolved {} transactions; found {} prior Pump.fun launches. Early selling and linked-wallet behaviour remain unverified. {}",
                                        creator_report.signatures_sampled,
                                        creator_report.transactions_resolved,
                                        creator_report.prior_pump_fun_launches,
                                        creator_report.caveat
                                    );
                                    report.checks.push(nodex_solana::SecurityCheck {
                                        name: "creator_history".into(),
                                        status: if status == "unverified" {
                                            "unverified"
                                        } else {
                                            "partial"
                                        }
                                        .into(),
                                        severity: "high".into(),
                                        detail,
                                    });
                                    report.creator_history = Some(creator_report);
                                }
                                Ok(Err(error)) => {
                                    let _=scan_events.send(json!({"type":"token_diagnostic","data":token_diagnostic(&scan_mint,"creator_history","solana_rpc",error.to_string(),provider_error_details(&format!("{error:#}")))}));
                                    report.checks.push(nodex_solana::SecurityCheck {
                                        name: "creator_history".into(),
                                        status: "unverified".into(),
                                        severity: "high".into(),
                                        detail: format!("Creator history lookup failed: {error}"),
                                    });
                                }
                                Err(_) => {
                                    let message="Creator history lookup exceeded its 15-second time budget.";
                                    let _=scan_events.send(json!({"type":"token_diagnostic","data":token_diagnostic(&scan_mint,"creator_history","solana_rpc",message,json!({"kind":"timeout","timeout_seconds":15}))}));
                                    report.checks.push(nodex_solana::SecurityCheck {
                                        name: "creator_history".into(),
                                        status: "unverified".into(),
                                        severity: "high".into(),
                                        detail: message.into(),
                                    });
                                }
                            }

                            if let Some(details) = report.rugcheck.error_details.clone() {
                                let message =
                                    report.rugcheck.error.clone().unwrap_or_else(|| {
                                        "RugCheck provider request failed.".into()
                                    });
                                let _=scan_events.send(json!({"type":"token_diagnostic","data":token_diagnostic(&scan_mint,"rugcheck","rugcheck",message,details)}));
                            }
                            for check in &report.checks {
                                if check.name == "holder_distribution"
                                    && check.detail.contains("NODEX_PROVIDER_ERROR_JSON:")
                                {
                                    let _=scan_events.send(json!({"type":"token_diagnostic","data":token_diagnostic(&scan_mint,"holder_distribution","solana_rpc",check.detail.clone(),provider_error_details(&check.detail))}));
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
                                    let launch_saved = match nodex_persistence::save_launch(
                                        &approved_database,
                                        &launch,
                                    )
                                    .await
                                    {
                                        Ok(true) => true,
                                        Ok(false) => true,
                                        Err(error) => {
                                            let _ = scan_events.send(json!({"type":"diagnostic","data":{
                                                "severity":"error","component":"launch_persistence",
                                                "message":error.to_string(),"mint_address":scan_mint
                                            }}));
                                            false
                                        }
                                    };
                                    let approved_state = if launch_saved {
                                        approved_registry.upsert_patch(
                                            &scan_mint,
                                            json!({
                                                "security":report.clone(),
                                                "market":{"status":"starting"}
                                            }),
                                        )
                                    } else {
                                        Err("Approved launch could not be persisted")
                                    };
                                    let approval_persisted = match approved_state {
                                        Ok(snapshot) => nodex_persistence::save_token_state(
                                            &approved_database,
                                            &snapshot,
                                        )
                                        .await
                                        .is_ok(),
                                        Err(error) => {
                                            let _ = scan_events.send(json!({"type":"diagnostic","data":{
                                                "severity":"error","component":"token_approval_registry",
                                                "message":error,"mint_address":scan_mint
                                            }}));
                                            false
                                        }
                                    };
                                    if approval_persisted {
                                        let _ = scan_events.send(json!({"type":"token_scan_update","data":report.clone()}));
                                        if let Some(snapshot) = approved_registry.get(&scan_mint) {
                                            let _ = scan_events.send(json!({"type":"token_state_updated","data":snapshot}));
                                        }
                                    } else {
                                        terminal_status = "unverified";
                                        let message="Security approval could not be persisted to SQLite; token is retained as unverified.";
                                        let _=scan_events.send(json!({"type":"token_diagnostic","data":token_diagnostic(&scan_mint,"token_persistence","sqlite",message,json!({"kind":"database_persistence_error","message":message}))}));
                                        if let Some(snapshot) = approved_registry.get(&scan_mint) {
                                            let _=scan_events.send(json!({"type":"token_state_updated","data":snapshot}));
                                        }
                                    }
                                }
                                "REJECTED" => {
                                    // Rejected launches remain visible for investigation.
                                    // Their explicit rejected security status keeps them out of
                                    // the approved-token market and paper-entry paths.
                                    let mut security_state =
                                        serde_json::to_value(&report).unwrap_or_else(|_| json!({}));
                                    if let Some(object) = security_state.as_object_mut() {
                                        object.insert("status".into(), json!("rejected"));
                                    }
                                    match approved_registry.upsert_patch(
                                        &scan_mint,
                                        json!({
                                            "security":security_state,
                                            "market":{"status":"not_started"}
                                        }),
                                    ) {
                                        Ok(snapshot) => {
                                            if let Err(error) = nodex_persistence::save_token_state(
                                                &approved_database,
                                                &snapshot,
                                            )
                                            .await
                                            {
                                                let message = format!(
                                                    "Rejected token state could not be persisted: {error}"
                                                );
                                                let _ = scan_events.send(json!({
                                                    "type":"token_diagnostic",
                                                    "data":token_diagnostic(
                                                        &scan_mint,
                                                        "token_persistence",
                                                        "sqlite",
                                                        message.clone(),
                                                        json!({"kind":"database_persistence_error","message":message})
                                                    )
                                                }));
                                            } else {
                                                let _ = scan_events.send(json!({
                                                    "type":"token_state_updated",
                                                    "data":snapshot
                                                }));
                                            }
                                        }
                                        Err(error) => {
                                            let _ = scan_events.send(json!({
                                                "type":"token_diagnostic",
                                                "data":token_diagnostic(
                                                    &scan_mint,
                                                    "token_state",
                                                    "nodex_token_registry",
                                                    error.to_string(),
                                                    json!({"kind":"token_registry_error","message":error.to_string()})
                                                )
                                            }));
                                        }
                                    }
                                    if let Err(error) =
                                        nodex_persistence::save_launch(&approved_database, &launch)
                                            .await
                                    {
                                        let _ = scan_events.send(json!({
                                            "type":"token_diagnostic",
                                            "data":token_diagnostic(
                                                &scan_mint,
                                                "launch_persistence",
                                                "sqlite",
                                                error.to_string(),
                                                json!({"kind":"database_persistence_error","message":error.to_string()})
                                            )
                                        }));
                                    }
                                    let _ = scan_events.send(json!({
                                        "type":"token_scan_update",
                                        "data":report.clone()
                                    }));
                                    let _ = scan_events.send(json!({
                                        "type":"token_approval_pending",
                                        "data":{
                                            "mint_address":scan_mint,
                                            "decision":"REJECTED",
                                            "reason":report.decision_reason
                                        }
                                    }));
                                }
                                _ => {
                                    let mut security_state =
                                        serde_json::to_value(&report).unwrap_or_else(|_| json!({}));
                                    if let Some(object) = security_state.as_object_mut() {
                                        object.insert("status".into(), json!("unverified"));
                                    }
                                    match approved_registry.upsert_patch(&scan_mint,json!({"security":security_state,"market":{"status":"not_started"}})){
                                        Ok(snapshot)=>{
                                            if let Err(error)=nodex_persistence::save_token_state(&approved_database,&snapshot).await{
                                                terminal_status="unverified";
                                                let _=scan_events.send(json!({"type":"token_diagnostic","data":token_diagnostic(&scan_mint,"token_persistence","sqlite",error.to_string(),json!({"kind":"database_persistence_error","message":error.to_string()}))}));
                                            }else{let _=scan_events.send(json!({"type":"token_state_updated","data":snapshot}));}
                                        }
                                        Err(error)=>{let _=scan_events.send(json!({"type":"token_diagnostic","data":token_diagnostic(&scan_mint,"token_state","nodex_token_registry",error.to_string(),json!({"kind":"token_registry_error","message":error.to_string()}))}));}
                                    }
                                    if let Err(error) =
                                        nodex_persistence::save_launch(&approved_database, &launch)
                                            .await
                                    {
                                        let _=scan_events.send(json!({"type":"token_diagnostic","data":token_diagnostic(&scan_mint,"launch_persistence","sqlite",error.to_string(),json!({"kind":"database_persistence_error","message":error.to_string()}))}));
                                    }
                                    let _ = scan_events.send(
                                        json!({"type":"token_scan_update","data":report.clone()}),
                                    );
                                    let _=scan_events.send(json!({"type":"token_approval_pending","data":{"mint_address":scan_mint,"decision":decision,"reason":"Token remains visible but isolated until an explicit APPROVED decision is produced."}}));
                                }
                            }
                            let _ = scan_events.send(json!({"type":"token_scan_completed","data":{
                                "mint_address":scan_mint,"decision":decision,"status":terminal_status,
                                "progress":100,"message":"Security scan finished.",
                                "received_at":chrono::Utc::now().to_rfc3339()
                            }}));
                        }
                        Err(error) => {
                            let error_details = provider_error_details(&error);
                            let _=scan_events.send(json!({"type":"token_diagnostic","data":token_diagnostic(&scan_mint,"security_scan","solana_rpc",error.clone(),error_details.clone())}));
                            let _=scan_events.send(json!({"type":"diagnostic","data":{"severity":"error","component":"token_security_scanner","message":error,"mint_address":scan_mint,"received_at":chrono::Utc::now().to_rfc3339()}}));
                            let _=scan_events.send(json!({"type":"token_scan_failed","data":{"mint_address":scan_mint,"decision":"UNVERIFIED","decision_reason":error,"error_details":error_details,"status":"failed","received_at":chrono::Utc::now().to_rfc3339()}}));
                            match approved_registry.upsert_patch(&scan_mint,json!({"security":{"status":"failed","decision":"UNVERIFIED","decision_reason":error,"error_details":error_details},"market":{"status":"not_started"}})){
                                Ok(snapshot)=>{
                                    if let Err(persist_error)=nodex_persistence::save_token_state(&approved_database,&snapshot).await{
                                        let _=scan_events.send(json!({"type":"token_diagnostic","data":token_diagnostic(&scan_mint,"token_persistence","sqlite",persist_error.to_string(),json!({"kind":"database_persistence_error","message":persist_error.to_string()}))}));
                                    }else{let _=scan_events.send(json!({"type":"token_state_updated","data":snapshot}));}
                                }
                                Err(state_error)=>{let _=scan_events.send(json!({"type":"token_diagnostic","data":token_diagnostic(&scan_mint,"token_state","nodex_token_registry",state_error.to_string(),json!({"kind":"token_registry_error","message":state_error.to_string()}))}));}
                            }
                            if let Err(persist_error) =
                                nodex_persistence::save_launch(&approved_database, &launch).await
                            {
                                let _=scan_events.send(json!({"type":"token_diagnostic","data":token_diagnostic(&scan_mint,"launch_persistence","sqlite",persist_error.to_string(),json!({"kind":"database_persistence_error","message":persist_error.to_string()}))}));
                            }
                        }
                    }
                    // Keep every discovered launch under a bounded, time-limited
                    // authority monitor, including tokens that are still unverified.
                    // This lightweight pass reads only the mint account; it does not
                    // repeat holder sampling or call external security providers.
                    if let Ok(monitor_permit) = authority_monitor_slots_for_scan.try_acquire_owned()
                    {
                        let monitor_rpc = scan_rpc.clone();
                        let monitor_events = scan_events.clone();
                        let monitor_registry = approved_registry.clone();
                        let monitor_database = approved_database.clone();
                        let monitor_mint = scan_mint.clone();
                        tokio::spawn(async move {
                            let _monitor_permit = monitor_permit;
                            let started = Instant::now();
                            let max_runtime = Duration::from_secs(300);
                            let mut rechecks = 0u32;
                            let mut previous_authorities: Option<(Option<String>, Option<String>)> =
                                None;
                            let _ = monitor_events.send(json!({
                                "type":"token_monitoring_started",
                                "data":{"mint_address":monitor_mint,"status":"watching",
                                    "max_runtime_seconds":300,
                                    "received_at":chrono::Utc::now().to_rfc3339()}
                            }));
                            while started.elapsed() < max_runtime {
                                let age = started.elapsed();
                                let interval = if age < Duration::from_secs(15) {
                                    Duration::from_secs(2)
                                } else if age < Duration::from_secs(60) {
                                    Duration::from_secs(5)
                                } else {
                                    Duration::from_secs(15)
                                };
                                tokio::time::sleep(interval).await;
                                if started.elapsed() >= max_runtime {
                                    break;
                                }
                                let checked_at = chrono::Utc::now().to_rfc3339();
                                match tokio::time::timeout(
                                    Duration::from_secs(6),
                                    monitor_rpc.scan_token_authorities(&monitor_mint),
                                )
                                .await
                                {
                                    Ok(Ok(report)) => {
                                        rechecks = rechecks.saturating_add(1);
                                        let authorities = (
                                            report.mint_authority.clone(),
                                            report.freeze_authority.clone(),
                                        );
                                        let changed =
                                            previous_authorities.as_ref() != Some(&authorities);
                                        previous_authorities = Some(authorities.clone());
                                        let active =
                                            authorities.0.is_some() || authorities.1.is_some();
                                        let reason = if active {
                                            "Mint or freeze authority remains active. Monitoring continues; paper entry is blocked."
                                        } else {
                                            "Mint and freeze authorities are now disabled. Other safety checks remain incomplete; paper entry is still blocked."
                                        };
                                        let patch = json!({
                                            "security":{
                                                "status":"watching",
                                                "decision":"UNVERIFIED",
                                                "decision_reason":reason,
                                                "mint_authority":authorities.0,
                                                "freeze_authority":authorities.1,
                                                "authority_recheck":{
                                                    "status":if active {"active"} else {"cleared"},
                                                    "checked_at":checked_at,
                                                    "context_slot":report.context_slot
                                                }
                                            },
                                            "monitoring":{
                                                "status":"watching",
                                                "authority_rechecks":rechecks,
                                                "last_checked_at":checked_at,
                                                "elapsed_seconds":started.elapsed().as_secs()
                                            }
                                        });
                                        match monitor_registry.upsert_patch(&monitor_mint, patch) {
                                            Ok(snapshot) => {
                                                if changed {
                                                    if let Err(error) =
                                                        nodex_persistence::save_token_state(
                                                            &monitor_database,
                                                            &snapshot,
                                                        )
                                                        .await
                                                    {
                                                        let _ = monitor_events.send(json!({
                                                            "type":"token_diagnostic",
                                                            "data":token_diagnostic(&monitor_mint,"authority_monitor_persistence","sqlite",error.to_string(),json!({"kind":"database_persistence_error","message":error.to_string()}))
                                                        }));
                                                    }
                                                    let _ = monitor_events.send(json!({"type":"token_state_updated","data":snapshot}));
                                                }
                                                let _ = monitor_events.send(json!({
                                                    "type":"token_monitoring_update",
                                                    "data":{
                                                        "mint_address":monitor_mint,
                                                        "status":"watching",
                                                        "authority_status":if active {"active"} else {"cleared"},
                                                        "mint_authority":authorities.0,
                                                        "freeze_authority":authorities.1,
                                                        "rechecks":rechecks,
                                                        "checked_at":checked_at,
                                                        "paper_entry_eligible":false
                                                    }
                                                }));
                                            }
                                            Err(error) => {
                                                let _ = monitor_events.send(json!({
                                                    "type":"token_diagnostic",
                                                    "data":token_diagnostic(&monitor_mint,"authority_monitor","token_registry",error.to_string(),json!({"kind":"registry_error","message":error.to_string()}))
                                                }));
                                                break;
                                            }
                                        }
                                    }
                                    Ok(Err(error)) => {
                                        let _ = monitor_events.send(json!({
                                            "type":"token_monitoring_update",
                                            "data":{"mint_address":monitor_mint,"status":"watching",
                                                "authority_status":"unknown","rechecks":rechecks,
                                                "error":error.to_string(),
                                                "checked_at":checked_at,
                                                "paper_entry_eligible":false}
                                        }));
                                        let _ = monitor_events.send(json!({
                                            "type":"token_diagnostic",
                                            "data":token_diagnostic(&monitor_mint,"authority_monitor","solana_rpc",error.to_string(),provider_error_details(&format!("{error:#}")))
                                        }));
                                    }
                                    Err(_) => {
                                        let message =
                                            "Authority recheck timed out after 6 seconds.";
                                        let _ = monitor_events.send(json!({
                                            "type":"token_monitoring_update",
                                            "data":{"mint_address":monitor_mint,"status":"watching",
                                                "authority_status":"unknown","rechecks":rechecks,
                                                "error":message,"checked_at":checked_at,
                                                "paper_entry_eligible":false}
                                        }));
                                    }
                                }
                            }
                            let finished_at = chrono::Utc::now().to_rfc3339();
                            if let Ok(snapshot) = monitor_registry.upsert_patch(
                                &monitor_mint,
                                json!({
                                    "monitoring":{"status":"completed","completed_at":finished_at,
                                        "authority_rechecks":rechecks}
                                }),
                            ) {
                                let _ = nodex_persistence::save_token_state(
                                    &monitor_database,
                                    &snapshot,
                                )
                                .await;
                                let _ = monitor_events
                                    .send(json!({"type":"token_state_updated","data":snapshot}));
                            }
                            let _ = monitor_events.send(json!({
                                "type":"token_monitoring_completed",
                                "data":{"mint_address":monitor_mint,"status":"completed",
                                    "rechecks":rechecks,"duration_seconds":started.elapsed().as_secs(),
                                    "completed_at":finished_at}
                            }));
                        });
                    } else {
                        let _ = scan_events.send(json!({
                            "type":"token_diagnostic",
                            "data":token_diagnostic(&scan_mint,"authority_monitor","nodex_engine",
                                "Background authority-monitor capacity is full; token remains retained but will not receive periodic rechecks.",
                                json!({"kind":"monitor_capacity_reached","capacity":24}))
                        }));
                    }
                    // Market data is discovery/monitoring, not a security approval. Subscribe
                    // every detected Pump.fun curve, including unverified and rejected launches.
                    if let Some(monitor) = approved_monitor.read().await.clone() {
                        match monitor
                            .subscribe_pump_fun_bonding_curve(&scan_mint, &approved_program_id)
                            .await
                        {
                            Ok(account_address) => {
                                let _ = scan_events.send(
                                    json!({"type":"market_subscription_requested","data":{
                                        "mint_address":scan_mint,"account_address":account_address,
                                        "venue":"pump_fun_bonding_curve","status":"pending",
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
                    inflight_mints_for_scan.lock().await.remove(&scan_mint);
                });
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

async fn storage_overview(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    verify_operator_key(&state, &headers)?;
    let overview = storage::inspect(&state.database).await;
    serde_json::to_value(overview)
        .map(Json)
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))
}

async fn cleanup_system_journal(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    verify_operator_key(&state, &headers)?;
    let before = storage::inspect(&state.database).await;
    let cleanup_output = storage::cleanup_system_journal()
        .await
        .map_err(|error| (StatusCode::FORBIDDEN, error))?;
    let after = storage::inspect(&state.database).await;
    let before_bytes = before.system_journal.reported_size_bytes;
    let after_bytes = after.system_journal.reported_size_bytes;
    let bytes_freed = before_bytes
        .zip(after_bytes)
        .map(|(old, new)| old.saturating_sub(new));
    Ok(Json(json!({
        "status": "ok",
        "category": "system_logs",
        "cleanup_output": cleanup_output,
        "storage_before_bytes": before_bytes,
        "storage_after_bytes": after_bytes,
        "bytes_freed": bytes_freed,
        "overview": after
    })))
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
    // This notification is emitted only after the SQLite upsert has committed.
    let _ = state
        .database_changes
        .send(json!({"type":"trading_config_updated","data":settings}))
        .await;
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
    let trading_settings = state.trading_settings.read().await.clone();
    let stream = state.stream_status.read().await.clone();
    let pumpswap_stream = state.pumpswap_status.read().await.clone();
    let control = state.control.read().await.clone();
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

async fn engine_activity_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Vec<serde_json::Value>>, (StatusCode, String)> {
    verify_operator_key(&state, &headers)?;
    let limit = query
        .get("limit")
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(500);
    let records = nodex_persistence::list_engine_activity_logs(&state.database, limit)
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(records))
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
    // Assemble one consistent dashboard snapshot from the engine's authoritative
    // in-memory state. The on-chain wallet and paper wallet are deliberately
    // separate balances and are never added together.
    let positions = state.portfolio_positions.read().await.clone();
    let paper_wallet = state.wallet.read().await.clone();
    let mut profile = state.profile.read().await.clone();

    let open_positions: Vec<serde_json::Value> = positions
        .iter()
        .filter(|position| {
            position.get("status").and_then(serde_json::Value::as_str) == Some("open")
        })
        .cloned()
        .map(|mut position| {
            // Risk configuration belongs to /api/v1/config/trading, not the
            // profile/portfolio snapshot. Do not expose per-position SL/TP fields here.
            if let Some(fields) = position.as_object_mut() {
                fields.retain(|key, _| {
                    let normalized = key.to_ascii_lowercase();
                    !normalized.contains("stop_loss")
                        && !normalized.contains("take_profit")
                        && !normalized.contains("stoploss")
                        && !normalized.contains("takeprofit")
                });
            }
            position
        })
        .collect();
    let sum_field = |items: &[serde_json::Value], field: &str| -> f64 {
        items
            .iter()
            .filter_map(|item| item.get(field).and_then(serde_json::Value::as_f64))
            .sum()
    };

    let total_value_sol = sum_field(&open_positions, "current_value_sol");
    let total_invested_sol = sum_field(&open_positions, "cost_basis_sol");
    let unrealized_pnl_sol = sum_field(&open_positions, "unrealized_pnl_sol");
    let realized_pnl_sol = sum_field(&positions, "realized_pnl_sol");
    let total_pnl_sol = unrealized_pnl_sol + realized_pnl_sol;
    let starting_balance_sol =
        paper_wallet.starting_balance_lamports as f64 / LAMPORTS_PER_SOL as f64;
    let available_balance_sol =
        paper_wallet.available_balance_lamports as f64 / LAMPORTS_PER_SOL as f64;
    let locked_rent_sol = paper_wallet.locked_rent_lamports as f64 / LAMPORTS_PER_SOL as f64;
    let invested_balance_sol =
        paper_wallet.invested_balance_lamports as f64 / LAMPORTS_PER_SOL as f64;

    // Paper-account equity includes unspent funds, marked open positions, and
    // recoverable rent reserves. Fees already debited from available funds are
    // not counted a second time.
    let portfolio_balance_sol = available_balance_sol + total_value_sol + locked_rent_sol;
    let unrealized_pnl_pct = if total_invested_sol > 0.0 {
        unrealized_pnl_sol / total_invested_sol * 100.0
    } else {
        0.0
    };
    let total_pnl_pct = if starting_balance_sol > 0.0 {
        total_pnl_sol / starting_balance_sol * 100.0
    } else {
        0.0
    };

    let portfolio = json!({
        "portfolio_balance_sol": portfolio_balance_sol,
        "total_portfolio_value_sol": portfolio_balance_sol,
        "total_value_sol": total_value_sol,
        "available_balance_sol": available_balance_sol,
        "invested_balance_sol": invested_balance_sol,
        "locked_rent_sol": locked_rent_sol,
        "starting_balance_sol": starting_balance_sol,
        "total_invested_sol": total_invested_sol,
        "unrealized_pnl_sol": unrealized_pnl_sol,
        "unrealized_pnl_pct": unrealized_pnl_pct,
        "realized_pnl_sol": realized_pnl_sol,
        "total_pnl_sol": total_pnl_sol,
        "total_pnl_pct": total_pnl_pct,
        "positions": open_positions,
        "position_count": open_positions.len()
    });

    if let Some(object) = profile.as_object_mut() {
        object.insert("paper_wallet".into(), json!(paper_wallet));
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
    websocket::is_frontend_event(value)
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
    let mut receiver = state.gateway.subscribe();
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
    let (events, _) = broadcast::channel::<serde_json::Value>(8192);
    // Persist every structured provider request/response card independently of
    // the WebSocket client, so the operator can inspect history after reconnecting.
    {
        let mut activity_receiver = events.subscribe();
        let activity_database = db.clone();
        tokio::spawn(async move {
            loop {
                match activity_receiver.recv().await {
                    Ok(event) => {
                        let event_type = event.get("type").and_then(serde_json::Value::as_str);
                        let activity = if event_type == Some("engine_activity") {
                            event.get("data").cloned()
                        } else if matches!(
                            event_type,
                            Some("token_scan_started")
                                | Some("token_scan_progress")
                                | Some("token_scan_update")
                                | Some("token_scan_completed")
                                | Some("token_scan_failed")
                                | Some("token_diagnostic")
                                | Some("diagnostic")
                                | Some("token_metadata_started")
                                | Some("token_metadata_failed")
                                | Some("token_metadata_updated")
                                | Some("token_monitoring_started")
                                | Some("token_monitoring_update")
                                | Some("token_monitoring_completed")
                                | Some("strategy_rejected_token")
                                | Some("paper_buy_executed")
                                | Some("paper_buy_failed")
                                | Some("paper_sell_executed")
                                | Some("paper_sell_failed")
                                | Some("swap_started")
                                | Some("swap_updated")
                                | Some("swap_completed")
                                | Some("swap_failed")
                                | Some("transaction_submitted")
                                | Some("transaction_confirmed")
                                | Some("transaction_failed")
                        ) {
                            let data = event.get("data").cloned().unwrap_or_else(|| event.clone());
                            let severity = if event_type
                                .is_some_and(|kind| kind.ends_with("_failed"))
                                || data.get("severity").and_then(serde_json::Value::as_str)
                                    == Some("error")
                            {
                                "error"
                            } else {
                                "info"
                            };
                            Some(serde_json::json!({
                                "id": uuid::Uuid::new_v4().to_string(),
                                "timestamp": data.get("timestamp")
                                    .or_else(|| data.get("received_at"))
                                    .cloned()
                                    .unwrap_or_else(|| serde_json::Value::String(chrono::Utc::now().to_rfc3339())),
                                "direction": "event",
                                "provider": "engine",
                                "operation": event_type.unwrap_or("engine_event"),
                                "token_mint": data.get("mint_address").or_else(|| data.get("token_mint")),
                                "severity": severity,
                                "event": data
                            }))
                        } else {
                            None
                        };
                        if let Some(activity) = activity {
                            if let Err(error) = nodex_persistence::save_engine_activity_log(
                                &activity_database,
                                &activity,
                            )
                            .await
                            {
                                tracing::error!(%error, "Could not persist engine activity log");
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        tracing::warn!(
                            dropped_events = count,
                            "Engine activity persistence subscriber lagged"
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }
    let (database_changes, database_change_receiver) = mpsc::channel(512);
    let gateway = websocket::Gateway::start(events.subscribe(), database_change_receiver, 4096);
    if let (Some(rpc_client), Some(address)) = (
        (!config.solana_http_urls.is_empty()).then(|| {
            SolanaRpc::new_with_urls(config.solana_http_urls.clone())
                .with_activity_events(events.clone())
        }),
        wallet_address.clone(),
    ) {
        let profile_for_sync = profile_state.clone();
        let database_for_sync = db.clone();
        let database_changes_for_sync = database_changes.clone();
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
                            let _ = database_changes_for_sync
                                .send(json!({"type":"profile_updated","data":snapshot}))
                                .await;
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
        database_changes.clone(),
    );
    let stream_status = Arc::new(RwLock::new(StreamStatus::default()));
    let rpc = (!config.solana_http_urls.is_empty()).then(|| {
        SolanaRpc::new_with_urls(config.solana_http_urls.clone())
            .with_activity_events(events.clone())
    });
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
        gateway,
        database_changes,
        stream_status,
        pumpswap_status,
        wallet: Arc::new(RwLock::new(wallet)),
        rpc,
        database: db.clone(),
        control,
        admin_api_key: std::env::var("NODEX_ADMIN_API_KEY").ok(),
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
        .route("/api/v1/admin/activity-logs", get(engine_activity_logs))
        .route("/api/v1/admin/storage/overview", get(storage_overview))
        .route(
            "/api/v1/admin/storage/system-journal/cleanup",
            post(cleanup_system_journal),
        )
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
