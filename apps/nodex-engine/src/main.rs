mod event_broadcaster;
mod market_monitoring;
mod paper_trading;
mod portfolio;
mod scanner;
mod sol_price;
mod storage;
mod token_details;
mod trading_decisions;
mod websocket;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, patch, post},
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use launch_detector::{detect_pump_fun_launch, CreateInstruction};
use nodex_core::{config::EngineConfig, TokenRegistry};
use nodex_solana::{
    decode_pump_fun_create, decode_pump_fun_trade_logs, decode_pump_swap_activity_logs,
    run_logs_stream, MarketMonitor, PumpSwapActivityEvent, SolanaRpc, StreamStatus,
    PUMPSWAP_PROGRAM_ID,
};
use nodex_trading::{
    main_bot::{
        capture_position_risk, CreatorReputation, LiveMarketActivity, PositionRiskPolicy,
        SecurityAssessment, TradingBotConfig, TradingBotDecision,
    },
    paper_wallet::{BuyCostBreakdown, PaperWallet, LAMPORTS_PER_SOL},
};
use portfolio::calculator::calculate_portfolio_snapshot;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sol_price::SolPriceSnapshot;
use solana_keypair::Keypair;
use solana_signer::Signer;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{
    collections::{HashMap, HashSet, VecDeque},
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
    sol_price: Arc<RwLock<SolPriceSnapshot>>,
    rpc: Option<SolanaRpc>,
    flux_rpc: Option<SolanaRpc>,
    database: sqlx::SqlitePool,
    control: Arc<RwLock<EngineControl>>,
    admin_api_key: Option<String>,
    trading_settings: Arc<RwLock<TradingSettings>>,
    runtime_enabled: watch::Sender<bool>,
    market_monitor: Arc<RwLock<Option<MarketMonitor>>>,
    solana_ws_urls: Vec<String>,
    pump_fun_program_id: String,
    ws_tickets: Arc<Mutex<HashMap<String, Instant>>>,
    token_registry: TokenRegistry,
    profile: Arc<RwLock<serde_json::Value>>,
    portfolio_positions: Arc<RwLock<Vec<serde_json::Value>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PreferredOwner {
    address: String,
    #[serde(default)]
    label: String,
}

fn default_true() -> bool {
    true
}
fn default_trailing_activation_profit_pct() -> f64 {
    20.0
}
fn default_minimum_distinct_buyers_high_score() -> usize { 5 }
fn default_minimum_distinct_buyers_other() -> usize { 8 }
fn default_minimum_recent_buyers_high_score() -> usize { 5 }
fn default_minimum_recent_buyers_other() -> usize { 8 }
fn default_minimum_30s_buy_pressure_pct() -> f64 { 75.0 }
fn default_minimum_30s_buy_sell_ratio() -> f64 { 2.5 }
fn default_minimum_observation_seconds() -> u64 { 90 }
fn default_maximum_early_entry_age_seconds() -> u64 { 300 }
fn default_maximum_early_entry_market_cap_usd() -> f64 { 150_000.0 }

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
struct TradingSettings {
    min_liquidity_usd: f64,
    max_entry_price_usd: f64,
    #[serde(default, skip_serializing)]
    max_investment_per_token_sol: f64,
    max_investment_per_token_usd: f64,
    max_simultaneous_positions: u32,
    min_safety_score: u8,
    max_slippage_pct: f64,
    stop_loss_pct: f64,
    take_profit_pct: f64,
    /// Optional hard profit cap for trailing-stop positions; 0 disables it.
    #[serde(default)]
    maximum_profit_pct: f64,
    min_5m_buy_volume_usd: f64,
    max_top10_holder_concentration_pct: f64,
    auto_buy_enabled: bool,
    /// Close simulated token accounts and refund locked rent after a fully settled exit.
    #[serde(default = "default_true")]
    auto_recover_token_account_rent: bool,
    trailing_stop_enabled: bool,
    trailing_stop_callback_pct: f64,
    #[serde(default = "default_trailing_activation_profit_pct")]
    trailing_activation_profit_pct: f64,
    #[serde(default = "default_minimum_distinct_buyers_high_score")]
    minimum_distinct_buyers_high_score: usize,
    #[serde(default = "default_minimum_distinct_buyers_other")]
    minimum_distinct_buyers_other: usize,
    #[serde(default = "default_minimum_recent_buyers_high_score")]
    minimum_recent_buyers_high_score: usize,
    #[serde(default = "default_minimum_recent_buyers_other")]
    minimum_recent_buyers_other: usize,
    #[serde(default = "default_minimum_30s_buy_pressure_pct")]
    minimum_30s_buy_pressure_pct: f64,
    #[serde(default = "default_minimum_30s_buy_sell_ratio")]
    minimum_30s_buy_sell_ratio: f64,
    #[serde(default = "default_minimum_observation_seconds")]
    minimum_observation_seconds: u64,
    #[serde(default = "default_maximum_early_entry_age_seconds")]
    maximum_early_entry_age_seconds: u64,
    #[serde(default = "default_maximum_early_entry_market_cap_usd")]
    maximum_early_entry_market_cap_usd: f64,
    blacklist_creators_with_prior_rugs: bool,
    paper_starting_balance: f64,
    paper_starting_balance_currency: String,
    diagnostic_mode: bool,
    block_unverified_trades: bool,
    execution_enabled: bool,
    whitelist_only_mode: bool,
    preferred_owners: Vec<PreferredOwner>,
}

impl Default for TradingSettings {
    fn default() -> Self {
        Self {
            min_liquidity_usd: 3_000.0,
            max_entry_price_usd: 0.0,
            // Position sizing is configured in USD. SOL is derived from the
            // latest quote immediately before a paper order is constructed.
            max_investment_per_token_sol: 0.0,
            max_investment_per_token_usd: 15.0,
            max_simultaneous_positions: 3,
            min_safety_score: 80,
            max_slippage_pct: 10.0,
            stop_loss_pct: 15.0,
            take_profit_pct: 100.0,
            maximum_profit_pct: 0.0,
            min_5m_buy_volume_usd: 1_000.0,
            max_top10_holder_concentration_pct: 20.0,
            auto_buy_enabled: true,
            auto_recover_token_account_rent: true,
            trailing_stop_enabled: false,
            trailing_stop_callback_pct: 12.0,
            trailing_activation_profit_pct: default_trailing_activation_profit_pct(),
            minimum_distinct_buyers_high_score: default_minimum_distinct_buyers_high_score(),
            minimum_distinct_buyers_other: default_minimum_distinct_buyers_other(),
            minimum_recent_buyers_high_score: default_minimum_recent_buyers_high_score(),
            minimum_recent_buyers_other: default_minimum_recent_buyers_other(),
            minimum_30s_buy_pressure_pct: default_minimum_30s_buy_pressure_pct(),
            minimum_30s_buy_sell_ratio: default_minimum_30s_buy_sell_ratio(),
            minimum_observation_seconds: default_minimum_observation_seconds(),
            maximum_early_entry_age_seconds: default_maximum_early_entry_age_seconds(),
            maximum_early_entry_market_cap_usd: default_maximum_early_entry_market_cap_usd(),
            blacklist_creators_with_prior_rugs: true,
            paper_starting_balance: 10.0,
            paper_starting_balance_currency: "USD".into(),
            diagnostic_mode: false,
            block_unverified_trades: false,
            execution_enabled: true,
            whitelist_only_mode: false,
            preferred_owners: Vec::new(),
        }
    }
}

impl TradingSettings {
    fn validate(&self) -> Result<(), String> {
        if self.preferred_owners.len() > 500 {
            return Err("preferred_owners must contain no more than 500 wallets.".into());
        }
        let mut owner_addresses = HashSet::new();
        for owner in &self.preferred_owners {
            if owner.address.trim() != owner.address || owner.address.is_empty() {
                return Err(
                    "Each preferred owner must have a non-empty, trimmed Solana wallet address."
                        .into(),
                );
            }
            let decoded = bs58::decode(&owner.address)
                .into_vec()
                .map_err(|_| format!("Invalid Solana wallet address: {}", owner.address))?;
            if decoded.len() != 32 {
                return Err(format!(
                    "Solana wallet address must decode to 32 bytes: {}",
                    owner.address
                ));
            }
            if !owner_addresses.insert(owner.address.as_str()) {
                return Err(format!(
                    "Duplicate preferred owner address: {}",
                    owner.address
                ));
            }
            if owner.label.len() > 80 {
                return Err("Preferred owner labels must be 80 characters or fewer.".into());
            }
        }
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
            ("maximum_profit_pct", self.maximum_profit_pct),
            ("min_5m_buy_volume_usd", self.min_5m_buy_volume_usd),
            (
                "max_top10_holder_concentration_pct",
                self.max_top10_holder_concentration_pct,
            ),
            (
                "trailing_stop_callback_pct",
                self.trailing_stop_callback_pct,
            ),
            (
                "trailing_activation_profit_pct",
                self.trailing_activation_profit_pct,
            ),
            ("minimum_30s_buy_pressure_pct", self.minimum_30s_buy_pressure_pct),
            ("minimum_30s_buy_sell_ratio", self.minimum_30s_buy_sell_ratio),
            ("maximum_early_entry_market_cap_usd", self.maximum_early_entry_market_cap_usd),
            ("paper_starting_balance", self.paper_starting_balance),
        ];
        for (name, value) in finite_non_negative {
            if !value.is_finite() || value < 0.0 {
                return Err(format!("{name} must be a finite, non-negative number."));
            }
        }
        if self.paper_starting_balance <= 0.0 || self.paper_starting_balance > 1_000_000.0 {
            return Err(
                "paper_starting_balance must be greater than 0 and no more than 1,000,000.".into(),
            );
        }
        if !matches!(self.paper_starting_balance_currency.as_str(), "USD" | "SOL") {
            return Err("paper_starting_balance_currency must be USD or SOL.".into());
        }
        if self.stop_loss_pct > 100.0
            || self.trailing_stop_callback_pct > 100.0
            || self.trailing_activation_profit_pct > 1_000.0
        {
            return Err(
                "Stop-loss and trailing-stop percentages must be between 0 and 100.".into(),
            );
        }
        if self.take_profit_pct > 1_000.0 {
            return Err("take_profit_pct must not exceed 1000.".into());
        }
        if self.maximum_profit_pct > 0.0
            && !(100.0..=100_000.0).contains(&self.maximum_profit_pct)
        {
            return Err("maximum_profit_pct must be 0 (disabled) or between 100 and 100000.".into());
        }
        if self.minimum_30s_buy_pressure_pct > 100.0
            || self.minimum_30s_buy_sell_ratio > 100.0
            || self.minimum_observation_seconds == 0
            || self.maximum_early_entry_age_seconds < self.minimum_observation_seconds
            || self.minimum_distinct_buyers_high_score == 0
            || self.minimum_distinct_buyers_other == 0
            || self.minimum_recent_buyers_high_score == 0
            || self.minimum_recent_buyers_other == 0
        {
            return Err("Entry buyer counts, observation window, buy pressure, and buy/sell ratio are invalid.".into());
        }
        if self.max_slippage_pct > 100.0 || self.max_top10_holder_concentration_pct > 100.0 {
            return Err("Slippage and top-10 concentration must be between 0 and 100.".into());
        }
        if self.min_safety_score > 100 {
            return Err("min_safety_score must be between 0 and 100.".into());
        }
        if self.max_investment_per_token_usd <= 0.0 {
            return Err("max_investment_per_token_usd must be greater than zero.".into());
        }
        if self.max_simultaneous_positions == 0 || self.max_simultaneous_positions > 1_000 {
            return Err("max_simultaneous_positions must be between 1 and 1000.".into());
        }
        Ok(())
    }
}

/// Adapt the persisted Trading Configuration table to the Main Trading Bot's
/// strongly typed runtime policy. The current SOL/USD quote resolves USD-sized orders.
fn main_bot_config_from_settings(
    settings: &TradingSettings,
    sol_price_usd: f64,
) -> TradingBotConfig {
    let position_size_usd = settings.max_investment_per_token_usd;
    // Never fall back to a stale SOL-denominated setting. If the live quote is
    // unavailable, expose a zero SOL order amount so an order cannot be built.
    let position_size_sol = if sol_price_usd.is_finite() && sol_price_usd > 0.0 {
        position_size_usd / sol_price_usd
    } else {
        0.0
    };

    TradingBotConfig {
        minimum_safety_score: f64::from(settings.min_safety_score),
        minimum_liquidity_usd: settings.min_liquidity_usd,
        maximum_entry_price_usd: settings.max_entry_price_usd,
        position_size_sol,
        position_size_usd,
        stop_loss_pct: settings.stop_loss_pct,
        take_profit_pct: settings.take_profit_pct,
        maximum_profit_pct: settings.maximum_profit_pct,
        trailing_stop_enabled: settings.trailing_stop_enabled,
        trailing_stop_callback_pct: settings.trailing_stop_callback_pct,
        trailing_activation_profit_pct: settings.trailing_activation_profit_pct,
        maximum_slippage_pct: settings.max_slippage_pct,
        minimum_5m_buy_volume_usd: settings.min_5m_buy_volume_usd,
        maximum_top10_concentration_pct: settings.max_top10_holder_concentration_pct,
        maximum_open_positions: settings.max_simultaneous_positions as usize,
        minimum_distinct_buyers_high_score: settings.minimum_distinct_buyers_high_score,
        minimum_distinct_buyers_other: settings.minimum_distinct_buyers_other,
        minimum_recent_buyers_high_score: settings.minimum_recent_buyers_high_score,
        minimum_recent_buyers_other: settings.minimum_recent_buyers_other,
        minimum_30s_buy_pressure_pct: settings.minimum_30s_buy_pressure_pct,
        minimum_30s_buy_sell_ratio: settings.minimum_30s_buy_sell_ratio,
        minimum_observation_seconds: settings.minimum_observation_seconds,
        maximum_early_entry_age_seconds: settings.maximum_early_entry_age_seconds,
        maximum_early_entry_market_cap_usd: settings.maximum_early_entry_market_cap_usd,
        high_creator_score_threshold: 80.0,
        low_score_observation_seconds: 30,
        blacklist_known_rug_creators: settings.blacklist_creators_with_prior_rugs,
        whitelist_only_mode: settings.whitelist_only_mode,
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

/// Build transient scan-progress fields without allowing late progress events
/// to downgrade a terminal approval or rejection in the canonical token state.
fn core_verification_gate_passed(security: &serde_json::Value) -> bool {
    // Supplementary scan decisions never override the three core checks.
    // Diagnostic-only scans are not evidence and therefore cannot verify a token.
    if security
        .get("diagnostic_only")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true)
    {
        return false;
    }
    ["mint_authority", "freeze_authority", "rugcheck_summary"]
        .iter()
        .all(|required| {
            security
                .get("checks")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|checks| {
                    checks.iter().any(|check| {
                        check.get("name").and_then(serde_json::Value::as_str) == Some(*required)
                            && check
                                .get("status")
                                .and_then(serde_json::Value::as_str)
                                .is_some_and(|status| {
                                    status.eq_ignore_ascii_case("passed")
                                        || status.eq_ignore_ascii_case("verified")
                                })
                    })
                })
        })
}

/// Market monitoring is reserved for tokens that passed the complete core
/// verification gate and received an explicit APPROVED verdict.
fn token_approved_for_market_monitoring(security: &serde_json::Value) -> bool {
    security
        .get("decision")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|decision| decision.eq_ignore_ascii_case("APPROVED"))
        && core_verification_gate_passed(security)
}

/// Paper execution must independently enforce the same approval boundary as
/// market subscription. A monitoring event is not authorization to enter.
fn paper_entry_security_gate_passed(security: Option<&serde_json::Value>) -> bool {
    security.is_some_and(token_approved_for_market_monitoring)
}

fn start_inactive_token_cleanup(
    database: sqlx::SqlitePool,
    registry: TokenRegistry,
    stream_status: Arc<RwLock<StreamStatus>>,
    market_monitor: Arc<RwLock<Option<MarketMonitor>>>,
    events: broadcast::Sender<serde_json::Value>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            interval.tick().await;
            // Never infer inactivity while the upstream feed is unhealthy.
            if !stream_status.read().await.connected || market_monitor.read().await.is_none() {
                continue;
            }
            let cutoff = chrono::Utc::now() - chrono::Duration::hours(1);
            let inactive_mints = registry
                .snapshot()
                .into_iter()
                .filter_map(|snapshot| {
                    let last_activity = snapshot
                        .data
                        .get("last_activity_at")
                        .and_then(serde_json::Value::as_str)
                        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
                        .map(|value| value.with_timezone(&chrono::Utc))
                        .unwrap_or(snapshot.updated_at);
                    (last_activity < cutoff).then_some(snapshot.mint_address)
                })
                .collect::<Vec<_>>();
            if inactive_mints.is_empty() {
                continue;
            }
            match nodex_persistence::delete_inactive_token_history(&database, &inactive_mints).await
            {
                Ok(deleted_mints) if !deleted_mints.is_empty() => {
                    for mint in &deleted_mints {
                        let _ = registry.remove(mint);
                    }
                    if let Some(monitor) = market_monitor.read().await.clone() {
                        for mint in &deleted_mints {
                            if let Err(error) = monitor.unsubscribe_mint(mint).await {
                                tracing::warn!(%error, mint_address = %mint, "Could not unsubscribe inactive token market feed");
                            }
                        }
                    }
                    tracing::info!(
                        deleted_tokens = deleted_mints.len(),
                        "Removed tokens inactive for one hour; tokens with paper positions were preserved"
                    );
                    let _ = events.send(json!({
                        "type":"tokens_removed_inactive",
                        "data":{
                            "mint_addresses":deleted_mints,
                            "inactive_for_seconds":3600,
                            "received_at":chrono::Utc::now().to_rfc3339()
                        }
                    }));
                }
                Ok(_) => {}
                Err(error) => tracing::error!(%error, "Inactive token cleanup failed"),
            }
        }
    });
}

/// Convert only useful operator activity into durable event history.
/// Successful low-level RPC request/response cards stay out of the UI and DB;
/// provider errors and meaningful engine lifecycle events remain observable.
fn activity_log_from_event(event: &serde_json::Value) -> Option<serde_json::Value> {
    let event_type = event.get("type").and_then(serde_json::Value::as_str)?;

    if event_type == "engine_activity" {
        let activity = event.get("data")?.clone();
        let severity = activity
            .get("severity")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let http_status = activity
            .get("http_status")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default();
        let has_error = activity
            .get("error")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|message| !message.is_empty());
        if severity.eq_ignore_ascii_case("error")
            || severity.eq_ignore_ascii_case("warning")
            || http_status >= 400
            || has_error
        {
            return Some(activity);
        }
        return None;
    }

    if !matches!(
        event_type,
        "launch_detected"
            | "new_token_detected"
            | "token_approval_pending"
            | "token_scan_started"
            | "token_scan_progress"
            | "token_scan_update"
            | "token_scan_completed"
            | "token_scan_failed"
            | "token_diagnostic"
            | "diagnostic"
            | "token_metadata_started"
            | "token_metadata_failed"
            | "token_metadata_updated"
            | "token_monitoring_started"
            | "token_monitoring_update"
            | "token_monitoring_completed"
            | "pump_fun_trade_activity"
            | "strategy_rejected_token"
            | "paper_buy_started"
            | "paper_buy_executed"
            | "paper_buy_failed"
            | "paper_entry_evaluation"
            | "market_subscription_requested"
            | "market_subscription_subscribed"
            | "market_subscription_waiting"
            | "market_subscription_failed"
            | "market_monitor_status"
            | "provider_status"
            | "connection_status"
            | "engine_state"
            | "market_curve_completed"
            | "paper_sell_executed"
            | "paper_sell_failed"
            | "paper_exit_triggered"
            | "swap_started"
            | "swap_updated"
            | "swap_completed"
            | "swap_failed"
            | "transaction_submitted"
            | "transaction_confirmed"
            | "transaction_failed"
    ) {
        return None;
    }

    let data = event.get("data").cloned().unwrap_or_else(|| event.clone());
    let severity = if event_type.ends_with("_failed")
        || data.get("severity").and_then(serde_json::Value::as_str) == Some("error")
    {
        "error"
    } else if data.get("severity").and_then(serde_json::Value::as_str) == Some("warning") {
        "warning"
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
        "operation": event_type,
        "token_mint": data.get("mint_address").or_else(|| data.get("token_mint")),
        "severity": severity,
        "event": data
    }))
}

fn interrupted_entry_recovery_patch() -> serde_json::Value {
    json!({
        "activity": {
            "status": "monitoring",
            "label": "Monitoring",
            "stage": "strategy_checks",
            "decision": "wait",
            "reasons": ["The previous entry evaluation was interrupted by a backend restart; waiting for fresh market data."],
            "sub_event": "Checking entry conditions",
            "updated_at": chrono::Utc::now().to_rfc3339()
        }
    })
}

fn paper_entry_activity_patch(data: &serde_json::Value) -> serde_json::Value {
    let decision = data
        .get("decision")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("wait")
        .to_ascii_lowercase();
    let reasons = data
        .get("reasons")
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(serde_json::Value::as_str)
                .filter(|reason| !reason.trim().is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let sub_event = match decision.as_str() {
        "reject" => "Strategy rejected",
        "evaluate_paper_entry" => "Entry conditions met",
        _ => "Checking entry conditions",
    };
    json!({
        "status": "monitoring",
        "label": "Monitoring",
        "stage": "strategy_checks",
        "decision": decision,
        "reasons": reasons,
        "sub_event": sub_event,
        "evaluated_at": data.get("evaluated_at").cloned().unwrap_or(serde_json::Value::Null)
    })
}

fn scan_progress_security_patch(
    current_security: Option<&serde_json::Value>,
    data: &serde_json::Value,
) -> serde_json::Value {
    let current_decision = current_security
        .and_then(|security| security.get("decision"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_ascii_uppercase();
    let preserve_terminal_verdict = matches!(current_decision.as_str(), "APPROVED" | "REJECTED");
    let mut security_patch = json!({
        "progress": data.get("progress").cloned().unwrap_or(serde_json::Value::Null),
        "stage": data.get("stage").cloned().unwrap_or(serde_json::Value::Null),
        "message": data.get("message").cloned().unwrap_or(serde_json::Value::Null)
    });
    if !preserve_terminal_verdict {
        security_patch["status"] = json!("scanning");
    }
    json!({"security":security_patch})
}

#[derive(Debug, Clone)]
struct ConfirmedTradeObservation {
    timestamp_ms: i64,
    trader: String,
    is_buy: bool,
    sol_amount_lamports: u64,
}

fn json_u64(value: Option<&serde_json::Value>) -> Option<u64> {
    value.and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_str().and_then(|text| text.parse::<u64>().ok()))
    })
}

fn market_quote_is_fresh(data: &serde_json::Value) -> bool {
    const MAX_QUOTE_AGE_MS: i64 = 30_000;
    let timestamp_ms = data
        .get("timestamp_ms")
        .or_else(|| data.get("block_time_ms"))
        .and_then(serde_json::Value::as_i64)
        .or_else(|| {
            data.get("received_at")
                .and_then(serde_json::Value::as_str)
                .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
                .map(|value| value.timestamp_millis())
        });
    let Some(timestamp_ms) = timestamp_ms else {
        return false;
    };
    let now_ms = chrono::Utc::now().timestamp_millis();
    timestamp_ms > 0
        && timestamp_ms <= now_ms.saturating_add(60_000)
        && now_ms.saturating_sub(timestamp_ms) <= MAX_QUOTE_AGE_MS
}

fn timestamp_ms_from_value(value: Option<&serde_json::Value>) -> Option<i64> {
    let value = value?;
    if let Some(number) = value.as_i64() {
        return Some(if (-10_000_000_000..10_000_000_000).contains(&number) {
            number.saturating_mul(1_000)
        } else {
            number
        });
    }
    let text = value.as_str()?;
    if let Ok(number) = text.parse::<i64>() {
        return Some(if number.abs() < 10_000_000_000 {
            number.saturating_mul(1_000)
        } else {
            number
        });
    }
    chrono::DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|timestamp| timestamp.timestamp_millis())
}

/// Reject an older market-account slot so delayed notifications cannot roll
/// the token's entry price backwards. If slot tracking has begun, slotless
/// updates are not trusted for entry evaluation.
fn market_update_slot_is_current(last_slot: Option<u64>, incoming_slot: Option<u64>) -> bool {
    match (last_slot, incoming_slot) {
        (Some(last), Some(incoming)) => incoming >= last,
        (Some(_), None) => false,
        (None, _) => true,
    }
}

#[allow(clippy::too_many_arguments)]
fn record_confirmed_trade(
    observations: &mut HashMap<String, VecDeque<ConfirmedTradeObservation>>,
    seen_keys: &mut HashSet<String>,
    mint: &str,
    signature: &str,
    trader: &str,
    side: &str,
    amount_lamports: u64,
    timestamp_ms: i64,
) {
    if mint.is_empty() || trader.is_empty() || amount_lamports == 0 {
        return;
    }
    let is_buy = side.eq_ignore_ascii_case("buy");
    if !is_buy && !side.eq_ignore_ascii_case("sell") {
        return;
    }
    let key = format!("{signature}:{mint}:{trader}:{side}:{amount_lamports}");
    if !seen_keys.insert(key) {
        return;
    }
    if seen_keys.len() > 20_000 {
        seen_keys.clear();
    }
    let now_ms = chrono::Utc::now().timestamp_millis();
    let timestamp_ms = if timestamp_ms <= 0 || timestamp_ms > now_ms.saturating_add(60_000) {
        now_ms
    } else {
        timestamp_ms
    };
    let trades = observations.entry(mint.to_owned()).or_default();
    trades.push_back(ConfirmedTradeObservation {
        timestamp_ms,
        trader: trader.to_owned(),
        is_buy,
        sol_amount_lamports: amount_lamports,
    });
    while trades
        .front()
        .is_some_and(|trade| now_ms.saturating_sub(trade.timestamp_ms) > 300_000)
        || trades.len() > 500
    {
        trades.pop_front();
    }
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
    wallet: Arc<RwLock<PaperWallet>>,
    sol_price: Arc<RwLock<SolPriceSnapshot>>,
    control: Arc<RwLock<EngineControl>>,
    trading_settings: Arc<RwLock<TradingSettings>>,
    database_changes: mpsc::Sender<serde_json::Value>,
) {
    tokio::spawn(async move {
        let mut trade_observations = HashMap::<String, VecDeque<ConfirmedTradeObservation>>::new();
        let mut pool_to_mint = HashMap::<String, String>::new();
        let mut pool_quote_is_sol = HashSet::<String>::new();
        let mut seen_trade_keys = HashSet::<String>::new();
        let mut last_evaluation_signatures = HashMap::<String, String>::new();
        let mut last_entry_attempts = HashMap::<String, Instant>::new();
        let mut last_market_slots = HashMap::<String, u64>::new();
        let mut last_activity_persisted = HashMap::<String, Instant>::new();
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
            if kind == "tokens_removed_inactive" {
                if let Some(mints) = data
                    .get("mint_addresses")
                    .and_then(serde_json::Value::as_array)
                {
                    for mint in mints.iter().filter_map(serde_json::Value::as_str) {
                        trade_observations.remove(mint);
                        last_evaluation_signatures.remove(mint);
                        last_entry_attempts.remove(mint);
                        last_market_slots.remove(mint);
                        last_activity_persisted.remove(mint);
                        let stale_pools = pool_to_mint
                            .iter()
                            .filter_map(|(pool, mapped_mint)| {
                                (mapped_mint.as_str() == mint).then_some(pool.clone())
                            })
                            .collect::<Vec<_>>();
                        for pool in stale_pools {
                            pool_to_mint.remove(&pool);
                            pool_quote_is_sol.remove(&pool);
                        }
                    }
                }
                continue;
            }
            if kind == "market_pool_verified" {
                if let (Some(mint), Some(pool)) = (
                    data.get("mint_address").and_then(serde_json::Value::as_str),
                    data.get("pool_address").and_then(serde_json::Value::as_str),
                ) {
                    pool_to_mint.insert(pool.to_owned(), mint.to_owned());
                    if data.get("quote_mint").and_then(serde_json::Value::as_str)
                        == Some("So11111111111111111111111111111111111111112")
                    {
                        pool_quote_is_sol.insert(pool.to_owned());
                    } else {
                        pool_quote_is_sol.remove(pool);
                    }
                }
                continue;
            }
            if kind == "pumpswap_trade_activity" {
                let pool = data
                    .get("pool_address")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                let Some(mint) = pool_to_mint.get(pool).cloned() else {
                    continue;
                };
                // quote_amount_raw is lamports only for WSOL-quoted pools. Do not
                // misread USDC or another quote mint as SOL volume.
                if !pool_quote_is_sol.contains(pool) {
                    continue;
                }
                let signature = data
                    .get("signature")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                let trader = data
                    .get("trader_address")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                let side = data
                    .get("event_type")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                let amount = json_u64(data.get("quote_amount_raw")).unwrap_or(0);
                let timestamp_ms = timestamp_ms_from_value(data.get("timestamp"))
                    .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
                record_confirmed_trade(
                    &mut trade_observations,
                    &mut seen_trade_keys,
                    &mint,
                    signature,
                    trader,
                    side,
                    amount,
                    timestamp_ms,
                );
                if let Ok(snapshot) = registry.upsert_patch(
                    &mint,
                    json!({"last_activity_at": chrono::Utc::now().to_rfc3339()}),
                ) {
                    let should_persist = last_activity_persisted
                        .get(&mint)
                        .map(|last| last.elapsed() >= Duration::from_secs(60))
                        .unwrap_or(true);
                    if should_persist {
                        if let Err(error) =
                            nodex_persistence::save_token_state(&database, &snapshot).await
                        {
                            tracing::warn!(%error, mint_address = %mint, "Could not persist confirmed trade activity timestamp");
                        } else {
                            last_activity_persisted.insert(mint.clone(), Instant::now());
                        }
                    }
                    let _ = events.send(json!({"type":"token_state_updated","data":snapshot}));
                }
                continue;
            }
            let Some(mint) = data.get("mint_address").and_then(serde_json::Value::as_str) else {
                continue;
            };
            if kind == "market_price_update" {
                let incoming_slot = json_u64(data.get("slot"));
                let last_slot = last_market_slots.get(mint).copied();
                if !market_update_slot_is_current(last_slot, incoming_slot) {
                    continue;
                }
                if let Some(slot) = incoming_slot {
                    last_market_slots.insert(mint.to_owned(), slot);
                }
            }
            if kind == "pump_fun_trade_activity" {
                let signature = data
                    .get("signature")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                let trader = data
                    .get("trader_address")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                let side = data
                    .get("side")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                let amount = json_u64(data.get("sol_amount_lamports")).unwrap_or(0);
                let timestamp_ms = timestamp_ms_from_value(data.get("timestamp"))
                    .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
                record_confirmed_trade(
                    &mut trade_observations,
                    &mut seen_trade_keys,
                    mint,
                    signature,
                    trader,
                    side,
                    amount,
                    timestamp_ms,
                );
                if let Ok(snapshot) = registry.upsert_patch(
                    mint,
                    json!({"last_activity_at": chrono::Utc::now().to_rfc3339()}),
                ) {
                    let should_persist = last_activity_persisted
                        .get(mint)
                        .map(|last| last.elapsed() >= Duration::from_secs(60))
                        .unwrap_or(true);
                    if should_persist {
                        if let Err(error) =
                            nodex_persistence::save_token_state(&database, &snapshot).await
                        {
                            tracing::warn!(%error, mint_address = mint, "Could not persist confirmed trade activity timestamp");
                        } else {
                            last_activity_persisted.insert(mint.to_owned(), Instant::now());
                        }
                    }
                    let _ = events.send(json!({"type":"token_state_updated","data":snapshot}));
                }
                continue;
            }
            if kind == "token_rejected_cleanup" {
                // Legacy cleanup events must never erase a token's scan evidence.
                // Rejected and unverified launches are retained for operator debugging.
                tracing::warn!(
                    mint_address = mint,
                    "Ignoring legacy token cleanup request; token state and launch history are retained"
                );
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
                let mut diagnostic_patch = json!({"diagnostics":diagnostics});
                let severity = data
                    .get("severity")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("error")
                    .to_ascii_lowercase();
                if matches!(severity.as_str(), "error" | "critical" | "fatal") {
                    let previous_activity = registry
                        .get(mint)
                        .and_then(|snapshot| snapshot.data.get("activity").cloned())
                        .unwrap_or_else(|| json!({}));
                    diagnostic_patch["activity"] = json!({
                        "status": previous_activity.get("status").cloned().unwrap_or(json!("")),
                        "label": previous_activity.get("label").cloned().unwrap_or(json!("")),
                        "stage": previous_activity.get("stage").cloned().unwrap_or(data.get("stage").cloned().unwrap_or(json!("error"))),
                        "sub_event": data.get("message").cloned().unwrap_or(json!("An error occurred")),
                        "progress": previous_activity.get("progress").cloned().unwrap_or(serde_json::Value::Null),
                        "error": {
                            "severity": severity,
                            "stage": data.get("stage").cloned().unwrap_or(serde_json::Value::Null),
                            "provider": data.get("provider").cloned().unwrap_or(serde_json::Value::Null),
                            "message": data.get("message").cloned().unwrap_or(json!("An error occurred")),
                            "raw_response": data.get("raw_response").cloned().unwrap_or(serde_json::Value::Null),
                            "occurred_at": data.get("occurred_at").cloned().unwrap_or(json!(chrono::Utc::now().to_rfc3339()))
                        }
                    });
                }
                match registry.upsert_patch(mint, diagnostic_patch) {
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
            let current_security = registry
                .get(mint)
                .and_then(|snapshot| snapshot.data.get("security").cloned());
            let current_activity = registry
                .get(mint)
                .and_then(|snapshot| snapshot.data.get("activity").cloned());
            let current_market_subscription = registry
                .get(mint)
                .and_then(|snapshot| snapshot.data.get("market_subscription").cloned());
            let activity_patch = match kind {
                "token_scan_retry_scheduled" => Some(json!({
                    "status": "retrying",
                    "label": "Retrying",
                    "progress": 0,
                    "scan_attempt": data.get("scan_attempt").cloned().unwrap_or(json!(0)),
                    "stage": "retry_wait",
                    "sub_event": "Retrying security scan"
                })),
                "token_scan_started" => Some(json!({
                    "status": "scanning",
                    "label": "Scanning",
                    "progress": 0,
                    "scan_attempt": data.get("scan_attempt").cloned().unwrap_or(json!(0)),
                    "stage": "initializing",
                    "sub_event": "Scanning"
                })),
                "token_scan_progress" => Some(json!({
                    "status": "scanning",
                    "label": "Scanning",
                    "progress": data.get("progress").cloned().unwrap_or(json!(0)),
                    "scan_attempt": data.get("scan_attempt").cloned().unwrap_or(json!(0)),
                    "stage": data.get("stage").cloned().unwrap_or(json!("security_checks")),
                    "sub_event": "Scanning"
                })),
                "token_scan_completed" => {
                    let decision = data
                        .get("decision")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_ascii_uppercase();
                    let diagnostic_only = data
                        .get("diagnostic_only")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false);
                    let verification_gate_passed = data
                        .get("verification_gate_passed")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false);
                    if !diagnostic_only && verification_gate_passed {
                        Some(json!({
                            "status": "verified",
                            "label": "Verified",
                            "progress": 100,
                            "scan_attempt": data.get("scan_attempt").cloned().unwrap_or(json!(0)),
                            "stage": "security_complete",
                            "sub_event": "Getting market data"
                        }))
                    } else if decision == "REJECTED" {
                        Some(json!({
                            "status": "rejected",
                            "label": "Risk detected",
                            "progress": 100,
                            "scan_attempt": data.get("scan_attempt").cloned().unwrap_or(json!(0)),
                            "stage": "security_complete",
                            "sub_event": "Unverified"
                        }))
                    } else {
                        Some(json!({
                            "status": "unverified",
                            "label": "Unverified",
                            "progress": 100,
                            "scan_attempt": data.get("scan_attempt").cloned().unwrap_or(json!(0)),
                            "stage": "security_complete",
                            "sub_event": "Unverified"
                        }))
                    }
                }
                "token_scan_failed" => Some(json!({
                    "status": "failed",
                    "label": "Scan failed",
                    "progress": 0,
                    "scan_attempt": data.get("scan_attempt").cloned().unwrap_or(json!(0)),
                    "stage": data.get("stage").cloned().unwrap_or(json!("security_failed")),
                    "sub_event": "Security scan failed",
                    "error": {
                        "severity": "error",
                        "stage": data.get("stage").cloned().unwrap_or(json!("security_failed")),
                        "provider": data.get("provider").cloned().unwrap_or(serde_json::Value::Null),
                        "message": data.get("message").cloned().unwrap_or(json!("Security scan failed")),
                        "raw_response": data.get("error_details").cloned().unwrap_or(serde_json::Value::Null),
                        "occurred_at": data.get("received_at").cloned().unwrap_or(json!(chrono::Utc::now().to_rfc3339()))
                    }
                })),
                "token_monitoring_started" | "token_monitoring_update" => {
                    let security_is_verified = current_security.as_ref().is_some_and(|security| {
                        !security
                            .get("diagnostic_only")
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(true)
                            && core_verification_gate_passed(security)
                    });
                    security_is_verified.then(|| json!({
                        "status": "monitoring",
                        "label": "Monitoring",
                        "stage": data.get("stage").cloned().unwrap_or(json!("market_watch")),
                        "sub_event": data.get("message").cloned().unwrap_or(json!("Monitoring token conditions"))
                    }))
                }
                "token_monitoring_completed" => {
                    let was_monitoring = current_activity
                        .as_ref()
                        .and_then(|activity| activity.get("status"))
                        .and_then(serde_json::Value::as_str)
                        == Some("monitoring");
                    was_monitoring.then(|| json!({
                        "status": "monitoring_stopped",
                        "label": "Monitoring stopped",
                        "stage": data.get("stage").cloned().unwrap_or(json!("monitoring_complete")),
                        "sub_event": data.get("message").cloned().unwrap_or(json!("Token is no longer being monitored"))
                    }))
                }
                "market_subscriptions_removed" => {
                    let was_monitoring = current_activity
                        .as_ref()
                        .and_then(|activity| activity.get("status"))
                        .and_then(serde_json::Value::as_str)
                        == Some("monitoring");
                    was_monitoring.then(|| json!({
                        "status": "monitoring_stopped",
                        "label": "Monitoring stopped",
                        "stage": "market_feed_removed",
                        "sub_event": data.get("message").cloned().unwrap_or(json!("Market feed subscription was removed"))
                    }))
                }
                // A provider subscription acknowledgement is not market data. Keep the
                // verified badge while waiting for the first valid price update.
                "market_subscription_requested"
                | "market_subscription_subscribed"
                | "market_subscription_active" => {
                    let security_is_verified = current_security.as_ref().is_some_and(|security| {
                        !security
                            .get("diagnostic_only")
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(true)
                            && core_verification_gate_passed(security)
                    });
                    security_is_verified.then(|| {
                        json!({
                            "status": "verified",
                            "label": "Verified",
                            "stage": "waiting_for_market_data",
                            "sub_event": "Getting market data"
                        })
                    })
                }
                "market_subscription_waiting" => {
                    let security_is_verified = current_security.as_ref().is_some_and(|security| {
                        !security
                            .get("diagnostic_only")
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(true)
                            && core_verification_gate_passed(security)
                    });
                    security_is_verified.then(|| {
                        json!({
                            "status":"verified",
                            "label":"Verified",
                            "stage":"waiting_for_market_data",
                            "sub_event":"Getting market data"
                        })
                    })
                }
                "market_subscription_failed" => {
                    let security_is_verified = current_security.as_ref().is_some_and(|security| {
                        !security
                            .get("diagnostic_only")
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(true)
                            && core_verification_gate_passed(security)
                    });
                    security_is_verified.then(|| {
                        json!({
                            "status":"verified",
                            "label":"Verified",
                            "stage":"market_data_unavailable",
                            "sub_event":"Market data unavailable"
                        })
                    })
                }
                "paper_entry_evaluation" => {
                    let is_monitoring = current_activity
                        .as_ref()
                        .and_then(|activity| activity.get("status"))
                        .and_then(serde_json::Value::as_str)
                        == Some("monitoring");
                    is_monitoring.then(|| paper_entry_activity_patch(&data))
                }
                "paper_buy_started" => Some(json!({
                    "status": "monitoring",
                    "label": "Monitoring",
                    "stage": data.get("stage").cloned().unwrap_or(json!("paper_order_start")),
                    "sub_event": "Paper buy in progress"
                })),
                "paper_buy_executed" => Some(json!({
                    "status": "monitoring",
                    "label": "Monitoring",
                    "stage": "position_open",
                    "sub_event": "Paper position opened"
                })),
                "paper_buy_failed" => Some(json!({
                    "status": "monitoring",
                    "label": "Monitoring",
                    "stage": data.get("stage").cloned().unwrap_or(json!("entry_failed")),
                    "sub_event": "Paper buy failed",
                    "error": {
                        "severity": "error",
                        "stage": data.get("stage").cloned().unwrap_or(json!("entry_failed")),
                        "provider": data.get("provider").cloned().unwrap_or(serde_json::Value::Null),
                        "message": data.get("message").cloned().or_else(|| data.get("reason").cloned()).unwrap_or(json!("Paper buy failed")),
                        "raw_response": data.get("error_details").cloned().unwrap_or(serde_json::Value::Null),
                        "occurred_at": data.get("received_at").cloned().unwrap_or(json!(chrono::Utc::now().to_rfc3339()))
                    }
                })),
                "paper_sell_failed" => Some(json!({
                    "status": "sell_failed",
                    "label": "Sell failed",
                    "stage": data.get("stage").cloned().unwrap_or(json!("exit_failed")),
                    "sub_event": data.get("reason").cloned().or_else(|| data.get("message").cloned()).unwrap_or(json!("Paper sell failed")),
                    "error": {
                        "severity": "error",
                        "stage": data.get("stage").cloned().unwrap_or(json!("exit_failed")),
                        "provider": data.get("provider").cloned().unwrap_or(serde_json::Value::Null),
                        "message": data.get("message").cloned().or_else(|| data.get("reason").cloned()).unwrap_or(json!("Paper sell failed")),
                        "raw_response": data.get("error_details").cloned().unwrap_or(serde_json::Value::Null),
                        "occurred_at": data.get("received_at").cloned().unwrap_or(json!(chrono::Utc::now().to_rfc3339()))
                    }
                })),
                "strategy_rejected_token" => {
                    let is_monitoring = current_activity
                        .as_ref()
                        .and_then(|activity| activity.get("status"))
                        .and_then(serde_json::Value::as_str)
                        == Some("monitoring");
                    is_monitoring.then(|| {
                        json!({
                            "status": "monitoring",
                            "label": "Monitoring",
                            "stage": data.get("stage").cloned().unwrap_or(json!("strategy_checks")),
                            "sub_event": "Strategy rejected"
                        })
                    })
                }
                "paper_sell_executed" => Some(json!({
                    "status": "closed",
                    "label": "Position closed",
                    "stage": "position_closed",
                    "sub_event": data.get("message").cloned().unwrap_or(json!("Paper position closed"))
                })),
                _ => None,
            };
            let mut activity_patch = activity_patch;
            if let Some(activity) = activity_patch.as_mut() {
                if let Some(error) = current_activity
                    .as_ref()
                    .and_then(|value| value.get("error"))
                {
                    if activity.get("error").is_none() {
                        activity["error"] = error.clone();
                    }
                }
                activity["updated_at"] = data
                    .get("received_at")
                    .cloned()
                    .unwrap_or(json!(chrono::Utc::now().to_rfc3339()));
            }
            let activity_patch = activity_patch.filter(|incoming| {
                let rank = |status: &str| match status {
                    "scanning" | "retrying" => 1,
                    "verified" => 2,
                    "monitoring" => 3,
                    "evaluating" | "waiting_for_entry" | "entry_ready" => 4,
                    "executing_buy" => 5,
                    "monitoring_stopped" => 3,
                    "rejected" | "unverified" | "failed" | "bought_paper" | "buy_failed"
                    | "sell_failed" | "strategy_rejected" | "closed" => 6,
                    _ => 0,
                };
                let current_status = current_activity
                    .as_ref()
                    .and_then(|activity| activity.get("status"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                let incoming_status = incoming
                    .get("status")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                let current_rank = rank(current_status);
                let incoming_rank = rank(incoming_status);
                let current_progress = current_activity
                    .as_ref()
                    .and_then(|activity| activity.get("progress"))
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
                let incoming_progress = incoming
                    .get("progress")
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(current_progress);
                let current_attempt = current_activity
                    .as_ref()
                    .and_then(|activity| activity.get("scan_attempt"))
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                let incoming_attempt = incoming
                    .get("scan_attempt")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                // A newer scan attempt may legitimately move a token back to Scanning.
                incoming_attempt > current_attempt
                    || incoming_rank > current_rank
                    || incoming_rank == current_rank && incoming_progress >= current_progress
            });
            if activity_patch.is_none()
                && matches!(
                    kind,
                    "token_scan_retry_scheduled"
                        | "token_scan_started"
                        | "token_scan_progress"
                        | "token_scan_completed"
                        | "token_scan_failed"
                        | "token_monitoring_started"
                        | "token_monitoring_update"
                        | "token_monitoring_completed"
                        | "paper_entry_evaluation"
                        | "paper_buy_started"
                        | "paper_buy_executed"
                        | "paper_buy_failed"
                        | "paper_sell_executed"
                        | "paper_sell_failed"
                        | "strategy_rejected_token"
                )
            {
                continue;
            }
            let mut domain_patch = match kind {
                "token_scan_started" | "token_scan_progress" => {
                    scan_progress_security_patch(current_security.as_ref(), &data)
                }
                "creator_history_update" => json!({
                    "security": {
                        "creator_history": data
                            .get("history")
                            .cloned()
                            .unwrap_or(serde_json::Value::Null)
                    }
                }),
                "market_price_update" => {
                    let mut patch = json!({
                        "market": data,
                        "last_activity_at": chrono::Utc::now().to_rfc3339()
                    });
                    let has_valid_market_price = data
                        .get("price_sol")
                        .and_then(serde_json::Value::as_f64)
                        .is_some_and(|price| price.is_finite() && price > 0.0)
                        || data
                            .get("price_usd")
                            .and_then(serde_json::Value::as_f64)
                            .is_some_and(|price| price.is_finite() && price > 0.0)
                        || data
                            .get("current_price_usd")
                            .and_then(serde_json::Value::as_f64)
                            .is_some_and(|price| price.is_finite() && price > 0.0);
                    let security_is_verified = current_security.as_ref().is_some_and(|security| {
                        !security
                            .get("diagnostic_only")
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(true)
                            && core_verification_gate_passed(security)
                    });
                    if has_valid_market_price && security_is_verified {
                        patch["activity"] = json!({
                            "status":"monitoring",
                            "label":"Monitoring",
                            "stage":"market_watch",
                            "sub_event":"Monitoring market activity",
                            "updated_at":chrono::Utc::now().to_rfc3339()
                        });
                    }
                    let mut subscription = current_market_subscription.unwrap_or_else(|| json!({}));
                    let subscription_status = subscription
                        .get("status")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default();
                    if matches!(subscription_status, "pending" | "subscribed") {
                        subscription["status"] = json!("active");
                        subscription["reason"] = serde_json::Value::Null;
                        subscription["last_market_update_at"] = data
                            .get("received_at")
                            .cloned()
                            .unwrap_or(json!(chrono::Utc::now().to_rfc3339()));
                    }
                    patch["market_subscription"] = subscription;
                    patch
                }
                "pumpswap_volume_update" => json!({
                    "statistics": {"pumpswap_volume": data}
                }),
                "token_scan_retry_scheduled"
                | "token_scan_completed"
                | "token_scan_failed"
                | "token_monitoring_started"
                | "token_monitoring_update"
                | "token_monitoring_completed"
                | "paper_entry_evaluation"
                | "paper_buy_started"
                | "paper_buy_executed"
                | "paper_buy_failed"
                | "paper_sell_executed"
                | "paper_sell_failed"
                | "strategy_rejected_token" => {
                    json!({"activity": activity_patch.clone().unwrap_or_else(|| json!({}))})
                }
                "market_subscription_requested"
                | "market_subscription_subscribed"
                | "market_subscription_waiting"
                | "market_subscription_active"
                | "market_subscription_failed"
                | "market_subscriptions_removed" => {
                    json!({"market_subscription": data})
                }
                _ => continue,
            };
            if let Some(activity) = activity_patch {
                domain_patch["activity"] = activity;
            }
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
                    } else {
                        let should_persist = last_activity_persisted
                            .get(mint)
                            .map(|last| last.elapsed() >= Duration::from_secs(60))
                            .unwrap_or(true);
                        if should_persist {
                            if let Err(error) =
                                nodex_persistence::save_token_state(&database, &snapshot).await
                            {
                                tracing::warn!(%error, mint_address = mint, "Could not persist market activity timestamp");
                            } else {
                                last_activity_persisted.insert(mint.to_owned(), Instant::now());
                            }
                        }
                    }
                    if kind == "market_price_update" {
                        if let Some(observations) = trade_observations.get_mut(mint) {
                            let now_ms = chrono::Utc::now().timestamp_millis();
                            while observations.front().is_some_and(|trade| {
                                now_ms.saturating_sub(trade.timestamp_ms) > 300_000
                            }) {
                                observations.pop_front();
                            }
                        }
                        let empty_observations = VecDeque::new();
                        let observations =
                            trade_observations.get(mint).unwrap_or(&empty_observations);
                        let venue = data
                            .get("venue")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default();
                        let quote_is_sol = match venue {
                            "pump_fun_bonding_curve" => true,
                            "pump_swap" => data
                                .get("pool_address")
                                .and_then(serde_json::Value::as_str)
                                .is_some_and(|pool| pool_quote_is_sol.contains(pool)),
                            _ => false,
                        };
                        if market_quote_is_fresh(&data) {
                            paper_trading::evaluate_and_execute_paper_entry(
                                mint,
                                &data,
                                observations,
                                quote_is_sol,
                                &registry,
                                &database,
                                &profile,
                                &portfolio_positions,
                                &wallet,
                                &sol_price,
                                &control,
                                &trading_settings,
                                &events,
                                &mut last_evaluation_signatures,
                                &mut last_entry_attempts,
                            )
                            .await;
                        }
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
                        && market_quote_is_fresh(&data)
                    {
                        if let Some(price_sol) =
                            data.get("price_sol").and_then(serde_json::Value::as_f64)
                        {
                            // Market data owns token prices. This subsystem only values open
                            // positions from the incoming authoritative market tick.
                            let price_snapshot = sol_price.read().await.clone();
                            let mut positions = portfolio_positions.write().await;
                            let mut triggered_positions = Vec::new();
                            let mut pending_exit_positions = Vec::new();
                            let mut updated_positions = Vec::new();
                            for position in positions.iter_mut().filter(|position| {
                                position
                                    .get("mint_address")
                                    .and_then(serde_json::Value::as_str)
                                    == Some(mint)
                                    && position.get("status").and_then(serde_json::Value::as_str)
                                        == Some("open")
                            }) {
                                portfolio::position_valuation::mark_position(
                                    position,
                                    price_sol,
                                    &price_snapshot,
                                );
                                let exit_was_requested = position
                                    .get("exit_requested")
                                    .and_then(serde_json::Value::as_bool)
                                    == Some(true);
                                if trading_decisions::position_exit::evaluate_exit(
                                    position, price_sol,
                                ) {
                                    triggered_positions.push(position.clone());
                                } else if exit_was_requested {
                                    pending_exit_positions.push(position.clone());
                                }
                                updated_positions.push(position.clone());
                            }
                            let position_snapshot = positions.clone();
                            drop(positions);
                            let wallet_snapshot = wallet.read().await.clone();
                            let portfolio_snapshot =
                                portfolio::calculator::calculate_portfolio_snapshot(
                                    &wallet_snapshot,
                                    &price_snapshot,
                                    &position_snapshot,
                                );

                            for triggered in &triggered_positions {
                                portfolio::event_publisher::publish_exit_triggered(
                                    &events, triggered,
                                );
                                let position_id = triggered
                                    .get("position_id")
                                    .and_then(serde_json::Value::as_str);
                                let triggered_mint = triggered
                                    .get("mint_address")
                                    .and_then(serde_json::Value::as_str);
                                if let (Some(position_id), Some(triggered_mint)) =
                                    (position_id, triggered_mint)
                                {
                                    if let Err(error) = nodex_persistence::save_portfolio_position(
                                        &database,
                                        position_id,
                                        triggered_mint,
                                        "open",
                                        triggered,
                                    )
                                    .await
                                    {
                                        tracing::error!(%error, position_id, "Could not persist independent paper exit trigger");
                                    }
                                }
                            }
                            for position in &updated_positions {
                                portfolio::event_publisher::publish_position_update(
                                    &events, position,
                                );
                            }

                            if let Err(error) = nodex_persistence::save_portfolio_snapshot(
                                &database,
                                &portfolio_snapshot,
                            )
                            .await
                            {
                                tracing::error!(%error, "Could not persist portfolio snapshot after token price update");
                            } else {
                                let profile_snapshot = {
                                    let mut current_profile = profile.write().await;
                                    if let Some(object) = current_profile.as_object_mut() {
                                        object
                                            .insert("paper_wallet".into(), json!(wallet_snapshot));
                                        object.insert("sol_price".into(), json!(price_snapshot));
                                        object
                                            .insert("portfolio".into(), portfolio_snapshot.clone());
                                    }
                                    current_profile.clone()
                                };
                                if let Err(error) =
                                    nodex_persistence::save_profile(&database, &profile_snapshot)
                                        .await
                                {
                                    tracing::error!(%error, "Could not persist profile after portfolio price update");
                                }
                                portfolio::event_publisher::publish_profile_and_summary(
                                    &events,
                                    profile_snapshot,
                                    portfolio_snapshot,
                                );
                            }

                            // Exit requests are not complete until the simulated
                            // sale is atomically settled in the paper wallet and SQLite.
                            for pending in triggered_positions
                                .iter()
                                .chain(pending_exit_positions.iter())
                            {
                                if let Some(position_id) = pending
                                    .get("position_id")
                                    .and_then(serde_json::Value::as_str)
                                {
                                    if let Err(error) = settle_triggered_paper_exit(
                                        &database,
                                        &wallet,
                                        &sol_price,
                                        &portfolio_positions,
                                        &profile,
                                        &events,
                                        position_id,
                                    )
                                    .await
                                    {
                                        tracing::error!(%error, position_id, "Paper exit settlement failed; position remains flagged for retry");
                                        let failed_snapshot = {
                                            let mut positions = portfolio_positions.write().await;
                                            positions
                                                .iter_mut()
                                                .find(|position| {
                                                    position
                                                        .get("position_id")
                                                        .and_then(serde_json::Value::as_str)
                                                        == Some(position_id)
                                                        || position
                                                            .get("id")
                                                            .and_then(serde_json::Value::as_str)
                                                            == Some(position_id)
                                                })
                                                .and_then(|position| {
                                                    if position
                                                        .get("settlement_failure_notified")
                                                        .and_then(serde_json::Value::as_bool)
                                                        == Some(true)
                                                    {
                                                        None
                                                    } else {
                                                        if let Some(object) =
                                                            position.as_object_mut()
                                                        {
                                                            object.insert(
                                                                "settlement_failure_notified"
                                                                    .into(),
                                                                json!(true),
                                                            );
                                                        }
                                                        Some(position.clone())
                                                    }
                                                })
                                        };
                                        if let Some(failed) = failed_snapshot {
                                            let mint = failed
                                                .get("mint_address")
                                                .and_then(serde_json::Value::as_str)
                                                .unwrap_or_default();
                                            if !mint.is_empty() {
                                                if let Err(persist_error) =
                                                    nodex_persistence::save_portfolio_position(
                                                        &database,
                                                        position_id,
                                                        mint,
                                                        "open",
                                                        &failed,
                                                    )
                                                    .await
                                                {
                                                    tracing::warn!(%persist_error, position_id, "Could not persist paper exit failure notification marker");
                                                }
                                            }
                                            let _ = events.send(json!({
                                                "type": "paper_sell_failed",
                                                "data": {
                                                    "position": failed,
                                                    "position_id": position_id,
                                                    "mint_address": failed.get("mint_address").or_else(|| failed.get("mint")).cloned().unwrap_or(serde_json::Value::Null),
                                                    "reason": error,
                                                    "retrying": true,
                                                    "paper_only": true,
                                                    "execution_mode": "simulated"
                                                }
                                            }));
                                        }
                                    }
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

/// Retry persisted exit requests independently of market ticks. This closes
/// the restart gap and ensures a failed settlement can complete even if the
/// token's next price update is delayed.
fn start_pending_exit_recovery(
    database: sqlx::SqlitePool,
    wallet: Arc<RwLock<PaperWallet>>,
    sol_price: Arc<RwLock<SolPriceSnapshot>>,
    positions: Arc<RwLock<Vec<serde_json::Value>>>,
    profile: Arc<RwLock<serde_json::Value>>,
    events: broadcast::Sender<serde_json::Value>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let pending_ids: Vec<String> = positions
                .read()
                .await
                .iter()
                .filter(|position| {
                    position.get("status").and_then(serde_json::Value::as_str) == Some("open")
                        && position
                            .get("exit_requested")
                            .and_then(serde_json::Value::as_bool)
                            == Some(true)
                })
                .filter_map(|position| {
                    position
                        .get("position_id")
                        .or_else(|| position.get("id"))
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                })
                .collect();
            for position_id in pending_ids {
                if let Err(error) = settle_triggered_paper_exit(
                    &database,
                    &wallet,
                    &sol_price,
                    &positions,
                    &profile,
                    &events,
                    &position_id,
                )
                .await
                {
                    tracing::warn!(%error, %position_id, "Pending paper exit recovery will retry");
                }
            }
        }
    });
}

/// Complete one triggered full-position paper sale. Wallet, ledger and
/// closed-position state are committed in one SQLite transaction before the
/// in-memory state and browser events are updated.
async fn settle_triggered_paper_exit(
    database: &sqlx::SqlitePool,
    wallet: &Arc<RwLock<PaperWallet>>,
    sol_price: &Arc<RwLock<SolPriceSnapshot>>,
    positions: &Arc<RwLock<Vec<serde_json::Value>>>,
    profile: &Arc<RwLock<serde_json::Value>>,
    events: &broadcast::Sender<serde_json::Value>,
    position_id: &str,
) -> Result<(), String> {
    let mut wallet_guard = wallet.write().await;
    let mut positions_guard = positions.write().await;
    let index = positions_guard
        .iter()
        .position(|position| {
            position
                .get("position_id")
                .and_then(serde_json::Value::as_str)
                == Some(position_id)
                || position.get("id").and_then(serde_json::Value::as_str) == Some(position_id)
        })
        .ok_or_else(|| "Triggered paper position no longer exists.".to_string())?;
    let current = positions_guard[index].clone();
    if current.get("status").and_then(serde_json::Value::as_str) != Some("open")
        || current
            .get("exit_requested")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
    {
        return Ok(());
    }

    let mint = current
        .get("mint_address")
        .and_then(serde_json::Value::as_str)
        .or_else(|| current.get("mint").and_then(serde_json::Value::as_str))
        .ok_or_else(|| "Triggered paper position has no mint address.".to_string())?
        .to_string();
    let exit_price_sol = current
        .get("exit_trigger_price_sol")
        .and_then(serde_json::Value::as_f64)
        .or_else(|| {
            current
                .get("current_price_sol")
                .and_then(serde_json::Value::as_f64)
        })
        .filter(|value| value.is_finite() && *value > 0.0)
        .ok_or_else(|| "Triggered paper exit has no valid market price.".to_string())?;
    let quantity = current
        .get("token_quantity")
        .or_else(|| current.get("quantity"))
        .and_then(serde_json::Value::as_f64)
        .filter(|value| value.is_finite() && *value > 0.0)
        .ok_or_else(|| "Triggered paper exit has no remaining token quantity.".to_string())?;
    let cost_basis_sol = current
        .get("cost_basis_sol")
        .or_else(|| current.get("invested_sol"))
        .and_then(serde_json::Value::as_f64)
        .filter(|value| value.is_finite() && *value > 0.0)
        .ok_or_else(|| "Triggered paper exit has no valid cost basis.".to_string())?;
    let proceeds_sol = quantity * exit_price_sol;
    let invested_lamports_f64 = cost_basis_sol * LAMPORTS_PER_SOL as f64;
    let proceeds_lamports_f64 = proceeds_sol * LAMPORTS_PER_SOL as f64;
    if !proceeds_sol.is_finite()
        || !invested_lamports_f64.is_finite()
        || !proceeds_lamports_f64.is_finite()
        || invested_lamports_f64 < 1.0
        || proceeds_lamports_f64 < 0.0
        || invested_lamports_f64 > u64::MAX as f64
        || proceeds_lamports_f64 > u64::MAX as f64
    {
        return Err(
            "Paper exit proceeds or cost basis exceed the supported settlement range.".into(),
        );
    }
    let invested_lamports = invested_lamports_f64.round() as u64;
    let proceeds_lamports = proceeds_lamports_f64.round() as u64;
    let mut next_wallet = wallet_guard.clone();
    let ledger = next_wallet
        .settle_full_sale(&mint, invested_lamports, proceeds_lamports)
        .map_err(|error| format!("Paper wallet rejected full-sale settlement: {error}"))?;

    let quote = sol_price.read().await.clone();
    let usd_quote_available = quote.price_usd.is_finite() && quote.price_usd > 0.0;
    let realized_pnl_sol = proceeds_sol - cost_basis_sol;
    let realized_pnl_pct = realized_pnl_sol / cost_basis_sol * 100.0;
    let initial_investment_usd = current
        .get("initial_investment_usd")
        .or_else(|| current.get("cost_basis_usd"))
        .and_then(serde_json::Value::as_f64)
        .filter(|value| value.is_finite() && *value > 0.0)
        .or_else(|| {
            current
                .get("entry_sol_price_usd")
                .and_then(serde_json::Value::as_f64)
                .filter(|value| value.is_finite() && *value > 0.0)
                .map(|entry_sol_usd| cost_basis_sol * entry_sol_usd)
        })
        .unwrap_or(0.0);
    let exit_proceeds_usd = if usd_quote_available {
        proceeds_sol * quote.price_usd
    } else {
        0.0
    };
    let realized_pnl_usd = if usd_quote_available {
        exit_proceeds_usd - initial_investment_usd
    } else {
        0.0
    };
    let exit_price_usd = if usd_quote_available {
        exit_price_sol * quote.price_usd
    } else {
        0.0
    };
    let mut closed = current.clone();
    if let Some(object) = closed.as_object_mut() {
        object.insert("status".into(), json!("closed"));
        object.insert("original_token_quantity".into(), json!(quantity));
        object.insert("token_quantity".into(), json!(0.0));
        object.insert("quantity".into(), json!(0.0));
        object.insert("remaining_token_quantity".into(), json!(0.0));
        object.insert("current_value_sol".into(), json!(0.0));
        object.insert("current_value_usd".into(), json!(0.0));
        object.insert("current_position_value_usd".into(), json!(0.0));
        object.insert("unrealized_pnl_sol".into(), json!(0.0));
        object.insert("unrealized_pnl_usd".into(), json!(0.0));
        object.insert("pnl_sol".into(), json!(realized_pnl_sol));
        object.insert("pnl_usd".into(), json!(realized_pnl_usd));
        object.insert("pnl_pct".into(), json!(realized_pnl_pct));
        object.insert("realized_pnl_sol".into(), json!(realized_pnl_sol));
        object.insert("realized_pnl_usd".into(), json!(realized_pnl_usd));
        object.insert("realized_pnl_pct".into(), json!(realized_pnl_pct));
        object.insert("exit_price_sol".into(), json!(exit_price_sol));
        object.insert("exit_price_usd".into(), json!(exit_price_usd));
        object.insert("exit_proceeds_sol".into(), json!(proceeds_sol));
        object.insert("exit_proceeds_usd".into(), json!(exit_proceeds_usd));
        object.insert("exit_proceeds_lamports".into(), json!(proceeds_lamports));
        object.insert("closed_at".into(), json!(chrono::Utc::now().to_rfc3339()));
        object.insert(
            "exit_reason".into(),
            current
                .get("exit_trigger_reason")
                .cloned()
                .unwrap_or_else(|| json!("risk_policy")),
        );
        object.insert("paper_only".into(), json!(true));
    }
    nodex_persistence::commit_paper_sell(
        database,
        &next_wallet,
        &ledger,
        position_id,
        &mint,
        &closed,
    )
    .await
    .map_err(|error| format!("Atomic paper sell persistence failed: {error}"))?;

    *wallet_guard = next_wallet.clone();
    positions_guard[index] = closed.clone();
    let position_snapshot = positions_guard.clone();
    drop(positions_guard);
    drop(wallet_guard);

    let portfolio = calculate_portfolio_snapshot(&next_wallet, &quote, &position_snapshot);
    let profile_snapshot = {
        let mut current_profile = profile.write().await;
        if let Some(object) = current_profile.as_object_mut() {
            object.insert("paper_wallet".into(), json!(next_wallet.clone()));
            object.insert("sol_price".into(), json!(quote));
            object.insert("portfolio".into(), portfolio.clone());
        }
        current_profile.clone()
    };
    if let Err(error) = nodex_persistence::save_portfolio_snapshot(database, &portfolio).await {
        tracing::error!(%error, position_id, "Paper sale settled but portfolio snapshot persistence failed");
    }
    if let Err(error) = nodex_persistence::save_profile(database, &profile_snapshot).await {
        tracing::error!(%error, position_id, "Paper sale settled but profile persistence failed");
    }
    let _ = events.send(json!({
        "type":"paper_sell_executed",
        "data":{
            "mint_address":mint,
            "position":closed.clone(),
            "ledger":ledger,
            "wallet":next_wallet.clone(),
            "exit_reason":closed.get("exit_reason"),
            "paper_only":true,
            "execution_mode":"simulated",
            "full_exit":true
        }
    }));
    portfolio::event_publisher::publish_position_update(events, &closed);
    let _ = events.send(json!({"type":"wallet_updated","data":{"wallet":next_wallet}}));
    portfolio::event_publisher::publish_profile_and_summary(events, profile_snapshot, portfolio);
    Ok(())
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
    if url.scheme() != "https"
        || unsafe_host
        || !url.username().is_empty()
        || url.password().is_some()
    {
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
                result.logo_url = take_string(content.pointer("/links/image"))
                    .and_then(|candidate| normalize_image_url(&candidate));
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
                    .and_then(|file| take_string(file.get("uri")))
                    .and_then(|candidate| normalize_image_url(&candidate));
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
                                        if result.logo_url.is_none() {
                                            result.logo_url=value.get("image").or_else(||value.get("image_url")).and_then(serde_json::Value::as_str).map(str::trim).filter(|v|!v.is_empty()).and_then(normalize_image_url);
                                        }
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
                                            .or_else(|| take(pair.pointer("/info/header")))
                                            .and_then(|candidate| normalize_image_url(&candidate));
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

/// Decode actual Pump.fun TradeEvent records directly from confirmed WebSocket logs.
/// This path does not infer trades from price ticks and does not fetch each transaction.
fn start_pump_fun_trade_processor(
    mut receiver: broadcast::Receiver<serde_json::Value>,
    events: broadcast::Sender<serde_json::Value>,
    program_id: String,
) {
    tokio::spawn(async move {
        let mut seen = HashSet::<String>::new();
        loop {
            let notification = match receiver.recv().await {
                Ok(value) => value,
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    tracing::warn!(
                        dropped_notifications = count,
                        "Pump.fun trade activity consumer lagged"
                    );
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
                    != Some(program_id.as_str())
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
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
            else {
                continue;
            };
            let logs = value
                .get("logs")
                .and_then(serde_json::Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(str::to_owned)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if logs.is_empty() {
                continue;
            }
            let decoded = match decode_pump_fun_trade_logs(&logs, &program_id) {
                Ok(trades) => trades,
                Err(error) => {
                    tracing::warn!(%error, signature, "Pump.fun TradeEvent decoding failed");
                    continue;
                }
            };
            if decoded.is_empty() || !seen.insert(signature.clone()) {
                continue;
            }
            if seen.len() > 20_000 {
                seen.clear();
                seen.insert(signature.clone());
            }
            let slot = notification
                .pointer("/params/result/context/slot")
                .and_then(serde_json::Value::as_u64);
            for trade in decoded {
                let _ = events.send(json!({
                    "type":"pump_fun_trade_activity",
                    "data":{
                        "signature":signature,
                        "slot":slot,
                        "mint_address":trade.mint_address,
                        "trader_address":trade.trader_address,
                        "side":if trade.is_buy {"buy"} else {"sell"},
                        "sol_amount_lamports":trade.sol_amount_lamports.to_string(),
                        "token_amount_raw":trade.token_amount_raw.to_string(),
                        "timestamp":trade.timestamp,
                        "venue":"pump_fun_bonding_curve",
                        "evidence":"confirmed_program_scoped_trade_event",
                        "received_at":chrono::Utc::now().to_rfc3339()
                    }
                }));
            }
        }
    });
}

/// Consume confirmed PumpSwap logs and emit only trade events decoded while
/// the PumpSwap program is the active invocation. This is observation only:
/// it never signs, submits, or simulates a transaction.
fn start_pumpswap_activity_processor(
    mut receiver: broadcast::Receiver<serde_json::Value>,
    events: broadcast::Sender<serde_json::Value>,
) {
    // logsNotification already contains the confirmed transaction's program
    // logs. Decode those directly instead of issuing one getTransaction RPC
    // request per PumpSwap swap; the old fan-out was exhausting provider quotas.
    let mut seen = HashSet::<String>::new();
    tokio::spawn(async move {
        loop {
            let notification = match receiver.recv().await {
                Ok(value) => value,
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    tracing::warn!(
                        dropped_notifications = count,
                        "PumpSwap activity notification reader lagged"
                    );
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
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
            else {
                continue;
            };
            if seen.contains(&signature) {
                continue;
            }
            let logs = value
                .get("logs")
                .and_then(serde_json::Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(str::to_owned)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if logs.is_empty() {
                continue;
            }
            let decoded = match decode_pump_swap_activity_logs(&logs, PUMPSWAP_PROGRAM_ID) {
                Ok(events) => events,
                Err(error) => {
                    tracing::warn!(%error, signature, "PumpSwap activity event decoding failed");
                    continue;
                }
            };
            seen.insert(signature.clone());
            if seen.len() > 20_000 {
                seen.clear();
                seen.insert(signature.clone());
            }
            let slot = notification
                .pointer("/params/result/context/slot")
                .and_then(serde_json::Value::as_u64);
            for activity in decoded {
                let (kind, timestamp, pool, user, base_amount, quote_amount) = match activity {
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
                    PumpSwapActivityEvent::Deposit { .. }
                    | PumpSwapActivityEvent::Withdraw { .. } => continue,
                };
                let _ = events.send(json!({
                    "type":"pumpswap_trade_activity",
                    "data":{
                        "signature":signature,
                        "slot":slot,
                        "event_type":kind,
                        "timestamp":timestamp,
                        "pool_address":pool,
                        "trader_address":user,
                        "base_amount_raw":base_amount.to_string(),
                        "quote_amount_raw":quote_amount.to_string(),
                        "evidence":"confirmed_program_scoped_logs_notification",
                        "received_at":chrono::Utc::now().to_rfc3339()
                    }
                }));
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
    let sol_price_usd = state.sol_price.read().await.price_usd;
    let bot_config = main_bot_config_from_settings(&settings, sol_price_usd);
    Json(json!({
        "config": settings,
        "main_bot_config": bot_config,
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
    if settings.execution_enabled {
        let control = state.control.read().await;
        // The connected executor is deliberately paper-only. Enabling this
        // switch is allowed only while the paper mode is active; real mode is
        // rejected by the engine-control endpoint and has no signing adapter.
        if control.mode != "paper"
            || !control.available_modes.paper
            || control.real_trading_available
        {
            return Err((
                StatusCode::CONFLICT,
                "Paper execution can only be enabled while paper mode is active. Live trading remains unavailable.".into(),
            ));
        }
    }
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
    let sol_price_usd = state.sol_price.read().await.price_usd;
    let bot_config = main_bot_config_from_settings(&settings, sol_price_usd);
    Ok(Json(
        json!({"status":"ok","config":settings,"main_bot_config":bot_config}),
    ))
}

async fn update_paper_execution(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    verify_operator_key(&state, &headers)?;
    let object = payload.as_object().ok_or((
        StatusCode::BAD_REQUEST,
        "Paper execution update must be a JSON object.".to_string(),
    ))?;
    if object.len() != 1 || !object.contains_key("enabled") {
        return Err((
            StatusCode::BAD_REQUEST,
            "Only the enabled boolean can be changed through this endpoint.".to_string(),
        ));
    }
    let enabled = payload
        .get("enabled")
        .and_then(serde_json::Value::as_bool)
        .ok_or((
            StatusCode::BAD_REQUEST,
            "The enabled field must be a boolean.".to_string(),
        ))?;

    let control = state.control.read().await;
    if enabled
        && (control.mode != "paper"
            || !control.available_modes.paper
            || control.real_trading_available)
    {
        return Err((
            StatusCode::CONFLICT,
            "Paper execution can only be enabled while paper mode is active. Live trading remains unavailable.".into(),
        ));
    }
    drop(control);

    // Hold the settings write lock through persistence so a concurrent full
    // trading-config update cannot be overwritten by this single-field change.
    let mut settings_guard = state.trading_settings.write().await;
    let mut settings = settings_guard.clone();
    settings.execution_enabled = enabled;
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
    .map_err(|error| (
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("Could not persist paper execution setting: {error}"),
    ))?;

    *settings_guard = settings.clone();
    drop(settings_guard);
    let _ = state
        .database_changes
        .send(json!({"type":"trading_config_updated","data":settings}))
        .await;
    Ok(Json(json!({
        "status":"ok",
        "config":settings,
        "mode":"paper",
        "real_trading_available":false
    })))
}

async fn patch_runtime_config(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(patch): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    verify_operator_key(&state, &headers)?;
    let patch_object = patch.as_object().ok_or((
        StatusCode::BAD_REQUEST,
        "Configuration patch must be a JSON object.".to_string(),
    ))?;
    const ALLOWED: &[&str] = &[
        "min_liquidity_usd",
        "max_entry_price_usd",
        "max_investment_per_token_usd",
        // Accept legacy clients during the USD position-sizing migration.
        "max_investment_per_token_sol",
        "max_simultaneous_positions",
        "min_safety_score",
        "max_slippage_pct",
        "stop_loss_pct",
        "take_profit_pct",
        "min_5m_buy_volume_usd",
        "max_top10_holder_concentration_pct",
        "auto_buy_enabled",
        "auto_recover_token_account_rent",
        "trailing_stop_enabled",
        "trailing_stop_callback_pct",
        "blacklist_creators_with_prior_rugs",
        "paper_starting_balance",
        "paper_starting_balance_currency",
        "diagnostic_mode",
        "block_unverified_trades",
        "execution_enabled",
        "whitelist_only_mode",
        "preferred_owners",
    ];
    if let Some(unknown) = patch_object
        .keys()
        .find(|key| !ALLOWED.contains(&key.as_str()))
    {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("Unknown configuration field: {unknown}"),
        ));
    }
    let current = state.trading_settings.read().await.clone();
    let mut merged = serde_json::to_value(&current)
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let target = merged
        .as_object_mut()
        .expect("TradingSettings serializes as an object");
    for (key, value) in patch_object {
        target.insert(key.clone(), value.clone());
    }
    let candidate: TradingSettings = serde_json::from_value(merged).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            format!("Invalid configuration: {error}"),
        )
    })?;
    candidate
        .validate()
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    if candidate.execution_enabled {
        let control = state.control.read().await;
        if control.mode != "paper"
            || !control.available_modes.paper
            || control.real_trading_available
        {
            return Err((
                StatusCode::CONFLICT,
                "Automatic execution can only be enabled for paper simulation. Live trading is unavailable.".into(),
            ));
        }
    }

    let balance_changed = candidate.paper_starting_balance != current.paper_starting_balance
        || candidate.paper_starting_balance_currency != current.paper_starting_balance_currency;
    let mut wallet_guard = state.wallet.write().await;
    let mut next_wallet = wallet_guard.clone();
    if balance_changed {
        let ledger_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM paper_ledger")
            .fetch_one(&state.database)
            .await
            .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        let position_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM portfolio_positions")
            .fetch_one(&state.database)
            .await
            .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        if ledger_count > 0 || position_count > 0 {
            return Err((StatusCode::CONFLICT,
                "Paper starting balance can only be changed before the first paper trade or position, to protect the existing ledger.".into()));
        }
        let price = state.sol_price.read().await.price_usd;
        let starting_lamports = if candidate.paper_starting_balance_currency == "SOL" {
            (candidate.paper_starting_balance * LAMPORTS_PER_SOL as f64).round() as u64
        } else {
            sol_price::usd_to_lamports(candidate.paper_starting_balance, price)
                .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?
        };
        if !price.is_finite() || price <= 0.0 {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "Cannot reset the paper starting balance without a valid SOL/USD quote.".into(),
            ));
        }
        next_wallet = PaperWallet::new_with_price(starting_lamports, price);
    }

    let serialized = serde_json::to_string(&candidate)
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let updated_at = chrono::Utc::now().to_rfc3339();
    let mut transaction = state
        .database
        .begin()
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    sqlx::query(
        "INSERT INTO nodex_runtime_config (config_key, config_json, updated_at) VALUES ('trading', ?, ?) ON CONFLICT(config_key) DO UPDATE SET config_json = excluded.config_json, updated_at = excluded.updated_at"
    )
    .bind(serialized).bind(&updated_at).execute(&mut *transaction).await
    .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, format!("Could not persist runtime configuration: {error}")))?;

    if balance_changed {
        sqlx::query(
            "UPDATE paper_wallets SET starting_balance_lamports=?, starting_sol_price_usd=?, available_balance_lamports=?, invested_balance_lamports=?, locked_rent_lamports=?, total_network_fees_lamports=?, total_priority_fees_lamports=?, total_swap_fees_lamports=?, total_rent_refunded_lamports=?, updated_at=? WHERE id='default'"
        )
        .bind(i64::try_from(next_wallet.starting_balance_lamports).unwrap_or(i64::MAX))
        .bind(next_wallet.starting_sol_price_usd)
        .bind(i64::try_from(next_wallet.available_balance_lamports).unwrap_or(i64::MAX))
        .bind(i64::try_from(next_wallet.invested_balance_lamports).unwrap_or(i64::MAX))
        .bind(i64::try_from(next_wallet.locked_rent_lamports).unwrap_or(i64::MAX))
        .bind(i64::try_from(next_wallet.total_network_fees_lamports).unwrap_or(i64::MAX))
        .bind(i64::try_from(next_wallet.total_priority_fees_lamports).unwrap_or(i64::MAX))
        .bind(i64::try_from(next_wallet.total_swap_fees_lamports).unwrap_or(i64::MAX))
        .bind(i64::try_from(next_wallet.total_rent_refunded_lamports).unwrap_or(i64::MAX))
        .bind(&updated_at)
        .execute(&mut *transaction)
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, format!("Could not update paper wallet: {error}")))?;
    }
    transaction
        .commit()
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    if balance_changed {
        *wallet_guard = next_wallet;
    }
    drop(wallet_guard);
    *state.trading_settings.write().await = candidate.clone();
    if balance_changed {
        let wallet_snapshot = state.wallet.read().await.clone();
        let price_snapshot = state.sol_price.read().await.clone();
        let positions_snapshot = state.portfolio_positions.read().await.clone();
        let portfolio_snapshot =
            calculate_portfolio_snapshot(&wallet_snapshot, &price_snapshot, &positions_snapshot);
        if let Err(error) =
            nodex_persistence::save_portfolio_snapshot(&state.database, &portfolio_snapshot).await
        {
            tracing::error!(%error, "Could not persist portfolio snapshot after starting-balance settings change");
        }
        let profile_snapshot = {
            let mut profile = state.profile.write().await;
            if let Some(object) = profile.as_object_mut() {
                object.insert("paper_wallet".into(), json!(wallet_snapshot));
                object.insert("sol_price".into(), json!(price_snapshot));
                object.insert("portfolio".into(), portfolio_snapshot.clone());
            }
            profile.clone()
        };
        if let Err(error) =
            nodex_persistence::save_profile(&state.database, &profile_snapshot).await
        {
            tracing::error!(%error, "Could not persist profile after starting-balance settings change");
        }
        let _ = state
            .events
            .send(json!({"type":"profile_updated","data":profile_snapshot}));
        let _ = state.events.send(json!({
            "type":"portfolio_summary_updated",
            "data":portfolio_snapshot
        }));
    }
    let _ = state
        .database_changes
        .send(json!({
            "type":"trading_config_updated","data":candidate,"updated_at":updated_at
        }))
        .await;
    let _ = state.events.send(json!({
        "type":"runtime_config_updated","data":candidate,"updated_at":updated_at
    }));
    Ok(Json(
        json!({"status":"ok","config":candidate,"updated_at":updated_at}),
    ))
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
            // Resume discovery and new entries. Position monitoring is independent
            // and remains enabled unless explicitly configured otherwise.
            control.paused = false;
            control.token_discovery_enabled = true;
            control.new_entries_enabled = true;
        }
        "pause" | "stop" => {
            // Pause only new-token discovery and new paper entries. Do not stop
            // existing market subscriptions or the open-position exit evaluator.
            control.paused = true;
            control.token_discovery_enabled = false;
            control.new_entries_enabled = false;
            control.position_monitoring_enabled = true;
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
        market_monitoring::ensure_market_monitor(&state).await;
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

/// Selectively remove stored tokens that have never entered paper trading.
/// Any token with an open or closed paper position remains intact.
async fn clear_token_and_event_history(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    verify_operator_key(&state, &headers)?;

    let (
        deleted_token_state_records,
        deleted_launch_records,
        deleted_chart_records,
        deleted_raw_event_records,
        database_mints,
    ) = nodex_persistence::clear_non_paper_token_history(&state.database)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Selective token cleanup failed: {error}"),
            )
        })?;

    // Re-read paper positions after the transaction commits so a position
    // created while cleanup was waiting for SQLite cannot be removed in memory.
    let positions = nodex_persistence::load_portfolio_positions(&state.database)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Could not refresh paper-traded token protection: {error}"),
            )
        })?;
    let paper_mints: HashSet<String> = positions
        .iter()
        .filter_map(|position| {
            position
                .get("mint_address")
                .and_then(serde_json::Value::as_str)
        })
        .map(str::to_owned)
        .collect();

    // Also remove transient in-memory token states that had not reached SQLite.
    let mut deleted_mints: HashSet<String> = database_mints.into_iter().collect();
    for snapshot in state.token_registry.snapshot() {
        if !paper_mints.contains(&snapshot.mint_address) {
            state.token_registry.remove(&snapshot.mint_address);
            deleted_mints.insert(snapshot.mint_address);
        }
    }
    for mint in &deleted_mints {
        let _ = state.events.send(json!({
            "type": "token_removed",
            "data": {
                "mint_address": mint,
                "reason": "storage_cleanup",
                "removed_at": chrono::Utc::now().to_rfc3339()
            }
        }));
    }

    tracing::warn!(
        deleted_token_state_records,
        deleted_launch_records,
        deleted_chart_records,
        deleted_raw_event_records,
        deleted_in_memory_tokens = deleted_mints.len(),
        "NodeX non-paper token history cleared through maintenance API"
    );

    Ok(Json(json!({
        "status": "ok",
        "deleted_token_records": deleted_token_state_records,
        "deleted_token_state_records": deleted_token_state_records,
        "deleted_launch_records": deleted_launch_records,
        "deleted_chart_records": deleted_chart_records,
        "deleted_raw_event_records": deleted_raw_event_records,
        "deleted_mints": deleted_mints,
        "preserved": [
            "all tokens with open or closed paper positions",
            "paper wallet and ledger",
            "paper positions and balances",
            "live token discovery and market subscriptions",
            "database schema and runtime configuration",
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

/// Re-resolves missing token identity fields on demand (for example after a
/// dashboard refresh), then persists and broadcasts the refreshed snapshot.
async fn refresh_token_metadata(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(mint): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    verify_operator_key(&state, &headers)?;
    let rpc = state.rpc.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "Solana HTTP RPC is not configured".to_string(),
    ))?;
    let metadata = resolve_discovery_metadata(rpc, &mint, &state.events)
        .await
        .map_err(|error| {
            (
                StatusCode::BAD_GATEWAY,
                format!("Token metadata refresh failed: {error}"),
            )
        })?;

    let mut identity = serde_json::Map::new();
    if let Some(value) = metadata.name {
        identity.insert("name".into(), json!(value));
    }
    if let Some(value) = metadata.symbol {
        identity.insert("symbol".into(), json!(value));
    }
    if let Some(value) = metadata.decimals {
        identity.insert("decimals".into(), json!(value));
    }
    if let Some(value) = metadata.metadata_uri {
        identity.insert("metadata_uri".into(), json!(value));
    }
    if let Some(value) = metadata.logo_url.clone() {
        identity.insert("logo_url".into(), json!(value));
    }
    identity.insert("metadata_status".into(), json!(metadata.status));

    let snapshot = state
        .token_registry
        .upsert_patch(&mint, json!({"identity": identity}))
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    nodex_persistence::save_token_state(&state.database, &snapshot)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Could not persist refreshed token metadata: {error}"),
            )
        })?;
    let _ = state
        .events
        .send(json!({"type":"token_state_updated","data":snapshot}));

    Ok(Json(json!({
        "status": "refreshed",
        "mint_address": mint,
        "metadata_status": metadata.status,
        "logo_url": metadata.logo_url
    })))
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

async fn creator_intelligence(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<
    Json<Vec<nodex_persistence::creator_intelligence::CreatorIntelligence>>,
    (StatusCode, String),
> {
    let limit = query
        .get("limit")
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(100)
        .clamp(1, 500);
    let creators = nodex_persistence::creator_intelligence::list(&state.database, limit)
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(creators))
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

async fn clear_engine_activity_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    verify_operator_key(&state, &headers)?;
    let deleted_records = nodex_persistence::clear_engine_activity_logs(&state.database)
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    tracing::info!(
        deleted_records,
        "Operator cleared persisted engine activity logs"
    );
    Ok(Json(json!({
        "status": "ok",
        "deleted_records": deleted_records
    })))
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
    let rpc = state.flux_rpc.or(state.rpc).ok_or((
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "Solana HTTP RPC is not configured".to_string(),
    ))?;
    let diagnostic_mode = state.trading_settings.read().await.diagnostic_mode;
    let report = rpc
        .scan_token_security_with_progress_mode(&mint, diagnostic_mode, |_, _, _| {})
        .await
        .map_err(|error| {
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

async fn sol_price(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(json!(state.sol_price.read().await.clone()))
}

/// Aggregate paper-account equity in USD: available SOL + open token holdings
/// + recoverable rent, all valued using the latest SOL/USD quote.
async fn portfolio_balance(State(state): State<AppState>) -> Json<serde_json::Value> {
    let wallet = state.wallet.read().await.clone();
    let price = state.sol_price.read().await.clone();
    let positions = state.portfolio_positions.read().await.clone();
    let snapshot = calculate_portfolio_snapshot(&wallet, &price, &positions);
    if let Err(error) = nodex_persistence::save_portfolio_snapshot(&state.database, &snapshot).await
    {
        tracing::error!(%error, "Could not persist portfolio valuation snapshot");
    }
    Json(snapshot)
}

/// Returns one assembled profile snapshot. Wallet SOL is the on-chain balance;
/// paper-trading funds and positions remain separate accounting domains.
#[derive(Debug, Deserialize)]
struct PositionRiskUpdate {
    stop_loss_pct: Option<f64>,
    take_profit_pct: Option<f64>,
    trailing_stop_enabled: Option<bool>,
    trailing_activation_profit_pct: Option<f64>,
    trailing_stop_callback_pct: Option<f64>,
    maximum_profit_pct: Option<f64>,
}

/// Manually close one complete open paper position using its latest market mark.
async fn manual_paper_sell(
    State(state): State<AppState>,
    Path(position_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    verify_operator_key(&state, &headers)?;
    {
        let mut positions = state.portfolio_positions.write().await;
        let position = positions
            .iter_mut()
            .find(|position| {
                position
                    .get("position_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(position_id.as_str())
                    || position.get("id").and_then(serde_json::Value::as_str)
                        == Some(position_id.as_str())
            })
            .ok_or((
                StatusCode::NOT_FOUND,
                "Paper position was not found.".to_string(),
            ))?;
        if position.get("status").and_then(serde_json::Value::as_str) != Some("open") {
            return Err((
                StatusCode::CONFLICT,
                "Only an open paper position can be sold.".into(),
            ));
        }
        let marked_at_ms = timestamp_ms_from_value(position.get("marked_at"));
        let now_ms = chrono::Utc::now().timestamp_millis();
        if !marked_at_ms.is_some_and(|timestamp| {
            timestamp > 0
                && timestamp <= now_ms.saturating_add(60_000)
                && now_ms.saturating_sub(timestamp) <= 30_000
        }) {
            return Err((
                StatusCode::CONFLICT,
                "The latest position price is stale. Wait for a fresh market update before manually selling.".into(),
            ));
        }
        let price = position
            .get("current_price_sol")
            .or_else(|| position.get("price_sol"))
            .and_then(serde_json::Value::as_f64)
            .filter(|price| price.is_finite() && *price > 0.0)
            .ok_or((
                StatusCode::CONFLICT,
                "No valid market price is available for this paper position.".into(),
            ))?;
        if position
            .get("exit_requested")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
        {
            let object = position.as_object_mut().ok_or((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Stored paper position is invalid.".to_string(),
            ))?;
            object.insert("exit_requested".into(), json!(true));
            object.insert("exit_trigger_reason".into(), json!("manual"));
            object.insert("exit_trigger_price_sol".into(), json!(price));
            object.insert(
                "exit_requested_at".into(),
                json!(chrono::Utc::now().to_rfc3339()),
            );
        }
        let snapshot = position.clone();
        let mint = snapshot
            .get("mint_address")
            .or_else(|| snapshot.get("mint"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned();
        if mint.is_empty() {
            return Err((
                StatusCode::CONFLICT,
                "Paper position is missing its token mint.".into(),
            ));
        }
        nodex_persistence::save_portfolio_position(
            &state.database,
            &position_id,
            &mint,
            "open",
            &snapshot,
        )
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Could not persist the manual paper exit request: {error}"),
            )
        })?;
    }
    if let Err(error) = settle_triggered_paper_exit(
        &state.database,
        &state.wallet,
        &state.sol_price,
        &state.portfolio_positions,
        &state.profile,
        &state.events,
        &position_id,
    )
    .await
    {
        let failed_position = state
            .portfolio_positions
            .read()
            .await
            .iter()
            .find(|position| {
                position
                    .get("position_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(position_id.as_str())
            })
            .cloned()
            .unwrap_or_else(|| json!({"position_id":position_id}));
        let _ = state.events.send(json!({
            "type":"paper_sell_failed",
            "data":{
                "position":failed_position,
                "position_id":position_id,
                "reason":error,
                "retrying":true,
                "paper_only":true,
                "execution_mode":"simulated"
            }
        }));
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            format!("Paper sale is pending settlement and will be retried: {error}"),
        ));
    }
    let closed_position = state
        .portfolio_positions
        .read()
        .await
        .iter()
        .find(|position| {
            position
                .get("position_id")
                .and_then(serde_json::Value::as_str)
                == Some(position_id.as_str())
                || position.get("id").and_then(serde_json::Value::as_str)
                    == Some(position_id.as_str())
        })
        .cloned()
        .ok_or((
            StatusCode::NOT_FOUND,
            "Closed paper position was not found after settlement.".to_string(),
        ))?;
    Ok(Json(json!({
        "status":"sold",
        "position_id":position_id,
        "paper_only":true,
        "execution_mode":"simulated",
        "full_exit":true,
        "position":closed_position
    })))
}

async fn get_position_risk(
    State(state): State<AppState>,
    Path(position_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    verify_operator_key(&state, &headers)?;
    let positions = state.portfolio_positions.read().await;
    let position = positions
        .iter()
        .find(|position| {
            position
                .get("position_id")
                .and_then(serde_json::Value::as_str)
                == Some(position_id.as_str())
                || position.get("id").and_then(serde_json::Value::as_str)
                    == Some(position_id.as_str())
        })
        .ok_or((
            StatusCode::NOT_FOUND,
            "Paper position was not found.".to_string(),
        ))?;
    Ok(Json(json!({
        "status":"ok",
        "position_id":position_id,
        "risk_policy":position.get("risk_policy").cloned().unwrap_or_else(|| json!({})),
        "trailing_stop_eligible":position.get("trailing_stop_eligible").and_then(serde_json::Value::as_bool).unwrap_or(false),
        "trailing_stop_eligibility_reason":position.get("trailing_stop_eligibility_reason").and_then(serde_json::Value::as_str).unwrap_or("Eligibility has not been assessed for this position.")
    })))
}

/// Update protection on one open paper position. Every setting is persisted on
/// that position; global defaults and all other positions remain unchanged.
async fn update_position_risk(
    State(state): State<AppState>,
    Path(position_id): Path<String>,
    headers: HeaderMap,
    Json(update): Json<PositionRiskUpdate>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    verify_operator_key(&state, &headers)?;

    for (name, value, max) in [
        ("stop_loss_pct", update.stop_loss_pct, 100.0),
        ("take_profit_pct", update.take_profit_pct, 1_000.0),
        (
            "trailing_activation_profit_pct",
            update.trailing_activation_profit_pct,
            5_000.0,
        ),
        (
            "trailing_stop_callback_pct",
            update.trailing_stop_callback_pct,
            100.0,
        ),
    ] {
        if value.is_some_and(|number| !number.is_finite() || number < 0.0 || number > max) {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("{name} must be between 0 and {max}."),
            ));
        }
    }

    if update
        .maximum_profit_pct
        .is_some_and(|number| !number.is_finite() || number < 0.0)
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "maximum_profit_pct must be a finite non-negative percentage.".into(),
        ));
    }

    let mut positions = state.portfolio_positions.write().await;
    let index = positions
        .iter()
        .position(|position| {
            position
                .get("position_id")
                .and_then(serde_json::Value::as_str)
                == Some(position_id.as_str())
                || position.get("id").and_then(serde_json::Value::as_str)
                    == Some(position_id.as_str())
        })
        .ok_or((
            StatusCode::NOT_FOUND,
            "Paper position was not found.".to_string(),
        ))?;

    let mut updated = positions[index].clone();
    if !updated
        .get("status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .eq_ignore_ascii_case("open")
    {
        return Err((
            StatusCode::CONFLICT,
            "Risk settings can only be changed for an open position.".into(),
        ));
    }
    if updated
        .get("exit_requested")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
    {
        return Err((
            StatusCode::CONFLICT,
            "An exit has already been triggered for this position.".into(),
        ));
    }

    let mut policy: PositionRiskPolicy = if let Some(value) = updated.get("risk_policy").cloned() {
        serde_json::from_value(value).map_err(|error| {
            (
                StatusCode::CONFLICT,
                format!("Saved position risk policy is invalid: {error}"),
            )
        })?
    } else {
        let entry_sol = updated
            .get("entry_price_sol")
            .and_then(serde_json::Value::as_f64)
            .or_else(|| {
                updated
                    .get("average_entry_price_sol")
                    .and_then(serde_json::Value::as_f64)
            })
            .unwrap_or(0.0);
        let sol_usd = state.sol_price.read().await.price_usd;
        let entry_usd = updated
            .get("entry_price_usd")
            .and_then(serde_json::Value::as_f64)
            .filter(|value| *value > 0.0)
            .unwrap_or(entry_sol * sol_usd);
        let settings = state.trading_settings.read().await.clone();
        let config = main_bot_config_from_settings(&settings, sol_usd);
        capture_position_risk(&config, entry_sol, entry_usd).ok_or((
            StatusCode::CONFLICT,
            "This position is missing a valid original entry price; its risk policy cannot be reconstructed.".to_string(),
        ))?
    };

    policy.stop_loss_pct = update.stop_loss_pct.unwrap_or(policy.stop_loss_pct);
    policy.take_profit_pct = update.take_profit_pct.unwrap_or(policy.take_profit_pct);
    policy.trailing_activation_profit_pct = update
        .trailing_activation_profit_pct
        .unwrap_or(policy.trailing_activation_profit_pct);
    policy.trailing_stop_callback_pct = update
        .trailing_stop_callback_pct
        .unwrap_or(policy.trailing_stop_callback_pct);
    policy.maximum_profit_pct = update
        .maximum_profit_pct
        .unwrap_or(policy.maximum_profit_pct);

    if let Some(enabled) = update.trailing_stop_enabled {
        policy.trailing_stop_enabled = enabled;
        // Availability is a user-controlled position setting, not a market-activity gate.
        if enabled {
            policy.trailing_stop_eligible = true;
            policy.trailing_stop_eligibility_reason =
                "Available: trailing stop can be enabled on any open paper position.".into();
        }
        if enabled {
            // Start trailing protection immediately from the latest known price
            // (or entry if no valid quote is available). Fixed SL/TP are retained
            // as saved settings but are inactive while trailing mode is enabled.
            let latest_price = updated
                .get("current_price_sol")
                .or_else(|| updated.get("price_sol"))
                .and_then(serde_json::Value::as_f64)
                .filter(|price| price.is_finite() && *price > 0.0)
                .unwrap_or(policy.entry_price_sol);
            policy.highest_price_sol = policy
                .highest_price_sol
                .max(policy.entry_price_sol)
                .max(latest_price);
            policy.trailing_stop_active = true;
        } else {
            policy.trailing_stop_active = false;
        }
    }

    policy.stop_loss_price_sol = if policy.stop_loss_pct > 0.0 {
        policy.entry_price_sol * (1.0 - policy.stop_loss_pct / 100.0)
    } else {
        0.0
    };
    policy.take_profit_price_sol = if policy.take_profit_pct > 0.0 {
        policy.entry_price_sol * (1.0 + policy.take_profit_pct / 100.0)
    } else {
        0.0
    };
    policy.stop_loss_price_usd = if policy.stop_loss_pct > 0.0 {
        policy.entry_price_usd * (1.0 - policy.stop_loss_pct / 100.0)
    } else {
        0.0
    };
    policy.take_profit_price_usd = if policy.take_profit_pct > 0.0 {
        policy.entry_price_usd * (1.0 + policy.take_profit_pct / 100.0)
    } else {
        0.0
    };
    policy.maximum_profit_price_sol = if policy.maximum_profit_pct > 0.0 {
        policy.entry_price_sol * (1.0 + policy.maximum_profit_pct / 100.0)
    } else {
        0.0
    };
    policy.maximum_profit_price_usd = if policy.maximum_profit_pct > 0.0 {
        policy.entry_price_usd * (1.0 + policy.maximum_profit_pct / 100.0)
    } else {
        0.0
    };
    policy.trailing_stop_price_sol = if policy.trailing_stop_enabled && policy.trailing_stop_active
    {
        policy.highest_price_sol * (1.0 - policy.trailing_stop_callback_pct / 100.0)
    } else {
        0.0
    };
    policy.captured_at = chrono::Utc::now().to_rfc3339();

    let object = updated.as_object_mut().ok_or((
        StatusCode::INTERNAL_SERVER_ERROR,
        "Stored paper position is not a JSON object.".to_string(),
    ))?;
    object.insert(
        "risk_policy".into(),
        serde_json::to_value(&policy)
            .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?,
    );
    for (key, value) in [
        ("stop_loss_pct", json!(policy.stop_loss_pct)),
        ("take_profit_pct", json!(policy.take_profit_pct)),
        ("stop_loss_price_sol", json!(policy.stop_loss_price_sol)),
        ("take_profit_price_sol", json!(policy.take_profit_price_sol)),
        ("stop_loss_price_usd", json!(policy.stop_loss_price_usd)),
        ("take_profit_price_usd", json!(policy.take_profit_price_usd)),
        (
            "trailing_stop_eligible",
            json!(policy.trailing_stop_eligible),
        ),
        (
            "trailing_stop_eligibility_reason",
            json!(policy.trailing_stop_eligibility_reason),
        ),
        ("trailing_stop_enabled", json!(policy.trailing_stop_enabled)),
        (
            "trailing_activation_profit_pct",
            json!(policy.trailing_activation_profit_pct),
        ),
        (
            "trailing_stop_callback_pct",
            json!(policy.trailing_stop_callback_pct),
        ),
        ("trailing_stop_active", json!(policy.trailing_stop_active)),
        (
            "trailing_stop_price_sol",
            json!(policy.trailing_stop_price_sol),
        ),
        ("maximum_profit_pct", json!(policy.maximum_profit_pct)),
        (
            "maximum_profit_price_sol",
            json!(policy.maximum_profit_price_sol),
        ),
        (
            "maximum_profit_price_usd",
            json!(policy.maximum_profit_price_usd),
        ),
        ("highest_price_sol", json!(policy.highest_price_sol)),
    ] {
        object.insert(key.into(), value);
    }

    let mint = updated
        .get("mint_address")
        .and_then(serde_json::Value::as_str)
        .or_else(|| updated.get("mint").and_then(serde_json::Value::as_str))
        .ok_or((
            StatusCode::CONFLICT,
            "Position is missing its mint address.".to_string(),
        ))?
        .to_string();
    nodex_persistence::save_portfolio_position(
        &state.database,
        &position_id,
        &mint,
        "open",
        &updated,
    )
    .await
    .map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Could not persist position risk update: {error}"),
        )
    })?;

    positions[index] = updated.clone();
    drop(positions);
    let _ = state
        .events
        .send(json!({"type":"position_updated","data":updated.clone()}));
    let _ = state
        .database_changes
        .send(json!({"type":"position_risk_updated","data":updated.clone()}))
        .await;
    Ok(Json(
        json!({"status":"ok","position":updated,"risk_policy":policy}),
    ))
}

async fn get_profile(State(state): State<AppState>) -> Json<serde_json::Value> {
    // Assemble one consistent dashboard snapshot from the engine's authoritative
    // in-memory state. The on-chain wallet and paper wallet are deliberately
    // separate balances and are never added together.
    let positions = state.portfolio_positions.read().await.clone();
    let paper_wallet = state.wallet.read().await.clone();
    let sol_price = state.sol_price.read().await.clone();
    let mut profile = state.profile.read().await.clone();

    let portfolio = calculate_portfolio_snapshot(&paper_wallet, &sol_price, &positions);

    let mut token_snapshots = state.token_registry.snapshot();
    token_snapshots.sort_by_key(|left| std::cmp::Reverse(left.updated_at));
    let token_snapshots: Vec<serde_json::Value> = token_snapshots
        .into_iter()
        .map(|snapshot| {
            // Flatten every tracked token into the profile bootstrap response.
            // Verification is independent of market and strategy activity.
            let mut value = snapshot.data;
            if let Some(fields) = value.as_object_mut() {
                let security = fields.get("security").cloned().unwrap_or_else(|| json!({}));
                // Recompute from the three authoritative checks so a stale
                // legacy flag cannot mislabel a token in the profile snapshot.
                let verified = core_verification_gate_passed(&security);
                let rejected = security
                    .get("decision")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|decision| decision.eq_ignore_ascii_case("REJECTED"));
                let status = if verified {
                    "VERIFIED"
                } else if rejected {
                    "REJECTED"
                } else {
                    "UNVERIFIED"
                };
                let security_status = match status {
                    "VERIFIED" => "verified",
                    "REJECTED" => "rejected",
                    _ => "unverified",
                };
                if let Some(security_object) = fields
                    .get_mut("security")
                    .and_then(serde_json::Value::as_object_mut)
                {
                    security_object.insert("status".into(), json!(security_status));
                    security_object.insert("verification_status".into(), json!(status));
                    security_object.insert("verification_gate_passed".into(), json!(verified));
                }
                fields.insert("verification_status".into(), json!(status));
                fields.insert(
                    "verification_label".into(),
                    json!(match status {
                        "VERIFIED" => "Verified",
                        "REJECTED" => "Rejected",
                        _ => "Unverified",
                    }),
                );
                fields.insert("is_verified".into(), json!(verified));
                fields.insert("mint_address".into(), json!(snapshot.mint_address));
                fields.insert("version".into(), json!(snapshot.version));
                fields.insert("updated_at".into(), json!(snapshot.updated_at));
            }
            value
        })
        .collect();

    if let Some(object) = profile.as_object_mut() {
        object.insert("paper_wallet".into(), json!(paper_wallet));
        object.insert("sol_price".into(), json!(sol_price));
        object.insert("portfolio".into(), portfolio);
        // The profile endpoint is the frontend's bootstrap request. Include the
        // complete persisted token states so identity metadata is restored by mint.
        let token_snapshot_count = token_snapshots.len();
        object.insert("tokens".into(), json!(token_snapshots));
        object.insert("token_snapshot_complete".into(), json!(true));
        object.insert("token_snapshot_count".into(), json!(token_snapshot_count));
    }
    if let Some(portfolio_snapshot) = profile.get("portfolio") {
        if let Err(error) =
            nodex_persistence::save_portfolio_snapshot(&state.database, portfolio_snapshot).await
        {
            tracing::error!(%error, "Could not persist profile portfolio snapshot");
        }
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
                        let price_sol = data.get("price_sol").and_then(serde_json::Value::as_f64);
                        let market_cap_sol = data.get("market_cap_sol").and_then(serde_json::Value::as_f64);
                        let sol_quote = state.sol_price.read().await.clone();
                        let usd_quote_valid = sol_quote.status == "live"
                            && sol_quote.price_usd.is_finite()
                            && sol_quote.price_usd > 0.0;
                        let price_usd = price_sol
                            .filter(|price| price.is_finite() && *price > 0.0)
                            .filter(|_| usd_quote_valid)
                            .map(|price| price * sol_quote.price_usd);
                        let market_cap_usd = market_cap_sol
                            .filter(|value| value.is_finite() && *value > 0.0)
                            .filter(|_| usd_quote_valid)
                            .map(|value| value * sol_quote.price_usd);
                        value = json!({
                            "type":"market_tick",
                            "data":{
                                "mint_address":mint,
                                "name":identity.and_then(|v| v.get("name")).cloned().unwrap_or(serde_json::Value::Null),
                                "symbol":identity.and_then(|v| v.get("symbol")).cloned().unwrap_or(serde_json::Value::Null),
                                "venue":data.get("venue").cloned().unwrap_or(serde_json::Value::Null),
                                "price_sol":price_sol,
                                "price_usd":price_usd,
                                "price_currency":if price_usd.is_some() {"USD"} else {"SOL"},
                                "sol_price_usd":if usd_quote_valid {Some(sol_quote.price_usd)} else {None::<f64>},
                                "sol_price_status":sol_quote.status,
                                "price_change_1m_pct":data.get("price_change_1m_pct").cloned().unwrap_or(serde_json::Value::Null),
                                "price_change_5m_pct":data.get("price_change_5m_pct").cloned().unwrap_or(serde_json::Value::Null),
                                "market_cap_sol":market_cap_sol,
                                "market_cap_usd":market_cap_usd,
                                "slot":data.get("slot").cloned().unwrap_or(serde_json::Value::Null),
                                "timestamp_ms":data.get("timestamp_ms").cloned().unwrap_or(serde_json::Value::Null),
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
                            // Stop only the launch/discovery stream and new entries.
                            // Existing market subscriptions and exit monitoring continue.
                            control.paused = true;
                            control.token_discovery_enabled = false;
                            control.new_entries_enabled = false;
                            control.position_monitoring_enabled = true;
                            let snapshot = control.clone();
                            drop(control);
                            state.runtime_enabled.send_replace(false);
                            Some(json!({"type":"engine_state","data":snapshot}))
                        }
                        Ok(ClientCommand::Resume) | Ok(ClientCommand::Start) => {
                            let mut control = state.control.write().await;
                            control.paused = false;
                            control.token_discovery_enabled = true;
                            control.new_entries_enabled = true;
                            let snapshot = control.clone();
                            drop(control);
                            state.runtime_enabled.send_replace(true);
                            market_monitoring::ensure_market_monitor(&state).await;
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
                            let has_open_position = state
                                .portfolio_positions
                                .read()
                                .await
                                .iter()
                                .any(|position| {
                                    let position_mint = position
                                        .get("mint_address")
                                        .or_else(|| position.get("mint"))
                                        .and_then(serde_json::Value::as_str);
                                    let status = position
                                        .get("status")
                                        .and_then(serde_json::Value::as_str)
                                        .unwrap_or("open");
                                    position_mint == Some(mint.as_str())
                                        && status.eq_ignore_ascii_case("open")
                                });
                            if !has_open_position {
                                Some(json!({"type":"command_rejected","data":{
                                    "command":"subscribe_token",
                                    "mint_address":mint,
                                    "reason":"Token detail subscriptions are available only while a paper position is open."
                                }}))
                            } else if mint.is_empty() || mint.len() > 64 {
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
    // Require a genuine spot quote before valuing the initial $10 paper balance.
    let initial_sol_price = sol_price::fetch_sol_usd_price().await?;
    tracing::info!(price_usd = initial_sol_price.price_usd, source = %initial_sol_price.source, "Fetched initial SOL/USD price");
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
    let mut trading_settings = match stored_trading_config {
        Some(raw) => serde_json::from_str::<TradingSettings>(&raw)
            .ok()
            .filter(|settings| {
                settings.max_investment_per_token_usd.is_finite()
                    && settings.max_investment_per_token_usd >= 0.0
            })
            .unwrap_or_default(),
        None => TradingSettings::default(),
    };
    // Migrate the old fixed-SOL position size exactly once into a USD budget.
    // This preserves the operator's prior budget at the migration-time SOL quote.
    let usd_position_size_migrated: Option<String> = sqlx::query_scalar(
        "SELECT config_json FROM nodex_runtime_config WHERE config_key = 'usd_position_size_v1'",
    )
    .fetch_optional(&db)
    .await?;
    if usd_position_size_migrated.is_none() {
        if trading_settings.max_investment_per_token_usd <= 0.0
            && trading_settings.max_investment_per_token_sol > 0.0
        {
            trading_settings.max_investment_per_token_usd =
                trading_settings.max_investment_per_token_sol * initial_sol_price.price_usd;
        }
        trading_settings.max_investment_per_token_sol = 0.0;
        trading_settings.validate().map_err(anyhow::Error::msg)?;
        let migrated = serde_json::to_string(&trading_settings)?;
        let updated_at = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO nodex_runtime_config (config_key, config_json, updated_at) VALUES ('trading', ?, ?) ON CONFLICT(config_key) DO UPDATE SET config_json=excluded.config_json, updated_at=excluded.updated_at",
        )
        .bind(migrated)
        .bind(&updated_at)
        .execute(&db)
        .await?;
        sqlx::query(
            "INSERT INTO nodex_runtime_config (config_key, config_json, updated_at) VALUES ('usd_position_size_v1', 'enabled', ?)",
        )
        .bind(updated_at)
        .execute(&db)
        .await?;
    }
    // One-time migration from the old diagnostic-only / require-every-checks
    // defaults. The operator explicitly chose the faster authority + RugCheck
    // launch gate; retain future manual changes after this migration marker.
    let fast_verification_migrated: Option<String> = sqlx::query_scalar(
        "SELECT config_json FROM nodex_runtime_config WHERE config_key = 'fast_verification_policy_v2'",
    )
    .fetch_optional(&db)
    .await?;
    if fast_verification_migrated.is_none() {
        trading_settings.diagnostic_mode = false;
        trading_settings.block_unverified_trades = false;
        sqlx::query(
            "INSERT INTO nodex_runtime_config (config_key, config_json, updated_at) VALUES ('fast_verification_policy_v2', 'enabled', ?)",
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&db)
        .await?;
        let migrated = serde_json::to_string(&trading_settings)?;
        sqlx::query(
            "INSERT INTO nodex_runtime_config (config_key, config_json, updated_at) VALUES ('trading', ?, ?) ON CONFLICT(config_key) DO UPDATE SET config_json=excluded.config_json, updated_at=excluded.updated_at",
        )
        .bind(migrated)
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&db)
        .await?;
    }
    // One-time opt-in requested for automatic PAPER execution. This updates the
    // existing persisted configuration once; later operator changes remain authoritative.
    let paper_auto_execution_migrated: Option<String> = sqlx::query_scalar(
        "SELECT config_json FROM nodex_runtime_config WHERE config_key = 'paper_auto_execution_defaults_v1'",
    )
    .fetch_optional(&db)
    .await?;
    if paper_auto_execution_migrated.is_none() {
        trading_settings.auto_buy_enabled = true;
        trading_settings.execution_enabled = true;
        trading_settings.validate().map_err(anyhow::Error::msg)?;
        let migrated = serde_json::to_string(&trading_settings)?;
        let updated_at = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO nodex_runtime_config (config_key, config_json, updated_at) VALUES ('trading', ?, ?) ON CONFLICT(config_key) DO UPDATE SET config_json=excluded.config_json, updated_at=excluded.updated_at",
        )
        .bind(migrated)
        .bind(&updated_at)
        .execute(&db)
        .await?;
        sqlx::query(
            "INSERT INTO nodex_runtime_config (config_key, config_json, updated_at) VALUES ('paper_auto_execution_defaults_v1', 'enabled', ?)",
        )
        .bind(updated_at)
        .execute(&db)
        .await?;
    }
    let trading_settings_json = serde_json::to_string(&trading_settings)?;
    sqlx::query(
        "INSERT INTO nodex_runtime_config (config_key, config_json, updated_at) VALUES ('trading', ?, ?) ON CONFLICT(config_key) DO NOTHING"
    )
    .bind(trading_settings_json)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(&db)
    .await?;
    let (pruned_states, pruned_launches) = nodex_persistence::prune_token_history(&db, 500).await?;
    tracing::info!(
        pruned_token_states = pruned_states,
        pruned_token_launches = pruned_launches,
        "Applied bounded token history retention"
    );
    let token_registry = TokenRegistry::new();
    for snapshot in nodex_persistence::load_token_states(&db).await? {
        let mint = snapshot.mint_address.clone();
        let activity_status = snapshot
            .data
            .get("activity")
            .and_then(serde_json::Value::as_object)
            .and_then(|activity| activity.get("status"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let evaluation_was_interrupted =
            matches!(activity_status.as_str(), "evaluating" | "entry_ready");
        token_registry
            .restore(snapshot)
            .map_err(|error| anyhow::anyhow!(error))?;

        // A process restart can interrupt an in-flight simulated entry between
        // its progress event and the final result. Never restore that stale
        // transient state as if an order were still executing.
        if evaluation_was_interrupted {
            let recovered = token_registry
                .upsert_patch(&mint, interrupted_entry_recovery_patch())
                .map_err(|error| anyhow::anyhow!(error))?;
            nodex_persistence::save_token_state(&db, &recovered).await?;
            tracing::warn!(
                mint_address = %mint,
                previous_status = %activity_status,
                "Recovered interrupted paper-entry evaluation after restart"
            );
        }
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
    // One backend-owned broadcaster is the source for all runtime event consumers.
    // Publishers receive a sender handle; subscribers are created through this owner.
    let event_broadcaster = event_broadcaster::EventBroadcaster::new(8192);
    let events = event_broadcaster.sender();
    let sol_price_state =
        sol_price::start_sol_price_feed(events.clone(), initial_sol_price.clone());

    // Read the broadcast stream quickly, filter out successful low-level RPC
    // chatter, and hand durable operator events to a bounded persistence queue.
    let (activity_queue, mut activity_queue_receiver) = mpsc::channel::<serde_json::Value>(4096);
    {
        let mut activity_receiver = event_broadcaster.subscribe();
        tokio::spawn(async move {
            loop {
                match activity_receiver.recv().await {
                    Ok(event) => {
                        if let Some(activity) = activity_log_from_event(&event) {
                            if activity_queue.send(activity).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        tracing::error!(
                            dropped_events = count,
                            "Engine activity event reader lagged before persistence queue"
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }
    {
        let activity_database = db.clone();
        tokio::spawn(async move {
            let mut batch = Vec::with_capacity(100);
            let mut flush_interval = tokio::time::interval(Duration::from_millis(250));
            flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            flush_interval.tick().await;

            loop {
                let mut flush_now = false;
                let mut queue_closed = false;
                tokio::select! {
                    activity = activity_queue_receiver.recv() => {
                        match activity {
                            Some(activity) => batch.push(activity),
                            None => {
                                flush_now = true;
                                queue_closed = true;
                            }
                        }
                    }
                    _ = flush_interval.tick() => flush_now = true,
                }

                if (batch.len() >= 100 || flush_now) && !batch.is_empty() {
                    match nodex_persistence::save_engine_activity_logs(&activity_database, &batch)
                        .await
                    {
                        Ok(()) => batch.clear(),
                        Err(error) => tracing::error!(
                            %error,
                            queued_events = batch.len(),
                            "Could not persist batched engine activity logs"
                        ),
                    }
                }

                if queue_closed {
                    break;
                }
            }
        });
    }
    let (database_changes, database_change_receiver) = mpsc::channel(512);
    let gateway = websocket::Gateway::start(
        event_broadcaster.subscribe(),
        database_change_receiver,
        4096,
        portfolio_positions.clone(),
    );
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

    // Seed the paper account from persisted runtime configuration. Existing
    // balances are migrated only when no paper trades, positions, or invested
    // funds exist; accounts with trading history are left untouched.
    let configured_starting_lamports = if trading_settings.paper_starting_balance_currency == "SOL"
    {
        (trading_settings.paper_starting_balance * LAMPORTS_PER_SOL as f64).round() as u64
    } else {
        sol_price::usd_to_lamports(
            trading_settings.paper_starting_balance,
            initial_sol_price.price_usd,
        )?
    };
    let wallet = match nodex_persistence::load_paper_wallet(&db).await? {
        Some(mut existing) => {
            // One-time migration for legacy wallets created before the SOL cost basis
            // was persisted. Freeze the baseline now instead of substituting the live
            // quote on every portfolio calculation.
            if !existing.starting_sol_price_usd.is_finite()
                || existing.starting_sol_price_usd <= 0.0
            {
                existing.starting_sol_price_usd = initial_sol_price.price_usd;
                nodex_persistence::save_paper_wallet(&db, &existing).await?;
                tracing::warn!(
                    starting_sol_price_usd = initial_sol_price.price_usd,
                    "Initialized missing legacy paper-wallet SOL cost basis"
                );
            }
            let ledger_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM paper_ledger")
                .fetch_one(&db)
                .await?;
            let position_count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM portfolio_positions")
                    .fetch_one(&db)
                    .await?;
            let untouched = ledger_count == 0
                && position_count == 0
                && existing.invested_balance_lamports == 0
                && existing.locked_rent_lamports == 0;
            if untouched && existing.starting_balance_lamports != configured_starting_lamports {
                let wallet = PaperWallet::new_with_price(
                    configured_starting_lamports,
                    initial_sol_price.price_usd,
                );
                nodex_persistence::save_paper_wallet(&db, &wallet).await?;
                tracing::info!(
                    starting_balance = trading_settings.paper_starting_balance,
                    starting_balance_currency = %trading_settings.paper_starting_balance_currency,
                    starting_balance_sol = configured_starting_lamports as f64 / LAMPORTS_PER_SOL as f64,
                    "Initialized paper wallet from runtime configuration"
                );
                wallet
            } else {
                existing
            }
        }
        None => {
            let wallet = PaperWallet::new_with_price(
                configured_starting_lamports,
                initial_sol_price.price_usd,
            );
            nodex_persistence::save_paper_wallet(&db, &wallet).await?;
            wallet
        }
    };

    let wallet_state = Arc::new(RwLock::new(wallet));

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
        new_entries_enabled: true,
        position_monitoring_enabled: true,
    }));

    let trading_settings_state = Arc::new(RwLock::new(trading_settings.clone()));

    // Keep canonical token state synchronized from independent worker events.
    start_token_state_projector(
        event_broadcaster.subscribe(),
        events.clone(),
        db.clone(),
        token_registry.clone(),
        profile_state.clone(),
        portfolio_positions.clone(),
        wallet_state.clone(),
        sol_price_state.clone(),
        control.clone(),
        trading_settings_state.clone(),
        database_changes.clone(),
    );
    portfolio::start_price_projector(
        event_broadcaster.subscribe(),
        events.clone(),
        db.clone(),
        wallet_state.clone(),
        sol_price_state.clone(),
        portfolio_positions.clone(),
        profile_state.clone(),
    );
    let stream_status = Arc::new(RwLock::new(StreamStatus::default()));
    let rpc = (!config.solana_http_urls.is_empty()).then(|| {
        let limit_env = if config.fluxrpc_http_urls.is_empty() {
            "HELIUS_MAX_CONCURRENT_REQUESTS"
        } else {
            "FLUXRPC_MAX_CONCURRENT_REQUESTS"
        };
        SolanaRpc::new_with_urls_and_limit(config.solana_http_urls.clone(), limit_env)
            .with_activity_events(events.clone())
    });
    let flux_rpc = (!config.fluxrpc_http_urls.is_empty()).then(|| {
        SolanaRpc::new_with_urls_and_limit(
            config.fluxrpc_http_urls.clone(),
            "FLUXRPC_MAX_CONCURRENT_REQUESTS",
        )
        .with_activity_events(events.clone())
    });
    // Keep launch detection and market monitoring on separate subscriptions.
    // The market monitor multiplexes all watched bonding curves over one WebSocket.
    let market_monitor = Arc::new(RwLock::new(None));
    start_inactive_token_cleanup(
        db.clone(),
        token_registry.clone(),
        stream_status.clone(),
        market_monitor.clone(),
        events.clone(),
    );
    let (runtime_enabled, runtime_receiver) = watch::channel(false);

    // Decode actual Pump.fun buys/sells from the same confirmed logs stream.
    if !config.solana_ws_urls.is_empty() {
        start_pump_fun_trade_processor(
            event_broadcaster.subscribe(),
            events.clone(),
            config.pump_fun_program_id.clone(),
        );
    }

    // Decode Pump.fun creation transactions from confirmed logs notifications.
    if let Some(launch_rpc) = rpc.clone() {
        scanner::start_launch_processor(
            event_broadcaster.subscribe(),
            event_broadcaster.clone(),
            launch_rpc,
            flux_rpc.clone(),
            db.clone(),
            config.pump_fun_program_id.clone(),
            control.clone(),
            trading_settings_state.clone(),
            market_monitor.clone(),
            token_registry.clone(),
        );
    } else {
        let _ = events.send(json!({"type":"diagnostic","data":{
            "severity":"warning","component":"solana_http_rpc",
            "message":"Token transaction decoding is disabled because no active Solana HTTP RPC provider is configured. Add FluxRPC keys or Helius fallback keys.",
            "received_at":chrono::Utc::now().to_rfc3339()
        }}));
    }

    // PumpSwap has its own confirmed-log stream. The consumer decodes only
    // program-scoped Buy/Sell Anchor events; price ticks and liquidity events
    // are never counted as trades or buyers.
    let pumpswap_status = Arc::new(RwLock::new(StreamStatus::default()));
    if !config.solana_ws_urls.is_empty() {
        start_pumpswap_activity_processor(event_broadcaster.subscribe(), events.clone());
        let activity_ws_urls = config.solana_ws_urls.clone();
        let activity_events = events.clone();
        let activity_status = pumpswap_status.clone();
        tokio::spawn(async move {
            run_logs_stream(
                activity_ws_urls,
                PUMPSWAP_PROGRAM_ID.to_owned(),
                activity_events,
                activity_status,
                None,
            )
            .await;
        });
    }

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
        tracing::warn!("No active Solana WebSocket provider configured; set FluxRPC keys or Helius fallback keys.");
    }

    let state = AppState {
        events,
        gateway,
        database_changes,
        stream_status,
        pumpswap_status,
        wallet: wallet_state.clone(),
        sol_price: sol_price_state,
        rpc,
        flux_rpc,
        database: db.clone(),
        control,
        admin_api_key: std::env::var("NODEX_ADMIN_API_KEY").ok(),
        trading_settings: trading_settings_state,
        runtime_enabled,
        market_monitor,
        solana_ws_urls: config.solana_ws_urls.clone(),
        pump_fun_program_id: config.pump_fun_program_id.clone(),
        ws_tickets: Arc::new(Mutex::new(HashMap::new())),
        token_registry,
        profile: profile_state,
        portfolio_positions,
    };
    // Restore saved-token market subscriptions on every process start, even when
    // the engine is paused. Pause disables new discovery and entries, not
    // monitoring or exits for tokens and positions that already exist.
    market_monitoring::ensure_market_monitor(&state).await;
    start_pending_exit_recovery(
        state.database.clone(),
        state.wallet.clone(),
        state.sol_price.clone(),
        state.portfolio_positions.clone(),
        state.profile.clone(),
        state.events.clone(),
    );
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
        .route(
            "/api/v1/config/trading/execution",
            patch(update_paper_execution),
        )
        .route(
            "/api/config",
            get(get_trading_config).patch(patch_runtime_config),
        )
        .route("/api/v1/paper-wallet", get(paper_wallet))
        .route("/api/v1/market/sol-price", get(sol_price))
        .route("/api/v1/portfolio/balance", get(portfolio_balance))
        .route(
            "/api/v1/portfolio/positions/:position_id/risk",
            get(get_position_risk).put(update_position_risk),
        )
        .route(
            "/api/v1/portfolio/positions/:position_id/sell",
            post(manual_paper_sell),
        )
        .route("/api/v1/profile", get(get_profile))
        .route("/api/v1/tokens/recent", get(recent_launches))
        .route("/api/v1/creators/intelligence", get(creator_intelligence))
        .route("/api/v1/tokens/:mint/state", get(token_state))
        .route(
            "/api/v1/tokens/:mint/metadata/refresh",
            post(refresh_token_metadata),
        )
        .route("/api/v1/events/recent", get(recent_events))
        .route("/api/v1/admin/activity-logs", get(engine_activity_logs))
        .route(
            "/api/v1/admin/activity-logs/clear",
            post(clear_engine_activity_logs),
        )
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

#[cfg(test)]
mod scan_progress_state_tests {
    use super::*;

    #[test]
    fn stale_market_quotes_are_rejected_for_entries_and_exits() {
        let now = chrono::Utc::now();
        let fresh = json!({
            "timestamp_ms": now.timestamp_millis(),
            "received_at": now.to_rfc3339()
        });
        let stale_time = now - chrono::Duration::seconds(45);
        let stale = json!({
            "timestamp_ms": stale_time.timestamp_millis(),
            "received_at": stale_time.to_rfc3339()
        });
        assert!(market_quote_is_fresh(&fresh));
        assert!(!market_quote_is_fresh(&stale));
        assert!(!market_quote_is_fresh(&json!({"price_sol": 1.0})));
    }

    #[test]
    fn market_updates_reject_older_or_unversioned_slots_after_slot_tracking_starts() {
        assert!(market_update_slot_is_current(None, Some(100)));
        assert!(market_update_slot_is_current(Some(100), Some(100)));
        assert!(market_update_slot_is_current(Some(100), Some(101)));
        assert!(!market_update_slot_is_current(Some(100), Some(99)));
        assert!(!market_update_slot_is_current(Some(100), None));
        assert!(market_update_slot_is_current(None, None));
    }

    #[test]
    fn activity_history_filters_successful_rpc_noise_and_keeps_provider_errors() {
        let success = json!({
            "type":"engine_activity",
            "data":{"direction":"response","operation":"getAsset","severity":"info","http_status":200}
        });
        assert!(activity_log_from_event(&success).is_none());

        let failure = json!({
            "type":"engine_activity",
            "data":{"direction":"response","operation":"getAsset","severity":"error","http_status":401,"error":"unauthorized"}
        });
        let persisted = activity_log_from_event(&failure).expect("provider failure is retained");
        assert_eq!(persisted["operation"], "getAsset");
        assert_eq!(persisted["http_status"], 401);
    }

    #[test]
    fn activity_history_retains_launch_and_market_subscription_lifecycle() {
        let launch = json!({
            "type":"launch_detected",
            "data":{"mint_address":"test-mint","signature":"test-signature"}
        });
        let persisted = activity_log_from_event(&launch).expect("launch event is retained");
        assert_eq!(persisted["operation"], "launch_detected");
        assert_eq!(persisted["event"]["mint_address"], "test-mint");

        let subscribed = json!({
            "type":"market_subscription_subscribed",
            "data":{"mint_address":"test-mint"}
        });
        assert_eq!(
            activity_log_from_event(&subscribed).expect("subscription event is retained")
                ["operation"],
            "market_subscription_subscribed"
        );
    }

    #[test]
    fn interrupted_entry_recovery_preserves_monitoring_lifecycle() {
        let patch = interrupted_entry_recovery_patch();
        assert_eq!(patch["activity"]["status"], "monitoring");
        assert_eq!(patch["activity"]["label"], "Monitoring");
        assert_eq!(patch["activity"]["decision"], "wait");
        assert_eq!(patch["activity"]["sub_event"], "Checking entry conditions");
    }

    #[test]
    fn paper_entry_wait_keeps_monitoring_and_uses_a_short_subtitle() {
        let activity = paper_entry_activity_patch(&json!({
            "decision":"wait",
            "reasons":["Insufficient confirmed buyers","Liquidity is below the configured minimum"]
        }));
        assert_eq!(activity["status"], "monitoring");
        assert_eq!(activity["label"], "Monitoring");
        assert_eq!(activity["sub_event"], "Checking entry conditions");
        assert_eq!(activity["reasons"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn paper_entry_rejection_and_approval_are_monitoring_sub_events() {
        let rejected = paper_entry_activity_patch(&json!({
            "decision":"reject",
            "reasons":["Creator is on the rug blacklist"]
        }));
        assert_eq!(rejected["status"], "monitoring");
        assert_eq!(rejected["label"], "Monitoring");
        assert_eq!(rejected["sub_event"], "Strategy rejected");

        let ready = paper_entry_activity_patch(&json!({
            "decision":"evaluate_paper_entry",
            "reasons":[]
        }));
        assert_eq!(ready["status"], "monitoring");
        assert_eq!(ready["label"], "Monitoring");
        assert_eq!(ready["sub_event"], "Entry conditions met");
    }

    #[test]
    fn late_scan_progress_does_not_downgrade_approved_verdict() {
        let current = json!({"decision":"APPROVED","status":"completed"});
        let patch = scan_progress_security_patch(
            Some(&current),
            &json!({"progress":42,"stage":"holder_check","message":"still processing"}),
        );
        assert_eq!(patch["security"]["progress"], 42);
        assert!(patch["security"].get("status").is_none());
    }

    #[test]
    fn scan_progress_sets_scanning_before_terminal_verdict() {
        let current = json!({"decision":"PENDING","status":"pending"});
        let patch = scan_progress_security_patch(
            Some(&current),
            &json!({"progress":42,"stage":"holder_check"}),
        );
        assert_eq!(patch["security"]["status"], "scanning");
        assert_eq!(patch["security"]["progress"], 42);
    }

    #[test]
    fn supplementary_failures_do_not_block_core_verification() {
        let mut report = json!({
            "decision":"APPROVED",
            "diagnostic_only":false,
            "checks":[
                {"name":"mint_authority","status":"passed"},
                {"name":"freeze_authority","status":"verified"},
                {"name":"rugcheck_summary","status":"passed"},
                {"name":"token_2022_extensions","status":"unverified","severity":"critical"},
                {"name":"holder_distribution","status":"unverified"}
            ]
        });
        assert!(core_verification_gate_passed(&report));
        report["decision"] = json!("REJECTED");
        assert!(core_verification_gate_passed(&report));
    }

    #[test]
    fn paper_entry_requires_the_same_explicit_approval_and_core_verification() {
        let approved = json!({
            "decision":"APPROVED",
            "diagnostic_only":false,
            "checks":[
                {"name":"mint_authority","status":"passed"},
                {"name":"freeze_authority","status":"passed"},
                {"name":"rugcheck_summary","status":"verified"}
            ]
        });
        assert!(paper_entry_security_gate_passed(Some(&approved)));

        let rejected = json!({
            "decision":"REJECTED",
            "diagnostic_only":false,
            "checks":[
                {"name":"mint_authority","status":"passed"},
                {"name":"freeze_authority","status":"passed"},
                {"name":"rugcheck_summary","status":"passed"}
            ]
        });
        assert!(!paper_entry_security_gate_passed(Some(&rejected)));

        let missing_core_check = json!({
            "decision":"APPROVED",
            "diagnostic_only":false,
            "checks":[
                {"name":"mint_authority","status":"passed"},
                {"name":"freeze_authority","status":"passed"}
            ]
        });
        assert!(!paper_entry_security_gate_passed(Some(&missing_core_check)));
        assert!(!paper_entry_security_gate_passed(None));
    }

    #[test]
    fn market_monitoring_requires_explicit_approval_and_core_verification() {
        let approved = json!({
            "decision":"APPROVED",
            "diagnostic_only":false,
            "checks":[
                {"name":"mint_authority","status":"passed"},
                {"name":"freeze_authority","status":"passed"},
                {"name":"rugcheck_summary","status":"verified"}
            ]
        });
        assert!(token_approved_for_market_monitoring(&approved));

        let rejected = json!({
            "decision":"REJECTED",
            "diagnostic_only":false,
            "checks":[
                {"name":"mint_authority","status":"passed"},
                {"name":"freeze_authority","status":"passed"},
                {"name":"rugcheck_summary","status":"passed"}
            ]
        });
        assert!(!token_approved_for_market_monitoring(&rejected));

        let diagnostic = json!({
            "decision":"APPROVED",
            "diagnostic_only":true,
            "checks":[
                {"name":"mint_authority","status":"passed"},
                {"name":"freeze_authority","status":"passed"},
                {"name":"rugcheck_summary","status":"passed"}
            ]
        });
        assert!(!token_approved_for_market_monitoring(&diagnostic));
    }

    #[test]
    fn missing_mandatory_core_check_cannot_be_verified() {
        let report = json!({
            "decision":"APPROVED",
            "diagnostic_only":false,
            "checks":[
                {"name":"mint_authority","status":"passed"},
                {"name":"freeze_authority","status":"passed"},
                {"name":"holder_distribution","status":"verified"}
            ]
        });
        assert!(!core_verification_gate_passed(&report));
    }

    #[test]
    fn diagnostic_only_scan_cannot_be_verified() {
        let mut report = json!({
            "decision":"APPROVED",
            "diagnostic_only":true,
            "checks":[
                {"name":"mint_authority","status":"passed"},
                {"name":"freeze_authority","status":"passed"},
                {"name":"rugcheck_summary","status":"passed"}
            ]
        });
        assert!(!core_verification_gate_passed(&report));
        report["diagnostic_only"] = json!(false);
        assert!(core_verification_gate_passed(&report));
        report["decision"] = json!("REJECTED");
        assert!(core_verification_gate_passed(&report));
    }
}

#[cfg(test)]
mod main_bot_config_tests {
    use super::*;

    #[test]
    fn main_bot_config_uses_persisted_trading_settings() {
        let settings = TradingSettings {
            min_liquidity_usd: 3_000.0,
            max_entry_price_usd: 0.0005,
            max_investment_per_token_sol: 0.01,
            max_investment_per_token_usd: 5.0,
            max_simultaneous_positions: 3,
            min_safety_score: 80,
            max_slippage_pct: 10.0,
            stop_loss_pct: 15.0,
            take_profit_pct: 100.0,
            maximum_profit_pct: 0.0,
            min_5m_buy_volume_usd: 1_000.0,
            max_top10_holder_concentration_pct: 20.0,
            auto_buy_enabled: true,
            auto_recover_token_account_rent: true,
            trailing_stop_enabled: false,
            trailing_stop_callback_pct: 12.0,
            trailing_activation_profit_pct: default_trailing_activation_profit_pct(),
            minimum_distinct_buyers_high_score: default_minimum_distinct_buyers_high_score(),
            minimum_distinct_buyers_other: default_minimum_distinct_buyers_other(),
            minimum_recent_buyers_high_score: default_minimum_recent_buyers_high_score(),
            minimum_recent_buyers_other: default_minimum_recent_buyers_other(),
            minimum_30s_buy_pressure_pct: default_minimum_30s_buy_pressure_pct(),
            minimum_30s_buy_sell_ratio: default_minimum_30s_buy_sell_ratio(),
            minimum_observation_seconds: default_minimum_observation_seconds(),
            maximum_early_entry_age_seconds: default_maximum_early_entry_age_seconds(),
            maximum_early_entry_market_cap_usd: default_maximum_early_entry_market_cap_usd(),
            blacklist_creators_with_prior_rugs: true,
            paper_starting_balance: 10.0,
            paper_starting_balance_currency: "USD".into(),
            diagnostic_mode: false,
            block_unverified_trades: false,
            execution_enabled: false,
            whitelist_only_mode: false,
            preferred_owners: Vec::new(),
        };
        let config = main_bot_config_from_settings(&settings, 200.0);
        assert_eq!(config.minimum_liquidity_usd, 3_000.0);
        assert_eq!(config.maximum_entry_price_usd, 0.0005);
        assert_eq!(config.position_size_usd, 5.0);
        assert_eq!(config.position_size_sol, 0.025);
        assert_eq!(config.stop_loss_pct, 15.0);
        assert_eq!(config.take_profit_pct, 100.0);
        assert_eq!(config.maximum_open_positions, 3);
        assert_eq!(config.maximum_top10_concentration_pct, 20.0);
    }

    #[test]
    fn paper_auto_buy_and_simulated_execution_are_enabled_by_default() {
        let settings = TradingSettings::default();
        assert!(settings.auto_buy_enabled);
        assert!(settings.execution_enabled);
        assert!(!settings.trailing_stop_enabled);
        assert_eq!(settings.stop_loss_pct, 15.0);
        assert_eq!(settings.take_profit_pct, 100.0);
    }

    #[test]
    fn usd_position_size_is_canonical_and_converted_to_sol() {
        let settings = TradingSettings::default();
        let config = main_bot_config_from_settings(&settings, 200.0);
        assert_eq!(
            config.position_size_usd,
            settings.max_investment_per_token_usd
        );
        assert_eq!(
            config.position_size_sol,
            settings.max_investment_per_token_usd / 200.0
        );
    }
}

#[cfg(test)]
mod portfolio_accounting_tests {
    use super::*;

    fn quote(price: f64) -> SolPriceSnapshot {
        SolPriceSnapshot {
            price_usd: price,
            source: "test".into(),
            updated_at: chrono::Utc::now(),
            status: "live".into(),
            last_error: None,
        }
    }

    #[test]
    fn sol_only_portfolio_marks_unrealized_pnl_and_percentage_up_and_down() {
        let wallet = PaperWallet::new_with_price(50_000_000, 200.0);
        let up = calculate_portfolio_snapshot(&wallet, &quote(220.0), &[]);
        assert!((up["portfolio_balance_usd"].as_f64().unwrap() - 11.0).abs() < 1e-9);
        assert!((up["unrealized_pnl_usd"].as_f64().unwrap() - 1.0).abs() < 1e-9);
        assert!((up["unrealized_pnl_pct"].as_f64().unwrap() - 10.0).abs() < 1e-9);
        assert!((up["total_pnl_pct"].as_f64().unwrap() - 10.0).abs() < 1e-9);

        let down = calculate_portfolio_snapshot(&wallet, &quote(180.0), &[]);
        assert!((down["portfolio_balance_usd"].as_f64().unwrap() - 9.0).abs() < 1e-9);
        assert!((down["unrealized_pnl_usd"].as_f64().unwrap() + 1.0).abs() < 1e-9);
        assert!((down["unrealized_pnl_pct"].as_f64().unwrap() + 10.0).abs() < 1e-9);
    }

    #[test]
    fn portfolio_snapshot_includes_sol_and_open_token_position_pnl() {
        let wallet = PaperWallet {
            starting_balance_lamports: 50_000_000,
            starting_sol_price_usd: 200.0,
            available_balance_lamports: 40_000_000,
            invested_balance_lamports: 10_000_000,
            locked_rent_lamports: 1_000_000,
            total_network_fees_lamports: 0,
            total_priority_fees_lamports: 0,
            total_swap_fees_lamports: 0,
            total_rent_refunded_lamports: 0,
            rent_by_token_account: HashMap::new(),
        };
        let positions = vec![json!({
            "mint_address":"token",
            "status":"open",
            "current_value_sol":0.02,
            "cost_basis_sol":0.01,
            "unrealized_pnl_sol":0.01
        })];
        let snapshot = calculate_portfolio_snapshot(&wallet, &quote(220.0), &positions);
        assert!((snapshot["sol_unrealized_pnl_usd"].as_f64().unwrap() - 0.82).abs() < 1e-9);
        assert!((snapshot["token_unrealized_pnl_usd"].as_f64().unwrap() - 2.2).abs() < 1e-9);
        assert!((snapshot["unrealized_pnl_usd"].as_f64().unwrap() - 3.02).abs() < 1e-9);
        assert_eq!(snapshot["position_count"], 1);
    }
}
