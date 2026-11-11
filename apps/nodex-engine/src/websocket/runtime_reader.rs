use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

const POSITION_SCOPED_MARKET_EVENTS: &[&str] = &[
    "market_price_update",
    "token_market_update",
    "token_statistics_update",
    "chart_candle_update",
    "chart_candle_closed",
    "market_account_update",
    "market_curve_completed",
    "pumpswap_volume_update",
];

// Every token lifecycle/market/position event must identify one exact mint.
// System-wide events (connection, provider and portfolio summaries) are not
// token-scoped and intentionally remain outside this list.
const TOKEN_SCOPED_EVENTS: &[&str] = &[
    "launch_detected",
    "new_token_detected",
    "token_removed",
    "token_approval_pending",
    "token_scan_started",
    "token_scan_update",
    "token_scan_progress",
    "token_scan_completed",
    "token_scan_failed",
    "token_monitoring_started",
    "token_monitoring_update",
    "token_monitoring_completed",
    "token_diagnostic",
    "token_metadata_started",
    "token_metadata_failed",
    "token_metadata_updated",
    "token_state_updated",
    "market_price_update",
    "token_market_update",
    "token_statistics_update",
    "chart_candle_update",
    "chart_candle_closed",
    "market_account_update",
    "market_curve_completed",
    "paper_buy_started",
    "paper_buy_executed",
    "paper_buy_failed",
    "paper_entry_evaluation",
    "paper_exit_triggered",
    "paper_sell_executed",
    "paper_sell_failed",
    "position_updated",
    "strategy_rejected_token",
    "market_subscription_requested",
    "market_subscription_active",
    "market_subscription_waiting",
    "market_subscriptions_removed",
    "creator_history_started",
    "creator_history_update",
    "pumpswap_activity",
    "pumpswap_trade_activity",
    "pump_fun_trade_activity",
    "pumpswap_volume_update",
];

fn event_mint(event: &Value) -> Option<String> {
    let data = event.get("data").unwrap_or(event);
    let mut candidates = vec![
        data.get("mint_address").and_then(Value::as_str),
        data.get("token_mint").and_then(Value::as_str),
        data.get("base_mint").and_then(Value::as_str),
        data.get("mint").and_then(Value::as_str),
        data.get("position").and_then(|position| {
            position
                .get("mint_address")
                .or_else(|| position.get("mint"))
                .and_then(Value::as_str)
        }),
        event.get("mint_address").and_then(Value::as_str),
        event.get("token_mint").and_then(Value::as_str),
        event.get("base_mint").and_then(Value::as_str),
        event.get("mint").and_then(Value::as_str),
    ]
    .into_iter()
    .flatten()
    .filter(|mint| !mint.trim().is_empty());
    let mint = candidates.next()?;
    if candidates.any(|candidate| candidate != mint) {
        return None;
    }
    Some(mint.to_owned())
}

fn attach_exact_mint(event: &mut Value, mint: &str) {
    if let Some(data) = event.get_mut("data") {
        if let Some(object) = data.as_object_mut() {
            object
                .entry("mint_address")
                .or_insert_with(|| Value::String(mint.to_owned()));
        }
    } else if let Some(object) = event.as_object_mut() {
        object.insert(
            "data".to_owned(),
            Value::Object(Map::from_iter([(
                "mint_address".to_owned(),
                Value::String(mint.to_owned()),
            )])),
        );
    }
}

fn remember_verified_pool(event: &Value, pool_to_mint: &mut HashMap<String, String>) {
    if event.get("type").and_then(Value::as_str) != Some("market_pool_verified") {
        return;
    }
    let Some(data) = event.get("data") else {
        return;
    };
    let (Some(pool), Some(mint)) = (
        data.get("pool_address").and_then(Value::as_str),
        data.get("mint_address").and_then(Value::as_str),
    ) else {
        return;
    };
    if !pool.trim().is_empty() && !mint.trim().is_empty() {
        pool_to_mint.insert(pool.to_owned(), mint.to_owned());
    }
}

fn attach_verified_pool_mint(
    event: &mut Value,
    pool_to_mint: &HashMap<String, String>,
) -> bool {
    if event.get("type").and_then(Value::as_str) != Some("pumpswap_trade_activity")
        || event_mint(event).is_some()
    {
        return true;
    }
    let Some(pool) = event
        .get("data")
        .and_then(|data| data.get("pool_address"))
        .and_then(Value::as_str)
    else {
        return false;
    };
    let Some(mint) = pool_to_mint.get(pool) else {
        // The global PumpSwap log stream includes unrelated pools. Only forward
        // trades for pools that this engine has already verified and mapped.
        return false;
    };
    attach_exact_mint(event, mint);
    event_mint(event).as_deref() == Some(mint.as_str())
}

fn remove_verified_pools_for_mint(event: &Value, pool_to_mint: &mut HashMap<String, String>) {
    if event.get("type").and_then(Value::as_str) != Some("token_removed") {
        return;
    }
    if let Some(mint) = event_mint(event) {
        pool_to_mint.retain(|_, mapped_mint| mapped_mint != &mint);
    }
}

fn has_open_position(positions: &[Value], mint: &str) -> bool {
    positions.iter().any(|position| {
        let position_mint = position
            .get("mint_address")
            .or_else(|| position.get("mint"))
            .and_then(Value::as_str);
        let status = position
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("open");
        position_mint == Some(mint) && status.eq_ignore_ascii_case("open")
    })
}

pub(super) fn enforce_exact_mint(mut event: Value) -> Option<Value> {
    let event_type = event
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !TOKEN_SCOPED_EVENTS.contains(&event_type) {
        return Some(event);
    }
    let mint = event_mint(&event)?;
    attach_exact_mint(&mut event, &mint);
    Some(event)
}

pub(super) fn spawn(
    mut runtime_events: broadcast::Receiver<Value>,
    frontend_events: broadcast::Sender<Value>,
    portfolio_positions: Arc<RwLock<Vec<Value>>>,
) {
    tokio::spawn(async move {
        let mut pool_to_mint = HashMap::<String, String>::new();
        loop {
            match runtime_events.recv().await {
                Ok(mut event) => {
                    // Learn pool identity from verified market state before the
                    // frontend allowlist filters out this internal event.
                    remember_verified_pool(&event, &mut pool_to_mint);
                    remove_verified_pools_for_mint(&event, &mut pool_to_mint);
                    if !super::is_frontend_event(&event) {
                        continue;
                    }
                    let event_type = event
                        .get("type")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned();
                    if !attach_verified_pool_mint(&mut event, &pool_to_mint) {
                        continue;
                    }
                    let Some(event) = enforce_exact_mint(event) else {
                        tracing::warn!(
                            event_type = %event_type,
                            "Dropped token-scoped event without exact mint address"
                        );
                        continue;
                    };
                    if POSITION_SCOPED_MARKET_EVENTS.contains(&event_type.as_str()) {
                        let Some(mint) = event_mint(&event) else {
                            continue;
                        };
                        let positions = portfolio_positions.read().await;
                        if !has_open_position(&positions, &mint) {
                            continue;
                        }
                    }
                    let _ = frontend_events.send(event);
                }
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    tracing::warn!(dropped_events = count, "WebSocket runtime reader lagged");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn token_events_require_and_receive_an_exact_mint() {
        let mut event = json!({
            "type": "token_scan_progress",
            "data": {"progress": 45},
            "token_mint": "MintExact"
        });
        let mint = event_mint(&event).expect("top-level mint is accepted");
        attach_exact_mint(&mut event, &mint);
        assert_eq!(event["data"]["mint_address"], "MintExact");

        let missing = json!({
            "type": "token_scan_progress",
            "data": {"progress": 45}
        });
        assert_eq!(event_mint(&missing), None);
    }

    #[test]
    fn verified_pumpswap_pool_attaches_its_exact_mint_to_trade_events() {
        let mut pool_to_mint = HashMap::new();
        remember_verified_pool(
            &json!({
                "type": "market_pool_verified",
                "data": {
                    "pool_address": "PoolExact",
                    "mint_address": "MintExact"
                }
            }),
            &mut pool_to_mint,
        );

        let mut trade = json!({
            "type": "pumpswap_trade_activity",
            "data": { "pool_address": "PoolExact", "signature": "TradeSignature" }
        });
        assert!(attach_verified_pool_mint(&mut trade, &pool_to_mint));
        assert_eq!(trade["data"]["mint_address"], "MintExact");
        assert!(enforce_exact_mint(trade).is_some());
    }

    #[test]
    fn unverified_pumpswap_pool_trade_is_not_forwarded_without_a_mint() {
        let mut trade = json!({
            "type": "pumpswap_trade_activity",
            "data": { "pool_address": "UnknownPool" }
        });
        assert!(!attach_verified_pool_mint(&mut trade, &HashMap::new()));
    }

    #[test]
    fn removing_a_token_clears_its_verified_pool_mapping() {
        let mut pool_to_mint = HashMap::from([
            ("PoolOne".to_owned(), "MintOne".to_owned()),
            ("PoolTwo".to_owned(), "MintTwo".to_owned()),
        ]);
        remove_verified_pools_for_mint(
            &json!({
                "type": "token_removed",
                "data": { "mint_address": "MintOne" }
            }),
            &mut pool_to_mint,
        );
        assert_eq!(pool_to_mint.len(), 1);
        assert_eq!(pool_to_mint.get("PoolTwo").map(String::as_str), Some("MintTwo"));
    }

    #[test]
    fn closed_positions_do_not_receive_market_ticks() {
        let positions = vec![json!({
            "mint": "MintClosed",
            "status": "CLOSED_PROFIT"
        })];
        assert!(!has_open_position(&positions, "MintClosed"));
    }

    #[test]
    fn token_state_snapshots_are_not_hidden_for_tokens_without_positions() {
        assert!(!POSITION_SCOPED_MARKET_EVENTS.contains(&"token_state_updated"));
        assert!(TOKEN_SCOPED_EVENTS.contains(&"token_state_updated"));
    }
}
