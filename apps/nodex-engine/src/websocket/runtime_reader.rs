use serde_json::{Map, Value};
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
    "creator_history_started",
    "creator_history_update",
    "pumpswap_activity",
    "pumpswap_trade_activity",
    "pump_fun_trade_activity",
    "pumpswap_volume_update",
];

fn event_mint(event: &Value) -> Option<String> {
    let data = event.get("data").unwrap_or(event);
    data.get("mint_address")
        .or_else(|| data.get("token_mint"))
        .or_else(|| data.get("base_mint"))
        .or_else(|| data.get("mint"))
        .or_else(|| {
            data.get("position").and_then(|position| {
                position
                    .get("mint_address")
                    .or_else(|| position.get("mint"))
            })
        })
        .or_else(|| event.get("token_mint"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .filter(|mint| !mint.trim().is_empty())
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
        loop {
            match runtime_events.recv().await {
                Ok(event) => {
                    if !super::is_frontend_event(&event) {
                        continue;
                    }
                    let event_type = event
                        .get("type")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned();
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
