use serde_json::Value;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

const POSITION_SCOPED_MARKET_EVENTS: &[&str] = &[
    "market_price_update",
    "token_state_updated",
    "token_market_update",
    "token_statistics_update",
    "chart_candle_update",
    "chart_candle_closed",
    "market_account_update",
    "market_curve_completed",
    "pumpswap_volume_update",
];

fn event_mint(event: &Value) -> Option<&str> {
    let data = event.get("data")?;
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
        .and_then(Value::as_str)
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
                        .unwrap_or_default();
                    if POSITION_SCOPED_MARKET_EVENTS.contains(&event_type) {
                        let Some(mint) = event_mint(&event) else {
                            tracing::warn!(
                                event_type,
                                "Dropped token market event without an exact mint address"
                            );
                            continue;
                        };
                        let positions = portfolio_positions.read().await;
                        if !has_open_position(&positions, mint) {
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
    fn market_events_require_an_exact_mint_and_open_position() {
        let positions = vec![json!({
            "mint_address":"MintOpen",
            "status":"OPEN"
        })];
        let open_event = json!({
            "type":"market_price_update",
            "data":{"mint_address":"MintOpen","price_sol":0.01}
        });
        let closed_event = json!({
            "type":"market_price_update",
            "data":{"mint_address":"MintClosed","price_sol":0.01}
        });
        let missing_mint_event = json!({
            "type":"market_price_update",
            "data":{"price_sol":0.01}
        });

        assert_eq!(event_mint(&open_event), Some("MintOpen"));
        assert!(has_open_position(&positions, "MintOpen"));
        assert!(!has_open_position(&positions, "MintClosed"));
        assert_eq!(event_mint(&missing_mint_event), None);
    }

    #[test]
    fn closed_positions_do_not_receive_market_ticks() {
        let positions = vec![json!({
            "mint":"MintClosed",
            "status":"CLOSED_PROFIT"
        })];
        assert!(!has_open_position(&positions, "MintClosed"));
    }
}
