use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
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
    "market_subscription_subscribed",
    "market_subscription_active",
    "market_subscription_waiting",
    "market_subscription_failed",
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

fn attach_verified_pool_mint(event: &mut Value, pool_to_mint: &HashMap<String, String>) -> bool {
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

fn should_forward_position_scoped_market_event(
    positions: &[Value],
    active_accounts_by_mint: &HashMap<String, HashSet<String>>,
    mint: &str,
) -> bool {
    has_open_position(positions, mint)
        || active_accounts_by_mint
            .get(mint)
            .is_some_and(|accounts| !accounts.is_empty())
}

fn update_active_subscription_tracking(
    event_type: &str,
    event: &Value,
    active_accounts_by_mint: &mut HashMap<String, HashSet<String>>,
) {
    let Some(mint) = event_mint(event) else {
        return;
    };
    let data = event.get("data").unwrap_or(event);
    let account = data
        .get("account_address")
        .or_else(|| data.get("account"))
        .and_then(Value::as_str)
        .filter(|account| !account.trim().is_empty())
        .map(str::to_owned);

    match event_type {
        "market_subscription_subscribed" | "market_subscription_active" => {
            if let Some(account) = account {
                active_accounts_by_mint
                    .entry(mint)
                    .or_default()
                    .insert(account);
            }
        }
        "market_subscription_failed" => {
            if let Some(account) = account {
                if let Some(accounts) = active_accounts_by_mint.get_mut(&mint) {
                    accounts.remove(&account);
                    if accounts.is_empty() {
                        active_accounts_by_mint.remove(&mint);
                    }
                }
            }
        }
        "market_subscriptions_removed" => {
            let accounts = data
                .get("account_addresses")
                .or_else(|| data.get("accounts"))
                .and_then(Value::as_array);
            if let Some(accounts) = accounts {
                if let Some(active) = active_accounts_by_mint.get_mut(&mint) {
                    for account in accounts.iter().filter_map(Value::as_str) {
                        active.remove(account);
                    }
                    if active.is_empty() {
                        active_accounts_by_mint.remove(&mint);
                    }
                }
            } else if let Some(account) = account {
                if let Some(active) = active_accounts_by_mint.get_mut(&mint) {
                    active.remove(&account);
                    if active.is_empty() {
                        active_accounts_by_mint.remove(&mint);
                    }
                }
            }
        }
        "token_removed" => {
            active_accounts_by_mint.remove(&mint);
        }
        // Waiting for capacity is an account-level pending state. It must never
        // deactivate other accounts that are already receiving updates.
        "market_subscription_waiting" => {}
        _ => {}
    }
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
        // Market data is useful for verified, actively subscribed tokens even
        // when no paper position exists. Keep the position check as a fallback
        // for position monitoring, but do not make it the only delivery path.
        let mut active_accounts_by_mint = HashMap::<String, HashSet<String>>::new();
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
                    if event_mint(&event).is_some() {
                        // The Solana JSON-RPC response includes a subscription ID and
                        // is emitted as "subscribed"; no separate "active" event exists.
                        update_active_subscription_tracking(
                            &event_type,
                            &event,
                            &mut active_accounts_by_mint,
                        );
                    }
                    if POSITION_SCOPED_MARKET_EVENTS.contains(&event_type.as_str()) {
                        let Some(mint) = event_mint(&event) else {
                            continue;
                        };
                        let positions = portfolio_positions.read().await;
                        if !should_forward_position_scoped_market_event(
                            &positions,
                            &active_accounts_by_mint,
                            &mint,
                        ) {
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
        assert_eq!(
            pool_to_mint.get("PoolTwo").map(String::as_str),
            Some("MintTwo")
        );
    }

    #[test]
    fn provider_subscription_acknowledgement_enables_market_ticks_without_a_position() {
        let mut active = HashMap::new();
        update_active_subscription_tracking(
            "market_subscription_subscribed",
            &json!({"type":"market_subscription_subscribed","data":{
                "mint_address":"MintVerified","account_address":"CurveAccount"
            }}),
            &mut active,
        );
        assert!(active
            .get("MintVerified")
            .is_some_and(|accounts| accounts.contains("CurveAccount")));
        assert!(should_forward_position_scoped_market_event(
            &[],
            &active,
            "MintVerified"
        ));

        update_active_subscription_tracking(
            "market_subscription_failed",
            &json!({"type":"market_subscription_failed","data":{
                "mint_address":"MintVerified","account_address":"CurveAccount"
            }}),
            &mut active,
        );
        assert!(!active.contains_key("MintVerified"));
    }

    #[test]
    fn closed_positions_do_not_receive_market_ticks_without_active_subscription() {
        let positions = vec![json!({
            "mint": "MintClosed",
            "status": "CLOSED_PROFIT"
        })];
        let active = HashMap::new();
        assert!(!should_forward_position_scoped_market_event(
            &positions,
            &active,
            "MintClosed"
        ));
    }

    #[test]
    fn actively_subscribed_verified_tokens_receive_market_ticks_without_positions() {
        let positions = Vec::new();
        let active = HashMap::from([(
            "MintVerified".to_owned(),
            HashSet::from(["CurveAccount".to_owned()]),
        )]);
        assert!(should_forward_position_scoped_market_event(
            &positions,
            &active,
            "MintVerified"
        ));
    }

    #[test]
    fn open_positions_keep_receiving_market_ticks_without_subscription_tracking() {
        let positions = vec![json!({
            "mint": "MintPosition",
            "status": "open"
        })];
        let active = HashMap::new();
        assert!(should_forward_position_scoped_market_event(
            &positions,
            &active,
            "MintPosition"
        ));
    }
    #[test]
    fn failing_one_account_does_not_deactivate_other_accounts_for_same_mint() {
        let mut active = HashMap::new();
        for account in ["CurveAccount", "PoolAccount", "VaultAccount"] {
            update_active_subscription_tracking(
                "market_subscription_subscribed",
                &json!({"type":"market_subscription_subscribed","data":{
                    "mint_address":"MintMulti","account_address":account
                }}),
                &mut active,
            );
        }
        update_active_subscription_tracking(
            "market_subscription_failed",
            &json!({"type":"market_subscription_failed","data":{
                "mint_address":"MintMulti","account_address":"PoolAccount"
            }}),
            &mut active,
        );
        assert_eq!(active["MintMulti"].len(), 2);
        assert!(should_forward_position_scoped_market_event(
            &[],
            &active,
            "MintMulti"
        ));
    }

    #[test]
    fn waiting_for_capacity_does_not_remove_existing_active_accounts() {
        let mut active = HashMap::from([(
            "MintMulti".to_owned(),
            HashSet::from(["CurveAccount".to_owned()]),
        )]);
        update_active_subscription_tracking(
            "market_subscription_waiting",
            &json!({"type":"market_subscription_waiting","data":{
                "mint_address":"MintMulti","status":"waiting_for_capacity"
            }}),
            &mut active,
        );
        assert!(should_forward_position_scoped_market_event(&[], &active, "MintMulti"));
    }

    #[test]
    fn token_state_snapshots_are_not_hidden_for_tokens_without_positions() {
        assert!(!POSITION_SCOPED_MARKET_EVENTS.contains(&"token_state_updated"));
        assert!(TOKEN_SCOPED_EVENTS.contains(&"token_state_updated"));
    }
}
