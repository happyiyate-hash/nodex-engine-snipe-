use serde_json::Value;

pub(super) fn is_frontend_event(value: &Value) -> bool {
    let event_type = value.get("type").and_then(Value::as_str);
    if event_type == Some("engine_activity") {
        let activity = value.get("data").unwrap_or(value);
        let severity = activity
            .get("severity")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let http_status = activity
            .get("http_status")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let has_error = activity
            .get("error")
            .and_then(Value::as_str)
            .is_some_and(|message| !message.is_empty());
        // Successful RPC request/response cards overwhelm the operator stream.
        // Keep provider failures visible; lifecycle events are forwarded separately.
        return severity.eq_ignore_ascii_case("error")
            || severity.eq_ignore_ascii_case("warning")
            || http_status >= 400
            || has_error;
    }

    matches!(
        event_type,
        Some("tokens_removed_inactive")
            | Some("connection_status")
            | Some("engine_state")
            | Some("trading_config_updated")
            | Some("launch_detected")
            | Some("new_token_detected")
            | Some("token_removed")
            | Some("token_rejected_cleanup")
            | Some("token_approval_pending")
            | Some("token_scan_retry_scheduled")
            | Some("token_scan_started")
            | Some("token_scan_update")
            | Some("token_scan_progress")
            | Some("token_scan_completed")
            | Some("token_scan_failed")
            | Some("token_monitoring_started")
            | Some("token_monitoring_update")
            | Some("token_monitoring_completed")
            | Some("token_diagnostic")
            | Some("token_metadata_started")
            | Some("token_metadata_failed")
            | Some("token_metadata_updated")
            | Some("token_state_updated")
            | Some("profile_updated")
            | Some("sol_price_update")
            | Some("market_price_update")
            | Some("token_market_update")
            | Some("token_statistics_update")
            | Some("chart_candle_update")
            | Some("chart_candle_closed")
            | Some("position_updated")
            | Some("portfolio_summary_updated")
            | Some("paper_buy_started")
            | Some("paper_buy_executed")
            | Some("paper_buy_failed")
            | Some("paper_sell_executed")
            | Some("paper_sell_failed")
            | Some("paper_entry_evaluation")
            | Some("paper_exit_triggered")
            | Some("strategy_rejected_token")
            | Some("diagnostic")
            | Some("provider_status")
            | Some("market_account_update")
            | Some("market_curve_completed")
            | Some("market_monitor_status")
            | Some("market_subscription_requested")
            | Some("market_subscription_subscribed")
            | Some("market_subscription_active")
            | Some("market_subscription_waiting")
            | Some("market_subscription_failed")
            | Some("market_subscriptions_removed")
            | Some("creator_history_started")
            | Some("creator_history_update")
            | Some("stream_warning")
            | Some("reconnect_recovery_status")
            | Some("pumpswap_activity")
            | Some("pumpswap_trade_activity")
            | Some("pump_fun_trade_activity")
            | Some("pumpswap_volume_update")
            | Some("command_rejected")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paper_exit_lifecycle_events_are_forwarded_to_the_frontend() {
        for event_type in [
            "paper_exit_triggered",
            "paper_sell_executed",
            "paper_sell_failed",
        ] {
            assert!(
                is_frontend_event(&serde_json::json!({"type":event_type,"data":{}})),
                "{event_type} must be forwarded"
            );
        }
    }
}
