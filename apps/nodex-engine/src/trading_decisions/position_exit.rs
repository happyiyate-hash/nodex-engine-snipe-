//! Exit decisions consume the latest portfolio valuation; they do not calculate P&L.

use nodex_trading::main_bot::{evaluate_position_exit, PositionRiskPolicy};
use serde_json::{json, Value};

/// Classify whether a position has the measured, orderly market behaviour
/// needed for an optional trailing stop. Missing evidence fails closed.
pub(crate) fn assess_trailing_stop_eligibility(evidence: &Value) -> (bool, String) {
    let liquidity = evidence.get("liquidity_usd").and_then(Value::as_f64);
    if !liquidity.is_some_and(|value| value.is_finite() && value >= 3_000.0) {
        return (false, "Trailing stop unavailable: confirmed liquidity is below $3,000 or unavailable.".into());
    }
    let pressure = evidence.get("buy_pressure_pct").and_then(Value::as_f64);
    if !pressure.is_some_and(|value| value.is_finite() && value >= 55.0) {
        return (false, "Trailing stop unavailable: confirmed buy pressure is below 55% or unavailable.".into());
    }
    let buys = evidence.get("buy_count_5m").and_then(Value::as_u64).unwrap_or(0);
    let sells = evidence.get("sell_count_5m").and_then(Value::as_u64).unwrap_or(0);
    if buys < 2 || buys < sells {
        return (false, "Trailing stop unavailable: recent confirmed buys do not sufficiently outweigh sells.".into());
    }
    let change_1m = evidence.get("price_change_1m_pct").and_then(Value::as_f64);
    if !change_1m.is_some_and(|value| value.is_finite() && (-20.0..=12.0).contains(&value)) {
        return (false, "Trailing stop unavailable: one-minute price behaviour is missing or too volatile.".into());
    }
    let change_5m = evidence.get("price_change_5m_pct").and_then(Value::as_f64);
    if !change_5m.is_some_and(|value| value.is_finite() && (-40.0..=35.0).contains(&value)) {
        return (false, "Trailing stop unavailable: five-minute price behaviour is missing or too volatile.".into());
    }
    (true, "Eligible: liquidity, confirmed buy pressure and recent price movement meet the orderly-trend criteria.".into())
}

/// Evaluate an open paper position using its already-computed P&L and risk policy.
/// Returns true only when this evaluation creates a new exit request.
pub(crate) fn evaluate_exit(position: &mut Value, price_sol: f64) -> bool {
    if !price_sol.is_finite() || price_sol <= 0.0 {
        return false;
    }
    let pnl_pct = position
        .get("unrealized_pnl_pct")
        .or_else(|| position.get("pnl_pct"))
        .and_then(Value::as_f64);
    let pnl_sol = position.get("unrealized_pnl_sol").and_then(Value::as_f64);
    if !pnl_sol.is_some_and(f64::is_finite) {
        return false;
    }

    let Some(risk_value) = position.get("risk_policy").cloned() else {
        return false;
    };
    let Ok(mut policy) = serde_json::from_value::<PositionRiskPolicy>(risk_value) else {
        return false;
    };
    let trigger = evaluate_position_exit(&mut policy, price_sol);
    let Some(object) = position.as_object_mut() else {
        return false;
    };
    object.insert(
        "risk_policy".into(),
        serde_json::to_value(&policy).unwrap_or(Value::Null),
    );
    object.insert("trailing_stop_eligible".into(), json!(policy.trailing_stop_eligible));
    object.insert("trailing_stop_eligibility_reason".into(), json!(policy.trailing_stop_eligibility_reason));
    object.insert("trailing_stop_enabled".into(), json!(policy.trailing_stop_enabled));
    object.insert("trailing_activation_profit_pct".into(), json!(policy.trailing_activation_profit_pct));
    object.insert("trailing_stop_callback_pct".into(), json!(policy.trailing_stop_callback_pct));
    object.insert("trailing_stop_active".into(), json!(policy.trailing_stop_active));
    object.insert("trailing_stop_price_sol".into(), json!(policy.trailing_stop_price_sol));
    object.insert("maximum_profit_pct".into(), json!(policy.maximum_profit_pct));
    object.insert("maximum_profit_price_sol".into(), json!(policy.maximum_profit_price_sol));
    object.insert("highest_price_sol".into(), json!(policy.highest_price_sol));
    let Some(trigger) = trigger else {
        return false;
    };
    if object.get("exit_requested").and_then(Value::as_bool) == Some(true) {
        return false;
    }

    object.insert("exit_requested".into(), json!(true));
    object.insert("exit_trigger_reason".into(), json!(trigger.reason));
    object.insert(
        "exit_trigger_price_sol".into(),
        json!(trigger.trigger_price_sol),
    );
    object.insert("exit_trigger_pnl_pct".into(), json!(trigger.pnl_pct));
    object.insert("exit_decision_unrealized_pnl_sol".into(), json!(pnl_sol));
    object.insert("exit_decision_unrealized_pnl_pct".into(), json!(pnl_pct));
    object.insert(
        "exit_triggered_at".into(),
        json!(chrono::Utc::now().to_rfc3339()),
    );
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trailing_stop_eligibility_requires_confirmed_orderly_market_evidence() {
        let evidence = json!({
            "liquidity_usd": 8000.0,
            "buy_pressure_pct": 62.0,
            "buy_count_5m": 8,
            "sell_count_5m": 3,
            "price_change_1m_pct": 4.0,
            "price_change_5m_pct": 18.0
        });
        assert!(assess_trailing_stop_eligibility(&evidence).0);
    }

    #[test]
    fn explosive_or_incomplete_market_evidence_is_not_trailing_eligible() {
        let explosive = json!({
            "liquidity_usd": 8000.0,
            "buy_pressure_pct": 80.0,
            "buy_count_5m": 9,
            "sell_count_5m": 1,
            "price_change_1m_pct": 18.0,
            "price_change_5m_pct": 30.0
        });
        let incomplete = json!({"liquidity_usd":8000.0});
        assert!(!assess_trailing_stop_eligibility(&explosive).0);
        assert!(!assess_trailing_stop_eligibility(&incomplete).0);
    }

    #[test]
    fn hard_maximum_profit_exits_at_five_hundred_percent_while_price_is_rising() {
        let config = nodex_trading::main_bot::TradingBotConfig::default();
        let mut policy = nodex_trading::main_bot::capture_position_risk(&config, 1.0, 100.0).unwrap();
        policy.take_profit_pct = 0.0;
        policy.take_profit_price_sol = 0.0;
        policy.maximum_profit_pct = 500.0;
        policy.maximum_profit_price_sol = 6.0;
        policy.trailing_stop_eligible = true;
        policy.trailing_stop_enabled = true;
        policy.trailing_activation_profit_pct = 50.0;
        let trigger = nodex_trading::main_bot::evaluate_position_exit(&mut policy, 6.0).unwrap();
        assert_eq!(trigger.reason, nodex_trading::main_bot::PositionExitReason::MaximumProfit);
        assert_eq!(trigger.pnl_pct, 500.0);
    }

    #[test]
    fn trailing_stop_activates_at_profit_threshold_and_tracks_high_water_mark() {
        let config = nodex_trading::main_bot::TradingBotConfig::default();
        let mut policy = nodex_trading::main_bot::capture_position_risk(&config, 1.0, 100.0).unwrap();
        policy.take_profit_pct = 0.0;
        policy.take_profit_price_sol = 0.0;
        policy.maximum_profit_pct = 500.0;
        policy.trailing_stop_eligible = true;
        policy.trailing_stop_enabled = true;
        policy.trailing_activation_profit_pct = 50.0;
        policy.trailing_stop_callback_pct = 10.0;
        assert!(nodex_trading::main_bot::evaluate_position_exit(&mut policy, 1.5).is_none());
        assert!(policy.trailing_stop_active);
        assert!((policy.trailing_stop_price_sol - 1.35).abs() < 1e-9);
        let trigger = nodex_trading::main_bot::evaluate_position_exit(&mut policy, 1.34).unwrap();
        assert_eq!(trigger.reason, nodex_trading::main_bot::PositionExitReason::TrailingStop);
    }

    #[test]
    fn exit_decision_requires_a_fresh_portfolio_valuation() {
        let mut position = json!({
            "risk_policy": {},
            "current_price_sol": 0.5
        });
        assert!(!evaluate_exit(&mut position, 0.5));
        assert_ne!(position.get("exit_requested"), Some(&json!(true)));
    }

    #[test]
    fn exit_decision_rejects_invalid_prices() {
        let mut position = json!({
            "unrealized_pnl_sol": 1.0,
            "unrealized_pnl_pct": 50.0,
            "risk_policy": {}
        });
        assert!(!evaluate_exit(&mut position, f64::NAN));
    }
}
