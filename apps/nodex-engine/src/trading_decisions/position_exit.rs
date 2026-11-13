//! Exit decisions consume the latest portfolio valuation; they do not calculate P&L.

use serde_json::{json, Value};
use nodex_trading::main_bot::{evaluate_position_exit, PositionRiskPolicy};

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
    let pnl_sol = position
        .get("unrealized_pnl_sol")
        .and_then(Value::as_f64);
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
    let Some(trigger) = trigger else {
        return false;
    };
    if object.get("exit_requested").and_then(Value::as_bool) == Some(true) {
        return false;
    }

    object.insert("exit_requested".into(), json!(true));
    object.insert("exit_trigger_reason".into(), json!(trigger.reason));
    object.insert("exit_trigger_price_sol".into(), json!(trigger.trigger_price_sol));
    object.insert("exit_trigger_pnl_pct".into(), json!(trigger.pnl_pct));
    object.insert("exit_decision_unrealized_pnl_sol".into(), json!(pnl_sol));
    object.insert("exit_decision_unrealized_pnl_pct".into(), json!(pnl_pct));
    object.insert("exit_triggered_at".into(), json!(chrono::Utc::now().to_rfc3339()));
    true
}

#[cfg(test)]
mod tests {
    use super::*;

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
