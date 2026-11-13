//! Derive paper-position values from the market price already accepted by market data.
//! This module does not write to the token registry or change the market-data price.

use super::super::*;
use nodex_trading::main_bot::{evaluate_position_exit, PositionRiskPolicy};

/// Mark one open position from the authoritative market tick.
/// Returns true only when this tick creates a new exit request for the position.
pub(crate) fn mark_position(
    position: &mut serde_json::Value,
    price_sol: f64,
    price_snapshot: &SolPriceSnapshot,
) -> bool {
    if !price_sol.is_finite() || price_sol <= 0.0 {
        return false;
    }

    let sol_usd = price_snapshot.price_usd;
    let usd_quote_valid =
        price_snapshot.status == "live" && sol_usd.is_finite() && sol_usd > 0.0;
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
    let entry_sol_usd = position
        .get("entry_sol_price_usd")
        .or_else(|| position.get("sol_usd_at_entry"))
        .and_then(serde_json::Value::as_f64)
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(sol_usd);
    let invested_usd = position
        .get("initial_investment_usd")
        .or_else(|| position.get("cost_basis_usd"))
        .and_then(serde_json::Value::as_f64)
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(cost_basis * entry_sol_usd);
    let entry_price_usd = position
        .get("entry_price_usd")
        .and_then(serde_json::Value::as_f64)
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or_else(|| {
            position
                .get("entry_price_sol")
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(price_sol)
                * entry_sol_usd
        });
    let current_price_usd = price_sol * sol_usd;
    let current_value_usd = current_value * sol_usd;
    let pnl_usd = current_value_usd - invested_usd;
    let pnl_pct = if invested_usd > 0.0 {
        pnl_usd / invested_usd * 100.0
    } else {
        0.0
    };

    if let Some(object) = position.as_object_mut() {
        object.insert("current_price_sol".into(), json!(price_sol));
        object.insert("current_value_sol".into(), json!(current_value));
        object.insert("unrealized_pnl_sol".into(), json!(pnl));
        if usd_quote_valid {
            object.insert("current_price_usd".into(), json!(current_price_usd));
            object.insert("entry_price_usd".into(), json!(entry_price_usd));
            object.insert("initial_investment_usd".into(), json!(invested_usd));
            object.insert("current_position_value_usd".into(), json!(current_value_usd));
            object.insert("pnl_usd".into(), json!(pnl_usd));
            object.insert("unrealized_pnl_usd".into(), json!(pnl_usd));
            object.insert("pnl_pct".into(), json!(pnl_pct));
            object.insert("unrealized_pnl_pct".into(), json!(pnl_pct));
            object.insert("sol_price_usd_at_mark".into(), json!(sol_usd));
            object.insert("sol_price_source_at_mark".into(), json!(price_snapshot.source.clone()));
        }
        object.insert("marked_at".into(), json!(chrono::Utc::now().to_rfc3339()));
    }

    // Keep the existing per-position risk policy behavior; execution remains paper-only.
    let risk_value = position.get("risk_policy").cloned();
    let mut exit_was_triggered = false;
    if let Some(value) = risk_value {
        if let Ok(mut policy) = serde_json::from_value::<PositionRiskPolicy>(value) {
            let trigger = evaluate_position_exit(&mut policy, price_sol);
            if let Some(object) = position.as_object_mut() {
                object.insert(
                    "risk_policy".into(),
                    serde_json::to_value(&policy).unwrap_or(serde_json::Value::Null),
                );
                if let Some(trigger) = trigger {
                    if object.get("exit_requested").and_then(serde_json::Value::as_bool)
                        != Some(true)
                    {
                        object.insert("exit_requested".into(), json!(true));
                        object.insert("exit_trigger_reason".into(), json!(trigger.reason));
                        object.insert("exit_trigger_price_sol".into(), json!(trigger.trigger_price_sol));
                        object.insert("exit_trigger_pnl_pct".into(), json!(trigger.pnl_pct));
                        object.insert("exit_triggered_at".into(), json!(chrono::Utc::now().to_rfc3339()));
                        exit_was_triggered = true;
                    }
                }
            }
        }
    }
    exit_was_triggered
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mark_position_updates_position_pnl_without_touching_market_state() {
        let mut position = json!({
            "mint_address":"MintA",
            "status":"open",
            "token_quantity":10.0,
            "cost_basis_sol":2.0,
            "entry_price_sol":0.2,
            "risk_policy": null
        });
        let quote = SolPriceSnapshot {
            price_usd: 100.0,
            source: "test".into(),
            updated_at: chrono::Utc::now(),
            status: "live".into(),
            last_error: None,
        };
        let triggered = mark_position(&mut position, 0.3, &quote);
        assert!(!triggered);
        assert_eq!(position["current_price_sol"], json!(0.3));
        assert_eq!(position["current_value_sol"], json!(3.0));
        assert_eq!(position["unrealized_pnl_sol"], json!(1.0));
        assert_eq!(position["unrealized_pnl_usd"], json!(100.0));
        assert_eq!(position["mint_address"], json!("MintA"));
    }

    #[test]
    fn invalid_market_price_does_not_overwrite_position_valuation() {
        let mut position = json!({"current_price_sol":0.5,"unrealized_pnl_sol":1.0});
        let quote = SolPriceSnapshot {
            price_usd: 100.0,
            source: "test".into(),
            updated_at: chrono::Utc::now(),
            status: "live".into(),
            last_error: None,
        };
        assert!(!mark_position(&mut position, f64::NAN, &quote));
        assert_eq!(position["current_price_sol"], json!(0.5));
        assert_eq!(position["unrealized_pnl_sol"], json!(1.0));
    }
}
