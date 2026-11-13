//! Pure portfolio aggregation. It never fetches or mutates market prices.

use super::super::*;

/// Build the one authoritative USD portfolio valuation used by REST, SQLite and WebSocket.
pub(crate) fn calculate_portfolio_snapshot(
    wallet: &PaperWallet,
    price: &SolPriceSnapshot,
    positions: &[serde_json::Value],
) -> serde_json::Value {
    let sol_price_usd = if price.price_usd.is_finite() && price.price_usd > 0.0 {
        price.price_usd
    } else {
        0.0
    };
    let open_positions: Vec<serde_json::Value> = positions
        .iter()
        .filter(|position| {
            position.get("status").and_then(serde_json::Value::as_str) == Some("open")
        })
        .map(|position| {
            let mut value = position.clone();
            if let Some(fields) = value.as_object_mut() {
                if let Some(value_sol) = fields
                    .get("current_value_sol")
                    .and_then(serde_json::Value::as_f64)
                {
                    fields.insert("current_value_usd".into(), json!(value_sol * sol_price_usd));
                }
                if let Some(price_sol) = fields
                    .get("current_price_sol")
                    .and_then(serde_json::Value::as_f64)
                {
                    fields.insert("current_price_usd".into(), json!(price_sol * sol_price_usd));
                }
                fields.insert("sol_price_usd".into(), json!(sol_price_usd));
            }
            value
        })
        .collect();
    let sum = |field: &str| -> f64 {
        open_positions
            .iter()
            .filter_map(|position| position.get(field).and_then(serde_json::Value::as_f64))
            .filter(|value| value.is_finite())
            .sum()
    };
    let positions_value_sol = sum("current_value_sol");
    let unrealized_pnl_sol = sum("unrealized_pnl_sol");
    let realized_pnl_sol: f64 = positions
        .iter()
        .filter_map(|position| {
            position.get("realized_pnl_sol").and_then(serde_json::Value::as_f64)
        })
        .filter(|value| value.is_finite())
        .sum();
    let available_sol = wallet.available_balance_lamports as f64 / LAMPORTS_PER_SOL as f64;
    let invested_sol = wallet.invested_balance_lamports as f64 / LAMPORTS_PER_SOL as f64;
    let locked_rent_sol = wallet.locked_rent_lamports as f64 / LAMPORTS_PER_SOL as f64;
    let total_sol = available_sol + positions_value_sol + locked_rent_sol;
    let starting_sol_price_usd =
        if wallet.starting_sol_price_usd.is_finite() && wallet.starting_sol_price_usd > 0.0 {
            wallet.starting_sol_price_usd
        } else {
            sol_price_usd
        };
    let starting_balance_sol = wallet.starting_balance_lamports as f64 / LAMPORTS_PER_SOL as f64;
    let starting_balance_usd = starting_balance_sol * starting_sol_price_usd;
    let sol_unrealized_pnl_usd =
        (available_sol + locked_rent_sol) * (sol_price_usd - starting_sol_price_usd);
    let token_unrealized_pnl_usd = unrealized_pnl_sol * sol_price_usd;
    let unrealized_pnl_usd = sol_unrealized_pnl_usd + token_unrealized_pnl_usd;
    let portfolio_balance_usd = total_sol * sol_price_usd;
    let total_pnl_usd = portfolio_balance_usd - starting_balance_usd;
    let unrealized_pnl_pct = if starting_balance_usd > 0.0 {
        unrealized_pnl_usd / starting_balance_usd * 100.0
    } else {
        0.0
    };
    let total_pnl_pct = if starting_balance_usd > 0.0 {
        total_pnl_usd / starting_balance_usd * 100.0
    } else {
        0.0
    };
    json!({
        "portfolio_balance_sol": total_sol, "total_portfolio_value_sol": total_sol,
        "portfolio_balance_usd": portfolio_balance_usd, "total_portfolio_value_usd": portfolio_balance_usd,
        "total_value_usd": portfolio_balance_usd, "available_balance_usd": available_sol * sol_price_usd,
        "invested_balance_usd": invested_sol * sol_price_usd, "starting_balance_usd": starting_balance_usd,
        "starting_balance_sol": starting_balance_sol, "starting_sol_price_usd": starting_sol_price_usd,
        "unrealized_pnl_usd": unrealized_pnl_usd, "sol_unrealized_pnl_usd": sol_unrealized_pnl_usd,
        "token_unrealized_pnl_usd": token_unrealized_pnl_usd, "unrealized_pnl_sol": unrealized_pnl_sol,
        "unrealized_pnl_pct": unrealized_pnl_pct, "realized_pnl_usd": realized_pnl_sol * sol_price_usd,
        "realized_pnl_sol": realized_pnl_sol, "total_pnl_usd": total_pnl_usd,
        "total_pnl_sol": if sol_price_usd > 0.0 { total_pnl_usd / sol_price_usd } else { 0.0 }, "total_pnl_pct": total_pnl_pct,
        "positions_value_usd": positions_value_sol * sol_price_usd, "positions_value_sol": positions_value_sol,
        "locked_rent_value_usd": locked_rent_sol * sol_price_usd, "available_balance_sol": available_sol,
        "invested_balance_sol": invested_sol, "locked_rent_sol": locked_rent_sol, "total_value_sol": total_sol,
        "sol_price_usd": sol_price_usd, "sol_price_source": price.source, "sol_price_updated_at": price.updated_at,
        "sol_price_status": price.status, "price_updated_at": price.updated_at, "price_status": price.status,
        "position_count": open_positions.len(), "positions": open_positions,
        "updated_at": chrono::Utc::now().to_rfc3339()
    })
}
