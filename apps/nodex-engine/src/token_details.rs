use serde_json::{json, Value};

/// Builds the stable payload consumed by the token-details screen.
/// Missing provider values remain null; clients should render them as an em dash.
pub fn build_token_detail_snapshot(
    mint: &str,
    state: Option<&nodex_core::TokenState>,
    positions: &[Value],
) -> Value {
    let data = state.map(|item| &item.data);
    let identity = data
        .and_then(|value| value.get("identity"))
        .cloned()
        .unwrap_or_else(|| json!({}));
    let market = data
        .and_then(|value| value.get("market"))
        .cloned()
        .unwrap_or_else(|| json!({}));
    let statistics = data
        .and_then(|value| value.get("statistics"))
        .cloned()
        .unwrap_or_else(|| json!({}));
    let chart = data
        .and_then(|value| value.get("chart"))
        .cloned()
        .unwrap_or_else(|| {
            json!({
                "status": "unavailable",
                "timeframe": "5m",
                "candles": [],
                "message": "Historical candle data is not available yet"
            })
        });

    let matching: Vec<&Value> = positions
        .iter()
        .filter(|position| {
            position.get("mint_address").and_then(Value::as_str) == Some(mint)
                && position.get("status").and_then(Value::as_str) == Some("open")
        })
        .collect();

    let token_balance: f64 = matching
        .iter()
        .map(|position| {
            position
                .get("token_quantity")
                .or_else(|| position.get("quantity"))
                .and_then(Value::as_f64)
                .unwrap_or(0.0)
        })
        .sum();
    let total_budget_sol: f64 = matching
        .iter()
        .map(|position| {
            position
                .get("cost_basis_sol")
                .or_else(|| position.get("invested_sol"))
                .and_then(Value::as_f64)
                .unwrap_or(0.0)
        })
        .sum();
    let current_value_sol: f64 = matching
        .iter()
        .map(|position| {
            position
                .get("current_value_sol")
                .and_then(Value::as_f64)
                .unwrap_or(0.0)
        })
        .sum();
    let unrealized_pnl_sol = current_value_sol - total_budget_sol;
    let average_entry_price_sol = if token_balance > 0.0 {
        total_budget_sol / token_balance
    } else {
        0.0
    };

    let mut market_data = market;
    if !market_data.is_object() {
        market_data = json!({});
    }
    let market_object = market_data
        .as_object_mut()
        .expect("market object normalized");
    for field in [
        "price_sol",
        "price_usd",
        "price_change_24h_pct",
        "market_cap_usd",
        "liquidity_usd",
        "volume_5m_usd",
        "volume_1h_usd",
        "buys_5m",
        "sells_5m",
        "holders",
        "bonding_curve_pct",
    ] {
        market_object.entry(field).or_insert(Value::Null);
    }

    let volume_5m = statistics
        .pointer("/pumpswap_volume/window_5m/total_volume_raw")
        .and_then(Value::as_str)
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(|lamports| lamports as f64 / 1_000_000_000.0);
    let buys_5m = statistics
        .pointer("/pumpswap_volume/window_5m/buy_trade_count")
        .cloned();
    let sells_5m = statistics
        .pointer("/pumpswap_volume/window_5m/sell_trade_count")
        .cloned();

    let mut statistics_data = json!({
        "market_cap_sol": market_data.get("market_cap_sol").cloned().unwrap_or(Value::Null),
        "liquidity_sol": Value::Null,
        "volume_5m_sol": volume_5m.map_or(Value::Null, |value| json!(value)),
        "volume_1h_sol": Value::Null,
        "buys_5m": buys_5m.unwrap_or(Value::Null),
        "sells_5m": sells_5m.unwrap_or(Value::Null),
        "holders": Value::Null,
        "bonding_curve_pct": Value::Null,
        "source_status": if statistics.get("pumpswap_volume").is_some() {
            "partial"
        } else {
            "waiting_for_provider"
        }
    });
    if let Some(object) = statistics_data.as_object_mut() {
        object.insert(
            "volume_5m_raw".into(),
            statistics
                .pointer("/pumpswap_volume/window_5m/total_volume_raw")
                .cloned()
                .unwrap_or(Value::Null),
        );
        object.insert(
            "quote_amount_unit".into(),
            statistics
                .pointer("/pumpswap_volume/quote_amount_unit")
                .cloned()
                .unwrap_or(Value::Null),
        );
    }

    json!({
        "mint_address": mint,
        "version": state.map(|item| item.version).unwrap_or(0),
        "updated_at": state.map(|item| item.updated_at.to_rfc3339()),
        "diagnostics": data.and_then(|value| value.get("diagnostics")).cloned().unwrap_or_else(|| json!([])),
        // Return the canonical persisted scanner report with the detail snapshot.
        // Missing evidence remains absent/null; the UI must not infer a pass.
        "security": data.and_then(|value| value.get("security")).cloned().unwrap_or(Value::Null),
        "identity": {
            "name": identity.get("name").cloned().unwrap_or(Value::Null),
            "symbol": identity.get("symbol").cloned().unwrap_or(Value::Null),
            "logo_url": identity.get("logo_url").cloned().unwrap_or(Value::Null),
            "metadata_uri": identity.get("metadata_uri").cloned().unwrap_or(Value::Null),
            "decimals": identity.get("decimals").cloned().unwrap_or(Value::Null),
            "total_supply": identity.get("total_supply").or_else(|| identity.get("total_supply_raw")).cloned().unwrap_or(Value::Null),
            "total_supply_raw": identity.get("total_supply_raw").cloned().unwrap_or(Value::Null)
        },
        "market": market_data,
        "statistics": statistics_data,
        "chart": chart,
        "position": {
            "has_position": !matching.is_empty(),
            "token_balance": token_balance,
            "average_entry_price_sol": if matching.is_empty() { Value::Null } else { json!(average_entry_price_sol) },
            "total_budget_sol": total_budget_sol,
            "current_value_sol": current_value_sol,
            "unrealized_pnl_sol": unrealized_pnl_sol,
            "unrealized_pnl_pct": if total_budget_sol > 0.0 { json!(unrealized_pnl_sol / total_budget_sol * 100.0) } else { Value::Null }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use nodex_core::TokenState;

    #[test]
    fn snapshot_keeps_unavailable_values_null_and_builds_position() {
        let mut state = TokenState::new("mint-1");
        state.data = json!({
            "identity": {"name":"Example", "symbol":"EX"},
            "market": {"price_sol":0.25},
            "security": {
                "decision":"UNVERIFIED",
                "decision_reason":"Holder evidence is incomplete.",
                "checks":[{"name":"mint_authority","status":"passed"}]
            }
        });
        let positions = vec![json!({
            "mint_address":"mint-1", "status":"open",
            "token_quantity":10.0, "cost_basis_sol":1.0,
            "current_value_sol":2.5
        })];
        let snapshot = build_token_detail_snapshot("mint-1", Some(&state), &positions);
        assert_eq!(snapshot["identity"]["name"], "Example");
        assert_eq!(snapshot["market"]["price_sol"], 0.25);
        assert_eq!(snapshot["security"]["decision"], "UNVERIFIED");
        assert_eq!(snapshot["security"]["checks"][0]["name"], "mint_authority");
        assert!(snapshot["market"]["holders"].is_null());
        assert_eq!(snapshot["position"]["token_balance"], 10.0);
        assert_eq!(snapshot["position"]["unrealized_pnl_sol"], 1.5);
        assert_eq!(snapshot["chart"]["status"], "unavailable");
    }

    #[test]
    fn no_position_is_not_reported_as_unknown_balance() {
        let state = TokenState {
            mint_address: "mint-1".into(),
            version: 1,
            updated_at: Utc::now(),
            data: json!({}),
        };
        let snapshot = build_token_detail_snapshot("mint-1", Some(&state), &[]);
        assert_eq!(snapshot["position"]["has_position"], false);
        assert_eq!(snapshot["position"]["token_balance"], 0.0);
        assert!(snapshot["position"]["average_entry_price_sol"].is_null());
    }
}
