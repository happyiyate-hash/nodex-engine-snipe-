//! Paper-only entry evaluation and simulated execution.
//!
//! This module deliberately contains no signer, transaction builder, or live
//! execution path. It evaluates strategy gates and commits simulated fills only.

use super::*;
use serde_json::json;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

/// A verified security report is reusable for 15 minutes. This matches the
/// existing diagnostic scan retry window and avoids rescanning on every market tick.
pub(super) const SECURITY_APPROVAL_TTL_SECONDS: i64 = 900;

fn security_approval_cache_is_fresh(
    security: Option<&serde_json::Value>,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    security
        .and_then(|value| value.get("scan_cache"))
        .and_then(|cache| cache.get("approval_expires_at"))
        .and_then(serde_json::Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .is_some_and(|expires_at| expires_at > now)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn evaluate_and_execute_paper_entry(
    mint: &str,
    market_data: &serde_json::Value,
    trades: &VecDeque<ConfirmedTradeObservation>,
    quote_is_sol: bool,
    registry: &TokenRegistry,
    database: &sqlx::SqlitePool,
    profile: &Arc<RwLock<serde_json::Value>>,
    portfolio_positions: &Arc<RwLock<Vec<serde_json::Value>>>,
    wallet: &Arc<RwLock<PaperWallet>>,
    sol_price: &Arc<RwLock<SolPriceSnapshot>>,
    control: &Arc<RwLock<EngineControl>>,
    trading_settings: &Arc<RwLock<TradingSettings>>,
    events: &broadcast::Sender<serde_json::Value>,
    last_evaluation_signatures: &mut HashMap<String, String>,
    last_entry_attempts: &mut HashMap<String, Instant>,
) {
    let settings = trading_settings.read().await.clone();
    let engine = control.read().await.clone();
    let mut gate_reasons = Vec::new();
    if engine.paused {
        gate_reasons.push("Engine is paused.".to_owned());
    }
    if engine.mode != "paper" || !engine.available_modes.paper {
        gate_reasons.push("Paper mode is not the active execution mode.".to_owned());
    }
    if !engine.new_entries_enabled {
        gate_reasons.push("New paper entries are disabled in engine control.".to_owned());
    }
    if !settings.execution_enabled {
        gate_reasons.push("Paper execution master switch is OFF.".to_owned());
    }
    if !settings.auto_buy_enabled {
        gate_reasons.push("Automatic paper buying is OFF.".to_owned());
    }
    if settings.diagnostic_mode {
        gate_reasons.push("Diagnostic-only mode blocks paper entries.".to_owned());
    }

    let sol_quote = sol_price.read().await.clone();
    let sol_quote_valid =
        sol_quote.status == "live" && sol_quote.price_usd.is_finite() && sol_quote.price_usd > 0.0;
    let config = main_bot_config_from_settings(
        &settings,
        if sol_quote_valid {
            sol_quote.price_usd
        } else {
            0.0
        },
    );

    let now_ms = chrono::Utc::now().timestamp_millis();
    let recent_trades = trades
        .iter()
        .filter(|trade| now_ms.saturating_sub(trade.timestamp_ms) <= 300_000)
        .collect::<Vec<_>>();
    let mut buyers = HashSet::new();
    let mut buy_volume_sol = 0.0;
    let mut total_volume_sol = 0.0;
    let mut buy_count = 0_u64;
    let mut sell_count_5m = 0_u64;
    for trade in &recent_trades {
        let amount_sol = trade.sol_amount_lamports as f64 / LAMPORTS_PER_SOL as f64;
        total_volume_sol += amount_sol;
        if trade.is_buy {
            buy_volume_sol += amount_sol;
            buy_count = buy_count.saturating_add(1);
            buyers.insert(trade.trader.as_str());
        } else {
            sell_count_5m = sell_count_5m.saturating_add(1);
        }
    }

    let price_sol = market_data
        .get("price_sol")
        .and_then(serde_json::Value::as_f64);
    let reserve_lamports = if quote_is_sol {
        json_u64(
            market_data
                .get("real_sol_reserves")
                .or_else(|| market_data.get("quote_vault_reserves"))
                .or_else(|| market_data.get("virtual_sol_reserves")),
        )
    } else {
        None
    };
    let reserve_sol = reserve_lamports
        .map(|value| value as f64 / LAMPORTS_PER_SOL as f64)
        .filter(|value| value.is_finite() && *value > 0.0);
    let liquidity_usd = reserve_sol
        .filter(|_| sol_quote_valid)
        .map(|reserve| reserve * sol_quote.price_usd * 2.0)
        .or_else(|| {
            market_data
                .get("liquidity_usd")
                .and_then(serde_json::Value::as_f64)
        });
    let price_usd = price_sol
        .filter(|value| value.is_finite() && *value > 0.0 && sol_quote_valid)
        .map(|value| value * sol_quote.price_usd);
    let buy_volume_5m_usd = if sol_quote_valid {
        buy_volume_sol * sol_quote.price_usd
    } else {
        0.0
    };
    let total_volume_5m_usd = if sol_quote_valid {
        total_volume_sol * sol_quote.price_usd
    } else {
        0.0
    };
    let estimated_slippage_pct = reserve_sol
        .filter(|_| config.position_size_sol.is_finite() && config.position_size_sol > 0.0)
        .map(|reserve| {
            let ratio = config.position_size_sol / reserve;
            ((1.0 + ratio).powi(2) - 1.0) * 100.0
        });

    let token_snapshot = registry.get(mint);
    let security = token_snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.data.get("security"));
    let security_decision = security
        .and_then(|value| value.get("decision"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if !paper_entry_security_gate_passed(security)
        || !security_approval_cache_is_fresh(security, chrono::Utc::now())
    {
        gate_reasons.push(
            "Paper entry is blocked until the token has an explicit APPROVED verdict, all core checks pass, and its cached approval is still valid.".to_owned(),
        );
    }
    let rugcheck = security.and_then(|value| value.get("rugcheck"));
    let rugcheck_score = rugcheck
        .and_then(|value| value.get("score_normalised"))
        .and_then(serde_json::Value::as_f64)
        .or_else(|| {
            rugcheck
                .and_then(|value| value.get("score_normalised"))
                .and_then(serde_json::Value::as_u64)
                .map(|value| value as f64)
        });
    let rugcheck_available = rugcheck
        .and_then(|value| value.get("status"))
        .and_then(serde_json::Value::as_str)
        == Some("available");
    let rugcheck_acceptable = security_decision == "APPROVED"
        && rugcheck_available
        && rugcheck_score.is_some_and(|score| score.is_finite() && (0.0..=20.0).contains(&score));
    let holder_distribution = security.and_then(|value| value.get("holder_distribution"));
    let top10_concentration_pct = holder_distribution
        .filter(|value| {
            json_u64(value.get("resolved_accounts")).unwrap_or(0) >= 10
                && json_u64(value.get("sampled_token_accounts")).unwrap_or(0) >= 10
        })
        .and_then(|value| {
            let observed = value
                .get("top_10_observed_owner_share_percent")
                .and_then(serde_json::Value::as_f64)?;
            let partial = value
                .get("sample_is_partial")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(true);
            Some((observed + if partial { 10.0 } else { 0.0 }).min(100.0))
        });
    let safety_score = rugcheck_score
        .filter(|score| score.is_finite() && (0.0..=100.0).contains(score))
        .map(|score| 100.0 - score);
    let mint_authority_disabled = security
        .and_then(|value| value.get("mint_authority"))
        .is_some_and(serde_json::Value::is_null);
    let freeze_authority_disabled = security
        .and_then(|value| value.get("freeze_authority"))
        .is_some_and(serde_json::Value::is_null);
    let diagnostic_only = security
        .and_then(|value| value.get("diagnostic_only"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);
    let creator_history = security.and_then(|value| value.get("creator_history"));
    let creator_address = creator_history
        .and_then(|value| value.get("creator_address"))
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            token_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.data.get("identity"))
                .and_then(|identity| {
                    identity
                        .get("creator_wallet")
                        .or_else(|| identity.get("creator_address"))
                })
                .and_then(serde_json::Value::as_str)
        })
        .or_else(|| {
            token_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.data.get("creator_wallet"))
                .and_then(serde_json::Value::as_str)
        })
        .unwrap_or_default();
    let preferred_owner = settings
        .preferred_owners
        .iter()
        .any(|owner| owner.address == creator_address);
    let known_rug_creator = creator_history
        .and_then(|value| value.get("known_rug_creator"))
        .and_then(serde_json::Value::as_bool);
    let creator_score = creator_history
        .and_then(|value| value.get("activity_score"))
        .and_then(serde_json::Value::as_f64);

    let scanned_at_ms = security
        .and_then(|value| value.get("scanned_at"))
        .and_then(|value| timestamp_ms_from_value(Some(value)));
    let first_trade_ms = recent_trades.first().map(|trade| trade.timestamp_ms);
    let seconds_since_launch = scanned_at_ms
        .or(first_trade_ms)
        .map(|timestamp| now_ms.saturating_sub(timestamp).max(0) as u64 / 1_000)
        .unwrap_or(0);

    let security_assessment = SecurityAssessment {
        mint_authority_disabled: mint_authority_disabled && !diagnostic_only,
        freeze_authority_disabled: freeze_authority_disabled && !diagnostic_only,
        rugcheck_available,
        rugcheck_acceptable,
        safety_score,
        top10_concentration_pct,
    };
    let creator_assessment = CreatorReputation {
        score: creator_score,
        known_rug_creator,
        preferred_owner,
    };
    let market_assessment = LiveMarketActivity {
        price_usd,
        liquidity_usd,
        buy_volume_5m_usd,
        distinct_buyers_5m: buyers.len(),
        buy_count_5m: buy_count,
        sell_count_5m,
        estimated_slippage_pct,
        price_change_1m_pct: market_data
            .get("price_change_1m_pct")
            .and_then(serde_json::Value::as_f64),
        price_change_5m_pct: market_data
            .get("price_change_5m_pct")
            .and_then(serde_json::Value::as_f64),
        seconds_since_launch,
        confirmed_trade_events: recent_trades.len() as u64,
        buy_pressure_pct: (total_volume_5m_usd > 0.0)
            .then_some(buy_volume_5m_usd / total_volume_5m_usd * 100.0),
    };

    let open_positions = portfolio_positions.read().await;
    let open_positions_count = open_positions
        .iter()
        .filter(|position| {
            position.get("status").and_then(serde_json::Value::as_str) == Some("open")
        })
        .count();
    let mut evaluation = trading_decisions::entry::evaluate_entry_decision(
        &config,
        &creator_assessment,
        &security_assessment,
        &market_assessment,
        open_positions_count,
        gate_reasons,
        sol_quote_valid,
    );
    drop(open_positions);

    trading_decisions::entry::enforce_entry_quote_requirements(
        &mut evaluation,
        price_sol,
        price_usd,
    );

    let decision_name = match evaluation.decision {
        TradingBotDecision::Reject => "reject",
        TradingBotDecision::Wait => "wait",
        TradingBotDecision::EvaluatePaperEntry => "evaluate_paper_entry",
    };
    let evaluation_signature = format!("{decision_name}:{}", evaluation.reasons.join("|"));
    if last_evaluation_signatures.get(mint) != Some(&evaluation_signature) {
        last_evaluation_signatures.insert(mint.to_owned(), evaluation_signature);
        let _ = events.send(json!({
            "type":"paper_entry_evaluation",
            "data":{
                "mint_address":mint,
                "decision":decision_name,
                "entry_speed":evaluation.entry_speed,
                "creator_score":evaluation.creator_score,
                "minimum_distinct_buyers":evaluation.minimum_distinct_buyers,
                "reasons":evaluation.reasons,
                "metrics":{
                    "price_usd":market_assessment.price_usd,
                    "liquidity_usd":market_assessment.liquidity_usd,
                    "buy_volume_5m_usd":market_assessment.buy_volume_5m_usd,
                    "distinct_buyers_5m":market_assessment.distinct_buyers_5m,
                    "buy_count_5m":market_assessment.buy_count_5m,
                    "sell_count_5m":market_assessment.sell_count_5m,
                    "estimated_slippage_pct":market_assessment.estimated_slippage_pct,
                    "confirmed_trade_events":market_assessment.confirmed_trade_events,
                    "buy_pressure_pct":market_assessment.buy_pressure_pct
                },
                "evaluated_at":chrono::Utc::now().to_rfc3339(),
                "paper_only":true
            }
        }));
    }

    if evaluation.decision != TradingBotDecision::EvaluatePaperEntry {
        return;
    }
    if last_entry_attempts
        .get(mint)
        .is_some_and(|last_attempt| last_attempt.elapsed() < Duration::from_secs(30))
    {
        return;
    }
    last_entry_attempts.insert(mint.to_owned(), Instant::now());

    let Some(price_sol) = price_sol.filter(|value| value.is_finite() && *value > 0.0) else {
        return;
    };
    let Some(price_usd) = price_usd.filter(|value| value.is_finite() && *value > 0.0) else {
        return;
    };
    let position_size_sol = config.position_size_sol;
    if !position_size_sol.is_finite() || position_size_sol <= 0.0 {
        let _ = events.send(json!({"type":"paper_buy_failed","data":{
            "mint_address":mint,"reason":"Configured paper position size is not valid in SOL.","paper_only":true
        }}));
        return;
    }
    let token_input_lamports_f64 = position_size_sol * LAMPORTS_PER_SOL as f64;
    if !token_input_lamports_f64.is_finite()
        || token_input_lamports_f64 < 1.0
        || token_input_lamports_f64 > u64::MAX as f64
    {
        let _ = events.send(json!({"type":"paper_buy_failed","data":{
            "mint_address":mint,"reason":"Configured paper position size is outside the supported lamport range.","paper_only":true
        }}));
        return;
    }
    let costs = BuyCostBreakdown {
        token_input_lamports: token_input_lamports_f64.round() as u64,
        network_fee_lamports: 0,
        priority_fee_lamports: 0,
        swap_fee_lamports: 0,
        rent_reserve_lamports: 0,
    };

    let _ = events.send(json!({
        "type": "paper_buy_started",
        "data": {
            "mint_address": mint,
            "paper_only": true,
            "execution_mode": "simulated",
            "stage": "reserving_paper_balance"
        }
    }));

    let mut wallet_guard = wallet.write().await;
    let mut positions_guard = portfolio_positions.write().await;
    if positions_guard.iter().any(|position| {
        position
            .get("mint_address")
            .and_then(serde_json::Value::as_str)
            == Some(mint)
            && position.get("status").and_then(serde_json::Value::as_str) == Some("open")
    }) {
        return;
    }
    let mut next_wallet = wallet_guard.clone();
    let ledger = match next_wallet.reserve_buy_for_token(mint, &costs) {
        Ok(ledger) => ledger,
        Err(error) => {
            let _ = events.send(json!({"type":"paper_buy_failed","data":{
                "mint_address":mint,"reason":error.to_string(),"paper_only":true
            }}));
            return;
        }
    };
    let slippage_pct = market_assessment
        .estimated_slippage_pct
        .unwrap_or(0.0)
        .max(0.0);
    let fill_price_sol = price_sol * (1.0 + slippage_pct / 100.0);
    let fill_price_usd = fill_price_sol * sol_quote.price_usd;
    if !fill_price_sol.is_finite() || fill_price_sol <= 0.0 || !fill_price_usd.is_finite() {
        let _ = events.send(json!({"type":"paper_buy_failed","data":{
            "mint_address":mint,"reason":"Estimated paper fill price is invalid.","paper_only":true
        }}));
        return;
    }
    let position_id = uuid::Uuid::new_v4().to_string();
    let token_quantity = position_size_sol / fill_price_sol;
    let current_value_sol = token_quantity * price_sol;
    let current_value_usd = current_value_sol * sol_quote.price_usd;
    let unrealized_pnl_sol = current_value_sol - position_size_sol;
    let unrealized_pnl_usd = current_value_usd - config.position_size_usd;
    let unrealized_pnl_pct = if config.position_size_usd > 0.0 {
        unrealized_pnl_usd / config.position_size_usd * 100.0
    } else {
        0.0
    };
    let opened_at = chrono::Utc::now().to_rfc3339();
    let mut position = json!({
        "position_id":position_id.clone(),
        "id":position_id.clone(),
        "mint_address":mint,
        "status":"open",
        "venue":market_data.get("venue").cloned().unwrap_or(json!("unknown")),
        "source":"automatic_paper_entry",
        "entry_price_sol":fill_price_sol,
        "average_entry_price_sol":fill_price_sol,
        "entry_price_usd":fill_price_usd,
        "entry_sol_price_usd":sol_quote.price_usd,
        "token_quantity":token_quantity,
        "quantity":token_quantity,
        "cost_basis_sol":position_size_sol,
        "invested_sol":position_size_sol,
        "initial_investment_usd":config.position_size_usd,
        "cost_basis_usd":config.position_size_usd,
        "current_price_sol":price_sol,
        "current_price_usd":price_usd,
        "current_value_sol":current_value_sol,
        "current_position_value_usd":current_value_usd,
        "unrealized_pnl_sol":unrealized_pnl_sol,
        "unrealized_pnl_usd":unrealized_pnl_usd,
        "pnl_usd":unrealized_pnl_usd,
        "pnl_pct":unrealized_pnl_pct,
        "unrealized_pnl_pct":unrealized_pnl_pct,
        "opened_at":opened_at.clone(),
        "marked_at":opened_at.clone(),
        "paper_only":true,
        "entry_evidence":{
            "decision":"evaluate_paper_entry",
            "confirmed_trade_events":market_assessment.confirmed_trade_events,
            "distinct_buyers_5m":market_assessment.distinct_buyers_5m,
            "buy_volume_5m_usd":market_assessment.buy_volume_5m_usd,
            "estimated_slippage_pct":market_assessment.estimated_slippage_pct,
            "security_decision":security_decision
        }
    });
    let Some(risk_policy) = capture_position_risk(&config, fill_price_sol, fill_price_usd) else {
        let _ = events.send(json!({"type":"paper_buy_failed","data":{
            "mint_address":mint,"reason":"Could not capture a valid per-position risk policy.","paper_only":true
        }}));
        return;
    };
    if let Some(object) = position.as_object_mut() {
        object.insert(
            "risk_policy".into(),
            serde_json::to_value(&risk_policy).unwrap_or(serde_json::Value::Null),
        );
        object.insert("stop_loss_pct".into(), json!(risk_policy.stop_loss_pct));
        object.insert("take_profit_pct".into(), json!(risk_policy.take_profit_pct));
        object.insert(
            "stop_loss_price_sol".into(),
            json!(risk_policy.stop_loss_price_sol),
        );
        object.insert(
            "take_profit_price_sol".into(),
            json!(risk_policy.take_profit_price_sol),
        );
        object.insert(
            "stop_loss_price_usd".into(),
            json!(risk_policy.stop_loss_price_usd),
        );
        object.insert(
            "take_profit_price_usd".into(),
            json!(risk_policy.take_profit_price_usd),
        );
    }

    let mut next_positions = positions_guard.clone();
    next_positions.push(position.clone());
    if let Err(error) = nodex_persistence::commit_paper_buy(
        database,
        &next_wallet,
        &ledger,
        &position_id,
        mint,
        &position,
    )
    .await
    {
        let _ = events.send(json!({"type":"paper_buy_failed","data":{
            "mint_address":mint,"reason":format!("Atomic paper buy persistence failed: {error}"),"paper_only":true
        }}));
        return;
    }

    *wallet_guard = next_wallet.clone();
    *positions_guard = next_positions.clone();
    drop(positions_guard);
    drop(wallet_guard);

    let portfolio = calculate_portfolio_snapshot(&next_wallet, &sol_quote, &next_positions);
    let mut profile_snapshot = profile.write().await;
    if let Some(object) = profile_snapshot.as_object_mut() {
        object.insert("paper_wallet".into(), json!(next_wallet.clone()));
        object.insert("portfolio".into(), portfolio.clone());
    }
    let profile_value = profile_snapshot.clone();
    drop(profile_snapshot);

    let _ = events.send(json!({
        "type":"paper_buy_executed",
        "data":{
            "mint_address":mint,
            "position":position.clone(),
            "ledger":ledger,
            "wallet":next_wallet.clone(),
            "paper_only":true,
            "execution_mode":"simulated"
        }
    }));
    let _ = events.send(json!({"type":"position_updated","data":position}));
    let _ = events.send(json!({"type":"profile_updated","data":profile_value}));
    let _ = events.send(json!({"type":"portfolio_summary_updated","data":portfolio}));
}

#[cfg(test)]
mod security_approval_cache_tests {
    use super::*;

    #[test]
    fn fresh_cached_approval_is_reused_without_an_external_rescan() {
        let now = chrono::Utc::now();
        let security = json!({
            "decision":"APPROVED",
            "scan_cache":{
                "approval_expires_at":(now + chrono::Duration::seconds(60)).to_rfc3339()
            }
        });
        assert!(security_approval_cache_is_fresh(Some(&security), now));
    }

    #[test]
    fn expired_missing_or_malformed_approval_cache_fails_closed() {
        let now = chrono::Utc::now();
        let expired = json!({"scan_cache":{"approval_expires_at":
            (now - chrono::Duration::seconds(1)).to_rfc3339()
        }});
        let missing = json!({"scan_cache":{}});
        let malformed = json!({"scan_cache":{"approval_expires_at":"invalid"}});
        assert!(!security_approval_cache_is_fresh(Some(&expired), now));
        assert!(!security_approval_cache_is_fresh(Some(&missing), now));
        assert!(!security_approval_cache_is_fresh(Some(&malformed), now));
        assert!(!security_approval_cache_is_fresh(None, now));
    }

    #[test]
    fn approval_cache_is_scoped_to_the_security_record_for_each_mint() {
        let now = chrono::Utc::now();
        let token_a = json!({"mint_address":"mint-a","security":{
            "scan_cache":{"approval_expires_at":
                (now + chrono::Duration::seconds(60)).to_rfc3339()}
        }});
        let token_b = json!({"mint_address":"mint-b","security":{
            "scan_cache":{"approval_expires_at":
                (now - chrono::Duration::seconds(60)).to_rfc3339()}
        }});
        assert!(security_approval_cache_is_fresh(
            token_a.get("security"),
            now
        ));
        assert!(!security_approval_cache_is_fresh(
            token_b.get("security"),
            now
        ));
    }
}
