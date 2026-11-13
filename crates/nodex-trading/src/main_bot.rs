use serde::{Deserialize, Serialize};

/// Main Trading Bot entry policy. Values are supplied from the persisted engine settings;
/// this module contains no provider calls, wallet signing, or transaction submission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingBotConfig {
    pub minimum_safety_score: f64,
    pub minimum_liquidity_usd: f64,
    pub maximum_entry_price_usd: f64,
    pub position_size_sol: f64,
    pub position_size_usd: f64,
    pub stop_loss_pct: f64,
    pub take_profit_pct: f64,
    pub trailing_stop_enabled: bool,
    pub trailing_stop_callback_pct: f64,
    pub maximum_slippage_pct: f64,
    pub minimum_5m_buy_volume_usd: f64,
    pub maximum_top10_concentration_pct: f64,
    pub maximum_open_positions: usize,
    pub minimum_distinct_buyers_high_score: usize,
    pub minimum_distinct_buyers_other: usize,
    pub high_creator_score_threshold: f64,
    pub low_score_observation_seconds: u64,
    pub blacklist_known_rug_creators: bool,
    pub whitelist_only_mode: bool,
}

impl Default for TradingBotConfig {
    fn default() -> Self {
        Self {
            minimum_safety_score: 80.0,
            minimum_liquidity_usd: 3_000.0,
            maximum_entry_price_usd: 0.0,
            position_size_sol: 0.01,
            position_size_usd: 0.0,
            stop_loss_pct: 15.0,
            take_profit_pct: 100.0,
            trailing_stop_enabled: false,
            trailing_stop_callback_pct: 10.0,
            maximum_slippage_pct: 10.0,
            minimum_5m_buy_volume_usd: 1_000.0,
            maximum_top10_concentration_pct: 20.0,
            maximum_open_positions: 3,
            minimum_distinct_buyers_high_score: 2,
            minimum_distinct_buyers_other: 3,
            high_creator_score_threshold: 80.0,
            low_score_observation_seconds: 30,
            blacklist_known_rug_creators: true,
            whitelist_only_mode: false,
        }
    }
}

/// Immutable risk settings copied onto a position at the moment it opens.
/// Later dashboard setting changes affect new positions, not existing positions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PositionRiskPolicy {
    pub entry_price_sol: f64,
    pub entry_price_usd: f64,
    pub stop_loss_pct: f64,
    pub take_profit_pct: f64,
    pub stop_loss_price_sol: f64,
    pub take_profit_price_sol: f64,
    pub stop_loss_price_usd: f64,
    pub take_profit_price_usd: f64,
    #[serde(default)]
    pub trailing_stop_eligible: bool,
    #[serde(default)]
    pub trailing_stop_eligibility_reason: String,
    #[serde(default)]
    pub trailing_stop_enabled: bool,
    #[serde(default = "default_trailing_activation_profit_pct")]
    pub trailing_activation_profit_pct: f64,
    #[serde(default = "default_trailing_stop_callback_pct")]
    pub trailing_stop_callback_pct: f64,
    #[serde(default)]
    pub trailing_stop_active: bool,
    #[serde(default = "default_maximum_profit_pct")]
    pub maximum_profit_pct: f64,
    #[serde(default)]
    pub maximum_profit_price_sol: f64,
    #[serde(default)]
    pub maximum_profit_price_usd: f64,
    #[serde(default)]
    pub trailing_stop_price_sol: f64,
    #[serde(default)]
    pub highest_price_sol: f64,
    pub captured_at: String,
}

fn default_trailing_activation_profit_pct() -> f64 {
    50.0
}
fn default_trailing_stop_callback_pct() -> f64 {
    10.0
}
fn default_maximum_profit_pct() -> f64 {
    0.0
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PositionExitReason {
    StopLoss,
    TakeProfit,
    TrailingStop,
    MaximumProfit,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PositionExitTrigger {
    pub reason: PositionExitReason,
    pub trigger_price_sol: f64,
    pub entry_price_sol: f64,
    pub pnl_pct: f64,
}

/// Freeze the configured exit thresholds at entry. All prices are derived from
/// that position's own fill price, so separate tokens/lots never share triggers.
pub fn capture_position_risk(
    config: &TradingBotConfig,
    entry_price_sol: f64,
    entry_price_usd: f64,
) -> Option<PositionRiskPolicy> {
    if !entry_price_sol.is_finite()
        || entry_price_sol <= 0.0
        || !entry_price_usd.is_finite()
        || entry_price_usd <= 0.0
        || !config.stop_loss_pct.is_finite()
        || !(0.0..=100.0).contains(&config.stop_loss_pct)
        || !config.take_profit_pct.is_finite()
        || !(0.0..=1_000.0).contains(&config.take_profit_pct)
        || !config.trailing_stop_callback_pct.is_finite()
        || !(0.0..=100.0).contains(&config.trailing_stop_callback_pct)
    {
        return None;
    }

    Some(PositionRiskPolicy {
        entry_price_sol,
        entry_price_usd,
        stop_loss_pct: config.stop_loss_pct,
        take_profit_pct: config.take_profit_pct,
        stop_loss_price_sol: entry_price_sol * (1.0 - config.stop_loss_pct / 100.0),
        take_profit_price_sol: entry_price_sol * (1.0 + config.take_profit_pct / 100.0),
        stop_loss_price_usd: entry_price_usd * (1.0 - config.stop_loss_pct / 100.0),
        take_profit_price_usd: entry_price_usd * (1.0 + config.take_profit_pct / 100.0),
        // Trailing is always opt-in per position; the frontend can enable it only
        // after the backend has assessed the token's behaviour as suitable.
        trailing_stop_eligible: false,
        trailing_stop_eligibility_reason:
            "Waiting for confirmed trade-flow and price-behaviour evidence.".into(),
        trailing_stop_enabled: false,
        trailing_activation_profit_pct: default_trailing_activation_profit_pct(),
        trailing_stop_callback_pct: config.trailing_stop_callback_pct,
        trailing_stop_active: false,
        maximum_profit_pct: default_maximum_profit_pct(),
        maximum_profit_price_sol: 0.0,
        maximum_profit_price_usd: 0.0,
        trailing_stop_price_sol: 0.0,
        highest_price_sol: entry_price_sol,
        captured_at: chrono::Utc::now().to_rfc3339(),
    })
}

/// Stamp an opening position with an immutable risk-policy snapshot. Call this
/// once after the paper fill price and quantity are known, before persistence.
pub fn attach_position_risk_policy(
    position: &mut serde_json::Value,
    config: &TradingBotConfig,
    entry_price_sol: f64,
    entry_price_usd: f64,
) -> bool {
    let Some(policy) = capture_position_risk(config, entry_price_sol, entry_price_usd) else {
        return false;
    };
    let Ok(value) = serde_json::to_value(&policy) else {
        return false;
    };
    let Some(object) = position.as_object_mut() else {
        return false;
    };
    object.insert("risk_policy".into(), value);
    object.insert(
        "stop_loss_price_sol".into(),
        serde_json::json!(policy.stop_loss_price_sol),
    );
    object.insert(
        "take_profit_price_sol".into(),
        serde_json::json!(policy.take_profit_price_sol),
    );
    object.insert(
        "stop_loss_price_usd".into(),
        serde_json::json!(policy.stop_loss_price_usd),
    );
    object.insert(
        "take_profit_price_usd".into(),
        serde_json::json!(policy.take_profit_price_usd),
    );
    object.insert(
        "stop_loss_pct".into(),
        serde_json::json!(policy.stop_loss_pct),
    );
    object.insert(
        "take_profit_pct".into(),
        serde_json::json!(policy.take_profit_pct),
    );
    true
}

/// Update one position's high-water mark and return only its own exit trigger.
pub fn evaluate_position_exit(
    policy: &mut PositionRiskPolicy,
    current_price_sol: f64,
) -> Option<PositionExitTrigger> {
    if !current_price_sol.is_finite() || current_price_sol <= 0.0 {
        return None;
    }
    policy.highest_price_sol = policy.highest_price_sol.max(current_price_sol);
    let pnl_pct = (current_price_sol / policy.entry_price_sol - 1.0) * 100.0;
    // Trailing mode is a complete exit-policy switch, not an extra rule layered
    // over the fixed stop-loss/take-profit. It starts protecting from the entry
    // price immediately, then follows the highest observed price.
    let trailing_mode = policy.trailing_stop_enabled;
    if trailing_mode {
        policy.trailing_stop_active = true;
        policy.highest_price_sol = policy.highest_price_sol.max(policy.entry_price_sol);
        policy.trailing_stop_price_sol =
            policy.highest_price_sol * (1.0 - policy.trailing_stop_callback_pct / 100.0);
    } else {
        policy.trailing_stop_active = false;
        policy.trailing_stop_price_sol = 0.0;
    }

    // The optional maximum-profit ceiling belongs only to eligible, enabled
    // trailing mode. A zero target disables it. Fixed SL/TP are ignored while
    // trailing mode is active and remain unchanged in normal mode.
    let trigger =
        if trailing_mode && policy.maximum_profit_pct > 0.0 && pnl_pct >= policy.maximum_profit_pct
        {
            Some(PositionExitReason::MaximumProfit)
        } else if trailing_mode && current_price_sol <= policy.trailing_stop_price_sol {
            Some(PositionExitReason::TrailingStop)
        } else if !trailing_mode
            && policy.stop_loss_pct > 0.0
            && current_price_sol <= policy.stop_loss_price_sol
        {
            Some(PositionExitReason::StopLoss)
        } else if !trailing_mode
            && policy.take_profit_pct > 0.0
            && current_price_sol >= policy.take_profit_price_sol
        {
            Some(PositionExitReason::TakeProfit)
        } else {
            None
        }?;

    Some(PositionExitTrigger {
        reason: trigger,
        trigger_price_sol: current_price_sol,
        entry_price_sol: policy.entry_price_sol,
        pnl_pct,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatorReputation {
    pub score: Option<f64>,
    pub known_rug_creator: Option<bool>,
    pub preferred_owner: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityAssessment {
    pub mint_authority_disabled: bool,
    pub freeze_authority_disabled: bool,
    pub rugcheck_available: bool,
    pub rugcheck_acceptable: bool,
    pub safety_score: Option<f64>,
    pub top10_concentration_pct: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveMarketActivity {
    pub price_usd: Option<f64>,
    pub liquidity_usd: Option<f64>,
    pub price_change_1m_pct: Option<f64>,
    pub price_change_5m_pct: Option<f64>,
    pub buy_volume_5m_usd: f64,
    pub distinct_buyers_5m: usize,
    pub buy_count_5m: u64,
    pub sell_count_5m: u64,
    pub estimated_slippage_pct: Option<f64>,
    pub seconds_since_launch: u64,
    /// Set only from confirmed, decoded on-chain trade events; never from price ticks.
    pub confirmed_trade_events: u64,
    pub buy_pressure_pct: Option<f64>,
    pub distinct_buyers_30s: usize,
    pub buy_count_30s: u64,
    pub sell_count_30s: u64,
    pub buy_pressure_30s_pct: Option<f64>,
    pub market_cap_usd: Option<f64>,
    /// Number of distinct holder accounts actually resolved in the current sample,
    /// not a claim about the token's total holder count.
    pub observed_holder_count: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradingBotDecision {
    Reject,
    Wait,
    EvaluatePaperEntry,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingBotEvaluation {
    pub decision: TradingBotDecision,
    pub entry_speed: String,
    pub creator_score: Option<f64>,
    pub minimum_distinct_buyers: usize,
    pub reasons: Vec<String>,
}

/// Evaluate one token against security, market, portfolio and creator-history gates.
/// A strong creator score only changes the evidence/waiting threshold; it never bypasses
/// verification, liquidity, concentration, slippage, volume or portfolio limits.
pub fn evaluate_entry(
    config: &TradingBotConfig,
    creator: &CreatorReputation,
    security: &SecurityAssessment,
    market: &LiveMarketActivity,
    open_positions: usize,
) -> TradingBotEvaluation {
    let high_score = creator
        .score
        .is_some_and(|score| score.is_finite() && score >= config.high_creator_score_threshold);
    let top10_sample_missing = security.top10_concentration_pct.is_none();
    let minimum_buyers = if high_score {
        config.minimum_distinct_buyers_high_score
    } else {
        config.minimum_distinct_buyers_other
    }
    .max(if top10_sample_missing {
        config.minimum_distinct_buyers_other.saturating_add(2)
    } else {
        0
    });
    let entry_speed = if high_score { "fast" } else { "confirmation" };
    const MIN_OBSERVATION_SECONDS: u64 = 90;
    const MAX_EARLY_ENTRY_AGE_SECONDS: u64 = 300;
    const MAX_EARLY_ENTRY_MARKET_CAP_USD: f64 = 150_000.0;
    const MIN_30S_BUY_PRESSURE_PCT: f64 = 65.0;
    let mut reasons = Vec::new();
    let mut rejected = false;
    let mut waiting = false;

    macro_rules! reject {
        ($reason:expr) => {{
            rejected = true;
            reasons.push($reason.to_owned());
        }};
    }
    macro_rules! wait {
        ($reason:expr) => {{
            waiting = true;
            reasons.push($reason.to_owned());
        }};
    }

    if !security.mint_authority_disabled || !security.freeze_authority_disabled {
        reject!("Mint and freeze authorities must both be disabled.");
    }
    if !security.rugcheck_available {
        reject!("RugCheck is unavailable; token cannot be paper-entry eligible.");
    } else if !security.rugcheck_acceptable {
        reject!("RugCheck did not return an acceptable risk assessment.");
    }
    match security.safety_score {
        Some(score) if score.is_finite() && score >= config.minimum_safety_score => {}
        Some(_) => reject!("Safety score is below the configured minimum."),
        None => reject!("Safety score is unavailable."),
    }
    if config.blacklist_known_rug_creators {
        match creator.known_rug_creator {
            Some(true) => reject!("Creator is on the known-rug blacklist."),
            Some(false) => {}
            None => reasons.push(
                "Creator rug history is inconclusive; using the slower confirmation path.".into(),
            ),
        }
    }
    if config.whitelist_only_mode && !creator.preferred_owner {
        reject!("Creator is not on the preferred-owner list; token remains monitor-only.");
    }
    if open_positions >= config.maximum_open_positions {
        reject!("Maximum number of open paper positions has been reached.");
    }

    match security.top10_concentration_pct {
        Some(value) if value.is_finite() && value <= config.maximum_top10_concentration_pct => {}
        Some(_) => reject!("Top-10 holder concentration exceeds the configured maximum, including the partial-sample safety buffer."),
        None => wait!("Waiting for a holder-distribution sample with at least ten resolved accounts; holder concentration cannot be assumed safe."),
    }
    match market.observed_holder_count {
        Some(count) if count >= 10 => {}
        Some(_) => wait!("Holder-distribution sample is too small to assess concentration safely."),
        None => wait!("Waiting for verified holder-distribution data."),
    }
    match market.market_cap_usd {
        Some(value) if value.is_finite() && value > 0.0 && value <= MAX_EARLY_ENTRY_MARKET_CAP_USD => {}
        Some(value) if value.is_finite() && value > MAX_EARLY_ENTRY_MARKET_CAP_USD => wait!("Market cap is already above the early-entry ceiling; avoiding a late entry."),
        _ => wait!("Waiting for a reliable market-cap estimate from token supply and live price."),
    }
    if market.seconds_since_launch < MIN_OBSERVATION_SECONDS {
        wait!("Collecting at least 90 seconds of confirmed market behaviour before entry.");
    }
    if market.seconds_since_launch > MAX_EARLY_ENTRY_AGE_SECONDS {
        reject!("The five-minute early-entry observation window has expired; skipping this token rather than chasing it late.");
    }
    match market.liquidity_usd {
        Some(value) if value.is_finite() && value >= config.minimum_liquidity_usd => {}
        Some(_) => wait!("Waiting for minimum liquidity."),
        None => wait!("Waiting for a verified liquidity value."),
    }
    match market.price_usd {
        Some(value)
            if value.is_finite()
                && value > 0.0
                && (config.maximum_entry_price_usd <= 0.0
                    || value <= config.maximum_entry_price_usd) => {}
        Some(_) => reject!("Entry price exceeds the configured maximum or is invalid."),
        None => wait!("Waiting for a valid live market price."),
    }
    match market.estimated_slippage_pct {
        Some(value) if value.is_finite() && value <= config.maximum_slippage_pct => {}
        Some(_) => reject!("Estimated entry slippage exceeds the configured maximum."),
        None => wait!("Waiting for an entry-slippage estimate."),
    }

    // Avoid buying the top of a fast candle. A modest negative 1m move while the
    // 5m trend remains positive is treated as a pullback, not a reason to chase.
    if market
        .price_change_1m_pct
        .is_some_and(|change| change.is_finite() && change > 12.0)
    {
        wait!("One-minute price expansion is too sharp; waiting for a pullback.");
    }
    if market
        .price_change_5m_pct
        .is_some_and(|change| change.is_finite() && change > 35.0)
    {
        wait!("Five-minute price expansion is too extended; avoiding a late entry.");
    }
    if market
        .price_change_1m_pct
        .is_some_and(|change| change.is_finite() && change < -20.0)
        || market
            .price_change_5m_pct
            .is_some_and(|change| change.is_finite() && change < -40.0)
    {
        wait!("Recent sell pressure is too sharp; waiting for price stabilization.");
    }
    if market.buy_volume_5m_usd < config.minimum_5m_buy_volume_usd {
        wait!("Five-minute confirmed buy volume is below the configured minimum.");
    }
    if market.distinct_buyers_5m < minimum_buyers {
        wait!("Not enough distinct buying wallets have participated yet.");
    }
    let minimum_recent_buyers = if high_score { 2 } else { 3 };
    if market.distinct_buyers_30s < minimum_recent_buyers {
        wait!("Not enough distinct wallets have bought in the last 30 seconds to confirm fresh demand.");
    }
    if market.buy_count_30s == 0 || market.sell_count_30s >= market.buy_count_30s {
        wait!("Recent confirmed sells are matching or exceeding buys; waiting for buyers to regain control.");
    }
    match market.buy_pressure_30s_pct {
        Some(value) if value.is_finite() && value >= MIN_30S_BUY_PRESSURE_PCT => {}
        Some(_) => wait!("Thirty-second buy pressure is below 65%; avoiding weak or sell-dominated flow."),
        None => wait!("Waiting for enough confirmed 30-second buy/sell volume to calculate buy pressure."),
    }
    if market.confirmed_trade_events == 0 || market.buy_count_5m == 0 {
        wait!("Waiting for confirmed on-chain buy activity; price movement alone is not a trade signal.");
    }
    if !high_score && market.seconds_since_launch < config.low_score_observation_seconds {
        wait!("Low-scoring or unknown creator: waiting for the additional market-confirmation period.");
    }

    let decision = if rejected {
        TradingBotDecision::Reject
    } else if waiting {
        TradingBotDecision::Wait
    } else {
        reasons.push(
            "All configured entry gates passed; paper-entry execution may evaluate this signal."
                .into(),
        );
        TradingBotDecision::EvaluatePaperEntry
    };

    TradingBotEvaluation {
        decision,
        entry_speed: entry_speed.into(),
        creator_score: creator.score,
        minimum_distinct_buyers: minimum_buyers,
        reasons,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn security() -> SecurityAssessment {
        SecurityAssessment {
            mint_authority_disabled: true,
            freeze_authority_disabled: true,
            rugcheck_available: true,
            rugcheck_acceptable: true,
            safety_score: Some(92.0),
            top10_concentration_pct: Some(12.0),
        }
    }

    fn market() -> LiveMarketActivity {
        LiveMarketActivity {
            price_usd: Some(0.0001),
            liquidity_usd: Some(10_000.0),
            price_change_1m_pct: Some(-4.0),
            price_change_5m_pct: Some(12.0),
            buy_volume_5m_usd: 2_000.0,
            distinct_buyers_5m: 3,
            buy_count_5m: 8,
            sell_count_5m: 1,
            estimated_slippage_pct: Some(2.0),
            seconds_since_launch: 90,
            confirmed_trade_events: 9,
            buy_pressure_pct: Some(80.0),
            distinct_buyers_30s: 3,
            buy_count_30s: 5,
            sell_count_30s: 1,
            buy_pressure_30s_pct: Some(80.0),
            market_cap_usd: Some(50_000.0),
            observed_holder_count: Some(12),
        }
    }

    fn creator(score: Option<f64>) -> CreatorReputation {
        CreatorReputation {
            score,
            known_rug_creator: Some(false),
            preferred_owner: true,
        }
    }

    #[test]
    fn position_policy_is_stored_on_the_position_record() {
        let config = TradingBotConfig::default();
        let mut position = serde_json::json!({"mint_address":"mint-a","status":"open"});
        assert!(attach_position_risk_policy(
            &mut position,
            &config,
            1.0,
            150.0
        ));
        assert_eq!(position["stop_loss_price_sol"], 0.85);
        assert_eq!(position["take_profit_price_sol"], 2.0);
        assert_eq!(position["risk_policy"]["stop_loss_pct"], 15.0);
        assert_eq!(position["risk_policy"]["take_profit_pct"], 100.0);
    }

    #[test]
    fn position_exit_levels_are_frozen_from_the_individual_entry_price() {
        let config = TradingBotConfig::default();
        let first = capture_position_risk(&config, 2.0, 300.0).unwrap();
        let second = capture_position_risk(&config, 0.5, 75.0).unwrap();
        assert_eq!(first.stop_loss_price_sol, 1.7);
        assert_eq!(first.take_profit_price_sol, 4.0);
        assert_eq!(second.stop_loss_price_sol, 0.425);
        assert_eq!(second.take_profit_price_sol, 1.0);
    }

    #[test]
    fn one_position_exit_trigger_does_not_change_another_position_policy() {
        let config = TradingBotConfig::default();
        let mut first = capture_position_risk(&config, 1.0, 150.0).unwrap();
        let mut second = capture_position_risk(&config, 0.25, 37.5).unwrap();
        let trigger = evaluate_position_exit(&mut first, 2.0).unwrap();
        assert_eq!(trigger.reason, PositionExitReason::TakeProfit);
        assert!(evaluate_position_exit(&mut second, 0.30).is_none());
        assert_eq!(second.take_profit_price_sol, 0.5);
    }

    #[test]
    fn trailing_stop_uses_each_positions_own_high_water_mark() {
        let config = TradingBotConfig {
            trailing_stop_enabled: true,
            trailing_stop_callback_pct: 10.0,
            ..TradingBotConfig::default()
        };
        let mut policy = capture_position_risk(&config, 1.0, 100.0).unwrap();
        policy.trailing_stop_eligible = true;
        policy.trailing_stop_enabled = true;
        assert!(evaluate_position_exit(&mut policy, 1.5).is_none());
        let trigger = evaluate_position_exit(&mut policy, 1.34).unwrap();
        assert_eq!(trigger.reason, PositionExitReason::TrailingStop);
    }

    #[test]
    fn high_creator_score_uses_fast_two_wallet_threshold() {
        let result = evaluate_entry(
            &TradingBotConfig::default(),
            &creator(Some(91.0)),
            &security(),
            &market(),
            0,
        );
        assert_eq!(result.decision, TradingBotDecision::EvaluatePaperEntry);
        assert_eq!(result.entry_speed, "fast");
        assert_eq!(result.minimum_distinct_buyers, 2);
    }

    #[test]
    fn strategy_waits_when_recent_sell_flow_matches_or_exceeds_buys() {
        let mut live = market();
        live.sell_count_30s = live.buy_count_30s;
        let result = evaluate_entry(
            &TradingBotConfig::default(),
            &creator(Some(91.0)),
            &security(),
            &live,
            0,
        );
        assert_eq!(result.decision, TradingBotDecision::Wait);
        assert!(result.reasons.iter().any(|reason| reason.contains("sells")));
    }

    #[test]
    fn strategy_waits_when_market_cap_is_above_early_entry_ceiling() {
        let mut live = market();
        live.market_cap_usd = Some(200_000.0);
        let result = evaluate_entry(
            &TradingBotConfig::default(),
            &creator(Some(91.0)),
            &security(),
            &live,
            0,
        );
        assert_eq!(result.decision, TradingBotDecision::Wait);
        assert!(result.reasons.iter().any(|reason| reason.contains("Market cap")));
    }

    #[test]
    fn strategy_waits_when_holder_sample_is_unavailable() {
        let mut live = market();
        live.observed_holder_count = None;
        let result = evaluate_entry(
            &TradingBotConfig::default(),
            &creator(Some(91.0)),
            &security(),
            &live,
            0,
        );
        assert_eq!(result.decision, TradingBotDecision::Wait);
        assert!(result.reasons.iter().any(|reason| reason.contains("holder-distribution")));
    }

    #[test]
    fn strategy_rejects_tokens_outside_the_five_minute_early_entry_window() {
        let mut live = market();
        live.seconds_since_launch = 301;
        let result = evaluate_entry(
            &TradingBotConfig::default(),
            &creator(Some(91.0)),
            &security(),
            &live,
            0,
        );
        assert_eq!(result.decision, TradingBotDecision::Reject);
        assert!(result.reasons.iter().any(|reason| reason.contains("five-minute")));
    }

    #[test]
    fn strategy_waits_for_pullback_after_a_sharp_one_minute_pump() {
        let mut live = market();
        live.price_change_1m_pct = Some(16.0);
        let result = evaluate_entry(
            &TradingBotConfig::default(),
            &creator(Some(91.0)),
            &security(),
            &live,
            0,
        );
        assert_eq!(result.decision, TradingBotDecision::Wait);
        assert!(result
            .reasons
            .iter()
            .any(|reason| reason.contains("pullback")));
    }

    #[test]
    fn strategy_waits_for_stabilization_after_a_sharp_selloff() {
        let mut live = market();
        live.price_change_1m_pct = Some(-24.0);
        let result = evaluate_entry(
            &TradingBotConfig::default(),
            &creator(Some(91.0)),
            &security(),
            &live,
            0,
        );
        assert_eq!(result.decision, TradingBotDecision::Wait);
        assert!(result
            .reasons
            .iter()
            .any(|reason| reason.contains("stabilization")));
    }

    #[test]
    fn unknown_creator_waits_for_three_wallets_and_confirmation_time() {
        let mut live = market();
        live.distinct_buyers_5m = 2;
        live.seconds_since_launch = 10;
        let result = evaluate_entry(
            &TradingBotConfig::default(),
            &creator(None),
            &security(),
            &live,
            0,
        );
        assert_eq!(result.decision, TradingBotDecision::Wait);
        assert_eq!(result.minimum_distinct_buyers, 3);
    }

    #[test]
    fn missing_holder_sample_requires_two_additional_buyers() {
        let mut security_report = security();
        security_report.top10_concentration_pct = None;
        let unknown_creator = CreatorReputation {
            score: None,
            known_rug_creator: None,
            preferred_owner: false,
        };
        let mut live = market();
        live.distinct_buyers_5m = 4;
        let waiting = evaluate_entry(
            &TradingBotConfig::default(),
            &unknown_creator,
            &security_report,
            &live,
            0,
        );
        assert_eq!(waiting.decision, TradingBotDecision::Wait);
        assert_eq!(waiting.minimum_distinct_buyers, 5);

        live.distinct_buyers_5m = 5;
        let eligible = evaluate_entry(
            &TradingBotConfig::default(),
            &unknown_creator,
            &security_report,
            &live,
            0,
        );
        assert_eq!(eligible.decision, TradingBotDecision::Wait);
        assert!(eligible
            .reasons
            .iter()
            .any(|reason| reason.contains("holder-distribution")));
    }

    #[test]
    fn unknown_rug_history_can_pass_only_after_full_market_confirmation() {
        let unknown_creator = CreatorReputation {
            score: None,
            known_rug_creator: None,
            preferred_owner: false,
        };
        let result = evaluate_entry(
            &TradingBotConfig::default(),
            &unknown_creator,
            &security(),
            &market(),
            0,
        );
        assert_eq!(result.decision, TradingBotDecision::EvaluatePaperEntry);
        assert_eq!(result.entry_speed, "confirmation");
        assert!(result
            .reasons
            .iter()
            .any(|reason| reason.contains("inconclusive")));
    }

    #[test]
    fn explicitly_known_rug_creator_is_still_rejected() {
        let known_rug_creator = CreatorReputation {
            score: Some(95.0),
            known_rug_creator: Some(true),
            preferred_owner: true,
        };
        let result = evaluate_entry(
            &TradingBotConfig::default(),
            &known_rug_creator,
            &security(),
            &market(),
            0,
        );
        assert_eq!(result.decision, TradingBotDecision::Reject);
    }

    #[test]
    fn creator_score_never_bypasses_security() {
        let mut assessment = security();
        assessment.rugcheck_acceptable = false;
        let result = evaluate_entry(
            &TradingBotConfig::default(),
            &creator(Some(99.0)),
            &assessment,
            &market(),
            0,
        );
        assert_eq!(result.decision, TradingBotDecision::Reject);
    }

    #[test]
    fn price_ticks_without_confirmed_trades_never_trigger_entry() {
        let mut live = market();
        live.confirmed_trade_events = 0;
        live.buy_count_5m = 0;
        let result = evaluate_entry(
            &TradingBotConfig::default(),
            &creator(Some(95.0)),
            &security(),
            &live,
            0,
        );
        assert_eq!(result.decision, TradingBotDecision::Wait);
    }

    #[test]
    fn unavailable_rugcheck_blocks_entry_but_not_market_monitoring() {
        let mut assessment = security();
        assessment.rugcheck_available = false;
        let result = evaluate_entry(
            &TradingBotConfig::default(),
            &creator(Some(95.0)),
            &assessment,
            &market(),
            0,
        );
        assert_eq!(result.decision, TradingBotDecision::Reject);
    }

    #[test]
    fn open_position_limit_is_enforced() {
        let result = evaluate_entry(
            &TradingBotConfig::default(),
            &creator(Some(95.0)),
            &security(),
            &market(),
            3,
        );
        assert_eq!(result.decision, TradingBotDecision::Reject);
    }
    #[test]
    fn trailing_mode_ignores_fixed_stop_loss_and_take_profit() {
        let config = TradingBotConfig::default();
        let mut policy = capture_position_risk(&config, 1.0, 100.0).unwrap();
        policy.trailing_stop_eligible = true;
        policy.trailing_stop_enabled = true;
        policy.maximum_profit_pct = 0.0;

        // The fixed 15% stop-loss would trigger at 0.84, but the 30%
        // trailing callback places its own initial stop at 0.70.
        policy.trailing_stop_callback_pct = 30.0;
        assert!(evaluate_position_exit(&mut policy, 0.84).is_none());

        // The old fixed +100% take-profit is also ignored in trailing mode.
        assert!(evaluate_position_exit(&mut policy, 2.0).is_none());
        assert!(policy.trailing_stop_active);
        assert_eq!(policy.trailing_stop_price_sol, 1.4);
        let trigger = evaluate_position_exit(&mut policy, 1.39).unwrap();
        assert_eq!(trigger.reason, PositionExitReason::TrailingStop);
    }

    #[test]
    fn normal_mode_ignores_optional_maximum_profit_ceiling() {
        let config = TradingBotConfig::default();
        let mut policy = capture_position_risk(&config, 1.0, 100.0).unwrap();
        policy.maximum_profit_pct = 10.0;
        policy.trailing_stop_eligible = true;
        policy.trailing_stop_enabled = false;

        // Normal take-profit remains the only applicable profit exit.
        assert!(evaluate_position_exit(&mut policy, 1.5).is_none());
        let trigger = evaluate_position_exit(&mut policy, 2.0).unwrap();
        assert_eq!(trigger.reason, PositionExitReason::TakeProfit);
    }

    #[test]
    fn trailing_protection_starts_at_entry_without_waiting_for_profit_activation() {
        let config = TradingBotConfig::default();
        let mut policy = capture_position_risk(&config, 1.0, 100.0).unwrap();
        policy.trailing_stop_eligible = true;
        policy.trailing_stop_enabled = true;
        policy.trailing_activation_profit_pct = 500.0;
        policy.trailing_stop_callback_pct = 20.0;

        let trigger = evaluate_position_exit(&mut policy, 0.79).unwrap();
        assert_eq!(trigger.reason, PositionExitReason::TrailingStop);
        assert!(policy.trailing_stop_active);
        assert!((policy.trailing_stop_price_sol - 0.8).abs() < 1e-9);
    }
}
