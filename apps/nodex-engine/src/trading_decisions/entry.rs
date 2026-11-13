//! Entry strategy evaluation and execution-readiness gates.

use super::super::*;
use nodex_trading::main_bot::{
    evaluate_entry, CreatorReputation, LiveMarketActivity, SecurityAssessment, TradingBotConfig,
    TradingBotDecision, TradingBotEvaluation,
};

/// Evaluate the existing entry strategy after all independent safety gates are collected.
pub(crate) fn evaluate_entry_decision(
    config: &TradingBotConfig,
    creator: &CreatorReputation,
    security: &SecurityAssessment,
    market: &LiveMarketActivity,
    open_positions_count: usize,
    mut gate_reasons: Vec<String>,
    sol_quote_valid: bool,
) -> TradingBotEvaluation {
    if gate_reasons.is_empty() && sol_quote_valid {
        evaluate_entry(config, creator, security, market, open_positions_count)
    } else {
        gate_reasons.push(if !sol_quote_valid {
            "A live SOL/USD quote is required before a paper order can be sized.".to_owned()
        } else {
            "Paper entry gates are disabled.".to_owned()
        });
        TradingBotEvaluation {
            decision: TradingBotDecision::Wait,
            entry_speed: "blocked".into(),
            creator_score: creator.score,
            minimum_distinct_buyers: config.minimum_distinct_buyers_other,
            reasons: gate_reasons,
        }
    }
}

/// A strategy approval is not executable until both quote denominations are valid.
pub(crate) fn enforce_entry_quote_requirements(
    evaluation: &mut TradingBotEvaluation,
    price_sol: Option<f64>,
    price_usd: Option<f64>,
) {
    if evaluation.decision != TradingBotDecision::EvaluatePaperEntry {
        return;
    }
    if !price_sol.is_some_and(|value| value.is_finite() && value > 0.0) {
        evaluation.decision = TradingBotDecision::Wait;
        evaluation.entry_speed = "blocked".into();
        evaluation
            .reasons
            .push("A valid SOL-denominated token price is not available yet.".into());
    } else if !price_usd.is_some_and(|value| value.is_finite() && value > 0.0) {
        evaluation.decision = TradingBotDecision::Wait;
        evaluation.entry_speed = "blocked".into();
        evaluation
            .reasons
            .push("A valid USD-denominated token price is not available yet.".into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_quote_blocks_entry_even_when_strategy_passes() {
        let mut evaluation = TradingBotEvaluation {
            decision: TradingBotDecision::EvaluatePaperEntry,
            entry_speed: "normal".into(),
            creator_score: None,
            minimum_distinct_buyers: 2,
            reasons: Vec::new(),
        };
        enforce_entry_quote_requirements(&mut evaluation, Some(1.0), None);
        assert_eq!(evaluation.decision, TradingBotDecision::Wait);
        assert_eq!(evaluation.entry_speed, "blocked");
        assert!(evaluation
            .reasons
            .iter()
            .any(|reason| reason.contains("USD-denominated")));
    }

    #[test]
    fn invalid_sol_quote_blocks_entry() {
        let mut evaluation = TradingBotEvaluation {
            decision: TradingBotDecision::EvaluatePaperEntry,
            entry_speed: "normal".into(),
            creator_score: None,
            minimum_distinct_buyers: 2,
            reasons: Vec::new(),
        };
        enforce_entry_quote_requirements(&mut evaluation, Some(f64::NAN), Some(100.0));
        assert_eq!(evaluation.decision, TradingBotDecision::Wait);
        assert!(evaluation
            .reasons
            .iter()
            .any(|reason| reason.contains("SOL-denominated")));
    }
}
