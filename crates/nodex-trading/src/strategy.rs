use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketSnapshot {
    pub price_change_pct_1m: f64,
    pub price_change_pct_5m: f64,
    pub estimated_round_trip_cost_pct: f64,
    pub expected_exit_slippage_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    pub minimum_score: u8,
    pub minimum_1m_momentum_pct: f64,
    pub minimum_5m_momentum_pct: f64,
    pub minimum_net_edge_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Decision { Reject, Wait, EnterPaper }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyDecision {
    pub decision: Decision,
    pub expected_net_edge_pct: f64,
    pub reason: String,
}

/// A risk score alone never triggers an entry. Costs and exit slippage reduce edge.
pub fn evaluate(score: u8, hard_rejected: bool, market: &MarketSnapshot, config: &StrategyConfig) -> StrategyDecision {
    let edge = market.price_change_pct_5m.max(0.0)
        - market.estimated_round_trip_cost_pct
        - market.expected_exit_slippage_pct;
    let (decision, reason) = if hard_rejected {
        (Decision::Reject, "Hard verification gate failed")
    } else if score < config.minimum_score {
        (Decision::Reject, "Risk score below configured minimum")
    } else if market.price_change_pct_1m < config.minimum_1m_momentum_pct
        || market.price_change_pct_5m < config.minimum_5m_momentum_pct {
        (Decision::Wait, "Waiting for positive momentum confirmation")
    } else if edge < config.minimum_net_edge_pct {
        (Decision::Wait, "Expected edge does not cover costs and exit slippage")
    } else {
        (Decision::EnterPaper, "Risk gates, momentum and estimated net edge passed")
    };
    StrategyDecision { decision, expected_net_edge_pct: edge, reason: reason.into() }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn score_alone_does_not_trigger_entry() {
        let market = MarketSnapshot { price_change_pct_1m: -1.0, price_change_pct_5m: 10.0, estimated_round_trip_cost_pct: 1.0, expected_exit_slippage_pct: 1.0 };
        let config = StrategyConfig { minimum_score: 70, minimum_1m_momentum_pct: 0.2, minimum_5m_momentum_pct: 1.0, minimum_net_edge_pct: 1.0 };
        assert_eq!(evaluate(100, false, &market, &config).decision, Decision::Wait);
    }
}
