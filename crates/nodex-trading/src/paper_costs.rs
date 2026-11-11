use serde::{Deserialize, Serialize};

use crate::paper_wallet::{BuyCostBreakdown, LAMPORTS_PER_SOL};

/// USD valuation of a paper buy using the SOL/USD quote captured at transaction time.
/// Rent is a cash debit but remains a recoverable asset until the token account is closed.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct BuyCostValuation {
    pub sol_usd_at_execution: f64,
    pub token_input_usd: f64,
    pub network_fee_usd: f64,
    pub priority_fee_usd: f64,
    pub swap_fee_usd: f64,
    pub rent_locked_usd: f64,
    pub total_cash_debit_usd: f64,
    pub non_recoverable_fees_usd: f64,
    pub position_cost_basis_usd: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostValuationError {
    InvalidSolUsdQuote,
    LamportArithmeticOverflow,
}

impl std::fmt::Display for CostValuationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidSolUsdQuote => write!(f, "SOL/USD quote must be finite and positive"),
            Self::LamportArithmeticOverflow => write!(f, "paper buy cost total overflowed"),
        }
    }
}

impl std::error::Error for CostValuationError {}

/// Convert every component of a paper buy into USD at the quote captured when
/// the order was prepared. The position cost basis includes the token input and
/// non-refundable fees; recoverable rent is tracked separately and not counted
/// as a permanent trading loss while it remains locked in the token account.
pub fn value_buy_costs(
    costs: &BuyCostBreakdown,
    sol_usd: f64,
) -> Result<BuyCostValuation, CostValuationError> {
    if !sol_usd.is_finite() || sol_usd <= 0.0 {
        return Err(CostValuationError::InvalidSolUsdQuote);
    }
    let total_lamports = costs
        .total_debit_lamports()
        .ok_or(CostValuationError::LamportArithmeticOverflow)?;
    let to_usd = |lamports: u64| lamports as f64 / LAMPORTS_PER_SOL as f64 * sol_usd;
    let token_input_usd = to_usd(costs.token_input_lamports);
    let network_fee_usd = to_usd(costs.network_fee_lamports);
    let priority_fee_usd = to_usd(costs.priority_fee_lamports);
    let swap_fee_usd = to_usd(costs.swap_fee_lamports);
    let rent_locked_usd = to_usd(costs.rent_reserve_lamports);
    let non_recoverable_fees_usd = network_fee_usd + priority_fee_usd + swap_fee_usd;
    Ok(BuyCostValuation {
        sol_usd_at_execution: sol_usd,
        token_input_usd,
        network_fee_usd,
        priority_fee_usd,
        swap_fee_usd,
        rent_locked_usd,
        total_cash_debit_usd: to_usd(total_lamports),
        non_recoverable_fees_usd,
        position_cost_basis_usd: token_input_usd + non_recoverable_fees_usd,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buy_costs_include_token_input_rent_and_every_fee_in_cash_debit() {
        let costs = BuyCostBreakdown {
            token_input_lamports: 10_000_000,
            network_fee_lamports: 5_000,
            priority_fee_lamports: 2_000,
            swap_fee_lamports: 300_000,
            rent_reserve_lamports: 2_039_280,
        };
        let value = value_buy_costs(&costs, 200.0).unwrap();
        assert!((value.total_cash_debit_usd - 2.469256).abs() < 1e-9);
        assert!((value.rent_locked_usd - 0.407856).abs() < 1e-9);
        assert!((value.swap_fee_usd - 0.06).abs() < 1e-9);
        assert!((value.position_cost_basis_usd - 2.0614).abs() < 1e-9);
    }

    #[test]
    fn uses_execution_quote_not_a_later_market_quote() {
        let costs = BuyCostBreakdown {
            token_input_lamports: 1_000_000_000,
            network_fee_lamports: 0,
            priority_fee_lamports: 0,
            swap_fee_lamports: 0,
            rent_reserve_lamports: 0,
        };
        let at_entry = value_buy_costs(&costs, 150.0).unwrap();
        assert_eq!(at_entry.token_input_usd, 150.0);
    }

    #[test]
    fn rejects_missing_or_invalid_execution_quote() {
        let costs = BuyCostBreakdown {
            token_input_lamports: 1,
            network_fee_lamports: 0,
            priority_fee_lamports: 0,
            swap_fee_lamports: 0,
            rent_reserve_lamports: 0,
        };
        assert_eq!(
            value_buy_costs(&costs, 0.0),
            Err(CostValuationError::InvalidSolUsdQuote)
        );
    }
}
