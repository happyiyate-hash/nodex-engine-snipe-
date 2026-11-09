use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub const LAMPORTS_PER_SOL: u64 = 1_000_000_000;
// Runtime startup calculates the actual $10 balance from a live SOL/USD quote.
pub const DEFAULT_STARTING_BALANCE_LAMPORTS: u64 = 10_000_000;
pub const DEFAULT_TRADE_AMOUNT_LAMPORTS: u64 = 10_000_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuyCostBreakdown {
    pub token_input_lamports: u64,
    pub network_fee_lamports: u64,
    pub priority_fee_lamports: u64,
    pub swap_fee_lamports: u64,
    pub rent_reserve_lamports: u64,
}

impl BuyCostBreakdown {
    pub fn total_debit_lamports(&self) -> Option<u64> {
        self.token_input_lamports
            .checked_add(self.network_fee_lamports)?
            .checked_add(self.priority_fee_lamports)?
            .checked_add(self.swap_fee_lamports)?
            .checked_add(self.rent_reserve_lamports)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaperWallet {
    pub starting_balance_lamports: u64,
    pub available_balance_lamports: u64,
    pub invested_balance_lamports: u64,
    pub locked_rent_lamports: u64,
    pub total_network_fees_lamports: u64,
    pub total_priority_fees_lamports: u64,
    pub total_swap_fees_lamports: u64,
    pub total_rent_refunded_lamports: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LedgerEntry {
    pub id: Uuid,
    pub kind: String,
    pub amount_lamports: u64,
    pub available_after_lamports: u64,
    pub reference: Option<String>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PaperWalletError {
    #[error("insufficient available paper SOL")]
    InsufficientBalance,
    #[error("lamport arithmetic overflow")]
    ArithmeticOverflow,
    #[error("rent refund exceeds locked rent")]
    InvalidRentRefund,
}

impl Default for PaperWallet {
    fn default() -> Self {
        Self::new(DEFAULT_STARTING_BALANCE_LAMPORTS)
    }
}

impl PaperWallet {
    pub fn new(starting_balance_lamports: u64) -> Self {
        Self {
            starting_balance_lamports,
            available_balance_lamports: starting_balance_lamports,
            invested_balance_lamports: 0,
            locked_rent_lamports: 0,
            total_network_fees_lamports: 0,
            total_priority_fees_lamports: 0,
            total_swap_fees_lamports: 0,
            total_rent_refunded_lamports: 0,
        }
    }

    pub fn reserve_buy(
        &mut self,
        costs: &BuyCostBreakdown,
    ) -> Result<LedgerEntry, PaperWalletError> {
        let total = costs
            .total_debit_lamports()
            .ok_or(PaperWalletError::ArithmeticOverflow)?;
        if total > self.available_balance_lamports {
            return Err(PaperWalletError::InsufficientBalance);
        }
        let invested = self
            .invested_balance_lamports
            .checked_add(costs.token_input_lamports)
            .ok_or(PaperWalletError::ArithmeticOverflow)?;
        let rent = self
            .locked_rent_lamports
            .checked_add(costs.rent_reserve_lamports)
            .ok_or(PaperWalletError::ArithmeticOverflow)?;
        let network = self
            .total_network_fees_lamports
            .checked_add(costs.network_fee_lamports)
            .ok_or(PaperWalletError::ArithmeticOverflow)?;
        let priority = self
            .total_priority_fees_lamports
            .checked_add(costs.priority_fee_lamports)
            .ok_or(PaperWalletError::ArithmeticOverflow)?;
        let swap = self
            .total_swap_fees_lamports
            .checked_add(costs.swap_fee_lamports)
            .ok_or(PaperWalletError::ArithmeticOverflow)?;
        let available = self.available_balance_lamports - total;

        self.available_balance_lamports = available;
        self.invested_balance_lamports = invested;
        self.locked_rent_lamports = rent;
        self.total_network_fees_lamports = network;
        self.total_priority_fees_lamports = priority;
        self.total_swap_fees_lamports = swap;

        Ok(LedgerEntry {
            id: Uuid::new_v4(),
            kind: "paper_buy_reserve".into(),
            amount_lamports: total,
            available_after_lamports: available,
            reference: None,
        })
    }

    pub fn refund_rent(&mut self, amount_lamports: u64) -> Result<LedgerEntry, PaperWalletError> {
        if amount_lamports > self.locked_rent_lamports {
            return Err(PaperWalletError::InvalidRentRefund);
        }
        let available = self
            .available_balance_lamports
            .checked_add(amount_lamports)
            .ok_or(PaperWalletError::ArithmeticOverflow)?;
        let refunded = self
            .total_rent_refunded_lamports
            .checked_add(amount_lamports)
            .ok_or(PaperWalletError::ArithmeticOverflow)?;
        self.locked_rent_lamports -= amount_lamports;
        self.available_balance_lamports = available;
        self.total_rent_refunded_lamports = refunded;
        Ok(LedgerEntry {
            id: Uuid::new_v4(),
            kind: "rent_refund".into(),
            amount_lamports,
            available_after_lamports: available,
            reference: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn costs() -> BuyCostBreakdown {
        BuyCostBreakdown {
            token_input_lamports: DEFAULT_TRADE_AMOUNT_LAMPORTS,
            network_fee_lamports: 5_000,
            priority_fee_lamports: 2_000,
            swap_fee_lamports: 300_000,
            rent_reserve_lamports: 2_039_280,
        }
    }

    #[test]
    fn defaults_to_small_fallback_and_point_zero_one_sol_trade() {
        let wallet = PaperWallet::default();
        assert_eq!(wallet.starting_balance_lamports, 10_000_000);
        assert_eq!(DEFAULT_TRADE_AMOUNT_LAMPORTS, 10_000_000);
    }

    #[test]
    fn buy_debits_costs_and_tracks_recoverable_rent_separately() {
        let mut wallet = PaperWallet::default();
        let entry = wallet.reserve_buy(&costs()).unwrap();
        assert_eq!(entry.amount_lamports, 102_346_280);
        assert_eq!(wallet.invested_balance_lamports, 100_000_000);
        assert_eq!(wallet.locked_rent_lamports, 2_039_280);
        assert_eq!(wallet.total_network_fees_lamports, 5_000);
    }

    #[test]
    fn rejects_buy_without_enough_available_balance() {
        let mut wallet = PaperWallet::new(1);
        assert_eq!(
            wallet.reserve_buy(&costs()),
            Err(PaperWalletError::InsufficientBalance)
        );
    }

    #[test]
    fn rent_refund_returns_to_available_balance() {
        let mut wallet = PaperWallet::default();
        wallet.reserve_buy(&costs()).unwrap();
        wallet.refund_rent(2_039_280).unwrap();
        assert_eq!(wallet.locked_rent_lamports, 0);
        assert_eq!(wallet.total_rent_refunded_lamports, 2_039_280);
    }
}
