use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

pub const LAMPORTS_PER_SOL: u64 = 1_000_000_000;
// Runtime startup calculates the actual $10 balance from a live SOL/USD quote.
pub const DEFAULT_STARTING_BALANCE_LAMPORTS: u64 = 20_000_000;
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PaperWallet {
    pub starting_balance_lamports: u64,
    /// SOL/USD quote captured when the original paper balance was funded.
    /// This lets the portfolio mark remaining SOL holdings to market.
    pub starting_sol_price_usd: f64,
    pub available_balance_lamports: u64,
    pub invested_balance_lamports: u64,
    pub locked_rent_lamports: u64,
    pub total_network_fees_lamports: u64,
    pub total_priority_fees_lamports: u64,
    pub total_swap_fees_lamports: u64,
    pub total_rent_refunded_lamports: u64,
    /// Rent held for each simulated token account, keyed by mint. Older wallet snapshots default to empty.
    #[serde(default)]
    pub rent_by_token_account: HashMap<String, u64>,
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
    #[error("rent is already tracked for this token account")]
    RentAccountAlreadyTracked,
}

impl Default for PaperWallet {
    fn default() -> Self {
        Self::new(DEFAULT_STARTING_BALANCE_LAMPORTS)
    }
}

impl PaperWallet {
    pub fn new(starting_balance_lamports: u64) -> Self {
        Self::new_with_price(starting_balance_lamports, 0.0)
    }

    pub fn new_with_price(starting_balance_lamports: u64, starting_sol_price_usd: f64) -> Self {
        Self {
            starting_balance_lamports,
            starting_sol_price_usd,
            available_balance_lamports: starting_balance_lamports,
            invested_balance_lamports: 0,
            locked_rent_lamports: 0,
            total_network_fees_lamports: 0,
            total_priority_fees_lamports: 0,
            total_swap_fees_lamports: 0,
            total_rent_refunded_lamports: 0,
            rent_by_token_account: HashMap::new(),
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

    /// Reserve buy costs and associate the rent reserve with the token mint.
    /// The mint key makes later full-close refunds idempotent per token account.
    pub fn reserve_buy_for_token(
        &mut self,
        mint: &str,
        costs: &BuyCostBreakdown,
    ) -> Result<LedgerEntry, PaperWalletError> {
        if self.rent_by_token_account.contains_key(mint) {
            return Err(PaperWalletError::RentAccountAlreadyTracked);
        }
        let entry = self.reserve_buy(costs)?;
        if costs.rent_reserve_lamports > 0 {
            self.rent_by_token_account
                .insert(mint.to_owned(), costs.rent_reserve_lamports);
        }
        Ok(LedgerEntry { reference: Some(mint.to_owned()), ..entry })
    }

    /// Simulate closing a token account after a fully settled zero-balance sale.
    /// A repeated close is a no-op, preventing duplicate rent credits.
    pub fn close_token_account_and_recover_rent(
        &mut self,
        mint: &str,
    ) -> Result<Option<LedgerEntry>, PaperWalletError> {
        let Some(amount) = self.rent_by_token_account.remove(mint) else {
            return Ok(None);
        };
        let entry = self.refund_rent(amount)?;
        Ok(Some(LedgerEntry { reference: Some(mint.to_owned()), ..entry }))
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
        assert_eq!(wallet.starting_balance_lamports, 20_000_000);
        assert_eq!(DEFAULT_TRADE_AMOUNT_LAMPORTS, 10_000_000);
    }

    #[test]
    fn buy_debits_costs_and_tracks_recoverable_rent_separately() {
        let mut wallet = PaperWallet::default();
        let entry = wallet.reserve_buy(&costs()).unwrap();
        assert_eq!(entry.amount_lamports, 12_346_280);
        assert_eq!(wallet.invested_balance_lamports, 10_000_000);
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
    fn full_token_account_close_refunds_rent_only_once() {
        let mut wallet = PaperWallet::default();
        wallet.reserve_buy_for_token("mint-1", &costs()).unwrap();
        let before = wallet.available_balance_lamports;
        let first = wallet.close_token_account_and_recover_rent("mint-1").unwrap();
        assert_eq!(first.as_ref().unwrap().amount_lamports, 2_039_280);
        assert_eq!(wallet.available_balance_lamports, before + 2_039_280);
        assert_eq!(wallet.locked_rent_lamports, 0);
        let after_first_close = wallet.available_balance_lamports;
        assert!(wallet.close_token_account_and_recover_rent("mint-1").unwrap().is_none());
        assert_eq!(wallet.available_balance_lamports, after_first_close);
    }

    #[test]
    fn token_account_rent_cannot_be_tracked_twice() {
        let mut wallet = PaperWallet::default();
        wallet.reserve_buy_for_token("mint-1", &costs()).unwrap();
        assert_eq!(
            wallet.reserve_buy_for_token("mint-1", &costs()),
            Err(PaperWalletError::RentAccountAlreadyTracked)
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
