//! The zero-sum check: every fill and fee is a transfer, so across all accounts cash changes sum
//! to zero, share changes sum to zero, and PnL marked at any price sums to zero.

use std::collections::BTreeMap;
use std::fmt;

use crate::{AccountClass, CASH_PER_TICK_LOT};

/// One account's change since the start of the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Balance {
    pub class: AccountClass,
    /// Cash change in cash units (see [`CASH_PER_TICK_LOT`]).
    pub cash_delta: i128,
    /// Share change in lots.
    pub shares_delta: i64,
}

impl Balance {
    /// PnL marked to `mark_px` (ticks), in cash units.
    pub fn pnl(&self, mark_px: i64) -> i128 {
        self.cash_delta + self.shares_delta as i128 * mark_px as i128 * CASH_PER_TICK_LOT
    }
}

/// PnL per account class, in cash units, marked at one price.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassPnl {
    pub mark_px: i64,
    pub by_class: BTreeMap<AccountClass, i128>,
}

impl ClassPnl {
    pub fn get(&self, class: AccountClass) -> i128 {
        self.by_class.get(&class).copied().unwrap_or(0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZeroSumError {
    pub cash_sum: i128,
    pub shares_sum: i64,
    pub pnl_sum: i128,
}

impl fmt::Display for ZeroSumError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ledger does not balance: cash sum {}, shares sum {}, PnL sum {}",
            self.cash_sum, self.shares_sum, self.pnl_sum
        )
    }
}

impl std::error::Error for ZeroSumError {}

/// PnL per class, without checking.
pub fn class_pnl(balances: &[Balance], mark_px: i64) -> ClassPnl {
    let mut by_class: BTreeMap<AccountClass, i128> =
        AccountClass::ALL.iter().map(|&c| (c, 0)).collect();
    for b in balances {
        *by_class.get_mut(&b.class).expect("all classes present") += b.pnl(mark_px);
    }
    ClassPnl { mark_px, by_class }
}

/// Check that cash, shares and marked PnL all sum to exactly zero, and return PnL per class.
pub fn zero_sum_check(balances: &[Balance], mark_px: i64) -> Result<ClassPnl, ZeroSumError> {
    let cash_sum: i128 = balances.iter().map(|b| b.cash_delta).sum();
    let shares_sum: i64 = balances.iter().map(|b| b.shares_delta).sum();
    let pnl = class_pnl(balances, mark_px);
    let pnl_sum: i128 = pnl.by_class.values().sum();
    if cash_sum != 0 || shares_sum != 0 || pnl_sum != 0 {
        return Err(ZeroSumError {
            cash_sum,
            shares_sum,
            pnl_sum,
        });
    }
    Ok(pnl)
}

/// Format cash units as dollars, assuming a $0.01 tick and 1-share lots (1 unit = $0.000001).
/// Integer formatting only.
pub fn format_cash(units: i128) -> String {
    // units / 10_000 = tick·lots = cents; dollars = units / 1_000_000.
    let sign = if units < 0 { "-" } else { "" };
    let a = units.unsigned_abs();
    let dollars = a / 1_000_000;
    let cents = (a % 1_000_000 + 5_000) / 10_000; // round to the cent
    let (dollars, cents) = if cents == 100 {
        (dollars + 1, 0)
    } else {
        (dollars, cents)
    };
    format!("{sign}${dollars}.{cents:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn balanced_transfers_pass_and_unbalanced_fail() {
        // Buyer pays 10 lots @ 100 ticks plus 1 bp fee; seller receives; protocol gets the fee.
        let notional = 100 * 10 * CASH_PER_TICK_LOT;
        let fee = 100 * 10; // 1 bp of notional in cash units
        let ok = [
            Balance {
                class: AccountClass::Sniper,
                cash_delta: -notional - fee,
                shares_delta: 10,
            },
            Balance {
                class: AccountClass::Lp,
                cash_delta: notional,
                shares_delta: -10,
            },
            Balance {
                class: AccountClass::Protocol,
                cash_delta: fee,
                shares_delta: 0,
            },
        ];
        let pnl = zero_sum_check(&ok, 105).unwrap();
        assert_eq!(
            pnl.get(AccountClass::Sniper),
            5 * 10 * CASH_PER_TICK_LOT - fee
        );
        assert_eq!(pnl.get(AccountClass::Lp), -5 * 10 * CASH_PER_TICK_LOT);

        let mut bad = ok;
        bad[2].cash_delta = 0; // fee taken from the buyer but never credited
        assert!(zero_sum_check(&bad, 105).is_err());
    }

    #[test]
    fn cash_formatting() {
        assert_eq!(format_cash(0), "$0.00");
        assert_eq!(format_cash(1_000_000), "$1.00");
        assert_eq!(format_cash(-800_000), "-$0.80");
        assert_eq!(format_cash(1_995_000), "$2.00");
        assert_eq!(format_cash(12_340_000), "$12.34");
    }
}
