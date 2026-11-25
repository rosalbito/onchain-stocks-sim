//! Market metrics (spec section 10): the five account classes, the zero-sum check (section 9),
//! race counting and every market metric of a run ([`measure`]).
//!
//! No floats here: rates are returned as integer numerator/denominator pairs and converted only
//! when a report prints them.

pub mod measure;
mod races;
mod zero_sum;

pub use measure::{measure, MeasureInput, Metrics, PricePath, Weekend};
pub use races::{Race, RaceOutcome, RaceStats, RaceTracker};
pub use zero_sum::{class_pnl, format_cash, zero_sum_check, Balance, ClassPnl, ZeroSumError};

/// Cash is held in units of 1/10_000 of a tick·lot, so a fee in whole basis points of notional is
/// exact: `fee = px_ticks * qty_lots * fee_bps`. With a $0.01 tick and 1-share lots, one cash unit
/// is $0.000001.
pub const CASH_PER_TICK_LOT: i128 = 10_000;

/// The five account classes of spec section 9.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AccountClass {
    Regular,
    Lp,
    Sniper,
    Protocol,
    Chain,
}

impl AccountClass {
    pub const ALL: [AccountClass; 5] = [
        AccountClass::Regular,
        AccountClass::Lp,
        AccountClass::Sniper,
        AccountClass::Protocol,
        AccountClass::Chain,
    ];

    pub fn label(self) -> &'static str {
        match self {
            AccountClass::Regular => "regular traders",
            AccountClass::Lp => "LPs",
            AccountClass::Sniper => "snipers",
            AccountClass::Protocol => "protocol",
            AccountClass::Chain => "chain",
        }
    }
}
