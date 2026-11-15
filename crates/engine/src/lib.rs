//! Deterministic continuous limit order book.
//!
//! The engine matches each record the moment it processes it, in `seq` order, and knows nothing
//! about agents' strategies, time or harnesses. Determinism rules (spec section 4): integer ticks
//! and lots, no floats, no hash-map iteration, no wall clock, no threads.
//!
//! ```
//! use engine::*;
//! let mut e = Engine::new();
//! let rec = |seq, agent, kind| LogRecord { seq, t_ns: 0, block: 0, agent, kind };
//! let new = |side, px_ticks, qty_lots, tif| MsgKind::New { side, px_ticks, qty_lots, tif, priority_fee: 0 };
//! e.on_message(&rec(1, 1, new(Side::Sell, 10_001, 10, Tif::Gtc)));
//! let ev = e.on_message(&rec(2, 2, new(Side::Buy, 10_002, 4, Tif::Ioc)));
//! assert!(ev.iter().any(|e| matches!(e.kind, EventKind::Fill { px: 10_001, qty: 4, .. })));
//! ```

mod book;
mod record;

pub use book::{Engine, RestingOrder};
pub use record::*;
