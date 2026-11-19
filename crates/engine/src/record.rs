//! The input-log record (spec section 4) and the engine's output events (spec section 5).

/// Price in whole ticks (default tick = $0.01).
pub type Px = i64;
/// Size in whole lots.
pub type Qty = u64;
/// Order id. An order's id is the `seq` of the `New` record that created it.
pub type OrderId = u64;
/// Sender id. Never used for priority; only for self-trade prevention and reporting.
pub type AgentId = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn opposite(self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

/// Time in force.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Tif {
    /// Good till cancelled: the remainder rests in the book.
    Gtc,
    /// Immediate or cancel: the remainder is cancelled.
    Ioc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MsgKind {
    New {
        side: Side,
        px_ticks: Px,
        qty_lots: Qty,
        tif: Tif,
        priority_fee: u64,
    },
    Cancel {
        order_id: OrderId,
        /// PGA bid, paid by the sender when the cancel is included (spec section 4).
        priority_fee: u64,
    },
    /// A Nasdaq price update. Ignored by matching, echoed as an event.
    Tick { px_ticks: Px },
    /// A live control change (ordering rule, block length, load, playback speed), logged so a
    /// run with live changes replays exactly (spec section 12). Opaque to the engine: it takes a
    /// `seq` like any record and produces no events. The harness gives `param` its meaning.
    Config { param: u32, value: u64 },
}

impl MsgKind {
    pub fn is_cancel(&self) -> bool {
        matches!(self, MsgKind::Cancel { .. })
    }

    pub fn priority_fee(&self) -> u64 {
        match self {
            MsgKind::New { priority_fee, .. } | MsgKind::Cancel { priority_fee, .. } => {
                *priority_fee
            }
            MsgKind::Tick { .. } | MsgKind::Config { .. } => 0,
        }
    }
}

/// One sequenced message. `seq` alone decides processing order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LogRecord {
    pub seq: u64,
    /// Virtual time the sequencer received it.
    pub t_ns: u64,
    /// Block it was sealed in.
    pub block: u64,
    /// Sender.
    pub agent: AgentId,
    pub kind: MsgKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CancelReason {
    /// The owner asked for it.
    User,
    /// IOC remainder after matching.
    IocRemainder,
    /// Resting order cancelled because an incoming order from the same agent would have hit it.
    SelfTrade,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RejectReason {
    ZeroQty,
    NonPositivePrice,
    /// `seq` did not increase. The record is ignored.
    SeqNotIncreasing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventKind {
    /// A `New` was accepted and assigned `order_id` (= its `seq`).
    Accepted {
        order_id: OrderId,
        agent: AgentId,
        side: Side,
        px: Px,
        qty: Qty,
        tif: Tif,
    },
    /// A trade at the resting (maker) order's price.
    Fill {
        maker_order: OrderId,
        maker_agent: AgentId,
        taker_order: OrderId,
        taker_agent: AgentId,
        /// Side of the taker (incoming) order.
        taker_side: Side,
        px: Px,
        qty: Qty,
        /// Maker quantity left resting after this fill.
        maker_remaining: Qty,
    },
    /// Some quantity of an order was removed from the book (or, for IOC, never rested).
    Cancelled {
        order_id: OrderId,
        agent: AgentId,
        qty: Qty,
        reason: CancelReason,
    },
    /// A cancel for an order that is not resting (already filled, cancelled or unknown).
    /// A no-op, reported.
    CancelMissed { order_id: OrderId, agent: AgentId },
    /// Aggregate quantity at a price level after a change. `qty == 0` means the level is gone.
    BookChange { side: Side, px: Px, qty: Qty },
    /// Echo of a `Tick` record.
    Tick { px: Px },
    /// The record was malformed and ignored.
    Rejected {
        agent: AgentId,
        reason: RejectReason,
    },
}

/// An engine output event, tagged with the `seq` of the record that caused it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Event {
    pub seq: u64,
    pub kind: EventKind,
}

/// The events produced by one record, in the order they happened.
pub type Events = Vec<Event>;
