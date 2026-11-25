//! The order book and the matching loop (spec section 5).
//!
//! `BTreeMap<px, Level>` per side, orders in a `slab`, and an id → slab key index for cancels.
//! Nothing here iterates a hash map, reads a clock or uses floats.

use std::collections::{BTreeMap, VecDeque};

use slab::Slab;

use crate::record::*;

#[derive(Debug, Clone)]
struct Order {
    id: OrderId,
    agent: AgentId,
    side: Side,
    px: Px,
    qty: Qty,
}

#[derive(Debug, Default, Clone)]
struct Level {
    /// Slab keys, earliest resting order first.
    orders: VecDeque<usize>,
    total: Qty,
}

/// A deterministic single-instrument CLOB with price-time priority.
#[derive(Debug, Default)]
pub struct Engine {
    orders: Slab<Order>,
    index: BTreeMap<OrderId, usize>,
    bids: BTreeMap<Px, Level>,
    asks: BTreeMap<Px, Level>,
    last_seq: Option<u64>,
    /// Empty order queues of removed levels, reused when a new level opens, so a level that
    /// comes and goes does not allocate each time.
    spare: Vec<VecDeque<usize>>,
}

/// At most this many spare level queues are kept.
const MAX_SPARE: usize = 256;

/// A resting order as seen from outside the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestingOrder {
    pub id: OrderId,
    pub agent: AgentId,
    pub side: Side,
    pub px: Px,
    pub qty: Qty,
}

impl Engine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Process one sequenced record and return the events it caused, each tagged with its `seq`.
    /// Allocates the returned `Vec`; [`Engine::on_message_into`] reuses a caller's buffer.
    pub fn on_message(&mut self, msg: &LogRecord) -> Events {
        let mut out = Vec::new();
        self.on_message_into(msg, &mut out);
        out
    }

    /// [`Engine::on_message`] into a reused buffer: `out` is cleared, then holds the events.
    pub fn on_message_into(&mut self, msg: &LogRecord, out: &mut Events) {
        out.clear();
        let seq = msg.seq;
        let push = |out: &mut Events, kind| out.push(Event { seq, kind });

        if let Some(last) = self.last_seq {
            if seq <= last {
                push(
                    out,
                    EventKind::Rejected {
                        agent: msg.agent,
                        reason: RejectReason::SeqNotIncreasing,
                    },
                );
                return;
            }
        }
        self.last_seq = Some(seq);

        match msg.kind {
            MsgKind::Tick { px_ticks } => push(out, EventKind::Tick { px: px_ticks }),
            MsgKind::Config { .. } => {}
            MsgKind::Cancel { order_id, .. } => self.cancel(seq, msg.agent, order_id, out),
            MsgKind::New {
                side,
                px_ticks,
                qty_lots,
                tif,
                ..
            } => {
                if qty_lots == 0 {
                    push(
                        out,
                        EventKind::Rejected {
                            agent: msg.agent,
                            reason: RejectReason::ZeroQty,
                        },
                    );
                } else if px_ticks <= 0 {
                    push(
                        out,
                        EventKind::Rejected {
                            agent: msg.agent,
                            reason: RejectReason::NonPositivePrice,
                        },
                    );
                } else {
                    self.new_order(seq, msg.agent, side, px_ticks, qty_lots, tif, out);
                }
            }
        }
    }

    /// Keep an emptied level's queue for the next new level.
    fn recycle(spare: &mut Vec<VecDeque<usize>>, level: Option<Level>) {
        if let Some(level) = level {
            if spare.len() < MAX_SPARE {
                spare.push(level.orders);
            }
        }
    }

    fn cancel(&mut self, seq: u64, agent: AgentId, order_id: OrderId, out: &mut Events) {
        // Only the owner may cancel; anyone else's cancel is a reported no-op.
        let owned = self
            .index
            .get(&order_id)
            .is_some_and(|&k| self.orders[k].agent == agent);
        if !owned {
            out.push(Event {
                seq,
                kind: EventKind::CancelMissed { order_id, agent },
            });
            return;
        }
        let key = self.index[&order_id];
        self.remove_resting(seq, key, CancelReason::User, out);
    }

    /// Remove a resting order from its level and the index, emitting `Cancelled` and `BookChange`.
    fn remove_resting(&mut self, seq: u64, key: usize, reason: CancelReason, out: &mut Events) {
        let order = self.orders.remove(key);
        self.index.remove(&order.id);
        let book = match order.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let level = book.get_mut(&order.px).expect("resting order has a level");
        let pos = level
            .orders
            .iter()
            .position(|&k| k == key)
            .expect("resting order is in its level");
        level.orders.remove(pos);
        level.total -= order.qty;
        let level_qty = level.total;
        if level.orders.is_empty() {
            Self::recycle(&mut self.spare, book.remove(&order.px));
        }
        out.push(Event {
            seq,
            kind: EventKind::Cancelled {
                order_id: order.id,
                agent: order.agent,
                qty: order.qty,
                reason,
            },
        });
        out.push(Event {
            seq,
            kind: EventKind::BookChange {
                side: order.side,
                px: order.px,
                qty: level_qty,
            },
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn new_order(
        &mut self,
        seq: u64,
        agent: AgentId,
        side: Side,
        px: Px,
        qty: Qty,
        tif: Tif,
        out: &mut Events,
    ) {
        let id: OrderId = seq;
        out.push(Event {
            seq,
            kind: EventKind::Accepted {
                order_id: id,
                agent,
                side,
                px,
                qty,
                tif,
            },
        });

        let mut remaining = qty;
        while remaining > 0 {
            // Best opposite level that crosses the limit.
            let best = match side {
                Side::Buy => self.asks.first_key_value().map(|(&p, _)| p),
                Side::Sell => self.bids.last_key_value().map(|(&p, _)| p),
            };
            let Some(level_px) = best else { break };
            let crosses = match side {
                Side::Buy => level_px <= px,
                Side::Sell => level_px >= px,
            };
            if !crosses {
                break;
            }

            let maker_key = {
                let book = match side {
                    Side::Buy => &self.asks,
                    Side::Sell => &self.bids,
                };
                *book[&level_px]
                    .orders
                    .front()
                    .expect("levels are never empty")
            };

            if self.orders[maker_key].agent == agent {
                // Self-trade: cancel the resting order and keep matching.
                self.remove_resting(seq, maker_key, CancelReason::SelfTrade, out);
                continue;
            }

            let maker = &mut self.orders[maker_key];
            let fill = remaining.min(maker.qty);
            maker.qty -= fill;
            remaining -= fill;
            let (maker_id, maker_agent, maker_left) = (maker.id, maker.agent, maker.qty);

            let book = match side {
                Side::Buy => &mut self.asks,
                Side::Sell => &mut self.bids,
            };
            let level = book.get_mut(&level_px).expect("level exists");
            level.total -= fill;
            let level_qty = level.total;
            if maker_left == 0 {
                level.orders.pop_front();
                if level.orders.is_empty() {
                    Self::recycle(&mut self.spare, book.remove(&level_px));
                }
                self.orders.remove(maker_key);
                self.index.remove(&maker_id);
            }

            out.push(Event {
                seq,
                kind: EventKind::Fill {
                    maker_order: maker_id,
                    maker_agent,
                    taker_order: id,
                    taker_agent: agent,
                    taker_side: side,
                    px: level_px,
                    qty: fill,
                    maker_remaining: maker_left,
                },
            });
            out.push(Event {
                seq,
                kind: EventKind::BookChange {
                    side: side.opposite(),
                    px: level_px,
                    qty: level_qty,
                },
            });
        }

        if remaining == 0 {
            return;
        }
        match tif {
            Tif::Ioc => out.push(Event {
                seq,
                kind: EventKind::Cancelled {
                    order_id: id,
                    agent,
                    qty: remaining,
                    reason: CancelReason::IocRemainder,
                },
            }),
            Tif::Gtc => {
                let key = self.orders.insert(Order {
                    id,
                    agent,
                    side,
                    px,
                    qty: remaining,
                });
                self.index.insert(id, key);
                let book = match side {
                    Side::Buy => &mut self.bids,
                    Side::Sell => &mut self.asks,
                };
                let spare = &mut self.spare;
                let level = book.entry(px).or_insert_with(|| Level {
                    orders: spare.pop().unwrap_or_default(),
                    total: 0,
                });
                level.orders.push_back(key);
                level.total += remaining;
                out.push(Event {
                    seq,
                    kind: EventKind::BookChange {
                        side,
                        px,
                        qty: level.total,
                    },
                });
            }
        }
    }

    pub fn best_bid(&self) -> Option<(Px, Qty)> {
        self.bids.last_key_value().map(|(&p, l)| (p, l.total))
    }

    pub fn best_ask(&self) -> Option<(Px, Qty)> {
        self.asks.first_key_value().map(|(&p, l)| (p, l.total))
    }

    /// Aggregate levels, best first.
    pub fn depth(&self, side: Side) -> Vec<(Px, Qty)> {
        match side {
            Side::Buy => self.bids.iter().rev().map(|(&p, l)| (p, l.total)).collect(),
            Side::Sell => self.asks.iter().map(|(&p, l)| (p, l.total)).collect(),
        }
    }

    pub fn resting(&self, id: OrderId) -> Option<RestingOrder> {
        self.index.get(&id).map(|&k| {
            let o = &self.orders[k];
            RestingOrder {
                id: o.id,
                agent: o.agent,
                side: o.side,
                px: o.px,
                qty: o.qty,
            }
        })
    }

    /// All resting orders in id order.
    pub fn resting_orders(&self) -> Vec<RestingOrder> {
        self.index
            .keys()
            .map(|&id| self.resting(id).expect("indexed"))
            .collect()
    }

    pub fn order_count(&self) -> usize {
        self.orders.len()
    }

    /// Check internal consistency. Returns a description of the first problem found.
    /// Used by tests; cheap enough to call after every message in a property test.
    pub fn check_invariants(&self) -> Result<(), String> {
        if let (Some((b, _)), Some((a, _))) = (self.best_bid(), self.best_ask()) {
            if b >= a {
                return Err(format!("book crossed: bid {b} >= ask {a}"));
            }
        }
        let mut seen = 0usize;
        for (side, book) in [(Side::Buy, &self.bids), (Side::Sell, &self.asks)] {
            for (&px, level) in book {
                if level.orders.is_empty() {
                    return Err(format!("empty level {side:?} {px}"));
                }
                let mut sum = 0;
                let mut last_id = None;
                for &k in &level.orders {
                    let o = self.orders.get(k).ok_or("dangling slab key")?;
                    if o.side != side || o.px != px || o.qty == 0 {
                        return Err(format!("bad order in level {side:?} {px}: {o:?}"));
                    }
                    if self.index.get(&o.id) != Some(&k) {
                        return Err(format!("index mismatch for {}", o.id));
                    }
                    if last_id.is_some_and(|l| l >= o.id) {
                        return Err(format!("time priority broken at {side:?} {px}"));
                    }
                    last_id = Some(o.id);
                    sum += o.qty;
                    seen += 1;
                }
                if sum != level.total {
                    return Err(format!("level total {} != sum {sum}", level.total));
                }
            }
        }
        if seen != self.orders.len() || seen != self.index.len() {
            return Err(format!(
                "count mismatch: levels {seen}, slab {}, index {}",
                self.orders.len(),
                self.index.len()
            ));
        }
        Ok(())
    }
}
