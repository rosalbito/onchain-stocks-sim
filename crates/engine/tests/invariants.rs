//! Property tests (spec section 13, item 1): after every message the book is not crossed and
//! quantity is conserved; the same log always produces the same events.

use std::collections::BTreeMap;

use engine::*;
use proptest::prelude::*;

#[derive(Debug, Clone)]
enum Op {
    New {
        agent: AgentId,
        side: Side,
        px: Px,
        qty: Qty,
        tif: Tif,
    },
    /// Cancel the order created `back` records ago (may be gone or not a New at all).
    Cancel {
        agent: AgentId,
        back: u64,
    },
    Tick {
        px: Px,
    },
}

fn op() -> impl Strategy<Value = Op> {
    let side = prop_oneof![Just(Side::Buy), Just(Side::Sell)];
    let tif = prop_oneof![Just(Tif::Gtc), Just(Tif::Ioc)];
    prop_oneof![
        6 => (0u32..4, side, 90i64..110, 1u64..30, tif)
            .prop_map(|(agent, side, px, qty, tif)| Op::New { agent, side, px, qty, tif }),
        3 => (0u32..4, 1u64..40).prop_map(|(agent, back)| Op::Cancel { agent, back }),
        1 => (90i64..110).prop_map(|px| Op::Tick { px }),
    ]
}

fn to_log(ops: &[Op]) -> Vec<LogRecord> {
    ops.iter()
        .enumerate()
        .map(|(i, op)| {
            let seq = i as u64 + 1;
            let (agent, kind) = match *op {
                Op::New {
                    agent,
                    side,
                    px,
                    qty,
                    tif,
                } => (
                    agent,
                    MsgKind::New {
                        side,
                        px_ticks: px,
                        qty_lots: qty,
                        tif,
                        priority_fee: 0,
                    },
                ),
                Op::Cancel { agent, back } => (
                    agent,
                    MsgKind::Cancel {
                        order_id: seq.saturating_sub(back),
                        priority_fee: 0,
                    },
                ),
                Op::Tick { px } => (0, MsgKind::Tick { px_ticks: px }),
            };
            LogRecord {
                seq,
                t_ns: seq * 1_000,
                block: seq,
                agent,
                kind,
            }
        })
        .collect()
}

#[derive(Default, Debug)]
struct Ledger {
    original: Qty,
    filled: Qty,
    cancelled: Qty,
    side: Option<Side>,
    limit: Px,
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn book_never_crossed_and_quantity_conserved(ops in prop::collection::vec(op(), 1..200)) {
        let log = to_log(&ops);
        let mut e = Engine::new();
        let mut orders: BTreeMap<OrderId, Ledger> = BTreeMap::new();

        for rec in &log {
            let events = e.on_message(rec);
            prop_assert!(events.iter().all(|ev| ev.seq == rec.seq), "events tagged with seq");

            let mut last_fill_px: Option<Px> = None;
            for ev in &events {
                match ev.kind {
                    EventKind::Accepted { order_id, side, px, qty, .. } => {
                        prop_assert_eq!(order_id, rec.seq);
                        orders.insert(order_id, Ledger { original: qty, side: Some(side), limit: px, ..Default::default() });
                    }
                    EventKind::Fill { maker_order, taker_order, taker_side, px, qty, maker_agent, taker_agent, .. } => {
                        prop_assert!(qty > 0);
                        prop_assert_ne!(maker_agent, taker_agent, "no self-trades");
                        let taker = &orders[&taker_order];
                        let maker = &orders[&maker_order];
                        prop_assert_eq!(px, maker.limit, "executes at the resting price");
                        prop_assert_eq!(maker.side, Some(taker_side.opposite()));
                        match taker_side {
                            Side::Buy => prop_assert!(px <= taker.limit),
                            Side::Sell => prop_assert!(px >= taker.limit),
                        }
                        // Levels are walked best first.
                        if let Some(prev) = last_fill_px {
                            match taker_side {
                                Side::Buy => prop_assert!(px >= prev),
                                Side::Sell => prop_assert!(px <= prev),
                            }
                        }
                        last_fill_px = Some(px);
                        orders.get_mut(&maker_order).unwrap().filled += qty;
                        orders.get_mut(&taker_order).unwrap().filled += qty;
                    }
                    EventKind::Cancelled { order_id, qty, .. } => {
                        orders.get_mut(&order_id).unwrap().cancelled += qty;
                    }
                    _ => {}
                }
            }

            prop_assert!(e.check_invariants().is_ok(), "{:?}", e.check_invariants());
            if let (Some((b, _)), Some((a, _))) = (e.best_bid(), e.best_ask()) {
                prop_assert!(b < a, "crossed after seq {}", rec.seq);
            }
            for (id, l) in &orders {
                let resting = e.resting(*id).map_or(0, |r| r.qty);
                prop_assert_eq!(
                    l.original, l.filled + l.cancelled + resting,
                    "order {} not conserved: {:?} resting {}", id, l, resting
                );
            }
        }
    }

    #[test]
    fn same_log_same_events(ops in prop::collection::vec(op(), 1..200)) {
        let log = to_log(&ops);
        let run = || {
            let mut e = Engine::new();
            log.iter().flat_map(|r| e.on_message(r)).collect::<Vec<_>>()
        };
        prop_assert_eq!(run(), run());
    }
}
