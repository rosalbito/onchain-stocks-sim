//! Hand-written logs for the sequencer (spec section 13, item 2): each ordering rule sorting the
//! same block, then the engine matching the result. The block is the race from the mental-model
//! doc: the LP's ask rests at 100.02; inside one 100 ms block a sniper's buy arrives at 8 ms and
//! the LP's cancel at 23 ms.

use engine::{Engine, EventKind, LogRecord, MsgKind, Side, Tif};
use sim::sequencer::{sort_block, Accepted, Pending, Sequencer};
use sim::OrderingRule;

const LP: u32 = 1;
const S1: u32 = 2;
const S2: u32 = 3;
const TRADER: u32 = 4;
const MS: u64 = 1_000_000;

fn ioc_buy(px: i64, qty: u64, fee: u64) -> MsgKind {
    MsgKind::New {
        side: Side::Buy,
        px_ticks: px,
        qty_lots: qty,
        tif: Tif::Ioc,
        priority_fee: fee,
    }
}

/// The block as it arrives: (arrival ms, agent, message).
fn block() -> Vec<Pending> {
    let raw = [
        (8, S1, ioc_buy(10_003, 10, 40_000)), // sniper 1: limit seen - θ, bids 4 cents
        (9, S2, ioc_buy(10_003, 10, 60_000)), // sniper 2: one ms slower, bids 6 cents
        (15, TRADER, ioc_buy(10_007, 2, 0)),  // a regular trader
        (
            23,
            LP,
            MsgKind::Cancel {
                order_id: 1,
                priority_fee: 0,
            },
        ),
        (23, LP, ioc_buy(10_003, 1, 60_000)), // same arrival as the cancel; same fee as S2
    ];
    raw.iter()
        .enumerate()
        .map(|(i, &(ms, agent, kind))| Pending {
            t_ns: ms * MS,
            tiebreak: i as u64,
            decided_ns: 0,
            agent,
            kind,
        })
        .collect()
}

/// Sorted block as (arrival ms, agent, label).
fn order(rule: OrderingRule) -> Vec<(u64, u32, &'static str)> {
    let mut b = block();
    sort_block(rule, &mut b);
    b.iter()
        .map(|p| {
            let label = if p.kind.is_cancel() { "cancel" } else { "new" };
            (p.t_ns / MS, p.agent, label)
        })
        .collect()
}

/// Rest the LP's ask, then run the sorted block through the engine. Returns fills as
/// (taker agent, px, qty).
fn play(rule: OrderingRule) -> Vec<(u32, i64, u64)> {
    let mut e = Engine::new();
    let mut seq = 0;
    let mut rec = |agent, t_ns, kind| {
        seq += 1;
        LogRecord {
            seq,
            t_ns,
            block: 0,
            agent,
            kind,
        }
    };
    let ask = MsgKind::New {
        side: Side::Sell,
        px_ticks: 10_002,
        qty_lots: 10,
        tif: Tif::Gtc,
        priority_fee: 0,
    };
    e.on_message(&rec(LP, 0, ask));
    let mut b = block();
    sort_block(rule, &mut b);
    let mut fills = vec![];
    for p in b {
        for ev in e.on_message(&rec(p.agent, p.t_ns, p.kind)) {
            if let EventKind::Fill {
                taker_agent,
                px,
                qty,
                ..
            } = ev.kind
            {
                fills.push((taker_agent, px, qty));
            }
        }
    }
    fills
}

#[test]
fn fcfs_sorts_by_arrival() {
    assert_eq!(
        order(OrderingRule::Fcfs),
        vec![
            (8, S1, "new"),
            (9, S2, "new"),
            (15, TRADER, "new"),
            // Same arrival time: the arrival counter decides (the cancel arrived first).
            (23, LP, "cancel"),
            (23, LP, "new"),
        ]
    );
    // The sniper buys 10 at 100.02; the LP loses $0.80 against a 100.10 price.
    assert_eq!(play(OrderingRule::Fcfs), vec![(S1, 10_002, 10)]);
}

#[test]
fn pga_sorts_by_fee_then_arrival_then_tiebreak() {
    // Fees 6c (S2 at 9 ms, LP's buy at 23 ms), 4c (S1), 0 (trader, LP's cancel).
    // Equal fees fall back to arrival time.
    assert_eq!(
        order(OrderingRule::Pga),
        vec![
            (9, S2, "new"),
            (23, LP, "new"),
            (8, S1, "new"),
            (15, TRADER, "new"),
            (23, LP, "cancel"),
        ]
    );
    // The higher bidder wins the stale ask.
    assert_eq!(play(OrderingRule::Pga), vec![(S2, 10_002, 10)]);
}

#[test]
fn cancel_priority_puts_cancels_first() {
    assert_eq!(
        order(OrderingRule::CancelPriority),
        vec![
            (23, LP, "cancel"),
            (8, S1, "new"),
            (9, S2, "new"),
            (15, TRADER, "new"),
            (23, LP, "new"),
        ]
    );
    // The sniper finds no ask at 100.02; nobody trades.
    assert!(play(OrderingRule::CancelPriority).is_empty());
}

#[test]
fn sequencer_forms_blocks_by_arrival_time() {
    // B = 100 ms: arrivals in [0, 100) go to block 0, [100, 200) to block 1. The first message
    // of a block asks for its close at (k + 1)B.
    let mut s = Sequencer::new(OrderingRule::Fcfs, 100);
    let kind = MsgKind::Cancel {
        order_id: 1,
        priority_fee: 0,
    };
    assert_eq!(
        s.accept(8 * MS, 0, S1, kind),
        Accepted::Buffered {
            schedule_close: Some((0, 100 * MS))
        }
    );
    assert_eq!(
        s.accept(99 * MS, 0, S1, kind),
        Accepted::Buffered {
            schedule_close: None
        }
    );
    // Arriving exactly at the close belongs to the next block.
    assert_eq!(
        s.accept(100 * MS, 0, S1, kind),
        Accepted::Buffered {
            schedule_close: Some((1, 200 * MS))
        }
    );
    assert_eq!(s.seal(0).len(), 2);
    assert_eq!(s.waiting(), 1);

    // B = 0: every message is its own block, sequenced at once.
    let mut s = Sequencer::new(OrderingRule::Fcfs, 0);
    assert_eq!(s.accept(5, 0, S1, kind), Accepted::Now { block: 0 });
    assert_eq!(s.accept(5, 0, S2, kind), Accepted::Now { block: 1 });
}

/// Spec section 4 (rev 36): a cancel carries a priority fee, and PGA sorts it like any other
/// message. An LP that outbids the snipers on its cancel wins the stale ask.
#[test]
fn pga_sorts_cancels_by_their_fee() {
    let mut b = block();
    for p in b.iter_mut() {
        if let MsgKind::Cancel { order_id, .. } = p.kind {
            p.kind = MsgKind::Cancel {
                order_id,
                priority_fee: 70_000,
            };
        }
    }
    sort_block(OrderingRule::Pga, &mut b);
    assert!(b[0].kind.is_cancel());
    assert_eq!(b[0].kind.priority_fee(), 70_000);
}

/// β ≠ 0 runs end to end under PGA: the LP pays fees on its cancels and the books balance.
#[test]
fn lp_beta_runs_and_pays_the_chain() {
    let mut s = sim::Scenario {
        name: "beta".into(),
        duration_s: 600,
        ..sim::Scenario::default()
    };
    s.sequencer.rule = OrderingRule::Pga;
    s.sequencer.block_ms = 250;
    s.price.jump_rate_per_s = 0.05;
    let base = sim::run(&s, 1).unwrap().priority_fees;
    s.lp.beta_bps = 10_000;
    let r = sim::run(&s, 1).unwrap();
    let lp_fees: u64 = r
        .records
        .iter()
        .filter(|x| r.agents.get(&x.agent) == Some(&metrics::AccountClass::Lp))
        .map(|x| x.kind.priority_fee())
        .sum();
    assert!(lp_fees > 0);
    assert!(
        r.priority_fees > base - base / 2,
        "fees still flow to the chain"
    );
    assert_eq!(r.pnl.by_class.values().sum::<i128>(), 0);
}
