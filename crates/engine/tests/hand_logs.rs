//! Hand-written logs with fills worked out by hand (spec section 13, item 2).
//!
//! Each test states the expected fills explicitly and also snapshots the full event stream with
//! `insta`, so any change to the engine's output shows up as a reviewed diff.
//! The ordering-rule cases ("each ordering rule sorting the same block") live in the `sim` crate,
//! next to the sequencer.

use engine::*;

const LP: AgentId = 1;
const SNIPER: AgentId = 2;
const TRADER: AgentId = 3;

fn gtc(side: Side, px: Px, qty: Qty) -> MsgKind {
    MsgKind::New {
        side,
        px_ticks: px,
        qty_lots: qty,
        tif: Tif::Gtc,
        priority_fee: 0,
    }
}

fn ioc(side: Side, px: Px, qty: Qty) -> MsgKind {
    MsgKind::New {
        side,
        px_ticks: px,
        qty_lots: qty,
        tif: Tif::Ioc,
        priority_fee: 0,
    }
}

fn cancel(order_id: OrderId) -> MsgKind {
    MsgKind::Cancel { order_id }
}

/// Run a log where record i gets seq i+1. Returns the engine and every event.
fn run(log: &[(AgentId, MsgKind)]) -> (Engine, Vec<Event>) {
    let mut e = Engine::new();
    let mut all = Vec::new();
    for (i, &(agent, kind)) in log.iter().enumerate() {
        let rec = LogRecord {
            seq: i as u64 + 1,
            t_ns: 0,
            block: 0,
            agent,
            kind,
        };
        all.extend(e.on_message(&rec));
        e.check_invariants().unwrap();
    }
    (e, all)
}

/// (maker order, taker order, px, qty)
fn fills(events: &[Event]) -> Vec<(OrderId, OrderId, Px, Qty)> {
    events
        .iter()
        .filter_map(|e| match e.kind {
            EventKind::Fill {
                maker_order,
                taker_order,
                px,
                qty,
                ..
            } => Some((maker_order, taker_order, px, qty)),
            _ => None,
        })
        .collect()
}

fn cancels(events: &[Event]) -> Vec<(OrderId, Qty, CancelReason)> {
    events
        .iter()
        .filter_map(|e| match e.kind {
            EventKind::Cancelled {
                order_id,
                qty,
                reason,
                ..
            } => Some((order_id, qty, reason)),
            _ => None,
        })
        .collect()
}

fn missed(events: &[Event]) -> Vec<OrderId> {
    events
        .iter()
        .filter_map(|e| match e.kind {
            EventKind::CancelMissed { order_id, .. } => Some(order_id),
            _ => None,
        })
        .collect()
}

#[test]
fn h01_gtc_rests_without_fill() {
    let (e, ev) = run(&[
        (LP, gtc(Side::Buy, 9_999, 10)),
        (LP, gtc(Side::Sell, 10_001, 10)),
    ]);
    assert!(fills(&ev).is_empty());
    assert_eq!(e.best_bid(), Some((9_999, 10)));
    assert_eq!(e.best_ask(), Some((10_001, 10)));
    insta::assert_debug_snapshot!(ev);
}

#[test]
fn h02_executes_at_resting_price() {
    // Buy limit 10_005 against an ask at 10_001: trades at 10_001, not 10_005.
    let (e, ev) = run(&[
        (LP, gtc(Side::Sell, 10_001, 10)),
        (TRADER, ioc(Side::Buy, 10_005, 4)),
    ]);
    assert_eq!(fills(&ev), vec![(1, 2, 10_001, 4)]);
    assert_eq!(e.best_ask(), Some((10_001, 6)));
    insta::assert_debug_snapshot!(ev);
}

#[test]
fn h03_sell_executes_at_resting_bid() {
    let (e, ev) = run(&[
        (LP, gtc(Side::Buy, 9_999, 10)),
        (TRADER, ioc(Side::Sell, 9_990, 10)),
    ]);
    assert_eq!(fills(&ev), vec![(1, 2, 9_999, 10)]);
    assert_eq!(e.best_bid(), None);
    insta::assert_debug_snapshot!(ev);
}

#[test]
fn h04_partial_fills_across_levels() {
    // Asks 5@100, 5@101, 5@102. Buy IOC 12 @102: 5@100, 5@101, 2@102; 3 left at 102.
    let (e, ev) = run(&[
        (LP, gtc(Side::Sell, 100, 5)),
        (LP, gtc(Side::Sell, 101, 5)),
        (LP, gtc(Side::Sell, 102, 5)),
        (TRADER, ioc(Side::Buy, 102, 12)),
    ]);
    assert_eq!(
        fills(&ev),
        vec![(1, 4, 100, 5), (2, 4, 101, 5), (3, 4, 102, 2)]
    );
    assert_eq!(e.depth(Side::Sell), vec![(102, 3)]);
    assert!(cancels(&ev).is_empty());
    insta::assert_debug_snapshot!(ev);
}

#[test]
fn h05_time_priority_inside_a_level() {
    // Two asks at 100: #1 qty 3 then #2 qty 4. Buy 5: 3 from #1, then 2 from #2.
    let (e, ev) = run(&[
        (LP, gtc(Side::Sell, 100, 3)),
        (SNIPER, gtc(Side::Sell, 100, 4)),
        (TRADER, ioc(Side::Buy, 100, 5)),
    ]);
    assert_eq!(fills(&ev), vec![(1, 3, 100, 3), (2, 3, 100, 2)]);
    assert_eq!(e.resting(2).unwrap().qty, 2);
    assert!(e.resting(1).is_none());
    insta::assert_debug_snapshot!(ev);
}

#[test]
fn h06_price_beats_time() {
    // Ask at 101 arrives first, ask at 100 second. Buy 1 @101 takes the better price.
    let (e, ev) = run(&[
        (LP, gtc(Side::Sell, 101, 1)),
        (SNIPER, gtc(Side::Sell, 100, 1)),
        (TRADER, ioc(Side::Buy, 101, 1)),
    ]);
    assert_eq!(fills(&ev), vec![(2, 3, 100, 1)]);
    assert_eq!(e.best_ask(), Some((101, 1)));
    insta::assert_debug_snapshot!(ev);
}

#[test]
fn h07_ioc_remainder_cancelled() {
    let (e, ev) = run(&[
        (LP, gtc(Side::Sell, 100, 5)),
        (TRADER, ioc(Side::Buy, 100, 8)),
    ]);
    assert_eq!(fills(&ev), vec![(1, 2, 100, 5)]);
    assert_eq!(cancels(&ev), vec![(2, 3, CancelReason::IocRemainder)]);
    assert_eq!(e.order_count(), 0);
    insta::assert_debug_snapshot!(ev);
}

#[test]
fn h08_gtc_remainder_rests() {
    let (e, ev) = run(&[
        (LP, gtc(Side::Sell, 100, 5)),
        (TRADER, gtc(Side::Buy, 100, 8)),
    ]);
    assert_eq!(fills(&ev), vec![(1, 2, 100, 5)]);
    assert!(cancels(&ev).is_empty());
    assert_eq!(e.best_bid(), Some((100, 3)));
    assert_eq!(e.best_ask(), None);
    insta::assert_debug_snapshot!(ev);
}

#[test]
fn h09_ioc_without_cross_is_cancelled_whole() {
    let (e, ev) = run(&[
        (LP, gtc(Side::Sell, 101, 5)),
        (TRADER, ioc(Side::Buy, 100, 5)),
    ]);
    assert!(fills(&ev).is_empty());
    assert_eq!(cancels(&ev), vec![(2, 5, CancelReason::IocRemainder)]);
    assert_eq!(e.best_ask(), Some((101, 5)));
    insta::assert_debug_snapshot!(ev);
}

#[test]
fn h10_limit_is_respected_and_gtc_rests_behind() {
    // Buy GTC @100 against asks at 101: no trade, rests as best bid at 100.
    let (e, ev) = run(&[
        (LP, gtc(Side::Sell, 101, 5)),
        (TRADER, gtc(Side::Buy, 100, 5)),
    ]);
    assert!(fills(&ev).is_empty());
    assert_eq!(e.best_bid(), Some((100, 5)));
    assert_eq!(e.best_ask(), Some((101, 5)));
    insta::assert_debug_snapshot!(ev);
}

#[test]
fn h11_cancel_removes_resting_order() {
    let (e, ev) = run(&[(LP, gtc(Side::Sell, 101, 5)), (LP, cancel(1))]);
    assert_eq!(cancels(&ev), vec![(1, 5, CancelReason::User)]);
    assert_eq!(e.best_ask(), None);
    insta::assert_debug_snapshot!(ev);
}

#[test]
fn h12_cancel_after_full_fill_is_reported_noop() {
    let (e, ev) = run(&[
        (LP, gtc(Side::Sell, 101, 5)),
        (TRADER, ioc(Side::Buy, 101, 5)),
        (LP, cancel(1)),
    ]);
    assert_eq!(fills(&ev), vec![(1, 2, 101, 5)]);
    assert_eq!(missed(&ev), vec![1]);
    assert!(cancels(&ev).is_empty());
    assert_eq!(e.order_count(), 0);
    insta::assert_debug_snapshot!(ev);
}

#[test]
fn h13_cancel_races_fill_cancel_first() {
    // The LP's cancel is sequenced before the sniper's IOC: the sniper finds nothing.
    let (e, ev) = run(&[
        (LP, gtc(Side::Sell, 10_002, 10)),
        (LP, cancel(1)),
        (SNIPER, ioc(Side::Buy, 10_003, 10)),
    ]);
    assert!(fills(&ev).is_empty());
    assert_eq!(
        cancels(&ev),
        vec![
            (1, 10, CancelReason::User),
            (3, 10, CancelReason::IocRemainder)
        ]
    );
    assert_eq!(e.order_count(), 0);
    insta::assert_debug_snapshot!(ev);
}

#[test]
fn h14_cancel_races_fill_fill_first() {
    // The mental-model race: the sniper's buy is sequenced first and takes 10 @ 100.02;
    // the LP's cancel arrives after and is a reported no-op.
    let (e, ev) = run(&[
        (LP, gtc(Side::Sell, 10_002, 10)),
        (SNIPER, ioc(Side::Buy, 10_003, 10)),
        (LP, cancel(1)),
    ]);
    assert_eq!(fills(&ev), vec![(1, 2, 10_002, 10)]);
    assert_eq!(missed(&ev), vec![1]);
    assert_eq!(e.order_count(), 0);
    insta::assert_debug_snapshot!(ev);
}

#[test]
fn h15_partial_fill_then_cancel_removes_remainder() {
    let (e, ev) = run(&[
        (LP, gtc(Side::Sell, 100, 10)),
        (TRADER, ioc(Side::Buy, 100, 4)),
        (LP, cancel(1)),
    ]);
    assert_eq!(fills(&ev), vec![(1, 2, 100, 4)]);
    assert_eq!(
        cancels(&ev),
        vec![(1, 6, CancelReason::User)],
        "only the unfilled 6 lots are cancelled"
    );
    assert_eq!(e.order_count(), 0);
    insta::assert_debug_snapshot!(ev);
}

#[test]
fn h16_self_trade_cancels_resting() {
    // LP's own buy would hit its own ask: the resting ask is cancelled and the buy rests.
    let (e, ev) = run(&[(LP, gtc(Side::Sell, 100, 5)), (LP, gtc(Side::Buy, 100, 3))]);
    assert!(fills(&ev).is_empty());
    assert_eq!(cancels(&ev), vec![(1, 5, CancelReason::SelfTrade)]);
    assert_eq!(e.best_bid(), Some((100, 3)));
    assert_eq!(e.best_ask(), None);
    insta::assert_debug_snapshot!(ev);
}

#[test]
fn h17_self_trade_skips_own_order_and_keeps_matching() {
    // Asks at 100: LP 2 lots (#1), sniper 2 lots (#2). LP buys IOC 3 @100:
    // #1 is cancelled (self-trade), 2 fill against #2, 1 IOC remainder cancelled.
    let (e, ev) = run(&[
        (LP, gtc(Side::Sell, 100, 2)),
        (SNIPER, gtc(Side::Sell, 100, 2)),
        (LP, ioc(Side::Buy, 100, 3)),
    ]);
    assert_eq!(fills(&ev), vec![(2, 3, 100, 2)]);
    assert_eq!(
        cancels(&ev),
        vec![
            (1, 2, CancelReason::SelfTrade),
            (3, 1, CancelReason::IocRemainder)
        ]
    );
    assert_eq!(e.order_count(), 0);
    insta::assert_debug_snapshot!(ev);
}

#[test]
fn h18_tick_is_echoed_and_ignored_by_matching() {
    let (e, ev) = run(&[
        (LP, gtc(Side::Sell, 10_001, 5)),
        (0, MsgKind::Tick { px_ticks: 10_050 }),
    ]);
    assert!(fills(&ev).is_empty());
    assert_eq!(ev.last().unwrap().kind, EventKind::Tick { px: 10_050 });
    assert_eq!(ev.last().unwrap().seq, 2);
    assert_eq!(e.best_ask(), Some((10_001, 5)));
    insta::assert_debug_snapshot!(ev);
}

#[test]
fn h19_cancel_by_non_owner_is_noop() {
    let (e, ev) = run(&[(LP, gtc(Side::Sell, 100, 5)), (SNIPER, cancel(1))]);
    assert_eq!(missed(&ev), vec![1]);
    assert_eq!(e.best_ask(), Some((100, 5)));
    insta::assert_debug_snapshot!(ev);
}

#[test]
fn h20_malformed_records_rejected() {
    let mut e = Engine::new();
    let rec = |seq, kind| LogRecord {
        seq,
        t_ns: 0,
        block: 0,
        agent: TRADER,
        kind,
    };
    let mut ev = e.on_message(&rec(1, gtc(Side::Buy, 100, 0)));
    ev.extend(e.on_message(&rec(2, gtc(Side::Buy, 0, 5))));
    ev.extend(e.on_message(&rec(3, gtc(Side::Buy, 100, 5))));
    ev.extend(e.on_message(&rec(3, gtc(Side::Buy, 100, 5)))); // seq repeated
    assert_eq!(e.order_count(), 1);
    let rejects: Vec<_> = ev
        .iter()
        .filter_map(|e| match e.kind {
            EventKind::Rejected { reason, .. } => Some(reason),
            _ => None,
        })
        .collect();
    assert_eq!(
        rejects,
        vec![
            RejectReason::ZeroQty,
            RejectReason::NonPositivePrice,
            RejectReason::SeqNotIncreasing
        ]
    );
    insta::assert_debug_snapshot!(ev);
}

#[test]
fn h21_sell_sweeps_bids_and_rests_remainder() {
    // Bids 5@99, 5@98. Sell GTC 12 @97: 5@99, 5@98, then 2 rest as the new best ask at 97.
    let (e, ev) = run(&[
        (LP, gtc(Side::Buy, 99, 5)),
        (LP, gtc(Side::Buy, 98, 5)),
        (TRADER, gtc(Side::Sell, 97, 12)),
    ]);
    assert_eq!(fills(&ev), vec![(1, 3, 99, 5), (2, 3, 98, 5)]);
    assert_eq!(e.best_bid(), None);
    assert_eq!(e.best_ask(), Some((97, 2)));
    insta::assert_debug_snapshot!(ev);
}

#[test]
fn h22_lp_two_level_quote_hit_by_stale_snipe() {
    // LP quotes the spec's default ladder around 100.00: 10 @ ±1, 5 @ ±2.
    // Price jumps to 100.05; a sniper buys IOC with limit 100.04 (seen - theta) and takes both
    // asks: 10 @ 100.01 and 5 @ 100.02. Bids are untouched.
    let (e, ev) = run(&[
        (LP, gtc(Side::Buy, 9_999, 10)),
        (LP, gtc(Side::Buy, 9_998, 5)),
        (LP, gtc(Side::Sell, 10_001, 10)),
        (LP, gtc(Side::Sell, 10_002, 5)),
        (0, MsgKind::Tick { px_ticks: 10_005 }),
        (SNIPER, ioc(Side::Buy, 10_004, 15)),
    ]);
    assert_eq!(fills(&ev), vec![(3, 6, 10_001, 10), (4, 6, 10_002, 5)]);
    assert_eq!(e.best_ask(), None);
    assert_eq!(e.depth(Side::Buy), vec![(9_999, 10), (9_998, 5)]);
    insta::assert_debug_snapshot!(ev);
}
