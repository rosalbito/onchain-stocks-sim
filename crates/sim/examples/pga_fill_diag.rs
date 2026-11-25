//! Why snipers fill more lots under PGA than under FCFS (docs/DECISIONS.md, entry 81).
//!
//! Runs calm_weekday at B = 250 ms, speed gap 0.5, 3 snipers, under FCFS and PGA, and splits the
//! snipers' filled lots by: the gain per lot against v at the fill, whether the LP's cancel for
//! the quote they hit was in the same block (sequenced after the sniper's order), and whether
//! the quote entered the book already stale.
//!
//! `cargo run --release -p sim --example pga_fill_diag -- [seeds] [alpha_bps]`

use std::collections::{BTreeMap, BTreeSet};

use engine::{Engine, EventKind, MsgKind, Side};
use metrics::AccountClass;
use sim::{OrderingRule, Scenario};

fn main() {
    let mut args = std::env::args().skip(1);
    let seeds: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(10);
    let alpha: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(1_000);
    let path = format!(
        "{}/../../scenarios/calm_weekday.toml",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut base = Scenario::from_toml(&std::fs::read_to_string(path).unwrap()).unwrap();
    base.sequencer.block_ms = 250;
    base.set_speed_gap(1, 2);
    base.sniper.alpha_bps = alpha;
    println!(
        "{:>5} {:>8} {:>8} {:>9} {:>9} {:>9} {:>12} {:>12} {:>11}",
        "rule",
        "IOCs",
        "lots",
        "gain 1",
        "gain 2-3",
        "gain 4+",
        "cancel same",
        "born stale",
        "gain $"
    );
    for rule in [OrderingRule::Fcfs, OrderingRule::Pga] {
        let mut s = base.clone();
        s.sequencer.rule = rule;
        // iocs, lots, gain==1, gain 2..=3, gain>=4, cancel in same block, born stale, gain
        let mut acc = [0i128; 8];
        for seed in 1..=seeds {
            let r = sim::run(&s, seed).unwrap();
            let is_sniper = |a: u32| r.agents.get(&a) == Some(&AccountClass::Sniper);
            // (block, order id) of every cancel.
            let cancels: BTreeSet<(u64, u64)> = r
                .records
                .iter()
                .filter_map(|rec| match rec.kind {
                    MsgKind::Cancel { order_id, .. } => Some((rec.block, order_id)),
                    _ => None,
                })
                .collect();
            let mut e = Engine::new();
            let mut born: BTreeMap<u64, i64> = BTreeMap::new();
            for rec in &r.records {
                // v when the record is processed: ticks at their time, messages at block close.
                let v = r.path.at(metrics::measure::processed_ns(rec, 250_000_000));
                if let MsgKind::New { side, px_ticks, .. } = rec.kind {
                    let st = match side {
                        Side::Sell => v - px_ticks,
                        Side::Buy => px_ticks - v,
                    };
                    born.insert(rec.seq, st);
                    if is_sniper(rec.agent) {
                        acc[0] += 1;
                    }
                }
                for ev in e.on_message(rec) {
                    if let EventKind::Fill {
                        maker_order,
                        taker_agent,
                        taker_side,
                        px,
                        qty,
                        ..
                    } = ev.kind
                    {
                        if !is_sniper(taker_agent) {
                            continue;
                        }
                        let gain = match taker_side {
                            Side::Buy => v - px,
                            Side::Sell => px - v,
                        };
                        let q = qty as i128;
                        acc[1] += q;
                        match gain {
                            ..=1 => acc[2] += q,
                            2..=3 => acc[3] += q,
                            _ => acc[4] += q,
                        }
                        if cancels.contains(&(rec.block, maker_order)) {
                            acc[5] += q;
                        }
                        if born[&maker_order] >= 1 {
                            acc[6] += q;
                        }
                        acc[7] += gain as i128 * q;
                    }
                }
            }
        }
        let f = |i: usize| acc[i] as f64 / seeds as f64;
        println!(
            "{:>5} {:>8.1} {:>8.1} {:>9.1} {:>9.1} {:>9.1} {:>12.1} {:>12.1} {:>11.2}",
            rule.name(),
            f(0),
            f(1),
            f(2),
            f(3),
            f(4),
            f(5),
            f(6),
            f(7) / 100.0
        );
    }
    println!(
        "(per-seed means over {seeds} seeds, alpha = {alpha} bps; lots are sniper-filled lots; \
         gain is per lot against v at the block close, in ticks)"
    );
}
