//! Why sniper take still grows with B under FCFS (docs/DECISIONS.md, entry 42).
//!
//! Replays calm_weekday under FCFS at each B and splits the snipers' filled lots by whether the
//! LP quote they hit was already stale (by ≥ 1 tick against the Nasdaq price) when it entered
//! the book, and whether it entered the book in the same block as the sniper's order.
//!
//! `cargo run --release -p sim --example fcfs_block_diag -- [seeds]`

use std::collections::BTreeMap;

use engine::{Engine, EventKind, MsgKind, Side};
use metrics::AccountClass;
use sim::{OrderingRule, Scenario};

fn main() {
    let seeds: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let path = format!(
        "{}/../../scenarios/calm_weekday.toml",
        env!("CARGO_MANIFEST_DIR")
    );
    let base = Scenario::from_toml(&std::fs::read_to_string(path).unwrap()).unwrap();
    println!(
        "{:>6} {:>10} {:>10} {:>10} {:>12} {:>14} {:>16} {:>12}",
        "B ms", "IOCs sent", "fills", "lots", "take $", "born stale", "same block", "stale by 1"
    );
    for block_ms in [0u64, 50, 250, 1_000] {
        let mut s = base.clone();
        s.sequencer.rule = OrderingRule::Fcfs;
        s.sequencer.block_ms = block_ms;
        // iocs, fills, lots, take, born-stale lots, same-block lots, 1-tick-stale lots
        let mut acc = [0i128; 7];
        for seed in 1..=seeds {
            let r = sim::run(&s, seed).unwrap();
            let is_sniper = |a: u32| r.agents.get(&a) == Some(&AccountClass::Sniper);
            let mut e = Engine::new();
            let mut v = 0i64;
            // order id -> (staleness when it entered the book, block)
            let mut born: BTreeMap<u64, (i64, u64)> = BTreeMap::new();
            for rec in &r.records {
                match rec.kind {
                    MsgKind::Tick { px_ticks } => v = px_ticks,
                    MsgKind::New { side, px_ticks, .. } => {
                        let st = match side {
                            Side::Sell => v - px_ticks,
                            Side::Buy => px_ticks - v,
                        };
                        born.insert(rec.seq, (st, rec.block));
                        if is_sniper(rec.agent) {
                            acc[0] += 1;
                        }
                    }
                    MsgKind::Cancel { .. } | MsgKind::Config { .. } => {}
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
                        acc[1] += 1;
                        acc[2] += q;
                        acc[3] += gain as i128 * q;
                        let (st, blk) = born[&maker_order];
                        if st >= 1 {
                            acc[4] += q;
                        }
                        if blk == rec.block && block_ms > 0 {
                            acc[5] += q;
                        }
                        if gain == 1 {
                            acc[6] += q;
                        }
                    }
                }
            }
        }
        let f = |i: usize| acc[i] as f64 / seeds as f64;
        println!(
            "{block_ms:>6} {:>10.1} {:>10.1} {:>10.1} {:>12.2} {:>14.1} {:>16.1} {:>12.1}",
            f(0),
            f(1),
            f(2),
            f(3) / 100.0,
            f(4),
            f(5),
            f(6)
        );
    }
    println!(
        "(per-seed means over {seeds} seeds; lots columns are sniper-filled lots; take is the \
         snipers' gain vs v at fill, before fees)"
    );
}
