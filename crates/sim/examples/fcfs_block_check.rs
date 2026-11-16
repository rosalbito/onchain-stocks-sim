//! Section 13 check: under first come, first served, the block length B should change only fill
//! delay, not race outcomes. Runs calm_weekday under FCFS at B ∈ {0, 50, 250, 1000} ms over
//! seeds 1..=N and prints race outcomes and sniper take per B.
//!
//! `cargo run --release -p sim --example fcfs_block_check -- [seeds]`

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
        "{:>6} {:>8} {:>8} {:>8} {:>8} {:>8} {:>14} {:>14}",
        "B ms", "races", "LP won", "sniper", "other", "LP rate", "take@fill $", "sniper PnL $"
    );
    for block_ms in [0u64, 50, 250, 1_000] {
        let mut s = base.clone();
        s.sequencer.rule = OrderingRule::Fcfs;
        s.sequencer.block_ms = block_ms;
        let (mut lp, mut sn, mut ot, mut take, mut pnl) = (0u64, 0u64, 0u64, 0i128, 0i128);
        for seed in 1..=seeds {
            let r = sim::run(&s, seed).unwrap();
            lp += r.races.lp_won;
            sn += r.races.sniper_won;
            ot += r.races.other_filled;
            take += r.take_at_fill[&AccountClass::Sniper];
            pnl += r.pnl.get(AccountClass::Sniper);
        }
        let n = seeds as f64;
        println!(
            "{:>6} {:>8.1} {:>8.1} {:>8.1} {:>8.1} {:>8.3} {:>14.2} {:>14.2}",
            block_ms,
            (lp + sn + ot) as f64 / n,
            lp as f64 / n,
            sn as f64 / n,
            ot as f64 / n,
            lp as f64 / (lp + sn + ot) as f64,
            take as f64 / n / 100.0,
            pnl as f64 / n / 1e6,
        );
    }
    println!(
        "(per-seed means over {seeds} seeds; take@fill = sniper gain vs v at fill, before fees)"
    );
}
