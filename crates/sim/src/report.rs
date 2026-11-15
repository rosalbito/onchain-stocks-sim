//! The short text summary of a run. Floats appear only here, for display.

use std::fmt::Write;

use metrics::{format_cash, AccountClass};

use crate::sim::RunResult;

fn px(ticks: i64) -> String {
    format!("{}.{:02}", ticks / 100, ticks % 100)
}

pub fn summary(r: &RunResult) -> String {
    let s = &r.scenario;
    let c = &r.counts;
    let mut o = String::new();
    let _ = writeln!(
        o,
        "scenario {}  seed {}  rule {}  block {} ms  duration {} s",
        s.name,
        r.seed,
        s.sequencer.rule.name(),
        s.sequencer.block_ms,
        s.duration_s
    );
    let _ = writeln!(
        o,
        "agents: {} LP, {} snipers, {} regular traders",
        s.lp.count, s.sniper.count, s.regular.count
    );
    let _ = writeln!(
        o,
        "log: {} records ({} ticks, {} new, {} cancels), {} blocks, {} bytes",
        c.records,
        c.ticks,
        c.news,
        c.cancels,
        c.blocks,
        r.log_bytes.len()
    );
    let _ = writeln!(o, "log sha256:   {}", r.log_hash);
    let _ = writeln!(o, "fills sha256: {}", r.fills_hash);
    let _ = writeln!(
        o,
        "fills: {} ({} lots, notional {}), cancels missed {}, unsequenced at end {}",
        c.fills,
        c.volume_lots,
        format_cash(c.notional_tick_lots * metrics::CASH_PER_TICK_LOT),
        c.cancel_missed,
        c.unsequenced
    );
    let rs = &r.races;
    let (won, of) = rs.lp_win_rate();
    let rate = if of > 0 {
        format!("{:.3}", won as f64 / of as f64)
    } else {
        "n/a".into()
    };
    let _ = writeln!(
        o,
        "races: {} resolved (LP {}, sniper {}, other {}), {} unresolved, {} episodes",
        rs.resolved(),
        rs.lp_won,
        rs.sniper_won,
        rs.other_filled,
        rs.unresolved,
        rs.episodes
    );
    let _ = writeln!(o, "LP race win rate: {rate} ({won}/{of})");
    if let Some((before, after)) = r.reopen {
        let _ = writeln!(
            o,
            "reopen: last visible {} -> reopen {} ({:+} ticks)",
            px(before),
            px(after),
            after - before
        );
    }
    let _ = writeln!(
        o,
        "PnL by account class, marked to the final Nasdaq price {}:",
        px(r.final_px)
    );
    let mut total = 0i128;
    for class in AccountClass::ALL {
        let v = r.pnl.get(class);
        total += v;
        let _ = writeln!(o, "  {:<16} {:>14}", class.label(), format_cash(v));
    }
    let _ = writeln!(
        o,
        "  {:<16} {:>14}  (zero-sum check passed)",
        "total",
        format_cash(total)
    );
    let take = |c: AccountClass| {
        format_cash(r.take_at_fill.get(&c).copied().unwrap_or(0) * metrics::CASH_PER_TICK_LOT)
    };
    let _ = writeln!(
        o,
        "gain on fills vs the true price at fill time, before fees: snipers {}, LPs {}, regular {}",
        take(AccountClass::Sniper),
        take(AccountClass::Lp),
        take(AccountClass::Regular)
    );
    let _ = writeln!(
        o,
        "fees: taker {} to protocol, priority {} to chain",
        format_cash(r.taker_fees),
        format_cash(r.priority_fees)
    );
    o
}
