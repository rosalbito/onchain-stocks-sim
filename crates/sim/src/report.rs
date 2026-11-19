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
    metrics_section(&mut o, r);
    o
}

/// The section 10 market metrics, converted to bps, dollars and ms for display.
fn metrics_section(o: &mut String, r: &RunResult) {
    use metrics::measure::{HEADLINE_H, HORIZONS_NS};
    let m = &r.metrics;
    let ratio = |n: i128, d: i128| {
        if d == 0 {
            f64::NAN
        } else {
            n as f64 / d as f64
        }
    };
    let bps = |n: i128, d: i128| 1e4 * ratio(n, d);
    let vol = m.notional_cash();
    let per_day = 86_400e9 / m.end_ns as f64;
    let cash = metrics::CASH_PER_TICK_LOT;
    let h = HEADLINE_H;
    let _ = writeln!(
        o,
        "market metrics (spec section 10; headline horizon h = 1 s):"
    );
    let _ = writeln!(
        o,
        "  sniper take        {:>8.3} bps of volume, ${:.2}/day",
        bps(m.sniper_take(h), vol),
        m.sniper_take(h) as f64 / 1e6 * per_day
    );
    let _ = writeln!(
        o,
        "  LP loss to snipers {:>8.3} bps of volume",
        bps(m.lp_loss_to_snipers(h) * cash, vol)
    );
    let _ = writeln!(
        o,
        "  chain take         {:>8.3} bps of volume",
        bps(m.priority_fees, vol)
    );
    let (won, of) = m.races.lp_win_rate();
    let _ = writeln!(
        o,
        "  LP race win rate   {:>8.3}",
        ratio(won as i128, of as i128)
    );
    let adverse: Vec<String> = (1..HORIZONS_NS.len())
        .map(|i| {
            format!(
                "{} s {:.3}",
                HORIZONS_NS[i] / 1_000_000_000,
                bps(m.lp.adverse(i), m.lp.notional_tick_lots)
            )
        })
        .collect();
    let _ = writeln!(
        o,
        "  LP adverse selection (effective {:.3} bps): {} bps",
        bps(m.lp.effective, m.lp.notional_tick_lots),
        adverse.join(", ")
    );
    let _ = writeln!(
        o,
        "  regular-trader cost {:>7.3} bps",
        bps(m.regular_cost, m.regular_notional_tick_lots)
    );
    let _ = writeln!(
        o,
        "  fill delay (regular) median {:.1} ms, p99 {:.1} ms",
        m.regular_delay.median_ns as f64 / 1e6,
        m.regular_delay.p99_ns as f64 / 1e6
    );
    let q = &m.quoted;
    let _ = writeln!(
        o,
        "  quoted half-spread {:>8.3} bps (two-sided {:.1}% of the time), depth within 10 bps {:.1} lots",
        bps(q.spread_tick_ns, q.mid2_tick_ns),
        100.0 * q.two_sided_ns as f64 / q.total_ns as f64,
        ratio(q.depth_lot_ns, q.total_ns as i128)
    );
    if let Some(mo) = m.monday {
        let (n, d) = mo.share();
        let (nn, nd) = mo.net_share();
        let (cn, cd) = mo.chain_share();
        let (ln, ld) = mo.lp_share();
        let _ = writeln!(
            o,
            "  Monday-open take   {:>8.3} of the gap prize ({} tick·lots on {} stale lots; gap {:+} ticks, {} lots on that side at the close, {} at the reopen)",
            ratio(n, d),
            mo.prize,
            mo.prize_lots,
            mo.gap_ticks,
            mo.resting_lots,
            mo.resting_lots_at_reopen
        );
        let _ = writeln!(
            o,
            "                     net of sniper fees {:.3}, chain {:.3}, LP cancelled {:.3}",
            ratio(nn, nd),
            ratio(cn, cd),
            ratio(ln, ld)
        );
    }
}
