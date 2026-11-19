//! The engine alone: cost per message (throughput and per-message latency), cost per block, and
//! allocations per message, on the load profiles.

use std::hint::black_box;
use std::time::{Duration, Instant};

use engine::{Engine, LogRecord, MsgKind, Tif};
use hdrhistogram::Histogram;
use sim::OrderingRule;

use crate::alloc;
use crate::profiles::{self, Profile};

/// Replay a log through a fresh engine once.
fn replay(records: &[LogRecord]) {
    let mut e = Engine::new();
    for r in records {
        black_box(e.on_message(black_box(r)));
    }
    black_box(&e);
}

/// Sustained cost: ns per message, the median over repeated full replays.
pub fn ns_per_msg(records: &[LogRecord], budget: Duration) -> f64 {
    replay(records); // warm-up
    let mut times = Vec::new();
    let start = Instant::now();
    while start.elapsed() < budget || times.len() < 5 {
        let t = Instant::now();
        replay(records);
        times.push(t.elapsed().as_nanos() as f64 / records.len() as f64);
    }
    median(&mut times)
}

pub fn median(v: &mut [f64]) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

/// Per-message latency, message in -> events out, one clock read on each side. On Apple
/// silicon the clock ticks every 41.7 ns, so values below ~100 ns are coarse.
pub fn per_msg_hist(records: &[LogRecord], reps: usize) -> Histogram<u64> {
    let mut h = Histogram::<u64>::new_with_bounds(1, 10_000_000, 3).unwrap();
    replay(records);
    for _ in 0..reps {
        let mut e = Engine::new();
        for r in records {
            let t = Instant::now();
            black_box(e.on_message(black_box(r)));
            h.saturating_record(t.elapsed().as_nanos() as u64);
        }
    }
    h
}

/// Allocations while replaying a log, split by message kind.
#[derive(Debug, Default, Clone, Copy)]
pub struct Allocs {
    pub msgs: [u64; 4],
    pub allocs: [u64; 4],
    pub reallocs: [u64; 4],
}

pub const KINDS: [&str; 4] = ["new GTC", "new IOC", "cancel", "tick"];

fn kind_idx(k: &MsgKind) -> usize {
    match k {
        MsgKind::New { tif: Tif::Gtc, .. } => 0,
        MsgKind::New { tif: Tif::Ioc, .. } => 1,
        MsgKind::Cancel { .. } => 2,
        MsgKind::Tick { .. } | MsgKind::Config { .. } => 3,
    }
}

impl Allocs {
    pub fn total_msgs(&self) -> u64 {
        self.msgs.iter().sum()
    }
    pub fn allocs_per_msg(&self) -> f64 {
        self.allocs.iter().sum::<u64>() as f64 / self.total_msgs() as f64
    }
    pub fn reallocs_per_msg(&self) -> f64 {
        self.reallocs.iter().sum::<u64>() as f64 / self.total_msgs() as f64
    }
    pub fn per_msg(&self) -> f64 {
        (self.allocs.iter().sum::<u64>() + self.reallocs.iter().sum::<u64>()) as f64
            / self.total_msgs() as f64
    }
    pub fn per_kind(&self, k: usize) -> f64 {
        if self.msgs[k] == 0 {
            return 0.0;
        }
        (self.allocs[k] + self.reallocs[k]) as f64 / self.msgs[k] as f64
    }
}

pub fn allocs(records: &[LogRecord]) -> Allocs {
    let mut a = Allocs::default();
    let mut e = Engine::new();
    for r in records {
        let k = kind_idx(&r.kind);
        let before = alloc::snapshot();
        let out = e.on_message(r);
        drop(black_box(out));
        let d = alloc::snapshot() - before;
        a.msgs[k] += 1;
        a.allocs[k] += d.allocs;
        a.reallocs[k] += d.reallocs;
    }
    a
}

/// Block processing: sort one sealed block by the rule and match it, on a deep book.
/// Returns (median µs per block, median ns per message) over `reps` fresh blocks.
pub fn block_cost(n: usize, rule: OrderingRule, reps: usize) -> (f64, f64) {
    let mut per_block = Vec::new();
    for rep in 0..reps {
        let b = profiles::burst(200, n, rule, 100 + rep as u64);
        // The sort is part of the block's cost: re-sort an unsorted copy inside the timing.
        let mut pending: Vec<sim::sequencer::Pending> = b
            .block
            .iter()
            .rev()
            .enumerate()
            .map(|(i, r)| sim::sequencer::Pending {
                t_ns: r.t_ns,
                tiebreak: i as u64,
                decided_ns: 0,
                agent: r.agent,
                kind: r.kind,
            })
            .collect();
        let mut e = profiles::engine_after(&b.warm);
        let first_seq = b.warm.len() as u64 + 1;
        let t = Instant::now();
        sim::sequencer::sort_block(rule, &mut pending);
        for (i, p) in pending.iter().enumerate() {
            let rec = LogRecord {
                seq: first_seq + i as u64,
                t_ns: p.t_ns,
                block: 1,
                agent: p.agent,
                kind: p.kind,
            };
            black_box(e.on_message(&rec));
        }
        per_block.push(t.elapsed().as_nanos() as f64);
    }
    let med = median(&mut per_block);
    (med / 1_000.0, med / n as f64)
}

pub struct ProfileRow {
    pub name: &'static str,
    pub msgs: usize,
    pub ns_per_msg: f64,
    pub p50: u64,
    pub p99: u64,
    pub p999: u64,
}

pub fn profile_row(p: &Profile, budget: Duration) -> ProfileRow {
    let ns = ns_per_msg(&p.records, budget);
    let reps = (200_000 / p.records.len()).clamp(1, 50);
    let h = per_msg_hist(&p.records, reps);
    ProfileRow {
        name: p.name,
        msgs: p.records.len(),
        ns_per_msg: ns,
        p50: h.value_at_quantile(0.5),
        p99: h.value_at_quantile(0.99),
        p999: h.value_at_quantile(0.999),
    }
}
