//! The section 13 experiment grid: ordering rule × B × speed gap × snipers × regime = 216 cells.
//!
//! Every run is single-threaded and deterministic; rayon spreads runs across cores. All rules
//! run on the same seeds (common random numbers), so rules are compared by paired differences.
//! This binary is a report: it converts the integer metrics to bps, dollars and ms, and computes
//! bootstrap intervals with floats. The wall clock is read only to report the grid's runtime.
//!
//! Output (parquet): `grid_runs.parquet` (one row per cell and seed), `grid_summary.parquet`
//! (mean and 95% bootstrap CI per cell and metric) and `grid_pairs.parquet` (paired differences
//! between rules on the same seeds).
//!
//! `cargo run --release -p grid -- --seeds 20 --out analysis/data`

mod table;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

use clap::Parser;
use metrics::measure::{HEADLINE_H, HORIZONS_NS};
use metrics::{AccountClass, Metrics, CASH_PER_TICK_LOT};
use rand_chacha::ChaCha8Rng;
use rand_core::{RngCore, SeedableRng};
use rayon::prelude::*;
use sim::{OrderingRule, RunOpts, Scenario};

use table::{Col, Table};

#[derive(Parser)]
#[command(about = "Run the section 13 experiment grid and write parquet results")]
struct Args {
    /// Seeds per cell (1..=N). The same seeds run under every ordering rule.
    #[arg(long, default_value_t = 20)]
    seeds: u64,
    /// Directory holding calm_weekday.toml and monday_open.toml.
    #[arg(long, default_value = "scenarios")]
    scenarios: PathBuf,
    /// Output directory for the parquet files.
    #[arg(long, default_value = "analysis/data")]
    out: PathBuf,
    /// The Monday-open window W, in seconds.
    #[arg(long, default_value_t = 300)]
    monday_window_s: u64,
    /// Bootstrap resamples per interval.
    #[arg(long, default_value_t = 2_000)]
    resamples: usize,
}

const BLOCKS_MS: [u64; 4] = [0, 50, 250, 1_000];
/// Sniper latency ÷ LP latency, as num/den.
const GAPS: [(u64, u64); 3] = [(1, 10), (1, 2), (1, 1)];
const SNIPERS: [u32; 3] = [1, 3, 10];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Regime {
    Weekday,
    Monday,
}

impl Regime {
    fn name(self) -> &'static str {
        match self {
            Regime::Weekday => "weekday",
            Regime::Monday => "monday_open",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Cell {
    regime: Regime,
    block_ms: u64,
    gap: (u64, u64),
    snipers: u32,
    rule: usize,
}

impl Cell {
    fn rule(&self) -> OrderingRule {
        OrderingRule::ALL[self.rule]
    }

    fn gap_f64(&self) -> f64 {
        self.gap.0 as f64 / self.gap.1 as f64
    }

    /// The cell without its rule: the group whose rules are compared on paired seeds.
    fn group(&self) -> (Regime, u64, (u64, u64), u32) {
        (self.regime, self.block_ms, self.gap, self.snipers)
    }
}

fn cells() -> Vec<Cell> {
    let mut v = Vec::new();
    for regime in [Regime::Weekday, Regime::Monday] {
        for block_ms in BLOCKS_MS {
            for gap in GAPS {
                for snipers in SNIPERS {
                    for rule in 0..3 {
                        v.push(Cell {
                            regime,
                            block_ms,
                            gap,
                            snipers,
                            rule,
                        });
                    }
                }
            }
        }
    }
    v
}

/// The per-run metrics, converted for the report. NaN where a metric is undefined (no races, no
/// Monday open, ...).
#[derive(Debug, Clone)]
struct Row {
    cell: Cell,
    seed: u64,
    fills: u64,
    volume_lots: u64,
    notional_usd: f64,
    /// Main chart value: weekday = LP loss to snipers over the run; Monday open = over the
    /// window W after the reopen, each in bps of the volume traded over the same span.
    lp_loss_bps: f64,
    /// LP loss to snipers over the whole run, bps of volume, per horizon.
    lp_loss_run_bps: [f64; 4],
    lp_loss_usd: f64,
    sniper_take_bps: f64,
    sniper_take_usd_day: f64,
    chain_take_bps: f64,
    lp_pnl_bps: f64,
    races: u64,
    races_lp: u64,
    races_sniper: u64,
    lp_race_win_rate: f64,
    lp_effective_bps: f64,
    lp_adverse_bps: [f64; 3],
    regular_cost_bps: f64,
    fill_delay_median_ms: f64,
    fill_delay_p99_ms: f64,
    half_spread_bps: f64,
    two_sided_share: f64,
    depth_lots: f64,
    monday_take_share: f64,
    monday_gap_ticks: f64,
    monday_lp_loss_usd: f64,
    monday_window_volume_usd: f64,
    unsequenced: u64,
}

fn ratio(n: i128, d: i128) -> f64 {
    if d == 0 {
        f64::NAN
    } else {
        n as f64 / d as f64
    }
}

/// Cash units to dollars ($0.01 tick, 1-share lots: 1 unit = $0.000001).
fn usd(cash: i128) -> f64 {
    cash as f64 / 1e6
}

fn row(cell: Cell, seed: u64, r: &sim::RunResult) -> Row {
    let m: &Metrics = &r.metrics;
    let h = HEADLINE_H;
    let vol = m.notional_cash();
    let bps = |cash: i128, den: i128| 1e4 * ratio(cash, den);
    let per_day = 86_400e9 / m.end_ns as f64;
    let (won, of) = m.races.lp_win_rate();
    let mo = m.monday;
    let lp_loss_run = |h: usize| bps(m.lp_loss_to_snipers(h) * CASH_PER_TICK_LOT, vol);
    let main = match (cell.regime, mo) {
        (Regime::Weekday, _) => lp_loss_run(h),
        (Regime::Monday, Some(mo)) => bps(
            mo.lp_loss_to_snipers * CASH_PER_TICK_LOT,
            mo.notional_tick_lots * CASH_PER_TICK_LOT,
        ),
        (Regime::Monday, None) => f64::NAN,
    };
    Row {
        cell,
        seed,
        fills: m.fills,
        volume_lots: m.volume_lots,
        notional_usd: usd(vol),
        lp_loss_bps: main,
        lp_loss_run_bps: [0, 1, 2, 3].map(lp_loss_run),
        lp_loss_usd: usd(m.lp_loss_to_snipers(h) * CASH_PER_TICK_LOT),
        sniper_take_bps: bps(m.sniper_take(h), vol),
        sniper_take_usd_day: usd(m.sniper_take(h)) * per_day,
        chain_take_bps: bps(m.priority_fees, vol),
        lp_pnl_bps: bps(m.pnl(h, AccountClass::Lp), vol),
        races: m.races.resolved(),
        races_lp: m.races.lp_won,
        races_sniper: m.races.sniper_won,
        lp_race_win_rate: ratio(won as i128, of as i128),
        lp_effective_bps: 1e4 * ratio(m.lp.effective, m.lp.notional_tick_lots),
        lp_adverse_bps: [1, 2, 3].map(|i| 1e4 * ratio(m.lp.adverse(i), m.lp.notional_tick_lots)),
        regular_cost_bps: 1e4 * ratio(m.regular_cost, m.regular_notional_tick_lots),
        fill_delay_median_ms: m.regular_delay.median_ns as f64 / 1e6,
        fill_delay_p99_ms: m.regular_delay.p99_ns as f64 / 1e6,
        half_spread_bps: 1e4 * ratio(m.quoted.spread_tick_ns, m.quoted.mid2_tick_ns),
        two_sided_share: ratio(m.quoted.two_sided_ns as i128, m.quoted.total_ns as i128),
        depth_lots: ratio(m.quoted.depth_lot_ns, m.quoted.total_ns as i128),
        monday_take_share: mo.map_or(f64::NAN, |mo| {
            let (n, d) = mo.share();
            ratio(n, d)
        }),
        monday_gap_ticks: mo.map_or(f64::NAN, |mo| mo.gap_ticks as f64),
        monday_lp_loss_usd: mo.map_or(
            f64::NAN,
            |mo| usd(mo.lp_loss_to_snipers * CASH_PER_TICK_LOT),
        ),
        monday_window_volume_usd: mo.map_or(f64::NAN, |mo| {
            usd(mo.notional_tick_lots * CASH_PER_TICK_LOT)
        }),
        unsequenced: r.counts.unsequenced,
    }
}

/// The f64 metrics summarised per cell, by name.
fn metric_values(r: &Row) -> Vec<(&'static str, f64)> {
    vec![
        ("lp_loss_bps", r.lp_loss_bps),
        ("lp_loss_run_bps", r.lp_loss_run_bps[HEADLINE_H]),
        ("lp_loss_usd", r.lp_loss_usd),
        ("sniper_take_bps", r.sniper_take_bps),
        ("sniper_take_usd_day", r.sniper_take_usd_day),
        ("chain_take_bps", r.chain_take_bps),
        ("lp_pnl_bps", r.lp_pnl_bps),
        ("lp_race_win_rate", r.lp_race_win_rate),
        ("lp_adverse_1s_bps", r.lp_adverse_bps[0]),
        ("lp_adverse_10s_bps", r.lp_adverse_bps[1]),
        ("lp_adverse_60s_bps", r.lp_adverse_bps[2]),
        ("regular_cost_bps", r.regular_cost_bps),
        ("fill_delay_median_ms", r.fill_delay_median_ms),
        ("fill_delay_p99_ms", r.fill_delay_p99_ms),
        ("half_spread_bps", r.half_spread_bps),
        ("depth_lots", r.depth_lots),
        ("monday_take_share", r.monday_take_share),
    ]
}

fn load(dir: &std::path::Path, name: &str) -> Result<Scenario, String> {
    let p = dir.join(format!("{name}.toml"));
    let text = std::fs::read_to_string(&p).map_err(|e| format!("{}: {e}", p.display()))?;
    Scenario::from_toml(&text)
}

fn scenario_for(base: &BTreeMap<Regime, Scenario>, c: &Cell) -> Scenario {
    let mut s = base[&c.regime].clone();
    s.sequencer.rule = c.rule();
    s.sequencer.block_ms = c.block_ms;
    s.sniper.count = c.snipers;
    s.set_speed_gap(c.gap.0, c.gap.1);
    s
}

// ------------------------------------------------------------------------------------------
// Statistics (report layer)
// ------------------------------------------------------------------------------------------

fn finite(xs: &[f64]) -> Vec<f64> {
    xs.iter().copied().filter(|x| x.is_finite()).collect()
}

fn mean(xs: &[f64]) -> f64 {
    xs.iter().sum::<f64>() / xs.len() as f64
}

fn sd(xs: &[f64]) -> f64 {
    if xs.len() < 2 {
        return f64::NAN;
    }
    let m = mean(xs);
    (xs.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (xs.len() - 1) as f64).sqrt()
}

/// Percentile bootstrap 95% interval of the mean. Deterministic: the resampling stream is seeded
/// by `key`.
fn boot_ci(xs: &[f64], resamples: usize, key: u64) -> (f64, f64) {
    let n = xs.len();
    if n == 0 {
        return (f64::NAN, f64::NAN);
    }
    let mut rng = ChaCha8Rng::seed_from_u64(key);
    let mut means: Vec<f64> = (0..resamples)
        .map(|_| {
            let mut s = 0.0;
            for _ in 0..n {
                s += xs[(rng.next_u64() % n as u64) as usize];
            }
            s / n as f64
        })
        .collect();
    means.sort_by(|a, b| a.total_cmp(b));
    let q = |p: f64| means[((p * (resamples - 1) as f64).round() as usize).min(resamples - 1)];
    (q(0.025), q(0.975))
}

// ------------------------------------------------------------------------------------------

fn main() -> ExitCode {
    match run(Args::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: Args) -> Result<(), String> {
    let base: BTreeMap<Regime, Scenario> = [
        (Regime::Weekday, load(&args.scenarios, "calm_weekday")?),
        (Regime::Monday, load(&args.scenarios, "monday_open")?),
    ]
    .into();
    let opts = RunOpts {
        monday_window_ns: args.monday_window_s * 1_000_000_000,
    };
    let cells = cells();
    let jobs: Vec<(Cell, u64)> = cells
        .iter()
        .flat_map(|&c| (1..=args.seeds).map(move |s| (c, s)))
        .collect();
    eprintln!(
        "{} cells × {} seeds = {} runs on {} threads",
        cells.len(),
        args.seeds,
        jobs.len(),
        rayon::current_num_threads()
    );
    let t0 = Instant::now();
    let rows: Vec<Row> = jobs
        .par_iter()
        .map(|&(c, seed)| {
            let s = scenario_for(&base, &c);
            let r =
                sim::run_with(&s, seed, &opts).unwrap_or_else(|e| panic!("{c:?} seed {seed}: {e}"));
            row(c, seed, &r)
        })
        .collect();
    let elapsed = t0.elapsed();
    eprintln!(
        "ran {} runs in {:.1} s ({:.1} ms per run per thread)",
        rows.len(),
        elapsed.as_secs_f64(),
        elapsed.as_secs_f64() * 1e3 * rayon::current_num_threads() as f64 / rows.len() as f64
    );

    std::fs::create_dir_all(&args.out).map_err(|e| e.to_string())?;
    write_runs(&rows, &args.out.join("grid_runs.parquet"))?;
    let summary = summarise(&rows, args.resamples);
    write_summary(&summary, &args.out.join("grid_summary.parquet"))?;
    let pairs = paired(&rows, args.resamples);
    write_pairs(&pairs, &args.out.join("grid_pairs.parquet"))?;
    report(&summary, &pairs, args.seeds);
    eprintln!(
        "wrote grid_runs, grid_summary and grid_pairs parquet to {}",
        args.out.display()
    );
    Ok(())
}

// ------------------------------------------------------------------------------------------
// Summaries
// ------------------------------------------------------------------------------------------

struct Summary {
    cell: Cell,
    metric: &'static str,
    n: usize,
    mean: f64,
    sd: f64,
    lo: f64,
    hi: f64,
}

fn cell_key(c: &Cell) -> u64 {
    let r = c.regime as u64;
    let g = GAPS.iter().position(|&g| g == c.gap).unwrap() as u64;
    let n = SNIPERS.iter().position(|&n| n == c.snipers).unwrap() as u64;
    let b = BLOCKS_MS.iter().position(|&b| b == c.block_ms).unwrap() as u64;
    (((r * 4 + b) * 3 + g) * 3 + n) * 3 + c.rule as u64
}

fn by_cell(rows: &[Row]) -> BTreeMap<Cell, Vec<&Row>> {
    let mut m: BTreeMap<Cell, Vec<&Row>> = BTreeMap::new();
    for r in rows {
        m.entry(r.cell).or_default().push(r);
    }
    m
}

fn summarise(rows: &[Row], resamples: usize) -> Vec<Summary> {
    let groups = by_cell(rows);
    let mut out = Vec::new();
    for (cell, rs) in &groups {
        let names: Vec<&'static str> = metric_values(rs[0]).iter().map(|x| x.0).collect();
        for (k, name) in names.iter().enumerate() {
            let xs = finite(&rs.iter().map(|r| metric_values(r)[k].1).collect::<Vec<_>>());
            let (lo, hi) = boot_ci(&xs, resamples, cell_key(cell) * 64 + k as u64);
            out.push(Summary {
                cell: *cell,
                metric: name,
                n: xs.len(),
                mean: if xs.is_empty() { f64::NAN } else { mean(&xs) },
                sd: sd(&xs),
                lo,
                hi,
            });
        }
    }
    out
}

struct Pair {
    group: (Regime, u64, (u64, u64), u32),
    metric: &'static str,
    a: OrderingRule,
    b: OrderingRule,
    n: usize,
    mean_diff: f64,
    sd_diff: f64,
    lo: f64,
    hi: f64,
}

impl Pair {
    fn separated(&self) -> bool {
        self.lo > 0.0 || self.hi < 0.0
    }
}

const PAIR_METRICS: [&str; 3] = ["lp_loss_bps", "sniper_take_bps", "lp_race_win_rate"];
const RULE_PAIRS: [(usize, usize); 3] = [(0, 1), (0, 2), (1, 2)];

fn paired(rows: &[Row], resamples: usize) -> Vec<Pair> {
    // group → rule → seed → row
    let mut g: BTreeMap<_, BTreeMap<usize, BTreeMap<u64, &Row>>> = BTreeMap::new();
    for r in rows {
        g.entry(r.cell.group())
            .or_default()
            .entry(r.cell.rule)
            .or_default()
            .insert(r.seed, r);
    }
    let mut out = Vec::new();
    for (gi, (group, by_rule)) in g.iter().enumerate() {
        for (mi, metric) in PAIR_METRICS.iter().enumerate() {
            let k = metric_values(rows.first().unwrap())
                .iter()
                .position(|x| x.0 == *metric)
                .unwrap();
            for (pi, &(a, b)) in RULE_PAIRS.iter().enumerate() {
                let diffs: Vec<f64> = by_rule[&a]
                    .iter()
                    .filter_map(|(seed, ra)| {
                        let rb = by_rule[&b].get(seed)?;
                        let d = metric_values(ra)[k].1 - metric_values(rb)[k].1;
                        d.is_finite().then_some(d)
                    })
                    .collect();
                let key = ((gi * 8 + mi) * 4 + pi) as u64 + (1 << 40);
                let (lo, hi) = boot_ci(&diffs, resamples, key);
                out.push(Pair {
                    group: *group,
                    metric,
                    a: OrderingRule::ALL[a],
                    b: OrderingRule::ALL[b],
                    n: diffs.len(),
                    mean_diff: if diffs.is_empty() {
                        f64::NAN
                    } else {
                        mean(&diffs)
                    },
                    sd_diff: sd(&diffs),
                    lo,
                    hi,
                });
            }
        }
    }
    out
}

// ------------------------------------------------------------------------------------------
// Parquet
// ------------------------------------------------------------------------------------------

fn write_runs(rows: &[Row], path: &std::path::Path) -> Result<(), String> {
    let mut t = Table::default();
    let s = |f: &dyn Fn(&Row) -> String| Col::Str(rows.iter().map(f).collect());
    let i = |f: &dyn Fn(&Row) -> i64| Col::I64(rows.iter().map(f).collect());
    let x = |f: &dyn Fn(&Row) -> f64| Col::F64(rows.iter().map(f).collect());
    t.push("regime", s(&|r| r.cell.regime.name().into()));
    t.push("rule", s(&|r| r.cell.rule().name().into()));
    t.push("block_ms", i(&|r| r.cell.block_ms as i64));
    t.push("speed_gap", x(&|r| r.cell.gap_f64()));
    t.push("snipers", i(&|r| r.cell.snipers as i64));
    t.push("seed", i(&|r| r.seed as i64));
    t.push("fills", i(&|r| r.fills as i64));
    t.push("volume_lots", i(&|r| r.volume_lots as i64));
    t.push("notional_usd", x(&|r| r.notional_usd));
    t.push("lp_loss_bps", x(&|r| r.lp_loss_bps));
    for (hi, &h) in HORIZONS_NS.iter().enumerate() {
        let name: &'static str = match h / 1_000_000_000 {
            0 => "lp_loss_run_h0_bps",
            1 => "lp_loss_run_h1s_bps",
            10 => "lp_loss_run_h10s_bps",
            _ => "lp_loss_run_h60s_bps",
        };
        t.push(name, x(&|r| r.lp_loss_run_bps[hi]));
    }
    t.push("lp_loss_usd", x(&|r| r.lp_loss_usd));
    t.push("sniper_take_bps", x(&|r| r.sniper_take_bps));
    t.push("sniper_take_usd_day", x(&|r| r.sniper_take_usd_day));
    t.push("chain_take_bps", x(&|r| r.chain_take_bps));
    t.push("lp_pnl_bps", x(&|r| r.lp_pnl_bps));
    t.push("races", i(&|r| r.races as i64));
    t.push("races_lp", i(&|r| r.races_lp as i64));
    t.push("races_sniper", i(&|r| r.races_sniper as i64));
    t.push("lp_race_win_rate", x(&|r| r.lp_race_win_rate));
    t.push("lp_effective_bps", x(&|r| r.lp_effective_bps));
    t.push("lp_adverse_1s_bps", x(&|r| r.lp_adverse_bps[0]));
    t.push("lp_adverse_10s_bps", x(&|r| r.lp_adverse_bps[1]));
    t.push("lp_adverse_60s_bps", x(&|r| r.lp_adverse_bps[2]));
    t.push("regular_cost_bps", x(&|r| r.regular_cost_bps));
    t.push("fill_delay_median_ms", x(&|r| r.fill_delay_median_ms));
    t.push("fill_delay_p99_ms", x(&|r| r.fill_delay_p99_ms));
    t.push("half_spread_bps", x(&|r| r.half_spread_bps));
    t.push("two_sided_share", x(&|r| r.two_sided_share));
    t.push("depth_lots", x(&|r| r.depth_lots));
    t.push("monday_take_share", x(&|r| r.monday_take_share));
    t.push("monday_gap_ticks", x(&|r| r.monday_gap_ticks));
    t.push("monday_lp_loss_usd", x(&|r| r.monday_lp_loss_usd));
    t.push(
        "monday_window_volume_usd",
        x(&|r| r.monday_window_volume_usd),
    );
    t.push("unsequenced", i(&|r| r.unsequenced as i64));
    t.write(path)
}

fn write_summary(rows: &[Summary], path: &std::path::Path) -> Result<(), String> {
    let mut t = Table::default();
    t.push(
        "regime",
        Col::Str(rows.iter().map(|r| r.cell.regime.name().into()).collect()),
    );
    t.push(
        "rule",
        Col::Str(rows.iter().map(|r| r.cell.rule().name().into()).collect()),
    );
    t.push(
        "block_ms",
        Col::I64(rows.iter().map(|r| r.cell.block_ms as i64).collect()),
    );
    t.push(
        "speed_gap",
        Col::F64(rows.iter().map(|r| r.cell.gap_f64()).collect()),
    );
    t.push(
        "snipers",
        Col::I64(rows.iter().map(|r| r.cell.snipers as i64).collect()),
    );
    t.push(
        "metric",
        Col::Str(rows.iter().map(|r| r.metric.into()).collect()),
    );
    t.push("n", Col::I64(rows.iter().map(|r| r.n as i64).collect()));
    t.push("mean", Col::F64(rows.iter().map(|r| r.mean).collect()));
    t.push("sd", Col::F64(rows.iter().map(|r| r.sd).collect()));
    t.push("ci_lo", Col::F64(rows.iter().map(|r| r.lo).collect()));
    t.push("ci_hi", Col::F64(rows.iter().map(|r| r.hi).collect()));
    t.write(path)
}

fn write_pairs(rows: &[Pair], path: &std::path::Path) -> Result<(), String> {
    let mut t = Table::default();
    t.push(
        "regime",
        Col::Str(rows.iter().map(|r| r.group.0.name().into()).collect()),
    );
    t.push(
        "block_ms",
        Col::I64(rows.iter().map(|r| r.group.1 as i64).collect()),
    );
    t.push(
        "speed_gap",
        Col::F64(
            rows.iter()
                .map(|r| r.group.2 .0 as f64 / r.group.2 .1 as f64)
                .collect(),
        ),
    );
    t.push(
        "snipers",
        Col::I64(rows.iter().map(|r| r.group.3 as i64).collect()),
    );
    t.push(
        "metric",
        Col::Str(rows.iter().map(|r| r.metric.into()).collect()),
    );
    t.push(
        "rule_a",
        Col::Str(rows.iter().map(|r| r.a.name().into()).collect()),
    );
    t.push(
        "rule_b",
        Col::Str(rows.iter().map(|r| r.b.name().into()).collect()),
    );
    t.push("n", Col::I64(rows.iter().map(|r| r.n as i64).collect()));
    t.push(
        "mean_diff",
        Col::F64(rows.iter().map(|r| r.mean_diff).collect()),
    );
    t.push(
        "sd_diff",
        Col::F64(rows.iter().map(|r| r.sd_diff).collect()),
    );
    t.push("ci_lo", Col::F64(rows.iter().map(|r| r.lo).collect()));
    t.push("ci_hi", Col::F64(rows.iter().map(|r| r.hi).collect()));
    t.push(
        "separated",
        Col::Bool(rows.iter().map(|r| r.separated()).collect()),
    );
    t.write(path)
}

// ------------------------------------------------------------------------------------------
// Console report: variance, seed count, gate
// ------------------------------------------------------------------------------------------

fn report(summary: &[Summary], pairs: &[Pair], seeds: u64) {
    println!("\n## Per-cell spread of LP loss to snipers (lp_loss_bps), seeds = {seeds}");
    println!("regime       B ms  gap  N  | fcfs mean (sd)      | pga mean (sd)       | cancel_priority mean (sd)");
    let lp: BTreeMap<Cell, &Summary> = summary
        .iter()
        .filter(|s| s.metric == "lp_loss_bps")
        .map(|s| (s.cell, s))
        .collect();
    let mut groups: Vec<_> = lp.keys().map(|c| c.group()).collect();
    groups.dedup();
    for g in &groups {
        let cell = |rule| Cell {
            regime: g.0,
            block_ms: g.1,
            gap: g.2,
            snipers: g.3,
            rule,
        };
        let f = |rule| {
            let s = lp[&cell(rule)];
            format!("{:>8.3} ({:>6.3})", s.mean, s.sd)
        };
        println!(
            "{:<12} {:>4} {:>4.1} {:>2}  | {} | {} | {}",
            g.0.name(),
            g.1,
            g.2 .0 as f64 / g.2 .1 as f64,
            g.3,
            f(0),
            f(1),
            f(2)
        );
    }

    // Seed count: for each B > 0 group and rule pair on LP loss, the n for which a 95% interval
    // of the paired difference has half-width ≤ half the mean difference.
    println!("\n## Seeds needed to separate the rules on LP loss (paired differences, B > 0)");
    let mut need: Vec<(f64, &Pair)> = Vec::new();
    for p in pairs
        .iter()
        .filter(|p| p.metric == "lp_loss_bps" && p.group.1 > 0)
    {
        let n = if p.mean_diff.abs() > 0.0 {
            (2.0 * 1.96 * p.sd_diff / p.mean_diff.abs()).powi(2).ceil()
        } else {
            f64::INFINITY
        };
        need.push((n, p));
    }
    need.sort_by(|a, b| b.0.total_cmp(&a.0));
    let over_200 = need.iter().filter(|(n, _)| *n > 200.0).count();
    let lte_20 = need.iter().filter(|(n, _)| *n <= 20.0).count();
    println!(
        "{} comparisons; {} need ≤ 20 seeds, {} need > 200. Hardest 15:",
        need.len(),
        lte_20,
        over_200
    );
    for (n, p) in need.iter().take(15) {
        println!(
            "  {:<12} B={:<4} gap={:.1} N={:<2} {:>15} - {:<15} diff {:>8.4} sd {:>7.4} -> n ≈ {}",
            p.group.0.name(),
            p.group.1,
            p.group.2 .0 as f64 / p.group.2 .1 as f64,
            p.group.3,
            p.a.name(),
            p.b.name(),
            p.mean_diff,
            p.sd_diff,
            n
        );
    }
    let sep = pairs
        .iter()
        .filter(|p| p.metric == "lp_loss_bps" && p.group.1 > 0 && p.separated())
        .count();
    let tot = pairs
        .iter()
        .filter(|p| p.metric == "lp_loss_bps" && p.group.1 > 0)
        .count();
    println!("\nseparated now (95% CI of the paired difference excludes 0): {sep}/{tot}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grid_has_216_cells_with_distinct_keys() {
        let c = cells();
        assert_eq!(c.len(), 216);
        let keys: std::collections::BTreeSet<u64> = c.iter().map(cell_key).collect();
        assert_eq!(keys.len(), 216);
    }

    #[test]
    fn bootstrap_is_deterministic_and_brackets_the_mean() {
        let xs: Vec<f64> = (0..50).map(|i| (i % 7) as f64).collect();
        let a = boot_ci(&xs, 1_000, 9);
        assert_eq!(a, boot_ci(&xs, 1_000, 9));
        let m = mean(&xs);
        assert!(a.0 < m && m < a.1, "{a:?} vs {m}");
        assert_eq!(boot_ci(&[2.0; 10], 100, 1), (2.0, 2.0));
    }

    #[test]
    fn speed_gap_sets_sniper_latency_relative_to_the_lp() {
        let mut s = Scenario::default();
        s.set_speed_gap(1, 10);
        assert_eq!(s.sniper.see.base_us, s.lp.see.base_us / 10);
        assert_eq!(s.sniper.send.jitter_us, s.lp.send.jitter_us / 10);
    }
}
