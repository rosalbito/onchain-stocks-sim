//! Phase 1 and 2 gates (spec sections 13 and 15):
//! - zero-sum on every run,
//! - same seed → byte-identical input log, and the log replays to the same fills,
//! - golden hashes of the log and fills for fixed seeds per ordering rule,
//! - the Budish check, including latency scaling in races isolated from price drift.

use std::collections::BTreeMap;

use metrics::AccountClass;
use sim::{OrderingRule, Scenario};

fn load(name: &str) -> Scenario {
    let path = format!("{}/../../scenarios/{name}.toml", env!("CARGO_MANIFEST_DIR"));
    Scenario::from_toml(&std::fs::read_to_string(path).unwrap()).unwrap()
}

const SCENARIOS: [&str; 3] = ["calm_weekday", "news_jump", "monday_open"];

fn with_rule(mut s: Scenario, rule: OrderingRule, block_ms: u64) -> Scenario {
    s.sequencer.rule = rule;
    s.sequencer.block_ms = block_ms;
    s
}

// ------------------------------------------------------------------------------------------
// Zero-sum and determinism
// ------------------------------------------------------------------------------------------

#[test]
fn zero_sum_on_every_scenario_rule_and_block_length() {
    for name in SCENARIOS {
        for rule in OrderingRule::ALL {
            for block_ms in [0, 50, 250, 1_000] {
                for seed in [1, 2] {
                    let s = with_rule(load(name), rule, block_ms);
                    // `run` fails if cash, shares or marked PnL do not sum to exactly zero.
                    let r = sim::run(&s, seed).unwrap_or_else(|e| {
                        panic!("{name} {rule:?} B={block_ms} seed {seed}: {e}")
                    });
                    let total: i128 = r.pnl.by_class.values().sum();
                    assert_eq!(total, 0);
                    let take: i128 = r.take_at_fill.values().sum();
                    assert_eq!(take, 0, "markout-at-fill transfers also balance");
                    assert!(
                        r.counts.fills > 0,
                        "{name} {rule:?} B={block_ms}: no trading"
                    );
                }
            }
        }
    }
}

#[test]
fn same_seed_gives_byte_identical_log() {
    for name in SCENARIOS {
        for rule in OrderingRule::ALL {
            let s = with_rule(load(name), rule, 250);
            let a = sim::run(&s, 7).unwrap();
            let b = sim::run(&s, 7).unwrap();
            assert_eq!(a.log_bytes, b.log_bytes, "{name} {rule:?}");
            assert_eq!(a.log_hash, b.log_hash);
            assert_eq!(a.fills_hash, b.fills_hash);
            let c = sim::run(&s, 8).unwrap();
            assert_ne!(a.log_hash, c.log_hash, "different seeds differ");
        }
    }
}

#[test]
fn log_on_disk_replays_to_the_same_fills() {
    for name in SCENARIOS {
        let s = with_rule(load(name), OrderingRule::CancelPriority, 50);
        let r = sim::run(&s, 3).unwrap();
        let file = sim::log::decode(&r.log_bytes).unwrap();
        assert_eq!(file.seed, 3);
        assert_eq!(Scenario::from_toml(&file.config_toml).unwrap(), s);
        let records: Vec<engine::LogRecord> = file.records.iter().map(Into::into).collect();
        assert_eq!(records, r.records);
        assert_eq!(sim::log::replay_fills_hash(&records), r.fills_hash);
        // seq is the log position.
        assert!(records
            .iter()
            .enumerate()
            .all(|(i, r)| r.seq == i as u64 + 1));
    }
}

#[test]
fn outside_inputs_are_the_same_under_every_rule() {
    // Common random numbers: the price path and the regular traders' orders do not depend on
    // the ordering rule. A regular trader skips an arrival when the opposite side of the book
    // is empty, which happens when the LP stops quoting a side at its inventory cap and can
    // differ by rule, so the cap is lifted here.
    let mut base = load("calm_weekday");
    base.lp.inventory_cap_lots = 1_000_000_000;
    let mut ticks = Vec::new();
    let mut regular = Vec::new();
    for rule in OrderingRule::ALL {
        let r = sim::run(&with_rule(base.clone(), rule, 250), 11).unwrap();
        let regular_id = *r
            .agents
            .iter()
            .find(|(_, c)| **c == AccountClass::Regular)
            .unwrap()
            .0;
        ticks.push(
            r.records
                .iter()
                .filter(|x| matches!(x.kind, engine::MsgKind::Tick { .. }))
                .map(|x| (x.t_ns, x.kind))
                .collect::<Vec<_>>(),
        );
        // Arrival time and side/size of each regular order (the limit depends on the book).
        regular.push(
            r.records
                .iter()
                .filter(|x| x.agent == regular_id)
                .map(|x| match x.kind {
                    engine::MsgKind::New { side, qty_lots, .. } => (x.t_ns, side, qty_lots),
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>(),
        );
    }
    assert!(ticks.windows(2).all(|w| w[0] == w[1]));
    // With the cap lifted the book never empties, so every arrival goes out: the regular
    // traders' orders (arrival time, side, size) must be identical under every rule.
    assert!(regular.windows(2).all(|w| w[0] == w[1]));
    assert!(regular[0].len() > 1_000);
}

// ------------------------------------------------------------------------------------------
// Golden hashes
// ------------------------------------------------------------------------------------------

/// Golden hashes of the input log and the fills log for fixed seeds per ordering rule.
/// A change here means the simulation's output changed: it needs an explicit sign-off
/// (review the diff with `cargo insta review`, or rerun with INSTA_UPDATE=always).
#[test]
fn golden_hashes() {
    let mut out = String::new();
    for name in SCENARIOS {
        for rule in OrderingRule::ALL {
            for block_ms in [0, 250] {
                for seed in [1, 2] {
                    let r = sim::run(&with_rule(load(name), rule, block_ms), seed).unwrap();
                    out.push_str(&format!(
                        "{name:<13} {:<15} B={block_ms:<4} seed={seed}  log={}  fills={}\n",
                        rule.name(),
                        &r.log_hash[..16],
                        &r.fills_hash[..16]
                    ));
                }
            }
        }
    }
    insta::assert_snapshot!(out);
}

// ------------------------------------------------------------------------------------------
// The Budish check (spec section 13, item 3)
// ------------------------------------------------------------------------------------------

/// The isolated-race setup: one LP, N snipers, jumps only (no diffusion, about one every 5 s, so
/// races never overlap), no regular traders, no inventory cap. With `equal_speed`, the LP has
/// exactly the snipers' see and send latency distributions.
fn budish(n: u32, rule: OrderingRule, block_ms: u64, equal_speed: bool) -> Scenario {
    let mut s = Scenario {
        name: "budish".into(),
        duration_s: 3_600,
        ..Scenario::default()
    };
    s.price.step_rate_per_s = 0.0;
    s.price.jump_rate_per_s = 0.2;
    s.regular.count = 0;
    s.lp.inventory_cap_lots = 1_000_000_000;
    s.sniper.count = n;
    if equal_speed {
        s.lp.see = s.sniper.see;
        s.lp.send = s.sniper.send;
    }
    s.sequencer.rule = rule;
    s.sequencer.block_ms = block_ms;
    s
}

const SEEDS: std::ops::RangeInclusive<u64> = 1..=8;

#[derive(Default, Debug)]
struct Agg {
    lp: u64,
    sniper: u64,
    other: u64,
    unresolved: u64,
    episodes: u64,
}

fn aggregate(s: &Scenario) -> Agg {
    let mut a = Agg::default();
    for seed in SEEDS {
        let r = sim::run(s, seed).unwrap();
        a.lp += r.races.lp_won;
        a.sniper += r.races.sniper_won;
        a.other += r.races.other_filled;
        a.unresolved += r.races.unresolved;
        a.episodes += r.races.episodes;
    }
    a
}

/// FCFS with N equally fast snipers: the LP wins about 1 race in N + 1.
///
/// Tolerance: within one episode (one jump) every stale quote goes to whoever's message is
/// sequenced first, so episodes, not races, are the independent trials. Each is Bernoulli with
/// p = 1/(N+1) by symmetry (i.i.d. latencies). We allow 4 binomial standard errors on the
/// episode count: sqrt(p(1-p)/episodes) is about 0.004-0.007 here (≈5,800 episodes), so the
/// band is ±0.015 to ±0.026. The runs are seeded, so the test is deterministic; the band says
/// a correct model would fail it with probability < 1e-4 for a fresh set of seeds.
#[test]
fn budish_fcfs_lp_wins_one_in_n_plus_one() {
    let mut report = String::new();
    for n in [1u32, 3, 10] {
        let a = aggregate(&budish(n, OrderingRule::Fcfs, 0, true));
        let resolved = a.lp + a.sniper + a.other;
        assert_eq!(a.other, 0, "no one else trades in this setup");
        assert!(a.unresolved <= SEEDS.count() as u64, "{a:?}");
        assert!(a.episodes > 5_000, "{a:?}");
        let rate = a.lp as f64 / resolved as f64;
        let p = 1.0 / (n as f64 + 1.0);
        let tol = 4.0 * (p * (1.0 - p) / a.episodes as f64).sqrt();
        report.push_str(&format!(
            "N={n:<2} LP won {}/{} = {rate:.4}, expected {p:.4} ± {tol:.4} ({} episodes)\n",
            a.lp, resolved, a.episodes
        ));
        assert!(
            (rate - p).abs() <= tol,
            "N={n}: LP win rate {rate:.4}, expected {p:.4} ± {tol:.4}"
        );
    }
    println!("{report}");
}

/// Cancel priority with each race's messages inside one block: the LP wins nearly every race.
/// With B = 1 s and races lasting milliseconds, the LP can lose only when the sniper's order and
/// the LP's cancel straddle a block boundary, a chance of roughly (cancel delay − snipe delay)
/// / B: about 0.1% at equal speed and 1.5% with the default (slower) LP. We require ≥ 95%.
#[test]
fn budish_cancel_priority_lp_wins_nearly_all() {
    for equal_speed in [true, false] {
        let a = aggregate(&budish(3, OrderingRule::CancelPriority, 1_000, equal_speed));
        let resolved = a.lp + a.sniper + a.other;
        let rate = a.lp as f64 / resolved as f64;
        println!(
            "cancel priority, B=1s, equal_speed={equal_speed}: LP won {}/{resolved} = {rate:.4}",
            a.lp
        );
        assert!(resolved > 5_000);
        assert!(
            rate >= 0.95,
            "equal_speed={equal_speed}: LP win rate {rate:.4}"
        );
    }
}

/// In races isolated from price drift, shrinking every latency in proportion must not change
/// the sniper take (FCFS). Spec section 13 scopes the check to isolated races: with diffusion on,
/// the price clock is not scaled, so a faster market sees fewer moves per race (DECISIONS 39).
/// This runs on the isolated setup of [`budish`]: jumps only, about one every 5 s, so no race
/// overlaps another price move.
///
/// Sniper take here is the snipers' gain on their fills against the true price at the moment of
/// the fill (markout horizon 0): the stale-quote prize, without the noise of marking their
/// leftover inventory to the end-of-run price. Exact equality is not expected: once a single
/// near-tie resolves differently, every later latency draw shifts, so the scaled run is an
/// independent sample of the same process. Tolerance: 4 standard errors of the difference of
/// two 8-seed totals, estimated from the per-seed spread of the unscaled run.
#[test]
fn budish_isolated_races_latency_scaling_keeps_sniper_take() {
    let take = |num: u64, den: u64| -> Vec<f64> {
        let mut s = budish(3, OrderingRule::Fcfs, 0, true);
        s.scale_latencies(num, den);
        SEEDS
            .map(|seed| sim::run(&s, seed).unwrap().take_at_fill[&AccountClass::Sniper] as f64)
            .collect()
    };
    let base = take(1, 1);
    let n = base.len() as f64;
    let mean = base.iter().sum::<f64>() / n;
    let sd = (base.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0)).sqrt();
    let total: f64 = base.iter().sum();
    let tol = 4.0 * sd * (2.0 * n).sqrt();
    let mut by_scale = BTreeMap::new();
    for (num, den) in [(1, 2), (1, 10)] {
        let t: f64 = take(num, den).iter().sum();
        by_scale.insert(den, t);
        println!(
            "latency ×{num}/{den}: sniper take {t} vs {total} tick·lots (diff {:+.2}%, tol ±{:.2}%)",
            100.0 * (t - total) / total,
            100.0 * tol / total
        );
        assert!(
            (t - total).abs() <= tol,
            "×{num}/{den}: {t} vs {total} (tol {tol})"
        );
    }
}
