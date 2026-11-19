//! Phase 4 gate, simulation side: a run driven step by step with live control changes replays
//! exactly from its input log (spec sections 12 and 15).
//!
//! - Live mode without controls is the batch run: same log bytes, same fills hash.
//! - Advancing in many small steps handles exactly what one big step does.
//! - With controls (rule, B, load, speed, stop), re-simulating from the log alone gives the same
//!   log byte for byte, and replaying the records through a fresh engine gives the same fills.

use engine::MsgKind;
use metrics::AccountClass;
use sim::log::{self, replay_fills_hash};
use sim::{Control, Live, OrderingRule, Scenario};

fn load(name: &str) -> Scenario {
    let path = format!("{}/../../scenarios/{name}.toml", env!("CARGO_MANIFEST_DIR"));
    Scenario::from_toml(&std::fs::read_to_string(path).unwrap()).unwrap()
}

const S: u64 = 1_000_000_000;
const MS: u64 = 1_000_000;

#[test]
fn live_without_controls_is_the_batch_run() {
    for name in ["calm_weekday", "news_jump", "monday_open"] {
        for (rule, block_ms) in [
            (OrderingRule::Fcfs, 0),
            (OrderingRule::Pga, 250),
            (OrderingRule::CancelPriority, 50),
        ] {
            let mut cfg = load(name);
            cfg.sequencer.rule = rule;
            cfg.sequencer.block_ms = block_ms;
            let batch = sim::run(&cfg, 3).unwrap();
            let mut live = Live::new(&cfg, 3).unwrap();
            // Uneven steps, including steps that land exactly on event times.
            let mut t = 0;
            let mut step = 1;
            while !live.done() {
                t += step * 7_919_993;
                step = step % 50_000 + 1_237;
                live.advance_to(t);
            }
            assert_eq!(
                live.log_bytes(),
                batch.log_bytes,
                "{name} {rule:?} B={block_ms}"
            );
            assert_eq!(
                live.fills_hash(),
                batch.fills_hash,
                "{name} {rule:?} B={block_ms}"
            );
            let total: i128 = AccountClass::ALL.iter().map(|&c| live.pnl().get(c)).sum();
            assert_eq!(total, 0);
        }
    }
}

/// Drive a calm weekday the way the dashboard would: a few minutes of virtual time in uneven
/// wall-clock-sized steps, with every kind of control change along the way.
fn live_run_with_controls(seed: u64, schedule: &[(u64, Control)]) -> Live {
    let mut cfg = load("calm_weekday");
    cfg.duration_s = 600;
    let mut live = Live::new(&cfg, seed).unwrap();
    let mut t = 0;
    let mut next = 0;
    while !live.done() {
        // About one 33 ms frame at 100× speed, with jitter, like a real frame loop.
        t += 3_300 * MS + (t / 7) % (900 * MS);
        live.advance_to(t);
        while next < schedule.len() && schedule[next].0 <= t {
            live.control(schedule[next].1);
            next += 1;
        }
    }
    live
}

fn schedule() -> Vec<(u64, Control)> {
    vec![
        (20 * S, Control::Speed(10)),
        (30 * S, Control::Rule(OrderingRule::Pga)),
        (45 * S, Control::BlockMs(250)),
        (60 * S, Control::Load(10)),
        (90 * S, Control::Rule(OrderingRule::CancelPriority)),
        (90 * S, Control::Speed(1_000)),
        (120 * S, Control::BlockMs(1_000)),
        (150 * S, Control::Load(100)),
        (165 * S, Control::BlockMs(0)),
        (180 * S, Control::Rule(OrderingRule::Fcfs)),
        (200 * S, Control::BlockMs(50)),
        (220 * S, Control::Load(1)),
        (240 * S, Control::Rule(OrderingRule::Pga)),
    ]
}

#[test]
fn live_run_with_control_changes_replays_exactly_from_its_log() {
    for seed in [1, 2, 3] {
        let live = live_run_with_controls(seed, &schedule());
        let bytes = live.log_bytes();
        assert_eq!(sim::config_records(live.records()), schedule().len());

        // Re-simulate from the log alone: identical log, identical fills.
        let hash = sim::replay_check(&bytes).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        assert_eq!(hash, live.fills_hash());

        // Replay the records through a fresh engine: identical fills.
        let file = log::decode(&bytes).unwrap();
        let records: Vec<engine::LogRecord> = file.records.iter().map(Into::into).collect();
        assert_eq!(replay_fills_hash(&records), live.fills_hash());

        // The controls really changed the run.
        let plain = live_run_with_controls(seed, &[]);
        assert_ne!(plain.fills_hash(), live.fills_hash());
        let total: i128 = AccountClass::ALL.iter().map(|&c| live.pnl().get(c)).sum();
        assert_eq!(total, 0);
    }
}

#[test]
fn controls_take_effect() {
    let live = live_run_with_controls(1, &schedule());
    assert_eq!(live.rule(), OrderingRule::Pga);
    assert_eq!(live.block_ms(), 50);
    assert_eq!(live.load(), 1);
    // PGA from 30 s: priority fees were paid.
    assert!(live.priority_fees() > 0);
    // Block numbers never go back, through every change of B.
    let recs = live.records();
    assert!(recs.windows(2).all(|w| w[0].block <= w[1].block));
    // Load ×100 between 150 s and 220 s: many more regular-trader orders than at ×1.
    let regular = |from: u64, to: u64| {
        recs.iter()
            .filter(|r| r.t_ns >= from && r.t_ns < to && r.agent == 5)
            .filter(|r| matches!(r.kind, MsgKind::New { .. }))
            .count()
    };
    assert!(regular(160 * S, 220 * S) > 10 * regular(0, 60 * S).max(1));
}

#[test]
fn a_stopped_run_replays_up_to_its_stop() {
    let mut cfg = load("news_jump");
    cfg.sequencer.block_ms = 250;
    let mut live = Live::new(&cfg, 7).unwrap();
    live.advance_to(61 * S + 3 * MS);
    live.control(Control::Rule(OrderingRule::CancelPriority));
    live.advance_to(75 * S);
    assert!(live.control(Control::Stop).is_some());
    assert!(live.control(Control::Load(5)).is_none());
    live.advance_to(200 * S);
    assert!(live.done());
    assert_eq!(live.clock(), 75 * S);
    let hash = sim::replay_check(&live.log_bytes()).unwrap();
    assert_eq!(hash, live.fills_hash());
}

#[test]
fn control_codes_round_trip() {
    let all = [
        Control::Rule(OrderingRule::Fcfs),
        Control::Rule(OrderingRule::Pga),
        Control::Rule(OrderingRule::CancelPriority),
        Control::BlockMs(0),
        Control::BlockMs(1_000),
        Control::Load(100),
        Control::Speed(10),
        Control::Stop,
    ];
    for c in all {
        let (p, v) = c.encode();
        assert_eq!(Control::decode(p, v), Some(c));
    }
    assert_eq!(Control::decode(1, 9), None);
    assert_eq!(Control::decode(99, 0), None);
}
