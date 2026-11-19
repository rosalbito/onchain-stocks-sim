//! The readers of the engine thread's rings. None of them writes back to the engine.
//!
//! - log writer: keeps the run's records and, when the run ends or stops, writes the input log
//!   (the batch format, `sim::log`) and checks it: re-simulated from the log alone, it must come
//!   back byte-identical with the same fills hash.
//! - standby: a second engine fed the same records (architecture section 5's hot standby). It
//!   times every `on_message` (the engine panel's latency) and compares its fills hash with the
//!   primary's at each checkpoint (drift detection).
//! - UI bridge: turns frames into the dashboard's JSON snapshot and hands it to the websocket
//!   tasks through a `tokio::sync::watch` (latest value wins).
//!
//! Idle readers spin briefly, then sleep 50 µs (architecture section 6).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use engine::{Engine, LogRecord};
use hdrhistogram::Histogram;
use metrics::AccountClass;
use rtrb::Consumer;
use serde_json::{json, Value};
use sim::log::{self, FillsHasher, LogFile, WireRecord};
use sim::OrderingRule;
use tokio::sync::watch;

use crate::engine_thread::{
    Frame, LogMsg, Outcome, RunInfo, StandbyMsg, UiMsg, RING_NAMES, SCENARIOS,
};

/// Spin, then sleep: what an idle reader does.
struct Idle(u32);

impl Idle {
    fn wait(&mut self) {
        self.0 += 1;
        if self.0 < 200 {
            std::hint::spin_loop();
        } else {
            thread::sleep(Duration::from_micros(50));
        }
    }
    fn reset(&mut self) {
        self.0 = 0;
    }
}

// ------------------------------------------------------------------------------------------
// Log writer
// ------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct LogStatus {
    /// The run being recorded, and its records so far.
    pub run_id: u64,
    pub records: u64,
    /// The last log written: its run, size, path and the result of its replay check.
    pub last_run: u64,
    pub last_records: u64,
    pub path: Option<String>,
    pub replay: Option<Result<String, String>>,
    /// Logs written and checked since the server started, and how many replayed exactly.
    pub written: u64,
    pub replayed_ok: u64,
}

pub fn spawn_log_writer(
    mut rx: Consumer<LogMsg>,
    out_dir: PathBuf,
    status: Arc<Mutex<LogStatus>>,
) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("log-writer".into())
        .spawn(move || {
            let mut run: Option<(Arc<RunInfo>, Vec<WireRecord>)> = None;
            let mut idle = Idle(0);
            loop {
                let Ok(msg) = rx.pop() else {
                    idle.wait();
                    continue;
                };
                idle.reset();
                match msg {
                    LogMsg::Start(info) => {
                        let mut s = status.lock().unwrap();
                        s.run_id = info.run_id;
                        s.records = 0;
                        run = Some((info, Vec::with_capacity(1 << 16)));
                    }
                    LogMsg::Record(r) => {
                        if let Some((_, recs)) = &mut run {
                            recs.push(WireRecord::from(&r));
                            if recs.len() % 1024 == 0 {
                                status.lock().unwrap().records = recs.len() as u64;
                            }
                        }
                    }
                    LogMsg::End { run_id, fills_hash } => {
                        if let Some((info, recs)) = run.take() {
                            debug_assert_eq!(info.run_id, run_id);
                            write_and_check(&out_dir, &info, recs, &fills_hash, &status);
                        }
                    }
                    LogMsg::Exit => return,
                }
            }
        })
        .expect("spawn log writer")
}

fn write_and_check(
    out_dir: &std::path::Path,
    info: &RunInfo,
    records: Vec<WireRecord>,
    primary_hash: &[u8; 64],
    status: &Mutex<LogStatus>,
) {
    let n = records.len() as u64;
    let file = LogFile {
        format_version: log::FORMAT_VERSION,
        scenario: info.scenario.clone(),
        seed: info.seed,
        config_toml: info.config_toml.clone(),
        records,
    };
    let bytes = log::encode(&file);
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let dir = out_dir.join("live");
    let path = dir.join(format!(
        "{}-seed{}-{stamp}-run{}.log",
        info.scenario, info.seed, info.run_id
    ));
    let written = std::fs::create_dir_all(&dir).and_then(|_| std::fs::write(&path, &bytes));
    let primary = String::from_utf8_lossy(primary_hash).to_string();
    // The gate: the log alone (scenario, seed, Config records) re-simulates to the same bytes,
    // and its records replay through a fresh engine to the primary's fills hash.
    let replay = match written {
        Err(e) => Err(format!("write {}: {e}", path.display())),
        Ok(()) => sim::replay_check(&bytes).and_then(|resim| {
            let recs: Vec<LogRecord> = file.records.iter().map(Into::into).collect();
            let engine_hash = log::replay_fills_hash(&recs);
            if resim != primary || engine_hash != primary {
                Err(format!(
                    "fills hash differs: primary {primary}, re-simulated {resim}, engine replay {engine_hash}"
                ))
            } else {
                Ok(primary.clone())
            }
        }),
    };
    let mut s = status.lock().unwrap();
    s.last_run = info.run_id;
    s.last_records = n;
    s.path = Some(path.display().to_string());
    s.written += 1;
    if replay.is_ok() {
        s.replayed_ok += 1;
    }
    match &replay {
        Ok(h) => eprintln!(
            "log {} ({n} records): replays exactly, fills {}",
            path.display(),
            &h[..16]
        ),
        Err(e) => eprintln!("log {}: REPLAY FAILED: {e}", path.display()),
    }
    s.replay = Some(replay);
}

// ------------------------------------------------------------------------------------------
// Standby
// ------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct StandbyStats {
    pub run_id: u64,
    pub records: u64,
    /// `on_message` latency over the last window (about 2 s), ns.
    pub p50: u64,
    pub p99: u64,
    pub p999: u64,
    pub max: u64,
    pub window_msgs: u64,
    /// Compared with the primary at the last checkpoint it reached.
    pub in_sync: Option<bool>,
    pub checked_at: u64,
}

pub fn spawn_standby(
    mut rx: Consumer<StandbyMsg>,
    stats: Arc<Mutex<StandbyStats>>,
) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("standby".into())
        .spawn(move || {
            let mut engine = Engine::new();
            let mut hasher = FillsHasher::default();
            let mut out = Vec::with_capacity(64);
            let mut processed = 0u64;
            let mut hist = Histogram::<u64>::new_with_bounds(1, 100_000_000, 3).unwrap();
            let mut window = Instant::now();
            let mut idle = Idle(0);
            loop {
                let Ok(msg) = rx.pop() else {
                    idle.wait();
                    continue;
                };
                idle.reset();
                match msg {
                    StandbyMsg::Reset(run_id) => {
                        engine = Engine::new();
                        hasher = FillsHasher::default();
                        processed = 0;
                        let mut s = stats.lock().unwrap();
                        *s = StandbyStats {
                            run_id,
                            p50: s.p50,
                            p99: s.p99,
                            p999: s.p999,
                            max: s.max,
                            ..Default::default()
                        };
                    }
                    StandbyMsg::Record(r) => {
                        let t = Instant::now();
                        engine.on_message_into(&r, &mut out);
                        let ns = t.elapsed().as_nanos() as u64;
                        hist.saturating_record(ns.max(1));
                        for ev in &out {
                            hasher.update(ev.seq, &ev.kind);
                        }
                        processed += 1;
                    }
                    StandbyMsg::Checkpoint {
                        records,
                        fills_hash,
                    } => {
                        let mut s = stats.lock().unwrap();
                        s.records = processed;
                        if processed == records {
                            let mine = hasher.clone().finish();
                            s.in_sync = Some(mine.as_bytes() == fills_hash);
                            s.checked_at = records;
                        }
                        if window.elapsed() >= Duration::from_secs(2) && !hist.is_empty() {
                            s.p50 = hist.value_at_quantile(0.5);
                            s.p99 = hist.value_at_quantile(0.99);
                            s.p999 = hist.value_at_quantile(0.999);
                            s.max = hist.max();
                            s.window_msgs = hist.len();
                            hist.reset();
                            window = Instant::now();
                        }
                    }
                    StandbyMsg::Exit => return,
                }
            }
        })
        .expect("spawn standby")
}

// ------------------------------------------------------------------------------------------
// UI bridge
// ------------------------------------------------------------------------------------------

/// The price series of one run, in fixed time buckets so finished points never move.
struct Series {
    run_id: u64,
    bucket_ns: u64,
    /// (bucket, t, true px, visible px, mid)
    points: Vec<(u64, u64, i64, i64, Option<f64>)>,
}

#[derive(Clone)]
struct FeedRace {
    order_id: u64,
    start_t_ns: u64,
    end_t_ns: u64,
    staleness: i64,
    qty: u64,
    outcome: Outcome,
    rule: OrderingRule,
}

pub fn spawn_ui_bridge(
    mut rx: Consumer<UiMsg>,
    tx: watch::Sender<Arc<str>>,
    standby: Arc<Mutex<StandbyStats>>,
    log: Arc<Mutex<LogStatus>>,
) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("ui-bridge".into())
        .spawn(move || {
            let mut series = Series {
                run_id: 0,
                bucket_ns: 1,
                points: Vec::new(),
            };
            // Races by order id; the outcome is fixed once seen resolved, with the rule then.
            let mut races: BTreeMap<u64, FeedRace> = BTreeMap::new();
            let mut idle = Idle(0);
            loop {
                let Ok(msg) = rx.pop() else {
                    idle.wait();
                    continue;
                };
                idle.reset();
                let f = match msg {
                    UiMsg::Frame(f) => f,
                    UiMsg::Exit => return,
                };
                if series.run_id != f.run_id {
                    series = Series {
                        run_id: f.run_id,
                        bucket_ns: (f.end_ns / 2_400).max(10_000_000),
                        points: Vec::new(),
                    };
                    races.clear();
                }
                let mid = match (f.n_bids, f.n_asks) {
                    (0, _) | (_, 0) => None,
                    _ => Some((f.bids[0].0 + f.asks[0].0) as f64 / 2.0),
                };
                let b = f.clock_ns / series.bucket_ns;
                let p = (b, f.clock_ns, f.true_px, f.visible_px, mid);
                match series.points.last_mut() {
                    Some(last) if last.0 == b => *last = p,
                    _ => series.points.push(p),
                }
                for r in &f.races[..f.n_races as usize] {
                    let e = races.entry(r.order_id).or_insert(FeedRace {
                        order_id: r.order_id,
                        start_t_ns: r.start_t_ns,
                        end_t_ns: 0,
                        staleness: r.staleness,
                        qty: r.qty,
                        outcome: Outcome::Open,
                        rule: f.rule,
                    });
                    if e.outcome == Outcome::Open && r.outcome != Outcome::Open {
                        e.outcome = r.outcome;
                        e.end_t_ns = r.end_t_ns;
                        e.rule = f.rule;
                    }
                }
                while races.len() > 64 {
                    races.pop_first();
                }
                let sb = standby.lock().unwrap().clone();
                let lg = log.lock().unwrap().clone();
                let snap = snapshot(&f, &series, &races, &sb, &lg);
                tx.send_replace(Arc::from(snap.to_string()));
            }
        })
        .expect("spawn ui bridge")
}

fn usd(cash_units: i128) -> f64 {
    // One cash unit is 1/10_000 of a tick·lot: $0.000001 with a $0.01 tick.
    cash_units as f64 / 1e6
}

fn px(ticks: i64) -> f64 {
    ticks as f64 / 100.0
}

pub fn rule_name(r: OrderingRule) -> &'static str {
    r.name()
}

fn snapshot(
    f: &Frame,
    series: &Series,
    races: &BTreeMap<u64, FeedRace>,
    sb: &StandbyStats,
    lg: &LogStatus,
) -> Value {
    let s = |ns: u64| ns as f64 / 1e9;
    // The price view: the whole run when it has a weekend or is short, else a 10-minute window
    // that scrolls with the clock.
    let span = if f.weekend.is_some() {
        f.end_ns
    } else {
        f.end_ns.min(600_000_000_000)
    };
    let v0 = f
        .clock_ns
        .saturating_sub(span * 4 / 5)
        .min(f.end_ns.saturating_sub(span));
    let pts: Vec<_> = series.points.iter().filter(|p| p.1 >= v0).collect();
    let stride = pts.len().div_ceil(900).max(1);
    let n = pts.len();
    let points: Vec<Value> = pts
        .iter()
        .enumerate()
        .filter(|(i, _)| i % stride == 0 || *i + 1 == n)
        .map(|(_, p)| json!([s(p.1), px(p.2), px(p.3), p.4.map(|m| m / 100.0)]))
        .collect();

    let mut feed: Vec<&FeedRace> = races
        .values()
        .filter(|r| r.outcome != Outcome::Open)
        .collect();
    feed.sort_by_key(|r| std::cmp::Reverse((r.end_t_ns, r.order_id)));
    let feed: Vec<Value> = feed
        .iter()
        .take(10)
        .map(|r| {
            json!({
                "id": r.order_id,
                "t": s(r.end_t_ns),
                "ms": (r.end_t_ns.saturating_sub(r.start_t_ns)) as f64 / 1e6,
                "winner": match r.outcome {
                    Outcome::Lp => "lp",
                    Outcome::Sniper => "sniper",
                    Outcome::Other => "trader",
                    Outcome::Open => "open",
                },
                "stake_usd": (r.staleness.max(0) as f64) * r.qty as f64 * 0.01,
                "staleness": r.staleness,
                "qty": r.qty,
                "rule": rule_name(r.rule),
            })
        })
        .collect();
    let open = races
        .values()
        .filter(|r| r.outcome == Outcome::Open)
        .count();

    let level = |l: &(i64, u64)| json!([px(l.0), l.1]);
    let pnl: serde_json::Map<String, Value> = AccountClass::ALL
        .iter()
        .zip(f.pnl)
        .map(|(c, v)| {
            let key = match c {
                AccountClass::Regular => "regular",
                AccountClass::Lp => "lp",
                AccountClass::Sniper => "sniper",
                AccountClass::Protocol => "protocol",
                AccountClass::Chain => "chain",
            };
            (key.to_string(), json!(usd(v)))
        })
        .collect();
    let rs = f.race_stats;
    let resolved = rs.lp_won + rs.sniper_won + rs.other_filled;

    json!({
        "type": "snapshot",
        "run": {
            "id": f.run_id,
            "scenario": SCENARIOS[f.scenario as usize].0,
            "scenarios": SCENARIOS.iter().map(|s| s.0).collect::<Vec<_>>(),
            "seed": f.seed,
            "rule": rule_name(f.rule),
            "block_ms": f.block_ms,
            "load": f.load,
            "speed": f.speed_milli as f64 / 1000.0,
            "seeking": f.seeking.map(s),
            "clock_s": s(f.clock_ns),
            "end_s": s(f.end_ns),
            "weekend": f.weekend.map(|(c, r)| json!([s(c), s(r)])),
            "hidden": f.hidden,
            "done": f.done,
            "stopped": f.stopped,
        },
        "book": {
            "bids": f.bids[..f.n_bids as usize].iter().map(level).collect::<Vec<_>>(),
            "asks": f.asks[..f.n_asks as usize].iter().map(level).collect::<Vec<_>>(),
            "more_bids": f.more_bids,
            "more_asks": f.more_asks,
        },
        "price": {
            "nasdaq": px(f.true_px),
            "visible": px(f.visible_px),
            "mid": series.points.last().and_then(|p| p.4).map(|m| m / 100.0),
            "view": [s(v0), s(v0 + span)],
            "series": points,
        },
        "races": {
            "feed": feed,
            "open": open,
            "lp": rs.lp_won,
            "sniper": rs.sniper_won,
            "other": rs.other_filled,
            "lp_win_rate": if resolved > 0 { Some(rs.lp_won as f64 / resolved as f64) } else { None },
        },
        "pnl": pnl,
        "pnl_sum": usd(f.pnl.iter().sum::<i128>()),
        "fees": { "chain": usd(f.priority_fees), "protocol": usd(f.taker_fees) },
        "engine": {
            "msgs_per_s": f.msgs_per_s,
            "step_ns_per_msg": f.step_ns_per_msg,
            "block": f.block,
            "seq": f.counts.records,
            "fills": f.counts.fills,
            "configs": f.counts.configs,
            "waiting": f.counts.unsequenced,
            "rings": f.rings.iter().zip(RING_NAMES).map(|(r, name)| json!({
                "name": name, "used": r.used, "cap": r.cap, "peak": r.peak,
            })).collect::<Vec<_>>(),
            "latency_ns": { "p50": sb.p50, "p99": sb.p99, "p999": sb.p999, "max": sb.max, "window": sb.window_msgs },
            "standby": { "run": sb.run_id, "records": sb.records, "in_sync": sb.in_sync, "checked_at": sb.checked_at },
            "fills_hash": String::from_utf8_lossy(&f.fills_hash[..16]),
        },
        "log": {
            "run": lg.run_id,
            "records": lg.records,
            "last_run": lg.last_run,
            "last_records": lg.last_records,
            "path": lg.path,
            "replay": match &lg.replay {
                None => json!(null),
                Some(Ok(h)) => json!({ "ok": true, "fills_hash": &h[..16] }),
                Some(Err(e)) => json!({ "ok": false, "error": e }),
            },
            "written": lg.written,
            "replayed_ok": lg.replayed_ok,
        },
    })
}
