//! The engine thread: the single writer (architecture section 4).
//!
//! It owns the book, and nothing else touches it. Three states:
//!
//! - **setup**: no run; frames still flow so the System page shows the process.
//! - **market**: the simulation (`sim::Live`) paced from the wall clock × playback speed. Controls
//!   from the control ring become logged `Config` records at the current virtual time.
//! - **stress**: a producer thread feeds synthetic flow through an input ring; the engine thread
//!   numbers each message, hands it to the log writer, matches it, and hands it to the standby.
//!
//! It publishes over one rtrb ring per reader: the log writer and the standby get every record
//! and make the engine wait when full (a record is never dropped); the UI bridge gets one
//! fixed-size frame about 30 times a second and loses it when full (only the newest matters).

use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use engine::{Engine, EventKind, LogRecord, MsgKind, Side};
use hdrhistogram::Histogram;
use metrics::{AccountClass, RaceOutcome, RaceStats};
use rtrb::{Consumer, Producer};
use sim::log::FillsHasher;
use sim::{Control, Counts, Live, OrderingRule, Scenario};

use crate::stress::{self, ProducerHandle, StressMsg, PARAM_STRESS_RATE};
use crate::sysinfo::{Telemetry, T_ENGINE, W_LOG, W_STANDBY};

/// The scenarios the setup screen offers, embedded so the binary runs from anywhere.
pub const SCENARIOS: [(&str, &str); 3] = [
    (
        "news_jump",
        include_str!("../../../scenarios/news_jump.toml"),
    ),
    (
        "calm_weekday",
        include_str!("../../../scenarios/calm_weekday.toml"),
    ),
    (
        "monday_open",
        include_str!("../../../scenarios/monday_open.toml"),
    ),
];

pub fn scenario_index(name: &str) -> Option<u8> {
    SCENARIOS
        .iter()
        .position(|(n, _)| *n == name)
        .map(|i| i as u8)
}

/// What the setup screen starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartSpec {
    /// Stress mode (synthetic flow) instead of the market simulation.
    pub stress: bool,
    /// Stress target rate, msgs/s (0 = as fast as possible).
    pub rate: u64,
    pub scenario: u8,
    pub rule: OrderingRule,
    pub block_ms: u64,
    pub seed: u64,
    /// Playback speed in thousandths (market).
    pub speed_milli: u64,
}

impl Default for StartSpec {
    fn default() -> Self {
        Self {
            stress: false,
            rate: 100_000,
            scenario: 0,
            rule: OrderingRule::Fcfs,
            block_ms: 0,
            seed: 1,
            speed_milli: 1_000,
        }
    }
}

/// A command from the edge to the engine thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    /// Stop any run and start this one.
    Start(StartSpec),
    /// A live change to the market run, logged as a `Config` record.
    Control(Control),
    /// A live change of the stress rate (logged by the producer, in the stream).
    StressRate(u64),
    /// Fast-forward to just before the next news jump, close or reopen. Pacing only.
    Skip,
    /// Stop the run and start the same spec again (with the current rule and B).
    Restart,
    /// Stop the run and go back to setup.
    Stop,
    Shutdown,
}

/// The header of a run, sent once to the log writer.
#[derive(Debug)]
pub struct RunInfo {
    pub run_id: u64,
    pub stress: bool,
    pub scenario: String,
    pub seed: u64,
    /// The scenario TOML, or the stress parameters.
    pub config: String,
}

pub enum LogMsg {
    Start(Arc<RunInfo>),
    Record(LogRecord),
    /// The run is over: close the log and check it against this fills hash.
    End {
        run_id: u64,
        records: u64,
        fills_hash: [u8; 64],
    },
    Exit,
}

#[derive(Clone, Copy)]
pub enum StandbyMsg {
    Reset(u64),
    Record(LogRecord),
    /// The primary had sequenced `records` records with this fills hash.
    Checkpoint {
        records: u64,
        fills_hash: [u8; 64],
    },
    Exit,
}

pub const DEPTH: usize = 10;
pub const RACES: usize = 8;
/// Points on the price chart.
pub const CHART: usize = 240;

#[derive(Debug, Clone, Copy, Default)]
pub struct RaceRow {
    pub start_t_ns: u64,
    /// 0 while open.
    pub end_t_ns: u64,
    pub staleness: i64,
    /// The quote's size when it was posted.
    pub qty: u64,
    pub outcome: Outcome,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Outcome {
    #[default]
    Open,
    Lp,
    Sniper,
    Other,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RingStat {
    pub used: u32,
    pub cap: u32,
    /// Highest fill since the last frame.
    pub peak: u32,
}

pub const RING_NAMES: [&str; 5] = ["control", "input", "log", "standby", "ui"];

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum State {
    #[default]
    Setup,
    Market,
    Stress,
}

/// The market part of a frame.
#[derive(Clone, Copy)]
pub struct MarketFrame {
    pub weekend: Option<(u64, u64)>,
    pub hidden: bool,
    pub seeking: Option<u64>,
    pub true_px: i64,
    pub rule: OrderingRule,
    pub load: u32,
    pub block: u64,
    pub counts: Counts,
    /// Cash units, in `AccountClass::ALL` order, marked to the true price now.
    pub pnl: [i128; 5],
    /// `transfer[from][to]`: what `from` lost to `to` at fill time, tick·lots.
    pub transfer: [[i128; 5]; 5],
    /// (taker, priority) fees paid by each class, cash units.
    pub fees: [(i128, i128); 5],
    pub races: [RaceRow; RACES],
    pub n_races: u8,
    pub race_stats: RaceStats,
    /// The chart covers [t0, t1]; point k is the state at t0 + (k+1)(t1 - t0)/CHART.
    pub chart_t0: u64,
    pub chart_t1: u64,
    /// (true price, bid + ask, or i64::MIN when one side is empty).
    pub chart: [(i64, i64); CHART],
    pub n_chart: u16,
}

impl Default for MarketFrame {
    fn default() -> Self {
        Self {
            weekend: None,
            hidden: false,
            seeking: None,
            true_px: 0,
            rule: OrderingRule::Fcfs,
            load: 1,
            block: 0,
            counts: Counts::default(),
            pnl: [0; 5],
            transfer: [[0; 5]; 5],
            fees: [(0, 0); 5],
            races: [RaceRow::default(); RACES],
            n_races: 0,
            race_stats: RaceStats::default(),
            chart_t0: 0,
            chart_t1: 0,
            chart: [(0, i64::MIN); CHART],
            n_chart: 0,
        }
    }
}

/// The stress part of a frame.
#[derive(Clone, Copy, Default)]
pub struct StressFrame {
    pub target_rate: u64,
    /// Producer hand-off to matched, over the last second, ns.
    pub e2e_p50: u64,
    pub e2e_p99: u64,
    pub e2e_max: u64,
    /// One drained batch of the input ring, over the last second.
    pub batch_p50_ns: u64,
    pub batch_p99_ns: u64,
    pub batch_mean: f64,
    pub orders_resting: u64,
}

/// One frame for the UI. Fixed size, so the ring holds values, not pointers.
#[derive(Clone, Copy)]
pub struct Frame {
    pub state: State,
    pub run_id: u64,
    pub spec: StartSpec,
    pub clock_ns: u64,
    pub end_ns: u64,
    pub done: bool,
    pub stopped: bool,
    pub records: u64,
    pub fills: u64,
    /// Records sequenced per wall second, over the last second.
    pub msgs_per_s: f64,
    pub bids: [(i64, u64); DEPTH],
    pub asks: [(i64, u64); DEPTH],
    pub n_bids: u8,
    pub n_asks: u8,
    pub levels: (u32, u32),
    pub rings: [RingStat; 5],
    pub fills_hash: [u8; 64],
    pub market: MarketFrame,
    pub stress: StressFrame,
}

// The ring's slots are allocated once, so a large variant costs memory, not allocations.
#[allow(clippy::large_enum_variant)]
pub enum UiMsg {
    Frame(Frame),
    Exit,
}

pub struct Rings {
    pub cmd: Consumer<Command>,
    pub log: Producer<LogMsg>,
    pub standby: Producer<StandbyMsg>,
    pub ui: Producer<UiMsg>,
}

const FRAME: Duration = Duration::from_millis(33);
/// At most this much wall time per simulation step; if the simulation can't keep up with the
/// playback speed, the speed drops instead of the frames.
const STEP_BUDGET: Duration = Duration::from_millis(20);
const INPUT_RING: usize = 1 << 16;
/// Messages taken from the input ring before the engine thread looks at anything else.
const BATCH: usize = 8_192;

/// Push and wait while the ring is full: the log and the standby never drop a record.
fn push_wait<T>(
    p: &mut Producer<T>,
    mut v: T,
    peak: &mut u32,
    waits: &std::sync::atomic::AtomicU64,
) {
    let mut waited = false;
    loop {
        match p.push(v) {
            Ok(()) => break,
            Err(rtrb::PushError::Full(back)) => {
                v = back;
                if !waited {
                    waits.fetch_add(1, Relaxed);
                    waited = true;
                }
                std::hint::spin_loop();
                thread::yield_now();
            }
        }
    }
    let used = (p.buffer().capacity() - p.slots()) as u32;
    *peak = (*peak).max(used);
}

fn hash_bytes(hex: &str) -> [u8; 64] {
    let mut out = [b'0'; 64];
    out.copy_from_slice(&hex.as_bytes()[..64]);
    out
}

fn scenario_cfg(spec: &StartSpec) -> Scenario {
    let mut cfg =
        Scenario::from_toml(SCENARIOS[spec.scenario as usize].1).expect("embedded scenario");
    cfg.sequencer.rule = spec.rule;
    cfg.sequencer.block_ms = spec.block_ms;
    // The headline PGA bid (DECISIONS 77): the snipers together bid 0.3 of the prize, split
    // equally. The config default (0.5 each) stays for the golden hashes, but live runs must
    // match the numbers on the slides. It only matters under PGA, and a live rule switch keeps it.
    cfg.sniper.alpha_bps = HEADLINE_ALPHA_BPS / cfg.sniper.count.max(1) as u64;
    cfg
}

/// The snipers' combined PGA bid, in bps of the prize (DECISIONS 77).
pub const HEADLINE_ALPHA_BPS: u64 = 3_000;

fn window_hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 60_000_000_000, 3).unwrap()
}

struct MarketRun {
    live: Live,
    published: usize,
    ended: bool,
    /// Skip targets (virtual times to land at), ascending.
    marks: Vec<u64>,
    seek: Option<u64>,
    carry: u128,
    last_tick: Instant,
    /// The book mid (bid + ask, or i64::MIN) whenever it changed, for the chart.
    mids: Vec<(u64, i64)>,
    /// The last non-zero speed, which sets the chart's window while paused.
    view_speed: u64,
}

struct StressRun {
    engine: Engine,
    input: Consumer<StressMsg>,
    producer: Option<ProducerHandle>,
    start: Instant,
    seq: u64,
    fills: u64,
    hasher: FillsHasher,
    out: Vec<engine::Event>,
    e2e: Histogram<u64>,
    batch: Histogram<u64>,
    batch_msgs: u64,
    batch_count: u64,
    window: Instant,
    last: StressFrame,
    /// The producer's `Stop` record went through: the run is complete.
    stopped: bool,
    ended: bool,
}

enum Run {
    Idle,
    Market(Box<MarketRun>),
    Stress(Box<StressRun>),
}

struct EngineThread {
    rings: Rings,
    tel: Arc<Telemetry>,
    spec: StartSpec,
    run: Run,
    run_id: u64,
    last_frame: Instant,
    window_start: Instant,
    window_records: u64,
    msgs_per_s: f64,
    peaks: [u32; 5],
    /// The stress run's log reached the cap.
    capped_seen: bool,
}

/// Spawn the engine thread, optionally starting a run at once.
pub fn spawn(
    rings: Rings,
    tel: Arc<Telemetry>,
    autostart: Option<StartSpec>,
) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("engine".into())
        .spawn(move || {
            crate::qos::prefer_performance_cores();
            let now = Instant::now();
            let mut t = EngineThread {
                rings,
                tel,
                spec: autostart.unwrap_or_default(),
                run: Run::Idle,
                run_id: 0,
                last_frame: now,
                window_start: now,
                window_records: 0,
                msgs_per_s: 0.0,
                peaks: [0; 5],
                capped_seen: false,
            };
            if let Some(spec) = autostart {
                t.start_run(spec);
            }
            t.run();
        })
        .expect("spawn engine thread")
}

impl EngineThread {
    fn run(&mut self) {
        loop {
            while let Ok(c) = self.rings.cmd.pop() {
                if c == Command::Shutdown {
                    self.stop_run();
                    self.frame();
                    let mut p = 0;
                    let w = &self.tel.waits[W_LOG];
                    push_wait(&mut self.rings.log, LogMsg::Exit, &mut p, w);
                    push_wait(&mut self.rings.standby, StandbyMsg::Exit, &mut p, w);
                    push_wait(&mut self.rings.ui, UiMsg::Exit, &mut p, w);
                    return;
                }
                self.command(c);
            }
            let busy = match &self.run {
                Run::Idle => false,
                Run::Market(_) => {
                    self.market_step();
                    true
                }
                Run::Stress(_) => self.stress_step(),
            };
            if self.last_frame.elapsed() >= FRAME {
                self.tel.record_thread_cpu(T_ENGINE);
                self.frame();
            }
            match &self.run {
                // Paced by the virtual clock: nothing to do until wall time moves on.
                Run::Market(m) => {
                    let idle = m.live.done() || (self.spec.speed_milli == 0 && m.seek.is_none());
                    thread::sleep(Duration::from_micros(if idle { 5_000 } else { 1_000 }));
                }
                // The hot path: spin while there is work, nap briefly when the ring is empty.
                Run::Stress(s) => {
                    if !busy {
                        if s.ended {
                            thread::sleep(Duration::from_millis(5));
                        } else {
                            thread::sleep(Duration::from_micros(20));
                        }
                    }
                }
                Run::Idle => thread::sleep(Duration::from_millis(5)),
            }
        }
    }

    fn command(&mut self, c: Command) {
        match c {
            Command::Start(spec) => {
                self.stop_run();
                self.start_run(spec);
            }
            Command::Restart => {
                if !matches!(self.run, Run::Idle) {
                    self.stop_run();
                    self.start_run(self.spec);
                }
            }
            Command::Stop => {
                self.stop_run();
                self.run = Run::Idle;
            }
            Command::Control(ctl) => {
                if let Run::Market(m) = &mut self.run {
                    match ctl {
                        Control::Speed(s) => {
                            self.spec.speed_milli = s;
                            if s > 0 {
                                m.view_speed = s;
                            }
                        }
                        Control::Rule(r) => self.spec.rule = r,
                        Control::BlockMs(b) => self.spec.block_ms = b,
                        Control::Load(_) | Control::Stop => {}
                    }
                    if ctl == Control::Stop {
                        self.stop_run();
                    } else if !m.live.done() {
                        m.live.control(ctl);
                    }
                }
            }
            Command::StressRate(r) => {
                if let Run::Stress(s) = &mut self.run {
                    self.spec.rate = r;
                    if let Some(p) = &s.producer {
                        p.rate.store(r, Relaxed);
                    }
                }
            }
            Command::Skip => {
                if let Run::Market(m) = &mut self.run {
                    let now = m.live.clock();
                    m.seek = m.marks.iter().copied().find(|&t| t > now + 1_000_000_000);
                }
            }
            Command::Shutdown => {}
        }
    }

    fn start_run(&mut self, spec: StartSpec) {
        self.spec = spec;
        self.run_id += 1;
        self.capped_seen = false;
        let (info, run) = if spec.stress {
            let (tx, rx) = rtrb::RingBuffer::new(INPUT_RING);
            let start = Instant::now();
            let producer =
                stress::spawn_producer(tx, spec.seed, spec.rate, start, self.tel.clone());
            let info = RunInfo {
                run_id: self.run_id,
                stress: true,
                scenario: "stress".into(),
                seed: spec.seed,
                config: format!("rate_per_s = {}\n", spec.rate),
            };
            let run = Run::Stress(Box::new(StressRun {
                engine: Engine::new(),
                input: rx,
                producer: Some(producer),
                start,
                seq: 0,
                fills: 0,
                hasher: FillsHasher::default(),
                out: Vec::with_capacity(64),
                e2e: window_hist(),
                batch: window_hist(),
                batch_msgs: 0,
                batch_count: 0,
                window: start,
                last: StressFrame {
                    target_rate: spec.rate,
                    ..Default::default()
                },
                stopped: false,
                ended: false,
            }));
            (info, run)
        } else {
            let cfg = scenario_cfg(&spec);
            let live = Live::new(&cfg, spec.seed).expect("valid scenario");
            // Land a few seconds before each scheduled jump, a minute before the close and
            // 30 s before the reopen.
            let s = 1_000_000_000u64;
            let mut marks: Vec<u64> = cfg
                .price
                .scheduled_jumps
                .iter()
                .map(|j| ((j.at_s * 1e9) as u64).saturating_sub(5 * s))
                .collect();
            if let Some(w) = &cfg.weekend {
                marks.push(((w.close_at_s * 1e9) as u64).saturating_sub(60 * s));
                marks.push(((w.reopen_at_s * 1e9) as u64).saturating_sub(30 * s));
            }
            marks.sort_unstable();
            let info = RunInfo {
                run_id: self.run_id,
                stress: false,
                scenario: cfg.name.clone(),
                seed: spec.seed,
                config: cfg.to_toml(),
            };
            let run = Run::Market(Box::new(MarketRun {
                live,
                published: 0,
                ended: false,
                marks,
                seek: None,
                carry: 0,
                last_tick: Instant::now(),
                mids: Vec::new(),
                view_speed: spec.speed_milli.max(1),
            }));
            (info, run)
        };
        self.run = run;
        let w = &self.tel.waits[W_LOG];
        push_wait(
            &mut self.rings.log,
            LogMsg::Start(Arc::new(info)),
            &mut self.peaks[2],
            w,
        );
        let w = &self.tel.waits[W_STANDBY];
        push_wait(
            &mut self.rings.standby,
            StandbyMsg::Reset(self.run_id),
            &mut self.peaks[3],
            w,
        );
        self.window_start = Instant::now();
        self.window_records = 0;
        self.msgs_per_s = 0.0;
    }

    /// Stop the current run where it is (a logged `Stop`), publish it and close its log.
    /// The run's state stays on screen until the next start.
    fn stop_run(&mut self) {
        match &mut self.run {
            Run::Idle => {}
            Run::Market(m) => {
                if !m.live.done() {
                    m.live.control(Control::Stop);
                }
                self.market_publish();
                self.market_end();
            }
            Run::Stress(_) => {
                // The producer writes its `Stop` record last; drain until it comes through.
                if let Run::Stress(s) = &mut self.run {
                    if let Some(p) = &s.producer {
                        p.stop.store(true, Relaxed);
                    }
                }
                while matches!(&self.run, Run::Stress(s) if !s.stopped) {
                    if !self.stress_step() {
                        thread::sleep(Duration::from_micros(50));
                    }
                }
                self.stress_end();
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Market
    // ------------------------------------------------------------------------------------------

    /// Advance virtual time by the wall time since the last step × the playback speed, then
    /// publish the new records.
    fn market_step(&mut self) {
        let speed = self.spec.speed_milli;
        let Run::Market(m) = &mut self.run else {
            return;
        };
        let now = Instant::now();
        let dt = now - m.last_tick;
        m.last_tick = now;
        let before = m.live.records().len();
        if let Some(target) = m.seek {
            // As fast as the simulation goes, a step budget at a time so frames keep flowing.
            while m.live.clock() < target && !m.live.done() && now.elapsed() < STEP_BUDGET {
                let t = target.min(m.live.clock() + 1_000_000_000);
                m.live.advance_to(t);
            }
            if m.live.clock() >= target || m.live.done() {
                m.seek = None;
            }
        } else if !m.live.done() && speed > 0 {
            // After a stall, don't jump more than 100 ms of wall time at once.
            let dt_ns = dt.as_nanos().min(100_000_000);
            let adv_milli = dt_ns * speed as u128 + m.carry;
            let adv = (adv_milli / 1_000) as u64;
            m.carry = adv_milli % 1_000;
            let target = m.live.clock().saturating_add(adv);
            // Chunks of about 2 ms of wall time each, so a slow step can stop early.
            let chunk = (speed * 2_000).max(1);
            let mut t = m.live.clock();
            while t < target && !m.live.done() {
                t = target.min(t + chunk);
                m.live.advance_to(t);
                if now.elapsed() > STEP_BUDGET {
                    m.carry = 0;
                    break;
                }
            }
        }
        let mid = match (m.live.best_bid(), m.live.best_ask()) {
            (Some((b, _)), Some((a, _))) => b + a,
            _ => i64::MIN,
        };
        if m.mids.last().is_none_or(|&(_, last)| last != mid) {
            m.mids.push((m.live.clock(), mid));
        }
        self.window_records += (m.live.records().len() - before) as u64;
        self.market_publish();
        if matches!(&self.run, Run::Market(m) if m.live.done()) {
            self.market_end();
        }
    }

    /// Send every new record to the log writer and the standby.
    fn market_publish(&mut self) {
        let Run::Market(m) = &mut self.run else {
            return;
        };
        let recs = &m.live.records()[m.published..];
        for r in recs {
            push_wait(
                &mut self.rings.log,
                LogMsg::Record(*r),
                &mut self.peaks[2],
                &self.tel.waits[W_LOG],
            );
            push_wait(
                &mut self.rings.standby,
                StandbyMsg::Record(*r),
                &mut self.peaks[3],
                &self.tel.waits[W_STANDBY],
            );
        }
        m.published += recs.len();
    }

    fn market_end(&mut self) {
        let Run::Market(m) = &mut self.run else {
            return;
        };
        if m.ended {
            return;
        }
        m.ended = true;
        let msg = LogMsg::End {
            run_id: self.run_id,
            records: m.published as u64,
            fills_hash: hash_bytes(&m.live.fills_hash()),
        };
        push_wait(
            &mut self.rings.log,
            msg,
            &mut self.peaks[2],
            &self.tel.waits[W_LOG],
        );
    }

    /// The chart window: about a minute of wall time at the current speed, at least 20 s of
    /// virtual time, at most the run. It trails the clock, and starts at 0 until it fills.
    fn chart_window(m: &MarketRun) -> (u64, u64) {
        let end = m.live.end_ns();
        let w = ((m.view_speed as u128 * 60_000_000) as u64).clamp(20_000_000_000, end.max(1));
        let t0 = m.live.clock().saturating_sub(w).min(end.saturating_sub(w));
        (t0, t0 + w)
    }

    fn market_frame(m: &MarketRun) -> MarketFrame {
        let live = &m.live;
        let pnl_all = live.pnl();
        let mut pnl = [0i128; 5];
        let mut transfer = [[0i128; 5]; 5];
        let mut fees = [(0i128, 0i128); 5];
        for (i, c) in AccountClass::ALL.iter().enumerate() {
            pnl[i] = pnl_all.get(*c);
            fees[i] = live.fees_paid(*c);
            for (j, d) in AccountClass::ALL.iter().enumerate() {
                transfer[i][j] = live.transfer(*c, *d);
            }
        }

        let mut races = [RaceRow::default(); RACES];
        let all = live.races();
        let tail = &all[all.len().saturating_sub(RACES)..];
        let records = live.records();
        for (dst, r) in races.iter_mut().zip(tail) {
            let qty = records
                .get(r.order_id as usize - 1)
                .and_then(|rec| match rec.kind {
                    MsgKind::New { qty_lots, .. } => Some(qty_lots),
                    _ => None,
                })
                .unwrap_or(0);
            *dst = RaceRow {
                start_t_ns: r.start_t_ns,
                end_t_ns: r.end_t_ns.unwrap_or(0),
                staleness: r.staleness,
                qty,
                outcome: match r.outcome {
                    RaceOutcome::LpWon => Outcome::Lp,
                    RaceOutcome::SniperWon => Outcome::Sniper,
                    RaceOutcome::OtherFilled => Outcome::Other,
                    RaceOutcome::Unresolved => Outcome::Open,
                },
            };
        }

        // The chart: the true price and the book mid at the end of each of CHART equal slices
        // of the window, up to the clock. Sampling the source (the true path, every mid change)
        // at a resolution set by the window means no zoom level crowds steps together.
        let (t0, t1) = Self::chart_window(m);
        let path = live.price_path();
        let mut pi = path.partition_point(|p| p.0 <= t0).saturating_sub(1);
        let mut mi = m.mids.partition_point(|p| p.0 <= t0).saturating_sub(1);
        let mut chart = [(0i64, i64::MIN); CHART];
        let mut n_chart = 0;
        let clock = live.clock();
        for (k, slot) in chart.iter_mut().enumerate() {
            let te = t0 + (t1 - t0) * (k as u64 + 1) / CHART as u64;
            let te = te.min(clock);
            while pi + 1 < path.len() && path[pi + 1].0 <= te {
                pi += 1;
            }
            while mi + 1 < m.mids.len() && m.mids[mi + 1].0 <= te {
                mi += 1;
            }
            let px = path.get(pi).map_or(live.true_px(), |p| p.1);
            let mid = m
                .mids
                .get(mi)
                .filter(|p| p.0 <= te)
                .map_or(i64::MIN, |p| p.1);
            *slot = (px, mid);
            n_chart = k + 1;
            if te >= clock {
                break;
            }
        }

        MarketFrame {
            weekend: live.weekend(),
            hidden: live.hidden(),
            seeking: m.seek,
            true_px: live.true_px(),
            rule: live.rule(),
            load: live.load(),
            block: live.block(),
            counts: live.counts(),
            pnl,
            transfer,
            fees,
            races,
            n_races: tail.len() as u8,
            race_stats: live.race_stats(),
            chart_t0: t0,
            chart_t1: t1,
            chart,
            n_chart: n_chart as u16,
        }
    }

    // ------------------------------------------------------------------------------------------
    // Stress
    // ------------------------------------------------------------------------------------------

    /// Take up to a batch from the input ring: number, log, match, hand to the standby.
    /// Returns whether there was anything to do.
    fn stress_step(&mut self) -> bool {
        let Run::Stress(s) = &mut self.run else {
            return false;
        };
        if s.stopped {
            return false;
        }
        let t = Instant::now();
        let mut n = 0;
        let mut sent = [0u64; 64];
        let mut n_sent = 0;
        while n < BATCH {
            let Ok(m) = s.input.pop() else { break };
            n += 1;
            s.seq += 1;
            let rec = LogRecord {
                seq: s.seq,
                t_ns: m.t_ns,
                block: s.seq - 1,
                agent: m.agent,
                kind: m.kind,
            };
            // Logged before it is matched.
            push_wait(
                &mut self.rings.log,
                LogMsg::Record(rec),
                &mut self.peaks[2],
                &self.tel.waits[W_LOG],
            );
            s.engine.on_message_into(&rec, &mut s.out);
            for ev in &s.out {
                if matches!(ev.kind, EventKind::Fill { .. }) {
                    s.fills += 1;
                    s.hasher.update(ev.seq, &ev.kind);
                }
            }
            push_wait(
                &mut self.rings.standby,
                StandbyMsg::Record(rec),
                &mut self.peaks[3],
                &self.tel.waits[W_STANDBY],
            );
            // Sample end-to-end latency on up to 64 messages a batch.
            if n_sent < sent.len() && (n % 128 == 1) {
                sent[n_sent] = m.sent_ns;
                n_sent += 1;
            }
            if let MsgKind::Config { param, value } = m.kind {
                if param == PARAM_STRESS_RATE {
                    s.last.target_rate = value;
                } else if Control::decode(param, value) == Some(Control::Stop) {
                    s.stopped = true;
                    break;
                }
            }
        }
        if n == 0 {
            return false;
        }
        let done = Instant::now();
        let done_ns = done.duration_since(s.start).as_nanos() as u64;
        for &x in &sent[..n_sent] {
            s.e2e.saturating_record(done_ns.saturating_sub(x).max(1));
        }
        s.batch.saturating_record((done - t).as_nanos() as u64);
        s.batch_msgs += n as u64;
        s.batch_count += 1;
        self.window_records += n as u64;
        // The log reached its cap: stop the producer; the run ends when its `Stop` arrives.
        if !self.capped_seen && self.tel.log_capped.load(Relaxed) != 0 {
            self.capped_seen = true;
            if let Some(p) = &s.producer {
                p.stop.store(true, Relaxed);
            }
        }
        if s.stopped {
            self.stress_end();
        }
        true
    }

    fn stress_end(&mut self) {
        let Run::Stress(s) = &mut self.run else {
            return;
        };
        if s.ended {
            return;
        }
        s.ended = true;
        if let Some(p) = s.producer.take() {
            let _ = p.thread.join();
        }
        let msg = LogMsg::End {
            run_id: self.run_id,
            records: s.seq,
            fills_hash: hash_bytes(&s.hasher.clone().finish()),
        };
        push_wait(
            &mut self.rings.log,
            msg,
            &mut self.peaks[2],
            &self.tel.waits[W_LOG],
        );
    }

    // ------------------------------------------------------------------------------------------
    // Frames
    // ------------------------------------------------------------------------------------------

    fn ring_stat(used: usize, cap: usize, peak: &mut u32) -> RingStat {
        let s = RingStat {
            used: used as u32,
            cap: cap as u32,
            peak: (*peak).max(used as u32),
        };
        *peak = 0;
        s
    }

    fn frame(&mut self) {
        self.last_frame = Instant::now();
        let w = self.window_start.elapsed();
        if w >= Duration::from_secs(1) {
            self.msgs_per_s = self.window_records as f64 / w.as_secs_f64();
            self.window_start = Instant::now();
            self.window_records = 0;
        }
        let mut bids = [(0, 0); DEPTH];
        let mut asks = [(0, 0); DEPTH];
        let (b, a) = match &self.run {
            Run::Idle => (Vec::new(), Vec::new()),
            Run::Market(m) => (m.live.depth(Side::Buy), m.live.depth(Side::Sell)),
            Run::Stress(s) => (s.engine.depth(Side::Buy), s.engine.depth(Side::Sell)),
        };
        for (dst, src) in bids.iter_mut().zip(&b) {
            *dst = *src;
        }
        for (dst, src) in asks.iter_mut().zip(&a) {
            *dst = *src;
        }

        let input = match &self.run {
            Run::Stress(s) => (s.input.slots(), s.input.buffer().capacity()),
            _ => (0, INPUT_RING),
        };
        let rings = [
            Self::ring_stat(
                self.rings.cmd.slots(),
                self.rings.cmd.buffer().capacity(),
                &mut self.peaks[0],
            ),
            Self::ring_stat(input.0, input.1, &mut self.peaks[1]),
            Self::ring_stat(
                self.rings.log.buffer().capacity() - self.rings.log.slots(),
                self.rings.log.buffer().capacity(),
                &mut self.peaks[2],
            ),
            Self::ring_stat(
                self.rings.standby.buffer().capacity() - self.rings.standby.slots(),
                self.rings.standby.buffer().capacity(),
                &mut self.peaks[3],
            ),
            Self::ring_stat(
                self.rings.ui.buffer().capacity() - self.rings.ui.slots(),
                self.rings.ui.buffer().capacity(),
                &mut self.peaks[4],
            ),
        ];

        let mut frame = Frame {
            state: State::Setup,
            run_id: self.run_id,
            spec: self.spec,
            clock_ns: 0,
            end_ns: 0,
            done: true,
            stopped: false,
            records: 0,
            fills: 0,
            msgs_per_s: self.msgs_per_s,
            bids,
            asks,
            n_bids: b.len().min(DEPTH) as u8,
            n_asks: a.len().min(DEPTH) as u8,
            levels: (b.len() as u32, a.len() as u32),
            rings,
            fills_hash: [b'0'; 64],
            market: MarketFrame::default(),
            stress: StressFrame::default(),
        };
        let mut checkpoint = None;
        match &mut self.run {
            Run::Idle => {}
            Run::Market(m) => {
                let live = &m.live;
                frame.state = State::Market;
                frame.clock_ns = live.clock();
                frame.end_ns = live.end_ns();
                frame.done = live.done();
                frame.stopped = live.stopped();
                frame.records = m.published as u64;
                frame.fills = live.counts().fills;
                frame.fills_hash = hash_bytes(&live.fills_hash());
                frame.market = Self::market_frame(m);
                checkpoint = Some((m.published as u64, frame.fills_hash));
            }
            Run::Stress(s) => {
                frame.state = State::Stress;
                frame.clock_ns = s.start.elapsed().as_nanos() as u64;
                frame.done = s.ended;
                frame.stopped = s.ended;
                frame.records = s.seq;
                frame.fills = s.fills;
                frame.fills_hash = hash_bytes(&s.hasher.clone().finish());
                checkpoint = Some((s.seq, frame.fills_hash));
                if s.window.elapsed() >= Duration::from_secs(1) {
                    let q = |h: &Histogram<u64>, p| {
                        if h.is_empty() {
                            0
                        } else {
                            h.value_at_quantile(p)
                        }
                    };
                    s.last.e2e_p50 = q(&s.e2e, 0.5);
                    s.last.e2e_p99 = q(&s.e2e, 0.99);
                    s.last.e2e_max = if s.e2e.is_empty() { 0 } else { s.e2e.max() };
                    s.last.batch_p50_ns = q(&s.batch, 0.5);
                    s.last.batch_p99_ns = q(&s.batch, 0.99);
                    s.last.batch_mean = if s.batch_count > 0 {
                        s.batch_msgs as f64 / s.batch_count as f64
                    } else {
                        0.0
                    };
                    s.e2e.reset();
                    s.batch.reset();
                    s.batch_msgs = 0;
                    s.batch_count = 0;
                    s.window = Instant::now();
                }
                s.last.orders_resting = s.engine.order_count() as u64;
                frame.stress = s.last;
            }
        }
        // The standby checks itself against the primary at this point.
        if let Some((records, fills_hash)) = checkpoint {
            push_wait(
                &mut self.rings.standby,
                StandbyMsg::Checkpoint {
                    records,
                    fills_hash,
                },
                &mut self.peaks[3],
                &self.tel.waits[W_STANDBY],
            );
        }
        // The UI only needs the newest frame: drop it if the bridge is behind.
        if self.rings.ui.push(UiMsg::Frame(frame)).is_err() {
            self.tel.ui_drops.fetch_add(1, Relaxed);
        }
    }
}
