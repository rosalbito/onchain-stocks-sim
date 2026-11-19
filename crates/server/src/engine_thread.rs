//! The engine thread: the single writer (architecture section 4).
//!
//! It owns the simulation, and with it the book. Nothing else touches either. It paces virtual
//! time from the wall clock and the playback speed, applies controls from the control ring at the
//! current virtual time (each one becomes a logged `Config` record), and publishes over one rtrb
//! ring per reader:
//!
//! - the log writer: every record, in `seq` order (waits if full: the log never drops one)
//! - the standby: every record plus a checkpoint per frame (waits if full)
//! - the UI bridge: one fixed-size `Frame` about 30 times a second (dropped if full: the UI only
//!   needs the newest)
//!
//! Every ring item is a fixed-size value; only a run start carries an `Arc` with the header.

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use engine::{LogRecord, Side};
use metrics::{AccountClass, RaceOutcome, RaceStats};
use rtrb::{Consumer, Producer};
use sim::{Control, Counts, Live, OrderingRule, Scenario};

/// The scenarios the dashboard can switch between, embedded so the binary runs from anywhere.
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

/// A command from the edge to the engine thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    /// A live change to the running simulation, logged as a `Config` record.
    Control(Control),
    /// Stop the current run (logged as `Stop`) and start this scenario.
    Scenario(u8),
    /// Stop the current run and start it again from t = 0, same scenario and seed.
    Restart,
    /// Stop the current run and start it again with this seed.
    Seed(u64),
    /// Fast-forward to just before the next moment worth watching (a scheduled news jump, the
    /// close, the reopen). Pacing only: the run is the same as if played through, so nothing is
    /// logged.
    Skip,
    Shutdown,
}

/// The header of a run, sent once to the log writer.
#[derive(Debug)]
pub struct RunInfo {
    pub run_id: u64,
    pub scenario: String,
    pub seed: u64,
    pub config_toml: String,
}

pub enum LogMsg {
    Start(Arc<RunInfo>),
    Record(LogRecord),
    /// The run is over: write the log, then check that it replays to this fills hash.
    End {
        run_id: u64,
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

pub const DEPTH: usize = 12;
pub const RACES: usize = 10;
/// True-price samples per frame, at fixed bucket boundaries (see `Frame::samples`).
pub const SAMPLES: usize = 128;
/// The price chart's resolution: a run is cut into this many buckets.
pub const BUCKETS: u64 = 2_400;

/// The chart bucket for a run of this length.
pub fn bucket_ns(end_ns: u64) -> u64 {
    (end_ns / BUCKETS).max(10_000_000)
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RaceRow {
    pub order_id: u64,
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

pub const RING_NAMES: [&str; 4] = ["control", "log", "standby", "ui"];

/// One frame for the UI. Fixed size, so the ring holds values, not pointers.
#[derive(Clone, Copy)]
pub struct Frame {
    pub run_id: u64,
    pub scenario: u8,
    pub seed: u64,
    pub clock_ns: u64,
    pub end_ns: u64,
    pub weekend: Option<(u64, u64)>,
    pub hidden: bool,
    pub done: bool,
    pub stopped: bool,
    pub true_px: i64,
    pub visible_px: i64,
    /// The true price at each bucket boundary crossed since the last frame (thinned to at most
    /// `SAMPLES`), so a fast-forward still draws the path it went through.
    pub samples: [(u64, i64); SAMPLES],
    pub n_samples: u16,
    pub bids: [(i64, u64); DEPTH],
    pub asks: [(i64, u64); DEPTH],
    pub n_bids: u8,
    pub n_asks: u8,
    /// Levels beyond the ones sent.
    pub more_bids: u32,
    pub more_asks: u32,
    /// Cash units, in `AccountClass::ALL` order, marked to the true price.
    pub pnl: [i128; 5],
    pub priority_fees: i128,
    pub taker_fees: i128,
    /// The latest races, newest last.
    pub races: [RaceRow; RACES],
    pub n_races: u8,
    pub race_stats: RaceStats,
    pub rule: OrderingRule,
    pub block_ms: u64,
    pub load: u32,
    pub speed_milli: u64,
    /// Fast-forwarding to this virtual time.
    pub seeking: Option<u64>,
    pub block: u64,
    pub counts: Counts,
    /// Records sequenced per wall second, over the last second.
    pub msgs_per_s: f64,
    /// Wall time of the simulation step per record (agents, sequencer and engine together).
    pub step_ns_per_msg: f64,
    pub rings: [RingStat; 4],
    pub fills_hash: [u8; 64],
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

pub struct Start {
    pub scenario: u8,
    pub seed: u64,
    pub speed_milli: u64,
    pub rule: Option<OrderingRule>,
    pub block_ms: Option<u64>,
}

const FRAME: Duration = Duration::from_millis(33);
/// At most this much wall time per simulation step; if the simulation can't keep up with the
/// playback speed, the speed drops instead of the frames.
const STEP_BUDGET: Duration = Duration::from_millis(20);

struct EngineThread {
    rings: Rings,
    live: Live,
    run_id: u64,
    scenario: u8,
    seed: u64,
    rule: OrderingRule,
    block_ms: u64,
    speed_milli: u64,
    published: usize,
    ended: bool,
    /// Skip targets for this run: (virtual time to land at), ascending.
    marks: Vec<u64>,
    seek: Option<u64>,
    /// Price path entries already sampled, the price then, and the next boundary to sample.
    path_idx: usize,
    path_px: i64,
    next_sample: u64,
    carry: u128,
    last_tick: Instant,
    last_frame: Instant,
    window_start: Instant,
    window_records: u64,
    window_step: Duration,
    msgs_per_s: f64,
    step_ns_per_msg: f64,
    peaks: [u32; 4],
}

/// Push and wait while the ring is full: the log and the standby never drop a record.
fn push_wait<T>(p: &mut Producer<T>, mut v: T, peak: &mut u32) {
    loop {
        match p.push(v) {
            Ok(()) => break,
            Err(rtrb::PushError::Full(back)) => {
                v = back;
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

fn scenario_cfg(idx: u8, rule: OrderingRule, block_ms: u64) -> Scenario {
    let mut cfg = Scenario::from_toml(SCENARIOS[idx as usize].1).expect("embedded scenario");
    cfg.sequencer.rule = rule;
    cfg.sequencer.block_ms = block_ms;
    cfg
}

/// Spawn the engine thread.
pub fn spawn(rings: Rings, start: Start) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("engine".into())
        .spawn(move || {
            crate::qos::prefer_performance_cores();
            let base = Scenario::from_toml(SCENARIOS[start.scenario as usize].1).unwrap();
            let rule = start.rule.unwrap_or(base.sequencer.rule);
            let block_ms = start.block_ms.unwrap_or(base.sequencer.block_ms);
            let cfg = scenario_cfg(start.scenario, rule, block_ms);
            let now = Instant::now();
            let mut t = EngineThread {
                rings,
                live: Live::new(&cfg, start.seed).expect("valid scenario"),
                run_id: 0,
                scenario: start.scenario,
                seed: start.seed,
                rule,
                block_ms,
                speed_milli: start.speed_milli,
                published: 0,
                ended: false,
                marks: Vec::new(),
                seek: None,
                path_idx: 0,
                path_px: 0,
                next_sample: 0,
                carry: 0,
                last_tick: now,
                last_frame: now,
                window_start: now,
                window_records: 0,
                window_step: Duration::ZERO,
                msgs_per_s: 0.0,
                step_ns_per_msg: 0.0,
                peaks: [0; 4],
            };
            t.start_run();
            t.run();
        })
        .expect("spawn engine thread")
}

impl EngineThread {
    fn run(&mut self) {
        loop {
            let mut shutdown = false;
            while let Ok(c) = self.rings.cmd.pop() {
                if c == Command::Shutdown {
                    shutdown = true;
                    break;
                }
                self.command(c);
            }
            if shutdown {
                self.stop_run();
                self.frame();
                let mut p = 0;
                push_wait(&mut self.rings.log, LogMsg::Exit, &mut p);
                push_wait(&mut self.rings.standby, StandbyMsg::Exit, &mut p);
                push_wait(&mut self.rings.ui, UiMsg::Exit, &mut p);
                return;
            }
            self.step();
            self.publish();
            if self.live.done() && !self.ended {
                self.end_run();
            }
            if self.last_frame.elapsed() >= FRAME {
                self.frame();
            }
            // Paced by the virtual clock: nothing to do until wall time moves on.
            let idle = self.live.done() || (self.speed_milli == 0 && self.seek.is_none());
            thread::sleep(Duration::from_micros(if idle { 5_000 } else { 1_000 }));
        }
    }

    fn command(&mut self, c: Command) {
        match c {
            Command::Control(ctl) => {
                match ctl {
                    Control::Speed(s) => self.speed_milli = s,
                    Control::Rule(r) => self.rule = r,
                    Control::BlockMs(b) => self.block_ms = b,
                    Control::Load(_) | Control::Stop => {}
                }
                if ctl == Control::Stop {
                    self.stop_run();
                } else if !self.live.done() {
                    self.live.control(ctl);
                }
            }
            Command::Scenario(i) if (i as usize) < SCENARIOS.len() => {
                self.stop_run();
                self.scenario = i;
                // A new scenario starts from its own rule and B.
                let base = Scenario::from_toml(SCENARIOS[i as usize].1).unwrap();
                self.rule = base.sequencer.rule;
                self.block_ms = base.sequencer.block_ms;
                self.start_run();
            }
            Command::Scenario(_) => {}
            Command::Restart => {
                self.stop_run();
                self.start_run();
            }
            Command::Seed(s) => {
                self.stop_run();
                self.seed = s;
                self.start_run();
            }
            Command::Skip => {
                let now = self.live.clock();
                self.seek = self
                    .marks
                    .iter()
                    .copied()
                    .find(|&m| m > now + 1_000_000_000);
            }
            Command::Shutdown => {}
        }
    }

    /// Start a run with the current scenario, seed, rule and B (the latter two in the log's
    /// config header, not as `Config` records, so a run without live changes is the batch run).
    fn start_run(&mut self) {
        let cfg = scenario_cfg(self.scenario, self.rule, self.block_ms);
        self.live = Live::new(&cfg, self.seed).expect("valid scenario");
        self.run_id += 1;
        self.published = 0;
        self.ended = false;
        self.seek = None;
        self.path_idx = 0;
        self.path_px = cfg.price.start_px_ticks;
        self.next_sample = 0;
        // Land a few seconds before each scheduled jump, a minute before the close and 30 s
        // before the reopen.
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
        self.marks = marks;
        self.carry = 0;
        self.last_tick = Instant::now();
        let info = Arc::new(RunInfo {
            run_id: self.run_id,
            scenario: cfg.name.clone(),
            seed: self.seed,
            config_toml: cfg.to_toml(),
        });
        push_wait(&mut self.rings.log, LogMsg::Start(info), &mut self.peaks[1]);
        push_wait(
            &mut self.rings.standby,
            StandbyMsg::Reset(self.run_id),
            &mut self.peaks[2],
        );
    }

    /// Stop the current run where it is (a logged `Stop`), publish it and close its log.
    fn stop_run(&mut self) {
        if !self.live.done() {
            self.live.control(Control::Stop);
        }
        self.publish();
        self.end_run();
    }

    fn end_run(&mut self) {
        if self.ended {
            return;
        }
        self.ended = true;
        let fills_hash = hash_bytes(&self.live.fills_hash());
        push_wait(
            &mut self.rings.log,
            LogMsg::End {
                run_id: self.run_id,
                fills_hash,
            },
            &mut self.peaks[1],
        );
    }

    /// Advance virtual time by the wall time since the last step × the playback speed.
    fn step(&mut self) {
        let now = Instant::now();
        let dt = now - self.last_tick;
        self.last_tick = now;
        if let Some(target) = self.seek {
            // As fast as the simulation goes, a step budget at a time so frames keep flowing.
            let before = self.live.records().len();
            while self.live.clock() < target && !self.live.done() && now.elapsed() < STEP_BUDGET {
                let t = target.min(self.live.clock() + 1_000_000_000);
                self.live.advance_to(t);
            }
            if self.live.clock() >= target || self.live.done() {
                self.seek = None;
            }
            self.window_records += (self.live.records().len() - before) as u64;
            self.window_step += now.elapsed();
            return;
        }
        if self.live.done() || self.speed_milli == 0 {
            return;
        }
        // After a stall, don't jump more than 100 ms of wall time at once.
        let dt_ns = dt.as_nanos().min(100_000_000);
        let adv_milli = dt_ns * self.speed_milli as u128 + self.carry;
        let adv = (adv_milli / 1_000) as u64;
        self.carry = adv_milli % 1_000;
        let target = self.live.clock().saturating_add(adv);
        // Chunks of about 2 ms of wall time each, so a slow step can stop early.
        let chunk = (self.speed_milli * 2_000).max(1);
        let before = self.live.records().len();
        let mut t = self.live.clock();
        while t < target && !self.live.done() {
            t = target.min(t + chunk);
            self.live.advance_to(t);
            if now.elapsed() > STEP_BUDGET {
                // Behind: drop the debt rather than let it grow.
                self.carry = 0;
                break;
            }
        }
        self.window_records += (self.live.records().len() - before) as u64;
        self.window_step += now.elapsed();
    }

    /// Send every new record to the log writer and the standby.
    fn publish(&mut self) {
        let recs = &self.live.records()[self.published..];
        for r in recs {
            push_wait(&mut self.rings.log, LogMsg::Record(*r), &mut self.peaks[1]);
            push_wait(
                &mut self.rings.standby,
                StandbyMsg::Record(*r),
                &mut self.peaks[2],
            );
        }
        self.published += recs.len();
    }

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
            self.step_ns_per_msg = if self.window_records > 0 {
                self.window_step.as_nanos() as f64 / self.window_records as f64
            } else {
                0.0
            };
            self.window_start = Instant::now();
            self.window_records = 0;
            self.window_step = Duration::ZERO;
        }
        let live = &self.live;
        let fills_hash = hash_bytes(&live.fills_hash());

        // The true price at every bucket boundary up to now.
        let bucket = bucket_ns(live.end_ns());
        let path = live.price_path();
        let mut crossed = Vec::new();
        while self.next_sample <= live.clock() {
            while let Some(&(t, p)) = path.get(self.path_idx) {
                if t > self.next_sample {
                    break;
                }
                self.path_px = p;
                self.path_idx += 1;
            }
            crossed.push((self.next_sample, self.path_px));
            self.next_sample += bucket;
        }
        let stride = crossed.len().div_ceil(SAMPLES).max(1);
        let mut samples = [(0, 0); SAMPLES];
        let mut n_samples = 0;
        for (i, s) in crossed.iter().enumerate() {
            if (i % stride == 0 || i + 1 == crossed.len()) && n_samples < SAMPLES {
                samples[n_samples] = *s;
                n_samples += 1;
            }
        }

        let mut bids = [(0, 0); DEPTH];
        let mut asks = [(0, 0); DEPTH];
        let (b, a) = (live.depth(Side::Buy), live.depth(Side::Sell));
        for (dst, src) in bids.iter_mut().zip(&b) {
            *dst = *src;
        }
        for (dst, src) in asks.iter_mut().zip(&a) {
            *dst = *src;
        }

        let pnl_all = live.pnl();
        let mut pnl = [0i128; 5];
        for (i, c) in AccountClass::ALL.iter().enumerate() {
            pnl[i] = pnl_all.get(*c);
        }

        let mut races = [RaceRow::default(); RACES];
        let all = live.races();
        let tail = &all[all.len().saturating_sub(RACES)..];
        let records = live.records();
        for (dst, r) in races.iter_mut().zip(tail) {
            let qty = records
                .get(r.order_id as usize - 1)
                .and_then(|rec| match rec.kind {
                    engine::MsgKind::New { qty_lots, .. } => Some(qty_lots),
                    _ => None,
                })
                .unwrap_or(0);
            *dst = RaceRow {
                order_id: r.order_id,
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

        let rings = [
            Self::ring_stat(
                self.rings.cmd.slots(),
                self.rings.cmd.buffer().capacity(),
                &mut self.peaks[0],
            ),
            Self::ring_stat(
                self.rings.log.buffer().capacity() - self.rings.log.slots(),
                self.rings.log.buffer().capacity(),
                &mut self.peaks[1],
            ),
            Self::ring_stat(
                self.rings.standby.buffer().capacity() - self.rings.standby.slots(),
                self.rings.standby.buffer().capacity(),
                &mut self.peaks[2],
            ),
            Self::ring_stat(
                self.rings.ui.buffer().capacity() - self.rings.ui.slots(),
                self.rings.ui.buffer().capacity(),
                &mut self.peaks[3],
            ),
        ];

        let frame = Frame {
            run_id: self.run_id,
            scenario: self.scenario,
            seed: self.seed,
            clock_ns: live.clock(),
            end_ns: live.end_ns(),
            weekend: live.weekend(),
            hidden: live.hidden(),
            done: live.done(),
            stopped: live.stopped(),
            true_px: live.true_px(),
            visible_px: live.visible_px(),
            samples,
            n_samples: n_samples as u16,
            bids,
            asks,
            n_bids: b.len().min(DEPTH) as u8,
            n_asks: a.len().min(DEPTH) as u8,
            more_bids: b.len().saturating_sub(DEPTH) as u32,
            more_asks: a.len().saturating_sub(DEPTH) as u32,
            pnl,
            priority_fees: live.priority_fees(),
            taker_fees: live.taker_fees(),
            races,
            n_races: tail.len() as u8,
            race_stats: live.race_stats(),
            rule: live.rule(),
            block_ms: live.block_ms(),
            load: live.load(),
            speed_milli: self.speed_milli,
            seeking: self.seek,
            block: live.block(),
            counts: live.counts(),
            msgs_per_s: self.msgs_per_s,
            step_ns_per_msg: self.step_ns_per_msg,
            rings,
            fills_hash,
        };
        // The standby checks itself against the primary at this point.
        push_wait(
            &mut self.rings.standby,
            StandbyMsg::Checkpoint {
                records: self.published as u64,
                fills_hash,
            },
            &mut self.peaks[2],
        );
        // The UI only needs the newest frame: drop it if the bridge is behind.
        let _ = self.rings.ui.push(UiMsg::Frame(frame));
    }
}
