//! Live mode: the same event loop, advanced step by step by a harness (the server), with control
//! changes applied between steps and logged as `Config` records (spec section 12).
//!
//! A control applied at virtual time `t` takes effect after every event at or before `t`. The
//! log records `t` as the record's `t_ns`, so [`replay`] can re-run the simulation from the log
//! alone (scenario, seed and the `Config` records) and get the same log byte for byte. The
//! harness decides *when* in virtual time a control lands (from the wall clock and the playback
//! speed); that choice is then an input, written down like any other.

use engine::{LogRecord, MsgKind, Px, Qty, Side};
use metrics::{class_pnl, ClassPnl, Race, RaceStats};

use super::{Counts, Sim};
use crate::config::{OrderingRule, Scenario};
use crate::log::{self, LogFile, WireKind, WireRecord};

/// A live control change. Each one is logged as `MsgKind::Config { param, value }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Control {
    /// Switch the ordering rule. Open blocks are sorted by the new rule when they close.
    Rule(OrderingRule),
    /// Change B. Open blocks close at once; later arrivals go into blocks of the new length.
    BlockMs(u64),
    /// Multiply the regular traders' arrival rate (the spec's load multiplier; 1 = as configured).
    Load(u32),
    /// Playback speed in thousandths (1000 = 1×, 10 = 1/100×, 0 = paused). It changes nothing in
    /// the simulation; it is logged so a replay can also reproduce the demo's pacing.
    Speed(u64),
    /// The run was stopped here (for example, to switch scenario). A replay stops at it.
    Stop,
}

pub const PARAM_RULE: u32 = 1;
pub const PARAM_BLOCK_MS: u32 = 2;
pub const PARAM_LOAD: u32 = 3;
pub const PARAM_SPEED: u32 = 4;
pub const PARAM_STOP: u32 = 5;

impl Control {
    pub fn encode(self) -> (u32, u64) {
        match self {
            Control::Rule(r) => (
                PARAM_RULE,
                match r {
                    OrderingRule::Fcfs => 0,
                    OrderingRule::Pga => 1,
                    OrderingRule::CancelPriority => 2,
                },
            ),
            Control::BlockMs(b) => (PARAM_BLOCK_MS, b),
            Control::Load(m) => (PARAM_LOAD, m as u64),
            Control::Speed(s) => (PARAM_SPEED, s),
            Control::Stop => (PARAM_STOP, 0),
        }
    }

    pub fn decode(param: u32, value: u64) -> Option<Self> {
        Some(match (param, value) {
            (PARAM_RULE, 0) => Control::Rule(OrderingRule::Fcfs),
            (PARAM_RULE, 1) => Control::Rule(OrderingRule::Pga),
            (PARAM_RULE, 2) => Control::Rule(OrderingRule::CancelPriority),
            (PARAM_BLOCK_MS, b) => Control::BlockMs(b),
            (PARAM_LOAD, m) => Control::Load(u32::try_from(m).ok()?),
            (PARAM_SPEED, s) => Control::Speed(s),
            (PARAM_STOP, 0) => Control::Stop,
            _ => return None,
        })
    }
}

/// A run advanced step by step. The harness owns the pacing; the run stays a pure function of
/// (scenario, seed, controls and the virtual times they were applied at).
pub struct Live {
    sim: Sim,
    seed: u64,
    /// The virtual time the run has been advanced to.
    clock: u64,
    stopped: bool,
}

impl Live {
    pub fn new(cfg: &Scenario, seed: u64) -> Result<Self, String> {
        cfg.validate()?;
        let mut sim = Sim::new(cfg, seed);
        sim.start();
        Ok(Self {
            sim,
            seed,
            clock: 0,
            stopped: false,
        })
    }

    /// Handle every event at or before `t_ns` (capped at the end of the run).
    pub fn advance_to(&mut self, t_ns: u64) {
        if self.stopped {
            return;
        }
        let t = t_ns.min(self.sim.end_ns).max(self.clock);
        self.sim.advance_to(t);
        self.clock = t;
    }

    /// Apply a control at the current clock and return its `Config` record. `None` once the run
    /// is stopped.
    pub fn control(&mut self, c: Control) -> Option<LogRecord> {
        if self.stopped {
            return None;
        }
        self.sim.now = self.clock;
        let rec = self.sim.control(c);
        if c == Control::Stop {
            self.stopped = true;
        }
        Some(rec)
    }

    pub fn clock(&self) -> u64 {
        self.clock
    }

    pub fn end_ns(&self) -> u64 {
        self.sim.end_ns
    }

    /// Reached the end of the scenario, or stopped.
    pub fn done(&self) -> bool {
        self.stopped || self.clock >= self.sim.end_ns
    }

    pub fn stopped(&self) -> bool {
        self.stopped
    }

    pub fn scenario(&self) -> &Scenario {
        &self.sim.cfg
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Every record sequenced so far, in `seq` order.
    pub fn records(&self) -> &[LogRecord] {
        &self.sim.records
    }

    pub fn counts(&self) -> Counts {
        let mut c = self.sim.counts;
        c.unsequenced = self.sim.sequencer.waiting() as u64;
        c
    }

    pub fn rule(&self) -> OrderingRule {
        self.sim.sequencer.rule()
    }

    pub fn block_ms(&self) -> u64 {
        self.sim.sequencer.block_ms()
    }

    pub fn load(&self) -> u32 {
        self.sim.load
    }

    /// The block open at the current clock (with B = 0, the next block number).
    pub fn block(&self) -> u64 {
        self.sim.sequencer.block_at(self.clock)
    }

    /// The true Nasdaq price now, hidden or not.
    pub fn true_px(&self) -> Px {
        self.sim.feed.px()
    }

    /// The last price published as a `Tick`.
    pub fn visible_px(&self) -> Px {
        self.sim.last_visible_px
    }

    /// Nasdaq is closed and its price hidden right now.
    pub fn hidden(&self) -> bool {
        self.sim.hidden(self.clock)
    }

    /// (close, reopen) in virtual ns, if the scenario has a weekend.
    pub fn weekend(&self) -> Option<(u64, u64)> {
        self.sim.window.map(|w| (w.close_ns, w.reopen_ns))
    }

    /// Aggregate book levels, best first.
    pub fn depth(&self, side: Side) -> Vec<(Px, Qty)> {
        self.sim.engine.depth(side)
    }

    pub fn best_bid(&self) -> Option<(Px, Qty)> {
        self.sim.engine.best_bid()
    }

    pub fn best_ask(&self) -> Option<(Px, Qty)> {
        self.sim.engine.best_ask()
    }

    /// PnL per account class, marked to the true Nasdaq price now. Sums to zero.
    pub fn pnl(&self) -> ClassPnl {
        class_pnl(&self.sim.ledger.balances(), self.sim.feed.px())
    }

    pub fn priority_fees(&self) -> i128 {
        self.sim.ledger.priority_fees
    }

    pub fn taker_fees(&self) -> i128 {
        self.sim.ledger.taker_fees
    }

    /// Every race so far, in start order (open races have no end yet).
    pub fn races(&self) -> &[Race] {
        self.sim.races.races()
    }

    pub fn race_stats(&self) -> RaceStats {
        self.sim.races.stats()
    }

    /// The fills hash of the log so far.
    pub fn fills_hash(&self) -> String {
        self.sim.fills.clone().finish()
    }

    /// The input log so far, in the same format as a batch run's.
    pub fn log_file(&self) -> LogFile {
        LogFile {
            format_version: log::FORMAT_VERSION,
            scenario: self.sim.cfg.name.clone(),
            seed: self.seed,
            config_toml: self.sim.cfg.to_toml(),
            records: self.sim.records.iter().map(WireRecord::from).collect(),
        }
    }

    pub fn log_bytes(&self) -> Vec<u8> {
        log::encode(&self.log_file())
    }
}

/// Re-run the simulation a log was made with: its scenario and seed, with every `Config` record
/// applied at its `t_ns`. Stops at a `Stop` record, or runs to the end of the scenario. For a log
/// written by a live run, the replay's log is byte-identical (see [`replay_check`]).
pub fn replay(file: &LogFile) -> Result<Live, String> {
    let cfg = Scenario::from_toml(&file.config_toml)?;
    let mut live = Live::new(&cfg, file.seed)?;
    for w in &file.records {
        let WireKind::Config { param, value } = w.kind else {
            continue;
        };
        let c = Control::decode(param, value)
            .ok_or_else(|| format!("seq {}: unknown control {param}={value}", w.seq))?;
        live.advance_to(w.t_ns);
        let rec = live
            .control(c)
            .ok_or_else(|| format!("seq {}: control after the run stopped", w.seq))?;
        if rec.seq != w.seq || rec.t_ns != w.t_ns {
            return Err(format!(
                "config record at seq {} (t {}) replayed at seq {} (t {})",
                w.seq, w.t_ns, rec.seq, rec.t_ns
            ));
        }
        if c == Control::Stop {
            return Ok(live);
        }
    }
    live.advance_to(live.end_ns());
    Ok(live)
}

/// Re-simulate a log and compare. Returns the fills hash if the replayed log is byte-identical.
pub fn replay_check(bytes: &[u8]) -> Result<String, String> {
    let file = log::decode(bytes)?;
    let live = replay(&file)?;
    if live.log_bytes() != bytes {
        let n = file.records.len().min(live.records().len());
        let first = (0..n)
            .find(|&i| LogRecord::from(&file.records[i]) != live.records()[i])
            .unwrap_or(n);
        return Err(format!(
            "replay differs: {} vs {} records, first difference at index {first}",
            file.records.len(),
            live.records().len()
        ));
    }
    Ok(live.fills_hash())
}

/// Count `Config` records in a log.
pub fn config_records(records: &[LogRecord]) -> usize {
    records
        .iter()
        .filter(|r| matches!(r.kind, MsgKind::Config { .. }))
        .count()
}
