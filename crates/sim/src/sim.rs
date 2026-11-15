//! The event loop (architecture section 3): a binary heap of events keyed by
//! `(time, tiebreak)`, where the tiebreak is a counter that increases on every push. One thread,
//! no wall clock; the same (scenario, seed) always gives byte-identical output.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};
use std::rc::Rc;

use engine::{AgentId, Engine, Event, EventKind, LogRecord, MsgKind};
use metrics::{zero_sum_check, AccountClass, ClassPnl, Race, RaceStats, RaceTracker};

use crate::agents::{Lp, Obs, Regular, Sniper};
use crate::config::{Latency, OrderingRule, Scenario};
use crate::ledger::Ledger;
use crate::log::{self, FillsHasher, LogFile, WireRecord};
use crate::price::{next_arrival, secs_to_ns, PriceFeed, Window};
use crate::rng::{Purpose, Rng, FEED_ID};
use crate::sequencer::{Accepted, Sequencer};

#[derive(Debug, Clone)]
enum Ev {
    /// The opening price at t = 0.
    Start,
    Step,
    Jump,
    ScheduledJump(i64),
    Close,
    Reopen,
    Observe {
        idx: usize,
        obs: Obs,
    },
    Submit {
        idx: usize,
        kind: MsgKind,
    },
    BlockClose(u64),
    Arrival {
        idx: usize,
    },
}

struct Item {
    t: u64,
    tie: u64,
    ev: Ev,
}

impl PartialEq for Item {
    fn eq(&self, o: &Self) -> bool {
        (self.t, self.tie) == (o.t, o.tie)
    }
}
impl Eq for Item {}
impl PartialOrd for Item {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Item {
    /// Reversed, so the max-heap pops the earliest `(t, tie)`.
    fn cmp(&self, o: &Self) -> Ordering {
        (o.t, o.tie).cmp(&(self.t, self.tie))
    }
}

enum Role {
    Lp(Lp),
    Sniper(Sniper),
    Regular(Regular),
}

struct Agent {
    id: AgentId,
    see: Latency,
    send: Latency,
    lat: Rng,
    act: Rng,
    /// Per-agent channels are FIFO: nothing is seen (or arrives) before what was sent earlier.
    see_clock: u64,
    send_clock: u64,
    role: Role,
}

impl Agent {
    fn see_at(&mut self, now: u64) -> u64 {
        let t =
            (now + self.lat.latency_ns(self.see.base_us, self.see.jitter_us)).max(self.see_clock);
        self.see_clock = t;
        t
    }

    fn send_at(&mut self, now: u64) -> u64 {
        let t = (now + self.lat.latency_ns(self.send.base_us, self.send.jitter_us))
            .max(self.send_clock);
        self.send_clock = t;
        t
    }

    fn watches_market(&self) -> bool {
        !matches!(self.role, Role::Regular(_))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    pub records: u64,
    pub ticks: u64,
    pub news: u64,
    pub cancels: u64,
    pub fills: u64,
    pub volume_lots: u64,
    /// Sum of px × qty over fills, in tick·lots.
    pub notional_tick_lots: i128,
    pub cancel_missed: u64,
    pub blocks: u64,
    /// Messages still in an open block when the run ended (never sequenced).
    pub unsequenced: u64,
}

/// Everything a run produces.
pub struct RunResult {
    pub scenario: Scenario,
    pub seed: u64,
    pub records: Vec<LogRecord>,
    /// The encoded input log (postcard).
    pub log_bytes: Vec<u8>,
    pub log_hash: String,
    pub fills_hash: String,
    pub counts: Counts,
    pub races: RaceStats,
    pub race_list: Vec<Race>,
    /// PnL per account class marked to the final Nasdaq price. Passed the zero-sum check.
    pub pnl: ClassPnl,
    pub final_px: i64,
    /// (last visible price before the close, the reopen price).
    pub reopen: Option<(i64, i64)>,
    pub taker_fees: i128,
    pub priority_fees: i128,
    pub agents: BTreeMap<AgentId, AccountClass>,
    /// Each class's gain on its fills against the true Nasdaq price at the moment of the fill
    /// (a markout with horizon 0), in tick·lots, before fees. Sums to zero across classes.
    /// The sniper entry is the stale-quote prize the snipers captured.
    pub take_at_fill: BTreeMap<AccountClass, i128>,
}

struct Sim {
    cfg: Scenario,
    end_ns: u64,
    now: u64,
    heap: BinaryHeap<Item>,
    pushes: u64,
    engine: Engine,
    seq: u64,
    records: Vec<LogRecord>,
    sequencer: Sequencer,
    ledger: Ledger,
    races: RaceTracker,
    agents: Vec<Agent>,
    feed: PriceFeed,
    window: Option<Window>,
    last_visible_px: i64,
    reopen: Option<(i64, i64)>,
    counts: Counts,
    fills: FillsHasher,
    market_batch: Vec<Event>,
    classes: BTreeMap<AgentId, AccountClass>,
    take_at_fill: BTreeMap<AccountClass, i128>,
}

/// Run a scenario with a seed. Fails only if the ledger does not balance.
pub fn run(cfg: &Scenario, seed: u64) -> Result<RunResult, String> {
    cfg.validate()?;
    let mut sim = Sim::new(cfg, seed);
    sim.run();
    sim.finish(seed)
}

impl Sim {
    fn new(cfg: &Scenario, seed: u64) -> Self {
        let pga = cfg.sequencer.rule == OrderingRule::Pga;
        // Agent ids: the feed is 0, then LPs, snipers and regular traders, in that order.
        let mut specs: Vec<(Latency, Latency, Role)> = Vec::new();
        for i in 0..cfg.lp.count {
            let id = FEED_ID + 1 + i;
            specs.push((cfg.lp.see, cfg.lp.send, Role::Lp(Lp::new(id, &cfg.lp))));
        }
        for _ in 0..cfg.sniper.count {
            let role = Role::Sniper(Sniper::new(&cfg.sniper, pga));
            specs.push((cfg.sniper.see, cfg.sniper.send, role));
        }
        for _ in 0..cfg.regular.count {
            let role = Role::Regular(Regular::new(&cfg.regular));
            specs.push((Latency::new(0, 0), cfg.regular.send, role));
        }
        let agents: Vec<Agent> = specs
            .into_iter()
            .enumerate()
            .map(|(i, (see, send, role))| {
                let id = FEED_ID + 1 + i as AgentId;
                Agent {
                    id,
                    see,
                    send,
                    lat: Rng::new(seed, id, Purpose::Latency),
                    act: Rng::new(seed, id, Purpose::Action),
                    see_clock: 0,
                    send_clock: 0,
                    role,
                }
            })
            .collect();
        let classes: BTreeMap<AgentId, AccountClass> = agents
            .iter()
            .map(|a| {
                let c = match a.role {
                    Role::Lp(_) => AccountClass::Lp,
                    Role::Sniper(_) => AccountClass::Sniper,
                    Role::Regular(_) => AccountClass::Regular,
                };
                (a.id, c)
            })
            .collect();

        let end_ns = cfg.duration_s * 1_000_000_000;
        let feed = PriceFeed::new(&cfg.price, cfg.weekend.as_ref(), end_ns, seed);
        Self {
            cfg: cfg.clone(),
            end_ns,
            now: 0,
            heap: BinaryHeap::new(),
            pushes: 0,
            engine: Engine::new(),
            seq: 0,
            records: Vec::new(),
            sequencer: Sequencer::new(cfg.sequencer.rule, cfg.sequencer.block_ms),
            ledger: Ledger::new(cfg.fees.taker_fee_bps, &classes),
            races: RaceTracker::new(cfg.sniper.theta_ticks, classes.clone()),
            classes,
            take_at_fill: AccountClass::ALL.iter().map(|&c| (c, 0)).collect(),
            agents,
            window: feed.window(),
            last_visible_px: cfg.price.start_px_ticks,
            feed,
            reopen: None,
            counts: Counts::default(),
            fills: FillsHasher::default(),
            market_batch: Vec::new(),
        }
    }

    fn push(&mut self, t: u64, ev: Ev) {
        self.heap.push(Item {
            t,
            tie: self.pushes,
            ev,
        });
        self.pushes += 1;
    }

    fn hidden(&self, t: u64) -> bool {
        self.window.is_some_and(|w| w.hidden(t))
    }

    fn run(&mut self) {
        self.push(0, Ev::Start);
        if let Some(t) = self.feed.next_step_time(0) {
            self.push(t, Ev::Step);
        }
        if let Some(t) = self.feed.next_jump_time(0) {
            self.push(t, Ev::Jump);
        }
        for j in self.cfg.price.scheduled_jumps.clone() {
            self.push(secs_to_ns(j.at_s), Ev::ScheduledJump(j.ticks));
        }
        if let Some(w) = self.window {
            self.push(w.close_ns, Ev::Close);
            self.push(w.reopen_ns, Ev::Reopen);
        }
        for idx in 0..self.agents.len() {
            if matches!(self.agents[idx].role, Role::Regular(_)) {
                self.schedule_arrival(idx, 0);
            }
        }

        while let Some(item) = self.heap.pop() {
            if item.t > self.end_ns {
                break;
            }
            self.now = item.t;
            self.handle(item.ev);
        }
    }

    fn handle(&mut self, ev: Ev) {
        let now = self.now;
        match ev {
            Ev::Start => {
                if !self.hidden(now) {
                    self.emit_tick(self.feed.px());
                }
            }
            Ev::Step => {
                let px = self.feed.step();
                if !self.hidden(now) {
                    self.emit_tick(px);
                }
                if let Some(t) = self.feed.next_step_time(now) {
                    self.push(t, Ev::Step);
                }
            }
            Ev::Jump => {
                let px = self.feed.jump();
                if !self.hidden(now) {
                    self.emit_tick(px);
                }
                if let Some(t) = self.feed.next_jump_time(now) {
                    self.push(t, Ev::Jump);
                }
            }
            Ev::ScheduledJump(ticks) => {
                let px = self.feed.shift(ticks);
                if !self.hidden(now) {
                    self.emit_tick(px);
                }
            }
            Ev::Close => {
                // The close is on the calendar: LPs know it without latency.
                for idx in 0..self.agents.len() {
                    if matches!(self.agents[idx].role, Role::Lp(_)) {
                        self.observe(idx, Obs::Closed);
                    }
                }
            }
            Ev::Reopen => {
                let px = self.feed.px();
                self.reopen = Some((self.last_visible_px, px));
                self.emit_tick(px);
            }
            Ev::Observe { idx, obs } => self.observe(idx, obs),
            Ev::Submit { idx, kind } => {
                let agent = self.agents[idx].id;
                match self.sequencer.accept(now, agent, kind) {
                    Accepted::Now { block } => {
                        self.process(LogRecord {
                            seq: 0,
                            t_ns: now,
                            block,
                            agent,
                            kind,
                        });
                        self.counts.blocks += 1;
                        self.flush_market_data();
                    }
                    Accepted::Buffered { schedule_close } => {
                        if let Some((block, t_close)) = schedule_close {
                            self.push(t_close, Ev::BlockClose(block));
                        }
                    }
                }
            }
            Ev::BlockClose(block) => {
                for p in self.sequencer.seal(block) {
                    self.process(LogRecord {
                        seq: 0,
                        t_ns: p.t_ns,
                        block,
                        agent: p.agent,
                        kind: p.kind,
                    });
                }
                self.counts.blocks += 1;
                self.flush_market_data();
            }
            Ev::Arrival { idx } => {
                let (bb, ba) = (
                    self.engine.best_bid().map(|b| b.0),
                    self.engine.best_ask().map(|a| a.0),
                );
                let a = &mut self.agents[idx];
                let Role::Regular(r) = &a.role else {
                    unreachable!()
                };
                let msg = r.decide(&mut a.act, bb, ba);
                // Draw the send latency whether or not an order goes out, so the regular
                // traders' draws are the same under every ordering rule.
                let t = a.send_at(now);
                if let Some(kind) = msg {
                    self.push(t, Ev::Submit { idx, kind });
                }
                self.schedule_arrival(idx, now);
            }
        }
    }

    fn schedule_arrival(&mut self, idx: usize, from: u64) {
        let window = self.window;
        let end = self.end_ns;
        let a = &mut self.agents[idx];
        let Role::Regular(r) = &a.role else {
            unreachable!()
        };
        let (weekday, weekend) = (r.rate_per_s(false), r.rate_per_s(true));
        let next = next_arrival(&mut a.act, from, end, |t| match window {
            Some(w) if t < w.close_ns => (weekday, w.close_ns),
            Some(w) if t < w.reopen_ns => (weekend, w.reopen_ns),
            _ => (weekday, u64::MAX),
        });
        if let Some(t) = next {
            self.push(t, Ev::Arrival { idx });
        }
    }

    /// An agent observes something now and may send messages.
    fn observe(&mut self, idx: usize, obs: Obs) {
        let now = self.now;
        let a = &mut self.agents[idx];
        let msgs = match &mut a.role {
            Role::Lp(lp) => lp.observe(&obs),
            Role::Sniper(s) => s.observe(&obs),
            Role::Regular(_) => Vec::new(),
        };
        if msgs.is_empty() {
            return;
        }
        // One send-latency draw per reaction: a batch travels together and keeps its order.
        let t = a.send_at(now);
        for kind in msgs {
            self.push(t, Ev::Submit { idx, kind });
        }
    }

    /// Log a visible Nasdaq price now. Ticks never wait for a block.
    fn emit_tick(&mut self, px: i64) {
        self.last_visible_px = px;
        let block = self.sequencer.block_at(self.now);
        self.process(LogRecord {
            seq: 0,
            t_ns: self.now,
            block,
            agent: FEED_ID,
            kind: MsgKind::Tick { px_ticks: px },
        });
        // The echo carries nothing agents need from market data.
        self.market_batch.clear();
        for idx in 0..self.agents.len() {
            if self.agents[idx].watches_market() {
                let t = self.agents[idx].see_at(self.now);
                self.push(
                    t,
                    Ev::Observe {
                        idx,
                        obs: Obs::Price(px),
                    },
                );
            }
        }
    }

    /// Sequence one record: assign the next `seq`, append to the log, charge inclusion fees,
    /// match, book the fills, and count races.
    fn process(&mut self, mut rec: LogRecord) {
        self.seq += 1;
        rec.seq = self.seq;
        self.records.push(rec);
        self.ledger.on_included(&rec);
        let events = self.engine.on_message(&rec);
        self.ledger.on_events(&events);
        self.races.on_record(&rec, &events);

        let c = &mut self.counts;
        c.records += 1;
        match rec.kind {
            MsgKind::Tick { .. } => c.ticks += 1,
            MsgKind::New { .. } => c.news += 1,
            MsgKind::Cancel { .. } => c.cancels += 1,
        }
        for ev in &events {
            self.fills.update(ev.seq, &ev.kind);
            match ev.kind {
                EventKind::Fill {
                    px,
                    qty,
                    taker_side,
                    taker_agent,
                    maker_agent,
                    ..
                } => {
                    c.fills += 1;
                    c.volume_lots += qty;
                    c.notional_tick_lots += px as i128 * qty as i128;
                    let (buyer, seller) = match taker_side {
                        engine::Side::Buy => (taker_agent, maker_agent),
                        engine::Side::Sell => (maker_agent, taker_agent),
                    };
                    let gain = (self.feed.px() - px) as i128 * qty as i128;
                    *self.take_at_fill.get_mut(&self.classes[&buyer]).unwrap() += gain;
                    *self.take_at_fill.get_mut(&self.classes[&seller]).unwrap() -= gain;
                }
                EventKind::CancelMissed { .. } => c.cancel_missed += 1,
                _ => {}
            }
        }
        self.market_batch.extend(events);
    }

    /// Deliver the market data of the block just processed to every agent that watches it.
    fn flush_market_data(&mut self) {
        if self.market_batch.is_empty() {
            return;
        }
        let batch: Rc<[Event]> = std::mem::take(&mut self.market_batch).into();
        for idx in 0..self.agents.len() {
            if self.agents[idx].watches_market() {
                let t = self.agents[idx].see_at(self.now);
                self.push(
                    t,
                    Ev::Observe {
                        idx,
                        obs: Obs::Market(batch.clone()),
                    },
                );
            }
        }
    }

    fn finish(self, seed: u64) -> Result<RunResult, String> {
        let final_px = self.feed.px();
        let pnl = zero_sum_check(&self.ledger.balances(), final_px).map_err(|e| e.to_string())?;
        let mut counts = self.counts;
        counts.unsequenced = self.sequencer.waiting() as u64;
        let file = LogFile {
            format_version: log::FORMAT_VERSION,
            scenario: self.cfg.name.clone(),
            seed,
            config_toml: self.cfg.to_toml(),
            records: self.records.iter().map(WireRecord::from).collect(),
        };
        let log_bytes = log::encode(&file);
        let log_hash = log::sha256_hex(&log_bytes);
        Ok(RunResult {
            scenario: self.cfg,
            seed,
            records: self.records,
            log_bytes,
            log_hash,
            fills_hash: self.fills.finish(),
            counts,
            races: self.races.stats(),
            race_list: self.races.races().to_vec(),
            pnl,
            final_px,
            reopen: self.reopen,
            taker_fees: self.ledger.taker_fees,
            priority_fees: self.ledger.priority_fees,
            agents: self.classes,
            take_at_fill: self.take_at_fill,
        })
    }
}
