//! The event loop (architecture section 3): a binary heap of events keyed by
//! `(time, tiebreak)`, where the tiebreak is a counter that increases on every push. One thread,
//! no wall clock; the same (scenario, seed) always gives byte-identical output.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};
use std::rc::Rc;

use engine::{AgentId, Engine, Event, EventKind, LogRecord, MsgKind};
use metrics::measure::DEFAULT_MONDAY_WINDOW_NS;
use metrics::{
    measure, zero_sum_check, AccountClass, ClassPnl, MeasureInput, Metrics, PricePath, Race,
    RaceStats, RaceTracker, Weekend,
};

use crate::agents::{Lp, Obs, Regular, Sniper};
use crate::config::{Latency, OrderingRule, Scenario};
use crate::ledger::Ledger;
use crate::log::{self, FillsHasher, LogFile, WireRecord};
use crate::price::{next_arrival, secs_to_ns, PriceFeed, Window};
use crate::rng::{Purpose, Rng, FEED_ID};
use crate::sequencer::{Accepted, Sequencer};

mod live;
pub use live::{config_records, replay, replay_check, Control, Live};

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
        /// When the agent decided to send it.
        decided: u64,
    },
    BlockClose(u64),
    Arrival {
        idx: usize,
        /// Arrivals scheduled before a live load change are dropped (see `Sim::control`).
        gen: u64,
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
    /// Logged live control changes (`Config` records).
    pub configs: u64,
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
    /// The true Nasdaq price at every change, hidden or not.
    pub path: PricePath,
    /// Each record's decision time, aligned with `records`.
    pub decided_ns: Vec<u64>,
    /// Every market metric of spec section 10.
    pub metrics: Metrics,
}

/// Measurement options that are not part of the scenario (so they don't change the log).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunOpts {
    /// The Monday-open window W.
    pub monday_window_ns: u64,
}

impl Default for RunOpts {
    fn default() -> Self {
        Self {
            monday_window_ns: DEFAULT_MONDAY_WINDOW_NS,
        }
    }
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
    events: Vec<Event>,
    market_batch: Vec<Event>,
    classes: BTreeMap<AgentId, AccountClass>,
    take_at_fill: BTreeMap<AccountClass, i128>,
    /// For the live dashboard: the fill-time gain of each (buyer class, seller class) pair, in
    /// tick·lots, and the fees each class paid (taker, priority), in cash units.
    pair_take: BTreeMap<(AccountClass, AccountClass), i128>,
    fees_paid: BTreeMap<AccountClass, (i128, i128)>,
    path: Vec<(u64, i64)>,
    decided: Vec<u64>,
    /// Live load multiplier on the regular traders' arrival rate (1 unless changed live).
    load: u32,
    arrival_gen: u64,
}

/// Run a scenario with a seed. Fails only if the ledger does not balance.
pub fn run(cfg: &Scenario, seed: u64) -> Result<RunResult, String> {
    run_with(cfg, seed, &RunOpts::default())
}

/// [`run`] with measurement options.
pub fn run_with(cfg: &Scenario, seed: u64, opts: &RunOpts) -> Result<RunResult, String> {
    cfg.validate()?;
    let mut sim = Sim::new(cfg, seed);
    sim.start();
    sim.advance_to(sim.end_ns);
    sim.finish(seed, opts)
}

impl Sim {
    fn new(cfg: &Scenario, seed: u64) -> Self {
        let pga = cfg.sequencer.rule == OrderingRule::Pga;
        // Agent ids: the feed is 0, then LPs, snipers and regular traders, in that order.
        let mut specs: Vec<(Latency, Latency, Role)> = Vec::new();
        for i in 0..cfg.lp.count {
            let id = FEED_ID + 1 + i;
            let mut see = cfg.lp.see;
            if cfg.lp.count > 1 {
                see.base_us += cfg.lp.see_spread_us * i as u64 / (cfg.lp.count as u64 - 1);
            }
            specs.push((see, cfg.lp.send, Role::Lp(Lp::new(id, &cfg.lp, pga))));
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
            pair_take: BTreeMap::new(),
            fees_paid: BTreeMap::new(),
            agents,
            window: feed.window(),
            last_visible_px: cfg.price.start_px_ticks,
            feed,
            reopen: None,
            counts: Counts::default(),
            fills: FillsHasher::default(),
            events: Vec::new(),
            market_batch: Vec::new(),
            path: Vec::new(),
            decided: Vec::new(),
            load: 1,
            arrival_gen: 0,
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

    /// Schedule the run's first events.
    fn start(&mut self) {
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
    }

    /// Handle every event at or before `t` (capped at the end of the run), in `(time, tiebreak)`
    /// order. Advancing in several steps handles exactly what one step to the same time does.
    fn advance_to(&mut self, t: u64) {
        let t = t.min(self.end_ns);
        while self.heap.peek().is_some_and(|item| item.t <= t) {
            let item = self.heap.pop().expect("peeked");
            self.now = item.t;
            self.handle(item.ev);
        }
    }

    /// Apply a live control change now: log it as a `Config` record, then change the setting.
    /// A replay applies the same records at the same times (after every event at or before
    /// `t_ns`), so it reproduces the run exactly.
    fn control(&mut self, c: Control) -> LogRecord {
        let now = self.now;
        let (param, value) = c.encode();
        self.process(
            LogRecord {
                seq: 0,
                t_ns: now,
                block: self.sequencer.block_at(now),
                agent: FEED_ID,
                kind: MsgKind::Config { param, value },
            },
            now,
        );
        let rec = *self.records.last().expect("just logged");
        match c {
            Control::Rule(rule) => {
                self.sequencer.set_rule(rule);
                let pga = rule == OrderingRule::Pga;
                for a in &mut self.agents {
                    match &mut a.role {
                        Role::Lp(lp) => lp.set_pga(pga),
                        Role::Sniper(s) => s.set_pga(pga),
                        Role::Regular(_) => {}
                    }
                }
            }
            Control::BlockMs(b) => {
                // Open blocks close now; later arrivals go into blocks of the new length.
                for (block, msgs) in self.sequencer.set_block_ms(now, b) {
                    for p in msgs {
                        self.process(
                            LogRecord {
                                seq: 0,
                                t_ns: p.t_ns,
                                block,
                                agent: p.agent,
                                kind: p.kind,
                            },
                            p.decided_ns,
                        );
                    }
                    self.counts.blocks += 1;
                }
                self.flush_market_data();
            }
            Control::Load(m) => {
                // Poisson arrivals are memoryless, so redrawing each trader's next arrival from
                // now at the new rate is exact; the arrivals already queued are dropped.
                self.load = m.max(1);
                self.arrival_gen += 1;
                for idx in 0..self.agents.len() {
                    if matches!(self.agents[idx].role, Role::Regular(_)) {
                        self.schedule_arrival(idx, now);
                    }
                }
            }
            Control::Speed(_) | Control::Stop => {}
        }
        rec
    }

    fn handle(&mut self, ev: Ev) {
        let now = self.now;
        match ev {
            Ev::Start => {
                self.path.push((now, self.feed.px()));
                if !self.hidden(now) {
                    self.emit_tick(self.feed.px());
                }
            }
            Ev::Step => {
                let px = self.feed.step();
                self.path.push((now, px));
                if !self.hidden(now) {
                    self.emit_tick(px);
                }
                if let Some(t) = self.feed.next_step_time(now) {
                    self.push(t, Ev::Step);
                }
            }
            Ev::Jump => {
                let px = self.feed.jump();
                self.path.push((now, px));
                if !self.hidden(now) {
                    self.emit_tick(px);
                }
                if let Some(t) = self.feed.next_jump_time(now) {
                    self.push(t, Ev::Jump);
                }
            }
            Ev::ScheduledJump(ticks) => {
                let px = self.feed.shift(ticks);
                self.path.push((now, px));
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
            Ev::Submit { idx, kind, decided } => {
                let agent = self.agents[idx].id;
                if let MsgKind::Cancel { order_id, .. } = kind {
                    self.races.on_cancel_arrival(order_id, now);
                }
                match self.sequencer.accept(now, decided, agent, kind) {
                    Accepted::Now { block } => {
                        self.process(
                            LogRecord {
                                seq: 0,
                                t_ns: now,
                                block,
                                agent,
                                kind,
                            },
                            decided,
                        );
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
                // A live change of B already sealed this block.
                if !self.sequencer.is_open(block) {
                    return;
                }
                for p in self.sequencer.seal(block) {
                    self.process(
                        LogRecord {
                            seq: 0,
                            t_ns: p.t_ns,
                            block,
                            agent: p.agent,
                            kind: p.kind,
                        },
                        p.decided_ns,
                    );
                }
                self.counts.blocks += 1;
                self.flush_market_data();
            }
            Ev::Arrival { idx, gen } => {
                if gen != self.arrival_gen {
                    return;
                }
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
                    self.push(
                        t,
                        Ev::Submit {
                            idx,
                            kind,
                            decided: now,
                        },
                    );
                }
                self.schedule_arrival(idx, now);
            }
        }
    }

    fn schedule_arrival(&mut self, idx: usize, from: u64) {
        let window = self.window;
        let end = self.end_ns;
        let (load, gen) = (self.load as f64, self.arrival_gen);
        let a = &mut self.agents[idx];
        let Role::Regular(r) = &a.role else {
            unreachable!()
        };
        // × 1.0 is exact, so a run without a live load change draws the same arrivals.
        let (weekday, weekend) = (r.rate_per_s(false) * load, r.rate_per_s(true) * load);
        let next = next_arrival(&mut a.act, from, end, |t| match window {
            Some(w) if t < w.close_ns => (weekday, w.close_ns),
            Some(w) if t < w.reopen_ns => (weekend, w.reopen_ns),
            _ => (weekday, u64::MAX),
        });
        if let Some(t) = next {
            self.push(t, Ev::Arrival { idx, gen });
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
            self.push(
                t,
                Ev::Submit {
                    idx,
                    kind,
                    decided: now,
                },
            );
        }
    }

    /// Log a visible Nasdaq price now. Ticks never wait for a block.
    fn emit_tick(&mut self, px: i64) {
        self.last_visible_px = px;
        let block = self.sequencer.block_at(self.now);
        self.process(
            LogRecord {
                seq: 0,
                t_ns: self.now,
                block,
                agent: FEED_ID,
                kind: MsgKind::Tick { px_ticks: px },
            },
            self.now,
        );
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
    fn process(&mut self, mut rec: LogRecord, decided: u64) {
        self.seq += 1;
        rec.seq = self.seq;
        self.records.push(rec);
        self.decided.push(decided);
        self.ledger.on_included(&rec);
        let fee = rec.kind.priority_fee() as i128;
        if fee > 0 {
            self.fees_paid
                .entry(self.classes[&rec.agent])
                .or_default()
                .1 += fee;
        }
        // One reused event buffer: the engine allocates nothing per message for its output.
        let mut events = std::mem::take(&mut self.events);
        self.engine.on_message_into(&rec, &mut events);
        self.ledger.on_events(&events);
        self.races.on_record(&rec, &events);

        let c = &mut self.counts;
        c.records += 1;
        match rec.kind {
            MsgKind::Tick { .. } => c.ticks += 1,
            MsgKind::New { .. } => c.news += 1,
            MsgKind::Cancel { .. } => c.cancels += 1,
            MsgKind::Config { .. } => c.configs += 1,
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
                    let pair = (self.classes[&buyer], self.classes[&seller]);
                    *self.pair_take.entry(pair).or_default() += gain;
                    let taker_fee = px as i128 * qty as i128 * self.cfg.fees.taker_fee_bps as i128;
                    self.fees_paid
                        .entry(self.classes[&taker_agent])
                        .or_default()
                        .0 += taker_fee;
                }
                EventKind::CancelMissed { .. } => c.cancel_missed += 1,
                _ => {}
            }
        }
        self.market_batch.extend_from_slice(&events);
        self.events = events;
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

    fn finish(self, seed: u64, opts: &RunOpts) -> Result<RunResult, String> {
        let final_px = self.feed.px();
        let pnl = zero_sum_check(&self.ledger.balances(), final_px).map_err(|e| e.to_string())?;
        let path = PricePath::new(self.path);
        let metrics = measure(&MeasureInput {
            records: &self.records,
            decided_ns: &self.decided,
            classes: &self.classes,
            path: &path,
            block_ns: self.cfg.sequencer.block_ms * 1_000_000,
            end_ns: self.end_ns,
            theta_ticks: self.cfg.sniper.theta_ticks,
            taker_fee_bps: self.cfg.fees.taker_fee_bps,
            weekend: self.window.map(|w| Weekend {
                close_ns: w.close_ns,
                reopen_ns: w.reopen_ns,
            }),
            monday_window_ns: opts.monday_window_ns,
        });
        let class_sum: i128 = metrics.class_pnl.iter().flatten().sum();
        if class_sum != 0 {
            return Err(format!("metrics PnL does not balance: {class_sum}"));
        }
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
            path,
            decided_ns: self.decided,
            metrics,
        })
    }
}
