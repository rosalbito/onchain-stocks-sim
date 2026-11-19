//! The market metrics of spec section 10, computed from one run's input log.
//!
//! [`measure`] replays the log through a fresh engine and values every fill against the true
//! Nasdaq price v (hidden or not). Besides the log it needs three things the log does not hold:
//! the true price path (the hidden weekend is never logged), each message's decision time (the
//! log has arrival times), and the run's parameters (block length, θ, taker fee, calendar).
//!
//! Integer math only. Every result is a numerator and a denominator in natural units (ticks,
//! lots, cash units, ns); reports convert to bps, dollars and shares.
//!
//! Units: tick·lots for price × quantity, cash units (1/10,000 tick·lot, see
//! [`CASH_PER_TICK_LOT`]) for fees and PnL, ns for time.

use std::collections::BTreeMap;

use engine::{AgentId, Engine, EventKind, LogRecord, MsgKind, OrderId, Px, Qty, Side};

use crate::{AccountClass, RaceStats, RaceTracker, CASH_PER_TICK_LOT};

/// Markout horizons h: 0 (the fill instant), then 1 s, 10 s and 60 s (spec section 10).
pub const HORIZONS_NS: [u64; 4] = [0, 1_000_000_000, 10_000_000_000, 60_000_000_000];
/// Index into [`HORIZONS_NS`] of the horizon the headline numbers use (1 s).
pub const HEADLINE_H: usize = 1;
/// Number of horizons.
pub const NH: usize = HORIZONS_NS.len();
/// Default Monday-open window W.
pub const DEFAULT_MONDAY_WINDOW_NS: u64 = 300_000_000_000;
/// "Depth within 10 bps of v".
pub const DEPTH_BAND_BPS: i64 = 10;

const NC: usize = AccountClass::ALL.len();

fn ci(c: AccountClass) -> usize {
    c as usize
}

/// The true Nasdaq price as a step function of time: `(t_ns, px)` at every change, sorted by
/// time, starting at t = 0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PricePath {
    points: Vec<(u64, Px)>,
}

impl PricePath {
    /// `points` must be sorted by time and start at t = 0. Several points at one time are
    /// allowed; the last one wins.
    pub fn new(points: Vec<(u64, Px)>) -> Self {
        assert!(
            points.first().is_some_and(|p| p.0 == 0),
            "price path must start at t = 0"
        );
        assert!(
            points.windows(2).all(|w| w[0].0 <= w[1].0),
            "price path must be sorted by time"
        );
        Self { points }
    }

    /// v at time t: the last change at or before t.
    pub fn at(&self, t: u64) -> Px {
        let i = self.points.partition_point(|p| p.0 <= t);
        self.points[i - 1].1
    }

    pub fn points(&self) -> &[(u64, Px)] {
        &self.points
    }
}

/// The hidden weekend, for the Monday-open take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Weekend {
    pub close_ns: u64,
    pub reopen_ns: u64,
}

/// What [`measure`] needs besides the log.
#[derive(Debug, Clone)]
pub struct MeasureInput<'a> {
    pub records: &'a [LogRecord],
    /// Decision time of each record, aligned with `records`: when the sender decided to send it
    /// (for a tick, its own time).
    pub decided_ns: &'a [u64],
    pub classes: &'a BTreeMap<AgentId, AccountClass>,
    pub path: &'a PricePath,
    /// Block length B in ns (0 = every message is its own block).
    pub block_ns: u64,
    /// Run length. Markouts past the end use the last price.
    pub end_ns: u64,
    pub theta_ticks: i64,
    pub taker_fee_bps: u64,
    pub weekend: Option<Weekend>,
    /// The Monday-open window W.
    pub monday_window_ns: u64,
}

/// When a record was processed (matched): ticks and B = 0 messages at their arrival, block
/// messages when their block closed.
pub fn processed_ns(rec: &LogRecord, block_ns: u64) -> u64 {
    if block_ns == 0 || matches!(rec.kind, MsgKind::Tick { .. }) {
        rec.t_ns
    } else {
        (rec.block + 1) * block_ns
    }
}

/// Percentiles of fill delays, by nearest rank.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Delays {
    /// Taker orders with at least one fill.
    pub count: u64,
    pub median_ns: u64,
    pub p99_ns: u64,
}

impl Delays {
    fn from(mut v: Vec<u64>) -> Self {
        if v.is_empty() {
            return Self::default();
        }
        v.sort_unstable();
        let rank = |pct: usize| {
            // Nearest rank: the smallest value with at least pct% of the data at or below it.
            let k = (v.len() * pct).div_ceil(100).max(1);
            v[k - 1]
        };
        Self {
            count: v.len() as u64,
            median_ns: rank(50),
            p99_ns: rank(99),
        }
    }
}

/// LP fills split into effective and realized half-spread (against v).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LpSpread {
    pub lots: u64,
    pub notional_tick_lots: i128,
    /// Σ q · s · (p − v at the fill), s = +1 when the LP sold. In tick·lots.
    pub effective: i128,
    /// Σ q · s · (p − v at fill + h), per horizon.
    pub realized: [i128; NH],
}

impl LpSpread {
    /// Adverse selection = effective − realized = Σ q · s · (v at fill + h − v at fill).
    pub fn adverse(&self, h: usize) -> i128 {
        self.effective - self.realized[h]
    }
}

/// Time-weighted quoted liquidity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Quoted {
    /// Σ (best ask − best bid) · dt while both sides are quoted, tick·ns.
    pub spread_tick_ns: i128,
    /// Σ (best ask + best bid) · dt over the same time, tick·ns. spread / this = half-spread / mid.
    pub mid2_tick_ns: i128,
    /// Time with both sides quoted, ns.
    pub two_sided_ns: u64,
    /// Σ (lots resting within 10 bps of v, both sides) · dt, lot·ns.
    pub depth_lot_ns: i128,
    /// Time integrated over (the run), ns.
    pub total_ns: u64,
}

/// The Monday-open take (docs/DECISIONS.md, entry 80). The *gap prize* is what the book offered
/// snipers at the reopen: Σ staleness × lots over the LP quotes resting just before the reopen
/// tick that the reopen price leaves stale by at least θ, each valued at the reopen price. The
/// prize then splits by who removed each of those quotes within W: snipers (fills), the LP (its
/// cancels), regular traders (fills), or nobody. Numerator and denominator cover the same quotes
/// at the same price, so every share is in [0, 1] except the net one, which subtracts fees.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MondayOpen {
    /// Reopen price − last visible price, ticks.
    pub gap_ticks: i64,
    /// Lots resting at the close on the stale side (asks if the gap is up, bids if down). A
    /// diagnostic: the spec's denominator used it, but the book changes over the weekend.
    pub resting_lots: u64,
    /// Lots resting on the stale side just before the reopen tick (a diagnostic).
    pub resting_lots_at_reopen: u64,
    /// The gap prize: Σ staleness × lots over the LP quotes stale by ≥ θ at the reopen,
    /// tick·lots.
    pub prize: i128,
    /// Lots in those quotes.
    pub prize_lots: u64,
    /// Part of the prize snipers filled within W, tick·lots (valued at the reopen price).
    pub sniper_took: i128,
    /// Part of the prize the LP cancelled within W, tick·lots.
    pub lp_kept: i128,
    /// Part of the prize regular traders filled within W, tick·lots.
    pub other_took: i128,
    /// Taker fees snipers paid on their fills of prize quotes, cash.
    pub sniper_taker_fees: i128,
    /// Priority fees on each sniper's first reaction after the reopen (its first price seen
    /// after the weekend is the reopen tick), filled or not, cash.
    pub sniper_priority_fees: i128,
    /// Priority fees on the LP's cancels of prize quotes, found or not, cash.
    pub lp_priority_fees: i128,
    /// Sniper PnL (horizon [`HEADLINE_H`], net of fees) on everything in [reopen, reopen + W),
    /// cash: the window's sniper take, races after the reopen included.
    pub sniper_pnl: i128,
    /// LP loss to snipers (horizon [`HEADLINE_H`]) on fills in the window, tick·lots.
    pub lp_loss_to_snipers: i128,
    /// Priority fees paid by anyone on messages processed in the window, cash.
    pub chain_fees: i128,
    /// The part of `chain_fees` the LP paid (its cancel bids), cash.
    pub chain_fees_lp: i128,
    /// Σ p · q over all fills in the window, tick·lots (the window's volume).
    pub notional_tick_lots: i128,
}

impl MondayOpen {
    /// The Monday-open take as a share of the gap: (sniper_took, prize), tick·lots. In [0, 1];
    /// undefined (zero denominator) when no resting quote was stale at the reopen.
    pub fn share(&self) -> (i128, i128) {
        (self.sniper_took, self.prize)
    }

    /// The snipers' share net of the fees they paid for the gap, cash units. At most the gross
    /// share; negative when their priority bids exceed what they took.
    pub fn net_share(&self) -> (i128, i128) {
        (
            self.sniper_took * CASH_PER_TICK_LOT
                - self.sniper_taker_fees
                - self.sniper_priority_fees,
            self.prize * CASH_PER_TICK_LOT,
        )
    }

    /// The chain's share: priority fees paid in the gap race (snipers' and the LP's), cash.
    pub fn chain_share(&self) -> (i128, i128) {
        (
            self.sniper_priority_fees + self.lp_priority_fees,
            self.prize * CASH_PER_TICK_LOT,
        )
    }

    /// The LP's share: the prize it cancelled in time, tick·lots, before its cancel fees.
    pub fn lp_share(&self) -> (i128, i128) {
        (self.lp_kept, self.prize)
    }
}

/// The LP's resting GTC quotes, by id, as the replay sees them.
#[derive(Default)]
struct LpQuotes {
    live: BTreeMap<OrderId, (Side, Px, Qty)>,
}

impl LpQuotes {
    fn on_event(&mut self, ev: &EventKind, is_lp: bool) {
        match *ev {
            EventKind::Accepted {
                order_id,
                side,
                px,
                qty,
                tif: engine::Tif::Gtc,
                ..
            } if is_lp => {
                self.live.insert(order_id, (side, px, qty));
            }
            EventKind::Fill {
                maker_order,
                taker_order,
                qty,
                maker_remaining,
                ..
            } => {
                if let Some(q) = self.live.get_mut(&maker_order) {
                    q.2 = maker_remaining;
                    if maker_remaining == 0 {
                        self.live.remove(&maker_order);
                    }
                }
                // A GTC that crossed on entry: its taker fills reduce what rests.
                if let Some(q) = self.live.get_mut(&taker_order) {
                    q.2 -= qty;
                    if q.2 == 0 {
                        self.live.remove(&taker_order);
                    }
                }
            }
            EventKind::Cancelled { order_id, .. } => {
                self.live.remove(&order_id);
            }
            _ => {}
        }
    }
}

/// The prize quotes while the window is open: id → (staleness at the reopen, lots still resting).
struct GapRace {
    open: BTreeMap<OrderId, (i64, Qty)>,
    /// Every prize quote, resolved or not, to attribute the LP's cancel fees.
    ids: std::collections::BTreeSet<OrderId>,
    m: MondayOpen,
}

impl GapRace {
    /// The LP quotes the reopen price `v` leaves stale by at least θ.
    fn at_reopen(lp: &LpQuotes, v: Px, theta: i64) -> Self {
        let mut g = GapRace {
            open: BTreeMap::new(),
            ids: std::collections::BTreeSet::new(),
            m: MondayOpen::default(),
        };
        for (&id, &(side, px, qty)) in &lp.live {
            let stale = match side {
                Side::Sell => v - px,
                Side::Buy => px - v,
            };
            if stale >= theta {
                g.open.insert(id, (stale, qty));
                g.ids.insert(id);
                g.m.prize += stale as i128 * qty as i128;
                g.m.prize_lots += qty;
            }
        }
        g
    }

    fn on_event(&mut self, ev: &EventKind, class: &impl Fn(AgentId) -> AccountClass, fee_bps: u64) {
        match *ev {
            EventKind::Fill {
                maker_order,
                taker_agent,
                px,
                qty,
                ..
            } => {
                let Some((stale, left)) = self.open.get_mut(&maker_order) else {
                    return;
                };
                let took = *stale as i128 * qty as i128;
                if class(taker_agent) == AccountClass::Sniper {
                    self.m.sniper_took += took;
                    self.m.sniper_taker_fees += px as i128 * qty as i128 * fee_bps as i128;
                } else {
                    self.m.other_took += took;
                }
                *left -= qty;
                if *left == 0 {
                    self.open.remove(&maker_order);
                }
            }
            EventKind::Cancelled { order_id, .. } => {
                if let Some((stale, left)) = self.open.remove(&order_id) {
                    self.m.lp_kept += stale as i128 * left as i128;
                }
            }
            _ => {}
        }
    }
}

/// Every market metric of one run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metrics {
    pub end_ns: u64,
    pub fills: u64,
    pub volume_lots: u64,
    /// Σ p · q over all fills, tick·lots. The "volume" that bps of volume divide by.
    pub notional_tick_lots: i128,
    /// PnL per account class at each horizon, net of fees, in cash units (spec section 10's
    /// formula). Sums to zero across classes at every horizon.
    pub class_pnl: [[i128; NC]; NH],
    /// Gross gain of the taker class from the maker class, before fees, per horizon, tick·lots:
    /// `flow[h][maker][taker]`.
    pub flow: [[[i128; NC]; NC]; NH],
    pub taker_fees: i128,
    /// Priority fees paid to the chain (chain take), cash units.
    pub priority_fees: i128,
    /// Priority fees by the class that paid them, cash units. Sums to `priority_fees`.
    pub priority_fees_by: [i128; NC],
    pub races: RaceStats,
    pub lp: LpSpread,
    /// Regular traders: Σ q · d · (p − v at decision), d = +1 for a buy, tick·lots (a cost).
    pub regular_cost: i128,
    pub regular_notional_tick_lots: i128,
    /// Fill delay of regular traders' orders (fill time − decision time).
    pub regular_delay: Delays,
    /// Fill delay of snipers' orders.
    pub sniper_delay: Delays,
    pub quoted: Quoted,
    pub monday: Option<MondayOpen>,
}

impl Metrics {
    pub fn pnl(&self, h: usize, c: AccountClass) -> i128 {
        self.class_pnl[h][ci(c)]
    }

    /// Sniper take: PnL summed over snipers, cash units.
    pub fn sniper_take(&self, h: usize) -> i128 {
        self.pnl(h, AccountClass::Sniper)
    }

    /// Priority fees one class paid, cash units.
    pub fn priority_fees_of(&self, c: AccountClass) -> i128 {
        self.priority_fees_by[ci(c)]
    }

    /// LP loss to snipers: the snipers' gross gain on fills against LPs, tick·lots.
    pub fn lp_loss_to_snipers(&self, h: usize) -> i128 {
        let (lp, sn) = (ci(AccountClass::Lp), ci(AccountClass::Sniper));
        self.flow[h][lp][sn] - self.flow[h][sn][lp]
    }

    /// Notional in cash units, the denominator for bps of volume.
    pub fn notional_cash(&self) -> i128 {
        self.notional_tick_lots * CASH_PER_TICK_LOT
    }
}

struct Book {
    bids: BTreeMap<Px, Qty>,
    asks: BTreeMap<Px, Qty>,
}

impl Book {
    fn apply(&mut self, side: Side, px: Px, qty: Qty) {
        let m = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        if qty == 0 {
            m.remove(&px);
        } else {
            m.insert(px, qty);
        }
    }

    /// Lots within `DEPTH_BAND_BPS` of v on both sides: |px − v| · 10⁴ ≤ band · v.
    fn depth_near(&self, v: Px) -> u64 {
        let band = (DEPTH_BAND_BPS * v) / 10_000; // whole ticks: |px − v| ≤ band ⇔ the bps test
        let lo = v - band;
        let hi = v + band;
        let sum = |m: &BTreeMap<Px, Qty>| m.range(lo..=hi).map(|(_, &q)| q).sum::<u64>();
        sum(&self.bids) + sum(&self.asks)
    }
}

/// Time integration of the quoted liquidity, split at every book change and price change.
struct Clock<'a> {
    path: &'a PricePath,
    /// Index of the next price change after `t`.
    next_px: usize,
    t: u64,
    q: Quoted,
}

impl Clock<'_> {
    fn advance(&mut self, to: u64, book: &Book) {
        while self.t < to {
            let pts = self.path.points();
            while self.next_px < pts.len() && pts[self.next_px].0 <= self.t {
                self.next_px += 1;
            }
            // The next price change is strictly after `t`, so the segment is never empty.
            let seg_end = match pts.get(self.next_px) {
                Some(p) if p.0 < to => p.0,
                _ => to,
            };
            let dt = seg_end - self.t;
            let v = self.path.at(self.t);
            if let (Some((&bid, _)), Some((&ask, _))) =
                (book.bids.last_key_value(), book.asks.first_key_value())
            {
                self.q.spread_tick_ns += (ask - bid) as i128 * dt as i128;
                self.q.mid2_tick_ns += (ask + bid) as i128 * dt as i128;
                self.q.two_sided_ns += dt;
            }
            self.q.depth_lot_ns += book.depth_near(v) as i128 * dt as i128;
            self.q.total_ns += dt;
            self.t = seg_end;
        }
    }
}

/// Compute every market metric of one run.
pub fn measure(inp: &MeasureInput) -> Metrics {
    assert_eq!(inp.records.len(), inp.decided_ns.len());
    let class = |a: AgentId| -> AccountClass {
        *inp.classes
            .get(&a)
            .unwrap_or_else(|| panic!("agent {a} has no class"))
    };
    // seq → index, for the taker's decision time (seq is the log position in a simulated run,
    // but a hand-built log need not start at 1).
    let index: BTreeMap<u64, usize> = inp
        .records
        .iter()
        .enumerate()
        .map(|(i, r)| (r.seq, i))
        .collect();
    let v_at = |t: u64| inp.path.at(t.min(inp.end_ns));

    let mut m = Metrics {
        end_ns: inp.end_ns,
        fills: 0,
        volume_lots: 0,
        notional_tick_lots: 0,
        class_pnl: [[0; NC]; NH],
        flow: [[[0; NC]; NC]; NH],
        taker_fees: 0,
        priority_fees: 0,
        priority_fees_by: [0; NC],
        races: RaceStats::default(),
        lp: LpSpread::default(),
        regular_cost: 0,
        regular_notional_tick_lots: 0,
        regular_delay: Delays::default(),
        sniper_delay: Delays::default(),
        quoted: Quoted::default(),
        monday: None,
    };
    let mut engine = Engine::new();
    let mut races =
        RaceTracker::new(inp.theta_ticks, inp.classes.clone()).with_cancel_arrivals(inp.records);
    let mut book = Book {
        bids: BTreeMap::new(),
        asks: BTreeMap::new(),
    };
    let mut clock = Clock {
        path: inp.path,
        next_px: 0,
        t: 0,
        q: Quoted::default(),
    };
    let (mut regular_delays, mut sniper_delays) = (Vec::new(), Vec::new());

    // Monday open: the book at the close, the last visible price, and the window.
    let mut at_close: Option<(u64, u64)> = None; // (bid lots, ask lots)
    let mut at_reopen: Option<(u64, u64)> = None;
    let mut last_visible: Option<Px> = None;
    let mut reopen_px: Option<Px> = None;
    let mut window_pnl = 0i128;
    let mut window_lp_loss = 0i128;
    let mut window_notional = 0i128;
    let mut window_chain = 0i128;
    let mut window_chain_lp = 0i128;
    let in_window = |t: u64| {
        inp.weekend
            .is_some_and(|w| t >= w.reopen_ns && t < w.reopen_ns + inp.monday_window_ns)
    };
    let class_is = |a: AgentId, c: AccountClass| inp.classes.get(&a) == Some(&c);
    // The gap race: the LP quotes resting at the reopen, and each sniper's first decision after
    // the reopen. A sniper acts only on prices it sees and no price is visible over the
    // weekend, so that decision is its reaction to the reopen tick.
    let mut lp_quotes = LpQuotes::default();
    let mut gap: Option<GapRace> = None;
    let mut first_reaction: BTreeMap<AgentId, u64> = BTreeMap::new();
    if let Some(w) = inp.weekend {
        for (r, &d) in inp.records.iter().zip(inp.decided_ns) {
            if d >= w.reopen_ns
                && matches!(r.kind, MsgKind::New { .. })
                && class_is(r.agent, AccountClass::Sniper)
            {
                let e = first_reaction.entry(r.agent).or_insert(d);
                *e = (*e).min(d);
            }
        }
    }

    for (i, rec) in inp.records.iter().enumerate() {
        let t = processed_ns(rec, inp.block_ns);
        if let Some(w) = inp.weekend {
            if at_close.is_none() && t >= w.close_ns {
                clock.advance(w.close_ns, &book);
                let lots = |m: &BTreeMap<Px, Qty>| m.values().sum::<u64>();
                at_close = Some((lots(&book.bids), lots(&book.asks)));
            }
            if let MsgKind::Tick { px_ticks } = rec.kind {
                if rec.t_ns < w.reopen_ns {
                    last_visible = Some(px_ticks);
                } else if reopen_px.is_none() {
                    reopen_px = Some(px_ticks);
                    let lots = |m: &BTreeMap<Px, Qty>| m.values().sum::<u64>();
                    at_reopen = Some((lots(&book.bids), lots(&book.asks)));
                    gap = Some(GapRace::at_reopen(&lp_quotes, px_ticks, inp.theta_ticks));
                }
            }
        }
        clock.advance(t, &book);

        // Inclusion fee: sender → chain.
        let fee = rec.kind.priority_fee() as i128;
        if fee > 0 {
            m.priority_fees += fee;
            let c = class(rec.agent);
            m.priority_fees_by[ci(c)] += fee;
            for h in 0..NH {
                m.class_pnl[h][ci(c)] -= fee;
                m.class_pnl[h][ci(AccountClass::Chain)] += fee;
            }
            if in_window(t) {
                window_chain += fee;
                if c == AccountClass::Lp {
                    window_chain_lp += fee;
                }
                if c == AccountClass::Sniper {
                    window_pnl -= fee;
                }
                if let Some(g) = gap.as_mut() {
                    match rec.kind {
                        MsgKind::New { .. }
                            if c == AccountClass::Sniper
                                && first_reaction.get(&rec.agent) == Some(&inp.decided_ns[i]) =>
                        {
                            g.m.sniper_priority_fees += fee;
                        }
                        MsgKind::Cancel { order_id, .. }
                            if c == AccountClass::Lp && g.ids.contains(&order_id) =>
                        {
                            g.m.lp_priority_fees += fee;
                        }
                        _ => {}
                    }
                }
            }
        }

        let events = engine.on_message(rec);
        races.on_record(rec, &events);
        let mut taker_filled = false;
        for ev in &events {
            let is_lp = matches!(ev.kind, EventKind::Accepted { agent, .. }
                if class_is(agent, AccountClass::Lp));
            lp_quotes.on_event(&ev.kind, is_lp);
            if let (Some(g), true) = (gap.as_mut(), in_window(t)) {
                g.on_event(&ev.kind, &class, inp.taker_fee_bps);
            }
            match ev.kind {
                EventKind::BookChange { side, px, qty } => book.apply(side, px, qty),
                EventKind::Fill {
                    maker_agent,
                    taker_agent,
                    taker_order,
                    taker_side,
                    px,
                    qty,
                    ..
                } => {
                    taker_filled = true;
                    let (mc, tc) = (class(maker_agent), class(taker_agent));
                    let q = qty as i128;
                    let d: i128 = match taker_side {
                        Side::Buy => 1,
                        Side::Sell => -1,
                    };
                    m.fills += 1;
                    m.volume_lots += qty;
                    m.notional_tick_lots += px as i128 * q;
                    let v0 = v_at(t);
                    let mut gains = [0i128; NH];
                    for (h, &dh) in HORIZONS_NS.iter().enumerate() {
                        let g = d * q * (v_at(t.saturating_add(dh)) - px) as i128;
                        gains[h] = g;
                        m.flow[h][ci(mc)][ci(tc)] += g;
                        m.class_pnl[h][ci(tc)] += g * CASH_PER_TICK_LOT;
                        m.class_pnl[h][ci(mc)] -= g * CASH_PER_TICK_LOT;
                    }
                    // Taker fee: taker → protocol.
                    let tfee = px as i128 * q * inp.taker_fee_bps as i128;
                    m.taker_fees += tfee;
                    for h in 0..NH {
                        m.class_pnl[h][ci(tc)] -= tfee;
                        m.class_pnl[h][ci(AccountClass::Protocol)] += tfee;
                    }
                    // LP effective and realized half-spread. s = +1 when the LP sold.
                    for (lp_is_taker, c) in [(false, mc), (true, tc)] {
                        if c != AccountClass::Lp {
                            continue;
                        }
                        let s = if lp_is_taker { -d } else { d };
                        m.lp.lots += qty;
                        m.lp.notional_tick_lots += px as i128 * q;
                        m.lp.effective += q * s * (px - v0) as i128;
                        for (h, &dh) in HORIZONS_NS.iter().enumerate() {
                            m.lp.realized[h] += q * s * (px - v_at(t.saturating_add(dh))) as i128;
                        }
                    }
                    if tc == AccountClass::Regular {
                        let dec = inp.decided_ns[index[&taker_order]];
                        m.regular_cost += d * q * (px - inp.path.at(dec)) as i128;
                        m.regular_notional_tick_lots += px as i128 * q;
                    }
                    if in_window(t) {
                        window_notional += px as i128 * q;
                        let g = gains[HEADLINE_H];
                        if tc == AccountClass::Sniper {
                            window_pnl += g * CASH_PER_TICK_LOT - tfee;
                            if mc == AccountClass::Lp {
                                window_lp_loss += g;
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        if taker_filled {
            if let MsgKind::New { .. } = rec.kind {
                let delay = t.saturating_sub(inp.decided_ns[i]);
                match class(rec.agent) {
                    AccountClass::Regular => regular_delays.push(delay),
                    AccountClass::Sniper => sniper_delays.push(delay),
                    _ => {}
                }
            }
        }
    }
    clock.advance(inp.end_ns, &book);

    m.races = races.stats();
    m.regular_delay = Delays::from(regular_delays);
    m.sniper_delay = Delays::from(sniper_delays);
    m.quoted = clock.q;
    if let (Some(_), Some((bids, asks)), Some(before), Some(after)) =
        (inp.weekend, at_close, last_visible, reopen_px)
    {
        let gap_ticks = after - before;
        let (rb, ra) = at_reopen.unwrap_or_default();
        m.monday = Some(MondayOpen {
            gap_ticks,
            resting_lots: if gap_ticks >= 0 { asks } else { bids },
            resting_lots_at_reopen: if gap_ticks >= 0 { ra } else { rb },
            sniper_pnl: window_pnl,
            lp_loss_to_snipers: window_lp_loss,
            chain_fees: window_chain,
            chain_fees_lp: window_chain_lp,
            notional_tick_lots: window_notional,
            ..gap.map(|g| g.m).unwrap_or_default()
        });
    }
    m
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine::Tif;

    const LP: AgentId = 1;
    const SNIPER: AgentId = 2;
    const TRADER: AgentId = 3;
    const MS: u64 = 1_000_000;
    const S: u64 = 1_000_000_000;

    fn classes() -> BTreeMap<AgentId, AccountClass> {
        [
            (LP, AccountClass::Lp),
            (SNIPER, AccountClass::Sniper),
            (TRADER, AccountClass::Regular),
            (0, AccountClass::Protocol), // the feed sends ticks; never trades or pays
        ]
        .into()
    }

    fn new(side: Side, px: Px, qty: Qty, tif: Tif, fee: u64) -> MsgKind {
        MsgKind::New {
            side,
            px_ticks: px,
            qty_lots: qty,
            tif,
            priority_fee: fee,
        }
    }

    fn tick(px: Px) -> MsgKind {
        MsgKind::Tick { px_ticks: px }
    }

    /// A tiny hand-built log: (arrival ms, decided ms, block, agent, message). Seq = position.
    struct Log {
        recs: Vec<LogRecord>,
        decided: Vec<u64>,
    }

    fn log(rows: &[(u64, u64, u64, AgentId, MsgKind)]) -> Log {
        Log {
            recs: rows
                .iter()
                .enumerate()
                .map(|(i, &(t, _, block, agent, kind))| LogRecord {
                    seq: i as u64 + 1,
                    t_ns: t * MS,
                    block,
                    agent,
                    kind,
                })
                .collect(),
            decided: rows.iter().map(|r| r.1 * MS).collect(),
        }
    }

    fn run(l: &Log, path: &[(u64, Px)], block_ms: u64, end_ms: u64) -> Metrics {
        run_with(l, path, block_ms, end_ms, None)
    }

    fn run_with(
        l: &Log,
        path: &[(u64, Px)],
        block_ms: u64,
        end_ms: u64,
        weekend: Option<Weekend>,
    ) -> Metrics {
        let path = PricePath::new(path.iter().map(|&(t, p)| (t * MS, p)).collect());
        let classes = classes();
        measure(&MeasureInput {
            records: &l.recs,
            decided_ns: &l.decided,
            classes: &classes,
            path: &path,
            block_ns: block_ms * MS,
            end_ns: end_ms * MS,
            theta_ticks: 1,
            taker_fee_bps: 1,
            weekend,
            monday_window_ns: 60 * S,
        })
    }

    /// The race from the mental-model doc, at B = 0: the LP's ask rests at 100.02, the price
    /// jumps to 100.10, a sniper takes 10 lots at 100.02 paying a 4-cent priority fee, the LP's
    /// cancel arrives late. Then the price drifts to 100.12 after 5 s.
    fn snipe_log() -> (Log, Vec<(u64, Px)>) {
        let l = log(&[
            (0, 0, 0, 0, tick(10_000)),
            (1, 0, 1, LP, new(Side::Sell, 10_002, 10, Tif::Gtc, 0)),
            (100, 100, 2, 0, tick(10_010)),
            (
                108,
                105,
                3,
                SNIPER,
                new(Side::Buy, 10_009, 10, Tif::Ioc, 40_000),
            ),
            (
                123,
                120,
                4,
                LP,
                MsgKind::Cancel {
                    order_id: 2,
                    priority_fee: 0,
                },
            ),
        ]);
        let path = vec![(0, 10_000), (100, 10_010), (5_100, 10_012)];
        (l, path)
    }

    #[test]
    fn sniper_take_lp_loss_and_chain_take() {
        let (l, path) = snipe_log();
        let m = run(&l, &path, 0, 120_000);
        assert_eq!(
            (m.fills, m.volume_lots, m.notional_tick_lots),
            (1, 10, 100_020)
        );
        // h = 0 and 1 s: v = 100.10, gain 8 ticks × 10 lots. h = 10 s, 60 s: v = 100.12.
        assert_eq!(m.lp_loss_to_snipers(0), 80);
        assert_eq!(m.lp_loss_to_snipers(1), 80);
        assert_eq!(m.lp_loss_to_snipers(2), 100);
        assert_eq!(m.lp_loss_to_snipers(3), 100);
        // Sniper take is net of fees: 80 tick·lots = 800_000 cash, minus the 40_000 priority
        // fee, minus the 1 bp taker fee on 100_020 tick·lots (= 100_020 cash).
        assert_eq!(m.sniper_take(1), 800_000 - 40_000 - 100_020);
        assert_eq!(m.priority_fees, 40_000);
        assert_eq!(m.pnl(1, AccountClass::Chain), 40_000);
        assert_eq!(m.pnl(1, AccountClass::Protocol), 100_020);
        assert_eq!(m.pnl(1, AccountClass::Lp), -800_000);
        // Zero-sum at every horizon.
        for h in 0..NH {
            assert_eq!(m.class_pnl[h].iter().sum::<i128>(), 0, "h={h}");
        }
        // One race, won by the sniper.
        assert_eq!((m.races.sniper_won, m.races.lp_won), (1, 0));
        // Sniper's fill delay: decided at 105 ms, filled at 108 ms (B = 0).
        assert_eq!(m.sniper_delay.count, 1);
        assert_eq!(m.sniper_delay.median_ns, 3 * MS);
    }

    #[test]
    fn lp_adverse_selection_is_effective_minus_realized() {
        let (l, path) = snipe_log();
        let m = run(&l, &path, 0, 120_000);
        // The LP sold (s = +1) 10 at 100.02 when v = 100.10: effective = 10 × (2 − 10) = −80.
        assert_eq!(m.lp.effective, -80);
        // Realized at 10 s: 10 × (100.02 − 100.12) = −100. Adverse selection = −80 − (−100) = 20.
        assert_eq!(m.lp.realized[2], -100);
        assert_eq!(m.lp.adverse(2), 20);
        assert_eq!(m.lp.adverse(1), 0, "v unchanged 1 s after the fill");
        assert_eq!(m.lp.notional_tick_lots, 100_020);
    }

    #[test]
    fn regular_cost_and_fill_delay_include_block_wait() {
        // B = 100 ms. The trader decides at 30 ms when v = 100.00, its buy arrives at 80 ms (in
        // block 0) and fills at the block close, 100 ms, against an ask at 100.01. Meanwhile v
        // moved to 100.03 at 90 ms, which is not the trader's cost basis.
        // Ticks never wait, so the 90 ms tick is logged before block 0's messages.
        let l = log(&[
            (0, 0, 0, 0, tick(10_000)),
            (90, 90, 0, 0, tick(10_003)),
            (10, 0, 0, LP, new(Side::Sell, 10_001, 5, Tif::Gtc, 0)),
            (80, 30, 0, TRADER, new(Side::Buy, 10_006, 2, Tif::Ioc, 0)),
        ]);
        let m = run(&l, &[(0, 10_000), (90, 10_003)], 100, 1_000);
        // Cost against v at decision: 2 × (100.01 − 100.00) = 2 tick·lots.
        assert_eq!(m.regular_cost, 2);
        assert_eq!(m.regular_notional_tick_lots, 20_002);
        // Delay = block close (100 ms) − decision (30 ms).
        assert_eq!(m.regular_delay.count, 1);
        assert_eq!(m.regular_delay.median_ns, 70 * MS);
        assert_eq!(m.regular_delay.p99_ns, 70 * MS);
    }

    #[test]
    fn delay_percentiles_by_nearest_rank() {
        let d = Delays::from((1..=200).rev().collect());
        assert_eq!((d.count, d.median_ns, d.p99_ns), (200, 100, 198));
        let d = Delays::from(vec![7]);
        assert_eq!((d.median_ns, d.p99_ns), (7, 7));
    }

    #[test]
    fn quoted_liquidity_is_time_weighted() {
        // Bid 99.99 × 10 and ask 100.01 × 10 from 0 ms; at 500 ms the ask moves out to 100.20.
        // v = 100.00 throughout. Run length 1 s.
        let l = log(&[
            (0, 0, 0, 0, tick(10_000)),
            (0, 0, 1, LP, new(Side::Buy, 9_999, 10, Tif::Gtc, 0)),
            (0, 0, 2, LP, new(Side::Sell, 10_001, 10, Tif::Gtc, 0)),
            (
                500,
                500,
                3,
                LP,
                MsgKind::Cancel {
                    order_id: 3,
                    priority_fee: 0,
                },
            ),
            (500, 500, 4, LP, new(Side::Sell, 10_020, 10, Tif::Gtc, 0)),
        ]);
        let m = run(&l, &[(0, 10_000)], 0, 1_000);
        let q = m.quoted;
        assert_eq!(q.total_ns, 1_000 * MS);
        assert_eq!(q.two_sided_ns, 1_000 * MS);
        // Spread 2 ticks for 500 ms, 21 ticks for 500 ms.
        assert_eq!(q.spread_tick_ns, (2 * 500 + 21 * 500) as i128 * MS as i128);
        assert_eq!(
            q.mid2_tick_ns,
            ((20_000 * 500) + (20_019 * 500)) as i128 * MS as i128
        );
        // Depth within 10 bps of 100.00 (±10 ticks): 20 lots, then 10 (the 100.20 ask is out).
        assert_eq!(q.depth_lot_ns, (20 * 500 + 10 * 500) as i128 * MS as i128);
    }

    #[test]
    fn depth_band_follows_the_true_price() {
        // One ask at 100.05. v moves from 100.00 to 100.20 at 400 ms (hidden or not, it counts).
        let l = log(&[
            (0, 0, 0, 0, tick(10_000)),
            (0, 0, 1, LP, new(Side::Sell, 10_005, 4, Tif::Gtc, 0)),
        ]);
        let m = run(&l, &[(0, 10_000), (400, 10_020)], 0, 1_000);
        assert_eq!(m.quoted.depth_lot_ns, 4 * 400 * MS as i128);
        assert_eq!(m.quoted.two_sided_ns, 0, "one-sided book has no spread");
    }

    #[test]
    fn monday_open_take() {
        // Close at 1 s, reopen at 10 s. At the close the LP has 15 lots of asks resting. The
        // hidden price ends at 100.50; the reopen tick jumps there from 100.00, a 50-tick gap.
        // A sniper takes 10 lots at 100.01 at 10.008 s (in the window), paying no fees but the
        // 1 bp taker fee; a later snipe at 80 s is outside the 60 s window.
        let l = log(&[
            (0, 0, 0, 0, tick(10_000)),
            (0, 0, 1, LP, new(Side::Sell, 10_001, 10, Tif::Gtc, 0)),
            (0, 0, 2, LP, new(Side::Sell, 10_002, 5, Tif::Gtc, 0)),
            (10_000, 10_000, 3, 0, tick(10_050)),
            (
                10_008,
                10_005,
                4,
                SNIPER,
                new(Side::Buy, 10_049, 10, Tif::Ioc, 0),
            ),
            (
                80_000,
                80_000,
                5,
                SNIPER,
                new(Side::Buy, 10_049, 5, Tif::Ioc, 0),
            ),
        ]);
        let path = [(0, 10_000), (5_000, 10_030), (9_000, 10_050)];
        let w = Weekend {
            close_ns: S,
            reopen_ns: 10 * S,
        };
        let m = run_with(&l, &path, 0, 100_000, Some(w));
        let mo = m.monday.unwrap();
        assert_eq!(mo.gap_ticks, 50);
        assert_eq!(mo.resting_lots, 15);
        assert_eq!(mo.resting_lots_at_reopen, 15);
        // 10 × (100.50 − 100.01) = 490 tick·lots = 4_900_000 cash, minus the fee 100_010.
        assert_eq!(mo.lp_loss_to_snipers, 490);
        assert_eq!(mo.notional_tick_lots, 100_010);
        assert_eq!(mo.sniper_pnl, 4_900_000 - 100_010);
        // Gap prize: 49 × 10 + 48 × 5 = 730 tick·lots. The sniper took the 490; the 100.02 ask
        // is still resting when the window ends (the 80 s snipe is outside it).
        assert_eq!((mo.prize, mo.prize_lots), (730, 15));
        assert_eq!(mo.share(), (490, 730));
        assert_eq!((mo.lp_kept, mo.other_took), (0, 0));
        assert_eq!(mo.sniper_taker_fees, 100_010);
        assert_eq!(mo.net_share(), (4_900_000 - 100_010, 7_300_000));
        // Over the whole run the second snipe counts too.
        assert_eq!(m.lp_loss_to_snipers(1), 490 + 5 * 48);
    }

    /// The case the spec's ratio got wrong. Between the close and the reopen a regular trader
    /// lifts one LP ask, so the book that faces the gap is not the book at the close; the LP's
    /// quotes sit 3 and 6 ticks out, so a 5-tick gap leaves one of them stale by 2 ticks, not 5;
    /// a race after the reopen pays the snipers too; and under PGA the losing sniper pays for
    /// nothing. The share counts only the quotes that faced the gap, valued at the reopen price.
    #[test]
    fn monday_share_counts_only_the_quotes_that_faced_the_gap() {
        const SNIPER2: AgentId = 4;
        let cancel = |order_id, priority_fee| MsgKind::Cancel {
            order_id,
            priority_fee,
        };
        let l = log(&[
            (0, 0, 0, 0, tick(10_000)),
            (0, 0, 1, LP, new(Side::Sell, 10_003, 10, Tif::Gtc, 0)), // id 2
            (0, 0, 2, LP, new(Side::Sell, 10_006, 5, Tif::Gtc, 0)),  // id 3
            (0, 0, 3, LP, new(Side::Buy, 9_997, 10, Tif::Gtc, 0)),   // id 4
            (500, 500, 4, LP, new(Side::Sell, 10_001, 10, Tif::Gtc, 0)), // id 5
            // Weekend (close at 1 s): a regular trader takes the 100.01 ask.
            (
                5_000,
                5_000,
                5,
                TRADER,
                new(Side::Buy, 10_006, 10, Tif::Ioc, 0),
            ),
            // Reopen at 10 s: +5 ticks. Only the 100.03 ask is stale (by 2): prize 20.
            (10_000, 10_000, 6, 0, tick(10_005)),
            // Sniper 1 (its view of the book is old) takes 6 of the 10 lots, bidding 3 cents.
            (
                10_008,
                10_005,
                7,
                SNIPER,
                new(Side::Buy, 10_004, 6, Tif::Ioc, 30_000),
            ),
            // The LP cancels the other 4 (bidding 0.5 cents) and the fresh 100.06 ask (0.1
            // cent, not a prize quote), and requotes.
            (10_023, 10_020, 8, LP, cancel(2, 5_000)),
            (10_023, 10_020, 9, LP, cancel(3, 1_000)),
            (
                10_024,
                10_020,
                10,
                LP,
                new(Side::Sell, 10_006, 10, Tif::Gtc, 0),
            ), // id 11
            // Sniper 2 reacts to the reopen too, late: it finds nothing but pays its 2 cents.
            (
                10_030,
                10_006,
                11,
                SNIPER2,
                new(Side::Buy, 10_004, 10, Tif::Ioc, 20_000),
            ),
            // A race after the reopen, inside the window: not part of the gap.
            (12_000, 12_000, 12, 0, tick(10_010)),
            (
                12_008,
                12_005,
                13,
                SNIPER,
                new(Side::Buy, 10_009, 10, Tif::Ioc, 1_000),
            ),
        ]);
        let path = [
            (0, 10_000),
            (3_000, 10_002),
            (9_000, 10_005),
            (12_000, 10_010),
        ];
        let w = Weekend {
            close_ns: S,
            reopen_ns: 10 * S,
        };
        let mut classes = classes();
        classes.insert(SNIPER2, AccountClass::Sniper);
        let path = PricePath::new(path.iter().map(|&(t, p)| (t * MS, p)).collect());
        let m = measure(&MeasureInput {
            records: &l.recs,
            decided_ns: &l.decided,
            classes: &classes,
            path: &path,
            block_ns: 0,
            end_ns: 100 * S,
            theta_ticks: 1,
            taker_fee_bps: 1,
            weekend: Some(w),
            monday_window_ns: 60 * S,
        });
        let mo = m.monday.unwrap();
        assert_eq!(mo.gap_ticks, 5);
        // 25 ask lots at the close, 15 at the reopen. The spec's denominator would have been
        // 5 × 25 = 125 tick·lots for a prize of 20.
        assert_eq!((mo.resting_lots, mo.resting_lots_at_reopen), (25, 15));
        assert_eq!((mo.prize, mo.prize_lots), (20, 10));
        // Snipers took 6 lots × 2 ticks, the LP kept 4 × 2: the shares add up to 1.
        assert_eq!(mo.share(), (12, 20));
        assert_eq!(mo.lp_share(), (8, 20));
        assert_eq!(mo.other_took, 0);
        // Fees for the gap: both snipers' first reactions (3 + 2 cents) and the LP's cancel of
        // the prize quote (0.5 cent); not the 0.1-cent cancel or the later race's bid.
        assert_eq!(mo.sniper_taker_fees, 10_003 * 6);
        assert_eq!(mo.sniper_priority_fees, 50_000);
        assert_eq!(mo.lp_priority_fees, 5_000);
        assert_eq!(mo.net_share(), (120_000 - 60_018 - 50_000, 200_000));
        assert_eq!(mo.chain_share(), (55_000, 200_000));
        // The window's sniper take still holds everything: the gap fill (6 × 2 at h = 1 s) and
        // the later race (10 × (100.10 − 100.06)), net of every sniper fee in the window.
        assert_eq!(
            mo.sniper_pnl,
            (12 + 40) * CASH_PER_TICK_LOT - 60_018 - 100_060 - 51_000
        );
        assert_eq!(mo.chain_fees, 57_000);
        assert_eq!(mo.chain_fees_lp, 6_000);
        assert_eq!(m.priority_fees_of(AccountClass::Lp), 6_000);
        assert_eq!(m.priority_fees_of(AccountClass::Sniper), 51_000);
    }

    #[test]
    fn monday_share_is_undefined_when_no_quote_faces_the_gap() {
        // A 2-tick gap against asks 3 ticks out: nothing is stale, so there is no prize.
        let l = log(&[
            (0, 0, 0, 0, tick(10_000)),
            (0, 0, 1, LP, new(Side::Sell, 10_003, 10, Tif::Gtc, 0)),
            (10_000, 10_000, 2, 0, tick(10_002)),
        ]);
        let w = Weekend {
            close_ns: S,
            reopen_ns: 10 * S,
        };
        let m = run_with(&l, &[(0, 10_000), (9_000, 10_002)], 0, 20_000, Some(w));
        let mo = m.monday.unwrap();
        assert_eq!((mo.gap_ticks, mo.resting_lots), (2, 10));
        assert_eq!(mo.share(), (0, 0));
    }

    #[test]
    fn markouts_past_the_end_use_the_last_price() {
        let (l, path) = snipe_log();
        // The run ends at 2 s; the 5.1 s move is beyond it.
        let m = run(&l, &path, 0, 2_000);
        assert_eq!(m.lp_loss_to_snipers(3), 80);
    }
}
