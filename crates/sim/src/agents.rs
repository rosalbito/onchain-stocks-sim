//! The three agent types (spec section 8). Rules are fixed and simple; only parameters vary.
//!
//! Agents never touch the engine. They react to what they observe (prices and market data, both
//! seen late) by returning messages, which the event loop delivers to the sequencer late.

use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use engine::{AgentId, Event, EventKind, MsgKind, OrderId, Px, Qty, Side, Tif};
use metrics::CASH_PER_TICK_LOT;

use crate::config::{LpCfg, RegularCfg, SniperCfg, WeekendPolicy};
use crate::rng::Rng;

/// Something an agent observes.
#[derive(Debug, Clone)]
pub enum Obs {
    /// A Nasdaq price (a logged `Tick`), seen after the agent's see-latency.
    Price(Px),
    /// Engine events from one block (or one message when B = 0), seen after the see-latency.
    Market(Rc<[Event]>),
    /// The market just closed for the weekend (a calendar fact, known without latency).
    Closed,
}

fn new_order(side: Side, px: Px, qty: Qty, tif: Tif, priority_fee: u64) -> MsgKind {
    MsgKind::New {
        side,
        px_ticks: px,
        qty_lots: qty,
        tif,
        priority_fee,
    }
}

// ---------------------------------------------------------------------------------------------
// Liquidity provider
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct LiveQuote {
    side: Side,
    px: Px,
    qty: Qty,
    /// Quote generation it was posted in. Older generations are obsolete.
    generation: u64,
    cancel_sent: bool,
}

#[derive(Debug, Clone, Copy)]
struct PendingQuote {
    side: Side,
    px: Px,
    qty: Qty,
    generation: u64,
}

/// Quotes two levels each side around the last price it saw. When the seen price moves by the
/// requote threshold or more, cancels everything and requotes. Stops quoting a side that could
/// push inventory past the cap. Refills a level of its current quote when it sees it filled.
///
/// Order ids are the `seq` of the `New`, so the LP learns them from the `Accepted` events it
/// sees. Orders it has sent but not yet seen accepted are tracked as pending; if they are
/// obsolete by the time their ack arrives, they are cancelled then.
///
/// Under PGA it bids β × exposure on each cancel, where exposure is what a sniper could take
/// from that quote at the last price the LP saw: max(0, staleness) × remaining lots.
#[derive(Debug)]
pub struct Lp {
    id: AgentId,
    cfg: LpCfg,
    pga: bool,
    seen_px: Option<Px>,
    center: Option<Px>,
    closed: bool,
    quoted_closed: bool,
    generation: u64,
    live: BTreeMap<OrderId, LiveQuote>,
    pending: VecDeque<PendingQuote>,
    /// Inventory as known from the fills it has seen.
    inventory: i64,
}

impl Lp {
    pub fn new(id: AgentId, cfg: &LpCfg, pga: bool) -> Self {
        Self {
            id,
            cfg: cfg.clone(),
            pga,
            seen_px: None,
            center: None,
            closed: false,
            quoted_closed: false,
            generation: 0,
            live: BTreeMap::new(),
            pending: VecDeque::new(),
            inventory: 0,
        }
    }

    pub fn inventory_seen(&self) -> i64 {
        self.inventory
    }

    pub fn observe(&mut self, obs: &Obs) -> Vec<MsgKind> {
        let mut out = Vec::new();
        match obs {
            Obs::Price(p) => {
                // A visible price means the market is open.
                self.closed = false;
                self.seen_px = Some(*p);
                let moved = self
                    .center
                    .is_none_or(|c| (p - c).abs() >= self.cfg.requote_ticks);
                if moved || self.quoted_closed {
                    self.requote(*p, &mut out);
                }
            }
            Obs::Closed => {
                self.closed = true;
                if self.cfg.weekend_policy != WeekendPolicy::Keep {
                    if let Some(p) = self.seen_px {
                        self.requote(p, &mut out);
                    }
                }
            }
            Obs::Market(events) => self.on_market(events, &mut out),
        }
        out
    }

    fn quoting_allowed(&self) -> bool {
        !(self.closed && self.cfg.weekend_policy == WeekendPolicy::Withdraw)
    }

    /// The PGA bid on a cancel: β × exposure, in cash units. Zero unless the rule is PGA.
    fn cancel_fee(&self, q: &LiveQuote) -> u64 {
        let (Some(v), true) = (self.seen_px, self.pga) else {
            return 0;
        };
        let staleness = match q.side {
            Side::Sell => v - q.px,
            Side::Buy => q.px - v,
        };
        let exposure = staleness.max(0) as i128 * q.qty as i128;
        (exposure * CASH_PER_TICK_LOT * self.cfg.beta_bps as i128 / 10_000) as u64
    }

    /// Cancel every live quote of a generation older than `before` (or all, with `u64::MAX`)
    /// that has no cancel in flight yet.
    fn cancel_live(&mut self, before: u64, out: &mut Vec<MsgKind>) {
        let ids: Vec<OrderId> = self
            .live
            .iter()
            .filter(|(_, q)| !q.cancel_sent && q.generation < before)
            .map(|(&id, _)| id)
            .collect();
        for id in ids {
            let priority_fee = self.cancel_fee(&self.live[&id]);
            self.live.get_mut(&id).expect("live").cancel_sent = true;
            out.push(MsgKind::Cancel {
                order_id: id,
                priority_fee,
            });
        }
    }

    fn requote(&mut self, p: Px, out: &mut Vec<MsgKind>) {
        self.cancel_live(u64::MAX, out);
        // Everything still pending becomes obsolete.
        self.generation += 1;
        self.center = Some(p);
        self.quoted_closed = self.closed;
        if !self.quoting_allowed() {
            return;
        }
        for side in [Side::Buy, Side::Sell] {
            if self.side_fits(side) {
                self.post_ladder(side, p, out);
            }
        }
    }

    fn width(&self) -> i64 {
        if self.closed && self.cfg.weekend_policy == WeekendPolicy::Widen {
            self.cfg.weekend_widen
        } else {
            1
        }
    }

    /// Whether a full ladder on `side` keeps inventory within the cap if it all fills.
    fn side_fits(&self, side: Side) -> bool {
        let total: i64 = self.cfg.levels.iter().map(|l| l.qty_lots as i64).sum();
        let cap = self.cfg.inventory_cap_lots;
        match side {
            Side::Buy => self.inventory + total <= cap,
            Side::Sell => self.inventory - total >= -cap,
        }
    }

    fn post_ladder(&mut self, side: Side, center: Px, out: &mut Vec<MsgKind>) {
        let width = self.width();
        for l in self.cfg.levels.clone() {
            let px = match side {
                Side::Buy => center - l.offset_ticks * width,
                Side::Sell => center + l.offset_ticks * width,
            };
            self.post(side, px, l.qty_lots, out);
        }
    }

    fn post(&mut self, side: Side, px: Px, qty: Qty, out: &mut Vec<MsgKind>) {
        if px <= 0 || qty == 0 {
            return;
        }
        out.push(new_order(side, px, qty, Tif::Gtc, 0));
        self.pending.push_back(PendingQuote {
            side,
            px,
            qty,
            generation: self.generation,
        });
    }

    /// Lots this LP has resting or pending on `side` in its current quote.
    fn current_side_qty(&self, side: Side) -> i64 {
        let live: Qty = self
            .live
            .values()
            .filter(|q| q.side == side && !q.cancel_sent && q.generation == self.generation)
            .map(|q| q.qty)
            .sum();
        let pending: Qty = self
            .pending
            .iter()
            .filter(|q| q.side == side && q.generation == self.generation)
            .map(|q| q.qty)
            .sum();
        (live + pending) as i64
    }

    fn on_market(&mut self, events: &[Event], out: &mut Vec<MsgKind>) {
        let mut refill: Vec<(Side, Px, Qty)> = Vec::new();
        for ev in events {
            match ev.kind {
                EventKind::Accepted {
                    order_id,
                    agent,
                    side,
                    px,
                    qty,
                    ..
                } if agent == self.id => {
                    let p = self
                        .pending
                        .pop_front()
                        .expect("LP acks arrive in send order");
                    debug_assert_eq!((p.side, p.px, p.qty), (side, px, qty));
                    self.live.insert(
                        order_id,
                        LiveQuote {
                            side,
                            px,
                            qty,
                            generation: p.generation,
                            cancel_sent: false,
                        },
                    );
                }
                EventKind::Fill {
                    maker_order,
                    maker_agent,
                    taker_agent,
                    taker_side,
                    px,
                    qty,
                    maker_remaining,
                    ..
                } => {
                    if maker_agent == self.id {
                        let side = taker_side.opposite();
                        self.inventory += match side {
                            Side::Buy => qty as i64,
                            Side::Sell => -(qty as i64),
                        };
                        if let Some(q) = self.live.get_mut(&maker_order) {
                            q.qty = maker_remaining;
                            if q.generation == self.generation && !q.cancel_sent {
                                refill.push((side, px, qty));
                            }
                            if maker_remaining == 0 {
                                self.live.remove(&maker_order);
                            }
                        }
                    }
                    if taker_agent == self.id {
                        self.inventory += match taker_side {
                            Side::Buy => qty as i64,
                            Side::Sell => -(qty as i64),
                        };
                    }
                }
                EventKind::Cancelled {
                    order_id, agent, ..
                }
                | EventKind::CancelMissed { order_id, agent } => {
                    if agent == self.id {
                        self.live.remove(&order_id);
                    }
                }
                _ => {}
            }
        }
        // Orders acked after a requote are obsolete: cancel them now that their ids are known.
        self.cancel_live(self.generation, out);
        if !self.quoting_allowed() {
            return;
        }
        let cap = self.cfg.inventory_cap_lots;
        for (side, px, qty) in refill {
            let fits = match side {
                Side::Buy => self.inventory + self.current_side_qty(Side::Buy) + qty as i64 <= cap,
                Side::Sell => {
                    self.inventory - self.current_side_qty(Side::Sell) - qty as i64 >= -cap
                }
            };
            if fits {
                self.post(side, px, qty, out);
            }
        }
        // Top up: a side the cap stopped (or thinned) comes back to the full ladder once
        // inventory allows it again (docs/DECISIONS.md, entry 50). Without this, a side lost to
        // the cap stays empty until the next price move, and over the hidden weekend there is
        // none. When the cap does not bind, the refills above already restored every level and
        // this posts nothing.
        if let Some(c) = self.center {
            let width = self.width();
            for side in [Side::Buy, Side::Sell] {
                if !self.side_fits(side) {
                    continue;
                }
                for l in self.cfg.levels.clone() {
                    let px = match side {
                        Side::Buy => c - l.offset_ticks * width,
                        Side::Sell => c + l.offset_ticks * width,
                    };
                    let have = self.current_level_qty(side, px);
                    if have < l.qty_lots {
                        self.post(side, px, l.qty_lots - have, out);
                    }
                }
            }
        }
    }

    /// Lots resting or pending at one price of the current quote.
    fn current_level_qty(&self, side: Side, px: Px) -> Qty {
        let live: Qty = self
            .live
            .values()
            .filter(|q| {
                q.side == side && q.px == px && !q.cancel_sent && q.generation == self.generation
            })
            .map(|q| q.qty)
            .sum();
        let pending: Qty = self
            .pending
            .iter()
            .filter(|q| q.side == side && q.px == px && q.generation == self.generation)
            .map(|q| q.qty)
            .sum();
        live + pending
    }
}

// ---------------------------------------------------------------------------------------------
// Sniper
// ---------------------------------------------------------------------------------------------

/// On each price it sees, finds resting quotes stale by at least θ (in its own, late view of the
/// book) and sends an IOC to take them, limit = seen price − θ for buys (+ θ for sells). Under
/// PGA it bids α × the expected prize.
#[derive(Debug)]
pub struct Sniper {
    cfg: SniperCfg,
    pga: bool,
    bids: BTreeMap<Px, Qty>,
    asks: BTreeMap<Px, Qty>,
}

impl Sniper {
    pub fn new(cfg: &SniperCfg, pga: bool) -> Self {
        Self {
            cfg: cfg.clone(),
            pga,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    fn fee(&self, prize_tick_lots: i128) -> u64 {
        if !self.pga {
            return 0;
        }
        (prize_tick_lots * CASH_PER_TICK_LOT * self.cfg.alpha_bps as i128 / 10_000) as u64
    }

    pub fn observe(&mut self, obs: &Obs) -> Vec<MsgKind> {
        let mut out = Vec::new();
        match obs {
            Obs::Market(events) => {
                for ev in events.iter() {
                    if let EventKind::BookChange { side, px, qty } = ev.kind {
                        let book = match side {
                            Side::Buy => &mut self.bids,
                            Side::Sell => &mut self.asks,
                        };
                        if qty == 0 {
                            book.remove(&px);
                        } else {
                            book.insert(px, qty);
                        }
                    }
                }
            }
            Obs::Price(v) => {
                let theta = self.cfg.theta_ticks;
                // Asks at or below v - θ: buy them.
                let limit = v - theta;
                let (qty, prize) = self
                    .asks
                    .range(..=limit)
                    .fold((0, 0i128), |(q, p), (&px, &lq)| {
                        (q + lq, p + (v - px) as i128 * lq as i128)
                    });
                if qty > 0 && limit > 0 {
                    out.push(new_order(Side::Buy, limit, qty, Tif::Ioc, self.fee(prize)));
                }
                // Bids at or above v + θ: sell into them.
                let limit = v + theta;
                let (qty, prize) = self
                    .bids
                    .range(limit..)
                    .fold((0, 0i128), |(q, p), (&px, &lq)| {
                        (q + lq, p + (px - v) as i128 * lq as i128)
                    });
                if qty > 0 {
                    out.push(new_order(Side::Sell, limit, qty, Tif::Ioc, self.fee(prize)));
                }
            }
            Obs::Closed => {}
        }
        out
    }
}

// ---------------------------------------------------------------------------------------------
// Regular trader
// ---------------------------------------------------------------------------------------------

/// Arrives at random and sends a marketable IOC: limit = best opposite price ± offset.
/// Doesn't watch the Nasdaq price.
#[derive(Debug)]
pub struct Regular {
    cfg: RegularCfg,
}

impl Regular {
    pub fn new(cfg: &RegularCfg) -> Self {
        Self { cfg: cfg.clone() }
    }

    pub fn rate_per_s(&self, weekend: bool) -> f64 {
        if weekend {
            self.cfg.weekend_rate_per_s
        } else {
            self.cfg.weekday_rate_per_s
        }
    }

    /// Lognormal size with the configured median, rounded, clamped to [1, cap].
    fn size(&self, rng: &mut Rng) -> Qty {
        let z = rng.normal();
        let x = libm::exp(libm::log(self.cfg.size_median_lots) + self.cfg.size_sigma * z);
        let lots = libm::round(x);
        if lots >= self.cfg.size_cap_lots as f64 {
            self.cfg.size_cap_lots
        } else if lots < 1.0 {
            1
        } else {
            lots as Qty
        }
    }

    /// Decide on arrival. Always makes the same draws, so the stream stays aligned whether or
    /// not an order is sent. Returns `None` if the opposite side of the book is empty.
    pub fn decide(
        &self,
        rng: &mut Rng,
        best_bid: Option<Px>,
        best_ask: Option<Px>,
    ) -> Option<MsgKind> {
        let buy = rng.coin();
        let qty = self.size(rng);
        let off = self.cfg.limit_offset_ticks;
        let (side, px) = if buy {
            (Side::Buy, best_ask? + off)
        } else {
            (Side::Sell, best_bid? - off)
        };
        (px > 0).then(|| new_order(side, px, qty, Tif::Ioc, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(kind: EventKind) -> Event {
        Event { seq: 1, kind }
    }

    #[test]
    fn lp_quotes_spec_ladder_and_requotes_on_move() {
        let mut lp = Lp::new(1, &LpCfg::default(), false);
        let out = lp.observe(&Obs::Price(10_000));
        assert_eq!(
            out,
            vec![
                new_order(Side::Buy, 9_999, 10, Tif::Gtc, 0),
                new_order(Side::Buy, 9_998, 5, Tif::Gtc, 0),
                new_order(Side::Sell, 10_001, 10, Tif::Gtc, 0),
                new_order(Side::Sell, 10_002, 5, Tif::Gtc, 0),
            ]
        );
        // Acks arrive: ids 1..=4.
        let acks: Vec<Event> = out
            .iter()
            .enumerate()
            .map(|(i, m)| match *m {
                MsgKind::New {
                    side,
                    px_ticks,
                    qty_lots,
                    tif,
                    ..
                } => ev(EventKind::Accepted {
                    order_id: i as u64 + 1,
                    agent: 1,
                    side,
                    px: px_ticks,
                    qty: qty_lots,
                    tif,
                }),
                _ => unreachable!(),
            })
            .collect();
        assert!(lp.observe(&Obs::Market(acks.into())).is_empty());
        // Same price: nothing. One tick: cancel all four and requote.
        assert!(lp.observe(&Obs::Price(10_000)).is_empty());
        let out = lp.observe(&Obs::Price(10_001));
        assert_eq!(
            &out[..4],
            &(1..=4)
                .map(|id| MsgKind::Cancel {
                    order_id: id,
                    priority_fee: 0
                })
                .collect::<Vec<_>>()[..]
        );
        assert_eq!(out.len(), 8);
    }

    #[test]
    fn lp_bids_beta_times_exposure_on_cancels_under_pga() {
        let cfg = LpCfg {
            beta_bps: 5_000,
            ..LpCfg::default()
        };
        for pga in [false, true] {
            let mut lp = Lp::new(1, &cfg, pga);
            let out = lp.observe(&Obs::Price(10_000));
            let acks: Vec<Event> = out
                .iter()
                .enumerate()
                .map(|(i, m)| match *m {
                    MsgKind::New {
                        side,
                        px_ticks,
                        qty_lots,
                        tif,
                        ..
                    } => ev(EventKind::Accepted {
                        order_id: i as u64 + 1,
                        agent: 1,
                        side,
                        px: px_ticks,
                        qty: qty_lots,
                        tif,
                    }),
                    _ => unreachable!(),
                })
                .collect();
            lp.observe(&Obs::Market(acks.into()));
            // Price jumps to 100.05: the asks at 100.01 (10 lots) and 100.02 (5 lots) are stale
            // by 4 and 3 ticks; the bids are not. Exposure 40 and 15 tick·lots; β = 0.5.
            let fees: Vec<u64> = lp
                .observe(&Obs::Price(10_005))
                .iter()
                .filter_map(|m| match *m {
                    MsgKind::Cancel { priority_fee, .. } => Some(priority_fee),
                    _ => None,
                })
                .collect();
            let want = if pga {
                vec![0, 0, 200_000, 75_000]
            } else {
                vec![0; 4]
            };
            assert_eq!(fees, want, "pga={pga}");
        }
    }

    #[test]
    fn lp_widens_on_close_and_cancels_late_acks() {
        let mut lp = Lp::new(1, &LpCfg::default(), false);
        lp.observe(&Obs::Price(10_000));
        // Close before any ack: the four pending quotes become obsolete; widened quotes posted.
        let out = lp.observe(&Obs::Closed);
        assert_eq!(out[0], new_order(Side::Buy, 9_997, 10, Tif::Gtc, 0));
        assert_eq!(out[3], new_order(Side::Sell, 10_006, 5, Tif::Gtc, 0));
        // The first ack arrives (old quote): the LP cancels it.
        let ack = ev(EventKind::Accepted {
            order_id: 7,
            agent: 1,
            side: Side::Buy,
            px: 9_999,
            qty: 10,
            tif: Tif::Gtc,
        });
        assert_eq!(
            lp.observe(&Obs::Market(vec![ack].into())),
            vec![MsgKind::Cancel {
                order_id: 7,
                priority_fee: 0
            }]
        );
    }

    #[test]
    fn lp_restores_a_capped_side_when_inventory_allows() {
        let cfg = LpCfg {
            inventory_cap_lots: 20,
            ..LpCfg::default()
        };
        let mut lp = Lp::new(1, &cfg, false);
        lp.inventory = 10; // 10 + 15 > 20: no bids at first
        let out = lp.observe(&Obs::Price(10_000));
        assert_eq!(out.len(), 2);
        // A regular trader buys 5 from an LP ask the LP knows about: inventory 5, so a full bid
        // ladder (15) fits again and comes back around the same center.
        let ack = ev(EventKind::Accepted {
            order_id: 1,
            agent: 1,
            side: Side::Sell,
            px: 10_001,
            qty: 10,
            tif: Tif::Gtc,
        });
        lp.observe(&Obs::Market(vec![ack].into()));
        let fill = ev(EventKind::Fill {
            maker_order: 1,
            maker_agent: 1,
            taker_order: 9,
            taker_agent: 7,
            taker_side: Side::Buy,
            px: 10_001,
            qty: 5,
            maker_remaining: 5,
        });
        let out = lp.observe(&Obs::Market(vec![fill].into()));
        assert_eq!(
            out,
            vec![
                new_order(Side::Sell, 10_001, 5, Tif::Gtc, 0), // the refill
                new_order(Side::Buy, 9_999, 10, Tif::Gtc, 0),
                new_order(Side::Buy, 9_998, 5, Tif::Gtc, 0),
            ]
        );
    }

    #[test]
    fn lp_respects_inventory_cap() {
        let cfg = LpCfg {
            inventory_cap_lots: 20,
            ..LpCfg::default()
        };
        let mut lp = Lp::new(1, &cfg, false);
        lp.inventory = 10; // 10 + 15 > 20: no bids
        let out = lp.observe(&Obs::Price(10_000));
        assert!(out.iter().all(|m| matches!(
            m,
            MsgKind::New {
                side: Side::Sell,
                ..
            }
        )));
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn sniper_takes_stale_asks_with_limit_seen_minus_theta() {
        let mut s = Sniper::new(&SniperCfg::default(), true);
        let book: Vec<Event> = [
            (Side::Sell, 10_001, 10),
            (Side::Sell, 10_002, 5),
            (Side::Buy, 9_999, 10),
        ]
        .into_iter()
        .map(|(side, px, qty)| ev(EventKind::BookChange { side, px, qty }))
        .collect();
        s.observe(&Obs::Market(book.into()));
        // Price 100.02: only the ask at 100.01 is stale by >= 1.
        let out = s.observe(&Obs::Price(10_002));
        // Prize 1 tick × 10 lots = 10 tick·lots; α = 0.5 → 5 tick·lots = 50_000 cash units.
        assert_eq!(
            out,
            vec![new_order(Side::Buy, 10_001, 10, Tif::Ioc, 50_000)]
        );
        // Price 99.97: the bid at 99.99 is stale by 2.
        let out = s.observe(&Obs::Price(9_997));
        assert_eq!(
            out,
            vec![new_order(Side::Sell, 9_998, 10, Tif::Ioc, 100_000)]
        );
    }

    #[test]
    fn regular_trader_crosses_by_offset() {
        let r = Regular::new(&RegularCfg::default());
        let mut rng = Rng::new(5, 9, crate::rng::Purpose::Action);
        let mut sides = [0, 0];
        for _ in 0..1_000 {
            match r.decide(&mut rng, Some(9_999), Some(10_001)) {
                Some(MsgKind::New {
                    side: Side::Buy,
                    px_ticks,
                    qty_lots,
                    ..
                }) => {
                    assert_eq!(px_ticks, 10_006);
                    assert!((1..=50).contains(&qty_lots));
                    sides[0] += 1;
                }
                Some(MsgKind::New {
                    side: Side::Sell,
                    px_ticks,
                    ..
                }) => {
                    assert_eq!(px_ticks, 9_994);
                    sides[1] += 1;
                }
                other => panic!("{other:?}"),
            }
        }
        assert!(sides[0] > 400 && sides[1] > 400);
        assert!(r.decide(&mut rng, None, None).is_none());
    }
}
