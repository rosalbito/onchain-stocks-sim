//! Race counting (spec section 10).
//!
//! A race starts when a price move (a logged `Tick`) makes a resting LP quote stale by at least
//! θ, and ends when that quote is filled by a sniper (sniper wins) or cancelled (LP wins).
//! Built only from the input log and the engine's events, so a replay gives the same races.

use std::collections::BTreeMap;

use engine::{AgentId, CancelReason, Event, EventKind, LogRecord, OrderId, Px, Qty, Side};

use crate::AccountClass;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RaceOutcome {
    /// The LP removed the quote first (its own cancel, or its own order self-trade-cancelling it).
    LpWon,
    /// A sniper's order filled the quote first.
    SniperWon,
    /// Someone else (a regular trader) filled it first. Counted as a race but won by neither.
    OtherFilled,
    /// Still open when the run ended. Excluded from the win rate.
    Unresolved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Race {
    pub order_id: OrderId,
    /// `seq` of the `Tick` that started it.
    pub start_seq: u64,
    pub start_t_ns: u64,
    pub end_t_ns: Option<u64>,
    /// Staleness in ticks when it started.
    pub staleness: i64,
    pub outcome: RaceOutcome,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RaceStats {
    pub lp_won: u64,
    pub sniper_won: u64,
    pub other_filled: u64,
    pub unresolved: u64,
    /// Number of distinct ticks that started at least one race.
    pub episodes: u64,
}

impl RaceStats {
    /// Races with an outcome (all races except unresolved ones).
    pub fn resolved(&self) -> u64 {
        self.lp_won + self.sniper_won + self.other_filled
    }

    /// LP race win rate as (numerator, denominator): races the LP won ÷ resolved races.
    pub fn lp_win_rate(&self) -> (u64, u64) {
        (self.lp_won, self.resolved())
    }
}

#[derive(Debug, Clone, Copy)]
struct Quote {
    side: Side,
    px: Px,
    qty: Qty,
}

#[derive(Debug)]
pub struct RaceTracker {
    theta: i64,
    classes: BTreeMap<AgentId, AccountClass>,
    /// Live LP quotes (GTC orders currently resting), by id.
    quotes: BTreeMap<OrderId, Quote>,
    /// Open race per quote, as an index into `races`.
    open: BTreeMap<OrderId, usize>,
    races: Vec<Race>,
    last_episode_seq: Option<u64>,
    episodes: u64,
}

impl RaceTracker {
    pub fn new(theta_ticks: i64, classes: BTreeMap<AgentId, AccountClass>) -> Self {
        Self {
            theta: theta_ticks,
            classes,
            quotes: BTreeMap::new(),
            open: BTreeMap::new(),
            races: Vec::new(),
            last_episode_seq: None,
            episodes: 0,
        }
    }

    fn class(&self, agent: AgentId) -> Option<AccountClass> {
        self.classes.get(&agent).copied()
    }

    fn close(&mut self, order_id: OrderId, t_ns: u64, outcome: RaceOutcome) {
        if let Some(i) = self.open.remove(&order_id) {
            self.races[i].end_t_ns = Some(t_ns);
            self.races[i].outcome = outcome;
        }
    }

    /// Feed one sequenced record and the events the engine produced for it.
    pub fn on_record(&mut self, rec: &LogRecord, events: &[Event]) {
        for ev in events {
            match ev.kind {
                EventKind::Accepted {
                    order_id,
                    agent,
                    side,
                    px,
                    qty,
                    tif: engine::Tif::Gtc,
                } if self.class(agent) == Some(AccountClass::Lp) => {
                    // Provisionally live; taker fills below reduce it, and if nothing is left
                    // it is dropped. GTC remainders rest.
                    self.quotes.insert(order_id, Quote { side, px, qty });
                }
                EventKind::Fill {
                    maker_order,
                    taker_order,
                    taker_agent,
                    qty,
                    maker_remaining,
                    ..
                } => {
                    if self.quotes.contains_key(&maker_order) {
                        let outcome = match self.class(taker_agent) {
                            Some(AccountClass::Sniper) => RaceOutcome::SniperWon,
                            _ => RaceOutcome::OtherFilled,
                        };
                        self.close(maker_order, rec.t_ns, outcome);
                        if maker_remaining == 0 {
                            self.quotes.remove(&maker_order);
                        } else if let Some(q) = self.quotes.get_mut(&maker_order) {
                            q.qty = maker_remaining;
                        }
                    }
                    if let Some(q) = self.quotes.get_mut(&taker_order) {
                        q.qty -= qty;
                        if q.qty == 0 {
                            self.quotes.remove(&taker_order);
                        }
                    }
                }
                EventKind::Cancelled {
                    order_id, reason, ..
                } => {
                    // Quotes are GTC, so the reason is a user cancel or a self-trade cancel:
                    // either way the LP's own message removed the quote.
                    if self.quotes.remove(&order_id).is_some() {
                        debug_assert_ne!(reason, CancelReason::IocRemainder);
                        self.close(order_id, rec.t_ns, RaceOutcome::LpWon);
                    }
                }
                EventKind::Tick { px } => self.on_tick(rec, px),
                _ => {}
            }
        }
    }

    fn on_tick(&mut self, rec: &LogRecord, v: Px) {
        let mut started = false;
        for (&id, q) in &self.quotes {
            if self.open.contains_key(&id) {
                continue;
            }
            let staleness = match q.side {
                Side::Sell => v - q.px, // ask below the true price
                Side::Buy => q.px - v,  // bid above the true price
            };
            if staleness >= self.theta {
                self.open.insert(id, self.races.len());
                self.races.push(Race {
                    order_id: id,
                    start_seq: rec.seq,
                    start_t_ns: rec.t_ns,
                    end_t_ns: None,
                    staleness,
                    outcome: RaceOutcome::Unresolved,
                });
                started = true;
            }
        }
        if started && self.last_episode_seq != Some(rec.seq) {
            self.last_episode_seq = Some(rec.seq);
            self.episodes += 1;
        }
    }

    pub fn races(&self) -> &[Race] {
        &self.races
    }

    pub fn stats(&self) -> RaceStats {
        let mut s = RaceStats {
            episodes: self.episodes,
            ..Default::default()
        };
        for r in &self.races {
            match r.outcome {
                RaceOutcome::LpWon => s.lp_won += 1,
                RaceOutcome::SniperWon => s.sniper_won += 1,
                RaceOutcome::OtherFilled => s.other_filled += 1,
                RaceOutcome::Unresolved => s.unresolved += 1,
            }
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine::{Engine, MsgKind, Tif};

    const LP: AgentId = 1;
    const SNIPER: AgentId = 2;

    fn classes() -> BTreeMap<AgentId, AccountClass> {
        [(LP, AccountClass::Lp), (SNIPER, AccountClass::Sniper)].into()
    }

    fn feed(log: &[(AgentId, MsgKind)]) -> RaceTracker {
        let mut e = Engine::new();
        let mut rt = RaceTracker::new(1, classes());
        for (i, &(agent, kind)) in log.iter().enumerate() {
            let rec = LogRecord {
                seq: i as u64 + 1,
                t_ns: (i as u64 + 1) * 1_000_000,
                block: 0,
                agent,
                kind,
            };
            let ev = e.on_message(&rec);
            rt.on_record(&rec, &ev);
        }
        rt
    }

    fn ask(px: Px) -> MsgKind {
        MsgKind::New {
            side: Side::Sell,
            px_ticks: px,
            qty_lots: 10,
            tif: Tif::Gtc,
            priority_fee: 0,
        }
    }

    fn snipe(px: Px) -> MsgKind {
        MsgKind::New {
            side: Side::Buy,
            px_ticks: px,
            qty_lots: 10,
            tif: Tif::Ioc,
            priority_fee: 0,
        }
    }

    #[test]
    fn one_tick_move_is_not_a_race() {
        // Ask at 101, price moves to 101: stale by 0 < θ.
        let rt = feed(&[(LP, ask(101)), (0, MsgKind::Tick { px_ticks: 101 })]);
        assert_eq!(rt.stats(), RaceStats::default());
    }

    #[test]
    fn sniper_wins_when_fill_comes_first() {
        let rt = feed(&[
            (LP, ask(101)),
            (0, MsgKind::Tick { px_ticks: 102 }),
            (SNIPER, snipe(101)),
            (LP, MsgKind::Cancel { order_id: 1 }),
        ]);
        let s = rt.stats();
        assert_eq!((s.sniper_won, s.lp_won, s.episodes), (1, 0, 1));
        assert_eq!(rt.races()[0].staleness, 1);
        assert_eq!(rt.races()[0].end_t_ns, Some(3_000_000));
    }

    #[test]
    fn lp_wins_when_cancel_comes_first() {
        let rt = feed(&[
            (LP, ask(101)),
            (0, MsgKind::Tick { px_ticks: 105 }),
            (LP, MsgKind::Cancel { order_id: 1 }),
            (SNIPER, snipe(104)),
        ]);
        let s = rt.stats();
        assert_eq!((s.sniper_won, s.lp_won), (0, 1));
        assert_eq!(s.lp_win_rate(), (1, 1));
    }

    #[test]
    fn open_race_at_end_is_unresolved() {
        let rt = feed(&[(LP, ask(101)), (0, MsgKind::Tick { px_ticks: 105 })]);
        assert_eq!(rt.stats().unresolved, 1);
        assert_eq!(rt.stats().resolved(), 0);
    }
}
