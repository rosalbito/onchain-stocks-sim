//! What the dashboard's charts need, built on the engine thread into fixed-size frame parts:
//! candles of the trades (both modes), the trade tape, and the race zoom (market mode): the
//! last race replayed on a millisecond axis, so the audience sees the latency race itself.

use engine::{MsgKind, Px, Qty};
use metrics::{AccountClass, RaceOutcome};
use sim::{Live, Trade};

use crate::engine_thread::Outcome;

/// Candles on the price chart.
pub const CANDLES: usize = 60;
/// Rows of the trade tape.
pub const TAPE: usize = 16;
/// LP quotes in the race zoom.
pub const ZOOM: usize = 48;

/// OHLC and volume of one time bucket. `vol == 0` means no trade in it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Candle {
    pub o: Px,
    pub h: Px,
    pub l: Px,
    pub c: Px,
    pub vol: Qty,
}

impl Candle {
    pub fn add(&mut self, px: Px, qty: Qty) {
        if self.vol == 0 {
            *self = Candle {
                o: px,
                h: px,
                l: px,
                c: px,
                vol: qty,
            };
        } else {
            self.h = self.h.max(px);
            self.l = self.l.min(px);
            self.c = px;
            self.vol += qty;
        }
    }
}

/// `n` candles of `width` ns from `t0`. Market mode: virtual time; stress: wall time since start.
#[derive(Debug, Clone, Copy)]
pub struct Candles {
    pub t0: u64,
    pub width: u64,
    pub n: u16,
    pub c: [Candle; CANDLES],
}

impl Default for Candles {
    fn default() -> Self {
        Self {
            t0: 0,
            width: 1,
            n: 0,
            c: [Candle::default(); CANDLES],
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TapeRow {
    pub t_ns: u64,
    pub px: Px,
    pub qty: Qty,
    pub buy: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Tape {
    pub rows: [TapeRow; TAPE],
    pub n: u8,
}

/// The chart window for market mode: about a minute of wall time at the playback speed (at
/// least 20 s of virtual time, at most the run), cut into `CANDLES` buckets and snapped to the
/// bucket width, so the chart moves one candle at a time instead of shimmering every frame.
pub fn market_window(clock: u64, end: u64, view_speed_milli: u64) -> (u64, u64) {
    let w = ((view_speed_milli as u128 * 60_000_000) as u64).clamp(20_000_000_000, end.max(1));
    let width = (w / CANDLES as u64).max(1);
    let w = width * CANDLES as u64;
    let t1 = clock.div_ceil(width).max(1) * width;
    let t1 = t1.max(w);
    (t1 - w, width)
}

/// Candles of `trades` (sorted by time) over `CANDLES` buckets of `width` from `t0`.
pub fn candles(trades: &[Trade], t0: u64, width: u64) -> Candles {
    let mut out = Candles {
        t0,
        width,
        n: CANDLES as u16,
        ..Candles::default()
    };
    let t1 = t0 + width * CANDLES as u64;
    let i = trades.partition_point(|t| t.t_ns < t0);
    for t in &trades[i..] {
        if t.t_ns >= t1 {
            break;
        }
        out.c[((t.t_ns - t0) / width) as usize].add(t.px, t.qty);
    }
    out
}

pub fn tape(trades: &[Trade]) -> Tape {
    let mut out = Tape::default();
    for (dst, t) in out.rows.iter_mut().zip(trades.iter().rev()) {
        *dst = TapeRow {
            t_ns: t.t_ns,
            px: t.px,
            qty: t.qty,
            buy: t.taker_buy,
        };
        out.n += 1;
    }
    out
}

/// One LP quote in the race zoom. Times are ns after the Nasdaq move; `i64::MIN` = none.
#[derive(Debug, Clone, Copy)]
pub struct ZoomRow {
    pub agent: u32,
    /// When the LP decided to cancel (it saw the move).
    pub see: i64,
    /// When its cancel reached the sequencer.
    pub arrive: i64,
    /// When the race ended (cancel executed or quote filled).
    pub end: i64,
    pub qty: Qty,
    pub outcome: Outcome,
}

impl Default for ZoomRow {
    fn default() -> Self {
        Self {
            agent: 0,
            see: i64::MIN,
            arrive: i64::MIN,
            end: i64::MIN,
            qty: 0,
            outcome: Outcome::Open,
        }
    }
}

/// The most recent Nasdaq move that started races, replayed on a nanosecond axis.
#[derive(Debug, Clone, Copy)]
pub struct Zoom {
    pub valid: bool,
    /// Absolute virtual time of the move.
    pub start_ns: u64,
    pub move_ticks: i64,
    /// The first sniper to react: decided, reached the sequencer. `i64::MIN` = no sniper.
    pub sniper_see: i64,
    pub sniper_arrive: i64,
    /// Races in the episode (may exceed `ZOOM`).
    pub races: u32,
    pub rows: [ZoomRow; ZOOM],
    pub n: u8,
}

impl Default for Zoom {
    fn default() -> Self {
        Self {
            valid: false,
            start_ns: 0,
            move_ticks: 0,
            sniper_see: i64::MIN,
            sniper_arrive: i64::MIN,
            races: 0,
            rows: [ZoomRow::default(); ZOOM],
            n: 0,
        }
    }
}

/// How far after the move to look for the reactions.
const ZOOM_SCAN_NS: u64 = 2_000_000_000;
const ZOOM_SCAN_RECORDS: usize = 50_000;

pub fn zoom(live: &Live) -> Zoom {
    let races = live.races();
    let Some(last) = races
        .iter()
        .rposition(|r| r.outcome != RaceOutcome::Unresolved)
    else {
        return Zoom::default();
    };
    let start = races[last].start_t_ns;
    let first = races[..last]
        .iter()
        .rposition(|r| r.start_t_ns != start)
        .map_or(0, |i| i + 1);
    let end = races[last..]
        .iter()
        .position(|r| r.start_t_ns != start)
        .map_or(races.len(), |i| last + i);
    let group = &races[first..end];

    let recs = live.records();
    let decided = live.decided();
    let i0 = (races[last].start_seq as usize).saturating_sub(1);
    let tick_px = match recs.get(i0).map(|r| r.kind) {
        Some(MsgKind::Tick { px_ticks }) => px_ticks,
        _ => return Zoom::default(),
    };
    let prev_px = recs[..i0]
        .iter()
        .rev()
        .take(ZOOM_SCAN_RECORDS)
        .find_map(|r| match r.kind {
            MsgKind::Tick { px_ticks } => Some(px_ticks),
            _ => None,
        })
        .unwrap_or(tick_px);
    let rel = |t: u64| t as i64 - start as i64;

    let mut z = Zoom {
        valid: true,
        start_ns: start,
        move_ticks: tick_px - prev_px,
        races: group.len() as u32,
        ..Zoom::default()
    };
    // Each quote's cancel, and the first sniper order sent after the move.
    let mut cancels: Vec<(u64, i64, i64)> = Vec::new();
    for (j, r) in recs.iter().enumerate().skip(i0 + 1).take(ZOOM_SCAN_RECORDS) {
        if r.t_ns > start + ZOOM_SCAN_NS {
            break;
        }
        let class = live.class_of(r.agent);
        match r.kind {
            MsgKind::Cancel { order_id, .. } if class == Some(AccountClass::Lp) => {
                cancels.push((order_id, rel(decided[j]), rel(r.t_ns)));
            }
            MsgKind::New { .. }
                if class == Some(AccountClass::Sniper)
                    && decided[j] >= start
                    && z.sniper_arrive == i64::MIN =>
            {
                z.sniper_see = rel(decided[j]);
                z.sniper_arrive = rel(r.t_ns);
            }
            _ => {}
        }
    }
    for (dst, r) in z.rows.iter_mut().zip(group) {
        let c = cancels.iter().find(|c| c.0 == r.order_id);
        let rec = recs.get(r.order_id as usize - 1);
        *dst = ZoomRow {
            agent: rec.map_or(0, |x| x.agent),
            see: c.map_or(i64::MIN, |c| c.1),
            arrive: c.map_or(i64::MIN, |c| c.2),
            end: r.end_t_ns.map_or(i64::MIN, rel),
            qty: rec
                .and_then(|x| match x.kind {
                    MsgKind::New { qty_lots, .. } => Some(qty_lots),
                    _ => None,
                })
                .unwrap_or(0),
            outcome: Outcome::from(r.outcome),
        };
        z.n += 1;
    }
    z
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trade(t_ns: u64, px: Px, qty: Qty) -> Trade {
        Trade {
            t_ns,
            px,
            qty,
            taker_buy: true,
        }
    }

    #[test]
    fn candles_bucket_trades_by_time() {
        let trades = [
            trade(5, 100, 1),
            trade(10, 101, 2),
            trade(12, 99, 1),
            trade(19, 100, 3),
            trade(25, 102, 1),
        ];
        let c = candles(&trades, 10, 10);
        assert_eq!(
            c.c[0],
            Candle {
                o: 101,
                h: 101,
                l: 99,
                c: 100,
                vol: 6
            }
        );
        assert_eq!(c.c[1].c, 102);
        assert_eq!(c.c[2].vol, 0);
    }

    #[test]
    fn window_snaps_to_candle_width() {
        // 1×: 60 s window, 1 s candles; the window ends at the next whole second.
        let (t0, w) = market_window(75_500_000_000, 3_600_000_000_000, 1_000);
        assert_eq!(w, 1_000_000_000);
        assert_eq!(t0, 16_000_000_000);
        // Early in the run it starts at 0.
        assert_eq!(market_window(3_000_000_000, 3_600_000_000_000, 1_000).0, 0);
    }
}
