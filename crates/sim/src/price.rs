//! The Nasdaq price path (spec section 7): a random walk in whole ticks with Poisson jumps,
//! hidden from Friday close to the Monday open.
//!
//! The feed only draws from its own streams, and the order of its draws depends only on its own
//! event times, so the path is identical under every ordering rule (common random numbers).

use crate::config::{PriceCfg, WeekendCfg};
use crate::rng::{Purpose, Rng, FEED_ID};

pub fn secs_to_ns(s: f64) -> u64 {
    (s * 1e9) as u64
}

/// The hidden window, in nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    pub close_ns: u64,
    pub reopen_ns: u64,
}

impl Window {
    pub fn from_cfg(w: &WeekendCfg) -> Self {
        Self {
            close_ns: secs_to_ns(w.close_at_s),
            reopen_ns: secs_to_ns(w.reopen_at_s),
        }
    }

    pub fn hidden(&self, t: u64) -> bool {
        self.close_ns <= t && t < self.reopen_ns
    }
}

/// Next arrival after `from` of a Poisson process whose rate is piecewise constant.
/// `rate_at(t)` returns the rate at `t` and the end of the segment containing `t`.
/// Exact by memorylessness: if a draw crosses a segment end, restart there with the new rate.
pub fn next_arrival(
    rng: &mut Rng,
    from: u64,
    end: u64,
    rate_at: impl Fn(u64) -> (f64, u64),
) -> Option<u64> {
    let mut t = from;
    while t < end {
        let (rate, seg_end) = rate_at(t);
        let seg_end = seg_end.min(end);
        if rate <= 0.0 {
            t = seg_end;
            continue;
        }
        let x = rng.exp_ns(rate);
        match t.checked_add(x) {
            Some(next) if next < seg_end => return Some(next),
            _ => t = seg_end,
        }
    }
    None
}

pub struct PriceFeed {
    cfg: PriceCfg,
    weekend: Option<(Window, f64)>,
    end_ns: u64,
    px: i64,
    diffusion: Rng,
    jumps: Rng,
}

impl PriceFeed {
    pub fn new(cfg: &PriceCfg, weekend: Option<&WeekendCfg>, end_ns: u64, seed: u64) -> Self {
        Self {
            cfg: cfg.clone(),
            weekend: weekend.map(|w| (Window::from_cfg(w), w.step_rate_per_s)),
            end_ns,
            px: cfg.start_px_ticks,
            diffusion: Rng::new(seed, FEED_ID, Purpose::Diffusion),
            jumps: Rng::new(seed, FEED_ID, Purpose::Jumps),
        }
    }

    /// The true (Nasdaq) price, visible or not.
    pub fn px(&self) -> i64 {
        self.px
    }

    pub fn window(&self) -> Option<Window> {
        self.weekend.map(|(w, _)| w)
    }

    pub fn hidden(&self, t: u64) -> bool {
        self.window().is_some_and(|w| w.hidden(t))
    }

    pub fn next_step_time(&mut self, from: u64) -> Option<u64> {
        let weekday = self.cfg.step_rate_per_s;
        let weekend = self.weekend;
        next_arrival(&mut self.diffusion, from, self.end_ns, |t| match weekend {
            Some((w, _)) if t < w.close_ns => (weekday, w.close_ns),
            Some((w, rate)) if t < w.reopen_ns => (rate, w.reopen_ns),
            _ => (weekday, u64::MAX),
        })
    }

    /// Random jumps happen on weekdays only, after `jumps_start_after_s`.
    pub fn next_jump_time(&mut self, from: u64) -> Option<u64> {
        let rate = self.cfg.jump_rate_per_s;
        let start = secs_to_ns(self.cfg.jumps_start_after_s);
        let weekend = self.weekend;
        next_arrival(&mut self.jumps, from, self.end_ns, |t| {
            if t < start {
                return (0.0, start);
            }
            match weekend {
                Some((w, _)) if t < w.close_ns => (rate, w.close_ns),
                Some((w, _)) if t < w.reopen_ns => (0.0, w.reopen_ns),
                _ => (rate, u64::MAX),
            }
        })
    }

    fn set(&mut self, px: i64) -> i64 {
        self.px = px.max(1);
        self.px
    }

    /// One diffusion step of ±1 tick.
    pub fn step(&mut self) -> i64 {
        let d = if self.diffusion.coin() { 1 } else { -1 };
        self.set(self.px + d)
    }

    /// One random jump of `jump_min..=jump_max` ticks, either way.
    pub fn jump(&mut self) -> i64 {
        let size = self
            .jumps
            .range_i64(self.cfg.jump_min_ticks, self.cfg.jump_max_ticks);
        let d = if self.jumps.coin() { size } else { -size };
        self.set(self.px + d)
    }

    pub fn shift(&mut self, ticks: i64) -> i64 {
        self.set(self.px + ticks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_arrivals_in_zero_rate_segments() {
        let mut rng = Rng::new(1, 0, Purpose::Jumps);
        let close = 10_000_000_000;
        let reopen = 20_000_000_000;
        let mut t = 0;
        let mut n = 0;
        while let Some(next) = next_arrival(&mut rng, t, 30_000_000_000, |t| {
            if t < close {
                (5.0, close)
            } else if t < reopen {
                (0.0, reopen)
            } else {
                (5.0, u64::MAX)
            }
        }) {
            assert!(!(close..reopen).contains(&next));
            t = next;
            n += 1;
        }
        // Expect about 5/s for 20 s.
        assert!((70..130).contains(&n), "{n}");
    }

    #[test]
    fn path_is_reproducible() {
        let cfg = PriceCfg::default();
        let run = || {
            let mut f = PriceFeed::new(&cfg, None, 600_000_000_000, 9);
            let mut t = 0;
            let mut out = vec![];
            while let Some(n) = f.next_step_time(t) {
                t = n;
                out.push((t, f.step()));
            }
            out
        };
        let a = run();
        assert_eq!(a, run());
        assert!(a.len() > 500, "about 1 step per second for 600 s");
    }
}
