//! Seeded randomness. Every agent (and the price feed) owns its own `ChaCha8Rng` streams, seeded
//! from the master seed and its id, so adding an agent leaves the others' draws unchanged.
//!
//! Continuous samplers use `libm` (a pure-Rust libm), not `std`'s `ln`/`exp`, which call the
//! platform's math library and are not guaranteed to round identically on every machine. Results
//! are converted to integer nanoseconds or lots immediately.

use rand_chacha::ChaCha8Rng;
use rand_core::{RngCore, SeedableRng};

/// What a stream is used for. Separate streams keep, for example, an agent's latency draws
/// independent of how many decisions it made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum Purpose {
    /// Latency draws (see and send).
    Latency = 0,
    /// Decisions: arrival times, sizes, sides.
    Action = 1,
    /// Price feed diffusion steps.
    Diffusion = 2,
    /// Price feed jumps.
    Jumps = 3,
}

/// The feed's id for seeding. Agents are numbered from 1.
pub const FEED_ID: u32 = 0;

pub struct Rng(ChaCha8Rng);

impl Rng {
    /// Stream `(id, purpose)` of the master seed.
    pub fn new(master_seed: u64, id: u32, purpose: Purpose) -> Self {
        let mut r = ChaCha8Rng::seed_from_u64(master_seed);
        r.set_stream(((id as u64) << 8) | purpose as u64);
        Self(r)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0.next_u64()
    }

    /// Uniform in `0..n` (n > 0), by widening multiply.
    pub fn below(&mut self, n: u64) -> u64 {
        debug_assert!(n > 0);
        ((self.next_u64() as u128 * n as u128) >> 64) as u64
    }

    /// Uniform in `lo..=hi`.
    pub fn range_i64(&mut self, lo: i64, hi: i64) -> i64 {
        lo + self.below((hi - lo) as u64 + 1) as i64
    }

    pub fn coin(&mut self) -> bool {
        self.next_u64() >> 63 == 1
    }

    /// Uniform in [0, 1) with 53 bits.
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }

    /// Exponential waiting time in nanoseconds for a Poisson process with `rate_per_s`.
    pub fn exp_ns(&mut self, rate_per_s: f64) -> u64 {
        let u = self.unit();
        let secs = -libm::log(1.0 - u) / rate_per_s;
        let ns = secs * 1e9;
        if ns >= u64::MAX as f64 {
            u64::MAX
        } else {
            ns as u64
        }
    }

    /// Standard normal, Box-Muller.
    pub fn normal(&mut self) -> f64 {
        let u1 = 1.0 - self.unit(); // (0, 1]
        let u2 = self.unit();
        libm::sqrt(-2.0 * libm::log(u1)) * libm::cos(2.0 * std::f64::consts::PI * u2)
    }

    /// Latency in nanoseconds: uniform in [base - jitter, base + jitter] microseconds.
    pub fn latency_ns(&mut self, base_us: u64, jitter_us: u64) -> u64 {
        let base = base_us * 1_000;
        let j = jitter_us * 1_000;
        base - j + self.below(2 * j + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streams_are_independent_and_reproducible() {
        let a: Vec<u64> = (0..4)
            .map({
                let mut r = Rng::new(7, 1, Purpose::Latency);
                move |_| r.next_u64()
            })
            .collect();
        let a2: Vec<u64> = (0..4)
            .map({
                let mut r = Rng::new(7, 1, Purpose::Latency);
                move |_| r.next_u64()
            })
            .collect();
        let b: Vec<u64> = (0..4)
            .map({
                let mut r = Rng::new(7, 2, Purpose::Latency);
                move |_| r.next_u64()
            })
            .collect();
        assert_eq!(a, a2);
        assert_ne!(a, b);
    }

    #[test]
    fn latency_within_bounds() {
        let mut r = Rng::new(1, 1, Purpose::Latency);
        for _ in 0..10_000 {
            let l = r.latency_ns(20_000, 5_000);
            assert!((15_000_000..=25_000_000).contains(&l));
        }
    }

    #[test]
    fn exponential_mean_is_close() {
        let mut r = Rng::new(3, 1, Purpose::Action);
        let n = 100_000u64;
        let total: u128 = (0..n).map(|_| r.exp_ns(2.0) as u128).sum();
        let mean_ms = total / n as u128 / 1_000_000;
        assert!((490..=510).contains(&mean_ms), "mean {mean_ms} ms");
    }
}
