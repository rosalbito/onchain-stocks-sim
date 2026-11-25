//! Stress mode: synthetic order flow at a target rate through the real path (a producer thread →
//! input ring → engine thread → log writer → disk, plus the standby and the UI).
//!
//! The flow is a pure function of the seed and of where in the stream the rate changed: every
//! message's content and its `t_ns` (the producer's schedule, not the wall clock) come from the
//! generator, and the producer itself writes each rate change into the stream as a `Config`
//! record. So the log can be checked two ways: replayed through a fresh engine (same fills), and
//! regenerated from its header and its `Config` records (same records).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use engine::{AgentId, MsgKind, OrderId, Side, Tif};
use rand_chacha::ChaCha8Rng;
use rand_core::{RngCore, SeedableRng};
use sim::Control;

use crate::sysinfo::{Telemetry, T_PRODUCER, W_INPUT};

/// `Config` param for a stress rate change (msgs/s, 0 = as fast as possible). Params 1 to 5 are
/// the simulation's (`sim::Control`).
pub const PARAM_STRESS_RATE: u32 = 6;

/// Makers post and cancel limit orders; takers send IOCs.
const MAKERS: u32 = 32;
const TAKERS: u32 = 32;
/// Above this many live maker orders, the generator cancels instead of posting.
const MAX_RESTING: usize = 50_000;
const START_MID: i64 = 10_000;

/// One message from the producer to the engine thread.
#[derive(Debug, Clone, Copy)]
pub struct StressMsg {
    /// Wall time the producer handed it over, ns since the run's start (for end-to-end latency).
    pub sent_ns: u64,
    /// The producer's schedule: what the log records as `t_ns`.
    pub t_ns: u64,
    pub agent: AgentId,
    pub kind: MsgKind,
}

/// The seeded flow: limit orders around a drifting mid (45%), cancels of live orders (40%) and
/// marketable IOCs (15%). It predicts order ids, since an id is the `seq` of its `New` and the
/// engine numbers the stream in order.
pub struct Generator {
    rng: ChaCha8Rng,
    /// The `seq` the next message will get.
    next_seq: u64,
    mid: i64,
    resting: Vec<(OrderId, AgentId)>,
    t_ns: u64,
    period_ns: u64,
}

fn period(rate: u64) -> u64 {
    if rate == 0 {
        1
    } else {
        (1_000_000_000 / rate).max(1)
    }
}

impl Generator {
    pub fn new(seed: u64, rate: u64) -> Self {
        Self {
            rng: ChaCha8Rng::seed_from_u64(seed),
            next_seq: 1,
            mid: START_MID,
            resting: Vec::new(),
            t_ns: 0,
            period_ns: period(rate),
        }
    }

    fn pick(&mut self, n: u64) -> u64 {
        self.rng.next_u64() % n
    }

    /// The next order-flow message.
    pub fn next(&mut self) -> (u64, AgentId, MsgKind) {
        self.t_ns += self.period_ns;
        self.next_seq += 1;
        let seq = self.next_seq - 1;
        if seq.is_multiple_of(64) {
            self.mid = (self.mid + if self.pick(2) == 0 { 1 } else { -1 }).max(100);
        }
        let roll = self.pick(100);
        let side = if self.pick(2) == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let (agent, kind) = if ((45..85).contains(&roll) && !self.resting.is_empty())
            || self.resting.len() >= MAX_RESTING
        {
            let i = self.pick(self.resting.len() as u64) as usize;
            let (order_id, agent) = self.resting.swap_remove(i);
            (
                agent,
                MsgKind::Cancel {
                    order_id,
                    priority_fee: 0,
                },
            )
        } else if roll < 85 {
            let agent = 1 + self.pick(MAKERS as u64) as u32;
            let off = 1 + self.pick(8) as i64;
            let px = match side {
                Side::Buy => self.mid - off,
                Side::Sell => self.mid + off,
            };
            self.resting.push((seq, agent));
            (
                agent,
                MsgKind::New {
                    side,
                    px_ticks: px,
                    qty_lots: 1 + self.pick(20),
                    tif: Tif::Gtc,
                    priority_fee: 0,
                },
            )
        } else {
            let agent = 1 + MAKERS + self.pick(TAKERS as u64) as u32;
            let px = match side {
                Side::Buy => self.mid + 3,
                Side::Sell => self.mid - 3,
            };
            (
                agent,
                MsgKind::New {
                    side,
                    px_ticks: px,
                    qty_lots: 1 + self.pick(30),
                    tif: Tif::Ioc,
                    priority_fee: 0,
                },
            )
        };
        (self.t_ns, agent, kind)
    }

    /// A `Config` record at this point of the stream (it takes a `seq` too). A rate change
    /// also changes the schedule from here on.
    pub fn config(&mut self, param: u32, value: u64) -> (u64, AgentId, MsgKind) {
        self.next_seq += 1;
        if param == PARAM_STRESS_RATE {
            self.period_ns = period(value);
        }
        (self.t_ns, 0, MsgKind::Config { param, value })
    }

    #[cfg(test)]
    pub fn resting(&self) -> usize {
        self.resting.len()
    }
}

/// Controls the engine thread holds over a running producer.
pub struct ProducerHandle {
    /// Target rate in msgs/s (0 = as fast as possible). The producer notices a change and logs it.
    pub rate: Arc<AtomicU64>,
    pub stop: Arc<AtomicBool>,
    pub thread: thread::JoinHandle<()>,
}

/// Spawn the producer. It ends after writing a `Stop` record once `stop` is set.
pub fn spawn_producer(
    mut tx: rtrb::Producer<StressMsg>,
    seed: u64,
    rate: u64,
    start: Instant,
    tel: Arc<Telemetry>,
) -> ProducerHandle {
    let rate_cell = Arc::new(AtomicU64::new(rate));
    let stop = Arc::new(AtomicBool::new(false));
    let (rc, st) = (rate_cell.clone(), stop.clone());
    let thread = thread::Builder::new()
        .name("producer".into())
        .spawn(move || {
            crate::qos::prefer_performance_cores();
            let mut gen = Generator::new(seed, rate);
            let mut cur_rate = rate;
            // Messages owed at the current rate since `since`.
            let mut since = Instant::now();
            let mut sent_at_rate = 0u64;
            let mut pending: Option<StressMsg> = None;
            let mut loops = 0u64;
            let stamp = |at: Instant| at.duration_since(start).as_nanos() as u64;
            loop {
                loops += 1;
                if loops.is_multiple_of(256) {
                    tel.record_thread_cpu(T_PRODUCER);
                }
                let stopping = st.load(Relaxed);
                let want = rc.load(Relaxed);
                if pending.is_none() && (want != cur_rate || stopping) {
                    let (t_ns, agent, kind) = if stopping {
                        let (p, v) = Control::Stop.encode();
                        gen.config(p, v)
                    } else {
                        gen.config(PARAM_STRESS_RATE, want)
                    };
                    pending = Some(StressMsg {
                        sent_ns: stamp(Instant::now()),
                        t_ns,
                        agent,
                        kind,
                    });
                    cur_rate = want;
                    since = Instant::now();
                    sent_at_rate = 0;
                }
                // How many are due now (flat out: a chunk at a time).
                let now = Instant::now();
                let due = if cur_rate == 0 {
                    sent_at_rate + 4_096
                } else {
                    (now.duration_since(since).as_nanos() as u64 * cur_rate / 1_000_000_000)
                        .min(sent_at_rate + 4_096)
                };
                let sent_ns = stamp(now);
                let mut full = false;
                while pending.is_some() || sent_at_rate < due {
                    let m = match pending.take() {
                        Some(m) => m,
                        None => {
                            let (t_ns, agent, kind) = gen.next();
                            sent_at_rate += 1;
                            StressMsg {
                                sent_ns,
                                t_ns,
                                agent,
                                kind,
                            }
                        }
                    };
                    let is_stop = matches!(m.kind, MsgKind::Config { param, .. } if param == Control::Stop.encode().0);
                    match tx.push(m) {
                        Ok(()) => {
                            tel.produced.fetch_add(1, Relaxed);
                            if is_stop {
                                tel.record_thread_cpu(T_PRODUCER);
                                return;
                            }
                        }
                        Err(rtrb::PushError::Full(back)) => {
                            pending = Some(back);
                            full = true;
                            break;
                        }
                    }
                }
                if full {
                    // Backpressure: the engine is behind. Wait for room.
                    tel.waits[W_INPUT].fetch_add(1, Relaxed);
                    std::hint::spin_loop();
                    thread::yield_now();
                } else if cur_rate != 0 {
                    // Ahead of schedule: sleep a little (the rate is kept on average).
                    thread::sleep(Duration::from_micros(50));
                }
            }
        })
        .expect("spawn producer");
    ProducerHandle {
        rate: rate_cell,
        stop,
        thread,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_flow_is_a_function_of_the_seed_and_the_config_positions() {
        let run = |seed| {
            let mut g = Generator::new(seed, 10_000);
            let mut out = Vec::new();
            for i in 0..20_000 {
                out.push(if i == 7_000 {
                    g.config(PARAM_STRESS_RATE, 0)
                } else {
                    g.next()
                });
            }
            (out, g.resting())
        };
        let (a, resting) = run(5);
        assert_eq!(a, run(5).0);
        assert_ne!(a, run(6).0);
        // A real mix, and the book stays bounded.
        let cancels = a
            .iter()
            .filter(|m| matches!(m.2, MsgKind::Cancel { .. }))
            .count();
        let iocs = a
            .iter()
            .filter(|m| matches!(m.2, MsgKind::New { tif: Tif::Ioc, .. }))
            .count();
        assert!((7_000..9_000).contains(&cancels), "{cancels}");
        assert!((2_500..3_500).contains(&iocs), "{iocs}");
        assert!(resting < 2_000, "{resting}");
        // The schedule: 100 µs apart at 10k/s, 1 ns apart flat out after the change.
        assert_eq!(a[1].0 - a[0].0, 100_000);
        assert_eq!(a[7_002].0 - a[7_001].0, 1);
    }
}
