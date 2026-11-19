//! Load profiles. Recorded ones are input logs from the simulation (the same scenarios as the
//! demo and the grid); the burst is synthetic: a deep book, then one block of thousands of
//! messages, the architecture doc's "Monday burst".

use engine::{Engine, LogRecord, MsgKind, OrderId, Side, Tif};
use rand_chacha::ChaCha8Rng;
use rand_core::{RngCore, SeedableRng};
use sim::sequencer::{sort_block, Pending};
use sim::{OrderingRule, Scenario};

pub struct Profile {
    pub name: &'static str,
    pub what: &'static str,
    pub records: Vec<LogRecord>,
}

fn scenario(name: &str) -> Scenario {
    let path = format!("{}/../../scenarios/{name}.toml", env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
    Scenario::from_toml(&text).expect("scenario parses")
}

fn record(cfg: &Scenario, seed: u64) -> Vec<LogRecord> {
    sim::run(cfg, seed).expect("run balances").records
}

/// Calm weekday: one hour of the default flow, FCFS, B = 250 ms.
pub fn calm() -> Profile {
    let mut cfg = scenario("calm_weekday");
    cfg.sequencer.block_ms = 250;
    Profile {
        name: "calm",
        what: "calm weekday, 1 h, FCFS, B = 250 ms",
        records: record(&cfg, 1),
    }
}

/// News burst: the news-jump scenario with a crowd: 5 LPs, 10 snipers, order flow ×100.
pub fn news_burst() -> Profile {
    let mut cfg = scenario("news_jump");
    cfg.sequencer.block_ms = 250;
    cfg.lp.count = 5;
    cfg.sniper.count = 10;
    cfg.regular.weekday_rate_per_s *= 100.0;
    Profile {
        name: "news burst",
        what: "news jump, 120 s, 5 LPs, 10 snipers, flow x100, B = 250 ms",
        records: record(&cfg, 1),
    }
}

/// Monday open: Friday close, the hidden weekend and the reopen, FCFS, B = 250 ms.
pub fn monday_open() -> Profile {
    let mut cfg = scenario("monday_open");
    cfg.sequencer.block_ms = 250;
    Profile {
        name: "monday open",
        what: "Friday 15:00 to Monday 10:00, FCFS, B = 250 ms",
        records: record(&cfg, 1),
    }
}

pub fn recorded() -> Vec<Profile> {
    vec![calm(), news_burst(), monday_open()]
}

/// A long realistic stream for the channel comparison: the calm flow ×10 over a 24 h weekday.
pub fn long_stream() -> Vec<LogRecord> {
    let mut cfg = scenario("calm_weekday");
    cfg.duration_s = 24 * 3_600;
    cfg.sequencer.block_ms = 250;
    cfg.regular.weekday_rate_per_s *= 10.0;
    record(&cfg, 1)
}

/// A synthetic Monday burst. `warm` builds a deep book (`levels` price levels a side, 4 orders
/// each, around 100.00); `block` is one block of `n` messages arriving in the same block after
/// the reopen jump: cancels of resting quotes, fresh quotes around the new price, and IOCs that
/// sweep several levels. Sorted by `rule` like a sealed block.
pub struct Burst {
    pub warm: Vec<LogRecord>,
    pub block: Vec<LogRecord>,
}

pub fn burst(levels: i64, n: usize, rule: OrderingRule, seed: u64) -> Burst {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut seq = 0u64;
    let mut warm = Vec::new();
    let mut resting: Vec<(OrderId, u32)> = Vec::new();
    let mid = 10_000i64;
    for lvl in 1..=levels {
        for k in 0..4u32 {
            for side in [Side::Buy, Side::Sell] {
                seq += 1;
                let agent = 1 + (k % 4);
                let px = match side {
                    Side::Buy => mid - lvl,
                    Side::Sell => mid + lvl,
                };
                warm.push(LogRecord {
                    seq,
                    t_ns: seq,
                    block: 0,
                    agent,
                    kind: MsgKind::New {
                        side,
                        px_ticks: px,
                        qty_lots: 5 + rng.next_u64() % 10,
                        tif: Tif::Gtc,
                        priority_fee: 0,
                    },
                });
                resting.push((seq, agent));
            }
        }
    }

    // The block: arrivals in one window after a +20-tick reopen jump.
    let new_mid = mid + 20;
    let mut pending = Vec::with_capacity(n);
    for i in 0..n {
        let roll = rng.next_u64() % 100;
        let fee = rng.next_u64() % 1_000;
        let (agent, kind) = if roll < 40 && !resting.is_empty() {
            let j = (rng.next_u64() as usize) % resting.len();
            let (order_id, agent) = resting.swap_remove(j);
            (
                agent,
                MsgKind::Cancel {
                    order_id,
                    priority_fee: fee,
                },
            )
        } else if roll < 80 {
            let side = if rng.next_u64() % 2 == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let off = 1 + (rng.next_u64() % 5) as i64;
            let px = match side {
                Side::Buy => new_mid - off,
                Side::Sell => new_mid + off,
            };
            (
                1 + (rng.next_u64() % 4) as u32,
                MsgKind::New {
                    side,
                    px_ticks: px,
                    qty_lots: 5 + rng.next_u64() % 10,
                    tif: Tif::Gtc,
                    priority_fee: fee,
                },
            )
        } else {
            // Snipers and traders: IOCs that walk several stale levels.
            let side = if rng.next_u64() % 4 == 0 {
                Side::Sell
            } else {
                Side::Buy
            };
            let px = match side {
                Side::Buy => new_mid - 1,
                Side::Sell => new_mid - 40,
            };
            (
                10 + (rng.next_u64() % 10) as u32,
                MsgKind::New {
                    side,
                    px_ticks: px,
                    qty_lots: 10 + rng.next_u64() % 60,
                    tif: Tif::Ioc,
                    priority_fee: fee,
                },
            )
        };
        pending.push(Pending {
            t_ns: 1_000 + i as u64,
            tiebreak: i as u64,
            decided_ns: 0,
            agent,
            kind,
        });
    }
    sort_block(rule, &mut pending);
    let block = pending
        .into_iter()
        .map(|p| {
            seq += 1;
            LogRecord {
                seq,
                t_ns: p.t_ns,
                block: 1,
                agent: p.agent,
                kind: p.kind,
            }
        })
        .collect();
    Burst { warm, block }
}

/// A fresh engine with `records` applied.
pub fn engine_after(records: &[LogRecord]) -> Engine {
    let mut e = Engine::new();
    for r in records {
        e.on_message(r);
    }
    e
}

/// The share of each message kind, for the profile table.
pub fn mix(records: &[LogRecord]) -> (usize, usize, usize, usize) {
    let (mut gtc, mut ioc, mut cancel, mut other) = (0, 0, 0, 0);
    for r in records {
        match r.kind {
            MsgKind::New { tif: Tif::Gtc, .. } => gtc += 1,
            MsgKind::New { tif: Tif::Ioc, .. } => ioc += 1,
            MsgKind::Cancel { .. } => cancel += 1,
            MsgKind::Tick { .. } | MsgKind::Config { .. } => other += 1,
        }
    }
    (gtc, ioc, cancel, other)
}
