//! The input log on disk: postcard-encoded, with the scenario and seed it was made with.
//!
//! The engine crate has no serde dependency, so this module mirrors its record type and converts
//! both ways. The log is the source of truth: replaying it through a fresh engine reproduces
//! every event (see `replay`).

use engine::{Engine, EventKind, LogRecord, MsgKind, Side, Tif};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// 2: `Cancel` carries `priority_fee` (spec rev 36).
pub const FORMAT_VERSION: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogFile {
    pub format_version: u32,
    pub scenario: String,
    pub seed: u64,
    /// The full scenario, as TOML, so a log is self-describing.
    pub config_toml: String,
    pub records: Vec<WireRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireTif {
    Gtc,
    Ioc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireKind {
    New {
        side: WireSide,
        px_ticks: i64,
        qty_lots: u64,
        tif: WireTif,
        priority_fee: u64,
    },
    Cancel {
        order_id: u64,
        priority_fee: u64,
    },
    Tick {
        px_ticks: i64,
    },
    /// A live control change. Appended last, so the encoding of every other kind (and every
    /// log without one) is unchanged.
    Config {
        param: u32,
        value: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireRecord {
    pub seq: u64,
    pub t_ns: u64,
    pub block: u64,
    pub agent: u32,
    pub kind: WireKind,
}

impl From<&LogRecord> for WireRecord {
    fn from(r: &LogRecord) -> Self {
        let kind = match r.kind {
            MsgKind::New {
                side,
                px_ticks,
                qty_lots,
                tif,
                priority_fee,
            } => WireKind::New {
                side: match side {
                    Side::Buy => WireSide::Buy,
                    Side::Sell => WireSide::Sell,
                },
                px_ticks,
                qty_lots,
                tif: match tif {
                    Tif::Gtc => WireTif::Gtc,
                    Tif::Ioc => WireTif::Ioc,
                },
                priority_fee,
            },
            MsgKind::Cancel {
                order_id,
                priority_fee,
            } => WireKind::Cancel {
                order_id,
                priority_fee,
            },
            MsgKind::Tick { px_ticks } => WireKind::Tick { px_ticks },
            MsgKind::Config { param, value } => WireKind::Config { param, value },
        };
        WireRecord {
            seq: r.seq,
            t_ns: r.t_ns,
            block: r.block,
            agent: r.agent,
            kind,
        }
    }
}

impl From<&WireRecord> for LogRecord {
    fn from(w: &WireRecord) -> Self {
        let kind = match w.kind {
            WireKind::New {
                side,
                px_ticks,
                qty_lots,
                tif,
                priority_fee,
            } => MsgKind::New {
                side: match side {
                    WireSide::Buy => Side::Buy,
                    WireSide::Sell => Side::Sell,
                },
                px_ticks,
                qty_lots,
                tif: match tif {
                    WireTif::Gtc => Tif::Gtc,
                    WireTif::Ioc => Tif::Ioc,
                },
                priority_fee,
            },
            WireKind::Cancel {
                order_id,
                priority_fee,
            } => MsgKind::Cancel {
                order_id,
                priority_fee,
            },
            WireKind::Tick { px_ticks } => MsgKind::Tick { px_ticks },
            WireKind::Config { param, value } => MsgKind::Config { param, value },
        };
        LogRecord {
            seq: w.seq,
            t_ns: w.t_ns,
            block: w.block,
            agent: w.agent,
            kind,
        }
    }
}

pub fn encode(file: &LogFile) -> Vec<u8> {
    postcard::to_allocvec(file).expect("log encodes")
}

pub fn decode(bytes: &[u8]) -> Result<LogFile, String> {
    let f: LogFile = postcard::from_bytes(bytes).map_err(|e| e.to_string())?;
    if f.format_version != FORMAT_VERSION {
        return Err(format!("unsupported log format {}", f.format_version));
    }
    Ok(f)
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Incremental hash of the fills, in log order: the "fills log" the golden hashes cover.
#[derive(Default, Clone)]
pub struct FillsHasher(Sha256);

impl FillsHasher {
    pub fn update(&mut self, seq: u64, kind: &EventKind) {
        if let EventKind::Fill {
            maker_order,
            maker_agent,
            taker_order,
            taker_agent,
            taker_side,
            px,
            qty,
            maker_remaining,
        } = *kind
        {
            let h = &mut self.0;
            h.update(seq.to_le_bytes());
            h.update(maker_order.to_le_bytes());
            h.update(maker_agent.to_le_bytes());
            h.update(taker_order.to_le_bytes());
            h.update(taker_agent.to_le_bytes());
            h.update([taker_side as u8]);
            h.update(px.to_le_bytes());
            h.update(qty.to_le_bytes());
            h.update(maker_remaining.to_le_bytes());
        }
    }

    pub fn finish(self) -> String {
        self.0
            .finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }
}

/// Replay a log through a fresh engine and return the fills hash.
pub fn replay_fills_hash(records: &[LogRecord]) -> String {
    let mut e = Engine::new();
    let mut h = FillsHasher::default();
    let mut events = Vec::new();
    for r in records {
        e.on_message_into(r, &mut events);
        for ev in &events {
            h.update(ev.seq, &ev.kind);
        }
    }
    h.finish()
}
