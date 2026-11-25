//! The simulated sequencer (spec section 6). Orders received in [kB, (k+1)B) go into block k;
//! at (k+1)B the block is sealed and sorted by the ordering rule, with equal keys falling back
//! to arrival time and then to an arrival counter. With B = 0 every message is its own block.

use std::cmp::Reverse;
use std::collections::BTreeMap;

use engine::{AgentId, MsgKind};

use crate::config::OrderingRule;

/// A message waiting in an open block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pending {
    /// Arrival time at the sequencer.
    pub t_ns: u64,
    /// Arrival counter, the final tie-break.
    pub tiebreak: u64,
    /// When the sender decided to send it. Not used for ordering; kept for the metrics.
    pub decided_ns: u64,
    pub agent: AgentId,
    pub kind: MsgKind,
}

/// Sort a sealed block in place by the ordering rule.
pub fn sort_block(rule: OrderingRule, block: &mut [Pending]) {
    match rule {
        OrderingRule::Fcfs => block.sort_by_key(|p| (p.t_ns, p.tiebreak)),
        OrderingRule::Pga => {
            block.sort_by_key(|p| (Reverse(p.kind.priority_fee()), p.t_ns, p.tiebreak))
        }
        OrderingRule::CancelPriority => {
            block.sort_by_key(|p| (!p.kind.is_cancel(), p.t_ns, p.tiebreak))
        }
    }
}

/// What the event loop must do after `accept`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Accepted {
    /// B = 0: sequence this message now, as block `block`.
    Now { block: u64 },
    /// Buffered in an open block. If this opened the block, schedule its close.
    Buffered { schedule_close: Option<(u64, u64)> },
}

pub struct Sequencer {
    rule: OrderingRule,
    block_ns: u64,
    open: BTreeMap<u64, Vec<Pending>>,
    arrivals: u64,
    /// With B = 0, the block counter (one block per message).
    next_solo_block: u64,
    /// Block numbering since the last live change of B: block `epoch_base + k` covers
    /// [epoch_start_ns + kB, epoch_start_ns + (k+1)B). Both are 0 unless B changed, so a run
    /// without live changes numbers blocks t / B, as before.
    epoch_start_ns: u64,
    epoch_base: u64,
}

impl Sequencer {
    pub fn new(rule: OrderingRule, block_ms: u64) -> Self {
        Self {
            rule,
            block_ns: block_ms * 1_000_000,
            open: BTreeMap::new(),
            arrivals: 0,
            next_solo_block: 0,
            epoch_start_ns: 0,
            epoch_base: 0,
        }
    }

    pub fn rule(&self) -> OrderingRule {
        self.rule
    }

    pub fn block_ms(&self) -> u64 {
        self.block_ns / 1_000_000
    }

    /// The block a message (or a tick) arriving at `t_ns` belongs to.
    pub fn block_at(&self, t_ns: u64) -> u64 {
        if self.block_ns == 0 {
            self.next_solo_block
        } else {
            self.epoch_base + (t_ns - self.epoch_start_ns) / self.block_ns
        }
    }

    /// When block `block` closes (B > 0).
    fn close_ns(&self, block: u64) -> u64 {
        self.epoch_start_ns + (block - self.epoch_base + 1) * self.block_ns
    }

    /// Whether `block` holds messages waiting for its close.
    pub fn is_open(&self, block: u64) -> bool {
        self.open.contains_key(&block)
    }

    /// Live change of the ordering rule. Blocks still open are sorted by the new rule when
    /// they close.
    pub fn set_rule(&mut self, rule: OrderingRule) {
        self.rule = rule;
    }

    /// Live change of B at `now`. Every open block is sealed now (returned in block order,
    /// each sorted by the current rule), and numbering restarts after the last block used, so
    /// block numbers keep increasing and a stale close event finds no block.
    pub fn set_block_ms(&mut self, now: u64, block_ms: u64) -> Vec<(u64, Vec<Pending>)> {
        let next = if self.block_ns == 0 {
            self.next_solo_block
        } else {
            self.block_at(now) + 1
        };
        let open = std::mem::take(&mut self.open);
        let sealed = open
            .into_iter()
            .map(|(block, mut msgs)| {
                sort_block(self.rule, &mut msgs);
                (block, msgs)
            })
            .collect();
        self.block_ns = block_ms * 1_000_000;
        self.epoch_start_ns = now;
        self.epoch_base = next;
        self.next_solo_block = next;
        sealed
    }

    pub fn accept(
        &mut self,
        t_ns: u64,
        decided_ns: u64,
        agent: AgentId,
        kind: MsgKind,
    ) -> Accepted {
        let tiebreak = self.arrivals;
        self.arrivals += 1;
        if self.block_ns == 0 {
            let block = self.next_solo_block;
            self.next_solo_block += 1;
            return Accepted::Now { block };
        }
        let block = self.block_at(t_ns);
        let close = self.close_ns(block);
        let entry = self.open.entry(block).or_default();
        let first = entry.is_empty();
        entry.push(Pending {
            t_ns,
            tiebreak,
            decided_ns,
            agent,
            kind,
        });
        Accepted::Buffered {
            schedule_close: first.then_some((block, close)),
        }
    }

    /// Seal block `k` and return its messages in processing order.
    pub fn seal(&mut self, block: u64) -> Vec<Pending> {
        let mut msgs = self.open.remove(&block).unwrap_or_default();
        sort_block(self.rule, &mut msgs);
        msgs
    }

    /// Messages still waiting in open blocks.
    pub fn waiting(&self) -> usize {
        self.open.values().map(Vec::len).sum()
    }
}
