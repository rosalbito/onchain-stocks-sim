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
}

impl Sequencer {
    pub fn new(rule: OrderingRule, block_ms: u64) -> Self {
        Self {
            rule,
            block_ns: block_ms * 1_000_000,
            open: BTreeMap::new(),
            arrivals: 0,
            next_solo_block: 0,
        }
    }

    pub fn rule(&self) -> OrderingRule {
        self.rule
    }

    /// The block a message (or a tick) arriving at `t_ns` belongs to.
    pub fn block_at(&self, t_ns: u64) -> u64 {
        if self.block_ns == 0 {
            self.next_solo_block
        } else {
            t_ns / self.block_ns
        }
    }

    pub fn accept(&mut self, t_ns: u64, agent: AgentId, kind: MsgKind) -> Accepted {
        let tiebreak = self.arrivals;
        self.arrivals += 1;
        if self.block_ns == 0 {
            let block = self.next_solo_block;
            self.next_solo_block += 1;
            return Accepted::Now { block };
        }
        let block = t_ns / self.block_ns;
        let entry = self.open.entry(block).or_default();
        let first = entry.is_empty();
        entry.push(Pending {
            t_ns,
            tiebreak,
            agent,
            kind,
        });
        Accepted::Buffered {
            schedule_close: first.then_some((block, (block + 1) * self.block_ns)),
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
