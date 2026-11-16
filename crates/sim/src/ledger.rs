//! The ledger (spec section 9). Every entry is a transfer between accounts, so totals always
//! balance: a fill moves cash and shares between buyer and seller and the taker fee to the
//! protocol; a priority fee moves from the sender to the chain when its message is included.

use std::collections::BTreeMap;

use engine::{AgentId, Event, EventKind, LogRecord, Side};
use metrics::{AccountClass, Balance, CASH_PER_TICK_LOT};

/// Starting balances, large enough that no one runs out (there is no margin in v1).
pub const START_CASH: i128 = 1_000_000_000_000_000_000;
pub const START_SHARES: i64 = 1_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Account {
    pub class: AccountClass,
    pub cash: i128,
    pub shares: i64,
}

impl Account {
    fn new(class: AccountClass) -> Self {
        Self {
            class,
            cash: START_CASH,
            shares: START_SHARES,
        }
    }

    pub fn balance(&self) -> Balance {
        Balance {
            class: self.class,
            cash_delta: self.cash - START_CASH,
            shares_delta: self.shares - START_SHARES,
        }
    }
}

pub struct Ledger {
    taker_fee_bps: i128,
    agents: BTreeMap<AgentId, Account>,
    protocol: Account,
    chain: Account,
    /// Totals for the summary.
    pub taker_fees: i128,
    pub priority_fees: i128,
}

impl Ledger {
    pub fn new(taker_fee_bps: u64, agents: &BTreeMap<AgentId, AccountClass>) -> Self {
        Self {
            taker_fee_bps: taker_fee_bps as i128,
            agents: agents
                .iter()
                .map(|(&id, &c)| (id, Account::new(c)))
                .collect(),
            protocol: Account::new(AccountClass::Protocol),
            chain: Account::new(AccountClass::Chain),
            taker_fees: 0,
            priority_fees: 0,
        }
    }

    fn agent(&mut self, id: AgentId) -> &mut Account {
        self.agents
            .get_mut(&id)
            .unwrap_or_else(|| panic!("no account for agent {id}"))
    }

    /// A message was included in the log: its sender pays its priority fee to the chain,
    /// whether or not it fills.
    pub fn on_included(&mut self, rec: &LogRecord) {
        let fee = rec.kind.priority_fee() as i128;
        if fee > 0 {
            self.agent(rec.agent).cash -= fee;
            self.chain.cash += fee;
            self.priority_fees += fee;
        }
    }

    pub fn on_events(&mut self, events: &[Event]) {
        for ev in events {
            if let EventKind::Fill {
                maker_agent,
                taker_agent,
                taker_side,
                px,
                qty,
                ..
            } = ev.kind
            {
                let (buyer, seller) = match taker_side {
                    Side::Buy => (taker_agent, maker_agent),
                    Side::Sell => (maker_agent, taker_agent),
                };
                let notional = px as i128 * qty as i128 * CASH_PER_TICK_LOT;
                // Cash units are 1/10_000 tick·lot, so fee_bps of notional is exact.
                let fee = px as i128 * qty as i128 * self.taker_fee_bps;
                let b = self.agent(buyer);
                b.cash -= notional;
                b.shares += qty as i64;
                let s = self.agent(seller);
                s.cash += notional;
                s.shares -= qty as i64;
                self.agent(taker_agent).cash -= fee;
                self.protocol.cash += fee;
                self.taker_fees += fee;
            }
        }
    }

    pub fn balances(&self) -> Vec<Balance> {
        self.agents
            .values()
            .chain([&self.protocol, &self.chain])
            .map(Account::balance)
            .collect()
    }

    pub fn account(&self, id: AgentId) -> Option<&Account> {
        self.agents.get(&id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine::MsgKind;

    #[test]
    fn sender_pays_priority_fee_on_inclusion_for_cancels_too() {
        let agents: BTreeMap<AgentId, AccountClass> = [(1, AccountClass::Lp)].into();
        let mut l = Ledger::new(1, &agents);
        let rec = |kind| LogRecord {
            seq: 1,
            t_ns: 0,
            block: 0,
            agent: 1,
            kind,
        };
        // A cancel for an order that does not exist still pays: inclusion, not success.
        l.on_included(&rec(MsgKind::Cancel {
            order_id: 99,
            priority_fee: 7_000,
        }));
        assert_eq!(l.account(1).unwrap().balance().cash_delta, -7_000);
        assert_eq!(l.priority_fees, 7_000);
        let pnl = metrics::zero_sum_check(&l.balances(), 10_000).unwrap();
        assert_eq!(pnl.get(AccountClass::Chain), 7_000);
    }
}
