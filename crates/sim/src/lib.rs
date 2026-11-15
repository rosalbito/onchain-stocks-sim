//! Deterministic simulation around the `engine` crate (spec sections 6 to 9 and 12): an event
//! queue on a virtual clock, the Nasdaq price feed with a hidden weekend, three agent types, the
//! simulated sequencer with three ordering rules, the ledger and the input log.

pub mod agents;
pub mod config;
pub mod ledger;
pub mod log;
pub mod price;
pub mod report;
pub mod rng;
pub mod sequencer;
mod sim;

pub use config::{OrderingRule, Scenario};
pub use sim::{run, Counts, RunResult};
