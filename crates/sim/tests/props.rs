//! Simulation-level property tests (spec section 13, item 1): for random seeds, ordering rules,
//! block lengths and agent counts, total PnL is zero and the same seed gives the same hash.

use proptest::prelude::*;
use sim::{OrderingRule, Scenario};

fn scenario(rule: usize, block_ms: u64, snipers: u32, regulars: u32, weekend: bool) -> Scenario {
    let mut s = Scenario {
        name: "prop".into(),
        duration_s: 180,
        ..Scenario::default()
    };
    s.sequencer.rule = OrderingRule::ALL[rule];
    s.sequencer.block_ms = block_ms;
    s.sniper.count = snipers;
    s.regular.count = regulars;
    s.regular.weekday_rate_per_s = 2.0; // busy book
    s.price.jump_rate_per_s = 0.1;
    if weekend {
        s.weekend = Some(sim::config::WeekendCfg {
            close_at_s: 60.0,
            reopen_at_s: 120.0,
            step_rate_per_s: 1.0,
        });
    }
    s
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn zero_sum_and_same_hash(
        seed in any::<u64>(),
        rule in 0usize..3,
        block_ms in prop_oneof![Just(0u64), Just(50), Just(250), Just(1_000)],
        snipers in 0u32..5,
        regulars in 1u32..4,
        weekend in any::<bool>(),
    ) {
        let s = scenario(rule, block_ms, snipers, regulars, weekend);
        let a = sim::run(&s, seed).map_err(TestCaseError::fail)?;
        prop_assert_eq!(a.pnl.by_class.values().sum::<i128>(), 0);
        prop_assert_eq!(a.take_at_fill.values().sum::<i128>(), 0);
        let b = sim::run(&s, seed).map_err(TestCaseError::fail)?;
        prop_assert_eq!(&a.log_hash, &b.log_hash);
        prop_assert_eq!(&a.fills_hash, &b.fills_hash);
    }
}
