//! Scenario configuration (spec section 12). A scenario is a named TOML file; every field has the
//! spec's default (sections 7 and 8), so a file only needs what differs.
//!
//! Times are in seconds, latencies in microseconds, money in integer basis points, so the
//! parameters that touch the ledger are integers. Rates are `f64` because they only drive the
//! seeded samplers (see `rng.rs`), never the engine, ledger or metrics.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderingRule {
    /// First come, first served: arrival time.
    Fcfs,
    /// Priority gas auction: priority fee (highest first), then arrival.
    Pga,
    /// Cancels first, then the rest by arrival.
    CancelPriority,
}

impl OrderingRule {
    pub const ALL: [OrderingRule; 3] = [
        OrderingRule::Fcfs,
        OrderingRule::Pga,
        OrderingRule::CancelPriority,
    ];

    pub fn name(self) -> &'static str {
        match self {
            OrderingRule::Fcfs => "fcfs",
            OrderingRule::Pga => "pga",
            OrderingRule::CancelPriority => "cancel_priority",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Scenario {
    pub name: String,
    /// Simulated length of the run.
    pub duration_s: u64,
    pub sequencer: SequencerCfg,
    pub fees: FeesCfg,
    pub price: PriceCfg,
    /// The hidden weekend. Absent means a weekday-only run.
    pub weekend: Option<WeekendCfg>,
    pub lp: LpCfg,
    pub sniper: SniperCfg,
    pub regular: RegularCfg,
}

impl Default for Scenario {
    fn default() -> Self {
        Self {
            name: "default".into(),
            duration_s: 3_600,
            sequencer: SequencerCfg::default(),
            fees: FeesCfg::default(),
            price: PriceCfg::default(),
            weekend: None,
            lp: LpCfg::default(),
            sniper: SniperCfg::default(),
            regular: RegularCfg::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SequencerCfg {
    pub rule: OrderingRule,
    /// Block length B in milliseconds. 0 = every message is its own block.
    pub block_ms: u64,
}

impl Default for SequencerCfg {
    fn default() -> Self {
        Self {
            rule: OrderingRule::Fcfs,
            block_ms: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FeesCfg {
    /// Taker fee to the protocol, in basis points of notional.
    pub taker_fee_bps: u64,
}

impl Default for FeesCfg {
    fn default() -> Self {
        Self { taker_fee_bps: 1 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduledJump {
    pub at_s: f64,
    /// Signed jump size in ticks.
    pub ticks: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PriceCfg {
    /// Start price in ticks (tick = $0.01): 10_000 = $100.00.
    pub start_px_ticks: i64,
    /// Diffusion: ±1 tick steps arriving as a Poisson process at this rate (weekdays).
    pub step_rate_per_s: f64,
    /// Poisson jumps: rate per second (default: one every 5 minutes).
    pub jump_rate_per_s: f64,
    pub jump_min_ticks: i64,
    pub jump_max_ticks: i64,
    /// No random jumps before this time.
    pub jumps_start_after_s: f64,
    /// Deterministic jumps in addition to the random ones.
    pub scheduled_jumps: Vec<ScheduledJump>,
}

impl Default for PriceCfg {
    fn default() -> Self {
        Self {
            start_px_ticks: 10_000,
            step_rate_per_s: 1.0,
            jump_rate_per_s: 1.0 / 300.0,
            jump_min_ticks: 5,
            jump_max_ticks: 20,
            jumps_start_after_s: 0.0,
            scheduled_jumps: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WeekendCfg {
    /// Friday 16:00 ET, as seconds after the run starts.
    pub close_at_s: f64,
    /// Monday 09:30 ET, as seconds after the run starts.
    pub reopen_at_s: f64,
    /// Hidden diffusion: ±1 tick steps at this rate while the price is hidden.
    pub step_rate_per_s: f64,
}

impl Default for WeekendCfg {
    fn default() -> Self {
        Self {
            close_at_s: 3_600.0,
            // Fri 16:00 -> Mon 09:30 is 65.5 hours.
            reopen_at_s: 3_600.0 + 65.5 * 3_600.0,
            step_rate_per_s: 0.1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Latency {
    pub base_us: u64,
    /// Uniform jitter: the draw is in [base - jitter, base + jitter].
    pub jitter_us: u64,
}

impl Latency {
    pub const fn new(base_us: u64, jitter_us: u64) -> Self {
        Self { base_us, jitter_us }
    }

    fn scaled(self, num: u64, den: u64) -> Self {
        Self {
            base_us: self.base_us * num / den,
            jitter_us: self.jitter_us * num / den,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WeekendPolicy {
    Keep,
    Widen,
    Withdraw,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuoteLevel {
    pub offset_ticks: i64,
    pub qty_lots: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LpCfg {
    pub count: u32,
    pub see: Latency,
    pub send: Latency,
    pub levels: Vec<QuoteLevel>,
    /// Requote when the seen price moves this many ticks from the quoted center.
    pub requote_ticks: i64,
    pub inventory_cap_lots: i64,
    pub weekend_policy: WeekendPolicy,
    pub weekend_widen: i64,
    /// PGA bid on cancels, as basis points of exposure (spec section 8: β; 10_000 = β 1.0).
    /// Exposure is max(0, staleness at the LP's seen price) × lots; see docs/DECISIONS.md.
    pub beta_bps: u64,
    /// With several LPs, LP i sees prices `see.base_us + i × spread / (count - 1)` late, so the
    /// LPs range from fast to slow. 0 (the default, and every grid scenario) means all equal;
    /// it is left out of the TOML then, so existing logs stay byte-identical.
    #[serde(skip_serializing_if = "is_zero")]
    pub see_spread_us: u64,
}

fn is_zero(v: &u64) -> bool {
    *v == 0
}

impl Default for LpCfg {
    fn default() -> Self {
        Self {
            count: 1,
            see: Latency::new(20_000, 5_000),
            send: Latency::new(3_000, 1_000),
            levels: vec![
                QuoteLevel {
                    offset_ticks: 1,
                    qty_lots: 10,
                },
                QuoteLevel {
                    offset_ticks: 2,
                    qty_lots: 5,
                },
            ],
            requote_ticks: 1,
            inventory_cap_lots: 100,
            weekend_policy: WeekendPolicy::Widen,
            weekend_widen: 3,
            beta_bps: 0,
            see_spread_us: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SniperCfg {
    pub count: u32,
    pub see: Latency,
    pub send: Latency,
    pub theta_ticks: i64,
    /// PGA bid as basis points of the expected prize (5000 = α 0.5).
    pub alpha_bps: u64,
}

impl Default for SniperCfg {
    fn default() -> Self {
        Self {
            count: 3,
            see: Latency::new(5_000, 1_000),
            send: Latency::new(3_000, 1_000),
            theta_ticks: 1,
            alpha_bps: 5_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RegularCfg {
    pub count: u32,
    /// Poisson arrival rate per trader.
    pub weekday_rate_per_s: f64,
    pub weekend_rate_per_s: f64,
    pub size_median_lots: f64,
    /// Lognormal shape (σ of the underlying normal).
    pub size_sigma: f64,
    pub size_cap_lots: u64,
    /// Limit = best opposite price ± this many ticks.
    pub limit_offset_ticks: i64,
    pub send: Latency,
}

impl Default for RegularCfg {
    fn default() -> Self {
        Self {
            count: 1,
            weekday_rate_per_s: 0.5,
            weekend_rate_per_s: 0.1,
            size_median_lots: 2.0,
            size_sigma: 1.0,
            size_cap_lots: 50,
            limit_offset_ticks: 5,
            send: Latency::new(50_000, 20_000),
        }
    }
}

impl Scenario {
    /// The live demo's crowd: the same three roles with more of each, so the book has depth and
    /// the engine sees thousands of messages a second at 1× instead of tens. 24 LPs from fast
    /// (20 ms) to slow (80 ms) with five-level ladders, 3 snipers as before, 60 regular traders,
    /// and a Nasdaq feed that moves 4× as often. The experiment grid never uses it.
    pub fn with_crowd(&self) -> Scenario {
        let mut s = self.clone();
        s.lp.count = 24;
        s.lp.see_spread_us = 60_000;
        s.lp.levels = [(1, 10), (2, 8), (3, 6), (4, 4), (5, 2)]
            .map(|(offset_ticks, qty_lots)| QuoteLevel {
                offset_ticks,
                qty_lots,
            })
            .to_vec();
        s.regular.count = 60;
        s.price.step_rate_per_s *= 4.0;
        s
    }

    pub fn from_toml(text: &str) -> Result<Self, String> {
        let s: Scenario = toml::from_str(text).map_err(|e| e.to_string())?;
        s.validate()?;
        Ok(s)
    }

    pub fn to_toml(&self) -> String {
        toml::to_string(self).expect("scenario serialises")
    }

    pub fn validate(&self) -> Result<(), String> {
        let lats = [
            ("lp.see", self.lp.see),
            ("lp.send", self.lp.send),
            ("sniper.see", self.sniper.see),
            ("sniper.send", self.sniper.send),
            ("regular.send", self.regular.send),
        ];
        for (name, l) in lats {
            if l.jitter_us > l.base_us {
                return Err(format!("{name}: jitter larger than base latency"));
            }
        }
        if self.price.jump_min_ticks < 0 || self.price.jump_max_ticks < self.price.jump_min_ticks {
            return Err("price: bad jump size range".into());
        }
        if self.price.start_px_ticks <= 0 {
            return Err("price.start_px_ticks must be positive".into());
        }
        if self.lp.requote_ticks < 1 || self.sniper.theta_ticks < 1 {
            return Err("lp.requote_ticks and sniper.theta_ticks must be >= 1".into());
        }
        if let Some(w) = &self.weekend {
            if w.close_at_s.partial_cmp(&w.reopen_at_s) != Some(std::cmp::Ordering::Less) {
                return Err("weekend: close must be before reopen".into());
            }
        }
        let rates = [
            self.price.step_rate_per_s,
            self.price.jump_rate_per_s,
            self.regular.weekday_rate_per_s,
            self.regular.weekend_rate_per_s,
            self.regular.size_sigma,
        ];
        if rates.iter().any(|r| !(r.is_finite() && *r >= 0.0)) {
            return Err("rates must be finite and non-negative".into());
        }
        if self.regular.size_median_lots < 1.0 || self.regular.size_cap_lots < 1 {
            return Err("regular: size median and cap must be >= 1 lot".into());
        }
        Ok(())
    }

    /// Set the speed gap (spec section 13): every sniper latency (see and send, base and jitter)
    /// becomes the LP's times `num / den`.
    pub fn set_speed_gap(&mut self, num: u64, den: u64) {
        self.sniper.see = self.lp.see.scaled(num, den);
        self.sniper.send = self.lp.send.scaled(num, den);
    }

    /// Multiply every agent latency (base and jitter) by `num / den`.
    pub fn scale_latencies(&mut self, num: u64, den: u64) {
        self.lp.see = self.lp.see.scaled(num, den);
        self.lp.send = self.lp.send.scaled(num, den);
        self.sniper.see = self.sniper.see.scaled(num, den);
        self.sniper.send = self.sniper.send.scaled(num, den);
        self.regular.send = self.regular.send.scaled(num, den);
    }
}
