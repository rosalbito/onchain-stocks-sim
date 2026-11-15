# Market Design for On-Chain Stocks: Spec v1 (locked)

Nov 13, 2026 · @Roberto carvalho

This is the locked v1. We build a deterministic Rust order-book (CLOB) engine with a simulated sequencer, and use it to measure who loses money to latency races on an on-chain stock, how the sequencer's ordering rule changes that, and how much load the engine takes. Anything not in this doc is a later branch.

## 1. Question and results

Given the same order flow, how much do latency races and the Monday open take from liquidity providers (LPs) and regular traders on an on-chain order book, and how does the sequencer's ordering rule change who keeps that money?

**Results for the stage:**

1. **The main chart:** LP losses to snipers (bps of volume) for each ordering rule (first come first served, priority gas auction, cancel priority) at several block times, with weekday and Monday-open lines.
2. **The engine benchmark:** messages per second and p50/p99/p99.9 latency for the engine alone and behind each channel type.
3. **The live demo:** a replayed Monday open and a stress run on the dashboard, with the ordering rule switched live.

**Hypotheses (the runs may refute them):**

- **H1.** Under first come, first served, LPs lose to snipers after every public price jump. With N equally fast snipers, the LP wins only about 1 race in N+1 ([Budish, Cramton and Shim, 2015](https://academic.oup.com/qje/article/130/4/1547/1916146)).
- **H2.** A priority gas auction doesn't shrink the LP's loss. It moves most of the prize from snipers to the chain, as fees.
- **H3.** Cancel priority removes most of the LP's loss. What's left comes from price jumps that straddle a block boundary.
- **H4.** The Monday open is the largest single transfer of the week.
- **H5.** One book on one core handles far more messages than a block needs. On-chain, network distance and the ordering rule decide who wins, not matching speed.

## 2. Scope

The deliverable is a reproducible measurement, a benchmark and a live demo, not a venue anyone trades on.

**Goals**

- A deterministic Rust CLOB engine on a single writer thread. The same input log always gives byte-identical output.
- A simulated sequencer that forms blocks and orders them by a switchable rule.
- Three agent types, a price feed with a hidden weekend and a Monday jump, and a ledger whose gains and losses sum to zero.
- Market metrics for every run, an experiment grid with confidence intervals, and named stress scenarios.
- An engine benchmark: the engine alone, then behind an SPSC ring, crossbeam and tokio `mpsc`.
- A live dashboard over a websocket, in the visual identity of rosalbito.dev.

**Non-goals**

- A real chain, a frequent batch auction (FBA) or an AMM. The slides explain them; we don't build them.
- Custody, settlement, KYC or a trading front end.
- Optimised strategies. Agents follow simple stated rules.
- Production durability: log replication, failover, sharding.

**Later branches**, in rough priority order: FBA as a second engine mode, a real sequencer (nitro-devnode, then testnet), measuring real latencies to the sequencer, fitting regular traders to wallet data, an adaptive or informed LP, a protocol-owned LP vault, oracle-pegged orders, and Stylus.

## 3. Locked decisions

| Decision | v1 choice | Why |
| --- | --- | --- |
| Matching | Continuous order book, price-time priority | What nearly every production venue runs; the race happens in it naturally |
| Sequencer | Simulated: blocks of length B, sorted by an ordering rule | The ordering rule is the main experiment; a real chain only adds cost and noise |
| Latency | Virtual clock; each agent has a see-latency and a send-latency with seeded jitter | Deterministic and fast; toxiproxy would add made-up delays that vary run to run |
| Price | One Nasdaq price path, hidden from Friday close to the Monday open | One process produces both weekday sniping and the Monday-open jump |
| Accounts | Regular traders, LPs, snipers, protocol, chain | Every dollar lost lands in one of them, so the books must balance |
| Concurrency | Simulation: one thread. Live mode: one engine thread, one SPSC ring per boundary | Matching is sequential; locks only slow it down |
| Network edge | tokio + axum websocket, `watch` for the latest state | Only the dashboard needs async I/O |
| Persistence | Append-only input log file plus snapshots; no message broker | The log already gives replay; Kafka or RabbitMQ add hops we don't need |
| Units | Prices in i64 ticks, sizes in u64 lots, notional in i128; no floats | Exact arithmetic is what makes replays identical |

## 4. Determinism contract

A run is a pure function: (config, seed) → input log → fills, ledger and metrics. Running the same pair twice gives byte-identical output on any machine.

**"Same order flow" means the same outside inputs.** Agents react to the market, so snipers send different orders under different ordering rules. What stays fixed across rules is everything outside the market: the price path, the regular traders' arrival times and sizes, and the latency draws. Each rule gets the same seed for each of these (common random numbers).

**Input-log record** (one per sequenced message):

| Field | Type | Notes |
| --- | --- | --- |
| `seq` | u64 | Position in the log. This alone decides processing order. |
| `t_ns` | u64 | Virtual time the sequencer received it |
| `block` | u64 | Block it was sealed in |
| `agent` | u32 | Sender. Used for the ledger and metrics, never for matching. |
| `kind` | enum | `New{side, px_ticks: i64, qty_lots: u64, tif: GTC\|IOC, priority_fee: u64}`, `Cancel{order_id}`, `Tick{px_ticks}` |

`Tick` is a Nasdaq price update. The engine passes it through untouched; it is logged so the dashboard and the metrics replay exactly.

**Rules the code must follow**

- No floats in the engine, the ledger or the metrics. Convert to bps only in reports.
- `BTreeMap` or `Vec` only; never iterate a `HashMap`. No wall-clock reads and no threads inside a simulation run. Runs execute in parallel with each other.
- Each agent gets its own `ChaCha8Rng` stream, seeded from the master seed and the agent id, so adding an agent leaves the others' draws unchanged.
- CI holds golden hashes of the fills log for a fixed set of seeds per ordering rule. Changing one needs an explicit sign-off.

## 5. Matching engine (CLOB)

The engine matches each message the moment it processes it, in `seq` order, and knows nothing about agents, time or harnesses.

| Rule | v1 behaviour |
| --- | --- |
| Priority | Price, then time (`seq`) |
| Execution price | The resting order's price |
| Partial fills | Walk price levels best first; inside a level, earliest resting order first |
| IOC remainder | Cancelled |
| GTC remainder | Rests in the book |
| Cancels | Apply when processed; a cancel for an already-filled order is a no-op that is reported |
| Self-trade | Cancel the resting order |
| `Tick` | Ignored by matching, echoed as an event |
| Market data | An event on every book change |

**Interface:** `Engine::on_message(&mut self, msg: &LogRecord) -> Events`. Events are fills, book changes, cancels and echoed ticks, each tagged with `seq`.

**Data structure:** start with `BTreeMap<px, VecDeque<OrderId>>` per side, orders in a `slab`, plus an id → location index for cancels. Switch to a fixed array of levels indexed by tick only if the benchmark shows the tree is the bottleneck.

## 6. Sequencer and blocks

The sequencer decides the order; the engine decides what that order means. The ordering rule is a config switch and the main experiment.

1. Every order the sequencer receives between time kB and (k+1)B goes into block k, tagged with its arrival time `t_ns`.
2. At (k+1)B a `BlockClose` event fires. The sequencer seals block k and sorts it by the ordering rule. Equal keys fall back to arrival time, then to a tiebreak counter.
3. The sorted messages get the next `seq` numbers, are appended to the input log, and go to the engine one by one.
4. An order that arrives after the close waits for the next block.

| Ordering rule | Sort key inside a block | Real-world example |
| --- | --- | --- |
| First come, first served | Arrival time | [Robinhood Chain's documented policy](https://docs.robinhood.com/chain/differences-from-ethereum) |
| Priority gas auction | `priority_fee`, highest first; then arrival | [Arbitrum One since Sept 2026](https://forum.arbitrum.foundation/t/constitutional-aip-transition-arbitrum-one-ordering-policy-to-priority-gas-auctions-pga/30942); Robinhood Chain [reportedly evaluating it](https://www.coindesk.com/business/2026/10/09/robinhood-chain-considers-technology-that-gives-paying-traders-priority) |
| Cancel priority | Cancels first, then the rest by arrival | Hyperliquid (to verify) |

The block length B is a parameter. With B = 0, every message is its own block and first come, first served becomes a pure continuous market. Under first come, first served, B never changes who wins, only how long fills wait. Under the other two rules, B is the window inside which the order can change. Price ticks are not orders: they're logged at their own time and never wait for a block.

## 7. Price feed and the weekend

There is one price: Nasdaq's. It always moves, but over the weekend nobody can see it, while the on-chain book keeps trading.

- **Path:** a random walk in whole ticks with Poisson jumps. Defaults: tick $0.01, start 100.00, a diffusion of about 1 tick per second, and jumps about every 5 minutes of 5 to 20 ticks either way. All seeded.
- **Visible (weekdays):** each move is a `Tick` in the log at its true time. Each agent sees it after its own see-latency.
- **Hidden (Friday 16:00 ET to Monday 09:30 ET by default):** the path keeps moving at a weekend volatility, but no `Tick` is emitted. Agents quote and trade against the last price they saw.
- **The reopen:** at 09:30 Monday, one `Tick` jumps straight to wherever the hidden path ended. That jump is the Monday-open gap.

The real reopen is a cascade (index futures Sunday evening, overnight venues from Sunday 20:00 ET, pre-market from 04:00, the opening cross at 09:30). v1 uses a single jump; the cascade is a later branch.

## 8. Agents

Three agent types produce all the flow. Their rules are fixed and simple; we sweep their parameters, never optimise them.

| Agent | Rule | Defaults |
| --- | --- | --- |
| Liquidity provider | Quotes two levels each side around the last price it saw: 10 lots at ±1 tick, 5 lots at ±2 ticks. When the seen price moves by 1 tick or more, cancels everything and requotes. Stops quoting the side that would push inventory past the cap. | 1 LP; see 20 ± 5 ms, send 3 ± 1 ms; inventory cap ±100 lots; weekend policy: widen ×3 (or keep, or withdraw); under PGA bids `β × exposure` on its cancels, β = 0 |
| Sniper | On each price it sees, finds resting quotes that are stale by at least θ and sends an IOC to take them, with limit = seen price − θ for buys (+ θ for sells). | 3 snipers; see 5 ± 1 ms, send 3 ± 1 ms; θ = 1 tick; under PGA bids `α × expected prize`, α = 0.5 |
| Regular trader | Arrives at random and sends a marketable IOC: limit = best opposite price ± 5 ticks. Doesn't watch the Nasdaq price. | Poisson, 0.5/s on weekdays and 0.1/s on weekends; size lognormal, median 2 lots, capped at 50; buy or sell 50/50; send 50 ± 20 ms |

These latencies reproduce the race from the mental-model doc: the sniper's order arrives at about 8 ms, the LP's cancel at about 23 ms. All values are config, and the experiment grid sweeps the important ones.

## 9. Ledger and accounts

No real funds move. A fill is a ledger entry, and every entry is a transfer between five account classes, so the totals always balance.

| Account class | Gets money from |
| --- | --- |
| Regular traders | Their trades |
| LPs | The spread from regular traders; loses to snipers |
| Snipers | Stale LP quotes |
| Protocol | Taker fees (default 1 bp of notional) |
| Chain | Priority fees under PGA, paid when a message is included, whether or not it fills |

Each agent account holds cash (i128) and shares (i64 lots), starting large enough that no one runs out; there's no margin in v1. A fill moves cash and shares between buyer and seller and the taker fee to the protocol. PnL is marked to the Nasdaq price. Because every fill and fee is a transfer, total PnL across all accounts is zero, and every run checks this.

## 10. Metrics

Every fill is valued against the Nasdaq price v, so each market metric is a transfer between account classes. A fill f has price p\_f, quantity q\_f and time t\_f; d\_a(f) is +1 if agent a bought and −1 if it sold; h is the markout horizon (1 s, 10 s, 60 s).

```latex
\mathrm{PnL}_a = \sum_{f \ni a} d_a(f)\, q_f \,\bigl(v_{t_f + h} - p_f\bigr) - \mathrm{fees}_a, \qquad \sum_a \mathrm{PnL}_a = 0
```

**A race** starts when a price move makes a resting quote stale by at least θ, and ends when that quote is either filled by a sniper (sniper wins) or cancelled (LP wins).

| Market metric | Definition | Reported as |
| --- | --- | --- |
| Sniper take | PnL summed over snipers | bps of volume, $ per day |
| LP race win rate | Races the LP won ÷ all races | share, 0 to 1 |
| LP adverse selection | Volume-weighted effective minus realized half-spread on LP fills | bps |
| Chain take | Priority fees paid (PGA only) | bps of volume |
| Regular-trader cost | Fill price against v at the moment the trader decided | bps |
| Fill delay | Fill time minus decision time, including the wait for a block | median and p99, ms |
| Quoted liquidity | Time-weighted half-spread; depth within 10 bps of v | bps, lots |
| Monday-open take | Sniper PnL in the first W after the reopen ÷ (gap × quantity resting at Friday close) | share of the gap, 0 to 1 |

| Engine metric | Definition |
| --- | --- |
| Throughput | Messages per second, sustained |
| Per-message latency | Message in → events out: p50, p99, p99.9 (HdrHistogram) |
| Block processing | Time to sort and match one block, at 100, 1k and 10k messages |
| Scaling | Cost against book depth |
| Memory | Allocations per message, peak RSS |

## 11. Architecture and build vs reuse

The full design, with diagrams, is in the [architecture doc](https://claude.ai/artifact/6mgaTvdBXsjjhGafLUjxSi). In short: one engine crate, driven by a simulation on a virtual clock and by a benchmark on the wall clock. The input log is the source of truth, and everything else can be rebuilt from it.

**Crates (one Cargo workspace):**

| Crate | Holds | Threads? |
| --- | --- | --- |
| `engine` | Book, matching, events. No dependencies beyond std and `slab`. | No |
| `sim` | Event queue, price feed, agents, sequencer, ledger, input log | No |
| `metrics` | Market metrics, zero-sum check, reports (parquet) | No |
| `bench` | Engine benchmark and the channel comparison | Yes |
| `server` | Live mode: engine thread, one `rtrb` ring per reader, axum websocket | Yes, tokio at the edge only |
| `dashboard` | Static web page that connects to the websocket | Browser |

**We write ourselves** (this is the talk): the engine, the sequencer and ordering rules, the agents, the price model, the ledger and metrics, the scenarios and the dashboard.

**We reuse:**

| Need | Crate or tool |
| --- | --- |
| Event queue | `std::collections::BinaryHeap` keyed by `(time, tiebreak)` |
| Order storage | `slab` |
| Randomness | `rand_chacha::ChaCha8Rng`, `rand_distr` |
| Config | `serde` + `toml` |
| Log and outputs | `postcard` for the input log; `arrow`/`parquet` for results |
| Parameter sweeps | `rayon`, across runs only |
| Tests | `proptest`, `insta` golden files, `cargo-fuzz` later |
| Benchmarks | `criterion`, `hdrhistogram`, `core_affinity`, `perf`, `cargo flamegraph` |
| Channels | `rtrb`; `crossbeam-channel` and `tokio::sync::mpsc` only as benchmark comparisons; `tokio::sync::watch` for the UI |
| Websocket | `axum` |
| Charts in the dashboard | TradingView `lightweight-charts`, restyled |
| Analysis | Python with polars and matplotlib |

Designs to read, not depend on: hftbacktest (latency and queue modelling), NautilusTrader, ABIDES, and the LMAX Disruptor and exchange-core.

## 12. Scenarios and stress profiles

A scenario is a named config file. The same scenarios drive the demo, the grid and the benchmark.

| Scenario | What happens | Shows |
| --- | --- | --- |
| Calm weekday | Defaults, no jumps for the first minutes | The baseline spread and fill delay |
| News jump | One 20-tick jump | A single race at 1/100 speed |
| Monday open | Friday close, a hidden weekend, the reopen jump | The largest transfer of the week |
| Cancel storm | LPs requote on every tick; most messages are cancels | The cancel path and its index |
| Order flood | Regular-trader rate ×10, then ×100 | Throughput and p99 under load |
| Deep book | Thousands of resting price levels | The price-level structure |
| Sweep | Large orders that walk many levels | The matching loop |

In live mode the dashboard can switch the ordering rule and the load multiplier mid-run. Each switch is a logged config event, so the run still replays exactly.

## 13. Validation and experiments

No number reaches a slide until the engine passes these checks, in order.

1. **Invariants (proptest).** The book is never crossed after a message is processed. Quantity is conserved. Total PnL is zero. The same seed gives the same hash.
2. **Hand-written logs.** About 20 short logs with fills worked out by hand: partial fills across levels, IOC remainders, a cancel racing a fill, self-trade, and each ordering rule sorting the same block.
3. **Budish check.** Under first come, first served with N equally fast snipers and one LP, the LP wins about 1 race in N+1. Shrinking every latency in proportion must not change the sniper take. Under cancel priority with every message in one block, the LP wins nearly every race. If any check fails, find the bug before going further.

**Experiment grid**

| Factor | Levels |
| --- | --- |
| Ordering rule | first come first served, PGA, cancel priority |
| Block length B | 0, 50 ms, 250 ms, 1 s |
| Speed gap (sniper latency ÷ LP latency) | 0.1, 0.5, 1.0 |
| Number of snipers | 1, 3, 10 |
| Regime | weekday hour; Friday close into the Monday open |

That's 216 cells. Under first come, first served, the B levels differ only in fill delay. Start with 200 seeds per cell and adjust after a pilot measures the variance. All ordering rules run on the same seeds, so we compare paired differences. Report the mean with a 95% bootstrap interval.

## 14. Dashboard and talk flow

The dashboard and the slides use the visual identity of rosalbito.dev: background #050507, ink #edebe6, hairlines #2a2a32, one violet accent #7b61ff, Geist and Geist Mono, light headings, small uppercase mono labels, 2px corners.

**Dashboard panels**

| Panel | Shows |
| --- | --- |
| Order book | The ladder, bids and asks, updated about 30× a second |
| Price | Nasdaq price against the book's mid. While Nasdaq is hidden, the line goes dark with a violet rim (an eclipse) |
| Races | Each race as it ends: who won and by how many ms. Every snipe flashes once in violet |
| Accounts | PnL for the five account classes |
| Engine | Messages/s, p99, ring fill level |
| Controls | Scenario, ordering rule, playback speed (1×, 1/100×), load multiplier |

**Talk flow**

1. **Two layers (slide).** The sequencer orders; the engine matches. CLOB, FBA, PGA and cancel priority placed on that picture, with who keeps the stale-quote prize under each.
2. **The race (demo).** A news jump at 1/100 speed: the sniper's order lands before the LP's cancel.
3. **Flip the rule (demo).** Same seed under PGA, then cancel priority. Watch the race panel and the accounts.
4. **Monday open (demo).** The weekend eclipse, then the reopen jump.
5. **Stress (demo).** Order flood ×10 and ×100; p99 holds.
6. **The latency ladder (slide).** Where FPGAs compete versus where on-chain sits.
7. **Results (slide).** The main chart from the full grid, plus the LMAX lock-cost table as the reason for one writer.

Backup: a pre-recorded video of steps 2 to 5.

## 15. Milestones and gates

Each phase ends at a gate, and no result reaches the slides before the checks under it pass.

| Phase | Delivers | Gate |
| --- | --- | --- |
| 1. Engine | `engine` crate, input-log format, determinism rules | Invariants and hand-written logs pass; golden hashes in CI |
| 2. Simulation | Price feed, agents, sequencer with three rules, ledger | Zero-sum on every run; the Budish check passes |
| 3. Measurement | Metrics, grid runner, the main chart | Confidence intervals narrow enough to rank the rules |
| 4. Live and benchmark | `bench`, `server`, dashboard | Benchmark numbers stable across runs; dashboard replays a log exactly |
| 5. Talk | Slides, demo script, backup video | A full rehearsal on the talk laptop |

Phases 3 and 4 can overlap once the log format is frozen in phase 1. If time runs short, cut from the grid (fewer cells) before cutting the live demo.

## 16. Open questions

- [ ] When is the talk, and how long is the slot?
- [ ] Robinhood Chain's block time. It sets the realistic B.
- [ ] Arbitrum PGA's exact rules: is order inside a block by fee only, and how are ties broken? Check the AIP before modelling.
- [ ] Does Hyperliquid really process cancels first within a block?
- [ ] Which historical Friday-close to Monday-open gaps to use for the weekend volatility.
- [ ] Is 1/100 speed enough for an audience to follow a race, or does the dashboard need a step-through mode?

**Related docs:** [mental model](https://claude.ai/artifact/S428jat3JZ2sjrZwnNKxim) · [architecture](https://claude.ai/artifact/6mgaTvdBXsjjhGafLUjxSi)

**Sources**

- [Budish, Cramton and Shim (2015), The High-Frequency Trading Arms Race](https://academic.oup.com/qje/article/130/4/1547/1916146)
- [Robinhood Chain: differences from Ethereum (first-come, first-served sequencing)](https://docs.robinhood.com/chain/differences-from-ethereum)
- [Arbitrum forum: AIP to move Arbitrum One to Priority Gas Auctions](https://forum.arbitrum.foundation/t/constitutional-aip-transition-arbitrum-one-ordering-policy-to-priority-gas-auctions-pga/30942)
- [CoinDesk: Robinhood Chain weighing payment-based ordering (Oct 9, 2026)](https://www.coindesk.com/business/2026/10/09/robinhood-chain-considers-technology-that-gives-paying-traders-priority)
- [LMAX Disruptor technical paper](https://lmax-exchange.github.io/disruptor/disruptor.html)
- [Martin Fowler, The LMAX Architecture](https://martinfowler.com/articles/lmax.html)
