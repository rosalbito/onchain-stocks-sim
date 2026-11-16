# Decisions

Choices made where SPEC.md was ambiguous or silent, one line of reasoning each. Entries marked
**Spec issue** are places where the spec looks wrong or contradicts itself; they are recorded
here, not silently changed.

## Engine and log format

1. **Order id = the `seq` of the `New` record.** The spec's record has no order-id field for `New`, and `seq` is already unique and deterministic.
2. **The engine uses `agent` for self-trade prevention and cancel ownership, never for priority.** Section 5 requires cancelling the resting order on a self-trade, which needs the owner; a cancel from a non-owner is a reported no-op.
3. **Extra events: `Accepted` per `New`, and `Rejected` for malformed records** (zero quantity, price ≤ 0, `seq` not increasing). Agents need `Accepted` to learn their order ids; rejecting beats panicking on bad input.
4. **A cancel for an order that isn't resting gives one `CancelMissed` event**, without saying whether it was filled, cancelled or never existed. Simplest reading of "a no-op that is reported".
5. **`BookChange` is emitted after every level change, with the level's new total**, not coalesced per message. "An event on every book change", literally.
6. **`Cancel{order_id, priority_fee}` (spec rev 36), and LP β ≠ 0 is allowed.** The fee is in the log (format version 2), PGA sorts cancels by it like any message, and the sender pays it to the chain when the cancel is included, whether or not it finds the order. Under PGA the LP bids β × exposure on each cancel, where exposure = max(0, staleness against the LP's last seen price) × remaining lots: what a sniper could take from that quote, mirroring the sniper's α × expected prize. With the default β = 0 nothing changes: every golden *fills* hash is unchanged; all 36 *log* hashes changed, because the format version and the new field are part of the encoded log.
7. **The input log on disk is postcard of `{format_version, scenario, seed, config_toml, records}`.** The engine has no serde, so `sim::log` mirrors the record type. Embedding the config makes a log self-describing, as the architecture asks.

## Money and the ledger

8. **Cash unit = 1/10,000 of a tick·lot ($0.000001 with a $0.01 tick and 1-share lots).** A fee in whole bps of notional is then exactly `px × qty × bps`, with no rounding. `priority_fee` uses the same unit.
9. **The taker pays the taker fee on each fill. The sender pays its priority fee when the message is included, filled or not** (section 9). Agents only bid when the rule is PGA.
10. **Accounts start at 10^18 cash units and 10^9 lots. PnL is marked to the true Nasdaq price at the end of the run**, even if it ends while the price is hidden. Section 9 says "marked to the Nasdaq price"; the markout horizons in section 10 are phase 3.
11. **The zero-sum check holds by construction.** It catches ledger bugs (for example, a fee debited but never credited, which a unit test shows) but cannot catch a wrong fill from the engine. It is a bookkeeping gate, not a correctness proof.

## Randomness and determinism

12. **Each agent's RNG is `ChaCha8Rng::seed_from_u64(master)` with `set_stream((id << 8) | purpose)`.** Latency and decisions use separate purposes, and the feed is id 0 with its own diffusion and jump streams. This follows section 4 and keeps outside inputs on their own streams.
13. **Floats appear only in the sim's samplers, through `libm` (pure Rust). `rand_distr` is not used.** `std`'s `ln`/`exp` (which `rand_distr` calls) go to the platform's math library, and results can differ in the last bit between machines. Every draw becomes integer ns or lots immediately, and the engine, ledger and metrics have no floats.
14. **"20 ± 5 ms" means uniform in [15, 25] ms, drawn in whole ns.** It is the simplest bounded jitter.
15. **One send-latency draw per reaction: all messages from one reaction arrive together, in order.** Otherwise an LP's cancels and new quotes could interleave with other traffic.
16. **Each agent's see and send channels are FIFO: a delivery is never earlier than the previous one.** One connection per agent, and it stops an agent seeing an older price after a newer one.

## Price feed and calendar

17. **Diffusion is ±1-tick steps arriving as a Poisson process at 1/s** ("about 1 tick per second"). Jumps are Poisson at 1/300 s, sized uniformly from 5 to 20 ticks, with a random sign.
18. **The hidden weekend has diffusion only (no jumps) at 0.1 steps/s, so the gap's standard deviation is about 150 ticks (1.5%) over 65.5 h.** Section 16 leaves weekend volatility open, and 1.5% is a typical single-stock weekend gap.
19. **Piecewise rates (weekday/weekend, "jumps start after") are sampled exactly**: a draw that crosses a boundary restarts there, which is valid because the process is memoryless.
20. **A visible `Tick` at t = 0 carries the start price**, so agents have something to quote around.
21. **Scenario timing.** Calm weekday: 1 h, with "no jumps for the first minutes" taken as 10 min. News jump: 120 s, diffusion on, one +20-tick jump at 60 s. Monday open: t = 0 is Friday 15:00 ET, then 1 h visible, 65.5 h hidden and 30 min of Monday.

## Sequencer

22. **With B = 0, each message is sequenced on arrival and the block number counts messages.** "Every message is its own block."
23. **`BlockClose` is scheduled when the first message enters a block**, not every B. The semantics are the same, and it avoids millions of empty events over a 67 h run.
24. **A tick's `block` field is the block open at its time (t / B), or the current counter when B = 0.** Ticks never wait for a block (section 6).
25. **Messages still in an open block when the run ends are never sequenced.** They are counted as `unsequenced` in the summary.

## Agents

26. **The LP learns its order ids from the `Accepted` events it sees.** On a requote it cancels every order it knows about and marks unacknowledged orders obsolete, then cancels those when their acks arrive. Ids are `seq`, so it cannot cancel an order before it has seen the ack.
27. **The LP re-posts a filled level of its current quote at the same price.** The spec doesn't say. Without refills, the weekend book drains within minutes, because no ticks arrive to trigger a requote.
28. **The inventory cap is checked against inventory from fills the LP has seen.** A side is skipped if filling all of it would push inventory past ±cap.
29. **The LP learns of the close from the calendar, without latency, and widens ×3 by scaling every offset.** On the first visible tick after the reopen it requotes at normal width, even if the price moved less than 1 tick.
30. **Snipers act only when they see a price**, as section 8 states, using their own late view of the book built from `BookChange` events. They send one IOC per stale side, sized to all the stale quantity. The expected prize is Σ (seen − px) × qty, and the PGA bid is α of it, with α in bps (5000 = 0.5).
31. **Regular traders read the live book when they decide.** The spec gives them no see-latency. They skip an arrival if the opposite side is empty, but still make every draw so their streams stay the same under every rule. Size is lognormal with median 2 and σ = 1 (σ unspecified), rounded and capped at 50. There is one regular-trader agent at 0.5/s.

## Metrics

32. **Races are counted per LP order.** A race starts at a logged `Tick` that leaves the order stale by ≥ θ. It ends as an LP win on the LP's own cancel (or a self-trade cancel), a sniper win on a sniper fill, or "other" on a regular trader's fill. A partial sniper fill ends the race, and the remainder can start a new one.
33. **LP race win rate = LP wins ÷ resolved races (LP + sniper + other); races still open at the end are excluded.** "All races" can't include races with no outcome.
34. **The summary adds each class's gain on its fills against the true price at the moment of the fill** (a horizon-0 markout, before fees). It isolates the stale-quote prize from inventory noise. Proper markouts at 1/10/60 s are phase 3.

## Gates

35. **Budish setup: races in isolation.** One LP and N snipers, jumps only (0.2/s, no diffusion), no regular traders, no inventory cap, LP latency equal to the snipers', 8 seeds × 1 h each. With diffusion on, two quick steps give the LP a head start (its first cancel is already in flight), which is a real effect but not the one the check is about.
36. **Budish tolerance: 4 binomial standard errors on episodes** (jumps), not races. All races in one episode go to whoever is sequenced first, so episodes are the independent trials.
37. **"Every message in one block" means B = 1 s.** That puts each race's messages, a few ms apart, in one block, except when they straddle a block boundary; the threshold is ≥ 95%. A B as long as the run would never execute anything.
38. **Golden hashes are an insta snapshot of the log and fills sha256** per scenario × rule × B ∈ {0, 250 ms} × seeds {1, 2}. There is a CI workflow file, but no remote exists, so it has not run in CI.
39. **"Shrinking every latency in proportion must not change the sniper take" holds only when races don't overlap price moves; spec rev 36 now scopes it to isolated races.** The gate test is `budish_isolated_races_latency_scaling_keeps_sniper_take`. In the isolated setup, ×1/2 and ×1/10 change the sniper take by +0.30% and −0.07%, which passes. In calm weekday, ×1/10 lowers it about 10%, because diffusion runs on an unscaled clock. The check therefore runs on the isolated setup and measures take at fill time; the end-of-run mark varied ±7% from inventory alone.
40. **Spec issue (resolved by spec rev 36): with B > 0, a quote whose cancel was already in flight counted as a race the LP "won".** A cancel only takes effect at block close, so the next tick made the quote stale and started a race the cancel then "won": in calm weekday under FCFS the LP race win rate went 0.21 → 0.55 → 0.84 → 0.93 for B = 0, 50, 250, 1000 ms (20 seeds). Section 10 now says such a quote doesn't start a race. `RaceTracker` skips a quote at a tick if a cancel for it reached the sequencer strictly before the tick's time (the sim reports arrivals when the sequencer accepts them; a replay can read them from the `Cancel` records' `t_ns`, which is equivalent). After the fix the rate is 0.212, 0.201, 0.213, 0.216: flat in B. Check: `cargo run --release -p sim --example fcfs_block_check -- 20`.
41. **With B = 0, the three rules order messages identically** (each block holds one message). PGA then differs only in fees paid, and cancel priority equals FCFS; the golden fills hashes show this.
42. **Spec issue: under FCFS, B still changes the sniper take at 1 s, and the race count misses why.** After entry 40, the LP win rate is flat in B, but in calm weekday (20 seeds) the snipers' take at fill time is $20.66, $20.65, $21.67 and $34.92 per hour for B = 0, 50, 250, 1000 ms (+0%, +5%, +69%), on 418, 434, 552 and 1,785 filled lots. Of the 1,785 lots at 1 s, 1,333 hit an LP quote that was already stale when it entered the book (vs 1.3 at B = 0), and 1,450 hit a quote that entered the book in the same block as the sniper's order. Mechanism: the LP posts quotes around the price it saw, they wait up to B for their block, and the price moves meanwhile (about 1 tick/s); at block close they rest already stale, and a sniper IOC later in the same block (limit = seen − θ, which sweeps every ask at or below it, including quotes the sniper never saw) takes them. The LP cannot cancel them in that block, because an order id is its `seq` (entry 1) and the LP learns it only from the ack after the block seals (entry 26), so a fresh quote is exposed for at least one block. No price move makes a resting quote stale, so these fills are not races under section 10; they still count in sniper take. This is a genuine effect of blocks in this model, not a counting bug: B lengthens the LP's effective reaction time for new quotes. It would shrink if orders carried client-chosen ids cancellable in the same block. The grid keeps the spec's definitions; the report and the chart show the FCFS line moving at B = 1 s. Check: `cargo run --release -p sim --example fcfs_block_diag -- 20`.
