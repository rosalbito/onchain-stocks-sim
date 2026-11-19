# Architecture: Simulation, Engine and Benchmark

Nov 14, 2026 · @Roberto carvalho

One single-threaded matching engine, driven by two harnesses: a simulation on a virtual clock for the economics, and a benchmark on the wall clock for performance. The input log is the source of truth. Everything else, including the in-memory book, can be rebuilt from it.

## 1. At a glance

&#91;embedded content: system at a glance · 2 harnesses, 1 engine, 3 readers\]

Either harness writes the input log. The engine thread is the only thing that changes the book. Its numbered events fan out to readers that never write back. Recovery is the newest snapshot plus a replay of the log after it.

## 2. Two harnesses, two clocks

The economics and the performance are separate questions, so each gets its own harness around the same engine crate. Timing code never enters the simulation. If it did, the same seed would give different results on a fast laptop and a slow one.

|  | Simulation | Benchmark |
| --- | --- | --- |
| Question | Who loses what to whom? | How much load can the engine take, and how fast does it answer? |
| Clock | **Virtual.** Latency is a number we add to timestamps. | **Wall clock.** Real nanoseconds on real hardware. |
| Input | Agents generating realistic flow, about 5–10k messages per simulated hour | Recorded simulation logs replayed at full speed, plus synthetic stress profiles |
| Concurrency | None: one loop, direct function calls | Real threads and channels |
| Output | bps lost per account, race win rates, the talk's chart | messages/s and p50/p99/p99.9 latency |
| Deterministic? | Yes, byte for byte per seed | No: results vary run to run, so we report distributions |

The engine has no idea which harness is driving it. It exposes `on_message(msg) -> events`, and that's all either harness calls. It runs one mechanism only: the continuous order book (CLOB).

## 3. The event loop and simulated latency

The simulation is a to-do list sorted by time. The loop takes the earliest item, handles it, and handling it can add new items further in the future. Latency is how far into the future an item lands. No sockets, no sleeping, no toxiproxy.

&#91;embedded content: simulation data flow · price feed to dashboard\]

Agents see each price late and their orders arrive late. The sequencer turns those arrivals into ordered blocks. Price ticks go into the same log as the orders, so the dashboard gets the Nasdaq price and the book from one numbered stream, and the two can never be shown out of step.

```rust
while let Some(ev) = queue.pop() {              // earliest (time, tiebreak) first
    match ev.kind {
        PriceMove(p)        => for a in agents { queue.push(ev.t + a.see_latency(), Observe(a, p)) }
        Observe(agent, p)   => for o in agent.react(p) { queue.push(ev.t + agent.send_latency(), Submit(o)) }
        Submit(order)       => sequencer.accept(order)            // held until the block closes
        BlockClose          => for o in sequencer.order_block() { // the ordering rule decides who goes first
                                   let out = engine.on_message(o); // order book matches now
                                   ledger.apply(&out); metrics.record(&out);
                               }
        MarketData(agent,d) => agent.update(d)                    // fills and book changes, seen late
    }
}
```

- **The queue** is a binary heap keyed by `(time, tiebreak)`. The tiebreak is a counter that increases on every push, so two events at the same nanosecond always come out in the same order.
- **The simulated sequencer** collects the orders that arrive during one block, then hands them to the engine in the order the **ordering rule** sets. The rule is a config switch, and it is the main experiment:
  - **First come, first served:** by arrival time. Robinhood Chain's documented policy.
  - **Priority gas auction:** highest priority fee first. Arbitrum One switched to this in Sept 2026, and Robinhood Chain is reportedly evaluating it. Exact tie-break rules still to verify.
  - **Cancel priority:** cancels go first, then everything else by arrival time.

  Block time is a parameter. Robinhood Chain's real block time is still an open question.
- **Latency** is drawn per agent from a seeded random generator, as a base plus jitter: e.g. sniper 5 ms ± 1, LP 20 ms ± 5. Each agent has two latencies: how late it *sees* things, and how late its order *arrives*.
- **Speed.** A simulated hour (5–10k messages) runs in milliseconds, so the whole experiment grid takes minutes, not days.

**How a block forms.** The block length B is a setting, for example 100 ms.

1. Every order the sequencer receives between time kB and (k+1)B goes into block k, tagged with its arrival time.
2. At (k+1)B a `BlockClose` event fires. The sequencer seals the block and sorts it by the ordering rule. Equal keys fall back to arrival time, then to the tiebreak counter.
3. The sorted messages get the next `seq` numbers, are appended to the input log, and go to the engine one by one.
4. The engine matches each one in turn. An order that arrives after the close waits for the next block.

The race from the mental model doc, inside one block from 0 to 100 ms. The sniper's buy arrives at 8 ms; the LP's cancel arrives at 23 ms.

| Ordering rule | Sorted by | Goes first | Result |
| --- | --- | --- | --- |
| First come, first served | arrival time | the sniper (8 ms) | the sniper buys 10 at 100.02; the LP loses $0.80 |
| Priority gas auction | priority fee, highest first | whoever bid more | most of the $0.80 goes to fees; the LP still loses unless it outbids |
| Cancel priority | cancels first, then arrival | the LP's cancel | the sniper finds no ask at 100.02; nobody loses |

Under first come, first served the block only packages the order and never changes it, so B doesn't affect who wins. Under the other two rules the block is the window inside which the order can change. There B is a real knob: a longer block protects more cancels but delays every trade.

**Why no toxiproxy.** We don't assume the traders share a network. We assume each trader's network can be summed up as two delays: how late it sees a price, and how long its order takes to reach the sequencer. Distance, routing and processing time all fold into those two numbers.

| Real-world effect | How the model covers it |
| --- | --- |
| Distance to the sequencer | A base latency per agent, e.g. sniper 5 ms, LP 20 ms |
| Jitter | A seeded random spread around the base |
| Rare slow messages | A mixture, e.g. 1% of messages take 10× longer |
| Congestion under load | Later: latency that grows with the number of messages already queued |
| Packet loss and resends | Left out; it looks like a rare slow message |

Toxiproxy wouldn't make this more realistic. It would add delays to traffic on one machine, so the numbers would still be ones we chose, but they'd vary between runs and break replays. Real numbers come from measuring real endpoints, which is the later server-location experiment. Those measurements become the base latencies here.

**Where everything runs in v1.** One Rust process on one machine (a laptop or the Mac mini) holds the simulation, the engine thread, the readers and the websocket server. The dashboard is a browser tab connected to that websocket. No cloud, no containers, no message broker. Several machines with real network delay between them is a later branch.

## 4. Why one thread owns the book

Matching is sequential by definition, so a second thread can't make it faster, only slower. The result depends on handling message 41 before message 42. Two threads on one book would need a lock, and then they'd just take turns.

What a lock adds on top of taking turns:

- **Handover cost.** Every time the lock changes hands, the book's data moves from one CPU core's cache to another. That's far slower than reading it from a cache it's already in.
- **Waiting.** A thread that finds the lock taken either spins or sleeps. Sleeping means the OS has to wake it later, which adds unpredictable delay.
- **Ordering bugs.** "Who got the lock first" is decided by the OS scheduler, not by the sequencer. Arrival order and processing order can then disagree.

With a single owner, the book stays in one core's cache, there's no lock, and processing order is exactly arrival order. Everything around the book (parsing, validation, fan-out to readers) can still run on other threads. They talk to the owner through queues (section 6). This is the *single-writer principle*, the design behind LMAX's exchange (Sources).

**Showing it on stage without building two engines.** The [LMAX Disruptor paper](https://lmax-exchange.github.io/disruptor/disruptor.html) (section 2.1) measured the cost directly: increment one 64-bit counter 500 million times.&#32;&#32;&#32;

| Method | Time (ms) | Slower than one thread |
| --- | --- | --- |
| Single thread | 300 | 1× |
| Single thread with lock | 10,000 | 33× |
| Two threads with lock | 224,000 | 747× |
| Single thread with CAS | 5,700 | 19× |
| Two threads with CAS | 30,000 | 100× |

Adding a lock and a second thread made the same work about 750 times slower, and the only shared thing was one counter. That table can go on a slide as it is.

**Our own version takes an afternoon.** About 50 lines of Rust with `criterion`: a small `BTreeMap` book updated by one thread, by one thread behind a `Mutex`, by 2, 4 and 8 threads behind a `Mutex`, and by one owner thread fed through a ring. The chart is throughput against thread count. `perf stat` next to it shows why: cache misses and context switches climb with the lock. The paper's numbers are from Java on 2011 hardware, so ours will differ; we expect the same shape, and the run will tell.

## 5. State, recovery and caches

The in-memory book isn't a cache. It's the state, and it can always be rebuilt by replaying the input log. Caches exist only on the read side, and each one carries the sequence number it reflects, so its staleness is always known exactly.

| Layer | What it is | If it's lost or wrong |
| --- | --- | --- |
| **Input log** | Every message the engine accepted, append-only, numbered `seq` 1, 2, 3… The source of truth. | Must not be lost. It's written before the engine processes the message. |
| **Engine's book** | `state = replay(log)`, held in memory | Load the last snapshot, then replay the log from that snapshot's `seq` |
| **Snapshots** | The book serialised every N messages, with its `seq` and a hash of the state | Fall back to an older snapshot and replay a bit more |
| **Hot standby** (later) | A second engine reading the same log. Determinism keeps it identical. | Already up to date: switch to it |
| **Read models** | The UI's copy of the book, counters, notifications, built from output events | Throw it away and rebuild from a snapshot plus events |

**The hot reload path** is `snapshot at seq N` + `replay N+1 … now`. Loading a snapshot takes milliseconds. The replay runs at engine speed, which is millions of messages per second, so even a few minutes of log replays almost instantly. A hot standby makes the reload zero.

**Why read-side caches can't break correctness:**

1. **Nothing that decides a trade reads a cache.** The engine uses only its own state. A wrong cache can show a wrong picture, but it can't cause a wrong fill.
2. **Staleness is a number, not a guess.** Every read model knows it reflects `seq = 41 230`. If it sees 41 231 followed by 41 233, it knows it missed one and resyncs from a snapshot. Compare a typical Redis cache in front of a database, where you don't know how stale an entry is.
3. **Drift is detectable.** Determinism means any copy at `seq` N must have the same state hash as the engine at N. Comparing hashes now and then catches bugs instead of letting them hide.

For v1 most of this is cheap or free. The log is a file. Snapshots and hashes double as test fixtures: they're the golden hashes in CI. Hot standby waits until we actually run the engine live.

**Is the log enough on its own?** Yes, on one condition: the engine is deterministic. Then the log plus the code version is the whole state. So the log records every message that changes the book, plus the config and code version it was made with. Price ticks are logged too. The engine doesn't need them, but a replay can then redraw the dashboard exactly.

| What goes wrong | What happens | Protection |
| --- | --- | --- |
| The process crashes mid-write | The last record is half written | Each record carries a checksum, and a bad tail is cut off on restart. That message was never confirmed to anyone, so nobody thinks it traded. |
| The disk or machine dies | The log on it is gone | Copy the log to a second machine before confirming, as LMAX does. The hot standby is that second machine. Not needed for v1. |
| The log writer falls behind | The disk is slower than the engine | Sync to disk once per block, not once per message. If it still falls behind, the ring fills and intake waits. The log itself can't go stale: a message counts only once it's logged. |
| RAM fills up | Resting orders are what grows | Cap orders per account and per book, and reject past the cap. Rings are fixed size and never grow. |
| The log file grows forever | It fills disk, not RAM | Split it into segments. Once a snapshot covers a segment, the segment can be archived. |

Sizes for v1: a simulated day is about 240k messages (10k an hour). At 64 bytes each that's about 15 MB, so none of this bites in the demo. It matters only once the engine runs live.

## 6. Ring buffers and channels

A single-producer, single-consumer (SPSC) ring buffer is a fixed array plus two counters, with no locks. It's the fastest way to hand messages to the engine thread. Tokio channels cost more per message but let a waiting task sleep instead of burning a core, so they belong at the I/O edges.

**How the ring works.** The array's capacity is a power of two. The producer owns `head` and the consumer owns `tail`, and each side only ever *writes* its own counter:

```rust
// producer
let h = head.load(Relaxed);
if h - tail.load(Acquire) == CAP { return Full }         // backpressure
slots[h & (CAP - 1)] = msg;                             // write the slot
head.store(h + 1, Release);                             // publish it

// consumer
let t = tail.load(Relaxed);
if t == head.load(Acquire) { return Empty }
let msg = slots[t & (CAP - 1)];                         // read the slot
tail.store(t + 1, Release);                             // free it
```

`Release` on the store and `Acquire` on the load guarantee the consumer never sees the new `head` before the slot's contents. No thread ever waits on another thread's lock, so it's *wait-free*.

**The ring, step by step.** Picture 8 slots; a real ring has thousands. Two counters only ever go up: `head` counts messages written, `tail` counts messages read. The slot for counter n is `n & 7`, which is n mod 8, and cheap because 8 is a power of two.

&#91;embedded content: an 8-slot ring · 5 written, 2 read, 3 waiting\]

The producer, line by line:

1. `let h = head.load(Relaxed)`: read my own counter. Only I write it, so no ordering is needed.
2. `if h - tail.load(Acquire) == CAP`: how many are waiting? If all 8 slots are taken, return `Full` and the caller waits. That's backpressure.
3. `slots[h & (CAP - 1)] = msg`: write the message into slot h mod 8.
4. `head.store(h + 1, Release)`: publish it. From this moment the consumer can see it.

The consumer is the mirror image:

1. `let t = tail.load(Relaxed)`: read my own counter.
2. `if t == head.load(Acquire)`: nothing new, so return `Empty`.
3. `let msg = slots[t & (CAP - 1)]`: read the message.
4. `tail.store(t + 1, Release)`: free the slot, so the producer may overwrite it.

**Why Acquire and Release exist at all.** CPUs and compilers reorder memory operations for speed. Without these two words, the consumer's core could see `head = 6` before the message bytes in slot 5 had arrived, and read garbage. Release on the producer's store and Acquire on the consumer's load are a promise between the two cores: the data first, then the counter. They cost almost nothing, and they replace a lock.

The counters never wrap in practice: a 64-bit counter at a billion messages a second takes about 580 years to overflow.

**The engineering details that make it fast:**

- **Padding against false sharing.** `head` and `tail` sit on separate cache lines (64 or 128 bytes, depending on the CPU). Otherwise every write by one side would invalidate the other side's cache.
- **Cached counters.** The producer keeps a local copy of `tail` and re-reads the shared one only when the buffer looks full. That saves a cross-core read on almost every push.
- **Batching.** The consumer drains everything available, then publishes `tail` once.
- **No allocation.** Slots are pre-allocated and messages are fixed-size `Copy` structs, so the hot path never calls the allocator.
- **Backpressure is a choice.** When the buffer is full, the input side *waits*, because orders must never be dropped. The UI side *drops* the stale update and keeps the newest one.
- **Core pinning.** The engine thread is pinned to its own core, so the OS never moves it and empties its cache.

**The waiting strategy** is what really separates the options. It's the same idea the LMAX Disruptor calls "wait strategies":

| Option | When the queue is empty | Wake-up cost | Where we use it |
| --- | --- | --- | --- |
| **SPSC ring, busy-spin** (`rtrb`) | Keeps checking. Burns 100% of one core. | Nanoseconds | Into the engine (hot path) |
| **crossbeam channel** | Spins briefly, then parks the thread | Microseconds (OS wake-up) | Benchmark comparison only |
| **tokio `mpsc`** | The task yields (`.await`) and the runtime wakes it later | Microseconds, plus scheduler jitter | Network edges: websocket, notifications |
| **tokio `watch`** | Same, but holds only the latest value | Same | UI state: older values are dropped automatically |

Your intuition about tokio is right. The receiving task pauses and the runtime resumes it later, which frees the core for other tasks. That's ideal for thousands of mostly idle connections, and too slow for the one hot loop. The benchmark (section 8) measures all four options so the talk can show the cost in numbers.

**Do we need crossbeam?** Not really. The engine's events go to several readers: ledger and metrics, the log writer, the snapshot writer and the UI bridge. An `rtrb` ring has exactly one reader, so the simplest fan-out is **one ring per reader**. The engine writes each event into 3 or 4 rings, which costs a few extra copies of a small struct, nanoseconds each.

The real question is how an idle reader waits. Busy-spinning burns a whole core per reader, which is wasteful for readers that aren't urgent. crossbeam's channel spins briefly and then parks the thread, a convenient way to wait without burning a core. A ring plus a short sleep when it's empty does the same job with one library fewer.

**v1 choice:** one `rtrb` ring per reader. Readers spin briefly, then sleep about 50 µs while the ring is empty. crossbeam stays only as a comparison point in the benchmark (section 8).

## 7. Scaling

You scale by splitting, not by coordinating: one book per core, and as many threads or machines as you like around the books. Very little needs coordinating across books.

| What grows | How it scales | Coordination needed |
| --- | --- | --- |
| **Number of stocks** | One single-threaded book per stock, each on its own core or machine. AAPL and TSLA never talk to each other. | None |
| **Order intake** (parsing, signature checks, validation) | Any number of threads or machines in front of the books | None: they only forward into each book's queue |
| **Market data and notifications** | Any number of readers off each book's event stream | None: readers never write |
| **One very busy stock** | Can't be split. One faster core, a tighter hot path, less work per message. | — |
| **A user's balance across several stocks** | A separate balance/risk service, or each book holds a pre-allocated slice of the balance | Yes, the only real case |

So "more machines" mostly means more *independent* machines. The hard limit is the busiest single book, which is why the benchmark measures one book's per-message cost so carefully.

**One very busy stock: nothing to split, a lot to shrink.** Price-time priority needs one sequence per book, so one book can't be shared across cores. The work per message can still shrink a lot:

1. **Move everything off the engine thread that doesn't need the book:** parsing, signature checks, balance pre-checks, encoding events. The engine only matches.
2. **A cheaper book:** a fixed array of price levels indexed by tick instead of a tree, orders in a pre-allocated pool, no allocation and no hashing on the hot path.
3. **Batches:** drain the ring in batches, so the cross-core reads are paid once per batch (section 6).
4. **The machine:** the fastest single core available, with the engine pinned to an isolated core.
5. **Less flow:** rate limits per account, minimum quote lifetimes or small cancel fees. These are market-design tools rather than engineering, and real venues use them.

On-chain the time budget is generous: the engine gets a whole block time, milliseconds at least, to match messages that take microseconds each.

Robinhood Chain's sequencer is the same pattern one level up: a single component that puts every message in one order. That's why the latency race exists there at all.

## 8. Benchmark plan

The benchmark answers two things: how much one book can handle, and what each layer around it costs. It runs the same engine crate as the simulation, fed from recorded logs and stress profiles.

**Metrics**

| Metric | What we measure |
| --- | --- |
| Throughput | messages/s sustained |
| Per-message latency | message in → events out: p50, p99, p99.9 |
| Block processing | time to order and match one block, at 100, 1k and 10k messages |
| Scaling | cost against book depth (price levels) |
| Memory | allocations per message, peak RSS |

**Load profiles**

| Profile | What it stresses |
| --- | --- |
| Recorded simulation flow, at 1×, 10×, 100× the agents | realistic mix |
| Monday burst: thousands of orders in the same millisecond or block | worst-case spike |
| Cancel-heavy: most messages are cancels, like real LP flow | the cancel index |
| Deep book: thousands of price levels | the price-level structure |
| Sweep: large orders that walk through many price levels | the matching loop |

**The layer comparison** (section 6), each as p50 / p99 / p99.9:

1. Engine alone: direct function calls
2. SPSC ring with a busy-spinning consumer
3. crossbeam channel (thread parks when idle)
4. tokio `mpsc` on a multi-threaded runtime

**Run rules:** release build, the engine thread pinned to a core, a warm-up before measuring, the same machine for every comparison. Latencies are recorded in an HdrHistogram, so tails are exact rather than averaged away. `criterion` covers micro-benchmarks of single operations, and `perf` plus flamegraphs explain any number that looks wrong.

**The slide this can produce:** if one book matches a 10k-message block in microseconds while blocks arrive milliseconds apart or more, the engine is not the bottleneck. Who gets filled first is decided by the ordering rule, not by computation.

## 9. Technology choices

Everything runs in one Rust process. Message brokers solve a problem we don't have.

| Need | Choice | Not this, because |
| --- | --- | --- |
| Hot path into the engine | `rtrb` SPSC ring, busy-spin | tokio: scheduler jitter on the one loop that must be steady |
| Fan-out to metrics and the log writer | One `rtrb` ring per reader; spin, then sleep | crossbeam: works, but one more library for the same job |
| UI and notifications | `tokio` + `axum` websocket, `tokio::sync::watch` | — |
| Durable log and replay | Our own append-only file, one fixed-size record per message | **Kafka / RabbitMQ:** they move messages *between machines* durably. Each hop adds roughly milliseconds, plus servers to run. Our log file already gives replay. |
| Book data structure | Start with `BTreeMap<price, VecDeque<order>>`, plus an id → location index for cancels | A fixed array indexed by tick is usually faster. We switch only if the benchmark says so. |
| Randomness | `rand_chacha`, one stream per agent | `thread_rng`: not reproducible |
| Latency histograms | `hdrhistogram` | plain averages hide the tail |
| Micro-benchmarks | `criterion` | — |
| Profiling | `perf`, `cargo flamegraph` | — |

**Crate layout:** `engine` (no dependencies, pure) · `sim` (event loop, agents, simulated sequencer) · `metrics` · `grid` (the section 13 experiment grid) · `bench` · `viz` (replay UI for the demo). Only `bench` and `viz` use threads or tokio; `grid` uses rayon across runs, never inside one.

**The grid crate** is the report layer (DECISIONS 51): it runs single-threaded, deterministic runs in parallel, converts the integer metrics to bps, dollars and shares, bootstraps the 95% intervals and writes parquet (`grid_runs`, `grid_summary`, `grid_pairs`) for `analysis/`. Under PGA the bids are grid levels: `--alpha-bps` (sniper bid, bps of the prize it sees), `--beta-bps` (LP cancel bid, bps of exposure) and `--alpha-split` (α is the snipers' combined bid, so each of N bids α / N). Every (α, β) pair is its own PGA arm; FCFS and cancel priority never bid and run once. Filters (`--regimes`, `--rules`, `--blocks-ms`, `--gaps`, `--snipers`) run a subset, which is how the bidding sweep in `analysis/data/pga_sweep` was made (`analysis/pga_sweep.py` tabulates it).

**Where Rust sits next to HFT stacks.** Top HFT firms run a different race from ours. They don't drop the operating system; they keep Linux but keep it out of the hot path. Rough orders of magnitude (general knowledge, not source-checked here):

| Setup | What it removes from the hot path | Rough time from market data in to order out |
| --- | --- | --- |
| Normal program on Linux (us) | Nothing | Tens of µs, mostly the OS network stack |
| Kernel bypass (DPDK, Solarflare Onload) | The OS network stack: the program reads the network card directly | A few µs |
| FPGA on the network card | The CPU: the trading logic is wired into the chip | Hundreds of ns |

OCaml is a different axis. It's the language Jane Street is known for. It has a garbage collector, and low latency comes from writing code that doesn't allocate, the same discipline as our hot path. The language matters less than not allocating, not locking and not sleeping.

On-chain we need none of this. An order spends milliseconds on the network to the sequencer, then waits for its block. Our engine handles a message in microseconds or less, so making it faster changes nothing about who wins. The ordering rule and the distance to the sequencer decide that. Kernel bypass and FPGAs pay off only where rivals compete at the microsecond level, as on Nasdaq. That contrast is a good slide: the latency ladder, with on-chain marked on it.

## Sources

- [Martin Fowler, The LMAX Architecture](https://martinfowler.com/articles/lmax.html): all business logic on a single thread, "6 million orders per second on a single thread"
- [LMAX Disruptor technical paper](https://lmax-exchange.github.io/disruptor/disruptor.html): ring buffers and wait strategies between threads
- [rtrb: a wait-free single-producer single-consumer ring buffer](https://docs.rs/rtrb/latest/rtrb/)
- [Robinhood Chain: first-come, first-served sequencing](https://docs.robinhood.com/chain/differences-from-ethereum)
- [Arbitrum forum: AIP to move Arbitrum One to Priority Gas Auctions](https://forum.arbitrum.foundation/t/constitutional-aip-transition-arbitrum-one-ordering-policy-to-priority-gas-auctions-pga/30942)
- [CoinDesk: Robinhood Chain weighing payment-based ordering (Oct 9, 2026)](https://www.coindesk.com/business/2026/10/09/robinhood-chain-considers-technology-that-gives-paying-traders-priority)
