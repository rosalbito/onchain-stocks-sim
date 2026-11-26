# Architecture decisions

The decisions that shape the system, one short record each: the context, what we chose, what we turned down, and what it costs. Numbers are from an Apple M3 (4 performance + 4 efficiency cores), release build; the commands to reproduce them are in the README. The full history is in [DECISIONS.md](DECISIONS.md) (entries cited as D-n).

---

## ADR-1 · One thread owns the order book

**Context.** Matching is sequential: message 42 can only be matched after 41, because 41 may have changed the book. Parallel threads on one book would have to take turns through a lock.

**Decision.** Exactly one thread, the engine thread, ever writes the book. Everything else (logging, the standby, the UI, the network) runs on other threads and talks to it through rings (ADR-3).

**Rejected.**
- A mutex around a shared book: in the LMAX Disruptor paper, one counter incremented 500M times went from 300 ms on one thread to 224,000 ms with two threads and a lock (750×).
- Sharding one book across threads: price-time priority needs a single order.

**Consequences.**
- One book's throughput is capped by one core: 5.3–5.7M msgs/s here (D-74). That is far above what one stock's book needs.
- Scaling means more books, not more cores per book (ADR-5).
- No locks on the hot path means no lock contention and no priority inversion.

---

## ADR-2 · Deterministic engine and an input log as the source of truth

**Context.** We run 43,200 simulations and need every result to be reproducible. Live, we need to prove that nothing was lost.

**Decision.**
- The engine never reads a clock, uses floats, or iterates a hash map. The market runs on a virtual clock.
- The log stores the input (every message the sequencer numbered, plus control changes), not the output. The book is `replay(log)`.
- After each live run, the log is read back from disk, replayed through a fresh engine, and the fills hash must match. The standby does the same check live.

**Rejected.**
- Logging the fills: a log of fills is larger, and it can't show that the engine would make them again.
- Wall-clock time inside the simulation: results would change from run to run.

**Consequences.**
- Same seed, same bytes: rerunning the headline cell reproduces `analysis/out/headline_b250.csv` byte for byte.
- The largest stress run checked: 141M records (3.07 GB) were replayed and gave the same fills hash (D-74).
- Durability is per second (the writer fsyncs once a second), not per message. That is acceptable for a demo but not for production.

---

## ADR-3 · The book: B-tree of price levels, FIFO queue per level, orders in a slab

**Context.** The book must find the best price, keep time priority within a price, and cancel any order by id. All of it must be deterministic and must not allocate on every message.

**Decision.**
- Per side: `BTreeMap<price, Level>`. A `Level` is a `VecDeque` of slab keys (oldest first) plus its total quantity.
- Orders live in a `Slab<Order>`: one growable array whose freed slots are reused.
- Cancels use an index `BTreeMap<OrderId, slab key>`.
- Up to 256 emptied level queues are kept and reused when a new price level opens.
- `on_message_into` fills a buffer the caller reuses, instead of returning a new `Vec`.

**Rejected.**
- A `HashMap` index: iterating it is not deterministic (the spec forbids it).
- A fixed array indexed by tick: usually faster, but more code and a fixed price range. The spec says to switch only if a benchmark demands it, and none did.
- A new allocation per order or per level: measured at 1.3 allocations per message before the change.

**Consequences.**
- Matching costs 21–38 ns per message on recorded flows (D-68).
- Allocations per message went from 1.25–1.38 to 0.001–0.16 (D-62). What remains comes from B-tree node splits and slab growth when resting orders pile up.
- The whole process uses about 31–33 MB at 1M msgs/s and flat out (D-74).

---

## ADR-4 · Threads hand off through bounded single-producer rings (`rtrb`); each reader either waits or drops

**Context.** The engine must pass every record to the log writer and the standby, plus a frame to the UI, without slowing down.

**Decision.**
- Every hot-path hand-off is a lock-free single-producer, single-consumer ring (`rtrb`): a fixed array with one writer and one reader, each moving only its own counter.
- Sizes: input, log and standby rings hold 64k slots; the UI ring holds 64 frames; the control ring holds 256 commands.
- **Wait vs drop, per reader:** the log and the standby must see everything, so when they're full the engine waits. The UI drops frames.
- Idle readers spin briefly, then sleep 50 µs.
- The web edge, where microseconds don't matter, uses ordinary tokio channels.

**Measured** (1.93M recorded messages; delay is the median send-to-matched time at 100k msgs/s; D-68):

| Hand-off | msgs/s, flat out | Delay |
| --- | ---: | ---: |
| Direct call (no other thread) | 29.0M | 83 ns |
| **rtrb ring** | **23.7M** | **125 ns** |
| std `sync_channel` | 18.4M | 5.1 µs |
| crossbeam bounded | 16.1M | 5.2 µs |
| tokio mpsc | 8.7M | 6.5 µs |
| `Mutex<VecDeque>` | 7.0M | 0.2 µs, tail to ms |

**Rejected.**
- Channels: their reader goes to sleep and the OS must wake it, which adds microseconds and millisecond tails on a busy machine.
- A mutex: fast in the typical case, but a descheduled lock holder stalls everyone.
- A direct call: fastest alone, but then one thread would do all three jobs in a row. At 5.5M msgs/s the engine thread spends ~180 ns a message, and the log writer and the standby keep their own cores 70–100% busy (~150 and ~170 ns a message). In a row that is ~500 ns, or about 2M msgs/s instead of 5.5M. This is arithmetic from measured CPU use, not a separate run. A ring moves ~320 ns of work off the engine's core for ~40 ns.

**Costs.**
- **Memory up front:** a ring is allocated at full size. Ours total about 15 MB (log 5.8, standby 5.2, input 3.7, UI 0.7), roughly half the process.
- **One reader per ring:** fan-out means one ring per reader, so every record is copied into two rings.
- **Bounded:** every reader needs an explicit full-ring policy, and a slow log writer slows the engine. That is deliberate.
- **Idle latency:** a sleeping reader adds up to 50 µs when traffic is light. Pure spinning would burn a core.
- **Upkeep:** a third-party crate with `unsafe` inside, and a less familiar pattern than channels. So we use it only on the hot path.

---

## ADR-5 · Cores: keep the engine on a fast core; scale by adding books

**Context.** On a laptop the OS moves threads between cores. The M3's efficiency cores run at a lower clock with smaller caches, and a moved thread starts with cold caches.

**Decision.**
- On macOS, which has no thread affinity, the engine and standby threads request user-interactive QoS, which keeps them on performance cores.
- In production on Linux: pin the engine thread to a core, take that core away from the scheduler (`isolcpus`, `nohz_full`), route interrupts elsewhere, and use real-time priority.

**Measured** (not a controlled test; D-68):
- With other programs using about 4 cores' worth of CPU, the engine's cost went from 35 to 75 ns/msg without QoS, and changed by under 1% with it.
- With about 7.4 cores' worth, it reached 72–84 ns/msg even with QoS. Priority is a request, not a reservation.

**Consequences: how to deploy.**
- One book needs one dedicated core for its engine thread. Its standby and log writer each need most of a core flat out, but are nearly idle at real-market rates.
- On bigger machines, run many books, each engine on its own isolated core, with the helpers and the OS on the rest.
- A second machine is for availability, not speed: run each book's standby there, fed the same log.
- In front of the books: gateways take connections, check orders and route each one **by ticker**. A load balancer may spread traffic over the gateways, which hold no state, but never over books: every order for a stock must reach that stock's one engine thread, or there is no single order of events. Each hop costs microseconds inside one region, small next to a 250 ms block, so add it only when needed.
- The sequencer needs no separate scaling: each book's sequencer runs on that book's engine thread. On a chain, the chain's own sequencer orders every book's transactions; that shared step is the chain's limit, not the engine's.

**Open.** Redo the core measurement as a controlled test: a fixed number of CPU burners, with and without QoS.

---

## ADR-6 · Fixed-size frames from the engine, JSON at the edge

**Context.** The dashboard needs the book, charts, races and stats 30 times a second. Building JSON on the engine thread would allocate and cost time on every frame.

**Decision.**
- The engine thread fills a fixed-size `Frame`: about 10.9 KB of plain arrays (20 book levels, 60 candles, 16 trades, the race zoom, counters).
- The UI bridge thread turns the newest frame into JSON.
- The web edge pushes snapshots on a tokio watch channel, which holds only the latest value.

**Rejected.**
- Serializing on the engine thread: it would allocate on the hot path.
- Streaming every record to the browser: millions a second would be unreadable, and the UI would set the engine's pace.

**Consequences.**
- The UI can never slow the engine: its ring drops.
- Detail is bounded by the frame's fixed sizes, for example 20 levels of depth and 16 trades.
