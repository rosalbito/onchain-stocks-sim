# Stale Quotes: a code walk

**The sequencer decides the order. The engine matches.**
Real code from this repo, trimmed to fit. Each stop says where it lives.

---

## 0 · The map

```
                 Command                     LogMsg
  browser ⇄ web ────────▶ ┌ ENGINE THREAD ┐ ───────▶ log writer → disk
                          │  the market   │ StandbyMsg
                          │  + the Engine │ ───────▶ standby: replay + check
                          └───────────────┘ UiMsg
                                            ───────▶ UI bridge → browser
  every arrow is a ring: one writer, one reader, no locks
```

One thread owns the book. Everything else only reads.

---

## 1 · What travels: one enum per ring

**Rust:** enums carry data, and `match` must handle every case.

```rust
pub enum MsgKind {                      // what the market sends
    New { side: Side, px_ticks: i64, qty_lots: u64,
          tif: Tif, priority_fee: u64 },
    Cancel { order_id: OrderId, priority_fee: u64 },
    Tick { px_ticks: i64 },             // the Nasdaq price
    Config { param: u32, value: u64 },  // a live switch, logged
}

pub enum Command {                      // browser → engine thread
    Start(StartSpec), Control(Control), StressRate(u64),
    Skip, Restart, Stop, Shutdown,
}

pub enum LogMsg {                       // engine thread → log writer
    Start(Arc<RunInfo>),                // open a new file
    Record(LogRecord),                  // one numbered message
    End { run_id: u64, records: u64, fills_hash: [u8; 64] },
    Exit,
}

pub enum StandbyMsg { Reset(u64), Record(LogRecord), Checkpoint { .. }, Exit }
pub enum UiMsg { Frame(Frame), Exit }   // Frame: fixed-size, no heap
```
`crates/engine/src/record.rs:37` · `crates/server/src/engine_thread.rs:89`, `:116`, `:129`, `:294` · trimmed

Prices are integer ticks (`i64`), never floats: a replay gives the same bytes.

---

## 2 · Threads: "one writer" is checked by the compiler

**Rust:** ownership. A ring gives you two ends; neither can be copied
(`Send`, not `Clone`). Each end is **moved** into exactly one thread.

```rust
let (log_tx, log_rx) = rtrb::RingBuffer::new(LOG_RING);
let (sb_tx, sb_rx) = rtrb::RingBuffer::new(STANDBY_RING);

readers::spawn_log_writer(log_rx, ..);   // reader ends → their threads
readers::spawn_standby(sb_rx, ..);

pub struct Rings {                       // the engine thread owns
    pub cmd: Consumer<Command>,          // every writer end
    pub log: Producer<LogMsg>,
    pub standby: Producer<StandbyMsg>,
    pub ui: Producer<UiMsg>,
}

thread::Builder::new().name("engine".into())
    .spawn(move || { /* `rings` moved in: no one else can write */ })
```
`crates/server/src/lib.rs:65` · `engine_thread.rs:299`, `:463` · trimmed

The one place with many writers: every browser connection can send a
`Command`. The compiler wouldn't let us share a `Producer` without a lock:

```rust
control: Arc<Mutex<rtrb::Producer<Command>>>,  // the web side, not the hot path
```
`crates/server/src/lib.rs:52`

---

## 3 · Who owns the Engine

**Rust:** `&mut self` means exclusive access. The `Engine` is a plain
struct, with no `Arc` and no `Mutex`, owned by exactly one run.

```rust
enum Run {                       // the engine thread is in one mode at a time
    Idle,
    Market(Box<MarketRun>),      // MarketRun → Live → Sim { engine: Engine, .. }
    Stress(Box<StressRun>),      // StressRun { engine: Engine, input: Consumer<..>, .. }
}

impl Engine {
    pub fn on_message_into(&mut self, msg: &LogRecord, out: &mut Events) { .. }
}
```
`engine_thread.rs:441`, `:383` · `crates/sim/src/sim.rs:196` · `crates/engine/src/book.rs:68` · trimmed

To match a message you need `&mut Engine`, and only its owner has one.
The standby has **its own** `Engine`, fed the same records.

---

## 4 · The engine thread's loop

```rust
fn run(&mut self) {
    loop {
        while let Ok(c) = self.rings.cmd.pop() {
            self.command(c);              // drain the commands first
        }
        match &self.run {
            Run::Market(_) => self.market_step(), // run the market to "now"
            Run::Stress(_) => self.stress_step(), // drain the input ring
            Run::Idle => {}
        }
        if self.last_frame.elapsed() >= FRAME {
            self.frame();                 // ~30×/s → UiMsg
        }
    }
}
```
`crates/server/src/engine_thread.rs:495` · trimmed

---

## 5 · Draining a ring

**Rust:** `pop()` returns a `Result`. "Empty" is a value you must
handle, not a crash. `let … else` handles it in one line.

The engine side, in stress mode: take up to 8,192 messages, stop when empty.

```rust
while n < BATCH {
    let Ok(m) = s.input.pop() else { break };        // empty → stop
    n += 1;
    s.seq += 1;
    let rec = LogRecord { seq: s.seq, t_ns: m.t_ns, block: s.seq - 1,
                          agent: m.agent, kind: m.kind };
    push_wait(&mut self.rings.log, LogMsg::Record(rec), ..);  // log it
    s.engine.on_message_into(&rec, &mut s.out);               // match it
    push_wait(&mut self.rings.standby, StandbyMsg::Record(rec), ..);
}
```
`crates/server/src/engine_thread.rs:943` · trimmed

The reader side, the log writer's thread: wait politely when empty.

```rust
loop {
    let Ok(msg) = rx.pop() else {
        idle.wait();              // spin 200 times, then sleep 50 µs
        continue;
    };
    idle.reset();
    match msg {
        LogMsg::Start(info) => { /* open a new file */ }
        LogMsg::Record(r) => { /* append it to the file */ }
        LogMsg::End { .. } => { /* close, replay, compare the fills hash */ }
        LogMsg::Exit => { /* finish and stop */ }
    }
}
```
`crates/server/src/readers.rs:124` · trimmed

When a ring is full, each writer does what its reader needs:

```rust
// the log must see everything: wait
fn push_wait<T>(p: &mut Producer<T>, mut v: T, ..) {
    loop {
        match p.push(v) {
            Ok(()) => break,
            Err(PushError::Full(back)) => {  // full: you get your value back
                v = back;
                std::hint::spin_loop();
            }
        }
    }
}

// the UI only needs the latest picture: drop it
if self.rings.ui.push(UiMsg::Frame(frame)).is_err() { /* dropped */ }
```
`engine_thread.rs:315`, `:1210` · trimmed

---

## 6 · Cores: one book, one core

A thread runs on one core at a time, so **one book can never use
more than one core.** That's the ceiling: ~5.5M msgs/s.

Nothing caps the **process**. Every thread with work gets its own core:

| thread     | market mode         | stress mode, flat out |
| ---------- | ------------------- | --------------------- |
| engine     | mostly asleep       | ~1 core (spins)       |
| producer   | doesn't exist       | ~1 core               |
| log writer | nearly idle         | ~0.7 – 1 core         |
| standby    | nearly idle         | ~0.7 – 1 core         |
| **total**  | **well under 1**    | **~4 cores of 8**     |

Logging, checking and the fake traffic each get their own core,
so none of them slows the engine.

**On the Mac** we can't pin a thread to a core. We only ask for the
fast ones:

```rust
pthread_set_qos_class_self_np(USER_INTERACTIVE, 0)  // a request, not a pin
```
`crates/server/src/qos.rs:9` · trimmed

**In production on Linux:** pin the engine to its own isolated core
(`isolcpus`, `nohz_full`) and send interrupts elsewhere.

---

## 7 · Memory: no allocation per message

**Rust:** no garbage collector. We decide when memory is allocated,
and the hot path doesn't.

```rust
// the caller lends a buffer; the engine refills it
pub fn on_message_into(&mut self, msg: &LogRecord, out: &mut Events) {
    out.clear();                          // keeps its capacity
    // …match, pushing events into `out`
}

pub struct Engine {
    orders: Slab<Order>,                  // one array, freed slots reused
    index: BTreeMap<OrderId, usize>,      // id → slot, for cancels
    bids: BTreeMap<Px, Level>,            // price → FIFO queue
    asks: BTreeMap<Px, Level>,
    spare: Vec<VecDeque<usize>>,          // emptied levels, kept for reuse
}
```
`crates/engine/src/book.rs:68`, `:30` · trimmed

| | before | after |
| --- | ---: | ---: |
| allocations per message | 1.25 – 1.38 | 0.001 – 0.16 |
| matching, ns per message | | 21 – 38 |
| whole process at 1M msgs/s | | ~32 MB |

Rings are allocated once, at full size, when the server starts.

---

## 8 · Inside the market: a heap and a sort

`market_step` turns real time × playback speed into virtual time `t`,
then runs every event up to it. **Latency is a later timestamp.**

**Rust:** `impl Ord` decides the heap's order; `peek` returns an `Option`.

```rust
while self.heap.peek().is_some_and(|item| item.t <= t) {
    let item = self.heap.pop().expect("peeked");   // earliest first
    self.now = item.t;
    self.handle(item.ev);                          // may push more
}
```
`crates/sim/src/sim.rs:363`

- `peek()` looks at the earliest event **without removing it**:
  `Some(item)`, or `None` if the heap is empty.
- `is_some_and(|item| item.t <= t)`: there is an event, **and** it's due.
- If not, stop: the rest belongs to the future. Otherwise `pop()` takes
  it and `handle` runs it, which may push new events.

When a block closes, it is sorted by the rule. This is the whole
experiment:

```rust
match rule {
    Fcfs => block.sort_by_key(|p| (p.t_ns, p.tiebreak)),
    Pga => block.sort_by_key(|p| {
        (Reverse(p.kind.priority_fee()), p.t_ns, p.tiebreak)
    }),
    CancelPriority => block.sort_by_key(|p| {
        (!p.kind.is_cancel(), p.t_ns, p.tiebreak)
    }),
}
```
`crates/sim/src/sequencer.rs:26` · open this file live

With `B = 0` there is no block: each message goes straight to the book.

---

## 9 · One race

A price jump at t = 0, blocks of B = 250 ms.

```
  0 ms   price jumps
  8 ms   sniper's IOC arrives   ┐
 23 ms   LP's cancel arrives    ┘ same block
250 ms   block closes → sort → match
```

| rule            | sniper arrives first | LP arrives first |
| --------------- | -------------------- | ---------------- |
| FCFS            | sniped               | **safe**         |
| PGA             | sniped               | sniped           |
| cancel priority | **safe**             | **safe**         |

- **B = 0:** the same order as FCFS, 242 ms sooner. Under FCFS, B only adds delay.
- **PGA:** a tip beats arriving first, so the LP loses even when it was faster.
- **Matching takes ~30 ns. Blocks are 250 ms apart.** Speed doesn't pick the winner. The ordering rule does.

---

## 10 · Where Rust does the work

| Rust feature | Where | What it buys |
| --- | --- | --- |
| Enums with data, exhaustive `match` | every message, `Run` (stops 1, 3) | can't forget a case; one mode at a time |
| Ownership and `move` | ring ends, the `Engine` (2, 3) | one writer per ring and per book, no locks |
| `Send` but not `Clone` | `rtrb` `Producer` / `Consumer` (2) | two writers to one ring won't compile |
| `&mut` borrows | `on_message_into(&mut self, .., &mut Events)` (3, 7) | exclusive access, and a reused buffer |
| `Result` / `Option`, `let … else` | `pop()`, `peek()` (5, 8) | "empty" is handled, never a crash |
| Traits | `impl Ord for Item` (8) | the heap pops the earliest event |
| No garbage collector | slab, reused buffers, rings sized once (7) | ~0 allocations a message, no GC pauses |
| Integer types | `i64` ticks, `u64` lots, `i128` money | same input, same bytes, every replay |

---

## Thanks

Code, docs and every number in this talk: **rosalbito.dev**
