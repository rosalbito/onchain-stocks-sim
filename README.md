# Stale Quotes

Tokenized stocks trade on-chain around the clock, but their price still comes from Nasdaq, which closes. Every time Nasdaq moves, and above all when it reopens on Monday, the on-chain order book is left holding **stale quotes**: prices a market maker (the LP) posted before the move. Whoever reaches the book first collects the difference. This project measures who that is under three sequencer ordering rules:

- **First come, first served (FCFS):** the fastest sniper wins.
- **Priority gas auction (PGA):** the highest fee wins.
- **Cancel priority:** in each block, cancels run before anything that could trade against them, so the LP's cancel wins.

It is built for the talk "Stale Quotes: Latency Races and Ordering Rules for On-Chain Stocks" (Subvisual, 26 Nov 2026). It has three parts:

- A **deterministic order-book matching engine** (a CLOB with price-time priority) in Rust.
- A **simulated market**: a hidden Nasdaq price, an LP, snipers and a regular trader, each with a latency, feeding a sequencer that cuts time into blocks and sorts each block by the ordering rule.
- A **live server and dashboard** that run the engine in real time, plus a stress mode that floods it with millions of messages a second.

Headline result, 250 ms blocks, 3 snipers twice as fast as the LP, mean of 200 runs. The table shows what the LP loses to snipers:

| | FCFS | PGA | Cancel priority |
| --- | ---: | ---: | ---: |
| Weekday, per hour | $19.22 | $40.48 | **$3.82** |
| Monday open, first 5 min | $18.37 | $20.19 | **$0.31** |

All agent parameters are illustrative, not calibrated to a real stock. The ratios between rules are what carry over.

## How it works

```
 browser ──websocket──▶ web edge ──control ring──▶ ┌──────────────── engine thread ────────────────┐
    ▲                  (axum, tokio)                │ market mode: price feed + agents run here     │
    │                                               │ 1 · sequencer: blocks of B ms, sorted by rule │
    │                       producer ──input ring──▶│ 2 · order book: matches one message at a time │
    │                    (stress mode only)         └───────────────────────┬───────────────────────┘
    │                                       3 · every numbered record, one ring per reader
    │                         ┌──────────────────────────┬──────────────────┴─────────────┐
    │                         ▼                          ▼                                ▼
    └── 4 · JSON, 30×/s ── UI bridge              standby                         log writer ──▶ disk
                           drops old frames       re-matches every record,        never drops a record
                                                  checks the fills match          (the input log)
```

1. **One thread owns the book.** The engine thread is the only thing that ever changes the order book. In market mode it also runs the simulated market (price feed, agents, sequencer) on a virtual clock, so a run is a pure function of its scenario, seed and logged control changes. In stress mode a producer thread sends synthetic orders through the input ring instead, first come, first served.
2. **The log is the source of truth.** Every message the sequencer numbers goes to the log writer, which streams it to disk. The log stores the input (orders, cancels, price ticks, control changes), not the fills. After every run the server reads the log back, replays it through a fresh engine, and checks that the fills hash is the same. The dashboard shows the result.
3. **Everything else reads.** Each reader has its own lock-free ring. The log writer and the standby must see every record, so when their ring is full the engine waits. The UI bridge needs only the latest picture, so it drops old frames, and a slow browser can never slow the engine.
4. **The websocket.** About 30 times a second the server sends the browser a JSON snapshot: price, book, candles, trades, the latest race, profits, CPU, memory and ring fill. It also sends a one-minute history twice a second. The browser sends commands back: start, stop, restart, rule, B, speed, load, pause, skip and the stress rate (see `crates/server/src/web.rs`).

| Thread | Job | If it falls behind |
| --- | --- | --- |
| Web edge (tokio) | HTTP, websocket, commands in | Nothing upstream notices |
| Engine | Sequence and match every message | Everything waits: it's the bottleneck by design |
| Producer (stress) | Synthetic flow at a target rate | Waits on a full input ring (backpressure, counted) |
| Log writer | Append every record to disk | The engine waits |
| Standby | A second engine fed the same records; times each message, checks fills | The engine waits |
| UI bridge | Newest frame → JSON | Frames are dropped |

The design decisions behind this (one writer, the book's data structures, rings, cores) are in [docs/ADR.md](docs/ADR.md), each with the measurement that backs it.

### Crates

| Crate | What it does |
| --- | --- |
| `engine` | The order book and matching. Deterministic: no clocks, no floats, no hash-map iteration. Depends only on `std` and `slab`. |
| `sim` | The market: price path, LP, snipers, regular traders, the sequencer and its three rules, the virtual clock, and `Live` for running step by step. |
| `metrics` | Races, the ledger across five account classes (it always sums to zero), LP loss, the Monday-open gap prize. |
| `grid` | The experiment: every rule × block length × speed gap × sniper count on the same seeds, written to parquet. |
| `server` | The live process: engine thread, readers, rings, stress producer, web edge, the embedded dashboard. |
| `bench` | Benchmarks: engine cost per message, allocations, ring vs channel vs mutex, block sort + match. |

## Run the demo

```sh
cargo run --release -p server
```

This builds the server, starts the engine thread and opens the dashboard at <http://127.0.0.1:7878/> on its setup screen. Pick the mode (market or stress), the scenario, rule, B, speed or stress rate, and the seed, then press Start (or Enter). Ctrl-C stops the server. The run's log is in `out/live/` and is checked on the way out. Old logs are trimmed to `--keep-logs-mb` (default 1,000) when the server stops.

- **Pages:** `1` system (msgs/s, latency, CPU, memory, the log on disk, rings), `2` market (price chart, race zoom, book, trades), `3` profits (who gained and who lost).
- **Live keys** (hidden; `?` shows them): `f` / `p` / `c` rule, `b` cycles B, `[` `]` slower / faster (stress: lower / higher rate), space pause, `l` cycles load, `s` skips to just before the news jump (or the close, then the reopen), `r` restart, Esc back to setup.
- **Stress mode** sends synthetic flow at 10k, 100k, 1M msgs/s or flat out through the real path (producer → input ring → engine thread → log writer → disk). A run stops when its log reaches `--log-cap-mb` (default 1,000).
- **Crowd:** market runs default to more agents of the same three roles, so the book is busy enough to watch; `--no-crowd`, or `"crowd": false` in a start command, runs the talk's model (1 LP, 3 snipers, 1 trader).
- To check a log by hand: `cargo run --release -p server -- replay out/live/<file>.log` (or a stress `.journal`).
- To skip the setup screen: `--start --scenario monday_open --speed 1000` or `--stress 1000000`; `--no-open` leaves the browser alone.

The page is plain HTML, CSS and canvas, embedded in the binary with its fonts, so it needs no network at the venue.

## Reproduce the talk's numbers

Every number on the slides comes from one of these. The simulation is deterministic, so a rerun gives the same numbers to the last digit, confidence intervals included.

One-time setup for the analysis scripts:

```sh
python3 -m venv analysis/.venv && analysis/.venv/bin/pip install -r analysis/requirements.txt
```

| Slides | Command | Time on an M3 |
| --- | --- | --- |
| Results, follow the money, Monday open, PGA (the headline cell) | `cargo run --release -p grid -- --seeds 200 --alpha-bps 3000 --alpha-split --blocks-ms 250 --gaps 0.5 --snipers 3 --out out/check` then `analysis/.venv/bin/python analysis/summary.py --data out/check --out out/check`. Compare `out/check/headline_b250.csv` with `analysis/out/headline_b250.csv`: they are identical. | ~1 min |
| The full grid (216 settings × 200 seeds) and the main chart | `cargo run --release -p grid -- --seeds 200 --alpha-bps 3000 --alpha-split`, then `analysis/summary.py` and `analysis/main_chart.py` | ~15–20 min, all cores |
| PGA bid sweep (what the LP pays if it bids back) | `analysis/pga_sweep.py`, data in `analysis/data/pga_sweep` (how it was made: DECISIONS entry 76) | seconds |
| Engine ns per message, allocations, ring vs channel, block cost | `cargo run --release -p bench -- engine`, `-- alloc`, `-- channels`, `-- blocks` (or `-- all --runs 3`) | a few min |
| msgs/s at max, p99 latency, memory, replay check | `cargo run --release -p server -- --stress 0` (max) or `--stress 1000000`, System page | live |

Results tables from the runs used for the talk are in [docs/DECISIONS.md](docs/DECISIONS.md): entries 68 (bench), 74 (stress) and 76 to 80 (grid and PGA).

## Docs

- [docs/ADR.md](docs/ADR.md): the main design decisions, short, with the numbers behind them. Start here.
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): the long version of the design.
- [docs/SPEC.md](docs/SPEC.md): the spec the code was built from.
- [docs/DECISIONS.md](docs/DECISIONS.md): every decision and measurement made along the way, numbered.

| Other entry points | Command |
| --- | --- |
| Tests and gates | `cargo test --workspace` |
| One batch run (log + summary) | `cargo run --release -p sim --bin stale-quotes -- --scenario scenarios/news_jump.toml` |
| The experiment grid | `cargo run --release -p grid -- --help` |
