# Stale Quotes

A deterministic CLOB matching engine and a simulated L2 sequencer, built for the talk "Stale Quotes: Latency Races and Ordering Rules for On-Chain Stocks". The design is in [docs/SPEC.md](docs/SPEC.md) and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md); choices made along the way are in [docs/DECISIONS.md](docs/DECISIONS.md).

## The live demo

```sh
cargo run --release -p server
```

This builds the server, starts the engine thread, and opens the dashboard at <http://127.0.0.1:7878/> on its setup screen. Pick the mode (market or stress), the scenario, rule, B, speed or stress rate, and the seed, then press Start (or Enter). Ctrl-C stops the server; the run's log is in `out/live/` and is checked on the way out.

- **Pages:** `1` system (msgs/s, latency, CPU, memory, the log on disk, rings), `2` market (price, book, races), `3` profits (who gained and who lost).
- **Live keys** (hidden; `?` shows them): `f` / `p` / `c` rule, `b` cycles B, `[` `]` slower / faster (stress: lower / higher rate), space pause, `l` cycles load, `s` skips to just before the news jump (or the close, then the reopen), `r` restart, Esc back to setup.
- **Stress mode** sends synthetic flow at 10k, 100k, 1M msgs/s or flat out through the real path (producer → input ring → engine thread → log writer → disk). A run stops when its log reaches `--log-cap-mb` (default 2,000), and only the latest stress log is kept.
- Every live change is logged as a `Config` record. When a run ends, the server reads its log back from disk: a market log is re-simulated byte for byte, a stress log is replayed and regenerated from its seed. The System page states the result. To check a log by hand: `cargo run --release -p server -- replay out/live/<file>.log` (or a stress `.journal`).
- To skip the setup screen: `--start --scenario monday_open --speed 1000` or `--stress 1000000`; `--no-open` leaves the browser alone.

The page is plain HTML, CSS and canvas, embedded in the binary with its fonts, so it needs no network at the venue.

## Other entry points

| What | Command |
| --- | --- |
| Tests and gates | `cargo test --workspace` |
| One batch run (log + summary) | `cargo run --release -p sim --bin stale-quotes -- --scenario scenarios/news_jump.toml` |
| The experiment grid | `cargo run --release -p grid -- --help` |
| The engine benchmark | `cargo run --release -p bench -- all --runs 3` |
