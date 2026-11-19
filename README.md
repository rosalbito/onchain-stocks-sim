# Stale Quotes

A deterministic CLOB matching engine and a simulated L2 sequencer, built for the talk "Stale Quotes: Latency Races and Ordering Rules for On-Chain Stocks". The design is in [docs/SPEC.md](docs/SPEC.md) and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md); choices made along the way are in [docs/DECISIONS.md](docs/DECISIONS.md).

## The live demo

```sh
cargo run --release -p server
```

This builds the server, starts the engine thread and the dashboard at <http://127.0.0.1:7878/>, and opens it in the browser. Ctrl-C stops it; the run's input log is written to `out/live/` and checked on the way out.

- `--scenario news_jump | calm_weekday | monday_open`, `--seed N`, `--speed 0.01`, `--rule pga`, `--block-ms 250`, `--no-open`
- Presenter keys: `f` / `p` / `c` rule, `b` cycles B, `[` `]` slower / faster, space pause, `l` cycles load, `s` skips to just before the news jump (or the close, then the reopen), `r` restart, `n` / `w` / `m` scenario.
- Every control is logged as a `Config` record. When a run ends or stops, the server re-simulates it from its log and shows "log replays exactly" on the engine panel. To check a log by hand: `cargo run --release -p server -- replay out/live/<file>.log`.

The page is plain HTML, CSS and canvas, embedded in the binary with its fonts, so it needs no network at the venue.

## Other entry points

| What | Command |
| --- | --- |
| Tests and gates | `cargo test --workspace` |
| One batch run (log + summary) | `cargo run --release -p sim --bin stale-quotes -- --scenario scenarios/news_jump.toml` |
| The experiment grid | `cargo run --release -p grid -- --help` |
| The engine benchmark | `cargo run --release -p bench -- all --runs 3` |
