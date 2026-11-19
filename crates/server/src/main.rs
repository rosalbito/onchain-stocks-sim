//! `stale-quotes-server`: the live demo. Starts the engine thread and the dashboard, and opens
//! the dashboard in the browser on its setup screen.
//!
//! ```text
//! cargo run --release -p server                         # setup screen
//! cargo run --release -p server -- --start --scenario monday_open --speed 1000
//! cargo run --release -p server -- --stress 1000000     # stress mode at 1M msgs/s
//! cargo run --release -p server -- replay out/live/<file>.log      # or a stress .journal
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use sim::OrderingRule;

#[derive(Parser)]
#[command(about = "Stale Quotes live demo: the engine thread, a websocket and the dashboard")]
struct Args {
    #[command(subcommand)]
    cmd: Option<Cmd>,
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:7878")]
    addr: SocketAddr,
    /// Start a market run at once instead of showing the setup screen.
    #[arg(long)]
    start: bool,
    /// Start a stress run at once at this many msgs/s (0 = as fast as possible).
    #[arg(long)]
    stress: Option<u64>,
    /// Scenario for --start: news_jump, calm_weekday or monday_open.
    #[arg(long, default_value = "news_jump")]
    scenario: String,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// Playback speed for --start (1 = real time, 0.01 = 1/100).
    #[arg(long, default_value_t = 1.0)]
    speed: f64,
    /// Ordering rule for --start: fcfs, pga or cancel_priority.
    #[arg(long, default_value = "fcfs")]
    rule: String,
    /// Block length B in ms for --start.
    #[arg(long, default_value_t = 0)]
    block_ms: u64,
    /// Logs go to <out>/live/.
    #[arg(long, default_value = "out")]
    out: PathBuf,
    /// A run stops when its log reaches this many MB (stress logs grow fast).
    #[arg(long, default_value_t = 2000)]
    log_cap_mb: u64,
    /// Don't open the dashboard in a browser.
    #[arg(long)]
    no_open: bool,
}

#[derive(Subcommand)]
enum Cmd {
    /// Check that a log written by the server replays exactly (.log or .journal).
    Replay { log: PathBuf },
}

fn main() -> ExitCode {
    let args = Args::parse();
    if let Some(Cmd::Replay { log }) = &args.cmd {
        return replay(log);
    }
    let Some(rule) = OrderingRule::ALL
        .into_iter()
        .find(|r| r.name() == args.rule)
    else {
        eprintln!("error: unknown rule {}", args.rule);
        return ExitCode::FAILURE;
    };
    let Some(scenario) = server::scenario_index(&args.scenario) else {
        eprintln!("error: unknown scenario {}", args.scenario);
        return ExitCode::FAILURE;
    };
    let autostart = (args.start || args.stress.is_some()).then(|| server::StartSpec {
        stress: args.stress.is_some(),
        rate: args.stress.unwrap_or(100_000),
        scenario,
        rule,
        block_ms: args.block_ms,
        seed: args.seed,
        speed_milli: (args.speed * 1000.0).round() as u64,
    });
    if cfg!(debug_assertions) {
        eprintln!("note: debug build; use --release for the demo");
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(async move {
        let opts = server::Options {
            addr: args.addr,
            autostart,
            out_dir: args.out,
            log_cap_bytes: args.log_cap_mb * 1_000_000,
        };
        let srv = match server::start(opts).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("error: {e}");
                return ExitCode::FAILURE;
            }
        };
        let url = format!("http://{}/", srv.addr);
        eprintln!("dashboard: {url}   (Ctrl-C to stop; logs in out/live/)");
        if !args.no_open {
            open(&url);
        }
        let _ = tokio::signal::ctrl_c().await;
        eprintln!("stopping: writing and checking the log");
        srv.shutdown().await;
        ExitCode::SUCCESS
    })
}

fn open(url: &str) {
    let cmd = if cfg!(target_os = "macos") {
        "open"
    } else if cfg!(target_os = "windows") {
        "explorer"
    } else {
        "xdg-open"
    };
    if let Err(e) = std::process::Command::new(cmd).arg(url).spawn() {
        eprintln!("could not open a browser ({e}); open {url} yourself");
    }
}

fn replay(path: &PathBuf) -> ExitCode {
    if path.extension().is_some_and(|e| e == "journal") {
        let t = std::time::Instant::now();
        return match server::verify_stress_journal(path) {
            Ok((n, hash)) => {
                println!("{}: {n} records", path.display());
                println!("replayed fills hash   {hash}");
                println!(
                    "OK: every record matches the flow regenerated from the seed ({:.1} s)",
                    t.elapsed().as_secs_f64()
                );
                ExitCode::SUCCESS
            }
            Err(e) => {
                println!("FAIL: {e}");
                ExitCode::FAILURE
            }
        };
    }
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("error: {}: {e}", path.display());
            return ExitCode::FAILURE;
        }
    };
    let file = match sim::log::decode(&bytes) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };
    let records: Vec<engine::LogRecord> = file.records.iter().map(Into::into).collect();
    let engine_hash = sim::log::replay_fills_hash(&records);
    println!(
        "{}: scenario {}, seed {}, {} records, {} live control changes",
        path.display(),
        file.scenario,
        file.seed,
        records.len(),
        sim::config_records(&records)
    );
    println!("engine replay fills hash   {engine_hash}");
    match sim::replay_check(&bytes) {
        Ok(h) if h == engine_hash => {
            println!("re-simulated fills hash    {h}");
            println!("OK: the log re-simulates byte for byte and replays to the same fills");
            ExitCode::SUCCESS
        }
        Ok(h) => {
            println!("re-simulated fills hash    {h}");
            println!("FAIL: fills hashes differ");
            ExitCode::FAILURE
        }
        Err(e) => {
            println!("FAIL: {e}");
            ExitCode::FAILURE
        }
    }
}
