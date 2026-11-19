//! `stale-quotes-server`: the live demo. Starts the engine thread and the dashboard, and opens
//! the dashboard in the browser.
//!
//! ```text
//! cargo run --release -p server                      # news jump, opens the browser
//! cargo run --release -p server -- --scenario monday_open --speed 1000
//! cargo run --release -p server -- replay out/live/<file>.log
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
    /// Scenario to start with: news_jump, calm_weekday or monday_open.
    #[arg(long, default_value = "news_jump")]
    scenario: String,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// Playback speed (1 = real time, 0.01 = 1/100).
    #[arg(long, default_value_t = 1.0)]
    speed: f64,
    /// Override the scenario's ordering rule: fcfs, pga or cancel_priority.
    #[arg(long)]
    rule: Option<String>,
    /// Override the scenario's block length B in ms.
    #[arg(long)]
    block_ms: Option<u64>,
    /// Logs go to <out>/live/.
    #[arg(long, default_value = "out")]
    out: PathBuf,
    /// Don't open the dashboard in a browser.
    #[arg(long)]
    no_open: bool,
}

#[derive(Subcommand)]
enum Cmd {
    /// Check that a log written by the server replays exactly.
    Replay { log: PathBuf },
}

fn main() -> ExitCode {
    let args = Args::parse();
    if let Some(Cmd::Replay { log }) = &args.cmd {
        return replay(log);
    }
    let rule = match &args.rule {
        None => None,
        Some(name) => match OrderingRule::ALL.into_iter().find(|r| r.name() == name) {
            Some(r) => Some(r),
            None => {
                eprintln!("error: unknown rule {name}");
                return ExitCode::FAILURE;
            }
        },
    };
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
            scenario: args.scenario,
            seed: args.seed,
            speed_milli: (args.speed * 1000.0).round() as u64,
            rule,
            block_ms: args.block_ms,
            out_dir: args.out,
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
