//! The engine benchmark (spec sections 1 and 10, architecture section 8).
//!
//! `bench alloc`     allocations per message on each load profile
//! `bench engine`    the engine alone: ns per message, per-message p50/p99/p99.9, per profile
//! `bench blocks`    time to sort and match one block of 100, 1k and 10k messages
//! `bench channels`  the layer comparison: direct calls vs rtrb vs crossbeam vs tokio mpsc vs
//!                   std mpsc vs Mutex<VecDeque>, throughput and latency percentiles
//! `bench all`       everything, `--runs` times, as markdown tables
//!
//! Release build only: `cargo run --release -p bench -- all --runs 3`.

mod alloc;
mod channels;
mod engine_cost;
mod profiles;
mod qos;

use std::time::Duration;

use clap::{Parser, Subcommand};
use sim::OrderingRule;

#[global_allocator]
static GLOBAL: alloc::Counting = alloc::Counting;

#[derive(Parser)]
#[command(about = "Engine and channel benchmarks")]
struct Args {
    #[command(subcommand)]
    cmd: Cmd,
    /// Repeat each measurement this many times (the stability check).
    #[arg(long, default_value_t = 1, global = true)]
    runs: usize,
    /// Wall-clock budget per throughput measurement, in ms.
    #[arg(long, default_value_t = 400, global = true)]
    budget_ms: u64,
}

#[derive(Subcommand, Clone, Copy)]
enum Cmd {
    Alloc,
    Engine,
    Blocks,
    Channels,
    All,
}

fn machine() -> String {
    let out = std::process::Command::new("sysctl")
        .args(["-n", "machdep.cpu.brand_string"])
        .output();
    let cpu = match out {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).trim().to_string(),
        _ => "unknown CPU".into(),
    };
    let cores = std::thread::available_parallelism().map_or(0, |n| n.get());
    format!("{cpu}, {cores} cores, {}", std::env::consts::OS)
}

fn main() {
    let args = Args::parse();
    qos::prefer_performance_cores();
    if cfg!(debug_assertions) {
        eprintln!("warning: debug build; run with --release for real numbers");
    }
    println!("machine: {}\n", machine());
    let budget = Duration::from_millis(args.budget_ms);
    let cmd = args.cmd;
    let want = |c: Cmd| {
        matches!(cmd, Cmd::All) || std::mem::discriminant(&cmd) == std::mem::discriminant(&c)
    };

    let recorded = if want(Cmd::Alloc) || want(Cmd::Engine) {
        profiles::recorded()
    } else {
        Vec::new()
    };
    if want(Cmd::Alloc) {
        alloc_table(&recorded);
    }
    for run in 1..=args.runs {
        if args.runs > 1 {
            println!("## run {run} of {}\n", args.runs);
        }
        if want(Cmd::Engine) {
            engine_table(&recorded, budget);
        }
        if want(Cmd::Blocks) {
            blocks_table();
        }
        if want(Cmd::Channels) {
            channels::table(budget);
        }
    }
}

fn alloc_table(recorded: &[profiles::Profile]) {
    let b = profiles::burst(200, 10_000, OrderingRule::Fcfs, 7);
    let mut burst = b.warm.clone();
    burst.extend(&b.block);
    let mut flows: Vec<(&str, &[engine::LogRecord])> =
        recorded.iter().map(|p| (p.name, &p.records[..])).collect();
    flows.push(("monday burst (synthetic)", &burst));
    for (api, title) in [
        (
            engine_cost::Api::Owned,
            "engine::on_message (returns a new Vec)",
        ),
        (
            engine_cost::Api::Reused,
            "engine::on_message_into (reused buffer)",
        ),
    ] {
        println!("### Allocations per message: {title}\n");
        print!("| profile | messages | allocs / msg | reallocs / msg | total / msg |");
        for k in engine_cost::KINDS {
            print!(" {k} |");
        }
        println!("\n|---|---:|---:|---:|---:|---:|---:|---:|---:|");
        for &(name, records) in &flows {
            let a = engine_cost::allocs(records, api);
            print!(
                "| {name} | {} | {:.3} | {:.3} | {:.3} |",
                records.len(),
                a.allocs_per_msg(),
                a.reallocs_per_msg(),
                a.per_msg()
            );
            for k in 0..4 {
                print!(" {:.3} |", a.per_kind(k));
            }
            println!();
        }
        println!();
    }
}

fn engine_table(recorded: &[profiles::Profile], budget: Duration) {
    println!("### Engine alone (on_message_into), per load profile\n");
    println!("| profile | messages | mix GTC/IOC/cancel/tick | ns/msg (sustained) | msgs/s | p50 ns | p99 ns | p99.9 ns |");
    println!("|---|---:|---|---:|---:|---:|---:|---:|");
    for p in recorded {
        let r = engine_cost::profile_row(p, budget);
        let (g, i, c, t) = profiles::mix(&p.records);
        println!(
            "| {} | {} | {g}/{i}/{c}/{t} | {:.1} | {:.2}M | {} | {} | {} |",
            r.name,
            r.msgs,
            r.ns_per_msg,
            1e3 / r.ns_per_msg,
            r.p50,
            r.p99,
            r.p999
        );
    }
    println!(
        "\n(profiles: {})\n",
        recorded
            .iter()
            .map(|p| format!("{}: {}", p.name, p.what))
            .collect::<Vec<_>>()
            .join("; ")
    );
}

fn blocks_table() {
    println!("### Block processing: sort one block by the rule, then match it (deep book, 200 levels a side)\n");
    println!("| messages in block | FCFS µs | PGA µs | cancel priority µs | FCFS ns/msg |");
    println!("|---:|---:|---:|---:|---:|");
    for n in [100, 1_000, 10_000] {
        let reps = (200_000 / n).clamp(15, 400);
        let f = engine_cost::block_cost(n, OrderingRule::Fcfs, reps);
        let p = engine_cost::block_cost(n, OrderingRule::Pga, reps);
        let c = engine_cost::block_cost(n, OrderingRule::CancelPriority, reps);
        println!(
            "| {n} | {:.1} | {:.1} | {:.1} | {:.1} |",
            f.0, p.0, c.0, f.1
        );
    }
    println!();
}
