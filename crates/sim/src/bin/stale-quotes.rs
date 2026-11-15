//! Run a scenario with a seed; write the input log (postcard) and a text summary.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use sim::{report, OrderingRule, Scenario};

#[derive(Parser)]
#[command(about = "Run a stale-quotes scenario and write its input log and summary")]
struct Args {
    /// Scenario TOML file.
    #[arg(long)]
    scenario: PathBuf,
    /// Master seed.
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// Override the ordering rule: fcfs, pga or cancel_priority.
    #[arg(long)]
    rule: Option<String>,
    /// Override the block length in ms.
    #[arg(long)]
    block_ms: Option<u64>,
    /// Output directory.
    #[arg(long, default_value = "out")]
    out: PathBuf,
}

fn main() -> ExitCode {
    let args = Args::parse();
    match run(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: Args) -> Result<(), String> {
    let text = std::fs::read_to_string(&args.scenario)
        .map_err(|e| format!("{}: {e}", args.scenario.display()))?;
    let mut scenario = Scenario::from_toml(&text)?;
    if let Some(rule) = args.rule {
        scenario.sequencer.rule = OrderingRule::ALL
            .into_iter()
            .find(|r| r.name() == rule)
            .ok_or_else(|| format!("unknown rule {rule}"))?;
    }
    if let Some(b) = args.block_ms {
        scenario.sequencer.block_ms = b;
    }
    let result = sim::run(&scenario, args.seed)?;
    let summary = report::summary(&result);

    std::fs::create_dir_all(&args.out).map_err(|e| e.to_string())?;
    let stem = format!(
        "{}-{}-b{}-seed{}",
        scenario.name,
        scenario.sequencer.rule.name(),
        scenario.sequencer.block_ms,
        args.seed
    );
    let log_path = args.out.join(format!("{stem}.log"));
    let sum_path = args.out.join(format!("{stem}.summary.txt"));
    std::fs::write(&log_path, &result.log_bytes).map_err(|e| e.to_string())?;
    std::fs::write(&sum_path, &summary).map_err(|e| e.to_string())?;
    print!("{summary}");
    println!("wrote {} and {}", log_path.display(), sum_path.display());
    Ok(())
}
