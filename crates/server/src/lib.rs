//! Live mode (spec sections 11, 12 and 14; architecture sections 4 to 6).
//!
//! ```text
//!  browser ──ws──▶ axum (tokio) ──control ring──▶ ENGINE THREAD (sim + book, single writer)
//!     ▲                                               │ one rtrb ring per reader
//!     │                                   ┌───────────┼──────────────┐
//!     │                                   ▼           ▼              ▼
//!     └── watch ◀── ui bridge ◀── frames  log writer  standby (replay, latency, drift check)
//! ```
//!
//! The engine thread is the only thing that changes the simulation or the book. Controls reach it
//! through the control ring and become `Config` records in the input log, so a run with live
//! changes replays exactly from its log; the log writer checks that after every run.

mod engine_thread;
mod readers;
mod web;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use sim::OrderingRule;
use tokio::sync::{oneshot, watch};

pub use engine_thread::{scenario_index, Command, SCENARIOS};
pub use web::parse_command;

pub struct Options {
    pub addr: SocketAddr,
    pub scenario: String,
    pub seed: u64,
    /// Playback speed in thousandths (1000 = 1×).
    pub speed_milli: u64,
    pub rule: Option<OrderingRule>,
    pub block_ms: Option<u64>,
    /// Logs go to `<out_dir>/live/`.
    pub out_dir: PathBuf,
}

pub struct Server {
    pub addr: SocketAddr,
    control: Arc<Mutex<rtrb::Producer<Command>>>,
    threads: Vec<JoinHandle<()>>,
    http_stop: oneshot::Sender<()>,
    http: tokio::task::JoinHandle<()>,
}

const LOG_RING: usize = 1 << 16;
const STANDBY_RING: usize = 1 << 16;
const UI_RING: usize = 64;
const CONTROL_RING: usize = 256;

/// Start the engine thread, the three readers and the HTTP server. Call from a tokio runtime.
pub async fn start(opts: Options) -> std::io::Result<Server> {
    let scenario = scenario_index(&opts.scenario).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("unknown scenario {}", opts.scenario),
        )
    })?;
    let (cmd_tx, cmd_rx) = rtrb::RingBuffer::new(CONTROL_RING);
    let (log_tx, log_rx) = rtrb::RingBuffer::new(LOG_RING);
    let (sb_tx, sb_rx) = rtrb::RingBuffer::new(STANDBY_RING);
    let (ui_tx, ui_rx) = rtrb::RingBuffer::new(UI_RING);
    let (snap_tx, snap_rx) = watch::channel::<Arc<str>>(Arc::from("{\"type\":\"starting\"}"));

    let log_status = Arc::new(Mutex::new(readers::LogStatus::default()));
    let standby = Arc::new(Mutex::new(readers::StandbyStats::default()));
    let threads = vec![
        readers::spawn_log_writer(log_rx, opts.out_dir.clone(), log_status.clone()),
        readers::spawn_standby(sb_rx, standby.clone()),
        readers::spawn_ui_bridge(ui_rx, snap_tx, standby, log_status),
        engine_thread::spawn(
            engine_thread::Rings {
                cmd: cmd_rx,
                log: log_tx,
                standby: sb_tx,
                ui: ui_tx,
            },
            engine_thread::Start {
                scenario,
                seed: opts.seed,
                speed_milli: opts.speed_milli,
                rule: opts.rule,
                block_ms: opts.block_ms,
            },
        ),
    ];

    let control = Arc::new(Mutex::new(cmd_tx));
    let app = web::router(web::AppState {
        snapshot: snap_rx,
        control: control.clone(),
    });
    let listener = tokio::net::TcpListener::bind(opts.addr).await?;
    let addr = listener.local_addr()?;
    let (http_stop, stop_rx) = oneshot::channel::<()>();
    let http = tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stop_rx.await;
            })
            .await;
    });
    Ok(Server {
        addr,
        control,
        threads,
        http_stop,
        http,
    })
}

impl Server {
    /// Send a command as if it came from the dashboard.
    pub fn command(&self, c: Command) -> bool {
        self.control.lock().unwrap().push(c).is_ok()
    }

    /// Stop the run (its log is written and checked), then every thread and the HTTP server.
    pub async fn shutdown(self) {
        while !self.command(Command::Shutdown) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let threads = self.threads;
        let _ = tokio::task::spawn_blocking(move || {
            for t in threads {
                let _ = t.join();
            }
        })
        .await;
        let _ = self.http_stop.send(());
        let _ = self.http.await;
    }
}
