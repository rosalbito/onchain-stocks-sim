//! Live mode (spec sections 11, 12 and 14; architecture sections 4 to 6).
//!
//! ```text
//!                                     stress producer ──input ring──┐
//!  browser ──ws──▶ axum (tokio) ──control ring──▶ ENGINE THREAD (the book's single writer)
//!     ▲                                               │ one rtrb ring per reader
//!     │                                   ┌───────────┼──────────────┐
//!     │                                   ▼           ▼              ▼
//!     └── watch ◀── ui bridge ◀── frames  log writer  standby (replay, latency, drift check)
//!                                         → disk
//! ```
//!
//! The engine thread is the only thing that changes the book. In market mode it runs the
//! simulation; in stress mode it matches a producer thread's synthetic flow. Every record goes
//! to the log writer, which appends it to a journal on disk and checks the whole log when the
//! run ends: a market run must re-simulate from its log byte for byte, a stress run must replay
//! and regenerate to the same fills.

mod charts;
mod engine_thread;
mod journal;
mod qos;
mod readers;
mod stress;
mod sysinfo;
mod web;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use tokio::sync::{oneshot, watch};

pub use engine_thread::{scenario_index, Command, StartSpec, SCENARIOS};
pub use journal::{JournalHeader, JournalReader};
pub use readers::verify_stress_journal;
pub use web::parse_command;

pub struct Options {
    pub addr: SocketAddr,
    /// Start this run at once instead of showing the setup screen.
    pub autostart: Option<StartSpec>,
    /// Logs go to `<out_dir>/live/`.
    pub out_dir: PathBuf,
    /// A run stops when its log reaches this size.
    pub log_cap_bytes: u64,
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

/// Start the engine thread, the readers and the HTTP server. Call from a tokio runtime.
pub async fn start(opts: Options) -> std::io::Result<Server> {
    let (cmd_tx, cmd_rx) = rtrb::RingBuffer::new(CONTROL_RING);
    let (log_tx, log_rx) = rtrb::RingBuffer::new(LOG_RING);
    let (sb_tx, sb_rx) = rtrb::RingBuffer::new(STANDBY_RING);
    let (ui_tx, ui_rx) = rtrb::RingBuffer::new(UI_RING);
    let (snap_tx, snap_rx) = watch::channel::<Arc<str>>(Arc::from("{\"type\":\"starting\"}"));
    let (hist_tx, hist_rx) = watch::channel::<Arc<str>>(Arc::from("{\"type\":\"starting\"}"));

    let tel = Arc::new(sysinfo::Telemetry::default());
    let log_status = Arc::new(Mutex::new(readers::LogStatus::default()));
    let standby = Arc::new(Mutex::new(readers::StandbyStats::default()));
    let threads = vec![
        readers::spawn_log_writer(
            log_rx,
            opts.out_dir.clone(),
            opts.log_cap_bytes,
            log_status.clone(),
            tel.clone(),
        ),
        readers::spawn_standby(sb_rx, standby.clone(), tel.clone()),
        readers::spawn_ui_bridge(
            ui_rx,
            readers::Bridge {
                snapshot: snap_tx,
                history: hist_tx,
                standby,
                log: log_status,
                tel: tel.clone(),
                out_dir: opts.out_dir.clone(),
            },
        ),
        engine_thread::spawn(
            engine_thread::Rings {
                cmd: cmd_rx,
                log: log_tx,
                standby: sb_tx,
                ui: ui_tx,
            },
            tel.clone(),
            opts.autostart,
        ),
    ];

    let control = Arc::new(Mutex::new(cmd_tx));
    let app = web::router(web::AppState {
        snapshot: snap_rx,
        history: hist_rx,
        control: control.clone(),
        tel,
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
