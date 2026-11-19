//! The readers of the engine thread's rings. None of them writes back to the engine.
//!
//! - **log writer**: appends every record to the run's journal on disk as it arrives (flushed
//!   every 100 ms, synced every second), and stops the run at the size cap. When the run ends, a
//!   check thread reads the journal back from disk and verifies it: a market run is converted to
//!   the batch log format and re-simulated from it; a stress run is replayed through a fresh
//!   engine and regenerated from its seed. Both must give the primary's fills hash.
//! - **standby**: a second engine fed the same records (architecture section 5's hot standby).
//!   It times every `on_message` and every block, and compares its fills hash with the
//!   primary's at each frame.
//! - **UI bridge**: turns frames into the dashboard's JSON, samples the process (CPU, memory,
//!   disk) twice a second into rolling histories, and hands both to the websocket tasks through
//!   `tokio::sync::watch` (latest value wins).
//!
//! Idle readers spin briefly, then sleep 50 µs (architecture section 6).

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use engine::{Engine, EventKind, LogRecord, MsgKind};
use hdrhistogram::Histogram;
use metrics::AccountClass;
use rtrb::Consumer;
use serde_json::{json, Value};
use sim::log::{self, FillsHasher, LogFile, WireRecord};
use tokio::sync::watch;

use crate::engine_thread::{
    Frame, LogMsg, Outcome, RunInfo, StandbyMsg, State, UiMsg, CHART, RING_NAMES, SCENARIOS,
};
use crate::journal::{JournalHeader, JournalReader, JournalWriter, JOURNAL_VERSION};
use crate::stress::Generator;
use crate::sysinfo::{self, Telemetry, THREADS, T_LOG, T_STANDBY, T_UI};

/// Spin, then sleep: what an idle reader does.
struct Idle(u32);

impl Idle {
    fn wait(&mut self) {
        self.0 += 1;
        if self.0 < 200 {
            std::hint::spin_loop();
        } else {
            thread::sleep(Duration::from_micros(50));
        }
    }
    fn reset(&mut self) {
        self.0 = 0;
    }
}

// ------------------------------------------------------------------------------------------
// Log writer
// ------------------------------------------------------------------------------------------

/// The result of checking a finished run's log.
#[derive(Debug, Clone)]
pub struct Check {
    pub run_id: u64,
    pub ok: bool,
    pub detail: String,
    pub records: u64,
    pub bytes: u64,
    pub secs: f64,
}

#[derive(Debug, Clone, Default)]
pub struct LogStatus {
    /// The run being logged, its journal and its size.
    pub run_id: u64,
    pub path: Option<String>,
    pub records: u64,
    pub cap: u64,
    /// A finished run's log is being read back and checked.
    pub checking: Option<u64>,
    pub last: Option<Check>,
    pub checks: u64,
    pub checks_ok: u64,
}

struct Current {
    info: Arc<RunInfo>,
    path: PathBuf,
    writer: JournalWriter,
    records: u64,
    last_flush: Instant,
    last_sync: Instant,
    dirty: bool,
}

fn stamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

pub fn spawn_log_writer(
    mut rx: Consumer<LogMsg>,
    out_dir: PathBuf,
    cap_bytes: u64,
    status: Arc<Mutex<LogStatus>>,
    tel: Arc<Telemetry>,
) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("log-writer".into())
        .spawn(move || {
            let dir = out_dir.join("live");
            let _ = std::fs::create_dir_all(&dir);
            status.lock().unwrap().cap = cap_bytes;
            let mut cur: Option<Current> = None;
            let mut checks: Vec<thread::JoinHandle<()>> = Vec::new();
            let mut idle = Idle(0);
            let mut n = 0u64;
            loop {
                n += 1;
                if n.is_multiple_of(4_096) {
                    tel.record_thread_cpu(T_LOG);
                }
                let Ok(msg) = rx.pop() else {
                    // Idle: get buffered bytes to the disk so the size on screen is real.
                    if let Some(c) = &mut cur {
                        if c.dirty && c.last_flush.elapsed() >= Duration::from_millis(100) {
                            let _ = c.writer.flush();
                            c.last_flush = Instant::now();
                            c.dirty = false;
                            tel.log_bytes.store(c.writer.bytes(), Relaxed);
                            status.lock().unwrap().records = c.records;
                        }
                    }
                    idle.wait();
                    continue;
                };
                idle.reset();
                match msg {
                    LogMsg::Start(info) => {
                        if info.stress {
                            // Keep only the latest stress log: they are large.
                            remove_old_stress_logs(&dir);
                        }
                        let path = dir.join(format!(
                            "{}-seed{}-{}-run{}.journal",
                            info.scenario,
                            info.seed,
                            stamp(),
                            info.run_id
                        ));
                        let header = JournalHeader {
                            version: JOURNAL_VERSION,
                            log_format: log::FORMAT_VERSION,
                            mode: if info.stress { "stress" } else { "market" }.into(),
                            scenario: info.scenario.clone(),
                            seed: info.seed,
                            config: info.config.clone(),
                        };
                        tel.log_capped.store(0, Relaxed);
                        match JournalWriter::create(&path, &header) {
                            Ok(writer) => {
                                tel.log_bytes.store(writer.bytes(), Relaxed);
                                let mut s = status.lock().unwrap();
                                s.run_id = info.run_id;
                                s.path = Some(path.display().to_string());
                                s.records = 0;
                                cur = Some(Current {
                                    info,
                                    path,
                                    writer,
                                    records: 0,
                                    last_flush: Instant::now(),
                                    last_sync: Instant::now(),
                                    dirty: false,
                                });
                            }
                            Err(e) => eprintln!("log {}: {e}", path.display()),
                        }
                    }
                    LogMsg::Record(r) => {
                        if let Some(c) = &mut cur {
                            if let Err(e) = c.writer.append(&r) {
                                eprintln!("log write: {e}");
                            }
                            c.records += 1;
                            c.dirty = true;
                            if c.records.is_multiple_of(4_096) {
                                let bytes = c.writer.bytes();
                                tel.log_bytes.store(bytes, Relaxed);
                                if bytes >= cap_bytes {
                                    tel.log_capped.store(1, Relaxed);
                                }
                                if c.last_flush.elapsed() >= Duration::from_millis(100) {
                                    let _ = c.writer.flush();
                                    c.last_flush = Instant::now();
                                    c.dirty = false;
                                    status.lock().unwrap().records = c.records;
                                }
                                if c.last_sync.elapsed() >= Duration::from_secs(1) {
                                    let _ = c.writer.sync();
                                    c.last_sync = Instant::now();
                                }
                            }
                        }
                    }
                    LogMsg::End {
                        run_id,
                        records,
                        fills_hash,
                    } => {
                        if let Some(mut c) = cur.take() {
                            debug_assert_eq!(c.info.run_id, run_id);
                            let _ = c.writer.sync();
                            tel.log_bytes.store(c.writer.bytes(), Relaxed);
                            status.lock().unwrap().records = c.records;
                            debug_assert_eq!(c.records, records);
                            let primary = String::from_utf8_lossy(&fills_hash).to_string();
                            let st = status.clone();
                            st.lock().unwrap().checking = Some(run_id);
                            checks.retain(|h| !h.is_finished());
                            checks.push(thread::spawn(move || {
                                let t = Instant::now();
                                let bytes = std::fs::metadata(&c.path).map_or(0, |m| m.len());
                                let result = if c.info.stress {
                                    check_stress(&c.path, &primary)
                                } else {
                                    check_market(&c.path, &c.info, &primary)
                                };
                                let check = Check {
                                    run_id,
                                    ok: result.is_ok(),
                                    detail: result.unwrap_or_else(|e| e),
                                    records: c.records,
                                    bytes,
                                    secs: t.elapsed().as_secs_f64(),
                                };
                                eprintln!(
                                    "run {run_id} log ({} records, {:.1} MB): {} {}",
                                    check.records,
                                    bytes as f64 / 1e6,
                                    if check.ok { "OK" } else { "FAILED" },
                                    check.detail
                                );
                                let mut s = st.lock().unwrap();
                                s.checks += 1;
                                if check.ok {
                                    s.checks_ok += 1;
                                }
                                if s.checking == Some(run_id) {
                                    s.checking = None;
                                }
                                s.last = Some(check);
                            }));
                        }
                    }
                    LogMsg::Exit => {
                        for h in checks {
                            let _ = h.join();
                        }
                        return;
                    }
                }
            }
        })
        .expect("spawn log writer")
}

fn remove_old_stress_logs(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if name.starts_with("stress-") && name.ends_with(".journal") {
            let _ = std::fs::remove_file(e.path());
        }
    }
}

/// A market run: convert the journal to the batch log, re-simulate it, and replay it.
fn check_market(path: &Path, info: &RunInfo, primary: &str) -> Result<String, String> {
    let mut rd = JournalReader::open(path).map_err(|e| e.to_string())?;
    let mut records = Vec::new();
    while let Some(r) = rd.next_record().map_err(|e| e.to_string())? {
        records.push(WireRecord::from(&r));
    }
    let file = LogFile {
        format_version: log::FORMAT_VERSION,
        scenario: info.scenario.clone(),
        seed: info.seed,
        config_toml: info.config.clone(),
        records,
    };
    let bytes = log::encode(&file);
    let out = path.with_extension("log");
    std::fs::write(&out, &bytes).map_err(|e| format!("{}: {e}", out.display()))?;
    let _ = std::fs::remove_file(path);
    let resim = sim::replay_check(&bytes)?;
    let recs: Vec<LogRecord> = file.records.iter().map(Into::into).collect();
    let engine_hash = log::replay_fills_hash(&recs);
    if resim != primary || engine_hash != primary {
        return Err(format!(
            "fills hash differs: primary {}, re-simulated {}, replayed {}",
            &primary[..16],
            &resim[..16],
            &engine_hash[..16]
        ));
    }
    Ok(format!(
        "re-simulated from its log byte for byte; fills {}",
        &primary[..12]
    ))
}

/// A stress run: replay the journal through a fresh engine, and regenerate every record from
/// the seed (with the logged rate changes at their positions).
fn check_stress(path: &Path, primary: &str) -> Result<String, String> {
    let (_, replayed) = verify_stress_journal(path)?;
    if replayed != primary {
        return Err(format!(
            "fills hash differs: primary {}, replayed {}",
            &primary[..16],
            &replayed[..16]
        ));
    }
    Ok(format!(
        "replayed from disk and regenerated from the seed; fills {}",
        &primary[..12]
    ))
}

/// Read a stress journal from disk, check every record against the flow regenerated from its
/// seed (and its logged rate changes), and replay it through a fresh engine. Returns the record
/// count and the fills hash.
pub fn verify_stress_journal(path: &Path) -> Result<(u64, String), String> {
    let mut rd = JournalReader::open(path).map_err(|e| e.to_string())?;
    let rate: u64 = rd
        .header
        .config
        .trim()
        .strip_prefix("rate_per_s = ")
        .and_then(|s| s.parse().ok())
        .ok_or("stress header has no rate")?;
    let mut gen = Generator::new(rd.header.seed, rate);
    let mut engine = Engine::new();
    let mut hasher = FillsHasher::default();
    let mut out = Vec::new();
    let mut n = 0u64;
    while let Some(r) = rd.next_record().map_err(|e| e.to_string())? {
        n += 1;
        let (t_ns, agent, kind) = match r.kind {
            MsgKind::Config { param, value } => gen.config(param, value),
            _ => gen.next(),
        };
        let want = LogRecord {
            seq: n,
            t_ns,
            block: n - 1,
            agent,
            kind,
        };
        if r != want {
            return Err(format!("record {n} differs from the regenerated flow"));
        }
        engine.on_message_into(&r, &mut out);
        for ev in &out {
            if matches!(ev.kind, EventKind::Fill { .. }) {
                hasher.update(ev.seq, &ev.kind);
            }
        }
    }
    Ok((n, hasher.finish()))
}

// ------------------------------------------------------------------------------------------
// Standby
// ------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct StandbyStats {
    pub run_id: u64,
    pub records: u64,
    /// `on_message` latency over the last second, ns.
    pub p50: u64,
    pub p99: u64,
    pub p999: u64,
    /// Matching one block (the sum of its messages' `on_message`), over the last second, ns.
    pub block_p50: u64,
    pub block_p99: u64,
    pub block_size: f64,
    /// Compared with the primary at the last checkpoint it reached.
    pub in_sync: Option<bool>,
    pub checked_at: u64,
}

pub fn spawn_standby(
    mut rx: Consumer<StandbyMsg>,
    stats: Arc<Mutex<StandbyStats>>,
    tel: Arc<Telemetry>,
) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("standby".into())
        .spawn(move || {
            crate::qos::prefer_performance_cores();
            let new_hist = || Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).unwrap();
            let mut engine = Engine::new();
            let mut hasher = FillsHasher::default();
            let mut out = Vec::with_capacity(64);
            let mut processed = 0u64;
            let mut msg_hist = new_hist();
            let mut block_hist = new_hist();
            let (mut cur_block, mut block_ns, mut block_n) = (u64::MAX, 0u64, 0u64);
            let (mut blocks, mut block_msgs) = (0u64, 0u64);
            let mut window = Instant::now();
            let mut idle = Idle(0);
            loop {
                let Ok(msg) = rx.pop() else {
                    idle.wait();
                    continue;
                };
                idle.reset();
                match msg {
                    StandbyMsg::Reset(run_id) => {
                        engine = Engine::new();
                        hasher = FillsHasher::default();
                        processed = 0;
                        msg_hist.reset();
                        block_hist.reset();
                        (cur_block, block_ns, block_n, blocks, block_msgs) = (u64::MAX, 0, 0, 0, 0);
                        *stats.lock().unwrap() = StandbyStats {
                            run_id,
                            ..Default::default()
                        };
                    }
                    StandbyMsg::Record(r) => {
                        let t = Instant::now();
                        engine.on_message_into(&r, &mut out);
                        let ns = (t.elapsed().as_nanos() as u64).max(1);
                        msg_hist.saturating_record(ns);
                        for ev in &out {
                            if matches!(ev.kind, EventKind::Fill { .. }) {
                                hasher.update(ev.seq, &ev.kind);
                            }
                        }
                        // A block is a run of records with the same block number.
                        if r.block != cur_block {
                            if block_n > 0 {
                                block_hist.saturating_record(block_ns);
                                blocks += 1;
                                block_msgs += block_n;
                            }
                            (cur_block, block_ns, block_n) = (r.block, 0, 0);
                        }
                        block_ns += ns;
                        block_n += 1;
                        processed += 1;
                        if processed.is_multiple_of(65_536) {
                            tel.record_thread_cpu(T_STANDBY);
                        }
                    }
                    StandbyMsg::Checkpoint {
                        records,
                        fills_hash,
                    } => {
                        tel.record_thread_cpu(T_STANDBY);
                        let mut s = stats.lock().unwrap();
                        s.records = processed;
                        if processed == records {
                            s.in_sync = Some(hasher.clone().finish().as_bytes() == fills_hash);
                            s.checked_at = records;
                        }
                        if window.elapsed() >= Duration::from_secs(1) {
                            let q = |h: &Histogram<u64>, p| {
                                if h.is_empty() {
                                    0
                                } else {
                                    h.value_at_quantile(p)
                                }
                            };
                            s.p50 = q(&msg_hist, 0.5);
                            s.p99 = q(&msg_hist, 0.99);
                            s.p999 = q(&msg_hist, 0.999);
                            s.block_p50 = q(&block_hist, 0.5);
                            s.block_p99 = q(&block_hist, 0.99);
                            s.block_size = if blocks > 0 {
                                block_msgs as f64 / blocks as f64
                            } else {
                                0.0
                            };
                            msg_hist.reset();
                            block_hist.reset();
                            (blocks, block_msgs) = (0, 0);
                            window = Instant::now();
                        }
                    }
                    StandbyMsg::Exit => return,
                }
            }
        })
        .expect("spawn standby")
}

// ------------------------------------------------------------------------------------------
// UI bridge
// ------------------------------------------------------------------------------------------

/// Samples kept for each sparkline: 2 a second for a minute.
const HISTORY: usize = 120;
const SAMPLE: Duration = Duration::from_millis(500);

pub const SERIES: [&str; 22] = [
    "msgs",
    "target",
    "lat50",
    "lat99",
    "blk50",
    "blk99",
    "cpu",
    "cpu_engine",
    "cpu_producer",
    "cpu_log",
    "cpu_standby",
    "cpu_ui",
    "rss",
    "log_mb",
    "log_mbps",
    "ws_fps",
    "ring_input",
    "ring_log",
    "ring_standby",
    "wait_input",
    "wait_log",
    "wait_standby",
];

struct Sampler {
    start: Instant,
    last: Instant,
    proc_cpu: u64,
    thread_cpu: [u64; 5],
    log_bytes: u64,
    ws_frames: u64,
    waits: [u64; 3],
    ui_drops: u64,
    disk_free: u64,
    t: VecDeque<f64>,
    series: Vec<VecDeque<f64>>,
    /// The latest values, shown as numbers.
    now: Value,
    peak_msgs: f64,
    peak_run: u64,
    /// Highest ring fill since the last sample, per ring.
    ring_peak: [f64; 5],
}

impl Sampler {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            start: now,
            last: now,
            proc_cpu: sysinfo::process_cpu_ns(),
            thread_cpu: [0; 5],
            log_bytes: 0,
            ws_frames: 0,
            waits: [0; 3],
            ui_drops: 0,
            disk_free: 0,
            t: VecDeque::new(),
            series: SERIES.iter().map(|_| VecDeque::new()).collect(),
            now: json!({}),
            peak_msgs: 0.0,
            peak_run: 0,
            ring_peak: [0.0; 5],
        }
    }

    fn push(&mut self, name: &str, v: f64) {
        let i = SERIES
            .iter()
            .position(|s| *s == name)
            .expect("known series");
        let q = &mut self.series[i];
        q.push_back(v);
        while q.len() > HISTORY {
            q.pop_front();
        }
    }

    /// Take a sample if it's time. Returns the history message when one was taken.
    fn sample(
        &mut self,
        f: &Frame,
        sb: &StandbyStats,
        tel: &Telemetry,
        out_dir: &Path,
    ) -> Option<Value> {
        for (i, r) in f.rings.iter().enumerate() {
            self.ring_peak[i] = self.ring_peak[i].max(r.peak as f64 / r.cap.max(1) as f64);
        }
        if self.peak_run != f.run_id {
            self.peak_run = f.run_id;
            self.peak_msgs = 0.0;
        }
        self.peak_msgs = self.peak_msgs.max(f.msgs_per_s);
        let dt = self.last.elapsed();
        if dt < SAMPLE {
            return None;
        }
        self.last = Instant::now();
        let secs = dt.as_secs_f64();
        let pct = |d: u64| d as f64 / 1e9 / secs * 100.0;

        let proc_cpu = sysinfo::process_cpu_ns();
        let cpu = pct(proc_cpu.saturating_sub(self.proc_cpu));
        self.proc_cpu = proc_cpu;
        let mut threads = [0.0; 5];
        for (i, slot) in tel.thread_cpu.iter().enumerate() {
            let v = slot.load(Relaxed);
            // A thread restarted (a new producer) reports a smaller total: count from there.
            threads[i] = if v >= self.thread_cpu[i] {
                pct(v - self.thread_cpu[i]).min(100.0)
            } else {
                0.0
            };
            self.thread_cpu[i] = v;
        }
        let rss = sysinfo::rss_bytes() as f64 / 1e6;
        let log_bytes = tel.log_bytes.load(Relaxed);
        let mbps = log_bytes.saturating_sub(self.log_bytes) as f64 / 1e6 / secs;
        self.log_bytes = log_bytes;
        let ws = tel.ws_frames.load(Relaxed);
        let ws_fps = ws.saturating_sub(self.ws_frames) as f64 / secs;
        self.ws_frames = ws;
        let mut waits = [0.0; 3];
        for (i, w) in tel.waits.iter().enumerate() {
            let v = w.load(Relaxed);
            waits[i] = v.saturating_sub(self.waits[i]) as f64 / secs;
            self.waits[i] = v;
        }
        let drops = tel.ui_drops.load(Relaxed);
        let drops_s = drops.saturating_sub(self.ui_drops) as f64 / secs;
        self.ui_drops = drops;
        self.disk_free = sysinfo::disk_free_bytes(out_dir);

        let (lat50, lat99, blk50, blk99) = match f.state {
            State::Stress => (
                f.stress.e2e_p50,
                f.stress.e2e_p99,
                f.stress.batch_p50_ns,
                f.stress.batch_p99_ns,
            ),
            _ => (sb.p50, sb.p99, sb.block_p50, sb.block_p99),
        };
        let target = match f.state {
            State::Stress => f.stress.target_rate as f64,
            _ => 0.0,
        };

        self.t.push_back(self.start.elapsed().as_secs_f64());
        while self.t.len() > HISTORY {
            self.t.pop_front();
        }
        let ring = self.ring_peak;
        self.ring_peak = [0.0; 5];
        for (name, v) in [
            ("msgs", f.msgs_per_s),
            ("target", target),
            ("lat50", lat50 as f64),
            ("lat99", lat99 as f64),
            ("blk50", blk50 as f64),
            ("blk99", blk99 as f64),
            ("cpu", cpu),
            ("cpu_engine", threads[0]),
            ("cpu_producer", threads[1]),
            ("cpu_log", threads[2]),
            ("cpu_standby", threads[3]),
            ("cpu_ui", threads[4]),
            ("rss", rss),
            ("log_mb", log_bytes as f64 / 1e6),
            ("log_mbps", mbps),
            ("ws_fps", ws_fps),
            ("ring_input", ring[1] * 100.0),
            ("ring_log", ring[2] * 100.0),
            ("ring_standby", ring[3] * 100.0),
            ("wait_input", waits[0]),
            ("wait_log", waits[1]),
            ("wait_standby", waits[2]),
        ] {
            self.push(name, v);
        }
        self.now = json!({
            "cpu": cpu,
            "threads": THREADS.iter().zip(threads).map(|(n, v)| json!({"name": n, "pct": v})).collect::<Vec<_>>(),
            "rss_mb": rss,
            "log_mbps": mbps,
            "disk_free": self.disk_free,
            "ws_fps": ws_fps,
            "waits_s": waits,
            "ui_drops_s": drops_s,
        });
        let series: serde_json::Map<String, Value> = SERIES
            .iter()
            .zip(&self.series)
            .map(|(n, q)| (n.to_string(), json!(q.iter().collect::<Vec<_>>())))
            .collect();
        Some(json!({ "type": "history", "t": self.t.iter().collect::<Vec<_>>(), "s": series }))
    }
}

pub struct Bridge {
    pub snapshot: watch::Sender<Arc<str>>,
    pub history: watch::Sender<Arc<str>>,
    pub standby: Arc<Mutex<StandbyStats>>,
    pub log: Arc<Mutex<LogStatus>>,
    pub tel: Arc<Telemetry>,
    pub out_dir: PathBuf,
}

pub fn spawn_ui_bridge(mut rx: Consumer<UiMsg>, b: Bridge) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("ui-bridge".into())
        .spawn(move || {
            let mut sampler = Sampler::new();
            let mut idle = Idle(0);
            loop {
                let Ok(msg) = rx.pop() else {
                    idle.wait();
                    continue;
                };
                idle.reset();
                let f = match msg {
                    UiMsg::Frame(f) => f,
                    UiMsg::Exit => return,
                };
                b.tel.record_thread_cpu(T_UI);
                let sb = b.standby.lock().unwrap().clone();
                let lg = b.log.lock().unwrap().clone();
                if let Some(h) = sampler.sample(&f, &sb, &b.tel, &b.out_dir) {
                    b.history.send_replace(Arc::from(h.to_string()));
                }
                let snap = snapshot(&f, &sb, &lg, &sampler, &b.tel);
                b.snapshot.send_replace(Arc::from(snap.to_string()));
            }
        })
        .expect("spawn ui bridge")
}

/// Cash units to dollars: one unit is 1/10_000 of a tick·lot, $0.000001 with a $0.01 tick.
fn usd(cash_units: i128) -> f64 {
    cash_units as f64 / 1e6
}

/// Tick·lots to dollars.
fn usd_tl(tick_lots: i128) -> f64 {
    tick_lots as f64 / 100.0
}

fn px(ticks: i64) -> f64 {
    ticks as f64 / 100.0
}

const CLASS_KEYS: [&str; 5] = ["regular", "lp", "sniper", "protocol", "chain"];

fn snapshot(f: &Frame, sb: &StandbyStats, lg: &LogStatus, smp: &Sampler, tel: &Telemetry) -> Value {
    let s = |ns: u64| ns as f64 / 1e9;
    let spec = &f.spec;
    let state = match f.state {
        State::Setup => "setup",
        State::Market => "market",
        State::Stress => "stress",
    };
    let level = |l: &(i64, u64)| json!([px(l.0), l.1]);
    let book = json!({
        "bids": f.bids[..f.n_bids as usize].iter().map(level).collect::<Vec<_>>(),
        "asks": f.asks[..f.n_asks as usize].iter().map(level).collect::<Vec<_>>(),
        "levels": [f.levels.0, f.levels.1],
    });

    let lat = match f.state {
        State::Stress => json!({
            "kind": "producer → matched",
            "p50": f.stress.e2e_p50, "p99": f.stress.e2e_p99, "max": f.stress.e2e_max,
            "on_message_p50": sb.p50, "on_message_p99": sb.p99,
        }),
        _ => json!({
            "kind": "on_message",
            "p50": sb.p50, "p99": sb.p99, "max": sb.p999,
        }),
    };
    let blk = match f.state {
        State::Stress => json!({
            "kind": "batch", "p50": f.stress.batch_p50_ns, "p99": f.stress.batch_p99_ns,
            "size": f.stress.batch_mean,
        }),
        _ => json!({
            "kind": "block", "p50": sb.block_p50, "p99": sb.block_p99, "size": sb.block_size,
        }),
    };
    let rings: Vec<Value> = f
        .rings
        .iter()
        .zip(RING_NAMES)
        .map(|(r, name)| json!({ "name": name, "used": r.used, "cap": r.cap, "peak": r.peak }))
        .collect();
    let sys = json!({
        "msgs_s": f.msgs_per_s,
        "peak_msgs_s": smp.peak_msgs,
        "records": f.records,
        "fills": f.fills,
        "lat": lat,
        "blk": blk,
        "now": smp.now,
        "rings": rings,
        "log": {
            "path": lg.path,
            "bytes": tel.log_bytes.load(Relaxed),
            "records": lg.records,
            "cap": lg.cap,
            "capped": tel.log_capped.load(Relaxed) != 0,
            "checking": lg.checking,
            "last": lg.last.as_ref().map(|c| json!({
                "run": c.run_id, "ok": c.ok, "detail": c.detail, "records": c.records,
                "bytes": c.bytes, "secs": c.secs,
            })),
            "checks": lg.checks,
            "checks_ok": lg.checks_ok,
        },
        "standby": { "run": sb.run_id, "records": sb.records, "in_sync": sb.in_sync, "checked_at": sb.checked_at },
        "fills_hash": String::from_utf8_lossy(&f.fills_hash[..12]),
        "stress": {
            "target": f.stress.target_rate,
            "produced": tel.produced.load(Relaxed),
            "resting": f.stress.orders_resting,
        },
    });

    let mut v = json!({
        "type": "snapshot",
        "state": state,
        "scenarios": SCENARIOS.iter().map(|s| s.0).collect::<Vec<_>>(),
        "spec": {
            "mode": if spec.stress { "stress" } else { "market" },
            "rate": spec.rate,
            "scenario": SCENARIOS[spec.scenario as usize].0,
            "rule": spec.rule.name(),
            "block_ms": spec.block_ms,
            "seed": spec.seed,
            "speed": spec.speed_milli as f64 / 1000.0,
        },
        "run": {
            "id": f.run_id,
            "clock_s": s(f.clock_ns),
            "end_s": s(f.end_ns),
            "done": f.done,
            "stopped": f.stopped,
        },
        "sys": sys,
        "book": book,
    });

    if f.state == State::Market {
        let m = &f.market;
        let chart: Vec<Value> = m.chart[..m.n_chart as usize]
            .iter()
            .map(|&(p, mid)| {
                if mid == i64::MIN {
                    json!([px(p), null])
                } else {
                    json!([px(p), mid as f64 / 200.0])
                }
            })
            .collect();
        let mid = match (f.n_bids, f.n_asks) {
            (0, _) | (_, 0) => None,
            _ => Some((f.bids[0].0 + f.asks[0].0) as f64 / 200.0),
        };
        let feed: Vec<Value> = m.races[..m.n_races as usize]
            .iter()
            .rev()
            .filter(|r| r.outcome != Outcome::Open)
            .map(|r| {
                json!({
                    "t": s(r.end_t_ns),
                    "ms": r.end_t_ns.saturating_sub(r.start_t_ns) as f64 / 1e6,
                    "winner": match r.outcome {
                        Outcome::Lp => "LP",
                        Outcome::Sniper => "sniper",
                        Outcome::Other => "trader",
                        Outcome::Open => "open",
                    },
                    "stake": r.staleness.max(0) as f64 * r.qty as f64 * 0.01,
                })
            })
            .collect();
        let rs = m.race_stats;
        let idx = |c: AccountClass| AccountClass::ALL.iter().position(|&x| x == c).unwrap();
        let (lp, sn, rg) = (
            idx(AccountClass::Lp),
            idx(AccountClass::Sniper),
            idx(AccountClass::Regular),
        );
        let classes: Vec<Value> = AccountClass::ALL
            .iter()
            .enumerate()
            .map(|(i, _)| {
                let take: i128 = (0..5).map(|j| m.transfer[j][i]).sum();
                json!({
                    "key": CLASS_KEYS[i],
                    "pnl": usd(m.pnl[i]),
                    "take_at_fill": usd_tl(take),
                    "taker_fees": usd(m.fees[i].0),
                    "priority_fees": usd(m.fees[i].1),
                })
            })
            .collect();
        let sum_fees = |k: usize| {
            m.fees
                .iter()
                .map(|f| if k == 0 { f.0 } else { f.1 })
                .sum::<i128>()
        };
        v["market"] = json!({
            "scenario": SCENARIOS[spec.scenario as usize].0,
            "rule": m.rule.name(),
            "block_ms": spec.block_ms,
            "load": m.load,
            "speed": spec.speed_milli as f64 / 1000.0,
            "seeking": m.seeking.map(s),
            "hidden": m.hidden,
            "weekend": m.weekend.map(|(c, r)| json!([s(c), s(r)])),
            "nasdaq": px(m.true_px),
            "mid": mid,
            "chart": { "t0": s(m.chart_t0), "t1": s(m.chart_t1), "n": CHART, "points": chart },
            "races": {
                "feed": feed,
                "sniper": rs.sniper_won, "lp": rs.lp_won, "other": rs.other_filled,
            },
            "block": m.block,
            "counts": { "news": m.counts.news, "cancels": m.counts.cancels, "ticks": m.counts.ticks, "configs": m.counts.configs },
        });
        v["profits"] = json!({
            "lp_to_sniper": usd_tl(m.transfer[lp][sn]),
            "regular_to_lp": usd_tl(m.transfer[rg][lp]),
            "regular_to_sniper": usd_tl(m.transfer[rg][sn]),
            "sniper_races": rs.sniper_won,
            "lp_races": rs.lp_won,
            "taker_fees": usd(sum_fees(0)),
            "priority_fees": usd(sum_fees(1)),
            "sniper_priority_fees": usd(m.fees[sn].1),
            "lp_priority_fees": usd(m.fees[lp].1),
            "classes": classes,
            "sum": usd(m.pnl.iter().sum::<i128>()),
            "mark": px(m.true_px),
        });
    }
    v
}
