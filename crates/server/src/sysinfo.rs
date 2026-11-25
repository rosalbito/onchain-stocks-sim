//! What the System page shows about the process: CPU time (the process and each named thread),
//! resident memory, and free disk space. Each thread records its own CPU time into a shared
//! slot; the UI bridge turns deltas into percentages.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// The threads whose CPU the dashboard shows, in slot order.
pub const THREADS: [&str; 5] = ["engine", "producer", "log", "standby", "ui"];
pub const T_ENGINE: usize = 0;
pub const T_PRODUCER: usize = 1;
pub const T_LOG: usize = 2;
pub const T_STANDBY: usize = 3;
pub const T_UI: usize = 4;

/// CPU time of the calling thread, ns.
pub fn thread_cpu_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid out-pointer; the clock id is a constant the platform defines.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    if rc != 0 {
        return 0;
    }
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

/// User + system CPU time of the whole process, ns.
pub fn process_cpu_ns() -> u64 {
    // SAFETY: an all-zero rusage is a valid value to be overwritten.
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: `ru` is a valid out-pointer.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) } != 0 {
        return 0;
    }
    let tv = |t: libc::timeval| t.tv_sec as u64 * 1_000_000_000 + t.tv_usec as u64 * 1_000;
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

/// Resident set size, bytes.
#[cfg(target_os = "macos")]
pub fn rss_bytes() -> u64 {
    // SAFETY: an all-zero proc_taskinfo is valid; proc_pidinfo fills at most `size` bytes.
    unsafe {
        let mut info: libc::proc_taskinfo = std::mem::zeroed();
        let size = std::mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
        let n = libc::proc_pidinfo(
            libc::getpid(),
            libc::PROC_PIDTASKINFO,
            0,
            &mut info as *mut _ as *mut libc::c_void,
            size,
        );
        if n == size {
            info.pti_resident_size
        } else {
            0
        }
    }
}

#[cfg(target_os = "linux")]
pub fn rss_bytes() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: u64 = statm
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    // SAFETY: sysconf has no preconditions.
    pages * unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn rss_bytes() -> u64 {
    0
}

/// Free space for an unprivileged user on the volume holding `path`, bytes.
pub fn disk_free_bytes(path: &Path) -> u64 {
    let Ok(c) = std::ffi::CString::new(path.as_os_str().to_string_lossy().as_bytes()) else {
        return 0;
    };
    // SAFETY: an all-zero statvfs is valid; `c` is a NUL-terminated path.
    unsafe {
        let mut st: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c.as_ptr(), &mut st) != 0 {
            return 0;
        }
        // The field types differ by platform (u32 on macOS, u64 on Linux).
        #[allow(clippy::unnecessary_cast)]
        let free = st.f_bavail as u64 * st.f_frsize as u64;
        free
    }
}

/// Counters shared by every thread. All relaxed: they feed a display, not a decision.
#[derive(Default)]
pub struct Telemetry {
    /// CPU time of each thread in `THREADS`, ns, as last recorded by that thread.
    pub thread_cpu: [AtomicU64; 5],
    /// Bytes the log writer has written for the current run.
    pub log_bytes: AtomicU64,
    /// Set when the current run's log reached the cap.
    pub log_capped: AtomicU64,
    /// Times a producer found a ring full and had to wait, per ring (input, log, standby).
    pub waits: [AtomicU64; 3],
    /// Frames the UI ring dropped because the bridge was behind.
    pub ui_drops: AtomicU64,
    /// Text frames sent to websocket clients.
    pub ws_frames: AtomicU64,
    /// Messages the stress producer has handed to the input ring.
    pub produced: AtomicU64,
}

impl Telemetry {
    pub fn record_thread_cpu(&self, slot: usize) {
        self.thread_cpu[slot].store(thread_cpu_ns(), Relaxed);
    }
}

pub const W_INPUT: usize = 0;
pub const W_LOG: usize = 1;
pub const W_STANDBY: usize = 2;
