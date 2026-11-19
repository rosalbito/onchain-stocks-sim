//! Keep the engine thread and the standby on performance cores.
//!
//! macOS has no thread affinity, so architecture section 6's core pinning can't be done
//! literally. On Apple silicon the scheduler keeps user-interactive threads on the performance
//! cores; lower classes may run on efficiency cores when the machine is busy. Elsewhere this is
//! a no-op. (The same helper is in `crates/bench`.)

#[cfg(target_os = "macos")]
pub fn prefer_performance_cores() {
    /// `QOS_CLASS_USER_INTERACTIVE` from `<sys/qos.h>`.
    const USER_INTERACTIVE: u32 = 0x21;
    extern "C" {
        fn pthread_set_qos_class_self_np(qos_class: u32, relative_priority: i32) -> i32;
    }
    // SAFETY: sets the calling thread's own QoS class; no pointers are involved.
    unsafe {
        pthread_set_qos_class_self_np(USER_INTERACTIVE, 0);
    }
}

#[cfg(not(target_os = "macos"))]
pub fn prefer_performance_cores() {}
