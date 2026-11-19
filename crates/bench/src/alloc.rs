//! A counting global allocator: every `alloc`/`alloc_zeroed` and every `realloc` is counted, so
//! a measured section can report allocations per message (spec section 10, "Memory").

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

pub struct Counting;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static REALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        BYTES.fetch_add(layout.size() as u64, Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        BYTES.fetch_add(layout.size() as u64, Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        REALLOCS.fetch_add(1, Relaxed);
        BYTES.fetch_add(new_size.saturating_sub(layout.size()) as u64, Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

/// Counter values at one moment.
#[derive(Debug, Clone, Copy, Default)]
pub struct Snapshot {
    pub allocs: u64,
    pub reallocs: u64,
    pub bytes: u64,
}

pub fn snapshot() -> Snapshot {
    Snapshot {
        allocs: ALLOCS.load(Relaxed),
        reallocs: REALLOCS.load(Relaxed),
        bytes: BYTES.load(Relaxed),
    }
}

impl std::ops::Sub for Snapshot {
    type Output = Snapshot;
    fn sub(self, o: Snapshot) -> Snapshot {
        Snapshot {
            allocs: self.allocs - o.allocs,
            reallocs: self.reallocs - o.reallocs,
            bytes: self.bytes - o.bytes,
        }
    }
}
