use std::sync::atomic::{AtomicU64, Ordering};

/// Counter for dropped snapshots due to channel backpressure
static DROPPED_SNAPSHOTS: AtomicU64 = AtomicU64::new(0);

/// Increment and return the new dropped count
pub fn increment_dropped_snapshots() -> u64 {
    DROPPED_SNAPSHOTS.fetch_add(1, Ordering::Relaxed) + 1
}

/// Get the current dropped count
pub fn get_dropped_snapshots() -> u64 {
    DROPPED_SNAPSHOTS.load(Ordering::Relaxed)
}
