use std::sync::atomic::{AtomicU64, Ordering};

/// Counter for dropped snapshots due to channel backpressure
pub static DROPPED_SNAPSHOTS: AtomicU64 = AtomicU64::new(0);

/// Increment and return the new dropped count
pub fn increment_dropped_snapshots() -> u64 {
    DROPPED_SNAPSHOTS.fetch_add(1, Ordering::Relaxed) + 1
}

/// Get the current dropped count
pub fn get_dropped_snapshots() -> u64 {
    DROPPED_SNAPSHOTS.load(Ordering::Relaxed)
}

/// Macro for printing messages with UTC timestamp prefix
#[macro_export]
macro_rules! println_with_timestamp {
    ($($arg:tt)*) => {{
        let now = chrono::Utc::now();
        print!("[{}] ", now.to_rfc3339());
        println!($($arg)*);
    }}
}
