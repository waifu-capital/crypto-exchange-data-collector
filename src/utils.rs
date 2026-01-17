use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};
use tracing::{info, warn};

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

/// Delete log files older than the specified retention period
pub fn cleanup_old_logs(logs_dir: &str, retention_days: u64) {
    let retention_duration = Duration::from_secs(retention_days * 24 * 60 * 60);
    let now = SystemTime::now();

    let entries = match fs::read_dir(logs_dir) {
        Ok(e) => e,
        Err(e) => {
            warn!(error = %e, "Failed to read logs directory for cleanup");
            return;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let metadata = match fs::metadata(&path) {
            Ok(m) => m,
            Err(_) => continue,
        };

        let modified = match metadata.modified() {
            Ok(m) => m,
            Err(_) => continue,
        };

        if let Ok(age) = now.duration_since(modified) {
            if age > retention_duration {
                if let Err(e) = fs::remove_file(&path) {
                    warn!(error = %e, path = %path.display(), "Failed to delete old log file");
                } else {
                    info!(path = %path.display(), "Deleted old log file");
                }
            }
        }
    }
}
