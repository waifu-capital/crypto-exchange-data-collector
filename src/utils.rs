use std::fs;
use std::time::{Duration, SystemTime};
use tracing::{info, warn};

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
