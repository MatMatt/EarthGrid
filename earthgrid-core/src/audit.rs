//! Audit log — tracks all mutations with timestamps and IPs.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// A single audit log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    pub ts: String,
    pub action: String,
    pub detail: String,
    pub ip: String,
    pub ok: bool,
}

/// Append-only audit log.
pub struct AuditLog {
    path: PathBuf,
}

impl AuditLog {
    /// Create a new audit log at the given path.
    pub fn new(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
        }
    }

    /// Log an action.
    pub fn log(&self, action: &str, detail: &str, ip: &str, success: bool) {
        let entry = AuditEntry {
            ts: chrono::Utc::now().to_rfc3339(),
            action: action.to_string(),
            detail: detail.to_string(),
            ip: ip.to_string(),
            ok: success,
        };
        if let Ok(json) = serde_json::to_string(&entry) {
            if let Ok(mut f) = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
            {
                let _ = writeln!(f, "{}", json);
            }
        }
    }

    /// Read recent entries.
    pub fn recent(&self, limit: usize) -> Vec<AuditEntry> {
        let mut entries = Vec::new();
        if let Ok(content) = fs::read_to_string(&self.path) {
            for line in content.lines().rev().take(limit) {
                if let Ok(entry) = serde_json::from_str(line) {
                    entries.push(entry);
                }
            }
        }
        entries.reverse();
        entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_audit_log() {
        let dir = TempDir::new().unwrap();
        let log = AuditLog::new(&dir.path().join("audit.jsonl"));
        log.log("ingest", "item-1", "127.0.0.1", true);
        log.log("auth_fail", "bad key", "10.0.0.1", false);

        let entries = log.recent(10);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].action, "ingest");
        assert!(!entries[1].ok);
    }
}
