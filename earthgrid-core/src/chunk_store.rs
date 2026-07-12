//! Content-Addressable Storage (CAS) — SHA-256 hash-based chunk storage.
//!
//! Every chunk is identified by its SHA-256 hash. Two-level directory structure
//! for efficient filesystem access: `ab/cd/abcd1234...`
//!
//! Uses SQLite for chunk indexing — instant startup instead of filesystem walk.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

use crate::error::{EarthGridError, Result};

/// Stats tracking for the chunk store (serializable snapshot for persistence).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreStats {
    pub started: f64,
    pub chunks_served: u64,
    pub bytes_served: u64,
    pub chunks_stored: u64,
    pub bytes_ingested: u64,
    pub requests_total: u64,
}

impl Default for StoreStats {
    fn default() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        Self {
            started: now,
            chunks_served: 0,
            bytes_served: 0,
            chunks_stored: 0,
            bytes_ingested: 0,
            requests_total: 0,
        }
    }
}

/// Content-addressed chunk store with SHA-256 hashing.
///
/// Stores raw byte chunks on disk, addressed by their SHA-256 hash.
/// Uses SQLite for fast indexing (chunk count, total bytes, listing).
/// Provides integrity verification, storage limits, and usage stats.
///
/// Stats are tracked with atomics — no per-op file writes. Use `flush_stats()`
/// to persist a snapshot to disk (call periodically or on shutdown).
pub struct ChunkStore {
    store_path: PathBuf,
    limit_bytes: u64,
    started: f64,
    stats_path: PathBuf,
    db: Connection,
    // Atomic counters — updated on every op, flushed to disk on demand
    chunks_served: AtomicU64,
    bytes_served: AtomicU64,
    chunks_stored: AtomicU64,
    bytes_ingested: AtomicU64,
    requests_total: AtomicU64,
    cached_total_bytes: AtomicU64,
    cached_chunk_count: AtomicU64,
}

impl ChunkStore {
    /// Create a new ChunkStore at the given path.
    ///
    /// On first run, migrates existing chunks from filesystem into SQLite index.
    /// Subsequent starts are instant (no filesystem walk needed).
    ///
    /// # Arguments
    /// * `store_path` - Directory for chunk storage
    /// * `limit_gb` - Maximum storage in GB (0 = unlimited)
    pub fn new(store_path: &Path, limit_gb: f64) -> Result<Self> {
        fs::create_dir_all(store_path)?;
        let stats_path = store_path
            .parent()
            .unwrap_or(store_path)
            .join("stats.json");
        let persisted_stats = Self::load_stats(&stats_path);
        let limit_bytes = if limit_gb > 0.0 {
            (limit_gb * 1024.0 * 1024.0 * 1024.0) as u64
        } else {
            0
        };

        // Open SQLite DB next to the store directory
        let db_path = store_path
            .parent()
            .unwrap_or(store_path)
            .join("chunks.db");
        let db = Connection::open(&db_path)?;

        // WAL mode for better concurrent read performance
        db.execute_batch("PRAGMA journal_mode=WAL;")?;

        // Create table if not exists
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS chunks (
                hash TEXT PRIMARY KEY,
                size_bytes INTEGER NOT NULL,
                created_at INTEGER NOT NULL
            );",
        )?;

        let mut store = Self {
            store_path: store_path.to_path_buf(),
            limit_bytes,
            started: persisted_stats.started,
            stats_path,
            db,
            chunks_served: AtomicU64::new(persisted_stats.chunks_served),
            bytes_served: AtomicU64::new(persisted_stats.bytes_served),
            chunks_stored: AtomicU64::new(persisted_stats.chunks_stored),
            bytes_ingested: AtomicU64::new(persisted_stats.bytes_ingested),
            requests_total: AtomicU64::new(persisted_stats.requests_total),
            cached_total_bytes: AtomicU64::new(0),
            cached_chunk_count: AtomicU64::new(0),
        };

        // Migrate from filesystem if DB is empty but store has files
        store.migrate_from_filesystem()?;

        // Seed cached counters from DB
        let count = Self::db_chunk_count(&store.db);
        let bytes = Self::db_total_bytes(&store.db);
        store.cached_chunk_count.store(count as u64, Ordering::SeqCst);
        store.cached_total_bytes.store(bytes, Ordering::SeqCst);

        println!(
            "📦 ChunkStore: {} chunks, {:.1} GB (SQLite index)",
            count,
            bytes as f64 / 1_073_741_824.0
        );

        Ok(store)
    }

    /// Persist current stat counters to stats.json. Called periodically and on shutdown.
    /// NOT called on every get/put — counters are updated atomically in memory.
    pub fn flush_stats(&self) {
        let snapshot = self.stats();
        if let Ok(json) = serde_json::to_string_pretty(&snapshot) {
            let _ = fs::write(&self.stats_path, json);
        }
    }

    /// Get a snapshot of current store statistics (from atomic counters).
    pub fn stats_snapshot(&self) -> StoreStats {
        StoreStats {
            started: self.started,
            chunks_served: self.chunks_served.load(Ordering::SeqCst),
            bytes_served: self.bytes_served.load(Ordering::SeqCst),
            chunks_stored: self.chunks_stored.load(Ordering::SeqCst),
            bytes_ingested: self.bytes_ingested.load(Ordering::SeqCst),
            requests_total: self.requests_total.load(Ordering::SeqCst),
        }
    }

    /// Calculate byte hash of data.
    pub fn hash_bytes(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hex::encode(hasher.finalize())
    }

    /// Store a chunk. Returns its SHA-256 hash.
    ///
    /// If the chunk already exists, this is a no-op (content-addressed dedup).
    pub fn put(&mut self, data: &[u8]) -> Result<String> {
        let hash = Self::hash_bytes(data);

        // Fast dedup check via chunk path
        let path = self.chunk_path(&hash);
        if path.exists() {
            return Ok(hash);
        }

        // Storage limit check (uses cached total, falls back to DB on overflow)
        if self.limit_bytes > 0 {
            let current = self.cached_total_bytes.load(Ordering::SeqCst);
            if current + data.len() as u64 > self.limit_bytes {
                return Err(EarthGridError::StorageLimitExceeded(format!(
                    "{:.1} GB",
                    self.limit_bytes as f64 / 1024.0 / 1024.0 / 1024.0
                )));
            }
        }

        // Write chunk to disk atomically (temp file → rename, crash-safe)
        let tmp_path = path.with_extension("tmp");
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&tmp_path, data)?;
        fs::rename(&tmp_path, &path)?;

        // Insert into SQLite index
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        self.db.execute(
            "INSERT OR IGNORE INTO chunks (hash, size_bytes, created_at) VALUES (?1, ?2, ?3)",
            params![hash, data.len() as i64, now],
        )?;

        // Update atomic counters (no disk write — flush_stats() persists later)
        self.chunks_stored.fetch_add(1, Ordering::SeqCst);
        self.bytes_ingested.fetch_add(data.len() as u64, Ordering::SeqCst);
        self.cached_total_bytes.fetch_add(data.len() as u64, Ordering::SeqCst);
        self.cached_chunk_count.fetch_add(1, Ordering::SeqCst);

        Ok(hash)
    }

    /// Retrieve a chunk by its SHA-256 hash.
    ///
    /// Returns `None` if the chunk doesn't exist.
    pub fn get(&self, hash: &str) -> Result<Option<Vec<u8>>> {
        let path = self.chunk_path(hash);
        if !path.exists() {
            return Ok(None);
        }
        let data = fs::read(&path)?;

        // Update atomic counters (no disk write — flush_stats() persists later)
        self.chunks_served.fetch_add(1, Ordering::SeqCst);
        self.bytes_served.fetch_add(data.len() as u64, Ordering::SeqCst);
        self.requests_total.fetch_add(1, Ordering::SeqCst);

        Ok(Some(data))
    }

    /// Check if a chunk exists.
    pub fn has(&self, hash: &str) -> bool {
        self.chunk_path(hash).exists()
    }

    /// Get the size in bytes of a single chunk from the DB index.
    pub fn chunk_size(&self, hash: &str) -> Result<u64> {
        let size: i64 = self.db.query_row(
            "SELECT size_bytes FROM chunks WHERE hash = ?1",
            rusqlite::params![hash],
            |r| r.get(0),
        )?;
        Ok(size as u64)
    }

    /// Verify a chunk's integrity against its hash.
    ///
    /// Returns `Ok(true)` if valid, `Err(IntegrityViolation)` if corrupted.
    pub fn verify(&self, hash: &str) -> Result<bool> {
        let path = self.chunk_path(hash);
        if !path.exists() {
            return Err(EarthGridError::ChunkNotFound(hash.to_string()));
        }
        let data = fs::read(&path)?;
        let actual = Self::hash_bytes(&data);
        if actual != hash {
            return Err(EarthGridError::IntegrityViolation {
                expected: hash.to_string(),
                actual,
            });
        }
        Ok(true)
    }

    /// Delete a chunk by hash. Returns `true` if it was deleted.
    pub fn delete(&mut self, hash: &str) -> Result<bool> {
        let path = self.chunk_path(hash);
        if path.exists() {
            let size = self.chunk_size(hash).unwrap_or(0);
            fs::remove_file(&path)?;
            self.db.execute(
                "DELETE FROM chunks WHERE hash = ?1",
                params![hash],
            )?;
            self.cached_total_bytes.fetch_sub(size, Ordering::SeqCst);
            self.cached_chunk_count.fetch_sub(1, Ordering::SeqCst);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Remove empty parent directories (best-effort cleanup after deletes).
    pub fn cleanup_dirs(&self) {
        // Remove empty two-level directories
        for entry in fs::read_dir(&self.store_path).into_iter().flatten().flatten() {
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                for inner in fs::read_dir(entry.path()).into_iter().flatten().flatten() {
                    if inner.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                        let _ = fs::remove_dir(inner.path());
                    }
                }
                let _ = fs::remove_dir(entry.path());
            }
        }
    }

    /// List all chunk hashes (with an optional limit).
    pub fn list_chunks(&self, limit: Option<usize>) -> Vec<String> {
        let limit_clause = limit.map(|l| format!(" LIMIT {}", l)).unwrap_or_default();
        let sql = format!("SELECT hash FROM chunks ORDER BY created_at DESC{}", limit_clause);
        match self.db.prepare(&sql) {
            Ok(mut stmt) => stmt
                .query_map([], |row| row.get::<_, String>(0))
                .into_iter()
                .flatten()
                .filter_map(|r| r.ok())
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Number of chunks in the store (from cached counter, not DB scan).
    pub fn chunk_count(&self) -> usize {
        self.cached_chunk_count.load(Ordering::SeqCst) as usize
    }

    /// Total bytes used by all chunks (from cached counter, not DB scan).
    pub fn total_bytes(&self) -> u64 {
        self.cached_total_bytes.load(Ordering::SeqCst)
    }

    /// Get current store statistics (snapshot from atomics).
    pub fn stats(&self) -> StoreStats {
        self.stats_snapshot()
    }

    // --- Private ---

    fn chunk_path(&self, hash: &str) -> PathBuf {
        self.store_path
            .join(&hash[..2])
            .join(&hash[2..4])
            .join(hash)
    }

    fn db_chunk_count(db: &Connection) -> usize {
        db.query_row("SELECT count(*) FROM chunks", [], |row| row.get::<_, i64>(0))
            .unwrap_or(0) as usize
    }

    fn db_total_bytes(db: &Connection) -> u64 {
        db.query_row(
            "SELECT COALESCE(sum(size_bytes), 0) FROM chunks",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0) as u64
    }

    fn load_stats(path: &Path) -> StoreStats {
        if path.exists() {
            if let Ok(data) = fs::read_to_string(path) {
                if let Ok(stats) = serde_json::from_str(&data) {
                    return stats;
                }
            }
        }
        StoreStats::default()
    }

    /// Migrate chunks from filesystem directory into the SQLite index.
    ///
    /// Only runs if the DB is empty and the store directory contains chunks.
    fn migrate_from_filesystem(&mut self) -> Result<()> {
        let count: i64 = self.db.query_row(
            "SELECT count(*) FROM chunks",
            [],
            |row| row.get(0),
        ).unwrap_or(0);

        if count > 0 {
            return Ok(()); // DB already populated
        }

        // Check if store directory exists
        if !self.store_path.exists() {
            return Ok(());
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let tx = self.db.transaction()?;

        let mut migrated = 0usize;
        for entry in WalkDir::new(&self.store_path)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_file() {
                if let Some(name) = entry.path().file_name().and_then(|n| n.to_str()) {
                    // Skip non-hash files (tmp files, etc.)
                    if name.len() == 64 && name.chars().all(|c| c.is_ascii_hexdigit()) {
                        let size = entry.metadata().map(|m| m.len()).unwrap_or(0) as i64;
                        tx.execute(
                            "INSERT OR IGNORE INTO chunks (hash, size_bytes, created_at) VALUES (?1, ?2, ?3)",
                            params![name, size, now],
                        )?;
                        migrated += 1;
                    }
                }
            }
        }

        tx.commit()?;

        println!("✅ Migration complete: {} chunks indexed", migrated);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_put_and_get() {
        let dir = tempdir().unwrap();
        let store_path = dir.path().join("store");
        let mut store = ChunkStore::new(&store_path, 0.0).unwrap();

        let data = b"hello earthgrid chunk store";
        let hash = store.put(data).unwrap();

        let retrieved = store.get(&hash).unwrap().unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn test_dedup() {
        let dir = tempdir().unwrap();
        let store_path = dir.path().join("store");
        let mut store = ChunkStore::new(&store_path, 0.0).unwrap();

        let hash1 = store.put(b"same data").unwrap();
        let hash2 = store.put(b"same data").unwrap();
        assert_eq!(hash1, hash2, "dedup should return same hash");
    }

    #[test]
    fn test_verify() {
        let dir = tempdir().unwrap();
        let store_path = dir.path().join("store");
        let mut store = ChunkStore::new(&store_path, 0.0).unwrap();

        let hash = store.put(b"verify me").unwrap();
        assert!(store.verify(&hash).unwrap());
        assert!(store.verify("nonexistent").is_err());
    }

    #[test]
    fn test_delete() {
        let dir = tempdir().unwrap();
        let store_path = dir.path().join("store");
        let mut store = ChunkStore::new(&store_path, 0.0).unwrap();

        let hash = store.put(b"delete me").unwrap();
        assert!(store.has(&hash));
        assert!(store.delete(&hash).unwrap());
        assert!(!store.has(&hash));
    }

    #[test]
    fn test_limit() {
        let dir = tempdir().unwrap();
        let store_path = dir.path().join("store");
        let mut store = ChunkStore::new(&store_path, 0.000001).unwrap(); // ~1 KB

        store.put(b"ok").unwrap();
        let big = vec![0u8; 2000];
        let err = store.put(&big).unwrap_err();
        assert!(matches!(err, EarthGridError::StorageLimitExceeded(_)));
    }

    #[test]
    fn test_stats_counters() {
        let dir = tempdir().unwrap();
        let store_path = dir.path().join("store");
        let mut store = ChunkStore::new(&store_path, 0.0).unwrap();

        store.put(b"chunk1").unwrap();
        store.put(b"chunk2").unwrap();
        let hash3 = store.put(b"chunk3").unwrap();
        store.get(&hash3).unwrap();

        let s = store.stats();
        assert_eq!(s.chunks_stored, 3);
        assert!(s.chunks_served >= 1);
        assert_eq!(store.chunk_count(), 3);
    }

    #[test]
    fn test_flush_and_reload() {
        let dir = tempdir().unwrap();
        let store_path = dir.path().join("store");
        {
            let mut store = ChunkStore::new(&store_path, 0.0).unwrap();
            store.put(b"persist").unwrap();
            store.flush_stats();
        }
        // Reload — stats should persist
        let store = ChunkStore::new(&store_path, 0.0).unwrap();
        let s = store.stats();
        assert_eq!(s.chunks_stored, 1);
    }

    #[test]
    fn test_atomic_write() {
        let dir = tempdir().unwrap();
        let store_path = dir.path().join("store");
        let mut store = ChunkStore::new(&store_path, 0.0).unwrap();

        let hash = store.put(b"atomic write").unwrap();
        let path = store.chunk_path(&hash);
        let tmp = path.with_extension("tmp");
        // Temp file should not exist after put (renamed)
        assert!(!tmp.exists());
        assert!(path.exists());

        let data = store.get(&hash).unwrap().unwrap();
        assert_eq!(data, b"atomic write");
    }
}