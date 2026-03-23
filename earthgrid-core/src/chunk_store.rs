//! Content-Addressable Storage (CAS) — SHA-256 hash-based chunk storage.
//!
//! Every chunk is identified by its SHA-256 hash. Two-level directory structure
//! for efficient filesystem access: `ab/cd/abcd1234...`
//!
//! Uses SQLite for chunk indexing — instant startup instead of filesystem walk.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

use crate::error::{EarthGridError, Result};

/// Stats tracking for the chunk store.
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
pub struct ChunkStore {
    store_path: PathBuf,
    limit_bytes: u64,
    stats: StoreStats,
    stats_path: PathBuf,
    db: Connection,
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
        let stats = Self::load_stats(&stats_path);
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
        let db = Connection::open(&db_path)
            ?;

        // WAL mode for better concurrent read performance
        db.execute_batch("PRAGMA journal_mode=WAL;")
            ?;

        // Create table if not exists
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS chunks (
                hash TEXT PRIMARY KEY,
                size_bytes INTEGER NOT NULL,
                created_at INTEGER NOT NULL
            );",
        )
        ?;

        let mut store = Self {
            store_path: store_path.to_path_buf(),
            limit_bytes,
            stats,
            stats_path,
            db,
        };

        // Migrate from filesystem if DB is empty but store has files
        store.migrate_from_filesystem()?;

        let count = store.chunk_count();
        let bytes = store.total_bytes();
        println!(
            "📦 ChunkStore: {} chunks, {:.1} GB (SQLite index)",
            count,
            bytes as f64 / 1_073_741_824.0
        );

        Ok(store)
    }

    /// One-time migration: scan filesystem and populate SQLite index.
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

        // Check if there are any chunk files
        let has_files = WalkDir::new(&self.store_path)
            .max_depth(3)
            .into_iter()
            .filter_map(|e| e.ok())
            .any(|e| e.file_type().is_file() && e.file_name().to_string_lossy().len() == 64);

        if !has_files {
            return Ok(()); // Fresh store, nothing to migrate
        }

        println!("🔄 Migrating chunk index to SQLite (one-time operation)...");

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let tx = self.db.transaction()
            ?;

        let mut migrated = 0usize;
        for entry in WalkDir::new(&self.store_path)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_file() {
                let name = entry.file_name().to_string_lossy();
                if name.len() == 64 {
                    let size = entry.metadata().map(|m| m.len()).unwrap_or(0) as i64;
                    tx.execute(
                        "INSERT OR IGNORE INTO chunks (hash, size_bytes, created_at) VALUES (?1, ?2, ?3)",
                        params![name.as_ref(), size, now],
                    )?;

                    migrated += 1;
                    if migrated % 100_000 == 0 {
                        println!("  ... migrated {} chunks", migrated);
                    }
                }
            }
        }

        tx.commit()
            ?;

        println!("✅ Migration complete: {} chunks indexed", migrated);
        Ok(())
    }

    /// Compute SHA-256 hash of raw bytes.
    pub fn hash_bytes(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hex::encode(hasher.finalize())
    }

    /// Store a chunk, returning its SHA-256 hash.
    ///
    /// Deduplicates automatically — storing the same data twice is a no-op.
    /// Enforces storage limit if configured.
    pub fn put(&mut self, data: &[u8]) -> Result<String> {
        let hash = Self::hash_bytes(data);
        let path = self.chunk_path(&hash);

        if path.exists() {
            return Ok(hash); // Already stored (content-addressed dedup)
        }

        // Check storage limit
        if self.limit_bytes > 0 {
            let current = self.total_bytes();
            if current + data.len() as u64 > self.limit_bytes {
                return Err(EarthGridError::StorageLimitExceeded(format!(
                    "{:.1} GB",
                    self.limit_bytes as f64 / 1024.0 / 1024.0 / 1024.0
                )));
            }
        }

        // Write chunk to disk
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, data)?;

        // Insert into SQLite index
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        self.db.execute(
            "INSERT OR IGNORE INTO chunks (hash, size_bytes, created_at) VALUES (?1, ?2, ?3)",
            params![hash, data.len() as i64, now],
        )?;

        // Update stats
        self.stats.chunks_stored += 1;
        self.stats.bytes_ingested += data.len() as u64;
        self.save_stats();

        Ok(hash)
    }

    /// Retrieve a chunk by its SHA-256 hash.
    ///
    /// Returns `None` if the chunk doesn't exist.
    pub fn get(&mut self, hash: &str) -> Result<Option<Vec<u8>>> {
        let path = self.chunk_path(hash);
        if !path.exists() {
            return Ok(None);
        }
        let data = fs::read(&path)?;

        // Update stats
        self.stats.chunks_served += 1;
        self.stats.bytes_served += data.len() as u64;
        self.stats.requests_total += 1;
        self.save_stats();

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

    /// Delete a chunk. Returns true if it existed.
    pub fn delete(&mut self, hash: &str) -> Result<bool> {
        let path = self.chunk_path(hash);
        if path.exists() {
            fs::remove_file(&path)?;
            // Remove from SQLite index
            self.db.execute(
                "DELETE FROM chunks WHERE hash = ?1",
                params![hash],
            )?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// List chunk hashes in the store.
    ///
    /// # Arguments
    /// * `limit` - Optional limit on number of results
    pub fn list_chunks(&self, limit: Option<usize>) -> Vec<String> {
        let query = match limit {
            Some(n) => format!("SELECT hash FROM chunks LIMIT {}", n),
            None => "SELECT hash FROM chunks".to_string(),
        };
        let mut stmt = match self.db.prepare(&query) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let rows = stmt.query_map([], |row| row.get::<_, String>(0));
        match rows {
            Ok(mapped) => mapped.filter_map(|r| r.ok()).collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Number of chunks in the store (from SQLite).
    pub fn chunk_count(&self) -> usize {
        self.db.query_row(
            "SELECT count(*) FROM chunks",
            [],
            |row| row.get::<_, i64>(0),
        ).unwrap_or(0) as usize
    }

    /// Total bytes used by all chunks (from SQLite).
    pub fn total_bytes(&self) -> u64 {
        self.db.query_row(
            "SELECT COALESCE(sum(size_bytes), 0) FROM chunks",
            [],
            |row| row.get::<_, i64>(0),
        ).unwrap_or(0) as u64
    }

    /// Get current store statistics.
    pub fn stats(&self) -> &StoreStats {
        &self.stats
    }

    // --- Private ---

    /// Two-level directory path: `ab/cd/abcd1234...`
    fn chunk_path(&self, hash: &str) -> PathBuf {
        self.store_path
            .join(&hash[..2])
            .join(&hash[2..4])
            .join(hash)
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

    fn save_stats(&self) {
        if let Ok(json) = serde_json::to_string_pretty(&self.stats) {
            let _ = fs::write(&self.stats_path, json);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_store() -> (ChunkStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = ChunkStore::new(&dir.path().join("chunks"), 1.0).unwrap();
        (store, dir)
    }

    #[test]
    fn test_put_and_get() {
        let (mut store, _dir) = test_store();
        let data = b"Hello, EarthGrid!";
        let hash = store.put(data).unwrap();
        assert_eq!(hash.len(), 64); // SHA-256 hex = 64 chars

        let retrieved = store.get(&hash).unwrap().unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn test_deduplication() {
        let (mut store, _dir) = test_store();
        let data = b"duplicate data";
        let h1 = store.put(data).unwrap();
        let h2 = store.put(data).unwrap();
        assert_eq!(h1, h2);
        assert_eq!(store.chunk_count(), 1);
    }

    #[test]
    fn test_has() {
        let (mut store, _dir) = test_store();
        let hash = store.put(b"test").unwrap();
        assert!(store.has(&hash));
        assert!(!store.has("0000000000000000000000000000000000000000000000000000000000000000"));
    }

    #[test]
    fn test_verify_ok() {
        let (mut store, _dir) = test_store();
        let hash = store.put(b"verify me").unwrap();
        assert!(store.verify(&hash).unwrap());
    }

    #[test]
    fn test_verify_corrupted() {
        let (mut store, _dir) = test_store();
        let hash = store.put(b"original").unwrap();
        // Corrupt the file
        let path = store.chunk_path(&hash);
        fs::write(&path, b"tampered!").unwrap();
        assert!(store.verify(&hash).is_err());
    }

    #[test]
    fn test_delete() {
        let (mut store, _dir) = test_store();
        let hash = store.put(b"delete me").unwrap();
        assert!(store.has(&hash));
        assert!(store.delete(&hash).unwrap());
        assert!(!store.has(&hash));
        assert_eq!(store.chunk_count(), 0);
    }

    #[test]
    fn test_list_chunks() {
        let (mut store, _dir) = test_store();
        store.put(b"chunk1").unwrap();
        store.put(b"chunk2").unwrap();
        store.put(b"chunk3").unwrap();
        assert_eq!(store.list_chunks(None).len(), 3);
    }

    #[test]
    fn test_list_chunks_with_limit() {
        let (mut store, _dir) = test_store();
        store.put(b"chunk1").unwrap();
        store.put(b"chunk2").unwrap();
        store.put(b"chunk3").unwrap();
        assert_eq!(store.list_chunks(Some(2)).len(), 2);
    }

    #[test]
    fn test_hash_deterministic() {
        let h1 = ChunkStore::hash_bytes(b"deterministic");
        let h2 = ChunkStore::hash_bytes(b"deterministic");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_storage_limit() {
        let dir = TempDir::new().unwrap();
        let store_path = dir.path().join("chunks");
        // Set limit to 1 KB (0.000001 GB ≈ 1073 bytes)
        let mut store = ChunkStore::new(&store_path, 0.000001).unwrap();
        // First put should succeed (small data)
        store.put(&[0u8; 512]).unwrap();
        // Second put should fail (exceeds ~1 KB limit)
        let result = store.put(&[1u8; 1024]);
        assert!(result.is_err());
    }

    #[test]
    fn test_stats_tracking() {
        let (mut store, _dir) = test_store();
        store.put(b"data1").unwrap();
        store.put(b"data2").unwrap();
        assert_eq!(store.stats().chunks_stored, 2);
    }

    #[test]
    fn test_chunk_count_and_bytes() {
        let (mut store, _dir) = test_store();
        assert_eq!(store.chunk_count(), 0);
        assert_eq!(store.total_bytes(), 0);

        let data = b"cached count test";
        store.put(data).unwrap();
        assert_eq!(store.chunk_count(), 1);
        assert_eq!(store.total_bytes(), data.len() as u64);

        store.put(b"second chunk").unwrap();
        assert_eq!(store.chunk_count(), 2);
    }

    #[test]
    fn test_delete_updates_counts() {
        let (mut store, _dir) = test_store();
        let data = b"to be deleted";
        let hash = store.put(data).unwrap();
        assert_eq!(store.chunk_count(), 1);

        store.delete(&hash).unwrap();
        assert_eq!(store.chunk_count(), 0);
        assert_eq!(store.total_bytes(), 0);
    }

    #[test]
    fn test_migration_from_filesystem() {
        let dir = TempDir::new().unwrap();
        let store_path = dir.path().join("chunks");

        // Create a store, add chunks, then drop it
        {
            let mut store = ChunkStore::new(&store_path, 0.0).unwrap();
            store.put(b"migrate1").unwrap();
            store.put(b"migrate2").unwrap();
            store.put(b"migrate3").unwrap();
            assert_eq!(store.chunk_count(), 3);
        }

        // Delete the DB to simulate pre-SQLite state
        let db_path = dir.path().join("chunks.db");
        let _ = fs::remove_file(&db_path);
        let _ = fs::remove_file(db_path.with_extension("db-wal"));
        let _ = fs::remove_file(db_path.with_extension("db-shm"));

        // Re-open — should auto-migrate from filesystem
        let store = ChunkStore::new(&store_path, 0.0).unwrap();
        assert_eq!(store.chunk_count(), 3);
    }
}
