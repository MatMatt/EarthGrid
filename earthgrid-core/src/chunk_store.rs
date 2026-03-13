//! Content-Addressable Storage (CAS) — SHA-256 hash-based chunk storage.
//!
//! Every chunk is identified by its SHA-256 hash. Two-level directory structure
//! for efficient filesystem access: `ab/cd/abcd1234...`

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

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
/// Provides integrity verification, storage limits, and usage stats.
pub struct ChunkStore {
    store_path: PathBuf,
    limit_bytes: u64,
    stats: StoreStats,
    stats_path: PathBuf,
}

impl ChunkStore {
    /// Create a new ChunkStore at the given path.
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

        Ok(Self {
            store_path: store_path.to_path_buf(),
            limit_bytes,
            stats,
            stats_path,
        })
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

        // Write chunk
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, data)?;

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
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// List all chunk hashes in the store.
    pub fn list_chunks(&self) -> Vec<String> {
        let mut chunks = Vec::new();
        for entry in WalkDir::new(&self.store_path)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_file() {
                let name = entry.file_name().to_string_lossy();
                if name.len() == 64 {
                    chunks.push(name.to_string());
                }
            }
        }
        chunks
    }

    /// Number of chunks in the store.
    pub fn chunk_count(&self) -> usize {
        self.list_chunks().len()
    }

    /// Total bytes used by all chunks.
    pub fn total_bytes(&self) -> u64 {
        let mut total = 0u64;
        for entry in WalkDir::new(&self.store_path)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_file() {
                let name = entry.file_name().to_string_lossy();
                if name.len() == 64 {
                    if let Ok(meta) = entry.metadata() {
                        total += meta.len();
                    }
                }
            }
        }
        total
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
    }

    #[test]
    fn test_list_chunks() {
        let (mut store, _dir) = test_store();
        store.put(b"chunk1").unwrap();
        store.put(b"chunk2").unwrap();
        store.put(b"chunk3").unwrap();
        assert_eq!(store.list_chunks().len(), 3);
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
}
