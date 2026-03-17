//! File-based ingest — splits files into fixed-size chunks, stores in ChunkStore,
//! and creates a STAC item cataloging the result.

use std::fs;
use std::io::Read;
use std::path::Path;

use sha2::{Digest, Sha256};

use crate::catalog::StacItem;
use crate::chunk_store::ChunkStore;
use crate::error::Result;

/// Default chunk size: 4 MB.
pub const DEFAULT_CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// Ingest a file by splitting it into fixed-size chunks.
///
/// Each chunk is SHA-256 hashed and stored in the ChunkStore.
/// Returns a StacItem with metadata and the list of chunk hashes.
///
/// Geometry/bbox are zeroed (no GDAL parsing).
pub fn ingest_file(
    path: &Path,
    collection: &str,
    chunk_size: usize,
    store: &mut ChunkStore,
) -> Result<StacItem> {
    let chunk_size = if chunk_size == 0 { DEFAULT_CHUNK_SIZE } else { chunk_size };

    let metadata = fs::metadata(path)?;
    let file_size = metadata.len();
    let filename = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    // Read file and split into chunks
    let mut file = fs::File::open(path)?;
    let mut chunk_hashes: Vec<String> = Vec::new();
    let mut file_hasher = Sha256::new();
    let mut buf = vec![0u8; chunk_size];

    loop {
        let mut bytes_read = 0;
        while bytes_read < chunk_size {
            match file.read(&mut buf[bytes_read..]) {
                Ok(0) => break,
                Ok(n) => bytes_read += n,
                Err(e) => return Err(e.into()),
            }
        }
        if bytes_read == 0 {
            break;
        }

        let chunk_data = &buf[..bytes_read];
        file_hasher.update(chunk_data);
        let hash = store.put(chunk_data)?;
        chunk_hashes.push(hash);
    }

    let file_hash = hex::encode(file_hasher.finalize());
    let short_hash = &file_hash[..12];

    // Build item ID from filename + content hash
    let stem = path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "file".to_string());
    let item_id = format!("{}_{}", stem, short_hash);

    let now = chrono::Utc::now().to_rfc3339();

    let item = StacItem {
        id: item_id,
        collection: collection.to_string(),
        bbox: [0.0, 0.0, 0.0, 0.0],
        properties: serde_json::json!({
            "earthgrid:filename": filename,
            "earthgrid:file_size": file_size,
            "earthgrid:file_hash": file_hash,
            "earthgrid:chunk_count": chunk_hashes.len(),
            "earthgrid:chunk_size": chunk_size,
            "datetime": &now,
        }),
        chunk_hashes,
        created_at: now,
    };

    Ok(item)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::io::Write;

    fn setup() -> (ChunkStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = ChunkStore::new(&dir.path().join("store"), 0.0).unwrap();
        (store, dir)
    }

    #[test]
    fn test_ingest_small_file() {
        let (mut store, dir) = setup();
        let test_file = dir.path().join("test.tif");
        fs::write(&test_file, b"Hello EarthGrid!").unwrap();

        let item = ingest_file(&test_file, "test-collection", DEFAULT_CHUNK_SIZE, &mut store).unwrap();
        assert!(item.id.starts_with("test_"));
        assert_eq!(item.collection, "test-collection");
        assert_eq!(item.chunk_hashes.len(), 1);
        assert!(store.has(&item.chunk_hashes[0]));
    }

    #[test]
    fn test_ingest_multi_chunk() {
        let (mut store, dir) = setup();
        let test_file = dir.path().join("big.bin");
        let mut f = fs::File::create(&test_file).unwrap();
        f.write_all(&[42u8; 10240]).unwrap();

        let item = ingest_file(&test_file, "binary", 1024, &mut store).unwrap();
        assert_eq!(item.chunk_hashes.len(), 10);
        for hash in &item.chunk_hashes {
            assert!(store.has(hash));
        }
    }

    #[test]
    fn test_ingest_dedup() {
        let (mut store, dir) = setup();
        let test_file = dir.path().join("dup.bin");
        fs::write(&test_file, vec![0u8; 4096]).unwrap();

        let item = ingest_file(&test_file, "dedup", 1024, &mut store).unwrap();
        assert_eq!(item.chunk_hashes.len(), 4);
        assert!(item.chunk_hashes.iter().all(|h| h == &item.chunk_hashes[0]));
        assert_eq!(store.chunk_count(), 1);
    }

    #[test]
    fn test_ingest_properties() {
        let (mut store, dir) = setup();
        let test_file = dir.path().join("props.dat");
        fs::write(&test_file, b"properties test").unwrap();

        let item = ingest_file(&test_file, "meta", DEFAULT_CHUNK_SIZE, &mut store).unwrap();
        let props = &item.properties;
        assert_eq!(props["earthgrid:filename"], "props.dat");
        assert_eq!(props["earthgrid:file_size"], 15);
        assert_eq!(props["earthgrid:chunk_count"], 1);
    }
}
