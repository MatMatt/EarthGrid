//! Replication module — sync STAC items and chunks from a remote EarthGrid peer.
//!
//! Uses reqwest for HTTP and tokio::Semaphore to cap concurrent chunk downloads.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, Semaphore};

use crate::{
    catalog::{Catalog, StacItem},
    chunk_store::ChunkStore,
    error::EarthGridError,
};

const CHUNK_CONCURRENCY: usize = 10;

/// Download tasks spawned per batch in `download_chunks`. Only `CHUNK_CONCURRENCY`
/// of them fetch at once; this caps how many exist at all.
const CHUNK_SPAWN_BATCH: usize = CHUNK_CONCURRENCY * 10;

/// Largest chunk body accepted from a peer. Sized above the largest chunk the
/// currently wired fetcher path produces: a tile chunk holds all bands, so a
/// 512x512 tile with nine Float64 bands is ~18.9 MB. Nothing enforces a maximum
/// on the producer side (`ingest_file` takes a caller-supplied chunk size,
/// `ingest_raster` unbounded dimensions), so a node can hold chunks larger than
/// this, and a peer will refuse them. `CHUNK_CONCURRENCY` (ten) concurrent
/// bodies of this size are the peak memory this caps.
const MAX_CHUNK_BYTES: u64 = 32 * 1024 * 1024;

// Bounds on peer-supplied `earthgrid:*` properties (see `validate_remote_item`).
// Sanity bounds against absurd peer values; the real allocation limits live in the reconstruct path.
const MAX_REMOTE_DIMENSION: u64 = 1_000_000;
const MIN_REMOTE_TILE_SIZE: u64 = 1;
const MAX_REMOTE_TILE_SIZE: u64 = 65_536;
// Must stay below `u32::MAX`: reconstruction casts `tile_cols` to u32 and divides by it.
const MAX_REMOTE_TILE_GRID: u64 = 1_000_000;
const MAX_REMOTE_BANDS: u64 = 4096;
const MAX_REMOTE_CHUNKS: usize = 1_000_000;

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SyncResult {
    pub peer_url: String,
    pub collections_processed: usize,
    pub items_processed: usize,
    pub chunks_downloaded: usize,
    pub bytes_downloaded: u64,
    pub errors: Vec<String>,
    pub dry_run: bool,
    pub storage_limit_hit: bool,
}

// ---------------------------------------------------------------------------
// Remote STAC types (minimal — we only need what we parse)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct RemoteCollection {
    pub id: String,
}

#[derive(Debug, Deserialize)]
struct CollectionsResponse {
    pub collections: Vec<RemoteCollection>,
}

#[derive(Debug, Deserialize)]
struct ItemsResponse {
    pub features: Vec<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Remote-item validation
// ---------------------------------------------------------------------------

/// Shorten a peer-supplied value before it goes into an error message.
fn clip(value: &str) -> String {
    value.chars().take(64).collect()
}

/// Read an optional unsigned `earthgrid:*` property. A value that is present
/// but not an unsigned integer is rejected — every consumer reads these with
/// `as_u64`.
fn remote_prop_u64(props: &serde_json::Value, key: &str) -> Result<Option<u64>, String> {
    match props.get(key) {
        None => Ok(None),
        Some(v) => v
            .as_u64()
            .map(Some)
            .ok_or_else(|| format!("{} = {} is not an unsigned integer", key, clip(&v.to_string()))),
    }
}

/// Validate a STAC item received from a peer before anything is stored.
///
/// Peer data is untrusted: the item is filed under `expected_collection` (the
/// collection we asked the peer for) whatever its own `collection` field says,
/// its id must match `^[A-Za-z0-9._:-]{1,128}$`, and the `earthgrid:*` tile
/// properties and the chunk-hash list must stay within sane bounds. The error
/// names the offending property and value.
pub fn validate_remote_item(item: &mut StacItem, expected_collection: &str) -> Result<(), String> {
    // Never trust the peer's collection field.
    item.collection = expected_collection.to_string();

    let id_ok = (1..=128).contains(&item.id.len())
        && item
            .id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'));
    if !id_ok {
        return Err(format!(
            "id {:?} does not match ^[A-Za-z0-9._:-]{{1,128}}$",
            clip(&item.id)
        ));
    }

    let props = &item.properties;
    let width = remote_prop_u64(props, "earthgrid:width")?;
    let height = remote_prop_u64(props, "earthgrid:height")?;
    let tile_size = remote_prop_u64(props, "earthgrid:tile_size")?;
    let tile_cols = remote_prop_u64(props, "earthgrid:tile_cols")?;
    let tile_rows = remote_prop_u64(props, "earthgrid:tile_rows")?;
    let bands = remote_prop_u64(props, "earthgrid:bands")?;

    for (key, value) in [("earthgrid:width", width), ("earthgrid:height", height)] {
        if let Some(v) = value {
            if v > MAX_REMOTE_DIMENSION {
                return Err(format!("{} = {} exceeds {}", key, v, MAX_REMOTE_DIMENSION));
            }
        }
    }
    if let Some(v) = tile_size {
        if !(MIN_REMOTE_TILE_SIZE..=MAX_REMOTE_TILE_SIZE).contains(&v) {
            return Err(format!(
                "earthgrid:tile_size = {} is outside {}..={}",
                v, MIN_REMOTE_TILE_SIZE, MAX_REMOTE_TILE_SIZE
            ));
        }
    }
    if tile_cols == Some(0) {
        return Err("earthgrid:tile_cols = 0 must be >= 1".to_string());
    }
    if tile_rows == Some(0) {
        return Err("earthgrid:tile_rows = 0 must be >= 1".to_string());
    }
    // Upper bound on the tile grid: a value above `u32::MAX` would truncate to
    // zero where reconstruction casts it to u32.
    for (key, value) in [("earthgrid:tile_cols", tile_cols), ("earthgrid:tile_rows", tile_rows)] {
        if let Some(v) = value {
            if v > MAX_REMOTE_TILE_GRID {
                return Err(format!("{} = {} exceeds {}", key, v, MAX_REMOTE_TILE_GRID));
            }
        }
    }
    if let Some(v) = bands {
        if v > MAX_REMOTE_BANDS {
            return Err(format!("earthgrid:bands = {} exceeds {}", v, MAX_REMOTE_BANDS));
        }
    }

    // No cross-property or per-hash checks: local ingestion legitimately writes
    // items with a partial tile geometry, band-level items that list more
    // hashes than `tile_cols * tile_rows`, and repeated hashes (identical
    // tiles share one). `download_chunks` deduplicates before fetching.
    let hash_count = item.chunk_hashes.len();
    if hash_count > MAX_REMOTE_CHUNKS {
        return Err(format!(
            "chunk_hashes has {} entries, more than {}",
            hash_count, MAX_REMOTE_CHUNKS
        ));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Replicator
// ---------------------------------------------------------------------------

pub struct Replicator {
    store: Arc<Mutex<ChunkStore>>,
    catalog: Arc<Mutex<Catalog>>,
    client: reqwest::Client,
    /// Operator-configured peer hosts — the only ones allowed in private ranges.
    known_hosts: std::collections::HashSet<String>,
}

impl Replicator {
    /// Create a new Replicator.
    pub fn new(store: Arc<Mutex<ChunkStore>>, catalog: Arc<Mutex<Catalog>>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            // A permitted peer URL must not redirect us into a blocked address.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            // No fallback to `Client::default()`: it follows redirects (and
            // panics on the same build failure anyway).
            .expect("building the replication HTTP client");
        Self { store, catalog, client, known_hosts: Default::default() }
    }

    /// Set the hosts allowed in private ranges (see `url_policy::operator_hosts`).
    pub fn with_known_hosts(mut self, known_hosts: std::collections::HashSet<String>) -> Self {
        self.known_hosts = known_hosts;
        self
    }

    /// Sync items (and their chunks) from a remote EarthGrid peer.
    ///
    /// # Arguments
    /// * `peer_url` — base URL of the remote node, e.g. `http://peer:7770`
    /// * `collections` — optional allowlist of collection IDs; empty = all
    /// * `max_items` — cap on items fetched per collection (0 = unlimited)
    /// * `dry_run` — if true, discover what would be synced but don't store anything
    pub async fn sync_from_peer(
        &self,
        peer_url: &str,
        collections: &[String],
        max_items: usize,
        dry_run: bool,
    ) -> SyncResult {
        let mut result = SyncResult {
            peer_url: peer_url.to_string(),
            dry_run,
            ..Default::default()
        };

        // Ids whose local-skip line was already printed during this run, so each
        // item prints one line per run. Dropped with the run — nothing accumulates.
        let mut reported_local_skips: HashSet<String> = HashSet::new();

        // Outbound URL policy — before any request goes to the peer.
        if let Err(e) = crate::url_policy::validate_outbound_url_async(peer_url, &self.known_hosts).await {
            result.errors.push(format!("Peer URL rejected: {}", e));
            return result;
        }

        // 1. Fetch remote collections
        let remote_collections = match self.fetch_collections(peer_url).await {
            Ok(c) => c,
            Err(e) => {
                result.errors.push(format!("Failed to fetch collections: {}", e));
                return result;
            }
        };

        // Filter to requested collections (if any)
        let target_collections: Vec<String> = if collections.is_empty() {
            remote_collections
        } else {
            remote_collections
                .into_iter()
                .filter(|id| collections.contains(id))
                .collect()
        };

        // 2. For each collection, fetch items
        for collection_id in &target_collections {
            let limit = if max_items > 0 { max_items } else { 10_000 };

            let items_json = match self.fetch_items(peer_url, collection_id, limit).await {
                Ok(v) => v,
                Err(e) => {
                    result.errors.push(format!(
                        "Collection {}: failed to fetch items: {}",
                        collection_id, e
                    ));
                    continue;
                }
            };

            result.collections_processed += 1;

            for item_json in &items_json {
                // Parse the remote item
                let mut item: StacItem = match serde_json::from_value(item_json.clone()) {
                    Ok(i) => i,
                    Err(e) => {
                        result.errors.push(format!(
                            "Collection {}: item parse error: {}",
                            collection_id, e
                        ));
                        continue;
                    }
                };

                // Validation gate — peer data is untrusted. Also files the item
                // under the collection we asked for, not the one the peer claims.
                if let Err(e) = validate_remote_item(&mut item, collection_id) {
                    result.errors.push(format!(
                        "Collection {}: remote item rejected: {}",
                        collection_id, e
                    ));
                    continue;
                }

                // Reject items with invalid chunk hashes — peer data is untrusted
                if let Some(bad) = item.chunk_hashes.iter().find(|h| !ChunkStore::is_valid_hash(h)) {
                    result.errors.push(format!(
                        "Collection {}: item {} has invalid chunk hash: {}",
                        collection_id, item.id, bad
                    ));
                    continue;
                }

                // Decide before downloading: an id we hold as a non-remote item is
                // never written here. Its chunks are still fetched when the store
                // is missing some (pre-`origin` items all count as local, and a sync
                // must keep repairing them); with all chunks present it is skipped
                // without any request. The guard inside `add_remote_item` stays the
                // authoritative one.
                //
                // The repair works from the local row's own chunk list: a same-id peer
                // item may reference different chunks, and fetching those would leave
                // the local item's missing chunks missing.
                let mut held_locally: Option<String> = None;
                let mut local_chunk_hashes: Option<Vec<String>> = None;
                if !dry_run {
                    let catalog = self.catalog.lock().await;
                    let origin = catalog.item_origin(&item.id);
                    match origin {
                        Ok(Some(origin)) if origin != "remote" => {
                            match catalog.get_item(&item.id) {
                                // The item decoder turns malformed `chunk_hashes_json` into an
                                // empty list without reporting it, so an empty local list
                                // beside a non-empty peer list is not proof of a complete item.
                                Ok(Some(local)) if local.chunk_hashes.is_empty() && !item.chunk_hashes.is_empty() => {
                                    result.errors.push(format!(
                                        "Collection {}: item {}: local chunk list unusable (empty, the peer lists {}), checking the peer's chunk list instead",
                                        collection_id, item.id, item.chunk_hashes.len()
                                    ));
                                }
                                Ok(Some(local)) => {
                                    // The local row never went through the gate above, and its
                                    // hashes end up in the peer's chunk URL — drop the invalid ones.
                                    let (valid, invalid): (Vec<String>, Vec<String>) = local
                                        .chunk_hashes
                                        .into_iter()
                                        .partition(|h| ChunkStore::is_valid_hash(h));
                                    if !invalid.is_empty() {
                                        result.errors.push(format!(
                                            "Collection {}: item {}: {} invalid chunk hash(es) in the local row rejected, not fetched",
                                            collection_id, item.id, invalid.len()
                                        ));
                                    }
                                    local_chunk_hashes = Some(valid);
                                }
                                Ok(None) => result.errors.push(format!(
                                    "Collection {}: item {}: local row not found, checking the peer's chunk list instead",
                                    collection_id, item.id
                                )),
                                Err(e) => result.errors.push(format!(
                                    "Collection {}: item {}: local row unreadable, checking the peer's chunk list instead: {}",
                                    collection_id, item.id, e
                                )),
                            }
                            held_locally = Some(origin);
                        }
                        Ok(_) => {}
                        Err(e) => {
                            // Undecidable — do not download on a guess.
                            result.errors.push(format!(
                                "Collection {}: item {}: catalog lookup failed, not fetching: {}",
                                collection_id, item.id, e
                            ));
                            continue;
                        }
                    }
                }

                // Collect missing chunk hashes — from the local row when the id is
                // held locally, from the peer's item otherwise
                let missing_hashes: Vec<String> = {
                    let store = self.store.lock().await;
                    local_chunk_hashes
                        .as_ref()
                        .unwrap_or(&item.chunk_hashes)
                        .iter()
                        .filter(|h| !store.has(h))
                        .cloned()
                        .collect()
                };

                if let Some(origin) = &held_locally {
                    let first_time = reported_local_skips.insert(item.id.clone());
                    if missing_hashes.is_empty() {
                        if first_time {
                            eprintln!(
                                "🔄 Replication: not fetching remote item {} from {} — already held locally (origin '{}'); no chunks requested",
                                item.id, peer_url, origin
                            );
                        }
                        continue;
                    }
                    if first_time {
                        eprintln!(
                            "🔄 Replication: not writing remote item {} from {} — already held locally (origin '{}'); fetching {} chunk(s) missing from the store",
                            item.id, peer_url, origin, missing_hashes.len()
                        );
                    }
                }

                if !dry_run && !missing_hashes.is_empty() {
                    // Download missing chunks concurrently; each one is hash-verified
                    // and stored as it arrives (peer data is untrusted)
                    //
                    // NOT IMPLEMENTED: there is no pre-fetch storage-limit check. The
                    // limit is enforced only by `store.put`, per chunk, so up to
                    // `CHUNK_CONCURRENCY` bodies of `MAX_CHUNK_BYTES` can be fetched
                    // and discarded before the limit is seen and the sync stops.
                    let (stored, bytes, errors, limit_hit) =
                        Self::download_chunks(&self.client, &self.store, peer_url, &missing_hashes).await;

                    result.chunks_downloaded += stored;
                    result.bytes_downloaded += bytes;
                    for e in errors {
                        result.errors.push(e);
                    }

                    if limit_hit {
                        result.storage_limit_hit = true;
                        result.errors.push("Storage limit reached; stopping sync".into());
                        return result;
                    }
                }

                // A locally held item only had its chunks repaired — it is never written.
                if held_locally.is_some() {
                    continue;
                }

                // Store item in catalog — never over a locally ingested item
                if !dry_run {
                    let catalog = self.catalog.lock().await;
                    match catalog.add_remote_item(&item) {
                        Ok(true) => {}
                        Ok(false) => {
                            // Lost a race with a local write — record the id first, so the
                            // local-skip branch does not print for it again in this run.
                            if reported_local_skips.insert(item.id.clone()) {
                                eprintln!(
                                    "🔄 Replication: skipped remote item {} from {} — a local item with that id already exists",
                                    item.id, peer_url
                                );
                            }
                        }
                        Err(e) => {
                            result.errors.push(format!("Failed to store item {}: {}", item.id, e));
                        }
                    }
                }

                result.items_processed += 1;
            }
        }

        result
    }

    /// Download a batch of chunks concurrently (semaphore-limited).
    ///
    /// Each body is capped at `MAX_CHUNK_BYTES`, hash-verified and written to the
    /// store as soon as it arrives, so at most `CHUNK_CONCURRENCY` bodies are held
    /// in memory. Tasks are spawned `CHUNK_SPAWN_BATCH` at a time, each batch
    /// awaited before the next, so the number of live tasks does not grow with
    /// the hash list. Once the store reports its storage limit, no further chunk
    /// is fetched.
    ///
    /// Returns `(chunks_stored, total_bytes, errors, storage_limit_hit)`.
    async fn download_chunks(
        client: &reqwest::Client,
        store: &Arc<Mutex<ChunkStore>>,
        peer_url: &str,
        hashes: &[String],
    ) -> (usize, u64, Vec<String>, bool) {
        let semaphore = Arc::new(Semaphore::new(CHUNK_CONCURRENCY));
        let limit_hit = Arc::new(AtomicBool::new(false));

        // Deduplicate — never fetch the same chunk twice
        let mut seen = HashSet::new();
        let unique: Vec<&String> = hashes.iter().filter(|h| seen.insert(h.as_str())).collect();

        let mut stored = 0usize;
        let mut total_bytes = 0u64;
        let mut errors = Vec::new();

        // Spawn in batches, awaiting each before the next, so the number of live
        // tasks (and the clones they hold) stays bounded whatever the list length.
        for batch in unique.chunks(CHUNK_SPAWN_BATCH) {
            // `Ok(Some(bytes))` = stored, `Ok(None)` = skipped because the storage limit was hit
            let mut handles: Vec<tokio::task::JoinHandle<Result<Option<u64>, String>>> =
                Vec::with_capacity(batch.len());

            for hash in batch {
                let hash: String = (*hash).clone();
                let client = client.clone();
                let store = store.clone();
                let peer_url = peer_url.to_string();
                let sem = semaphore.clone();
                let limit_hit = limit_hit.clone();

                handles.push(tokio::spawn(async move {
                    let _permit = sem.acquire().await;
                    if limit_hit.load(Ordering::SeqCst) {
                        return Ok(None);
                    }
                    let url = format!("{}/api/chunks/{}", peer_url, hash);
                    let mut resp = match client.get(&url).send().await {
                        Ok(resp) if resp.status().is_success() => resp,
                        Ok(resp) => {
                            return Err(format!("Chunk {} returned HTTP {}", hash, resp.status()));
                        }
                        Err(e) => return Err(format!("Chunk {} fetch error: {}", hash, e)),
                    };

                    let declared = resp.content_length();
                    if let Some(len) = declared {
                        if len > MAX_CHUNK_BYTES {
                            return Err(format!(
                                "Chunk {} rejected: Content-Length {} exceeds the {} byte cap",
                                hash, len, MAX_CHUNK_BYTES
                            ));
                        }
                    }

                    // Bounded read, the equivalent of `.take(MAX_CHUNK_BYTES + 1)`: a lying
                    // or missing Content-Length cannot make us buffer more than the cap.
                    let mut data: Vec<u8> =
                        Vec::with_capacity(declared.unwrap_or(0).min(MAX_CHUNK_BYTES) as usize);
                    loop {
                        match resp.chunk().await {
                            Ok(Some(part)) => {
                                if (data.len() + part.len()) as u64 > MAX_CHUNK_BYTES {
                                    return Err(format!(
                                        "Chunk {} rejected: body exceeds the {} byte cap",
                                        hash, MAX_CHUNK_BYTES
                                    ));
                                }
                                data.extend_from_slice(&part);
                            }
                            Ok(None) => break,
                            Err(e) => return Err(format!("Failed to read chunk {}: {}", hash, e)),
                        }
                    }

                    // Verify the hash before storing — peer data is untrusted
                    let actual_sha = hex::encode(Sha256::digest(&data));
                    if actual_sha != hash {
                        return Err(format!(
                            "Hash mismatch for chunk {}: expected {}, got {}",
                            hash, hash, actual_sha
                        ));
                    }

                    // Store immediately; the buffer is dropped when this task ends
                    let put_result = store.lock().await.put(&data);
                    match put_result {
                        Ok(_) => Ok(Some(data.len() as u64)),
                        Err(EarthGridError::StorageLimitExceeded(_)) => {
                            limit_hit.store(true, Ordering::SeqCst);
                            Ok(None)
                        }
                        Err(e) => Err(format!("Failed to store chunk {}: {}", hash, e)),
                    }
                }));
            }

            for handle in handles {
                match handle.await {
                    Ok(Ok(Some(bytes))) => {
                        stored += 1;
                        total_bytes += bytes;
                    }
                    Ok(Ok(None)) => {}
                    Ok(Err(e)) => errors.push(e),
                    Err(e) => errors.push(format!("Task join error: {}", e)),
                }
            }

            // Stop early so a full store is not hammered with further fetches.
            if limit_hit.load(Ordering::SeqCst) {
                break;
            }
        }

        (stored, total_bytes, errors, limit_hit.load(Ordering::SeqCst))
    }

    // -----------------------------------------------------------------------
    // Private HTTP helpers
    // -----------------------------------------------------------------------

    async fn fetch_collections(&self, peer_url: &str) -> anyhow::Result<Vec<String>> {
        let url = format!("{}/api/stac/collections", peer_url);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .context("GET /stac/collections")?;
        if !resp.status().is_success() {
            anyhow::bail!("HTTP {}", resp.status());
        }
        let body: CollectionsResponse = resp.json().await.context("parse collections")?;
        Ok(body.collections.into_iter().map(|c| c.id).collect())
    }

    async fn fetch_items(
        &self,
        peer_url: &str,
        collection_id: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let url = format!(
            "{}/api/stac/collections/{}/items?limit={}",
            peer_url, collection_id, limit
        );
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .context("GET collection items")?;
        if !resp.status().is_success() {
            anyhow::bail!("HTTP {}", resp.status());
        }
        let body: ItemsResponse = resp.json().await.context("parse items")?;
        Ok(body.features)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(n: u8) -> String {
        format!("{:064x}", n)
    }

    /// A 1024x1024 four-band item cut into 2x2 tiles of 512, as `ingest` writes it.
    fn valid_item() -> StacItem {
        StacItem {
            id: "S2A_32UNF_20260311_B04".to_string(),
            collection: "sentinel-2".to_string(),
            bbox: [12.0, 55.0, 13.0, 56.0],
            properties: serde_json::json!({
                "datetime": "2026-03-11T00:00:00Z",
                "earthgrid:width": 1024,
                "earthgrid:height": 1024,
                "earthgrid:bands": 4,
                "earthgrid:tile_size": 512,
                "earthgrid:tile_cols": 2,
                "earthgrid:tile_rows": 2,
            }),
            chunk_hashes: (0..4).map(hash).collect(),
            created_at: 0.0,
            geometry: None,
        }
    }

    fn rejection(item: &mut StacItem) -> String {
        validate_remote_item(item, "sentinel-2").expect_err("item must be rejected")
    }

    #[test]
    fn accepts_valid_item() {
        let mut item = valid_item();
        assert_eq!(validate_remote_item(&mut item, "sentinel-2"), Ok(()));

        // Items from `ingest_file` carry no tile geometry at all.
        let mut file_item = valid_item();
        file_item.properties = serde_json::json!({
            "datetime": "2026-03-11T00:00:00Z",
            "earthgrid:filename": "scene.zip",
            "earthgrid:chunk_count": 4,
        });
        assert_eq!(validate_remote_item(&mut file_item, "sentinel-2"), Ok(()));
    }

    #[test]
    fn remote_item_is_stored_under_expected_collection() {
        let mut item = valid_item();
        item.collection = "peer-chosen".to_string();
        validate_remote_item(&mut item, "sentinel-2").unwrap();
        assert_eq!(item.collection, "sentinel-2");

        let catalog = Catalog::in_memory().unwrap();
        assert!(catalog.add_remote_item(&item).unwrap());
        assert!(catalog.get_collection_item("sentinel-2", &item.id).unwrap().is_some());
        assert!(catalog.get_collection_item("peer-chosen", &item.id).unwrap().is_none());
        assert!(catalog.get_collection("peer-chosen").unwrap().is_none());
    }

    #[test]
    fn rejects_out_of_range_properties() {
        for (key, value) in [
            ("earthgrid:width", 1000001),
            ("earthgrid:height", 1000001),
            ("earthgrid:bands", 4097),
            ("earthgrid:tile_size", 0),
            ("earthgrid:tile_size", 65537),
            ("earthgrid:tile_cols", 0),
            ("earthgrid:tile_cols", 1000001),
            ("earthgrid:tile_rows", 1000001),
        ] {
            let mut item = valid_item();
            item.properties[key] = serde_json::json!(value);
            let msg = rejection(&mut item);
            let expected = format!("{key} = {value}");
            assert!(msg.contains(&expected), "{msg} must name {expected}");
        }
    }

    #[test]
    fn rejects_non_integer_tile_properties() {
        let mut item = valid_item();
        item.properties["earthgrid:width"] = serde_json::json!(-1);
        assert!(rejection(&mut item).contains("earthgrid:width = -1"));

        let mut item = valid_item();
        item.properties["earthgrid:tile_size"] = serde_json::json!("512");
        assert!(rejection(&mut item).contains("earthgrid:tile_size"));

        let mut item = valid_item();
        item.properties["earthgrid:tile_rows"] = serde_json::json!(1.5);
        assert!(rejection(&mut item).contains("earthgrid:tile_rows"));
    }

    #[test]
    fn accepts_partial_and_uncovered_tile_geometry() {
        // Local items do not always carry the whole tile geometry.
        let mut item = valid_item();
        item.properties.as_object_mut().unwrap().remove("earthgrid:tile_cols");
        assert_eq!(validate_remote_item(&mut item, "sentinel-2"), Ok(()));

        // No tile_cols * tile_size >= width consistency check.
        let mut item = valid_item();
        item.properties["earthgrid:width"] = serde_json::json!(2000);
        assert_eq!(validate_remote_item(&mut item, "sentinel-2"), Ok(()));
    }

    #[test]
    fn hash_list_is_capped_but_not_tied_to_tile_grid() {
        // A legacy band-level item lists more hashes than the tile grid has tiles.
        let mut item = valid_item();
        item.chunk_hashes.push(hash(4));
        assert_eq!(validate_remote_item(&mut item, "sentinel-2"), Ok(()));

        // More than the absolute cap.
        let mut item = valid_item();
        item.properties = serde_json::json!({});
        item.chunk_hashes = (0..=MAX_REMOTE_CHUNKS).map(|n| format!("{:064x}", n)).collect();
        assert!(rejection(&mut item).contains("chunk_hashes has 1000001 entries"));
    }

    #[test]
    fn accepts_duplicate_hashes() {
        // Identical tiles (nodata edges, open water) share one hash.
        let mut item = valid_item();
        item.chunk_hashes[3] = hash(0);
        assert_eq!(validate_remote_item(&mut item, "sentinel-2"), Ok(()));
    }

    #[test]
    fn rejects_bad_ids() {
        for id in [
            "../etc/passwd".to_string(),
            "collection/item".to_string(),
            "a".repeat(200),
            "a".repeat(129),
            String::new(),
            "item id".to_string(),
            "ítem".to_string(),
        ] {
            let mut item = valid_item();
            item.id = id.clone();
            assert!(rejection(&mut item).starts_with("id "), "{id:?} must be rejected");
        }

        let mut item = valid_item();
        item.id = "a".repeat(128);
        assert_eq!(validate_remote_item(&mut item, "sentinel-2"), Ok(()));
    }
}
