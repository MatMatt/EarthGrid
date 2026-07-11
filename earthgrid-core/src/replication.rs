//! Replication module — sync STAC items and chunks from a remote EarthGrid peer.
//!
//! Uses reqwest for HTTP and tokio::Semaphore to cap concurrent chunk downloads.

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
// Replicator
// ---------------------------------------------------------------------------

pub struct Replicator {
    store: Arc<Mutex<ChunkStore>>,
    catalog: Arc<Mutex<Catalog>>,
    client: reqwest::Client,
}

impl Replicator {
    /// Create a new Replicator.
    pub fn new(store: Arc<Mutex<ChunkStore>>, catalog: Arc<Mutex<Catalog>>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .unwrap_or_default();
        Self { store, catalog, client }
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
                let item: StacItem = match serde_json::from_value(item_json.clone()) {
                    Ok(i) => i,
                    Err(e) => {
                        result.errors.push(format!(
                            "Collection {}: item parse error: {}",
                            collection_id, e
                        ));
                        continue;
                    }
                };

                // Collect missing chunk hashes
                let missing_hashes: Vec<String> = {
                    let store = self.store.lock().await;
                    item.chunk_hashes
                        .iter()
                        .filter(|h| !store.has(h))
                        .cloned()
                        .collect()
                };

                if !dry_run && !missing_hashes.is_empty() {
                    // Download missing chunks concurrently
                    let (downloaded, bytes, errors) =
                        Self::download_chunks(&self.client, peer_url, &missing_hashes).await;

                    result.chunks_downloaded += downloaded.len();
                    result.bytes_downloaded += bytes;
                    for e in errors {
                        result.errors.push(e);
                    }

                    // Store downloaded chunks (verify hashes first — peer data is untrusted)
                    let mut store = self.store.lock().await;
                    for (hash, data) in downloaded {
                        let actual_sha = hex::encode(Sha256::digest(&data));
                        if actual_sha != hash {
                            result.errors.push(format!(
                                "Hash mismatch for chunk {}: expected {}, got {}",
                                hash, hash, actual_sha
                            ));
                            continue;
                        }
                        match store.put(&data) {
                            Ok(_) => {}
                            Err(EarthGridError::StorageLimitExceeded(_)) => {
                                result.storage_limit_hit = true;
                                result.errors.push("Storage limit reached; stopping sync".into());
                                return result;
                            }
                            Err(e) => {
                                result.errors.push(format!("Failed to store chunk {}: {}", hash, e));
                            }
                        }
                    }
                }

                // Store item in catalog
                if !dry_run {
                    let catalog = self.catalog.lock().await;
                    if let Err(e) = catalog.add_item(&item) {
                        result.errors.push(format!("Failed to store item {}: {}", item.id, e));
                    }
                }

                result.items_processed += 1;
            }
        }

        result
    }

    /// Download a batch of chunks concurrently (semaphore-limited).
    ///
    /// Returns `(downloaded_chunks, total_bytes, errors)`.
    async fn download_chunks(
        client: &reqwest::Client,
        peer_url: &str,
        hashes: &[String],
    ) -> (Vec<(String, Vec<u8>)>, u64, Vec<String>) {
        let semaphore = Arc::new(Semaphore::new(CHUNK_CONCURRENCY));
        let mut handles = Vec::with_capacity(hashes.len());

        for hash in hashes {
            let hash = hash.clone();
            let client = client.clone();
            let peer_url = peer_url.to_string();
            let sem = semaphore.clone();

            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await;
                let url = format!("{}/api/chunks/{}", peer_url, hash);
                match client.get(&url).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        match resp.bytes().await {
                            Ok(bytes) => Ok((hash, bytes.to_vec())),
                            Err(e) => Err(format!("Failed to read chunk {}: {}", hash, e)),
                        }
                    }
                    Ok(resp) => Err(format!(
                        "Chunk {} returned HTTP {}",
                        hash,
                        resp.status()
                    )),
                    Err(e) => Err(format!("Chunk {} fetch error: {}", hash, e)),
                }
            }));
        }

        let mut downloaded = Vec::new();
        let mut total_bytes = 0u64;
        let mut errors = Vec::new();

        for handle in handles {
            match handle.await {
                Ok(Ok((hash, data))) => {
                    total_bytes += data.len() as u64;
                    downloaded.push((hash, data));
                }
                Ok(Err(e)) => errors.push(e),
                Err(e) => errors.push(format!("Task join error: {}", e)),
            }
        }

        (downloaded, total_bytes, errors)
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
            "{}/stac/collections/{}/items?limit={}",
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
