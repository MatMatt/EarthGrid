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
    routes::chunks::{read_bounded, HOP_HEADER, MAX_PEER_RESPONSE_BYTES},
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

                // Reject items with invalid chunk hashes — peer data is untrusted
                if let Some(bad) = item.chunk_hashes.iter().find(|h| !ChunkStore::is_valid_hash(h)) {
                    result.errors.push(format!(
                        "Collection {}: item {} has invalid chunk hash: {}",
                        collection_id, item.id, bad
                    ));
                    continue;
                }

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
                // Hop marker: on a local miss the peer answers 404 instead of fanning out.
                match client.get(&url).header(HOP_HEADER, "1").send().await {
                    Ok(resp) if resp.status().is_success() => {
                        match read_bounded(resp, MAX_PEER_RESPONSE_BYTES).await {
                            Some(bytes) => Ok((hash, bytes)),
                            None => Err(format!(
                                "Failed to read chunk {}: unreadable or larger than {} bytes",
                                hash, MAX_PEER_RESPONSE_BYTES
                            )),
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

// ---------------------------------------------------------------------------
// Tests (a throwaway peer on a loopback port — no external network)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;

    type Seen = Arc<std::sync::Mutex<Vec<HeaderMap>>>;

    /// Serve `body` for every chunk request, recording the request headers.
    /// Returns the peer's base URL.
    async fn spawn_peer(body: Vec<u8>, seen: Seen) -> String {
        let app = axum::Router::new().route(
            "/api/chunks/{sha}",
            axum::routing::get(move |headers: HeaderMap| {
                let (body, seen) = (body.clone(), seen.clone());
                async move {
                    seen.lock().unwrap().push(headers);
                    body
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{}", addr)
    }

    fn client() -> reqwest::Client {
        reqwest::Client::builder().no_proxy().build().unwrap()
    }

    #[tokio::test]
    async fn test_chunk_requests_carry_hop_marker() {
        let seen = Seen::default();
        let body = b"chunk-bytes".to_vec();
        let hash = ChunkStore::hash_bytes(&body);
        let peer = spawn_peer(body.clone(), seen.clone()).await;

        let (downloaded, bytes, errors) =
            Replicator::download_chunks(&client(), &peer, &[hash.clone()]).await;
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(bytes, body.len() as u64);
        assert_eq!(downloaded, vec![(hash, body)]);

        // The header map the peer actually received carries the marker, and the
        // inbound check the chunk and search handlers use recognises it.
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].get(HOP_HEADER).and_then(|v| v.to_str().ok()), Some("1"));
        assert!(crate::routes::chunks::is_forwarded(&seen[0]));
    }

    #[tokio::test]
    async fn test_oversized_chunk_response_is_rejected() {
        let hash = ChunkStore::hash_bytes(b"oversized");
        let peer = spawn_peer(vec![0u8; MAX_PEER_RESPONSE_BYTES + 1], Seen::default()).await;

        let (downloaded, bytes, errors) = Replicator::download_chunks(&client(), &peer, &[hash]).await;
        assert!(downloaded.is_empty());
        assert_eq!(bytes, 0);
        assert_eq!(errors.len(), 1, "{errors:?}");
    }
}
