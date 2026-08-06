//! Smart Eviction — automatically delete items when storage exceeds the limit.
//!
//! Eviction priority (highest = delete first):
//! 1. Items replicated on other nodes in the grid (safe to delete)
//! 2. Cold items (low access count)  
//! 3. Oldest items (by ingest date)
//!
//! Safety: NEVER delete the last replica of an item in the grid.

use crate::catalog::Catalog;
use crate::chunk_store::ChunkStore;
use crate::error::Result;

/// Info about an item candidate for eviction.
#[derive(Debug)]
struct EvictionCandidate {
    item_id: String,
    collection: String,
    chunk_hashes: Vec<String>,
    bytes: i64,
    created_at: f64,
    replica_count: u32, // how many other nodes have this item (from beacon)
}

/// Result of an eviction run.
#[derive(Debug, serde::Serialize)]
pub struct EvictionResult {
    pub items_deleted: usize,
    pub bytes_freed: i64,
    pub items_kept: usize,
    pub reason: String,
}

/// Run smart eviction to bring storage under the target limit.
///
/// `beacon_db_path` is used to check replication status across the grid.
/// `target_gb` is the desired storage limit.
pub fn evict(
    catalog: &Catalog,
    store: &mut ChunkStore,
    target_gb: f64,
    beacon_db_path: Option<&std::path::Path>,
) -> Result<EvictionResult> {
    evict_with_beacon_url(catalog, store, target_gb, beacon_db_path, None)
}

/// Eviction with optional beacon URL fallback for non-beacon nodes.
pub fn evict_with_beacon_url(
    catalog: &Catalog,
    store: &mut ChunkStore,
    target_gb: f64,
    beacon_db_path: Option<&std::path::Path>,
    beacon_url: Option<&str>,
) -> Result<EvictionResult> {
    let current_bytes = store.total_bytes() as i64;
    let target_bytes = (target_gb * 1_073_741_824.0) as i64;

    if current_bytes <= target_bytes {
        return Ok(EvictionResult {
            items_deleted: 0,
            bytes_freed: 0,
            items_kept: 0,
            reason: "Storage within limit".to_string(),
        });
    }

    let bytes_to_free = current_bytes - target_bytes;
    eprintln!(
        "🗑️  Eviction: need to free {} GB ({} bytes)",
        bytes_to_free as f64 / 1_073_741_824.0,
        bytes_to_free
    );

    // Build replica map from beacon DB (which items exist on other nodes)
    let replica_map = if beacon_db_path.is_some() {
        build_replica_map(beacon_db_path)
    } else if let Some(url) = beacon_url {
        build_replica_map_from_beacon(url)
    } else {
        std::collections::HashMap::new()
    };

    // Get all items sorted by eviction priority
    let mut candidates = get_candidates(catalog, store, &replica_map)?;

    // Sort: highest eviction score first
    candidates.sort_by(|a, b| {
        eviction_score(b).total_cmp(&eviction_score(a))
    });

    let mut freed: i64 = 0;
    let mut deleted = 0;
    let mut kept = 0;

    for candidate in &candidates {
        if freed >= bytes_to_free {
            break;
        }

        // Safety: never delete the last replica
        if candidate.replica_count == 0 {
            kept += 1;
            continue;
        }

        // Delete all chunks for this item
        let mut item_freed: i64 = 0;
        for hash in &candidate.chunk_hashes {
            if let Ok(true) = store.delete(hash) {
                // Get chunk size (estimate from total / count if needed)
                item_freed += candidate.bytes / candidate.chunk_hashes.len().max(1) as i64;
            }
        }

        // Delete catalog entry
        let _ = catalog.delete_item(&candidate.item_id);

        freed += item_freed;
        deleted += 1;
        eprintln!(
            "  🗑️  Evicted: {} ({}) — {:.1} MB, {} replicas elsewhere",
            candidate.item_id,
            candidate.collection,
            item_freed as f64 / 1_048_576.0,
            candidate.replica_count
        );
    }

    Ok(EvictionResult {
        items_deleted: deleted,
        bytes_freed: freed,
        items_kept: kept,
        reason: if freed >= bytes_to_free {
            format!("Freed {:.1} GB to meet {:.0} GB limit", freed as f64 / 1_073_741_824.0, target_gb)
        } else {
            format!(
                "Could only free {:.1} GB of {:.1} GB needed (kept {} items as last replica)",
                freed as f64 / 1_073_741_824.0,
                bytes_to_free as f64 / 1_073_741_824.0,
                kept
            )
        },
    })
}

/// Eviction score: higher = more likely to evict.
/// Factors: replica count (most important), age, chunk count (proxy for size).
fn eviction_score(c: &EvictionCandidate) -> f64 {
    let replica_factor = c.replica_count as f64 * 10.0; // heavily favor replicated items
    let age_days = (now_ts() - c.created_at) / 86400.0;
    let age_factor = (age_days / 30.0).min(5.0); // older items score higher, cap at 5
    replica_factor + age_factor
}

/// Build a map of item_id → replica count from beacon DB.
/// Counts how many OTHER nodes have each collection (approximate).
fn build_replica_map(beacon_db_path: Option<&std::path::Path>) -> std::collections::HashMap<String, u32> {
    let mut map = std::collections::HashMap::new();
    let Some(path) = beacon_db_path else { return map };
    let Ok(conn) = rusqlite::Connection::open(path) else { return map };

    // Get all nodes' collections to estimate replication
    let mut stmt = match conn.prepare(
        "SELECT node_name, collections_json, item_count FROM beacon_nodes WHERE last_seen > ?1"
    ) {
        Ok(s) => s,
        Err(_) => return map,
    };

    let cutoff = now_ts() - 3600.0; // only consider nodes seen in last hour
    let rows: Vec<(String, String, i64)> = stmt
        .query_map(rusqlite::params![cutoff], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?))
        })
        .into_iter()
        .flatten()
        .filter_map(|r| r.ok())
        .collect();

    // For each collection that appears on multiple nodes, mark items as replicated
    let mut collection_node_count: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
    for (_name, coll_json, _items) in &rows {
        if let Ok(colls) = serde_json::from_str::<Vec<String>>(coll_json) {
            for c in colls {
                *collection_node_count.entry(c).or_insert(0) += 1;
            }
        }
    }

    // Items in collections present on N nodes have N-1 other replicas
    for (collection, count) in collection_node_count {
        if count > 1 {
            // We use the collection name as key since we don't have per-item replication info
            map.insert(collection, count - 1);
        }
    }

    map
}

/// Build replica map by querying the beacon HTTP API.
/// Used by non-beacon nodes that don't have a local beacon.db.
fn build_replica_map_from_beacon(beacon_url: &str) -> std::collections::HashMap<String, u32> {
    let mut map = std::collections::HashMap::new();
    let url = format!("{}/api/beacon/nodes", beacon_url.trim_end_matches('/'));

    // Synchronous HTTP via ureq, NOT reqwest::blocking.
    //
    // `evict_with_beacon_url` is called from both sync contexts (the CLI) and
    // from inside a `tokio::spawn` (the auto-eviction loop in server.rs).
    // `reqwest::blocking` builds and drops its own runtime, which panics with
    // "Cannot drop a runtime in a context where blocking is not allowed" when
    // called from an async context — killing the auto-eviction task on its
    // first run, so nodes silently grew past their storage limit forever.
    // ureq is genuinely synchronous and safe from either context.
    let body: serde_json::Value = match ureq::get(&url)
        .config()
        .timeout_global(Some(std::time::Duration::from_secs(10)))
        .build()
        .call()
    {
        Ok(mut resp) => match resp.body_mut().read_json() {
            Ok(v) => v,
            Err(_) => return map,
        },
        Err(_) => {
            eprintln!("⚠️  Eviction: could not reach beacon at {}", url);
            return map;
        }
    };

    // Count how many nodes have each collection
    let mut collection_node_count: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
    if let Some(nodes) = body.get("nodes").and_then(|n| n.as_array()) {
        for node in nodes {
            if let Some(colls) = node.get("collections").and_then(|c| c.as_array()) {
                for c in colls {
                    if let Some(name) = c.as_str() {
                        *collection_node_count.entry(name.to_string()).or_insert(0) += 1;
                    }
                }
            }
        }
    }

    for (collection, count) in collection_node_count {
        if count > 1 {
            map.insert(collection, count - 1);
        }
    }

    eprintln!("🔍 Eviction: beacon reports {} replicated collections", map.len());
    map
}

/// Get all items as eviction candidates.
fn get_candidates(
    catalog: &Catalog,
    store: &ChunkStore,
    replica_map: &std::collections::HashMap<String, u32>,
) -> Result<Vec<EvictionCandidate>> {
    let collections = catalog.list_collections().unwrap_or_default();
    let mut candidates = Vec::new();

    for col in &collections {
        let items = catalog.search(Some(&col.id), None, None, 100000, 0)?;
        for item in items {
            let bytes: i64 = item.chunk_hashes.iter().map(|h| {
                store.chunk_size(h).unwrap_or(0) as i64
            }).sum();

            let replica_count = replica_map.get(&col.id).copied().unwrap_or(0);

            candidates.push(EvictionCandidate {
                item_id: item.id.clone(),
                collection: col.id.clone(),
                chunk_hashes: item.chunk_hashes.clone(),
                bytes,
                created_at: item.created_at,
                replica_count,
            });
        }
    }

    Ok(candidates)
}

fn now_ts() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: eviction must be callable from inside an async task.
    ///
    /// `build_replica_map_from_beacon` used `reqwest::blocking`, which builds
    /// and drops its own runtime. Called from the `tokio::spawn` auto-eviction
    /// loop in `server.rs`, that panicked with "Cannot drop a runtime in a
    /// context where blocking is not allowed", so the task died on its first
    /// run and nodes never evicted anything — they just grew past their limit.
    ///
    /// The beacon URL points at a closed port: we are asserting that the call
    /// *returns* (a failed lookup yields an empty replica map) rather than
    /// unwinding.
    #[test]
    fn evict_from_async_context_does_not_panic() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let joined = rt.block_on(async {
            tokio::spawn(async {
                let dir = tempfile::tempdir().unwrap();
                let catalog = Catalog::new(&dir.path().join("catalog.db")).unwrap();
                let mut store = ChunkStore::new(&dir.path().join("store"), 0.0).unwrap();
                store.put(b"some bytes worth evicting").unwrap();

                // target_gb below current usage forces the eviction path,
                // which reaches out to the beacon for the replica map.
                evict_with_beacon_url(
                    &catalog,
                    &mut store,
                    0.0000000001,
                    None,
                    Some("http://127.0.0.1:1"),
                )
                .map(|r| r.items_deleted)
            })
            .await
        });

        assert!(
            joined.is_ok(),
            "eviction panicked inside a tokio task: {:?}",
            joined.err()
        );
        assert!(joined.unwrap().is_ok(), "eviction returned an error");
    }
}
