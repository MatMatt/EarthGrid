//! Smart Eviction — automatically delete items when storage exceeds the limit.
//!
//! Eviction priority (highest = delete first):
//! 1. Items replicated on other nodes in the grid (safe to delete)
//! 2. Cold items (low access count)  
//! 3. Oldest items (by ingest date)
//!
//! Safety: NEVER delete the last replica of an item in the grid.

use crate::catalog::{Catalog, ChunkReferenceCounts};
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

    // Chunks are content addressed and shared between items, so a chunk of
    // the item being evicted may still be needed by another item (possibly in
    // another dataset). Build the reference counts once for this run; the
    // loop below decrements them in memory, no per-chunk SQL.
    let mut chunk_refs = catalog.chunk_reference_counts(None)?;
    if !chunk_refs.complete {
        eprintln!(
            "⚠️  Eviction: chunk references are incomplete (some catalogue rows could not be read); no chunk will be deleted unless proven unreferenced, so nothing unproven will be removed"
        );
    }

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

        // Safety: a collection claim is not proof that THIS item is held
        // elsewhere. `replica_count` comes from build_replica_map, which only
        // counts nodes that claim the whole collection in the beacon registry;
        // a laptop that claims a collection while holding a handful of its
        // items would make every item here look replicated. Deleting on that
        // count could drop the only copy, so until replication of the item is
        // proven it is kept. The count still drives the ordering above.
        if !item_replication_proven(candidate) {
            kept += 1;
            eprintln!(
                "  🔒 Kept: {} ({}) — {} node(s) claim the collection, but replication of this item is not proven; not deleting",
                candidate.item_id,
                candidate.collection,
                candidate.replica_count
            );
            continue;
        }

        // Delete the chunks of this item that no other item still references
        let deleted_chunks = release_item_chunks(store, &candidate.chunk_hashes, &mut chunk_refs);
        // Get chunk size (estimate from total / count if needed)
        let item_freed: i64 = deleted_chunks as i64
            * (candidate.bytes / candidate.chunk_hashes.len().max(1) as i64);

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
                "Could only free {:.1} GB of {:.1} GB needed (kept {} items: no proven replica elsewhere)",
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

/// Whether replication of this specific item on another node is proven.
///
/// The beacon registry only records which collections each node claims, not
/// which items it actually holds, so `replica_count` is a per-collection
/// guess, not a per-item fact. A real proof needs the per-item holder map
/// (each node reporting the item ids it holds); that map does not exist yet,
/// so this always returns false and eviction keeps every item. When the item
/// map lands, this is the one place to look it up.
fn item_replication_proven(_candidate: &EvictionCandidate) -> bool {
    false
}

/// Delete the chunks of a removed item that no other catalogue item still
/// references.
///
/// `chunk_refs` holds how many item rows point at each hash, built once per
/// run by `Catalog::chunk_reference_counts`. Each listed hash is decremented;
/// the chunk is only deleted from the store once its count reaches zero.
/// Missing information means keep: a hash with no count entry is never
/// deleted (the catalogue never told us who uses it), and when the counts are
/// incomplete no chunk is deleted at all, because a zero count may just mean
/// an unreadable row.
/// Returns the number of chunks actually deleted.
fn release_item_chunks(
    store: &mut ChunkStore,
    chunk_hashes: &[String],
    chunk_refs: &mut ChunkReferenceCounts,
) -> usize {
    let mut deleted = 0usize;
    for hash in chunk_hashes {
        let Some(remaining) = chunk_refs.counts.get_mut(hash) else {
            continue; // no count for this hash: unknown, not unreferenced — keep it
        };
        *remaining = remaining.saturating_sub(1);
        if *remaining > 0 {
            continue; // still referenced by another item — keep it
        }
        if !chunk_refs.complete {
            continue; // references incomplete: a zero count is not proof — keep it
        }
        if let Ok(true) = store.delete(hash) {
            deleted += 1;
        }
    }
    deleted
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

    fn item(id: &str, collection: &str, hashes: Vec<String>) -> crate::catalog::StacItem {
        crate::catalog::StacItem {
            id: id.to_string(),
            collection: collection.to_string(),
            bbox: [0.0, 0.0, 1.0, 1.0],
            properties: serde_json::json!({}),
            chunk_hashes: hashes,
            created_at: now_ts(),
            geometry: None,
        }
    }

    /// Chunks are content addressed and shared between datasets. Evicting an
    /// item must only delete the chunks nothing else references: the shared
    /// chunk stays, the other item still reconstructs, the unshared chunk goes.
    ///
    /// Goes through the chunk-release step of the eviction loop directly,
    /// because the loop itself never deletes until item replication is proven
    /// (see `collection_claim_alone_keeps_item`).
    #[test]
    fn evicting_an_item_keeps_chunks_shared_with_another_collection() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = Catalog::new(&dir.path().join("catalog.db")).unwrap();
        let mut store = ChunkStore::new(&dir.path().join("store"), 0.0).unwrap();

        let shared = store.put(b"shared between two datasets").unwrap();
        let only_a = store.put(b"only in dataset a").unwrap();

        let a = item("a-1", "dataset-a", vec![shared.clone(), only_a.clone()]);
        let b = item("b-1", "dataset-b", vec![shared.clone()]);
        catalog.add_item(&a).unwrap();
        catalog.add_item(&b).unwrap();

        let mut chunk_refs = catalog.chunk_reference_counts(None).unwrap();
        assert!(chunk_refs.complete);
        assert_eq!(chunk_refs.counts.get(&shared), Some(&2));
        assert_eq!(chunk_refs.counts.get(&only_a), Some(&1));

        // Evict item a
        let deleted = release_item_chunks(&mut store, &a.chunk_hashes, &mut chunk_refs);
        catalog.delete_item(&a.id).unwrap();

        assert_eq!(deleted, 1, "only the unshared chunk may be deleted");
        assert!(!store.has(&only_a), "unshared chunk of the evicted item must be deleted");
        assert!(store.has(&shared), "chunk still referenced by dataset-b must be kept");

        let b_again = catalog.get_item(&b.id).unwrap().expect("item b must survive");
        let bytes = crate::reconstruct::reconstruct_raw(&b_again, &mut store)
            .expect("item b must still reconstruct after evicting item a");
        assert_eq!(bytes, b"shared between two datasets");
    }

    /// A catalogue row whose chunk hash list cannot be parsed must not make
    /// its chunks look unreferenced. The counting pass must report that the
    /// references are incomplete, and releasing an item's chunks must then
    /// keep every chunk, even one whose count dropped to zero.
    #[test]
    fn unparseable_catalog_row_keeps_chunks_and_reports_incomplete() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("catalog.db");
        let catalog = Catalog::new(&db_path).unwrap();
        let mut store = ChunkStore::new(&dir.path().join("store"), 0.0).unwrap();

        let shared = store.put(b"also used by the unreadable row").unwrap();
        let only_a = store.put(b"only listed by item a").unwrap();
        let a = item("a-1", "dataset-a", vec![shared.clone(), only_a.clone()]);
        catalog.add_item(&a).unwrap();

        // A second row whose hash list is not valid JSON. In truth it uses
        // `shared`, but the counting pass cannot know that.
        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.execute(
                "INSERT INTO collections (id, title) VALUES ('dataset-b', 'dataset-b')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO items (id, collection, bbox_west, bbox_south, bbox_east, bbox_north, chunk_hashes_json)
                 VALUES ('broken-1', 'dataset-b', 0.0, 0.0, 1.0, 1.0, 'not a json list')",
                [],
            )
            .unwrap();
        }

        let mut chunk_refs = catalog.chunk_reference_counts(None).unwrap();
        assert!(!chunk_refs.complete, "an unreadable row must be reported as incomplete references");
        // The readable row still counts; the broken one adds nothing.
        assert_eq!(chunk_refs.counts.get(&shared), Some(&1));
        assert_eq!(chunk_refs.counts.get(&only_a), Some(&1));

        let deleted = release_item_chunks(&mut store, &a.chunk_hashes, &mut chunk_refs);

        assert_eq!(deleted, 0, "with incomplete references nothing may be deleted");
        assert!(store.has(&shared), "chunk of the unreadable row must be kept");
        assert!(store.has(&only_a), "a zero count is not proof when references are incomplete");
    }

    /// A hash with no entry in the reference count map is unknown, not
    /// unreferenced: it must be kept, even when the counts are complete.
    #[test]
    fn hash_without_count_entry_is_kept() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = Catalog::new(&dir.path().join("catalog.db")).unwrap();
        let mut store = ChunkStore::new(&dir.path().join("store"), 0.0).unwrap();

        let counted = store.put(b"listed in the catalogue").unwrap();
        let uncounted = store.put(b"not listed anywhere the count can see").unwrap();
        catalog.add_item(&item("a-1", "dataset-a", vec![counted.clone()])).unwrap();

        let mut chunk_refs = catalog.chunk_reference_counts(None).unwrap();
        assert!(chunk_refs.complete);
        assert_eq!(chunk_refs.counts.get(&uncounted), None);

        // Release an item that lists both hashes: the counted one reaches
        // zero and goes, the one with no entry stays.
        let deleted = release_item_chunks(
            &mut store,
            &[counted.clone(), uncounted.clone()],
            &mut chunk_refs,
        );

        assert_eq!(deleted, 1, "only the hash with a proven zero count may be deleted");
        assert!(!store.has(&counted));
        assert!(store.has(&uncounted), "a hash with no count entry must be kept");
    }

    /// A node that merely claims a collection in the beacon registry is not
    /// proof that it holds this item. Such an item must be kept (and counted
    /// as kept), never deleted, even though the replica map scores it as
    /// replicated.
    #[test]
    fn collection_claim_alone_keeps_item() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = Catalog::new(&dir.path().join("catalog.db")).unwrap();
        let mut store = ChunkStore::new(&dir.path().join("store"), 0.0).unwrap();

        let hash = store.put(b"the only copy of this item").unwrap();
        let it = item("s2-1", "sentinel-2", vec![hash.clone()]);
        catalog.add_item(&it).unwrap();

        // Beacon registry: two live nodes both claim "sentinel-2".
        let beacon_db = dir.path().join("beacon.db");
        {
            let conn = rusqlite::Connection::open(&beacon_db).unwrap();
            conn.execute_batch(
                "CREATE TABLE beacon_nodes (
                    node_name TEXT NOT NULL,
                    collections_json TEXT NOT NULL,
                    item_count INTEGER NOT NULL,
                    last_seen REAL NOT NULL
                );",
            )
            .unwrap();
            for name in ["beacon", "laptop"] {
                conn.execute(
                    "INSERT INTO beacon_nodes (node_name, collections_json, item_count, last_seen) VALUES (?1, '[\"sentinel-2\"]', 1, ?2)",
                    rusqlite::params![name, now_ts()],
                )
                .unwrap();
            }
        }
        // The claim does make the collection look replicated...
        assert_eq!(build_replica_map(Some(beacon_db.as_path())).get("sentinel-2"), Some(&1));

        // ...but that is not proof, so the item is kept.
        let result = evict(&catalog, &mut store, 0.0, Some(beacon_db.as_path())).unwrap();
        assert_eq!(result.items_deleted, 0, "unproven replication must not delete");
        assert_eq!(result.items_kept, 1, "the item must be counted as kept");
        assert_eq!(result.bytes_freed, 0);
        assert!(store.has(&hash), "the only copy must stay in the store");
        assert!(catalog.get_item(&it.id).unwrap().is_some(), "catalog entry must stay");
    }
}
