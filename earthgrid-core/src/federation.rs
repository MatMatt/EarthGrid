//! Federated search across EarthGrid peer nodes.
//!
//! Fans out STAC searches to all peers in parallel (tokio + reqwest),
//! then merges and deduplicates results by item ID.
//!
//! Ported from federation.py.

use std::collections::HashSet;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::catalog::StacItem;

// ---------------------------------------------------------------------------
// Search parameters
// ---------------------------------------------------------------------------

/// Parameters for a federated STAC search.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FederatedQuery {
    /// Collection IDs to search (empty = all).
    pub collections: Vec<String>,
    /// Bounding box [west, south, east, north].
    pub bbox: Option<[f64; 4]>,
    /// STAC datetime range string, e.g. "2020-01-01T00:00:00Z/2020-12-31T23:59:59Z".
    pub datetime: Option<String>,
    /// Max results per peer.
    pub limit: usize,
}

// ---------------------------------------------------------------------------
// Internal wire type (STAC FeatureCollection)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct StacSearchResponse {
    features: Option<Vec<Value>>,
}

// ---------------------------------------------------------------------------
// federated_search
// ---------------------------------------------------------------------------

/// Fan out a STAC search to all `peers`, merge and deduplicate results.
///
/// Each peer is queried at `<peer>/stac/search` with `GET` params.
/// Results are deduplicated by STAC item ID.
/// Returns at most `query.limit` items total.
///
/// # Arguments
/// * `peers`   – base URLs of peer nodes (e.g. `["http://node1:8080"]`)
/// * `query`   – search parameters
/// * `timeout` – per-peer HTTP timeout
pub async fn federated_search(
    peers: &[String],
    query: &FederatedQuery,
    timeout: Duration,
) -> Vec<StacItem> {
    let limit = if query.limit == 0 { 100 } else { query.limit };

    // Launch one search task per peer in parallel.
    let tasks: Vec<_> = peers
        .iter()
        .map(|url| {
            let url = url.trim_end_matches('/').to_string();
            let query = query.clone();
            tokio::spawn(async move { search_peer(&url, &query, timeout).await })
        })
        .collect();

    // Collect all results.
    let mut all_items: Vec<StacItem> = Vec::new();
    for task in tasks {
        if let Ok(items) = task.await {
            all_items.extend(items);
        }
    }

    // Deduplicate by item ID (same hash = same data across nodes).
    let mut seen: HashSet<String> = HashSet::new();
    let mut deduped: Vec<StacItem> = Vec::new();
    for item in all_items {
        if seen.insert(item.id.clone()) {
            deduped.push(item);
            if deduped.len() >= limit {
                break;
            }
        }
    }

    deduped
}

// ---------------------------------------------------------------------------
// Per-peer search helper
// ---------------------------------------------------------------------------

async fn search_peer(
    base_url: &str,
    query: &FederatedQuery,
    timeout: Duration,
) -> Vec<StacItem> {
    let client = match reqwest::Client::builder().timeout(timeout).build() {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };

    let mut params: Vec<(&str, String)> = vec![
        ("limit", query.limit.max(1).to_string()),
    ];
    if !query.collections.is_empty() {
        params.push(("collections", query.collections.join(",")));
    }
    if let Some(bbox) = &query.bbox {
        params.push(("bbox", format!("{},{},{},{}", bbox[0], bbox[1], bbox[2], bbox[3])));
    }
    if let Some(dt) = &query.datetime {
        params.push(("datetime", dt.clone()));
    }

    let url = format!("{}/stac/search", base_url);
    let resp = match client.get(&url).query(&params).send().await {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };

    if !resp.status().is_success() {
        return Vec::new();
    }

    let body: StacSearchResponse = match resp.json().await {
        Ok(b) => b,
        Err(_) => return Vec::new(),
    };

    // Convert raw JSON features → StacItem.
    body.features
        .unwrap_or_default()
        .into_iter()
        .filter_map(|f| json_to_stac_item(f, base_url))
        .collect()
}

// ---------------------------------------------------------------------------
// JSON → StacItem conversion
// ---------------------------------------------------------------------------

fn json_to_stac_item(v: Value, source_node: &str) -> Option<StacItem> {
    let id = v.get("id")?.as_str()?.to_string();
    let collection = v
        .get("collection")
        .and_then(|c| c.as_str())
        .unwrap_or("")
        .to_string();

    // Parse bbox: GeoJSON spec is [west, south, east, north]
    let bbox = v
        .get("bbox")
        .and_then(|b| b.as_array())
        .and_then(|a| {
            if a.len() >= 4 {
                Some([
                    a[0].as_f64()?,
                    a[1].as_f64()?,
                    a[2].as_f64()?,
                    a[3].as_f64()?,
                ])
            } else {
                None
            }
        })
        .unwrap_or([0.0, 0.0, 0.0, 0.0]);

    let mut properties = v
        .get("properties")
        .cloned()
        .unwrap_or(Value::Object(serde_json::Map::new()));

    // Tag with source node for traceability.
    if let Value::Object(ref mut map) = properties {
        map.insert(
            "earthgrid:source_node".to_string(),
            Value::String(source_node.to_string()),
        );
    }

    let chunk_hashes = v
        .get("earthgrid:chunk_hashes")
        .and_then(|c| c.as_array())
        .map(|a| a.iter().filter_map(|h| h.as_str().map(str::to_string)).collect())
        .unwrap_or_default();

    let created_at = properties
        .get("created")
        .and_then(|c| c.as_f64())
        .unwrap_or(0.0);

    Some(StacItem {
        id,
        collection,
        bbox,
        properties,
        chunk_hashes,
        created_at,
        geometry: None,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_defaults() {
        let q = FederatedQuery::default();
        assert_eq!(q.limit, 0);
        assert!(q.collections.is_empty());
        assert!(q.bbox.is_none());
    }
}
