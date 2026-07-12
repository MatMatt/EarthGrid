//! EarthGrid Client — M2M access to any EarthGrid node.
//!
//! Ported from client.py.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::Value;

use crate::error::{EarthGridError, Result};

// ---------------------------------------------------------------------------
// Item
// ---------------------------------------------------------------------------

/// A STAC item returned from search.
#[derive(Debug, Clone)]
pub struct Item {
    pub id: String,
    pub collection: String,
    pub bbox: Vec<f64>,
    pub properties: Value,
    pub assets: Value,
    pub source_node: String,
}

impl Item {
    fn from_json(data: &Value) -> Self {
        let id = data["id"].as_str().unwrap_or("").to_string();
        let collection = data["collection"].as_str().unwrap_or("").to_string();
        let bbox: Vec<f64> = data["bbox"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_f64()).collect())
            .unwrap_or_default();
        let properties = data.get("properties").cloned().unwrap_or_default();
        let assets = data.get("assets").cloned().unwrap_or_default();
        let source_node = properties
            .get("earthgrid:source_node")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        Item { id, collection, bbox, properties, assets, source_node }
    }

    /// Number of chunks in the data asset.
    pub fn chunk_count(&self) -> u64 {
        self.assets
            .get("data")
            .and_then(|a| a.get("earthgrid:chunk_count"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
    }

    /// Datetime string from properties.
    pub fn datetime(&self) -> &str {
        self.properties
            .get("datetime")
            .and_then(|v| v.as_str())
            .unwrap_or("")
    }
}

impl std::fmt::Display for Item {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Item({}, collection={})", self.id, self.collection)
    }
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// EarthGrid M2M client.
///
/// ```ignore
/// use earthgrid_core::client::Client;
/// let client = Client::new("http://localhost:8400", None, 30);
/// let results = client.search(serde_json::json!({
///     "collections": "sentinel-2-l2a",
///     "bbox": [11, 47, 12, 48]
/// })).await.unwrap();
/// ```
pub struct Client {
    pub base_url: String,
    pub api_key: Option<String>,
    http: reqwest::Client,
}

impl Client {
    /// Create a new client.
    pub fn new(base_url: &str, api_key: Option<&str>, timeout_secs: u64) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .unwrap_or_default();
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key: api_key.map(str::to_string),
            http,
        }
    }

    /// Add API key header if configured.
    fn with_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(key) = &self.api_key {
            req.header("x-api-key", key)
        } else {
            req
        }
    }

    /// GET a JSON endpoint.
    async fn get_json(&self, path: &str) -> Result<Value> {
        let url = format!("{}{}", self.base_url, path);
        let req = self.with_auth(self.http.get(&url));
        let resp = req.send().await.map_err(|e| EarthGridError::Other(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(EarthGridError::Other(format!(
                "HTTP {} from {}",
                resp.status(),
                url
            )));
        }
        resp.json::<Value>()
            .await
            .map_err(|e| EarthGridError::Other(e.to_string()))
    }

    /// GET raw bytes.
    async fn get_bytes(&self, path: &str) -> Result<Vec<u8>> {
        let url = format!("{}{}", self.base_url, path);
        let req = self.with_auth(self.http.get(&url));
        let resp = req.send().await.map_err(|e| EarthGridError::Other(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(EarthGridError::Other(format!(
                "HTTP {} from {}",
                resp.status(),
                url
            )));
        }
        resp.bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| EarthGridError::Other(e.to_string()))
    }

    // -------------------------------------------------------------------------
    // Node info
    // -------------------------------------------------------------------------

    /// Check node health. Returns the full health JSON.
    pub async fn health(&self) -> Result<Value> {
        self.get_json("/health").await
    }

    /// Get detailed node statistics.
    pub async fn status(&self) -> Result<Value> {
        self.get_json("/stats").await
    }

    /// Get node info (landing page).
    pub async fn info(&self) -> Result<Value> {
        self.get_json("/").await
    }

    // -------------------------------------------------------------------------
    // Search
    // -------------------------------------------------------------------------

    /// Search for STAC items.
    ///
    /// `query` may contain: `collections`, `bbox` (array or comma string),
    /// `datetime`, `limit`, `offset`.
    pub async fn search(&self, query: Value) -> Result<Vec<Item>> {
        // Build GET params from query object
        let mut params: Vec<(String, String)> = Vec::new();

        if let Some(c) = query.get("collections").and_then(|v| v.as_str()) {
            params.push(("collections".into(), c.to_string()));
        }
        if let Some(c) = query.get("collection").and_then(|v| v.as_str()) {
            params.push(("collection".into(), c.to_string()));
        }
        if let Some(bbox) = query.get("bbox") {
            let bbox_str = match bbox {
                Value::Array(arr) => arr
                    .iter()
                    .filter_map(|v| v.as_f64())
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
                Value::String(s) => s.clone(),
                _ => String::new(),
            };
            if !bbox_str.is_empty() {
                params.push(("bbox".into(), bbox_str));
            }
        }
        if let Some(dt) = query.get("datetime").and_then(|v| v.as_str()) {
            params.push(("datetime".into(), dt.to_string()));
        }
        let limit = query.get("limit").and_then(|v| v.as_u64()).unwrap_or(100);
        params.push(("limit".into(), limit.to_string()));
        if let Some(offset) = query.get("offset").and_then(|v| v.as_u64()) {
            params.push(("offset".into(), offset.to_string()));
        }

        let url = format!("{}/api/stac/search", self.base_url);
        let req = self.with_auth(self.http.get(&url)).query(&params);
        let resp = req.send().await.map_err(|e| EarthGridError::Other(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(EarthGridError::Other(format!("HTTP {} from /stac/search", resp.status())));
        }
        let data: Value = resp.json().await.map_err(|e| EarthGridError::Other(e.to_string()))?;

        let items = data["features"]
            .as_array()
            .map(|arr| arr.iter().map(Item::from_json).collect())
            .unwrap_or_default();
        Ok(items)
    }

    /// Get a single item by collection and item ID.
    pub async fn get_item(&self, collection: &str, item_id: &str) -> Result<Item> {
        let path = format!("/api/stac/collections/{}/items/{}", collection, item_id);
        let data = self.get_json(&path).await?;
        Ok(Item::from_json(&data))
    }

    // -------------------------------------------------------------------------
    // Download
    // -------------------------------------------------------------------------

    /// Download reconstructed GeoTIFF for an item.
    ///
    /// Uses `GET /download/{collection}/{item_id}`.
    pub async fn download(&self, item: &Item, path: &Path) -> Result<PathBuf> {
        let endpoint = format!("/download/{}/{}", item.collection, item.id);
        let data: Vec<u8> = self.get_bytes(&endpoint).await?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(EarthGridError::Io)?;
        }
        tokio::fs::write(path, &data).await.map_err(EarthGridError::Io)?;
        Ok(path.to_path_buf())
    }

    // -------------------------------------------------------------------------
    // Bootstrap
    // -------------------------------------------------------------------------

    /// Create a Client connected to the first healthy bootstrap peer.
    ///
    /// Reads peers from `EARTHGRID_BOOTSTRAP_PEERS` or `EARTHGRID_PEERS` env var.
    pub async fn from_bootstrap(timeout: u64) -> Result<Self> {
        let peers_raw = std::env::var("EARTHGRID_BOOTSTRAP_PEERS")
            .or_else(|_| std::env::var("EARTHGRID_PEERS"))
            .unwrap_or_default();

        let peers: Vec<&str> = peers_raw.split(',').map(str::trim).filter(|s| !s.is_empty()).collect();

        if peers.is_empty() {
            return Err(EarthGridError::Other(
                "No bootstrap peers configured (EARTHGRID_BOOTSTRAP_PEERS)".into(),
            ));
        }

        let probe = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout))
            .build()
            .unwrap_or_default();

        for peer in peers {
            let health_url = format!("{}/health", peer.trim_end_matches('/'));
            if let Ok(resp) = probe.get(&health_url).send().await {
                if resp.status().is_success() {
                    return Ok(Self::new(peer, None, timeout));
                }
            }
        }

        Err(EarthGridError::Other("No healthy bootstrap peer found".into()))
    }
}

impl std::fmt::Display for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Client({})", self.base_url)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_item_from_json() {
        let data = serde_json::json!({
            "id": "test-item",
            "collection": "sentinel-2",
            "bbox": [10.0, 46.0, 11.0, 47.0],
            "properties": {
                "datetime": "2024-01-01T00:00:00Z",
                "earthgrid:source_node": "node-abc"
            },
            "assets": {
                "data": {
                    "earthgrid:chunk_count": 42
                }
            }
        });
        let item = Item::from_json(&data);
        assert_eq!(item.id, "test-item");
        assert_eq!(item.collection, "sentinel-2");
        assert_eq!(item.bbox, vec![10.0, 46.0, 11.0, 47.0]);
        assert_eq!(item.datetime(), "2024-01-01T00:00:00Z");
        assert_eq!(item.chunk_count(), 42);
        assert_eq!(item.source_node, "node-abc");
    }

    #[test]
    fn test_client_new() {
        let c = Client::new("http://localhost:8400", Some("key123"), 30);
        assert_eq!(c.base_url, "http://localhost:8400");
        assert_eq!(c.api_key, Some("key123".to_string()));
    }

    #[test]
    fn test_client_strips_trailing_slash() {
        let c = Client::new("http://localhost:8400/", None, 10);
        assert_eq!(c.base_url, "http://localhost:8400");
    }

    #[test]
    fn test_item_display() {
        let item = Item {
            id: "abc".into(),
            collection: "s2".into(),
            bbox: vec![],
            properties: Value::Null,
            assets: Value::Null,
            source_node: String::new(),
        };
        assert_eq!(format!("{}", item), "Item(abc, collection=s2)");
    }
}
