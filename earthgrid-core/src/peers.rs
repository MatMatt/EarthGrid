//! Peer registry for EarthGrid federation.
//!
//! Tracks known peer nodes with their URL, node-id, and last-seen timestamp.
//! Provides federated STAC search by fanning out to all alive peers.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Peer
// ---------------------------------------------------------------------------

/// A known EarthGrid peer node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Peer {
    pub url: String,
    pub node_id: String,
    pub node_name: String,
    pub last_seen: f64,
    pub collections: Vec<String>,
    pub item_count: usize,
}

impl Peer {
    pub fn new(url: &str, node_id: &str, node_name: &str) -> Self {
        Self {
            url: url.trim_end_matches('/').to_string(),
            node_id: node_id.to_string(),
            node_name: node_name.to_string(),
            last_seen: now_secs(),
            collections: vec![],
            item_count: 0,
        }
    }

    /// Peer is considered alive if seen within last 5 minutes.
    pub fn alive(&self) -> bool {
        now_secs() - self.last_seen < 300.0
    }

    pub fn touch(&mut self) {
        self.last_seen = now_secs();
    }
}

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

// ---------------------------------------------------------------------------
// PeerRegistry
// ---------------------------------------------------------------------------

/// In-memory registry of known peers.
#[derive(Default, Clone)]
pub struct PeerRegistry {
    /// Keyed by normalised URL (no trailing slash).
    peers: HashMap<String, Peer>,
}

impl PeerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, url: &str, node_id: &str, node_name: &str) -> Peer {
        let peer = Peer::new(url, node_id, node_name);
        self.peers.insert(peer.url.clone(), peer.clone());
        peer
    }

    pub fn remove(&mut self, url: &str) {
        self.peers.remove(url.trim_end_matches('/'));
    }

    pub fn get(&self, url: &str) -> Option<&Peer> {
        self.peers.get(url.trim_end_matches('/'))
    }

    pub fn list(&self) -> Vec<&Peer> {
        self.peers.values().collect()
    }

    /// Update a peer after a successful sync.
    pub fn update_from_info(&mut self, url: &str, info: &NodeInfo) {
        let key = url.trim_end_matches('/').to_string();
        let peer = self.peers.entry(key.clone()).or_insert_with(|| Peer::new(&key, "", ""));
        peer.node_id = info.node_id.clone();
        peer.node_name = info.node_name.clone();
        peer.collections = info.collections.clone();
        peer.item_count = info.item_count;
        peer.touch();
    }
}

// ---------------------------------------------------------------------------
// NodeInfo — what we get back from GET /node-info or GET /
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct NodeInfo {
    #[serde(default)]
    pub node_id: String,
    #[serde(default)]
    pub node_name: String,
    #[serde(default)]
    pub collections: Vec<String>,
    #[serde(default)]
    pub item_count: usize,
}

// ---------------------------------------------------------------------------
// Federation search result item
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederatedItem {
    #[serde(flatten)]
    pub item: serde_json::Value,
    /// URL of the peer that returned this result.
    #[serde(rename = "earthgrid:source_node")]
    pub source_node: String,
}

// ---------------------------------------------------------------------------
// FederatedSearch parameters
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SearchParams {
    pub collections: Option<Vec<String>>,
    pub bbox: Option<[f64; 4]>,
    pub datetime: Option<String>,
    pub limit: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_and_list() {
        let mut reg = PeerRegistry::new();
        reg.add("https://peer1.example.com", "id-1", "node-1");
        reg.add("https://peer2.example.com/", "id-2", "node-2");
        assert_eq!(reg.list().len(), 2);
    }

    #[test]
    fn test_remove() {
        let mut reg = PeerRegistry::new();
        reg.add("https://peer.example.com", "id", "name");
        reg.remove("https://peer.example.com/");
        assert!(reg.list().is_empty());
    }

    #[test]
    fn test_trailing_slash_normalised() {
        let mut reg = PeerRegistry::new();
        reg.add("https://peer.example.com/", "id", "name");
        assert!(reg.get("https://peer.example.com").is_some());
        assert!(reg.get("https://peer.example.com/").is_some());
    }

    #[test]
    fn test_alive_freshly_added() {
        let peer = Peer::new("https://peer.example.com", "id", "name");
        assert!(peer.alive());
    }
}
