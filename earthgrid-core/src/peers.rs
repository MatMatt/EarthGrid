//! Peer registry for EarthGrid federation.
//!
//! Tracks known peer nodes with their URL, node-id, and last-seen timestamp.
//! Supports gossip-based peer discovery and failure tracking.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Maximum number of peers to track.
pub const MAX_PEERS: usize = 50;

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
    #[serde(default)]
    pub consecutive_failures: u32,
    #[serde(default)]
    pub marked_dead: bool,
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
            consecutive_failures: 0,
            marked_dead: false,
        }
    }

    /// Peer is considered alive if seen within last 5 minutes and not marked dead.
    pub fn alive(&self) -> bool {
        !self.marked_dead && now_secs() - self.last_seen < 300.0
    }

    pub fn touch(&mut self) {
        self.last_seen = now_secs();
        self.consecutive_failures = 0;
        self.marked_dead = false;
    }

    /// Record a sync failure. After 3 consecutive failures, mark dead.
    pub fn record_failure(&mut self) {
        self.consecutive_failures += 1;
        if self.consecutive_failures >= 3 {
            self.marked_dead = true;
        }
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
        let key = url.trim_end_matches('/').to_string();
        if self.peers.len() >= MAX_PEERS && !self.peers.contains_key(&key) {
            // At capacity — don't add new peers
            return Peer::new(url, node_id, node_name);
        }
        let peer = Peer::new(url, node_id, node_name);
        self.peers.insert(key, peer.clone());
        peer
    }

    /// Add a peer only if we don't know it yet (gossip discovery).
    /// Returns true if added.
    pub fn add_if_new(&mut self, url: &str) -> bool {
        let key = url.trim_end_matches('/').to_string();
        if self.peers.contains_key(&key) {
            return false;
        }
        if self.peers.len() >= MAX_PEERS {
            return false;
        }
        self.peers.insert(key.clone(), Peer::new(&key, "", ""));
        true
    }

    pub fn remove(&mut self, url: &str) {
        self.peers.remove(url.trim_end_matches('/'));
    }

    pub fn get(&self, url: &str) -> Option<&Peer> {
        self.peers.get(url.trim_end_matches('/'))
    }

    pub fn get_mut(&mut self, url: &str) -> Option<&mut Peer> {
        self.peers.get_mut(url.trim_end_matches('/'))
    }

    pub fn list(&self) -> Vec<&Peer> {
        self.peers.values().collect()
    }

    pub fn count(&self) -> usize {
        self.peers.len()
    }

    /// Get all peer URLs (for heartbeat iteration).
    pub fn urls(&self) -> Vec<String> {
        self.peers.keys().cloned().collect()
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

    /// Record a failure for a peer.
    pub fn record_failure(&mut self, url: &str) {
        let key = url.trim_end_matches('/').to_string();
        if let Some(peer) = self.peers.get_mut(&key) {
            peer.record_failure();
        }
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
// GossipPeerList — response from GET /peers.json
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct GossipPeerList {
    #[serde(default)]
    pub peers: Vec<GossipPeerEntry>,
}

#[derive(Debug, Deserialize)]
pub struct GossipPeerEntry {
    pub url: String,
    #[serde(default)]
    pub node_id: String,
    #[serde(default)]
    pub node_name: String,
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

    #[test]
    fn test_failure_tracking() {
        let mut peer = Peer::new("https://peer.example.com", "id", "name");
        assert!(peer.alive());
        peer.record_failure();
        assert!(peer.alive()); // 1 failure
        peer.record_failure();
        assert!(peer.alive()); // 2 failures
        peer.record_failure();
        assert!(!peer.alive()); // 3 failures → dead
    }

    #[test]
    fn test_touch_resets_failures() {
        let mut peer = Peer::new("https://peer.example.com", "id", "name");
        peer.record_failure();
        peer.record_failure();
        peer.touch();
        assert_eq!(peer.consecutive_failures, 0);
        assert!(!peer.marked_dead);
    }

    #[test]
    fn test_add_if_new() {
        let mut reg = PeerRegistry::new();
        assert!(reg.add_if_new("https://peer1.example.com"));
        assert!(!reg.add_if_new("https://peer1.example.com")); // duplicate
        assert!(reg.add_if_new("https://peer2.example.com"));
        assert_eq!(reg.count(), 2);
    }

    #[test]
    fn test_max_peers() {
        let mut reg = PeerRegistry::new();
        for i in 0..MAX_PEERS {
            reg.add_if_new(&format!("https://peer{}.example.com", i));
        }
        assert_eq!(reg.count(), MAX_PEERS);
        assert!(!reg.add_if_new("https://overflow.example.com"));
        assert_eq!(reg.count(), MAX_PEERS);
    }
}
