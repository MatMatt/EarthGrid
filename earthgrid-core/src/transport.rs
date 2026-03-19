//! Request/Response protocol for EarthGrid P2P operations.
//!
//! Uses CBOR serialization over libp2p request-response.
//! Supports: chunk transfer, catalog queries, job delegation.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Codec (libp2p CBOR-based request-response)
// ---------------------------------------------------------------------------

/// Codec type for the EarthGrid RPC protocol.
/// Uses libp2p's built-in CBOR codec for serialization.
pub type EarthGridCodec = libp2p::request_response::cbor::Codec<EarthGridRequest, EarthGridResponse>;

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/// Requests that can be sent between EarthGrid nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EarthGridRequest {
    /// Fetch a chunk by its SHA-256 hash.
    GetChunk {
        hash: String,
    },

    /// Search the catalog (STAC-like query).
    SearchCatalog {
        /// Collection filter (optional).
        collection: Option<String>,
        /// Bounding box [west, south, east, north] (optional).
        bbox: Option<[f64; 4]>,
        /// ISO datetime range, e.g. "2024-01-01/2024-12-31".
        datetime: Option<String>,
        /// Max results.
        limit: usize,
    },

    /// Delegate an openEO process graph for execution.
    ExecuteJob {
        /// JSON-encoded process graph.
        process_graph: serde_json::Value,
    },

    /// Get node info (lightweight ping).
    NodeInfo,

    /// Request the peer list for gossip.
    GetPeers,
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// Responses sent back from an EarthGrid node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EarthGridResponse {
    /// Raw chunk data.
    Chunk {
        hash: String,
        data: Vec<u8>,
    },

    /// Chunk not found.
    ChunkNotFound {
        hash: String,
    },

    /// Catalog search results (JSON array of STAC items).
    CatalogResults {
        items: Vec<serde_json::Value>,
        total: usize,
    },

    /// Job execution result (GeoTIFF bytes or error).
    JobResult {
        /// GeoTIFF bytes (empty if error).
        data: Vec<u8>,
        /// Content type (e.g. "image/tiff").
        content_type: String,
    },

    /// Job execution error.
    JobError {
        message: String,
    },

    /// Node information.
    Info {
        node_id: String,
        node_name: String,
        version: String,
        collections: Vec<String>,
        item_count: usize,
        chunk_count: usize,
        storage_bytes: u64,
    },

    /// Peer list for gossip.
    Peers {
        peers: Vec<PeerEntry>,
    },

    /// Generic error.
    Error {
        message: String,
    },
}

/// A peer entry for gossip exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerEntry {
    pub peer_id: String,
    pub addresses: Vec<String>,
    pub node_name: String,
    pub collections: Vec<String>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_serialization() {
        let req = EarthGridRequest::GetChunk {
            hash: "abc123".to_string(),
        };
        let bytes = serde_json::to_vec(&req).unwrap();
        let decoded: EarthGridRequest = serde_json::from_slice(&bytes).unwrap();
        match decoded {
            EarthGridRequest::GetChunk { hash } => assert_eq!(hash, "abc123"),
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_response_serialization() {
        let resp = EarthGridResponse::Chunk {
            hash: "abc123".to_string(),
            data: vec![1, 2, 3, 4],
        };
        let bytes = serde_json::to_vec(&resp).unwrap();
        let decoded: EarthGridResponse = serde_json::from_slice(&bytes).unwrap();
        match decoded {
            EarthGridResponse::Chunk { hash, data } => {
                assert_eq!(hash, "abc123");
                assert_eq!(data, vec![1, 2, 3, 4]);
            }
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_search_request() {
        let req = EarthGridRequest::SearchCatalog {
            collection: Some("sentinel-2-l2a".to_string()),
            bbox: Some([11.0, 46.0, 12.0, 47.0]),
            datetime: Some("2024-01-01/2024-12-31".to_string()),
            limit: 100,
        };
        let bytes = serde_json::to_vec(&req).unwrap();
        let decoded: EarthGridRequest = serde_json::from_slice(&bytes).unwrap();
        match decoded {
            EarthGridRequest::SearchCatalog { collection, bbox, limit, .. } => {
                assert_eq!(collection.unwrap(), "sentinel-2-l2a");
                assert!(bbox.is_some());
                assert_eq!(limit, 100);
            }
            _ => panic!("Wrong variant"),
        }
    }
}
