//! Beacon Federation — real-time sync between beacons via WebSocket.
//!
//! Each beacon connects to peer beacons and exchanges node events in real-time.
//! On initial connection a full sync is performed, then individual events are pushed.
//! Loop prevention: events received from remote beacons are applied locally but NOT re-broadcast.

use std::time::Duration;

use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::beacon::{BeaconNode, BeaconState};

// ---------------------------------------------------------------------------
// Event types
// ---------------------------------------------------------------------------

/// Events broadcast between federated beacons.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BeaconEvent {
    #[serde(rename = "node_register")]
    NodeRegister {
        node: BeaconNode,
        beacon_origin: String,
        ts: f64,
    },
    #[serde(rename = "node_heartbeat")]
    NodeHeartbeat {
        node: BeaconNode,
        beacon_origin: String,
        ts: f64,
    },
    #[serde(rename = "node_pruned")]
    NodePruned {
        node_id: String,
        beacon_origin: String,
        ts: f64,
    },
    #[serde(rename = "full_sync")]
    FullSync {
        nodes: Vec<BeaconNode>,
        beacon_origin: String,
        ts: f64,
    },
}

// ---------------------------------------------------------------------------
// Federation state
// ---------------------------------------------------------------------------

/// Shared federation state, added to BeaconState.
#[derive(Clone)]
pub struct FederationState {
    /// Broadcast channel for local events (register/heartbeat/prune).
    pub tx: broadcast::Sender<BeaconEvent>,
    /// This beacon's unique ID (generated on startup).
    pub beacon_id: String,
}

impl FederationState {
    pub fn new(beacon_id: String) -> Self {
        let (tx, _) = broadcast::channel(256);
        Self { tx, beacon_id }
    }

    /// Broadcast a local event to all connected peer beacons.
    pub fn broadcast(&self, event: BeaconEvent) {
        // Ignore send errors (no receivers = no peers connected)
        let _ = self.tx.send(event);
    }
}

// ---------------------------------------------------------------------------
// WebSocket handler (inbound connections from peer beacons)
// ---------------------------------------------------------------------------

/// Header carrying the federation credential on the WebSocket handshake.
pub const FEDERATION_KEY_HEADER: &str = "x-earthgrid-federation-key";

/// The credential two beacons share in order to federate.
///
/// Deliberately *not* the node's `EARTHGRID_API_KEY`. A federated peer needs
/// exactly one capability — beacon registry sync — and nothing else. Reusing
/// the grid key would hand every peer write access to the whole node API
/// (`/api/fetch`, `/api/replicate`, the fetch queue, …), because the dialer
/// must transmit whatever key the listener checks.
#[derive(Debug, Clone, Default)]
pub struct FederationAuth {
    key: String,
}

impl FederationAuth {
    /// Read `EARTHGRID_FEDERATION_KEY` from the environment.
    pub fn from_env() -> Self {
        Self {
            key: std::env::var("EARTHGRID_FEDERATION_KEY").unwrap_or_default(),
        }
    }

    /// Whether a federation key has been configured.
    pub fn is_configured(&self) -> bool {
        !self.key.is_empty()
    }

    /// The configured key, for the outbound dialer.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Whether `presented` matches the configured key.
    ///
    /// Returns false when nothing is configured: federation **fails closed**,
    /// so an unconfigured beacon refuses to federate rather than accepting
    /// everyone. Comparison is constant-time.
    pub fn verify(&self, presented: Option<&str>) -> bool {
        if !self.is_configured() {
            return false;
        }
        presented.is_some_and(|p| crate::auth::constant_time_eq_str(p, &self.key))
    }
}

/// GET /beacon/ws — upgrade to WebSocket for federation sync.
///
/// A peer on this socket is not a passive reader: `apply_remote_event` lets it
/// upsert nodes into the registry and, via `NodePruned`, **delete** any node by
/// ID. Previously the upgrade was unauthenticated, so anyone who could reach
/// the port could wipe a beacon's registry or fill it with fabricated nodes.
///
/// Fails closed. With no `EARTHGRID_FEDERATION_KEY` configured the endpoint is
/// disabled outright — an unconfigured beacon federates with nobody instead of
/// with everybody.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<BeaconState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !state.federation_auth.is_configured() {
        warn!("Federation: WS refused — EARTHGRID_FEDERATION_KEY is not set");
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "beacon federation is disabled: set EARTHGRID_FEDERATION_KEY to enable it",
        )
            .into_response();
    }

    let presented = headers
        .get(FEDERATION_KEY_HEADER)
        .and_then(|v| v.to_str().ok());

    if !state.federation_auth.verify(presented) {
        warn!("Federation: rejecting WS upgrade with missing or invalid federation key");
        return (StatusCode::UNAUTHORIZED, "invalid federation key").into_response();
    }

    ws.on_upgrade(move |socket| handle_peer_connection(socket, state))
        .into_response()
}

async fn handle_peer_connection(socket: WebSocket, state: BeaconState) {
    let Some(ref fed) = state.federation else {
        warn!("Federation not enabled, dropping WS connection");
        return;
    };

    let beacon_id = fed.beacon_id.clone();
    let (mut sender, mut receiver) = socket.split();

    // 1. Send full sync to the connecting peer
    {
        let registry = state.registry.lock().await;
        let nodes = registry.list(false).unwrap_or_default();
        let event = BeaconEvent::FullSync {
            nodes,
            beacon_origin: beacon_id.clone(),
            ts: now_ts(),
        };
        if let Ok(json) = serde_json::to_string(&event) {
            let _ = sender.send(Message::Text(json.into())).await;
        }
    }

    // 2. Subscribe to local broadcast channel for outbound events
    let mut rx = fed.tx.subscribe();

    // 3. Spawn task to forward local events to this peer
    let send_beacon_id = beacon_id.clone();
    let mut send_task = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    // Don't forward events that originated from a remote beacon
                    let origin = match &event {
                        BeaconEvent::NodeRegister { beacon_origin, .. } => beacon_origin,
                        BeaconEvent::NodeHeartbeat { beacon_origin, .. } => beacon_origin,
                        BeaconEvent::NodePruned { beacon_origin, .. } => beacon_origin,
                        BeaconEvent::FullSync { beacon_origin, .. } => beacon_origin,
                    };
                    if origin != &send_beacon_id {
                        // This event came from another beacon, don't re-broadcast
                        continue;
                    }
                    if let Ok(json) = serde_json::to_string(&event) {
                        if sender.send(Message::Text(json.into())).await.is_err() {
                            break;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("Federation WS lagged by {} events", n);
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    // 4. Receive events from the peer and apply locally
    let recv_state = state.clone();
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            match msg {
                Message::Text(text) => {
                    if let Ok(event) = serde_json::from_str::<BeaconEvent>(&text) {
                        apply_remote_event(&recv_state, event).await;
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
    });

    // Wait for either task to finish, then abort the other
    tokio::select! {
        _ = &mut send_task => { recv_task.abort(); }
        _ = &mut recv_task => { send_task.abort(); }
    }

    info!("Federation peer disconnected");
}

/// Apply an event received from a remote beacon to local DB.
/// These updates are NOT re-broadcast (loop prevention).
async fn apply_remote_event(state: &BeaconState, event: BeaconEvent) {
    let registry = state.registry.lock().await;

    match event {
        BeaconEvent::FullSync { nodes, beacon_origin, .. } => {
            info!("Federation full_sync from {} — {} nodes", beacon_origin, nodes.len());
            for node in nodes {
                upsert_if_newer(&registry, &node);
            }
        }
        BeaconEvent::NodeRegister { node, beacon_origin, .. } => {
            debug!("Federation register from {}: {}", beacon_origin, node.node_id);
            upsert_if_newer(&registry, &node);
        }
        BeaconEvent::NodeHeartbeat { node, beacon_origin, .. } => {
            debug!("Federation heartbeat from {}: {}", beacon_origin, node.node_id);
            upsert_if_newer(&registry, &node);
        }
        BeaconEvent::NodePruned { node_id, beacon_origin, .. } => {
            debug!("Federation prune from {}: {}", beacon_origin, node_id);
            let _ = registry.remove(&node_id);
        }
    }
}

/// Insert or update a node only if the incoming last_seen is newer.
fn upsert_if_newer(registry: &crate::beacon::BeaconRegistry, node: &BeaconNode) {
    // Check if we already have this node with a newer timestamp
    if let Ok(Some(existing)) = registry.get(&node.node_id) {
        if existing.last_seen >= node.last_seen {
            return; // Our data is newer or same, skip
        }
    }

    // Use federated_upsert to bypass the URL-conflict check
    if let Err(e) = registry.federated_upsert(node) {
        warn!("Federation upsert failed for {}: {}", node.node_id, e);
    }
}

// ---------------------------------------------------------------------------
// Outbound: connect to peer beacons
// ---------------------------------------------------------------------------

/// Spawn background tasks that connect to each peer beacon via WebSocket.
pub fn spawn_peer_connections(state: BeaconState, peer_urls: Vec<String>) {
    if !state.federation_auth.is_configured() {
        warn!(
            "Federation: {} peer(s) configured but EARTHGRID_FEDERATION_KEY is not set — \
             not connecting. Set the same key on every federated beacon.",
            peer_urls.len()
        );
        return;
    }

    for url in peer_urls {
        let state = state.clone();
        tokio::spawn(async move {
            connect_to_peer_loop(state, url).await;
        });
    }
}

/// Build the WebSocket handshake request for a peer beacon, attaching the
/// shared federation key.
fn build_federation_request(
    ws_url: &str,
    federation_key: &str,
) -> std::result::Result<tokio_tungstenite::tungstenite::handshake::client::Request, String> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    if federation_key.is_empty() {
        return Err("no federation key configured".to_string());
    }

    let mut request = ws_url
        .into_client_request()
        .map_err(|e| format!("invalid websocket url: {e}"))?;

    let value = federation_key
        .parse()
        .map_err(|_| "federation key is not a valid header value".to_string())?;
    request.headers_mut().insert(FEDERATION_KEY_HEADER, value);

    Ok(request)
}

/// Connect to a single peer beacon, reconnecting with exponential backoff.
async fn connect_to_peer_loop(state: BeaconState, peer_url: String) {
    let ws_url = peer_url
        .trim_end_matches('/')
        .replace("http://", "ws://")
        .replace("https://", "wss://")
        + "/api/beacon/ws";

    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(60);

    loop {
        info!("Federation: connecting to peer {}", ws_url);

        // Present the shared federation key so the peer's `ws_handler` accepts
        // us. Rebuilt each attempt because `connect_async` consumes the request.
        let request = match build_federation_request(&ws_url, state.federation_auth.key()) {
            Ok(r) => r,
            Err(e) => {
                warn!("Federation: cannot build request for {}: {}", ws_url, e);
                return;
            }
        };

        match tokio_tungstenite::connect_async(request).await {
            Ok((ws_stream, _)) => {
                info!("Federation: connected to {}", ws_url);
                backoff = Duration::from_secs(1); // Reset backoff on success

                let (mut write, mut read) = ws_stream.split();

                let Some(ref fed) = state.federation else { return; };
                let beacon_id = fed.beacon_id.clone();
                let mut rx = fed.tx.subscribe();

                // Send our full sync to the peer
                {
                    let registry = state.registry.lock().await;
                    let nodes = registry.list(false).unwrap_or_default();
                    let event = BeaconEvent::FullSync {
                        nodes,
                        beacon_origin: beacon_id.clone(),
                        ts: now_ts(),
                    };
                    if let Ok(json) = serde_json::to_string(&event) {
                        let _ = write.send(tokio_tungstenite::tungstenite::Message::Text(json.into())).await;
                    }
                }

                // Forward local events to peer
                let send_beacon_id = beacon_id.clone();
                let mut send_task = tokio::spawn(async move {
                    loop {
                        match rx.recv().await {
                            Ok(event) => {
                                let origin = match &event {
                                    BeaconEvent::NodeRegister { beacon_origin, .. } => beacon_origin,
                                    BeaconEvent::NodeHeartbeat { beacon_origin, .. } => beacon_origin,
                                    BeaconEvent::NodePruned { beacon_origin, .. } => beacon_origin,
                                    BeaconEvent::FullSync { beacon_origin, .. } => beacon_origin,
                                };
                                if origin != &send_beacon_id {
                                    continue; // Don't forward remote events
                                }
                                if let Ok(json) = serde_json::to_string(&event) {
                                    if write.send(tokio_tungstenite::tungstenite::Message::Text(json.into())).await.is_err() {
                                        break;
                                    }
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                warn!("Federation outbound lagged by {} events", n);
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                });

                // Receive events from peer
                let recv_state = state.clone();
                let mut recv_task = tokio::spawn(async move {
                    while let Some(Ok(msg)) = read.next().await {
                        match msg {
                            tokio_tungstenite::tungstenite::Message::Text(text) => {
                                if let Ok(event) = serde_json::from_str::<BeaconEvent>(&text) {
                                    apply_remote_event(&recv_state, event).await;
                                }
                            }
                            tokio_tungstenite::tungstenite::Message::Close(_) => break,
                            _ => {}
                        }
                    }
                });

                tokio::select! {
                    _ = &mut send_task => { recv_task.abort(); }
                    _ = &mut recv_task => { send_task.abort(); }
                }

                warn!("Federation: disconnected from {}", ws_url);
            }
            Err(e) => {
                warn!("Federation: failed to connect to {}: {}", ws_url, e);
            }
        }

        // Exponential backoff
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

fn now_ts() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_serialization() {
        let event = BeaconEvent::NodeRegister {
            node: BeaconNode {
                node_id: "n1".into(),
                node_name: "Test".into(),
                url: "http://localhost:8400".into(),
                collections: vec!["sentinel-2-l2a".into()],
                item_count: 100,
                chunk_count: 500,
                chunks_bytes: 1_000_000,
                can_source: true,
                storage_limit_gb: 100.0,
                last_seen: 1234567890.0,
                sponsor_name: None,
                sponsor_url: None,
                node_url: None,
                group_id: None,
                uptime_seconds: 3600,
                catalog_version: 0,
                alive: true,
            },
            beacon_origin: "beacon-abc".into(),
            ts: 1234567890.0,
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("node_register"));
        assert!(json.contains("beacon-abc"));

        let parsed: BeaconEvent = serde_json::from_str(&json).unwrap();
        match parsed {
            BeaconEvent::NodeRegister { node, beacon_origin, .. } => {
                assert_eq!(node.node_id, "n1");
                assert_eq!(beacon_origin, "beacon-abc");
            }
            _ => panic!("Wrong event type"),
        }
    }

    #[test]
    fn test_event_pruned_serialization() {
        let event = BeaconEvent::NodePruned {
            node_id: "dead-node".into(),
            beacon_origin: "beacon-xyz".into(),
            ts: 9999999.0,
        };
        let json = serde_json::to_string(&event).unwrap();
        let parsed: BeaconEvent = serde_json::from_str(&json).unwrap();
        match parsed {
            BeaconEvent::NodePruned { node_id, beacon_origin, .. } => {
                assert_eq!(node_id, "dead-node");
                assert_eq!(beacon_origin, "beacon-xyz");
            }
            _ => panic!("Wrong event type"),
        }
    }

    #[test]
    fn test_full_sync_serialization() {
        let event = BeaconEvent::FullSync {
            nodes: vec![],
            beacon_origin: "beacon-1".into(),
            ts: 1000.0,
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("full_sync"));
        assert!(json.contains("beacon-1"));
    }

    #[test]
    fn federation_request_carries_federation_key() {
        let req = build_federation_request("ws://beacon.example/api/beacon/ws", "fed-secret")
            .expect("request should build");
        assert_eq!(
            req.headers()
                .get(FEDERATION_KEY_HEADER)
                .map(|v| v.to_str().unwrap()),
            Some("fed-secret"),
            "dialer must present the key the peer's ws_handler requires"
        );
        assert!(
            req.headers().get("x-api-key").is_none(),
            "the node's grid API key must never be sent to a federated peer"
        );
    }

    #[test]
    fn federation_request_requires_a_key() {
        assert!(
            build_federation_request("ws://beacon.example/api/beacon/ws", "").is_err(),
            "dialing without a federation key must fail rather than connect anonymously"
        );
    }

    #[test]
    fn federation_request_rejects_bad_url() {
        assert!(build_federation_request("not a url", "k").is_err());
    }

    /// Federation must fail closed: an unconfigured beacon federates with
    /// nobody, rather than accepting every anonymous client.
    #[test]
    fn unconfigured_federation_auth_rejects_everything() {
        let auth = FederationAuth::default();
        assert!(!auth.is_configured());
        assert!(!auth.verify(None));
        assert!(!auth.verify(Some("")));
        assert!(!auth.verify(Some("anything")));
    }

    #[test]
    fn configured_federation_auth_accepts_only_the_key() {
        let auth = FederationAuth { key: "correct-horse".to_string() };
        assert!(auth.is_configured());
        assert!(auth.verify(Some("correct-horse")));

        assert!(!auth.verify(None));
        assert!(!auth.verify(Some("")));
        assert!(!auth.verify(Some("wrong")));
        assert!(!auth.verify(Some("correct-horse ")), "no trimming");
        assert!(!auth.verify(Some("correct-hors")), "prefix must not pass");
        assert!(!auth.verify(Some("correct-horse-battery")), "extension must not pass");
    }
}
