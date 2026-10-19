use axum::{
    extract::{ConnectInfo, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use std::net::SocketAddr;

use crate::auth::AccessLevel;
use crate::server::{AppState, api_key, authorize, err};
use crate::routes::chunks::{is_forwarded, HOP_HEADER};
use crate::peers::NodeInfo;


#[derive(Deserialize)]
pub struct RegisterPeerQuery {
    pub url: String,
    pub node_id: Option<String>,
    pub node_name: Option<String>,
}


// ---------------------------------------------------------------------------
// Phase 2: Peers + Federation handlers
// ---------------------------------------------------------------------------

/// GET /peers — list all known peers
pub(crate) async fn list_peers(State(state): State<AppState>) -> Json<serde_json::Value> {
    let registry = state.peers.lock().await;
    let peers: Vec<serde_json::Value> = registry
        .list()
        .into_iter()
        .map(|p| serde_json::json!({
            "url": p.url,
            "node_id": p.node_id,
            "node_name": p.node_name,
            "alive": p.alive(),
            "last_seen": p.last_seen,
            "collections": p.collections,
            "item_count": p.item_count,
        }))
        .collect();
    let count = peers.len();
    Json(serde_json::json!({"peers": peers, "count": count}))
}


/// POST /peers?url=...&node_id=...&node_name=... — register a peer
pub(crate) async fn register_peer(
    State(state): State<AppState>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Query(q): Query<RegisterPeerQuery>,
) -> impl IntoResponse {
    // A registered peer is not passive: the heartbeat/gossip loop contacts it
    // every 60s and the auto-replication loop pulls items and chunks from it
    // every 5 minutes. Letting anyone register a URL turns one unauthenticated
    // request into a standing data-ingestion channel from an attacker's server.
    if let Err(e) = authorize(
        &state.auth,
        state.user_auth.as_deref(),
        &headers,
        addr,
        AccessLevel::Write,
        &state.data_dir,
    ) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }

    if q.url.is_empty() {
        return err(StatusCode::BAD_REQUEST, "url is required").into_response();
    }
    // Outbound URL policy: the heartbeat and replication loops will fetch this
    // URL, so it is checked before it is stored. A private address is accepted
    // only if the operator configured its host as a peer.
    let known_hosts = crate::url_policy::operator_hosts();
    if let Err(e) = crate::url_policy::validate_outbound_url_async(&q.url, known_hosts).await {
        return err(StatusCode::BAD_REQUEST, &e.to_string()).into_response();
    }
    let mut registry = state.peers.lock().await;
    let peer = registry.add(
        &q.url,
        q.node_id.as_deref().unwrap_or(""),
        q.node_name.as_deref().unwrap_or(""),
    );
    (StatusCode::CREATED, Json(serde_json::json!({
        "status": "registered",
        "url": peer.url,
        "node_id": peer.node_id,
    }))).into_response()
}


/// POST /federation/sync — sync with all known peers (fetch their node-info)
pub(crate) async fn federation_sync(
    State(state): State<AppState>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    // Iterates peer URLs and pulls their node-info: without a key, anyone can
    // make this node talk to arbitrary hosts on demand.
    if let Err(e) = authorize(
        &state.auth,
        state.user_auth.as_deref(),
        &headers,
        addr,
        AccessLevel::Write,
        &state.data_dir,
    ) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }
    let peer_urls: Vec<String> = {
        let registry = state.peers.lock().await;
        registry.list().into_iter().map(|p| p.url.clone()).collect()
    };

    // No fallback to `Client::default()`: that client follows redirects.
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()
    {
        Ok(c) => c,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    };

    let mut synced = 0usize;
    let mut failed = 0usize;
    let mut results = vec![];

    let known_hosts = crate::url_policy::operator_hosts();
    for url in &peer_urls {
        // Outbound URL policy — before any request is made to this peer.
        if crate::url_policy::validate_outbound_url_async(url, known_hosts).await.is_err() {
            results.push(serde_json::json!({"url": url, "status": "blocked"}));
            failed += 1;
            continue;
        }
        // Try /node-info first, then fall back to /
        let info_url = format!("{}/api/node-info", url);
        match client.get(&info_url).header(HOP_HEADER, "1").send().await {
            Ok(resp) if resp.status().is_success() => {
                if let Ok(info) = resp.json::<NodeInfo>().await {
                    let mut registry = state.peers.lock().await;
                    registry.update_from_info(url, &info);
                    results.push(serde_json::json!({
                        "url": url,
                        "node_id": info.node_id,
                        "node_name": info.node_name,
                        "status": "synced",
                    }));
                    synced += 1;
                } else {
                    results.push(serde_json::json!({"url": url, "status": "parse_error"}));
                    failed += 1;
                }
            }
            _ => {
                results.push(serde_json::json!({"url": url, "status": "unreachable"}));
                failed += 1;
            }
        }
    }

    Json(serde_json::json!({
        "synced": synced,
        "failed": failed,
        "peers": results,
    }))
        .into_response()
}


/// GET /federation/search — federated STAC search across all alive peers + local
pub(crate) async fn federation_search(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<crate::routes::stac::SearchQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(100).min(1000);

    // 1. Local search
    let local_items = {
        let catalog = state.catalog.lock().await;
        let collection = q.collection.as_deref().or(
            q.collections.as_deref().and_then(|s| s.split(',').next())
        );
        let bbox = q.bbox.as_deref().and_then(crate::routes::stac::parse_bbox_str);
        let datetime = q.datetime.as_deref().and_then(crate::catalog::DatetimeFilter::parse);
        catalog.search(collection, bbox, datetime.as_ref(), limit, 0).unwrap_or_default()
    };

    // 2. Build params for peer queries — no peers if a peer forwarded this request
    let peer_urls: Vec<String> = if is_forwarded(&headers) {
        Vec::new()
    } else {
        let registry = state.peers.lock().await;
        registry.list().into_iter()
            .filter(|p| p.alive())
            .map(|p| p.url.clone())
            .collect()
    };

    // No fallback to `Client::default()`: that client follows redirects.
    let Ok(client) = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .build()
    else {
        return Json(serde_json::json!({
            "type": "FeatureCollection",
            "numberMatched": 0,
            "numberReturned": 0,
            "features": [],
            "context": {"source": "federation"},
        }));
    };

    let mut all_items: Vec<serde_json::Value> = local_items
        .into_iter()
        .map(|i| {
            let mut v = serde_json::to_value(i).unwrap_or_default();
            if let Some(obj) = v.as_object_mut() {
                obj.insert("earthgrid:source_node".to_string(), serde_json::json!("local"));
            }
            v
        })
        .collect();

    // Fan out to peers concurrently
    let mut handles = vec![];
    for url in peer_urls {
        let client = client.clone();
        let q_bbox = q.bbox.clone();
        let q_col = q.collection.clone().or(q.collections.clone());
        let q_dt = q.datetime.clone();
        handles.push(tokio::spawn(async move {
            // Outbound URL policy — before any request is made to this peer.
            if crate::url_policy::validate_outbound_url_async(&url, crate::url_policy::operator_hosts()).await.is_err() {
                return vec![];
            }
            let mut params = vec![("limit", limit.to_string())];
            if let Some(c) = &q_col { params.push(("collections", c.clone())); }
            if let Some(b) = &q_bbox { params.push(("bbox", b.clone())); }
            if let Some(d) = &q_dt { params.push(("datetime", d.clone())); }

            let resp = client
                .get(format!("{}/api/stac/search", url))
                .query(&params)
                .header(HOP_HEADER, "1")
                .send()
                .await;

            match resp {
                Ok(r) if r.status().is_success() => {
                    if let Ok(data) = r.json::<serde_json::Value>().await {
                        let features = data.get("features")
                            .and_then(|f| f.as_array())
                            .cloned()
                            .unwrap_or_default();
                        // Tag each with source node
                        features.into_iter().map(|mut f| {
                            if let Some(obj) = f.as_object_mut() {
                                obj.insert("earthgrid:source_node".to_string(), serde_json::json!(url));
                            }
                            f
                        }).collect::<Vec<_>>()
                    } else { vec![] }
                }
                _ => vec![],
            }
        }));
    }

    for handle in handles {
        if let Ok(items) = handle.await {
            all_items.extend(items);
        }
    }

    // Deduplicate by item id
    let mut seen = std::collections::HashSet::new();
    let deduped: Vec<_> = all_items.into_iter()
        .filter(|item| {
            let id = item.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
            if id.is_empty() { return true; }
            seen.insert(id)
        })
        .take(limit)
        .collect();

    let count = deduped.len();
    Json(serde_json::json!({
        "type": "FeatureCollection",
        "numberMatched": count,
        "numberReturned": count,
        "features": deduped,
        "context": {"source": "federation"},
    }))
}



/// GET /peers.json — gossip-friendly peer list for discovery
pub(crate) async fn peers_json(State(state): State<AppState>) -> Json<serde_json::Value> {
    let registry = state.peers.lock().await;
    let peers: Vec<serde_json::Value> = registry
        .list()
        .into_iter()
        .map(|p| serde_json::json!({
            "url": p.url,
            "node_id": p.node_id,
            "node_name": p.node_name,
        }))
        .collect();
    Json(serde_json::json!({"peers": peers}))
}


// ---------------------------------------------------------------------------
// Sync handler — POST /sync, POST /sync-item
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
pub struct SyncRequest {
    pub peer_url: Option<String>,
    pub collection: Option<String>,
    pub max_items: Option<usize>,
}


pub(crate) async fn sync_from_peer(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<SyncRequest>,
) -> impl IntoResponse {
    let key = api_key(&headers);
    if let Err(e) = state.auth.check_write(key) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }

    let peer_url = match req.peer_url {
        Some(u) => u,
        None => {
            return err(StatusCode::BAD_REQUEST, "peer_url required").into_response();
        }
    };

    state.audit.log("sync", &peer_url, req.collection.as_deref().unwrap_or("*"), true);

    (StatusCode::ACCEPTED, Json(serde_json::json!({
        "status": "sync_started",
        "peer_url": peer_url,
        "collection": req.collection,
        "max_items": req.max_items.unwrap_or(500),
    }))).into_response()
}


#[derive(serde::Deserialize)]
pub struct SyncItemRequest {
    pub item_id: String,
    pub collection: String,
    pub source_url: Option<String>,
}


pub(crate) async fn sync_item(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<SyncItemRequest>,
) -> impl IntoResponse {
    let key = api_key(&headers);
    if let Err(e) = state.auth.check_write(key) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }

    state.audit.log("sync_item", &req.collection, &req.item_id, true);

    (StatusCode::ACCEPTED, Json(serde_json::json!({
        "status": "sync_queued",
        "item_id": req.item_id,
        "collection": req.collection,
        "source_url": req.source_url,
    }))).into_response()
}


// ---------------------------------------------------------------------------
// Federation: user sync
// ---------------------------------------------------------------------------

pub(crate) async fn federation_list_users(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let key = api_key(&headers);
    if let Err(e) = state.auth.check_write(key) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }
    match &state.user_auth {
        Some(ua) => {
            match ua.list_users() {
                Ok(users) => (StatusCode::OK, Json(serde_json::json!({"users": users}))).into_response(),
                Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
            }
        }
        None => (StatusCode::OK, Json(serde_json::json!({"users": []}))).into_response(),
    }
}


#[derive(serde::Deserialize)]
pub struct ImportUsersBody {
    pub users: Vec<serde_json::Value>,
}


pub(crate) async fn federation_import_users(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<ImportUsersBody>,
) -> impl IntoResponse {
    let key = api_key(&headers);
    if let Err(e) = state.auth.check_write(key) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }
    let mut added = 0usize;
    let mut skipped = 0usize;
    if let Some(ua) = &state.user_auth {
        for user in &body.users {
            let username = user.get("username").and_then(|v| v.as_str()).unwrap_or("");
            let role = user.get("role").and_then(|v| v.as_str()).unwrap_or("member");
            if username.is_empty() { skipped += 1; continue; }
            match ua.add_user(username, role) {
                Ok(_) => added += 1,
                Err(_) => skipped += 1, // likely already exists
            }
        }
    } else {
        skipped = body.users.len();
    }
    state.audit.log("user_sync", &format!("added={} skipped={}", added, skipped), "", true);
    (StatusCode::OK, Json(serde_json::json!({
        "added": added,
        "updated": 0,
        "skipped": skipped,
    }))).into_response()
}


#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderName, HeaderValue};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    fn test_state(dir: &std::path::Path) -> AppState {
        AppState {
            store: Arc::new(Mutex::new(crate::chunk_store::ChunkStore::new(&dir.join("store"), 0.0).unwrap())),
            catalog: Arc::new(Mutex::new(crate::catalog::Catalog::new(&dir.join("catalog.db")).unwrap())),
            audit: Arc::new(crate::audit::AuditLog::new(&dir.join("audit.jsonl"))),
            auth: crate::auth::AuthConfig { api_key: String::new(), admin_key: String::new() },
            peers: Arc::new(Mutex::new(crate::peers::PeerRegistry::new())),
            stats: Arc::new(crate::stats::StatsEngine::new(&dir.join("stats.db")).unwrap()),
            perf: Arc::new(crate::perf::PerfRecorder::new()),
            gamification: Arc::new(crate::gamification::GamificationEngine::new(&dir.join("gamification.db")).unwrap()),
            version: String::new(),
            node_id: String::new(),
            node_name: String::new(),
            user_auth: None,
            node_identity: None,
            storage_limit_gb: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            data_dir: dir.to_path_buf(),
            active_requests: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            is_beacon: false,
            ui_enabled: true,
            tile_grid: Arc::new(std::collections::HashMap::new()),
            fetch_queue: Arc::new(crate::fetch_queue::FetchQueueBackend::Local(
                crate::fetch_queue::LocalFetchQueue::new(&dir.join("fetch_queue.db")).unwrap(),
            )),
        }
    }

    /// Run the handler while this test holds the peer registry lock. It can
    /// only finish within `wait` if it never reads the peer list.
    async fn finishes_without_peer_list(headers: HeaderMap, wait: Duration) -> bool {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());
        let _registry = state.peers.lock().await;
        let q = crate::routes::stac::SearchQuery {
            collection: None, collections: None, bbox: None, datetime: None, limit: None, offset: None,
        };
        tokio::time::timeout(wait, federation_search(State(state.clone()), headers, Query(q))).await.is_ok()
    }

    #[tokio::test]
    async fn test_forwarded_search_never_reads_peer_list() {
        let mut headers = HeaderMap::new();
        headers.insert(HeaderName::from_bytes(HOP_HEADER.as_bytes()).unwrap(), HeaderValue::from_static("1"));
        assert!(finishes_without_peer_list(headers, Duration::from_secs(10)).await);
    }

    #[tokio::test]
    async fn test_unmarked_search_reads_peer_list() {
        // Same setup without the marker: the handler waits on the registry, so
        // the test above fails if the guard is removed.
        assert!(!finishes_without_peer_list(HeaderMap::new(), Duration::from_millis(300)).await);
    }
}

