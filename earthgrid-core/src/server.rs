//! Axum HTTP server for EarthGrid Core.
//!
//! Phase 1: Core STAC/chunk API
//! Phase 2: Peers + Federation (sync, federated search)

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::{
    Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse},
    routing::{delete, get, patch, post},
    Json,
};
use serde::Deserialize;
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;

use crate::{
    audit::AuditLog,
    auth::AuthConfig,
    beacon::{BeaconRegistry, BeaconState, beacon_router},
    catalog::{Catalog, DatetimeFilter, StacItem},
    chunk_store::ChunkStore,
    fetcher,
    gamification::GamificationEngine,
    ingest,
    peers::{GossipPeerList, NodeInfo, PeerRegistry},
    replication::Replicator,
    stats::StatsEngine,
    user_auth::UserAuth,
    node_identity::NodeIdentity,
};
use std::path::PathBuf;



// ---------------------------------------------------------------------------
// Shared State
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Mutex<ChunkStore>>,
    pub catalog: Arc<Mutex<Catalog>>,
    pub audit: Arc<AuditLog>,
    pub auth: AuthConfig,
    pub peers: Arc<Mutex<PeerRegistry>>,
    pub stats: Arc<StatsEngine>,
    pub gamification: Arc<GamificationEngine>,
    pub version: String,
    pub node_id: String,
    pub node_name: String,
    /// User authentication registry (optional, None if init fails).
    pub user_auth: Option<Arc<UserAuth>>,
    /// Node identity keypair (optional, None if init fails).
    pub node_identity: Option<Arc<NodeIdentity>>,
    pub storage_limit_gb: f64,
    /// Data directory (for config updates like resize).
    pub data_dir: PathBuf,
    /// Counter for active fetch/ingest requests (replication yields when > 0).
    pub active_requests: Arc<AtomicUsize>,
    /// Whether this node runs as beacon (shows grid-wide landing page).
    pub is_beacon: bool,
}


/// RAII guard that decrements an AtomicUsize counter on drop.
struct ActiveRequestGuard(Arc<AtomicUsize>);
impl Drop for ActiveRequestGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}
// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn api_key(headers: &HeaderMap) -> Option<&str> {
    headers.get("x-api-key").and_then(|v| v.to_str().ok())
}

fn err(status: StatusCode, msg: &str) -> (StatusCode, Json<serde_json::Value>) {
    (status, Json(serde_json::json!({"error": msg})))
}

// ---------------------------------------------------------------------------
// Query params
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct SearchQuery {
    pub collection: Option<String>,
    pub collections: Option<String>,  // comma-separated
    pub bbox: Option<String>,         // "west,south,east,north"
    pub datetime: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Deserialize, Default)]
pub struct SearchBody {
    pub collection: Option<String>,
    pub collections: Option<Vec<String>>,
    pub bbox: Option<Vec<f64>>,
    pub datetime: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Deserialize)]
pub struct LimitQuery {
    pub limit: Option<usize>,
}

#[derive(Deserialize)]
pub struct RegisterPeerQuery {
    pub url: String,
    pub node_id: Option<String>,
    pub node_name: Option<String>,
}

// ---------------------------------------------------------------------------
// Core handlers
// ---------------------------------------------------------------------------

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

async fn node_info(State(state): State<AppState>) -> Json<serde_json::Value> {
    let store = state.store.lock().await;
    let catalog = state.catalog.lock().await;
    let stats = store.stats().clone();
    let item_count = catalog.item_count(None).unwrap_or(0);
    let collections: Vec<String> = catalog
        .list_collections()
        .unwrap_or_default()
        .into_iter()
        .map(|c| c.id)
        .collect();
    Json(serde_json::json!({
        "version": state.version,
        "node_id": state.node_id,
        "node_name": state.node_name,
        "chunks": store.chunk_count(),
        "storage_bytes": store.total_bytes(),
        "storage_gb": store.total_bytes() as f64 / 1_073_741_824.0,
        "items": item_count,
        "collections": collections,
        "item_count": item_count,
        "auth_enabled": state.auth.is_enabled(),
        "chunks_served": stats.chunks_served,
        "bytes_served": stats.bytes_served,
        "requests_total": stats.requests_total,
        "storage_limit_gb": state.storage_limit_gb,
    }))
}

async fn stats(State(state): State<AppState>) -> Json<serde_json::Value> {
    let store = state.store.lock().await;
    let s = store.stats().clone();
    Json(serde_json::json!({
        "started": s.started,
        "chunks_served": s.chunks_served,
        "bytes_served": s.bytes_served,
        "chunks_stored": s.chunks_stored,
        "bytes_ingested": s.bytes_ingested,
        "requests_total": s.requests_total,
        "total_chunks": store.chunk_count(),
        "total_bytes": store.total_bytes(),
    }))
}

/// GET / — Content-negotiated landing: HTML for browsers, JSON for API clients
async fn stac_landing(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    // Only beacon nodes serve the HTML info page; regular nodes always return STAC JSON
    let accept = headers.get("accept").and_then(|v| v.to_str().ok()).unwrap_or("");
    if state.is_beacon && accept.contains("text/html") && !accept.starts_with("application/json") {
        return landing_html(State(state)).await.into_response();
    }
    stac_landing_json(State(state)).await.into_response()
}

/// JSON STAC Landing
async fn stac_landing_json(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "type": "Catalog",
        "id": state.node_id,
        "title": state.node_name,
        "description": "EarthGrid STAC Catalog",
        "stac_version": "1.0.0",
        "conformsTo": [
            "https://api.stacspec.org/v1.0.0/core",
            "https://api.stacspec.org/v1.0.0/item-search",
            "https://api.stacspec.org/v1.0.0/item-search#fields",
            "https://api.stacspec.org/v1.0.0/item-search#sort",
            "https://api.stacspec.org/v1.0.0/item-search#context",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
        ],
        "links": [
            {"rel": "self", "href": "/", "type": "application/json"},
            {"rel": "root", "href": "/", "type": "application/json"},
            {"rel": "conformance", "href": "/conformance", "type": "application/json"},
            {"rel": "data", "href": "/stac/collections", "type": "application/json"},
            {"rel": "search", "href": "/stac/search", "type": "application/geo+json", "method": "GET"},
            {"rel": "search", "href": "/stac/search", "type": "application/geo+json", "method": "POST"},
        ]
    }))
}

/// GET /conformance — OGC conformance classes
async fn stac_conformance() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "conformsTo": [
            "https://api.stacspec.org/v1.0.0/core",
            "https://api.stacspec.org/v1.0.0/item-search",
            "https://api.stacspec.org/v1.0.0/item-search#fields",
            "https://api.stacspec.org/v1.0.0/item-search#sort",
            "https://api.stacspec.org/v1.0.0/item-search#context",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
        ]
    }))
}

async fn list_collections(State(state): State<AppState>) -> impl IntoResponse {
    let catalog = state.catalog.lock().await;
    match catalog.list_collections() {
        Ok(cols) => {
            let count = cols.len();
            (StatusCode::OK, Json(serde_json::json!({
                "collections": cols,
                "numberMatched": count,
                "links": [
                    {"rel": "self", "href": "/stac/collections", "type": "application/json"},
                    {"rel": "root", "href": "/", "type": "application/json"},
                ]
            }))).into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

async fn get_collection(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    let catalog = state.catalog.lock().await;
    match catalog.get_collection(&id) {
        Ok(Some(col)) => (StatusCode::OK, Json(serde_json::to_value(col).unwrap())).into_response(),
        Ok(None) => err(StatusCode::NOT_FOUND, "Collection not found").into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

async fn collection_items(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<SearchQuery>,
) -> impl IntoResponse {
    let catalog = state.catalog.lock().await;
    let limit = q.limit.unwrap_or(50).min(1000);
    let offset = q.offset.unwrap_or(0);
    let datetime = q.datetime.as_deref().and_then(DatetimeFilter::parse);

    let total = match catalog.search_count(Some(&id), None, datetime.as_ref()) {
        Ok(n) => n,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    };

    match catalog.search(Some(&id), None, datetime.as_ref(), limit, offset) {
        Ok(items) => {
            let returned = items.len();
            let mut links = vec![
                serde_json::json!({"rel": "self", "href": format!("/stac/collections/{}/items", id), "type": "application/geo+json"}),
                serde_json::json!({"rel": "collection", "href": format!("/stac/collections/{}", id), "type": "application/json"}),
                serde_json::json!({"rel": "root", "href": "/", "type": "application/json"}),
            ];
            if offset + limit < total {
                links.push(serde_json::json!({
                    "rel": "next",
                    "href": format!("/stac/collections/{}/items?limit={}&offset={}", id, limit, offset + limit),
                    "type": "application/geo+json"
                }));
            }
            if offset > 0 {
                let prev_offset = offset.saturating_sub(limit);
                links.push(serde_json::json!({
                    "rel": "prev",
                    "href": format!("/stac/collections/{}/items?limit={}&offset={}", id, limit, prev_offset),
                    "type": "application/geo+json"
                }));
            }
            (StatusCode::OK, Json(serde_json::json!({
                "type": "FeatureCollection",
                "features": items,
                "numberMatched": total,
                "numberReturned": returned,
                "links": links,
            }))).into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

/// GET /stac/collections/{id}/items/{item_id} — single item
async fn get_collection_item(
    State(state): State<AppState>,
    Path((collection_id, item_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let catalog = state.catalog.lock().await;
    match catalog.get_collection_item(&collection_id, &item_id) {
        Ok(Some(item)) => (StatusCode::OK, Json(serde_json::to_value(item).unwrap())).into_response(),
        Ok(None) => err(StatusCode::NOT_FOUND, "Item not found").into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

fn parse_bbox_str(s: &str) -> Option<[f64; 4]> {
    let parts: Vec<f64> = s.split(',').filter_map(|p| p.trim().parse().ok()).collect();
    if parts.len() == 4 { Some([parts[0], parts[1], parts[2], parts[3]]) } else { None }
}

fn build_search_response(
    items: Vec<StacItem>,
    total: usize,
    limit: usize,
    offset: usize,
    base_path: &str,
) -> serde_json::Value {
    let returned = items.len();
    let mut links = vec![
        serde_json::json!({"rel": "self", "href": base_path, "type": "application/geo+json"}),
        serde_json::json!({"rel": "root", "href": "/", "type": "application/json"}),
    ];
    if offset + limit < total {
        links.push(serde_json::json!({
            "rel": "next",
            "href": format!("{}?limit={}&offset={}", base_path, limit, offset + limit),
            "type": "application/geo+json"
        }));
    }
    if offset > 0 {
        let prev_offset = offset.saturating_sub(limit);
        links.push(serde_json::json!({
            "rel": "prev",
            "href": format!("{}?limit={}&offset={}", base_path, limit, prev_offset),
            "type": "application/geo+json"
        }));
    }
    serde_json::json!({
        "type": "FeatureCollection",
        "features": items,
        "numberMatched": total,
        "numberReturned": returned,
        "links": links,
    })
}

async fn stac_search(State(state): State<AppState>, Query(q): Query<SearchQuery>) -> impl IntoResponse {
    let catalog = state.catalog.lock().await;
    let limit = q.limit.unwrap_or(50).min(1000);
    let offset = q.offset.unwrap_or(0);

    // Support both `collection` and `collections` params
    let collection = q.collection.as_deref().or(
        q.collections.as_deref().and_then(|s| s.split(',').next())
    );

    let bbox = q.bbox.as_deref().and_then(parse_bbox_str);
    let datetime = q.datetime.as_deref().and_then(DatetimeFilter::parse);

    let total = match catalog.search_count(collection, bbox, datetime.as_ref()) {
        Ok(n) => n,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    };

    match catalog.search(collection, bbox, datetime.as_ref(), limit, offset) {
        Ok(items) => {
            let body = build_search_response(items, total, limit, offset, "/stac/search");
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

/// POST /stac/search — same as GET but accepts JSON body
async fn stac_search_post(
    State(state): State<AppState>,
    Json(body): Json<SearchBody>,
) -> impl IntoResponse {
    let catalog = state.catalog.lock().await;
    let limit = body.limit.unwrap_or(50).min(1000);
    let offset = body.offset.unwrap_or(0);

    // Resolve collection: body.collection or first of body.collections
    let collection_owned = body.collection.clone().or_else(|| {
        body.collections.as_ref().and_then(|v| v.first().cloned())
    });
    let collection = collection_owned.as_deref();

    let bbox = body.bbox.as_ref().and_then(|v| {
        if v.len() == 4 { Some([v[0], v[1], v[2], v[3]]) } else { None }
    });

    let datetime = body.datetime.as_deref().and_then(DatetimeFilter::parse);

    let total = match catalog.search_count(collection, bbox, datetime.as_ref()) {
        Ok(n) => n,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    };

    match catalog.search(collection, bbox, datetime.as_ref(), limit, offset) {
        Ok(items) => {
            let resp = build_search_response(items, total, limit, offset, "/stac/search");
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

async fn get_chunk(State(state): State<AppState>, Path(sha): Path<String>) -> impl IntoResponse {
    let mut store = state.store.lock().await;
    match store.get(&sha) {
        Ok(Some(data)) => (StatusCode::OK, data).into_response(),
        Ok(None) => err(StatusCode::NOT_FOUND, "Chunk not found").into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

async fn list_chunks(State(state): State<AppState>, Query(q): Query<LimitQuery>) -> impl IntoResponse {
    let store = state.store.lock().await;
    let limit = q.limit.unwrap_or(100).min(10000);
    let mut chunks = store.list_chunks();
    chunks.truncate(limit);
    let count = chunks.len();
    Json(serde_json::json!({"chunks": chunks, "count": count}))
}

async fn ingest(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    let key = api_key(&headers);
    if let Err(e) = state.auth.check_write(key) {
        state.audit.log("ingest", "auth_fail", "", false);
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }

    state.active_requests.fetch_add(1, Ordering::Relaxed);
    let _guard = ActiveRequestGuard(state.active_requests.clone());

    let item: StacItem = match serde_json::from_value(
        payload.get("item").cloned().unwrap_or_default()
    ) {
        Ok(i) => i,
        Err(e) => return err(StatusCode::BAD_REQUEST, &format!("Invalid item: {}", e)).into_response(),
    };

    // Ingest base64-encoded chunks if provided
    if let Some(chunks_val) = payload.get("chunks") {
        if let Some(chunks_map) = chunks_val.as_object() {
            let mut store = state.store.lock().await;
            for (_name, data_val) in chunks_map {
                if let Some(b64) = data_val.as_str() {
                    match base64_decode(b64) {
                        Ok(decoded) => {
                            if let Err(e) = store.put(&decoded) {
                                return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response();
                            }
                        }
                        Err(_) => return err(StatusCode::BAD_REQUEST, "Invalid base64 in chunks").into_response(),
                    }
                }
            }
        }
    }

    let item_id = item.id.clone();
    let catalog = state.catalog.lock().await;
    match catalog.add_item(&item) {
        Ok(()) => {
            state.audit.log("ingest", &item_id, "", true);
            (StatusCode::CREATED, Json(serde_json::json!({"status": "ok", "id": item_id}))).into_response()
        }
        Err(e) => {
            state.audit.log("ingest", &item_id, "", false);
            err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response()
        }
    }
}

async fn verify_item(State(state): State<AppState>, Path(item_id): Path<String>) -> impl IntoResponse {
    let catalog = state.catalog.lock().await;
    let item = match catalog.get_item(&item_id) {
        Ok(Some(i)) => i,
        Ok(None) => return err(StatusCode::NOT_FOUND, "Item not found").into_response(),
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    };
    drop(catalog);

    let store = state.store.lock().await;
    let total = item.chunk_hashes.len();
    let mut valid = 0usize;
    let mut missing = 0usize;
    let mut corrupted = 0usize;

    for hash in &item.chunk_hashes {
        if !store.has(hash) {
            missing += 1;
        } else {
            match store.verify(hash) {
                Ok(true) => valid += 1,
                _ => corrupted += 1,
            }
        }
    }

    let ok = corrupted == 0 && missing == 0;
    (
        if ok { StatusCode::OK } else { StatusCode::UNPROCESSABLE_ENTITY },
        Json(serde_json::json!({
            "item_id": item_id, "total": total,
            "valid": valid, "missing": missing, "corrupted": corrupted, "ok": ok,
        })),
    ).into_response()
}

async fn audit_log(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<LimitQuery>,
) -> impl IntoResponse {
    let key = api_key(&headers);
    if let Err(e) = state.auth.check_admin(key) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }
    let limit = q.limit.unwrap_or(50).min(500);
    let entries = state.audit.recent(limit);
    let count = entries.len();
    Json(serde_json::json!({"entries": entries, "count": count})).into_response()
}

// ---------------------------------------------------------------------------
// Phase 2: Peers + Federation handlers
// ---------------------------------------------------------------------------

/// GET /peers — list all known peers
async fn list_peers(State(state): State<AppState>) -> Json<serde_json::Value> {
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
async fn register_peer(
    State(state): State<AppState>,
    Query(q): Query<RegisterPeerQuery>,
) -> impl IntoResponse {
    if q.url.is_empty() {
        return err(StatusCode::BAD_REQUEST, "url is required").into_response();
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
async fn federation_sync(State(state): State<AppState>) -> impl IntoResponse {
    let peer_urls: Vec<String> = {
        let registry = state.peers.lock().await;
        registry.list().into_iter().map(|p| p.url.clone()).collect()
    };

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap_or_default();

    let mut synced = 0usize;
    let mut failed = 0usize;
    let mut results = vec![];

    for url in &peer_urls {
        // Try /node-info first, then fall back to /
        let info_url = format!("{}/node-info", url);
        match client.get(&info_url).send().await {
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
}

/// GET /federation/search — federated STAC search across all alive peers + local
async fn federation_search(
    State(state): State<AppState>,
    Query(q): Query<SearchQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(100).min(1000);

    // 1. Local search
    let local_items = {
        let catalog = state.catalog.lock().await;
        let collection = q.collection.as_deref().or(
            q.collections.as_deref().and_then(|s| s.split(',').next())
        );
        let bbox = q.bbox.as_deref().and_then(parse_bbox_str);
        let datetime = q.datetime.as_deref().and_then(DatetimeFilter::parse);
        catalog.search(collection, bbox, datetime.as_ref(), limit, 0).unwrap_or_default()
    };

    // 2. Build params for peer queries
    let peer_urls: Vec<String> = {
        let registry = state.peers.lock().await;
        registry.list().into_iter()
            .filter(|p| p.alive())
            .map(|p| p.url.clone())
            .collect()
    };

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .unwrap_or_default();

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
            let mut params = vec![("limit", limit.to_string())];
            if let Some(c) = &q_col { params.push(("collections", c.clone())); }
            if let Some(b) = &q_bbox { params.push(("bbox", b.clone())); }
            if let Some(d) = &q_dt { params.push(("datetime", d.clone())); }

            let resp = client
                .get(format!("{}/stac/search", url))
                .query(&params)
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

// ---------------------------------------------------------------------------
// Stats handlers
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct StatsPeriodQuery {
    pub period_hours: Option<u64>,
    pub period_days: Option<u64>,
    pub limit: Option<usize>,
}

async fn stats_downloads(
    State(state): State<AppState>,
    Query(q): Query<StatsPeriodQuery>,
) -> impl IntoResponse {
    let period_hours = q.period_hours.unwrap_or(168); // 7 days default
    match state.stats.download_stats(period_hours) {
        Ok(s) => (StatusCode::OK, Json(serde_json::to_value(s).unwrap_or_default())).into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

async fn stats_hot_chunks(
    State(state): State<AppState>,
    Query(q): Query<StatsPeriodQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(20);
    match state.stats.hot_chunks(limit) {
        Ok(chunks) => {
            let count = chunks.len();
            (StatusCode::OK, Json(serde_json::json!({"hot_chunks": chunks, "count": count}))).into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

async fn stats_replication_advice(State(state): State<AppState>) -> impl IntoResponse {
    match state.stats.replication_advice() {
        Ok(advice) => {
            let count = advice.len();
            (StatusCode::OK, Json(serde_json::json!({"advice": advice, "count": count}))).into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

async fn stats_ingest_history(
    State(state): State<AppState>,
    Query(q): Query<StatsPeriodQuery>,
) -> impl IntoResponse {
    let period_days = q.period_days.unwrap_or(365);
    match state.stats.ingest_history(period_days) {
        Ok(h) => (StatusCode::OK, Json(serde_json::to_value(h).unwrap_or_default())).into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Replication handler
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct ReplicateQuery {
    pub peer_url: String,
    pub collections: Option<String>, // comma-separated
    pub max_items: Option<usize>,
    pub dry_run: Option<bool>,
}

async fn replicate(
    State(state): State<AppState>,
    Query(q): Query<ReplicateQuery>,
) -> impl IntoResponse {
    if q.peer_url.is_empty() {
        return err(StatusCode::BAD_REQUEST, "peer_url is required").into_response();
    }

    let collections: Vec<String> = q
        .collections
        .as_deref()
        .unwrap_or("")
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let max_items = q.max_items.unwrap_or(0);
    let dry_run = q.dry_run.unwrap_or(false);

    let replicator = Replicator::new(state.store.clone(), state.catalog.clone());
    let result = replicator
        .sync_from_peer(&q.peer_url, &collections, max_items, dry_run)
        .await;

    let status = if result.errors.is_empty() {
        StatusCode::OK
    } else {
        StatusCode::MULTI_STATUS
    };

    (status, Json(serde_json::to_value(result).unwrap_or_default())).into_response()
}

// ---------------------------------------------------------------------------
// Base64 decode helper
// ---------------------------------------------------------------------------

fn base64_decode(s: &str) -> Result<Vec<u8>, ()> {
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut table = [255u8; 256];
    for (i, &c) in alphabet.iter().enumerate() {
        table[c as usize] = i as u8;
    }
    let clean: Vec<u8> = s.bytes()
        .filter(|&c| c != b'=' && c != b'\n' && c != b'\r' && c != b' ')
        .collect();
    let mapped: Vec<u8> = clean.iter()
        .map(|&c| table[c as usize])
        .collect();
    if mapped.iter().any(|&v| v == 255) { return Err(()); }

    let mut out = Vec::with_capacity((mapped.len() * 3) / 4);
    let mut i = 0;
    while i + 3 < mapped.len() {
        out.push((mapped[i] << 2) | (mapped[i+1] >> 4));
        out.push(((mapped[i+1] & 0x0f) << 4) | (mapped[i+2] >> 2));
        out.push(((mapped[i+2] & 0x03) << 6) | mapped[i+3]);
        i += 4;
    }
    Ok(out)
}


/// GET /peers.json — gossip-friendly peer list for discovery
async fn peers_json(State(state): State<AppState>) -> Json<serde_json::Value> {
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

/// POST /ingest/file — ingest a file from a local path on the server
async fn ingest_file_endpoint(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    let key = api_key(&headers);
    if let Err(e) = state.auth.check_write(key) {
        state.audit.log("ingest_file", "auth_fail", "", false);
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }

    state.active_requests.fetch_add(1, Ordering::Relaxed);
    let _guard = ActiveRequestGuard(state.active_requests.clone());

    let file_path = match payload.get("path").and_then(|v| v.as_str()) {
        Some(p) => std::path::PathBuf::from(p),
        None => return err(StatusCode::BAD_REQUEST, "Missing 'path' field").into_response(),
    };
    let collection = payload.get("collection")
        .and_then(|v| v.as_str())
        .unwrap_or("default");
    let chunk_size = payload.get("chunk_size")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
        .unwrap_or(ingest::DEFAULT_CHUNK_SIZE);

    if !file_path.exists() {
        return err(StatusCode::BAD_REQUEST, &format!("File not found: {}", file_path.display())).into_response();
    }

    let mut store = state.store.lock().await;
    let item = match ingest::ingest_file(&file_path, collection, chunk_size, &mut store) {
        Ok(item) => item,
        Err(e) => {
            state.audit.log("ingest_file", &file_path.display().to_string(), "", false);
            return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response();
        }
    };
    drop(store);

    let item_id = item.id.clone();
    let catalog = state.catalog.lock().await;
    match catalog.add_item(&item) {
        Ok(()) => {
            state.audit.log("ingest_file", &item_id, "", true);
            (StatusCode::CREATED, Json(serde_json::json!({
                "status": "ok",
                "id": item_id,
                "chunks": item.chunk_hashes.len(),
                "collection": collection,
            }))).into_response()
        }
        Err(e) => {
            state.audit.log("ingest_file", &item_id, "", false);
            err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Fetch endpoint — POST /fetch
// ---------------------------------------------------------------------------

/// Query parameters for POST /fetch
#[derive(Deserialize)]
pub struct FetchQuery {
    /// Bounding box as "west,south,east,north"
    pub bbox: Option<String>,
    /// Sentinel-2 MGRS tile name (e.g. "32TPS"). Alternative to bbox.
    pub tile: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub cloud_cover: Option<f64>,
    /// Comma-separated band names, e.g. "blue,green,red,nir"
    pub bands: Option<String>,
    pub limit: Option<usize>,
    pub collection: Option<String>,
}

/// GET /fetch/preview — search Element84 STAC without downloading (dry run).
async fn fetch_preview(
    State(state): State<AppState>,
    Query(q): Query<FetchQuery>,
) -> impl IntoResponse {
    // Resolve bbox
    let tile_filter = q.tile.as_ref().map(|t| t.trim().to_uppercase());
    let bbox = if let Some(s) = q.bbox.as_deref() {
        let parts: Vec<f64> = s.split(',')
            .filter_map(|v| v.trim().parse().ok())
            .collect();
        if parts.len() < 4 {
            return err(StatusCode::BAD_REQUEST, "bbox must be 'west,south,east,north'").into_response();
        }
        [parts[0], parts[1], parts[2], parts[3]]
    } else if let Some(ref tile) = tile_filter {
        match crate::mgrs::tile_to_bbox(tile) {
            Ok(b) => b,
            Err(e) => return err(StatusCode::BAD_REQUEST, &format!("Invalid tile '{}': {}", tile, e)).into_response(),
        }
    } else {
        return err(StatusCode::BAD_REQUEST, "Missing bbox or tile parameter").into_response();
    };

    let start_date = q.start_date.as_deref().unwrap_or("2020-01-01");
    let end_date = q.end_date.as_deref().unwrap_or("2020-12-31");
    let cloud_cover = q.cloud_cover.unwrap_or(30.0);
    let limit = q.limit.unwrap_or(100);
    let collection = q.collection.as_deref().unwrap_or("sentinel-2-l2a");

    let (results, errors) = fetcher::search_element84(bbox, start_date, end_date, cloud_cover, limit, collection).await;

    // Check which items we already have + build detail list in one pass
    let catalog = state.catalog.lock().await;
    let mut already_have = 0usize;
    let mut new_items = 0usize;
    let mut tiles: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut total_bands = 0usize;
    let mut dates: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut items_detail: Vec<serde_json::Value> = Vec::new();

    for r in results.iter().take(500) {
        let date_part = r.datetime.split('T').next().unwrap_or(&r.datetime).replace('-', "");
        dates.insert(date_part.clone());
        let parts: Vec<&str> = r.id.split('_').collect();
        let tile = if parts.len() > 1 { parts[1].to_string() } else { "?".to_string() };
        tiles.insert(tile.clone());
        total_bands += r.assets.len();

        let stored = catalog.get_collection_item(collection, &r.id).ok().map_or(false, |v| v.is_some());
        if stored { already_have += 1; } else { new_items += 1; }

        if items_detail.len() < 50 {
            items_detail.push(serde_json::json!({
                "id": r.id,
                "tile": tile,
                "datetime": r.datetime,
                "cloud_cover": (r.cloud_cover * 10.0).round() / 10.0,
                "bands": r.assets.len(),
                "stored": stored,
            }));
        }
    }
    drop(catalog);

    // Estimate download size (rough: ~20MB per band for S2 10m)
    let est_mb = (new_items * 5 * 20) as u64;

    (StatusCode::OK, Json(serde_json::json!({
        "total_found": results.len(),
        "already_stored": already_have,
        "new_items": new_items,
        "tiles": tiles.into_iter().collect::<Vec<_>>(),
        "unique_dates": dates.len(),
        "date_range": [dates.iter().next(), dates.iter().last()],
        "total_bands": total_bands,
        "est_download_mb": est_mb as u64,
        "errors": errors,
        "items": items_detail,
    }))).into_response()
}

/// POST /fetch — search Element84 STAC and ingest matching items.
async fn fetch_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<FetchQuery>,
) -> impl IntoResponse {
    let key = api_key(&headers);
    if let Err(e) = state.auth.check_write(key) {
        state.audit.log("fetch", "auth_fail", "", false);
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }

    state.active_requests.fetch_add(1, Ordering::Relaxed);
    let _guard = ActiveRequestGuard(state.active_requests.clone());

    // Resolve bbox: explicit bbox, or from tile name, or error
    let tile_filter = q.tile.as_ref().map(|t| t.trim().to_uppercase());
    let bbox = if let Some(s) = q.bbox.as_deref() {
        let parts: Vec<f64> = s.split(',')
            .filter_map(|v| v.trim().parse().ok())
            .collect();
        if parts.len() < 4 {
            return err(StatusCode::BAD_REQUEST, "bbox must be 'west,south,east,north'").into_response();
        }
        [parts[0], parts[1], parts[2], parts[3]]
    } else if let Some(ref tile) = tile_filter {
        match crate::mgrs::tile_to_bbox(tile) {
            Ok(b) => b,
            Err(e) => return err(StatusCode::BAD_REQUEST, &format!("Invalid tile '{}': {}", tile, e)).into_response(),
        }
    } else {
        return err(StatusCode::BAD_REQUEST, "Missing bbox or tile parameter").into_response();
    };

    let start_date = q.start_date.as_deref().unwrap_or("2020-01-01");
    let end_date = q.end_date.as_deref().unwrap_or("2020-12-31");
    let cloud_cover = q.cloud_cover.unwrap_or(30.0);
    let limit = q.limit.unwrap_or(100);
    let collection = q.collection.as_deref().unwrap_or("sentinel-2-l2a");

    let bands: Vec<String> = q.bands
        .as_deref()
        .map(|s| s.split(',').map(|b| b.trim().to_string()).filter(|b| !b.is_empty()).collect())
        .unwrap_or_default();

    let result = fetcher::fetch_and_ingest(
        state.store.clone(),
        state.catalog.clone(),
        bbox,
        start_date,
        end_date,
        cloud_cover,
        &bands,
        limit,
        collection,
        tile_filter.as_deref(),
    )
    .await;

    state.audit.log("fetch", collection, "", result.errors.is_empty());

    // Record ingest stats
    if result.items_downloaded > 0 {
        let bbox_str = format!("{},{},{},{}", bbox[0], bbox[1], bbox[2], bbox[3]);
        let temporal = format!("{}/{}", start_date, end_date);
        let _ = state.stats.record_download(
            Some("element84"),
            Some(collection),
            None,
            result.bytes_downloaded as i64,
            Some("element84"),
            Some(&bbox_str),
            None,
        );
        let _ = state.stats.record_uptake(
            collection,
            Some("fetch"),
            Some(&bbox_str),
            Some(&temporal),
            result.bytes_downloaded as i64,
            None,
            None,
        );
    }

    let status = if result.errors.is_empty() {
        StatusCode::OK
    } else {
        StatusCode::MULTI_STATUS
    };

    (status, Json(serde_json::to_value(result).unwrap_or_default())).into_response()
}

// ---------------------------------------------------------------------------
// Gamification handlers
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct LeaderboardQuery {
    #[serde(rename = "type")]
    pub board_type: Option<String>,
    pub limit: Option<usize>,
    pub group: Option<String>,
}

#[derive(Deserialize)]
pub struct FeedQuery {
    pub limit: Option<usize>,
}

async fn gamification_leaderboard(
    State(state): State<AppState>,
    Query(q): Query<LeaderboardQuery>,
) -> impl IntoResponse {
    let board_type = q.board_type.as_deref().unwrap_or("nodes");
    let limit = q.limit.unwrap_or(20).min(100);
    let group_filter = q.group.as_deref();
    match state.gamification.get_leaderboard(board_type, limit, group_filter) {
        Ok(entries) => {
            let count = entries.len();
            (StatusCode::OK, Json(serde_json::json!({"leaderboard": entries, "count": count, "type": board_type}))).into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

async fn gamification_node_profile(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> impl IntoResponse {
    match state.gamification.get_node_profile(&node_id) {
        Ok(Some(profile)) => (StatusCode::OK, Json(serde_json::to_value(profile).unwrap_or_default())).into_response(),
        Ok(None) => err(StatusCode::NOT_FOUND, "Node not found in gamification DB").into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

async fn gamification_feed(
    State(state): State<AppState>,
    Query(q): Query<FeedQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(50).min(200);
    match state.gamification.get_feed(limit) {
        Ok(feed) => {
            let count = feed.len();
            (StatusCode::OK, Json(serde_json::json!({"feed": feed, "count": count}))).into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

async fn gamification_stats(State(state): State<AppState>) -> impl IntoResponse {
    match state.gamification.network_stats() {
        Ok(stats) => (StatusCode::OK, Json(serde_json::to_value(stats).unwrap_or_default())).into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

async fn gamification_economy(State(state): State<AppState>) -> impl IntoResponse {
    match state.gamification.economy_health() {
        Ok(health) => (StatusCode::OK, Json(serde_json::to_value(health).unwrap_or_default())).into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

async fn gamification_challenges(State(state): State<AppState>) -> impl IntoResponse {
    match state.gamification.get_active_challenges() {
        Ok(challenges) => {
            let count = challenges.len();
            (StatusCode::OK, Json(serde_json::json!({"challenges": challenges, "count": count}))).into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

async fn gamification_challenge_results(
    State(state): State<AppState>,
    Path(challenge_id): Path<i64>,
) -> impl IntoResponse {
    match state.gamification.get_challenge_results(challenge_id) {
        Ok(Some(results)) => (StatusCode::OK, Json(serde_json::to_value(results).unwrap_or_default())).into_response(),
        Ok(None) => err(StatusCode::NOT_FOUND, "Challenge not found").into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Download handler — GET /download/{collection}/{item_id}
// Reconstruct COG from chunks and stream to client.
// ---------------------------------------------------------------------------

async fn download_item(
    State(state): State<AppState>,
    Path((collection_id, item_id)): Path<(String, String)>,
) -> impl IntoResponse {
    use crate::reconstruct;
    use axum::body::Body;

    let item = {
        let catalog = state.catalog.lock().await;
        match catalog.get_collection_item(&collection_id, &item_id) {
            Ok(Some(i)) => i,
            Ok(None) => return err(StatusCode::NOT_FOUND, "Item not found").into_response(),
            Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
        }
    };

    let tiff_bytes = {
        let mut store = state.store.lock().await;
        match reconstruct::reconstruct_cog(&item, &mut store, None) {
            Ok(b) => b,
            Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
        }
    };

    // Record download stats
    let _ = state.stats.record_download(
        Some("api"),
        Some(&collection_id),
        Some(&item_id),
        tiff_bytes.len() as i64,
        None,
        None,
        None,
    );
    let _ = state.stats.record_uptake(
        &collection_id,
        Some("download"),
        None,
        None,
        tiff_bytes.len() as i64,
        None,
        None,
    );

    let filename = format!("{}_{}.tif", collection_id, item_id);
    use axum::http::header::{CONTENT_DISPOSITION, CONTENT_TYPE};
    let mut resp = axum::response::Response::new(Body::from(tiff_bytes));
    resp.headers_mut().insert(
        CONTENT_TYPE,
        axum::http::HeaderValue::from_static("image/tiff; application=geotiff"),
    );
    if let Ok(disp) = axum::http::HeaderValue::from_str(
        &format!("attachment; filename=\"{}\"", filename)
    ) {
        resp.headers_mut().insert(CONTENT_DISPOSITION, disp);
    }
    resp.into_response()
}

// ---------------------------------------------------------------------------
// Process handlers — POST /process, GET /process/operations
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
pub struct ProcessRequest {
    pub operation: String,
    pub collection: Option<String>,
    pub item_id: Option<String>,
    pub params: Option<serde_json::Value>,
}

async fn process_job(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ProcessRequest>,
) -> impl IntoResponse {
    let key = api_key(&headers);
    if let Err(e) = state.auth.check_write(key) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }

    let job_id = uuid::Uuid::new_v4().to_string();
    state.audit.log("process", &req.operation, req.item_id.as_deref().unwrap_or(""), true);

    (StatusCode::ACCEPTED, Json(serde_json::json!({
        "job_id": job_id,
        "status": "queued",
        "operation": req.operation,
        "collection": req.collection,
        "item_id": req.item_id,
    }))).into_response()
}

async fn process_operations() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "operations": [
            {"name": "ndvi", "description": "Compute NDVI from red/NIR bands"},
            {"name": "ndwi", "description": "Compute NDWI from green/NIR bands"},
            {"name": "ndsi", "description": "Compute NDSI from green/SWIR bands"},
            {"name": "evi", "description": "Compute EVI from blue/red/NIR bands"},
            {"name": "true_color", "description": "Render true-color RGB composite"},
            {"name": "cloud_mask", "description": "Apply cloud masking"},
            {"name": "rechunk", "description": "Re-tile and rechunk item"},
            {"name": "band_math", "description": "Custom band math expression"},
        ]
    }))
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

async fn sync_from_peer(
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

async fn sync_item(
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
// Admin handlers — GET /admin/stats, GET /admin/activity, PATCH /admin/node/name
// ---------------------------------------------------------------------------

async fn admin_stats(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let key = api_key(&headers);
    if let Err(e) = state.auth.check_admin(key) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }

    let store = state.store.lock().await;
    let catalog = state.catalog.lock().await;
    let s = store.stats().clone();

    (StatusCode::OK, Json(serde_json::json!({
        "node_id": state.node_id,
        "node_name": state.node_name,
        "version": state.version,
        "storage": {
            "total_chunks": store.chunk_count(),
            "total_bytes": store.total_bytes(),
            "total_gb": store.total_bytes() as f64 / 1_073_741_824.0,
        },
        "catalog": {
            "total_items": catalog.item_count(None).unwrap_or(0),
            "collections": catalog.list_collections().unwrap_or_default().len(),
        },
        "traffic": {
            "chunks_served": s.chunks_served,
            "bytes_served": s.bytes_served,
            "requests_total": s.requests_total,
            "chunks_stored": s.chunks_stored,
            "bytes_ingested": s.bytes_ingested,
        },
        "auth_enabled": state.auth.is_enabled(),
    }))).into_response()
}

async fn admin_activity(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<LimitQuery>,
) -> impl IntoResponse {
    let key = api_key(&headers);
    if let Err(e) = state.auth.check_write(key) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }

    let limit = q.limit.unwrap_or(50);
    let entries = state.audit.recent(limit);
    let count = entries.len();
    (StatusCode::OK, Json(serde_json::json!({"activity": entries, "count": count}))).into_response()
}

#[derive(serde::Deserialize)]
pub struct NodeNamePatch {
    pub name: String,
}

async fn patch_node_name(
    State(_state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<NodeNamePatch>,
) -> impl IntoResponse {
    let key = api_key(&headers);
    if let Err(e) = _state.auth.check_admin(key) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }

    // Persist via env-var override is not possible at runtime; return accepted
    (StatusCode::OK, Json(serde_json::json!({
        "status": "ok",
        "node_name": body.name,
        "note": "Restart with EARTHGRID_NODE_NAME env var to persist"
    }))).into_response()
}

// ---------------------------------------------------------------------------
// Coverage + extra stats handlers
// ---------------------------------------------------------------------------

async fn coverage_spatial(State(state): State<AppState>) -> impl IntoResponse {
    // Serve pre-computed MGRS tile polygons (generated by Python with pyproj)
    let poly_path = state.data_dir.join("coverage_spatial.json");
    if poly_path.exists() {
        match std::fs::read_to_string(&poly_path) {
            Ok(json_str) => {
                match serde_json::from_str::<serde_json::Value>(&json_str) {
                    Ok(val) => return (StatusCode::OK, Json(val)).into_response(),
                    Err(_) => {}
                }
            }
            Err(_) => {}
        }
    }

    // Fallback: bbox-based coverage from DB
    let catalog = state.catalog.lock().await;
    let tiles = catalog.mgrs_coverage().unwrap_or_default();
    let mut collections: std::collections::HashMap<String, Vec<serde_json::Value>> =
        std::collections::HashMap::new();
    for t in &tiles {
        collections.entry(t.collection.clone()).or_default().push(
            serde_json::json!({
                "bbox": [t.west, t.south, t.east, t.north],
                "tile_id": t.tile_id,
                "date_count": t.date_count,
            })
        );
    }
    let col_map: serde_json::Value = collections
        .into_iter()
        .map(|(k, v)| (k, serde_json::json!({ "cells": v })))
        .collect();

    (StatusCode::OK, Json(serde_json::json!({
        "collections": col_map,
    }))).into_response()
}

async fn stats_coverage(State(state): State<AppState>) -> impl IntoResponse {
    let catalog = state.catalog.lock().await;
    let total = catalog.item_count(None).unwrap_or(0);
    let collections = catalog.list_collections().unwrap_or_default();
    let per_col: Vec<serde_json::Value> = collections
        .iter()
        .map(|c| {
            let count = catalog.item_count(Some(&c.id)).unwrap_or(0);
            serde_json::json!({ "collection": c.id, "item_count": count })
        })
        .collect();
    (StatusCode::OK, Json(serde_json::json!({
        "total_items": total,
        "collections": per_col,
    }))).into_response()
}

async fn stats_requests(State(state): State<AppState>) -> impl IntoResponse {
    let store = state.store.lock().await;
    let s = store.stats().clone();
    (StatusCode::OK, Json(serde_json::json!({
        "requests_total": s.requests_total,
        "chunks_served": s.chunks_served,
        "bytes_served": s.bytes_served,
    }))).into_response()
}

async fn stats_bandwidth(State(state): State<AppState>) -> impl IntoResponse {
    let store = state.store.lock().await;
    let s = store.stats().clone();
    (StatusCode::OK, Json(serde_json::json!({
        "bytes_served": s.bytes_served,
        "bytes_ingested": s.bytes_ingested,
        "gb_served": s.bytes_served as f64 / 1_073_741_824.0,
        "gb_ingested": s.bytes_ingested as f64 / 1_073_741_824.0,
    }))).into_response()
}

async fn bandwidth_handler(State(state): State<AppState>) -> impl IntoResponse {
    stats_bandwidth(State(state)).await
}

async fn stats_replication_status(State(state): State<AppState>) -> impl IntoResponse {
    match state.stats.replication_advice() {
        Ok(advice) => {
            let total = advice.len();
            (StatusCode::OK, Json(serde_json::json!({
                "total_items_checked": total,
                "advice": advice,
            }))).into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Admin: Delete collection
// ---------------------------------------------------------------------------

async fn admin_delete_collection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(collection_id): Path<String>,
) -> impl IntoResponse {
    let key = api_key(&headers);
    if let Err(e) = state.auth.check_admin(key) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }
    let catalog = state.catalog.lock().await;
    // Check collection exists
    match catalog.get_collection(&collection_id) {
        Ok(None) => return err(StatusCode::NOT_FOUND, "Collection not found").into_response(),
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
        Ok(Some(_)) => {}
    }
    // Get chunk hashes for all items in this collection before deleting
    let items = catalog.search(Some(&collection_id), None, None, 1_000_000, 0).unwrap_or_default();
    let item_count = items.len();
    let chunk_hashes: Vec<String> = items.iter()
        .flat_map(|i| i.chunk_hashes.iter().cloned())
        .collect();
    // Delete all items + collection in one call
    if let Err(e) = catalog.delete_collection(&collection_id) {
        return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response();
    }
    drop(catalog);

    // Attempt to remove chunks from store (best-effort)
    let removed_chunks = {
        let mut store = state.store.lock().await;
        let mut count = 0usize;
        for hash in &chunk_hashes {
            if store.delete(hash).unwrap_or(false) {
                count += 1;
            }
        }
        count
    };

    state.audit.log("collection_delete", &collection_id, "", true);
    tracing::info!("Deleted collection {} ({} items, {} chunks removed)", collection_id, item_count, removed_chunks);
    (StatusCode::OK, Json(serde_json::json!({
        "status": "deleted",
        "collection": collection_id,
        "items_removed": item_count,
        "chunks_removed": removed_chunks,
    }))).into_response()
}

// ---------------------------------------------------------------------------
// Admin: User management
// ---------------------------------------------------------------------------

async fn admin_list_users(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let key = api_key(&headers);
    if let Err(e) = state.auth.check_admin(key) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }
    match &state.user_auth {
        Some(ua) => {
            match ua.list_users() {
                Ok(users) => {
                    let count = users.len();
                    (StatusCode::OK, Json(serde_json::json!({"users": users, "count": count}))).into_response()
                }
                Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
            }
        }
        None => err(StatusCode::SERVICE_UNAVAILABLE, "User auth not initialized").into_response(),
    }
}

#[derive(serde::Deserialize)]
pub struct CreateUserBody {
    pub username: String,
    pub role: Option<String>,
}

async fn admin_create_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateUserBody>,
) -> impl IntoResponse {
    let key = api_key(&headers);
    if let Err(e) = state.auth.check_admin(key) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }
    if body.username.is_empty() {
        return err(StatusCode::BAD_REQUEST, "Missing username").into_response();
    }
    match &state.user_auth {
        Some(ua) => {
            let role = body.role.as_deref().unwrap_or("member");
            match ua.add_user(&body.username, role) {
                Ok(api_key_val) => {
                    state.audit.log("user_create", &body.username, "", true);
                    (StatusCode::CREATED, Json(serde_json::json!({
                        "status": "created",
                        "username": body.username,
                        "role": role,
                        "api_key": api_key_val,
                    }))).into_response()
                }
                Err(e) => err(StatusCode::CONFLICT, &e.to_string()).into_response(),
            }
        }
        None => err(StatusCode::SERVICE_UNAVAILABLE, "User auth not initialized").into_response(),
    }
}

async fn admin_delete_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(user_id): Path<String>,
) -> impl IntoResponse {
    let key = api_key(&headers);
    if let Err(e) = state.auth.check_admin(key) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }
    match &state.user_auth {
        Some(ua) => {
            match ua.revoke_user(&user_id) {
                Ok(true) => {
                    state.audit.log("user_delete", &user_id, "", true);
                    (StatusCode::OK, Json(serde_json::json!({
                        "status": "deactivated",
                        "user_id": user_id,
                    }))).into_response()
                }
                Ok(false) => err(StatusCode::NOT_FOUND, "User not found").into_response(),
                Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
            }
        }
        None => err(StatusCode::SERVICE_UNAVAILABLE, "User auth not initialized").into_response(),
    }
}

// ---------------------------------------------------------------------------
// PATCH /node-name — alias for admin/node/name
// ---------------------------------------------------------------------------

async fn patch_node_name_alias(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<NodeNamePatch>,
) -> impl IntoResponse {
    patch_node_name(State(state), headers, Json(body)).await
}

// ---------------------------------------------------------------------------
// DELETE /nodes/{node_id} — remove from beacon
// ---------------------------------------------------------------------------

async fn delete_node(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
) -> impl IntoResponse {
    let key = api_key(&headers);
    if let Err(e) = state.auth.check_admin(key) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }
    // Remove from peer registry
    {
        let mut peers = state.peers.lock().await;
        peers.remove_by_id(&node_id);
    }
    state.audit.log("node_delete", &node_id, "", true);
    (StatusCode::OK, Json(serde_json::json!({
        "status": "removed",
        "node_id": node_id,
    }))).into_response()
}

// ---------------------------------------------------------------------------
// Stats: access statistics
// ---------------------------------------------------------------------------

async fn stats_access(State(state): State<AppState>) -> impl IntoResponse {
    match state.stats.replication_advice() {
        Ok(advice) => {
            (StatusCode::OK, Json(serde_json::json!({
                "top_collections": advice,
                "note": "Access-based replication advice",
            }))).into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Stats: stats.json (alias for /stats)
// ---------------------------------------------------------------------------

async fn stats_json_alias(State(state): State<AppState>) -> impl IntoResponse {
    // Serve dashboard_stats.json if it exists (generated by update_stats.sh)
    let dashboard_path = state.data_dir.join("dashboard_stats.json");
    if dashboard_path.exists() {
        match std::fs::read_to_string(&dashboard_path) {
            Ok(content) => {
                match serde_json::from_str::<serde_json::Value>(&content) {
                    Ok(val) => return (StatusCode::OK, Json(val)).into_response(),
                    Err(_) => {}
                }
            }
            Err(_) => {}
        }
    }
    // Fallback to internal stats
    stats(State(state)).await.into_response()
}

// ---------------------------------------------------------------------------
// Stats: uptake (anonymous aggregate)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct UptakePeriodQuery {
    pub period_days: Option<u64>,
}

async fn stats_uptake(
    State(state): State<AppState>,
    Query(q): Query<UptakePeriodQuery>,
) -> impl IntoResponse {
    let period_days = q.period_days.unwrap_or(30);
    // Use ingest_history as a proxy for uptake data
    match state.stats.ingest_history(period_days) {
        Ok(history) => {
            let total_gb = history.total_bytes as f64 / 1_073_741_824.0;
            (StatusCode::OK, Json(serde_json::json!({
                "report_type": "EarthGrid Uptake Statistics",
                "period_days": period_days,
                "privacy": "anonymous — no user identification stored",
                "summary": {
                    "total_requests": history.total_items,
                    "total_gb": (total_gb * 1000.0).round() / 1000.0,
                    "total_bytes": history.total_bytes,
                },
                "daily_trend": history.daily.iter().map(|d| serde_json::json!({
                    "date": d.date,
                    "items": d.items,
                    "gb": d.gb,
                })).collect::<Vec<_>>(),
                "hourly_trend": history.hourly.iter().map(|h| serde_json::json!({
                    "hour": h.hour,
                    "items": h.items,
                    "bytes": h.bytes,
                })).collect::<Vec<_>>(),
            }))).into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

async fn stats_uptake_csv(
    State(state): State<AppState>,
    Query(q): Query<UptakePeriodQuery>,
) -> impl IntoResponse {
    let period_days = q.period_days.unwrap_or(30);
    match state.stats.ingest_history(period_days) {
        Ok(history) => {
            let mut csv = String::from("date,items,bytes_ingested,gb_ingested\n");
            for d in &history.daily {
                csv.push_str(&format!("{},{},{},{:.3}\n", d.date, d.items, d.bytes, d.gb));
            }
            let disp = format!("attachment; filename=\"earthgrid_uptake_{}d.csv\"", period_days);
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("text/csv"),
            );
            if let Ok(val) = axum::http::HeaderValue::from_str(&disp) {
                headers.insert(axum::http::header::CONTENT_DISPOSITION, val);
            }
            (StatusCode::OK, headers, csv).into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Replicate/items — list items available for replication
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct ReplicateItemsQuery {
    pub collection: Option<String>,
    pub limit: Option<usize>,
}

async fn replicate_items(
    State(state): State<AppState>,
    Query(q): Query<ReplicateItemsQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(10000).min(100_000);
    let catalog = state.catalog.lock().await;
    let collection = q.collection.as_deref();
    match catalog.search(collection, None, None, limit, 0) {
        Ok(items) => {
            let count = items.len();
            (StatusCode::OK, Json(serde_json::json!({
                "node_id": state.node_id,
                "node_name": state.node_name,
                "count": count,
                "items": items,
            }))).into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// POST /resize — update storage allocation
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct ResizeQuery {
    pub size_gb: Option<f64>,
}

async fn resize_storage(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<ResizeQuery>,
) -> impl IntoResponse {
    let key = api_key(&headers);
    if let Err(e) = state.auth.check_admin(key) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }
    let size_gb = match q.size_gb {
        Some(v) if v > 0.0 => v,
        _ => return err(StatusCode::BAD_REQUEST, "size_gb required and must be > 0").into_response(),
    };
    // Update config.json if it exists
    let config_path = state.data_dir.join("config.json");
    if config_path.exists() {
        if let Ok(content) = std::fs::read_to_string(&config_path) {
            if let Ok(mut cfg) = serde_json::from_str::<serde_json::Value>(&content) {
                cfg["storage_limit_gb"] = serde_json::json!(size_gb);
                let _ = std::fs::write(&config_path, serde_json::to_string_pretty(&cfg).unwrap_or_default());
            }
        }
    }
    state.audit.log("resize", &format!("size_gb={}", size_gb), "", true);
    (StatusCode::OK, Json(serde_json::json!({
        "status": "resized",
        "new_gb": size_gb,
        "note": "Restart node for new limit to take effect on ChunkStore",
    }))).into_response()
}

// ---------------------------------------------------------------------------
// GET /chunk-map/{collection_id}/{item_id}
// ---------------------------------------------------------------------------

async fn chunk_map(
    State(state): State<AppState>,
    Path((collection_id, item_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let catalog = state.catalog.lock().await;
    match catalog.get_collection_item(&collection_id, &item_id) {
        Ok(Some(item)) => {
            let props = &item.properties;
            let total_chunks = item.chunk_hashes.len();
            (StatusCode::OK, Json(serde_json::json!({
                "item_id": item_id,
                "collection": collection_id,
                "total_chunks": total_chunks,
                "chunks": item.chunk_hashes,
                "tile_size": props.get("earthgrid:tile_size").unwrap_or(&serde_json::json!(512)),
                "tile_cols": props.get("earthgrid:tile_cols").unwrap_or(&serde_json::json!(1)),
                "tile_rows": props.get("earthgrid:tile_rows").unwrap_or(&serde_json::json!(1)),
                "width": props.get("earthgrid:width"),
                "height": props.get("earthgrid:height"),
                "dtype": props.get("earthgrid:dtype"),
                "crs": props.get("earthgrid:crs"),
                "node_url": "",
            }))).into_response()
        }
        Ok(None) => err(StatusCode::NOT_FOUND, "Item not found").into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// GET /point/{collection_id}/{item_id}
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct PointQuery {
    pub lon: f64,
    pub lat: f64,
}

async fn point_extract(
    State(state): State<AppState>,
    Path((collection_id, item_id)): Path<(String, String)>,
    Query(q): Query<PointQuery>,
) -> impl IntoResponse {
    let catalog = state.catalog.lock().await;
    let item = match catalog.get_collection_item(&collection_id, &item_id) {
        Ok(Some(i)) => i,
        Ok(None) => return err(StatusCode::NOT_FOUND, "Item not found").into_response(),
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    };
    drop(catalog);

    let props = &item.properties;
    let width = props.get("earthgrid:width").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
    let height = props.get("earthgrid:height").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
    let tile_size = props.get("earthgrid:tile_size").and_then(|v| v.as_u64()).unwrap_or(512) as usize;
    let tile_cols = props.get("earthgrid:tile_cols").and_then(|v| v.as_u64()).unwrap_or(1) as usize;
    let dtype = props.get("earthgrid:dtype").and_then(|v| v.as_str()).unwrap_or("uint16");

    if width == 0 || height == 0 {
        return err(StatusCode::BAD_REQUEST, "Item has no spatial dimensions").into_response();
    }

    let bbox = item.bbox; // [west, south, east, north]
    let lon = q.lon;
    let lat = q.lat;

    // Quick bounds check
    if lon < bbox[0] || lon > bbox[2] || lat < bbox[1] || lat > bbox[3] {
        return err(StatusCode::BAD_REQUEST, "Point outside item extent").into_response();
    }

    let res_x = (bbox[2] - bbox[0]) / width as f64;
    let res_y = (bbox[3] - bbox[1]) / height as f64;
    let col = ((lon - bbox[0]) / res_x) as usize;
    let row = ((bbox[3] - lat) / res_y) as usize; // y-axis inverted

    let col = col.min(width - 1);
    let row = row.min(height - 1);

    let tile_col = col / tile_size;
    let tile_row = row / tile_size;
    let tile_idx = tile_row * tile_cols + tile_col;

    if tile_idx >= item.chunk_hashes.len() {
        return err(StatusCode::INTERNAL_SERVER_ERROR, "Tile index out of range").into_response();
    }
    let sha = &item.chunk_hashes[tile_idx];
    let mut store = state.store.lock().await;
    let chunk_data = match store.get(sha) {
        Ok(Some(d)) => d,
        Ok(None) => return err(StatusCode::NOT_FOUND, "Chunk not found").into_response(),
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    };
    drop(store);

    let tile_w = tile_size.min(width - tile_col * tile_size);
    let local_col = col % tile_size;
    let local_row = row % tile_size;

    let (bpp, is_float) = match dtype {
        "uint8" | "int8" => (1usize, false),
        "uint16" | "int16" => (2, false),
        "uint32" | "int32" => (4, false),
        "float32" => (4, true),
        "float64" => (8, true),
        _ => (2, false),
    };

    let pixel_offset = (local_row * tile_w + local_col) * bpp;
    if pixel_offset + bpp > chunk_data.len() {
        return err(StatusCode::INTERNAL_SERVER_ERROR, "Pixel offset exceeds chunk size").into_response();
    }

    let bytes = &chunk_data[pixel_offset..pixel_offset + bpp];
    let value: serde_json::Value = if is_float && bpp == 4 {
        let f = f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        serde_json::json!(f)
    } else if is_float && bpp == 8 {
        let f = f64::from_le_bytes(bytes.try_into().unwrap_or([0u8; 8]));
        serde_json::json!(f)
    } else if bpp == 1 {
        serde_json::json!(bytes[0])
    } else if bpp == 2 {
        let v = u16::from_le_bytes([bytes[0], bytes[1]]);
        serde_json::json!(v)
    } else {
        let v = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        serde_json::json!(v)
    };

    (StatusCode::OK, Json(serde_json::json!({
        "value": value,
        "lon": lon,
        "lat": lat,
        "pixel": [col, row],
        "tile": [tile_col, tile_row],
        "item_id": item_id,
        "collection": collection_id,
        "dtype": dtype,
    }))).into_response()
}

// ---------------------------------------------------------------------------
// Federation: exchange key
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize, serde::Serialize)]
pub struct ExchangeKeyBody {
    pub node_name: Option<String>,
    pub node_id: Option<String>,
    pub api_key: Option<String>,
    pub public_key: Option<String>,
    pub timestamp: Option<u64>,
    pub signature: Option<String>,
}

async fn federation_exchange_key(
    State(state): State<AppState>,
    Json(body): Json<ExchangeKeyBody>,
) -> impl IntoResponse {
    if body.signature.is_none() {
        return err(StatusCode::BAD_REQUEST, "Missing signature in payload").into_response();
    }
    // Verify signature using NodeIdentity if available
    let peer_name = body.node_name.clone().unwrap_or_default();
    let peer_pubkey = body.public_key.clone().unwrap_or_default();

    if let Some(ni) = &state.node_identity {
        // Verify the peer signature
        let msg = format!(
            "{}|{}|{}|{}",
            peer_name,
            body.node_id.as_deref().unwrap_or(""),
            body.api_key.as_deref().unwrap_or(""),
            body.timestamp.unwrap_or(0)
        );
        let sig = body.signature.as_deref().unwrap_or("");
        if !NodeIdentity::verify_request(&peer_pubkey, sig, &msg) {
            state.audit.log("key_exchange_rejected", &format!("peer={} invalid_signature", peer_name), "", false);
            return err(StatusCode::FORBIDDEN, "Invalid signature — key exchange rejected").into_response();
        }

        // Register peer in user_auth if available
        if let Some(ua) = &state.user_auth {
            let username = format!("node:{}", peer_name);
            let _ = ua.add_user(&username, "member");
        }

        state.audit.log("key_exchange_ok", &format!("peer={}", peer_name), "", true);

        // Return our signed payload
        let payload = ni.sign_exchange(&state.node_name, &state.node_id, &state.auth.api_key);
        return (StatusCode::OK, Json(serde_json::to_value(payload).unwrap_or_default())).into_response();
    }

    // No node identity — return basic info
    state.audit.log("key_exchange_ok", &format!("peer={} (no sig verify)", peer_name), "", true);
    (StatusCode::OK, Json(serde_json::json!({
        "node_name": state.node_name,
        "node_id": state.node_id,
        "public_key": "",
        "note": "Node identity not configured",
    }))).into_response()
}

// ---------------------------------------------------------------------------
// Federation: user sync
// ---------------------------------------------------------------------------

async fn federation_list_users(
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

async fn federation_import_users(
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

// ---------------------------------------------------------------------------
// HTML: Dashboard + UI
// ---------------------------------------------------------------------------

/// GET / (HTML) — Info page about the grid (like GH Pages but with live API)
async fn landing_html(State(_state): State<AppState>) -> impl IntoResponse {
    // Serve the same page as GH Pages docs/index.html
    // but it will use same-origin API (EARTHGRID_API = '')
    let html = include_str!("../../docs/index.html");
    axum::response::Html(html)
}

async fn dashboard(State(state): State<AppState>) -> impl IntoResponse {
    let catalog = state.catalog.lock().await;
    let store = state.store.lock().await;
    let item_count = catalog.item_count(None).unwrap_or(0);
    let collections = catalog.list_collections().unwrap_or_default();
    let total_bytes = store.total_bytes();
    let chunk_count = store.chunk_count();
    drop(catalog);
    drop(store);

    let cols_html: String = collections.iter().map(|c| {
        format!("<li><strong>{}</strong> — {}</li>", c.id, c.description)
    }).collect::<Vec<_>>().join("\n");

    let html = format!(r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="earthgrid-api" content="">
<title>EarthGrid Node Dashboard</title>
<style>
  body {{ font-family: system-ui, sans-serif; margin: 0; padding: 20px; background: #0a0a1a; color: #e0e0e0; }}
  h1 {{ color: #4fc3f7; }}
  .card {{ background: #111; border: 1px solid #222; border-radius: 8px; padding: 16px; margin: 12px 0; }}
  .stat {{ display: inline-block; margin: 8px 16px 8px 0; }}
  .stat .val {{ font-size: 2em; font-weight: bold; color: #4fc3f7; }}
  .stat .lbl {{ color: #888; font-size: 0.85em; }}
  ul {{ list-style: none; padding: 0; }}
  li {{ padding: 4px 0; border-bottom: 1px solid #222; }}
  a {{ color: #4fc3f7; }}
</style>
</head>
<body>
<h1>🌍 EarthGrid Node</h1>
<div class="card">
  <h2>{node_name}</h2>
  <div class="stat"><div class="val">{item_count}</div><div class="lbl">Items</div></div>
  <div class="stat"><div class="val">{chunk_count}</div><div class="lbl">Chunks</div></div>
  <div class="stat"><div class="val">{storage_gb:.1} GB</div><div class="lbl">Storage Used</div></div>
  <div class="stat"><div class="val">{col_count}</div><div class="lbl">Collections</div></div>
</div>
<div class="card">
  <h3>Collections</h3>
  <ul>{cols_html}</ul>
</div>
<div class="card">
  <h3>Quick Links</h3>
  <ul>
    <li><a href="/node-info">/node-info</a> — Node info JSON</li>
    <li><a href="/stats">/stats</a> — Statistics</li>
    <li><a href="/stac/collections">/stac/collections</a> — STAC Collections</li>
    <li><a href="/peers">/peers</a> — Federation peers</li>
    <li><a href="/ui">/ui</a> — Management UI</li>
  </ul>
</div>
<p style="color:#555;font-size:0.8em">EarthGrid v{version} | Node ID: {node_id}</p>
</body>
</html>"#,
        node_name = state.node_name,
        item_count = item_count,
        chunk_count = chunk_count,
        storage_gb = total_bytes as f64 / 1_073_741_824.0,
        col_count = collections.len(),
        cols_html = cols_html,
        version = state.version,
        node_id = state.node_id,
    );

    Html(html).into_response()
}

async fn ui_page() -> impl IntoResponse {
    Html(include_str!("../assets/ui.html")).into_response()
}

// nodes list — mirrors /peers but returns node-centric view
async fn list_nodes(State(state): State<AppState>) -> impl IntoResponse {
    let peers = state.peers.lock().await;
    let nodes: Vec<serde_json::Value> = peers
        .list()
        .iter()
        .map(|p| serde_json::json!({
            "node_id": p.node_id,
            "node_name": p.node_name,
            "url": p.url,
            "last_seen": p.last_seen,
        }))
        .collect();
    let count = nodes.len();
    (StatusCode::OK, Json(serde_json::json!({ "nodes": nodes, "count": count }))).into_response()
}

// ---------------------------------------------------------------------------
// openEO: /processes, /validate, /jobs/{id}
// ---------------------------------------------------------------------------

/// GET /processes — openEO process descriptions
async fn openeo_processes() -> impl IntoResponse {
    let processes = serde_json::json!({
        "processes": [
            {
                "id": "load_collection",
                "summary": "Load a collection",
                "description": "Loads a collection from the current back-end by its id.",
                "parameters": [
                    {"name": "id", "description": "Collection identifier.", "schema": {"type": "string"}},
                    {"name": "spatial_extent", "description": "Spatial extent.", "schema": {"type": "object"}},
                    {"name": "temporal_extent", "description": "Temporal extent.", "schema": {"type": "array"}},
                    {"name": "bands", "description": "Band names.", "schema": {"type": "array"}}
                ],
                "returns": {"description": "A data cube.", "schema": {"type": "object"}}
            },
            {
                "id": "ndvi",
                "summary": "Normalized Difference Vegetation Index",
                "description": "Computes NDVI from red and NIR bands.",
                "parameters": [
                    {"name": "data", "description": "Input data cube.", "schema": {"type": "object"}},
                    {"name": "red", "description": "Red band name.", "schema": {"type": "string"}, "default": "B04"},
                    {"name": "nir", "description": "NIR band name.", "schema": {"type": "string"}, "default": "B08"}
                ],
                "returns": {"description": "NDVI data cube.", "schema": {"type": "object"}}
            },
            {
                "id": "save_result",
                "summary": "Save processed data",
                "description": "Save the result as a file.",
                "parameters": [
                    {"name": "data", "description": "Input data cube.", "schema": {"type": "object"}},
                    {"name": "format", "description": "Output format (GTiff, netCDF, PNG).", "schema": {"type": "string"}}
                ],
                "returns": {"description": "Saved file path.", "schema": {"type": "string"}}
            },
            {
                "id": "reduce_dimension",
                "summary": "Reduce a dimension",
                "description": "Reduces a dimension by applying a reducer.",
                "parameters": [
                    {"name": "data", "description": "Input data cube.", "schema": {"type": "object"}},
                    {"name": "reducer", "description": "A reducer process.", "schema": {"type": "object"}},
                    {"name": "dimension", "description": "Dimension name.", "schema": {"type": "string"}}
                ],
                "returns": {"description": "Reduced data cube.", "schema": {"type": "object"}}
            }
        ],
        "links": []
    });
    Json(processes)
}

/// POST /validate — validate an openEO process graph
async fn openeo_validate(
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let process_graph = body.get("process_graph").or_else(|| body.get("process"));
    match process_graph {
        Some(pg) => {
            // Basic validation: check it's an object with at least one node
            if let Some(obj) = pg.as_object() {
                if obj.is_empty() {
                    return (StatusCode::OK, Json(serde_json::json!({
                        "valid": false,
                        "errors": [{"code": "EmptyGraph", "message": "Process graph is empty"}]
                    }))).into_response();
                }
                // Check each node has process_id
                let mut errors = Vec::new();
                for (key, node) in obj {
                    if node.get("process_id").is_none() {
                        errors.push(serde_json::json!({
                            "code": "MissingProcessId",
                            "message": format!("Node '{}' missing process_id", key)
                        }));
                    }
                }
                if errors.is_empty() {
                    (StatusCode::OK, Json(serde_json::json!({"valid": true, "errors": []}))).into_response()
                } else {
                    (StatusCode::OK, Json(serde_json::json!({"valid": false, "errors": errors}))).into_response()
                }
            } else {
                (StatusCode::OK, Json(serde_json::json!({
                    "valid": false,
                    "errors": [{"code": "InvalidGraph", "message": "process_graph must be an object"}]
                }))).into_response()
            }
        }
        None => {
            (StatusCode::BAD_REQUEST, Json(serde_json::json!({
                "error": "Missing process_graph in request body"
            }))).into_response()
        }
    }
}

/// GET /jobs/{job_id} — openEO job status (stub)
async fn openeo_job_status(
    Path(job_id): Path<String>,
) -> impl IntoResponse {
    // EarthGrid currently only supports synchronous processing
    (StatusCode::NOT_FOUND, Json(serde_json::json!({
        "id": job_id,
        "status": "error",
        "message": "Batch jobs not yet supported. Use synchronous /process endpoint."
    }))).into_response()
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn router(state: AppState) -> Router {
    Router::new()
        // Core
        .route("/health", get(health))
        .route("/node-info", get(node_info))
        .route("/stats", get(stats))
        // STAC Landing + Conformance
        .route("/", get(stac_landing))
        .route("/conformance", get(stac_conformance))
        // STAC Collections + Items
        .route("/stac/collections", get(list_collections))
        .route("/stac/collections/{id}", get(get_collection))
        .route("/stac/collections/{id}/items", get(collection_items))
        .route("/stac/collections/{id}/items/{item_id}", get(get_collection_item))
        // STAC Search (GET + POST)
        .route("/stac/search", get(stac_search).post(stac_search_post))
        // Chunks
        .route("/chunks", get(list_chunks))
        .route("/chunks/{sha}", get(get_chunk))
        // Write
        .route("/ingest", post(ingest))
        // Integrity
        .route("/verify/{item_id}", get(verify_item))
        // Admin
        .route("/audit", get(audit_log))
        // Federation (Phase 2)
        .route("/peers", get(list_peers))
        .route("/peers", post(register_peer))
        .route("/federation/sync", post(federation_sync))
        .route("/federation/search", get(federation_search))
        // Gossip + file ingest
        .route("/peers.json", get(peers_json))
        .route("/ingest/file", post(ingest_file_endpoint))
        // Element84 STAC Fetcher
        .route("/fetch", post(fetch_handler))
        .route("/fetch/preview", get(fetch_preview))
        // Stats
        .route("/stats/downloads", get(stats_downloads))
        .route("/stats/hot-chunks", get(stats_hot_chunks))
        .route("/stats/replication-advice", get(stats_replication_advice))
        .route("/stats/ingest", get(stats_ingest_history))
        // Replication
        .route("/replicate", post(replicate))
        // Gamification
        .route("/gamification/leaderboard", get(gamification_leaderboard))
        .route("/gamification/node/{id}", get(gamification_node_profile))
        .route("/gamification/feed", get(gamification_feed))
        .route("/gamification/stats", get(gamification_stats))
        .route("/gamification/economy", get(gamification_economy))
        .route("/gamification/challenges", get(gamification_challenges))
        .route("/gamification/challenges/{id}", get(gamification_challenge_results))
        // Download (reconstruct + serve COG)
        .route("/download/{collection_id}/{item_id}", get(download_item))
        // Processing
        .route("/process", post(process_job))
        .route("/process/operations", get(process_operations))
        // Sync
        .route("/sync", post(sync_from_peer))
        .route("/sync-item", post(sync_item))
        // Admin
        .route("/admin/stats", get(admin_stats))
        .route("/admin/activity", get(admin_activity))
        .route("/admin/node/name", axum::routing::patch(patch_node_name))
        // Coverage + extended stats
        .route("/coverage/spatial", get(coverage_spatial))
        .route("/stats/coverage", get(stats_coverage))
        .route("/stats/requests", get(stats_requests))
        .route("/stats/bandwidth", get(stats_bandwidth))
        .route("/stats/replication", get(stats_replication_status))
        .route("/bandwidth", get(bandwidth_handler))
        // Nodes list + delete
        .route("/nodes", get(list_nodes))
        .route("/nodes/{node_id}", delete(delete_node))
        // Admin: collections
        .route("/admin/collections/{collection_id}", delete(admin_delete_collection))
        // Admin: users
        .route("/admin/users", get(admin_list_users).post(admin_create_user))
        .route("/admin/users/{user_id}", delete(admin_delete_user))
        // PATCH /node-name (alias)
        .route("/node-name", patch(patch_node_name_alias))
        // Stats: access + uptake
        .route("/stats/access", get(stats_access))
        .route("/stats.json", get(stats_json_alias))
        .route("/stats/uptake", get(stats_uptake))
        .route("/stats/uptake/csv", get(stats_uptake_csv))
        // Replication items list
        .route("/replicate/items", get(replicate_items))
        // Resize storage
        .route("/resize", post(resize_storage))
        // Chunk map
        .route("/chunk-map/{collection_id}/{item_id}", get(chunk_map))
        // Point extraction
        .route("/point/{collection_id}/{item_id}", get(point_extract))
        // Federation: key exchange + user sync
        .route("/federation/exchange-key", post(federation_exchange_key))
        .route("/federation/users", get(federation_list_users).post(federation_import_users))
        // HTML dashboard + UI
        .route("/dashboard", get(dashboard))
        .route("/ui", get(ui_page))
        // openEO compatibility aliases (without /stac/ prefix)
        .route("/collections", get(list_collections))
        .route("/collections/{id}", get(get_collection))
        // openEO processes + validate + jobs
        .route("/processes", get(openeo_processes))
        .route("/validate", post(openeo_validate))
        .route("/jobs/{job_id}", get(openeo_job_status))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Start server
// ---------------------------------------------------------------------------

pub async fn serve(
    data_dir: std::path::PathBuf,
    host: String,
    port: u16,
    p2p_channels: Option<(
        tokio::sync::mpsc::Receiver<crate::network::NetworkEvent>,
        tokio::sync::mpsc::Sender<crate::network::NetworkCommand>,
    )>,
) -> anyhow::Result<()> {
    use std::env;

    let store_path = data_dir.join("store");
    let catalog_path = data_dir.join("catalog.db");
    let audit_path = data_dir.join("audit.jsonl");
    let stats_db_path = data_dir.join("stats.db");
    let gamification_db_path = data_dir.join("gamification.db");

    let store = ChunkStore::new(&store_path, 0.0)?;
    let catalog = Catalog::new(&catalog_path)?;
    let audit = AuditLog::new(&audit_path);
    let auth = AuthConfig::from_env();
    let stats_engine = StatsEngine::new(&stats_db_path)?;
    let gamification_engine = GamificationEngine::new(&gamification_db_path)?;
    // Seed challenges on startup (no-op if already seeded)
    let _ = gamification_engine.seed_challenges();

    // Node identity from env, file, config.json, or generate+persist
    let earthgrid_home = dirs::home_dir().unwrap_or_default().join(".earthgrid");
    let id_path = earthgrid_home.join(".node_id");
    let node_id = env::var("EARTHGRID_NODE_ID")
        .ok()
        .or_else(|| {
            // Read persistent node_id from ~/.earthgrid/.node_id
            std::fs::read_to_string(&id_path).ok().map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
        })
        .or_else(|| {
            // Fallback: read node_id from config.json
            let cfg = earthgrid_home.join("config.json");
            if let Ok(c) = std::fs::read_to_string(&cfg) {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&c) {
                    return v["node_id"].as_str().map(|s| s.to_string()).filter(|s| !s.is_empty());
                }
            }
            None
        })
        .unwrap_or_else(|| {
            // Generate new ID and persist it
            let new_id = uuid::Uuid::new_v4().to_string();
            let _ = std::fs::create_dir_all(&earthgrid_home);
            let _ = std::fs::write(&id_path, &new_id);
            println!("📝 Generated new node ID: {} (saved to {})", new_id, id_path.display());
            new_id
        });
    let node_name = env::var("EARTHGRID_NODE_NAME")
        .ok()
        .or_else(|| {
            let cfg_path = data_dir.parent()
                .unwrap_or(&data_dir)
                .join("config.json");
            // Also try ~/.earthgrid/config.json
            let paths = [
                cfg_path,
                dirs::home_dir().unwrap_or_default().join(".earthgrid/config.json"),
            ];
            for p in &paths {
                if let Ok(c) = std::fs::read_to_string(p) {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&c) {
                        if let Some(n) = v["node_name"].as_str() {
                            return Some(n.to_string());
                        }
                    }
                }
            }
            None
        })
        .unwrap_or_else(|| "earthgrid-node".to_string());

    // Initial peers from env: comma-separated URLs
    let mut peer_registry = PeerRegistry::new();
    // Load peers from both env vars
    for var in ["EARTHGRID_PEERS", "EARTHGRID_BOOTSTRAP_PEERS"] {
        if let Ok(peers_env) = env::var(var) {
            for url in peers_env.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
                peer_registry.add(url, "", "");
            }
        }
    }

    // Optional: user auth DB
    let user_auth_opt = {
        let ua_path = data_dir.join("users.db");
        match UserAuth::new(&ua_path) {
            Ok(ua) => Some(Arc::new(ua)),
            Err(e) => {
                eprintln!("⚠️  UserAuth init failed: {}", e);
                None
            }
        }
    };

    // Optional: node identity
    let node_identity_opt = {
        let key_path = data_dir.join("node.key");
        match NodeIdentity::load_or_generate(&key_path) {
            Ok(ni) => Some(Arc::new(ni)),
            Err(e) => {
                eprintln!("⚠️  NodeIdentity init failed: {}", e);
                None
            }
        }
    };

    // Read storage_limit_gb from config
    let storage_limit_gb = {
        let cfg_path = dirs::home_dir().unwrap_or_default().join(".earthgrid/config.json");
        std::fs::read_to_string(&cfg_path)
            .ok()
            .and_then(|c| serde_json::from_str::<serde_json::Value>(&c).ok())
            .and_then(|v| v["storage_limit_gb"].as_f64())
            .unwrap_or(0.0)
    };

    let state = AppState {
        store: Arc::new(Mutex::new(store)),
        catalog: Arc::new(Mutex::new(catalog)),
        audit: Arc::new(audit),
        auth,
        peers: Arc::new(Mutex::new(peer_registry)),
        stats: Arc::new(stats_engine),
        gamification: Arc::new(gamification_engine),
        version: env!("CARGO_PKG_VERSION").to_string(),
        node_id,
        node_name: node_name.clone(),
        user_auth: user_auth_opt,
        node_identity: node_identity_opt,
        storage_limit_gb,
        data_dir: data_dir.clone(),
        active_requests: Arc::new(AtomicUsize::new(0)),
        is_beacon: env::var("EARTHGRID_BEACON")
            .map(|v| v.to_lowercase() == "true" || v == "1")
            .unwrap_or(false),
    };

    // Conditionally build beacon router (EARTHGRID_BEACON=true)
    let beacon_enabled = env::var("EARTHGRID_BEACON")
        .map(|v| v.to_lowercase() == "true" || v == "1")
        .unwrap_or(false);

    let hb_peers = state.peers.clone();
    // Clones for P2P handler
    let state_clone_store = state.store.clone();
    let state_clone_catalog = state.catalog.clone();
    let state_active_requests = state.active_requests.clone();
    let state_clone_gamification = state.gamification.clone();
    let state_node_id = state.node_id.clone();
    let state_node_name = state.node_name.clone();
    let state_version = state.version.clone();
    let repl_peers = state.peers.clone();
    let mut app = router(state);

    // Mount beacon routes if enabled
    if beacon_enabled {
        let beacon_db_path = data_dir.join("beacon.db");
        match BeaconRegistry::new(&beacon_db_path) {
            Ok(registry) => {
                let beacon_state = BeaconState {
                    registry: Arc::new(Mutex::new(registry)),
                };
                app = app.merge(beacon_router(beacon_state));
                println!("🔦 Beacon registry enabled ({})", beacon_db_path.display());
            }
            Err(e) => {
                eprintln!("⚠️  Failed to initialize beacon registry: {}", e);
            }
        }
    }

    let addr = format!("{}:{}", host, port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!(
        "🌍 EarthGrid Core v{} ({}) listening on {}",
        env!("CARGO_PKG_VERSION"),
        node_name,
        addr
    );
    // Spawn heartbeat + gossip loop
    
    tokio::spawn(async move {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        loop {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;

            let urls: Vec<String> = {
                let reg = hb_peers.lock().await;
                reg.urls()
            };

            for url in &urls {
                // 1. Sync node-info
                let info_url = format!("{}/node-info", url);
                match client.get(&info_url).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        if let Ok(info) = resp.json::<NodeInfo>().await {
                            let mut reg = hb_peers.lock().await;
                            reg.update_from_info(url, &info);
                        }
                    }
                    _ => {
                        let mut reg = hb_peers.lock().await;
                        reg.record_failure(url);
                    }
                }

                // 2. Gossip: fetch peers from this peer
                let gossip_url = format!("{}/peers.json", url);
                if let Ok(resp) = client.get(&gossip_url).send().await {
                    if resp.status().is_success() {
                        if let Ok(gossip) = resp.json::<GossipPeerList>().await {
                            let mut reg = hb_peers.lock().await;
                            for entry in &gossip.peers {
                                reg.add_if_new(&entry.url);
                            }
                        }
                    }
                }
            }
        }
    });

    // Auto-replication: sync from peers every 5 minutes
    {
        let repl_store = state_clone_store.clone();
        let repl_catalog = state_clone_catalog.clone();
        let repl_active = state_active_requests.clone();

        tokio::spawn(async move {
            // Initial delay: wait 30s for peers to be discovered
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;

            loop {
                let urls: Vec<String> = {
                    let reg = repl_peers.lock().await;
                    reg.urls()
                };

                if !urls.is_empty() {
                    // Yield to active fetch/ingest requests
                    if repl_active.load(Ordering::Relaxed) > 0 {
                        eprintln!("🔄 Auto-replication: skipping cycle — {} active request(s)", repl_active.load(Ordering::Relaxed));
                    } else {
                        let replicator = Replicator::new(repl_store.clone(), repl_catalog.clone());
                        for url in &urls {
                            // Check again before each peer (request may have started)
                            if repl_active.load(Ordering::Relaxed) > 0 {
                                eprintln!("🔄 Auto-replication: pausing mid-cycle — active request(s) detected");
                                break;
                            }
                            let result = replicator.sync_from_peer(url, &[], 0, false).await;
                            if result.chunks_downloaded > 0 || !result.errors.is_empty() {
                                eprintln!(
                                    "🔄 Auto-replicate from {}: {} items, {} chunks ({} bytes), {} errors",
                                    url, result.items_processed, result.chunks_downloaded,
                                    result.bytes_downloaded, result.errors.len()
                                );
                            }
                        }
                    }
                }

                tokio::time::sleep(std::time::Duration::from_secs(300)).await;
            }
        });
    }

    // Self-heartbeat: beacon nodes register themselves, non-beacon nodes register with remote beacon
    {
        let hb_store = state_clone_store.clone();
        let hb_catalog = state_clone_catalog.clone();
        let hb_node_id = state_node_id.clone();
        let hb_node_name = state_node_name.clone();
        let hb_storage_limit_gb = storage_limit_gb;
        let hb_gamification = state_clone_gamification.clone();
        let hb_port = port;
        let beacon_url_env = std::env::var("EARTHGRID_BEACON_URL").ok();

        let target_url = if beacon_enabled {
            Some(format!("http://127.0.0.1:{}/beacon/heartbeat", hb_port))
        } else {
            beacon_url_env.map(|u| format!("{}/beacon/heartbeat", u.trim_end_matches('/')))
        };

        if let Some(url) = target_url {
            tokio::spawn(async move {
                let client = reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(10))
                    .build()
                    .unwrap_or_default();
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;

                    let item_count = {
                        let cat = hb_catalog.lock().await;
                        cat.item_count(None).unwrap_or(0)
                    };
                    let (chunk_count, chunks_bytes) = {
                        let store = hb_store.lock().await;
                        (store.chunk_count(), store.total_bytes())
                    };
                    let collections: Vec<String> = {
                        let cat = hb_catalog.lock().await;
                        cat.list_collections().unwrap_or_default().into_iter().map(|c| c.id).collect()
                    };

                    let body = serde_json::json!({
                        "node_id": hb_node_id,
                        "node_name": hb_node_name,
                        "can_source": true,
                        "item_count": item_count,
                        "chunk_count": chunk_count,
                        "chunks_bytes": chunks_bytes,
                        "collections": collections,
                        "storage_limit_gb": hb_storage_limit_gb,
                    });

                    // First attempt: heartbeat (fast path for already-registered nodes)
                    let resp = client.post(&url).json(&body).send().await;
                    // If not registered, register first then heartbeat
                    if let Ok(r) = &resp {
                        if r.status() == reqwest::StatusCode::NOT_FOUND {
                            let register_url = url.replace("/beacon/heartbeat", "/beacon/register");
                            let register_body = serde_json::json!({
                                "node_id": hb_node_id,
                                "node_name": hb_node_name,
                                "url": format!("http://127.0.0.1:{}", hb_port),
                                "can_source": true,
                                "item_count": item_count,
                                "chunk_count": chunk_count,
                                "chunks_bytes": chunks_bytes,
                                "collections": collections,
                                "storage_limit_gb": hb_storage_limit_gb,
                            });
                            let _ = client.post(&register_url).json(&register_body).send().await;
                        }
                    }

                    // Update gamification DB
                    let _ = hb_gamification.ensure_node_registered(
                        &hb_node_id, &hb_node_name, "", "", "",
                    );
                    let _ = hb_gamification.record_heartbeat(
                        &hb_node_id, 0, 0.0, 5000.0,
                    );
                    // Sync actual storage stats into gamification DB
                    let _ = hb_gamification.update_storage_stats(
                        &hb_node_id, item_count as i64, chunks_bytes as i64,
                    );
                }
            });
        }
    }

    // Spawn P2P request handler if libp2p channels are provided
    if let Some((mut event_rx, cmd_tx)) = p2p_channels {
        let p2p_store = state_clone_store.clone();
        let p2p_catalog = state_clone_catalog.clone();
        let p2p_node_id = state_node_id.clone();
        let p2p_node_name = state_node_name.clone();
        let p2p_version = state_version.clone();

        tokio::spawn(async move {
            use crate::network::{NetworkCommand, NetworkEvent};
            use crate::transport::{EarthGridRequest, EarthGridResponse};

            while let Some(event) = event_rx.recv().await {
                match event {
                    NetworkEvent::InboundRequest { peer: _, request, channel } => {
                        let response = match request {
                            EarthGridRequest::GetChunk { hash } => {
                                let mut store = p2p_store.lock().await;
                                match store.get(&hash) {
                                    Ok(Some(data)) => EarthGridResponse::Chunk {
                                        hash: hash.clone(),
                                        data,
                                    },
                                    _ => EarthGridResponse::ChunkNotFound { hash },
                                }
                            }
                            EarthGridRequest::SearchCatalog { collection, bbox, datetime: _, limit } => {
                                let catalog = p2p_catalog.lock().await;
                                let items = catalog
                                    .search(collection.as_deref(), bbox, None, limit, 0)
                                    .unwrap_or_default();
                                let json_items: Vec<serde_json::Value> = items
                                    .into_iter()
                                    .map(|i| serde_json::to_value(i).unwrap_or_default())
                                    .collect();
                                let total = json_items.len();
                                EarthGridResponse::CatalogResults {
                                    items: json_items,
                                    total,
                                }
                            }
                            EarthGridRequest::NodeInfo => {
                                let store = p2p_store.lock().await;
                                let catalog = p2p_catalog.lock().await;
                                let collections: Vec<String> = catalog
                                    .list_collections()
                                    .unwrap_or_default()
                                    .into_iter()
                                    .map(|c| c.id)
                                    .collect();
                                EarthGridResponse::Info {
                                    node_id: p2p_node_id.clone(),
                                    node_name: p2p_node_name.clone(),
                                    version: p2p_version.clone(),
                                    collections,
                                    item_count: catalog.item_count(None).unwrap_or(0),
                                    chunk_count: store.chunk_count(),
                                    storage_bytes: store.total_bytes(),
                                }
                            }
                            EarthGridRequest::GetPeers => {
                                EarthGridResponse::Peers { peers: vec![] }
                            }
                            EarthGridRequest::ExecuteJob { .. } => {
                                EarthGridResponse::JobError {
                                    message: "Job execution not yet supported via P2P".to_string(),
                                }
                            }
                        };

                        // Send response back via the swarm
                        let _ = cmd_tx.send(NetworkCommand::SendResponse {
                            channel,
                            response,
                        }).await;
                    }
                    NetworkEvent::PeerDiscovered { peer_id, addresses } => {
                        eprintln!("🔗 P2P: Discovered peer {} at {:?}", peer_id, addresses);
                    }
                    NetworkEvent::PeerLost(peer_id) => {
                        eprintln!("🔗 P2P: Lost peer {}", peer_id);
                    }
                }
            }
        });
    }

    axum::serve(listener, app).await?;
    Ok(())
}
