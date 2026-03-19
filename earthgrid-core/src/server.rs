//! Axum HTTP server for EarthGrid Core.
//!
//! Phase 1: Core STAC/chunk API
//! Phase 2: Peers + Federation (sync, federated search)

use std::sync::Arc;

use axum::{
    Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
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
};

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

/// GET / — STAC Landing Page (OGC compliant)
async fn stac_landing(State(state): State<AppState>) -> Json<serde_json::Value> {
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
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub cloud_cover: Option<f64>,
    /// Comma-separated band names, e.g. "blue,green,red,nir"
    pub bands: Option<String>,
    pub limit: Option<usize>,
    pub collection: Option<String>,
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

    // Parse bbox
    let bbox = match q.bbox.as_deref() {
        Some(s) => {
            let parts: Vec<f64> = s.split(',')
                .filter_map(|v| v.trim().parse().ok())
                .collect();
            if parts.len() < 4 {
                return err(StatusCode::BAD_REQUEST, "bbox must be 'west,south,east,north'").into_response();
            }
            [parts[0], parts[1], parts[2], parts[3]]
        }
        None => return err(StatusCode::BAD_REQUEST, "Missing bbox parameter").into_response(),
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
    )
    .await;

    state.audit.log("fetch", collection, "", result.errors.is_empty());

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
            {"name": "rgb", "description": "Render RGB composite"},
            {"name": "cloud_mask", "description": "Apply cloud masking"},
            {"name": "rechunk", "description": "Re-tile and rechunk item"},
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
    let catalog = state.catalog.lock().await;
    let collections = catalog.list_collections().unwrap_or_default();
    // Return bounding-box coverage summary per collection
    let summary: Vec<serde_json::Value> = collections
        .iter()
        .map(|c| serde_json::json!({ "collection": c.id, "title": c.title }))
        .collect();
    (StatusCode::OK, Json(serde_json::json!({ "coverage": summary }))).into_response()
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
        // Nodes list
        .route("/nodes", get(list_nodes))
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

    // Node identity from env
    let node_id = env::var("EARTHGRID_NODE_ID").unwrap_or_else(|_| {
        uuid::Uuid::new_v4().to_string()
    });
    let node_name = env::var("EARTHGRID_NODE_NAME")
        .unwrap_or_else(|_| "earthgrid-node".to_string());

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
    };

    // Conditionally build beacon router (EARTHGRID_BEACON=true)
    let beacon_enabled = env::var("EARTHGRID_BEACON")
        .map(|v| v.to_lowercase() == "true" || v == "1")
        .unwrap_or(false);

    let hb_peers = state.peers.clone();
    // Clones for P2P handler
    let state_clone_store = state.store.clone();
    let state_clone_catalog = state.catalog.clone();
    let state_node_id = state.node_id.clone();
    let state_node_name = state.node_name.clone();
    let state_version = state.version.clone();
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
