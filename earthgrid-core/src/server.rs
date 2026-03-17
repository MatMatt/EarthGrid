//! Axum HTTP server for EarthGrid Core.
//!
//! Exposes a STAC-compatible REST API backed by ChunkStore, Catalog, Auth, and Audit.

use std::sync::Arc;

use axum::{
    Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;

use crate::{
    audit::AuditLog,
    auth::AuthConfig,
    catalog::{Catalog, StacItem},
    chunk_store::ChunkStore,
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
    pub version: String,
}

// ---------------------------------------------------------------------------
// Helper: extract X-API-Key
// ---------------------------------------------------------------------------

fn api_key(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
}

fn err(status: StatusCode, msg: &str) -> (StatusCode, Json<serde_json::Value>) {
    (status, Json(serde_json::json!({"error": msg})))
}

// ---------------------------------------------------------------------------
// Request/Response types
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct SearchQuery {
    pub collection: Option<String>,
    pub bbox: Option<String>,   // "west,south,east,north"
    pub limit: Option<usize>,
}

#[derive(Deserialize)]
pub struct LimitQuery {
    pub limit: Option<usize>,
}

#[derive(Deserialize, Serialize)]
pub struct IngestRequest {
    pub item: StacItem,
    pub chunks: std::collections::HashMap<String, String>, // filename → base64 data
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

// GET /health
async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

// GET /node-info
async fn node_info(State(state): State<AppState>) -> Json<serde_json::Value> {
    let store = state.store.lock().await;
    let catalog = state.catalog.lock().await;
    let stats = store.stats().clone();
    let item_count = catalog.item_count(None).unwrap_or(0);
    Json(serde_json::json!({
        "version": state.version,
        "chunks": store.chunk_count(),
        "storage_bytes": store.total_bytes(),
        "storage_gb": store.total_bytes() as f64 / 1_073_741_824.0,
        "items": item_count,
        "auth_enabled": state.auth.is_enabled(),
        "chunks_served": stats.chunks_served,
        "bytes_served": stats.bytes_served,
        "requests_total": stats.requests_total,
    }))
}

// GET /stats
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

// GET /stac/collections
async fn list_collections(State(state): State<AppState>) -> impl IntoResponse {
    let catalog = state.catalog.lock().await;
    match catalog.list_collections() {
        Ok(cols) => (StatusCode::OK, Json(serde_json::json!({
            "collections": cols,
            "numberMatched": cols.len(),
        }))).into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

// GET /stac/collections/:id
async fn get_collection(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let catalog = state.catalog.lock().await;
    match catalog.get_collection(&id) {
        Ok(Some(col)) => (StatusCode::OK, Json(serde_json::to_value(col).unwrap())).into_response(),
        Ok(None) => err(StatusCode::NOT_FOUND, "Collection not found").into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

// GET /stac/collections/:id/items
async fn collection_items(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<LimitQuery>,
) -> impl IntoResponse {
    let catalog = state.catalog.lock().await;
    let limit = q.limit.unwrap_or(50).min(1000);
    match catalog.search(Some(&id), None, limit) {
        Ok(items) => (StatusCode::OK, Json(serde_json::json!({
            "type": "FeatureCollection",
            "features": items,
            "numberMatched": items.len(),
        }))).into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

// GET /stac/search
async fn stac_search(
    State(state): State<AppState>,
    Query(q): Query<SearchQuery>,
) -> impl IntoResponse {
    let catalog = state.catalog.lock().await;
    let limit = q.limit.unwrap_or(50).min(1000);
    let bbox = q.bbox.as_deref().and_then(|s| {
        let parts: Vec<f64> = s.split(',').filter_map(|p| p.trim().parse().ok()).collect();
        if parts.len() == 4 { Some([parts[0], parts[1], parts[2], parts[3]]) } else { None }
    });
    match catalog.search(q.collection.as_deref(), bbox, limit) {
        Ok(items) => (StatusCode::OK, Json(serde_json::json!({
            "type": "FeatureCollection",
            "features": items,
            "numberMatched": items.len(),
        }))).into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

// GET /chunks/:sha
async fn get_chunk(
    State(state): State<AppState>,
    Path(sha): Path<String>,
) -> impl IntoResponse {
    let mut store = state.store.lock().await;
    match store.get(&sha) {
        Ok(Some(data)) => (StatusCode::OK, data).into_response(),
        Ok(None) => err(StatusCode::NOT_FOUND, "Chunk not found").into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

// GET /chunks
async fn list_chunks(
    State(state): State<AppState>,
    Query(q): Query<LimitQuery>,
) -> impl IntoResponse {
    let store = state.store.lock().await;
    let limit = q.limit.unwrap_or(100).min(10000);
    let mut chunks = store.list_chunks();
    chunks.truncate(limit);
    Json(serde_json::json!({"chunks": chunks, "count": chunks.len()}))
}

// POST /ingest  (requires write auth)
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

    let item: StacItem = match serde_json::from_value(payload.get("item").cloned().unwrap_or_default()) {
        Ok(i) => i,
        Err(e) => return err(StatusCode::BAD_REQUEST, &format!("Invalid item: {}", e)).into_response(),
    };

    // Ingest base64-encoded chunks if provided
    if let Some(chunks_val) = payload.get("chunks") {
        if let Some(chunks_map) = chunks_val.as_object() {
            let mut store = state.store.lock().await;
            for (_name, data_val) in chunks_map {
                if let Some(b64) = data_val.as_str() {
                    use std::io::Read;
                    let decoded: Vec<u8> = {
                        let mut buf = Vec::new();
                        let mut dec = base64_decode_reader(b64);
                        if dec.read_to_end(&mut buf).is_err() {
                            return err(StatusCode::BAD_REQUEST, "Invalid base64 in chunks").into_response();
                        }
                        buf
                    };
                    if let Err(e) = store.put(&decoded) {
                        return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response();
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

// Simple base64 decoder (no external dep — use standard decode)
fn base64_decode_reader(s: &str) -> std::io::Cursor<Vec<u8>> {
    // Manual base64 decode
    let alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = Vec::new();
    let clean: String = s.chars().filter(|c| *c != '=' && *c != '\n' && *c != '\r').collect();
    let bytes: Vec<u8> = clean
        .chars()
        .filter_map(|c| alphabet.find(c).map(|i| i as u8))
        .collect();
    let mut i = 0;
    while i + 3 < bytes.len() {
        let b0 = bytes[i];
        let b1 = bytes[i + 1];
        let b2 = bytes[i + 2];
        let b3 = bytes[i + 3];
        output.push((b0 << 2) | (b1 >> 4));
        output.push(((b1 & 0x0f) << 4) | (b2 >> 2));
        output.push(((b2 & 0x03) << 6) | b3);
        i += 4;
    }
    std::io::Cursor::new(output)
}

// GET /verify/:item_id
async fn verify_item(
    State(state): State<AppState>,
    Path(item_id): Path<String>,
) -> impl IntoResponse {
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
            "item_id": item_id,
            "total": total,
            "valid": valid,
            "missing": missing,
            "corrupted": corrupted,
            "ok": ok,
        })),
    ).into_response()
}

// GET /audit  (requires admin auth)
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
    Json(serde_json::json!({"entries": entries, "count": entries.len()})).into_response()
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/node-info", get(node_info))
        .route("/stats", get(stats))
        .route("/stac/collections", get(list_collections))
        .route("/stac/collections/{id}", get(get_collection))
        .route("/stac/collections/{id}/items", get(collection_items))
        .route("/stac/search", get(stac_search))
        .route("/chunks", get(list_chunks))
        .route("/chunks/{sha}", get(get_chunk))
        .route("/ingest", post(ingest))
        .route("/verify/{item_id}", get(verify_item))
        .route("/audit", get(audit_log))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Start server
// ---------------------------------------------------------------------------

pub async fn serve(data_dir: std::path::PathBuf, host: String, port: u16) -> anyhow::Result<()> {
    let store_path = data_dir.join("store");
    let catalog_path = data_dir.join("catalog.db");
    let audit_path = data_dir.join("audit.jsonl");

    let store = ChunkStore::new(&store_path, 0.0)?;
    let catalog = Catalog::new(&catalog_path)?;
    let audit = AuditLog::new(&audit_path);
    let auth = AuthConfig::from_env();

    let state = AppState {
        store: Arc::new(Mutex::new(store)),
        catalog: Arc::new(Mutex::new(catalog)),
        audit: Arc::new(audit),
        auth,
        version: env!("CARGO_PKG_VERSION").to_string(),
    };

    let app = router(state);
    let addr = format!("{}:{}", host, port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("🌍 EarthGrid Core v{} listening on {}", env!("CARGO_PKG_VERSION"), addr);
    axum::serve(listener, app).await?;
    Ok(())
}
