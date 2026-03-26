use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;

use crate::server::{AppState, api_key, err, LimitQuery};
use crate::replication::Replicator;



/// Fetch a chunk from alive peers. Returns the raw bytes on first success.
pub(crate) async fn fetch_chunk_from_peers(state: &AppState, sha: &str) -> Option<Vec<u8>> {
    let peer_urls: Vec<String> = {
        let registry = state.peers.lock().await;
        registry.list().into_iter()
            .filter(|p| p.alive())
            .map(|p| p.url.clone())
            .collect()
    };
    if peer_urls.is_empty() { return None; }

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .ok()?;

    // Race all peers — first success wins
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1);
    for url in &peer_urls {
        let client = client.clone();
        let url = url.clone();
        let sha = sha.to_string();
        let tx = tx.clone();
        tokio::spawn(async move {
            let resp = client.get(format!("{}/api/chunks/{}", url, sha)).send().await;
            if let Ok(r) = resp {
                if r.status().is_success() {
                    if let Ok(bytes) = r.bytes().await {
                        let _ = tx.send(bytes.to_vec()).await;
                    }
                }
            }
        });
    }
    drop(tx); // Drop sender so rx completes when all spawned tasks finish
    rx.recv().await
}

pub(crate) async fn get_chunk(State(state): State<AppState>, Path(sha): Path<String>) -> impl IntoResponse {
    // 1. Try local store first
    {
        let mut store = state.store.lock().await;
        match store.get(&sha) {
            Ok(Some(data)) => return (StatusCode::OK, data).into_response(),
            Ok(None) => {} // Fall through to peer lookup
            Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
        }
    }

    // 2. Chunk not local — try alive peers
    eprintln!("📡 Chunk {} not local, trying peers...", &sha[..8.min(sha.len())]);
    match fetch_chunk_from_peers(&state, &sha).await {
        Some(data) => {
            // Cache locally for future requests
            let mut store = state.store.lock().await;
            let _ = store.put(&data);
            eprintln!("📡 Chunk {} fetched from peer and cached", &sha[..8.min(sha.len())]);
            (StatusCode::OK, data).into_response()
        }
        None => err(StatusCode::NOT_FOUND, "Chunk not found on any node").into_response(),
    }
}


pub(crate) async fn list_chunks(State(state): State<AppState>, Query(q): Query<LimitQuery>) -> impl IntoResponse {
    let store = state.store.lock().await;
    let limit = q.limit.unwrap_or(100).min(10000);
    let chunks = store.list_chunks(Some(limit));
    let count = chunks.len();
    Json(serde_json::json!({"chunks": chunks, "count": count}))
}


pub(crate) async fn verify_item(State(state): State<AppState>, Path(item_id): Path<String>) -> impl IntoResponse {
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


pub(crate) async fn replicate(
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
// ---------------------------------------------------------------------------
// Replicate/items — list items available for replication
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct ReplicateItemsQuery {
    pub collection: Option<String>,
    pub limit: Option<usize>,
}


pub(crate) async fn replicate_items(
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


pub(crate) async fn resize_storage(
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
    // Update config.json — try ~/.earthgrid/config.json first, then data_dir
    let config_path = {
        let home_cfg = dirs::home_dir().unwrap_or_default().join(".earthgrid/config.json");
        if home_cfg.exists() { home_cfg } else { state.data_dir.join("config.json") }
    };
    let old_gb = f64::from_bits(state.storage_limit_gb.load(std::sync::atomic::Ordering::Relaxed));
    if config_path.exists() {
        if let Ok(content) = std::fs::read_to_string(&config_path) {
            if let Ok(mut cfg) = serde_json::from_str::<serde_json::Value>(&content) {
                cfg["storage_limit_gb"] = serde_json::json!(size_gb);
                let _ = std::fs::write(&config_path, serde_json::to_string_pretty(&cfg).unwrap_or_default());
            }
        }
    }
    // Update in-memory limit
    state.storage_limit_gb.store(size_gb.to_bits(), std::sync::atomic::Ordering::Relaxed);

    // Smart eviction: if new limit is below current usage, evict items
    let current_bytes = {
        let store = state.store.lock().await;
        store.total_bytes()
    };
    let current_gb = current_bytes as f64 / 1_073_741_824.0;
    let eviction_result = if size_gb < current_gb {
        let beacon_db_path = state.data_dir.join("beacon.db");
        let beacon_path = if beacon_db_path.exists() { Some(beacon_db_path) } else { None };
        let catalog = state.catalog.lock().await;
        let mut store = state.store.lock().await;
        match crate::eviction::evict(
            &catalog,
            &mut store,
            size_gb,
            beacon_path.as_deref(),
        ) {
            Ok(result) => Some(result),
            Err(e) => {
                eprintln!("⚠️  Eviction error: {}", e);
                None
            }
        }
    } else {
        None
    };

    state.audit.log("resize", &format!("size_gb={} (was {})", size_gb, old_gb), "", true);
    let mut resp = serde_json::json!({
        "status": "resized",
        "old_gb": old_gb,
        "new_gb": size_gb,
        "config_path": config_path.display().to_string(),
    });
    if let Some(ev) = eviction_result {
        resp["eviction"] = serde_json::json!({
            "items_deleted": ev.items_deleted,
            "bytes_freed": ev.bytes_freed,
            "gb_freed": ev.bytes_freed as f64 / 1_073_741_824.0,
            "items_kept_as_last_replica": ev.items_kept,
            "reason": ev.reason,
        });
    } else {
        resp["note"] = serde_json::json!("Storage within new limit, no eviction needed");
    }
    (StatusCode::OK, Json(resp)).into_response()
}


// ---------------------------------------------------------------------------
// GET /chunk-map/{collection_id}/{item_id}
// ---------------------------------------------------------------------------

pub(crate) async fn chunk_map(
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


pub(crate) async fn point_extract(
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

