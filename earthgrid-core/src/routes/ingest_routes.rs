use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;

use crate::server::{AppState, api_key, err};
use std::sync::atomic::Ordering;
use crate::server::ActiveRequestGuard;
use crate::{fetcher, ingest};
use crate::catalog::StacItem;


pub(crate) async fn ingest(
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
                    match crate::routes::chunks::base64_decode(b64) {
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


/// POST /ingest/file — ingest a file from a local path on the server
pub(crate) async fn ingest_file_endpoint(
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
pub(crate) async fn fetch_preview(
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
pub(crate) async fn fetch_handler(
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

