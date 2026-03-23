use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};

use crate::server::{AppState, api_key, err, LimitQuery};
use axum::response::Html;


// ---------------------------------------------------------------------------
// Core handlers
// ---------------------------------------------------------------------------

pub(crate) async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}


pub(crate) async fn node_info(State(state): State<AppState>) -> Json<serde_json::Value> {
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
    let cfg_path = dirs::home_dir().unwrap_or_default().join(".earthgrid/config.json");
    let cfg: Option<serde_json::Value> = std::fs::read_to_string(&cfg_path)
        .ok()
        .and_then(|c| serde_json::from_str(&c).ok());
    let beacon_url = cfg.as_ref().and_then(|v| v["beacon_url"].as_str().map(|s| s.to_string()));
    let auto_update = cfg.as_ref()
        .and_then(|v| v["auto_update"].as_str().map(|s| s.to_string()))
        .unwrap_or_else(|| "no".to_string());

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
        "openeo": true,
        "beacon_url": beacon_url,
        "auto_update": auto_update,
        "is_beacon": state.is_beacon,
    }))
}


pub(crate) async fn audit_log(
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
// Download handler — GET /download/{collection}/{item_id}
// Reconstruct COG from chunks and stream to client.
// ---------------------------------------------------------------------------

pub(crate) async fn download_item(
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
// HTML: Dashboard + UI
// ---------------------------------------------------------------------------

/// GET / (HTML) — Info page about the grid (like GH Pages but with live API)
pub(crate) async fn landing_html() -> impl IntoResponse {
    Html(include_str!("../../assets/beacon.html")).into_response()
}


pub(crate) async fn dashboard() -> impl IntoResponse {
    Html(include_str!("../../assets/ui.html")).into_response()
}



// nodes list — mirrors /peers but returns node-centric view
pub(crate) async fn list_nodes(State(state): State<AppState>) -> impl IntoResponse {
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
pub(crate) async fn openeo_processes() -> impl IntoResponse {
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
pub(crate) async fn openeo_validate(
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
pub(crate) async fn openeo_job_status(
    Path(job_id): Path<String>,
) -> impl IntoResponse {
    // EarthGrid currently only supports synchronous processing
    (StatusCode::NOT_FOUND, Json(serde_json::json!({
        "id": job_id,
        "status": "error",
        "message": "Batch jobs not yet supported. Use synchronous /process endpoint."
    }))).into_response()
}


/// GET /file_formats — openEO output/input format catalogue
pub(crate) async fn openeo_file_formats() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({
        "input": {
            "GTiff": {
                "title": "GeoTIFF",
                "gis_data_types": ["raster"],
                "parameters": {}
            }
        },
        "output": {
            "GTiff": {
                "title": "GeoTIFF",
                "gis_data_types": ["raster"],
                "parameters": {}
            },
            "netCDF": {
                "title": "netCDF-4",
                "gis_data_types": ["raster"],
                "parameters": {}
            }
        }
    }))).into_response()
}


/// POST /result — openEO synchronous execution
pub(crate) async fn openeo_result(
    headers: HeaderMap,
    State(state): State<AppState>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .or_else(|| headers.get("x-api-key").and_then(|v| v.to_str().ok()));

    if state.auth.check_write(token).is_err() {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"code": "AuthenticationRequired", "message": "API key required"})),
        ).into_response();
    }

    // Accept both wrapped (process.process_graph) and bare (process_graph) payloads
    let graph_value = body
        .get("process")
        .and_then(|p| p.get("process_graph"))
        .cloned()
        .or_else(|| body.get("process_graph").cloned())
        .map(|pg| serde_json::json!({ "process_graph": pg }))
        .unwrap_or(body);

    let graph: crate::openeo::ProcessGraph = match serde_json::from_value(graph_value) {
        Ok(g) => g,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "code": "ProcessGraphInvalid",
                    "message": format!("Invalid process graph: {}", e)
                })),
            ).into_response();
        }
    };

    let fmt = crate::openeo::extract_output_format(&graph).unwrap_or_else(|| "GTiff".to_string());
    match crate::openeo::execute_sync(&graph, &state.catalog, &state.store).await {
        Ok((data, meta)) => {
            if let Some(ref m) = meta {
                match crate::openeo::wrap_output(&data, m, &fmt) {
                    Ok((output, ct)) => (
                        StatusCode::OK,
                        [(axum::http::header::CONTENT_TYPE, ct)],
                        output,
                    ).into_response(),
                    Err(e) => (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({"code": "FormatError", "message": e})),
                    ).into_response(),
                }
            } else {
                (
                    StatusCode::OK,
                    [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                    data,
                ).into_response()
            }
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"code": "ProcessingError", "message": e})),
        ).into_response(),
    }
}

