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
pub(crate) async fn landing_html(State(_state): State<AppState>) -> impl IntoResponse {
    // Serve the same page as GH Pages docs/index.html
    // but it will use same-origin API (EARTHGRID_API = '')
    let html = include_str!("../../../docs/index.html");
    axum::response::Html(html)
}


pub(crate) async fn dashboard(State(state): State<AppState>) -> impl IntoResponse {
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


pub(crate) async fn ui_page() -> impl IntoResponse {
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

