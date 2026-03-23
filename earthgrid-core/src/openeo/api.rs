//! openEO HTTP routes and job API.

use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;

use crate::server::AppState;

use super::execute::execute_sync;
use super::graph::extract_output_format;
use super::output::wrap_output;
use super::types::{JobResult, JobStore, ProcessGraph};

#[derive(Clone)]
pub struct OpenEOState {
    pub app: AppState,
    pub jobs: JobStore,
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

async fn openeo_capabilities() -> Json<serde_json::Value> {
    Json(super::catalogue::capabilities(""))
}

async fn well_known_openeo() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "versions": [{
            "url": "/",
            "api_version": super::API_VERSION,
            "production": false
        }]
    }))
}

async fn credentials_basic(headers: HeaderMap, State(state): State<OpenEOState>) -> impl IntoResponse {
    // Extract Basic auth, return bearer token
    if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        let parts: Vec<&str> = auth.splitn(2, ' ').collect();
        if parts.len() == 2 && parts[0].eq_ignore_ascii_case("basic") {
            // Decode base64
            if let Ok(decoded) = base64_decode(parts[1]) {
                if let Some((_user, pass)) = decoded.split_once(':') {
                    // Check if password matches API key
                    if state.app.auth.check_write(Some(pass)).is_ok() {
                        return (
                            StatusCode::OK,
                            Json(serde_json::json!({
                                "access_token": pass,
                                "token_type": "bearer"
                            })),
                        );
                    }
                }
            }
        }
    }
    (
        StatusCode::FORBIDDEN,
        Json(serde_json::json!({"code": "AuthenticationRequired", "message": "Invalid credentials"})),
    )
}

fn base64_decode(input: &str) -> Result<String, String> {
    // Simple base64 decode without external crate
    use std::collections::HashMap;
    let chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut table = HashMap::new();
    for (i, c) in chars.chars().enumerate() {
        table.insert(c, i as u8);
    }

    let input = input.trim_end_matches('=');
    let mut bytes = Vec::new();
    let mut buf = 0u32;
    let mut bits = 0;

    for c in input.chars() {
        if let Some(&val) = table.get(&c) {
            buf = (buf << 6) | val as u32;
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                bytes.push((buf >> bits) as u8);
                buf &= (1 << bits) - 1;
            }
        }
    }

    String::from_utf8(bytes).map_err(|e| e.to_string())
}

async fn me_handler(headers: HeaderMap, State(state): State<OpenEOState>) -> impl IntoResponse {
    // Check bearer token
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .or_else(|| headers.get("x-api-key").and_then(|v| v.to_str().ok()));

    if let Some(t) = token {
        if state.app.auth.check_write(Some(t)).is_ok() {
            return (
                StatusCode::OK,
                Json(serde_json::json!({
                    "user_id": "earthgrid-user",
                    "name": "EarthGrid User",
                    "budget": null,
                    "links": []
                })),
            );
        }
    }
    if !state.app.auth.is_enabled() {
        return (
            StatusCode::OK,
            Json(serde_json::json!({
                "user_id": "earthgrid-user",
                "name": "EarthGrid User (open mode)",
                "budget": null,
                "links": []
            })),
        );
    }
    (
        StatusCode::FORBIDDEN,
        Json(serde_json::json!({"code": "AuthenticationRequired", "message": "Not authenticated"})),
    )
}

async fn openeo_collections(State(state): State<OpenEOState>) -> impl IntoResponse {
    let cat = state.app.catalog.lock().await;
    match cat.list_collections() {
        Ok(collections) => {
            let cols: Vec<serde_json::Value> = collections
                .iter()
                .map(|c| {
                    serde_json::json!({
                        "stac_version": "1.0.0",
                        "id": c.id,
                        "description": c.description,
                        "license": "proprietary",
                        "extent": {
                            "spatial": {"bbox": [[-180, -90, 180, 90]]},
                            "temporal": {"interval": [[null, null]]}
                        },
                        "links": []
                    })
                })
                .collect();
            (StatusCode::OK, Json(serde_json::json!({"collections": cols, "links": []})))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("{}", e)})),
        ),
    }
}

async fn openeo_collection(
    Path(id): Path<String>,
    State(state): State<OpenEOState>,
) -> impl IntoResponse {
    let cat = state.app.catalog.lock().await;
    match cat.get_collection(&id) {
        Ok(Some(c)) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "stac_version": "1.0.0",
                "id": c.id,
                "description": c.description,
                "license": "proprietary",
                "extent": {
                    "spatial": {"bbox": [[-180, -90, 180, 90]]},
                    "temporal": {"interval": [[null, null]]}
                },
                "links": []
            })),
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"code": "CollectionNotFound", "message": format!("Collection '{}' not found", id)})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("{}", e)})),
        ),
    }
}

async fn openeo_processes() -> Json<serde_json::Value> {
    Json(super::catalogue::process_catalogue())
}

async fn openeo_result(
    headers: HeaderMap,
    State(state): State<OpenEOState>,
    Json(graph): Json<ProcessGraph>,
) -> impl IntoResponse {
    // Auth check
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .or_else(|| headers.get("x-api-key").and_then(|v| v.to_str().ok()));

    if state.app.auth.check_write(token).is_err() {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"code": "AuthenticationRequired", "message": "API key required"})),
        ).into_response();
    }

    match execute_sync(&graph, &state.app.catalog, &state.app.store).await {
        Ok((data, meta)) => {
            let fmt = extract_output_format(&graph).unwrap_or_else(|| "GTiff".to_string());
            if let Some(ref m) = meta {
                match wrap_output(&data, m, &fmt) {
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

// ---------------------------------------------------------------------------
// Jobs API
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct CreateJobRequest {
    pub process: ProcessGraph,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

fn now_epoch() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

async fn create_job(
    headers: HeaderMap,
    State(state): State<OpenEOState>,
    Json(req): Json<CreateJobRequest>,
) -> impl IntoResponse {
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .or_else(|| headers.get("x-api-key").and_then(|v| v.to_str().ok()));

    if state.app.auth.check_write(token).is_err() {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"code": "AuthenticationRequired", "message": "API key required"})),
        );
    }

    let job_id = uuid::Uuid::new_v4().to_string();
    let now = now_epoch();

    let job = JobResult {
        job_id: job_id.clone(),
        status: "created".to_string(),
        data: None,
        errors: Vec::new(),
        created: now,
        updated: now,
    };

    // Store job
    state.jobs.lock().await.insert(job_id.clone(), job);

    // Spawn async execution
    let jobs = state.jobs.clone();
    let catalog = state.app.catalog.clone();
    let store = state.app.store.clone();
    let graph = req.process;
    let jid = job_id.clone();

    tokio::spawn(async move {
        // Update to running
        if let Some(j) = jobs.lock().await.get_mut(&jid) {
            j.status = "running".to_string();
            j.updated = now_epoch();
        }

        match execute_sync(&graph, &catalog, &store).await {
            Ok((data, _meta)) => {
                if let Some(j) = jobs.lock().await.get_mut(&jid) {
                    j.status = "finished".to_string();
                    j.data = Some(data);
                    j.updated = now_epoch();
                }
            }
            Err(e) => {
                if let Some(j) = jobs.lock().await.get_mut(&jid) {
                    j.status = "error".to_string();
                    j.errors.push(e);
                    j.updated = now_epoch();
                }
            }
        }
    });

    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "id": job_id,
            "status": "created",
            "created": now
        })),
    )
}

async fn list_jobs(State(state): State<OpenEOState>) -> Json<serde_json::Value> {
    let jobs = state.jobs.lock().await;
    let job_list: Vec<serde_json::Value> = jobs
        .values()
        .map(|j| {
            serde_json::json!({
                "id": j.job_id,
                "status": j.status,
                "created": j.created,
                "updated": j.updated
            })
        })
        .collect();
    Json(serde_json::json!({"jobs": job_list, "links": []}))
}

async fn get_job(
    Path(job_id): Path<String>,
    State(state): State<OpenEOState>,
) -> impl IntoResponse {
    let jobs = state.jobs.lock().await;
    match jobs.get(&job_id) {
        Some(j) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "id": j.job_id,
                "status": j.status,
                "created": j.created,
                "updated": j.updated,
                "errors": j.errors
            })),
        ),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"code": "JobNotFound", "message": format!("Job '{}' not found", job_id)})),
        ),
    }
}

async fn get_job_results(
    Path(job_id): Path<String>,
    State(state): State<OpenEOState>,
) -> impl IntoResponse {
    let jobs = state.jobs.lock().await;
    match jobs.get(&job_id) {
        Some(j) if j.status == "finished" => {
            if let Some(data) = &j.data {
                let headers = [
                    ("content-type", "application/octet-stream"),
                    ("content-disposition", "attachment; filename=\"result.tif\""),
                ];
                (StatusCode::OK, (headers, data.clone()).into_response())
            } else {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": "Job finished but no data"})).into_response(),
                )
            }
        }
        Some(j) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "JobNotFinished",
                "message": format!("Job status: {}", j.status)
            }))
            .into_response(),
        ),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"code": "JobNotFound", "message": format!("Job '{}' not found", job_id)}))
                .into_response(),
        ),
    }
}

async fn get_job_logs(
    Path(job_id): Path<String>,
    State(state): State<OpenEOState>,
) -> impl IntoResponse {
    let jobs = state.jobs.lock().await;
    match jobs.get(&job_id) {
        Some(j) => {
            let logs: Vec<serde_json::Value> = j
                .errors
                .iter()
                .enumerate()
                .map(|(i, e)| {
                    serde_json::json!({
                        "id": format!("{}", i),
                        "level": if j.status == "error" { "error" } else { "info" },
                        "message": e
                    })
                })
                .collect();
            (StatusCode::OK, Json(serde_json::json!({"logs": logs, "links": []})))
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"code": "JobNotFound", "message": format!("Job '{}' not found", job_id)})),
        ),
    }
}

async fn delete_job(
    Path(job_id): Path<String>,
    State(state): State<OpenEOState>,
) -> impl IntoResponse {
    let mut jobs = state.jobs.lock().await;
    if jobs.remove(&job_id).is_some() {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

/// POST /validate — validate an openEO process graph.
///
/// Returns a list of validation errors (empty array = valid).
async fn validate_process_graph(
    State(_state): State<OpenEOState>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    // Extract the process graph from the request body.
    // Accept both {"process_graph": {...}} and {"process": {"process_graph": {...}}}
    let pg = body.get("process_graph")
        .or_else(|| body.get("process").and_then(|p| p.get("process_graph")));

    let mut errors: Vec<serde_json::Value> = Vec::new();

    match pg {
        None => {
            errors.push(serde_json::json!({
                "code": "ProcessGraphMissing",
                "message": "No process_graph found in request body."
            }));
        }
        Some(pg_val) => {
            if let Some(nodes) = pg_val.as_object() {
                // Validate each node: check process_id is known
                let known_processes = [
                    "load_collection", "save_result", "ndvi", "ndwi", "evi", "cloud_mask",
                    "aggregate_temporal_period", "resample_spatial",
                    "reduce_dimension", "apply", "normalized_difference", "array_element",
                    "multiply", "add", "subtract", "divide",
                    "filter_temporal", "filter_bbox", "filter_bands", "mean", "min", "max", "median",
                ];
                for (node_id, node) in nodes {
                    let process_id = node.get("process_id").and_then(|v| v.as_str()).unwrap_or("");
                    if process_id.is_empty() {
                        errors.push(serde_json::json!({
                            "code": "ProcessIdMissing",
                            "message": format!("Node '{}' has no process_id.", node_id)
                        }));
                    } else if !known_processes.contains(&process_id) {
                        // Unknown process — warn but don't fail (backend may support more)
                        errors.push(serde_json::json!({
                            "code": "ProcessUnsupported",
                            "message": format!("Process '{}' is not supported by this backend.", process_id),
                            "level": "warning"
                        }));
                    }
                }
            } else {
                errors.push(serde_json::json!({
                    "code": "ProcessGraphInvalid",
                    "message": "process_graph must be an object of process nodes."
                }));
            }
        }
    }

    (StatusCode::OK, Json(serde_json::json!({"errors": errors})))
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn openeo_router(state: OpenEOState) -> Router {
    Router::new()
        .route("/", get(openeo_capabilities))
        .route("/.well-known/openeo", get(well_known_openeo))
        .route("/credentials/basic", get(credentials_basic))
        .route("/me", get(me_handler))
        .route("/collections", get(openeo_collections))
        .route("/collections/{id}", get(openeo_collection))
        .route("/processes", get(openeo_processes))
        .route("/result", post(openeo_result))
        .route("/jobs", get(list_jobs).post(create_job))
        .route("/jobs/{job_id}", get(get_job).delete(delete_job))
        .route("/jobs/{job_id}/results", get(get_job_results))
        .route("/jobs/{job_id}/logs", get(get_job_logs))
        .route("/validate", post(validate_process_graph))
        .with_state(state)
}
