use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};

use crate::server::{AppState, api_key, err};


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


pub(crate) async fn process_job(
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


pub(crate) async fn process_operations() -> Json<serde_json::Value> {
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

