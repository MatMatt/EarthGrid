use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;

use crate::server::{AppState, err};
use crate::catalog::{DatetimeFilter, StacItem};


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


/// GET / — Content-negotiated landing: HTML for browsers, JSON for API clients
pub(crate) async fn stac_landing(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    // Only beacon nodes serve the HTML info page; regular nodes always return STAC JSON
    let accept = headers.get("accept").and_then(|v| v.to_str().ok()).unwrap_or("");
    if state.is_beacon && accept.contains("text/html") && !accept.starts_with("application/json") {
        return crate::routes::misc::landing_html().await.into_response();
    }
    stac_landing_json(State(state)).await.into_response()
}


/// JSON STAC Landing
pub(crate) async fn stac_landing_json(State(state): State<AppState>) -> Json<serde_json::Value> {
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
pub(crate) async fn stac_conformance() -> Json<serde_json::Value> {
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


pub(crate) async fn list_collections(State(state): State<AppState>) -> impl IntoResponse {
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


pub(crate) async fn get_collection(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    let catalog = state.catalog.lock().await;
    match catalog.get_collection(&id) {
        Ok(Some(col)) => (StatusCode::OK, Json(serde_json::to_value(col).unwrap())).into_response(),
        Ok(None) => err(StatusCode::NOT_FOUND, "Collection not found").into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}


pub(crate) async fn collection_items(
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
pub(crate) async fn get_collection_item(
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


pub(crate) fn parse_bbox_str(s: &str) -> Option<[f64; 4]> {
    let parts: Vec<f64> = s.split(',').filter_map(|p| p.trim().parse().ok()).collect();
    if parts.len() == 4 { Some([parts[0], parts[1], parts[2], parts[3]]) } else { None }
}


pub(crate) fn build_search_response(
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


pub(crate) async fn stac_search(State(state): State<AppState>, Query(q): Query<SearchQuery>) -> impl IntoResponse {
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
pub(crate) async fn stac_search_post(
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

