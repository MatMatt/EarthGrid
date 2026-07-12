use axum::http::HeaderMap;
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

/// GET /.well-known/openeo — openEO version discovery
pub(crate) async fn well_known_openeo(headers: HeaderMap) -> Json<serde_json::Value> {
    let host = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost:8400");
    let proto = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("http");
    let base_url = format!("{proto}://{host}/");

    Json(serde_json::json!({
        "versions": [{
            "url": base_url,
            "api_version": "1.2.0",
            "production": false
        }]
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
        Ok(Some(col)) => serde_json::to_value(col)
            .map(|v| (StatusCode::OK, Json(v)).into_response())
            .unwrap_or_else(|e| err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response()),
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
        Ok(Some(item)) => serde_json::to_value(item)
            .map(|v| (StatusCode::OK, Json(v)).into_response())
            .unwrap_or_else(|e| err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response()),
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
        Ok(items) if !items.is_empty() || offset > 0 => {
            let body = build_search_response(items, total, limit, offset, "/stac/search");
            (StatusCode::OK, Json(body)).into_response()
        }
        Ok(_empty) => {
            // No local results at offset 0 — try alive peers
            drop(catalog);
            let peer_urls: Vec<String> = {
                let registry = state.peers.lock().await;
                registry.list().into_iter()
                    .filter(|p| p.alive())
                    .map(|p| p.url.clone())
                    .collect()
            };
            if peer_urls.is_empty() {
                let body = build_search_response(vec![], 0, limit, 0, "/stac/search");
                return (StatusCode::OK, Json(body)).into_response();
            }

            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .unwrap_or_default();

            let mut all_features: Vec<serde_json::Value> = Vec::new();
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
                    let resp = client.get(format!("{}/api/stac/search", url))
                        .query(&params).send().await;
                    match resp {
                        Ok(r) if r.status().is_success() => {
                            r.json::<serde_json::Value>().await.ok()
                                .and_then(|d| d.get("features").and_then(|f| f.as_array().cloned()))
                                .unwrap_or_default()
                                .into_iter()
                                .map(|mut f| {
                                    if let Some(obj) = f.as_object_mut() {
                                        obj.insert("earthgrid:source_node".to_string(), serde_json::json!(url));
                                    }
                                    f
                                }).collect::<Vec<_>>()
                        }
                        _ => vec![],
                    }
                }));
            }
            for h in handles {
                if let Ok(items) = h.await { all_features.extend(items); }
            }
            // Deduplicate by id
            let mut seen = std::collections::HashSet::new();
            let deduped: Vec<_> = all_features.into_iter()
                .filter(|i| {
                    let id = i.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    if id.is_empty() { return true; }
                    seen.insert(id)
                })
                .take(limit)
                .collect();
            let count = deduped.len();
            (StatusCode::OK, Json(serde_json::json!({
                "type": "FeatureCollection",
                "numberMatched": count,
                "numberReturned": count,
                "features": deduped,
                "context": {"source": "peer_fallback"},
            }))).into_response()
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

