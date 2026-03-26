use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};

use crate::server::{AppState, api_key, err, check_admin_or_session, check_write_or_session, LimitQuery};


// ---------------------------------------------------------------------------
// Admin handlers — GET /admin/stats, GET /admin/activity, PATCH /admin/node/name
// ---------------------------------------------------------------------------

pub(crate) async fn admin_stats(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(e) = check_admin_or_session(&state.auth, &headers) {
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


pub(crate) async fn admin_activity(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<LimitQuery>,
) -> impl IntoResponse {
    if let Err(e) = check_write_or_session(&state.auth, &headers) {
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


pub(crate) async fn patch_node_name(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<NodeNamePatch>,
) -> impl IntoResponse {
    let key = api_key(&headers);
    if let Err(e) = state.auth.check_admin(key) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }

    let new_name = body.name.trim().to_string();
    if new_name.is_empty() || new_name.len() > 64 {
        return err(StatusCode::BAD_REQUEST, "Name must be 1-64 chars").into_response();
    }

    let old_name = state.node_name.clone();

    // Persist to config.json
    let cfg_path = dirs::home_dir().unwrap_or_default().join(".earthgrid/config.json");
    if let Ok(content) = std::fs::read_to_string(&cfg_path) {
        if let Ok(mut cfg) = serde_json::from_str::<serde_json::Value>(&content) {
            cfg["node_name"] = serde_json::Value::String(new_name.clone());
            if let Ok(serialized) = serde_json::to_string_pretty(&cfg) {
                let _ = std::fs::write(&cfg_path, serialized);
            }
        }
    }

    (StatusCode::OK, Json(serde_json::json!({
        "status": "renamed",
        "old": old_name,
        "new": new_name,
    }))).into_response()
}


// ---------------------------------------------------------------------------
// Admin: Delete collection
// ---------------------------------------------------------------------------

pub(crate) async fn admin_delete_collection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(collection_id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = check_admin_or_session(&state.auth, &headers) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }
    let catalog = state.catalog.lock().await;
    // Check collection exists
    match catalog.get_collection(&collection_id) {
        Ok(None) => return err(StatusCode::NOT_FOUND, "Collection not found").into_response(),
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
        Ok(Some(_)) => {}
    }
    // Get chunk hashes for all items in this collection before deleting
    let items = catalog.search(Some(&collection_id), None, None, 1_000_000, 0).unwrap_or_default();
    let item_count = items.len();
    let chunk_hashes: Vec<String> = items.iter()
        .flat_map(|i| i.chunk_hashes.iter().cloned())
        .collect();
    // Delete all items + collection in one call
    if let Err(e) = catalog.delete_collection(&collection_id) {
        return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response();
    }
    drop(catalog);

    // Attempt to remove chunks from store (best-effort)
    let removed_chunks = {
        let mut store = state.store.lock().await;
        let mut count = 0usize;
        for hash in &chunk_hashes {
            if store.delete(hash).unwrap_or(false) {
                count += 1;
            }
        }
        count
    };

    state.audit.log("collection_delete", &collection_id, "", true);
    tracing::info!("Deleted collection {} ({} items, {} chunks removed)", collection_id, item_count, removed_chunks);
    (StatusCode::OK, Json(serde_json::json!({
        "status": "deleted",
        "collection": collection_id,
        "items_removed": item_count,
        "chunks_removed": removed_chunks,
    }))).into_response()
}


// ---------------------------------------------------------------------------
// Admin: User management
// ---------------------------------------------------------------------------

pub(crate) async fn admin_list_users(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(e) = check_admin_or_session(&state.auth, &headers) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }
    match &state.user_auth {
        Some(ua) => {
            match ua.list_users() {
                Ok(users) => {
                    let count = users.len();
                    (StatusCode::OK, Json(serde_json::json!({"users": users, "count": count}))).into_response()
                }
                Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
            }
        }
        None => err(StatusCode::SERVICE_UNAVAILABLE, "User auth not initialized").into_response(),
    }
}


#[derive(serde::Deserialize)]
pub struct CreateUserBody {
    pub username: String,
    pub role: Option<String>,
}


pub(crate) async fn admin_create_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateUserBody>,
) -> impl IntoResponse {
    if let Err(e) = check_admin_or_session(&state.auth, &headers) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }
    if body.username.is_empty() {
        return err(StatusCode::BAD_REQUEST, "Missing username").into_response();
    }
    match &state.user_auth {
        Some(ua) => {
            let role = body.role.as_deref().unwrap_or("member");
            match ua.add_user(&body.username, role) {
                Ok(api_key_val) => {
                    state.audit.log("user_create", &body.username, "", true);
                    (StatusCode::CREATED, Json(serde_json::json!({
                        "status": "created",
                        "username": body.username,
                        "role": role,
                        "api_key": api_key_val,
                    }))).into_response()
                }
                Err(e) => err(StatusCode::CONFLICT, &e.to_string()).into_response(),
            }
        }
        None => err(StatusCode::SERVICE_UNAVAILABLE, "User auth not initialized").into_response(),
    }
}


pub(crate) async fn admin_delete_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(user_id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = check_admin_or_session(&state.auth, &headers) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }
    match &state.user_auth {
        Some(ua) => {
            match ua.revoke_user(&user_id) {
                Ok(true) => {
                    state.audit.log("user_delete", &user_id, "", true);
                    (StatusCode::OK, Json(serde_json::json!({
                        "status": "deactivated",
                        "user_id": user_id,
                    }))).into_response()
                }
                Ok(false) => err(StatusCode::NOT_FOUND, "User not found").into_response(),
                Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
            }
        }
        None => err(StatusCode::SERVICE_UNAVAILABLE, "User auth not initialized").into_response(),
    }
}


// ---------------------------------------------------------------------------
// PATCH /node-name — alias for admin/node/name
// ---------------------------------------------------------------------------

pub(crate) async fn patch_node_name_alias(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<NodeNamePatch>,
) -> impl IntoResponse {
    patch_node_name(State(state), headers, Json(body)).await
}


// ---------------------------------------------------------------------------
// DELETE /nodes/{node_id} — remove from beacon
// ---------------------------------------------------------------------------

pub(crate) async fn delete_node(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = check_admin_or_session(&state.auth, &headers) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }
    // Remove from peer registry
    {
        let mut peers = state.peers.lock().await;
        peers.remove_by_id(&node_id);
    }
    state.audit.log("node_delete", &node_id, "", true);
    (StatusCode::OK, Json(serde_json::json!({
        "status": "removed",
        "node_id": node_id,
    }))).into_response()
}

