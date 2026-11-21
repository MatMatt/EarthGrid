use axum::{
    extract::{ConnectInfo, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use std::net::SocketAddr;

use crate::auth::AccessLevel;
use crate::server::{AppState, api_key, err, authorize, LimitQuery};


// ---------------------------------------------------------------------------
// Admin handlers — GET /admin/stats, GET /admin/activity, PATCH /admin/node/name
// ---------------------------------------------------------------------------

pub(crate) async fn admin_stats(
    State(state): State<AppState>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    if let Err(e) = authorize(&state.auth, state.user_auth.as_deref(), &headers, addr, AccessLevel::Admin, &state.data_dir) {
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
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Query(q): Query<LimitQuery>,
) -> impl IntoResponse {
    if let Err(e) = authorize(&state.auth, state.user_auth.as_deref(), &headers, addr, AccessLevel::Admin, &state.data_dir) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }

    let limit = q.limit.unwrap_or(50).min(500);
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
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Path(collection_id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = authorize(&state.auth, state.user_auth.as_deref(), &headers, addr, AccessLevel::Admin, &state.data_dir) {
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
    // Chunks are content addressed and shared between collections. Count, in
    // one pass, how many items OUTSIDE this collection still reference each
    // hash; a chunk with any such reference must survive the delete below.
    let other_refs = match catalog.chunk_reference_counts(Some(&collection_id)) {
        Ok(m) => m,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    };
    if !other_refs.complete {
        tracing::warn!(
            "Deleting collection {}: chunk references are incomplete (some catalogue rows could not be read); no chunk will be deleted unless proven unreferenced, so nothing unproven will be removed",
            collection_id
        );
    }
    // Delete all items + collection in one call
    if let Err(e) = catalog.delete_collection(&collection_id) {
        return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response();
    }
    drop(catalog);

    // Attempt to remove chunks from store (best-effort), keeping shared ones
    let removed_chunks = {
        let mut store = state.store.lock().await;
        let mut count = 0usize;
        for hash in &chunk_hashes {
            if other_refs.counts.get(hash).copied().unwrap_or(0) > 0 {
                continue; // still referenced by another collection — keep it
            }
            if !other_refs.complete {
                continue; // references incomplete: a missing count is not proof — keep it
            }
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
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    if let Err(e) = authorize(&state.auth, state.user_auth.as_deref(), &headers, addr, AccessLevel::Admin, &state.data_dir) {
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
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(body): Json<CreateUserBody>,
) -> impl IntoResponse {
    if let Err(e) = authorize(&state.auth, state.user_auth.as_deref(), &headers, addr, AccessLevel::Admin, &state.data_dir) {
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
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Path(user_id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = authorize(&state.auth, state.user_auth.as_deref(), &headers, addr, AccessLevel::Admin, &state.data_dir) {
        return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
    }
    match &state.user_auth {
        Some(ua) => {
            // Path parameter may be a user_id or a username; revoke_user() matches on username
            let users = match ua.list_users() {
                Ok(users) => users,
                Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
            };
            let user = match users.iter().find(|u| u.user_id == user_id)
                .or_else(|| users.iter().find(|u| u.username == user_id))
            {
                Some(u) => u,
                None => return err(StatusCode::NOT_FOUND, "User not found").into_response(),
            };
            match ua.revoke_user(&user.username) {
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
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Path(node_id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = authorize(&state.auth, state.user_auth.as_deref(), &headers, addr, AccessLevel::Admin, &state.data_dir) {
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


#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    fn test_state(dir: &std::path::Path) -> AppState {
        AppState {
            store: Arc::new(Mutex::new(crate::chunk_store::ChunkStore::new(&dir.join("store"), 0.0).unwrap())),
            catalog: Arc::new(Mutex::new(crate::catalog::Catalog::new(&dir.join("catalog.db")).unwrap())),
            audit: Arc::new(crate::audit::AuditLog::new(&dir.join("audit.jsonl"))),
            auth: crate::auth::AuthConfig { api_key: String::new(), admin_key: String::new() },
            peers: Arc::new(Mutex::new(crate::peers::PeerRegistry::new())),
            stats: Arc::new(crate::stats::StatsEngine::new(&dir.join("stats.db")).unwrap()),
            perf: Arc::new(crate::perf::PerfRecorder::new()),
            gamification: Arc::new(crate::gamification::GamificationEngine::new(&dir.join("gamification.db")).unwrap()),
            version: String::new(),
            node_id: String::new(),
            node_name: String::new(),
            user_auth: None,
            node_identity: None,
            storage_limit_gb: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            data_dir: dir.to_path_buf(),
            active_requests: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            is_beacon: false,
            ui_enabled: true,
            tile_grid: Arc::new(std::collections::HashMap::new()),
            fetch_queue: Arc::new(crate::fetch_queue::FetchQueueBackend::Local(
                crate::fetch_queue::LocalFetchQueue::new(&dir.join("fetch_queue.db")).unwrap(),
            )),
        }
    }

    fn item(id: &str, collection: &str, hashes: Vec<String>) -> crate::catalog::StacItem {
        crate::catalog::StacItem {
            id: id.to_string(),
            collection: collection.to_string(),
            bbox: [0.0, 0.0, 1.0, 1.0],
            properties: serde_json::json!({}),
            chunk_hashes: hashes,
            created_at: 0.0,
            geometry: None,
        }
    }

    /// Deleting a collection must only delete the chunks no other collection
    /// references: the shared chunk stays and the other collection's item
    /// still reconstructs, while the unshared chunk is removed.
    #[tokio::test]
    async fn delete_collection_keeps_chunks_shared_with_another_collection() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());

        let (shared, only_a) = {
            let mut store = state.store.lock().await;
            (
                store.put(b"shared between two collections").unwrap(),
                store.put(b"only in collection a").unwrap(),
            )
        };
        let b = item("b-1", "coll-b", vec![shared.clone()]);
        {
            let catalog = state.catalog.lock().await;
            for id in ["coll-a", "coll-b"] {
                catalog.add_collection(&crate::catalog::StacCollection {
                    id: id.to_string(),
                    title: id.to_string(),
                    description: String::new(),
                }).unwrap();
            }
            catalog.add_item(&item("a-1", "coll-a", vec![shared.clone(), only_a.clone()])).unwrap();
            catalog.add_item(&b).unwrap();
        }

        let addr = SocketAddr::from(([127, 0, 0, 1], 1));
        let resp = admin_delete_collection(
            State(state.clone()),
            HeaderMap::new(),
            ConnectInfo(addr),
            Path("coll-a".to_string()),
        )
        .await
        .into_response();
        assert_eq!(resp.status(), StatusCode::OK);

        let mut store = state.store.lock().await;
        assert!(!store.has(&only_a), "unshared chunk of the deleted collection must be removed");
        assert!(store.has(&shared), "chunk still referenced by coll-b must be kept");

        let catalog = state.catalog.lock().await;
        assert!(catalog.get_collection("coll-a").unwrap().is_none());
        let b_again = catalog.get_item(&b.id).unwrap().expect("item in coll-b must survive");
        let bytes = crate::reconstruct::reconstruct_raw(&b_again, &mut *store)
            .expect("item in coll-b must still reconstruct after deleting coll-a");
        assert_eq!(bytes, b"shared between two collections");
    }
}

