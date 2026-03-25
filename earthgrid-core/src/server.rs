//! Axum HTTP server for EarthGrid Core.
//!
//! Phase 1: Core STAC/chunk API
//! Phase 2: Peers + Federation (sync, federated search)

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::{
    Router,
    http::{HeaderMap, StatusCode},
    routing::{delete, get, patch, post},
    Json,
};
use serde::Deserialize;
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;

use crate::{
    audit::AuditLog,
    auth::AuthConfig,
    beacon::{BeaconRegistry, BeaconState, beacon_router},
    beacon_federation::{FederationState, spawn_peer_connections},
    catalog::Catalog,
    chunk_store::ChunkStore,
    gamification::GamificationEngine,
    peers::{GossipPeerList, NodeInfo, PeerRegistry},
    replication::Replicator,
    stats::StatsEngine,
    user_auth::UserAuth,
    node_identity::NodeIdentity,
};
use std::path::PathBuf;


// ---------------------------------------------------------------------------
// Shared State
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Mutex<ChunkStore>>,
    pub catalog: Arc<Mutex<Catalog>>,
    pub audit: Arc<AuditLog>,
    pub auth: AuthConfig,
    pub peers: Arc<Mutex<PeerRegistry>>,
    pub stats: Arc<StatsEngine>,
    pub gamification: Arc<GamificationEngine>,
    pub version: String,
    pub node_id: String,
    pub node_name: String,
    /// User authentication registry (optional, None if init fails).
    pub user_auth: Option<Arc<UserAuth>>,
    /// Node identity keypair (optional, None if init fails).
    pub node_identity: Option<Arc<NodeIdentity>>,
    pub storage_limit_gb: Arc<std::sync::atomic::AtomicU64>,
    /// Data directory (for config updates like resize).
    pub data_dir: PathBuf,
    /// Counter for active fetch/ingest requests (replication yields when > 0).
    pub active_requests: Arc<AtomicUsize>,
    /// Whether this node runs as beacon (shows grid-wide landing page).
    pub is_beacon: bool,
    /// Whether the web UI is enabled (default: true).
    pub ui_enabled: bool,
}


/// RAII guard that decrements an AtomicUsize counter on drop.
pub(crate) struct ActiveRequestGuard(pub(crate) Arc<AtomicUsize>);
impl Drop for ActiveRequestGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}
// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub(crate) fn api_key(headers: &HeaderMap) -> Option<&str> {
    headers.get("x-api-key").and_then(|v| v.to_str().ok())
}

/// Check if the request originates from localhost using ConnectInfo.
/// Requires `into_make_service_with_connect_info::<SocketAddr>()`.
pub(crate) fn is_localhost(headers: &HeaderMap) -> bool {
    // If X-Forwarded-For is present, we're behind a proxy — don't trust localhost
    if headers.get("x-forwarded-for").is_some() {
        return false;
    }
    // Without a proxy, absence of X-Forwarded-For on a direct connection means localhost
    // (The ratelimiter also uses this pattern for LAN detection)
    true
}

/// Extract user role from session cookie. Returns (username, role) if valid.
pub(crate) fn session_user(headers: &HeaderMap) -> Option<(String, String)> {
    let cookie_header = headers.get("cookie")?.to_str().ok()?;
    let token = crate::session::extract_cookie(cookie_header)?;
    let secret = crate::session::session_secret();
    crate::session::validate_token(&token, &secret)
}

/// Check admin access: localhost bypasses, then API key, then session cookie.
pub(crate) fn check_admin_or_session(auth: &crate::auth::AuthConfig, headers: &HeaderMap) -> Result<(), crate::error::EarthGridError> {
    if is_localhost(headers) {
        return Ok(());
    }
    // Try API key first
    if let Some(key) = api_key(headers) {
        return auth.check_admin(Some(key));
    }
    // Fall back to session cookie
    if let Some((_username, role)) = session_user(headers) {
        if role == "admin" {
            return Ok(());
        }
    }
    Err(crate::error::EarthGridError::AuthRequired)
}

/// Check write access: localhost bypasses, then API key, then session cookie.
pub(crate) fn check_write_or_session(auth: &crate::auth::AuthConfig, headers: &HeaderMap) -> Result<(), crate::error::EarthGridError> {
    if is_localhost(headers) {
        return Ok(());
    }
    // Try API key first
    if let Some(key) = api_key(headers) {
        return auth.check_write(Some(key));
    }
    // Fall back to session cookie
    if let Some((_username, role)) = session_user(headers) {
        if role == "admin" || role == "user" || role == "member" {
            return Ok(());
        }
    }
    Err(crate::error::EarthGridError::AuthRequired)
}

pub(crate) fn err(status: StatusCode, msg: &str) -> (StatusCode, Json<serde_json::Value>) {
    (status, Json(serde_json::json!({"error": msg})))
}

#[derive(Deserialize)]
pub struct LimitQuery {
    pub limit: Option<usize>,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn router(state: AppState) -> Router {
    Router::new()
        // Core
        .route("/api/health", get(crate::routes::misc::health))
        .route("/api/node-info", get(crate::routes::misc::node_info))
        .route("/api/stats", get(crate::routes::stats::stats))
        // STAC Landing + Conformance
        .route("/", get(|| async {
            axum::response::Html(r#"<!DOCTYPE html><html><head><script>window.location.replace(window.location.href.replace(/\/$/, '') + '/dashboard')</script></head><body>Redirecting to <a href="dashboard">dashboard</a>...</body></html>"#)
        }))
        .route("/dashboard", get(|| async { axum::response::Html(include_str!("../assets/beacon.html")) }))
        // STAC spec backward-compat aliases (root level)
        .route("/collections", get(crate::routes::stac::list_collections))
        .route("/collections/{id}", get(crate::routes::stac::get_collection))
        .route("/conformance", get(crate::routes::stac::stac_conformance))
        .route("/.well-known/openeo", get(crate::routes::stac::well_known_openeo))
        .route("/health", get(crate::routes::misc::health))
        .route("/api/.well-known/openeo", get(crate::routes::stac::well_known_openeo))
        .route("/api/conformance", get(crate::routes::stac::stac_conformance))
        // STAC Collections + Items
        .route("/api/stac/collections", get(crate::routes::stac::list_collections))
        .route("/api/stac/collections/{id}", get(crate::routes::stac::get_collection))
        .route("/api/stac/collections/{id}/items", get(crate::routes::stac::collection_items))
        .route("/api/stac/collections/{id}/items/{item_id}", get(crate::routes::stac::get_collection_item))
        // STAC Search (GET + POST)
        .route("/api/stac/search", get(crate::routes::stac::stac_search).post(crate::routes::stac::stac_search_post))
        // Chunks
        .route("/api/chunks", get(crate::routes::chunks::list_chunks))
        .route("/api/chunks/{sha}", get(crate::routes::chunks::get_chunk))
        // Write
        // Integrity
        .route("/api/verify/{item_id}", get(crate::routes::chunks::verify_item))
        // Admin
        .route("/api/audit", get(crate::routes::misc::audit_log))
        // Federation (Phase 2)
        .route("/api/peers", get(crate::routes::federation::list_peers))
        .route("/api/peers", post(crate::routes::federation::register_peer))
        .route("/api/federation/sync", post(crate::routes::federation::federation_sync))
        .route("/api/federation/search", get(crate::routes::federation::federation_search))
        // Gossip + file ingest
        .route("/api/peers.json", get(crate::routes::federation::peers_json))
        // Element84 STAC Fetcher
        .route("/api/fetch", post(crate::routes::ingest_routes::fetch_handler))
        .route("/api/fetch/preview", get(crate::routes::ingest_routes::fetch_preview))
        .route("/api/catalog/changes", get(crate::routes::ingest_routes::catalog_changes))
        // Stats
        .route("/api/stats/downloads", get(crate::routes::stats::stats_downloads))
        .route("/api/stats/hot-chunks", get(crate::routes::stats::stats_hot_chunks))
        .route("/api/stats/replication-advice", get(crate::routes::stats::stats_replication_advice))
        .route("/api/stats/ingest", get(crate::routes::stats::stats_ingest_history))
        // Replication
        .route("/api/replicate", post(crate::routes::chunks::replicate))
        // Gamification
        .route("/api/gamification/leaderboard", get(crate::routes::gamification_routes::gamification_leaderboard))
        .route("/api/gamification/node/{id}", get(crate::routes::gamification_routes::gamification_node_profile))
        .route("/api/gamification/feed", get(crate::routes::gamification_routes::gamification_feed))
        .route("/api/gamification/stats", get(crate::routes::gamification_routes::gamification_stats))
        .route("/api/gamification/economy", get(crate::routes::gamification_routes::gamification_economy))
        .route("/api/gamification/challenges", get(crate::routes::gamification_routes::gamification_challenges))
        .route("/api/gamification/challenges/{id}", get(crate::routes::gamification_routes::gamification_challenge_results))
        // Download (reconstruct + serve COG)
        .route("/api/download/{collection_id}/{item_id}", get(crate::routes::misc::download_item))
        // Processing
        .route("/api/process", post(crate::routes::process::process_job))
        .route("/api/process/operations", get(crate::routes::process::process_operations))
        // Sync
        .route("/api/sync", post(crate::routes::federation::sync_from_peer))
        .route("/api/sync-item", post(crate::routes::federation::sync_item))
        // Admin
        .route("/api/admin/stats", get(crate::routes::admin::admin_stats))
        .route("/api/admin/activity", get(crate::routes::admin::admin_activity))
        .route("/api/admin/node/name", axum::routing::patch(crate::routes::admin::patch_node_name))
        // Coverage + extended stats
        .route("/api/coverage/spatial", get(crate::routes::stats::coverage_spatial))
        .route("/api/stats/coverage", get(crate::routes::stats::stats_coverage))
        .route("/api/stats/requests", get(crate::routes::stats::stats_requests))
        .route("/api/stats/bandwidth", get(crate::routes::stats::stats_bandwidth))
        .route("/api/stats/replication", get(crate::routes::stats::stats_replication_status))
        .route("/api/bandwidth", get(crate::routes::stats::bandwidth_handler))
        // Nodes list + delete
        .route("/api/nodes", get(crate::routes::misc::list_nodes))
        .route("/api/nodes/{node_id}", delete(crate::routes::admin::delete_node))
        // Admin: collections
        .route("/api/admin/collections/{collection_id}", delete(crate::routes::admin::admin_delete_collection))
        // Admin: users
        .route("/api/admin/users", get(crate::routes::admin::admin_list_users).post(crate::routes::admin::admin_create_user))
        .route("/api/admin/users/{user_id}", delete(crate::routes::admin::admin_delete_user))
        // PATCH /node-name (alias)
        .route("/api/node-name", patch(crate::routes::admin::patch_node_name_alias))
        // Stats: access + uptake
        .route("/api/stats/access", get(crate::routes::stats::stats_access))
        .route("/api/stats.json", get(crate::routes::stats::stats_json_alias))
        .route("/api/stats/uptake", get(crate::routes::stats::stats_uptake))
        .route("/api/stats/uptake/csv", get(crate::routes::stats::stats_uptake_csv))
        // Replication items list
        .route("/api/replicate/items", get(crate::routes::chunks::replicate_items))
        // Resize storage
        .route("/api/resize", post(crate::routes::chunks::resize_storage))
        // Chunk map
        .route("/api/chunk-map/{collection_id}/{item_id}", get(crate::routes::chunks::chunk_map))
        // Point extraction
        .route("/api/point/{collection_id}/{item_id}", get(crate::routes::chunks::point_extract))
        // Federation: key exchange + user sync
        .route("/api/federation/exchange-key", post(crate::routes::federation::federation_exchange_key))
        .route("/api/federation/users", get(crate::routes::federation::federation_list_users).post(crate::routes::federation::federation_import_users))
        // HTML Node UI (session-authenticated, can be disabled via EARTHGRID_UI_ENABLED=false)
        .route("/ui", get(crate::routes::misc::ui_dispatch))
        .route("/ui/", get(crate::routes::misc::ui_dispatch))
        .route("/ui/login", get(crate::routes::misc::login_dispatch).post(crate::routes::misc::login_post_dispatch))
        .route("/ui/logout", post(crate::routes::misc::logout_dispatch))
        .route("/ui/me", get(crate::routes::misc::session_me_dispatch))
        // openEO compatibility aliases (without /stac/ prefix)
        .route("/api/collections", get(crate::routes::stac::list_collections))
        .route("/api/collections/{id}", get(crate::routes::stac::get_collection))
        // openEO processes + validate + jobs
        .route("/api/processes", get(crate::routes::misc::openeo_processes))
        .route("/api/file_formats", get(crate::routes::misc::openeo_file_formats))
        .route("/api/result", post(crate::routes::misc::openeo_result))
        .route("/api/validate", post(crate::routes::misc::openeo_validate))
        .route("/api/jobs/{job_id}", get(crate::routes::misc::openeo_job_status))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Start server
// ---------------------------------------------------------------------------

pub async fn serve(
    data_dir: std::path::PathBuf,
    host: String,
    port: u16,
    p2p_channels: Option<(
        tokio::sync::mpsc::Receiver<crate::network::NetworkEvent>,
        tokio::sync::mpsc::Sender<crate::network::NetworkCommand>,
    )>,
) -> anyhow::Result<()> {
    use std::env;

    let store_path = data_dir.join("store");
    let catalog_path = data_dir.join("catalog.db");
    let audit_path = data_dir.join("audit.jsonl");
    let stats_db_path = data_dir.join("stats.db");
    let gamification_db_path = data_dir.join("gamification.db");

    let store = ChunkStore::new(&store_path, 0.0)?;
    let catalog = Catalog::new(&catalog_path)?;
    let audit = AuditLog::new(&audit_path);
    let auth = AuthConfig::from_env();
    let stats_engine = StatsEngine::new(&stats_db_path)?;
    let gamification_engine = GamificationEngine::new(&gamification_db_path)?;
    // Seed challenges on startup (no-op if already seeded)
    let _ = gamification_engine.seed_challenges();

    // Node identity from env, file, config.json, or generate+persist
    let earthgrid_home = dirs::home_dir().unwrap_or_default().join(".earthgrid");
    let id_path = earthgrid_home.join(".node_id");
    let data_dir_id_path = data_dir.join(".node_id");
    let node_id = env::var("EARTHGRID_NODE_ID")
        .ok()
        .or_else(|| {
            // Read persistent node_id from ~/.earthgrid/.node_id
            std::fs::read_to_string(&id_path).ok().map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
        })
        .or_else(|| {
            // Fallback: read from data_dir/.node_id
            std::fs::read_to_string(&data_dir_id_path).ok().map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
        })
        .or_else(|| {
            // Fallback: read node_id from config.json (home or data_dir parent)
            let paths = [
                earthgrid_home.join("config.json"),
                data_dir.join("config.json"),
            ];
            for cfg in &paths {
                if let Ok(c) = std::fs::read_to_string(cfg) {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&c) {
                        if let Some(id) = v["node_id"].as_str().map(|s| s.to_string()).filter(|s| !s.is_empty()) {
                            return Some(id);
                        }
                    }
                }
            }
            None
        })
        .unwrap_or_else(|| {
            // Generate new ID and persist to BOTH locations
            let new_id = uuid::Uuid::new_v4().to_string();
            let _ = std::fs::create_dir_all(&earthgrid_home);
            let _ = std::fs::write(&id_path, &new_id);
            let _ = std::fs::write(&data_dir_id_path, &new_id);
            println!("📝 Generated new node ID: {} (saved to {})", new_id, id_path.display());
            new_id
        });
    // Ensure data_dir also has the node_id for consistency
    if !data_dir_id_path.exists() || std::fs::read_to_string(&data_dir_id_path).ok().map(|s| s.trim().to_string()) != Some(node_id.clone()) {
        let _ = std::fs::write(&data_dir_id_path, &node_id);
    }
    let node_name = env::var("EARTHGRID_NODE_NAME")
        .ok()
        .or_else(|| {
            let cfg_path = data_dir.parent()
                .unwrap_or(&data_dir)
                .join("config.json");
            // Also try ~/.earthgrid/config.json
            let paths = [
                cfg_path,
                dirs::home_dir().unwrap_or_default().join(".earthgrid/config.json"),
            ];
            for p in &paths {
                if let Ok(c) = std::fs::read_to_string(p) {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&c) {
                        if let Some(n) = v["node_name"].as_str() {
                            return Some(n.to_string());
                        }
                    }
                }
            }
            None
        })
        .or_else(|| {
            // Last resort: read from data_dir/config.json
            let cfg = data_dir.join("config.json");
            if let Ok(c) = std::fs::read_to_string(&cfg) {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&c) {
                    if let Some(n) = v["node_name"].as_str().filter(|s| !s.is_empty()) {
                        return Some(n.to_string());
                    }
                }
            }
            None
        })
        .unwrap_or_else(|| "earthgrid-node".to_string());

    // Initial peers from env: comma-separated URLs
    let mut peer_registry = PeerRegistry::new();
    // Load peers from both env vars
    for var in ["EARTHGRID_PEERS", "EARTHGRID_BOOTSTRAP_PEERS"] {
        if let Ok(peers_env) = env::var(var) {
            for url in peers_env.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
                peer_registry.add(url, "", "");
            }
        }
    }
    // Load cached peers from disk (persistent peer discovery)
    let peer_cache_path = dirs::home_dir()
        .unwrap_or_default()
        .join(".earthgrid/known_peers.json");
    for url in PeerRegistry::load_cache(&peer_cache_path) {
        peer_registry.add_if_new(&url);
    }
    if peer_registry.count() > 0 {
        println!("📡 Loaded {} peers (env + cache)", peer_registry.count());
    }

    // Optional: user auth DB
    let user_auth_opt = {
        let ua_path = data_dir.join("users.db");
        match UserAuth::new(&ua_path) {
            Ok(ua) => Some(Arc::new(ua)),
            Err(e) => {
                eprintln!("⚠️  UserAuth init failed: {}", e);
                None
            }
        }
    };

    // Optional: node identity
    let node_identity_opt = {
        let key_path = data_dir.join(".node_key");
        match NodeIdentity::load_or_generate(&key_path) {
            Ok(ni) => Some(Arc::new(ni)),
            Err(e) => {
                eprintln!("⚠️  NodeIdentity init failed: {}", e);
                None
            }
        }
    };

    // Read storage_limit_gb from config
    let storage_limit_gb = {
        let cfg_path = dirs::home_dir().unwrap_or_default().join(".earthgrid/config.json");
        std::fs::read_to_string(&cfg_path)
            .ok()
            .and_then(|c| serde_json::from_str::<serde_json::Value>(&c).ok())
            .and_then(|v| v["storage_limit_gb"].as_f64())
            .unwrap_or(0.0)
    };

    let state = AppState {
        store: Arc::new(Mutex::new(store)),
        catalog: Arc::new(Mutex::new(catalog)),
        audit: Arc::new(audit),
        auth,
        peers: Arc::new(Mutex::new(peer_registry)),
        stats: Arc::new(stats_engine),
        gamification: Arc::new(gamification_engine),
        version: env!("CARGO_PKG_VERSION").to_string(),
        node_id,
        node_name: node_name.clone(),
        user_auth: user_auth_opt,
        node_identity: node_identity_opt,
        storage_limit_gb: Arc::new(std::sync::atomic::AtomicU64::new(storage_limit_gb.to_bits())),
        data_dir: data_dir.clone(),
        active_requests: Arc::new(AtomicUsize::new(0)),
        is_beacon: {
            let from_env = env::var("EARTHGRID_BEACON")
                .map(|v| v.to_lowercase() == "true" || v == "1")
                .unwrap_or(false);
            let from_cfg = {
                let cfg_path = dirs::home_dir().unwrap_or_default().join(".earthgrid/config.json");
                std::fs::read_to_string(&cfg_path)
                    .ok()
                    .and_then(|c| serde_json::from_str::<serde_json::Value>(&c).ok())
                    .and_then(|v| v["also_beacon"].as_bool())
                    .unwrap_or(false)
            };
            from_env || from_cfg
        },
        ui_enabled: {
            let from_env = std::env::var("EARTHGRID_UI_ENABLED")
                .map(|v| v.to_lowercase() != "false" && v != "0")
                .unwrap_or(true);
            let from_cfg = {
                let cfg_path = dirs::home_dir().unwrap_or_default().join(".earthgrid/config.json");
                std::fs::read_to_string(&cfg_path)
                    .ok()
                    .and_then(|c| serde_json::from_str::<serde_json::Value>(&c).ok())
                    .and_then(|v| v["ui_enabled"].as_bool())
                    .unwrap_or(true)
            };
            from_env && from_cfg
        },
    };

    // Conditionally build beacon router (EARTHGRID_BEACON=true or also_beacon in config)
    let beacon_enabled = state.is_beacon;

    let hb_peers = state.peers.clone();
    // Clones for P2P handler
    let state_clone_store = state.store.clone();
    let state_clone_catalog = state.catalog.clone();
    let state_active_requests = state.active_requests.clone();
    let state_clone_gamification = state.gamification.clone();
    let state_node_id = state.node_id.clone();
    let state_node_name = state.node_name.clone();
    let state_version = state.version.clone();
    let sync_is_beacon = state.is_beacon;
    let sync_gamification = state.gamification.clone();
    let repl_peers = state.peers.clone();
    // Clones for auto-eviction (must be before router() consumes state)
    let evict_store_c = state.store.clone();
    let evict_catalog_c = state.catalog.clone();
    let evict_limit_c = state.storage_limit_gb.clone();
    let evict_data_dir_c = data_dir.clone();
    let evict_is_beacon_c = beacon_enabled;

    let mut app = router(state);

    // Mount beacon routes if enabled
    if beacon_enabled {
        let beacon_db_path = data_dir.join("beacon.db");
        match BeaconRegistry::new(&beacon_db_path) {
            Ok(registry) => {
                let beacon_id = uuid::Uuid::new_v4().to_string();
                let federation = FederationState::new(beacon_id.clone());
                let beacon_state = BeaconState {
                    registry: Arc::new(Mutex::new(registry)),
                    federation: Some(federation),
                };
                app = app.merge(beacon_router(beacon_state.clone()));
                println!("🔦 Beacon registry enabled ({}) [beacon_id={}]", beacon_db_path.display(), &beacon_id[..8]);

                // Self-registration: register this node in its own beacon DB
                {
                    let self_collections: Vec<String> = {
                        let cat = state_clone_catalog.lock().await;
                        cat.list_collections().unwrap_or_default().into_iter().map(|c| c.id).collect()
                    };
                    let (self_chunks, self_bytes) = {
                        let store = state_clone_store.lock().await;
                        (store.chunk_count(), store.total_bytes())
                    };
                    let self_items = {
                        let cat = state_clone_catalog.lock().await;
                        cat.item_count(None).unwrap_or(0)
                    };
                    let self_url = std::env::var("EARTHGRID_PUBLIC_URL")
                        .unwrap_or_else(|_| format!("http://127.0.0.1:{}", port));
                    let reg = beacon_state.registry.lock().await;
                    let self_req = crate::beacon::RegisterRequest {
                        node_id: state_node_id.clone(),
                        node_name: Some(state_node_name.clone()),
                        url: self_url,
                        collections: Some(self_collections),
                        item_count: Some(self_items as i64),
                        chunk_count: Some(self_chunks as i64),
                        chunks_bytes: Some(self_bytes as i64),
                        can_source: Some(true),
                        storage_limit_gb: Some(storage_limit_gb),
                        sponsor_name: None,
                        sponsor_url: None,
                        node_url: None,
                        group: None,
                        catalog_version: None,
                    };
                    // Try heartbeat first (updates existing), fall back to register (creates new)
                    match reg.heartbeat(&crate::beacon::HeartbeatRequest {
                        node_id: self_req.node_id.clone(),
                        url: Some(self_req.url.clone()),
                        node_name: self_req.node_name.clone(),
                        item_count: self_req.item_count,
                        chunk_count: self_req.chunk_count,
                        chunks_bytes: self_req.chunks_bytes,
                        uptime_seconds: None,
                        collections: self_req.collections.clone(),
                        can_source: self_req.can_source,
                        storage_limit_gb: self_req.storage_limit_gb,
                        catalog_version: self_req.catalog_version.map(|v| v as u64),
                    }) {
                        Ok(Some(node)) => println!("✅ Beacon self-registered: {} ({})", node.node_name, &node.node_id[..8]),
                        Ok(None) => {
                            // Not registered yet, do initial register
                            match reg.register(&self_req) {
                                Ok(node) => println!("✅ Beacon self-registered: {} ({})", node.node_name, &node.node_id[..8]),
                                Err(e) => eprintln!("⚠️  Beacon self-registration failed: {}", e),
                            }
                        }
                        Err(e) => eprintln!("⚠️  Beacon self-heartbeat failed: {}", e),
                    }
                }

                // Federation: connect to peer beacons if configured
                let peer_urls: Vec<String> = std::env::var("EARTHGRID_BEACON_PEERS")
                    .unwrap_or_default()
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                if !peer_urls.is_empty() {
                    println!("🔗 Beacon federation: connecting to {} peer(s)", peer_urls.len());
                    spawn_peer_connections(beacon_state.clone(), peer_urls);
                }
            }
            Err(e) => {
                eprintln!("⚠️  Failed to initialize beacon registry: {}", e);
            }
        }
    }

    let addr = format!("{}:{}", host, port);
    // Sync beacon_nodes -> gamification leaderboard every 60s (beacon only)
    if sync_is_beacon {
        let gami_sync = sync_gamification;
        let beacon_db_path = data_dir.join("beacon.db");
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                if let Ok(conn) = rusqlite::Connection::open(&beacon_db_path) {
                    let mut stmt = match conn.prepare(
                        "SELECT node_id, node_name, item_count, chunks_bytes, storage_limit_gb FROM beacon_nodes"
                    ) {
                        Ok(s) => s,
                        Err(_) => continue,
                    };
                    let rows: Vec<(String, String, i64, i64, f64)> = stmt
                        .query_map([], |r| {
                            Ok((
                                r.get::<_, String>(0)?,
                                r.get::<_, String>(1)?,
                                r.get::<_, i64>(2)?,
                                r.get::<_, i64>(3)?,
                                r.get::<_, f64>(4)?,
                            ))
                        })
                        .into_iter()
                        .flatten()
                        .filter_map(|r| r.ok())
                        .collect();
                    for (node_id, node_name, items, bytes, pledged) in &rows {
                        let _ = gami_sync.sync_node_stats(node_id, node_name, *items, *bytes, *pledged);
                    }
                }
            }
        });
    }

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!(
        "🌍 EarthGrid Core v{} ({}) listening on {}",
        env!("CARGO_PKG_VERSION"),
        node_name,
        addr
    );
    // Spawn heartbeat + gossip loop
    
    tokio::spawn(async move {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        loop {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;

            let urls: Vec<String> = {
                let reg = hb_peers.lock().await;
                reg.urls()
            };

            for url in &urls {
                // 1. Sync node-info
                let info_url = format!("{}/api/node-info", url);
                match client.get(&info_url).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        if let Ok(info) = resp.json::<NodeInfo>().await {
                            let mut reg = hb_peers.lock().await;
                            reg.update_from_info(url, &info);
                        }
                    }
                    _ => {
                        let mut reg = hb_peers.lock().await;
                        reg.record_failure(url);
                    }
                }

                // 2. Gossip: fetch peers from this peer
                let gossip_url = format!("{}/peers.json", url);
                if let Ok(resp) = client.get(&gossip_url).send().await {
                    if resp.status().is_success() {
                        if let Ok(gossip) = resp.json::<GossipPeerList>().await {
                            let mut reg = hb_peers.lock().await;
                            let before = reg.count();
                            for entry in &gossip.peers {
                                reg.add_if_new(&entry.url);
                            }
                            // Save to disk if new peers discovered
                            if reg.count() > before {
                                let cache_path = dirs::home_dir()
                                    .unwrap_or_default()
                                    .join(".earthgrid/known_peers.json");
                                reg.save_cache(&cache_path);
                                tracing::info!("Peer cache updated: {} peers", reg.count());
                            }
                        }
                    }
                }
            }
        }
    });

    // Auto-replication: sync from peers every 5 minutes
    {
        let repl_store = state_clone_store.clone();
        let repl_catalog = state_clone_catalog.clone();
        let repl_active = state_active_requests.clone();

        tokio::spawn(async move {
            // Initial delay: wait 30s for peers to be discovered
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;

            loop {
                let urls: Vec<String> = {
                    let reg = repl_peers.lock().await;
                    reg.urls()
                };

                if !urls.is_empty() {
                    // Yield to active fetch/ingest requests
                    if repl_active.load(Ordering::Relaxed) > 0 {
                        eprintln!("🔄 Auto-replication: skipping cycle — {} active request(s)", repl_active.load(Ordering::Relaxed));
                    } else {
                        let replicator = Replicator::new(repl_store.clone(), repl_catalog.clone());
                        for url in &urls {
                            // Check again before each peer (request may have started)
                            if repl_active.load(Ordering::Relaxed) > 0 {
                                eprintln!("🔄 Auto-replication: pausing mid-cycle — active request(s) detected");
                                break;
                            }
                            let result = replicator.sync_from_peer(url, &[], 0, false).await;
                            if result.chunks_downloaded > 0 || !result.errors.is_empty() {
                                eprintln!(
                                    "🔄 Auto-replicate from {}: {} items, {} chunks ({} bytes), {} errors",
                                    url, result.items_processed, result.chunks_downloaded,
                                    result.bytes_downloaded, result.errors.len()
                                );
                            }
                        }
                    }
                }

                tokio::time::sleep(std::time::Duration::from_secs(300)).await;
            }
        });
    }

    // Auto-eviction: check storage limit every 10 minutes
    {
        let evict_store = evict_store_c.clone();
        let evict_catalog = evict_catalog_c.clone();
        let evict_storage_limit = evict_limit_c.clone();
        let evict_data_dir = evict_data_dir_c.clone();
        let evict_is_beacon = evict_is_beacon_c;
        let evict_beacon_url: Option<String> = std::env::var("EARTHGRID_BEACON_URL").ok().or_else(|| {
            let cfg_path = crate::config::Settings::config_dir().join("config.json");
            std::fs::read_to_string(&cfg_path).ok()
                .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                .and_then(|v| v["beacon_url"].as_str().map(|s| s.to_string()))
        });

        tokio::spawn(async move {
            // Initial delay: let node start up
            tokio::time::sleep(std::time::Duration::from_secs(120)).await;

            loop {
                let limit_gb = f64::from_bits(evict_storage_limit.load(Ordering::Relaxed));
                if limit_gb > 0.0 {
                    let current_bytes = {
                        let store = evict_store.lock().await;
                        store.total_bytes() as f64
                    };
                    let limit_bytes = limit_gb * 1_073_741_824.0;

                    if current_bytes > limit_bytes {
                        eprintln!(
                            "🗑️  Auto-eviction: {:.1} GB used > {:.0} GB limit — running eviction",
                            current_bytes / 1_073_741_824.0, limit_gb
                        );
                        let beacon_db = if evict_is_beacon {
                            Some(evict_data_dir.join("beacon.db"))
                        } else {
                            None
                        };
                        let catalog = evict_catalog.lock().await;
                        let mut store = evict_store.lock().await;
                        match crate::eviction::evict_with_beacon_url(
                            &catalog,
                            &mut store,
                            limit_gb,
                            beacon_db.as_deref(),
                            evict_beacon_url.as_deref(),
                        ) {
                            Ok(result) => {
                                if result.items_deleted > 0 {
                                    eprintln!(
                                        "🗑️  Auto-eviction done: {} items deleted, {:.1} GB freed — {}",
                                        result.items_deleted,
                                        result.bytes_freed as f64 / 1_073_741_824.0,
                                        result.reason
                                    );
                                }
                            }
                            Err(e) => eprintln!("⚠️  Auto-eviction error: {}", e),
                        }
                    }
                }

                tokio::time::sleep(std::time::Duration::from_secs(600)).await;
            }
        });
    }

    // Self-heartbeat: beacon nodes register themselves, non-beacon nodes register with remote beacon
    {
        let hb_store = state_clone_store.clone();
        let hb_catalog = state_clone_catalog.clone();
        let hb_node_id = state_node_id.clone();
        let hb_node_name = state_node_name.clone();
        let hb_storage_limit_gb = storage_limit_gb;
        let hb_gamification = state_clone_gamification.clone();
        let hb_port = port;
        let beacon_url_env = std::env::var("EARTHGRID_BEACON_URL").ok().or_else(|| {
            // Fallback: read beacon_url from config.json
            let cfg_path = dirs::home_dir().unwrap_or_default().join(".earthgrid/config.json");
            std::fs::read_to_string(&cfg_path)
                .ok()
                .and_then(|c| serde_json::from_str::<serde_json::Value>(&c).ok())
                .and_then(|v| v["beacon_url"].as_str().map(|s| s.to_string()))
        });

        // Build initial beacon list: bootstrap + cached
        let mut initial_beacons: Vec<String> = Vec::new();
        if beacon_enabled {
            initial_beacons.push(format!("http://127.0.0.1:{}", hb_port));
        }
        if let Some(ref bu) = beacon_url_env {
            if !initial_beacons.contains(bu) {
                initial_beacons.push(bu.clone());
            }
        }
        // Load cached beacons from disk
        let beacon_cache_path = dirs::home_dir()
            .unwrap_or_default()
            .join(".earthgrid/known_beacons.json");
        if let Ok(cached) = std::fs::read_to_string(&beacon_cache_path) {
            if let Ok(list) = serde_json::from_str::<Vec<String>>(&cached) {
                for b in list {
                    if !initial_beacons.contains(&b) {
                        initial_beacons.push(b);
                    }
                }
            }
        }

        if !initial_beacons.is_empty() {
            let beacon_cache_path_clone = beacon_cache_path.clone();
            tokio::spawn(async move {
                let client = reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(10))
                    .build()
                    .unwrap_or_default();
                // Track all known beacons (grows over time via discovery)
                let mut all_beacons: Vec<String> = initial_beacons;
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;

                    let item_count = {
                        let cat = hb_catalog.lock().await;
                        cat.item_count(None).unwrap_or(0)
                    };
                    let (chunk_count, chunks_bytes) = {
                        let store = hb_store.lock().await;
                        (store.chunk_count(), store.total_bytes())
                    };
                    let collections: Vec<String> = {
                        let cat = hb_catalog.lock().await;
                        cat.list_collections().unwrap_or_default().into_iter().map(|c| c.id).collect()
                    };
                    let catalog_version = {
                        let cat = hb_catalog.lock().await;
                        cat.catalog_version().unwrap_or(0)
                    };
                    let public_url = std::env::var("EARTHGRID_PUBLIC_URL")
                        .unwrap_or_else(|_| format!("http://127.0.0.1:{}", hb_port));
                    let body = serde_json::json!({
                        "node_id": hb_node_id,
                        "node_name": hb_node_name,
                        "url": public_url,
                        "can_source": true,
                        "item_count": item_count,
                        "chunk_count": chunk_count,
                        "chunks_bytes": chunks_bytes,
                        "collections": collections.clone(),
                        "storage_limit_gb": hb_storage_limit_gb,
                        "catalog_version": catalog_version,
                    });

                    let mut any_success = false;
                    let mut discovered_beacons: Vec<String> = Vec::new();

                    // Send heartbeat to ALL known beacons
                    for beacon_base in &all_beacons {
                        let hb_url = format!("{}/api/beacon/heartbeat", beacon_base.trim_end_matches('/'));
                        match client.post(&hb_url).json(&body).send().await {
                            Ok(r) => {
                                let status = r.status();
                                if status == reqwest::StatusCode::NOT_FOUND {
                                    // Not registered — register first
                                    let reg_url = hb_url.replace("/api/beacon/heartbeat", "/api/beacon/register");
                                    if let Ok(rr) = client.post(&reg_url).json(&body).send().await {
                                        // Try to extract known_beacons from register response too
                                        if let Ok(resp_body) = rr.json::<serde_json::Value>().await {
                                            if let Some(kb) = resp_body.get("known_beacons").and_then(|v| v.as_array()) {
                                                for b in kb {
                                                    if let Some(s) = b.as_str() {
                                                        if !discovered_beacons.contains(&s.to_string()) {
                                                            discovered_beacons.push(s.to_string());
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    any_success = true;
                                } else if status.is_success() {
                                    // Extract known_beacons from heartbeat response
                                    if let Ok(resp_body) = r.json::<serde_json::Value>().await {
                                        if let Some(kb) = resp_body.get("known_beacons").and_then(|v| v.as_array()) {
                                            for b in kb {
                                                if let Some(s) = b.as_str() {
                                                    if !discovered_beacons.contains(&s.to_string()) {
                                                        discovered_beacons.push(s.to_string());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    any_success = true;
                                } else {
                                    let _ = r.bytes().await;
                                }
                            }
                            Err(_) => {} // beacon unreachable, try next
                        }
                    }

                    // Merge discovered beacons into our list
                    let mut changed = false;
                    for db in &discovered_beacons {
                        if !all_beacons.contains(db) {
                            tracing::info!("Discovered new beacon: {}", db);
                            all_beacons.push(db.clone());
                            changed = true;
                        }
                    }

                    // Persist beacon cache to disk if changed
                    if changed {
                        if let Ok(json) = serde_json::to_string_pretty(&all_beacons) {
                            let _ = std::fs::write(&beacon_cache_path_clone, json);
                            tracing::info!("Updated beacon cache: {} beacons", all_beacons.len());
                        }
                    }

                    if any_success {
                        // Update gamification DB
                        let _ = hb_gamification.ensure_node_registered(
                            &hb_node_id, &hb_node_name, "", "", "",
                        );
                        let _ = hb_gamification.record_heartbeat(
                            &hb_node_id, 0, 0.0, 5000.0,
                        );
                        let _ = hb_gamification.update_storage_stats(
                            &hb_node_id, item_count as i64, chunks_bytes as i64,
                        );
                    }
                }
            });
        }
    }

    // Spawn P2P request handler if libp2p channels are provided
    if let Some((mut event_rx, cmd_tx)) = p2p_channels {
        let p2p_store = state_clone_store.clone();
        let p2p_catalog = state_clone_catalog.clone();
        let p2p_node_id = state_node_id.clone();
        let p2p_node_name = state_node_name.clone();
        let p2p_version = state_version.clone();

        tokio::spawn(async move {
            use crate::network::{NetworkCommand, NetworkEvent};
            use crate::transport::{EarthGridRequest, EarthGridResponse};

            while let Some(event) = event_rx.recv().await {
                match event {
                    NetworkEvent::InboundRequest { peer: _, request, channel } => {
                        let response = match request {
                            EarthGridRequest::GetChunk { hash } => {
                                let mut store = p2p_store.lock().await;
                                match store.get(&hash) {
                                    Ok(Some(data)) => EarthGridResponse::Chunk {
                                        hash: hash.clone(),
                                        data,
                                    },
                                    _ => EarthGridResponse::ChunkNotFound { hash },
                                }
                            }
                            EarthGridRequest::SearchCatalog { collection, bbox, datetime: _, limit } => {
                                let catalog = p2p_catalog.lock().await;
                                let items = catalog
                                    .search(collection.as_deref(), bbox, None, limit, 0)
                                    .unwrap_or_default();
                                let json_items: Vec<serde_json::Value> = items
                                    .into_iter()
                                    .map(|i| serde_json::to_value(i).unwrap_or_default())
                                    .collect();
                                let total = json_items.len();
                                EarthGridResponse::CatalogResults {
                                    items: json_items,
                                    total,
                                }
                            }
                            EarthGridRequest::NodeInfo => {
                                let store = p2p_store.lock().await;
                                let catalog = p2p_catalog.lock().await;
                                let collections: Vec<String> = catalog
                                    .list_collections()
                                    .unwrap_or_default()
                                    .into_iter()
                                    .map(|c| c.id)
                                    .collect();
                                EarthGridResponse::Info {
                                    node_id: p2p_node_id.clone(),
                                    node_name: p2p_node_name.clone(),
                                    version: p2p_version.clone(),
                                    collections,
                                    item_count: catalog.item_count(None).unwrap_or(0),
                                    chunk_count: store.chunk_count(),
                                    storage_bytes: store.total_bytes(),
                                }
                            }
                            EarthGridRequest::GetPeers => {
                                EarthGridResponse::Peers { peers: vec![] }
                            }
                            EarthGridRequest::ExecuteJob { .. } => {
                                EarthGridResponse::JobError {
                                    message: "Job execution not yet supported via P2P".to_string(),
                                }
                            }
                        };

                        // Send response back via the swarm
                        let _ = cmd_tx.send(NetworkCommand::SendResponse {
                            channel,
                            response,
                        }).await;
                    }
                    NetworkEvent::PeerDiscovered { peer_id, addresses } => {
                        eprintln!("🔗 P2P: Discovered peer {} at {:?}", peer_id, addresses);
                    }
                    NetworkEvent::PeerLost(peer_id) => {
                        eprintln!("🔗 P2P: Lost peer {}", peer_id);
                    }
                }
            }
        });
    }

    axum::serve(listener, app.into_make_service_with_connect_info::<std::net::SocketAddr>()).await?;
    Ok(())
}
