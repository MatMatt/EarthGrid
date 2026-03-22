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
    pub storage_limit_gb: f64,
    /// Data directory (for config updates like resize).
    pub data_dir: PathBuf,
    /// Counter for active fetch/ingest requests (replication yields when > 0).
    pub active_requests: Arc<AtomicUsize>,
    /// Whether this node runs as beacon (shows grid-wide landing page).
    pub is_beacon: bool,
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
        .route("/health", get(crate::routes::misc::health))
        .route("/node-info", get(crate::routes::misc::node_info))
        .route("/stats", get(crate::routes::stats::stats))
        // STAC Landing + Conformance
        .route("/", get(crate::routes::stac::stac_landing))
        .route("/.well-known/openeo", get(crate::routes::stac::well_known_openeo))
        .route("/conformance", get(crate::routes::stac::stac_conformance))
        // STAC Collections + Items
        .route("/stac/collections", get(crate::routes::stac::list_collections))
        .route("/stac/collections/{id}", get(crate::routes::stac::get_collection))
        .route("/stac/collections/{id}/items", get(crate::routes::stac::collection_items))
        .route("/stac/collections/{id}/items/{item_id}", get(crate::routes::stac::get_collection_item))
        // STAC Search (GET + POST)
        .route("/stac/search", get(crate::routes::stac::stac_search).post(crate::routes::stac::stac_search_post))
        // Chunks
        .route("/chunks", get(crate::routes::chunks::list_chunks))
        .route("/chunks/{sha}", get(crate::routes::chunks::get_chunk))
        // Write
        .route("/ingest", post(crate::routes::ingest_routes::ingest))
        // Integrity
        .route("/verify/{item_id}", get(crate::routes::chunks::verify_item))
        // Admin
        .route("/audit", get(crate::routes::misc::audit_log))
        // Federation (Phase 2)
        .route("/peers", get(crate::routes::federation::list_peers))
        .route("/peers", post(crate::routes::federation::register_peer))
        .route("/federation/sync", post(crate::routes::federation::federation_sync))
        .route("/federation/search", get(crate::routes::federation::federation_search))
        // Gossip + file ingest
        .route("/peers.json", get(crate::routes::federation::peers_json))
        .route("/ingest/file", post(crate::routes::ingest_routes::ingest_file_endpoint))
        // Element84 STAC Fetcher
        .route("/fetch", post(crate::routes::ingest_routes::fetch_handler))
        .route("/fetch/preview", get(crate::routes::ingest_routes::fetch_preview))
        // Stats
        .route("/stats/downloads", get(crate::routes::stats::stats_downloads))
        .route("/stats/hot-chunks", get(crate::routes::stats::stats_hot_chunks))
        .route("/stats/replication-advice", get(crate::routes::stats::stats_replication_advice))
        .route("/stats/ingest", get(crate::routes::stats::stats_ingest_history))
        // Replication
        .route("/replicate", post(crate::routes::chunks::replicate))
        // Gamification
        .route("/gamification/leaderboard", get(crate::routes::gamification_routes::gamification_leaderboard))
        .route("/gamification/node/{id}", get(crate::routes::gamification_routes::gamification_node_profile))
        .route("/gamification/feed", get(crate::routes::gamification_routes::gamification_feed))
        .route("/gamification/stats", get(crate::routes::gamification_routes::gamification_stats))
        .route("/gamification/economy", get(crate::routes::gamification_routes::gamification_economy))
        .route("/gamification/challenges", get(crate::routes::gamification_routes::gamification_challenges))
        .route("/gamification/challenges/{id}", get(crate::routes::gamification_routes::gamification_challenge_results))
        // Download (reconstruct + serve COG)
        .route("/download/{collection_id}/{item_id}", get(crate::routes::misc::download_item))
        // Processing
        .route("/process", post(crate::routes::process::process_job))
        .route("/process/operations", get(crate::routes::process::process_operations))
        // Sync
        .route("/sync", post(crate::routes::federation::sync_from_peer))
        .route("/sync-item", post(crate::routes::federation::sync_item))
        // Admin
        .route("/admin/stats", get(crate::routes::admin::admin_stats))
        .route("/admin/activity", get(crate::routes::admin::admin_activity))
        .route("/admin/node/name", axum::routing::patch(crate::routes::admin::patch_node_name))
        // Coverage + extended stats
        .route("/coverage/spatial", get(crate::routes::stats::coverage_spatial))
        .route("/stats/coverage", get(crate::routes::stats::stats_coverage))
        .route("/stats/requests", get(crate::routes::stats::stats_requests))
        .route("/stats/bandwidth", get(crate::routes::stats::stats_bandwidth))
        .route("/stats/replication", get(crate::routes::stats::stats_replication_status))
        .route("/bandwidth", get(crate::routes::stats::bandwidth_handler))
        // Nodes list + delete
        .route("/nodes", get(crate::routes::misc::list_nodes))
        .route("/nodes/{node_id}", delete(crate::routes::admin::delete_node))
        // Admin: collections
        .route("/admin/collections/{collection_id}", delete(crate::routes::admin::admin_delete_collection))
        // Admin: users
        .route("/admin/users", get(crate::routes::admin::admin_list_users).post(crate::routes::admin::admin_create_user))
        .route("/admin/users/{user_id}", delete(crate::routes::admin::admin_delete_user))
        // PATCH /node-name (alias)
        .route("/node-name", patch(crate::routes::admin::patch_node_name_alias))
        // Stats: access + uptake
        .route("/stats/access", get(crate::routes::stats::stats_access))
        .route("/stats.json", get(crate::routes::stats::stats_json_alias))
        .route("/stats/uptake", get(crate::routes::stats::stats_uptake))
        .route("/stats/uptake/csv", get(crate::routes::stats::stats_uptake_csv))
        // Replication items list
        .route("/replicate/items", get(crate::routes::chunks::replicate_items))
        // Resize storage
        .route("/resize", post(crate::routes::chunks::resize_storage))
        // Chunk map
        .route("/chunk-map/{collection_id}/{item_id}", get(crate::routes::chunks::chunk_map))
        // Point extraction
        .route("/point/{collection_id}/{item_id}", get(crate::routes::chunks::point_extract))
        // Federation: key exchange + user sync
        .route("/federation/exchange-key", post(crate::routes::federation::federation_exchange_key))
        .route("/federation/users", get(crate::routes::federation::federation_list_users).post(crate::routes::federation::federation_import_users))
        // HTML dashboard + UI
        .route("/dashboard", get(crate::routes::misc::dashboard))
        // openEO compatibility aliases (without /stac/ prefix)
        .route("/collections", get(crate::routes::stac::list_collections))
        .route("/collections/{id}", get(crate::routes::stac::get_collection))
        // openEO processes + validate + jobs
        .route("/processes", get(crate::routes::misc::openeo_processes))
        .route("/file_formats", get(crate::routes::misc::openeo_file_formats))
        .route("/result", post(crate::routes::misc::openeo_result))
        .route("/validate", post(crate::routes::misc::openeo_validate))
        .route("/jobs/{job_id}", get(crate::routes::misc::openeo_job_status))
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
    let node_id = env::var("EARTHGRID_NODE_ID")
        .ok()
        .or_else(|| {
            // Read persistent node_id from ~/.earthgrid/.node_id
            std::fs::read_to_string(&id_path).ok().map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
        })
        .or_else(|| {
            // Fallback: read node_id from config.json
            let cfg = earthgrid_home.join("config.json");
            if let Ok(c) = std::fs::read_to_string(&cfg) {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&c) {
                    return v["node_id"].as_str().map(|s| s.to_string()).filter(|s| !s.is_empty());
                }
            }
            None
        })
        .unwrap_or_else(|| {
            // Generate new ID and persist it
            let new_id = uuid::Uuid::new_v4().to_string();
            let _ = std::fs::create_dir_all(&earthgrid_home);
            let _ = std::fs::write(&id_path, &new_id);
            println!("📝 Generated new node ID: {} (saved to {})", new_id, id_path.display());
            new_id
        });
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
        let key_path = data_dir.join("node.key");
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
        storage_limit_gb,
        data_dir: data_dir.clone(),
        active_requests: Arc::new(AtomicUsize::new(0)),
        is_beacon: env::var("EARTHGRID_BEACON")
            .map(|v| v.to_lowercase() == "true" || v == "1")
            .unwrap_or(false),
    };

    // Conditionally build beacon router (EARTHGRID_BEACON=true)
    let beacon_enabled = env::var("EARTHGRID_BEACON")
        .map(|v| v.to_lowercase() == "true" || v == "1")
        .unwrap_or(false);

    let hb_peers = state.peers.clone();
    // Clones for P2P handler
    let state_clone_store = state.store.clone();
    let state_clone_catalog = state.catalog.clone();
    let state_active_requests = state.active_requests.clone();
    let state_clone_gamification = state.gamification.clone();
    let state_node_id = state.node_id.clone();
    let state_node_name = state.node_name.clone();
    let state_version = state.version.clone();
    let repl_peers = state.peers.clone();
    let mut app = router(state);

    // Mount beacon routes if enabled
    if beacon_enabled {
        let beacon_db_path = data_dir.join("beacon.db");
        match BeaconRegistry::new(&beacon_db_path) {
            Ok(registry) => {
                let beacon_state = BeaconState {
                    registry: Arc::new(Mutex::new(registry)),
                };
                app = app.merge(beacon_router(beacon_state));
                println!("🔦 Beacon registry enabled ({})", beacon_db_path.display());
            }
            Err(e) => {
                eprintln!("⚠️  Failed to initialize beacon registry: {}", e);
            }
        }
    }

    let addr = format!("{}:{}", host, port);
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
                let info_url = format!("{}/node-info", url);
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
                            for entry in &gossip.peers {
                                reg.add_if_new(&entry.url);
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

    // Self-heartbeat: beacon nodes register themselves, non-beacon nodes register with remote beacon
    {
        let hb_store = state_clone_store.clone();
        let hb_catalog = state_clone_catalog.clone();
        let hb_node_id = state_node_id.clone();
        let hb_node_name = state_node_name.clone();
        let hb_storage_limit_gb = storage_limit_gb;
        let hb_gamification = state_clone_gamification.clone();
        let hb_port = port;
        let beacon_url_env = std::env::var("EARTHGRID_BEACON_URL").ok();

        let target_url = if beacon_enabled {
            Some(format!("http://127.0.0.1:{}/beacon/heartbeat", hb_port))
        } else {
            beacon_url_env.map(|u| format!("{}/beacon/heartbeat", u.trim_end_matches('/')))
        };

        if let Some(url) = target_url {
            tokio::spawn(async move {
                let client = reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(10))
                    .build()
                    .unwrap_or_default();
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

                    let body = serde_json::json!({
                        "node_id": hb_node_id,
                        "node_name": hb_node_name,
                        "can_source": true,
                        "item_count": item_count,
                        "chunk_count": chunk_count,
                        "chunks_bytes": chunks_bytes,
                        "collections": collections,
                        "storage_limit_gb": hb_storage_limit_gb,
                    });

                    // First attempt: heartbeat (fast path for already-registered nodes)
                    let resp = client.post(&url).json(&body).send().await;
                    // If not registered, register first then heartbeat
                    if let Ok(r) = &resp {
                        if r.status() == reqwest::StatusCode::NOT_FOUND {
                            let register_url = url.replace("/beacon/heartbeat", "/beacon/register");
                            let register_body = serde_json::json!({
                                "node_id": hb_node_id,
                                "node_name": hb_node_name,
                                "url": std::env::var("EARTHGRID_PUBLIC_URL").unwrap_or_else(|_| format!("http://127.0.0.1:{}", hb_port)),
                                "can_source": true,
                                "item_count": item_count,
                                "chunk_count": chunk_count,
                                "chunks_bytes": chunks_bytes,
                                "collections": collections,
                                "storage_limit_gb": hb_storage_limit_gb,
                            });
                            let _ = client.post(&register_url).json(&register_body).send().await;
                        }
                    }

                    // Update gamification DB
                    let _ = hb_gamification.ensure_node_registered(
                        &hb_node_id, &hb_node_name, "", "", "",
                    );
                    let _ = hb_gamification.record_heartbeat(
                        &hb_node_id, 0, 0.0, 5000.0,
                    );
                    // Sync actual storage stats into gamification DB
                    let _ = hb_gamification.update_storage_stats(
                        &hb_node_id, item_count as i64, chunks_bytes as i64,
                    );
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

    axum::serve(listener, app).await?;
    Ok(())
}
