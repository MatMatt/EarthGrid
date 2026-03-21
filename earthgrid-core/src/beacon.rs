//! Beacon module — distributed node registry for EarthGrid.
//!
//! Nodes register themselves and send periodic heartbeats.
//! Any node can act as a beacon (registry) when EARTHGRID_BEACON=true.
//!
//! Storage: SQLite table `beacon_nodes` (separate from the catalog DB,
//! or shared if the same path is configured — WAL mode for concurrent access).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json,
};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::error::Result;

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

/// A registered EarthGrid node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BeaconNode {
    pub node_id: String,
    pub node_name: String,
    pub url: String,
    /// JSON array of collection IDs this node holds.
    pub collections: Vec<String>,
    pub item_count: i64,
    pub chunk_count: i64,
    pub chunks_bytes: i64,
    pub can_source: bool,
    pub storage_limit_gb: f64,
    pub last_seen: f64,
    pub sponsor_name: Option<String>,
    pub sponsor_url: Option<String>,
    pub node_url: Option<String>,
    pub group_id: Option<String>,
    pub uptime_seconds: i64,
    /// Computed: last_seen > now - 300s
    pub alive: bool,
}

#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub node_id: String,
    pub node_name: Option<String>,
    pub url: String,
    pub collections: Option<Vec<String>>,
    pub item_count: Option<i64>,
    pub chunk_count: Option<i64>,
    pub chunks_bytes: Option<i64>,
    pub can_source: Option<bool>,
    pub storage_limit_gb: Option<f64>,
    pub sponsor_name: Option<String>,
    pub sponsor_url: Option<String>,
    pub node_url: Option<String>,
    pub group: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct HeartbeatRequest {
    pub node_id: String,
    pub item_count: Option<i64>,
    pub chunk_count: Option<i64>,
    pub chunks_bytes: Option<i64>,
    pub uptime_seconds: Option<i64>,
    pub collections: Option<Vec<String>>,
    pub can_source: Option<bool>,
    pub storage_limit_gb: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct ListNodesQuery {
    pub alive_only: Option<bool>,
}

fn now_ts() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

fn is_alive(last_seen: f64) -> bool {
    now_ts() - last_seen < 300.0
}

fn err(status: StatusCode, msg: &str) -> (StatusCode, Json<serde_json::Value>) {
    (status, Json(serde_json::json!({"error": msg})))
}

// ---------------------------------------------------------------------------
// BeaconRegistry
// ---------------------------------------------------------------------------

/// In-memory cache backed by SQLite.
pub struct BeaconRegistry {
    conn: Connection,
}

impl BeaconRegistry {
    pub fn new(db_path: &std::path::Path) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(db_path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")?;
        let reg = Self { conn };
        reg.init_tables()?;
        Ok(reg)
    }

    #[cfg(test)]
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let reg = Self { conn };
        reg.init_tables()?;
        Ok(reg)
    }

    fn init_tables(&self) -> Result<()> {
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS beacon_nodes (
                node_id TEXT PRIMARY KEY,
                node_name TEXT NOT NULL DEFAULT '',
                url TEXT NOT NULL,
                collections_json TEXT NOT NULL DEFAULT '[]',
                item_count INTEGER NOT NULL DEFAULT 0,
                chunk_count INTEGER NOT NULL DEFAULT 0,
                chunks_bytes INTEGER NOT NULL DEFAULT 0,
                can_source INTEGER NOT NULL DEFAULT 0,
                storage_limit_gb REAL NOT NULL DEFAULT 0.0,
                last_seen REAL NOT NULL,
                sponsor_name TEXT,
                sponsor_url TEXT,
                node_url TEXT,
                group_id TEXT,
                uptime_seconds INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_beacon_last_seen ON beacon_nodes(last_seen);",
        )?;
        Ok(())
    }

    fn row_to_node(row: &rusqlite::Row) -> rusqlite::Result<BeaconNode> {
        let collections_json: String = row.get(3)?;
        let last_seen: f64 = row.get(9)?;
        Ok(BeaconNode {
            node_id: row.get(0)?,
            node_name: row.get(1)?,
            url: row.get(2)?,
            collections: serde_json::from_str(&collections_json).unwrap_or_default(),
            item_count: row.get(4)?,
            chunk_count: row.get(5)?,
            chunks_bytes: row.get(6)?,
            can_source: row.get::<_, i64>(7)? != 0,
            storage_limit_gb: row.get(8)?,
            last_seen,
            sponsor_name: row.get(10)?,
            sponsor_url: row.get(11)?,
            node_url: row.get(12)?,
            group_id: row.get(13)?,
            uptime_seconds: row.get(14)?,
            alive: is_alive(last_seen),
        })
    }

    /// Register or update a node.
    pub fn register(&self, req: &RegisterRequest) -> Result<BeaconNode> {
        let collections_json = serde_json::to_string(
            &req.collections.clone().unwrap_or_default(),
        )?;
        let now = now_ts();
        self.conn.execute(
            "INSERT INTO beacon_nodes
                (node_id, node_name, url, collections_json, item_count, chunk_count, chunks_bytes,
                 can_source, storage_limit_gb, last_seen, sponsor_name, sponsor_url, node_url, group_id, uptime_seconds)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, 0)
             ON CONFLICT(node_id) DO UPDATE SET
                node_name = excluded.node_name,
                url = excluded.url,
                collections_json = excluded.collections_json,
                item_count = excluded.item_count,
                chunk_count = excluded.chunk_count,
                chunks_bytes = excluded.chunks_bytes,
                can_source = excluded.can_source,
                storage_limit_gb = excluded.storage_limit_gb,
                last_seen = excluded.last_seen,
                sponsor_name = excluded.sponsor_name,
                sponsor_url = excluded.sponsor_url,
                node_url = excluded.node_url,
                group_id = excluded.group_id",
            params![
                req.node_id,
                req.node_name.as_deref().unwrap_or(""),
                req.url,
                collections_json,
                req.item_count.unwrap_or(0),
                req.chunk_count.unwrap_or(0),
                req.chunks_bytes.unwrap_or(0),
                req.can_source.unwrap_or(false) as i64,
                req.storage_limit_gb.unwrap_or(0.0),
                now,
                req.sponsor_name.as_deref(),
                req.sponsor_url.as_deref(),
                req.node_url.as_deref(),
                req.group.as_deref(),
            ],
        )?;
        self.get(&req.node_id)?.ok_or_else(|| crate::error::EarthGridError::Other("Node not found after insert".to_string()))
    }

    /// Update heartbeat fields for an existing node.
    pub fn heartbeat(&self, req: &HeartbeatRequest) -> Result<Option<BeaconNode>> {
        // Opportunistic cleanup: prune stale nodes (>1h) and dedup on each heartbeat
        let _ = self.prune_stale(3600.0);
        let _ = self.dedup_by_name();

        let now = now_ts();

        // Build dynamic UPDATE statement only for provided fields
        let mut sets = vec!["last_seen = ?1".to_string()];
        let mut pos = 2usize;

        if req.item_count.is_some() {
            sets.push(format!("item_count = ?{}", pos));
            pos += 1;
        }
        if req.chunk_count.is_some() {
            sets.push(format!("chunk_count = ?{}", pos));
            pos += 1;
        }
        if req.chunks_bytes.is_some() {
            sets.push(format!("chunks_bytes = ?{}", pos));
            pos += 1;
        }
        if req.uptime_seconds.is_some() {
            sets.push(format!("uptime_seconds = ?{}", pos));
            pos += 1;
        }
        if req.collections.is_some() {
            sets.push(format!("collections_json = ?{}", pos));
            pos += 1;
        }
        if req.can_source.is_some() {
            sets.push(format!("can_source = ?{}", pos));
            pos += 1;
        }
        if req.storage_limit_gb.is_some() {
            sets.push(format!("storage_limit_gb = ?{}", pos));
            pos += 1;
        }

        // node_id placeholder
        let node_id_pos = pos;

        let sql = format!(
            "UPDATE beacon_nodes SET {} WHERE node_id = ?{}",
            sets.join(", "),
            node_id_pos
        );

        // Build params dynamically using rusqlite's params_from_iter
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        param_values.push(Box::new(now));

        if let Some(v) = req.item_count { param_values.push(Box::new(v)); }
        if let Some(v) = req.chunk_count { param_values.push(Box::new(v)); }
        if let Some(v) = req.chunks_bytes { param_values.push(Box::new(v)); }
        if let Some(v) = req.uptime_seconds { param_values.push(Box::new(v)); }
        if let Some(ref v) = req.collections {
            param_values.push(Box::new(serde_json::to_string(v).unwrap_or_default()));
        }
        if let Some(v) = req.can_source { param_values.push(Box::new(v as i64)); }
        if let Some(v) = req.storage_limit_gb { param_values.push(Box::new(v)); }
        param_values.push(Box::new(req.node_id.clone()));

        let param_refs: Vec<&dyn rusqlite::types::ToSql> = param_values.iter().map(|p| p.as_ref()).collect();
        let affected = self.conn.execute(&sql, param_refs.as_slice())?;

        if affected == 0 {
            return Ok(None);
        }
        self.get(&req.node_id)
    }

    /// Get a single node by ID.
    pub fn get(&self, node_id: &str) -> Result<Option<BeaconNode>> {
        let mut stmt = self.conn.prepare(
            "SELECT node_id, node_name, url, collections_json, item_count, chunk_count, chunks_bytes,
                    can_source, storage_limit_gb, last_seen, sponsor_name, sponsor_url, node_url, group_id, uptime_seconds
             FROM beacon_nodes WHERE node_id = ?1",
        )?;
        let mut rows = stmt.query_map(params![node_id], Self::row_to_node)?;
        match rows.next() {
            Some(Ok(n)) => Ok(Some(n)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    /// List all nodes, optionally filtering to alive-only.
    pub fn list(&self, alive_only: bool) -> Result<Vec<BeaconNode>> {
        let sql = if alive_only {
            let threshold = now_ts() - 300.0;
            format!(
                "SELECT node_id, node_name, url, collections_json, item_count, chunk_count, chunks_bytes,
                        can_source, storage_limit_gb, last_seen, sponsor_name, sponsor_url, node_url, group_id, uptime_seconds
                 FROM beacon_nodes WHERE last_seen > {} ORDER BY last_seen DESC",
                threshold
            )
        } else {
            "SELECT node_id, node_name, url, collections_json, item_count, chunk_count, chunks_bytes,
                    can_source, storage_limit_gb, last_seen, sponsor_name, sponsor_url, node_url, group_id, uptime_seconds
             FROM beacon_nodes ORDER BY last_seen DESC"
                .to_string()
        };

        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map([], Self::row_to_node)?;
        let mut nodes = Vec::new();
        for row in rows {
            nodes.push(row?);
        }
        Ok(nodes)
    }

    /// Remove a node by ID. Returns true if it existed.
    pub fn remove(&self, node_id: &str) -> Result<bool> {
        let affected = self
            .conn
            .execute("DELETE FROM beacon_nodes WHERE node_id = ?1", params![node_id])?;
        Ok(affected > 0)
    }

    /// Prune stale nodes that haven't sent a heartbeat in `max_age_secs`.
    pub fn prune_stale(&self, max_age_secs: f64) -> Result<usize> {
        let threshold = now_ts() - max_age_secs;
        let affected = self.conn.execute(
            "DELETE FROM beacon_nodes WHERE last_seen < ?1",
            params![threshold],
        )?;
        if affected > 0 {
            println!("Pruned {} stale beacon node(s)", affected);
        }
        Ok(affected)
    }

    /// Deduplicate: if a node_name is registered with multiple IDs, keep only the most recent.
    pub fn dedup_by_name(&self) -> Result<usize> {
        let affected = self.conn.execute(
            "DELETE FROM beacon_nodes WHERE rowid NOT IN (
                SELECT MAX(rowid) FROM beacon_nodes GROUP BY node_name
            ) AND node_name != ''",
            [],
        )?;
        if affected > 0 {
            println!("Deduped {} beacon node(s) with duplicate names", affected);
        }
        Ok(affected)
    }
}

// ---------------------------------------------------------------------------
// Axum handlers
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct BeaconState {
    pub registry: Arc<Mutex<BeaconRegistry>>,
}

async fn register_node(
    State(state): State<BeaconState>,
    Json(req): Json<RegisterRequest>,
) -> impl IntoResponse {
    if req.node_id.is_empty() {
        return err(StatusCode::BAD_REQUEST, "node_id is required").into_response();
    }
    if req.url.is_empty() {
        return err(StatusCode::BAD_REQUEST, "url is required").into_response();
    }
    let registry = state.registry.lock().await;
    match registry.register(&req) {
        Ok(node) => (StatusCode::CREATED, Json(serde_json::to_value(node).unwrap())).into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

async fn heartbeat_node(
    State(state): State<BeaconState>,
    Json(req): Json<HeartbeatRequest>,
) -> impl IntoResponse {
    if req.node_id.is_empty() {
        return err(StatusCode::BAD_REQUEST, "node_id is required").into_response();
    }
    let registry = state.registry.lock().await;
    match registry.heartbeat(&req) {
        Ok(Some(node)) => (StatusCode::OK, Json(serde_json::to_value(node).unwrap())).into_response(),
        Ok(None) => err(StatusCode::NOT_FOUND, "Node not registered").into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

async fn list_nodes(
    State(state): State<BeaconState>,
    Query(q): Query<ListNodesQuery>,
) -> impl IntoResponse {
    let alive_only = q.alive_only.unwrap_or(false);
    let registry = state.registry.lock().await;
    match registry.list(alive_only) {
        Ok(nodes) => {
            let count = nodes.len();
            (StatusCode::OK, Json(serde_json::json!({
                "nodes": nodes,
                "count": count,
                "alive_only": alive_only,
            }))).into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

async fn get_node(
    State(state): State<BeaconState>,
    Path(node_id): Path<String>,
) -> impl IntoResponse {
    let registry = state.registry.lock().await;
    match registry.get(&node_id) {
        Ok(Some(node)) => (StatusCode::OK, Json(serde_json::to_value(node).unwrap())).into_response(),
        Ok(None) => err(StatusCode::NOT_FOUND, "Node not found").into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

async fn remove_node(
    State(state): State<BeaconState>,
    Path(node_id): Path<String>,
) -> impl IntoResponse {
    let registry = state.registry.lock().await;
    match registry.remove(&node_id) {
        Ok(true) => (StatusCode::OK, Json(serde_json::json!({"status": "removed", "node_id": node_id}))).into_response(),
        Ok(false) => err(StatusCode::NOT_FOUND, "Node not found").into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Router factory
// ---------------------------------------------------------------------------

/// Build the beacon router. Mount at "/" or nest under a prefix.
pub fn beacon_router(state: BeaconState) -> Router {
    Router::new()
        .route("/beacon/register", post(register_node))
        .route("/beacon/heartbeat", post(heartbeat_node))
        .route("/beacon/nodes", get(list_nodes))
        .route("/beacon/nodes/{node_id}", get(get_node))
        .route("/beacon/nodes/{node_id}", delete(remove_node))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_get() {
        let reg = BeaconRegistry::in_memory().unwrap();
        let req = RegisterRequest {
            node_id: "node-1".to_string(),
            node_name: Some("Test Node".to_string()),
            url: "http://localhost:3000".to_string(),
            collections: Some(vec!["sentinel-2".to_string()]),
            item_count: Some(100),
            chunk_count: Some(500),
            chunks_bytes: Some(1_000_000),
            can_source: Some(true),
            storage_limit_gb: Some(100.0),
            sponsor_name: None,
            sponsor_url: None,
            node_url: None,
            group: None,
        };
        let node = reg.register(&req).unwrap();
        assert_eq!(node.node_id, "node-1");
        assert_eq!(node.item_count, 100);
        assert!(node.alive);

        let fetched = reg.get("node-1").unwrap().unwrap();
        assert_eq!(fetched.url, "http://localhost:3000");
    }

    #[test]
    fn test_heartbeat() {
        let reg = BeaconRegistry::in_memory().unwrap();
        let req = RegisterRequest {
            node_id: "node-2".to_string(),
            node_name: None,
            url: "http://localhost:3001".to_string(),
            collections: None,
            item_count: Some(0),
            chunk_count: None,
            chunks_bytes: None,
            can_source: None,
            storage_limit_gb: None,
            sponsor_name: None,
            sponsor_url: None,
            node_url: None,
            group: None,
        };
        reg.register(&req).unwrap();

        let hb = HeartbeatRequest {
            node_id: "node-2".to_string(),
            item_count: Some(42),
            chunk_count: Some(200),
            chunks_bytes: Some(512_000),
            uptime_seconds: Some(3600),
            collections: None,
            can_source: None,
        };
        let updated = reg.heartbeat(&hb).unwrap().unwrap();
        assert_eq!(updated.item_count, 42);
        assert_eq!(updated.uptime_seconds, 3600);
    }

    #[test]
    fn test_list_and_remove() {
        let reg = BeaconRegistry::in_memory().unwrap();
        for i in 0..3 {
            reg.register(&RegisterRequest {
                node_id: format!("n-{}", i),
                node_name: None,
                url: format!("http://localhost:{}", 4000 + i),
                collections: None,
                item_count: None,
                chunk_count: None,
                chunks_bytes: None,
                can_source: None,
                storage_limit_gb: None,
                sponsor_name: None,
                sponsor_url: None,
                node_url: None,
                group: None,
            }).unwrap();
        }
        let all = reg.list(false).unwrap();
        assert_eq!(all.len(), 3);

        assert!(reg.remove("n-0").unwrap());
        assert_eq!(reg.list(false).unwrap().len(), 2);
        assert!(!reg.remove("n-0").unwrap()); // already gone
    }
}
