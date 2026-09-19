//! Beacon module — distributed node registry for EarthGrid.
//!
//! Nodes register themselves and send periodic heartbeats.
//! Any node can act as a beacon (registry) when EARTHGRID_BEACON=true.
//!
//! Storage: SQLite table `beacon_nodes` (separate from the catalog DB,
//! or shared if the same path is configured — WAL mode for concurrent access).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::HeaderMap;

use crate::auth::AuthConfig;
use crate::server::api_key;

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
use crate::beacon_federation::FederationState;
use crate::node_identity::NodeIdentity;

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
    /// Monotonic catalog version — changes on every ingest/delete.
    pub catalog_version: u64,
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
    pub catalog_version: Option<u64>,
    /// Hex Ed25519 public key of the registering node.
    #[serde(default)]
    pub public_key: Option<String>,
    /// Hex Ed25519 signature over [`register_message`].
    #[serde(default)]
    pub signature: Option<String>,
    /// Unix seconds at which the request was signed.
    #[serde(default)]
    pub timestamp: Option<u64>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct GridMetricPoint {
    pub ts: f64,
    pub nodes_total: i64,
    pub nodes_alive: i64,
    pub total_items: i64,
    pub total_chunks: i64,
    pub total_bytes: i64,
    pub total_storage_limit_gb: f64,
}

#[derive(Debug, Deserialize)]
pub struct HeartbeatRequest {
    pub node_id: String,
    pub url: Option<String>,
    pub node_name: Option<String>,
    pub item_count: Option<i64>,
    pub chunk_count: Option<i64>,
    pub chunks_bytes: Option<i64>,
    pub uptime_seconds: Option<i64>,
    pub collections: Option<Vec<String>>,
    pub can_source: Option<bool>,
    pub storage_limit_gb: Option<f64>,
    pub catalog_version: Option<u64>,
    /// Hex Ed25519 public key of the node.
    #[serde(default)]
    pub public_key: Option<String>,
    /// Hex Ed25519 signature over [`heartbeat_message`].
    #[serde(default)]
    pub signature: Option<String>,
    /// Unix seconds at which the request was signed.
    #[serde(default)]
    pub timestamp: Option<u64>,
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
// Signed registration
// ---------------------------------------------------------------------------

/// Signing domain of `/api/beacon/register`. Bump on any format change.
pub const REGISTER_DOMAIN: &str = "earthgrid-beacon-register-v2";

/// Signing domain of `/api/beacon/heartbeat`. Distinct from
/// [`REGISTER_DOMAIN`], so a signature made for one endpoint never verifies on
/// the other. Bump on any format change.
pub const HEARTBEAT_DOMAIN: &str = "earthgrid-beacon-heartbeat-v2";

/// Replay window: a signed register/heartbeat is accepted only if its
/// timestamp is within this many seconds of the beacon's clock.
pub const REPLAY_WINDOW_SECS: u64 = 300;

/// Builder of the canonical signed message: the domain tag, then one
/// `name=value` line per field.
///
/// An absent field is `~`, which no present value can render as. Strings are
/// length-prefixed (`<byte length>:<value>`), so `None` and `Some("")` sign
/// differently and no value can shift a field boundary to make two different
/// requests produce the same message.
struct SignedFields(String);

impl SignedFields {
    fn new(domain: &str) -> Self {
        Self(format!("{}\n", domain))
    }

    fn text(mut self, name: &str, value: Option<&str>) -> Self {
        match value {
            Some(v) => self.0.push_str(&format!("{}={}:{}\n", name, v.len(), v)),
            None => self.0.push_str(&format!("{}=~\n", name)),
        }
        self
    }

    /// Integers and booleans, in their decimal / `true`|`false` form.
    fn value<T: std::fmt::Display>(mut self, name: &str, value: Option<T>) -> Self {
        match value {
            Some(v) => self.0.push_str(&format!("{}={}\n", name, v)),
            None => self.0.push_str(&format!("{}=~\n", name)),
        }
        self
    }

    /// Fixed three decimals: the value crosses JSON between signer and
    /// verifier, and must render identically on both sides.
    fn float(self, name: &str, value: Option<f64>) -> Self {
        self.value(name, value.map(signed_float_text))
    }

    /// `<count>` followed by each item length-prefixed.
    fn list(mut self, name: &str, value: Option<&[String]>) -> Self {
        match value {
            Some(items) => {
                self.0.push_str(&format!("{}={}", name, items.len()));
                for item in items {
                    self.0.push_str(&format!("[{}:{}]", item.len(), item));
                }
                self.0.push('\n');
            }
            None => self.0.push_str(&format!("{}=~\n", name)),
        }
        self
    }
}

/// A float field as it appears in the signed message: fixed three decimals.
fn signed_float_text(v: f64) -> String {
    format!("{:.3}", v)
}

/// The number a signed float field actually commits to — the three-decimal
/// rendering parsed back. This, not the received `f64`, is what gets stored:
/// otherwise a value could move inside its rounding bucket under a signature
/// that still verifies.
fn signed_float(v: f64) -> f64 {
    signed_float_text(v).parse().unwrap_or(v)
}

/// SHA-256 (hex) of a canonical signed message, kept with the last accepted
/// timestamp to recognise an idempotent duplicate of the same request.
fn message_digest(message: &str) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(message.as_bytes()))
}

/// The canonical message a node signs for `/api/beacon/register`. It covers
/// every field of the body: nothing a register can change is left unsigned.
pub fn register_message(req: &RegisterRequest) -> String {
    SignedFields::new(REGISTER_DOMAIN)
        .text("node_id", Some(req.node_id.as_str()))
        .text("node_name", req.node_name.as_deref())
        .text("url", Some(req.url.as_str()))
        .list("collections", req.collections.as_deref())
        .value("item_count", req.item_count)
        .value("chunk_count", req.chunk_count)
        .value("chunks_bytes", req.chunks_bytes)
        .value("can_source", req.can_source)
        .float("storage_limit_gb", req.storage_limit_gb)
        .text("sponsor_name", req.sponsor_name.as_deref())
        .text("sponsor_url", req.sponsor_url.as_deref())
        .text("node_url", req.node_url.as_deref())
        .text("group", req.group.as_deref())
        .value("catalog_version", req.catalog_version)
        .value("timestamp", req.timestamp)
        .0
}

/// The canonical message a node signs for `/api/beacon/heartbeat`. It covers
/// every field of the body: nothing a heartbeat can change is left unsigned.
pub fn heartbeat_message(req: &HeartbeatRequest) -> String {
    SignedFields::new(HEARTBEAT_DOMAIN)
        .text("node_id", Some(req.node_id.as_str()))
        .text("url", req.url.as_deref())
        .text("node_name", req.node_name.as_deref())
        .value("item_count", req.item_count)
        .value("chunk_count", req.chunk_count)
        .value("chunks_bytes", req.chunks_bytes)
        .value("uptime_seconds", req.uptime_seconds)
        .list("collections", req.collections.as_deref())
        .value("can_source", req.can_source)
        .float("storage_limit_gb", req.storage_limit_gb)
        .value("catalog_version", req.catalog_version)
        .value("timestamp", req.timestamp)
        .0
}

/// Sender side: sign a canonical message ([`register_message`] or
/// [`heartbeat_message`]) with the node identity.
/// Returns `(public_key_hex, signature_hex)`.
pub fn sign_request(identity: &NodeIdentity, message: &str) -> (String, String) {
    (identity.public_key_hex(), identity.sign_hex(message))
}

/// Beacon side: check that `message` — the canonical message rebuilt from the
/// received body — is signed by the key the request presents, and is fresh.
/// Fails closed — an unsigned request is an error, not a legacy client.
pub fn verify_request(
    message: &str,
    timestamp: Option<u64>,
    public_key: Option<&str>,
    signature: Option<&str>,
) -> std::result::Result<(), &'static str> {
    let (Some(timestamp), Some(public_key), Some(signature)) = (timestamp, public_key, signature) else {
        return Err("signed request required: public_key, signature and timestamp must be present");
    };
    if (now_ts() as u64).abs_diff(timestamp) > REPLAY_WINDOW_SECS {
        return Err("timestamp outside the replay window");
    }
    if !NodeIdentity::verify_hex(public_key, signature, message) {
        return Err("invalid signature");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// BeaconRegistry
// ---------------------------------------------------------------------------

// Helper structs for coverage aggregation
struct TileRow {
    collection: String,
    tile_id: String,
    w: f64, s: f64, e: f64, n: f64,
    dates_json: String,
    bands_json: String,
    _node_id: String,
    item_count: i64,
    polygon_json: Option<String>,
}

struct TileAgg {
    collection: String,
    tile_id: String,
    w: f64, s: f64, e: f64, n: f64,
    dates: std::collections::BTreeSet<String>,
    bands: std::collections::BTreeSet<String>,
    node_count: i64,
    item_count: i64,
    polygon_json: Option<String>,
}

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
            CREATE INDEX IF NOT EXISTS idx_beacon_last_seen ON beacon_nodes(last_seen);

            CREATE TABLE IF NOT EXISTS beacon_node_pins (
                node_id TEXT PRIMARY KEY,
                public_key TEXT NOT NULL,
                last_timestamp INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS grid_metrics (
                ts REAL NOT NULL,
                nodes_total INTEGER NOT NULL DEFAULT 0,
                nodes_alive INTEGER NOT NULL DEFAULT 0,
                total_items INTEGER NOT NULL DEFAULT 0,
                total_chunks INTEGER NOT NULL DEFAULT 0,
                total_bytes INTEGER NOT NULL DEFAULT 0,
                total_storage_limit_gb REAL NOT NULL DEFAULT 0.0
            );
            CREATE INDEX IF NOT EXISTS idx_grid_metrics_ts ON grid_metrics(ts);",
        )?;
        // Safe migration: add catalog_version column if missing
        let _ = self.conn.execute_batch(
            "ALTER TABLE beacon_nodes ADD COLUMN catalog_version INTEGER NOT NULL DEFAULT 0;",
        );
        // Safe migration: pins used to live in a `public_key` column of
        // beacon_nodes, where pruning or deduplicating the row destroyed them.
        // Carry any such pin over; fails harmlessly where the column never existed.
        let _ = self.conn.execute_batch(
            "INSERT OR IGNORE INTO beacon_node_pins (node_id, public_key)
             SELECT node_id, public_key FROM beacon_nodes WHERE public_key IS NOT NULL;",
        );
        // Safe migration: digest of the signed message behind last_timestamp
        let _ = self.conn.execute_batch(
            "ALTER TABLE beacon_node_pins ADD COLUMN last_digest TEXT NOT NULL DEFAULT '';",
        );
        // Safe migration: add dates_json and bands_json to beacon_node_tiles
        let _ = self.conn.execute_batch(
            "ALTER TABLE beacon_node_tiles ADD COLUMN dates_json TEXT NOT NULL DEFAULT '[]';",
        );
        let _ = self.conn.execute_batch(
            "ALTER TABLE beacon_node_tiles ADD COLUMN bands_json TEXT NOT NULL DEFAULT '[]';",
        );
        let _ = self.conn.execute_batch(
            "ALTER TABLE beacon_node_tiles ADD COLUMN item_count INTEGER NOT NULL DEFAULT 0;",
        );
        let _ = self.conn.execute_batch(
            "ALTER TABLE beacon_node_tiles ADD COLUMN polygon_json TEXT;",
        );

        // Spatial coverage tiles aggregated from all nodes
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS beacon_node_tiles (
                node_id TEXT NOT NULL,
                collection TEXT NOT NULL,
                tile_id TEXT NOT NULL,
                bbox_west REAL,
                bbox_south REAL,
                bbox_east REAL,
                bbox_north REAL,
                date_count INTEGER NOT NULL DEFAULT 0,
                item_count INTEGER NOT NULL DEFAULT 0,
                dates_json TEXT NOT NULL DEFAULT '[]',
                bands_json TEXT NOT NULL DEFAULT '[]',
                PRIMARY KEY (node_id, collection, tile_id)
            );
            CREATE INDEX IF NOT EXISTS idx_bnt_node ON beacon_node_tiles(node_id);

            CREATE TABLE IF NOT EXISTS beacon_node_stats (
                node_id TEXT NOT NULL,
                updated_at REAL NOT NULL,
                total_items INTEGER NOT NULL DEFAULT 0,
                total_chunks INTEGER NOT NULL DEFAULT 0,
                total_bytes INTEGER NOT NULL DEFAULT 0,
                bytes_ingested INTEGER NOT NULL DEFAULT 0,
                bytes_served INTEGER NOT NULL DEFAULT 0,
                chunks_served INTEGER NOT NULL DEFAULT 0,
                requests_total INTEGER NOT NULL DEFAULT 0,
                collections_json TEXT NOT NULL DEFAULT '[]',
                PRIMARY KEY (node_id)
            );"
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
            catalog_version: row.get::<_, i64>(15).unwrap_or(0) as u64,
            alive: is_alive(last_seen),
        })
    }

    /// The pin of a node: its public key, the timestamp of its last accepted
    /// signed request and the digest of that request's signed message. Lives in
    /// `beacon_node_pins`, not in the node's `beacon_nodes` row, so it outlives
    /// pruning and deduplication of that row.
    fn pin(&self, node_id: &str) -> Result<Option<(String, u64, String)>> {
        match self.conn.query_row(
            "SELECT public_key, last_timestamp, last_digest FROM beacon_node_pins WHERE node_id = ?1",
            params![node_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64, row.get::<_, String>(2)?)),
        ) {
            Ok(pin) => Ok(Some(pin)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// The public key pinned for a node, if any.
    pub fn pinned_key(&self, node_id: &str) -> Result<Option<String>> {
        Ok(self.pin(node_id)?.map(|(key, _, _)| key))
    }

    /// Pin a node's public key on its first signed contact. An existing pin is
    /// never replaced, and it survives the node going stale — only the admin
    /// delete ([`remove`](Self::remove)) releases it.
    pub fn pin_public_key(&self, node_id: &str, public_key: &str) -> Result<()> {
        if public_key.is_empty() {
            return Ok(());
        }
        self.conn.execute(
            "INSERT OR IGNORE INTO beacon_node_pins (node_id, public_key) VALUES (?1, ?2)",
            params![node_id, public_key.to_ascii_lowercase()],
        )?;
        Ok(())
    }

    /// Record the timestamp of an accepted signed request, with the digest of
    /// its signed `message`. The timestamp only moves forward;
    /// [`authenticate`](Self::authenticate) refuses anything older, and an equal
    /// timestamp unless it carries this very message.
    pub fn record_accepted_timestamp(&self, node_id: &str, timestamp: u64, message: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE beacon_node_pins SET last_timestamp = ?1, last_digest = ?3
             WHERE node_id = ?2 AND last_timestamp < ?1",
            params![timestamp as i64, node_id, message_digest(message)],
        )?;
        Ok(())
    }

    /// Whether `node_name` is held by a pinned node other than `node_id`.
    fn name_held_by_other_pinned_node(&self, node_name: &str, node_id: &str) -> Result<bool> {
        if node_name.is_empty() {
            return Ok(false);
        }
        let holders: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM beacon_nodes
             WHERE node_name = ?1 AND node_id != ?2
               AND node_id IN (SELECT node_id FROM beacon_node_pins)",
            params![node_name, node_id],
            |row| row.get(0),
        )?;
        Ok(holders > 0)
    }

    /// Refuse a register/heartbeat that takes the node_name of a different
    /// pinned node: sharing a name is what made `dedup_by_name` delete the
    /// other row.
    fn reject_name_of_pinned_node(&self, node_name: Option<&str>, node_id: &str) -> Result<()> {
        let name = node_name.unwrap_or("");
        if self.name_held_by_other_pinned_node(name, node_id)? {
            return Err(crate::error::EarthGridError::Other(format!(
                "node_name '{}' belongs to another registered node. Choose a different name.",
                name
            )));
        }
        Ok(())
    }

    /// Authenticate a register/heartbeat request. `message` is the canonical
    /// message rebuilt from the received body ([`register_message`] or
    /// [`heartbeat_message`]). The request must carry a fresh, valid signature
    /// over it (see [`verify_request`]); once a key is pinned for the node_id
    /// it must be signed by exactly that key, with a timestamp newer than the
    /// last one accepted — the replay window rests on the wall clock, and this
    /// keeps an old signature dead even if that clock is moved back. Fails closed.
    ///
    /// One exception: an equal timestamp is accepted when the signed message is
    /// byte-for-byte the one already accepted. A node that reaches this beacon
    /// through two URLs sends the same signed request to both every cycle; the
    /// second copy is an idempotent duplicate, not a replay of something older.
    /// An equal timestamp with any other message is refused.
    pub fn authenticate(
        &self,
        node_id: &str,
        message: &str,
        timestamp: Option<u64>,
        public_key: Option<&str>,
        signature: Option<&str>,
    ) -> std::result::Result<(), &'static str> {
        verify_request(message, timestamp, public_key, signature)?;
        match self.pin(node_id) {
            Ok(Some((pinned, _, _))) if !pinned.eq_ignore_ascii_case(public_key.unwrap_or("")) => {
                Err("public key does not match the key pinned for this node_id")
            }
            Ok(Some((_, last, _))) if timestamp.unwrap_or(0) < last => {
                Err("timestamp is older than the last accepted request for this node_id")
            }
            Ok(Some((_, last, digest))) if timestamp.unwrap_or(0) == last && digest != message_digest(message) => {
                Err("timestamp equals the last accepted request for this node_id but the message differs")
            }
            Ok(_) => Ok(()),
            Err(_) => Err("could not read the pinned key for this node_id"),
        }
    }

    /// Register or update a node.
    pub fn register(&self, req: &RegisterRequest) -> Result<BeaconNode> {
        // Reject if node_id already exists with a different URL
        let existing: Option<String> = self.conn.query_row(
            "SELECT url FROM beacon_nodes WHERE node_id = ?1",
            rusqlite::params![req.node_id],
            |row| row.get(0),
        ).ok();
        if let Some(ref old_url) = existing {
            if old_url != &req.url && !old_url.is_empty() {
                return Err(crate::error::EarthGridError::Other(format!(
                    "node_id {} is already registered with a different URL ({}).                      Use a different node_id or remove the existing node first.",
                    req.node_id, old_url
                )));
            }
        }

        // Reject if node_name already exists under a different node_id
        if let Some(ref name) = req.node_name {
            if !name.is_empty() {
                let existing_id: Option<String> = self.conn.query_row(
                    "SELECT node_id FROM beacon_nodes WHERE node_name = ?1",
                    rusqlite::params![name],
                    |row| row.get(0),
                ).ok();
                if let Some(ref eid) = existing_id {
                    if eid != &req.node_id {
                        return Err(crate::error::EarthGridError::Other(format!(
                            "node_name '{}' is already taken by node {}. Choose a different name.",
                            name, eid
                        )));
                    }
                }
            }
        }
        // The check above looks at one holder of the name; a pinned holder is
        // refused explicitly, whichever row the lookup happened to return.
        self.reject_name_of_pinned_node(req.node_name.as_deref(), &req.node_id)?;

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
                // Stored exactly as signed (three decimals), not as received
                signed_float(req.storage_limit_gb.unwrap_or(0.0)),
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
    /// Return URLs of all known beacon nodes (nodes with also_beacon or the self-beacon).
    /// Used to propagate beacon discovery to regular nodes.
    pub fn known_beacon_urls(&self) -> Vec<String> {
        // Return all unique node URLs that are beacons
        // For now, return the beacon's own URL + any federated peer beacon URLs
        let mut urls = Vec::new();
        // Add self (this beacon's external URL) from env or config
        if let Ok(public) = std::env::var("EARTHGRID_PUBLIC_URL") {
            urls.push(public);
        }
        // Add federation peer URLs
        if let Ok(peers) = std::env::var("EARTHGRID_BEACON_PEERS") {
            for p in peers.split(',') {
                let p = p.trim().to_string();
                if !p.is_empty() && !urls.contains(&p) {
                    urls.push(p);
                }
            }
        }
        // Also scan beacon_nodes for nodes that might be beacons themselves
        // (they register with can_source=true and have beacon-like URLs)
        if let Ok(nodes) = self.list(true) {
            for node in &nodes {
                if !node.url.is_empty() && !urls.contains(&node.url) {
                    // We include all alive node URLs — the receiving node
                    // can try them as potential beacons
                    urls.push(node.url.clone());
                }
            }
        }
        urls
    }

    pub fn heartbeat(&self, req: &HeartbeatRequest) -> Result<Option<BeaconNode>> {
        // Opportunistic cleanup: prune stale nodes (>1h) and dedup on each heartbeat
        let _ = self.prune_stale(3600.0);
        let _ = self.dedup_by_name();
        // Record grid-wide metrics snapshot (max every 10 min)
        let _ = self.record_grid_snapshot();

        // A heartbeat can rename the node: same rule as register.
        self.reject_name_of_pinned_node(req.node_name.as_deref(), &req.node_id)?;

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
        if req.catalog_version.is_some() {
            sets.push(format!("catalog_version = ?{}", pos));
            pos += 1;
        }
        if req.url.is_some() {
            sets.push(format!("url = ?{}", pos));
            pos += 1;
        }
        if req.node_name.is_some() {
            sets.push(format!("node_name = ?{}", pos));
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
        // Stored exactly as signed (three decimals), not as received
        if let Some(v) = req.storage_limit_gb { param_values.push(Box::new(signed_float(v))); }
        if let Some(v) = req.catalog_version { param_values.push(Box::new(v as i64)); }
        if let Some(ref v) = req.url { param_values.push(Box::new(v.clone())); }
        if let Some(ref v) = req.node_name { param_values.push(Box::new(v.clone())); }
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

    /// Remove a node by ID **and release its pinned key** — the admin delete
    /// path, and the only way a pin is ever cleared. Returns true if it existed.
    pub fn remove(&self, node_id: &str) -> Result<bool> {
        let affected = self
            .conn
            .execute("DELETE FROM beacon_nodes WHERE node_id = ?1", params![node_id])?;
        let unpinned = self
            .conn
            .execute("DELETE FROM beacon_node_pins WHERE node_id = ?1", params![node_id])?;
        Ok(affected > 0 || unpinned > 0)
    }

    /// Retire a node's registry row but keep its pin, so the node_id still
    /// belongs to the same key when the node comes back. Returns true if it existed.
    pub fn retire(&self, node_id: &str) -> Result<bool> {
        let affected = self
            .conn
            .execute("DELETE FROM beacon_nodes WHERE node_id = ?1", params![node_id])?;
        Ok(affected > 0)
    }

    /// Prune stale nodes that haven't sent a heartbeat in `max_age_secs`.
    ///
    /// Only the `beacon_nodes` row and the tiles go. The pin in
    /// `beacon_node_pins` stays, so a node_id that went quiet cannot be claimed
    /// afresh by another key.
    pub fn prune_stale(&self, max_age_secs: f64) -> Result<usize> {
        let threshold = now_ts() - max_age_secs;
        // Also clean up tiles for pruned nodes
        self.conn.execute(
            "DELETE FROM beacon_node_tiles WHERE node_id IN (SELECT node_id FROM beacon_nodes WHERE last_seen < ?1)",
            params![threshold],
        )?;
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
    ///
    /// The row of a pinned node is never the one deleted: an unpinned row
    /// sharing a pinned node's name goes instead, whatever its age, and two
    /// pinned rows are both kept. Sharing a name must not evict a pinned node.
    pub fn dedup_by_name(&self) -> Result<usize> {
        let affected = self.conn.execute(
            "DELETE FROM beacon_nodes
             WHERE node_name != ''
               AND node_id NOT IN (SELECT node_id FROM beacon_node_pins)
               AND (
                    rowid NOT IN (SELECT MAX(rowid) FROM beacon_nodes GROUP BY node_name)
                    OR node_name IN (
                        SELECT node_name FROM beacon_nodes
                        WHERE node_id IN (SELECT node_id FROM beacon_node_pins)
                    )
               )",
            [],
        )?;
        if affected > 0 {
            println!("Deduped {} beacon node(s) with duplicate names", affected);
        }
Ok(affected)
    }

    /// Get the stored catalog_version for a node.
    pub fn get_catalog_version(&self, node_id: &str) -> Option<u64> {
        self.conn.query_row(
            "SELECT catalog_version FROM beacon_nodes WHERE node_id = ?1",
            params![node_id],
            |row| row.get::<_, i64>(0),
        ).ok().map(|v| v as u64)
    }

    /// Replace all tiles for a node with fresh data from /coverage/spatial.
    pub fn store_node_tiles(&self, node_id: &str, coverage: &serde_json::Value) -> Result<usize> {
        self.conn.execute(
            "DELETE FROM beacon_node_tiles WHERE node_id = ?1",
            params![node_id],
        )?;
        let mut count = 0usize;
        if let Some(collections) = coverage.get("collections").and_then(|c| c.as_object()) {
            for (collection, data) in collections {
                if let Some(cells) = data.get("cells").and_then(|c| c.as_array()) {
                    for cell in cells {
                        let tile_id = cell.get("tile_id").and_then(|t| t.as_str()).unwrap_or("");
                        let bbox = cell.get("bbox").and_then(|b| b.as_array());
                        let (w, s, e, n) = if let Some(bbox) = bbox {
                            (
                                bbox.get(0).and_then(|v| v.as_f64()).unwrap_or(0.0),
                                bbox.get(1).and_then(|v| v.as_f64()).unwrap_or(0.0),
                                bbox.get(2).and_then(|v| v.as_f64()).unwrap_or(0.0),
                                bbox.get(3).and_then(|v| v.as_f64()).unwrap_or(0.0),
                            )
                        } else if let Some(polygon) = cell.get("polygon").and_then(|p| p.as_array()) {
                            // Derive bbox from polygon coordinates [lon, lat]
                            let mut min_lon = f64::MAX;
                            let mut min_lat = f64::MAX;
                            let mut max_lon = f64::MIN;
                            let mut max_lat = f64::MIN;
                            for coord in polygon {
                                if let Some(arr) = coord.as_array() {
                                    if let (Some(lon), Some(lat)) = (arr.get(0).and_then(|v| v.as_f64()), arr.get(1).and_then(|v| v.as_f64())) {
                                        min_lon = min_lon.min(lon);
                                        min_lat = min_lat.min(lat);
                                        max_lon = max_lon.max(lon);
                                        max_lat = max_lat.max(lat);
                                    }
                                }
                            }
                            if min_lon < f64::MAX { (min_lon, min_lat, max_lon, max_lat) } else { (0.0, 0.0, 0.0, 0.0) }
                        } else {
                            (0.0, 0.0, 0.0, 0.0)
                        };
                        let date_count = cell.get("date_count").and_then(|d| d.as_i64()).unwrap_or(0);
                        let item_count = cell.get("item_count").and_then(|d| d.as_i64()).unwrap_or(0);
                        let dates_json = cell.get("dates")
                            .map(|d| serde_json::to_string(d).unwrap_or_else(|_| "[]".to_string()))
                            .unwrap_or_else(|| "[]".to_string());
                        let bands_json = cell.get("bands")
                            .map(|b| serde_json::to_string(b).unwrap_or_else(|_| "[]".to_string()))
                            .unwrap_or_else(|| "[]".to_string());
                        let polygon_json = cell.get("polygon")
                            .map(|p| serde_json::to_string(p).unwrap_or_else(|_| "null".to_string()));
                        self.conn.execute(
                            "INSERT OR REPLACE INTO beacon_node_tiles
                                (node_id, collection, tile_id, bbox_west, bbox_south, bbox_east, bbox_north, date_count, item_count, dates_json, bands_json, polygon_json)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                            params![node_id, collection, tile_id, w, s, e, n, date_count, item_count, dates_json, bands_json, polygon_json],
                        )?;
                        count += 1;
                    }
                }
            }
        }
        Ok(count)
    }

    /// Get aggregated spatial coverage from all nodes.
    /// Merges dates/bands across nodes for the same tile (union of dates).
    pub fn get_aggregated_coverage(&self) -> Result<serde_json::Value> {
        let mut stmt = self.conn.prepare(
            "SELECT collection, tile_id, bbox_west, bbox_south, bbox_east, bbox_north,
                    dates_json, bands_json, node_id, item_count, polygon_json
             FROM beacon_node_tiles
             ORDER BY collection, tile_id"
        )?;

        // Aggregate per (collection, tile_id)
        let mut tile_map: std::collections::HashMap<(String, String), TileAgg> =
            std::collections::HashMap::new();

        let rows = stmt.query_map([], |row| {
            Ok(TileRow {
                collection: row.get(0)?,
                tile_id: row.get(1)?,
                w: row.get(2)?,
                s: row.get(3)?,
                e: row.get(4)?,
                n: row.get(5)?,
                dates_json: row.get(6)?,
                bands_json: row.get(7)?,
                _node_id: row.get(8)?,
                item_count: row.get::<_, i64>(9).unwrap_or(0),
                polygon_json: row.get::<_, Option<String>>(10).unwrap_or(None),
            })
        })?;

        for row in rows {
            if let Ok(r) = row {
                let key = (r.collection.clone(), r.tile_id.clone());
                let agg = tile_map.entry(key).or_insert_with(|| TileAgg {
                    collection: r.collection,
                    tile_id: r.tile_id,
                    w: r.w, s: r.s, e: r.e, n: r.n,
                    dates: std::collections::BTreeSet::new(),
                    bands: std::collections::BTreeSet::new(),
                    node_count: 0,
                    item_count: 0,
                    polygon_json: None,
                });
                // Use the first available real polygon
                if agg.polygon_json.is_none() {
                    if let Some(ref pj) = r.polygon_json {
                        if pj != "null" {
                            agg.polygon_json = Some(pj.clone());
                        }
                    }
                }
                // Merge dates (union)
                if let Ok(dates) = serde_json::from_str::<Vec<String>>(&r.dates_json) {
                    for d in dates { agg.dates.insert(d); }
                }
                // Merge bands (union)
                if let Ok(bands) = serde_json::from_str::<Vec<String>>(&r.bands_json) {
                    for b in bands { agg.bands.insert(b); }
                }
                agg.item_count += r.item_count;
                agg.node_count += 1;
            }
        }

        let mut collections: std::collections::HashMap<String, Vec<serde_json::Value>> =
            std::collections::HashMap::new();
        for ((_, _), agg) in tile_map {
            let dates: Vec<&String> = agg.dates.iter().collect();
            let bands: Vec<&String> = agg.bands.iter().collect();
            // Use real polygon if available, fallback to bbox rectangle
            let polygon: serde_json::Value = if let Some(ref pj) = agg.polygon_json {
                serde_json::from_str(pj).unwrap_or_else(|_| serde_json::json!([
                    [agg.w, agg.n], [agg.e, agg.n], [agg.e, agg.s], [agg.w, agg.s], [agg.w, agg.n]
                ]))
            } else {
                serde_json::json!([
                    [agg.w, agg.n], [agg.e, agg.n], [agg.e, agg.s], [agg.w, agg.s], [agg.w, agg.n]
                ])
            };
            collections.entry(agg.collection).or_default().push(
                serde_json::json!({
                    "bbox": [agg.w, agg.s, agg.e, agg.n],
                    "polygon": polygon,
                    "tile_id": agg.tile_id,
                    "date_count": dates.len(),
                    "item_count": agg.item_count,
                    "dates": dates,
                    "bands": bands,
                    "node_count": agg.node_count,
                })
            );
        }
        let col_map: serde_json::Value = collections
            .into_iter()
            .map(|(k, v)| (k, serde_json::json!({ "cells": v })))
            .collect();
        Ok(serde_json::json!({
            "collections": col_map,
            "source": "beacon_aggregated",
        }))
    }

    /// Store stats fetched from a node.
    pub fn store_node_stats(
        &self,
        node_id: &str,
        stats: &serde_json::Value,
        coverage: &serde_json::Value,
    ) -> Result<()> {
        let now = now_ts();
        let total_items = coverage.get("total_items").and_then(|v| v.as_i64()).unwrap_or(0);
        let total_chunks = stats.get("total_chunks").and_then(|v| v.as_i64()).unwrap_or(0);
        let total_bytes = stats.get("total_bytes").and_then(|v| v.as_i64()).unwrap_or(0);
        let bytes_ingested = stats.get("bytes_ingested").and_then(|v| v.as_i64()).unwrap_or(0);
        let bytes_served = stats.get("bytes_served").and_then(|v| v.as_i64()).unwrap_or(0);
        let chunks_served = stats.get("chunks_served").and_then(|v| v.as_i64()).unwrap_or(0);
        let requests_total = stats.get("requests_total").and_then(|v| v.as_i64()).unwrap_or(0);
        let collections_json = coverage.get("collections")
            .map(|c| serde_json::to_string(c).unwrap_or_default())
            .unwrap_or_else(|| "[]".to_string());

        self.conn.execute(
            "INSERT INTO beacon_node_stats
                (node_id, updated_at, total_items, total_chunks, total_bytes,
                 bytes_ingested, bytes_served, chunks_served, requests_total, collections_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT(node_id) DO UPDATE SET
                updated_at = excluded.updated_at,
                total_items = excluded.total_items,
                total_chunks = excluded.total_chunks,
                total_bytes = excluded.total_bytes,
                bytes_ingested = excluded.bytes_ingested,
                bytes_served = excluded.bytes_served,
                chunks_served = excluded.chunks_served,
                requests_total = excluded.requests_total,
                collections_json = excluded.collections_json",
            params![
                node_id, now, total_items, total_chunks, total_bytes,
                bytes_ingested, bytes_served, chunks_served, requests_total, collections_json,
            ],
        )?;
        Ok(())
    }

    /// Get consolidated grid stats from all nodes.
    pub fn get_grid_node_stats(&self) -> Result<serde_json::Value> {
        let mut stmt = self.conn.prepare(
            "SELECT s.node_id, n.node_name, n.url, s.updated_at,
                    s.total_items, s.total_chunks, s.total_bytes,
                    s.bytes_ingested, s.bytes_served, s.chunks_served,
                    s.requests_total, s.collections_json
             FROM beacon_node_stats s
             LEFT JOIN beacon_nodes n ON s.node_id = n.node_id
             ORDER BY s.total_bytes DESC"
        )?;
        let mut nodes = Vec::new();
        let rows = stmt.query_map([], |row| {
            let collections_json: String = row.get(11)?;
            let collections: serde_json::Value = serde_json::from_str(&collections_json)
                .unwrap_or(serde_json::json!([]));
            Ok(serde_json::json!({
                "node_id": row.get::<_, String>(0)?,
                "node_name": row.get::<_, String>(1).unwrap_or_default(),
                "url": row.get::<_, String>(2).unwrap_or_default(),
                "updated_at": row.get::<_, f64>(3)?,
                "total_items": row.get::<_, i64>(4)?,
                "total_chunks": row.get::<_, i64>(5)?,
                "total_bytes": row.get::<_, i64>(6)?,
                "total_gb": row.get::<_, i64>(6)? as f64 / 1_073_741_824.0,
                "bytes_ingested": row.get::<_, i64>(7)?,
                "bytes_served": row.get::<_, i64>(8)?,
                "chunks_served": row.get::<_, i64>(9)?,
                "requests_total": row.get::<_, i64>(10)?,
                "collections": collections,
            }))
        })?;
        for row in rows {
            if let Ok(n) = row { nodes.push(n); }
        }

        // Totals
        let total_items: i64 = nodes.iter().filter_map(|n| n["total_items"].as_i64()).sum();
        let total_bytes: i64 = nodes.iter().filter_map(|n| n["total_bytes"].as_i64()).sum();
        let total_served: i64 = nodes.iter().filter_map(|n| n["bytes_served"].as_i64()).sum();
        let total_requests: i64 = nodes.iter().filter_map(|n| n["requests_total"].as_i64()).sum();

        Ok(serde_json::json!({
            "nodes": nodes,
            "node_count": nodes.len(),
            "totals": {
                "items": total_items,
                "bytes": total_bytes,
                "gb": total_bytes as f64 / 1_073_741_824.0,
                "bytes_served": total_served,
                "gb_served": total_served as f64 / 1_073_741_824.0,
                "requests": total_requests,
            }
        }))
    }

    /// Count tiles stored for a node.
    pub fn node_tile_count(&self, node_id: &str) -> Result<usize> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM beacon_node_tiles WHERE node_id = ?1",
            params![node_id],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Check if stats exist for a node.
    pub fn has_node_stats(&self, node_id: &str) -> bool {
        self.conn.query_row(
            "SELECT COUNT(*) FROM beacon_node_stats WHERE node_id = ?1",
            params![node_id],
            |row| row.get::<_, i64>(0),
        ).unwrap_or(0) > 0
    }

    /// Remove tiles for a node (called when node is pruned).
    pub fn remove_node_tiles(&self, node_id: &str) -> Result<usize> {
        let affected = self.conn.execute(
            "DELETE FROM beacon_node_tiles WHERE node_id = ?1",
            params![node_id],
        )?;
        Ok(affected)
    }

    /// Federated upsert: insert or update a node from a remote beacon.
    /// Bypasses URL-conflict checks (the remote beacon is authoritative) — but
    /// only for nodes this beacon holds no pin for. A federated event carries
    /// no node signature, so for a pinned node_id it may refresh the counters
    /// of an existing row and nothing else: url, node_name and the sponsor /
    /// group fields change only through the node's own signed requests, and a
    /// retired pinned node is not re-created from a peer's word. A row that
    /// would take the node_name of a different pinned node is not created either.
    pub fn federated_upsert(&self, node: &BeaconNode) -> Result<()> {
        let collections_json = serde_json::to_string(&node.collections)?;
        if self.pin(&node.node_id)?.is_some() {
            self.conn.execute(
                "UPDATE beacon_nodes SET
                    collections_json = ?1,
                    item_count = ?2,
                    chunk_count = ?3,
                    chunks_bytes = ?4,
                    can_source = ?5,
                    storage_limit_gb = ?6,
                    last_seen = ?7,
                    uptime_seconds = ?8
                 WHERE node_id = ?9",
                rusqlite::params![
                    collections_json,
                    node.item_count,
                    node.chunk_count,
                    node.chunks_bytes,
                    node.can_source as i64,
                    node.storage_limit_gb,
                    node.last_seen,
                    node.uptime_seconds,
                    node.node_id,
                ],
            )?;
            return Ok(());
        }
        if self.name_held_by_other_pinned_node(&node.node_name, &node.node_id)? {
            return Ok(());
        }
        self.conn.execute(
            "INSERT INTO beacon_nodes
                (node_id, node_name, url, collections_json, item_count, chunk_count, chunks_bytes,
                 can_source, storage_limit_gb, last_seen, sponsor_name, sponsor_url, node_url, group_id, uptime_seconds)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
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
                group_id = excluded.group_id,
                uptime_seconds = excluded.uptime_seconds",
            rusqlite::params![
                node.node_id,
                node.node_name,
                node.url,
                collections_json,
                node.item_count,
                node.chunk_count,
                node.chunks_bytes,
                node.can_source as i64,
                node.storage_limit_gb,
                node.last_seen,
                node.sponsor_name,
                node.sponsor_url,
                node.node_url,
                node.group_id,
                node.uptime_seconds,
            ],
        )?;
        Ok(())
    }

    /// Record a point-in-time snapshot of grid-wide metrics.
    /// Called periodically (e.g. every heartbeat or every N minutes).
    pub fn record_grid_snapshot(&self) -> Result<()> {
        let now = now_ts();
        // Only record if last snapshot is > 10 min old
        let last: f64 = self.conn.query_row(
            "SELECT COALESCE(MAX(ts), 0.0) FROM grid_metrics", [], |r| r.get(0)
        ).unwrap_or(0.0);
        if now - last < 600.0 { return Ok(()); }

        self.conn.execute(
            "INSERT INTO grid_metrics (ts, nodes_total, nodes_alive, total_items, total_chunks, total_bytes, total_storage_limit_gb)
             SELECT ?1,
                    COUNT(*),
                    SUM(CASE WHEN (?1 - last_seen) < 3600 THEN 1 ELSE 0 END),
                    SUM(item_count),
                    SUM(chunk_count),
                    SUM(chunks_bytes),
                    SUM(storage_limit_gb)
             FROM beacon_nodes",
            rusqlite::params![now],
        )?;
        // Keep max 1 year of data (~52k rows at 10-min intervals)
        let cutoff = now - 365.0 * 86400.0;
        self.conn.execute("DELETE FROM grid_metrics WHERE ts < ?1", rusqlite::params![cutoff])?;
        Ok(())
    }

    /// Get grid metrics time series for the given number of days.
    pub fn get_grid_metrics(&self, days: f64) -> Result<Vec<GridMetricPoint>> {
        let cutoff = now_ts() - days * 86400.0;
        let mut stmt = self.conn.prepare(
            "SELECT ts, nodes_total, nodes_alive, total_items, total_chunks, total_bytes, total_storage_limit_gb
             FROM grid_metrics WHERE ts >= ?1 ORDER BY ts ASC"
        )?;
        let rows = stmt.query_map(rusqlite::params![cutoff], |row| {
            Ok(GridMetricPoint {
                ts: row.get(0)?,
                nodes_total: row.get(1)?,
                nodes_alive: row.get(2)?,
                total_items: row.get(3)?,
                total_chunks: row.get(4)?,
                total_bytes: row.get(5)?,
                total_storage_limit_gb: row.get(6)?,
            })
        })?;
        let mut result = Vec::new();
        for r in rows { result.push(r?); }
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Axum handlers
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct BeaconState {
    pub registry: Arc<Mutex<BeaconRegistry>>,
    pub federation: Option<FederationState>,
    pub auth: AuthConfig,
    /// Credential for beacon-to-beacon federation. Separate from `auth` on
    /// purpose: a federated peer gets registry sync only, not the node API.
    pub federation_auth: crate::beacon_federation::FederationAuth,
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
    // Fail closed: no signature, a bad signature, a stale timestamp or a key
    // other than the pinned one all end here. Unsigned registration is what
    // let anyone repoint a node_id at their own server.
    // The signature covers the whole body under the register domain, so
    // every field applied below is one the key holder signed.
    let message = register_message(&req);
    if let Err(msg) = registry.authenticate(
        &req.node_id,
        &message,
        req.timestamp,
        req.public_key.as_deref(),
        req.signature.as_deref(),
    ) {
        return err(StatusCode::UNAUTHORIZED, msg).into_response();
    }
    match registry.register(&req) {
        Ok(node) => {
            // First signed contact pins the key to this node_id; every accepted
            // request moves the node's last accepted timestamp forward.
            if let Err(e) = registry
                .pin_public_key(&node.node_id, req.public_key.as_deref().unwrap_or(""))
                .and_then(|_| registry.record_accepted_timestamp(&node.node_id, req.timestamp.unwrap_or(0), &message))
            {
                return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response();
            }
            // First registration → fetch coverage
            let node_url = node.url.clone();
            let node_id = node.node_id.clone();
            let reg_clone = state.registry.clone();
            tokio::spawn(async move {
                fetch_and_store_coverage(&reg_clone, &node_id, &node_url).await;
            });
            (StatusCode::CREATED, Json(serde_json::to_value(node).unwrap())).into_response()
        }
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

    // Always refresh coverage on heartbeat.
    // Skip HTTP round-trip for the beacon's own node — read local catalog directly.
    let needs_coverage = true;

    let registry = state.registry.lock().await;
    // Fail closed, as in register_node: a heartbeat can change url, node_name,
    // collections and the counters, so all of them are under the signature —
    // in the heartbeat domain, which a register signature does not satisfy. A
    // different public key for an existing node_id is rejected.
    let message = heartbeat_message(&req);
    if let Err(msg) = registry.authenticate(
        &req.node_id,
        &message,
        req.timestamp,
        req.public_key.as_deref(),
        req.signature.as_deref(),
    ) {
        return err(StatusCode::UNAUTHORIZED, msg).into_response();
    }
    match registry.heartbeat(&req) {
        Ok(Some(node)) => {
            // First signed contact pins the key (rows that predate signing);
            // every accepted request moves the last accepted timestamp forward.
            if let Err(e) = registry
                .pin_public_key(&node.node_id, req.public_key.as_deref().unwrap_or(""))
                .and_then(|_| registry.record_accepted_timestamp(&node.node_id, req.timestamp.unwrap_or(0), &message))
            {
                return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response();
            }
            // Spawn async coverage fetch if version changed or no tiles yet
            if needs_coverage {
                let node_url = node.url.clone();
                let node_id = node.node_id.clone();
                let reg_clone = state.registry.clone();
                tokio::spawn(async move {
                    fetch_and_store_coverage(&reg_clone, &node_id, &node_url).await;
                });
            }
            // Include known beacons in response so nodes can discover new beacons
            let known_beacons = registry.known_beacon_urls();
            let mut resp = serde_json::to_value(&node).unwrap();
            resp["known_beacons"] = serde_json::json!(known_beacons);
            (StatusCode::OK, Json(resp)).into_response()
        }
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
    headers: HeaderMap,
    Path(node_id): Path<String>,
) -> impl IntoResponse {
    if let Some(key) = api_key(&headers) {
        if let Err(e) = state.auth.check_admin(Some(key)) {
            return err(StatusCode::UNAUTHORIZED, &e.to_string()).into_response();
        }
    } else if state.auth.is_enabled() {
        return err(StatusCode::UNAUTHORIZED, "Admin key required").into_response();
    }
    let registry = state.registry.lock().await;
    match registry.remove(&node_id) {
        Ok(true) => (StatusCode::OK, Json(serde_json::json!({"status": "removed", "node_id": node_id}))).into_response(),
        Ok(false) => err(StatusCode::NOT_FOUND, "Node not found").into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Async coverage fetch helper
// ---------------------------------------------------------------------------

/// Fetch /coverage/spatial and /stats from a node and store in the beacon DB.
async fn fetch_and_store_coverage(
    registry: &Arc<Mutex<BeaconRegistry>>,
    node_id: &str,
    node_url: &str,
) {
    let base = node_url.trim_end_matches('/');
    let nid = &node_id[..8.min(node_id.len())];

    // Outbound URL policy — before any request is made. The node chose this
    // URL, and any keypair can register: a private address is reachable only
    // if the beacon's operator configured that host as a peer. A signed
    // registration does not grant the exception to itself.
    let known_hosts = crate::url_policy::operator_hosts();
    if let Err(e) = crate::url_policy::validate_outbound_url_async(node_url, known_hosts).await {
        eprintln!("⚠️  Coverage fetch skipped for {}: {}", nid, e);
        return;
    }

    // No fallback to `Client::default()`: that client follows redirects.
    let Ok(client) = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
    else {
        eprintln!("⚠️  Coverage fetch skipped for {}: HTTP client could not be built", nid);
        return;
    };

    // Fetch spatial coverage
    let coverage_url = format!("{}/api/coverage/spatial?source=local", base);
    let coverage = match client.get(&coverage_url).send().await {
        Ok(resp) if resp.status().is_success() => {
            match resp.json::<serde_json::Value>().await {
                Ok(c) => Some(c),
                Err(e) => { eprintln!("⚠️  Coverage parse failed for {}: {}", nid, e); None }
            }
        }
        Ok(resp) => { eprintln!("⚠️  Coverage fetch {} returned {}", nid, resp.status()); None }
        Err(e) => { eprintln!("⚠️  Coverage fetch failed for {}: {}", nid, e); None }
    };

    if let Some(ref cov) = coverage {
        let reg = registry.lock().await;
        match reg.store_node_tiles(node_id, cov) {
            Ok(n) => println!("🗺️  Coverage sync: {} tiles for {}", n, nid),
            Err(e) => eprintln!("⚠️  Coverage store failed for {}: {}", nid, e),
        }
    }

    // Fetch stats + stats/coverage
    let stats_url = format!("{}/api/stats", base);
    let stats_cov_url = format!("{}/api/stats/coverage", base);

    let stats = match client.get(&stats_url).send().await {
        Ok(resp) if resp.status().is_success() => resp.json::<serde_json::Value>().await.ok(),
        _ => None,
    };
    let stats_coverage = match client.get(&stats_cov_url).send().await {
        Ok(resp) if resp.status().is_success() => resp.json::<serde_json::Value>().await.ok(),
        _ => None,
    };

    if let (Some(ref s), Some(ref sc)) = (&stats, &stats_coverage) {
        let reg = registry.lock().await;
        match reg.store_node_stats(node_id, s, sc) {
            Ok(()) => println!("📊 Stats sync: {} updated", nid),
            Err(e) => eprintln!("⚠️  Stats store failed for {}: {}", nid, e),
        }
    }
}

// ---------------------------------------------------------------------------
// Router factory
// ---------------------------------------------------------------------------

/// Build the beacon router. Mount at "/" or nest under a prefix.
#[derive(Debug, Deserialize)]
pub struct MetricsQuery {
    pub days: Option<f64>,
}

async fn grid_metrics(
    State(state): State<BeaconState>,
    axum::extract::Query(q): axum::extract::Query<MetricsQuery>,
) -> Json<serde_json::Value> {
    let days = q.days.unwrap_or(30.0);
    let reg = state.registry.lock().await;
    match reg.get_grid_metrics(days) {
        Ok(points) => Json(serde_json::json!({
            "days": days,
            "count": points.len(),
            "metrics": points,
        })),
        Err(e) => Json(serde_json::json!({
            "error": format!("{}", e),
            "metrics": [],
        })),
    }
}

async fn grid_node_stats(
    State(state): State<BeaconState>,
) -> impl IntoResponse {
    let reg = state.registry.lock().await;
    match reg.get_grid_node_stats() {
        Ok(stats) => (StatusCode::OK, Json(stats)).into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

pub fn beacon_router(state: BeaconState) -> Router {
    Router::new()
        .route("/api/beacon/register", post(register_node))
        .route("/api/beacon/heartbeat", post(heartbeat_node))
        .route("/api/beacon/nodes", get(list_nodes))
        .route("/api/beacon/nodes/{node_id}", get(get_node))
        .route("/api/beacon/nodes/{node_id}", delete(remove_node))
        .route("/api/beacon/metrics", get(grid_metrics))
        .route("/api/beacon/grid-stats", get(grid_node_stats))
        .route("/api/beacon/ws", axum::routing::any(crate::beacon_federation::ws_handler))
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
            catalog_version: None,
            public_key: None,
            signature: None,
            timestamp: None,
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
            catalog_version: None,
            public_key: None,
            signature: None,
            timestamp: None,
        };
        reg.register(&req).unwrap();

        let hb = HeartbeatRequest {
            node_id: "node-2".to_string(),
            url: None,
            node_name: None,
            item_count: Some(42),
            chunk_count: Some(200),
            chunks_bytes: Some(512_000),
            uptime_seconds: Some(3600),
            collections: None,
            can_source: None,
            storage_limit_gb: None,
            catalog_version: None,
            public_key: None,
            signature: None,
            timestamp: None,
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
                catalog_version: None,
                public_key: None,
                signature: None,
                timestamp: None,
            }).unwrap();
        }
        let all = reg.list(false).unwrap();
        assert_eq!(all.len(), 3);

        assert!(reg.remove("n-0").unwrap());
        assert_eq!(reg.list(false).unwrap().len(), 2);
        assert!(!reg.remove("n-0").unwrap()); // already gone
    }

    fn test_identity() -> (NodeIdentity, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let id = NodeIdentity::load_or_generate(&dir.path().join(".node_key")).unwrap();
        (id, dir)
    }

    fn unsigned_register(node_id: &str, url: &str, name: Option<&str>, ts: u64) -> RegisterRequest {
        RegisterRequest {
            node_id: node_id.to_string(),
            node_name: name.map(|n| n.to_string()),
            url: url.to_string(),
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
            catalog_version: None,
            public_key: None,
            signature: None,
            timestamp: Some(ts),
        }
    }

    fn unsigned_heartbeat(node_id: &str, ts: u64) -> HeartbeatRequest {
        HeartbeatRequest {
            node_id: node_id.to_string(),
            url: None,
            node_name: None,
            item_count: None,
            chunk_count: None,
            chunks_bytes: None,
            uptime_seconds: None,
            collections: None,
            can_source: None,
            storage_limit_gb: None,
            catalog_version: None,
            public_key: None,
            signature: None,
            timestamp: Some(ts),
        }
    }

    fn sign_register(id: &NodeIdentity, req: &mut RegisterRequest) {
        let (public_key, signature) = sign_request(id, &register_message(req));
        req.public_key = Some(public_key);
        req.signature = Some(signature);
    }

    fn sign_heartbeat(id: &NodeIdentity, req: &mut HeartbeatRequest) {
        let (public_key, signature) = sign_request(id, &heartbeat_message(req));
        req.public_key = Some(public_key);
        req.signature = Some(signature);
    }

    fn signed_register(id: &NodeIdentity, node_id: &str, url: &str, name: &str) -> RegisterRequest {
        let mut req = unsigned_register(node_id, url, Some(name), now_ts() as u64);
        sign_register(id, &mut req);
        req
    }

    fn auth_register(reg: &BeaconRegistry, req: &RegisterRequest) -> std::result::Result<(), &'static str> {
        reg.authenticate(&req.node_id, &register_message(req), req.timestamp, req.public_key.as_deref(), req.signature.as_deref())
    }

    fn auth_heartbeat(reg: &BeaconRegistry, req: &HeartbeatRequest) -> std::result::Result<(), &'static str> {
        reg.authenticate(&req.node_id, &heartbeat_message(req), req.timestamp, req.public_key.as_deref(), req.signature.as_deref())
    }

    /// Register `req` the way the handler does: authenticate, apply, pin, record.
    fn accept_register(reg: &BeaconRegistry, req: &RegisterRequest) {
        auth_register(reg, req).unwrap();
        reg.register(req).unwrap();
        reg.pin_public_key(&req.node_id, req.public_key.as_deref().unwrap()).unwrap();
        reg.record_accepted_timestamp(&req.node_id, req.timestamp.unwrap(), &register_message(req)).unwrap();
    }

    fn federated_node(node_id: &str, name: &str, url: &str) -> BeaconNode {
        BeaconNode {
            node_id: node_id.to_string(),
            node_name: name.to_string(),
            url: url.to_string(),
            collections: vec![],
            item_count: 7,
            chunk_count: 0,
            chunks_bytes: 0,
            can_source: false,
            storage_limit_gb: 0.0,
            last_seen: now_ts() + 10.0,
            sponsor_name: None,
            sponsor_url: None,
            node_url: None,
            group_id: None,
            uptime_seconds: 0,
            catalog_version: 0,
            alive: true,
        }
    }

    #[test]
    fn test_signed_message_has_no_field_boundary_confusion() {
        let msg = |node_id: &str, url: &str, name: Option<&str>| register_message(&unsigned_register(node_id, url, name, 1));
        assert_ne!(msg("ab", "c", Some("n")), msg("a", "bc", Some("n")));
        assert_ne!(msg("a", "b\nnode_name=1:c", None), msg("a", "b", Some("c")));
        assert!(msg("a", "b", Some("n")).starts_with(REGISTER_DOMAIN));

        // An absent field and an explicit empty string are different messages
        assert_ne!(msg("a", "b", None), msg("a", "b", Some("")));
        let mut hb = unsigned_heartbeat("a", 1);
        let absent = heartbeat_message(&hb);
        hb.url = Some(String::new());
        assert_ne!(absent, heartbeat_message(&hb));
        assert!(absent.starts_with(HEARTBEAT_DOMAIN));

        // Lists: absent, empty, and different splits of the same characters
        let list = |c: Option<Vec<&str>>| {
            let mut req = unsigned_register("a", "b", None, 1);
            req.collections = c.map(|v| v.into_iter().map(String::from).collect());
            register_message(&req)
        };
        assert_ne!(list(None), list(Some(vec![])));
        assert_ne!(list(Some(vec!["ab"])), list(Some(vec!["a", "b"])));
        assert_ne!(list(Some(vec!["a][1:b"])), list(Some(vec!["a", "b"])));
    }

    #[test]
    fn test_verify_request_fails_closed() {
        let (id, _dir) = test_identity();
        let ts = now_ts() as u64;
        let message = |node_id: &str, url: &str, name: &str, ts: u64| {
            register_message(&unsigned_register(node_id, url, Some(name), ts))
        };
        let good = message("n1", "http://node.example:8400", "alpha", ts);
        let (pk, sig) = sign_request(&id, &good);

        assert!(verify_request(&good, Some(ts), Some(&pk), Some(&sig)).is_ok());

        // Unsigned or partially signed
        assert!(verify_request(&good, None, None, None).is_err());
        assert!(verify_request(&good, Some(ts), Some(&pk), None).is_err());
        assert!(verify_request(&good, Some(ts), None, Some(&sig)).is_err());
        assert!(verify_request(&good, None, Some(&pk), Some(&sig)).is_err());

        // Any signed field changed
        assert!(verify_request(&message("n2", "http://node.example:8400", "alpha", ts), Some(ts), Some(&pk), Some(&sig)).is_err());
        assert!(verify_request(&message("n1", "http://evil.example:8400", "alpha", ts), Some(ts), Some(&pk), Some(&sig)).is_err());
        assert!(verify_request(&message("n1", "http://node.example:8400", "beta", ts), Some(ts), Some(&pk), Some(&sig)).is_err());
        assert!(verify_request(&message("n1", "http://node.example:8400", "alpha", ts + 1), Some(ts + 1), Some(&pk), Some(&sig)).is_err());

        // Validly signed but outside the replay window (both directions)
        for stale in [ts - REPLAY_WINDOW_SECS - 5, ts + REPLAY_WINDOW_SECS + 5] {
            let old = message("n1", "http://node.example:8400", "alpha", stale);
            let (pk, sig) = sign_request(&id, &old);
            assert!(verify_request(&old, Some(stale), Some(&pk), Some(&sig)).is_err());
        }
    }

    #[test]
    fn test_signature_covers_every_mutable_field() {
        let reg = BeaconRegistry::in_memory().unwrap();
        let (id, _dir) = test_identity();
        let ts = now_ts() as u64;

        let base = || {
            let mut req = unsigned_register("n1", "http://node.example:8400", Some("alpha"), ts);
            req.collections = Some(vec!["sentinel-2-l2a".to_string()]);
            req.storage_limit_gb = Some(50.0);
            req
        };
        let mut signed = base();
        sign_register(&id, &mut signed);
        assert!(auth_register(&reg, &signed).is_ok());

        for i in 0..13 {
            let mut req = base();
            req.public_key = signed.public_key.clone();
            req.signature = signed.signature.clone();
            match i {
                0 => req.collections = Some(vec!["everything".to_string()]),
                1 => req.collections = None,
                2 => req.item_count = Some(1_000_000),
                3 => req.chunk_count = Some(1),
                4 => req.chunks_bytes = Some(1),
                5 => req.can_source = Some(true),
                6 => req.storage_limit_gb = Some(9999.0),
                7 => req.sponsor_name = Some("evil".to_string()),
                8 => req.sponsor_url = Some("http://evil.example".to_string()),
                9 => req.node_url = Some("http://evil.example".to_string()),
                10 => req.group = Some("evil".to_string()),
                11 => req.catalog_version = Some(99),
                _ => req.node_name = None,
            }
            assert!(auth_register(&reg, &req).is_err(), "register tamper #{i} must break the signature");
        }

        let hb_base = || {
            let mut req = unsigned_heartbeat("n1", ts);
            req.item_count = Some(3);
            req
        };
        let mut hb = hb_base();
        sign_heartbeat(&id, &mut hb);
        assert!(auth_heartbeat(&reg, &hb).is_ok());
        for i in 0..10 {
            let mut req = hb_base();
            req.public_key = hb.public_key.clone();
            req.signature = hb.signature.clone();
            match i {
                0 => req.url = Some("http://evil.example:8400".to_string()),
                1 => req.node_name = Some("evil".to_string()),
                2 => req.item_count = Some(4),
                3 => req.chunk_count = Some(1),
                4 => req.chunks_bytes = Some(1),
                5 => req.uptime_seconds = Some(1),
                6 => req.collections = Some(vec![]),
                7 => req.can_source = Some(true),
                8 => req.storage_limit_gb = Some(1.0),
                _ => req.catalog_version = Some(1),
            }
            assert!(auth_heartbeat(&reg, &req).is_err(), "heartbeat tamper #{i} must break the signature");
        }
    }

    #[test]
    fn test_register_and_heartbeat_signatures_are_not_interchangeable() {
        let reg = BeaconRegistry::in_memory().unwrap();
        let (id, _dir) = test_identity();
        let ts = now_ts() as u64;

        // The same logical content, signed for the heartbeat endpoint...
        let mut hb = unsigned_heartbeat("n1", ts);
        hb.url = Some("http://node.example:8400".to_string());
        hb.node_name = Some("alpha".to_string());
        sign_heartbeat(&id, &mut hb);
        assert!(auth_heartbeat(&reg, &hb).is_ok());

        // ...replayed against register: rejected
        let mut as_register = unsigned_register("n1", "http://node.example:8400", Some("alpha"), ts);
        as_register.public_key = hb.public_key.clone();
        as_register.signature = hb.signature.clone();
        assert!(auth_register(&reg, &as_register).is_err());

        // ...and the other way round
        let register = signed_register(&id, "n1", "http://node.example:8400", "alpha");
        let mut as_heartbeat = unsigned_heartbeat("n1", register.timestamp.unwrap());
        as_heartbeat.url = Some(register.url.clone());
        as_heartbeat.node_name = register.node_name.clone();
        as_heartbeat.public_key = register.public_key.clone();
        as_heartbeat.signature = register.signature.clone();
        assert!(auth_heartbeat(&reg, &as_heartbeat).is_err());
    }

    #[test]
    fn test_public_key_is_pinned_on_first_signed_contact() {
        let reg = BeaconRegistry::in_memory().unwrap();
        let (owner, _d1) = test_identity();
        let (attacker, _d2) = test_identity();

        let req = signed_register(&owner, "n1", "http://node.example:8400", "alpha");
        assert!(auth_register(&reg, &req).is_ok());
        reg.register(&req).unwrap();
        reg.pin_public_key("n1", req.public_key.as_deref().unwrap()).unwrap();
        assert_eq!(reg.pinned_key("n1").unwrap(), Some(owner.public_key_hex()));

        // Another key, correctly signing a takeover of the same node_id: rejected
        let evil = signed_register(&attacker, "n1", "http://evil.example:8400", "alpha");
        assert!(auth_register(&reg, &evil).is_err());
        // ...and the pin cannot be replaced
        reg.pin_public_key("n1", &attacker.public_key_hex()).unwrap();
        assert_eq!(reg.pinned_key("n1").unwrap(), Some(owner.public_key_hex()));

        // The owner can still move its own URL
        let moved = signed_register(&owner, "n1", "http://node2.example:8400", "alpha");
        assert!(auth_register(&reg, &moved).is_ok());

        // An unknown node_id has no pin yet
        assert_eq!(reg.pinned_key("nope").unwrap(), None);
    }

    #[test]
    fn test_name_collision_cannot_erase_a_pin() {
        let reg = BeaconRegistry::in_memory().unwrap();
        let (victim, _d1) = test_identity();
        let (attacker, _d2) = test_identity();
        accept_register(&reg, &signed_register(&victim, "victim", "http://node.example:8400", "alpha"));
        accept_register(&reg, &signed_register(&attacker, "evil", "http://evil.example:8400", "beta"));

        // Taking the victim's name is refused on both endpoints
        assert!(reg.register(&signed_register(&attacker, "evil", "http://evil.example:8400", "alpha")).is_err());
        let mut rename = unsigned_heartbeat("evil", now_ts() as u64 + 1);
        rename.node_name = Some("alpha".to_string());
        sign_heartbeat(&attacker, &mut rename);
        assert!(auth_heartbeat(&reg, &rename).is_ok(), "validly signed — it is the registry that must refuse it");
        assert!(reg.heartbeat(&rename).is_err());

        // Even with the collision forced into the table (newer rows, pinned
        // and unpinned), dedup never deletes the pinned victim
        reg.conn.execute("UPDATE beacon_nodes SET node_name = 'alpha' WHERE node_id = 'evil'", []).unwrap();
        reg.conn.execute(
            "INSERT INTO beacon_nodes (node_id, node_name, url, last_seen) VALUES ('unpinned', 'alpha', 'http://x.example', ?1)",
            params![now_ts()],
        ).unwrap();
        reg.dedup_by_name().unwrap();
        assert!(reg.get("victim").unwrap().is_some(), "pinned row must survive dedup");
        assert!(reg.get("unpinned").unwrap().is_none(), "the unpinned duplicate is the one that goes");
        assert_eq!(reg.pinned_key("victim").unwrap(), Some(victim.public_key_hex()));

        // So the attacker still cannot claim the victim's node_id
        assert!(auth_register(&reg, &signed_register(&attacker, "victim", "http://evil.example:8400", "alpha")).is_err());
    }

    #[test]
    fn test_pin_survives_pruning_and_only_remove_clears_it() {
        let reg = BeaconRegistry::in_memory().unwrap();
        let (owner, _d1) = test_identity();
        let (attacker, _d2) = test_identity();
        accept_register(&reg, &signed_register(&owner, "n1", "http://node.example:8400", "alpha"));

        // The node goes quiet for more than an hour and is pruned
        reg.conn.execute("UPDATE beacon_nodes SET last_seen = ?1", params![now_ts() - 7200.0]).unwrap();
        assert_eq!(reg.prune_stale(3600.0).unwrap(), 1);
        assert!(reg.get("n1").unwrap().is_none());
        assert_eq!(reg.pinned_key("n1").unwrap(), Some(owner.public_key_hex()));

        // The id cannot be claimed afresh by another key
        let claim = signed_register(&attacker, "n1", "http://evil.example:8400", "alpha");
        assert!(auth_register(&reg, &claim).is_err());

        // Only the admin delete releases the pin
        assert!(reg.remove("n1").unwrap());
        assert_eq!(reg.pinned_key("n1").unwrap(), None);
        assert!(auth_register(&reg, &claim).is_ok());
    }

    #[test]
    fn test_federated_upsert_cannot_touch_a_pinned_identity() {
        let reg = BeaconRegistry::in_memory().unwrap();
        let (owner, _d1) = test_identity();
        accept_register(&reg, &signed_register(&owner, "n1", "http://node.example:8400", "alpha"));

        // Counters may be refreshed; url and node_name may not
        reg.federated_upsert(&federated_node("n1", "renamed", "http://evil.example:8400")).unwrap();
        let node = reg.get("n1").unwrap().unwrap();
        assert_eq!(node.url, "http://node.example:8400");
        assert_eq!(node.node_name, "alpha");
        assert_eq!(node.item_count, 7);

        // No new row may take a pinned node's name
        reg.federated_upsert(&federated_node("shadow", "alpha", "http://evil.example:8400")).unwrap();
        assert!(reg.get("shadow").unwrap().is_none());
        // Unrelated unpinned nodes still federate
        reg.federated_upsert(&federated_node("other", "gamma", "http://other.example:8400")).unwrap();
        assert!(reg.get("other").unwrap().is_some());

        // A retired pinned node is not re-created from a peer's word
        assert!(reg.retire("n1").unwrap());
        assert_eq!(reg.pinned_key("n1").unwrap(), Some(owner.public_key_hex()));
        reg.federated_upsert(&federated_node("n1", "alpha", "http://evil.example:8400")).unwrap();
        assert!(reg.get("n1").unwrap().is_none());
    }

    #[test]
    fn test_old_signature_cannot_be_replayed() {
        let reg = BeaconRegistry::in_memory().unwrap();
        let (owner, _dir) = test_identity();
        let ts = now_ts() as u64;

        let mut first = unsigned_register("n1", "http://node.example:8400", Some("alpha"), ts);
        sign_register(&owner, &mut first);
        accept_register(&reg, &first);

        // The very same request again — the node reaching this beacon through
        // a second URL — is an idempotent duplicate and is accepted
        assert!(auth_register(&reg, &first).is_ok());
        // The same timestamp carrying any other message is refused
        let mut same_ts = unsigned_register("n1", "http://node.example:8400", Some("alpha"), ts);
        same_ts.item_count = Some(1);
        sign_register(&owner, &mut same_ts);
        assert!(auth_register(&reg, &same_ts).is_err());
        let mut same_ts_hb = unsigned_heartbeat("n1", ts);
        sign_heartbeat(&owner, &mut same_ts_hb);
        assert!(auth_heartbeat(&reg, &same_ts_hb).is_err());
        // Anything older is refused — by the recorded timestamp, whatever the
        // beacon's clock says
        let mut older = unsigned_heartbeat("n1", ts - 1);
        sign_heartbeat(&owner, &mut older);
        assert!(auth_heartbeat(&reg, &older).is_err());

        // A newer one is accepted, and moves the mark forward
        let mut newer = unsigned_heartbeat("n1", ts + 1);
        sign_heartbeat(&owner, &mut newer);
        assert!(auth_heartbeat(&reg, &newer).is_ok());
        reg.record_accepted_timestamp("n1", ts + 1, &heartbeat_message(&newer)).unwrap();
        assert!(auth_heartbeat(&reg, &newer).is_ok(), "duplicate of the last accepted request");
        // ...which kills the previous one, duplicate or not
        assert!(auth_register(&reg, &first).is_err());
        // The mark never moves backwards, and neither does its digest
        reg.record_accepted_timestamp("n1", ts - 100, &heartbeat_message(&older)).unwrap();
        assert!(auth_heartbeat(&reg, &older).is_err());
        assert!(auth_heartbeat(&reg, &newer).is_ok());
    }

    #[test]
    fn test_storage_limit_is_stored_as_signed() {
        let reg = BeaconRegistry::in_memory().unwrap();
        let (owner, _dir) = test_identity();
        let ts = now_ts() as u64;

        // Two values in the same three-decimal bucket sign identically...
        let mut a = unsigned_register("n1", "http://node.example:8400", Some("alpha"), ts);
        a.storage_limit_gb = Some(100.0004);
        let mut b = unsigned_register("n1", "http://node.example:8400", Some("alpha"), ts);
        b.storage_limit_gb = Some(100.00012345);
        assert_eq!(register_message(&a), register_message(&b));

        // ...so both must store the one number the signature commits to
        sign_register(&owner, &mut a);
        accept_register(&reg, &a);
        assert_eq!(reg.get("n1").unwrap().unwrap().storage_limit_gb, 100.0);

        let mut hb = unsigned_heartbeat("n1", ts + 1);
        hb.storage_limit_gb = Some(250.12349);
        sign_heartbeat(&owner, &mut hb);
        assert!(auth_heartbeat(&reg, &hb).is_ok());
        assert_eq!(reg.heartbeat(&hb).unwrap().unwrap().storage_limit_gb, 250.123);
    }
}
