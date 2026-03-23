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
use crate::beacon_federation::FederationState;

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
            CREATE INDEX IF NOT EXISTS idx_beacon_last_seen ON beacon_nodes(last_seen);

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
                PRIMARY KEY (node_id, collection, tile_id)
            );
            CREATE INDEX IF NOT EXISTS idx_bnt_node ON beacon_node_tiles(node_id);"
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
        // Record grid-wide metrics snapshot (max every 10 min)
        let _ = self.record_grid_snapshot();

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
        if let Some(v) = req.storage_limit_gb { param_values.push(Box::new(v)); }
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
                        } else {
                            (0.0, 0.0, 0.0, 0.0)
                        };
                        let date_count = cell.get("date_count").and_then(|d| d.as_i64()).unwrap_or(0);
                        self.conn.execute(
                            "INSERT OR REPLACE INTO beacon_node_tiles
                                (node_id, collection, tile_id, bbox_west, bbox_south, bbox_east, bbox_north, date_count)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                            params![node_id, collection, tile_id, w, s, e, n, date_count],
                        )?;
                        count += 1;
                    }
                }
            }
        }
        Ok(count)
    }

    /// Get aggregated spatial coverage from all nodes.
    pub fn get_aggregated_coverage(&self) -> Result<serde_json::Value> {
        let mut stmt = self.conn.prepare(
            "SELECT collection, tile_id, bbox_west, bbox_south, bbox_east, bbox_north,
                    SUM(date_count) as total_dates, COUNT(DISTINCT node_id) as node_count
             FROM beacon_node_tiles
             GROUP BY collection, tile_id"
        )?;
        let mut collections: std::collections::HashMap<String, Vec<serde_json::Value>> =
            std::collections::HashMap::new();
        let rows = stmt.query_map([], |row| {
            let collection: String = row.get(0)?;
            let tile_id: String = row.get(1)?;
            let w: f64 = row.get(2)?;
            let s: f64 = row.get(3)?;
            let e: f64 = row.get(4)?;
            let n: f64 = row.get(5)?;
            let date_count: i64 = row.get(6)?;
            let node_count: i64 = row.get(7)?;
            Ok((collection, tile_id, w, s, e, n, date_count, node_count))
        })?;
        for row in rows {
            if let Ok((collection, tile_id, w, s, e, n, date_count, node_count)) = row {
                collections.entry(collection).or_default().push(
                    serde_json::json!({
                        "bbox": [w, s, e, n],
                        "tile_id": tile_id,
                        "date_count": date_count,
                        "node_count": node_count,
                    })
                );
            }
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

    /// Count tiles stored for a node.
    pub fn node_tile_count(&self, node_id: &str) -> Result<usize> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM beacon_node_tiles WHERE node_id = ?1",
            params![node_id],
            |row| row.get(0),
        )?;
        Ok(count as usize)
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
    /// Bypasses URL-conflict checks (the remote beacon is authoritative).
    pub fn federated_upsert(&self, node: &BeaconNode) -> Result<()> {
        let collections_json = serde_json::to_string(&node.collections)?;
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
        Ok(node) => {
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

    // Check if catalog_version changed or tiles are missing → need coverage refresh
    let (old_version, has_tiles) = {
        let reg = state.registry.lock().await;
        let cv = reg.get_catalog_version(&req.node_id);
        let tiles = reg.node_tile_count(&req.node_id).unwrap_or(0);
        (cv, tiles > 0)
    };
    let new_version = req.catalog_version;
    let version_changed = match (old_version, new_version) {
        (Some(old), Some(new)) => new != old,
        (None, Some(_)) => true, // first time seeing this node's version
        _ => false,
    };
    let needs_coverage = version_changed || !has_tiles;

    let registry = state.registry.lock().await;
    match registry.heartbeat(&req) {
        Ok(Some(node)) => {
            // Spawn async coverage fetch if version changed or no tiles yet
            if needs_coverage {
                let node_url = node.url.clone();
                let node_id = node.node_id.clone();
                let reg_clone = state.registry.clone();
                tokio::spawn(async move {
                    fetch_and_store_coverage(&reg_clone, &node_id, &node_url).await;
                });
            }
            (StatusCode::OK, Json(serde_json::to_value(node).unwrap())).into_response()
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
// Async coverage fetch helper
// ---------------------------------------------------------------------------

/// Fetch /coverage/spatial from a node and store tiles in the beacon DB.
async fn fetch_and_store_coverage(
    registry: &Arc<Mutex<BeaconRegistry>>,
    node_id: &str,
    node_url: &str,
) {
    let url = format!("{}/coverage/spatial?source=local", node_url.trim_end_matches('/'));
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .unwrap_or_default();

    match client.get(&url).send().await {
        Ok(resp) => {
            if resp.status().is_success() {
                match resp.json::<serde_json::Value>().await {
                    Ok(coverage) => {
                        let reg = registry.lock().await;
                        match reg.store_node_tiles(node_id, &coverage) {
                            Ok(n) => println!("🗺️  Coverage sync: {} tiles stored for node {}", n, &node_id[..8.min(node_id.len())]),
                            Err(e) => eprintln!("⚠️  Coverage store failed for {}: {}", &node_id[..8.min(node_id.len())], e),
                        }
                    }
                    Err(e) => eprintln!("⚠️  Coverage parse failed for {}: {}", &node_id[..8.min(node_id.len())], e),
                }
            } else {
                eprintln!("⚠️  Coverage fetch {} returned {}", &node_id[..8.min(node_id.len())], resp.status());
            }
        }
        Err(e) => {
            // Node may be behind NAT or unreachable — this is normal for some nodes
            eprintln!("⚠️  Coverage fetch failed for {}: {}", &node_id[..8.min(node_id.len())], e);
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

pub fn beacon_router(state: BeaconState) -> Router {
    Router::new()
        .route("/beacon/register", post(register_node))
        .route("/beacon/heartbeat", post(heartbeat_node))
        .route("/beacon/nodes", get(list_nodes))
        .route("/beacon/nodes/{node_id}", get(get_node))
        .route("/beacon/nodes/{node_id}", delete(remove_node))
        .route("/beacon/metrics", get(grid_metrics))
        .route("/beacon/ws", axum::routing::any(crate::beacon_federation::ws_handler))
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
            }).unwrap();
        }
        let all = reg.list(false).unwrap();
        assert_eq!(all.len(), 3);

        assert!(reg.remove("n-0").unwrap());
        assert_eq!(reg.list(false).unwrap().len(), 2);
        assert!(!reg.remove("n-0").unwrap()); // already gone
    }
}
