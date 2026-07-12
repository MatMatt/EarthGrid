//! Statistics engine for EarthGrid — SQLite-backed access, bandwidth, and uptake logging.
//!
//! Schema is kept identical to the Python stats.py for compatibility:
//! both Rust and Python processes may read/write the same DB concurrently.
//! WAL mode + busy_timeout handle concurrent access safely.

use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Mutex;

use crate::error::Result;

// ---------------------------------------------------------------------------
// Return types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct DownloadStats {
    pub period_hours: u64,
    pub total_bytes: i64,
    pub total_downloads: i64,
    pub by_provider: Vec<ProviderStat>,
    pub by_collection: Vec<CollectionStat>,
    pub daily_trend: Vec<DailyStat>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProviderStat {
    pub provider: String,
    pub downloads: i64,
    pub bytes: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CollectionStat {
    pub collection_id: String,
    pub downloads: i64,
    pub bytes: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DailyStat {
    pub date: String,
    pub downloads: i64,
    pub bytes: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HotChunk {
    pub chunk_sha: String,
    pub access_count: i64,
    pub collection_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicationAdvice {
    pub collection_id: String,
    pub access_count: i64,
    pub tier: String, // "hot" | "warm" | "cold"
    pub advice: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct IngestHistory {
    pub period_days: u64,
    pub total_bytes: i64,
    pub total_items: i64,
    #[serde(rename = "total_gb_fetched")]
    pub total_gb_fetched: f64,
    #[serde(rename = "total_items_fetched")]
    pub total_items_fetched: i64,
    pub daily: Vec<DailyIngest>,
    pub hourly: Vec<HourlyIngest>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DailyIngest {
    pub date: String,
    pub items: i64,
    pub bytes: i64,
    pub gb: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HourlyIngest {
    pub hour: String,
    pub items: i64,
    pub bytes: i64,
}

// ---------------------------------------------------------------------------
// StatsEngine
// ---------------------------------------------------------------------------

pub struct StatsEngine {
    conn: Mutex<Connection>,
}

impl StatsEngine {
    /// Open or create the stats DB, initialise tables, enable WAL.
    pub fn new(db_path: &Path) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(db_path)?;
        // WAL for concurrent Python/Rust access; busy_timeout avoids SQLITE_BUSY
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA busy_timeout=10000;
             PRAGMA synchronous=NORMAL;",
        )?;
        let engine = Self { conn: Mutex::new(conn) };
        engine.init_tables()?;
        Ok(engine)
    }

    fn init_tables(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS chunk_access (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                chunk_sha       TEXT    NOT NULL,
                timestamp       REAL    NOT NULL DEFAULT (strftime('%s','now')),
                access_type     TEXT    NOT NULL DEFAULT 'get',
                node_id         TEXT,
                collection_id   TEXT,
                item_id         TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_ca_sha       ON chunk_access(chunk_sha);
            CREATE INDEX IF NOT EXISTS idx_ca_ts        ON chunk_access(timestamp);
            CREATE INDEX IF NOT EXISTS idx_ca_col       ON chunk_access(collection_id);

            CREATE TABLE IF NOT EXISTS collection_access (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                collection_id   TEXT    NOT NULL,
                timestamp       REAL    NOT NULL DEFAULT (strftime('%s','now')),
                access_type     TEXT    NOT NULL DEFAULT 'search',
                query_bbox      TEXT,
                query_time_range TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_coa_col      ON collection_access(collection_id);
            CREATE INDEX IF NOT EXISTS idx_coa_ts       ON collection_access(timestamp);

            CREATE TABLE IF NOT EXISTS bandwidth_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp       REAL    NOT NULL DEFAULT (strftime('%s','now')),
                node_id         TEXT,
                direction       TEXT    NOT NULL DEFAULT 'out',
                bytes_transferred INTEGER NOT NULL DEFAULT 0,
                nice_level      INTEGER NOT NULL DEFAULT 0,
                source_user_id  TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_bw_ts        ON bandwidth_log(timestamp);
            CREATE INDEX IF NOT EXISTS idx_bw_node      ON bandwidth_log(node_id);

            CREATE TABLE IF NOT EXISTS download_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp       REAL    NOT NULL DEFAULT (strftime('%s','now')),
                origin          TEXT,
                provider        TEXT,
                collection_id   TEXT,
                item_id         TEXT,
                bytes_transferred INTEGER NOT NULL DEFAULT 0,
                bbox            TEXT,
                client_ip       TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_dl_ts        ON download_log(timestamp);
            CREATE INDEX IF NOT EXISTS idx_dl_col       ON download_log(collection_id);
            CREATE INDEX IF NOT EXISTS idx_dl_provider  ON download_log(provider);

            CREATE TABLE IF NOT EXISTS uptake_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp       REAL    NOT NULL DEFAULT (strftime('%s','now')),
                collection_id   TEXT    NOT NULL,
                job_type        TEXT,
                bbox            TEXT,
                temporal_extent TEXT,
                bytes_out       INTEGER NOT NULL DEFAULT 0,
                process_ids     TEXT,
                bbox_km2        REAL
            );
            CREATE INDEX IF NOT EXISTS idx_ul_ts        ON uptake_log(timestamp);
            CREATE INDEX IF NOT EXISTS idx_ul_col       ON uptake_log(collection_id);

            CREATE TABLE IF NOT EXISTS perf_minutes (
                ts              REAL    NOT NULL,
                endpoint        TEXT    NOT NULL,
                count           INTEGER NOT NULL DEFAULT 0,
                bytes           INTEGER NOT NULL DEFAULT 0,
                sum_us          INTEGER NOT NULL DEFAULT 0,
                max_us          INTEGER NOT NULL DEFAULT 0,
                b0  INTEGER NOT NULL DEFAULT 0, b1  INTEGER NOT NULL DEFAULT 0,
                b2  INTEGER NOT NULL DEFAULT 0, b3  INTEGER NOT NULL DEFAULT 0,
                b4  INTEGER NOT NULL DEFAULT 0, b5  INTEGER NOT NULL DEFAULT 0,
                b6  INTEGER NOT NULL DEFAULT 0, b7  INTEGER NOT NULL DEFAULT 0,
                b8  INTEGER NOT NULL DEFAULT 0, b9  INTEGER NOT NULL DEFAULT 0,
                b10 INTEGER NOT NULL DEFAULT 0, b11 INTEGER NOT NULL DEFAULT 0,
                b12 INTEGER NOT NULL DEFAULT 0, b13 INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_pm_ts ON perf_minutes(ts);",
        )?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Record methods
    // -----------------------------------------------------------------------

    pub fn record_chunk_access(
        &self,
        sha: &str,
        access_type: &str,
        node_id: Option<&str>,
        collection_id: Option<&str>,
        item_id: Option<&str>,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO chunk_access (chunk_sha, access_type, node_id, collection_id, item_id)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![sha, access_type, node_id, collection_id, item_id],
        )?;
        Ok(())
    }

    pub fn record_collection_access(
        &self,
        collection_id: &str,
        access_type: &str,
        bbox: Option<&str>,
        time_range: Option<&str>,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO collection_access (collection_id, access_type, query_bbox, query_time_range)
             VALUES (?1, ?2, ?3, ?4)",
            params![collection_id, access_type, bbox, time_range],
        )?;
        Ok(())
    }

    pub fn record_bandwidth(
        &self,
        bytes: i64,
        direction: &str,
        nice_level: i64,
        node_id: Option<&str>,
        source_user_id: Option<&str>,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO bandwidth_log (bytes_transferred, direction, nice_level, node_id, source_user_id)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![bytes, direction, nice_level, node_id, source_user_id],
        )?;
        Ok(())
    }

    pub fn record_download(
        &self,
        origin: Option<&str>,
        collection_id: Option<&str>,
        item_id: Option<&str>,
        bytes: i64,
        provider: Option<&str>,
        bbox: Option<&str>,
        client_ip: Option<&str>,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO download_log (origin, collection_id, item_id, bytes_transferred, provider, bbox, client_ip)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![origin, collection_id, item_id, bytes, provider, bbox, client_ip],
        )?;
        Ok(())
    }

    pub fn record_uptake(
        &self,
        collection_id: &str,
        job_type: Option<&str>,
        bbox: Option<&str>,
        temporal_extent: Option<&str>,
        bytes_out: i64,
        process_ids: Option<&str>,
        bbox_km2: Option<f64>,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO uptake_log (collection_id, job_type, bbox, temporal_extent, bytes_out, process_ids, bbox_km2)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![collection_id, job_type, bbox, temporal_extent, bytes_out, process_ids, bbox_km2],
        )?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Query methods
    // -----------------------------------------------------------------------

    /// Aggregate download stats for the last `period_hours` hours.
    pub fn download_stats(&self, period_hours: u64) -> Result<DownloadStats> {
        let cutoff = unix_now() - (period_hours as f64 * 3600.0);
        let conn = self.conn.lock().unwrap();

        // Totals
        let (total_bytes, total_downloads): (i64, i64) = conn.query_row(
            "SELECT COALESCE(SUM(bytes_transferred), 0), COUNT(*) FROM download_log WHERE timestamp >= ?1",
            params![cutoff],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;

        // By provider
        let mut stmt = conn.prepare(
            "SELECT COALESCE(provider,'unknown'), COUNT(*), COALESCE(SUM(bytes_transferred),0)
             FROM download_log WHERE timestamp >= ?1
             GROUP BY provider ORDER BY COUNT(*) DESC",
        )?;
        let by_provider: Vec<ProviderStat> = stmt
            .query_map(params![cutoff], |row| {
                Ok(ProviderStat {
                    provider: row.get(0)?,
                    downloads: row.get(1)?,
                    bytes: row.get(2)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();

        // By collection
        let mut stmt = conn.prepare(
            "SELECT COALESCE(collection_id,'unknown'), COUNT(*), COALESCE(SUM(bytes_transferred),0)
             FROM download_log WHERE timestamp >= ?1
             GROUP BY collection_id ORDER BY COUNT(*) DESC",
        )?;
        let by_collection: Vec<CollectionStat> = stmt
            .query_map(params![cutoff], |row| {
                Ok(CollectionStat {
                    collection_id: row.get(0)?,
                    downloads: row.get(1)?,
                    bytes: row.get(2)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();

        // Daily trend
        let mut stmt = conn.prepare(
            "SELECT date(timestamp,'unixepoch') AS d, COUNT(*), COALESCE(SUM(bytes_transferred),0)
             FROM download_log WHERE timestamp >= ?1
             GROUP BY d ORDER BY d",
        )?;
        let daily_trend: Vec<DailyStat> = stmt
            .query_map(params![cutoff], |row| {
                Ok(DailyStat {
                    date: row.get(0)?,
                    downloads: row.get(1)?,
                    bytes: row.get(2)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(DownloadStats {
            period_hours,
            total_bytes,
            total_downloads,
            by_provider,
            by_collection,
            daily_trend,
        })
    }

    /// Return the `limit` most-accessed chunks.
    pub fn hot_chunks(&self, limit: usize) -> Result<Vec<HotChunk>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT chunk_sha, COUNT(*) AS cnt, collection_id
             FROM chunk_access
             GROUP BY chunk_sha
             ORDER BY cnt DESC
             LIMIT ?1",
        )?;
        let chunks: Vec<HotChunk> = stmt
            .query_map(params![limit as i64], |row| {
                Ok(HotChunk {
                    chunk_sha: row.get(0)?,
                    access_count: row.get(1)?,
                    collection_id: row.get(2)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(chunks)
    }

    /// Classify collections as hot / warm / cold based on 30-day access counts.
    pub fn replication_advice(&self) -> Result<Vec<ReplicationAdvice>> {
        let cutoff = unix_now() - 30.0 * 86400.0;
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT collection_id, COUNT(*) AS cnt
             FROM collection_access
             WHERE timestamp >= ?1
             GROUP BY collection_id
             ORDER BY cnt DESC",
        )?;

        let rows: Vec<(String, i64)> = stmt
            .query_map(params![cutoff], |row| Ok((row.get(0)?, row.get(1)?)))?
            .filter_map(|r| r.ok())
            .collect();

        // Simple percentile-based classification
        let counts: Vec<i64> = rows.iter().map(|(_, c)| *c).collect();
        let p75 = percentile(&counts, 75);
        let p25 = percentile(&counts, 25);

        let advice = rows
            .into_iter()
            .map(|(collection_id, access_count)| {
                let (tier, advice) = if access_count >= p75 {
                    ("hot", "Increase replication factor to ≥3")
                } else if access_count >= p25 {
                    ("warm", "Keep replication factor at 2")
                } else {
                    ("cold", "Single replica sufficient; consider archiving")
                };
                ReplicationAdvice {
                    collection_id,
                    access_count,
                    tier: tier.to_string(),
                    advice: advice.to_string(),
                }
            })
            .collect();

        Ok(advice)
    }

    /// Ingest history: daily + hourly breakdown for the last `period_days` days.
    pub fn ingest_history(&self, period_days: u64) -> Result<IngestHistory> {
        let cutoff = unix_now() - (period_days as f64 * 86400.0);
        let conn = self.conn.lock().unwrap();

        let (total_bytes, total_items): (i64, i64) = conn.query_row(
            "SELECT COALESCE(SUM(bytes_out),0), COUNT(*) FROM uptake_log WHERE timestamp >= ?1",
            params![cutoff],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;

        // Daily breakdown
        let mut stmt = conn.prepare(
            "SELECT date(timestamp,'unixepoch') AS d,
                    COUNT(*) AS items,
                    COALESCE(SUM(bytes_out),0) AS bytes
             FROM uptake_log
             WHERE timestamp >= ?1
             GROUP BY d
             ORDER BY d",
        )?;
        let daily: Vec<DailyIngest> = stmt
            .query_map(params![cutoff], |row| {
                let bytes: i64 = row.get(2)?;
                Ok(DailyIngest {
                    date: row.get(0)?,
                    items: row.get(1)?,
                    bytes,
                    gb: bytes as f64 / 1_073_741_824.0,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();

        // Hourly breakdown (last 7 days for manageability)
        let hourly_cutoff = unix_now() - 7.0 * 86400.0;
        let mut stmt = conn.prepare(
            "SELECT strftime('%Y-%m-%d %H:00', timestamp, 'unixepoch') AS h,
                    COUNT(*) AS items,
                    COALESCE(SUM(bytes_out),0) AS bytes
             FROM uptake_log
             WHERE timestamp >= ?1
             GROUP BY h
             ORDER BY h",
        )?;
        let hourly: Vec<HourlyIngest> = stmt
            .query_map(params![hourly_cutoff], |row| {
                Ok(HourlyIngest {
                    hour: row.get(0)?,
                    items: row.get(1)?,
                    bytes: row.get(2)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(IngestHistory {
            period_days,
            total_bytes,
            total_gb_fetched: total_bytes as f64 / 1_073_741_824.0,
            total_items_fetched: total_items,
            total_items,
            daily,
            hourly,
        })
    }

    /// Per-collection uptake breakdown for a given period.
    pub fn uptake_by_collection(&self, period_days: u64) -> Result<Vec<serde_json::Value>> {
        let cutoff = unix_now() - (period_days as f64 * 86400.0);
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT COALESCE(collection_id,'unknown'),
                    COUNT(*),
                    COALESCE(SUM(bytes_out),0),
                    COALESCE(SUM(bbox_km2),0)
             FROM uptake_log WHERE timestamp >= ?1
             GROUP BY collection_id ORDER BY SUM(bytes_out) DESC",
        )?;
        let rows: Vec<serde_json::Value> = stmt
            .query_map(params![cutoff], |row| {
                let bytes: i64 = row.get(2)?;
                let km2: f64 = row.get(3)?;
                Ok(serde_json::json!({
                    "collection": row.get::<_, String>(0)?,
                    "requests": row.get::<_, i64>(1)?,
                    "gb": bytes as f64 / 1_073_741_824.0,
                    "aoi_km2": km2,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Delete log entries older than `retain_days`.
    pub fn cleanup(&self, retain_days: u64) -> Result<()> {
        let cutoff = unix_now() - (retain_days as f64 * 86400.0);
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(&format!(
            "DELETE FROM chunk_access       WHERE timestamp < {cutoff};
             DELETE FROM collection_access  WHERE timestamp < {cutoff};
             DELETE FROM bandwidth_log      WHERE timestamp < {cutoff};
             DELETE FROM download_log       WHERE timestamp < {cutoff};
             DELETE FROM uptake_log         WHERE timestamp < {cutoff};",
        ))?;
        Ok(())
    }

    /// Write drained performance snapshots into perf_minutes table (called every 60s).
    pub fn write_perf_snapshots(&self, ts: f64, snaps: &[crate::perf::PerfSnapshot]) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let ts_floor = (ts / 60.0).floor() * 60.0;
        for s in snaps {
            conn.execute(
                "INSERT INTO perf_minutes (ts, endpoint, count, bytes, sum_us, max_us,
                    b0,b1,b2,b3,b4,b5,b6,b7,b8,b9,b10,b11,b12,b13)
                 VALUES (?1,?2,?3,?4,?5,?6,
                    ?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20)",
                rusqlite::params![
                    ts_floor, s.endpoint, s.count as i64, s.bytes as i64,
                    s.sum_us as i64, s.max_us as i64,
                    s.buckets[0] as i64, s.buckets[1] as i64, s.buckets[2] as i64,
                    s.buckets[3] as i64, s.buckets[4] as i64, s.buckets[5] as i64,
                    s.buckets[6] as i64, s.buckets[7] as i64, s.buckets[8] as i64,
                    s.buckets[9] as i64, s.buckets[10] as i64, s.buckets[11] as i64,
                    s.buckets[12] as i64, s.buckets[13] as i64,
                ],
            )?;
        }
        let cutoff = ts - 7.0 * 86400.0;
        conn.execute("DELETE FROM perf_minutes WHERE ts < ?1", rusqlite::params![cutoff])?;
        Ok(())
    }

    /// Query performance data for the given window in seconds.
    pub fn query_perf(&self, window_secs: f64) -> Result<serde_json::Value> {
        let conn = self.conn.lock().unwrap();
        let now = unix_now();
        let cutoff = now - window_secs;

        let mut stmt = conn.prepare(
            "SELECT endpoint, SUM(count), SUM(bytes), SUM(sum_us), MAX(max_us),
                    SUM(b0),SUM(b1),SUM(b2),SUM(b3),SUM(b4),SUM(b5),SUM(b6),
                    SUM(b7),SUM(b8),SUM(b9),SUM(b10),SUM(b11),SUM(b12),SUM(b13)
             FROM perf_minutes WHERE ts >= ?1 GROUP BY endpoint"
        )?;

        let endpoints: Vec<serde_json::Value> = stmt
            .query_map(rusqlite::params![cutoff], |row| {
                let ep: String = row.get(0)?;
                let count: i64 = row.get(1)?;
                let bytes: i64 = row.get(2)?;
                let sum_us: i64 = row.get(3)?;
                let max_us: i64 = row.get(4)?;
                let buckets: Vec<i64> = (5..19).map(|i| row.get::<_, i64>(i).unwrap_or(0)).collect();
                Ok((ep, count, bytes, sum_us, max_us, buckets))
            })?
            .filter_map(|r| r.ok())
            .map(|(ep, count, bytes, sum_us, max_us, buckets)| {
                let rps = count as f64 / window_secs;
                let mbps = bytes as f64 / window_secs / 1_048_576.0;
                let mean_ms = if count > 0 { sum_us as f64 / count as f64 / 1000.0 } else { 0.0 };
                let (p50, p95, p99) = percentiles_from_buckets(&buckets);
                serde_json::json!({
                    "endpoint": ep,
                    "requests": count, "rps": (rps*100.0).round()/100.0,
                    "mb_per_s": (mbps*100.0).round()/100.0,
                    "latency_ms": {
                        "mean": (mean_ms*100.0).round()/100.0,
                        "p50": p50, "p95": p95, "p99": p99,
                        "max": max_us as f64 / 1000.0
                    }
                })
            })
            .collect();

        let mut stmt2 = conn.prepare(
            "SELECT ts, endpoint, count, bytes FROM perf_minutes WHERE ts >= ?1 ORDER BY ts"
        )?;
        let series: Vec<serde_json::Value> = stmt2
            .query_map(rusqlite::params![cutoff], |row| {
                Ok(serde_json::json!({
                    "ts": row.get::<_, f64>(0)?,
                    "endpoint": row.get::<_, String>(1)?,
                    "count": row.get::<_, i64>(2)?,
                    "bytes": row.get::<_, i64>(3)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(serde_json::json!({"window_secs": window_secs, "endpoints": endpoints, "series": series}))
    }
}

// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn unix_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// Simple percentile from a sorted slice (returns value at given percentile).
fn percentile(values: &[i64], p: u8) -> i64 {
    if values.is_empty() { return 0; }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let idx = ((p as f64 / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

/// Compute p50, p95, p99 from cumulative histogram bucket counts.
fn percentiles_from_buckets(buckets: &[i64]) -> (f64, f64, f64) {
    let bounds = crate::perf::BUCKET_BOUNDS_MS;
    let total: i64 = buckets.iter().sum();
    if total == 0 { return (0.0, 0.0, 0.0); }
    let p = |rank: f64| -> f64 {
        let target = (rank / 100.0 * total as f64).ceil() as i64;
        let mut cum = 0i64;
        for (i, &b) in buckets.iter().enumerate() {
            cum += b;
            if cum >= target {
                let lower = if i == 0 { 0 } else { bounds[i - 1] };
                let upper = if i < bounds.len() { bounds[i] } else { *bounds.last().unwrap_or(&10000) };
                let prev_cum = cum - b;
                let frac = (target - prev_cum) as f64 / b.max(1) as f64;
                return lower as f64 + frac * (upper - lower) as f64;
            }
        }
        *bounds.last().unwrap_or(&10000) as f64
    };
    (p(50.0), p(95.0), p(99.0))
}
