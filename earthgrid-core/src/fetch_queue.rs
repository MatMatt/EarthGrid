//! Persistent fetch job queue backed by SQLite (local) or HTTP proxy (remote).
//! Beacon nodes use the local SQLite queue; non-beacon nodes proxy to the beacon.

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Mutex;

use crate::error::EarthGridError;

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchJob {
    pub id: i64,
    pub collection: String,
    pub bbox: String,           // "w,s,e,n"
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub cloud_cover: f64,
    pub bands: Option<String>,  // comma-separated
    pub limit_count: i64,
    pub status: String,         // pending, running, completed, failed, paused
    pub assigned_node: Option<String>,
    pub progress_total: i64,
    pub progress_done: i64,
    pub stage: Option<String>,   // human-readable progress text
    pub error: Option<String>,
    pub created_at: f64,
    pub started_at: Option<f64>,
    pub completed_at: Option<f64>,
    pub retry_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewFetchJob {
    pub collection: String,
    pub bbox: String,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub cloud_cover: Option<f64>,
    pub bands: Option<String>,
    pub limit_count: Option<i64>,
}

// ---------------------------------------------------------------------------
// FetchQueueBackend — Local (SQLite) or Remote (HTTP proxy to beacon)
// ---------------------------------------------------------------------------

pub enum FetchQueueBackend {
    Local(LocalFetchQueue),
    Remote(RemoteFetchQueue),
}

impl FetchQueueBackend {
    pub fn enqueue(&self, job: NewFetchJob) -> Result<i64, EarthGridError> {
        match self {
            Self::Local(q) => q.enqueue(job),
            Self::Remote(q) => tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(q.enqueue(job))
            }),
        }
    }

    pub fn claim_next(&self, node_id: &str) -> Result<Option<FetchJob>, EarthGridError> {
        match self {
            Self::Local(q) => q.claim_next(node_id),
            Self::Remote(q) => tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(q.claim_next(node_id))
            }),
        }
    }

    pub fn update_progress(&self, job_id: i64, done: i64, total: i64) -> Result<(), EarthGridError> {
        match self {
            Self::Local(q) => q.update_progress(job_id, done, total),
            Self::Remote(q) => tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(q.update_progress(job_id, done, total))
            }),
        }
    }

    pub fn complete(&self, job_id: i64) -> Result<(), EarthGridError> {
        match self {
            Self::Local(q) => q.complete(job_id),
            Self::Remote(q) => tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(q.complete(job_id))
            }),
        }
    }

    pub fn fail(&self, job_id: i64, error: &str) -> Result<(), EarthGridError> {
        match self {
            Self::Local(q) => q.fail(job_id, error),
            Self::Remote(q) => tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(q.fail(job_id, error))
            }),
        }
    }

    pub fn release_stale(&self, timeout_secs: f64) -> Result<usize, EarthGridError> {
        match self {
            Self::Local(q) => q.release_stale(timeout_secs),
            Self::Remote(_) => Ok(0), // beacon handles stale recovery
        }
    }

    pub fn list(&self, status_filter: Option<&str>) -> Result<Vec<FetchJob>, EarthGridError> {
        match self {
            Self::Local(q) => q.list(status_filter),
            Self::Remote(q) => tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(q.list(status_filter))
            }),
        }
    }

    pub fn get(&self, job_id: i64) -> Result<Option<FetchJob>, EarthGridError> {
        match self {
            Self::Local(q) => q.get(job_id),
            Self::Remote(q) => tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(q.get(job_id))
            }),
        }
    }

    pub fn cancel(&self, job_id: i64) -> Result<bool, EarthGridError> {
        match self {
            Self::Local(q) => q.cancel(job_id),
            Self::Remote(q) => tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(q.cancel(job_id))
            }),
        }
    }

    pub fn retry(&self, job_id: i64) -> Result<bool, EarthGridError> {
        match self {
            Self::Local(q) => q.retry(job_id),
            Self::Remote(q) => tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(q.retry(job_id))
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// LocalFetchQueue — SQLite-backed (used by beacon)
// ---------------------------------------------------------------------------

pub struct LocalFetchQueue {
    conn: Mutex<Connection>,
}

impl LocalFetchQueue {
    pub fn new(db_path: &Path) -> Result<Self, EarthGridError> {
        let conn = Connection::open(db_path)
            .map_err(|e| EarthGridError::Other(e.to_string()))?;
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS fetch_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                collection TEXT NOT NULL,
                bbox TEXT NOT NULL,
                start_date TEXT,
                end_date TEXT,
                cloud_cover REAL DEFAULT 30.0,
                bands TEXT,
                limit_count INTEGER DEFAULT 100,
                status TEXT NOT NULL DEFAULT 'pending',
                assigned_node TEXT,
                progress_total INTEGER DEFAULT 0,
                progress_done INTEGER DEFAULT 0,
                stage TEXT,
                error TEXT,
                created_at REAL NOT NULL,
                started_at REAL,
                completed_at REAL,
                retry_count INTEGER DEFAULT 0
            );",
        )?;
        // Safe migration: add stage column for real-time progress text
        let _ = conn.execute_batch(
            "ALTER TABLE fetch_jobs ADD COLUMN stage TEXT;",
        );
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn enqueue(&self, job: NewFetchJob) -> Result<i64, EarthGridError> {
        let conn = self.conn.lock().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        // Deduplicate: check for existing pending/running job with same params
        let existing: Option<i64> = conn.query_row(
            "SELECT id FROM fetch_jobs WHERE collection=?1 AND bbox=?2 \
             AND start_date IS ?3 AND end_date IS ?4 \
             AND cloud_cover=?5 AND bands IS ?6 AND limit_count=?7 \
             AND status IN ('pending', 'running') \
             ORDER BY created_at ASC LIMIT 1",
            params![
                job.collection,
                job.bbox,
                job.start_date,
                job.end_date,
                job.cloud_cover.unwrap_or(30.0),
                job.bands,
                job.limit_count.unwrap_or(100),
            ],
            |r| r.get(0),
        ).ok();

        if let Some(id) = existing {
            return Ok(id);
        }

        conn.execute(
            "INSERT INTO fetch_jobs
                (collection, bbox, start_date, end_date, cloud_cover, bands, limit_count, status, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'pending', ?8)",
            params![
                job.collection,
                job.bbox,
                job.start_date,
                job.end_date,
                job.cloud_cover.unwrap_or(30.0),
                job.bands,
                job.limit_count.unwrap_or(100),
                now,
            ],
        ).map_err(|e| EarthGridError::Other(e.to_string()))?;
        Ok(conn.last_insert_rowid())
    }

    pub fn claim_next(&self, node_id: &str) -> Result<Option<FetchJob>, EarthGridError> {
        let conn = self.conn.lock().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        // Release stale jobs stuck in "running" (>10 min) so workers can retry them
        drop(conn.execute(
            "UPDATE fetch_jobs SET status='pending', assigned_node=NULL, started_at=NULL WHERE status='running' AND started_at < ?1",
            params![now - 600.0],
        ));

        let id_opt: Option<i64> = conn
            .query_row(
                "SELECT id FROM fetch_jobs WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1",
                [],
                |r| r.get(0),
            )
            .ok();

        if let Some(id) = id_opt {
            let updated = conn.execute(
                "UPDATE fetch_jobs SET status='running', assigned_node=?1, started_at=?2 WHERE id=?3 AND status='pending'",
                params![node_id, now, id],
            ).map_err(|e| EarthGridError::Other(e.to_string()))?;

            if updated > 0 {
                return self.get_job_with_conn(&conn, id).map(Some);
            }
        }
        Ok(None)
    }

    pub fn update_progress(&self, job_id: i64, done: i64, total: i64) -> Result<(), EarthGridError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE fetch_jobs SET progress_done=?1, progress_total=?2 WHERE id=?3",
            params![done, total, job_id],
        ).map_err(|e| EarthGridError::Other(e.to_string()))?;
        Ok(())
    }

    pub fn complete(&self, job_id: i64) -> Result<(), EarthGridError> {
        let conn = self.conn.lock().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        conn.execute(
            "UPDATE fetch_jobs SET status='completed', completed_at=?1, error=NULL WHERE id=?2",
            params![now, job_id],
        ).map_err(|e| EarthGridError::Other(e.to_string()))?;
        Ok(())
    }

    pub fn fail(&self, job_id: i64, error: &str) -> Result<(), EarthGridError> {
        let conn = self.conn.lock().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        conn.execute(
            "UPDATE fetch_jobs SET status='failed', completed_at=?1, error=?2, retry_count=retry_count+1 WHERE id=?3",
            params![now, error, job_id],
        ).map_err(|e| EarthGridError::Other(e.to_string()))?;
        Ok(())
    }

    pub fn release_stale(&self, timeout_secs: f64) -> Result<usize, EarthGridError> {
        let conn = self.conn.lock().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        let cutoff = now - timeout_secs;
        let n = conn.execute(
            "UPDATE fetch_jobs SET status='pending', assigned_node=NULL, started_at=NULL WHERE status='running' AND started_at < ?1",
            params![cutoff],
        ).map_err(|e| EarthGridError::Other(e.to_string()))?;
        Ok(n)
    }

    pub fn list(&self, status_filter: Option<&str>) -> Result<Vec<FetchJob>, EarthGridError> {
        let conn = self.conn.lock().unwrap();
        let mut jobs = Vec::new();
        if let Some(s) = status_filter {
            let mut stmt = conn.prepare(
                "SELECT id,collection,bbox,start_date,end_date,cloud_cover,bands,limit_count,status,assigned_node,progress_total,progress_done,error,created_at,started_at,completed_at,retry_count FROM fetch_jobs WHERE status=?1 ORDER BY created_at DESC",
            ).map_err(|e| EarthGridError::Other(e.to_string()))?;
            let rows = stmt.query_map(params![s], Self::row_to_job)
                .map_err(|e| EarthGridError::Other(e.to_string()))?;
            for row in rows {
                jobs.push(row.map_err(|e| EarthGridError::Other(e.to_string()))?);
            }
        } else {
            let mut stmt = conn.prepare(
                "SELECT id,collection,bbox,start_date,end_date,cloud_cover,bands,limit_count,status,assigned_node,progress_total,progress_done,error,created_at,started_at,completed_at,retry_count FROM fetch_jobs ORDER BY created_at DESC",
            ).map_err(|e| EarthGridError::Other(e.to_string()))?;
            let rows = stmt.query_map([], Self::row_to_job)
                .map_err(|e| EarthGridError::Other(e.to_string()))?;
            for row in rows {
                jobs.push(row.map_err(|e| EarthGridError::Other(e.to_string()))?);
            }
        }
        Ok(jobs)
    }

    pub fn get(&self, job_id: i64) -> Result<Option<FetchJob>, EarthGridError> {
        let conn = self.conn.lock().unwrap();
        match self.get_job_with_conn(&conn, job_id) {
            Ok(job) => Ok(Some(job)),
            Err(EarthGridError::ItemNotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn cancel(&self, job_id: i64) -> Result<bool, EarthGridError> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(
            "UPDATE fetch_jobs SET status='paused' WHERE id=?1 AND status NOT IN ('completed')",
            params![job_id],
        ).map_err(|e| EarthGridError::Other(e.to_string()))?;
        Ok(n > 0)
    }

    pub fn retry(&self, job_id: i64) -> Result<bool, EarthGridError> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(
            "UPDATE fetch_jobs SET status='pending', error=NULL, started_at=NULL, completed_at=NULL, progress_done=0, progress_total=0 WHERE id=?1 AND status IN ('failed', 'paused')",
            params![job_id],
        ).map_err(|e| EarthGridError::Other(e.to_string()))?;
        Ok(n > 0)
    }

    fn get_job_with_conn(&self, conn: &Connection, job_id: i64) -> Result<FetchJob, EarthGridError> {
        conn.query_row(
            "SELECT id,collection,bbox,start_date,end_date,cloud_cover,bands,limit_count,status,assigned_node,progress_total,progress_done,error,created_at,started_at,completed_at,retry_count FROM fetch_jobs WHERE id=?1",
            params![job_id],
            Self::row_to_job,
        ).map_err(|_| EarthGridError::ItemNotFound(format!("job {}", job_id)))
    }

    fn row_to_job(r: &rusqlite::Row<'_>) -> rusqlite::Result<FetchJob> {
        Ok(FetchJob {
            id: r.get(0)?,
            collection: r.get(1)?,
            bbox: r.get(2)?,
            start_date: r.get(3)?,
            end_date: r.get(4)?,
            cloud_cover: r.get(5)?,
            bands: r.get(6)?,
            limit_count: r.get(7)?,
            status: r.get(8)?,
            assigned_node: r.get(9)?,
            progress_total: r.get(10)?,
            progress_done: r.get(11)?,
            error: r.get(12)?,
            created_at: r.get(13)?,
            started_at: r.get(14)?,
            completed_at: r.get(15)?,
            retry_count: r.get(16)?,
        })
    }
}

// ---------------------------------------------------------------------------
// RemoteFetchQueue — HTTP proxy to beacon node
// ---------------------------------------------------------------------------

pub struct RemoteFetchQueue {
    beacon_url: String,
    admin_key: String,
    client: reqwest::Client,
}

impl RemoteFetchQueue {
    pub fn new(beacon_url: &str, admin_key: &str) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_default();
        Self {
            beacon_url: beacon_url.trim_end_matches('/').to_string(),
            admin_key: admin_key.to_string(),
            client,
        }
    }

    async fn enqueue(&self, job: NewFetchJob) -> Result<i64, EarthGridError> {
        let mut params = vec![
            ("collection", job.collection),
            ("bbox", job.bbox),
        ];
        if let Some(sd) = job.start_date { params.push(("start_date", sd)); }
        if let Some(ed) = job.end_date { params.push(("end_date", ed)); }
        if let Some(cc) = job.cloud_cover { params.push(("cloud_cover", cc.to_string())); }
        if let Some(b) = job.bands { params.push(("bands", b)); }
        if let Some(l) = job.limit_count { params.push(("limit_count", l.to_string())); }

        let resp = self.client.post(format!("{}/api/fetch/queue", self.beacon_url))
            .header("x-api-key", &self.admin_key)
            .query(&params)
            .send().await
            .map_err(|e| EarthGridError::Other(format!("beacon enqueue: {}", e)))?;

        let data: serde_json::Value = resp.json().await
            .map_err(|e| EarthGridError::Other(format!("beacon enqueue parse: {}", e)))?;
        data["job_id"].as_i64()
            .ok_or_else(|| EarthGridError::Other("beacon enqueue: no job_id".to_string()))
    }

    async fn claim_next(&self, node_id: &str) -> Result<Option<FetchJob>, EarthGridError> {
        let resp = self.client.post(format!("{}/api/fetch/queue/claim", self.beacon_url))
            .header("x-api-key", &self.admin_key)
            .query(&[("node_id", node_id)])
            .send().await
            .map_err(|e| EarthGridError::Other(format!("beacon claim: {}", e)))?;

        if resp.status() == reqwest::StatusCode::NO_CONTENT {
            return Ok(None);
        }

        let data: serde_json::Value = resp.json().await
            .map_err(|e| EarthGridError::Other(format!("beacon claim parse: {}", e)))?;

        if data.get("job").is_some() {
            let job: FetchJob = serde_json::from_value(data["job"].clone())
                .map_err(|e| EarthGridError::Other(format!("beacon claim deserialize: {}", e)))?;
            Ok(Some(job))
        } else {
            Ok(None)
        }
    }

    async fn update_progress(&self, job_id: i64, done: i64, total: i64) -> Result<(), EarthGridError> {
        let _ = self.client.post(format!("{}/api/fetch/queue/{}/progress", self.beacon_url, job_id))
            .header("x-api-key", &self.admin_key)
            .json(&serde_json::json!({"done": done, "total": total}))
            .send().await;
        Ok(()) // progress updates are best-effort
    }

    async fn complete(&self, job_id: i64) -> Result<(), EarthGridError> {
        self.client.post(format!("{}/api/fetch/queue/{}/complete", self.beacon_url, job_id))
            .header("x-api-key", &self.admin_key)
            .send().await
            .map_err(|e| EarthGridError::Other(format!("beacon complete: {}", e)))?;
        Ok(())
    }

    async fn fail(&self, job_id: i64, error: &str) -> Result<(), EarthGridError> {
        self.client.post(format!("{}/api/fetch/queue/{}/fail", self.beacon_url, job_id))
            .header("x-api-key", &self.admin_key)
            .json(&serde_json::json!({"error": error}))
            .send().await
            .map_err(|e| EarthGridError::Other(format!("beacon fail: {}", e)))?;
        Ok(())
    }

    async fn list(&self, status_filter: Option<&str>) -> Result<Vec<FetchJob>, EarthGridError> {
        let mut url = format!("{}/api/fetch/queue", self.beacon_url);
        if let Some(s) = status_filter {
            url.push_str(&format!("?status={}", s));
        }
        let resp = self.client.get(&url)
            .header("x-api-key", &self.admin_key)
            .send().await
            .map_err(|e| EarthGridError::Other(format!("beacon list: {}", e)))?;

        let data: serde_json::Value = resp.json().await
            .map_err(|e| EarthGridError::Other(format!("beacon list parse: {}", e)))?;

        let jobs: Vec<FetchJob> = serde_json::from_value(data["jobs"].clone())
            .unwrap_or_default();
        Ok(jobs)
    }

    async fn get(&self, job_id: i64) -> Result<Option<FetchJob>, EarthGridError> {
        let resp = self.client.get(format!("{}/api/fetch/queue/{}", self.beacon_url, job_id))
            .header("x-api-key", &self.admin_key)
            .send().await
            .map_err(|e| EarthGridError::Other(format!("beacon get: {}", e)))?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let job: FetchJob = resp.json().await
            .map_err(|e| EarthGridError::Other(format!("beacon get parse: {}", e)))?;
        Ok(Some(job))
    }

    async fn cancel(&self, job_id: i64) -> Result<bool, EarthGridError> {
        let resp = self.client.delete(format!("{}/api/fetch/queue/{}", self.beacon_url, job_id))
            .header("x-api-key", &self.admin_key)
            .send().await
            .map_err(|e| EarthGridError::Other(format!("beacon cancel: {}", e)))?;
        Ok(resp.status().is_success())
    }

    async fn retry(&self, job_id: i64) -> Result<bool, EarthGridError> {
        let resp = self.client.post(format!("{}/api/fetch/queue/{}/retry", self.beacon_url, job_id))
            .header("x-api-key", &self.admin_key)
            .send().await
            .map_err(|e| EarthGridError::Other(format!("beacon retry: {}", e)))?;
        Ok(resp.status().is_success())
    }
}

// ---------------------------------------------------------------------------
// Backwards-compatible type alias
// ---------------------------------------------------------------------------

/// FetchQueue is now FetchQueueBackend. This alias keeps existing code working.
pub type FetchQueue = FetchQueueBackend;
