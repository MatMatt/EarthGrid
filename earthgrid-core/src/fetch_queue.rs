//! Persistent fetch job queue backed by SQLite.
//! Supports atomic job claiming, progress tracking, and crash recovery.

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
// FetchQueue
// ---------------------------------------------------------------------------

pub struct FetchQueue {
    conn: Mutex<Connection>,
}

impl FetchQueue {
    /// Create or open the fetch queue database.
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
                error TEXT,
                created_at REAL NOT NULL,
                started_at REAL,
                completed_at REAL,
                retry_count INTEGER DEFAULT 0
            );",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Enqueue a new fetch job. Returns the new job id.
    pub fn enqueue(&self, job: NewFetchJob) -> Result<i64, EarthGridError> {
        let conn = self.conn.lock().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
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

    /// Atomically claim the next pending job for a node. Returns the claimed job or None.
    pub fn claim_next(&self, node_id: &str) -> Result<Option<FetchJob>, EarthGridError> {
        let conn = self.conn.lock().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        // Find the oldest pending job
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

    /// Update progress for a running job.
    pub fn update_progress(&self, job_id: i64, done: i64, total: i64) -> Result<(), EarthGridError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE fetch_jobs SET progress_done=?1, progress_total=?2 WHERE id=?3",
            params![done, total, job_id],
        ).map_err(|e| EarthGridError::Other(e.to_string()))?;
        Ok(())
    }

    /// Mark a job as completed.
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

    /// Mark a job as failed with an error message.
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

    /// Reset jobs stuck in 'running' for longer than timeout_secs back to 'pending'.
    /// Called on startup for crash recovery.
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

    /// List jobs, optionally filtered by status.
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

    /// Get a single job by ID.
    pub fn get(&self, job_id: i64) -> Result<Option<FetchJob>, EarthGridError> {
        let conn = self.conn.lock().unwrap();
        match self.get_job_with_conn(&conn, job_id) {
            Ok(job) => Ok(Some(job)),
            Err(EarthGridError::ItemNotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Cancel a job (set to 'paused').
    pub fn cancel(&self, job_id: i64) -> Result<bool, EarthGridError> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(
            "UPDATE fetch_jobs SET status='paused' WHERE id=?1 AND status NOT IN ('completed')",
            params![job_id],
        ).map_err(|e| EarthGridError::Other(e.to_string()))?;
        Ok(n > 0)
    }

    /// Retry a failed or paused job (reset to pending).
    pub fn retry(&self, job_id: i64) -> Result<bool, EarthGridError> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(
            "UPDATE fetch_jobs SET status='pending', error=NULL, started_at=NULL, completed_at=NULL, progress_done=0, progress_total=0 WHERE id=?1 AND status IN ('failed', 'paused')",
            params![job_id],
        ).map_err(|e| EarthGridError::Other(e.to_string()))?;
        Ok(n > 0)
    }

    // Internal: fetch job row with existing connection
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
