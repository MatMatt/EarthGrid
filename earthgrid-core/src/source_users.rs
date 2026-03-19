//! Source user credential management for EarthGrid data acquisition.
//!
//! Manages CDSE / Copernicus accounts that donate access for downloading data.
//! Credentials are base64-encoded at rest (not plaintext).
//! Schema is compatible with source_users.py for shared SQLite access.

use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Mutex;

use crate::error::Result;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A registered source user account.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceUser {
    pub id: i64,
    pub name: String,
    pub provider: String,
    pub username: String,
    /// Base64-encoded password (decode to use).
    pub password_encoded: String,
    pub max_requests_hour: i64,
    pub max_download_gb: f64,
    pub last_used: f64,
    pub success_count: i64,
    pub fail_count: i64,
    pub consecutive_fails: i64,
    pub is_healthy: bool,
    pub is_enabled: bool,
    pub total_downloaded_bytes: i64,
    pub created_at: f64,
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// SQLite-backed registry for source user credentials.
pub struct SourceUserRegistry {
    conn: Mutex<Connection>,
}

impl SourceUserRegistry {
    /// Open or create the source-users DB at `db_path`, enable WAL mode.
    pub fn new(db_path: &Path) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(db_path)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA busy_timeout=10000;
             PRAGMA synchronous=NORMAL;",
        )?;
        let reg = Self { conn: Mutex::new(conn) };
        reg.init_tables()?;
        Ok(reg)
    }

    fn init_tables(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS source_users (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                name                    TEXT    NOT NULL,
                provider                TEXT    NOT NULL DEFAULT 'cdse',
                username                TEXT    NOT NULL,
                encrypted_password      TEXT    NOT NULL DEFAULT '',
                encrypted_token         TEXT    NOT NULL DEFAULT '',
                max_requests_hour       INTEGER NOT NULL DEFAULT 100,
                max_download_gb         REAL    NOT NULL DEFAULT 50.0,
                last_used               REAL    NOT NULL DEFAULT 0,
                success_count           INTEGER NOT NULL DEFAULT 0,
                fail_count              INTEGER NOT NULL DEFAULT 0,
                consecutive_fails       INTEGER NOT NULL DEFAULT 0,
                is_healthy              INTEGER NOT NULL DEFAULT 1,
                is_enabled              INTEGER NOT NULL DEFAULT 1,
                total_downloaded_bytes  INTEGER NOT NULL DEFAULT 0,
                created_at              REAL    NOT NULL
            );
            CREATE TABLE IF NOT EXISTS download_log (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id             INTEGER NOT NULL,
                timestamp           REAL    NOT NULL,
                collection          TEXT    NOT NULL DEFAULT '',
                item_id             TEXT    NOT NULL DEFAULT '',
                bytes_downloaded    INTEGER NOT NULL DEFAULT 0,
                success             INTEGER NOT NULL DEFAULT 1,
                error_msg           TEXT    NOT NULL DEFAULT '',
                FOREIGN KEY (user_id) REFERENCES source_users(id)
            );",
        )?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Write methods
    // -----------------------------------------------------------------------

    /// Add a source user with base64-encoded password. Returns the new row ID.
    pub fn add_user(
        &self,
        name: &str,
        provider: &str,
        username: &str,
        password: &str,
    ) -> Result<i64> {
        let enc_pw = b64_encode(password.as_bytes());
        let now = unix_now();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO source_users
             (name, provider, username, encrypted_password, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![name, provider, username, enc_pw, now],
        )?;
        Ok(conn.last_insert_rowid())
    }

    /// Return all active (enabled + healthy) users for a given provider.
    pub fn get_users(&self, provider: &str) -> Result<Vec<SourceUser>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, provider, username, encrypted_password,
                    max_requests_hour, max_download_gb, last_used,
                    success_count, fail_count, consecutive_fails,
                    is_healthy, is_enabled, total_downloaded_bytes, created_at
             FROM source_users
             WHERE provider = ?1 AND is_enabled = 1 AND is_healthy = 1
             ORDER BY last_used ASC",
        )?;
        let users: Vec<SourceUser> = stmt
            .query_map(params![provider], |row| {
                Ok(SourceUser {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    provider: row.get(2)?,
                    username: row.get(3)?,
                    password_encoded: row.get(4)?,
                    max_requests_hour: row.get(5)?,
                    max_download_gb: row.get(6)?,
                    last_used: row.get(7)?,
                    success_count: row.get(8)?,
                    fail_count: row.get(9)?,
                    consecutive_fails: row.get(10)?,
                    is_healthy: row.get::<_, i64>(11)? != 0,
                    is_enabled: row.get::<_, i64>(12)? != 0,
                    total_downloaded_bytes: row.get(13)?,
                    created_at: row.get(14)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(users)
    }

    /// Round-robin selection: returns the least-recently-used active user for `provider`.
    pub fn rotate_user(&self, provider: &str) -> Result<Option<SourceUser>> {
        let users = self.get_users(provider)?;
        Ok(users.into_iter().next()) // already sorted by last_used ASC
    }

    /// Update `last_used` and increment `requests_today` for `username`.
    pub fn mark_used(&self, username: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE source_users
             SET last_used = ?1,
                 success_count = success_count + 1,
                 consecutive_fails = 0
             WHERE username = ?2",
            params![unix_now(), username],
        )?;
        Ok(())
    }

    /// Mark a user inactive (rate-limited, quota exceeded, etc.).
    pub fn deactivate(&self, username: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE source_users SET is_enabled = 0 WHERE username = ?1",
            params![username],
        )?;
        Ok(())
    }

    /// Mark a user unhealthy after repeated failures.
    pub fn mark_unhealthy(&self, username: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE source_users
             SET is_healthy = 0,
                 fail_count = fail_count + 1,
                 consecutive_fails = consecutive_fails + 1
             WHERE username = ?1",
            params![username],
        )?;
        Ok(())
    }

    /// Restore health status for a user (admin reset).
    pub fn reset_health(&self, username: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE source_users SET is_healthy = 1, consecutive_fails = 0
             WHERE username = ?1",
            params![username],
        )?;
        Ok(())
    }

    /// Decode the stored base64 password for a user.
    pub fn decode_password(encoded: &str) -> String {
        b64_decode(encoded)
            .and_then(|b| String::from_utf8(b).ok())
            .unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn unix_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// Minimal standard-base64 encoder (no external crate).
fn b64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(CHARS[((n >> 18) & 0x3f) as usize] as char);
        out.push(CHARS[((n >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(CHARS[((n >> 6) & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(CHARS[(n & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

/// Minimal standard-base64 decoder (no external crate).
fn b64_decode(s: &str) -> Option<Vec<u8>> {
    const INV: [u8; 256] = {
        let mut t = [0xffu8; 256];
        let chars = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut i = 0usize;
        while i < chars.len() {
            t[chars[i] as usize] = i as u8;
            i += 1;
        }
        t
    };
    let bytes: Vec<u8> = s.bytes().filter(|&b| b != b'=').collect();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    for chunk in bytes.chunks(4) {
        let v: Vec<u8> = chunk.iter().map(|&b| {
            let c = INV[b as usize];
            c
        }).collect();
        if v.iter().any(|&c| c == 0xff) { return None; }
        let n = ((v[0] as u32) << 18)
              | ((v.get(1).copied().unwrap_or(0) as u32) << 12)
              | ((v.get(2).copied().unwrap_or(0) as u32) << 6)
              | (v.get(3).copied().unwrap_or(0) as u32);
        out.push((n >> 16) as u8);
        if v.len() > 2 { out.push((n >> 8) as u8); }
        if v.len() > 3 { out.push(n as u8); }
    }
    Some(out)
}
