//! Per-user API key management for EarthGrid.
//!
//! Network-wide UUID-based API keys; user records can be synced between
//! nodes via federation.  Schema is compatible with user_auth.py.

use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Mutex;
use uuid::Uuid;

use crate::error::Result;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// An authenticated user (returned from key validation / listing).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthUser {
    pub user_id: String,
    pub username: String,
    pub role: String,
    pub node_origin: String,
    pub created_at: f64,
    pub last_used: f64,
}

/// Valid roles.
pub const ROLE_ADMIN: &str = "admin";
pub const ROLE_USER: &str = "user";
pub const ROLE_READONLY: &str = "readonly";
pub const ROLE_MEMBER: &str = "member"; // legacy Python compat

// ---------------------------------------------------------------------------
// UserAuth
// ---------------------------------------------------------------------------

/// SQLite-backed per-user API key registry.
pub struct UserAuth {
    conn: Mutex<Connection>,
}

impl UserAuth {
    /// Open or create the user-auth DB at `db_path`, enable WAL mode.
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
        let ua = Self { conn: Mutex::new(conn) };
        ua.init_tables()?;
        Ok(ua)
    }

    fn init_tables(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS users (
                user_id     TEXT PRIMARY KEY,
                username    TEXT UNIQUE NOT NULL,
                api_key     TEXT UNIQUE NOT NULL,
                node_origin TEXT NOT NULL DEFAULT '',
                role        TEXT NOT NULL DEFAULT 'user',
                created_at  REAL NOT NULL,
                updated_at  REAL NOT NULL,
                last_used   REAL NOT NULL DEFAULT 0,
                active      INTEGER NOT NULL DEFAULT 1
            );
            CREATE INDEX IF NOT EXISTS idx_users_api_key ON users(api_key);
            CREATE INDEX IF NOT EXISTS idx_users_active  ON users(active);",
        )?;
        // Migration: add last_used column if missing (pre-v0.6 DBs)
        let _ = conn.execute_batch(
            "ALTER TABLE users ADD COLUMN last_used REAL NOT NULL DEFAULT 0;"
        );
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Write methods
    // -----------------------------------------------------------------------

    /// Create a new user with a generated UUID API key.
    ///
    /// Returns the generated API key (only time it's plaintext).
    /// Roles: "admin", "user", "readonly", "member" (legacy).
    pub fn add_user(&self, username: &str, role: &str) -> Result<String> {
        let user_id = Uuid::new_v4().simple().to_string()[..16].to_string();
        let api_key = Uuid::new_v4().to_string();
        let now = unix_now();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO users
             (user_id, username, api_key, role, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
            params![user_id, username, api_key, role, now],
        )?;
        Ok(api_key)
    }

    /// Validate an API key. Updates `last_used` on success.
    ///
    /// Returns `Some(AuthUser)` if valid and active, `None` otherwise.
    pub fn validate_key(&self, key: &str) -> Result<Option<AuthUser>> {
        if key.is_empty() {
            return Ok(None);
        }
        let conn = self.conn.lock().unwrap();
        let result = conn.query_row(
            "SELECT user_id, username, role, node_origin, created_at, last_used
             FROM users WHERE api_key = ?1 AND active = 1",
            params![key],
            |row| {
                Ok(AuthUser {
                    user_id: row.get(0)?,
                    username: row.get(1)?,
                    role: row.get(2)?,
                    node_origin: row.get(3)?,
                    created_at: row.get(4)?,
                    last_used: row.get(5)?,
                })
            },
        );
        match result {
            Ok(user) => {
                // Update last_used (best-effort, ignore error)
                let _ = conn.execute(
                    "UPDATE users SET last_used = ?1 WHERE api_key = ?2",
                    params![unix_now(), key],
                );
                Ok(Some(user))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// List all active users (API keys are not included).
    pub fn list_users(&self) -> Result<Vec<AuthUser>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT user_id, username, role, node_origin, created_at, last_used
             FROM users WHERE active = 1 ORDER BY created_at",
        )?;
        let users: Vec<AuthUser> = stmt
            .query_map([], |row| {
                Ok(AuthUser {
                    user_id: row.get(0)?,
                    username: row.get(1)?,
                    role: row.get(2)?,
                    node_origin: row.get(3)?,
                    created_at: row.get(4)?,
                    last_used: row.get(5)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(users)
    }

    /// Soft-revoke a user (set active = 0).
    pub fn revoke_user(&self, username: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "UPDATE users SET active = 0, updated_at = ?1 WHERE username = ?2",
            params![unix_now(), username],
        )?;
        Ok(rows > 0)
    }

    /// Check if an API key has at least `required_role` privileges.
    ///
    /// Hierarchy: admin > user ≥ member > readonly
    pub fn check_role(user: &AuthUser, required_role: &str) -> bool {
        match required_role {
            ROLE_ADMIN => user.role == ROLE_ADMIN,
            ROLE_USER | ROLE_MEMBER => matches!(user.role.as_str(), "admin" | "user" | "member"),
            ROLE_READONLY => !user.role.is_empty(),
            _ => false,
        }
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_add_validate_revoke() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("users.db");
        let ua = UserAuth::new(&db).unwrap();

        let key = ua.add_user("alice", ROLE_USER).unwrap();
        assert!(!key.is_empty());

        let user = ua.validate_key(&key).unwrap().expect("key should be valid");
        assert_eq!(user.username, "alice");
        assert_eq!(user.role, ROLE_USER);

        ua.revoke_user("alice").unwrap();
        assert!(ua.validate_key(&key).unwrap().is_none());
    }

    #[test]
    fn test_list_users() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("users.db");
        let ua = UserAuth::new(&db).unwrap();

        ua.add_user("bob", ROLE_READONLY).unwrap();
        ua.add_user("charlie", ROLE_ADMIN).unwrap();

        let users = ua.list_users().unwrap();
        assert_eq!(users.len(), 2);
    }

    #[test]
    fn test_invalid_key() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("users.db");
        let ua = UserAuth::new(&db).unwrap();
        assert!(ua.validate_key("not-a-real-key").unwrap().is_none());
        assert!(ua.validate_key("").unwrap().is_none());
    }
}
