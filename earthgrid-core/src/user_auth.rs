//! Per-user API key management for EarthGrid.
//!
//! Network-wide UUID-based API keys; user records can be synced between
//! nodes via federation.  Schema is compatible with user_auth.py.

use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
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
        // Pre-create the DB 0o600 so SQLite never creates it world-readable
        // (the -wal/-shm files inherit the main file's mode). `create_new`
        // semantics: an existing database is never opened for writing here, so
        // a concurrently starting process cannot truncate it.
        match crate::auth::create_private_file(db_path) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
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
                active      INTEGER NOT NULL DEFAULT 1,
                api_key_hash TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_users_api_key ON users(api_key);
            CREATE INDEX IF NOT EXISTS idx_users_active  ON users(active);",
        )?;
        // Migration: add last_used column if missing (pre-v0.6 DBs)
        let _ = conn.execute_batch(
            "ALTER TABLE users ADD COLUMN last_used REAL NOT NULL DEFAULT 0;"
        );
        // Migration: API keys are stored as SHA-256 digests, never in cleartext.
        let _ = conn.execute_batch(
            "ALTER TABLE users ADD COLUMN api_key_hash TEXT;"
        );
        // Migration: index the digest, so validating a key is one lookup and
        // not a scan of every user. Created here, after the column exists on
        // pre-existing DBs too.
        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_users_api_key_hash ON users(api_key_hash);"
        )?;
        // Hash the plaintext keys of pre-existing rows in place. `api_key` is
        // UNIQUE NOT NULL, so it is overwritten with a non-secret placeholder
        // derived from the digest. Every existing key keeps working.
        let tx = conn.unchecked_transaction()?;
        let mut stmt = tx.prepare("SELECT user_id, api_key FROM users WHERE api_key_hash IS NULL")?;
        let pending: Vec<(String, String)> = stmt
            .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?
            .collect::<std::result::Result<_, _>>()?;
        drop(stmt);
        for (user_id, api_key) in &pending {
            let digest = hash_api_key(api_key);
            tx.execute(
                "UPDATE users SET api_key_hash = ?1, api_key = ?2 WHERE user_id = ?3",
                params![digest, format!("sha256:{}", digest), user_id],
            )?;
        }
        tx.commit()?;
        if !pending.is_empty() {
            // Best effort: purge the old plaintext from free pages and the WAL.
            let _ = conn.execute_batch("VACUUM; PRAGMA wal_checkpoint(TRUNCATE);");
        }
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
        // Only the digest is persisted; `api_key` (UNIQUE NOT NULL) gets a
        // non-secret placeholder derived from it.
        let digest = hash_api_key(&api_key);
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO users
             (user_id, username, api_key, role, created_at, updated_at, api_key_hash)
             VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?6)",
            params![user_id, username, format!("sha256:{}", digest), role, now, digest],
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
        // Keys are stored as SHA-256 digests: hash the presented value and look
        // the digest up through idx_users_api_key_hash, so a wrong key costs one
        // index lookup. The row's digest is still compared in constant time.
        // `active` is checked on the row, not in the WHERE clause, so the
        // planner has only the digest index to choose.
        let presented = hash_api_key(key);
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT user_id, username, role, node_origin, created_at, last_used, api_key_hash, active
             FROM users WHERE api_key_hash = ?1",
        )?;
        let candidates: Vec<(AuthUser, String, i64)> = stmt
            .query_map(params![presented], |row| {
                Ok((
                    AuthUser {
                        user_id: row.get(0)?,
                        username: row.get(1)?,
                        role: row.get(2)?,
                        node_origin: row.get(3)?,
                        created_at: row.get(4)?,
                        last_used: row.get(5)?,
                    },
                    row.get::<_, String>(6)?,
                    row.get::<_, i64>(7)?,
                ))
            })?
            .collect::<std::result::Result<_, _>>()?;
        let mut matched: Option<AuthUser> = None;
        for (user, stored, active) in candidates {
            if crate::auth::constant_time_eq_str(&stored, &presented) && active == 1 {
                matched = Some(user);
            }
        }
        match matched {
            Some(user) => {
                // Update last_used (best-effort, ignore error)
                let _ = conn.execute(
                    "UPDATE users SET last_used = ?1 WHERE user_id = ?2",
                    params![unix_now(), user.user_id],
                );
                Ok(Some(user))
            }
            None => Ok(None),
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

/// SHA-256 hex digest of an API key — the only form in which keys are stored.
fn hash_api_key(key: &str) -> String {
    hex::encode(Sha256::digest(key.as_bytes()))
}

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
    fn test_key_is_not_stored_in_cleartext() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("users.db");
        let ua = UserAuth::new(&db).unwrap();
        let key = ua.add_user("dave", ROLE_USER).unwrap();

        let conn = ua.conn.lock().unwrap();
        let (stored, hash): (String, String) = conn
            .query_row("SELECT api_key, api_key_hash FROM users WHERE username = 'dave'", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert!(!stored.contains(&key), "plaintext key must not be persisted");
        assert_eq!(hash, hash_api_key(&key));
    }

    #[test]
    fn test_legacy_plaintext_keys_are_migrated_and_keep_working() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("users.db");
        {
            // Pre-migration schema with a plaintext key
            let conn = Connection::open(&db).unwrap();
            conn.execute_batch(
                "CREATE TABLE users (
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
                INSERT INTO users (user_id, username, api_key, role, created_at, updated_at)
                VALUES ('u1', 'erin', 'legacy-plaintext-key', 'admin', 1.0, 1.0);",
            )
            .unwrap();
        }

        let ua = UserAuth::new(&db).unwrap();
        let user = ua.validate_key("legacy-plaintext-key").unwrap().expect("legacy key still valid");
        assert_eq!(user.username, "erin");
        assert_eq!(user.role, ROLE_ADMIN);

        // Re-opening is idempotent: no double hashing
        drop(ua);
        let ua = UserAuth::new(&db).unwrap();
        assert!(ua.validate_key("legacy-plaintext-key").unwrap().is_some());

        let conn = ua.conn.lock().unwrap();
        let stored: String = conn
            .query_row("SELECT api_key FROM users WHERE user_id = 'u1'", [], |r| r.get(0))
            .unwrap();
        assert_ne!(stored, "legacy-plaintext-key");
        // The stored placeholder is not itself a credential
        drop(conn);
        assert!(ua.validate_key(&stored).unwrap().is_none());
    }

    #[test]
    fn test_key_lookup_uses_the_digest_index() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("users.db");
        let ua = UserAuth::new(&db).unwrap();
        let conn = ua.conn.lock().unwrap();
        let plan: Vec<String> = conn
            .prepare(
                "EXPLAIN QUERY PLAN SELECT user_id, active FROM users WHERE api_key_hash = ?1",
            )
            .unwrap()
            .query_map(params!["x"], |r| r.get::<_, String>(3))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert!(
            plan.iter().any(|p| p.contains("idx_users_api_key_hash")),
            "key validation must be an index lookup, not a scan: {plan:?}"
        );
    }

    #[test]
    fn test_opening_an_existing_db_never_truncates_it() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("users.db");
        let first = UserAuth::new(&db).unwrap();
        let key = first.add_user("frank", ROLE_USER).unwrap();

        // A second opener (another process starting up) while the first is live
        let second = UserAuth::new(&db).unwrap();
        assert!(second.validate_key(&key).unwrap().is_some());
        assert!(first.validate_key(&key).unwrap().is_some());
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
