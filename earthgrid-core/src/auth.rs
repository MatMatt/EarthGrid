//! Authentication and authorization for EarthGrid.
//!
//! Unified auth model with two identity planes, one enforcement point.
//!
//! - **Humans**: per-user keys from `users.db` (roles admin/user/readonly),
//!   presented as `x-api-key`/Bearer on API calls, or exchanged for a session cookie.
//! - **Nodes**: shared grid key (`EARTHGRID_API_KEY`) for peer coordination.
//! - **Localhost**: no bypass - loopback connections authenticate like any other.
//!
//! The `authorize()` function is the single entry point for all auth checks.

use crate::error::{EarthGridError, Result};
use crate::user_auth::AuthUser;

/// What level of access is required.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AccessLevel {
    Write,
    Admin,
}

/// Who was authenticated and how.
#[derive(Debug, Clone)]
pub enum Identity {
    /// Open mode (auth not enabled) - full trust. Loopback peers are NOT trusted.
    Localhost,
    /// Authenticated via shared grid key (EARTHGRID_API_KEY / EARTHGRID_ADMIN_KEY).
    GridKey,
    /// Authenticated via per-user key from users.db.
    User(AuthUser),
    /// Authenticated via session cookie.
    Session { username: String, role: String },
}

/// Authentication configuration.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// API key for write operations. Empty = open access.
    pub api_key: String,
    /// Admin key for destructive operations. Empty = blocked if api_key is set.
    pub admin_key: String,
}

impl AuthConfig {
    /// Create auth config from environment variables.
    pub fn from_env() -> Self {
        Self {
            api_key: std::env::var("EARTHGRID_API_KEY").unwrap_or_default(),
            admin_key: std::env::var("EARTHGRID_ADMIN_KEY").unwrap_or_default(),
        }
    }

    /// Check if auth is enabled.
    pub fn is_enabled(&self) -> bool {
        !self.api_key.is_empty()
    }

    /// Validate a write operation against env keys.
    pub fn check_write(&self, provided_key: Option<&str>) -> Result<()> {
        if self.api_key.is_empty() {
            return Ok(()); // Open mode
        }
        match provided_key {
            Some(key) if constant_time_eq_str(key, &self.api_key) => Ok(()),
            Some(key) if !self.admin_key.is_empty() && constant_time_eq_str(key, &self.admin_key) => Ok(()),
            _ => Err(EarthGridError::AuthRequired),
        }
    }

    /// Validate an admin/destructive operation against env keys.
    pub fn check_admin(&self, provided_key: Option<&str>) -> Result<()> {
        if self.api_key.is_empty() && self.admin_key.is_empty() {
            return Ok(()); // Fully open mode
        }
        if self.admin_key.is_empty() {
            return Err(EarthGridError::Forbidden); // No admin key = blocked
        }
        match provided_key {
            Some(key) if constant_time_eq_str(key, &self.admin_key) => Ok(()),
            _ => Err(EarthGridError::AuthRequired),
        }
    }
}

/// Constant-time string comparison to prevent timing attacks on key checks.
pub(crate) fn constant_time_eq_str(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.bytes().zip(b.bytes()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Create a new secret file with mode 0600 set at creation time, so there is no
/// window in which its contents are world-readable.
///
/// Never opens a path it did not just create: an existing file is left
/// untouched and the call fails with `ErrorKind::AlreadyExists`. There is no
/// truncating open, so a second process racing this one cannot empty a file
/// the first has already written.
pub(crate) fn create_private_file(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    opts.open(path)
}

/// Atomically create the secret file `path` (mode 0600) holding `contents`.
///
/// The bytes go into a private temp file beside `path`, which is then
/// hard-linked into place. The link is exclusive — it fails with
/// `ErrorKind::AlreadyExists` when `path` is there, and unlike a rename never
/// replaces it — and the name only ever appears with the complete contents, so
/// a concurrent reader sees no file or the whole secret, never a partial one.
/// Where hard links are unsupported it falls back to [`create_private_file`]
/// on `path` itself: still exclusive, but visible mid-write, which is why
/// callers that lose the race re-read with a short bounded retry.
///
/// An existing `path` is never unlinked, truncated or overwritten.
pub(crate) fn write_new_private_file(path: &std::path::Path, contents: &[u8]) -> std::io::Result<()> {
    use std::io::Write;

    let mut tmp_name = path.file_name().unwrap_or_default().to_os_string();
    tmp_name.push(format!(".{}.{}.tmp", std::process::id(), uuid::Uuid::new_v4().simple()));
    let tmp = path.with_file_name(tmp_name);

    let mut f = create_private_file(&tmp)?;
    let written = f.write_all(contents).and_then(|_| f.sync_all());
    drop(f);
    let result = written.and_then(|_| match std::fs::hard_link(&tmp, path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(e),
        Err(_) => create_private_file(path).and_then(|mut f| f.write_all(contents).and_then(|_| f.sync_all())),
    });
    let _ = std::fs::remove_file(&tmp);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_mode() {
        let auth = AuthConfig {
            api_key: String::new(),
            admin_key: String::new(),
        };
        assert!(!auth.is_enabled());
        assert!(auth.check_write(None).is_ok());
        assert!(auth.check_admin(None).is_ok());
    }

    #[test]
    fn test_write_auth() {
        let auth = AuthConfig {
            api_key: "secret".to_string(),
            admin_key: String::new(),
        };
        assert!(auth.is_enabled());
        assert!(auth.check_write(Some("secret")).is_ok());
        assert!(auth.check_write(Some("wrong")).is_err());
        assert!(auth.check_write(None).is_err());
    }

    #[test]
    fn test_admin_auth() {
        let auth = AuthConfig {
            api_key: "write-key".to_string(),
            admin_key: "admin-key".to_string(),
        };
        assert!(auth.check_admin(Some("admin-key")).is_ok());
        assert!(auth.check_admin(Some("write-key")).is_err());
        assert!(auth.check_admin(None).is_err());
    }

    #[test]
    fn test_admin_blocked_without_key() {
        let auth = AuthConfig {
            api_key: "write-key".to_string(),
            admin_key: String::new(),
        };
        assert!(auth.check_admin(Some("write-key")).is_err());
    }

    #[test]
    fn test_admin_key_also_writes() {
        let auth = AuthConfig {
            api_key: "write-key".to_string(),
            admin_key: "admin-key".to_string(),
        };
        assert!(auth.check_write(Some("admin-key")).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn test_create_private_file_is_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret");
        create_private_file(&path).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn test_create_private_file_never_truncates_an_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret");
        std::fs::write(&path, b"already here").unwrap();
        let e = create_private_file(&path).unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&path).unwrap(), b"already here");
    }

    #[test]
    fn test_write_new_private_file_is_exclusive_and_leaves_no_temp_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret");
        write_new_private_file(&path, b"winner").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"winner");

        // The loser of the race gets AlreadyExists and changes nothing
        let e = write_new_private_file(&path, b"loser").unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&path).unwrap(), b"winner");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1, "temp files must be cleaned up");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
    }
}