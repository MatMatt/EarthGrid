//! Authentication and authorization for EarthGrid.
//!
//! API key-based auth with two tiers:
//! - `api_key`: required for write operations (ingest, process, sync)
//! - `admin_key`: required for destructive operations (delete, audit)
//!
//! If no keys are configured, the node operates in open mode (backward compatible).

use crate::error::{EarthGridError, Result};

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

    /// Validate a write operation.
    pub fn check_write(&self, provided_key: Option<&str>) -> Result<()> {
        if self.api_key.is_empty() {
            return Ok(()); // Open mode
        }
        match provided_key {
            Some(key) if key == self.api_key => Ok(()),
            Some(key) if key == self.admin_key && !self.admin_key.is_empty() => Ok(()),
            _ => Err(EarthGridError::AuthRequired),
        }
    }

    /// Validate an admin/destructive operation.
    pub fn check_admin(&self, provided_key: Option<&str>) -> Result<()> {
        if self.api_key.is_empty() && self.admin_key.is_empty() {
            return Ok(()); // Fully open mode
        }
        if self.admin_key.is_empty() {
            return Err(EarthGridError::Forbidden); // No admin key = blocked
        }
        match provided_key {
            Some(key) if key == self.admin_key => Ok(()),
            _ => Err(EarthGridError::AuthRequired),
        }
    }
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
        // Admin ops blocked entirely when no admin key configured
        assert!(auth.check_admin(Some("write-key")).is_err());
    }

    #[test]
    fn test_admin_key_also_writes() {
        let auth = AuthConfig {
            api_key: "write-key".to_string(),
            admin_key: "admin-key".to_string(),
        };
        // Admin key should also allow write operations
        assert!(auth.check_write(Some("admin-key")).is_ok());
    }
}
