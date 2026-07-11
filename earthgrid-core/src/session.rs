//! Cookie-based session management for the EarthGrid Web UI.
//!
//! Sessions are HMAC-SHA256 signed tokens stored in an HttpOnly cookie.
//! Token format: `username|role|expiry_unix|signature`
//! No server-side session store needed — the signature is the proof.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::path::Path;

type HmacSha256 = Hmac<Sha256>;

/// Default session lifetime: 30 days.
const SESSION_LIFETIME_SECS: u64 = 2_592_000;

/// Cookie name for UI sessions.
pub const SESSION_COOKIE: &str = "eg_session";

/// Create a signed session token.
///
/// Token payload: `username|role|expiry_unix`
/// Signature: HMAC-SHA256(payload, secret)
/// Final token: `payload|hex(signature)`
pub fn create_token(username: &str, role: &str, secret: &[u8]) -> String {
    let expiry = unix_now() + SESSION_LIFETIME_SECS;
    let payload = format!("{}|{}|{}", username, role, expiry);
    let sig = sign(&payload, secret);
    format!("{}|{}", payload, sig)
}

/// Validate a session token and return (username, role) if valid.
pub fn validate_token(token: &str, secret: &[u8]) -> Option<(String, String)> {
    // Token: username|role|expiry|signature
    let parts: Vec<&str> = token.splitn(4, '|').collect();
    if parts.len() != 4 {
        return None;
    }
    let username = parts[0];
    let role = parts[1];
    let expiry: u64 = parts[2].parse().ok()?;
    let provided_sig = parts[3];

    // Check expiry
    if unix_now() > expiry {
        return None;
    }

    // Verify signature
    let payload = format!("{}|{}|{}", username, role, expiry);
    let expected_sig = sign(&payload, secret);
    if !constant_time_eq(provided_sig.as_bytes(), expected_sig.as_bytes()) {
        return None;
    }

    Some((username.to_string(), role.to_string()))
}

/// Extract the session cookie value from a Cookie header string.
pub fn extract_cookie(cookie_header: &str) -> Option<String> {
    for part in cookie_header.split(';') {
        let trimmed = part.trim();
        if let Some(val) = trimmed.strip_prefix(&format!("{}=", SESSION_COOKIE)) {
            let val = val.trim();
            if !val.is_empty() {
                return Some(val.to_string());
            }
        }
    }
    None
}

/// Build a Set-Cookie header value for login.
pub fn set_cookie(token: &str) -> String {
    format!(
        "{}={}; Path=/; HttpOnly; SameSite=Strict; Max-Age={}",
        SESSION_COOKIE, token, SESSION_LIFETIME_SECS
    )
}

/// Build a Set-Cookie header value that clears the session.
pub fn clear_cookie() -> String {
    format!(
        "{}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0",
        SESSION_COOKIE
    )
}

/// Load or generate the session signing secret.
///
/// Looks for `data_dir/.session_secret` first. If it doesn't exist, generates
/// 32 random bytes, writes them (mode 0600), and uses those. Never falls back
/// to env vars or hardcoded defaults — the secret is always a persisted
/// random value independent of API keys.
pub fn session_secret(data_dir: &Path) -> Vec<u8> {
    let secret_path = data_dir.join(".session_secret");

    if let Ok(existing) = std::fs::read(&secret_path) {
        if existing.len() >= 32 {
            return existing;
        }
    }

    // Generate new secret from OS randomness
    let mut secret = vec![0u8; 32];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| std::io::Read::read_exact(&mut f, &mut secret))
        .unwrap_or_else(|_| {
            // Fallback: use a hash of current time + pid (worse but better than hardcoded)
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            std::time::SystemTime::now().hash(&mut h);
            std::process::id().hash(&mut h);
            let hash = h.finish();
            for i in 0..32 {
                secret[i] = ((hash >> ((i % 8) * 8)) & 0xFF) as u8;
            }
        });

    if let Some(parent) = secret_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(&secret_path, &secret);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&secret_path, std::fs::Permissions::from_mode(0o600));
    }

    secret
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn sign(payload: &str, secret: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret)
        .expect("HMAC accepts any key length");
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Constant-time comparison to prevent timing attacks.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_validate() {
        let secret = b"test-secret-key";
        let token = create_token("alice", "admin", secret);
        let result = validate_token(&token, secret);
        assert!(result.is_some());
        let (user, role) = result.unwrap();
        assert_eq!(user, "alice");
        assert_eq!(role, "admin");
    }

    #[test]
    fn test_wrong_secret() {
        let token = create_token("bob", "user", b"secret-1");
        assert!(validate_token(&token, b"secret-2").is_none());
    }

    #[test]
    fn test_tampered_token() {
        let secret = b"my-secret";
        let token = create_token("carol", "readonly", secret);
        // Change role in token
        let tampered = token.replacen("readonly", "admin", 1);
        assert!(validate_token(&tampered, secret).is_none());
    }

    #[test]
    fn test_extract_cookie() {
        let header = "eg_session=abc123; other=xyz";
        assert_eq!(extract_cookie(header), Some("abc123".to_string()));

        let header2 = "other=xyz; eg_session=token_value";
        assert_eq!(extract_cookie(header2), Some("token_value".to_string()));

        let header3 = "other=xyz";
        assert_eq!(extract_cookie(header3), None);
    }

    #[test]
    fn test_set_clear_cookie() {
        let cookie = set_cookie("mytoken");
        assert!(cookie.contains("eg_session=mytoken"));
        assert!(cookie.contains("HttpOnly"));
        assert!(cookie.contains("SameSite=Strict"));

        let clear = clear_cookie();
        assert!(clear.contains("Max-Age=0"));
    }

    #[test]
    fn test_persisted_secret() {
        let dir = tempfile::tempdir().unwrap();
        let secret = session_secret(dir.path());
        assert_eq!(secret.len(), 32);

        // Second call returns the same secret
        let secret2 = session_secret(dir.path());
        assert_eq!(secret, secret2);
    }
}