//! EarthGrid Node Identity — Ed25519 keypair for P2P authentication.
//!
//! Each node generates a unique Ed25519 keypair on first start.
//! The public key serves as the node's identity across the network.
//! Requests are signed with the private key; peers verify with the public key.
//!
//! Uses `libp2p::identity` (already a direct dep) which wraps `ed25519-dalek`.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// NodeIdentity
// ---------------------------------------------------------------------------

/// Ed25519 keypair for node authentication.
pub struct NodeIdentity {
    keypair: libp2p::identity::Keypair,
}

impl NodeIdentity {
    /// Load keypair from `key_path`, or generate and save a new one.
    pub fn load_or_generate(key_path: &Path) -> Result<Self> {
        if let Some(parent) = key_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating key dir {}", parent.display()))?;
        }

        if key_path.exists() {
            let b64 = std::fs::read_to_string(key_path)
                .with_context(|| format!("reading key file {}", key_path.display()))?;
            let mut raw = base64_decode(b64.trim())
                .with_context(|| "decoding base64 key file")?;

            // libp2p ed25519_from_bytes expects 32-byte secret; our file stores
            // the full 64-byte [secret||public] exported by to_bytes().
            let secret: Vec<u8> = if raw.len() == 64 {
                raw[..32].to_vec()
            } else {
                raw.clone()
            };
            raw.iter_mut().for_each(|b| *b = 0); // zero out

            let keypair = libp2p::identity::Keypair::ed25519_from_bytes(secret)
                .map_err(|e| anyhow::anyhow!("parsing ed25519 key: {e:?}"))?;

            let identity = NodeIdentity { keypair };
            info!("Loaded node identity: {}…", &identity.public_key_b64()[..16]);
            Ok(identity)
        } else {
            let keypair = libp2p::identity::Keypair::generate_ed25519();
            let identity = NodeIdentity { keypair };

            // Save 64-byte [secret||public] as base64
            let raw_bytes = identity
                .keypair
                .clone()
                .try_into_ed25519()
                .map_err(|e| anyhow::anyhow!("keypair to ed25519: {e:?}"))?
                .to_bytes();
            let b64 = base64_encode(&raw_bytes);
            std::fs::write(key_path, &b64)
                .with_context(|| format!("writing key file {}", key_path.display()))?;
            // chmod 0o600 — best effort
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(
                    key_path,
                    std::fs::Permissions::from_mode(0o600),
                );
            }

            warn!("Generated new node identity: {}…", &identity.public_key_b64()[..16]);
            Ok(identity)
        }
    }

    /// Base64-encoded public key (shareable; used as node identity across the network).
    pub fn public_key_b64(&self) -> String {
        let pub_key = self
            .keypair
            .public()
            .try_into_ed25519()
            .expect("keypair is always ed25519");
        base64_encode(pub_key.to_bytes())
    }

    /// Hex-encoded public key.
    pub fn public_key_hex(&self) -> String {
        let pub_key = self
            .keypair
            .public()
            .try_into_ed25519()
            .expect("keypair is always ed25519");
        hex::encode(pub_key.to_bytes())
    }

    /// Sign a UTF-8 message; returns base64-encoded Ed25519 signature.
    pub fn sign(&self, message: &str) -> String {
        let sig = self
            .keypair
            .sign(message.as_bytes())
            .expect("ed25519 signing never fails");
        base64_encode(&sig)
    }

    /// Verify an arbitrary signed message from a known peer.
    ///
    /// `public_key_b64` — base64-encoded 32-byte Ed25519 public key.  
    /// `signature_b64`  — base64-encoded 64-byte Ed25519 signature.  
    /// `message`        — original UTF-8 message that was signed.
    pub fn verify_request(public_key_b64: &str, signature_b64: &str, message: &str) -> bool {
        let pub_bytes = match base64_decode(public_key_b64) {
            Ok(b) => b,
            Err(_) => return false,
        };
        let sig_bytes = match base64_decode(signature_b64) {
            Ok(b) => b,
            Err(_) => return false,
        };

        let pub_key = match libp2p::identity::ed25519::PublicKey::try_from_bytes(&pub_bytes) {
            Ok(k) => k,
            Err(_) => return false,
        };

        pub_key.verify(message.as_bytes(), &sig_bytes)
    }
}

// ---------------------------------------------------------------------------
// SignedPayload
// ---------------------------------------------------------------------------

/// Payload exchanged during P2P peer authentication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedPayload {
    pub node_name: String,
    pub node_id: String,
    pub api_key: String,
    pub public_key: String,  // base64 Ed25519 public key
    pub timestamp: u64,
    pub signature: String,   // base64 Ed25519 signature
}

// ---------------------------------------------------------------------------
// Base64 helpers (hand-rolled, no dependency)
// ---------------------------------------------------------------------------

/// Encode bytes to standard (padded) base64.
fn base64_encode(data: impl AsRef<[u8]>) -> String {
    use std::fmt::Write;
    // stdlib-only base64 encoder
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let data = data.as_ref();
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    let mut i = 0;
    while i + 2 < data.len() {
        let b0 = data[i] as u32;
        let b1 = data[i + 1] as u32;
        let b2 = data[i + 2] as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        let _ = write!(out, "{}{}{}{}",
            CHARS[((n >> 18) & 63) as usize] as char,
            CHARS[((n >> 12) & 63) as usize] as char,
            CHARS[((n >> 6) & 63) as usize] as char,
            CHARS[(n & 63) as usize] as char,
        );
        i += 3;
    }
    let rem = data.len() - i;
    if rem == 1 {
        let b0 = data[i] as u32;
        let _ = write!(out, "{}{}==",
            CHARS[((b0 >> 2) & 63) as usize] as char,
            CHARS[((b0 << 4) & 63) as usize] as char,
        );
    } else if rem == 2 {
        let b0 = data[i] as u32;
        let b1 = data[i + 1] as u32;
        let _ = write!(out, "{}{}{}=",
            CHARS[((b0 >> 2) & 63) as usize] as char,
            CHARS[(((b0 << 4) | (b1 >> 4)) & 63) as usize] as char,
            CHARS[((b1 << 2) & 63) as usize] as char,
        );
    }
    out
}

/// Decode standard (padded) base64.
fn base64_decode(s: &str) -> Result<Vec<u8>> {
    const INV: [i8; 128] = {
        let mut t = [-1i8; 128];
        let chars = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut i = 0usize;
        while i < chars.len() {
            t[chars[i] as usize] = i as i8;
            i += 1;
        }
        t
    };
    let s = s.trim_end_matches('=');
    let n = s.len();
    let mut out = Vec::with_capacity(n * 3 / 4);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i + 3 < n {
        let (a, b, c, d) = (bytes[i], bytes[i+1], bytes[i+2], bytes[i+3]);
        if a > 127 || b > 127 || c > 127 || d > 127 {
            anyhow::bail!("invalid base64 character");
        }
        let (va, vb, vc, vd) = (INV[a as usize], INV[b as usize], INV[c as usize], INV[d as usize]);
        if va < 0 || vb < 0 || vc < 0 || vd < 0 {
            anyhow::bail!("invalid base64 character");
        }
        let v = ((va as u32) << 18) | ((vb as u32) << 12) | ((vc as u32) << 6) | (vd as u32);
        out.push((v >> 16) as u8);
        out.push((v >> 8) as u8);
        out.push(v as u8);
        i += 4;
    }
    let rem = n - i;
    if rem == 2 {
        let (a, b) = (bytes[i], bytes[i+1]);
        if a > 127 || b > 127 { anyhow::bail!("invalid base64"); }
        let (va, vb) = (INV[a as usize], INV[b as usize]);
        if va < 0 || vb < 0 { anyhow::bail!("invalid base64"); }
        let v = ((va as u32) << 18) | ((vb as u32) << 12);
        out.push((v >> 16) as u8);
    } else if rem == 3 {
        let (a, b, c) = (bytes[i], bytes[i+1], bytes[i+2]);
        if a > 127 || b > 127 || c > 127 { anyhow::bail!("invalid base64"); }
        let (va, vb, vc) = (INV[a as usize], INV[b as usize], INV[c as usize]);
        if va < 0 || vb < 0 || vc < 0 { anyhow::bail!("invalid base64"); }
        let v = ((va as u32) << 18) | ((vb as u32) << 12) | ((vc as u32) << 6);
        out.push((v >> 16) as u8);
        out.push((v >> 8) as u8);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_identity() -> (NodeIdentity, TempDir) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(".node_key");
        let id = NodeIdentity::load_or_generate(&path).unwrap();
        (id, dir)
    }

    #[test]
    fn generate_and_reload() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(".node_key");
        let id1 = NodeIdentity::load_or_generate(&path).unwrap();
        let pub1 = id1.public_key_b64();

        // Reload
        let id2 = NodeIdentity::load_or_generate(&path).unwrap();
        assert_eq!(pub1, id2.public_key_b64(), "reloaded key must match");
    }

    #[test]
    fn sign_and_verify_request() {
        let (id, _dir) = make_identity();
        let msg = "hello earthgrid";
        let sig = id.sign(msg);
        assert!(
            NodeIdentity::verify_request(&id.public_key_b64(), &sig, msg),
            "valid signature should verify"
        );
        assert!(
            !NodeIdentity::verify_request(&id.public_key_b64(), &sig, "tampered"),
            "tampered message should not verify"
        );
    }

    #[test]
    fn public_key_hex_is_64_chars() {
        let (id, _dir) = make_identity();
        assert_eq!(id.public_key_hex().len(), 64, "ed25519 pubkey is 32 bytes = 64 hex chars");
    }

    #[test]
    fn base64_roundtrip() {
        for data in [b"".as_slice(), b"a", b"ab", b"abc", b"abcd", &[0u8, 1, 2, 255, 128]] {
            let enc = base64_encode(data);
            let dec = base64_decode(&enc).unwrap();
            assert_eq!(dec, data, "base64 roundtrip failed for {:?}", data);
        }
    }
}
