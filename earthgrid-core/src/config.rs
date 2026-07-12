//! EarthGrid node configuration.
//!
//! Config is stored as JSON at `~/.earthgrid/config.json` (no `toml` crate available).
//! All fields can also be overridden via `EARTHGRID_*` environment variables.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Settings {
    pub node_id: String,
    pub node_name: String,
    pub store_path: PathBuf,
    pub catalog_path: PathBuf,
    pub host: String,
    pub port: u16,
    pub peers: Vec<String>,
    pub role: String,              // "node" or "beacon"
    pub beacon_url: String,
    pub beacon_peers: Vec<String>,
    pub public_url: String,
    pub storage_limit_gb: f64,
    pub also_beacon: bool,
    pub api_key: String,
    pub admin_key: String,
    pub require_auth_read: bool,
    pub auto_update: String,       // "yes" | "ask" | "no"

    // Replication
    pub replication_factor: u32,
    pub preferred_collections: String,
    pub preferred_bbox: String,    // "west,south,east,north"

    // Paths
    pub identity_key_path: String,
    pub users_db: String,
    pub source_key: String,
    pub source_users_db: String,
    pub stats_db: String,
    pub beacon_db: String,

    // Bandwidth
    pub bw_limit_mbps: f64,
    pub bw_schedule: String,       // JSON: {"8": 50, "18": 75}
    pub max_download_volume_gb: f64,

    // Stats
    pub stats_retain_days: u32,

    // Community
    pub sponsor_name: String,
    pub sponsor_url: String,
    pub node_url: String,
    pub group: String,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            node_id: String::new(),
            node_name: String::new(),
            store_path: PathBuf::from("./data/store"),
            catalog_path: PathBuf::from("./data/catalog.db"),
            host: "0.0.0.0".to_string(),
            port: 8400,
            peers: Vec::new(),
            role: "node".to_string(),
            beacon_url: "http://mattiuzzi.zapto.org/earthgrid".to_string(),
            beacon_peers: Vec::new(),
            public_url: String::new(),
            storage_limit_gb: 50.0,
            also_beacon: false,
            api_key: String::new(),
            admin_key: String::new(),
            require_auth_read: false,
            auto_update: "no".to_string(),
            replication_factor: 2,
            preferred_collections: String::new(),
            preferred_bbox: String::new(),
            identity_key_path: "./data/.node_key".to_string(),
            users_db: "./data/users.db".to_string(),
            source_key: String::new(),
            source_users_db: "./data/source_users.db".to_string(),
            stats_db: "./data/stats.db".to_string(),
            beacon_db: "./data/beacon.db".to_string(),
            bw_limit_mbps: 0.0,
            bw_schedule: String::new(),
            max_download_volume_gb: 0.0,
            stats_retain_days: 90,
            sponsor_name: String::new(),
            sponsor_url: String::new(),
            node_url: String::new(),
            group: String::new(),
        }
    }
}

impl Settings {
    // -----------------------------------------------------------------------
    // Load / save
    // -----------------------------------------------------------------------

    /// Load settings from a JSON config file. Missing fields use defaults.
    /// Default beacon URL baked into the binary.
    pub const DEFAULT_BEACON_URL: &str = "https://mattiuzzi.zapto.org/earthgrid";

    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading config from {}", path.display()))?;
        let mut s: Self = serde_json::from_str(&raw)
            .with_context(|| format!("parsing config JSON from {}", path.display()))?;
        // If beacon_url is empty after loading config, use the compiled default
        if s.beacon_url.is_empty() {
            s.beacon_url = Self::DEFAULT_BEACON_URL.to_string();
        }
        s.apply_env_overrides();
        s.ensure_node_id();
        s.ensure_node_name();
        Ok(s)
    }

    /// Load from default path, or create a default config if missing.
    pub fn load_or_default() -> Result<Self> {
        let path = Self::default_config_path();
        if path.exists() {
            Self::load(&path)
        } else {
            let mut s = Self::default();
            s.apply_env_overrides();
            s.ensure_node_id();
            s.ensure_node_name();
            Ok(s)
        }
    }

    /// Save settings as a JSON file.
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating config dir {}", parent.display()))?;
        }
        let json = serde_json::to_string_pretty(self).context("serializing config")?;
        std::fs::write(path, &json)
            .with_context(|| format!("writing config to {}", path.display()))?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Paths
    // -----------------------------------------------------------------------

    /// Default config directory: `~/.earthgrid/`
    pub fn config_dir() -> PathBuf {
        dirs_home().join(".earthgrid")
    }

    /// Default config file path: `~/.earthgrid/config.json`
    pub fn default_config_path() -> PathBuf {
        Self::config_dir().join("config.json")
    }

    /// Convenience: `http://{host}:{port}`
    pub fn base_url(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Apply `EARTHGRID_*` environment variable overrides.
    fn apply_env_overrides(&mut self) {
        macro_rules! env_str {
            ($var:expr, $field:expr) => {
                if let Ok(v) = std::env::var($var) {
                    *$field = v;
                }
            };
        }
        macro_rules! env_bool {
            ($var:expr, $field:expr) => {
                if let Ok(v) = std::env::var($var) {
                    *$field = matches!(v.to_lowercase().as_str(), "1" | "true" | "yes");
                }
            };
        }
        macro_rules! env_u16 {
            ($var:expr, $field:expr) => {
                if let Ok(v) = std::env::var($var) {
                    if let Ok(n) = v.parse() {
                        *$field = n;
                    }
                }
            };
        }
        macro_rules! env_f64 {
            ($var:expr, $field:expr) => {
                if let Ok(v) = std::env::var($var) {
                    if let Ok(n) = v.parse() {
                        *$field = n;
                    }
                }
            };
        }
        env_str!("EARTHGRID_NODE_ID", &mut self.node_id);
        env_str!("EARTHGRID_NODE_NAME", &mut self.node_name);
        env_str!("EARTHGRID_HOST", &mut self.host);
        env_u16!("EARTHGRID_PORT", &mut self.port);
        env_str!("EARTHGRID_ROLE", &mut self.role);
        env_str!("EARTHGRID_BEACON_URL", &mut self.beacon_url);
        env_str!("EARTHGRID_PUBLIC_URL", &mut self.public_url);
        env_f64!("EARTHGRID_STORAGE_LIMIT_GB", &mut self.storage_limit_gb);
        env_bool!("EARTHGRID_ALSO_BEACON", &mut self.also_beacon);
        env_str!("EARTHGRID_API_KEY", &mut self.api_key);
        env_str!("EARTHGRID_ADMIN_KEY", &mut self.admin_key);
        env_bool!("EARTHGRID_REQUIRE_AUTH_READ", &mut self.require_auth_read);
        env_str!("EARTHGRID_AUTO_UPDATE", &mut self.auto_update);
    }

    /// Auto-generate `node_id` if empty, persisting it to `~/.earthgrid/.node_id`.
    fn ensure_node_id(&mut self) {
        if !self.node_id.is_empty() {
            return;
        }
        let id_file = Self::config_dir().join(".node_id");
        if id_file.exists() {
            if let Ok(id) = std::fs::read_to_string(&id_file) {
                let id = id.trim().to_string();
                if !id.is_empty() {
                    self.node_id = id;
                    return;
                }
            }
        }
        // Generate a short UUID-based ID
        let id = uuid::Uuid::new_v4().as_simple().to_string()[..12].to_string();
        if let Some(parent) = id_file.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = std::fs::write(&id_file, &id);
        self.node_id = id;
    }

    /// Auto-generate a memorable `node_name` if empty.
    fn ensure_node_name(&mut self) {
        if !self.node_name.is_empty() {
            return;
        }
        const ADJ: &[&str] = &[
            "swift", "bold", "calm", "dark", "fair", "keen", "wild",
            "warm", "cool", "free", "pure", "vast", "deep", "high",
            "blue", "gold", "iron", "jade", "onyx", "ruby",
        ];
        const NOUN: &[&str] = &[
            "peak", "lake", "reef", "mesa", "vale", "cove", "dune",
            "glen", "rift", "ford", "cape", "isle", "arch", "dale",
            "knoll", "ridge", "brook", "cliff", "grove", "shore",
        ];
        // Deterministic seed from node_id bytes so the name is stable
        let seed = self.node_id.bytes().fold(0usize, |acc, b| acc.wrapping_add(b as usize));
        let adj = ADJ[seed % ADJ.len()];
        let noun = NOUN[(seed / ADJ.len()) % NOUN.len()];
        let suffix = &self.node_id[..4.min(self.node_id.len())];
        self.node_name = format!("{}-{}-{}", adj, noun, suffix);
    }
}

// ---------------------------------------------------------------------------
// Home directory helper
// ---------------------------------------------------------------------------

fn dirs_home() -> PathBuf {
    std::env::var("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp"))
}

// ---------------------------------------------------------------------------
// Public IP detection
// ---------------------------------------------------------------------------

/// Try to detect the public IP by querying well-known services.
/// Uses plain HTTP (port 80) via stdlib to avoid async dependencies.
pub fn detect_public_ip() -> Option<String> {
    let services: &[(&str, u16, &str)] = &[
        (
            "ifconfig.me",
            80,
            "GET /ip HTTP/1.0\r\nHost: ifconfig.me\r\nUser-Agent: EarthGrid\r\n\r\n",
        ),
        (
            "api.ipify.org",
            80,
            "GET / HTTP/1.0\r\nHost: api.ipify.org\r\nUser-Agent: EarthGrid\r\n\r\n",
        ),
        (
            "icanhazip.com",
            80,
            "GET / HTTP/1.0\r\nHost: icanhazip.com\r\nUser-Agent: EarthGrid\r\n\r\n",
        ),
    ];

    for (host, port, request) in services {
        if let Ok(mut stream) = std::net::TcpStream::connect((*host, *port)) {
            let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(5)));
            let _ = stream.set_write_timeout(Some(std::time::Duration::from_secs(5)));
            if stream.write_all(request.as_bytes()).is_ok() {
                let mut response = String::new();
                let _ = stream.read_to_string(&mut response);
                if let Some(body) = response.split("\r\n\r\n").nth(1) {
                    let ip = body.trim().to_string();
                    if !ip.is_empty() && ip.contains('.') && ip.len() <= 15 {
                        return Some(ip);
                    }
                }
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn default_settings_are_sane() {
        let s = Settings::default();
        assert_eq!(s.host, "0.0.0.0");
        assert_eq!(s.port, 8400);
        assert_eq!(s.role, "node");
        assert!((s.storage_limit_gb - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn save_and_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.json");

        let mut s = Settings::default();
        s.node_id = "test-node-01".to_string();
        s.node_name = "test-node".to_string();
        s.port = 9000;
        s.peers = vec!["http://peer1:8400".to_string()];

        s.save(&path).unwrap();
        assert!(path.exists());

        let loaded = Settings::load(&path).unwrap();
        assert_eq!(loaded.node_id, "test-node-01");
        assert_eq!(loaded.node_name, "test-node");
        assert_eq!(loaded.port, 9000);
        assert_eq!(loaded.peers, vec!["http://peer1:8400".to_string()]);
    }

    #[test]
    fn ensure_node_id_generates_id() {
        let mut s = Settings::default();
        // override home so we don't pollute real ~/.earthgrid
        // Just call directly — it will write to ~/.earthgrid/.node_id but that's OK for CI
        s.ensure_node_id();
        assert!(!s.node_id.is_empty());
        assert_eq!(s.node_id.len(), 12);
    }

    #[test]
    fn ensure_node_name_is_deterministic() {
        let mut s = Settings::default();
        s.node_id = "abc123def456".to_string();
        s.ensure_node_name();
        let name1 = s.node_name.clone();
        s.node_name = String::new();
        s.ensure_node_name();
        assert_eq!(s.node_name, name1, "name should be deterministic from node_id");
    }

    #[test]
    fn base_url_format() {
        let mut s = Settings::default();
        s.host = "127.0.0.1".to_string();
        s.port = 8400;
        assert_eq!(s.base_url(), "http://127.0.0.1:8400");
    }
}
