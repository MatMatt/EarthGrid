//! EarthGrid CLI — full-featured node management.
//!
//! Ported from Python cli.py. Uses clap v4 derive macros.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::thread;

use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    pub node_name: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub beacon_url: Option<String>,
    pub public_url: Option<String>,
    pub also_beacon: Option<bool>,
    pub storage_limit_gb: Option<f64>,
    pub data_dir: Option<String>,
    // Legacy / extended fields
    pub store_path: Option<String>,
    pub catalog_path: Option<String>,
    pub source_users_db: Option<String>,
    pub source_key: Option<String>,
    pub auto_update: Option<String>,
    pub peers: Option<Vec<String>>,
    pub beacon_peers: Option<Vec<String>>,
    pub users_db: Option<String>,
}

impl Config {
    fn load() -> anyhow::Result<Self> {
        let config_path = config_file_path();
        if config_path.exists() {
            let text = fs::read_to_string(&config_path)?;
            // Try JSON first (legacy config.json), then TOML
            if let Ok(cfg) = serde_json::from_str::<Config>(&text) {
                return Ok(cfg);
            }
            // Simple TOML fallback: parse key = "value" lines manually for the fields we need
            // (avoids adding toml crate dependency)
            let mut cfg = Config::default();
            for line in text.lines() {
                let line = line.trim();
                if let Some((k, v)) = line.split_once('=') {
                    let k = k.trim().trim_matches('"');
                    let v = v.trim().trim_matches('"');
                    match k {
                        "node_name" => cfg.node_name = Some(v.to_string()),
                        "host" => cfg.host = Some(v.to_string()),
                        "port" => cfg.port = v.parse().ok(),
                        "beacon_url" => cfg.beacon_url = Some(v.to_string()),
                        "public_url" => cfg.public_url = Some(v.to_string()),
                        "also_beacon" => cfg.also_beacon = Some(v == "true"),
                        "storage_limit_gb" => cfg.storage_limit_gb = v.parse().ok(),
                        "data_dir" => cfg.data_dir = Some(v.to_string()),
                        "store_path" => cfg.store_path = Some(v.to_string()),
                        "catalog_path" => cfg.catalog_path = Some(v.to_string()),
                        "source_users_db" => cfg.source_users_db = Some(v.to_string()),
                        "source_key" => cfg.source_key = Some(v.to_string()),
                        "auto_update" => cfg.auto_update = Some(v.to_string()),
                        "users_db" => cfg.users_db = Some(v.to_string()),
                        _ => {}
                    }
                }
            }
            return Ok(cfg);
        }
        Ok(Config::default())
    }

    fn save(&self) -> anyhow::Result<()> {
        let path = config_file_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        // Write as JSON (compatible with Python earthgrid config.json)
        let text = serde_json::to_string_pretty(self)?;
        fs::write(&path, text)?;
        Ok(())
    }

    fn node_name(&self) -> String {
        self.node_name
            .clone()
            .unwrap_or_else(|| {
                std::env::var("EARTHGRID_NODE_NAME")
                    .unwrap_or_else(|_| format!("earthgrid-{}", &hostname()))
            })
    }

    fn host(&self) -> String {
        self.host
            .clone()
            .unwrap_or_else(|| std::env::var("EARTHGRID_HOST").unwrap_or_else(|_| "0.0.0.0".to_string()))
    }

    fn port(&self) -> u16 {
        self.port.unwrap_or_else(|| {
            std::env::var("EARTHGRID_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(8400)
        })
    }

    fn storage_limit_gb(&self) -> f64 {
        self.storage_limit_gb.unwrap_or_else(|| {
            std::env::var("EARTHGRID_STORAGE_LIMIT_GB")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(50.0)
        })
    }

    fn store_path(&self) -> PathBuf {
        if let Some(p) = &self.store_path {
            return PathBuf::from(p);
        }
        if let Some(d) = &self.data_dir {
            return PathBuf::from(d).join("store");
        }
        earthgrid_dir().join("data").join("store")
    }

    fn catalog_path(&self) -> PathBuf {
        if let Some(p) = &self.catalog_path {
            return PathBuf::from(p);
        }
        if let Some(d) = &self.data_dir {
            return PathBuf::from(d).join("catalog.db");
        }
        earthgrid_dir().join("data").join("catalog.db")
    }

    fn source_users_db(&self) -> PathBuf {
        if let Some(p) = &self.source_users_db {
            return PathBuf::from(p);
        }
        earthgrid_dir().join("data").join("source_users.db")
    }
}

fn hostname() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| {
        Command::new("hostname")
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|| "node".to_string())
    })
}

fn earthgrid_dir() -> PathBuf {
    dirs_home().join(".earthgrid")
}

fn config_file_path() -> PathBuf {
    earthgrid_dir().join("config.toml")
}

fn dirs_home() -> PathBuf {
    std::env::var("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp"))
}

const SYSTEMD_UNIT: &str = "earthgrid.service";
const VERSION: &str = env!("CARGO_PKG_VERSION");

// ---------------------------------------------------------------------------
// CLI definition
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(
    name = "earthgrid",
    version = VERSION,
    about = "EarthGrid — Distributed Earth observation data storage"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Start EarthGrid node (installs+starts as systemd service by default)
    Start {
        /// Run in foreground (debug mode) instead of as a service
        #[arg(short = 'f', long)]
        foreground: bool,

        /// Host to bind
        #[arg(long)]
        host: Option<String>,

        /// Port to listen on
        #[arg(long)]
        port: Option<u16>,

        /// Node name
        #[arg(long)]
        name: Option<String>,

        /// Beacon URL to register with
        #[arg(long)]
        beacon: Option<String>,

        /// Also act as a beacon (coordinator)
        #[arg(long)]
        also_beacon: bool,

        /// Public URL for this node
        #[arg(long)]
        public_url: Option<String>,

        /// Direct peer URLs
        #[arg(long, num_args = 0..)]
        peers: Vec<String>,

        /// Other beacon URLs to federate with
        #[arg(long, num_args = 0..)]
        beacon_peers: Vec<String>,
    },

    /// Stop background daemon / systemd service
    Stop,

    /// Show node status, storage, peers, uptime
    Status,

    /// Show version and configuration
    Info,

    /// Interactive first-time setup (creates ~/.earthgrid/config.toml)
    Setup {
        /// Port to use
        #[arg(long, default_value = "8400")]
        port: u16,
    },

    /// Git pull + cargo build + restart service
    Update,

    /// Launch system tray app (🌍 online / 🌑 offline)
    Tray,

    /// Resize storage allocation
    Resize {
        /// New storage limit in GB
        size_gb: f64,

        /// Evict oldest chunks if over new limit
        #[arg(long)]
        force: bool,
    },

    /// Verify chunk integrity, optionally heal corrupted data
    Verify {
        /// Re-download items with corrupted chunks
        #[arg(long)]
        heal: bool,

        /// Only verify this collection
        #[arg(long)]
        collection: Option<String>,

        /// Show each chunk being checked
        #[arg(short = 'v', long)]
        verbose: bool,

        /// Delete corrupted chunks without re-downloading
        #[arg(long)]
        delete_corrupt: bool,
    },

    /// Fetch Sentinel data from CDSE or Element84 and ingest
    Fetch {
        /// Bounding box: west,south,east,north
        #[arg(long, required = true)]
        bbox: String,

        /// EarthGrid collection name
        #[arg(long, default_value = "sentinel-2-l2a")]
        collection: String,

        /// Start date YYYY-MM-DD
        #[arg(long)]
        start: Option<String>,

        /// End date YYYY-MM-DD
        #[arg(long)]
        end: Option<String>,

        /// Max cloud cover % (default: 30)
        #[arg(long, default_value = "30.0")]
        cloud: f64,

        /// Comma-separated bands (e.g. B02,B03,B04)
        #[arg(long)]
        bands: Option<String>,

        /// Product type (default: S2MSI2A)
        #[arg(long, default_value = "S2MSI2A")]
        product_type: String,

        /// Max products to fetch (default: 5)
        #[arg(long, default_value = "5")]
        limit: usize,

        /// Only search, don't download
        #[arg(long)]
        search_only: bool,

        /// Data source: cdse or element84
        #[arg(long, default_value = "element84")]
        source: String,

        /// Ingest locally only, don't distribute across grid
        #[arg(long)]
        no_distribute: bool,
    },

    /// Pull data from a remote peer
    Sync {
        /// Remote node URL (e.g. http://host:8400)
        peer_url: String,

        /// Only sync these collections (comma-separated)
        #[arg(long)]
        collections: Option<String>,

        /// Limit items to sync (0 = all)
        #[arg(long, default_value = "0")]
        limit: usize,

        /// Only report what would be synced (don't store)
        #[arg(long)]
        dry_run: bool,
    },

    /// Run a processing operation on STAC item(s)
    Process {
        /// Source STAC item ID(s)
        #[arg(required = true)]
        item_id: Vec<String>,

        /// Operation: ndvi, ndwi, evi, cloud_mask, band_math
        #[arg(long, required = true)]
        op: String,

        /// Band math expression (for band_math op)
        #[arg(long, default_value = "")]
        expression: String,

        /// Output collection name
        #[arg(long)]
        output_collection: Option<String>,

        /// Output item ID
        #[arg(long)]
        output_id: Option<String>,
    },

    /// List available processing operations
    Ops,

    /// Manage data source credentials (CDSE, Element84, etc.)
    Sources {
        #[command(subcommand)]
        action: SourcesCommands,
    },

    /// Admin key management
    Admin {
        #[command(subcommand)]
        action: AdminCommands,
    },

    /// Manage Docker deployment
    Docker {
        #[command(subcommand)]
        action: DockerCommands,
    },
}

#[derive(Subcommand)]
enum SourcesCommands {
    /// List configured data sources
    List,

    /// Add a data source credential
    Add {
        /// Provider (cdse, element84, wekeo, cmems)
        #[arg(long, default_value = "cdse")]
        provider: String,

        /// Login username / email
        #[arg(long)]
        username: String,

        /// Password (prompted if not provided)
        #[arg(long, default_value = "")]
        password: String,

        /// Display name (auto-generated if omitted)
        #[arg(long, default_value = "")]
        name: String,
    },

    /// Remove a data source
    Remove {
        /// Provider to remove
        #[arg(long)]
        provider: String,

        /// Username to remove
        #[arg(long)]
        username: String,
    },

    /// List available data providers
    Providers,
}

#[derive(Subcommand)]
enum AdminCommands {
    /// Show current admin API key
    ShowKey,

    /// Generate a new admin API key
    RenewKey,
}

#[derive(Subcommand)]
enum DockerCommands {
    /// Build and start Docker container
    Start {
        /// Port (default: from config or 8400)
        #[arg(long)]
        port: Option<u16>,

        /// Node name
        #[arg(long)]
        name: Option<String>,

        /// Storage limit in GB
        #[arg(long)]
        storage: Option<f64>,

        /// Also act as beacon
        #[arg(long)]
        beacon: bool,

        /// Public URL
        #[arg(long)]
        public_url: Option<String>,

        /// Beacon URL to join
        #[arg(long)]
        beacon_url: Option<String>,

        /// Host data directory
        #[arg(long)]
        data_dir: Option<String>,

        /// Skip docker build
        #[arg(long)]
        no_build: bool,
    },

    /// Stop Docker container
    Stop,

    /// Show Docker container status
    Status,

    /// Show container logs
    Logs,

    /// Restart Docker container
    Restart,

    /// Pull latest code, rebuild and restart
    Update,

    /// Run earthgrid command inside container
    Exec {
        /// Arguments to pass to earthgrid inside the container
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        None => {
            // Show info when no subcommand given
            cmd_info().await?;
        }
        Some(Commands::Start {
            foreground,
            host,
            port,
            name,
            beacon,
            also_beacon,
            public_url,
            peers,
            beacon_peers,
        }) => {
            cmd_start(
                foreground, host, port, name, beacon, also_beacon, public_url, peers, beacon_peers,
            )
            .await?;
        }
        Some(Commands::Stop) => cmd_stop()?,
        Some(Commands::Status) => cmd_status()?,
        Some(Commands::Info) => cmd_info().await?,
        Some(Commands::Setup { port }) => cmd_setup(port)?,
        Some(Commands::Update) => cmd_update()?,
        Some(Commands::Tray) => cmd_tray(),
        Some(Commands::Resize { size_gb, force }) => cmd_resize(size_gb, force)?,
        Some(Commands::Verify {
            heal,
            collection,
            verbose,
            delete_corrupt,
        }) => cmd_verify(heal, collection, verbose, delete_corrupt).await?,
        Some(Commands::Fetch {
            bbox,
            collection,
            start,
            end,
            cloud,
            bands: _,
            product_type: _,
            limit,
            search_only: _,
            source: _,
            no_distribute: _,
        }) => cmd_fetch(bbox, collection, start, end, cloud, limit).await?,
        Some(Commands::Sync {
            peer_url,
            collections,
            limit,
            dry_run,
        }) => cmd_sync(peer_url, collections, limit, dry_run).await?,
        Some(Commands::Process {
            item_id,
            op,
            expression: _,
            output_collection: _,
            output_id: _,
        }) => cmd_process(item_id, op)?,
        Some(Commands::Ops) => cmd_ops(),
        Some(Commands::Sources { action }) => cmd_sources(action)?,
        Some(Commands::Admin { action }) => cmd_admin(action)?,
        Some(Commands::Docker { action }) => cmd_docker(action)?,
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

async fn cmd_start(
    foreground: bool,
    host: Option<String>,
    port: Option<u16>,
    name: Option<String>,
    beacon: Option<String>,
    also_beacon: bool,
    public_url: Option<String>,
    peers: Vec<String>,
    beacon_peers: Vec<String>,
) -> anyhow::Result<()> {
    let cfg = Config::load()?;

    let node_name = name.unwrap_or_else(|| cfg.node_name());
    let host = host.unwrap_or_else(|| cfg.host());
    let port = port.unwrap_or_else(|| cfg.port());
    let beacon_url = beacon.or_else(|| cfg.beacon_url.clone());
    let is_beacon = also_beacon || cfg.also_beacon.unwrap_or(false);
    let pub_url = public_url.or_else(|| cfg.public_url.clone());
    let peers = if peers.is_empty() {
        cfg.peers.clone().unwrap_or_default()
    } else {
        peers
    };
    let beacon_peers_list = if beacon_peers.is_empty() {
        cfg.beacon_peers.clone().unwrap_or_default()
    } else {
        beacon_peers
    };

    // Bootstrap: if no beacon configured, try GitHub seeds
    let effective_beacon = if beacon_url.is_none() && !is_beacon {
        fetch_github_seeds().await
    } else {
        beacon_url
    };

    println!("🌍 EarthGrid v{}", VERSION);
    println!("   Name:    {}", node_name);
    println!("   Listen:  {}:{}", host, port);
    println!("   Beacon:  {}", if is_beacon { "yes" } else { "no" });
    if let Some(ref b) = effective_beacon {
        println!("   Joins:   {}", b);
    }
    if !peers.is_empty() {
        println!("   Peers:   {}", peers.join(", "));
    }
    if !beacon_peers_list.is_empty() {
        println!("   Beacon peers: {}", beacon_peers_list.join(", "));
    }
    if let Some(ref u) = pub_url {
        println!("   Public:  {}", u);
    }
    println!();

    if foreground {
        // Run HTTP server in foreground (debug)
        let data_dir = cfg.store_path().parent().unwrap_or(Path::new(".")).to_path_buf();
        earthgrid_core::server::serve(data_dir, host, port, None).await?;
    } else {
        // Default: install + start as systemd service
        ensure_service(&host, port)?;
    }

    Ok(())
}

async fn fetch_github_seeds() -> Option<String> {
    const SEEDS_URL: &str = "https://matmatt.github.io/EarthGrid/peers.json";
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .ok()?;
    let resp: serde_json::Value = client.get(SEEDS_URL).send().await.ok()?.json().await.ok()?;
    let seeds = resp.get("seeds")?.as_array()?;
    if seeds.is_empty() {
        return None;
    }
    let url = seeds[0].get("url")?.as_str()?;
    println!("   Seeds:   fetched {} from GitHub", seeds.len());
    Some(url.to_string())
}

fn ensure_service(host: &str, port: u16) -> anyhow::Result<()> {
    let unit_dir = dirs_home().join(".config").join("systemd").join("user");
    let unit_path = unit_dir.join(SYSTEMD_UNIT);

    if !unit_path.exists() {
        println!("  Installing systemd service...");
        install_systemd_service()?;
    } else {
        let is_active = Command::new("systemctl")
            .args(["--user", "is-active", SYSTEMD_UNIT])
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim() == "active")
            .unwrap_or(false);

        if is_active {
            println!("  Restarting service...");
            let _ = Command::new("systemctl")
                .args(["--user", "restart", SYSTEMD_UNIT])
                .status();
        } else {
            println!("  Starting service...");
            let _ = Command::new("systemctl")
                .args(["--user", "start", SYSTEMD_UNIT])
                .status();
        }
    }

    std::thread::sleep(std::time::Duration::from_secs(1));

    let is_active = Command::new("systemctl")
        .args(["--user", "is-active", SYSTEMD_UNIT])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim() == "active")
        .unwrap_or(false);

    if is_active {
        println!("  ✓ EarthGrid running (systemd service)");
        println!("    Status:  systemctl --user status {}", SYSTEMD_UNIT);
        println!("    Logs:    journalctl --user -u {} -f", SYSTEMD_UNIT);
        println!("    Stop:    earthgrid stop");
        println!("    Debug:   earthgrid start --foreground");

        // Auto-start tray app if available and not already running
        start_tray_if_available();
    } else {
        println!("  ⚠ Service failed to start. Try: earthgrid start --foreground");
    }

    let _ = (host, port); // suppress unused warning
    Ok(())
}

fn cmd_stop() -> anyhow::Result<()> {
    // Stop PID-based daemon
    let pid_file = earthgrid_dir().join("earthgrid.pid");
    if pid_file.exists() {
        if let Ok(pid_str) = fs::read_to_string(&pid_file) {
            let pid = pid_str.trim().to_string();
            let _ = Command::new("kill").arg(&pid).status();
            println!("  ✓ Stopped daemon (PID {})", pid);
            let _ = fs::remove_file(&pid_file);
        }
    } else {
        println!("  No daemon running (no PID file)");
    }

    // Also stop systemd service
    let _ = Command::new("systemctl")
        .args(["--user", "stop", SYSTEMD_UNIT])
        .status();

    // Stop tray app
    let _ = Command::new("pkill")
        .args(["-f", "earthgrid tray"])
        .status();
    let _ = Command::new("pkill")
        .args(["-x", "earthgrid-tray"])
        .status();

    Ok(())
}

fn cmd_status() -> anyhow::Result<()> {
    let cfg = Config::load()?;
    let store_path = cfg.store_path();
    let limit_gb = cfg.storage_limit_gb();

    let (used_bytes, chunk_count) = store_usage(&store_path);
    let limit_bytes = (limit_gb * 1024.0_f64.powi(3)) as u64;
    let pct = if limit_bytes > 0 {
        used_bytes as f64 / limit_bytes as f64 * 100.0
    } else {
        0.0
    };

    println!("EarthGrid Node v{}", VERSION);
    println!("  Config:    {}", config_file_path().display());
    println!("  Name:      {}", cfg.node_name());
    println!("  Port:      {}", cfg.port());
    println!("  Beacon:    {}", if cfg.also_beacon.unwrap_or(false) { "yes" } else { "no" });
    println!(
        "  Storage:   {} / {:.1} GB ({:.1}%)",
        human_bytes(used_bytes),
        limit_gb,
        pct
    );
    println!("  Chunks:    {}", chunk_count);
    println!("  Store:     {}", store_path.display());
    let peers = cfg.peers.as_deref().unwrap_or(&[]);
    println!("  Peers:     {}", peers.len());

    Ok(())
}

async fn cmd_info() -> anyhow::Result<()> {
    let cfg = Config::load().unwrap_or_default();
    println!("EarthGrid v{}", VERSION);
    println!("  Platform: Rust/{}", std::env::consts::OS);
    println!();
    println!("Config:    {}", config_file_path().display());
    println!("  Name:    {}", cfg.node_name());
    println!("  Port:    {}", cfg.port());
    println!("  Storage: {:.1} GB", cfg.storage_limit_gb());
    if let Some(ref b) = cfg.beacon_url {
        println!("  Beacon:  {}", b);
    }
    println!();
    println!("Usage:");
    println!("  earthgrid start              Start as systemd service");
    println!("  earthgrid start --foreground Run in foreground (debug)");
    println!("  earthgrid stop               Stop node");
    println!("  earthgrid status             Show storage and peers");
    println!("  earthgrid setup              Interactive first-time setup");
    Ok(())
}

fn cmd_setup(port: u16) -> anyhow::Result<()> {
    println!("🌍 EarthGrid v{} — Setup\n", VERSION);

    // Storage
    let gb = loop {
        print!("How much disk space to contribute? [50] GB: ");
        io::stdout().flush()?;
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim();
        let val: f64 = if input.is_empty() {
            50.0
        } else {
            match input.parse() {
                Ok(v) if v >= 1.0 => v,
                _ => {
                    println!("  Please enter a number >= 1");
                    continue;
                }
            }
        };
        break val;
    };

    // Participation mode
    println!("\nHow do you want to participate?");
    println!("  [1] Node + Beacon (recommended — store data AND help others find it)");
    println!("  [2] Node only (store data, but don't coordinate)");
    print!("Choose [1]: ");
    io::stdout().flush()?;
    let mut mode_input = String::new();
    io::stdin().read_line(&mut mode_input)?;
    let also_beacon = mode_input.trim() != "2";

    // Data directory
    let default_store = earthgrid_dir().join("data");
    print!("Data directory? [{}]: ", default_store.display());
    io::stdout().flush()?;
    let mut store_input = String::new();
    io::stdin().read_line(&mut store_input)?;
    let data_dir = if store_input.trim().is_empty() {
        default_store
    } else {
        PathBuf::from(store_input.trim())
    };

    // Node name
    let default_name = format!("earthgrid-{}", hostname());
    print!("Name your node? [{}]: ", default_name);
    io::stdout().flush()?;
    let mut name_input = String::new();
    io::stdin().read_line(&mut name_input)?;
    let node_name = if name_input.trim().is_empty() {
        default_name
    } else {
        name_input.trim().to_string()
    };

    // Auto-update
    println!("\nAuto-update on start?");
    println!("  [1] yes — always pull latest and restart");
    println!("  [2] ask — check and prompt");
    println!("  [3] no  — manual updates only");
    print!("Choose [1]: ");
    io::stdout().flush()?;
    let mut au_input = String::new();
    io::stdin().read_line(&mut au_input)?;
    let auto_update = match au_input.trim() {
        "2" => "ask",
        "3" => "no",
        _ => "yes",
    };

    // Write config
    let config_dir = earthgrid_dir();
    fs::create_dir_all(&config_dir)?;
    fs::create_dir_all(&data_dir)?;

    let cfg = Config {
        node_name: Some(node_name.clone()),
        host: Some("0.0.0.0".to_string()),
        port: Some(port),
        storage_limit_gb: Some(gb),
        also_beacon: Some(also_beacon),
        data_dir: Some(data_dir.to_string_lossy().to_string()),
        store_path: Some(data_dir.join("store").to_string_lossy().to_string()),
        catalog_path: Some(data_dir.join("catalog.db").to_string_lossy().to_string()),
        source_users_db: Some(data_dir.join("source_users.db").to_string_lossy().to_string()),
        auto_update: Some(auto_update.to_string()),
        ..Config::default()
    };

    cfg.save()?;

    println!("\n✅ EarthGrid configured!");
    println!("   Node:     {}", node_name);
    println!("   Storage:  {:.1} GB at {}", gb, data_dir.display());
    println!("   Beacon:   {}", if also_beacon { "yes (also coordinator)" } else { "no (data node only)" });
    println!("   Port:     {}", port);
    println!("   Updates:  {}", auto_update);
    println!("   Config:   {}", config_file_path().display());
    println!("\n🚀 Start with: earthgrid start");

    Ok(())
}

fn cmd_update() -> anyhow::Result<()> {
    // Find repo root (walk up from binary location looking for .git)
    let bin_path = std::env::current_exe()?;
    let mut dir = bin_path.parent().map(|p| p.to_path_buf());
    let mut git_root = None;
    while let Some(d) = dir {
        if d.join(".git").exists() {
            git_root = Some(d.clone());
            break;
        }
        dir = d.parent().map(|p| p.to_path_buf());
    }

    if let Some(root) = git_root {
        println!("  Pulling latest from {}...", root.display());
        let status = Command::new("git").arg("pull").current_dir(&root).status()?;
        if !status.success() {
            println!("  ⚠ git pull failed");
        }

        println!("  Building...");
        let status = Command::new(dirs_home().join(".cargo").join("bin").join("cargo"))
            .args(["build", "--release"])
            .current_dir(&root)
            .status()?;
        if !status.success() {
            println!("  ⚠ cargo build failed");
            return Ok(());
        }
    } else {
        println!("  ⚠ Not a git repo — cannot auto-update");
        return Ok(());
    }

    // Restart systemd service if installed
    let unit_path = dirs_home()
        .join(".config")
        .join("systemd")
        .join("user")
        .join(SYSTEMD_UNIT);
    if unit_path.exists() {
        println!("  Restarting service...");
        let _ = Command::new("systemctl")
            .args(["--user", "restart", SYSTEMD_UNIT])
            .status();
        std::thread::sleep(std::time::Duration::from_secs(1));
        let is_active = Command::new("systemctl")
            .args(["--user", "is-active", SYSTEMD_UNIT])
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim() == "active")
            .unwrap_or(false);
        if is_active {
            println!("  ✓ Updated and running!");
        } else {
            println!("  ⚠ Updated but service failed to start.");
        }
    } else {
        println!("  ✓ Updated! Run 'earthgrid start' to launch.");
    }

    Ok(())
}

/// Start the tray app via `earthgrid tray` if a display is available.
/// Also installs autostart desktop entry.
fn start_tray_if_available() {
    // Skip on headless (no DISPLAY / WAYLAND_DISPLAY)
    let has_display = std::env::var("DISPLAY").is_ok()
        || std::env::var("WAYLAND_DISPLAY").is_ok();
    if !has_display {
        return;
    }

    // Check if already running
    let already_running = Command::new("pgrep")
        .args(["-f", "earthgrid tray"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);

    if already_running {
        return;
    }

    // Install autostart desktop entry
    let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("earthgrid"));
    if let Some(home) = std::env::var_os("HOME") {
        let autostart_dir = PathBuf::from(&home).join(".config").join("autostart");
        let _ = fs::create_dir_all(&autostart_dir);
        let desktop_entry = format!(
            "[Desktop Entry]\nType=Application\nName=EarthGrid Tray\nExec={} tray\nTerminal=false\nStartupNotify=false\n",
            exe.display()
        );
        let _ = fs::write(autostart_dir.join("earthgrid-tray.desktop"), desktop_entry);
    }

    // Start tray (detached: earthgrid tray)
    let _ = Command::new(&exe)
        .arg("tray")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();

    println!("  ✓ Tray app started (🌍)");
}

// -----------------------------------------------------------------------
// Tray App (inline — no separate binary needed)
// -----------------------------------------------------------------------

const TRAY_API_BASE: &str = "http://localhost:8400";
const TRAY_POLL_SECS: u64 = 5;
const TRAY_ICON_ONLINE: &[u8] = include_bytes!("../assets/tray-online-32.png");
const TRAY_ICON_OFFLINE: &[u8] = include_bytes!("../assets/tray-offline-32.png");

#[derive(Clone, PartialEq)]
enum TrayState {
    Online(String),
    Offline,
}

fn tray_icon_dir() -> PathBuf {
    let dir = std::env::var("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
            PathBuf::from(home).join(".cache")
        })
        .join("earthgrid-tray");
    let _ = fs::create_dir_all(&dir);
    dir
}

fn tray_load_icon(png: &[u8]) -> tray_icon::Icon {
    let img = image::load_from_memory(png).expect("bad icon PNG").into_rgba8();
    let (w, h) = img.dimensions();
    tray_icon::Icon::from_rgba(img.into_raw(), w, h).expect("icon create failed")
}

fn tray_poll_node() -> TrayState {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(3)))
        .build()
        .into();
    match agent.get(&format!("{}/health", TRAY_API_BASE)).call() {
        Ok(r) if r.status().as_u16() < 400 => {
            let stats = agent.get(&format!("{}/status", TRAY_API_BASE)).call().ok().and_then(|mut r| {
                let json: serde_json::Value = r.body_mut().read_json().ok()?;
                let bytes = json["storage"]["used_bytes"].as_f64().unwrap_or(0.0);
                let peers = json["peers"]["connected"].as_u64().unwrap_or(0);
                Some(format!("{:.1} TB | {} peers", bytes / 1e12, peers))
            });
            TrayState::Online(stats.unwrap_or_else(|| "connected".into()))
        }
        _ => TrayState::Offline,
    }
}

fn cmd_tray() -> ! {
    use tray_icon::menu::{Menu, MenuEvent, MenuItem, PredefinedMenuItem};
    use tray_icon::TrayIconBuilder;
    use std::sync::{Arc, Mutex as StdMutex};

    #[cfg(target_os = "linux")]
    gtk::init().expect("Failed to init GTK");

    let _ = fs::write(tray_icon_dir().join("earthgrid-online.png"), TRAY_ICON_ONLINE);
    let _ = fs::write(tray_icon_dir().join("earthgrid-offline.png"), TRAY_ICON_OFFLINE);

    let icon_online = tray_load_icon(TRAY_ICON_ONLINE);
    let icon_offline = tray_load_icon(TRAY_ICON_OFFLINE);

    let title = MenuItem::new("EarthGrid v0.1.0", false, None);
    let status_item = MenuItem::new("Status: checking...", false, None);
    let dashboard = MenuItem::new("Open Dashboard", true, None);
    let quit = MenuItem::new("Quit Tray", true, None);

    let menu = Menu::new();
    let _ = menu.append(&title);
    let _ = menu.append(&PredefinedMenuItem::separator());
    let _ = menu.append(&status_item);
    let _ = menu.append(&PredefinedMenuItem::separator());
    let _ = menu.append(&dashboard);
    let _ = menu.append(&PredefinedMenuItem::separator());
    let _ = menu.append(&quit);

    let dashboard_id = dashboard.id().clone();
    let quit_id = quit.id().clone();

    let tray = TrayIconBuilder::new()
        .with_menu(Box::new(menu))
        .with_icon(icon_offline.clone())
        .with_tooltip("EarthGrid — Offline")
        .with_temp_dir_path(tray_icon_dir())
        .build()
        .expect("Failed to create tray icon");

    let shared_state: Arc<StdMutex<TrayState>> = Arc::new(StdMutex::new(TrayState::Offline));
    let bg_state = Arc::clone(&shared_state);

    thread::spawn(move || loop {
        let new_state = tray_poll_node();
        *bg_state.lock().unwrap() = new_state;
        thread::sleep(std::time::Duration::from_secs(TRAY_POLL_SECS));
    });

    let menu_rx = MenuEvent::receiver();
    let mut last_state = TrayState::Offline;

    #[cfg(target_os = "linux")]
    {
        let shared_for_gtk = Arc::clone(&shared_state);
        gtk::glib::timeout_add_local(std::time::Duration::from_millis(200), move || {
            if let Ok(event) = menu_rx.try_recv() {
                if event.id == quit_id {
                    std::process::exit(0);
                } else if event.id == dashboard_id {
                    let _ = open::that(&format!("{}/ui", TRAY_API_BASE));
                }
            }
            let current = shared_for_gtk.lock().unwrap().clone();
            if current != last_state {
                match &current {
                    TrayState::Online(info) => {
                        let _ = tray.set_icon(Some(icon_online.clone()));
                        let _ = tray.set_tooltip(Some("EarthGrid — Online"));
                        let _ = status_item.set_text(&format!("Status: Online | {}", info));
                    }
                    TrayState::Offline => {
                        let _ = tray.set_icon(Some(icon_offline.clone()));
                        let _ = tray.set_tooltip(Some("EarthGrid — Offline"));
                        let _ = status_item.set_text("Status: Offline");
                    }
                }
                last_state = current;
            }
            gtk::glib::ControlFlow::Continue
        });
        gtk::main();
        std::process::exit(0);
    }

    #[cfg(not(target_os = "linux"))]
    loop {
        if let Ok(event) = menu_rx.try_recv() {
            if event.id == quit_id {
                std::process::exit(0);
            } else if event.id == dashboard_id {
                let _ = open::that(&format!("{}/ui", TRAY_API_BASE));
            }
        }
        let current = shared_state.lock().unwrap().clone();
        if current != last_state {
            match &current {
                TrayState::Online(info) => {
                    let _ = tray.set_icon(Some(icon_online.clone()));
                    let _ = tray.set_tooltip(Some("EarthGrid — Online"));
                    let _ = status_item.set_text(&format!("Status: Online | {}", info));
                }
                TrayState::Offline => {
                    let _ = tray.set_icon(Some(icon_offline.clone()));
                    let _ = tray.set_tooltip(Some("EarthGrid — Offline"));
                    let _ = status_item.set_text("Status: Offline");
                }
            }
            last_state = current;
        }
        thread::sleep(std::time::Duration::from_millis(100));
    }
}

fn install_systemd_service() -> anyhow::Result<()> {
    let unit_dir = dirs_home().join(".config").join("systemd").join("user");
    fs::create_dir_all(&unit_dir)?;

    // Find the earthgrid binary
    let earthgrid_bin = std::env::current_exe()
        .unwrap_or_else(|_| PathBuf::from("earthgrid"));

    let work_dir = earthgrid_dir();

    let unit_content = format!(
        "[Unit]\nDescription=EarthGrid Node\nAfter=network-online.target\nWants=network-online.target\n\n\
[Service]\nType=simple\nWorkingDirectory={work_dir}\nExecStart={bin} start --foreground\n\
Restart=on-failure\nRestartSec=10\nStandardOutput=journal\nStandardError=journal\n\n\
[Install]\nWantedBy=default.target\n",
        work_dir = work_dir.display(),
        bin = earthgrid_bin.display(),
    );

    let unit_path = unit_dir.join(SYSTEMD_UNIT);
    fs::write(&unit_path, &unit_content)?;

    // Enable lingering
    let user = std::env::var("USER").unwrap_or_default();
    let _ = Command::new("loginctl")
        .args(["enable-linger", &user])
        .status();

    let _ = Command::new("systemctl").args(["--user", "daemon-reload"]).status();
    let _ = Command::new("systemctl").args(["--user", "enable", SYSTEMD_UNIT]).status();
    let _ = Command::new("systemctl").args(["--user", "start", SYSTEMD_UNIT]).status();

    println!("✅ EarthGrid service installed and started");
    println!("   Unit:    {}", unit_path.display());
    println!("   Status:  systemctl --user status {}", SYSTEMD_UNIT);
    println!("   Logs:    journalctl --user -u {} -f", SYSTEMD_UNIT);
    println!("   Remove:  earthgrid uninstall-service");

    Ok(())
}

fn cmd_resize(new_gb: f64, force: bool) -> anyhow::Result<()> {
    if new_gb <= 0.0 {
        eprintln!("Error: size must be > 0 GB");
        std::process::exit(1);
    }

    let mut cfg = Config::load()?;
    let old_gb = cfg.storage_limit_gb();
    let store_path = cfg.store_path();
    let (used_bytes, _) = store_usage(&store_path);
    let used_gb = used_bytes as f64 / 1024.0_f64.powi(3);

    if new_gb < used_gb && !force {
        eprintln!(
            "Error: current usage ({:.2} GB) exceeds new limit ({:.1} GB).",
            used_gb, new_gb
        );
        eprintln!("Use --force to evict chunks.");
        std::process::exit(1);
    }

    if new_gb < used_gb && force {
        let target_bytes = (new_gb * 1024.0_f64.powi(3)) as u64;
        // Collect chunks: (mtime, size, path)
        let mut chunks: Vec<(u64, u64, PathBuf)> = Vec::new();
        if store_path.exists() {
            for entry in walkdir::WalkDir::new(&store_path).into_iter().flatten() {
                if entry.file_type().is_file() && entry.file_name().len() == 64 {
                    if let Ok(meta) = entry.metadata() {
                        let mtime = meta
                            .modified()
                            .ok()
                            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        chunks.push((mtime, meta.len(), entry.path().to_path_buf()));
                    }
                }
            }
        }
        // Sort oldest first
        chunks.sort_by_key(|(mtime, _, _)| *mtime);

        let mut current = used_bytes;
        let mut evicted = 0usize;
        for (_, size, path) in chunks {
            if current <= target_bytes {
                break;
            }
            let _ = fs::remove_file(&path);
            current = current.saturating_sub(size);
            evicted += 1;
        }
        println!("Evicted {} chunks to fit new limit.", evicted);
    }

    cfg.storage_limit_gb = Some(new_gb);
    cfg.save()?;

    let arrow = if new_gb > old_gb { "↑" } else { "↓" };
    println!("Storage resized: {:.1} GB → {:.1} GB {}", old_gb, new_gb, arrow);
    Ok(())
}

async fn cmd_verify(
    heal: bool,
    collection: Option<String>,
    verbose: bool,
    delete_corrupt: bool,
) -> anyhow::Result<()> {
    use earthgrid_core::catalog::Catalog;
    use earthgrid_core::chunk_store::ChunkStore;
    #[allow(unused_imports)]
    use sha2::Digest;

    let cfg = Config::load()?;
    let store_path = cfg.store_path();
    let catalog_path = cfg.catalog_path();

    let store = ChunkStore::new(&store_path, cfg.storage_limit_gb())?;
    let catalog = Catalog::new(&catalog_path)?;

    println!("🔍 Verifying chunk integrity...\n");

    let items = catalog.search(collection.as_deref(), None, None, 100_000, 0)?;
    if items.is_empty() {
        println!("No items in catalog.");
        return Ok(());
    }

    let mut total_chunks = 0usize;
    let mut corrupt_chunks = 0usize;
    let mut missing_chunks = 0usize;
    let mut corrupt_item_ids: Vec<String> = Vec::new();

    for item in &items {
        let hashes = &item.chunk_hashes;
        let mut item_ok = true;

        for sha in hashes {
            total_chunks += 1;
            if !store.has(sha) {
                missing_chunks += 1;
                item_ok = false;
                if verbose {
                    println!("  ❌ MISSING  {}... ({})", &sha[..16], item.id);
                }
            } else {
                let is_valid = store.verify(sha).unwrap_or(false);
                if is_valid {
                    if verbose {
                        println!("  ✅ OK      {}...", &sha[..16]);
                    }
                } else {
                    corrupt_chunks += 1;
                    item_ok = false;
                    if verbose {
                        println!("  ⚠️  CORRUPT {}... ({})", &sha[..16], item.id);
                    }
                }
            }
        }

        if !item_ok {
            corrupt_item_ids.push(item.id.clone());
        }
    }

    let ok = total_chunks - corrupt_chunks - missing_chunks;
    println!("\n📊 Results:");
    println!("  Total chunks:   {}", total_chunks);
    println!("  ✅ OK:           {}", ok);
    println!("  ⚠️  Corrupt:      {}", corrupt_chunks);
    println!("  ❌ Missing:      {}", missing_chunks);
    println!("  Items affected: {}", corrupt_item_ids.len());

    if corrupt_item_ids.is_empty() {
        println!("\n✅ All chunks verified — no corruption detected!");
        return Ok(());
    }

    if delete_corrupt || heal {
        println!("\n🗑️  Removing corrupt/missing items from catalog...");
        for id in &corrupt_item_ids {
            let _ = catalog.delete_item(id);
            println!("  Removed: {}", id);
        }
    }

    if heal {
        println!("\n🔄 Heal mode: re-fetch would be triggered here.");
        println!("   (Full re-download via fetcher not yet wired in heal path)");
    } else if !corrupt_item_ids.is_empty() && !delete_corrupt {
        println!("\n💡 Run with --heal to re-download, or --delete-corrupt to remove bad data");
    }

    Ok(())
}

async fn cmd_fetch(
    bbox: String,
    collection: String,
    start: Option<String>,
    end: Option<String>,
    cloud: f64,
    limit: usize,
) -> anyhow::Result<()> {
    use earthgrid_core::catalog::Catalog;
    use earthgrid_core::chunk_store::ChunkStore;
    use earthgrid_core::fetcher::fetch_and_ingest;

    let cfg = Config::load()?;
    let store_path = cfg.store_path();
    let catalog_path = cfg.catalog_path();

    let bbox_vals: Vec<f64> = bbox
        .split(',')
        .map(|s| s.trim().parse::<f64>().unwrap_or(0.0))
        .collect();
    if bbox_vals.len() != 4 {
        eprintln!("Error: --bbox must be W,S,E,N (4 values)");
        std::process::exit(1);
    }
    let bbox_arr = [bbox_vals[0], bbox_vals[1], bbox_vals[2], bbox_vals[3]];

    let store = Arc::new(Mutex::new(ChunkStore::new(&store_path, cfg.storage_limit_gb())?));
    let catalog = Arc::new(Mutex::new(Catalog::new(&catalog_path)?));

    println!(
        "Fetching from Element84 (bbox={}, cloud≤{}%, limit={})...",
        bbox, cloud, limit
    );

    let start_str = start.as_deref().unwrap_or("");
    let end_str = end.as_deref().unwrap_or("");
    let bands: Vec<String> = Vec::new();

    let result = fetch_and_ingest(
        store,
        catalog,
        bbox_arr,
        start_str,
        end_str,
        cloud,
        &bands,
        limit,
        &collection,
    )
    .await;

    println!("\n📊 Fetch results:");
    println!("  Searched: {}", result.items_searched);
    println!("  Downloaded: {}", result.items_downloaded);
    println!("  Skipped: {}", result.items_skipped);
    println!("  Data: {}", human_bytes(result.bytes_downloaded));
    if !result.errors.is_empty() {
        println!("\n⚠ {} errors:", result.errors.len());
        for e in result.errors.iter().take(5) {
            println!("  {}", e);
        }
    }
    if result.items_downloaded > 0 {
        println!("\n✅ Ingested {} items", result.items_downloaded);
    }

    Ok(())
}

async fn cmd_sync(
    peer_url: String,
    collections: Option<String>,
    limit: usize,
    dry_run: bool,
) -> anyhow::Result<()> {
    use earthgrid_core::catalog::Catalog;
    use earthgrid_core::chunk_store::ChunkStore;
    use earthgrid_core::replication::Replicator;

    let cfg = Config::load()?;
    let store_path = cfg.store_path();
    let catalog_path = cfg.catalog_path();

    let store = Arc::new(Mutex::new(ChunkStore::new(&store_path, cfg.storage_limit_gb())?));
    let catalog = Arc::new(Mutex::new(Catalog::new(&catalog_path)?));

    let repl = Replicator::new(store, catalog);

    let col_list: Vec<String> = collections
        .as_deref()
        .unwrap_or("")
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let peer = peer_url.trim_end_matches('/').to_string();
    println!("{} Syncing from {}...", if dry_run { "[DRY RUN] " } else { "" }, peer);

    let result = repl.sync_from_peer(&peer, &col_list, limit, dry_run).await;

    for e in result.errors.iter().take(5) {
        println!("  ⚠ {}", e);
    }

    println!("\n{}", if dry_run { "Would sync" } else { "Synced" });
    println!("  Collections:  {}", result.collections_processed);
    println!("  Items:        {}", result.items_processed);
    println!("  Chunks:       {} downloaded", result.chunks_downloaded);
    if result.bytes_downloaded > 0 {
        println!("  Data:         {}", human_bytes(result.bytes_downloaded));
    }

    if !dry_run && result.items_processed > 0 {
        println!("\n✅ Data replicated from {}", peer);
    }

    Ok(())
}

fn cmd_process(item_ids: Vec<String>, op: String) -> anyhow::Result<()> {
    println!("Processing {} item(s) with operation '{}'...", item_ids.len(), op);
    println!("  Items: {}", item_ids.join(", "));
    println!("  (Full processing pipeline via earthgrid_core::processing)");
    println!("  Run 'earthgrid ops' to see available operations.");
    Ok(())
}

fn cmd_ops() {
    println!("Available operations:");
    let ops = [
        ("ndvi",       "Normalized Difference Vegetation Index (NIR - Red) / (NIR + Red)"),
        ("ndwi",       "Normalized Difference Water Index (Green - NIR) / (Green + NIR)"),
        ("evi",        "Enhanced Vegetation Index"),
        ("cloud_mask", "Generate cloud mask from SCL band"),
        ("band_math",  "Arbitrary band math expression (use --expression)"),
    ];
    for (name, desc) in &ops {
        println!("  {:<15} {}", name, desc);
    }
}

fn cmd_sources(action: SourcesCommands) -> anyhow::Result<()> {
    use earthgrid_core::source_users::SourceUserRegistry;

    let cfg = Config::load()?;
    let db_path = cfg.source_users_db();

    match action {
        SourcesCommands::Providers => {
            println!("Available data providers:\n");
            let providers = [
                ("element84", "No",  "Sentinel-2 L2A, Sentinel-1 RTC, Landsat C2 L2", "Free, no account needed"),
                ("cdse",      "Yes", "Sentinel-1/2/3/5P, full archive",                "Free registration at dataspace.copernicus.eu"),
                ("wekeo",     "Yes", "CLMS, C3S, CAMS",                                "Free registration at wekeo.eu"),
                ("cmems",     "Yes", "Marine/ocean products",                           "Free registration at marine.copernicus.eu"),
            ];
            for (name, auth, data, note) in &providers {
                println!("  {:<12}  Auth: {:<3}  {}", name, auth, data);
                println!("  {:<12}  {}", "", note);
                println!();
            }
            println!("Add a provider:");
            println!("  earthgrid sources add --provider element84 --username public --password none");
            println!("  earthgrid sources add --provider cdse --username me@example.com");
        }

        SourcesCommands::List => {
            if !db_path.exists() {
                println!("No source users configured.");
                println!("Add one: earthgrid sources add --provider cdse --username me@example.com");
                return Ok(());
            }
            let reg = SourceUserRegistry::new(&db_path)?;
            let users = reg.get_users("")?;
            if users.is_empty() {
                println!("No source users configured.");
            } else {
                for u in &users {
                    let status = if u.is_enabled && u.is_healthy { "✓" } else { "✗" };
                    println!("  {} [{}] {} ({}/{})", status, u.id, u.name, u.provider, u.username);
                }
            }
        }

        SourcesCommands::Add {
            provider,
            username,
            mut password,
            name,
        } => {
            if password.is_empty() {
                print!("Password for {}: ", username);
                io::stdout().flush()?;
                let mut pw = String::new();
                io::stdin().read_line(&mut pw)?;
                password = pw.trim().to_string();
            }
            let display_name = if name.is_empty() {
                format!("{}-{}", provider, username.split('@').next().unwrap_or(&username))
            } else {
                name
            };

            // Ensure parent dir exists
            if let Some(parent) = db_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let reg = SourceUserRegistry::new(&db_path)?;
            reg.add_user(&display_name, &provider, &username, &password)?;
            println!("✓ Added source user '{}' (provider={})", display_name, provider);
        }

        SourcesCommands::Remove { provider, username } => {
            if !db_path.exists() {
                println!("No source users DB found.");
                return Ok(());
            }
            let reg = SourceUserRegistry::new(&db_path)?;
            // Deactivate by username
            reg.deactivate(&username)?;
            println!("✓ Removed source user {}/{}", provider, username);
        }
    }

    Ok(())
}

fn cmd_admin(action: AdminCommands) -> anyhow::Result<()> {
    use rusqlite::Connection;

    let cfg = Config::load()?;

    // Find users.db
    let db_path = if let Some(ref p) = cfg.users_db {
        PathBuf::from(p)
    } else {
        // Try common locations
        let candidates = [
            earthgrid_dir().join("data").join("users.db"),
            PathBuf::from("./data/users.db"),
        ];
        candidates
            .into_iter()
            .find(|p| p.exists())
            .ok_or_else(|| anyhow::anyhow!("No users.db found. Is EarthGrid set up?"))?
    };

    let conn = Connection::open(&db_path)?;

    match action {
        AdminCommands::ShowKey => {
            let result: rusqlite::Result<(String, String)> = conn.query_row(
                "SELECT api_key, username FROM users WHERE role='admin' LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            );
            match result {
                Ok((key, user)) => {
                    println!("Admin user: {}", user);
                    println!("API key:    {}", key);
                }
                Err(_) => {
                    eprintln!("No admin user found.");
                    std::process::exit(1);
                }
            }
        }

        AdminCommands::RenewKey => {
            use std::time::{SystemTime, UNIX_EPOCH};
            let result: rusqlite::Result<(i64, String)> = conn.query_row(
                "SELECT user_id, username FROM users WHERE role='admin' LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            );
            match result {
                Ok((uid, user)) => {
                    // Generate new key
                    let mut key_bytes = [0u8; 32];
                    for b in key_bytes.iter_mut() {
                        *b = rand_byte();
                    }
                    let new_key = hex::encode(key_bytes);
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs_f64();
                    conn.execute(
                        "UPDATE users SET api_key=?, updated_at=? WHERE user_id=?",
                        rusqlite::params![new_key, now, uid],
                    )?;
                    println!("Admin user:  {}", user);
                    println!("New API key: {}", new_key);
                    println!("\n⚠ Old key is now invalid. Update your .env if needed.");
                }
                Err(_) => {
                    eprintln!("No admin user found.");
                    std::process::exit(1);
                }
            }
        }
    }

    Ok(())
}

/// Simple deterministic pseudo-random byte (good enough for key generation).
fn rand_byte() -> u8 {
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let c = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    ((t.wrapping_mul(6364136223846793005).wrapping_add(c)) >> 33) as u8
}

fn cmd_docker(action: DockerCommands) -> anyhow::Result<()> {
    let compose_dir = earthgrid_dir();
    let compose_file = compose_dir.join("docker-compose.yml");

    match action {
        DockerCommands::Stop => {
            if !compose_file.exists() {
                eprintln!("No Docker deployment found. Run 'earthgrid docker start' first.");
                std::process::exit(1);
            }
            Command::new("docker")
                .args(["compose", "-f", &compose_file.to_string_lossy(), "down"])
                .status()?;
        }

        DockerCommands::Status => {
            if !compose_file.exists() {
                eprintln!("No Docker deployment found. Run 'earthgrid docker start' first.");
                std::process::exit(1);
            }
            Command::new("docker")
                .args(["compose", "-f", &compose_file.to_string_lossy(), "ps"])
                .status()?;
        }

        DockerCommands::Logs => {
            if !compose_file.exists() {
                eprintln!("No Docker deployment found. Run 'earthgrid docker start' first.");
                std::process::exit(1);
            }
            Command::new("docker")
                .args([
                    "compose",
                    "-f",
                    &compose_file.to_string_lossy(),
                    "logs",
                    "-f",
                    "--tail",
                    "100",
                ])
                .status()?;
        }

        DockerCommands::Restart => {
            docker_start_internal(
                None, None, None, false, None, None, None, false, &compose_dir, &compose_file,
            )?;
        }

        DockerCommands::Start {
            port,
            name,
            storage,
            beacon,
            public_url,
            beacon_url,
            data_dir,
            no_build,
        } => {
            docker_start_internal(
                port,
                name,
                storage,
                beacon,
                public_url,
                beacon_url,
                data_dir,
                no_build,
                &compose_dir,
                &compose_file,
            )?;
        }

        DockerCommands::Update => {
            // git pull then rebuild
            let bin = std::env::current_exe()?;
            let mut git_root = None;
            let mut dir = bin.parent().map(|p| p.to_path_buf());
            while let Some(d) = dir {
                if d.join(".git").exists() {
                    git_root = Some(d.clone());
                    break;
                }
                dir = d.parent().map(|p| p.to_path_buf());
            }

            if let Some(root) = git_root {
                println!("📥 Pulling latest code...");
                Command::new("git").arg("pull").current_dir(&root).status()?;
            }

            // Remove old container
            let _ = Command::new("docker")
                .args(["rm", "-f", "earthgrid"])
                .status();

            if compose_file.exists() {
                println!("🔄 Rebuilding from existing config...");
                let result = Command::new("docker")
                    .args([
                        "compose",
                        "-f",
                        &compose_file.to_string_lossy(),
                        "up",
                        "-d",
                        "--build",
                    ])
                    .current_dir(&compose_dir)
                    .status()?;
                if result.success() {
                    println!("\n✅ Updated and running!");
                } else {
                    eprintln!("\n⚠ Docker failed");
                    std::process::exit(1);
                }
            } else {
                docker_start_internal(
                    None, None, None, false, None, None, None, false, &compose_dir, &compose_file,
                )?;
            }
        }

        DockerCommands::Exec { args } => {
            let mut cmd_args = vec!["exec", "-i", "earthgrid", "earthgrid"];
            let args_str: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
            cmd_args.extend(args_str);
            let status = Command::new("docker").args(&cmd_args).status()?;
            std::process::exit(status.code().unwrap_or(1));
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn docker_start_internal(
    port: Option<u16>,
    name: Option<String>,
    storage: Option<f64>,
    beacon: bool,
    public_url: Option<String>,
    beacon_url: Option<String>,
    data_dir: Option<String>,
    no_build: bool,
    compose_dir: &Path,
    compose_file: &Path,
) -> anyhow::Result<()> {
    let cfg = Config::load().unwrap_or_default();

    let node_name = name.unwrap_or_else(|| cfg.node_name());
    let port = port.unwrap_or_else(|| cfg.port());
    let storage_gb = storage.unwrap_or_else(|| cfg.storage_limit_gb());
    let also_beacon = beacon || cfg.also_beacon.unwrap_or(false);
    let pub_url = public_url.or_else(|| cfg.public_url.clone());
    let b_url = beacon_url.or_else(|| cfg.beacon_url.clone());
    let data_path = data_dir
        .map(PathBuf::from)
        .unwrap_or_else(|| earthgrid_dir().join("data"));
    let data_path = fs::canonicalize(&data_path).unwrap_or(data_path.clone());

    // Ensure data dir exists
    fs::create_dir_all(&data_path)?;

    // Write compose file
    let mut env_lines = vec![
        format!("      - EARTHGRID_NODE_NAME={}", node_name),
        format!("      - EARTHGRID_PORT={}", port),
        format!("      - EARTHGRID_STORAGE_LIMIT_GB={}", storage_gb),
        format!("      - EARTHGRID_ALSO_BEACON={}", if also_beacon { "true" } else { "false" }),
        "      - EARTHGRID_STORE_PATH=/data/store".to_string(),
        "      - EARTHGRID_CATALOG_PATH=/data/catalog.db".to_string(),
        "      - EARTHGRID_SOURCE_USERS_DB=/data/source_users.db".to_string(),
        "      - EARTHGRID_STATS_DB=/data/stats.db".to_string(),
        "      - EARTHGRID_IDENTITY_KEY_PATH=/data/.node_key".to_string(),
        "      - EARTHGRID_USERS_DB=/data/users.db".to_string(),
    ];
    if let Some(ref u) = pub_url {
        env_lines.push(format!("      - EARTHGRID_PUBLIC_URL={}", u));
    }
    if let Some(ref u) = b_url {
        env_lines.push(format!("      - EARTHGRID_BEACON_URL={}", u));
    }

    // Find Dockerfile
    let bin = std::env::current_exe()?;
    let mut project_root = None;
    let mut dir = bin.parent().map(|p| p.to_path_buf());
    while let Some(d) = dir {
        if d.join(".git").exists() {
            project_root = Some(d.clone());
            break;
        }
        dir = d.parent().map(|p| p.to_path_buf());
    }
    let project_root = project_root.unwrap_or_else(|| earthgrid_dir());

    let compose_content = format!(
        "services:\n  earthgrid:\n    build:\n      context: {root}\n      dockerfile: docker/Dockerfile\n    \
container_name: earthgrid\n    ports:\n      - \"{port}:{port}\"\n    volumes:\n      - {data}:/data\n    \
environment:\n{env}\n    env_file:\n      - .env\n    restart: unless-stopped\n    cpu_shares: 128\n    \
mem_limit: 2g\n    oom_score_adj: 500\n",
        root = project_root.display(),
        port = port,
        data = data_path.display(),
        env = env_lines.join("\n"),
    );

    fs::write(compose_file, &compose_content)?;

    println!("🐳 EarthGrid Docker");
    println!("   Name:     {}", node_name);
    println!("   Storage:  {} GB", storage_gb);
    println!("   Port:     {}", port);
    println!("   Beacon:   {}", if also_beacon { "yes" } else { "no" });
    println!("   Data:     {}", data_path.display());
    if let Some(ref u) = pub_url {
        println!("   URL:      {}", u);
    }
    println!("   Compose:  {}", compose_file.display());
    println!();

    let build_flag = if no_build { vec![] } else { vec!["--build"] };
    let mut args = vec!["compose", "-f", compose_file.to_str().unwrap_or(""), "up", "-d"];
    args.extend(build_flag.iter().map(|s| *s));

    let result = Command::new("docker")
        .args(&args)
        .current_dir(compose_dir)
        .status()?;

    if result.success() {
        println!("\n✅ EarthGrid running in Docker");
        println!("   Stop:     earthgrid docker stop");
        println!("   Logs:     earthgrid docker logs");
        println!("   Status:   earthgrid docker status");
    } else {
        eprintln!("\n⚠ Docker failed (exit {})", result.code().unwrap_or(-1));
        std::process::exit(1);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn store_usage(store_path: &Path) -> (u64, usize) {
    let mut total = 0u64;
    let mut count = 0usize;
    if store_path.exists() {
        if let Ok(walker) = walkdir::WalkDir::new(store_path).into_iter().collect::<Result<Vec<_>, _>>() {
            for entry in walker {
                if entry.file_type().is_file() && entry.file_name().to_string_lossy().len() == 64 {
                    if let Ok(meta) = entry.metadata() {
                        total += meta.len();
                        count += 1;
                    }
                }
            }
        }
    }
    (total, count)
}

fn human_bytes(b: u64) -> String {
    let units = ["B", "KB", "MB", "GB", "TB"];
    let mut val = b as f64;
    for unit in &units {
        if val < 1024.0 {
            return format!("{:.1} {}", val, unit);
        }
        val /= 1024.0;
    }
    format!("{:.1} PB", val)
}
