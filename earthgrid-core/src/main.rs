//! EarthGrid CLI + HTTP Server.

use std::fs;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::process;

use clap::{Parser, Subcommand};
use earthgrid_core::chunk_store::ChunkStore;
use earthgrid_core::catalog::Catalog;
use earthgrid_core::auth::AuthConfig;
use earthgrid_core::mgrs;

/// Default earthgrid home directory
fn earthgrid_home() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join(".earthgrid")
}

fn pid_file() -> PathBuf {
    earthgrid_home().join("earthgrid.pid")
}

fn config_file() -> PathBuf {
    earthgrid_home().join("config.json")
}

fn default_data_dir() -> PathBuf {
    // Check config.json for store_path → derive data_dir from parent
    if let Ok(contents) = fs::read_to_string(config_file()) {
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&contents) {
            if let Some(sp) = val["store_path"].as_str() {
                if let Some(parent) = std::path::Path::new(sp).parent() {
                    return parent.to_path_buf();
                }
            }
        }
    }
    earthgrid_home().join("data")
}

/// Load port from config file (fallback to 8400)
fn config_port() -> u16 {
    if let Ok(contents) = fs::read_to_string(config_file()) {
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&contents) {
            if let Some(p) = val["port"].as_u64() {
                return p as u16;
            }
        }
    }
    8400
}

/// Load host from config file (fallback to localhost)
fn config_host() -> String {
    if let Ok(contents) = fs::read_to_string(config_file()) {
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&contents) {
            if let Some(h) = val["host"].as_str() {
                return h.to_string();
            }
        }
    }
    "localhost".to_string()
}

#[derive(Parser)]
#[command(name = "earthgrid", version, about = "Distributed EO data storage")]
struct Cli {
    /// Path to data directory
    #[arg(long, default_value_os_t = default_data_dir())]
    data_dir: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Verify integrity of an item's chunks
    Verify {
        /// Item ID to verify
        item_id: String,
    },

    /// List items in the catalog
    List {
        /// Filter by collection
        #[arg(long)]
        collection: Option<String>,

        /// Max results
        #[arg(long, default_value = "50")]
        limit: usize,
    },

    /// Start the HTTP server (foreground)
    Serve {
        /// Host to bind
        #[arg(long, default_value = "0.0.0.0")]
        host: String,

        /// Port to listen on
        #[arg(long, default_value = "8400")]
        port: u16,

        /// libp2p listen port (0 = random)
        #[arg(long, default_value = "9400")]
        p2p_port: u16,

        /// Bootstrap peers (multiaddr, comma-separated)
        #[arg(long, env = "EARTHGRID_BOOTSTRAP_PEERS")]
        bootstrap_peers: Option<String>,

        /// Disable libp2p networking
        #[arg(long)]
        no_p2p: bool,
    },

    /// Start node as background daemon
    Start {
        /// Port to listen on
        #[arg(long, default_value = "8400")]
        port: u16,

        /// libp2p listen port
        #[arg(long, default_value = "9400")]
        p2p_port: u16,

        /// Bootstrap peers (multiaddr, comma-separated)
        #[arg(long, env = "EARTHGRID_BOOTSTRAP_PEERS")]
        bootstrap_peers: Option<String>,

        /// Disable libp2p networking
        #[arg(long)]
        no_p2p: bool,
    },

    /// Stop the running daemon
    Stop,

    /// Run storage eviction to free space (deletes replicated/cold items)
    Evict {
        /// Target storage size in GB (default: use configured storage_limit_gb)
        #[arg(long)]
        target: Option<f64>,

        /// Dry run — show what would be evicted without deleting
        #[arg(long)]
        dry_run: bool,
    },

    /// Show node info, storage stats, and daemon status
    Status,

    /// Pull latest code, rebuild, and restart
    Update {
        /// Force source build (git pull + cargo build) even if no repo is auto-detected
        #[arg(long, conflicts_with = "binary")]
        source: bool,

        /// Force binary update from GitHub Releases (skip source build even if repo exists)
        #[arg(long, conflicts_with = "source")]
        binary: bool,
    },

    /// Fetch satellite data via the running node
    Fetch {
        /// Bounding box: minLon,minLat,maxLon,maxLat
        #[arg(long)]
        bbox: Option<String>,

        /// Sentinel-2 MGRS tile name (e.g. 32TPS). Alternative to --bbox.
        #[arg(long)]
        tile: Option<String>,

        /// Start date (ISO 8601)
        #[arg(long)]
        start: Option<String>,

        /// End date (ISO 8601)
        #[arg(long)]
        end: Option<String>,

        /// Collection name
        #[arg(long)]
        collection: Option<String>,

        /// Bands (comma-separated)
        #[arg(long)]
        bands: Option<String>,

        /// Max results
        #[arg(long, default_value = "10")]
        limit: usize,

        /// Max cloud cover (0-100)
        #[arg(long)]
        cloud_cover: Option<f64>,
    },

    /// Interactive first-time setup
    Setup,

    /// Install systemd user service
    InstallService,

    /// Uninstall systemd user service
    UninstallService,

    /// Change storage allocation
    Resize {
        /// New storage size in GB
        #[arg(long)]
        size: f64,
    },

    /// Manage users and API keys
    Admin {
        #[command(subcommand)]
        action: AdminAction,
    },
}

#[derive(Subcommand)]
enum AdminAction {
    /// Add a new user
    AddUser {
        /// Username
        username: String,
        /// Role: admin, user, or readonly
        #[arg(long, default_value = "user")]
        role: String,
    },
    /// List all users
    ListUsers,
    /// Delete a user
    DeleteUser {
        /// Username to delete
        username: String,
    },
}

/// Load environment variables from ~/.earthgrid/.env (simple key=value format).
fn load_dotenv() {
    let env_path = earthgrid_home().join(".env");
    if let Ok(content) = std::fs::read_to_string(&env_path) {
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((key, val)) = line.split_once('=') {
                let key = key.trim();
                let val = val.trim();
                // Only set if not already set (env vars take precedence)
                if std::env::var(key).is_err() {
                    std::env::set_var(key, val);
                }
            }
        }
    }
}

fn main() -> anyhow::Result<()> {
    // Load .env from ~/.earthgrid/.env (if exists) before parsing CLI
    load_dotenv();
    let cli = Cli::parse();
    let store_path = cli.data_dir.join("store");
    let catalog_path = cli.data_dir.join("catalog.db");

    match cli.command {
        Commands::Verify { item_id } => {
            let store = ChunkStore::new(&store_path, 0.0)?;
            let catalog = Catalog::new(&catalog_path)?;
            let item = catalog
                .get_item(&item_id)?
                .ok_or_else(|| anyhow::anyhow!("Item not found: {}", item_id))?;

            let total = item.chunk_hashes.len();
            let mut valid = 0;
            let mut missing = 0;
            let mut corrupted = 0;

            for hash in &item.chunk_hashes {
                if !store.has(hash) {
                    missing += 1;
                    eprintln!("  ❌ MISSING: {}...", &hash[..16]);
                } else {
                    match store.verify(hash) {
                        Ok(true) => valid += 1,
                        _ => {
                            corrupted += 1;
                            eprintln!("  ⚠️ CORRUPTED: {}...", &hash[..16]);
                        }
                    }
                }
            }

            println!(
                "\n{} — {}/{} valid, {} missing, {} corrupted → {}",
                item_id, valid, total, missing, corrupted,
                if corrupted == 0 && missing == 0 { "✅ OK" } else { "❌ FAILED" }
            );
        }

        Commands::List { collection, limit } => {
            let catalog = Catalog::new(&catalog_path)?;
            let items = catalog.search(collection.as_deref(), None, None, limit, 0)?;
            if items.is_empty() {
                println!("No items found.");
            } else {
                for item in &items {
                    println!(
                        "  {} | {} | {} chunks | bbox [{:.1},{:.1},{:.1},{:.1}]",
                        item.id, item.collection, item.chunk_hashes.len(),
                        item.bbox[0], item.bbox[1], item.bbox[2], item.bbox[3]
                    );
                }
                println!("\n{} items", items.len());
            }
        }



        Commands::Serve { host, port, p2p_port, bootstrap_peers, no_p2p } => {
            run_serve(cli.data_dir, host, port, p2p_port, bootstrap_peers, no_p2p)?;
        }

        Commands::Start { port, p2p_port, bootstrap_peers, no_p2p } => {
            // Check if already running
            if let Some(pid) = read_pid() {
                if is_process_alive(pid) {
                    println!("⚠️  EarthGrid is already running (PID {})", pid);
                    return Ok(());
                }
            }

            // Check if port is already in use
            match std::net::TcpListener::bind(format!("0.0.0.0:{}", port)) {
                Ok(_listener) => { /* port free, listener drops immediately */ }
                Err(_) => {
                    eprintln!("❌ Port {} is already in use. Is EarthGrid already running?", port);
                    return Ok(());
                }
            }

            // Spawn self as background process with 'serve'
            let exe = std::env::current_exe()?;
            let data_dir_str = cli.data_dir.to_string_lossy().to_string();
            let mut args = vec![
                "--data-dir".to_string(), data_dir_str,
                "serve".to_string(),
                "--host".to_string(), "0.0.0.0".to_string(),
                "--port".to_string(), port.to_string(),
                "--p2p-port".to_string(), p2p_port.to_string(),
            ];
            if no_p2p { args.push("--no-p2p".to_string()); }
            if let Some(bp) = bootstrap_peers {
                args.push("--bootstrap-peers".to_string());
                args.push(bp);
            }

            let log_file = earthgrid_home().join("earthgrid.log");
            let log = fs::OpenOptions::new()
                .create(true).append(true).open(&log_file)?;
            let log_err = log.try_clone()?;

            let mut child = process::Command::new(&exe)
                .args(&args)
                .stdin(process::Stdio::null())
                .stdout(log)
                .stderr(log_err)
                .spawn()?;

            let pid = child.id();
            // Write PID
            fs::create_dir_all(earthgrid_home())?;
            fs::write(pid_file(), pid.to_string())?;

            // Wait briefly and verify child is still alive
            std::thread::sleep(std::time::Duration::from_millis(500));
            match child.try_wait() {
                Ok(Some(status)) => {
                    let _ = fs::remove_file(pid_file());
                    eprintln!("❌ EarthGrid exited immediately ({}). Check {}", status, log_file.display());
                    return Ok(());
                }
                _ => {} // Still running or can't check — OK
            }

            println!("🚀 EarthGrid started (PID {})", pid);
            println!("   Port: {}", port);
            println!("   Log:  {}", log_file.display());
            println!("   PID:  {}", pid_file().display());
        }

        Commands::Evict { target, dry_run } => {
            let config = earthgrid_core::config::Settings::load_or_default()?;
            let target_gb = target.unwrap_or(config.storage_limit_gb);
            let data_dir = cli.data_dir.clone();
            let store_path = data_dir.join("store");
            let catalog_path = data_dir.join("catalog.db");
            let beacon_db = earthgrid_core::config::Settings::config_dir().join("beacon.db");
            let beacon_path = if beacon_db.exists() { Some(beacon_db.as_path()) } else { None };

            let mut store = ChunkStore::new(&store_path, target_gb)?;
            let catalog = Catalog::new(&catalog_path)?;

            let current_gb = store.total_bytes() as f64 / 1_073_741_824.0;
            println!("📊 Current storage: {:.1} GB / {:.0} GB limit", current_gb, target_gb);

            if current_gb <= target_gb {
                println!("✅ Storage within limit, nothing to evict");
            } else {
                println!("🗑️  Need to free {:.1} GB", current_gb - target_gb);
                if dry_run {
                    println!("   (dry run — no files will be deleted)");
                    // TODO: show candidates without deleting
                    println!("   Dry run not yet implemented, run without --dry-run to evict");
                } else {
                    match earthgrid_core::eviction::evict(&catalog, &mut store, target_gb, beacon_path) {
                        Ok(result) => {
                            println!("✅ Eviction complete:");
                            println!("   Items deleted: {}", result.items_deleted);
                            println!("   Space freed:   {:.1} GB", result.bytes_freed as f64 / 1_073_741_824.0);
                            println!("   Items kept:    {} (last replica — not safe to delete)", result.items_kept);
                            let new_gb = store.total_bytes() as f64 / 1_073_741_824.0;
                            println!("   New storage:   {:.1} GB", new_gb);
                        }
                        Err(e) => eprintln!("❌ Eviction failed: {}", e),
                    }
                }
            }
        }

        Commands::Stop => {
            match read_pid() {
                None => println!("⚠️  No PID file found — is EarthGrid running?"),
                Some(pid) => {
                    if !is_process_alive(pid) {
                        println!("⚠️  Process {} is not running (stale PID file)", pid);
                        let _ = fs::remove_file(pid_file());
                    } else {
                        // Send SIGTERM
                        let status = process::Command::new("kill")
                            .arg("-TERM")
                            .arg(pid.to_string())
                            .status()?;
                        if status.success() {
                            println!("🛑 Sent SIGTERM to PID {}", pid);
                            let _ = fs::remove_file(pid_file());
                        } else {
                            eprintln!("❌ Failed to kill PID {}", pid);
                        }
                    }
                }
            }
        }

        Commands::Status => {
            // Always show local disk info
            let store = ChunkStore::new(&store_path, 0.0)?;
            let catalog = Catalog::new(&catalog_path)?;
            let auth = AuthConfig::from_env();
            let stats = store.stats();

            println!("🌍 EarthGrid v{}", env!("CARGO_PKG_VERSION"));
            println!("   Store:    {}", store_path.display());
            println!("   Catalog:  {}", catalog_path.display());
            println!("   Chunks:   {}", store.chunk_count());
            println!("   Storage:  {:.2} GB", store.total_bytes() as f64 / 1e9);
            println!("   Items:    {}", catalog.item_count(None)?);
            println!("   Auth:     {}", if auth.is_enabled() { "enabled" } else { "open" });
            println!("   Served:   {} chunks ({:.2} GB)",
                stats.chunks_served,
                stats.bytes_served as f64 / 1e9
            );

            // Daemon status
            let pid = read_pid();
            let running = pid.map(is_process_alive).unwrap_or(false);

            if let Some(p) = pid {
                if running {
                    println!("\n✅ Daemon running (PID {})", p);

                    // Fetch live info from HTTP API
                    let port = config_port();
                    let url = format!("http://localhost:{}/node-info", port);
                    match ureq::get(&url).call() {
                        Ok(resp) => {
                            let body: serde_json::Value = resp.into_body().read_json()?;
                            println!("   Peers:     {}", body["peers"].as_u64().unwrap_or(0));
                            println!("   API:       http://localhost:{}", port);
                        }
                        Err(e) => {
                            println!("   API:       unreachable ({})", e);
                        }
                    }
                } else {
                    println!("\n⚠️  Daemon not running (stale PID {})", p);
                }
            } else {
                println!("\n⚠️  Daemon not running");
            }
        }

        Commands::Update { source, binary } => {
            // Detect: dev mode (git repo + cargo) vs binary-only mode
            let repo_dir = find_repo_dir();
            let has_repo = !binary && (source || (repo_dir.join("Cargo.toml").exists()
                && has_git_dir(&repo_dir)));

            println!("📦 Updating EarthGrid...");

            if has_repo {
                // === DEV MODE: git pull + cargo build ===
                println!("   Mode: source (repo: {})", repo_dir.display());

                // Find git root (may be parent of repo_dir)
                let git_dir = {
                    let mut d = repo_dir.clone();
                    let result = loop {
                        if d.join(".git").exists() { break d; }
                        match d.parent() {
                            Some(p) => d = p.to_path_buf(),
                            None => break repo_dir.clone(),
                        }
                    };
                    result
                };

                println!("\n1️⃣  git pull...");
                let status = process::Command::new("git")
                    .args(["pull"])
                    .current_dir(&git_dir)
                    .status()?;
                if !status.success() {
                    anyhow::bail!("git pull failed");
                }

                println!("\n2️⃣  cargo build --release...");
                let cargo = find_cargo();
                let status = process::Command::new(&cargo)
                    .args(["build", "--release"])
                    .current_dir(&repo_dir)
                    .status()?;
                if !status.success() {
                    anyhow::bail!("cargo build failed");
                }

                println!("\n3️⃣  cargo install...");
                let status = process::Command::new(&cargo)
                    .args(["install", "--path", ".", "--force"])
                    .current_dir(&repo_dir)
                    .status()?;
                if !status.success() {
                    anyhow::bail!("cargo install failed");
                }
            } else {
                // === BINARY MODE: download from GitHub releases ===
                println!("   Mode: binary update (no source repo found)");
                let current_version = env!("CARGO_PKG_VERSION");
                println!("   Current: v{}", current_version);

                // Detect platform
                let asset_name = if cfg!(target_os = "macos") && cfg!(target_arch = "aarch64") {
                    "earthgrid-macos-arm64.tar.gz"
                } else if cfg!(target_os = "linux") && cfg!(target_arch = "x86_64") {
                    "earthgrid-linux-x86_64.tar.gz"
                } else {
                    anyhow::bail!("No pre-built binary for this platform. Use source build.");
                };

                println!("\n1️⃣  Checking latest release...");
                let client = reqwest::blocking::Client::builder()
                    .user_agent("earthgrid-updater")
                    .build()?;
                let release: serde_json::Value = client
                    .get("https://api.github.com/repos/MatMatt/EarthGrid/releases/latest")
                    .send()?
                    .json()?;
                let tag = release["tag_name"].as_str().unwrap_or("unknown");
                let remote_version = tag.trim_start_matches('v');
                println!("   Latest:  {}", tag);

                if remote_version == current_version {
                    println!("✅ Already up to date (v{})", current_version);
                } else {
                    // Find asset URL
                    let assets = release["assets"].as_array()
                        .ok_or_else(|| anyhow::anyhow!("No assets in release"))?;
                    let asset_url = assets.iter()
                        .find(|a| a["name"].as_str() == Some(asset_name))
                        .and_then(|a| a["browser_download_url"].as_str())
                        .ok_or_else(|| anyhow::anyhow!("Asset {} not found in release", asset_name))?;

                    println!("\n2️⃣  Downloading {}...", asset_name);
                    let bytes = client.get(asset_url).send()?.bytes()?;
                    let tmp_dir = std::env::temp_dir().join("earthgrid-update");
                    let _ = std::fs::remove_dir_all(&tmp_dir);
                    std::fs::create_dir_all(&tmp_dir)?;
                    let archive_path = tmp_dir.join(asset_name);
                    std::fs::write(&archive_path, &bytes)?;

                    println!("\n3️⃣  Extracting...");
                    let status = process::Command::new("tar")
                        .args(["xzf", &archive_path.to_string_lossy()])
                        .current_dir(&tmp_dir)
                        .status()?;
                    if !status.success() {
                        anyhow::bail!("Failed to extract archive");
                    }

                    // Find the binary in extracted files
                    let bin_name = if cfg!(target_os = "windows") { "earthgrid.exe" } else { "earthgrid" };
                    let new_binary = tmp_dir.join(bin_name);
                    if !new_binary.exists() {
                        // Maybe inside a subdirectory
                        let entries: Vec<_> = std::fs::read_dir(&tmp_dir)?
                            .filter_map(|e| e.ok())
                            .collect();
                        let found = entries.iter()
                            .find(|e| e.file_name() == bin_name)
                            .or_else(|| entries.iter().find(|e| {
                                e.path().is_dir() && e.path().join(bin_name).exists()
                            }));
                        if found.is_none() {
                            anyhow::bail!("Binary not found in archive");
                        }
                    }

                    // Replace current binary
                    let current_exe = std::env::current_exe()?;
                    println!("   Replacing: {}", current_exe.display());
                    let backup = current_exe.with_extension("old");
                    let _ = std::fs::rename(&current_exe, &backup);
                    if let Err(e) = std::fs::copy(&new_binary, &current_exe) {
                        // Restore backup on failure
                        let _ = std::fs::rename(&backup, &current_exe);
                        anyhow::bail!("Failed to replace binary: {}", e);
                    }
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        let _ = std::fs::set_permissions(&current_exe, std::fs::Permissions::from_mode(0o755));
                    }
                    let _ = std::fs::remove_file(&backup);
                    let _ = std::fs::remove_dir_all(&tmp_dir);
                    println!("   ✅ Updated v{} → {}", current_version, tag);
                }
            }

            // Restart: try systemd first, then stop/start
            println!("\n4️⃣  Restarting...");
            let sysd = process::Command::new("systemctl")
                .args(["--user", "is-active", "--quiet", "earthgrid"])
                .status();
            if sysd.map(|s| s.success()).unwrap_or(false) {
                process::Command::new("systemctl")
                    .args(["--user", "restart", "earthgrid"])
                    .status()?;
                println!("✅ Restarted via systemd");
            } else {
                // Stop old daemon if running, then auto-restart
                let was_running = {
                    let mut found = false;
                    // Try PID file first
                    if let Some(pid) = read_pid() {
                        if is_process_alive(pid) {
                            let _ = process::Command::new("kill").arg("-TERM").arg(pid.to_string()).status();
                            std::thread::sleep(std::time::Duration::from_secs(2));
                            found = true;
                        }
                    }
                    // Always try pgrep as fallback (PID file may be stale)
                    if !found {
                        let pgrep = process::Command::new("pgrep")
                            .args(["-f", "earthgrid.*serve"])
                            .output();
                        if let Ok(out) = pgrep {
                            if out.status.success() {
                                for line in String::from_utf8_lossy(&out.stdout).lines() {
                                    if let Ok(pid) = line.trim().parse::<u32>() {
                                        let _ = process::Command::new("kill").arg("-TERM").arg(pid.to_string()).status();
                                    }
                                }
                                std::thread::sleep(std::time::Duration::from_secs(2));
                                found = true;
                            }
                        }
                    }
                    found
                };

                if was_running {
                    // Auto-restart: find binary in ~/.cargo/bin or PATH
                    let exe = dirs::home_dir()
                        .map(|h| h.join(".cargo/bin/earthgrid"))
                        .filter(|p| p.exists())
                        .unwrap_or_else(|| std::path::PathBuf::from("earthgrid"));
                    // Small delay to let cargo finish replacing the binary
                    std::thread::sleep(std::time::Duration::from_millis(500));
                    let data_dir_str = cli.data_dir.to_string_lossy().to_string();
                    let child = process::Command::new(&exe)
                        .args(["--data-dir", &data_dir_str, "serve", "--host", "0.0.0.0"])
                        .stdin(process::Stdio::null())
                        .stdout(process::Stdio::null())
                        .stderr(process::Stdio::null())
                        .spawn();
                    match child {
                        Ok(c) => println!("✅ Update complete. Restarted (PID {})", c.id()),
                        Err(e) => println!("✅ Update complete. Auto-restart failed: {}. Run `earthgrid start`.", e),
                    }
                } else {
                    println!("✅ Update complete. Run `earthgrid start` to begin.");
                }
            }
        }

        Commands::Fetch { bbox, tile, start, end, collection, bands, limit, cloud_cover } => {
            if bbox.is_some() && tile.is_some() {
                eprintln!("❌ Cannot use both --bbox and --tile. Pick one.");
                process::exit(1);
            }

            // Resolve tile name to bbox if provided
            let resolved_bbox = match (&bbox, &tile) {
                (Some(b), _) => Some(b.clone()),
                (_, Some(t)) => {
                    match mgrs::tile_to_bbox(t) {
                        Ok(b) => {
                            let bbox_str = format!("{:.4},{:.4},{:.4},{:.4}", b[0], b[1], b[2], b[3]);
                            println!("📍 Tile {} → bbox {}", t.to_uppercase(), bbox_str);
                            Some(bbox_str)
                        }
                        Err(e) => {
                            eprintln!("❌ Invalid tile name '{}': {}", t, e);
                            process::exit(1);
                        }
                    }
                }
                _ => None,
            };

            let port = config_port();
            let mut params = vec![format!("limit={}", limit)];
            if let Some(b) = resolved_bbox { params.push(format!("bbox={}", b)); }
            if let Some(t) = &tile { params.push(format!("tile={}", t.to_uppercase())); }
            if let Some(s) = start { params.push(format!("start_date={}", s)); }
            if let Some(e) = end { params.push(format!("end_date={}", e)); }
            if let Some(c) = collection { params.push(format!("collection={}", c)); }
            if let Some(b) = bands { params.push(format!("bands={}", b)); }
            if let Some(cc) = cloud_cover { params.push(format!("cloud_cover={}", cc)); }
            let host = config_host();
            let url = format!("http://{}:{}/api/fetch?{}", host, port, params.join("&"));

            println!("🛰️  Fetching from {}...", url);
            match ureq::post(&url).send("") {
                Ok(resp) => {
                    let body: serde_json::Value = resp.into_body().read_json()?;
                    println!("{}", serde_json::to_string_pretty(&body)?);
                }
                Err(e) => {
                    eprintln!("❌ Fetch failed: {}", e);
                    eprintln!("   Is EarthGrid running? Try: earthgrid status");
                    process::exit(1);
                }
            }
        }

        Commands::Setup => {
            println!("🛠️  EarthGrid First-Time Setup");
            println!("   Press Enter to accept defaults.\n");

            let home = earthgrid_home();
            fs::create_dir_all(&home)?;

            // Existing config?
            let existing: serde_json::Value = fs::read_to_string(config_file())
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or(serde_json::json!({}));

            // Generate a random default name if none exists
            let default_name = existing["node_name"].as_str()
                .filter(|n| !n.is_empty() && *n != "earthgrid-node")
                .map(|s| s.to_string())
                .unwrap_or_else(|| {
                    let id = uuid::Uuid::new_v4().to_string();
                    let seed = id.bytes().fold(0usize, |acc, b| acc.wrapping_add(b as usize));
                    let adj = ["swift","bold","calm","dark","fair","keen","wild","warm","cool","free","pure","vast","deep","high","blue","gold","iron","jade","onyx","ruby"][seed % 20];
                    let noun = ["peak","lake","reef","mesa","vale","cove","dune","glen","rift","ford","cape","isle","arch","dale","knoll","ridge","brook","cliff","grove","shore"][(seed / 20) % 20];
                    let suffix = &id[..4];
                    format!("{adj}-{noun}-{suffix}")
                });
            let node_name = prompt("Node name", &default_name)?;
            let storage_path = prompt("Storage path",
                existing["data_dir"].as_str().unwrap_or(&home.join("data").to_string_lossy()))?;
            let storage_limit = prompt("Storage limit (GB)",
                &existing["storage_limit_gb"].as_f64().unwrap_or(100.0).to_string())?;
            let port = prompt("HTTP port",
                &existing["port"].as_u64().unwrap_or(8400).to_string())?;
            let beacon_url = prompt("Beacon/bootstrap URL (leave blank for none)",
                existing["beacon_url"].as_str().unwrap_or(""))?;

            let config = serde_json::json!({
                "node_name": node_name,
                "data_dir": storage_path,
                "storage_limit_gb": storage_limit.parse::<f64>().unwrap_or(100.0),
                "port": port.parse::<u16>().unwrap_or(8400),
                "beacon_url": beacon_url,
            });

            fs::write(config_file(), serde_json::to_string_pretty(&config)?)?;
            println!("\n✅ Config written to {}", config_file().display());

            // Ask to install service
            let install = prompt("Install systemd user service? (y/N)", "N")?;
            if install.to_lowercase() == "y" {
                install_systemd_service()?;
            }

            println!("\n🚀 Setup complete! Run `earthgrid start` to begin.");
        }

        Commands::InstallService => {
            install_systemd_service()?;
        }

        Commands::UninstallService => {
            uninstall_systemd_service()?;
        }

        Commands::Resize { size } => {
            let cfg_path = config_file();
            let contents = fs::read_to_string(&cfg_path)
                .unwrap_or_else(|_| "{}".to_string());
            let mut config: serde_json::Value = serde_json::from_str(&contents)
                .unwrap_or(serde_json::json!({}));

            config["storage_limit_gb"] = serde_json::json!(size);
            fs::create_dir_all(earthgrid_home())?;
            fs::write(&cfg_path, serde_json::to_string_pretty(&config)?)?;
            println!("✅ Storage limit updated to {:.1} GB", size);

            // Auto-evict if current storage exceeds new limit
            let data_dir = cli.data_dir.clone();
            let store_path = data_dir.join("store");
            let catalog_path = data_dir.join("catalog.db");

            if store_path.exists() {
                let mut store = ChunkStore::new(&store_path, size)?;
                let current_gb = store.total_bytes() as f64 / 1_073_741_824.0;

                if current_gb > size {
                    println!("\n📊 Current storage: {:.1} GB — exceeds new limit", current_gb);
                    println!("🗑️  Running eviction to free {:.1} GB...", current_gb - size);

                    let catalog = Catalog::new(&catalog_path)?;
                    let beacon_db = earthgrid_core::config::Settings::config_dir().join("beacon.db");
                    let beacon_path = if beacon_db.exists() { Some(beacon_db.as_path()) } else { None };

                    match earthgrid_core::eviction::evict(&catalog, &mut store, size, beacon_path) {
                        Ok(result) => {
                            println!("   Items deleted: {}", result.items_deleted);
                            println!("   Space freed:   {:.1} GB", result.bytes_freed as f64 / 1_073_741_824.0);
                            if result.items_kept > 0 {
                                println!("   Items kept:    {} (last replica — not safe to delete)", result.items_kept);
                            }
                            let new_gb = store.total_bytes() as f64 / 1_073_741_824.0;
                            println!("   New storage:   {:.1} GB", new_gb);
                            if new_gb > size {
                                println!("   ⚠️  Still over limit — {} items are last replicas and cannot be evicted", result.items_kept);
                                println!("   Use `earthgrid evict --force` to delete anyway (data may be lost!)");
                            }
                        }
                        Err(e) => eprintln!("   ❌ Eviction failed: {}", e),
                    }
                } else {
                    println!("   Storage ({:.1} GB) is within new limit ✅", current_gb);
                }
            }

            println!("\n   Restart EarthGrid to apply: earthgrid stop && earthgrid start");
        }
        Commands::Admin { action } => {
            let users_db = cli.data_dir.join("users.db");

            let ua = earthgrid_core::user_auth::UserAuth::new(&users_db)?;

            match action {
                AdminAction::AddUser { username, role } => {
                    match role.as_str() {
                        "admin" | "user" | "readonly" => {}
                        _ => {
                            eprintln!("❌ Invalid role '{}'. Use: admin, user, or readonly", role);
                            process::exit(1);
                        }
                    }
                    match ua.add_user(&username, &role) {
                        Ok(api_key) => {
                            println!("✅ User created");
                            println!("   Username: {}", username);
                            println!("   Role:     {}", role);
                            println!("   API Key:  {}", api_key);
                        }
                        Err(e) => eprintln!("❌ Failed: {}", e),
                    }
                }
                AdminAction::ListUsers => {
                    match ua.list_users() {
                        Ok(users) => {
                            if users.is_empty() {
                                println!("No users found.");
                            } else {
                                println!("{:<20} {:<10} {:<20} {}", "USERNAME", "ROLE", "CREATED", "LAST USED");
                                println!("{}", "-".repeat(70));
                                for u in &users {
                                    let created = chrono_ts(u.created_at);
                                    let used = if u.last_used > 0.0 { chrono_ts(u.last_used) } else { "never".to_string() };
                                    println!("{:<20} {:<10} {:<20} {}", u.username, u.role, created, used);
                                }
                                println!("\n{} users", users.len());
                            }
                        }
                        Err(e) => eprintln!("❌ {}", e),
                    }
                }
                AdminAction::DeleteUser { username } => {
                    match ua.revoke_user(&username) {
                        Ok(true) => println!("✅ User '{}' deleted", username),
                        Ok(false) => println!("⚠️  User '{}' not found", username),
                        Err(e) => eprintln!("❌ {}", e),
                    }
                }
            }
        }
    }

    Ok(())
}

// Need std::path::Path for find_repo_dir
use std::path::Path;


fn chrono_ts(ts: f64) -> String {
    let secs = ts as i64;
    let dt = chrono::DateTime::from_timestamp(secs, 0);
    match dt {
        Some(dt) => dt.format("%Y-%m-%d %H:%M").to_string(),
        None => format!("{:.0}", ts),
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────────

fn run_serve(
    data_dir: PathBuf,
    host: String,
    port: u16,
    p2p_port: u16,
    bootstrap_peers: Option<String>,
    no_p2p: bool,
) -> anyhow::Result<()> {
    let bootstrap: Vec<String> = bootstrap_peers
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let p2p_channels = if !no_p2p {
            let node_name = std::env::var("EARTHGRID_NODE_NAME")
                .unwrap_or_else(|_| String::new()); // empty → ensure_node_name() generates one

            let net_config = earthgrid_core::network::NetworkConfig {
                data_dir: data_dir.clone(),
                listen_port: p2p_port,
                bootstrap_peers: bootstrap,
                node_name,
            };

            match earthgrid_core::network::start(net_config).await {
                Ok((event_rx, cmd_tx, peer_id)) => {
                    println!("🔗 libp2p peer ID: {}", peer_id);
                    println!("   P2P port: {}", p2p_port);
                    Some((event_rx, cmd_tx))
                }
                Err(e) => {
                    eprintln!("⚠️  libp2p failed to start: {} (HTTP-only mode)", e);
                    None
                }
            }
        } else {
            None
        };

        earthgrid_core::server::serve(data_dir, host, port, p2p_channels).await
    })?;
    Ok(())
}

fn read_pid() -> Option<u32> {
    fs::read_to_string(pid_file()).ok()?.trim().parse().ok()
}

fn is_process_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        // signal 0 checks process existence without sending a signal
        std::process::Command::new("kill")
            .args(["-0", &pid.to_string()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }
    #[cfg(windows)]
    {
        // Match exact PID field to avoid substring false positives.
        let pid_str = pid.to_string();
        std::process::Command::new("tasklist")
            .args(["/FI", &format!("PID eq {}", pid), "/NH"])
            .output()
            .map(|o| {
                String::from_utf8_lossy(&o.stdout).lines().any(|line| {
                    let mut parts = line.split_whitespace();
                    parts.nth(1) == Some(pid_str.as_str())
                })
            })
            .unwrap_or(false)
    }
    #[cfg(not(any(unix, windows)))]
    {
        false
    }
}

fn prompt(label: &str, default: &str) -> anyhow::Result<String> {
    let stdin = io::stdin();
    if default.is_empty() {
        print!("  {}: ", label);
    } else {
        print!("  {} [{}]: ", label, default);
    }
    io::stdout().flush()?;
    let mut line = String::new();
    stdin.lock().read_line(&mut line)?;
    let trimmed = line.trim().to_string();
    if trimmed.is_empty() {
        Ok(default.to_string())
    } else {
        Ok(trimmed)
    }
}

fn find_cargo() -> String {
    // Try common locations
    let candidates = [
        "/home/matteo/.cargo/bin/cargo",
        "/root/.cargo/bin/cargo",
        "cargo",
    ];
    for c in &candidates {
        if PathBuf::from(c).exists() {
            return c.to_string();
        }
    }
    // Fall back to PATH
    "cargo".to_string()
}

/// Check if dir or any parent (up to 3 levels) contains .git
fn has_git_dir(dir: &PathBuf) -> bool {
    let mut d = dir.clone();
    for _ in 0..4 {
        if d.join(".git").exists() {
            return true;
        }
        match d.parent() {
            Some(p) => d = p.to_path_buf(),
            None => break,
        }
    }
    false
}

fn find_repo_dir() -> PathBuf {
    // Try known locations
    for known in &[
        dirs::home_dir().map(|h| h.join("EarthGrid/earthgrid-core")),
        dirs::home_dir().map(|h| h.join("earthgrid-core")),
    ] {
        if let Some(p) = known {
            if p.join("Cargo.toml").exists() {
                return p.clone();
            }
        }
    }
    // Walk up from binary
    if let Ok(exe) = std::env::current_exe() {
        let mut dir = exe.parent().unwrap_or(Path::new("/")).to_path_buf();
        for _ in 0..5 {
            if dir.join("Cargo.toml").exists() {
                return dir;
            }
            dir = dir.parent().unwrap_or(Path::new("/")).to_path_buf();
        }
    }
    PathBuf::from(".")
}

fn service_file_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join(".config/systemd/user/earthgrid.service")
}

fn install_systemd_service() -> anyhow::Result<()> {
    let exe = std::env::current_exe()?;
    let home = earthgrid_home();
    let data_dir = home.join("data");
    let port = config_port();

    let service_content = format!(
        r#"[Unit]
Description=EarthGrid Node
After=network.target

[Service]
Type=simple
ExecStart={exe} --data-dir {data_dir} serve --host 0.0.0.0 --port {port}
Restart=on-failure
RestartSec=10
StandardOutput=append:{log}
StandardError=append:{log}

[Install]
WantedBy=default.target
"#,
        exe = exe.display(),
        data_dir = data_dir.display(),
        port = port,
        log = home.join("earthgrid.log").display(),
    );

    let svc_path = service_file_path();
    fs::create_dir_all(svc_path.parent().unwrap())?;
    fs::write(&svc_path, service_content)?;

    // Reload and enable
    let _ = process::Command::new("systemctl").args(["--user", "daemon-reload"]).status();
    let _ = process::Command::new("systemctl").args(["--user", "enable", "earthgrid"]).status();

    println!("✅ Service installed: {}", svc_path.display());
    println!("   Start:  systemctl --user start earthgrid");
    println!("   Status: systemctl --user status earthgrid");
    Ok(())
}

fn uninstall_systemd_service() -> anyhow::Result<()> {
    let _ = process::Command::new("systemctl").args(["--user", "stop", "earthgrid"]).status();
    let _ = process::Command::new("systemctl").args(["--user", "disable", "earthgrid"]).status();

    let svc_path = service_file_path();
    if svc_path.exists() {
        fs::remove_file(&svc_path)?;
        println!("✅ Service file removed: {}", svc_path.display());
    } else {
        println!("⚠️  Service file not found: {}", svc_path.display());
    }

    let _ = process::Command::new("systemctl").args(["--user", "daemon-reload"]).status();
    println!("✅ EarthGrid service uninstalled.");
    Ok(())
}
