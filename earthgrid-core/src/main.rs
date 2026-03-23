//! EarthGrid CLI + HTTP Server.

use std::fs;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::process;

use clap::{Parser, Subcommand};
use earthgrid_core::chunk_store::ChunkStore;
use earthgrid_core::catalog::Catalog;
use earthgrid_core::auth::AuthConfig;
use earthgrid_core::ingest;
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
    /// Show node info and statistics
    Info,

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

    /// Ingest a file into the store
    Ingest {
        /// Path to file to ingest
        file: PathBuf,

        /// Collection name
        #[arg(long)]
        collection: String,

        /// Chunk size in bytes (default 4MB)
        #[arg(long)]
        chunk_size: Option<usize>,
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

    /// Show daemon status and node stats
    Status,

    /// Pull latest code, rebuild, and restart
    Update,

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
        Commands::Info => {
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
        }

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

        Commands::Ingest { file, collection, chunk_size } => {
            let mut store = ChunkStore::new(&store_path, 0.0)?;
            let catalog = Catalog::new(&catalog_path)?;
            let cs = chunk_size.unwrap_or(ingest::DEFAULT_CHUNK_SIZE);

            println!("📥 Ingesting {} → collection '{}'", file.display(), collection);
            let item = ingest::ingest_file(&file, &collection, cs, &mut store)?;
            catalog.add_item(&item)?;

            println!("✅ Ingested: {}", item.id);
            println!("   Chunks: {}", item.chunk_hashes.len());
            println!("   File size: {} bytes", item.properties["earthgrid:file_size"]);
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
            let pid = read_pid();
            let running = pid.map(is_process_alive).unwrap_or(false);

            if let Some(p) = pid {
                if running {
                    println!("✅ EarthGrid is running (PID {})", p);
                } else {
                    println!("⚠️  EarthGrid is NOT running (stale PID {})", p);
                    return Ok(());
                }
            } else {
                println!("⚠️  EarthGrid is NOT running (no PID file)");
                return Ok(());
            }

            // Fetch node info from API
            let port = config_port();
            let url = format!("http://localhost:{}/node-info", port);
            match ureq::get(&url).call() {
                Ok(resp) => {
                    let body: serde_json::Value = resp.into_body().read_json()?;
                    println!("\n📊 Node Info:");
                    println!("   Node ID:   {}", body["node_id"].as_str().unwrap_or("?"));
                    println!("   Version:   {}", body["version"].as_str().unwrap_or("?"));
                    println!("   Items:     {}", body["item_count"].as_u64().unwrap_or(0));
                    println!("   Chunks:    {}", body["chunks"].as_u64().unwrap_or(0));
                    println!("   Storage:   {:.2} GB",
                        body["storage_gb"].as_f64().unwrap_or(0.0));
                    println!("   Peers:     {}", body["peers"].as_u64().unwrap_or(0));
                    println!("   API:       http://localhost:{}", port);
                }
                Err(e) => {
                    println!("   (Could not reach API: {})", e);
                }
            }
        }

        Commands::Update => {
            // Find repo dir (go up from binary or use known path)
            let repo_dir = find_repo_dir();
            println!("📦 Updating EarthGrid...");
            println!("   Repo: {}", repo_dir.display());

            // git pull
            println!("\n1️⃣  git pull...");
            let status = process::Command::new("git")
                .args(["pull"])
                .current_dir(&repo_dir)
                .status()?;
            if !status.success() {
                anyhow::bail!("git pull failed");
            }

            // cargo build --release
            println!("\n2️⃣  cargo build --release...");
            let cargo = find_cargo();
            let status = process::Command::new(&cargo)
                .args(["build", "--release"])
                .current_dir(&repo_dir)
                .status()?;
            if !status.success() {
                anyhow::bail!("cargo build failed");
            }

            // cargo install
            println!("\n3️⃣  cargo install...");
            let status = process::Command::new(&cargo)
                .args(["install", "--path", ".", "--force"])
                .current_dir(&repo_dir)
                .status()?;
            if !status.success() {
                anyhow::bail!("cargo install failed");
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
                let was_running = if let Some(pid) = read_pid() {
                    if is_process_alive(pid) {
                        let _ = process::Command::new("kill").arg("-TERM").arg(pid.to_string()).status();
                        std::thread::sleep(std::time::Duration::from_secs(2));
                        true
                    } else { false }
                } else { false };

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
            let url = format!("http://localhost:{}/fetch?{}", port, params.join("&"));

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

            let node_name = prompt("Node name",
                existing["node_name"].as_str().unwrap_or("earthgrid-node"))?;
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
            println!("   Restart EarthGrid to apply: earthgrid stop && earthgrid start");
        }
    }

    Ok(())
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
                .unwrap_or_else(|_| "earthgrid-node".to_string());

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

fn find_repo_dir() -> PathBuf {
    // Try known location first
    let known = PathBuf::from("/home/matteo/EarthGrid/earthgrid-core");
    if known.join("Cargo.toml").exists() {
        return known;
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

// Need std::path::Path for find_repo_dir
use std::path::Path;
