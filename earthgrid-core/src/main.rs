//! EarthGrid Core CLI + HTTP Server.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use earthgrid_core::chunk_store::ChunkStore;
use earthgrid_core::catalog::Catalog;
use earthgrid_core::auth::AuthConfig;
use earthgrid_core::ingest;

#[derive(Parser)]
#[command(name = "earthgrid-core", version, about = "Distributed EO data storage")]
struct Cli {
    /// Path to data directory
    #[arg(long, default_value = "./data")]
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

    /// Start the HTTP server
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
        /// e.g. /ip4/1.2.3.4/tcp/9400/p2p/12D3Koo...
        #[arg(long, env = "EARTHGRID_BOOTSTRAP_PEERS")]
        bootstrap_peers: Option<String>,

        /// Disable libp2p networking
        #[arg(long)]
        no_p2p: bool,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let store_path = cli.data_dir.join("store");
    let catalog_path = cli.data_dir.join("catalog.db");

    match cli.command {
        Commands::Info => {
            let store = ChunkStore::new(&store_path, 0.0)?;
            let catalog = Catalog::new(&catalog_path)?;
            let auth = AuthConfig::from_env();
            let stats = store.stats();
            println!("🌍 EarthGrid Core v{}", env!("CARGO_PKG_VERSION"));
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
                item_id,
                valid,
                total,
                missing,
                corrupted,
                if corrupted == 0 && missing == 0 { "✅ OK" } else { "❌ FAILED" }
            );
        }

        Commands::List { collection, limit } => {
            let catalog = Catalog::new(&catalog_path)?;
            let items = catalog.search(collection.as_deref(), None, limit)?;
            if items.is_empty() {
                println!("No items found.");
            } else {
                for item in &items {
                    println!(
                        "  {} | {} | {} chunks | bbox [{:.1},{:.1},{:.1},{:.1}]",
                        item.id,
                        item.collection,
                        item.chunk_hashes.len(),
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
            let bootstrap: Vec<String> = bootstrap_peers
                .unwrap_or_default()
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async {
                // Start libp2p network (if not disabled)
                if !no_p2p {
                    let node_name = std::env::var("EARTHGRID_NODE_NAME")
                        .unwrap_or_else(|_| "earthgrid-node".to_string());

                    let net_config = earthgrid_core::network::NetworkConfig {
                        data_dir: cli.data_dir.clone(),
                        listen_port: p2p_port,
                        bootstrap_peers: bootstrap,
                        node_name,
                    };

                    match earthgrid_core::network::start(net_config).await {
                        Ok((_event_rx, _cmd_tx, peer_id)) => {
                            println!("🔗 libp2p peer ID: {}", peer_id);
                            println!("   P2P port: {}", p2p_port);
                            // TODO: wire event_rx/cmd_tx into server for P2P request handling
                        }
                        Err(e) => {
                            eprintln!("⚠️  libp2p failed to start: {} (HTTP-only mode)", e);
                        }
                    }
                }

                // Start HTTP server
                earthgrid_core::server::serve(cli.data_dir.clone(), host, port).await
            })?;
        }
    }

    Ok(())
}
