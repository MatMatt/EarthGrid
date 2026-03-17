//! EarthGrid Core CLI + HTTP Server.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use earthgrid_core::chunk_store::ChunkStore;
use earthgrid_core::catalog::Catalog;
use earthgrid_core::auth::AuthConfig;

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

    /// Start the HTTP server
    Serve {
        /// Host to bind
        #[arg(long, default_value = "0.0.0.0")]
        host: String,

        /// Port to listen on
        #[arg(long, default_value = "8400")]
        port: u16,
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

        Commands::Serve { host, port } => {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(earthgrid_core::server::serve(cli.data_dir, host, port))?;
        }
    }

    Ok(())
}
