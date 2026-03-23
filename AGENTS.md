# AGENTS.md — AI Contributor Guide

> Context for AI coding tools (Claude Code, Codex, Cursor, etc.)
> Read this before making any changes.

## Project Overview

EarthGrid is a **decentralized P2P network for Earth observation data**. Nodes store satellite imagery (Sentinel-2, Landsat) as content-addressed chunks and serve them via STAC API and openEO processing.

Think: BitTorrent meets STAC for satellite data.

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | **Rust** (100% — no Python, no JS backend) |
| HTTP | axum |
| P2P | libp2p (Kademlia, mDNS, Relay, DCUtR) |
| Database | SQLite via rusqlite (bundled) |
| Geospatial | GDAL (libgdal-dev required) |
| Config | TOML (`~/.earthgrid/config.toml`) |
| CI | GitHub Actions |

## Project Structure

```
earthgrid-core/
├── src/
│   ├── main.rs          # CLI entry point (clap)
│   ├── server.rs         # axum HTTP server
│   ├── lib.rs            # Module declarations
│   ├── config.rs         # Configuration
│   ├── error.rs          # Error types
│   ├── chunk_store.rs    # Content-addressed storage
│   ├── catalog.rs        # STAC catalog management
│   ├── ingest.rs         # GDAL ingest + COG
│   ├── fetcher.rs        # Remote STAC fetch + distributed delegation
│   ├── openeo.rs         # openEO processing gateway
│   ├── processing.rs     # Raster operations (NDVI, etc.)
│   ├── network.rs        # libp2p networking
│   ├── federation.rs     # Cross-node catalog sync
│   ├── beacon.rs         # Node registry/discovery
│   ├── beacon_federation.rs  # Beacon-to-beacon WebSocket sync
│   ├── replication.rs    # Auto-sync from peers
│   ├── smart_replication.rs  # Intelligent replication
│   ├── peers.rs          # Peer management
│   ├── auth.rs           # API key auth
│   ├── user_auth.rs      # User management
│   ├── gamification.rs   # Leaderboard scoring
│   ├── stats.rs          # Network statistics
│   ├── bandwidth.rs      # Speed measurement
│   ├── ratelimit.rs      # Rate limiting
│   ├── transport.rs      # P2P transport
│   ├── client.rs         # HTTP client
│   ├── node_identity.rs  # Node ID + name generation
│   ├── source_users.rs   # Data provider credentials
│   ├── mgrs.rs           # MGRS tile handling
│   ├── audit.rs          # Action logging
│   └── reconstruct.rs    # File reconstruction from chunks
│   └── routes/
│       ├── mod.rs
│       ├── admin.rs      # Admin endpoints
│       ├── chunks.rs     # Chunk upload/download
│       ├── federation.rs # Catalog exchange
│       ├── gamification_routes.rs
│       ├── ingest_routes.rs  # Fetch + ingest
│       ├── process.rs    # Processing requests
│       ├── stac.rs       # STAC API
│       ├── stats.rs      # Stats API
│       └── misc.rs       # Health, info
├── Cargo.toml
└── tests/
docs/
├── beacon-guide.md
├── federation.md
├── storage-strategy.md
├── TESTING.md
└── index.html            # GitHub Pages dashboard
```

## Build

```bash
# Prerequisites: Rust stable, libgdal-dev
sudo apt install libgdal-dev   # Ubuntu/Debian

# Build
cargo build --release

# With tray app
cargo build --release --features tray

# Run tests
cargo test

# Docker
docker build -t earthgrid .
```

## Coding Conventions

### Must Follow
- **No Python**. Everything is Rust.
- **No `unwrap()` in library code** — use `?` or proper error handling via `EarthGridError`
- **No `unsafe`** unless absolutely necessary (and documented why)
- **Rusqlite pattern**: avoid block-scoped `stmt` — use flat scope to prevent lifetime errors:
  ```rust
  // ❌ Bad — lifetime issues
  {
      let mut stmt = conn.prepare("...")?;
      let rows = stmt.query_map([], |row| { ... })?;
      rows.collect()  // stmt dropped too early
  }

  // ✅ Good — flat scope
  let mut stmt = conn.prepare("...")?;
  let results: Vec<_> = stmt.query_map([], |row| { ... })?.collect::<Result<_, _>>()?;
  ```
- **Validate after edits**: always run `cargo build --release` and `cargo test` before committing
- **Commit messages**: conventional commits (`feat:`, `fix:`, `docs:`, `refactor:`)

### Style
- Keep modules focused — one responsibility per file
- Use `thiserror` for error types
- Prefer `serde` derive for all structs that cross API boundaries
- SQLite tables: snake_case, columns: snake_case
- API endpoints: RESTful, kebab-case paths

### Don't
- Don't add Python scripts or wrapper layers
- Don't use `sed` for patching Rust files — edit properly
- Don't add external databases (PostgreSQL, Redis) — SQLite only
- Don't break the EUPL-1.2 license compatibility

## Key Design Decisions

1. **Content-addressed storage**: Every chunk is SHA-256 hashed. Deduplication and integrity built in.
2. **STAC-native**: All metadata follows STAC spec. Items, collections, catalogs.
3. **Beacon is optional**: Beacons help discovery but nodes can work peer-to-peer via gossip.
4. **openEO as processing API**: Standard interface, not custom endpoints.
5. **Distributed fetch**: Beacon delegates downloads across nodes weighted by free storage.
6. **No personal uploads**: Only official data sources (Copernicus, Landsat, etc.)

## Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `EARTHGRID_PORT` | HTTP port | `8400` |
| `EARTHGRID_DATA_DIR` | Data storage path | `./data` |
| `EARTHGRID_NODE_NAME` | Node display name | `node-alpha` |
| `EARTHGRID_PUBLIC_URL` | Public URL for registration | `http://192.168.188.60:8400` |
| `EARTHGRID_BEACON_URL` | Beacon URL to register with | `http://192.168.188.60:8400` |
| `EARTHGRID_SOURCE_KEY` | Data source API key | (Element84 key) |
| `EARTHGRID_ADMIN_KEY` | Admin API key | (generated) |
| `EARTHGRID_STORAGE_LIMIT_GB` | Max storage in GB | `1000` |

## Testing

```bash
# Run all tests
cargo test

# Run specific module tests
cargo test --lib chunk_store
cargo test --lib catalog

# Current: 138 tests, all passing
```

## Contributing Workflow

1. Check `STATUS.md` for current feature status and open targets
2. Create a feature branch or work on master (small team)
3. `cargo build --release` — must compile with zero warnings
4. `cargo test` — all tests must pass
5. Commit with conventional message
6. Push to GitHub

## Active Nodes for Testing

- **Nucleus** (192.168.188.60:8400) — beacon + node-alpha
- **LenovoTP** (192.168.188.219:8400) — wild-mesa-9687
