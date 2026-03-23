# EarthGrid 🌍

Distributed storage and openEO processing for Earth observation data.

**No single point of failure. No vendor lock-in. Community-driven.**

[![Live Dashboard](https://img.shields.io/badge/dashboard-live-brightgreen)](https://matmatt.github.io/EarthGrid/)
[![Rust](https://img.shields.io/badge/rust-stable-orange)](https://github.com/MatMatt/EarthGrid)
[![Tests](https://img.shields.io/badge/tests-81%20passing-brightgreen)](https://github.com/MatMatt/EarthGrid)
[![License](https://img.shields.io/badge/license-EUPL--1.2-blue)](LICENSE)

> ## 🚧 Early Stage — Test Phase
>
> **Do not install if you expect a functioning service.**
>
> EarthGrid is in active development and test phase. Things break, APIs change, data may be lost. It is not ready for production use.
>
> **We are looking for a few early testers:**
> - 🔦 **Beacon operators** — [How to run a beacon →](docs/beacon-guide.md)
> - 📦 **Data nodes** — spare disk space and a stable connection
>
> Not looking for mass adoption yet — a small, stable test network first.

## What is EarthGrid?

A federated network where anyone can run a node, store satellite data, and make it available to others. Think BitTorrent meets STAC for Earth observation.

EarthGrid stores **only official data** from sources like Copernicus (Sentinel) and Landsat. No personal uploads. The network exists as a **public good** for resilient access to Earth observation data.

## Why?

- Centralized platforms (CDSE, AWS, Google) = single points of failure + vendor lock-in
- Petabytes of EO data locked behind complex APIs and registrations
- Developing countries can't afford cloud storage but need local access
- Content-addressed storage provides integrity guarantees by design

---

## Installation

### Download pre-built binaries

Download the latest release for your platform from [**Releases**](https://github.com/MatMatt/EarthGrid/releases):

| Platform | Binary | Tray App |
|---|---|---|
| 🐧 Linux x86_64 | `earthgrid-linux-x86_64` | `earthgrid-tray-linux-x86_64` |
| 🍎 macOS arm64 | `earthgrid-macos-arm64` | `earthgrid-tray-macos-arm64` |
| 🪟 Windows x86_64 | `earthgrid-windows-x86_64.exe` | `earthgrid-tray-windows-x86_64.exe` |

```bash
# Linux / macOS (binary)
chmod +x earthgrid-*
sudo mv earthgrid-linux-x86_64 /usr/local/bin/earthgrid
```

#### Debian/Ubuntu (.deb package)

```bash
# Downloads and installs earthgrid-core + GDAL dependency
sudo dpkg -i earthgrid-core_*_amd64.deb
sudo apt-get install -f  # resolve dependencies if needed
```

#### Docker

```bash
docker pull ghcr.io/matmatt/earthgrid-core:latest

docker run -d \
  --name earthgrid \
  -p 8400:8400 \
  -v earthgrid-data:/data \
  ghcr.io/matmatt/earthgrid-core:latest serve
```

### Prerequisites

| Requirement | Version | Why |
|---|---|---|
| **GDAL** | ≥ 3.4 | Raster I/O (required on all platforms) |

```bash
# Debian / Ubuntu
sudo apt install libgdal-dev

# macOS
brew install gdal

# Windows — choose one of:
# Option 1: OSGeo4W (recommended, simple installer)
#   https://trac.osgeo.org/osgeo4w/
#   Download: https://download.osgeo.org/osgeo4w/v2/osgeo4w-setup.exe
#   Install GDAL, then set:
#     set GDAL_HOME=C:\OSGeo4W
#     set GDAL_LIB_DIR=C:\OSGeo4W\lib
#
# Option 2: vcpkg (Microsoft package manager)
#   https://vcpkg.io
#   vcpkg install gdal:x64-windows
#   set GDAL_HOME=C:\vcpkg\installed\x64-windows
#   set GDAL_LIB_DIR=C:\vcpkg\installed\x64-windows\lib
#
# Option 3: conda-forge
#   conda install -c conda-forge gdal
#   (GDAL_HOME auto-set by conda environment)
```

### Build from source

```bash
# Install Rust (if not installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source ~/.cargo/env

# Clone and build
git clone https://github.com/MatMatt/EarthGrid.git
cd EarthGrid/earthgrid-core
cargo build --release

# Binaries in target/release/
#   earthgrid       — core node + CLI
#   earthgrid-tray  — system tray app (Linux/macOS/Windows)
```

### Setup

```bash
earthgrid setup
```

The interactive setup wizard will ask for:

1. **Storage limit** — how much disk space to contribute (default: 50 GB)
2. **Participation mode** — Node + Beacon (recommended) or Node only
3. **Data directory** — where to store data (default: `~/.earthgrid/data`)
4. **Node name** — a friendly name for your node (auto-generated if skipped)
5. **Data sources** — Element84 is always enabled; optionally add CDSE credentials

After setup completes, EarthGrid **starts automatically** as a systemd user service.

```
✅ EarthGrid configured!
   Node:     my-node
   Storage:  100 GB at /mnt/data/earthgrid
   Beacon:   yes
   Port:     8400

🌐 WebUI: http://localhost:8400/ui
```

### System Tray App

Cross-platform system tray app (Linux, macOS, Windows). Shows node status at a glance:

- 🌍 **Online** — node is running and connected
- 🌑 **Offline** — node not running or unreachable

**Linux:**
```bash
cp earthgrid-tray ~/.local/bin/
cp earthgrid-tray.desktop ~/.config/autostart/  # auto-start
earthgrid-tray &
```

**macOS:**
```bash
cp earthgrid-tray /usr/local/bin/
earthgrid-tray &
```

**Windows:**
```
earthgrid-tray.exe
```

Right-click menu: Status, Open Dashboard, Quit.

### Verify

```bash
earthgrid status                         # Check node status
curl http://localhost:8400/health         # API health check
```

Open `http://localhost:8400/ui` in your browser to see the WebUI.

---

## Architecture

### Rust Core (v0.1.0)

The entire codebase is written in Rust for performance and reliability:

| Module | Description |
|---|---|
| `main.rs` | CLI (clap v4) — all subcommands |
| `gamification.rs` | Achievements, leaderboards, challenges |
| `server.rs` | HTTP API (Actix-web) |
| `openeo.rs` | openEO v1.2.0 gateway |
| `fetcher.rs` | CDSE + Element84 data fetching |
| `catalog.rs` | STAC catalog (SQLite) |
| `beacon.rs` | Peer discovery + coordination |
| `stats.rs` | Download statistics |
| `smart_replication.rs` | Beacon-coordinated replication |
| `ingest.rs` | GDAL spatial tiling + COG ingest |
| `config.rs` | TOML config management |
| `bandwidth.rs` | Token bucket rate control |
| `chunk_store.rs` | Content-addressed storage (SHA-256) |
| `client.rs` | M2M client for node access |
| `reconstruct.rs` | COG reconstruction from chunks |
| `processing.rs` | NDVI, NDWI, EVI, cloud mask |
| `node_identity.rs` | Ed25519 keypair (libp2p) |
| `source_users.rs` | CDSE credential management |
| `ratelimit.rs` | Sliding window rate limiter |
| `user_auth.rs` | API key management |
| `federation.rs` | Federated search across peers |
| + more | network, transport, peers, auth, audit |
| **Total** | **14k LOC, 81 tests passing** |

### Network Roles

```
┌─────────────────────────────────────────────────────┐
│                  COORDINATION LAYER                  │
│                                                     │
│   Beacon A  ←──federation──→  Beacon B              │
│      ↑                           ↑                  │
│   registry                    registry              │
│                                                     │
├─────────────────────────────────────────────────────┤
│                    DATA LAYER                        │
│                                                     │
│   Node 1          Node 2          Node 3            │
│   [S2 data]       [S2 data]       [S1 data]         │
│   can_source ✓    can_source ✗    can_source ✓      │
│                                                     │
└─────────────────────────────────────────────────────┘
```

**Beacon** — Lightweight coordinator. Maintains a registry of nodes, routes queries, federates with other beacons. Stores no data. → [Beacon Setup Guide](docs/beacon-guide.md)

**Node** — Stores and serves data chunks. Every chunk is identified by its SHA-256 hash (content-addressed). Nodes auto-sync data between each other.

### How Data Flows

1. Someone requests EO data for an area
2. Beacon checks which nodes have it
3. If cached → served directly from the nearest node
4. If not cached → any node with GDAL fetches from the provider, converts to COG, chunks, and stores
5. Other nodes automatically replicate the new data

### Storage Format

All data is stored as **Cloud-Optimized GeoTIFF (COG)** — one format everywhere:
- LZW compression, 256×256 tiling, PREDICTOR=2
- Conversion happens at **ingest time** (CDSE → COG → chunks)
- Reconstruction is reassembly only — no conversion needed

---

## CLI Reference

### Node management

```bash
earthgrid setup                          # Interactive first-time setup
earthgrid start                          # Start node as background daemon
earthgrid stop                           # Stop the running daemon
earthgrid status                         # Show if running + node stats
earthgrid info                           # Show local storage info
earthgrid update                         # Git pull + cargo build + restart
earthgrid resize --size 100              # Change storage limit to 100 GB
earthgrid serve                          # Start HTTP server (foreground)
```

### Data operations

```bash
earthgrid fetch --bbox 12.4,55.6,12.6,55.7 --collection sentinel-2-l2a
earthgrid fetch --bbox ... --start 2025-06-01 --end 2025-06-30
earthgrid fetch --bbox ... --limit 10    # Limit number of STAC items
earthgrid fetch --bbox ... --cloud-cover 20  # Max cloud cover %
earthgrid ingest <file> --collection sentinel-2-l2a  # Ingest a local file
earthgrid list                           # List items in catalog
earthgrid list --collection sentinel-2-l2a --limit 20
earthgrid verify <item_id>               # Verify chunk integrity of an item
```

### Docker

```bash
docker pull ghcr.io/matmatt/earthgrid-core:latest
docker run -d -p 8400:8400 -v earthgrid-data:/data ghcr.io/matmatt/earthgrid-core:latest serve
```

### Docker Compose (dev + prod volume path)

The compose file supports both local development and production storage paths:

```bash
# Dev (default, in docker/.env): EARTHGRID_HOST_DATA_DIR=./data
cd docker
docker compose up -d --build
```

```bash
# Prod host path (example)
cd docker
EARTHGRID_HOST_DATA_DIR=/mnt/sda/earthgrid docker compose up -d --build
```

Or set it once in `docker/.env`:

```bash
EARTHGRID_HOST_DATA_DIR=/mnt/sda/earthgrid
```

### Run locally (no Docker)

From source:

```bash
cd earthgrid-core
cargo run -- --data-dir ~/.earthgrid-data serve --host 0.0.0.0 --port 8400
```

Health check:

```bash
curl http://localhost:8400/health
```

Optional environment variables:

```bash
export EARTHGRID_NODE_NAME=node-local
export EARTHGRID_STORE_PATH=$HOME/.earthgrid-data/store
export EARTHGRID_CATALOG_PATH=$HOME/.earthgrid-data/catalog.db
```

---

## Security Model

### What's open (by design)

| Action | Why |
|---|---|
| Browse the STAC catalog | Public data should be discoverable |
| Download data | Public data should be accessible |
| View network status | Transparency builds trust |
| Search across the network | That's the whole point |

### What's protected

| Action | Protection | Why |
|---|---|---|
| Ingest new data | API key | Prevents unauthorized writes |
| Run processing | Per-user API key | Only authenticated users can process |
| Source credentials | **CLI only** | Provider credentials never leave the node |

### Node Authentication

Every node generates an **Ed25519 keypair** on first start. Peers verify each other's identity via signed key exchange — no secrets to share. Replay attacks blocked by 5-minute timestamp window.

### Built-in protections

- **Content-addressed storage**: Every chunk verified by SHA-256
- **Rate limiting**: 120 req/min per IP, burst limit 20/2s (LAN exempt)
- **Integrity verification**: `earthgrid verify` checks all chunks against stored hashes

---

## openEO Gateway

EarthGrid includes an openEO v1.2.0 compatible gateway.

**Python:**

```python
import openeo

conn = openeo.connect("http://localhost:8400")
cube = conn.load_collection("sentinel-2-l2a",
    spatial_extent={"west": 12.4, "south": 55.6, "east": 12.6, "north": 55.7},
    temporal_extent=["2026-03-01", "2026-03-12"],
    bands=["B04", "B08"])
cube.ndvi(red="B04", nir="B08").save_result("GTiff").download("ndvi.tif")
```

**R:**

```r
library(openeo)

con <- connect("http://localhost:8400")
p <- processes()

cube <- p$load_collection("sentinel-2-l2a",
    spatial_extent = list(west=12.4, south=55.6, east=12.6, north=55.7),
    temporal_extent = c("2026-03-01", "2026-03-12"),
    bands = c("B04", "B08"))
ndvi <- p$ndvi(cube, red="B04", nir="B08")
result <- p$save_result(ndvi, format="GTiff")
compute_result(result, "ndvi.tif")
```

---

## Gamification

Opt-in gamification encourages participation:

- **Achievements**: First Seed, Mesh Pioneer, Always On, Terabyte Club, Petabyte Dream
- **Leaderboards**: By nodes, users, groups
- **Challenges**: Weekly (Ingest Champion, Storage Hero) + Monthly (Marathon)
- **Economy Health**: Network freshness, diversity, redundancy, reuse

All gamification is privacy-respecting and opt-in.

---

## Data Sources

| Provider | Account | Data | Format |
|---|---|---|---|
| **Element84** (AWS) | ❌ No | S2 L2A, S1 RTC, Landsat C2 L2 | Already COG |
| **CDSE** (Copernicus) | ✅ Free | S1, S2, S3, S5P, full archive | Converted to COG |

---

## Resource Usage

EarthGrid runs at the **lowest possible priority**:

- CPU: `nice -n 19` (lowest priority)
- I/O: `ionice -c 3` (idle class)

Your other workloads always come first.

---

## Dashboard

Live network stats: **[matmatt.github.io/EarthGrid](https://matmatt.github.io/EarthGrid/)**

---

## Data Licensing & Attribution

| Data type | Required attribution |
|---|---|
| Unmodified Sentinel data | *"Copernicus Sentinel data [Year]"* |
| Modified Sentinel data | *"Contains modified Copernicus Sentinel data [Year]"* |
| Landsat data | *"Landsat Level-2 data courtesy of USGS"* |

---

## Disclaimer

EarthGrid is provided **"as is"**, without warranty of any kind. Not affiliated with ESA, Copernicus, USGS, or any other data provider.

## License

[EUPL-1.2](LICENSE) — European Union Public Licence.
