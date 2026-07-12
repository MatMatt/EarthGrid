# EarthGrid 🌍

Distributed storage and openEO processing for Earth observation data.

**No single point of failure. No vendor lock-in. Community-driven.**

[![Live Dashboard](https://img.shields.io/badge/dashboard-live-brightgreen)](https://mattiuzzi.zapto.org/earthgrid)
[![Rust](https://img.shields.io/badge/rust-stable-orange)](https://github.com/MatMatt/EarthGrid)
[![Tests](https://img.shields.io/badge/tests-148%20passing-brightgreen)](https://github.com/MatMatt/EarthGrid)
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

Pick one method — they all get you to the same result:

| Method | GDAL required? | Best for |
| ------ | -------------- | -------- |
| [**Binary**](#option-1-pre-built-binary) | Yes (install first) | Most users |
| [**.deb package**](#option-2-debianubuntu-deb-package) | Auto-installed | Debian/Ubuntu |
| [**Docker**](#option-3-docker) | No (bundled) | Quick test, no setup |
| [**From source**](#option-4-build-from-source) | Yes (install first) | Contributors, custom builds |

---

### Option 1: Pre-built binary

**Step 1 — Install GDAL:**

```bash
# macOS
brew install gdal

# Debian / Ubuntu
sudo apt install libgdal-dev
```

<details>
<summary>Windows GDAL options</summary>

```bash
# Option A: OSGeo4W (recommended)
#   https://download.osgeo.org/osgeo4w/v2/osgeo4w-setup.exe
#   Install GDAL, then set:
#     set GDAL_HOME=C:\OSGeo4W
#     set GDAL_LIB_DIR=C:\OSGeo4W\lib

# Option B: vcpkg
#   vcpkg install gdal:x64-windows

# Option C: conda-forge
#   conda install -c conda-forge gdal
```
</details>

**Step 2 — Download and install:**

Go to [**Releases**](https://github.com/MatMatt/EarthGrid/releases) and download the binary for your platform:

```bash
# Linux
chmod +x earthgrid-linux-x86_64
sudo mv earthgrid-linux-x86_64 /usr/local/bin/earthgrid

# macOS
chmod +x earthgrid-macos-arm64
sudo mv earthgrid-macos-arm64 /usr/local/bin/earthgrid
```

---

### Option 2: Debian/Ubuntu (.deb package)

GDAL is installed automatically as a dependency.

```bash
sudo dpkg -i earthgrid-core_*_amd64.deb
sudo apt-get install -f  # resolve dependencies if needed
```

---

### Option 3: Docker

No prerequisites — GDAL is included in the image.

```bash
docker pull ghcr.io/matmatt/earthgrid-core:latest

docker run -d \
  --name earthgrid \
  -p 8400:8400 \
  -v earthgrid-data:/data \
  ghcr.io/matmatt/earthgrid-core:latest serve
```

---

### Option 4: Build from source

Requires GDAL (see [Option 1](#option-1-pre-built-binary)) and Rust.

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

| Module                 | Description                                                                             |
| ---------------------- | --------------------------------------------------------------------------------------- |
| `main.rs`              | CLI (clap v4) — all subcommands                                                         |
| `gamification.rs`      | Achievements, leaderboards, challenges                                                  |
| `server.rs`            | HTTP API (axum)                                                                         |
| `openeo/`              | openEO v1.2 gateway — graph execution, formats, temporal aggregate, `gdalwarp` resample |
| `fetcher.rs`           | CDSE + Element84 data fetching                                                          |
| `catalog.rs`           | STAC catalog (SQLite)                                                                   |
| `beacon.rs`            | Peer discovery + coordination                                                           |
| `stats.rs`             | Download statistics                                                                     |
| `smart_replication.rs` | Beacon-coordinated replication                                                          |
| `ingest.rs`            | GDAL spatial tiling + COG ingest                                                        |
| `config.rs`            | TOML config management                                                                  |
| `bandwidth.rs`         | Token bucket rate control                                                               |
| `chunk_store.rs`       | Content-addressed storage (SHA-256)                                                     |
| `client.rs`            | M2M client for node access                                                              |
| `reconstruct.rs`       | COG reconstruction from chunks                                                          |
| `processing.rs`        | NDVI, NDWI, EVI, cloud mask                                                             |
| `node_identity.rs`     | Ed25519 keypair (libp2p)                                                                |
| `source_users.rs`      | CDSE credential management                                                              |
| `ratelimit.rs`         | Sliding window rate limiter                                                             |
| `user_auth.rs`         | API key management                                                                      |
| `federation.rs`        | Federated search across peers                                                           |
| + more                 | network, transport, peers, auth, audit                                                  |
| **Total**              | **~15k LOC, 148 tests passing**                                                         |

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
earthgrid status                         # Show node info, storage stats & daemon status
earthgrid update                         # Auto-detect: source build or binary download
earthgrid update --source                # Force source build (git pull + cargo build)
earthgrid update --binary                # Force binary download from GitHub Releases
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

### Docker management

```bash
earthgrid docker update                  # Pull + rebuild + restart container
earthgrid docker logs                    # Show container logs
```

For initial Docker setup, see [Installation → Option 3](#option-3-docker).

To customize the host data directory, set `EARTHGRID_HOST_DATA_DIR` in `docker/.env`:

```bash
EARTHGRID_HOST_DATA_DIR=/mnt/sda/earthgrid
```

---

## MCP Server (AI Agent Integration)

EarthGrid includes a **Model Context Protocol** server that lets AI agents (Claude, Codex, etc.)
interact with the grid — trigger data fetches, query grid-wide coverage, and search the catalog.

### Setup

```bash
# Build the MCP binary (standalone crate)
cd earthgrid-mcp && cargo build --release

# Run it — connects to a running EarthGrid node
./target/release/earthgrid-mcp --api-key <your-key> --api-url http://localhost:8400
```

The `--api-key` is required — it's sent as `x-api-key` on every request.
Only the node owner can trigger data ingestion.

### Tools exposed

| Tool | Description | Auth |
|------|-------------|------|
| `earthgrid_fetch_enqueue` | Trigger data fetch from Element84 STAC | Key required |
| `earthgrid_fetch_status` | Check job progress | Key required |
| `earthgrid_fetch_list` | List fetch jobs by status | Key required |
| `earthgrid_coverage` | Get spatial coverage tiles (GeoJSON) | Read-only |
| `earthgrid_catalog_search` | Search STAC items | Read-only |

### Example: trigger a fetch

```
> earthgrid_fetch_enqueue
  bbox: "10.5,46.0,11.5,46.5"
  start_date: "2026-06-01"
  end_date: "2026-07-01"
  limit: 5
  bands: "B04,B08"

← {"job_id": 462, "status": "pending"}
```

### Wiring into an AI agent

Configure your agent's MCP client to launch `earthgrid-mcp` as a subprocess.
The server speaks JSON-RPC over stdio. Example config for Claude Code:

```json
{
  "mcpServers": {
    "earthgrid": {
      "command": "/path/to/earthgrid-mcp",
      "args": ["--api-key", "${EARTHGRID_API_KEY}", "--api-url", "http://localhost:8400"]
    }
  }
}
```

---

## Security Model

### What's open (by design)

| Action                    | Why                                |
| ------------------------- | ---------------------------------- |
| Browse the STAC catalog   | Public data should be discoverable |
| Download data             | Public data should be accessible   |
| View network status       | Transparency builds trust          |
| Search across the network | That's the whole point             |

### What's protected

| Action             | Protection       | Why                                       |
| ------------------ | ---------------- | ----------------------------------------- |
| Ingest new data    | API key          | Prevents unauthorized writes              |
| Run processing     | Per-user API key | Only authenticated users can process      |
| Source credentials | **CLI only**     | Provider credentials never leave the node |

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

Requires `pip install openeo`. The node must already hold matching scenes (e.g. `POST /fetch` for that bbox/bands) or `/result` will auto-fetch missing bands when possible. Use `fetch_metadata=False` because collection metadata from EarthGrid is minimal (no band dimension in STAC). 

```python
import openeo

conn = openeo.connect("http://localhost:8400")
cube = conn.load_collection(
    "sentinel-2-l2a",
    spatial_extent={"west": 12.4, "south": 55.6, "east": 12.6, "north": 55.7},
    temporal_extent=["2024-06-01", "2024-06-15"],
    bands=["B04", "B08"],
    fetch_metadata=False,
)
cube.ndvi(red="B04", nir="B08").save_result("GTiff").download("ndvi.tif")
```

**R:**

Install [`openeo` from CRAN](https://cran.r-project.org/package=openeo). Use a `temporal_extent` that overlaps ingested Sentinel-2 data (same idea as Python: run `POST /fetch` first or rely on auto-fetch when the graph runs).

```r
library(openeo)

con <- connect("http://localhost:8400")
p <- processes(con)

cube <- p$load_collection(
    "sentinel-2-l2a",
    spatial_extent = list(west = 12.4, south = 55.6, east = 12.6, north = 55.7),
    temporal_extent = c("2024-06-01", "2024-06-15"),
    bands = c("B04", "B08"))
ndvi <- p$ndvi(cube, red = "B04", nir = "B08")
result <- p$save_result(ndvi, format = "GTiff")
compute_result(result, output_file = "ndvi.tif", con = con)
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

| Provider              | Account | Data                          | Format           |
| --------------------- | ------- | ----------------------------- | ---------------- |
| **Element84** (AWS)   | ❌ No    | S2 L2A, S1 RTC, Landsat C2 L2 | Already COG      |
| **CDSE** (Copernicus) | ✅ Free  | S1, S2, S3, S5P, full archive | Converted to COG |

---

## Resource Usage

EarthGrid runs at the **lowest possible priority**:

- CPU: `nice -n 19` (lowest priority)
- I/O: `ionice -c 3` (idle class)

Your other workloads always come first.

---

## Dashboard

Live network stats: **[mattiuzzi.zapto.org/earthgrid](https://mattiuzzi.zapto.org/earthgrid)**

---

## Data Licensing & Attribution

| Data type                | Required attribution                                  |
| ------------------------ | ----------------------------------------------------- |
| Unmodified Sentinel data | *"Copernicus Sentinel data [Year]"*                   |
| Modified Sentinel data   | *"Contains modified Copernicus Sentinel data [Year]"* |
| Landsat data             | *"Landsat Level-2 data courtesy of USGS"*             |

---

## Disclaimer

EarthGrid is provided **"as is"**, without warranty of any kind. Not affiliated with ESA, Copernicus, USGS, or any other data provider.

## License

[EUPL-1.2](LICENSE) — European Union Public Licence.
