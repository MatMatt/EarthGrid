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
| 🍎 macOS arm64 | `earthgrid-macos-arm64` | — |
| 🪟 Windows x86_64 | `earthgrid-windows-x86_64.exe` | — |

```bash
# Linux / macOS
chmod +x earthgrid-*
sudo mv earthgrid-linux-x86_64 /usr/local/bin/earthgrid
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

# Windows
# GDAL included via vcpkg or OSGeo4W
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
#   earthgrid-tray  — system tray app (Linux/GTK)
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

The tray app shows your node status at a glance:

- 🌍 **Online** — node is running and connected
- 🌑 **Offline** — node not running or unreachable

```bash
# Install (Linux)
cp earthgrid-tray ~/.local/bin/

# Auto-start on login
cp earthgrid-tray.desktop ~/.config/autostart/

# Run
earthgrid-tray &
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

| Module | LOC | Description |
|---|---|---|
| `main.rs` | 1,783 | CLI (clap v4) — all subcommands |
| `gamification.rs` | 1,637 | Achievements, leaderboards, challenges |
| `server.rs` | 1,280 | HTTP API (Actix-web) |
| `openeo.rs` | 937 | openEO v1.2.0 gateway |
| `fetcher.rs` | 629 | CDSE + Element84 data fetching |
| `catalog.rs` | 543 | STAC catalog (SQLite) |
| `beacon.rs` | 532 | Peer discovery + coordination |
| `stats.rs` | 521 | Download statistics |
| `smart_replication.rs` | 443 | Beacon-coordinated replication |
| `ingest.rs` | 406 | GDAL spatial tiling + COG ingest |
| `config.rs` | 388 | TOML config management |
| `bandwidth.rs` | 356 | Token bucket rate control |
| `chunk_store.rs` | 356 | Content-addressed storage (SHA-256) |
| `client.rs` | 352 | M2M client for node access |
| `reconstruct.rs` | 359 | COG reconstruction from chunks |
| `processing.rs` | 330 | NDVI, NDWI, EVI, cloud mask |
| `node_identity.rs` | 370 | Ed25519 keypair (libp2p) |
| `source_users.rs` | 290 | CDSE credential management |
| `ratelimit.rs` | 252 | Sliding window rate limiter |
| `user_auth.rs` | 244 | API key management |
| `federation.rs` | 225 | Federated search across peers |
| + more | — | network, transport, peers, auth, audit |
| **Total** | **14,141** | **81 tests passing** |

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
earthgrid start                          # Install systemd service + start
earthgrid start --foreground             # Run in foreground (debug)
earthgrid stop                           # Stop node
earthgrid status                         # Show storage usage + peers
earthgrid info                           # Show config
earthgrid update                         # Git pull + cargo build + restart
earthgrid resize 100                     # Change storage limit to 100 GB
earthgrid install-service                # Install systemd service
earthgrid uninstall-service              # Remove systemd service
```

### Data operations

```bash
earthgrid fetch --bbox 12.4,55.6,12.6,55.7 --collection sentinel-2-l2a
earthgrid fetch --bbox ... --start 2026-03-01 --end 2026-03-12
earthgrid fetch --bbox ... --limit 0     # Fetch ALL available
earthgrid sync <peer_url>                # Pull data from a peer
earthgrid verify                         # Verify chunk integrity
earthgrid verify --heal                  # Auto-repair corrupted chunks
earthgrid ops                            # List processing operations
earthgrid process <item_id> --op ndvi    # Run NDVI on an item
```

### Data source management

```bash
earthgrid sources list                   # List source accounts
earthgrid sources providers              # Available providers
earthgrid sources add --provider cdse --username me@example.org
earthgrid sources remove --provider cdse --username me@example.org
```

### Admin

```bash
earthgrid admin show-key                 # Show admin API key
earthgrid admin renew-key                # Generate new key
```

### Docker

```bash
earthgrid docker start                   # Build + start container
earthgrid docker stop                    # Stop container
earthgrid docker status                  # Container status
earthgrid docker logs                    # View logs
earthgrid docker restart                 # Restart
earthgrid docker update                  # Rebuild + restart
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

EarthGrid includes an openEO v1.2.0 compatible gateway. Missing data is automatically fetched on demand.

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

EarthGrid is provided **"as is"**, without warranty of any kind. Not affiliated with ESA, EEA, Copernicus, USGS, or any other data provider.

## License

[EUPL-1.2](LICENSE) — European Union Public Licence.
