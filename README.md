# EarthGrid 🌍

Distributed storage and openEO processing for Earth observation data.

**No single point of failure. No vendor lock-in. Community-driven.**

[![Live Dashboard](https://img.shields.io/badge/dashboard-live-brightgreen)](https://matmatt.github.io/EarthGrid/)
[![Python](https://img.shields.io/badge/python-≥3.9-blue)](https://github.com/MatMatt/EarthGrid)
[![License](https://img.shields.io/badge/license-EUPL--1.2-blue)](LICENSE)
> ## 🚧 Early Stage — Test Phase
>
> **Do not install if you expect a functioning service.**
>
> EarthGrid is in active development and test phase. Things break, APIs change, data may be lost. It is not ready for production use.
>
> **We are looking for a few early testers:**
> - 🔦 **1–3 more beacon operators** — a small VPS (~€4/month) is enough. [How to run a beacon →](docs/beacon-guide.md)
> - 📦 **A handful of data nodes** — spare disk space (50–500 GB) and a stable connection
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

### Prerequisites

| Requirement | Version | Why |
|---|---|---|
| Python | ≥ 3.9 | Core application |
| Rust | ≥ 1.70 | Rust core (P2P, storage engine) |
| GDAL | ≥ 3.4 | Raster I/O (`libgdal-dev` on Debian/Ubuntu) |
| git | any | Clone the repository |

### 1. Install system dependencies

**Debian / Ubuntu:**

```bash
sudo apt update && sudo apt install -y python3 python3-pip libgdal-dev git
```

**macOS (Homebrew):**

```bash
brew install python gdal git
```

### 2. Install Rust (if not installed)

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source ~/.cargo/env
```

### 3. Clone and build

```bash
git clone https://github.com/MatMatt/EarthGrid.git
cd EarthGrid

# Build the Rust core
cd earthgrid-core && cargo build --release && cd ..

# Install Python package
pip install -e .
```

### 4. Setup

```bash
earthgrid setup
```

The interactive setup wizard will ask for:

1. **Storage limit** — how much disk space to contribute (default: 50 GB)
2. **Participation mode** — Node + Beacon (recommended) or Node only
3. **Data directory** — where to store data (default: `~/.earthgrid/data`)
4. **Node name** — a friendly name for your node (auto-generated if skipped)
5. **Auto-update** — pull latest code on start (recommended)
6. **Data sources** — Element84 is always enabled; optionally add CDSE credentials for the full Sentinel archive

After setup completes, EarthGrid **starts automatically** as a systemd user service that survives reboots.

```
✅ EarthGrid configured!
   Node:     my-node
   Storage:  100 GB at /mnt/data/earthgrid
   Beacon:   yes (also coordinator)
   Sources:  CDSE (me@copernicus.eu), Element84 (public)
   Port:     8400

🌐 WebUI: http://localhost:8400/ui

🚀 Starting EarthGrid...
  ✓ EarthGrid running (systemd service)
```

### 5. Verify

```bash
earthgrid status                         # Check node status
curl http://localhost:8400/health         # API health check
```

Open `http://localhost:8400/ui` in your browser to see the WebUI.

### Updating

```bash
earthgrid update                         # Pull latest code + restart
```

Or manually:

```bash
cd ~/EarthGrid && git pull && pip install -e .
earthgrid stop && earthgrid start
```

### Uninstalling

```bash
earthgrid stop
earthgrid uninstall-service
pip uninstall earthgrid
rm -rf ~/EarthGrid ~/.earthgrid
```

---

## Architecture

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

**Source Node** — A node that has credentials to download from official sources (CDSE, WEkEO, Element84, CMEMS). When the network needs data that doesn't exist yet, a source node fetches it. Credentials never leave the node.

### How Data Flows

1. Someone requests EO data for an area (any sensor/provider)
2. Beacon checks which nodes have it
3. If cached → served directly from the nearest node
4. If not cached → a source node fetches it from the appropriate provider, stores it, and serves it
5. Other nodes automatically replicate the new data

### Bootstrap & Discovery

New nodes discover the network via a hardcoded list of bootstrap peers:

1. `earthgrid start` (no config needed)
2. Contacts bootstrap peers → finds the network
3. Registers with a beacon → learns about other nodes via gossip
4. Bootstrap list is only needed for initial discovery — after that, the node is self-sufficient

Custom bootstrap peers can be added via `EARTHGRID_BOOTSTRAP_PEERS` env var or `~/.earthgrid/config.json`.

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
| Run processing (NDVI etc.) | Per-user API key | Only authenticated nodes/users can process |
| Manage source accounts (CDSE etc.) | **CLI only** (no network access) | Provider credentials never leave the node |

### Source Account Credentials (CDSE, WEkEO, etc.)

Source account credentials are:
- Stored **encrypted** on the local node (AES + HMAC)
- Managed **only via CLI** — no API endpoint to read them exists
- **Never transmitted** over the network
- The network only knows: "this node can source data" (boolean flag)

### Node Authentication

Every node generates an **Ed25519 keypair** on first start. When two nodes meet via federation, they exchange signed key requests — no secrets to share, no keys to leak. Replay attacks are blocked by a 5-minute timestamp window.

### Built-in protections

- **Content-addressed storage**: Every chunk verified by SHA-256. Corrupted or fake data is automatically rejected.
- **Rate limiting**: Built-in (120 req/min per IP, burst limit 20/2s).
- **Integrity verification**: `GET /verify/{item_id}` checks all chunks against stored hashes.

---

## Data Sources

EarthGrid can fetch from multiple upstream providers. **All data is stored as Cloud-Optimized GeoTIFF (COG)** regardless of source format.

| Provider | Account needed | Data | Notes |
|---|---|---|---|
| **Element84** (AWS) | ❌ No (always enabled) | S2 L2A, S1 RTC, Landsat C2 L2 | Already COG — fastest ingest |
| **CDSE** (Copernicus) | ✅ Free | S1, S2, S3, S5P, CLMS, full archive | JP2000 → converted to COG on ingest |
| **WEkEO** | 🔜 Coming soon | CLMS (legacy), CMEMS, C3S, CAMS | Climate, marine & atmosphere services |

Element84 is always enabled (no credentials needed). Add CDSE during `earthgrid setup` for access to the full Copernicus archive.

---

## CLI Reference

### Node management

```bash
earthgrid setup                          # Interactive first-time setup
earthgrid start                          # Start node
earthgrid stop                           # Stop node
earthgrid status                         # Show storage usage
earthgrid update                         # Pull latest code + restart
earthgrid resize 100                     # Change storage limit to 100 GB
earthgrid info                           # Show config
earthgrid install-service                # Install systemd service
earthgrid uninstall-service              # Remove systemd service
```

### Data operations

```bash
earthgrid fetch --bbox 12.4,55.6,12.6,55.7   # Fetch available data for area
earthgrid fetch --bbox ... --collection S2     # Filter by collection
earthgrid fetch --bbox ... --start 2026-03-01  # Temporal filter
earthgrid fetch --bbox ... --limit 0           # Fetch ALL available (no limit)
earthgrid sync <peer_url>                      # Pull data from a peer
earthgrid ops                                  # List processing operations
```

### Data source management

```bash
earthgrid sources list                     # List source accounts
earthgrid sources add --provider cdse --username me@copernicus.eu
earthgrid sources remove 1                 # Remove by ID
```

---

## openEO Gateway

EarthGrid includes an openEO-compatible gateway. Missing data is automatically fetched from upstream sources.

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

**curl:**

```bash
curl -X POST http://localhost:8400/openeo/process \
  -H "Content-Type: application/json" \
  -d '{"process_graph": {"load": {"process_id": "load_collection", "arguments": {"id": "sentinel-2-l2a", "spatial_extent": {"west": 12.4, "south": 55.6, "east": 12.6, "north": 55.7}, "temporal_extent": ["2026-03-01", "2026-03-12"], "bands": ["B04", "B08"]}, "result": false}, "ndvi": {"process_id": "ndvi", "arguments": {"data": {"from_node": "load"}, "red": "B04", "nir": "B08"}, "result": false}, "save": {"process_id": "save_result", "arguments": {"data": {"from_node": "ndvi"}, "format": "GTiff"}, "result": true}}}'
```

Processing results are **ephemeral** — computed on-the-fly and returned directly. Only original sensor data is stored in the grid.

---

## API Reference

### Public endpoints (no auth)

| Endpoint | Description |
|---|---|
| `GET /` | Node status, version, coverage stats |
| `GET /health` | Health check |
| `GET /stac/collections` | List STAC collections |
| `GET /stac/search` | STAC spatial/temporal search |
| `GET /chunks/{sha256}` | Download chunk by hash |
| `GET /download/{collection}/{item}` | Reassemble & download item |
| `GET /verify/{item_id}` | Verify chunk integrity |
| `GET /nodes` | List network nodes (beacon) |
| `GET /stats/coverage` | km² per sensor |
| `GET /openeo/collections` | openEO collections |
| `GET /openeo/processes` | openEO supported processes |

### Protected endpoints (API key required)

| Endpoint | Auth | Description |
|---|---|---|
| `POST /ingest` | Write key | Ingest GeoTIFF |
| `POST /process` | Write key | Run processing operation |
| `POST /result` | User key | Execute openEO process graph |
| `GET /credentials/basic` | Basic auth | Validate user, get bearer token |
| `GET /me` | Bearer token | Current user info |

---

## Supported Data

Currently: **Sentinel-2 L2A** and **Sentinel-1 GRD** (COG/GeoTIFF, regular grid).

S1 GRD files use GCPs instead of a geotransform — EarthGrid automatically runs `gdalwarp` during ingest to produce a properly georeferenced raster.

**Not supported yet:** Sentinel-3 (OLCI, SLSTR, SRAL) — swath data with irregular geometry requires a reprojection/gridding step that EarthGrid doesn't have yet.

---

## Dashboard

Live network stats: **[matmatt.github.io/EarthGrid](https://matmatt.github.io/EarthGrid/)**

Shows: Network nodes, km² coverage per sensor, redundancy index, total storage, and anonymous uptake statistics.

---

## Resource Usage

EarthGrid runs at the **lowest possible priority**:

- CPU: `nice -n 19` (lowest priority)
- I/O: `ionice -c 3` (idle class)

Your other workloads always come first.

---

## Data Licensing & Attribution

All data served by EarthGrid originates from official Copernicus and public sources. **The data is free and open**, but usage requires proper attribution.

| Data type | Required attribution |
|---|---|
| Unmodified Sentinel data | *"Copernicus Sentinel data [Year]"* |
| Modified Sentinel data | *"Contains modified Copernicus Sentinel data [Year]"* |
| Copernicus Service Information | *"Copernicus Service information [Year]"* |
| Landsat data | *"Landsat Level-2 data courtesy of USGS"* |

EarthGrid **redistributes** official data as-is (content-addressed, integrity-verified). Users remain subject to the original data provider's licence terms.

> ⚠️ **If you use data from EarthGrid in publications, products or services, you must attribute the original data source as described above.**

---

## Disclaimer

EarthGrid is provided **"as is"**, without warranty of any kind. The authors accept no liability for any loss or consequence arising from the use of this software or data obtained through it.

EarthGrid is an independent, community-driven project. It is not affiliated with ESA, EEA, Copernicus, USGS, or any other data provider.

## License

[EUPL-1.2](LICENSE) — European Union Public Licence.
