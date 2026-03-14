# EarthGrid 🌍

Distributed storage for Earth observation data.

**No single point of failure. No vendor lock-in. Community-driven.**

[![Live Dashboard](https://img.shields.io/badge/dashboard-live-brightgreen)](https://matmatt.github.io/EarthGrid/)
[![Python](https://img.shields.io/badge/python-≥3.9-blue)](https://pypi.org/project/earthgrid/)
[![License](https://img.shields.io/badge/license-EUPL--1.2-blue)](LICENSE)

## What is EarthGrid?

A federated network where anyone can run a node, store satellite data, and make it available to others. Think BitTorrent meets STAC for Earth observation.

EarthGrid stores **only official data** from sources like Copernicus, Sentinel, and Landsat. No personal uploads. The network exists as a **public good** for resilient access to Earth observation data.

## Why?

- Centralized platforms (CDSE, AWS, Google) = single points of failure + vendor lock-in
- Petabytes of EO data locked behind complex APIs and registrations
- Developing countries can't afford cloud storage but need local access
- Content-addressed storage provides integrity guarantees by design

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

**Beacon** — Lightweight coordinator. Maintains a registry of nodes, routes queries, federates with other beacons. Stores no data.

**Node** — Stores and serves data chunks. Every chunk is identified by its SHA-256 hash (content-addressed). Nodes auto-sync data between each other.

**Source Node** — A node that has credentials to download from official sources (CDSE, WEkEO, Element84, CMEMS). When the network needs data that doesn't exist yet, a source node fetches it. Credentials never leave the node. Each node can have accounts for multiple providers.

### How Data Flows

1. Someone requests EO data for Copenhagen (any sensor/provider)
2. Beacon checks which nodes have it
3. If cached → served directly from the nearest node
4. If not cached → a source node fetches it from the appropriate provider (CDSE, WEkEO, etc.), stores it, and serves it
5. Other nodes automatically replicate the new data

### Bootstrap & Discovery

New nodes discover the network via a seed list hosted on GitHub Pages:

1. `earthgrid start` (no config needed)
2. Fetches `peers.json` from GitHub Pages → finds beacon URL
3. Registers with beacon → learns about other nodes via gossip
4. GitHub is only needed for initial discovery — after that, the node is self-sufficient

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
| Run processing (NDVI etc.) | API key | Prevents CPU abuse |
| Manage credentials | **CLI only** (no network access) | Credentials never leave the node |

### Credentials are local

Source user credentials (CDSE login etc.) are:
- Stored **encrypted** on the local node
- Managed **only via CLI** — no API endpoint exists
- **Never transmitted** over the network
- The network only knows: "this node can source data" (boolean flag)

```bash
earthgrid users add --name MyAccount --username me@copernicus.eu
earthgrid users list
earthgrid users remove 1
```

### Built-in protections

- **Content-addressed storage**: Every chunk verified by SHA-256. Corrupted or fake data is automatically rejected.
- **Rate limiting**: Built-in (120 req/min per IP, burst limit 20/2s). No nginx config needed.
- **Integrity verification**: `GET /verify/{item_id}` checks all chunks against stored hashes.

---

## Data Sources

EarthGrid can fetch from multiple upstream providers. **All data is stored as Cloud-Optimized GeoTIFF (COG)** regardless of source format — so the data in the grid is identical no matter where it came from.

| Provider | Account needed | Data | Notes |
|---|---|---|---|
| **Element84** (AWS) | ❌ No | S2 L2A, S1 RTC, Landsat C2 L2 | Already COG — fastest ingest |
| **CDSE** (Copernicus) | ✅ Free | S1, S2, S3, S5P, full archive | JP2000 → converted to COG on ingest |
| **WEkEO** | ✅ Free | CLMS, C3S, CAMS | Climate & land services |
| **CMEMS** | ✅ Free | Marine data | Ocean & marine products |

### Which source should I use?

- **Just want to contribute?** → Element84. No account, instant start.
- **Need the full Sentinel archive?** → Add CDSE (free registration).
- **Both?** → Recommended. More sources = more data available to the network.

Since EarthGrid converts everything to COG, data from Element84 and CDSE is **byte-identical in the grid**. Element84 is simply faster to ingest because the source is already COG (no JP2000→COG conversion step).

---

## Data Licensing & Attribution

All data served by EarthGrid originates from official Copernicus and public sources. **The data is free and open**, but usage requires proper attribution.

### Copernicus Sentinel Data

Free, full and open access under [EU Regulation 1159/2013](https://sentinels.copernicus.eu/documents/247904/690755/Sentinel_Data_Legal_Notice). You may reproduce, distribute, adapt and combine the data freely — but **you must cite the source**:

| Data type | Required attribution |
|---|---|
| Unmodified Sentinel data | *"Copernicus Sentinel data [Year]"* |
| Modified Sentinel data | *"Contains modified Copernicus Sentinel data [Year]"* |
| Copernicus Service Information | *"Copernicus Service information [Year]"* |
| Modified Service Information | *"Contains modified Copernicus Service information [Year]"* |

### Copernicus Services (CLMS, CMEMS, C3S, CAMS)

Each Copernicus Service has its own licence. Common requirements:

| Service | Licence | Citation |
|---|---|---|
| **CLMS** (Land) | [Copernicus Land](https://land.copernicus.eu/en/data-policy) | *"© Copernicus Land Monitoring Service [Year], EEA"* |
| **CMEMS** (Marine) | [Copernicus Marine](https://marine.copernicus.eu/user-corner/service-commitments-and-licence) | Product-specific DOI (see product page) |
| **C3S** (Climate) | [Copernicus Climate](https://cds.climate.copernicus.eu/disclaimer) | *"Contains modified Copernicus Climate Change Service information [Year]"* |
| **CAMS** (Atmosphere) | [Copernicus Atmosphere](https://ads.atmosphere.copernicus.eu/disclaimer) | *"Contains modified Copernicus Atmosphere Monitoring Service information [Year]"* |

### Landsat (via Element84)

[USGS Data Policy](https://www.usgs.gov/data-management/data-policies-and-guidance) — free and open, citation recommended: *"Landsat Level-2 data courtesy of USGS"*.

### EarthGrid's role

EarthGrid **redistributes** official data as-is (content-addressed, integrity-verified). It does not claim ownership of any upstream data. Users of data obtained through EarthGrid remain subject to the original data provider's licence terms.

> ⚠️ **If you use data from EarthGrid in publications, products or services, you must attribute the original data source as described above.**

---

## Quick Start

### Docker *(coming soon)*

```bash
docker run -d --name earthgrid \
  -v ./earthgrid-data:/data \
  -p 8400:8400 \
  matmatt/earthgrid
```

> ⚠️ Docker Hub image not yet published. For now, build from source (see below) or use `pip install`.

No Python needed. No dependencies. Just Docker.

### pip *(coming soon)*

```bash
pip install earthgrid
earthgrid setup
earthgrid start
```

> ⚠️ PyPI package not yet published. For now, install from source (see below).

Requires Python ≥ 3.9. The node auto-discovers the network via GitHub Pages seeds.

### From source

```bash
git clone https://github.com/MatMatt/EarthGrid.git
cd EarthGrid
pip install -e .
earthgrid setup
earthgrid start
```

---

## CLI Reference

### Node management

```bash
earthgrid setup                          # Interactive first-time setup
earthgrid start                          # Start node (auto-discovers network)
earthgrid start --also-beacon            # Also act as beacon
earthgrid start --beacon <url>           # Join specific beacon
earthgrid status                         # Show storage usage
earthgrid resize 100                     # Change storage limit to 100 GB
earthgrid info                           # Show config
```

### Data operations

```bash
earthgrid fetch --bbox 12.4,55.6,12.6,55.7   # Fetch available data for area
earthgrid fetch --bbox ... --collection S2     # Filter by collection
earthgrid fetch --bbox ... --start 2026-03-01  # Temporal filter
earthgrid sync <peer_url>                      # Pull data from a peer
earthgrid ops                                  # List processing operations
```

### Credential management (local only)

```bash
earthgrid users list                     # List all source accounts
earthgrid users add --provider cdse --username me@copernicus.eu
earthgrid users add --provider wekeo --username me@wekeo.eu
earthgrid users add --provider element84  # No auth needed (public)
earthgrid users remove 1                 # Remove by ID
```

Supported providers: **CDSE** (Sentinel, Landsat), **WEkEO** (CLMS, C3S, CAMS), **Element84** (public S2/S1/Landsat mirror), **CMEMS** (marine data).

---

## openEO Gateway

EarthGrid includes an openEO-compatible gateway. Missing data is automatically fetched from upstream sources.

### Direct API *(works now)*

**Python:**
```python
import requests

r = requests.post("http://localhost:8400/openeo/process", json={
    "process_graph": {
        "load": {
            "process_id": "load_collection",
            "arguments": {
                "id": "sentinel-2-l2a",
                "spatial_extent": {"west": 12.4, "south": 55.6, "east": 12.6, "north": 55.7},
                "temporal_extent": ["2026-03-01", "2026-03-12"],
                "bands": ["B04", "B08"]
            }, "result": False
        },
        "ndvi": {
            "process_id": "ndvi",
            "arguments": {"data": {"from_node": "load"}, "red": "B04", "nir": "B08"},
            "result": False
        },
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "ndvi"}, "format": "GTiff"},
            "result": True
        }
    }
})
```

**curl:**
```bash
curl -X POST http://localhost:8400/openeo/process \
  -H "Content-Type: application/json" \
  -d '{"process_graph": {"load": {"process_id": "load_collection", "arguments": {"id": "sentinel-2-l2a", "spatial_extent": {"west": 12.4, "south": 55.6, "east": 12.6, "north": 55.7}, "temporal_extent": ["2026-03-01", "2026-03-12"], "bands": ["B04", "B08"]}, "result": false}, "ndvi": {"process_id": "ndvi", "arguments": {"data": {"from_node": "load"}, "red": "B04", "nir": "B08"}, "result": false}, "save": {"process_id": "save_result", "arguments": {"data": {"from_node": "ndvi"}, "format": "GTiff"}, "result": true}}}'
```

> These examples assume EarthGrid is running locally on port 8400.
> Replace `localhost` with your node's address if accessing remotely.

### openEO Client *(coming soon)*

Full compatibility with the official openEO Python and R clients is in progress.

```python
# Python (openeo client) — coming soon
import openeo
conn = openeo.connect("http://localhost:8400")
cube = conn.load_collection("sentinel-2-l2a",
    spatial_extent={"west": 12.4, "south": 55.6, "east": 12.6, "north": 55.7},
    temporal_extent=["2026-03-01", "2026-03-12"],
    bands=["B04", "B08"])
cube.ndvi(red="B04", nir="B08").download("ndvi.tif")
```

```r
# R (openeo client) — coming soon
library(openeo)
conn <- connect("http://localhost:8400")
p <- processes()
cube <- p\$load_collection("sentinel-2-l2a",
    spatial_extent = list(west=12.4, south=55.6, east=12.6, north=55.7),
    temporal_extent = c("2026-03-01", "2026-03-12"),
    bands = c("B04", "B08"))
result <- p\$ndvi(cube, red="B04", nir="B08")
compute_result(result, "ndvi.tif")
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
| `GET /stats/requests` | km² queried |
| `GET /process/operations` | List available operations |
| `GET /openeo/collections` | openEO collections |
| `GET /openeo/processes` | openEO supported processes |

### Protected endpoints (API key required)

| Endpoint | Auth | Description |
|---|---|---|
| `POST /ingest` | Write key | Ingest GeoTIFF |
| `POST /process` | Write key | Run processing operation |
| `POST /sync-item` | Write key | Trigger item sync |
| `POST /openeo/process` | Write key | Execute openEO process graph |

### Beacon endpoints

| Endpoint | Description |
|---|---|
| `POST /register` | Register node with beacon |
| `POST /heartbeat` | Node heartbeat |
| `GET /seed/nodes` | Bootstrap seed list |
| `POST /beacon/sync` | Federate with other beacons |

---

## Dashboard

Live network stats: **[matmatt.github.io/EarthGrid](https://matmatt.github.io/EarthGrid/)**

Shows: Network nodes, km² coverage per sensor, redundancy index, km² queried, total storage.

Auto-updated every 30 seconds. Seed list (`peers.json`) updated every 10 minutes from beacon.

---

## Resource Usage

EarthGrid runs at the **lowest possible priority**:

- CPU: `nice -n 19` (lowest priority)
- I/O: `ionice -c 3` (idle class)
- Docker: `cpu_shares: 128`, `mem_limit: 2g`

Your other workloads always come first.

---

## License

[EUPL-1.2](LICENSE) — European Union Public Licence.
