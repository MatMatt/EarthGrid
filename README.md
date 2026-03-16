# EarthGrid 🌍

Distributed storage for Earth observation data.

**No single point of failure. No vendor lock-in. Community-driven.**

[![Live Dashboard](https://img.shields.io/badge/dashboard-live-brightgreen)](https://matmatt.github.io/EarthGrid/)
[![Python](https://img.shields.io/badge/python-≥3.9-blue)](https://github.com/MatMatt/EarthGrid)
[![License](https://img.shields.io/badge/license-EUPL--1.2-blue)](LICENSE)
> ⚠️ **Early development** — disruptive changes might occur. Code review and consolidation needed. Planned: UDF support (openEO user-defined functions).

## What is EarthGrid?

A federated network where anyone can run a node, store satellite data, and make it available to others. Think BitTorrent meets STAC for Earth observation.

EarthGrid stores **only official data** from sources like Copernicus (Sentinel) and Landsat. No personal uploads. The network exists as a **public good** for resilient access to Earth observation data.

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

New nodes discover the network via a hardcoded list of bootstrap peers (compiled into the package, like Bitcoin seed nodes):

1. `earthgrid start` (no config needed)
2. Contacts bootstrap peers from the built-in list → finds the network
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

> **Not to be confused with EarthGrid user accounts** (see below). Source accounts are login credentials for upstream data providers like CDSE or WEkEO. They allow your node to download satellite data on behalf of the network.

Source account credentials are:
- Stored **encrypted** on the local node (AES + HMAC)
- Managed **only via CLI** — no API endpoint to read them exists
- **Never transmitted** over the network
- The network only knows: "this node can source data" (boolean flag)

```bash
# These manage data provider logins (CDSE, WEkEO, etc.), NOT EarthGrid user accounts
earthgrid sources add --provider cdse --username me@copernicus.eu
earthgrid sources list
earthgrid sources remove 1
```

### User Authentication (EarthGrid accounts)

> **Not to be confused with source accounts** (above). EarthGrid user accounts control **who can process data** on the network. Source accounts control **where data is downloaded from**.

**Running a node = being authenticated.** When nodes discover each other via federation, they automatically exchange API keys. No manual user creation needed — your node's API key works on all peers in the network.

#### How node authentication works

Every node generates an **Ed25519 keypair** on first start:
- Private key stays on the node (`/data/.node_key`, never transmitted)
- Public key = node identity across the network

When two nodes meet via federation:
1. Node A signs a key exchange request with its private key
2. Node B **verifies the signature** — proves Node A is who it claims to be
3. Node B registers Node A as a user and returns its own signed response
4. Node A verifies and registers Node B

**No secrets to share. No keys to leak.** A fake node cannot forge a valid signature without the private key. Replay attacks are blocked by a 5-minute timestamp window.

```
Node A                            Node B
  |                                 |
  |--- signed(name+key+ts) ------->|
  |                     verify sig  |
  |                  register A  ✅ |
  |<-- signed(name+key+ts) --------|
  | verify sig                      |
  | register B  ✅                  |
```






**How it works:**

- **Node operators**: Authentication is automatic. When your node joins the network via federation, it exchanges Ed25519-signed keys with other nodes. No setup needed — start your node and you can process data on any peer.

### Built-in protections

- **Content-addressed storage**: Every chunk verified by SHA-256. Corrupted or fake data is automatically rejected.
- **Rate limiting**: Built-in (120 req/min per IP, burst limit 20/2s). No nginx config needed.
- **Integrity verification**: `GET /verify/{item_id}` checks all chunks against stored hashes.

---

## Data Sources

EarthGrid can fetch from multiple upstream providers. **All data is stored as Cloud-Optimized GeoTIFF (COG)** regardless of source format — so the data in the grid is identical no matter where it came from.

| Provider | Account needed | Data | Notes |
|---|---|---|---|
| **Element84** (AWS) | ❌ No (built-in) | S2 L2A, S1 RTC, Landsat C2 L2 | Already COG — fastest ingest |
| **CDSE** (Copernicus) | ✅ Free | S1, S2, S3, S5P, CLMS, full archive | JP2000 → converted to COG on ingest |
| **WEkEO** | 🔜 Coming soon | CLMS (legacy), CMEMS, C3S, CAMS | Climate, marine & atmosphere services |
| *More sources* | | | *Coming soon (CMS, CDS..., etc.)* |

### Source setup

Element84 (public access) is included by default — no account needed. During `earthgrid setup`, the installer walks you through additional sources (CDSE, WEkEO, etc.) one by one. Add credentials or skip.

All data is converted to COG on ingest, so the data in the grid is identical regardless of source. More sources = more data available to the network.

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
| **C3S** (Climate) | [Copernicus Climate](https://cds.climate.copernicus.eu/disclaimer) | *"Contains modified Copernicus Climate Change Service information [Year]"* |
| **CAMS** (Atmosphere) | [Copernicus Atmosphere](https://ads.atmosphere.copernicus.eu/disclaimer) | *"Contains modified Copernicus Atmosphere Monitoring Service information [Year]"* |

### Landsat (via Element84)

[USGS Data Policy](https://www.usgs.gov/data-management/data-policies-and-guidance) — free and open, citation recommended: *"Landsat Level-2 data courtesy of USGS"*.

### EarthGrid's role

EarthGrid **redistributes** official data as-is (content-addressed, integrity-verified). It does not claim ownership of any upstream data. Users of data obtained through EarthGrid remain subject to the original data provider's licence terms.

> ⚠️ **If you use data from EarthGrid in publications, products or services, you must attribute the original data source as described above.**

---

## Quick Start

### Docker

```bash
git clone https://github.com/MatMatt/EarthGrid.git
cd EarthGrid
pip install -e .
earthgrid docker start --storage 100 --beacon --name my-node
```

The CLI generates `docker-compose.yml` automatically — no manual config needed. All data paths (`/data/`) are set correctly by default.

```bash
earthgrid docker start     # Build and start container
earthgrid docker stop      # Stop container
earthgrid docker status    # Show container status + config
earthgrid docker logs      # Tail container logs
earthgrid docker restart   # Regenerate compose + restart
earthgrid docker update    # git pull + rebuild + restart
```

**Options:**

| Flag | Description | Default |
|---|---|---|
| `--storage <GB>` | Storage limit | 100 GB |
| `--name <name>` | Node name | from config |
| `--beacon` | Also act as beacon | no |
| `--port <port>` | Port | 8400 |
| `--public-url <url>` | Public URL (for beacon registration) | — |
| `--beacon-url <url>` | Beacon to join | auto-discover |
| `--data-dir <path>` | Host data directory | ~/.earthgrid/data |
| `--no-build` | Skip docker build | — |

Config is stored in `~/.earthgrid/docker-compose.yml`. Subsequent `docker update` reuses the existing config.

### From source *(recommended)*

```bash
git clone https://github.com/MatMatt/EarthGrid.git
cd EarthGrid
pip install -e .
earthgrid setup
earthgrid start
```

Requires Python ≥ 3.9.

### pip *(coming soon)*

```bash
pip install earthgrid
earthgrid setup
earthgrid start
```

> PyPI package not yet published.

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

### Docker management

```bash
earthgrid docker start --storage 100 --beacon   # Build + start container
earthgrid docker stop                            # Stop container
earthgrid docker status                          # Show status + config
earthgrid docker logs                            # Tail logs
earthgrid docker restart                         # Regenerate + restart
earthgrid docker update                          # Pull + rebuild + restart
earthgrid docker exec sources list                 # Run commands inside container
earthgrid docker exec admin show-key             # Show admin API key
```

### Data operations

```bash
earthgrid fetch --bbox 12.4,55.6,12.6,55.7   # Fetch available data for area
earthgrid fetch --bbox ... --collection S2     # Filter by collection
earthgrid fetch --bbox ... --start 2026-03-01  # Temporal filter
earthgrid sync <peer_url>                      # Pull data from a peer
earthgrid ops                                  # List processing operations
```

### Data source management

These commands manage credentials for upstream data providers (CDSE, WEkEO, etc.) — the accounts your node uses to **download satellite data**. This is separate from EarthGrid user accounts (see API endpoints below).

```bash
earthgrid sources list                     # List source accounts (CDSE, WEkEO, etc.)
earthgrid sources add --provider cdse --username me@copernicus.eu
earthgrid sources add --provider wekeo --username me@wekeo.eu
earthgrid sources remove 1                 # Remove by ID
```

**Docker:** Use `earthgrid docker exec` to run these commands inside the container:

```bash
earthgrid docker exec sources list
earthgrid docker exec sources add --provider cdse --username me@copernicus.eu
earthgrid docker exec admin show-key
earthgrid docker exec admin renew-key
```

Supported providers: **Element84** (built-in, no auth needed), **CDSE** (Sentinel, Landsat), **WEkEO** (CLMS, CMEMS, C3S, CAMS). More coming soon.

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

### openEO Client

Works with the official openEO Python and R clients. Authentication required for processing.

```python
# Python (openeo client) — no auth needed on your own node
import openeo
conn = openeo.connect("http://localhost:8400")
cube = conn.load_collection("sentinel-2-l2a",
    spatial_extent={"west": 12.4, "south": 55.6, "east": 12.6, "north": 55.7},
    temporal_extent=["2026-03-01", "2026-03-12"],
    bands=["B04", "B08"])
cube.ndvi(red="B04", nir="B08").save_result("GTiff").download("ndvi.tif")
```

```r
# R (openeo client) — no auth needed on your own node
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
| `GET /stats/uptake` | Anonymous uptake report (requests, GB, km²) |
| `GET /stats/uptake/csv` | Uptake CSV export for reporting |

### Protected endpoints (API key required)

| Endpoint | Auth | Description |
|---|---|---|
| `POST /ingest` | Write key | Ingest GeoTIFF |
| `POST /process` | Write key | Run processing operation |
| `POST /sync-item` | Write key | Trigger item sync |
| `POST /result` | User key | Execute openEO process graph |
| `GET /credentials/basic` | Basic auth | Validate user, get bearer token |
| `GET /me` | Bearer token | Current user info |



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

Shows: Network nodes, km² coverage per sensor, redundancy index, total storage, and anonymous uptake statistics (requests, GB delivered, km² queried).

Auto-updated every 30 seconds. Includes CSV export for reporting.

---

## Resource Usage

EarthGrid runs at the **lowest possible priority**:

- CPU: `nice -n 19` (lowest priority)
- I/O: `ionice -c 3` (idle class)
- Docker: `cpu_shares: 128`, `mem_limit: 2g`

Your other workloads always come first.

---



---



---

## Example: NDVI Time Series for Copenhagen

Extract an NDVI time series at a single point — shows seasonal vegetation dynamics from a full year of Sentinel-2 data.

> All examples connect to any EarthGrid node. Replace `localhost:8400` with your node URL.

<details open>
<summary><b>🐍 Python</b></summary>

```python
import requests
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

BASE = "http://localhost:8400"
LON, LAT = 12.57, 55.68  # Copenhagen — Frederiksberg Gardens

# Search all Sentinel-2 scenes covering this point
items = requests.get(f"{BASE}/stac/search", params={
    "bbox": f"{LON-0.01},{LAT-0.01},{LON+0.01},{LAT+0.01}",
    "collections": "sentinel-2-l2a",
    "limit": 500,
}).json()["features"]

# Group by date (B04 + B08 pairs)
from collections import defaultdict
dates = defaultdict(dict)
for item in items:
    band = item["id"].rsplit("_", 1)[-1]
    base_id = item["id"].rsplit("_", 1)[0]
    dt = item["properties"]["datetime"][:10]
    dates[dt][band] = item

# Extract NDVI at the point for each date
ndvi_ts = []
for dt in sorted(dates):
    if "B04" not in dates[dt] or "B08" not in dates[dt]:
        continue
    try:
        for band, name in [("B04", "red"), ("B08", "nir")]:
            item = dates[dt][band]
            r = requests.get(f"{BASE}/point/{item['collection']}/{item['id']}",
                             params={"lon": LON, "lat": LAT}, timeout=30)
            if r.status_code == 200:
                locals()[name] = r.json()["value"]
        if red and nir and (nir + red) > 0:
            ndvi_ts.append((datetime.strptime(dt, "%Y-%m-%d"), (nir - red) / (nir + red)))
    except Exception:
        continue

# Plot
dates_plot, values = zip(*ndvi_ts)
plt.figure(figsize=(14, 5))
plt.fill_between(dates_plot, values, alpha=0.3, color="#3fb950")
plt.plot(dates_plot, values, "o-", color="#3fb950", markersize=4, linewidth=1.5)
plt.axhline(y=0, color="#555", linewidth=0.5, linestyle="--")
plt.ylabel("NDVI")
plt.title(f"NDVI Time Series — Copenhagen ({LAT}°N, {LON}°E)\nSentinel-2 via EarthGrid")
plt.grid(True, alpha=0.2)
plt.tight_layout()
plt.savefig("ndvi_timeseries.png", dpi=150)
```

</details>

<details>
<summary><b>🐍 Python (openEO)</b></summary>

```python
import openeo

conn = openeo.connect("http://localhost:8400")
cube = conn.load_collection("sentinel-2-l2a",
    spatial_extent={"west": 12.56, "south": 55.67, "east": 12.58, "north": 55.69},
    temporal_extent=["2025-01-01", "2025-12-31"],
    bands=["B04", "B08"])

ndvi = cube.ndvi(red="B04", nir="B08")
# Aggregate to point → time series
ts = ndvi.aggregate_spatial(
    geometries={"type": "Point", "coordinates": [12.57, 55.68]},
    reducer="mean")
ts.download("ndvi_timeseries.json")
```

</details>

<details>
<summary><b>📊 R</b></summary>

```r
library(httr); library(jsonlite); library(ggplot2)

base <- "http://localhost:8400"
lon <- 12.57; lat <- 55.68

# Search all scenes
items <- fromJSON(content(GET(paste0(base, "/stac/search"),
  query = list(bbox = paste(lon-0.01, lat-0.01, lon+0.01, lat+0.01, sep=","),
               collections = "sentinel-2-l2a", limit = 500)
), "text"))$features

# Extract point values per date
ndvi_df <- data.frame(date = as.Date(character()), ndvi = numeric())
dates <- unique(substr(items$properties$datetime, 1, 10))

for (dt in sort(dates)) {
  b04 <- items[grepl("B04", items$id) & grepl(gsub("-","",dt), items$id), ]
  b08 <- items[grepl("B08", items$id) & grepl(gsub("-","",dt), items$id), ]
  if (nrow(b04) == 0 || nrow(b08) == 0) next
  tryCatch({
    red <- fromJSON(content(GET(paste0(base, "/point/", b04$collection[1], "/", b04$id[1]),
                    query = list(lon=lon, lat=lat)), "text"))$value
    nir <- fromJSON(content(GET(paste0(base, "/point/", b08$collection[1], "/", b08$id[1]),
                    query = list(lon=lon, lat=lat)), "text"))$value
    if (!is.null(red) && !is.null(nir) && (nir + red) > 0)
      ndvi_df <- rbind(ndvi_df, data.frame(date=as.Date(dt), ndvi=(nir-red)/(nir+red)))
  }, error = function(e) NULL)
}

ggplot(ndvi_df, aes(date, ndvi)) +
  geom_area(alpha=0.3, fill="#3fb950") +
  geom_point(color="#3fb950", size=2) + geom_line(color="#3fb950") +
  labs(title="NDVI Time Series — Copenhagen", y="NDVI") +
  theme_minimal()
ggsave("ndvi_timeseries.png", width=14, height=5)
```

</details>

<details>
<summary><b>🟨 JavaScript</b></summary>

```javascript
const BASE = "http://localhost:8400";
const [LON, LAT] = [12.57, 55.68];

const resp = await fetch(
  `${BASE}/stac/search?bbox=${LON-0.01},${LAT-0.01},${LON+0.01},${LAT+0.01}&collections=sentinel-2-l2a&limit=500`
);
const items = (await resp.json()).features;

// Group by date
const dates = {};
for (const item of items) {
  const band = item.id.split("_").pop();
  const dt = item.properties.datetime.slice(0, 10);
  if (!dates[dt]) dates[dt] = {};
  dates[dt][band] = item;
}

// Extract NDVI at point
const ndvi = [];
for (const [dt, bands] of Object.entries(dates).sort()) {
  if (!bands.B04 || !bands.B08) continue;
  try {
    const r = await fetch(`${BASE}/point/${bands.B04.collection}/${bands.B04.id}?lon=${LON}&lat=${LAT}`);
    const red = (await r.json()).value;
    const n = await fetch(`${BASE}/point/${bands.B08.collection}/${bands.B08.id}?lon=${LON}&lat=${LAT}`);
    const nir = (await n.json()).value;
    if (nir + red > 0) ndvi.push({ date: dt, ndvi: (nir - red) / (nir + red) });
  } catch {}
}

console.table(ndvi);  // Plot with Chart.js, D3, or export as CSV
```

</details>

<details>
<summary><b>🐚 Shell (curl + jq)</b></summary>

```bash
BASE="http://localhost:8400"
LON=12.57; LAT=55.68

# Search all scenes at point
curl -s "$BASE/stac/search?bbox=$((LON-1))e-2,$((LAT-1))e-2,$((LON+1))e-2,$((LAT+1))e-2&collections=sentinel-2-l2a&limit=100" | \
  jq -r '.features[] | select(.id | contains("B04")) | .id' | while read ID; do
    DT=$(echo "$ID" | grep -oP '\d{8}')
    NIR_ID=$(echo "$ID" | sed 's/B04/B08/')
    RED=$(curl -s "$BASE/point/sentinel-2-l2a/$ID?lon=$LON&lat=$LAT" | jq '.value')
    NIR=$(curl -s "$BASE/point/sentinel-2-l2a/$NIR_ID?lon=$LON&lat=$LAT" | jq '.value')
    NDVI=$(echo "scale=4; ($NIR - $RED) / ($NIR + $RED)" | bc 2>/dev/null)
    echo "$DT,$NDVI"
done > ndvi_timeseries.csv
```

</details>

<details>
<summary><b>🌍 Julia</b></summary>

```julia
using HTTP, JSON3, Plots, Dates

base = "http://localhost:8400"
lon, lat = 12.57, 55.68

# Search
resp = HTTP.get("$base/stac/search", query=Dict(
    "bbox" => "$(lon-0.01),$(lat-0.01),$(lon+0.01),$(lat+0.01)",
    "collections" => "sentinel-2-l2a", "limit" => 500))
items = JSON3.read(resp.body).features

# Group by date
dates = Dict{String, Dict{String, Any}}()
for item in items
    band = split(String(item.id), "_")[end]
    dt = String(item.properties.datetime)[1:10]
    get!(dates, dt, Dict())[band] = item
end

# Extract NDVI
ndvi_dates, ndvi_vals = Date[], Float64[]
for dt in sort(collect(keys(dates)))
    haskey(dates[dt], "B04") && haskey(dates[dt], "B08") || continue
    try
        r = HTTP.get("$base/point/sentinel-2-l2a/$(dates[dt]["B04"].id)?lon=$lon&lat=$lat")
        red = JSON3.read(r.body).value
        n = HTTP.get("$base/point/sentinel-2-l2a/$(dates[dt]["B08"].id)?lon=$lon&lat=$lat")
        nir = JSON3.read(n.body).value
        if nir + red > 0
            push!(ndvi_dates, Date(dt))
            push!(ndvi_vals, (nir - red) / (nir + red))
        end
    catch end
end

plot(ndvi_dates, ndvi_vals, fill=0, alpha=0.3, lw=2, marker=:circle, ms=3,
     title="NDVI Time Series — Copenhagen", ylabel="NDVI",
     color="#3fb950", legend=false, size=(900, 300))
savefig("ndvi_timeseries.png")
```

</details>

> **Tip:** Make sure data is available first. Use `earthgrid fetch --bbox 12.4,55.6,12.7,55.75 --limit 0` to download all available scenes, or let auto-replication fill your node.


## Disclaimer

EarthGrid is provided **"as is"**, without warranty of any kind, express or implied. The authors and contributors accept no liability for any loss, damage, or consequence arising from the use of this software or data obtained through it.

**Data accuracy:** EarthGrid redistributes data from official sources (Copernicus, USGS, etc.) using content-addressed storage with SHA-256 integrity verification. While this ensures bit-level fidelity of stored data, EarthGrid makes no guarantees about the accuracy, completeness, or fitness for purpose of upstream data.

**Not an official service:** EarthGrid is an independent, community-driven project. It is not affiliated with, endorsed by, or operated by the European Space Agency (ESA), the European Environment Agency (EEA), Copernicus, USGS, or any other data provider.

**No guaranteed availability:** Nodes are operated by volunteers. Data availability, network uptime, and transfer speeds are not guaranteed.

**User responsibility:** Users are responsible for complying with the licence terms of the original data providers (see [Data Licensing](#data-licensing--attribution)) and for verifying data suitability for their use case.

## License

[EUPL-1.2](LICENSE) — European Union Public Licence.
