# EarthGrid 🌍

Distributed storage and processing for Earth observation data.

**No single point of failure. No vendor lock-in. Community-driven.**

[![Live Dashboard](https://img.shields.io/badge/dashboard-live-brightgreen)](https://matmatt.github.io/EarthGrid/)

## What is this?

EarthGrid is a federated network where anyone can run a node, store satellite data, and make it accessible to others. Think BitTorrent meets STAC for Earth observation data.

## Why?

- Centralized platforms (CDSE, AWS, Google) = single points of failure + vendor lock-in
- Petabytes of EO data locked behind complex APIs and registrations
- Developing countries can't afford cloud storage but need local access
- Erasure coding provides efficient redundancy at ~1.4x storage instead of 3x

## Key Features

| Feature | Description |
|---|---|
| 🧩 **Content-Addressed** | Every chunk identified by SHA-256 hash. Tamper-proof — corrupted or fake data is automatically rejected |
| 🔄 **Auto-Sync** | New data automatically propagates to all registered nodes |
| 🐢 **Idle Resources** | Runs at lowest CPU/IO priority (`nice 19`, `ionice idle`). Never competes with other workloads |
| 🛰️ **STAC Native** | Built-in STAC catalog. Every item is discoverable and interoperable |
| 🧮 **Processing** | NDVI, NDWI, EVI, cloud masking, band math — process data where it lives |
| 🌐 **Federation** | Beacons coordinate, data flows peer-to-peer. Works across firewalls via HTTPS |
| 🔒 **Integrity** | SHA-256 verification on every chunk transfer. `/verify/{item_id}` for explicit checks |

## Architecture

**Semi-decentralized:** Beacon nodes coordinate, data flows peer-to-peer.

```
Beacon A ←→ Beacon B ←→ Beacon C       (coordination layer — federated)
  ↑            ↑            ↑
Node 1,2     Node 3,4     Node 5        (data layer — chunks + STAC catalog)
```

- **Beacons** = lightweight coordinators (no data, just routing). Anyone can run one.
- **Nodes** = store and serve data chunks. Content-addressed (SHA-256).
- **Federation** = beacons sync node registries, queries span the entire network.

See [ARCHITECTURE.md](ARCHITECTURE.md) for details.

## Quick Start

### Join the network

```bash
pip install earthgrid
earthgrid setup
earthgrid start --beacon https://mattiuzzi.zapto.org/earthgrid
```

The setup wizard asks for storage size and configuration. Works on **Windows, Mac, Linux**.

### From source

```bash
git clone https://github.com/MatMatt/EarthGrid.git
cd EarthGrid
pip install -e .
earthgrid setup
earthgrid start --beacon https://mattiuzzi.zapto.org/earthgrid
```

### Docker

```bash
cd docker && docker compose up -d
```

Add beacon URL in `docker-compose.yml`:
```yaml
environment:
  - EARTHGRID_BEACON=https://mattiuzzi.zapto.org/earthgrid
```

## Usage

```bash
earthgrid start                          # start with setup config
earthgrid start --also-beacon            # also coordinate other nodes
earthgrid start --beacon <url>           # join existing network
earthgrid sync <peer_url>               # pull data from a peer
earthgrid info                           # show config
earthgrid status                         # show storage usage
```

## Processing

Built-in operations — process data where it lives:

```bash
# NDVI from Sentinel-2 bands
curl -X POST "http://localhost:8400/process?items=B04,B08&operation=ndvi"

# Available operations
curl http://localhost:8400/process/operations
```

| Operation | Description |
|---|---|
| `ndvi` | Normalized Difference Vegetation Index |
| `ndwi` | Normalized Difference Water Index |
| `ndsi` | Normalized Difference Snow Index |
| `evi` | Enhanced Vegetation Index |
| `cloud_mask` | Cloud mask from SCL band |
| `true_color` | RGB composite |
| `band_math` | Custom expressions |

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /` | Node/beacon info + stats |
| `GET /health` | Health check |
| `GET /nodes` | List registered nodes (beacon) |
| `GET /stac/collections` | List STAC collections |
| `GET /stac/search` | STAC search |
| `POST /ingest` | Ingest GeoTIFF (multipart upload) |
| `GET /chunks/{sha256}` | Download chunk by hash |
| `POST /sync-item` | Auto-sync trigger from beacon |
| `GET /verify/{item_id}` | Verify chunk integrity |
| `GET /download/{collection}/{item}` | Reassemble & download item |
| `POST /process` | Run processing operation |
| `GET /process/operations` | List available operations |
| `POST /register` | Register node with beacon |
| `POST /heartbeat` | Node heartbeat to beacon |

## Data Integrity

All data is **content-addressed** (SHA-256). Every chunk's hash is verified on:
- Initial ingest
- Peer-to-peer transfer (auto-sync)
- Manual verification (`/verify/{item_id}`)

Tampered chunks are **automatically rejected**. No fake data enters the pool.

## Resource Usage

EarthGrid runs at the **lowest possible priority**:
- CPU: `nice -n 19` (lowest priority)
- I/O: `ionice -c 3` (idle class — only uses disk when nothing else needs it)
- Docker: `cpu_shares: 128`, `mem_limit: 2g`

Your other workloads always come first.

## EarthGrid stores ONLY official data

No personal uploads. No private files. The network exists to provide resilient, distributed access to official Earth observation data (Copernicus, Sentinel, Landsat, etc.) as a **public good**.

## Dashboard

Live network stats: **[matmatt.github.io/EarthGrid](https://matmatt.github.io/EarthGrid/)**

## License

[EUPL-1.2](LICENSE) — European Union Public Licence. A European license for a European project.
