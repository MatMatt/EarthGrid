# EarthGrid 🌍

Distributed storage and access for satellite imagery and Earth observation products.

## What is this?

EarthGrid is a federated network where anyone can run a node, store satellite data, and make it accessible to others. Think BitTorrent meets STAC for Earth observation data.

**No single point of failure. No vendor lock-in. Community-driven.**

## Why?

- Centralized platforms (CDSE, AWS, Google) = single points of failure + vendor lock-in
- Petabytes of EO data locked behind complex APIs and registrations
- Developing countries can't afford cloud storage but need local access
- Erasure coding provides efficient redundancy at ~1.4x storage instead of 3x

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

## Install

```bash
# Beacon only (runs everywhere — Windows, Mac, Linux, no GDAL needed)
pip install earthgrid

# Full install with geospatial ingest (needs GDAL/rasterio)
pip install earthgrid[geo]

# Or from source
git clone https://github.com/MatMatt/EarthGrid.git
cd EarthGrid
pip install -e ".[geo]"
```

## Quick Start

### Run a data node

```bash
earthgrid node --port 8400
# → http://localhost:8400/
```

### Run a beacon (coordinator)

```bash
earthgrid beacon --port 8400
# → Nodes register here, queries get routed
```

### Connect node to beacon

```bash
earthgrid node --port 8401 --beacon http://beacon.example.org:8400
# → Auto-registers + heartbeat every 60s
```

### Federate beacons

```bash
earthgrid beacon --port 8400 --beacon-peers http://other-beacon:8400
# → Beacons sync node registries every 2 min
```

### Ingest data (needs `earthgrid[geo]`)

```bash
curl -X POST "http://localhost:8400/ingest?collection=sentinel-2&item_id=my_scene" \
  -F "file=@scene.tif"
```

### Docker

```bash
cd docker
docker compose up -d
# → Node running on port 8400
```

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /` | Node/beacon info + stats |
| `GET /health` | Health check |
| `GET /stac/search?collections=...&bbox=...` | STAC search |
| `GET /stac/collections` | List collections |
| `POST /ingest` | Ingest GeoTIFF (node only) |
| `GET /chunks/{sha}` | Download chunk by hash |
| `GET /search` | Routed search (beacon only) |
| `GET /nodes` | List registered nodes (beacon only) |
| `POST /beacon/peer?url=...` | Add peer beacon |

## EarthGrid stores ONLY official data

No personal uploads. No private files. The network exists to provide resilient,
distributed access to official Earth observation data (Copernicus, Sentinel, Landsat, etc.)
as a **public good**.

## License

[EUPL-1.2](LICENSE) — European Union Public Licence. A European license for a European project.
