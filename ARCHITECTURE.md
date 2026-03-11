# EarthGrid — Architecture

## Vision
A distributed, federated system for storing and accessing satellite imagery and derived Earth observation products. No single point of failure. No vendor lock-in. Community-driven.

## Principles
1. **Easy to deploy** — One command to spin up a node. `docker run earthgrid` and you're part of the network. No complex setup, no infrastructure expertise needed. A Raspberry Pi, a laptop, a VPS — anything works.
2. **Distributed redundant access** — Every dataset lives on multiple nodes. If one goes down, others serve it. No single point of failure. The network self-heals.
3. **Content-addressed** — Every chunk has a hash. Same data = same hash, everywhere.
4. **STAC-native** — Discovery via federated STAC catalogs. No proprietary metadata.
5. **Chunk-level** — Atomic unit = Zarr chunk or COG tile, not whole scenes.
6. **Organic replication** — Popular data lives on more nodes automatically.
7. **Open participation** — Anyone can run a node, contribute storage, serve data.

## Three Layers

### 1. Storage Layer (ChunkStore)
- Content-Addressable Storage (CAS): SHA-256 hash → chunk
- Supports: Zarr chunks, COG tiles, NetCDF slices
- Local filesystem backend (extensible to S3, etc.)
- Chunk manifest: maps logical dataset → list of chunk hashes

#### Storage Modes (per node)

Every node operates with two storage pools:

**Active Storage** — "I keep what I need"
- Data the node operator explicitly wants (their region, their mission, their products)
- Full control: ingest, delete, manage
- Example: A university in Brazil keeps Sentinel-2 Amazon tiles

**Guardian Storage** — "I keep to protect the network"
- Data assigned by the network to ensure minimum redundancy
- Node pledges a portion of its disk (configurable, e.g. 20%)
- Network distributes chunks that are under-replicated (fewer than N copies exist)
- Node cannot cherry-pick guardian data — it accepts what the network needs
- Guardian chunks are lower priority for eviction (only removed if truly full)
- Incentive: you guard others' data, others guard yours

#### Replication Policy
- Every chunk has a **replication target** (default: 3 copies across the network)
- Network continuously monitors replication count per chunk
- When a node goes offline → its chunks become under-replicated → guardian assignments shift
- Popular data naturally exceeds target (many nodes want it)
- Rare/niche data relies on guardian storage to stay above minimum
- Critical datasets (e.g. climate records) can have higher targets (5+)

```
/store/
  active/     → operator-chosen data
    ab/cd/abcd1234...sha256
  guardian/   → network-assigned redundancy
    ef/01/ef012345...sha256
```

### 2. Catalog Layer (Federation)
- Each node runs a STAC-compatible API
- Node registry: nodes announce themselves + their catalog summary
- Federation query: "find Sentinel-2 L2A, bbox, time" → asks all known nodes
- Results ranked by proximity / availability / freshness
- Derived products carry provenance chain (source datasets + processing)

```
Node A (Vienna)     → HR-S&I Alps, Sentinel-2 Central Europe
Node B (Copenhagen) → Coastal Zones, Baltic Sea SST
Node C (São Paulo)  → Sentinel-2 Amazon, deforestation products
```

### 3. Processing Layer
- **Compute where the data lives** — no need to download terabytes first
- openEO-compatible process graphs (band math, indices, aggregation)
- Built-in common operations: NDVI, NDWI, NDSI, cloud masking, compositing
- Custom UDFs (User Defined Functions) in Python
- Results = new derived products → published back into the network
- Chain processing: Node A computes → result stored → Node B refines
- GPU-accelerated where available (PyTorch, ONNX for ML inference)

### 4. Visualization Layer
- **Every node can render** — no separate map server needed
- Dynamic tile server: XYZ/TMS tiles generated on-the-fly from stored chunks
- WMS/WMTS endpoints for GIS client compatibility (QGIS, ArcGIS)
- Color ramps and band combinations configurable per collection
- Time-series animation: temporal slider across available dates
- Lightweight web viewer built into the node UI
- OGC API Tiles + OGC API Maps for standards compliance

## MVP Scope (v0.1)

### What we build first:
1. **EarthGrid-node** — Single Python/FastAPI service
   - ChunkStore (local filesystem, SHA-256)
   - STAC API (stac-fastapi or custom lightweight)
   - Ingest endpoint: upload COG/Zarr → chunk + catalog
   - Download endpoint: get chunks by hash or STAC query
   - Node info endpoint: what do I have, how much space

2. **Federation** — Simple HTTP-based
   - Node registry (hardcoded peers for MVP, DHT later)
   - Peer sync: exchange catalog summaries
   - Federated search: fan-out query to known peers

3. **Seed data**
   - HR-S&I (Snow & Ice) for Alps region
   - Small enough to fit on Nucleus + Peaq
   - Downloaded from CDSE, chunked, ingested

4. **Two nodes**
   - Nucleus (Vienna/Hvidovre) — primary
   - Peaq or VPS — secondary, tests federation

### What we skip for now:
- Incentive/token system
- Authentication/authorization
- Large-scale replication strategies

## Tech Stack
- **Language**: Python 3.11+
- **Framework**: FastAPI
- **Storage**: Local filesystem (CAS layout)
- **Catalog**: SQLite (per-node), STAC-compatible
- **Formats**: COG (rasterio), Zarr (zarr-python)
- **Hashing**: SHA-256 (hashlib)
- **Federation**: HTTP/REST (httpx async)
- **Container**: Docker
- **CI**: GitHub Actions

## Directory Structure
```
EarthGrid/
├── EarthGrid/
│   ├── __init__.py
│   ├── main.py           # FastAPI app
│   ├── chunk_store.py    # CAS storage
│   ├── catalog.py        # STAC catalog + SQLite
│   ├── ingest.py         # COG/Zarr → chunks
│   ├── federation.py     # peer discovery + sync
│   └── config.py         # node configuration
├── tests/
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── scripts/
│   └── seed_hrsi.py      # download + ingest seed data
├── ARCHITECTURE.md
├── README.md
├── pyproject.toml
└── .env.example
```

## API Endpoints (MVP)

### Local Node
- `GET /` — Node info (id, name, capacity, chunk count)
- `GET /health` — Health check
- `POST /ingest` — Upload file → chunk + catalog
- `GET /chunks/{hash}` — Download chunk by hash
- `GET /stac/collections` — STAC collections
- `GET /stac/search` — STAC item search (bbox, datetime, collection)

### Federation
- `GET /peers` — List known peers
- `POST /peers` — Register a peer
- `GET /federation/search` — Federated STAC search across peers
- `GET /federation/sync` — Exchange catalog summaries with peers

## Data Flow

```
1. INGEST
   COG/Zarr file → split into chunks → SHA-256 each
   → store in CAS → create STAC item → update catalog

2. QUERY (local)
   STAC search → match items in SQLite → return chunk hashes
   → client downloads chunks → reassembles

3. QUERY (federated)
   STAC search → fan out to all peers → collect results
   → rank by proximity → return merged results
   → client downloads from best source

4. REPLICATE (future)
   Node A has popular dataset → Node B requests chunks
   → Node B stores locally → now served from 2 locations
```
