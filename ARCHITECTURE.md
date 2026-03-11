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
- Each node stores what it wants/can
- Chunk manifest: maps logical dataset → list of chunk hashes

```
/store/
  ab/cd/abcd1234...sha256  → raw bytes (zarr chunk)
  ef/01/ef012345...sha256  → raw bytes (COG tile)
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

### 3. Compute Layer (future)
- openEO-compatible processing
- "Compute where the data lives"
- Results = new derived products → published back to the network
- Phase 2 — not in MVP

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
- Compute layer
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
