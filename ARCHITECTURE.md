# EarthGrid Architecture

## Overview

EarthGrid is a distributed, self-filling geospatial data grid. It stores Earth observation data as content-addressed chunks across a federation of nodes, providing intelligent redundancy and on-demand data acquisition.

**License:** EUPL-1.2

## Core Concepts

### Content-Addressed Storage
- All data is split into chunks, each identified by its SHA-256 hash
- Two-level directory structure: `ab/cd/abcd1234...`
- Chunks are immutable — same data always produces the same hash
- Deduplication is automatic: identical data is stored only once

### Intelligent Redundancy (not Full Replication)
- Each chunk exists on **N nodes** (replication factor, dynamically computed)
- NOT every node has everything — but everything is reachable
- When a node needs a chunk, it fetches from the nearest peer that has it
- If no peer has it → download from source (CDSE, Element84, etc.)

### Dynamic Replication Strategy
The replication factor adapts automatically based on network conditions:

**Inputs:**
- Total nodes alive in the grid
- Total free storage across all nodes
- Per-item access frequency (from stats engine)
- Network health (node churn rate, uptime streaks)

**Rules:**
| Category | Replication Factor | Trigger |
|---|---|---|
| Hot data (frequently requested) | min(nodes, 4–6) | Access count > threshold in 7d window |
| Default | max(2, min(3, alive_nodes - 1)) | Standard for all new ingests |
| Cold data (rarely accessed) | max(2, grid_factor) | No access for 30+ days |
| Minimum | 2 | Never below 2 (single-node-failure safe) |
| Network < 3 nodes | 1 | Degrade gracefully, warn operator |

**Auto-tuning loop (beacon):**
1. Every 5 minutes, beacon computes 
2. For each item: compare current copies vs. target factor
3. Under-replicated → assign replication task to node with most free space
4. Over-replicated → mark lowest-uptime copy as eviction candidate
5. Hot promotion: items with >10 requests/week get +1 factor
6. Cold demotion: items with 0 requests/30d get -1 factor (min 2)

The goal: **maximize resilience within available storage**, not blindly replicate everything.

## Architecture Components

### 1. openEO Gateway

The primary user interface. Users submit openEO process graphs to EarthGrid.

```
User → openEO Process Graph → EarthGrid Gateway
```

**Flow:**
1. Parse process graph → identify required collections, spatial extent, temporal range
2. Resolve to chunks: which chunks are needed?
3. Check availability:
   - **Local?** → Use directly
   - **On peer node?** → Fetch from nearest peer
   - **Nowhere in grid?** → Download via Source Users
4. Execute openEO process graph on assembled data
5. Return result to user

The gateway translates openEO's standardized API into EarthGrid's chunk-based operations.

### 2. Source Users (Data Providers)

A pool of Copernicus/CDSE accounts contributed by users for downloading data that isn't yet in the grid.

**Account Management:**
- Encrypted credential storage (never plaintext)
- Round-robin or least-recently-used selection
- Per-account rate limiting (respect CDSE quotas)
- Health monitoring: detect expired/blocked accounts
- Automatic failover to next available account

**Supported Sources:**
- CDSE (Copernicus Data Space Ecosystem) — Sentinel-1, -2, -3, -5P
- Element84 Earth Search (public COGs on AWS) — no auth needed
- CMEMS (marine data)
- C3S/CDS (climate data)

### 3. Auto-Ingest Pipeline

When data is downloaded from a source, it enters the standard ingest pipeline:

```
Download → Validate → Chunk (SHA-256) → Store locally → Propagate to N-1 peers
```

- Downloaded data is treated identically to manually ingested data
- STAC metadata is preserved and indexed
- Chunks propagate asynchronously to peer nodes
- Propagation targets selected by: geographic proximity, available storage, current load

### 4. Statistics & Monitoring

Tracks all data access patterns to drive replication and caching decisions.

**Metrics collected:**
- Per-collection request count (daily/weekly/monthly)
- Per-chunk access frequency
- Per-node storage utilization
- Per-source-user download volume
- Bandwidth consumption per node

**Dashboard provides:**
- Most requested datasets (drives replication promotion)
- Least accessed data (candidates for replication demotion)
- Source user utilization and health
- Network-wide storage distribution
- Chunk availability map (which nodes have what)

### 5. Bandwidth Control (Nice Level)

Priority-based bandwidth allocation, inspired by Unix `nice`.

| Nice Level | Priority | Use Case |
|---|---|---|
| -10 | Highest | User-facing openEO requests (real-time) |
| 0 | Normal | Standard data propagation |
| 10 | Low | Background replication balancing |
| 19 | Lowest | Pre-fetching, speculative caching |

**Controls:**
- Max bandwidth per download stream
- Max concurrent downloads per source user
- Time-based scheduling: full bandwidth off-peak, throttled during peak hours
- Per-node bandwidth caps (respect upstream limits)

### 6. Supported Data Formats

**Supported:**
- **Sentinel-2 L2A** — COG/GeoTIFF, regular grid, direct ingest
- **Sentinel-1 GRD** — GeoTIFF with GCPs (SAR). Auto-warped via `gdalwarp` during ingest
  to produce a properly georeferenced raster (EPSG:4326, LZW compressed, tiled)

**Not supported (yet):**
- **Sentinel-3** (OLCI, SLSTR, SRAL) — swath-based netCDF with irregular geometry.
  Incompatible with EarthGrid's regular-grid chunk system. Would break spatial queries
  (BBOX), prevent mosaicking, and produce overlapping chunks. Requires a
  reprojection/gridding preprocessing step that EarthGrid doesn't currently have.

## Data Flow

```
                    ┌─────────────┐
                    │   User      │
                    │ (openEO)    │
                    └──────┬──────┘
                           │ Process Graph
                           ▼
                    ┌─────────────┐
                    │   Gateway   │
                    │  (openEO)   │
                    └──────┬──────┘
                           │ Which chunks needed?
                           ▼
                    ┌─────────────┐
                    │  Chunk      │  Local? ──────→ Use
                    │  Resolver   │  Peer?  ──────→ Fetch from peer
                    └──────┬──────┘  Missing? ────→ Download
                           │
                           ▼
              ┌────────────────────────┐
              │   Source User Pool     │
              │  (CDSE accounts)       │
              │  Round-robin + quotas  │
              └────────────┬───────────┘
                           │ Download
                           ▼
              ┌────────────────────────┐
              │   Auto-Ingest          │
              │  Chunk → Store → Push  │
              │  to N-1 peer nodes     │
              └────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │   Stats Engine         │
              │  Track access patterns │
              │  Drive replication     │
              └────────────────────────┘
```

## Node Types

| Type | Role | Storage | Compute |
|---|---|---|---|
| **Full Node** | Store + process + serve | Yes | Yes |
| **Beacon** | Discovery + routing only | Minimal (metadata) | No |
| **Source Node** | Provides download credentials | No (or cache) | No |

## Security

### API Authentication
- **Two-tier keys:** `EARTHGRID_API_KEY` (read/write), `EARTHGRID_ADMIN_KEY` (destructive ops)
- Source user credentials: encrypted at rest, never exposed via API
- Inter-node communication: mutual TLS (planned)

### Data Integrity
- SHA-256 verification on every chunk transfer
- Corrupt chunks automatically re-fetched from peers or source

## Current State (v0.6.1)

Single Rust binary. Python prototype has been fully replaced.

**Implemented:**
- Content-addressed chunk storage (SHA-256, two-level dirs)
- STAC catalog with spatial/temporal search
- Axum HTTP server with full REST API
- openEO v1.2 Gateway (process graph parsing + execution)
- Spectral indices: NDVI, NDWI, EVI, cloud mask
- Source User management (encrypted credential pool, round-robin)
- Auto-ingest pipeline (download → chunk → propagate)
- Statistics engine (access tracking, bandwidth, downloads)
- Bandwidth control with Unix-style nice levels
- Smart replication (access-driven promote/demote)
- Peer federation (register, sync, gossip, federated search)
- Beacon mode for node discovery
- Beacon federation (real-time WebSocket sync between beacons)
- libp2p networking (Kademlia DHT, mDNS, relay, NAT traversal)
- Two-tier API key authentication
- Gamification (leaderboards, achievements, challenges)
- CLI (clap-based: ingest, serve, start/stop, fetch, setup)
- Docker image (single binary, Ubuntu + GDAL)
- GDAL bindings for GeoTIFF/COG ingest and SAR warping

**Planned:**
- [ ] Mutual TLS for inter-node authentication
- [ ] Replication factor auto-tuning based on network-wide stats
- [ ] openEO User Defined Functions (UDF) support
- [ ] Sentinel-1 GRD/SLC ingest and processing
- [ ] Landsat Collection 2 ingest pipeline
- [ ] Sentinel-3 support (requires swath-to-grid preprocessing)
- [ ] CLMS data products (where processable via openEO)
- [ ] Additional Sentinel missions (S5P atmospheric, S6 altimetry)

## Tech Stack

- **Language:** Rust (edition 2021)
- **HTTP:** Axum 0.8, Tokio
- **P2P:** libp2p 0.54
- **Storage:** Content-addressed filesystem (SHA-256), SQLite (rusqlite)
- **Geospatial:** GDAL 0.19
- **Container:** Docker
- **License:** EUPL-1.2
