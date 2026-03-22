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
- Each chunk exists on **N nodes** (replication factor, default N=3)
- NOT every node has everything — but everything is reachable
- When a node needs a chunk, it fetches from the nearest peer that has it
- If no peer has it → download from source (CDSE, Element84, etc.)

### Replication Strategy
| Category | Replication Factor | Trigger |
|---|---|---|
| Hot data (frequently requested) | 4–6 | Auto-promote based on access stats |
| Default | 3 | Standard for all new ingests |
| Cold data (rarely accessed) | 2 | Auto-demote after inactivity period |
| Minimum | 2 | Never below 2 (single-node-failure safe) |

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

## Current State (v0.5.0)

**Implemented:**
- Content-addressed chunk storage (SHA-256, two-level dirs)
- STAC catalog with spatial/temporal search (OGC API Features compliant)
- Peer federation (register, sync, key exchange, item sync)
- Beacon mode for node discovery
- Two-tier API key authentication + user auth
- Full Rust implementation (single binary, axum HTTP server)
- openEO Gateway (process graph parsing + execution)
- Auto-ingest pipeline (fetch from CDSE/Element84 → chunk → propagate)
- Statistics engine (access tracking, ingest history, uptake, coverage)
- Bandwidth control and rate limiting
- Smart replication (hot/cold data management)
- Gamification system (leaderboard, achievements, challenges, economy)
- Processing operations (NDVI, NDWI, NDSI, EVI, cloud mask, band math, true color)
- Point extraction and chunk-map endpoints
- Admin API (user management, collection management, node management)
- GitHub Pages dashboard with auto-updated stats

**Planned:**
- [ ] Replication factor auto-promote/demote based on access patterns
- [ ] Mutual TLS for inter-node communication
- [ ] Sentinel-1 GRD support (auto-warp during ingest)
- [ ] Additional data sources (CMEMS, C3S/CDS)

## Tech Stack

- **Runtime:** Rust (single binary, ~21 MB)
- **HTTP:** axum (async, tokio-based)
- **Storage:** Content-addressed filesystem (SHA-256)
- **Metadata:** STAC catalog (SQLite-backed)
- **Container:** Docker (alpine-based)
- **License:** EUPL-1.2

## Migration: Python → Rust

**Decision (2026-03-18):** Rust replaces Python completely. Single binary, no Python runtime.

### Roadmap

**Phase 1 — Current:** Python is the running node. Rust core exists as parallel implementation with matching APIs.

**Phase 2 — Feature parity:** Build out remaining Rust functionality:
- [ ] openEO Gateway (process graph parsing + execution)
- [ ] Source User management (encrypted credentials)
- [ ] Auto-ingest pipeline
- [ ] CLI (clap-based, replacing Python Click)
- [ ] Docker image (single static binary, alpine-based)
- [ ] GDAL bindings (via gdal-rs crate)

**Phase 3 — Switch:** Deploy Rust binary alongside Python on Nucleus, compare, then decommission Python.

**Phase 4 — Cleanup:** Remove `earthgrid/` Python directory. Single codebase in `earthgrid-core/`.

### Why

- One static binary, zero dependencies for users
- Better performance (chunks, networking, concurrency)
- Simpler deployment (no venv, no pip, no Python version issues)
- Infrastructure software should be self-contained
