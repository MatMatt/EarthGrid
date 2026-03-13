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

## Current State (v0.2.0)

**Implemented:**
- Content-addressed chunk storage (SHA-256, two-level dirs)
- STAC catalog with spatial/temporal search
- Basic peer federation (register, sync)
- Beacon mode for node discovery
- Two-tier API key authentication
- Rust core library (21/21 tests passing)

**Planned:**
- [ ] openEO Gateway (process graph parsing + execution)
- [ ] Source User management (encrypted credential pool)
- [ ] Auto-ingest pipeline (download → chunk → propagate)
- [ ] Statistics engine (access tracking, replication decisions)
- [ ] Bandwidth nice levels
- [ ] Replication factor management (auto-promote/demote)
- [ ] Rust HTTP server (replacing Python prototype)

## Tech Stack

- **Current:** Python (FastAPI), Rust (core library)
- **Storage:** Content-addressed filesystem (SHA-256)
- **Metadata:** STAC catalog (JSON)
- **Target:** Full Rust implementation
- **Container:** Docker
- **License:** EUPL-1.2
