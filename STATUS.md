# EarthGrid Status

> Auto-generated context for contributors (human and AI).
> Last updated: 2026-03-23 | Version: v0.6.1

## Architecture

- **Language**: Rust for the node (HTTP + P2P + storage). Clients may use Python/R **openEO** libraries against the HTTP API only.
- **Framework**: axum (HTTP), libp2p (P2P networking)
- **Storage**: SQLite (rusqlite) — content-addressed chunk store
- **License**: EUPL-1.2
- **Binary size**: ~100MB Docker image
- **Tests**: 148 passing + 1 ignored (`openeo::geoprocess` `gdalwarp` roundtrip when CLI absent)
- **LOC**: ~18k+ (`earthgrid-core/src/`)

## Module Status

| Module | File | Status | Description |
|--------|------|--------|-------------|
| CLI | `main.rs` | ✅ Done | `info`, `verify`, `list`, `ingest`, `serve`, `start`, `fetch` |
| HTTP Server | `server.rs` | ✅ Done | axum-based, all routes mounted |
| Chunk Store | `chunk_store.rs` | ✅ Done | Content-addressed storage, SHA-256 verified |
| Catalog | `catalog.rs` | ✅ Done | STAC-based, collection management, change detection |
| Auth | `auth.rs` | ✅ Done | API key auth for admin/ingest endpoints |
| User Auth | `user_auth.rs` | ✅ Done | Source user management |
| Audit | `audit.rs` | ✅ Done | Action logging |
| Ingest | `ingest.rs` | ✅ Done | GDAL-based, COG support, STAC metadata |
| Reconstruct | `reconstruct.rs` | ✅ Done | Rebuild files from chunks |
| Peers | `peers.rs` | ✅ Done | Peer discovery and management |
| Network | `network.rs` | ✅ Done | libp2p: Kademlia, mDNS, Relay, DCUtR |
| Transport | `transport.rs` | ✅ Done | P2P transport layer |
| Gossip | `network.rs` | ✅ Done | Peer exchange via gossip protocol |
| Federation | `federation.rs` | ✅ Done | Cross-node catalog sync |
| Beacon Federation | `beacon_federation.rs` | ✅ Done | Real-time beacon-to-beacon sync via WebSocket |
| Beacon | `beacon.rs` | ✅ Done | Central registry for node discovery |
| Fetcher | `fetcher.rs` | ✅ Done | STAC fetch from Element84/CDSE, distributed fetch |
| openEO | `openeo/` | ✅ Done | v1.2-style `/result`, `/processes`, `/validate`; `execute_sync`; `aggregate_temporal_period` (NDVI + multiband GTiff); `resample_spatial` via `gdalwarp`; `geoprocess` (labels/reducers); optional full router in `api.rs` |
| Processing | `processing.rs` | ✅ Done | NDVI, NDWI, EVI, cloud_mask, band_math |
| Replication | `replication.rs` | ✅ Done | Auto-sync from peers (every 5 min) |
| Smart Replication | `smart_replication.rs` | ✅ Done | Intelligent replication decisions |
| Stats | `stats.rs` | ✅ Done | Network and node statistics |
| Gamification | `gamification.rs` | ✅ Done | Contributor leaderboard and scoring |
| Bandwidth | `bandwidth.rs` | ✅ Done | Bandwidth measurement and tracking |
| Rate Limiting | `ratelimit.rs` | ✅ Done | API rate limiting |
| Config | `config.rs` | ✅ Done | TOML config (`~/.earthgrid/config.toml`) |
| Node Identity | `node_identity.rs` | ✅ Done | Persistent node ID + generated names |
| Source Users | `source_users.rs` | ✅ Done | Data provider credentials (Element84, CDSE, WEkEO) |
| MGRS | `mgrs.rs` | ✅ Done | Military Grid Reference System tile handling |
| Client | `client.rs` | ✅ Done | HTTP client for node-to-node communication |
| Error | `error.rs` | ✅ Done | Unified error types |
| Tray App | (feature flag) | ✅ Done | Optional system tray icon (🌍/🌑) |

### Routes (`src/routes/`)

| Route | File | Description |
|-------|------|-------------|
| Admin | `admin.rs` | Collection delete, node rename, config |
| Chunks | `chunks.rs` | Chunk upload/download |
| Federation | `federation.rs` | Catalog exchange between nodes |
| Gamification | `gamification_routes.rs` | Leaderboard API |
| Ingest | `ingest_routes.rs` | Fetch + ingest triggers |
| Processing | `process.rs` | openEO-style processing requests |
| STAC | `stac.rs` | STAC API (items, collections, search) |
| Stats | `stats.rs` | Network statistics |
| Misc | `misc.rs` | Health, info, openEO `/processes` `/validate` `/result`, static files |

## Network Status

| Node | Role | Items | Status |
|------|------|-------|--------|
| node-alpha | Beacon + Node | — | Online |
| (example) | Node | — | Online |

## Infrastructure

- **Beacon**: `<your-domain>/earthgrid` (port 8400, Nucleus)
- **Dashboard**: GitHub Pages + live beacon API
- **CI/CD**: GitHub Actions — Linux (.tar.gz + .deb), macOS arm64, Docker (ghcr.io)
- **Docker**: `ghcr.io/matmatt/earthgrid-core:latest`

## Data Sources

- **Element84** (`earth-search.aws.element84.com/v1`): Sentinel-2 L2A (primary)
- **CDSE** (`catalogue.dataspace.copernicus.eu`): Sentinel-1/2/3, S5P (OData)
- **Landsat**: via Element84 `landsat-c2-l2` collection

## Recent Changes (v0.6.x)

- openEO: `openeo/` module tree (`graph`, `execute`, `output`, `geoprocess`, `catalogue`, `api`); temporal aggregation + spatial resample; auto-fetch missing bands unchanged
- README: Python/R openEO examples aligned with real data windows and client quirks (`fetch_metadata=False` for Python; `con` passed into R `processes` / `compute_result`)
- Distributed fetch: beacon delegates items across nodes by free storage
- Catalog change detection (`catalog_version` + `/catalog/changes` endpoint)
- Coverage table redesign (Grid Level → Tiles → Files)
- openEO auto-fetch: missing bands downloaded on demand
- Beacon federation via WebSocket
- Test count: 148 passing + 1 ignored (`gdalwarp` integration)

## Known Issues

- Windows build fails (GDAL `gdal_i.lib` linking)
- `admin_key` field in `GridNode` unused (suppressed with `#[allow(dead_code)]`)
- Collection documents for openEO/STAC are minimal (band dimension missing): Python client needs `fetch_metadata=False` for fluent `load_collection` until metadata is enriched
- `resample_spatial` requires **`gdalwarp` on PATH** (not bundled with the Rust GDAL crate)

## Targets / Roadmap

### Short-term
- [ ] Spatial-aware job splitting (Phase 3 federation — beacon assigns bbox per node)
- [ ] Processing baseline tracking (detect upstream reprocessing)
- [ ] Hardware info in heartbeat (cpu_cores, ram_total_gb)
- [ ] Windows GDAL CI fix

### Medium-term
- [ ] DHT-only mode (no beacon required, >1000 nodes)
- [ ] Sentinel-1 ingest pipeline
- [ ] Multi-beacon federation at scale
- [ ] v1.0 release

### Long-term
- [ ] Public good network: developing countries can run local nodes
- [ ] Plugin system for custom processing
- [ ] Mobile-friendly dashboard
