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
pip install earthgrid
earthgrid setup
```

The setup wizard asks everything: storage, beacon, ingest capability.
Works on **Windows, Mac, Linux** — no prerequisites.

```
🌍 EarthGrid Setup

Storage:  [100] GB
Beacon:   [Y/n]
Ingest:   Will you ingest GeoTIFF data? [y/N]

✅ Ready! Start: earthgrid start
```

### From source

```bash
git clone https://github.com/MatMatt/EarthGrid.git
cd EarthGrid
pip install -e .
earthgrid setup
```

### Docker

```bash
cd docker && docker compose up -d
```

## Usage

```bash
earthgrid start                          # start with setup config
earthgrid start --also-beacon            # also coordinate other nodes
earthgrid start --beacon http://b:8400   # join existing network
earthgrid info                           # show config
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
