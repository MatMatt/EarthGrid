# EarthGrid 🌍

Distributed storage and access for satellite imagery and derived Earth observation products.

## What is this?

EarthGrid is a federated network where anyone can run a node, store satellite data, and make it accessible to others. Think BitTorrent meets STAC for Earth observation data.

**No single point of failure. No vendor lock-in. Community-driven.**

## Why?

- Centralized platforms (CDSE, AWS, Google) = single points of failure + vendor lock-in
- Petabytes of EO data locked behind complex APIs and registrations
- Derived products only from official channels — no community contributions
- Developing countries can't afford cloud storage but need local access

## How?

1. **Content-addressed chunks** — Every data tile has a hash. Same data = same hash.
2. **Federated STAC catalogs** — Each node publishes what it has. Queries span the network.
3. **Organic replication** — Popular data naturally lives on more nodes.
4. **Standard formats** — COG, Zarr, STAC. No proprietary anything.

## Status

🚧 **Early development** — MVP in progress.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the design.

## Quick Start

```bash
# Clone
git clone https://github.com/MatMatt/earthgrid.git
cd earthgrid

# Run
docker compose up -d

# Check
curl http://localhost:8400/
```

## License

Apache 2.0
