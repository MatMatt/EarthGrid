# EarthGrid — Distributed Testing Strategy

## The Problem
How do you test a distributed system when you only have 2-3 nodes?

## Phase 1: Local Network (now)
- Node Alpha (Nucleus) + Node Beta (Peaq) on same LAN
- Tests: federation sync, search fan-out, chunk replication
- Limitation: same network, no real latency/partition testing

## Phase 2: Seed Node + External Testers
The key insight: **make it dead simple for anyone to join with test data.**

### Seed Endpoint
Every node exposes a seed endpoint:

```
GET /seed/bundle
→ Returns a manifest of available seed datasets with sizes

POST /seed/pull?source=http://other-node:8400
→ Pull a copy of another node's catalog + chunks
→ Node-to-node replication over HTTP
→ No CDSE account needed, no credentials, no setup
```

### How a tester joins:
```bash
# 1. Start a node (anywhere in the world)
docker run -p 8400:8400 earthgrid/node

# 2. Pull test data from a seed node
curl -X POST "http://localhost:8400/seed/pull?source=http://mattiuzzi.zapto.org:8400"

# 3. Done — you have data + you're federated
```

### Test Dataset Bundle
A curated ~2 GB package containing:
- Sentinel-2 L2A: 3-5 tiles, different regions (Alps, Copenhagen, Amazon)
- Multiple dates per tile (temporal search testing)
- Multiple bands (B02/B03/B04 @ 10m, TCI @ 60m)
- Pre-computed NDVI derived product (tests derived product chain)

## Phase 3: Internet-Scale Testing
- Public seed node at mattiuzzi.zapto.org:8400
- GitHub README: "Run one command to join the network"
- CI/CD: automated federation tests with GitHub Actions + cloud VMs
- Geographic diversity: ask colleagues (Antonio in Malmö? EEA contacts?)

## What We Test
1. **Federation discovery** — nodes find each other
2. **Catalog sync** — new node gets catalog from peers
3. **Federated search** — query spans multiple nodes
4. **Chunk replication** — data copies between nodes
5. **Node failure** — take one offline, data still accessible from others
6. **Latency** — search performance across WAN
7. **Concurrent ingest** — multiple nodes ingesting simultaneously

## Metrics to Track
- Search latency (local vs federated)
- Chunk transfer speed (LAN vs WAN)
- Catalog sync time
- Recovery time after node failure
