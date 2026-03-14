# EarthGrid Storage Strategy

## Design Principles

1. **Every chunk should exist on ≥2 nodes** (redundancy target)
2. **Downloads from sources should be parallelized across nodes** (swarm fetch)
3. **Chunks are band-level tiles** for maximum reuse and partial downloads
4. **Hot data replicates more** — popular chunks spread automatically
5. **Content-addressed (SHA-256)** — integrity by design, dedup free

---

## Chunk Architecture

### Current: Tile = all bands × 512×512
```
One chunk = bands × 512 × 512 pixels → big, inflexible
```

### New: Band-Level Tiling
```
One chunk = 1 band × 512 × 512 pixels → small, composable
```

**Why?**
- NDVI needs only B04 + B08 → download 2 chunks, not 13
- Different bands have different popularity → hot bands replicate more
- Better dedup across overlapping products
- Aligns with COG internal tile structure

### Chunk Hierarchy
```
Item (S2 tile)
 └── Band (B04)
      └── Chunk (row=2, col=3, sha=abc...)
```

**Chunk metadata** (stored in catalog):
```json
{
  "sha256": "abc123...",
  "band": "B04",
  "row": 2,
  "col": 3,
  "width": 512,
  "height": 512,
  "dtype": "uint16",
  "size_bytes": 524288
}
```

---

## Swarm Fetch — Parallel Source Download

When the network needs data that no node has:

### Current: Single source node
```
Request → Beacon → Source Node A → CDSE → downloads all → serves
```

### New: Swarm fetch across multiple source nodes
```
Request → Beacon finds 3 source nodes with CDSE/Element84 creds

Source Node A: fetches bands B02, B03, B04, B08
Source Node B: fetches bands B05, B06, B07, B8A  
Source Node C: fetches bands B01, B09, B11, B12

All 3 download in parallel → chunks stored → replicated across network
```

### How it works:

1. **Beacon** receives a fetch request for data not in the grid
2. Beacon identifies all **source nodes** that can serve this provider
3. Beacon **assigns band-ranges** to each source node (round-robin or by capacity)
4. Each source node downloads its assigned bands, chunks them, stores locally
5. Source nodes **announce** new chunks to the beacon
6. Other nodes pull the chunks they want (proximity-based)

### Band Assignment Strategy:
```python
def assign_bands(source_nodes, bands):
    """Distribute bands across source nodes by capacity."""
    # Sort nodes by: available bandwidth > proximity to source > storage free
    nodes = sorted(source_nodes, key=lambda n: n.score, reverse=True)
    assignments = {n.id: [] for n in nodes}
    for i, band in enumerate(bands):
        target = nodes[i % len(nodes)]
        assignments[target.id].append(band)
    return assignments
```

---

## Smart Replication

### Replication Targets

| Redundancy Level | When | Min Copies |
|---|---|---|
| **Critical** | Data exists on only 1 node | Replicate immediately to 2+ |
| **Normal** | Data on 2-3 nodes | No action needed |
| **Hot** | Data requested >10x/day | Spread to more nodes (up to 5) |
| **Cold** | Not requested in 30 days | Can be evicted if storage full |

### Push vs Pull

**Current:** Pull only (node asks peer for data)
**New:** Push + Pull hybrid

- **Push:** When a source node ingests new data, it proactively pushes chunks to nearby nodes
- **Pull:** Nodes periodically check peers for new data they're interested in
- **On-demand:** When a request comes for data that's far away, replicate to the requesting node's region

### Eviction Policy (when storage full)

Priority to keep (highest first):
1. Chunks with **redundancy = 1** (we're the only copy — NEVER evict)
2. Chunks accessed in **last 7 days** (hot)
3. Chunks with **low redundancy** (2 copies)
4. Chunks by **age** (newest first)

Evict: Cold chunks with redundancy ≥ 3 (safe to remove, other nodes have copies)

---

## Parallel Download for Clients

When a client downloads an item, it can fetch chunks from **multiple nodes simultaneously**:

```
Client requests item S2_T32UMF_20260314

Beacon responds with chunk map:
  B04_r0c0 → [node-A, node-C]     ← available on 2 nodes
  B04_r0c1 → [node-A, node-B]
  B04_r1c0 → [node-B, node-C]
  B08_r0c0 → [node-A]             ← only 1 copy → high priority for replication

Client downloads from all nodes in parallel:
  From node-A: B04_r0c0, B04_r0c1
  From node-B: B04_r1c0
  From node-C: (backup if A or B slow)
```

### Smart Routing
- Beacon returns chunks sorted by **proximity** (nearest node first)
- Client uses **parallel HTTP range requests** across nodes
- If one node is slow → shift remaining chunks to faster nodes
- Integrity verified via SHA-256 on every chunk

---

## Network-Level Dedup

Since chunks are content-addressed (SHA-256):
- Two nodes ingesting the **same tile** from different sources (CDSE vs Element84) → identical chunks after COG conversion
- Beacon's registry knows which nodes have which chunks → zero redundant transfers
- Chunks that are identical across products (e.g., same area, same cloud mask) → stored once

---

## Implementation Priority

### Phase 1: Band-level chunking (breaking change)
- [ ] Refactor `ingest.py`: one chunk per band per tile
- [ ] Update chunk metadata in catalog
- [ ] Update `reconstruct.py` for band-level reassembly
- [ ] Migration tool for existing chunks

### Phase 2: Swarm fetch
- [ ] Beacon: `/fetch-plan` endpoint — returns band assignments for source nodes  
- [ ] Source nodes: accept band-specific fetch tasks
- [ ] Parallel execution across source nodes

### Phase 3: Smart replication
- [ ] Redundancy tracking per chunk in beacon registry
- [ ] Push replication after ingest
- [ ] Eviction policy with redundancy awareness
- [ ] Hot chunk detection + auto-spread

### Phase 4: Parallel client download
- [ ] `/chunk-map/{item_id}` endpoint — returns chunks + node locations
- [ ] Client library: parallel download from multiple nodes
- [ ] Adaptive routing (slow node → switch)
