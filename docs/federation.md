# Federation Protocol

EarthGrid uses a two-layer federation model: **beacon federation** (real-time registry sync between beacons) and **data federation** (fan-out STAC search across data nodes).

## Overview

```
┌──────────┐  WebSocket   ┌──────────┐  WebSocket   ┌──────────┐
│ Beacon A │◄────────────►│ Beacon B │◄────────────►│ Beacon C │
│ (nodes   │  push events │ (nodes   │  push events │ (nodes   │
│  1,2,3)  │              │  4,5)    │              │  6,7)    │
└──────────┘              └──────────┘              └──────────┘
     ▲                         ▲                         ▲
     │ register/heartbeat      │                         │
   Nodes                    Nodes                     Nodes
```

- Each **node** registers with one beacon and sends heartbeats to that beacon only.
- **Beacons** federate among themselves — a node registered on Beacon A becomes visible on Beacon B and C within milliseconds.
- **Data queries** fan out to all known nodes in parallel, merge results, deduplicate by item ID.

## Beacon Federation (Registry Sync)

### Wire Protocol

Beacons communicate over **WebSocket** at the `/beacon/ws` endpoint. Messages are JSON-encoded `BeaconEvent` objects with a `type` discriminator.

### Connection Sequence

```
Beacon A                              Beacon B
   │                                     │
   │──── WS CONNECT /beacon/ws ─────────►│
   │                                     │
   │◄─── full_sync (B's nodes) ─────────│
   │                                     │
   │──── full_sync (A's nodes) ─────────►│
   │                                     │
   │          (bidirectional push)        │
   │◄─── node_register ────────────────►│
   │◄─── node_heartbeat ───────────────►│
   │◄─── node_pruned ─────────────────►│
   │                                     │
```

1. **Connect**: Beacon A opens a WebSocket to `ws://<beacon-b>:8400/beacon/ws`
2. **Full sync**: Both sides immediately send their entire node registry as a `full_sync` event
3. **Push events**: After sync, any local changes are pushed in real-time

### Event Types

All events include a `beacon_origin` (the beacon where the event originated) and `ts` (Unix timestamp).

#### `node_register`

A new node joined the network.

```json
{
  "type": "node_register",
  "beacon_origin": "beacon-abc123",
  "ts": 1711180800.0,
  "node": {
    "node_id": "n-xyz789",
    "node_name": "node-alpha",
    "url": "http://203.0.113.42:8400",
    "collections": ["sentinel-2-l2a", "sentinel-1-grd"],
    "item_count": 1500,
    "chunk_count": 42000,
    "chunks_bytes": 85000000000,
    "can_source": true,
    "storage_limit_gb": 500.0,
    "last_seen": 1711180800.0,
    "uptime_seconds": 86400,
    "alive": true
  }
}
```

#### `node_heartbeat`

Periodic stats update from an existing node.

```json
{
  "type": "node_heartbeat",
  "beacon_origin": "beacon-abc123",
  "ts": 1711180860.0,
  "node": { "..." }
}
```

#### `node_pruned`

A stale node was removed (missed too many heartbeats).

```json
{
  "type": "node_pruned",
  "beacon_origin": "beacon-abc123",
  "ts": 1711181000.0,
  "node_id": "n-deadbeef"
}
```

#### `full_sync`

Complete node registry snapshot, sent on initial connection.

```json
{
  "type": "full_sync",
  "beacon_origin": "beacon-abc123",
  "ts": 1711180800.0,
  "nodes": [ { "..." }, { "..." } ]
}
```

### Loop Prevention

Events carry a `beacon_origin` field identifying which beacon generated the event. When Beacon B receives an event from Beacon A:

1. **Apply locally** — upsert/remove the node in B's registry
2. **Do NOT re-broadcast** — the event is not forwarded to Beacon C

This means federation is **one-hop only**: each beacon must be directly connected to every other beacon it wants to sync with. This prevents infinite loops and keeps the protocol simple.

### Conflict Resolution

When a beacon receives a node update that already exists locally:

- Compare `last_seen` timestamps
- **Newer wins** — update only if incoming `last_seen > existing.last_seen`
- Same or older → silently discard

Federated upserts bypass the URL-conflict check (a node may legitimately re-register with the same URL after restart).

### Reconnection

If a WebSocket connection drops, the client side reconnects with **exponential backoff**:

| Attempt | Delay |
|---|---|
| 1 | 1 second |
| 2 | 2 seconds |
| 3 | 4 seconds |
| ... | doubles each time |
| max | 60 seconds (cap) |

On successful reconnect, a full sync is exchanged again to catch up on missed events.

## Data Federation (Federated Search)

Fan-out STAC search across all known peer nodes.

### Query Parameters

```json
{
  "collections": ["sentinel-2-l2a"],
  "bbox": [12.0, 55.0, 13.0, 56.0],
  "datetime": "2024-01-01T00:00:00Z/2024-12-31T23:59:59Z",
  "limit": 100
}
```

### Search Flow

```
Client
  │
  ▼
Gateway / Node
  │
  ├──► GET peer1/stac/search?collections=...&bbox=...
  ├──► GET peer2/stac/search?collections=...&bbox=...
  └──► GET peer3/stac/search?collections=...&bbox=...
        (parallel, with per-peer timeout)
  │
  ▼
Merge + Deduplicate (by STAC item ID)
  │
  ▼
Return ≤ limit results
```

- All peers are queried in **parallel** (tokio tasks)
- Results are **deduplicated** by STAC item ID (same chunk hash = same data)
- Returned items are capped at `limit` (default: 100)
- Individual peer failures are silently ignored (partial results are fine)

## Configuration Reference

| Environment Variable | Description | Default |
|---|---|---|
| `EARTHGRID_ALSO_BEACON` | Enable beacon mode on a data node | `false` |
| `EARTHGRID_BEACON_URL` | URL of the beacon to register with (for data nodes) | *(empty)* |
| `EARTHGRID_BEACON_PEERS` | Comma-separated list of peer beacon URLs for federation | *(empty)* |
| `EARTHGRID_PUBLIC_URL` | This node's publicly reachable URL (auto-detected if unset) | auto |

### Example: Two Federated Beacons

**Beacon A** (`beacon-eu.example.com`):
```bash
EARTHGRID_ALSO_BEACON=true
EARTHGRID_BEACON_PEERS=http://beacon-us.example.com:8400
```

**Beacon B** (`beacon-us.example.com`):
```bash
EARTHGRID_ALSO_BEACON=true
EARTHGRID_BEACON_PEERS=http://beacon-eu.example.com:8400
```

Both beacons will connect to each other. The protocol handles bidirectional connections gracefully — if both sides connect simultaneously, both WebSocket pairs operate independently.

## Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/beacon/ws` | WebSocket | Federation sync (beacon-to-beacon) |
| `/beacon/register` | POST | Node self-registration |
| `/beacon/heartbeat` | POST | Node heartbeat |
| `/beacon/nodes` | GET | List all registered nodes |
| `/beacon/nodes/{id}` | GET | Get specific node |
| `/beacon/nodes/{id}` | DELETE | Remove a node |
| `/beacon/metrics` | GET | Grid metrics time series |
| `/seed/nodes` | GET | Bootstrap seed list for new nodes |
| `/peers.json` | GET | Gossip-friendly peer list |
| `/stac/search` | GET | STAC search (local + federated) |

## Design Decisions

- **WebSocket over HTTP polling**: Real-time propagation (ms latency vs polling intervals). Lower overhead for frequent heartbeats.
- **One-hop federation**: Simpler than gossip protocols. Each beacon connects directly to all peers it wants. Scales to dozens of beacons easily.
- **Timestamp-wins conflict resolution**: No vector clocks needed. Monotonic wall-clock timestamps are sufficient for node registry data.
- **Nodes don't know about federation**: A node talks to one beacon. The federation layer is transparent. This keeps node implementation simple.
