# Running a Beacon Node

A beacon is a lightweight coordinator that helps other nodes find each other. It stores no data — only a registry of nodes and peer beacons.

## What a Beacon Does

- Maintains a registry of data nodes (who's online, what they have)
- Routes discovery queries (where is Sentinel-2 data for Denmark?)
- Federates with other beacons — shares node registries across the network
- Provides bootstrap endpoints for new nodes joining the network

## Requirements

1. **EarthGrid installed** (Docker or from source)
2. **Port reachable from the internet** (default: 8400)
3. That's it. No special hardware, no extra software.

### Making the port reachable

Depending on your setup:

| Setup | What to do |
|---|---|
| **VPS / cloud server** | Open port in firewall: `ufw allow 8400` |
| **Home server behind router** | Port forwarding: router admin → forward external 8400 → local IP:8400 |
| **Tailscale / WireGuard** | Nothing — peers on the same mesh can reach you directly |
| **Cloudflare Tunnel / ngrok** | Set up a tunnel and use the tunnel URL as `EARTHGRID_PUBLIC_URL` |

> **Tip:** A small VPS (2 vCPU, 2–4 GB RAM, ~€4/month) is ideal for beacons. Always online, fixed IP, no router configuration needed. EU providers: [Hetzner](https://hetzner.com), [Netcup](https://netcup.de), [OVH](https://ovh.com), [Contabo](https://contabo.com).

## Setup

### Option 1: Docker (recommended)

```bash
earthgrid docker start --beacon --name my-beacon
```

### Option 2: Environment variable

```bash
export EARTHGRID_ALSO_BEACON=true
earthgrid start
```

### Option 3: Config file

In `~/.earthgrid/.env`:
```
EARTHGRID_ALSO_BEACON=true
```

## Public URL (auto-detected)

When a beacon starts without `EARTHGRID_PUBLIC_URL` set, EarthGrid automatically detects your public IP address (via ifconfig.me, ipify.org, or icanhazip.com) and constructs the URL:

```
http://<detected-ip>:8400
```

You'll see this in the logs:
```
INFO earthgrid.config: Auto-detected public URL: http://203.0.113.42:8400
```

### Manual override

If you have a domain name or non-standard port, set the URL explicitly:

```bash
export EARTHGRID_PUBLIC_URL=https://my-beacon.example.com
```

## Verifying your Beacon

After starting, check that your beacon is reachable from outside:

```bash
# From another machine:
curl http://<your-public-ip>:8400/health
# Should return: {"status": "ok"}

curl http://<your-public-ip>:8400/node-info
# Should show node info + beacon status
```

## Federation

Beacons can federate with each other — sharing their node registries so the whole network stays connected even if individual beacons go down.

```bash
# Add a peer beacon
export EARTHGRID_BEACON_PEERS=http://other-beacon.example.com:8400

# Or add multiple (comma-separated)
export EARTHGRID_BEACON_PEERS=http://beacon1.example.com:8400,http://beacon2.example.com:8400
```

Federation syncs happen automatically in the background.

## Beacon + Data Node

A beacon can also store data — it's both coordinator and participant:

```bash
earthgrid docker start --beacon --storage 100 --name my-hybrid-node
```

This is the default setup: `EARTHGRID_ALSO_BEACON=true` runs the beacon alongside the data node in the same process.

## Resource Usage

A pure beacon (no data storage) is very lightweight:

| Resource | Usage |
|---|---|
| CPU | Minimal (handles registration + heartbeats) |
| RAM | ~50–100 MB |
| Disk | <1 MB (SQLite registry) |
| Bandwidth | Low (metadata only, no data chunks) |

## Beacon Endpoints

| Endpoint | Description |
|---|---|
| `POST /register` | Data node registers itself |
| `POST /heartbeat` | Periodic node heartbeat |
| `GET /nodes` | List all registered nodes |
| `GET /seed/nodes` | Bootstrap seed list for new nodes |
| `POST /beacon/sync` | Federate with peer beacons |
| `GET /peers.json` | Gossip-friendly peer list |
