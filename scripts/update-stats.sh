#!/bin/bash
# Snapshot EarthGrid stats to docs/stats.json for GitHub Pages dashboard.
# Run via cron: */10 * * * * /home/matteo/EarthGrid/scripts/update-stats.sh

set -euo pipefail

BEACON="http://localhost:8400"
REPO="/home/matteo/EarthGrid"
STATS_FILE="$REPO/docs/stats.json"

python3 -c "
import json, sys, urllib.request
from datetime import datetime, timezone

BEACON = 'http://localhost:8400'

def fetch(path):
    try:
        with urllib.request.urlopen(BEACON + path, timeout=10) as r:
            return json.loads(r.read())
    except:
        return {}

root = fetch('/')
eg = root.get('earthgrid', {})
nodes_data = fetch('/nodes')
coverage = fetch('/stats/coverage')
uptake = fetch('/stats/uptake?period_days=365')
ingest = fetch('/stats/ingest?period_days=365')
requests_data = fetch('/stats/requests')

nodes = nodes_data.get('nodes', [])
alive = [n for n in nodes if n.get('alive')]

# Compute totals - fallback to root/earthgrid if /nodes not available
total_bytes = sum(n.get('chunks_bytes', 0) for n in nodes) or eg.get('chunks_bytes', 0)
total_avail_gb = sum(n.get('storage_limit_gb', 0) for n in nodes)
nodes_total = len(nodes) if nodes else (1 if eg.get('item_count', 0) > 0 else 0)
nodes_alive = len(alive) if nodes else nodes_total

try:
    rep = fetch('/replication/health')
    redundancy = rep.get('average_replication', 1.0)
except:
    redundancy = 1.0

stats = {
    'updated': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    'version': eg.get('version', root.get('backend_version', '')),
    'network': {
        'nodes_alive': nodes_alive,
        'nodes_total': nodes_total,
        'total_bytes': total_bytes,
        'available_storage_gb': total_avail_gb or eg.get('storage_limit_gb', 0),
        'redundancy': round(redundancy, 1),
    },
    'coverage': coverage.get('sensors', {}),
    'spatial_coverage': fetch('/coverage/spatial'),
    'uptake': uptake,
    'ingest': ingest,
    'items': eg.get('item_count', 0),
    'chunks': eg.get('chunks', 0),
    'km2_requested': requests_data.get('total_km2_queried', requests_data.get('total_km2_requested', 0)),
    'operations': fetch('/process/operations').get('operations', []),
    'gamification': {
        'leaderboard_nodes': fetch('/gamification/leaderboard?type=nodes&limit=20'),
        'leaderboard_users': fetch('/gamification/leaderboard?type=users&limit=20'),
        'leaderboard_groups': fetch('/gamification/leaderboard?type=groups&limit=20'),
        'achievements': fetch('/gamification/achievements'),
        'feed': fetch('/gamification/feed?limit=30'),
        'economy': fetch('/gamification/economy'),
        'challenges': fetch('/gamification/challenges'),
    },
}
print(json.dumps(stats, indent=2))
" > "$STATS_FILE"


# --- Long-term ingest history log ---
HISTORY_FILE="$REPO/data/ingest-history.jsonl"
mkdir -p "$REPO/data"
python3 -c "
import json, sys
from datetime import datetime, timezone

stats = json.load(open('$STATS_FILE'))
ingest = stats.get('ingest', {})
entry = {
    'ts': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    'items': ingest.get('total_items_fetched', 0),
    'gb': ingest.get('total_gb_fetched', 0),
    'collections': ingest.get('collections', 0),
    'daily': ingest.get('daily', [])[-1] if ingest.get('daily') else None,
    'hourly_last': ingest.get('hourly', [])[-1] if ingest.get('hourly') else None,
    'network_bytes': stats.get('network', {}).get('total_bytes', 0),
    'nodes_alive': stats.get('network', {}).get('nodes_alive', 0),
}
with open('$HISTORY_FILE', 'a') as f:
    f.write(json.dumps(entry) + '\n')
"
