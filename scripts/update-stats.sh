#!/bin/bash
# Snapshot EarthGrid stats to docs/stats.json for GitHub Pages dashboard.
# Run via cron: */10 * * * * /home/matteo/EarthGrid/scripts/update-stats.sh

set -euo pipefail

REPO="/home/matteo/EarthGrid"
STATS_FILE="$REPO/docs/stats.json"

python3 -c "
import json, urllib.request
from datetime import datetime, timezone

API = 'http://localhost:8400'

def fetch(path):
    try:
        with urllib.request.urlopen(API + path, timeout=10) as r:
            return json.loads(r.read())
    except:
        return {}

# Primary data sources (Rust API)
info = fetch('/node-info')
peers_data = fetch('/peers')
coverage = fetch('/stats/coverage')
uptake = fetch('/stats/uptake?period_days=365')
ingest = fetch('/stats/ingest?period_days=365')
stats_data = fetch('/stats')

# Peers
peers = peers_data.get('peers', [])
alive_peers = [p for p in peers if p.get('alive', True)]

# Network totals: this node + peers
self_bytes = info.get('storage_bytes', 0)
self_gb = info.get('storage_gb', 0)
peer_bytes = sum(p.get('storage_bytes', p.get('chunks_bytes', 0)) for p in alive_peers)
peer_gb = sum(p.get('storage_gb', p.get('storage_limit_gb', 0)) for p in alive_peers)

# Available storage: pledged limit from config (not raw disk free)
import json as _json
try:
    with open('/home/matteo/.earthgrid/config.json') as _f:
        _cfg = _json.load(_f)
    avail_gb = _cfg.get('storage_limit_gb', 0)
except:
    avail_gb = 0
# Add peer pledged storage
avail_gb += sum(p.get('storage_limit_gb', p.get('available_gb', 0)) for p in alive_peers)

nodes_alive = 1 + len(alive_peers)  # self + alive peers
nodes_total = 1 + len(peers)

stats = {
    'updated': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    'version': info.get('version', ''),
    'network': {
        'nodes_alive': nodes_alive,
        'nodes_total': nodes_total,
        'total_bytes': self_bytes + peer_bytes,
        'available_storage_gb': round(avail_gb, 1),
        'redundancy': 1.0,
    },
    'coverage': {
        c['collection']: {
            'items': c.get('item_count', 0),
            'tiles': c.get('item_count', 0),
        }
        for c in coverage.get('collections', [])
    },
    'spatial_coverage': fetch('/coverage/spatial'),
    'uptake': uptake,
    'ingest': ingest,
    'items': info.get('items', info.get('item_count', 0)),
    'chunks': info.get('chunks', 0),
    'km2_requested': uptake.get('summary', {}).get('total_aoi_km2', 0),
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
import json
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

# --- Auto-commit stats.json to GitHub Pages ---
cd "$REPO"
if git diff --quiet docs/stats.json 2>/dev/null; then
  : # no changes
else
  git add docs/stats.json
  git commit -q -m "chore: auto-update stats.json [skip ci]"
  git push -q origin master 2>/dev/null || true
fi
