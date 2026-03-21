#!/bin/bash
# Snapshot EarthGrid stats to docs/stats.json for GitHub Pages dashboard.
# Aggregates data from ALL known nodes.
# Run via cron: */10 * * * * /home/matteo/EarthGrid/scripts/update-stats.sh

set -euo pipefail

REPO="/home/matteo/EarthGrid"
STATS_FILE="$REPO/docs/stats.json"

python3 -c "
import json, urllib.request
from datetime import datetime, timezone

# All known nodes
NODES = [
    {'url': 'http://localhost:8400',       'public_url': 'https://mattiuzzi.zapto.org/earthgrid'},
    {'url': 'http://192.168.188.219:8400', 'public_url': None},
]

def fetch(base, path):
    try:
        with urllib.request.urlopen(base + path, timeout=10) as r:
            return json.loads(r.read())
    except:
        return {}

# Gather info from all nodes
node_infos = []
for n in NODES:
    info = fetch(n['url'], '/node-info')
    if info.get('node_id'):
        info['_base'] = n['url']
        info['_public'] = n.get('public_url')
        node_infos.append(info)

if not node_infos:
    exit(0)

# Use first node (Nucleus) as primary for detailed stats
PRIMARY = NODES[0]['url']
coverage = fetch(PRIMARY, '/stats/coverage')
uptake = fetch(PRIMARY, '/stats/uptake?period_days=365')
ingest = fetch(PRIMARY, '/stats/ingest?period_days=365')

# Aggregate network totals
total_bytes = sum(n.get('storage_bytes', 0) for n in node_infos)
total_items = sum(n.get('item_count', n.get('items', 0)) for n in node_infos)
total_chunks = sum(n.get('chunks', 0) for n in node_infos)
avail_gb = sum(n.get('storage_limit_gb', 0) for n in node_infos)
nodes_alive = len(node_infos)
nodes_total = len(NODES)

# Version from primary
version = node_infos[0].get('version', '')

# Get spatial data for accurate tile/date counts
_spatial = fetch(PRIMARY, '/coverage/spatial')
_tile_stats = {}
for _col, _cdata in (_spatial.get('collections', {}) if _spatial else {}).items():
    _cells = _cdata.get('cells', [])
    _tile_stats[_col] = {'tiles': len(_cells), 'dates': sum(c.get('date_count', 0) for c in _cells)}

stats = {
    'updated': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    'version': version,
    'network': {
        'nodes_alive': nodes_alive,
        'nodes_total': nodes_total,
        'total_bytes': total_bytes,
        'available_storage_gb': round(avail_gb, 1),
        'redundancy': 1.0,
    },
    'nodes': [
        {
            'node_id': n.get('node_id'),
            'node_name': n.get('node_name'),
            'items': n.get('item_count', n.get('items', 0)),
            'storage_gb': round(n.get('storage_gb', 0), 1),
            'storage_limit_gb': n.get('storage_limit_gb', 0),
            'chunks': n.get('chunks', 0),
            'version': n.get('version', ''),
            'public_url': n.get('_public'),
        }
        for n in node_infos
    ],
    'coverage': {
        c['collection']: {
            'items': c.get('item_count', 0),
            'tiles': _tile_stats.get(c['collection'], {}).get('tiles', 0),
            'dates': _tile_stats.get(c['collection'], {}).get('dates', 0),
        }
        for c in coverage.get('collections', [])
    },
    'spatial_coverage': _spatial,
    'uptake': uptake,
    'ingest': ingest,
    'items': total_items,
    'chunks': total_chunks,
    'km2_requested': uptake.get('summary', {}).get('total_aoi_km2', 0),
    'operations': fetch(PRIMARY, '/process/operations').get('operations', []),
    'gamification': {
        'leaderboard_nodes': fetch(PRIMARY, '/gamification/leaderboard?type=nodes&limit=20'),
        'leaderboard_users': fetch(PRIMARY, '/gamification/leaderboard?type=users&limit=20'),
        'leaderboard_groups': fetch(PRIMARY, '/gamification/leaderboard?type=groups&limit=20'),
        'achievements': fetch(PRIMARY, '/gamification/achievements'),
        'feed': fetch(PRIMARY, '/gamification/feed?limit=30'),
        'economy': fetch(PRIMARY, '/gamification/economy'),
        'challenges': fetch(PRIMARY, '/gamification/challenges'),
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
