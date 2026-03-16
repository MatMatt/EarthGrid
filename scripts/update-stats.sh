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
nodes_data = fetch('/nodes')
coverage = fetch('/stats/coverage')
uptake = fetch('/stats/uptake?period_days=365')
ingest = fetch('/stats/ingest?period_days=365')
requests_data = fetch('/stats/requests')

nodes = nodes_data.get('nodes', [])
alive = [n for n in nodes if n.get('alive')]

# Compute totals
total_bytes = sum(n.get('chunks_bytes', 0) for n in nodes) or root.get('chunks_bytes', 0)
total_avail_gb = sum(n.get('storage_limit_gb', 0) for n in nodes)

try:
    rep = fetch('/replication/health')
    redundancy = rep.get('average_replication', 1.0)
except:
    redundancy = 1.0

stats = {
    'updated': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    'version': root.get('version', ''),
    'network': {
        'nodes_alive': len(alive),
        'nodes_total': len(nodes),
        'total_bytes': total_bytes,
        'available_storage_gb': total_avail_gb,
        'redundancy': round(redundancy, 1),
    },
    'coverage': coverage.get('sensors', {}),
    'uptake': uptake,
    'ingest': ingest,
    'km2_requested': requests_data.get('total_km2_queried', requests_data.get('total_km2_requested', 0)),
    'operations': fetch('/process/operations').get('operations', []),
}
print(json.dumps(stats, indent=2))
" > "$STATS_FILE"

cd "$REPO"
if ! git diff --quiet docs/stats.json 2>/dev/null; then
    git add docs/stats.json
    git commit -m "auto: update stats.json [$(date -u +%Y-%m-%dT%H:%M:%SZ)]" --no-verify
    git push
fi
