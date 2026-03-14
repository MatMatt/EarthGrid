#!/bin/bash
# Update peers.json on GitHub Pages from beacon's live node list.
# Run via cron: */10 * * * * /home/matteo/EarthGrid/scripts/update-peers.sh

set -euo pipefail

BEACON="http://localhost:8400"
REPO="/home/matteo/EarthGrid"
PEERS_FILE="$REPO/docs/peers.json"

# Fetch live seed nodes from beacon
SEEDS=$(curl -sf "$BEACON/seed/nodes" 2>/dev/null) || exit 0

# Also include the beacon itself
BEACON_INFO=$(curl -sf "$BEACON/" 2>/dev/null) || exit 0

# Build peers.json
python3 -c "
import json, sys
from datetime import datetime, timezone

seeds = json.loads('$SEEDS')
beacon = json.loads('$BEACON_INFO')

peers = []

# Add beacon itself as a seed
if beacon.get('item_count', 0) > 0:
    peers.append({
        'url': 'https://mattiuzzi.zapto.org/earthgrid',
        'name': beacon.get('node_name', 'beacon'),
        'collections': beacon.get('collections', []),
        'items': beacon.get('item_count', 0),
    })

# Add registered nodes
for n in seeds.get('seed_nodes', []):
    if n.get('url'):
        peers.append({
            'url': n['url'],
            'name': n.get('node_name', 'unknown'),
            'collections': n.get('collections', []),
            'items': n.get('item_count', 0),
        })

out = {
    'seeds': peers,
    'updated': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    'source': 'https://mattiuzzi.zapto.org/earthgrid',
}
print(json.dumps(out, indent=2))
" > "$PEERS_FILE"

# Commit and push only if changed
cd "$REPO"
if ! git diff --quiet docs/peers.json 2>/dev/null; then
    git add docs/peers.json
    git commit -m "auto: update peers.json [$(date -u +%Y-%m-%dT%H:%M:%SZ)]" --no-verify
    git push
fi
