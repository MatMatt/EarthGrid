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

# Build peers.json — only include publicly reachable nodes
python3 -c "
import json, re, sys
from datetime import datetime, timezone

seeds = json.loads('$SEEDS')
beacon = json.loads('$BEACON_INFO')

def is_public(url):
    # Filter out LAN/private IPs
    private = re.search(r'//(?:192\.168\.|10\.|172\.(?:1[6-9]|2[0-9]|3[01])\.|127\.|localhost|0\.0\.0\.0)', url)
    return not private

peers = []

# Add beacon itself as a seed
if beacon.get('item_count', 0) > 0:
    peers.append({
        'url': 'https://mattiuzzi.zapto.org/earthgrid',
        'name': beacon.get('node_name', 'beacon'),
        'collections': beacon.get('collections', []),
        'items': beacon.get('item_count', 0),
    })

# Add registered nodes (only public)
for n in seeds.get('seed_nodes', []):
    url = n.get('url', '')
    if url and is_public(url):
        peers.append({
            'url': url,
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
