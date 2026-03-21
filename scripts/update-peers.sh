#!/bin/bash
# Update peers.json on GitHub Pages from all known nodes.
# Run via cron: */10 * * * * /home/matteo/EarthGrid/scripts/update-peers.sh

set -euo pipefail

REPO="/home/matteo/EarthGrid"
PEERS_FILE="$REPO/docs/peers.json"

python3 -c "
import json, urllib.request
from datetime import datetime, timezone

NODES = [
    {'url': 'http://localhost:8400',       'public_url': 'https://mattiuzzi.zapto.org/earthgrid'},
    {'url': 'http://192.168.188.219:8400', 'public_url': None},
]

peers = []
for n in NODES:
    try:
        with urllib.request.urlopen(n['url'] + '/node-info', timeout=10) as r:
            info = json.loads(r.read())
        peers.append({
            'url': n.get('public_url') or n['url'],
            'name': info.get('node_name', 'unknown'),
            'collections': info.get('collections', []),
            'items': info.get('item_count', info.get('items', 0)),
            'storage_gb': round(info.get('storage_gb', 0), 1),
        })
    except:
        pass

out = {
    'seeds': peers,
    'updated': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    'source': 'https://mattiuzzi.zapto.org/earthgrid',
}
print(json.dumps(out, indent=2))
" > "\$PEERS_FILE"
