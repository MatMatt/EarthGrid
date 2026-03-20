import json, urllib.request
from datetime import datetime, timezone

base = "http://127.0.0.1:8400"

def get(path):
    try:
        with urllib.request.urlopen(base + path, timeout=10) as r:
            return json.loads(r.read())
    except:
        return {}

beacon = get("/beacon/nodes")
nodes = beacon.get("nodes", [])
alive = [n for n in nodes if n.get("alive")]
coverage = get("/stats/coverage")
spatial = get("/coverage/spatial")

out = {
    "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "version": "0.1.0",
    "network": {
        "nodes_alive": len(alive),
        "nodes_total": len(nodes),
        "total_bytes": sum(n.get("chunks_bytes", 0) for n in alive),
        "available_storage_gb": sum(n.get("storage_limit_gb", 0) for n in alive),
        "redundancy": 1.0
    },
    "items": sum(n.get("item_count", 0) for n in alive),
    "chunks": sum(n.get("chunk_count", 0) for n in alive),
    "coverage": coverage,
    "spatial_coverage": spatial,
}
with open("/mnt/sda/earthgrid/dashboard_stats.json", "w") as f:
    json.dump(out, f, indent=2)
