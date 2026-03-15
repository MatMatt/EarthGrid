"""EarthGrid Smart Replication — beacon-coordinated data distribution.

The beacon tracks what each node has and assigns replication tasks
to maintain the configured replication factor.

Node preferences:
- collections: which collections to store (empty = all)
- bbox: geographic area of interest (empty = global)
- storage_limit_gb: max storage to use

Replication rules:
- replication_factor=1: no replication (each chunk on 1 node)
- replication_factor=2: each chunk on at least 2 nodes
- replication_factor=0: store everything everywhere (full mirror)
- Nodes only receive data matching their preferences
"""
from __future__ import annotations
import logging
import time
from dataclasses import dataclass, field

logger = logging.getLogger("earthgrid.replication_planner")


@dataclass
class NodePreferences:
    """What a node wants to store."""
    node_id: str = ""
    collections: list[str] = field(default_factory=list)  # empty = all
    bbox: list[float] = field(default_factory=list)  # [west, south, east, north], empty = global
    storage_limit_gb: float = 50.0
    storage_used_gb: float = 0.0
    replication_factor: int = 2  # minimum copies in network

    @property
    def has_space(self) -> bool:
        return self.storage_used_gb < self.storage_limit_gb * 0.9

    def accepts_collection(self, collection_id: str) -> bool:
        if not self.collections:
            return True  # no filter = accept all
        return collection_id in self.collections

    def accepts_bbox(self, item_bbox: list[float]) -> bool:
        if not self.bbox or not item_bbox:
            return True  # no filter = accept all
        # Simple bbox intersection check
        w, s, e, n = self.bbox
        iw, is_, ie, in_ = item_bbox[:4]
        return not (ie < w or iw > e or in_ < s or is_ > n)


class ReplicationPlanner:
    """Beacon-side planner that assigns replication tasks to nodes.

    Called by the beacon to determine which nodes should pull which items.
    """

    def __init__(self):
        self.node_prefs: dict[str, NodePreferences] = {}
        # Track which nodes have which items (item_id → set of node_ids)
        self.item_locations: dict[str, set[str]] = {}

    def set_preferences(self, node_id: str, prefs: NodePreferences):
        """Update preferences for a node."""
        prefs.node_id = node_id
        self.node_prefs[node_id] = prefs

    def report_items(self, node_id: str, item_ids: list[str]):
        """Node reports which items it has."""
        for item_id in item_ids:
            if item_id not in self.item_locations:
                self.item_locations[item_id] = set()
            self.item_locations[item_id].add(node_id)

    def get_replication_tasks(self, target_node_id: str,
                              max_tasks: int = 50) -> list[dict]:
        """Get replication tasks for a specific node.

        Returns list of {item_id, source_node_id, source_url} that this
        node should pull to improve network redundancy.
        """
        prefs = self.node_prefs.get(target_node_id)
        if not prefs or not prefs.has_space:
            return []

        replication_factor = prefs.replication_factor
        if replication_factor <= 0:
            return []  # 0 = no auto-replication

        tasks = []

        # Find under-replicated items that match this node's preferences
        for item_id, locations in self.item_locations.items():
            if len(tasks) >= max_tasks:
                break

            # Skip if already on this node
            if target_node_id in locations:
                continue

            # Skip if already at target replication factor
            # Count only alive nodes
            alive_locations = {nid for nid in locations if nid in self.node_prefs}
            if len(alive_locations) >= replication_factor:
                continue

            # Check if item matches node preferences
            # (we'd need item metadata here — for now just collection check)
            # TODO: add bbox check when item metadata is tracked

            # Find a source node
            source_node = None
            for src_id in alive_locations:
                src_prefs = self.node_prefs.get(src_id)
                if src_prefs:
                    source_node = src_id
                    break

            if source_node:
                tasks.append({
                    "item_id": item_id,
                    "source_node_id": source_node,
                    "current_copies": len(alive_locations),
                    "target_copies": replication_factor,
                })

        # Sort by most under-replicated first
        tasks.sort(key=lambda t: t["current_copies"])

        return tasks[:max_tasks]

    def get_network_health(self) -> dict:
        """Get replication health summary."""
        if not self.item_locations:
            return {
                "total_items": 0,
                "fully_replicated": 0,
                "under_replicated": 0,
                "single_copy": 0,
                "replication_factor": 0,
            }

        # Use the most common replication factor
        factors = [p.replication_factor for p in self.node_prefs.values() if p.replication_factor > 0]
        target_rf = max(factors) if factors else 1

        total = len(self.item_locations)
        fully = sum(1 for locs in self.item_locations.values() if len(locs) >= target_rf)
        single = sum(1 for locs in self.item_locations.values() if len(locs) == 1)
        under = total - fully

        return {
            "total_items": total,
            "fully_replicated": fully,
            "under_replicated": under,
            "single_copy": single,
            "replication_factor": target_rf,
            "health_pct": round(fully / total * 100, 1) if total else 100.0,
        }
