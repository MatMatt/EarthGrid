//! EarthGrid Smart Replication — beacon-coordinated data distribution.
//!
//! Ported from smart_replication.py.
//!
//! The beacon tracks what each node has and assigns replication tasks
//! to maintain the configured replication factor.
//!
//! Node preferences:
//! - collections: which collections to store (empty = all)
//! - bbox: geographic area of interest (empty = global)
//! - storage_limit_gb: max storage to use
//!
//! Replication rules:
//! - replication_factor=1: no replication (each item on 1 node)
//! - replication_factor=2: each item on at least 2 nodes
//! - replication_factor=0: store everything everywhere (full mirror)
//! - Nodes only receive data matching their preferences

use std::collections::HashMap;

// ---------------------------------------------------------------------------
// NodePreferences
// ---------------------------------------------------------------------------

/// What a node wants to store.
#[derive(Debug, Clone)]
pub struct NodePreferences {
    pub node_id: String,
    /// Collections to accept — empty means accept all.
    pub collections: Vec<String>,
    /// Geographic filter `[west, south, east, north]` — empty means global.
    pub bbox: Vec<f64>,
    pub storage_limit_gb: f64,
    pub storage_used_gb: f64,
    /// Minimum copies in the network.
    pub replication_factor: u32,
}

impl Default for NodePreferences {
    fn default() -> Self {
        Self {
            node_id: String::new(),
            collections: Vec::new(),
            bbox: Vec::new(),
            storage_limit_gb: 50.0,
            storage_used_gb: 0.0,
            replication_factor: 2,
        }
    }
}

impl NodePreferences {
    /// True when used storage is below 90% of the limit.
    pub fn has_space(&self) -> bool {
        self.storage_used_gb < self.storage_limit_gb * 0.9
    }

    /// True when this node accepts the given collection.
    pub fn accepts_collection(&self, coll: &str) -> bool {
        if self.collections.is_empty() {
            return true; // no filter = accept all
        }
        self.collections.iter().any(|c| c == coll)
    }

    /// True when this node's bbox intersects with `item_bbox`.
    ///
    /// `item_bbox` must be `[west, south, east, north]`.
    pub fn accepts_bbox(&self, item_bbox: &[f64]) -> bool {
        if self.bbox.is_empty() || item_bbox.is_empty() {
            return true; // no filter = accept all
        }
        if item_bbox.len() < 4 || self.bbox.len() < 4 {
            return true;
        }
        let (w, s, e, n) = (self.bbox[0], self.bbox[1], self.bbox[2], self.bbox[3]);
        let (iw, is_, ie, in_) = (item_bbox[0], item_bbox[1], item_bbox[2], item_bbox[3]);
        // Non-overlapping conditions (inverted)
        !(ie < w || iw > e || in_ < s || is_ > n)
    }
}

// ---------------------------------------------------------------------------
// ReplicationPlan
// ---------------------------------------------------------------------------

/// A replication assignment: push/pull `item_id` to `target_nodes`.
#[derive(Debug, Clone)]
pub struct ReplicationPlan {
    pub item_id: String,
    pub target_nodes: Vec<String>,
}

// ---------------------------------------------------------------------------
// plan_replication (functional API, mirrors the Python logic)
// ---------------------------------------------------------------------------

/// Compute which nodes should receive which items to satisfy the replication factor.
///
/// # Arguments
/// * `items` — `(item_id, collection, bbox)` tuples describing available items
/// * `nodes` — all known nodes and their preferences
/// * `item_holders` — which nodes currently hold each item
/// * `replication_factor` — minimum copies required
pub fn plan_replication(
    items: &[(String, String, Vec<f64>)],
    nodes: &[NodePreferences],
    item_holders: &HashMap<String, Vec<String>>,
    replication_factor: u32,
) -> Vec<ReplicationPlan> {
    if replication_factor == 0 {
        return Vec::new(); // 0 = no auto-replication
    }

    let mut plans: Vec<ReplicationPlan> = Vec::new();

    for (item_id, collection, item_bbox) in items {
        let holders: &[String] = item_holders
            .get(item_id.as_str())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        // Count alive copies (nodes we know about)
        let alive_copies: Vec<&str> = holders
            .iter()
            .filter(|nid| nodes.iter().any(|n| &n.node_id == *nid))
            .map(String::as_str)
            .collect();

        let current = alive_copies.len() as u32;
        if current >= replication_factor {
            continue; // already well-replicated
        }

        // Find candidate nodes that:
        // 1. Don't already hold this item
        // 2. Have space
        // 3. Accept the collection + bbox
        let mut targets: Vec<String> = nodes
            .iter()
            .filter(|n| {
                !holders.contains(&n.node_id)
                    && n.has_space()
                    && n.accepts_collection(collection)
                    && n.accepts_bbox(item_bbox)
            })
            .map(|n| n.node_id.clone())
            .collect();

        let needed = (replication_factor - current) as usize;
        targets.truncate(needed);

        if !targets.is_empty() {
            plans.push(ReplicationPlan {
                item_id: item_id.clone(),
                target_nodes: targets,
            });
        }
    }

    plans
}

// ---------------------------------------------------------------------------
// ReplicationPlanner (stateful beacon-side object, mirrors the Python class)
// ---------------------------------------------------------------------------

/// Beacon-side planner that assigns replication tasks to nodes.
pub struct ReplicationPlanner {
    pub node_prefs: HashMap<String, NodePreferences>,
    /// item_id → set of node_ids that hold it
    pub item_locations: HashMap<String, std::collections::HashSet<String>>,
}

impl Default for ReplicationPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicationPlanner {
    pub fn new() -> Self {
        Self {
            node_prefs: HashMap::new(),
            item_locations: HashMap::new(),
        }
    }

    /// Update preferences for a node.
    pub fn set_preferences(&mut self, node_id: &str, mut prefs: NodePreferences) {
        prefs.node_id = node_id.to_string();
        self.node_prefs.insert(node_id.to_string(), prefs);
    }

    /// Node reports which items it holds.
    pub fn report_items(&mut self, node_id: &str, item_ids: &[String]) {
        for id in item_ids {
            self.item_locations
                .entry(id.clone())
                .or_default()
                .insert(node_id.to_string());
        }
    }

    /// Get replication tasks for a specific node.
    ///
    /// Returns items this node should pull to improve network redundancy.
    pub fn get_replication_tasks(
        &self,
        target_node_id: &str,
        max_tasks: usize,
    ) -> Vec<HashMap<String, serde_json::Value>> {
        let prefs = match self.node_prefs.get(target_node_id) {
            Some(p) => p,
            None => return Vec::new(),
        };

        if !prefs.has_space() {
            return Vec::new();
        }

        let replication_factor = prefs.replication_factor;
        if replication_factor == 0 {
            return Vec::new();
        }

        let mut tasks: Vec<HashMap<String, serde_json::Value>> = Vec::new();

        for (item_id, locations) in &self.item_locations {
            if tasks.len() >= max_tasks {
                break;
            }

            // Skip if already on this node
            if locations.contains(target_node_id) {
                continue;
            }

            // Alive = known to us
            let alive: Vec<&str> = locations
                .iter()
                .filter(|nid| self.node_prefs.contains_key(*nid))
                .map(String::as_str)
                .collect();

            if alive.len() as u32 >= replication_factor {
                continue;
            }

            // Find a source node
            let source_node = alive.first().copied().unwrap_or("");
            if source_node.is_empty() {
                continue;
            }

            let mut task: HashMap<String, serde_json::Value> = HashMap::new();
            task.insert("item_id".into(), serde_json::Value::String(item_id.clone()));
            task.insert("source_node_id".into(), serde_json::Value::String(source_node.to_string()));
            task.insert("current_copies".into(), serde_json::Value::Number(alive.len().into()));
            task.insert(
                "target_copies".into(),
                serde_json::Value::Number(replication_factor.into()),
            );
            tasks.push(task);
        }

        // Sort by most under-replicated first
        tasks.sort_by_key(|t| {
            t.get("current_copies")
                .and_then(|v| v.as_u64())
                .unwrap_or(0)
        });

        tasks.truncate(max_tasks);
        tasks
    }

    /// Get replication health summary.
    pub fn get_network_health(&self) -> HashMap<String, serde_json::Value> {
        let mut result = HashMap::new();

        if self.item_locations.is_empty() {
            result.insert("total_items".into(), serde_json::json!(0));
            result.insert("fully_replicated".into(), serde_json::json!(0));
            result.insert("under_replicated".into(), serde_json::json!(0));
            result.insert("single_copy".into(), serde_json::json!(0));
            result.insert("replication_factor".into(), serde_json::json!(0));
            result.insert("health_pct".into(), serde_json::json!(100.0));
            return result;
        }

        let factors: Vec<u32> = self
            .node_prefs
            .values()
            .filter(|p| p.replication_factor > 0)
            .map(|p| p.replication_factor)
            .collect();
        let target_rf = factors.iter().copied().max().unwrap_or(1);

        let total = self.item_locations.len();
        let fully = self
            .item_locations
            .values()
            .filter(|locs| locs.len() as u32 >= target_rf)
            .count();
        let single = self
            .item_locations
            .values()
            .filter(|locs| locs.len() == 1)
            .count();
        let under = total - fully;
        let health_pct = if total > 0 {
            (fully as f64 / total as f64 * 100.0 * 10.0).round() / 10.0
        } else {
            100.0
        };

        result.insert("total_items".into(), serde_json::json!(total));
        result.insert("fully_replicated".into(), serde_json::json!(fully));
        result.insert("under_replicated".into(), serde_json::json!(under));
        result.insert("single_copy".into(), serde_json::json!(single));
        result.insert("replication_factor".into(), serde_json::json!(target_rf));
        result.insert("health_pct".into(), serde_json::json!(health_pct));
        result
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_prefs(node_id: &str, storage_used: f64) -> NodePreferences {
        NodePreferences {
            node_id: node_id.to_string(),
            storage_used_gb: storage_used,
            storage_limit_gb: 100.0,
            replication_factor: 2,
            ..Default::default()
        }
    }

    #[test]
    fn test_has_space() {
        let p = make_prefs("n1", 80.0);
        assert!(p.has_space()); // 80 < 90

        let p2 = make_prefs("n2", 91.0);
        assert!(!p2.has_space()); // 91 > 90
    }

    #[test]
    fn test_accepts_collection() {
        let mut p = make_prefs("n1", 0.0);
        p.collections = vec!["s2".into(), "landsat".into()];
        assert!(p.accepts_collection("s2"));
        assert!(!p.accepts_collection("modis"));

        let p_all = make_prefs("n2", 0.0); // no filter
        assert!(p_all.accepts_collection("anything"));
    }

    #[test]
    fn test_accepts_bbox() {
        let mut p = make_prefs("n1", 0.0);
        p.bbox = vec![10.0, 46.0, 20.0, 52.0]; // Europe

        // Overlapping bbox
        assert!(p.accepts_bbox(&[11.0, 47.0, 12.0, 48.0]));
        // Non-overlapping (east of filter)
        assert!(!p.accepts_bbox(&[25.0, 47.0, 30.0, 50.0]));
        // Empty item bbox = accept
        assert!(p.accepts_bbox(&[]));
    }

    #[test]
    fn test_plan_replication_basic() {
        let nodes = vec![
            make_prefs("node-a", 0.0),
            make_prefs("node-b", 0.0),
            make_prefs("node-c", 0.0),
        ];

        let items = vec![
            ("item-1".to_string(), "s2".to_string(), vec![10.0, 46.0, 11.0, 47.0]),
        ];

        let mut holders: HashMap<String, Vec<String>> = HashMap::new();
        holders.insert("item-1".into(), vec!["node-a".into()]);

        let plans = plan_replication(&items, &nodes, &holders, 2);
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].item_id, "item-1");
        assert_eq!(plans[0].target_nodes.len(), 1);
        assert!(!plans[0].target_nodes.contains(&"node-a".to_string()));
    }

    #[test]
    fn test_plan_replication_already_met() {
        let nodes = vec![
            make_prefs("node-a", 0.0),
            make_prefs("node-b", 0.0),
        ];
        let items = vec![
            ("item-1".to_string(), "s2".to_string(), vec![]),
        ];
        let mut holders: HashMap<String, Vec<String>> = HashMap::new();
        holders.insert("item-1".into(), vec!["node-a".into(), "node-b".into()]);

        let plans = plan_replication(&items, &nodes, &holders, 2);
        assert!(plans.is_empty(), "Already at target RF, no plan needed");
    }

    #[test]
    fn test_plan_replication_no_space() {
        let mut full_node = make_prefs("node-b", 95.0); // over limit
        full_node.storage_limit_gb = 100.0;
        let nodes = vec![make_prefs("node-a", 0.0), full_node];

        let items = vec![("item-1".to_string(), "s2".to_string(), vec![])];
        let mut holders: HashMap<String, Vec<String>> = HashMap::new();
        holders.insert("item-1".into(), vec!["node-a".into()]);

        let plans = plan_replication(&items, &nodes, &holders, 2);
        // node-b has no space, so no targets
        assert!(plans.is_empty());
    }

    #[test]
    fn test_replication_planner_network_health() {
        let mut planner = ReplicationPlanner::new();
        let p = make_prefs("node-a", 0.0);
        planner.set_preferences("node-a", p);
        planner.report_items("node-a", &["item-1".to_string(), "item-2".to_string()]);

        let health = planner.get_network_health();
        assert_eq!(health["total_items"], 2);
        assert_eq!(health["single_copy"], 2);
    }
}
