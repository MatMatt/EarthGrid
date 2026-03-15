import re

with open('/home/matteo/EarthGrid/docs/index.html') as f:
    content = f.read()

# Fix 1: Node count — don't add phantom node
content = content.replace(
    "const allNodes = [{alive: true}].concat(nodes);\n    document.getElementById('vNodes').textContent = allNodes.length;",
    "document.getElementById('vNodes').textContent = nodes.length;"
)

# Fix 2: km² requested field name
content = content.replace(
    "rd.total_km2_requested",
    "rd.total_km2_queried || rd.total_km2_requested"
)

# Fix 3: Coverage should aggregate from ALL nodes, not just beacon
# Replace the coverage section to query all nodes
old_cov = '''    // Coverage per sensor
    const cr = await fetch(BEACON + '/stats/coverage');
    const cd = await cr.json();
    const sensors = cd.sensors || {};'''

new_cov = '''    // Coverage per sensor — aggregate from all reachable nodes
    let sensors = {};
    // Start with beacon's own coverage
    try {
      const cr = await fetch(BEACON + '/stats/coverage');
      const cd = await cr.json();
      sensors = cd.sensors || {};
    } catch(e) {}
    // Merge coverage from peer nodes (if reachable)
    for (const node of nodes) {
      if (!node.alive || !node.url || node.url === BEACON) continue;
      try {
        const pcr = await fetch(node.url + '/stats/coverage');
        const pcd = await pcr.json();
        for (const [col, s] of Object.entries(pcd.sensors || {})) {
          if (!sensors[col]) {
            sensors[col] = {area_km2: 0, tiles: 0, items: 0};
          }
          // Items and tiles might overlap — take max for conservative estimate
          sensors[col].area_km2 = Math.max(sensors[col].area_km2, s.area_km2);
          sensors[col].tiles = Math.max(sensors[col].tiles, s.tiles);
          sensors[col].items = Math.max(sensors[col].items, s.items);
        }
      } catch(e) {} // peer unreachable — skip
    }'''

content = content.replace(old_cov, new_cov)

# Fix 4: Redundancy should be network-wide
content = content.replace(
    "document.getElementById('vRedundancy').textContent = (d.redundancy_index || 1.0) + 'x';",
    '''// Compute network-wide redundancy from beacon
    try {
      const repR = await fetch(BEACON + '/replication/health');
      const repD = await repR.json();
      document.getElementById('vRedundancy').textContent = (repD.average_replication || 1.0).toFixed(1) + 'x';
    } catch(e) {
      document.getElementById('vRedundancy').textContent = (d.redundancy_index || 1.0) + 'x';
    }'''
)

with open('/home/matteo/EarthGrid/docs/index.html', 'w') as f:
    f.write(content)
print('DASHBOARD PATCHED')
