"""EarthGrid Gamification — Node, User & Group performance tracking.

Opt-in gamification layer: tracks contributions, awards achievements,
and maintains leaderboards at three levels:
  - Node: individual node performance
  - User: aggregated across all their nodes
  - Group: teams of users contributing together

Privacy: fully opt-in. Non-opted-in nodes/users are invisible.
"""
from __future__ import annotations
import logging
import sqlite3
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger("earthgrid.gamification")

# --- Achievement Definitions ---
ACHIEVEMENTS = [
    {"id": "first_seed",      "icon": "🌱", "name": "First Seed",      "desc": "Ingested your first data",            "threshold": 1,     "field": "items_ingested"},
    {"id": "mesh_pioneer",    "icon": "🤝", "name": "Mesh Pioneer",    "desc": "Connected to 10+ peers",              "threshold": 10,    "field": "max_peers"},
    {"id": "always_on",       "icon": "💪", "name": "Always On",       "desc": "30 consecutive days online",          "threshold": 30,    "field": "streak_days"},
    {"id": "terabyte_club",   "icon": "🏔️", "name": "Terabyte Club",   "desc": "Stored 1 TB+ of data",               "threshold": 1024,  "field": "gb_stored"},
    {"id": "data_relay",      "icon": "📡", "name": "Data Relay",      "desc": "Served 100 GB+ to other nodes",       "threshold": 100,   "field": "gb_served"},
    {"id": "early_adopter",   "icon": "🧪", "name": "Early Adopter",   "desc": "Among the first 100 users",          "threshold": 100,   "field": "user_rank"},
    {"id": "continental",     "icon": "🌍", "name": "Continental",     "desc": "Data spanning 3+ continents",         "threshold": 3,     "field": "continents"},
    {"id": "global_citizen",  "icon": "🌐", "name": "Global Citizen",  "desc": "Data from all 6 inhabited continents","threshold": 6,     "field": "continents"},
    {"id": "speed_demon",     "icon": "🔥", "name": "Speed Demon",     "desc": "Fastest query response in network",   "threshold": 1,     "field": "speed_rank"},
    {"id": "power_node",      "icon": "⚡", "name": "Power Node",      "desc": "Top 3 in any category for 7+ days",   "threshold": 7,     "field": "top3_streak"},
    {"id": "centurion",       "icon": "💯", "name": "Centurion",       "desc": "100+ days total uptime",              "threshold": 100,   "field": "uptime_days"},
    {"id": "petabyte_dream",  "icon": "🚀", "name": "Petabyte Dream",  "desc": "Served 1 TB+ total",                 "threshold": 1024,  "field": "gb_served"},
]

# Map MGRS grid zone prefix → continent
_ZONE_CONTINENT = {
    "C": "Antarctica", "D": "Antarctica",
    "E": "Africa", "F": "Africa", "G": "Africa", "H": "Africa",
    "J": "Asia", "K": "Asia", "L": "Asia", "M": "Asia",
    "N": "Europe", "P": "Europe", "Q": "Asia", "R": "Europe",
    "S": "South America", "T": "Europe", "U": "Europe",
    "V": "Europe", "W": "Europe", "X": "Europe",
}
# Latitude bands for rough continent mapping
def _tile_to_continent(tile_id: str) -> str:
    """Best-effort MGRS tile → continent. Returns '' if unknown."""
    if not tile_id or len(tile_id) < 3:
        return ""
    lat_band = tile_id[2].upper() if len(tile_id) >= 3 else ""
    zone_num = int(tile_id[:2]) if tile_id[:2].isdigit() else 0
    # Rough heuristic based on UTM zone + latitude band
    if lat_band in ("C", "D"):
        return "Antarctica"
    if lat_band in ("N", "P", "Q", "R", "S", "T", "U", "V", "W", "X"):
        if 1 <= zone_num <= 9 or 29 <= zone_num <= 38:
            return "Europe"
        if 10 <= zone_num <= 19:
            return "North America"
        if 39 <= zone_num <= 60:
            return "Asia"
        if 20 <= zone_num <= 28:
            return "North America"
    if lat_band in ("E", "F", "G", "H", "J", "K"):
        if 29 <= zone_num <= 38:
            return "Africa"
        if 39 <= zone_num <= 54:
            return "Asia"
        if 55 <= zone_num <= 60:
            return "Oceania"
        if 1 <= zone_num <= 9:
            return "Africa"
        if 17 <= zone_num <= 23:
            return "South America"
    if lat_band in ("L", "M"):
        if 1 <= zone_num <= 9:
            return "Africa"
        if 17 <= zone_num <= 25:
            return "South America"
        if 49 <= zone_num <= 60:
            return "Oceania"
        if 39 <= zone_num <= 48:
            return "Asia"
    return ""


class GamificationEngine:
    """SQLite-backed gamification tracking."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            # Node scores
            conn.execute("""CREATE TABLE IF NOT EXISTS node_scores (
                node_id TEXT PRIMARY KEY,
                owner_user TEXT NOT NULL DEFAULT '',
                group_id TEXT NOT NULL DEFAULT '',
                opted_in INTEGER NOT NULL DEFAULT 0,
                items_ingested INTEGER NOT NULL DEFAULT 0,
                bytes_stored INTEGER NOT NULL DEFAULT 0,
                bytes_served INTEGER NOT NULL DEFAULT 0,
                queries_answered INTEGER NOT NULL DEFAULT 0,
                max_peers INTEGER NOT NULL DEFAULT 0,
                uptime_seconds REAL NOT NULL DEFAULT 0,
                streak_days INTEGER NOT NULL DEFAULT 0,
                streak_last_date TEXT NOT NULL DEFAULT '',
                continents TEXT NOT NULL DEFAULT '',
                first_seen REAL NOT NULL DEFAULT 0,
                last_seen REAL NOT NULL DEFAULT 0,
                score INTEGER NOT NULL DEFAULT 0,
                display_alias TEXT NOT NULL DEFAULT '',
                anonymous INTEGER NOT NULL DEFAULT 0
            )""")

            # User scores (aggregated)
            conn.execute("""CREATE TABLE IF NOT EXISTS user_scores (
                username TEXT PRIMARY KEY,
                opted_in INTEGER NOT NULL DEFAULT 0,
                display_name TEXT NOT NULL DEFAULT '',
                total_nodes INTEGER NOT NULL DEFAULT 0,
                total_items INTEGER NOT NULL DEFAULT 0,
                total_bytes_stored INTEGER NOT NULL DEFAULT 0,
                total_bytes_served INTEGER NOT NULL DEFAULT 0,
                total_queries INTEGER NOT NULL DEFAULT 0,
                total_score INTEGER NOT NULL DEFAULT 0,
                first_seen REAL NOT NULL DEFAULT 0,
                last_seen REAL NOT NULL DEFAULT 0
            )""")

            # Groups
            conn.execute("""CREATE TABLE IF NOT EXISTS groups (
                group_id TEXT PRIMARY KEY,
                group_name TEXT UNIQUE NOT NULL,
                created_by TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL DEFAULT 0,
                description TEXT NOT NULL DEFAULT '',
                total_score INTEGER NOT NULL DEFAULT 0,
                display_alias TEXT NOT NULL DEFAULT '',
                anonymous INTEGER NOT NULL DEFAULT 0
            )""")

            # Group membership
            conn.execute("""CREATE TABLE IF NOT EXISTS group_members (
                group_id TEXT NOT NULL,
                username TEXT NOT NULL,
                joined_at REAL NOT NULL DEFAULT 0,
                role TEXT NOT NULL DEFAULT 'member',
                PRIMARY KEY (group_id, username)
            )""")

            # Achievements earned
            conn.execute("""CREATE TABLE IF NOT EXISTS achievements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT NOT NULL DEFAULT 'node',
                entity_id TEXT NOT NULL,
                achievement_id TEXT NOT NULL,
                earned_at REAL NOT NULL,
                UNIQUE(entity_type, entity_id, achievement_id)
            )""")

            # Activity feed
            conn.execute("""CREATE TABLE IF NOT EXISTS activity_feed (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                node_id TEXT NOT NULL DEFAULT '',
                username TEXT NOT NULL DEFAULT '',
                event_type TEXT NOT NULL DEFAULT '',
                detail TEXT NOT NULL DEFAULT '',
                score_delta INTEGER NOT NULL DEFAULT 0
            )""")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_feed_ts ON activity_feed(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_node_owner ON node_scores(owner_user)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_node_group ON node_scores(group_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ach_entity ON achievements(entity_type, entity_id)")

    # --- Opt-in ---

    def opt_in_node(self, node_id: str, owner_user: str = "",
                     anonymous: bool = False):
        """Opt a node into gamification.
        
        anonymous=True: node participates but with no traceable link to a user.
        The node gets a random display alias like 'node-a7f3' instead.
        """
        now = time.time()
        display_alias = ""
        if anonymous:
            import hashlib
            display_alias = "node-" + hashlib.sha256(node_id.encode()).hexdigest()[:4]
            owner_user = ""  # no user link
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""INSERT INTO node_scores
                (node_id, owner_user, opted_in, display_alias, anonymous, first_seen, last_seen)
                VALUES (?, ?, 1, ?, ?, ?, ?)
                ON CONFLICT(node_id) DO UPDATE SET opted_in=1, owner_user=?,
                display_alias=?, anonymous=?""",
                (node_id, owner_user, display_alias, 1 if anonymous else 0,
                 now, now, owner_user, display_alias, 1 if anonymous else 0))
        mode = "anonymous" if anonymous else f"owner={owner_user}"
        logger.info(f"Node {node_id} opted in ({mode})")

    def opt_in_user(self, username: str, display_name: str = ""):
        """Opt a user into gamification."""
        now = time.time()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""INSERT INTO user_scores (username, opted_in, display_name, first_seen, last_seen)
                VALUES (?, 1, ?, ?, ?)
                ON CONFLICT(username) DO UPDATE SET opted_in=1, display_name=?""",
                (username, display_name or username, now, now, display_name or username))

    # --- Groups ---

    def create_group(self, group_id: str, group_name: str, created_by: str,
                     description: str = "") -> dict:
        now = time.time()
        with sqlite3.connect(self.db_path) as conn:
            try:
                conn.execute("""INSERT INTO groups (group_id, group_name, created_by, created_at, description)
                    VALUES (?, ?, ?, ?, ?)""",
                    (group_id, group_name, created_by, now, description))
                conn.execute("""INSERT INTO group_members (group_id, username, joined_at, role)
                    VALUES (?, ?, ?, 'admin')""",
                    (group_id, created_by, now))
            except sqlite3.IntegrityError:
                raise ValueError(f"Group '{group_name}' already exists")
        return {"group_id": group_id, "group_name": group_name, "created_by": created_by}

    def join_group(self, group_id: str, username: str):
        now = time.time()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""INSERT OR IGNORE INTO group_members (group_id, username, joined_at)
                VALUES (?, ?, ?)""", (group_id, username, now))

    def set_node_group(self, node_id: str, group_id: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE node_scores SET group_id=? WHERE node_id=?",
                         (group_id, node_id))

    # --- Event Recording ---

    def record_event(self, node_id: str, event_type: str, detail: str = "",
                     bytes_delta: int = 0, items_delta: int = 0,
                     queries_delta: int = 0, bytes_served_delta: int = 0,
                     peers_count: int = 0, tiles: list[str] | None = None):
        """Record a gamification event for a node."""
        now = time.time()
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM node_scores WHERE node_id=?",
                               (node_id,)).fetchone()
            if not row or not row["opted_in"]:
                return  # not opted in, skip

            # Update node scores
            updates = []
            params = []
            if items_delta:
                updates.append("items_ingested = items_ingested + ?")
                params.append(items_delta)
            if bytes_delta:
                updates.append("bytes_stored = bytes_stored + ?")
                params.append(bytes_delta)
            if bytes_served_delta:
                updates.append("bytes_served = bytes_served + ?")
                params.append(bytes_served_delta)
            if queries_delta:
                updates.append("queries_answered = queries_answered + ?")
                params.append(queries_delta)
            if peers_count and peers_count > (row["max_peers"] or 0):
                updates.append("max_peers = ?")
                params.append(peers_count)

            # Continent tracking
            if tiles:
                existing = set(row["continents"].split(",")) if row["continents"] else set()
                for t in tiles:
                    c = _tile_to_continent(t)
                    if c:
                        existing.add(c)
                existing.discard("")
                updates.append("continents = ?")
                params.append(",".join(sorted(existing)))

            # Streak check
            today = time.strftime("%Y-%m-%d", time.gmtime())
            if row["streak_last_date"] != today:
                yesterday = time.strftime("%Y-%m-%d",
                    time.gmtime(now - 86400))
                if row["streak_last_date"] == yesterday:
                    updates.append("streak_days = streak_days + 1")
                elif row["streak_last_date"] == "":
                    updates.append("streak_days = 1")
                else:
                    updates.append("streak_days = 1")  # streak broken
                updates.append("streak_last_date = ?")
                params.append(today)

            updates.append("last_seen = ?")
            params.append(now)

            # Score calculation: 1 per item + 1 per GB stored + 2 per GB served + 1 per 100 queries
            score_add = items_delta + (bytes_delta // (1024**3)) + \
                        (bytes_served_delta // (1024**3)) * 2 + (queries_delta // 100)
            if score_add > 0:
                updates.append("score = score + ?")
                params.append(score_add)

            if updates:
                params.append(node_id)
                conn.execute(
                    f"UPDATE node_scores SET {', '.join(updates)} WHERE node_id=?",
                    params)

            # Activity feed — use alias for anonymous nodes
            if score_add > 0 or event_type in ("achievement", "join", "ingest"):
                is_anon = bool(row.get("anonymous", 0))
                feed_node = row.get("display_alias") or node_id if is_anon else node_id
                feed_user = "" if is_anon else row["owner_user"]
                conn.execute("""INSERT INTO activity_feed
                    (timestamp, node_id, username, event_type, detail, score_delta)
                    VALUES (?, ?, ?, ?, ?, ?)""",
                    (now, feed_node, feed_user, event_type, detail, score_add))

            # Check achievements
            self._check_achievements(conn, node_id, row["owner_user"])

    def record_heartbeat(self, node_id: str, peers_count: int = 0,
                         uptime_seconds: float = 0):
        """Record a node heartbeat (called periodically by federation sync)."""
        now = time.time()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""UPDATE node_scores SET
                last_seen=?, uptime_seconds=?, max_peers=MAX(max_peers, ?)
                WHERE node_id=? AND opted_in=1""",
                (now, uptime_seconds, peers_count, node_id))
            # Streak update
            today = time.strftime("%Y-%m-%d", time.gmtime())
            row = conn.execute(
                "SELECT streak_last_date, streak_days FROM node_scores WHERE node_id=?",
                (node_id,)).fetchone()
            if row and row[0] != today:
                yesterday = time.strftime("%Y-%m-%d", time.gmtime(now - 86400))
                new_streak = (row[1] + 1) if row[0] == yesterday else 1
                conn.execute(
                    "UPDATE node_scores SET streak_days=?, streak_last_date=? WHERE node_id=?",
                    (new_streak, today, node_id))

    def _check_achievements(self, conn: sqlite3.Connection, node_id: str,
                            username: str):
        """Check and award achievements for a node."""
        conn.row_factory = sqlite3.Row
        node = conn.execute("SELECT * FROM node_scores WHERE node_id=?",
                            (node_id,)).fetchone()
        if not node:
            return

        existing = set(r[0] for r in conn.execute(
            "SELECT achievement_id FROM achievements WHERE entity_type='node' AND entity_id=?",
            (node_id,)).fetchall())

        now = time.time()
        for ach in ACHIEVEMENTS:
            if ach["id"] in existing:
                continue
            field = ach["field"]
            threshold = ach["threshold"]
            earned = False

            if field == "items_ingested":
                earned = (node["items_ingested"] or 0) >= threshold
            elif field == "max_peers":
                earned = (node["max_peers"] or 0) >= threshold
            elif field == "streak_days":
                earned = (node["streak_days"] or 0) >= threshold
            elif field == "gb_stored":
                earned = ((node["bytes_stored"] or 0) / (1024**3)) >= threshold
            elif field == "gb_served":
                earned = ((node["bytes_served"] or 0) / (1024**3)) >= threshold
            elif field == "continents":
                n_cont = len([c for c in node["continents"].split(",") if c]) if node["continents"] else 0
                earned = n_cont >= threshold
            elif field == "uptime_days":
                earned = ((node["uptime_seconds"] or 0) / 86400) >= threshold
            elif field == "user_rank":
                rank = conn.execute(
                    "SELECT COUNT(*) FROM user_scores WHERE opted_in=1").fetchone()[0]
                earned = rank <= threshold
            # speed_rank and top3_streak need external signals — skip auto-check

            if earned:
                try:
                    conn.execute("""INSERT INTO achievements
                        (entity_type, entity_id, achievement_id, earned_at)
                        VALUES ('node', ?, ?, ?)""",
                        (node_id, ach["id"], now))
                    # Feed entry
                    conn.execute("""INSERT INTO activity_feed
                        (timestamp, node_id, username, event_type, detail, score_delta)
                        VALUES (?, ?, ?, 'achievement', ?, 0)""",
                        (now, node_id, username,
                         f"{ach['icon']} {ach['name']}: {ach['desc']}"))
                    logger.info(f"Achievement unlocked: {ach['name']} for node {node_id}")
                except sqlite3.IntegrityError:
                    pass

    # --- Aggregation ---

    def refresh_user_scores(self):
        """Aggregate node scores into user scores."""
        with sqlite3.connect(self.db_path) as conn:
            users = conn.execute(
                "SELECT DISTINCT owner_user FROM node_scores WHERE opted_in=1 AND owner_user != ''").fetchall()
            for (username,) in users:
                row = conn.execute("""SELECT
                    COUNT(*) as nodes,
                    SUM(items_ingested) as items,
                    SUM(bytes_stored) as stored,
                    SUM(bytes_served) as served,
                    SUM(queries_answered) as queries,
                    SUM(score) as score,
                    MIN(first_seen) as first,
                    MAX(last_seen) as last
                    FROM node_scores WHERE owner_user=? AND opted_in=1""",
                    (username,)).fetchone()
                now = time.time()
                conn.execute("""INSERT INTO user_scores
                    (username, opted_in, total_nodes, total_items, total_bytes_stored,
                     total_bytes_served, total_queries, total_score, first_seen, last_seen)
                    VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(username) DO UPDATE SET
                    total_nodes=?, total_items=?, total_bytes_stored=?,
                    total_bytes_served=?, total_queries=?, total_score=?,
                    last_seen=?""",
                    (username, row[0], row[1] or 0, row[2] or 0, row[3] or 0,
                     row[4] or 0, row[5] or 0, row[6] or now, row[7] or now,
                     row[0], row[1] or 0, row[2] or 0, row[3] or 0,
                     row[4] or 0, row[5] or 0, row[7] or now))

    def refresh_group_scores(self):
        """Aggregate member scores into group scores."""
        with sqlite3.connect(self.db_path) as conn:
            groups = conn.execute("SELECT group_id FROM groups").fetchall()
            for (gid,) in groups:
                row = conn.execute("""SELECT COALESCE(SUM(us.total_score), 0)
                    FROM group_members gm
                    JOIN user_scores us ON gm.username = us.username
                    WHERE gm.group_id=?""", (gid,)).fetchone()
                conn.execute("UPDATE groups SET total_score=? WHERE group_id=?",
                             (row[0], gid))

    # --- Leaderboards ---

    def get_leaderboard(self, board_type: str = "nodes", limit: int = 20,
                        period: str = "all") -> list[dict]:
        """Get leaderboard. board_type: nodes|users|groups"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            if board_type == "nodes":
                rows = conn.execute("""SELECT node_id, owner_user, score,
                    items_ingested, bytes_stored, bytes_served, streak_days,
                    uptime_seconds, max_peers, display_alias, anonymous
                    FROM node_scores WHERE opted_in=1
                    ORDER BY score DESC LIMIT ?""", (limit,)).fetchall()
                return [{"rank": i+1,
                         "node_id": r["display_alias"] if r["anonymous"] else r["node_id"],
                         "owner": "" if r["anonymous"] else r["owner_user"],
                         "anonymous": bool(r["anonymous"]),
                         "score": r["score"],
                         "items": r["items_ingested"],
                         "gb_stored": round((r["bytes_stored"] or 0) / (1024**3), 1),
                         "gb_served": round((r["bytes_served"] or 0) / (1024**3), 1),
                         "streak": r["streak_days"], "peers": r["max_peers"],
                         "uptime_days": round((r["uptime_seconds"] or 0) / 86400, 1)}
                        for i, r in enumerate(rows)]
            elif board_type == "users":
                self.refresh_user_scores()
                rows = conn.execute("""SELECT username, display_name, total_score,
                    total_nodes, total_items, total_bytes_stored, total_bytes_served
                    FROM user_scores WHERE opted_in=1
                    ORDER BY total_score DESC LIMIT ?""", (limit,)).fetchall()
                return [{"rank": i+1, "username": r["username"],
                         "display_name": r["display_name"],
                         "score": r["total_score"], "nodes": r["total_nodes"],
                         "items": r["total_items"],
                         "gb_stored": round((r["total_bytes_stored"] or 0) / (1024**3), 1),
                         "gb_served": round((r["total_bytes_served"] or 0) / (1024**3), 1)}
                        for i, r in enumerate(rows)]
            elif board_type == "groups":
                self.refresh_group_scores()
                rows = conn.execute("""SELECT g.group_id, g.group_name, g.total_score,
                    g.description, COUNT(gm.username) as members
                    FROM groups g
                    LEFT JOIN group_members gm ON g.group_id = gm.group_id
                    GROUP BY g.group_id
                    ORDER BY g.total_score DESC LIMIT ?""", (limit,)).fetchall()
                return [{"rank": i+1, "group_id": r["group_id"],
                         "group_name": r["group_name"], "score": r["total_score"],
                         "members": r["members"], "description": r["description"]}
                        for i, r in enumerate(rows)]
            return []

    def get_node_profile(self, node_id: str) -> Optional[dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            node = conn.execute("SELECT * FROM node_scores WHERE node_id=? AND opted_in=1",
                                (node_id,)).fetchone()
            if not node:
                return None
            achs = conn.execute("""SELECT a.achievement_id, a.earned_at
                FROM achievements a WHERE a.entity_type='node' AND a.entity_id=?
                ORDER BY a.earned_at""", (node_id,)).fetchall()
            ach_list = []
            for a in achs:
                defn = next((x for x in ACHIEVEMENTS if x["id"] == a["achievement_id"]), None)
                if defn:
                    ach_list.append({**defn, "earned_at": a["earned_at"]})
            is_anon = bool(node.get("anonymous", 0))
            return {
                "node_id": node.get("display_alias") or node["node_id"] if is_anon else node["node_id"],
                "owner": "" if is_anon else node["owner_user"],
                "anonymous": is_anon,
                "group": node["group_id"],
                "score": node["score"],
                "items_ingested": node["items_ingested"],
                "gb_stored": round((node["bytes_stored"] or 0) / (1024**3), 1),
                "gb_served": round((node["bytes_served"] or 0) / (1024**3), 1),
                "queries": node["queries_answered"],
                "streak_days": node["streak_days"],
                "uptime_days": round((node["uptime_seconds"] or 0) / 86400, 1),
                "max_peers": node["max_peers"],
                "continents": [c for c in node["continents"].split(",") if c],
                "achievements": ach_list,
                "first_seen": node["first_seen"],
            }

    def get_user_profile(self, username: str) -> Optional[dict]:
        self.refresh_user_scores()
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            user = conn.execute("SELECT * FROM user_scores WHERE username=? AND opted_in=1",
                                (username,)).fetchone()
            if not user:
                return None
            nodes = conn.execute("""SELECT node_id, score, streak_days,
                items_ingested, bytes_stored
                FROM node_scores WHERE owner_user=? AND opted_in=1
                ORDER BY score DESC""", (username,)).fetchall()
            # Collect achievements across all nodes
            node_ids = [n["node_id"] for n in nodes]
            all_achs = set()
            if node_ids:
                placeholders = ",".join("?" * len(node_ids))
                achs = conn.execute(f"""SELECT DISTINCT achievement_id
                    FROM achievements WHERE entity_type='node'
                    AND entity_id IN ({placeholders})""", node_ids).fetchall()
                all_achs = set(a[0] for a in achs)
            # Groups
            groups = conn.execute("""SELECT g.group_id, g.group_name
                FROM group_members gm JOIN groups g ON gm.group_id = g.group_id
                WHERE gm.username=?""", (username,)).fetchall()

            return {
                "username": user["username"],
                "display_name": user["display_name"],
                "score": user["total_score"],
                "total_nodes": user["total_nodes"],
                "items": user["total_items"],
                "gb_stored": round((user["total_bytes_stored"] or 0) / (1024**3), 1),
                "gb_served": round((user["total_bytes_served"] or 0) / (1024**3), 1),
                "queries": user["total_queries"],
                "nodes": [{"node_id": n["node_id"], "score": n["score"],
                           "streak": n["streak_days"]} for n in nodes],
                "achievements": [a for a in ACHIEVEMENTS if a["id"] in all_achs],
                "groups": [{"group_id": g["group_id"], "group_name": g["group_name"]}
                           for g in groups],
                "first_seen": user["first_seen"],
            }

    def get_group_profile(self, group_id: str) -> Optional[dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            group = conn.execute("SELECT * FROM groups WHERE group_id=?",
                                 (group_id,)).fetchone()
            if not group:
                return None
            members = conn.execute("""SELECT gm.username, gm.role, gm.joined_at,
                COALESCE(us.total_score, 0) as score
                FROM group_members gm
                LEFT JOIN user_scores us ON gm.username = us.username
                WHERE gm.group_id=?
                ORDER BY score DESC""", (group_id,)).fetchall()
            return {
                "group_id": group["group_id"],
                "group_name": group["group_name"],
                "description": group["description"],
                "score": group["total_score"],
                "created_by": group["created_by"],
                "members": [{"username": m["username"], "role": m["role"],
                             "score": m["score"]} for m in members],
            }

    # --- Activity Feed ---

    def get_feed(self, limit: int = 50) -> list[dict]:
        """Recent activity feed."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""SELECT * FROM activity_feed
                ORDER BY timestamp DESC LIMIT ?""", (limit,)).fetchall()
            return [{"timestamp": r["timestamp"], "node_id": r["node_id"],
                     "username": r["username"], "event_type": r["event_type"],
                     "detail": r["detail"], "score_delta": r["score_delta"]}
                    for r in rows]

    # --- Network Stats ---

    def network_stats(self) -> dict:
        """Aggregate gamification stats for the whole network."""
        with sqlite3.connect(self.db_path) as conn:
            nodes = conn.execute(
                "SELECT COUNT(*) FROM node_scores WHERE opted_in=1").fetchone()[0]
            users = conn.execute(
                "SELECT COUNT(*) FROM user_scores WHERE opted_in=1").fetchone()[0]
            groups = conn.execute(
                "SELECT COUNT(*) FROM groups").fetchone()[0]
            total_score = conn.execute(
                "SELECT COALESCE(SUM(score), 0) FROM node_scores WHERE opted_in=1").fetchone()[0]
            total_achs = conn.execute(
                "SELECT COUNT(*) FROM achievements").fetchone()[0]
            return {
                "opted_in_nodes": nodes,
                "opted_in_users": users,
                "groups": groups,
                "total_network_score": total_score,
                "total_achievements_earned": total_achs,
                "available_achievements": len(ACHIEVEMENTS),
            }

    def cleanup(self, retain_days: int = 90):
        """Remove old feed entries."""
        cutoff = time.time() - (retain_days * 86400)
        with sqlite3.connect(self.db_path) as conn:
            c = conn.execute("DELETE FROM activity_feed WHERE timestamp < ?",
                             (cutoff,)).rowcount
            if c:
                logger.info(f"Gamification cleanup: removed {c} feed entries (>{retain_days}d)")
