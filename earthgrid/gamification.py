"""EarthGrid Gamification — Node, User & Group performance tracking.

Opt-in gamification layer: tracks contributions, awards achievements,
and maintains leaderboards at three levels:
  - Node: individual node performance
  - User: aggregated across all their nodes
  - Group: teams of users contributing together

Privacy: all nodes participate by default (anonymous).
Display details (user name, node name) are opt-in.
Node stats are always visible with node_id only.
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
                storage_pledged_gb REAL NOT NULL DEFAULT 0,
                score INTEGER NOT NULL DEFAULT 0,
                display_alias TEXT NOT NULL DEFAULT '',
                anonymous INTEGER NOT NULL DEFAULT 0,
                sponsor_name TEXT NOT NULL DEFAULT '',
                sponsor_url TEXT NOT NULL DEFAULT '',
                node_url TEXT NOT NULL DEFAULT ''
            )""")
            # Migrate: add new columns if missing
            existing = {r[1] for r in conn.execute("PRAGMA table_info(node_scores)").fetchall()}
            for col in ["sponsor_name", "sponsor_url", "node_url"]:
                if col not in existing:
                    conn.execute(f"ALTER TABLE node_scores ADD COLUMN {col} TEXT NOT NULL DEFAULT ''")

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
        self._init_challenges_db()

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

            # Score is recalculated from current state (not incremental)
            # Weights: uptime (10/day) > pledged storage (5/GB) > items (1) > served (3/GB) > queries (1/100)
            _cur = conn.execute(
                "SELECT items_ingested, bytes_served, queries_answered, uptime_seconds, streak_days, storage_pledged_gb FROM node_scores WHERE node_id=?",
                (node_id,)).fetchone()
            if _cur:
                _items = (_cur[0] or 0) + items_delta
                _gb_served = ((_cur[1] or 0) + bytes_served_delta) / (1024**3)
                _queries = (_cur[2] or 0) + queries_delta
                _uptime_days = (_cur[3] or 0) / 86400
                _streak = _cur[4] or 0
                _gb_pledged = _cur[5] or 0
                new_score = int(
                    _uptime_days * 10 +      # 10 pts per day online
                    _gb_pledged * 5 +         # 5 pts per GB pledged to network
                    _items * 1 +              # 1 pt per item
                    _gb_served * 3 +          # 3 pts per GB served
                    _queries // 100 +         # 1 pt per 100 queries
                    _streak * 2              # 2 pts per streak day
                )
                updates.append("score = ?")
                params.append(new_score)

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
                         uptime_seconds: float = 0, storage_pledged_gb: float = 0):
        """Record a node heartbeat (called periodically by federation sync)."""
        now = time.time()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""UPDATE node_scores SET
                last_seen=?, uptime_seconds=?, max_peers=MAX(max_peers, ?),
                storage_pledged_gb=MAX(storage_pledged_gb, ?)
                WHERE node_id=?""",
                (now, uptime_seconds, peers_count, storage_pledged_gb or 0, node_id))
            # Streak update
            today = time.strftime("%Y-%m-%d", time.gmtime())
            row = conn.execute(
                "SELECT streak_last_date, streak_days, items_ingested, bytes_stored, bytes_served, queries_answered FROM node_scores WHERE node_id=?",
                (node_id,)).fetchone()
            if row:
                if row[0] != today:
                    yesterday = time.strftime("%Y-%m-%d", time.gmtime(now - 86400))
                    new_streak = (row[1] + 1) if row[0] == yesterday else 1
                    conn.execute(
                        "UPDATE node_scores SET streak_days=?, streak_last_date=? WHERE node_id=?",
                        (new_streak, today, node_id))
                else:
                    new_streak = row[1]
                # Recalculate score from current state
                _uptime_days = uptime_seconds / 86400
                _gb_pledged = storage_pledged_gb or 0
                _gb_served = (row[4] or 0) / (1024**3)
                score = int(
                    _uptime_days * 10 +       # 10 pts per day online
                    _gb_pledged * 5 +          # 5 pts per GB pledged to network
                    (row[2] or 0) * 1 +       # 1 pt per item
                    _gb_served * 3 +           # 3 pts per GB served
                    (row[5] or 0) // 100 +    # 1 pt per 100 queries
                    new_streak * 2             # 2 pts per streak day
                )
                conn.execute("UPDATE node_scores SET score=? WHERE node_id=?",
                             (score, node_id))

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


    def ensure_node_registered(self, node_id: str, node_name: str = "",
                               sponsor_name: str = "", sponsor_url: str = "",
                               node_url: str = ""):
        """Ensure node exists in gamification DB (auto-registered, anonymous by default)."""
        now = int(time.time())
        with sqlite3.connect(self.db_path) as conn:
            existing = conn.execute("SELECT node_id FROM node_scores WHERE node_id=?",
                                     (node_id,)).fetchone()
            if not existing:
                conn.execute("""INSERT INTO node_scores
                    (node_id, opted_in, anonymous, display_alias, first_seen, last_seen,
                     sponsor_name, sponsor_url, node_url)
                    VALUES (?, 1, 1, ?, ?, ?, ?, ?, ?)""",
                    (node_id, node_name or node_id, now, now,
                     sponsor_name, sponsor_url, node_url))
                logger.info(f"Auto-registered node {node_id} in gamification")
            else:
                updates = ["last_seen=?"]
                params = [now]
                if node_name:
                    updates.append("display_alias=?")
                    params.append(node_name)
                if sponsor_name:
                    updates.append("sponsor_name=?")
                    params.append(sponsor_name)
                if sponsor_url:
                    updates.append("sponsor_url=?")
                    params.append(sponsor_url)
                if node_url:
                    updates.append("node_url=?")
                    params.append(node_url)
                params.append(node_id)
                conn.execute(f"UPDATE node_scores SET {','.join(updates)} WHERE node_id=?",
                             params)

    def get_leaderboard(self, board_type: str = "nodes", limit: int = 20,
                        period: str = "all") -> list[dict]:
        """Get leaderboard. board_type: nodes|users|groups"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            if board_type == "nodes":
                rows = conn.execute("""SELECT node_id, owner_user, score,
                    items_ingested, bytes_stored, bytes_served, streak_days,
                    uptime_seconds, max_peers, display_alias, anonymous,
                    storage_pledged_gb, group_id,
                    COALESCE(sponsor_name, '') as sponsor_name,
                    COALESCE(sponsor_url, '') as sponsor_url,
                    COALESCE(node_url, '') as node_url
                    FROM node_scores
                    ORDER BY score DESC LIMIT ?""", (limit,)).fetchall()
                return [{"rank": i+1,
                         "node_id": r["node_id"],
                         "node_name": r["display_alias"] or r["node_id"],
                         "owner": "" if r["anonymous"] else (r["owner_user"] or ""),
                         "anonymous": bool(r["anonymous"]),
                         "score": r["score"],
                         "items": r["items_ingested"],
                         "gb_stored": round((r["bytes_stored"] or 0) / (1024**3), 1),
                         "gb_pledged": r["storage_pledged_gb"],
                         "gb_served": round((r["bytes_served"] or 0) / (1024**3), 1),
                         "streak": r["streak_days"], "peers": r["max_peers"],
                         "uptime_days": round((r["uptime_seconds"] or 0) / 86400, 1),
                         "group": r["group_id"] or "",
                         "sponsor_name": r["sponsor_name"],
                         "sponsor_url": r["sponsor_url"],
                         "node_url": r["node_url"]}
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
            node = conn.execute("SELECT * FROM node_scores WHERE node_id=?",
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
                "SELECT COUNT(*) FROM node_scores").fetchone()[0]
            users = conn.execute(
                "SELECT COUNT(*) FROM user_scores WHERE opted_in=1").fetchone()[0]
            groups = conn.execute(
                "SELECT COUNT(*) FROM groups").fetchone()[0]
            total_score = conn.execute(
                "SELECT COALESCE(SUM(score), 0) FROM node_scores").fetchone()[0]
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


    def economy_health(self) -> dict:
        """Compute network economy health indicator.
        
        Factors:
        1. Storage headroom (30%): pledged_total >> stored_total means room to grow
        2. Node diversity (25%): more active nodes = more resilient
        3. Data redundancy (25%): items replicated across multiple nodes
        4. Data serving (20%): bytes_served > 0 means actual reuse
        
        Returns dict with score (0-100), status (green/yellow/red), and breakdown.
        """
        import time as _time
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT node_id, items_ingested, bytes_stored, bytes_served, "
                "storage_pledged_gb, last_seen, uptime_seconds FROM node_scores"
            ).fetchall()
        
        if not rows:
            return {
                "score": 0, "status": "red", "label": "No nodes",
                "factors": {}, "nodes_total": 0, "nodes_alive": 0,
            }
        
        now = _time.time()
        alive_threshold = 300  # 5 min
        
        nodes_total = len(rows)
        nodes_alive = sum(1 for r in rows if (now - r[5]) < alive_threshold)
        
        total_stored_gb = sum((r[2] or 0) for r in rows) / (1024**3)
        total_pledged_gb = sum((r[4] or 0) for r in rows)
        total_served_gb = sum((r[3] or 0) for r in rows) / (1024**3)
        total_items = max(sum((r[1] or 0) for r in rows), 1)
        
        # -- Factor 1: Storage Headroom (30%) --
        # Score high if plenty of room to grow
        if total_pledged_gb <= 0:
            storage_score = 0
        else:
            utilization = total_stored_gb / total_pledged_gb
            if utilization < 0.01:
                storage_score = 20  # barely used = not great either
            elif utilization < 0.5:
                storage_score = 100  # sweet spot: room to grow
            elif utilization < 0.8:
                storage_score = 70  # getting full
            elif utilization < 0.95:
                storage_score = 40  # tight
            else:
                storage_score = 10  # critically full
        
        # -- Factor 2: Node Diversity (25%) --
        # More alive nodes = better. Scale: 1=poor, 3=ok, 5+=great
        if nodes_alive <= 0:
            diversity_score = 0
        elif nodes_alive == 1:
            diversity_score = 20  # single point of failure
        elif nodes_alive == 2:
            diversity_score = 50
        elif nodes_alive <= 4:
            diversity_score = 75
        elif nodes_alive <= 10:
            diversity_score = 90
        else:
            diversity_score = 100
        
        # -- Factor 3: Data Redundancy (25%) --
        # How many nodes hold copies of the same items
        # Approximate: count unique items across nodes vs. sum of items
        # If sum >> unique, there's redundancy
        items_per_node = [(r[1] or 0) for r in rows if (now - r[5]) < alive_threshold]
        sum_items = sum(items_per_node)
        if sum_items <= 0 or not items_per_node:
            redundancy_score = 0
        else:
            max_unique = max(items_per_node)  # the node with most items
            if max_unique <= 0:
                redundancy_score = 0
            else:
                # replication factor estimate: sum_items / max_unique
                est_replication = sum_items / max_unique
                if est_replication >= 3:
                    redundancy_score = 100
                elif est_replication >= 2:
                    redundancy_score = 80
                elif est_replication >= 1.5:
                    redundancy_score = 60
                elif est_replication > 1:
                    redundancy_score = 40
                else:
                    redundancy_score = 15  # no redundancy
        
        # -- Factor 4: Data Reuse / Serving (20%) --
        # Any data being served = the network has value
        if total_served_gb <= 0:
            reuse_score = 5  # network exists but no one is using it yet
        elif total_served_gb < 1:
            reuse_score = 30
        elif total_served_gb < total_stored_gb * 0.01:
            reuse_score = 50
        elif total_served_gb < total_stored_gb * 0.1:
            reuse_score = 75
        else:
            reuse_score = 100  # great reuse
        
        # -- Composite Score --
        score = round(
            storage_score * 0.30 +
            diversity_score * 0.25 +
            redundancy_score * 0.25 +
            reuse_score * 0.20
        )
        score = max(0, min(100, score))
        
        if score >= 70:
            status = "green"
            label = "Healthy"
        elif score >= 40:
            status = "yellow"
            label = "Growing"
        else:
            status = "red"
            label = "Needs attention"
        
        return {
            "score": score,
            "status": status,
            "label": label,
            "nodes_total": nodes_total,
            "nodes_alive": nodes_alive,
            "storage_pledged_gb": round(total_pledged_gb, 1),
            "storage_used_gb": round(total_stored_gb, 1),
            "storage_utilization_pct": round((total_stored_gb / total_pledged_gb * 100) if total_pledged_gb > 0 else 0, 1),
            "data_served_gb": round(total_served_gb, 1),
            "estimated_replication_factor": round(sum_items / max(max(items_per_node) if items_per_node else 1, 1), 2),
            "factors": {
                "storage_headroom": {"score": storage_score, "weight": 0.30},
                "node_diversity": {"score": diversity_score, "weight": 0.25},
                "data_redundancy": {"score": redundancy_score, "weight": 0.25},
                "data_reuse": {"score": reuse_score, "weight": 0.20},
            },
        }

    def cleanup(self, retain_days: int = 90):
        """Remove old feed entries."""
        cutoff = time.time() - (retain_days * 86400)
        with sqlite3.connect(self.db_path) as conn:
            c = conn.execute("DELETE FROM activity_feed WHERE timestamp < ?",
                             (cutoff,)).rowcount
            if c:
                logger.info(f"Gamification cleanup: removed {c} feed entries (>{retain_days}d)")

    # ===== Challenges System =====

    def _init_challenges_db(self):
        """Create challenges tables (called from _init_db)."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS challenges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                metric TEXT NOT NULL,
                period TEXT NOT NULL DEFAULT 'weekly',
                start_ts REAL NOT NULL,
                end_ts REAL NOT NULL,
                created_at REAL NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS challenge_entries (
                challenge_id INTEGER NOT NULL,
                node_id TEXT NOT NULL,
                score REAL NOT NULL DEFAULT 0,
                last_updated REAL NOT NULL DEFAULT 0,
                PRIMARY KEY (challenge_id, node_id)
            )""")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ch_active ON challenges(active)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_che_cid ON challenge_entries(challenge_id)")

    # Challenge templates
    CHALLENGE_TEMPLATES = [
        {"title": "Weekly Ingest Champion", "description": "Most items ingested this week",
         "metric": "items_ingested", "period": "weekly"},
        {"title": "Storage Hero", "description": "Most GB added this week",
         "metric": "gb_stored", "period": "weekly"},
        {"title": "Serving Star", "description": "Most GB served this week",
         "metric": "gb_served", "period": "weekly"},
        {"title": "Monthly Marathon", "description": "Highest total score this month",
         "metric": "score", "period": "monthly"},
    ]

    def seed_challenges(self):
        """Create initial challenges for current period if none exist."""
        self._init_challenges_db()
        now = time.time()
        with sqlite3.connect(self.db_path) as conn:
            active = conn.execute("SELECT COUNT(*) FROM challenges WHERE active=1").fetchone()[0]
            if active > 0:
                return  # already seeded

        # Create weekly + monthly
        import datetime
        today = datetime.datetime.utcfromtimestamp(now)
        # Weekly: Mon-Sun
        weekday = today.weekday()
        week_start = datetime.datetime(today.year, today.month, today.day) - datetime.timedelta(days=weekday)
        week_end = week_start + datetime.timedelta(days=7)
        # Monthly: 1st-last
        month_start = datetime.datetime(today.year, today.month, 1)
        if today.month == 12:
            month_end = datetime.datetime(today.year + 1, 1, 1)
        else:
            month_end = datetime.datetime(today.year, today.month + 1, 1)

        with sqlite3.connect(self.db_path) as conn:
            for tmpl in self.CHALLENGE_TEMPLATES:
                if tmpl["period"] == "weekly":
                    start = week_start.timestamp()
                    end = week_end.timestamp()
                else:
                    start = month_start.timestamp()
                    end = month_end.timestamp()
                conn.execute(
                    """INSERT INTO challenges (title, description, metric, period, start_ts, end_ts, created_at, active)
                       VALUES (?, ?, ?, ?, ?, ?, ?, 1)""",
                    (tmpl["title"], tmpl["description"], tmpl["metric"], tmpl["period"],
                     start, end, now))
        logger.info(f"Seeded {len(self.CHALLENGE_TEMPLATES)} challenges")

    def rotate_challenges(self):
        """Close expired challenges and create new ones."""
        self._init_challenges_db()
        now = time.time()
        with sqlite3.connect(self.db_path) as conn:
            expired = conn.execute(
                "SELECT id, period FROM challenges WHERE active=1 AND end_ts < ?", (now,)
            ).fetchall()
            for ch_id, period in expired:
                conn.execute("UPDATE challenges SET active=0 WHERE id=?", (ch_id,))
                logger.info(f"Challenge {ch_id} expired ({period})")

        if expired:
            self.seed_challenges()

    def get_active_challenges(self) -> list[dict]:
        """Get active challenges with top-3 entries."""
        self._init_challenges_db()
        now = time.time()
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            challenges = conn.execute(
                "SELECT * FROM challenges WHERE active=1 ORDER BY end_ts"
            ).fetchall()
            result = []
            for ch in challenges:
                top3 = conn.execute(
                    """SELECT ce.node_id, ce.score,
                              COALESCE(ns.display_alias, ns.owner_user, ce.node_id) as display_name,
                              ns.group_id as node_group
                       FROM challenge_entries ce
                       LEFT JOIN node_scores ns ON ce.node_id = ns.node_id
                       WHERE ce.challenge_id = ?
                       ORDER BY ce.score DESC LIMIT 3""",
                    (ch["id"],)
                ).fetchall()
                total_participants = conn.execute(
                    "SELECT COUNT(*) FROM challenge_entries WHERE challenge_id=? AND score > 0",
                    (ch["id"],)
                ).fetchone()[0]
                remaining_s = max(0, ch["end_ts"] - now)
                result.append({
                    "id": ch["id"],
                    "title": ch["title"],
                    "description": ch["description"],
                    "metric": ch["metric"],
                    "period": ch["period"],
                    "start_ts": ch["start_ts"],
                    "end_ts": ch["end_ts"],
                    "remaining_seconds": remaining_s,
                    "remaining_human": _format_duration(remaining_s),
                    "participants": total_participants,
                    "top3": [{"node_id": r["node_id"], "display_name": r["display_name"],
                              "score": r["score"], "group": r["node_group"] or ""} for r in top3],
                })
            return result

    def get_challenge_results(self, challenge_id: int) -> dict:
        """Full ranking for a challenge."""
        self._init_challenges_db()
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            ch = conn.execute("SELECT * FROM challenges WHERE id=?", (challenge_id,)).fetchone()
            if not ch:
                return {"error": "Challenge not found"}
            entries = conn.execute(
                """SELECT ce.node_id, ce.score,
                          COALESCE(ns.display_alias, ns.owner_user, ce.node_id) as display_name,
                          ns.group_id as node_group
                   FROM challenge_entries ce
                   LEFT JOIN node_scores ns ON ce.node_id = ns.node_id
                   WHERE ce.challenge_id = ?
                   ORDER BY ce.score DESC""",
                (challenge_id,)
            ).fetchall()
            return {
                "id": ch["id"],
                "title": ch["title"],
                "description": ch["description"],
                "metric": ch["metric"],
                "period": ch["period"],
                "active": bool(ch["active"]),
                "rankings": [{"rank": i+1, "node_id": r["node_id"],
                               "display_name": r["display_name"],
                               "score": r["score"], "group": r["node_group"] or ""}
                              for i, r in enumerate(entries)],
            }

    def update_challenge_scores(self):
        """Refresh all active challenge scores from node_scores."""
        self._init_challenges_db()
        now = time.time()
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            challenges = conn.execute(
                "SELECT id, metric, start_ts FROM challenges WHERE active=1"
            ).fetchall()
            nodes = conn.execute("SELECT * FROM node_scores").fetchall()
            for ch in challenges:
                metric = ch["metric"]
                for node in nodes:
                    # Calculate score based on metric
                    if metric == "items_ingested":
                        score = node["items_ingested"]
                    elif metric == "gb_stored":
                        score = round(node["bytes_stored"] / (1024**3), 2)
                    elif metric == "gb_served":
                        score = round(node["bytes_served"] / (1024**3), 2)
                    elif metric == "score":
                        score = node["score"]
                    else:
                        score = 0
                    if score > 0:
                        conn.execute(
                            """INSERT INTO challenge_entries (challenge_id, node_id, score, last_updated)
                               VALUES (?, ?, ?, ?)
                               ON CONFLICT(challenge_id, node_id) DO UPDATE SET score=?, last_updated=?""",
                            (ch["id"], node["node_id"], score, now, score, now))


def _format_duration(seconds: float) -> str:
    """Format seconds to human-readable duration."""
    if seconds <= 0:
        return "ended"
    d = int(seconds // 86400)
    h = int((seconds % 86400) // 3600)
    m = int((seconds % 3600) // 60)
    if d > 0:
        return f"{d}d {h}h"
    if h > 0:
        return f"{h}h {m}m"
    return f"{m}m"
