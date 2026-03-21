//! EarthGrid Gamification — Node, User & Group performance tracking.
//!
//! Opt-in gamification layer: tracks contributions, awards achievements,
//! and maintains leaderboards at three levels:
//!   - Node: individual node performance
//!   - User: aggregated across all their nodes
//!   - Group: teams of users contributing together
//!
//! Schema is kept identical to the Python gamification.py for compatibility.
//! WAL mode + busy_timeout handle concurrent Python/Rust access safely.

use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Mutex;

use crate::error::Result;

// ---------------------------------------------------------------------------
// Achievement definitions (mirror of Python ACHIEVEMENTS list)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AchievementDef {
    pub id: &'static str,
    pub icon: &'static str,
    pub name: &'static str,
    pub desc: &'static str,
    pub threshold: f64,
    pub field: &'static str,
}

pub const ACHIEVEMENTS: &[AchievementDef] = &[
    AchievementDef { id: "first_seed",     icon: "🌱", name: "First Seed",     desc: "Ingested your first data",           threshold: 1.0,    field: "items_ingested" },
    AchievementDef { id: "mesh_pioneer",   icon: "🤝", name: "Mesh Pioneer",   desc: "Connected to 10+ peers",             threshold: 10.0,   field: "max_peers" },
    AchievementDef { id: "always_on",      icon: "💪", name: "Always On",      desc: "30 consecutive days online",         threshold: 30.0,   field: "streak_days" },
    AchievementDef { id: "terabyte_club",  icon: "🏔️", name: "Terabyte Club",  desc: "Stored 1 TB+ of data",              threshold: 1024.0, field: "gb_stored" },
    AchievementDef { id: "data_relay",     icon: "📡", name: "Data Relay",     desc: "Served 100 GB+ to other nodes",      threshold: 100.0,  field: "gb_served" },
    AchievementDef { id: "speed_demon",    icon: "🔥", name: "Speed Demon",    desc: "Fastest query response in network",  threshold: 1.0,    field: "speed_rank" },
    AchievementDef { id: "power_node",     icon: "⚡", name: "Power Node",     desc: "Top 3 in any category for 7+ days", threshold: 7.0,    field: "top3_streak" },
    AchievementDef { id: "centurion",      icon: "💯", name: "Centurion",      desc: "100+ days total uptime",             threshold: 100.0,  field: "uptime_days" },
    AchievementDef { id: "petabyte_dream", icon: "🚀", name: "Petabyte Dream", desc: "Served 1 TB+ total",                threshold: 1024.0, field: "gb_served" },
];

// ---------------------------------------------------------------------------
// Challenge templates
// ---------------------------------------------------------------------------

struct ChallengeTmpl {
    title: &'static str,
    description: &'static str,
    metric: &'static str,
    period: &'static str,
}

const CHALLENGE_TEMPLATES: &[ChallengeTmpl] = &[
    ChallengeTmpl { title: "Weekly Ingest Champion", description: "Most items ingested this week",   metric: "items_ingested", period: "weekly" },
    ChallengeTmpl { title: "Storage Hero",           description: "Most GB added this week",          metric: "gb_stored",      period: "weekly" },
    ChallengeTmpl { title: "Serving Star",           description: "Most GB served this week",         metric: "gb_served",      period: "weekly" },
    ChallengeTmpl { title: "Monthly Marathon",       description: "Highest total score this month",   metric: "score",          period: "monthly" },
];

// ---------------------------------------------------------------------------
// Return types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct LeaderboardEntry {
    pub rank: usize,
    // node fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub anonymous: Option<bool>,
    // user fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nodes: Option<i64>,
    // group fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub members: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    // common
    pub score: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gb_stored: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gb_served: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gb_pledged: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub streak: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peers: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uptime_days: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sponsor_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sponsor_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AchievementEntry {
    pub id: String,
    pub icon: String,
    pub name: String,
    pub desc: String,
    pub earned_at: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeProfile {
    pub node_id: String,
    pub owner: String,
    pub anonymous: bool,
    pub group: String,
    pub score: i64,
    pub items_ingested: i64,
    pub gb_stored: f64,
    pub gb_served: f64,
    pub queries: i64,
    pub streak_days: i64,
    pub uptime_days: f64,
    pub max_peers: i64,
    pub continents: Vec<String>,
    pub achievements: Vec<AchievementEntry>,
    pub first_seen: f64,
    pub sponsor_name: String,
    pub sponsor_url: String,
    pub node_url: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FeedEntry {
    pub timestamp: f64,
    pub node_id: String,
    pub username: String,
    pub event_type: String,
    pub detail: String,
    pub score_delta: i64,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct NetworkStats {
    pub opted_in_nodes: i64,
    pub opted_in_users: i64,
    pub groups: i64,
    pub total_network_score: i64,
    pub total_achievements_earned: i64,
    pub available_achievements: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FactorScore {
    pub score: f64,
    pub weight: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EconomyHealthFactors {
    pub storage_headroom: FactorScore,
    pub node_diversity: FactorScore,
    pub data_redundancy: FactorScore,
    pub data_reuse: FactorScore,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EconomyHealth {
    pub score: i64,
    pub status: String,
    pub label: String,
    pub nodes_total: usize,
    pub nodes_alive: usize,
    pub storage_pledged_gb: f64,
    pub storage_used_gb: f64,
    pub storage_utilization_pct: f64,
    pub data_served_gb: f64,
    pub estimated_replication_factor: f64,
    pub factors: EconomyHealthFactors,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChallengeTop3Entry {
    pub node_id: String,
    pub display_name: String,
    pub score: f64,
    pub group: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Challenge {
    pub id: i64,
    pub title: String,
    pub description: String,
    pub metric: String,
    pub period: String,
    pub start_ts: f64,
    pub end_ts: f64,
    pub remaining_seconds: f64,
    pub remaining_human: String,
    pub participants: i64,
    pub top3: Vec<ChallengeTop3Entry>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChallengeRankEntry {
    pub rank: usize,
    pub node_id: String,
    pub display_name: String,
    pub score: f64,
    pub group: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChallengeResults {
    pub id: i64,
    pub title: String,
    pub description: String,
    pub metric: String,
    pub period: String,
    pub active: bool,
    pub rankings: Vec<ChallengeRankEntry>,
}

// ---------------------------------------------------------------------------
// GamificationEngine
// ---------------------------------------------------------------------------

pub struct GamificationEngine {
    conn: Mutex<Connection>,
}

impl GamificationEngine {
    /// Open or create the gamification DB, initialise tables, enable WAL.
    pub fn new(db_path: &Path) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(db_path)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA busy_timeout=10000;
             PRAGMA synchronous=NORMAL;",
        )?;
        let engine = Self { conn: Mutex::new(conn) };
        engine.init_tables()?;
        Ok(engine)
    }

    fn init_tables(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS node_scores (
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
            );
            CREATE TABLE IF NOT EXISTS user_scores (
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
            );
            CREATE TABLE IF NOT EXISTS groups (
                group_id TEXT PRIMARY KEY,
                group_name TEXT UNIQUE NOT NULL,
                created_by TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL DEFAULT 0,
                description TEXT NOT NULL DEFAULT '',
                total_score INTEGER NOT NULL DEFAULT 0,
                display_alias TEXT NOT NULL DEFAULT '',
                anonymous INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS group_members (
                group_id TEXT NOT NULL,
                username TEXT NOT NULL,
                joined_at REAL NOT NULL DEFAULT 0,
                role TEXT NOT NULL DEFAULT 'member',
                PRIMARY KEY (group_id, username)
            );
            CREATE TABLE IF NOT EXISTS achievements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT NOT NULL DEFAULT 'node',
                entity_id TEXT NOT NULL,
                achievement_id TEXT NOT NULL,
                earned_at REAL NOT NULL,
                UNIQUE(entity_type, entity_id, achievement_id)
            );
            CREATE TABLE IF NOT EXISTS activity_feed (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                node_id TEXT NOT NULL DEFAULT '',
                username TEXT NOT NULL DEFAULT '',
                event_type TEXT NOT NULL DEFAULT '',
                detail TEXT NOT NULL DEFAULT '',
                score_delta INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS challenges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                metric TEXT NOT NULL,
                period TEXT NOT NULL DEFAULT 'weekly',
                start_ts REAL NOT NULL,
                end_ts REAL NOT NULL,
                created_at REAL NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS challenge_entries (
                challenge_id INTEGER NOT NULL,
                node_id TEXT NOT NULL,
                score REAL NOT NULL DEFAULT 0,
                last_updated REAL NOT NULL DEFAULT 0,
                PRIMARY KEY (challenge_id, node_id)
            );
            CREATE INDEX IF NOT EXISTS idx_feed_ts    ON activity_feed(timestamp);
            CREATE INDEX IF NOT EXISTS idx_node_owner ON node_scores(owner_user);
            CREATE INDEX IF NOT EXISTS idx_node_group ON node_scores(group_id);
            CREATE INDEX IF NOT EXISTS idx_ach_entity ON achievements(entity_type, entity_id);
            CREATE INDEX IF NOT EXISTS idx_ch_active  ON challenges(active);
            CREATE INDEX IF NOT EXISTS idx_che_cid    ON challenge_entries(challenge_id);",
        )?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Node registration
    // -----------------------------------------------------------------------

    /// Ensure a node exists in the gamification DB (auto-registered, anonymous by default).
    pub fn ensure_node_registered(
        &self,
        node_id: &str,
        node_name: &str,
        sponsor_name: &str,
        sponsor_url: &str,
        node_url: &str,
    ) -> Result<()> {
        let now = unix_now();
        let conn = self.conn.lock().unwrap();
        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM node_scores WHERE node_id=?1",
                params![node_id],
                |r| r.get::<_, i64>(0),
            )
            .unwrap_or(0) > 0;

        if !exists {
            // Dedup: remove old entries with same display_alias but different node_id
            if !node_name.is_empty() {
                let removed = conn.execute(
                    "DELETE FROM node_scores WHERE display_alias = ?1 AND node_id != ?2",
                    params![node_name, node_id],
                ).unwrap_or(0);
                if removed > 0 {
                    eprintln!("Gamification: deduped {} old entry/entries for {}", removed, node_name);
                }
            }

            conn.execute(
                "INSERT INTO node_scores
                 (node_id, opted_in, anonymous, display_alias, first_seen, last_seen,
                  sponsor_name, sponsor_url, node_url)
                 VALUES (?1, 1, 1, ?2, ?3, ?3, ?4, ?5, ?6)",
                params![
                    node_id,
                    if node_name.is_empty() { node_id } else { node_name },
                    now,
                    sponsor_name,
                    sponsor_url,
                    node_url
                ],
            )?;
        } else {
            // Dedup: also clean old entries with same name but different ID
            if !node_name.is_empty() {
                let removed = conn.execute(
                    "DELETE FROM node_scores WHERE display_alias = ?1 AND node_id != ?2",
                    params![node_name, node_id],
                ).unwrap_or(0);
                if removed > 0 {
                    eprintln!("Gamification: deduped {} old entry/entries for {}", removed, node_name);
                }
            }

            let mut parts = vec!["last_seen=?1".to_string()];
            let mut idx = 2usize;
            let mut bind_vals: Vec<String> = vec![now.to_string()];

            if !node_name.is_empty() {
                parts.push(format!("display_alias=?{}", idx));
                bind_vals.push(node_name.to_string());
                idx += 1;
            }
            if !sponsor_name.is_empty() {
                parts.push(format!("sponsor_name=?{}", idx));
                bind_vals.push(sponsor_name.to_string());
                idx += 1;
            }
            if !sponsor_url.is_empty() {
                parts.push(format!("sponsor_url=?{}", idx));
                bind_vals.push(sponsor_url.to_string());
                idx += 1;
            }
            if !node_url.is_empty() {
                parts.push(format!("node_url=?{}", idx));
                bind_vals.push(node_url.to_string());
                idx += 1;
            }
            let node_id_idx = idx;
            bind_vals.push(node_id.to_string());

            let sql = format!(
                "UPDATE node_scores SET {} WHERE node_id=?{}",
                parts.join(", "),
                node_id_idx
            );
            // Use a rusqlite Statement with positional params via execute_named workaround
            // Since we build dynamic SQL we use the connection directly with params_from_iter
            conn.execute(&sql, rusqlite::params_from_iter(bind_vals.iter()))?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Event recording
    // -----------------------------------------------------------------------

    /// Record a gamification event for a node.
    pub fn record_event(
        &self,
        node_id: &str,
        event_type: &str,
        detail: &str,
        bytes_delta: i64,
        items_delta: i64,
        queries_delta: i64,
        bytes_served_delta: i64,
        peers_count: i64,
    ) -> Result<()> {
        let now = unix_now();
        let conn = self.conn.lock().unwrap();

        // Check node exists and is opted in
        let row = conn.query_row(
            "SELECT opted_in, items_ingested, bytes_stored, bytes_served, queries_answered,
                    uptime_seconds, streak_days, streak_last_date, max_peers,
                    storage_pledged_gb, anonymous, owner_user, display_alias
             FROM node_scores WHERE node_id=?1",
            params![node_id],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,   // opted_in
                    r.get::<_, i64>(1)?,   // items_ingested
                    r.get::<_, i64>(2)?,   // bytes_stored
                    r.get::<_, i64>(3)?,   // bytes_served
                    r.get::<_, i64>(4)?,   // queries_answered
                    r.get::<_, f64>(5)?,   // uptime_seconds
                    r.get::<_, i64>(6)?,   // streak_days
                    r.get::<_, String>(7)?,// streak_last_date
                    r.get::<_, i64>(8)?,   // max_peers
                    r.get::<_, f64>(9)?,   // storage_pledged_gb
                    r.get::<_, i64>(10)?,  // anonymous
                    r.get::<_, String>(11)?,// owner_user
                    r.get::<_, String>(12)?,// display_alias
                ))
            },
        );

        let (
            opted_in, cur_items, _cur_bytes, cur_bytes_served, cur_queries,
            cur_uptime, cur_streak, streak_last_date, cur_max_peers,
            storage_pledged_gb, is_anon, owner_user, display_alias,
        ) = match row {
            Ok(r) => r,
            Err(_) => return Ok(()), // node not found
        };

        if opted_in == 0 {
            return Ok(());
        }

        // Streak update
        let today = today_str();
        let (new_streak, update_streak) = if streak_last_date != today {
            let yesterday = yesterday_str(now);
            let new = if streak_last_date == yesterday && !streak_last_date.is_empty() {
                cur_streak + 1
            } else {
                1
            };
            (new, true)
        } else {
            (cur_streak, false)
        };

        // New max_peers
        let new_max_peers = if peers_count > cur_max_peers { peers_count } else { cur_max_peers };

        // Score calculation
        let new_items = cur_items + items_delta;
        let new_bytes_served = cur_bytes_served + bytes_served_delta;
        let gb_served = new_bytes_served as f64 / 1_073_741_824.0;
        let uptime_days = cur_uptime / 86400.0;
        let new_queries = cur_queries + queries_delta;
        let score = (
            uptime_days * 10.0
            + storage_pledged_gb * 5.0
            + new_items as f64 * 1.0
            + gb_served * 3.0
            + (new_queries / 100) as f64
            + new_streak as f64 * 2.0
        ) as i64;

        // Build update
        if update_streak {
            conn.execute(
                "UPDATE node_scores SET
                    items_ingested = items_ingested + ?1,
                    bytes_stored   = bytes_stored   + ?2,
                    bytes_served   = bytes_served   + ?3,
                    queries_answered = queries_answered + ?4,
                    max_peers      = ?5,
                    streak_days    = ?6,
                    streak_last_date = ?7,
                    last_seen      = ?8,
                    score          = ?9
                 WHERE node_id=?10",
                params![
                    items_delta, bytes_delta, bytes_served_delta, queries_delta,
                    new_max_peers, new_streak, today, now, score, node_id
                ],
            )?;
        } else {
            conn.execute(
                "UPDATE node_scores SET
                    items_ingested = items_ingested + ?1,
                    bytes_stored   = bytes_stored   + ?2,
                    bytes_served   = bytes_served   + ?3,
                    queries_answered = queries_answered + ?4,
                    max_peers      = ?5,
                    last_seen      = ?6,
                    score          = ?7
                 WHERE node_id=?8",
                params![
                    items_delta, bytes_delta, bytes_served_delta, queries_delta,
                    new_max_peers, now, score, node_id
                ],
            )?;
        }

        // Activity feed for meaningful events
        let interesting = matches!(event_type, "achievement" | "join" | "ingest");
        if interesting {
            let feed_node = if is_anon != 0 && !display_alias.is_empty() {
                display_alias.clone()
            } else {
                node_id.to_string()
            };
            let feed_user = if is_anon != 0 { String::new() } else { owner_user.clone() };
            conn.execute(
                "INSERT INTO activity_feed (timestamp, node_id, username, event_type, detail, score_delta)
                 VALUES (?1, ?2, ?3, ?4, ?5, 0)",
                params![now, feed_node, feed_user, event_type, detail],
            )?;
        }

        // Check achievements
        self.check_achievements_inner(&conn, node_id, &owner_user)?;
        Ok(())
    }

    /// Record a node heartbeat.
    pub fn record_heartbeat(
        &self,
        node_id: &str,
        peers_count: i64,
        uptime_seconds: f64,
        storage_pledged_gb: f64,
    ) -> Result<()> {
        let now = unix_now();
        let conn = self.conn.lock().unwrap();

        conn.execute(
            "UPDATE node_scores SET
                last_seen = ?1,
                uptime_seconds = ?2,
                max_peers = MAX(max_peers, ?3),
                storage_pledged_gb = MAX(storage_pledged_gb, ?4)
             WHERE node_id=?5",
            params![now, uptime_seconds, peers_count, storage_pledged_gb, node_id],
        )?;

        // Streak + score recalculation
        let row = conn.query_row(
            "SELECT streak_days, streak_last_date, items_ingested, bytes_stored,
                    bytes_served, queries_answered
             FROM node_scores WHERE node_id=?1",
            params![node_id],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, i64>(4)?,
                    r.get::<_, i64>(5)?,
                ))
            },
        );
        if let Ok((streak, last_date, items, _bytes, bytes_served, queries)) = row {
            let today = today_str();
            let new_streak = if last_date != today {
                let yesterday = yesterday_str(now);
                if last_date == yesterday && !last_date.is_empty() { streak + 1 } else { 1 }
            } else {
                streak
            };

            let uptime_days = uptime_seconds / 86400.0;
            let gb_served = bytes_served as f64 / 1_073_741_824.0;
            let score = (
                uptime_days * 10.0
                + storage_pledged_gb * 5.0
                + items as f64
                + gb_served * 3.0
                + (queries / 100) as f64
                + new_streak as f64 * 2.0
            ) as i64;

            if last_date != today {
                conn.execute(
                    "UPDATE node_scores SET streak_days=?1, streak_last_date=?2, score=?3
                     WHERE node_id=?4",
                    params![new_streak, today, score, node_id],
                )?;
            } else {
                conn.execute(
                    "UPDATE node_scores SET score=?1 WHERE node_id=?2",
                    params![score, node_id],
                )?;
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Achievement checking
    // -----------------------------------------------------------------------

    fn check_achievements_inner(
        &self,
        conn: &Connection,
        node_id: &str,
        owner_user: &str,
    ) -> Result<()> {
        let row = conn.query_row(
            "SELECT items_ingested, max_peers, streak_days, bytes_stored,
                    bytes_served, continents, uptime_seconds
             FROM node_scores WHERE node_id=?1",
            params![node_id],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, i64>(4)?,
                    r.get::<_, String>(5)?,
                    r.get::<_, f64>(6)?,
                ))
            },
        );
        let (items, max_peers, streak, bytes, bytes_served, continents, uptime_s) =
            match row { Ok(r) => r, Err(_) => return Ok(()) };

        // Collect already-earned achievement IDs
        let mut stmt = conn.prepare(
            "SELECT achievement_id FROM achievements
             WHERE entity_type='node' AND entity_id=?1",
        )?;
        let existing: std::collections::HashSet<String> = stmt
            .query_map(params![node_id], |r| r.get(0))?
            .filter_map(|r| r.ok())
            .collect();

        let now = unix_now();
        let gb_stored = bytes as f64 / 1_073_741_824.0;
        let gb_served_val = bytes_served as f64 / 1_073_741_824.0;
        let uptime_days = uptime_s / 86400.0;
        let n_continents = continents.split(',').filter(|s| !s.is_empty()).count();

        for ach in ACHIEVEMENTS {
            if existing.contains(ach.id) {
                continue;
            }
            let earned = match ach.field {
                "items_ingested" => items as f64 >= ach.threshold,
                "max_peers"      => max_peers as f64 >= ach.threshold,
                "streak_days"    => streak as f64 >= ach.threshold,
                "gb_stored"      => gb_stored >= ach.threshold,
                "gb_served"      => gb_served_val >= ach.threshold,
                "continents"     => n_continents as f64 >= ach.threshold,
                "uptime_days"    => uptime_days >= ach.threshold,
                _ => false, // speed_rank, top3_streak require external signals
            };

            if earned {
                let result = conn.execute(
                    "INSERT OR IGNORE INTO achievements
                     (entity_type, entity_id, achievement_id, earned_at)
                     VALUES ('node', ?1, ?2, ?3)",
                    params![node_id, ach.id, now],
                );
                if result.unwrap_or(0) > 0 {
                    // Feed entry for achievement
                    let _ = conn.execute(
                        "INSERT INTO activity_feed
                         (timestamp, node_id, username, event_type, detail, score_delta)
                         VALUES (?1, ?2, ?3, 'achievement', ?4, 0)",
                        params![
                            now, node_id, owner_user,
                            format!("{} {}: {}", ach.icon, ach.name, ach.desc)
                        ],
                    );
                }
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Leaderboards
    // -----------------------------------------------------------------------

    /// Get leaderboard. board_type: "nodes" | "users" | "groups"
    pub fn get_leaderboard(
        &self,
        board_type: &str,
        limit: usize,
        _group_filter: Option<&str>,
    ) -> Result<Vec<LeaderboardEntry>> {
        let conn = self.conn.lock().unwrap();
        let limit = limit as i64;

        match board_type {
            "nodes" => {
                let mut stmt = conn.prepare(
                    "SELECT node_id, owner_user, score, items_ingested,
                            bytes_stored, bytes_served, streak_days, uptime_seconds,
                            max_peers, display_alias, anonymous, storage_pledged_gb,
                            group_id, sponsor_name, sponsor_url, node_url
                     FROM node_scores
                     ORDER BY score DESC LIMIT ?1",
                )?;
                let entries: Vec<LeaderboardEntry> = stmt
                    .query_map(params![limit], |r| {
                        Ok((
                            r.get::<_, String>(0)?,  // node_id
                            r.get::<_, String>(1)?,  // owner_user
                            r.get::<_, i64>(2)?,     // score
                            r.get::<_, i64>(3)?,     // items_ingested
                            r.get::<_, i64>(4)?,     // bytes_stored
                            r.get::<_, i64>(5)?,     // bytes_served
                            r.get::<_, i64>(6)?,     // streak_days
                            r.get::<_, f64>(7)?,     // uptime_seconds
                            r.get::<_, i64>(8)?,     // max_peers
                            r.get::<_, String>(9)?,  // display_alias
                            r.get::<_, i64>(10)?,    // anonymous
                            r.get::<_, f64>(11)?,    // storage_pledged_gb
                            r.get::<_, String>(12)?, // group_id
                            r.get::<_, String>(13)?, // sponsor_name
                            r.get::<_, String>(14)?, // sponsor_url
                            r.get::<_, String>(15)?, // node_url
                        ))
                    })?
                    .filter_map(|r| r.ok())
                    .enumerate()
                    .map(|(i, r)| {
                        let anon = r.10 != 0;
                        LeaderboardEntry {
                            rank: i + 1,
                            node_id: Some(r.0.clone()),
                            node_name: Some(if !r.9.is_empty() { r.9.clone() } else { r.0.clone() }),
                            owner: Some(if anon { String::new() } else { r.1.clone() }),
                            anonymous: Some(anon),
                            score: r.2,
                            items: Some(r.3),
                            gb_stored: Some(round2(r.4 as f64 / 1_073_741_824.0)),
                            gb_served: Some(round2(r.5 as f64 / 1_073_741_824.0)),
                            gb_pledged: Some(r.11),
                            streak: Some(r.6),
                            peers: Some(r.8),
                            uptime_days: Some(round2(r.7 / 86400.0)),
                            group: Some(r.12),
                            sponsor_name: Some(r.13),
                            sponsor_url: Some(r.14),
                            node_url: Some(r.15),
                            // user/group fields
                            username: None, display_name: None, nodes: None,
                            group_id: None, group_name: None, members: None,
                            description: None,
                        }
                    })
                    .collect();
                Ok(entries)
            }
            "users" => {
                self.refresh_user_scores_inner(&conn)?;
                let mut stmt = conn.prepare(
                    "SELECT username, display_name, total_score, total_nodes,
                            total_items, total_bytes_stored, total_bytes_served
                     FROM user_scores WHERE opted_in=1
                     ORDER BY total_score DESC LIMIT ?1",
                )?;
                let entries: Vec<LeaderboardEntry> = stmt
                    .query_map(params![limit], |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, i64>(2)?,
                            r.get::<_, i64>(3)?,
                            r.get::<_, i64>(4)?,
                            r.get::<_, i64>(5)?,
                            r.get::<_, i64>(6)?,
                        ))
                    })?
                    .filter_map(|r| r.ok())
                    .enumerate()
                    .map(|(i, r)| LeaderboardEntry {
                        rank: i + 1,
                        username: Some(r.0),
                        display_name: Some(r.1),
                        score: r.2,
                        nodes: Some(r.3),
                        items: Some(r.4),
                        gb_stored: Some(round2(r.5 as f64 / 1_073_741_824.0)),
                        gb_served: Some(round2(r.6 as f64 / 1_073_741_824.0)),
                        node_id: None, node_name: None, owner: None, anonymous: None,
                        group: None, group_id: None, group_name: None, members: None,
                        description: None, gb_pledged: None, streak: None,
                        peers: None, uptime_days: None, sponsor_name: None,
                        sponsor_url: None, node_url: None,
                    })
                    .collect();
                Ok(entries)
            }
            "groups" => {
                self.refresh_group_scores_inner(&conn)?;
                let mut stmt = conn.prepare(
                    "SELECT g.group_id, g.group_name, g.total_score, g.description,
                            COUNT(gm.username) as members
                     FROM groups g
                     LEFT JOIN group_members gm ON g.group_id = gm.group_id
                     GROUP BY g.group_id
                     ORDER BY g.total_score DESC LIMIT ?1",
                )?;
                let entries: Vec<LeaderboardEntry> = stmt
                    .query_map(params![limit], |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, i64>(2)?,
                            r.get::<_, String>(3)?,
                            r.get::<_, i64>(4)?,
                        ))
                    })?
                    .filter_map(|r| r.ok())
                    .enumerate()
                    .map(|(i, r)| LeaderboardEntry {
                        rank: i + 1,
                        group_id: Some(r.0),
                        group_name: Some(r.1),
                        score: r.2,
                        description: Some(r.3),
                        members: Some(r.4),
                        node_id: None, node_name: None, owner: None, anonymous: None,
                        username: None, display_name: None, nodes: None,
                        group: None, items: None, gb_stored: None, gb_served: None,
                        gb_pledged: None, streak: None, peers: None, uptime_days: None,
                        sponsor_name: None, sponsor_url: None, node_url: None,
                    })
                    .collect();
                Ok(entries)
            }
            _ => Ok(vec![]),
        }
    }

    // -----------------------------------------------------------------------
    // Node profile
    // -----------------------------------------------------------------------

    pub fn get_node_profile(&self, node_id: &str) -> Result<Option<NodeProfile>> {
        let conn = self.conn.lock().unwrap();
        let row = conn.query_row(
            "SELECT node_id, owner_user, group_id, score, items_ingested,
                    bytes_stored, bytes_served, queries_answered, streak_days,
                    uptime_seconds, max_peers, continents, first_seen,
                    anonymous, display_alias, sponsor_name, sponsor_url, node_url
             FROM node_scores WHERE node_id=?1",
            params![node_id],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, i64>(4)?,
                    r.get::<_, i64>(5)?,
                    r.get::<_, i64>(6)?,
                    r.get::<_, i64>(7)?,
                    r.get::<_, i64>(8)?,
                    r.get::<_, f64>(9)?,
                    r.get::<_, i64>(10)?,
                    r.get::<_, String>(11)?,
                    r.get::<_, f64>(12)?,
                    r.get::<_, i64>(13)?,
                    r.get::<_, String>(14)?,
                    r.get::<_, String>(15)?,
                    r.get::<_, String>(16)?,
                    r.get::<_, String>(17)?,
                ))
            },
        );

        let row = match row {
            Ok(r) => r,
            Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let is_anon = row.13 != 0;
        let display_alias = &row.14;
        let display_node_id = if is_anon && !display_alias.is_empty() {
            display_alias.clone()
        } else {
            row.0.clone()
        };

        // Achievements
        let mut stmt = conn.prepare(
            "SELECT achievement_id, earned_at FROM achievements
             WHERE entity_type='node' AND entity_id=?1 ORDER BY earned_at",
        )?;
        let achievements: Vec<AchievementEntry> = stmt
            .query_map(params![node_id], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, f64>(1)?))
            })?
            .filter_map(|r| r.ok())
            .filter_map(|(ach_id, earned_at)| {
                ACHIEVEMENTS.iter().find(|a| a.id == ach_id).map(|def| AchievementEntry {
                    id: def.id.to_string(),
                    icon: def.icon.to_string(),
                    name: def.name.to_string(),
                    desc: def.desc.to_string(),
                    earned_at,
                })
            })
            .collect();

        let continents: Vec<String> = row.11
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();

        Ok(Some(NodeProfile {
            node_id: display_node_id,
            owner: if is_anon { String::new() } else { row.1 },
            anonymous: is_anon,
            group: row.2,
            score: row.3,
            items_ingested: row.4,
            gb_stored: round2(row.5 as f64 / 1_073_741_824.0),
            gb_served: round2(row.6 as f64 / 1_073_741_824.0),
            queries: row.7,
            streak_days: row.8,
            uptime_days: round2(row.9 / 86400.0),
            max_peers: row.10,
            continents,
            achievements,
            first_seen: row.12,
            sponsor_name: row.15,
            sponsor_url: row.16,
            node_url: row.17,
        }))
    }

    // -----------------------------------------------------------------------
    // Activity feed
    // -----------------------------------------------------------------------

    pub fn get_feed(&self, limit: usize) -> Result<Vec<FeedEntry>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT timestamp, node_id, username, event_type, detail, score_delta
             FROM activity_feed ORDER BY timestamp DESC LIMIT ?1",
        )?;
        let entries: Vec<FeedEntry> = stmt
            .query_map(params![limit as i64], |r| {
                Ok(FeedEntry {
                    timestamp: r.get(0)?,
                    node_id: r.get(1)?,
                    username: r.get(2)?,
                    event_type: r.get(3)?,
                    detail: r.get(4)?,
                    score_delta: r.get(5)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(entries)
    }

    // -----------------------------------------------------------------------
    // Network stats
    // -----------------------------------------------------------------------

    pub fn network_stats(&self) -> Result<NetworkStats> {
        let conn = self.conn.lock().unwrap();
        let nodes: i64 = conn.query_row(
            "SELECT COUNT(*) FROM node_scores", [], |r| r.get(0),
        ).unwrap_or(0);
        let users: i64 = conn.query_row(
            "SELECT COUNT(*) FROM user_scores WHERE opted_in=1", [], |r| r.get(0),
        ).unwrap_or(0);
        let groups: i64 = conn.query_row(
            "SELECT COUNT(*) FROM groups", [], |r| r.get(0),
        ).unwrap_or(0);
        let total_score: i64 = conn.query_row(
            "SELECT COALESCE(SUM(score), 0) FROM node_scores", [], |r| r.get(0),
        ).unwrap_or(0);
        let total_achs: i64 = conn.query_row(
            "SELECT COUNT(*) FROM achievements", [], |r| r.get(0),
        ).unwrap_or(0);

        Ok(NetworkStats {
            opted_in_nodes: nodes,
            opted_in_users: users,
            groups,
            total_network_score: total_score,
            total_achievements_earned: total_achs,
            available_achievements: ACHIEVEMENTS.len(),
        })
    }

    // -----------------------------------------------------------------------
    // Economy health
    // -----------------------------------------------------------------------

    pub fn economy_health(&self) -> Result<EconomyHealth> {
        let conn = self.conn.lock().unwrap();
        let mut stmt_1 = conn.prepare(
            "SELECT items_ingested, bytes_stored, bytes_served, storage_pledged_gb, last_seen
             FROM node_scores",
        )?;
        let rows: Vec<(i64, i64, i64, f64, f64)> = stmt_1.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, f64>(3)?,
                r.get::<_, f64>(4)?,
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();

        if rows.is_empty() {
            return Ok(EconomyHealth {
                score: 0,
                status: "red".to_string(),
                label: "No nodes".to_string(),
                nodes_total: 0,
                nodes_alive: 0,
                storage_pledged_gb: 0.0,
                storage_used_gb: 0.0,
                storage_utilization_pct: 0.0,
                data_served_gb: 0.0,
                estimated_replication_factor: 0.0,
                factors: EconomyHealthFactors {
                    storage_headroom: FactorScore { score: 0.0, weight: 0.30 },
                    node_diversity:   FactorScore { score: 0.0, weight: 0.25 },
                    data_redundancy:  FactorScore { score: 0.0, weight: 0.25 },
                    data_reuse:       FactorScore { score: 0.0, weight: 0.20 },
                },
            });
        }

        let now = unix_now();
        let alive_threshold = 300.0;

        let nodes_total = rows.len();
        let nodes_alive = rows.iter().filter(|r| (now - r.4) < alive_threshold).count();

        let total_stored_gb: f64 = rows.iter().map(|r| r.1 as f64 / 1_073_741_824.0).sum();
        let total_pledged_gb: f64 = rows.iter().map(|r| r.3).sum();
        let total_served_gb: f64 = rows.iter().map(|r| r.2 as f64 / 1_073_741_824.0).sum();

        // Factor 1: Storage headroom (30%)
        let storage_score = if total_pledged_gb <= 0.0 {
            0.0
        } else {
            let util = total_stored_gb / total_pledged_gb;
            if util < 0.01 { 20.0 }
            else if util < 0.5 { 100.0 }
            else if util < 0.8 { 70.0 }
            else if util < 0.95 { 40.0 }
            else { 10.0 }
        };

        // Factor 2: Node diversity (25%)
        let diversity_score = match nodes_alive {
            0 => 0.0,
            1 => 20.0,
            2 => 50.0,
            3..=4 => 75.0,
            5..=10 => 90.0,
            _ => 100.0,
        };

        // Factor 3: Data redundancy (25%)
        let alive_items: Vec<i64> = rows.iter()
            .filter(|r| (now - r.4) < alive_threshold)
            .map(|r| r.0)
            .collect();
        let sum_items: i64 = alive_items.iter().sum();
        let redundancy_score = if sum_items <= 0 || alive_items.is_empty() {
            0.0
        } else {
            let max_unique = *alive_items.iter().max().unwrap_or(&1);
            if max_unique <= 0 {
                0.0
            } else {
                let est = sum_items as f64 / max_unique as f64;
                if est >= 3.0 { 100.0 }
                else if est >= 2.0 { 80.0 }
                else if est >= 1.5 { 60.0 }
                else if est > 1.0 { 40.0 }
                else { 15.0 }
            }
        };

        // Factor 4: Data reuse (20%)
        let reuse_score = if total_served_gb <= 0.0 {
            5.0
        } else if total_served_gb < 1.0 {
            30.0
        } else if total_stored_gb > 0.0 && total_served_gb < total_stored_gb * 0.01 {
            50.0
        } else if total_stored_gb > 0.0 && total_served_gb < total_stored_gb * 0.1 {
            75.0
        } else {
            100.0
        };

        let score_raw: f64 = storage_score * 0.30
            + diversity_score * 0.25
            + redundancy_score * 0.25
            + reuse_score * 0.20;
        let score_f = score_raw.round().clamp(0.0, 100.0);
        let score = score_f as i64;

        let (status, label) = if score >= 70 {
            ("green", "Healthy")
        } else if score >= 40 {
            ("yellow", "Growing")
        } else {
            ("red", "Needs attention")
        };

        let util_pct = if total_pledged_gb > 0.0 {
            (total_stored_gb / total_pledged_gb * 100.0).round() / 10.0 * 10.0
        } else {
            0.0
        };

        let max_unique = alive_items.iter().max().copied().unwrap_or(1).max(1);
        let est_replication = if sum_items > 0 {
            (sum_items as f64 / max_unique as f64 * 100.0).round() / 100.0
        } else {
            0.0
        };

        Ok(EconomyHealth {
            score,
            status: status.to_string(),
            label: label.to_string(),
            nodes_total,
            nodes_alive,
            storage_pledged_gb: round1(total_pledged_gb),
            storage_used_gb: round1(total_stored_gb),
            storage_utilization_pct: round1(util_pct),
            data_served_gb: round1(total_served_gb),
            estimated_replication_factor: est_replication,
            factors: EconomyHealthFactors {
                storage_headroom: FactorScore { score: storage_score,   weight: 0.30 },
                node_diversity:   FactorScore { score: diversity_score,  weight: 0.25 },
                data_redundancy:  FactorScore { score: redundancy_score, weight: 0.25 },
                data_reuse:       FactorScore { score: reuse_score,      weight: 0.20 },
            },
        })
    }

    // -----------------------------------------------------------------------
    // Challenges
    // -----------------------------------------------------------------------

    /// Seed initial challenges if none exist.
    pub fn seed_challenges(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let active: i64 = conn.query_row(
            "SELECT COUNT(*) FROM challenges WHERE active=1", [], |r| r.get(0),
        ).unwrap_or(0);
        if active > 0 {
            return Ok(());
        }

        let now = unix_now();
        let (week_start, week_end, month_start, month_end) = period_bounds(now);

        for tmpl in CHALLENGE_TEMPLATES {
            let (start, end) = if tmpl.period == "weekly" {
                (week_start, week_end)
            } else {
                (month_start, month_end)
            };
            conn.execute(
                "INSERT INTO challenges (title, description, metric, period, start_ts, end_ts, created_at, active)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1)",
                params![tmpl.title, tmpl.description, tmpl.metric, tmpl.period, start, end, now],
            )?;
        }
        Ok(())
    }

    /// Close expired challenges and create new ones.
    pub fn rotate_challenges(&self) -> Result<()> {
        let now = unix_now();
        let conn = self.conn.lock().unwrap();
        let mut stmt_2 = conn.prepare(
            "SELECT id FROM challenges WHERE active=1 AND end_ts < ?1",
        )?;
        let expired: Vec<i64> = stmt_2.query_map(params![now], |r| r.get(0))?
            .filter_map(|r| r.ok())
            .collect();
        drop(stmt_2);
        for id in &expired {
            conn.execute("UPDATE challenges SET active=0 WHERE id=?1", params![id])?;
        }
        drop(conn);
        if !expired.is_empty() {
            self.seed_challenges()?;
        }
        Ok(())
    }

    /// Get active challenges with top-3 entries.
    pub fn get_active_challenges(&self) -> Result<Vec<Challenge>> {
        let now = unix_now();
        let conn = self.conn.lock().unwrap();
        let mut stmt_3 = conn.prepare(
            "SELECT id, title, description, metric, period, start_ts, end_ts
             FROM challenges WHERE active=1 ORDER BY end_ts",
        )?;
        let challenges: Vec<(i64, String, String, String, String, f64, f64)> = stmt_3.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, f64>(5)?,
                r.get::<_, f64>(6)?,
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();

        let mut result = Vec::new();
        for (id, title, desc, metric, period, start_ts, end_ts) in challenges {
            let mut stmt_4 = conn.prepare(
                "SELECT ce.node_id, ce.score,
                        COALESCE(NULLIF(ns.display_alias,''), NULLIF(ns.owner_user,''), ce.node_id) as dname,
                        COALESCE(ns.group_id, '') as ng
                 FROM challenge_entries ce
                 LEFT JOIN node_scores ns ON ce.node_id = ns.node_id
                 WHERE ce.challenge_id = ?1
                 ORDER BY ce.score DESC LIMIT 3",
            )?;
            let top3: Vec<ChallengeTop3Entry> = stmt_4.query_map(params![id], |r| {
                Ok(ChallengeTop3Entry {
                    node_id: r.get(0)?,
                    score: r.get(1)?,
                    display_name: r.get(2)?,
                    group: r.get(3)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();

            let participants: i64 = conn.query_row(
                "SELECT COUNT(*) FROM challenge_entries WHERE challenge_id=?1 AND score > 0",
                params![id],
                |r| r.get(0),
            ).unwrap_or(0);

            let remaining_s = (end_ts - now).max(0.0);
            result.push(Challenge {
                id,
                title,
                description: desc,
                metric,
                period,
                start_ts,
                end_ts,
                remaining_seconds: remaining_s,
                remaining_human: format_duration(remaining_s),
                participants,
                top3,
            });
        }
        Ok(result)
    }

    /// Full ranking for a challenge.
    pub fn get_challenge_results(&self, challenge_id: i64) -> Result<Option<ChallengeResults>> {
        let conn = self.conn.lock().unwrap();
        let ch = conn.query_row(
            "SELECT id, title, description, metric, period, active
             FROM challenges WHERE id=?1",
            params![challenge_id],
            |r| Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, i64>(5)?,
            )),
        );

        let (id, title, desc, metric, period, active) = match ch {
            Ok(r) => r,
            Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let mut stmt = conn.prepare(
            "SELECT ce.node_id, ce.score,
                    COALESCE(NULLIF(ns.display_alias,''), NULLIF(ns.owner_user,''), ce.node_id) as dname,
                    COALESCE(ns.group_id, '') as ng
             FROM challenge_entries ce
             LEFT JOIN node_scores ns ON ce.node_id = ns.node_id
             WHERE ce.challenge_id = ?1
             ORDER BY ce.score DESC",
        )?;
        let rankings: Vec<ChallengeRankEntry> = stmt
            .query_map(params![id], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, f64>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                ))
            })?
            .filter_map(|r| r.ok())
            .enumerate()
            .map(|(i, (node_id, score, display_name, group))| ChallengeRankEntry {
                rank: i + 1,
                node_id,
                display_name,
                score,
                group,
            })
            .collect();

        Ok(Some(ChallengeResults {
            id,
            title,
            description: desc,
            metric,
            period,
            active: active != 0,
            rankings,
        }))
    }

    /// Refresh active challenge scores from node_scores.
    pub fn update_challenge_scores(&self) -> Result<()> {
        let now = unix_now();
        let conn = self.conn.lock().unwrap();

        let mut stmt_5 = conn.prepare(
            "SELECT id, metric FROM challenges WHERE active=1",
        )?;
        let challenges: Vec<(i64, String)> = stmt_5.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .filter_map(|r| r.ok())
            .collect();

        let mut stmt_6 = conn.prepare(
            "SELECT node_id, items_ingested, bytes_stored, bytes_served, score
             FROM node_scores",
        )?;
        let nodes: Vec<(String, i64, i64, i64, i64)> = stmt_6.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)))?
            .filter_map(|r| r.ok())
            .collect();

        for (ch_id, metric) in &challenges {
            for (node_id, items, bytes, bytes_served, score) in &nodes {
                let val: f64 = match metric.as_str() {
                    "items_ingested" => *items as f64,
                    "gb_stored"      => *bytes as f64 / 1_073_741_824.0,
                    "gb_served"      => *bytes_served as f64 / 1_073_741_824.0,
                    "score"          => *score as f64,
                    _ => 0.0,
                };
                if val > 0.0 {
                    conn.execute(
                        "INSERT INTO challenge_entries (challenge_id, node_id, score, last_updated)
                         VALUES (?1, ?2, ?3, ?4)
                         ON CONFLICT(challenge_id, node_id) DO UPDATE SET score=?3, last_updated=?4",
                        params![ch_id, node_id, val, now],
                    )?;
                }
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Cleanup
    // -----------------------------------------------------------------------

    /// Sync actual storage stats from node heartbeat
    pub fn update_storage_stats(&self, node_id: &str, items: i64, bytes_stored: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE node_scores SET items_ingested = MAX(items_ingested, ?1), bytes_stored = MAX(bytes_stored, ?2) WHERE node_id = ?3",
            rusqlite::params![items, bytes_stored, node_id],
        )?;
        Ok(())
    }

    pub fn cleanup(&self, retain_days: u64) -> Result<()> {
        let cutoff = unix_now() - (retain_days as f64 * 86400.0);
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM activity_feed WHERE timestamp < ?1",
            params![cutoff],
        )?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Internal aggregation helpers
    // -----------------------------------------------------------------------

    fn refresh_user_scores_inner(&self, conn: &Connection) -> Result<()> {
        let mut stmt_7 = conn.prepare(
            "SELECT DISTINCT owner_user FROM node_scores WHERE opted_in=1 AND owner_user != ''",
        )?;
        let users: Vec<String> = stmt_7.query_map([], |r| r.get(0))?
            .filter_map(|r| r.ok())
            .collect();

        let now = unix_now();
        for username in users {
            let row = conn.query_row(
                "SELECT COUNT(*), COALESCE(SUM(items_ingested),0), COALESCE(SUM(bytes_stored),0),
                        COALESCE(SUM(bytes_served),0), COALESCE(SUM(queries_answered),0),
                        COALESCE(SUM(score),0), MIN(first_seen), MAX(last_seen)
                 FROM node_scores WHERE owner_user=?1 AND opted_in=1",
                params![&username],
                |r| Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, i64>(4)?,
                    r.get::<_, i64>(5)?,
                    r.get::<_, f64>(6).unwrap_or(now),
                    r.get::<_, f64>(7).unwrap_or(now),
                )),
            )?;
            conn.execute(
                "INSERT INTO user_scores
                 (username, opted_in, total_nodes, total_items, total_bytes_stored,
                  total_bytes_served, total_queries, total_score, first_seen, last_seen)
                 VALUES (?1, 1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(username) DO UPDATE SET
                 total_nodes=?2, total_items=?3, total_bytes_stored=?4,
                 total_bytes_served=?5, total_queries=?6, total_score=?7, last_seen=?9",
                params![
                    username, row.0, row.1, row.2, row.3, row.4, row.5, row.6, row.7
                ],
            )?;
        }
        Ok(())
    }

    fn refresh_group_scores_inner(&self, conn: &Connection) -> Result<()> {
        let mut stmt_8 = conn.prepare("SELECT group_id FROM groups")?;
        let groups: Vec<String> = stmt_8.query_map([], |r| r.get(0))?
            .filter_map(|r| r.ok())
            .collect();
        for gid in groups {
            let total: i64 = conn.query_row(
                "SELECT COALESCE(SUM(us.total_score), 0)
                 FROM group_members gm
                 JOIN user_scores us ON gm.username = us.username
                 WHERE gm.group_id=?1",
                params![&gid],
                |r| r.get(0),
            ).unwrap_or(0);
            conn.execute(
                "UPDATE groups SET total_score=?1 WHERE group_id=?2",
                params![total, gid],
            )?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn unix_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn today_str() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let days = now / 86400;
    // Use epoch-day number as streak date key — consistent, no chrono needed
    format!("{}", days)
}

fn yesterday_str(now: f64) -> String {
    let days = (now as u64 - 86400) / 86400;
    format!("{}", days)
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

fn round1(v: f64) -> f64 {
    (v * 10.0).round() / 10.0
}

/// Compute period start/end timestamps for weekly and monthly challenges.
/// Returns (week_start, week_end, month_start, month_end) as unix timestamps.
fn period_bounds(now: f64) -> (f64, f64, f64, f64) {
    // Use simple epoch arithmetic — 1970-01-01 was a Thursday (weekday=3 in Mon=0 indexing)
    let secs = now as u64;
    let days_since_epoch = secs / 86400;
    // Monday=0 in Python's weekday(); epoch day 0 = Thursday => offset 3
    let weekday = (days_since_epoch + 3) % 7; // 0=Mon..6=Sun
    let week_start = ((days_since_epoch - weekday) * 86400) as f64;
    let week_end = week_start + 7.0 * 86400.0;

    // Month bounds via rough calculation
    // Use a simple lookup. We compute the month from epoch.
    let (year, month, _) = epoch_to_ymd(secs);
    let month_start = ymd_to_epoch(year, month, 1) as f64;
    let (ny, nm) = if month == 12 { (year + 1, 1) } else { (year, month + 1) };
    let month_end = ymd_to_epoch(ny, nm, 1) as f64;

    (week_start, week_end, month_start, month_end)
}

fn epoch_to_ymd(secs: u64) -> (u64, u64, u64) {
    let days = secs / 86400;
    // Gregorian calendar algorithm (Fliegel-Van Flandern)
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

fn ymd_to_epoch(y: u64, m: u64, d: u64) -> u64 {
    let y = if m <= 2 { y - 1 } else { y };
    let m = if m <= 2 { m + 9 } else { m - 3 };
    let era = y / 400;
    let yoe = y - era * 400;
    let doy = (153 * m + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe;
    (days - 719468) * 86400
}

fn format_duration(seconds: f64) -> String {
    if seconds <= 0.0 {
        return "ended".to_string();
    }
    let s = seconds as u64;
    let d = s / 86400;
    let h = (s % 86400) / 3600;
    let m = (s % 3600) / 60;
    if d > 0 {
        format!("{}d {}h", d, h)
    } else if h > 0 {
        format!("{}h {}m", h, m)
    } else {
        format!("{}m", m)
    }
}
