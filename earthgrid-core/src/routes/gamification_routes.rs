use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;

use crate::server::{AppState, err};


// ---------------------------------------------------------------------------
// Gamification handlers
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct LeaderboardQuery {
    #[serde(rename = "type")]
    pub board_type: Option<String>,
    pub limit: Option<usize>,
    pub group: Option<String>,
}


#[derive(Deserialize)]
pub struct FeedQuery {
    pub limit: Option<usize>,
}


pub(crate) async fn gamification_leaderboard(
    State(state): State<AppState>,
    Query(q): Query<LeaderboardQuery>,
) -> impl IntoResponse {
    let board_type = q.board_type.as_deref().unwrap_or("nodes");
    let limit = q.limit.unwrap_or(20).min(100);
    let group_filter = q.group.as_deref();
    match state.gamification.get_leaderboard(board_type, limit, group_filter) {
        Ok(entries) => {
            let count = entries.len();
            (StatusCode::OK, Json(serde_json::json!({"leaderboard": entries, "count": count, "type": board_type}))).into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}


pub(crate) async fn gamification_node_profile(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> impl IntoResponse {
    match state.gamification.get_node_profile(&node_id) {
        Ok(Some(profile)) => (StatusCode::OK, Json(serde_json::to_value(profile).unwrap_or_default())).into_response(),
        Ok(None) => err(StatusCode::NOT_FOUND, "Node not found in gamification DB").into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}


pub(crate) async fn gamification_feed(
    State(state): State<AppState>,
    Query(q): Query<FeedQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(50).min(200);
    match state.gamification.get_feed(limit) {
        Ok(feed) => {
            let count = feed.len();
            (StatusCode::OK, Json(serde_json::json!({"feed": feed, "count": count}))).into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}


pub(crate) async fn gamification_stats(State(state): State<AppState>) -> impl IntoResponse {
    match state.gamification.network_stats() {
        Ok(stats) => (StatusCode::OK, Json(serde_json::to_value(stats).unwrap_or_default())).into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}


pub(crate) async fn gamification_economy(State(state): State<AppState>) -> impl IntoResponse {
    match state.gamification.economy_health() {
        Ok(health) => (StatusCode::OK, Json(serde_json::to_value(health).unwrap_or_default())).into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}


pub(crate) async fn gamification_challenges(State(state): State<AppState>) -> impl IntoResponse {
    match state.gamification.get_active_challenges() {
        Ok(challenges) => {
            let count = challenges.len();
            (StatusCode::OK, Json(serde_json::json!({"challenges": challenges, "count": count}))).into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}


pub(crate) async fn gamification_challenge_results(
    State(state): State<AppState>,
    Path(challenge_id): Path<i64>,
) -> impl IntoResponse {
    match state.gamification.get_challenge_results(challenge_id) {
        Ok(Some(results)) => (StatusCode::OK, Json(serde_json::to_value(results).unwrap_or_default())).into_response(),
        Ok(None) => err(StatusCode::NOT_FOUND, "Challenge not found").into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()).into_response(),
    }
}

