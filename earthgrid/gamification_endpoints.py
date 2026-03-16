"""EarthGrid Gamification API — FastAPI router."""
from __future__ import annotations
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from typing import Optional

from .gamification import GamificationEngine, ACHIEVEMENTS

router = APIRouter(prefix="/gamification", tags=["gamification"])

# Engine instance set by main.py
_engine: Optional[GamificationEngine] = None


def set_engine(engine: GamificationEngine):
    global _engine
    _engine = engine


def _get_engine() -> GamificationEngine:
    if _engine is None:
        raise HTTPException(503, "Gamification not initialized")
    return _engine


# --- Models ---

class OptInRequest(BaseModel):
    username: str
    display_name: str = ""
    node_id: str = ""
    group_id: str = ""


class GroupCreateRequest(BaseModel):
    group_name: str
    description: str = ""


class GroupJoinRequest(BaseModel):
    username: str


# --- Endpoints ---

@router.get("/leaderboard")
async def leaderboard(
    type: str = Query("nodes", regex="^(nodes|users|groups)$"),
    period: str = Query("all", regex="^(week|month|all)$"),
    limit: int = Query(20, ge=1, le=100),
):
    """Leaderboard — top nodes, users, or groups."""
    engine = _get_engine()
    return {
        "type": type,
        "period": period,
        "entries": engine.get_leaderboard(board_type=type, limit=limit, period=period),
    }


@router.get("/node/{node_id}/profile")
async def node_profile(node_id: str):
    """Node gamification profile."""
    engine = _get_engine()
    profile = engine.get_node_profile(node_id)
    if not profile:
        raise HTTPException(404, "Node not found or not opted in")
    return profile


@router.get("/user/{username}/profile")
async def user_profile(username: str):
    """User gamification profile (aggregated across all nodes)."""
    engine = _get_engine()
    profile = engine.get_user_profile(username)
    if not profile:
        raise HTTPException(404, "User not found or not opted in")
    return profile


@router.get("/group/{group_id}/profile")
async def group_profile(group_id: str):
    """Group profile with members and scores."""
    engine = _get_engine()
    profile = engine.get_group_profile(group_id)
    if not profile:
        raise HTTPException(404, "Group not found")
    return profile


@router.get("/achievements")
async def list_achievements():
    """All available achievements."""
    return {"achievements": ACHIEVEMENTS, "total": len(ACHIEVEMENTS)}


@router.post("/opt-in")
async def opt_in(req: OptInRequest):
    """Opt in a user and/or node to gamification."""
    engine = _get_engine()
    engine.opt_in_user(req.username, req.display_name)
    if req.node_id:
        engine.opt_in_node(req.node_id, req.username)
        if req.group_id:
            engine.set_node_group(req.node_id, req.group_id)
    return {"status": "opted_in", "username": req.username, "node_id": req.node_id}


@router.post("/group")
async def create_group(req: GroupCreateRequest, request: Request):
    """Create a new group (requires auth)."""
    engine = _get_engine()
    # Extract username from API key
    from .config import settings
    api_key = request.headers.get("x-api-key", "")
    if not api_key and not (request.client and request.client.host in ("127.0.0.1", "::1")):
        raise HTTPException(401, "Authentication required to create groups")
    group_id = uuid.uuid4().hex[:12]
    # Determine creator from API key or default
    creator = "anonymous"
    if api_key:
        from .user_auth import UserAuth
        ua = UserAuth(settings.store_path.parent / "users.db")
        user = ua.validate_key(api_key)
        if user:
            creator = user["username"]
    try:
        result = engine.create_group(group_id, req.group_name, creator, req.description)
    except ValueError as e:
        raise HTTPException(409, str(e))
    return result


@router.post("/group/{group_id}/join")
async def join_group(group_id: str, req: GroupJoinRequest):
    """Join a group."""
    engine = _get_engine()
    engine.join_group(group_id, req.username)
    return {"status": "joined", "group_id": group_id, "username": req.username}


@router.get("/feed")
async def activity_feed(limit: int = Query(50, ge=1, le=200)):
    """Recent activity feed."""
    engine = _get_engine()
    return {"feed": engine.get_feed(limit=limit)}


@router.get("/stats")
async def network_stats():
    """Network-wide gamification statistics."""
    engine = _get_engine()
    return engine.network_stats()
