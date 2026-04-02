"""MemoryVG API router.

Exposes personal memory CRUD and search via eight endpoints:

- ``POST   /api/v1/memoryvg/add``                  — add memories from messages
- ``POST   /api/v1/memoryvg/search``               — semantic similarity search
- ``GET    /api/v1/memoryvg``                      — list all memories (filterable)
- ``GET    /api/v1/memoryvg/{memory_id}``           — retrieve a single memory
- ``GET    /api/v1/memoryvg/{memory_id}/history``   — modification history
- ``PUT    /api/v1/memoryvg/{memory_id}``           — update memory content
- ``DELETE /api/v1/memoryvg/{memory_id}``           — delete a single memory
- ``DELETE /api/v1/memoryvg``                      — delete all memories (filtered)
"""

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from hetamem.memvg_manager import manager

router = APIRouter(prefix="/api/v1/hetamem/vg", tags=["memoryvg"])
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class Message(BaseModel):
    role: str
    content: str


class AddRequest(BaseModel):
    messages: list[Message]
    user_id: str | None = None
    agent_id: str | None = None
    run_id: str | None = None
    metadata: dict | None = None


class SearchRequest(BaseModel):
    query: str
    user_id: str | None = None
    agent_id: str | None = None
    run_id: str | None = None
    limit: int = 10
    threshold: float | None = None


class UpdateRequest(BaseModel):
    data: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/add")
async def add_memory(request: AddRequest):
    """Extract and store memories from a message conversation."""
    if not any([request.user_id, request.agent_id, request.run_id]):
        raise HTTPException(
            status_code=422,
            detail="At least one of 'user_id', 'agent_id', or 'run_id' must be provided.",
        )
    messages = [m.model_dump() for m in request.messages]
    try:
        return await manager.add(
            messages,
            user_id=request.user_id,
            agent_id=request.agent_id,
            run_id=request.run_id,
            metadata=request.metadata,
        )
    except Exception:
        logger.exception("add_memory failed")
        raise HTTPException(status_code=500, detail="Memory add failed")


@router.post("/search")
async def search_memory(request: SearchRequest):
    """Search memories by semantic similarity."""
    if not request.query.strip():
        raise HTTPException(status_code=400, detail="query must not be empty")
    if not any([request.user_id, request.agent_id, request.run_id]):
        raise HTTPException(
            status_code=422,
            detail="At least one of 'user_id', 'agent_id', or 'run_id' must be provided.",
        )
    try:
        return await manager.search(
            query=request.query,
            user_id=request.user_id,
            agent_id=request.agent_id,
            run_id=request.run_id,
            limit=request.limit,
            threshold=request.threshold,
        )
    except Exception:
        logger.exception("search_memory failed")
        raise HTTPException(status_code=500, detail="Memory search failed")


@router.get("")
async def list_memories(
    user_id: str | None = None,
    agent_id: str | None = None,
    run_id: str | None = None,
    limit: int = 100,
):
    """List all memories, optionally filtered by user/agent/run scope."""
    try:
        return await manager.get_all(
            user_id=user_id,
            agent_id=agent_id,
            run_id=run_id,
            limit=limit,
        )
    except Exception:
        logger.exception("list_memories failed")
        raise HTTPException(status_code=500, detail="Memory list failed")


@router.get("/{memory_id}/history")
async def get_memory_history(memory_id: str):
    """Return the full modification history of a memory entry."""
    try:
        return await manager.history(memory_id)
    except Exception:
        logger.exception("get_memory_history failed")
        raise HTTPException(status_code=500, detail="History retrieval failed")


@router.get("/{memory_id}")
async def get_memory(memory_id: str):
    """Retrieve a single memory entry by ID."""
    try:
        result = await manager.get(memory_id)
    except Exception:
        logger.exception("get_memory failed")
        raise HTTPException(status_code=500, detail="Memory retrieval failed")
    if result is None:
        raise HTTPException(status_code=404, detail="Memory not found")
    return result


@router.put("/{memory_id}")
async def update_memory(memory_id: str, request: UpdateRequest):
    """Update the text content of a memory entry."""
    if not request.data.strip():
        raise HTTPException(status_code=400, detail="data must not be empty")
    try:
        return await manager.update(memory_id, request.data)
    except Exception:
        logger.exception("update_memory failed")
        raise HTTPException(status_code=500, detail="Memory update failed")


@router.delete("/{memory_id}")
async def delete_memory(memory_id: str):
    """Delete a single memory entry by ID."""
    try:
        return await manager.delete(memory_id)
    except Exception:
        logger.exception("delete_memory failed")
        raise HTTPException(status_code=500, detail="Memory delete failed")


@router.delete("")
async def delete_all_memories(
    user_id: str | None = None,
    agent_id: str | None = None,
    run_id: str | None = None,
):
    """Delete all memories matching the given scope filters."""
    try:
        return await manager.delete_all(
            user_id=user_id,
            agent_id=agent_id,
            run_id=run_id,
        )
    except Exception:
        logger.exception("delete_all_memories failed")
        raise HTTPException(status_code=500, detail="Memory delete all failed")
