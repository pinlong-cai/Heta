"""MemoryKB API router.

Exposes the MemoryKB pipeline (LightRAG-based long-term memory) via two
endpoints:

- ``POST /api/v1/memorykb/insert`` — write a memory entry (text + optional media)
- ``POST /api/v1/memorykb/query``  — retrieve and synthesise an answer
"""

import logging
import uuid

from fastapi import APIRouter, BackgroundTasks, File, Form, HTTPException, UploadFile
from pydantic import BaseModel

from hetamem.memkb_manager import manager

router = APIRouter(prefix="/api/v1/hetamem/kb", tags=["memorykb"])
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class InsertResponse(BaseModel):
    id: str
    query: str
    status: str = "accepted"
    videocaption: str | None = None
    audiocaption: str | None = None
    imagecaption: str | None = None


class QueryRequest(BaseModel):
    query: str
    mode: str = "hybrid"
    use_pm: bool = False


class QueryResponse(BaseModel):
    query: str
    mode: str
    pm_used: bool
    pm_memory: str | None = None
    pm_relevant: bool
    rag_memory: str | None = None
    final_answer: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

async def _run_insert(job_id: str, query: str, **media_kwargs) -> None:
    """Background task: run LightRAG insertion and log result."""
    try:
        await manager.insert(query=query, **media_kwargs)
        logger.info("kb_insert job=%s completed", job_id)
    except Exception:
        logger.exception("kb_insert job=%s failed", job_id)


@router.post("/insert", response_model=InsertResponse, status_code=202)
async def insert_memory(
    background_tasks: BackgroundTasks,
    query: str = Form(...),
    video: UploadFile | None = File(None),
    audio: UploadFile | None = File(None),
    image: UploadFile | None = File(None),
):
    """Queue a memory entry for insertion into the knowledge base.

    Returns immediately with status ``accepted``; LightRAG entity extraction
    and graph construction run in the background.  Use ``kb_query`` after a
    few seconds to verify the content is searchable.

    The ``query`` field (required) carries the text content.  Media files are
    optional; if provided they are transcribed / captioned and merged into the
    memory entry before insertion.
    """
    if not query.strip():
        raise HTTPException(status_code=400, detail="query must not be empty")

    job_id = str(uuid.uuid4())
    media_kwargs = {
        "video": await video.read() if video else None,
        "video_name": video.filename if video else None,
        "audio": await audio.read() if audio else None,
        "audio_name": audio.filename if audio else None,
        "image": await image.read() if image else None,
        "image_name": image.filename if image else None,
    }
    background_tasks.add_task(_run_insert, job_id, query, **media_kwargs)
    logger.info("kb_insert job=%s queued", job_id)
    return InsertResponse(id=job_id, query=query)


@router.post("/query", response_model=QueryResponse)
async def query_memory(request: QueryRequest):
    """Query the knowledge base and return a synthesised answer.

    ``mode`` controls the LightRAG retrieval strategy:
    ``local`` | ``global`` | ``hybrid`` (default) | ``naive``.

    Set ``use_pm=true`` to additionally query the parametric memory model
    before falling back to RAG retrieval.
    """
    if not request.query.strip():
        raise HTTPException(status_code=400, detail="query must not be empty")

    try:
        result = await manager.query(
            query=request.query,
            mode=request.mode,
            use_pm=request.use_pm,
        )
    except Exception:
        logger.exception("query_memory failed")
        raise HTTPException(status_code=500, detail="Memory query failed")

    return QueryResponse(**result)
