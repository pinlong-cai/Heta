"""Chat API router.

Provides the ``POST /api/v1/chat`` endpoint that validates incoming requests,
resolves the target datasets' process_mode, validates the requested
query_mode against the mode registry, and dispatches to the query processor.

Available process_modes and their query_modes are defined in
:mod:`hetadb.core.mode_registry`.
"""

from __future__ import annotations

import json
import logging
import uuid

from fastapi import APIRouter
from pydantic import BaseModel, Field

from hetadb.core.chat_processor import query_chat
from hetadb.core.file_processor import ConfigManager
from hetadb.core.mode_registry import get_supported_parse_modes, validate as validate_mode

router = APIRouter(prefix="/api/v1/hetadb/chat", tags=["chat"])
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    """Incoming knowledge-base query."""
    query: str = Field(..., max_length=4096)
    top_k: int | None = None
    kb_id: str | None = None
    user_id: str | None = None
    max_results: int | None = 20
    query_mode: str = "naive"


class QueryResult(BaseModel):
    """Single retrieval result."""
    kb_id: str
    kb_name: str
    score: float
    content: str
    text: str
    source_id: list[str]


class Citation(BaseModel):
    """A single file-level source reference attached to a chat answer."""
    index: int
    source_file: str
    dataset: str
    file_url: str | None = None


class QueryResponse(BaseModel):
    """Standardised query response envelope."""
    success: bool
    message: str
    data: list[QueryResult]
    total_count: int
    query_info: dict
    request_id: str
    code: int
    response: str | None = None
    citations: list[Citation] | None = None


# ---------------------------------------------------------------------------
# Process-mode resolution
# ---------------------------------------------------------------------------

def _get_datasets_process_mode(kb_id: str) -> int | None:
    """Determine the process_mode for all parsed datasets under *kb_id*.

    Scans ``workspace/kb/{kb_id}/`` for dataset subdirectories and reads
    each dataset's ``_meta.json`` to discover the recorded ``process_mode``.

    Returns:
        The common process_mode if all datasets agree, or ``None``
        if the KB has no parsed datasets.

    Raises:
        ValueError: if datasets under the same KB have mixed process_modes.
    """
    kb_path = ConfigManager().get_workspace_root() / "kb" / kb_id
    if not kb_path.exists():
        return None

    modes: set[int] = set()
    for ds_path in kb_path.iterdir():
        if not ds_path.is_dir():
            continue
        meta_file = ds_path / "_meta.json"
        if not meta_file.exists():
            continue
        with open(meta_file, encoding="utf-8") as f:
            meta = json.load(f)
        modes.add(meta.get("process_mode", 0))

    if not modes:
        return None
    if len(modes) > 1:
        raise ValueError(
            f"KB '{kb_id}' contains datasets with mixed process_modes: {modes}. "
            "All datasets in a KB must use the same processing pipeline."
        )
    return modes.pop()


_SUPPORTED_PROCESS_MODES = get_supported_parse_modes()


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.post("", response_model=QueryResponse)
async def query_knowledge(request: QueryRequest):
    """Query the knowledge base.

    Resolves the target datasets' processing mode and dispatches
    to the corresponding query processor.
    """
    request_id = str(uuid.uuid4())
    logger.info(
        "[%s] chat request: query_mode=%s query='%s'",
        request_id, request.query_mode, request.query,
    )

    # Parameter validation
    if not request.query or not request.query.strip():
        logger.warning("[%s] empty query", request_id)
        return QueryResponse(**_error(request_id, 400, "query must not be empty"))

    if not request.user_id or not request.user_id.strip():
        logger.warning("[%s] missing user_id", request_id)
        return QueryResponse(**_error(request_id, 400, "user_id must not be empty"))

    if request.kb_id is None:
        logger.warning("[%s] missing kb_id", request_id)
        return QueryResponse(**_error(request_id, 400, "kb_id must not be empty"))

    # Resolve process_mode from dataset metadata
    try:
        process_mode = _get_datasets_process_mode(request.kb_id)
    except ValueError as e:
        logger.error("[%s] %s", request_id, e)
        return QueryResponse(**_error(request_id, 400, str(e)))

    if process_mode is None:
        process_mode = 0

    if process_mode not in _SUPPORTED_PROCESS_MODES:
        msg = f"Unsupported process_mode: {process_mode}"
        logger.warning("[%s] %s", request_id, msg)
        return QueryResponse(**_error(request_id, 400, msg))

    # Validate query_mode against the registry for this process_mode
    if not validate_mode(process_mode, request.query_mode):
        msg = f"query_mode '{request.query_mode}' is not valid for process_mode {process_mode}"
        logger.warning("[%s] %s", request_id, msg)
        return QueryResponse(**_error(request_id, 400, msg))

    try:
        params = request.model_dump()
        params["process_mode"] = process_mode
        result = await query_chat(params)

        if not isinstance(result, dict):
            logger.error("[%s] query_chat returned unexpected type: %s", request_id, type(result))
            return QueryResponse(**_error(request_id, 500, "Upstream processor returned an invalid response"))

        return QueryResponse(**result)
    except Exception:  # noqa: BLE001
        logger.error("[%s] unhandled error in chat endpoint", request_id, exc_info=True)
        return QueryResponse(**_error(request_id, 500, "Internal server error"))


def _error(request_id: str, code: int, message: str) -> dict:
    """Build a minimal error payload matching QueryResponse."""
    return {
        "success": False,
        "message": message,
        "data": [],
        "total_count": 0,
        "request_id": request_id,
        "code": code,
        "query_info": {},
        "response": None,
    }
