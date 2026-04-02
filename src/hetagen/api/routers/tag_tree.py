"""
Tag tree router.

Endpoints:
    POST /hetagen/tag-tree/submit          — Upload Excel and submit parsing task (legacy)
    GET  /hetagen/tag-tree/status/{task_id} — Query task status and results
    POST /hetagen/tag-tree/generate        — Generate tree from KB or pure LLM (V1)
"""

import asyncio
import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Literal

import yaml
from fastapi import APIRouter, BackgroundTasks, File, Form, HTTPException, UploadFile
from pydantic import BaseModel, Field, model_validator

from common.tasks import create_task, get_task, update_task, TaskStatus
from hetagen.core.tag_tree_parser import parse_tag_tree
from common.llm_client import create_use_llm_async
from hetagen.utils.path import PROJECT_ROOT, HETAGEN_DATA_DIR

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/hetagen/tag-tree", tags=["tag-tree"])

CONFIG_PATH = PROJECT_ROOT / "config.yaml"

_llm_client = None


def get_llm_client():
    """Lazy-load LLM client to avoid initialization issues at import time."""
    global _llm_client
    if _llm_client is None:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            llm_config = yaml.safe_load(f)["hetagen"]["llm"]
        _llm_client = create_use_llm_async(
            url=llm_config["base_url"],
            api_key=llm_config["api_key"],
            model=llm_config["model"],
        )
    return _llm_client


def run_tag_tree_task(
    task_id: str,
    file_path: str,
    tree_name: str,
    tree_description: str,
    sheet_name: str | int,
):
    """Execute tag tree parsing in background thread."""
    update_task(task_id, status=TaskStatus.RUNNING)
    try:
        output_dir = HETAGEN_DATA_DIR / "tasks" / task_id
        output_dir.mkdir(parents=True, exist_ok=True)
        output_json = str(output_dir / "tag_tree.json")

        result = asyncio.run(parse_tag_tree(
            input_excel=file_path,
            output_json=output_json,
            tree_name=tree_name,
            tree_description=tree_description,
            qa_client=get_llm_client(),
            sheet_name=sheet_name,
        ))
        os.unlink(file_path)

        task = get_task(task_id)
        task.metadata["result"] = {
            "tree_name": result["tree_name"],
            "tree_description": result["tree_description"],
            "nodes": result["nodes"],
            "node_count": result["node_count"],
            "path_count": result["path_count"],
        }
        update_task(task_id, status=TaskStatus.COMPLETED)
    except Exception as e:
        logger.exception("Tag tree task %s failed", task_id)
        update_task(task_id, status=TaskStatus.FAILED, error=str(e))
        if os.path.exists(file_path):
            os.unlink(file_path)


@router.post("/submit")
async def submit_tag_tree(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    tree_name: str = Form(default="tag_tree"),
    tree_description: str = Form(default=""),
    sheet_name: str = Form(default="0"),
):
    """Upload Excel file and submit tag tree parsing task."""
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Only .xlsx or .xls files supported")

    with tempfile.NamedTemporaryFile(delete=False, suffix=Path(file.filename).suffix) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    parsed_sheet = int(sheet_name) if sheet_name.isdigit() else sheet_name

    task = create_task("tag_tree")
    background_tasks.add_task(
        run_tag_tree_task, task.task_id, tmp_path, tree_name, tree_description, parsed_sheet
    )
    return {"task_id": task.task_id, "status": "pending", "message": "Task submitted"}


# ---------------------------------------------------------------------------
# V1: KB / Pure-LLM tree generation
# ---------------------------------------------------------------------------

class GenerateTreeRequest(BaseModel):
    topic: str = Field(..., min_length=1, description="Domain topic for the tree")
    mode: Literal["kb", "pure_llm"] = Field(..., description="'kb' or 'pure_llm'")
    kb_name: str | None = Field(default=None, description="Required when mode='kb'")

    @model_validator(mode="after")
    def check_kb_name(self) -> "GenerateTreeRequest":
        if self.mode == "kb" and not self.kb_name:
            raise ValueError("kb_name is required when mode='kb'")
        return self


def _run_generate_tree_task(
    task_id: str,
    topic: str,
    mode: str,
    kb_name: str | None,
) -> None:
    """Execute tree generation in background thread."""
    update_task(task_id, status=TaskStatus.RUNNING)
    try:
        llm_client = get_llm_client()

        workspace_root = None
        embedding_cfg = None
        if mode == "kb":
            from hetadb.core.file_processor import ConfigManager
            mgr = ConfigManager()
            workspace_root = mgr.get_workspace_root()
            emb = mgr.get_embedding_config()
            embedding_cfg = {
                "api_key": emb.api_key,
                "embedding_url": emb.base_url,
                "embedding_model": emb.model,
                "embedding_timeout": emb.timeout,
            }

        from hetagen.core.kb_tree.tree_builder import generate_tree
        result = asyncio.run(generate_tree(
            topic=topic,
            mode=mode,
            llm_client=llm_client,
            kb_name=kb_name,
            workspace_root=workspace_root,
            embedding_cfg=embedding_cfg,
        ))

        # Persist result to disk so it survives process restarts
        output_dir = HETAGEN_DATA_DIR / "tasks" / task_id
        output_dir.mkdir(parents=True, exist_ok=True)
        with open(output_dir / "result.json", "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        task = get_task(task_id)
        task.metadata["result"] = result
        update_task(task_id, status=TaskStatus.COMPLETED)
    except Exception as e:
        logger.exception("Tree generation task %s failed", task_id)
        update_task(task_id, status=TaskStatus.FAILED, error=str(e))


@router.post("/generate")
async def generate_tree_endpoint(
    request: GenerateTreeRequest,
    background_tasks: BackgroundTasks,
):
    """Submit a tree generation task.

    Supports two modes:
    - ``kb``: uses a HetaDB knowledge base as grounding context
    - ``pure_llm``: uses the LLM's own world knowledge
    """
    task = create_task("kb_tree")
    background_tasks.add_task(
        _run_generate_tree_task,
        task.task_id,
        request.topic,
        request.mode,
        request.kb_name,
    )
    return {
        "task_id": task.task_id,
        "status": "pending",
        "message": f"Tree generation started (mode={request.mode})",
    }


@router.get("/status/{task_id}")
async def get_tag_tree_status(task_id: str):
    """Query task status. Returns parsed tree structure when completed.

    Falls back to disk if the task is no longer in memory (e.g. after restart).
    Checks ``result.json`` (kb_tree) and ``tag_tree.json`` (legacy Excel tasks).
    """
    task = get_task(task_id)

    if task is None:
        # Attempt disk recovery
        task_dir = HETAGEN_DATA_DIR / "tasks" / task_id
        for filename in ("result.json", "tag_tree.json"):
            result_path = task_dir / filename
            if result_path.exists():
                with open(result_path, encoding="utf-8") as f:
                    result = json.load(f)
                return {"task_id": task_id, "status": "completed", "result": result}
        raise HTTPException(status_code=404, detail="Task not found")

    response = {"task_id": task_id, "status": task.status.value}
    if task.status == TaskStatus.COMPLETED:
        result = task.metadata.get("result")
        if result is None:
            # Result evicted from metadata but file still on disk
            result_path = HETAGEN_DATA_DIR / "tasks" / task_id / "result.json"
            if result_path.exists():
                with open(result_path, encoding="utf-8") as f:
                    result = json.load(f)
        response["result"] = result
    elif task.status == TaskStatus.FAILED:
        response["error"] = task.error
    return response
