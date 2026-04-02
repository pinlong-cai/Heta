"""
Pipeline router for table generation and Text-to-SQL.

Endpoints:
    POST /hetagen/gentable/submit - Submit a pipeline task (async polling)
    GET /hetagen/gentable/status/{task_id} - Query task status and results
    WS /hetagen/gentable/stream - Stream pipeline execution via WebSocket
"""

import asyncio
import csv
import logging
import shutil
import uuid
from fastapi import APIRouter, BackgroundTasks, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from common.tasks import create_task, get_task, update_task, TaskStatus
from hetagen.core.table_flow.pipeline import run_pipeline
from hetagen.utils.path import HETAGEN_DATA_DIR

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/hetagen/pipeline", tags=["pipeline"])


class PipelineRequest(BaseModel):
    question: str
    sql_question: str | None = None
    top_k: int = 5
    threshold: float = 0.5
    max_workers: int = 16


def run_pipeline_task(task_id: str, request: PipelineRequest):
    """Execute pipeline in background thread."""
    update_task(task_id, status=TaskStatus.RUNNING)
    try:
        output_dir = HETAGEN_DATA_DIR / "tasks" / task_id
        output_dir.mkdir(parents=True, exist_ok=True)
        output_dir = str(output_dir)

        result = run_pipeline(
            question=request.question,
            sql_question=request.sql_question,
            output_dir=output_dir,
            top_k=request.top_k,
            threshold=request.threshold,
            max_workers=request.max_workers,
            verbose=False,
        )
        csv_data = []
        with open(result["csv_path"], "r", encoding="utf-8") as f:
            csv_data = list(csv.DictReader(f))

        task = get_task(task_id)
        task.metadata["result"] = {
            "table_name": result["table_name"],
            "schema": result["schema"],
            "csv_data": csv_data,
            "sql": result["sql"],
            "query_results": result["results"],
        }
        update_task(task_id, status=TaskStatus.COMPLETED)
    except Exception as e:
        logger.exception("Pipeline task %s failed", task_id)
        update_task(task_id, status=TaskStatus.FAILED, error=str(e))


@router.post("/submit")
async def submit_pipeline(request: PipelineRequest, background_tasks: BackgroundTasks):
    """Submit a pipeline task. Returns task_id for status polling."""
    task = create_task("pipeline")
    background_tasks.add_task(run_pipeline_task, task.task_id, request)
    return {"task_id": task.task_id, "status": "pending", "message": "Task submitted"}


@router.get("/status/{task_id}")
async def get_pipeline_status(task_id: str):
    """Query task status. Returns result when completed."""
    task = get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    response = {"task_id": task_id, "status": task.status.value}
    if task.status == TaskStatus.COMPLETED:
        response["result"] = task.metadata.get("result")
    elif task.status == TaskStatus.FAILED:
        response["error"] = task.error
    return response


@router.websocket("/stream")
async def stream_pipeline(websocket: WebSocket):
    """
    Stream pipeline execution via WebSocket.

    Client sends: {"question": "...", "top_k": 5, ...}
    Server sends: {"type": "progress|result|error", ...}
    Cleanup on disconnect.
    """
    await websocket.accept()

    task_id = uuid.uuid4().hex[:12]
    output_dir = HETAGEN_DATA_DIR / "tasks" / task_id
    success = False

    try:
        data = await websocket.receive_json()
        question = data.get("question")
        if not question:
            await websocket.send_json({"type": "error", "message": "Question required"})
            return

        output_dir.mkdir(parents=True, exist_ok=True)

        loop = asyncio.get_event_loop()

        def progress_callback(step: int):
            asyncio.run_coroutine_threadsafe(
                websocket.send_json({"type": "progress", "step": step}), loop
            )
        result = await loop.run_in_executor(None, lambda: run_pipeline(
            question=question,
            sql_question=data.get("sql_question"),
            output_dir=str(output_dir),
            top_k=data.get("top_k", 5),
            threshold=data.get("threshold", 0.5),
            max_workers=data.get("max_workers", 16),
            progress_callback=progress_callback,
        ))

        csv_data = []
        with open(result["csv_path"], "r", encoding="utf-8") as f:
            csv_data = list(csv.DictReader(f))

        await websocket.send_json({
            "type": "result",
            "data": {
                "table_name": result["table_name"],
                "schema": result["schema"],
                "csv_data": csv_data,
                "sql": result["sql"],
                "query_results": result["results"],
            }
        })
        success = True

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected: %s", task_id)
    except Exception as e:
        logger.exception("Pipeline stream failed")
        try:
            await websocket.send_json({"type": "error", "message": str(e)})
        except Exception:
            pass
    finally:
        if not success and output_dir.exists():
            shutil.rmtree(output_dir, ignore_errors=True)
