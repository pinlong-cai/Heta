"""
Hetagen API entry point.

Provides REST endpoints for table generation pipeline and tag tree parsing.
"""

import logging
from contextlib import asynccontextmanager

import colorlog
from fastapi import FastAPI

from hetagen.api.routers import pipeline, tag_tree
from hetadb.core.db_build.vector_db.vector_db import ensure_milvus_databases

_root = logging.getLogger()
if not _root.handlers:
    _handler = colorlog.StreamHandler()
    _handler.setFormatter(colorlog.ColoredFormatter(
        "%(asctime)s | %(log_color)s%(levelname)-8s%(reset)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "bold_red",
        },
    ))
    _root.setLevel(logging.INFO)
    _root.addHandler(_handler)

logger = logging.getLogger("hetagen")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Ensuring Milvus databases...")
    ensure_milvus_databases()
    yield


app = FastAPI(title="Hetagen API", version="0.1.0", lifespan=lifespan)

app.include_router(pipeline.router)
app.include_router(tag_tree.router)


@app.get("/")
async def root():
    return {"message": "Hetagen API", "version": "0.1.0"}


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
