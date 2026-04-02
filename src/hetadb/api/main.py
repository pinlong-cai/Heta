"""
HetaDB API entry point.

Provides REST endpoints for chat, file management, and configuration.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from hetadb.api.routers import chat_router, files_router, config_router
from hetadb.api.routers.files import cleanup_stale_upload_sessions
from hetadb.config.log_config import setup_logging, load_config
from hetadb.core.db_build.vector_db.vector_db import ensure_milvus_databases

setup_logging()
logger = logging.getLogger("hetadb")

cfg = load_config()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Ensuring Milvus databases...")
    ensure_milvus_databases()
    cleanup_stale_upload_sessions()
    yield


app = FastAPI(title=cfg.get("title", "HetaDB API"), version=cfg.get("version", "0.1.0"), lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(chat_router)
app.include_router(files_router)
app.include_router(config_router)

@app.get("/")
async def root():
    return {"service": cfg.get("title"), "version": cfg.get("version")}


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    fastapi_cfg = cfg.get("fastapi", {})
    host = fastapi_cfg.get("host", "0.0.0.0")
    port = fastapi_cfg.get("port", 8001)

    logger.info("Starting HetaDB API on %s:%s", host, port)

    uvicorn.run(
        "hetadb.api.main:app",
        host=host,
        port=port,
        reload=fastapi_cfg.get("reload", False),
        log_level=fastapi_cfg.get("log_level", "info"),
    )
