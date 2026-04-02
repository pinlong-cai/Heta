"""
HetaMem API entry point.

Provides REST endpoints for personal memory (MemoryVG) and
knowledge-base memory (MemoryKB).
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from hetamem.config.log_config import setup_logging, load_config
from hetamem.api.routers.kb import router as kb_router
from hetamem.api.routers.vg import router as vg_router
from hetamem.memkb_manager import manager as kb_manager
from hetamem.memvg_manager import manager as vg_manager
from hetadb.core.db_build.vector_db.vector_db import ensure_milvus_databases

setup_logging()
logger = logging.getLogger("hetamem")

cfg = load_config()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Ensuring Milvus databases...")
    ensure_milvus_databases()
    logger.info("Initializing MemoryKB...")
    await kb_manager.initialize()
    logger.info("Initializing MemoryVG...")
    await vg_manager.initialize()
    yield


app = FastAPI(
    title=cfg.get("title", "HetaMem API"),
    version=cfg.get("version", "0.1.0"),
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(kb_router)
app.include_router(vg_router)


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
    port = fastapi_cfg.get("port", 8003)

    logger.info("Starting HetaMem API on %s:%s", host, port)

    uvicorn.run(
        "hetamem.api.main:app",
        host=host,
        port=port,
        reload=fastapi_cfg.get("reload", False),
        log_level=fastapi_cfg.get("log_level", "info"),
    )
