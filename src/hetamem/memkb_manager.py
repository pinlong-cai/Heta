"""
MemoryKB service manager.

Bridges config.yaml into the MemoryKB orchestrator by injecting LLM/embedding
credentials as environment variables (which the underlying LightRAG and
build_memory modules read at import time), then delegates all operations to
the existing orchestrator functions.
"""

import logging
import os

import yaml

from hetamem.utils.path import PROJECT_ROOT

logger = logging.getLogger("hetamem")


def _load_memorykb_config() -> dict:
    with open(PROJECT_ROOT / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f).get("hetamem", {}).get("memorykb", {})


def _inject_env(cfg: dict) -> None:
    """Write LLM and embedding credentials into env vars consumed by MemoryKB internals."""
    llm = cfg.get("llm", {})
    emb = cfg.get("embedding", {})

    # LightRAG and build_memory both read OPENAI_API_KEY / OPENAI_BASE_URL.
    # Use setdefault so explicit env overrides (e.g. from shell) still win.
    os.environ.setdefault("OPENAI_API_KEY",  llm.get("api_key",  ""))
    os.environ.setdefault("OPENAI_BASE_URL", llm.get("base_url", ""))
    os.environ.setdefault("OPENAI_API_BASE", llm.get("base_url", ""))  # build_memory alias
    os.environ.setdefault("OPENAI_MODEL",    llm.get("model", "gpt-4o-mini"))

    # Expose embedding config for any component that reads these vars
    os.environ.setdefault("EMBEDDING_API_KEY",   emb.get("api_key",  llm.get("api_key",  "")))
    os.environ.setdefault("EMBEDDING_BASE_URL",  emb.get("base_url", llm.get("base_url", "")))
    os.environ.setdefault("EMBEDDING_MODEL",     emb.get("model", "text-embedding-3-small"))
    os.environ.setdefault("EMBEDDING_DIM",       str(emb.get("dim", 1024)))


# Inject env vars immediately so that MemoryKB module-level code picks them up
# on the first import (which happens during initialize()).
_inject_env(_load_memorykb_config())


class MemoryKBManager:
    """Service layer over the MemoryKB orchestrator."""

    def __init__(self) -> None:
        self._initialized = False

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        """Initialize LightRAG instances. Call once at application startup."""
        if self._initialized:
            return

        # Import here so env vars are guaranteed to be set first.
        from MemoryKB.Long_Term_Memory.Graph_Construction.lightrag.kg.shared_storage import (
            initialize_share_data,
        )
        from MemoryKB.orchestrator import initialize_rag

        initialize_share_data(workers=1)
        await initialize_rag()

        self._initialized = True
        logger.info("MemoryKB initialized")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def insert(
        self,
        query: str,
        video: bytes | None = None,
        video_name: str | None = None,
        audio: bytes | None = None,
        audio_name: str | None = None,
        image: bytes | None = None,
        image_name: str | None = None,
    ) -> dict:
        """Insert a memory entry. Media is accepted as raw bytes + filename.

        The underlying orchestrator expects file-like objects with .read() and
        .filename; we provide a lightweight shim so callers are not coupled to
        FastAPI's UploadFile.
        """
        from MemoryKB.orchestrator import handle_insert

        class _FileLike:
            def __init__(self, data: bytes, name: str):
                self._data = data
                self.filename = name

            async def read(self) -> bytes:
                return self._data

        return await handle_insert(
            query,
            video=_FileLike(video, video_name) if video and video_name else None,
            audio=_FileLike(audio, audio_name) if audio and audio_name else None,
            image=_FileLike(image, image_name) if image and image_name else None,
        )

    async def query(
        self,
        query: str,
        mode: str = "hybrid",
        use_pm: bool = False,
    ) -> dict:
        """Query the knowledge base and return a synthesized answer."""
        from MemoryKB.orchestrator import handle_query

        return await handle_query(query, mode=mode, use_pm=use_pm)


# Module-level singleton — initialized via FastAPI lifespan in api/main.py.
manager = MemoryKBManager()
