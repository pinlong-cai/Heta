"""
MemoryVG service manager.

Loads memoryvg config from config.yaml and passes it directly to
AsyncMemory.from_config(), then exposes CRUD methods for the API layer.
"""

import logging

import yaml

from common.config import get_persistence
from hetamem.utils.path import PROJECT_ROOT

logger = logging.getLogger("hetamem")


def _load_memoryvg_config() -> dict:
    with open(PROJECT_ROOT / "config.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f).get("hetamem", {}).get("memoryvg", {})
    # Normalise field name: config.yaml uses base_url everywhere; MemoryVG
    # expects openai_base_url in llm/embedder config dicts.
    for section in ("llm", "embedder"):
        inner = cfg.get(section, {}).get("config", {})
        if "base_url" in inner:
            inner.setdefault("openai_base_url", inner.pop("base_url"))
    # Inject Milvus URL (shared persistence) into vector_store config.
    vs = cfg.get("vector_store", {})
    vs.setdefault("config", {})["url"] = get_persistence("milvus")["url"]
    cfg["vector_store"] = vs
    # Inject Neo4j config (shared persistence) into graph_store config.
    gs = cfg.get("graph_store", {})
    gs["config"] = get_persistence("neo4j")
    cfg["graph_store"] = gs
    return cfg


class MemoryVGManager:
    """Service layer over the MemoryVG personal memory store."""

    def __init__(self) -> None:
        self._mem = None

    async def initialize(self) -> None:
        """Initialize AsyncMemory. Call once at application startup."""
        if self._mem is not None:
            return
        from MemoryVG.memory.main import AsyncMemory

        self._mem = await AsyncMemory.from_config(config_dict=_load_memoryvg_config())
        logger.info("MemoryVG initialized")

    async def add(
        self,
        messages: list[dict],
        user_id: str | None = None,
        agent_id: str | None = None,
        run_id: str | None = None,
        metadata: dict | None = None,
    ) -> dict:
        return await self._mem.add(
            messages,
            user_id=user_id,
            agent_id=agent_id,
            run_id=run_id,
            metadata=metadata,
        )

    async def get(self, memory_id: str):
        return await self._mem.get(memory_id)

    async def get_all(
        self,
        user_id: str | None = None,
        agent_id: str | None = None,
        run_id: str | None = None,
        limit: int = 100,
    ) -> dict:
        return await self._mem.get_all(
            user_id=user_id,
            agent_id=agent_id,
            run_id=run_id,
            limit=limit,
        )

    async def search(
        self,
        query: str,
        user_id: str | None = None,
        agent_id: str | None = None,
        run_id: str | None = None,
        limit: int = 10,
        threshold: float | None = None,
    ) -> dict:
        kwargs: dict = dict(
            query=query, user_id=user_id, agent_id=agent_id, run_id=run_id, limit=limit
        )
        if threshold is not None:
            kwargs["threshold"] = threshold
        return await self._mem.search(**kwargs)

    async def update(self, memory_id: str, data: str) -> dict:
        return await self._mem.update(memory_id, data)

    async def delete(self, memory_id: str) -> dict:
        return await self._mem.delete(memory_id)

    async def delete_all(
        self,
        user_id: str | None = None,
        agent_id: str | None = None,
        run_id: str | None = None,
    ) -> dict:
        return await self._mem.delete_all(
            user_id=user_id, agent_id=agent_id, run_id=run_id
        )

    async def history(self, memory_id: str) -> list:
        return await self._mem.history(memory_id)


# Module-level singleton — initialized via FastAPI lifespan in api/main.py.
manager = MemoryVGManager()
