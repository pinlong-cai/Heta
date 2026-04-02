"""HetaDB API router registry."""

from hetadb.api.routers.chat import router as chat_router
from hetadb.api.routers.files import router as files_router
from hetadb.api.routers.config import router as config_router
from hetadb.api.routers.schema import router as schema_router

__all__ = [
    "chat_router",
    "files_router",
    "config_router",
    "schema_router",
]