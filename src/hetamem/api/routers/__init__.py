"""HetaMem API router registry."""

from hetamem.api.routers.kb import router as kb_router
from hetamem.api.routers.vg import router as vg_router

__all__ = [
    "kb_router",
    "vg_router",
]
