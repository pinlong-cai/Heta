"""Mode registry — maps parse_mode integers to allowed chat query modes.

This is the single source of truth for which query strategies are available
for each processing pipeline.  Both the API layer and the frontend derive
their mode lists from this registry, so adding a new mode here is the only
change required to expose it end-to-end.

Usage::

    from hetadb.core.mode_registry import get_query_modes, validate

    modes = get_query_modes(parse_mode=0)  # list[QueryMode]
    ok = validate(parse_mode=0, query_mode_id="naive")  # True
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class QueryMode:
    """A single chat retrieval strategy."""

    id: str
    label: str
    desc: str


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------
# Key: parse_mode (int) written into dataset _meta.json during processing.
# Value: ordered list of QueryModes available for that processing pipeline.
#
# To add a new parse_mode or query strategy:
#   1. Add an entry here.
#   2. Register the handler in chat_processor._HANDLERS.
#   That's it — the API and frontend pick up the change automatically.

REGISTRY: dict[int, list[QueryMode]] = {
    0: [
        QueryMode(
            id="naive",
            label="KB Retrieval",
            desc="Direct vector similarity search over chunked knowledge base.",
        ),
        QueryMode(
            id="rerank",
            label="Hybrid Rerank",
            desc="BM25 + vector retrieval fused via RRF, optionally reranked by a cross-encoder.",
        ),
        QueryMode(
            id="rewriter",
            label="Query Rewriter",
            desc="Expands the query into multiple variations before retrieval.",
        ),
        QueryMode(
            id="multihop",
            label="Multi-hop",
            desc="Multi-step graph traversal for complex reasoning queries.",
        ),
        QueryMode(
            id="direct",
            label="Direct LLM",
            desc="Skips retrieval entirely and queries the LLM directly.",
        ),
    ],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_query_modes(parse_mode: int) -> list[QueryMode]:
    """Return all query modes available for *parse_mode*."""
    return list(REGISTRY.get(parse_mode, []))


def get_supported_parse_modes() -> set[int]:
    """Return the set of parse_mode values that have registered query modes."""
    return set(REGISTRY.keys())


def validate(parse_mode: int, query_mode_id: str) -> bool:
    """Return True if *query_mode_id* is valid for *parse_mode*."""
    return any(m.id == query_mode_id for m in REGISTRY.get(parse_mode, []))
