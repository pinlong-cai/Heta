"""Lightweight entity search for KB-mode tree node enrichment.

Provides a single function that embeds a query string and searches the Milvus
entity collections for a given KB, returning the top-K matching entities with
their names and descriptions.

Entity collection naming convention: ``{kb_name}__{dataset}_entity_collection``
"""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Number of search results to retrieve per dataset before merging
_SEARCH_LIMIT_PER_DATASET = 10


def search_kb_entities(
    query: str,
    kb_name: str,
    datasets: list[str],
    embedding_cfg: dict,
    top_k: int = 5,
) -> list[dict]:
    """Search KB entity collections for the most relevant entities to *query*.

    Searches across all datasets in the KB and returns the globally top-*top_k*
    results ranked by inner-product similarity.

    Args:
        query:         Search query (typically the tree node name + category path).
        kb_name:       Knowledge base name.
        datasets:      Dataset names belonging to this KB.
        embedding_cfg: Dict with keys ``api_key``, ``embedding_url``,
                       ``embedding_model``, ``embedding_timeout``.
        top_k:         Number of results to return.

    Returns:
        List of dicts with ``nodename`` and ``description`` keys, sorted by
        relevance.  Returns an empty list on error or if no collections exist.
    """
    if not datasets:
        return []

    try:
        from pymilvus import Collection, utility
        from hetadb.core.db_build.vector_db.vector_db import connect_milvus
        from hetadb.core.db_build.graph_db.graph_vector import embedding as get_embedding
    except ImportError as e:
        logger.error("Failed to import search dependencies: %s", e)
        return []

    # Embed the query
    try:
        vectors = get_embedding(
            texts=[query],
            api_key=embedding_cfg["api_key"],
            embedding_url=embedding_cfg["embedding_url"],
            embedding_model=embedding_cfg["embedding_model"],
            embedding_timeout=embedding_cfg.get("embedding_timeout", 30),
        )
        query_vector = vectors[0]
    except Exception as e:
        logger.error("Failed to embed query '%s': %s", query, e)
        return []

    try:
        connect_milvus()
    except Exception as e:
        logger.error("Milvus connection failed: %s", e)
        return []

    all_hits: list[dict] = []
    search_params = {"ef": 64}

    for dataset in datasets:
        collection_name = f"{kb_name}__{dataset}_entity_collection"
        if not utility.has_collection(collection_name):
            logger.debug("Entity collection not found: %s", collection_name)
            continue
        try:
            col = Collection(collection_name)
            col.load()
            results = col.search(
                data=[query_vector],
                anns_field="embedding",
                param=search_params,
                limit=_SEARCH_LIMIT_PER_DATASET,
                expr="",
                output_fields=["nodename", "description"],
            )
            if results and results[0]:
                for hit in results[0]:
                    item = hit.entity if hasattr(hit, "entity") else hit.data
                    all_hits.append({
                        "nodename": item.get("nodename", ""),
                        "description": item.get("description", ""),
                        "score": hit.distance,
                    })
        except Exception as e:
            logger.warning("Search failed on collection %s: %s", collection_name, e)

    # Sort by score descending and deduplicate by nodename
    all_hits.sort(key=lambda x: x["score"], reverse=True)
    seen: set[str] = set()
    unique_hits: list[dict] = []
    for hit in all_hits:
        name = hit["nodename"]
        if name and name not in seen:
            seen.add(name)
            unique_hits.append({"nodename": name, "description": hit["description"]})
        if len(unique_hits) >= top_k:
            break

    return unique_hits
