"""Standalone vector similarity retrieval against a Milvus collection."""

import json
import logging
import requests
import numpy as np
import yaml
from sentence_transformers import SentenceTransformer
from pymilvus import connections, Collection

from common.config import get_persistence
from hetagen.utils.path import PROJECT_ROOT

logger = logging.getLogger(__name__)

with open(PROJECT_ROOT / "config.yaml", encoding="utf-8") as _f:
    _cfg = yaml.safe_load(_f)["hetagen"]
    _milvus_cfg = _cfg["milvus"]
    _emb_cfg = _cfg["embedding_api"]

_milvus_globals = get_persistence("milvus")

MODEL_PATH = "/home/fanwenzhuo/Documents/models/bge-m3"
MILVUS_HOST: str = _milvus_globals["host"]
MILVUS_PORT: str = str(_milvus_globals["port"])
MILVUS_DB: str = _milvus_cfg["db_name"]
COLLECTION_NAME: str = _milvus_cfg["collection_name"]

# Remote embedding configuration
USE_REMOTE_EMBEDDING = True
REMOTE_EMBEDDING_API_KEY: str = _emb_cfg["api_key"]
REMOTE_EMBEDDING_BASE_URL: str = _emb_cfg["base_url"].rstrip("/")
REMOTE_EMBEDDING_MODEL: str = _emb_cfg["model"]


def connect_milvus() -> None:
    """Connect to the Milvus service."""
    connections.connect(
        alias="autotable",
        host=MILVUS_HOST,
        port=MILVUS_PORT,
        db_name=MILVUS_DB,
    )


def disconnect_milvus() -> None:
    """Disconnect from Milvus."""
    connections.disconnect("autotable")


def get_query_embedding(query: str, model: SentenceTransformer) -> list:
    """Vectorize a query string using a local SentenceTransformer model.

    Args:
        query: Query text.
        model: SentenceTransformer model instance.

    Returns:
        Normalized embedding as a list of floats.
    """
    embedding = model.encode(
        query,
        normalize_embeddings=True  # bge-m3 recommends normalized embeddings
    )
    return embedding.tolist()


def search_similar_chunks(
    query_embedding: list,
    collection: Collection,
    top_k: int,
    threshold: float
) -> list[dict]:
    """Search for similar text chunks in Milvus.

    Args:
        query_embedding: Query vector.
        collection: Milvus Collection.
        top_k: Maximum number of results to return.
        threshold: Similarity score threshold.

    Returns:
        List of dicts with ``id``, ``content``, and ``score`` keys.
    """
    search_params = {
        "metric_type": "IP",  # inner product (suitable for normalized vectors)
        "params": {"nprobe": 16}
    }

    results = collection.search(
        data=[query_embedding],
        anns_field="embedding",
        param=search_params,
        limit=top_k,
        output_fields=["id", "content"]
    )

    # Filter results below the threshold
    retrieved_chunks = []
    for hits in results:
        for hit in hits:
            score = hit.score
            if score >= threshold:
                retrieved_chunks.append({
                    "id": hit.entity.get("id"),
                    "content": hit.entity.get("content"),
                    "score": round(score, 4)
                })

    return retrieved_chunks


def retrieve(query: str, top_k: int, threshold: float) -> list[dict]:
    """Main retrieval function: vectorize a query and return similar chunks.

    Args:
        query: Query text.
        top_k: Maximum number of results to return.
        threshold: Similarity score threshold (0-1).

    Returns:
        List of dicts with ``id``, ``content``, and ``score`` keys.
    """
    logger.debug("Vectorizing query: %s", query[:50])

    if USE_REMOTE_EMBEDDING:
        query_embedding = get_remote_embedding(query)
    else:
        logger.info("Loading local model: %s", MODEL_PATH)
        model = SentenceTransformer(MODEL_PATH, device='cpu')
        query_embedding = get_query_embedding(query, model)

    try:
        logger.info("Connecting to Milvus at %s:%s", MILVUS_HOST, MILVUS_PORT)
        connect_milvus()

        collection = Collection(name=COLLECTION_NAME, using="autotable")
        collection.load()

        logger.debug("Searching top_k=%d threshold=%.2f", top_k, threshold)
        results = search_similar_chunks(
            query_embedding=query_embedding,
            collection=collection,
            top_k=top_k,
            threshold=threshold
        )

        logger.debug("Found %d matching chunks", len(results))
        return results

    finally:
        disconnect_milvus()

def get_remote_embedding(text: str) -> list:
    """Call the remote embedding API."""
    headers = {
        "Authorization": f"Bearer {REMOTE_EMBEDDING_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": REMOTE_EMBEDDING_MODEL,
        "input": text,
        "encoding_format": "float"
    }

    response = requests.post(
        f"{REMOTE_EMBEDDING_BASE_URL}/embeddings",
        headers=headers,
        json=payload,
        timeout=30
    )

    response.raise_for_status()
    embedding = response.json()["data"][0]["embedding"]

    # L2-normalize to unit vector
    embedding = np.array(embedding)
    embedding = embedding / np.linalg.norm(embedding)

    return embedding.tolist()


if __name__ == '__main__':
    QUERY = "比较苹果和微软的市场估值和营收"
    THRESHOLD = 0.5
    TOP_K = 5
    # Run retrieval
    results = retrieve(
        query=QUERY,
        top_k=TOP_K,
        threshold=THRESHOLD
    )

    # Output results
    output_json = json.dumps(results, ensure_ascii=False, indent=2)
    print("\n========== Retrieval Results ==========")
    print(output_json)
