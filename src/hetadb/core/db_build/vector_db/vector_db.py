"""Milvus vector database operations.

Provides collection management, batch insert/delete, and similarity search
for KG entity nodes, relations, and text chunks.
"""

import json
import logging
import time
from typing import Any

from pymilvus import (
    Collection,
    CollectionSchema,
    DataType,
    FieldSchema,
    connections,
    db,
    utility,
)

from hetadb.utils.load_config import get_milvus_config

logger = logging.getLogger(__name__)

_DESCRIPTION_MAX_LEN = 4096
_NODENAME_MAX_LEN = 1024


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

_MILVUS_DATABASES = ("hetadb", "hetagen", "hetamem")


def ensure_milvus_databases(retries: int = 6, delay: float = 10.0) -> None:
    """Create required Milvus databases if they do not exist.

    Connects without a specific database, then creates each required database.
    Safe to call repeatedly — existing databases are skipped.
    Retries on connection failure to handle slow Milvus gRPC startup.
    """
    cfg = get_milvus_config()
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            connections.connect(host=cfg["host"], port=cfg["port"])
            existing = set(db.list_database())
            for name in _MILVUS_DATABASES:
                if name not in existing:
                    db.create_database(name)
                    logger.info("Created Milvus database: %s", name)
                else:
                    logger.debug("Milvus database already exists: %s", name)
            return
        except Exception as e:
            last_exc = e
            logger.warning(
                "Milvus not ready (attempt %d/%d): %s — retrying in %.0fs",
                attempt, retries, e, delay,
            )
            if attempt < retries:
                time.sleep(delay)
    logger.error("Failed to ensure Milvus databases after %d attempts: %s", retries, last_exc)
    raise last_exc


def connect_milvus() -> None:
    """Connect to Milvus using config from ``config.yaml``."""
    try:
        cfg = get_milvus_config()
        connections.connect(host=cfg["host"], port=cfg["port"], db_name=cfg["db_name"])
        logger.info("Connected to Milvus at %s:%s (db=%s)", cfg["host"], cfg["port"], cfg["db_name"])
    except Exception as e:
        logger.error("Failed to connect to Milvus: %s", e)
        raise


# ---------------------------------------------------------------------------
# Collection management
# ---------------------------------------------------------------------------

def ensure_nodes_collection(collection_name: str, dim: int = 1024) -> Collection:
    """Return the node collection, creating it if it does not exist."""
    connect_milvus()
    if utility.has_collection(collection_name):
        logger.info("Collection %s already exists", collection_name)
        collection = Collection(collection_name)
        collection.load()
        return collection

    fields = [
        FieldSchema(name="id", dtype=DataType.VARCHAR, max_length=255, is_primary=True),
        FieldSchema(name="nodename", dtype=DataType.VARCHAR, max_length=1024),
        FieldSchema(name="description", dtype=DataType.VARCHAR, max_length=_DESCRIPTION_MAX_LEN),
        FieldSchema(name="type", dtype=DataType.VARCHAR, max_length=64),
        FieldSchema(name="subtype", dtype=DataType.VARCHAR, max_length=64),
        FieldSchema(name="attr", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=dim),
    ]

    schema = CollectionSchema(fields, description="KG entity deduplication collection")
    collection = Collection(name=collection_name, schema=schema)
    collection.create_index(
        field_name="embedding",
        index_params={"metric_type": "IP", "index_type": "IVF_FLAT", "params": {"nlist": 1024}},
    )
    collection.load()
    logger.info("Created collection %s with dim=%d", collection_name, dim)
    return collection


def ensure_rel_collection(collection_name: str, dim: int = 1024) -> Collection:
    """Return the relation collection, creating it if it does not exist."""
    if utility.has_collection(collection_name):
        logger.info("Collection %s already exists", collection_name)
        collection = Collection(collection_name)
        collection.load()
        return collection

    fields = [
        FieldSchema(name="id", dtype=DataType.VARCHAR, max_length=255, is_primary=True),
        FieldSchema(name="node1", dtype=DataType.VARCHAR, max_length=1024),
        FieldSchema(name="node2", dtype=DataType.VARCHAR, max_length=1024),
        FieldSchema(name="relation", dtype=DataType.VARCHAR, max_length=256),
        FieldSchema(name="type", dtype=DataType.VARCHAR, max_length=64),
        FieldSchema(name="description", dtype=DataType.VARCHAR, max_length=_DESCRIPTION_MAX_LEN),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=dim),
    ]

    schema = CollectionSchema(fields, description="KG relation deduplication collection")
    collection = Collection(name=collection_name, schema=schema)
    collection.create_index(
        field_name="embedding",
        index_params={"metric_type": "IP", "index_type": "IVF_FLAT", "params": {"nlist": 1024}},
    )
    collection.load()
    logger.info("Created relation collection %s with dim=%d", collection_name, dim)
    return collection


def ensure_chunk_collection(collection_name: str, embedding_dim: int) -> Collection:
    """Return the chunk collection, creating it if it does not exist."""
    if collection_name in utility.list_collections():
        logger.info("Collection %s already exists", collection_name)
        collection = Collection(collection_name)
        collection.load()
        return collection

    fields = [
        FieldSchema(name="chunk_id", dtype=DataType.VARCHAR, max_length=255, is_primary=True),
        FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="text_embedding", dtype=DataType.FLOAT_VECTOR, dim=embedding_dim),
        FieldSchema(name="source_id", dtype=DataType.VARCHAR, max_length=1000),
        FieldSchema(name="source_chunk", dtype=DataType.VARCHAR, max_length=65535),
    ]

    schema = CollectionSchema(fields, description="Chunk embeddings")
    collection = Collection(name=collection_name, schema=schema)
    collection.create_index(
        field_name="text_embedding",
        index_params={"metric_type": "IP", "index_type": "IVF_FLAT", "params": {"nlist": 1024}},
    )
    collection.load()
    logger.info("Created chunk collection %s with dim=%d", collection_name, embedding_dim)
    return collection


# ---------------------------------------------------------------------------
# Node operations
# ---------------------------------------------------------------------------

def milvus_to_nodes_record_format(milvus_data: dict[str, Any]) -> dict[str, Any]:
    """Convert Milvus row data to the standard node record format."""
    field_mappings = {
        "id": ["id", "Id", "ID"],
        "nodename": ["nodename", "NodeName", "node_name", "Node_Name"],
        "description": ["description", "Description", "desc", "Desc"],
        "type": ["type", "Type", "TYPE"],
        "subtype": ["subtype", "SubType", "sub_type", "Sub_Type"],
        "embedding": ["embedding", "Embedding", "embed", "Embed"],
    }

    reserved_fields: set[str] = set()
    for aliases in field_mappings.values():
        reserved_fields.update(aliases)

    # Extract standard fields
    extracted: dict[str, Any] = {}
    for std_name, aliases in field_mappings.items():
        value = None
        for alias in aliases:
            if alias in milvus_data:
                value = milvus_data[alias]
                break
        if std_name in ("type", "subtype", "nodename", "description", "id"):
            extracted[std_name] = value if value is not None else ""
        else:
            extracted[std_name] = value

    # Truncate nodename / description to Milvus max_length
    if len(extracted.get("nodename", "")) > _NODENAME_MAX_LEN:
        extracted["nodename"] = extracted["nodename"][:_NODENAME_MAX_LEN]
    if len(extracted.get("description", "")) > _DESCRIPTION_MAX_LEN:
        extracted["description"] = extracted["description"][:_DESCRIPTION_MAX_LEN]

    # Merge extra fields into attr
    attr_dict: dict[str, Any] = {}
    attr_str = milvus_data.get("attr", "{}")
    try:
        attr_dict = json.loads(attr_str) if attr_str else {}
    except (json.JSONDecodeError, TypeError):
        attr_dict = {}

    for key, value in milvus_data.items():
        if key not in reserved_fields:
            attr_dict[key] = value

    return {
        "id": extracted.get("id") or "",
        "nodename": extracted.get("nodename") or "",
        "description": extracted.get("description") or "",
        "type": extracted.get("type") or "",
        "subtype": extracted.get("subtype") or "",
        "attr": json.dumps(attr_dict, ensure_ascii=False),
        "embedding": extracted.get("embedding", []),
    }


def insert_nodes_records_to_milvus(
    collection: Collection, records: list[dict[str, Any]],
) -> None:
    """Batch-insert node records into Milvus, skipping invalid embeddings."""
    if not records:
        return

    emb_dim = None
    for field in collection.schema.fields:
        if field.name == "embedding":
            emb_dim = field.params.get("dim")
            break
    if emb_dim is None:
        logger.error("Cannot determine embedding dim, skipping insert")
        return

    valid_records: list[dict[str, Any]] = []
    dropped = 0
    for rec in records:
        emb = rec.get("embedding")
        if not isinstance(emb, list) or len(emb) != emb_dim:
            dropped += 1
            continue
        valid_records.append(rec)

    if not valid_records:
        logger.warning("No valid records to insert")
        return

    milvus_data = [milvus_to_nodes_record_format(r) for r in valid_records]

    ids = [d["id"] for d in milvus_data]
    nodenames = [d["nodename"] for d in milvus_data]
    descriptions = [d["description"] for d in milvus_data]
    types = [d.get("type") or "" for d in milvus_data]
    subtypes = [d.get("subtype") or "" for d in milvus_data]
    attrs = [d["attr"] for d in milvus_data]
    embeddings = [d["embedding"] for d in milvus_data]

    try:
        collection.insert([ids, nodenames, descriptions, types, subtypes, attrs, embeddings])
        collection.flush()
        logger.info(
            "Inserted %d node records into Milvus (dropped %d invalid)",
            len(valid_records), dropped,
        )
    except Exception as e:
        logger.error("Failed to insert node records: %s", e)
        raise


def delete_nodes_records_from_milvus(
    collection: Collection, ids: list[str],
) -> None:
    """Delete node records by ID from Milvus."""
    if not ids:
        return
    try:
        expr = f"id in {json.dumps(ids)}"
        collection.delete(expr)
        collection.flush()
        logger.info("Deleted %d node records from Milvus", len(ids))
    except Exception as e:
        logger.error("Failed to delete node records: %s", e)
        raise


def search_similar_entities(
    collection: Collection, embedding: list[float], top_k: int = 10,
) -> list[dict[str, Any]]:
    """Search for similar entity nodes in Milvus by embedding vector."""
    if not embedding:
        return []
    try:
        results = collection.search(
            data=[embedding],
            anns_field="embedding",
            param={"metric_type": "IP", "params": {"nprobe": 10}},
            limit=top_k,
            output_fields=["id", "nodename", "description", "type", "subtype", "attr"],
        )
        similar: list[dict[str, Any]] = []
        if results and len(results) > 0:
            for hit in results[0]:
                similar.append({
                    "id": hit.entity.get("id"),
                    "nodename": hit.entity.get("nodename"),
                    "description": hit.entity.get("description"),
                    "type": hit.entity.get("type"),
                    "subtype": hit.entity.get("subtype"),
                    "attr": hit.entity.get("attr"),
                    "score": hit.score,
                })
        return similar
    except Exception as e:
        logger.error("Entity search failed: %s", e)
        return []


# ---------------------------------------------------------------------------
# Relation operations
# ---------------------------------------------------------------------------

def rel_milvus_to_record_format(data: dict[str, Any]) -> dict[str, Any]:
    """Convert a Milvus relation row to the standard relation record format."""
    return {
        "Id": data.get("id", ""),
        "Node1": data.get("node1", ""),
        "Node2": data.get("node2", ""),
        "Relation": data.get("relation", ""),
        "Type": data.get("type", ""),
        "Description": data.get("description", ""),
        "embedding": data.get("embedding", []),
    }


def rel_record_to_milvus_format(record: dict[str, Any]) -> dict[str, Any]:
    """Convert a standard relation record to Milvus insert format."""
    emb = record.get("embedding", [])
    if not isinstance(emb, list):
        emb = []
    desc = record.get("Description") or ""
    if len(desc) > _DESCRIPTION_MAX_LEN:
        desc = desc[:_DESCRIPTION_MAX_LEN]
    node1 = record.get("Node1") or ""
    if len(node1) > _NODENAME_MAX_LEN:
        node1 = node1[:_NODENAME_MAX_LEN]
    node2 = record.get("Node2") or ""
    if len(node2) > _NODENAME_MAX_LEN:
        node2 = node2[:_NODENAME_MAX_LEN]
    return {
        "id": record.get("Id") or "",
        "node1": node1,
        "node2": node2,
        "relation": record.get("Relation") or "",
        "type": record.get("Type") or "",
        "description": desc,
        "embedding": emb,
    }


def insert_relations_to_milvus(
    collection: Collection, records: list[dict[str, Any]],
) -> None:
    """Batch-insert relation records into Milvus, skipping invalid embeddings."""
    if not records:
        return

    emb_dim = None
    for field in collection.schema.fields:
        if field.name == "embedding":
            emb_dim = field.params.get("dim")
            break
    if emb_dim is None:
        logger.error("Cannot determine embedding dim, skipping insert")
        return

    valid_records: list[dict[str, Any]] = []
    dropped = 0
    for rec in records:
        emb = rec.get("embedding")
        if not isinstance(emb, list) or len(emb) != emb_dim:
            dropped += 1
            continue
        valid_records.append(rec)

    if not valid_records:
        logger.warning("No valid records to insert")
        return

    rows = [rel_record_to_milvus_format(r) for r in valid_records]
    ids = [r["id"] for r in rows]
    node1s = [r["node1"] for r in rows]
    node2s = [r["node2"] for r in rows]
    relations = [r["relation"] for r in rows]
    types = [r.get("type") or "" for r in rows]
    descriptions = [r["description"] for r in rows]
    embeddings = [r["embedding"] for r in rows]

    try:
        collection.insert([ids, node1s, node2s, relations, types, descriptions, embeddings])
        collection.flush()
        logger.info(
            "Inserted %d relation records into Milvus (dropped %d invalid)",
            len(valid_records), dropped,
        )
    except Exception as e:
        logger.error("Failed to insert relation records: %s", e)
        raise


def delete_relations_from_milvus(
    collection: Collection, ids: list[str],
) -> None:
    """Delete relation records by ID from Milvus."""
    if not ids:
        return
    try:
        expr = f"id in {json.dumps(ids)}"
        collection.delete(expr)
        collection.flush()
        logger.info("Deleted %d relation records from Milvus", len(ids))
    except Exception as e:
        logger.error("Failed to delete relation records: %s", e)
        raise


def search_similar_relations(
    collection: Collection, embedding: list[float], top_k: int = 10,
) -> list[dict[str, Any]]:
    """Search for similar relations in Milvus by embedding vector."""
    if not embedding:
        return []
    try:
        results = collection.search(
            data=[embedding],
            anns_field="embedding",
            param={"metric_type": "IP", "params": {"nprobe": 10}},
            limit=top_k,
            output_fields=["id", "node1", "node2", "relation", "type", "description"],
        )
        similar: list[dict[str, Any]] = []
        if results and len(results) > 0:
            for hit in results[0]:
                similar.append({
                    "id": hit.entity.get("id"),
                    "node1": hit.entity.get("node1"),
                    "node2": hit.entity.get("node2"),
                    "relation": hit.entity.get("relation"),
                    "type": hit.entity.get("type"),
                    "description": hit.entity.get("description"),
                    "score": hit.score,
                })
        return similar
    except Exception as e:
        logger.error("Relation search failed: %s", e)
        return []


# ---------------------------------------------------------------------------
# Chunk operations
# ---------------------------------------------------------------------------

def get_chunk_text_by_id(
    collection: Collection, chunk_id: str,
) -> tuple[str, list, str, str] | None:
    """Retrieve chunk fields from Milvus by chunk_id."""
    try:
        expr = f'chunk_id == "{chunk_id}"'
        res = collection.query(
            expr=expr,
            output_fields=["text", "text_embedding", "source_chunk", "source_id"],
        )
        if res and isinstance(res, list) and len(res) > 0:
            row = res[0]
            return (
                row.get("text"),
                row.get("text_embedding"),
                row.get("source_chunk"),
                row.get("source_id"),
            )
    except Exception as e:
        logger.debug("Failed to query chunk_id=%s from Milvus: %s", chunk_id, e)
    return None


def insert_chunk_batch_milvus(
    collection: Collection, batch: list[dict[str, Any]],
) -> None:
    """Batch-insert chunk records into Milvus."""
    if not batch:
        return

    chunk_ids = [item.get("chunk_id") for item in batch]
    texts = [item.get("text") for item in batch]
    embeddings = [item.get("embedding") for item in batch]
    sources = [item.get("source", "") for item in batch]
    source_chunks = [
        item.get("source_chunk", json.dumps([item.get("chunk_id")])) for item in batch
    ]

    try:
        collection.insert([chunk_ids, texts, embeddings, sources, source_chunks])
        collection.flush()
        logger.info("Inserted %d chunks into collection %s", len(batch), collection.name)
    except Exception as e:
        logger.error("Failed to insert chunks into Milvus: %s", e)
        raise


def delete_chunks_by_source_ids(
    collection_name: str, source_ids: list[str],
) -> int:
    """Delete chunk records from Milvus by source_id.

    Returns:
        Number of deleted records.
    """
    if not source_ids:
        return 0

    try:
        if not utility.has_collection(collection_name):
            logger.warning("Collection %s does not exist", collection_name)
            return 0

        collection = Collection(collection_name)
        collection.load()

        source_ids_str = ", ".join(f'"{sid}"' for sid in source_ids)
        expr = f"source_id in [{source_ids_str}]"

        query_result = collection.query(expr=expr, output_fields=["chunk_id"])
        count = len(query_result) if query_result else 0

        if count > 0:
            collection.delete(expr)
            collection.flush()
            logger.info(
                "Deleted %d chunks from %s for %d sources",
                count, collection_name, len(source_ids),
            )
        else:
            logger.info(
                "No chunks found in %s for %d sources",
                collection_name, len(source_ids),
            )

        return count
    except Exception as e:
        logger.error("Failed to delete chunks from Milvus: %s", e)
        raise
