"""Knowledge-base query service.

Provides connection management (Milvus, PostgreSQL, embedding API) and
optimised retrieval routines used by the chat processor pipeline.
"""

from __future__ import annotations

import ast
import logging
import threading
import time
from typing import Any

import numpy as np
import psycopg2
from openai import OpenAI
from psycopg2.extras import RealDictCursor
from pymilvus import Collection, connections, db

from common.llm_client import create_use_llm, create_use_llm_async
from hetadb.utils.load_config import (
    get_chat_cfg,
    get_embedding_cfg,
    get_milvus_config,
    get_postgres_conn_config,
    get_query_defaults,
    get_search_params,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_chunk_ids_from_strings(chunk_id_strings: set[str]) -> set[str]:
    """Parse a set of chunk-id strings (possibly JSON lists) into flat unique IDs.

    Handles plain IDs, ``"['a','b']"`` style strings, and nested lists.
    """
    parsed: set[str] = set()

    def _flatten(obj: Any) -> None:
        if isinstance(obj, str):
            s = obj.strip()
            if s:
                parsed.add(s)
        elif isinstance(obj, list):
            for item in obj:
                _flatten(item)
        else:
            parsed.add(str(obj).strip())

    for raw in chunk_id_strings:
        if not raw or not isinstance(raw, str):
            continue
        raw = raw.strip()
        try:
            _flatten(ast.literal_eval(raw))
        except (ValueError, SyntaxError):
            if raw:
                parsed.add(raw)

    return {cid for cid in parsed if cid}


# ---------------------------------------------------------------------------
# Connection manager
# ---------------------------------------------------------------------------

class ConnectionManager:
    """Centralised manager for Milvus, PostgreSQL, and embedding-API connections."""

    def __init__(self) -> None:
        self._milvus_connected = False
        self._milvus_lock = threading.Lock()
        self._embedding_client: OpenAI | None = None
        self._collections: dict[str, Collection] = {}

        # Lazy config — loaded on first access
        self._milvus_config: dict | None = None
        self._postgres_config: dict | None = None
        self._embedding_config: dict | None = None

    # -- config properties ---------------------------------------------------

    @property
    def milvus_config(self) -> dict:
        if self._milvus_config is None:
            self._milvus_config = get_milvus_config()
        return self._milvus_config

    @property
    def postgres_config(self) -> dict:
        if self._postgres_config is None:
            self._postgres_config = get_postgres_conn_config()
        return self._postgres_config

    @property
    def embedding_config(self) -> dict:
        if self._embedding_config is None:
            cfg = get_embedding_cfg()
            self._embedding_config = {
                "api_key": cfg["api_key"],
                "base_url": cfg["base_url"],
                "model_name": cfg["model"],
                "timeout": int(cfg["timeout"]),
            }
        return self._embedding_config

    # -- Milvus --------------------------------------------------------------

    def connect_milvus(self, dataset: str | None = None) -> None:
        """Ensure Milvus is connected and the correct database is selected."""
        with self._milvus_lock:
            if not self._milvus_connected:
                try:
                    connections.connect(
                        alias=self.milvus_config["alias"],
                        host=self.milvus_config["host"],
                        port=self.milvus_config["port"],
                        db_name=self.milvus_config.get("db_name", "default"),
                    )
                    self._milvus_connected = True
                    logger.info(
                        "Milvus connected: %s:%s (db=%s)",
                        self.milvus_config["host"], self.milvus_config["port"],
                        self.milvus_config.get("db_name", "default"),
                    )
                except Exception:
                    logger.error("Milvus connection failed", exc_info=True)
                    raise

        if self.milvus_config.get("database"):
            db.using_database(self.milvus_config["database"])
            logger.debug("Switched to Milvus database: %s", self.milvus_config["database"])

    def get_collection(
        self, collection_name: str, dataset: str | None = None,
    ) -> Collection:
        """Return a loaded Milvus collection (cached after first load)."""
        self.connect_milvus(dataset)
        cache_key = f"{dataset}_{collection_name}" if dataset else collection_name
        if cache_key not in self._collections:
            collection = Collection(collection_name)
            collection.load()
            self._collections[cache_key] = collection
            logger.info("Loaded collection '%s' (dataset=%s)", collection_name, dataset)
        return self._collections[cache_key]

    def get_kg_collection(self, dataset: str) -> Collection:
        return self.get_collection(f"{dataset}_entity_collection", dataset)

    def get_relation_collection(self, dataset: str) -> Collection:
        return self.get_collection(f"{dataset}_relation_collection", dataset)

    def get_chunk_collection(self, dataset: str) -> Collection:
        return self.get_collection(f"{dataset}_chunk_collection", dataset)

    def get_sentence_collection(self, dataset: str) -> Collection:
        return self.get_collection(f"{dataset}_sentence_collection", dataset)

    # -- Embedding -----------------------------------------------------------

    def get_embedding_client(self) -> OpenAI:
        if self._embedding_client is None:
            self._embedding_client = OpenAI(
                api_key=self.embedding_config["api_key"],
                base_url=self.embedding_config["base_url"],
                timeout=self.embedding_config["timeout"],
            )
            logger.info("Embedding client initialised: %s", self.embedding_config["base_url"])
        return self._embedding_client

    def get_embedding(self, prompt: str, model: str | None = None) -> list[float]:
        """Compute an embedding vector for *prompt*."""
        client = self.get_embedding_client()
        model = model or self.embedding_config["model_name"]
        response = client.embeddings.create(model=model, input=[prompt])
        return response.data[0].embedding

    # -- teardown ------------------------------------------------------------

    def disconnect_all(self) -> None:
        """Release all external connections."""
        try:
            if self._milvus_connected:
                connections.disconnect(self.milvus_config["alias"])
                self._milvus_connected = False
                logger.info("Milvus disconnected")
        except Exception:
            logger.warning("Error disconnecting Milvus", exc_info=True)
        self._collections.clear()
        self._embedding_client = None


# Singleton instance shared across the process
connection_manager = ConnectionManager()


# ---------------------------------------------------------------------------
# Optimised KB query
# ---------------------------------------------------------------------------

class OptimizedKbQuery:
    """Retrieval operations against Milvus vector collections and PostgreSQL."""

    def __init__(self) -> None:
        self.connection_manager = connection_manager

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _wrap_embedding(
        embedding: list[float] | None,
        query: str | None,
        cm: ConnectionManager,
    ) -> list[list[float]]:
        """Return ``[[float, ...]]`` suitable for Milvus search."""
        if embedding is not None:
            return [embedding]
        if query is None:
            raise ValueError("Either query or embedding must be provided")
        return [cm.get_embedding(prompt=query)]

    # -- chunk vector search -------------------------------------------------

    def get_top_similar_chunks(
        self,
        query: str | None = None,
        top_k: int = 10000,
        dataset: str | None = None,
        embedding: list[float] | None = None,
    ) -> tuple[list[str], dict[str, float]]:
        """Return the *top_k* most similar chunk IDs from Milvus with their scores."""
        try:
            collection = self.connection_manager.get_chunk_collection(dataset)
            emb_data = self._wrap_embedding(embedding, query, self.connection_manager)

            results = collection.search(
                data=emb_data,
                anns_field="text_embedding",
                param={"metric_type": "IP", "params": {"nprobe": 128}},
                limit=top_k,
                expr="",
                output_fields=["chunk_id"],
            )

            chunk_ids: list[str] = []
            chunk_scores: dict[str, float] = {}
            if results and len(results) > 0:
                for hit in results[0]:
                    cid = hit.entity.get("chunk_id")
                    if cid:
                        chunk_ids.append(cid)
                        chunk_scores[cid] = float(hit.distance)

            logger.info("Retrieved %d similar chunks from Milvus", len(chunk_ids))
            return chunk_ids, chunk_scores

        except Exception:
            logger.error("Failed to retrieve similar chunks", exc_info=True)
            return [], {}

    # -- KG source retrieval -------------------------------------------------

    def query_kg_source(
        self,
        query: str | None = None,
        top_k: int | None = None,
        threshold: float | None = None,
        db_weight: float = 1.5,
        dataset: str | None = None,
        embedding: list[float] | None = None,
    ) -> tuple[list[dict], bool]:
        """Retrieve entity and relation candidates from the knowledge graph.

        Returns:
            ``(results, use_db)`` — the matched KG items and whether table-type
            nodes were found (signalling that SQL queries should be executed).
        """
        defaults = get_query_defaults()
        top_k = top_k or defaults["top_k"]
        threshold = threshold or defaults["threshold"]

        emb_data = self._wrap_embedding(embedding, query, self.connection_manager)

        entity_limit = top_k // 2
        relation_limit = top_k - entity_limit
        search_cfg = get_search_params()
        search_params = {"ef": search_cfg.get("ef_multiplier", 10) * top_k}

        all_results: list[dict] = []
        min_score = float("inf")
        use_db = False
        entity_collection = None

        # 1. Entity search
        try:
            entity_collection = self.connection_manager.get_kg_collection(dataset)
            entity_result = entity_collection.search(
                data=emb_data,
                anns_field="embedding",
                param=search_params,
                limit=entity_limit,
                expr="",
                output_fields=["*"],
            )
            if entity_result and entity_result[0]:
                if entity_result[0].distances:
                    min_score = min(min_score, entity_result[0].distances[-1])
                for record in entity_result[0]:
                    item = record.entity if hasattr(record, "entity") else record.data
                    all_results.append({
                        "id": item.get("id", ""),
                        "type": item.get("type", "entity"),
                        "description": item.get("description", ""),
                        "nodename": item.get("nodename", ""),
                    })
        except Exception:
            logger.warning("Entity collection search failed", exc_info=True)

        # 2. Relation search
        try:
            rel_collection = self.connection_manager.get_relation_collection(dataset)
            relation_result = rel_collection.search(
                data=emb_data,
                anns_field="embedding",
                param=search_params,
                limit=relation_limit,
                expr="",
                output_fields=["*"],
            )
            if relation_result and relation_result[0]:
                if relation_result[0].distances:
                    min_score = min(min_score, relation_result[0].distances[-1])
                for record in relation_result[0]:
                    item = record.entity if hasattr(record, "entity") else record.data
                    all_results.append({
                        "id": item.get("id", ""),
                        "type": "relation",
                        "description": item.get("description", ""),
                        "node1": item.get("node1", ""),
                        "node2": item.get("node2", ""),
                        "relation": item.get("relation", ""),
                    })
        except Exception:
            logger.warning("Relation collection search failed", exc_info=True)

        if not all_results:
            return [], False

        # Check whether any table-type nodes exist
        types_set = {item["type"] for item in all_results}
        if "table" in types_set:
            use_db = True
            logger.debug("Table-type nodes found in KG results")
        else:
            logger.debug("No table-type nodes; checking DB with weight boost")
            try:
                if entity_collection is not None:
                    db_result = entity_collection.query(
                        expr='type=="table"', output_fields=["*"],
                    )
                    for item in db_result:
                        can_emb = [item["embedding"]]
                        if emb_data:
                            ip = np.dot(emb_data, np.array(can_emb).T)[0][0]
                            if ip * db_weight > min_score:
                                use_db = True
                                all_results.append(item)
            except Exception:
                logger.warning("Table-type DB check failed", exc_info=True)

        return all_results, use_db

    # -- batch DB lookup -----------------------------------------------------

    def _query_chunk_ids_by_cluster_ids(
        self, cluster_ids: list[str], dataset: str, cursor,
    ) -> list[str]:
        """Look up chunk IDs corresponding to *cluster_ids*."""
        chunk_ids: list[str] = []
        if cluster_ids and dataset:
            table = f"{dataset}_cluster_chunk_relation"
            cursor.execute(
                f"SELECT chunk_id FROM {table} WHERE cluster_id = ANY(%s)",
                (cluster_ids,),
            )
            chunk_ids = [row["chunk_id"] for row in cursor.fetchall()]
        return chunk_ids

    def _parse_entities_relations(
        self,
        res: list[dict],
        entity_map: dict[str, Any],
        triple_relation_map: dict[str, Any],
        relation_map: dict[str, Any],
        all_entities: dict[str, Any],
    ) -> tuple[list[list[str]], list[list[str]]]:
        """Extract entity and relation lists from DB lookup results."""
        parsed_entities: list[list[str]] = []
        parsed_relations: list[list[str]] = []

        for item in res:
            item_id = item["id"]
            item_type = item["type"]

            if item_type == "entity":
                entity = entity_map.get(item_id)
                if entity:
                    parsed_entities.append([entity["node_name"], entity["description"]])
                    # One-hop relations (capped at 20 per entity)
                    for rel in relation_map.get(entity["node_name"], [])[:20]:
                        parsed_relations.append([rel["node1"], rel["node2"], rel["description"]])

            elif item_type == "relation":
                relation = triple_relation_map.get(item_id)
                if relation:
                    parsed_relations.append([
                        relation["node1"], relation["node2"], relation["description"],
                    ])
                    # Add endpoint entities
                    for node_name in [relation["node1"], relation["node2"]]:
                        if node_name in all_entities:
                            ent = all_entities[node_name]
                            parsed_entities.append([ent["node_name"], ent["description"]])
                        else:
                            ent = next(
                                (e for e in entity_map.values() if e["node_name"] == node_name),
                                None,
                            )
                            if ent:
                                parsed_entities.append([ent["node_name"], ent["description"]])

        return parsed_entities, parsed_relations

    @staticmethod
    def _deduplicate_entities_relations(
        parsed_entities: list[list[str]],
        parsed_relations: list[list[str]],
    ) -> tuple[list[list[str]], list[list[str]]]:
        """Remove duplicate entities and relations."""
        seen_e: set[tuple[str, str]] = set()
        unique_e = []
        for e in parsed_entities:
            key = (e[0], e[1])
            if key not in seen_e:
                unique_e.append(e)
                seen_e.add(key)

        seen_r: set[tuple[str, str, str]] = set()
        unique_r = []
        for r in parsed_relations:
            key = (r[0], r[1], r[2])
            if key not in seen_r:
                unique_r.append(r)
                seen_r.add(key)

        return unique_e, unique_r

    def query_by_res_batch_optimized(  # noqa: PLR0912, PLR0915
        self,
        res: list[dict],
        entity_table: str,
        relation_table: str,
        allowed_chunk_ids: list[str] | None = None,
        dataset: str | None = None,
    ) -> tuple[list[list[str]], list[list[str]], list[str]]:
        """Batch-query entities, relations, and chunk IDs from PostgreSQL.

        Returns:
            ``(entities, relations, chunk_ids)``
        """
        start = time.time()
        attr_ids = [item["id"] for item in res if item["type"] == "entity"]
        triple_ids = [item["id"] for item in res if item["type"] == "relation"]

        pg_config = get_postgres_conn_config()
        con = psycopg2.connect(**pg_config)
        con.autocommit = False
        cur = con.cursor(cursor_factory=RealDictCursor)
        cur.execute("SET statement_timeout = '30s'")

        try:
            # Entities
            entity_map: dict[str, Any] = {}
            if attr_ids:
                cur.execute(
                    f"SELECT node_id, node_name, description FROM {entity_table} WHERE node_id = ANY(%s)",
                    (attr_ids,),
                )
                for row in cur.fetchall():
                    entity_map[row["node_id"]] = row

            # Relations
            triple_relation_map: dict[str, Any] = {}
            if triple_ids:
                cur.execute(
                    f"SELECT node_id, node1, node2, semantics, description FROM {relation_table} WHERE node_id = ANY(%s)",
                    (triple_ids,),
                )
                for row in cur.fetchall():
                    triple_relation_map[row["node_id"]] = row

            # One-hop relations for matched entities
            relation_map: dict[str, list] = {}
            node_names = [e["node_name"] for e in entity_map.values()]
            if node_names:
                cur.execute(
                    f"SELECT node_id, node1, node2, semantics, description FROM {relation_table}"
                    " WHERE node1 = ANY(%s) OR node2 = ANY(%s)",
                    (node_names, node_names),
                )
                for row in cur.fetchall():
                    relation_map.setdefault(row["node1"], []).append(row)
                    if row["node2"] != row["node1"]:
                        relation_map.setdefault(row["node2"], []).append(row)

            # Collect remaining entity names from relations
            all_node_names = set()
            for rel in triple_relation_map.values():
                all_node_names.add(rel["node1"])
                all_node_names.add(rel["node2"])
            all_node_names -= set(node_names)

            all_entities: dict[str, Any] = {}
            if all_node_names:
                cur.execute(
                    f"SELECT node_id, node_name, description FROM {entity_table} WHERE node_name = ANY(%s)",
                    (list(all_node_names),),
                )
                for row in cur.fetchall():
                    all_entities[row["node_name"]] = row

            # Cluster → chunk mapping
            unique_cluster_ids = list({item["id"] for item in res})
            chunk_ids = self._query_chunk_ids_by_cluster_ids(unique_cluster_ids, dataset, cur)

            # Parse
            parsed_entities, parsed_relations = self._parse_entities_relations(
                res, entity_map, triple_relation_map, relation_map, all_entities,
            )
        finally:
            cur.close()
            con.close()

        elapsed = time.time() - start
        logger.info(
            "Batch query: %d items in %.2fs (entities=%d, relations=%d, clusters=%d, chunks=%d)",
            len(res), elapsed, len(attr_ids), len(triple_ids),
            len(unique_cluster_ids), len(chunk_ids),
        )

        unique_entities, unique_relations = self._deduplicate_entities_relations(
            parsed_entities, parsed_relations,
        )
        return unique_entities, unique_relations, chunk_ids

    # -- chunk source retrieval ----------------------------------------------

    def query_chunks_source(
        self,
        query: str | None = None,
        top_k: int | None = None,
        threshold: float | None = None,
        dataset: str | None = None,
        embedding: list[float] | None = None,
    ) -> tuple[list[str], dict[str, float]]:
        """Retrieve the most similar chunk IDs from Milvus (standalone entry point)."""
        defaults = get_query_defaults()
        top_k = top_k or defaults.get("max_chunks", 100)
        threshold = threshold or defaults["threshold"]

        collection = self.connection_manager.get_chunk_collection(dataset)
        emb_data = self._wrap_embedding(embedding, query, self.connection_manager)

        result = collection.search(
            data=emb_data,
            anns_field="text_embedding",
            param={"metric_type": "IP", "params": {"nprobe": 128}},
            limit=top_k,
            expr="",
            output_fields=["chunk_id"],
        )

        if not result or not result[0]:
            return [], {}

        chunk_ids: list[str] = []
        chunk_scores: dict[str, float] = {}
        for hit in result[0]:
            cid = hit.entity.get("chunk_id")
            if cid:
                chunk_ids.append(cid)
                chunk_scores[cid] = float(hit.distance)
        return chunk_ids, chunk_scores

    # -- chunk content lookup ------------------------------------------------

    def query_chunks_by_ids_batch(
        self, chunk_ids: list[str], dataset: str | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Batch-fetch chunk content from PostgreSQL by chunk IDs."""
        if not chunk_ids:
            logger.warning("Empty chunk_ids, skipping query")
            return {}
        if dataset is None:
            raise ValueError("dataset must not be None")

        chunk_table = f"{dataset}_chunks"
        unique_ids = list(set(chunk_ids))
        logger.info("Querying %d unique chunks from %s", len(unique_ids), chunk_table)

        pg_config = get_postgres_conn_config()
        con = psycopg2.connect(**pg_config)
        con.autocommit = False
        cur = con.cursor(cursor_factory=RealDictCursor)

        try:
            cur.execute("SET statement_timeout = '30s'")
            cur.execute(
                f"SELECT chunk_id, content_text, source_id, created_at FROM {chunk_table} WHERE chunk_id = ANY(%s)",
                (unique_ids,),
            )
            chunk_map: dict[str, dict] = {}
            for row in cur.fetchall():
                chunk_map[row["chunk_id"]] = {
                    "status": "success",
                    "data": {
                        "chunk_id": row["chunk_id"],
                        "content_text": row["content_text"],
                        "source_id": row.get("source_id") or "",
                        "created_at": str(row.get("created_at", "")) if row.get("created_at") else "",
                    },
                }
            for cid in unique_ids:
                if cid not in chunk_map:
                    chunk_map[cid] = {"status": "not_found", "data": {}}

            found = sum(1 for v in chunk_map.values() if v["status"] == "success")
            logger.info("Retrieved %d/%d chunks", found, len(unique_ids))
            return chunk_map

        except Exception:
            logger.error("Batch chunk query failed", exc_info=True)
            return {cid: {"status": "error", "data": {}} for cid in unique_ids}
        finally:
            cur.close()
            con.close()


# ---------------------------------------------------------------------------
# BM25 search (PostgreSQL tsvector)
# ---------------------------------------------------------------------------

def bm25_search_chunks(query: str, dataset: str, top_k: int = 200) -> list[str]:
    """Return chunk IDs ranked by BM25 (ts_rank) for *query*.

    Tokenises the query with :func:`hetadb.utils.utils.tokenize_for_tsvector`
    so mixed Chinese/English queries are handled correctly.  Returns an empty
    list on any error rather than raising, so the caller can degrade gracefully.

    Uses the pre-computed ``content_tsv`` tsvector column (GIN-indexed) rather
    than computing ``to_tsvector`` at query time, so the index is actually hit.
    """
    from hetadb.utils.utils import tokenize_for_tsvector

    tokens = tokenize_for_tsvector(query)
    if not tokens:
        return []

    chunk_table = f"{dataset}_chunks"
    pg_config = get_postgres_conn_config()
    con = psycopg2.connect(**pg_config)
    try:
        cur = con.cursor()
        cur.execute("SET statement_timeout = '10s'")
        cur.execute(
            f"""
            SELECT chunk_id
            FROM {chunk_table}
            WHERE content_tsv @@ plainto_tsquery('simple', %s)
            ORDER BY ts_rank(content_tsv, plainto_tsquery('simple', %s)) DESC
            LIMIT %s
            """,
            (tokens, tokens, top_k),
        )
        return [row[0] for row in cur.fetchall()]
    except Exception:
        logger.warning("BM25 search failed for dataset=%s", dataset, exc_info=True)
        return []
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Cross-encoder rerankers
# ---------------------------------------------------------------------------

class RemoteReranker:
    """Cross-encoder reranker that calls the Qwen3-Reranker HTTP service.

    Expected endpoint: ``POST /rerank``
    Request body:  ``{"pairs": [["query", "doc"], ...]}``
    Response body: ``{"scores": [float, ...]}``
    """

    def __init__(self, url: str, timeout: int = 60) -> None:
        # Ensure the URL points to the /rerank endpoint
        self.url = url.rstrip("/") + "/rerank"
        self.timeout = timeout

    def score(self, query: str, passages: list[str]) -> list[float]:
        import requests
        pairs = [[query, p] for p in passages]
        resp = requests.post(self.url, json={"pairs": pairs}, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()["scores"]


def _build_reranker() -> RemoteReranker | None:
    """Instantiate a reranker from config, or return None if not configured.

    Reads ``project.query_defaults.reranker_url`` from config.yaml.
    """
    url = get_query_defaults().get("reranker_url", "")
    if url:
        return RemoteReranker(url=url)
    return None


# Module-level singleton — built once on first use, avoids per-request config reads.
_reranker: RemoteReranker | None | bool = False  # False = not yet initialised


def get_reranker() -> RemoteReranker | None:
    """Return the shared reranker instance (lazy-initialised singleton)."""
    global _reranker
    if _reranker is False:
        _reranker = _build_reranker()
    return _reranker  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# SQL query helper (used by chat_processor for table-type nodes)
# ---------------------------------------------------------------------------

def execute_sql_query(
    query: str, table_info_dir: str, table_name: str,
) -> dict[str, Any]:
    """Execute a text-to-SQL query against a single table."""
    from hetadb.core.retrieval.ans_by_sql import DescriptiveText2SQLEngine

    llm_config = get_chat_cfg()
    postgres_config = get_postgres_conn_config()
    use_llm = create_use_llm(
        url=llm_config["base_url"],
        api_key=llm_config["api_key"],
        model=llm_config["model"],
        timeout=llm_config["timeout"],
    )
    engine = DescriptiveText2SQLEngine(
        table_info_dir=table_info_dir,
        postgres_config=postgres_config,
        use_llm=use_llm,
    )
    answer = engine.query(question=query, table_name=table_name)
    return {"table_name": table_name, "answer": answer}


# ---------------------------------------------------------------------------
# Singleton query instance
# ---------------------------------------------------------------------------

optimized_kb_query = OptimizedKbQuery()


# ---------------------------------------------------------------------------
# Top-K utility
# ---------------------------------------------------------------------------

def get_top_k_items(data: dict[str, float], k: int) -> dict[str, float]:
    """Return the *k* highest-scoring entries from *data*."""
    top_keys = [
        key
        for key, _ in sorted(data.items(), key=lambda x: x[1], reverse=True)[:k]
    ]
    return {key: data[key] for key in top_keys}


# ---------------------------------------------------------------------------
# Answer generation (using common LLM client)
# ---------------------------------------------------------------------------

def _build_llm_caller() -> tuple[Any, Any]:
    """Create sync and async LLM callers from the shared chat config."""
    cfg = get_chat_cfg()
    sync_llm = create_use_llm(
        url=cfg["base_url"],
        api_key=cfg["api_key"],
        model=cfg["model"],
        timeout=cfg["timeout"],
    )
    async_llm = create_use_llm_async(
        url=cfg["base_url"],
        api_key=cfg["api_key"],
        model=cfg["model"],
        timeout=cfg["timeout"],
        max_concurrent_requests=cfg.get("max_concurrent_requests", 5),
    )
    return sync_llm, async_llm


async def generate_answer_from_content(
    query: str,
    content_texts: list[str],
    request_id: str | None = None,
    source_labels: list[int | None] | None = None,
) -> str:
    """Generate an answer grounded in retrieved *content_texts*.

    If *source_labels* is provided (parallel list of citation indices or None),
    each chunk is prefixed with ``[N]`` and the LLM is instructed to use inline
    citations so the response text contains markers like ``[1]``, ``[2]``.
    """
    if not content_texts:
        logger.warning("[%s] No content_texts provided, cannot generate answer", request_id)
        return ""

    try:
        use_citations = source_labels and any(l is not None for l in source_labels)
        if use_citations:
            parts = []
            for text, label in zip(content_texts, source_labels):  # type: ignore[arg-type]
                prefix = f"[{label}] " if label is not None else ""
                parts.append(f"{prefix}{text}")
            combined_content = "\n\n".join(parts)
            citation_instruction = "\n\n请在回答中用 [数字] 标注引用来源, 例如: 根据[1]..."
        else:
            combined_content = "\n\n".join(content_texts)
            citation_instruction = ""

        prompt = f"""基于以下相关内容，回答用户的问题。

相关内容：
{combined_content}

用户问题：{query}

请基于上述相关内容，回答用户的问题。如果相关内容不足以回答问题，请说明无法完全回答。{citation_instruction}"""

        _, async_llm = _build_llm_caller()
        t0 = time.time()
        logger.info("[%s] Generating answer from %d content texts", request_id, len(content_texts))
        answer = await async_llm(prompt)
        logger.info("[%s] Answer generated in %.3fs", request_id, time.time() - t0)
        return answer

    except Exception:
        logger.error("[%s] Answer generation failed", request_id, exc_info=True)
        return ""


async def generate_answer(
    query: str, request_id: str | None = None,
) -> str:
    """Generate a direct LLM answer without retrieval context."""
    try:
        prompt = f"""
        你是一个知识库助手，专门回答用户关于特定主题的问题。请给出准确的回答：
        用户问题：{query}
        """

        _, async_llm = _build_llm_caller()
        t0 = time.time()
        logger.info("[%s] Generating direct answer", request_id)
        answer = await async_llm(prompt)
        logger.info("[%s] Answer generated in %.3fs", request_id, time.time() - t0)
        return answer

    except Exception:
        logger.error("[%s] Direct answer generation failed", request_id, exc_info=True)
        return ""
