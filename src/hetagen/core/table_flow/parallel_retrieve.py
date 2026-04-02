"""Parallel vector retrieval and LLM answer generation.

Retrieves relevant chunks from a Milvus collection for each query in a batch,
then generates precise answers using an LLM that can be directly inserted into
table cells.
"""

import json
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import yaml
from openai import OpenAI
from sentence_transformers import SentenceTransformer
from pymilvus import connections, Collection

from common.config import get_persistence
from hetagen.core.table_flow.config import Config
from hetagen.core.table_flow.table_schema import TableDefiner
from hetagen.core.table_flow.generate_queries import QueryGenerator
from hetagen.core.table_flow.utils import convert_number
from hetagen.utils.path import PROJECT_ROOT

logger = logging.getLogger(__name__)

with open(PROJECT_ROOT / "config.yaml", encoding="utf-8") as _f:
    _hetagen_cfg = yaml.safe_load(_f)["hetagen"]
    _milvus_cfg = _hetagen_cfg["milvus"]
    _emb_cfg = _hetagen_cfg["embedding_api"]

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


# Answer generation system prompt (intentional Chinese content for LLM)
ANSWER_SYSTEM_PROMPT = """你是一个精确答案提取助手。根据给定的查询问题和检索到的相关信息，提取出精确的答案。

# 要求
- 答案必须简洁精确，能直接填入表格单元格；
- **保留数值与量级**：必须保留数字及其数量级后缀（如：万、亿、万亿）；
- **去除物理/货币单位**：**绝对不要**输出货币单位（如：美元、人民币）或物理单位（如：吨、米、个）；
- 如果问的是日期，返回标准格式（如"2024-01-01"或"2024 Q1"）；
- 如果问的是名称或文本，直接返回对应内容；
- 如果信息不足以回答，返回"未找到"；
- 不要添加任何解释性文字，只输出答案本身；

# 输出格式
直接输出答案，不要包含任何多余内容。

# 示例
- 输入："2023年营收为954.03亿美元"
  -> 输出："954.03亿"
- 输入："净利润达到10.5亿元"
  -> 输出："10.5亿"
- 输入："市值约2.3万亿人民币"
  -> 输出："2.3万亿"
- 输入："销量为500万台"
  -> 输出："500万"
- 输入："增长率为15%"
  -> 输出："15%" (百分比通常保留)
"""


class ParallelRetriever:
    """Parallel vector retriever with LLM-based answer generation."""

    def __init__(
        self,
        config: Config | None = None,
        top_k: int = 5,
        threshold: float = 0.5,
        max_workers: int = 10
    ):
        """Initialize the parallel retriever.

        Args:
            config: Configuration object.
            top_k: Maximum number of retrieval results per query.
            threshold: Similarity score threshold.
            max_workers: Maximum number of worker threads for parallel retrieval.
        """
        self.config = config or Config()
        self.top_k = top_k
        self.threshold = threshold
        self.max_workers = max_workers

        # Initialize LLM client
        self.llm_client = OpenAI(
            api_key=self.config.llm_api_key,
            base_url=self.config.llm_base_url,
        )

        # Lazily initialized components
        self._embedding_model: SentenceTransformer | None = None
        self._collection: Collection | None = None

    def _load_embedding_model(self) -> SentenceTransformer:
        """Load the embedding model (singleton)."""
        if self._embedding_model is None:
            if USE_REMOTE_EMBEDDING:
                # No local model needed when using the remote API
                logger.debug("Using remote embedding API, skipping local model load")
                # Return a dummy object to prevent accidental local calls
                class DummyModel:
                    def encode(self, text, normalize_embeddings=True):
                        raise RuntimeError("Local model should not be called when using remote embedding API")
                self._embedding_model = DummyModel()
            else:
                logger.info("Loading local embedding model: %s", MODEL_PATH)
                self._embedding_model = SentenceTransformer(MODEL_PATH, device='cpu')
        return self._embedding_model

    def _connect_milvus(self) -> None:
        """Connect to Milvus."""
        connections.connect(
            alias="autotable",
            host=MILVUS_HOST,
            port=MILVUS_PORT,
            db_name=MILVUS_DB,
        )

    def _disconnect_milvus(self) -> None:
        """Disconnect from Milvus."""
        connections.disconnect("autotable")

    def _get_collection(self) -> Collection:
        """Get the Milvus collection (singleton)."""
        if self._collection is None:
            logger.info("Connecting to Milvus at %s:%s", MILVUS_HOST, MILVUS_PORT)
            self._connect_milvus()
            self._collection = Collection(name=COLLECTION_NAME, using="autotable")
            self._collection.load()
        return self._collection

    def _get_query_embedding(self, query: str) -> list:
        """Vectorize a query string."""
        if USE_REMOTE_EMBEDDING:
            return self._get_remote_embedding(query)
        else:
            model = self._load_embedding_model()
            embedding = model.encode(query, normalize_embeddings=True)
            return embedding.tolist()

    def _get_remote_embedding(self, text: str) -> list:
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

        # L2-normalize to unit vector (in case the API does not normalize)
        import numpy as np
        embedding = np.array(embedding)
        embedding = embedding / np.linalg.norm(embedding)

        return embedding.tolist()

    def _search_similar_chunks(self, query_embedding: list) -> list[dict]:
        """Search for similar text chunks in Milvus."""
        collection = self._get_collection()

        search_params = {
            "metric_type": "IP",
            "params": {"nprobe": 16}
        }

        results = collection.search(
            data=[query_embedding],
            anns_field="embedding",
            param=search_params,
            limit=self.top_k,
            output_fields=["id", "content"]
        )

        retrieved_chunks = []
        for hits in results:
            for hit in hits:
                score = hit.score
                if score >= self.threshold:
                    retrieved_chunks.append({
                        "id": hit.entity.get("id"),
                        "content": hit.entity.get("content"),
                        "score": round(score, 4)
                    })

        return retrieved_chunks

    def retrieve_single(self, query: str) -> list[dict]:
        """Retrieve relevant chunks for a single query.

        Args:
            query: Query text.

        Returns:
            List of matching text chunks with scores.
        """
        query_embedding = self._get_query_embedding(query)
        return self._search_similar_chunks(query_embedding)

    def generate_answer(self, query: str, retrieved_chunks: list[dict]) -> str:
        """Generate a precise answer from retrieved chunks using the LLM.

        Args:
            query: Query question.
            retrieved_chunks: Retrieved text chunks.

        Returns:
            Precise answer suitable for direct insertion into a table cell.
        """
        if not retrieved_chunks:
            return "未找到"

        # Build context
        context_parts = []
        for i, chunk in enumerate(retrieved_chunks, 1):
            context_parts.append(f"[{i}] {chunk.get('content', '')}")
        context = "\n".join(context_parts)

        user_prompt = f"""查询问题：{query}

检索到的相关信息：
{context}

请根据以上信息，提取出精确的答案。"""

        try:
            response = self.llm_client.chat.completions.create(
                model=self.config.llm_model,
                temperature=0.0,
                messages=[
                    {"role": "system", "content": ANSWER_SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
            )
            answer = response.choices[0].message.content.strip()
            return answer
        except Exception as e:
            logger.error("LLM call failed: %s", e)
            return "生成失败"

    def process_single_task(self, task: dict[str, Any]) -> dict[str, Any]:
        """Process a single task: retrieve, generate answer, and convert numerics.

        Args:
            task: Task dict containing a ``query`` field.

        Returns:
            Task dict augmented with ``answer`` and ``retrieved_chunks``.
        """
        query = task.get("query", "")
        task_id = task.get("task_id", "unknown")

        logger.debug("[Task %s] Retrieving: %s", task_id, query[:50])

        # Retrieve relevant chunks
        retrieved_chunks = self.retrieve_single(query)

        logger.debug("[Task %s] Found %d chunks, generating answer...", task_id, len(retrieved_chunks))

        # Generate answer
        answer = self.generate_answer(query, retrieved_chunks)

        # Convert numeric values (e.g. "123万" -> numeric), keep non-numeric as-is
        converted_answer = convert_number(answer)

        # Map "未找到" to empty string for CSV/SQL output convenience
        if converted_answer == "未找到":
            converted_answer = ""

        logger.debug("[Task %s] Answer: %s -> %s", task_id, answer, converted_answer)

        # Build result (preserve original fields, add new ones)
        result = task.copy()
        result["answer"] = converted_answer
        result["retrieved_chunks"] = retrieved_chunks

        return result

    def process_batch(self, tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Process a batch of tasks in parallel.

        Args:
            tasks: List of task dicts, each containing a ``query`` field.

        Returns:
            List of task dicts augmented with ``answer``.
        """
        if not tasks:
            return []

        logger.info("Processing %d tasks in parallel...", len(tasks))

        # Pre-load model and connection to avoid redundant init in threads
        if not USE_REMOTE_EMBEDDING:
            self._load_embedding_model()
        self._get_collection()

        results = [None] * len(tasks)

        # Use a thread pool for parallel processing
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_idx = {
                executor.submit(self.process_single_task, task): idx
                for idx, task in enumerate(tasks)
            }

            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    results[idx] = future.result()
                except Exception as e:
                    logger.error("Task %d failed: %s", idx, e)
                    # Preserve original task, attach error info
                    results[idx] = tasks[idx].copy()
                    results[idx]["answer"] = "处理失败"
                    results[idx]["error"] = str(e)

        logger.info("All tasks completed")
        return results

    def close(self):
        """Release resources."""
        if self._collection is not None:
            self._disconnect_milvus()
            self._collection = None
