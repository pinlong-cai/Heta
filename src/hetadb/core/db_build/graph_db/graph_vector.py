"""Knowledge graph vectorization utilities.

Provides embedding generation, multi-threaded batch processing,
and file management for writing embedding results to JSONL.
"""

import json
import logging
import queue
import threading
import time
from pathlib import Path
from typing import Any

import numpy as np
from openai import OpenAI
from tqdm import tqdm

logger = logging.getLogger(__name__)


def _embed_chunk(
    client: OpenAI,
    chunk: list[str],
    embedding_model: str,
    max_retries: int,
    retry_delay: int,
    embedding_dim: int = 1024,
) -> list:
    """Call the embedding API for a single chunk with exponential-backoff retry.

    Returns a list of embedding vectors, or zero vectors on permanent failure.
    The fallback zero vectors use *embedding_dim* to match the configured model
    dimension, avoiding shape mismatches when results are stacked later.
    """
    current_delay = retry_delay
    for retry in range(max_retries + 1):
        try:
            result = client.embeddings.create(input=chunk, model=embedding_model)
            return [d.embedding for d in result.data]
        except Exception as e:
            if retry < max_retries:
                logger.warning(
                    "Embedding API error: %s. Retrying in %ds...", e, current_delay
                )
                time.sleep(current_delay)
                current_delay *= 2
            else:
                logger.error(
                    "Failed embedding after %d attempts: %s", max_retries + 1, e
                )
                return np.zeros((len(chunk), embedding_dim)).tolist()


def embedding(
    texts: list[str],
    api_key: str,
    embedding_url: str,
    embedding_model: str,
    embedding_timeout: int,
    max_retries: int = 5,
    retry_delay: int = 2,
    api_batch_size: int = 64,
    embedding_dim: int = 1024,
):
    """Generate embeddings for a list of texts via OpenAI-compatible API.

    Splits *texts* into chunks of at most *api_batch_size* before each API
    call to stay within per-request limits.  Results are concatenated and
    returned as a single list aligned with the input order.

    *embedding_dim* is used only as the fallback dimension when the API call
    fails permanently, ensuring consistent vector shapes across all records.

    Returns a list of embedding vectors on success, or a zero matrix on failure.
    """
    client = OpenAI(
        api_key=api_key, base_url=embedding_url, timeout=float(embedding_timeout)
    )
    results = []
    for start in range(0, len(texts), api_batch_size):
        chunk = texts[start : start + api_batch_size]
        results.extend(_embed_chunk(client, chunk, embedding_model, max_retries, retry_delay, embedding_dim))
    return results


def check_embedding_connection(
    api_key: str,
    embedding_url: str,
    embedding_model: str,
    embedding_timeout: int,
) -> bool:
    """Verify that the embedding API is reachable."""
    try:
        embedding(
            ["test"], api_key, embedding_url, embedding_model, embedding_timeout,
            max_retries=1,
        )
        return True
    except Exception:
        return False


class FileManager:
    """Thread-safe JSONL writer that splits output across files by size limit."""

    def __init__(
        self,
        base_path: str,
        file_type: str,
        max_size_bytes: int = 3 * 1024 * 1024 * 1024,
        start_index: int = 0,
    ):
        self.base_path = base_path
        self.file_type = file_type
        self.max_size_bytes = max_size_bytes
        self.current_file_index = start_index
        self.current_file_size = 0
        self.current_file = None
        self.lock = threading.Lock()
        self.records_written = 0
        self._open_next_file()

    def _get_file_path(self) -> Path:
        return (
            Path(self.base_path)
            / f"{self.file_type}_embeddings_{self.current_file_index}.jsonl"
        )

    def _open_next_file(self):
        if self.current_file:
            self.current_file.close()
            logger.info(
                "Closed file after %d records (%.2f MB)",
                self.records_written, self.current_file_size / 1024 / 1024,
            )
        file_path = self._get_file_path()
        self.current_file = open(file_path, "w", encoding="utf-8")
        self.current_file_size = 0
        logger.info("Started writing to %s", file_path)

    def write_record(self, record: dict):
        """Serialize *record* as JSON and append to the current output file."""
        with self.lock:
            json_str = json.dumps(record, ensure_ascii=False) + "\n"
            str_size = len(json_str.encode("utf-8"))
            if self.current_file_size + str_size > self.max_size_bytes:
                self.current_file_index += 1
                self._open_next_file()
            self.current_file.write(json_str)
            self.current_file.flush()
            self.current_file_size += str_size
            self.records_written += 1
            if self.records_written % 1000 == 0:
                logger.info(
                    "Written %d records (%.2f MB)",
                    self.records_written, self.current_file_size / 1024 / 1024,
                )

    def close(self):
        """Flush and close the current output file."""
        if self.current_file:
            self.current_file.close()
            logger.info("Final file closed after %d records", self.records_written)


class Worker(threading.Thread):
    """Background thread that pulls batches from a queue, generates embeddings,
    and writes results via a FileManager."""

    def __init__(
        self,
        task_queue: queue.Queue,
        file_manager: FileManager,
        worker_id: int,
        api_key: str,
        embedding_url: str,
        embedding_model: str,
        embedding_timeout: int,
        max_retries: int = 5,
        retry_delay: int = 2,
        embedding_dim: int = 1024,
    ):
        super().__init__(daemon=True)
        self.task_queue = task_queue
        self.file_manager = file_manager
        self.worker_id = worker_id
        self.api_key = api_key
        self.embedding_url = embedding_url
        self.embedding_model = embedding_model
        self.embedding_timeout = embedding_timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.embedding_dim = embedding_dim

    def run(self):
        while True:
            try:
                batch = self.task_queue.get(timeout=5)
                if batch is None:
                    self.task_queue.task_done()
                    break
                batch_texts, batch_records = batch
                embeddings = embedding(
                    batch_texts,
                    self.api_key,
                    self.embedding_url,
                    self.embedding_model,
                    self.embedding_timeout,
                    self.max_retries,
                    self.retry_delay,
                    embedding_dim=self.embedding_dim,
                )
                for i, record in enumerate(batch_records):
                    record["embedding"] = embeddings[i]
                    self.file_manager.write_record(record)
                self.task_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error("Worker %d error: %s", self.worker_id, e)
                self.task_queue.task_done()


def process_file(
    input_file: str | Path,
    file_type: str,
    file_manager: FileManager,
    batch_size: int,
    num_threads: int,
    api_key: str,
    embedding_url: str,
    embedding_model: str,
    embedding_timeout: int,
    max_retries: int = 5,
    retry_delay: int = 2,
    embedding_dim: int = 1024,
) -> int:
    """Generate embeddings for records in JSONL file(s) using a thread pool.

    Args:
        input_file: Path to a single JSONL file or a directory of JSONL files.
        file_type: Label for the data type (e.g. "attr", "triple").
        file_manager: FileManager instance for writing output.
        batch_size: Number of texts per embedding API call.
        num_threads: Number of worker threads.
        api_key: Embedding API key.
        embedding_url: Embedding API base URL.
        embedding_model: Embedding model name.
        embedding_timeout: Timeout per API call in seconds.
        max_retries: Max retry attempts on API failure.
        retry_delay: Initial retry delay in seconds (doubles on each retry).
        embedding_dim: Dimension of the embedding model output. Used as the
            fallback vector size when the API call fails permanently, ensuring
            all records have consistent embedding shapes.

    Returns:
        Number of records processed.
    """
    input_path = Path(input_file)

    if input_path.is_dir():
        jsonl_files = sorted(input_path.glob("*.jsonl"))
        if not jsonl_files:
            logger.warning("No .jsonl files found in %s", input_file)
            return 0
        logger.info("Found %d .jsonl files in %s", len(jsonl_files), input_file)
    elif input_path.is_file():
        jsonl_files = [input_path]
    else:
        logger.error("Input path does not exist: %s", input_file)
        return 0

    logger.info("Processing %d file(s) for %s", len(jsonl_files), file_type)
    task_queue: queue.Queue[tuple[list[str], list[dict[str, Any]]] | None] = (
        queue.Queue(maxsize=100)
    )
    workers = [
        Worker(
            task_queue, file_manager, i,
            api_key, embedding_url, embedding_model, embedding_timeout,
            max_retries, retry_delay, embedding_dim,
        )
        for i in range(num_threads)
    ]
    for w in workers:
        w.start()

    processed_count = 0
    batch_texts: list[str] = []
    batch_records: list[dict] = []

    for jsonl_file in jsonl_files:
        logger.info("Processing file: %s", jsonl_file)
        with open(jsonl_file, encoding="utf-8") as f:
            for line in tqdm(f, desc=f"Queueing {file_type} from {jsonl_file.name}"):
                try:
                    rec = json.loads(line.strip())
                    desc = (
                        rec.get("Description")
                        or rec.get("description")
                        or rec.get("text")
                    )
                    if isinstance(desc, list):
                        desc = ",".join(desc)
                    if not desc:
                        desc = rec.get("NodeName") or rec.get("nodename")
                    if not desc:
                        logger.warning(
                            "Skipping record with no embeddable text: %s",
                            rec.get("Id", "?"),
                        )
                        continue
                    batch_texts.append(str(desc))
                    batch_records.append(rec)
                    if len(batch_texts) >= batch_size:
                        task_queue.put((batch_texts.copy(), batch_records.copy()))
                        processed_count += len(batch_texts)
                        batch_texts, batch_records = [], []
                except Exception:
                    continue

    # Flush remaining batch
    if batch_texts:
        task_queue.put((batch_texts, batch_records))
        processed_count += len(batch_texts)

    # Signal workers to stop
    for _ in workers:
        task_queue.put(None)
    task_queue.join()
    for w in workers:
        w.join()

    logger.info(
        "Processed %d %s records from %d file(s)",
        processed_count, file_type, len(jsonl_files),
    )
    return processed_count
