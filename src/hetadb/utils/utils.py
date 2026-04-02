"""Shared utility functions for HetaDB.

Provides LLM response parsing, text normalization, JSONL I/O,
embedding record iteration, and vector-based clustering.
"""

import itertools
import json
import logging
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

import re

import jieba
import zhconv

import numpy as np
from sklearn.cluster import AgglomerativeClustering  # type: ignore
from sklearn.preprocessing import normalize  # type: ignore

logger = logging.getLogger(__name__)

# Suppress jieba's default INFO logging and pre-load its dictionary
logging.getLogger("jieba").setLevel(logging.WARNING)
jieba.initialize()

# Matches one or more consecutive CJK Unified Ideographs (covers BMP + Extension A/B/C)
_CJK_RE = re.compile(r"[\u4e00-\u9fff\u3400-\u4dbf\U00020000-\U0002a6df]+")


def tokenize_for_tsvector(text: str) -> str:
    """Tokenize mixed Chinese/English text for PostgreSQL tsvector.

    Strategy:
    - Traditional Chinese is first converted to Simplified (zhconv).
    - CJK character runs are word-segmented with jieba.
    - Non-CJK runs (English, numbers, punctuation) are split on whitespace only,
      so hyphenated tokens like ``GPT-4`` or ``BERT-base`` are preserved intact.

    The returned string is suitable as input to ``to_tsvector('simple', %s)``.
    """
    simplified = zhconv.convert(text or "", "zh-hans")
    tokens: list[str] = []
    last = 0
    for m in _CJK_RE.finditer(simplified):
        # Non-CJK span before this match: split on whitespace
        non_cjk = simplified[last:m.start()]
        tokens.extend(non_cjk.split())
        # CJK span: jieba word segmentation
        tokens.extend(jieba.cut(m.group()))
        last = m.end()
    # Remaining non-CJK tail
    tokens.extend(simplified[last:].split())
    return " ".join(t for t in tokens if t.strip())


def _parse_llm_response(resp: Any, caller_logger: logging.Logger) -> Any:
    """Parse an LLM response string into a dict or list.

    Handles markdown code-block wrappers and returns ``{}`` on failure.
    *caller_logger* is used so messages appear under the calling module's logger.
    """
    if isinstance(resp, (dict, list)):
        return resp

    if not isinstance(resp, str):
        caller_logger.warning("LLM returned non-string type: %s", type(resp))
        return {}

    resp_clean = resp.strip()

    # Strip markdown code fences if present
    if resp_clean.startswith("```"):
        first_newline = resp_clean.find("\n")
        if first_newline != -1:
            resp_clean = resp_clean[first_newline + 1:]
        if resp_clean.endswith("```"):
            resp_clean = resp_clean[:-3]
        resp_clean = resp_clean.strip()

    try:
        return json.loads(resp_clean)
    except json.JSONDecodeError as e:
        caller_logger.error(
            "Failed to parse LLM JSON response: %s\nContent: %s", e, resp_clean[:500],
        )
        return {}
    except RecursionError:
        caller_logger.error(
            "JSON decoding hit recursion limit (response may be excessively nested). "
            "Content prefix: %s", resp_clean[:500],
        )
        return {}


def normalize_name(name: Any) -> str:
    """Strip whitespace from *name*, returning empty string for None."""
    return str(name).strip() if name is not None else ""


def clean_str(value: Any, max_len: int | None = None) -> str:
    """Convert value to str, strip NUL characters, and optionally trim length."""
    text = "" if value is None else str(value)
    if "\x00" in text:
        text = text.replace("\x00", "")
    if max_len is not None:
        text = text[:max_len]
    return text


def iter_embedding_records(embedding_dir: Path) -> Iterable[dict[str, Any]]:
    """Stream embedding records from all JSONL files in *embedding_dir*."""
    files = sorted(embedding_dir.glob("*.jsonl"))

    def _normalize_record(rec: dict[str, Any]) -> dict[str, Any]:
        """Unify field names: id/Id -> Id, ChunkId/chunkid -> chunk_id."""
        rec = rec.copy()
        if "Id" not in rec and "id" in rec:
            rec["Id"] = rec.pop("id")
        if "chunk_id" not in rec:
            if "ChunkId" in rec:
                rec["chunk_id"] = rec.pop("ChunkId")
            elif "chunkid" in rec:
                rec["chunk_id"] = rec.pop("chunkid")
        return rec

    for fp in files:
        with fp.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    yield _normalize_record(json.loads(line))
                except json.JSONDecodeError:
                    continue


def take_n(iterator: Iterable[Any], n: int) -> list[Any]:
    """Take at most *n* items from *iterator*."""
    return list(itertools.islice(iterator, n))


def write_jsonl(records: Iterable[dict[str, Any]], output_path: Path) -> None:
    """Write records to a JSONL file, creating parent directories as needed."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def cluster_by_embedding(
    records: Sequence[dict[str, Any]], similarity_threshold: float,
) -> list[list[dict[str, Any]]]:
    """Group records by cosine similarity using agglomerative clustering.

    Args:
        records: Records with an optional ``"embedding"`` field.
        similarity_threshold: Cosine similarity threshold (e.g. 0.85 -> distance 0.15).

    Returns:
        A list of clusters, each a list of records.
        Records without embeddings are placed in singleton clusters.
    """
    if not records:
        return []

    emb_records: list[dict[str, Any]] = []
    emb_list: list[np.ndarray] = []
    no_emb_clusters: list[list[dict[str, Any]]] = []

    for rec in records:
        emb = rec.get("embedding")
        if emb is None:
            no_emb_clusters.append([rec])
        else:
            emb_records.append(rec)
            emb_list.append(np.asarray(emb, dtype=np.float32))

    clusters: list[list[dict[str, Any]]] = []

    if emb_records:
        if len(emb_records) == 1:
            # AgglomerativeClustering requires at least 2 samples
            clusters.append(emb_records)
        else:
            norm_embs = normalize(np.stack(emb_list, axis=0))
            distance_threshold = max(0.0, 1.0 - similarity_threshold)
            hac = AgglomerativeClustering(
                n_clusters=None,
                distance_threshold=distance_threshold,
                metric="euclidean",
                linkage="average",
            )
            labels = hac.fit_predict(norm_embs)

            label_to_recs: dict[int, list[dict[str, Any]]] = {}
            for rec, lbl in zip(emb_records, labels):
                label_to_recs.setdefault(lbl, []).append(rec)

            clusters.extend(label_to_recs.values())

    clusters.extend(no_emb_clusters)
    return clusters
