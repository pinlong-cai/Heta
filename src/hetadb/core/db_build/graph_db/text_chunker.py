"""Text chunking: split parsed documents into overlapping token-based chunks."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from pathlib import Path
from typing import Any

import tiktoken

logger = logging.getLogger("hetadb.text_chunker")


def split_text_with_overlap_with_pos(
    text: str,
    chunk_size: int = 1024,
    overlap: int = 50,
    encoding_name: str = "cl100k_base",
) -> list[dict]:
    if not text:
        return []

    encoding = tiktoken.get_encoding(encoding_name)
    tokens = encoding.encode(text)
    total = len(tokens)

    results = []
    start = 0
    PUNCTS = ["。", ".", ",", "，", "!", "?", "！", "？", "\n"]

    while start < total:
        end = min(start + chunk_size, total)
        window_tokens = tokens[start:end]
        window_text = encoding.decode(window_tokens)

        split_pos = max(window_text.rfind(p) for p in PUNCTS)
        if split_pos > 0:
            final_text = window_text[: split_pos + 1].strip()
            final_tokens = encoding.encode(final_text)
            end = start + len(final_tokens)
        else:
            final_text = window_text.strip()

        if final_text:
            results.append({
                "text": final_text,
                "token_start": start,
                "token_end": end,
            })

        if end >= total:
            break

        next_start = end - overlap
        if next_start <= start:
            break

        start = next_start

    return results


def process_json_data_to_texts(
    data: dict[str, Any], chunk_size: int = 1024, overlap: int = 50,
) -> tuple[list[str], list[int], dict[str, Any]]:
    """Extract merged page text per record and split into overlapping chunks."""
    p_cnt = 0
    multipage_texts: list[str] = []
    text_nums_per_page_list: list[int] = []
    while "json_content" in data and f"page_{p_cnt}" in data["json_content"]:
        try:
            merge_text_dic = data["json_content"][f"page_{p_cnt}"][-1]
            element_type = merge_text_dic.get("type", "")
            if element_type == "image":
                caption = merge_text_dic.get("caption", "")
                desc = merge_text_dic.get("desc", "")
                merge_text = f"{caption}: {desc}" if caption else desc
            else:
                merge_text = merge_text_dic.get("text", "")
            merge_text = merge_text.replace("\n", ",")
        except Exception:
            merge_text = ""
        texts = split_text_with_overlap_with_pos(merge_text, chunk_size, overlap)
        full_texts = [item["text"] for item in texts]
        multipage_texts.extend(full_texts)
        text_nums_per_page_list.append(len(full_texts))
        p_cnt += 1
    return multipage_texts, text_nums_per_page_list, data.get("meta", {})


def create_batches_by_bytes(
    lines: Iterable[str], max_batch_bytes: int = 10 * 1024 * 1024,
) -> list[list[str]]:
    """Group JSONL lines into batches limited by UTF-8 byte size."""
    batches: list[list[str]] = []
    current_batch: list[str] = []
    current_size = 0
    for line in lines:
        line_bytes = len(line.encode("utf-8"))
        if current_batch and current_size + line_bytes > max_batch_bytes:
            batches.append(current_batch)
            current_batch = []
            current_size = 0
        current_batch.append(line)
        current_size += line_bytes
    if current_batch:
        batches.append(current_batch)
    return batches


def generate_chunk_id(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def process_batch(
    json_lines_batch: list[str], chunk_size: int = 1024, overlap: int = 50,
) -> tuple[list[str], int]:
    """Parse a batch of JSONL lines, split into chunks, return serialized results."""
    results: list[str] = []
    total_chunks = 0

    for json_line in json_lines_batch:
        try:
            data = json.loads(json_line)
        except Exception:
            data = {}

        multipage_texts, text_nums_per_page_list, meta = process_json_data_to_texts(
            data, chunk_size, overlap,
        )
        meta = meta.copy() if meta else {}
        meta["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%S")

        text_idx = 0
        for page_idx, chunk_count in enumerate(text_nums_per_page_list):
            for _ in range(chunk_count):
                if text_idx < len(multipage_texts):
                    text = multipage_texts[text_idx]
                    if text:
                        chunk_id = generate_chunk_id(text)
                        chunk_obj = {
                            "chunk_id": chunk_id,
                            "text": text,
                            "meta": {**meta, "page": page_idx},
                        }
                        results.append(json.dumps(chunk_obj, ensure_ascii=False))
                        total_chunks += 1
                    text_idx += 1

    return results, total_chunks


def write_batches_to_file(
    file_batches: list[list[str]],
    output_path: Path,
    max_workers: int,
    total_lines: int,
    chunk_size: int = 1024,
    overlap: int = 50,
) -> tuple[int, float]:
    """Process all batches for a file and persist to disk."""
    start_time = time.time()
    total_emb_count = 0
    output_stream = BytesIO()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_batch, batch, chunk_size, overlap): len(batch)
            for batch in file_batches
        }
        processed_line_count = 0
        for future in as_completed(futures):
            batch_results, emb_cnt = future.result()
            total_emb_count += emb_cnt
            for result in batch_results:
                output_stream.write(result.encode("utf-8"))
                output_stream.write(b"\n")
            processed_line_count += futures[future]
            elapsed = time.time() - start_time
            logger.info(
                "Progress %s/%s lines, chunks %s, elapsed %.1fs",
                processed_line_count, total_lines, total_emb_count, elapsed,
            )

    output_path.write_bytes(output_stream.getvalue())
    elapsed = time.time() - start_time
    return total_emb_count, elapsed


def chunk_directory(
    input_dir: Path,
    output_dir: Path,
    max_batch_bytes: int,
    max_workers: int,
    chunk_size: int = 1024,
    overlap: int = 50,
) -> None:
    """Iterate over JSONL files and generate chunked JSONL outputs."""
    output_dir.mkdir(parents=True, exist_ok=True)
    jsonl_files = sorted(input_dir.rglob("*.jsonl"))
    if not jsonl_files:
        logger.warning("No JSONL files found under %s", input_dir)
        return

    logger.info("Found %s files", len(jsonl_files))

    for idx, input_file in enumerate(jsonl_files, start=1):
        output_file = output_dir / input_file.name.replace("doc", "chunk")
        if output_file.exists():
            logger.info("Skipping %s (already processed)", input_file.name)
            continue

        with input_file.open(encoding="utf-8") as f:
            lines = [line.strip() for line in f.readlines() if line.strip()]

        file_batches = create_batches_by_bytes(lines, max_batch_bytes=max_batch_bytes)
        logger.info(
            "[%s/%s] Processing %s (%s lines, %s batches)",
            idx, len(jsonl_files), input_file.name, len(lines), len(file_batches),
        )

        total_chunks, elapsed = write_batches_to_file(
            file_batches=file_batches,
            output_path=output_file,
            max_workers=max_workers,
            total_lines=len(lines),
            chunk_size=chunk_size,
            overlap=overlap,
        )
        logger.info(
            "Finished %s -> %s | chunks=%s | time=%.1fs",
            input_file.name, output_file.name, total_chunks, elapsed,
        )


# --- Rechunking by source ---

def _load_init_config(init_file_path: Path) -> dict[str, dict[str, Any]]:
    """Load source-to-chunk_ids mapping from init.json."""
    try:
        with open(init_file_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        logger.info("Loaded init config with %d sources", len(config))
        return config
    except Exception as e:
        logger.error("Failed to load init config from %s: %s", init_file_path, e)
        return {}


def _load_chunk_files(
    chunk_dir: Path, config: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    """Load chunks from JSONL files, optionally filtered by config."""
    chunk_map = {}
    chunk_files = list(chunk_dir.glob("*.jsonl"))

    required_chunk_ids = None
    if config:
        required_chunk_ids = set()
        for source_config in config.values():
            required_chunk_ids.update(source_config.get("chunk_ids", []))

    for chunk_file in chunk_files:
        try:
            with open(chunk_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        chunk_data = json.loads(line)
                        chunk_id = chunk_data.get("chunk_id")
                        if chunk_id and (required_chunk_ids is None or chunk_id in required_chunk_ids):
                            chunk_map[chunk_id] = chunk_data
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            logger.error("Failed to read chunk file %s: %s", chunk_file, e)

    logger.info("Loaded %d chunks from %d files", len(chunk_map), len(chunk_files))
    return chunk_map


def _build_merged_tokens_and_ranges(chunk_ids, chunk_map, encoding):
    merged_tokens = []
    original_ranges = []

    cur = 0
    for cid in chunk_ids:
        chunk = chunk_map.get(cid)
        if not chunk:
            continue

        text = chunk.get("text", "")
        if not text:
            continue

        tokens = encoding.encode(text)
        length = len(tokens)
        merged_tokens.extend(tokens)

        source_chunk = chunk.get("source_chunk")
        if source_chunk is None:
            source_chunk = [cid]
        elif isinstance(source_chunk, str):
            source_chunk = json.loads(source_chunk)

        original_ranges.append({
            "token_start": cur,
            "token_end": cur + length,
            "source_chunk": source_chunk,
        })

        cur += length

    return merged_tokens, original_ranges


def _collect_source_chunks(new_start, new_end, original_ranges):
    result = set()
    for r in original_ranges:
        if r["token_end"] > new_start and r["token_start"] < new_end:
            result.update(r["source_chunk"])
    return list(result)


def _merge_and_rechunk_sources(
    init_config, chunk_map, chunk_size=1024, overlap=50, encoding_name="cl100k_base",
):
    encoding = tiktoken.get_encoding(encoding_name)
    source_chunks = {}

    for source, config in init_config.items():
        chunk_ids = config.get("chunk_ids", [])
        if not chunk_ids:
            continue

        merged_tokens, original_ranges = _build_merged_tokens_and_ranges(
            chunk_ids, chunk_map, encoding,
        )

        if not merged_tokens:
            continue

        merged_text = encoding.decode(merged_tokens)

        new_chunks = []
        rechunks = split_text_with_overlap_with_pos(
            merged_text, chunk_size, overlap, encoding_name,
        )

        for item in rechunks:
            text = item["text"]
            ts, te = item["token_start"], item["token_end"]
            source_chunk = _collect_source_chunks(ts, te, original_ranges)
            new_chunks.append({
                "chunk_id": generate_chunk_id(text),
                "text": text,
                "source": source,
                "source_chunk": source_chunk,
            })

        source_chunks[source] = new_chunks
        logger.info("Source %s: %d -> %d chunks", source, len(chunk_ids), len(new_chunks))

    return source_chunks


def _write_rechunked_sources(
    source_chunks: dict[str, list[dict[str, Any]]], output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    total_chunks = 0
    for source, chunks in source_chunks.items():
        if not chunks:
            continue

        output_filename = f"rechunk_{source.replace('.', '_')}.jsonl"
        output_path = output_dir / output_filename

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                for chunk in chunks:
                    f.write(json.dumps(chunk, ensure_ascii=False) + "\n")
            total_chunks += len(chunks)
        except Exception as e:
            logger.error("Failed to write rechunks for source %s: %s", source, e)

    logger.info("Total rechunked sources: %d, total chunks: %d", len(source_chunks), total_chunks)


def rechunk_by_source(
    chunk_dir: Path,
    output_dir: Path,
    chunk_size: int = 1024,
    overlap: int = 50,
) -> None:
    """Rechunk by merging chunks per source document and re-splitting."""
    start_time = time.time()

    init_file_path = chunk_dir / "init.json"
    if not init_file_path.exists():
        logger.warning("init.json not found at %s", init_file_path)
        return

    init_config = _load_init_config(init_file_path)
    if not init_config:
        return

    chunk_map = _load_chunk_files(chunk_dir, config=init_config)
    if not chunk_map:
        return

    source_chunks = _merge_and_rechunk_sources(init_config, chunk_map, chunk_size, overlap)
    _write_rechunked_sources(source_chunks, output_dir)

    logger.info("Rechunking completed in %.1fs", time.time() - start_time)
