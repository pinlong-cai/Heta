"""KB-driven knowledge tree generation pipeline.

Three-step process:
    1. KB Overview  — build context from KB (KB mode only)
    2. Skeleton     — one LLM call generates the full tree structure
    3. Enrichment   — parallel LLM calls fill node descriptions

Two modes:
    ``"kb"``       — uses real KB data as LLM context
    ``"pure_llm"`` — LLM uses its own world knowledge only
"""

import asyncio
import json
import logging
import re
from collections.abc import Awaitable, Callable
from pathlib import Path

from hetagen.core.kb_tree.prompts import (
    ENRICH_PROMPT_KB,
    ENRICH_PROMPT_PURE_LLM,
    SKELETON_PROMPT_KB,
    SKELETON_PROMPT_PURE_LLM,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def generate_tree(
    topic: str,
    mode: str,
    llm_client: Callable[..., Awaitable[str]],
    kb_name: str | None = None,
    workspace_root: Path | None = None,
    embedding_cfg: dict | None = None,
) -> dict:
    """Generate a knowledge tree for *topic*.

    Args:
        topic:          Domain or subject for the tree (e.g. "糖尿病诊疗").
        mode:           ``"kb"`` or ``"pure_llm"``.
        llm_client:     Async function ``(prompt: str) -> str``.
        kb_name:        Required when ``mode="kb"``.
        workspace_root: Required when ``mode="kb"``.
        embedding_cfg:  Required when ``mode="kb"`` for node enrichment.
                        Dict with keys ``api_key``, ``embedding_url``,
                        ``embedding_model``, ``embedding_timeout``.

    Returns:
        Dict with ``tree_name``, ``tree_description``, ``nodes``, ``mode``,
        ``node_count`` keys, compatible with the existing tag-tree response format.
    """
    if mode not in {"kb", "pure_llm"}:
        raise ValueError(f"mode must be 'kb' or 'pure_llm', got '{mode}'")
    if mode == "kb" and not kb_name:
        raise ValueError("kb_name is required for mode='kb'")

    # --- Step 1: prepare KB context ---
    kb_overview_text = ""
    datasets: list[str] = []
    if mode == "kb":
        from hetadb.core.kb_profile.overview import generate_kb_overview, format_for_prompt
        overview = generate_kb_overview(kb_name, workspace_root)
        kb_overview_text = format_for_prompt(overview)
        datasets = overview.get("datasets", [])
        logger.info("KB overview generated for '%s': %d datasets", kb_name, len(datasets))

    # --- Step 2: generate skeleton ---
    logger.info("Generating tree skeleton for topic '%s' (mode=%s)", topic, mode)
    nodes = await _generate_skeleton(topic, mode, kb_overview_text, llm_client)
    if not nodes:
        logger.warning("Skeleton generation produced no nodes")
        return _build_result(topic, mode, kb_name, nodes=[])

    # --- Step 3: enrich node descriptions ---
    flat = _flatten_nodes(nodes)
    logger.info("Enriching %d nodes (mode=%s)", len(flat), mode)

    if mode == "kb":
        await _enrich_nodes_kb(
            flat, kb_name, datasets, llm_client,
            embedding_cfg or {}, workspace_root,
        )
    else:
        await _enrich_nodes_pure_llm(flat, llm_client)

    return _build_result(topic, mode, kb_name, nodes)


# ---------------------------------------------------------------------------
# Step 2: skeleton generation
# ---------------------------------------------------------------------------

async def _generate_skeleton(
    topic: str,
    mode: str,
    kb_overview_text: str,
    llm_client: Callable[..., Awaitable[str]],
) -> list[dict]:
    """Call LLM once to produce the full tree skeleton (descriptions left empty)."""
    if mode == "kb":
        prompt = SKELETON_PROMPT_KB.format(topic=topic, kb_overview=kb_overview_text)
    else:
        prompt = SKELETON_PROMPT_PURE_LLM.format(topic=topic)

    raw = await llm_client(prompt)
    return _parse_tree_json(raw)


def _parse_tree_json(raw: str) -> list[dict]:
    """Extract a JSON array from the LLM response, tolerating markdown fences."""
    # Strip ```json ... ``` or ``` ... ``` fences
    text = re.sub(r"```(?:json)?\s*", "", raw).strip().rstrip("`").strip()

    # Find the outermost JSON array
    start = text.find("[")
    end = text.rfind("]")
    if start == -1 or end == -1:
        logger.error("No JSON array found in LLM skeleton response")
        return []
    try:
        nodes = json.loads(text[start : end + 1])
        if not isinstance(nodes, list):
            logger.error("Parsed skeleton is not a list")
            return []
        return nodes
    except json.JSONDecodeError as e:
        logger.error("Failed to parse skeleton JSON: %s\nRaw: %s", e, text[:300])
        return []


# ---------------------------------------------------------------------------
# Step 3: node enrichment helpers
# ---------------------------------------------------------------------------

def _flatten_nodes(
    nodes: list[dict],
    parent_path: list[str] | None = None,
) -> list[tuple[dict, str]]:
    """Recursively flatten the tree into (node_dict, category_path) pairs."""
    if parent_path is None:
        parent_path = []
    result = []
    for node in nodes:
        name = node.get("node_name", "")
        path = parent_path + [name]
        category_path = " -> ".join(path)
        node["category"] = category_path
        result.append((node, category_path))
        children = node.get("children", [])
        if children:
            result.extend(_flatten_nodes(children, path))
    return result


async def _enrich_nodes_pure_llm(
    flat: list[tuple[dict, str]],
    llm_client: Callable[..., Awaitable[str]],
) -> None:
    """Fill descriptions using the LLM's own knowledge (no KB context)."""
    async def _enrich_one(node: dict, category_path: str) -> None:
        prompt = ENRICH_PROMPT_PURE_LLM.format(
            node_name=node["node_name"],
            category_path=category_path,
        )
        try:
            node["description"] = await llm_client(prompt)
        except Exception as e:
            logger.error("LLM call failed for node '%s': %s", node["node_name"], e)
            node["description"] = ""

    await asyncio.gather(*[_enrich_one(n, p) for n, p in flat])


async def _enrich_nodes_kb(
    flat: list[tuple[dict, str]],
    kb_name: str,
    datasets: list[str],
    llm_client: Callable[..., Awaitable[str]],
    embedding_cfg: dict,
    workspace_root: Path | None,
) -> None:
    """Fill descriptions using vector-retrieved KB entities as context."""
    from hetadb.core.kb_profile.entity_search import search_kb_entities

    async def _enrich_one(node: dict, category_path: str) -> None:
        node_name = node["node_name"]
        query = f"{node_name} {category_path}"

        # Run blocking Milvus search in a thread pool
        loop = asyncio.get_event_loop()
        hits = await loop.run_in_executor(
            None,
            lambda: search_kb_entities(
                query=query,
                kb_name=kb_name,
                datasets=datasets,
                embedding_cfg=embedding_cfg,
                top_k=5,
            ),
        )

        if hits:
            entities_text = "\n".join(
                f"- {h['nodename']}：{h['description']}" for h in hits if h.get("nodename")
            )
        else:
            # Fallback: pure LLM if no KB hits
            logger.debug("No KB hits for '%s', falling back to pure LLM", node_name)
            entities_text = ""

        if entities_text:
            prompt = ENRICH_PROMPT_KB.format(
                node_name=node_name,
                category_path=category_path,
                kb_entities=entities_text,
            )
        else:
            prompt = ENRICH_PROMPT_PURE_LLM.format(
                node_name=node_name,
                category_path=category_path,
            )

        try:
            node["description"] = await llm_client(prompt)
        except Exception as e:
            logger.error("LLM call failed for node '%s': %s", node_name, e)
            node["description"] = ""

    await asyncio.gather(*[_enrich_one(n, p) for n, p in flat])


# ---------------------------------------------------------------------------
# Result builder
# ---------------------------------------------------------------------------

def _build_result(
    topic: str,
    mode: str,
    kb_name: str | None,
    nodes: list[dict],
) -> dict:
    source = f"知识库 {kb_name}" if mode == "kb" else "大模型"
    node_count = _count_nodes(nodes)
    return {
        "tree_name": topic,
        "tree_description": f"基于{source}生成的领域知识树",
        "nodes": nodes,
        "node_count": node_count,
        "mode": mode,
    }


def _count_nodes(nodes: list[dict]) -> int:
    count = 0
    for node in nodes:
        count += 1
        count += _count_nodes(node.get("children", []))
    return count
