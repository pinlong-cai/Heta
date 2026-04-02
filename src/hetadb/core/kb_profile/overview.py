"""KB overview generation for HetaGen tree skeleton context.

Reads final node and relation JSONL files from the workspace and produces:
  - A structured overview dict (raw data)
  - A formatted prompt string for LLM consumption

Typical usage (HetaGen Step 1 — context preparation):
    overview = generate_kb_overview(kb_name, workspace_root)
    prompt_context = format_for_prompt(overview)
    # → feed prompt_context to LLM for tree skeleton generation
"""

import json
import logging
from collections import Counter
from pathlib import Path

logger = logging.getLogger(__name__)

# Max description length before truncation (characters, not tokens)
_DESC_MAX_CHARS = 120
# Number of top SubTypes shown per Type in the prompt
_TOP_SUBTYPES_PER_TYPE = 10


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _read_jsonl_dir(directory: Path) -> list[dict]:
    """Read all *.jsonl files in *directory* and return a flat list of records."""
    if not directory.exists():
        return []
    records = []
    for path in sorted(directory.glob("*.jsonl")):
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    logger.warning("Skipped malformed JSON line in %s", path)
    return records


def _truncate(text: str, max_chars: int = _DESC_MAX_CHARS) -> str:
    """Truncate *text* to *max_chars* characters, appending '...' if cut."""
    if not text:
        return ""
    text = text.strip()
    return text if len(text) <= max_chars else text[:max_chars].rstrip() + "..."


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def generate_kb_overview(
    kb_name: str,
    workspace_root: Path,
    top_nodes: int = 20,
    sample_relations: int = 15,
) -> dict:
    """Generate a structured overview of a knowledge base.

    Reads from ``workspace/kb/{kb_name}/`` across all datasets, aggregating
    final nodes and relations.  No database connection required.

    Args:
        kb_name:          Name of the knowledge base.
        workspace_root:   Resolved workspace root path.
        top_nodes:        Number of high-connectivity nodes to sample.
        sample_relations: Number of hierarchical relations to sample.

    Returns:
        Dict with keys ``kb_name``, ``datasets``, ``stats``,
        ``high_connectivity_nodes``, and ``hierarchical_relations``.
        Returns a minimal dict with empty stats if the KB has no processed data.
    """
    kb_path = workspace_root / "kb" / kb_name
    if not kb_path.exists():
        logger.warning("KB '%s' not found at %s", kb_name, kb_path)
        return _empty_overview(kb_name)

    datasets = sorted(d.name for d in kb_path.iterdir() if d.is_dir())
    if not datasets:
        return _empty_overview(kb_name)

    all_nodes: list[dict] = []
    all_relations: list[dict] = []

    for dataset in datasets:
        base = kb_path / dataset / "kg_file"
        all_nodes.extend(_read_jsonl_dir(base / "final_nodes"))
        all_relations.extend(_read_jsonl_dir(base / "final_res"))

    # --- stats ---
    type_counter: Counter = Counter()
    subtype_by_type: dict[str, Counter] = {}

    for node in all_nodes:
        t = node.get("Type") or "Unknown"
        sub = node.get("SubType") or "Unknown"
        type_counter[t] += 1
        subtype_by_type.setdefault(t, Counter())[sub] += 1

    # --- connectivity-based node sampling ---
    node_degree: Counter = Counter()
    for rel in all_relations:
        for field in ("Node1", "Node2"):
            name = rel.get(field, "").strip()
            if name:
                node_degree[name] += 1

    # Build NodeName → node record map (last-write wins on duplicates)
    node_by_name: dict[str, dict] = {
        n["NodeName"]: n for n in all_nodes if n.get("NodeName")
    }

    high_connectivity_nodes = []
    for name, degree in node_degree.most_common():
        if len(high_connectivity_nodes) >= top_nodes:
            break
        node = node_by_name.get(name)
        if node is None:
            continue
        high_connectivity_nodes.append({
            "name": name,
            "type": node.get("Type", ""),
            "subtype": node.get("SubType", ""),
            "description": node.get("Description", ""),
            "degree": degree,
        })

    # --- hierarchical relation sampling ---
    hierarchical_types = {"分类关系", "部分整体关系"}
    hierarchical_relations = []
    seen_pairs: set[tuple] = set()

    for rel in all_relations:
        if rel.get("Type") not in hierarchical_types:
            continue
        pair = (rel.get("Node1", ""), rel.get("Node2", ""))
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        hierarchical_relations.append({
            "node1": rel.get("Node1", ""),
            "node2": rel.get("Node2", ""),
            "relation": rel.get("Relation", ""),
            "type": rel.get("Type", ""),
            "description": rel.get("Description", ""),
        })
        if len(hierarchical_relations) >= sample_relations:
            break

    return {
        "kb_name": kb_name,
        "datasets": datasets,
        "stats": {
            "total_nodes": len(all_nodes),
            "total_relations": len(all_relations),
            "type_distribution": {
                t: {
                    "count": count,
                    "subtypes": dict(subtype_by_type[t].most_common(_TOP_SUBTYPES_PER_TYPE)),
                }
                for t, count in type_counter.most_common()
            },
        },
        "high_connectivity_nodes": high_connectivity_nodes,
        "hierarchical_relations": hierarchical_relations,
    }


def _empty_overview(kb_name: str) -> dict:
    return {
        "kb_name": kb_name,
        "datasets": [],
        "stats": {"total_nodes": 0, "total_relations": 0, "type_distribution": {}},
        "high_connectivity_nodes": [],
        "hierarchical_relations": [],
    }


def format_for_prompt(overview: dict) -> str:
    """Format a KB overview dict as a compact text string for LLM consumption.

    The output is intended as context for tree skeleton generation in HetaGen.
    Total length is kept around 400 tokens for prompt budget efficiency.

    Args:
        overview: Dict returned by :func:`generate_kb_overview`.

    Returns:
        Multi-section markdown-style string ready to be embedded in a prompt.
    """
    lines: list[str] = []

    # --- header ---
    lines.append("## 知识库概况")
    lines.append(f"知识库名称：{overview['kb_name']}")
    if overview["datasets"]:
        lines.append(f"数据集：{'、'.join(overview['datasets'])}")
    stats = overview["stats"]
    lines.append(
        f"节点总数：{stats['total_nodes']:,}  关系总数：{stats['total_relations']:,}"
    )

    # --- type distribution ---
    type_dist = stats.get("type_distribution", {})
    if type_dist:
        lines.append("\n## 实体类型分布")
        for t, info in type_dist.items():
            count = info["count"]
            subtypes = info.get("subtypes", {})
            if subtypes:
                subtype_str = "、".join(
                    f"{sub} {n}个" for sub, n in subtypes.items()
                )
                lines.append(f"{t}：{count} 个（{subtype_str}）")
            else:
                lines.append(f"{t}：{count} 个")

    # --- high connectivity nodes ---
    nodes = overview.get("high_connectivity_nodes", [])
    if nodes:
        lines.append("\n## 核心节点（按关联度排序）")
        for i, node in enumerate(nodes, 1):
            label = f"{node['subtype']}" if node.get("subtype") else node.get("type", "")
            desc = _truncate(node.get("description", ""))
            degree_hint = f"degree={node['degree']}"
            lines.append(f"{i}. {node['name']} [{label}, {degree_hint}]")
            if desc:
                lines.append(f"   {desc}")

    # --- hierarchical relations ---
    rels = overview.get("hierarchical_relations", [])
    if rels:
        lines.append("\n## 领域层级结构示例")
        for rel in rels:
            desc = _truncate(rel.get("description", ""), max_chars=60)
            rel_label = rel.get("relation") or rel.get("type", "")
            line = f"- {rel['node1']} → [{rel_label}] → {rel['node2']}"
            if desc:
                line += f"  （{desc}）"
            lines.append(line)

    return "\n".join(lines)
