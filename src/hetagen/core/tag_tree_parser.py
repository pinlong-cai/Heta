"""Tag tree parser: build a structured knowledge tree from a local Excel file.

Parses an Excel file where each column represents a hierarchy level,
enriches every node with LLM-generated descriptions, and outputs a
nested JSON tree.
"""

import json
import logging
import yaml
import asyncio
import pandas as pd
from pathlib import Path
import argparse

from common.llm_client import create_use_llm_async

logger = logging.getLogger(__name__)


def parse_excel_file(file_path: Path, sheet_name: str | int = 0) -> pd.DataFrame:
    """Read the given Excel file, drop all-empty rows, and return a cleaned DataFrame."""
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    if not str(file_path).endswith(('.xlsx', '.xls')):
        raise ValueError("Only .xlsx or .xls files are supported")

    df = pd.read_excel(file_path, sheet_name=sheet_name)
    df = df.dropna(how='all')
    logger.info("Parsed %s (sheet: %s) — %d rows, %d columns", file_path, sheet_name, len(df), len(df.columns))
    return df


def build_tree_structure(df: pd.DataFrame) -> list[list[str]]:
    """Convert a DataFrame into a list of paths, one per leaf node's full classification chain."""
    df_filled = df.ffill()
    tree_paths = []
    for _, row in df_filled.iterrows():
        path = []
        for col in df_filled.columns:
            val = row[col]
            if pd.notna(val) and str(val).strip():
                path.append(str(val).strip())
        if path:
            tree_paths.append(path)
    logger.info("Built tree structure: %d paths", len(tree_paths))
    return tree_paths


def build_nested_tree_from_paths(tree_paths: list[list[str]]) -> list[dict]:
    """Convert flat path lists into a nested tree; each node has 'node_name' and 'children'."""
    root_nodes = []
    node_map = {}  # key: (parent_key, name) -> node dict

    for path in tree_paths:
        current_level = root_nodes
        parent_key = None
        for i, node_name in enumerate(path):
            node_key = (parent_key, node_name)
            if node_key not in node_map:
                node_dict = {
                    "node_name": node_name,
                    "category": "",
                    "description": "",
                    "children": []}
                node_map[node_key] = node_dict
                current_level.append(node_dict)
            else:
                node_dict = node_map[node_key]
                # Avoid duplicate insertion when paths overlap
                if node_dict not in current_level:
                    current_level.append(node_dict)
            current_level = node_dict["children"]
            parent_key = node_key

    return root_nodes


async def enrich_nodes_with_llm(tree_paths: list[list[str]], qa_client) -> dict[tuple, dict]:
    """Call LLM for every unique path prefix (node) to generate descriptions."""
    unique_nodes = set()
    for path in tree_paths:
        for i in range(len(path)):
            unique_nodes.add(tuple(path[:i+1]))

    total = len(unique_nodes)
    logger.info("Generating descriptions for %d unique nodes", total)

    node_info_map = {}
    count = 0

    for path_tuple in unique_nodes:
        count += 1
        node_name = path_tuple[-1]

        prompt = f'请简要描述"{node_name}"的定义或特征，其分类路径为：{" -> ".join(path_tuple)}。注意：生成1个文本段落即可，不用赘述分类路径信息，学科术语要给出中文对应名称，字数不超过200字。'

        try:
            description = await qa_client(prompt)
        except Exception as e:
            logger.error("LLM call failed for %s: %s", path_tuple, e)
            description = ""

        node_info_map[path_tuple] = {
            "category": ' -> '.join(path_tuple),
            "description": description
        }

        if count % 5 == 0 or count == total:
            logger.info("LLM description progress: %d/%d (%.1f%%)", count, total, count / total * 100)

    logger.info("LLM node description complete — %d nodes processed", count)
    return node_info_map


def inject_enrich_info(nodes: list[dict], enrich_map: dict[tuple, dict], parent_path: list[str] | None = None):
    """Recursively inject enrichment info from *enrich_map* into the nested tree."""
    if parent_path is None:
        parent_path = []

    for node in nodes:
        current_path = parent_path + [node["node_name"]]
        path_key = tuple(current_path)

        info = enrich_map.get(path_key, {})
        node["category"] = info.get("category", "")
        node["description"] = info.get("description", "")

        inject_enrich_info(node["children"], enrich_map, current_path)


async def parse_tag_tree(
    input_excel: str,
    output_json: str = "tag_tree.json",
    tree_name: str = "local_tag_tree",
    tree_description: str | None = None,
    qa_client=None,
    sheet_name: str | int = 0,
):
    """Parse Excel, build paths, enrich with LLM, assemble nested tree, and write JSON."""
    input_path = Path(input_excel)
    output_path = Path(output_json)

    # 1. Parse Excel
    df = parse_excel_file(input_path, sheet_name=sheet_name)

    # 2. Build paths
    tree_paths = build_tree_structure(df)

    # 3. Enrich nodes via LLM
    node_enrich_info = await enrich_nodes_with_llm(tree_paths, qa_client)

    # 4. Build nested structure
    nested_tree = build_nested_tree_from_paths(tree_paths)

    # 5. Inject descriptions
    inject_enrich_info(nested_tree, node_enrich_info)

    # 6. Assemble output
    result = {
        "tree_name": tree_name,
        "tree_description": tree_description or "",
        "nodes": nested_tree,
        "node_count": len(set(
            node
            for path in tree_paths
            for node in path
        )),
        "path_count": len(tree_paths),
        "enriched": bool(node_enrich_info)
    }

    # 7. Save JSON
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    logger.info("Tag tree saved to: %s", output_path)
    return result


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Build a tag tree from a local Excel file")
    parser.add_argument("--input", default="example_data/tag_tree/biological_taxonomy.xlsx", help="Path to input Excel file")
    parser.add_argument("--output", default="example_data/tag_tree/tag_tree.json", help="Path to output JSON file")
    parser.add_argument("--name", default="生物分类标签树", help="Name of the tag tree")
    parser.add_argument("--desc", default="基于生物分类学构建的标签体系", help="Description of the tag tree")
    parser.add_argument("--sheet", default=0, help="Excel sheet name or index (default: 0, i.e. the first sheet)")

    args = parser.parse_args()

    from hetagen.utils.path import PROJECT_ROOT
    with open(PROJECT_ROOT / "config.yaml", encoding="utf-8") as f:
        llm_config = yaml.safe_load(f)["hetagen"]["llm"]

    my_llm = create_use_llm_async(
        url=llm_config["base_url"],
        api_key=llm_config["api_key"],
        model=llm_config["model"],
    )

    asyncio.run(parse_tag_tree(
        input_excel=args.input,
        output_json=args.output,
        tree_name=args.name,
        tree_description=args.desc,
        qa_client=my_llm,
        sheet_name=args.sheet,
    ))
