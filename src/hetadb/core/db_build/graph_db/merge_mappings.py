"""Adaptive merge of node-name mapping files.

Loads the global mapping table and per-round dedup mapping files,
builds equivalence classes via Union-Find, and produces a single
old_name -> canonical_name mapping.
"""

import json
import logging
import re
from collections import defaultdict
from pathlib import Path

logger = logging.getLogger(__name__)


def normalize_name(name: str) -> str:
    """Lowercase and strip whitespace for case-insensitive matching."""
    return name.strip().lower()


def contains_chinese(s: str) -> bool:
    """Return True if *s* contains any CJK Unified Ideograph."""
    return any("\u4e00" <= c <= "\u9fff" for c in s)


def choose_canonical(names: list[str]) -> str:
    """Pick a canonical name: prefer Chinese, then longer, keep original case."""
    unique_names = list(set(names))
    unique_names.sort(key=lambda x: (not contains_chinese(x), -len(x)))
    return unique_names[0]


class UnionFind:
    """Disjoint-set with path compression."""

    def __init__(self):
        self.parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        if x not in self.parent:
            self.parent[x] = x
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra

    def groups(self) -> dict[str, list[str]]:
        res: dict[str, list[str]] = defaultdict(list)
        for x in self.parent:
            res[self.find(x)].append(x)
        return dict(res)


def load_adaptive_mappings(
    batch_merge_dir: str,
    final_nodes_dir: str,
) -> list[dict[str, list[str]]]:
    """Load mapping files in order: global table first, then per-round dedup mappings."""
    mappings: list[dict[str, list[str]]] = []

    # Global mapping table
    global_path = Path(batch_merge_dir) / "global_mapping_table.json"
    if global_path.exists():
        try:
            with global_path.open("r", encoding="utf-8") as f:
                mappings.append(json.load(f))
            logger.info("Loaded global mapping: %s", global_path)
        except Exception as e:
            logger.error("Failed to load %s: %s", global_path, e)
    else:
        logger.warning("Global mapping file not found: %s", global_path)

    # Per-round dedup mapping files
    final_path = Path(final_nodes_dir)
    if final_path.exists():
        mapping_files = sorted(
            final_path.glob("merged_round_*_dedup_mapping.json"),
            key=lambda p: int(m.group(1)) if (m := re.search(r"merged_round_(\d+)_dedup_mapping\.json", p.name)) else 0,
        )
        for mf in mapping_files:
            try:
                with mf.open("r", encoding="utf-8") as f:
                    mappings.append(json.load(f))
                logger.info("Loaded round mapping: %s", mf)
            except Exception as e:
                logger.error("Failed to load %s: %s", mf, e)
    else:
        logger.warning("Final nodes directory not found: %s", final_path)

    logger.info("Loaded %d mapping files", len(mappings))
    return mappings


def build_equivalence_classes(
    mappings: list[dict[str, list[str]]],
) -> dict[str, list[str]]:
    """Union all new_name <-> old_name pairs and return equivalence groups."""
    uf = UnionFind()
    for mp in mappings:
        for new_name, old_names in mp.items():
            for o in old_names:
                uf.union(new_name, o)
    return uf.groups()


def build_new_to_old(eq_classes: dict[str, list[str]]) -> dict[str, list[str]]:
    """Build canonical_name -> [all aliases] mapping."""
    new2old: dict[str, list[str]] = {}
    for _, names in eq_classes.items():
        canon = choose_canonical(names)
        new2old[canon] = sorted(set(names))
    return new2old


def build_old_to_new(eq_classes: dict[str, list[str]]) -> dict[str, str]:
    """Build old_name -> canonical_name mapping."""
    old2new: dict[str, str] = {}
    for _, names in eq_classes.items():
        canon = choose_canonical(names)
        for n in names:
            old2new[n] = canon
    return old2new


def merge_mappings_adaptive(
    batch_merge_dir: str,
    final_nodes_dir: str,
    output_dir: str,
) -> None:
    """Merge all mapping files and produce a final old -> canonical mapping.

    Args:
        batch_merge_dir: Directory containing ``global_mapping_table.json``.
        final_nodes_dir: Directory containing ``merged_round_*_dedup_mapping.json``.
        output_dir: Output directory for the final mapping file.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    mappings = load_adaptive_mappings(batch_merge_dir, final_nodes_dir)
    if not mappings:
        logger.warning("No mapping files found, nothing to merge")
        return

    eq_classes = build_equivalence_classes(mappings)
    old2new = build_old_to_new(eq_classes)

    final_file = output_path / "final_mapping.json"
    with final_file.open("w", encoding="utf-8") as f:
        json.dump(old2new, f, ensure_ascii=False, indent=2)

    logger.info(
        "Mapping merge complete: %d unique mappings, saved to %s",
        len(old2new), final_file,
    )
