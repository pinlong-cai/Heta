"""Shared persistence-layer config accessor.

Provides a single source of truth for infrastructure connection parameters
(postgresql, milvus, neo4j) read from the project-level ``config.yaml``.
"""

from pathlib import Path

import yaml

_PROJECT_ROOT = Path(__file__).resolve().parents[3]


def get_persistence(service: str) -> dict:
    """Return the connection config for a shared persistence service.

    Args:
        service: One of ``"postgresql"``, ``"milvus"``, ``"neo4j"``.

    Returns:
        The service config dict, or an empty dict if not found.
    """
    with open(_PROJECT_ROOT / "config.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg.get("persistence", {}).get(service, {})
