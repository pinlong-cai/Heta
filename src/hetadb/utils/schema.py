"""Workspace schema utilities for custom entity extraction schemas."""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# The four top-level Type values recognised by the KG extraction prompt.
VALID_TYPES = {"客观实体", "抽象实体", "事件实体", "文献实体"}


def schema_entities_to_str(entities: list[dict]) -> str:
    """Convert a list of entity definitions to a double-brace JSON schema string.

    Builds a two-level dict ``{Type: {SubType: [attrs]}}`` and JSON-encodes it
    with ``{{``/``}}`` escaping so the result can be safely embedded in a
    Python ``.format()`` prompt template.

    Args:
        entities: List of dicts with keys ``type``, ``subtype``, ``attributes``.

    Returns:
        JSON string safe for use as ``{entity_schema}`` in prompt templates.
    """
    nested: dict[str, dict[str, list[str]]] = {}
    for ent in entities:
        t = ent.get("type", "")
        sub = ent.get("subtype", "")
        attrs = ent.get("attributes", [])
        if t and sub:
            nested.setdefault(t, {})[sub] = attrs

    json_str = json.dumps(nested, ensure_ascii=False, indent=2)
    return json_str.replace("{", "{{").replace("}", "}}")


def load_workspace_schema(workspace_root: Path, schema_name: str) -> str:
    """Load a named schema from ``workspace/schemas/`` and return as entity_schema_str.

    Returns an empty string if the schema file does not exist, so callers
    can fall back to the default entity template.

    Args:
        workspace_root: Resolved workspace root path.
        schema_name:    Name of the schema (without ``.json`` extension).

    Returns:
        Double-brace JSON string ready for prompt injection, or ``""`` if not found.
    """
    schema_path = workspace_root / "schemas" / f"{schema_name}.json"
    if not schema_path.exists():
        logger.warning("Schema '%s' not found at %s", schema_name, schema_path)
        return ""

    with open(schema_path, encoding="utf-8") as f:
        data = json.load(f)

    entities = data.get("entities", [])
    logger.info("Loaded schema '%s' with %d entity definitions", schema_name, len(entities))
    return schema_entities_to_str(entities)
