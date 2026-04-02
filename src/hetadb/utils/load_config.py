"""Centralized configuration loading with lazy initialization and thread safety.

Reads from two sources:
- Project config: ``PROJECT_ROOT/config.yaml`` → connections, API keys, server settings.
- DB parameter config: ``PACKAGE_ROOT/config/db_config.yaml`` → processing parameters.
"""

from __future__ import annotations

import logging
from pathlib import Path
from threading import Lock
from typing import Any

import yaml

from common.config import get_persistence
from hetadb.utils.path import PROJECT_ROOT, PACKAGE_ROOT

logger = logging.getLogger("hetadb.load_config")

_config: dict[str, Any] = {}
_config_lock = Lock()


def _load_yaml(path: Path) -> dict[str, Any]:
    """Load a YAML file and return its content as a dict."""
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Config file must contain a mapping: {path}")
    return data


def reload_config():
    """Reload all configs from disk, injecting shared persistence values."""
    global _config
    with _config_lock:
        project = _load_yaml(PROJECT_ROOT / "config.yaml")["hetadb"]
        # Inject shared persistence: postgresql comes entirely from the global section;
        # milvus merges global host/port/url with module-specific keys (e.g. sentence_mode).
        project["postgresql"] = get_persistence("postgresql")
        milvus_globals = get_persistence("milvus")
        project["milvus"] = {**milvus_globals, **project.get("milvus", {})}
        db_param = _load_yaml(PACKAGE_ROOT / "config" / "db_config.yaml")
        _config = {
            "project": project,
            "db_param": db_param,
        }


def _ensure_loaded():
    if not _config:
        reload_config()


def get_embedding_cfg() -> dict[str, Any]:
    """Return embedding API config."""
    _ensure_loaded()
    with _config_lock:
        return _config["project"]["embedding_api"]


def get_chat_cfg() -> dict[str, Any]:
    """Return LLM chat config."""
    _ensure_loaded()
    with _config_lock:
        return _config["project"]["llm"]


def get_vlm_cfg() -> dict[str, Any]:
    """Return VLM config."""
    _ensure_loaded()
    with _config_lock:
        return _config["project"]["vlm"]


def get_postgres_conn_config() -> dict[str, Any]:
    """Return PostgreSQL connection config.

    Can be passed directly to ``psycopg2.connect(**config)``.
    """
    _ensure_loaded()
    with _config_lock:
        return _config["project"]["postgresql"]


def get_milvus_config() -> dict[str, Any]:
    """Return Milvus connection config."""
    _ensure_loaded()
    with _config_lock:
        mv = _config["project"]["milvus"]
        return {
            "alias": "default",
            "host": mv["host"],
            "port": mv["port"],
            "db_name": mv.get("db_name", "default"),
        }


def get_fastapi_config() -> dict[str, Any]:
    """Return FastAPI server config."""
    _ensure_loaded()
    with _config_lock:
        return _config["project"]["fastapi"]


def get_s3_config() -> dict[str, Any]:
    """Return S3 config."""
    _ensure_loaded()
    with _config_lock:
        return _config["project"]["s3"]


def get_oss_config() -> dict[str, Any]:
    """Return OSS config."""
    _ensure_loaded()
    with _config_lock:
        return _config["project"]["oss"]


def get_query_defaults() -> dict[str, Any]:
    """Return query default parameters (top_k, threshold, weights)."""
    _ensure_loaded()
    with _config_lock:
        return _config["project"].get("query_defaults", {
            "top_k": 10,
            "threshold": 0.0,
            "similarity_weight": 1.5,
            "occur_weight": 1.0,
        })


def get_search_params() -> dict[str, Any]:
    """Return Milvus HNSW search parameters with sensible defaults."""
    _ensure_loaded()
    with _config_lock:
        return _config["project"].get("search_params", {
            "ef_multiplier": 10,
        })


def get_db_param_config() -> dict[str, Any]:
    """Return processing parameter config (chunk, graph, vector)."""
    _ensure_loaded()
    with _config_lock:
        return _config["db_param"]["parameter"]
