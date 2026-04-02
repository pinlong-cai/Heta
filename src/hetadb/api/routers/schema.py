"""Custom entity schema management API.

Schemas are stored as JSON files under ``workspace/schemas/`` and can be
referenced by name when triggering KB processing.  Each schema defines a set
of entity types (SubTypes) and their extractable attributes; Type must be one
of the four system-level categories.

Endpoints:
    POST   /api/v1/hetadb/schemas            — create a new schema
    GET    /api/v1/hetadb/schemas            — list all schemas
    GET    /api/v1/hetadb/schemas/{name}     — get schema detail
    DELETE /api/v1/hetadb/schemas/{name}     — delete a schema
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, field_validator

from hetadb.core.file_processor import ConfigManager
from hetadb.utils.schema import VALID_TYPES, schema_entities_to_str

router = APIRouter(prefix="/api/v1/hetadb/schemas", tags=["schemas"])
logger = logging.getLogger(__name__)

_workspace_root: Path | None = None


def _get_workspace_root() -> Path:
    global _workspace_root
    if _workspace_root is None:
        _workspace_root = ConfigManager().get_workspace_root()
    return _workspace_root


def _schemas_dir() -> Path:
    d = _get_workspace_root() / "schemas"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _schema_path(name: str) -> Path:
    return _schemas_dir() / f"{name}.json"


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class EntityDefinition(BaseModel):
    type: str = Field(..., description="Top-level entity type (must be one of the four system types)")
    subtype: str = Field(..., min_length=1, description="SubType name recognised by the KG extraction prompt")
    attributes: list[str] = Field(default_factory=list, description="Attribute names to extract for this SubType")

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        if v not in VALID_TYPES:
            raise ValueError(f"type must be one of {sorted(VALID_TYPES)}, got '{v}'")
        return v


class SchemaCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, pattern=r"^[A-Za-z0-9_\-]+$",
                      description="Schema name (alphanumeric, hyphens, underscores)")
    entities: list[EntityDefinition] = Field(..., min_length=1)


class ApiResponse(BaseModel):
    success: bool
    message: str
    data: Any = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("", response_model=ApiResponse, status_code=201)
async def create_schema(body: SchemaCreateRequest):
    """Create a new entity schema.

    Returns 409 if a schema with the same name already exists.
    """
    path = _schema_path(body.name)
    if path.exists():
        raise HTTPException(status_code=409, detail=f"Schema '{body.name}' already exists")

    payload = {
        "name": body.name,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "entities": [e.model_dump() for e in body.entities],
    }
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Failed to write schema: {e}")

    logger.info("Created schema '%s' (%d entities)", body.name, len(body.entities))
    return ApiResponse(
        success=True,
        message=f"Schema '{body.name}' created",
        data={"name": body.name, "entity_count": len(body.entities)},
    )


@router.get("")
async def list_schemas():
    """List all available schemas."""
    schemas_dir = _schemas_dir()
    schemas = []
    for path in sorted(schemas_dir.glob("*.json")):
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Skipping corrupted schema file %s: %s", path.name, e)
            continue
        schemas.append({
            "name": data.get("name", path.stem),
            "created_at": data.get("created_at"),
            "entity_count": len(data.get("entities", [])),
        })
    return {"success": True, "data": schemas}


@router.get("/{name}")
async def get_schema(name: str):
    """Get schema detail including all entity definitions and a preview of the prompt string."""
    path = _schema_path(name)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Schema '{name}' not found")

    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.error("Failed to read schema '%s': %s", name, e)
        raise HTTPException(status_code=500, detail=f"Schema file is corrupted: {e}")

    entities = data.get("entities", [])
    try:
        prompt_preview = schema_entities_to_str(entities)
    except Exception as e:
        logger.warning("Failed to generate prompt preview for schema '%s': %s", name, e)
        prompt_preview = None

    return {
        "success": True,
        "data": {
            **data,
            "prompt_preview": prompt_preview,
        },
    }


@router.delete("/{name}", response_model=ApiResponse)
async def delete_schema(name: str):
    """Delete a schema by name."""
    path = _schema_path(name)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Schema '{name}' not found")

    try:
        path.unlink()
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete schema: {e}")
    logger.info("Deleted schema '%s'", name)
    return ApiResponse(success=True, message=f"Schema '{name}' deleted")
