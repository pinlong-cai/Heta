"""Extract intermediate JSON into unified JSONL output with size-based rolling."""

from __future__ import annotations

import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO, TypedDict

logger = logging.getLogger("hetadb.file_parsing")

ROLL_SIZE = 3 * 1024**3


class MetaDict(TypedDict, total=False):
    source: str
    hash_name: str
    dataset: str
    timestamp: str
    total_pages: int
    file_type: str
    tag: list[str]
    description: str


class UnifiedDoc(TypedDict, total=True):
    meta: MetaDict
    json_content: dict[str, list]


class TextElement(TypedDict, total=False):
    id: str
    type: str
    text: str
    bbox: list[int]


class ImageElement(TypedDict, total=False):
    id: str
    type: str  # "image" | "table" | "interline_equation"
    url: str
    bbox: list[int]
    source: str
    hash_name: str
    caption: str
    desc: str


def load_hash_mapping(json_path: Path | str) -> tuple[dict[str, str], dict[str, str]]:
    with open(json_path, encoding="utf-8") as f:
        mapping: dict[str, str] = json.load(f)
    return mapping, {v: k for k, v in mapping.items()}


def _now_iso() -> str:
    return datetime.now().isoformat()


class JsonlRoller:
    """Write UnifiedDoc records to JSONL files with size-based rolling."""

    def __init__(self, out_dir: Path, prefix: str = "doc", roll_size: int = ROLL_SIZE):
        self.out_dir = Path(out_dir)
        self.prefix = prefix
        self.roll_size = roll_size
        self.timestamp = ""
        self._current_fp: TextIO | None = None
        self._current_path: Path | None = None
        self._open_next()

    def _open_next(self) -> None:
        if self._current_fp:
            self._current_fp.close()
            self._current_fp = None
        self.timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S_%f")
        fname = f"{self.prefix}_{self.timestamp}.jsonl"
        self._current_path = self.out_dir / fname
        self._current_path.parent.mkdir(parents=True, exist_ok=True)
        self._current_fp = open(self._current_path, "a", encoding="utf-8")
        logger.info("Opened JSONL -> %s", self._current_path)

    def write(self, obj: UnifiedDoc) -> None:
        line = json.dumps(obj, ensure_ascii=False) + "\n"
        assert self._current_path is not None
        assert self._current_fp is not None
        if self._current_path.stat().st_size + len(line.encode("utf-8")) > self.roll_size:
            self._open_next()
        self._current_fp.write(line)
        self._current_fp.flush()

    def close(self) -> None:
        if self._current_fp:
            self._current_fp.close()


def process_middle_files(temp_output: Path, jsonls_dir: Path) -> None:
    """Merge intermediate JSON files into rolled JSONL output and remove temp dir."""
    roller = JsonlRoller(jsonls_dir)
    middle_files = sorted(temp_output.rglob("*.json"))

    for middle_json in middle_files:
        if middle_json.stat().st_size == 0:
            continue
        try:
            content = json.loads(middle_json.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue

        unified = UnifiedDoc(meta=content["meta"], json_content=content["json_content"])
        if unified is None:
            continue
        roller.write(unified)
    roller.close()
    shutil.rmtree(temp_output)
