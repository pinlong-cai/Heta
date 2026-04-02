"""Plain text file parser. Entry point: parse()"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from hetadb.core.file_parsing.convert_to_unified import (
    MetaDict,
    TextElement,
    UnifiedDoc,
    _now_iso,
    load_hash_mapping,
    process_middle_files,
)

logger = logging.getLogger("hetadb.file_parsing")


def _parse_single(
    file: Path,
    output_dir: Path,
    dataset: str,
    hash_to_filename: dict,
) -> None:
    """Parse one text file and write its unified JSON to output_dir."""
    try:
        with open(file, encoding="utf-8") as f:
            content = f.read()
    except UnicodeDecodeError:
        try:
            with open(file) as f:
                content = f.read()
        except Exception as e:
            logger.error("Failed to read %s: %s", file, e)
            return

    meta = MetaDict(
        source=hash_to_filename[file.name],
        hash_name=file.name,
        dataset=dataset,
        timestamp=_now_iso(),
        total_pages=1,
        file_type="text",
        description="",
    )
    text_info = TextElement(id="merge_text_0", type="merge_text", text=content)
    final_json = UnifiedDoc(meta=meta, json_content={"page_0": [text_info]})

    output_json = output_dir / (file.stem + ".json")
    with open(output_json, "w", encoding="utf-8") as f:
        f.write(json.dumps(final_json, ensure_ascii=False, indent=4))


def parse(
    file_list: list[Path],
    jsonls_dir: Path,
    dataset: str,
    mapping_json: Path,
    max_workers: int = 8,
) -> None:
    text_parser_output = Path(jsonls_dir.parent / "text_parser_temp_output").expanduser().resolve()
    text_parser_output.mkdir(parents=True, exist_ok=True)

    _, hash_to_filename = load_hash_mapping(Path(mapping_json))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_parse_single, file, text_parser_output, dataset, hash_to_filename): file
            for file in file_list
        }
        for future in as_completed(futures):
            exc = future.exception()
            if exc:
                logger.error("text_parser failed for %s: %s", futures[future], exc)

    process_middle_files(text_parser_output, jsonls_dir)
