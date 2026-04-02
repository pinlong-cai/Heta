"""Copy files to an output directory with SHA-256 hashed names.

Hash seed = dataset + original stem + timestamp + 6-digit random number.
"""

import hashlib
import json
import logging
import random
import shutil
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("hetadb.utils")

HASH_LENGTH = 32


def get_sha256_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:HASH_LENGTH]


def rename_files_to_hash(
    dataset: str, files_to_process: list[Path], output_dir: Path,
) -> Path:
    """Copy each file into *output_dir* under a hashed name and save the mapping JSON."""
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    mapping = {}
    for file_path in files_to_process:
        original_name = file_path.name
        ext = file_path.suffix.lower()

        now_str = datetime.now().strftime("%Y%m%d%H%M%S")
        rand_6 = f"{random.randint(0, 999999):06d}"

        hash_seed = f"{dataset} {file_path.stem} {now_str} {rand_6}"
        hash_name = get_sha256_hash(hash_seed) + ext

        new_path = output_dir / hash_name
        try:
            shutil.copy2(file_path, new_path)
            mapping[original_name] = hash_name
        except Exception as e:
            logger.error("Error copying %s: %s", original_name, e)

    now_str = datetime.now().strftime("%Y%m%d%H%M%S")
    mapping_json_path = output_dir / f"hash_mapping_{now_str}.json"
    mapping_json_path.write_text(
        json.dumps(mapping, indent=4, ensure_ascii=False), encoding="utf-8",
    )
    logger.info("Mapping saved to %s", mapping_json_path)
    return mapping_json_path
