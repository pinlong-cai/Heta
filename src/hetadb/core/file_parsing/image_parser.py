"""Image description generator via VLM. Entry point: create_description()"""

import base64
import imghdr
import json
import logging
import re
import shutil
from io import BytesIO
from pathlib import Path
from typing import Any

from PIL import Image
from tqdm.asyncio import tqdm_asyncio

from hetadb.core.file_parsing.convert_to_unified import (
    ImageElement,
    MetaDict,
    UnifiedDoc,
    _now_iso,
    load_hash_mapping,
    process_middle_files,
)

logger = logging.getLogger("hetadb.file_parsing")
logging.getLogger("httpx").setLevel(logging.WARNING)


def is_valid_image(image_data) -> tuple[bool, str]:
    try:
        if not image_data:
            return False, "empty image data"
        image = Image.open(BytesIO(image_data))
        image.verify()
        return True, ""
    except Exception as e:
        return False, str(e)


def get_image_mime(image_data):
    img_type = imghdr.what(None, image_data)
    mime_map = {
        "jpeg": "image/jpeg",
        "jpg": "image/jpeg",
        "png": "image/png",
        "gif": "image/gif",
        "bmp": "image/bmp",
        "webp": "image/webp",
        "tiff": "image/tiff",
    }
    return mime_map.get(img_type)


async def get_image_desc_async(
    image_path: Path, ref_text: str, caption_text: str, vlm_client: Any, max_retries: int = 3,
) -> tuple:
    with open(image_path, "rb") as f:
        image_data = f.read()
    valid, error_msg = is_valid_image(image_data)
    if not valid:
        return "", ""

    # Composite images with an alpha channel onto white before encoding —
    # VLM APIs render transparency as black, making dark text invisible.
    img = Image.open(BytesIO(image_data))
    if img.mode in ("RGBA", "LA", "PA"):
        white = Image.new("RGB", img.size, (255, 255, 255))
        white.paste(img, mask=img.split()[-1])
        buf = BytesIO()
        white.save(buf, format="PNG")
        image_data = buf.getvalue()

    mime_type = get_image_mime(image_data) or "image/jpeg"
    base64_str = base64.b64encode(image_data).decode("utf-8")

    caption_hint = f"已知图片标题/caption为：{caption_text}\n" if caption_text else ""
    ref_hint = f"参考上下文：{ref_text}\n" if ref_text else ""
    text_prompt = (
        "You are an expert document and image analyst. "
        "Your task is to extract ALL information from this image with maximum accuracy.\n\n"
        "CRITICAL RULES:\n"
        "- Extract text VERBATIM. Do NOT translate, paraphrase, or reformat.\n"
        "- Do NOT hallucinate or infer values. Mark unreadable text as [unreadable].\n"
        "- Preserve original language (Chinese, English, or mixed).\n\n"
        "STEP 1 — Classify the image as one of:\n"
        "chart | table | diagram | document | photograph | mixed | other\n\n"
        "STEP 2 — Extract based on type:\n"
        "• chart: title, axis labels, ALL tick values, ALL legend entries with colors, "
        "every data label/annotation, source/footnote. "
        "Preserve exact numeric formats (e.g. '1.2M', '92.3%'). "
        "Then describe chart type, key trends, and main conclusion.\n"
        "• table: all headers + every cell value row by row. Key insights.\n"
        "• diagram/flowchart: all node labels, edge labels, flow description.\n"
        "• document/screenshot: transcribe ALL readable text in reading order. "
        "Describe document type.\n"
        "• photograph: describe subjects, actions, setting, any visible text. "
        "Focus on information-relevant content.\n\n"
        f"{caption_hint}"
        f"{ref_hint}"
        "Output ONLY valid JSON, no markdown, no explanation:\n"
        '{"caption": "<title text visible in image, empty string if none>", '
        '"image_type": "<type from Step 1>", '
        '"desc": "<complete extraction and description from Step 2>"}'
    )

    caption, desc = "", ""
    for attempt in range(max_retries):
        try:
            result = await vlm_client(text_prompt, base64_str, mime_type)
            if not result:
                continue
            start = result.find("{")
            end = result.rfind("}")
            if start == -1 or end == -1 or start >= end:
                logger.warning("VLM non-JSON response (attempt %d/%d): %.120s", attempt + 1, max_retries, result)
                continue
            result_dict = json.loads(result[start : end + 1])
            caption = result_dict.get("caption", "")
            desc = result_dict.get("desc", "")
            if desc:
                break
        except json.JSONDecodeError as e:
            logger.warning("VLM JSON parse error (attempt %d/%d): %s", attempt + 1, max_retries, e)
        except Exception as e:
            logger.error("VLM call failed (attempt %d/%d): %s", attempt + 1, max_retries, e)
    return caption, desc


async def process_image_in_jsonl_lines(
    file_key, image_parsed_set, jsonl_images_out, image_parser_output, vlm,
):
    """Process all lines of a single JSONL file, return valid image count and updated parsed set."""
    tasks = []
    task_metadata = []
    file_line_results = {}
    valid_image_count = 0

    with open(file_key, encoding="utf-8") as _fh:
        lines = [line.strip() for line in _fh.readlines()]

    for line_index, json_line in enumerate(lines):
        try:
            data = json.loads(json_line)
        except Exception as e:
            logger.error("Cannot parse %s line %d: %s", file_key, line_index, e)
            continue

        if "json_content" not in data:
            continue

        original_meta = data.get("meta", {})

        page_keys = sorted(
            (k for k in data["json_content"].keys() if k.startswith("page_")),
            key=lambda k: int(re.search(r"\d+$", k).group()),
        )

        page_texts = {}
        images_in_line = []

        for page_key in page_keys:
            page_list = data["json_content"][page_key]
            if not page_list:
                continue
            if page_list[-1].get("type") == "merge_text":
                page_texts[page_key] = page_list[-1]["text"]
            images_in_line.extend(
                item for item in page_list if item.get("type") == "image"
            )

        file_line_key = (file_key, line_index)
        file_line_results[file_line_key] = {
            "meta": original_meta,
            "original_json_content": data.get("json_content", {}),
            "processed_items": [],
        }

        for image_item in images_in_line:
            cnt = int(image_item["id"].split("_")[1])
            text = " ".join([page_texts.get(f"page_{cnt + i}", "") for i in [-1, 0, 1]])
            if "meta" in data and "description" in data["meta"]:
                text = data["meta"]["description"] + " " + text
            image_item["desc"] = text

            if "web_url" in image_item:
                image_key = image_item["web_url"]
            elif "url" in image_item:
                image_key = image_item["url"]

            if image_key not in image_parsed_set:
                image_parsed_set.add(image_key)

                image_path = jsonl_images_out / image_key
                if image_path.exists():
                    ref_text = image_item["desc"]
                    caption = image_item.get("caption", "")
                    task = get_image_desc_async(image_path, ref_text, caption, vlm)
                    tasks.append(task)
                    task_metadata.append(
                        {"file_line_key": file_line_key, "image_item": image_item},
                    )

    if not tasks:
        return 0, image_parsed_set

    results = await tqdm_asyncio.gather(*tasks, desc="Processing JSONL images", total=len(tasks))

    for meta, result in zip(task_metadata, results):
        file_line_key = meta["file_line_key"]
        image_item = meta["image_item"]
        image_item["caption"], image_item["desc"] = result

        if image_item["desc"].strip() or image_item["caption"].strip():
            valid_image_count += 1

        file_line_results[file_line_key]["processed_items"].append(image_item)

    file_outputs = {}
    for (key, line_index), result_data in file_line_results.items():
        if key not in file_outputs:
            file_outputs[key] = {}
        file_outputs[key][line_index] = result_data

    for key, line_results in file_outputs.items():
        for line_index in sorted(line_results.keys()):
            result_data = line_results[line_index]
            for image_item in result_data["processed_items"]:
                original_meta = result_data["meta"]
                meta = MetaDict(
                    source=original_meta["source"],
                    hash_name=image_item["url"],
                    dataset=original_meta.get("dataset", ""),
                    timestamp=_now_iso(),
                    total_pages=1,
                    file_type="image",
                    description=image_item["caption"] + ": " + image_item["desc"],
                )
                image_info = ImageElement(
                    id="image_0_0",
                    type="image",
                    source=original_meta["source"],
                    hash_name=image_item["url"],
                    caption=image_item["caption"],
                    desc=image_item["desc"],
                )
                final_json = UnifiedDoc(
                    meta=meta, json_content={"page_0": [image_info]},
                )
                image_name = image_item["url"].split(".")[0]
                filename = f"{image_name}.json"
                output_json = image_parser_output / filename
                with open(output_json, "w", encoding="utf-8") as f:
                    f.write(json.dumps(final_json, ensure_ascii=False, indent=4))

    return valid_image_count, image_parsed_set


async def create_description(
    file_list: list[Path],
    jsonl_images_dir: Path,
    jsonl_path_list: Path,
    image_desc_dir: Path,
    dataset: str,
    mapping_json: Path,
    vlm: Any,
):
    image_parser_output = image_desc_dir.parent / "image_parser_temp_output"
    image_parser_output = Path(image_parser_output).expanduser().resolve()
    image_parser_output.mkdir(parents=True, exist_ok=True)

    mapping_json = Path(mapping_json)
    filename_to_hash, hash_to_filename = load_hash_mapping(mapping_json)

    image_desc_dir.mkdir(parents=True, exist_ok=True)

    image_parsed_set: set[str] = set()

    # Phase 1: images embedded in JSONL files from prior parsers
    file_keys = [
        file
        for file in jsonl_path_list.rglob("*")
        if file.suffix.lower().endswith(".jsonl")
    ]

    global_valid_count = 0

    for file_key in file_keys:
        valid_image_count, image_parsed_set = await process_image_in_jsonl_lines(
            file_key, image_parsed_set, jsonl_images_dir, image_parser_output, vlm,
        )
        global_valid_count += valid_image_count

    # Phase 2: raw image files
    tasks = []
    task_metadata = []
    for image_path in file_list:
        task = get_image_desc_async(
            image_path, ref_text="", caption_text="", vlm_client=vlm,
        )
        tasks.append(task)
        task_metadata.append(
            {"source": hash_to_filename[image_path.name], "hash_name": image_path.name},
        )

    results = await tqdm_asyncio.gather(*tasks, desc="Processing raw images", total=len(tasks))
    for image_item, result in zip(task_metadata, results):
        image_item["caption"], image_item["desc"] = result

        if not image_item["desc"].strip():
            logger.warning("Empty VLM description for %s, skipping indexing", image_item["hash_name"])
            continue

        meta = MetaDict(
            source=image_item["source"],
            hash_name=image_item["hash_name"],
            dataset=dataset,
            timestamp=_now_iso(),
            total_pages=1,
            file_type="image",
            description=image_item["caption"] + ": " + image_item["desc"],
        )
        image_info = ImageElement(
            id="image_0_0",
            type="image",
            source="image file",
            hash_name=image_item["hash_name"],
            caption=image_item["caption"],
            desc=image_item["desc"],
        )
        final_json = UnifiedDoc(meta=meta, json_content={"page_0": [image_info]})
        image_name = image_item["hash_name"].split(".")[0]
        filename = f"{image_name}.json"
        output_json = image_parser_output / filename
        with open(output_json, "w", encoding="utf-8") as f:
            f.write(json.dumps(final_json, ensure_ascii=False, indent=4))

        image_parsed_set.add(image_path.name)
        global_valid_count += 1

    # Merge intermediate files to both output directories
    temp_backup = image_parser_output.parent / "image_parser_temp_backup"
    if temp_backup.exists():
        shutil.rmtree(temp_backup)
    shutil.copytree(image_parser_output, temp_backup)

    process_middle_files(image_parser_output, image_desc_dir)
    process_middle_files(temp_backup, jsonl_path_list)

    logger.info("Total valid images: %d", global_valid_count)
