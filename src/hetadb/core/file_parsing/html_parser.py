"""HTML file parser. Entry point: parse()"""

import hashlib
import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from hetadb.core.file_parsing.convert_to_unified import (
    ImageElement,
    MetaDict,
    TextElement,
    UnifiedDoc,
    _now_iso,
    load_hash_mapping,
    process_middle_files,
)

logger = logging.getLogger("hetadb.file_parsing")


def sha256_hash(url):
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def _parse_single(
    file: Path,
    source_url: str,
    image_hashs: set[str],
    output_dir: Path,
    dataset: str,
    hash_to_filename: dict,
) -> None:
    """Parse one HTML file and write its unified JSON to output_dir."""
    with open(file, encoding="utf-8") as f:
        html_content = f.read()

    extracted_data = convert_single(html_content, source_url, image_hashs)
    title = extracted_data.get("title", "").strip()
    desc = extracted_data.get("description", "").strip()
    description = f"{title},{desc}" if title and desc else title

    meta = MetaDict(
        source=hash_to_filename[file.name],
        hash_name=file.name,
        dataset=dataset,
        timestamp=_now_iso(),
        total_pages=1,
        file_type="html",
        description=description,
    )
    final_json = UnifiedDoc(meta=meta, json_content=extracted_data.get("content", {}))

    output_json = output_dir / (file.stem + ".json")
    with open(output_json, "w", encoding="utf-8") as f:
        f.write(json.dumps(final_json, ensure_ascii=False, indent=4))


def parse(
    file_list: list[Path],
    jsonls_dir: Path,
    dataset: str,
    mapping_json: Path,
    url_list: list[str] | None = None,
    image_hashs: set[str] | None = None,
    max_workers: int = 8,
) -> None:
    # Pair files with URLs upfront to preserve order before parallelising.
    urls = list(url_list) if url_list else []
    image_hashs = image_hashs or set()
    pairs = [(file, urls[i] if i < len(urls) else "") for i, file in enumerate(file_list)]

    html_parser_output = Path(jsonls_dir.parent / "html_parser_output").expanduser().resolve()
    html_parser_output.mkdir(parents=True, exist_ok=True)

    _, hash_to_filename = load_hash_mapping(Path(mapping_json))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_parse_single, file, url, image_hashs, html_parser_output, dataset, hash_to_filename): file
            for file, url in pairs
        }
        for future in as_completed(futures):
            exc = future.exception()
            if exc:
                logger.error("html_parser failed for %s: %s", futures[future], exc)

    process_middle_files(html_parser_output, jsonls_dir)


def _init_result() -> dict:
    return {
        "content": {"page_0": []},
        "image_urls": [],
        "table_texts": [],
        "title": "",
        "description": "",
    }


def convert_single(
    html_content: str, source_url: str, image_hashs: set[str], is_url_hash: bool = True,
) -> dict:
    result = _init_result()
    if not html_content.strip():
        return result

    try:
        soup = BeautifulSoup(html_content, "html.parser")
        _extract_metadata(soup, result)
        original_images = _extract_all_image_candidates(soup, source_url)
        _remove_noise_tags(soup)

        root_element = soup.find("body") or soup
        conversion_result = _traverse_dom(
            root_element, source_url, image_hashs, original_images, is_url_hash,
        )

        result["content"]["page_0"] = conversion_result["page_elements"]
        result["image_urls"] = conversion_result["image_urls"]
        result["table_texts"] = conversion_result["table_texts"]

        merge_text = _build_merge_text(conversion_result["all_text_parts"])
        result["content"]["page_0"].append(
            TextElement(id="merge_text_0", type="merge_text", text=merge_text),
        )

        return result

    except Exception as e:
        logger.error("Failed to convert HTML: %s", e)
        return result


def _extract_metadata(soup: BeautifulSoup, result: dict):
    result["title"] = soup.title.get_text(strip=True) if soup.title else ""

    desc_tag = soup.find("meta", attrs={"name": "description"})
    og_desc_tag = soup.find("meta", attrs={"property": "og:description"})
    result["description"] = (
        desc_tag.get("content", "").strip()  # type: ignore[union-attr]
        if desc_tag
        else (
            og_desc_tag.get("content", "").strip()  # type: ignore[union-attr]
            if og_desc_tag
            else ""
        )
    )


def _extract_all_image_candidates(soup: BeautifulSoup, source_url: str) -> list[dict]:
    candidates = []

    def get_img_url(img):
        for c in [
            img.get("data-src"),
            img.get("data-original"),
            img.get("data-lazy-src"),
            img.get("src"),
            (
                img.get("srcset", "").split(",")[-1].strip().split()[0]
                if img.get("srcset")
                else None
            ),
        ]:
            if c and isinstance(c, str) and c.strip():
                return c.strip()
        return ""

    for img in soup.find_all("img"):
        src = get_img_url(img)
        if not src or ".svg" in src.lower():
            continue
        full_url = urljoin(source_url, src)
        alt = (img.get("alt") or "").strip()  # type: ignore[union-attr]
        title = (img.get("title") or "").strip()  # type: ignore[union-attr]
        candidates.append({"url": full_url, "alt": alt, "title": title})

    for source in soup.find_all("source", srcset=True):
        parent = source.find_parent("picture")
        fallback_img = parent.find("img") if parent else None
        for part in source["srcset"].split(","):  # type: ignore[union-attr]
            url_part = part.strip().split()[0]
            if not url_part or ".svg" in url_part.lower():
                continue
            full_url = urljoin(source_url, url_part)
            alt = (fallback_img.get("alt") or "").strip() if fallback_img else ""  # type: ignore[union-attr]
            title = (fallback_img.get("title") or "").strip() if fallback_img else ""  # type: ignore[union-attr]
            candidates.append({"url": full_url, "alt": alt, "title": title})

    for prop in ["og:image", "twitter:image"]:
        tag = soup.find("meta", property=prop) or soup.find(
            "meta", attrs={"name": prop},
        )
        if tag and tag.get("content"):
            img_url = urljoin(source_url, tag["content"])  # type: ignore[type-var]
            if img_url and ".svg" not in img_url.lower():  # type: ignore[attr-defined]
                candidates.append(
                    {"url": img_url, "alt": f"social: {prop}", "title": ""},
                )

    for video in soup.find_all("video", poster=True):
        poster_url = urljoin(source_url, video["poster"])  # type: ignore[type-var]
        if poster_url and ".svg" not in poster_url.lower():  # type: ignore[attr-defined]
            candidates.append({"url": poster_url, "alt": "video poster", "title": ""})

    seen = set()
    unique_candidates = []
    for img in candidates:  # type: ignore[assignment]
        if img["url"] not in seen:
            seen.add(img["url"])
            unique_candidates.append(img)
    return unique_candidates  # type: ignore[return-value]


def _remove_noise_tags(soup: BeautifulSoup):
    for tag in soup.find_all(
        ["script", "style", "footer", "nav", "aside", "iframe", "button"],
    ):
        tag.decompose()


def _traverse_dom(
    root_element, source_url: str, image_hashs: set[str],
    original_images: list[dict], is_url_hash: bool,
):
    page_elements = []
    all_text_parts = []
    image_urls = []
    table_texts = []
    counters = {"text": 0, "image": 0, "table": 0}
    processed_image_urls = set()

    def is_in_whitelist(url: str) -> bool:
        return url in image_hashs or len(image_hashs) == 0

    def process_node(node):
        nonlocal \
            counters, \
            page_elements, \
            all_text_parts, \
            image_urls, \
            table_texts, \
            processed_image_urls

        if not node.name and isinstance(node, str):
            text = node.replace("\\n", "").replace("\\t", "").strip()
            if text:
                elem_id = f"text_0_{counters['text']}"
                counters["text"] += 1
                page_elements.append(
                    TextElement(id=elem_id, type="text", text=text),
                )
                all_text_parts.append(text)
            return

        if node.name and node.get("style") and "url(" in node["style"]:
            for bg_url_raw in re.findall(r'url\([\'"]?(.*?)[\'"]?\)', node["style"]):
                bg_url = urljoin(source_url, bg_url_raw.strip())
                img_hash = sha256_hash(bg_url) if is_url_hash else bg_url
                if (
                    not bg_url
                    or ".svg" in bg_url.lower()
                    or bg_url in processed_image_urls
                    or not is_in_whitelist(img_hash)
                ):
                    continue
                elem_id = f"image_0_{counters['image']}"
                counters["image"] += 1
                caption = "background-image"
                page_elements.append(
                    ImageElement(id=elem_id, type="image", url=img_hash, caption=caption),
                )
                image_urls.append(bg_url)
                processed_image_urls.add(bg_url)

        if node.name == "video" and node.get("poster"):
            poster_url = urljoin(source_url, node["poster"])
            img_hash = sha256_hash(poster_url) if is_url_hash else poster_url
            if (
                poster_url
                and ".svg" not in poster_url.lower()
                and poster_url not in processed_image_urls
                and is_in_whitelist(img_hash)
            ):
                elem_id = f"image_0_{counters['image']}"
                counters["image"] += 1
                caption = "video poster"
                page_elements.append(
                    ImageElement(id=elem_id, type="image", url=img_hash, caption=caption),
                )
                image_urls.append(poster_url)
                processed_image_urls.add(poster_url)

        if node.name == "source" and node.get("srcset"):
            for part in node["srcset"].split(","):
                url = part.strip().split()[0]
                if not url or ".svg" in url.lower():
                    continue
                full_url = urljoin(source_url, url)
                img_hash = sha256_hash(full_url) if is_url_hash else full_url
                if full_url not in processed_image_urls and is_in_whitelist(img_hash):
                    caption = f"source: {url.split('.')[-1].upper()}"
                    elem_id = f"image_0_{counters['image']}"
                    counters["image"] += 1
                    page_elements.append(
                        ImageElement(id=elem_id, type="image", url=img_hash, caption=caption),
                    )
                    all_text_parts.append(caption)
                    image_urls.append(full_url)
                    processed_image_urls.add(full_url)
            return

        if node.name == "img":
            src = _get_img_url(node)
            if not src or ".svg" in src.lower():
                return
            full_src = urljoin(source_url, src)
            img_hash = sha256_hash(full_src) if is_url_hash else full_src
            if full_src not in processed_image_urls and is_in_whitelist(img_hash):
                caption = (node.get("alt") or node.get("title") or "").strip()
                elem_id = f"image_0_{counters['image']}"
                counters["image"] += 1
                if caption:
                    all_text_parts.append(caption)
                page_elements.append(
                    ImageElement(id=elem_id, type="image", url=img_hash, caption=caption),
                )
                image_urls.append(full_src)
                processed_image_urls.add(full_src)
            return

        if node.name == "table":
            table_text = node.get_text(separator=" ", strip=True)
            if not table_text:
                return
            elem_id = f"table_0_{counters['table']}"
            counters["table"] += 1
            caption_tag = node.find("caption")
            caption_text = caption_tag.get_text(strip=True) if caption_tag else ""
            elem_dict = {"id": elem_id, "type": "table", "text": table_text}
            if caption_text:
                all_text_parts.append(caption_text)
            all_text_parts.append(table_text)
            page_elements.append(
                ImageElement(
                    id=elem_id, type="table", text=table_text, caption=caption_text,
                ),
            )
            table_texts.append(elem_dict)
            return

        if node.name:
            for child in node.children:
                process_node(child)

    for child in root_element.children:
        process_node(child)

    # Recover images that were not encountered during DOM traversal
    lost_images = [
        img
        for img in original_images
        if img["url"] not in processed_image_urls
        and is_in_whitelist(sha256_hash(img["url"]))
    ]
    for img_info in reversed(lost_images):
        url = img_info["url"]
        hash_url = sha256_hash(url) if is_url_hash else url
        caption = img_info["alt"] or img_info["title"]
        elem_id = f"image_0_{counters['image']}"
        counters["image"] += 1
        if caption:
            all_text_parts.append(caption)
        page_elements.insert(
            0,
            ImageElement(id=elem_id, type="image", url=hash_url, caption=caption),
        )
        image_urls.append(url)
        processed_image_urls.add(url)

    return {
        "page_elements": page_elements,
        "image_urls": image_urls,
        "table_texts": table_texts,
        "all_text_parts": all_text_parts,
    }


def _get_img_url(img) -> Any:
    for c in [
        img.get("data-src"),
        img.get("data-original"),
        img.get("data-lazy-src"),
        img.get("src"),
        (
            img.get("srcset", "").split(",")[-1].strip().split()[0]
            if img.get("srcset")
            else None
        ),
    ]:
        if c and isinstance(c, str) and c.strip():
            return c.strip()
    return None


def _build_merge_text(all_text_parts: list[str]) -> str:
    return (
        re.sub(r"\s+", " ", " ".join(all_text_parts))
        .replace("\\n", "")
        .replace("\\t", "")
        .strip()
    )
