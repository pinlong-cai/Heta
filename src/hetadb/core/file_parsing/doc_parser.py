"""Document parser for PDF/DOC/DOCX/PPT/PPTX via MinerU. Entry point: batch_parse()"""

import io
import json
import logging
import re
import shutil
import subprocess
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Literal

from mineru.backend.pipeline.model_json_to_middle_json import result_to_middle_json
from mineru.backend.pipeline.pipeline_analyze import doc_analyze
from mineru.data.data_reader_writer import FileBasedDataWriter
from PIL import Image

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


def batch_parse(
    path_list: Sequence[str | Path],
    jsonls_dir: Path,
    image_dir: Path,
    dataset: str,
    mapping_json: Path,
    *,
    lang: Literal["zh", "en"] = "en",
    parse_method: str = "auto",
    formula_enable: bool = True,
    table_enable: bool = True,
    start_page_id: int = 0,
    end_page_id: int | None = None,
) -> None:
    mineru_output = jsonls_dir.parent / "mineru_output"
    mineru_output = Path(mineru_output).expanduser().resolve()
    mineru_output.mkdir(parents=True, exist_ok=True)

    mapping_json = Path(mapping_json)
    filename_to_hash, hash_to_filename = load_hash_mapping(mapping_json)

    pdf_bytes_list: list[bytes] = []
    names: list[str] = []
    for path in path_list:
        try:
            p = Path(path).expanduser().resolve()
            if not p.exists():
                raise FileNotFoundError(p)
            pdf_bytes_list.append(_to_pdf_bytes(p))
            names.append(p.stem)
            if p.stem + ".pdf" not in filename_to_hash:
                hash_to_filename[p.stem + ".pdf"] = hash_to_filename[p.name]
        except Exception:
            logger.warning("%s conversion failed", p.name)

    (
        infer_results,
        all_image_lists,
        all_pdf_docs,
        lang_list,
        ocr_enabled_list,
    ) = doc_analyze(
        pdf_bytes_list,
        [lang] * len(pdf_bytes_list),
        parse_method=parse_method,
        formula_enable=formula_enable,
        table_enable=table_enable,
    )

    # result_to_middle_json calls MinerU's table/formula rendering which uses
    # shared PyTorch model state — not thread-safe; must remain serial.
    converter = _MinerUConverter()
    for idx in range(len(infer_results)):
        try:
            sub_dir = mineru_output / names[idx]
            sub_dir.mkdir(exist_ok=True)
            img_dir = sub_dir / "images"
            img_dir.mkdir(exist_ok=True)

            image_writer = FileBasedDataWriter(str(img_dir))
            middle_json = result_to_middle_json(
                infer_results[idx],
                all_image_lists[idx],
                all_pdf_docs[idx],
                image_writer,
                lang_list[idx],
                ocr_enabled_list[idx],
                formula_enable,
            )

            meta = MetaDict(
                source=str(hash_to_filename[names[idx] + ".pdf"]),
                hash_name=names[idx] + ".pdf",
                dataset=str(dataset),
                timestamp=_now_iso(),
                total_pages=len(middle_json["pdf_info"]),
                file_type="pdf",
                description="",
            )

            json_content: dict[str, list[dict[str, Any]]] = {}
            for page_idx, page_info in enumerate(middle_json["pdf_info"]):
                json_content[f"page_{page_idx}"] = converter.convert_page(page_idx, page_info, img_dir)

            with open(mineru_output / f"{names[idx]}.json", "w", encoding="utf-8") as f:
                f.write(json.dumps(UnifiedDoc(meta=meta, json_content=json_content), ensure_ascii=False, indent=4))
            _copy_images(sub_dir / "images", image_dir)
        except Exception:
            logger.exception("doc post-processing failed for %s", names[idx])

    process_middle_files(mineru_output, jsonls_dir)


class _MinerUConverter:
    """Convert MinerU middle JSON pages to unified format."""

    def convert_page(
        self, page_idx: int, page_info: dict[str, Any], img_dir: Path,
    ) -> list[dict[str, Any]]:
        elements: list[Any] = []
        counters = {"text": 0, "image": 0, "table": 0, "interline_equation": 0}
        all_text_parts: list[str] = []

        for blk in page_info.get("para_blocks", []):
            if blk["type"] not in ("image", "table", "interline_equation"):
                text = self._extract_text(blk)
                if text:
                    elements.append(
                        TextElement(
                            id=f"text_{page_idx}_{counters['text']}",
                            type="text",
                            text=text,
                            bbox=self._fmt_bbox(blk.get("bbox", [])),
                        )
                    )
                    counters["text"] += 1
                    all_text_parts.append(text)

            if blk["type"] == "image":
                img_url, caption = self._collect_image(blk, img_dir)
                if img_url:
                    elements.append(
                        ImageElement(
                            id=f"image_{page_idx}_{counters['image']}",
                            type="image",
                            url=img_url,
                            bbox=self._fmt_bbox(blk.get("bbox", [])),
                            caption=caption,
                        )
                    )
                    counters["image"] += 1

            if blk["type"] == "table":
                tbl_url, caption = self._collect_table(blk, img_dir)
                if tbl_url:
                    elements.append(
                        ImageElement(
                            id=f"image_{page_idx}_{counters['table']}",
                            type="table",
                            url=tbl_url,
                            bbox=self._fmt_bbox(blk.get("bbox", [])),
                            caption=caption,
                        )
                    )
                    counters["table"] += 1

            if blk["type"] == "interline_equation":
                eq_url, caption = self._collect_equation(blk, img_dir)
                if eq_url:
                    elements.append(
                        ImageElement(
                            id=f"image_{page_idx}_{counters['interline_equation']}",
                            type="interline_equation",
                            url=eq_url,
                            bbox=self._fmt_bbox(blk.get("bbox", [])),
                            caption=caption,
                        )
                    )
                    counters["interline_equation"] += 1

        merge_text = re.sub(r"\s+", " ", " ".join(all_text_parts)).strip()
        if merge_text:
            elements.append(
                TextElement(
                    id=f"merge_text_{page_idx}",
                    type="merge_text",
                    text=merge_text,
                )
            )

        return elements

    @staticmethod
    def _collect_image(blk: dict[str, Any], img_dir: Path) -> tuple[str | None, str]:
        img_url = None
        caption_parts: list[str] = []
        for b in blk.get("blocks", []):
            if b["type"] == "image_body":
                p = _MinerUConverter._get_path(b, img_dir)
                if p.exists():
                    img_url = p.name
            elif b["type"] == "image_caption":
                caption_parts.append(_MinerUConverter._extract_text(b))
        return img_url, " ".join(caption_parts).strip()

    @staticmethod
    def _collect_table(blk: dict[str, Any], img_dir: Path) -> tuple[str | None, str]:
        tbl_url = None
        caption_parts: list[str] = []
        for b in blk.get("blocks", []):
            if b["type"] == "table_body":
                p = _MinerUConverter._get_path(b, img_dir)
                if p and p.exists():
                    tbl_url = p.name
            elif b["type"] == "table_caption":
                caption_parts.append(_MinerUConverter._extract_text(b))
        return tbl_url, " ".join(caption_parts).strip()

    @staticmethod
    def _collect_equation(blk: dict[str, Any], img_dir: Path) -> tuple[str | None, str]:
        eq_url = None
        caption = ""
        for line in blk.get("lines", []):
            for sp in line.get("spans", []):
                if sp.get("type") == "interline_equation":
                    img_path = (
                        img_dir / sp["image_path"] if "image_path" in sp else None
                    )
                    if img_path and img_path.exists():
                        eq_url = img_path.name
                    caption = sp.get("content", "")
        return eq_url, caption

    @staticmethod
    def _get_path(body: dict[str, Any], img_dir: Path) -> Path:
        for line in body.get("lines", []):
            for span in line.get("spans", []):
                if "image_path" in span:
                    return img_dir / str(span["image_path"])
        return Path()

    @staticmethod
    def _extract_text(blk: dict[str, Any]) -> str:
        spans: list[str] = []
        for line in blk.get("lines", []):
            for sp in line.get("spans", []):
                if sp.get("type") in ("text", "inline_equation") and "content" in sp:
                    spans.append(sp["content"])
        return " ".join(spans).strip()

    @staticmethod
    def _fmt_bbox(bbox: list[Any]) -> list[int]:
        if not bbox:
            return []
        return [int(round(float(x))) for x in bbox]


# --- File format converters ---

def _office_to_pdf_bytes(file_path: str | Path) -> bytes:
    """Convert doc/docx/ppt/pptx to PDF bytes via LibreOffice."""
    file_path = Path(file_path).expanduser().resolve()
    suffix = file_path.suffix.lower()
    if suffix not in {".doc", ".docx", ".ppt", ".pptx"}:
        raise ValueError(f"unsupported office format: {suffix}")

    with tempfile.TemporaryDirectory(dir=file_path.parent) as tmp_dir:
        cmd = [
            "libreoffice", "--headless", "--convert-to", "pdf",
            "--outdir", tmp_dir, str(file_path),
        ]
        try:
            subprocess.run(cmd, check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            logger.error(e.stderr.decode())
            raise RuntimeError("libreoffice convert failed") from e

        pdf_files = list(Path(tmp_dir).glob("*.pdf"))
        if not pdf_files:
            raise RuntimeError("libreoffice did not produce pdf")
        return pdf_files[0].read_bytes()


def _image_to_pdf_bytes(file_path: str | Path) -> bytes:
    """Wrap a single image into a one-page PDF."""
    img = Image.open(file_path).convert("RGB")
    buf = io.BytesIO()
    img.save(buf, format="pdf")
    return buf.getvalue()


_CONVERTERS = {
    ".jpg": _image_to_pdf_bytes,
    ".jpeg": _image_to_pdf_bytes,
    ".png": _image_to_pdf_bytes,
    ".pdf": lambda p: Path(p).read_bytes(),
    ".doc": _office_to_pdf_bytes,
    ".docx": _office_to_pdf_bytes,
    ".ppt": _office_to_pdf_bytes,
    ".pptx": _office_to_pdf_bytes,
}


def _to_pdf_bytes(file_path: str | Path) -> bytes:
    suffix = Path(file_path).suffix.lower()
    if suffix not in _CONVERTERS:
        raise ValueError(f"unsupported file type: {suffix}")
    return _CONVERTERS[suffix](file_path)


def _copy_images(src_dir: Path, dest_dir: Path) -> None:
    if not src_dir.exists():
        logger.warning("Source images dir not found: %s", src_dir)
        return
    dest_dir.mkdir(parents=True, exist_ok=True)
    for item in src_dir.iterdir():
        if item.is_file():
            shutil.copy2(item, dest_dir)
