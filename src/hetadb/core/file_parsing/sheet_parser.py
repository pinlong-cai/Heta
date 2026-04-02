"""Spreadsheet parser (CSV/XLS/XLSX/ODS). Entry point: parse()"""

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd
from tqdm.asyncio import tqdm_asyncio

from hetadb.core.file_parsing.convert_to_unified import (
    MetaDict,
    TextElement,
    UnifiedDoc,
    _now_iso,
    load_hash_mapping,
)

logger = logging.getLogger("hetadb.file_parsing")


def parse_table_structure(input_path: Path) -> dict[str, Any]:
    """Load file into DataFrames keyed by sheet name."""
    suffix = input_path.suffix.lower()

    if suffix == ".csv":
        df = {"csv_sheet": pd.read_csv(input_path)}
    elif suffix in {".xls", ".xlsx", ".ods"}:
        df = pd.read_excel(input_path, sheet_name=None)
    else:
        raise ValueError(f"unsupported file type: {suffix}")

    return df


async def get_sheet_desc_async(
    ref_text: str, contents: str, name_text: str, llm_client: Any, max_retries: int = 3,
) -> tuple[str, str]:
    text_prompt = (
        "请给这个表格提供说明，根据表格列名和前三行内容推测图片标题，并给出表格内容的描述；\n"
        f"表格列名为：{ref_text}\n"
        f"前几行内容为：{contents}, 与列名依次对应\n"
        f"表格的文件名是{name_text}，需要甄别是否与表格内容有关\n"
        "要求语言简洁凝练\n"
        """严格按照下列JSON格式输出，不要其他任何解释：{"caption": "表格标题", "desc": "表格内容描述"}\n。"""
    )

    caption = ""
    desc = ""
    for attempt in range(max_retries):
        try:
            result = await llm_client(text_prompt)
            if not isinstance(result, str):
                continue
            start = result.find("{")
            end = result.rfind("}")
            # If the response contains no JSON object, retry rather than abort.
            if start == -1 or end == -1 or start >= end:
                continue
            result_dict = json.loads(result[start : end + 1])
            caption = result_dict.get("caption") or ""
            desc = result_dict.get("desc") or ""
            if caption and desc:
                return caption, desc
        except Exception:
            logger.warning(
                "Attempt %d/%d failed to get description for '%s'",
                attempt + 1, max_retries, name_text, exc_info=True,
            )

    if caption or desc:
        logger.warning(
            "Partial description for '%s': caption=%r desc=%r", name_text, caption, desc,
        )
    else:
        logger.warning(
            "All %d attempts failed for '%s'; description will be empty",
            max_retries, name_text,
        )
    return caption, desc


_TABLE_CHUNK_ROWS = 50  # max rows per text chunk for large tables


def _df_to_text_chunks(
    df: pd.DataFrame, caption: str, desc: str, chunk_rows: int = _TABLE_CHUNK_ROWS,
) -> list[str]:
    """Render a DataFrame as one or more Markdown-table text chunks.

    The first chunk always includes the caption/description header so retrieval
    can match on table metadata even when the relevant rows are in a later chunk.
    Large tables are split every *chunk_rows* rows to stay within embedding limits.
    """
    header = f"{caption}: {desc}" if desc else caption
    try:
        md_rows = df.to_markdown(index=False)
    except ImportError:
        # tabulate not installed — fall back to CSV-style text
        md_rows = df.to_csv(index=False)

    # Split the markdown body into row groups
    lines = md_rows.splitlines()
    # lines[0] = header row, lines[1] = separator, lines[2:] = data rows
    table_header = "\n".join(lines[:2]) if len(lines) >= 2 else md_rows
    data_lines = lines[2:] if len(lines) > 2 else []

    chunks: list[str] = []
    for i in range(0, max(len(data_lines), 1), chunk_rows):
        row_block = "\n".join(data_lines[i : i + chunk_rows])
        body = f"{table_header}\n{row_block}" if row_block else table_header
        prefix = header if i == 0 else f"{caption} (continued)"
        chunks.append(f"{prefix}\n\n{body}")

    return chunks or [header]


async def parse(
    file_list: list[Path],
    csv_out: Path,
    jsonls_dir: Path,
    dataset: str,
    mapping_json: Path,
    llm: Any,
) -> None:
    csv_out = Path(csv_out).expanduser().resolve()
    csv_out.mkdir(parents=True, exist_ok=True)
    if not jsonls_dir.exists():
        jsonls_dir.mkdir(parents=True, exist_ok=True)

    mapping_json = Path(mapping_json)
    filename_to_hash, hash_to_filename = load_hash_mapping(mapping_json)

    tasks = []
    df_data: dict[str, list[pd.DataFrame]] = {}
    for file in file_list:
        df = parse_table_structure(file)
        df_data[file.name] = []
        for sheet_name, df_sheet in df.items():
            columns_str = ",".join(
                [f"{col} ({str(df_sheet[col].dtype)})" for col in df_sheet.columns],
            )
            task = get_sheet_desc_async(
                ref_text=columns_str,
                contents=df_sheet.head(3).to_string(),
                name_text=hash_to_filename[file.name],
                llm_client=llm,
            )
            tasks.append(task)
            df_data[file.name].append(df_sheet)

    results = await tqdm_asyncio.gather(*tasks, desc="Generating table descriptions", total=len(tasks))

    for file in file_list:
        for idx, df_sheet in enumerate(df_data[file.name]):
            caption, desc = results.pop(0)

            csv_filename = "table_" + file.stem + "_page_" + str(idx) + ".csv"
            csv_path = csv_out / csv_filename
            df_sheet.to_csv(csv_path, index=False, encoding="utf-8")

            # Fall back to the CSV stem when LLM fails to produce a caption.
            effective_caption = caption or Path(csv_filename).stem

            # Render the full table as Markdown text chunks so the content is
            # indexed by the normal text pipeline instead of the SQL path.
            text_chunks = _df_to_text_chunks(df_sheet, effective_caption, desc)
            json_content: dict[str, list] = {}
            for chunk_idx, chunk_text in enumerate(text_chunks):
                json_content[f"page_{chunk_idx}"] = [
                    TextElement(id=f"text_{chunk_idx}", type="text", text=chunk_text)
                ]

            meta = MetaDict(
                source=hash_to_filename[file.name],
                hash_name=file.name,
                dataset=dataset,
                timestamp=_now_iso(),
                total_pages=len(text_chunks),
                file_type="text",
                description=f"{dataset}_{effective_caption}",
            )

            final_json = UnifiedDoc(meta=meta, json_content=json_content)
            json_filename = "table_" + file.stem + "_page_" + str(idx) + ".json"
            json_path = jsonls_dir / json_filename
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(final_json, f, ensure_ascii=False, indent=4)
