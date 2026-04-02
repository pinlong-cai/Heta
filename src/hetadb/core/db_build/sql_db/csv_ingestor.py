"""CSV ingestion into PostgreSQL with LLM-generated table descriptions.

Reads CSV files, auto-creates tables, inserts data, and generates
KG table-node metadata via LLM.
"""

import json
import logging
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


def normalize_identifier(name: str) -> str:
    """Normalize a string for use as a PostgreSQL quoted identifier.

    Preserves Unicode (e.g. CJK characters) since PostgreSQL supports them
    inside double-quoted identifiers.  Returns ``"_"`` for empty input.
    """
    if not isinstance(name, str):
        name = str(name)
    stripped = name.strip()
    return stripped if stripped else "_"


class AutoSchemaCSVIngestor:
    """Ingest CSV files into PostgreSQL and generate KG table nodes."""

    def __init__(
        self,
        csv_dir: str,
        table_desc_dir: str,
        table_info_dir: str,
        kg_node_dir: str,
        postgres_config: dict[str, Any],
        use_llm: Callable[..., str] | None = None,
    ):
        self.csv_dir = Path(csv_dir)
        self.desc_dir = Path(table_desc_dir)
        self.info_dir = Path(table_info_dir)
        self.kg_node_dir = Path(kg_node_dir)
        self.info_dir.mkdir(parents=True, exist_ok=True)
        self.pg_config = postgres_config
        self.llm = use_llm

    def get_conn(self):
        """Open a new PostgreSQL connection."""
        return psycopg2.connect(**self.pg_config)

    def create_table_from_csv(self, csv_path: Path, csv_caption: str) -> str:
        """Read CSV header and auto-create a PostgreSQL table (all TEXT columns).

        Returns:
            The normalized table name.
        """
        table_name = normalize_identifier(csv_caption)

        df_sample = pd.read_csv(csv_path, nrows=0, dtype=str)
        columns = [normalize_identifier(col) for col in df_sample.columns]

        col_defs = ",\n    ".join(f'"{col}" TEXT' for col in columns)
        create_sql = (
            f'CREATE TABLE IF NOT EXISTS public."{table_name}" (\n    {col_defs}\n);'
        )

        conn = self.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(create_sql)
                conn.commit()
            logger.info(
                "Created table public.\"%s\" (%d columns)", table_name, len(columns),
            )
        finally:
            conn.close()

        return table_name

    def load_csv_and_insert(self, csv_path: Path, table_name: str) -> None:
        """Read the full CSV and bulk-insert into PostgreSQL."""
        df = pd.read_csv(csv_path, dtype=str, on_bad_lines="skip")
        df.columns = [normalize_identifier(col) for col in df.columns]
        df = df.fillna("")

        conn = self.get_conn()
        try:
            with conn.cursor() as cur:
                cols = list(df.columns)
                col_str = ", ".join(f'"{c}"' for c in cols)
                sql = f'INSERT INTO public."{table_name}" ({col_str}) VALUES %s'
                vals = [tuple(row) for row in df.values]
                execute_values(cur, sql, vals, page_size=1000)
                conn.commit()
            logger.info("Inserted %d rows into %s", len(df), table_name)
        finally:
            conn.close()

    def load_table_description(self, table_name: str) -> dict[str, Any]:
        """Load a previously saved table description JSON."""
        desc_path = self.desc_dir / f"{table_name}.json"
        if not desc_path.exists():
            raise FileNotFoundError(
                f"Description file for table '{table_name}' not found: {desc_path}"
            )
        with open(desc_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def generate_table_description(
        self, csv_path: Path, table_name: str,
    ) -> dict[str, Any]:
        """Call LLM to generate a table description from sample CSV rows."""
        df_sample = pd.read_csv(csv_path, nrows=5, dtype=str, on_bad_lines="skip")
        sample_json = df_sample.head(3).to_dict(orient="records")

        columns = [normalize_identifier(col) for col in df_sample.columns]
        table_structure = ", ".join(f'"{col}"' for col in columns)

        prompt = (
            f"You are a database documentation engineer. Based on the following CSV "
            f"sample data, generate a description for a PostgreSQL table.\n"
            f"Table name: {table_name}\n"
            f"Columns: {table_structure}\n"
            f"First 3 sample rows: {json.dumps(sample_json, ensure_ascii=False)}\n\n"
            f"Output a JSON object with these fields:\n"
            f'- "table_purpose": one-sentence summary of the table\'s purpose\n'
            f'- "field_descriptions": object mapping each field name to a brief description\n'
            f'- "example_queries": array of 3 representative SELECT queries (double-quoted identifiers, semicolon-terminated)\n'
            f"Output ONLY the raw JSON string, no markdown or extra text."
        )

        resp = self.llm(prompt)
        try:
            desc = json.loads(resp)
        except (json.JSONDecodeError, ValueError) as exc:
            raise RuntimeError(f"Failed to parse LLM response: {resp}") from exc

        # Attach per-column sample values extracted directly from the CSV.
        # These are stored verbatim so the SQL generator can see the actual
        # data format (e.g. "540B", "1.6T") and avoid incorrect type casts.
        normalized_cols = [normalize_identifier(c) for c in df_sample.columns]
        df_sample.columns = normalized_cols
        field_samples: dict[str, list[str]] = {}
        for col in normalized_cols:
            samples = df_sample[col].dropna().unique()[:5].tolist()
            field_samples[col] = [str(v) for v in samples]
        desc["field_samples"] = field_samples

        info_path = self.info_dir / f"{table_name}.json"
        with open(info_path, "w", encoding="utf-8") as f:
            json.dump(desc, f, ensure_ascii=False, indent=2)

        return desc

    def process_csv(self, csv_path: Path, csv_desc: str) -> None:
        """Full pipeline for one CSV: create table -> insert -> generate description."""
        logger.info("Processing CSV: %s", csv_path)

        # Check processing record to avoid re-processing
        record_path = self.info_dir / "record.txt"
        if record_path.exists():
            with open(record_path, "r", encoding="utf-8") as f:
                processed = {line.strip() for line in f if line.strip()}
            if csv_path.stem in processed:
                logger.info("CSV %s already processed, skipping", csv_path.name)
                return

        table_name = self.create_table_from_csv(csv_path, csv_desc)
        self.load_csv_and_insert(csv_path, table_name)
        desc = self.generate_table_description(csv_path, table_name)

        # Append table node to KG node file
        self.kg_node_dir.mkdir(parents=True, exist_ok=True)
        node_path = self.kg_node_dir / "table_node.jsonl"
        node_record = {
            "NodeName": table_name,
            "Type": "table",
            "SubType": "data_table",
            "Description": f"{desc['table_purpose']}|{desc['field_descriptions']}",
            "Id": csv_path.name,
        }
        with open(node_path, "a", encoding="utf-8") as f:
            json.dump(node_record, f, ensure_ascii=False)
            f.write("\n")

        # Mark as processed
        with open(record_path, "a", encoding="utf-8") as f:
            f.write(f"{csv_path.stem}\n")

    def run(self) -> None:
        """Process all CSV files in the configured directory."""
        csv_files = list(self.csv_dir.glob("*.csv"))
        if not csv_files:
            logger.info("No CSV files found in %s", self.csv_dir)
            return

        # Build a stem → caption mapping from the pre-generated description JSONs.
        # Each JSON was written by sheet_parser and carries meta.description which
        # holds the LLM-generated caption (prefixed with the dataset name).
        # We read the files directly here instead of going through
        # load_table_description() to avoid opening the same file twice.
        table_captions: dict[str, str] = {}
        for desc_file in self.desc_dir.glob("*.json"):
            try:
                with open(desc_file, "r", encoding="utf-8") as fh:
                    raw = json.load(fh)
                meta = raw.get("meta", {})
                table_captions[desc_file.stem] = meta.get("description", "")
            except Exception:
                logger.warning("Could not read description file %s", desc_file, exc_info=True)

        skipped = 0
        for csv_file in csv_files:
            caption = table_captions.get(csv_file.stem)
            # A missing description means sheet_parser never wrote the JSON for
            # this CSV (e.g. the pipeline was interrupted between CSV export and
            # JSON write).  Skip with a warning rather than crashing the whole run.
            if caption is None:
                logger.warning(
                    "No description file for %s — skipping (re-run sheet parsing to fix)",
                    csv_file.name,
                )
                skipped += 1
                continue
            self.process_csv(csv_file, caption)

        logger.info("CSV ingestion complete: %d processed, %d skipped", len(csv_files) - skipped, skipped)
