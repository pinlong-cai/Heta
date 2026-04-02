"""CSV to SQLite conversion utilities."""

import csv
import logging
import os
import re
import sqlite3
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _sanitize_table_name(name: str) -> str:
    """Convert a filename into a valid SQLite table name.

    Args:
        name: Original filename.

    Returns:
        Sanitized table name.
    """
    # Strip file extension
    name = Path(name).stem
    # Replace illegal characters with underscores
    name = re.sub(r'[^\w\u4e00-\u9fff]', '_', name)
    # Collapse consecutive underscores
    name = re.sub(r'_+', '_', name)
    # Strip leading/trailing underscores
    name = name.strip('_')
    return name if name else "data_table"


def csv_to_sqlite(
    csv_path: str,
    sqlite_path: str | None = None,
    table_name: str | None = None,
    encoding: str = "utf-8",
    schema: dict[str, Any] | Any | None = None
) -> str:
    """Convert a CSV file into a SQLite database.

    Args:
        csv_path: Path to the CSV file.
        sqlite_path: Output SQLite database path; defaults to the CSV path
            with a ``.db`` suffix.
        table_name: Table name; defaults to a sanitized version of the CSV
            filename.
        encoding: CSV file encoding, defaults to ``"utf-8"``.
        schema: Table schema definition (TableSchema object or dict) used
            to determine column types.

    Returns:
        Path to the generated SQLite database file.
    """
    csv_path = Path(csv_path)

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    # Determine SQLite file path
    if sqlite_path is None:
        sqlite_path = csv_path.with_suffix(".db")
    else:
        sqlite_path = Path(sqlite_path)

    # Determine table name
    if table_name is None:
        table_name = _sanitize_table_name(csv_path.name)

    # Read CSV file
    with open(csv_path, "r", encoding=encoding, newline="") as f:
        reader = csv.reader(f)
        headers = next(reader)  # first row is the header

        # Convert empty strings to None
        rows = []
        for row in reader:
            cleaned_row = [val if val != "" else None for val in row]
            rows.append(cleaned_row)

    # Remove existing database file if present
    if sqlite_path.exists():
        os.remove(sqlite_path)

    # Build column-name to type mapping from schema
    column_types = {}
    if schema:
        # If schema is an object, convert to dict
        if hasattr(schema, "to_dict"):
            schema = schema.to_dict()
        for col in schema.get("columns", []):
            col_name = col.get("name", "")
            data_type = col.get("data_type", "other")
            # number -> DECIMAL(28,6), other -> TEXT
            column_types[col_name] = "DECIMAL(28,6)" if data_type == "number" else "TEXT"

    # Create SQLite database and insert data
    conn = sqlite3.connect(str(sqlite_path))
    cursor = conn.cursor()

    # Create table (use schema-derived types, default to TEXT)
    columns_def = []
    for col in headers:
        col_type = column_types.get(col, "TEXT")
        columns_def.append(f'"{col}" {col_type}')
    create_sql = f'CREATE TABLE "{table_name}" ({", ".join(columns_def)})'
    cursor.execute(create_sql)

    # Insert data
    if rows:
        placeholders = ", ".join(["?" for _ in headers])
        insert_sql = f'INSERT INTO "{table_name}" VALUES ({placeholders})'
        cursor.executemany(insert_sql, rows)

    conn.commit()
    conn.close()

    logger.info("Saved %d rows to SQLite: %s (table=%s)", len(rows), sqlite_path, table_name)

    return str(sqlite_path)


def query_sqlite(
    sqlite_path: str,
    sql: str,
    table_name: str | None = None
) -> list:
    """Execute a SQL query against a SQLite database.

    Args:
        sqlite_path: Path to the SQLite database file.
        sql: SQL query string.
        table_name: If provided, ``{table}`` placeholders in *sql* are
            replaced with this name.

    Returns:
        List of result rows as dicts.
    """
    if table_name:
        sql = sql.replace("{table}", f'"{table_name}"')

    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(sql)
    results = cursor.fetchall()

    # Convert to list of dicts
    results = [dict(row) for row in results]

    conn.close()
    return results


def get_table_info(sqlite_path: str, table_name: str) -> dict:
    """Get structural information about a SQLite table.

    Args:
        sqlite_path: Path to the SQLite database file.
        table_name: Table name.

    Returns:
        Dict with ``table_name``, ``columns``, and ``row_count`` keys.
    """
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()

    # Get table schema
    cursor.execute(f'PRAGMA table_info("{table_name}")')
    columns = cursor.fetchall()

    # Get row count
    cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
    row_count = cursor.fetchone()[0]

    conn.close()

    return {
        "table_name": table_name,
        "columns": [{"name": col[1], "type": col[2]} for col in columns],
        "row_count": row_count
    }


if __name__ == "__main__":
    # Test: convert a CSV file
    csv_file = "data/苹果公司与微软公司的市场估值与营收对比表.csv"
    csv_path = Path(__file__).parent / csv_file

    print("=" * 60)
    print("CSV to SQLite Test")
    print("=" * 60)

    # Convert CSV to SQLite
    sqlite_path = csv_to_sqlite(str(csv_path))

    # Get table info
    table_name = _sanitize_table_name(csv_path.name)
    print("\n" + "=" * 60)
    print("Table structure:")
    print("=" * 60)
    info = get_table_info(sqlite_path, table_name)
    print(f"Table name: {info['table_name']}")
    print(f"Columns: {[col['name'] for col in info['columns']]}")
    print(f"Row count: {info['row_count']}")

    # Query tests
    print("\n" + "=" * 60)
    print("Query tests:")
    print("=" * 60)

    # Query all data
    print("\n1. All rows:")
    results = query_sqlite(sqlite_path, f'SELECT * FROM "{table_name}"')
    for row in results:
        print(f"   {row}")

    # Query Apple data
    print("\n2. Apple data:")
    results = query_sqlite(
        sqlite_path,
        f'SELECT * FROM "{table_name}" WHERE "实体" = "苹果公司"'
    )
    for row in results:
        print(f"   {row}")

    # Query market valuations
    print("\n3. Market valuations:")
    results = query_sqlite(
        sqlite_path,
        f'SELECT "实体", "市场估值（美元）" FROM "{table_name}"'
    )
    for row in results:
        print(f"   {row}")
