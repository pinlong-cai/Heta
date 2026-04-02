"""CSV generation from a table schema and retrieval answers."""

import csv
from io import StringIO
from typing import Any

from hetagen.core.table_flow.utils import generate_time_values


def generate_csv(
    schema: dict[str, Any],
    answers: list[dict[str, Any]],
) -> str:
    """Build a complete CSV string from a table schema and retrieval answers.

    Args:
        schema: Table schema dict (as produced by ``TableSchema.to_dict``).
        answers: Answer list; each dict contains entity, time, metric, answer.

    Returns:
        A CSV-formatted string.
    """
    # Extract column definitions
    columns = schema.get("columns", [])
    column_names = [col.get("name", "") for col in columns]

    # Build header row: entity column + schema columns
    headers = ["实体"] + column_names

    # Entity list
    entities = schema.get("entities", [])

    # Time dimension (if present)
    time_dimension = schema.get("time_dimension")
    time_column_name = None
    time_values = []

    if time_dimension:
        time_column_name = time_dimension.get("column_name")
        time_values = generate_time_values(time_dimension)

    # Find the time column index in headers
    time_col_idx = None
    if time_column_name and time_column_name in headers:
        time_col_idx = headers.index(time_column_name)

    # Build lookup index: (entity, time, metric) -> answer
    answer_map = {}
    for item in answers:
        entity = item.get("entity")
        time_value = item.get("time")
        metric = item.get("metric")
        answer = item.get("answer", "")

        if entity and metric:
            key = (entity, time_value, metric)
            answer_map[key] = answer

    # Build data rows
    rows = []
    for entity in entities:
        if time_values and time_col_idx is not None:
            # Time dimension present — one row per time value
            for time_value in time_values:
                row = [entity]
                for col_name in column_names:
                    if col_name == time_column_name:
                        row.append(time_value)
                    else:
                        key = (entity, time_value, col_name)
                        row.append(answer_map.get(key, ""))
                rows.append(row)
        else:
            # No time dimension — one row per entity
            row = [entity]
            for col_name in column_names:
                key = (entity, None, col_name)
                row.append(answer_map.get(key, ""))
            rows.append(row)

    # Serialize to CSV
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(headers)
    writer.writerows(rows)

    return output.getvalue()
