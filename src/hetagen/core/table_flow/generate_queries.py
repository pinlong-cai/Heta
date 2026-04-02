"""Query generation from a table schema.

Expands a TableSchema into a list of (entity, time, metric) retrieval tasks,
then calls the LLM in parallel batches to produce natural-language query strings.
"""

from __future__ import annotations

import json
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from openai import OpenAI

from hetagen.core.table_flow.config import Config
from hetagen.core.table_flow.utils import generate_time_values
from hetagen.core.table_flow.table_schema import TableSchema, TableDefiner

# System prompt: enforce output format
SYSTEM_PROMPT = """你是一名查询问题生成助手，给定表格结构定义与查询任务列表，请为每个任务生成清晰、可直接用于搜索的查询句。
# 要求
- queries 数组长度与任务列表完全一致，顺序保持一致；
- 每个 query 对象必须携带任务中的 task_id，用于对齐；
- 查询句必须包含实体名和指标名，若有时间值，需在查询中明确提及；
- 每个查询只针对单一指标，不要合并多个指标；
- 只输出 JSON，不要添加多余文字；

# 输出JSON格式（JSON 数组，每个元素对应一个任务）
[
  {
    "task_id": 0,
    "query": "自然语言查询句"
  },
  {
    "task_id": 1,
    "query": "自然语言查询句"
  },
  ...
]
"""


def _schema_to_dict(schema: TableSchema | dict[str, Any]) -> dict[str, Any]:
    """Normalize a TableSchema or raw dict into a plain dict."""
    if isinstance(schema, TableSchema):
        return schema.to_dict()
    return schema


def _build_tasks(schema_dict: dict[str, Any]) -> tuple[list[dict], dict | None, list[str]]:
    """Build a flat task list from the schema.

    Each task maps to one (entity, optional time value, metric) triple
    and is assigned a sequential task_id.

    Returns:
        (tasks, time_dimension, metric_columns)
    """
    entities: list[str] = schema_dict.get("entities", [])
    time_dimension = schema_dict.get("time_dimension")
    columns = schema_dict.get("columns", [])

    # Determine the time column name
    time_column = None
    if time_dimension:
        time_column = time_dimension.get("column_name")

    # Non-time columns are treated as metric columns
    metric_columns = [
        col.get("name") for col in columns
        if col.get("name") and col.get("name") != time_column
    ]

    # Generate the list of time values
    time_values: list[str | None]
    if time_dimension:
        time_values = generate_time_values(time_dimension) or []
    else:
        time_values = [None]

    if not time_values:
        time_values = [None]

    # Expand tasks: entity x time x metric
    tasks: list[dict] = []
    task_id_counter = 0
    for entity in entities:
        for tv in time_values:
            for metric in metric_columns:
                tasks.append({
                    "task_id": task_id_counter,
                    "entity": entity,
                    "time": tv,
                    "metric": metric,
                })
                task_id_counter += 1
    return tasks, time_dimension, metric_columns


def _default_query(entity: str, time_value: str | None, metric: str) -> str:
    """Fallback query generation when LLM output is unavailable."""
    if time_value:
        return f"请查询{entity}在{time_value}的{metric}是多少？"
    return f"请查询{entity}的{metric}是多少？"


class QueryGenerator:
    """Generates natural-language queries from a table schema."""

    def __init__(self, config: Config | None = None):
        self.config = config or Config()
        self.client = OpenAI(
            api_key=self.config.llm_api_key,
            base_url=self.config.llm_base_url,
        )

    def _build_user_prompt(self, schema_dict: dict[str, Any], tasks: list[dict]) -> str:
        """Build the user prompt containing the schema and task list for the LLM."""
        return (
            "下方是表格的结构定义，请针对任务列表生成查询句。\n\n"
            f"表格标题：{schema_dict.get('title', '')}\n"
            f"表格描述：{schema_dict.get('description', '')}\n\n"
            "任务列表（需与输出一一对应，每个任务只查询单一指标）：\n"
            f"{json.dumps(tasks, ensure_ascii=False, indent=2)}\n"
        )

    def _call_llm_batch(
        self,
        schema_dict: dict[str, Any],
        tasks: list[dict],
        max_retries: int = 5,
        base_delay: float = 1.0,
    ) -> list[dict]:
        """Call the LLM for a single batch of tasks and return parsed queries.

        Returns an empty list on failure. Retries with exponential back-off
        on HTTP 429 rate-limit errors.

        Args:
            schema_dict: Table schema as a dict.
            tasks: Task list for this batch.
            max_retries: Maximum retry attempts (default 5).
            base_delay: Initial back-off delay in seconds (default 1.0).
        """
        user_prompt = self._build_user_prompt(schema_dict, tasks)

        for attempt in range(max_retries + 1):
            try:
                response = self.client.chat.completions.create(
                    model=self.config.llm_model,
                    temperature=self.config.llm_temperature,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt},
                    ],
                    response_format={
                        "type": "json_schema",
                        "json_schema": {
                            "name": "query_list",
                            "schema": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "task_id": {"type": "integer"},
                                        "query": {"type": "string"},
                                    },
                                    "required": ["task_id", "query"],
                                    "additionalProperties": False,
                                },
                            },
                        },
                    },
                )
                content = response.choices[0].message.content
                parsed = json.loads(content)
                queries = None
                if isinstance(parsed, list):
                    queries = parsed
                elif isinstance(parsed, dict):
                    queries = parsed.get("queries")
                if isinstance(queries, list) and queries:
                    return queries
                return []
            except Exception as e:
                # Check for HTTP 429 rate-limit error
                is_rate_limit = False
                if hasattr(e, "status_code") and e.status_code == 429:
                    is_rate_limit = True
                elif "429" in str(e) or "rate" in str(e).lower():
                    is_rate_limit = True

                if is_rate_limit and attempt < max_retries:
                    # Exponential back-off with random jitter
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    time.sleep(delay)
                    continue
                # Non-rate-limit error or retries exhausted — give up
                break
        return []

    def _call_llm_concurrent(
        self,
        schema_dict: dict[str, Any],
        tasks: list[dict],
        batch_size: int = 5,
        max_workers: int = 32,
    ) -> list[dict]:
        """Split tasks into batches and call the LLM concurrently.

        Uses high concurrency with per-request exponential back-off:
        effective concurrency self-adjusts when rate-limited.

        Args:
            schema_dict: Table schema as a dict.
            tasks: All tasks to generate queries for.
            batch_size: Number of tasks per batch (default 5).
            max_workers: Maximum concurrent workers (default 32).

        Returns:
            Merged list of all query dicts.
        """
        if not tasks:
            return []

        # Split tasks into batches
        batches = [
            tasks[i : i + batch_size] for i in range(0, len(tasks), batch_size)
        ]

        all_queries: list[dict] = []

        # Dispatch all batches via thread pool
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._call_llm_batch, schema_dict, batch): batch
                for batch in batches
            }
            for future in as_completed(futures):
                batch_result = future.result()
                all_queries.extend(batch_result)

        return all_queries

    def generate(self, schema: TableSchema | dict[str, Any]) -> list[dict[str, Any]]:
        """Generate query strings for every (entity, time, metric) cell.

        Returns:
            A list of dicts, each containing:
            ``task_id``, ``entity``, ``time`` (dict with key/value or None),
            ``metric``, and ``query``.
        """
        schema_dict = _schema_to_dict(schema)
        tasks, td, metric_columns = _build_tasks(schema_dict)

        if not tasks:
            return []

        llm_queries = self._call_llm_concurrent(schema_dict, tasks)
        llm_query_by_id: dict[str, str | None] = {}
        if llm_queries:
            for item in llm_queries:
                tid = item.get("task_id")
                q = item.get("query")
                if tid is not None and q:
                    llm_query_by_id[str(tid)] = q

        results: list[dict[str, Any]] = []
        time_column = td.get("column_name") if td else None

        for idx, task in enumerate(tasks):
            task_id = task.get("task_id")
            entity = task.get("entity")
            time_value = task.get("time")
            metric = task.get("metric")

            # Prefer LLM result; fall back to default when missing
            llm_query = llm_query_by_id.get(str(task_id)) if task_id is not None else None
            if llm_query is None and llm_queries and idx < len(llm_queries):
                llm_query = llm_queries[idx].get("query")

            query_text = llm_query or _default_query(entity, time_value, metric)

            # Build the time field
            time_field = None
            if time_column and time_value is not None:
                time_field = {
                    "key": time_column,
                    "value": time_value,
                }

            results.append({
                "task_id": task_id,
                "entity": entity,
                "time": time_field,
                "metric": metric,
                "query": query_text,
            })

        return results


if __name__ == "__main__":
    config = Config()
    table_definer = TableDefiner(config)
    # schema = table_definer.define_table("腾讯过去5年的季度营收和净利润变化")
    schema = table_definer.define_table("比较苹果和微软的市值和营收")

    generator = QueryGenerator(config)
    queries = generator.generate(schema)

    print(json.dumps(queries, ensure_ascii=False, indent=2))
