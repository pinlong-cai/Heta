"""Descriptive Text-to-SQL engine.

Translates a natural-language question into a SQL SELECT query using an LLM,
executes it against PostgreSQL, then generates a human-readable answer.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any, Callable

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


class DescriptiveText2SQLEngine:
    """Generate and execute SQL from natural language, then summarise the result."""

    def __init__(
        self,
        table_info_dir: str,
        postgres_config: dict[str, Any],
        use_llm: Callable[..., str] | None = None,
    ) -> None:
        self.pg_config = postgres_config
        self.info_dir = Path(table_info_dir)
        self.llm = use_llm

    def load_table_info(self, table_name: str) -> dict[str, Any]:
        """Load the table description JSON produced during CSV import."""
        info_path = self.info_dir / f"{table_name}.json"
        if not info_path.exists():
            raise FileNotFoundError(
                f"Table description file not found: {info_path}. Import the CSV first."
            )
        with open(info_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def sanitize_and_execute_sql(self, sql: str) -> list[dict[str, Any]]:
        """Execute a read-only SQL statement with basic safety checks."""
        sql = sql.strip().rstrip(";")
        if not re.match(r"^\s*SELECT\s", sql, re.IGNORECASE):
            raise ValueError("Only SELECT queries are allowed")
        if re.search(r"(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER)", sql, re.IGNORECASE):
            raise ValueError("Illegal SQL operation detected")

        conn = None
        try:
            conn = psycopg2.connect(**self.pg_config)
            conn.cursor().execute("SET statement_timeout = '30s'")
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql)
                return [dict(row) for row in cur.fetchall()]
        except Exception:
            logger.error("SQL execution failed", exc_info=True)
            raise
        finally:
            if conn:
                conn.close()

    def _build_sql_prompt(
        self,
        question: str,
        table_name: str,
        table_purpose: str,
        table_structure: str,
        field_info: str,
        example_sql: str,
        field_samples: dict[str, list[str]] | None = None,
        prior_sql: str | None = None,
        error_msg: str | None = None,
    ) -> str:
        samples_section = ""
        if field_samples:
            lines = [f"  {col}: {samples}" for col, samples in field_samples.items()]
            samples_section = "字段实际示例值（直接来自数据库，反映真实数据格式）：\n" + "\n".join(lines) + "\n\n"

        base = (
            f"你是一个 SQL 生成器。请根据用户的提问和提供的数据库表结构，生成唯一且正确的SQL查询语句。\n"
            f"用户问题为：{question}\n\n"
            f"数据库表名称为：{table_name};\n"
            f"数据库表用途为：{table_purpose};\n"
            f"数据库表结构如下：{table_structure};\n"
            f"字段说明如下：{field_info};\n"
            f"{samples_section}"
            f"表查询的若干示例:{example_sql}\n\n"
            f"严格规则（必须遵守）：\n"
            f"1. 所有字段名和表名必须用双引号括起来\n"
            f"2. 只允许生成 SELECT 查询语句，禁止 INSERT、UPDATE、DELETE 等任何其他操作\n"
            f"3. 仅对示例值为纯数字的字段使用 ::NUMERIC 转换；若示例值含单位后缀（如 540B、1.6T、70M），该字段为 TEXT，禁止直接 ::NUMERIC\n"
            f"4. 输出必须仅包含 SQL 语句，不得包含解释、说明、Markdown 格式或代码块\n"
            f"5. SQL 语句必须以英文分号 `;` 结尾\n"
            f"6. 对于不确定的文本字段或时间的匹配尽量使用 LIKE 进行模糊匹配\n"
            f"7. 若 SELECT 子句中包含聚合函数（SUM/AVG/COUNT/MAX/MIN），则所有非聚合字段必须出现在 GROUP BY 子句中\n"
            f"8. 若需对含单位后缀的 TEXT 字段做数值比较，使用 REGEXP_REPLACE 提取纯数字后转换，"
            f"例如：CAST(REGEXP_REPLACE(\"Parameters\", '[^0-9.]', '', 'g') AS NUMERIC) > 100；"
            f"注意单位换算（1T=1000B，1B=1000M），在提取数字前先用 LIKE '%T%' / LIKE '%B%' 分支处理不同量级"
        )
        if prior_sql and error_msg:
            base += (
                f"\n\n上一次生成的 SQL 执行失败，请修正后重新输出：\n"
                f"失败的 SQL：{prior_sql}\n"
                f"错误信息：{error_msg}"
            )
        return base

    def _is_table_relevant(
        self, question: str, table_name: str, table_purpose: str, table_structure: str,
    ) -> bool:
        """Ask the LLM whether this table can answer the question.

        Returns True if the table is relevant, False otherwise.  Defaults to
        True on LLM failure so we never silently drop a potentially useful table.
        """
        prompt = (
            f"你是一个数据库助手。判断以下数据库表是否能回答用户的问题。\n"
            f"用户问题：{question}\n"
            f"表名：{table_name}\n"
            f"表用途：{table_purpose}\n"
            f"表字段：{table_structure}\n\n"
            f"仅输出 YES 或 NO，不要任何解释。"
        )
        try:
            resp = self.llm(prompt).strip().upper()
            return resp.startswith("Y")
        except Exception:
            logger.warning("Table relevance check failed for '%s', assuming relevant", table_name)
            return True

    def query(self, question: str, table_name: str, max_retries: int = 2) -> str:
        """End-to-end pipeline: question → SQL → execute → natural-language answer.

        On SQL execution failure the error is fed back to the LLM for correction,
        up to *max_retries* attempts, covering GROUP BY violations, missing quotes,
        wrong column names, and other runtime errors.
        """
        # 1. Load table description
        info = self.load_table_info(table_name)
        table_purpose = info["table_purpose"]
        table_structure = ", ".join(info["field_descriptions"].keys())
        field_info = "\n".join(
            f"{field}: {desc}" for field, desc in info["field_descriptions"].items()
        )
        example_sql = "\n".join(info["example_queries"])
        # field_samples: verbatim values from the CSV, e.g. {"Parameters": ["540B", "70B"]}
        field_samples: dict[str, list[str]] = info.get("field_samples", {})

        # 2. Relevance gate: skip tables that cannot answer the question
        if not self._is_table_relevant(question, table_name, table_purpose, table_structure):
            logger.info("Table '%s' deemed irrelevant to question, skipping", table_name)
            return "None"

        # 3. Generate SQL with self-correction loop
        sql = ""
        result: list[dict] = []
        last_error = ""
        for attempt in range(max_retries):
            prompt = self._build_sql_prompt(
                question, table_name, table_purpose, table_structure, field_info,
                example_sql,
                field_samples=field_samples,
                prior_sql=sql if attempt > 0 else None,
                error_msg=last_error if attempt > 0 else None,
            )
            sql = self.llm(prompt)
            logger.info("Generated SQL for '%s' (attempt %d): %s", table_name, attempt + 1, sql)
            try:
                result = self.sanitize_and_execute_sql(sql)
                logger.info("SQL executed successfully for '%s': %d rows", table_name, len(result))
                break
            except Exception as e:
                last_error = str(e)
                logger.warning(
                    "SQL attempt %d/%d failed for table '%s': %s",
                    attempt + 1, max_retries, table_name, last_error,
                )
                if attempt == max_retries - 1:
                    logger.error(
                        "All %d SQL attempts failed for table '%s'", max_retries, table_name,
                    )
                    return "None"

        # 4. Summarise result via LLM
        context_str = "无结果" if not result else str(result[:5])

        answer_info_prompt = f"""你是一个问答助手。严格依据给定上下文作答，输出自然、易读的中文答案。

        上下文：
        - 原始问题：{question}
        - 生成的SQL：{sql}
        - 检索到的数据库信息：{context_str}

        输出规则（必须遵守）：
        - 仅输出一段连贯的中文文字，不要使用列表、加粗、标题、代码块或任何Markdown格式。
        - 句子简洁通顺，直给结论，不要输出过多的信息，例如从表中检索出的信息。
        - 若问题涉及专有名词，在答案中保持表述一致。
        - 若信息不足，直接输出"None"。
        - 不要输出与问题无关的信息。
        - 若检索到的数据库信息过多，则概述回答，不要输出所有信息。"""

        answer = self.llm(answer_info_prompt)
        return answer.strip()
