"""Text-to-SQL conversion using an LLM.

Generates a SQL DDL from a TableSchema and converts a natural-language
question into a SQL query against that schema.
"""

import re
from typing import Any

from openai import OpenAI

from hetagen.core.table_flow.config import Config
from hetagen.core.table_flow.prompts import TEXT2SQL_PROMPT_TEMPLATE
from hetagen.core.table_flow.table_schema import TableSchema


def _sanitize_column_name(name: str) -> str:
    """Convert a column name to a SQL-friendly format.

    Args:
        name: Raw column name (may contain CJK characters, parentheses, etc.).

    Returns:
        SQL-safe column name wrapped in backticks.
    """
    # Keep the original name as identifier, wrapped in backticks for CJK / special chars
    # Remove newlines and collapse whitespace
    name = re.sub(r'\s+', ' ', name).strip()
    return f"`{name}`"


def schema_to_ddl(
    schema: TableSchema | dict[str, Any],
    table_name: str = "data_table"
) -> str:
    """Generate a SQL DDL (CREATE TABLE) statement from a TableSchema.

    Args:
        schema: TableSchema object or equivalent dict.
        table_name: Table name, defaults to ``"data_table"``.

    Returns:
        SQL DDL statement string.
    """
    # Convert to dict
    if isinstance(schema, TableSchema):
        schema_dict = schema.to_dict()
    else:
        schema_dict = schema

    columns_sql = []

    # Add entity column (one entity per row)
    columns_sql.append("`实体` VARCHAR(255) NOT NULL COMMENT '实体名称'")

    # Get time dimension info
    time_dimension = schema_dict.get("time_dimension")
    time_column_name = None
    if time_dimension:
        time_column_name = time_dimension.get("column_name")

    # Add metric columns
    for col in schema_dict.get("columns", []):
        col_name = col.get("name", "")
        col_desc = col.get("description", "")
        safe_col_name = _sanitize_column_name(col_name)

        # Escape single quotes in the comment
        col_desc_escaped = col_desc.replace("'", "''")
        columns_sql.append(f"{safe_col_name} TEXT COMMENT '{col_desc_escaped}'")

    # Build CREATE TABLE statement
    ddl = f"CREATE TABLE `{table_name}` (\n  " + ",\n  ".join(columns_sql) + "\n);"

    # Append table metadata comments
    title = schema_dict.get("title", "")
    description = schema_dict.get("description", "")
    entities = schema_dict.get("entities", [])

    ddl += f"\n\n-- 表格标题: {title}"
    ddl += f"\n-- 表格描述: {description}"
    ddl += f"\n-- 实体列表: {', '.join(entities)}"

    # If a time dimension exists, append time-related comments
    if time_dimension:
        td_type = time_dimension.get("type", "")
        td_format = time_dimension.get("format", "")
        td_start = time_dimension.get("start", "")
        td_end = time_dimension.get("end", "")
        ddl += f"\n-- 时间维度: 类型={td_type}, 格式={td_format}, 范围={td_start} ~ {td_end}"

    return ddl


class Text2SQLConverter:
    """Text-to-SQL converter using an OpenAI-compatible LLM."""

    def __init__(self, config: Config | None = None):
        """Initialize the converter.

        Args:
            config: Configuration object; created automatically when *None*.
        """
        self.config = config or Config()
        self.client = OpenAI(
            api_key=self.config.llm_api_key,
            base_url=self.config.llm_base_url,
        )

    def convert(
        self,
        question: str,
        schema: TableSchema | dict[str, Any],
        table_name: str = "data_table"
    ) -> str:
        """Convert a natural-language question into a SQL query.

        Args:
            question: User's natural-language question.
            schema: TableSchema object or equivalent dict.
            table_name: Target table name.

        Returns:
            SQL query string.
        """
        # Generate DDL
        ddl = schema_to_ddl(schema, table_name)

        # Fill in the prompt template
        prompt = TEXT2SQL_PROMPT_TEMPLATE.format(
            table_schema=ddl,
            question=question
        )

        # Call the LLM
        response = self.client.chat.completions.create(
            model=self.config.llm_model,
            temperature=self.config.llm_temperature,
            messages=[
                {"role": "user", "content": prompt},
            ],
        )

        sql = response.choices[0].message.content.strip()

        # Strip markdown code-block markers if present
        sql = self._clean_sql(sql)

        return sql

    def _clean_sql(self, sql: str) -> str:
        """Remove markdown code-block markers from a SQL string.

        Args:
            sql: Raw SQL string potentially wrapped in markdown fences.

        Returns:
            Cleaned SQL string.
        """
        sql = sql.strip()
        # Remove leading code-block marker
        if sql.startswith("```sql"):
            sql = sql[6:]
        elif sql.startswith("```"):
            sql = sql[3:]
        # Remove trailing code-block marker
        if sql.endswith("```"):
            sql = sql[:-3]
        return sql.strip()


def text_to_sql(
    question: str,
    schema: TableSchema | dict[str, Any],
    config: Config | None = None,
    table_name: str = "data_table"
) -> str:
    """Convenience wrapper: convert a natural-language question to SQL.

    Args:
        question: User's natural-language question.
        schema: TableSchema object or equivalent dict.
        config: Configuration object; created automatically when *None*.
        table_name: Target table name.

    Returns:
        SQL query string.
    """
    converter = Text2SQLConverter(config)
    return converter.convert(question, schema, table_name)


if __name__ == "__main__":
    import json
    from hetagen.core.table_flow.table_schema import TableDefiner

    config = Config()

    # Generate table schema first
    print("Generating table schema...")
    table_definer = TableDefiner(config)
    schema = table_definer.define_table("比较苹果和微软的市值和营收")

    print("\nTable schema JSON:")
    print(json.dumps(schema.to_dict(), ensure_ascii=False, indent=2))

    print("\n" + "=" * 50)
    print("Generated SQL DDL:")
    print("=" * 50)
    print(schema_to_ddl(schema))

    print("\n" + "=" * 50)
    print("Text-to-SQL conversion examples:")
    print("=" * 50)

    # Convert questions to SQL
    converter = Text2SQLConverter(config)

    test_questions = [
        "苹果的市值是多少",
        "哪个公司的营收更高",
        "列出所有公司的市值和营收",
    ]

    for q in test_questions:
        print(f"\nQuestion: {q}")
        sql = converter.convert(q, schema)
        print(f"SQL: {sql}")
