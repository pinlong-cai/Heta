"""LLM-driven table schema definition.

Asks the LLM to define a structured table schema (entities, columns,
optional time dimension) based on a user question.
"""

import json
from dataclasses import dataclass, field
from datetime import datetime
from openai import OpenAI

from hetagen.core.table_flow.config import Config


SYSTEM_PROMPT_TEMPLATE = """你是一个表格结构设计专家。根据用户的问题，设计一个合适的表格结构用于聚合查询结果来回答问题。

当前日期：{time_now}

# 要求：
- 分析用户问题中的实体范围：
    - 如果用户问题中明确指定了实体，仅查询这些实体；
    - 如果用户未提供具体实体，但描述了特定范围，请基于你的世界知识生成一份覆盖面较广的候选实体（建议为用户请求数量的2-3倍）；
- 设计表格的列，每列对应一个需要查询的属性；
- 列只能表示属性，不能将实体与属性绑定生成列；
- 对于涉及数值的列，必须在列名中指定单位（如：市值（美元）、营收（人民币））；
- 不得在列中设计任何用于表示实体名称、公司名称、主体名称、机构名称等与实体含义重复的列；
- 保留用户问题中的原始实体名称；
- 严格按照用户问题设计列，不要随意扩展；
- 对属性不要进行不必要的拆分，比如不要把市值拆分成市值和货币类型；
- 对于时间列，必须在描述中指定简洁的格式（如："YYYY Qn"）；
- 若用户问题涉及按时间展开的数据（如“过去几年”“各季度变化”），必须输出time_dimension字段，用于声明时间行的生成规则；
- 若用户问题不要求按时间展开，则time_dimension必须为 null；
- 时间粒度（year / quarter / explicit）只能来源于用户问题的显式表述，不得根据常识或趋势类词语自行推断；

# 输出JSON格式：
{
    "title": "表格标题",
    "description": "表格描述",
    "entities": ["实体1的标准名", "实体2的标准名", ...],
    "columns": [
        {
            "name": "列名1",
            "description": "该列的信息及格式约束",
            "data_type": "number 或 other"
        },
        {
            "name": "列名2",
            "description": "该列的信息及格式约束",
            "data_type": "number 或 other"
        },
        ...
    ]
    "time_dimension": {
    "type": "year | quarter | explicit",
    "format": "YYYY | YYYY Qn | YYYY-MM",
    "start": "...",
    "end": "...",
    "values": [...],
    "column_name": "时间列名",
    }

}

## data_type字段说明：
- number: 数值类型，如市值、营收、数量等，用于数值计算和排序
- other: 其他类型，如文本、日期、时间等

## time_dimension字段说明：
- type: 时间行的生成规则，必须为 year、quarter 或 explicit，分别表示按年、季度、按用户指定的具体时间值生成时间行；
- start: 时间行的开始时间，必须为 YYYY 或 YYYY Qn 格式；
- end: 时间行的结束时间，必须为 YYYY 或 YYYY Qn 格式；
- values: 时间行的值列表，必须为 YYYY 或 YYYY Qn 格式，只有当type为explicit时才需要输出；
- format: 时间行的格式，必须为 YYYY 或 YYYY Qn 格式；
- column_name: 时间列名，必须为时间列的列名；
# 示例：

- 问题：比较苹果和微软的市值和营收
- 输出：
{
  "title": "苹果公司与微软公司的市值与营收对比表",
  "description": "用于比较苹果公司与微软公司在同一统计口径下的市值与营收数据。",
  "entities": [
    "苹果公司",
    "微软公司"
  ],
  "columns": [
    {
      "name": "市值（美元）",
      "description": "公司的市值数据（包含金额与货币单位），并确保与对比方使用相同的统计时点。",
      "data_type": "number"
    },
    {
      "name": "营收（美元）",
      "description": "公司的营收数据（包含金额与货币单位），并确保与对比方使用相同的统计期间（例如最近一个财年或最近12个月）。",
      "data_type": "number"
    }
  ],
  "time_dimension": null
}

- 问题：腾讯过去5年的季度营收和净利润变化
- 输出：
{
  "title": "腾讯控股有限公司过去5年的季度营收与净利润变化表",
  "description": "用于展示腾讯控股有限公司在过去5年内各季度的营收与净利润数据，并便于观察随时间的变化趋势。",
  "entities": [
    "腾讯控股有限公司"
  ],
  "columns": [
    {
      "name": "季度",
      "description": "财务数据对应的季度，统一使用'YYYY Qn'格式。",
      "data_type": "other"
    },
    {
      "name": "营收（人民币）",
      "description": "该季度的营业收入金额（包含单位）。",
      "data_type": "number"
    },
    {
      "name": "净利润（人民币）",
      "description": "该季度的净利润金额（包含单位）。",
      "data_type": "number"
    }
  ],
  "time_dimension": {
    "type": "quarter",
    "format": "YYYY Qn",
    "start": "2021 Q1",
    "end": "2025 Q4",
    "column_name": "季度"
  }
}
# 注意：
- 只输出JSON，不要其他内容；
"""

@dataclass
class ColumnDef:
    """Table column definition."""

    name: str  # column name
    description: str  # column description (guides attribute matching)
    data_type: str = "other"  # data type: "number" (numeric) or "other" (text)


@dataclass
class TimeDimension:
    """Time dimension definition."""

    type: str  # generation rule: year | quarter | explicit
    format: str  # format string: YYYY | YYYY Qn | YYYY-MM
    column_name: str  # time column name, must match a column in *columns*
    start: str | None = None  # start of the time range
    end: str | None = None  # end of the time range
    values: list[str] = field(default_factory=list)  # explicit time values

    def to_dict(self) -> dict:
        result = {
            "type": self.type,
            "format": self.format,
            "column_name": self.column_name,
        }
        if self.start:
            result["start"] = self.start
        if self.end:
            result["end"] = self.end
        if self.values:
            result["values"] = self.values
        return result


@dataclass
class TableSchema:
    """Complete table schema definition."""

    title: str  # table title
    description: str  # table description
    entities: list[str]  # entity names to query (prefer canonical / full names)
    columns: list[ColumnDef]  # column definitions
    time_dimension: TimeDimension | None = None  # optional time dimension

    def to_dict(self) -> dict:
        result = {
            "title": self.title,
            "description": self.description,
            "entities": self.entities,
            "columns": [{"name": c.name, "description": c.description, "data_type": c.data_type} for c in self.columns],
            "time_dimension": self.time_dimension.to_dict() if self.time_dimension else None,
        }
        return result



class TableDefiner:
    """LLM-powered table schema definer."""

    def __init__(self, config: Config):
        self.config = config
        self.client = OpenAI(
            api_key=config.llm_api_key,
            base_url=config.llm_base_url,
        )

    def define_table(self, user_question: str, context: str | None = None) -> TableSchema:
        """Define a table schema from a user question via LLM.

        Args:
            user_question: The user's natural-language question.
            context: Optional context (e.g. available entity types).

        Returns:
            TableSchema
        """
        user_prompt = f"用户问题：{user_question}"
        if context:
            user_prompt += f"\n\n可用信息：{context}"

        # Inject the current date into the system prompt template
        time_now = datetime.now().strftime("%Y-%m-%d")
        system_prompt = SYSTEM_PROMPT_TEMPLATE.replace("{time_now}", time_now)

        response = self.client.chat.completions.create(
            model=self.config.llm_model,
            temperature=self.config.llm_temperature,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
        )

        result = json.loads(response.choices[0].message.content)

        columns = [
            ColumnDef(
                name=col["name"],
                description=col.get("description", ""),
                data_type=col.get("data_type", "other"),
            )
            for col in result.get("columns", [])
        ]

        # Parse time dimension
        time_dimension = None
        td_data = result.get("time_dimension")
        if td_data:
            time_dimension = TimeDimension(
                type=td_data.get("type", "year"),
                format=td_data.get("format", "YYYY"),
                column_name=td_data.get("column_name", "时间"),
                start=td_data.get("start"),
                end=td_data.get("end"),
                values=td_data.get("values", []),
            )

        return TableSchema(
            title=result.get("title", "查询结果"),
            description=result.get("description", ""),
            entities=result.get("entities", []),
            columns=columns,
            time_dimension=time_dimension,
        )

if __name__ == "__main__":
    config = Config()
    table_definer = TableDefiner(config)
    # Example: compare Apple and Microsoft market cap & revenue
    # table_schema = table_definer.define_table("比较苹果和微软的市值和营收")
    table_schema = table_definer.define_table("腾讯过去5年的季度营收和净利润变化")
    # table_schema = table_definer.define_table("2020-2024年，比亚迪和特斯拉每年的汽车交付量")
    # table_schema = table_definer.define_table("列出全球市值最高的5家科技公司及其当前CEO")
    # table_schema = table_definer.define_table("列出太阳系中拥有光环的行星，并给出它们的卫星数量")


    print(json.dumps(table_schema.to_dict(), ensure_ascii=False, indent=2))