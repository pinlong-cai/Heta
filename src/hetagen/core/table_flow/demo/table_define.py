"""
动态表格定义模块

让大模型根据用户问题一次性定义表格结构，
包括需要查询的实体列表和表格的列定义。
"""

import json
from dataclasses import dataclass, field
from typing import Optional
from openai import OpenAI

from hetagen.core.table_flow.config import Config


SYSTEM_PROMPT = """你是一个表格结构设计专家。根据用户的问题，设计一个合适的表格结构来回答问题。

# 要求：
1. 分析用户问题，确定需要查询哪些实体；
2. 设计表格的列，每列对应一个需要查询的属性；
3. 尽量使用实体的标准名称或详细全称，避免使用简称或别名；
4. 严格按照用户问题设计列，不要随意扩展；
5. 对属性不要进行不必要的拆分，比如不要把市值拆分成市值和货币类型。
6. 对于时间列，必须在描述中指定简洁的格式（如："YYYY Qn"）

# 输出JSON格式：
{
    "title": "表格标题",
    "description": "表格描述",
    "entities": ["实体1的标准名", "实体2的标准名", ...],
    "columns": [
        {
            "name": "列名1", 
            "description": "该列的信息及格式约束", 
        },
        {
            "name": "列名2", 
            "description": "该列的信息及格式约束", 
        },
        ...
    ]
}

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
      "name": "公司名称",
      "description": "公司全称。"
    },
    {
      "name": "市值（美元）",
      "description": "公司的市值数据（包含金额与货币单位），并确保与对比方使用相同的统计时点。"
    },
    {
      "name": "营收（美元）",
      "description": "公司的营收数据（包含金额与货币单位），并确保与对比方使用相同的统计期间（例如最近一个财年或最近12个月）。"
    }
  ]
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
      "description": "财务数据对应的季度，统一使用'YYYY Qn'格式。"
    },
    {
      "name": "营收（人民币）",
      "description": "该季度的营业收入金额（包含单位）。"
    },
    {
      "name": "净利润（人民币）",
      "description": "该季度的净利润金额（包含单位）。"
    }
  ]
}
注意：
- 只输出JSON，不要其他内容
"""

@dataclass
class ColumnDef:
    """表格列定义"""

    name: str  # 列名
    description: str  # 列描述（用于指导属性匹配）


@dataclass
class TableSchema:
    """表格结构定义"""

    title: str  # 表格标题
    description: str  # 表格描述
    entities: list[str]  # 需要查询的实体名称列表（尽量使用标准名/全称）
    columns: list[ColumnDef]  # 列定义

    def to_dict(self) -> dict:

        return {
            "title": self.title,
            "description": self.description,
            "entities": self.entities,
            "columns": [{"name": c.name, "description": c.description} for c in self.columns],
           
        }





class TableDefiner:
    """表格定义器"""

    def __init__(self, config: Config):
        self.config = config
        self.client = OpenAI(
            api_key=config.llm_api_key,
            base_url=config.llm_base_url,
        )

    def define_table(self, user_question: str, context: Optional[str] = None) -> TableSchema:
        """
        根据用户问题定义表格结构

        Args:
            user_question: 用户问题
            context: 可选的上下文信息（如可用的实体类型列表）

        Returns:
            TableSchema
        """
        user_prompt = f"用户问题：{user_question}"
        if context:
            user_prompt += f"\n\n可用信息：{context}"

        response = self.client.chat.completions.create(
            model=self.config.llm_model,
            temperature=self.config.llm_temperature,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
        )

        result = json.loads(response.choices[0].message.content)

        columns = [
            ColumnDef(
                name=col["name"],
                description=col.get("description", ""),
            )
            for col in result.get("columns", [])
        ]

        return TableSchema(
            title=result.get("title", "查询结果"),
            description=result.get("description", ""),
            entities=result.get("entities", []),
            columns=columns,
        )

if __name__ == "__main__":
    config = Config()
    table_definer = TableDefiner(config)
    # table_schema = table_definer.define_table("比较苹果和微软的市值和营收")
    # table_schema = table_definer.define_table("腾讯过去5年的季度营收和净利润变化")
    # table_schema = table_definer.define_table("2020-2024年，比亚迪和特斯拉每年的汽车交付量")
    # table_schema = table_definer.define_table("列出全球市值最高的5家科技公司及其当前CEO")
    table_schema = table_definer.define_table("列出太阳系中拥有光环的行星，并给出它们的卫星数量")

    
    print(json.dumps(table_schema.to_dict(), ensure_ascii=False, indent=2))