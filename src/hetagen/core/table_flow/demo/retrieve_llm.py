"""
数据提取模块

根据表格定义（description 和 columns），从给定文本中提取数据，
并填充到对应的列中，以 JSON 形式输出。
"""

import json
from dataclasses import dataclass
from typing import Optional
from openai import OpenAI

from hetagen.core.table_flow.config import Config
from hetagen.core.table_flow.demo.table_define import TableSchema, ColumnDef


SYSTEM_PROMPT = """你是一个专业的数据提取专家。根据给定的表格结构定义，从文本中提取相关数据。

任务：
1. 仔细阅读提供的文本内容；
2. 根据表格描述和列定义，从文本中准确提取对应的数据；
3. 如果文本中找不到某列所需的数据，该字段填写 null；
4. 确保提取的数据准确，不要编造不存在的信息。

输出JSON格式：
{
    "extracted_data": {
        "列名1": "提取的值1",
        "列名2": "提取的值2",
        ...
    },
}

注意：
- 只输出JSON，不要其他内容
- 严格按照列定义提取数据，不要添加额外的字段
- 如果某个数据在文本中有多种表述，选择最准确和完整的版本
- 数值类数据请保留原始单位
"""


@dataclass
class ExtractedRow:
    """提取的单行数据"""
    
    data: dict[str, Optional[str]]  # 列名 -> 值的映射


class DataRetriever:
    """数据提取器"""

    def __init__(self, config: Config):
        self.config = config
        self.client = OpenAI(
            api_key=config.llm_api_key,
            base_url=config.llm_base_url,
        )

    def extract_from_text(
        self, 
        table_schema: TableSchema, 
        text: str,
        entity_name: Optional[str] = None
    ) -> ExtractedRow:
        """
        从文本中提取数据填充到表格列中

        Args:
            table_schema: 表格结构定义
            text: 需要提取数据的文本
            entity_name: 可选，当前处理的实体名称

        Returns:
            ExtractedRow: 提取的行数据
        """
        # 构建列定义描述
        columns_desc = "\n".join([
            f"- {col.name}: {col.description}" 
            for col in table_schema.columns
        ])

        user_prompt = f"""表格描述：{table_schema.description}

需要提取的列：
{columns_desc}

"""
        if entity_name:
            user_prompt += f"当前处理的实体：{entity_name}\n\n"
        
        user_prompt += f"请从以下文本中提取数据：\n\n{text}"

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

        extracted_data = result.get("extracted_data", {})
        
        # 确保所有列都有对应的值（缺失的设为 None）
        for col in table_schema.columns:
            if col.name not in extracted_data:
                extracted_data[col.name] = None

        return ExtractedRow(data=extracted_data)

    def extract_batch(
        self, 
        table_schema: TableSchema, 
        texts: list[tuple[str, str]]
    ) -> list[ExtractedRow]:
        """
        批量从多段文本中提取数据

        Args:
            table_schema: 表格结构定义
            texts: [(entity_name, text), ...] 实体名和对应文本的列表

        Returns:
            list[ExtractedRow]: 提取的多行数据
        """
        results = []
        for entity_name, text in texts:
            row = self.extract_from_text(table_schema, text, entity_name)
            results.append(row)
        return results

    def to_json(self, rows: list[ExtractedRow]) -> list[dict]:
        """
        将提取结果转换为字典列表格式的 JSON

        Args:
            rows: 提取的行数据列表

        Returns:
            list[dict]: 字典列表格式
        """
        return [row.data for row in rows]


if __name__ == "__main__":
    # 示例用法
    config = Config()
    
    # 创建一个示例表格定义
    table_schema = TableSchema(
        title="苹果公司与微软公司的市值与营收对比表",
        description="用于比较苹果公司与微软公司在同一统计口径下的市值与营收数据。",
        entities=["苹果公司", "微软公司"],
        columns=[
            ColumnDef(name="公司名称", description="公司全称。"),
            ColumnDef(name="市值（美元）", description="公司的市值数据（包含金额与货币单位）。"),
            ColumnDef(name="营收（美元）", description="公司的营收数据（包含金额与货币单位）。"),
        ],
    )
    
    # 示例文本
    sample_texts = [
        ("苹果公司", """
        苹果公司（Apple Inc.）是一家美国跨国科技公司，总部位于加利福尼亚州库比蒂诺。
        截至2024年，苹果公司的市值约为2.8万亿美元，是全球市值最高的公司之一。
        2023财年，苹果公司的总营收为3832亿美元。
        """),
        ("微软公司", """
        微软公司（Microsoft Corporation）是一家美国跨国科技企业，总部位于华盛顿州雷德蒙德。
        截至2024年，微软的市值约为2.9万亿美元，与苹果公司不相上下。
        2023财年，微软的总营收达到2119亿美元。
        """),
    ]
    
    # 提取数据并输出
    retriever = DataRetriever(config)
    rows = retriever.extract_batch(table_schema, sample_texts)
    result = retriever.to_json(rows)
    print(json.dumps(result, ensure_ascii=False, indent=2))

