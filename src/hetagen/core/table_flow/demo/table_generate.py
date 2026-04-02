"""
表格生成模块

将 TableSchema 定义的表格结构和 DataRetriever 检索到的数据
整合生成 CSV 表格文件。
"""

import csv
import json
from pathlib import Path
from typing import Optional, Union

from hetagen.core.table_flow.demo.table_define import TableSchema, TableDefiner, ColumnDef
from hetagen.core.table_flow.demo.retrieve_llm import DataRetriever, ExtractedRow
from hetagen.core.table_flow.config import Config


class TableGenerator:
    """表格生成器"""

    def __init__(self, table_schema: TableSchema):
        """
        初始化表格生成器

        Args:
            table_schema: 表格结构定义
        """
        self.table_schema = table_schema

    def get_column_names(self) -> list[str]:
        """
        获取表格的列名列表

        Returns:
            list[str]: 列名列表
        """
        return [col.name for col in self.table_schema.columns]

    def generate_csv(
        self,
        data: list[dict],
        output_path: Union[str, Path],
        encoding: str = "utf-8-sig",
    ) -> Path:
        """
        生成 CSV 文件

        Args:
            data: 字典列表格式的数据（由 DataRetriever.to_json() 返回）
            output_path: 输出文件路径
            encoding: 文件编码，默认 utf-8-sig（兼容 Excel）

        Returns:
            Path: 生成的文件路径
        """
        output_path = Path(output_path)
        column_names = self.get_column_names()

        with open(output_path, "w", newline="", encoding=encoding) as f:
            writer = csv.DictWriter(f, fieldnames=column_names, extrasaction="ignore")
            writer.writeheader()
            for row in data:
                # 只写入定义的列，忽略额外的键
                filtered_row = {col: row.get(col, "") for col in column_names}
                writer.writerow(filtered_row)

        return output_path

    def generate_csv_string(self, data: list[dict]) -> str:
        """
        生成 CSV 格式的字符串

        Args:
            data: 字典列表格式的数据

        Returns:
            str: CSV 格式的字符串
        """
        import io

        column_names = self.get_column_names()
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=column_names, extrasaction="ignore")
        writer.writeheader()
        for row in data:
            filtered_row = {col: row.get(col, "") for col in column_names}
            writer.writerow(filtered_row)

        return output.getvalue()


def generate_table_from_texts(
    user_question: str,
    texts: list[tuple[str, str]],
    output_path: Optional[Union[str, Path]] = None,
    config: Optional[Config] = None,
) -> tuple[TableSchema, list[dict], Optional[Path]]:
    """
    完整的表格生成流程：定义表格 -> 提取数据 -> 生成 CSV

    Args:
        user_question: 用户问题
        texts: [(entity_name, text), ...] 实体名和对应文本的列表
        output_path: 可选，输出 CSV 文件路径
        config: 可选，配置对象

    Returns:
        tuple: (表格结构, 提取的数据, CSV文件路径)
    """
    if config is None:
        config = Config()

    # 1. 定义表格结构
    definer = TableDefiner(config)
    table_schema = definer.define_table(user_question)

    # 2. 提取数据
    retriever = DataRetriever(config)
    rows = retriever.extract_batch(table_schema, texts)
    data = retriever.to_json(rows)

    # 3. 生成 CSV
    csv_path = None
    if output_path:
        generator = TableGenerator(table_schema)
        csv_path = generator.generate_csv(data, output_path)

    return table_schema, data, csv_path


if __name__ == "__main__":
    config = Config()

    # 示例：创建表格结构
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

    # 示例数据（模拟 DataRetriever.to_json() 的输出）
    sample_data = [
        {
            "公司名称": "苹果公司（Apple Inc.）",
            "市值（美元）": "约为2.8万亿美元",
            "营收（美元）": "3832亿美元",
        },
        {
            "公司名称": "微软公司（Microsoft Corporation）",
            "市值（美元）": "2.9万亿美元",
            "营收（美元）": "2119亿美元",
        },
    ]

    # 生成 CSV
    generator = TableGenerator(table_schema)

    # 输出到文件
    output_file = generator.generate_csv(sample_data, "output.csv")
    print(f"CSV 文件已生成: {output_file}")

    # 也可以直接获取 CSV 字符串
    csv_string = generator.generate_csv_string(sample_data)
    print("\nCSV 内容预览:")
    print(csv_string)

