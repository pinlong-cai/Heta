"""
数据集生成模块

读取 Excel 文件中的公司信息，调用大模型并发生成公司描述，保存为 JSONL 文件。
"""

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import pandas as pd
from openai import OpenAI

from hetagen.core.table_flow.config import Config


# 系统提示词
SYSTEM_PROMPT = """你是一个专业的公司信息描述撰写专家。请根据提供的公司信息，生成一段详细的公司描述。

# 要求
- 描述必须包含公司的市值信息
- 描述内容应涵盖：公司概况、主营业务、行业地位、市场表现等方面
- 描述应客观、专业、信息丰富
- 生成的描述长度控制在 800tokens 左右
- 直接输出描述内容，不要添加标题或额外格式

# 输出
直接输出公司描述文本，不要包含任何多余内容。
"""

USER_PROMPT_TEMPLATE = """请为以下公司生成详细描述：

- 股票代码：{symbol}
- 公司名称：{name}
- 市值：{market_cap}

请生成一段 800-1000 tokens 的详细描述，必须包含市值信息。
"""


def format_market_cap(value: float) -> str:
    """
    将市值数值格式化为可读字符串

    Args:
        value: 市值数值

    Returns:
        格式化后的字符串
    """
    if value >= 1e12:
        return f"{value / 1e12:.2f} 万亿美元"
    elif value >= 1e9:
        return f"{value / 1e9:.2f} 亿美元"
    elif value >= 1e6:
        return f"{value / 1e6:.2f} 百万美元"
    else:
        return f"{value:.2f} 美元"


def count_tokens(text: str) -> int:
    """
    估算文本的 token 数

    Args:
        text: 输入文本

    Returns:
        token 数量（估算值）
    """
    # 简单估算：中文约 1.5 字符/token，英文约 4 字符/token
    chinese_chars = sum(1 for c in text if '\u4e00' <= c <= '\u9fff')
    other_chars = len(text) - chinese_chars
    return int(chinese_chars / 1.5 + other_chars / 4)


def read_excel_data(excel_path: str) -> list[dict]:
    """
    读取 Excel 文件中的公司数据

    Args:
        excel_path: Excel 文件路径

    Returns:
        公司数据列表
    """
    df = pd.read_excel(excel_path)

    data = []
    for _, row in df.iterrows():
        data.append({
            "symbol": row["Symbol"],
            "name": row["Name"],
            "market_cap": row["Market Cap"]
        })

    return data


class DatasetGenerator:
    """数据集生成器"""

    def __init__(self, config: Optional[Config] = None):
        """
        初始化数据集生成器

        Args:
            config: 配置对象
        """
        self.config = config or Config()
        self.client = OpenAI(
            api_key=self.config.llm_api_key,
            base_url=self.config.llm_base_url,
        )

    def generate_description(self, symbol: str, name: str, market_cap: float) -> str:
        """
        调用大模型生成公司描述

        Args:
            symbol: 股票代码
            name: 公司名称
            market_cap: 市值

        Returns:
            公司描述
        """
        market_cap_str = format_market_cap(market_cap)

        user_prompt = USER_PROMPT_TEMPLATE.format(
            symbol=symbol,
            name=name,
            market_cap=market_cap_str
        )

        response = self.client.chat.completions.create(
            model=self.config.llm_model,
            temperature=0.7,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
        )

        return response.choices[0].message.content.strip()

    def process_company(self, idx: int, company: dict) -> dict:
        """
        处理单个公司数据

        Args:
            idx: 索引
            company: 公司数据

        Returns:
            处理结果
        """
        symbol = company["symbol"]
        name = company["name"]
        market_cap = company["market_cap"]

        print(f"[{idx + 1}] 正在生成 {symbol} ({name}) 的描述...")

        content = self.generate_description(symbol, name, market_cap)
        length = count_tokens(content)

        print(f"    生成完成，token 数: {length}")

        return {
            "id": idx,
            "content": content,
            "length": length
        }

    def generate_dataset(
        self,
        excel_path: str,
        output_path: Optional[str] = None,
        max_workers: int = 16
    ) -> str:
        """
        并发生成数据集

        Args:
            excel_path: Excel 文件路径
            output_path: 输出 JSONL 文件路径，为 None 时与 Excel 同名
            max_workers: 最大并发线程数

        Returns:
            输出文件路径
        """
        excel_path = Path(excel_path)

        if output_path is None:
            output_path = excel_path.with_suffix(".jsonl")
        else:
            output_path = Path(output_path)

        # 读取 Excel 数据
        print(f"正在读取 Excel 文件: {excel_path}")
        companies = read_excel_data(str(excel_path))
        print(f"共读取 {len(companies)} 条公司数据")

        print(f"\n开始并发生成描述 (max_workers={max_workers})，输出文件: {output_path}")
        print("=" * 60)

        # 存储结果
        results = [None] * len(companies)

        # 使用线程池并发处理
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_idx = {
                executor.submit(self.process_company, idx, company): idx
                for idx, company in enumerate(companies)
            }

            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    results[idx] = future.result()
                except Exception as e:
                    print(f"    [错误] 索引 {idx}: {e}")
                    results[idx] = {
                        "id": idx,
                        "content": f"生成失败: {e}",
                        "length": 0
                    }

        # 按顺序写入文件
        with open(output_path, "w", encoding="utf-8") as f:
            for result in results:
                f.write(json.dumps(result, ensure_ascii=False) + "\n")

        print("=" * 60)
        print(f"数据集生成完成，共 {len(companies)} 条记录")
        print(f"输出文件: {output_path}")

        return str(output_path)


def make_dataset(
    excel_path: str,
    output_path: Optional[str] = None,
    config: Optional[Config] = None,
    max_workers: int = 10
) -> str:
    """
    便捷函数：并发生成数据集

    Args:
        excel_path: Excel 文件路径
        output_path: 输出 JSONL 文件路径
        config: 配置对象
        max_workers: 最大并发线程数

    Returns:
        输出文件路径
    """
    generator = DatasetGenerator(config)
    return generator.generate_dataset(excel_path, output_path, max_workers)


if __name__ == "__main__":
    # 测试：并发生成数据集
    excel_file = "data/top_100.xlsx"
    excel_path = Path(__file__).parent / excel_file

    print("=" * 60)
    print("数据集并发生成测试")
    print("=" * 60)

    config = Config()
    generator = DatasetGenerator(config)

    # 并发生成完整数据集
    output_path = generator.generate_dataset(str(excel_path), max_workers=16)

    # 验证输出
    print("\n" + "=" * 60)
    print("验证输出文件:")
    print("=" * 60)

    with open(output_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
        print(f"总记录数: {len(lines)}")

        # 显示前 3 条记录
        print("\n前 3 条记录预览:")
        for i, line in enumerate(lines[:3]):
            data = json.loads(line)
            print(f"\n[{data['id']}] Token 数: {data['length']}")
            print(f"    内容预览: {data['content'][:100]}...")
