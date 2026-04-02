import argparse
import asyncio
import yaml

from common.llm_client import create_use_llm_async
from hetagen.core.tag_tree_parser import parse_tag_tree
from hetagen.utils.path import PROJECT_ROOT



parser = argparse.ArgumentParser(description="从本地 Excel 构建标签树")
parser.add_argument("--input", default="examples/example_data/tag_tree/biological_taxonomy.xlsx", help="输入 Excel 文件路径")
parser.add_argument("--output", default="examples/example_data/tag_tree/tag_tree.json", help="输出 JSON 路径")
parser.add_argument("--name", default="生物分类标签树", help="标签树名称")
parser.add_argument("--desc", default="基于生物分类学构建的标签体系", help="标签树描述")

args = parser.parse_args()

with open(PROJECT_ROOT / "config.yaml", encoding="utf-8") as f:
    llm_config = yaml.safe_load(f)["hetagen"]["llm"]

# 使用大模型直接生成标签节点描述 或者 查询知识库形成描述
my_llm= create_use_llm_async(
    url=llm_config["base_url"],
    api_key=llm_config["api_key"],
    model=llm_config["model"],
)
# my_kb_client = ...     # 待构建知识库查询服务    

asyncio.run(parse_tag_tree(
    input_excel=args.input,
    output_json=args.output,
    tree_name=args.name,
    tree_description=args.desc,
    qa_client=my_llm,  # ← 新增参数
))