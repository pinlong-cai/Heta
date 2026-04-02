"""
向量数据库存储脚本
将 embedding.py 生成的向量文件存入 Milvus 向量数据库
"""

import json
from pathlib import Path

import yaml
from pymilvus import (
    connections,
    Collection,
    CollectionSchema,
    FieldSchema,
    DataType,
    utility
)

from common.config import get_persistence
from hetagen.utils.path import PROJECT_ROOT

with open(PROJECT_ROOT / "config.yaml", encoding="utf-8") as _f:
    _milvus_cfg = yaml.safe_load(_f)["hetagen"]["milvus"]

_milvus_globals = get_persistence("milvus")

INPUT_FILE = "src/table_flow/data/top_100_embeddings.jsonl"
MILVUS_HOST: str = _milvus_globals["host"]
MILVUS_PORT: str = str(_milvus_globals["port"])
MILVUS_DB: str = _milvus_cfg["db_name"]
COLLECTION_NAME: str = _milvus_cfg["collection_name"]
EMBEDDING_DIM: int = _milvus_cfg["embedding_dim"]
INDEX_TYPE: str = _milvus_cfg["index_type"]
METRIC_TYPE: str = _milvus_cfg["metric_type"]
NLIST: int = _milvus_cfg["nlist"]
BATCH_SIZE: int = _milvus_cfg["batch_size"]


def load_jsonl(file_path: str) -> list[dict]:
    """读取 JSONL 文件"""
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                data.append(json.loads(line))
    return data


def connect_milvus() -> None:
    """连接到 Milvus 服务"""
    print(f"正在连接 Milvus: {MILVUS_HOST}:{MILVUS_PORT}")
    connections.connect(
        alias="autotable",
        host=MILVUS_HOST,
        port=MILVUS_PORT,
        db_name=MILVUS_DB,
    )
    print("Milvus 连接成功")


def create_collection() -> Collection:
    """创建或获取 Collection"""
    # 如果 Collection 已存在，先删除
    if utility.has_collection(COLLECTION_NAME, using="autotable"):
        print(f"Collection '{COLLECTION_NAME}' 已存在，正在删除...")
        utility.drop_collection(COLLECTION_NAME, using="autotable")
    
    # 定义 Schema
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema(name="content", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="length", dtype=DataType.INT64),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=EMBEDDING_DIM)
    ]
    schema = CollectionSchema(fields=fields, description="文本块向量存储")
    
    # 创建 Collection
    print(f"正在创建 Collection: {COLLECTION_NAME}")
    collection = Collection(name=COLLECTION_NAME, schema=schema, using="autotable")
    print(f"Collection '{COLLECTION_NAME}' 创建成功")
    
    return collection


def create_index(collection: Collection) -> None:
    """为向量字段创建索引"""
    index_params = {
        "index_type": INDEX_TYPE,
        "metric_type": METRIC_TYPE,
        "params": {"nlist": NLIST}
    }
    
    print(f"正在创建索引 (类型: {INDEX_TYPE}, 度量: {METRIC_TYPE})...")
    collection.create_index(field_name="embedding", index_params=index_params)
    print("索引创建成功")


def insert_data(collection: Collection, data: list[dict]) -> None:
    """批量插入数据到 Collection"""
    total = len(data)
    inserted = 0
    
    print(f"正在插入数据，共 {total} 条...")
    
    for i in range(0, total, BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        
        # 准备批量数据
        ids = [item['id'] for item in batch]
        contents = [item['content'] for item in batch]
        lengths = [item.get('length', len(item['content'])) for item in batch]
        embeddings = [item['embedding'] for item in batch]
        
        # 插入数据
        collection.insert([ids, contents, lengths, embeddings])
        
        inserted += len(batch)
        print(f"已插入: {inserted}/{total}")
    
    # 刷新数据，确保持久化
    collection.flush()
    print(f"数据插入完成，共 {inserted} 条")


def store_vectors() -> None:
    """主函数：将向量文件存入 Milvus"""
    
    # 检查输入文件是否存在
    if not Path(INPUT_FILE).exists():
        print(f"错误: 输入文件不存在: {INPUT_FILE}")
        return
    
    # 读取数据
    print(f"正在读取文件: {INPUT_FILE}")
    data = load_jsonl(INPUT_FILE)
    print(f"共读取 {len(data)} 条数据")
    
    # 验证数据包含 embedding 字段
    if not data or 'embedding' not in data[0]:
        print("错误: 数据中不包含 embedding 字段，请先运行 embedding.py")
        return
    
    # 验证向量维度
    if len(data[0]['embedding']) != EMBEDDING_DIM:
        print(f"错误: 向量维度不匹配，期望 {EMBEDDING_DIM}，实际 {len(data[0]['embedding'])}")
        return
    
    try:
        # 连接 Milvus
        connect_milvus()
        
        # 创建 Collection
        collection = create_collection()
        
        # 插入数据
        insert_data(collection, data)
        
        # 创建索引
        create_index(collection)
        
        # 加载 Collection 到内存（用于后续查询）
        print("正在加载 Collection 到内存...")
        collection.load()
        print("Collection 加载完成")
        
        # 打印统计信息
        print("\n========== 完成 ==========")
        print(f"Collection: {COLLECTION_NAME}")
        print(f"数据总量: {collection.num_entities}")
        print(f"向量维度: {EMBEDDING_DIM}")
        print(f"索引类型: {INDEX_TYPE}")
        print(f"度量类型: {METRIC_TYPE}")
        
    except Exception as e:
        print(f"错误: {e}")
        raise
    finally:
        # 断开连接
        connections.disconnect("autotable")
        print("Milvus 连接已断开")


if __name__ == '__main__':
    store_vectors()

