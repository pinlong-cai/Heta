"""
文本块向量化脚本
使用 sentence_transformers 或远程 API 对 JSONL 文件中的文本进行向量化
"""

import json
from pathlib import Path
from sentence_transformers import SentenceTransformer
import requests
import numpy as np
from tqdm import tqdm


# ==================== 配置参数 ====================
INPUT_FILE = "src/table_flow/data/top_100.jsonl"
OUTPUT_FILE = "src/table_flow/data/top_100_embeddings.jsonl"
MODEL_PATH = "/home/fanwenzhuo/Documents/models/bge-m3"
BATCH_SIZE = 32

# 远程 Embedding 配置
USE_REMOTE_EMBEDDING = True  # 切换为 True 使用远程 API
REMOTE_EMBEDDING_API_KEY = "sk-hgmmcqrpuezfywnaimvouswzqfphafszqrciqbfjepvsonjo"
REMOTE_EMBEDDING_BASE_URL = "https://api.siliconflow.cn/v1"
REMOTE_EMBEDDING_MODEL = "BAAI/bge-m3"
REMOTE_BATCH_SIZE = 10  # 远程 API 批处理大小(通常比本地小)
# =================================================


def load_jsonl(file_path: str) -> list[dict]:
    """读取 JSONL 文件"""
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                data.append(json.loads(line))
    return data


def save_jsonl(data: list[dict], file_path: str) -> None:
    """保存为 JSONL 文件"""
    with open(file_path, 'w', encoding='utf-8') as f:
        for item in data:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')


def get_remote_embeddings_batch(texts: list[str]) -> list[list[float]]:
    """
    批量调用远程 embedding API
    
    Args:
        texts: 文本列表
    
    Returns:
        embedding 列表
    """
    headers = {
        "Authorization": f"Bearer {REMOTE_EMBEDDING_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": REMOTE_EMBEDDING_MODEL,
        "input": texts,
        "encoding_format": "float"
    }
    
    response = requests.post(
        f"{REMOTE_EMBEDDING_BASE_URL}/embeddings",
        headers=headers,
        json=payload,
        timeout=60
    )
    
    response.raise_for_status()
    data = response.json()["data"]
    
    # 提取并归一化 embeddings
    embeddings = []
    for item in sorted(data, key=lambda x: x["index"]):
        embedding = np.array(item["embedding"])
        embedding = embedding / np.linalg.norm(embedding)
        embeddings.append(embedding.tolist())
    
    return embeddings


def embed_chunks() -> None:
    """对 JSONL 文件中的文本块进行向量化"""
    
    # 检查输入文件是否存在
    if not Path(INPUT_FILE).exists():
        print(f"错误: 输入文件不存在: {INPUT_FILE}")
        return
    
    # 读取数据
    print(f"正在读取文件: {INPUT_FILE}")
    data = load_jsonl(INPUT_FILE)
    print(f"共读取 {len(data)} 条数据")
    
    # 提取所有 content 用于向量化
    contents = [item['content'] for item in data]
    
    if USE_REMOTE_EMBEDDING:
        # 使用远程 API
        print("正在使用远程 API 进行向量化...")
        all_embeddings = []
        
        # 分批处理
        for i in tqdm(range(0, len(contents), REMOTE_BATCH_SIZE)):
            batch = contents[i:i + REMOTE_BATCH_SIZE]
            batch_embeddings = get_remote_embeddings_batch(batch)
            all_embeddings.extend(batch_embeddings)
        
        embeddings = all_embeddings
        
    else:
        # 使用本地模型
        print(f"正在加载模型: {MODEL_PATH}")
        model = SentenceTransformer(MODEL_PATH, device='cpu')
        
        print("正在进行向量化...")
        embeddings = model.encode(
            contents,
            batch_size=BATCH_SIZE,
            show_progress_bar=True,
            normalize_embeddings=True
        )
        embeddings = [emb.tolist() for emb in embeddings]
    
    # 将 embedding 添加到原数据中
    for item, embedding in zip(data, embeddings):
        item['embedding'] = embedding
    
    # 保存结果
    print(f"正在保存结果到: {OUTPUT_FILE}")
    save_jsonl(data, OUTPUT_FILE)
    print(f"向量化完成！共处理 {len(data)} 条数据")


if __name__ == '__main__':
    embed_chunks()