from MemoryVG import Memory
from openai import OpenAI
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 从环境变量中获取 API 密钥
os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")

os.environ["OPENAI_BASE_URL"] = "http://35.220.164.252:3888/v1/"

client = OpenAI(
    base_url="http://35.220.164.252:3888/v1/",
    # base_url="http://34.13.73.248:3888/v1", # 谷歌负载均衡网络，全球节点，适合国外访问
    # base_url="https://api.boyuerichdata.opensphereai.com/v1", # 直连香港https，数据加密
    api_key=os.environ["OPENAI_API_KEY"],
)
# --------------------------
# 2. hetamem核心配置（同时启用Neo4j和Milvus）
# --------------------------
config = {
    "embedder": {
        "provider": "openai",
        "config": {"model": "text-embedding-3-large", "embedding_dims": 1024},
    },
    "graph_store": {
        "provider": "neo4j",
        "config": {
            "url": "bolt://10.1.48.31:7687",#换成自己的主机地址
            "username": "neo4j",
            "password": "neo4j2025",
        },
    },
    "vector_store": {
        "provider": "milvus",
        "config": {
            "collection_name": "hetamem",
            "embedding_model_dims": 1024,
            "url": "http://10.1.48.31:19530",
            "metric_type": "COSINE",
        },
    },
    "version": "v1.1",
}


m = Memory.from_config(config_dict=config)
res = m.add(
    messages="I am working on improving my tennis skills.",
    user_id="alice",
    infer = False,
)
# {'results': , 'relations': }
# result是id标识的向量记忆，relations是以图谱形式存储的关系
# add 默认开启infer，根据prompt定义好的记忆规则，从文本中提取记忆和关系。
# 可以设置infer=False，直接将原文本当做记忆插入

print(f"更新前的记忆库结果：{m.get_all(user_id='alice')}\n")
memory_id = res["results"][0]["id"] #只能根据id更新
m.update(memory_id=memory_id, data="Likes to play basketball")
print(f"更新后的记忆库结果：{m.get_all(user_id='alice')}\n")

print(f"查询某条记忆的更新记录：{m.history(memory_id=memory_id)}\n")
m.delete(memory_id=memory_id)
print(f"删除记忆后的结果：{m.get_all(user_id='alice')}\n")


movie_messages = [
    {
        "role": "user",
        "content": "I'm planning to watch a movie tonight. Any recommendations?",
    },
    {
        "role": "assistant",
        "content": "How about a thriller movies? They can be quite engaging.",
    },
    {
        "role": "user",
        "content": "I'm not a big fan of thriller movies but I love sci-fi movies.",
    },
]

print("==============start==================")
res = m.add(
    messages=movie_messages,
    user_id="alice",
)
search1_result = m.search(query="what does alice love?", user_id="alice", limit=3)
for item in search1_result.get("results"):
    memory = item.get("memory","")
    score = item.get("score")
    print(f"memory: {memory}, score: {score}")
print("===============end===================")