# MemoryKB

MemoryKB 是 HetaMem 的长期知识图谱层，基于 **LightRAG**（NanoVectorDB + NetworkX）构建。与存储独立事实的 MemoryVG 不同，MemoryKB 随 Agent 跨会话积累知识而持续构建一个包含实体和关系的丰富图谱。

---

## 工作原理

1. **插入** — 提交 Agent 希望保留的文本知识（例如综合答案、研究发现或领域解释）。LightRAG 从中提取实体和关系，将其合并到现有图谱，并持久化到本地存储。
2. **索引** — 图谱构建为**异步**操作，约需 200 秒。接口会立即返回；知识在索引完成后才可查询。
3. **查询** — 以自然语言提问，LightRAG 检索相关图谱邻域并综合生成答案。

---

## 插入知识

```bash
curl -X POST http://localhost:8000/api/v1/hetamem/kb/insert \
  -F "query=alice喜欢足球"
```

**响应**（HTTP 202）

```json
{
  "id": "a1b2c3d4-...",
  "query": "alice喜欢足球",
  "status": "accepted",
  "videocaption": null,
  "audiocaption": null,
  "imagecaption": null
}
```

!!! warning "异步索引延迟"
    插入操作会触发异步的 LightRAG 图谱构建任务，约需 **200 秒**。请勿在插入后立即查询 — 新增知识在任务完成前不可见。

---

## 查询知识图谱

```bash
curl -X POST http://localhost:8000/api/v1/hetamem/kb/query \
  -H "Content-Type: application/json" \
  -d '{"query": "alice喜欢什么", "mode": "hybrid"}'
```

**响应**

```json
{
  "query": "alice喜欢什么",
  "mode": "hybrid",
  "pm_used": false,
  "pm_memory": null,
  "pm_relevant": false,
  "rag_memory": "Based on the knowledge graph, Alice is associated with football ...",
  "final_answer": "根据记忆，alice 喜欢足球。"
}
```

---

## 检索模式

| `mode` | 策略 | 适用场景 |
|--------|------|---------|
| `hybrid` | 结合局部与全局图谱搜索 | 通用查询；推荐默认值 |
| `local` | 遍历匹配实体的直接邻居 | 关于特定实体的细节性问题 |
| `global` | 用高层概念词检索关系向量库，通过关系定位实体 | 宏观主题性或关系性问题 |
| `naive` | 朴素向量检索，不使用图谱结构 | 快速基线对比 |

查询接口还支持 `use_pm` 参数（默认 `false`）。这是实验性功能，开启后会在 RAG 检索前先查询参数化记忆服务（需单独部署），默认关闭、无需配置。

```bash
# 局部模式 — 实体级别的详细答案
curl -X POST http://localhost:8000/api/v1/hetamem/kb/query \
  -H "Content-Type: application/json" \
  -d '{"query": "alice喜欢什么运动", "mode": "local"}'

# 全局模式 — 宏观主题性答案
curl -X POST http://localhost:8000/api/v1/hetamem/kb/query \
  -H "Content-Type: application/json" \
  -d '{"query": "知识库里都记录了哪些人的信息", "mode": "global"}'
```

---

## 适用与不适用场景

| 适合使用 MemoryKB 的情况 | 不适合使用 MemoryKB 的情况 |
|---|---|
| 跨 Agent 重启积累领域知识 | 需要即时召回 — 请使用 MemoryVG |
| 构建持续增长的实体/关系图谱 | 知识为临时性或会话级别 |
| 宏观主题性问题查询 | 需要检索已上传人类文档的内容 — 请使用 HetaDB |

!!! tip
    推荐模式：将 MemoryVG 作为快速缓存，将 MemoryKB 用于持久化长期知识。从 HetaDB 获取答案后，将关键发现存入 MemoryVG 以便快速召回；若该知识是 Agent 应永久保留的，则同步写入 MemoryKB。
