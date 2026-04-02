# MemoryVG

MemoryVG 是 HetaMem 的情节记忆层，基于 **mem0** 和 **Milvus** 构建。添加消息时，系统通过两次 LLM 调用完成增量式记忆合并：先提取新事实，再与已有记忆对比决定增删改操作。后续查询通过语义相似度召回最相关的事实。

---

## 工作原理

1. **添加** — 传入 `messages` 列表（格式与聊天补全相同）。系统先调用 LLM 提取事实列表，再对每条新事实搜索已有相似记忆，最后由 LLM 决定对每条记忆执行 ADD、UPDATE 还是 DELETE，实现增量合并而非简单追加。
2. **搜索** — 传入自然语言查询，Milvus 按余弦相似度返回最相关的事实排名。建议以 `score > 0.85` 作为高置信度召回的筛选阈值。
3. **增删改查** — 每条存储的事实都有唯一的 `memory_id`，支持单独读取、更新、删除或查看完整变更历史。

---

## 添加记忆

```bash
curl -X POST http://localhost:8000/api/v1/hetamem/vg/add \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      {"role": "user",      "content": "I prefer concise Python examples."},
      {"role": "assistant", "content": "Noted."}
    ],
    "agent_id": "agent"
  }'
```

**响应**

```json
{
  "results": [
    { "id": "a1b2c3d4-...", "memory": "Prefers concise Python examples", "event": "ADD" }
  ]
}
```

!!! tip
    也可以只传入单条 assistant 消息来缓存一个答案，以便后续快速召回 — 无需附带 user 消息。

---

## 搜索记忆

```bash
curl -X POST http://localhost:8000/api/v1/hetamem/vg/search \
  -H "Content-Type: application/json" \
  -d '{"query": "user coding preferences", "agent_id": "agent"}'
```

**响应**

```json
{
  "results": [
    {
      "id": "a1b2c3d4-...",
      "memory": "Prefers concise Python examples",
      "score": 0.91
    }
  ]
}
```

---

## 完整 CRUD 接口参考

| 方法 | 接口路径 | 说明 |
|------|----------|------|
| `POST` | `/api/v1/hetamem/vg/add` | 从消息提取事实，与已有记忆增量合并（ADD / UPDATE / DELETE） |
| `POST` | `/api/v1/hetamem/vg/search` | 对已存储记忆进行语义搜索 |
| `GET`  | `/api/v1/hetamem/vg` | 列出某作用域下的所有记忆 |
| `GET`  | `/api/v1/hetamem/vg/{memory_id}` | 按 ID 获取单条记忆 |
| `GET`  | `/api/v1/hetamem/vg/{memory_id}/history` | 查看某条记忆的变更审计日志 |
| `PUT`  | `/api/v1/hetamem/vg/{memory_id}` | 覆盖更新某条记忆的文本内容 |
| `DELETE` | `/api/v1/hetamem/vg/{memory_id}` | 删除指定记忆 |
| `DELETE` | `/api/v1/hetamem/vg` | 删除某作用域下的全部记忆 |

所有接口均接受 `agent_id`、`user_id` 和/或 `run_id` 来选择正确的作用域。

---

## 列出记忆

```bash
curl "http://localhost:8000/api/v1/hetamem/vg?agent_id=agent"
```

---

## 更新记忆

```bash
curl -X PUT http://localhost:8000/api/v1/hetamem/vg/a1b2c3d4-... \
  -H "Content-Type: application/json" \
  -d '{"data": "Prefers concise Python examples with type hints"}'
```

---

## 删除记忆

```bash
curl -X DELETE "http://localhost:8000/api/v1/hetamem/vg/a1b2c3d4-..."
```

---

## 查看变更历史

```bash
curl "http://localhost:8000/api/v1/hetamem/vg/a1b2c3d4-.../history"
```

返回该条记忆所有 `ADD`、`UPDATE`、`DELETE` 事件的带时间戳审计日志。
