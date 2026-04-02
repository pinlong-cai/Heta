# HetaGen

!!! warning "早期阶段"
    HetaGen 正处于积极开发中，API 和输出格式可能在版本间发生变化。未经充分测试，不建议在生产环境中使用。

HetaGen 是 Heta 的知识库驱动结构化内容生成层。它将自然语言问题和知识库内容转化为结构化输出：表格、层级标签树以及可执行的 SQL。

---

## 功能概览

| 功能 | 说明 |
|------|------|
| **表格合成** | 根据问题查询知识库，生成带有模式定义和 CSV 数据的结构化表格 |
| **Text-to-SQL** | 针对合成的表格生成并执行 SQL 查询 |
| **标签树构建** | 为领域主题生成层级知识树，可选择以 HetaDB 知识库作为背景 |

---

## 表格合成与 Text-to-SQL

表格流水线以异步任务形式运行。提交问题后获取 `task_id`，再轮询获取结果。

### 提交任务

```bash
curl -X POST http://localhost:8000/api/v1/hetagen/pipeline/submit \
  -H "Content-Type: application/json" \
  -d '{
    "question":     "List the top 10 NASDAQ companies by market cap",
    "sql_question": null,
    "top_k":        5,
    "threshold":    0.5
  }'
```

**响应**

```json
{ "task_id": "abc123", "status": "pending", "message": "Task submitted" }
```

### 轮询结果

```bash
curl http://localhost:8000/api/v1/hetagen/pipeline/status/abc123
```

**响应（已完成）**

```json
{
  "task_id": "abc123",
  "status": "completed",
  "result": {
    "table_name": "nasdaq_top10",
    "schema": { "title": "...", "entities": [], "columns": [] },
    "csv_data": [{ "Company": "Apple", "Market Cap (USD)": "3.2T" }],
    "sql": "SELECT * FROM nasdaq_top10 ORDER BY market_cap DESC LIMIT 10",
    "query_results": []
  }
}
```

### 流式进度（WebSocket）

```
WS /api/v1/hetagen/pipeline/stream
```

连接后，发送与提交接口相同的 JSON 请求体。服务端会实时推送 `progress`、`result` 和 `error` 消息。

---

## 标签树构建

为领域主题生成层级知识树，支持两种模式：

| 模式 | 说明 |
|------|------|
| `kb` | 基于真实知识库实体生成树，需提供 `kb_name`。对已有领域的描述更准确。 |
| `pure_llm` | 仅使用 LLM 世界知识，无需知识库依赖，速度更快。 |

### 提交标签树任务

```bash
curl -X POST http://localhost:8000/api/v1/hetagen/tag-tree/generate \
  -H "Content-Type: application/json" \
  -d '{
    "topic":   "糖尿病诊疗",
    "mode":    "kb",
    "kb_name": "medical_kb"
  }'
```

**响应**

```json
{ "task_id": "xyz789", "status": "pending", "message": "Tree generation started (mode=kb)" }
```

### 轮询结果

```bash
curl http://localhost:8000/api/v1/hetagen/tag-tree/status/xyz789
```

---

## API 接口参考

| 方法 | 接口路径 | 说明 |
|------|----------|------|
| `POST` | `/api/v1/hetagen/pipeline/submit` | 提交表格 + SQL 生成任务 |
| `GET`  | `/api/v1/hetagen/pipeline/status/{task_id}` | 轮询任务状态 / 结果 |
| `WS`   | `/api/v1/hetagen/pipeline/stream` | 流式执行流水线 |
| `POST` | `/api/v1/hetagen/tag-tree/generate` | 提交知识树生成任务 |
| `GET`  | `/api/v1/hetagen/tag-tree/status/{task_id}` | 轮询树任务状态 / 结果 |
| `POST` | `/api/v1/hetagen/tag-tree/submit` | （旧版）从 Excel 文件解析标签树 |
