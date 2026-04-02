# 接入文档

本页介绍从原始文件到可查询知识库的完整操作步骤。

---

## 第一步 — 创建知识库

知识库（KB）是一个命名容器，用于组织一个或多个数据集。

```bash
curl -X POST http://localhost:8000/api/v1/hetadb/files/knowledge-bases \
  -H "Content-Type: application/json" \
  -d '{"name": "research_kb"}'
```

**响应**

```json
{ "success": true, "message": "Knowledge base 'research_kb' created" }
```

!!! tip
    知识库名称只能包含字母、数字和下划线（`^[A-Za-z0-9_]+$`），最长 64 个字符。
    建议使用有描述性的名称 — 后续所有查询请求都需要引用 `kb_id`。

---

## 第二步 — 创建数据集并上传文件

文件先上传至 `raw_files` 中的*数据集*，数据集独立于知识库存在，可在后续被解析入一个或多个知识库。

**创建数据集：**

```bash
curl -X POST http://localhost:8000/api/v1/hetadb/files/raw-files/datasets \
  -H "Content-Type: application/json" \
  -d '{"name": "paper"}'
```

**上传文件：**

```bash
curl -X POST http://localhost:8000/api/v1/hetadb/files/raw-files/datasets/paper/file \
  -F "file=@paper.pdf"
```

**响应**

```json
{
  "success": true,
  "message": "File uploaded",
  "data": { "filename": "paper.pdf", "size": 102400 }
}
```

!!! tip
    对于大文件，建议使用分块上传流程：先通过 `POST /api/v1/hetadb/files/raw-files/datasets/{dataset}/upload/init` 初始化会话，再逐块上传至 `.../upload/{upload_id}/chunk`，最后调用 `.../upload/{upload_id}/complete` 完成合并。

---

## 第三步 — 触发解析

向知识库提交解析任务，将一个或多个数据集处理入库。解析为**异步**操作 — 请求会立即返回，解析在后台进行。

```bash
curl -X POST http://localhost:8000/api/v1/hetadb/files/knowledge-bases/research_kb/parse \
  -H "Content-Type: application/json" \
  -d '{"datasets": ["paper"]}'
```

**响应**

```json
{
  "success": true,
  "message": "Processing started for 1 dataset(s)",
  "data": {
    "tasks": [{ "task_id": "abc123def456", "dataset": "paper" }],
    "mode": 0
  }
}
```

请保存返回的 `task_id`，后续查询解析进度时需要用到。

!!! warning
    在解析完成前请勿发起查询请求。对尚未完成索引的知识库进行查询可能返回不完整或空结果。

---

## 第四步 — 检查状态

使用上一步返回的 `task_id` 轮询任务状态。

```bash
curl http://localhost:8000/api/v1/hetadb/files/processing/tasks/abc123def456
```

**响应**

```json
{
  "task_id": "abc123def456",
  "task_type": "file_processing",
  "status": "completed",
  "progress": 100.0,
  "message": "Completed",
  "metadata": { "kb_name": "research_kb", "dataset": "paper", "mode": 0 },
  "created_at": "2026-03-30T10:00:00",
  "started_at": "2026-03-30T10:00:01",
  "completed_at": "2026-03-30T10:02:30",
  "error": null
}
```

等待 `status` 变为 `"completed"`。其他终态为 `"failed"`（失败）和 `"cancelled"`（已取消）。

处理时间因文件大小和复杂度而异：

| 文档规模 | 大致耗时 |
|---|---|
| 短篇 PDF（< 10 页） | 30–90 秒 |
| 中等报告（10–50 页） | 2–5 分钟 |
| 大型文档（50+ 页） | 5–20 分钟 |

---

## 第五步 — 查询

状态显示 `"completed"` 后即可开始对话：

```bash
curl -X POST http://localhost:8000/api/v1/hetadb/chat \
  -H "Content-Type: application/json" \
  -d '{
    "query":      "What are the main contributions?",
    "kb_id":      "research_kb",
    "user_id":    "agent",
    "query_mode": "rerank"
  }'
```

**响应**

```json
{
  "success": true,
  "response": "The paper makes three main contributions ...",
  "citations": [
    { "index": 1, "source_file": "paper.pdf", "dataset": "paper", "file_url": null }
  ]
}
```

五种检索策略的详细说明请参见 [查询模式](query-modes.zh.md)。

---

## 列出知识库

```bash
curl http://localhost:8000/api/v1/hetadb/files/knowledge-bases
```

```json
{
  "success": true,
  "data": [{ "name": "research_kb", "created_at": "2026-03-30T10:00:00Z", "status": "ready" }]
}
```

`status` 为 `"deleting"` 的知识库不可查询，请跳过。
