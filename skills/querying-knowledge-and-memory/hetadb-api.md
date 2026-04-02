# HetaDB API — Full Reference

Base URL: `http://<host>:8000`

---

## List Knowledge Bases

```
GET /api/v1/hetadb/files/knowledge-bases
```

**Response:**
```json
{
  "success": true,
  "data": [
    { "name": "finance-reports", "created_at": "2026-03-01T10:00:00Z", "status": "ready" }
  ]
}
```

Use `data[].name` as `kb_id`. `status` has two values: `"ready"` (queryable) and `"deleting"` (skip). There is no processing/pending status at the KB level — dataset parse progress is tracked separately via task IDs.

---

## Chat (Document Query)

```
POST /api/v1/hetadb/chat
Content-Type: application/json
```

**Request fields:**

| Field | Type | Required | Default | Notes |
|-------|------|----------|---------|-------|
| `query` | string | yes | — | Max 4096 chars |
| `kb_id` | string | yes | — | From list endpoint |
| `user_id` | string | yes | — | Any stable agent/user identifier |
| `query_mode` | string | no | `"naive"` | See modes below |
| `max_results` | int | no | `20` | Max chunks retrieved |
| `top_k` | int | no | null | Vector search candidate pool size |

**Query modes:**

| Mode | When to use |
|------|-------------|
| `naive` | Simple factual lookup — fastest, good default |
| `rerank` | Need highest-precision chunks from large KBs |
| `rewriter` | Query is ambiguous or poorly phrased |
| `multihop` | Complex reasoning across multiple documents |
| `direct` | Skip retrieval entirely — pure LLM answer |

**Response:**
```json
{
  "success": true,
  "code": 200,
  "message": "OK",
  "response": "Q3 revenue was $4.2B, up 12% year-over-year...",
  "data": [
    {
      "kb_id": "finance-reports",
      "kb_name": "Finance Reports",
      "score": 0.87,
      "content": "...",
      "text": "...",
      "source_id": ["dataset-name"]
    }
  ],
  "citations": [
    {
      "index": 1,
      "source_file": "Q3_2025_report.pdf",
      "dataset": "finance-q3",
      "file_url": "https://..."
    }
  ],
  "total_count": 5,
  "request_id": "uuid",
  "query_info": {}
}
```

Key fields:
- `response` — LLM-synthesised answer
- `citations[].source_file` — source document name
- `citations[].file_url` — presigned download link (null if S3 not configured)

**Error response:**
```json
{ "success": false, "code": 400, "message": "kb_id must not be empty" }
```
