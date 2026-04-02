# HetaGen

!!! warning "Early stage"
    HetaGen is under active development. APIs and output formats may change
    between releases. It is not recommended for production use without
    thorough testing.

HetaGen is Heta's knowledge-base-driven structured content generation layer.
It turns natural-language questions and knowledge-base content into structured
outputs: tables, hierarchical tag trees, and executable SQL.

---

## Capabilities

| Capability | Description |
|---|---|
| **Table synthesis** | Given a question, queries the knowledge base and generates a structured table with a schema and CSV data |
| **Text-to-SQL** | Generates and executes SQL against the synthesised table |
| **Tag-tree construction** | Generates a hierarchical knowledge tree for a domain topic, optionally grounded in a HetaDB knowledge base |

---

## Table Synthesis and Text-to-SQL

The table pipeline runs as an async task. Submit a question, receive a
`task_id`, then poll for results.

### Submit a task

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

**Response**

```json
{ "task_id": "abc123", "status": "pending", "message": "Task submitted" }
```

### Poll for results

```bash
curl http://localhost:8000/api/v1/hetagen/pipeline/status/abc123
```

**Response (completed)**

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

### Stream progress (WebSocket)

```
WS /api/v1/hetagen/pipeline/stream
```

After connecting, send the same JSON body as the submit endpoint. The server
streams `progress`, `result`, and `error` messages in real time.

---

## Tag-Tree Construction

Generate a hierarchical knowledge tree for a domain topic. Supports two modes:

| Mode | Description |
|------|-------------|
| `kb` | Grounds the tree in real KB entities — requires `kb_name`. More accurate for known domains. |
| `pure_llm` | Uses LLM world knowledge only. No KB dependency; faster. |

### Submit a tag-tree task

```bash
curl -X POST http://localhost:8000/api/v1/hetagen/tag-tree/generate \
  -H "Content-Type: application/json" \
  -d '{
    "topic":   "Diabetes diagnosis and treatment",
    "mode":    "kb",
    "kb_name": "medical_kb"
  }'
```

**Response**

```json
{ "task_id": "xyz789", "status": "pending", "message": "Tree generation started (mode=kb)" }
```

### Poll for results

```bash
curl http://localhost:8000/api/v1/hetagen/tag-tree/status/xyz789
```

---

## API Endpoint Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/hetagen/pipeline/submit` | Submit table + SQL generation task |
| `GET`  | `/api/v1/hetagen/pipeline/status/{task_id}` | Poll task status / result |
| `WS`   | `/api/v1/hetagen/pipeline/stream` | Stream pipeline execution |
| `POST` | `/api/v1/hetagen/tag-tree/generate` | Submit knowledge tree generation |
| `GET`  | `/api/v1/hetagen/tag-tree/status/{task_id}` | Poll tree task status / result |
| `POST` | `/api/v1/hetagen/tag-tree/submit` | *(legacy)* Parse tag tree from Excel file |
