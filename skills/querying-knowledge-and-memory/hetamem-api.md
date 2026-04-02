# HetaMem API — Full Reference

Base URL: `http://<host>:8000`

Two independent systems under HetaMem:
- **MemoryVG** `/api/v1/hetamem/vg/*` — personal episodic memory (mem0-based)
- **MemoryKB** `/api/v1/hetamem/kb/*` — agent knowledge graph (LightRAG-based)

---

## MemoryVG — Personal Episodic Memory

### Add Memory

```
POST /api/v1/hetamem/vg/add
Content-Type: application/json
```

**Request:**
```json
{
  "messages": [
    {"role": "user", "content": "I prefer Python code examples."},
    {"role": "assistant", "content": "Noted, I'll use Python."}
  ],
  "agent_id": "agent",
  "metadata": null
}
```

| Field | Required | Notes |
|-------|----------|-------|
| `messages` | yes | List of `{role, content}` — LLM extracts facts automatically |
| `agent_id` | yes | Always `"agent"` in v1. Must match the value used in search. |
| `metadata` | no | Additional key-value tags |

**Response:** Returns extracted memory entries with event type (ADD / UPDATE) and graph relations.
```json
{
  "results": [
    { "id": "uuid", "memory": "Prefers Python code examples", "event": "ADD" }
  ],
  "relations": { "added_entities": [...], "deleted_entities": [...] }
}
```

> Memory is available for search **immediately** after add.

---

### Search Memory

```
POST /api/v1/hetamem/vg/search
Content-Type: application/json
```

**Request:**
```json
{
  "query": "What are this agent's preferences?",
  "agent_id": "agent",
  "limit": 10,
  "threshold": null
}
```

| Field | Required | Default | Notes |
|-------|----------|---------|-------|
| `query` | yes | — | Natural language search |
| `agent_id` | yes | — | Always `"agent"` in v1. Must match the value used at add time. |
| `limit` | no | `10` | Max results |
| `threshold` | no | null | Minimum similarity score (0–1) |

**Response:**
```json
{
  "results": [
    { "id": "uuid", "memory": "Prefers Python code examples", "score": 0.82, "user_id": "agent-001" }
  ],
  "relations": [...]
}
```

Read `results[].memory` for recalled facts. `score` indicates similarity (higher = more relevant).

---

### Other CRUD Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/v1/hetamem/vg?user_id=X` | List all memories for scope |
| `GET` | `/api/v1/hetamem/vg/{id}` | Get single memory by ID |
| `GET` | `/api/v1/hetamem/vg/{id}/history` | Full modification history |
| `PUT` | `/api/v1/hetamem/vg/{id}` | Update memory: `{"data": "new text"}` |
| `DELETE` | `/api/v1/hetamem/vg/{id}` | Delete single memory |
| `DELETE` | `/api/v1/hetamem/vg?user_id=X` | Delete all memories for scope |

---

## MemoryKB — Agent Knowledge Graph

### Insert Knowledge

```
POST /api/v1/hetamem/kb/insert
Content-Type: multipart/form-data
```

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `query` | string (form) | yes | Text to add to knowledge graph |
| `image` | file | no | Captioned and merged into text |
| `audio` | file | no | Transcribed and merged |
| `video` | file | no | Captioned and merged |

**Response (202 Accepted):**
```json
{ "id": "uuid", "query": "...", "status": "accepted" }
```

> **Async** — LightRAG entity extraction and graph construction runs in background.
> Content takes approximately **200 seconds** to become searchable after insert.
> Do NOT query immediately after insert.

---

### Query Knowledge Graph

```
POST /api/v1/hetamem/kb/query
Content-Type: application/json
```

**Request:**
```json
{
  "query": "What machine learning concepts has the agent encountered?",
  "mode": "hybrid",
  "use_pm": false
}
```

| Field | Required | Default | Notes |
|-------|----------|---------|-------|
| `query` | yes | — | Natural language question |
| `mode` | no | `"hybrid"` | Retrieval strategy (see below) |
| `use_pm` | no | `false` | Also query parametric memory before RAG |

**Modes:**

| Mode | When to use |
|------|-------------|
| `hybrid` | Default — balances entity detail and thematic context |
| `local` | Specific entity recall (people, concepts, events) |
| `global` | High-level patterns and themes |
| `naive` | Simple chunk similarity, fastest |

`use_pm: true` — queries a parametric memory model first; useful if the KB has been extensively populated.

**Response:**
```json
{
  "query": "...",
  "mode": "hybrid",
  "pm_used": false,
  "pm_memory": null,
  "pm_relevant": false,
  "rag_memory": "Retrieved knowledge graph context...",
  "final_answer": "The agent has accumulated knowledge about neural networks, deep learning..."
}
```

Read `final_answer`. If `rag_memory` is empty or `final_answer` expresses uncertainty, consider querying HetaDB instead.
