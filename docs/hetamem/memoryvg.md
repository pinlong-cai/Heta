# MemoryVG

MemoryVG is HetaMem's episodic memory layer, built on **mem0** and **Milvus**.
Adding messages triggers a two-pass LLM process: first extracting new facts,
then comparing them against existing memories to decide which to add, update,
or delete — an incremental merge rather than a simple append. Later queries use
semantic similarity to recall the most relevant facts.

---

## How It Works

1. **Add** — pass a list of `messages` (same format as a chat completion).
   The system makes two LLM calls: one to extract a list of facts from the
   messages, and a second to compare each new fact against existing similar
   memories and decide whether to ADD, UPDATE, or DELETE.
2. **Search** — pass a natural-language query. Milvus returns the most similar
   facts ranked by cosine similarity. Filter by `score > 0.85` to accept only
   high-confidence recalls.
3. **CRUD** — every stored fact has a unique `memory_id`. You can retrieve,
   update, delete, or audit its full history individually.

---

## Adding Memories

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

**Response**

```json
{
  "results": [
    { "id": "a1b2c3d4-...", "memory": "Prefers concise Python examples", "event": "ADD" }
  ]
}
```

!!! tip
    You can also add a single assistant message to cache an answer for fast
    recall later — no user turn required.

---

## Searching Memories

```bash
curl -X POST http://localhost:8000/api/v1/hetamem/vg/search \
  -H "Content-Type: application/json" \
  -d '{"query": "user coding preferences", "agent_id": "agent"}'
```

**Response**

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

## Full CRUD Endpoint Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/hetamem/vg/add` | Extract facts from messages and incrementally merge with existing memories (ADD / UPDATE / DELETE) |
| `POST` | `/api/v1/hetamem/vg/search` | Semantic search over stored memories |
| `GET`  | `/api/v1/hetamem/vg` | List all memories for a scope |
| `GET`  | `/api/v1/hetamem/vg/{memory_id}` | Retrieve a single memory by ID |
| `GET`  | `/api/v1/hetamem/vg/{memory_id}/history` | Audit log of changes to a memory |
| `PUT`  | `/api/v1/hetamem/vg/{memory_id}` | Overwrite a memory's text |
| `DELETE` | `/api/v1/hetamem/vg/{memory_id}` | Delete a specific memory |
| `DELETE` | `/api/v1/hetamem/vg` | Delete all memories in a scope |

All endpoints accept `agent_id`, `user_id`, and/or `run_id` to select the
correct scope.

---

## Listing Memories

```bash
curl "http://localhost:8000/api/v1/hetamem/vg?agent_id=agent"
```

---

## Updating a Memory

```bash
curl -X PUT http://localhost:8000/api/v1/hetamem/vg/a1b2c3d4-... \
  -H "Content-Type: application/json" \
  -d '{"data": "Prefers concise Python examples with type hints"}'
```

---

## Deleting a Memory

```bash
curl -X DELETE "http://localhost:8000/api/v1/hetamem/vg/a1b2c3d4-..."
```

---

## Viewing History

```bash
curl "http://localhost:8000/api/v1/hetamem/vg/a1b2c3d4-.../history"
```

Returns a timestamped audit log of every `ADD`, `UPDATE`, and `DELETE` event
for that memory.
