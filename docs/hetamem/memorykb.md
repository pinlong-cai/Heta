# MemoryKB

MemoryKB is HetaMem's long-term knowledge graph layer, built on **LightRAG**
(NanoVectorDB + NetworkX). Unlike MemoryVG which stores individual facts,
MemoryKB builds a rich graph of entities and relations that grows as the agent
accumulates knowledge across sessions.

---

## How It Works

1. **Insert** — submit text describing knowledge the agent wants to retain
   (e.g., a synthesised answer, a research finding, or a domain explanation).
   LightRAG extracts entities and relations, merges them into the existing graph,
   and persists everything to local storage.
2. **Index** — graph construction is **asynchronous** and takes approximately
   200 seconds. The endpoint returns immediately; the knowledge becomes
   queryable after indexing completes.
3. **Query** — ask a natural-language question. LightRAG retrieves relevant
   graph neighbourhoods and synthesises an answer.

---

## Inserting Knowledge

```bash
curl -X POST http://localhost:8000/api/v1/hetamem/kb/insert \
  -F "query=Alice likes football"
```

**Response** (HTTP 202)

```json
{
  "id": "a1b2c3d4-...",
  "query": "Alice likes football",
  "status": "accepted",
  "videocaption": null,
  "audiocaption": null,
  "imagecaption": null
}
```

!!! warning "Async indexing delay"
    Insertion triggers an asynchronous LightRAG graph construction job that
    takes approximately **200 seconds**. Do not query immediately after
    inserting — newly added knowledge will not be visible until the job
    completes.

---

## Querying the Knowledge Graph

```bash
curl -X POST http://localhost:8000/api/v1/hetamem/kb/query \
  -H "Content-Type: application/json" \
  -d '{"query": "What does Alice like?", "mode": "hybrid"}'
```

**Response**

```json
{
  "query": "What does Alice like?",
  "mode": "hybrid",
  "pm_used": false,
  "pm_memory": null,
  "pm_relevant": false,
  "rag_memory": "Based on the knowledge graph, Alice is associated with football ...",
  "final_answer": "According to memory, Alice likes football."
}
```

---

## Retrieval Modes

| `mode` | Strategy | Best for |
|--------|----------|---------|
| `hybrid` | Combines local and global graph search | General queries; recommended default |
| `local` | Traverses the immediate neighbourhood of matched entities | Detailed questions about specific entities |
| `global` | Searches the relationship vector store with high-level concept keywords, then resolves entities from matched relations | Broad thematic or relational questions |
| `naive` | Plain vector search without graph structure | Quick baseline comparison |

The query endpoint also accepts a `use_pm` parameter (default `false`). This is an experimental feature that queries a separately-deployed parametric memory service before falling back to RAG retrieval. Disabled by default; no configuration required for normal use.

```bash
# Local mode — detailed entity-level answer
curl -X POST http://localhost:8000/api/v1/hetamem/kb/query \
  -H "Content-Type: application/json" \
  -d '{"query": "What sport does Alice like?", "mode": "local"}'

# Global mode — broad thematic answer
curl -X POST http://localhost:8000/api/v1/hetamem/kb/query \
  -H "Content-Type: application/json" \
  -d '{"query": "What people are mentioned in the knowledge base?", "mode": "global"}'
```

---

## When to Use MemoryKB

| Use MemoryKB when… | Avoid MemoryKB when… |
|---|---|
| Accumulating domain knowledge across agent restarts | You need instant recall — use MemoryVG instead |
| Building a growing entity/relation graph | The knowledge is ephemeral or session-specific |
| Querying with broad thematic questions | You need facts from uploaded human documents — use HetaDB |

!!! tip
    The recommended pattern is to use MemoryVG as a fast cache and MemoryKB
    for durable long-term knowledge. After receiving an answer from HetaDB,
    store the key finding in MemoryVG for quick recall, and insert it into
    MemoryKB when it represents knowledge the agent should carry forward
    indefinitely.
