---
name: querying-knowledge-and-memory
description: Use when the agent needs to answer questions from stored documents, recall past observations or context, store new knowledge for later, or discover available knowledge bases. Triggers on any of: "search the knowledge base", "what do you know about X", "remember this", "recall what happened", "find in documents", "store this observation", "look it up", "do you remember". Covers HetaDB (human document KBs), MemoryKB (agent knowledge graph), and MemoryVG (personal episodic memory).
---

# Querying Knowledge and Memory

## Three Systems — Know Which to Use

| System | Built by | Role |
|--------|----------|------|
| **HetaDB** | Humans (file upload) | Multimodal document knowledge bases |
| **MemoryKB** | Agent (text insert) | Agent's own accumulating knowledge graph |
| **MemoryVG** | Agent (conversation) | Fast personal memory & cross-session cache |

Base URL: `http://<host>:8000`

---

## Orchestration — Always Follow This Order

**When answering a knowledge question:**

```
Step 1 — Search MemoryVG (fast personal recall)
  → Answer is clear and specific  →  return it, done
  → Vague or absent               →  continue to Step 2

Step 2 — Query HetaDB (human knowledge base)
  → Synthesised answer + citations

Step 3 — Agent decides what to store:
  → Quick recall next time?  →  MemoryVG add
  → Worth accumulating?      →  MemoryKB insert
  → Ephemeral / trivial?     →  skip
```

---

## API Summary

### HetaDB — Document Knowledge Bases

```
GET  /api/v1/hetadb/files/knowledge-bases
```
→ Returns `data[].name` — use as `kb_id`. Skip KBs with `status: "deleting"`; all others are queryable.

```
POST /api/v1/hetadb/chat
{ "query": "...", "kb_id": "...", "user_id": "agent", "query_mode": "naive" }
```
→ Required: `query`, `kb_id`. `user_id` is always `"agent"`. Read `response` field from result.
→ Full query mode guide: see `hetadb-api.md`

---

### MemoryVG — Personal Memory Cache

```
POST /api/v1/hetamem/vg/search
{ "query": "...", "agent_id": "agent" }
```
→ `agent_id` is always `"agent"`. Read `results[].memory`.

```
POST /api/v1/hetamem/vg/add
{ "messages": [{"role": "assistant", "content": "..."}], "agent_id": "agent" }
```
→ `agent_id` is always `"agent"`. LLM extracts facts from messages. Available immediately.

---

### MemoryKB — Agent Knowledge Graph

```
POST /api/v1/hetamem/kb/insert          (multipart/form-data)
query=<text to add to knowledge graph>
```
→ **Async** — LightRAG graph construction takes ~200s. Do not query immediately.

```
POST /api/v1/hetamem/kb/query
{ "query": "...", "mode": "hybrid" }
```
→ Read `final_answer` from result.

---

## When NOT to Use Each

| Don't use | For |
|-----------|-----|
| HetaDB | Storing agent observations (no insert API) |
| MemoryKB | Quick recall — async delay makes it unsuitable as cache |
| MemoryVG | Deep multi-document reasoning — use HetaDB instead |

---

Full parameter reference: `hetadb-api.md`, `hetamem-api.md`
