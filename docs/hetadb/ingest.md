# Ingesting Documents

This page walks through every step required to go from a raw file to a queryable knowledge base.

---

## Step 1 — Create a Knowledge Base

A knowledge base (KB) is a named container that groups one or more datasets.

```bash
curl -X POST http://localhost:8000/api/v1/hetadb/files/knowledge-bases \
  -H "Content-Type: application/json" \
  -d '{"name": "research_kb"}'
```

**Response**

```json
{ "success": true, "message": "Knowledge base 'research_kb' created" }
```

!!! tip
    KB names may only contain letters, digits, and underscores (`^[A-Za-z0-9_]+$`), max 64 characters.
    Use descriptive names — you will reference `kb_id` in every subsequent request.

---

## Step 2 — Create a Dataset and Upload Files

Files are uploaded to *datasets* in `raw_files`, independent of any knowledge base. A dataset can then be parsed into one or more KBs.

**Create the dataset:**

```bash
curl -X POST http://localhost:8000/api/v1/hetadb/files/raw-files/datasets \
  -H "Content-Type: application/json" \
  -d '{"name": "paper"}'
```

**Upload a file:**

```bash
curl -X POST http://localhost:8000/api/v1/hetadb/files/raw-files/datasets/paper/file \
  -F "file=@paper.pdf"
```

**Response**

```json
{
  "success": true,
  "message": "File uploaded",
  "data": { "filename": "paper.pdf", "size": 102400 }
}
```

!!! tip
    For large files use the chunked upload flow: initialise a session with
    `POST /api/v1/hetadb/files/raw-files/datasets/{dataset}/upload/init`,
    stream parts to `.../upload/{upload_id}/chunk`, then finalise with
    `.../upload/{upload_id}/complete`.

---

## Step 3 — Trigger Parsing

Submit a parse job to process one or more datasets into the knowledge base.
The request returns immediately — parsing runs in the background.

```bash
curl -X POST http://localhost:8000/api/v1/hetadb/files/knowledge-bases/research_kb/parse \
  -H "Content-Type: application/json" \
  -d '{"datasets": ["paper"]}'
```

**Response**

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

Save the `task_id` — you will need it to poll parse progress.

!!! warning
    Do not issue chat queries until parsing has completed. Querying a partially indexed KB may return incomplete or empty results.

---

## Step 4 — Check Status

Poll the task endpoint with the `task_id` returned above.

```bash
curl http://localhost:8000/api/v1/hetadb/files/processing/tasks/abc123def456
```

**Response**

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

Wait until `status` is `"completed"`. Other terminal states are `"failed"` and `"cancelled"`.

Typical processing time depends on file size and complexity:

| Document size | Approximate time |
|---|---|
| Short PDF (< 10 pages) | 30–90 s |
| Medium report (10–50 pages) | 2–5 min |
| Large document (50+ pages) | 5–20 min |

---

## Step 5 — Query

Once status is `"completed"`, start chatting:

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

**Response**

```json
{
  "success": true,
  "response": "The paper makes three main contributions ...",
  "citations": [
    { "index": 1, "source_file": "paper.pdf", "dataset": "paper", "file_url": null }
  ]
}
```

See [Query Modes](query-modes.md) for details on all five retrieval strategies.

---

## Listing Knowledge Bases

```bash
curl http://localhost:8000/api/v1/hetadb/files/knowledge-bases
```

```json
{
  "success": true,
  "data": [{ "name": "research_kb", "created_at": "2026-03-30T10:00:00Z", "status": "ready" }]
}
```

Skip any KB whose `status` is `"deleting"` — it is not queryable.
