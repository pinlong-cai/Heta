# Query Modes

HetaDB exposes five retrieval strategies via the `query_mode` field of
`POST /api/v1/hetadb/chat`. This page describes each mode, when to use it,
and provides a ready-to-run curl example.

---

## Overview

| `query_mode` | Strategy | Typical latency |
|---|---|---|
| `naive` | Parallel vector + KG retrieval, weighted scoring | Fastest |
| `rerank` | BM25 + vector RRF fusion → cross-encoder rerank | Medium |
| `rewriter` | LLM generates 3 query variants, parallel retrieval | Medium–slow |
| `multihop` | ReAct reasoning loop (max 3 rounds) | Slowest |
| `direct` | LLM only — no retrieval | Very fast |

---

## naive

**Strategy:** Runs vector retrieval and knowledge-graph retrieval in parallel,
then combines results with weighted scoring. No re-ranking step.

**When to use:** The default choice for most queries. Use it when you want fast
responses and the question is clearly stated.

```bash
curl -X POST http://localhost:8000/api/v1/hetadb/chat \
  -H "Content-Type: application/json" \
  -d '{
    "query":      "What is the abstract of this paper?",
    "kb_id":      "research_kb",
    "user_id":    "agent",
    "query_mode": "naive"
  }'
```

---

## rerank

**Strategy:** Combines BM25 keyword retrieval and dense vector retrieval using
Reciprocal Rank Fusion (RRF), then re-ranks the fused candidates with a
cross-encoder model. Produces the highest-precision results.

**When to use:** Factual questions, technical queries, or any case where
citation accuracy matters most.

```bash
curl -X POST http://localhost:8000/api/v1/hetadb/chat \
  -H "Content-Type: application/json" \
  -d '{
    "query":      "What loss function does the model use?",
    "kb_id":      "research_kb",
    "user_id":    "agent",
    "query_mode": "rerank"
  }'
```

!!! tip
    `rerank` is the recommended mode for production agents that need reliable
    citations. The extra latency is usually worth the precision gain.
    Cross-encoder reranking requires `hetadb.query_defaults.reranker_url` to be configured;
    without it the mode falls back to pure RRF ordering — still better than `naive` but
    without cross-encoder scoring.

---

## rewriter

**Strategy:** An LLM generates three paraphrased variants of the original
query. Each variant triggers independent retrieval; results are merged and
deduplicated before synthesis.

**When to use:** Ambiguous or under-specified queries where a single phrasing
may miss relevant chunks (e.g., jargon-heavy or informal user input).

```bash
curl -X POST http://localhost:8000/api/v1/hetadb/chat \
  -H "Content-Type: application/json" \
  -d '{
    "query":      "how does the thing handle sequences",
    "kb_id":      "research_kb",
    "user_id":    "agent",
    "query_mode": "rewriter"
  }'
```

---

## multihop

**Strategy:** Implements a ReAct (Reason + Act) loop that iterates up to three
retrieval rounds. After each round the LLM decides whether the accumulated
context is sufficient to answer or whether another retrieval step is needed.

**When to use:** Complex questions that require chaining multiple facts together
(e.g., "Compare the approaches used in sections 3 and 5 and explain which
performs better on dataset X").

```bash
curl -X POST http://localhost:8000/api/v1/hetadb/chat \
  -H "Content-Type: application/json" \
  -d '{
    "query":      "How does the proposed method compare to the baseline across all three datasets?",
    "kb_id":      "research_kb",
    "user_id":    "agent",
    "query_mode": "multihop"
  }'
```

!!! warning
    `multihop` is the slowest mode. Avoid it for simple factual lookups where
    `rerank` or `naive` will suffice.

---

## direct

**Strategy:** The LLM answers entirely from its parametric (pre-trained)
knowledge. No retrieval is performed. The `data[]` and `citations[]` fields
in the response will always be empty.

**When to use:** Quick LLM opinions, general knowledge questions unrelated to
your documents, or when you want to bypass the retrieval stack entirely for
testing purposes.

```bash
curl -X POST http://localhost:8000/api/v1/hetadb/chat \
  -H "Content-Type: application/json" \
  -d '{
    "query":      "What is the capital of France?",
    "kb_id":      "research_kb",
    "user_id":    "agent",
    "query_mode": "direct"
  }'
```

!!! warning
    `direct` mode does not read your knowledge base at all. Any facts specific
    to your uploaded documents will not appear in the answer.
