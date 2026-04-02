# HetaDB

HetaDB is Heta's document knowledge-base layer. It ingests multimodal files,
extracts a knowledge graph, stores vector embeddings, and answers natural-language
questions with LLM-synthesised responses and inline citations.

---

## Supported Formats

| Format | Extensions |
|--------|-----------|
| Documents | `.pdf`, `.docx`, `.doc` |
| Presentations | `.pptx`, `.ppt` |
| Spreadsheets | `.xlsx`, `.xls`, `.csv`, `.ods` |
| Images | `.png`, `.jpg`, `.jpeg`, `.gif`, `.webp`, `.bmp`, `.tiff` |
| Plain text / markup | `.txt`, `.text`, `.md`, `.markdown`, `.html` |
| Archives | `.zip`, `.7z`, `.rar`, `.tar`, `.tar.gz`, `.tar.xz`, `.tar.bz2` |

---

## Six-Stage Processing Pipeline

Once you trigger a parse job, HetaDB runs each file through six sequential stages:

| # | Stage | What happens |
|---|-------|-------------|
| 1 | **File parsing** | Four parser types run concurrently: `doc_parser` for PDF/DOC/DOCX/PPT/PPTX (extracts text, tables, and embedded images); `html_parser` for HTML; `text_parser` for TXT/MD; `sheet_parser` for CSV/XLS/XLSX (LLM generates table schema and descriptions, piped into the text stream). Second phase (serial): `image_parser` uses VLM to generate text descriptions for standalone images and images embedded in documents. Archives (ZIP/7Z/RAR/TAR) are recursively extracted before parsing |
| 2 | **Text chunking** | Split text into overlapping token-based chunks; LLM-assisted merge of semantically similar chunks (intermediate chunk vectors stored in Milvus). **Rechunk**: post-merge chunks are grouped by source document, each document's chunk tokens are concatenated and re-split uniformly; every new chunk records a `source_chunk` provenance field. Final rechunked chunks written to PostgreSQL |
| 3 | **Graph extraction** | LLM concurrently extracts entities and relations from the rechunked chunks produced in stage 2; raw output written as JSONL to `kg_file/rechunked/` (no merging at this stage) |
| 4 | **Node processing** | LLM dedup → embedding → vector-similarity cluster merge → Milvus semantic dedup; final nodes stored in Milvus (embeddings) and PostgreSQL (metadata) |
| 5 | **Relation processing** | Node ID mapping applied; LLM dedup → embedding → cluster merge → Milvus semantic dedup; final relations stored in Milvus (embeddings) and PostgreSQL (metadata and relation-chunk provenance) |
| 6 | **Table embedding** | CSV files processed by LLM to generate table schema; raw data loaded into PostgreSQL; table node embeddings written to Milvus entity collection; enables natural-language-to-SQL queries |

The job is fully asynchronous. Poll `GET /api/v1/hetadb/files/processing/tasks/{task_id}`
(using the task ID returned by the parse call) until `status` is `"completed"` before issuing chat queries.

---

## Query Modes

| `query_mode` | Strategy | Best for |
|---|---|---|
| `naive` | Parallel vector + KG retrieval, weighted scoring | Fast general queries; good default |
| `rerank` | BM25 + vector RRF fusion → cross-encoder rerank | Highest precision; factual questions |
| `rewriter` | LLM generates 3 query variants, aggregates results | Ambiguous or under-specified queries |
| `multihop` | ReAct reasoning loop (max 3 rounds) | Multi-step / chain-of-thought questions |
| `direct` | LLM answers from parametric knowledge only | Quick LLM opinions; no retrieval needed |

See [Query Modes](query-modes.md) for per-mode examples and guidance.

---

## Sub-pages

- [Ingesting Documents](ingest.md) — create a knowledge base, upload files, trigger parsing, check status
- [Query Modes](query-modes.md) — detailed guide with curl examples for each retrieval strategy
