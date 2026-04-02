# Manual Setup

Run Heta without Docker — useful for development or when you already have the infrastructure services running.

## Prerequisites

- Python 3.10
- PostgreSQL (running)
- Milvus (running)
- Neo4j (running)

## Install

```bash
# 1. Create and activate environment
conda create -n heta python=3.10 -y
conda activate heta

# 2. Install backend
pip install -e .

# 3. Build frontend
cd heta-frontend && npm install && npm run build && cd ..

# 4. Copy and fill in config
cp config.example.yaml config.yaml
# Edit config.yaml: set providers.dashscope.api_key etc.
```

## Run — Unified Mode

Runs all modules (HetaDB, HetaMem, HetaGen) on a single port:

```bash
PYTHONPATH=src python src/main.py
# → http://localhost:8000
```

## Run — Per-Module Mode

Each module runs independently on its own port:

```bash
export PYTHONPATH=/path/to/Heta/src

python src/hetadb/api/main.py              # HetaDB   → :8001
python src/hetagen/api/main.py             # HetaGen  → :8002
python src/hetamem/api/main.py             # HetaMem  → :8003

# MCP servers (optional)
HETAMEM_BASE_URL=http://localhost:8000 python src/hetamem/mcp/server.py  # → :8011
HETADB_BASE_URL=http://localhost:8000  python src/hetadb/mcp/server.py   # → :8012
```

## Port Reference

| Service | Port |
|---------|------|
| Heta unified API | 8000 |
| HetaDB (standalone) | 8001 |
| HetaGen (standalone) | 8002 |
| HetaMem (standalone) | 8003 |
| HetaMem MCP | 8011 |
| HetaDB MCP | 8012 |
| PostgreSQL | 5432 |
| Milvus | 19530 |
| Neo4j Browser / Bolt | 7474 / 7687 |
| MinIO S3 / Console | 9000 / 9001 |
