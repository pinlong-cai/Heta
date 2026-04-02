# Docker Compose

The recommended way to run Heta. Brings up the full stack — API, web UI, PostgreSQL, Milvus, Neo4j, and MinIO — with a single command.

## Prerequisites

- Docker ≥ 24.0
- Docker Compose ≥ 2.20
- At least one LLM provider account

## 1. Clone and copy config

=== "Global"

    ```bash
    git clone https://github.com/HetaTeam/Heta.git
    cd Heta
    cp config.example.yaml config.yaml
    ```

=== "China"

    ```bash
    git clone https://github.com/HetaTeam/Heta.git
    cd Heta
    cp config.example.zh.yaml config.yaml
    ```

## 2. Fill in API keys

The `providers` block in `config.yaml` defines service connection details. Modules reference them via YAML anchors (`<<: *provider_name`). The example configs use Alibaba Cloud DashScope and SiliconFlow as defaults, but **any OpenAI-compatible API or self-hosted model works** — just update `api_key`, `base_url`, and the model name for the relevant provider.

=== "Global"

    `config.example.yaml` uses four providers. Fill in all API keys — no other changes needed:

    ```yaml
    providers:
      dashscope:
        api_key: "YOUR_DASHSCOPE_API_KEY"
      siliconflow:
        api_key: "YOUR_SILICONFLOW_API_KEY"
      openai:
        api_key: "YOUR_OPENAI_API_KEY"
      gemini:
        api_key: "YOUR_GEMINI_API_KEY"
    ```

    Default assignment: DashScope → HetaDB LLM/VLM and MemoryVG LLM; SiliconFlow → HetaDB embedding and HetaGen VLM/embedding; OpenAI → MemoryKB and MemoryVG embedder; Gemini → HetaGen LLM.

=== "China"

    `config.example.zh.yaml` already points to DashScope and SiliconFlow with Chinese-region models. Just fill in your API keys:

    ```yaml
    providers:
      dashscope:
        api_key: "YOUR_DASHSCOPE_API_KEY"   # https://dashscope.aliyun.com
      siliconflow:
        api_key: "YOUR_SILICONFLOW_API_KEY" # https://siliconflow.cn
    ```


## 3. Start

```bash
docker-compose up -d
```

First run pulls images and builds the stack (~10–20 min).

## 4. Verify

```bash
docker-compose ps           # all services should show: healthy
curl localhost:8000/health
```

## Service URLs

| URL | Description |
|-----|-------------|
| http://localhost | Heta web UI |
| http://localhost:8000/docs | REST API (Swagger) |
| http://localhost:7474 | Neo4j browser |
| http://localhost:9001 | MinIO console |

## Stop

```bash
docker-compose down         # stop, keep data
docker-compose down -v      # stop and delete all volumes
```
