# Docker Compose

推荐的 Heta 运行方式。一条命令即可启动完整服务栈——API、Web UI、PostgreSQL、Milvus、Neo4j 和 MinIO。

## 前提条件

- Docker ≥ 24.0
- Docker Compose ≥ 2.20
- 至少一个 LLM 服务商账号

## 1. 克隆并复制配置

=== "国内（DashScope + SiliconFlow）"

    ```bash
    git clone https://github.com/HetaTeam/Heta.git
    cd Heta
    cp config.example.zh.yaml config.yaml
    ```

=== "国际（OpenAI + Gemini）"

    ```bash
    git clone https://github.com/HetaTeam/Heta.git
    cd Heta
    cp config.example.yaml config.yaml
    ```

## 2. 填入 API Key

`config.yaml` 的 `providers` 块定义服务商连接信息，各模块通过 YAML anchor（`<<: *provider_name`）引用。示例配置以阿里云 DashScope 和硅基流动为默认服务商，但**任何兼容 OpenAI 接口的 API 或自部署模型均可替换**——只需修改对应 provider 的 `api_key`、`base_url` 和模型名即可。

=== "国内（DashScope + SiliconFlow）"

    `config.example.zh.yaml` 已全部指向国内服务商，并配置好对应模型。只需填入 API Key：

    ```yaml
    providers:
      dashscope:
        api_key: "YOUR_DASHSCOPE_API_KEY"   # https://dashscope.aliyun.com
      siliconflow:
        api_key: "YOUR_SILICONFLOW_API_KEY" # https://siliconflow.cn
    ```

=== "国际（全部四个服务商）"

    `config.example.yaml` 使用全部四个服务商，填入各 API Key 即可，无需修改模块配置：

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

    各服务商默认分工：DashScope → HetaDB LLM/VLM 和 MemoryVG LLM；SiliconFlow → HetaDB 向量化和 HetaGen VLM/向量化；OpenAI → MemoryKB 和 MemoryVG embedder；Gemini → HetaGen LLM。


## 3. 启动

```bash
docker-compose up -d
```

首次运行需拉取镜像并构建（约 10–20 分钟）。

## 4. 验证

```bash
docker-compose ps           # 所有服务状态应为 healthy
curl localhost:8000/health
```

## 服务地址

| 地址 | 说明 |
|------|------|
| http://localhost | Heta Web UI |
| http://localhost:8000/docs | REST API（Swagger） |
| http://localhost:7474 | Neo4j 浏览器 |
| http://localhost:9001 | MinIO 控制台 |

## 停止

```bash
docker-compose down         # 停止，保留数据
docker-compose down -v      # 停止并删除所有数据卷
```
