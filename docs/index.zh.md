<p align="center">
  <img src="../images/banner.png" width="800" alt="Heta — 面向智能体的知识管理平台"/>
</p>

<p align="center">
  <b>面向智能体的知识管理平台</b><br/>
  多数据库集成调度 · 双模式记忆 · 结构化知识生成
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10-blue?logo=python&logoColor=white"/>
  <img src="https://img.shields.io/badge/License-AGPL%20v3-blue"/>
  <img src="https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white"/>
</p>

---

## Heta 是什么？

Heta是一款面向AI智能体的一体化知识基础设施，通过整合多种底层数据库，赋能智能体实现三大核心能力：外部知识获取、自身记忆积累，以及基于知识的推理与生成。

- **HetaDB** — 统一协调多类型数据库，达成外部知识智能接入，无需顾虑数据来源格式及存储库选型。
- **HetaMem** — 双模式记忆：集成MemoryVG以支持基于向量检索的快速情景回溯，并构建随智能体演进的长期知识图谱（MemoryKB）。  
- **HetaGen** — 基于已有知识库进行归纳与扩展，生成更具价值的结构化内容。

---

## 核心功能

**HetaDB**

- 基于Mineru等工具实现多格式文件解析（涵盖PDF、HTML、图片、表格、Markdown、Office及压缩包等）
- 解析知识自动路由至适配数据库，实现统一管控，无需人工干预存储策略
- 提供多维查询策略以适配差异化场景：`naive`（向量检索）· `rerank`（BM25 + 向量 + 交叉编码器）· `rewriter`（检索增强）· `multihop`（多跳推理）· `direct`（直接查询）
- 支持源文档溯源，满足 Agentic Search 的需求

**HetaMem**

- **MemoryVG** — 面向高频、碎片化信息的轻量记忆机制，可快速存储与检索对话内容、用户偏好、上下文事实等；支持完整 CRUD 与历史审计  
- **MemoryKB** — 基于 LightRAG 构建的长期知识图谱，随智能体持续学习与演化

**HetaGen** *(早期阶段)*

- 基于知识库生成结构化表格数据  
- 从主题（如物理、化学、生物）出发，自动构建层次化的知识结构体系（结构树）  
- 支持对生成结果执行 Text-to-SQL 查询  

!!! tip "MCP 集成"
    HetaDB 和 HetaMem 提供可选的 MCP 服务器（端口 8012 / 8011），可直接集成 Claude Desktop、Cursor 等 MCP 兼容客户端。

---

## 快速入口

- [Docker Compose 快速开始](quick-start/docker.md) — 推荐，一条命令启动全套服务
- [手动安装](quick-start/manual.md) — 独立运行各模块
- [连接 MCP 客户端](quick-start/mcp-clients.md) — Claude Desktop、Cursor
- [REST API 参考](reference/api.md)
