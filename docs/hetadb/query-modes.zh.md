# 查询模式

HetaDB 通过 `POST /api/v1/hetadb/chat` 请求体中的 `query_mode` 字段支持五种检索策略。本页对每种模式进行说明，包括适用场景和可直接运行的 curl 示例。

---

## 总览

| `query_mode` | 策略 | 典型延迟 |
|---|---|---|
| `naive` | 向量检索与知识图谱检索并行，加权评分 | 最快 |
| `rerank` | BM25 + 向量 RRF 融合 → 交叉编码器重排 | 中等 |
| `rewriter` | LLM 生成 3 个查询变体，并行检索 | 中等偏慢 |
| `multihop` | ReAct 推理循环（最多 3 轮） | 最慢 |
| `direct` | 仅依赖 LLM — 无检索 | 极快 |

---

## naive

**策略：** 同时执行向量检索和知识图谱检索，再通过加权评分合并结果，无重排步骤。

**适用场景：** 大多数查询的默认选择。当问题表述清晰且希望快速响应时使用。

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

**策略：** 将 BM25 关键词检索和稠密向量检索通过倒数排名融合（RRF）合并，再用交叉编码器模型对融合候选集进行重排，精度最高。

**适用场景：** 事实性问题、技术查询，或任何对引用准确性要求较高的场景。

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
    `rerank` 是生产环境 Agent 需要可靠引用时的推荐模式。额外的延迟通常带来显著的精度提升，物有所值。
    交叉编码器重排依赖 `hetadb.query_defaults.reranker_url` 配置；未配置时退化为纯 RRF 排序，仍优于 `naive` 但无交叉编码器增益。

---

## rewriter

**策略：** LLM 针对原始查询生成三个改写变体，每个变体独立触发检索，结果合并后去重再生成答案。

**适用场景：** 模糊或表述不清的查询，单一措辞可能遗漏相关文本块的情况（例如带有行话或非正式用语的用户输入）。

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

**策略：** 实现 ReAct（推理 + 行动）循环，最多迭代三轮检索。每轮检索后，LLM 判断当前上下文是否足以作答，或是否需要继续检索。

**适用场景：** 需要串联多个事实的复杂问题（例如"对比第 3 节和第 5 节的方法，解释哪种在数据集 X 上表现更好"）。

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
    `multihop` 是最慢的模式。对于简单的事实查询，`rerank` 或 `naive` 通常已经足够，无需使用此模式。

---

## direct

**策略：** LLM 完全依赖其参数化（预训练）知识作答，不执行任何检索。响应中的 `data[]` 和 `citations[]` 字段始终为空。

**适用场景：** 快速获取 LLM 意见、与文档无关的通识性问题，或纯粹用于测试目的时绕过检索栈。

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
    `direct` 模式完全不读取知识库。上传文档中的特定事实不会出现在答案中。
