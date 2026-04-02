# HetaDB

HetaDB 是 Heta 的文档知识库层。它负责接收多模态文件、提取知识图谱、存储向量嵌入，并以 LLM 综合生成的答案和内联引用回答自然语言问题。

---

## 支持的文件格式

| 格式 | 扩展名 |
|------|--------|
| 文档 | `.pdf`、`.docx`、`.doc` |
| 演示文稿 | `.pptx`、`.ppt` |
| 电子表格 | `.xlsx`、`.xls`、`.csv`、`.ods` |
| 图片 | `.png`、`.jpg`、`.jpeg`、`.gif`、`.webp`、`.bmp`、`.tiff` |
| 纯文本 / 标记语言 | `.txt`、`.text`、`.md`、`.markdown`、`.html` |
| 压缩包 | `.zip`、`.7z`、`.rar`、`.tar`、`.tar.gz`、`.tar.xz`、`.tar.bz2` |

---

## 六阶段处理流水线

触发解析任务后，HetaDB 会将每个文件依次经过六个处理阶段：

| # | 阶段 | 说明 |
|---|------|------|
| 1 | **文件解析** | 四类解析器并发运行：`doc_parser` 处理 PDF/DOC/DOCX/PPT/PPTX（提取文本、表格及嵌入图片）；`html_parser` 处理 HTML；`text_parser` 处理 TXT/MD；`sheet_parser` 处理 CSV/XLS/XLSX（LLM 生成表格 Schema 与描述，表格描述同步写入文本流）。第二阶段串行：`image_parser` 用 VLM 为独立图片及文档中的嵌入图片生成文字描述。压缩包（ZIP/7Z/RAR/TAR）在解析前自动递归解压 |
| 2 | **文本分块** | 将文本切分为有重叠的 token 块；LLM 辅助合并语义相似块，中间块向量存入 Milvus；**Rechunk**：将 merge 后的 chunk 按原始文档分组，把同一文档的所有 chunk token 拼接还原后重新切分，每个新 chunk 记录 `source_chunk` 溯源字段；最终 rechunked chunk 写入 PostgreSQL |
| 3 | **图谱抽取** | LLM 并发地从第二阶段产出的 rechunked chunk 中提取实体与关系；原始结果写入 `kg_file/rechunked/` 下的 JSONL 文件（此阶段不做合并） |
| 4 | **节点处理** | LLM 去重 → 嵌入 → 向量相似度聚类合并 → Milvus 语义去重；最终节点存入 Milvus（向量）和 PostgreSQL（元数据） |
| 5 | **关系处理** | 应用节点 ID 映射；LLM 去重 → 嵌入 → 聚类合并 → Milvus 语义去重；最终关系存入 Milvus（向量）和 PostgreSQL（元数据及关系-chunk 溯源） |
| 6 | **表格嵌入** | LLM 处理 CSV 文件生成表格 Schema 与节点描述；原始数据导入 PostgreSQL；节点嵌入写入 Milvus 实体集合；支持自然语言转 SQL 查询 |

处理任务完全异步。请使用解析调用返回的 `task_id`，轮询 `GET /api/v1/hetadb/files/processing/tasks/{task_id}`，直到 `status` 返回 `"completed"` 后再发起查询。

---

## 查询模式

| `query_mode` | 策略 | 适用场景 |
|---|---|---|
| `naive` | 向量检索与知识图谱检索并行，加权评分 | 快速通用查询；推荐默认值 |
| `rerank` | BM25 + 向量 RRF 融合 → 交叉编码器重排 | 最高精度；适用于事实性问题 |
| `rewriter` | LLM 生成 3 个查询变体，聚合结果 | 模糊或表述不清的查询 |
| `multihop` | ReAct 推理循环（最多 3 轮） | 多步骤 / 思维链问题 |
| `direct` | LLM 仅依赖参数化知识作答 | 快速获取 LLM 意见；无需检索 |

详情请参见 [查询模式](query-modes.zh.md)。

---

## 子页面

- [接入文档](ingest.zh.md) — 创建知识库、上传文件、触发解析、检查状态
- [查询模式](query-modes.zh.md) — 每种检索策略的详细说明与 curl 示例
