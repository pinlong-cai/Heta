TEXT2SQL_PROMPT_TEMPLATE = """
你是一个 Text-to-SQL 专家。请根据下面的表格结构，将用户的自然语言问题转换为 SQL 查询语句。

# 表格结构
{table_schema}

# 要求
- 只允许生成 SELECT 查询语句，禁止 INSERT、UPDATE、DELETE 等任何其他操作;
- 注意查询语句中是否需要筛选条件；
- 输出必须仅包含 SQL 语句，不得包含解释、说明、Markdown 格式或代码块；


# 用户问题
{question}

# 示例
- 问题：找出这些公司中市值最高的十家公司。
- 输出：SELECT 实体，市值 FROM 表格 ORDER BY 市值 DESC LIMIT 10;
"""