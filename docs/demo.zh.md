# 演示

## 版本一：使用查询技能（推荐）

将 `skills/querying-knowledge-and-memory/SKILL.md` 加载到 Agent 的系统提示中即可。技能文件封装了完整的三步检索编排逻辑，Agent 天然懂得何时用 MemoryVG、何时转向 HetaDB、何时写入记忆。

---

## 版本二：LangChain Agent

将 Heta 三个接口封装为 LangChain 工具，由 LLM 自主决策检索与存储策略：

```python
import httpx
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI

BASE = "http://localhost:8000"
AGENT_ID = "agent"

@tool
def search_memory(query: str) -> str:
    """Search personal memory for previously seen facts."""
    res = httpx.post(f"{BASE}/api/v1/hetamem/vg/search",
                     json={"query": query, "agent_id": AGENT_ID}).json()
    results = res.get("results", [])
    return "\n".join(r["memory"] for r in results[:3]) if results else "No memory found."

@tool
def query_knowledge_base(query: str, kb_id: str) -> str:
    """Query a document knowledge base and return a synthesised answer with citations."""
    res = httpx.post(f"{BASE}/api/v1/hetadb/chat", json={
        "query":      query,
        "kb_id":      kb_id,
        "user_id":    AGENT_ID,
        "query_mode": "naive",
    }).json()
    return res.get("response", "No answer found.")

@tool
def store_memory(content: str) -> str:
    """Store a finding into personal memory for fast recall in future sessions."""
    httpx.post(f"{BASE}/api/v1/hetamem/vg/add", json={
        "messages": [{"role": "assistant", "content": content}],
        "agent_id": AGENT_ID,
    })
    return "Stored."

# Agent 构建
llm = ChatOpenAI(model="gpt-4o")
tools = [search_memory, query_knowledge_base, store_memory]

prompt = ChatPromptTemplate.from_messages([
    ("system",
     "你是一名研究助手。回答前先检查记忆，必要时查询知识库，"
     "将重要发现存入记忆供后续使用。"),
    ("human", "{input}"),
    MessagesPlaceholder("agent_scratchpad"),
])

agent = create_tool_calling_agent(llm, tools, prompt)
executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

# 运行
executor.invoke({"input": "research-kb 里的论文用了什么损失函数？"})
```

Agent 会自行按技能的三步逻辑决策：先调 `search_memory`，未命中则调 `query_knowledge_base`，最后酌情调 `store_memory`。

