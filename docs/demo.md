# Demo

## Option 1: Use the Querying Skill (recommended)

Load `skills/querying-knowledge-and-memory/SKILL.md` into the agent's system prompt. The skill encodes the full three-step retrieval orchestration — when to use MemoryVG, when to fall back to HetaDB, and when to write to memory.

---

## Option 2: LangChain Agent

Wrap the three Heta endpoints as LangChain tools and let the LLM decide when to call each one:

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

# Build the agent
llm = ChatOpenAI(model="gpt-4o")
tools = [search_memory, query_knowledge_base, store_memory]

prompt = ChatPromptTemplate.from_messages([
    ("system",
     "You are a research assistant. Always check memory first before querying a "
     "knowledge base. Store useful findings for future recall."),
    ("human", "{input}"),
    MessagesPlaceholder("agent_scratchpad"),
])

agent = create_tool_calling_agent(llm, tools, prompt)
executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

# Run
executor.invoke({"input": "What loss function does the paper in research-kb use?"})
```

The agent follows the same three-step pattern as the skill: it calls `search_memory` first, falls back to `query_knowledge_base` on a miss, and calls `store_memory` for findings worth keeping.

