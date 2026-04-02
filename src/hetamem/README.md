# HetaMem
HetaMem is a powerful multimodal memory management system designed specifically for the memory modules of intelligent agents and knowledge bases. It integrates graph and vector storage to enable efficient storage, retrieval, and full-lifecycle management of diverse memory data, supporting the core memory requirements of agents (e.g., conversational memory, behavioral memory) and knowledge bases (e.g., storage and reasoning for structured and unstructured knowledge).

### ✨ Key Features
- Supports multimodal memory storage and management, including text, video, audio, image, and other data types

- Integrates graph storage (Neo4j) and vector storage (Milvus) for efficient memory retrieval and relationship reasoning

- Provides complete memory lifecycle management, supporting add, update, query, and delete operations

- Tracks memory update history for easy backtracking and auditing

## 🚀 Getting Started

### Prerequisites
- Python 3.10+

- Conda for environment management

- Pre-deployed Neo4j database (for graph storage)

- Pre-deployed Milvus vector database (for vector storage)

- OpenAI API key or OpenAI API-compatible service endpoint

### Docker Environment Configuration

For a quick and consistent environment setup, you can use Docker Compose. Run the following commands:

```bash

cd docker

docker-compose up -d

```

This will start all the required dependent services (including Neo4j and Milvus) in detached mode. Ensure Docker and Docker Compose are installed on your system before executing.

### Installation

1. **Clone the repository:**

```bash
git clone https://github.com/HetaTeam/HetaMem.git
cd hrag
```

2. **Create a virtual environment:**

```bash
conda create -n hetamem python=3.10
conda activate hetamem
```

3. **Install the required dependencies:**

```bash
pip install -r requirements.txt
```

4. **Configure environment variables**

Create a `.env` file and add the following content:

```
OPENAI_API_KEY=your_api_key_here
```

## 💻 Usage
### Run the Demo Examples
Execute the following commands in your terminal to run the demo scripts:
```bash
python demo_vg.py  # Run basic memory management example
# or
python demo_kb.py  # Run MemVerse example
```
### Basic Memory Management Example (demo_vg.py)

#### 1. Initialize the Memory System

```python
from MemoryVG import Memory

# Configure storage and embedding model
config = {
    "embedder": {
        "provider": "openai",
        "config": {"model": "text-embedding-3-large", "embedding_dims": 1024},
    },
    "graph_store": {
        "provider": "neo4j",
        "config": {
            "url": "bolt://your_neo4j_host:7687",
            "username": "your_username",
            "password": "your_password",
        },
    },
    "vector_store": {
        "provider": "milvus",
        "config": {
            "collection_name": "hetamem",
            "embedding_model_dims": 1024,
            "url": "http://your_milvus_host:19530",
            "metric_type": "COSINE",
        },
    },
    "version": "v1.1",
}

# Create memory instance from config
m = Memory.from_config(config_dict=config)
```

#### 2. Basic Operation Examples

```python
# Add memory
res = m.add(
    messages="I am working on improving my tennis skills.",
    user_id="alice",
    infer=False,  # Disable inference, store raw text directly
)

# Update memory
memory_id = res["results"][0]["id"]
m.update(memory_id=memory_id, data="Likes to play basketball")

# Query all memories
print(m.get_all(user_id='alice'))

# Query memory update history
print(m.history(memory_id=memory_id))

# Delete memory
m.delete(memory_id=memory_id)
```

#### 3. Conversation Memory and Retrieval

```python
# Add conversation history
movie_messages = [
    {"role": "user", "content": "I'm planning to watch a movie tonight. Any recommendations?"},
    {"role": "assistant", "content": "How about a thriller movies? They can be quite engaging."},
    {"role": "user", "content": "I'm not a big fan of thriller movies but I love sci-fi movies."},
]
m.add(messages=movie_messages, user_id="alice")

# Retrieve memories
search_result = m.search(query="what does alice love?", user_id="alice", limit=3)
for item in search_result.get("results"):
    print(f"memory: {item.get('memory')}, score: {item.get('score')}")
```

### MemVerse Example (demo_kb.py)

```python
import asyncio
from demo_kb import MemverseClient

async def main():
    # Initialize client
    client = MemverseClient()
    
    # Add knowledge
    add_result = await client.add("alice likes football")
    print("Add result:", add_result)
    
    # Query knowledge
    search_result = await client.search("what does alice like?")
    print("Search result:", search_result)

asyncio.run(main())
```
For more details and complete usage examples, please refer to:[Memverse](https://github.com/KnowledgeXLab/MemVerse)

## Acknowledgements

We utilized the following repositories during development:

- [mem0](https://github.com/mem0ai/mem0)

- [Memverse](https://github.com/KnowledgeXLab/MemVerse)

