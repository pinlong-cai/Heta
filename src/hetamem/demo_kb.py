import asyncio
import sys
from pathlib import Path
memverse_path = str(Path(__file__).parent)+"/MemoryKB/Long_Term_Memory/Graph_Construction"
sys.path.insert(0, memverse_path)


from MemoryKB.orchestrator import initialize_rag, handle_insert, handle_query
from MemoryKB.Long_Term_Memory.Graph_Construction.lightrag.kg.shared_storage import initialize_share_data

class MemverseClient:
    async def add(self, query, video=None, audio=None, image=None):
        return await handle_insert(query, video=video, audio=audio, image=image)

    async def search(self, query, mode="hybrid", use_pm=False):
        return await handle_query(query, mode=mode, use_pm=use_pm)

async def main():
    initialize_share_data(workers=1)
    print("=== 初始化（initialize_rag） ===")
    await initialize_rag()

    client = MemverseClient()

    print("\n=== 测试 add ===")
    add_result = await client.add("alice喜欢足球")
    print("add_result:", add_result)

    print("\n=== 测试 search ===")
    search_result = await client.search("alice喜欢什么")
    print("search_result:", search_result)


if __name__ == "__main__":
    asyncio.run(main())
