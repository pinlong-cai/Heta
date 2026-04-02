import sys
from pathlib import Path

_HETAMEM_DIR = Path(__file__).resolve().parent

# Make MemoryKB and MemoryVG importable as top-level packages.
if str(_HETAMEM_DIR) not in sys.path:
    sys.path.insert(0, str(_HETAMEM_DIR))

# lightrag uses absolute self-imports (e.g. `from lightrag.utils import ...`),
# so its parent directory must also be on sys.path.
_GRAPH_CONSTRUCTION = (
    _HETAMEM_DIR / "MemoryKB" / "Long_Term_Memory" / "Graph_Construction"
)
if str(_GRAPH_CONSTRUCTION) not in sys.path:
    sys.path.insert(0, str(_GRAPH_CONSTRUCTION))
