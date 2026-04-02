"""Project path definitions for the hetamem package."""

from pathlib import Path

# Heta/ — the repository root (src/hetamem/utils/ → src/hetamem/ → src/ → Heta/)
PROJECT_ROOT: Path = Path(__file__).resolve().parents[3]

# src/hetamem/ — the package root
PACKAGE_ROOT: Path = Path(__file__).resolve().parents[1]

# LightRAG graph storage — anchored to the source tree regardless of CWD.
# Physical location: src/hetamem/MemoryKB/Long_Term_Memory/Graph_Construction/MMKG/
MMKG_DIR: Path = (
    PACKAGE_ROOT / "MemoryKB" / "Long_Term_Memory" / "Graph_Construction" / "MMKG"
)
MMKG_CORE_DIR: Path = MMKG_DIR / "core"
MMKG_EPISODIC_DIR: Path = MMKG_DIR / "episodic"
MMKG_SEMANTIC_DIR: Path = MMKG_DIR / "semantic"
