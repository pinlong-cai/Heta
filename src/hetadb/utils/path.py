"""Project path definitions for hetadb module."""

from pathlib import Path

# Project root directory (Heta/)
PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Package directory (hetadb/)
PACKAGE_ROOT = Path(__file__).resolve().parents[1]

# Data output directory
DATA_DIR = PROJECT_ROOT / "data"
