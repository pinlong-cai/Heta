"""
Project path definitions for hetagen module.
"""
from pathlib import Path

# Project root directory (Heta/)
PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Package directory (hetagen/)
PACKAGE_ROOT = Path(__file__).resolve().parents[1]

# Data output directory
DATA_DIR = PROJECT_ROOT / "data"

# Hetagen output directory
HETAGEN_DATA_DIR = DATA_DIR / "hetagen"
