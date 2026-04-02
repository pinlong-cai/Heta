"""
Shared logging and config utilities.

Intended for use in application entry points (main.py).  Modules should
obtain their logger via ``logging.getLogger(__name__)`` and never call
``setup_logging`` themselves.
"""

import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Optional

import colorlog
import yaml

# Project root: src/common/config/ → src/common/ → src/ → <project root>
_PROJECT_ROOT = Path(__file__).resolve().parents[3]

_COLOR_FMT = colorlog.ColoredFormatter(
    "%(asctime)s | %(log_color)s%(levelname)-8s%(reset)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    log_colors={
        "DEBUG": "cyan",
        "INFO": "green",
        "WARNING": "yellow",
        "ERROR": "red",
        "CRITICAL": "bold_red",
    },
)

_FILE_FMT = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def setup_logging(name: str = "heta", level: int = logging.INFO) -> None:
    """Configure console + daily-rotated file logging for *name*.

    Both handlers are attached to the root logger so that every child logger
    (``hetadb.*``, ``hetamem.*``, third-party libs, etc.) writes to both the
    console and the log file without any extra setup.

    Args:
        name:  Log-file prefix and subdirectory name (e.g. ``"heta"``).
        level: Root log level. Defaults to ``logging.INFO``.
    """
    root = logging.getLogger()
    if root.handlers:
        return  # already configured — avoid duplicate handlers on reload

    root.setLevel(level)

    console_handler = colorlog.StreamHandler()
    console_handler.setFormatter(_COLOR_FMT)
    root.addHandler(console_handler)

    log_dir = _PROJECT_ROOT / "log" / name
    log_dir.mkdir(parents=True, exist_ok=True)

    file_handler = TimedRotatingFileHandler(
        log_dir / f"{name}.log",
        when="midnight",
        backupCount=7,
        encoding="utf-8",
    )
    file_handler.setFormatter(_FILE_FMT)
    root.addHandler(file_handler)


def load_config(section: Optional[str] = None) -> dict:
    """Load configuration from the project-level ``config.yaml``.

    Args:
        section: Top-level key to return (e.g. ``"hetadb"``).
                 If ``None``, the full config dict is returned.

    Returns:
        The requested config section, or an empty dict if not found.
    """
    with open(_PROJECT_ROOT / "config.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg.get(section, {}) if section else cfg


