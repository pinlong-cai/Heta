"""Logging and config utilities for hetamem."""

import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import colorlog
import yaml

from hetamem.utils.path import PROJECT_ROOT

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


def setup_logging() -> None:
    """Configure hetamem logging with daily-rotated file and console output."""
    root = logging.getLogger()
    if root.handlers:
        return  # already configured — avoid duplicate handlers on reload

    root.setLevel(logging.INFO)

    console_handler = colorlog.StreamHandler()
    console_handler.setFormatter(_COLOR_FMT)
    root.addHandler(console_handler)

    log_dir = PROJECT_ROOT / "log" / "hetamem"
    log_dir.mkdir(parents=True, exist_ok=True)

    file_handler = TimedRotatingFileHandler(
        log_dir / "hetamem.log",
        when="midnight",
        backupCount=7,
        encoding="utf-8",
    )
    file_handler.setFormatter(_FILE_FMT)
    root.addHandler(file_handler)


def load_config() -> dict:
    """Load hetamem config section from project-level config.yaml."""
    with open(PROJECT_ROOT / "config.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg.get("hetamem", {})
