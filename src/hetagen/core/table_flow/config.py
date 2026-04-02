"""
Table flow LLM configuration.

Loads LLM credentials from the project-level config.yaml.
"""

import yaml

from hetagen.utils.path import PROJECT_ROOT


class Config:
    """LLM configuration for table flow components."""

    def __init__(self):
        with open(PROJECT_ROOT / "config.yaml", encoding="utf-8") as f:
            llm_cfg = yaml.safe_load(f)["hetagen"]["llm"]
        self.llm_api_key: str = llm_cfg["api_key"]
        self.llm_base_url: str = llm_cfg["base_url"]
        self.llm_model: str = llm_cfg["model"]
        self.llm_temperature: float = 0.0

    def validate(self):
        """Raise if required fields are missing."""
        if not self.llm_api_key:
            raise ValueError("hetagen.llm.api_key is not set in config.yaml")
        return self
