"""Query rewriter — generates multiple search-query variations via LLM.

Given a single user query, produces alternative phrasings to improve recall
when searching the knowledge base.  The LLM caller is injected at construction
time so this module shares the unified client (retry, backoff, semaphore) with
the rest of the system.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Callable

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """You are a helpful assistant that generates multiple search queries based on a single input query.

Perform query expansion. If there are multiple common ways of phrasing a user question or common synonyms for key words in the question, make sure to return multiple versions of the query with the different phrasings.
If there are acronyms or words you are not familiar with, do not try to rephrase them.
Return 3 different versions of the question.
Do not include any other text or explanation.

---Output Format Requirements---
Return the queries as a JSON object with a key "queries" and a list of strings as the value.
The output should strictly follow this format:
{
  "queries": [
    "Expanded_Query_1",
    "Expanded_Query_2",
    "Expanded_Query_3"
  ]
}

---Example---
Input Query: Were Scott Derrickson and Ed Wood of the same nationality?

---Output---:
{
  "queries": [
    "What is Scott Derrickson's nationality?",
    "What is Ed Wood's nationality?",
    "Are Scott Derrickson and Ed Wood from the same country?"
  ]
}"""


class QueryRewriter:
    """Generates alternative query formulations using a shared LLM callable.

    Args:
        use_llm: Sync callable ``(prompt: str) -> str`` — the unified LLM
                 client created by ``create_use_llm``.  Injected rather than
                 constructed here so that retry logic, concurrency limits, and
                 configuration all come from the shared source of truth.
    """

    def __init__(self, use_llm: Callable[[str], str]) -> None:
        self._use_llm = use_llm

    def rewrite(
        self, query: str, max_variations: int = 3, fallback_on_error: bool = True,
    ) -> list[str]:
        """Generate alternative query formulations.

        Args:
            query: Original user query.
            max_variations: Maximum number of variations to return.
            fallback_on_error: If True, return empty list on failure instead of raising.

        Returns:
            List of rewritten queries (may be empty on failure when
            ``fallback_on_error`` is True).
        """
        prompt = f"{_SYSTEM_PROMPT}\n\nUser query: {query}"
        try:
            response = self._use_llm(prompt)
            return self._parse_response(response, max_variations)
        except Exception:
            logger.warning("Query rewrite failed for query='%s'", query, exc_info=True)
            if fallback_on_error:
                return []
            raise

    def _parse_response(self, response: str, max_variations: int) -> list[str]:
        """Parse the LLM response into a list of query strings."""
        response = _strip_thinking(response)
        try:
            data: dict = json.loads(response)
            queries: list[str] = data.get("queries", [])
            if not isinstance(queries, list):
                raise ValueError("queries must be a list")
            return [q for q in queries if isinstance(q, str) and q.strip()][:max_variations]
        except json.JSONDecodeError:
            return [q.strip() for q in response.split("\n") if q.strip()][:max_variations]

    def batch_rewrite(self, queries: list[str]) -> list[list[str]]:
        """Rewrite multiple queries in batch."""
        return [self.rewrite(query) for query in queries]


def _strip_thinking(response: str) -> str:
    """Remove ``<think>...</think>`` sections that break JSON parsing."""
    if not response:
        return response
    return re.sub(r"<think>.*?</think>", "", response, flags=re.IGNORECASE | re.DOTALL)
