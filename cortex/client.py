"""
Snowflake Cortex COMPLETE client.

This module is the single integration seam between the app and the LLM. It
exposes a ``complete()`` function whose signature mirrors
``snowflake.cortex.Complete`` from the Snowpark ML library:

    snowflake.cortex.Complete(
        model: str,
        prompt: str | list[dict],
        session: Session | None = None,
    ) -> str

Swap ``USE_LOCAL_STUB = False`` (or unset the env var) to route calls to a
real Snowflake session. The rest of the app does not change.

Docs: https://docs.snowflake.com/en/sql-reference/functions/complete-snowflake-cortex
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any

# ---------------------------------------------------------------------------
# Public configuration
# ---------------------------------------------------------------------------

USE_LOCAL_STUB: bool = os.getenv("CORTEX_MODE", "stub").lower() != "snowflake"
DEFAULT_MODEL: str = os.getenv("CORTEX_MODEL", "claude-sonnet-4-5")


@dataclass
class CortexResponse:
    """Structured response used internally. Mirrors the Cortex JSON shape
    when ``options`` is passed to the SQL function."""
    text: str
    model: str
    usage: dict[str, int]


# ---------------------------------------------------------------------------
# Public API — signature-compatible with snowflake.cortex.Complete
# ---------------------------------------------------------------------------

def complete(
    model: str,
    prompt: str | list[dict[str, str]],
    session: Any | None = None,
    options: dict[str, Any] | None = None,
) -> str:
    """
    Call Snowflake Cortex COMPLETE (or the local stub in demo mode).

    Args:
        model: Model name, e.g. "claude-sonnet-4-5", "mistral-large2",
            "llama3.1-70b".
        prompt: Either a string prompt or a list of ``{"role", "content"}``
            messages for multi-turn.
        session: A Snowpark ``Session``. Unused in stub mode.
        options: Optional inference hyperparameters (temperature, max_tokens).

    Returns:
        The model's text completion.
    """
    if USE_LOCAL_STUB:
        return _local_stub(model, prompt, options or {}).text

    # Real Snowflake path. Imported lazily so the stub works without the
    # snowflake-ml-python dependency installed.
    from snowflake.cortex import Complete  # type: ignore
    return Complete(model=model, prompt=prompt, session=session)


# ---------------------------------------------------------------------------
# Local stub — deterministic, rule-based parser that understands the HR /
# Legal vocabulary of the demo warehouse and emits a JSON "routing" object
# the app can execute against SQLite. This is designed so that the exact
# same prompt would, in production, be handled by a real Cortex-hosted LLM.
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT_MARKER = "You are an analytics router"


def _local_stub(
    model: str,
    prompt: str | list[dict[str, str]],
    options: dict[str, Any],
) -> CortexResponse:
    """Heuristic parser that simulates what a real LLM would return given
    the system prompt in ``ui/prompts.py``."""
    # Extract the latest user question from either a string or a message list
    user_text = _extract_user_text(prompt).lower()

    intent = _classify_intent(user_text)
    payload = json.dumps(intent)

    return CortexResponse(
        text=payload,
        model=model,
        usage={"prompt_tokens": len(user_text) // 4, "completion_tokens": len(payload) // 4},
    )


def _extract_user_text(prompt: str | list[dict[str, str]]) -> str:
    if isinstance(prompt, str):
        return prompt
    # Message list — grab the last user turn
    for msg in reversed(prompt):
        if msg.get("role") == "user":
            return msg.get("content", "")
    return ""


def _classify_intent(text: str) -> dict[str, Any]:
    """Map a natural language question onto one of the warehouse queries
    the app can execute. Returns a dict with shape::

        {"query": <name>, "params": {...}, "chart": <kind>, "summary_hint": <str>}
    """
    t = text.lower()

    # --- HR intents ---------------------------------------------------------
    if _any_in(t, ["attrition", "turnover", "churn", "leaving", "quit"]):
        dept = _extract_department(t)
        return {
            "query": "attrition_by_department",
            "params": {"department": dept} if dept else {},
            "chart": "bar",
            "summary_hint": (
                f"Attrition in {dept}" if dept else "Attrition across departments"
            ),
        }

    if _any_in(t, ["headcount", "how many people", "how many employees", "team size", "fte"]):
        dept = _extract_department(t)
        return {
            "query": "headcount_by_department",
            "params": {"department": dept} if dept else {},
            "chart": "bar",
            "summary_hint": "Active headcount by department",
        }

    if _any_in(t, ["compensation", "salary", "pay", "comp band", "comp bands"]):
        dept = _extract_department(t)
        return {
            "query": "compensation_by_level",
            "params": {"department": dept} if dept else {},
            "chart": "bar",
            "summary_hint": (
                f"Compensation bands in {dept}" if dept else "Compensation bands by level"
            ),
        }

    if _any_in(t, ["hire", "hiring", "new hires", "recent hires"]):
        return {
            "query": "hires_last_12_months",
            "params": {},
            "chart": "line",
            "summary_hint": "Hiring trend over the last 12 months",
        }

    if _any_in(t, ["tenure", "how long"]):
        return {
            "query": "tenure_by_department",
            "params": {},
            "chart": "bar",
            "summary_hint": "Average tenure by department",
        }

    # --- Legal intents ------------------------------------------------------
    if _any_in(t, ["legal spend", "outside counsel", "firm", "firms", "law firm"]):
        quarter = _extract_quarter(t)
        return {
            "query": "legal_spend_by_firm",
            "params": {"quarter": quarter} if quarter else {},
            "chart": "bar",
            "summary_hint": (
                f"Legal spend by outside counsel, {quarter}"
                if quarter
                else "Legal spend by outside counsel firm"
            ),
        }

    if _any_in(t, ["legal", "matter", "matters", "litigation", "case", "cases"]):
        if "open" in t or "active" in t:
            return {
                "query": "open_matters_by_type",
                "params": {},
                "chart": "bar",
                "summary_hint": "Open legal matters by type",
            }
        if "trend" in t or "over time" in t or "monthly" in t:
            return {
                "query": "legal_spend_trend",
                "params": {},
                "chart": "line",
                "summary_hint": "Legal spend trend over the last 12 months",
            }
        return {
            "query": "spend_by_matter_type",
            "params": {},
            "chart": "bar",
            "summary_hint": "Legal spend by matter type",
        }

    # --- Fallback -----------------------------------------------------------
    return {
        "query": "overview",
        "params": {},
        "chart": None,
        "summary_hint": "Workforce and legal ops overview",
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DEPARTMENTS = [
    "engineering", "product", "design", "sales", "marketing",
    "customer success", "finance", "people", "legal", "operations",
]


def _extract_department(text: str) -> str | None:
    for dept in _DEPARTMENTS:
        if dept in text:
            return dept.title() if dept != "customer success" else "Customer Success"
    # Aliases
    aliases = {"eng": "Engineering", "hr": "People", "cs": "Customer Success"}
    for alias, dept in aliases.items():
        if re.search(rf"\b{alias}\b", text):
            return dept
    return None


def _extract_quarter(text: str) -> str | None:
    """Parse references like 'this quarter', 'last quarter', 'Q1 2026'."""
    m = re.search(r"q([1-4])\s*(20\d{2})?", text)
    if m:
        q = m.group(1)
        y = m.group(2) or "2026"
        return f"Q{q} {y}"
    if "this quarter" in text:
        return "Q2 2026"
    if "last quarter" in text:
        return "Q1 2026"
    return None


def _any_in(text: str, needles: list[str]) -> bool:
    return any(n in text for n in needles)
