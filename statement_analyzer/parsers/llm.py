"""Optional LLM fallback for layouts no rule-based parser can read.

Used only when enabled (``Settings.enable_llm`` / STATEMENT_ENABLE_LLM=1) and
only after the deterministic parsers fail validation. The output goes through
the same normalizer and balance reconciliation as every other parser, so a
hallucinated row shows up as a reconciliation failure instead of silently
entering the analysis.

Privacy: this sends statement text to an external API. Make sure that is
allowed for your customers' data before enabling it.
"""
from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

from ..config import DEFAULT_SETTINGS, Settings
from .base import Document, StatementParser

log = logging.getLogger(__name__)

_TOOL = {
    "name": "record_transactions",
    "description": "Record every transaction row found in the bank statement text.",
    "input_schema": {
        "type": "object",
        "properties": {
            "transactions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "date": {"type": "string", "description": "Transaction date, ISO YYYY-MM-DD"},
                        "narration": {"type": "string"},
                        "reference": {"type": "string"},
                        "amount": {"type": "number", "description": "Always positive"},
                        "direction": {"type": "string", "enum": ["credit", "debit"]},
                        "balance": {"type": ["number", "null"], "description": "Balance after the transaction"},
                    },
                    "required": ["date", "narration", "amount", "direction"],
                },
            }
        },
        "required": ["transactions"],
    },
}

_PROMPT = (
    "Extract every transaction row from this bank statement text. "
    "Ignore headers, footers, opening/closing balance lines and summary totals. "
    "Keep rows in the order they appear. Do not invent rows.\n\n<statement>\n{text}\n</statement>"
)


class LLMUnavailable(RuntimeError):
    """Raised when the fallback is enabled but cannot run (missing package/key)."""


class LLMParser(StatementParser):
    name = "LLM fallback"
    date_formats = ["%Y-%m-%d"]

    def __init__(self, settings: Settings = DEFAULT_SETTINGS):
        self.settings = settings

    def unavailable_reason(self) -> Optional[str]:
        """Why this parser cannot run right now (None when it can)."""
        try:
            import anthropic  # noqa: F401
        except ImportError:
            return "the 'anthropic' package is not installed (pip install anthropic)"
        if not self.settings.llm_api_key:
            return ("no API key: set ANTHROPIC_API_KEY, or pass "
                    "Settings(llm_api_key='sk-ant-...')")
        return None

    def client(self):
        """Build the API client, with a clear error instead of a raw SDK failure."""
        reason = self.unavailable_reason()
        if reason:
            raise LLMUnavailable(f"LLM fallback is enabled but unusable: {reason}")
        import anthropic

        return anthropic.Anthropic(api_key=self.settings.llm_api_key)

    def parse(self, doc: Document) -> pd.DataFrame:
        client = self.client()
        step = self.settings.llm_pages_per_call
        rows: list[dict] = []
        for start in range(0, len(doc.pages), step):
            chunk = "\n\n".join(doc.pages[start:start + step])
            response = client.messages.create(
                model=self.settings.llm_model,
                max_tokens=16000,
                tools=[_TOOL],
                tool_choice={"type": "tool", "name": _TOOL["name"]},
                messages=[{"role": "user", "content": _PROMPT.format(text=chunk)}],
            )
            for block in response.content:
                if block.type == "tool_use":
                    rows.extend(block.input.get("transactions", []))
        return pd.DataFrame.from_records(rows)
