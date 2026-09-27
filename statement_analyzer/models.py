"""Canonical data model shared by every parser, source and the analyzer.

Every input (PDF, MBS JSON, Mono JSON, ...) is converted into ONE schema, so
nothing downstream needs to know which bank a statement came from.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

CREDIT = "credit"
DEBIT = "debit"

# Fields a parser may emit (as raw strings). The normalizer turns them into
# the canonical, typed columns below.
RAW_FIELDS = (
    "date", "value_date", "narration", "reference", "channel",
    "amount",            # unsigned, or signed with an explicit +/- prefix
    "debit", "credit",   # separate money-out / money-in columns
    "balance",
    "direction",         # already known ("credit"/"debit"), e.g. from JSON or LLM
)

# Canonical transaction columns (after normalization).
#   amount    -> always positive
#   direction -> "credit" | "debit"
#   balance   -> running balance after the transaction (NaN if unknown)
#   reconciled-> True/False when previous balance +/- amount == balance, else NA
CANONICAL_COLUMNS = [
    "date", "value_date", "narration", "reference", "channel",
    "amount", "direction", "balance", "reconciled",
]


@dataclass
class QualityReport:
    """How much we can trust an extraction. Used to pick between parsers."""

    rows: int = 0
    rows_with_balance: int = 0
    checked: int = 0            # rows where reconciliation was possible
    reconciled: int = 0         # rows where prev_balance +/- amount == balance
    heuristic_directions: int = 0
    warnings: list[str] = field(default_factory=list)

    @property
    def reconciliation_rate(self) -> Optional[float]:
        return self.reconciled / self.checked if self.checked else None

    @property
    def score(self) -> float:
        """Higher is better. Rewards rows that are *verified* by the balance."""
        if self.checked:
            return self.reconciled + 0.1 * (self.rows - self.checked)
        return 0.5 * self.rows

    def as_dict(self) -> dict:
        rate = self.reconciliation_rate
        return {
            "rows": self.rows,
            "rows_with_balance": self.rows_with_balance,
            "reconciliation_rate": None if rate is None else round(rate, 4),
            "heuristic_directions": self.heuristic_directions,
            "warnings": self.warnings,
        }


@dataclass
class Statement:
    """A fully normalized statement."""

    transactions: pd.DataFrame
    account_name: Optional[str] = None
    account_number: Optional[str] = None
    source: str = "unknown"          # e.g. "pdf:Zenith", "mbs", "mono"
    opening_balance: Optional[float] = None
    quality: QualityReport = field(default_factory=QualityReport)
    meta: dict = field(default_factory=dict)

    @property
    def empty(self) -> bool:
        return self.transactions.empty
