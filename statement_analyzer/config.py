"""All tunable knobs in one place"""
from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Settings:
    # ------------------------------------------------------------ extraction
    # Keep only the most recent N months of history.
    cutoff_months: int = 36

    # A parser result is "good enough" when this share of rows reconcile
    # against the running balance. Below it, other parsers are tried too.
    min_reconciliation: float = 0.90

    # Tolerance (in currency units) for balance reconciliation.
    balance_tolerance: float = 0.02

    # ------------------------------------------------------------------ LLM
    # Optional LLM fallback for unknown layouts. Off by default because it
    # sends statement text to an external API.
    enable_llm: bool = field(default_factory=lambda: os.getenv("STATEMENT_ENABLE_LLM", "0") == "1")
    llm_model: str = field(default_factory=lambda: os.getenv("STATEMENT_LLM_MODEL", "claude-sonnet-4-5"))
    llm_pages_per_call: int = 3
    llm_api_key: Optional[str] = field(default_factory=lambda: os.getenv("ANTHROPIC_API_KEY") or None)

    # ------------------------------------------------------- categorisation
    min_transfer_amount: float = 100
    min_salary_amount: float = 100_000
    min_loan_repayment_amount: float = 100
    max_salary_amount: float = 5_000_000

    # -------------------------------------------------------- underwriting
    # Salary baseline: "min_non_zero" is conservative, "median" is typical.
    salary_baseline_mode: str = "min_non_zero"
    # A salary is "recent" if the last one landed within this many days of the
    # statement's end date.
    salary_recency_days: int = 90
    salary_window_days: tuple[int, int] = (20, 7)   # paid on/after the 20th, or by the 7th
    salary_diff_tolerance: float = 0.05       # amounts within 5% are the same "salary"
    salary_min_cycles: int = 4                # seen in at least N monthly cycles
    salary_max_cycles: int = 12
    salary_max_cv: float = 0.10               # and stable (CV under 10%)
    salary_max_date_drift: int = 3
    salary_max_cycle_gap: int = 2

    # Share of income considered available for a new loan repayment.
    repayment_fraction_salaried: float = 0.5
    repayment_fraction_non_salaried: float = 0.40
    # For non-salaried customers, the share of gross inflow treated as income.
    inflow_margin: float = 0.15

    # Weekly trend needs a minimum history; below it the slope is unknown.
    min_weeks_for_slope: int = 8
    # Balance counts as "zeroed" below max(zeroing_base, fraction x median daily inflow).
    zeroing_base: float = 1_000.0
    zeroing_inflow_fraction: float = 0.05

    def with_(self, **changes) -> "Settings":
        """Return a copy with some fields changed (Settings is immutable)."""
        return replace(self, **changes)


DEFAULT_SETTINGS = Settings()
