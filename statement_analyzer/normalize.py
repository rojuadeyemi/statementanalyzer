"""Raw parser output -> canonical transactions, with balance reconciliation.

This is where the old code's repeated block

    balance_diff = df["Balance"].diff()
    df["Deposit"] = df["Amount"].where(balance_diff > 0, 0)
    ...  # plus a "fix first row" guess

is replaced by proper reconciliation: for each row, pick the direction for
which ``previous_balance ± amount == balance``. The same check then *scores*
the extraction — a row that doesn't reconcile means a missed or mis-read row.
"""
from __future__ import annotations

import logging
import math
import re
from typing import Optional, Sequence

import numpy as np
import pandas as pd

from .models import CANONICAL_COLUMNS, CREDIT, DEBIT, RAW_FIELDS, QualityReport
from .parsing import clean_text, explicit_sign, parse_amounts, parse_dates

log = logging.getLogger(__name__)

_BALANCE_LINES = re.compile(r"opening\s+balance|closing\s+balance|balance\s+b/?f|balance\s+c/?f|brought\s+forward", re.I)
_CREDIT_HINTS = re.compile(r"\b(?:cr|credit|received|from|deposit|inflow|reversal|refund|lodgement)\b", re.I)


def normalize(
    raw: pd.DataFrame,
    *,
    date_formats: Optional[Sequence[str]] = None,
    order: str = "auto",
    opening_balance: Optional[float] = None,
    drop_patterns: Sequence[str] = (),
    tolerance: float = 0.02,
) -> tuple[pd.DataFrame, QualityReport]:
    report = QualityReport()
    if raw is None or raw.empty or "date" not in raw.columns:
        report.warnings.append("no rows extracted")
        return pd.DataFrame(columns=CANONICAL_COLUMNS), report

    raw = raw.reset_index(drop=True)
    col = lambda name: raw[name] if name in raw.columns else pd.Series([None] * len(raw))  # noqa: E731

    df = pd.DataFrame(index=raw.index)
    df["date"] = parse_dates(col("date"), date_formats)
    df["value_date"] = parse_dates(col("value_date"), date_formats) if "value_date" in raw else pd.NaT
    for f in ("narration", "reference", "channel"):
        df[f] = clean_text(col(f))
    df["balance"] = parse_amounts(col("balance"))

    # ---- amount + explicitly known direction -------------------------------
    direction = pd.Series([None] * len(raw), dtype="object")
    if {"debit", "credit"} & set(raw.columns):
        debit = parse_amounts(col("debit")).abs().fillna(0)
        credit = parse_amounts(col("credit")).abs().fillna(0)
        df["amount"] = np.where(debit > 0, debit, credit)
        direction[debit > 0] = DEBIT
        direction[(debit == 0) & (credit > 0)] = CREDIT
    else:
        signed = parse_amounts(col("amount"))
        df["amount"] = signed.abs()
        signs = col("amount").map(explicit_sign)
        direction[signs < 0] = DEBIT
        direction[signs > 0] = CREDIT
    if "direction" in raw.columns:
        known = raw["direction"].astype(str).str.lower().isin([CREDIT, DEBIT])
        direction[known] = raw.loc[known, "direction"].str.lower()
    df["direction"] = direction

    # ---- drop non-transactions ---------------------------------------------
    bad_date = df["date"].isna()
    if bad_date.any():
        report.warnings.append(f"{int(bad_date.sum())} rows dropped: unparseable date")
    keep = ~bad_date & df["amount"].fillna(0).gt(0) & ~df["narration"].str.contains(_BALANCE_LINES, na=False)
    for pattern in drop_patterns:
        keep &= ~df["narration"].str.contains(pattern, case=False, regex=True, na=False)

    extras = [c for c in raw.columns if c not in RAW_FIELDS]
    for c in extras:
        df[c] = raw[c]
    df = df[keep].reset_index(drop=True)

    # ---- chronological order (oldest first) ---------------------------------
    if order == "desc" or (order == "auto" and len(df) > 1 and df["date"].iloc[0] > df["date"].iloc[-1]):
        df = df.iloc[::-1].reset_index(drop=True)

    # ---- reconcile ----------------------------------------------------------
    df["direction"], df["reconciled"], heuristic = _reconcile(
        df["amount"].to_numpy(float), df["balance"].to_numpy(float),
        df["direction"].tolist(), df["narration"].tolist(), opening_balance, tolerance,
    )

    report.rows = len(df)
    report.rows_with_balance = int(df["balance"].notna().sum())
    report.checked = int(df["reconciled"].notna().sum())
    report.reconciled = int(df["reconciled"].fillna(False).astype(bool).sum())
    report.heuristic_directions = heuristic
    rate = report.reconciliation_rate
    if rate is not None and rate < 0.95:
        report.warnings.append(f"only {rate:.0%} of rows reconcile with the running balance")
    if heuristic:
        report.warnings.append(f"{heuristic} row(s) have debit/credit guessed from narration")

    ordered = CANONICAL_COLUMNS + [c for c in extras if c in df.columns]
    return df[ordered], report


def _reconcile(amount, balance, direction, narration, opening, tol):
    n = len(amount)
    dirs = list(direction)
    ok: list = [pd.NA] * n
    heuristic = 0
    prev = opening
    for i in range(n):
        a, b = amount[i], balance[i]
        has_b = not math.isnan(b)
        if prev is not None and has_b:
            if dirs[i] is None:
                dirs[i] = CREDIT if abs(prev + a - b) <= abs(prev - a - b) else DEBIT
            expected = prev + a if dirs[i] == CREDIT else prev - a
            ok[i] = bool(abs(expected - b) <= tol)
        if dirs[i] is None:
            # No previous balance (first row, no opening balance): best guess.
            dirs[i] = CREDIT if _CREDIT_HINTS.search(narration[i] or "") else DEBIT
            heuristic += 1
        if has_b:
            prev = b
        elif prev is not None:
            prev = prev + a if dirs[i] == CREDIT else prev - a
    return pd.Series(dirs, dtype="object"), pd.Series(ok, dtype="boolean"), heuristic
