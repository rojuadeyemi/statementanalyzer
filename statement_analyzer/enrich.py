"""Categorisation and counterparty extraction.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

import pandas as pd

from .config import DEFAULT_SETTINGS, Settings
from .models import CREDIT, DEBIT
from .pattern import GAMBLING_PATTERN, REPAYMENT_PATTERN,LOAN_RECEIVED, SALARY_RECEIVED, ALLOWANCES,EXCLUSION,TRANSFER_PATTERN

UNCATEGORISED = "others"


@dataclass(frozen=True)
class Rule:
    category: str
    pattern: str = None
    direction: Optional[str] = None
    min_amount: float = 0.0


def default_rules(s: Settings = DEFAULT_SETTINGS) -> list[Rule]:
    return [
        Rule("reversal", r"revers|rvsl|\bREV-|returned|refund"),
        Rule("charges", EXCLUSION),
        Rule("VAS", r"startimes|gotv|dstv|electricity|cable|airtime|\bmtn\b|airtel|\bglo\b|9mobile|\bvtu\b|voucher"
                    r"|internet bundle|recharge|night plan|\bdata\b"),
        Rule("bonus/allowance", ALLOWANCES,CREDIT),
        Rule("salary", SALARY_RECEIVED, CREDIT, s.min_salary_amount),
        Rule("salary_payment", SALARY_RECEIVED, DEBIT, s.min_salary_amount),
        Rule("loan_repayment", REPAYMENT_PATTERN, DEBIT, s.min_loan_repayment_amount),
        Rule("loan", LOAN_RECEIVED, CREDIT),
        Rule("betting", GAMBLING_PATTERN),
        Rule("thrift", r"contribution|\bajo\b|esusu"),
        Rule("travelling", r"embassy|\bflight|visa (?:fee|application|processing)|vfs global|tls ?contact|\bircc\b"
                           r"|uscis|express entry|airfare|one-way ticket|ielts|toefl|wes fee|cas deposit"
                           r"|sevis|proof of funds|gic payment|\bform a\b"),
        Rule("transfer", TRANSFER_PATTERN,min_amount=s.min_transfer_amount),
        Rule("transfer",min_amount=s.min_transfer_amount),
    ]


def categorize(df: pd.DataFrame, rules: Optional[list[Rule]] = None) -> pd.Series:
    rules = rules or default_rules()
    category = pd.Series(UNCATEGORISED, index=df.index, dtype="object")
    narration = df["narration"].fillna("")
    for rule in rules:
        mask = (category == UNCATEGORISED)
        if rule.pattern:
            mask &= narration.str.contains(rule.pattern, case=False, regex=True)
        if rule.direction:
            mask &= df["direction"] == rule.direction
        if rule.min_amount:
            mask &= df["amount"] >= rule.min_amount
        category[mask] = rule.category
    return category

# ---------------------------------------------------------------------------
# Counterparties
# ---------------------------------------------------------------------------
_NAME = r"[A-Za-z0-9 '\-&.]+"
_PARTY_PATTERNS = [re.compile(p, re.I) for p in (
    rf"\bTrffrm[:\s]+(?P<sender>.+?)\s+TO[:\s]+(?P<receiver>{_NAME})",
    rf"\bTrf IFO[:\s]+(?P<receiver>.+?)\s+FRM[:\s]+(?P<sender>{_NAME})",
    rf"\bTrf IFO[:\s]+(?P<sender>.+?)\s+For[:\s]+(?P<receiver>{_NAME})",
    rf"\bTrfBy[:\s]*(?P<sender>.+?)\s+IFO[:\s]+(?P<receiver>{_NAME})",
    rf"\bFRM[:\s]+(?P<sender>.+?)\s+TO[:\s]+(?P<receiver>{_NAME})",
    rf"\bFrom[:\s]+(?P<sender>{_NAME}?)\s+To[:\s]+(?P<receiver>{_NAME})",
    rf"\bTo[:\s]+(?P<receiver>{_NAME}?)\s+From[:\s]+(?P<sender>{_NAME})",
)]
_ONLY_RECEIVER = re.compile(rf"\bto[:\s]+(?P<receiver>{_NAME})", re.I)
_ONLY_SENDER = re.compile(rf"\b(?:from|by)[:\s]+(?P<sender>{_NAME})", re.I)
_TRAILING_JUNK = re.compile(r"\b(?:ref|reference|narration|via|on|at)\b.*$|\d{6,}.*$", re.I)


def _clean_party(value: Optional[str], keep: str) -> Optional[str]:
    if not value:
        return None
    parts = [p for p in re.split(r"[|/]", value) if p.strip()]
    if not parts:
        return None
    value = parts[-1] if keep == "last" else parts[0]
    value = _TRAILING_JUNK.sub("", value).strip(" -:.")
    value = re.sub(r"\s+", " ", value)
    return value.title() if len(value) > 1 else None


@lru_cache(maxsize=50_000)
def extract_counterparties(narration: str) -> tuple[Optional[str], Optional[str]]:
    """Return (sender, receiver) from a narration."""
    if not narration:
        return None, None
    text = re.sub(r"\s+", " ", narration).replace("GTBank/", "").replace("/NIP Transfer", "")
    for rx in _PARTY_PATTERNS:
        m = rx.search(text)
        if m:
            return _clean_party(m.group("sender"), "last"), _clean_party(m.group("receiver"), "first")
    m = _ONLY_RECEIVER.search(text)
    if m:
        return None, _clean_party(m.group("receiver"), "first")
    m = _ONLY_SENDER.search(text)
    if m:
        return _clean_party(m.group("sender"), "last"), None
    return None, None


def enrich(df: pd.DataFrame, rules: Optional[list[Rule]] = None) -> pd.DataFrame:
    df = df.copy()
    df["category"] = categorize(df, rules)
    parties = df["narration"].fillna("").map(extract_counterparties)
    df["sender"] = parties.str[0]
    df["receiver"] = parties.str[1]
    df["month"] = df["date"].dt.strftime("%Y-%m")
    iso = df["date"].dt.isocalendar()
    df["week"] = (iso["year"] * 100 + iso["week"]).astype("Int64")
    return df
