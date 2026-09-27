"""Structured (JSON) statement sources: MBS, Mono and a configurable generic one.

Each source only *maps fields*; everything else goes through the same
normalizer as PDFs.
"""
from __future__ import annotations

import json
from typing import Any, Optional

import pandas as pd

from ..config import DEFAULT_SETTINGS, Settings
from ..models import Statement
from ..normalize import normalize


def _as_records(value: Any) -> list[dict]:
    if isinstance(value, str):
        value = json.loads(value)
    return list(value or [])


def _build(raw: pd.DataFrame, source: str, settings: Settings, order: str = "auto", **meta) -> Statement:
    df, report = normalize(raw, order=order, tolerance=settings.balance_tolerance)
    return Statement(transactions=df, source=source, quality=report, **meta)


def from_mbs(data: dict, settings: Settings = DEFAULT_SETTINGS) -> Statement:
    """MyBankStatement payload: {'TicketNo':..., 'Details': [...]}."""
    records = _as_records(data["Details"])[1:]   # first record is the opening-balance row
    raw = pd.DataFrame(records).rename(columns={
        "PTransactionDate": "date", "PCredit": "credit", "PDebit": "debit",
        "PNarration": "narration", "PBalance": "balance",
    })
    return _build(raw, "mbs", settings)


def from_mono(data: dict, settings: Settings = DEFAULT_SETTINGS) -> Statement:
    """Mono payload: {'data': [{date, type, amount(kobo), narration, balance(kobo)}]}, newest first."""
    raw = pd.DataFrame(_as_records(data.get("data"))).rename(columns=str.lower)
    if raw.empty:
        return _build(raw, "mono", settings)
    raw = raw.rename(columns={"type": "direction"})
    for col in ("amount", "balance"):
        if col in raw:
            raw[col] = pd.to_numeric(raw[col], errors="coerce") / 100
    return _build(raw, "mono", settings, order="desc")


DEFAULT_GENERIC_MAPPING = {
    "transaction_date": "date",
    "description": "narration",
    "amount": "amount",
    "balance": "balance",
    "transaction_type": "direction",
}


def from_generic(data: dict, field_mapping: Optional[dict] = None, settings: Settings = DEFAULT_SETTINGS) -> Statement:
    records = data.get("transactions") or data.get("data") or data.get("records") or []
    raw = pd.DataFrame(_as_records(records)).rename(columns=field_mapping or DEFAULT_GENERIC_MAPPING)
    return _build(raw, "generic-json", settings)


def from_dict(data: dict, field_mapping: Optional[dict] = None, settings: Settings = DEFAULT_SETTINGS) -> Statement:
    """Auto-detect the JSON flavour."""
    if "bankStatement" in data:
        inner = data["bankStatement"]
        data = json.loads(inner) if isinstance(inner, str) else inner
    if "result" in data and isinstance(data["result"], str):
        data = json.loads(data["result"])
    if field_mapping:
        return from_generic(data, field_mapping, settings)
    if "Details" in data:
        return from_mbs(data, settings)
    if "data" in data:
        return from_mono(data, settings)
    return from_generic(data, None, settings)
