"""Single entry point: anything in -> enriched ``Statement`` out."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

from dateutil.relativedelta import relativedelta

from .config import DEFAULT_SETTINGS, Settings
from .enrich import Rule, enrich
from .models import Statement
from .sources import from_dict, parse_pdf


def _decode(value: Any, max_depth: int = 5) -> Any:
    """Unwrap JSON that may have been string-encoded several times."""
    for _ in range(max_depth):
        if not isinstance(value, str):
            break
        value = value.strip()
        if not value:
            return {}
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            break
    return value


def load_statement(
    source: Any,
    *,
    settings: Settings = DEFAULT_SETTINGS,
    field_mapping: Optional[dict] = None,
    rules: Optional[list[Rule]] = None,
) -> Statement:
    """Load a PDF path, JSON path, JSON string or dict into an enriched Statement."""
    if isinstance(source, (str, Path)) and Path(str(source)).suffix.lower() in {".pdf", ".json", ".txt"}:
        path = Path(source)
        if not path.exists():
            raise FileNotFoundError(path)
        if path.suffix.lower() == ".pdf":
            statement = parse_pdf(path, settings)
        else:
            statement = from_dict(_decode(path.read_text(encoding="utf-8")), field_mapping, settings)
    else:
        data = _decode(source)
        if not isinstance(data, dict):
            raise ValueError(f"Unsupported input type: {type(source).__name__}")
        statement = from_dict(data, field_mapping, settings)

    df = statement.transactions
    if not df.empty:
        cutoff = df["date"].max() - relativedelta(months=settings.cutoff_months)
        df = df[df["date"] >= cutoff]
        # Exact duplicates (same date, narration, amount AND balance) come from
        # overlapping pages; without a balance they may be genuine repeats.
        if df["balance"].notna().any():
            df = df.drop_duplicates(subset=["date", "narration", "amount", "direction", "balance"])
        statement.transactions = enrich(df.reset_index(drop=True), rules)
    return statement
