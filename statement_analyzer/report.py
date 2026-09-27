"""Excel and JSON report builders (kept out of the analyzer class)."""
from __future__ import annotations

import json
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from typing import BinaryIO, Union

import numpy as np
import pandas as pd

from .analyzer import StatementAnalyzer


def _sheets(a: StatementAnalyzer) -> dict[str, pd.DataFrame | pd.Series]:
    return {
        "Statement Summary": a.risk_indicators,
        "Affordability": a.affordability.as_series(),
        "Transaction Data": a.df,
        "Month-on-Month Cashflow": a.cashflow_monthly,
        "Week-on-Week Cashflow": a.cashflow_weekly,
        "Category Cashflow": a.cashflow_by_category,
        "Account Sweep": a.account_sweep,
        "Inflow By Sender": a.inflow_sources,
        "Outflow By Receiver": a.outflow_destinations,
        "Account Balance": a.balance_by_month,
    }


def write_excel(a: StatementAnalyzer, target: Union[str, Path, BinaryIO, None] = None) -> BytesIO | Path:
    """Write to a path, a file-like object, or (default) return an in-memory buffer."""
    buffer = BytesIO() if target is None else target
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        for name, data in _sheets(a).items():
            data.to_excel(writer, sheet_name=name, index=isinstance(data, pd.Series))
    if target is None:
        buffer.seek(0)
    return buffer


def _default(obj):
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return None if np.isnan(obj) else float(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, (pd.Timestamp, datetime, date)):
        return obj.isoformat()
    if obj is pd.NA or obj is pd.NaT:
        return None
    if isinstance(obj, pd.Series):
        return obj.to_dict()
    if isinstance(obj, pd.DataFrame):
        return json.loads(obj.to_json(orient="records", date_format="iso"))
    return str(obj)


def build_json(a: StatementAnalyzer) -> dict:
    inflow = float(a.inflows["amount"].sum())
    outflow = float(a.outflows["amount"].sum())
    return json.loads(json.dumps({
        "report_generated_at": datetime.now().isoformat(timespec="seconds"),
        "extraction": {"source": a.statement.source, **a.statement.quality.as_dict()},
        "summary": {
            "total_transactions": len(a.df),
            "tenor": len(a.months),
            "start_date": str(a.df["date"].min().date()),
            "end_date": str(a.df["date"].max().date()),
            "total_inflow": inflow,
            "total_outflow": outflow,
            "net_position": inflow - outflow,
        },
        "risk_indicators": a.risk_indicators,
        "affordability": a.affordability.as_dict(),
        "cashflow_summary": a.cashflow_monthly,
        "cashflow_summary_weekly": a.cashflow_weekly,
        "cashflows_by_category": a.cashflow_by_category,
        "inflow_sources": a.inflow_sources,
        "outflow_destinations": a.outflow_destinations,
        "round_trip_transfers": a.account_sweep,
        "average_monthly_balance": a.balance_by_month,
        "loan_transactions": {"repayments": a.loan_repayments, "disbursements": a.loan_disbursements},
    }, default=_default))


def write_json(a: StatementAnalyzer, path: Union[str, Path]) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(build_json(a), indent=2), encoding="utf-8")
    return path
