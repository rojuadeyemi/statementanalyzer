"""Low-level value parsing: amounts, dates and text cleanup.

These replace the per-bank ``float(x.replace(',', ''))`` calls and the
format-guessing loops that were duplicated across the old processors.
"""
from __future__ import annotations

import math
import re
import warnings
from typing import Optional, Sequence

import pandas as pd

# --------------------------------------------------------------------------
# Amounts
# --------------------------------------------------------------------------
_EMPTY_TOKENS = {"", "-", "--", "---", "----", "n/a", "na", "nil", "none", "nan"}
_NUMBER_RE = re.compile(r"(?P<sign>[-+]?)\s*(?P<paren>\()?\s*(?P<num>\d[\d,]*(?:\.\d+)?)\s*\)?")
_DR_SUFFIX = re.compile(r"\bDR\.?$", re.I)


def parse_amount(value) -> Optional[float]:
    """Parse '1,234.50', '-500', '(200.00)', 'NGN 1,000', '1,000.00 DR', ...

    Takes the LAST number in the cell. PDF extraction often glues a stray
    fragment (a date, a row index) in front of the real amount, and the real
    amount is almost always the final token.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return None if (isinstance(value, float) and math.isnan(value)) else float(value)

    text = str(value).strip()
    if text.lower() in _EMPTY_TOKENS:
        return None
    text = text.replace("₦", " ").replace("NGN", " ").replace("\n", " ")

    matches = list(_NUMBER_RE.finditer(text))
    if not matches:
        return None
    m = matches[-1]
    number = float(m.group("num").replace(",", ""))
    negative = m.group("sign") == "-" or bool(m.group("paren")) or bool(_DR_SUFFIX.search(text))
    return -number if negative else number


def explicit_sign(value) -> int:
    """+1 / -1 if the raw text carries an explicit sign, else 0."""
    if value is None:
        return 0
    text = str(value).strip()
    if text.startswith("-") or (text.startswith("(") and text.endswith(")")) or _DR_SUFFIX.search(text):
        return -1
    if text.startswith("+") or re.search(r"\bCR\.?$", text, re.I):
        return 1
    return 0


def parse_amounts(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.map(parse_amount), errors="coerce")


# --------------------------------------------------------------------------
# Dates
# --------------------------------------------------------------------------
# Pull a date-looking token out of noisy text ("2024-01-05T10:22", "05 Jan 2024 10:12").
_DATE_TOKEN = re.compile(
    r"""
      \d{4}[-/. ]\d{1,2}[-/. ]\d{1,2}              # 2024-01-05, 2024/01/05
    | \d{4}\s+[A-Za-z]{3,9}\s+\d{1,2}              # 2024 Aug 20
    | \d{1,2}[-/. ]?[A-Za-z]{3,9}[-/. ,]*\d{2,4}   # 05-Jan-2024, 05 Jan 24, 05Jan2024
    | [A-Za-z]{3,9}[ .-]*\d{1,2},?\s*\d{4}         # Jan 05, 2024 / Sep 01,2025
    | \d{1,2}[-/.]\d{1,2}[-/.]\d{2,4}              # 05/01/2024
    """,
    re.X,
)

# After normalisation every separator is '-', so a small format list covers
# all the layouts seen across banks.
DEFAULT_DATE_FORMATS = (
    "%d-%m-%Y", "%Y-%m-%d", "%d-%b-%Y", "%d-%b-%y", "%d-%B-%Y",
    "%b-%d-%Y", "%Y-%b-%d", "%m-%d-%Y", "%d-%m-%y",
)


def _normalise_date_text(value) -> Optional[str]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    m = _DATE_TOKEN.search(text)
    if not m:
        return None
    token = m.group(0)
    token = re.sub(r"(?<=\d)(?=[A-Za-z])|(?<=[A-Za-z])(?=\d)", "-", token)
    token = re.sub(r"[\s/.,-]+", "-", token).strip("-")
    return token


def parse_dates(values: pd.Series, formats: Optional[Sequence[str]] = None) -> pd.Series:
    """Parse a column of dates.

    With ``formats`` (bank specs know theirs) every format is tried and the
    results are combined. Without it, the single format that parses the most
    values wins; ties go to day-first, which is the Nigerian convention.
    """
    tokens = values.map(_normalise_date_text).astype("object")
    if tokens.isna().all():
        return pd.Series(pd.NaT, index=values.index, dtype="datetime64[ns]")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        if formats:
            result = pd.Series(pd.NaT, index=values.index, dtype="datetime64[ns]")
            for fmt in formats:
                result = result.fillna(pd.to_datetime(tokens, format=fmt, errors="coerce"))
            return result

        best, best_rate = None, 0.0
        for fmt in DEFAULT_DATE_FORMATS:
            parsed = pd.to_datetime(tokens, format=fmt, errors="coerce")
            rate = parsed.notna().mean()
            if rate > best_rate:
                best, best_rate = parsed, rate
        if best is None or best_rate < 0.5:
            best = pd.to_datetime(tokens, errors="coerce", dayfirst=True, format="mixed")
        return best.astype("datetime64[ns]")


# --------------------------------------------------------------------------
# Text
# --------------------------------------------------------------------------
def clean_text(series: pd.Series) -> pd.Series:
    return (
        series.fillna("")
        .astype(str)
        .str.replace(r"[\r\n\t|]+", " ", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
