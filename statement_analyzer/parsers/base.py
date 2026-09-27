"""Parser contract + the one generic line-regex engine.

Old design: one hand-written function per bank, each re-implementing the
same loop (split lines -> regex -> float() -> deposit/withdrawal guess ->
DataFrame). New design: a bank is *data* (a ``LineSpec``). The engine does the
looping, multi-line joining and preprocessing once; the normalizer does all
number/date/direction work once.
"""
from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Sequence

import pandas as pd

# --------------------------------------------------------------------------
# Document
# --------------------------------------------------------------------------


@dataclass
class Document:
    """Extracted PDF text (one string per page) + optional pdfplumber handle."""

    pages: list[str]
    pdf: Any = None           # pdfplumber.PDF, needed only by layout/table parsers
    name: str = ""

    @property
    def first(self) -> str:
        return self.pages[0] if self.pages else ""

    @property
    def last(self) -> str:
        return self.pages[-1] if self.pages else ""

    @property
    def text(self) -> str:
        return "\n".join(self.pages)

    def lines(self) -> list[str]:
        """All non-empty lines across pages, whitespace-normalised."""
        out: list[str] = []
        for page in self.pages:
            for ln in (page or "").split("\n"):
                ln = re.sub(r"\s+", " ", ln.replace("|", " ")).strip()
                if ln:
                    out.append(ln)
        return out


# --------------------------------------------------------------------------
# Detection helpers (declarative signatures)
# --------------------------------------------------------------------------
Detector = Callable[[Document], bool]


def contains(pattern: str, where: str = "first", flags: int = re.I) -> Detector:
    """True if ``pattern`` appears on the first / last / any page."""
    rx = re.compile(pattern, flags)

    def _detect(doc: Document) -> bool:
        text = {"first": doc.first, "last": doc.last}.get(where, doc.text)
        return bool(rx.search(text or ""))

    return _detect


def all_of(*detectors: Detector) -> Detector:
    return lambda doc: all(d(doc) for d in detectors)


def any_of(*detectors: Detector) -> Detector:
    return lambda doc: any(d(doc) for d in detectors)


# --------------------------------------------------------------------------
# Parser contract
# --------------------------------------------------------------------------


class StatementParser(ABC):
    """Turns a Document into a *raw* DataFrame whose columns are RAW_FIELDS.

    Parsers never convert numbers/dates or guess debit vs credit — that is the
    normalizer's job, done identically for every bank.
    """

    name: str = "base"
    date_formats: Optional[Sequence[str]] = None
    drop_patterns: Sequence[str] = ()
    order: str = "auto"            # "auto" | "asc" | "desc" (newest first)

    def detect(self, doc: Document) -> bool:  # generic parsers accept anything
        return True

    @abstractmethod
    def parse(self, doc: Document) -> pd.DataFrame: ...


# --------------------------------------------------------------------------
# Regex building blocks
# --------------------------------------------------------------------------
# Strict: thousands separators + exactly two decimals ("1,234.56", "-50.00")
AMT = r"[-+]?\d{1,3}(?:,?\d{3})*\.\d{2}"
# Loose: decimals optional ("500", "+1,200", "3,000.5")
LOOSE_AMT = r"[-+]?\d[\d,]*(?:\.\d+)?"


@dataclass
class LineSpec(StatementParser):
    """Declarative definition of a text-line based statement layout.

    patterns        Regexes with named groups from RAW_FIELDS. Tried in order.
    join_lines      Also try joining up to N consecutive lines (wrapped rows).
    narration_before  If a matched row has no narration, use the previous
                    (unmatched) line — layouts that print narration above.
    preprocess      Optional function(lines) -> lines for layout quirks.
    transform       Optional function(record) -> record for row-level quirks.
    """

    name: str = "line-spec"
    detector: Detector = field(default=lambda doc: False)
    patterns: Sequence[str] = ()
    date_formats: Optional[Sequence[str]] = None
    join_lines: int = 1
    narration_before: bool = False
    preprocess: Optional[Callable[[list[str]], list[str]]] = None
    transform: Optional[Callable[[dict], Optional[dict]]] = None
    drop_patterns: Sequence[str] = ()
    order: str = "auto"

    def __post_init__(self) -> None:
        self._compiled = [re.compile(p) for p in self.patterns]

    def detect(self, doc: Document) -> bool:
        return self.detector(doc)

    def _match(self, text: str) -> Optional[dict]:
        for rx in self._compiled:
            m = rx.search(text)
            if m:
                return {k: v for k, v in m.groupdict().items() if v is not None}
        return None

    def parse(self, doc: Document) -> pd.DataFrame:
        lines = doc.lines()
        if self.preprocess:
            lines = self.preprocess(lines)

        records: list[dict] = []
        last_used = -1
        i, n = 0, len(lines)
        while i < n:
            record, used = None, 0
            for k in range(1, self.join_lines + 1):
                if i + k > n:
                    break
                record = self._match(" ".join(lines[i:i + k]))
                if record:
                    used = k
                    break

            if not record:
                i += 1
                continue

            if self.narration_before and not record.get("narration", "").strip():
                if i > 0 and last_used < i - 1:
                    record["narration"] = lines[i - 1]

            if self.transform:
                record = self.transform(record)
            if record:
                records.append(record)
            last_used = i + used - 1
            i += used

        return pd.DataFrame.from_records(records)
