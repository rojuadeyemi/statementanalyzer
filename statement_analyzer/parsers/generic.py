"""Bank-agnostic parsers for layouts we have no spec for.

Four strategies, tried by the pipeline and scored against the running balance:

1. ``ColumnLayoutParser`` — find the header row, take each header label's
   x-position as a column boundary, and drop every word into the column it sits
   under. Lines without a date are wrapped text of the row above.
2. ``TableParser`` — pdfplumber's ruled tables, with header sniffing (exact
   known headers, the "Account Statement" sub-header case, the Keystone
   single-cell header) and **row splitting for merged cells**.
3. ``TextRowParser`` — no tables at all: run the row splitters straight over the
   page text. This rescues statements where pdfplumber finds no ruled table and
   the header is unreadable.
4. ``extract_transaction_moniepoint`` — the fixed-header table path.

The row splitter (``split_transaction_row``) keeps **every** regex from the
original generic.py, in the original order, so a row that used to match
pattern N still matches pattern N. What changed around them only adds:

* Patterns use **named groups**, so a match produces canonical fields
  (date / narration / debit / credit / balance / ...) instead of a positional
  list that had to line up with whatever header was found. Misaligned columns
  were a large share of the old failures.
* ``split_by_structure`` catches rows none of the 11 patterns match: it finds
  the dates and the trailing numbers and infers the rest, so it does not care
  about column order.
* Rows whose header mapping produces nothing are re-tried through the splitter
  instead of being dropped, and ``TextRowParser`` applies the splitters to raw
  page text when there is no usable table at all.

The patterns are module-level compiled objects: tidier, though not much faster,
since ``re`` caches compiled patterns anyway. The gain here is coverage, not CPU.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Iterable, Optional

import pandas as pd

from .base import Document, StatementParser

log = logging.getLogger(__name__)

TABLE_SETTINGS = {"intersection_tolerance": 10}
DEFAULT_KEYWORDS = ("balance",)

# ---------------------------------------------------------------------------
# Header vocabulary
# ---------------------------------------------------------------------------
HEADER_SYNONYMS: dict[str, list[str]] = {
    "value_date": ["value date", "val date", "value dt", "val. date", "val. dt"],
    "date": [
        "transaction date", "trans date", "trans. date", "tran date", "txn date",
        "posting date", "post date", "date posted", "booking date", "entry date",
        "date/time", "date", "dates",
    ],
    "narration": [
        "transaction details", "transaction detail", "transaction description",
        "narration", "narrative", "description", "details", "remarks", "remark",
        "particulars", "memo", "transaction remarks",
    ],
    "reference": [
        "reference id", "reference no", "reference", "ref no", "ref", "transaction id",
        "chq no", "cheque no", "instrument no",
    ],
    "debit": [
        "debit amount", "withdrawals", "withdrawal", "money out", "debits", "debit",
        "dr", "outflow", "amount out",
    ],
    "credit": [
        "credit amount", "lodgements", "lodgement", "deposits", "deposit", "money in",
        "credits", "credit", "cr", "inflow", "amount in",
    ],
    "amount": ["transaction amount", "amount"],
    "balance": [
        "running balance", "available balance", "closing balance", "balance",
        "balance after", "ledger balance",
    ],
    "channel": ["channel", "transaction type", "type"],
}
_NOISE_WORDS = {"ngn", "(ngn)", "₦", "(₦)", "n", "#"}

# Exact header rows seen in the wild — checked before the fuzzy sniff so that a
# known layout can never be mis-detected.
KNOWN_HEADERS: list[list[str]] = [
    ["Date", "Transaction Details", "Reference", "Value Date", "Withdrawals", "Lodgements", "Balance"],
    ["TXN DATE", "VAL DATE", "REMARKS", "DEBIT", "CREDIT", "BALANCE"],
    ["", "Transaction Date", "Transaction Detail", "Money In (NGN)", "Money Out (NGN)", "Transaction ID", ""],
]

# (tokens, field) sorted longest first so "value date" wins over "date".
_SYNONYM_TOKENS = sorted(
    ((tuple(s.split()), f) for f, syns in HEADER_SYNONYMS.items() for s in syns),
    key=lambda t: -len(t[0]),
)

_ROW_NOISE = re.compile(
    r"page\s+\d+\s+of\s+\d+|printed\s+on|generated\s+on|opening\s+balance|closing\s+balance"
    r"|total\s+(?:debit|credit|withdrawal|lodgement)|brought\s+forward|carried\s+forward",
    re.I,
)
# Header-ish rows that are NOT the transaction header (kept from the old sniff).
_NOT_A_HEADER = ("print. date", "transaction description", "total credits", "opening")

_DATE_START = re.compile(
    r"^\s*(?:\d{1,4}[-/. ][A-Za-z\d]{1,9}[-/. ]\d{2,4}|\d{1,2}\s?[A-Za-z]{3,9}\s?\d{2,4}|[A-Za-z]{3}\s+\d{1,2},?\s*\d{4})"
)


def _norm_token(word: str) -> str:
    return re.sub(r"[^a-z0-9/.]", "", str(word).lower()).strip(".")


def match_header(cells: list[str]) -> Optional[list[Optional[str]]]:
    """Map header cell texts to canonical fields (None per unrecognised cell)."""
    mapping: list[Optional[str]] = []
    for cell in cells:
        tokens = tuple(t for t in (_norm_token(w) for w in str(cell or "").split()) if t and t not in _NOISE_WORDS)
        mapping.append(next((f for syn, f in _SYNONYM_TOKENS if tokens == syn), None))
    return mapping if _is_header(mapping) else None


def _is_header(fields: list[Optional[str]]) -> bool:
    found = {f for f in fields if f}
    has_money = bool(found & {"amount", "debit", "credit", "balance"})
    return ("date" in found or "value_date" in found) and has_money and len(found) >= 3


# ===========================================================================
# Row splitting — every pattern from the original generic.py, compiled once
# ===========================================================================
# Shared building blocks (identical to the original sub-expressions).
_DT = r"\d{2}\s+[A-Za-z]{3}\s+\d{4}\s+\d{2}:\d{2}(?::\d{2})?"      # 02 May 2025 10:11(:12)
_DT_SEC = r"\d{4} \w{3} \d{2} \d{2}:\d{2}:\d{2}"                    # 2025 May 02 10:11:12
_DT_LOOSE = r"\d{4} \w{3} \d{2} \d{2}:(?:\d{2})?(?::\d{2})?:?"      # 2025 May 02 06: / 19:20:
_DMY = r"\d{2} \w{3} \d{4}"                                          # 02 May 2025
_DMY_WS = r"\d{2}\s+[A-Za-z]{3}\s+\d{4}"
_NUM = r"[\+\-]?\d{1,3}(?:,\d{3})*(?:\.\d{2})?"
_UNSIGNED = r"\d{1,3}(?:,\d{3})*(?:\.\d{2})?"
_DASH_NUM = r"--|[\-\d,.]+"
_DASH_BAL = r"--|[\d,\.]*"
_CHANNEL = r"[a-zA-Z\-]+"

# Order matters: it is the original order, so a row that used to match pattern N
# still matches pattern N.
ROW_PATTERNS: list[re.Pattern] = [re.compile(p) for p in (
    # 1. old pattern_7 — datetime, value date, narration, debit, credit, balance, channel, ref
    rf"^.*?(?P<date>{_DT})\s*:?\s*\d+?\s*(?P<value_date>{_DMY_WS})\s+(?P<narration>.*)\s+"
    rf"(?P<debit>{_DASH_NUM})\s+(?P<credit>{_DASH_NUM})\s+(?P<balance>{_NUM})\s+"
    rf"(?P<channel>.*?)\s*(?P<reference>.+)$",

    # 2. old pattern_8 — narration before the value date
    rf"(?P<date>{_DT})\s*(?P<narration>.+?)\s+(?P<value_date>{_DMY_WS})\s+"
    rf"(?P<debit>{_DASH_NUM})\s+(?P<credit>{_DASH_NUM})\s+(?P<balance>{_NUM})\s+"
    rf"(?P<channel>.*?)\s+(?P<reference>.+)$",

    # 3. old "pattern" — optional txn date, optional row index, then value date
    rf"\s*(?:(?P<date>(?:{_DMY})|(?:{_DT_LOOSE}))\s*)?(?:\d{{1,3}}\s+)?"
    rf"(?P<value_date>{_DMY})\s+(?P<narration>.+?)\s+(?P<amount>{_NUM})\s+"
    rf"(?P<balance>{_UNSIGNED})\s+(?P<channel>{_CHANNEL})\s+(?P<reference>[\w:\s]+)",

    # 4. old fallback — trailing "<n> point"
    rf"^\s*(?P<date>{_DT_LOOSE})\s+(?P<narration>.+?)\s+(?P<value_date>{_DMY})\s+"
    rf"(?P<amount>{_NUM})\s+(?P<balance>{_UNSIGNED})\s+(?P<channel>{_CHANNEL})\s+"
    rf"(?P<reference>[\w\d]+)\s+\d+\s+point$",

    # 5. old second_fallback — reference before the value date, "point" mid-row
    rf"^\s*(?P<date>{_DT_LOOSE})\s+(?P<reference>[\w\d]+)\s+(?P<value_date>{_DMY})\s+"
    rf"(?P<narration>.+?)\s+point\s+(?P<amount>{_NUM})\s+(?P<balance>{_UNSIGNED})\s+"
    rf"(?P<channel>{_CHANNEL})\s+\d+\s+\d+$",

    # 6. old pattern_1 — description first
    rf"^(?P<narration>.+?)\s+(?P<date>{_DT_SEC})\s+(?P<value_date>{_DMY})\s+"
    rf"(?P<amount>{_NUM})\s+(?P<balance>{_UNSIGNED})\s+(?P<channel>{_CHANNEL})\s+(?P<reference>\d{{15,}})",

    # 7. old pattern_2 — datetime first
    rf"^(?P<date>\d{{4}} \w{{3}} \d{{2}} \d{{2}}:\d{{2}}:?)\s+(?P<narration>.+?)\s+(?P<value_date>{_DMY})\s+"
    rf"(?P<amount>{_NUM})\s+(?P<balance>{_UNSIGNED})\s+(?P<channel>{_CHANNEL})\s+(?P<reference>\d{{15,}})",

    # 8. old pattern_3 — balance may be "--"
    rf"^(?P<date>\d{{4}} \w{{3}} \d{{2}} \d{{2}}:\d{{2}}(?::\d{{2}})?):?\s+(?P<value_date>{_DMY})\s+"
    rf"(?P<narration>.+?)\s+(?P<amount>{_NUM})\s+(?P<balance>{_DASH_BAL})\s+"
    rf"(?P<channel>{_CHANNEL})\s+(?P<reference>[A-Z0-9]+)",

    # 9. old pattern_4 — row index + value date only
    rf"^\s*\d{{1,3}}\s+(?P<value_date>{_DMY})\s+(?P<narration>.+?)\s+(?P<amount>{_NUM})\s+"
    rf"(?P<balance>{_DASH_BAL})\s+(?P<channel>{_CHANNEL})\s+(?P<reference>[A-Z0-9]+)",

    # 10. old pattern_5 — narration first, then datetime + value date + debit/credit
    rf"(?P<narration>.*?)\s+(?P<date>{_DMY_WS}\s+\d{{2}}:\d{{2}}:\d{{2}})\s+(?P<value_date>{_DMY_WS})\s+"
    rf"(?P<debit>{_DASH_NUM})\s+(?P<credit>{_DASH_NUM})\s+(?P<balance>{_NUM})\s+"
    rf"(?P<channel>.*?)\s*(?P<reference>.+)$",

    # 11. old pattern_6 — row index first
    rf"\d+\s+(?P<date>{_DMY_WS}\s+\d{{2}}:\d{{2}}:\d{{2}})\s+(?P<value_date>{_DMY_WS})\s+"
    rf"(?P<narration>.+?)\s+(?P<debit>[\-\d,.]+)\s+(?P<credit>[\-\d,.]+)\s+(?P<balance>{_NUM})\s+"
    rf"(?P<channel>.*?)\s+(?P<reference>.+)$",
)]

# --- structural fallback ---------------------------------------------------
_ANY_DATE = re.compile(
    r"\d{1,2}[-/ ][A-Za-z]{3,9}[-/ ]\d{2,4}"      # 02 May 2025 / 02-May-25
    r"|\d{4}[-/][01]?\d[-/][0-3]?\d"              # 2025-05-02
    r"|[0-3]?\d[-/][01]?\d[-/]\d{2,4}"            # 02/05/2025
    r"|\d{4}\s+[A-Za-z]{3}\s+\d{1,2}"             # 2025 May 02
    r"|[A-Za-z]{3}\s+\d{1,2},\s?\d{4}"            # May 02, 2025
)
_TRAILING_TIME = re.compile(r"\s*\d{1,2}:\d{2}(?::\d{2})?:?\s*$")
_MONEY_TOKEN = re.compile(r"^(?:--|[\+\-]?\d[\d,]*(?:\.\d{1,2})?)$")
_REF_TOKEN = re.compile(r"^[A-Za-z][A-Za-z\-]*$|^[A-Z0-9\-/]{6,}$")


def split_by_structure(text: str) -> Optional[dict]:
    """Layout-agnostic splitter: locate the dates, then the trailing numbers.

    Used only when none of the explicit patterns match. It is what makes
    genuinely inconsistent layouts survive: it needs a date and at least one
    amount, and does not care about column order beyond that.
    """
    dates = list(_ANY_DATE.finditer(text))
    if not dates:
        return None

    tail = text[dates[-1].end():]
    tokens = tail.split()
    numbers: list[str] = []
    trailing: list[str] = []
    i = len(tokens)
    while i > 0:
        token = tokens[i - 1]
        if _MONEY_TOKEN.match(token):
            numbers.insert(0, token)
            i -= 1
            continue
        if not numbers and _REF_TOKEN.match(token) and len(trailing) < 2:
            trailing.insert(0, token)      # channel / reference printed after the money
            i -= 1
            continue
        break
    if not numbers:
        return None

    record: dict = {"date": dates[0].group(0)}
    if len(dates) > 1:
        record["value_date"] = dates[1].group(0)

    narration = " ".join(tokens[:i]).strip(" -:|")
    if not narration:
        # Narration sits before the date (some layouts print it first).
        narration = _TRAILING_TIME.sub("", text[: dates[0].start()]).strip(" -:|")
    record["narration"] = narration

    if len(numbers) >= 3:
        record["debit"], record["credit"], record["balance"] = numbers[0], numbers[1], numbers[-1]
    elif len(numbers) == 2:
        record["amount"], record["balance"] = numbers[0], numbers[1]
    else:
        record["amount"] = numbers[0]

    if trailing:
        record["channel"] = trailing[0]
        if len(trailing) > 1:
            record["reference"] = trailing[-1]
    return record


def split_transaction_row(row_text: str) -> Optional[dict]:
    """Split one merged/unstructured row into canonical fields.

    Returns a dict (not a positional list), so the caller never has to guess
    which column a value belongs to. ``None`` when nothing matches.
    """
    if not row_text:
        return None
    clean = re.sub(r"\s+", " ", str(row_text).replace("\n", " ").replace("|", " ")).strip()
    if len(clean) < 8:
        return None

    for rx in ROW_PATTERNS:
        m = rx.match(clean)
        if not m:
            continue
        record = {k: v.strip() for k, v in m.groupdict().items() if v and v.strip()}
        if record.get("balance") == "--":
            record.pop("balance")
        for key in ("debit", "credit"):
            if record.get(key) == "--":
                record.pop(key)
        record.setdefault("date", record.get("value_date"))
        return record

    return split_by_structure(clean)


# ===========================================================================
# Table helpers (kept from the original, now returning canonical records)
# ===========================================================================
def is_single_cell_row(row: Iterable) -> bool:
    return sum(1 for cell in row if cell) == 1


def remove_blank_first2rows(row: Optional[list]) -> list:
    """Drop up to two leading blank cells and every trailing blank cell."""
    if not row:
        return []
    row = list(row)
    if len(row) >= 2:
        if not row[0] and not row[1]:
            row = row[2:]
        elif not row[0]:
            row = row[1:]
    while row and (row[-1] is None or str(row[-1]).strip() == ""):
        row = row[:-1]
    return row


def _clean_table(table: list[list]) -> list[list]:
    cleaned = [[cell for cell in row if cell is not None] for row in table or []]
    return [row for row in cleaned if any(str(c).strip() for c in row) and len(row) > 1]


def find_transaction_table(tables, keywords=DEFAULT_KEYWORDS):
    """Locate the header row and the rows beneath it. Returns (header, rows).

    Keeps every rule from the original sniff:
      * exact known header rows,
      * the "Account Statement" title with the real header on the next row,
      * a keyword/synonym match on the first row,
      * rows that merely *look* like a header ("Print. Date", "Total Credits",
        "Opening ...") are rejected.
    """
    for table in tables or []:
        cleaned = _clean_table(table)
        if not cleaned:
            continue

        for i, row in enumerate(cleaned):
            stripped = [str(c).strip() for c in row]
            if any(stripped == known or row == known for known in KNOWN_HEADERS) or match_header(row):
                return row, cleaned[i:]

        first = str(cleaned[0]).lower()
        if "account statement" in first:
            if len(cleaned) < 3:
                continue
            second = str(cleaned[1]).lower()
            if "print. date" not in second and any(k in second for k in keywords):
                return cleaned[1], cleaned[1:]
        elif any(k in first for k in keywords) and all(k not in first for k in _NOT_A_HEADER):
            return cleaned[0], cleaned
    return None, None


def _labels_from_text(text: str) -> tuple[list[str], list[Optional[str]]]:
    """Split a run-together header into labels, keeping multi-word ones intact.

    "Trans Date Value Date Narration Amount Balance" -> five columns, not seven.
    The original code split on spaces, which put one word in each column and
    shifted every value in the row.
    """
    words = str(text).split()
    tokens = [_norm_token(w) for w in words]
    labels: list[str] = []
    fields: list[Optional[str]] = []
    i = 0
    while i < len(words):
        if not tokens[i] or tokens[i] in _NOISE_WORDS:
            i += 1
            continue
        for syn, field in _SYNONYM_TOKENS:
            k = len(syn)
            if tuple(tokens[i:i + k]) == syn:
                labels.append(" ".join(words[i:i + k]))
                fields.append(field)
                i += k
                break
        else:
            labels.append(words[i])
            fields.append(None)
            i += 1
    return labels, fields


def _header_fields(header: list) -> tuple[list[Optional[str]], list]:
    """Canonical field per header cell, handling the single-cell (Keystone) case."""
    header = remove_blank_first2rows(header)
    if header and is_single_cell_row(header):
        text = str(next(c for c in header if c)).split("\nDate")[0]
        labels, fields = _labels_from_text(text)
        if _is_header(fields):
            return fields, labels
        header = text.split(" ")           # original behaviour as a last resort
    return (match_header(header) or [None] * len(header)), header


def align_and_split_table(table, header) -> list[dict]:
    """Turn table rows into canonical records.

    * merged single-cell rows go through the row splitter,
    * normal rows are trimmed/padded to the header length and mapped by field,
    * rows whose mapping yields no date/amount are re-tried through the
      splitter on their joined text (this is what saves inconsistent layouts).
    """
    fields, header = _header_fields(list(header or []))
    width = len(header)
    records: list[dict] = []

    for raw_row in table or []:
        row = remove_blank_first2rows(raw_row)
        if not row or not any(str(c).strip() for c in row if c is not None):
            continue
        row = [c for c in row if c is not None]

        if is_single_cell_row(row):
            record = split_transaction_row(str(next(c for c in row if c)))
            if record:
                records.append(record)
            continue

        padded = row[:width] + [""] * max(0, width - len(row))
        record = {f: str(v).strip() for f, v in zip(fields, padded) if f and str(v).strip()}
        if record.get("date") or record.get("value_date"):
            records.append(record)
            continue

        fallback = split_transaction_row(" ".join(str(c) for c in row if c))
        if fallback:
            records.append(fallback)
        elif record and records:
            # No date and unsplittable: wrapped narration for the row above.
            extra = record.get("narration") or " ".join(str(c) for c in row if c)
            if extra and not _ROW_NOISE.search(extra):
                records[-1]["narration"] = f"{records[-1].get('narration', '')} {extra}".strip()
    return records


# ===========================================================================
# 1. Column layout parser (word positions)
# ===========================================================================
@dataclass
class _Column:
    field: Optional[str]
    x0: float
    x1: float


def _group_lines(words: list[dict], tolerance: float = 3.0) -> list[list[dict]]:
    lines: list[list[dict]] = []
    for w in sorted(words, key=lambda w: (round(w["top"]), w["x0"])):
        if lines and abs(lines[-1][0]["top"] - w["top"]) <= tolerance:
            lines[-1].append(w)
        else:
            lines.append([w])
    return [sorted(ln, key=lambda w: w["x0"]) for ln in lines]


def _header_columns(line: list[dict]) -> Optional[list[_Column]]:
    tokens = [_norm_token(w["text"]) for w in line]
    cols: list[_Column] = []
    i = 0
    while i < len(line):
        if not tokens[i] or tokens[i] in _NOISE_WORDS:
            i += 1
            continue
        for syn, field in _SYNONYM_TOKENS:
            k = len(syn)
            if tuple(tokens[i:i + k]) == syn:
                cols.append(_Column(field, line[i]["x0"], line[i + k - 1]["x1"]))
                i += k
                break
        else:
            cols.append(_Column(None, line[i]["x0"], line[i]["x1"]))
            i += 1
    merged: list[_Column] = []
    for c in cols:
        if merged and c.field is None and merged[-1].field is None:
            merged[-1].x1 = c.x1
        else:
            merged.append(c)
    return merged if _is_header([c.field for c in merged]) else None


_TEXT_FIELDS = {"narration", "reference", "channel", None}


def _boundaries(cols: list[_Column]) -> list[float]:
    """Text columns run up to the next label; other columns split at the midpoint."""
    bounds = []
    for cur, nxt in zip(cols, cols[1:]):
        bounds.append(nxt.x0 - 1 if cur.field in _TEXT_FIELDS else (cur.x1 + nxt.x0) / 2)
    return bounds + [float("inf")]


def _assign(words: list[dict], cols: list[_Column], bounds: list[float]) -> dict[str, str]:
    cells: dict[str, list[str]] = {}
    for w in words:
        center = (w["x0"] + w["x1"]) / 2
        idx = next(i for i, b in enumerate(bounds) if center <= b)
        field = cols[idx].field
        if field:
            cells.setdefault(field, []).append(w["text"])
    return {k: " ".join(v) for k, v in cells.items()}


class ColumnLayoutParser(StatementParser):
    name = "Generic (column layout)"
    TEXT_FIELDS = ("narration", "reference", "channel")

    @staticmethod
    def _find_header(lines: list[list[dict]]) -> tuple[Optional[list[_Column]], int]:
        for idx, line in enumerate(lines):
            found = _header_columns(line)
            if found:
                return found, idx + 1
        for idx in range(len(lines) - 1):      # header wrapped over two lines
            found = _header_columns(sorted(lines[idx] + lines[idx + 1], key=lambda w: w["x0"]))
            if found:
                return found, idx + 2
        return None, 0

    def parse(self, doc: Document) -> pd.DataFrame:
        if doc.pdf is None:
            return pd.DataFrame()

        records: list[dict] = []
        cols: Optional[list[_Column]] = None
        for page in doc.pdf.pages:
            lines = _group_lines(page.extract_words(keep_blank_chars=False, use_text_flow=False))
            found, start = self._find_header(lines)
            if found:
                cols = found
            if cols is None:
                continue
            bounds = _boundaries(cols)
            date_field = "date" if any(c.field == "date" for c in cols) else "value_date"

            current: Optional[dict] = None
            for line in lines[start:]:
                text = " ".join(w["text"] for w in line)
                if _header_columns(line):
                    continue
                cells = _assign(line, cols, bounds)
                if _DATE_START.match(cells.get(date_field, "")):
                    current = cells
                    records.append(current)
                elif current is not None and not _ROW_NOISE.search(text):
                    for f in self.TEXT_FIELDS:            # wrapped narration
                        if cells.get(f):
                            current[f] = f"{current.get(f, '')} {cells[f]}".strip()
        return pd.DataFrame.from_records(records)


# ===========================================================================
# 2. Ruled-table parser
# ===========================================================================
class TableParser(StatementParser):
    name = "Generic (table)"

    def __init__(self, keywords: Iterable[str] = DEFAULT_KEYWORDS):
        self.keywords = tuple(keywords)

    def parse(self, doc: Document) -> pd.DataFrame:
        if doc.pdf is None:
            return pd.DataFrame()
        return pd.DataFrame.from_records(extract_generic_records(doc.pdf, self.keywords))


def extract_generic_records(pdf, keywords: Iterable[str] = DEFAULT_KEYWORDS) -> list[dict]:
    """Walk every table on every page and collect canonical records."""
    keywords = tuple(keywords)
    records: list[dict] = []
    header: Optional[list] = None

    for page in pdf.pages:
        try:
            tables = page.extract_tables(TABLE_SETTINGS) or []
        except Exception:                      # a bad page must not stop the document
            log.exception("table extraction failed on a page")
            continue

        page_header, page_table = find_transaction_table(tables, keywords)
        if page_header:
            header = page_header
        if not header:
            continue

        # Header-only table on this page: the rows live in the next table.
        if page_header and page_table is not None and len(page_table) < 2 and len(tables) > 1:
            for table in tables[1:]:
                records.extend(align_and_split_table(_clean_table(table), header))
            continue

        for table in tables:
            rows = _clean_table(table)
            if not rows:
                continue
            if rows is page_table or (page_header and rows and rows[0] == page_header):
                rows = rows[1:]                # drop the header row itself
            elif match_header(rows[0]):
                rows = rows[1:]                # repeated header on a later page
            records.extend(align_and_split_table(rows, header))
    return records


# ===========================================================================
# 3. Text row parser — no tables needed
# ===========================================================================
class TextRowParser(StatementParser):
    """Runs the row splitters straight over the page text.

    For statements where pdfplumber draws no usable table and the header cannot
    be read, this is often the only thing that extracts anything. Rows that the
    splitters reject are simply skipped, so noise lines cost nothing.
    """

    name = "Generic (text rows)"

    def parse(self, doc: Document) -> pd.DataFrame:
        records: list[dict] = []
        lines = doc.lines()
        i, n = 0, len(lines)
        while i < n:
            line = lines[i]
            if _ROW_NOISE.search(line):
                i += 1
                continue
            record = split_transaction_row(line)
            if not record and i + 1 < n:       # row wrapped over two printed lines
                record = split_transaction_row(f"{line} {lines[i + 1]}")
                if record:
                    i += 1
            if record and (record.get("amount") or record.get("debit") or record.get("credit")):
                records.append(record)
            i += 1
        return pd.DataFrame.from_records(records)


# ===========================================================================
# 4. Fixed-header table path (Moniepoint-style exports)
# ===========================================================================
MONIEPOINT_HEADER = ["date", "narration", "reference", "debit", "credit", "balance"]
MONIEPOINT_SIGNATURE = re.compile(r"[A-Z][a-z]+\s*\d{2}\s*[A-Z][a-z]+\s*Page")


class MoniepointTableParser(StatementParser):
    """Table with known columns but no printed header row."""

    name = "Generic (fixed-header table)"

    def detect(self, doc: Document) -> bool:
        return bool(MONIEPOINT_SIGNATURE.search(doc.first))

    def parse(self, doc: Document) -> pd.DataFrame:
        return extract_transaction_moniepoint(doc.pdf) if doc.pdf is not None else pd.DataFrame()


def extract_transaction_moniepoint(pdf) -> pd.DataFrame:
    """Tables whose columns are known but whose header row is not printed."""
    records: list[dict] = []
    for page in pdf.pages:
        try:
            tables = page.extract_tables() or []
        except Exception:
            log.exception("table extraction failed on a page")
            continue
        if not tables or not tables[0]:
            continue
        for row in tables[0]:
            cells = [str(c).strip() if c is not None else "" for c in row][:len(MONIEPOINT_HEADER)]
            if not any(cells) or not cells[0]:
                continue
            records.append({f: v for f, v in zip(MONIEPOINT_HEADER, cells) if v})
    return pd.DataFrame.from_records(records)


# ===========================================================================
# Backwards-compatible entry point
# ===========================================================================
def extract_transaction_generic(pdf, keywords: Iterable[str] = DEFAULT_KEYWORDS) -> pd.DataFrame:
    """Original entry point. Returns canonical columns instead of bank headers.

    Order: the Moniepoint signature, then ruled tables, then raw text rows —
    whichever produces rows first.
    """
    first_page_text = (pdf.pages[0].extract_text() or "") if pdf.pages else ""
    if MONIEPOINT_SIGNATURE.search(first_page_text):
        df = extract_transaction_moniepoint(pdf)
        if not df.empty:
            return df

    df = pd.DataFrame.from_records(extract_generic_records(pdf, keywords))
    if not df.empty:
        return df

    doc = Document(pages=[p.extract_text() or "" for p in pdf.pages], pdf=pdf)
    return TextRowParser().parse(doc)
