"""PDF pipeline: extract text -> pick parser(s) -> validate -> best result."""
from __future__ import annotations

import logging
import re
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import pdfplumber

from ..config import DEFAULT_SETTINGS, Settings
from ..models import QualityReport, Statement
from ..normalize import normalize
from ..parsers import (
    ColumnLayoutParser,
    Document,
    LLMParser,
    MoniepointTableParser,
    StatementParser,
    TableParser,
    TextRowParser,
    detect_bank,
)
from ..parsing import parse_amount

logging.getLogger("pdfminer").setLevel(logging.ERROR)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Account metadata
# ---------------------------------------------------------------------------
_NAME_RX = re.compile(
    r"(?i)\b(?:account name|acc(?:t)?\.? name|customer name|cust\. name|account summary|hello|name)"
    r"\s*[:\-]?\s*([A-Z0-9][\'A-Z0-9 &.\-]+?)(?=\s*(?:total|start|available|current|balance|currency"
    r"|account|debit|credit|address|opening|period)\b|$|,)"
)
_NUMBER_RX = re.compile(
    r"(?i)(?:acc(?:ount)?|acct|iban)?\s*(?:number|no\.?|#)?\s*[:\-]?\s*"
    r"(\d{10,14}|\d{3}X{4}\d{3}|\d{3}\s?\d{3}\s?\d{4})\b"
)
_OPENING_RX = re.compile(r"(?i)opening\s+balance[^\d\-]{0,25}(-?[\d,]+\.\d{2})")


def extract_account_meta(text: str) -> tuple[Optional[str], Optional[str]]:
    flat = re.sub(r"\s+", " ", (text or "").replace("\n", " "))
    flat = re.sub(r"\d{1,2}[-/]\d{1,2}[-/]\d{2,4}", "", flat)

    name = None
    if "CUSTOMER STATEMENT" in flat:
        m = re.search(r"CUSTOMER STATEMENT\s*(.*?)\s*(?=Trans\.|$)", flat)
    elif "NGN Type:" in flat:
        m = re.search(r"Type:\s*[A-Z]{3}\s*(.*?)\s+(?=\d+)", flat)
    else:
        m = _NAME_RX.search(flat)
    if m:
        name = (
            m.group(1).split("Opening")[0].split("Business Name")[-1]
            .replace("Account Number", "").replace("Wallet", "").strip()
        ) or None
        if name and name.lower() == "account number":
            name = None

    num = _NUMBER_RX.search(text or "")
    number = num.group(1).replace(" ", "") if num else None
    return (name.upper() if name else None), number


def find_opening_balance(text: str) -> Optional[float]:
    m = _OPENING_RX.search(text or "")
    return parse_amount(m.group(1)) if m else None


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


def _run(parser: StatementParser, doc: Document, opening, settings: Settings):
    try:
        raw = parser.parse(doc)
    except Exception:  # a broken parser must never kill the pipeline
        log.exception("parser %s crashed", parser.name)
        raw = pd.DataFrame()
    df, report = normalize(
        raw,
        date_formats=parser.date_formats,
        order=parser.order,
        opening_balance=opening,
        drop_patterns=parser.drop_patterns,
        tolerance=settings.balance_tolerance,
    )
    log.info("%s: %d rows, reconciliation=%s", parser.name, report.rows, report.reconciliation_rate)
    return parser.name, df, report


def _good_enough(report: QualityReport, settings: Settings) -> bool:
    if report.rows == 0:
        return False
    rate = report.reconciliation_rate
    return rate is None or rate >= settings.min_reconciliation


def parse_pdf(path: str | Path, settings: Settings = DEFAULT_SETTINGS) -> Statement:
    started = time.perf_counter()
    path = Path(path)
    with pdfplumber.open(path) as pdf:
        doc = Document(pages=[p.extract_text() or "" for p in pdf.pages], pdf=pdf, name=path.name)
        name, number = extract_account_meta(doc.first)
        opening = find_opening_balance(doc.first)

        results = []
        bank = detect_bank(doc)
        if bank:
            results.append(_run(bank, doc, opening, settings))

        # Generic parsers run when no spec matched OR the spec's output fails
        # validation. They are ordered cheapest-and-most-structured first; the
        # loop stops as soon as one passes validation, and the best-scoring
        # result wins in any case.
        if not results or not _good_enough(results[-1][2], settings):
            fixed_header = MoniepointTableParser()
            generic: list[StatementParser] = [ColumnLayoutParser(), TableParser(), TextRowParser()]
            if fixed_header.detect(doc):
                generic.insert(0, fixed_header)
            for parser in generic:
                results.append(_run(parser, doc, opening, settings))
                if _good_enough(results[-1][2], settings):
                    break

        best = max(results, key=lambda r: r[2].score)
        llm_note = None
        if not _good_enough(best[2], settings) and settings.enable_llm:
            llm = LLMParser(settings)
            reason = llm.unavailable_reason()
            if reason:
                # Misconfiguration must not throw away a usable rule-based result.
                llm_note = f"LLM fallback skipped: {reason}"
                log.warning(llm_note)
            else:
                results.append(_run(llm, doc, opening, settings))
                best = max(results, key=lambda r: r[2].score)

        parser_name, df, report = best
        if llm_note:
            report.warnings.append(llm_note)
        if df.empty:
            log.warning("No transactions extracted from %s (tried: %s)", path.name, [r[0] for r in results])

        return Statement(
            transactions=df,
            account_name=name,
            account_number=number,
            source=f"pdf:{parser_name}",
            opening_balance=opening,
            quality=report,
            meta={
                "file": path.name,
                "pages": len(doc.pages),
                "parsers_tried": {r[0]: r[2].as_dict() for r in results},
                "seconds": round(time.perf_counter() - started, 2),
            },
        )
