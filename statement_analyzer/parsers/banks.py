"""Bank-specific layouts, expressed as data.

Adding a bank = adding one ``LineSpec`` to ``BANK_PARSERS``. No new loop, no
new DataFrame code, no deposit/withdrawal guessing — the engine and the
normalizer handle all of that.

Named groups understood by the normalizer:
    date, value_date, narration, reference, channel,
    amount   (unsigned -> direction inferred from balance; or +/- signed)
    debit / credit (separate columns), balance
Any other named group is kept as an extra column.
"""
from __future__ import annotations

import re

from .base import AMT, LOOSE_AMT, LineSpec, all_of, any_of, contains

# ---------------------------------------------------------------------------
# Layout quirk handlers
# ---------------------------------------------------------------------------


def _wema_join_split_dates(lines: list[str]) -> list[str]:
    """'05-Jan-' / '<ref> <details> <amt> <bal>' / '2024'  ->  one line."""
    out, i = [], 0
    while i < len(lines):
        if (
            re.fullmatch(r"\d{2}-[A-Za-z]{3}-", lines[i])
            and i + 2 < len(lines)
            and re.fullmatch(r"\d{4}", lines[i + 2])
        ):
            out.append(f"{lines[i]}{lines[i + 2]} {lines[i + 1]}")
            i += 3
            continue
        out.append(lines[i])
        i += 1
    return out


_OPAY_AMT = r"[-+]?\d{1,3}(?:,\d{3})*(?:\.\d{2})?"
_OPAY_CLASSIC_ROW = (
    r"^.*?(?P<date>\d{2}\s+[A-Za-z]{3}\s+\d{4})\s+"
    r"(?P<narration>[A-Za-z0-9()].*?)?\s*"
    rf"(?P<amount>[-+]\d{{1,3}}(?:,\d{{3}})*(?:\.\d{{2}})?)\s+"     # always signed
    rf"(?P<balance>--|{_OPAY_AMT})\s+"
    r"(?P<channel>.*)$"
)
_OPAY_BROKEN_HEADER = re.compile(
    r"^\d{4}\s+[A-Za-z]{3}\s+\d{1,2}\s+\d{1,2}:\s*\d{0,2}:?\s+\d{1,2}\s+[A-Za-z]{3}"
)


def _opay_fix_broken_rows(lines: list[str]) -> list[str]:
    """Row split over two lines with the value-date year missing:
    '2024 Aug 20 16: 20 Aug' + '<rest>'  ->  '2024 Aug 20 16: 20 Aug 2024 <rest>'."""
    row = re.compile(_OPAY_CLASSIC_ROW)
    out, i = [], 0
    while i < len(lines):
        ln = lines[i]
        if _OPAY_BROKEN_HEADER.match(ln) and not row.search(ln):
            year = ln.split()[0]
            ln = re.sub(r"\b(\d{1,2}\s+[A-Za-z]{3})\b(?!\s+\d{4})", rf"\1 {year}", ln)
            if i + 1 < len(lines):
                ln = f"{ln} {lines[i + 1]}"
                i += 1
        out.append(ln)
        i += 1
    return out


_OPAY_V2_BODY = re.compile(rf"^(?P<narration>.*?)\s+(?P<nums>(?:{_OPAY_AMT}\s+){{3,4}}{_OPAY_AMT})\s*$")


def _opay_v2_stitch_dates(lines: list[str]) -> list[str]:
    """Opay 'settlement' layout prints the date vertically:
        '2024- ...'   (year)
        '... 09- ...' (month)
        '<narration> <amount> <debit> [credit] <bal before> [bal after]'
        '... 01T12: ...' (day)
    Rows without their own date fragments belong to the previous date.
    Output: 'YYYY-MM-DD <body>'."""
    out: list[str] = []
    year = month = None
    body = None
    last_date = None
    for ln in lines:
        is_body = bool(_OPAY_V2_BODY.search(ln))
        if not is_body and re.match(r"^\d{4}-(?:\s|$)", ln):
            if body and last_date:
                out.append(f"{last_date} {body}")
            year, month, body = ln[:4], None, None
            continue
        if not is_body and year and month is None:
            m = re.search(r"(?:^|\s)(\d{2})-(?:\s|$)", ln)
            if m:
                month = m.group(1)
                continue
        if is_body:
            if year and body is None:
                body = ln                     # waiting for the day fragment
            elif last_date:
                out.append(f"{last_date} {ln}")
            continue
        m = re.search(r"(\d{2})T\d{2}:", ln)
        if m and body and year and month:
            last_date = f"{year}-{month}-{m.group(1)}"
            out.append(f"{last_date} {body}")
            year = month = body = None
    return out


def _opay_v2_transform(rec: dict) -> dict:
    nums = rec.pop("nums").split()
    amount, debit_indicator, balance = nums[0], nums[1], nums[-1]
    is_debit = float(debit_indicator.replace(",", "")) > 0
    rec["debit" if is_debit else "credit"] = amount.lstrip("+-")
    rec["balance"] = balance
    return rec


_OPAY_DROP = (r"OWealth Withdrawal", r"Spend & Save Deposit")

# ---------------------------------------------------------------------------
# Bank specs — order matters: most specific signatures first.
# ---------------------------------------------------------------------------

PREMIUM_TRUST = LineSpec(
    name="Premium Trust",
    detector=contains(r"contactpremium@premiumtrustbank\.com", "last"),
    patterns=[
        rf"^(?P<date>\d{{2}}-[A-Z][a-z]{{2}}-\d{{2}})\s+(?P<narration>.+?)\s+"
        rf"(?P<value_date>\d{{2}}-[A-Z][a-z]{{2}}-\d{{2}})\s+(?P<amount>{AMT})\s+(?P<balance>{AMT})$",
        # narration printed on the previous line
        rf"^(?P<date>\d{{2}}-[A-Z][a-z]{{2}}-\d{{2}})\s+"
        rf"(?P<value_date>\d{{2}}-[A-Z][a-z]{{2}}-\d{{2}})\s+(?P<amount>{AMT})\s+(?P<balance>{AMT})$",
    ],
    narration_before=True,
    date_formats=["%d-%b-%y"],
)

STERLING = LineSpec(
    name="Sterling",
    detector=contains(r"www\.sterling\.ng"),
    patterns=[
        rf"^(?P<date>\d{{2}}/[A-Za-z]{{3}}/\d{{4}})\s+(?P<value_date>\d{{2}}/[A-Za-z]{{3}}/\d{{4}})\s+"
        rf"(?P<narration>.+?)\s+(?P<debit>{AMT}|-)\s+(?P<credit>{AMT}|-)\s+(?P<balance>{AMT})$",
    ],
    date_formats=["%d-%b-%Y"],
)

TAJ = LineSpec(
    name="TAJ",
    detector=contains(r"tajconnect@tajbank\.com"),
    patterns=[
        rf"^(?P<date>\d{{2}}-[A-Z]{{3}}-\d{{2}})\s+(?P<value_date>\d{{2}}-[A-Z]{{3}}-\d{{2}})\s+"
        rf"(?P<branch>\d+)\s+(?P<narration>.+?)\s+(?P<amount>{AMT})\s+(?P<balance>{AMT})$",
    ],
    date_formats=["%d-%b-%y"],
)

MONIEPOINT = LineSpec(
    name="Moniepoint",
    detector=all_of(contains(r"Business Name", flags=0), contains(r"Currency NGN", flags=0)),
    patterns=[
        rf"^(?P<date>\d{{4}}-\d{{2}}-\d{{2}})T\S*\s+(?P<narration>.+?)\s+"
        rf"(?P<debit>{AMT})\s+(?P<credit>{AMT})\s+(?P<balance>{AMT})$",
    ],
    join_lines=2,
    date_formats=["%Y-%m-%d"],
)

PALMPAY = LineSpec(
    name="PalmPay",
    detector=contains(r"PalmPay Business Statement", flags=0),
    patterns=[
        rf"^(?P<date>\d{{4}}/\d{{2}}/\d{{2}}) \d{{2}}:\d{{2}}:\d{{2}}\s+(?P<narration>.+?)\s+"
        rf"(?P<reference>[A-Za-z0-9]+)\s+(?P<amount>{LOOSE_AMT})\s+(?P<balance>{LOOSE_AMT})(?:\s|$)",
    ],
    date_formats=["%Y-%m-%d"],
)

ZENITH = LineSpec(
    name="Zenith",
    detector=contains(r"Account Number: CA", flags=0),
    patterns=[
        # debit / credit / balance
        rf"^(?P<date>\d{{2}}/\d{{2}}/\d{{4}})\s+(?P<value_date>\d{{2}}/\d{{2}}/\d{{4}})\s*(?P<narration>.*?)\s+"
        rf"(?P<debit>{LOOSE_AMT})\s+(?P<credit>{LOOSE_AMT})\s+(?P<balance>{LOOSE_AMT})$",
        # single amount / balance
        rf"^(?P<date>\d{{2}}/\d{{2}}/\d{{4}})\s+(?P<value_date>\d{{2}}/\d{{2}}/\d{{4}})\s*(?P<narration>.*?)\s+"
        rf"(?P<amount>{LOOSE_AMT})\s+(?P<balance>{LOOSE_AMT})$",
    ],
    date_formats=["%d-%m-%Y"],
)

FIDELITY = LineSpec(
    name="Fidelity",
    detector=contains(r"fidelitybank\.ng"),
    patterns=[
        r"^(?P<date>\d{2}-[A-Za-z]{3}-\d{2})\s+(?P<value_date>\d{2}-[A-Za-z]{3}-\d{2})\s+"
        r"(?P<channel>\S+)\s+(?P<narration>.*?)\s+"
        r"(?P<amount>[\d,]+\.\d{1,2})\s+(?P<balance>[\d,]+\.\d{1,2})$",
    ],
    date_formats=["%d-%b-%y"],
)

OPAY_SETTLEMENT = LineSpec(
    name="Opay (settlement layout)",
    detector=contains(r"Reversal Transaction Settlement"),
    patterns=[rf"^(?P<date>\d{{4}}-\d{{2}}-\d{{2}})\s+{_OPAY_V2_BODY.pattern[1:]}"],
    preprocess=_opay_v2_stitch_dates,
    transform=_opay_v2_transform,
    date_formats=["%Y-%m-%d"],
    drop_patterns=_OPAY_DROP,
)

OPAY_2026 = LineSpec(
    name="Opay (2026 layout)",
    detector=contains(r"Pos-service@opay", "last"),
    patterns=[
        r"(?P<date>[A-Za-z]{3}\s+\d{2},\s?\d{4})\s+\d{2}:\d{2}:\d{2}\s+(?P<narration>.+?)\s+"
        r"(?P<amount>-?[\d,]+(?:\.\d+)?)\s+(?P<fee>[\d,]+(?:\.\d+)?)\s+(?P<settlement>-?[\d,]+(?:\.\d+)?)"
        r"(?:\s+₦\s*(?P<balance>[\d,]+(?:\.\d+)?))?",
    ],
    date_formats=["%b-%d-%Y"],
    drop_patterns=_OPAY_DROP,
    order="desc",
)

OPAY_CLASSIC = LineSpec(
    name="Opay",
    detector=contains(r"Note: Current Balance includes OWealth Balance"),
    patterns=[_OPAY_CLASSIC_ROW],
    preprocess=_opay_fix_broken_rows,
    date_formats=["%d-%b-%Y"],
    drop_patterns=_OPAY_DROP,
)

WEMA_STANBIC_FCMB = LineSpec(
    name="Wema / Stanbic IBTC / FCMB",
    detector=contains(r"alat\.ng|www\.stanbicibtcbank\.com|the following pie chart represents the various"),
    patterns=[
        rf"^(?P<date>\d{{2}}[-\s][A-Za-z]{{3}}[-\s]\d{{4}})\s+(?P<reference>\S+)\s+(?P<narration>.+?)\s+"
        rf"(?P<amount>{LOOSE_AMT})\s+(?P<balance>{LOOSE_AMT})$",
        rf"^(?P<date>\d{{2}}-\d{{2}}-\d{{4}})\s+(?P<value_date>\d{{2}}-\d{{2}}-\d{{4}})\s+(?P<narration>.+?)\s+"
        rf"(?P<amount>{LOOSE_AMT})\s+(?P<balance>{LOOSE_AMT})$",
    ],
    preprocess=_wema_join_split_dates,
    date_formats=["%d-%b-%Y", "%d-%m-%Y"],
)

BANK_PARSERS: list[LineSpec] = [
    PREMIUM_TRUST,
    STERLING,
    TAJ,
    MONIEPOINT,
    PALMPAY,
    ZENITH,
    FIDELITY,
    OPAY_SETTLEMENT,
    OPAY_2026,
    OPAY_CLASSIC,
    WEMA_STANBIC_FCMB,
]


def detect_bank(doc) -> LineSpec | None:
    return next((p for p in BANK_PARSERS if p.detect(doc)), None)
