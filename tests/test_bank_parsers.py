"""Each bank spec against synthetic statement text shaped like its regexes."""
import pandas as pd

from statement_analyzer.normalize import normalize
from statement_analyzer.parsers import Document, detect_bank
from statement_analyzer.parsers import banks


def run(spec, text, opening=None):
    raw = spec.parse(Document(pages=[text]))
    return normalize(raw, date_formats=spec.date_formats, order=spec.order,
                     drop_patterns=spec.drop_patterns, opening_balance=opening)


def test_sterling_debit_credit_columns_and_detection():
    text = """www.sterling.ng
Account Statement
01/Jan/2024 01/Jan/2024 Opening Balance - - 10,000.00
02/Jan/2024 02/Jan/2024 NIP transfer from JOHN DOE - 5,000.00 15,000.00
03/Jan/2024 03/Jan/2024 POS purchase SHOPRITE 2,500.00 - 12,500.00
"""
    doc = Document(pages=[text])
    assert detect_bank(doc) is banks.STERLING
    df, q = run(banks.STERLING, text, opening=10_000)
    assert list(df["direction"]) == ["credit", "debit"]
    assert list(df["amount"]) == [5000, 2500]
    assert q.reconciliation_rate == 1.0


def test_zenith_amount_only_direction_from_balance():
    text = """Account Number: CA 1234567890
05/01/2024 05/01/2024 TRF FROM ALICE 20,000.00 120,000.00
06/01/2024 06/01/2024 ATM WITHDRAWAL 5,000.00 115,000.00
07/01/2024 07/01/2024 AIRTIME MTN 1,000.00 114,000.00
"""
    df, q = run(banks.ZENITH, text, opening=100_000)
    assert list(df["direction"]) == ["credit", "debit", "debit"]
    assert q.reconciliation_rate == 1.0


def test_moniepoint_wrapped_row_joined():
    text = """Business Name ACME Currency NGN
2024-02-01T10:22:11 Transfer from Bola 0.00 3,000.00 13,000.00
2024-02-02T09:00:00 Payment to Supplier with a very long
narration 1,000.00 0.00 12,000.00
"""
    doc = Document(pages=[text])
    assert detect_bank(doc) is banks.MONIEPOINT
    df, q = run(banks.MONIEPOINT, text, opening=10_000)
    assert len(df) == 2
    assert "very long narration" in df["narration"].iloc[1]
    assert q.reconciliation_rate == 1.0


def test_premium_narration_on_previous_line():
    text = """Salary for January
05-Jan-24 05-Jan-24 150,000.00 250,000.00
06-Jan-24 Transfer to Mum 06-Jan-24 50,000.00 200,000.00
"""
    df, _ = run(banks.PREMIUM_TRUST, text, opening=100_000)
    assert df["narration"].tolist() == ["Salary for January", "Transfer to Mum"]
    assert df["direction"].tolist() == ["credit", "debit"]


def test_wema_split_date_lines():
    text = """alat.ng
05-Jan-
REF001 Transfer from Tunde 5,000.00 25,000.00
2024
06-Jan-2024 REF002 POS Purchase 2,000.00 23,000.00
"""
    df, q = run(banks.WEMA_STANBIC_FCMB, text, opening=20_000)
    assert len(df) == 2
    assert df["date"].iloc[0] == pd.Timestamp("2024-01-05")
    assert q.reconciliation_rate == 1.0


def test_opay_classic_signed_amounts_and_broken_rows():
    text = """Note: Current Balance includes OWealth Balance
2024 Aug 19 10:00:01 19 Aug 2024 Transfer from Kemi +10,000.00 30,000.00 Mobile 1234567
2024 Aug 20 16: 20 Aug
Airtime purchase -500.00 29,500.00 Mobile 99887766
2024 Aug 21 09:00:00 21 Aug 2024 OWealth Withdrawal +100.00 29,600.00 Mobile 111
"""
    df, _ = run(banks.OPAY_CLASSIC, text)
    assert df["direction"].tolist() == ["credit", "debit"]   # OWealth row dropped
    assert df["date"].iloc[1] == pd.Timestamp("2024-08-20")


def test_palmpay_signed():
    text = """PalmPay Business Statement
2024/03/01 08:00:00 Received from Ade TX123ABC +5000.00 15000.00
2024/03/02 09:00:00 Transfer to Ola TX124ABC -2000.00 13000.00
"""
    df, q = run(banks.PALMPAY, text, opening=10_000)
    assert df["direction"].tolist() == ["credit", "debit"]
    assert q.reconciliation_rate == 1.0


def test_newest_first_is_reversed():
    text = """Account Number: CA 1
07/01/2024 07/01/2024 C 1,000.00 114,000.00
06/01/2024 06/01/2024 B 5,000.00 115,000.00
05/01/2024 05/01/2024 A 20,000.00 120,000.00
"""
    df, q = run(banks.ZENITH, text, opening=100_000)
    assert df["narration"].tolist() == ["A", "B", "C"]
    assert q.reconciliation_rate == 1.0


def test_opay_settlement_layout_vertical_dates():
    text = """Reversal Transaction Settlement
2024- 
Wallet 09- 
Transfer from Ade 5,000.00 0.00 5,000.00 10,000.00 15,000.00
Mobile 01T12: 
Airtime 500.00 500.00 0.00 15,000.00 14,500.00
"""
    df, q = run(banks.OPAY_SETTLEMENT, text)
    assert df["date"].tolist() == [pd.Timestamp("2024-09-01")] * 2
    assert df["direction"].tolist() == ["credit", "debit"]
    assert df["balance"].tolist() == [15000, 14500]


def test_opay_2026_newest_first():
    text = """Sep 02,2025 10:00:00 Transfer to Bisi -2,000.00 0.00 -2,000.00 ₦ 8,000.00
Sep 01,2025 09:00:00 Transfer from Ayo 5,000.00 0.00 5,000.00 ₦ 10,000.00
Pos-service@opay"""
    doc = Document(pages=[text])
    assert detect_bank(doc) is banks.OPAY_2026
    df, q = run(banks.OPAY_2026, text)
    assert df["narration"].tolist() == ["Transfer from Ayo", "Transfer to Bisi"]
    assert df["direction"].tolist() == ["credit", "debit"]
    assert q.reconciliation_rate == 1.0
