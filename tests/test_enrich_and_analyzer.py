import json

import pandas as pd

from statement_analyzer import StatementAnalyzer, build_json, load_statement, write_excel
from statement_analyzer.enrich import categorize, extract_counterparties


def frame(rows):
    return pd.DataFrame(rows, columns=["narration", "amount", "direction"])


def test_categories():
    df = frame([
        ("JAN SALARY ACME LTD", 250_000, "credit"),
        ("SALARY PAYMENT TO STAFF", 80_000, "debit"),
        ("SMS ALERT CHARGES", 50, "debit"),
        ("NIP Transfer charge", 10.75, "debit"),
        ("POS/VISA PURCHASE SHOPRITE", 5_000, "debit"),
        ("VFS Global visa application", 60_000, "debit"),
        ("Remita loan repayment", 20_000, "debit"),
        ("FairMoney credit", 50_000, "credit"),
        ("Bet9ja deposit", 2_000, "debit"),
    ])
    assert categorize(df).tolist() == [
        "salary", "salary_payment", "charges", "charges", "transfer",
        "travelling", "loan_repayment", "loan", "betting",
    ]


def test_counterparties():
    assert extract_counterparties("TRF FRM JOHN DOE TO JANE ROE") == ("John Doe", "Jane Roe")
    assert extract_counterparties("To: MAMA PUT From: BAYO OKE") == ("Bayo Oke", "Mama Put")
    assert extract_counterparties("Transfer from ADA OBI") == ("Ada Obi", None)


def mono_payload():
    # Mono: newest first, amounts in kobo
    data = [
        {"date": "2024-03-05", "type": "debit", "amount": 500000, "narration": "Transfer to Chidi", "balance": 9500000},
        {"date": "2024-02-20", "type": "credit", "amount": 3000000, "narration": "Salary Feb ACME", "balance": 10000000},
        {"date": "2024-02-10", "type": "debit", "amount": 200000, "narration": "Remita loan repayment", "balance": 7000000},
        {"date": "2024-01-15", "type": "credit", "amount": 5000000, "narration": "Transfer from Musa", "balance": 7200000},
    ]
    return {"data": data}


def test_mono_end_to_end_and_reports(tmp_path):
    s = load_statement(json.dumps(mono_payload()))
    assert s.source == "mono"
    assert s.transactions["date"].is_monotonic_increasing
    assert s.quality.reconciliation_rate == 1.0

    a = StatementAnalyzer(s)
    r = a.risk_indicators
    assert r["Total Inflow"] == 80_000
    assert r["Loan Repayment Count"] == 1
    assert r["Flight Risk"] == "No"
    assert a.latest_month == "2024-03"
    assert a.last_month_inflow == 0      # March has only a debit

    report = build_json(a)
    assert report["summary"]["total_transactions"] == 4
    write_excel(a, tmp_path / "r.xlsx")
    assert (tmp_path / "r.xlsx").stat().st_size > 0


def test_analyzer_without_balance():
    payload = {"transactions": [
        {"transaction_date": "2024-01-01", "description": "Transfer from A", "amount": "+1000", "balance": None},
        {"transaction_date": "2024-01-02", "description": "Transfer to B", "amount": "-400", "balance": None},
    ]}
    a = StatementAnalyzer(load_statement(payload))
    r = a.risk_indicators
    assert r["Zeroing Rate"] is None and r["Balance Floor"] is None
    assert r["Total Outflow"] == 400
