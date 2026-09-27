import numpy as np
import pandas as pd
import pytest

from statement_analyzer import Settings, StatementAnalyzer, load_statement
from statement_analyzer.enrich import enrich
from statement_analyzer.parsers.llm import LLMParser, LLMUnavailable
from statement_analyzer.underwriting import NON_SALARIED, SALARIED, assess_affordability


def build(rows) -> pd.DataFrame:
    """rows: (date, narration, amount, direction, balance)"""
    df = pd.DataFrame(rows, columns=["date", "narration", "amount", "direction", "balance"])
    df["date"] = pd.to_datetime(df["date"])
    df["value_date"] = df["date"]
    df["reference"] = ""
    df["channel"] = ""
    df["reconciled"] = True
    return enrich(df, None)


def salaried_rows():
    rows, balance = [], 200_000.0
    for month in range(1, 7):
        for day, narration, amount, direction in [
            (25, "ACME LTD JANUARY SALARY", 400_000, "credit"),
            (26, "Remita loan repayment", 60_000, "debit"),
            (27, "Transfer to LANDLORD", 150_000, "debit"),
        ]:
            balance += amount if direction == "credit" else -amount
            rows.append((f"2024-{month:02d}-{day}", narration, amount, direction, balance))
    return rows


def test_salaried_profile_from_narration():
    p = assess_affordability(build(salaried_rows()))
    assert p.customer_type == SALARIED
    assert p.salary_source == "narration"
    assert p.salary == 400_000
    assert p.existing_dtir == pytest.approx(0.15, abs=1e-3)      # 60k / 400k
    # (0.33 - 0.15) * 400,000
    assert p.disposable_income == pytest.approx(72_000, abs=1)
    assert p.salary_recent is True     # last salary is 2 days before the statement ends
    assert p.cv_net is None and p.slope is None


def test_salary_found_by_pattern_when_narration_is_useless():
    rows, balance = [], 100_000.0
    for month in range(1, 6):
        for day, narration, amount, direction in [
            (28, "NIP TRSF FRM ACME VENTURES LTD", 505_000 if month % 2 else 500_000, "credit"),
            (2, "POS purchase SHOPRITE", 20_000, "debit"),
        ]:
            balance += amount if direction == "credit" else -amount
            rows.append((f"2024-{month:02d}-{day}", narration, amount, direction, balance))
    p = assess_affordability(build(rows))
    assert p.customer_type == SALARIED
    assert p.salary_source == "pattern"
    assert p.salary == 500_000            # conservative baseline: min non-zero
    assert p.last_salary_date is not None


def test_non_salaried_profile_uses_weekly_transfers():
    rows, balance = [], 50_000.0
    day = pd.Timestamp("2024-01-01")
    rng = np.random.default_rng(7)
    for week in range(12):
        for offset, amount, direction in [(0, 120_000, "credit"), (2, 90_000, "debit")]:
            amount = float(amount + rng.integers(-5_000, 5_000))
            balance += amount if direction == "credit" else -amount
            rows.append(((day + pd.Timedelta(days=7 * week + offset)).date(),
                         "NIP transfer from CUSTOMER" if direction == "credit" else "Transfer to SUPPLIER",
                         amount, direction, balance))
    p = assess_affordability(build(rows))
    assert p.customer_type == NON_SALARIED
    assert p.salary is None
    assert 0 <= p.cv_net <= 2
    assert p.slope is not None                      # 12 weeks >= min_weeks_for_slope
    assert p.persistence == 1.0                     # every week ends positive
    assert p.disposable_income > 0
    assert p.balance_floor is not None


def test_settings_change_repayment_capacity():
    df = build(salaried_rows())
    strict = assess_affordability(df, Settings(repayment_fraction_salaried=0.25))
    assert strict.disposable_income == pytest.approx((0.25 - 0.15) * 400_000, abs=1)

    median_mode = assess_affordability(df, Settings(salary_baseline_mode="median"))
    assert median_mode.salary == 400_000


def test_no_balance_column_is_survivable():
    rows = [(f"2024-0{m}-25", "ACME SALARY", 300_000, "credit", np.nan) for m in range(1, 5)]
    p = assess_affordability(build(rows))
    assert p.zeroing_rate is None and p.balance_floor is None


def test_analyzer_exposes_affordability_and_reports():
    from statement_analyzer import build_json
    from statement_analyzer.models import QualityReport, Statement

    statement = Statement(transactions=build(salaried_rows()), source="test", quality=QualityReport())
    a = StatementAnalyzer(statement)
    assert a.risk_indicators["Customer Type"] == SALARIED
    assert a.risk_indicators["Disposable Income"] > 0
    assert build_json(a)["affordability"]["customer_type"] == SALARIED


# --------------------------------------------------------------------- LLM
def test_llm_without_key_reports_a_clear_reason():
    pytest.importorskip("anthropic")
    parser = LLMParser(Settings(enable_llm=True, llm_api_key=None))
    reason = parser.unavailable_reason()
    assert reason and "ANTHROPIC_API_KEY" in reason
    with pytest.raises(LLMUnavailable):
        parser.client()


def test_pipeline_does_not_crash_when_llm_enabled_without_key(tmp_path):
    pytest.importorskip("reportlab")
    from tests.test_pdf_end_to_end import _sterling_pdf

    pdf = tmp_path / "s.pdf"
    _sterling_pdf(pdf)
    s = load_statement(pdf, settings=Settings(enable_llm=True, llm_api_key=None, min_reconciliation=1.1))
    assert not s.transactions.empty
    assert any("LLM fallback skipped" in w for w in s.quality.warnings)
