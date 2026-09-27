"""Smoke tests for the Streamlit app (no browser needed)."""
import pytest

pytest.importorskip("streamlit")
from pathlib import Path  # noqa: E402

from streamlit.testing.v1 import AppTest  # noqa: E402

from statement_analyzer import StatementAnalyzer  # noqa: E402
from statement_analyzer.models import QualityReport, Statement  # noqa: E402
from tests.test_underwriting import build, salaried_rows  # noqa: E402


APP = Path(__file__).resolve().parent.parent / "app.py"


def test_app_runs_and_renders_sidebar():
    at = AppTest.from_file(str(APP), default_timeout=60).run()
    assert not at.exception
    assert at.title[0].value == "Statement Analyzer"
    # The sidebar builds Settings on every run; toggling the LLM on must not crash.
    at.toggle[0].set_value(True).run()
    assert not at.exception
    assert at.warning or at.success        # it reports whether the key is usable


def test_charts_build_valid_specs():
    import app

    a = StatementAnalyzer(Statement(transactions=build(salaried_rows()), quality=QualityReport()))
    for chart in (app.cashflow_chart(a.cashflow_monthly), app.balance_chart(a.df), app.category_chart(a.df)):
        spec = chart.to_dict()
        assert spec["encoding"]
    assert app.money(1234.5) == "₦1,234.50"
    assert app.money(None) == "—"
