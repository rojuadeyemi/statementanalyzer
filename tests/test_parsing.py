import pandas as pd
import pytest

from statement_analyzer.parsing import explicit_sign, parse_amount, parse_dates


@pytest.mark.parametrize("raw, expected", [
    ("1,234.50", 1234.50),
    ("-500.00", -500.0),
    ("+2,000", 2000.0),
    ("(200.00)", -200.0),
    ("NGN 1,000.00", 1000.0),
    ("₦ 3,500.25", 3500.25),
    ("1,000.00 DR", -1000.0),
    ("0317 1,723,000.00", 1723000.0),   # stray fragment glued in front
    ("--", None),
    ("", None),
    (None, None),
    (12.5, 12.5),
])
def test_parse_amount(raw, expected):
    assert parse_amount(raw) == expected


def test_explicit_sign():
    assert explicit_sign("-5.00") == -1
    assert explicit_sign("+5.00") == 1
    assert explicit_sign("5.00") == 0


@pytest.mark.parametrize("raw, fmt, expected", [
    ("05/01/2024", ["%d-%m-%Y"], "2024-01-05"),
    ("05/Jan/2024", ["%d-%b-%Y"], "2024-01-05"),
    ("05-JAN-24", ["%d-%b-%y"], "2024-01-05"),
    ("2024-01-05T10:22:33", ["%Y-%m-%d"], "2024-01-05"),
    ("Sep 01,2025", ["%b-%d-%Y"], "2025-09-01"),
    ("2024/01/05 12:00:00", ["%Y-%m-%d"], "2024-01-05"),
])
def test_parse_dates_with_format(raw, fmt, expected):
    assert parse_dates(pd.Series([raw]), fmt).iloc[0] == pd.Timestamp(expected)


def test_parse_dates_inferred_dayfirst():
    s = pd.Series(["05/01/2024", "13/01/2024", "20/01/2024"])
    out = parse_dates(s)
    assert list(out.dt.month) == [1, 1, 1]
