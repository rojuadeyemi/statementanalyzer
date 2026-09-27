"""Generate real PDFs and run the full pipeline on them."""
import pytest

reportlab = pytest.importorskip("reportlab")
from reportlab.lib.pagesizes import A4  # noqa: E402
from reportlab.pdfgen import canvas  # noqa: E402

from statement_analyzer import StatementAnalyzer, load_statement  # noqa: E402


def _unknown_bank_pdf(path):
    """Column layout, no known signature, wrapped narration, 2 pages, header repeated."""
    c = canvas.Canvas(str(path), pagesize=A4)
    cols = {"Txn Date": 40, "Description": 110, "Money Out": 330, "Money In": 410, "Balance": 490}
    rows = [
        ("02 Jan 2024", ["Transfer from TUNDE BAKARE", "salary support"], "", "50,000.00", "150,000.00"),
        ("03 Jan 2024", ["POS purchase SHOPRITE"], "12,500.00", "", "137,500.00"),
        ("04 Jan 2024", ["Airtime MTN"], "1,000.00", "", "136,500.00"),
        ("05 Jan 2024", ["Transfer to MAMA PUT"], "6,500.00", "", "130,000.00"),
    ]

    def header(y):
        for label, x in cols.items():
            c.drawString(x, y, label)

    c.drawString(40, 800, "Hello FOLAKE ADEYEMI")
    c.drawString(40, 785, "Account Number: 0123456789")
    c.drawString(40, 770, "Opening Balance: 100,000.00")
    y = 740
    header(y)
    for i, (d, narr, out, inn, bal) in enumerate(rows):
        if i == 2:                       # page break with repeated header
            c.drawString(250, 60, "Page 1 of 2")
            c.showPage()
            y = 800
            header(y)
        y -= 20
        c.drawString(cols["Txn Date"], y, d)
        c.drawString(cols["Description"], y, narr[0])
        for x_key, val in (("Money Out", out), ("Money In", inn), ("Balance", bal)):
            if val:
                c.drawString(cols[x_key], y, val)
        for extra in narr[1:]:
            y -= 12
            c.drawString(cols["Description"], y, extra)
    c.save()


def _sterling_pdf(path):
    c = canvas.Canvas(str(path), pagesize=A4)
    lines = [
        "www.sterling.ng",
        "Account Name: JOHN DOE Currency NGN",
        "01/Jan/2024 01/Jan/2024 Opening Balance - - 10,000.00",
        "02/Jan/2024 02/Jan/2024 NIP transfer from JOHN DOE - 5,000.00 15,000.00",
        "03/Jan/2024 03/Jan/2024 POS purchase SHOPRITE 2,500.00 - 12,500.00",
    ]
    y = 800
    for ln in lines:
        c.drawString(40, y, ln)
        y -= 16
    c.save()


def test_unknown_layout_uses_column_parser(tmp_path):
    pdf = tmp_path / "unknown.pdf"
    _unknown_bank_pdf(pdf)
    s = load_statement(pdf)
    df = s.transactions
    assert s.source == "pdf:Generic (column layout)"
    assert len(df) == 4
    assert s.quality.reconciliation_rate == 1.0
    assert df["narration"].iloc[0] == "Transfer from TUNDE BAKARE salary support"
    assert df["direction"].tolist() == ["credit", "debit", "debit", "debit"]
    assert s.account_number == "0123456789"
    assert s.opening_balance == 100_000
    a = StatementAnalyzer(s)
    assert a.risk_indicators["Closing Balance"] == 130_000


def test_known_bank_pdf(tmp_path):
    pdf = tmp_path / "sterling.pdf"
    _sterling_pdf(pdf)
    s = load_statement(pdf)
    assert s.source == "pdf:Sterling"
    assert s.transactions["amount"].tolist() == [5000, 2500]
    assert s.account_name == "JOHN DOE"
