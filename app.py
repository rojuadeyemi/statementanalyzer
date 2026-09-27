"""Streamlit interface for statement_analyzer.

    streamlit run app.py

Upload one or more statements (PDF or JSON), review extraction quality, the
risk summary and the affordability view, then download the Excel/JSON report.
"""
from __future__ import annotations

import hmac
import inspect
import json
import os
import tempfile
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

from statement_analyzer import Settings, StatementAnalyzer, build_json, load_statement, write_excel
from statement_analyzer.parsers.llm import LLMParser

# Validated categorical slots (blue / orange): CVD ΔE 24.7, normal-vision ΔE 33.6.
INFLOW, OUTFLOW, NEUTRAL = "#2a78d6", "#eb6834", "#8a8a85"
MONEY = "₦{:,.2f}"

st.set_page_config(page_title="Statement Analyzer", page_icon="📄", layout="wide")


# ---------------------------------------------------------------- data layer
@st.cache_data(show_spinner=False, max_entries=8)
def _load(file_bytes: bytes, filename: str, settings: Settings):
    """Cached so switching tabs or tweaking the view doesn't re-parse the PDF."""
    suffix = Path(filename).suffix or ".pdf"
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(file_bytes)
        path = Path(tmp.name)
    try:
        return load_statement(path, settings=settings)
    finally:
        path.unlink(missing_ok=True)


def password_accepted() -> bool:
    """Gate the app when APP_PASSWORD is set (i.e. anywhere it is deployed).

    Statements are personal financial data; a public URL should not serve them
    to whoever finds it. With no APP_PASSWORD set — running locally — the app
    is open as before.
    """
    expected = os.environ.get("APP_PASSWORD")
    if not expected or st.session_state.get("authenticated"):
        return True

    with st.form("sign-in"):
        entered = st.text_input("Password", type="password")
        if st.form_submit_button("Enter"):
            if hmac.compare_digest(entered, expected):
                st.session_state["authenticated"] = True
                st.rerun()
            st.error("Incorrect password.")
    return False


def fill(element) -> dict:
    """Kwargs that make an element fill its container, whatever Streamlit version is installed.

    Streamlit renamed ``use_container_width=True`` to ``width="stretch"``, and
    did it for different elements in different releases — the deployed version
    may be older than the one you develop against. Ask the function itself.
    """
    try:
        params = inspect.signature(element).parameters
    except (TypeError, ValueError):      # wrapped without a usable signature
        return {"use_container_width": True}
    # Prefer the legacy kwarg while it exists: older releases have a `width`
    # that takes pixels, not "stretch", so its presence alone proves nothing.
    if "use_container_width" in params:
        return {"use_container_width": True}
    if "width" in params:
        return {"width": "stretch"}
    return {}


def money(value) -> str:
    return "—" if value is None or pd.isna(value) else MONEY.format(float(value))


def as_table(series: pd.Series) -> pd.DataFrame:
    """Render a mixed-type summary Series as text (Arrow rejects mixed object columns)."""
    return series.map(lambda v: "—" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)).to_frame("value")


# ------------------------------------------------------------------ sidebar
def sidebar_settings() -> Settings:
    st.sidebar.header("Settings")

    with st.sidebar.expander("Extraction", expanded=False):
        cutoff = st.number_input("History window (months)", 1, 60, 36)
        min_rec = st.slider("Minimum reconciliation before retrying parsers", 0.0, 1.0, 0.90, 0.05,
                            help="If fewer rows than this reconcile against the running balance, "
                                 "the generic parsers are tried as well and the best result wins.")

    with st.sidebar.expander("Underwriting", expanded=False):
        baseline = st.radio("Salary baseline", ["min_non_zero", "median"], horizontal=True,
                            help="min_non_zero is the conservative choice.")
        frac_sal = st.slider("Repayment capacity — salaried", 0.0, 1.0, 0.33, 0.01)
        frac_non = st.slider("Repayment capacity — non-salaried", 0.0, 1.0, 0.30, 0.01)
        margin = st.slider("Income margin on inflow (non-salaried)", 0.0, 1.0, 0.30, 0.05)
        recency = st.number_input("Salary counts as recent within (days)", 7, 120, 45)

    with st.sidebar.expander("AI fallback", expanded=False):
        st.caption("Used only when every rule-based parser fails validation. "
                   "It sends statement text to the Claude API.")
        enable_llm = st.toggle("Enable AI fallback", value=False)
        api_key = st.text_input("Anthropic API key", type="password",
                                placeholder="sk-ant-…  (or set ANTHROPIC_API_KEY)")
        model = st.text_input("Model", value=Settings().llm_model)

    settings = Settings(
        cutoff_months=int(cutoff), min_reconciliation=float(min_rec),
        salary_baseline_mode=baseline, repayment_fraction_salaried=float(frac_sal),
        repayment_fraction_non_salaried=float(frac_non), inflow_margin=float(margin),
        salary_recency_days=int(recency), enable_llm=bool(enable_llm), llm_model=model,
        **({"llm_api_key": api_key} if api_key else {}),
    )

    if enable_llm:
        reason = LLMParser(settings).unavailable_reason()
        if reason:
            st.sidebar.warning(f"AI fallback can't run: {reason}. Rule-based parsing still works.")
        else:
            st.sidebar.success("AI fallback ready.")
    return settings


# ------------------------------------------------------------------- charts
def cashflow_chart(cashflow: pd.DataFrame) -> alt.Chart:
    data = cashflow.rename(columns={"sum_credit": "Inflow", "sum_debit": "Outflow"})
    long = data.melt(id_vars="month", value_vars=["Inflow", "Outflow"],
                     var_name="Direction", value_name="Amount")
    return (
        alt.Chart(long)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("month:O", title=None, axis=alt.Axis(labelAngle=0)),
            xOffset=alt.XOffset("Direction:N"),
            y=alt.Y("Amount:Q", title="₦", axis=alt.Axis(format="~s")),
            color=alt.Color("Direction:N",
                            scale=alt.Scale(domain=["Inflow", "Outflow"], range=[INFLOW, OUTFLOW]),
                            legend=alt.Legend(title=None, orient="top")),
            tooltip=[alt.Tooltip("month:O", title="Month"), "Direction:N",
                     alt.Tooltip("Amount:Q", format=",.2f")],
        )
        .properties(height=260)
    )


def balance_chart(df: pd.DataFrame) -> alt.Chart:
    daily = df.dropna(subset=["balance"]).groupby("date", as_index=False)["balance"].last()
    return (
        alt.Chart(daily)
        .mark_line(strokeWidth=2, color=INFLOW, point=False)
        .encode(
            x=alt.X("date:T", title=None),
            y=alt.Y("balance:Q", title="₦", axis=alt.Axis(format="~s")),
            tooltip=[alt.Tooltip("date:T", title="Date"), alt.Tooltip("balance:Q", format=",.2f")],
        )
        .properties(height=260)
        .interactive()
    )


def category_chart(df: pd.DataFrame) -> alt.Chart:
    data = (df.groupby(["category", "direction"], as_index=False)["amount"].sum()
              .sort_values("amount", ascending=False))
    return (
        alt.Chart(data)
        .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
        .encode(
            y=alt.Y("category:N", sort="-x", title=None),
            x=alt.X("amount:Q", title="₦", axis=alt.Axis(format="~s")),
            color=alt.Color("direction:N",
                            scale=alt.Scale(domain=["credit", "debit"], range=[INFLOW, OUTFLOW]),
                            legend=alt.Legend(title=None, orient="top")),
            tooltip=["category:N", "direction:N", alt.Tooltip("amount:Q", format=",.2f")],
        )
        .properties(height=max(200, 26 * data["category"].nunique()))
    )


# -------------------------------------------------------------------- panes
def overview(a: StatementAnalyzer) -> None:
    r = a.risk_indicators
    c = st.columns(4)
    c[0].metric("Total inflow", money(r["Total Inflow"]))
    c[1].metric("Total outflow", money(r["Total Outflow"]))
    c[2].metric("Closing balance", money(r["Closing Balance"]))
    c[3].metric("Tenor", r["Tenor"])

    c = st.columns(4)
    c[0].metric("Inflow / outflow", r["Inflow-Outflow Ratio"] or "—")
    c[1].metric("Concentration risk", r["Concentration Risk"] or "—")
    c[2].metric("Zeroing rate", r["Zeroing Rate"] or "—")
    c[3].metric("Flight risk", r["Flight Risk"])

    st.subheader("Month-on-month cashflow")
    st.altair_chart(cashflow_chart(a.cashflow_monthly), **fill(st.altair_chart))

    left, right = st.columns(2)
    with left:
        st.subheader("Where the money goes")
        st.altair_chart(category_chart(a.df), **fill(st.altair_chart))
    with right:
        st.subheader("Balance")
        if a.has_balance:
            st.altair_chart(balance_chart(a.df), **fill(st.altair_chart))
        else:
            st.info("This statement has no running balance column.")

    with st.expander("Full summary table"):
        st.dataframe(as_table(a.risk_indicators), **fill(st.dataframe))


def affordability(a: StatementAnalyzer) -> None:
    p = a.affordability
    st.subheader(f"{p.customer_type.replace('_', '-').title()} profile")

    c = st.columns(4)
    c[0].metric("Monthly repayment capacity", money(p.disposable_income))
    c[1].metric("Existing DTIR", f"{p.existing_dtir:.1%}")
    c[2].metric("Baseline salary" if p.salary else "Median period inflow",
                money(p.salary if p.salary else p.inflow_median))
    c[3].metric("Balance floor", money(p.balance_floor))

    c = st.columns(4)
    c[0].metric("Net-to-gross", f"{p.net_to_gross:.0%}")
    c[1].metric("Persistence", f"{p.persistence:.0%}",
                help="Salaried: share of months with a salary. Non-salaried: share of weeks ending positive.")
    c[2].metric("Volatility (CV)", "—" if p.cv_net is None else f"{p.cv_net:.2f}")
    c[3].metric("Weeks of history", p.series_length)

    notes = []
    if p.salary_source == "pattern":
        notes.append("Salary was detected from the **pattern** of recurring credits, not from the narration.")
    if p.salary_recent is False:
        notes.append("The last salary is **older than the recency window** — treat the salary as stale.")
    if p.slope is not None and p.slope < 0:
        notes.append("Weekly net inflow is **trending down**.")
    if p.betting_amount > 0:
        notes.append(f"Betting spend peaked at **{money(p.betting_amount)}** in a month.")
    if p.zeroing_rate and p.zeroing_rate > 0.5:
        notes.append(f"The account is left empty on **{p.zeroing_rate:.0%}** of days.")
    for note in notes:
        st.markdown(f"- {note}")

    with st.expander("All affordability fields"):
        st.dataframe(as_table(p.as_series()), **fill(st.dataframe))


def transactions(a: StatementAnalyzer) -> None:
    df = a.df
    left, right, third = st.columns(3)
    categories = left.multiselect("Category", sorted(df["category"].unique()))
    direction = right.selectbox("Direction", ["all", "credit", "debit"])
    search = third.text_input("Search narration")

    view = df
    if categories:
        view = view[view["category"].isin(categories)]
    if direction != "all":
        view = view[view["direction"] == direction]
    if search:
        view = view[view["narration"].str.contains(search, case=False, na=False)]

    st.caption(f"{len(view):,} of {len(df):,} transactions")
    st.dataframe(view, height=520, **fill(st.dataframe),
                 column_config={"amount": st.column_config.NumberColumn(format="%.2f"),
                                "balance": st.column_config.NumberColumn(format="%.2f")})

    left, right = st.columns(2)
    with left:
        st.subheader("Top senders")
        st.dataframe(a.inflow_sources.head(15), hide_index=True, **fill(st.dataframe))
    with right:
        st.subheader("Top receivers")
        st.dataframe(a.outflow_destinations.head(15), hide_index=True, **fill(st.dataframe))


def downloads(a: StatementAnalyzer, stem: str) -> None:
    left, right = st.columns(2)
    left.download_button("⬇️  Excel report", write_excel(a).getvalue(), f"{stem}.xlsx",
                         "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                         **fill(st.download_button))
    right.download_button("⬇️  JSON report", json.dumps(build_json(a), indent=2), f"{stem}.json",
                          "application/json", **fill(st.download_button))


# --------------------------------------------------------------------- main
def main() -> None:
    st.title("Statement Analyzer")
    st.caption("Upload a bank statement to extract transactions, check them against the running "
               "balance, and size up affordability.")

    if not password_accepted():
        return

    settings = sidebar_settings()
    files = st.file_uploader("Bank statements", type=["pdf", "json", "txt"],
                             accept_multiple_files=True)
    if not files:
        st.info("Upload a PDF or JSON statement to begin.")
        return

    names = [f.name for f in files]
    chosen = st.selectbox("Statement", names) if len(names) > 1 else names[0]
    file = next(f for f in files if f.name == chosen)

    with st.spinner(f"Reading {file.name}… (large statements can take a few seconds)"):
        try:
            statement = _load(file.getvalue(), file.name, settings)
        except Exception as exc:
            st.error(f"Could not read {file.name}: {exc}")
            return

    if statement.empty:
        st.error("No transactions could be extracted from this file.")
        return

    header = st.columns(4)
    header[0].metric("Account name", statement.account_name or "—")
    header[1].metric("Account number", statement.account_number or "—")
    header[2].metric("Transactions", f"{len(statement.transactions):,}")


    q = statement.quality
    rate = q.reconciliation_rate
    label = "n/a (no running balance)" if rate is None else f"{rate:.0%}"
    header[3].metric("Rows reconciled against the balance", f"{q.reconciled}/{q.checked}" if q.checked else "—",
                    delta=label, delta_color="off")

    analyzer = StatementAnalyzer(statement, settings)
    tabs = st.tabs(["Overview", "Affordability", "Transactions", "Download"])
    with tabs[0]:
        overview(analyzer)
    with tabs[1]:
        affordability(analyzer)
    with tabs[2]:
        transactions(analyzer)
    with tabs[3]:
        downloads(analyzer, Path(file.name).stem)


if __name__ == "__main__":
    main()
