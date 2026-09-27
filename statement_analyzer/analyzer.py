"""Financial behaviour and risk metrics on a normalized Statement.
"""
from __future__ import annotations

from functools import cached_property
from typing import Optional

import numpy as np
import pandas as pd

from .config import DEFAULT_SETTINGS, Settings
from .loader import load_statement
from .models import CREDIT, DEBIT, Statement
from .underwriting import AffordabilityAnalyzer, AffordabilityProfile

EXCLUDED_CATEGORIES = {"others", "charges"}


def _ratio(num: float, den: float) -> Optional[float]:
    return float(num) / float(den) if den else None


class StatementAnalyzer:
    def __init__(self, statement: Statement, settings: Settings = DEFAULT_SETTINGS):
        if statement.empty:
            raise ValueError("Statement has no transactions")
        self.statement = statement
        self.settings = settings
        self.df = statement.transactions
        self.has_balance = bool(self.df["balance"].notna().any())

        self.core = self.df[~self.df["category"].isin(EXCLUDED_CATEGORIES)]
        self.inflows = self.core[self.core["direction"] == CREDIT]
        self.outflows = self.core[self.core["direction"] == DEBIT]
        self.transfer_in = self.inflows[self.inflows["category"] == "transfer"]
        self.transfer_out = self.outflows[self.outflows["category"] == "transfer"]

    @classmethod
    def from_source(cls, source, settings: Settings = DEFAULT_SETTINGS, **kwargs) -> "StatementAnalyzer":
        return cls(load_statement(source, settings=settings, **kwargs), settings)

    # ------------------------------------------------------------ affordability
    @cached_property
    def affordability(self) -> AffordabilityProfile:
        """Underwriting view: customer type, salary, DTIR, repayment capacity."""
        return AffordabilityAnalyzer(self.df, self.settings).compute()

    # ------------------------------------------------------------------ basics
    @cached_property
    def months(self) -> list[str]:
        return sorted(self.df["month"].unique())

    @property
    def latest_month(self) -> str:
        return self.months[-1]

    @cached_property
    def monthly_by_category(self) -> pd.DataFrame:
        return self.core.pivot_table(index="month", columns="category", values="amount",
                                     aggfunc="sum", fill_value=0).reindex(self.months, fill_value=0)

    def _category_monthly(self, category: str) -> pd.Series:
        return self.monthly_by_category.get(category, pd.Series(0.0, index=self.months))

    @property
    def last_month_inflow(self) -> float:
        return float(self.inflows.loc[self.inflows["month"] == self.latest_month, "amount"].sum())

    @cached_property
    def opening_balance(self) -> Optional[float]:
        if self.statement.opening_balance is not None:
            return self.statement.opening_balance
        if not self.has_balance:
            return None
        first = self.df.iloc[0]
        if pd.isna(first["balance"]):
            return None
        delta = first["amount"] if first["direction"] == CREDIT else -first["amount"]
        return round(float(first["balance"] - delta), 2)

    @cached_property
    def closing_balance(self) -> Optional[float]:
        bal = self.df["balance"].dropna()
        return float(bal.iloc[-1]) if len(bal) else None

    # ---------------------------------------------------------------- cashflow
    def _cashflow(self, period: str) -> pd.DataFrame:
        g = self.core.groupby([period, "direction"])["amount"].agg(["sum", "count"]).unstack(fill_value=0)
        g.columns = [f"{a}_{b}" for a, b in g.columns]
        for c in ("sum_credit", "sum_debit", "count_credit", "count_debit"):
            if c not in g:
                g[c] = 0
        total_count = (g["count_credit"] + g["count_debit"]).replace(0, np.nan)
        g["net_cashflow"] = g["sum_credit"] - g["sum_debit"]
        g["avg_txn_size"] = ((g["sum_credit"] + g["sum_debit"]) / total_count).fillna(0)
        g["avg_inflow_size"] = (g["sum_credit"] / g["count_credit"].replace(0, np.nan)).fillna(0)
        g["avg_outflow_size"] = (g["sum_debit"] / g["count_debit"].replace(0, np.nan)).fillna(0)
        if self.has_balance:
            closing = self.df.groupby(period)["balance"].last()
            g["closing_balance"] = closing
            g["opening_balance"] = closing.shift(1).reindex(g.index)
            if len(g):
                g.iloc[0, g.columns.get_loc("opening_balance")] = self.opening_balance
        return g.sort_index(ascending=False).reset_index()

    @cached_property
    def cashflow_monthly(self) -> pd.DataFrame:
        return self._cashflow("month")

    @cached_property
    def cashflow_weekly(self) -> pd.DataFrame:
        return self._cashflow("week")

    @cached_property
    def cashflow_by_category(self) -> pd.DataFrame:
        g = self.core.groupby(["month", "category"])["amount"].agg(["sum", "count"]).unstack(fill_value=0)
        g.columns = [f"{a}_{b}" for a, b in g.columns]
        return g.sort_index(ascending=False).reset_index()

    # --------------------------------------------------------------- behaviour
    @cached_property
    def inflow_sources(self) -> pd.DataFrame:
        return (self.transfer_in.groupby("sender", dropna=False)
                .agg(total_inflow=("amount", "sum"), txn_count=("amount", "size"))
                .reset_index().sort_values("total_inflow", ascending=False))

    @cached_property
    def outflow_destinations(self) -> pd.DataFrame:
        return (self.transfer_out.groupby("receiver", dropna=False)
                .agg(total_outflow=("amount", "sum"), txn_count=("amount", "size"))
                .reset_index().sort_values("total_outflow", ascending=False))

    @cached_property
    def account_sweep(self) -> pd.DataFrame:
        """Same receiver, same amount, same day, more than once."""
        s = self.transfer_out.groupby(["receiver", "amount", "date"]).size().reset_index(name="repeat_count")
        return s[s["repeat_count"] > 1]

    @cached_property
    def balance_by_month(self) -> pd.DataFrame:
        if not self.has_balance:
            return pd.DataFrame()
        return (self.df.groupby("month")["balance"].agg(avg_balance="mean", min="min", max="max")
                .reset_index().sort_values("month", ascending=False))

    # -------------------------------------------------------------------- risk
    @property
    def dtir(self) -> float:
        """Existing debt-to-income: latest month repayments over income."""
        return self.affordability.existing_dtir

    @property
    def zeroing_rate(self) -> Optional[float]:
        """Share of days whose closing balance sits at an effectively empty level."""
        return self.affordability.zeroing_rate

    @cached_property
    def risk_indicators(self) -> pd.Series:
        total_in = float(self.inflows["amount"].sum())
        total_out = float(self.outflows["amount"].sum())
        net = self.cashflow_monthly["net_cashflow"]
        volatility = _ratio(net.std(ddof=1), abs(net.mean())) if len(net) > 1 else None
        shares = self.inflow_sources[self.inflow_sources["sender"].notna()]['total_inflow'] / total_in if total_in else pd.Series(dtype=float)
        betting = self._category_monthly("betting")
        loan_rep = self.core[self.core["category"] == "loan_repayment"]
        loans = self.core[self.core["category"] == "loan"]
        balance_floor = None
        if self.has_balance:
            eom = self.df.groupby("month")["balance"].last().dropna()
            balance_floor = round(float(np.percentile(eom, 25)), 2) if len(eom) else None
        aff = self.affordability
        rate = self.statement.quality.reconciliation_rate

        pct = lambda v: None if v is None else f"{v:.1%}"  # noqa: E731
        return pd.Series({
            "Account Name": self.statement.account_name,
            "Account Number": self.statement.account_number,
            "Source": self.statement.source,
            "Extraction Confidence": pct(rate),
            "Tenor": f"{len(self.months)} Months",
            "Start Date": str(self.df["date"].min().date()),
            "End Date": str(self.df["date"].max().date()),
            "Inflow Count": len(self.inflows),
            "Outflow Count": len(self.outflows),
            "Total Transactions": len(self.df),
            "Total Inflow": round(total_in, 2),
            "Total Outflow": round(total_out, 2),
            "Average Inflow": round(float(self.transfer_in["amount"].mean()), 2) if len(self.transfer_in) else 0,
            "Saving Rate": round(1 - total_out / total_in, 2) if total_in else None,
            "Opening Balance": self.opening_balance,
            "Closing Balance": self.closing_balance,
            "Inflow-Outflow Ratio": round(total_in / total_out, 2) if total_out else None,
            "Debit-Credit Frequency Ratio": round(len(self.outflows) / len(self.inflows), 2) if len(self.inflows) else None,
            "Loan Repayment Amount": round(float(loan_rep["amount"].sum()), 2),
            "Loan Repayment Count": len(loan_rep),
            "Loan Disbursement Amount": round(float(loans["amount"].sum()), 2),
            "Loan Disbursement Count": len(loans),
            "VAS Amount": round(float(self.core.loc[self.core["category"] == "VAS", "amount"].sum()), 2),
            "Flight Risk": "Yes" if (self.df["category"] == "travelling").any() else "No",
            "Concentration Risk": pct(float(shares.max())) if len(shares) else None,
            "DTIR": pct(aff.existing_dtir),
            "Zeroing Rate": pct(aff.zeroing_rate),
            "Balance Floor": balance_floor,
            "Betting Ratio": round(_ratio(betting.max(), self.last_month_inflow) or 0, 2),
            "Cashflow Volatility": None if volatility is None else round(volatility, 2),
            # --- underwriting view -------------------------------------------------
            "Customer Type": aff.customer_type,
            "Salary Source": aff.salary_source,
            "Baseline Salary": aff.salary,
            "Salary Recent": aff.salary_recent,
            "Median Inflow (period)": aff.inflow_median,
            "Net-to-Gross": aff.net_to_gross,
            "Persistence": aff.persistence,
            "Disposable Income": aff.disposable_income,
        }, name="value")

    # ----------------------------------------------------------------- helpers
    @property
    def loan_repayments(self) -> pd.DataFrame:
        return self.df[self.df["category"] == "loan_repayment"]

    @property
    def loan_disbursements(self) -> pd.DataFrame:
        return self.df[self.df["category"] == "loan"]

    def summary_text(self) -> str:
        sections = [
            ("ACCOUNT STATEMENT SUMMARY", self.risk_indicators),
            ("AFFORDABILITY", self.affordability.as_series()),
            ("MONTH-ON-MONTH CASHFLOW", self.cashflow_monthly),
            ("CASHFLOW BY CATEGORY", self.cashflow_by_category),
            ("ROUND-TRIP TRANSFERS (ACCOUNT SWEEP)", self.account_sweep),
            ("INFLOW SOURCES BY SENDER", self.inflow_sources),
            ("OUTFLOWS BY RECEIVER", self.outflow_destinations),
            ("AVERAGE MONTHLY BALANCE", self.balance_by_month),
            ("LOAN REPAYMENT TRANSACTIONS", self.loan_repayments),
            ("LOAN DISBURSEMENT TRANSACTIONS", self.loan_disbursements),
        ]
        bar = "=" * 60
        return "\n".join(f"\n{bar}\n{title}\n{bar}\n{data}" for title, data in sections)
