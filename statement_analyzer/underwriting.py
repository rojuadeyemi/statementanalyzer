"""Affordability / underwriting features (merged from the old ``Aggregator``).

Two customer profiles:

* **salaried** — a recurring salary is found, either from the narration
  (category == "salary") or, failing that, from the *pattern* of large
  month-end credits. Capacity is based on the baseline salary.
* **non-salaried** — no salary pattern. Capacity is based on weekly transfer
  inflow, discounted for volatility.

Fixes carried over from the original script:
* ``compute()`` read ``potential_salary.get('salary')`` — a key that never
  existed — so a detected salary pattern was thrown away and the baseline
  became 0.
* The zeroing rate computed a median inflow and then ignored it, comparing
  every end-of-day balance to a flat constant. It now uses
  ``max(zeroing_base, fraction x median daily inflow)``.
* Division by zero when there is no inflow at all, and crashes when the
  statement has no balance column.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Optional

import numpy as np
import pandas as pd

from .config import DEFAULT_SETTINGS, Settings
from .models import CREDIT, DEBIT

SALARIED = "salaried"
NON_SALARIED = "non_salaried"


@dataclass
class AffordabilityProfile:
    """Everything an underwriting scorecard needs from a statement."""

    customer_type: str
    series_length: int                     # number of distinct weeks observed
    inflow_median: float                   # median gross inflow per period
    base_net: float                        # typical net cashflow per period
    net_to_gross: float
    persistence: float                     # share of periods that end positive
    cv_net: Optional[float]                # volatility penalty (non-salaried)
    slope: Optional[float]                 # net inflow trend (non-salaried)
    salary: Optional[float]                # baseline salary (salaried)
    salary_source: Optional[str]           # "narration" | "pattern" | None
    salary_recent: Optional[bool]
    last_salary_date: Optional[pd.Timestamp]
    existing_dtir: float                   # current repayments / income
    disposable_income: float               # monthly headroom for a new loan
    betting_amount: float                  # worst month's betting spend
    zeroing_rate: Optional[float]
    balance_floor: Optional[float]

    def as_dict(self) -> dict:
        d = asdict(self)
        if isinstance(d.get("last_salary_date"), pd.Timestamp):
            d["last_salary_date"] = d["last_salary_date"].date().isoformat()
        return d

    def as_series(self) -> pd.Series:
        return pd.Series(self.as_dict(), name="value")


def _clamp(x, lo, hi):
    return np.clip(x, lo, hi)


def _median(values) -> float:
    values = np.asarray(values, dtype=float)
    return float(np.nanmedian(values)) if values.size else 0.0


def _percentile(values, q: float) -> Optional[float]:
    values = np.asarray(pd.Series(values).dropna(), dtype=float)
    return round(float(np.percentile(values, q)), 2) if values.size else None


class AffordabilityAnalyzer:
    """Computes an :class:`AffordabilityProfile` from normalized transactions."""

    REQUIRED = {"date", "amount", "direction", "category", "month", "week"}

    def __init__(self, df: pd.DataFrame, settings: Settings = DEFAULT_SETTINGS):
        missing = self.REQUIRED - set(df.columns)
        if missing:
            raise ValueError(f"Missing expected columns: {sorted(missing)}")
        self.df = df
        self.s = settings
        self.has_balance = "balance" in df.columns and bool(df["balance"].notna().any())

        self.is_credit = df["direction"].eq(CREDIT)
        self.is_transfer = df["category"].eq("transfer")
        self.core = df[~df["category"].isin({"others", "charges"})]

        self.monthly = (
            df[df["category"].isin(["salary", "loan_repayment", "betting"])]
            .pivot_table(index="month", columns="category", values="amount", aggfunc="sum", fill_value=0)
            .sort_index()
        )

    # ---------------------------------------------------------------- pieces
    def _series(self, category: str) -> pd.Series:
        return self.monthly[category] if category in self.monthly else pd.Series(dtype=float)

    @property
    def betting_amount(self) -> float:
        s = self._series("betting")
        return float(s.max()) if len(s) else 0.0

    @property
    def last_repayment(self) -> float:
        s = self._series("loan_repayment")
        return float(s.iloc[-1]) if len(s) else 0.0

    def _period_net_gross(self, frame: pd.DataFrame, index_col: str) -> tuple[np.ndarray, np.ndarray]:
        if frame.empty:
            return np.zeros(0), np.zeros(0)
        pivot = frame.pivot_table(index=index_col, columns="direction", values="amount",
                                  aggfunc="sum", fill_value=0).sort_index()
        credit = pivot[CREDIT] if CREDIT in pivot else pd.Series(0.0, index=pivot.index)
        debit = pivot[DEBIT] if DEBIT in pivot else pd.Series(0.0, index=pivot.index)
        return (credit - debit).to_numpy(float), credit.to_numpy(float)

    def _period_balance_floor(self, index_col: str) -> Optional[float]:
        if not self.has_balance:
            return None
        return _percentile(self.df.groupby(index_col)["balance"].last(), 25)

    @property
    def zeroing_rate(self) -> Optional[float]:
        """Share of days whose closing balance sits at an effectively empty level."""
        if not self.has_balance:
            return None
        eod = self.df.groupby("date")["balance"].last().dropna()
        if eod.empty:
            return None
        daily_inflow = self.df[self.is_credit & self.is_transfer].groupby("date")["amount"].sum()
        common = eod.index.intersection(daily_inflow.index)
        median_inflow = _median(daily_inflow.loc[common]) if len(common) else 0.0
        threshold = max(self.s.zeroing_base, self.s.zeroing_inflow_fraction * median_inflow)
        return round(float((eod < threshold).mean()), 3)

    # ------------------------------------------------------ salary detection
    def detect_salary(self) -> tuple[pd.Series, Optional[pd.Timestamp], Optional[str]]:
        """Monthly salary amounts, the date of the last one, and how it was found."""
        labelled = self.df[self.df["category"].eq("salary")]
        if not labelled.empty:
            monthly = labelled.groupby("month")["amount"].sum().sort_index()
            return monthly, labelled["date"].max(), "narration"

        amounts, last_date = self._salary_by_pattern()
        if amounts:
            return pd.Series(amounts, dtype=float), last_date, "pattern"
        return pd.Series(dtype=float), None, None

    def _salary_by_pattern(self) -> tuple[list[float], Optional[pd.Timestamp]]:
        """Find a recurring, stable salary credit.

        A candidate must:
          * fall inside the configured amount range and the salary window,
          * arrive once per salary cycle, for enough cycles,
          * be stable in amount (CV below the threshold), and
          * land within a few days of its usual payday.

        Amounts are bucketed on a log scale, so "the same salary give or take a
        few percent" clusters together without any pairwise comparison. Paydays
        are measured against the END of the salary cycle's month rather than the
        day number, so a payday that straddles month end (31 Jan, 1 Mar, 30 Mar,
        1 May) reads as regular instead of swinging by 15 days.
        """
        s = self.s
        credits = self.df.loc[
            self.is_credit
            & self.df["amount"].between(s.salary_pattern_min_amount, s.salary_pattern_max_amount),
            ["date", "amount"],
        ].copy()
        if credits.empty:
            return [], None

        late, early = s.salary_window_days
        day = credits["date"].dt.day
        credits = credits[(day >= late) | (day <= early)].copy()
        if credits.empty:
            return [], None

        # Credits on the 1st-7th belong to the previous month's salary cycle.
        cycle_date = credits["date"].where(credits["date"].dt.day > early,
                                           credits["date"] - pd.DateOffset(months=1))
        credits["cycle"] = cycle_date.dt.to_period("M")
        credits["cluster"] = (np.log(credits["amount"]) / np.log1p(s.salary_diff_tolerance)).round().astype(int)

        # One payment per cluster per cycle (a salary arrives once a month).
        # Cycles with two payments of the same size are dropped from here on, so
        # a one-off duplicate can't distort the stability or regularity checks.
        per_cycle = credits.groupby(["cluster", "cycle"]).size().reset_index(name="n")
        clean = credits.merge(per_cycle.loc[per_cycle["n"] == 1, ["cluster", "cycle"]],
                              on=["cluster", "cycle"], how="inner")

        cycles = clean.groupby("cluster")["cycle"].nunique()
        candidates = cycles[(cycles >= s.salary_min_cycles) & (cycles <= s.salary_max_cycles)]
        if candidates.empty:
            return [], None

        stats = clean.groupby("cluster")["amount"].agg(["mean", "std"]).loc[candidates.index]
        stats["cv"] = (stats["std"] / stats["mean"]).fillna(0)
        stable = stats[stats["cv"] < s.salary_max_cv]
        if stable.empty:
            return [], None

        # Regular payday AND regular cycle: paid at the same point in the month,
        # and paid every month (a single skipped month is tolerated).
        kept = {}
        for cluster in stable.index:
            rows = clean[clean["cluster"] == cluster]
            if self._payday_is_regular(rows) and self._cycle_is_regular(rows):
                kept[cluster] = self._cycle_continuity(rows)
        if not kept:
            return [], None

        stable = stable.loc[list(kept)].assign(continuity=pd.Series(kept))
        # Prefer an unbroken monthly salary; fall back to the larger one.
        best = stable.sort_values(["continuity", "mean"], ascending=False).index[0]
        rows = clean[clean["cluster"] == best].sort_values("date")
        return rows["amount"].tolist(), rows["date"].iloc[-1]

    @staticmethod
    def _cycle_ordinals(rows: pd.DataFrame) -> pd.Series:
        """Salary cycles as month numbers, de-duplicated and sorted."""
        cycles = rows["cycle"].astype("period[M]")
        return (cycles.dt.year * 12 + cycles.dt.month).drop_duplicates().sort_values()

    def _cycle_is_regular(self, rows: pd.DataFrame) -> bool:
        """Is the salary paid every cycle, with no long gap?

        Measured as the gap between *consecutive* cycles, not the spread around
        the median: a perfectly monthly salary has median-deviations that grow
        with the length of the history, so six consecutive months would fail a
        tolerance that three months passes.
        """
        ordinals = self._cycle_ordinals(rows)
        if len(ordinals) < 2:
            return False
        return bool(ordinals.diff().dropna().max() <= self.s.salary_max_cycle_gap)

    def _cycle_continuity(self, rows: pd.DataFrame) -> float:
        """1.0 when every month in the observed span was paid; lower with gaps.

        Used to rank candidates, so an unbroken monthly salary is preferred over
        one of the same size with a skipped month.
        """
        ordinals = self._cycle_ordinals(rows)
        if len(ordinals) < 2:
            return 0.0
        span = int(ordinals.iloc[-1] - ordinals.iloc[0]) + 1
        return round(len(ordinals) / span, 3)

    def _payday_is_regular(self, rows: pd.DataFrame) -> bool:
        """Do these payments land within `salary_max_date_drift` days of the usual payday?

        Each payment is positioned relative to the last day of its cycle month
        (31 Jan -> 0, 30 Jan -> -1, 1 Feb -> +1), which makes 28/29/30/31-day
        months comparable and keeps month-end paydays together.
        """
        if len(rows) < 2:
            return False
        cycle_end = rows["cycle"].astype("period[M]").dt.to_timestamp(how="end").dt.normalize()
        position = (rows["date"].dt.normalize() - cycle_end).dt.days
        return bool((position - position.median()).abs().max() <= self.s.salary_max_date_drift)

    def _baseline_salary(self, salary: pd.Series) -> float:
        non_zero = salary[salary > 0]
        if non_zero.empty:
            return 0.0
        return float(non_zero.min() if self.s.salary_baseline_mode == "min_non_zero" else non_zero.median())

    # ------------------------------------------------------------- profiles
    def _salaried(self, salary: pd.Series, last_salary_date, source: str) -> AffordabilityProfile:
        s = self.s
        net, gross = self._period_net_gross(self.core, "month")
        baseline = self._baseline_salary(salary)
        repayment = self.last_repayment
        dtir = repayment / baseline if baseline else 0.0
        gross_med = _median(gross)
        recent = None
        if last_salary_date is not None and pd.notna(last_salary_date):
            recent = bool(last_salary_date >= self.df["date"].max() - pd.Timedelta(days=s.salary_recency_days))

        return AffordabilityProfile(
            customer_type=SALARIED,
            series_length=int(self.df["week"].nunique()),
            inflow_median=round(gross_med, 2),
            base_net=round(_median(net), 2),
            net_to_gross=round(_median(net) / gross_med, 3) if gross_med else 0.0,
            persistence=round(float((salary > 0).sum() / len(gross)), 3) if len(gross) else 0.0,
            cv_net=None,
            slope=None,
            salary=round(baseline, 2),
            salary_source=source,
            salary_recent=recent,
            last_salary_date=last_salary_date,
            existing_dtir=round(dtir, 3),
            disposable_income=round(max(baseline * (s.repayment_fraction_salaried - dtir), 0.0), 2),
            betting_amount=round(self.betting_amount, 2),
            zeroing_rate=self.zeroing_rate,
            balance_floor=self._period_balance_floor("month"),
        )

    def _non_salaried(self) -> AffordabilityProfile:
        s = self.s
        net, gross = self._period_net_gross(self.df[self.is_transfer], "week")
        if len(net) == 0:
            net, gross = np.zeros(1), np.zeros(1)

        p10, p90 = np.nanpercentile(net, [10, 90]) if len(net) > 1 else (net[0], net[0])
        base_net = _median(_clamp(net, p10, p90))       # ignore one-off spikes
        gross_med = _median(gross)

        mean_net = float(np.nanmean(net))
        cv = float(np.nanstd(net) / abs(mean_net)) if abs(mean_net) > 0 else 1.0
        monthly_inflow = gross_med * 4                   # weekly median -> month
        income = monthly_inflow * s.inflow_margin
        repayment = self.last_repayment

        slope = None
        if len(net) >= s.min_weeks_for_slope:
            slope = round(float(np.polyfit(np.arange(len(net)), net, 1)[0]), 2)

        return AffordabilityProfile(
            customer_type=NON_SALARIED,
            series_length=int(self.df["week"].nunique()),
            inflow_median=round(gross_med, 2),
            base_net=round(base_net, 2),
            net_to_gross=round(base_net / gross_med, 3) if gross_med else 0.0,
            persistence=round(float(np.mean(net >= 0)), 3),
            cv_net=round(float(_clamp(cv, 0, 2)), 3),
            slope=slope,
            salary=None,
            salary_source=None,
            salary_recent=None,
            last_salary_date=None,
            existing_dtir=round(repayment / monthly_inflow, 3) if monthly_inflow else 0.0,
            disposable_income=round(s.repayment_fraction_non_salaried * income, 2),
            betting_amount=round(self.betting_amount, 2),
            zeroing_rate=self.zeroing_rate,
            balance_floor=self._period_balance_floor("week"),
        )

    def compute(self) -> AffordabilityProfile:
        salary, last_date, source = self.detect_salary()
        if len(salary) and float(salary.sum()) > 0:
            return self._salaried(salary, last_date, source)
        return self._non_salaried()


def assess_affordability(df: pd.DataFrame, settings: Settings = DEFAULT_SETTINGS) -> AffordabilityProfile:
    return AffordabilityAnalyzer(df, settings).compute()
