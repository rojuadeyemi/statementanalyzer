import pandas as pd
import numpy as np
from analyzer.data_extraction import DataExtractor
from datetime import datetime,date
import json
import os
from functools import cached_property

class Analyzer:
    """Robust Statement Analyzer: Provides financial behavior and risk insights."""

    def __init__(self, file_path: str):

        self.df, self.account_name, self.account_number = DataExtractor(file_path).transform_data()
        self.file_name = os.path.basename(file_path).split(".")[0]
        self.now = datetime.now()
        self.timestamp = self.now.strftime("%Y%m%d%H%M%S")
        self.opening_balance = None
        self.closing_balance = None

        # Ensure key columns exist
        expected_cols = {'date', 'balance', 'amount', 'type', 'monthyear', 'weekno', 'category'}
        missing = expected_cols - set(self.df.columns)
        if missing:
            raise ValueError(f"Missing expected columns: {missing}")

        # Cache common dataframes
        self.non_others_df = self.df[self.df['category']!='others']
        self.inflows =self.non_others_df[self.non_others_df["type"] == "credit"]
        self.outflows = self.non_others_df[self.non_others_df["type"] == "debit"]
        self.transfer_only_inflow = self.inflows[self.inflows['category']=='transfer']
        self.transfer_only_outflow = self.outflows[self.outflows['category']=='transfer']

        self.last_month_inflow = self.cashflow_summary['sum_credit'].iloc[-1]
        self.loan_repayments = self.data.get('loan_repayment',pd.Series())

        self.data = (self.non_others_df.groupby(["monthyear","category"])["amount"]
                     .sum()
                     .unstack(fill_value=0)
        )

        if self.account_name:
            self.account_name = self.account_name.upper()
        
        self.opening_balance = None
        self.closing_balance = None
        if 'balance' in self.df.columns:
            first = self.df.iloc[0]

            if first["type"] == "credit":
                self.opening_balance = first["balance"] - first["amount"]

            else:
                self.opening_balance = first["balance"] + first["amount"]

            self.closing_balance = self.df['balance'].iloc[-1]
        
        self.last_month_inflow = self.cashflow_summary['sum_credit'].iloc[-1]
                
    # Cashflow Analysis
    @cached_property
    def cashflow_summary(self) -> pd.DataFrame:
        """Summarize inflow and outflow trends by month."""
        summary =
            (
            self.non_others_df
    .groupby(["monthyear","type"])["amount"]
    .agg(["sum","count"])
    .unstack(fill_value=0)
            .sort_index(ascending=False)
            )
        
        summary.columns = [f"{a}_{b}" for a, b in summary.columns]

        monthly_debit = summary.get("sum_debit", 0)

        credit_count = summary["count_credit"].replace(0, np.nan)
        debit_count = summary["count_debit"].replace(0, np.nan)

        summary["net_cashflow"] = summary.get("sum_credit", 0) - monthly_debit
        summary["avg_txn_size"] = ((
            summary.get("sum_credit", 0) + monthly_debit
        ) / (credit_count + debit_count)).fillna(0)

        summary["avg_inflow_size"] = (summary.get("sum_credit", 0)/ credit_count).fillna(0)
        summary["avg_outflow_size"] =(monthly_debit/ debit_count).fillna(0)
        
        if 'balance' in self.df.columns:
            # Group by month
            grouped = self.df.groupby("monthyear")
            summary['closing_balance'] = grouped['balance'].last()
            summary['opening_balance'] = summary['closing_balance'].shift(-1).fillna(self.opening_balance)
        
        summary.reset_index(inplace=True)
        return summary
        
    @cached_property
    def cashflow_summary_wk(self) -> pd.DataFrame:
        """Summarize inflow and outflow trends by week."""
        summary = (
            self.non_others_df
    .groupby(["weekno","type"])["amount"]
    .agg(["sum","count"])
    .unstack(fill_value=0)
            .sort_index(ascending=False)
        )
        
        summary.columns = [f"{a}_{b}" for a, b in summary.columns]

        weekly_debit = summary.get("sum_debit", 0)

        credit_count = summary["count_credit"].replace(0, np.nan)
        debit_count = summary["count_debit"].replace(0, np.nan)

        summary["net_cashflow"] = summary.get("sum_credit", 0) - weekly_debit
        summary["avg_txn_size"] = ((
            summary.get("sum_credit", 0) + weekly_debit
        ) / (credit_count + debit_count)).fillna(0)

        summary["avg_inflow_size"] = (summary.get("sum_credit", 0)/ credit_count).fillna(0)
        summary["avg_outflow_size"] =(weekly_debit/ debit_count).fillna(0)
        
        if 'balance' in self.df.columns:
            # Group by week
            grouped = self.df.groupby("weekno")
            summary['closing_balance'] = grouped['balance'].last()
            summary['opening_balance'] = summary['closing_balance'].shift(-1).fillna(self.opening_balance)
        
        summary.reset_index(inplace=True)
        return summary

    @property
    def dtir(self):

        latest_repayment = (
            self.loan_repayments.iloc[-1]
            if self.loan_repayments is not None and not self.loan_repayments.empty
            else 0
        )

        return latest_repayment/self.last_month_inflow if self.last_month_inflow > 0 else 0

    @cached_property
    def cashflows_by_category(self):
        """Monthly cashflow breakdown by category."""
        
        result = (
            self.non_others_df
    .groupby(["monthyear","category"])["amount"]
    .agg(["sum","count"])
    .unstack(fill_value=0)
            .sort_index(ascending=False)
        )
        
        result.columns = [f"{a}_{b}" for a, b in result.columns]
        result.reset_index(inplace=True)
        return result

    # Behavioral Analytics
    @cached_property
    def inflow_sources(self) -> pd.DataFrame:
        """Identify frequent senders (incoming transfers)."""
        
        if self.transfer_only_inflow.empty:
            return pd.DataFrame()

        return (
            self.transfer_only_inflow.groupby("sender")
            .agg(total_inflow=("amount", "sum"), txn_count=("sender", "count"))
            .reset_index()
            .sort_values("total_inflow", ascending=False)
        )

    @cached_property
    def outflow_destinations(self) -> pd.DataFrame:
        """Identify common recipients (outgoing transfers)."""
        
        if self.transfer_only_outflow.empty:
            return pd.DataFrame()

        return (
            self.transfer_only_outflow.groupby("receiver", dropna=False)
            .agg(total_outflow=("amount", "sum"), txn_count=("receiver", "count"))
            .reset_index()
            .sort_values("total_outflow", ascending=False)
        )

    @cached_property
    def account_sweep(self) -> pd.DataFrame:
        """Detect repetitive round-trips (same receiver, same day, same amount)."""
        
        sweep = (
            self.transfer_only_outflow.groupby(["receiver", "amount", "date"])
            .size()
            .reset_index(name="repeat_count")
        )

        return sweep[sweep["repeat_count"] > 1]

    @cached_property
    def average_monthly_balance(self) -> pd.DataFrame | None:
        """Estimate monthly average balance."""
        if 'balance' in self.df.columns:
            avg_bal = (
                self.df.groupby("monthyear")["balance"]
                .agg(["mean", "min", "max"])
                .rename(columns={"mean": "avg_balance"})
                .reset_index()
            )
            
            return avg_bal.sort_values("monthyear", ascending=False)
        else:
            return pd.DataFrame()

    # Risk & Behavioral Insights
    @cached_property
    def risk_indicators(self):
        """Compute behavioral red flags and liquidity patterns."""
        inflow = self.inflows["amount"]
        outflow = self.outflows["amount"]
        loan_disbursements = self.data.get('loan',pd.Series())
        average_inflow = self.transfer_only_inflow['amount'].mean()

        flight_risk = "Exist" if (self.df["category"] == "travelling").sum() > 0 else "Not exist"
        
        bal_floor = None
        if 'balance' in self.df.columns:
            self.closing_balance = self.df['balance'].iloc[-1]
            balances = self.df.groupby("monthyear")["balance"].last()
            bal_floor = np.nanpercentile(balances.values, 25) if len(balances) else 0

        if not self.data.get("betting", pd.Series()).empty:
            betting_amount = self.data.get("betting").max()
        else:
            betting_amount = 0
            
        betting_ratio = betting_amount/self.last_month_inflow if self.last_month_inflow > 0 else 0

        net = self.cashflow_summary['net_cashflow']

        volatility = net.std()/np.abs(net.mean())

        sender_share = (
        self.inflow_sources["total_inflow"]
        / self.inflow_sources["total_inflow"].sum()
    )
    
        largest_share = sender_share.max()
        
        return pd.Series({  "Account Name": self.account_name,
                            "Account Number": self.account_number,
                            "Tenor": f'{self.df["monthyear"].nunique()} Months',
                            "Start Date": str(self.df["date"].min().date()),
                            "End Date": str(self.df["date"].max().date()),
                            "Inflow Count": self.inflows.shape[0],
                            "Outflow Count": self.outflows.shape[0],
                            "Total Transactions": self.df.shape[0],
                            "Total Inflow": int(inflow.sum()),
                            "Total Outflow": int(abs(outflow.sum())),
                            "Average Inflow": int(np.nan_to_num(average_inflow)),
                            "Saving Rate": round((inflow.sum() - abs(outflow.sum()))/inflow.sum(), 2) if inflow.sum() > 0 else 0,
                            "Opening Balance":round(self.opening_balance,2),
                            "Closing Balance":round(self.closing_balance,2),
                            "Inflow-Outflow Ratio": round(inflow.sum() / abs(outflow.sum()), 2) if abs(outflow.sum()) > 0 else None,
                            "Debit-Credit Frequency Ratio": round(self.outflows.shape[0] / self.inflows.shape[0], 2) if self.inflows.shape[0] > 0 else None,
                            "Loan Repayment Amount": int(self.loan_repayments.sum()),
                            "Loan Repayment Count": len(self.loan_repayments),
                            "Loan Disbursement Amount": int(abs(loan_disbursements.sum())),
                            "Loan Disbursement Count": len(loan_disbursements),
                            "VAS Amount": abs(self.df[self.df["category"] == "VAS"]["amount"].sum()),
                            "Flight Risk": flight_risk,
                          "Concentration Risk": round(np.nan_to_num(largest_share),2),
                            "DTIR":self.dtir,
                            "Zeroing Rate": self.zeroing_rate,
                            "Balance Floor": bal_floor,
                            "Betting Ratio": round(betting_ratio,2),
                          "Cashflow Volatility": volatility

                        }, name="value")

    @property
    def zeroing_rate(self):

        """Calculate percentage of days account balance ends in zero value."""

        # End of day balances
        eod_bal = self.df.groupby("date")["balance"].last().dropna()

        if len(eod_bal) == 0:
            return 1.0
        
        inflow = (
            self.transfer_only_inflow
            .groupby("date")["amount"]
            .sum()
        )

        common = eod_bal.index.intersection(inflow.index)

        median_inflow = (np.nanmedian(inflow.loc[common].values) if len(common) else 0)

        threshold = median_inflow * 0.05

        return round(float(np.mean(eod_bal.loc[common].values < threshold)),2)

    # Summary Output
    def output(self):
        """Print structured report summary."""
        
        print("\n" + "=" * 60)
        print("ACCOUNT STATEMENT SUMMARY")
        print("=" * 60)
        print(self.risk_indicators)

        
        print("=" * 60)
        print("MONTH-ON-MONTH CASHFLOW")
        print("=" * 60)
        print(self.cashflow_summary)

        print("\n" + "=" * 60)
        print("CASHFLOW BY CATEGORY")
        print("=" * 60)
        print(self.cashflows_by_category)

        print("\n" + "=" * 60)
        print("ROUND-TRIP TRANSFERS (ACCOUNT SWEEP)")
        print("=" * 60)
        print(self.account_sweep)

        print("\n" + "=" * 60)
        print("INFLOW SOURCES BY SENDER")
        print("=" * 60)
        print(self.inflow_sources)

        print("\n" + "=" * 60)
        print("OUTFLOWS BY RECEIVER")
        print("=" * 60)
        print(self.outflow_destinations)
        
        print("\n" + "=" * 60)
        print("AVERAGE MONTHLY BALANCE")
        print("=" * 60)
        print(self.average_monthly_balance)

        print("\n" + "=" * 60)
        print("LOAN REPAYMENT TRANSACTIONS")
        print("=" * 60)
        print(self.df[self.df["category"].eq("loan_repayment"]))

        print("\n" + "=" * 60)
        print("LOAN DISBURSEMENT TRANSACTIONS")
        print("=" * 60)
        print(self.df[self.df["category"].eq("loan"))

    # ----------------------------------------------------------------------
    # REPORT BUILDER
    # ----------------------------------------------------------------------
    def save_excel_report(self):
        
        with pd.ExcelWriter(f"reports/{self.file_name}_{self.timestamp}.xlsx", engine='openpyxl') as writer:
            self.risk_indicators.to_excel(writer, sheet_name='Statement Summary')
            self.df.to_excel(writer, sheet_name='Transaction Data', index=False)
            self.cashflow_summary.to_excel(writer, sheet_name='Month-on-Month Cashflow', index=False)
            self.cashflow_summary_wk.to_excel(writer, sheet_name='Week-on-Week Cashflow', index=False)
            self.cashflows_by_category.to_excel(writer, sheet_name='Category Cashflow', index=False)
            self.account_sweep.to_excel(writer, sheet_name='Account Sweep', index=False)
            self.inflow_sources.to_excel(writer, sheet_name='Inflow By Sender', index=False)
            self.outflow_destinations.to_excel(writer, sheet_name='Outflow By Receiver', index=False)
            self.average_monthly_balance.to_excel(writer, sheet_name='Account Balance', index=False)

    def generate_excel_report(self):
        """Generate Excel file in memory and return buffer."""
        from io import BytesIO

        output = BytesIO()

        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            self.risk_indicators.to_excel(writer, sheet_name='Statement Summary')
            self.df.to_excel(writer, sheet_name='Transaction Data', index=False)
            self.cashflow_summary.to_excel(writer, sheet_name='Month-on-Month Cashflow', index=False)
            self.cashflow_summary_wk.to_excel(writer, sheet_name='Week-on-Week Cashflow', index=False)
            self.cashflows_by_category.to_excel(writer, sheet_name='Category Cashflow', index=False)
            self.account_sweep.to_excel(writer, sheet_name='Account Sweep', index=False)
            self.inflow_sources.to_excel(writer, sheet_name='Inflow By Sender', index=False)
            self.outflow_destinations.to_excel(writer, sheet_name='Outflow By Receiver', index=False)
            self.average_monthly_balance.to_excel(writer, sheet_name='Account Balance', index=False)

        output.seek(0)
        return output


    def generate_json_report(self) -> str:
        """Return a structured JSON report for API or dashboard consumption."""

        report = {
            "report_generated_at": self.now.strftime("%Y-%m-%d %H:%M:%S"),
            "summary": {
                "total_transactions": int(self.df.shape[0]),
                "tenor": int(self.df["monthyear"].nunique()),
                "start_date": str(self.df["date"].min().date()),
                "end_date": str(self.df["date"].max().date()),
                "total_inflow": int(self.inflows["amount"].sum()),
                "total_outflow": int(abs(self.outflows["amount"].sum())),
                "net_position": int(self.inflows["amount"].sum() - abs(self.outflows["amount"].sum())),
            },
            "cashflow_summary": self.cashflow_summary,
            "cashflows_by_category": self.cashflows_by_category,
            "inflow_sources": self.inflow_sources,
            "outflow_destinations": self.outflow_destinations,
            "round_trip_transfers": self.account_sweep,
            "average_monthly_balance": self.average_monthly_balance,
            "risk_indicators": self.risk_indicators,
            "loan_transactions": {
                "repayments": self.df[self.df["category"] == "loan_repayment"],
                "disbursements": self.df[self.df["category"] == "loan"],
            },
        }
    
        return json.dumps(report, indent=2, default=Analyzer.safe_json_convert)

    def save_json(self):
        # Writing to a file
        os.makedirs('reports', exist_ok=True)
        report_file = os.path.join("reports", f"{self.file_name}_{self.timestamp}.json")
        with open(report_file, "w") as file:
            json.dump(self.generate_json_report(), file)

    @staticmethod
    def safe_json_convert(obj):
        """Convert non-serializable objects to JSON-friendly types."""
        if isinstance(obj, (np.integer,)):
            return int(obj)
        elif isinstance(obj, (np.floating,)):
            return float(obj)
        elif isinstance(obj, (np.bool_)):
            return bool(obj)
        elif isinstance(obj, (pd.Timestamp, datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, pd.Series):
            return obj.to_dict()
        elif isinstance(obj, pd.DataFrame):
            return obj.to_dict(orient="records")
        else:
            return str(obj)
