import pandas as pd
import numpy as np
from analyzer.data_extraction import DataExtractor
from datetime import datetime,date
import json
import os

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
        expected_cols = {"date", "amount", "category", "type", "monthyear"}
        missing = expected_cols - set(self.df.columns)
        if missing:
            raise ValueError(f"Missing expected columns: {missing}")

        # Cache common dataframes
        self.non_others_df = self.df[self.df['category']!='others']
        self.inflows =self.df[self.df["type"] == "credit"]
        self.outflows = self.df[self.df["type"] == "debit"]
        self.transfer_only_inflow = self.inflows[self.inflows['category']=='transfer']
        self.transfer_only_outflow = self.outflows[self.outflows['category']=='transfer']

        if self.account_name:
            self.account_name = self.account_name.upper()
        
        if 'balance' in self.df.columns:
            self.opening_balance = self.df['balance'].iloc[0] + self.df['amount'].iloc[0]*(-1 if self.df['type'].iloc[0]=='credit' else 1)

    # Cashflow Analysis
    def cashflow_summary(self) -> pd.DataFrame:
        """Summarize inflow and outflow trends by month."""
        summary = (
            self.df.pivot_table(
                index="monthyear",
                columns="type",
                values="amount",
                aggfunc=["sum", "count"],
                fill_value=0,
            )
            .sort_index(ascending=False)
        )
        
        summary.columns = [f"{a}_{b}" for a, b in summary.columns]

        monthly_debit = summary.get("sum_debit", 0)

        summary["net_cashflow"] = summary.get("sum_credit", 0) - monthly_debit
        summary["avg_txn_size"] = (
            summary.get("sum_credit", 0) + monthly_debit
        ) / (summary.get("count_credit", 1) + summary.get("count_debit", 1))

        summary["avg_inflow_size"] = summary.get("sum_credit", 0)/ summary.get("count_credit", 1)
        summary["avg_outflow_size"] =monthly_debit/ summary.get("count_debit", 1)
        
        if 'balance' in self.df.columns:
            # Group by month
            grouped = self.df.groupby("monthyear")
            summary['closing_balance'] = grouped['balance'].last()
            summary['opening_balance'] = summary['closing_balance'].shift(-1).fillna(self.opening_balance)
        
        summary.reset_index(inplace=True)
        return summary

    def cashflow_summary_wk(self) -> pd.DataFrame:
        """Summarize inflow and outflow trends by week."""
        summary = (
            self.df.pivot_table(
                index="weekno",
                columns="type",
                values="amount",
                aggfunc=["sum", "count"],
                fill_value=0,
            )
            .sort_index(ascending=False)
        )
        
        summary.columns = [f"{a}_{b}" for a, b in summary.columns]

        weekly_debit = summary.get("sum_debit", 0)

        summary["net_cashflow"] = summary.get("sum_credit", 0) - weekly_debit
        summary["avg_txn_size"] = (
            summary.get("sum_credit", 0) + weekly_debit
        ) / (summary.get("count_credit", 1) + summary.get("count_debit", 1))

        summary["avg_inflow_size"] = summary.get("sum_credit", 0)/ summary.get("count_credit", 1)
        summary["avg_outflow_size"] =weekly_debit/ summary.get("count_debit", 1)
        
        if 'balance' in self.df.columns:
            # Group by month
            grouped = self.df.groupby("weekno")
            summary['closing_balance'] = grouped['balance'].last()
            summary['opening_balance'] = summary['closing_balance'].shift(-1).fillna(self.opening_balance)
        
        summary.reset_index(inplace=True)
        return summary
    

    def cashflows_by_category(self):
        """Monthly cashflow breakdown by category."""
        
        result = (
            self.non_others_df.pivot_table(
                index="monthyear",
                columns="category",
                values="amount",
                aggfunc=["sum", "count"],
                fill_value=0,
            )
            .sort_index(ascending=False)
        )
        result.columns = [f"{a}_{b}" for a, b in result.columns]
        result.reset_index(inplace=True)
        return result

    # Behavioral Analytics
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

    def account_sweep(self) -> pd.DataFrame:
        """Detect repetitive round-trips (same receiver, same day, same amount)."""
        
        sweep = (
            self.transfer_only_outflow.groupby(["receiver", "amount", "date"])
            .size()
            .reset_index(name="repeat_count")
        )

        return sweep[sweep["repeat_count"] > 1]

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
    def risk_indicators(self):
        """Compute behavioral red flags and liquidity patterns."""
        inflow = self.inflows["amount"]
        outflow = self.outflows["amount"]
        loan_repayments = self.df[self.df["category"] == "loan_repayment"]["amount"]
        loan_disbursements = self.df[self.df["category"] == "loan"]["amount"]
        average_inflow = self.transfer_only_inflow['amount'].mean()

        flight_risk = "Exist" if (self.df["category"] == "travelling").sum() > 0 else "Not exist"
        
        if 'balance' in self.df.columns:
            self.closing_balance = self.df['balance'].iloc[-1]

        return pd.Series({  "Account Name": self.account_name,
                            "Account Number": self.account_number,
                            "Tenor": f'{self.df["monthyear"].nunique()} Months',
                            "Start Date": str(self.df["date"].min().date()),
                            "End Date": str(self.df["date"].max().date()),
                            "Inflow Count": len(self.inflows),
                            "Outflow Count": len(self.outflows),
                            "Total Transactions": len(self.df),
                            "Total Inflow": int(inflow.sum()),
                            "Total Outflow": int(abs(outflow.sum())),
                            "Average Inflow": int(np.nan_to_num(average_inflow)),
                            "Net Position": int(inflow.sum() - abs(outflow.sum())),
                            "Opening Balance":round(self.opening_balance,2),
                            "Closing Balance":round(self.closing_balance,2),
                            "Inflow-Outflow Ratio": round(inflow.sum() / abs(outflow.sum()), 2) if abs(outflow.sum()) > 0 else None,
                            "Debit-Credit Frequency Ratio": round(len(self.outflows) / len(self.inflows), 2) if len(self.inflows) > 0 else None,
                            "Loan Repayment Amount": int(loan_repayments.sum()),
                            "Loan Repayment Count": len(loan_repayments),
                            "Loan Disbursement Amount": int(abs(loan_disbursements.sum())),
                            "Loan Disbursement Count": len(loan_disbursements),
                            "VAS": abs(self.df[self.df["category"] == "VAS"]["amount"].sum()),
                            "Flight Risk": flight_risk

                        }, name="value")

    # Summary Output

    def output(self):
        """Print structured report summary."""
        
        print("\n" + "=" * 60)
        print("ACCOUNT STATEMENT SUMMARY")
        print("=" * 60)
        print(self.risk_indicators())

        
        print("=" * 60)
        print("MONTH-ON-MONTH CASHFLOW")
        print("=" * 60)
        print(self.cashflow_summary())

        print("\n" + "=" * 60)
        print("CASHFLOW BY CATEGORY")
        print("=" * 60)
        print(self.cashflows_by_category())

        print("\n" + "=" * 60)
        print("ROUND-TRIP TRANSFERS (ACCOUNT SWEEP)")
        print("=" * 60)
        print(self.account_sweep())

        print("\n" + "=" * 60)
        print("INFLOW SOURCES BY SENDER")
        print("=" * 60)
        print(self.inflow_sources())

        print("\n" + "=" * 60)
        print("OUTFLOWS BY RECEIVER")
        print("=" * 60)
        print(self.outflow_destinations())
        
        print("\n" + "=" * 60)
        print("AVERAGE MONTHLY BALANCE")
        print("=" * 60)
        print(self.average_monthly_balance())

        print("\n" + "=" * 60)
        print("LOAN REPAYMENT TRANSACTIONS")
        print("=" * 60)
        print(self.df[self.df["category"] == "loan_repayment"])

        print("\n" + "=" * 60)
        print("LOAN DISBURSEMENT TRANSACTIONS")
        print("=" * 60)
        print(self.df[self.df["category"] == "loan"])

    # ----------------------------------------------------------------------
    # REPORT BUILDER
    # ----------------------------------------------------------------------
    def save_excel_report(self):
        
        with pd.ExcelWriter(f"reports/{self.file_name}_{self.timestamp}.xlsx", engine='openpyxl') as writer:
            self.risk_indicators().to_excel(writer, sheet_name='Statement Summary')
            self.df.to_excel(writer, sheet_name='Transaction Data', index=False)
            self.cashflow_summary().to_excel(writer, sheet_name='Month-on-Month Cashflow', index=False)
            self.cashflow_summary_wk().to_excel(writer, sheet_name='Week-on-Week Cashflow', index=False)
            self.cashflows_by_category().to_excel(writer, sheet_name='Category Cashflow', index=False)
            self.account_sweep().to_excel(writer, sheet_name='Account Sweep', index=False)
            self.inflow_sources().to_excel(writer, sheet_name='Inflow By Sender', index=False)
            self.outflow_destinations().to_excel(writer, sheet_name='Outflow By Receiver', index=False)
            self.average_monthly_balance().to_excel(writer, sheet_name='Account Balance', index=False)

    def generate_excel_report(self):
        """Generate Excel file in memory and return buffer."""
        from io import BytesIO

        output = BytesIO()

        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            self.risk_indicators().to_excel(writer, sheet_name='Statement Summary')
            self.df.to_excel(writer, sheet_name='Transaction Data', index=False)
            self.cashflow_summary().to_excel(writer, sheet_name='Month-on-Month Cashflow', index=False)
            self.cashflow_summary_wk().to_excel(writer, sheet_name='Week-on-Week Cashflow', index=False)
            self.cashflows_by_category().to_excel(writer, sheet_name='Category Cashflow', index=False)
            self.account_sweep().to_excel(writer, sheet_name='Account Sweep', index=False)
            self.inflow_sources().to_excel(writer, sheet_name='Inflow By Sender', index=False)
            self.outflow_destinations().to_excel(writer, sheet_name='Outflow By Receiver', index=False)
            self.average_monthly_balance().to_excel(writer, sheet_name='Account Balance', index=False)

        output.seek(0)  # 🔥 VERY IMPORTANT
        return output


    def generate_json_report(self) -> str:
        """Return a structured JSON report for API or dashboard consumption."""

        report = {
            "report_generated_at": self.now.strftime("%Y-%m-%d %H:%M:%S"),
            "summary": {
                "total_transactions": int(len(self.df)),
                "tenor": int(self.df["monthyear"].nunique()),
                "start_date": str(self.df["date"].min().date()),
                "end_date": str(self.df["date"].max().date()),
                "total_inflow": int(self.inflows["amount"].sum()),
                "total_outflow": int(abs(self.outflows["amount"].sum())),
                "net_position": int(self.inflows["amount"].sum() - abs(self.outflows["amount"].sum())),
            },
            "cashflow_summary": self.cashflow_summary(),
            "cashflows_by_category": self.cashflows_by_category(),
            "inflow_sources": self.inflow_sources(),
            "outflow_destinations": self.outflow_destinations(),
            "round_trip_transfers": self.account_sweep(),
            "average_monthly_balance": self.average_monthly_balance(),
            "risk_indicators": self.risk_indicators(),
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