import pandas as pd
import re

def extract_transaction_zenith(pdf):
    transactions = []
    for page in pdf.pages:
        text = page.extract_text()
        if not text:
            continue
        lines = text.split('\n')
        for line in lines:
            # Match lines with transaction-like format: date ref details amount balance
            match = re.match(r"^(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})(\s|.*?)\s+([\+\-\d,.]+)\s+([\+\-\d,.]+)$", line)
            match1 = re.match(r"^(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})(\s|.*?)\s+([\+\-\d,.]+)\s+([\+\-\d,.]+)\s+([\+\-\d,.]+)$", line)
            if not match and not match1:
                continue

            if match1:
                trans_date = match1.group(1)
                value_date = match1.group(2)
                narration = match1.group(3).strip()
                debit = float(match1.group(4).replace(',', ''))
                credit = float(match1.group(5).replace(',', ''))
                balance = float(match1.group(6).replace(',', ''))
                transactions.append([trans_date, value_date, narration, debit,credit, balance])
    
            elif match:
                trans_date = match.group(1)
                value_date = match.group(2)
                narration = match.group(3).strip()
                amount = float(match.group(4).replace(',', ''))
                balance = float(match.group(5).replace(',', ''))
                transactions.append([trans_date, value_date, narration, amount, balance])
    # Create DataFrame
    if len(transactions[0])==6:
        df = pd.DataFrame(transactions, columns=["DATE POSTED"," VALUE DATE", "DESCRIPTION", "DEBIT","CREDIT", "BALANCE"])
        return df
        
    df = pd.DataFrame(transactions, columns=["DATE POSTED"," VALUE DATE", "Narration", "Amount", "Balance"])

    # Convert numeric
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")
    df["Balance"] = pd.to_numeric(df["Balance"], errors="coerce")

    # --- Vectorized Deposit / Withdrawal detection ---
    balance_diff = df["Balance"].diff()

    df["Deposit"] = df["Amount"].where(balance_diff > 0, 0)
    df["Withdrawal"] = df["Amount"].where(balance_diff <= 0, 0)

    # Fix first row
    if len(df) > 1:
        if df.loc[1, "Balance"] > df.loc[0, "Balance"]:
            df.loc[0, "Deposit"] = df.loc[0, "Amount"]
        else:
            df.loc[0, "Withdrawal"] = df.loc[0, "Amount"]

    df.drop(columns=["Amount"], inplace=True)
    
    # Final column order
    df = df[["DATE POSTED"," VALUE DATE", "Narration", "Deposit", "Withdrawal", "Balance"]]

    return df