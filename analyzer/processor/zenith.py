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
            if not match:
                continue
            trans_date = match.group(1)
            value_date = match.group(2)
            narration = match.group(3).strip()
            amount = float(match.group(4).replace(',', ''))
            balance = float(match.group(5).replace(',', ''))
            transactions.append([trans_date, value_date, narration, amount, balance])
    
    # Create DataFrame
    df = pd.DataFrame(transactions, columns=["DATE POSTED"," VALUE DATE", "Narration", "Amount", "Balance"])
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")
    df["Balance"] = pd.to_numeric(df["Balance"], errors="coerce")
    
    # Initialize columns
    df["Credit"] = 0.0
    df["Debit"] = 0.0
    
    # Handle first row by looking at next row
    if len(df) >= 2:
        delta = df.loc[1, "Balance"] - df.loc[0, "Balance"]
        if delta > 0:
            df.loc[0, "Debit"] = df.loc[0, "Amount"]
        else:
            df.loc[0, "Credit"] = df.loc[0, "Amount"]
    
    # Handle all other rows
    for i in range(1, len(df)):
        prev_balance = df.loc[i - 1, "Balance"]
        curr_balance = df.loc[i, "Balance"]
        amt = df.loc[i, "Amount"]
        if pd.isna(prev_balance) or pd.isna(curr_balance):
            continue
        if curr_balance > prev_balance:
            df.loc[i, "Credit"] = amt
        else:
            df.loc[i, "Debit"] = amt
    
    # Drop ambiguous 'Amount' column
    df.drop(columns=["Amount"], inplace=True)
    
    # Final column order
    df = df[["DATE POSTED"," VALUE DATE", "Narration", "Credit", "Debit", "Balance"]]

    return df