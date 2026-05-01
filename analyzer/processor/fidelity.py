import pandas as pd
import re

def extract_transaction_fidelity(pdf):

    transactions = []
    for page in pdf.pages:
        text = page.extract_text()
        if not text:
            continue
        lines = text.split('\n')
        for line in lines:
            # Match lines with transaction-like format: date ref details amount balance
            match = re.match(r"^(\d{2}-[A-Za-z]{3}-\d{2})\s+(\d{2}-[A-Za-z]{3}-\d{2})\s+(\S+)\s+(.*?)\s+([\d,]+\.\d{1,2})\s+([\d,]+\.\d{1,2})$", line)
            if not match:
                continue
            trans_date = match.group(1)
            value_date = match.group(2)
            channel = match.group(3)
            description = match.group(4).strip()
            amount = float(match.group(5).replace(',', ''))
            balance = float(match.group(6).replace(',', ''))
            transactions.append([trans_date, value_date, channel, description, amount, balance])

    # Create DataFrame
    df = pd.DataFrame(transactions, columns=["DATE POSTED"," VALUE DATE","CHANNEL","Narration", "Amount", "Balance"])
       # Build dataframe
    df = pd.DataFrame(transactions, columns=header)

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
    df = df[["DATE POSTED"," VALUE DATE","CHANNEL", "Narration", "Deposit", "Withdrawal", "Balance"]]
    
    return df