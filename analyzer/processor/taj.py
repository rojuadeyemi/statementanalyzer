import pandas as pd
import re

def extract_transaction_taj(pdf):
    transactions = []
    header=['Trans Date',
   'Value date',
   'Branch',
   'Narration',
   'Amount',
   'Balance']
    for page in pdf.pages:
        
        text = page.extract_text()
        
        if not text:
            continue
        lines = text.split('\n')
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            pattern = re.compile(
                                r'^(\d{2}-[A-Z]{3}-\d{2})\s+'
                                r'(\d{2}-[A-Z]{3}-\d{2})\s+'
                                r'(\d+)\s+'
                                r'(.+?)\s+'
                                r'([\d,]+\.\d{2})\s+'
                                r'([\d,]+\.\d{2})$'
                            )
            # Pattern 1: normal single-line match
            match = pattern.match(line)
            if match:
                trans_date = match.group(1)
                val_date = match.group(2)
                branch_code = match.group(3)
                ref = match.group(4)
                amount = match.group(5).replace(',', '')
                balance = match.group(6).replace(',', '')
                transactions.append([trans_date, val_date, branch_code,ref,
                                     amount, balance])
                
                i += 1
                continue
                
            i += 1
    
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
    
    return df