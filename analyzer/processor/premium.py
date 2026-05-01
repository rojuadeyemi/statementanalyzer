import pandas as pd
import re

PATTERN_SINGLE = re.compile(
    r'^(\d{2}-[A-Z][a-z]{2}-\d{2})\s+'
    r'(.+?)\s+'
    r'(\d{2}-[A-Z][a-z]{2}-\d{2})\s+'
    r'([\d,]+\.\d{2})\s+'
    r'([\d,]+\.\d{2})$'
)

PATTERN_SPLIT = re.compile(
    r'^(\d{2}-[A-Z][a-z]{2}-\d{2})\s+'
    r'(\d{2}-[A-Z][a-z]{2}-\d{2})\s+'
    r'([\d,]+\.\d{2})\s+'
    r'([\d,]+\.\d{2})$'
)

def extract_transaction_premium(pdf):

    transactions = []
    header = ['Trans Date','Narration','Value date','Amount','Balance']

    for page in pdf.pages:

        text = page.extract_text()
        if not text:
            continue

        lines = [l.strip() for l in text.split('\n')]

        i = 0
        while i < len(lines):

            line = lines[i]

            # --- Pattern 1: normal line ---
            match = PATTERN_SINGLE.match(line)

            if match:
                transactions.append([
                    match.group(1),
                    match.group(2),
                    match.group(3),
                    match.group(4).replace(',', ''),
                    match.group(5).replace(',', '')
                ])

            # --- Pattern 2: narration on previous line ---
            elif i > 0:

                match2 = PATTERN_SPLIT.match(line)

                if match2:
                    narration = lines[i-1]

                    transactions.append([
                        match2.group(1),
                        narration,
                        match2.group(2),
                        match2.group(3).replace(',', ''),
                        match2.group(4).replace(',', '')
                    ])

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