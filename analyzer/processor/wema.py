import pandas as pd
import re

def extract_transaction_wema(pdf):
    transactions = []
    header=[ "Value Date", "Reference", "Details", "Amount", "Balance"]
    for page in pdf.pages:
        text = page.extract_text()
        if not text:
            continue
        lines = text.split('\n')
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            pattern = r"^(\d{2}[-\s][A-Za-z]{3}[-\s]\d{4})\s+(\S+)\s+(.+?)\s+([\+\-\d,.]+)\s+([\+\-\d,.]+)$"
            # Pattern 1: normal single-line match
            match = re.match(pattern, line)
            if match:
                date = match.group(1)
                ref = match.group(2)
                details = match.group(3).strip()
                amount = match.group(4).replace(',', '')
                balance = match.group(5).replace(',', '')
                transactions.append([date, ref, details, amount, balance])
                i += 1
                continue

            # Pattern 2: fallback partial date
            partial_date = re.match(r"^(\d{2}-[A-Za-z]{3}-)$", line)
            if partial_date and i + 2 < len(lines):
                next_line = lines[i + 1].strip()
                year_line = lines[i + 2].strip()
                m2 = re.match(
                    r"^(\S+)\s+(.+?)\s+([\+\-\d,.]+)\s+([\+\-\d,.]+)$", next_line)
                year_match = re.match(r"^\d{4}$", year_line)
                if m2 and year_match:
                    date = partial_date.group(1) + year_line
                    ref = m2.group(1)
                    details = m2.group(2).strip()
                    amount = m2.group(3).replace(',', '')
                    balance = m2.group(4).replace(',', '')
                    transactions.append([date, ref, details, amount, balance])
                    i += 3
                    continue

            # Pattern 3: two-date full numeric date with amount/balance
            match_two_dates = re.match(
                r"^(\d{2}-\d{2}-\d{4})\s+(\d{2}-\d{2}-\d{4})\s+(.+?)\s+([\d,.]+)\s+([\d,.]+)$", line)
            if match_two_dates:
                trans_date = match_two_dates.group(1)
                value_date = match_two_dates.group(2)
                details = match_two_dates.group(3).strip()
                amount = match_two_dates.group(4).replace(',', '')
                balance = match_two_dates.group(5).replace(',', '')
                transactions.append([trans_date,value_date, details, amount, balance])
                header=["Posting Date", "Value Date", "Details", "Amount", "Balance"]
                i += 1
                continue

            i += 1

    # Convert to DataFrame
    df = pd.DataFrame(transactions, columns=header)
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")
    df["Balance"] = pd.to_numeric(df["Balance"], errors="coerce")

    # Initialize credit/debit columns
    df["Credit (₦)"] = 0.0
    df["Debit (₦)"] = 0.0

    if len(df) >= 2:
        delta = df.loc[1, "Balance"] - df.loc[0, "Balance"]
        if delta > 0:
            df.loc[0, "Credit (₦)"] = df.loc[0, "Amount"]
        else:
            df.loc[0, "Debit (₦)"] = df.loc[0, "Amount"]

    for i in range(1, len(df)):
        prev_balance = df.loc[i - 1, "Balance"]
        curr_balance = df.loc[i, "Balance"]
        amt = df.loc[i, "Amount"]
        if pd.isna(prev_balance) or pd.isna(curr_balance):
            continue
        if curr_balance > prev_balance:
            df.loc[i, "Credit (₦)"] = amt
        else:
            df.loc[i, "Debit (₦)"] = amt

    df.drop(columns=["Amount"], inplace=True)

    return df