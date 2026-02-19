import pandas as pd
import re

def extract_transaction_monie_correct(pdf):
    transactions = []

    # Reusable pattern: date + any description + 3 numbers
    txn_pattern = re.compile(
        r"^(\d{4}-\d{2}-\d{2})T\d+:?\s+(.+?)\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})$"
    )
    for page in pdf.pages:
        text = page.extract_text()
        if not text:
            continue

        lines = [line.strip() for line in text.split('\n') if line.strip()]
        i = 0

        while i < len(lines):
            # Try single line first
            m_single = txn_pattern.match(lines[i])
            if m_single:
                txn_time, description, debit, credit, balance = m_single.groups()
                transactions.append([
                    txn_time,
                    description,
                    float(debit.replace(',', '')),
                    float(credit.replace(',', '')),
                    float(balance.replace(',', ''))
                ])
                i += 1
                continue

            # Try 2-line combo: KEEP full line1 + line2
            if i + 1 < len(lines):
                combined = f"{lines[i]} {lines[i + 1]}"
                m_join = txn_pattern.match(combined)
                if m_join:
                    txn_time, description, debit, credit, balance = m_join.groups()
                    transactions.append([
                        txn_time,
                        description,
                        float(debit.replace(',', '')),
                        float(credit.replace(',', '')),
                        float(balance.replace(',', ''))
                    ])
                    i += 2
                    continue

            # Not matched, then skip
            #print(lines[i])
            i += 1

    df = pd.DataFrame(
        transactions,
        columns=["Date", "Description", "Debit", "Credit", "Balance"]
    )
    #df['Date'] = df['Date'].str.split('T').str[0]
    return df