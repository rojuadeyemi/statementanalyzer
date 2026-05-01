import pandas as pd
import re


def extract_transaction_sterling(pdf):
    transactions = []

    for page in pdf.pages:
        text = page.extract_text()
        if not text:
            continue
        lines = text.split('\n')
        for line in lines:
            # Match lines with transaction-like format: date ref details amount balance
            match = re.match(r"^(\d{2}/[A-Za-z]{3}/\d{4})\s+(\d{2}/[A-Za-z]{3}/\d{4})\s+(.+?)\s+(-?\d{1,3}(?:,\d{3})*(?:\.\d{2})|-)\s+(-?\d{1,3}(?:,\d{3})*(?:\.\d{2})|-)\s+(-?\d{1,3}(?:,\d{3})*(?:\.\d{2}))$", line)
            
            if not match:
                continue
            trans_date = match.group(1)
            value_date = match.group(2)
            narration = match.group(3).strip()
            debit = match.group(4).replace(',', '').replace('-', '')
            credit = match.group(5).replace(',', '').replace('-', '')
            balance = match.group(6).replace(',', '').replace('-', '')
            transactions.append([trans_date, value_date, narration, debit, credit, balance])
        
    # Create DataFrame
    df = pd.DataFrame(transactions, columns=["Tran Date", "Value Date", "Narration", "Debit", "Credit", "Balance"])
    
    return df