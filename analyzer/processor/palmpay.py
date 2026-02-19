import pandas as pd
import re

def extract_transaction_palmpay(pdf):
    transactions = []
    for page in pdf.pages:
        text = page.extract_text()
        if not text:
            continue
        lines = text.split('\n')
        for line in lines:
            # Match lines with transaction-like format: date ref details amount balance
            match = re.match(r"(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})\s+(.+?)\s+([A-Z0-9a-z]+)\s+([\+\-\d,.]+)\s+([\+\-\d,.]+)", line)
            if not match:
                continue
            timestamp = match.group(1)
            ref_type = match.group(2)
            reference = match.group(3)
            amount = float(match.group(4))
            balance = float(match.group(5))
            narration = match.group(2)
            transactions.append([timestamp, ref_type, reference, amount, balance,narration])
    # Create DataFrame
    df = pd.DataFrame(transactions, columns=["Tran Date","Transaction Type", "Reference ID","Amount(NGN)" , "Balance(NGN)",'narration'])
    
    return df