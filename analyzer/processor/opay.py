import pandas as pd
import re

def extract_transaction_opay_new(pdf):
    transactions = []
    
    # Build main regex
    prefix_re = r'^.*?'

    first_line = (
        r'^(\d{4}-)\s+'
    )

    second_line = (
        r'(\d{2}-)\s+'
    )

    third_line = (
        r'^(.*?)\s+'  # narration (greedy, allows anything)
        r'([\+\-]?\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s+'   # Amount
        r'([\+\-]?\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*'   # Debit
        r'(\s*|[\+\-]?\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s+'   # Credit
        r'([\+\-]?\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*'   # balance before
        r'(\s+|[\+\-]?\d{1,3}(?:,\d{3})*(?:\.\d{2})?)'   # balance after
    )

    fourth_line = (
        r'(\d{2})T\d{2}:\s*'     # Value date
    )

    pattern1 = re.compile(prefix_re + first_line)
    pattern2 = re.compile(prefix_re + second_line)
    pattern3 = re.compile(prefix_re + third_line)
    pattern4 = re.compile(prefix_re + fourth_line)

    date = None
    # ---------------- PAGE LOOP ----------------
    for page in pdf.pages:
        text = page.extract_text()
        if not text:
            continue

        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
        n = len(lines)
        i = 0
        
        while i + 4 < n:

            line = lines[i]
            line = line.replace(r'|', '')
                
            # Try search
            m1 = pattern1.match(line)

            if m1:
                m2 = pattern2.match(lines[i+1])
                if m2:
                    m3 = pattern3.match(lines[i+2])
                    m4 = pattern4.match(lines[i+3])
                if not m2:
                    m2 = pattern2.match(lines[i+2])
                    m3 = pattern3.match(lines[i+3])
                    m4 = pattern4.match(lines[i+4])
                if not m3:
                    m3 = pattern3.match(lines[i+2]) or pattern3.match(lines[i+1])
                    m4 = pattern4.match(lines[i+3]) or pattern4.match(lines[i+2])
            else:
                m3 = pattern3.match(lines[i])


            if m1 and m2 and m3 and m4:

                date = m1.group(1) + m2.group(1) + m4.group(1)
                debit = float(m3.group(3).split(".")[0].replace(",",""))
                amount = float(m3.group(2).split(".")[0].replace(",",""))
                
                amount = -1*amount if debit > 0 else amount
                balance = m3.group(6) or m3.group(5)

                transactions.append([
                    date,
                    m3.group(1),
                    amount,
                    balance
                ])
                i += 4
                continue

            if not date:
                i += 1
                continue

            if m3:
                
                debit = float(m3.group(3).split(".")[0].replace(",",""))
                amount = float(m3.group(2).split(".")[0].replace(",",""))
                amount = -1*amount if debit > 0 else amount
                balance = m3.group(6) or m3.group(5)
                transactions.append([
                    date, # use the date before as a proxy
                    m3.group(1),
                    amount,
                    balance
                ])
                i += 1
                continue

            if not m3 and  (i+4 < n):
                m3= pattern3.match(lines[i+1])
                if m3:
                    i += 2
                if not m3:
                    m3 = pattern3.match(lines[i+2])
                    if m3:
                        i +=3
                if not m3:
                    m3= pattern3.match(lines[i+3])
                    if m3:
                        i += 4

                if m3:

                    debit = float(m3.group(3).split(".")[0].replace(",",""))
                    amount = float(m3.group(2).split(".")[0].replace(",",""))
                    amount = -1*amount if debit > 0 else amount
                    balance = m3.group(6) or m3.group(5)
                    transactions.append([
                        date, # use the date before as a proxy
                        m3.group(1),
                        amount,
                        balance
                    ])
                
                    continue

            # Print those not matched for inspection
            #print(line)
            i += 1
            continue

    return pd.DataFrame(transactions, columns=["Value Date", "Narration", "Amount","Balance"])

def extract_transaction_opay(pdf):
    transactions = []

    # check if it is a special Opay statement
    if "Reversal Transaction Settlement" in pdf.pages[0].extract_text():
        return extract_transaction_opay_new(pdf)
    
    # Build main regex
    prefix_re = r'^.*?'

    body_re = (
        r'(\d{2}\s+[A-Za-z]{3}\s+\d{4})\s+'     # Value date
        r'([A-Za-z0-9()].*?)?\s*'                             # Narration
        r'([\+\-]\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s+'   # STRICT amount
        r'(--|[\+\-]?\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s+'   # STRICT balance
        r'(.*)$'
    )

    pattern = re.compile(prefix_re + body_re)

    # Regex for fixing
    broken_header = re.compile(
        r'^\d{4}\s+[A-Za-z]{3}\s+\d{1,2}\s+'      # 2024 Aug 20
        r'\d{1,2}:\s*\d{0,2}:?\s+'                  # 16:  or 16:30
        r'\d{1,2}\s+[A-Za-z]{3}'                 # 20 Aug
    )

    # Regex to repair missing year: "20 Aug" = "20 Aug 2024"
    fix_value_date = re.compile(
        r'\b(\d{1,2}\s+[A-Za-z]{3})\b'
    )
    pending = None
    
    # ---------------- PAGE LOOP ----------------
    for page in pdf.pages:
        text = page.extract_text()
        if not text:
            continue

        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
        n = len(lines)
        i = 0
        
        while i < n:
            line = lines[i]
            line = line.replace(r'|', '')

            # Skip useless lines early
            if len(line) < 15 or line[-1]==":":
                i += 1
                continue
                
            if pending:
                line = pending + " " + line
                pending = None
                
            # Try normal search
            m = pattern.match(line)
    
            if m:
                transactions.append([
                    m.group(1),
                    m.group(2),
                    m.group(3),
                    m.group(4),
                    m.group(5)
                ])
                
                i += 1
                continue

            # If fails, try broken line fix
            if broken_header.match(line):
                
                # Fix missing year (only once)
                first_date = re.search(r'\d{4}\s+[A-Za-z]{3}\s+\d{1,2}', line)
                if first_date:
                    year = first_date.group(0).split()[0]
                    line = fix_value_date.sub(rf"\1 {year}", line)
                    
                if i + 1 == n:
                    pending = line
                    i+=1
                    continue
                
                if i + 1 < n:
                    merged = line + " " + lines[i + 1]

                # Try match again
                m2 = pattern.match(merged)
                if m2:
                
                    transactions.append([
                        m2.group(1),
                        m2.group(2).strip(),
                        m2.group(3),
                        m2.group(4),
                        m2.group(5).strip()
                    ])
                    i += 2
                    continue

            # if there's still no match
            #print(line)
            i += 1

    return pd.DataFrame(transactions, columns=[
        "Value Date", "Narration", "Amount", "Balance", "Channel - Reference"
    ])

