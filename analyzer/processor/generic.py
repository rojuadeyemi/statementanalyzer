import pandas as pd
import numpy as np
import re
pd.set_option("future.no_silent_downcasting", True)

def extract_transaction_generic(pdf, keywords):
    
    dataframes = []
    header = None  # Placeholder for the table header
    table_settings = {"intersection_tolerance": 10}
    #page_counter = 0

    for page_number, page in enumerate(pdf.pages):
        
        tables = page.extract_tables(table_settings)
        
        if page_number == 0:  # First page: Identify the header
            
            header, table = find_transaction_table(tables, keywords)
            
            corrected_rows = []
            header = remove_blank_first2rows(header)
            
            if not header or not table:                    
                continue  # Skip if no transaction table is found

            elif len(tables) > 1 and len(table) < 2:
                
    
                corrected_rows = align_and_split_table(tables[1], header)
                
            elif header and table:
                if is_single_cell_row(header):  # --- Case with Keystone
                    
                    header = header[0].split('\nDate')[0].split(" ")
                
                corrected_rows = align_and_split_table(table[1:], header)
            if corrected_rows:

                dataframes.append(pd.DataFrame(corrected_rows, columns=header))
                #page_counter +=1
                #print(page_number)
        else:  # Subsequent pages
            
            corrected_rows = []  # Initialize in case no processing occurs
            header1, table1 = find_transaction_table(tables, keywords)
            for table in tables:

                if not header:  # Re-extract header if not found
                    header, table = header1, table1

                if any(keyword in str(table[0]) for keyword in header):
                    table = table[1:]

                elif table1 and any(keyword in str(table1[0]) for keyword in header):
                    table = table1[1:]

                corrected_rows = align_and_split_table(table, header)
                    
                try:
                    if corrected_rows and header:  # Avoid appending empty DataFrames

                        dataframes.append(pd.DataFrame(corrected_rows, columns=header))
                        #page_counter +=1
                        #print(page_number)
                        break
                except Exception as e:
                    print(f"Error creating DataFrame: {e}")
                    return
             
    # Combine all DataFrames into one, clean empty rows
    df = (pd.concat(dataframes, ignore_index=True)
            .replace({"": np.nan}, regex=False)
            .dropna(how='all'))
    #print(page_counter)
    return df


def find_transaction_table(tables, keywords):

    for table in tables:

        # Clean all rows upfront for easier reuse
        cleaned_table = [[cell for cell in row if cell is not None] for row in table]

        for i, row in enumerate(cleaned_table):
            if row == ['Date', 'Transaction Details', 'Reference', 'Value Date', 'Withdrawals', 'Lodgements', 'Balance'] or row==['TXN DATE', 'VAL DATE', 'REMARKS', 'DEBIT', 'CREDIT', 'BALANCE'] or row==['','Transaction Date', 'Transaction Detail', 'Money In (NGN)', 'Money Out (NGN)', 'Transaction ID','']:
                return row, cleaned_table[i:]

        if 'account statement' in str(cleaned_table[0]).lower():
            if len(cleaned_table) <3:
                continue
            
            elif 'print. date' not in str(cleaned_table[1]).lower() and any(keyword in str(cleaned_table[1]).lower() for keyword in keywords):

                return  cleaned_table[1], cleaned_table[1:]
                
        elif all(key not in str(cleaned_table[0]).lower() for key in ['print. date', 'transaction description','total credits']) and any(keyword in str(cleaned_table[0]).lower() for keyword in keywords): # For Opay

            return cleaned_table[0], cleaned_table
    
    return None, None

def split_transaction_row(row_text):
   
    # Join multiple lines into a single line
    clean_text = row_text.replace('\n', ' ').replace('|', '')
    clean_text = re.sub(r'\s+', ' ', clean_text).strip()

    pattern_7= (r'^.*?'
        r"(\d{2}\s+[A-Za-z]{3}\s+\d{4}\s+\d{2}:\d{2}(?::\d{2})?)\s*"
                r":?\s*\d+?\s*"
                    r"(\d{2}\s+[A-Za-z]{3}\s+\d{4})\s+"
                    r"(.*?)\s+"
                    r"(--|[\-\d,.]+)\s+"
                    r"(--|[\-\d,.]+)\s+"
                    r"([\+\-]?\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s+"
                    r"(.*?)\s*"
                    r"(.+)$"                                                               # Transaction Reference
                )
    
    m10 = re.match(pattern_7, clean_text)
    if m10:
        #print(clean_text)
        return [m10.group(1), m10.group(2), m10.group(3), m10.group(4), m10.group(5), m10.group(6), m10.group(7),m10.group(8)]
    
    pattern_8 = (
                r"(\d{2}\s+[A-Za-z]{3}\s+\d{4}\s+\d{2}:\d{2}(?::\d{2})?)\s*"
                r"(.+?)\s+"
                r"(\d{2}\s+[A-Za-z]{3}\s+\d{4})\s+"
                r"(--|[\-\d,.]+)\s+"
                r"(--|[\-\d,.]+)\s+"
                r"([\+\-]?\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s+"
                r"(.*?)\s+"
                r"(.+)$"        
            )
    m11 = re.match(pattern_8, clean_text)
    if m11:
        #print("--0",clean_text)
        return [m11.group(1), m11.group(3), m11.group(2), m11.group(4), m11.group(5), m11.group(6), m11.group(7),m11.group(8)]
    
    # Regex pattern to match the structure
    pattern = (
        r"\s*"
        r"(?:"
        r"((?:\d{2} \w{3} \d{4})|"  # 02 May 2025 style
        r"(?:\d{4} \w{3} \d{2} \d{2}:(?:\d{2})?(?::\d{2})?:?))\s*"  # 2025 May 02 06: or 19:20: or 18:58:
        r")?"
        r"(?:\d{1,3}\s+)?"   # Optional index
        r"(\d{2} \w{3} \d{4})\s+"  # Value Date
        r"(.+?)\s+"
        r"([\+\-]?\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s+"
        r"(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s+"
        r"([a-zA-Z\-]+)\s+"
        r"([\w:\s]+)"  # Reference allows letters, numbers, colons, line breaks, spaces
    )

    match = re.match(pattern, clean_text)
    if match:
        print("--1", clean_text)
        return list(match.groups())
        
    fallback = (
            r"^\s*"
            r"((?:\d{4} \w{3} \d{2} \d{2}:"         # Transaction Date
            r"(?:\d{2})?(?::\d{2})?:?))\s+"
            r"(.+?)\s+"
            r"(\d{2} \w{3} \d{4})\s+"
            r"([\+\-]?\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s+"
            r"(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s+"
            r"([a-zA-Z\-]+)\s+"
            r"([\w\d]+)\s+"
            r"(\d+)\s+point$"
        )
    m2 = re.match(fallback, clean_text)
    if m2:
        print("--2", clean_text)
        txn_date, desc, value_date, amount, balance, channel, ref, idx = m2.groups()
        return [txn_date, value_date, desc, amount, balance, channel, ref]

    second_fallback = (
            r"^\s*"
            r"((?:\d{4} \w{3} \d{2} \d{2}:"           # Transaction Date
            r"(?:\d{2})?(?::\d{2})?:?))\s+"
            r"([\w\d]+)\s+"                            # Reference
            r"(\d{2} \w{3} \d{4})\s+"
            r"(.+?)\s+"
            r"point\s+"
            r"([\+\-]?\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s+"
            r"(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s+"
            r"([a-zA-Z\-]+)\s+"
            r"(\d+\s+\d+)$"                            # trailing junk
        )
    m3 = re.match(second_fallback, clean_text)
    if m3:
        print("--3", clean_text)
        txn_date, ref, value_date, desc, amount, balance, channel, junk = m3.groups()
        return [txn_date, value_date, desc, amount, balance, channel, ref]
    # New fallback 3: Description first
    pattern_1 = (
        r"^(.+?)\s+"  # Description
        r"(\d{4} \w{3} \d{2} \d{2}:\d{2}:\d{2})\s+"  # Transaction DateTime
        r"(\d{2} \w{3} \d{4})\s+"  # Value Date
        r"([\+\-]?\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s+"  # Amount
        r"(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s+"  # Balance
        r"([a-zA-Z\-]+)\s+"  # Channel
        r"(\d{15,})"  # Reference
    )
    m4 = re.match(pattern_1, clean_text)
    if m4:
        print("--4", clean_text)
        desc, txn_date, value_date, amount, balance, channel, ref = m4.groups()
        return [txn_date, value_date, desc, amount, balance, channel, ref]

    # New fallback 4: DateTime first
    pattern_2 = (
        r"^(\d{4} \w{3} \d{2} \d{2}:\d{2}:?)\s+"  # Transaction DateTime
        r"(.+?)\s+"  # Description
        r"(\d{2} \w{3} \d{4})\s+"  # Value Date
        r"([\+\-]?\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s+"  # Amount
        r"(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s+"  # Balance
        r"([a-zA-Z\-]+)\s+"  # Channel
        r"(\d{15,})"  # Reference
    )
    m5 = re.match(pattern_2, clean_text)
    if m5:
        print("--5", clean_text)
        txn_date, desc, value_date, amount, balance, channel, ref = m5.groups()
        return [txn_date, value_date, desc, amount, balance, channel, ref]
        
    pattern_3 = (
        r"^(\d{4} \w{3} \d{2} \d{2}:\d{2}(?::\d{2})?):?\s+"  # Allow no seconds, optional final :
        r"(\d{2} \w{3} \d{4})\s+"
        r"(.+?)\s+"
        r"([+\-]?\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s+"
        r"(--|[\d,.]*)\s+"
        r"([a-zA-Z\-]+)\s+"
        r"([A-Z0-9]+)"
    )
    m6 = re.match(pattern_3, clean_text)
    if m6:
        print("--6", clean_text)
        txn_date, value_date, desc, amount, balance, channel, ref = m6.groups()
        balance = None if balance.strip() == '--' else balance
        return [txn_date, value_date, desc, amount, balance, channel, ref]

    # Fallback for index + value date only
    pattern_4 = (
        r"^\s*(\d{1,3})\s+"
        r"(\d{2} \w{3} \d{4})\s+"
        r"(.+?)\s+"
        r"([-\+]?\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s+"
        r"(--|[\d,\.]*)\s+"
        r"([a-zA-Z\-]+)\s+"
        r"([A-Z0-9]+)"
    )
    m7 = re.match(pattern_4, clean_text)
    if m7:
        print("--7", clean_text)
        idx, value_date, desc, amount, balance, channel, ref = m7.groups()
        txn_date = value_date
        balance = None if balance.strip() == '--' else balance
        return [txn_date, value_date, desc, amount, balance, channel, ref]
    

    pattern_5= (r"(.*?)\s+"
            r"(\d{2}\s+[A-Za-z]{3}\s+\d{4}\s+\d{2}:\d{2}:\d{2})\s+"
            r"(\d{2}\s+[A-Za-z]{3}\s+\d{4})\s+"
            r"(--|[\-\d,.]+)\s+"
            r"(--|[\-\d,.]+)\s+"
            r"([\+\-]?\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s+"
            r"(.*?)\s*"
            r"(.+)$"
          )
    
    m8 = re.match(pattern_5, clean_text)
    if m8:
        #print(clean_text)
        return [m8.group(2), m8.group(3), m8.group(1), m8.group(4), m8.group(5), m8.group(6), m8.group(7),m8.group(8)]

    pattern_6= (r"(\d+)\s+"
            r"(\d{2}\s+[A-Za-z]{3}\s+\d{4}\s+\d{2}:\d{2}:\d{2})\s+"
            r"(\d{2}\s+[A-Za-z]{3}\s+\d{4})\s+"
            r"(.+?)\s+"
            r"([\-\d,.]+)\s+"
            r"([\-\d,.]+)\s+"
            r"([\+\-]?\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s+"
            r"(.*?)\s+"
            r"(.+)$"
          )
    m9 = re.match(pattern_6, clean_text)
    if m9:
        #print("--8", clean_text)
        return [m9.group(2), m9.group(3), m9.group(4), m9.group(5), m9.group(6), m9.group(7), m9.group(8),m9.group(1)]


def align_and_split_table(table, header):
    """
    Align rows to the header length and split misaligned rows into proper columns.

    Args:
        table: The extracted table (list of rows).
        header_length: The expected number of columns.

    Returns:
        list: A corrected table with rows aligned to the header length.
    """
    header_length = len(header)
    aligned_table = []
    
    for row in table:
        
        row = remove_blank_first2rows(row)
        split_row = None
        if sum(1 for cell in row if cell)==0 :
            continue
        else:
            row = [row_x for row_x in row if row_x is not None]
        
        if is_single_cell_row(row):
            # Split the single cell into columns
            try:
                split_row=split_transaction_row(row[0])

            except:
                pass
            if split_row is None:
                #print("This row can not be split:", row)
                continue

            aligned_table.append(split_row)               
        else:
            # Trim or pad rows to match the header length
            aligned_table.append(row[:header_length] + [''] * (header_length - len(row)))
            
    return aligned_table



def is_single_cell_row(row):
    non_empty_cells = sum(1 for cell in row if cell)  # Count non-empty cells
    return non_empty_cells == 1


def remove_blank_first2rows(row):
    # Safely remove up to the first 2 blank elements
    if row and len(row) >= 2:
        if not row[0] and not row[1]:
            row = row[2:]
        elif not row[0]:
            row = row[1:]

    # Remove trailing blanks (None, empty strings)
    while row and (row[-1] is None or row[-1] == ''):
        row = row[:-1]

    return row
