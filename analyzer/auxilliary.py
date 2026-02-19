import pandas as pd
import numpy as np
import re
import logging
logging.getLogger('pdfminer').setLevel(logging.ERROR)
import warnings

def transaction_data(df):

    # Standardize column names
    df = df.copy()
    df.columns = df.columns.str.lower()

    # Define column name patterns
    column_patterns = {
        'date': r'^value\s*\w*|\w*\s*date\s*\w*',
        'remark': r'^remark\s*\w*|\w*\s*detail\s*\w*|\s*\w*narration|^descrip\s*\w*',
        'balance': r'\w*balance\w*',
        'transaction_value': r'debit\/|amount'
    }

    # Identify columns dynamically
    identified_columns = {key: next((col for col in df.columns if re.search(pattern, col)), None) for key, pattern in column_patterns.items()}
    
    last_remark = str(df[identified_columns['remark']].iloc[-1])
    last_date   = str(df[identified_columns['date']].iloc[-1])
    
    if "Closing Balance" in last_remark or "Closing Balance" in last_date:
        df = df.iloc[:-1]
    
    first_remark = str(df[identified_columns['remark']].iloc[0])
    first_date   = str(df[identified_columns['date']].iloc[0])
    
    if "Opening Balance" in first_remark or "Opening Balance" in first_date:
        df = df.iloc[1:]

    # Clean and prepare data
    new_df = pd.DataFrame()
    new_df['date'] = clean_and_parse_dates(df[identified_columns['date']])

    # Assign narration or category
    new_df['narration'] = df[identified_columns['remark']]
    if identified_columns['balance']:
        new_df['balance'] = clean_numeric_column(df[identified_columns['balance']])

    # Handle missing transaction value column
    if not identified_columns['transaction_value']:
        
        credit_col = next((col for col in df.columns if re.search(r'^credit|^deposit|^\w*\s*in$|lodgement|money in', col)), None)
        debit_col = next((col for col in df.columns if re.search(r'^debit|^withdrawal|^\w*\s*out|money out', col)), None)

        if credit_col and debit_col:

            df[debit_col] = clean_numeric_column(df[debit_col])
            df[credit_col] = clean_numeric_column(df[credit_col])

            df[[debit_col, credit_col]] = df.apply(lambda row:pd.Series(process_text(row[debit_col], row[credit_col])),axis=1)
            amount = np.where(abs(df[debit_col]) > 0, df[debit_col], df[credit_col])
            
            type_ = np.where(abs(df[debit_col]) > 0, 'debit', 'credit')

    else:

        amount = clean_numeric_column(df[identified_columns['transaction_value']])
        type_ = np.where(amount < 0, 'debit', 'credit')

    new_df['amount'] = amount
    new_df['type'] = type_

    nan_df = new_df[(new_df['date'].isna())].copy()
    new_df = new_df.dropna(subset=['date']).copy()  # Ensure date is not NaN
    
    return new_df, nan_df

def clean_and_parse_dates(series:pd.Series) -> pd.Series:
 
    # Normalize whitespace and broken formatting
    s = (
        series
        .astype(str)
        .str.replace(r'[\r\n\t]+', '', regex=True)
        .str.replace(r'\s+', ' ', regex=True)
        .str.replace(r'\s*-\s*', '-', regex=True)
        .str.replace(r'\s*:\s*', ':', regex=True)
        .str.replace(r'T\s+', 'T', regex=True)
        .str.strip()
    )

    # Merge split day+month + year rows (e.g., "01 SEP" + "2024")
    prev = s.shift(1).fillna('')
    mask_merge = s.str.fullmatch(r'\d{4}') & prev.str.fullmatch(r'\d{1,2}\s+[A-Za-z]{3}')
    s = s.where(~mask_merge, prev + ' ' + s)

    # Extract date-like text only (ignore timestamps)
    s = s.str.extract(r'([\d]{1,4}[-/][A-Za-z\d]{1,3}[-/][A-Za-z\d]{1,4})', expand=False).fillna(s)

    # Define candidate formats
    candidate_formats = [
        "%Y-%m-%d",     # 2024-09-01
        "%m-%d-%Y",     # 09-01-2024
        "%d-%m-%Y",     # 01-09-2024
        "%d/%m/%Y",     # 01/09/2024
        "%m/%d/%Y",     # 09/01/2024
        "%d-%b-%Y",     # 01-Sep-2024
        "%d %b %Y",     # 01 Sep 2024
        "%d-%b-%y",     # 01-Sep-24
    ]

    # Attempt formats in order
    date_col = pd.Series(pd.NaT, index=s.index)
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        for fmt in candidate_formats:
            parsed = pd.to_datetime(s, format=fmt, errors="coerce")
            if parsed.notna().mean() >=0.8:
                date_col = parsed
                break

    # Fallback — ISO, text-based, or ambiguous formats
    if date_col.isna().all():
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            date_col = pd.to_datetime(s, errors="coerce", dayfirst=True)

    return date_col.dt.tz_localize(None)


# Helper function to clean numeric columns
def clean_numeric_column(series):

    # Convert to string and drop NaN safely
    s = series.fillna(0).astype(str)

    # STEP 1: Apply the date-fragment cleaning ONLY IF the text contains a comma
    mask = s.str.contains(",", na=False)

    s.loc[mask] = (
        s.loc[mask]
        # Remove leading date fragments like 0317, 03-17, 03/17, 03172024, etc.
        .str.replace(r'^\s*\d{2}[-/]?\d{2}(?:[-/]?\d{2,4})?\s*', '', regex=True)
        # Remove leading stray numbers e.g. "0317 1,723,000.00"
        .str.replace(r'^\s*\d+\s+', '', regex=True)
        .str.strip()
    )

    # STEP 2: General cleanup for all values
    s = (
        s.str.replace(r"\s+", "", regex=True)
         .str.replace(",", "")
         .str.replace("----", "")
         .str.replace(r"\s*Cr$", "", regex=True)
         .str.replace(r"NGN\s*", "",regex=True)
         .str.replace("+", "")
         .str.strip()
    )

    # STEP 3: Convert to numeric
    return pd.to_numeric(s, errors="coerce")

def process_text(x1, x2):
    # Ignore NaN values
    if pd.notna(x2) and isinstance(x2, float) and x2 != 0:
        return 0, x2

    if pd.notna(x1) and isinstance(x1, float) and x1 != 0:
        return x1, 0

    if '\n' in str(x1):
        return x1.split("\n")[-1], 0

    if '\n' in str(x2):
        return 0, x2.split("\n")[-1]

    return x1, x2  # Return as-is

# Regular Expression to extract sender and receiver
def extract_sender_receiver(narration: str):

    if not narration or not isinstance(narration, str):
        return None, None

    text = re.sub(r'\s+', ' ', narration.strip(), flags=re.IGNORECASE)
    text = text.replace("\n", " ").replace("GTBank/","").replace("/NIP Transfer","")

    # Define flexible regex patterns
    patterns = [r'\bTrffrm[:\s]+(.+?)\s+TO[:\s]+([A-Za-z0-9\s]+)(?:Ref:|\b|-|$)',
        r'\bTrf IFO[:\s]+(.+?)\s+For[:\s]+([A-Za-z0-9\s]+)(?:\b|-|$)',
        r'\bTrf IFO[:\s]+(.+?)\s+FRM[:\s]+([A-Za-z0-9\s]+)(?:\b|-|$)',
        r'\bTrfBy[:\s]*(.+?)\s+IFO[:\s]+([A-Za-z0-9\s]+)(?:\b|-|$)',
        r'\bFRM[:\s]+(.+?)\s+TO[:\s]+([A-Za-z0-9\s]+)(?:\b|-|$)',
        r"\bFrom[:\s]+([A-Za-z0-9\s']+)\s+To[:\s]+([A-Za-z0-9\s']+)(?:\b|-|$)",
        r"To[:\s]+([-A-Za-z0-9\s']+)\s+From[:\s]+([-A-Za-z0-9\s']+)(?:\b|-|$)",
        r'\bFRM\s+([A-Za-z0-9\s]+)\s+TO\s+([A-Za-z0-9\s]+)'
    ]
    
    for index, pattern in enumerate(patterns):
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            # Get first two non-empty groups
            groups = [g.strip(" -:") for g in match.groups() if g]
            if index!=2:
                if len(groups) >= 2:
                    return (groups[0].split('|')[-1].split('/')[-1].strip().title(), 
                groups[1].split('|')[-1].split('./')[0].strip().title())
                elif len(groups) == 1:
                    return groups[0].split('|')[-1].split('/')[-1].strip().title(), None
                else:
                    None, None
            else:
                if len(groups) >=2:
                    return (groups[1].split('|')[-1].split('./')[0].strip().title(), 
                            groups[0].split('|')[-1].split('/')[-1].strip().title())
    
    # Fallback heuristic extraction
    if "to " in text.lower() and "from " in text.lower():
        parts = re.split(r'\bto\b', text, flags=re.IGNORECASE)
        sender_part = re.split(r'\bfrom\b', parts[0], flags=re.IGNORECASE)[-1].strip(" -:").split('|')[-1].split('/')[-1].strip()
        receiver_part = parts[1].strip(" -:").split('|')[-1].split('./')[0].strip() if len(parts) > 1 else None
        if sender_part and receiver_part:
            return sender_part.title(), receiver_part.title()
    elif "to " in text.lower():
        pattern = r'To[:\s]+([A-Za-z0-9\s\-]+)\s+(?:\b|-|$)'
        match = re.search(pattern, text, flags=re.IGNORECASE)
 
        if match:
            receiver_part = match.group(1)
        else:
            parts = re.split(r'\bto\b', text, flags=re.IGNORECASE)
            receiver_part = parts[-1].strip(" -:").split('|')[-1].split('./')[0].strip() if len(parts) > 1 else None
            receiver_part = receiver_part.split('/')[-1].strip() if receiver_part else None
        if receiver_part:
            return None, receiver_part.title()
    elif "from " in text.lower():
        parts = re.split(r'\bfrom\b', text, flags=re.IGNORECASE)

        if len(parts)>1:
            return parts[1].split('|')[-1].split('/')[-1].strip().title(), None
            
    elif "by " in text.lower():
        parts = re.split(r'\bby\b', text, flags=re.IGNORECASE)

        if len(parts)>1:
            return parts[1].split('|')[-1].split('/')[-1].strip().title(), None
    return None, None
    





