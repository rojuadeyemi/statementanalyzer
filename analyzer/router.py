import pdfplumber
from analyzer.registry import ProcessorRegistry
import pandas as pd
from datetime import datetime
import json, time, os
import logging
import re
logging.getLogger('pdfminer').setLevel(logging.ERROR)


def extract_tables_from_pdf(file_path):
    """Auto-detect bank and route to the right processor."""
    
    start_time = time.time()
    with pdfplumber.open(file_path) as pdf:
        first_text = pdf.pages[0].extract_text()
        last_text = pdf.pages[-1].extract_text()
        
        pattern1 = (r"(?i)\b(?:account name|acc name|customer name|cust. name|acct. name|acct name|Account Summary|name|Hello)\s*[:\-]?\s*([A-Z0-9][\'A-Z0-9\s&.\-]+?)(?=\s*(?:total|start|available|CURRENT|Uncleared|balance|currency|account|debit|credit|address|to|business|opening)\b|$|,\s*)")
        
        pattern2 = (r"(?i)(?:\s*acc|account|acc.|acct|Polaris Bank Limited|IBAN)\s*(?:number|no\.?|#)?\s*[:\-]?[\w\s]*\s*(\d{10,14}|\d{3}XXXX\d{3}|\d{3}\s*\d{3}\s*\d{4})")
        name = number = None

        first_text_mod = re.sub(r"\s*TOTAL WITHDRAWALS\s*[\+\-\d,.]+","",first_text.replace("\n"," "))
        first_text_mod = re.sub(r"Account Number Address","",first_text_mod)
        first_text_mod = re.sub(r"\s+\d{1,2}[-\/]\d{1,2}[-\/]\d{2,4}|\d{2,}\s*"," ",first_text_mod)
        if 'Hello' in first_text_mod and 'Here is your' in first_text_mod:
            first_text_mod = re.sub(r"Here is your\s*.*","",first_text_mod)
        first_text_account = re.sub(r"\s*Address\s*[A-Za-z\s]+"," ",first_text.replace("\n"," "))

        if 'CUSTOMER STATEMENT' in first_text_mod:
            match1 = re.search(r"CUSTOMER STATEMENT\s*(.*?)\s*(?=Trans\.|$)",first_text_mod)
        elif 'NGN Type:' in first_text_mod:
            match1 = re.search(r"Type: [A-Z]{3}\s*(.*?)\s+(?=\s+|\d+)",first_text_mod)
        else:
            match1 = re.search(pattern1,first_text_mod)
        match2 = re.search(pattern2,first_text_account)
        
        if match1:
            name = match1.group(1)
            #print(name)
            name = name.split("\nOpening")[0].split("Business Name")[-1].strip()
            if name == 'Account Number':
                name = None
        
        if match2:
            number = match2.group(1)

        # Try all registered processors
        for Processor in ProcessorRegistry.get_processors():
            try:
                proc = Processor(pdf)
                if proc.detect(first_text, last_text):
                    
                    print(f" Processor in Use: {proc.name}")
                    df = proc.extract()
                    
                    # Save metadata
                    save_meta(
                        file_name=file_path,
                        processor=proc.name,
                        pages_processed=len(pdf.pages),
                        time_taken=round(time.time() - start_time, 2))

                    return df, name, number
            except Exception as e:
                
                # if everything else fails, log and keep meta data
                tables = pdf.pages[0].extract_tables()
                table = tables[0]
                
                headers = table[0]
                sample_rows = table[1:5]
                unknown_layout(file_path,headers,sample_rows)
                
                return pd.DataFrame()

def unknown_layout(file_name, headers, sample_rows):
    """Log unknown layout for manual review."""
    log_dir = "logs/unrecognized_layouts"
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f'{os.path.basename(file_name)}_{datetime.now().strftime("%Y%m%d%H%M%S")}.json')

    data = {"file": file_name, "headers": headers, "sample_rows": sample_rows}
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f" Unrecognized layout logged: {log_path}")

def save_meta(file_name: str, processor: str, pages_processed: int, time_taken: float):
    os.makedirs("logs/extraction_meta", exist_ok=True)
    log_file = os.path.join("logs/extraction_meta", f"{os.path.basename(file_name)}_.json")
    metadata = {
        "file_name": str(file_name),
        "processor": processor,
        "pages_processed": pages_processed,
        "time_taken_sec": round(time_taken, 2),
        "logged_at": datetime.now().isoformat()
    }

    with open(log_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

