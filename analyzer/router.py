import pdfplumber
from analyzer.registry import ProcessorRegistry
import pandas as pd
from datetime import datetime
import json, time, os
import logging
import re

logging.getLogger('pdfminer').setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


NAME_PATTERN = re.compile(
    r"(?i)\b(?:account name|acc name|customer name|cust\. name|acct\. name|acct name|Account Summary|name|Hello)"
    r"\s*[:\-]?\s*([A-Z0-9][\'A-Z0-9\s&.\-]+?)(?="
    r"\s*(?:total|start|available|current|balance|currency|account|debit|credit|address|opening)\b|$|,\s*)"
)

ACCOUNT_PATTERN = re.compile(
    r"(?i)(?:acc|account|acct|IBAN)?\s*(?:number|no\.?|#)?\s*[:\-]?\s*"
    r"(\d{10,14}|\d{3}XXXX\d{3}|\d{3}\s*\d{3}\s*\d{4})"
)

CURRENT_TIME = datetime.now()
# Helpers
def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text)

    # remove noisy patterns
    text = re.sub(r"\s*TOTAL WITHDRAWALS\s*[\+\-\d,.]+", "", text, flags=re.I)
    text = re.sub(r"Account Number Address", "", text, flags=re.I)
    text = re.sub(r"\d{1,2}[-/]\d{1,2}[-/]\d{2,4}", "", text)

    return text.strip()


def extract_name_and_number(first_text: str):
    name, number = None, None

    clean_text = normalize_text(first_text)

    # --- Name extraction priority rules
    if "CUSTOMER STATEMENT" in clean_text:
        match = re.search(r"CUSTOMER STATEMENT\s*(.*?)\s*(?=Trans\.|$)", clean_text)
    elif "NGN Type:" in clean_text:
        match = re.search(r"Type:\s*[A-Z]{3}\s*(.*?)\s+(?=\d+)", clean_text)
    else:
        match = NAME_PATTERN.search(clean_text)

    if match:
        name = match.group(1)
        name = (
            name.split("Opening")[0]
            .split("Business Name")[-1]
            .replace("Account Number", "")
            .replace("Wallet", "")
            .strip()
        )
        if name.lower() in {"account number", ""}:
            name = None

    # --- Account number
    acc_match = ACCOUNT_PATTERN.search(first_text)
    if acc_match:
        number = acc_match.group(1).replace(" ", "")

    return name, number


def log_unknown_layout(file_name, headers, sample_rows):
    log_dir = "logs/unrecognized_layouts"
    os.makedirs(log_dir, exist_ok=True)

    log_path = os.path.join(
        log_dir,
        f"{os.path.basename(file_name)}_{CURRENT_TIME.strftime('%Y%m%d%H%M%S')}.json"
    )

    data = {
        "file": str(file_name),
        "headers": headers,
        "sample_rows": sample_rows,
    }

    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    logger.warning(f"Unrecognized layout logged: {log_path}")

def save_meta(file_name: str, processor: str, pages_processed: int, time_taken: float):
    os.makedirs("logs/extraction_meta", exist_ok=True)

    log_file = os.path.join(
        "logs/extraction_meta",
        f"{os.path.basename(file_name)}.json"
    )

    metadata = {
        "file_name": str(file_name),
        "processor": processor,
        "pages_processed": pages_processed,
        "time_taken_sec": round(time_taken, 2),
        "logged_at": CURRENT_TIME.isoformat(),
    }

    with open(log_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

# Main Function
def extract_tables_from_pdf(file_path):
    start_time = time.time()

    try:
        with pdfplumber.open(file_path) as pdf:
            first_text = pdf.pages[0].extract_text() or ""
            last_text = pdf.pages[-1].extract_text() or ""

            # Extract metadata
            name, number = extract_name_and_number(first_text)

            selected_processor = None
            df = None

            # --- Try processors
            for Processor in ProcessorRegistry.get_processors():
                try:
                    proc = Processor(pdf)

                    if proc.detect(first_text, last_text):
                        selected_processor = proc
                        logger.info(f"Processor in Use: {proc.name}")
                        print(f" Processor in Use: {proc.name}")

                        df = proc.extract()
                        print("it worked")

                        if df is not None and not df.empty:
                            break

                except Exception as e:
                    logger.exception(f"Processor {Processor.__name__} failed")
                    print(f"Processor {Processor.__name__} failed")

            # --- If processor succeeded
            if df is not None and not df.empty:
                
                save_meta(
                    file_name=file_path,
                    processor=selected_processor.name,
                    pages_processed=len(pdf.pages),
                    time_taken=time.time() - start_time,
                )
                
                return df, name, number

            # --- Fallback: log unknown layout
            try:
                tables = pdf.pages[0].extract_tables()
                if tables:
                    table = tables[0]
                    headers = table[0]
                    sample_rows = table[1:5]
                else:
                    headers, sample_rows = [], []

                log_unknown_layout(file_path, headers, sample_rows)

            except Exception:
                logger.exception("Failed during fallback extraction")

            return pd.DataFrame(), name, number

    except Exception:
        logger.exception("Critical failure during PDF extraction")
        return pd.DataFrame(), None, None