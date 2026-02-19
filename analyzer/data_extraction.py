import pandas as pd
import numpy as np
import json
from dateutil.relativedelta import relativedelta
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Type, Generic, TypeVar
from enum import Enum
from analyzer.router import extract_tables_from_pdf
from analyzer.auxilliary import transaction_data,extract_sender_receiver
from pathlib import Path

T = TypeVar('T')

class StatementType(Enum):
    MBS = "mbs"
    MONO = "mono"
    PDF = "pdf"
    GENERIC = "generic"

class BaseStatementProcessor(ABC, Generic[T]):
    """
    Abstract base class for statement processors.
    Implements Template Method pattern for extensibiility.
    """

    def __init__(self, data: T):
        self.data = data
        self.df = pd.DataFrame()

    @abstractmethod
    def validate_data_format(self) -> bool:
        """Validate if data format is supported"""
        pass

    @abstractmethod
    def extract_transactions(self) -> pd.DataFrame:
        """Extract transactions from data"""

    @abstractmethod
    def normalize_fields(self) -> None:
        """Normalize field names to standard format"""
        pass

    def enrich_dates(self: pd.DataFrame) -> pd.DataFrame:
        if 'date' not in self.df.columns:
            raise Exception('date field not found in the dataframe')

        self.df['date'] = pd.to_datetime(self.df['date'], errors='coerce').dt.tz_localize(None)
        self.df['monthyear'] = self.df['date'].dt.strftime('%Y-%m')
        iso = self.df['date'].dt.isocalendar()
        self.df['weekno'] = iso.year * 100 + iso.week
    
    def apply_date_cutoff(self, months=36):
        if 'date' in self.df.columns:
            cutoff = self.df['date'].max() - relativedelta(months=months)
            self.df = self.df[self.df['date'] >= cutoff]

    def process(self) -> pd.DataFrame:
        if not self.validate_data_format():
            raise ValueError("Invalid data format")

        self.df = self.extract_transactions()
        if self.df.empty:
            return self.df

        self.normalize_fields()
        self.enrich_dates()
        self.apply_date_cutoff()

        return self.df

class MBSProcessor(BaseStatementProcessor[Dict[str, Any]]):
    """Optimized MBS Statement Processor"""

    FIELD_MAP = {
        'PTransactionDate': 'date',
        'PCredit': 'credit',
        'PDebit': 'debit',
        'PNarration': 'narration',
        'PBalance': 'balance'}
    
    def validate_data_format(self) -> bool:
        #print("Validating MBS data format", self.data)
        return isinstance(self.data, dict) and 'TicketNo' in self.data

    def extract_transactions(self) -> pd.DataFrame:
        try:
            if isinstance(self.data['Details'], str):
                parsed_data = json.loads(self.data['Details'])
                return pd.DataFrame(parsed_data).iloc[1:]
            elif isinstance(self.data['Details'], list):
                return pd.DataFrame(self.data['Details']).iloc[1:]
            else:
                raise ValueError(f"Unexpected data format for MBS data: {self.data['Details']}")
        except (json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Error parsing MBS data: {e}")

    def normalize_fields(self):
        if self.df.empty:
            return
        
        self.df = self.df.rename(columns=self.FIELD_MAP)
        numeric_cols = ['credit', 'debit', 'balance'] 
        self.df[numeric_cols] = self.df[numeric_cols].astype(float)

        self.df['amount'] = self.df['credit'].fillna(0) + self.df['debit'].fillna(0)
        self.df['type'] = np.where(self.df['credit'] > 0, 'credit', 'debit')

class PDFProcessor(BaseStatementProcessor[pd.DataFrame]):
    """Optimized PDF Statement Processor"""

    def validate_data_format(self) -> bool:
    
        return isinstance(self.data, pd.DataFrame)

    def extract_transactions(self) -> pd.DataFrame:
        try:
            if isinstance(self.data, pd.DataFrame):
                return self.data
            else:
                raise ValueError(f"Unexpected data format for PDF data: {self.data}")
        except KeyError as e:
            raise ValueError(f"Error parsing PDF data: {e}")

    def normalize_fields(self) -> None:

        self.df,nan_df = transaction_data(self.df)
        

class MonoProcessor(BaseStatementProcessor[Dict[str, Any]]):
    """Optimized Mono Statement Processor"""
    FIELD_MAP = {
            'Date': 'date',
            'Type': 'type',
            'Amount': 'amount',
            'Category': 'category',
            'Narration': 'narration',
            'Balance': 'balance'
        }
    def validate_data_format(self) -> bool:
        return isinstance(self.data, dict) and 'data' in self.data

    def extract_transactions(self) -> pd.DataFrame:
        try:
            data_list = self.data.get('data', [])
            if not data_list:
                return pd.DataFrame()
            return pd.DataFrame(data_list)
        except (json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Error parsing Mono data: {e}")

    def normalize_fields(self):
        if self.df.empty:
            return
        
        self.df = self.df.rename(columns=self.FIELD_MAP)

        self.df['amount'] = self.df['amount'] / 100
        self.df['balance'] = self.df['balance'] / 100

        # Reverse Mono to match oldest to newest chronology
        self.df = self.df.iloc[::-1].reset_index(drop=True)

class GenericProcessor(BaseStatementProcessor[Dict[str, Any]]):
    """
    Generic processor for new statement providers.
    Configurable field mapping for extensibility
    """

    def __init__(self, data: Dict[str, Any], field_mapping: Optional[Dict[str, str]] = None):
        super().__init__(data)
        self.field_mapping = field_mapping or self._get_default_mapping()

    def _get_default_mapping(self) -> Dict[str, str]:
        """ Default field mapping for generic processor - can be overriden"""
        return {
            'transaction_date': 'date',
            'description': 'narration',
            'amount': 'amount',
            'balance': 'balance',
            'transaction_type': 'type'
        }

    def validate_data_format(self) -> bool:

        return isinstance(self.data, dict) and (
                'transaction_date' in self.data or 'data' in self.data
        )

    def extract_transactions(self) -> pd.DataFrame:
        try:

            transaction_data = (
                    self.data.get('transactions') or
                    self.data.get('data') or
                    self.data.get('records', [])
            )

            if isinstance(transaction_data, str):
                transaction_data = json.loads(transaction_data)

            return pd.DataFrame(transaction_data)

        except (json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Error parsing generic data: {e}")

    def normalize_fields(self) -> None:
        if self.df.empty:
            return

        self.df = self.df.rename(columns=self.field_mapping)

        numeric_cols = ['amount', 'balance']
        for col in numeric_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')

class BaseDataTransformer:
    """Base class for common data transformation methods."""
    CATEGORY_RULES = [
            ('reversal', r'revers|rvsl|REV-|RETURNED'),
            ('airtime', r'airtime|mtn|airtel|glo|9mobile|VTU|Voucher|Data purchase|Data subscription|Internet bundle|Data renewal|Recharge card|recharge|data plan|SME data purchase|data bundle|Night plan|Unlimited data'),
            ('salary',r'monthly pay|pay roll|salary|gross pay|net pay|income|payroll|/[A-Z]{2}\s+A$'),
            ('loan_repayment', r'loan|remita|payday|pdl|repayment|Loan installment|EMI payment'),
            ('loan_credit', r'loan|Credit facility|Disbursed amount|Bank facility|disbursement|FairMoney credit|Salary advance|Mortgage credit|Aella Credit deposit|QuickCheck funds'),
            ('bonus/allowance', r'13th month salary|allowance|bonus|/[A-Z]{2}\s+A$'),
            ('betting', r'\bbet\b|betting|MSport|1Xbet|PariPesa'),
            ('turnover', r'Turnover'),
            ('drift', r'Contribution'),
            ('travelling', r'Embassy payment|flight|VISA|VFS Global|TLS Contact|IRCC|USCIS|Express Entry|Airfare|One-way ticket|IELTS|TOEFL|WES fee|CAS deposit|SEVIS fee|Proof of Funds|GIC Payment|Form A'),
            ('transfer', r'Received|Send|Sent|INWARD|OUTWARD|TRSF|HYD|trf|transfer|TRANSFER|NIBSS|POS|RTGS|NEFT|IMPS|payment|merchant settlement|GTWORLD|NIP|HBR')
                    ]
    
    EXCLUSIONS = r'charge|commission|Electronic Money|VAT|TAX DEDUCTION'

    def categorize_narration(self):
        if self.df.empty or 'narration' not in self.df.columns:
            return self.df

        self.df['category'] = 'others'
        narration = self.df['narration']

        for category, pattern in self.CATEGORY_RULES:

            # Only classify unclassified rows
            unclassified = self.df['category'] == 'others'

            mask = (
                unclassified &
                narration.str.contains(pattern, regex=True, na=False,case=False)
            )

            # Common exclusions
            if category in {
                'transfer', 'salary', 'loan_repayment',
                'loan_credit', 'betting', 'travelling', 'airtime'
            }:
                mask &= ~narration.str.contains(
                    self.EXCLUSIONS, regex=True, na=False,case=False
                )

            # Category-specific rules
            if category == 'loan_repayment':
                mask &= (self.df['type'] == 'debit') & (self.df['amount'] > 100)

            if category == 'salary':
                mask &= (self.df['type'] == 'credit') & (self.df['amount'] > 30_000)
            elif category == 'loan_credit':
                mask &= (self.df['type'] == 'credit')
                category = 'loan'

            self.df.loc[mask, 'category'] = category

        # Add sender/receiver columns
        self.df[['sender', 'receiver']] = self.df['narration'].apply(lambda x: pd.Series(extract_sender_receiver(x)))

            
        return self.df
    
class ProcessorFactory:
    """Factory class for creating statement processors."""

    _processors: Dict[StatementType, Type[BaseStatementProcessor]] = {
        StatementType.MBS: MBSProcessor,
        StatementType.MONO: MonoProcessor,
        StatementType.PDF: PDFProcessor,
        StatementType.GENERIC: GenericProcessor
    }

    @classmethod
    def register_processor(cls, statement_type: StatementType,
                           processor_class: Type[BaseStatementProcessor]) -> None:
        """Register a new statement processor class."""
        cls._processors[statement_type] = processor_class

    @staticmethod
    def _sanitize_path(value: str) -> str:
        """Fix corrupted UNC or Windows paths caused by escape sequences."""
        # Map ASCII control chars to their intended literal escape form
        ascii_fix_map = {
            '\x07': r'\\a',  # BEL (\a)
            '\x08': r'\\b',  # Backspace (\b)
            '\x0c': r'\\f',  # Form feed (\f)
            '\x0a': r'\\n',  # Newline (\n)
            '\x0d': r'\\r',  # Carriage return (\r)
            '\x09': r'\\t',  # Tab (\t)
            '\x0b': r'\\v',  # Vertical tab (\v)
        }

        for bad, fix in ascii_fix_map.items():
            value = value.replace(bad, fix)

        # Fix UNC prefix — ensure starts with double backslash
        if value.startswith("\\") and not value.startswith("\\\\"):
            value = "\\" + value

        return value

    @staticmethod
    def _decode_json(value: Any, max_depth: int = 100) -> dict:
        """Decode nested JSON safely up to a maximum depth."""

        obj = value
        for _ in range(max_depth):
            if isinstance(obj, str):
                s = obj.strip()
                if not s:
                    return {}
                try:
                    obj = json.loads(s)
                except json.JSONDecodeError:
                    break
            else:
                break
        return obj

    @classmethod
    def format_data(cls, payload: Any) -> dict | pd.DataFrame:
        """
        Standardize input from dict, Path, or string.
        Handles:
        - dicts with 'bankStatement'
        - UNC / Windows / local file paths (.pdf, .json, .txt)
        - raw JSON strings
        """

        # 1 Handle dict input
        if isinstance(payload, dict):
            if "bankStatement" in payload:
                return {"bankStatement": cls._decode_json(payload["bankStatement"])}, None, None
            
            return cls._decode_json(payload), None, None

        # 2 Handle string or Path that may represent a file
        if isinstance(payload, (str, Path)):

            # Optional: only bother if it looks like a file path
            if any(ext in payload.lower() for ext in [".json", ".pdf", ".txt"]):
                try:
                    # Sanitize UNC or escaped path
                    clean_path = Path(cls._sanitize_path(payload))

                    # Resolve the path fully
                    if clean_path.exists():
                        suffix = clean_path.suffix.lower()
                        if suffix in (".json", ".txt"):
                            return cls._decode_json(clean_path.read_text(encoding="utf-8")), None, None
                        elif suffix == ".pdf":
                            
                            return extract_tables_from_pdf(clean_path)
                except Exception as e:
                    # If path handling fails (e.g. invalid chars), treat it as a JSON string
                    print(f"[WARN] Could not open path ({e}). Treating input as JSON text.")

        # 3 Handle plain JSON string (fallback)
        if isinstance(payload, str):
            return cls._decode_json(payload), None, None

        raise ValueError(
            "Unsupported input type. Expected dict, valid file path (.pdf/.json/.txt), or JSON string."
        )

    @classmethod
    def create_processor(cls, data: Dict[str, Any],
                         processor_type: Optional[StatementType] = None,
                         **kwargs
                         ) -> BaseStatementProcessor:
        """
        Create appropriate processor based on data format or explicit type
        Auto-detection with fallback to generic processor.
        """

        if isinstance(data, dict):
            if 'Details' in data:
                return cls._processors[StatementType.MBS](data)
            elif 'data' in data:
                return cls._processors[StatementType.MONO](data)
        if isinstance(data, pd.DataFrame):
            return cls._processors[StatementType.PDF](data)

        return cls._processors[StatementType.GENERIC](data, **kwargs)

class DataExtractor(BaseDataTransformer):

    def __init__(self, data):
        super().__init__()
        
        self.data, self.account_name, self.account_number = ProcessorFactory.format_data(data) # returns raw dataframe or json dict
        
    def transform_data(self, processor_type: Optional[StatementType] = None,
                       field_mapping: Optional[Dict[str, str]] = None) -> pd.DataFrame:
        """Transform data using appropriate processor
            Enhanced with generic processor support
        """
        try:
            # Create processor using factory
            processor = ProcessorFactory.create_processor(
                self.data,
                processor_type,
                field_mapping=field_mapping
            )
            

            self.df = processor.process()


            if self.df.empty:
                return self.df

            # Final Categorization
            df = self.categorize_narration()

            return df, self.account_name, self.account_number

        except Exception as e:
            print(f"Error processing data: {e}")
            return pd.DataFrame()