import pandas as pd
import numpy as np
import json
from dateutil.relativedelta import relativedelta
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Type, Generic, TypeVar, Union,Tuple
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
        # remove redundancies - Opay
        self.df = self.df[~self.df['narration'].str.contains('OWealth Withdrawal|Spend & Save Deposit', case=False, na=False)]
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

            transaction_data_ = (
                    self.data.get('transactions') or
                    self.data.get('data') or
                    self.data.get('records', [])
            )

            if isinstance(transaction_data_, str):
                transaction_data_ = json.loads(transaction_data_)

            return pd.DataFrame(transaction_data_)

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
            ('reversal', r'revers|rvsl|REV-|RETURNED|refund'),
            ('VAS', r'Startimes|gotv|dstv|electricity|cable|airtime|\bmtn\b|airtel|\bglo\b|9mobile|VTU|Voucher|Internet bundle|Recharge card|recharge|Night plan|Data'),
            ('salary',r'monthly pay|pay roll|salary|gross pay|net pay|income|payroll'),
            ('loan_repayment', r'loan|remita|payday|pdl|repayment|EMI payment|AutoDebit|Auto Debit'),
            ('loan_credit', r'loan|Credit facility|Disbursed amount|Bank facility|disbursement|FairMoney credit|Salary advance|Mortgage credit|Aella Credit deposit|QuickCheck funds'),
            ('bonus/allowance', r'13th month salary|allowance|bonus|/[A-Z]{2}\s+A$'),
            ('betting', r'\bbet\b|betting|MSport|1Xbet|PariPesa'),
            ('turnover', r'Turnover'),
            ('drift', r'Contribution'),
            ('travelling', r'Embassy|flight|VISA|VFS Global|TLS Contact|IRCC|USCIS|Express Entry|Airfare|One-way ticket|IELTS|TOEFL|WES fee|CAS deposit|SEVIS fee|Proof of Funds|GIC Payment|Form A'),
            ('transfer', r'From|\*\*\*|Transfer|Cash|Received|Send|Sent|INWARD|OUTWARD|TRSF|HYD|trf|NIBSS|POS|RTGS|NEFT|IMPS|payment|merchant settlement|GTWORLD|NIP|HBR')
                    ]
    
    EXCLUSIONS = r'charge|commission|Electronic Money|levy'

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
                'loan_credit', 'betting', 'travelling', 'VAS'
            }:
                mask &= ~narration.str.contains(
                    self.EXCLUSIONS, regex=True, na=False,case=False
                )

            # Category-specific rules
            if category == 'loan_repayment':
                mask &= (self.df['type'] == 'debit') & (self.df['amount'] > 100)

            if category == 'salary':
                mask &= (self.df['type'] == 'credit') & (self.df['amount'] > 30_000)
            if category == 'salary':
                mask &= (self.df['type'] == 'debit')
                category = 'salary payment'
            elif category == 'loan_credit':
                mask &= (self.df['type'] == 'credit')
                category = 'loan'

            self.df.loc[mask, 'category'] = category

        # Add sender/receiver columns
        self.df[['sender', 'receiver']] = self.df['narration'].apply(lambda x: pd.Series(extract_sender_receiver(x)))

            
        return self.df


class InputLoader:
    """Handles all input normalization and loading."""

    @staticmethod
    def _decode_json(value: Any, max_depth: int = 5) -> dict:
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
    def load(cls, payload: Any) -> Tuple[Union[dict, pd.DataFrame], str | None, str | None]:
        """
        Returns:
            (data, name, account_number)
        """

        # 1. Dict input
        if isinstance(payload, dict):
            if "bankStatement" in payload:
                data = cls._decode_json(payload["bankStatement"])
                return {"bankStatement": data}, None, None

            return cls._decode_json(payload), None, None

        # 2. File path (str or Path)
        if isinstance(payload, (str, Path)):
            path = Path(payload)

            if not path.exists():
                raise FileNotFoundError(f"Invalid path: {path}")

            suffix = path.suffix.lower()

            if suffix in (".json", ".txt"):
                content = path.read_text(encoding="utf-8")
                return cls._decode_json(content), None, None

            elif suffix == ".pdf":
                # Delegate to your extractor
                return extract_tables_from_pdf(path)

        # 3. Raw JSON string
        if isinstance(payload, str):
            return cls._decode_json(payload), None, None

        raise ValueError("Unsupported input type")


class ProcessorFactory:
    """Responsible ONLY for selecting processors."""

    _processors: Dict[StatementType, Type[BaseStatementProcessor]] = {
        StatementType.MBS: MBSProcessor,
        StatementType.MONO: MonoProcessor,
        StatementType.PDF: PDFProcessor,
        StatementType.GENERIC: GenericProcessor
    }

    @classmethod
    def register_processor(cls, statement_type, processor_class):
        cls._processors[statement_type] = processor_class

    @classmethod
    def create_processor(
        cls,
        data: Any,
        processor_type: Optional[StatementType] = None,
        **kwargs
    ) -> BaseStatementProcessor:

        # Explicit override
        if processor_type:
            return cls._processors[processor_type](data, **kwargs)
        
        # Auto-detection
        if isinstance(data, dict):
            if 'Details' in data:
                return cls._processors[StatementType.MBS](data)

            if 'result' in data:
                parsed = json.loads(data['result'])
                return cls._processors[StatementType.MBS](parsed)

            if 'data' in data:
                return cls._processors[StatementType.MONO](data)

        if isinstance(data, pd.DataFrame):
            return cls._processors[StatementType.PDF](data)

        return cls._processors[StatementType.GENERIC](data, **kwargs)

class DataExtractor(BaseDataTransformer):

    def __init__(self, payload):
        super().__init__()

        self.data, self.account_name, self.account_number = InputLoader.load(payload)       

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

            # Final Categorization
            df = self.categorize_narration()

            df = df.sort_values('date')

            return df, self.account_name, self.account_number

        except Exception as e:
            print(f"Error processing data: {e}")
            return pd.DataFrame()