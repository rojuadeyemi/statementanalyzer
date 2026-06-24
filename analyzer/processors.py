import re
from analyzer.processor.moniepoint import extract_transaction_monie_correct
from analyzer.processor.moniepoint_v2 import extract_transaction_moniepoint
from analyzer.processor.wema import extract_transaction_wema
from analyzer.processor.taj import extract_transaction_taj
from analyzer.processor.opay import extract_transaction_opay
from analyzer.processor.palmpay import extract_transaction_palmpay
from analyzer.processor.zenith import extract_transaction_zenith
from analyzer.processor.fidelity import extract_transaction_fidelity
from analyzer.processor.premium import extract_transaction_premium
from analyzer.processor.sterling import extract_transaction_sterling
from abc import ABC, abstractmethod
import pandas as pd

class BaseProcessor(ABC):
    """Base class for all bank statement processors."""

    name = "Base"

    def __init__(self, page_text):
        self.page_text = page_text

    @abstractmethod
    def detect(self) -> bool:
        """Return True if this processor matches the document."""
        pass

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Extract transaction data into standardized DataFrame."""
        pass

class PtrustProcessor(BaseProcessor):
    name = "Premium Trust"

    def detect(self):
        return bool(re.search(r"contactpremium@premiumtrustbank.com", self.page_text[-1]))

    def extract(self):
        return extract_transaction_premium(self.page_text)
    
class SterlingProcessor(BaseProcessor):
    name = "Sterling"

    def detect(self):
        return bool(re.search(r"www.sterling.ng", self.page_text[0]))

    def extract(self):
        return extract_transaction_sterling(self.page_text)

class TAJProcessor(BaseProcessor):
    name = "TAJ"

    def detect(self):
        return bool(re.search(r"tajconnect@tajbank.com", self.page_text[0]))

    def extract(self):
        return extract_transaction_taj(self.page_text)
        
class MoniepointProcessor(BaseProcessor):
    name = "Moniepoint"

    def detect(self):
        return bool(
            re.search(r"Business Name", self.page_text[0])
            and re.search(r"Currency NGN", self.page_text[0])
        )

    def extract(self):
        return extract_transaction_monie_correct(self.page_text)
    
class MoniepointProcessor_v2(BaseProcessor):
    name = "Moniepoint_v2"

    def detect(self):
        return bool(
            re.search(r'([A-Z][a-z]+\s*\d{2}\s*[A-Z][a-z]+\s*Page)', self.page_text[0])
        )

    def extract(self):
        return extract_transaction_moniepoint(self.page_text)
'''
class KudaProcessor(BaseProcessor):
    name = "Kuda"

    def detect(self, first_text, last_text):
        return bool(
                re.search(r"mybankStatement\s+®\s*\|\s*(\d{2}/\d{2}/\d{4})\s*\|\s*(\d+-\d+)", last_text)
        )

    def extract(self):
        return extract_transaction_kuda(self.pdf)'''

class PalmPayProcessor(BaseProcessor):
    name = "PalmPay"

    def detect(self):
        return bool(
                re.search(r"PalmPay Business Statement", self.page_text[0])
        )

    def extract(self):
        return extract_transaction_palmpay(self.page_text)
        
class ZenithProcessor(BaseProcessor):
    name = "Zenith"

    def detect(self):
        return bool(
                re.search(r"Account Number: CA", self.page_text[0])
        )

    def extract(self):
        return extract_transaction_zenith(self.page_text)

class FidelityProcessor(BaseProcessor):
    name = "Fidelity"

    def detect(self):
        return bool(
                re.search(r"fidelitybank.ng", self.page_text[0])
        )

    def extract(self):
        return extract_transaction_fidelity(self.page_text)

class OpayProcessor(BaseProcessor):
    """Processor for Opay statements."""
    name = "Opay"

    def detect(self):
        return bool(re.search(r"Note: Current Balance includes OWealth Balance|Reversal Transaction Settlement", self.page_text[0], re.IGNORECASE) or re.search(r"Pos-service@opay", self.page_text[-1], re.IGNORECASE)
                   )

    def extract(self):
        return extract_transaction_opay(self.page_text)

class MultipleProcessor(BaseProcessor):
    name = "Wema Corporate_Stanbic IBTC_FCMB Business"

    def detect(self):
        return bool(
                re.search(r"alat.ng|www.stanbicibtcbank.com|the following pie chart represents the various", self.page_text[0])
        )

    def extract(self):
        return extract_transaction_wema(self.page_text)
