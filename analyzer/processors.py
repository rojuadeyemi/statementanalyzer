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
from analyzer.processor.generic import extract_transaction_generic
from analyzer.processor.base import BaseProcessor


class PtrustProcessor(BaseProcessor):
    name = "Premium Trust"

    def detect(self, first_text, last_text):
        return bool(re.search(r"contactpremium@premiumtrustbank.com", last_text))

    def extract(self):
        return extract_transaction_premium(self.pdf)
    
class SterlingProcessor(BaseProcessor):
    name = "Sterling"

    def detect(self, first_text, last_text):
        return bool(re.search(r"www.sterling.ng", first_text))

    def extract(self):
        return extract_transaction_sterling(self.pdf)

class TAJProcessor(BaseProcessor):
    name = "TAJ"

    def detect(self, first_text, last_text):
        return bool(re.search(r"tajconnect@tajbank.com", first_text))

    def extract(self):
        return extract_transaction_taj(self.pdf)
        
class MoniepointProcessor(BaseProcessor):
    name = "Moniepoint"

    def detect(self, first_text, last_text):
        return bool(
            re.search(r"Business Name", first_text)
            and re.search(r"Currency NGN", first_text)
        )

    def extract(self):
        return extract_transaction_monie_correct(self.pdf)
    
class MoniepointProcessor_v2(BaseProcessor):
    name = "Moniepoint_v2"

    def detect(self, first_text, last_text):
        return bool(
            re.search(r'([A-Z][a-z]+\s*\d{2}\s*[A-Z][a-z]+\s*Page)', first_text)
        )

    def extract(self):
        return extract_transaction_moniepoint(self.pdf)
"""
class KudaProcessor(BaseProcessor):
    name = "Kuda"

    def detect(self, first_text, last_text):
        return bool(
                re.search(r"mybankStatement\s+®\s*\|\s*(\d{2}/\d{2}/\d{4})\s*\|\s*(\d+-\d+)", last_text)
        )

    def extract(self):
        return extract_transaction_kuda(self.pdf)"""

class PalmPayProcessor(BaseProcessor):
    name = "PalmPay"

    def detect(self, first_text, last_text):
        return bool(
                re.search(r"PalmPay Business Statement", first_text)
        )

    def extract(self):
        return extract_transaction_palmpay(self.pdf)
        
class ZenithProcessor(BaseProcessor):
    name = "Zenith"

    def detect(self, first_text, last_text):
        return bool(
                re.search(r"Account Number: CA", first_text)
        )

    def extract(self):
        return extract_transaction_zenith(self.pdf)

class FidelityProcessor(BaseProcessor):
    name = "Fidelity"

    def detect(self, first_text, last_text):
        return bool(
                re.search(r"fidelitybank.ng", first_text)
        )

    def extract(self):
        return extract_transaction_fidelity(self.pdf)

class OpayProcessor(BaseProcessor):
    """Processor for Opay statements."""
    name = "Opay"

    def detect(self, first_text, last_text):
        return bool(re.search(r"Note: Current Balance includes OWealth Balance|Reversal Transaction Settlement", first_text, re.IGNORECASE) or re.search(r"Pos-service@opay", last_text, re.IGNORECASE)
                   )

    def extract(self):
        return extract_transaction_opay(self.pdf)

class MultipleProcessor(BaseProcessor):
    name = "Wema Corporate_Stanbic IBTC_FCMB Business"

    def detect(self, first_text, last_text):
        return bool(
                re.search(r"alat.ng|www.stanbicibtcbank.com|the following pie chart represents the various", last_text)
        )

    def extract(self):
        return extract_transaction_wema(self.pdf)

class GenericProcessor(BaseProcessor):
    name = "MultiPurpose"

    def detect(self, first_text, last_text):
        return True

    def extract(self):
        return extract_transaction_generic(self.pdf,keywords=['balance'])
