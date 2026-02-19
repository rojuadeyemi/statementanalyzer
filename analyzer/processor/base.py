from abc import ABC, abstractmethod
import pandas as pd

class BaseProcessor(ABC):
    """Base class for all bank statement processors."""

    name = "Base"

    def __init__(self, pdf):
        self.pdf = pdf

    @abstractmethod
    def detect(self, first_text: str, last_text: str) -> bool:
        """Return True if this processor matches the document."""
        pass

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Extract transaction data into standardized DataFrame."""
        pass
