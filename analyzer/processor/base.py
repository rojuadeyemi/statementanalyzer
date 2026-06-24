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
