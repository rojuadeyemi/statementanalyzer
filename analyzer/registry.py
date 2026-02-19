from typing import Type, List
from analyzer.processor.base import BaseProcessor

class ProcessorRegistry:
    _processors: List[Type[BaseProcessor]] = []

    @classmethod
    def register(cls, processor_cls: Type[BaseProcessor]):
        cls._processors.append(processor_cls)

    @classmethod
    def get_processors(cls) -> List[Type[BaseProcessor]]:
        return cls._processors