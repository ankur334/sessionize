from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class BaseExtractor(ABC):
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
    
    @abstractmethod
    def extract(self, spark, config: Dict[str, Any]):
        pass
    
    @abstractmethod
    def validate_config(self) -> bool:
        pass
    
    def get_extractor_type(self) -> str:
        return self.__class__.__name__