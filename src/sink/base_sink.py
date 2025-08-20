from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class BaseSink(ABC):
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
    
    @abstractmethod
    def write(self, df, config: Dict[str, Any]) -> None:
        pass
    
    @abstractmethod
    def validate_config(self) -> bool:
        pass
    
    def get_sink_type(self) -> str:
        return self.__class__.__name__
    
    @abstractmethod
    def get_output_path(self) -> Optional[str]:
        pass