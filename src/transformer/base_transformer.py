from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class BaseTransformer(ABC):
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
    
    @abstractmethod
    def transform(self, df, config: Dict[str, Any]):
        pass
    
    @abstractmethod
    def validate_config(self) -> bool:
        pass
    
    def get_transformer_type(self) -> str:
        return self.__class__.__name__
    
    def chain(self, other_transformer: 'BaseTransformer') -> 'ChainedTransformer':
        return ChainedTransformer([self, other_transformer])


class ChainedTransformer(BaseTransformer):
    
    def __init__(self, transformers: list):
        super().__init__()
        self.transformers = transformers
    
    def transform(self, df, config: Dict[str, Any]):
        result = df
        for transformer in self.transformers:
            result = transformer.transform(result, config)
        return result
    
    def validate_config(self) -> bool:
        return all(t.validate_config() for t in self.transformers)