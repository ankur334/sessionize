from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import logging


class BaseRunner(ABC):
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.spark = None
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def initialize_spark(self) -> None:
        pass
    
    @abstractmethod
    def run(self) -> None:
        pass
    
    @abstractmethod
    def stop(self) -> None:
        pass
    
    def validate_pipeline_config(self) -> bool:
        required_keys = ['pipeline.extractor', 'pipeline.transformer', 'pipeline.sink']
        for key in required_keys:
            keys = key.split('.')
            current = self.config
            for k in keys:
                if k not in current:
                    self.logger.error(f"Missing required configuration: {key}")
                    return False
                current = current[k]
        return True
    
    def get_spark_config(self) -> Dict[str, Any]:
        return self.config.get('spark', {})
    
    def get_pipeline_config(self) -> Dict[str, Any]:
        return self.config.get('pipeline', {})