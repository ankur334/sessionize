from typing import Any, Dict
from src.extractor.base_extractor import BaseExtractor
import logging


class FileExtractor(BaseExtractor):
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def extract(self, spark, config: Dict[str, Any]):
        path = config.get('path')
        if not path:
            raise ValueError("Path is required for FileExtractor")
        
        format_type = config.get('format', 'parquet')
        options = config.get('options', {})
        
        self.logger.info(f"Extracting {format_type} data from {path}")
        
        reader = spark.read.format(format_type)
        
        for key, value in options.items():
            reader = reader.option(key, value)
        
        df = reader.load(path)
        
        self.logger.info(f"Extracted {df.count()} records")
        return df
    
    def validate_config(self) -> bool:
        return 'path' in self.config