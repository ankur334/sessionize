from typing import Any, Dict, Optional
from src.sink.base_sink import BaseSink
import logging


class FileSink(BaseSink):
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def write(self, df, config: Dict[str, Any]) -> None:
        path = config.get('path')
        if not path:
            raise ValueError("Path is required for FileSink")
        
        format_type = config.get('format', 'parquet')
        mode = config.get('mode', 'overwrite')
        partition_by = config.get('partitionBy', [])
        options = config.get('options', {})
        
        self.logger.info(f"Writing {format_type} data to {path} with mode {mode}")
        
        writer = df.write.mode(mode).format(format_type)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        for key, value in options.items():
            writer = writer.option(key, value)
        
        writer.save(path)
        
        self.logger.info(f"Successfully wrote data to {path}")
    
    def validate_config(self) -> bool:
        return 'path' in self.config
    
    def get_output_path(self) -> Optional[str]:
        return self.config.get('path')