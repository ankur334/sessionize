from typing import Any, Dict, Optional, Union
import logging
from src.runner.batch_runner import BatchRunner
from src.runner.streaming_runner import StreamingRunner
from src.config.config_manager import ConfigManager
from src.utils.logger import setup_logger


class PipelineController:
    
    def __init__(self, config_path: Optional[str] = None, config: Optional[Dict[str, Any]] = None):
        if config_path:
            self.config = ConfigManager.load_config(config_path)
        elif config:
            self.config = config
        else:
            raise ValueError("Either config_path or config must be provided")
        
        self.logger = setup_logger(self.__class__.__name__)
        self.runner = None
        self.pipeline_type = self._determine_pipeline_type()
    
    def _determine_pipeline_type(self) -> str:
        pipeline_config = self.config.get('pipeline', {})
        return pipeline_config.get('type', 'batch')
    
    def initialize(self) -> None:
        self.logger.info(f"Initializing {self.pipeline_type} pipeline controller")
        
        if self.pipeline_type == 'batch':
            self.runner = BatchRunner(self.config)
        elif self.pipeline_type == 'streaming':
            self.runner = StreamingRunner(self.config)
        else:
            raise ValueError(f"Unsupported pipeline type: {self.pipeline_type}")
    
    def execute(self) -> None:
        if not self.runner:
            self.initialize()
        
        try:
            self.logger.info(f"Executing {self.pipeline_type} pipeline")
            self.runner.run()
            self.logger.info("Pipeline execution completed successfully")
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {e}")
            raise
        finally:
            self.cleanup()
    
    def cleanup(self) -> None:
        if self.runner:
            self.logger.info("Cleaning up pipeline resources")
            self.runner.stop()
    
    def validate(self) -> bool:
        self.logger.info("Validating pipeline configuration")
        
        required_keys = [
            'pipeline.extractor',
            'pipeline.transformer', 
            'pipeline.sink'
        ]
        
        if not ConfigManager.validate_config(self.config, required_keys):
            self.logger.error("Pipeline configuration validation failed")
            return False
        
        self.logger.info("Pipeline configuration is valid")
        return True
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        return {
            'type': self.pipeline_type,
            'config_valid': self.validate(),
            'runner_initialized': self.runner is not None,
            'spark_session_active': self.runner.spark is not None if self.runner else False
        }
    
    @classmethod
    def create_from_config(cls, config_path: str) -> 'PipelineController':
        return cls(config_path=config_path)
    
    @classmethod
    def create_from_dict(cls, config: Dict[str, Any]) -> 'PipelineController':
        return cls(config=config)