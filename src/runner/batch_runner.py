from typing import Any, Dict, Optional
import logging
from src.runner.base_runner import BaseRunner


class BatchRunner(BaseRunner):
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.extractor = None
        self.transformer = None
        self.sink = None
    
    def initialize_spark(self) -> None:
        try:
            from pyspark.sql import SparkSession
            
            spark_config = self.get_spark_config()
            builder = SparkSession.builder
            
            if 'app_name' in spark_config:
                builder = builder.appName(spark_config['app_name'])
            else:
                builder = builder.appName("SessionizeBatchPipeline")
            
            if 'master' in spark_config:
                builder = builder.master(spark_config['master'])
            
            if 'config' in spark_config:
                for key, value in spark_config['config'].items():
                    builder = builder.config(key, value)
            
            self.spark = builder.getOrCreate()
            self.logger.info("Spark session initialized successfully")
            
        except ImportError:
            raise ImportError("PySpark is required. Install it with: pip install pyspark")
        except Exception as e:
            self.logger.error(f"Failed to initialize Spark session: {e}")
            raise
    
    def initialize_components(self) -> None:
        pipeline_config = self.get_pipeline_config()
        
        extractor_config = pipeline_config.get('extractor', {})
        self.extractor = self._load_component('extractor', extractor_config)
        
        transformer_config = pipeline_config.get('transformer', {})
        self.transformer = self._load_component('transformer', transformer_config)
        
        sink_config = pipeline_config.get('sink', {})
        self.sink = self._load_component('sink', sink_config)
    
    def _load_component(self, component_type: str, config: Dict[str, Any]):
        component_class = config.get('class')
        if not component_class:
            component_class = config.get('type', 'default').title() + component_type.title()
        
        try:
            module_path = f"src.{component_type}.{config.get('type', 'file')}_{component_type}"
            module = __import__(module_path, fromlist=[component_class])
            cls = getattr(module, component_class)
            return cls(config)
        except (ImportError, AttributeError) as e:
            self.logger.warning(f"Could not load {component_type} class {component_class}: {e}")
            return None
    
    def run(self) -> None:
        try:
            if not self.validate_pipeline_config():
                raise ValueError("Invalid pipeline configuration")
            
            self.logger.info("Starting batch pipeline")
            
            if not self.spark:
                self.initialize_spark()
            
            self.initialize_components()
            
            df = None
            if self.extractor:
                self.logger.info("Extracting data...")
                df = self.extractor.extract(self.spark, self.get_pipeline_config().get('extractor', {}))
            
            if df is not None and self.transformer:
                self.logger.info("Transforming data...")
                df = self.transformer.transform(df, self.get_pipeline_config().get('transformer', {}))
            
            if df is not None and self.sink:
                self.logger.info("Writing data to sink...")
                self.sink.write(df, self.get_pipeline_config().get('sink', {}))
            
            self.logger.info("Batch pipeline completed successfully")
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {e}")
            raise
    
    def stop(self) -> None:
        if self.spark:
            self.spark.stop()
            self.logger.info("Spark session stopped")