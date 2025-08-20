from typing import Any, Dict, Optional
import logging
from src.runner.base_runner import BaseRunner


class StreamingRunner(BaseRunner):
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.extractor = None
        self.transformer = None
        self.sink = None
        self.stream_query = None
    
    def initialize_spark(self) -> None:
        try:
            from pyspark.sql import SparkSession
            
            spark_config = self.get_spark_config()
            builder = SparkSession.builder
            
            if 'app_name' in spark_config:
                builder = builder.appName(spark_config['app_name'])
            else:
                builder = builder.appName("SessionizeStreamingPipeline")
            
            if 'master' in spark_config:
                builder = builder.master(spark_config['master'])
            
            if 'config' in spark_config:
                for key, value in spark_config['config'].items():
                    builder = builder.config(key, value)
            
            builder = builder.config("spark.sql.streaming.schemaInference", "true")
            
            self.spark = builder.getOrCreate()
            self.spark.sparkContext.setLogLevel("WARN")
            self.logger.info("Spark streaming session initialized successfully")
            
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
            module_path = f"src.{component_type}.{config.get('type', 'streaming')}_{component_type}"
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
            
            self.logger.info("Starting streaming pipeline")
            
            if not self.spark:
                self.initialize_spark()
            
            self.initialize_components()
            
            stream_df = None
            if self.extractor:
                self.logger.info("Setting up streaming data extraction...")
                stream_df = self.extractor.extract(self.spark, self.get_pipeline_config().get('extractor', {}))
            
            if stream_df is not None and self.transformer:
                self.logger.info("Setting up streaming transformations...")
                stream_df = self.transformer.transform(stream_df, self.get_pipeline_config().get('transformer', {}))
            
            if stream_df is not None and self.sink:
                self.logger.info("Setting up streaming sink...")
                sink_config = self.get_pipeline_config().get('sink', {})
                
                output_mode = sink_config.get('output_mode', 'append')
                trigger = sink_config.get('trigger', {'processingTime': '10 seconds'})
                checkpoint_location = sink_config.get('checkpoint_location', '/tmp/sessionize_checkpoint')
                
                query = stream_df.writeStream \
                    .outputMode(output_mode) \
                    .option("checkpointLocation", checkpoint_location)
                
                if 'processingTime' in trigger:
                    query = query.trigger(processingTime=trigger['processingTime'])
                elif 'once' in trigger and trigger['once']:
                    query = query.trigger(once=True)
                elif 'continuous' in trigger:
                    query = query.trigger(continuous=trigger['continuous'])
                
                if hasattr(self.sink, 'write_stream'):
                    self.stream_query = self.sink.write_stream(query, sink_config)
                else:
                    format_type = sink_config.get('format', 'console')
                    if format_type == 'console':
                        self.stream_query = query.format('console').start()
                    else:
                        path = sink_config.get('path', '/tmp/sessionize_output')
                        self.stream_query = query.format(format_type).option("path", path).start()
                
                self.logger.info("Streaming pipeline started. Awaiting termination...")
                self.stream_query.awaitTermination()
            
        except KeyboardInterrupt:
            self.logger.info("Streaming pipeline interrupted by user")
            self.stop()
        except Exception as e:
            self.logger.error(f"Streaming pipeline execution failed: {e}")
            raise
    
    def stop(self) -> None:
        if self.stream_query:
            self.stream_query.stop()
            self.logger.info("Streaming query stopped")
        
        if self.spark:
            self.spark.stop()
            self.logger.info("Spark session stopped")