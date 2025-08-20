from typing import Any, Dict, Optional
from pyspark.sql import DataFrame, SparkSession
from src.extractor.base_extractor import BaseExtractor
from src.common.exceptions import ExtractionError, ConfigurationError
import logging


class KafkaExtractor(BaseExtractor):
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.required_config = ['kafka.bootstrap.servers', 'subscribe']
    
    def extract(self, spark: SparkSession, config: Dict[str, Any]) -> DataFrame:
        try:
            if not self.validate_config():
                raise ConfigurationError("Invalid Kafka configuration")
            
            kafka_config = self._prepare_kafka_config(config)
            
            self.logger.info(f"Creating Kafka stream from topic: {kafka_config.get('subscribe', config.get('topics'))}")
            
            stream_df = spark.readStream.format("kafka")
            
            for key, value in kafka_config.items():
                stream_df = stream_df.option(key, value)
            
            df = stream_df.load()
            
            df = df.selectExpr(
                "CAST(key AS STRING) as key",
                "CAST(value AS STRING) as value",
                "topic",
                "partition",
                "offset",
                "timestamp",
                "timestampType"
            )
            
            self.logger.info("Kafka stream created successfully")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to create Kafka stream: {e}")
            raise ExtractionError(f"Kafka extraction failed: {e}")
    
    def _prepare_kafka_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        kafka_config = {}
        
        if 'kafka.bootstrap.servers' in config:
            kafka_config['kafka.bootstrap.servers'] = config['kafka.bootstrap.servers']
        elif 'bootstrap_servers' in config:
            kafka_config['kafka.bootstrap.servers'] = config['bootstrap_servers']
        
        if 'subscribe' in config:
            kafka_config['subscribe'] = config['subscribe']
        elif 'topics' in config:
            kafka_config['subscribe'] = ','.join(config['topics']) if isinstance(config['topics'], list) else config['topics']
        elif 'subscribePattern' in config:
            kafka_config['subscribePattern'] = config['subscribePattern']
        elif 'assign' in config:
            import json
            kafka_config['assign'] = json.dumps(config['assign'])
        
        kafka_config['startingOffsets'] = config.get('startingOffsets', 'latest')
        kafka_config['failOnDataLoss'] = str(config.get('failOnDataLoss', 'false')).lower()
        
        if 'maxOffsetsPerTrigger' in config:
            kafka_config['maxOffsetsPerTrigger'] = str(config['maxOffsetsPerTrigger'])
        
        if 'kafka.group.id' in config:
            kafka_config['kafka.group.id'] = config['kafka.group.id']
        elif 'group_id' in config:
            kafka_config['kafka.group.id'] = config['group_id']
        
        for key, value in config.items():
            if key.startswith('kafka.') and key not in kafka_config:
                kafka_config[key] = value
        
        return kafka_config
    
    def validate_config(self) -> bool:
        if not self.config:
            self.logger.error("No configuration provided")
            return False
        
        has_bootstrap_servers = any(key in self.config for key in ['kafka.bootstrap.servers', 'bootstrap_servers'])
        if not has_bootstrap_servers:
            self.logger.error("Missing required configuration: kafka.bootstrap.servers")
            return False
        
        has_topic_config = any(key in self.config for key in ['subscribe', 'topics', 'subscribePattern', 'assign'])
        if not has_topic_config:
            self.logger.error("Missing topic configuration: must specify subscribe, topics, subscribePattern, or assign")
            return False
        
        return True
    
    def extract_batch(self, spark: SparkSession, config: Dict[str, Any]) -> DataFrame:
        try:
            if not self.validate_config():
                raise ConfigurationError("Invalid Kafka configuration")
            
            kafka_config = self._prepare_kafka_config(config)
            
            batch_config = kafka_config.copy()
            if 'startingOffsets' not in batch_config:
                batch_config['startingOffsets'] = 'earliest'
            if 'endingOffsets' not in batch_config:
                batch_config['endingOffsets'] = 'latest'
            
            self.logger.info(f"Reading Kafka batch from topic: {kafka_config.get('subscribe', config.get('topics'))}")
            
            batch_df = spark.read.format("kafka")
            
            for key, value in batch_config.items():
                batch_df = batch_df.option(key, value)
            
            df = batch_df.load()
            
            df = df.selectExpr(
                "CAST(key AS STRING) as key",
                "CAST(value AS STRING) as value",
                "topic",
                "partition",
                "offset",
                "timestamp",
                "timestampType"
            )
            
            self.logger.info("Kafka batch read successfully")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read Kafka batch: {e}")
            raise ExtractionError(f"Kafka batch extraction failed: {e}")