#!/usr/bin/env python3
"""
Kafka to Iceberg Streaming Pipeline

A standalone pipeline runner that streams data from Kafka to Iceberg tables.
Can be executed directly or orchestrated via Airflow.
"""

import sys
import os
import argparse
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.controller.pipeline_controller import PipelineController
from src.utils.logger import setup_logging
import logging


class KafkaToIcebergPipeline:
    """Kafka to Iceberg streaming pipeline runner."""
    
    def __init__(self, kafka_servers="localhost:9092", kafka_topic="events-topic", 
                 iceberg_database="events", iceberg_table="user_events"):
        self.kafka_servers = kafka_servers
        self.kafka_topic = kafka_topic
        self.iceberg_database = iceberg_database
        self.iceberg_table = iceberg_table
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get_pipeline_config(self):
        """Get the pipeline configuration."""
        return {
            "spark": {
                "app_name": "KafkaToIcebergStreaming",
                "master": "local[*]",
                "packages": [
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3"
                ],
                "config": {
                    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                    "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
                    "spark.sql.catalog.local.type": "hadoop",
                    "spark.sql.catalog.local.warehouse": "/tmp/iceberg_warehouse",
                    "spark.sql.streaming.checkpointLocation": "/tmp/iceberg_checkpoint",
                    "spark.sql.streaming.schemaInference": "true"
                }
            },
            "pipeline": {
                "type": "streaming",
                "extractor": {
                    "type": "kafka",
                    "kafka.bootstrap.servers": self.kafka_servers,
                    "subscribe": self.kafka_topic,
                    "startingOffsets": "latest",
                    "failOnDataLoss": False,
                    "maxOffsetsPerTrigger": 10000
                },
                "transformer": {
                    "type": "json",
                    "schema": [
                        {"name": "event_id", "type": "string", "nullable": False},
                        {"name": "event_type", "type": "string", "nullable": False},
                        {"name": "user_id", "type": "string", "nullable": False},
                        {"name": "timestamp", "type": "timestamp", "nullable": False},
                        {"name": "session_id", "type": "string", "nullable": True},
                        {"name": "page_url", "type": "string", "nullable": True},
                        {"name": "action", "type": "string", "nullable": True},
                        {"name": "duration_ms", "type": "integer", "nullable": True},
                        {"name": "properties", "type": "map<string,string>", "nullable": True}
                    ],
                    "value_column": "value",
                    "timestamp_column": "timestamp",
                    "timestamp_format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
                    "add_metadata": True,
                    "operations": [
                        {"filter": "event_type != 'heartbeat'"},
                        {"watermark": "timestamp, 10 minutes"}
                    ],
                    "drop_columns": ["key", "value", "topic", "partition", "offset", "timestampType", "kafka_timestamp"]
                },
                "sink": {
                    "type": "iceberg",
                    "catalog": "local",
                    "database": self.iceberg_database,
                    "table": self.iceberg_table,
                    "partition_by": ["event_type"],
                    "mode": "append",
                    "create_table_if_not_exists": True,
                    "merge_schema": True,
                    "output_mode": "append",
                    "trigger": {"processingTime": "30 seconds"}
                }
            },
            "logging": {
                "level": "INFO",
                "file_logging": True,
                "log_dir": "logs"
            }
        }
    
    def run(self, test_mode=False):
        """Run the pipeline."""
        try:
            config = self.get_pipeline_config()
            
            if test_mode:
                config["pipeline"]["sink"]["trigger"] = {"once": True}
                self.logger.info("Running in test mode - will process once and stop")
            
            self.logger.info(f"Starting Kafka to Iceberg pipeline")
            self.logger.info(f"Kafka: {self.kafka_servers} -> Topic: {self.kafka_topic}")
            self.logger.info(f"Iceberg: {self.iceberg_database}.{self.iceberg_table}")
            
            controller = PipelineController(config=config)
            
            if not controller.validate():
                raise Exception("Pipeline configuration validation failed")
            
            controller.execute()
            
        except KeyboardInterrupt:
            self.logger.info("Pipeline interrupted by user")
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise


def main():
    """Main entry point for the pipeline."""
    parser = argparse.ArgumentParser(description="Kafka to Iceberg Streaming Pipeline")
    parser.add_argument("--kafka-servers", default="localhost:9092", 
                       help="Kafka bootstrap servers")
    parser.add_argument("--kafka-topic", default="events-topic",
                       help="Kafka topic to consume from")
    parser.add_argument("--iceberg-database", default="events",
                       help="Iceberg database name")
    parser.add_argument("--iceberg-table", default="user_events",
                       help="Iceberg table name")
    parser.add_argument("--test-mode", action="store_true",
                       help="Run in test mode (process once and stop)")
    parser.add_argument("--log-level", default="INFO",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Logging level")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging({
        "level": args.log_level,
        "file_logging": True,
        "log_dir": "logs"
    })
    
    # Create and run pipeline
    pipeline = KafkaToIcebergPipeline(
        kafka_servers=args.kafka_servers,
        kafka_topic=args.kafka_topic,
        iceberg_database=args.iceberg_database,
        iceberg_table=args.iceberg_table
    )
    
    try:
        pipeline.run(test_mode=args.test_mode)
        return 0
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())