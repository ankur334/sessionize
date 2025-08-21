#!/usr/bin/env python3
"""
User Sessionization Pipeline

Real-time streaming pipeline for user sessionization based on clickstream events.
Implements business rules for user session detection:
- Session ends after 30 minutes of inactivity
- Session ends after 2 hours of continuous activity (max session duration)

Sample Usage:
    python main.py run user_sessionization_pipeline --kafka-topic clickstream-events --test-mode
    python main.py run user_sessionization_pipeline --help
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


class UserSessionizationPipeline:
    """Real-time user sessionization pipeline for clickstream events."""
    
    def __init__(self, kafka_servers="localhost:9092", kafka_topic="clickstream-events",
                 iceberg_database="sessions", iceberg_table="user_sessions",
                 test_mode=False, inactivity_timeout_minutes=30, max_session_duration_hours=2):
        self.kafka_servers = kafka_servers
        self.kafka_topic = kafka_topic
        self.iceberg_database = iceberg_database
        self.iceberg_table = iceberg_table
        self.test_mode = test_mode
        self.inactivity_timeout_minutes = inactivity_timeout_minutes
        self.max_session_duration_hours = max_session_duration_hours
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get_pipeline_config(self):
        """Get the pipeline configuration for user sessionization."""
        return {
            "spark": {
                "app_name": "UserSessionizationPipeline",
                "master": "local[*]",
                "packages": [
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3"
                ],
                "config": {
                    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                    "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
                    "spark.sql.catalog.local.type": "hadoop",
                    "spark.sql.catalog.local.warehouse": "/tmp/sessionize_warehouse",
                    "spark.sql.streaming.checkpointLocation": "/tmp/sessionize_checkpoint",
                    "spark.sql.streaming.schemaInference": "true",
                    "spark.sql.adaptive.enabled": "false",  # Disable for streaming
                    "spark.sql.streaming.stateStore.providerClass": "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider"
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
                    "maxOffsetsPerTrigger": 1000,
                    "kafka.group.id": "sessionization-consumer-group"
                },
                "transformer": {
                    "type": "sessionization",
                    "user_id_column": "uuid",
                    "timestamp_column": "event_timestamp",
                    "inactivity_timeout_minutes": self.inactivity_timeout_minutes,
                    "max_session_duration_hours": self.max_session_duration_hours,
                    "watermark_delay": "10 minutes",
                    
                    # JSON schema for clickstream events
                    "schema": [
                        {"name": "event_id", "type": "string", "nullable": False},
                        {"name": "page_name", "type": "string", "nullable": True},
                        {"name": "event_timestamp", "type": "string", "nullable": False},
                        {"name": "booking_details", "type": "map<string,string>", "nullable": True},
                        {"name": "uuid", "type": "string", "nullable": False},
                        {
                            "name": "event_details", 
                            "type": "struct", 
                            "nullable": True,
                            "fields": [
                                {"name": "event_name", "type": "string", "nullable": True},
                                {"name": "event_type", "type": "string", "nullable": True},
                                {"name": "event_value", "type": "string", "nullable": True}
                            ]
                        }
                    ],
                    "value_column": "value",
                    "add_metadata": True,
                    
                    # Pre-processing operations
                    "operations": [
                        {"filter": "uuid IS NOT NULL"},
                        {"filter": "event_timestamp IS NOT NULL"}
                    ],
                    "drop_columns": ["key", "topic", "partition", "offset", "timestampType"]
                },
                "sink": {
                    "type": "iceberg",
                    "catalog": "local",
                    "database": self.iceberg_database,
                    "table": self.iceberg_table,
                    
                    # ADVANCED PARTITIONING STRATEGY for sessionization workloads
                    "partition_by": [
                        "session_date",      # Date-based partitioning for time-based queries
                        "user_hash_bucket"   # Hash-based bucketing for balanced distribution
                    ],
                    
                    "mode": "append",
                    "create_table_if_not_exists": True,
                    "merge_schema": True,
                    "output_mode": "append",  # Use append mode for Iceberg streaming
                    "trigger": {
                        "processingTime": "30 seconds"
                    } if not self.test_mode else {
                        "once": True
                    },
                    
                    # OPTIMIZED ICEBERG TABLE PROPERTIES
                    "table_properties": {
                        # File format and compression
                        "write.format.default": "parquet",
                        "write.parquet.compression-codec": "zstd",  # Better compression than snappy
                        
                        # Partitioning and bucketing optimizations
                        "write.target-file-size-bytes": "134217728",  # 128MB target file size
                        "write.distribution-mode": "hash",           # Hash distribution for balanced writes
                        
                        # Performance optimizations
                        "commit.retry.num-retries": "3",
                        "commit.retry.min-wait-ms": "100",
                        "commit.retry.max-wait-ms": "60000",
                        
                        # Metadata and cleanup
                        "history.expire.max-snapshot-age-ms": "432000000",  # 5 days
                        "write.metadata.delete-after-commit.enabled": "true",
                        "write.metadata.previous-versions-max": "100",
                        
                        # Query optimization  
                        "read.split.target-size": "134217728",       # 128MB read splits
                        "format-version": "2"                       # Use Iceberg v2 features
                    }
                }
            },
            "logging": {
                "level": "INFO",
                "file_logging": True,
                "log_dir": "logs"
            }
        }
    
    def run(self):
        """Run the sessionization pipeline."""
        try:
            config = self.get_pipeline_config()
            
            if self.test_mode:
                self.logger.info("Running in test mode - will process once and stop")
            
            self.logger.info("Starting User Sessionization Pipeline")
            self.logger.info(f"Kafka: {self.kafka_servers} -> Topic: {self.kafka_topic}")
            self.logger.info(f"Sessions: {self.iceberg_database}.{self.iceberg_table}")
            self.logger.info(f"Rules: {self.inactivity_timeout_minutes}min inactivity, "
                           f"{self.max_session_duration_hours}hr max duration")
            
            controller = PipelineController(config=config)
            
            if not controller.validate():
                raise Exception("Pipeline configuration validation failed")
            
            controller.execute()
            
            self.logger.info("User sessionization pipeline completed successfully")
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            return 1
        
        return 0


def main():
    """Main entry point for the sessionization pipeline."""
    parser = argparse.ArgumentParser(description="User Sessionization Pipeline")
    
    # Kafka configuration
    parser.add_argument("--kafka-servers", default="localhost:9092",
                       help="Kafka bootstrap servers")
    parser.add_argument("--kafka-topic", default="clickstream-events",
                       help="Kafka topic to consume clickstream events from")
    
    # Iceberg configuration
    parser.add_argument("--iceberg-database", default="sessions",
                       help="Iceberg database name for session data")
    parser.add_argument("--iceberg-table", default="user_sessions",
                       help="Iceberg table name for session data")
    
    # Sessionization configuration
    parser.add_argument("--inactivity-timeout", type=int, default=30,
                       help="Session inactivity timeout in minutes (default: 30)")
    parser.add_argument("--max-session-duration", type=int, default=2,
                       help="Maximum session duration in hours (default: 2)")
    
    # Runtime configuration
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
    
    # Create and run the pipeline
    pipeline = UserSessionizationPipeline(
        kafka_servers=args.kafka_servers,
        kafka_topic=args.kafka_topic,
        iceberg_database=args.iceberg_database,
        iceberg_table=args.iceberg_table,
        test_mode=args.test_mode,
        inactivity_timeout_minutes=args.inactivity_timeout,
        max_session_duration_hours=args.max_session_duration
    )
    
    return pipeline.run()


if __name__ == "__main__":
    sys.exit(main())