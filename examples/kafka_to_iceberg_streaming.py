#!/usr/bin/env python3
"""
Kafka to Iceberg Streaming Pipeline Example

This example demonstrates a streaming pipeline that:
1. Reads JSON messages from Kafka topics
2. Parses and transforms the JSON data
3. Writes the processed data to Apache Iceberg tables

Prerequisites:
- Kafka cluster running (localhost:9092 by default)
- Iceberg catalog configured (uses local filesystem by default)
- Required dependencies installed (pyspark, pyiceberg)
"""

import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.controller.pipeline_controller import PipelineController
from src.config.config_manager import ConfigManager
from src.utils.logger import setup_logging
import logging


def create_sample_pipeline_config():
    """Create a sample configuration for Kafka to Iceberg streaming pipeline."""
    return {
        "spark": {
            "app_name": "KafkaToIcebergStreaming",
            "master": "local[*]",
            "config": {
                "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
                "spark.sql.catalog.spark_catalog.type": "hive",
                "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
                "spark.sql.catalog.local.type": "hadoop",
                "spark.sql.catalog.local.warehouse": "/tmp/iceberg_warehouse",
                "spark.sql.streaming.checkpointLocation": "/tmp/iceberg_checkpoint",
                "spark.sql.streaming.schemaInference": "true",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true"
            }
        },
        "pipeline": {
            "type": "streaming",
            "extractor": {
                "type": "kafka",
                "kafka.bootstrap.servers": "localhost:9092",
                "subscribe": "events-topic",
                "startingOffsets": "latest",
                "failOnDataLoss": False,
                "maxOffsetsPerTrigger": 10000,
                "kafka.group.id": "sessionize-streaming-consumer"
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
                "drop_columns": ["key", "topic", "partition", "offset", "timestampType"]
            },
            "sink": {
                "type": "iceberg",
                "catalog": "local",
                "database": "events",
                "table": "user_events",
                "partition_by": ["event_type", "date(timestamp)"],
                "mode": "append",
                "create_table_if_not_exists": True,
                "merge_schema": True,
                "table_properties": {
                    "write.format.default": "parquet",
                    "write.parquet.compression-codec": "snappy",
                    "write.metadata.delete-after-commit.enabled": "true",
                    "write.metadata.previous-versions-max": "10",
                    "history.expire.max-snapshot-age-ms": "86400000"
                },
                "output_mode": "append",
                "trigger": {
                    "processingTime": "30 seconds"
                },
                "checkpoint_location": "/tmp/iceberg_streaming_checkpoint",
                "commit_interval_ms": 60000,
                "fanout_enabled": True
            }
        },
        "logging": {
            "level": "INFO",
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "file_logging": True,
            "log_dir": "logs"
        }
    }


def create_sessionization_pipeline_config():
    """Create a configuration for user session analysis pipeline."""
    return {
        "spark": {
            "app_name": "UserSessionAnalysis",
            "master": "local[*]",
            "config": {
                "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
                "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
                "spark.sql.catalog.local.type": "hadoop",
                "spark.sql.catalog.local.warehouse": "/tmp/iceberg_warehouse",
                "spark.sql.streaming.stateStore.retention": "2h",
                "spark.sql.shuffle.partitions": "100"
            }
        },
        "pipeline": {
            "type": "streaming",
            "extractor": {
                "type": "kafka",
                "kafka.bootstrap.servers": "localhost:9092",
                "subscribe": "clickstream",
                "startingOffsets": "earliest",
                "kafka.group.id": "session-analyzer"
            },
            "transformer": {
                "type": "json",
                "schema": {
                    "user_id": "string",
                    "session_id": "string",
                    "timestamp": "timestamp",
                    "page": "string",
                    "action": "string",
                    "duration_seconds": "int"
                },
                "operations": [
                    {"watermark": "timestamp, 30 minutes"},
                    {
                        "groupBy": ["user_id", "session_id", "window(timestamp, '30 minutes')"],
                        "agg": {
                            "duration_seconds": "sum",
                            "page": "count"
                        }
                    }
                ]
            },
            "sink": {
                "type": "iceberg",
                "catalog": "local",
                "database": "analytics",
                "table": "user_sessions",
                "partition_by": ["date(window.start)"],
                "mode": "append",
                "output_mode": "update",
                "trigger": {
                    "processingTime": "1 minute"
                }
            }
        }
    }


def main():
    """Main function to run the streaming pipeline."""
    setup_logging({
        "level": "INFO",
        "file_logging": True,
        "log_dir": "logs"
    })
    
    logger = logging.getLogger(__name__)
    
    import argparse
    parser = argparse.ArgumentParser(description="Kafka to Iceberg Streaming Pipeline")
    parser.add_argument(
        "--config",
        type=str,
        help="Path to configuration file (YAML or JSON)"
    )
    parser.add_argument(
        "--pipeline",
        type=str,
        choices=["events", "sessions"],
        default="events",
        help="Type of pipeline to run"
    )
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Run in test mode with shorter intervals"
    )
    
    args = parser.parse_args()
    
    try:
        if args.config:
            logger.info(f"Loading configuration from: {args.config}")
            config = ConfigManager.load_config(args.config)
        else:
            if args.pipeline == "sessions":
                config = create_sessionization_pipeline_config()
                logger.info("Using sessionization pipeline configuration")
            else:
                config = create_sample_pipeline_config()
                logger.info("Using default events pipeline configuration")
            
            if args.test_mode:
                config["pipeline"]["sink"]["trigger"] = {"once": True}
                logger.info("Running in test mode - will process once and stop")
        
        controller = PipelineController(config=config)
        
        if not controller.validate():
            logger.error("Pipeline configuration validation failed")
            sys.exit(1)
        
        logger.info("Starting Kafka to Iceberg streaming pipeline...")
        logger.info(f"Reading from Kafka topic: {config['pipeline']['extractor'].get('subscribe', 'unknown')}")
        logger.info(f"Writing to Iceberg table: {config['pipeline']['sink'].get('table', 'unknown')}")
        
        controller.execute()
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()