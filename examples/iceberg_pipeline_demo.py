#!/usr/bin/env python3
"""
Iceberg Pipeline Demo - Batch and Streaming Examples

This script demonstrates the Sessionize framework's Iceberg sink functionality
with both batch and streaming modes. It uses a rate source for streaming data
generation, making it easy to test without external dependencies like Kafka.

Features demonstrated:
- Batch write to Iceberg tables with partitioning
- Streaming write to Iceberg tables with schema evolution
- Table creation and metadata management
- Data verification and querying

Usage:
    python examples/iceberg_pipeline_demo.py --mode batch
    python examples/iceberg_pipeline_demo.py --mode streaming
    python examples/iceberg_pipeline_demo.py --mode both
"""

import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, current_timestamp, to_json, struct, to_timestamp
from src.sink.iceberg_sink import IcebergSink
from src.transformer.json_transformer import JsonTransformer
from src.utils.logger import setup_logging
import logging
import time


def create_test_spark_session():
    """Create a Spark session configured for Iceberg."""
    return SparkSession.builder \
        .appName("IcebergTestPipeline") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg_warehouse") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/test_checkpoint") \
        .getOrCreate()


def create_test_stream(spark):
    """Create a test stream using rate source."""
    return spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 5) \
        .option("startingTimestamp", "2024-01-01 00:00:00") \
        .load() \
        .selectExpr(
            "CAST(value AS STRING) as event_id",
            "CASE WHEN value % 3 = 0 THEN 'click' WHEN value % 3 = 1 THEN 'view' ELSE 'purchase' END as event_type",
            "CONCAT('user_', CAST(value % 10 AS STRING)) as user_id",
            "timestamp",
            "CONCAT('session_', CAST(value % 5 AS STRING)) as session_id",
            "CONCAT('https://example.com/page', CAST(value % 20 AS STRING)) as page_url",
            "CASE WHEN value % 2 = 0 THEN 'button_click' ELSE 'scroll' END as action",
            "CAST(value * 100 AS INT) as duration_ms"
        )


def demo_batch_write_to_iceberg():
    """Test batch write to Iceberg table."""
    spark = create_test_spark_session()
    
    logger = logging.getLogger(__name__)
    logger.info("Creating test batch data...")
    
    # Create test batch data
    data = [
        ("evt_001", "click", "user_1", "2024-01-01 10:00:00", "session_1", "https://example.com/home", "button_click", 250),
        ("evt_002", "view", "user_2", "2024-01-01 10:01:00", "session_2", "https://example.com/products", "scroll", 1500),
        ("evt_003", "purchase", "user_1", "2024-01-01 10:02:00", "session_1", "https://example.com/checkout", "button_click", 3000),
        ("evt_004", "click", "user_3", "2024-01-01 10:03:00", "session_3", "https://example.com/about", "scroll", 500),
        ("evt_005", "view", "user_2", "2024-01-01 10:04:00", "session_2", "https://example.com/product/123", "button_click", 750),
    ]
    
    columns = ["event_id", "event_type", "user_id", "timestamp", "session_id", "page_url", "action", "duration_ms"]
    df = spark.createDataFrame(data, columns)
    
    # Convert timestamp string to timestamp type
    from pyspark.sql.functions import to_timestamp
    df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    
    # Configure Iceberg sink
    iceberg_config = {
        "catalog": "local",
        "database": "test_db",
        "table": "test_events",
        "partition_by": ["event_type"],
        "mode": "append",
        "create_table_if_not_exists": True,
        "table_properties": {
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        }
    }
    
    # Write to Iceberg
    sink = IcebergSink(iceberg_config)
    
    logger.info("Writing batch data to Iceberg table...")
    sink.write(df, iceberg_config)
    
    # Read back and verify
    logger.info("Reading data from Iceberg table...")
    result_df = spark.read.format("iceberg").load("local.test_db.test_events")
    
    logger.info(f"Total records written: {result_df.count()}")
    logger.info("Sample data from Iceberg table:")
    result_df.show(5, truncate=False)
    
    # Show table properties
    logger.info("Table partitions:")
    spark.sql("SELECT * FROM local.test_db.test_events.partitions").show()
    
    spark.stop()
    return True


def demo_streaming_write_to_iceberg():
    """Test streaming write to Iceberg table."""
    spark = create_test_spark_session()
    
    logger = logging.getLogger(__name__)
    logger.info("Creating test streaming data...")
    
    # Create test stream
    stream_df = create_test_stream(spark)
    
    # Add processing timestamp
    stream_df = stream_df.withColumn("processing_time", current_timestamp())
    
    # Configure Iceberg sink for streaming
    iceberg_config = {
        "catalog": "local",
        "database": "test_db",
        "table": "streaming_events",
        "partition_by": ["event_type"],
        "mode": "append",
        "create_table_if_not_exists": True,
        "merge_schema": True,
        "table_properties": {
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        },
        "output_mode": "append",
        "trigger": {
            "processingTime": "10 seconds"
        },
        "checkpoint_location": "/tmp/test_streaming_checkpoint",
        "commit_interval_ms": 10000
    }
    
    # Create Iceberg sink
    sink = IcebergSink(iceberg_config)
    
    # Create table first using a sample batch
    logger.info("Creating streaming table schema...")
    sample_data = [("sample", "click", "user_1", "2024-01-01 10:00:00", "session_1", "https://example.com/test", "test", 100, "2024-01-01 10:00:00")]
    sample_columns = ["event_id", "event_type", "user_id", "timestamp", "session_id", "page_url", "action", "duration_ms", "processing_time"]
    sample_df = spark.createDataFrame(sample_data, sample_columns)
    sample_df = sample_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    sample_df = sample_df.withColumn("processing_time", to_timestamp(col("processing_time"), "yyyy-MM-dd HH:mm:ss"))
    
    # Write empty sample to create schema
    sink.write(sample_df.limit(0), iceberg_config)
    
    logger.info("Starting streaming write to Iceberg table...")
    
    # Write stream
    query_builder = stream_df.writeStream \
        .outputMode("append") \
        .trigger(processingTime="10 seconds")
    
    query = sink.write_stream(query_builder, iceberg_config)
    
    # Let it run for 30 seconds
    logger.info("Streaming for 30 seconds...")
    time.sleep(30)
    
    # Stop the stream
    query.stop()
    
    # Read and verify
    logger.info("Reading streaming data from Iceberg table...")
    result_df = spark.read.format("iceberg").load("local.test_db.streaming_events")
    
    logger.info(f"Total records in streaming table: {result_df.count()}")
    logger.info("Sample streaming data:")
    result_df.orderBy("timestamp").show(10, truncate=False)
    
    spark.stop()
    return True


def main():
    """Main function to run tests."""
    setup_logging({
        "level": "INFO",
        "file_logging": True,
        "log_dir": "logs"
    })
    
    logger = logging.getLogger(__name__)
    
    import argparse
    parser = argparse.ArgumentParser(description="Iceberg Pipeline Demo - Batch and Streaming Examples")
    parser.add_argument(
        "--mode",
        type=str,
        choices=["batch", "streaming", "both"],
        default="batch",
        help="Demo mode to run (batch, streaming, or both)"
    )
    
    args = parser.parse_args()
    
    try:
        if args.mode in ["batch", "both"]:
            logger.info("=" * 50)
            logger.info("Running BATCH test...")
            logger.info("=" * 50)
            demo_batch_write_to_iceberg()
            logger.info("Batch test completed successfully!")
        
        if args.mode in ["streaming", "both"]:
            logger.info("=" * 50)
            logger.info("Running STREAMING test...")
            logger.info("=" * 50)
            demo_streaming_write_to_iceberg()
            logger.info("Streaming test completed successfully!")
        
        logger.info("All tests completed successfully!")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()