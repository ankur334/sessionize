#!/usr/bin/env python3
"""
Iceberg Data Verification Script

This script reads data from the Iceberg table to verify that the 
Kafka to Iceberg streaming pipeline is working correctly.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with Iceberg support."""
    return SparkSession.builder \
        .appName("IcebergDataVerification") \
        .master("local[*]") \
        .config("spark.jars.packages", 
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
        .config("spark.sql.extensions", 
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", 
                "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg_warehouse") \
        .getOrCreate()

def main():
    spark = None
    try:
        logger.info("Creating Spark session with Iceberg support...")
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        
        table_name = "local.events.user_events"
        logger.info(f"Reading data from Iceberg table: {table_name}")
        
        # Read data from Iceberg table
        df = spark.table(table_name)
        
        # Show basic statistics
        total_records = df.count()
        logger.info(f"Total records in table: {total_records}")
        
        if total_records > 0:
            logger.info("Sample data from table:")
            df.show(10, truncate=False)
            
            # Show event type distribution
            logger.info("Event type distribution:")
            df.groupBy("event_type").count().show()
            
            # Show schema
            logger.info("Table schema:")
            df.printSchema()
            
            # Show partition information
            logger.info("Partitioning information:")
            partitions_df = spark.sql(f"SHOW PARTITIONS {table_name}")
            partitions_df.show()
            
        else:
            logger.warning("No data found in the table. Pipeline may not have processed any events yet.")
            
    except Exception as e:
        logger.error(f"Failed to read from Iceberg table: {e}")
        return 1
    finally:
        if spark:
            spark.stop()
    
    return 0

if __name__ == "__main__":
    exit(main())