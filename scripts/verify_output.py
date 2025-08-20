#!/usr/bin/env python3
"""
Batch Output Verification Script

This script reads and verifies the output data from batch processing 
to ensure the pipeline is working correctly.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    spark = None
    try:
        logger.info("Creating Spark session for output verification...")
        spark = SparkSession.builder \
            .appName("VerifyBatchOutput") \
            .master("local[*]") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        output_path = "data/output/batch_result"
        logger.info(f"Reading output data from: {output_path}")
        
        # Check if output directory exists
        if not Path(output_path).exists():
            logger.error(f"Output directory not found: {output_path}")
            logger.info("Please run the batch processing pipeline first")
            return 1
        
        df = spark.read.parquet(output_path)
        
        record_count = df.count()
        logger.info(f"Total records after filtering (age > 25): {record_count}")
        
        if record_count > 0:
            logger.info("Data Preview:")
            df.show()
            
            logger.info("Schema:")
            df.printSchema()
            
            # Show some statistics
            logger.info("Age statistics:")
            df.describe("age").show()
        else:
            logger.warning("No records found in output. Check the filtering conditions.")
        
        logger.info("Output verification completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"Failed to verify output: {e}")
        return 1
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    exit(main())