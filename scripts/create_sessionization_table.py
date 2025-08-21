#!/usr/bin/env python3
"""
Create Iceberg table for sessionization pipeline with proper partitioning

Usage:
    python scripts/create_sessionization_table.py
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import *
except ImportError:
    print("PySpark is required. Install it with: pip install pyspark")
    sys.exit(1)


def create_spark_session():
    """Create Spark session with Iceberg support."""
    return SparkSession.builder \
        .appName("CreateSessionizationTable") \
        .master("local[4]") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/tmp/sessionize_warehouse") \
        .getOrCreate()


def create_sessionization_table():
    """Create the Iceberg table for sessionization data with partitioning."""
    
    print("🔧 Creating Iceberg table for sessionization pipeline")
    print("=" * 60)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Create database
        print("📁 Creating database: sessions")
        spark.sql("CREATE DATABASE IF NOT EXISTS local.sessions")
        
        # Drop existing table if it exists
        print("🗑️  Dropping existing table if present...")
        spark.sql("DROP TABLE IF EXISTS local.sessions.user_sessions_partitioned")
        
        # Create table with proper schema and partitioning
        print("📋 Creating table with sessionization schema and partitioning...")
        
        create_table_sql = """
        CREATE TABLE local.sessions.user_sessions_partitioned (
            -- Original user and event data
            uuid STRING,
            event_id STRING,
            page_name STRING,
            event_timestamp STRING,
            booking_details MAP<STRING, STRING>,
            event_details STRUCT<
                event_name: STRING,
                event_type: STRING,
                event_value: STRING
            >,
            
            -- Sessionization data
            session_id STRING,
            session_start_time_ms BIGINT,
            session_end_time_ms BIGINT,
            session_duration_seconds DOUBLE,
            session_start_time TIMESTAMP,
            session_end_time TIMESTAMP,
            
            -- Event timing
            event_time TIMESTAMP,
            event_time_ms BIGINT,
            
            -- Partition columns (added by transformer)
            session_date STRING,
            user_hash_bucket INT
        ) USING iceberg
        PARTITIONED BY (session_date, user_hash_bucket)
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes' = '134217728',
            'write.distribution-mode' = 'hash',
            'commit.retry.num-retries' = '3',
            'commit.retry.min-wait-ms' = '100',
            'commit.retry.max-wait-ms' = '60000',
            'history.expire.max-snapshot-age-ms' = '432000000',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '100',
            'read.split.target-size' = '134217728',
            'format-version' = '2'
        )
        """
        
        spark.sql(create_table_sql)
        print("✅ Created table: local.sessions.user_sessions_partitioned")
        
        # Verify table creation
        print("\n🔍 Verifying table structure:")
        table_info = spark.sql("DESCRIBE TABLE local.sessions.user_sessions_partitioned")
        table_info.show(truncate=False)
        
        # Show partition information
        print("\n📊 Partition Information:")
        partitions = spark.sql("SHOW PARTITIONS local.sessions.user_sessions_partitioned")
        if partitions.count() == 0:
            print("   No partitions yet (table is empty)")
        else:
            partitions.show(10, truncate=False)
        
        # Show table properties
        print("\n⚙️  Table Properties:")
        properties = spark.sql("SHOW TBLPROPERTIES local.sessions.user_sessions_partitioned")
        properties.show(truncate=False)
        
        print("\n✅ SUCCESS")
        print("=" * 40)
        print("✅ Iceberg table created successfully")
        print("✅ Partitioning: session_date, user_hash_bucket")
        print("✅ Optimized table properties configured")
        print("✅ Ready for sessionization pipeline")
        print("\n🚀 You can now run the sessionization pipeline!")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to create table: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        spark.stop()


if __name__ == "__main__":
    success = create_sessionization_table()
    sys.exit(0 if success else 1)