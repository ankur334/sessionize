#!/usr/bin/env python3

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("VerifyOutput") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n=== Reading output data from data/output/batch_result ===\n")
df = spark.read.parquet("data/output/batch_result")

print(f"Total records after filtering (age > 25): {df.count()}")
print("\nData Preview:")
df.show()

print("\nSchema:")
df.printSchema()

spark.stop()