import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from src.transformer.base_transformer import BaseTransformer
from src.runner.batch_runner import BatchRunner
from src.config.config_manager import ConfigManager
from src.utils.logger import setup_logging
from typing import Any, Dict
import logging


class DataQualityTransformer(BaseTransformer):
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def transform(self, df, config: Dict[str, Any]):
        self.logger.info("Applying data quality transformations")
        
        from pyspark.sql import functions as F
        
        result = df.dropna(how='any')
        
        result = result.dropDuplicates()
        
        if 'timestamp_col' in config:
            timestamp_col = config['timestamp_col']
            result = result.withColumn(
                f"{timestamp_col}_formatted",
                F.date_format(F.col(timestamp_col), "yyyy-MM-dd HH:mm:ss")
            )
        
        for col in result.columns:
            if 'string' in str(result.schema[col].dataType):
                result = result.withColumn(col, F.trim(F.col(col)))
        
        result = result.withColumn("processing_timestamp", F.current_timestamp())
        
        self.logger.info(f"Data quality transformations complete. Records: {result.count()}")
        return result
    
    def validate_config(self) -> bool:
        return True


def create_sample_data(spark):
    data = [
        (1, "John Doe", 25, "john@example.com", "2024-01-01"),
        (2, "Jane Smith", 30, "jane@example.com", "2024-01-02"),
        (3, "Bob Wilson", None, "bob@example.com", "2024-01-03"),
        (4, "Alice Brown", 28, "alice@example.com", "2024-01-04"),
        (1, "John Doe", 25, "john@example.com", "2024-01-01"),
    ]
    
    columns = ["id", "name", "age", "email", "timestamp"]
    
    df = spark.createDataFrame(data, columns)
    
    df.write.mode("overwrite").csv("data/input/sample.csv", header=True)
    print("Sample data created at data/input/sample.csv")


def main():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("CreateSampleData") \
        .master("local[*]") \
        .getOrCreate()
    
    create_sample_data(spark)
    
    config = {
        "spark": {
            "app_name": "DataQualityPipeline",
            "master": "local[*]"
        },
        "pipeline": {
            "extractor": {
                "type": "file",
                "format": "csv",
                "path": "data/input/sample.csv",
                "options": {
                    "header": True,
                    "inferSchema": True
                }
            },
            "transformer": {
                "type": "custom",
                "class": "DataQualityTransformer",
                "timestamp_col": "timestamp"
            },
            "sink": {
                "type": "file",
                "format": "parquet",
                "path": "data/output/quality_checked",
                "mode": "overwrite"
            }
        },
        "logging": {
            "level": "INFO"
        }
    }
    
    setup_logging(config.get('logging'))
    
    runner = BatchRunner(config)
    runner.transformer = DataQualityTransformer(config['pipeline']['transformer'])
    
    try:
        runner.run()
        print("\nPipeline completed successfully!")
        print("Output saved to: data/output/quality_checked")
    finally:
        runner.stop()
        spark.stop()


if __name__ == "__main__":
    main()