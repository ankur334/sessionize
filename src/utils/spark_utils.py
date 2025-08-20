from typing import Any, Dict, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import json
from src.common.constants import DEFAULT_SPARK_CONFIG
from src.common.exceptions import SparkInitializationError


class SparkUtils:
    
    @staticmethod
    def create_spark_session(
        app_name: str = "SessionizePipeline",
        master: str = "local[*]",
        config: Optional[Dict[str, Any]] = None
    ) -> SparkSession:
        try:
            builder = SparkSession.builder.appName(app_name).master(master)
            
            merged_config = DEFAULT_SPARK_CONFIG.copy()
            if config:
                merged_config.update(config)
            
            for key, value in merged_config.items():
                builder = builder.config(key, value)
            
            spark = builder.getOrCreate()
            return spark
            
        except Exception as e:
            raise SparkInitializationError(f"Failed to create Spark session: {e}")
    
    @staticmethod
    def stop_spark_session(spark: SparkSession) -> None:
        if spark:
            spark.stop()
    
    @staticmethod
    def get_dataframe_info(df: DataFrame) -> Dict[str, Any]:
        return {
            'columns': df.columns,
            'schema': df.schema.json(),
            'count': df.count(),
            'partitions': df.rdd.getNumPartitions()
        }
    
    @staticmethod
    def repartition_dataframe(
        df: DataFrame,
        num_partitions: Optional[int] = None,
        partition_cols: Optional[list] = None
    ) -> DataFrame:
        if partition_cols:
            return df.repartition(*partition_cols)
        elif num_partitions:
            return df.repartition(num_partitions)
        else:
            return df
    
    @staticmethod
    def coalesce_dataframe(df: DataFrame, num_partitions: int) -> DataFrame:
        return df.coalesce(num_partitions)
    
    @staticmethod
    def cache_dataframe(df: DataFrame) -> DataFrame:
        return df.cache()
    
    @staticmethod
    def unpersist_dataframe(df: DataFrame) -> DataFrame:
        return df.unpersist()
    
    @staticmethod
    def show_dataframe_sample(df: DataFrame, num_rows: int = 20, truncate: bool = True) -> None:
        df.show(num_rows, truncate)
    
    @staticmethod
    def get_spark_version(spark: SparkSession) -> str:
        return spark.version
    
    @staticmethod
    def set_log_level(spark: SparkSession, level: str = "WARN") -> None:
        spark.sparkContext.setLogLevel(level)
    
    @staticmethod
    def register_temp_view(df: DataFrame, view_name: str) -> None:
        df.createOrReplaceTempView(view_name)
    
    @staticmethod
    def read_schema_from_json(schema_path: str) -> StructType:
        with open(schema_path, 'r') as f:
            schema_json = json.load(f)
        return StructType.fromJson(schema_json)
    
    @staticmethod
    def save_schema_to_json(schema: StructType, output_path: str) -> None:
        schema_json = json.loads(schema.json())
        with open(output_path, 'w') as f:
            json.dump(schema_json, f, indent=2)