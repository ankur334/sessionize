import pytest
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from typing import Generator

sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession, None, None]:
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .appName("SessionizeTests") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture
def sample_config():
    """Provide a sample configuration for testing."""
    return {
        "spark": {
            "app_name": "TestPipeline",
            "master": "local[*]",
            "config": {
                "spark.sql.shuffle.partitions": 2
            }
        },
        "pipeline": {
            "type": "batch",
            "extractor": {
                "type": "file",
                "format": "csv",
                "path": "tests/data/input/test.csv"
            },
            "transformer": {
                "type": "passthrough"
            },
            "sink": {
                "type": "file",
                "format": "parquet",
                "path": "tests/data/output/test_output"
            }
        }
    }


@pytest.fixture
def sample_streaming_config():
    """Provide a sample streaming configuration for testing."""
    return {
        "spark": {
            "app_name": "TestStreamingPipeline",
            "master": "local[*]",
            "config": {
                "spark.sql.shuffle.partitions": 2
            }
        },
        "pipeline": {
            "type": "streaming",
            "extractor": {
                "type": "rate",
                "format": "rate"
            },
            "transformer": {
                "type": "passthrough"
            },
            "sink": {
                "type": "console",
                "format": "console",
                "output_mode": "append",
                "trigger": {
                    "once": True
                }
            }
        }
    }


@pytest.fixture
def test_dataframe(spark_session):
    """Create a test DataFrame."""
    data = [
        (1, "Alice", 30, "alice@example.com", "Engineering"),
        (2, "Bob", 25, "bob@example.com", "Marketing"),
        (3, "Charlie", 35, "charlie@example.com", "Sales"),
        (4, "Diana", 28, "diana@example.com", "Engineering"),
        (5, "Eve", 32, "eve@example.com", "HR")
    ]
    columns = ["id", "name", "age", "email", "department"]
    
    return spark_session.createDataFrame(data, columns)