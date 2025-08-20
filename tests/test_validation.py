import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.utils.validation import DataValidator, ConfigValidator
from src.common.exceptions import ValidationError


class TestDataValidator:
    
    def test_validate_min_rows(self, test_dataframe):
        validations = {"min_rows": 3}
        assert DataValidator.validate_dataframe(test_dataframe, validations) == True
        
        validations = {"min_rows": 10}
        with pytest.raises(ValidationError):
            DataValidator.validate_dataframe(test_dataframe, validations)
    
    def test_validate_required_columns(self, test_dataframe):
        validations = {"required_columns": ["id", "name"]}
        assert DataValidator.validate_dataframe(test_dataframe, validations) == True
        
        validations = {"required_columns": ["id", "missing_column"]}
        with pytest.raises(ValidationError):
            DataValidator.validate_dataframe(test_dataframe, validations)
    
    def test_validate_no_nulls(self, spark_session):
        data = [(1, "a"), (2, None), (3, "c")]
        df = spark_session.createDataFrame(data, ["id", "value"])
        
        validations = {"no_nulls_in": ["id"]}
        assert DataValidator.validate_dataframe(df, validations) == True
        
        validations = {"no_nulls_in": ["value"]}
        with pytest.raises(ValidationError):
            DataValidator.validate_dataframe(df, validations)
    
    def test_validate_column_types(self, test_dataframe):
        column_types = {"id": "bigint", "name": "string"}
        assert DataValidator.validate_column_types(test_dataframe, column_types) == True
        
        column_types = {"id": "string"}
        with pytest.raises(ValidationError):
            DataValidator.validate_column_types(test_dataframe, column_types)
    
    def test_validate_unique_columns(self, spark_session):
        data = [(1, "a"), (2, "b"), (3, "a")]
        df = spark_session.createDataFrame(data, ["id", "value"])
        
        assert DataValidator.validate_unique_columns(df, ["id"]) == True
        
        with pytest.raises(ValidationError):
            DataValidator.validate_unique_columns(df, ["value"])


class TestConfigValidator:
    
    def test_validate_required_keys(self):
        config = {
            "level1": {
                "level2": "value"
            }
        }
        
        assert ConfigValidator.validate_required_keys(config, ["level1.level2"]) == True
        
        with pytest.raises(ValidationError):
            ConfigValidator.validate_required_keys(config, ["missing.key"])
    
    def test_validate_config_types(self):
        config = {
            "string_key": "value",
            "int_key": 42,
            "nested": {
                "bool_key": True
            }
        }
        
        type_specs = {
            "string_key": str,
            "int_key": int,
            "nested.bool_key": bool
        }
        
        assert ConfigValidator.validate_config_types(config, type_specs) == True
        
        type_specs = {"string_key": int}
        with pytest.raises(ValidationError):
            ConfigValidator.validate_config_types(config, type_specs)
    
    def test_validate_config_values(self):
        config = {
            "format": "parquet",
            "batch_size": 100
        }
        
        value_specs = {
            "format": {"allowed": ["parquet", "csv", "json"]},
            "batch_size": {"min": 10, "max": 1000}
        }
        
        assert ConfigValidator.validate_config_values(config, value_specs) == True
        
        value_specs = {"format": {"allowed": ["avro", "orc"]}}
        with pytest.raises(ValidationError):
            ConfigValidator.validate_config_values(config, value_specs)