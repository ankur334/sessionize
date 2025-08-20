from typing import Any, Dict, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from src.common.exceptions import ValidationError


class DataValidator:
    
    @staticmethod
    def validate_dataframe(df: DataFrame, validations: Dict[str, Any]) -> bool:
        if not df:
            raise ValidationError("DataFrame is None")
        
        if 'min_rows' in validations:
            row_count = df.count()
            if row_count < validations['min_rows']:
                raise ValidationError(f"DataFrame has {row_count} rows, expected at least {validations['min_rows']}")
        
        if 'required_columns' in validations:
            missing_cols = set(validations['required_columns']) - set(df.columns)
            if missing_cols:
                raise ValidationError(f"Missing required columns: {missing_cols}")
        
        if 'schema' in validations:
            expected_schema = validations['schema']
            if isinstance(expected_schema, StructType):
                if df.schema != expected_schema:
                    raise ValidationError("DataFrame schema does not match expected schema")
        
        if 'no_nulls_in' in validations:
            for col in validations['no_nulls_in']:
                null_count = df.filter(df[col].isNull()).count()
                if null_count > 0:
                    raise ValidationError(f"Column {col} contains {null_count} null values")
        
        return True
    
    @staticmethod
    def validate_column_types(df: DataFrame, column_types: Dict[str, str]) -> bool:
        schema_dict = {field.name: field.dataType.simpleString() for field in df.schema.fields}
        
        for col_name, expected_type in column_types.items():
            if col_name not in schema_dict:
                raise ValidationError(f"Column {col_name} not found in DataFrame")
            
            actual_type = schema_dict[col_name]
            if actual_type != expected_type:
                raise ValidationError(f"Column {col_name} has type {actual_type}, expected {expected_type}")
        
        return True
    
    @staticmethod
    def validate_unique_columns(df: DataFrame, columns: List[str]) -> bool:
        for col in columns:
            total_count = df.count()
            distinct_count = df.select(col).distinct().count()
            
            if total_count != distinct_count:
                raise ValidationError(f"Column {col} contains duplicate values")
        
        return True
    
    @staticmethod
    def validate_value_ranges(df: DataFrame, ranges: Dict[str, Dict[str, Any]]) -> bool:
        for col_name, range_spec in ranges.items():
            if 'min' in range_spec:
                min_val = df.agg({col_name: 'min'}).collect()[0][0]
                if min_val < range_spec['min']:
                    raise ValidationError(f"Column {col_name} has minimum value {min_val}, expected >= {range_spec['min']}")
            
            if 'max' in range_spec:
                max_val = df.agg({col_name: 'max'}).collect()[0][0]
                if max_val > range_spec['max']:
                    raise ValidationError(f"Column {col_name} has maximum value {max_val}, expected <= {range_spec['max']}")
        
        return True
    
    @staticmethod
    def validate_allowed_values(df: DataFrame, allowed_values: Dict[str, List[Any]]) -> bool:
        for col_name, values in allowed_values.items():
            distinct_values = df.select(col_name).distinct().collect()
            distinct_values = [row[0] for row in distinct_values]
            
            invalid_values = set(distinct_values) - set(values)
            if invalid_values:
                raise ValidationError(f"Column {col_name} contains invalid values: {invalid_values}")
        
        return True


class ConfigValidator:
    
    @staticmethod
    def validate_required_keys(config: Dict[str, Any], required_keys: List[str]) -> bool:
        for key in required_keys:
            if '.' in key:
                keys = key.split('.')
                current = config
                for k in keys:
                    if not isinstance(current, dict) or k not in current:
                        raise ValidationError(f"Missing required configuration key: {key}")
                    current = current[k]
            else:
                if key not in config:
                    raise ValidationError(f"Missing required configuration key: {key}")
        
        return True
    
    @staticmethod
    def validate_config_types(config: Dict[str, Any], type_specs: Dict[str, type]) -> bool:
        for key_path, expected_type in type_specs.items():
            keys = key_path.split('.')
            current = config
            
            for k in keys[:-1]:
                if k not in current:
                    raise ValidationError(f"Missing configuration key: {key_path}")
                current = current[k]
            
            final_key = keys[-1]
            if final_key not in current:
                raise ValidationError(f"Missing configuration key: {key_path}")
            
            if not isinstance(current[final_key], expected_type):
                raise ValidationError(f"Configuration key {key_path} has incorrect type. Expected {expected_type.__name__}")
        
        return True
    
    @staticmethod
    def validate_config_values(config: Dict[str, Any], value_specs: Dict[str, Any]) -> bool:
        for key_path, spec in value_specs.items():
            keys = key_path.split('.')
            current = config
            
            for k in keys[:-1]:
                current = current.get(k, {})
            
            value = current.get(keys[-1])
            
            if 'allowed' in spec:
                if value not in spec['allowed']:
                    raise ValidationError(f"Configuration key {key_path} has invalid value: {value}. Allowed: {spec['allowed']}")
            
            if 'min' in spec:
                if value < spec['min']:
                    raise ValidationError(f"Configuration key {key_path} value {value} is less than minimum {spec['min']}")
            
            if 'max' in spec:
                if value > spec['max']:
                    raise ValidationError(f"Configuration key {key_path} value {value} is greater than maximum {spec['max']}")
        
        return True