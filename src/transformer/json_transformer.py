from typing import Any, Dict, Optional, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType, ArrayType, MapType
from src.transformer.base_transformer import BaseTransformer
from src.common.exceptions import TransformationError
import logging
import json


class JsonTransformer(BaseTransformer):
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.schema = self._build_schema()
        self.value_column = config.get('value_column', 'value')
        self.add_metadata = config.get('add_metadata', True)
        self.timestamp_column = config.get('timestamp_column', None)
        self.timestamp_format = config.get('timestamp_format', 'yyyy-MM-dd HH:mm:ss')
    
    def transform(self, df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        try:
            self.logger.info("Starting JSON transformation")
            
            if self.value_column not in df.columns:
                raise TransformationError(f"Value column '{self.value_column}' not found in DataFrame")
            
            if self.schema:
                parsed_df = df.withColumn("parsed_data", from_json(col(self.value_column), self.schema))
                result_df = parsed_df.select("*", "parsed_data.*").drop("parsed_data")
            else:
                self.logger.warning("No schema defined, attempting to infer JSON structure")
                from pyspark.sql.functions import get_json_object
                
                json_fields = config.get('json_fields', self.config.get('fields', []))
                if json_fields:
                    result_df = df
                    for field in json_fields:
                        field_name = field if isinstance(field, str) else field.get('name')
                        json_path = f"$.{field_name}"
                        result_df = result_df.withColumn(field_name, get_json_object(col(self.value_column), json_path))
                else:
                    result_df = df
            
            if self.add_metadata:
                result_df = result_df.withColumn("processing_time", current_timestamp())
            
            if self.timestamp_column:
                if self.timestamp_column in result_df.columns:
                    result_df = result_df.withColumn(
                        self.timestamp_column,
                        to_timestamp(col(self.timestamp_column), self.timestamp_format)
                    )
            
            operations = config.get('operations', self.config.get('operations', []))
            result_df = self._apply_operations(result_df, operations)
            
            columns_to_drop = config.get('drop_columns', self.config.get('drop_columns', []))
            if columns_to_drop:
                result_df = result_df.drop(*columns_to_drop)
            
            self.logger.info("JSON transformation completed successfully")
            return result_df
            
        except Exception as e:
            self.logger.error(f"JSON transformation failed: {e}")
            raise TransformationError(f"Failed to transform JSON data: {e}")
    
    def _build_schema(self) -> Optional[StructType]:
        schema_config = self.config.get('schema', None)
        
        if not schema_config:
            return None
        
        if isinstance(schema_config, str):
            try:
                schema_dict = json.loads(schema_config)
                return StructType.fromJson(schema_dict)
            except json.JSONDecodeError:
                self.logger.error("Invalid JSON schema string")
                return None
        
        if isinstance(schema_config, dict):
            if 'fields' in schema_config:
                return StructType.fromJson(schema_config)
            else:
                return self._build_schema_from_config(schema_config)
        
        if isinstance(schema_config, list):
            fields = []
            for field_config in schema_config:
                field = self._create_struct_field(field_config)
                if field:
                    fields.append(field)
            return StructType(fields) if fields else None
        
        return None
    
    def _build_schema_from_config(self, schema_config: Dict[str, str]) -> StructType:
        fields = []
        for field_name, field_type in schema_config.items():
            spark_type = self._get_spark_type(field_type)
            if spark_type:
                fields.append(StructField(field_name, spark_type, True))
        return StructType(fields) if fields else None
    
    def _create_struct_field(self, field_config: Dict[str, Any]) -> Optional[StructField]:
        if isinstance(field_config, dict):
            name = field_config.get('name')
            type_str = field_config.get('type', 'string')
            nullable = field_config.get('nullable', True)
            
            if not name:
                return None
            
            spark_type = self._get_spark_type(type_str)
            if spark_type:
                return StructField(name, spark_type, nullable)
        
        return None
    
    def _get_spark_type(self, type_str: str):
        type_mapping = {
            'string': StringType(),
            'int': IntegerType(),
            'integer': IntegerType(),
            'double': DoubleType(),
            'float': DoubleType(),
            'boolean': BooleanType(),
            'bool': BooleanType(),
            'timestamp': TimestampType(),
            'array<string>': ArrayType(StringType()),
            'map<string,string>': MapType(StringType(), StringType())
        }
        
        return type_mapping.get(type_str.lower(), StringType())
    
    def _apply_operations(self, df: DataFrame, operations: List[Dict[str, Any]]) -> DataFrame:
        for operation in operations:
            if isinstance(operation, dict):
                if 'filter' in operation:
                    df = df.filter(operation['filter'])
                elif 'select' in operation:
                    df = df.select(*operation['select'])
                elif 'watermark' in operation:
                    parts = operation['watermark'].split(',')
                    if len(parts) == 2:
                        column = parts[0].strip()
                        delay = parts[1].strip()
                        df = df.withWatermark(column, delay)
                elif 'groupBy' in operation:
                    group_cols = operation['groupBy']
                    agg_dict = operation.get('agg', {})
                    if agg_dict:
                        from pyspark.sql import functions as F
                        agg_exprs = []
                        for col_name, agg_func in agg_dict.items():
                            if agg_func == 'count':
                                agg_exprs.append(F.count(col_name).alias(f"{col_name}_count"))
                            elif agg_func == 'sum':
                                agg_exprs.append(F.sum(col_name).alias(f"{col_name}_sum"))
                            elif agg_func == 'avg':
                                agg_exprs.append(F.avg(col_name).alias(f"{col_name}_avg"))
                            elif agg_func == 'max':
                                agg_exprs.append(F.max(col_name).alias(f"{col_name}_max"))
                            elif agg_func == 'min':
                                agg_exprs.append(F.min(col_name).alias(f"{col_name}_min"))
                        
                        if agg_exprs:
                            df = df.groupBy(*group_cols).agg(*agg_exprs)
        
        return df
    
    def validate_config(self) -> bool:
        if not self.config:
            return True
        
        if 'schema' in self.config and self.config['schema']:
            if not self._build_schema():
                self.logger.error("Invalid schema configuration")
                return False
        
        return True