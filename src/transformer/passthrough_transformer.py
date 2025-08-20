from typing import Any, Dict
from src.transformer.base_transformer import BaseTransformer
import logging


class PassthroughTransformer(BaseTransformer):
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def transform(self, df, config: Dict[str, Any]):
        operations = config.get('operations', [])
        
        result_df = df
        
        for operation in operations:
            if isinstance(operation, dict):
                for op_type, op_value in operation.items():
                    result_df = self._apply_operation(result_df, op_type, op_value)
            elif isinstance(operation, str):
                if operation.startswith('filter:'):
                    filter_expr = operation.replace('filter:', '').strip()
                    result_df = result_df.filter(filter_expr)
                elif operation.startswith('select:'):
                    columns = operation.replace('select:', '').strip().split(',')
                    result_df = result_df.select(*[col.strip() for col in columns])
        
        return result_df
    
    def _apply_operation(self, df, op_type: str, op_value: Any):
        if op_type == 'filter':
            self.logger.info(f"Applying filter: {op_value}")
            return df.filter(op_value)
        elif op_type == 'select':
            self.logger.info(f"Selecting columns: {op_value}")
            return df.select(*op_value)
        elif op_type == 'drop':
            self.logger.info(f"Dropping columns: {op_value}")
            return df.drop(*op_value)
        elif op_type == 'rename':
            for old_name, new_name in op_value.items():
                df = df.withColumnRenamed(old_name, new_name)
            return df
        elif op_type == 'distinct':
            return df.distinct()
        elif op_type == 'limit':
            return df.limit(op_value)
        else:
            self.logger.warning(f"Unknown operation: {op_type}")
            return df
    
    def validate_config(self) -> bool:
        return True