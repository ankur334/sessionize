import pytest
from src.transformer.base_transformer import BaseTransformer, ChainedTransformer


class TestTransformer(BaseTransformer):
    def transform(self, df, config):
        return df.filter("id > 0")
    
    def validate_config(self):
        return True


class AddColumnTransformer(BaseTransformer):
    def transform(self, df, config):
        from pyspark.sql.functions import lit
        return df.withColumn("new_col", lit("value"))
    
    def validate_config(self):
        return True


class TestBaseTransformer:
    
    def test_transformer_initialization(self):
        config = {"key": "value"}
        transformer = TestTransformer(config)
        
        assert transformer.config == config
    
    def test_get_transformer_type(self):
        transformer = TestTransformer()
        
        assert transformer.get_transformer_type() == "TestTransformer"
    
    def test_chain_transformers(self):
        transformer1 = TestTransformer()
        transformer2 = AddColumnTransformer()
        
        chained = transformer1.chain(transformer2)
        
        assert isinstance(chained, ChainedTransformer)
        assert len(chained.transformers) == 2
    
    def test_chained_transformer_transform(self, spark_session):
        data = [(1, "a"), (2, "b"), (0, "c")]
        df = spark_session.createDataFrame(data, ["id", "value"])
        
        transformer1 = TestTransformer()
        transformer2 = AddColumnTransformer()
        chained = ChainedTransformer([transformer1, transformer2])
        
        result = chained.transform(df, {})
        
        assert result.count() == 2
        assert "new_col" in result.columns
    
    def test_chained_transformer_validate_config(self):
        transformer1 = TestTransformer()
        transformer2 = AddColumnTransformer()
        chained = ChainedTransformer([transformer1, transformer2])
        
        assert chained.validate_config() == True