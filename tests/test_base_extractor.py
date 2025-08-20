import pytest
from src.extractor.base_extractor import BaseExtractor


class TestExtractor(BaseExtractor):
    def extract(self, spark, config):
        return spark.createDataFrame([(1, "test")], ["id", "value"])
    
    def validate_config(self):
        return True


class TestBaseExtractor:
    
    def test_extractor_initialization(self):
        config = {"key": "value"}
        extractor = TestExtractor(config)
        
        assert extractor.config == config
    
    def test_extractor_initialization_without_config(self):
        extractor = TestExtractor()
        
        assert extractor.config == {}
    
    def test_get_extractor_type(self):
        extractor = TestExtractor()
        
        assert extractor.get_extractor_type() == "TestExtractor"
    
    def test_abstract_methods_must_be_implemented(self):
        with pytest.raises(TypeError):
            BaseExtractor()