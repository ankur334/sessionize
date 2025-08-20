import pytest
from src.runner.base_runner import BaseRunner


class TestRunner(BaseRunner):
    def initialize_spark(self):
        pass
    
    def run(self):
        pass
    
    def stop(self):
        pass


class TestBaseRunner:
    
    def test_runner_initialization(self):
        config = {
            "spark": {"app_name": "Test"},
            "pipeline": {
                "extractor": {"type": "file"},
                "transformer": {"type": "passthrough"},
                "sink": {"type": "file"}
            }
        }
        runner = TestRunner(config)
        
        assert runner.config == config
        assert runner.spark is None
    
    def test_validate_pipeline_config_valid(self):
        config = {
            "pipeline": {
                "extractor": {"type": "file"},
                "transformer": {"type": "passthrough"},
                "sink": {"type": "file"}
            }
        }
        runner = TestRunner(config)
        
        assert runner.validate_pipeline_config() == True
    
    def test_validate_pipeline_config_missing_extractor(self):
        config = {
            "pipeline": {
                "transformer": {"type": "passthrough"},
                "sink": {"type": "file"}
            }
        }
        runner = TestRunner(config)
        
        assert runner.validate_pipeline_config() == False
    
    def test_get_spark_config(self):
        spark_config = {"app_name": "Test", "master": "local"}
        config = {"spark": spark_config, "pipeline": {}}
        runner = TestRunner(config)
        
        assert runner.get_spark_config() == spark_config
    
    def test_get_spark_config_missing(self):
        config = {"pipeline": {}}
        runner = TestRunner(config)
        
        assert runner.get_spark_config() == {}
    
    def test_get_pipeline_config(self):
        pipeline_config = {"extractor": {"type": "file"}}
        config = {"spark": {}, "pipeline": pipeline_config}
        runner = TestRunner(config)
        
        assert runner.get_pipeline_config() == pipeline_config