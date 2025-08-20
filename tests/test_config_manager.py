import pytest
import json
import tempfile
from pathlib import Path
from src.config.config_manager import ConfigManager


class TestConfigManager:
    
    def test_load_json_config(self):
        config_data = {
            "spark": {"app_name": "TestApp"},
            "pipeline": {"type": "batch"}
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            temp_path = f.name
        
        try:
            loaded_config = ConfigManager.load_config(temp_path)
            assert loaded_config == config_data
        finally:
            Path(temp_path).unlink()
    
    def test_load_yaml_config(self):
        yaml_content = """
spark:
  app_name: TestApp
pipeline:
  type: batch
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            temp_path = f.name
        
        try:
            loaded_config = ConfigManager.load_config(temp_path)
            assert loaded_config['spark']['app_name'] == 'TestApp'
            assert loaded_config['pipeline']['type'] == 'batch'
        finally:
            Path(temp_path).unlink()
    
    def test_load_nonexistent_file(self):
        with pytest.raises(FileNotFoundError):
            ConfigManager.load_config("nonexistent.yaml")
    
    def test_merge_configs(self):
        config1 = {"a": 1, "b": {"c": 2}}
        config2 = {"b": {"d": 3}, "e": 4}
        
        merged = ConfigManager.merge_configs(config1, config2)
        
        assert merged == {"a": 1, "b": {"c": 2, "d": 3}, "e": 4}
    
    def test_validate_config(self):
        config = {
            "spark": {"app_name": "Test"},
            "pipeline": {
                "extractor": {"type": "file"},
                "transformer": {"type": "passthrough"}
            }
        }
        
        assert ConfigManager.validate_config(config, ["spark.app_name"])
        assert ConfigManager.validate_config(config, ["pipeline.extractor.type"])
        assert not ConfigManager.validate_config(config, ["missing.key"])
    
    def test_get_nested_value(self):
        config = {
            "level1": {
                "level2": {
                    "level3": "value"
                }
            }
        }
        
        assert ConfigManager.get_nested_value(config, "level1.level2.level3") == "value"
        assert ConfigManager.get_nested_value(config, "missing.key", "default") == "default"