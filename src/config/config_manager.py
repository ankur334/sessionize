import os
import json
from typing import Any, Dict, Optional
from pathlib import Path


class ConfigManager:
    
    @staticmethod
    def load_config(config_path: str) -> Dict[str, Any]:
        config_path = Path(config_path)
        
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        if config_path.suffix == '.json':
            return ConfigManager._load_json(config_path)
        elif config_path.suffix in ['.yaml', '.yml']:
            return ConfigManager._load_yaml(config_path)
        else:
            raise ValueError(f"Unsupported configuration file format: {config_path.suffix}")
    
    @staticmethod
    def _load_json(path: Path) -> Dict[str, Any]:
        with open(path, 'r') as f:
            return json.load(f)
    
    @staticmethod
    def _load_yaml(path: Path) -> Dict[str, Any]:
        try:
            import yaml
            with open(path, 'r') as f:
                return yaml.safe_load(f)
        except ImportError:
            raise ImportError("PyYAML is required to load YAML configuration files. Install it with: pip install pyyaml")
    
    @staticmethod
    def merge_configs(*configs: Dict[str, Any]) -> Dict[str, Any]:
        result = {}
        for config in configs:
            result = ConfigManager._deep_merge(result, config)
        return result
    
    @staticmethod
    def _deep_merge(dict1: Dict[str, Any], dict2: Dict[str, Any]) -> Dict[str, Any]:
        result = dict1.copy()
        for key, value in dict2.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = ConfigManager._deep_merge(result[key], value)
            else:
                result[key] = value
        return result
    
    @staticmethod
    def validate_config(config: Dict[str, Any], required_keys: list) -> bool:
        for key in required_keys:
            if '.' in key:
                keys = key.split('.')
                current = config
                for k in keys:
                    if k not in current:
                        return False
                    current = current[k]
            else:
                if key not in config:
                    return False
        return True
    
    @staticmethod
    def get_nested_value(config: Dict[str, Any], key_path: str, default: Any = None) -> Any:
        keys = key_path.split('.')
        current = config
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        return current