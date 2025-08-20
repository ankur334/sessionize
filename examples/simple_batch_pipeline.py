import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from src.runner.batch_runner import BatchRunner
from src.config.config_manager import ConfigManager
from src.utils.logger import setup_logging


def main():
    config_path = "configs/batch_config.yaml"
    
    config = ConfigManager.load_config(config_path)
    
    setup_logging(config.get('logging'))
    
    runner = BatchRunner(config)
    
    try:
        runner.run()
    finally:
        runner.stop()


if __name__ == "__main__":
    main()