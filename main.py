#!/usr/bin/env python3

import sys
import argparse
from pathlib import Path
from src.config.config_manager import ConfigManager
from src.utils.logger import setup_logging, LoggerFactory
from src.runner.batch_runner import BatchRunner
from src.runner.streaming_runner import StreamingRunner


def main():
    parser = argparse.ArgumentParser(
        description="Sessionize - Apache Spark Pipeline Framework"
    )
    parser.add_argument(
        "--config",
        "-c",
        type=str,
        required=True,
        help="Path to configuration file (YAML or JSON)"
    )
    parser.add_argument(
        "--mode",
        "-m",
        type=str,
        choices=["batch", "streaming"],
        default="batch",
        help="Pipeline mode: batch or streaming (default: batch)"
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    try:
        config = ConfigManager.load_config(args.config)
        
        if args.verbose:
            if 'logging' not in config:
                config['logging'] = {}
            config['logging']['level'] = 'DEBUG'
        
        setup_logging(config.get('logging'))
        logger = LoggerFactory.get_logger("Sessionize", config.get('logging'))
        
        logger.info(f"Starting Sessionize in {args.mode} mode")
        logger.info(f"Configuration loaded from: {args.config}")
        
        if args.mode == "batch":
            runner = BatchRunner(config)
        else:
            runner = StreamingRunner(config)
        
        runner.run()
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if 'runner' in locals():
            runner.stop()
            logger.info("Pipeline stopped")


if __name__ == "__main__":
    main()
