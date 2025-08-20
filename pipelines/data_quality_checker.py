#!/usr/bin/env python3
"""
Data Quality Checker Pipeline

A standalone pipeline runner for data quality validation and reporting.
Can be executed directly or orchestrated via Airflow.
"""

import sys
import os
import argparse
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.controller.pipeline_controller import PipelineController
from src.utils.logger import setup_logging
import logging


class DataQualityChecker:
    """Data quality checking pipeline runner."""
    
    def __init__(self, input_path, output_path, input_format="parquet"):
        self.input_path = input_path
        self.output_path = output_path
        self.input_format = input_format
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get_pipeline_config(self):
        """Get the pipeline configuration with data quality checks."""
        return {
            "spark": {
                "app_name": "DataQualityChecker",
                "master": "local[*]",
                "config": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true"
                }
            },
            "pipeline": {
                "type": "batch",
                "extractor": {
                    "type": "file",
                    "path": self.input_path,
                    "format": self.input_format
                },
                "transformer": {
                    "type": "json",
                    "operations": [
                        # Data quality checks as transformations
                        {"filter": "id IS NOT NULL"},  # Remove null IDs
                        {"filter": "age >= 0 AND age <= 150"},  # Valid age range
                        {"filter": "email RLIKE '^[^@]+@[^@]+\\.[^@]+$'"}  # Valid email format
                    ]
                },
                "sink": {
                    "type": "file",
                    "path": self.output_path,
                    "format": "parquet",
                    "mode": "overwrite"
                }
            },
            "logging": {
                "level": "INFO",
                "file_logging": True,
                "log_dir": "logs"
            }
        }
    
    def run(self):
        """Run the data quality pipeline."""
        try:
            config = self.get_pipeline_config()
            
            self.logger.info(f"Starting data quality checking pipeline")
            self.logger.info(f"Input: {self.input_path} ({self.input_format})")
            self.logger.info(f"Output: {self.output_path}")
            
            controller = PipelineController(config=config)
            
            if not controller.validate():
                raise Exception("Pipeline configuration validation failed")
            
            controller.execute()
            
            self.logger.info("Data quality checking completed successfully")
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise


def main():
    """Main entry point for the pipeline."""
    parser = argparse.ArgumentParser(description="Data Quality Checker Pipeline")
    parser.add_argument("input_path", help="Input file/directory path")
    parser.add_argument("output_path", help="Output file/directory path for clean data")
    parser.add_argument("--input-format", default="parquet",
                       choices=["parquet", "json", "csv"],
                       help="Input file format")
    parser.add_argument("--log-level", default="INFO",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Logging level")
    
    args = parser.parse_args()
    
    # Validate input path
    if not Path(args.input_path).exists():
        print(f"Error: Input path '{args.input_path}' does not exist")
        return 1
    
    # Setup logging
    setup_logging({
        "level": args.log_level,
        "file_logging": True,
        "log_dir": "logs"
    })
    
    # Create and run pipeline
    pipeline = DataQualityChecker(
        input_path=args.input_path,
        output_path=args.output_path,
        input_format=args.input_format
    )
    
    try:
        pipeline.run()
        return 0
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())