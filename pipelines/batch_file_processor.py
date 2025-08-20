#!/usr/bin/env python3
"""
Batch File Processing Pipeline

A standalone pipeline runner for batch processing files with various transformations.
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


class BatchFileProcessor:
    """Batch file processing pipeline runner."""
    
    def __init__(self, input_path, output_path, input_format="parquet", 
                 output_format="parquet", filter_condition=None):
        self.input_path = input_path
        self.output_path = output_path
        self.input_format = input_format
        self.output_format = output_format
        self.filter_condition = filter_condition
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get_pipeline_config(self):
        """Get the pipeline configuration."""
        config = {
            "spark": {
                "app_name": "BatchFileProcessor",
                "master": "local[*]",
                "config": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    "spark.sql.shuffle.partitions": "200"
                }
            },
            "pipeline": {
                "type": "batch",
                "extractor": {
                    "type": "file",
                    "path": self.input_path,
                    "format": self.input_format
                },
                "sink": {
                    "type": "file",
                    "path": self.output_path,
                    "format": self.output_format,
                    "mode": "overwrite"
                }
            },
            "logging": {
                "level": "INFO",
                "file_logging": True,
                "log_dir": "logs"
            }
        }
        
        # Add transformer if filter condition is specified
        if self.filter_condition:
            config["pipeline"]["transformer"] = {
                "type": "json",
                "operations": [
                    {"filter": self.filter_condition}
                ]
            }
        
        return config
    
    def run(self):
        """Run the pipeline."""
        try:
            config = self.get_pipeline_config()
            
            self.logger.info(f"Starting batch file processing pipeline")
            self.logger.info(f"Input: {self.input_path} ({self.input_format})")
            self.logger.info(f"Output: {self.output_path} ({self.output_format})")
            if self.filter_condition:
                self.logger.info(f"Filter: {self.filter_condition}")
            
            controller = PipelineController(config=config)
            
            if not controller.validate():
                raise Exception("Pipeline configuration validation failed")
            
            controller.execute()
            
            self.logger.info("Batch processing completed successfully")
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise


def main():
    """Main entry point for the pipeline."""
    parser = argparse.ArgumentParser(description="Batch File Processing Pipeline")
    parser.add_argument("input_path", help="Input file/directory path")
    parser.add_argument("output_path", help="Output file/directory path")
    parser.add_argument("--input-format", default="parquet",
                       choices=["parquet", "json", "csv", "delta"],
                       help="Input file format")
    parser.add_argument("--output-format", default="parquet",
                       choices=["parquet", "json", "csv", "delta"],
                       help="Output file format")
    parser.add_argument("--filter", dest="filter_condition",
                       help="Filter condition (e.g., 'age > 25')")
    parser.add_argument("--log-level", default="INFO",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Logging level")
    
    args = parser.parse_args()
    
    # Validate paths
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
    pipeline = BatchFileProcessor(
        input_path=args.input_path,
        output_path=args.output_path,
        input_format=args.input_format,
        output_format=args.output_format,
        filter_condition=args.filter_condition
    )
    
    try:
        pipeline.run()
        return 0
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())