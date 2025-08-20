#!/usr/bin/env python3
"""
Pipeline Runner

Simple script to run individual pipelines. Similar to logflow approach
where each pipeline can be executed independently for Airflow orchestration.
"""

import sys
import os
import argparse
import importlib
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from src.utils.logger import setup_logging
import logging


class PipelineRunner:
    """Simple pipeline runner for orchestration."""
    
    def __init__(self):
        self.pipelines_dir = Path(__file__).parent / "pipelines"
        self.available_pipelines = self._discover_pipelines()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _discover_pipelines(self):
        """Discover available pipeline modules."""
        pipelines = {}
        
        if not self.pipelines_dir.exists():
            return pipelines
        
        for pipeline_file in self.pipelines_dir.glob("*.py"):
            if pipeline_file.name.startswith("__"):
                continue
            
            pipeline_name = pipeline_file.stem
            pipelines[pipeline_name] = {
                'file': pipeline_file,
                'module': f"pipelines.{pipeline_name}"
            }
        
        return pipelines
    
    def list_pipelines(self):
        """List all available pipelines."""
        print("Available Pipelines:")
        print("=" * 40)
        
        if not self.available_pipelines:
            print("No pipelines found in the pipelines directory.")
            return
        
        for name, info in self.available_pipelines.items():
            print(f"• {name}")
            
            # Try to get description from module docstring
            try:
                module = importlib.import_module(info['module'])
                if module.__doc__:
                    description = module.__doc__.strip().split('\n')[0]
                    print(f"  {description}")
            except Exception:
                pass
        
        print(f"\nTotal: {len(self.available_pipelines)} pipelines")
    
    def run_pipeline(self, pipeline_name, args=None):
        """Run a specific pipeline."""
        if pipeline_name not in self.available_pipelines:
            available = ', '.join(self.available_pipelines.keys())
            raise ValueError(f"Pipeline '{pipeline_name}' not found. Available: {available}")
        
        pipeline_info = self.available_pipelines[pipeline_name]
        
        try:
            # Import the pipeline module
            module = importlib.import_module(pipeline_info['module'])
            
            # Execute the main function with provided args
            if hasattr(module, 'main'):
                self.logger.info(f"Running pipeline: {pipeline_name}")
                
                # Temporarily replace sys.argv for the pipeline
                original_argv = sys.argv
                try:
                    sys.argv = [pipeline_info['file'].name] + (args or [])
                    result = module.main()
                    return result
                finally:
                    sys.argv = original_argv
            else:
                raise AttributeError(f"Pipeline {pipeline_name} does not have a main() function")
                
        except Exception as e:
            self.logger.error(f"Failed to run pipeline {pipeline_name}: {e}")
            raise


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Sessionize Pipeline Runner")
    parser.add_argument("action", choices=["list", "run"], 
                       help="Action to perform")
    parser.add_argument("--pipeline", "-p", 
                       help="Pipeline name to run (required for 'run' action)")
    parser.add_argument("--pipeline-args", nargs="*", 
                       help="Arguments to pass to the pipeline")
    parser.add_argument("--log-level", default="INFO",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Logging level")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging({
        "level": args.log_level,
        "file_logging": True,
        "log_dir": "logs"
    })
    
    runner = PipelineRunner()
    
    try:
        if args.action == "list":
            runner.list_pipelines()
            return 0
        
        elif args.action == "run":
            if not args.pipeline:
                print("Error: --pipeline is required for 'run' action")
                return 1
            
            result = runner.run_pipeline(args.pipeline, args.pipeline_args or [])
            return result if result is not None else 0
        
    except Exception as e:
        logging.error(f"Pipeline runner failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())