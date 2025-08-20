#!/usr/bin/env python3
"""
Sessionize Main Entry Point

Simple entry point for running different data pipelines, similar to logflow.
You can pass pipeline names as arguments to run specific pipelines.

Usage:
    python main.py --help                              # Show help
    python main.py list                                # List available pipelines
    python main.py run kafka_to_iceberg_pipeline       # Run specific pipeline
    python main.py run batch_file_processor --input-path /data/input --output-path /data/output
    
    # Legacy mode (config-based runner)
    python main.py config --config configs/streaming_config.yaml --mode streaming
"""

import sys
import os
import argparse
import importlib
from pathlib import Path
from typing import List, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from src.utils.logger import setup_logging, LoggerFactory
from src.config.config_manager import ConfigManager
from src.runner.batch_runner import BatchRunner
from src.runner.streaming_runner import StreamingRunner
import logging


class SessionizeRunner:
    """Main runner for Sessionize pipelines."""
    
    def __init__(self):
        self.pipelines_dir = Path(__file__).parent / "pipelines"
        self.available_pipelines = self._discover_pipelines()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _discover_pipelines(self) -> dict:
        """Discover available pipeline modules."""
        pipelines = {}
        
        if not self.pipelines_dir.exists():
            self.logger.warning(f"Pipelines directory not found: {self.pipelines_dir}")
            return pipelines
        
        for pipeline_file in self.pipelines_dir.glob("*.py"):
            if pipeline_file.name.startswith("__"):
                continue
            
            pipeline_name = pipeline_file.stem
            pipelines[pipeline_name] = {
                'file': pipeline_file,
                'module': f"pipelines.{pipeline_name}",
                'path': str(pipeline_file)
            }
        
        return pipelines
    
    def list_pipelines(self) -> None:
        """List all available pipelines with descriptions."""
        print("\n🔧 Sessionize - Available Pipelines")
        print("=" * 50)
        
        if not self.available_pipelines:
            print("❌ No pipelines found in the pipelines/ directory.")
            print("\nTo create a new pipeline:")
            print("  1. Add a new .py file in the pipelines/ directory")
            print("  2. Implement a main() function in your pipeline")
            print("  3. Run 'python main.py list' to see it here")
            return
        
        for name, info in self.available_pipelines.items():
            print(f"\n📋 {name}")
            
            # Try to get description from module docstring
            try:
                module = importlib.import_module(info['module'])
                if module.__doc__:
                    description = module.__doc__.strip().split('\n')[0]
                    print(f"   └─ {description}")
                else:
                    print(f"   └─ Pipeline module: {info['file'].name}")
            except Exception as e:
                print(f"   └─ {info['file'].name} (failed to load: {str(e)})")
        
        print(f"\n✅ Total: {len(self.available_pipelines)} pipelines available")
        
        # Show example usage
        if self.available_pipelines:
            example_pipeline = list(self.available_pipelines.keys())[0]
            print(f"\n💡 Example usage:")
            print(f"   python main.py run {example_pipeline}")
            print(f"   python main.py run {example_pipeline} --help")
    
    def run_pipeline(self, pipeline_name: str, pipeline_args: Optional[List[str]] = None) -> int:
        """Run a specific pipeline with optional arguments."""
        if pipeline_name not in self.available_pipelines:
            available = ', '.join(sorted(self.available_pipelines.keys()))
            print(f"❌ Pipeline '{pipeline_name}' not found.")
            print(f"\n📋 Available pipelines: {available}")
            print(f"\n💡 Run 'python main.py list' for more details")
            return 1
        
        pipeline_info = self.available_pipelines[pipeline_name]
        
        try:
            print(f"🚀 Running pipeline: {pipeline_name}")
            
            # Import the pipeline module
            module = importlib.import_module(pipeline_info['module'])
            
            # Check if pipeline has main function
            if not hasattr(module, 'main'):
                print(f"❌ Pipeline '{pipeline_name}' does not have a main() function")
                print(f"   Please ensure your pipeline implements a main() function")
                return 1
            
            # Execute the main function with provided args
            self.logger.info(f"Executing pipeline: {pipeline_name}")
            
            # Temporarily replace sys.argv for the pipeline
            original_argv = sys.argv.copy()
            try:
                # Set up argv as if the pipeline script was called directly
                sys.argv = [pipeline_info['path']] + (pipeline_args or [])
                result = module.main()
                
                # Handle return value
                if result is None:
                    result = 0
                elif isinstance(result, bool):
                    result = 0 if result else 1
                
                if result == 0:
                    print(f"✅ Pipeline '{pipeline_name}' completed successfully")
                else:
                    print(f"❌ Pipeline '{pipeline_name}' failed with exit code {result}")
                
                return result
                
            finally:
                sys.argv = original_argv
                
        except KeyboardInterrupt:
            print(f"\n⏹️  Pipeline '{pipeline_name}' interrupted by user")
            return 130
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {e}", exc_info=True)
            print(f"❌ Pipeline '{pipeline_name}' failed: {e}")
            return 1


def run_config_based_pipeline(args) -> int:
    """Legacy function to run config-based pipelines (original main functionality)."""
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
        return 0
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return 1
    finally:
        if 'runner' in locals():
            runner.stop()
            logger.info("Pipeline stopped")


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser."""
    parser = argparse.ArgumentParser(
        description="Sessionize - Data Pipeline Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s list                                    # List available pipelines
  %(prog)s run kafka_to_iceberg_pipeline           # Run Kafka to Iceberg pipeline
  %(prog)s run batch_file_processor --help         # Show pipeline-specific help
  
  # Legacy config-based mode
  %(prog)s config --config configs/streaming_config.yaml --mode streaming

For pipeline-specific arguments, use -- to separate them:
  %(prog)s run batch_file_processor -- --input-path /data --output-path /results
        """
    )
    
    # Add subcommands
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List available pipelines')
    
    # Run command
    run_parser = subparsers.add_parser('run', help='Run a specific pipeline')
    run_parser.add_argument('pipeline_name', help='Name of the pipeline to run')
    run_parser.add_argument('pipeline_args', nargs=argparse.REMAINDER, 
                           help='Arguments to pass to the pipeline')
    
    # Config-based command (legacy)
    config_parser = subparsers.add_parser('config', help='Run config-based pipeline (legacy mode)')
    config_parser.add_argument(
        "--config", "-c", type=str, required=True,
        help="Path to configuration file (YAML or JSON)"
    )
    config_parser.add_argument(
        "--mode", "-m", type=str, choices=["batch", "streaming"], default="batch",
        help="Pipeline mode: batch or streaming (default: batch)"
    )
    config_parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable verbose logging"
    )
    
    # Global options
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Set logging level (default: INFO)')
    parser.add_argument('--quiet', '-q', action='store_true',
                       help='Suppress non-error output')
    
    return parser


def main() -> int:
    """Main entry point for Sessionize."""
    
    # Handle case where no arguments provided
    if len(sys.argv) == 1:
        sys.argv.append('--help')
    
    parser = create_parser()
    args = parser.parse_args()
    
    # Setup logging
    log_config = {
        "level": args.log_level,
        "file_logging": True,
        "log_dir": "logs"
    }
    
    if not args.quiet:
        setup_logging(log_config)
    else:
        # Minimal logging for quiet mode
        logging.basicConfig(level=getattr(logging, args.log_level))
    
    try:
        if args.command == 'list':
            runner = SessionizeRunner()
            runner.list_pipelines()
            return 0
        
        elif args.command == 'run':
            runner = SessionizeRunner()
            return runner.run_pipeline(args.pipeline_name, args.pipeline_args)
        
        elif args.command == 'config':
            return run_config_based_pipeline(args)
        
        else:
            # This should not happen due to argparse, but just in case
            parser.print_help()
            return 1
            
    except KeyboardInterrupt:
        if not args.quiet:
            print("\n⏹️  Interrupted by user")
        return 130
    except Exception as e:
        logging.error(f"Sessionize runner failed: {e}", exc_info=True)
        if not args.quiet:
            print(f"❌ Sessionize failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
