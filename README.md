# Sessionize - Apache Spark Batch & Streaming Pipeline Framework

A comprehensive Python framework for building scalable data pipelines using Apache Spark, supporting both batch and streaming processing modes.

## Features

- **Dual Processing Modes**: Support for both batch and streaming pipelines
- **Modular Architecture**: Separated concerns with extractor, transformer, and sink components
- **Configuration Management**: Centralized configuration handling
- **Abstract Base Classes**: Extensible design patterns for custom implementations
- **Production Ready**: Logging, error handling, and monitoring capabilities

## Project Structure

```
sessionize/
│
├── src/                      # Source code
│   ├── extractor/           # Data extraction modules
│   ├── transformer/         # Data transformation logic
│   ├── sink/               # Data output/sink modules
│   ├── runner/             # Batch and streaming runners
│   ├── controller/         # Pipeline orchestration
│   ├── config/             # Configuration management
│   ├── utils/              # Utility functions
│   └── common/             # Common components and constants
│
├── tests/                   # Test suite
├── examples/               # Example pipelines
├── configs/                # Configuration files
├── data/                   # Data directories
│   ├── input/             # Input data
│   └── output/            # Output data
└── logs/                   # Application logs
```

## Installation

### Prerequisites

- Python 3.8+
- Apache Spark 3.0+
- Java 8 or 11

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd sessionize
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Install the package in development mode:
```bash
pip install -e .
```

## Quick Start

### Batch Pipeline Example

```python
from src.runner.batch_runner import BatchRunner
from src.config.config_manager import ConfigManager

# Load configuration
config = ConfigManager.load_config("configs/batch_config.yaml")

# Initialize and run batch pipeline
runner = BatchRunner(config)
runner.run()
```

### Streaming Pipeline Example

```python
from src.runner.streaming_runner import StreamingRunner
from src.config.config_manager import ConfigManager

# Load configuration
config = ConfigManager.load_config("configs/streaming_config.yaml")

# Initialize and run streaming pipeline
runner = StreamingRunner(config)
runner.run()
```

## Configuration

Configuration files are stored in the `configs/` directory. Example configuration:

```yaml
spark:
  app_name: "MyPipeline"
  master: "local[*]"
  config:
    spark.sql.shuffle.partitions: 200

pipeline:
  extractor:
    type: "file"
    format: "parquet"
    path: "data/input/"
  
  transformer:
    type: "custom"
    class: "MyTransformer"
  
  sink:
    type: "file"
    format: "parquet"
    path: "data/output/"
    mode: "overwrite"
```

## Creating Custom Components

### Custom Extractor

```python
from src.extractor.base_extractor import BaseExtractor

class MyExtractor(BaseExtractor):
    def extract(self, spark, config):
        # Implementation here
        pass
```

### Custom Transformer

```python
from src.transformer.base_transformer import BaseTransformer

class MyTransformer(BaseTransformer):
    def transform(self, df, config):
        # Implementation here
        pass
```

### Custom Sink

```python
from src.sink.base_sink import BaseSink

class MySink(BaseSink):
    def write(self, df, config):
        # Implementation here
        pass
```

## Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src tests/

# Run specific test file
pytest tests/test_extractor.py
```

## Development

### Code Style

This project uses:
- `black` for code formatting
- `flake8` for linting
- `mypy` for type checking

Run code quality checks:
```bash
# Format code
black src/ tests/

# Check linting
flake8 src/ tests/

# Type checking
mypy src/
```

## Deployment

### Docker

Build and run with Docker:
```bash
docker build -t sessionize .
docker run -v $(pwd)/data:/app/data sessionize
```

### Kubernetes

Deploy to Kubernetes cluster:
```bash
kubectl apply -f k8s/deployment.yaml
```

## Monitoring

The framework includes built-in monitoring capabilities:
- Spark UI: http://localhost:4040 (when running locally)
- Custom metrics exported to monitoring systems
- Structured logging for debugging

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues, questions, or contributions, please open an issue on GitHub.