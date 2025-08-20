# Sessionize - Enterprise Data Pipeline Framework

A production-ready Python framework for building scalable data pipelines using **Apache Spark**, **Apache Kafka**, and **Apache Iceberg**. Sessionize supports both batch and real-time streaming processing with a modular architecture and simple command-line interface, similar to [logflow](https://github.com/ankur334/logflow).

![Python](https://img.shields.io/badge/python-3.8%2B-blue)
![Spark](https://img.shields.io/badge/spark-3.5.0-orange)
![Kafka](https://img.shields.io/badge/kafka-latest-red)
![Iceberg](https://img.shields.io/badge/iceberg-1.4.3-green)
![License](https://img.shields.io/badge/license-MIT-blue)

## 🚀 Features

### Core Capabilities
- **🔄 Dual Processing Modes**: Unified framework for both batch and streaming pipelines
- **🧩 Modular Architecture**: Pluggable extractors, transformers, and sinks
- **🚀 Simple CLI Interface**: Run pipelines with `python main.py run pipeline_name`
- **🎯 Airflow Ready**: Individual executable pipelines for orchestration
- **🏭 Production Ready**: Comprehensive logging, error handling, and monitoring
- **🔌 Extensible Design**: Abstract base classes for custom implementations

### Technology Stack
- **Data Processing**: Apache Spark 3.5.0 with PySpark
- **Stream Processing**: Apache Kafka with real-time data ingestion
- **Data Lake**: Apache Iceberg tables with ACID transactions
- **Data Formats**: Parquet, Delta Lake, JSON, Avro support
- **Orchestration**: Custom pipeline controller with dependency management

### Built-in Components

| Component Type | Available Implementations |
|----------------|---------------------------|
| **Extractors** | File (Parquet, JSON, CSV), Kafka Streaming, Database |
| **Transformers** | JSON Parser, SQL Transformer, Custom Python logic |
| **Sinks** | File Writer, Iceberg Tables, Kafka Producer, Console |

## 📁 Project Architecture

```
sessionize/
├── 🚀 main.py                 # Main CLI entry point (similar to logflow)
├── 🔧 src/                    # Core framework source code
│   ├── extractor/            # Data ingestion modules
│   │   ├── kafka_extractor.py    # Kafka streaming reader
│   │   ├── file_extractor.py     # File-based data reader  
│   │   └── base_extractor.py     # Abstract base class
│   ├── transformer/          # Data transformation logic
│   │   ├── json_transformer.py   # JSON parsing & schema validation
│   │   ├── passthrough_transformer.py    # Pass-through transformer
│   │   └── base_transformer.py   # Abstract base class
│   ├── sink/                 # Data output modules
│   │   ├── iceberg_sink.py       # Apache Iceberg table writer
│   │   ├── file_sink.py          # File system writer
│   │   └── base_sink.py          # Abstract base class
│   ├── runner/               # Pipeline execution engines
│   │   ├── streaming_runner.py   # Spark Structured Streaming
│   │   ├── batch_runner.py       # Spark batch processing
│   │   └── base_runner.py        # Abstract base class
│   ├── controller/           # Pipeline orchestration
│   ├── config/               # Configuration management
│   ├── utils/                # Utility functions
│   └── common/               # Shared components
├── 🏭 pipelines/              # Individual executable pipelines
│   ├── kafka_to_iceberg_pipeline.py   # Kafka → Iceberg streaming
│   ├── batch_file_processor.py        # Batch file processing
│   └── data_quality_checker.py        # Data quality validation
├── 📋 examples/               # Production-ready pipeline examples
│   ├── kafka_to_iceberg_streaming.py  # Complete streaming pipeline
│   └── batch_processing_example.py    # Batch processing example
├── 🛠️ scripts/                # Utility & development scripts
│   ├── kafka-producer.py      # Test data generator
│   ├── start-kafka.sh         # Local Kafka cluster setup
│   ├── verify_iceberg_data.py # Data validation tools
│   └── validate-setup.py      # Environment validation
├── ⚙️ configs/               # Pipeline configurations
├── 📊 data/                  # Data directories (input/output)
├── 🧪 tests/                 # Comprehensive test suite
├── 📝 logs/                  # Application logs
└── 🐳 docker-compose.yml     # Kafka infrastructure
```

## ⚡ Quick Start

### Prerequisites
- **Python 3.8+** with pip and venv
- **Java 8 or 11** (for Spark)
- **Docker & Docker Compose** (for Kafka)
- **8GB+ RAM** recommended for local development

### Installation

```bash
# 1. Clone repository
git clone <repository-url>
cd sessionize

# 2. Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Install package in development mode
pip install -e .

# 5. Validate setup
python scripts/validate-setup.py
```

## 🚀 Command Line Interface

Sessionize provides a simple CLI similar to [logflow](https://github.com/ankur334/logflow) for running individual pipelines:

### List Available Pipelines
```bash
python main.py list
```
Output:
```
🔧 Sessionize - Available Pipelines
==================================================

📋 kafka_to_iceberg_pipeline
   └─ Kafka to Iceberg Streaming Pipeline

📋 batch_file_processor
   └─ Batch File Processing Pipeline

📋 data_quality_checker
   └─ Data Quality Checker Pipeline

✅ Total: 3 pipelines available
```

### Run Individual Pipelines
```bash
# Basic usage
python main.py run <pipeline_name>

# With arguments
python main.py run batch_file_processor data/input/sample.csv /tmp/output --input-format csv

# Get pipeline-specific help
python main.py run kafka_to_iceberg_pipeline --help

# Test mode (streaming pipelines)
python main.py run kafka_to_iceberg_pipeline --test-mode --kafka-topic my-topic
```

### Legacy Configuration Mode
```bash
# Run with YAML/JSON configuration files
python main.py config --config configs/streaming_config.yaml --mode streaming
python main.py config --config configs/batch_config.yaml --mode batch
```

### Airflow Integration
Each pipeline in the `pipelines/` directory can be executed independently, making them perfect for Airflow DAGs:

```python
# In your Airflow DAG
from airflow.operators.bash import BashOperator

kafka_pipeline = BashOperator(
    task_id='kafka_to_iceberg',
    bash_command='python /path/to/sessionize/main.py run kafka_to_iceberg_pipeline --test-mode',
    dag=dag
)

batch_pipeline = BashOperator(
    task_id='batch_processing',
    bash_command='python /path/to/sessionize/main.py run batch_file_processor /data/input /data/output',
    dag=dag
)
```

## 🔥 Complete Examples

### 📡 Real-time Kafka to Iceberg Pipeline

Stream JSON events from Kafka to Iceberg data lake with automatic schema evolution:

```bash
# 1. Start Kafka infrastructure
./scripts/start-kafka.sh

# 2. Generate test streaming data
python scripts/kafka-producer.py --num-events 1000 --rate 50 &

# 3. Run streaming pipeline (new CLI interface)
python main.py run kafka_to_iceberg_pipeline --test-mode

# 4. Verify data ingestion
python scripts/verify_iceberg_data.py
```

**Pipeline Features:**
- Real-time JSON schema validation and parsing
- Automatic timestamp parsing with timezone handling  
- Data partitioning by event type
- Exactly-once processing guarantees
- Schema evolution support

### 📊 Batch Data Processing

Process large datasets with optimized Spark operations:

```bash
# 1. Run batch processing pipeline (new CLI interface)
python main.py run batch_file_processor data/input/sample.csv /tmp/output --input-format csv

# 2. Verify processed output
python scripts/verify_output.py
```

## 🏗️ Pipeline Configuration

### Streaming Configuration Example

```yaml
# kafka_to_iceberg_config.yaml
spark:
  app_name: "KafkaToIcebergStreaming"
  master: "local[*]"
  packages:
    - "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    - "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3"
  config:
    spark.sql.extensions: "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    spark.sql.catalog.local: "org.apache.iceberg.spark.SparkCatalog"
    spark.sql.catalog.local.type: "hadoop"
    spark.sql.catalog.local.warehouse: "/tmp/iceberg_warehouse"

pipeline:
  type: "streaming"
  
  extractor:
    type: "kafka"
    kafka.bootstrap.servers: "localhost:9092"
    subscribe: "events-topic"
    startingOffsets: "latest"
    maxOffsetsPerTrigger: 10000
  
  transformer:
    type: "json"
    schema:
      - {name: "event_id", type: "string", nullable: false}
      - {name: "event_type", type: "string", nullable: false}  
      - {name: "user_id", type: "string", nullable: false}
      - {name: "timestamp", type: "timestamp", nullable: false}
      - {name: "properties", type: "map<string,string>", nullable: true}
    timestamp_format: "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
    operations:
      - {filter: "event_type != 'heartbeat'"}
      - {watermark: "timestamp, 10 minutes"}
  
  sink:
    type: "iceberg"
    catalog: "local"
    database: "events" 
    table: "user_events"
    partition_by: ["event_type"]
    mode: "append"
    create_table_if_not_exists: true
    merge_schema: true
    output_mode: "append"
    trigger: {processingTime: "30 seconds"}
```

### Batch Configuration Example

```yaml
# batch_processing_config.yaml
spark:
  app_name: "BatchDataProcessing"
  master: "local[*]"
  config:
    spark.sql.adaptive.enabled: "true"
    spark.sql.adaptive.coalescePartitions.enabled: "true"

pipeline:
  type: "batch"
  
  extractor:
    type: "file"
    format: "parquet"
    path: "data/input/"
    schema_inference: true
  
  transformer:
    type: "sql"
    queries:
      - "SELECT * FROM input_data WHERE age > 25"
      - "SELECT department, COUNT(*) as count FROM filtered_data GROUP BY department"
  
  sink:
    type: "file"
    format: "parquet"
    path: "data/output/batch_result"
    mode: "overwrite"
    partitions: ["department"]
```

## 🔧 Development Scripts

### Essential Development Tools

| Script | Purpose | Usage |
|--------|---------|-------|
| 🚀 `kafka-producer.py` | Generate realistic test events | `python scripts/kafka-producer.py --num-events 1000 --rate 10` |
| 🐳 `start-kafka.sh` | Launch complete Kafka cluster | `./scripts/start-kafka.sh` |
| ✅ `validate-setup.py` | Validate environment & dependencies | `python scripts/validate-setup.py` |
| 📊 `verify_iceberg_data.py` | Inspect Iceberg table contents | `python scripts/verify_iceberg_data.py` |
| 📈 `verify_output.py` | Validate batch processing results | `python scripts/verify_output.py` |

### Advanced Usage Examples

```bash
# Generate high-volume test data for performance testing
python scripts/kafka-producer.py --num-events 100000 --rate 1000 --continuous

# Run streaming pipeline with custom configuration
python examples/kafka_to_iceberg_streaming.py --config configs/production.yaml

# Process data with session analysis
python examples/kafka_to_iceberg_streaming.py --pipeline sessions
```

## 🧩 Building Custom Components

### Custom Extractor Implementation

```python
from src.extractor.base_extractor import BaseExtractor
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any

class DatabaseExtractor(BaseExtractor):
    """Extract data from JDBC-compatible databases."""
    
    def extract(self, spark: SparkSession, config: Dict[str, Any]) -> DataFrame:
        return spark.read \
            .format("jdbc") \
            .option("url", config["jdbc_url"]) \
            .option("dbtable", config["table"]) \
            .option("user", config["username"]) \
            .option("password", config["password"]) \
            .load()
    
    def validate_config(self) -> bool:
        required_fields = ["jdbc_url", "table", "username", "password"]
        return all(field in self.config for field in required_fields)
```

### Custom Transformer Implementation

```python
from src.transformer.base_transformer import BaseTransformer
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, regexp_replace
from typing import Dict, Any

class DataCleaningTransformer(BaseTransformer):
    """Clean and standardize data quality."""
    
    def transform(self, df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        # Remove invalid records
        cleaned_df = df.filter(col("user_id").isNotNull())
        
        # Standardize email format
        cleaned_df = cleaned_df.withColumn(
            "email", 
            regexp_replace(col("email"), r"^\s+|\s+$", "")
        )
        
        # Apply business rules
        cleaned_df = cleaned_df.withColumn(
            "user_segment",
            when(col("purchase_amount") > 1000, "premium")
            .when(col("purchase_amount") > 100, "standard")
            .otherwise("basic")
        )
        
        return cleaned_df
```

### Custom Sink Implementation

```python
from src.sink.base_sink import BaseSink
from pyspark.sql import DataFrame
from typing import Dict, Any

class ElasticsearchSink(BaseSink):
    """Write data to Elasticsearch for real-time analytics."""
    
    def write(self, df: DataFrame, config: Dict[str, Any]) -> None:
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", config["index_name"]) \
            .option("es.nodes", config["elasticsearch_hosts"]) \
            .option("es.port", config.get("port", "9200")) \
            .mode(config.get("mode", "append")) \
            .save()
    
    def write_stream(self, query_builder, config: Dict[str, Any]):
        return query_builder \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", config["index_name"]) \
            .option("es.nodes", config["elasticsearch_hosts"]) \
            .option("checkpointLocation", config["checkpoint_location"]) \
            .start()
```

## 🧪 Testing & Quality Assurance

```bash
# Run complete test suite
pytest tests/ -v

# Test with coverage reporting
pytest --cov=src --cov-report=html tests/

# Run specific component tests
pytest tests/test_extractors/ -v
pytest tests/test_transformers/ -v
pytest tests/test_sinks/ -v

# Code quality checks
black src/ tests/ examples/ scripts/
flake8 src/ tests/
mypy src/

# Integration testing
python scripts/validate-setup.py
python tests/integration/test_kafka_to_iceberg_pipeline.py
```

## 🚀 Deployment & Production

### Docker Deployment

```dockerfile
# Dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN pip install -e .

CMD ["python", "examples/kafka_to_iceberg_streaming.py"]
```

```bash
# Build and deploy
docker build -t sessionize-pipeline .
docker run -d \
  --name sessionize \
  -v $(pwd)/configs:/app/configs \
  -v $(pwd)/logs:/app/logs \
  sessionize-pipeline
```

### Kubernetes Deployment

```yaml
# k8s/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sessionize-streaming
spec:
  replicas: 1
  selector:
    matchLabels:
      app: sessionize-streaming
  template:
    metadata:
      labels:
        app: sessionize-streaming
    spec:
      containers:
      - name: sessionize
        image: sessionize-pipeline:latest
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka-service:9092"
        - name: ICEBERG_WAREHOUSE
          value: "s3a://data-lake/warehouse"
        volumeMounts:
        - name: config-volume
          mountPath: /app/configs
      volumes:
      - name: config-volume
        configMap:
          name: sessionize-config
```

## 📊 Monitoring & Observability

The framework includes comprehensive monitoring capabilities:

### Built-in Metrics
- **Pipeline Health**: Success/failure rates, processing latency
- **Data Quality**: Record counts, schema validation errors
- **Resource Usage**: CPU, memory, disk I/O metrics
- **Streaming Metrics**: Kafka lag, throughput, watermark delays

### Monitoring Interfaces
- **Spark UI**: http://localhost:4040 (development)
- **Structured Logging**: JSON formatted logs with correlation IDs
- **Custom Metrics**: Integration with Prometheus/Grafana
- **Alerting**: Configurable alerts for pipeline failures

### Production Monitoring Setup

```python
# monitoring/metrics_collector.py
from src.utils.metrics import MetricsCollector

collector = MetricsCollector(
    prometheus_gateway="http://prometheus:9091",
    tags={"environment": "production", "pipeline": "kafka-to-iceberg"}
)

# Track custom business metrics
collector.increment("events_processed", tags={"event_type": "purchase"})
collector.histogram("processing_latency_ms", latency_value)
collector.gauge("active_sessions", session_count)
```

## 🤝 Contributing

We welcome contributions! Please see our contribution guidelines:

### Development Setup

```bash
# Fork the repository and clone your fork
git clone https://github.com/yourusername/sessionize.git
cd sessionize

# Create feature branch
git checkout -b feature/amazing-new-feature

# Set up development environment
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .

# Run tests to ensure everything works
pytest tests/ -v
```

### Code Standards
- **Code Style**: Black formatting (line length 88)
- **Type Hints**: All public functions must have type annotations
- **Documentation**: Docstrings for all classes and public methods
- **Testing**: 90%+ test coverage for new code
- **Commit Messages**: Conventional commit format

### Pull Request Process
1. Update documentation for any new features
2. Add tests for new functionality
3. Ensure all CI checks pass
4. Request review from maintainers
5. Address feedback promptly

## 📜 License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.

## 🆘 Support & Community

- **🐛 Bug Reports**: [GitHub Issues](https://github.com/yourusername/sessionize/issues)
- **💡 Feature Requests**: [GitHub Discussions](https://github.com/yourusername/sessionize/discussions)
- **📖 Documentation**: [Wiki Pages](https://github.com/yourusername/sessionize/wiki)
- **💬 Community Chat**: [Discord/Slack Channel](#)

## 🏆 Acknowledgments

Built with these amazing open source technologies:
- [Apache Spark](https://spark.apache.org/) - Unified analytics engine
- [Apache Kafka](https://kafka.apache.org/) - Distributed streaming platform  
- [Apache Iceberg](https://iceberg.apache.org/) - Open table format for data lakes
- [PySpark](https://spark.apache.org/docs/latest/api/python/) - Python API for Spark

---

**⭐ Star this repository if you find it helpful!**