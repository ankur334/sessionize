DEFAULT_SPARK_CONFIG = {
    'spark.sql.adaptive.enabled': 'true',
    'spark.sql.adaptive.coalescePartitions.enabled': 'true',
    'spark.sql.adaptive.skewJoin.enabled': 'true',
    'spark.sql.shuffle.partitions': '200',
    'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
    'spark.sql.execution.arrow.pyspark.enabled': 'true'
}

SUPPORTED_FILE_FORMATS = [
    'parquet',
    'csv',
    'json',
    'avro',
    'orc',
    'delta',
    'text'
]

SUPPORTED_STREAMING_SOURCES = [
    'kafka',
    'file',
    'socket',
    'rate'
]

SUPPORTED_STREAMING_SINKS = [
    'kafka',
    'file',
    'console',
    'memory',
    'foreach',
    'delta'
]

OUTPUT_MODES = {
    'append': 'Append new rows only',
    'complete': 'Output all rows every time',
    'update': 'Output only changed rows'
}

TRIGGER_TYPES = {
    'processingTime': 'Fixed interval micro-batches',
    'once': 'Single batch processing',
    'continuous': 'Continuous processing (experimental)',
    'availableNow': 'Process all available data then stop'
}

DEFAULT_CHECKPOINT_LOCATION = '/tmp/sessionize_checkpoint'

LOG_LEVELS = {
    'DEBUG': 10,
    'INFO': 20,
    'WARNING': 30,
    'ERROR': 40,
    'CRITICAL': 50
}

PIPELINE_STATES = {
    'INITIALIZED': 'Pipeline initialized',
    'RUNNING': 'Pipeline is running',
    'COMPLETED': 'Pipeline completed successfully',
    'FAILED': 'Pipeline failed',
    'STOPPED': 'Pipeline stopped by user'
}