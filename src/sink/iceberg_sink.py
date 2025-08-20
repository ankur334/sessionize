from typing import Any, Dict, Optional
from pyspark.sql import DataFrame
from src.sink.base_sink import BaseSink
from src.common.exceptions import SinkError, ConfigurationError
import logging


class IcebergSink(BaseSink):
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.catalog_name = config.get('catalog', 'spark_catalog')
        self.database = config.get('database', 'default')
        self.table = config.get('table')
        self.table_path = config.get('table_path')
        self.partition_by = config.get('partition_by', [])
        self.table_properties = config.get('table_properties', {})
        self.write_mode = config.get('mode', 'append')
        self.format_version = config.get('format_version', 2)
        self.create_table_if_not_exists = config.get('create_table_if_not_exists', True)
        self.merge_schema = config.get('merge_schema', True)
    
    def write(self, df: DataFrame, config: Dict[str, Any]) -> None:
        try:
            if not self.validate_config():
                raise ConfigurationError("Invalid Iceberg sink configuration")
            
            table_identifier = self._get_table_identifier()
            
            self.logger.info(f"Writing data to Iceberg table: {table_identifier}")
            
            spark = df.sparkSession
            self._configure_spark_for_iceberg(spark)
            
            if self.create_table_if_not_exists:
                self._create_table_if_not_exists(spark, df, table_identifier)
            
            writer = df.writeTo(table_identifier)
            
            if self.partition_by:
                writer = writer.partitionedBy(*self.partition_by)
            
            if self.table_properties:
                for key, value in self.table_properties.items():
                    writer = writer.tableProperty(key, str(value))
            
            if self.write_mode == 'append':
                writer.append()
            elif self.write_mode == 'overwrite':
                writer.overwritePartitions()
            elif self.write_mode == 'create':
                writer.create()
            elif self.write_mode == 'replace':
                writer.createOrReplace()
            else:
                raise SinkError(f"Unsupported write mode: {self.write_mode}")
            
            self.logger.info(f"Successfully wrote data to Iceberg table: {table_identifier}")
            
        except Exception as e:
            self.logger.error(f"Failed to write to Iceberg table: {e}")
            raise SinkError(f"Iceberg sink write failed: {e}")
    
    def write_stream(self, query_builder, config: Dict[str, Any]):
        try:
            if not self.validate_config():
                raise ConfigurationError("Invalid Iceberg sink configuration")
            
            table_identifier = self._get_table_identifier()
            
            self.logger.info(f"Setting up streaming write to Iceberg table: {table_identifier}")
            
            checkpoint_location = config.get('checkpoint_location', f"/tmp/iceberg_checkpoint_{self.table}")
            
            stream_query = query_builder \
                .format("iceberg") \
                .option("path", table_identifier) \
                .option("checkpointLocation", checkpoint_location)
            
            if self.merge_schema:
                stream_query = stream_query.option("mergeSchema", "true")
            
            if config.get('fanout_enabled', False):
                stream_query = stream_query.option("fanout.enabled", "true")
            
            commit_interval = config.get('commit_interval_ms', 60000)
            stream_query = stream_query.option("commit.interval.ms", str(commit_interval))
            
            for key, value in config.items():
                if key.startswith('iceberg.'):
                    stream_query = stream_query.option(key, str(value))
            
            query = stream_query.start()
            
            self.logger.info(f"Streaming write to Iceberg table started: {table_identifier}")
            return query
            
        except Exception as e:
            self.logger.error(f"Failed to setup streaming write to Iceberg: {e}")
            raise SinkError(f"Iceberg streaming sink setup failed: {e}")
    
    def _configure_spark_for_iceberg(self, spark):
        # Skip setting extensions if already configured (static configs can't be changed after session creation)
        try:
            spark.conf.set(f"spark.sql.catalog.{self.catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        except Exception:
            pass
        
        if self.table_path:
            spark.conf.set(f"spark.sql.catalog.{self.catalog_name}.type", "hadoop")
            spark.conf.set(f"spark.sql.catalog.{self.catalog_name}.warehouse", self.table_path)
        else:
            catalog_type = self.config.get('catalog_type', 'hadoop')
            if catalog_type == 'hadoop':
                spark.conf.set(f"spark.sql.catalog.{self.catalog_name}.type", "hadoop")
                warehouse_path = self.config.get('warehouse', '/tmp/iceberg_warehouse')
                spark.conf.set(f"spark.sql.catalog.{self.catalog_name}.warehouse", warehouse_path)
            elif catalog_type == 'hive':
                spark.conf.set(f"spark.sql.catalog.{self.catalog_name}.type", "hive")
                spark.conf.set(f"spark.sql.catalog.{self.catalog_name}.uri", self.config.get('metastore_uri', 'thrift://localhost:9083'))
            elif catalog_type == 'glue':
                spark.conf.set(f"spark.sql.catalog.{self.catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
                spark.conf.set(f"spark.sql.catalog.{self.catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
                spark.conf.set(f"spark.sql.catalog.{self.catalog_name}.warehouse", self.config.get('warehouse', 's3://my-bucket/iceberg'))
                if 'glue_database' in self.config:
                    spark.conf.set(f"spark.sql.catalog.{self.catalog_name}.glue.catalog-database", self.config['glue_database'])
            elif catalog_type == 'rest':
                spark.conf.set(f"spark.sql.catalog.{self.catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
                spark.conf.set(f"spark.sql.catalog.{self.catalog_name}.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
                spark.conf.set(f"spark.sql.catalog.{self.catalog_name}.uri", self.config.get('rest_uri', 'http://localhost:8181'))
                if 'warehouse' in self.config:
                    spark.conf.set(f"spark.sql.catalog.{self.catalog_name}.warehouse", self.config['warehouse'])
        
        spark.conf.set("spark.sql.catalog.spark_catalog.cache-enabled", "false")
    
    def _create_table_if_not_exists(self, spark, df: DataFrame, table_identifier: str):
        try:
            table_exists = spark.catalog.tableExists(table_identifier)
            if not table_exists:
                self.logger.info(f"Creating Iceberg table: {table_identifier}")
                
                create_stmt = f"CREATE TABLE IF NOT EXISTS {table_identifier} "
                
                if self.partition_by:
                    schema_str = ", ".join([f"{field.name} {field.dataType.simpleString()}" for field in df.schema.fields])
                    partition_str = ", ".join(self.partition_by)
                    create_stmt += f"({schema_str}) USING iceberg PARTITIONED BY ({partition_str})"
                else:
                    create_stmt += f"USING iceberg AS SELECT * FROM {df.createOrReplaceTempView('temp_create_table')}"
                    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_identifier} USING iceberg AS SELECT * FROM temp_create_table WHERE 1=0")
                    return
                
                if self.table_properties:
                    props = ", ".join([f"'{k}'='{v}'" for k, v in self.table_properties.items()])
                    create_stmt += f" TBLPROPERTIES ({props})"
                
                spark.sql(create_stmt)
                self.logger.info(f"Created Iceberg table: {table_identifier}")
        except Exception as e:
            self.logger.warning(f"Could not check/create table: {e}")
    
    def _get_table_identifier(self) -> str:
        if not self.table:
            raise ConfigurationError("Table name is required for Iceberg sink")
        
        if '.' in self.table:
            return self.table
        else:
            return f"{self.catalog_name}.{self.database}.{self.table}"
    
    def validate_config(self) -> bool:
        if not self.config:
            self.logger.error("No configuration provided")
            return False
        
        if not self.table:
            self.logger.error("Table name is required for Iceberg sink")
            return False
        
        valid_write_modes = ['append', 'overwrite', 'create', 'replace']
        if self.write_mode not in valid_write_modes:
            self.logger.error(f"Invalid write mode: {self.write_mode}. Must be one of {valid_write_modes}")
            return False
        
        return True
    
    def get_output_path(self) -> Optional[str]:
        if self.table_path:
            return f"{self.table_path}/{self.database}/{self.table}"
        else:
            warehouse = self.config.get('warehouse', '/tmp/iceberg_warehouse')
            return f"{warehouse}/{self.database}/{self.table}"