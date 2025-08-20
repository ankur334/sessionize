#!/usr/bin/env python3
"""
Setup Validation Script for Sessionize

This script validates that all components are properly configured and working.
"""

import sys
import subprocess
import requests
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SetupValidator:
    def __init__(self):
        self.errors = []
        self.warnings = []
    
    def log_error(self, message):
        self.errors.append(message)
        logger.error(f"❌ {message}")
    
    def log_warning(self, message):
        self.warnings.append(message)
        logger.warning(f"⚠️  {message}")
    
    def log_success(self, message):
        logger.info(f"✅ {message}")
    
    def validate_python_environment(self):
        """Validate Python environment and dependencies."""
        logger.info("🐍 Validating Python environment...")
        
        # Check Python version
        if sys.version_info < (3, 8):
            self.log_error(f"Python 3.8+ required, found {sys.version}")
        else:
            self.log_success(f"Python version: {sys.version.split()[0]}")
        
        # Check required packages
        required_packages = [
            'pyspark', 'kafka-python', 'pyyaml', 
            'pyiceberg', 'pandas', 'numpy'
        ]
        
        for package in required_packages:
            try:
                __import__(package.replace('-', '_'))
                self.log_success(f"Package {package} is installed")
            except ImportError:
                self.log_error(f"Required package {package} is not installed")
    
    def validate_docker(self):
        """Validate Docker installation and status."""
        logger.info("🐳 Validating Docker...")
        
        try:
            result = subprocess.run(['docker', '--version'], 
                                  capture_output=True, text=True, check=True)
            self.log_success(f"Docker: {result.stdout.strip()}")
        except (subprocess.CalledProcessError, FileNotFoundError):
            self.log_error("Docker is not installed or not accessible")
            return False
        
        try:
            result = subprocess.run(['docker', 'info'], 
                                  capture_output=True, text=True, check=True)
            self.log_success("Docker daemon is running")
        except subprocess.CalledProcessError:
            self.log_error("Docker daemon is not running")
            return False
        
        return True
    
    def validate_kafka_services(self):
        """Validate Kafka cluster services."""
        logger.info("🔧 Validating Kafka services...")
        
        services = {
            'Kafka UI': 'http://localhost:8080',
            'Schema Registry': 'http://localhost:8081/subjects',
            'Kafka Connect': 'http://localhost:8083/connectors',
            'KSQL Server': 'http://localhost:8088/info'
        }
        
        for service_name, url in services.items():
            try:
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    self.log_success(f"{service_name} is accessible")
                else:
                    self.log_warning(f"{service_name} returned status {response.status_code}")
            except requests.exceptions.ConnectionError:
                if service_name in ['Kafka Connect', 'KSQL Server']:
                    self.log_warning(f"{service_name} is not running (optional service)")
                else:
                    self.log_error(f"{service_name} is not accessible")
            except requests.exceptions.Timeout:
                self.log_error(f"{service_name} request timed out")
    
    def validate_kafka_connectivity(self):
        """Validate Kafka broker connectivity."""
        logger.info("📡 Validating Kafka connectivity...")
        
        try:
            # Test producer
            producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda v: str(v).encode('utf-8')
            )
            
            # Send test message
            future = producer.send('test-validation', value='validation-message')
            producer.flush()
            self.log_success("Kafka producer connectivity successful")
            producer.close()
            
        except Exception as e:
            self.log_error(f"Kafka producer failed: {e}")
            return False
        
        try:
            # Test consumer
            consumer = KafkaConsumer(
                'test-validation',
                bootstrap_servers=['localhost:9092'],
                auto_offset_reset='latest',
                consumer_timeout_ms=5000
            )
            self.log_success("Kafka consumer connectivity successful")
            consumer.close()
            
        except Exception as e:
            self.log_error(f"Kafka consumer failed: {e}")
            return False
        
        return True
    
    def validate_spark_setup(self):
        """Validate Spark configuration."""
        logger.info("⚡ Validating Spark setup...")
        
        try:
            from pyspark.sql import SparkSession
            
            spark = SparkSession.builder \
                .appName("ValidationTest") \
                .master("local[1]") \
                .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
                .getOrCreate()
            
            # Test basic operation
            df = spark.createDataFrame([(1, 'test')], ['id', 'value'])
            count = df.count()
            
            if count == 1:
                self.log_success("Spark basic operations working")
            else:
                self.log_error("Spark basic operations failed")
            
            spark.stop()
            
        except Exception as e:
            self.log_error(f"Spark validation failed: {e}")
    
    def validate_iceberg_setup(self):
        """Validate Iceberg integration."""
        logger.info("🧊 Validating Iceberg setup...")
        
        try:
            from pyspark.sql import SparkSession
            
            spark = SparkSession.builder \
                .appName("IcebergValidation") \
                .master("local[1]") \
                .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.local.type", "hadoop") \
                .config("spark.sql.catalog.local.warehouse", "/tmp/validation_warehouse") \
                .getOrCreate()
            
            # Test Iceberg table creation
            df = spark.createDataFrame([(1, 'test')], ['id', 'value'])
            df.writeTo("local.default.validation_test").create()
            
            # Test reading
            result_df = spark.read.format("iceberg").load("local.default.validation_test")
            count = result_df.count()
            
            if count == 1:
                self.log_success("Iceberg integration working")
            else:
                self.log_error("Iceberg integration failed")
            
            spark.stop()
            
        except Exception as e:
            self.log_error(f"Iceberg validation failed: {e}")
    
    def run_validation(self):
        """Run all validation checks."""
        logger.info("🔍 Starting Sessionize setup validation...\n")
        
        self.validate_python_environment()
        print()
        
        if self.validate_docker():
            self.validate_kafka_services()
            print()
            self.validate_kafka_connectivity()
            print()
        
        self.validate_spark_setup()
        print()
        self.validate_iceberg_setup()
        print()
        
        # Summary
        logger.info("📋 Validation Summary:")
        if self.errors:
            logger.error(f"❌ {len(self.errors)} error(s) found:")
            for error in self.errors:
                logger.error(f"   • {error}")
        
        if self.warnings:
            logger.warning(f"⚠️  {len(self.warnings)} warning(s):")
            for warning in self.warnings:
                logger.warning(f"   • {warning}")
        
        if not self.errors:
            logger.info("🎉 All validations passed! Sessionize is ready to use.")
            return True
        else:
            logger.error("❌ Setup validation failed. Please fix the errors above.")
            return False


def main():
    validator = SetupValidator()
    success = validator.run_validation()
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())