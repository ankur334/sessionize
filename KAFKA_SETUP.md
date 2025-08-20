# Kafka Setup for Sessionize

This document explains how to set up and use the improved Kafka cluster for testing Sessionize pipelines.

## 🚀 Quick Start

### Start Basic Kafka Cluster (Recommended for Development)
```bash
./scripts/start-kafka.sh
```

### Start Full Kafka Cluster (with Connect and KSQL)
```bash
./scripts/start-kafka.sh --full
```

### Clean Start (Remove all data)
```bash
./scripts/start-kafka.sh --cleanup
```

## 📋 What's Included

### Basic Profile (Default)
- **Kafka Broker** (KRaft mode, no ZooKeeper needed)
- **Schema Registry** for Avro/JSON schema management
- **Kafka UI** for visual cluster management

### Full Profile (--full flag)
- All basic services +
- **Kafka Connect** for data integration
- **KSQL Server** for stream processing

## 🎯 Key Improvements

### 1. Modern KRaft Mode
- **No ZooKeeper Required**: Uses Kafka's native metadata management
- **Faster Startup**: Single-node cluster for development
- **Simplified Architecture**: Fewer moving parts

### 2. Health Checks
- **Service Dependencies**: Services wait for dependencies to be healthy
- **Automatic Validation**: Built-in health checks for all components
- **Graceful Startup**: Proper service orchestration

### 3. Development Optimizations
- **Auto Topic Creation**: Topics created automatically
- **Short Retention**: 1-hour log retention for development
- **Performance Tuning**: Optimized for local development

### 4. Monitoring & Management
- **JMX Metrics**: Exposed on port 9997
- **Modern UI**: Enhanced Kafka UI with schema registry integration
- **Service Profiles**: Optional services for different use cases

## 🔧 Service Endpoints

| Service | URL | Description |
|---------|-----|-------------|
| Kafka Broker | `localhost:9092` | Main Kafka broker |
| Schema Registry | `http://localhost:8081` | Schema management |
| Kafka UI | `http://localhost:8080` | Web-based cluster management |
| Kafka Connect | `http://localhost:8083` | Data integration (full profile) |
| KSQL Server | `http://localhost:8088` | Stream processing (full profile) |

## 📊 Testing with Sample Data

### Generate Test Events
```bash
# Basic user events
python scripts/kafka-producer.py --topic events-topic --num-events 1000 --rate 10

# Clickstream events for session analysis
python scripts/kafka-producer.py --topic clickstream --event-type clickstream --num-events 500 --rate 5

# Continuous production
python scripts/kafka-producer.py --topic events-topic --continuous --rate 2
```

### Manual Testing
```bash
# List topics
docker exec sessionize-kafka kafka-topics --list --bootstrap-server localhost:9092

# Create a new topic
docker exec sessionize-kafka kafka-topics --create --topic my-topic --bootstrap-server localhost:9092 --partitions 3

# Produce messages manually
docker exec -it sessionize-kafka kafka-console-producer --topic events-topic --bootstrap-server localhost:9092

# Consume messages
docker exec -it sessionize-kafka kafka-console-consumer --topic events-topic --bootstrap-server localhost:9092 --from-beginning
```

## 🛠️ Running Sessionize Pipelines

### 1. Kafka to Iceberg Streaming
```bash
# Start the full streaming pipeline
source .venv/bin/activate
python examples/kafka_to_iceberg_streaming.py --config configs/kafka_iceberg_streaming.yaml
```

### 2. Testing Pipeline Components
```bash
# Test individual components
python examples/iceberg_pipeline_demo.py --mode streaming

# Test with real Kafka data
python examples/kafka_to_iceberg_streaming.py --test-mode
```

## 📁 Data Persistence

### Volumes
- `kafka_data`: Kafka logs and metadata
- `kafka_connect_data`: Kafka Connect data

### Cleanup
```bash
# Stop and remove all data
docker-compose down -v

# Or use the script
./scripts/start-kafka.sh --cleanup
```

## 🔍 Troubleshooting

### Common Issues

1. **Port Already in Use**
   ```bash
   # Check what's using the port
   lsof -i :9092
   
   # Stop existing services
   docker-compose down
   ```

2. **Services Not Starting**
   ```bash
   # Check logs
   docker-compose logs -f kafka
   docker-compose logs -f schema-registry
   ```

3. **Memory Issues**
   ```bash
   # Ensure Docker has at least 8GB RAM allocated
   docker system info | grep Memory
   ```

4. **Connection Issues**
   ```bash
   # Test Kafka connectivity
   docker exec sessionize-kafka kafka-broker-api-versions --bootstrap-server localhost:9092
   ```

### Useful Commands

```bash
# View all container logs
docker-compose logs -f

# Restart specific service
docker-compose restart kafka

# Check service health
docker-compose ps

# Monitor resource usage
docker stats
```

## 🎨 Kafka UI Features

Access the Kafka UI at `http://localhost:8080` to:

- View topics, partitions, and messages
- Monitor consumer groups and lag
- Manage schemas (with Schema Registry)
- View cluster configuration
- Produce and consume messages visually

## 📈 Performance Tips

### Development
- Use the basic profile for faster startup
- Enable auto topic creation for convenience
- Short retention periods to save disk space

### Testing
- Use the full profile for integration testing
- Enable all monitoring features
- Test with realistic data volumes

### Production Considerations
- Use external Schema Registry
- Configure proper retention policies
- Set up monitoring and alerting
- Use multiple brokers for high availability

## 🔐 Security Notes

This setup is designed for **development and testing only**. For production:

- Enable authentication (SASL/SCRAM or mTLS)
- Use encrypted connections (TLS)
- Implement proper network security
- Set up monitoring and alerting
- Configure backup and disaster recovery

## 📚 Additional Resources

- [Confluent Platform Documentation](https://docs.confluent.io/platform/current/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [KRaft Mode Guide](https://kafka.apache.org/documentation/#kraft)
- [Docker Compose Reference](https://docs.docker.com/compose/)

---

**Next Steps**: Once Kafka is running, try the [Kafka to Iceberg streaming pipeline](examples/kafka_to_iceberg_streaming.py) or run the [pipeline demos](examples/iceberg_pipeline_demo.py)!