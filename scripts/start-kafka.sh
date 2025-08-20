#!/bin/bash

# Kafka Cluster Startup Script for Sessionize

set -e

echo "🚀 Starting Kafka Cluster for Sessionize..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if Docker Compose is available
if ! command -v docker-compose >/dev/null 2>&1; then
    print_error "Docker Compose is not installed. Please install Docker Compose and try again."
    exit 1
fi

# Parse command line arguments
PROFILE="basic"
CLEANUP=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --full)
            PROFILE="full"
            shift
            ;;
        --cleanup)
            CLEANUP=true
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --full      Start with all services (Kafka Connect, KSQL)"
            echo "  --cleanup   Clean up volumes and containers before starting"
            echo "  --help      Show this help message"
            echo ""
            echo "Basic services (default): Kafka, Schema Registry, Kafka UI"
            echo "Full services: All basic services + Kafka Connect + KSQL"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use --help for usage information."
            exit 1
            ;;
    esac
done

# Navigate to the project directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# Cleanup if requested
if [ "$CLEANUP" = true ]; then
    print_warning "Cleaning up existing containers and volumes..."
    docker-compose down -v --remove-orphans 2>/dev/null || true
    docker system prune -f >/dev/null 2>&1 || true
    print_success "Cleanup completed"
fi

# Start services based on profile
if [ "$PROFILE" = "full" ]; then
    print_status "Starting FULL Kafka cluster (Kafka + Schema Registry + UI + Connect + KSQL)..."
    docker-compose --profile full up -d
else
    print_status "Starting BASIC Kafka cluster (Kafka + Schema Registry + UI)..."
    docker-compose up -d kafka schema-registry kafka-ui
fi

# Wait for services to be healthy
print_status "Waiting for services to start..."
sleep 10

# Check service health
check_service() {
    local service_name=$1
    local port=$2
    local endpoint=${3:-""}
    
    print_status "Checking $service_name..."
    
    for i in {1..30}; do
        if curl -s "http://localhost:$port$endpoint" >/dev/null 2>&1; then
            print_success "$service_name is ready!"
            return 0
        fi
        sleep 2
    done
    
    print_error "$service_name failed to start properly"
    return 1
}

# Check basic services
check_service "Kafka UI" 8080
check_service "Schema Registry" 8081 "/subjects"

# Check full services if running in full mode
if [ "$PROFILE" = "full" ]; then
    check_service "Kafka Connect" 8083 "/connectors"
    check_service "KSQL Server" 8088 "/info"
fi

# Create test topic
print_status "Creating test topics..."
docker exec -it sessionize-kafka kafka-topics --create --topic events-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>/dev/null || true
docker exec -it sessionize-kafka kafka-topics --create --topic test-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>/dev/null || true

# Display service URLs
echo ""
print_success "🎉 Kafka cluster is ready!"
echo ""
echo "📋 Service URLs:"
echo "   • Kafka Broker:      localhost:9092"
echo "   • Schema Registry:   http://localhost:8081"
echo "   • Kafka UI:          http://localhost:8080"

if [ "$PROFILE" = "full" ]; then
    echo "   • Kafka Connect:     http://localhost:8083"
    echo "   • KSQL Server:       http://localhost:8088"
fi

echo ""
echo "📊 Available Topics:"
docker exec sessionize-kafka kafka-topics --list --bootstrap-server localhost:9092

echo ""
echo "🛠️  Quick Commands:"
echo "   • List topics:       docker exec sessionize-kafka kafka-topics --list --bootstrap-server localhost:9092"
echo "   • Create topic:      docker exec sessionize-kafka kafka-topics --create --topic my-topic --bootstrap-server localhost:9092 --partitions 3"
echo "   • Produce messages:  docker exec -it sessionize-kafka kafka-console-producer --topic events-topic --bootstrap-server localhost:9092"
echo "   • Consume messages:  docker exec -it sessionize-kafka kafka-console-consumer --topic events-topic --bootstrap-server localhost:9092 --from-beginning"
echo "   • Stop cluster:      docker-compose down"
echo "   • View logs:         docker-compose logs -f kafka"

echo ""
print_success "Ready to run Sessionize pipelines! 🚀"