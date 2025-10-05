#!/bin/bash
# Kafka Development Setup for Satoshi Data Ingestion Layer

# Enable strict error handling
set -euo pipefail

echo "🚀 Starting Kafka for Satoshi HFT Data Pipeline..."

# Start Kafka cluster
docker-compose -f docker-compose.kafka.yml up -d

# Function to wait for Kafka readiness
wait_for_kafka() {
    local max_attempts=30
    local attempt=1
    echo "⏳ Waiting for Kafka to be ready..."
    
    while [ $attempt -le $max_attempts ]; do
        echo "Attempt $attempt/$max_attempts: Checking Kafka readiness..."
        if docker exec kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; then
            echo "✅ Kafka is ready!"
            return 0
        fi
        
        sleep 3
        ((attempt++))
    done
    
    echo "❌ Kafka failed to become ready after $max_attempts attempts"
    exit 1
}

# Function to create topic with error handling
create_topic() {
    local topic_name="$1"
    local partitions="$2"
    echo "Creating topic: $topic_name"
    if ! docker exec kafka kafka-topics --create --topic "$topic_name" --partitions "$partitions" --replication-factor 1 --bootstrap-server localhost:9092; then
        echo "❌ Failed to create topic: $topic_name"
        exit 1
    fi
}

# Wait for Kafka to be ready
wait_for_kafka

# Create all topics for data ingestion layer
echo "📊 Creating data ingestion topics..."

# Raw data topics
create_topic "raw_data.exchange_feed" 20
create_topic "raw_data.options_chain" 12
create_topic "raw_data.onchain_events" 8
create_topic "raw_data.offchain_events" 6

# Clean data topics
create_topic "clean.pass_fail" 4

# Incident topics
create_topic "incidents.SchemaViolation" 6
create_topic "incidents.Freshness" 4
create_topic "incidents.Anomaly" 6
create_topic "incidents.Leakage" 4

# Control topics
create_topic "control.circuit_breaker" 2
create_topic "control.command_acks" 2

echo "✅ Kafka ready for data ingestion layer!"
echo "🌐 Kafka UI available at: http://localhost:8080"
echo "📡 Bootstrap server: localhost:9092"
