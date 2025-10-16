#!/bin/bash
# Start Kafka in KRaft mode (no Zookeeper) for local development

set -e

KAFKA_DIR=$(brew --prefix kafka 2>/dev/null || echo "")
if [ -z "$KAFKA_DIR" ]; then
    echo "⚠️  Kafka not found via Homebrew, using PATH"
    KAFKA_BIN=""
else
    KAFKA_BIN="$KAFKA_DIR/bin/"
fi

DATA_DIR="/tmp/satoshi-kafka"
LOG_DIR="$DATA_DIR/logs"
CLUSTER_ID=$("${KAFKA_BIN}kafka-storage" random-uuid)

echo "🚀 Starting Kafka for Satoshi pipeline..."
echo "📁 Data directory: $DATA_DIR"
echo "🆔 Cluster ID: $CLUSTER_ID"

# Clean up old data
if [ -d "$DATA_DIR" ]; then
    echo "🧹 Cleaning old data..."
    rm -rf "$DATA_DIR"
fi

mkdir -p "$LOG_DIR"

# Create Kafka config
cat > /tmp/kafka-kraft-config.properties << EOF
# KRaft Configuration for Local Development
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093

# Listeners
listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
advertised.listeners=PLAINTEXT://localhost:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT

# Logs
log.dirs=$DATA_DIR/kraft-logs
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# Log Retention (short for dev)
log.retention.hours=24
log.retention.check.interval.ms=300000
log.segment.bytes=1073741824

# Performance
num.partitions=4
default.replication.factor=1
min.insync.replicas=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1

# Auto-create topics
auto.create.topics.enable=true

# Compression
compression.type=lz4
EOF

echo "📝 Formatting storage..."
"${KAFKA_BIN}kafka-storage" format -t $CLUSTER_ID -c /tmp/kafka-kraft-config.properties

echo "🎯 Starting Kafka broker..."
"${KAFKA_BIN}kafka-server-start" /tmp/kafka-kraft-config.properties
