#!/bin/bash
# Create Data Engineering Topics for Satoshi
# 
# SINGLE SOURCE OF TRUTH for topic creation
# Auto-detects Docker or Homebrew Kafka installation

echo "🚀 Creating Satoshi Data Engineering Topics..."

# Detect Kafka installation type
KAFKA_MODE=""

# Check for Homebrew kafka-topics command
if command -v kafka-topics &> /dev/null; then
    if kafka-topics --bootstrap-server localhost:9092 --list &> /dev/null; then
        KAFKA_MODE="homebrew"
        echo "✅ Detected: Homebrew Kafka installation"
    fi
fi

# Check for Docker Kafka (if not already found Homebrew)
if [[ -z "$KAFKA_MODE" ]] && docker info >/dev/null 2>&1; then
    if docker ps --filter "name=kafka" --format "table {{.Names}}" | grep -q kafka; then
        KAFKA_MODE="docker"
        echo "✅ Detected: Docker Kafka installation"
    fi
fi

# Error if no Kafka found
if [[ -z "$KAFKA_MODE" ]]; then
    echo "❌ Error: No Kafka installation detected" >&2
    echo "" >&2
    echo "Please ensure one of the following:" >&2
    echo "  1. Homebrew Kafka is running: brew services start kafka" >&2
    echo "  2. Docker Kafka is running: docker-compose up -d kafka" >&2
    exit 1
fi

echo "🔧 Using Kafka mode: $KAFKA_MODE"

# Function to create topic with validation and error handling
create_topic() {
    local topic_name="$1"
    local partitions="$2"
    
    # Validate inputs
    if [[ -z "$topic_name" ]]; then
        echo "❌ Error: Topic name cannot be empty" >&2
        exit 1
    fi
    
    if ! [[ "$partitions" =~ ^[1-9][0-9]*$ ]]; then
        echo "❌ Error: Partitions must be a positive integer, got: '$partitions'" >&2
        exit 1
    fi
    
    echo "📊 Creating topic: '$topic_name' (partitions: $partitions)"
    
    # Execute command based on Kafka mode
    if [[ "$KAFKA_MODE" == "docker" ]]; then
        # Docker mode - use docker exec
        if ! docker exec kafka kafka-topics --create --topic "$topic_name" --partitions "$partitions" --replication-factor 1 --bootstrap-server localhost:9092 2>/dev/null; then
            # Topic might already exist, check if we can list it
            if docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null | grep -q "^$topic_name$"; then
                echo "⚠️  Topic '$topic_name' already exists"
            else
                echo "❌ Error: Failed to create topic '$topic_name'" >&2
                exit 1
            fi
        else
            echo "✅ Successfully created topic '$topic_name' with $partitions partitions"
        fi
    else
        # Homebrew mode - use kafka-topics directly
        if kafka-topics --bootstrap-server localhost:9092 --list | grep -q "^$topic_name$"; then
            echo "⚠️  Topic '$topic_name' already exists, checking partition count..."
            
            # Get current partition count
            current_partitions=$(kafka-topics --bootstrap-server localhost:9092 --describe --topic "$topic_name" | grep "PartitionCount:" | awk '{print $2}' | head -1)
            
            if [[ -n "$current_partitions" && "$current_partitions" -lt "$partitions" ]]; then
                echo "🔧 Increasing partitions from $current_partitions to $partitions..."
                if ! kafka-topics --bootstrap-server localhost:9092 --alter --topic "$topic_name" --partitions "$partitions"; then
                    echo "❌ Error: Failed to increase partitions for topic '$topic_name'" >&2
                    exit 1
                fi
                echo "✅ Updated topic '$topic_name' to $partitions partitions"
            else
                echo "✅ Topic '$topic_name' already has sufficient partitions ($current_partitions)"
            fi
        else
            # Create new topic
            if ! kafka-topics --bootstrap-server localhost:9092 --create --topic "$topic_name" --partitions "$partitions" --replication-factor 1; then
                echo "❌ Error: Failed to create topic '$topic_name'" >&2
                exit 1
            fi
            echo "✅ Successfully created topic '$topic_name' with $partitions partitions"
        fi
    fi
}

# ═══════════════════════════════════════════════════════════════════════════════════
# 🥉 BRONZE LAYER - Raw Ingestion (raw_data.* topics)
# ═══════════════════════════════════════════════════════════════════════════════════

# Core Raw Data Topics (collectors write here)
create_topic "raw_data.exchange_feed" 20
create_topic "raw_data.options_chain" 12
create_topic "raw_data.onchain_events" 12
create_topic "raw_data.offchain_events" 6

# Granular Market Data Topics
create_topic "raw_data.market.trades" 20
create_topic "raw_data.market.book" 16
create_topic "raw_data.market.funding" 8
create_topic "raw_data.market.oi" 8

# Granular On-Chain Topics
create_topic "raw_data.onchain.blocks" 8
create_topic "raw_data.onchain.mempool" 16

# Macro/TradFi Data Collection (NEW - Phase 3)
create_topic "raw_data.macro.economic_indicators" 4
create_topic "raw_data.tradfi.indices" 4
create_topic "raw_data.tradfi.equities" 4
create_topic "raw_data.tradfi.commodities" 4

# Crypto Market Metrics Collection (NEW - Phase 3)
create_topic "raw_data.crypto.market_metrics" 4

# ═══════════════════════════════════════════════════════════════════════════════════
# 🥈 SILVER LAYER - Quality Validated Data (clean.* topics)
# ═══════════════════════════════════════════════════════════════════════════════════

# Clean Market Data (Quality Orchestrator writes here)
create_topic "clean.market.trades" 16
create_topic "clean.market.book" 12
create_topic "clean.market.orderbook" 12
create_topic "clean.market.funding" 8
create_topic "clean.market.oi" 8
create_topic "clean.market.options" 10
create_topic "clean.market.onchain" 12
create_topic "clean.market.events" 8

# Clean Macro/TradFi Data (NEW - Phase 3)
create_topic "clean.macro.economic_indicators" 4
create_topic "clean.tradfi.indices" 4
create_topic "clean.tradfi.equities" 4
create_topic "clean.tradfi.commodities" 4

# Clean Crypto Metrics (NEW - Phase 3)
create_topic "clean.crypto.market_metrics" 4

# Quality Audit
create_topic "clean.pass_fail" 4

# ═══════════════════════════════════════════════════════════════════════════════════
# 🥇 GOLD LAYER - Production Ready Analytics (curated.data.* topics)
# ═══════════════════════════════════════════════════════════════════════════════════

# OHLCV Bars (Multi-Timeframe)
create_topic "curated.data.ohlcv_1s" 8
create_topic "curated.data.ohlcv_5s" 8
create_topic "curated.data.ohlcv_1m" 8
create_topic "curated.data.ohlcv_5m" 8
create_topic "curated.data.ohlcv_15m" 8
create_topic "curated.data.ohlcv_1h" 8
create_topic "curated.data.ohlcv_1d" 8

# Reference Data
create_topic "curated.data.symbols" 8
create_topic "curated.data.options_chain" 8
create_topic "curated.data.orderbook_snapshot" 8

c

# ═══════════════════════════════════════════════════════════════════════════════════
# 🛡️ OPERATIONAL TOPICS - Monitoring & Control
# ═══════════════════════════════════════════════════════════════════════════════════

# Incident Management
create_topic "incidents.SchemaViolation" 6
create_topic "incidents.Freshness" 4
create_topic "incidents.Anomaly" 6
create_topic "incidents.Leakage" 4
create_topic "incidents.leakage" 4
create_topic "incidents.ohlcv_aggregator" 4
create_topic "incidents.orderbook_curator" 4
create_topic "incidents.options_curator" 4
create_topic "incidents.symbol_normalizer" 4

# System Control
create_topic "control.circuit_breaker" 2
create_topic "control.command_acks" 2
create_topic "control.breaker_intent" 2
create_topic "control.breaker_state" 2
create_topic "control.config_update" 2
create_topic "control.venue_maintenance" 2
create_topic "control.options_symbols" 2
create_topic "control.event_sources" 2
create_topic "control.calendar_update" 2

# Data Lineage & Audit
create_topic "audit.data_lineage" 4
create_topic "audit.quality_metrics" 6
create_topic "audit.reconciliation_reports" 4

echo ""
echo "✅ All topics created/verified successfully!"
echo ""
echo "📊 Topic Summary by Layer:"
echo ""

# List topics based on Kafka mode
if [[ "$KAFKA_MODE" == "docker" ]]; then
    echo "Bronze (Raw Data):"
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep "^raw_data\." | wc -l | xargs echo "  raw_data.* topics:"
    echo ""
    echo "Silver (Clean Data):"
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep "^clean\." | wc -l | xargs echo "  clean.* topics:"
    echo ""
    echo "Gold (Curated Data):"
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep "^curated\." | wc -l | xargs echo "  curated.* topics:"
    echo ""
    echo "Operational:"
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep "^incidents\." | wc -l | xargs echo "  incidents.* topics:"
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep "^control\." | wc -l | xargs echo "  control.* topics:"
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep "^audit\." | wc -l | xargs echo "  audit.* topics:"
    echo ""
    echo "🔍 Full Topic List:"
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep -E "(raw_data|clean|curated|control|incidents|audit)" | sort
else
    echo "Bronze (Raw Data):"
    kafka-topics --bootstrap-server localhost:9092 --list | grep "^raw_data\." | wc -l | xargs echo "  raw_data.* topics:"
    echo ""
    echo "Silver (Clean Data):"
    kafka-topics --bootstrap-server localhost:9092 --list | grep "^clean\." | wc -l | xargs echo "  clean.* topics:"
    echo ""
    echo "Gold (Curated Data):"
    kafka-topics --bootstrap-server localhost:9092 --list | grep "^curated\." | wc -l | xargs echo "  curated.* topics:"
    echo ""
    echo "Operational:"
    kafka-topics --bootstrap-server localhost:9092 --list | grep "^incidents\." | wc -l | xargs echo "  incidents.* topics:"
    kafka-topics --bootstrap-server localhost:9092 --list | grep "^control\." | wc -l | xargs echo "  control.* topics:"
    kafka-topics --bootstrap-server localhost:9092 --list | grep "^audit\." | wc -l | xargs echo "  audit.* topics:"
    echo ""
    echo "🔍 Full Topic List:"
    kafka-topics --bootstrap-server localhost:9092 --list | grep -E "(raw_data|clean|curated|control|incidents|audit)" | sort
fi