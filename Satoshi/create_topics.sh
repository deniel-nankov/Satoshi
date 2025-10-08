#!/bin/bash
# Create Data Engineering Topics for Satoshi

echo "🚀 Creating Satoshi Data Engineering Topics..."

# Validate Docker is running
if ! docker info >/dev/null 2>&1; then
    echo "❌ Error: Docker daemon is not running or not accessible" >&2
    exit 1
fi

# Check if Kafka container is running
if ! docker ps --filter "name=kafka" --format "table {{.Names}}" | grep -q kafka; then
    echo "❌ Error: Kafka container is not running. Please start it first." >&2
    exit 1
fi

echo "✅ Docker and Kafka container verified"

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
    
    # Verify Kafka container is still running
    if ! docker ps --filter "name=kafka" --format "table {{.Names}}" | grep -q kafka; then
        echo "❌ Error: Kafka container is no longer running" >&2
        exit 1
    fi
    
    echo "📊 Creating topic: '$topic_name' (partitions: $partitions)"
    
    # Create topic and capture exit status
    if ! docker exec kafka kafka-topics --create --topic "$topic_name" --partitions "$partitions" --replication-factor 1 --bootstrap-server localhost:9092; then
        echo "❌ Error: Failed to create topic '$topic_name'" >&2
        exit 1
    fi
    
    # Verify topic was created successfully
    if docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 | grep -q "^$topic_name$"; then
        echo "✅ Topic '$topic_name' created successfully"
    else
        echo "❌ Warning: Topic '$topic_name' creation may have failed (not found in list)" >&2
    fi
}

# ═══════════════════════════════════════════════════════════════════════════════════
# 🥉 BRONZE LAYER - Raw Ingestion (Unprocessed Data from Sources)
# ═══════════════════════════════════════════════════════════════════════════════════

# Exchange Data Streams
create_topic "bronze.exchange.trades" 20
create_topic "bronze.exchange.orderbook" 16
create_topic "bronze.exchange.funding" 8
create_topic "bronze.exchange.liquidations" 12

# Options Market Data
create_topic "bronze.options.chains" 12
create_topic "bronze.options.greeks" 8
create_topic "bronze.options.vol_surface" 6

# On-Chain Data
create_topic "bronze.onchain.blocks" 8
create_topic "bronze.onchain.mempool" 16
create_topic "bronze.onchain.events" 12
create_topic "bronze.onchain.flows" 10

# Off-Chain Events
create_topic "bronze.offchain.social" 4
create_topic "bronze.offchain.news" 6
create_topic "bronze.offchain.macro" 4

# Legacy Raw Data Topics (for backward compatibility)
create_topic "raw_data.exchange_feed" 20
create_topic "raw_data.options_chain" 12
create_topic "raw_data.onchain_events" 8
create_topic "raw_data.offchain_events" 6

# ═══════════════════════════════════════════════════════════════════════════════════
# 🥈 SILVER LAYER - Validated & Enriched (Quality Assured + Schema Compliant)
# ═══════════════════════════════════════════════════════════════════════════════════

# Quality Control
create_topic "silver.quality.validated" 12
create_topic "silver.quality.enriched" 10
create_topic "silver.quality.reconciled" 8

# Normalized Market Data
create_topic "silver.market.unified_trades" 16
create_topic "silver.market.normalized_book" 12
create_topic "silver.market.cross_venue_rates" 8

# Feature Engineering
create_topic "silver.features.technical_indicators" 10
create_topic "silver.features.volatility_metrics" 8
create_topic "silver.features.correlation_matrix" 6

# Risk Metrics
create_topic "silver.risk.var_estimates" 8
create_topic "silver.risk.exposure_metrics" 6
create_topic "silver.risk.stress_scenarios" 4

# Legacy Clean Data Topics (for backward compatibility)
create_topic "clean.pass_fail" 4

# ═══════════════════════════════════════════════════════════════════════════════════
# 🥇 GOLD LAYER - Sophisticated Analytics for Intraday/Intraweek Alpha Generation
# ═══════════════════════════════════════════════════════════════════════════════════

# Mathematical Feature Engineering (Sophisticated Statistics)
create_topic "gold.analytics.statistical_features" 12
create_topic "gold.analytics.correlation_matrices" 10
create_topic "gold.analytics.regime_detection" 8
create_topic "gold.analytics.factor_decomposition" 8

# Hidden Value Discovery (Innovation-Focused)
create_topic "gold.discovery.hidden_patterns" 10
create_topic "gold.discovery.value_buckets" 8
create_topic "gold.discovery.market_inefficiencies" 8
create_topic "gold.discovery.behavioral_anomalies" 6

# Cross-Asset Intelligence (Comprehensive Coverage)
create_topic "gold.intelligence.cross_market_signals" 10
create_topic "gold.intelligence.macro_factor_exposure" 8
create_topic "gold.intelligence.liquidity_dynamics" 8
create_topic "gold.intelligence.volatility_clustering" 6

# Advanced Mathematical Models (Sophisticated Algorithms)
create_topic "gold.models.machine_learning_features" 12
create_topic "gold.models.statistical_arbitrage" 10
create_topic "gold.models.behavioral_finance_signals" 8
create_topic "gold.models.complexity_theory_metrics" 6

# Innovation Lab (Experimental Features)
create_topic "gold.innovation.novel_indicators" 8
create_topic "gold.innovation.crypto_native_features" 8
create_topic "gold.innovation.multi_dimensional_analysis" 6
create_topic "gold.innovation.network_theory_metrics" 6

# ═══════════════════════════════════════════════════════════════════════════════════
# 🛡️ OPERATIONAL TOPICS - Monitoring & Control
# ═══════════════════════════════════════════════════════════════════════════════════

# Incident Management
create_topic "incidents.SchemaViolation" 6
create_topic "incidents.Freshness" 4
create_topic "incidents.Anomaly" 6
create_topic "incidents.Leakage" 4

# System Control
create_topic "control.circuit_breaker" 2
create_topic "control.command_acks" 2

# Data Lineage & Audit
create_topic "audit.data_lineage" 4
create_topic "audit.quality_metrics" 6
create_topic "audit.reconciliation_reports" 4

echo "✅ All topics created successfully!"
echo "📊 Listing all topics:"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list