# Data Pipeline - Bronze to Silver Layer

Complete data ingestion and quality pipeline implementation.

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│                     DATA COLLECTORS                          │
├──────────────────────────────────────────────────────────────┤
│  • ExchangeConnectorAgent    → raw_data.exchange_feed       │
│  • OptionsChainCollectorAgent → raw_data.options_chain       │
│  • OnchainCollectorAgent      → raw_data.onchain_events      │
│  • EventsCollectorAgent       → raw_data.offchain_events     │
└──────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────┐
│              DATA QUALITY ORCHESTRATOR (Bronze→Silver)        │
├──────────────────────────────────────────────────────────────┤
│  Pipeline Stages:                                            │
│  1. Schema Validation        → Structural integrity          │
│  2. Leakage Detection        → Temporal contamination        │
│  3. Anomaly Detection        → Statistical outliers          │
│  4. Freshness Validation     → Data timeliness              │
│  5. Cross-Source Reconciliation → Multi-source validation    │
│  6. Final Quality Scoring    → Overall quality metrics       │
└──────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────┐
│                        OUTPUTS                               │
├──────────────────────────────────────────────────────────────┤
│  • clean.market.trades       → Validated trade data         │
│  • clean.market.options      → Validated options data       │
│  • clean.market.onchain      → Validated blockchain data    │
│  • clean.market.events       → Validated event data         │
│  • incidents.*               → Quality issues & alerts       │
└──────────────────────────────────────────────────────────────┘
```

## Components Status

### ✅ Fully Implemented
- **Data Quality Orchestrator** - 6-stage quality pipeline
- **Schema Validator Agent** - Structure validation
- **Leakage Police** - Temporal contamination detection
- **Anomaly Detector** - Statistical outlier detection
- **Freshness Agent** - Data timeliness validation
- **Reconciler Agent** - Cross-source validation
- **Exchange Connector Agent** - CEX/DEX market data
- **Options Chain Collector** - Options market data
- **Onchain Collector** - Blockchain events
- **Events Collector** - Off-chain events (social, news)

### 🔧 Infrastructure
- **Streaming Bus** - Kafka/Redpanda integration with circuit breakers
- **Metrics Collection** - Prometheus-compatible metrics
- **Circuit Breaker System** - Fault tolerance and recovery

## Prerequisites

### Required
1. **Kafka/Redpanda** running on `localhost:9092` (or configured endpoint)
2. **Python 3.11+**
3. **Dependencies** (install via `pip install -r infra/requirements.txt`)

### Optional (for full functionality)
- Exchange API keys (Binance, Coinbase, etc.)
- Blockchain RPC endpoints (Alchemy, Infura, etc.)
- Social media API keys (Twitter, etc.)

## Quick Start

### 1. Test Pipeline (No External Dependencies)

```bash
# Test with mock data - validates pipeline works end-to-end
python test_data_pipeline.py
```

This will:
- Publish mock data to `raw_data.*` topics
- Process through quality pipeline
- Output to `clean.*` and `incidents.*` topics
- Report test results

Expected output:
```
✅ TEST PASSED - Pipeline is operational!
Raw data published:    4
Clean data received:   4
Incidents received:    0-2
```

### 2. Run Full Pipeline (Requires API Keys)

```bash
# Development mode (lenient quality thresholds)
python run_data_pipeline.py

# Institutional mode (strict quality thresholds)
python run_data_pipeline.py institutional
```

Configuration needed in `run_data_pipeline.py`:
- Exchange API keys
- Blockchain RPC URLs
- Social media API keys

## Configuration

### Development Mode
- Quality Threshold: 90%
- Failure Tolerance: 20 failures before circuit break
- Recovery Timeout: 30 seconds
- Mode: `RESILIENT` (allows warnings)

### Institutional Mode
- Quality Threshold: 99%
- Failure Tolerance: 5 failures before circuit break
- Recovery Timeout: 60 seconds
- Mode: `STRICT` (all checks must pass)

## Monitoring

### Logs
- Console output with INFO level
- File output: `data_pipeline.log`

### Metrics
Access Prometheus metrics via the metrics collector:
```python
from infra.monitoring.prometheus_metrics import get_metrics_collector
metrics = get_metrics_collector()
```

Key metrics:
- `quality_pipeline_duration_seconds` - Processing latency
- `quality_pipeline_decisions_total` - Pass/fail counts
- `quality_circuit_breaker_state` - Circuit breaker status
- `messages_sent` / `messages_received` - Throughput

### Health Checks

Get orchestrator health:
```python
health = await orchestrator.get_orchestration_health()
```

Returns:
- `orchestrator_running` - Pipeline status
- `pipeline_mode` - Current mode (STRICT/RESILIENT/DEGRADED/EMERGENCY)
- `circuit_breaker_open` - Circuit breaker state
- `component_health` - Individual agent health
- `quality_threshold` - Current quality threshold

## Data Flow Details

### Input Topics (Bronze Layer)
| Topic | Source | Data Type | Retention |
|-------|--------|-----------|-----------|
| `raw_data.exchange_feed` | Exchange Connector | Market trades | 7 days |
| `raw_data.options_chain` | Options Collector | Options surface | 7 days |
| `raw_data.onchain_events` | Onchain Collector | Blockchain events | 30 days |
| `raw_data.offchain_events` | Events Collector | Social/news events | 7 days |

### Output Topics (Silver Layer)
| Topic | Content | Quality | Retention |
|-------|---------|---------|-----------|
| `clean.market.trades` | Validated trades | 95%+ | 24 hours |
| `clean.market.options` | Validated options | 95%+ | 24 hours |
| `clean.market.onchain` | Validated blockchain | 95%+ | 7 days |
| `clean.market.events` | Validated events | 95%+ | 24 hours |

### Incident Topics
| Topic | Severity | Description |
|-------|----------|-------------|
| `incidents.SchemaViolation` | High | Data structure issues |
| `incidents.Leakage` | Critical | Temporal contamination |
| `incidents.Anomaly` | Medium | Statistical outliers |
| `incidents.Freshness` | Medium | Stale data warnings |

## Quality Pipeline Details

### Stage 1: Schema Validation
- Validates data structure against registered schemas
- Canonicalizes field names and types
- **Threshold**: 99% (strict)
- **Timeout**: 500ms

### Stage 2: Leakage Detection
- Prevents future data from contaminating current state
- Checks temporal ordering
- **Threshold**: 100% (zero tolerance)
- **Timeout**: 1000ms

### Stage 3: Anomaly Detection
- Statistical outlier detection
- Z-score and IQR analysis
- **Threshold**: 90%
- **Timeout**: 800ms

### Stage 4: Freshness Validation
- Checks data timeliness
- Monitors staleness
- **Threshold**: 95%
- **Timeout**: 300ms

### Stage 5: Cross-Source Reconciliation
- Validates against multiple sources
- Detects discrepancies
- **Threshold**: 93%
- **Timeout**: 2000ms

### Stage 6: Final Quality Scoring
- Weighted scoring across all stages
- Overall quality assessment
- **Threshold**: 95%
- **Timeout**: 200ms

## Troubleshooting

### Pipeline Not Starting

```bash
# Check Kafka is running
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Check Python dependencies
pip install -r infra/requirements.txt

# Check logs
tail -f data_pipeline.log
```

### No Clean Data Output

Common issues:
1. **Quality threshold too strict** - Lower threshold in development mode
2. **All agents not registered** - Check logs for missing agents
3. **Circuit breaker open** - Wait for recovery timeout or reset manually
4. **Input data malformed** - Check raw_data topic messages

### High Incident Rate

If seeing many incidents:
1. **Schema violations** - Update schemas or fix input data format
2. **Freshness issues** - Check data source latency
3. **Anomalies** - May be legitimate market events or need tuning

## Next Steps

### Completed ✅
- Bronze → Silver data quality pipeline
- All data collectors implemented
- Quality agents operational

### Next Phase 🚧
- **Feature Engineering Layer** (Silver → Gold)
  - Feature Factory
  - Specialized feature agents
  - Feature Store for optimized serving

### Future Enhancements 📋
- Real-time quality dashboards (Grafana)
- Advanced anomaly models (ML-based)
- Multi-region deployment
- Enhanced reconciliation logic

## Architecture Compliance

This implementation follows `docs/ARCHITECTURE.md`:
- ✅ Data Quality Layer (Components: Orchestrator + 5 Quality Agents)
- ✅ Separation of Concerns (Business logic vs Infrastructure)
- ✅ Circuit Breaker Integration
- ✅ Institutional-grade data integrity
- ✅ Comprehensive monitoring

## Support

For issues or questions:
1. Check logs in `data_pipeline.log`
2. Review architecture docs in `docs/`
3. Check component health status
4. Run test pipeline for validation

---

**Status**: ✅ **Production Ready** (with proper API configuration)
**Last Updated**: October 15, 2025
