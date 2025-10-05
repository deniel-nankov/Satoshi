# Satoshi - Institutional HFT Data Infrastructure

Real-time market data processing, risk management, and algorithmic trading platform built for institutional high-frequency trading operations.

## Architecture Overview

```
Satoshi/
├── engines/          # Core processing engines
│   ├── data/        # Market data ingestion & validation
│   ├── execution/   # Order management & execution
│   ├── features/    # Feature engineering pipeline
│   ├── governance/  # Data governance & compliance
│   ├── modeling/    # Quantitative models & signals
│   ├── risk/        # Real-time risk management
│   └── strategy/    # Trading strategy execution
├── infra/           # Infrastructure layer
│   ├── api/         # Health monitoring & metrics API
│   ├── bus/         # Message streaming (Kafka/Redpanda)
│   ├── columnar/    # Columnar storage (Parquet/Delta)
│   ├── lakehouse/   # Data lakehouse operations
│   ├── monitoring/  # Observability & alerting (Prometheus/Grafana)
│   ├── registry/    # Schema & metadata registry
│   ├── secrets/     # Secrets management
│   └── tsdb/        # Time-series database
├── orchestration/   # Workflow management (Dagster)
├── tests/           # Comprehensive test suite
│   ├── unit/        # Unit tests
│   ├── integration/ # Integration tests
│   └── performance/ # Performance benchmarks
├── pyproject.toml   # Poetry dependency management
├── Makefile         # Development commands
└── dev.sh           # Development workflow script
```

## Quick Start

```bash
# Setup development environment
make setup

# Run linting & formatting
make lint
make format

# Run tests
make test-unit          # Unit tests only
make test-integration   # Integration tests
make test-all          # All tests with coverage

# Start development services
make kafka             # Start Kafka cluster
make api               # Start FastAPI server
make dagster           # Start Dagster UI
make start             # Start all services

# Development workflow
make dev               # Format + lint + test
make ci                # Full CI/CD simulation

# Stop services
make stop
```

## Development Commands

```bash
# Using Makefile (recommended)
make help              # Show all available commands
make setup             # Initial setup
make dev               # Quick development cycle
make start             # Start all services

# Using development script
./dev.sh help          # Show help
./dev.sh setup         # Setup environment
./dev.sh start         # Start services
./dev.sh test unit     # Run unit tests

# Manual commands
poetry install         # Install dependencies
poetry run pytest     # Run tests
poetry run ruff check  # Lint code
```

## Development Workflow

1. **Code Quality**: All code must pass ruff, mypy, and pytest
2. **Performance**: Sub-microsecond latency requirements for trading paths
3. **Reliability**: 99.99% uptime with circuit breakers and graceful degradation
4. **Observability**: Comprehensive metrics, logging, and distributed tracing

## Key Features

- **Real-time Market Data**: Multi-venue tick data ingestion (trades, order books, options)
- **Data Quality**: Schema validation, anomaly detection, freshness monitoring
- **Risk Management**: Real-time position, market, and operational risk controls
- **Execution**: Low-latency order management with smart routing
- **Analytics**: Feature engineering and quantitative signal generation
- **Compliance**: Data lineage, audit trails, and regulatory reporting

## Performance Specifications

- **Latency**: <10μs order processing, <100μs risk checks
- **Throughput**: 1M+ messages/second per topic
- **Storage**: Petabyte-scale with microsecond-precision timestamps
- **Availability**: 99.99% uptime with <1s failover

## Technology Stack

- **Language**: Python 3.11+ with AsyncIO
- **Messaging**: Kafka/Redpanda for streaming
- **Storage**: Delta Lake + PostgreSQL for OLTP
- **Monitoring**: Prometheus + Grafana
- **Orchestration**: Dagster for batch workflows
- **API**: FastAPI for health/metrics endpoints

## License

Proprietary - All rights reserved
