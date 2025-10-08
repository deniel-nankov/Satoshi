#!/usr/bin/env python3
"""
Storage Infrastructure - Main Module
Entry point for the complete storage infrastructure.
"""

# Core components that should always work
try:
    from .bus.streaming_bus import StreamingBus
except ImportError as e:
    print(f"⚠️  StreamingBus import warning: {e}")
    StreamingBus = None

# Optional components - graceful degradation
try:
    from .columnar.arrow_flight import FeatureFlightServer, FeatureFlightClient, FeatureVector
except ImportError as e:
    print(f"⚠️  Arrow Flight components not available: {e}")
    FeatureFlightServer = FeatureFlightClient = FeatureVector = None

try:
    from .lakehouse.iceberg_lakehouse import LakehouseManager, DatasetConfig, DatasetType
except ImportError as e:
    print(f"⚠️  Lakehouse components not available: {e}")
    LakehouseManager = DatasetConfig = DatasetType = None

try:
    from .tsdb.clickhouse_tsdb import ClickHouseTSDB, TSDBConfig, MetricType
except ImportError as e:
    print(f"⚠️  ClickHouse TSDB not available: {e}")
    ClickHouseTSDB = TSDBConfig = MetricType = None

try:
    from .registry.postgres_registry import PostgreSQLRegistry, RegistryConfig, FeatureSpec, ModelArtifact
except ImportError as e:
    print(f"⚠️  PostgreSQL Registry not available: {e}")
    PostgreSQLRegistry = RegistryConfig = FeatureSpec = ModelArtifact = None

try:
    from .secrets.secrets_manager import SecretsManager, SecretsConfig, SecretType, SecretProvider
except ImportError as e:
    print(f"⚠️  Secrets Manager not available: {e}")
    SecretsManager = SecretsConfig = SecretType = SecretProvider = None

__version__ = "1.0.0"
__author__ = "Satoshi Trading System"

# Only export components that loaded successfully
__all__ = []

if StreamingBus:
    __all__.append("StreamingBus")

if FeatureFlightServer:
    __all__.extend(["FeatureFlightServer", "FeatureFlightClient", "FeatureVector"])

if LakehouseManager:
    __all__.extend(["LakehouseManager", "DatasetConfig", "DatasetType"])

if ClickHouseTSDB:
    __all__.extend(["ClickHouseTSDB", "TSDBConfig", "MetricType"])

if PostgreSQLRegistry:
    __all__.extend(["PostgreSQLRegistry", "RegistryConfig", "FeatureSpec", "ModelArtifact"])

if SecretsManager:
    __all__.extend(["SecretsManager", "SecretsConfig", "SecretType", "SecretProvider"])
