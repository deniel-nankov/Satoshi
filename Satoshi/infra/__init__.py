#!/usr/bin/env python3
"""
Storage Infrastructure - Main Module
Entry point for the complete storage infrastructure.
"""

from .bus.streaming_bus import StreamingBus
from .columnar.arrow_flight import FeatureFlightServer, FeatureFlightClient, FeatureVector
from .lakehouse.iceberg_lakehouse import LakehouseManager, DatasetConfig, DatasetType
from .tsdb.clickhouse_tsdb import ClickHouseTSDB, TSDBConfig, MetricType
from .registry.postgres_registry import PostgreSQLRegistry, RegistryConfig, FeatureSpec, ModelArtifact
from .secrets.secrets_manager import SecretsManager, SecretsConfig, SecretType, SecretProvider

__version__ = "1.0.0"
__author__ = "Satoshi Trading System"

__all__ = [  
    # Streaming bus
    "StreamingBus", 
    
    # Columnar IPC
    "FeatureFlightServer",
    "FeatureFlightClient", 
    "FeatureVector",
    
    # Lakehouse
    "LakehouseManager",
    "DatasetConfig",
    "DatasetType",
    
    # TSDB
    "ClickHouseTSDB",
    "TSDBConfig", 
    "MetricType",
    
    # Registry
    "PostgreSQLRegistry",
    "RegistryConfig",
    "FeatureSpec",
    "ModelArtifact",
    
    # Secrets
    "SecretsManager",
    "SecretsConfig",
    "SecretType", 
    "SecretProvider"
]
