#!/usr/bin/env python3
"""
Lakehouse Infrastructure - Parquet + Iceberg
Time-travel capable data lake for historical analysis and backfills.

Key Features:
- Time-travel queries for model training and backtesting
- Schema evolution without breaking existing queries
- ACID transactions for data consistency
- Efficient columnar storage with Parquet
- Partition pruning for fast queries
- Compaction and cleanup automation
"""

import asyncio
import logging
import time
import os
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict
from enum import Enum
import json
from pathlib import Path
import hashlib

# PyIceberg imports (when available)
try:
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.table import Table
    from pyiceberg.schema import Schema
    from pyiceberg.types import *
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import YearTransform, MonthTransform, DayTransform, IdentityTransform
    ICEBERG_AVAILABLE = True
except ImportError:
    ICEBERG_AVAILABLE = False
    print("⚠️  PyIceberg not installed. Install with: pip install pyiceberg")

# PyArrow for Parquet
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.dataset as ds
    ARROW_AVAILABLE = True
except ImportError:
    ARROW_AVAILABLE = False
    print("⚠️  PyArrow not installed. Install with: pip install pyarrow")

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

class DatasetType(Enum):
    """Types of datasets in the lakehouse."""
    RAW_MARKET_DATA = "raw_market_data"
    CLEAN_MARKET_DATA = "clean_market_data"
    FEATURES = "features"
    LABELS = "labels"
    SIGNALS = "signals"
    BACKTEST_RESULTS = "backtest_results"
    MODEL_PREDICTIONS = "model_predictions"
    EXECUTION_TELEMETRY = "execution_telemetry"
    INCIDENTS = "incidents"

@dataclass
class PartitionConfig:
    """Partitioning configuration for optimal query performance."""
    partition_by: List[str]  # e.g., ["year", "month", "day", "venue"]
    sort_by: List[str]  # e.g., ["timestamp", "symbol"]
    bucket_count: Optional[int] = None  # For hash partitioning

@dataclass
class RetentionPolicy:
    """Data retention and lifecycle management."""
    hot_tier_days: int  # Keep in fast storage
    warm_tier_days: int  # Move to cheaper storage
    cold_tier_days: int  # Archive storage
    deletion_days: Optional[int] = None  # Delete after this many days

@dataclass
class DatasetConfig:
    """Configuration for lakehouse datasets."""
    name: str
    dataset_type: DatasetType
    schema: Dict[str, str]  # Simplified schema definition
    partition_config: PartitionConfig
    retention_policy: RetentionPolicy
    description: str
    owner: str

class LakehouseManager:
    """
    Manages the Iceberg lakehouse for time-travel analytics.
    Handles schema evolution, partitioning, and data lifecycle.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize lakehouse manager."""
        self.config = config
        
        # Secure warehouse path configuration
        if "warehouse_path" not in config:
            # Check if in production
            is_production = config.get("environment", "").lower() in ["prod", "production"]
            if is_production:
                raise ValueError("warehouse_path must be explicitly configured in production environment")
            
            # Check for environment variable first
            env_warehouse_path = os.environ.get("SATOSHI_WAREHOUSE_PATH")
            if env_warehouse_path:
                self.warehouse_path = env_warehouse_path
            else:
                # Use secure default under user's home directory
                home_dir = Path.home()
                self.warehouse_path = str(home_dir / ".satoshi" / "lakehouse")
        else:
            self.warehouse_path = config["warehouse_path"]
            
        self.catalog_uri = config.get("catalog_uri", f"sqlite:///{self.warehouse_path}/catalog.db")
        
        # Ensure warehouse directory exists with restrictive permissions
        warehouse_dir = Path(self.warehouse_path)
        warehouse_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        
        # Initialize catalog (if Iceberg available)
        self.catalog = None
        if ICEBERG_AVAILABLE:
            self.catalog = SqlCatalog(
                "satoshi_catalog",
                **{
                    "uri": self.catalog_uri,
                    "warehouse": f"file://{self.warehouse_path}"
                }
            )
        
        # Dataset configurations
        self.dataset_configs: Dict[str, DatasetConfig] = {}
        
        # Performance metrics
        self.metrics = {
            "tables_created": 0,
            "writes_completed": 0,
            "reads_completed": 0,
            "bytes_written": 0,
            "bytes_read": 0,
            "query_latency_ms": 0
        }
        
        # Setup standard datasets
        self._setup_standard_datasets()
        
        logger.info(f"Lakehouse initialized at {self.warehouse_path}")
    
    def _setup_standard_datasets(self) -> None:
        """Setup standard dataset configurations."""
        
        # Raw market data
        self.register_dataset_config(DatasetConfig(
            name="raw_trades",
            dataset_type=DatasetType.RAW_MARKET_DATA,
            schema={
                "timestamp_ns": "int64",
                "venue": "string",
                "symbol": "string",
                "price": "double",
                "size": "double",
                "side": "string",
                "trade_id": "string",
                "record_id": "string"
            },
            partition_config=PartitionConfig(
                partition_by=["venue", "year", "month", "day"],
                sort_by=["timestamp_ns", "symbol"]
            ),
            retention_policy=RetentionPolicy(
                hot_tier_days=7,
                warm_tier_days=90,
                cold_tier_days=365 * 2
            ),
            description="Raw trade executions from all venues",
            owner="data_ingestion"
        ))
        
        # Features dataset
        self.register_dataset_config(DatasetConfig(
            name="features_vector",
            dataset_type=DatasetType.FEATURES,
            schema={
                "entity": "string",
                "asof_timestamp_ns": "int64",
                "window_size": "int32",
                "horizon": "int32",
                "feature_type": "string",
                "feature_values": "string",  # JSON encoded
                "leakage_proof_id": "string",
                "lineage": "string"  # JSON encoded
            },
            partition_config=PartitionConfig(
                partition_by=["feature_type", "year", "month"],
                sort_by=["asof_timestamp_ns", "entity"]
            ),
            retention_policy=RetentionPolicy(
                hot_tier_days=30,
                warm_tier_days=180,
                cold_tier_days=365 * 5
            ),
            description="Computed feature vectors for model training",
            owner="feature_factory"
        ))
        
        # Backtest results
        self.register_dataset_config(DatasetConfig(
            name="backtest_results",
            dataset_type=DatasetType.BACKTEST_RESULTS,
            schema={
                "experiment_id": "string",
                "model_version": "string",
                "start_date": "date32",
                "end_date": "date32",
                "strategy": "string",
                "returns": "double",
                "sharpe": "double",
                "max_drawdown": "double",
                "win_rate": "double",
                "config": "string"  # JSON encoded
            },
            partition_config=PartitionConfig(
                partition_by=["strategy", "year"],
                sort_by=["start_date", "experiment_id"]
            ),
            retention_policy=RetentionPolicy(
                hot_tier_days=90,
                warm_tier_days=365,
                cold_tier_days=365 * 10  # Keep backtests for 10 years
            ),
            description="Historical backtest results and performance metrics",
            owner="research"
        ))
    
    def register_dataset_config(self, config: DatasetConfig) -> None:
        """Register a dataset configuration."""
        self.dataset_configs[config.name] = config
        logger.info(f"Registered dataset config: {config.name}")
    
    def create_table(self, dataset_name: str) -> bool:
        """Create an Iceberg table from dataset configuration."""
        if dataset_name not in self.dataset_configs:
            logger.error(f"Dataset configuration not found: {dataset_name}")
            return False
        
        config = self.dataset_configs[dataset_name]
        
        if not ICEBERG_AVAILABLE:
            logger.warning("Iceberg not available, creating Parquet dataset instead")
            return self._create_parquet_dataset(config)
        
        try:
            # Convert schema to Iceberg schema
            iceberg_schema = self._create_iceberg_schema(config.schema)
            
            # Create partition spec
            partition_spec = self._create_partition_spec(config.partition_config)
            
            # Create table
            self.catalog.create_table(
                identifier=f"satoshi.{dataset_name}",
                schema=iceberg_schema,
                partition_spec=partition_spec,
                properties={
                    "write.format.default": "parquet",
                    "write.parquet.compression-codec": "zstd",
                    "write.target-file-size-bytes": "134217728",  # 128MB
                    "history.expire.max-snapshot-age-ms": str(30 * 24 * 60 * 60 * 1000)  # 30 days
                }
            )
            
            self.metrics["tables_created"] += 1
            logger.info(f"Created Iceberg table: {dataset_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create table {dataset_name}: {e}")
            return False
    
    def _create_iceberg_schema(self, schema_def: Dict[str, str]) -> Schema:
        """Convert schema definition to Iceberg schema."""
        fields = []
        field_id = 1
        
        type_mapping = {
            "string": StringType(),
            "int32": IntegerType(),
            "int64": LongType(),
            "double": DoubleType(),
            "float": FloatType(),
            "boolean": BooleanType(),
            "date32": DateType(),
            "timestamp": TimestampType(),
            "binary": BinaryType()
        }
        
        for field_name, field_type in schema_def.items():
            iceberg_type = type_mapping.get(field_type, StringType())
            fields.append(NestedField(
                field_id=field_id,
                name=field_name,
                field_type=iceberg_type,
                required=True
            ))
            field_id += 1
        
        return Schema(*fields)
    
    def _create_partition_spec(self, partition_config: PartitionConfig) -> PartitionSpec:
        """Create Iceberg partition specification."""
        fields = []
        field_id = 1000  # Start partition field IDs at 1000
        
        for partition_field in partition_config.partition_by:
            if partition_field in ["year", "month", "day"]:
                # Date partitioning with proper transform objects
                source_field = "timestamp_ns" if "timestamp_ns" in partition_field else "asof_timestamp_ns"
                
                # Create proper PyIceberg transform objects
                if partition_field == "year":
                    transform = YearTransform()
                elif partition_field == "month":
                    transform = MonthTransform()
                elif partition_field == "day":
                    transform = DayTransform()
            else:
                # Identity partitioning for non-date fields
                transform = IdentityTransform()
            
            fields.append(PartitionField(
                source_id=field_id,
                field_id=field_id,
                transform=transform,
                name=partition_field
            ))
            field_id += 1
        
        return PartitionSpec(*fields)
    
    def _create_parquet_dataset(self, config: DatasetConfig) -> bool:
        """Create Parquet dataset as fallback when Iceberg unavailable."""
        try:
            dataset_path = Path(self.warehouse_path) / config.name
            dataset_path.mkdir(parents=True, exist_ok=True)
            
            # Save configuration
            config_path = dataset_path / "config.json"
            with open(config_path, 'w') as f:
                json.dump(asdict(config), f, indent=2, default=str)
            
            logger.info(f"Created Parquet dataset: {config.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create Parquet dataset {config.name}: {e}")
            return False
    
    def write_data(self, dataset_name: str, data: pd.DataFrame, 
                   mode: str = "append") -> bool:
        """
        Write data to a dataset.
        
        Args:
            dataset_name: Name of the dataset
            data: DataFrame to write
            mode: "append" or "overwrite"
            
        Returns:
            bool: Success status
        """
        if dataset_name not in self.dataset_configs:
            logger.error(f"Dataset not found: {dataset_name}")
            return False
        
        try:
            start_time = time.time()
            
            if ICEBERG_AVAILABLE and self.catalog:
                # Write to Iceberg table
                table = self.catalog.load_table(f"satoshi.{dataset_name}")
                
                if mode == "overwrite":
                    table.overwrite(data)
                else:
                    table.append(data)
            
            else:
                # Write to Parquet dataset
                dataset_path = Path(self.warehouse_path) / dataset_name
                
                # Add partitioning columns if needed
                config = self.dataset_configs[dataset_name]
                if "timestamp_ns" in data.columns:
                    data = self._add_partition_columns(data, config.partition_config)
                
                # Write Parquet file
                timestamp = int(time.time() * 1000)
                file_path = dataset_path / f"data_{timestamp}.parquet"
                
                if ARROW_AVAILABLE:
                    table = pa.Table.from_pandas(data)
                    pq.write_table(table, file_path, compression="zstd")
                else:
                    data.to_parquet(file_path, compression="zstd")
            
            # Update metrics
            end_time = time.time()
            self.metrics["writes_completed"] += 1
            self.metrics["bytes_written"] += len(data) * data.memory_usage(deep=True).sum()
            
            logger.info(f"Wrote {len(data)} rows to {dataset_name} in {(end_time - start_time)*1000:.1f}ms")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write data to {dataset_name}: {e}")
            return False
    
    def read_data(self, dataset_name: str, 
                  start_time: Optional[int] = None,
                  end_time: Optional[int] = None,
                  filters: Optional[List[tuple]] = None,
                  columns: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        Read data from a dataset with optional filtering.
        
        Args:
            dataset_name: Name of the dataset
            start_time: Start timestamp (nanoseconds)
            end_time: End timestamp (nanoseconds)
            filters: Additional filters as list of tuples
            columns: Columns to select
            
        Returns:
            DataFrame or None if failed
        """
        if dataset_name not in self.dataset_configs:
            logger.error(f"Dataset not found: {dataset_name}")
            return None
        
        try:
            start_read_time = time.time()
            
            if ICEBERG_AVAILABLE and self.catalog:
                # Read from Iceberg table
                table = self.catalog.load_table(f"satoshi.{dataset_name}")
                
                # Build scan with filters
                scan = table.scan()
                
                if start_time and end_time:
                    # Add time range filter
                    timestamp_col = "timestamp_ns" if "timestamp_ns" in table.schema.column_names else "asof_timestamp_ns"
                    scan = scan.filter(f"{timestamp_col} >= {start_time} AND {timestamp_col} <= {end_time}")
                
                if columns:
                    scan = scan.select(*columns)
                
                # Execute scan
                df = scan.to_pandas()
            
            else:
                # Read from Parquet dataset
                dataset_path = Path(self.warehouse_path) / dataset_name
                
                if not dataset_path.exists():
                    logger.error(f"Dataset path not found: {dataset_path}")
                    return None
                
                # Read all Parquet files
                parquet_files = list(dataset_path.glob("*.parquet"))
                
                if not parquet_files:
                    logger.warning(f"No data files found in {dataset_path}")
                    return pd.DataFrame()
                
                dfs = []
                for file_path in parquet_files:
                    if ARROW_AVAILABLE:
                        table = pq.read_table(file_path, columns=columns)
                        df_chunk = table.to_pandas()
                    else:
                        df_chunk = pd.read_parquet(file_path, columns=columns)
                    
                    dfs.append(df_chunk)
                
                df = pd.concat(dfs, ignore_index=True)
                
                # Apply time filters
                if start_time and end_time:
                    timestamp_col = "timestamp_ns" if "timestamp_ns" in df.columns else "asof_timestamp_ns"
                    if timestamp_col in df.columns:
                        df = df[(df[timestamp_col] >= start_time) & (df[timestamp_col] <= end_time)]
            
            # Update metrics
            end_read_time = time.time()
            self.metrics["reads_completed"] += 1
            self.metrics["bytes_read"] += df.memory_usage(deep=True).sum()
            self.metrics["query_latency_ms"] = int((end_read_time - start_read_time) * 1000)
            
            logger.info(f"Read {len(df)} rows from {dataset_name} in {self.metrics['query_latency_ms']}ms")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read data from {dataset_name}: {e}")
            return None
    
    def _add_partition_columns(self, data: pd.DataFrame, 
                             partition_config: PartitionConfig) -> pd.DataFrame:
        """Add partitioning columns for efficient querying."""
        df = data.copy()
        
        # Add date partitions if timestamp exists
        timestamp_col = "timestamp_ns" if "timestamp_ns" in df.columns else "asof_timestamp_ns"
        
        if timestamp_col in df.columns:
            timestamps = pd.to_datetime(df[timestamp_col])
            
            if "year" in partition_config.partition_by:
                df["year"] = timestamps.dt.year
            if "month" in partition_config.partition_by:
                df["month"] = timestamps.dt.month
            if "day" in partition_config.partition_by:
                df["day"] = timestamps.dt.day
        
        return df
    
    def time_travel_query(self, dataset_name: str, timestamp: int,
                         filters: Optional[Dict[str, Any]] = None) -> Optional[pd.DataFrame]:
        """
        Query data as it existed at a specific timestamp (time travel).
        
        Args:
            dataset_name: Dataset name
            timestamp: Timestamp for time travel (nanoseconds)
            filters: Additional filters
            
        Returns:
            DataFrame at the specified time or None
        """
        if not ICEBERG_AVAILABLE:
            logger.warning("Time travel requires Iceberg. Using latest data instead.")
            return self.read_data(dataset_name)
        
        try:
            table = self.catalog.load_table(f"satoshi.{dataset_name}")
            
            # Convert nanoseconds to milliseconds for Iceberg snapshot comparison
            target_timestamp_ms = timestamp // 1_000_000
            
            # Get all snapshots
            history = table.history()
            
            # Find the most recent snapshot <= requested timestamp
            selected_snapshot = None
            for snapshot in history:
                # Iceberg snapshot timestamps are typically in milliseconds
                snapshot_timestamp_ms = snapshot.timestamp_ms
                
                if snapshot_timestamp_ms <= target_timestamp_ms:
                    if selected_snapshot is None or snapshot_timestamp_ms > selected_snapshot.timestamp_ms:
                        selected_snapshot = snapshot
            
            if selected_snapshot is None:
                logger.warning(f"No snapshot found for timestamp {timestamp} in {dataset_name}")
                return None
            
            # Read from selected snapshot
            scan = table.scan(snapshot_id=selected_snapshot.snapshot_id)
            
            return scan.to_pandas()
            
        except Exception as e:
            logger.error(f"Time travel query failed for {dataset_name}: {e}")
            return None
    
    def get_dataset_info(self, dataset_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a dataset."""
        if dataset_name not in self.dataset_configs:
            return None
        
        config = self.dataset_configs[dataset_name]
        
        info = {
            "name": config.name,
            "type": config.dataset_type.value,
            "schema": config.schema,
            "partitioning": config.partition_config.partition_by,
            "retention": {
                "hot_days": config.retention_policy.hot_tier_days,
                "warm_days": config.retention_policy.warm_tier_days,
                "cold_days": config.retention_policy.cold_tier_days
            },
            "description": config.description,
            "owner": config.owner
        }
        
        # Add runtime stats if available
        try:
            if ICEBERG_AVAILABLE and self.catalog:
                table = self.catalog.load_table(f"satoshi.{dataset_name}")
                info["records"] = table.scan().to_pandas().shape[0]
            else:
                # Count Parquet files
                dataset_path = Path(self.warehouse_path) / dataset_name
                info["files"] = len(list(dataset_path.glob("*.parquet")))
        except:
            pass
        
        return info
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get lakehouse performance metrics."""
        return self.metrics.copy()

# Example usage
async def main():
    """Example usage of the lakehouse system."""
    
    print("🏛️  Lakehouse Infrastructure Demo")
    print("=" * 50)
    
    config = {
        "warehouse_path": "/tmp/satoshi_lakehouse_demo",
        "catalog_uri": "sqlite:///tmp/satoshi_lakehouse_demo/catalog.db"
    }
    
    lakehouse = LakehouseManager(config)
    
    # Create tables
    for dataset_name in ["raw_trades", "features_vector", "backtest_results"]:
        success = lakehouse.create_table(dataset_name)
        print(f"✅ Created table: {dataset_name}" if success else f"❌ Failed: {dataset_name}")
    
    # Create sample data
    n_rows = 1000
    sample_trades = pd.DataFrame({
        "timestamp_ns": np.arange(n_rows) * 1_000_000_000,
        "venue": np.random.choice(["binance", "coinbase", "kraken"], n_rows),
        "symbol": np.random.choice(["BTC-PERP", "ETH-PERP"], n_rows),
        "price": 45000 + np.random.normal(0, 1000, n_rows),
        "size": np.random.uniform(0.1, 10.0, n_rows),
        "side": np.random.choice(["buy", "sell"], n_rows),
        "trade_id": [f"trade_{i}" for i in range(n_rows)],
        "record_id": [f"record_{i}" for i in range(n_rows)]
    })
    
    # Write data
    success = lakehouse.write_data("raw_trades", sample_trades)
    print(f"✅ Wrote {len(sample_trades)} trades" if success else "❌ Failed to write trades")
    
    # Read data back
    read_data = lakehouse.read_data("raw_trades", columns=["symbol", "price", "size"])
    if read_data is not None:
        print(f"✅ Read {len(read_data)} rows back")
        print(f"   Columns: {list(read_data.columns)}")
        print(f"   Price range: ${read_data['price'].min():.0f} - ${read_data['price'].max():.0f}")
    
    # Show dataset info
    info = lakehouse.get_dataset_info("raw_trades")
    print(f"\n📊 Dataset Info:")
    print(f"   Name: {info['name']}")
    print(f"   Type: {info['type']}")
    print(f"   Partitioning: {info['partitioning']}")
    print(f"   Hot retention: {info['retention']['hot_days']} days")
    
    # Show metrics
    metrics = lakehouse.get_metrics()
    print(f"\n📈 Metrics:")
    print(f"   Tables created: {metrics['tables_created']}")
    print(f"   Writes completed: {metrics['writes_completed']}")
    print(f"   Reads completed: {metrics['reads_completed']}")
    print(f"   Bytes written: {metrics['bytes_written']:,}")

if __name__ == "__main__":
    asyncio.run(main())
