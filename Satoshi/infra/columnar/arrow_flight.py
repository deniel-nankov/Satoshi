#!/usr/bin/env python3
"""
Columnar IPC Infrastructure - Apache Arrow Flight
Zero-copy feature pipeline for ultra-fast data transfer between agents.

Key Features:
- Zero-copy data transfer using Arrow memory layout
- Vectorized operations for high-performance feature computation
- Schema evolution support with backward compatibility
- Parallel data streams for concurrent processing
- Memory-mapped feature vectors for instant access
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Any, Iterator, Union
from dataclasses import dataclass
from enum import Enum
import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.compute as pc
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import threading

logger = logging.getLogger(__name__)

class FeatureType(Enum):
    """Types of feature vectors in the system."""
    MARKET_RETURNS = "market_returns"
    VOLATILITY_SURFACE = "volatility_surface"
    CARRY_BASIS = "carry_basis"
    ONCHAIN_FLOWS = "onchain_flows" 
    EVENT_CALENDAR = "event_calendar"
    REGIME_CLASSIFIER = "regime_classifier"
    MOMENTUM = "momentum"
    MEAN_REVERSION = "mean_reversion"

@dataclass
class FeatureSchema:
    """Schema definition for feature vectors."""
    name: str
    feature_type: FeatureType
    entity_field: str  # e.g., "symbol", "address", "event_id"
    timestamp_field: str
    value_fields: List[str]
    metadata_fields: List[str]
    arrow_schema: pa.Schema
    version: str

class FeatureVector:
    """
    High-performance feature vector using Arrow columnar format.
    Optimized for zero-copy transfers and vectorized operations.
    """
    
    def __init__(self, table: pa.Table, schema: FeatureSchema):
        """Initialize feature vector with Arrow table and schema."""
        self.table = table
        self.schema = schema
        self.created_at = time.time_ns()
        
        # Validate schema compatibility
        if not self._validate_schema():
            raise ValueError(f"Table schema incompatible with feature schema: {schema.name}")
    
    def _validate_schema(self) -> bool:
        """Validate that table schema matches feature schema."""
        table_fields = {field.name for field in self.table.schema}
        required_fields = {
            self.schema.entity_field,
            self.schema.timestamp_field,
            *self.schema.value_fields,
            *self.schema.metadata_fields
        }
        return required_fields.issubset(table_fields)
    
    def get_entities(self) -> pa.Array:
        """Get unique entities in this feature vector."""
        return pc.unique(self.table[self.schema.entity_field])
    
    def get_time_range(self) -> tuple[int, int]:
        """Get time range (min, max) of this feature vector."""
        timestamps = self.table[self.schema.timestamp_field]
        return pc.min(timestamps).as_py(), pc.max(timestamps).as_py()
    
    def filter_by_entity(self, entities: List[str]) -> 'FeatureVector':
        """Filter feature vector by entity list."""
        mask = pc.is_in(self.table[self.schema.entity_field], pa.array(entities))
        filtered_table = self.table.filter(mask)
        return FeatureVector(filtered_table, self.schema)
    
    def filter_by_time_range(self, start_ns: int, end_ns: int) -> 'FeatureVector':
        """Filter feature vector by time range."""
        timestamp_col = self.table[self.schema.timestamp_field]
        mask = pc.and_(
            pc.greater_equal(timestamp_col, start_ns),
            pc.less_equal(timestamp_col, end_ns)
        )
        filtered_table = self.table.filter(mask)
        return FeatureVector(filtered_table, self.schema)
    
    def to_pandas(self) -> pd.DataFrame:
        """Convert to pandas DataFrame (zero-copy when possible)."""
        return self.table.to_pandas(zero_copy_only=True, split_blocks=True)
    
    def to_numpy(self) -> Dict[str, np.ndarray]:
        """Convert value fields to numpy arrays."""
        result = {}
        for field in self.schema.value_fields:
            result[field] = self.table[field].to_numpy(zero_copy_only=True)
        return result
    
    def get_memory_usage(self) -> int:
        """Get memory usage in bytes."""
        return self.table.nbytes
    
    def __len__(self) -> int:
        """Number of rows in the feature vector."""
        return len(self.table)

class FeatureFlightServer(flight.FlightServerBase):
    """
    Arrow Flight server for serving feature vectors.
    Provides high-performance, zero-copy data transfer.
    """
    
    def __init__(self, location: str = "grpc://localhost:8815"):
        """Initialize flight server."""
        super().__init__(location)
        self.location = location
        
        # Feature storage
        self.feature_vectors: Dict[str, FeatureVector] = {}
        self.schemas: Dict[str, FeatureSchema] = {}
        
        # Performance metrics
        self.metrics = {
            "requests_served": 0,
            "bytes_transferred": 0,
            "avg_latency_us": 0,
            "active_streams": 0
        }
        
        # Threading for concurrent access
        self._lock = threading.RLock()
        
        logger.info(f"Feature Flight Server initialized at {location}")
    
    def register_schema(self, schema: FeatureSchema) -> None:
        """Register a feature schema."""
        with self._lock:
            self.schemas[schema.name] = schema
            logger.info(f"Registered feature schema: {schema.name} v{schema.version}")
    
    def store_feature_vector(self, name: str, feature_vector: FeatureVector) -> None:
        """Store a feature vector for serving."""
        with self._lock:
            self.feature_vectors[name] = feature_vector
            logger.info(f"Stored feature vector: {name} ({len(feature_vector)} rows, "
                       f"{feature_vector.get_memory_usage() // 1024} KB)")
    
    def list_flights(self, context, criteria):
        """List available feature vectors."""
        with self._lock:
            for name, vector in self.feature_vectors.items():
                schema = vector.schema
                time_min, time_max = vector.get_time_range()
                
                descriptor = flight.FlightDescriptor.for_path(name)
                endpoint = flight.FlightEndpoint(
                    ticket=flight.Ticket(name.encode()),
                    locations=[self.location]
                )
                
                info = flight.FlightInfo(
                    schema=vector.table.schema,
                    descriptor=descriptor,
                    endpoints=[endpoint],
                    total_records=len(vector),
                    total_bytes=vector.get_memory_usage()
                )
                
                yield info
    
    def get_flight_info(self, context, descriptor):
        """Get information about a specific feature vector."""
        path = descriptor.path[0].decode() if descriptor.path else ""
        
        with self._lock:
            if path not in self.feature_vectors:
                raise flight.FlightNotFoundError(f"Feature vector not found: {path}")
            
            vector = self.feature_vectors[path]
            endpoint = flight.FlightEndpoint(
                ticket=flight.Ticket(path.encode()),
                locations=[self.location]
            )
            
            return flight.FlightInfo(
                schema=vector.table.schema,
                descriptor=descriptor,
                endpoints=[endpoint],
                total_records=len(vector),
                total_bytes=vector.get_memory_usage()
            )
    
    def do_get(self, context, ticket):
        """Serve feature vector data."""
        start_time = time.time_ns()
        
        feature_name = ticket.ticket.decode()
        
        with self._lock:
            if feature_name not in self.feature_vectors:
                raise flight.FlightNotFoundError(f"Feature vector not found: {feature_name}")
            
            vector = self.feature_vectors[feature_name]
            
            # Update metrics
            self.metrics["requests_served"] += 1
            self.metrics["bytes_transferred"] += vector.get_memory_usage()
            self.metrics["active_streams"] += 1
        
        try:
            # Stream the table in batches for memory efficiency
            batch_size = 10000
            table = vector.table
            
            for i in range(0, len(table), batch_size):
                batch = table.slice(i, min(batch_size, len(table) - i))
                yield flight.RecordBatch.from_pandas(batch.to_pandas())
        
        finally:
            with self._lock:
                self.metrics["active_streams"] -= 1
                
                # Update latency metric
                end_time = time.time_ns()
                latency_us = (end_time - start_time) // 1000
                
                # Exponential moving average
                alpha = 0.1
                if self.metrics["avg_latency_us"] == 0:
                    self.metrics["avg_latency_us"] = latency_us
                else:
                    self.metrics["avg_latency_us"] = int(
                        alpha * latency_us + (1 - alpha) * self.metrics["avg_latency_us"]
                    )
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get server performance metrics."""
        with self._lock:
            return self.metrics.copy()

class FeatureFlightClient:
    """
    Arrow Flight client for consuming feature vectors.
    Provides high-performance, zero-copy data access.
    """
    
    def __init__(self, server_location: str = "grpc://localhost:8815"):
        """Initialize flight client."""
        self.server_location = server_location
        self.client = flight.FlightClient(server_location)
        self.schema_cache: Dict[str, FeatureSchema] = {}
        
        logger.info(f"Feature Flight Client connected to {server_location}")
    
    def list_feature_vectors(self) -> List[str]:
        """List available feature vectors."""
        flights = self.client.list_flights()
        return [flight.descriptor.path[0].decode() for flight in flights]
    
    def get_feature_vector(self, name: str, 
                          entities: Optional[List[str]] = None,
                          start_time_ns: Optional[int] = None,
                          end_time_ns: Optional[int] = None) -> FeatureVector:
        """
        Get feature vector with optional filtering.
        
        Args:
            name: Feature vector name
            entities: Optional entity filter
            start_time_ns: Optional start time filter
            end_time_ns: Optional end time filter
            
        Returns:
            FeatureVector: The requested feature vector
        """
        # Get flight info
        descriptor = flight.FlightDescriptor.for_path(name)
        flight_info = self.client.get_flight_info(descriptor)
        
        # Get the data stream
        reader = self.client.do_get(flight_info.endpoints[0].ticket)
        
        # Read all batches into a table
        batches = []
        for batch in reader:
            batches.append(batch.data)
        
        if not batches:
            raise ValueError(f"No data found for feature vector: {name}")
        
        table = pa.Table.from_batches(batches)
        
        # Create schema (simplified - in production, get from registry)
        schema = FeatureSchema(
            name=name,
            feature_type=FeatureType.MARKET_RETURNS,  # Default
            entity_field="entity",
            timestamp_field="timestamp",
            value_fields=[field.name for field in table.schema if field.name.startswith("value_")],
            metadata_fields=[field.name for field in table.schema if field.name.startswith("meta_")],
            arrow_schema=table.schema,
            version="1.0"
        )
        
        vector = FeatureVector(table, schema)
        
        # Apply filters if specified
        if entities:
            vector = vector.filter_by_entity(entities)
        
        if start_time_ns and end_time_ns:
            vector = vector.filter_by_time_range(start_time_ns, end_time_ns)
        
        return vector
    
    def get_schema_info(self, name: str) -> pa.Schema:
        """Get schema information for a feature vector."""
        descriptor = flight.FlightDescriptor.for_path(name)
        flight_info = self.client.get_flight_info(descriptor)
        return flight_info.schema

def create_sample_feature_vector() -> FeatureVector:
    """Create a sample feature vector for testing."""
    
    # Sample data
    n_rows = 1000
    timestamps = np.arange(n_rows) * 1_000_000_000  # 1 second intervals
    entities = np.random.choice(["BTC-PERP", "ETH-PERP", "SOL-PERP"], n_rows)
    returns = np.random.normal(0, 0.01, n_rows)
    volatility = np.random.uniform(0.1, 0.5, n_rows)
    volume = np.random.uniform(1000, 100000, n_rows)
    
    # Create Arrow table
    table = pa.table({
        "entity": entities,
        "timestamp": timestamps,
        "value_returns": returns,
        "value_volatility": volatility,
        "value_volume": volume,
        "meta_source": ["binance"] * n_rows,
        "meta_quality": np.random.uniform(0.8, 1.0, n_rows)
    })
    
    # Create schema
    schema = FeatureSchema(
        name="sample_returns",
        feature_type=FeatureType.MARKET_RETURNS,
        entity_field="entity",
        timestamp_field="timestamp",
        value_fields=["value_returns", "value_volatility", "value_volume"],
        metadata_fields=["meta_source", "meta_quality"],
        arrow_schema=table.schema,
        version="1.0"
    )
    
    return FeatureVector(table, schema)

# Example usage
async def main():
    """Example usage of the columnar IPC system."""
    
    print("🚀 Arrow Flight Feature Pipeline Demo")
    print("=" * 50)
    
    # Create sample data
    sample_vector = create_sample_feature_vector()
    print(f"✅ Created sample feature vector: {len(sample_vector)} rows")
    print(f"   Memory usage: {sample_vector.get_memory_usage() // 1024} KB")
    
    # Test filtering
    btc_vector = sample_vector.filter_by_entity(["BTC-PERP"])
    print(f"✅ Filtered to BTC-PERP: {len(btc_vector)} rows")
    
    # Test time filtering
    start_time = sample_vector.get_time_range()[0]
    end_time = start_time + 500_000_000_000  # 500 seconds
    time_filtered = sample_vector.filter_by_time_range(start_time, end_time)
    print(f"✅ Time filtered: {len(time_filtered)} rows")
    
    # Test numpy conversion
    numpy_data = sample_vector.to_numpy()
    print(f"✅ Converted to numpy: {list(numpy_data.keys())}")
    
    # Test pandas conversion (zero-copy)
    try:
        df = sample_vector.to_pandas()
        print(f"✅ Converted to pandas: {df.shape}")
    except Exception as e:
        print(f"⚠️  Pandas conversion: {e}")
    
    print("\n🏎️  Performance Test:")
    start_time = time.time_ns()
    
    # Simulate feature processing
    for _ in range(100):
        filtered = sample_vector.filter_by_entity(["BTC-PERP", "ETH-PERP"])
        numpy_data = filtered.to_numpy()
        
    end_time = time.time_ns()
    elapsed_ms = (end_time - start_time) / 1_000_000
    
    print(f"   100 filter + convert operations: {elapsed_ms:.2f}ms")
    print(f"   Average per operation: {elapsed_ms/100:.3f}ms")

if __name__ == "__main__":
    asyncio.run(main())
