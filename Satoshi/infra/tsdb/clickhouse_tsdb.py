#!/usr/bin/env python3
"""
Time Series Database Infrastructure - ClickHouse
Ultra-fast OLAP database for telemetry, incidents, and real-time analytics.

Key Features:
- Columnar storage optimized for time series
- Blazing fast aggregations and joins
- Real-time incident analysis
- Execution telemetry tracking
- Performance metrics collection
- Horizontal scaling capabilities
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict
from enum import Enum
import json
from datetime import datetime, timezone

# ClickHouse client
try:
    import clickhouse_connect
    CLICKHOUSE_AVAILABLE = True
except ImportError:
    CLICKHOUSE_AVAILABLE = False
    print("⚠️  clickhouse-connect not installed. Install with: pip install clickhouse-connect")

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

class MetricType(Enum):
    """Types of metrics stored in the TSDB."""
    EXECUTION_LATENCY = "execution_latency"
    FILL_RATE = "fill_rate"
    SLIPPAGE = "slippage"
    INCIDENT_COUNT = "incident_count"
    STREAM_FRESHNESS = "stream_freshness"
    MODEL_ACCURACY = "model_accuracy"
    RISK_EXPOSURE = "risk_exposure"
    PNL = "pnl"

@dataclass
class TSDBConfig:
    """Configuration for ClickHouse TSDB."""
    host: str = "localhost"
    port: int = 8123
    database: str = "satoshi_tsdb"
    username: str = "default"
    password: str = ""
    secure: bool = False
    compress: bool = True
    
    # Performance settings
    max_execution_time: int = 60
    max_memory_usage: int = 10 * 1024 * 1024 * 1024  # 10GB
    max_threads: int = 8

class ClickHouseTSDB:
    """
    High-performance time series database for trading telemetry.
    Optimized for real-time analytics and incident investigation.
    """
    
    def __init__(self, config: TSDBConfig):
        """Initialize ClickHouse TSDB."""
        self.config = config
        self.client = None
        
        if CLICKHOUSE_AVAILABLE:
            try:
                self.client = clickhouse_connect.get_client(
                    host=config.host,
                    port=config.port,
                    database=config.database,
                    username=config.username,
                    password=config.password,
                    secure=config.secure,
                    compress=config.compress,
                    settings={
                        'max_execution_time': config.max_execution_time,
                        'max_memory_usage': config.max_memory_usage,
                        'max_threads': config.max_threads
                    }
                )
                logger.info(f"Connected to ClickHouse at {config.host}:{config.port}")
            except Exception as e:
                logger.error(f"Failed to connect to ClickHouse: {e}")
                self.client = None
        
        # Performance metrics
        self.metrics = {
            "queries_executed": 0,
            "rows_inserted": 0,
            "rows_queried": 0,
            "avg_query_time_ms": 0,
            "avg_insert_time_ms": 0
        }
        
        # Initialize schema
        self._initialize_schema()
    
    def _initialize_schema(self) -> None:
        """Initialize the TSDB schema with optimized tables."""
        if not self.client:
            logger.warning("ClickHouse client not available, skipping schema initialization")
            return
        
        try:
            # Create database if not exists
            self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.config.database}")
            
            # Execution telemetry table
            self.client.command("""
                CREATE TABLE IF NOT EXISTS execution_telemetry (
                    timestamp DateTime64(9, 'UTC'),
                    order_id String,
                    venue String,
                    symbol String,
                    side Enum8('buy' = 1, 'sell' = 2),
                    order_type Enum8('market' = 1, 'limit' = 2, 'stop' = 3),
                    send_timestamp_ns UInt64,
                    ack_timestamp_ns UInt64,
                    fill_timestamp_ns UInt64,
                    quantity Float64,
                    fill_price Float64,
                    slippage_bps Float32,
                    queue_time_ms UInt32,
                    execution_venue String,
                    commission Float64,
                    reject_reason String,
                    metadata String
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (timestamp, venue, symbol)
                TTL timestamp + INTERVAL 90 DAY
                SETTINGS index_granularity = 8192
            """)
            
            # Incidents table
            self.client.command("""
                CREATE TABLE IF NOT EXISTS incidents (
                    timestamp DateTime64(9, 'UTC'),
                    incident_id String,
                    incident_class Enum8(
                        'SchemaViolation' = 1,
                        'Freshness' = 2,
                        'Anomaly' = 3,
                        'Leakage' = 4,
                        'RiskBreach' = 5
                    ),
                    severity Enum8('info' = 1, 'warn' = 2, 'crit' = 3),
                    source_agent String,
                    impacted_streams Array(String),
                    proposed_action String,
                    evidence String,
                    resolution_status Enum8('open' = 1, 'investigating' = 2, 'resolved' = 3),
                    resolution_timestamp Nullable(DateTime64(9, 'UTC')),
                    resolution_notes String
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (timestamp, incident_class, severity)
                TTL timestamp + INTERVAL 1 YEAR
                SETTINGS index_granularity = 8192
            """)
            
            # Performance metrics table
            self.client.command("""
                CREATE TABLE IF NOT EXISTS performance_metrics (
                    timestamp DateTime64(9, 'UTC'),
                    metric_type String,
                    entity String,
                    value Float64,
                    unit String,
                    tags Map(String, String),
                    source_agent String
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (timestamp, metric_type, entity)
                TTL timestamp + INTERVAL 30 DAY
                SETTINGS index_granularity = 8192
            """)
            
            # Stream freshness monitoring
            self.client.command("""
                CREATE TABLE IF NOT EXISTS stream_freshness (
                    timestamp DateTime64(9, 'UTC'),
                    stream_name String,
                    last_update_timestamp DateTime64(9, 'UTC'),
                    staleness_ms UInt32,
                    expected_interval_ms UInt32,
                    staleness_ratio Float32,
                    circuit_breaker_state Enum8('closed' = 1, 'open' = 2, 'half_open' = 3),
                    confidence Float32,
                    false_positive_rate Float32
                ) ENGINE = ReplacingMergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (timestamp, stream_name)
                TTL timestamp + INTERVAL 7 DAY
                SETTINGS index_granularity = 8192
            """)
            
            # PnL tracking
            self.client.command("""
                CREATE TABLE IF NOT EXISTS pnl_tracking (
                    timestamp DateTime64(9, 'UTC'),
                    strategy String,
                    venue String,
                    symbol String,
                    position_size Float64,
                    mark_price Float64,
                    unrealized_pnl Float64,
                    realized_pnl Float64,
                    fees Float64,
                    funding_paid Float64,
                    portfolio_value Float64,
                    risk_metrics String
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (timestamp, strategy, symbol)
                TTL timestamp + INTERVAL 2 YEAR
                SETTINGS index_granularity = 8192
            """)
            
            logger.info("TSDB schema initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize TSDB schema: {e}")
    
    def insert_execution_telemetry(self, telemetry_data: List[Dict[str, Any]]) -> bool:
        """Insert execution telemetry data."""
        if not self.client or not telemetry_data:
            return False
        
        try:
            start_time = time.time()
            
            # Prepare data for insertion
            prepared_data = []
            for record in telemetry_data:
                prepared_record = {
                    'timestamp': record.get('timestamp', datetime.now(timezone.utc)),
                    'order_id': record.get('order_id', ''),
                    'venue': record.get('venue', ''),
                    'symbol': record.get('symbol', ''),
                    'side': record.get('side', 'buy'),
                    'order_type': record.get('order_type', 'market'),
                    'send_timestamp_ns': record.get('send_timestamp_ns', 0),
                    'ack_timestamp_ns': record.get('ack_timestamp_ns', 0),
                    'fill_timestamp_ns': record.get('fill_timestamp_ns', 0),
                    'quantity': record.get('quantity', 0.0),
                    'fill_price': record.get('fill_price', 0.0),
                    'slippage_bps': record.get('slippage_bps', 0.0),
                    'queue_time_ms': record.get('queue_time_ms', 0),
                    'execution_venue': record.get('execution_venue', ''),
                    'commission': record.get('commission', 0.0),
                    'reject_reason': record.get('reject_reason', ''),
                    'metadata': json.dumps(record.get('metadata', {}))
                }
                prepared_data.append(prepared_record)
            
            self.client.insert('execution_telemetry', prepared_data)
            
            # Update metrics
            end_time = time.time()
            self.metrics["rows_inserted"] += len(prepared_data)
            insert_time_ms = (end_time - start_time) * 1000
            
            # Update average
            if self.metrics["avg_insert_time_ms"] == 0:
                self.metrics["avg_insert_time_ms"] = insert_time_ms
            else:
                self.metrics["avg_insert_time_ms"] = (
                    0.9 * self.metrics["avg_insert_time_ms"] + 0.1 * insert_time_ms
                )
            
            logger.info(f"Inserted {len(prepared_data)} execution telemetry records in {insert_time_ms:.1f}ms")
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert execution telemetry: {e}")
            return False
    
    def insert_incident(self, incident: Dict[str, Any]) -> bool:
        """Insert an incident record."""
        if not self.client:
            return False
        
        try:
            prepared_incident = {
                'timestamp': incident.get('timestamp', datetime.now(timezone.utc)),
                'incident_id': incident.get('incident_id', ''),
                'incident_class': incident.get('class', 'Anomaly'),
                'severity': incident.get('severity', 'info'),
                'source_agent': incident.get('source_agent', ''),
                'impacted_streams': incident.get('impacted_streams', []),
                'proposed_action': incident.get('proposed_action', ''),
                'evidence': json.dumps(incident.get('evidence_ref', {})),
                'resolution_status': 'open',
                'resolution_timestamp': None,
                'resolution_notes': ''
            }
            
            self.client.insert('incidents', [prepared_incident])
            logger.info(f"Inserted incident: {incident.get('incident_id')}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert incident: {e}")
            return False
    
    def insert_performance_metric(self, metric_type: MetricType, entity: str, 
                                value: float, unit: str = "", 
                                tags: Optional[Dict[str, str]] = None,
                                source_agent: str = "") -> bool:
        """Insert a performance metric."""
        if not self.client:
            return False
        
        try:
            metric_record = {
                'timestamp': datetime.now(timezone.utc),
                'metric_type': metric_type.value,
                'entity': entity,
                'value': value,
                'unit': unit,
                'tags': tags or {},
                'source_agent': source_agent
            }
            
            self.client.insert('performance_metrics', [metric_record])
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert performance metric: {e}")
            return False
    
    def query_execution_metrics(self, start_time: datetime, end_time: datetime,
                              venue: Optional[str] = None,
                              symbol: Optional[str] = None) -> Optional[pd.DataFrame]:
        """Query execution performance metrics."""
        if not self.client:
            return None
        
        try:
            query_start = time.time()
            
            # Base query with parameterized placeholders
            query = """
                SELECT 
                    venue,
                    symbol,
                    COUNT(*) as total_orders,
                    AVG(slippage_bps) as avg_slippage_bps,
                    AVG(queue_time_ms) as avg_queue_time_ms,
                    SUM(quantity * fill_price) as total_volume,
                    SUM(commission) as total_fees,
                    COUNT(CASE WHEN reject_reason != '' THEN 1 END) as rejected_orders
                FROM execution_telemetry
                WHERE timestamp BETWEEN %(start_time)s AND %(end_time)s
            """
            
            # Parameters for safe execution
            params = {
                'start_time': start_time,
                'end_time': end_time
            }
            
            # Add conditional WHERE clauses with parameters
            if venue:
                query += " AND venue = %(venue)s"
                params['venue'] = venue
            if symbol:
                query += " AND symbol = %(symbol)s"
                params['symbol'] = symbol
            
            query += " GROUP BY venue, symbol ORDER BY total_volume DESC"
            
            result = self.client.query_df(query, parameters=params)
            
            # Update metrics
            query_end = time.time()
            self.metrics["queries_executed"] += 1
            self.metrics["rows_queried"] += len(result)
            query_time_ms = (query_end - query_start) * 1000
            
            # Update average
            if self.metrics["avg_query_time_ms"] == 0:
                self.metrics["avg_query_time_ms"] = query_time_ms
            else:
                self.metrics["avg_query_time_ms"] = (
                    0.9 * self.metrics["avg_query_time_ms"] + 0.1 * query_time_ms
                )
            
            logger.info(f"Executed execution metrics query in {query_time_ms:.1f}ms, returned {len(result)} rows")
            return result
            
        except Exception as e:
            logger.error(f"Failed to query execution metrics: {e}")
            return None
    
    def query_incident_summary(self, start_time: datetime, end_time: datetime) -> Optional[pd.DataFrame]:
        """Query incident summary statistics."""
        if not self.client:
            return None
        
        try:
            # Parameterized query to prevent SQL injection
            query = """
                SELECT 
                    incident_class,
                    severity,
                    source_agent,
                    COUNT(*) as incident_count,
                    COUNT(CASE WHEN resolution_status = 'resolved' THEN 1 END) as resolved_count,
                    AVG(resolution_timestamp - timestamp) as avg_resolution_time_sec
                FROM incidents
                WHERE timestamp BETWEEN %(start_time)s AND %(end_time)s
                GROUP BY incident_class, severity, source_agent
                ORDER BY incident_count DESC
            """
            
            params = {
                'start_time': start_time,
                'end_time': end_time
            }
            
            result = self.client.query_df(query, parameters=params)
            logger.info(f"Retrieved incident summary: {len(result)} rows")
            return result
            
        except Exception as e:
            logger.error(f"Failed to query incident summary: {e}")
            return None
    
    def query_real_time_metrics(self, metric_types: List[MetricType],
                              lookback_minutes: int = 60) -> Optional[pd.DataFrame]:
        """Query real-time performance metrics."""
        if not self.client:
            return None
        
        try:
            # Validate and normalize metric types
            if not metric_types:
                logger.warning("No metric types provided for real-time metrics query")
                return None
                
            # Validate metric types against known enum values
            valid_metric_types = []
            for mt in metric_types:
                if isinstance(mt, MetricType):
                    valid_metric_types.append(mt.value)
                elif isinstance(mt, str) and mt in [e.value for e in MetricType]:
                    valid_metric_types.append(mt)
                else:
                    logger.warning(f"Invalid metric type: {mt}")
            
            if not valid_metric_types:
                logger.warning("No valid metric types after validation")
                return None
            
            # Create parameterized query with IN clause
            metric_placeholders = ','.join(['%(metric_%d)s' % i for i in range(len(valid_metric_types))])
            
            query = f"""
                SELECT 
                    metric_type,
                    entity,
                    AVG(value) as avg_value,
                    MAX(value) as max_value,
                    MIN(value) as min_value,
                    COUNT(*) as sample_count
                FROM performance_metrics
                WHERE timestamp >= NOW() - INTERVAL %(lookback_minutes)s MINUTE
                  AND metric_type IN ({metric_placeholders})
                GROUP BY metric_type, entity
                ORDER BY metric_type, avg_value DESC
            """
            
            # Build parameters dictionary
            params = {'lookback_minutes': lookback_minutes}
            for i, metric_type in enumerate(valid_metric_types):
                params[f'metric_{i}'] = metric_type
            
            result = self.client.query_df(query, parameters=params)
            logger.info(f"Retrieved real-time metrics: {len(result)} rows")
            return result
            
        except Exception as e:
            logger.error(f"Failed to query real-time metrics: {e}")
            return None
    
    def get_stream_health_dashboard(self) -> Optional[pd.DataFrame]:
        """Get current stream health status."""
        if not self.client:
            return None
        
        try:
            query = """
                SELECT 
                    stream_name,
                    MAX(timestamp) as last_check,
                    argMax(staleness_ms, timestamp) as current_staleness_ms,
                    argMax(circuit_breaker_state, timestamp) as circuit_breaker_state,
                    argMax(confidence, timestamp) as confidence,
                    argMax(false_positive_rate, timestamp) as false_positive_rate
                FROM stream_freshness
                WHERE timestamp >= NOW() - INTERVAL 1 HOUR
                GROUP BY stream_name
                ORDER BY current_staleness_ms DESC
            """
            
            result = self.client.query_df(query)
            logger.info(f"Retrieved stream health dashboard: {len(result)} streams")
            return result
            
        except Exception as e:
            logger.error(f"Failed to get stream health dashboard: {e}")
            return None
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get TSDB performance metrics."""
        return self.metrics.copy()
    
    def close(self) -> None:
        """Close ClickHouse connection."""
        if self.client:
            self.client.close()
            logger.info("ClickHouse connection closed")

# Example usage
async def main():
    """Example usage of the ClickHouse TSDB."""
    
    print("⚡ ClickHouse TSDB Demo")
    print("=" * 50)
    
    config = TSDBConfig(
        host="localhost",
        port=8123,
        database="satoshi_tsdb_demo",
        username="default",
        password=""
    )
    
    tsdb = ClickHouseTSDB(config)
    
    if not tsdb.client:
        print("❌ ClickHouse not available")
        return
    
    # Insert sample execution telemetry
    sample_telemetry = [
        {
            'order_id': f'order_{i}',
            'venue': 'binance',
            'symbol': 'BTC-PERP',
            'side': 'buy' if i % 2 == 0 else 'sell',
            'order_type': 'market',
            'send_timestamp_ns': int(time.time_ns()) - i * 1000000,
            'ack_timestamp_ns': int(time.time_ns()) - i * 1000000 + 500000,
            'fill_timestamp_ns': int(time.time_ns()) - i * 1000000 + 1000000,
            'quantity': 0.1 * (i + 1),
            'fill_price': 45000 + i * 10,
            'slippage_bps': np.random.uniform(0, 5),
            'queue_time_ms': np.random.randint(1, 100),
            'execution_venue': 'binance',
            'commission': 0.001 * 0.1 * (i + 1) * (45000 + i * 10)
        }
        for i in range(100)
    ]
    
    success = tsdb.insert_execution_telemetry(sample_telemetry)
    print(f"✅ Inserted execution telemetry" if success else "❌ Failed to insert telemetry")
    
    # Insert sample incident
    sample_incident = {
        'incident_id': 'INC_001',
        'class': 'Freshness',
        'severity': 'warn',
        'source_agent': 'freshness_agent',
        'impacted_streams': ['trades.binance.BTC-PERP'],
        'proposed_action': 'CircuitBreak',
        'evidence_ref': {'staleness_ms': 5000, 'threshold_ms': 2000}
    }
    
    success = tsdb.insert_incident(sample_incident)
    print(f"✅ Inserted incident" if success else "❌ Failed to insert incident")
    
    # Insert performance metrics
    success = tsdb.insert_performance_metric(
        MetricType.EXECUTION_LATENCY,
        entity="binance.BTC-PERP",
        value=2.5,
        unit="ms",
        source_agent="order_manager"
    )
    print(f"✅ Inserted performance metric" if success else "❌ Failed to insert metric")
    
    # Query execution metrics
    end_time = datetime.now(timezone.utc)
    start_time = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    
    exec_metrics = tsdb.query_execution_metrics(start_time, end_time)
    if exec_metrics is not None and len(exec_metrics) > 0:
        print(f"✅ Queried execution metrics: {len(exec_metrics)} venues")
        print(f"   Total volume: ${exec_metrics['total_volume'].sum():,.0f}")
        print(f"   Avg slippage: {exec_metrics['avg_slippage_bps'].mean():.2f} bps")
    
    # Get metrics
    metrics = tsdb.get_metrics()
    print(f"\n📊 TSDB Metrics:")
    print(f"   Queries executed: {metrics['queries_executed']}")
    print(f"   Rows inserted: {metrics['rows_inserted']}")
    print(f"   Avg query time: {metrics['avg_query_time_ms']:.1f}ms")
    
    tsdb.close()

if __name__ == "__main__":
    if CLICKHOUSE_AVAILABLE:
        asyncio.run(main())
    else:
        print("❌ ClickHouse not available. Install with: pip install clickhouse-connect")
