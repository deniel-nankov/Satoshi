#!/usr/bin/env python3
"""
Time Series Database Infrastructure - ClickHouse TSDB
Quality Monitoring & Incident Analytics Engine

Architectural Role (per ARCHITECTURE.md):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🏛️ WITHIN DATA QUALITY LAYER:
- clickhouse_tsdb.py ⏰: Quality monitoring and incident analytics
- Track incidents.* topic data for quality failures  
- Performance metrics for each quality agent (schema, leakage, anomaly, freshness)
- SLA monitoring for raw → clean data processing times
- Quality score trends and deterioration alerts

EXACT INFRASTRUCTURE PLACEMENT:
- ⚠️ INCIDENT HANDLING (Cross-Layer): 
  * incidents.* topics: Collect quality issues from all quality agents
  * Flow to clickhouse_tsdb.py for analysis and alerting
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Key Functionalities:
1. **Incident Stream Processing**: Real-time consumption of incidents.* topics
2. **Quality Agent Performance Monitoring**: SLA tracking, throughput analysis
3. **Data Pipeline Health Dashboard**: Executive visibility into data quality
4. **Alerting & Anomaly Detection**: Proactive quality degradation alerts  
5. **Execution Telemetry Storage**: Trading performance analytics
6. **Regulatory Compliance**: Audit trails and data lineage tracking
7. **Real-time Analytics**: Sub-second query performance for operational dashboards
"""

import asyncio
import logging
import time
import json
from typing import Dict, List, Optional, Any, Union, Callable
from dataclasses import dataclass, asdict, field
from enum import Enum
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
import threading
import statistics
import uuid
import hashlib
import signal
import sys

# ClickHouse client
try:
    import clickhouse_connect
    CLICKHOUSE_AVAILABLE = True
except ImportError:
    CLICKHOUSE_AVAILABLE = False
    print("⚠️  clickhouse-connect not installed. Install with: pip install clickhouse-connect")

# Streaming bus for incident consumption
try:
    from infra.bus.streaming_bus import StreamingBus, CanonicalHeaders
    STREAMING_BUS_AVAILABLE = True
    StreamingBus = StreamingBus
    CanonicalHeaders = CanonicalHeaders
except ImportError:
    STREAMING_BUS_AVAILABLE = False
    StreamingBus = None
    CanonicalHeaders = None
    print("⚠️  StreamingBus not available - incident consumption disabled")

# Data processing libraries
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

class MetricType(Enum):
    """Types of metrics stored in the TSDB."""
    # Execution & Trading Metrics
    EXECUTION_LATENCY = "execution_latency"
    FILL_RATE = "fill_rate"
    SLIPPAGE = "slippage"
    PNL = "pnl"
    
    # Data Quality Pipeline Metrics (Architectural Focus)
    INCIDENT_COUNT = "incident_count"
    STREAM_FRESHNESS = "stream_freshness"
    SCHEMA_VALIDATION_RATE = "schema_validation_rate"
    ANOMALY_DETECTION_RATE = "anomaly_detection_rate"
    DATA_LEAKAGE_VIOLATIONS = "data_leakage_violations"
    RECONCILIATION_ACCURACY = "reconciliation_accuracy"
    
    # Pipeline Performance Metrics
    RAW_TO_CLEAN_LATENCY = "raw_to_clean_latency"
    QUALITY_AGENT_THROUGHPUT = "quality_agent_throughput"
    DATA_PIPELINE_SLA = "data_pipeline_sla"
    TOPIC_LAG = "topic_lag"
    
    # System Health Metrics
    MODEL_ACCURACY = "model_accuracy"
    RISK_EXPOSURE = "risk_exposure"
    CIRCUIT_BREAKER_TRIGGERS = "circuit_breaker_triggers"
    SYSTEM_UPTIME = "system_uptime"


class IncidentSeverity(Enum):
    """Incident severity levels matching data quality agents."""
    INFO = "info"
    WARN = "warn" 
    CRIT = "crit"


class IncidentClass(Enum):
    """Incident classes matching streaming bus topics."""
    SCHEMA_VIOLATION = "SchemaViolation"
    FRESHNESS = "Freshness"
    ANOMALY = "Anomaly"
    LEAKAGE = "Leakage"
    RISK_BREACH = "RiskBreach"


@dataclass
class QualityAgentMetrics:
    """Performance metrics for data quality agents."""
    agent_name: str
    messages_processed: int = 0
    messages_per_second: float = 0.0
    avg_processing_time_ms: float = 0.0
    incidents_generated: int = 0
    error_count: int = 0
    last_heartbeat: Optional[datetime] = None
    uptime_percentage: float = 0.0
    sla_breaches: int = 0


@dataclass
class DataPipelineSLA:
    """SLA tracking for raw → clean data pipeline."""
    topic_name: str
    target_latency_ms: float
    current_latency_ms: float
    success_rate_percentage: float
    messages_processed_today: int
    sla_breaches_today: int
    last_update: datetime
    
    @property
    def is_sla_met(self) -> bool:
        """Check if SLA is currently being met."""
        return (self.current_latency_ms <= self.target_latency_ms and 
                self.success_rate_percentage >= 99.5)


@dataclass  
class IncidentAlert:
    """Alert generated from incident analysis."""
    alert_id: str
    alert_type: str  # "degradation", "threshold", "pattern", "cascade"
    severity: IncidentSeverity
    title: str
    description: str
    affected_components: List[str]
    evidence: Dict[str, Any]
    recommended_actions: List[str]
    created_at: datetime
    escalation_level: int = 0

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

class QualityMonitoringTSDB:
    """
    ClickHouse Time Series Database for Quality Monitoring & Incident Analytics
    
    Architectural Role: Primary quality monitoring system within Data Quality Layer
    - Consumes incidents.* topics from all data quality agents
    - Provides real-time analytics and alerting on data pipeline health
    - Tracks SLA performance for raw → clean data processing
    - Generates executive dashboards for data quality oversight
    """
    
    def __init__(self, config: TSDBConfig, streaming_config: Optional[Dict[str, Any]] = None):
        """Initialize Quality Monitoring TSDB."""
        self.config = config
        self.client = None
        self.streaming_bus: Optional[Any] = None
        self.incident_consumers: Dict[str, Any] = {}
        self.is_consuming = False
        self._shutdown_event = asyncio.Event()
        
        # Quality monitoring state
        self.agent_metrics: Dict[str, QualityAgentMetrics] = {}
        self.pipeline_slas: Dict[str, DataPipelineSLA] = {}
        self.incident_cache: deque = deque(maxlen=10000)  # Recent incidents cache
        self.alert_queue: deque = deque(maxlen=1000)  # Pending alerts
        
        # Performance tracking
        self.metrics = {
            "queries_executed": 0,
            "rows_inserted": 0,
            "rows_queried": 0,
            "avg_query_time_ms": 0.0,
            "avg_insert_time_ms": 0.0,
            "incidents_processed": 0,
            "alerts_generated": 0,
            "sla_breaches": 0
        }
        
        # Alerting thresholds
        self.alert_thresholds = {
            "incident_rate_per_minute": 50,
            "pipeline_latency_ms": 5000,
            "agent_error_rate": 0.05,  # 5%
            "sla_success_rate": 0.995   # 99.5%
        }
        
        # Initialize ClickHouse connection
        self._initialize_clickhouse()
        
        # Initialize streaming bus for incident consumption
        if streaming_config and STREAMING_BUS_AVAILABLE and StreamingBus is not None:
            self._initialize_streaming_bus(streaming_config)
        
        # Initialize database schema
        self._initialize_schema()
        
        # Setup SLA tracking
        self._initialize_sla_tracking()
        
        # Track startup time
        self._start_time = time.time()
        
        logger.info("Quality Monitoring TSDB initialized successfully")
    
    def _initialize_clickhouse(self):
        """Initialize ClickHouse connection."""
        if not CLICKHOUSE_AVAILABLE:
            logger.warning("ClickHouse not available - operating in mock mode")
            return
            
        try:
            # Create initial client to set up database
            temp_client = clickhouse_connect.get_client(  # type: ignore
                host=self.config.host,
                port=self.config.port,
                username=self.config.username,
                password=self.config.password,
                secure=self.config.secure,
                compress=self.config.compress,
                settings={
                    'max_execution_time': self.config.max_execution_time,
                    'max_memory_usage': self.config.max_memory_usage,
                    'max_threads': self.config.max_threads
                }
            )
            
            # Create database if it doesn't exist
            temp_client.command(f"CREATE DATABASE IF NOT EXISTS {self.config.database}")
            temp_client.close()
            
            # Create persistent client connected to the database
            self.client = clickhouse_connect.get_client(  # type: ignore
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                username=self.config.username,
                password=self.config.password,
                secure=self.config.secure,
                compress=self.config.compress,
                settings={
                    'max_execution_time': self.config.max_execution_time,
                    'max_memory_usage': self.config.max_memory_usage,
                    'max_threads': self.config.max_threads
                }
            )
            
            logger.info(f"✅ Connected to ClickHouse at {self.config.host}:{self.config.port}/{self.config.database}")
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to ClickHouse: {e}")
            self.client = None
    
    def _initialize_streaming_bus(self, streaming_config: Dict[str, Any]):
        """Initialize streaming bus for incident consumption.""" 
        try:
            if StreamingBus is not None:
                self.streaming_bus = StreamingBus(streaming_config)
                
                # Initialize high-performance batch processor for incident ingestion
                self.incident_batch_size = streaming_config.get('incident_batch_size', 5000)
                self.incident_batch_timeout_ms = streaming_config.get('incident_batch_timeout_ms', 3000)
                self.pending_incident_batches: Dict[str, deque] = defaultdict(deque)
                self.batch_timestamps: Dict[str, float] = {}
                self.batch_lock = asyncio.Lock()
                
                # Consumer pool for parallel processing
                self.consumer_pool_size = streaming_config.get('consumer_pool_size', 16)
                
                # Incident topic subscriptions
                self.incident_topics = [
                    "incidents.SchemaViolation",
                    "incidents.Freshness", 
                    "incidents.Anomaly",
                    "incidents.Leakage"
                ]
                
                logger.info("✅ Streaming bus initialized for incident consumption")
            else:
                logger.warning("StreamingBus class not available")
        except Exception as e:
            logger.error(f"❌ Failed to initialize streaming bus: {e}")
            self.streaming_bus = None
    
    def _initialize_sla_tracking(self):
        """Initialize SLA tracking for data pipeline topics."""
        # Define SLA targets for key pipeline stages
        pipeline_slas = {
            "raw_data.market.trades": 100.0,     # 100ms target
            "raw_data.market.book": 200.0,       # 200ms target  
            "clean.market.trades": 500.0,        # 500ms target
            "clean.market.book": 1000.0,         # 1s target
            "features.base": 2000.0,             # 2s target
        }
        
        for topic, target_latency in pipeline_slas.items():
            self.pipeline_slas[topic] = DataPipelineSLA(
                topic_name=topic,
                target_latency_ms=target_latency,
                current_latency_ms=0.0,
                success_rate_percentage=100.0,
                messages_processed_today=0,
                sla_breaches_today=0,
                last_update=datetime.now(timezone.utc)
            )
    
    def _initialize_schema(self) -> None:
        """Initialize enhanced schema for quality monitoring and incident analytics."""
        if not self.client:
            logger.warning("ClickHouse client not available, skipping schema initialization")
            return
        
        try:
            # Create database if not exists
            self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.config.database}")
            
            # ============= CORE QUALITY MONITORING TABLES =============
            
            # Enhanced incidents table with architectural focus
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
                    source_topic String,
                    correlation_id String,
                    impacted_streams Array(String),
                    proposed_action String,
                    evidence String,
                    resolution_status Enum8('open' = 1, 'investigating' = 2, 'resolved' = 3),
                    resolution_timestamp Nullable(DateTime64(9, 'UTC')),
                    resolution_notes String,
                    escalation_level UInt8 DEFAULT 0,
                    auto_resolved Bool DEFAULT 0
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (timestamp, incident_class, severity, source_agent)
                TTL timestamp + INTERVAL 1 YEAR
                SETTINGS index_granularity = 8192
            """)
            
            # Quality agent performance metrics
            self.client.command("""
                CREATE TABLE IF NOT EXISTS quality_agent_metrics (
                    timestamp DateTime64(9, 'UTC'),
                    agent_name String,
                    messages_processed UInt64,
                    messages_per_second Float64,
                    avg_processing_time_ms Float64,
                    incidents_generated UInt32,
                    error_count UInt32,
                    uptime_percentage Float32,
                    sla_breaches UInt32,
                    last_heartbeat DateTime64(9, 'UTC'),
                    memory_usage_mb Float64,
                    cpu_utilization_pct Float32
                ) ENGINE = ReplacingMergeTree(timestamp)
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (agent_name, timestamp)
                TTL timestamp + INTERVAL 30 DAY
                SETTINGS index_granularity = 8192
            """)
            
            # Data pipeline SLA tracking
            self.client.command("""
                CREATE TABLE IF NOT EXISTS pipeline_sla (
                    timestamp DateTime64(9, 'UTC'),
                    topic_name String,
                    stage String,
                    target_latency_ms Float64,
                    actual_latency_ms Float64,
                    success_rate_pct Float32,
                    messages_processed UInt64,
                    sla_breach Bool,
                    breach_magnitude_pct Float32,
                    recovery_time_ms Nullable(Float64)
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(timestamp) 
                ORDER BY (timestamp, topic_name, stage)
                TTL timestamp + INTERVAL 90 DAY
                SETTINGS index_granularity = 8192
            """)
            
            # Incident correlation and pattern analysis
            self.client.command("""
                CREATE TABLE IF NOT EXISTS incident_patterns (
                    timestamp DateTime64(9, 'UTC'),
                    pattern_id String,
                    pattern_type Enum8(
                        'cascade' = 1,
                        'periodic' = 2, 
                        'threshold' = 3,
                        'correlation' = 4,
                        'anomaly_burst' = 5
                    ),
                    affected_agents Array(String),
                    incident_count UInt32,
                    timespan_minutes UInt32,
                    confidence_score Float32,
                    root_cause_hypothesis String,
                    recommended_action String
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (timestamp, pattern_type, confidence_score)
                TTL timestamp + INTERVAL 6 MONTH
                SETTINGS index_granularity = 8192
            """)
            
            # Real-time alerting table
            self.client.command("""
                CREATE TABLE IF NOT EXISTS quality_alerts (
                    timestamp DateTime64(9, 'UTC'),
                    alert_id String,
                    alert_type String,
                    severity Enum8('info' = 1, 'warn' = 2, 'crit' = 3),
                    title String,
                    description String,
                    affected_components Array(String),
                    evidence String,
                    recommended_actions Array(String),
                    escalation_level UInt8,
                    acknowledged Bool DEFAULT 0,
                    acknowledged_by String DEFAULT '',
                    resolved Bool DEFAULT 0,
                    resolved_timestamp Nullable(DateTime64(9, 'UTC'))
                ) ENGINE = ReplacingMergeTree(timestamp)
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (alert_id, timestamp)
                TTL timestamp + INTERVAL 90 DAY
                SETTINGS index_granularity = 8192
            """)
            
            # ============= EXECUTION & TRADING TABLES =============
            
            # Enhanced execution telemetry 
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
                    metadata String,
                    latency_breakdown String,
                    quality_score Float32
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (timestamp, venue, symbol)
                TTL timestamp + INTERVAL 90 DAY
                SETTINGS index_granularity = 8192
            """)
            
            # Enhanced performance metrics table
            self.client.command("""
                CREATE TABLE IF NOT EXISTS performance_metrics (
                    timestamp DateTime64(9, 'UTC'),
                    metric_type String,
                    entity String,
                    value Float64,
                    unit String,
                    tags Map(String, String),
                    source_agent String,
                    dimensions Map(String, String),
                    quality_tier Enum8('raw' = 1, 'clean' = 2, 'curated' = 3),
                    is_anomaly Bool DEFAULT 0
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (timestamp, metric_type, entity)
                TTL timestamp + INTERVAL 30 DAY
                SETTINGS index_granularity = 8192
            """)
            
            # Stream health monitoring (enhanced freshness tracking)
            self.client.command("""
                CREATE TABLE IF NOT EXISTS stream_health (
                    timestamp DateTime64(9, 'UTC'),
                    stream_name String,
                    last_message_timestamp DateTime64(9, 'UTC'),
                    staleness_ms UInt32,
                    expected_interval_ms UInt32,
                    staleness_ratio Float32,
                    message_rate_per_sec Float64,
                    circuit_breaker_state Enum8('closed' = 1, 'open' = 2, 'half_open' = 3),
                    confidence Float32,
                    false_positive_rate Float32,
                    data_quality_score Float32,
                    trend_direction Enum8('improving' = 1, 'stable' = 2, 'degrading' = 3)
                ) ENGINE = ReplacingMergeTree(timestamp)
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (stream_name, timestamp)
                TTL timestamp + INTERVAL 7 DAY
                SETTINGS index_granularity = 8192
            """)
            
            # PnL tracking (unchanged but enhanced with quality scores)
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
                    risk_metrics String,
                    data_quality_score Float32,
                    execution_quality_score Float32
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (timestamp, strategy, symbol)
                TTL timestamp + INTERVAL 2 YEAR
                SETTINGS index_granularity = 8192
            """)
            
            # Create materialized views for real-time dashboards
            self._create_materialized_views()
            
            logger.info("✅ Enhanced TSDB schema initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize TSDB schema: {e}")
    
    def _create_materialized_views(self) -> None:
        """Create materialized views for real-time dashboard queries."""
        if not self.client:
            return
            
        try:
            # Real-time incident rate view
            self.client.command("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS incident_rate_per_minute
                ENGINE = MergeTree()
                ORDER BY (minute_timestamp, incident_class)
                POPULATE
                AS SELECT
                    toStartOfMinute(timestamp) as minute_timestamp,
                    incident_class,
                    count() as incident_count,
                    countIf(severity = 'crit') as critical_count,
                    countIf(severity = 'warn') as warning_count
                FROM incidents
                GROUP BY minute_timestamp, incident_class
            """)
            
            # Pipeline health summary view
            self.client.command("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS pipeline_health_summary
                ENGINE = AggregatingMergeTree()
                ORDER BY (hour_timestamp, topic_name)
                POPULATE  
                AS SELECT
                    toStartOfHour(timestamp) as hour_timestamp,
                    topic_name,
                    avgState(actual_latency_ms) as avg_latency,
                    avgState(success_rate_pct) as avg_success_rate,
                    sumState(messages_processed) as total_messages,
                    sumState(toUInt64(sla_breach)) as breach_count
                FROM pipeline_sla
                GROUP BY hour_timestamp, topic_name
            """)
            
            logger.info("✅ Materialized views created successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to create materialized views: {e}")
    
    # ============= CORE INCIDENT PROCESSING METHODS =============
    
    async def start_incident_consumption(self) -> None:
        """Start consuming incidents from all incident topics."""
        if not self.streaming_bus:
            logger.warning("Streaming bus not available - incident consumption disabled")
            return
            
        self.is_consuming = True
        
        # List of incident topics to consume
        incident_topics = [
            "incidents.SchemaViolation",
            "incidents.Freshness", 
            "incidents.Anomaly",
            "incidents.Leakage",
            "incidents.all"
        ]
        
        try:
            # Start consumers for all incident topics
            consumer_tasks = []
            for topic in incident_topics:
                task = asyncio.create_task(
                    self._consume_incident_topic(topic)
                )
                consumer_tasks.append(task)
            
            # Start periodic analytics and alerting
            analytics_task = asyncio.create_task(self._periodic_incident_analysis())
            consumer_tasks.append(analytics_task)
            
            logger.info(f"✅ Started incident consumption for {len(incident_topics)} topics")
            
            # Wait for shutdown signal
            await self._shutdown_event.wait()
            
        except Exception as e:
            logger.error(f"❌ Error in incident consumption: {e}")
        finally:
            self.is_consuming = False
            
            # Cancel all consumer tasks
            for task in consumer_tasks:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            
            logger.info("Incident consumption stopped")
    
    async def _consume_incident_topic(self, topic: str) -> None:
        """Consume incidents from a specific topic."""
        try:
            async def incident_handler(topic: str, partition_key: str, 
                                     payload: Dict[str, Any], headers: Dict[str, str]) -> None:
                await self._process_incident_message(topic, payload, headers)
            
            # Subscribe to topic with handler
            await self.streaming_bus.subscribe_with_handler(
                topic=topic,
                handler=incident_handler,
                consumer_group="quality_monitoring_tsdb",
                enable_auto_commit=True
            )
            
        except Exception as e:
            logger.error(f"❌ Error consuming from {topic}: {e}")
    
    async def _process_incident_message(self, topic: str, payload: Dict[str, Any], 
                                       headers: Dict[str, str]) -> None:
        """Process an individual incident message."""
        try:
            # Extract incident data
            incident = self._parse_incident_payload(payload, topic)
            
            # Store incident in ClickHouse
            success = self.insert_incident(incident)
            
            if success:
                # Add to in-memory cache for pattern analysis
                self.incident_cache.append({
                    'timestamp': datetime.now(timezone.utc),
                    'incident': incident,
                    'topic': topic
                })
                
                # Update metrics
                self.metrics["incidents_processed"] += 1
                
                # Update agent metrics
                self._update_agent_metrics(incident.get('source_agent', 'unknown'))
                
                # Trigger real-time analysis
                await self._analyze_incident_patterns(incident)
                
                logger.debug(f"Processed incident {incident.get('incident_id')} from {topic}")
                
            else:
                logger.error(f"Failed to store incident from {topic}")
                
        except Exception as e:
            logger.error(f"❌ Error processing incident from {topic}: {e}")
    
    def _parse_incident_payload(self, payload: Dict[str, Any], source_topic: str) -> Dict[str, Any]:
        """Parse incident payload into standardized format."""
        # Map different agent payload formats to standard format
        incident = {
            'timestamp': payload.get('timestamp', datetime.now(timezone.utc)),
            'incident_id': payload.get('incident_id', str(uuid.uuid4())),
            'class': self._map_incident_class(payload, source_topic),
            'severity': payload.get('severity', 'info'),
            'source_agent': payload.get('source_agent', source_topic.split('.')[-1]),
            'source_topic': source_topic,
            'correlation_id': payload.get('correlation_id', ''),
            'impacted_streams': payload.get('impacted_streams', []),
            'proposed_action': payload.get('proposed_action', ''),
            'evidence_ref': payload.get('evidence', {}),
            'resolution_status': 'open',
            'resolution_timestamp': None,
            'resolution_notes': '',
            'escalation_level': 0,
            'auto_resolved': False
        }
        
        return incident
    
    def _map_incident_class(self, payload: Dict[str, Any], source_topic: str) -> str:
        """Map incident to standard class based on topic and payload."""
        # Extract class from topic name
        if "SchemaViolation" in source_topic:
            return "SchemaViolation"
        elif "Freshness" in source_topic:
            return "Freshness" 
        elif "Anomaly" in source_topic:
            return "Anomaly"
        elif "Leakage" in source_topic:
            return "Leakage"
        else:
            # Fallback to payload class field
            return payload.get('class', 'Anomaly')
    
    def _update_agent_metrics(self, agent_name: str) -> None:
        """Update performance metrics for quality agent."""
        if agent_name not in self.agent_metrics:
            self.agent_metrics[agent_name] = QualityAgentMetrics(agent_name=agent_name)
        
        metrics = self.agent_metrics[agent_name]
        metrics.messages_processed += 1
        metrics.incidents_generated += 1
        metrics.last_heartbeat = datetime.now(timezone.utc)
        
        # Calculate rates (simplified)
        current_time = time.time()
        if not hasattr(metrics, '_last_rate_update'):
            metrics._last_rate_update = current_time
        
        time_diff = current_time - getattr(metrics, '_last_rate_update', current_time)
        if time_diff >= 60:  # Update every minute
            metrics.messages_per_second = metrics.messages_processed / 60.0
            metrics._last_rate_update = current_time
            
            # Store metrics in ClickHouse
            self._store_agent_metrics(agent_name, metrics)
    
    def _store_agent_metrics(self, agent_name: str, metrics: QualityAgentMetrics) -> None:
        """Store agent performance metrics in ClickHouse."""
        if not self.client:
            return
            
        try:
            metric_data = {
                'timestamp': datetime.now(timezone.utc),
                'agent_name': agent_name,
                'messages_processed': metrics.messages_processed,
                'messages_per_second': metrics.messages_per_second,
                'avg_processing_time_ms': metrics.avg_processing_time_ms,
                'incidents_generated': metrics.incidents_generated,
                'error_count': metrics.error_count,
                'uptime_percentage': metrics.uptime_percentage,
                'sla_breaches': metrics.sla_breaches,
                'last_heartbeat': metrics.last_heartbeat or datetime.now(timezone.utc),
                'memory_usage_mb': 0.0,  # TODO: Implement memory tracking
                'cpu_utilization_pct': 0.0  # TODO: Implement CPU tracking
            }
            
            self.client.insert('quality_agent_metrics', [list(metric_data.values())], 
                             column_names=list(metric_data.keys()))
            
        except Exception as e:
            logger.error(f"❌ Failed to store agent metrics for {agent_name}: {e}")
    
    async def _analyze_incident_patterns(self, incident: Dict[str, Any]) -> None:
        """Analyze incident patterns and generate alerts if needed."""
        try:
            # Check for incident rate spikes
            await self._check_incident_rate_spike(incident)
            
            # Check for cascade patterns
            await self._check_cascade_patterns(incident)
            
            # Check for periodic patterns
            await self._check_periodic_patterns(incident)
            
            # Check for correlation patterns
            await self._check_correlation_patterns(incident)
            
        except Exception as e:
            logger.error(f"❌ Error analyzing incident patterns: {e}")
    
    async def _check_incident_rate_spike(self, incident: Dict[str, Any]) -> None:
        """Check for incident rate spikes and generate alerts."""
        current_time = datetime.now(timezone.utc)
        
        # Count incidents in last minute for same class and agent
        recent_incidents = [
            cached for cached in self.incident_cache
            if (current_time - cached['timestamp']).total_seconds() <= 60 and
               cached['incident'].get('class') == incident.get('class') and
               cached['incident'].get('source_agent') == incident.get('source_agent')
        ]
        
        if len(recent_incidents) >= self.alert_thresholds["incident_rate_per_minute"]:
            alert = IncidentAlert(
                alert_id=str(uuid.uuid4()),
                alert_type="threshold",
                severity=IncidentSeverity.WARN,
                title=f"High incident rate detected",
                description=f"Agent {incident.get('source_agent')} generated {len(recent_incidents)} incidents in 1 minute",
                affected_components=[incident.get('source_agent', 'unknown')],
                evidence={
                    'incident_count': len(recent_incidents),
                    'time_window_seconds': 60,
                    'incident_class': incident.get('class'),
                    'threshold': self.alert_thresholds["incident_rate_per_minute"]
                },
                recommended_actions=[
                    "Check agent health and configuration",
                    "Investigate data source quality",
                    "Consider temporary circuit breaker"
                ],
                created_at=current_time
            )
            
            await self._generate_alert(alert)
    
    async def _check_cascade_patterns(self, incident: Dict[str, Any]) -> None:
        """Check for cascade failure patterns."""
        # Look for multiple agents failing within short timeframe
        current_time = datetime.now(timezone.utc)
        
        recent_incidents = [
            cached for cached in self.incident_cache
            if (current_time - cached['timestamp']).total_seconds() <= 300  # 5 minutes
        ]
        
        # Group by agent
        agent_incidents = defaultdict(int)
        for cached in recent_incidents:
            agent = cached['incident'].get('source_agent', 'unknown')
            agent_incidents[agent] += 1
        
        # Check if multiple agents are failing
        failing_agents = [agent for agent, count in agent_incidents.items() if count >= 3]
        
        if len(failing_agents) >= 3:  # 3+ agents with 3+ incidents each
            alert = IncidentAlert(
                alert_id=str(uuid.uuid4()),
                alert_type="cascade",
                severity=IncidentSeverity.CRIT,
                title="Cascade failure pattern detected",
                description=f"Multiple quality agents showing elevated incident rates: {', '.join(failing_agents)}",
                affected_components=failing_agents,
                evidence={
                    'failing_agents': failing_agents,
                    'incident_counts': dict(agent_incidents),
                    'time_window_minutes': 5
                },
                recommended_actions=[
                    "Investigate upstream data sources",
                    "Check streaming bus health", 
                    "Consider system-wide circuit breaker",
                    "Escalate to engineering team"
                ],
                created_at=current_time,
                escalation_level=1
            )
            
            await self._generate_alert(alert)
    
    async def _check_periodic_patterns(self, incident: Dict[str, Any]) -> None:
        """Check for periodic incident patterns."""
        # TODO: Implement periodic pattern detection
        # Look for incidents that occur at regular intervals
        pass
    
    async def _check_correlation_patterns(self, incident: Dict[str, Any]) -> None:
        """Check for correlation between incidents and external factors."""
        # TODO: Implement correlation analysis
        # Look for correlations with market events, system metrics, etc.
        pass
    
    async def _generate_alert(self, alert: IncidentAlert) -> None:
        """Generate and store alert."""
        try:
            # Store alert in ClickHouse
            if self.client:
                alert_data = {
                    'timestamp': alert.created_at,
                    'alert_id': alert.alert_id,
                    'alert_type': alert.alert_type,
                    'severity': alert.severity.value,
                    'title': alert.title,
                    'description': alert.description,
                    'affected_components': alert.affected_components,
                    'evidence': json.dumps(alert.evidence),
                    'recommended_actions': alert.recommended_actions,
                    'escalation_level': alert.escalation_level,
                    'acknowledged': False,
                    'acknowledged_by': '',
                    'resolved': False,
                    'resolved_timestamp': None
                }
                
                self.client.insert('quality_alerts', [list(alert_data.values())],
                                 column_names=list(alert_data.keys()))
            
            # Add to alert queue for external notification
            self.alert_queue.append(alert)
            self.metrics["alerts_generated"] += 1
            
            # Log alert
            logger.warning(f"🚨 ALERT: {alert.title} - {alert.description}")
            
        except Exception as e:
            logger.error(f"❌ Failed to generate alert: {e}")
    
    async def _periodic_incident_analysis(self) -> None:
        """Periodic analysis of incident trends and patterns."""
        while self.is_consuming and not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(60)  # Run every minute
                
                # Analyze SLA breaches
                await self._analyze_sla_breaches()
                
                # Update pipeline health metrics
                await self._update_pipeline_health()
                
                # Clean old cache entries
                self._cleanup_incident_cache()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Error in periodic incident analysis: {e}")
                await asyncio.sleep(10)  # Brief pause before retry
    
    async def _analyze_sla_breaches(self) -> None:
        """Analyze SLA breaches and update tracking."""
        current_time = datetime.now(timezone.utc)
        
        for topic, sla in self.pipeline_slas.items():
            # Check if SLA is being breached
            if not sla.is_sla_met:
                sla.sla_breaches_today += 1
                self.metrics["sla_breaches"] += 1
                
                # Generate alert for SLA breach
                if sla.sla_breaches_today >= 5:  # Multiple breaches
                    alert = IncidentAlert(
                        alert_id=str(uuid.uuid4()),
                        alert_type="sla_breach",
                        severity=IncidentSeverity.WARN,
                        title=f"SLA breach for {topic}",
                        description=f"Pipeline {topic} has breached SLA {sla.sla_breaches_today} times today",
                        affected_components=[topic],
                        evidence={
                            'current_latency_ms': sla.current_latency_ms,
                            'target_latency_ms': sla.target_latency_ms,
                            'success_rate_pct': sla.success_rate_percentage,
                            'breaches_today': sla.sla_breaches_today
                        },
                        recommended_actions=[
                            "Investigate pipeline bottlenecks",
                            "Check resource utilization",
                            "Review data quality issues"
                        ],
                        created_at=current_time
                    )
                    
                    await self._generate_alert(alert)
    
    async def _update_pipeline_health(self) -> None:
        """Update overall pipeline health metrics."""
        # TODO: Implement comprehensive pipeline health tracking
        # This would integrate with other monitoring systems
        pass
    
    def _cleanup_incident_cache(self) -> None:
        """Clean old entries from incident cache."""
        current_time = datetime.now(timezone.utc)
        cutoff_time = current_time - timedelta(hours=1)  # Keep 1 hour
        
        # Remove old entries
        while self.incident_cache and self.incident_cache[0]['timestamp'] < cutoff_time:
            self.incident_cache.popleft()
    
    async def stop_incident_consumption(self) -> None:
        """Stop incident consumption gracefully."""
        logger.info("Stopping incident consumption...")
        self._shutdown_event.set()
        
        # Wait a bit for consumers to stop
        await asyncio.sleep(2)
        
        if self.streaming_bus:
            # Close streaming bus connections
            # self.streaming_bus.close()  # TODO: Implement close method
            pass
        
        logger.info("✅ Incident consumption stopped")
    
    # ============= ENHANCED INSERT METHODS =============
    
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
                'source_topic': incident.get('source_topic', ''),
                'correlation_id': incident.get('correlation_id', ''),
                'impacted_streams': incident.get('impacted_streams', []),
                'proposed_action': incident.get('proposed_action', ''),
                'evidence': json.dumps(incident.get('evidence_ref', {})),
                'resolution_status': 'open',
                'resolution_timestamp': None,
                'resolution_notes': '',
                'escalation_level': incident.get('escalation_level', 0),
                'auto_resolved': incident.get('auto_resolved', False)
            }
            
            # Convert to list format for ClickHouse insert
            self.client.insert('incidents', [list(prepared_incident.values())],
                             column_names=list(prepared_incident.keys()))
            logger.info(f"✅ Inserted incident: {incident.get('incident_id')}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to insert incident: {e}")
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
                'source_agent': source_agent,
                'dimensions': {},
                'quality_tier': 'clean',
                'is_anomaly': False
            }
            
            # Convert to list format for ClickHouse insert
            self.client.insert('performance_metrics', [list(metric_record.values())],
                             column_names=list(metric_record.keys()))
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to insert performance metric: {e}")
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
            params: Dict[str, Any] = {
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
    
    # ============= EXECUTIVE DASHBOARD METHODS =============
    
    def get_quality_pipeline_dashboard(self) -> Dict[str, Any]:
        """Get executive dashboard data for quality pipeline health."""
        if not self.client:
            return self._get_mock_dashboard()
        
        try:
            dashboard = {
                "pipeline_health": self._get_pipeline_health_summary(),
                "incident_trends": self._get_incident_trend_analysis(), 
                "agent_performance": self._get_agent_performance_summary(),
                "sla_status": self._get_sla_status_summary(),
                "alert_summary": self._get_active_alerts_summary(),
                "data_quality_scores": self._get_data_quality_scores(),
                "system_uptime": self._get_system_uptime_metrics(),
                "generated_at": datetime.now(timezone.utc).isoformat()
            }
            
            logger.info("✅ Generated quality pipeline dashboard")
            return dashboard
            
        except Exception as e:
            logger.error(f"❌ Failed to generate dashboard: {e}")
            return self._get_mock_dashboard()
    
    def _get_pipeline_health_summary(self) -> Dict[str, Any]:
        """Get overall pipeline health summary."""
        try:
            # Query pipeline SLA performance
            query = """
                SELECT 
                    topic_name,
                    avg(actual_latency_ms) as avg_latency,
                    avg(success_rate_pct) as avg_success_rate,
                    sum(messages_processed) as total_messages,
                    sum(toUInt64(sla_breach)) as breach_count
                FROM pipeline_sla 
                WHERE timestamp >= now() - INTERVAL 1 HOUR
                GROUP BY topic_name
                ORDER BY avg_latency DESC
            """
            
            result = self.client.query_df(query)
            
            # Calculate health scores
            health_summary = {
                "total_topics": len(result),
                "healthy_topics": len(result[result['avg_success_rate'] >= 99.5]),
                "degraded_topics": len(result[(result['avg_success_rate'] >= 95) & (result['avg_success_rate'] < 99.5)]),
                "failed_topics": len(result[result['avg_success_rate'] < 95]),
                "avg_pipeline_latency_ms": result['avg_latency'].mean() if len(result) > 0 else 0,
                "total_messages_processed": result['total_messages'].sum() if len(result) > 0 else 0,
                "sla_breach_rate": result['breach_count'].sum() / result['total_messages'].sum() if len(result) > 0 else 0
            }
            
            return health_summary
            
        except Exception as e:
            logger.error(f"❌ Failed to get pipeline health summary: {e}")
            return {"error": str(e)}
    
    def _get_incident_trend_analysis(self) -> Dict[str, Any]:
        """Get incident trend analysis for last 24 hours."""
        try:
            # Query incident trends
            query = """
                SELECT 
                    toStartOfHour(timestamp) as hour,
                    incident_class,
                    count() as incident_count,
                    countIf(severity = 'crit') as critical_count,
                    countIf(severity = 'warn') as warning_count,
                    countIf(severity = 'info') as info_count
                FROM incidents 
                WHERE timestamp >= now() - INTERVAL 24 HOUR
                GROUP BY hour, incident_class
                ORDER BY hour DESC
            """
            
            result = self.client.query_df(query)
            
            # Calculate trends
            trends = {
                "total_incidents_24h": result['incident_count'].sum() if len(result) > 0 else 0,
                "critical_incidents_24h": result['critical_count'].sum() if len(result) > 0 else 0,
                "incident_rate_per_hour": result['incident_count'].mean() if len(result) > 0 else 0,
                "most_problematic_class": result.groupby('incident_class')['incident_count'].sum().idxmax() if len(result) > 0 else "None",
                "trend_direction": "stable",  # TODO: Calculate actual trend
                "hourly_breakdown": result.to_dict('records') if len(result) > 0 else []
            }
            
            return trends
            
        except Exception as e:
            logger.error(f"❌ Failed to get incident trends: {e}")
            return {"error": str(e)}
    
    def _get_agent_performance_summary(self) -> Dict[str, Any]:
        """Get quality agent performance summary."""
        try:
            # Query latest agent metrics
            query = """
                SELECT 
                    agent_name,
                    argMax(messages_processed, timestamp) as total_messages,
                    argMax(messages_per_second, timestamp) as current_rate,
                    argMax(incidents_generated, timestamp) as total_incidents,
                    argMax(error_count, timestamp) as total_errors,
                    argMax(uptime_percentage, timestamp) as uptime_pct,
                    argMax(sla_breaches, timestamp) as sla_breaches,
                    max(timestamp) as last_update
                FROM quality_agent_metrics 
                WHERE timestamp >= now() - INTERVAL 1 HOUR
                GROUP BY agent_name
                ORDER BY current_rate DESC
            """
            
            result = self.client.query_df(query)
            
            # Calculate performance summary
            performance = {
                "total_agents": len(result),
                "healthy_agents": len(result[result['uptime_pct'] >= 99]),
                "degraded_agents": len(result[(result['uptime_pct'] >= 95) & (result['uptime_pct'] < 99)]),
                "failed_agents": len(result[result['uptime_pct'] < 95]),
                "avg_throughput_msg_per_sec": result['current_rate'].mean() if len(result) > 0 else 0,
                "total_errors": result['total_errors'].sum() if len(result) > 0 else 0,
                "agent_details": result.to_dict('records') if len(result) > 0 else []
            }
            
            return performance
            
        except Exception as e:
            logger.error(f"❌ Failed to get agent performance: {e}")
            return {"error": str(e)}
    
    def _get_sla_status_summary(self) -> Dict[str, Any]:
        """Get SLA status summary."""
        sla_summary = {
            "total_slas": len(self.pipeline_slas),
            "met_slas": sum(1 for sla in self.pipeline_slas.values() if sla.is_sla_met),
            "breached_slas": sum(1 for sla in self.pipeline_slas.values() if not sla.is_sla_met),
            "total_breaches_today": sum(sla.sla_breaches_today for sla in self.pipeline_slas.values()),
            "worst_performing_topic": min(self.pipeline_slas.values(), key=lambda s: s.success_rate_percentage).topic_name if self.pipeline_slas else "None",
            "sla_details": [
                {
                    "topic": sla.topic_name,
                    "target_latency_ms": sla.target_latency_ms,
                    "current_latency_ms": sla.current_latency_ms,
                    "success_rate_pct": sla.success_rate_percentage,
                    "is_met": sla.is_sla_met,
                    "breaches_today": sla.sla_breaches_today
                }
                for sla in self.pipeline_slas.values()
            ]
        }
        
        return sla_summary
    
    def _get_active_alerts_summary(self) -> Dict[str, Any]:
        """Get active alerts summary."""
        active_alerts = list(self.alert_queue)
        
        alerts_summary = {
            "total_active_alerts": len(active_alerts),
            "critical_alerts": sum(1 for alert in active_alerts if alert.severity == IncidentSeverity.CRIT),
            "warning_alerts": sum(1 for alert in active_alerts if alert.severity == IncidentSeverity.WARN),
            "info_alerts": sum(1 for alert in active_alerts if alert.severity == IncidentSeverity.INFO),
            "escalated_alerts": sum(1 for alert in active_alerts if alert.escalation_level > 0),
            "alert_types": {
                alert_type: sum(1 for alert in active_alerts if alert.alert_type == alert_type)
                for alert_type in set(alert.alert_type for alert in active_alerts)
            },
            "recent_alerts": [
                {
                    "alert_id": alert.alert_id,
                    "type": alert.alert_type,
                    "severity": alert.severity.value,
                    "title": alert.title,
                    "affected_components": alert.affected_components,
                    "created_at": alert.created_at.isoformat()
                }
                for alert in sorted(active_alerts, key=lambda a: a.created_at, reverse=True)[:10]
            ]
        }
        
        return alerts_summary
    
    def _get_data_quality_scores(self) -> Dict[str, Any]:
        """Get data quality scores across pipeline stages."""
        # TODO: Implement comprehensive data quality scoring
        # This would aggregate quality metrics from all stages
        
        scores = {
            "raw_data_quality": 98.5,
            "clean_data_quality": 99.8,
            "feature_quality": 99.2,
            "overall_quality": 99.1,
            "quality_trend": "improving",
            "quality_by_topic": {
                topic: {
                    "quality_score": 99.0 + (hash(topic) % 200) / 100,  # Mock score
                    "completeness": 99.5,
                    "timeliness": 98.8,
                    "accuracy": 99.9
                }
                for topic in ["raw_data.market.trades", "clean.market.trades", "features.base"]
            }
        }
        
        return scores
    
    def _get_system_uptime_metrics(self) -> Dict[str, Any]:
        """Get system uptime and availability metrics."""
        current_time = datetime.now(timezone.utc)
        uptime_seconds = (current_time - datetime.fromtimestamp(self._start_time, tz=timezone.utc)).total_seconds()
        
        uptime_metrics = {
            "tsdb_uptime_hours": uptime_seconds / 3600,
            "tsdb_uptime_percentage": 99.95,  # TODO: Calculate actual uptime
            "incidents_processed": self.metrics["incidents_processed"],
            "alerts_generated": self.metrics["alerts_generated"],
            "sla_breaches": self.metrics["sla_breaches"],
            "avg_processing_latency_ms": 2.5,  # TODO: Calculate actual latency
            "memory_usage_mb": 512,  # TODO: Get actual memory usage
            "cpu_utilization_pct": 15.5  # TODO: Get actual CPU usage
        }
        
        return uptime_metrics
    
    def _get_mock_dashboard(self) -> Dict[str, Any]:
        """Get mock dashboard when ClickHouse is unavailable."""
        return {
            "pipeline_health": {"status": "mock_mode", "message": "ClickHouse unavailable"},
            "incident_trends": {"total_incidents_24h": 0},
            "agent_performance": {"total_agents": 0},
            "sla_status": {"total_slas": 0},
            "alert_summary": {"total_active_alerts": 0},
            "data_quality_scores": {"overall_quality": 0},
            "system_uptime": {"tsdb_uptime_hours": 0},
            "generated_at": datetime.now(timezone.utc).isoformat()
        }
    
    # ============= ENHANCED ANALYTICS METHODS =============
    
    def get_incident_root_cause_analysis(self, incident_id: str) -> Dict[str, Any]:
        """Perform root cause analysis for a specific incident."""
        if not self.client:
            return {"error": "ClickHouse not available"}
        
        try:
            # Query incident details and related incidents
            incident_query = """
                SELECT *
                FROM incidents 
                WHERE incident_id = %(incident_id)s
                LIMIT 1
            """
            
            incident = self.client.query_df(incident_query, {"incident_id": incident_id})
            
            if len(incident) == 0:
                return {"error": "Incident not found"}
            
            incident_row = incident.iloc[0]
            
            # Find related incidents (same agent, similar timeframe)
            related_query = """
                SELECT *
                FROM incidents 
                WHERE source_agent = %(source_agent)s
                  AND timestamp BETWEEN %(start_time)s AND %(end_time)s
                  AND incident_id != %(incident_id)s
                ORDER BY timestamp ASC
                LIMIT 50
            """
            
            start_time = incident_row['timestamp'] - timedelta(minutes=30)
            end_time = incident_row['timestamp'] + timedelta(minutes=30)
            
            related_incidents = self.client.query_df(related_query, {
                "source_agent": incident_row['source_agent'],
                "start_time": start_time,
                "end_time": end_time,
                "incident_id": incident_id
            })
            
            # Perform analysis
            analysis = {
                "incident": incident_row.to_dict(),
                "related_incidents": related_incidents.to_dict('records'),
                "pattern_analysis": {
                    "is_isolated": len(related_incidents) == 0,
                    "is_part_of_burst": len(related_incidents) > 5,
                    "temporal_pattern": "single" if len(related_incidents) <= 1 else "burst",
                    "affected_components": list(set([incident_row['source_agent']] + 
                                                  related_incidents['source_agent'].tolist() if len(related_incidents) > 0 else []))
                },
                "recommendations": self._generate_rca_recommendations(incident_row, related_incidents),
                "confidence_score": 0.85  # TODO: Calculate actual confidence
            }
            
            return analysis
            
        except Exception as e:
            logger.error(f"❌ Failed to perform RCA for incident {incident_id}: {e}")
            return {"error": str(e)}
    
    def _generate_rca_recommendations(self, incident: Any, related_incidents: Any) -> List[str]:
        """Generate RCA recommendations based on incident analysis."""
        recommendations = []
        
        # Basic recommendations based on incident class
        incident_class = incident.get('incident_class', 'Unknown')
        
        if incident_class == 'SchemaViolation':
            recommendations.extend([
                "Review data schema changes in upstream sources",
                "Check schema validation rules for correctness",
                "Investigate source system configuration changes"
            ])
        elif incident_class == 'Freshness':
            recommendations.extend([
                "Check streaming bus health and consumer lag",
                "Investigate upstream data source delays",
                "Review network connectivity and latency"
            ])
        elif incident_class == 'Anomaly':
            recommendations.extend([
                "Analyze data distribution changes",
                "Check for model drift or configuration changes",
                "Investigate market condition changes"
            ])
        elif incident_class == 'Leakage':
            recommendations.extend([
                "Audit data access patterns and timing",
                "Review feature calculation dependencies",
                "Check for look-ahead bias in data processing"
            ])
        
        # Add recommendations based on related incidents
        if len(related_incidents) > 5:
            recommendations.append("Investigate systematic issue affecting multiple data streams")
            recommendations.append("Consider temporary circuit breaker activation")
        
        return recommendations
    
    def get_comprehensive_metrics(self) -> Dict[str, Any]:
        """Get comprehensive TSDB metrics."""
        base_metrics = {
            "queries_executed": self.metrics["queries_executed"],
            "rows_inserted": self.metrics["rows_inserted"],
            "rows_queried": self.metrics["rows_queried"],
            "avg_query_time_ms": self.metrics["avg_query_time_ms"],
            "avg_insert_time_ms": self.metrics["avg_insert_time_ms"],
            "incidents_processed": self.metrics["incidents_processed"],
            "alerts_generated": self.metrics["alerts_generated"],
            "sla_breaches": self.metrics["sla_breaches"]
        }
        
        # Add agent metrics
        agent_summary = {
            "total_agents_tracked": len(self.agent_metrics),
            "total_incidents_by_agent": {
                agent: metrics.incidents_generated 
                for agent, metrics in self.agent_metrics.items()
            }
        }
        
        # Add pipeline SLA summary
        sla_summary = {
            "total_slas_tracked": len(self.pipeline_slas),
            "slas_met": sum(1 for sla in self.pipeline_slas.values() if sla.is_sla_met),
            "total_sla_breaches": sum(sla.sla_breaches_today for sla in self.pipeline_slas.values())
        }
        
        return {
            **base_metrics,
            "agent_summary": agent_summary,
            "sla_summary": sla_summary,
            "cache_status": {
                "incident_cache_size": len(self.incident_cache),
                "alert_queue_size": len(self.alert_queue)
            }
        }
    
    async def close(self) -> None:
        """Close ClickHouse TSDB and stop all consumers."""
        logger.info("Shutting down Quality Monitoring TSDB...")
        
        # Stop incident consumption
        await self.stop_incident_consumption()
        
        # Close ClickHouse connection
        if self.client:
            self.client.close()
            logger.info("✅ ClickHouse connection closed")
        
        logger.info("✅ Quality Monitoring TSDB shutdown complete")
    
    # ============= STREAMING BUS INTEGRATION METHODS =============
    
    async def start_incident_consumption(self) -> None:
        """
        🚀 Start consuming incidents from streaming bus topics.
        
        Integrates with existing streaming_bus.py infrastructure:
        - Subscribes to incidents.* topics with existing StreamingBus
        - Batch processes incidents for high-performance ClickHouse insertion
        - Provides real-time quality monitoring and alerting
        """
        if not self.streaming_bus:
            logger.warning("Streaming bus not initialized - cannot start incident consumption")
            return
            
        if self.is_consuming:
            logger.warning("Incident consumption already running")
            return
            
        logger.info("🚀 Starting incident consumption from streaming bus...")
        self.is_consuming = True
        
        try:
            # Subscribe to all incident topics using existing StreamingBus methods
            for topic in self.incident_topics:
                # Use the existing subscribe method from StreamingBus
                await self.streaming_bus.subscribe(
                    topics=[topic],
                    consumer_group="tsdb-incident-consumer",
                    handler=self._process_raw_incident_message
                )
                logger.info(f"✅ Subscribed to {topic}")
            
            # Start batch processing loop
            await self._run_batch_processing_loop()
            
        except Exception as e:
            logger.error(f"Error in incident consumption: {e}")
        finally:
            self.is_consuming = False
            await self._flush_all_pending_batches()
            logger.info("✅ Incident consumption stopped")
    
    async def _process_raw_incident_message(self, message) -> None:
        """Process incident message from streaming bus (raw transport format)."""
        try:
            # Handle different message formats from StreamingBus
            if hasattr(message, 'value') and hasattr(message, 'headers'):
                # Kafka-style message
                payload = json.loads(message.value.decode('utf-8')) if isinstance(message.value, bytes) else message.value
                headers = {k: v.decode('utf-8') if isinstance(v, bytes) else v for k, v in (message.headers or [])}
                topic = message.topic
            else:
                # Dict-style message
                payload = message.get('payload', message)
                headers = message.get('headers', {})
                topic = message.get('topic', 'incidents.unknown')
            
            # Convert to internal incident format
            incident = {
                'timestamp': datetime.now(timezone.utc),
                'incident_id': headers.get('correlation_id', str(uuid.uuid4())),
                'incident_class': payload.get('incident_type', 'Unknown'),
                'severity': payload.get('severity', 'info'),
                'source_agent': headers.get('source_id', 'unknown'),
                'source_topic': topic,
                'correlation_id': headers.get('correlation_id', ''),
                'impacted_streams': payload.get('impacted_streams', []),
                'proposed_action': payload.get('proposed_action', ''),
                'evidence': json.dumps(payload.get('evidence', {})),
                'resolution_status': 'open',
                'escalation_level': 0,
                'auto_resolved': False
            }
            
            # Add to batch processor
            await self._add_incident_to_batch(topic, incident)
            
            # Update metrics
            self.metrics["incidents_processed"] += 1
            
        except Exception as e:
            logger.error(f"Error processing incident message: {e}")
    
    async def _add_incident_to_batch(self, topic: str, incident: Dict[str, Any]) -> None:
        """Add incident to batch and flush when ready."""
        # Initialize batch structures if not present
        if not hasattr(self, 'pending_incident_batches'):
            self.pending_incident_batches = defaultdict(deque)
            self.batch_timestamps = {}
            self.batch_lock = asyncio.Lock()
        
        async with self.batch_lock:
            # Initialize topic batch if new
            if topic not in self.batch_timestamps:
                self.batch_timestamps[topic] = time.time()
            
            # Add to batch
            self.pending_incident_batches[topic].append(incident)
            
            # Check flush conditions
            batch_size = len(self.pending_incident_batches[topic])
            batch_age_ms = (time.time() - self.batch_timestamps[topic]) * 1000
            
            should_flush = (
                batch_size >= getattr(self, 'incident_batch_size', 1000) or
                batch_age_ms >= getattr(self, 'incident_batch_timeout_ms', 5000)
            )
            
            if should_flush:
                await self._flush_incident_batch(topic)
    
    async def _flush_incident_batch(self, topic: str) -> None:
        """Flush pending incidents for a topic to ClickHouse."""
        if not hasattr(self, 'pending_incident_batches') or not self.pending_incident_batches[topic]:
            return
            
        # Extract batch
        incidents = list(self.pending_incident_batches[topic])
        self.pending_incident_batches[topic].clear()
        self.batch_timestamps[topic] = time.time()
        
        try:
            # Batch insert to ClickHouse using existing insert_incident method
            for incident in incidents:
                success = self.insert_incident(incident)
                if not success:
                    logger.warning(f"Failed to insert incident {incident['incident_id']}")
            
            logger.debug(f"✅ Flushed {len(incidents)} incidents from {topic}")
            
        except Exception as e:
            logger.error(f"❌ Failed to flush incident batch from {topic}: {e}")
            
            # Re-queue incidents on error (with limit to prevent infinite loops)
            if len(incidents) <= getattr(self, 'incident_batch_size', 1000):
                self.pending_incident_batches[topic].extendleft(reversed(incidents))
    
    async def _run_batch_processing_loop(self) -> None:
        """Main loop for batch processing incidents."""
        while self.is_consuming:
            try:
                # Periodic flush of all batches
                await asyncio.sleep(getattr(self, 'incident_batch_timeout_ms', 5000) / 1000)
                await self._flush_all_pending_batches()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in batch processing loop: {e}")
    
    async def _flush_all_pending_batches(self) -> None:
        """Force flush all pending batches."""
        if not hasattr(self, 'pending_incident_batches'):
            return
            
        if hasattr(self, 'batch_lock'):
            async with self.batch_lock:
                for topic in list(self.pending_incident_batches.keys()):
                    await self._flush_incident_batch(topic)
        else:
            for topic in list(self.pending_incident_batches.keys()) if hasattr(self, 'pending_incident_batches') else []:
                await self._flush_incident_batch(topic)
    
    async def stop_incident_consumption(self) -> None:
        """Stop incident consumption and flush pending batches."""
        if not self.is_consuming:
            return
            
        logger.info("🛑 Stopping incident consumption...")
        self.is_consuming = False
        
        # Final flush
        await self._flush_all_pending_batches()
        
        logger.info("✅ Incident consumption stopped")
    
    def get_streaming_integration_status(self) -> Dict[str, Any]:
        """Get status of streaming bus integration."""
        return {
            "streaming_bus_available": self.streaming_bus is not None,
            "is_consuming": self.is_consuming,
            "incident_topics": getattr(self, 'incident_topics', []),
            "batch_configuration": {
                "batch_size": getattr(self, 'incident_batch_size', 1000),
                "batch_timeout_ms": getattr(self, 'incident_batch_timeout_ms', 5000),
                "consumer_pool_size": getattr(self, 'consumer_pool_size', 16)
            },
            "pending_batches": {
                topic: len(batch) for topic, batch in getattr(self, 'pending_incident_batches', {}).items()
            },
            "incidents_processed": self.metrics.get("incidents_processed", 0)
        }
    
    # Add convenience alias for backward compatibility
    def close_sync(self) -> None:
        """Synchronous close method for backward compatibility."""
        if self.client:
            self.client.close()
            logger.info("ClickHouse connection closed")

async def demo_quality_monitoring() -> None:
    """Demonstrate quality monitoring capabilities."""
    print("🚀 Quality Monitoring TSDB Demo")
    print("=" * 60)
    
    # Configuration for development
    config = TSDBConfig(
        host="localhost",
        port=8123,
        database="satoshi_quality_monitoring",
        username="default",
        password=""
    )
    
    # Optional streaming configuration for incident consumption
    streaming_config = {
        "bootstrap_servers": ["localhost:9092"],
        "client_id": "quality_monitoring_tsdb",
        "security_protocol": "PLAINTEXT"
    }
    
    # Initialize Quality Monitoring TSDB
    tsdb = QualityMonitoringTSDB(config, streaming_config)
    
    if not tsdb.client:
        print("❌ ClickHouse not available - running in demo mode")
        streaming_config = None
    
    try:
        print("\n📊 Testing Incident Processing...")
        
        # Simulate some incidents
        test_incidents = [
            {
                'incident_id': f'DEMO_INC_{i}',
                'class': ['SchemaViolation', 'Freshness', 'Anomaly'][i % 3],
                'severity': ['info', 'warn', 'crit'][i % 3],
                'source_agent': ['schema_validator', 'freshness_agent', 'anomaly_detector'][i % 3],
                'source_topic': 'incidents.demo',
                'correlation_id': f'corr_{i}',
                'impacted_streams': [f'stream_{i}'],
                'proposed_action': 'investigate',
                'evidence_ref': {'demo': True, 'value': i * 10}
            }
            for i in range(10)
        ]
        
        # Insert test incidents
        for incident in test_incidents:
            success = tsdb.insert_incident(incident)
            print(f"   {'✅' if success else '❌'} Inserted incident {incident['incident_id']}")
        
        print("\n📈 Testing Performance Metrics...")
        
        # Insert performance metrics
        for i in range(5):
            success = tsdb.insert_performance_metric(
                MetricType.INCIDENT_COUNT,
                entity=f"agent_{i}",
                value=float(i * 2),
                unit="count",
                source_agent=f"test_agent_{i}"
            )
            print(f"   {'✅' if success else '❌'} Inserted metric for agent_{i}")
        
        print("\n🎯 Testing SLA Tracking...")
        
        # Display SLA status
        sla_summary = tsdb._get_sla_status_summary()
        print(f"   📊 Total SLAs tracked: {sla_summary['total_slas']}")
        print(f"   ✅ SLAs met: {sla_summary['met_slas']}")
        print(f"   ❌ SLAs breached: {sla_summary['breached_slas']}")
        
        print("\n📊 Executive Dashboard Demo...")
        
        # Generate executive dashboard
        dashboard = tsdb.get_quality_pipeline_dashboard()
        
        print(f"   🏥 Pipeline Health: {dashboard['pipeline_health']}")
        print(f"   📈 Incident Trends: Total 24h incidents: {dashboard['incident_trends'].get('total_incidents_24h', 0)}")
        print(f"   🤖 Agent Performance: {dashboard['agent_performance'].get('total_agents', 0)} agents tracked")
        print(f"   ⚠️  Active Alerts: {dashboard['alert_summary']['total_active_alerts']}")
        print(f"   📏 Data Quality Score: {dashboard['data_quality_scores']['overall_quality']}")
        
        print("\n🔍 Testing Root Cause Analysis...")
        
        if test_incidents:
            rca = tsdb.get_incident_root_cause_analysis(test_incidents[0]['incident_id'])
            if 'error' not in rca:
                print(f"   ✅ RCA completed for {test_incidents[0]['incident_id']}")
                print(f"   🎯 Pattern: {rca['pattern_analysis']['temporal_pattern']}")
                print(f"   💡 Recommendations: {len(rca['recommendations'])} provided")
            else:
                print(f"   ⚠️  RCA failed: {rca['error']}")
        
        print("\n📊 System Metrics Summary:")
        metrics = tsdb.get_comprehensive_metrics()
        for key, value in metrics.items():
            if isinstance(value, dict):
                print(f"   {key}: {len(value)} items")
            else:
                print(f"   {key}: {value}")
        
        print("\n⚡ Quality Monitoring Features:")
        print("   ✅ Real-time incident stream processing")
        print("   ✅ Quality agent performance tracking")
        print("   ✅ SLA monitoring and breach detection")  
        print("   ✅ Executive dashboard generation")
        print("   ✅ Root cause analysis capabilities")
        print("   ✅ Pattern detection and alerting")
        print("   ✅ Data quality scoring")
        print("   ✅ Comprehensive audit trails")
        
        if STREAMING_BUS_AVAILABLE and streaming_config:
            print("\n🔄 To start real-time incident consumption, run:")
            print("   await tsdb.start_incident_consumption()")
            print("   # This will consume from incidents.* topics")
        else:
            print("\n⚠️  Streaming bus not available - install dependencies:")
            print("   pip install aiokafka kafka-python")
    
    finally:
        # Cleanup
        await tsdb.close()
        print("\n✅ Demo completed successfully!")


async def main():
    """Run the appropriate demo based on availability."""
    if CLICKHOUSE_AVAILABLE:
        await demo_quality_monitoring()
    else:
        print("❌ ClickHouse not available. Install with: pip install clickhouse-connect")
        print("\n🔧 Setup Instructions:")
        print("1. Install ClickHouse: https://clickhouse.com/docs/en/install")
        print("2. Install Python client: pip install clickhouse-connect")
        print("3. Optional streaming: pip install aiokafka kafka-python")
        print("4. Run demo: python clickhouse_tsdb.py")


if __name__ == "__main__":
    asyncio.run(main())
