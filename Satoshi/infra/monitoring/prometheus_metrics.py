#!/usr/bin/env python3
"""
Prometheus Metrics Collector

Institutional-grade metrics collection for HFT infrastructure.
Provides real-time performance, latency, and business metrics.

Metrics Categories:
- System: CPU, memory, network, disk I/O
- Application: Request rates, error rates, latencies
- Business: Trade counts, PnL, risk exposure
- Infrastructure: Kafka lag, database connections, circuit breakers

Architecture:
- Uses official prometheus_client library for metric registration
- Maintains global REGISTRY for cross-process visibility
- Provides high-level API with automatic label handling
- Thread-safe operations for concurrent access
"""

import time
import psutil
import asyncio
from typing import Dict, List, Any, Optional, Callable, TYPE_CHECKING
from dataclasses import dataclass, field
from collections import defaultdict, deque
from threading import Lock
import logging

# Initialize logger first
logger = logging.getLogger(__name__)

# Prometheus client library for official metric types and REGISTRY
if TYPE_CHECKING:
    from prometheus_client import Counter, Gauge, Histogram, Summary, Info, CollectorRegistry

try:
    from prometheus_client import Counter, Gauge, Histogram, Summary, Info
    from prometheus_client import REGISTRY, CollectorRegistry
    PROMETHEUS_CLIENT_AVAILABLE = True
except ImportError:
    PROMETHEUS_CLIENT_AVAILABLE = False
    Counter = None  # type: ignore
    Gauge = None  # type: ignore
    Histogram = None  # type: ignore
    Summary = None  # type: ignore
    Info = None  # type: ignore
    REGISTRY = None  # type: ignore
    CollectorRegistry = None  # type: ignore
    logger.warning("prometheus_client not available - metrics will not be exposed")

# =============================
# METRIC DEFINITIONS
# =============================

@dataclass
class MetricSample:
    """Individual metric sample with timestamp."""
    name: str
    value: float
    timestamp: float
    labels: Dict[str, str] = field(default_factory=dict)
    help_text: str = ""

@dataclass
class TimeSeries:
    """Time series data for a metric."""
    name: str
    help_text: str
    metric_type: str  # counter, gauge, histogram, summary
    samples: deque = field(default_factory=lambda: deque(maxlen=1000))
    labels: Dict[str, str] = field(default_factory=dict)

class MetricsCollector:
    """
    High-performance metrics collector optimized for HFT workloads.
    
    Features:
    - Official prometheus_client integration with global REGISTRY
    - Sub-millisecond metric recording with lock-free operations
    - Automatic system metrics collection
    - Thread-safe metric registration and updates
    - Dynamic label support for dimensional metrics
    
    Architecture:
    - Primary storage: prometheus_client metrics (Counter, Gauge, Histogram)
    - Fallback storage: Internal dictionaries for compatibility
    - Global REGISTRY ensures metrics visible across process boundaries
    """
    
    def __init__(self, collection_interval: float = 1.0):
        self.collection_interval = collection_interval
        
        # prometheus_client metric objects (registered with global REGISTRY)
        # Using Any type since Counter/Gauge/Histogram may be None if prometheus_client unavailable
        self._prom_counters: Dict[str, Any] = {}
        self._prom_gauges: Dict[str, Any] = {}
        self._prom_histograms: Dict[str, Any] = {}
        
        # Legacy internal storage (for backward compatibility and custom operations)
        self.metrics: Dict[str, TimeSeries] = {}
        self.counters: Dict[str, float] = defaultdict(float)
        self.gauges: Dict[str, float] = {}
        self.histograms: Dict[str, List[float]] = defaultdict(list)
        
        # Thread safety
        self._lock = Lock()
        
        # System metrics
        self._last_cpu_times = psutil.cpu_times()
        self._last_network_io = psutil.net_io_counters()
        self._last_disk_io = psutil.disk_io_counters()
        
        # Collection task
        self._collection_task: Optional[asyncio.Task] = None
        self._running = False
        
        # Initialize core metrics
        self._initialize_core_metrics()
        
        logger.info(f"MetricsCollector initialized (prometheus_client={'available' if PROMETHEUS_CLIENT_AVAILABLE else 'unavailable'})")
    
    def _initialize_core_metrics(self) -> None:
        """Initialize essential system, application, and crypto-native business metrics."""
        # System metrics
        self.register_gauge("system_cpu_percent", "CPU utilization percentage")
        self.register_gauge("system_memory_percent", "Memory utilization percentage") 
        self.register_gauge("system_disk_percent", "Disk utilization percentage")
        self.register_counter("system_network_bytes_sent", "Network bytes sent")
        self.register_counter("system_network_bytes_recv", "Network bytes received")
        
        # Application metrics
        self.register_counter("http_requests_total", "Total HTTP requests")
        self.register_counter("http_request_errors_total", "Total HTTP request errors")
        self.register_histogram("http_request_duration_seconds", "HTTP request duration")
        
        # =================================================================================
        # DATA QUALITY AGENT METRICS - Integrated with Quality Orchestrator
        # =================================================================================
        
        # Schema Validator Metrics
        self.register_gauge("schema_validator_status", "Schema validator agent status (1=healthy, 0=unhealthy)")
        self.register_counter("schema_validation_total", "Total schema validations performed", {"table_name": "", "status": "", "venue": ""})
        self.register_histogram("schema_validation_duration_seconds", "Time spent validating schemas", labels={"table_name": "", "venue": ""})
        self.register_counter("schema_violations_total", "Total schema violations detected", {"table_name": "", "violation_type": "", "venue": ""})
        self.register_gauge("schema_compliance_ratio", "Ratio of compliant vs total messages", {"table_name": "", "venue": ""})
        
        # Leakage Police Metrics
        self.register_gauge("leakage_police_status", "Leakage police agent status (1=healthy, 0=unhealthy)")
        self.register_counter("leakage_incidents_total", "Total leakage incidents detected", {"leakage_type": "", "severity": "", "source": ""})
        self.register_gauge("leakage_detection_score", "Current leakage detection confidence score", {"feature_set": ""})
        self.register_histogram("leakage_analysis_duration_seconds", "Time spent analyzing for leakage", labels={"analysis_type": ""})
        self.register_gauge("temporal_leakage_risk", "Current temporal leakage risk score (0-1)", {"time_window": ""})
        
        # Anomaly Detector Metrics
        self.register_gauge("anomaly_detector_status", "Anomaly detector agent status (1=healthy, 0=unhealthy)")
        self.register_counter("anomalies_detected_total", "Total anomalies detected", {"anomaly_type": "", "severity": "", "table": ""})
        self.register_gauge("anomaly_detection_score", "Current anomaly detection confidence", {"detector_type": "", "symbol": ""})
        self.register_histogram("anomaly_analysis_duration_seconds", "Time spent on anomaly detection")
        self.register_gauge("statistical_deviation_score", "Current statistical deviation from baseline", {"metric": "", "symbol": ""})
        
        # Freshness Agent Metrics
        self.register_gauge("freshness_agent_status", "Freshness agent status (1=healthy, 0=unhealthy)")
        self.register_gauge("data_staleness_seconds", "Current data staleness in seconds", {"stream_name": "", "venue": ""})
        self.register_counter("freshness_violations_total", "Total freshness SLA violations", {"stream_name": "", "severity": ""})
        self.register_gauge("freshness_sla_ratio", "Ratio of fresh vs stale messages", {"stream_name": "", "sla_threshold": ""})
        self.register_histogram("freshness_check_duration_seconds", "Time spent checking data freshness")
        
        # Reconciler Agent Metrics
        self.register_gauge("reconciler_agent_status", "Reconciler agent status (1=healthy, 0=unhealthy)")
        self.register_counter("reconciliation_discrepancies_total", "Total reconciliation discrepancies", {"source_pair": "", "discrepancy_type": ""})
        self.register_gauge("cross_source_accuracy_ratio", "Cross-source data accuracy ratio", {"source_pair": ""})
        self.register_histogram("reconciliation_duration_seconds", "Time spent on cross-source reconciliation", labels={"source_count": ""})
        self.register_gauge("data_consistency_score", "Overall data consistency score across sources", {"domain": ""})
        
        # Data Quality Orchestrator Metrics
        self.register_counter("quality_pipeline_executions_total", "Total quality pipeline executions", {"pipeline_mode": "", "result": ""})
        self.register_histogram("quality_pipeline_duration_seconds", "End-to-end quality pipeline execution time", labels={"pipeline_mode": ""})
        self.register_gauge("overall_data_quality_score", "Overall weighted data quality score", {"pipeline_mode": ""})
        self.register_gauge("quality_circuit_breaker_state", "Quality circuit breaker state (0=closed, 1=open)", {"component": ""})
        self.register_counter("quality_incidents_published_total", "Total quality incidents published", {"incident_type": "", "severity": ""})
        
        # =================================================================================
        # CRYPTO-NATIVE BUSINESS METRICS - Institutional Trading Focus
        # =================================================================================
        
        # Core Trading Metrics
        self.register_counter("trades_executed_total", "Total trades executed", {"symbol": "", "side": "", "venue": ""})
        self.register_counter("orders_placed_total", "Total orders placed", {"symbol": "", "order_type": "", "venue": ""})
        self.register_gauge("portfolio_value_usd", "Current portfolio value in USD", {"strategy": ""})
        self.register_gauge("risk_exposure_percent", "Current risk exposure percentage", {"asset_class": ""})
        
        # Crypto-Specific Market Metrics
        self.register_gauge("funding_rate_current", "Current perpetual funding rate (8h annualized)", {"symbol": "", "venue": ""})
        self.register_gauge("basis_spread_bps", "Current futures basis spread in basis points", {"symbol": "", "expiry": "", "venue": ""})
        self.register_gauge("implied_volatility_current", "Current implied volatility (30-day ATM)", {"symbol": "", "venue": ""})
        self.register_gauge("realized_volatility_30d", "30-day realized volatility", {"symbol": ""})
        self.register_gauge("vol_surface_skew", "Volatility surface skew (25D RR)", {"symbol": "", "expiry": ""})
        
        # Crypto On-Chain Metrics
        self.register_gauge("onchain_volume_24h_usd", "24-hour on-chain transaction volume", {"chain": "", "token": ""})
        self.register_gauge("stablecoin_supply_total", "Total stablecoin supply", {"stablecoin": ""})
        self.register_gauge("bridge_inflows_24h_usd", "24-hour bridge inflows to exchanges", {"bridge": "", "destination": ""})
        self.register_gauge("whale_wallet_activity", "Large wallet activity score", {"token": "", "threshold_usd": ""})
        self.register_gauge("network_hash_rate", "Network hash rate", {"chain": ""})
        
        # =================================================================================
        # DATA COLLECTION FLOW METRICS - Bronze Layer Ingestion
        # =================================================================================
        
        # Onchain Data Collection
        self.register_counter("onchain_blocks_processed_total", "Total blockchain blocks processed", {"chain": "", "status": ""})
        self.register_counter("onchain_transfer_events_total", "Total ERC20/native transfer events collected", {"chain": "", "token": ""})
        self.register_gauge("onchain_latest_block", "Latest block number processed", {"chain": ""})
        self.register_gauge("onchain_sync_lag_blocks", "Number of blocks behind chain tip", {"chain": ""})
        self.register_histogram("onchain_block_processing_duration_seconds", "Time to process a block batch", labels={"chain": "", "batch_size": ""})
        self.register_counter("onchain_rpc_calls_total", "Total RPC calls made", {"chain": "", "method": "", "status": ""})
        self.register_counter("onchain_events_published_total", "Total onchain events published to Kafka", {"chain": "", "event_type": ""})
        
        # Exchange Data Collection
        self.register_counter("exchange_trades_collected_total", "Total trades collected from exchanges", {"exchange": "", "symbol": ""})
        self.register_counter("exchange_orderbook_updates_total", "Total orderbook snapshots collected", {"exchange": "", "symbol": ""})
        self.register_counter("exchange_api_calls_total", "Total exchange API calls made", {"exchange": "", "endpoint": "", "status": ""})
        self.register_histogram("exchange_api_latency_seconds", "Exchange API response latency", labels={"exchange": "", "endpoint": ""})
        self.register_counter("exchange_data_published_total", "Total exchange data published to Kafka", {"exchange": "", "data_type": ""})
        self.register_gauge("exchange_websocket_status", "Exchange websocket connection status (1=connected)", {"exchange": "", "channel": ""})
        
        # Options Data Collection
        self.register_counter("options_data_published_total", "Total options chain data published to Kafka", {"venue": "", "symbol": ""})
        
        # Events Data Collection
        self.register_counter("events_data_published_total", "Total off-chain events published to Kafka", {"source": "", "event_type": ""})
        
        # Data Ingestion Rate Metrics
        self.register_gauge("data_ingestion_rate_per_second", "Current data ingestion rate", {"source": "", "data_type": ""})
        self.register_counter("data_bytes_ingested_total", "Total bytes ingested", {"source": "", "data_type": ""})
        self.register_histogram("data_message_size_bytes", "Size of ingested messages", labels={"source": "", "data_type": ""})
        
        # Market Microstructure Metrics  
        self.register_gauge("orderbook_depth_bps", "Order book depth at N bps from mid", {"symbol": "", "venue": "", "depth_bps": ""})
        self.register_gauge("bid_ask_spread_bps", "Current bid-ask spread in basis points", {"symbol": "", "venue": ""})
        self.register_gauge("market_impact_bps", "Estimated market impact for standard size", {"symbol": "", "venue": "", "notional_usd": ""})
        self.register_gauge("venue_dominance_ratio", "Venue's share of total volume", {"symbol": "", "venue": ""})
        self.register_histogram("order_fill_latency_seconds", "Time from order placement to fill", labels={"venue": "", "order_type": ""})
        
        # Risk Management Metrics
        self.register_gauge("var_95_daily_usd", "Daily 95% Value at Risk", {"strategy": "", "lookback_days": ""})
        self.register_gauge("expected_shortfall_usd", "Expected shortfall beyond VaR", {"strategy": "", "confidence": ""})
        self.register_gauge("correlation_btc_spy", "Rolling correlation between BTC and SPY", {"window_days": ""})
        self.register_gauge("portfolio_beta", "Portfolio beta to market benchmark", {"benchmark": ""})
        self.register_gauge("max_drawdown_percent", "Maximum drawdown from peak", {"strategy": "", "period": ""})
        
        # Alpha Generation Metrics
        self.register_gauge("sharpe_ratio", "Rolling Sharpe ratio", {"strategy": "", "window_days": ""})
        self.register_gauge("information_ratio", "Information ratio vs benchmark", {"strategy": "", "benchmark": ""})
        self.register_gauge("alpha_decay_rate", "Rate of alpha decay over time", {"strategy": "", "feature_set": ""})
        self.register_counter("strategy_signals_total", "Total strategy signals generated", {"strategy": "", "signal_type": ""})
        self.register_gauge("signal_accuracy_ratio", "Ratio of profitable signals", {"strategy": "", "lookback_days": ""})
        
        # Execution Quality Metrics
        self.register_histogram("execution_slippage_bps", "Execution slippage in basis points", labels={"symbol": "", "venue": "", "strategy": ""})
        self.register_gauge("fill_ratio", "Ratio of filled vs placed orders", {"symbol": "", "venue": "", "order_type": ""})
        self.register_histogram("time_to_fill_seconds", "Time from order placement to complete fill", labels={"venue": "", "size_bucket": ""})
        self.register_gauge("execution_shortfall_bps", "Implementation shortfall in basis points", {"strategy": "", "symbol": ""})
        
        # Regulatory and Compliance Metrics
        self.register_counter("compliance_violations_total", "Total compliance violations detected", {"violation_type": "", "severity": ""})
        self.register_gauge("position_limit_utilization", "Current position limit utilization ratio", {"symbol": "", "limit_type": ""})
        self.register_counter("wash_trading_alerts_total", "Total wash trading alerts", {"symbol": "", "venue": ""})
        self.register_gauge("surveillance_score", "Current surveillance risk score", {"entity": "", "risk_type": ""})
        
        # Infrastructure metrics  
        self.register_gauge("kafka_consumer_lag", "Kafka consumer lag", {"consumer_group": "", "topic": ""})
        self.register_counter("kafka_messages_consumed", "Kafka messages consumed", {"consumer_group": "", "topic": ""})
        self.register_counter("kafka_messages_produced", "Kafka messages produced", {"topic": ""})
        self.register_gauge("circuit_breaker_state", "Circuit breaker state (0=closed, 1=open)", {"component": "", "breaker_type": ""})
        self.register_counter("breaker_intent_decisions_total", "Breaker intents processed by orchestrator", {"component_id": "", "intent": "", "decision": "", "severity": "", "requested_by": ""})
        self.register_gauge("breaker_component_state", "Breaker state as observed by orchestrator (0=closed, 0.5=half_open, 1=open)", {"component_id": ""})
        self.register_gauge("breaker_state_last_update_timestamp", "Last update timestamp for breaker state", {"component_id": ""})
        
        # =================================================================================
        # DASHBOARD-REQUIRED METRICS (Grafana dashboard expectations)
        # =================================================================================
        
        # Streaming Bus Health
        self.register_gauge("streaming_bus_health_status", "StreamingBus health status (1=healthy, 0=unhealthy)")
        self.register_gauge("streaming_bus_active_topics", "Number of active Kafka topics")
        self.register_histogram("data_processing_latency_ms", "Data processing latency in milliseconds", labels={"layer": "", "component": ""})
        self.register_gauge("data_quality_score", "Overall data quality score (0-100)", {"layer": ""})
        
        # Gold Layer - OHLCV Aggregator
        self.register_counter("ohlcv_bars_created_total", "Total OHLCV bars created", {"interval": "", "venue": "", "symbol": ""})
        self.register_histogram("ohlcv_bar_latency_p95", "95th percentile OHLCV bar latency", labels={"interval": ""})
        self.register_gauge("ohlcv_active_windows", "Number of active OHLCV windows", {"interval": ""})
        self.register_counter("ohlcv_circuit_breaker_trips", "OHLCV circuit breaker trip count", {"reason": ""})
        self.register_counter("ohlcv_publish_failures", "OHLCV publish failure count", {"interval": "", "reason": ""})
        
        # Platinum Layer - Feature Engineering
        self.register_histogram("feature_computation_seconds", "Time to compute features", labels={"symbol": "", "feature_type": ""})
        self.register_counter("features_published_total", "Total features published", {"symbol": "", "feature_type": ""})
        self.register_counter("feature_computation_errors_total", "Total feature computation errors", {"symbol": "", "error_type": ""})
        self.register_gauge("feature_confidence_score", "Feature quality confidence score", {"symbol": ""})
        self.register_gauge("feature_data_age_milliseconds", "Age of input data in milliseconds", {"symbol": ""})
        
        # Platinum Layer - OrderbookDepthAnalyzer (Microstructure Features)
        self.register_counter("depth_features_computed_total", "Total orderbook depth features computed", {"symbol": "", "venue": ""})
        self.register_histogram("depth_computation_duration_seconds", "Time to compute depth features", labels={"symbol": ""})
        self.register_gauge("depth_spoofing_score", "Current spoofing detection score (0-1)", {"symbol": "", "venue": ""})
        self.register_gauge("depth_imbalance_10bps", "Current 10bps depth imbalance (-1 to 1)", {"symbol": "", "venue": ""})
        self.register_counter("depth_analyzer_errors_total", "Total depth analyzer errors", {"error_type": ""})
        
        # WorkloadDistributor / Partition Management Metrics
        self.register_counter("workload_hot_keys_detected_total", "Total hot keys detected (BTC, ETH, etc.)", {"topic": ""})
        self.register_counter("workload_skew_detections_total", "Total partition skew detections", {"topic": ""})
        self.register_counter("workload_routing_decisions_total", "Partition routing decisions by strategy", {"topic": "", "strategy": ""})
        self.register_gauge("workload_active_hot_keys", "Number of currently active hot keys", {"topic": ""})
        self.register_gauge("workload_partition_load_imbalance", "Current partition load imbalance ratio", {"topic": ""})
        
        # Quality pipeline orchestration metrics
        self.register_counter("quality_pipeline_messages_total", "Quality pipeline messages processed", {"source_topic": "", "decision": ""})
        self.register_histogram("quality_pipeline_duration_seconds", "End-to-end quality pipeline duration", labels={"source_topic": ""})
        # Rate budget and throttling metrics
        self.register_gauge("rate_budget_available_tokens", "Available tokens in shared rate budget", {"domain": ""})
        self.register_gauge("rate_budget_configured_qps", "Configured QPS for rate budget domain", {"domain": ""})
        self.register_gauge("rate_budget_configured_burst", "Configured burst capacity for rate budget domain", {"domain": ""})
        self.register_gauge("rate_budget_qps_utilization", "Recent permits consumed in last second", {"domain": ""})
        self.register_gauge("rate_budget_avg_wait_seconds", "Average wait time for rate budget borrows", {"domain": ""})
        self.register_gauge("rate_budget_throttled_events", "Total throttled events for domain", {"domain": ""})
        self.register_gauge("rate_budget_borrow_count", "Total borrow count for rate budget domain", {"domain": ""})
        self.register_gauge("rate_budget_rate_limit_responses", "Upstream rate-limit responses (429)", {"domain": ""})
        self.register_gauge("rate_limit_responses_count", "Agent reported rate-limit responses", {"component": "", "domain": ""})
        self.register_gauge("rate_budget_timeouts_count", "Agent rate-budget timeout events", {"component": "", "domain": ""})
        
    def register_counter(self, name: str, help_text: str, labels: Optional[Dict[str, str]] = None) -> None:
        """
        Register a counter metric with prometheus_client.
        
        Counters are monotonically increasing values (e.g., total requests, errors).
        Registered with global REGISTRY for Prometheus scraping.
        """
        with self._lock:
            # Store in internal registry for backward compatibility
            self.metrics[name] = TimeSeries(
                name=name,
                help_text=help_text,
                metric_type="counter",
                labels=labels or {}
            )
            
            # Register with prometheus_client if available
            if PROMETHEUS_CLIENT_AVAILABLE and Counter is not None and REGISTRY is not None and name not in self._prom_counters:
                try:
                    label_names = list(labels.keys()) if labels else []
                    self._prom_counters[name] = Counter(
                        name,
                        help_text,
                        labelnames=label_names,
                        registry=REGISTRY
                    )
                    logger.debug(f"Registered Counter: {name} with labels {label_names}")
                except Exception as e:
                    # Metric may already be registered (e.g., across multiple instances)
                    logger.debug(f"Counter {name} already registered or error: {e}")
    
    def register_gauge(self, name: str, help_text: str, labels: Optional[Dict[str, str]] = None) -> None:
        """
        Register a gauge metric with prometheus_client.
        
        Gauges can go up or down (e.g., temperature, active connections).
        Registered with global REGISTRY for Prometheus scraping.
        """
        with self._lock:
            # Store in internal registry for backward compatibility
            self.metrics[name] = TimeSeries(
                name=name,
                help_text=help_text,
                metric_type="gauge",
                labels=labels or {}
            )
            
            # Register with prometheus_client if available
            if PROMETHEUS_CLIENT_AVAILABLE and Gauge is not None and REGISTRY is not None and name not in self._prom_gauges:
                try:
                    label_names = list(labels.keys()) if labels else []
                    self._prom_gauges[name] = Gauge(
                        name,
                        help_text,
                        labelnames=label_names,
                        registry=REGISTRY
                    )
                    logger.debug(f"Registered Gauge: {name} with labels {label_names}")
                except Exception as e:
                    logger.debug(f"Gauge {name} already registered or error: {e}")
    
    def register_histogram(self, name: str, help_text: str, 
                         buckets: Optional[List[float]] = None, labels: Optional[Dict[str, str]] = None) -> None:
        """
        Register a histogram metric with prometheus_client.
        
        Histograms track distributions of values (e.g., request latencies).
        Uses HFT-optimized buckets by default (microseconds to seconds).
        Registered with global REGISTRY for Prometheus scraping.
        """
        if buckets is None:
            # HFT-optimized latency buckets (microseconds to seconds)
            buckets = [0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 
                      0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]
        
        with self._lock:
            # Store in internal registry for backward compatibility
            self.metrics[name] = TimeSeries(
                name=name,
                help_text=help_text,
                metric_type="histogram",
                labels=labels or {}
            )
            # Store bucket configuration in TimeSeries metadata
            self.metrics[name].__dict__['buckets'] = buckets
            
            # Register with prometheus_client if available
            if PROMETHEUS_CLIENT_AVAILABLE and Histogram is not None and REGISTRY is not None and name not in self._prom_histograms:
                try:
                    label_names = list(labels.keys()) if labels else []
                    self._prom_histograms[name] = Histogram(
                        name,
                        help_text,
                        labelnames=label_names,
                        buckets=buckets,
                        registry=REGISTRY
                    )
                    logger.debug(f"Registered Histogram: {name} with labels {label_names} and {len(buckets)} buckets")
                except Exception as e:
                    logger.debug(f"Histogram {name} already registered or error: {e}")
    
    def increment_counter(self, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        """
        Increment a counter metric (thread-safe, uses prometheus_client).
        
        Updates both prometheus_client Counter and internal storage for compatibility.
        Automatically handles label dimensions.
        """
        # Update internal counter (legacy compatibility)
        key = f"{name}_{hash(frozenset(labels.items()) if labels else frozenset())}"
        self.counters[key] += value
        
        # Update prometheus_client Counter if available
        if PROMETHEUS_CLIENT_AVAILABLE and name in self._prom_counters:
            try:
                if labels:
                    self._prom_counters[name].labels(**labels).inc(value)
                else:
                    self._prom_counters[name].inc(value)
            except Exception as e:
                logger.debug(f"Error updating prometheus Counter {name}: {e}")
        
        # Record sample for time series (internal tracking)
        sample = MetricSample(
            name=name,
            value=self.counters[key],
            timestamp=time.time(),
            labels=labels or {}
        )
        
        if name in self.metrics:
            self.metrics[name].samples.append(sample)
    
    def set_gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """
        Set a gauge metric value (thread-safe, uses prometheus_client).
        
        Updates both prometheus_client Gauge and internal storage for compatibility.
        Gauges can be set to any value (unlike counters which only increment).
        """
        # Update internal gauge (legacy compatibility)
        key = f"{name}_{hash(frozenset(labels.items()) if labels else frozenset())}"
        self.gauges[key] = value
        
        # Update prometheus_client Gauge if available
        if PROMETHEUS_CLIENT_AVAILABLE and name in self._prom_gauges:
            try:
                if labels:
                    self._prom_gauges[name].labels(**labels).set(value)
                else:
                    self._prom_gauges[name].set(value)
            except Exception as e:
                logger.debug(f"Error updating prometheus Gauge {name}: {e}")
        
        # Record sample for time series (internal tracking)
        sample = MetricSample(
            name=name,
            value=value,
            timestamp=time.time(),
            labels=labels or {}
        )
        
        if name in self.metrics:
            self.metrics[name].samples.append(sample)
    
    def observe_histogram(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """
        Observe a value in a histogram metric (thread-safe, uses prometheus_client).
        
        Updates both prometheus_client Histogram and internal storage for compatibility.
        Histograms automatically calculate quantiles, sums, and bucket counts.
        """
        # Update internal histogram (legacy compatibility)
        key = f"{name}_{hash(frozenset(labels.items()) if labels else frozenset())}"
        self.histograms[key].append(value)
        
        # Keep only recent observations to prevent memory growth
        if len(self.histograms[key]) > 1000:
            self.histograms[key] = self.histograms[key][-1000:]
        
        # Update prometheus_client Histogram if available
        if PROMETHEUS_CLIENT_AVAILABLE and name in self._prom_histograms:
            try:
                if labels:
                    self._prom_histograms[name].labels(**labels).observe(value)
                else:
                    self._prom_histograms[name].observe(value)
            except Exception as e:
                logger.debug(f"Error updating prometheus Histogram {name}: {e}")
        
        # Record sample for time series (internal tracking)
        sample = MetricSample(
            name=name,
            value=value,
            timestamp=time.time(),
            labels=labels or {}
        )
        
        if name in self.metrics:
            self.metrics[name].samples.append(sample)
    
    def time_function(self, metric_name: str = "function_duration_seconds"):
        """Decorator to time function execution."""
        def decorator(func: Callable) -> Callable:
            if asyncio.iscoroutinefunction(func):
                async def async_wrapper(*args, **kwargs):
                    start_time = time.time()
                    try:
                        result = await func(*args, **kwargs)
                        return result
                    finally:
                        duration = time.time() - start_time
                        self.observe_histogram(metric_name, duration, 
                                             {"function": func.__name__})
                return async_wrapper
            else:
                def sync_wrapper(*args, **kwargs):
                    start_time = time.time()
                    try:
                        result = func(*args, **kwargs)
                        return result
                    finally:
                        duration = time.time() - start_time
                        self.observe_histogram(metric_name, duration,
                                             {"function": func.__name__})
                return sync_wrapper
        return decorator
    
    async def collect_system_metrics(self) -> None:
        """Collect system-level metrics."""
        try:
            # CPU metrics - use short blocking interval for accuracy
            # interval=0.1 gives accurate reading without blocking too long
            cpu_percent = psutil.cpu_percent(interval=0.1)
            self.set_gauge("system_cpu_percent", cpu_percent)
            
            # Memory metrics
            memory = psutil.virtual_memory()
            self.set_gauge("system_memory_percent", memory.percent)
            self.set_gauge("system_memory_available_bytes", memory.available)
            
            # Disk metrics
            disk = psutil.disk_usage('/')
            disk_percent = (disk.used / disk.total) * 100
            self.set_gauge("system_disk_percent", disk_percent)
            
            # Network metrics
            net_io = psutil.net_io_counters()
            if self._last_network_io:
                bytes_sent_delta = net_io.bytes_sent - self._last_network_io.bytes_sent
                bytes_recv_delta = net_io.bytes_recv - self._last_network_io.bytes_recv
                self.increment_counter("system_network_bytes_sent", bytes_sent_delta)
                self.increment_counter("system_network_bytes_recv", bytes_recv_delta)
            self._last_network_io = net_io
            
        except Exception as e:
            logger.error(f"Failed to collect system metrics: {e}")
    
    def generate_prometheus_output(self) -> str:
        """Generate Prometheus exposition format output."""
        output_lines = []
        
        # Process counters
        for key, value in self.counters.items():
            if '_' in key:
                name = key.rsplit('_', 1)[0]
                if name in self.metrics:
                    metric = self.metrics[name]
                    output_lines.append(f"# HELP {name} {metric.help_text}")
                    output_lines.append(f"# TYPE {name} counter")
                    output_lines.append(f"{name} {value}")
        
        # Process gauges
        for key, value in self.gauges.items():
            if '_' in key:
                name = key.rsplit('_', 1)[0]
                if name in self.metrics:
                    metric = self.metrics[name]
                    output_lines.append(f"# HELP {name} {metric.help_text}")
                    output_lines.append(f"# TYPE {name} gauge")
                    output_lines.append(f"{name} {value}")
        
        # Process histograms
        for key, observations in self.histograms.items():
            if '_' in key and observations:
                name = key.rsplit('_', 1)[0]
                if name in self.metrics:
                    metric = self.metrics[name]
                    output_lines.append(f"# HELP {name} {metric.help_text}")
                    output_lines.append(f"# TYPE {name} histogram")
                    
                    # Generate histogram buckets
                    buckets = getattr(metric, 'buckets', [0.001, 0.01, 0.1, 1.0, 10.0])
                    total_count = len(observations)
                    
                    for bucket in buckets:
                        count = sum(1 for obs in observations if obs <= bucket)
                        output_lines.append(f"{name}_bucket{{le=\"{bucket}\"}} {count}")
                    
                    output_lines.append(f"{name}_bucket{{le=\"+Inf\"}} {total_count}")
                    output_lines.append(f"{name}_count {total_count}")
                    
                    if observations:
                        total_sum = sum(observations)
                        output_lines.append(f"{name}_sum {total_sum}")
        
        return '\n'.join(output_lines) + '\n'
    
    async def start_collection(self) -> None:
        """Start automatic metrics collection."""
        if self._running:
            return
            
        self._running = True
        self._collection_task = asyncio.create_task(self._collection_loop())
        logger.info(f"Started metrics collection with {self.collection_interval}s interval")
    
    async def stop_collection(self) -> None:
        """Stop automatic metrics collection."""
        self._running = False
        if self._collection_task:
            self._collection_task.cancel()
            try:
                await self._collection_task
            except asyncio.CancelledError:
                pass
        logger.info("Stopped metrics collection")
    
    async def _collection_loop(self) -> None:
        """Main collection loop."""
        while self._running:
            try:
                await self.collect_system_metrics()
                await asyncio.sleep(self.collection_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in metrics collection loop: {e}")
                await asyncio.sleep(self.collection_interval)
    
    def get_metric_summary(self) -> Dict[str, Any]:
        """Get summary of all metrics for debugging."""
        return {
            "total_metrics": len(self.metrics),
            "counters": len(self.counters),
            "gauges": len(self.gauges),
            "histograms": len(self.histograms),
            "collection_interval": self.collection_interval,
            "running": self._running
        }
    
    # =================================================================================
    # QUALITY AGENT INTEGRATION METHODS
    # =================================================================================
    
    def record_quality_metrics(self, orchestrator_results: Dict[str, Any]) -> None:
        """Record metrics from Data Quality Orchestrator pipeline results."""
        try:
            # Overall pipeline metrics
            if 'pipeline_mode' in orchestrator_results:
                mode = orchestrator_results['pipeline_mode']
                self.increment_counter("quality_pipeline_executions_total", 
                                     labels={"pipeline_mode": mode, "result": "success"})
                
                if 'execution_time' in orchestrator_results:
                    self.observe_histogram("quality_pipeline_duration_seconds", 
                                         orchestrator_results['execution_time'],
                                         labels={"pipeline_mode": mode})
                
                if 'overall_score' in orchestrator_results:
                    self.set_gauge("overall_data_quality_score", 
                                 orchestrator_results['overall_score'],
                                 labels={"pipeline_mode": mode})
            
            # Individual agent metrics
            if 'agent_results' in orchestrator_results:
                for agent_name, results in orchestrator_results['agent_results'].items():
                    self._record_agent_metrics(agent_name, results)
                    
        except Exception as e:
            logger.error(f"Error recording quality metrics: {e}")
    
    def _record_agent_metrics(self, agent_name: str, results: Dict[str, Any]) -> None:
        """Record metrics for individual quality agents."""
        if agent_name == "schema_validator":
            self._record_schema_validator_metrics(results)
        elif agent_name == "leakage_police":
            self._record_leakage_police_metrics(results)
        elif agent_name == "anomaly_detector":
            self._record_anomaly_detector_metrics(results)
        elif agent_name == "freshness_agent":
            self._record_freshness_agent_metrics(results)
        elif agent_name == "reconciler_agent":
            self._record_reconciler_agent_metrics(results)
    
    def _record_schema_validator_metrics(self, results: Dict[str, Any]) -> None:
        """Record Schema Validator specific metrics."""
        table_name = results.get('table_name', 'unknown')
        venue = results.get('venue', 'unknown')
        
        # Validation count and status
        status = "pass" if results.get('is_valid', False) else "fail"
        self.increment_counter("schema_validation_total", 
                             labels={"table_name": table_name, "status": status, "venue": venue})
        
        # Validation duration
        if 'validation_time' in results:
            self.observe_histogram("schema_validation_duration_seconds", 
                                 results['validation_time'],
                                 labels={"table_name": table_name, "venue": venue})
        
        # Violations
        if 'violations' in results and results['violations']:
            for violation in results['violations']:
                self.increment_counter("schema_violations_total",
                                     labels={"table_name": table_name, 
                                           "violation_type": violation.get('type', 'unknown'),
                                           "venue": venue})
        
        # Compliance ratio
        if 'compliance_ratio' in results:
            self.set_gauge("schema_compliance_ratio", 
                         results['compliance_ratio'],
                         labels={"table_name": table_name, "venue": venue})
    
    def _record_freshness_agent_metrics(self, results: Dict[str, Any]) -> None:
        """Record Freshness Agent specific metrics."""
        stream_name = results.get('stream_name', 'unknown')
        venue = results.get('venue', 'unknown')
        
        # Data staleness
        if 'staleness_seconds' in results:
            self.set_gauge("data_staleness_seconds", 
                         results['staleness_seconds'],
                         labels={"stream_name": stream_name, "venue": venue})
        
        # SLA violations
        if 'sla_violation' in results and results['sla_violation']:
            severity = results.get('violation_severity', 'medium')
            self.increment_counter("freshness_violations_total",
                                 labels={"stream_name": stream_name, "severity": severity})
    
    def _record_leakage_police_metrics(self, results: Dict[str, Any]) -> None:
        """Record Leakage Police specific metrics."""
        # Leakage incidents
        if 'leakage_detected' in results and results['leakage_detected']:
            leakage_type = results.get('leakage_type', 'unknown')
            severity = results.get('severity', 'medium')
            source = results.get('source', 'unknown')
            
            self.increment_counter("leakage_incidents_total",
                                 labels={"leakage_type": leakage_type, "severity": severity, "source": source})
        
        # Detection score
        if 'detection_score' in results:
            feature_set = results.get('feature_set', 'default')
            self.set_gauge("leakage_detection_score", 
                         results['detection_score'],
                         labels={"feature_set": feature_set})
    
    def _record_anomaly_detector_metrics(self, results: Dict[str, Any]) -> None:
        """Record Anomaly Detector specific metrics."""
        # Anomaly detection
        if 'anomalies' in results and results['anomalies']:
            for anomaly in results['anomalies']:
                self.increment_counter("anomalies_detected_total",
                                     labels={"anomaly_type": anomaly.get('type', 'unknown'),
                                           "severity": anomaly.get('severity', 'medium'),
                                           "table": anomaly.get('table', 'unknown')})
        
        # Detection score
        if 'detection_score' in results:
            detector_type = results.get('detector_type', 'statistical')
            symbol = results.get('symbol', 'unknown')
            self.set_gauge("anomaly_detection_score", 
                         results['detection_score'],
                         labels={"detector_type": detector_type, "symbol": symbol})
    
    def _record_reconciler_agent_metrics(self, results: Dict[str, Any]) -> None:
        """Record Reconciler Agent specific metrics."""
        # Discrepancies
        if 'discrepancies' in results and results['discrepancies']:
            for discrepancy in results['discrepancies']:
                source_pair = discrepancy.get('source_pair', 'unknown')
                discrepancy_type = discrepancy.get('type', 'unknown')
                self.increment_counter("reconciliation_discrepancies_total",
                                     labels={"source_pair": source_pair, 
                                           "discrepancy_type": discrepancy_type})
        
        # Cross-source accuracy
        if 'accuracy_ratio' in results:
            source_pair = results.get('source_pair', 'unknown')
            self.set_gauge("cross_source_accuracy_ratio", 
                         results['accuracy_ratio'],
                         labels={"source_pair": source_pair})
    
    # =================================================================================
    # CRYPTO-NATIVE BUSINESS METRICS
    # =================================================================================
    
    def record_trading_metrics(self, trade_data: Dict[str, Any]) -> None:
        """Record crypto-specific trading metrics."""
        try:
            symbol = trade_data.get('symbol', 'unknown')
            venue = trade_data.get('venue', 'unknown')
            side = trade_data.get('side', 'unknown')
            
            # Basic trade metrics
            self.increment_counter("trades_executed_total", 
                                 labels={"symbol": symbol, "side": side, "venue": venue})
            
            # Execution quality metrics
            if 'slippage_bps' in trade_data:
                strategy = trade_data.get('strategy', 'unknown')
                self.observe_histogram("execution_slippage_bps", 
                                     trade_data['slippage_bps'],
                                     labels={"symbol": symbol, "venue": venue, "strategy": strategy})
            
            if 'fill_latency' in trade_data:
                order_type = trade_data.get('order_type', 'unknown')
                self.observe_histogram("order_fill_latency_seconds", 
                                     trade_data['fill_latency'],
                                     labels={"venue": venue, "order_type": order_type})
                
        except Exception as e:
            logger.error(f"Error recording trading metrics: {e}")
    
    def record_market_data_metrics(self, market_data: Dict[str, Any]) -> None:
        """Record crypto market data metrics."""
        try:
            symbol = market_data.get('symbol', 'unknown')
            venue = market_data.get('venue', 'unknown')
            
            # Market microstructure
            if 'bid_ask_spread_bps' in market_data:
                self.set_gauge("bid_ask_spread_bps", 
                             market_data['bid_ask_spread_bps'],
                             labels={"symbol": symbol, "venue": venue})
            
            if 'funding_rate' in market_data:
                self.set_gauge("funding_rate_current", 
                             market_data['funding_rate'],
                             labels={"symbol": symbol, "venue": venue})
            
            if 'basis_spread_bps' in market_data:
                expiry = market_data.get('expiry', 'perp')
                self.set_gauge("basis_spread_bps", 
                             market_data['basis_spread_bps'],
                             labels={"symbol": symbol, "expiry": expiry, "venue": venue})
                
        except Exception as e:
            logger.error(f"Error recording market data metrics: {e}")
    
    def record_onchain_metrics(self, onchain_data: Dict[str, Any]) -> None:
        """Record crypto on-chain metrics."""
        try:
            chain = onchain_data.get('chain', 'unknown')
            token = onchain_data.get('token', 'unknown')
            
            # On-chain volume
            if 'volume_24h_usd' in onchain_data:
                self.set_gauge("onchain_volume_24h_usd", 
                             onchain_data['volume_24h_usd'],
                             labels={"chain": chain, "token": token})
            
            # Bridge flows
            if 'bridge_inflows_24h' in onchain_data:
                bridge = onchain_data.get('bridge', 'unknown')
                destination = onchain_data.get('destination', 'unknown')
                self.set_gauge("bridge_inflows_24h_usd", 
                             onchain_data['bridge_inflows_24h'],
                             labels={"bridge": bridge, "destination": destination})
                
        except Exception as e:
            logger.error(f"Error recording on-chain metrics: {e}")

    # =================================================================================
    # RATE BUDGET METRICS
    # =================================================================================

    def record_rate_budget_metrics(self, rate_snapshot: Dict[str, Any]) -> None:
        """Record shared rate budget metrics from the streaming bus."""
        try:
            if not rate_snapshot:
                return

            for domain, stats in rate_snapshot.items():
                labels = {"domain": domain}
                self.set_gauge("rate_budget_available_tokens", stats.get("available_tokens", 0.0), labels)
                self.set_gauge("rate_budget_configured_qps", stats.get("configured_qps", 0.0), labels)
                self.set_gauge("rate_budget_configured_burst", stats.get("configured_burst", 0.0), labels)
                self.set_gauge("rate_budget_qps_utilization", stats.get("qps_utilization", 0.0), labels)
                self.set_gauge("rate_budget_avg_wait_seconds", stats.get("avg_wait_sec", 0.0), labels)
                self.set_gauge("rate_budget_throttled_events", stats.get("throttled_events", 0.0), labels)
                self.set_gauge("rate_budget_borrow_count", stats.get("borrow_count", 0.0), labels)
                self.set_gauge("rate_budget_rate_limit_responses", stats.get("429_count", 0.0), labels)
        except Exception as e:
            logger.error(f"Error recording rate budget metrics: {e}")

    def record_rate_limit_counters(self, component: str, metrics: Dict[str, Any], domain_keys: Optional[List[str]] = None) -> None:
        """
        Record per-component rate-limit counters reported by agents.
        Expects metrics dict containing 'rate_limit_responses' and 'rate_budget_timeouts'.
        """
        try:
            responses = metrics.get("rate_limit_responses")
            timeouts = metrics.get("rate_budget_timeouts")
            if isinstance(responses, dict):
                for domain, value in responses.items():
                    labels = {"component": component, "domain": domain}
                    self.set_gauge("rate_limit_responses_count", float(value), labels)
            elif responses is not None:
                labels = {"component": component, "domain": domain_keys[0] if domain_keys else "default"}
                self.set_gauge("rate_limit_responses_count", float(responses), labels)

            if isinstance(timeouts, dict):
                for domain, value in timeouts.items():
                    labels = {"component": component, "domain": domain}
                    self.set_gauge("rate_budget_timeouts_count", float(value), labels)
            elif timeouts is not None:
                labels = {"component": component, "domain": domain_keys[0] if domain_keys else "default"}
                self.set_gauge("rate_budget_timeouts_count", float(timeouts), labels)
        except Exception as e:
            logger.error(f"Error recording agent rate-limit counters: {e}")
    
    def record_breaker_intent_decision(self, component_id: str, intent: str,
                                       decision: str, severity: str,
                                       requested_by: str) -> None:
        """Track breaker intent decisions made by the orchestrator."""
        try:
            labels = {
                "component_id": component_id,
                "intent": intent,
                "decision": decision,
                "severity": severity,
                "requested_by": requested_by
            }
            self.increment_counter("breaker_intent_decisions_total", labels=labels)
        except Exception as exc:
            logger.error(f"Error recording breaker intent decision: {exc}")
    
    def record_breaker_state(self, state_snapshot: Dict[str, Any]) -> None:
        """Update breaker state gauges from orchestrator snapshots."""
        try:
            component_id = state_snapshot.get("component_id")
            if not component_id:
                return
            state = (state_snapshot.get("state") or "").lower()
            state_value = {"closed": 0.0, "half_open": 0.5, "open": 1.0}.get(state, -1.0)
            labels = {"component_id": component_id}
            self.set_gauge("breaker_component_state", state_value, labels)
            self.set_gauge("breaker_state_last_update_timestamp", float(time.time()), labels)
        except Exception as exc:
            logger.error(f"Error recording breaker state snapshot: {exc}")
    
    def record_quality_pipeline_metrics(self, source_topic: str, decision: str,
                                        duration_seconds: float) -> None:
        """Record metrics for quality pipeline processing."""
        try:
            labels = {"source_topic": source_topic, "decision": decision}
            self.increment_counter("quality_pipeline_messages_total", labels=labels)
            self.observe_histogram(
                "quality_pipeline_duration_seconds",
                duration_seconds,
                {"source_topic": source_topic}
            )
        except Exception as exc:
            logger.error(f"Error recording quality pipeline metrics: {exc}")
    
    def record_workload_distributor_metrics(self, workload_metrics: Dict[str, Any]) -> None:
        """
        Record WorkloadDistributor metrics for hot key detection and partition balance.
        
        Args:
            workload_metrics: Dict with keys:
                - hot_keys_detected: int (total lifetime hot keys detected)
                - skew_detections: int (total skew detections)
                - routing_decisions: Dict[str, int] (strategy -> count)
                - active_hot_keys: Dict[str, List[str]] (topic -> [key1, key2, ...])
        """
        try:
            # Record hot key detections (total)
            if "hot_keys_detected" in workload_metrics:
                self.set_gauge("workload_hot_keys_detected_total", 
                             float(workload_metrics["hot_keys_detected"]),
                             {"topic": "all"})
            
            # Record skew detections (total)
            if "skew_detections" in workload_metrics:
                self.set_gauge("workload_skew_detections_total",
                             float(workload_metrics["skew_detections"]),
                             {"topic": "all"})
            
            # Record routing decisions by strategy
            routing_decisions = workload_metrics.get("routing_decisions", {})
            for strategy, count in routing_decisions.items():
                self.set_gauge("workload_routing_decisions_total",
                             float(count),
                             {"topic": "all", "strategy": strategy})
            
            # Record active hot keys per topic
            active_hot_keys = workload_metrics.get("active_hot_keys", {})
            for topic, keys in active_hot_keys.items():
                self.set_gauge("workload_active_hot_keys",
                             float(len(keys)),
                             {"topic": topic})
                
        except Exception as exc:
            logger.error(f"Error recording workload distributor metrics: {exc}")

# =============================
# CONVENIENCE DECORATORS
# =============================

# Global metrics collector instance
_global_collector: Optional[MetricsCollector] = None

def get_metrics_collector() -> MetricsCollector:
    """Get or create global metrics collector."""
    global _global_collector
    if _global_collector is None:
        _global_collector = MetricsCollector()
    return _global_collector

def timed(metric_name: str = "function_duration_seconds"):
    """Convenience decorator for timing functions."""
    collector = get_metrics_collector()
    return collector.time_function(metric_name)

def count_calls(metric_name: str = "function_calls_total"):
    """Convenience decorator for counting function calls."""
    def decorator(func: Callable) -> Callable:
        collector = get_metrics_collector()
        
        if asyncio.iscoroutinefunction(func):
            async def async_wrapper(*args, **kwargs):
                collector.increment_counter(metric_name, labels={"function": func.__name__})
                return await func(*args, **kwargs)
            return async_wrapper
        else:
            def sync_wrapper(*args, **kwargs):
                collector.increment_counter(metric_name, labels={"function": func.__name__})
                return func(*args, **kwargs)
            return sync_wrapper
    return decorator

# =============================
# EXAMPLE USAGE
# =============================

if __name__ == "__main__":
    import asyncio
    
    async def example_usage():
        collector = MetricsCollector(collection_interval=0.1)
        await collector.start_collection()
        
        # Simulate some metrics
        for i in range(10):
            collector.increment_counter("test_counter", 1.0)
            collector.set_gauge("test_gauge", i * 10)
            collector.observe_histogram("test_histogram", i * 0.001)
            await asyncio.sleep(0.1)
        
        # Generate output
        print("Prometheus Output:")
        print(collector.generate_prometheus_output())
        
        await collector.stop_collection()
    
    asyncio.run(example_usage())
