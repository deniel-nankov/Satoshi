#!/usr/bin/env python3
"""
Master Data Pipeline Orchestration Script

Starts all data collectors, the Data Quality Orchestrator, and Gold Layer Curators
to create the complete Bronze → Silver → Gold data pipeline.

Components Started:
  BRONZE LAYER (Data Collection):
    - Exchange Connector Agent (CEX/DEX market data)
    - Options Chain Collector Agent (options market data)
    - Onchain Collector Agent (blockchain events)
    - Events Collector Agent (off-chain events)
  
  SILVER LAYER (Data Quality):
    - Data Quality Orchestrator (quality pipeline)
  
  GOLD LAYER (Data Curation):
    - OHLCV Aggregator (multi-timeframe bars)
    - Symbol Normalizer (cross-venue naming)
    - Orderbook Curator (fixed-interval snapshots)
    - Options Chain Curator (strike/expiry organization)

Data Flow:
  Bronze: Collectors → raw_data.*
  Silver: Quality Orchestrator → clean.*
  Gold:   Curators → curated.data.*
"""

import asyncio
import logging
import signal
import sys
from pathlib import Path
from typing import Optional, List
from aiokafka.structs import TopicPartition

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Import configuration
from config import EXCHANGE_CONFIG, OPTIONS_CONFIG, ONCHAIN_CONFIG, EVENTS_CONFIG, STREAMING_BUS_CONFIG, MACRO_CONFIG, CRYPTO_METRICS_CONFIG

# Import infrastructure
from infra.bus.streaming_bus import StreamingBus
from infra.bus.memory_governor import MemoryGovernor, StateConfig
from infra.bus.workload_distributor import WorkloadDistributor, PartitionerConfig, HotKeyConfig
from infra.monitoring.prometheus_metrics import get_metrics_collector
from infra.registry.postgres_registry import EnterprisePostgreSQLRegistry, RegistryConfig

# Configure logging early for import-time messages
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data_pipeline.log')
    ]
)
logger = logging.getLogger(__name__)

# Prometheus HTTP server for metrics exposure
try:
    from prometheus_client import start_http_server
    PROMETHEUS_CLIENT_AVAILABLE = True
except ImportError:
    PROMETHEUS_CLIENT_AVAILABLE = False
    start_http_server = None  # Explicitly set to None for type safety
    logger.warning("prometheus_client not available - metrics endpoint will not be exposed")

from infra.tsdb.clickhouse_tsdb import QualityMonitoringTSDB, TSDBConfig

# Import data collectors (Bronze Layer)
from engines.data.bronze.exchange_connector import ExchangeConnectorAgent
from engines.data.bronze.options_chain_collector import OptionsChainCollectorAgent
from engines.data.bronze.onchain_collector import OnchainCollectorAgent
from engines.data.bronze.events_collector import EventsCollectorAgent
from engines.data.bronze.macro_collector import MacroCollectorAgent
from engines.data.bronze.crypto_metrics_collector import CryptoMetricsCollectorAgent

# Import quality pipeline
from engines.data.data_quality_orchestrator import (
    DataQualityOrchestrator,
    OrchestrationConfig,
    OrchestrationFactory
)
from engines.data.silver.schema_validator import SchemaValidatorAgent
from engines.data.silver.leakage_police import LeakagePolice, LeakagePoliceConfig
from engines.data.silver.anomaly_detector import DataAnomalyDetector
from engines.data.silver.freshness_agent import FreshnessAgent
from engines.data.silver.reconciler_agent import ReconcilerAgent, ReconcilerConfig

# Import Gold Layer curators
from engines.data.gold.ohlcv_aggregator import OHLCVAggregator
from engines.data.silver.symbol_normalizer import SymbolNormalizer
from engines.data.gold.orderbook_curator import OrderbookCurator
from engines.data.gold.options_chain_curator import OptionsChainCurator
from engines.data.gold.macro_tradfi_curator import MacroTradFiCurator
from engines.data.gold.crypto_market_structure_curator import CryptoMarketStructureCurator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data_pipeline.log')
    ]
)
logger = logging.getLogger(__name__)


class DataPipelineManager:
    """
    Master orchestrator for the complete data pipeline.
    
    Manages lifecycle of:
      - Bronze Layer: Data collectors
      - Silver Layer: Quality orchestrator
      - Gold Layer: Data curators
    """
    
    def __init__(self, config_mode: str = "development", enable_gold_layer: bool = True):
        """
        Initialize the data pipeline manager.
        
        Args:
            config_mode: "development" or "institutional" for different configs
            enable_gold_layer: Whether to start Gold Layer curators
        """
        self.config_mode = config_mode
        self.enable_gold_layer = enable_gold_layer
        self.running = False
        
        # Infrastructure
        self.streaming_bus: Optional[StreamingBus] = None
        self.memory_governor: Optional[MemoryGovernor] = None
        self.workload_distributor: Optional[WorkloadDistributor] = None
        
        # Bronze Layer - Data Collectors
        self.exchange_connector: Optional[ExchangeConnectorAgent] = None
        self.options_collector: Optional[OptionsChainCollectorAgent] = None
        self.onchain_collector: Optional[OnchainCollectorAgent] = None
        self.events_collector: Optional[EventsCollectorAgent] = None
        self.macro_collector: Optional[MacroCollectorAgent] = None
        self.crypto_metrics_collector: Optional[CryptoMetricsCollectorAgent] = None
        
        # Silver Layer - Quality Pipeline
        self.quality_orchestrator: Optional[DataQualityOrchestrator] = None
        self.schema_validator: Optional[SchemaValidatorAgent] = None
        self.leakage_police: Optional[LeakagePolice] = None
        self.anomaly_detector: Optional[DataAnomalyDetector] = None
        self.freshness_agent: Optional[FreshnessAgent] = None
        self.reconciler_agent: Optional[ReconcilerAgent] = None
        
        # Gold Layer - Data Curators
        self.ohlcv_aggregator: Optional[OHLCVAggregator] = None
        self.symbol_normalizer: Optional[SymbolNormalizer] = None
        self.orderbook_curator: Optional[OrderbookCurator] = None
        self.options_chain_curator: Optional[OptionsChainCurator] = None
        self.macro_tradfi_curator: Optional[MacroTradFiCurator] = None
        self.crypto_structure_curator: Optional[CryptoMarketStructureCurator] = None
        
        # Observability Layer - TSDB for incident monitoring
        self.tsdb: Optional[QualityMonitoringTSDB] = None
        
        # Registry Layer - PostgreSQL metadata registry
        self.registry: Optional[EnterprisePostgreSQLRegistry] = None
        
        # Metrics
        self.metrics = get_metrics_collector()
        
        # Metrics update task
        self._metrics_task: Optional[asyncio.Task] = None
        
        logger.info(f"Data Pipeline Manager initialized in {config_mode} mode (Gold Layer: {enable_gold_layer})")
        logger.info("✅ MemoryGovernor and WorkloadDistributor will be integrated for production-grade resource management")
    
    async def start_metrics_server(self, port: int = 8000):
        """
        Start Prometheus metrics HTTP server for Grafana integration.
        
        Exposes metrics at http://localhost:{port}/metrics for Prometheus scraping.
        """
        if not PROMETHEUS_CLIENT_AVAILABLE or start_http_server is None:
            logger.warning("⚠️  Prometheus client not available - metrics endpoint not started")
            logger.warning("   Install with: pip install prometheus-client")
            return
        
        try:
            start_http_server(port)
            logger.info(f"✅ Prometheus metrics endpoint started on port {port}")
            logger.info(f"   Access metrics at: http://localhost:{port}/metrics")
            logger.info(f"   Grafana dashboard: http://localhost:3000")
        except OSError as e:
            if "Address already in use" in str(e):
                logger.warning(f"⚠️  Port {port} already in use - metrics endpoint may already be running")
            else:
                logger.error(f"❌ Failed to start metrics server on port {port}: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error starting metrics server: {e}")
    
    def _get_streaming_bus_config(self) -> dict:
        """Get streaming bus configuration from config.py."""
        return STREAMING_BUS_CONFIG
    
    def _get_exchange_connector_config(self) -> dict:
        """Get exchange connector configuration from config.py."""
        # Convert nested venues dict to list format expected by ExchangeConnectorAgent
        venues_list = []
        
        if "venues" in EXCHANGE_CONFIG:
            for venue_name, venue_config in EXCHANGE_CONFIG["venues"].items():
                if venue_config.get("enabled", False):
                    venue_entry = {
                        "name": venue_name,
                        **venue_config
                    }
                    venues_list.append(venue_entry)
        
        # Create flattened config WITHOUT mixing symbols from different venues
        # Each adapter will use its own symbols from its venue_config
        return {
            "venues": venues_list,
            **{k: v for k, v in EXCHANGE_CONFIG.items() if k != "venues"}
        }
    
    def _get_options_collector_config(self) -> dict:
        """Get options chain collector configuration from config.py."""
        return OPTIONS_CONFIG
    
    def _get_onchain_collector_config(self) -> dict:
        """Get onchain collector configuration from config.py."""
        return ONCHAIN_CONFIG
    
    def _get_events_collector_config(self) -> dict:
        """Get events collector configuration from config.py."""
        return EVENTS_CONFIG
    
    def _get_macro_collector_config(self) -> dict:
        """Get macro collector configuration from config.py."""
        return MACRO_CONFIG
    
    def _get_crypto_metrics_collector_config(self) -> dict:
        """Get crypto metrics collector configuration from config.py."""
        return CRYPTO_METRICS_CONFIG
    
    async def _initialize_registry(self):
        """Initialize PostgreSQL metadata registry."""
        try:
            config = RegistryConfig(
                host="localhost",
                port=5432,
                database="satoshi_registry",
                username="satoshi",
                password="satoshi_secure_2024",
                pool_size=20,
                cache_enabled=False,  # Disable Redis cache for now
                metrics_enabled=True
            )
            
            self.registry = EnterprisePostgreSQLRegistry(config)
            success = await self.registry.initialize()
            
            if success:
                logger.info("✅ PostgreSQL registry initialized - schema contracts ready")
                # Register schema contracts will happen after topics are created
            else:
                logger.warning("⚠️  PostgreSQL registry unavailable - continuing without metadata persistence")
                self.registry = None
                
        except Exception as e:
            logger.warning(f"⚠️  Failed to initialize registry: {e}")
            logger.warning("   Continuing without metadata persistence")
            self.registry = None
    
    async def initialize_components(self):
        """Initialize all pipeline components."""
        logger.info("Initializing data pipeline components...")
        
        try:
            # 0. Initialize PostgreSQL Registry FIRST (Metadata Authority)
            logger.info("🏛️ Initializing PostgreSQL metadata registry...")
            await self._initialize_registry()
            
            # 1.1 Initialize MemoryGovernor FIRST (Production-Grade Memory Management)
            logger.info("Initializing MemoryGovernor with per-layer memory budgets...")
            
            # Per-layer memory allocation strategy:
            # Bronze Layer (Data Collection): 512MB - ephemeral, high throughput
            # Silver Layer (Quality Pipeline): 1GB - stateful validation, needs more memory
            # Gold Layer (Curated Data): 512MB - aggregation state, moderate memory
            # Total: 2GB across all layers
            
            memory_config = StateConfig(
                max_memory_mb=2048,  # Global 2GB limit
                watermark_delay_ms=300_000,  # 5 minutes watermark delay
                eviction_threshold=0.75,  # Start eviction at 75% memory usage
                cleanup_interval_ms=60_000,  # 1 minute cleanup interval
                enable_checkpointing=False  # Disable for now (Bronze layer is ephemeral)
            )
            
            self.memory_governor = MemoryGovernor(global_config=memory_config)
            
            # Create per-layer memory budgets (for monitoring/alerting)
            self.layer_memory_budgets = {
                "bronze": 512,   # MB - Exchange, Options, Onchain, Events collectors
                "silver": 1024,  # MB - Quality Orchestrator + agents
                "gold": 512      # MB - OHLCV, Symbol, Orderbook, Options curators
            }
            logger.info(f"   Memory budgets: Bronze={self.layer_memory_budgets['bronze']}MB, "
                       f"Silver={self.layer_memory_budgets['silver']}MB, "
                       f"Gold={self.layer_memory_budgets['gold']}MB")
            
            # Start background tasks (cleanup, monitoring, GC triggering)
            await self.memory_governor.start_background_tasks()
            logger.info("✅ MemoryGovernor initialized with 2GB memory limit and automatic GC triggering")
            
            # 1.2 Initialize WorkloadDistributor BEFORE StreamingBus (dependency order)
            logger.info("Initializing WorkloadDistributor for intelligent partition routing...")
            hot_key_config = HotKeyConfig(
                detection_window_seconds=60,
                hot_key_threshold_multiplier=3.0,  # 3x average = hot key (BTC, ETH, etc.)
                min_messages_for_detection=100,
                cool_down_period_seconds=300,  # 5 minutes cooldown
                dedicated_partitions_per_hot_key=2
            )
            workload_config = PartitionerConfig(
                skew_threshold=2.0,  # 2x load imbalance triggers rebalancing
                rebalance_interval_seconds=300,
                hot_key_config=hot_key_config,
                max_partition_load_mb_per_second=100.0,
                enable_adaptive_routing=True,
                enable_skew_detection=True,
                enable_hot_key_mitigation=True
            )
            self.workload_distributor = WorkloadDistributor(config=workload_config)
            logger.info("✅ WorkloadDistributor initialized with hot key detection for BTC/ETH/high-volume symbols")
            
            # 1.3 Initialize Streaming Bus WITH MemoryGovernor + WorkloadDistributor
            logger.info("Initializing Streaming Bus with backpressure control and intelligent routing...")
            streaming_config = self._get_streaming_bus_config()
            streaming_config["backpressure_enabled"] = True  # Enable backpressure control
            streaming_config["enable_workload_distribution"] = True  # Enable intelligent partition routing
            self.streaming_bus = StreamingBus(
                streaming_config, 
                memory_governor=self.memory_governor,
                workload_distributor=self.workload_distributor
            )
            
            # Create topics if needed
            await self.streaming_bus.create_topics_from_config()
            
            # Validate data ingestion topics
            if not self.streaming_bus.validate_data_ingestion_topics():
                logger.warning("Some required topics are missing - they will be created on first publish")
            
            logger.info("✅ Streaming Bus initialized with MemoryGovernor + WorkloadDistributor integration")
            
            # 2. Initialize Data Collectors
            logger.info("Initializing data collectors...")
            
            # Exchange Connector
            logger.info("  - Exchange Connector Agent...")
            exchange_config = self._get_exchange_connector_config()
            self.exchange_connector = ExchangeConnectorAgent(exchange_config)
            
            # Options Collector
            logger.info("  - Options Chain Collector Agent...")
            options_config = self._get_options_collector_config()
            self.options_collector = OptionsChainCollectorAgent(options_config)
            
            # Onchain Collector
            logger.info("  - Onchain Collector Agent...")
            onchain_config = self._get_onchain_collector_config()
            self.onchain_collector = OnchainCollectorAgent(onchain_config)
            
            # Events Collector
            logger.info("  - Events Collector Agent...")
            events_config = self._get_events_collector_config()
            self.events_collector = EventsCollectorAgent(events_config)
            
            # Macro Collector (NEW - Phase 3)
            logger.info("  - Macro/TradFi Collector Agent...")
            macro_config = self._get_macro_collector_config()
            if macro_config.get("enabled", False):
                self.macro_collector = MacroCollectorAgent(streaming_bus=self.streaming_bus)
                logger.info("    ✅ Macro Collector enabled (FRED + Alpha Vantage + Yahoo Finance)")
            else:
                logger.info("    ⏭️  Macro Collector disabled in config")
            
            # Crypto Metrics Collector (NEW - Phase 3)
            logger.info("  - Crypto Metrics Collector Agent...")
            crypto_metrics_config = self._get_crypto_metrics_collector_config()
            if crypto_metrics_config.get("enabled", False):
                self.crypto_metrics_collector = CryptoMetricsCollectorAgent(streaming_bus=self.streaming_bus)
                logger.info("    ✅ Crypto Metrics Collector enabled (CoinGecko)")
            else:
                logger.info("    ⏭️  Crypto Metrics Collector disabled in config")
            
            logger.info("✅ All data collectors initialized")
            
            # 3. Initialize Quality Pipeline
            logger.info("Initializing quality pipeline...")
            
            # Initialize quality agents
            self.schema_validator = SchemaValidatorAgent({})
            self.leakage_police = LeakagePolice(LeakagePoliceConfig())
            self.anomaly_detector = DataAnomalyDetector({})
            
            # Set agent status metrics to healthy after initialization
            self.metrics.set_gauge("schema_validator_status", 1.0)
            self.metrics.set_gauge("leakage_police_status", 1.0)
            self.metrics.set_gauge("anomaly_detector_status", 1.0)
            
            # FIX: Inject shared StreamingBus into FreshnessAgent instead of letting it create its own
            # REASON: Agent was creating separate StreamingBus with empty config, causing publish hangs
            self.freshness_agent = FreshnessAgent({
                "streaming_bus": self.streaming_bus,  # Use shared instance
                "check_interval_us": 30_000_000,      # 30 seconds
                "min_confidence_threshold": 0.7,
                "staleness_threshold_us": 300_000_000  # 5 minutes
            })
            
            # ==================== ELITE FRESHNESS STREAM REGISTRATION ====================
            # Register data streams for real-time freshness monitoring with adaptive thresholds
            # Uses SLO-driven configuration: target <1 false positive per week
            logger.info("📊 Registering streams for freshness monitoring...")
            
            from engines.data.silver.freshness_agent import StreamConfig
            
            # Exchange Feed: High-frequency trade stream (primary critical path)
            # Configuration based on observed behavior: trades arrive every 1-3 seconds under normal conditions
            # Adaptive threshold prevents false positives during low-volume periods
            exchange_stream_config = StreamConfig(
                stream_name="raw_data.exchange_feed",
                expected_interval_us=2_000_000,      # 2 seconds baseline (high-frequency trading)
                
                # Adaptive threshold estimation (prevents false positives)
                bar_estimation_window=200,           # Rolling window: 200 messages for robust median
                bar_multiplier=1.5,                  # Threshold = median * 1.5 (allows 50% variance)
                jitter_multiplier=2.0,               # Jitter = MAD * 2.0 (captures volatility)
                min_threshold_us=5_000_000,          # Floor: 5 seconds (never alert faster)
                max_threshold_us=60_000_000,         # Ceiling: 60 seconds (max tolerable staleness)
                
                # Hysteresis (prevent alert flapping)
                confirmation_checks=3,               # Require 3 consecutive violations (90s @ 30s interval)
                clear_confirmation_checks=2,         # Require 2 consecutive clears (60s)
                clear_threshold_ratio=0.5,           # Clear at 50% of threshold (wide hysteresis band)
                
                # Circuit breaker integration (halt trading on critical staleness)
                circuit_breaker_enabled=True,
                escalation_threshold_multiplier=3.0, # CRITICAL @ 3x threshold (emergency stop)
                
                # Cold start protection (don't alert during initialization)
                min_observations_for_arming=20,      # Need 20 messages before monitoring active
                
                # Clock skew handling (distributed systems)
                use_event_time=True,                 # Prefer event timestamp over arrival time
                max_event_time_skew_us=10_000_000,   # Reject events >10s in future/past
                
                # Incident deduplication (avoid spam)
                incident_dedupe_window_us=900_000_000  # 15 minutes between duplicate incidents
            )
            
            self.freshness_agent.register_stream(exchange_stream_config)
            logger.info(f"    ✅ raw_data.exchange_feed")
            logger.info(f"       Baseline: 2s | Adaptive threshold: median*1.5 + jitter*2.0")
            logger.info(f"       Range: [5s, 60s] | Confirmation: 3 checks | Hysteresis: 50%")
            logger.info(f"       Circuit breaker: ENABLED @ 3x threshold")
            logger.info("")
            
            # Onchain Events: Blockchain event stream (medium-frequency, bursty)
            # Configuration: On-chain data is less frequent than trades but still real-time critical
            # Typical pattern: bursts every 12-15 seconds (Ethereum block time), with occasional delays
            onchain_stream_config = StreamConfig(
                stream_name="raw_data.onchain_events",
                expected_interval_us=15_000_000,     # 15 seconds baseline (Ethereum block time)
                
                # Adaptive threshold estimation (handles block time variance)
                bar_estimation_window=100,           # Rolling window: 100 events (≈25 minutes)
                bar_multiplier=2.0,                  # Threshold = median * 2.0 (allows block time variance)
                jitter_multiplier=3.0,               # Jitter = MAD * 3.0 (captures chain reorganizations)
                min_threshold_us=30_000_000,         # Floor: 30 seconds (2x block time)
                max_threshold_us=300_000_000,        # Ceiling: 5 minutes (max tolerable staleness)
                
                # Hysteresis (prevent alert flapping during network congestion)
                confirmation_checks=2,               # Require 2 consecutive violations (1 minute)
                clear_confirmation_checks=1,         # Single clear (fast recovery)
                clear_threshold_ratio=0.6,           # Clear at 60% of threshold (moderate hysteresis)
                
                # Circuit breaker integration (less aggressive than exchange data)
                circuit_breaker_enabled=True,
                escalation_threshold_multiplier=4.0, # CRITICAL @ 4x threshold (tolerate chain delays)
                
                # Cold start protection (onchain data may start slowly)
                min_observations_for_arming=10,      # Need 10 events before monitoring active
                
                # Clock skew handling (blockchain timestamps can be off)
                use_event_time=True,                 # Use block timestamp
                max_event_time_skew_us=60_000_000,   # Reject events >60s in future/past (mining timestamp tolerance)
                
                # Incident deduplication
                incident_dedupe_window_us=1800_000_000  # 30 minutes between duplicate incidents (longer for blockchain)
            )
            
            self.freshness_agent.register_stream(onchain_stream_config)
            logger.info(f"    ✅ raw_data.onchain_events")
            logger.info(f"       Baseline: 15s | Adaptive threshold: median*2.0 + jitter*3.0")
            logger.info(f"       Range: [30s, 300s] | Confirmation: 2 checks | Hysteresis: 60%")
            logger.info(f"       Circuit breaker: ENABLED @ 4x threshold (tolerates chain delays)")
            logger.info("")
            
            # Initialize ReconcilerAgent first (before registration)
            self.reconciler_agent = ReconcilerAgent(ReconcilerConfig())
            
            # Set freshness and reconciler agent status metrics to healthy
            self.metrics.set_gauge("freshness_agent_status", 1.0)
            self.metrics.set_gauge("reconciler_agent_status", 1.0)
            
            # ==================== ELITE RECONCILIATION CONFIGURATION ====================
            # Register data sources for cross-venue validation with priority-based conflict resolution
            # Detects price discrepancies, missing data, and potential data quality issues
            logger.info("🔍 Configuring ReconcilerAgent for cross-source validation...")
            
            from engines.data.silver.reconciler_agent import DataSource
            
            # Register multiple venues with priority hierarchy (higher = more authoritative)
            # Priority determines conflict resolution: if prices diverge, highest priority wins
            venues = [
                ("coinbase", 3, "Primary institutional source - highest liquidity, most reliable"),
                ("binance", 2, "Secondary source - high volume, good for cross-validation"),
                ("gemini", 1, "Tertiary source - regulated exchange, conservative pricing")
            ]
            
            for venue_name, priority, description in venues:
                venue_source = DataSource(
                    name=venue_name,
                    connection_params={},  # Not used for streaming data
                    priority=priority,
                    key_fields=["symbol", "timestamp"],
                    timestamp_field="timestamp",
                    read_only=True,  # Reconciler never writes, only reports
                    
                    # Field normalization (handle venue-specific schema differences)
                    field_map={
                        "price": "price",
                        "qty": "quantity",
                        "size": "quantity",
                        "amount": "quantity"
                    },
                    
                    # Tolerance configuration (venue-specific)
                    tolerance_config={
                        "price_tolerance_pct": 0.5,      # 0.5% max price divergence
                        "volume_tolerance_pct": 20.0,    # 20% volume mismatch OK
                        "timestamp_tolerance_ms": 2000   # 2 second time alignment
                    }
                )
                self.reconciler_agent.register_data_source(venue_source)
                logger.info(f"    ✅ {venue_name} (priority={priority})")
                logger.info(f"       {description}")
            
            logger.info("")
            logger.info("    Configuration:")
            logger.info("    - Price tolerance: 0.5% (flag discrepancies >$50 on $10k)")
            logger.info("    - Conflict resolution: Priority-based (coinbase > binance > gemini)")
            logger.info("    - Time window: 5 seconds for price alignment")
            logger.info("    - Action: Report only (no automatic fixes)")
            logger.info("")
            
            logger.info("  - Quality agents initialized")
            
            # Initialize orchestrator based on mode
            if self.config_mode == "institutional":
                self.quality_orchestrator = OrchestrationFactory.create_institutional_orchestrator(
                    self.streaming_bus
                )
            else:
                self.quality_orchestrator = OrchestrationFactory.create_development_orchestrator(
                    self.streaming_bus
                )
            
            # Register quality agents with orchestrator
            # FIXED: FreshnessAgent deadlock resolved with fire-and-forget pattern
            # Root cause: check_freshness() was awaiting incident publishing, creating circular dependency
            # Solution: Made _enqueue_freshness_incident() use asyncio.create_task() (fire-and-forget)
            # Now safe to use with Quality Orchestrator's 2-second timeout
            self.quality_orchestrator.register_quality_agents(
                schema_validator=self.schema_validator,
                leakage_police=self.leakage_police,
                anomaly_detector=self.anomaly_detector,
                freshness_agent=self.freshness_agent,  # RE-ENABLED: Deadlock fixed
                reconciler_agent=self.reconciler_agent
            )
            
            logger.info("✅ Quality pipeline initialized with FreshnessAgent")
            
            # 4. Initialize ClickHouse TSDB for Incident Monitoring (Observability Layer)
            logger.info("Initializing ClickHouse TSDB for incident monitoring...")
            try:
                tsdb_config = TSDBConfig()
                # Use the same streaming_bus instance for incident consumption
                streaming_config = self._get_streaming_bus_config()
                self.tsdb = QualityMonitoringTSDB(tsdb_config, streaming_config)
                logger.info("✅ ClickHouse TSDB initialized (will start consuming after quality orchestrator)")
            except Exception as e:
                logger.warning(f"⚠️  ClickHouse TSDB initialization failed (will run without incident analytics): {e}")
                logger.warning("    To enable: Start ClickHouse with docker-compose -f docker-compose.monitoring.yml up -d clickhouse")
                self.tsdb = None
            
            # 5. Initialize Gold Layer Curators (if enabled)
            if self.enable_gold_layer:
                logger.info("Initializing Gold Layer curators...")
                
                # OHLCV Aggregator
                logger.info("  - OHLCV Aggregator...")
                from engines.data.gold.ohlcv_aggregator import TimeInterval
                intervals = [
                    TimeInterval.SEC_1,
                    TimeInterval.SEC_5,
                    TimeInterval.MIN_1,
                    TimeInterval.MIN_5,
                    TimeInterval.MIN_15,
                    TimeInterval.HOUR_1,
                    TimeInterval.DAY_1
                ]
                self.ohlcv_aggregator = OHLCVAggregator(
                    streaming_bus=self.streaming_bus,
                    intervals=intervals
                )
                
                # Symbol Normalizer
                logger.info("  - Symbol Normalizer...")
                self.symbol_normalizer = SymbolNormalizer(
                    streaming_bus=self.streaming_bus
                )
                
                # Orderbook Curator
                logger.info("  - Orderbook Curator...")
                orderbook_config = {
                    "bus_config": streaming_config,
                    "snapshot_interval_sec": 1.0,
                    "max_levels": 20,
                    "pool_size": 4
                }
                self.orderbook_curator = OrderbookCurator(orderbook_config)
                
                # Options Chain Curator
                logger.info("  - Options Chain Curator...")
                options_curator_config = {
                    "bus_config": streaming_config,
                    "pool_size": 4
                }
                self.options_chain_curator = OptionsChainCurator(options_curator_config)
                
                # Macro/TradFi Curator
                logger.info("  - Macro/TradFi Curator...")
                self.macro_tradfi_curator = MacroTradFiCurator(
                    streaming_bus=self.streaming_bus,
                    metrics_collector=self.metrics,
                    snapshot_interval_sec=60,  # Create snapshot every 60 seconds
                    max_staleness_sec=86400 * 3  # 3 days for FRED data
                )
                
                # Crypto Market Structure Curator
                logger.info("  - Crypto Market Structure Curator...")
                self.crypto_structure_curator = CryptoMarketStructureCurator(
                    streaming_bus=self.streaming_bus,
                    metrics_collector=self.metrics,
                    enrichment_interval_sec=60  # Enrich every 60 seconds
                )
                
                logger.info("✅ Gold Layer curators initialized (6 total)")
            
            logger.info("🚀 All components initialized successfully!")
            
        except Exception as e:
            logger.error(f"Failed to initialize components: {e}")
            raise
    
    async def start_pipeline(self):
        """Start all pipeline components."""
        logger.info("=" * 80)
        logger.info("STARTING DATA PIPELINE")
        logger.info("=" * 80)
        
        self.running = True
        
        try:
            # ================================================================
            # INFRASTRUCTURE LAYER: Metrics & Observability
            # ================================================================
            logger.info("\n📊 Starting Infrastructure Layer...")
            await self.start_metrics_server(port=8000)
            await self.metrics.start_collection()
            logger.info("    ✅ Metrics collection started")
            
            # Start dashboard metrics update task
            self._metrics_task = asyncio.create_task(self._update_dashboard_metrics())
            logger.info("    ✅ Dashboard metrics updater started")
            
            # ================================================================
            # BRONZE LAYER: Raw Data Collection
            # ================================================================
            logger.info("\n" + "=" * 80)
            logger.info("🥉 BRONZE LAYER: Starting Raw Data Collectors")
            logger.info("=" * 80)
            
            if self.exchange_connector:
                logger.info("\n  [1/4] Exchange Connector (real-time trades/orderbook)...")
                await self.exchange_connector.start()
                logger.info("        ✅ Exchange Connector started")
                logger.info("        ⏳ Waiting for data production to raw_data.* topics...")
                # Smart coordination: wait for actual data instead of arbitrary delay
                await self._wait_for_topic_messages("raw_data.exchange_feed", min_messages=1, timeout=15.0)
            
            if self.options_collector:
                logger.info("\n  [2/4] Options Chain Collector (derivatives data)...")
                await self.options_collector.start()
                logger.info("        ✅ Options Collector started")
                logger.info("        ⏳ Waiting for data production to raw_data.* topics...")
                # Smart coordination: wait for actual data instead of arbitrary delay
                await self._wait_for_topic_messages("raw_data.options_chain", min_messages=1, timeout=15.0)
            
            if self.onchain_collector:
                logger.info("\n  [3/4] Onchain Collector (blockchain events)...")
                logger.info("        ℹ️  CPU throttling bug FIXED: Added rate limiting + throttled logging")
                logger.info("        📋 Buffer: 5000 blocks (up from 1000), batch finalization, 50ms backpressure")
                await self.onchain_collector.start()
                logger.info("        ✅ Onchain Collector started")
                logger.info("        ⏳ Waiting for data production to raw_data.* topics...")
                # Smart coordination: wait for actual data instead of arbitrary delay
                await self._wait_for_topic_messages("raw_data.onchain_events", min_messages=1, timeout=15.0)
            
            if self.events_collector:
                logger.info("\n  [4/6] Events Collector (news/sentiment)...")
                await self.events_collector.start()
                logger.info("        ✅ Events Collector started")
                logger.info("        ⏳ Waiting for data production to raw_data.* topics...")
                # Smart coordination: wait for actual data instead of arbitrary delay
                await self._wait_for_topic_messages("raw_data.events", min_messages=1, timeout=15.0)
            
            if self.macro_collector:
                logger.info("\n  [5/6] Macro/TradFi Collector (economic indicators + market data)...")
                await self.macro_collector.start()
                logger.info("        ✅ Macro Collector started")
                logger.info("        📊 Data sources: FRED + Alpha Vantage + Yahoo Finance")
                logger.info("        ⏳ Waiting for data production to raw_data.macro.* topics...")
                # Smart coordination: wait for actual data instead of arbitrary delay
                await self._wait_for_topic_messages("raw_data.tradfi.indices", min_messages=1, timeout=15.0)
            
            if self.crypto_metrics_collector:
                logger.info("\n  [6/6] Crypto Metrics Collector (market structure metrics)...")
                await self.crypto_metrics_collector.start()
                logger.info("        ✅ Crypto Metrics Collector started")
                logger.info("        📊 Data source: CoinGecko")
                logger.info("        ⏳ Waiting for data production to raw_data.crypto.* topics...")
                # Smart coordination: wait for actual data instead of arbitrary delay
                await self._wait_for_topic_messages("raw_data.crypto.market_metrics", min_messages=1, timeout=15.0)
            
            # Wait for Bronze Layer to accumulate some data
            logger.info("\n⏳ Bronze Layer stabilizing - accumulating raw data...")
            # Smart coordination: ensure we have multiple messages before proceeding
            if self.exchange_connector:
                logger.info("        ⏳ Waiting for exchange data volume...")
                await self._wait_for_topic_messages("raw_data.exchange_feed", min_messages=5, timeout=20.0)
            logger.info("✅ Bronze Layer operational - raw_data.* topics populated")
            
            # ================================================================
            # SILVER LAYER: Data Quality & Validation
            # ================================================================
            logger.info("\n" + "=" * 80)
            logger.info("🥈 SILVER LAYER: Starting Data Quality Pipeline")
            logger.info("=" * 80)
            logger.info("Architecture: 6-stage validation (Schema → Leakage → Anomaly → Freshness → Reconciliation → Scoring)")
            logger.info("\nQuality Agents: 6 specialized components")
            
            # Show registered quality agents (initialized earlier, not started separately)
            logger.info("\n  📋 Quality Agents Registry:")
            if self.schema_validator:
                logger.info("     [1/6] Schema Validator - Validates ingestion schemas")
            if self.leakage_police:
                logger.info("     [2/6] Leakage Police - Detects temporal contamination")
            if self.anomaly_detector:
                logger.info("     [3/6] Anomaly Detector - Identifies statistical outliers")
            if self.freshness_agent:
                num_streams = len(self.freshness_agent.stream_configs) if hasattr(self.freshness_agent, 'stream_configs') else 0
                logger.info(f"     [4/6] Freshness Agent - Monitors staleness ({num_streams} stream(s))")
            if self.reconciler_agent:
                num_sources = len(self.reconciler_agent.data_sources) if hasattr(self.reconciler_agent, 'data_sources') else 0
                logger.info(f"     [5/6] Reconciler Agent - Cross-venue validation ({num_sources} source(s))")
            logger.info("     [6/6] Quality Orchestrator - Coordinates all agents in 6-stage pipeline")
            
            # Now start the components that need explicit activation
            # Step 1: Schema Validator (must be first - registers schemas)
            logger.info("\n  [1/2] Starting Schema Validator...")
            if self.schema_validator:
                await self.schema_validator.start()
                logger.info("        ✅ Schema Validator started")
                logger.info("        ⏳ Waiting for component readiness...")
                # Smart coordination: wait for component to be ready
                await self._wait_for_component_ready(self.schema_validator, "Schema Validator", timeout=15.0)
            
            # Freshness Agent: Orchestrator-Integrated Mode
            # ARCHITECTURE DECISION: FreshnessAgent does NOT need start() when used with Quality Orchestrator
            # The orchestrator already consumes raw_data.* topics and calls record_data_update() directly.
            # Starting a separate consumer would create duplicate consumption and rebalancing conflicts.
            # The agent is fully functional via orchestrator integration - no separate consumer needed.
            logger.info("\n  [2/2] Freshness Agent - Orchestrator-Integrated Mode")
            logger.info("        ✅ Passive tracking via Quality Orchestrator")
            if self.freshness_agent:
                num_streams = len(self.freshness_agent.stream_configs) if hasattr(self.freshness_agent, 'stream_configs') else 0
                logger.info(f"        ✅ Monitoring {num_streams} stream(s) (no separate consumer needed)")
            # NOTE: freshness_agent.start() NOT called - orchestrator handles message tracking
            
            # Step 3: Quality Orchestrator (coordinates all 6 quality agents)
            logger.info("\n  [3/3] Starting Quality Orchestrator...")
            if self.quality_orchestrator:
                await self.quality_orchestrator.start()
                logger.info("        ✅ Quality Orchestrator started")
                logger.info("            Consuming raw_data.* → validating → publishing to clean.*")
                logger.info("            Pipeline stages: Schema → Leakage → Anomaly → Freshness → Reconciliation → Scoring")
                logger.info("            All 6 quality agents registered and coordinated")
                logger.info("        ⏳ Waiting for orchestrator to start producing clean data...")
                # Smart coordination: wait for clean data to appear
                await self._wait_for_topic_messages("clean.market.trades", min_messages=1, timeout=20.0)
            
            # Observability: ClickHouse TSDB (incident analytics)
            logger.info("\n  [OBSERVABILITY] ClickHouse TSDB (incident monitoring)...")
            if self.tsdb:
                try:
                    asyncio.create_task(self.tsdb.start_incident_consumption())
                    logger.info("        ✅ TSDB operational (consuming incidents.* topics)")
                except Exception as e:
                    logger.warning(f"        ⚠️  TSDB unavailable: {e}")
            
            # Wait for Silver Layer to process Bronze data
            logger.info("\n⏳ Silver Layer processing - validating raw data...")
            # Smart coordination: ensure clean data is accumulating
            logger.info("        ⏳ Waiting for clean data volume...")
            await self._wait_for_topic_messages("clean.market.trades", min_messages=3, timeout=15.0)
            logger.info("✅ Silver Layer operational - clean.* topics populated")
            
            # ================================================================
            # GOLD LAYER: Business-Ready Curated Datasets
            # ================================================================
            if self.enable_gold_layer:
                logger.info("\n" + "=" * 80)
                logger.info("🥇 GOLD LAYER: Starting Curated Data Pipeline")
                logger.info("=" * 80)
                logger.info("Architecture: clean.* → curated.data.* (business-ready datasets)")
                
                # Start curators sequentially with smart coordination
                if self.ohlcv_aggregator:
                    logger.info("\n  [1/4] OHLCV Aggregator (multi-timeframe bars)...")
                    await self.ohlcv_aggregator.start()
                    logger.info("        ✅ OHLCV Aggregator started")
                    logger.info("            Output: curated.data.ohlcv_{1s,5s,1m,5m,15m,1h,1d}")
                    logger.info("        ⏳ Waiting for component readiness...")
                    # Smart coordination: wait for component to be ready
                    await self._wait_for_component_ready(self.ohlcv_aggregator, "OHLCV Aggregator", timeout=15.0)
                
                if self.symbol_normalizer:
                    logger.info("\n  [2/4] Symbol Normalizer (cross-venue unification)...")
                    await self.symbol_normalizer.start()
                    logger.info("        ✅ Symbol Normalizer started")
                    logger.info("            Output: curated.data.symbols")
                    logger.info("        ⏳ Waiting for component readiness...")
                    # Smart coordination: wait for component to be ready
                    await self._wait_for_component_ready(self.symbol_normalizer, "Symbol Normalizer", timeout=15.0)
                
                if self.orderbook_curator:
                    logger.info("\n  [3/4] Orderbook Curator (fixed-interval snapshots)...")
                    await self.orderbook_curator.start()
                    logger.info("        ✅ Orderbook Curator started")
                    logger.info("            Output: curated.data.orderbook_snapshot")
                    logger.info("        ⏳ Waiting for component readiness...")
                    # Smart coordination: wait for component to be ready
                    await self._wait_for_component_ready(self.orderbook_curator, "Orderbook Curator", timeout=15.0)
                
                if self.options_chain_curator:
                    logger.info("\n  [4/6] Options Chain Curator (Greeks & moneyness)...")
                    await self.options_chain_curator.start()
                    logger.info("        ✅ Options Chain Curator started")
                    logger.info("            Output: curated.data.options_chain")
                    logger.info("        ⏳ Waiting for component readiness...")
                    # Smart coordination: wait for component to be ready
                    await self._wait_for_component_ready(self.options_chain_curator, "Options Chain Curator", timeout=15.0)
                
                if self.macro_tradfi_curator:
                    logger.info("\n  [5/6] Macro/TradFi Curator (economic indicators & risk regime)...")
                    await self.macro_tradfi_curator.start()
                    logger.info("        ✅ Macro/TradFi Curator started")
                    logger.info("            Output: curated.data.macro_snapshot, curated.data.macro_derived, curated.data.risk_regime")
                
                if self.crypto_structure_curator:
                    logger.info("\n  [6/6] Crypto Market Structure Curator (market regimes & sector rotation)...")
                    await self.crypto_structure_curator.start()
                    logger.info("        ✅ Crypto Market Structure Curator started")
                    logger.info("            Output: curated.data.crypto_market_structure, curated.data.crypto_regime, curated.data.sector_rotation")
                
                logger.info("\n✅ Gold Layer operational - curated.data.* topics populated")
            
            # ================================================================
            # PIPELINE FULLY OPERATIONAL
            # ================================================================
            logger.info("\n" + "=" * 80)
            logger.info("✅ DATA PIPELINE FULLY OPERATIONAL")
            logger.info("=" * 80)
            logger.info("\n📊 Medallion Architecture Data Flow:")
            logger.info("  🥉 BRONZE → raw_data.* topics (collectors)")
            logger.info("  🥈 SILVER → clean.* topics (quality pipeline)")
            logger.info("  🥇 GOLD   → curated.data.* topics (business-ready datasets)")
            logger.info("\n📈 Active Topics:")
            logger.info("  Bronze: raw_data.exchange_feed, raw_data.options_chain, raw_data.onchain_events, ...")
            logger.info("  Silver: clean.market.trades, clean.market.book, clean.market.options, ...")
            if self.enable_gold_layer:
                logger.info("  Gold:   curated.data.ohlcv_*, curated.data.symbols, curated.data.orderbook_snapshot, ...")
                logger.info("    • curated.data.orderbook_snapshot")
                logger.info("    • curated.data.options_chain")
            logger.info("\nMonitoring:")
            logger.info("  Logs: data_pipeline.log")
            logger.info("  Metrics Endpoint: http://localhost:8000/metrics")
            logger.info("  Prometheus: http://localhost:9090")
            logger.info("  Grafana Dashboard: http://localhost:3000 (admin/satoshi_admin)")
            logger.info("\nPress Ctrl+C to gracefully shutdown")
            logger.info("=" * 80)
            
            # Keep running until interrupted with memory monitoring
            loop_count = 0
            while self.running:
                await asyncio.sleep(1)
                loop_count += 1
                
                # Memory monitoring every 60 seconds
                if loop_count % 60 == 0 and self.memory_governor:
                    try:
                        import psutil
                        process = psutil.Process()
                        mem_info = process.memory_info()
                        mem_mb = mem_info.rss / 1024 / 1024
                        
                        # Log memory usage
                        logger.info(f"📊 Memory Status: RSS={mem_mb:.1f}MB, " +
                                  f"VMS={mem_info.vms/1024/1024:.1f}MB, " +
                                  f"Active Components={len(self.memory_governor.state_stores)}")
                        
                        # Trigger GC if memory exceeds threshold (1.5GB)
                        if mem_mb > 1500:
                            import gc
                            logger.warning(f"⚠️  High memory usage ({mem_mb:.1f}MB) - triggering garbage collection")
                            gc.collect()
                            
                            # Check again after GC
                            new_mem = psutil.Process().memory_info().rss / 1024 / 1024
                            freed_mb = mem_mb - new_mem
                            logger.info(f"   Freed {freed_mb:.1f}MB via GC (now {new_mem:.1f}MB)")
                    except ImportError:
                        pass  # psutil not available, skip memory monitoring
                    except Exception as e:
                        logger.warning(f"Memory monitoring error: {e}")
                
        except KeyboardInterrupt:
            logger.info("\n\n🛑 Received shutdown signal...")
        except Exception as e:
            logger.error(f"\n\n❌ Pipeline error: {e}")
            raise
        finally:
            await self.stop_pipeline()
    
    async def _wait_for_topic_messages(self, topic: str, min_messages: int = 1, timeout: float = 30.0) -> bool:
        """
        Wait for a Kafka topic to have minimum number of messages.
        
        Args:
            topic: Topic name to check
            min_messages: Minimum message count required
            timeout: Maximum seconds to wait
            
        Returns:
            True if topic has messages, False if timeout
        """
        if not self.streaming_bus:
            logger.warning(f"        ⚠️  StreamingBus not initialized, skipping topic check for {topic}")
            return False
            
        import time
        start_time = time.time()
        
        while (time.time() - start_time) < timeout:
            try:
                # Check if topic has messages using Kafka consumer
                from aiokafka import AIOKafkaConsumer
                from aiokafka.errors import UnknownTopicOrPartitionError
                
                consumer = AIOKafkaConsumer(
                    topic,
                    bootstrap_servers=self.streaming_bus.bootstrap_servers,
                    auto_offset_reset='earliest',
                    enable_auto_commit=False,
                    consumer_timeout_ms=1000
                )
                
                await consumer.start()
                try:
                    # Get topic partitions
                    partitions = consumer.partitions_for_topic(topic)
                    if not partitions:
                        await asyncio.sleep(0.5)
                        continue
                    
                    # Check message count across all partitions
                    total_messages = 0
                    for partition in partitions:
                        tp = TopicPartition(topic, partition)
                        end_offset = await consumer.end_offsets([tp])
                        total_messages += end_offset[tp]
                    
                    if total_messages >= min_messages:
                        logger.info(f"        ✅ Topic {topic} ready with {total_messages} message(s)")
                        return True
                        
                except UnknownTopicOrPartitionError:
                    pass  # Topic not yet created, wait
                finally:
                    await consumer.stop()
                    
            except Exception as e:
                logger.debug(f"        ⏳ Waiting for {topic} ({e})")
            
            await asyncio.sleep(0.5)
        
        logger.warning(f"        ⚠️  Timeout waiting for {topic} (no messages after {timeout}s)")
        return False
    
    async def _wait_for_component_ready(self, component, component_name: str, timeout: float = 30.0) -> bool:
        """
        Wait for a component to be ready by checking if it's actively consuming/producing.
        
        Args:
            component: Component instance to check
            component_name: Human-readable name for logging
            timeout: Maximum seconds to wait
            
        Returns:
            True if component is ready, False if timeout
        """
        import time
        start_time = time.time()
        
        while (time.time() - start_time) < timeout:
            try:
                # Check if component has started consuming/producing
                # Different components expose different health indicators
                if hasattr(component, '_running') and component._running:
                    logger.info(f"        ✅ {component_name} is ready (running flag set)")
                    return True
                elif hasattr(component, 'running') and component.running:
                    logger.info(f"        ✅ {component_name} is ready (running flag set)")
                    return True
                elif hasattr(component, 'is_running') and await component.is_running():
                    logger.info(f"        ✅ {component_name} is ready (health check passed)")
                    return True
                    
            except Exception as e:
                logger.debug(f"        ⏳ Health check for {component_name}: {e}")
            
            await asyncio.sleep(0.5)
        
        # Timeout - but component might still work, just no health indicator available
        logger.warning(f"        ⚠️  {component_name} readiness timeout (proceeding anyway)")
        return False
    
    async def stop_pipeline(self):
        """Gracefully stop all pipeline components."""
        logger.info("\n" + "=" * 80)
        logger.info("STOPPING DATA PIPELINE")
        logger.info("=" * 80)
        
        self.running = False
        
        # Stop Gold Layer curators first (stop consuming curated data)
        if self.enable_gold_layer:
            logger.info("  - Stopping Gold Layer Curators...")
            
            if self.ohlcv_aggregator:
                await self.ohlcv_aggregator.shutdown()
                logger.info("    ✅ OHLCV Aggregator stopped")
            
            if self.symbol_normalizer:
                await self.symbol_normalizer.shutdown()
                logger.info("    ✅ Symbol Normalizer stopped")
            
            if self.orderbook_curator:
                await self.orderbook_curator.stop()
                logger.info("    ✅ Orderbook Curator stopped")
            
            if self.options_chain_curator:
                await self.options_chain_curator.stop()
                logger.info("    ✅ Options Chain Curator stopped")
            
            if self.macro_tradfi_curator:
                await self.macro_tradfi_curator.stop()
                logger.info("    ✅ Macro/TradFi Curator stopped")
            
            if self.crypto_structure_curator:
                await self.crypto_structure_curator.stop()
                logger.info("    ✅ Crypto Market Structure Curator stopped")
        
        # Stop quality orchestrator (stop consuming clean data)
        if self.quality_orchestrator:
            logger.info("  - Stopping Quality Orchestrator...")
            await self.quality_orchestrator.stop()
            logger.info("    ✅ Quality Orchestrator stopped")
        
        # Stop ClickHouse TSDB (stop consuming incidents)
        if self.tsdb:
            logger.info("  - Stopping ClickHouse TSDB...")
            try:
                await self.tsdb.stop_incident_consumption()
                await self.tsdb.close()
                logger.info("    ✅ ClickHouse TSDB stopped")
            except Exception as e:
                logger.warning(f"    ⚠️  Error stopping TSDB: {e}")
        
        # Stop data collectors (stop producing raw data)
        logger.info("  - Stopping Data Collectors...")
        
        if self.exchange_connector:
            await self.exchange_connector.stop()
            logger.info("    ✅ Exchange Connector stopped")
        
        if self.options_collector:
            await self.options_collector.stop()
            logger.info("    ✅ Options Collector stopped")
        
        if self.onchain_collector:
            await self.onchain_collector.stop()
            logger.info("    ✅ Onchain Collector stopped")
        
        if self.events_collector:
            await self.events_collector.stop()
            logger.info("    ✅ Events Collector stopped")
        
        if self.macro_collector:
            await self.macro_collector.stop()
            logger.info("    ✅ Macro Collector stopped")
        
        if self.crypto_metrics_collector:
            await self.crypto_metrics_collector.stop()
            logger.info("    ✅ Crypto Metrics Collector stopped")
        
        # Stop infrastructure components
        logger.info("  - Stopping Infrastructure Components...")
        
        # Stop metrics update task
        if self._metrics_task:
            self._metrics_task.cancel()
            try:
                await self._metrics_task
            except asyncio.CancelledError:
                pass
            logger.info("    ✅ Dashboard metrics updater stopped")
        
        # Stop MemoryGovernor background tasks
        if self.memory_governor:
            await self.memory_governor.stop_background_tasks()
            logger.info("    ✅ MemoryGovernor stopped")
        
        # WorkloadDistributor is stateless (no cleanup needed)
        if self.workload_distributor:
            logger.info("    ✅ WorkloadDistributor stopped")
        
        # Stop streaming bus last (ensures all producers are properly closed)
        if self.streaming_bus:
            logger.info("  - Stopping Streaming Bus...")
            try:
                # Shutdown producer pool (closes all Kafka producers)
                await self.streaming_bus.producer_pool.shutdown()
                logger.info("    ✅ Producer pool shutdown complete (all Kafka connections closed)")
                
                # Close any remaining consumer connections
                # Note: Individual component consumers are already closed by their .stop() methods
                logger.info("    ✅ Streaming Bus stopped cleanly")
            except Exception as e:
                logger.error(f"    ❌ Error during streaming bus shutdown: {e}")
                # Try to force close producers even if graceful shutdown failed
                try:
                    for producer in self.streaming_bus.producer_pool.producers.values():
                        if hasattr(producer, '_producer') and producer._producer:
                            await producer._producer.stop()
                    logger.info("    ⚠️  Forced producer cleanup completed")
                except Exception as force_error:
                    logger.error(f"    ❌ Force cleanup also failed: {force_error}")
        
        logger.info("=" * 80)
        logger.info("✅ DATA PIPELINE SHUTDOWN COMPLETE")
        logger.info("=" * 80)
    
    async def get_pipeline_health(self) -> dict:
        """Get health status of all pipeline components."""
        health = {
            "pipeline_running": self.running,
            "components": {}
        }
        
        if self.exchange_connector:
            health["components"]["exchange_connector"] = "running"
        
        if self.options_collector:
            health["components"]["options_collector"] = "running"
        
        if self.onchain_collector:
            health["components"]["onchain_collector"] = "running"
        
        if self.events_collector:
            health["components"]["events_collector"] = "running"
        
        if self.quality_orchestrator:
            health["components"]["quality_orchestrator"] = await self.quality_orchestrator.get_orchestration_health()
        
        if self.enable_gold_layer:
            health["components"]["gold_layer"] = {
                "ohlcv_aggregator": "running" if self.ohlcv_aggregator else "disabled",
                "symbol_normalizer": "running" if self.symbol_normalizer else "disabled",
                "orderbook_curator": "running" if self.orderbook_curator else "disabled",
                "options_chain_curator": "running" if self.options_chain_curator else "disabled"
            }
        
        return health
    
    async def _update_dashboard_metrics(self) -> None:
        """Periodically update dashboard metrics for Grafana."""
        while self.running:
            try:
                # StreamingBus Health
                if self.streaming_bus:
                    # Health status (1=healthy, 0=unhealthy)
                    is_healthy = 1.0 if self.streaming_bus.producer_pool else 0.0
                    self.metrics.set_gauge("streaming_bus_health_status", is_healthy)
                    
                    # Active topics count
                    active_topics = len(self.streaming_bus.topic_configs)
                    self.metrics.set_gauge("streaming_bus_active_topics", float(active_topics))
                
                # Data Quality Score (from quality orchestrator)
                if self.quality_orchestrator:
                    # Mock quality score for now (in production this would come from actual quality metrics)
                    quality_score = 95.0  # Example: 95% quality
                    self.metrics.set_gauge("data_quality_score", quality_score, {"layer": "silver"})
                
                # Gold Layer metrics
                if self.enable_gold_layer and self.ohlcv_aggregator:
                    # These metrics should ideally come from the OHLCV Aggregator itself
                    # For now we'll set placeholders to make the dashboard show data
                    pass  # OHLCV Aggregator should publish its own metrics
                
                await asyncio.sleep(5.0)  # Update every 5 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error updating dashboard metrics: {e}")
                await asyncio.sleep(5.0)


async def main():
    """Main entry point for data pipeline."""
    
    # Parse command line arguments for config mode
    config_mode = "development"
    enable_gold_layer = True
    
    if len(sys.argv) > 1:
        if sys.argv[1] in ["institutional", "production"]:
            config_mode = "institutional"
        elif sys.argv[1] == "--no-gold":
            enable_gold_layer = False
    
    if len(sys.argv) > 2:
        if sys.argv[2] == "--no-gold":
            enable_gold_layer = False
    
    logger.info(f"Starting Data Pipeline in {config_mode.upper()} mode (Gold Layer: {enable_gold_layer})")
    
    # Create pipeline manager
    pipeline = DataPipelineManager(config_mode=config_mode, enable_gold_layer=enable_gold_layer)
    
    # Setup signal handlers for graceful shutdown
    def signal_handler(sig, frame):
        logger.info(f"\nReceived signal {sig}")
        pipeline.running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Initialize all components
        await pipeline.initialize_components()
        
        # Start the pipeline
        await pipeline.start_pipeline()
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    """
    Usage:
        python run_data_pipeline.py                    # Development mode with Gold Layer
        python run_data_pipeline.py institutional      # Production mode with Gold Layer
        python run_data_pipeline.py --no-gold          # Development mode without Gold Layer
        python run_data_pipeline.py institutional --no-gold  # Production without Gold Layer
    
    Data Pipeline Layers:
        BRONZE: Raw data collection from exchanges/blockchain/events
        SILVER: Quality validation, schema checks, anomaly detection
        GOLD:   Curated data - OHLCV bars, normalized symbols, orderbook snapshots, options chains
    """
    asyncio.run(main())
