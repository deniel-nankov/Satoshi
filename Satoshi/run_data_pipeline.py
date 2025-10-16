#!/usr/bin/env python3
"""
Master Data Pipeline Orchestration Script

Starts all data collectors and the Data Quality Orchestrator to create
the complete Bronze → Silver data pipeline.

Components Started:
- Exchange Connector Agent (CEX/DEX market data)
- Options Chain Collector Agent (options market data)
- Onchain Collector Agent (blockchain events)
- Events Collector Agent (off-chain events)
- Data Quality Orchestrator (quality pipeline)

Data Flow:
  Collectors → raw_data.* → Quality Orchestrator → clean.* + incidents.*
"""

import asyncio
import logging
import signal
import sys
from pathlib import Path
from typing import Optional, List

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Import infrastructure
from infra.bus.streaming_bus import StreamingBus
from infra.monitoring.prometheus_metrics import get_metrics_collector

# Import data collectors
from engines.data.exchange_connector import ExchangeConnectorAgent
from engines.data.options_chain_collector import OptionsChainCollectorAgent
from engines.data.onchain_collector import OnchainCollectorAgent
from engines.data.events_collector import EventsCollectorAgent

# Import quality pipeline
from engines.data.data_quality_orchestrator import (
    DataQualityOrchestrator,
    OrchestrationConfig,
    OrchestrationFactory
)
from engines.data.schema_validator import SchemaValidatorAgent
from engines.data.leakage_police import LeakagePolice, LeakagePoliceConfig
from engines.data.anomaly_detector import DataAnomalyDetector
from engines.data.freshness_agent import FreshnessAgent
from engines.data.reconciler_agent import ReconcilerAgent, ReconcilerConfig

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
    
    Manages lifecycle of all data collectors and quality orchestrator.
    """
    
    def __init__(self, config_mode: str = "development"):
        """
        Initialize the data pipeline manager.
        
        Args:
            config_mode: "development" or "institutional" for different configs
        """
        self.config_mode = config_mode
        self.running = False
        
        # Components
        self.streaming_bus: Optional[StreamingBus] = None
        self.exchange_connector: Optional[ExchangeConnectorAgent] = None
        self.options_collector: Optional[OptionsChainCollectorAgent] = None
        self.onchain_collector: Optional[OnchainCollectorAgent] = None
        self.events_collector: Optional[EventsCollectorAgent] = None
        self.quality_orchestrator: Optional[DataQualityOrchestrator] = None
        
        # Quality agents
        self.schema_validator: Optional[SchemaValidatorAgent] = None
        self.leakage_police: Optional[LeakagePolice] = None
        self.anomaly_detector: Optional[DataAnomalyDetector] = None
        self.freshness_agent: Optional[FreshnessAgent] = None
        self.reconciler_agent: Optional[ReconcilerAgent] = None
        
        # Metrics
        self.metrics = get_metrics_collector()
        
        logger.info(f"Data Pipeline Manager initialized in {config_mode} mode")
    
    def _get_streaming_bus_config(self) -> dict:
        """Get streaming bus configuration based on mode."""
        base_config = {
            "bootstrap_servers": ["localhost:9092"],
            "client_id": "satoshi-data-pipeline",
            "security_protocol": "PLAINTEXT",
            "environment": self.config_mode
        }
        
        if self.config_mode == "institutional":
            # Production-grade configuration
            base_config.update({
                "enable_ssl": True,
                "ssl_cafile": "/path/to/ca-cert",
                "ssl_certfile": "/path/to/client-cert",
                "ssl_keyfile": "/path/to/client-key",
                "security_protocol": "SSL"
            })
        
        return base_config
    
    def _get_exchange_connector_config(self) -> dict:
        """
        Get exchange connector configuration.
        
        IMPLEMENTED VENUES (6):
        - binance: Spot trading
        - binance_futures: Perpetuals/futures
        - coinbase: Coinbase Pro
        - gemini: Gemini exchange
        - kraken: Kraken exchange
        - okx: OKX exchange
        
        ⚠️  CONFIGURATION REQUIRED:
        Copy config_template.py to config.py and add your API keys!
        """
        return {
            "venues": {
                # Copy from config_template.py or config.py
                # See config_template.py for ALL 6 exchange configurations
                "binance": {
                    "enabled": False,  # Set to True after adding API keys
                    "api_key": "YOUR_BINANCE_API_KEY",
                    "api_secret": "YOUR_BINANCE_SECRET",
                    "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
                    "data_types": ["trades", "book"],
                    "rate_limit_qps": 100
                },
                # Add more venues from config_template.py
            },
            "streaming_bus": self._get_streaming_bus_config(),
            "circuit_breaker_failure_threshold": 5 if self.config_mode == "development" else 3,
            "health_check_interval": 60.0,
            "max_retries": 5,
            "target_uptime_pct": 99.9 if self.config_mode == "institutional" else 99.0
        }
    
    def _get_options_collector_config(self) -> dict:
        """Get options chain collector configuration."""
        return {
            "venues": ["deribit", "binance_options"],
            "symbols": ["BTC", "ETH"],
            "collection_interval_sec": 60,
            "streaming_bus": self._get_streaming_bus_config(),
            "rate_limit_qps": 20
        }
    
    def _get_onchain_collector_config(self) -> dict:
        """
        Get onchain collector configuration.
        
        SUPPORTED CHAINS (7):
        - ethereum: Ethereum mainnet
        - bsc: Binance Smart Chain
        - polygon: Polygon PoS
        - arbitrum: Arbitrum One (L2)
        - optimism: Optimism (L2)
        - base: Base (Coinbase L2)
        - avalanche: Avalanche C-Chain
        
        ⚠️  CONFIGURATION REQUIRED:
        Add RPC endpoints (Alchemy, Infura, etc.) in config.py
        """
        return {
            "chains": {
                # Copy from config_template.py - 7 chains configured
                "ethereum": {
                    "enabled": False,  # Set to True after adding RPC URL
                    "rpc_url": "https://eth-mainnet.g.alchemy.com/v2/YOUR_ALCHEMY_KEY",
                    "block_polling_interval": 12.0,
                    "confirmations_required": 12
                },
                # Add more chains from config_template.py
            },
            "streaming_bus": self._get_streaming_bus_config(),
            "rate_limit_qps": 10
        }
    
    def _get_events_collector_config(self) -> dict:
        """
        Get events collector configuration.
        
        IMPLEMENTED SOURCES (10+):
        - snapshot: DAO governance proposals
        - compound_governance: Compound proposals
        - github: Protocol release monitoring (6+ repos)
        - binance_status: Exchange maintenance
        - coinbase_status: Exchange health
        - token_unlocks: Token vesting schedules
        - cryptopanic: Crypto news aggregator
        - coindesk: News RSS feed
        
        ⚠️  CONFIGURATION REQUIRED:
        Add API keys for GitHub, CryptoPanic, etc. in config.py
        """
        return {
            "sources": {
                # Copy complete config from config_template.py
                "github": {
                    "enabled": False,  # Set to True after adding token
                    "token": "YOUR_GITHUB_TOKEN",
                    "tracked_repos": [
                        "ethereum/go-ethereum",
                        "bitcoin/bitcoin",
                        "solana-labs/solana"
                    ],
                    "poll_interval_sec": 600
                },
                # Add more sources from config_template.py
            },
            "streaming_bus": self._get_streaming_bus_config(),
            "rate_limit_qps": 5
        }
    
    async def initialize_components(self):
        """Initialize all pipeline components."""
        logger.info("Initializing data pipeline components...")
        
        try:
            # 1. Initialize Streaming Bus (shared infrastructure)
            logger.info("Initializing Streaming Bus...")
            streaming_config = self._get_streaming_bus_config()
            self.streaming_bus = StreamingBus(streaming_config)
            
            # Create topics if needed
            await self.streaming_bus.create_topics_from_config()
            
            # Validate data ingestion topics
            if not self.streaming_bus.validate_data_ingestion_topics():
                logger.warning("Some required topics are missing - they will be created on first publish")
            
            # Start rate budget listener
            await self.streaming_bus.ensure_rate_budget_listener()
            
            logger.info("✅ Streaming Bus initialized")
            
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
            
            logger.info("✅ All data collectors initialized")
            
            # 3. Initialize Quality Pipeline
            logger.info("Initializing quality pipeline...")
            
            # Initialize quality agents
            self.schema_validator = SchemaValidatorAgent({})
            self.leakage_police = LeakagePolice(LeakagePoliceConfig())
            self.anomaly_detector = DataAnomalyDetector({})
            self.freshness_agent = FreshnessAgent({})
            self.reconciler_agent = ReconcilerAgent(ReconcilerConfig())
            
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
            self.quality_orchestrator.register_quality_agents(
                schema_validator=self.schema_validator,
                leakage_police=self.leakage_police,
                anomaly_detector=self.anomaly_detector,
                freshness_agent=self.freshness_agent,
                reconciler_agent=self.reconciler_agent
            )
            
            logger.info("✅ Quality pipeline initialized")
            
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
            # Start data collectors
            logger.info("\n📡 Starting Data Collectors...")
            
            if self.exchange_connector:
                logger.info("  - Starting Exchange Connector...")
                await self.exchange_connector.start()
                logger.info("    ✅ Exchange Connector running")
            
            if self.options_collector:
                logger.info("  - Starting Options Chain Collector...")
                await self.options_collector.start()
                logger.info("    ✅ Options Collector running")
            
            if self.onchain_collector:
                logger.info("  - Starting Onchain Collector...")
                await self.onchain_collector.start()
                logger.info("    ✅ Onchain Collector running")
            
            if self.events_collector:
                logger.info("  - Starting Events Collector...")
                await self.events_collector.start()
                logger.info("    ✅ Events Collector running")
            
            # Wait a moment for collectors to start publishing
            logger.info("\n⏳ Waiting for initial data collection (5 seconds)...")
            await asyncio.sleep(5)
            
            # Start quality orchestrator
            logger.info("\n🛡️  Starting Quality Orchestrator...")
            if self.quality_orchestrator:
                await self.quality_orchestrator.start()
                logger.info("    ✅ Quality Orchestrator running")
            
            logger.info("\n" + "=" * 80)
            logger.info("✅ DATA PIPELINE FULLY OPERATIONAL")
            logger.info("=" * 80)
            logger.info("\nData Flow:")
            logger.info("  Collectors → raw_data.* topics")
            logger.info("  Quality Orchestrator → clean.* topics + incidents.* topics")
            logger.info("\nMonitoring:")
            logger.info("  Logs: data_pipeline.log")
            logger.info("  Metrics: Available via Prometheus endpoint")
            logger.info("\nPress Ctrl+C to gracefully shutdown")
            logger.info("=" * 80)
            
            # Keep running until interrupted
            while self.running:
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("\n\n🛑 Received shutdown signal...")
        except Exception as e:
            logger.error(f"\n\n❌ Pipeline error: {e}")
            raise
        finally:
            await self.stop_pipeline()
    
    async def stop_pipeline(self):
        """Gracefully stop all pipeline components."""
        logger.info("\n" + "=" * 80)
        logger.info("STOPPING DATA PIPELINE")
        logger.info("=" * 80)
        
        self.running = False
        
        # Stop quality orchestrator first (stop consuming)
        if self.quality_orchestrator:
            logger.info("  - Stopping Quality Orchestrator...")
            await self.quality_orchestrator.stop()
            logger.info("    ✅ Quality Orchestrator stopped")
        
        # Stop data collectors
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
        
        # Stop streaming bus last
        if self.streaming_bus:
            await self.streaming_bus.producer_pool.shutdown()
            logger.info("    ✅ Streaming Bus stopped")
        
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
        
        return health


async def main():
    """Main entry point for data pipeline."""
    
    # Parse command line arguments for config mode
    config_mode = "development"
    if len(sys.argv) > 1:
        if sys.argv[1] in ["institutional", "production"]:
            config_mode = "institutional"
    
    logger.info(f"Starting Data Pipeline in {config_mode.upper()} mode")
    
    # Create pipeline manager
    pipeline = DataPipelineManager(config_mode=config_mode)
    
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
        python run_data_pipeline.py                    # Development mode
        python run_data_pipeline.py institutional      # Production mode
    """
    asyncio.run(main())
