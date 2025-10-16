#!/usr/bin/env python3
"""
Test Data Pipeline - Simplified validation script

Tests the complete data flow:
1. Mock data injection → raw_data.* topics
2. Quality orchestrator processes → clean.* topics
3. Incidents published → incidents.* topics

This script validates the pipeline without requiring external API keys.
"""

import asyncio
import logging
import json
import time
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from infra.bus.streaming_bus import StreamingBus
from engines.data.data_quality_orchestrator import (
    DataQualityOrchestrator,
    OrchestrationFactory
)
from engines.data.schema_validator import SchemaValidatorAgent
from engines.data.leakage_police import LeakagePolice, LeakagePoliceConfig
from engines.data.anomaly_detector import DataAnomalyDetector
from engines.data.freshness_agent import FreshnessAgent
from engines.data.reconciler_agent import ReconcilerAgent, ReconcilerConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataPipelineTester:
    """Test harness for data pipeline validation."""
    
    def __init__(self):
        self.streaming_bus = None
        self.orchestrator = None
        self.test_results = {
            "raw_data_published": 0,
            "clean_data_received": 0,
            "incidents_received": 0,
            "test_passed": False
        }
    
    async def setup(self):
        """Setup test infrastructure."""
        logger.info("Setting up test environment...")
        
        # Initialize streaming bus
        streaming_config = {
            "bootstrap_servers": ["localhost:9092"],
            "client_id": "pipeline-tester",
            "security_protocol": "PLAINTEXT"
        }
        self.streaming_bus = StreamingBus(streaming_config)
        
        # Create test topics
        await self.streaming_bus.create_topics_from_config()
        
        # Initialize quality pipeline
        schema_validator = SchemaValidatorAgent({})
        leakage_police = LeakagePolice(LeakagePoliceConfig())
        anomaly_detector = DataAnomalyDetector({})
        freshness_agent = FreshnessAgent({})
        reconciler_agent = ReconcilerAgent(ReconcilerConfig())
        
        self.orchestrator = OrchestrationFactory.create_development_orchestrator(
            self.streaming_bus
        )
        
        self.orchestrator.register_quality_agents(
            schema_validator=schema_validator,
            leakage_police=leakage_police,
            anomaly_detector=anomaly_detector,
            freshness_agent=freshness_agent,
            reconciler_agent=reconciler_agent
        )
        
        logger.info("✅ Test environment ready")
    
    async def publish_test_data(self):
        """Publish test data to raw_data topics."""
        logger.info("\n📤 Publishing test data...")
        
        test_messages = [
            {
                "topic": "raw_data.exchange_feed",
                "partition_key": "binance:BTCUSDT",
                "payload": {
                    "venue": "binance",
                    "symbol": "BTCUSDT",
                    "trade_id": "test_trade_1",
                    "price": 42000.50,
                    "quantity": 0.1,
                    "side": "buy",
                    "timestamp_utc_us": int(time.time() * 1_000_000),
                    "is_maker": True
                },
                "headers": {
                    "data_type": "trade",
                    "venue": "binance",
                    "source": "test_harness"
                }
            },
            {
                "topic": "raw_data.options_chain",
                "partition_key": "deribit:BTC",
                "payload": {
                    "venue": "deribit",
                    "underlying": "BTC",
                    "strike": 40000,
                    "expiry": "2025-12-31",
                    "option_type": "call",
                    "iv": 0.65,
                    "bid": 1500,
                    "ask": 1520,
                    "timestamp_utc_us": int(time.time() * 1_000_000)
                },
                "headers": {
                    "data_type": "options_surface",
                    "venue": "deribit",
                    "source": "test_harness"
                }
            },
            {
                "topic": "raw_data.onchain_events",
                "partition_key": "ethereum:12345",
                "payload": {
                    "chain": "ethereum",
                    "block_number": 12345,
                    "tx_hash": "0xtest123",
                    "event_type": "transfer",
                    "from_address": "0xabc",
                    "to_address": "0xdef",
                    "amount": "1000000000000000000",
                    "token": "USDT",
                    "timestamp_utc_us": int(time.time() * 1_000_000)
                },
                "headers": {
                    "data_type": "flows",
                    "chain": "ethereum",
                    "source": "test_harness"
                }
            },
            {
                "topic": "raw_data.offchain_events",
                "partition_key": "twitter:event_1",
                "payload": {
                    "source": "twitter",
                    "event_type": "tweet",
                    "author": "@test_user",
                    "content": "Bitcoin hits new high!",
                    "sentiment": "positive",
                    "timestamp_utc_us": int(time.time() * 1_000_000)
                },
                "headers": {
                    "data_type": "social_event",
                    "source": "twitter",
                    "source": "test_harness"
                }
            }
        ]
        
        for msg in test_messages:
            success = await self.streaming_bus.publish_with_headers(
                topic=msg["topic"],
                partition_key=msg["partition_key"],
                payload=msg["payload"],
                headers=msg["headers"]
            )
            
            if success:
                self.test_results["raw_data_published"] += 1
                logger.info(f"  ✅ Published to {msg['topic']}")
            else:
                logger.error(f"  ❌ Failed to publish to {msg['topic']}")
        
        logger.info(f"\n📊 Published {self.test_results['raw_data_published']}/{len(test_messages)} test messages")
    
    async def start_orchestrator(self):
        """Start the quality orchestrator."""
        logger.info("\n🛡️  Starting Quality Orchestrator...")
        await self.orchestrator.start()
        logger.info("  ✅ Orchestrator running")
    
    async def monitor_output(self, duration_seconds=10):
        """Monitor clean and incident topics for output."""
        logger.info(f"\n👀 Monitoring output topics for {duration_seconds} seconds...")
        
        # Subscribe to clean topics
        async def clean_handler(topic: str, partition_key: str, 
                               payload: dict, headers: dict):
            self.test_results["clean_data_received"] += 1
            logger.info(f"  ✅ Received clean data on {topic}")
            logger.debug(f"     Quality Score: {payload.get('quality_score', 'N/A')}")
        
        # Subscribe to incident topics
        async def incident_handler(topic: str, partition_key: str,
                                   payload: dict, headers: dict):
            self.test_results["incidents_received"] += 1
            logger.info(f"  ⚠️  Received incident on {topic}")
            logger.debug(f"     Incident Type: {payload.get('incident_type', 'N/A')}")
        
        # Start consumers
        clean_task = asyncio.create_task(
            self.streaming_bus.subscribe_with_worker_pool(
                consumer_group="test_clean_monitor",
                topics=["clean.market.trades", "clean.market.options", 
                       "clean.market.onchain", "clean.market.events"],
                handler=clean_handler,
                pool_size=2
            )
        )
        
        incident_task = asyncio.create_task(
            self.streaming_bus.subscribe_with_worker_pool(
                consumer_group="test_incident_monitor",
                topics=["incidents.SchemaViolation", "incidents.Anomaly",
                       "incidents.Leakage", "incidents.Freshness"],
                handler=incident_handler,
                pool_size=2
            )
        )
        
        # Monitor for specified duration
        await asyncio.sleep(duration_seconds)
        
        # Cancel consumers
        clean_task.cancel()
        incident_task.cancel()
        
        try:
            await clean_task
        except asyncio.CancelledError:
            pass
        
        try:
            await incident_task
        except asyncio.CancelledError:
            pass
        
        logger.info("\n📊 Monitoring complete")
    
    async def evaluate_results(self):
        """Evaluate test results."""
        logger.info("\n" + "=" * 80)
        logger.info("TEST RESULTS")
        logger.info("=" * 80)
        
        logger.info(f"Raw data published:    {self.test_results['raw_data_published']}")
        logger.info(f"Clean data received:   {self.test_results['clean_data_received']}")
        logger.info(f"Incidents received:    {self.test_results['incidents_received']}")
        
        # Determine if test passed
        self.test_results["test_passed"] = (
            self.test_results["raw_data_published"] > 0 and
            self.test_results["clean_data_received"] > 0
        )
        
        logger.info("=" * 80)
        if self.test_results["test_passed"]:
            logger.info("✅ TEST PASSED - Pipeline is operational!")
        else:
            logger.info("❌ TEST FAILED - Pipeline not processing correctly")
        logger.info("=" * 80)
        
        # Get orchestrator health
        if self.orchestrator:
            health = await self.orchestrator.get_orchestration_health()
            logger.info("\n📊 Orchestrator Health:")
            logger.info(f"  Running: {health['orchestrator_running']}")
            logger.info(f"  Mode: {health['pipeline_mode']}")
            logger.info(f"  Circuit Breaker: {'OPEN' if health['circuit_breaker_open'] else 'CLOSED'}")
            logger.info(f"  Quality Threshold: {health['quality_threshold']}")
        
        return self.test_results["test_passed"]
    
    async def cleanup(self):
        """Cleanup test resources."""
        logger.info("\n🧹 Cleaning up...")
        
        if self.orchestrator:
            await self.orchestrator.stop()
        
        if self.streaming_bus:
            await self.streaming_bus.producer_pool.shutdown()
        
        logger.info("✅ Cleanup complete")


async def main():
    """Run pipeline test."""
    logger.info("=" * 80)
    logger.info("DATA PIPELINE TEST")
    logger.info("=" * 80)
    
    tester = DataPipelineTester()
    
    try:
        # Setup
        await tester.setup()
        
        # Start orchestrator first
        await tester.start_orchestrator()
        
        # Give orchestrator time to initialize
        await asyncio.sleep(2)
        
        # Publish test data
        await tester.publish_test_data()
        
        # Monitor output
        await tester.monitor_output(duration_seconds=15)
        
        # Evaluate results
        test_passed = await tester.evaluate_results()
        
        # Cleanup
        await tester.cleanup()
        
        # Exit with appropriate code
        sys.exit(0 if test_passed else 1)
        
    except KeyboardInterrupt:
        logger.info("\n\n🛑 Test interrupted")
        await tester.cleanup()
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n\n❌ Test failed with error: {e}", exc_info=True)
        await tester.cleanup()
        sys.exit(1)


if __name__ == "__main__":
    """
    Usage:
        python test_data_pipeline.py
    
    Prerequisites:
        - Kafka/Redpanda running on localhost:9092
        - No other requirements (test data is mocked)
    """
    asyncio.run(main())
