#!/usr/bin/env python3
"""
Crypto Market Metrics Collector Agent

Mission: Collect crypto-wide market metrics from CoinGecko
Source: CoinGecko API (free tier, 50 calls/min)
Output Topic: raw_data.crypto.market_metrics
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from collections import deque
import hashlib
import json

# CoinGecko API
try:
    from pycoingecko import CoinGeckoAPI
    COINGECKO_AVAILABLE = True
except ImportError:
    COINGECKO_AVAILABLE = False
    print("⚠️  pycoingecko not installed. Install with: pip install pycoingecko")

from infra.bus.streaming_bus import StreamingBus

logger = logging.getLogger(__name__)


@dataclass
class CryptoMarketMetrics:
    """Crypto market-wide metrics from CoinGecko."""
    timestamp_utc_us: int
    source: str = "coingecko"
    total_market_cap_usd: Optional[float] = None
    total_volume_24h_usd: Optional[float] = None
    btc_dominance_pct: Optional[float] = None
    eth_dominance_pct: Optional[float] = None
    defi_market_cap_usd: Optional[float] = None
    defi_volume_24h_usd: Optional[float] = None
    defi_dominance_pct: Optional[float] = None
    active_cryptocurrencies: Optional[int] = None


class CryptoMetricsCollectorAgent:
    """
    Collects crypto market-wide metrics from CoinGecko.
    
    Metrics Collected:
    - Total crypto market cap
    - 24h trading volume
    - BTC dominance %
    - ETH dominance %
    - DeFi market cap & volume
    - Active cryptocurrencies count
    
    Output Topic:
    - raw_data.crypto.market_metrics
    """
    
    def __init__(self, streaming_bus: StreamingBus, config: Optional[Dict[str, Any]] = None):
        self.streaming_bus = streaming_bus
        self.config = config or {}
        
        # Collection interval (5 minutes default)
        self.collection_interval_sec = self.config.get("collection_interval_sec", 300)
        
        # API client
        self.cg = CoinGeckoAPI() if COINGECKO_AVAILABLE else None
        
        # Deduplication cache
        self.seen_hashes: deque = deque(maxlen=1000)
        
        # Circuit breaker state
        self.failures = 0
        self.circuit_open = False
        self.failure_threshold = 3
        
        # Metrics
        self.metrics = {
            "metrics_collected": 0,
            "duplicates_detected": 0,
            "api_errors": 0,
        }
        
        # Control flags
        self.running = False
        self._task: Optional[asyncio.Task] = None
        
        logger.info("CryptoMetricsCollectorAgent initialized")
    
    async def start(self) -> None:
        """Start collection task."""
        if self.running:
            logger.warning("CryptoMetricsCollectorAgent already running")
            return
        
        if not self.cg:
            logger.error("CoinGecko API not available - cannot start collector")
            return
        
        self.running = True
        logger.info("🪙 Starting CryptoMetricsCollectorAgent...")
        
        self._task = asyncio.create_task(self._collect_crypto_metrics())
        
        logger.info("✅ CryptoMetricsCollectorAgent started")
    
    async def stop(self) -> None:
        """Stop collection task."""
        logger.info("🛑 Stopping CryptoMetricsCollectorAgent...")
        self.running = False
        
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        
        logger.info("✅ CryptoMetricsCollectorAgent stopped")
    
    async def _collect_crypto_metrics(self) -> None:
        """Collect crypto market metrics from CoinGecko."""
        while self.running:
            try:
                if self.circuit_open:
                    logger.warning("Circuit breaker open - skipping collection")
                    await asyncio.sleep(60)
                    continue
                
                current_time_us = int(time.time() * 1_000_000)
                
                # Get global market data
                try:
                    global_data = await asyncio.to_thread(self.cg.get_global)
                    
                    if not global_data:
                        logger.warning("No global data returned from CoinGecko")
                        self._record_failure()
                        await asyncio.sleep(self.collection_interval_sec)
                        continue
                    
                    # Extract metrics
                    data = global_data.get('data', {})
                    
                    metrics = CryptoMarketMetrics(
                        timestamp_utc_us=current_time_us,
                        source="coingecko",
                        total_market_cap_usd=data.get('total_market_cap', {}).get('usd'),
                        total_volume_24h_usd=data.get('total_volume', {}).get('usd'),
                        btc_dominance_pct=data.get('market_cap_percentage', {}).get('btc'),
                        eth_dominance_pct=data.get('market_cap_percentage', {}).get('eth'),
                        active_cryptocurrencies=data.get('active_cryptocurrencies')
                    )
                    
                    # Get DeFi metrics if available
                    try:
                        defi_data = await asyncio.to_thread(
                            self.cg.get_global_decentralized_finance_defi
                        )
                        if defi_data:
                            metrics.defi_market_cap_usd = defi_data.get('defi_market_cap')
                            metrics.defi_volume_24h_usd = defi_data.get('trading_volume_24h')
                            metrics.defi_dominance_pct = defi_data.get('defi_dominance')
                    except Exception as e:
                        logger.debug(f"DeFi metrics not available: {e}")
                    
                    # Check for duplicates
                    if self._is_duplicate(metrics):
                        self.metrics["duplicates_detected"] += 1
                        logger.debug("Duplicate metrics detected, skipping")
                        await asyncio.sleep(self.collection_interval_sec)
                        continue
                    
                    # Publish to Kafka
                    await self.streaming_bus.publish(
                        topic="raw_data.crypto.market_metrics",
                        key="global",
                        value=asdict(metrics)
                    )
                    
                    self.metrics["metrics_collected"] += 1
                    self._record_success()
                    
                    logger.info(
                        f"🪙 Collected crypto metrics: "
                        f"Market Cap=${metrics.total_market_cap_usd/1e9:.2f}B, "
                        f"BTC Dom={metrics.btc_dominance_pct:.1f}%, "
                        f"Active={metrics.active_cryptocurrencies}"
                    )
                    
                except Exception as e:
                    logger.error(f"Error collecting crypto metrics: {e}")
                    self._record_failure()
                    self.metrics["api_errors"] += 1
                
                # Wait for next collection cycle
                await asyncio.sleep(self.collection_interval_sec)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in crypto metrics collection loop: {e}", exc_info=True)
                await asyncio.sleep(60)
    
    def _is_duplicate(self, metrics: CryptoMarketMetrics) -> bool:
        """Check if metrics are duplicate using hash."""
        # Create hash excluding timestamp (which always changes)
        metrics_dict = asdict(metrics)
        metrics_dict.pop('timestamp_utc_us', None)
        
        data_str = json.dumps(metrics_dict, sort_keys=True)
        data_hash = hashlib.sha256(data_str.encode()).hexdigest()
        
        if data_hash in self.seen_hashes:
            return True
        
        self.seen_hashes.append(data_hash)
        return False
    
    def _record_success(self) -> None:
        """Record successful API call."""
        self.failures = 0
        self.circuit_open = False
    
    def _record_failure(self) -> None:
        """Record failed API call and potentially open circuit breaker."""
        self.failures += 1
        
        if self.failures >= self.failure_threshold:
            self.circuit_open = True
            logger.error("Circuit breaker opened for CoinGecko API")
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get agent health status."""
        return {
            "running": self.running,
            "metrics": self.metrics,
            "circuit_open": self.circuit_open,
            "failures": self.failures,
            "api_available": COINGECKO_AVAILABLE and self.cg is not None,
        }
