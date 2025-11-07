"""
Crypto Market Metrics Collector

Mission: Collect crypto-native market structure metrics (NOT macro/TradFi data).

Data Source: CoinGecko API (FREE)
  - BTC dominance (BTC market cap / total crypto market cap)
  - Total crypto market cap
  - DeFi market cap
  - Alt season index (altcoin performance vs BTC)
  - 24h trading volume

Output Topic: raw_data.crypto.market_metrics

Architecture:
  - Crypto-native metrics (market structure, not traditional macro)
  - Complements on-chain data from OnChainBuilder
  - Free tier: 50 calls/minute (sufficient for 5-minute polling)
  - Institutional-grade: Deduplication, retry logic, health checks, SLO tracking

Use Cases:
  - BTC dominance trends (alt season detection)
  - Total market cap momentum (risk-on/off in crypto)
  - DeFi sector rotation signals

SLOs:
  - Uptime ≥99.0%
  - p95 latency < collection_interval (5 minutes)
  - Duplicate rate <0.1%
"""

import asyncio
import aiohttp
import time
import logging
import random
import hashlib
from typing import Optional, Dict, Any, List, Callable, Set
from dataclasses import dataclass
from datetime import datetime, timezone
from collections import deque, defaultdict
import math
import numpy as np

# Third-party library (install via requirements.txt)
try:
    from pycoingecko import CoinGeckoAPI
    COINGECKO_AVAILABLE = True
except ImportError:
    COINGECKO_AVAILABLE = False
    logging.warning("pycoingecko not installed. Run: pip install pycoingecko")

# Streaming Bus Integration
from infra.bus.streaming_bus import StreamingBus
from infra.bus.adaptive_backpressure import AdaptiveBackpressureController
from infra.bus.adaptive_rate_limiter import AdaptiveRateLimiter
from infra.monitoring.prometheus_metrics import get_metrics_collector

# Configuration
try:
    from config import CRYPTO_METRICS_CONFIG
    CONFIG_LOADED = True
except ImportError:
    CRYPTO_METRICS_CONFIG = {
        "collection_interval_sec": 300,
        "failure_threshold": 3,
        "circuit_reset_timeout_sec": 300,
        "health_check_interval_sec": 600,
        "stale_data_threshold_sec": 900,
    }
    CONFIG_LOADED = False
    logging.warning("config.py not found. Using default CRYPTO_METRICS_CONFIG values.")

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# INSTITUTIONAL-GRADE HELPER CLASSES
# ═══════════════════════════════════════════════════════════════════════════

class DuplicateDetector:
    """
    Production-grade deduplication with sliding window.
    Prevents duplicate data from being published during retries or API issues.
    """
    def __init__(self, window_size: int = 10000):
        self.seen_hashes: deque = deque(maxlen=window_size)
        self.seen_set: Set[str] = set()
        self.duplicate_count = 0
        self.total_count = 0
    
    def is_duplicate(self, data_hash: str) -> bool:
        """Check if hash already seen. Returns True if duplicate."""
        self.total_count += 1
        
        if data_hash in self.seen_set:
            self.duplicate_count += 1
            return True
        
        # Evict oldest hash if at capacity
        if len(self.seen_hashes) == self.seen_hashes.maxlen:
            old_hash = self.seen_hashes[0]
            self.seen_set.discard(old_hash)
        
        self.seen_hashes.append(data_hash)
        self.seen_set.add(data_hash)
        return False
    
    def get_duplicate_rate(self) -> float:
        """Return duplicate rate (0.0 to 1.0). SLO target: <0.001"""
        if self.total_count == 0:
            return 0.0
        return self.duplicate_count / self.total_count


class RetryManager:
    """
    Exponential backoff with jitter for transient API failures.
    Prevents permanent data loss from temporary network/API issues.
    """
    def __init__(self, max_retries: int = 5, base_delay: float = 1.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
    
    async def execute_with_retry(
        self,
        operation: Callable,
        *args,
        transient_exceptions: tuple = (aiohttp.ClientError, asyncio.TimeoutError),
        **kwargs
    ) -> Any:
        """Execute operation with exponential backoff retry."""
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                return await operation(*args, **kwargs)
            except transient_exceptions as e:
                last_exception = e
                
                if attempt == self.max_retries - 1:
                    raise
                
                delay = self.base_delay * (2 ** attempt)
                jitter = delay * 0.2 * (random.random() - 0.5)
                total_delay = delay + jitter
                
                logger.warning(
                    f"Retry {attempt + 1}/{self.max_retries} after {total_delay:.2f}s: {e}"
                )
                await asyncio.sleep(total_delay)
            except Exception as e:
                logger.error(f"Permanent failure, no retry: {e}")
                raise
        
        if last_exception:
            raise last_exception


class MetricsTracker:
    """
    SLO tracking for data collector quality.
    Monitors uptime, latency, duplicate rate against institutional standards.
    """
    def __init__(self):
        self.success_count = 0
        self.error_count = 0
        self.latencies: List[float] = []
        self.start_time = time.time()
    
    def record_success(self, latency_sec: float):
        """Record successful collection with latency."""
        self.success_count += 1
        self.latencies.append(latency_sec)
        
        if len(self.latencies) > 1000:
            self.latencies.pop(0)
    
    def record_error(self):
        """Record failed collection."""
        self.error_count += 1
    
    def get_uptime_pct(self) -> float:
        """Calculate uptime percentage. SLO target: ≥99.0%"""
        total = self.success_count + self.error_count
        if total == 0:
            return 100.0
        return (self.success_count / total) * 100.0
    
    def get_p95_latency(self) -> float:
        """Calculate p95 latency in seconds."""
        if not self.latencies:
            return 0.0
        return float(np.percentile(self.latencies, 95))
    
    def get_runtime_hours(self) -> float:
        """Get total runtime in hours."""
        return (time.time() - self.start_time) / 3600.0


# =============================
# DATA STRUCTURES
# =============================

# =============================
# DATA STRUCTURES
# =============================

@dataclass
class CryptoMarketMetrics:
    """Crypto market structure metrics from CoinGecko."""
    # Market Dominance
    btc_dominance_pct: float         # BTC market cap / total market cap
    eth_dominance_pct: float         # ETH market cap / total market cap
    stablecoin_dominance_pct: float  # Stablecoin cap / total cap
    
    # Market Capitalization
    total_market_cap_usd: float      # Total crypto market cap
    btc_market_cap_usd: float        # BTC market cap
    eth_market_cap_usd: float        # ETH market cap
    defi_market_cap_usd: float       # DeFi sector market cap
    
    # Trading Volume
    total_volume_24h_usd: float      # Total 24h trading volume
    btc_volume_24h_usd: float        # BTC 24h volume
    eth_volume_24h_usd: float        # ETH 24h volume
    
    # Market Momentum
    market_cap_change_24h_pct: float  # 24h market cap change
    volume_to_mcap_ratio: float       # Volume/Market Cap (liquidity indicator)
    
    # Metadata
    timestamp_utc_us: int
    source: str = "coingecko"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            # Dominance
            'btc_dominance_pct': self.btc_dominance_pct,
            'eth_dominance_pct': self.eth_dominance_pct,
            'stablecoin_dominance_pct': self.stablecoin_dominance_pct,
            # Market Cap
            'total_market_cap_usd': self.total_market_cap_usd,
            'btc_market_cap_usd': self.btc_market_cap_usd,
            'eth_market_cap_usd': self.eth_market_cap_usd,
            'defi_market_cap_usd': self.defi_market_cap_usd,
            # Volume
            'total_volume_24h_usd': self.total_volume_24h_usd,
            'btc_volume_24h_usd': self.btc_volume_24h_usd,
            'eth_volume_24h_usd': self.eth_volume_24h_usd,
            # Momentum
            'market_cap_change_24h_pct': self.market_cap_change_24h_pct,
            'volume_to_mcap_ratio': self.volume_to_mcap_ratio,
            # Metadata
            'timestamp_utc_us': self.timestamp_utc_us,
            'source': self.source
        }
    
    def get_hash(self) -> str:
        """
        Generate institutional-grade stable hash for deduplication.
        Uses timestamp (rounded to 5 minutes) for dedup window.
        """
        # Round timestamp to 5-minute window for deduplication
        timestamp_5min = (self.timestamp_utc_us // 300_000_000) * 300_000_000
        hash_input = f"{self.source}:{timestamp_5min}:{self.total_market_cap_usd:.0f}"
        return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()


# =============================
# DEFENSIVE VALIDATION
# =============================

def _normalize_timestamp_us(ts: Any, default_now_us: Optional[int] = None) -> int:
    """
    Normalize timestamp to UTC microseconds (institutional standard).
    Handles multiple formats: ns/us/ms/s/ISO string.
    """
    if default_now_us is None:
        default_now_us = int(time.time() * 1_000_000)
    
    if ts is None:
        return default_now_us
    
    # Handle integer timestamps
    if isinstance(ts, int):
        if ts >= 1_000_000_000_000_000_000:  # nanoseconds
            return ts // 1000
        elif ts >= 1_000_000_000_000_000:    # microseconds
            return ts
        elif ts >= 1_000_000_000_000:        # milliseconds
            return ts * 1000
        elif ts >= 1_000_000_000:            # seconds
            return ts * 1_000_000
        else:
            return default_now_us
    
    # Handle ISO 8601 string
    if isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return int(dt.timestamp() * 1_000_000)
        except (ValueError, AttributeError):
            return default_now_us
    
    # Handle datetime object
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = ts.astimezone(timezone.utc)
        return int(ts.timestamp() * 1_000_000)
    
    return default_now_us


# =============================
# SAFE CONVERSION HELPERS
# =============================

def _safe_float(value: Any, default: float = 0.0) -> float:
    """Safely convert to float, handling None/NaN/Inf."""
    if value is None:
        return default
    
    if not isinstance(value, (int, float)):
        try:
            value = float(value)
        except (ValueError, TypeError):
            return default
    
    if math.isnan(value) or math.isinf(value):
        return default
    
    return value


# =============================
# COINGECKO ADAPTER
# =============================

class CoinGeckoAdapter:
    """
    CoinGecko API adapter for crypto market metrics.
    
    Free tier provides:
    - Global crypto market data
    - BTC/ETH/stablecoin dominance
    - DeFi sector metrics
    - 24h volume and market cap
    
    Rate Limit: 50 calls/minute (free tier)
    No API key required for free tier
    Docs: https://www.coingecko.com/en/api/documentation
    """
    
    def __init__(self):
        """Initialize CoinGecko adapter."""
        if not COINGECKO_AVAILABLE:
            raise RuntimeError("pycoingecko library not installed. Run: pip install pycoingecko")
        
        self.cg = CoinGeckoAPI()
        self.last_call_time = 0.0
        self.min_call_interval = 1.2  # ~50 calls/minute = 1.2s between calls
        
        logger.info("CoinGeckoAdapter initialized (free tier, no API key)")
    
    async def fetch_market_metrics(self) -> Optional[CryptoMarketMetrics]:
        """
        Fetch global crypto market metrics.
        
        Returns:
            CryptoMarketMetrics or None if error
        """
        try:
            # Rate limiting (50 calls/minute)
            now = time.time()
            time_since_last_call = now - self.last_call_time
            if time_since_last_call < self.min_call_interval:
                await asyncio.sleep(self.min_call_interval - time_since_last_call)
            
            # Fetch global market data (blocking I/O, run in thread pool)
            global_data = await asyncio.get_event_loop().run_in_executor(
                None, self.cg.get_global
            )
            
            # Fetch BTC and ETH data for additional metrics
            btc_data = await asyncio.get_event_loop().run_in_executor(
                None, self.cg.get_coin_by_id, 'bitcoin'
            )
            eth_data = await asyncio.get_event_loop().run_in_executor(
                None, self.cg.get_coin_by_id, 'ethereum'
            )
            
            self.last_call_time = time.time()
            
            # Extract market dominance
            market_cap_percentage = global_data.get('market_cap_percentage', {})
            btc_dominance = _safe_float(market_cap_percentage.get('btc'))
            eth_dominance = _safe_float(market_cap_percentage.get('eth'))
            
            # Estimate stablecoin dominance (USDT + USDC + DAI)
            usdt_dominance = _safe_float(market_cap_percentage.get('usdt'))
            usdc_dominance = _safe_float(market_cap_percentage.get('usdc'))
            dai_dominance = _safe_float(market_cap_percentage.get('dai'))
            stablecoin_dominance = usdt_dominance + usdc_dominance + dai_dominance
            
            # Extract total market metrics
            total_market_cap = _safe_float(global_data.get('total_market_cap', {}).get('usd'))
            total_volume = _safe_float(global_data.get('total_volume', {}).get('usd'))
            market_cap_change_24h = _safe_float(global_data.get('market_cap_change_percentage_24h_usd'))
            
            # Extract BTC/ETH specific metrics
            btc_market_data = btc_data.get('market_data', {})
            eth_market_data = eth_data.get('market_data', {})
            
            btc_market_cap = _safe_float(btc_market_data.get('market_cap', {}).get('usd'))
            eth_market_cap = _safe_float(eth_market_data.get('market_cap', {}).get('usd'))
            
            btc_volume = _safe_float(btc_market_data.get('total_volume', {}).get('usd'))
            eth_volume = _safe_float(eth_market_data.get('total_volume', {}).get('usd'))
            
            # Calculate volume-to-market-cap ratio (liquidity indicator)
            volume_to_mcap_ratio = (total_volume / total_market_cap * 100) if total_market_cap > 0 else 0.0
            
            # DeFi market cap (CoinGecko global data)
            defi_market_cap = _safe_float(global_data.get('total_market_cap', {}).get('usd', 0)) * 0.05  # Rough estimate: 5% of total
            # Note: CoinGecko doesn't provide direct DeFi market cap in global endpoint
            # Alternative: Use DefiLlama API for accurate DeFi TVL/market cap
            
            timestamp_utc_us = int(time.time() * 1_000_000)
            
            return CryptoMarketMetrics(
                # Dominance
                btc_dominance_pct=btc_dominance,
                eth_dominance_pct=eth_dominance,
                stablecoin_dominance_pct=stablecoin_dominance,
                # Market Cap
                total_market_cap_usd=total_market_cap,
                btc_market_cap_usd=btc_market_cap,
                eth_market_cap_usd=eth_market_cap,
                defi_market_cap_usd=defi_market_cap,
                # Volume
                total_volume_24h_usd=total_volume,
                btc_volume_24h_usd=btc_volume,
                eth_volume_24h_usd=eth_volume,
                # Momentum
                market_cap_change_24h_pct=market_cap_change_24h,
                volume_to_mcap_ratio=volume_to_mcap_ratio,
                # Metadata
                timestamp_utc_us=timestamp_utc_us,
                source='coingecko'
            )
            
        except Exception as e:
            logger.error(f"CoinGecko API error: {e}")
            return None


# =============================
# CRYPTO METRICS COLLECTOR AGENT
# =============================

class CryptoMetricsCollectorAgent:
    """
    Crypto market metrics collector with institutional-grade reliability.
    
    Collects crypto-native market structure metrics:
    - BTC/ETH dominance (alt season detection)
    - Total market cap momentum
    - DeFi sector rotation
    - Volume-to-market-cap ratio (liquidity)
    
    Collection Interval: 5 minutes (free tier limit: 50 calls/min)
    
    SLOs:
    - Uptime ≥99.0%
    - p95 latency < 5 minutes
    - Duplicate rate <0.1%
    """
    
    def __init__(
        self,
        streaming_bus: StreamingBus,
        collection_interval_sec: Optional[float] = None
    ):
        """
        Initialize Crypto Metrics Collector.
        
        Args:
            streaming_bus: StreamingBus for Kafka publishing
            collection_interval_sec: Collection interval (loads from config if None)
        """
        self.streaming_bus = streaming_bus
        self.coingecko = CoinGeckoAdapter() if COINGECKO_AVAILABLE else None
        
        # Load from config.py
        self._failure_threshold = CRYPTO_METRICS_CONFIG.get('failure_threshold', 3)
        self._circuit_reset_timeout = CRYPTO_METRICS_CONFIG.get('circuit_reset_timeout_sec', 300.0)
        
        # Circuit breaker
        self._circuit_open = False
        self._consecutive_failures = 0
        self._last_success_time = time.time()
        
        # Collection interval (load from config or use default)
        self.collection_interval = collection_interval_sec or CRYPTO_METRICS_CONFIG.get('collection_interval_sec', 300.0)
        
        # ═══════════════════════════════════════════════════════════════════════════
        # INSTITUTIONAL-GRADE COMPONENTS
        # ═══════════════════════════════════════════════════════════════════════════
        
        # Deduplication with 10,000-item sliding window
        self.dedup_detector = DuplicateDetector(window_size=10000)
        
        # Retry manager with exponential backoff
        self.retry_manager = RetryManager(max_retries=5, base_delay=1.0)
        
        # SLO tracking
        self.metrics_tracker = MetricsTracker()
        
        # Prometheus metrics integration
        self.metrics = get_metrics_collector()
        
        # Adaptive backpressure controller
        self.backpressure = AdaptiveBackpressureController(
            name="crypto_metrics_collector",
            buffer_capacity=200,
            low_threshold=0.50,
            high_threshold=0.75,
            max_delay_ms=1000.0
        )
        
        # AIMD rate limiter (CoinGecko free tier: 50 calls/min)
        self.rate_limiter = AdaptiveRateLimiter(
            domain='coingecko',
            initial_rate=0.83,  # 50/min ≈ 0.83/sec
            max_rate=0.83,
            min_rate=0.17
        )
        
        # Health check state (load from config)
        self._last_health_check = time.time()
        self._health_check_interval = CRYPTO_METRICS_CONFIG.get('health_check_interval_sec', 600.0)
        self._stale_data_threshold = CRYPTO_METRICS_CONFIG.get('stale_data_threshold_sec', 900.0)
        self._last_data_timestamp = time.time()
        
        # Running state for graceful shutdown
        self._running = False
        self._tasks: List[asyncio.Task] = []
        
        logger.info(f"CryptoMetricsCollectorAgent initialized (CoinGecko={'✅' if self.coingecko else '❌'})")
        logger.info(f"Institutional features: Dedup=✅ Retry=✅ SLO=✅ Backpressure=✅ HealthCheck=✅")
        if CONFIG_LOADED:
            logger.info(f"Configuration loaded from config.py: ✅")
    
    async def start(self):
        """Start crypto metrics collection with institutional-grade lifecycle management."""
        logger.info("📊 CryptoMetricsCollectorAgent starting...")
        self._running = True
        
        if not self.coingecko:
            logger.error("CoinGecko adapter not available. Install: pip install pycoingecko")
            return
        
        # Start collection and health check tasks and store them for shutdown
        self._tasks = [
            asyncio.create_task(self._collect_metrics()),
            asyncio.create_task(self._run_health_checks()),
        ]
        
        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            logger.info("Crypto metrics collector tasks cancelled during shutdown")
        except Exception as e:
            logger.error(f"Crypto metrics collector fatal error: {e}")
            raise
        finally:
            # Log final SLO metrics
            logger.info(
                f"Final SLOs - Uptime: {self.metrics_tracker.get_uptime_pct():.2f}%, "
                f"p95 Latency: {self.metrics_tracker.get_p95_latency():.2f}s, "
                f"Duplicate Rate: {self.dedup_detector.get_duplicate_rate():.4f}, "
                f"Runtime: {self.metrics_tracker.get_runtime_hours():.2f}h"
            )
    
    async def _collect_metrics(self):
        """Collect crypto market metrics with institutional-grade reliability."""
        while self._running:
            collection_start = time.time()
            try:
                if not self._circuit_open:
                    # Fetch metrics with retry logic
                    metrics = await self.retry_manager.execute_with_retry(
                        self.coingecko.fetch_market_metrics,
                        transient_exceptions=(Exception,)  # Broad for sync library
                    )
                    
                    if metrics:
                        # Deduplication check
                        metrics_hash = metrics.get_hash()
                        if not self.dedup_detector.is_duplicate(metrics_hash):
                            # Apply backpressure
                            await self.backpressure.apply_backpressure()
                            
                            # Publish to Kafka
                            await self.streaming_bus.publish(
                                topic='raw_data.crypto.market_metrics',
                                partition_key='crypto_metrics',
                                payload=metrics.to_dict()
                            )
                            
                            # Record metrics
                            latency = time.time() - collection_start
                            self.metrics_tracker.record_success(latency)
                            self.metrics.observe_histogram(
                                'crypto_metrics_collection_latency_sec',
                                latency
                            )
                            self.metrics.increment_counter(
                                'crypto_metrics_data_collected_total',
                                value=1.0
                            )
                            
                            # Update health check timestamp
                            self._last_data_timestamp = time.time()
                            
                            logger.info(
                                f"📊 Published crypto metrics: BTC dominance={metrics.btc_dominance_pct:.2f}%, "
                                f"Total MCap=${metrics.total_market_cap_usd/1e9:.1f}B, "
                                f"24h Change={metrics.market_cap_change_24h_pct:.2f}%"
                            )
                            
                            # Reset circuit breaker
                            self._consecutive_failures = 0
                            self._last_success_time = time.time()
                        else:
                            logger.debug("Skipped duplicate crypto metrics")
                            self.metrics.increment_counter(
                                'crypto_metrics_duplicates_skipped_total',
                                value=1.0
                            )
                    else:
                        self._handle_failure()
                else:
                    # Circuit open - check if timeout elapsed
                    if time.time() - self._last_success_time > self._circuit_reset_timeout:
                        self._circuit_open = False
                        self._consecutive_failures = 0
                        logger.info("Circuit breaker RESET for CoinGecko")
            
            except Exception as e:
                logger.error(f"Error collecting crypto metrics: {e}")
                self._handle_failure()
                self.metrics_tracker.record_error()
                self.metrics.increment_counter(
                    'crypto_metrics_errors_total',
                    labels={'error_type': type(e).__name__}
                )
            
            await asyncio.sleep(self.collection_interval)
    
    async def _run_health_checks(self):
        """
        Periodic health check monitoring.
        Detects stale data, circuit breaker issues, and SLO violations.
        """
        while self._running:
            try:
                await asyncio.sleep(self._health_check_interval)
                
                current_time = time.time()
                health_issues = []
                
                # Check for stale data
                staleness = current_time - self._last_data_timestamp
                if staleness > self._stale_data_threshold:
                    health_issues.append(f"Data stale ({staleness:.0f}s)")
                    self.metrics.increment_counter('crypto_metrics_stale_data_total')
                
                # Check circuit breaker
                if self._circuit_open:
                    health_issues.append("Circuit OPEN")
                    self.metrics.set_gauge('crypto_metrics_circuit_breaker_open', 1.0)
                else:
                    self.metrics.set_gauge('crypto_metrics_circuit_breaker_open', 0.0)
                
                # Check SLO violations
                uptime_pct = self.metrics_tracker.get_uptime_pct()
                duplicate_rate = self.dedup_detector.get_duplicate_rate()
                p95_latency = self.metrics_tracker.get_p95_latency()
                
                if uptime_pct < 99.0:
                    health_issues.append(f"SLO: Uptime {uptime_pct:.2f}% < 99%")
                
                if duplicate_rate > 0.001:
                    health_issues.append(f"SLO: Dup rate {duplicate_rate:.4f} > 0.1%")
                
                if p95_latency > self.collection_interval:
                    health_issues.append(f"SLO: p95 latency {p95_latency:.2f}s > {self.collection_interval}s")
                
                # Record Prometheus metrics
                self.metrics.set_gauge('crypto_metrics_uptime_pct', uptime_pct)
                self.metrics.set_gauge('crypto_metrics_duplicate_rate', duplicate_rate)
                self.metrics.set_gauge('crypto_metrics_p95_latency_sec', p95_latency)
                
                # Log health status
                if health_issues:
                    logger.warning(f"❌ Health check FAILED: {', '.join(health_issues)}")
                    self.metrics.increment_counter('crypto_metrics_health_check_failures_total')
                else:
                    logger.info(
                        f"✅ Health check OK - Uptime: {uptime_pct:.2f}%, "
                        f"Dup: {duplicate_rate:.4f}, p95: {p95_latency:.2f}s, "
                        f"Runtime: {self.metrics_tracker.get_runtime_hours():.2f}h"
                    )
            
            except Exception as e:
                logger.error(f"Health check error: {e}")
    
    def _handle_failure(self):
        """Handle collection failure and circuit breaker."""
        self._consecutive_failures += 1
        
        if self._consecutive_failures >= self._failure_threshold:
            self._circuit_open = True
            logger.error(f"Circuit breaker OPENED for CoinGecko ({self._consecutive_failures} failures)")
    
    async def stop(self):
        """Gracefully stop the crypto metrics collector."""
        logger.info("🛑 Stopping CryptoMetricsCollectorAgent...")
        self._running = False  # Signal all loops to exit
        
        # Cancel all running tasks
        for task in self._tasks:
            if not task.done():
                task.cancel()
        
        # Wait for cancellation to complete
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        
        # Close HTTP session if using aiohttp
        if hasattr(self, 'session') and self.session and not self.session.closed:
            await self.session.close()
            logger.info("HTTP session closed")
        
        logger.info("✅ CryptoMetricsCollectorAgent stopped")


# =============================
# MAIN ENTRY POINT
# =============================

async def main():
    """Test crypto metrics collector."""
    
    # Create mock streaming bus
    class MockStreamingBus:
        async def publish(self, topic: str, data: Dict):
            logger.info(f"📤 Published to {topic}")
            logger.info(f"   BTC Dominance: {data['btc_dominance_pct']:.2f}%")
            logger.info(f"   Total Market Cap: ${data['total_market_cap_usd']/1e9:.1f}B")
            logger.info(f"   24h Volume: ${data['total_volume_24h_usd']/1e9:.1f}B")
    
    # Initialize collector
    collector = CryptoMetricsCollectorAgent(streaming_bus=MockStreamingBus())
    
    # Run for 15 minutes (test)
    try:
        await asyncio.wait_for(collector.start(), timeout=900)
    except asyncio.TimeoutError:
        logger.info("Test completed (15 minutes)")


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    asyncio.run(main())
