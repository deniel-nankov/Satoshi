"""
Macro/TradFi Data Collector Agent

Mission: Collect traditional finance and macroeconomic data for crypto correlation analysis.

Data Sources:
  1. FRED API (Federal Reserve) - FREE
     - Federal Funds Rate, CPI, GDP, Unemployment
     - Official government data, daily/monthly updates
     
  2. Alpha Vantage - $49/month Premium
     - VIX (Volatility Index) - CRITICAL for risk-on/off detection
     - DXY (Dollar Index) - Primary crypto inverse correlation
     - GLD (Gold), USO (Oil) - Safe haven/risk indicators
     - Real-time quotes (no delay)
     
  3. Yahoo Finance - FREE (Fallback/Validation)
     - SPY, QQQ, TLT - Equity market correlation
     - 15-minute delay (acceptable for 1h+ strategies)
     - Fallback when Alpha Vantage hits rate limits

Output Topics:
  - raw_data.macro.economic_indicators (FRED data: Fed rate, CPI, GDP)
  - raw_data.tradfi.indices (Alpha Vantage: VIX, DXY)
  - raw_data.tradfi.equities (Alpha Vantage/Yahoo: SPY, QQQ, TLT)
  - raw_data.tradfi.commodities (Alpha Vantage: GLD, USO)

Architecture:
  - Multi-source redundancy (Alpha Vantage primary, Yahoo Finance fallback)
  - Circuit breaker per data source
  - Exponential backoff with jitter for retries
  - Deduplication via stable hashing
  - Adaptive rate limiting (AIMD)
  - Health checks and SLO tracking
  - Prometheus metrics integration

SLOs:
  - Uptime ≥99.0%
  - p95 latency < collection interval
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
from decimal import Decimal
from collections import deque, defaultdict
import json
import math
import numpy as np

# Third-party libraries (install via requirements.txt)
try:
    from fredapi import Fred
    FRED_AVAILABLE = True
except ImportError:
    FRED_AVAILABLE = False
    logging.warning("fredapi not installed. Run: pip install fredapi")

try:
    import yfinance as yf
    YAHOO_AVAILABLE = True
except ImportError:
    YAHOO_AVAILABLE = False
    logging.warning("yfinance not installed. Run: pip install yfinance")

# Streaming Bus Integration
from infra.bus.streaming_bus import StreamingBus
from infra.bus.adaptive_backpressure import AdaptiveBackpressureController
from infra.bus.adaptive_rate_limiter import AdaptiveRateLimiter
from infra.monitoring.prometheus_metrics import get_metrics_collector

# Configuration
try:
    from config import MACRO_CONFIG
    CONFIG_LOADED = True
except ImportError:
    MACRO_CONFIG = {
        "collection_interval_sec": 60,
        "fred_interval_sec": 3600,
        "failure_threshold": 5,
        "circuit_reset_timeout_sec": 300,
        "health_check_interval_sec": 300,
        "stale_data_threshold_sec": 600,
        "indices_symbols": ["^VIX", "DXY"],
        "equities_symbols": ["SPY", "QQQ", "TLT"],
        "commodities_symbols": ["GLD", "USO"],
    }
    CONFIG_LOADED = False
    logging.warning("config.py not found. Using default MACRO_CONFIG values.")

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# INSTITUTIONAL-GRADE HELPER CLASSES
# ═══════════════════════════════════════════════════════════════════════════

class DuplicateDetector:
    """
    Production-grade deduplication with sliding window.
    Prevents duplicate data from being published during failovers, retries, or source overlaps.
    
    Architecture:
      - Fixed-size sliding window (10,000 items)
      - O(1) duplicate check via set
      - Automatic eviction of old hashes
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
            old_hash = self.seen_hashes[0]  # Will be evicted on next append
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
    
    Architecture:
      - Base delay: 1 second
      - Exponential backoff: 2^attempt
      - Jitter: ±20% randomization to prevent thundering herd
      - Max retries: 5 (prevents infinite loops)
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
        """
        Execute operation with exponential backoff retry.
        
        Args:
            operation: Async callable to retry
            transient_exceptions: Exceptions that should trigger retry
            *args, **kwargs: Passed to operation
        
        Returns:
            Operation result
        
        Raises:
            Last exception if all retries exhausted
        """
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                return await operation(*args, **kwargs)
            except transient_exceptions as e:
                last_exception = e
                
                if attempt == self.max_retries - 1:
                    # Final attempt failed
                    raise
                
                # Calculate delay with exponential backoff + jitter
                delay = self.base_delay * (2 ** attempt)
                jitter = delay * 0.2 * (random.random() - 0.5)  # ±10%
                total_delay = delay + jitter
                
                logger.warning(
                    f"Retry {attempt + 1}/{self.max_retries} after {total_delay:.2f}s: {e}"
                )
                await asyncio.sleep(total_delay)
            except Exception as e:
                # Permanent error (e.g., authentication failure)
                logger.error(f"Permanent failure, no retry: {e}")
                raise
        
        # Should never reach here, but satisfy type checker
        if last_exception:
            raise last_exception


class MetricsTracker:
    """
    SLO tracking for data collector quality.
    Monitors uptime, latency, duplicate rate against institutional standards.
    
    SLO Targets:
      - Uptime: ≥99.0%
      - p95 Latency: < collection_interval
      - Duplicate Rate: <0.1%
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
        
        # Keep last 1000 latencies for p95 calculation
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

@dataclass
class MacroIndicator:
    """Economic indicator from FRED (Federal Reserve Economic Data)."""
    series_id: str           # e.g. 'DFF' (Federal Funds Rate)
    name: str                # Human-readable name
    value: float             # Latest value
    timestamp_utc_us: int    # Observation timestamp (UTC microseconds)
    frequency: str           # 'daily', 'monthly', 'quarterly'
    units: str               # e.g. 'percent', 'billions_usd'
    source: str = "fred"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'series_id': self.series_id,
            'name': self.name,
            'value': self.value,
            'timestamp_utc_us': self.timestamp_utc_us,
            'frequency': self.frequency,
            'units': self.units,
            'source': self.source
        }
    
    def get_hash(self) -> str:
        """
        Generate institutional-grade stable hash for deduplication.
        Uses series_id + observation timestamp for unique identification.
        """
        hash_input = f"{self.source}:{self.series_id}:{self.timestamp_utc_us}:{self.value}"
        return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()


@dataclass
class TradFiQuote:
    """Real-time quote from Alpha Vantage or Yahoo Finance."""
    symbol: str              # e.g. 'SPY', 'VIX', 'DXY'
    price: float             # Current price
    change_pct: float        # Percent change (daily)
    volume: Optional[int]    # Trading volume (if available)
    timestamp_utc_us: int    # Quote timestamp (UTC microseconds)
    source: str              # 'alpha_vantage', 'yahoo_finance', 'iex_cloud'
    latency_sec: float       # Data latency (0 = real-time, 900 = 15-min delay)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'symbol': self.symbol,
            'price': self.price,
            'change_pct': self.change_pct,
            'volume': self.volume,
            'timestamp_utc_us': self.timestamp_utc_us,
            'source': self.source,
            'latency_sec': self.latency_sec
        }
    
    def get_hash(self) -> str:
        """
        Generate institutional-grade stable hash for deduplication.
        Uses symbol + timestamp (rounded to minute) for dedup window.
        """
        # Round timestamp to minute for dedup window
        timestamp_minute = (self.timestamp_utc_us // 60_000_000) * 60_000_000
        hash_input = f"{self.source}:{self.symbol}:{timestamp_minute}:{self.price}"
        return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()


# =============================
# DEFENSIVE VALIDATION
# =============================

def _normalize_timestamp_us(ts: Any, default_now_us: Optional[int] = None) -> int:
    """
    Normalize timestamp to UTC microseconds (institutional standard).
    Handles multiple formats: ns/us/ms/s/ISO string.
    
    Args:
        ts: Timestamp in various formats
        default_now_us: Default value if conversion fails (current time if None)
    
    Returns:
        Timestamp in UTC microseconds
    """
    if default_now_us is None:
        default_now_us = int(time.time() * 1_000_000)
    
    if ts is None:
        return default_now_us
    
    # Handle integer timestamps (ns/us/ms/s)
    if isinstance(ts, int):
        if ts >= 1_000_000_000_000_000_000:  # nanoseconds (>=2001)
            return ts // 1000
        elif ts >= 1_000_000_000_000_000:    # microseconds (>=2001)
            return ts
        elif ts >= 1_000_000_000_000:        # milliseconds (>=2001)
            return ts * 1000
        elif ts >= 1_000_000_000:            # seconds (>=2001)
            return ts * 1_000_000
        else:
            # Too small, probably invalid
            return default_now_us
    
    # Handle ISO 8601 string
    if isinstance(ts, str):
        try:
            # Replace 'Z' with '+00:00' for ISO parsing
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            # Ensure UTC
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


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Safely convert to float, handling None/NaN/Inf."""
    if value is None:
        return default
    
    if isinstance(value, Decimal):
        try:
            value = float(value)
        except (ValueError, OverflowError):
            return default
    
    if not isinstance(value, (int, float)):
        try:
            value = float(value)
        except (ValueError, TypeError):
            return default
    
    if math.isnan(value) or math.isinf(value):
        return default
    
    return value


def _safe_int(value: Any, default: int = 0) -> int:
    """Safely convert to int, handling None/invalid values."""
    if value is None:
        return default
    
    if not isinstance(value, int):
        try:
            value = int(value)
        except (ValueError, TypeError):
            return default
    
    return value


# =============================
# FRED API ADAPTER
# =============================

class FREDAdapter:
    """
    Federal Reserve Economic Data (FRED) API adapter.
    
    Free API providing official US economic indicators:
    - Federal Funds Rate (DFF)
    - Consumer Price Index (CPIAUCSL)
    - Unemployment Rate (UNRATE)
    - GDP (GDP)
    - 10-Year Treasury Rate (DGS10)
    
    Rate Limit: Unlimited (no documented limits)
    Data Frequency: Daily to Quarterly (depends on series)
    Sign up: https://fred.stlouisfed.org/docs/api/api_key.html
    """
    
    def __init__(self, api_key: str):
        """
        Initialize FRED adapter.
        
        Args:
            api_key: FRED API key (free from fred.stlouisfed.org)
        """
        if not FRED_AVAILABLE:
            raise RuntimeError("fredapi library not installed. Run: pip install fredapi")
        
        self.api_key = api_key
        self.fred = Fred(api_key=api_key)
        self.series_metadata = {
            'DFF': {'name': 'Federal Funds Rate', 'units': 'percent', 'frequency': 'daily'},
            'CPIAUCSL': {'name': 'Consumer Price Index', 'units': 'index_1982_84=100', 'frequency': 'monthly'},
            'UNRATE': {'name': 'Unemployment Rate', 'units': 'percent', 'frequency': 'monthly'},
            'GDP': {'name': 'Gross Domestic Product', 'units': 'billions_usd', 'frequency': 'quarterly'},
            'DGS10': {'name': '10-Year Treasury Rate', 'units': 'percent', 'frequency': 'daily'},
            'M2SL': {'name': 'M2 Money Supply', 'units': 'billions_usd', 'frequency': 'monthly'},
        }
        
        logger.info(f"FREDAdapter initialized with {len(self.series_metadata)} series")
    
    def fetch_indicator(self, series_id: str) -> Optional[MacroIndicator]:
        """
        Fetch latest value for a FRED series.
        
        Args:
            series_id: FRED series ID (e.g. 'DFF', 'CPIAUCSL')
        
        Returns:
            MacroIndicator with latest value, or None if error
        """
        try:
            # Fetch series data
            series = self.fred.get_series(series_id)
            
            if series is None or len(series) == 0:
                logger.warning(f"No data for FRED series {series_id}")
                return None
            
            # Get latest observation
            latest_value = float(series.iloc[-1])
            latest_date = series.index[-1]
            
            # Convert to UTC microseconds
            timestamp_utc_us = int(latest_date.timestamp() * 1_000_000)
            
            # Get metadata
            metadata = self.series_metadata.get(series_id, {
                'name': series_id,
                'units': 'unknown',
                'frequency': 'unknown'
            })
            
            return MacroIndicator(
                series_id=series_id,
                name=metadata['name'],
                value=_safe_float(latest_value),
                timestamp_utc_us=timestamp_utc_us,
                frequency=metadata['frequency'],
                units=metadata['units'],
                source='fred'
            )
            
        except Exception as e:
            logger.error(f"FRED API error for series {series_id}: {e}")
            return None
    
    def fetch_all_indicators(self) -> List[MacroIndicator]:
        """Fetch all configured FRED series."""
        indicators = []
        
        for series_id in self.series_metadata.keys():
            indicator = self.fetch_indicator(series_id)
            if indicator:
                indicators.append(indicator)
        
        logger.info(f"Fetched {len(indicators)}/{len(self.series_metadata)} FRED indicators")
        return indicators


# =============================
# ALPHA VANTAGE API ADAPTER
# =============================

class AlphaVantageAdapter:
    """
    Alpha Vantage API adapter for real-time TradFi data.
    
    Premium tier ($49/month) provides:
    - Real-time quotes (no delay)
    - VIX (Volatility Index) - CRITICAL for regime detection
    - DXY (Dollar Index) - Crypto inverse correlation
    - Equities (SPY, QQQ, TLT)
    - Commodities (GLD, USO)
    
    Rate Limit: 75 calls/minute (premium), 5 calls/minute (free)
    Latency: Real-time (0 seconds)
    Sign up: https://www.alphavantage.co/premium/
    """
    
    def __init__(self, api_key: str, session: Optional[aiohttp.ClientSession] = None):
        """
        Initialize Alpha Vantage adapter.
        
        Args:
            api_key: Alpha Vantage API key
            session: Optional aiohttp session for connection pooling
        """
        self.api_key = api_key
        self.session = session
        self.base_url = "https://www.alphavantage.co/query"
        self.call_count = 0
        self.last_reset_time = time.time()
        
        logger.info("AlphaVantageAdapter initialized")
    
    async def fetch_quote(self, symbol: str) -> Optional[TradFiQuote]:
        """
        Fetch real-time quote from Alpha Vantage.
        
        Args:
            symbol: Ticker symbol (e.g. 'SPY', 'VIX', 'GLD')
        
        Returns:
            TradFiQuote with real-time data, or None if error
        """
        try:
            # Alpha Vantage uses GLOBAL_QUOTE endpoint for real-time quotes
            params = {
                'function': 'GLOBAL_QUOTE',
                'symbol': symbol,
                'apikey': self.api_key
            }
            
            async with self.session.get(self.base_url, params=params) as resp:
                if resp.status != 200:
                    logger.error(f"Alpha Vantage HTTP {resp.status} for {symbol}")
                    return None
                
                data = await resp.json()
                
                # Check for API errors
                if 'Error Message' in data:
                    logger.error(f"Alpha Vantage API error: {data['Error Message']}")
                    return None
                
                if 'Note' in data:
                    logger.warning(f"Alpha Vantage rate limit: {data['Note']}")
                    return None
                
                # Parse quote data
                quote_data = data.get('Global Quote', {})
                if not quote_data:
                    logger.warning(f"No quote data for {symbol}")
                    return None
                
                # Extract fields
                price = _safe_float(quote_data.get('05. price'))
                change_pct = _safe_float(quote_data.get('10. change percent', '0').replace('%', ''))
                volume = _safe_int(quote_data.get('06. volume'))
                
                # Latest trading day timestamp
                latest_trading_day = quote_data.get('07. latest trading day', '')
                timestamp_utc_us = int(time.time() * 1_000_000)  # Use current time for real-time data
                
                return TradFiQuote(
                    symbol=symbol,
                    price=price,
                    change_pct=change_pct,
                    volume=volume if volume > 0 else None,
                    timestamp_utc_us=timestamp_utc_us,
                    source='alpha_vantage',
                    latency_sec=0.0  # Real-time
                )
                
        except Exception as e:
            logger.error(f"Alpha Vantage error for {symbol}: {e}")
            return None


# =============================
# YAHOO FINANCE ADAPTER (FALLBACK)
# =============================

class YahooFinanceAdapter:
    """
    Yahoo Finance API adapter (fallback for Alpha Vantage).
    
    Free tier provides:
    - 15-minute delayed quotes (acceptable for 1h+ strategies)
    - SPY, QQQ, TLT, VIX (with ^VIX prefix)
    - No rate limits for reasonable usage (~2000 req/day)
    
    Rate Limit: ~2000 requests/day (undocumented)
    Latency: 15 minutes (free tier)
    Library: yfinance (pip install yfinance)
    """
    
    def __init__(self):
        """Initialize Yahoo Finance adapter."""
        if not YAHOO_AVAILABLE:
            raise RuntimeError("yfinance library not installed. Run: pip install yfinance")
        
        logger.info("YahooFinanceAdapter initialized (15-min delay)")
    
    async def fetch_quote(self, symbol: str) -> Optional[TradFiQuote]:
        """
        Fetch quote from Yahoo Finance (15-min delay).
        
        Args:
            symbol: Ticker symbol (e.g. 'SPY', '^VIX')
        
        Returns:
            TradFiQuote with 15-min delayed data, or None if error
        """
        try:
            # Run yfinance in thread pool (blocking I/O)
            ticker = await asyncio.get_event_loop().run_in_executor(
                None, yf.Ticker, symbol
            )
            
            # Get current quote
            info = ticker.info
            
            # Extract fields
            price = _safe_float(info.get('currentPrice') or info.get('regularMarketPrice'))
            change_pct = _safe_float(info.get('regularMarketChangePercent'))
            volume = _safe_int(info.get('regularMarketVolume'))
            
            if price == 0.0:
                logger.warning(f"No price data for {symbol} from Yahoo Finance")
                return None
            
            timestamp_utc_us = int(time.time() * 1_000_000)
            
            return TradFiQuote(
                symbol=symbol,
                price=price,
                change_pct=change_pct,
                volume=volume if volume > 0 else None,
                timestamp_utc_us=timestamp_utc_us,
                source='yahoo_finance',
                latency_sec=900.0  # 15-minute delay
            )
            
        except Exception as e:
            logger.error(f"Yahoo Finance error for {symbol}: {e}")
            return None


# =============================
# MACRO COLLECTOR AGENT
# =============================

class MacroCollectorAgent:
    """
    Multi-source macro/TradFi data collector with failover.
    
    Data Sources:
    1. FRED API - Economic indicators (free, daily/monthly)
    2. Alpha Vantage - Real-time indices/equities (paid, real-time)
    3. Yahoo Finance - Delayed equities (free, 15-min delay, fallback)
    
    Failover Strategy:
    - VIX/DXY: Alpha Vantage primary, Yahoo Finance fallback
    - SPY/QQQ/TLT: Alpha Vantage primary, Yahoo Finance fallback
    - FRED: No fallback (official government source)
    
    Collection Intervals:
    - Real-time (VIX, DXY): 60 seconds
    - Equities (SPY, QQQ, TLT): 60 seconds
    - FRED indicators: 1 hour (data updates daily/monthly)
    """
    
    def __init__(
        self,
        streaming_bus: StreamingBus,
        fred_api_key: Optional[str] = None,
        alpha_vantage_api_key: Optional[str] = None,
        collection_interval_sec: Optional[float] = None,
        fred_interval_sec: Optional[float] = None
    ):
        """
        Initialize Macro Collector Agent.
        
        Args:
            streaming_bus: StreamingBus for Kafka publishing
            fred_api_key: FRED API key (optional, loads from config if None)
            alpha_vantage_api_key: Alpha Vantage key (optional, loads from config if None)
            collection_interval_sec: Real-time collection interval (loads from config if None)
            fred_interval_sec: FRED collection interval (loads from config if None)
        """
        self.streaming_bus = streaming_bus
        
        # Load from config.py if not provided
        if fred_api_key is None:
            fred_api_key = MACRO_CONFIG.get('fred_api_key')
        if alpha_vantage_api_key is None:
            alpha_vantage_api_key = MACRO_CONFIG.get('alpha_vantage_api_key')
        
        # Initialize adapters
        self.fred = FREDAdapter(fred_api_key) if fred_api_key and FRED_AVAILABLE else None
        self.alpha_vantage = None  # Created in start() after session exists
        self.yahoo_finance = YahooFinanceAdapter() if YAHOO_AVAILABLE else None
        self._alpha_vantage_api_key = alpha_vantage_api_key  # Store for later
        
        # Circuit breaker state (load threshold from config)
        self._failure_threshold = MACRO_CONFIG.get('failure_threshold', 5)
        self._circuit_reset_timeout = MACRO_CONFIG.get('circuit_reset_timeout_sec', 300.0)
        self._circuit_breakers = {
            'fred': {'open': False, 'failures': 0, 'last_success': time.time()},
            'alpha_vantage': {'open': False, 'failures': 0, 'last_success': time.time()},
            'yahoo_finance': {'open': False, 'failures': 0, 'last_success': time.time()},
        }
        
        # Collection intervals (load from config or use defaults)
        self.realtime_interval = collection_interval_sec or MACRO_CONFIG.get('collection_interval_sec', 60.0)
        self.fred_interval = fred_interval_sec or MACRO_CONFIG.get('fred_interval_sec', 3600.0)
        
        # Session for HTTP requests
        self.session: Optional[aiohttp.ClientSession] = None
        
        # ═══════════════════════════════════════════════════════════════════════════
        # INSTITUTIONAL-GRADE COMPONENTS (NEW)
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
            name="macro_collector",
            buffer_capacity=500,
            low_threshold=0.50,
            high_threshold=0.75,
            max_delay_ms=1000.0
        )
        
        # AIMD rate limiters per source (using correct parameter names)
        self.rate_limiters = {
            'fred': AdaptiveRateLimiter(
                domain='fred_api',
                initial_rate=1.0,  # 60/min = 1/sec
                max_rate=2.0,
                min_rate=0.17
            ),
            'alpha_vantage': AdaptiveRateLimiter(
                domain='alpha_vantage',
                initial_rate=1.25,  # 75/min = 1.25/sec (premium tier)
                max_rate=1.25,
                min_rate=0.5
            ),
            'yahoo_finance': AdaptiveRateLimiter(
                domain='yahoo_finance',
                initial_rate=1.39,  # 2000/day ≈ 1.39/min
                max_rate=1.39,
                min_rate=0.07
            ),
        }
        
        # Health check state (load from config)
        self._last_health_check = time.time()
        self._health_check_interval = MACRO_CONFIG.get('health_check_interval_sec', 300.0)
        self._stale_data_threshold = MACRO_CONFIG.get('stale_data_threshold_sec', 600.0)
        self._last_data_timestamp = defaultdict(lambda: time.time())
        
        # Running state for graceful shutdown
        self._running = False
        self._tasks: List[asyncio.Task] = []
        
        logger.info(f"MacroCollectorAgent initialized (FRED={'✅' if self.fred else '❌'}, "
                   f"AlphaVantage={'✅' if alpha_vantage_api_key else '❌'}, "
                   f"Yahoo={'✅' if self.yahoo_finance else '❌'})")
        logger.info(f"Institutional features: Dedup=✅ Retry=✅ SLO=✅ Backpressure=✅ HealthCheck=✅")
        if CONFIG_LOADED:
            logger.info(f"Configuration loaded from config.py: ✅")
    
    async def start(self):
        """Start macro data collection with institutional-grade lifecycle management."""
        logger.info("🌍 MacroCollectorAgent starting...")
        self._running = True
        
        # Create HTTP session with production-grade configuration
        timeout = aiohttp.ClientTimeout(total=15, connect=5, sock_read=10)
        connector = aiohttp.TCPConnector(
            limit=50,              # Max total connections
            limit_per_host=10,     # Max per host
            ttl_dns_cache=600,     # 10-minute DNS cache
            keepalive_timeout=60   # Keep connections alive
        )
        self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        
        # Initialize Alpha Vantage with session
        if self._alpha_vantage_api_key:
            self.alpha_vantage = AlphaVantageAdapter(self._alpha_vantage_api_key, self.session)
        
        # Start collection tasks and store them for shutdown
        self._tasks = [
            asyncio.create_task(self._collect_realtime_indices()),
            asyncio.create_task(self._collect_equities()),
            asyncio.create_task(self._run_health_checks()),  # Health monitoring
        ]
        
        if self.fred:
            self._tasks.append(asyncio.create_task(self._collect_fred_indicators()))
        
        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            logger.info("Macro collector tasks cancelled during shutdown")
        except Exception as e:
            logger.error(f"Macro collector fatal error: {e}")
            raise
        finally:
            # CRITICAL: Proper session cleanup to prevent resource leaks
            if self.session:
                await self.session.close()
                logger.info("HTTP session closed successfully")
            
            # Log final SLO metrics
            logger.info(
                f"Final SLOs - Uptime: {self.metrics_tracker.get_uptime_pct():.2f}%, "
                f"p95 Latency: {self.metrics_tracker.get_p95_latency():.2f}s, "
                f"Duplicate Rate: {self.dedup_detector.get_duplicate_rate():.4f}, "
                f"Runtime: {self.metrics_tracker.get_runtime_hours():.2f}h"
            )
    
    async def _collect_realtime_indices(self):
        """Collect real-time indices (VIX, DXY) with institutional-grade reliability."""
        symbols = ['^VIX', 'DXY']  # VIX (volatility), DXY (dollar index)
        
        while self._running:
            collection_start = time.time()
            try:
                quotes = []
                
                for symbol in symbols:
                    # Retry with exponential backoff on transient failures
                    quote = await self.retry_manager.execute_with_retry(
                        self._fetch_with_failover,
                        symbol,
                        transient_exceptions=(aiohttp.ClientError, asyncio.TimeoutError)
                    )
                    
                    if quote:
                        # Deduplication check
                        quote_hash = quote.get_hash()
                        if not self.dedup_detector.is_duplicate(quote_hash):
                            quotes.append(quote)
                        else:
                            logger.debug(f"Skipped duplicate: {symbol}")
                            self.metrics.increment_counter(
                                'macro_collector_duplicates_skipped_total',
                                labels={'symbol': symbol}
                            )
                
                # Publish with backpressure control
                if quotes:
                    # Apply adaptive backpressure
                    await self.backpressure.apply_backpressure()
                    
                    await self.streaming_bus.publish(
                        topic='raw_data.tradfi.indices',
                        partition_key='indices',
                        payload={
                            'quotes': [q.to_dict() for q in quotes],
                            'timestamp_utc_us': int(time.time() * 1_000_000)
                        }
                    )
                    
                    # Record metrics
                    latency = time.time() - collection_start
                    self.metrics_tracker.record_success(latency)
                    self.metrics.observe_histogram(
                        'macro_collector_collection_latency_sec',
                        latency,
                        labels={'source': 'indices'}
                    )
                    self.metrics.increment_counter(
                        'macro_collector_data_collected_total',
                        value=len(quotes),
                        labels={'source': 'indices'}
                    )
                    
                    # Update health check timestamp
                    self._last_data_timestamp['indices'] = time.time()
                    
                    logger.info(f"📊 Published {len(quotes)} index quotes (VIX, DXY)")
                
            except Exception as e:
                self.metrics_tracker.record_error()
                self.metrics.increment_counter(
                    'macro_collector_errors_total',
                    labels={'source': 'indices', 'error_type': type(e).__name__}
                )
                logger.error(f"Error collecting indices: {e}")
            
            await asyncio.sleep(self.realtime_interval)
    
    async def _collect_equities(self):
        """Collect equity ETFs (SPY, QQQ, TLT) with institutional-grade reliability."""
        symbols = ['SPY', 'QQQ', 'TLT']  # S&P 500, Nasdaq, Treasuries
        
        while self._running:
            collection_start = time.time()
            try:
                quotes = []
                
                for symbol in symbols:
                    # Retry with exponential backoff
                    quote = await self.retry_manager.execute_with_retry(
                        self._fetch_with_failover,
                        symbol,
                        transient_exceptions=(aiohttp.ClientError, asyncio.TimeoutError)
                    )
                    
                    if quote:
                        # Deduplication check
                        quote_hash = quote.get_hash()
                        if not self.dedup_detector.is_duplicate(quote_hash):
                            quotes.append(quote)
                        else:
                            logger.debug(f"Skipped duplicate: {symbol}")
                            self.metrics.increment_counter(
                                'macro_collector_duplicates_skipped_total',
                                labels={'symbol': symbol}
                            )
                
                # Publish with backpressure control
                if quotes:
                    # Apply adaptive backpressure
                    await self.backpressure.apply_backpressure()
                    
                    await self.streaming_bus.publish(
                        topic='raw_data.tradfi.equities',
                        partition_key='equities',
                        payload={
                            'quotes': [q.to_dict() for q in quotes],
                            'timestamp_utc_us': int(time.time() * 1_000_000)
                        }
                    )
                    
                    # Record metrics
                    latency = time.time() - collection_start
                    self.metrics_tracker.record_success(latency)
                    self.metrics.observe_histogram(
                        'macro_collector_collection_latency_sec',
                        latency,
                        labels={'source': 'equities'}
                    )
                    self.metrics.increment_counter(
                        'macro_collector_data_collected_total',
                        value=len(quotes),
                        labels={'source': 'equities'}
                    )
                    
                    # Update health check timestamp
                    self._last_data_timestamp['equities'] = time.time()
                    
                    logger.info(f"📈 Published {len(quotes)} equity quotes (SPY, QQQ, TLT)")
                
            except Exception as e:
                self.metrics_tracker.record_error()
                self.metrics.increment_counter(
                    'macro_collector_errors_total',
                    labels={'source': 'equities', 'error_type': type(e).__name__}
                )
                logger.error(f"Error collecting equities: {e}")
            
            await asyncio.sleep(self.realtime_interval)
    
    async def _collect_fred_indicators(self):
        """Collect FRED economic indicators with institutional-grade reliability."""
        while self._running:
            collection_start = time.time()
            try:
                if not self._is_circuit_open('fred'):
                    # Fetch all indicators with retry logic (blocking I/O in thread pool)
                    indicators = await self.retry_manager.execute_with_retry(
                        asyncio.get_event_loop().run_in_executor,
                        None,
                        self.fred.fetch_all_indicators,
                        transient_exceptions=(Exception,)  # Broader for sync code
                    )
                    
                    if indicators:
                        # Deduplicate indicators
                        unique_indicators = []
                        for ind in indicators:
                            ind_hash = ind.get_hash()
                            if not self.dedup_detector.is_duplicate(ind_hash):
                                unique_indicators.append(ind)
                            else:
                                logger.debug(f"Skipped duplicate FRED indicator: {ind.series_id}")
                                self.metrics.increment_counter(
                                    'macro_collector_duplicates_skipped_total',
                                    labels={'source': 'fred', 'series_id': ind.series_id}
                                )
                        
                        if unique_indicators:
                            # Apply backpressure
                            await self.backpressure.apply_backpressure()
                            
                            # Publish to Kafka
                            await self.streaming_bus.publish(
                                topic='raw_data.macro.economic_indicators',
                                partition_key='fred',
                                payload={
                                    'indicators': [ind.to_dict() for ind in unique_indicators],
                                    'timestamp_utc_us': int(time.time() * 1_000_000)
                                }
                            )
                            
                            # Record metrics
                            latency = time.time() - collection_start
                            self.metrics_tracker.record_success(latency)
                            self.metrics.observe_histogram(
                                'macro_collector_collection_latency_sec',
                                latency,
                                labels={'source': 'fred'}
                            )
                            self.metrics.increment_counter(
                                'macro_collector_data_collected_total',
                                value=len(unique_indicators),
                                labels={'source': 'fred'}
                            )
                            
                            # Update health check timestamp
                            self._last_data_timestamp['fred'] = time.time()
                            
                            logger.info(f"💰 Published {len(unique_indicators)} FRED indicators")
                            self._record_success('fred')
                    else:
                        self._record_failure('fred')
                
            except Exception as e:
                logger.error(f"Error collecting FRED data: {e}")
                self._record_failure('fred')
                self.metrics_tracker.record_error()
                self.metrics.increment_counter(
                    'macro_collector_errors_total',
                    labels={'source': 'fred', 'error_type': type(e).__name__}
                )
            
            await asyncio.sleep(self.fred_interval)
    
    async def _fetch_with_failover(self, symbol: str) -> Optional[TradFiQuote]:
        """
        Fetch quote with Alpha Vantage → Yahoo Finance failover.
        
        Args:
            symbol: Ticker symbol
        
        Returns:
            TradFiQuote or None
        """
        # Try Alpha Vantage first (real-time)
        if self.alpha_vantage and not self._is_circuit_open('alpha_vantage'):
            try:
                quote = await self.alpha_vantage.fetch_quote(symbol)
                if quote:
                    self._record_success('alpha_vantage')
                    return quote
                else:
                    self._record_failure('alpha_vantage')
            except Exception as e:
                logger.warning(f"Alpha Vantage failed for {symbol}: {e}")
                self._record_failure('alpha_vantage')
        
        # Fallback to Yahoo Finance (15-min delay)
        if self.yahoo_finance and not self._is_circuit_open('yahoo_finance'):
            try:
                quote = await self.yahoo_finance.fetch_quote(symbol)
                if quote:
                    self._record_success('yahoo_finance')
                    return quote
                else:
                    self._record_failure('yahoo_finance')
            except Exception as e:
                logger.warning(f"Yahoo Finance failed for {symbol}: {e}")
                self._record_failure('yahoo_finance')
        
        return None
    
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
                
                # Check for stale data streams
                for source, last_ts in self._last_data_timestamp.items():
                    staleness = current_time - last_ts
                    if staleness > self._stale_data_threshold:
                        health_issues.append(f"{source} stale ({staleness:.0f}s)")
                        
                        # Record Prometheus metric
                        self.metrics.increment_counter(
                            'macro_collector_stale_data_total',
                            labels={'source': source}
                        )
                
                # Check circuit breakers
                for source, breaker in self._circuit_breakers.items():
                    if breaker.get('open'):
                        health_issues.append(f"{source} circuit OPEN")
                        
                        # Record Prometheus metric
                        self.metrics.set_gauge(
                            'macro_collector_circuit_breaker_open',
                            1.0,
                            labels={'source': source}
                        )
                    else:
                        self.metrics.set_gauge(
                            'macro_collector_circuit_breaker_open',
                            0.0,
                            labels={'source': source}
                        )
                
                # Check SLO violations
                uptime_pct = self.metrics_tracker.get_uptime_pct()
                duplicate_rate = self.dedup_detector.get_duplicate_rate()
                p95_latency = self.metrics_tracker.get_p95_latency()
                
                if uptime_pct < 99.0:
                    health_issues.append(f"SLO: Uptime {uptime_pct:.2f}% < 99%")
                
                if duplicate_rate > 0.001:
                    health_issues.append(f"SLO: Dup rate {duplicate_rate:.4f} > 0.1%")
                
                if p95_latency > self.realtime_interval:
                    health_issues.append(f"SLO: p95 latency {p95_latency:.2f}s > {self.realtime_interval}s")
                
                # Record Prometheus metrics
                self.metrics.set_gauge('macro_collector_uptime_pct', uptime_pct)
                self.metrics.set_gauge('macro_collector_duplicate_rate', duplicate_rate)
                self.metrics.set_gauge('macro_collector_p95_latency_sec', p95_latency)
                
                # Log health status
                if health_issues:
                    logger.warning(f"❌ Health check FAILED: {', '.join(health_issues)}")
                    self.metrics.increment_counter('macro_collector_health_check_failures_total')
                else:
                    logger.info(
                        f"✅ Health check OK - Uptime: {uptime_pct:.2f}%, "
                        f"Dup: {duplicate_rate:.4f}, p95: {p95_latency:.2f}s, "
                        f"Runtime: {self.metrics_tracker.get_runtime_hours():.2f}h"
                    )
                
            except Exception as e:
                logger.error(f"Health check error: {e}")
    
    def _is_circuit_open(self, source: str) -> bool:
        """Check if circuit breaker is open for a source."""
        breaker = self._circuit_breakers.get(source, {})
        
        if breaker.get('open'):
            # Check if timeout has elapsed
            if time.time() - breaker['last_success'] > self._circuit_reset_timeout:
                # Try to reset
                breaker['open'] = False
                breaker['failures'] = 0
                logger.info(f"Circuit breaker RESET for {source}")
                return False
            return True
        
        return False
    
    def _record_success(self, source: str):
        """Record successful API call."""
        breaker = self._circuit_breakers.get(source, {})
        breaker['failures'] = 0
        breaker['last_success'] = time.time()
        breaker['open'] = False
    
    def _record_failure(self, source: str):
        """Record failed API call and open circuit if threshold exceeded."""
        breaker = self._circuit_breakers.get(source, {})
        breaker['failures'] = breaker.get('failures', 0) + 1
        
        if breaker['failures'] >= self._failure_threshold:
            breaker['open'] = True
            logger.error(f"Circuit breaker OPENED for {source} ({breaker['failures']} failures)")
    
    async def stop(self):
        """Gracefully stop the macro collector."""
        logger.info("🛑 Stopping MacroCollectorAgent...")
        self._running = False
        
        # Cancel all running tasks
        for task in self._tasks:
            if not task.done():
                task.cancel()
        
        # Wait for tasks to complete cancellation
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        
        # Close HTTP session
        if self.session and not self.session.closed:
            await self.session.close()
            logger.info("HTTP session closed")
        
        logger.info("✅ MacroCollectorAgent stopped")


# =============================
# MAIN ENTRY POINT
# =============================

async def main():
    """Test macro collector with sample API calls."""
    import os
    
    # Load API keys from environment
    fred_key = os.getenv('FRED_API_KEY')
    alpha_vantage_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    
    if not fred_key:
        logger.warning("FRED_API_KEY not set. Get free key at: https://fred.stlouisfed.org/docs/api/api_key.html")
    
    if not alpha_vantage_key:
        logger.warning("ALPHA_VANTAGE_API_KEY not set. Using Yahoo Finance fallback only.")
    
    # Create mock streaming bus
    class MockStreamingBus:
        async def publish(self, topic: str, data: Dict):
            logger.info(f"📤 Published to {topic}: {len(data)} items")
    
    # Initialize collector
    collector = MacroCollectorAgent(
        streaming_bus=MockStreamingBus(),
        fred_api_key=fred_key,
        alpha_vantage_api_key=alpha_vantage_key
    )
    
    # Run for 5 minutes (test)
    try:
        await asyncio.wait_for(collector.start(), timeout=300)
    except asyncio.TimeoutError:
        logger.info("Test completed (5 minutes)")


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    asyncio.run(main())
