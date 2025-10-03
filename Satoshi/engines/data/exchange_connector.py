
"""
Exchange Connector Agent (CEX/Perps/Futures)

Mission: Pull trades, 1–5s books, funding, OI, borrow, maintenance windows.
Owns: Venue adapters, retry/backoff, time alignment.
Inputs → Outputs: Venue APIs → raw_data.market.{trades,book,funding,oi} (normalized schema).

SLOs/KPIs: 
- Uptime ≥99.5%
- p95 ingest lag ≤2× bar size 
- duplicate rate <0.1%

Do/Don't: Do normalize timestamps; don't impute or "fix" content.
"""

import asyncio
import aiohttp
import json
import time
import logging
# Module logger for hygiene
logger = logging.getLogger(__name__)
import random
import math
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Optional, List, Dict, Any, Set, Callable, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import hashlib
from collections import defaultdict, deque


# ============================================================================
# SCHEMAS & DATA STRUCTURES
# ============================================================================

class VenueType(Enum):
    CEX = "cex"
    PERPS = "perps" 
    FUTURES = "futures"


class Side(Enum):
    BUY = "buy"
    SELL = "sell"
    BID = "bid"
    ASK = "ask"


class DataType(Enum):
    TRADES = "trades"
    BOOK = "book"
    FUNDING = "funding"
    OI = "oi"
    BORROW = "borrow"
    MAINTENANCE = "maintenance"


@dataclass
class TradeData:
    """Normalized trade data structure."""
    venue: str
    venue_type: VenueType
    symbol: str
    timestamp_utc_us: int  # Event time UTC microseconds
    price: Decimal
    quantity: Decimal
    side: Side
    trade_id: str
    venue_timestamp_utc_us: Optional[int] = None
    capture_timestamp_utc_us: Optional[int] = None  # Ingestion time
    fees: Optional[Decimal] = None
    is_maker: Optional[bool] = None
    
    def get_hash(self) -> str:
        """Generate hash for duplicate detection using venue-stable identifiers."""
        hash_input = f"{self.venue}:{self.symbol}:{self.trade_id}"
        if self.venue_timestamp_utc_us:
            hash_input += f":{self.venue_timestamp_utc_us}"
        return hashlib.md5(hash_input.encode()).hexdigest()


@dataclass
class BookLevel:
    """Single book level (bid/ask)."""
    price: Decimal
    quantity: Decimal


@dataclass
class BookData:
    """Normalized order book data structure."""
    venue: str
    venue_type: VenueType
    symbol: str
    timestamp_utc_us: int  # Capture time UTC microseconds
    bids: List[BookLevel]
    asks: List[BookLevel]
    venue_timestamp_utc_us: Optional[int] = None
    sequence_number: Optional[int] = None
    
    def get_hash(self) -> str:
        """Generate hash for duplicate detection using stable content."""
        bid_str = ",".join([f"{b.price}:{b.quantity}" for b in self.bids[:5]])
        ask_str = ",".join([f"{a.price}:{a.quantity}" for a in self.asks[:5]])
        hash_input = f"{self.venue}:{self.symbol}:{bid_str}:{ask_str}"
        if self.sequence_number:
            hash_input += f":{self.sequence_number}"
        return hashlib.md5(hash_input.encode()).hexdigest()


@dataclass
class FundingData:
    """Normalized funding rate data."""
    venue: str
    venue_type: VenueType
    symbol: str
    timestamp_utc_us: int  # UTC microseconds
    funding_rate: Decimal  # 8-hour rate typically
    funding_rate_annual: Optional[Decimal] = None
    next_funding_time_utc_us: Optional[int] = None
    venue_timestamp_utc_us: Optional[int] = None
    
    def get_hash(self) -> str:
        """Generate hash for duplicate detection using venue-stable identifiers."""
        return hashlib.md5(f"{self.venue}:{self.symbol}:{self.funding_rate}".encode()).hexdigest()


@dataclass
class OpenInterestData:
    """Normalized open interest data."""
    venue: str
    venue_type: VenueType
    symbol: str
    timestamp_utc_us: int  # UTC microseconds
    open_interest: Decimal
    open_interest_usd: Optional[Decimal] = None
    venue_timestamp_utc_us: Optional[int] = None
    
    def get_hash(self) -> str:
        """Generate hash for duplicate detection using venue-stable identifiers."""
        return hashlib.md5(f"{self.venue}:{self.symbol}:{self.open_interest}".encode()).hexdigest()


@dataclass
class BorrowData:
    """Normalized borrow rate data."""
    venue: str
    venue_type: VenueType
    symbol: str
    timestamp_utc_us: int  # UTC microseconds
    borrow_rate_annual: Decimal
    available_quantity: Optional[Decimal] = None
    venue_timestamp_utc_us: Optional[int] = None
    
    def get_hash(self) -> str:
        """Generate hash for duplicate detection using venue-stable identifiers."""
        return hashlib.md5(f"{self.venue}:{self.symbol}:{self.borrow_rate_annual}".encode()).hexdigest()


@dataclass
class MaintenanceWindow:
    """Venue maintenance window information."""
    venue: str
    venue_type: VenueType
    start_time_utc_us: int
    end_time_utc_us: int
    affected_symbols: List[str]
    maintenance_type: str  # e.g., "system", "symbol", "trading_halt"
    description: Optional[str] = None


@dataclass
class VenueMetrics:
    """Real-time venue performance metrics."""
    venue: str
    timestamp_utc_us: int
    uptime_pct: float
    latency_p95_ms: float
    error_rate_pct: float
    duplicate_rate_pct: float
    last_data_timestamp_utc_us: Optional[int] = None


# ============================================================================
# RETRY & BACKOFF LOGIC
# ============================================================================

@dataclass
class RetryConfig:
    """Configuration for retry/backoff behavior."""
    max_retries: int = 5
    base_delay_ms: int = 100
    max_delay_ms: int = 30000
    backoff_multiplier: float = 2.0
    jitter: bool = True


class RetryManager:
    """Handles exponential backoff with jitter."""
    
    def __init__(self, config: RetryConfig):
        self.config = config
        self.attempt_counts: Dict[str, int] = defaultdict(int)
        
    async def execute_with_retry(self, operation_id: str, operation: Callable, *args, **kwargs):
        """Execute operation with retry logic."""
        attempt = 0
        while attempt <= self.config.max_retries:
            try:
                result = await operation(*args, **kwargs)
                # Reset on success
                self.attempt_counts[operation_id] = 0
                return result
            except Exception as e:
                attempt += 1
                self.attempt_counts[operation_id] = attempt
                
                if attempt > self.config.max_retries:
                    logger.error(f"Operation {operation_id} failed after {attempt} attempts: {e}")
                    raise
                
                delay_ms = min(
                    self.config.base_delay_ms * (self.config.backoff_multiplier ** (attempt - 1)),
                    self.config.max_delay_ms
                )
                
                if self.config.jitter:
                    delay_ms *= random.uniform(0.5, 1.5)  # Proper random jitter
                
                logger.warning(f"Operation {operation_id} failed (attempt {attempt}), retrying in {delay_ms:.0f}ms: {e}")
                await asyncio.sleep(delay_ms / 1000)


# ==========================================================================
# MAIN EXCHANGE CONNECTOR AGENT
# ============================================================================

class VenueAdapter(ABC):
    """Abstract base class for venue-specific adapters."""
    def __init__(self, venue: str, venue_type: VenueType, config: Dict[str, Any]):
        self.venue = venue
        self.venue_type = venue_type
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None  # Initialized in start()
        self.retry_manager = RetryManager(RetryConfig())
        # Light rate limiting courtesy
        self.rate_limit_semaphore = asyncio.Semaphore(config.get("concurrent_requests", 5))

    @abstractmethod
    async def get_trades(self, symbol: str, since_timestamp_us: Optional[int] = None) -> List[TradeData]:
        pass
    @abstractmethod
    async def get_book(self, symbol: str, depth: int = 20) -> BookData:
        pass
    @abstractmethod
    async def get_funding(self, symbol: str) -> Optional[FundingData]:
        pass
    @abstractmethod
    async def get_open_interest(self, symbol: str) -> Optional[OpenInterestData]:
        pass
    @abstractmethod
    async def get_borrow_rate(self, symbol: str) -> Optional[BorrowData]:
        pass
    @abstractmethod
    async def get_maintenance_windows(self) -> List[MaintenanceWindow]:
        pass

    async def start(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            connector=aiohttp.TCPConnector(limit=100)
        )
    async def stop(self):
        if self.session:
            await self.session.close()
    def normalize_timestamp(self, timestamp: Any) -> int:
        if isinstance(timestamp, int):
            if 1_000_000_000_000 <= timestamp <= 9_999_999_999_999:
                return timestamp * 1000
            elif timestamp > 9_999_999_999_999:
                return timestamp
            else:
                return timestamp * 1_000_000
        elif isinstance(timestamp, float):
            return int(timestamp * 1_000_000)
        elif isinstance(timestamp, datetime):
            return int(timestamp.timestamp() * 1_000_000)
        else:
            raise ValueError(f"Unsupported timestamp format: {type(timestamp)}")


class BinanceFuturesAdapter(VenueAdapter):
    """Binance USDT Futures adapter (https://fapi.binance.com)"""
    def __init__(self, config: Dict[str, Any]):
        super().__init__("binance_futures", VenueType.FUTURES, config)
        self.base_url = "https://fapi.binance.com"

    async def get_open_interest(self, symbol: str) -> Optional[OpenInterestData]:
        """Fetch open interest for a symbol from Binance Futures."""
        if not self.session:
            raise RuntimeError("Session is not initialized. Did you forget to call the 'start' method?")
        normalized_symbol = symbol.upper()
        params = {"symbol": normalized_symbol}
        async with self.rate_limit_semaphore:
            async with self.session.get(f"{self.base_url}/futures/data/openInterest", params=params) as resp:
                resp.raise_for_status()
                try:
                    data = await resp.json()
                except Exception:
                    body = await resp.text()
                    logger.error(f"Failed to parse JSON for open interest: {body}")
                    return None
                capture_time = int(time.time() * 1_000_000)
                venue_ts = data.get("timestamp")
                open_interest = Decimal(str(data.get("openInterest", 0)))
                return OpenInterestData(
                    venue=self.venue,
                    venue_type=self.venue_type,
                    symbol=normalized_symbol,
                    timestamp_utc_us=capture_time,
                    open_interest=open_interest,
                    open_interest_usd=None,
                    venue_timestamp_utc_us=venue_ts
                )


    async def get_funding(self, symbol: str) -> Optional[FundingData]:
        if not self.session:
            raise RuntimeError("Session is not initialized. Did you forget to call the 'start' method?")
        normalized_symbol = symbol.upper()
        params = {"symbol": normalized_symbol}
        async with self.rate_limit_semaphore:
            async with self.session.get(f"{self.base_url}/fapi/v1/premiumIndex", params=params) as resp:
                resp.raise_for_status()
                try:
                    data = await resp.json()
                except Exception:
                    body = await resp.text()
                    logger.error(f"Failed to parse JSON for funding: {body}")
                    return None
                capture_time = int(time.time() * 1_000_000)
                venue_ts = data.get("time")
                return FundingData(
                    venue=self.venue,
                    venue_type=self.venue_type,
                    symbol=normalized_symbol,
                    timestamp_utc_us=capture_time,
                    funding_rate=Decimal(str(data.get("lastFundingRate", 0))),
                    funding_rate_annual=None,
                    next_funding_time_utc_us=data.get("nextFundingTime"),
                    venue_timestamp_utc_us=venue_ts
                )

    async def get_borrow_rate(self, symbol: str) -> Optional[BorrowData]:
        # Binance Futures does not have a public borrow endpoint; for margin borrow, use SAPI on spot
        # We'll return None for now, but you could implement SAPI spot borrow if needed.
        return None

    async def get_book(self, symbol: str, depth: int = 20) -> BookData:
        if not self.session:
            raise RuntimeError("Session is not initialized. Did you forget to call the 'start' method?")
        normalized_symbol = symbol.upper()
        params = {"symbol": normalized_symbol, "limit": depth}
        async with self.rate_limit_semaphore:
            async with self.session.get(f"{self.base_url}/fapi/v1/depth", params=params) as resp:
                resp.raise_for_status()
                try:
                    data = await resp.json()
                except Exception:
                    body = await resp.text()
                    logger.error(f"Failed to parse JSON for futures book: {body}")
                    raise
                current_time = int(time.time() * 1_000_000)
                bids = [BookLevel(Decimal(str(bid[0])), Decimal(str(bid[1]))) for bid in data["bids"]]
                asks = [BookLevel(Decimal(str(ask[0])), Decimal(str(ask[1]))) for ask in data["asks"]]
                return BookData(
                    venue=self.venue,
                    venue_type=self.venue_type,
                    symbol=normalized_symbol,
                    timestamp_utc_us=current_time,
                    bids=bids,
                    asks=asks,
                    venue_timestamp_utc_us=None,
                    sequence_number=data.get("lastUpdateId")
                )

    async def get_trades(self, symbol: str, since_timestamp_us: Optional[int] = None) -> List[TradeData]:
        if not self.session:
            raise RuntimeError("Session is not initialized. Did you forget to call the 'start' method?")
        normalized_symbol = symbol.upper()
        params = {"symbol": normalized_symbol, "limit": 1000}
        if since_timestamp_us:
            since_ms = (since_timestamp_us // 1000) + 1
            params["startTime"] = since_ms
        async with self.rate_limit_semaphore:
            async with self.session.get(f"{self.base_url}/fapi/v1/aggTrades", params=params) as resp:
                if resp.status == 429:
                    retry_after = resp.headers.get('Retry-After')
                    if retry_after:
                        await asyncio.sleep(int(retry_after))
                    else:
                        await asyncio.sleep(1)
                resp.raise_for_status()
                try:
                    data = await resp.json()
                except Exception:
                    body = await resp.text()
                    logger.error(f"Failed to parse JSON for futures trades: {body}")
                    raise
                trades = []
                capture_time = int(time.time() * 1_000_000)
                for trade in data:
                    trades.append(TradeData(
                        venue=self.venue,
                        venue_type=self.venue_type,
                        symbol=normalized_symbol,
                        timestamp_utc_us=self.normalize_timestamp(trade["T"]),
                        price=Decimal(str(trade["p"])),
                        quantity=Decimal(str(trade["q"])),
                        side=Side.BUY if not trade["m"] else Side.SELL,
                        trade_id=str(trade["a"]),
                        venue_timestamp_utc_us=self.normalize_timestamp(trade["T"]),
                        capture_timestamp_utc_us=capture_time
                    ))
                return trades

    async def get_maintenance_windows(self) -> List[MaintenanceWindow]:
        # Binance does not have a public endpoint for futures maintenance windows; return empty for now.
        return []

    async def get_historical_open_interest(self, symbol: str, period: str = "5m", limit: int = 30):
        """Fetch historical open interest (optional, not used by default collectors)."""
        if not self.session:
            raise RuntimeError("Session is not initialized. Did you forget to call the 'start' method?")
        normalized_symbol = symbol.upper()
        params = {"symbol": normalized_symbol, "period": period, "limit": limit}
        async with self.rate_limit_semaphore:
            async with self.session.get(f"{self.base_url}/futures/data/openInterestHist", params=params) as resp:
                resp.raise_for_status()
                try:
                    data = await resp.json()
                except Exception:
                    body = await resp.text()
                    logger = logging.getLogger(__name__)
                    logger.error(f"Failed to parse JSON for OI history: {body}")
                    raise
                # Returns a list of dicts with openInterest, timestamp, etc.
                return data


class BinanceAdapter(VenueAdapter):
    """Binance venue adapter."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__("binance", VenueType.CEX, config)
        self.base_url = "https://api.binance.com"
        
    async def get_trades(self, symbol: str, since_timestamp_us: Optional[int] = None) -> List[TradeData]:
        """Fetch recent trades from Binance using aggTrades endpoint."""
        async def _fetch():
            if not self.session:
                raise RuntimeError("Session is not initialized. Did you forget to call the 'start' method?")
            
            normalized_symbol = symbol.upper()  # Ensure uppercase for Binance
            params = {"symbol": normalized_symbol, "limit": 1000}
            if since_timestamp_us:
                # Add +1ms to avoid inclusive duplicate on startTime
                since_ms = (since_timestamp_us // 1000) + 1
                params["startTime"] = since_ms
                
            async with self.rate_limit_semaphore:
                async with self.session.get(f"{self.base_url}/api/v3/aggTrades", params=params) as resp:
                    if resp.status == 429:
                        # Respect Retry-After header if present
                        retry_after = resp.headers.get('Retry-After')
                        if retry_after:
                            await asyncio.sleep(int(retry_after))
                        else:
                            await asyncio.sleep(1)  # Default pause
                    resp.raise_for_status()
                    try:
                        data = await resp.json()
                    except Exception:
                        body = await resp.text()
                        logger.error(f"Failed to parse JSON for trades: {body}")
                        raise
                    trades = []
                    capture_time = int(time.time() * 1_000_000)
                    for trade in data:
                        trades.append(TradeData(
                            venue=self.venue,
                            venue_type=self.venue_type,
                            symbol=normalized_symbol,
                            timestamp_utc_us=self.normalize_timestamp(trade["T"]),  # Event time
                            price=Decimal(str(trade["p"])),
                            quantity=Decimal(str(trade["q"])),
                            side=Side.BUY if not trade["m"] else Side.SELL,  # m=true means maker sell
                            trade_id=str(trade["a"]),  # Aggregate trade ID
                            venue_timestamp_utc_us=self.normalize_timestamp(trade["T"]),
                            capture_timestamp_utc_us=capture_time
                        ))
                    return trades
                
        result = await self.retry_manager.execute_with_retry(f"trades_{self.venue}_{symbol}", _fetch)
        if not isinstance(result, list):
            raise TypeError(f"Expected result to be List[TradeData], got {type(result)}")
        return result
        
    async def get_book(self, symbol: str, depth: int = 20) -> BookData:
        """Fetch order book from Binance."""
        async def _fetch():
            normalized_symbol = symbol.upper()  # Ensure uppercase for Binance
            params = {"symbol": normalized_symbol, "limit": depth}
            if not self.session:
                raise RuntimeError("Session is not initialized. Did you forget to call the 'start' method?")
            
            async with self.rate_limit_semaphore:
                async with self.session.get(f"{self.base_url}/api/v3/depth", params=params) as resp:
                    if resp.status == 429:
                        # Respect Retry-After header if present
                        retry_after = resp.headers.get('Retry-After')
                        if retry_after:
                            await asyncio.sleep(int(retry_after))
                        else:
                            await asyncio.sleep(1)  # Default pause
                    resp.raise_for_status()
                    try:
                        data = await resp.json()
                    except Exception:
                        body = await resp.text()
                        logger.error(f"Failed to parse JSON for book: {body}")
                        raise
                    current_time = int(time.time() * 1_000_000)
                    bids = [BookLevel(Decimal(str(bid[0])), Decimal(str(bid[1]))) for bid in data["bids"]]
                    asks = [BookLevel(Decimal(str(ask[0])), Decimal(str(ask[1]))) for ask in data["asks"]]
                    return BookData(
                        venue=self.venue,
                        venue_type=self.venue_type,
                        symbol=normalized_symbol,
                        timestamp_utc_us=current_time,  # Capture time for books
                        bids=bids,
                        asks=asks,
                        venue_timestamp_utc_us=None,  # Don't treat lastUpdateId as timestamp
                        sequence_number=data.get("lastUpdateId")  # Store as sequence number
                    )
                
        result = await self.retry_manager.execute_with_retry(f"book_{self.venue}_{symbol}", _fetch)
        if not isinstance(result, BookData):
            raise TypeError(f"Expected result to be BookData, got {type(result)}")
        return result
        
    async def get_funding(self, symbol: str) -> Optional[FundingData]:
        """Binance spot doesn't have funding rates."""
        return None
        
    async def get_open_interest(self, symbol: str) -> Optional[OpenInterestData]:
        """Binance spot doesn't have open interest."""
        return None
        
    async def get_borrow_rate(self, symbol: str) -> Optional[BorrowData]:
        """Fetch margin borrow rate from Binance."""
        # Implementation would depend on specific API endpoints
        return None
        
    async def get_maintenance_windows(self) -> List[MaintenanceWindow]:
        """Fetch maintenance info from Binance."""
        # Implementation would depend on specific API endpoints
        return []
    
# ==========================================================================
# COINBASE ADAPTER (place after all dataclasses and VenueAdapter, before ExchangeConnectorAgent)
# ==========================================================================

class CoinbaseAdapter(VenueAdapter):
    """Coinbase spot market adapter (https://api.exchange.coinbase.com)"""
    def __init__(self, config: Dict[str, Any]):
        super().__init__("coinbase", VenueType.CEX, config)
        self.base_url = "https://api.exchange.coinbase.com"

    async def get_trades(self, symbol: str, since_timestamp_us: Optional[int] = None) -> List[TradeData]:
        if not self.session:
            raise RuntimeError("Session is not initialized. Did you forget to call the 'start' method?")
        # Coinbase uses product_id like 'BTC-USD'
        product_id = symbol.replace("USDT", "USD") if symbol.endswith("USDT") else symbol
        params = {"limit": 100}
        url = f"{self.base_url}/products/{product_id}/trades"
        async with self.rate_limit_semaphore:
            async with self.session.get(url, params=params) as resp:
                resp.raise_for_status()
                try:
                    data = await resp.json()
                except Exception:
                    body = await resp.text()
                    logger.error(f"Failed to parse JSON for coinbase trades: {body[:500]}")
                    return []
                capture_time = int(time.time() * 1_000_000)
                trades = []
                for trade in data:
                    # time is ISO8601, e.g. '2023-09-30T12:34:56.789Z'
                    try:
                        event_time = int(datetime.strptime(trade["time"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=None).timestamp() * 1_000_000)
                    except Exception:
                        event_time = capture_time
                    trades.append(TradeData(
                        venue=self.venue,
                        venue_type=self.venue_type,
                        symbol=product_id,
                        timestamp_utc_us=event_time,
                        price=Decimal(str(trade["price"])),
                        quantity=Decimal(str(trade["size"])),
                        side=Side.BUY if trade["side"] == "buy" else Side.SELL,
                        trade_id=str(trade["trade_id"]),
                        venue_timestamp_utc_us=event_time,
                        capture_timestamp_utc_us=capture_time
                    ))
                return trades

    async def get_book(self, symbol: str, depth: int = 20) -> BookData:
        if not self.session:
            raise RuntimeError("Session is not initialized. Did you forget to call the 'start' method?")
        product_id = symbol.replace("USDT", "USD") if symbol.endswith("USDT") else symbol
        params = {"level": 2}
        url = f"{self.base_url}/products/{product_id}/book"
        async with self.rate_limit_semaphore:
            async with self.session.get(url, params=params) as resp:
                resp.raise_for_status()
                try:
                    data = await resp.json()
                except Exception:
                    body = await resp.text()
                    logger.error(f"Failed to parse JSON for coinbase book: {body[:500]}")
                    raise
                current_time = int(time.time() * 1_000_000)
                bids = [BookLevel(Decimal(str(b[0])), Decimal(str(b[1]))) for b in data.get("bids", [])[:depth]]
                asks = [BookLevel(Decimal(str(a[0])), Decimal(str(a[1]))) for a in data.get("asks", [])[:depth]]
                return BookData(
                    venue=self.venue,
                    venue_type=self.venue_type,
                    symbol=product_id,
                    timestamp_utc_us=current_time,
                    bids=bids,
                    asks=asks,
                    venue_timestamp_utc_us=None,
                    sequence_number=None
                )

    async def get_funding(self, symbol: str) -> Optional[FundingData]:
        # Coinbase spot does not have funding rates
        return None

    async def get_open_interest(self, symbol: str) -> Optional[OpenInterestData]:
        if not self.session:
            raise RuntimeError("Session is not initialized. Did you forget to call the 'start' method?")
        product_id = symbol.replace("USDT", "USD") if symbol.endswith("USDT") else symbol
        url = f"{self.base_url}/products/{product_id}/stats"
        async with self.rate_limit_semaphore:
            async with self.session.get(url) as resp:
                resp.raise_for_status()
                try:
                    data = await resp.json()
                except Exception:
                    body = await resp.text()
                    logger.error(f"Failed to parse JSON for coinbase OI: {body[:500]}")
                    return None
                # Coinbase does not provide OI directly, but we can use open/high/low/volume as a proxy if needed
                # Here, we return None for strictness
                return None

    async def get_borrow_rate(self, symbol: str) -> Optional[BorrowData]:
        # Coinbase spot does not have borrow rates
        return None

    async def get_maintenance_windows(self) -> List[MaintenanceWindow]:
        # Coinbase does not have a public endpoint for maintenance windows
        return []



# ============================================================================
# DUPLICATE DETECTION & METRICS
# ============================================================================

class DuplicateDetector:
    """Tracks and detects duplicate data."""
    
    def __init__(self, window_size: int = 10000):
        self.window_size = window_size
        self.seen_hashes: Dict[str, deque] = defaultdict(lambda: deque(maxlen=window_size))
        self.duplicate_counts: Dict[str, int] = defaultdict(int)
        self.total_counts: Dict[str, int] = defaultdict(int)
        
    def is_duplicate(self, data_type: str, data_hash: str) -> bool:
        """Check if data hash is a duplicate."""
        self.total_counts[data_type] += 1
        
        if data_hash in self.seen_hashes[data_type]:
            self.duplicate_counts[data_type] += 1
            return True
            
        self.seen_hashes[data_type].append(data_hash)
        return False
        
    def get_duplicate_rate(self, data_type: str) -> float:
        """Get duplicate rate for data type."""
        total = self.total_counts[data_type]
        if total == 0:
            return 0.0
        return self.duplicate_counts[data_type] / total


class MetricsCollector:
    """Collects and tracks SLO/KPI metrics."""
    
    def __init__(self):
        self.start_time = time.time()
        self.error_counts: Dict[str, int] = defaultdict(int)
        self.success_counts: Dict[str, int] = defaultdict(int)
        self.latencies: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.last_data_times: Dict[str, int] = {}
        
    def record_success(self, operation: str, latency_ms: float):
        """Record successful operation."""
        self.success_counts[operation] += 1
        self.latencies[operation].append(latency_ms)
        
    def record_error(self, operation: str):
        """Record failed operation."""
        self.error_counts[operation] += 1
        
    def record_data_timestamp(self, venue_symbol: str, timestamp_us: int):
        """Record latest data timestamp."""
        self.last_data_times[venue_symbol] = timestamp_us
        
    def get_uptime_pct(self) -> float:
        """Calculate uptime percentage."""
        total_ops = sum(self.success_counts.values()) + sum(self.error_counts.values())
        if total_ops == 0:
            return 100.0
        return (sum(self.success_counts.values()) / total_ops) * 100
        
    def get_p95_latency_ms(self, operation: str) -> float:
        """Get 95th percentile latency."""
        latencies = list(self.latencies[operation])
        if not latencies:
            return 0.0
        latencies.sort()
        idx = max(0, math.ceil(0.95 * len(latencies)) - 1)
        return latencies[idx]
        
    def get_error_rate_pct(self, operation: str) -> float:
        """Get error rate percentage."""
        total = self.success_counts[operation] + self.error_counts[operation]
        if total == 0:
            return 0.0
        return (self.error_counts[operation] / total) * 100


# ============================================================================
# MAIN EXCHANGE CONNECTOR AGENT
# ============================================================================

class ExchangeConnectorAgent:
    """
    Main Exchange Connector Agent for CEX/Perps/Futures data ingestion.
    
    Handles multiple venues, retry logic, time alignment, and normalized output.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.adapters: Dict[str, VenueAdapter] = {}
        self.duplicate_detector = DuplicateDetector()
        self.metrics = MetricsCollector()
        self.running = False
        self.tasks: List[asyncio.Task] = []
        
        # SLO targets
        self.target_uptime_pct = 99.5
        self.target_duplicate_rate_pct = 0.1
        
        # Output queues
        self.output_queues: Dict[DataType, asyncio.Queue] = {
            DataType.TRADES: asyncio.Queue(maxsize=10000),
            DataType.BOOK: asyncio.Queue(maxsize=1000),
            DataType.FUNDING: asyncio.Queue(maxsize=100),
            DataType.OI: asyncio.Queue(maxsize=100),
            DataType.BORROW: asyncio.Queue(maxsize=100),
        }
        
        self._setup_adapters()

    def _adapter_supports(self, adapter, method_name):
        """Return True if adapter implements a non-None method for the given data type."""
        method = getattr(adapter, method_name, None)
        return callable(method) and not (method is VenueAdapter.__dict__.get(method_name))
        
    def _setup_adapters(self):
        """Initialize venue adapters based on config."""
        for venue_config in self.config.get("venues", []):
            venue_name = venue_config["name"]
            venue_class = venue_config.get("adapter_class", "BinanceAdapter")
            # Simple factory pattern
            if venue_class == "BinanceAdapter":
                adapter = BinanceAdapter(venue_config)
            elif venue_class == "BinanceFuturesAdapter" or (venue_class == "BinanceAdapter" and self.config.get("futures_enabled")):
                adapter = BinanceFuturesAdapter(venue_config)
            elif venue_class == "CoinbaseAdapter":
                adapter = CoinbaseAdapter(venue_config)
            else:
                raise ValueError(f"Unknown adapter class: {venue_class}")
            self.adapters[venue_name] = adapter
            
    async def start(self):
        """Start the connector agent."""
        logger.info("Starting Exchange Connector Agent...")
        self.running = True
        # Start all adapters
        for adapter in self.adapters.values():
            await adapter.start()
        # Start data collection tasks
        for venue_name, adapter in self.adapters.items():
            for symbol in self.config.get("symbols", []):
                # Trades collection
                task = asyncio.create_task(
                    self._collect_trades(adapter, symbol)
                )
                self.tasks.append(task)
                # Book collection
                task = asyncio.create_task(
                    self._collect_book(adapter, symbol)
                )
                self.tasks.append(task)
                # Funding collection (if supported)
                if adapter.venue_type in [VenueType.PERPS, VenueType.FUTURES]:
                    task = asyncio.create_task(
                        self._collect_funding(adapter, symbol)
                    )
                    self.tasks.append(task)
                # OI collector (if supported)
                if self._adapter_supports(adapter, 'get_open_interest'):
                    # Try once to see if implemented
                    try:
                        oi = await adapter.get_open_interest(symbol)
                        if oi is not None:
                            task = asyncio.create_task(
                                self._collect_oi(adapter, symbol)
                            )
                            self.tasks.append(task)
                    except Exception:
                        pass
                # Borrow collector (if supported)
                if self._adapter_supports(adapter, 'get_borrow_rate'):
                    try:
                        borrow = await adapter.get_borrow_rate(symbol)
                        if borrow is not None:
                            task = asyncio.create_task(
                                self._collect_borrow(adapter, symbol)
                            )
                            self.tasks.append(task)
                    except Exception:
                        pass
        # Start metrics reporting
        metrics_task = asyncio.create_task(self._report_metrics())
        self.tasks.append(metrics_task)
        logger.info(f"Started {len(self.tasks)} collection tasks")
    async def _collect_oi(self, adapter: VenueAdapter, symbol: str):
        """Collect open interest for a symbol."""
        interval_sec = self.config.get("oi_interval_sec", 20)
        operation_key = f"oi_{adapter.venue}_{symbol}"
        next_ts = time.monotonic()
        while self.running:
            try:
                start_time = time.time()
                oi = await adapter.get_open_interest(symbol)
                latency_ms = (time.time() - start_time) * 1000
                self.metrics.record_success(operation_key, latency_ms)
                if oi:
                    # Dedup
                    if not self.duplicate_detector.is_duplicate("oi", oi.get_hash()):
                        q = self.output_queues[DataType.OI]
                        if q.full():
                            try: q.get_nowait()
                            except asyncio.QueueEmpty: pass
                        try:
                            q.put_nowait(oi)
                        except asyncio.QueueFull:
                            op_key = f"queue_full_oi_{adapter.venue}_{symbol}"
                            self.metrics.record_error(op_key)
                    # Deadman
                    self.metrics.record_data_timestamp(f"{adapter.venue}_{symbol}", oi.timestamp_utc_us)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error collecting {operation_key}: {e}")
                self.metrics.record_error(operation_key)
            next_ts += interval_sec
            await asyncio.sleep(max(0, next_ts - time.monotonic()))

    async def _collect_borrow(self, adapter: VenueAdapter, symbol: str):
        """Collect borrow rate for a symbol."""
        interval_sec = self.config.get("borrow_interval_sec", 120)
        operation_key = f"borrow_{adapter.venue}_{symbol}"
        next_ts = time.monotonic()
        while self.running:
            try:
                start_time = time.time()
                borrow = await adapter.get_borrow_rate(symbol)
                latency_ms = (time.time() - start_time) * 1000
                self.metrics.record_success(operation_key, latency_ms)
                if borrow:
                    if not self.duplicate_detector.is_duplicate("borrow", borrow.get_hash()):
                        q = self.output_queues[DataType.BORROW]
                        if q.full():
                            try: q.get_nowait()
                            except asyncio.QueueEmpty: pass
                        try:
                            q.put_nowait(borrow)
                        except asyncio.QueueFull:
                            op_key = f"queue_full_borrow_{adapter.venue}_{symbol}"
                            self.metrics.record_error(op_key)
                    self.metrics.record_data_timestamp(f"{adapter.venue}_{symbol}", borrow.timestamp_utc_us)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error collecting {operation_key}: {e}")
                self.metrics.record_error(operation_key)
            next_ts += interval_sec
            await asyncio.sleep(max(0, next_ts - time.monotonic()))
        
    async def stop(self):
        """Stop the connector agent."""
        logger.info("Stopping Exchange Connector Agent...")
        self.running = False
        # Cancel all tasks
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        # Stop all adapters
        for adapter in self.adapters.values():
            await adapter.stop()
        logger.info("Exchange Connector Agent stopped")
        
    async def _collect_trades(self, adapter: VenueAdapter, symbol: str):
        """Collect trades for a symbol."""
        last_timestamp = None
        interval_sec = self.config.get("trade_interval_sec", 1)
        operation_key = f"trades_{adapter.venue}_{symbol}"
        next_ts = time.monotonic()
        
        while self.running:
            try:
                start_time = time.time()
                capture_time = int(time.time() * 1_000_000)  # Define capture time here
                
                trades = await adapter.get_trades(symbol, last_timestamp)
                
                latency_ms = (time.time() - start_time) * 1000
                self.metrics.record_success(operation_key, latency_ms)
                
                for trade in trades:
                    # Duplicate detection
                    if not self.duplicate_detector.is_duplicate("trades", trade.get_hash()):
                        q = self.output_queues[DataType.TRADES]
                        if q.full():
                            try:
                                q.get_nowait()
                            except asyncio.QueueEmpty:
                                pass
                        try:
                            q.put_nowait(trade)
                        except asyncio.QueueFull:
                            op_key = f"queue_full_trades_{adapter.venue}_{symbol}"
                            self.metrics.record_error(op_key)
                            # Drop the trade under backpressure
                    # Record capture time for deadman monitoring (not event time)
                    self.metrics.record_data_timestamp(
                        f"{adapter.venue}_{symbol}", 
                        trade.capture_timestamp_utc_us or capture_time
                    )
                
                # Batch update of last_timestamp - compute once after the loop
                if trades:
                    last_timestamp = max(t.venue_timestamp_utc_us for t in trades if t.venue_timestamp_utc_us)
                    
            except asyncio.CancelledError:
                raise  # Ensure fast shutdown
            except Exception as e:
                logger.error(f"Error collecting {operation_key}: {e}")
                self.metrics.record_error(operation_key)
                
            # Cadence drift guard - maintain alignment
            next_ts += interval_sec
            await asyncio.sleep(max(0, next_ts - time.monotonic()))
            
    async def _collect_book(self, adapter: VenueAdapter, symbol: str):
        """Collect order book for a symbol."""
        interval_sec = self.config.get("book_interval_sec", 5)
        operation_key = f"book_{adapter.venue}_{symbol}"
        next_ts = time.monotonic()
        
        while self.running:
            try:
                start_time = time.time()
                
                book = await adapter.get_book(symbol)
                
                latency_ms = (time.time() - start_time) * 1000
                self.metrics.record_success(operation_key, latency_ms)
                
                # Duplicate detection
                if not self.duplicate_detector.is_duplicate("book", book.get_hash()):
                    q = self.output_queues[DataType.BOOK]
                    if q.full():
                        try:
                            q.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                    try:
                        q.put_nowait(book)
                    except asyncio.QueueFull:
                        op_key = f"queue_full_book_{adapter.venue}_{symbol}"
                        self.metrics.record_error(op_key)
                        # Drop the book under backpressure
                    
                self.metrics.record_data_timestamp(
                    f"{adapter.venue}_{symbol}", 
                    book.timestamp_utc_us
                )
                
            except asyncio.CancelledError:
                raise  # Ensure fast shutdown
            except Exception as e:
                logger.error(f"Error collecting {operation_key}: {e}")
                self.metrics.record_error(operation_key)
                
            # Cadence drift guard - maintain alignment
            next_ts += interval_sec
            await asyncio.sleep(max(0, next_ts - time.monotonic()))
            
    async def _collect_funding(self, adapter: VenueAdapter, symbol: str):
        """Collect funding rates for a symbol."""
        interval_sec = self.config.get("funding_interval_sec", 300)  # 5 minutes
        operation_key = f"funding_{adapter.venue}_{symbol}"
        next_ts = time.monotonic()
        
        while self.running:
            try:
                start_time = time.time()
                
                funding = await adapter.get_funding(symbol)
                if funding:
                    latency_ms = (time.time() - start_time) * 1000
                    self.metrics.record_success(operation_key, latency_ms)
                    
                    # Duplicate detection
                    if not self.duplicate_detector.is_duplicate("funding", funding.get_hash()):
                        q = self.output_queues[DataType.FUNDING]
                        if q.full():
                            try:
                                q.get_nowait()
                            except asyncio.QueueEmpty:
                                pass
                        try:
                            q.put_nowait(funding)
                        except asyncio.QueueFull:
                            op_key = f"queue_full_funding_{adapter.venue}_{symbol}"
                            self.metrics.record_error(op_key)
                            # Drop the funding data under backpressure
                        
                    self.metrics.record_data_timestamp(
                        f"{adapter.venue}_{symbol}", 
                        funding.timestamp_utc_us
                    )
                    
            except asyncio.CancelledError:
                raise  # Ensure fast shutdown
            except Exception as e:
                logger.error(f"Error collecting {operation_key}: {e}")
                self.metrics.record_error(operation_key)
                
            # Cadence drift guard - maintain alignment
            next_ts += interval_sec
            await asyncio.sleep(max(0, next_ts - time.monotonic()))
            
    async def _report_metrics(self):
        """Report SLO/KPI metrics periodically."""
        report_interval_sec = self.config.get("metrics_interval_sec", 60)
        
        while self.running:
            try:
                current_time = int(time.time() * 1_000_000)
                uptime_pct = self.metrics.get_uptime_pct()
                
                # Check SLO compliance
                slo_violations = []
                if uptime_pct < self.target_uptime_pct:
                    slo_violations.append(f"Uptime {uptime_pct:.2f}% < {self.target_uptime_pct}%")
                    
                for data_type in ["trades", "book", "funding", "oi", "borrow"]:
                    dup_rate = self.duplicate_detector.get_duplicate_rate(data_type) * 100
                    if dup_rate > self.target_duplicate_rate_pct:
                        slo_violations.append(f"{data_type} duplicate rate {dup_rate:.2f}% > {self.target_duplicate_rate_pct}%")
                
                # Log metrics for all operations
                logger.info(f"Metrics - Uptime: {uptime_pct:.2f}%")
                for venue in self.adapters:
                    for symbol in self.config.get("symbols", []):
                        # Report metrics for all data types
                        for data_type in ["trades", "book", "funding", "oi", "borrow"]:
                            operation_key = f"{data_type}_{venue}_{symbol}"
                            p95_latency = self.metrics.get_p95_latency_ms(operation_key)
                            error_rate = self.metrics.get_error_rate_pct(operation_key)
                            if p95_latency > 0 or error_rate > 0:  # Only log if there's data
                                logger.info(f"  {operation_key} - P95 latency: {p95_latency:.1f}ms, Error rate: {error_rate:.2f}%")
                                
                                # Latency guardrail: warn if p95 > 2x interval
                                if data_type == "trades":
                                    interval_ms = self.config.get("trade_interval_sec", 1) * 1000
                                elif data_type == "book":
                                    interval_ms = self.config.get("book_interval_sec", 5) * 1000
                                else:  # funding
                                    interval_ms = self.config.get("funding_interval_sec", 300) * 1000
                                
                                if p95_latency > 2 * interval_ms:
                                    logger.warning(f"LATENCY ALERT: {operation_key} p95 {p95_latency:.1f}ms > 2x interval ({2 * interval_ms:.0f}ms)")
                        
                        # Deadman alert check
                        venue_symbol_key = f"{venue}_{symbol}"
                        if venue_symbol_key in self.metrics.last_data_times:
                            last_data_time = self.metrics.last_data_times[venue_symbol_key]
                            time_since_last_data = (current_time - last_data_time) / 1_000_000  # Convert to seconds
                            trade_interval = self.config.get("trade_interval_sec", 1)
                            deadman_threshold = trade_interval * 5  # 5x interval threshold
                            
                            if time_since_last_data > deadman_threshold:
                                logger.warning(f"DEADMAN ALERT: {venue_symbol_key} - No data for {time_since_last_data:.1f}s (threshold: {deadman_threshold}s)")
                
                if slo_violations:
                    logger.warning(f"SLO violations: {', '.join(slo_violations)}")
                else:
                    logger.info("All SLOs met ✓")
                    
            except asyncio.CancelledError:
                raise  # Ensure fast shutdown
            except Exception as e:
                logger.error(f"Error reporting metrics: {e}")
                
            await asyncio.sleep(report_interval_sec)
            
    async def get_output_data(self, data_type: DataType, timeout: float = 1.0) -> Optional[Any]:
        """Get normalized output data."""
        try:
            return await asyncio.wait_for(
                self.output_queues[data_type].get(), 
                timeout=timeout
            )
        except asyncio.TimeoutError:
            return None


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

async def main():
    """Example usage of the Exchange Connector Agent."""
    
    # Configuration
    config = {
        "venues": [
            {
                "name": "binance",
                "adapter_class": "BinanceAdapter",
                "api_key": "your_api_key",
                "secret": "your_secret"
            }
        ],
        "symbols": ["BTCUSDT", "ETHUSDT"],
        "trade_interval_sec": 1,
        "book_interval_sec": 5,
        "funding_interval_sec": 300,
        "metrics_interval_sec": 60
    }
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and start agent
    agent = ExchangeConnectorAgent(config)
    
    try:
        await agent.start()
        
        # Example: consume trade data
        while True:
            trade = await agent.get_output_data(DataType.TRADES, timeout=5.0)
            if trade:
                print(f"Trade: {trade.venue} {trade.symbol} {trade.price} {trade.quantity}")
                
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        await agent.stop()


if __name__ == "__main__":
    asyncio.run(main())
