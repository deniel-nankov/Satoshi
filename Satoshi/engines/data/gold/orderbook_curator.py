"""
┌──────────────────────────────────────────────────────────────────────────┐
│ ORDERBOOK CURATOR (Gold Layer GD-3)                                     │
├──────────────────────────────────────────────────────────────────────────┤
│ Purpose: Transform clean orderbook data into fixed-interval snapshots   │
│          with top 20 levels and volume aggregation per level            │
│                                                                          │
│ Data Flow:                                                               │
│   clean.market.book → curated.data.orderbook_snapshot                   │
│                                                                          │
│ Transformations:                                                         │
│   [1] Fixed-interval snapshots (configurable, default 1s)               │
│   [2] Top 20 bid/ask levels (depth limiting)                            │
│   [3] Volume aggregation per level                                      │
│   [4] Book imbalance calculation (bid_vol / ask_vol)                    │
│   [5] Total liquidity aggregation                                       │
│                                                                          │
│ Boundaries:                                                              │
│   ✅ DO:   Snapshot, limit depth, aggregate volume, compute imbalance   │
│   ❌ DON'T: Calculate spread/mid (Feature Layer), execute trades        │
│                                                                          │
│ Consumer Group: orderbook_curator                                        │
│ Publish Topic:  curated.data.orderbook_snapshot                          │
│ Instance:       Independent Kafka consumer (no orchestrator)             │
└──────────────────────────────────────────────────────────────────────────┘
"""

import asyncio
import time
import logging
from dataclasses import dataclass, asdict
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Set, Any, Tuple
from enum import Enum
from collections import defaultdict
import hashlib

from infra.bus.streaming_bus import StreamingBus

# Institutional Controls (optional import)
try:
    from engines.data.gold.gold_layer_institutional_controls import (
        InstitutionalControls,
        SLAMetric,
        DEFAULT_SLA_THRESHOLDS,
    )
    INSTITUTIONAL_CONTROLS_AVAILABLE = True
except ImportError:
    INSTITUTIONAL_CONTROLS_AVAILABLE = False
    InstitutionalControls = None  # type: ignore
    SLAMetric = None  # type: ignore

# =============================
# LOGGING CONFIGURATION
# =============================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================
# ENUMS & DATACLASSES
# =============================

class Side(Enum):
    """Order book side: bid or ask."""
    BID = "bid"
    ASK = "ask"

@dataclass
class BookLevel:
    """Single price level in the orderbook."""
    price: Decimal
    quantity: Decimal
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "price": float(self.price),
            "quantity": float(self.quantity)
        }

@dataclass
class LiquidityBucket:
    """
    Liquidity aggregated within a price distance band from mid.
    Gold Layer Task #4: Liquidity Bucketing for market depth analysis.
    """
    distance_bps: int  # Distance from mid in basis points (10bps = 0.1%)
    bid_volume: Decimal  # Total bid volume within this bucket
    ask_volume: Decimal  # Total ask volume within this bucket
    bid_levels_count: int  # Number of bid levels in bucket
    ask_levels_count: int  # Number of ask levels in bucket
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "distance_bps": self.distance_bps,
            "bid_volume": float(self.bid_volume),
            "ask_volume": float(self.ask_volume),
            "bid_levels_count": self.bid_levels_count,
            "ask_levels_count": self.ask_levels_count
        }

@dataclass
class OrderbookSnapshot:
    """
    Curated orderbook snapshot with fixed-interval sampling.
    
    Gold Layer additions:
      - Fixed-interval sampling (time-based snapshots)
      - Top N levels (depth limiting)
      - Volume aggregation per level
      - Book imbalance (bid_volume / ask_volume)
      - Total liquidity metrics
      - Liquidity bucketing by distance from mid (Task #4)
    """
    symbol: str
    venue: str
    snapshot_time_utc_us: int  # Snapshot timestamp (fixed interval)
    
    # Top N levels (default 20)
    bids: List[BookLevel]  # Sorted descending by price
    asks: List[BookLevel]  # Sorted ascending by price
    
    # Aggregated metrics (Gold Layer enrichment)
    total_bid_volume: Decimal
    total_ask_volume: Decimal
    book_imbalance: Decimal  # bid_volume / ask_volume (>1 = bid pressure)
    num_bid_levels: int
    num_ask_levels: int
    
    # Best levels (top of book)
    best_bid: Optional[Decimal]
    best_ask: Optional[Decimal]
    
    # Liquidity bucketing (Task #4: Gold Layer enhancement)
    # Aggregated liquidity at different price distances from mid
    # Buckets: 10bps (0.1%), 50bps (0.5%), 100bps (1.0%), 500bps (5.0%), 1000bps (10.0%)
    liquidity_buckets: List[LiquidityBucket]
    mid_price: Optional[Decimal]  # Midpoint used for bucket calculations
    
    # Source metadata
    source_timestamp_utc_us: int  # Original book timestamp
    sequence_number: Optional[int]
    venue_timestamp_utc_us: Optional[int]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "venue": self.venue,
            "snapshot_time_utc_us": self.snapshot_time_utc_us,
            "bids": [level.to_dict() for level in self.bids],
            "asks": [level.to_dict() for level in self.asks],
            "total_bid_volume": float(self.total_bid_volume),
            "total_ask_volume": float(self.total_ask_volume),
            "book_imbalance": float(self.book_imbalance),
            "num_bid_levels": self.num_bid_levels,
            "num_ask_levels": self.num_ask_levels,
            "best_bid": float(self.best_bid) if self.best_bid is not None else None,
            "best_ask": float(self.best_ask) if self.best_ask is not None else None,
            "liquidity_buckets": [bucket.to_dict() for bucket in self.liquidity_buckets],
            "mid_price": float(self.mid_price) if self.mid_price is not None else None,
            "source_timestamp_utc_us": self.source_timestamp_utc_us,
            "sequence_number": self.sequence_number,
            "venue_timestamp_utc_us": self.venue_timestamp_utc_us
        }

# =============================
# ORDERBOOK SNAPSHOT BUILDER
# =============================

class OrderbookSnapshotBuilder:
    """
    Core logic for building fixed-interval orderbook snapshots.
    Maintains latest book state and samples at regular intervals.
    """
    
    def __init__(self, snapshot_interval_us: int = 1_000_000, max_levels: int = 20):
        """
        Initialize snapshot builder.
        
        Args:
            snapshot_interval_us: Snapshot interval in microseconds (default 1s)
            max_levels: Maximum number of levels per side (default 20)
        """
        self.snapshot_interval_us = snapshot_interval_us
        self.max_levels = max_levels
        
        # Latest book state per symbol:venue
        self.latest_books: Dict[str, Dict[str, Any]] = {}  # "symbol:venue" → raw book data
        
        # Last snapshot time per symbol:venue
        self.last_snapshot_times: Dict[str, int] = {}
        
        # Deduplication cache (hash → timestamp)
        self.seen_snapshots: Dict[str, int] = {}
        self.cache_max_size = 10000
        
        logger.info(f"OrderbookSnapshotBuilder initialized (interval={snapshot_interval_us}us, max_levels={max_levels})")
    
    def update_book(self, raw_book: Dict[str, Any]) -> Optional[OrderbookSnapshot]:
        """
        Update latest book state and create snapshot if interval elapsed.
        
        Args:
            raw_book: Raw book data from clean.market.book
        
        Returns:
            OrderbookSnapshot if interval elapsed, None otherwise
        """
        try:
            # Extract metadata
            symbol = raw_book.get("symbol", "")
            venue = raw_book.get("venue", "")
            timestamp_utc_us = raw_book.get("timestamp", 0)
            
            if not symbol or not venue or timestamp_utc_us <= 0:
                logger.warning(f"Invalid book: missing required fields (symbol={symbol}, venue={venue}, timestamp={timestamp_utc_us})")
                return None
            
            book_key = f"{symbol}:{venue}"
            
            # Update latest book state
            self.latest_books[book_key] = raw_book
            
            # Check if snapshot interval elapsed
            last_snapshot_time = self.last_snapshot_times.get(book_key, 0)
            time_since_snapshot = timestamp_utc_us - last_snapshot_time
            
            if time_since_snapshot >= self.snapshot_interval_us:
                # Create snapshot
                snapshot = self._build_snapshot(raw_book, timestamp_utc_us)
                
                if snapshot:
                    # Update last snapshot time
                    self.last_snapshot_times[book_key] = timestamp_utc_us
                    
                    # Deduplication check
                    snapshot_hash = self._get_snapshot_hash(snapshot)
                    if snapshot_hash in self.seen_snapshots:
                        return None  # Duplicate
                    
                    self.seen_snapshots[snapshot_hash] = timestamp_utc_us
                    self._cleanup_cache_if_needed()
                    
                    return snapshot
            
            return None
            
        except Exception as e:
            logger.error(f"Error updating book: {e}")
            return None
    
    def force_snapshot(self, symbol: str, venue: str, snapshot_time_us: int) -> Optional[OrderbookSnapshot]:
        """
        Force create snapshot from latest book state (used during shutdown).
        
        Args:
            symbol: Symbol identifier
            venue: Venue identifier
            snapshot_time_us: Snapshot timestamp
        
        Returns:
            OrderbookSnapshot or None if no book state available
        """
        book_key = f"{symbol}:{venue}"
        raw_book = self.latest_books.get(book_key)
        
        if not raw_book:
            return None
        
        return self._build_snapshot(raw_book, snapshot_time_us)
    
    def _build_snapshot(self, raw_book: Dict[str, Any], snapshot_time_us: int) -> Optional[OrderbookSnapshot]:
        """Build OrderbookSnapshot from raw book data."""
        try:
            symbol = raw_book.get("symbol", "")
            venue = raw_book.get("venue", "")
            source_timestamp_utc_us = raw_book.get("timestamp", snapshot_time_us)
            sequence_number = raw_book.get("sequence_number")
            venue_timestamp_utc_us = raw_book.get("venue_timestamp_utc_us")
            
            # Parse bids and asks
            raw_bids = raw_book.get("bids", [])
            raw_asks = raw_book.get("asks", [])
            
            # Convert to BookLevel objects
            bids = self._parse_book_levels(raw_bids, Side.BID)
            asks = self._parse_book_levels(raw_asks, Side.ASK)
            
            if not bids or not asks:
                logger.warning(f"Empty orderbook for {symbol}@{venue}")
                return None
            
            # Sort and limit to top N levels
            bids = sorted(bids, key=lambda x: x.price, reverse=True)[:self.max_levels]
            asks = sorted(asks, key=lambda x: x.price)[:self.max_levels]
            
            # Compute aggregated metrics
            total_bid_volume: Decimal = sum((level.quantity for level in bids), start=Decimal("0"))
            total_ask_volume: Decimal = sum((level.quantity for level in asks), start=Decimal("0"))
            
            # Book imbalance (bid pressure indicator)
            book_imbalance: Decimal
            if total_ask_volume > 0:
                book_imbalance = total_bid_volume / total_ask_volume
            else:
                book_imbalance = Decimal("999.0")  # Infinite bid pressure
            
            # Best levels
            best_bid = bids[0].price if bids else None
            best_ask = asks[0].price if asks else None
            
            # Task #4: Compute liquidity buckets (aggregate volume by distance from mid)
            mid_price: Optional[Decimal] = None
            if best_bid is not None and best_ask is not None:
                mid_price = (best_bid + best_ask) / Decimal("2")
            
            liquidity_buckets = self._compute_liquidity_buckets(
                bids=bids,
                asks=asks,
                mid_price=mid_price
            )
            
            snapshot = OrderbookSnapshot(
                symbol=symbol,
                venue=venue,
                snapshot_time_utc_us=snapshot_time_us,
                bids=bids,
                asks=asks,
                total_bid_volume=total_bid_volume,
                total_ask_volume=total_ask_volume,
                book_imbalance=book_imbalance,
                num_bid_levels=len(bids),
                num_ask_levels=len(asks),
                best_bid=best_bid,
                best_ask=best_ask,
                liquidity_buckets=liquidity_buckets,
                mid_price=mid_price,
                source_timestamp_utc_us=source_timestamp_utc_us,
                sequence_number=sequence_number,
                venue_timestamp_utc_us=venue_timestamp_utc_us
            )
            
            return snapshot
            
        except Exception as e:
            logger.error(f"Error building snapshot: {e}")
            return None
    
    def _parse_book_levels(self, raw_levels: List[Any], side: Side) -> List[BookLevel]:
        """Parse raw book levels into BookLevel objects."""
        levels = []
        
        for raw_level in raw_levels:
            try:
                # Support both dict and list formats
                if isinstance(raw_level, dict):
                    price = self._parse_decimal(raw_level.get("price"))
                    quantity = self._parse_decimal(raw_level.get("quantity"))
                elif isinstance(raw_level, (list, tuple)) and len(raw_level) >= 2:
                    price = self._parse_decimal(raw_level[0])
                    quantity = self._parse_decimal(raw_level[1])
                else:
                    logger.warning(f"Invalid level format: {raw_level}")
                    continue
                
                if price is not None and quantity is not None and price > 0 and quantity > 0:
                    levels.append(BookLevel(price=price, quantity=quantity))
            
            except Exception as e:
                logger.warning(f"Error parsing level {raw_level}: {e}")
                continue
        
        return levels
    
    def _parse_decimal(self, value: Any) -> Optional[Decimal]:
        """Safely parse Decimal from various types."""
        if value is None:
            return None
        try:
            if isinstance(value, Decimal):
                return value
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return None
    
    def _get_snapshot_hash(self, snapshot: OrderbookSnapshot) -> str:
        """Generate stable hash for snapshot deduplication."""
        # Use top 5 levels for deduplication
        bid_str = ",".join([f"{level.price}:{level.quantity}" for level in snapshot.bids[:5]])
        ask_str = ",".join([f"{level.price}:{level.quantity}" for level in snapshot.asks[:5]])
        
        hash_input = f"{snapshot.venue}:{snapshot.symbol}:{snapshot.snapshot_time_utc_us}:{bid_str}:{ask_str}"
        
        if snapshot.sequence_number is not None:
            hash_input += f":{snapshot.sequence_number}"
        
        return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()
    
    def _compute_liquidity_buckets(
        self,
        bids: List[BookLevel],
        asks: List[BookLevel],
        mid_price: Optional[Decimal]
    ) -> List[LiquidityBucket]:
        """
        Task #4: Compute liquidity buckets by distance from mid.
        
        Aggregates volume at different price distances from mid:
          - 10bps (0.1%): Tight spread, high frequency trading zone
          - 50bps (0.5%): Immediate liquidity for medium-sized orders
          - 100bps (1.0%): Standard retail/small institutional depth
          - 500bps (5.0%): Large institutional depth
          - 1000bps (10.0%): Deep reserve liquidity
        
        This is pure data aggregation (Gold Layer compliant), NOT prediction.
        """
        # Define bucket thresholds in basis points (1 bp = 0.01%)
        bucket_thresholds_bps = [10, 50, 100, 500, 1000]  # 0.1%, 0.5%, 1.0%, 5.0%, 10.0%
        
        if mid_price is None or mid_price <= 0:
            # No mid price: return empty buckets
            return [
                LiquidityBucket(
                    distance_bps=threshold,
                    bid_volume=Decimal("0"),
                    ask_volume=Decimal("0"),
                    bid_levels_count=0,
                    ask_levels_count=0
                )
                for threshold in bucket_thresholds_bps
            ]
        
        # Initialize bucket accumulators
        buckets = []
        
        for threshold_bps in bucket_thresholds_bps:
            # Convert basis points to decimal multiplier
            # threshold_bps of 10 means 0.1% = 0.001 = mid * (1 ± 0.001)
            threshold_decimal = Decimal(threshold_bps) / Decimal("10000")
            
            # Price boundaries for this bucket
            lower_bound = mid_price * (Decimal("1") - threshold_decimal)
            upper_bound = mid_price * (Decimal("1") + threshold_decimal)
            
            # Aggregate bid volume within bucket (prices >= lower_bound)
            bid_volume = Decimal("0")
            bid_levels_count = 0
            for bid in bids:
                if bid.price >= lower_bound:
                    bid_volume += bid.quantity
                    bid_levels_count += 1
                else:
                    break  # Bids are sorted descending, so stop when below threshold
            
            # Aggregate ask volume within bucket (prices <= upper_bound)
            ask_volume = Decimal("0")
            ask_levels_count = 0
            for ask in asks:
                if ask.price <= upper_bound:
                    ask_volume += ask.quantity
                    ask_levels_count += 1
                else:
                    break  # Asks are sorted ascending, so stop when above threshold
            
            bucket = LiquidityBucket(
                distance_bps=threshold_bps,
                bid_volume=bid_volume,
                ask_volume=ask_volume,
                bid_levels_count=bid_levels_count,
                ask_levels_count=ask_levels_count
            )
            buckets.append(bucket)
        
        return buckets
    
    def _cleanup_cache_if_needed(self):
        """Cleanup deduplication cache if it exceeds max size."""
        if len(self.seen_snapshots) > self.cache_max_size:
            # Remove oldest 20% of entries
            sorted_items = sorted(self.seen_snapshots.items(), key=lambda x: x[1])
            cutoff_index = len(sorted_items) // 5
            for snapshot_hash, _ in sorted_items[:cutoff_index]:
                del self.seen_snapshots[snapshot_hash]
            logger.info(f"Cleaned deduplication cache: {len(sorted_items[:cutoff_index])} entries removed")

# =============================
# ORDERBOOK CURATOR AGENT
# =============================

class OrderbookCurator:
    """
    Gold Layer Orderbook Curator.
    
    Subscribes:  clean.market.book
    Publishes:   curated.data.orderbook_snapshot
    Consumer Group: orderbook_curator
    
    Transformations:
      1. Fixed-interval snapshots (default 1s)
      2. Top 20 bid/ask levels (depth limiting)
      3. Volume aggregation per level
      4. Book imbalance calculation (bid_vol / ask_vol)
      5. Total liquidity aggregation
    
    Enterprise Features:
      - Circuit breaker (auto-recovery after failures)
      - Health monitoring (periodic status checks)
      - Metrics tracking (snapshots created, books processed)
      - Incident reporting (critical failures)
      - Graceful shutdown (final snapshots on exit)
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.running = False
        
        # Core components
        self.streaming_bus: Optional[StreamingBus] = None
        
        # Snapshot configuration
        snapshot_interval_sec = self.config.get("snapshot_interval_sec", 1.0)
        snapshot_interval_us = int(snapshot_interval_sec * 1_000_000)
        max_levels = self.config.get("max_levels", 20)
        
        self.snapshot_builder = OrderbookSnapshotBuilder(
            snapshot_interval_us=snapshot_interval_us,
            max_levels=max_levels
        )
        
        # Circuit breaker
        self.consecutive_failures = 0
        self.max_consecutive_failures = 5
        self.circuit_open = False
        self.circuit_recovery_time = 0
        self.circuit_recovery_delay_sec = 60
        
        # Metrics
        self.metrics_collector = self._get_metrics_collector()
        self.snapshots_created = 0
        self.books_processed = 0
        self.errors_count = 0
        self.start_time = time.time()
        
        # Health monitoring
        self.health_check_interval_sec = 60
        self.last_health_check = 0
        
        # Institutional Controls (optional)
        self.institutional_controls: Optional[InstitutionalControls] = None  # type: ignore
        enable_controls = self.config.get("enable_institutional_controls", True)
        clickhouse_client = self.config.get("clickhouse_client")
        
        if enable_controls and INSTITUTIONAL_CONTROLS_AVAILABLE and InstitutionalControls is not None:
            try:
                # Custom SLA thresholds for orderbook snapshots
                sla_thresholds = {
                    SLAMetric.LATENCY: 200.0,  # type: ignore  # 200ms P95 latency (book processing)
                    SLAMetric.COMPLETENESS: 0.99,  # type: ignore  # 99% completeness
                    SLAMetric.FRESHNESS: 2.0,  # type: ignore  # 2 second max lag
                    SLAMetric.QUALITY_SCORE: 0.97,  # type: ignore  # 97% quality score
                }
                
                self.institutional_controls = InstitutionalControls(
                    component_name="orderbook_curator",
                    component_version="1.0.0",
                    clickhouse_client=clickhouse_client,
                    sla_thresholds=sla_thresholds
                )
                logger.info("✅ Institutional controls enabled for Orderbook Curator")
            except Exception as e:
                logger.warning(f"⚠️ Failed to initialize institutional controls: {e}")
                self.institutional_controls = None
        
        logger.info(f"OrderbookCurator initialized (interval={snapshot_interval_sec}s, max_levels={max_levels})")
    
    def _get_metrics_collector(self):
        """Get metrics collector from monitoring infrastructure."""
        try:
            from infra.monitoring.prometheus_metrics import get_metrics_collector
            return get_metrics_collector()
        except (ImportError, Exception):
            logger.warning("Metrics collector not available")
            return None
    
    async def start(self):
        """Start the Orderbook Curator agent (non-blocking)."""
        self.running = True
        logger.info("Starting Orderbook Curator...")
        
        try:
            # Initialize streaming bus
            bus_config = self.config.get("bus_config", {
                "bootstrap_servers": ["localhost:9092"],
                "client_id": "orderbook-curator"
            })
            self.streaming_bus = StreamingBus(config=bus_config)
            
            # Start health monitoring (background task)
            asyncio.create_task(self._health_monitor())
            
            # Subscribe to clean.market.book (background task - non-blocking)
            input_topic = "clean.market.book"
            pool_size = self.config.get("pool_size", 4)
            
            logger.info(f"Subscribing to {input_topic} with pool_size={pool_size}...")
            
            # Launch subscription in background task instead of awaiting
            subscription_task = asyncio.create_task(
                self.streaming_bus.subscribe_with_worker_pool(
                    topics=[input_topic],
                    consumer_group="orderbook_curator",
                    handler=self._process_book,
                    pool_size=pool_size
                )
            )
            
            # Store task for cleanup
            self._subscription_task = subscription_task
            
            logger.info("✅ Orderbook Curator background tasks started (subscription, health monitor)")
            
        except Exception as e:
            self.running = False
            logger.error(f"Failed to start Orderbook Curator: {e}")
            await self._publish_incident("startup_failure", str(e))
            raise
    
    async def _process_book(self, topic: str, partition_key: str, payload: Dict[str, Any], headers: Dict[str, str]):
        """
        Process individual orderbook from clean.market.book.
        Creates snapshot if interval elapsed.
        """
        try:
            # Circuit breaker check
            if self.circuit_open:
                current_time = time.time()
                if current_time < self.circuit_recovery_time:
                    return  # Circuit still open
                else:
                    # Attempt recovery
                    logger.info("Circuit breaker recovery attempt")
                    self.circuit_open = False
                    self.consecutive_failures = 0
            
            self.books_processed += 1
            
            # Update book state and check for snapshot
            snapshot = self.snapshot_builder.update_book(payload)
            
            if snapshot:
                await self._publish_snapshot(snapshot)
                self.snapshots_created += 1
                
                # Metrics
                if self.metrics_collector and hasattr(self.metrics_collector, 'increment_counter'):
                    self.metrics_collector.increment_counter("orderbook_curator.snapshots_created")
                
                logger.debug(f"Snapshot created: {snapshot.symbol}@{snapshot.venue} ({snapshot.num_bid_levels}x{snapshot.num_ask_levels} levels)")
            
            # Metrics
            if self.metrics_collector and hasattr(self.metrics_collector, 'increment_counter'):
                self.metrics_collector.increment_counter("orderbook_curator.books_processed")
            
            # Reset failure counter on success
            if self.consecutive_failures > 0:
                self.consecutive_failures = 0
            
        except Exception as e:
            self.errors_count += 1
            self.consecutive_failures += 1
            logger.error(f"Error processing orderbook: {e}")
            
            # Circuit breaker trigger
            if self.consecutive_failures >= self.max_consecutive_failures:
                self.circuit_open = True
                self.circuit_recovery_time = time.time() + self.circuit_recovery_delay_sec
                logger.error(f"Circuit breaker OPEN after {self.consecutive_failures} failures (recovery in {self.circuit_recovery_delay_sec}s)")
                await self._publish_incident("circuit_breaker_open", f"Failures: {self.consecutive_failures}")
            
            # Metrics
            if self.metrics_collector and hasattr(self.metrics_collector, 'increment_counter'):
                self.metrics_collector.increment_counter("orderbook_curator.errors")
    
    async def _publish_snapshot(self, snapshot: OrderbookSnapshot):
        """Publish curated orderbook snapshot with institutional headers."""
        try:
            if not self.streaming_bus:
                logger.error("Streaming bus not available")
                return
            
            partition_key = f"{snapshot.symbol}:{snapshot.venue}"
            
            # Use canonical headers for data lineage and audit compliance
            if not hasattr(self, '_sequence_number'):
                self._sequence_number = 0
            
            await self.streaming_bus.publish_with_canonical_headers(
                topic="curated.data.orderbook_snapshot",
                partition_key=partition_key,
                payload=snapshot.to_dict(),
                source_id="orderbook_curator.001",
                sequence_number=self._sequence_number,
                producer_version="1.0.0"
            )
            
            self._sequence_number += 1
            
        except Exception as e:
            logger.error(f"Error publishing snapshot: {e}")
            raise
    
    async def _health_monitor(self):
        """Periodic health monitoring and metrics reporting."""
        logger.info(f"Health monitor started (interval={self.health_check_interval_sec}s)")
        
        while self.running:
            try:
                await asyncio.sleep(self.health_check_interval_sec)
                
                current_time = time.time()
                uptime_sec = current_time - self.start_time
                uptime_min = uptime_sec / 60
                
                # Compute rates
                snapshots_per_min = self.snapshots_created / uptime_min if uptime_min > 0 else 0
                books_per_min = self.books_processed / uptime_min if uptime_min > 0 else 0
                
                # Snapshot efficiency (what % of books result in snapshots)
                snapshot_rate_pct = (self.snapshots_created / self.books_processed * 100) if self.books_processed > 0 else 0
                
                # Log health status
                logger.info(
                    f"Health Check - "
                    f"Uptime: {uptime_min:.1f}m | "
                    f"Snapshots: {self.snapshots_created} ({snapshots_per_min:.1f}/min, {snapshot_rate_pct:.1f}% of books) | "
                    f"Books: {self.books_processed} ({books_per_min:.1f}/min) | "
                    f"Errors: {self.errors_count} | "
                    f"Circuit: {'OPEN' if self.circuit_open else 'CLOSED'}"
                )
                
                # Publish metrics
                if self.metrics_collector:
                    if hasattr(self.metrics_collector, 'set_gauge'):
                        self.metrics_collector.set_gauge("orderbook_curator.uptime_sec", uptime_sec)
                        self.metrics_collector.set_gauge("orderbook_curator.snapshots_created_total", self.snapshots_created)
                        self.metrics_collector.set_gauge("orderbook_curator.books_processed_total", self.books_processed)
                        self.metrics_collector.set_gauge("orderbook_curator.errors_total", self.errors_count)
                        self.metrics_collector.set_gauge("orderbook_curator.circuit_open", 1 if self.circuit_open else 0)
                        self.metrics_collector.set_gauge("orderbook_curator.snapshot_rate_pct", snapshot_rate_pct)
                
                self.last_health_check = current_time
                
            except asyncio.CancelledError:
                logger.info("Health monitor cancelled")
                break
            except Exception as e:
                logger.error(f"Error in health monitor: {e}")
    
    async def _publish_incident(self, incident_type: str, details: str):
        """Publish critical incident with institutional headers for audit trail."""
        try:
            if not self.streaming_bus:
                return
            
            incident = {
                "agent": "orderbook_curator",
                "incident_type": incident_type,
                "details": details,
                "timestamp_utc_us": int(time.time() * 1_000_000),
                "severity": "critical"
            }
            
            # Incidents need lineage tracking for regulatory compliance
            if not hasattr(self, '_incident_sequence'):
                self._incident_sequence = 0
            
            await self.streaming_bus.publish_with_canonical_headers(
                topic="incidents.orderbook_curator",
                partition_key="orderbook_curator",
                payload=incident,
                source_id="orderbook_curator.001",
                sequence_number=self._incident_sequence,
                producer_version="1.0.0"
            )
            
            self._incident_sequence += 1
            
        except Exception as e:
            logger.error(f"Error publishing incident: {e}")
    
    async def stop(self):
        """Graceful shutdown."""
        logger.info("Stopping Orderbook Curator...")
        self.running = False
        
        # Create final snapshots from latest book states
        try:
            current_time_us = int(time.time() * 1_000_000)
            book_keys = list(self.snapshot_builder.latest_books.keys())
            
            for book_key in book_keys:
                try:
                    symbol, venue = book_key.split(":", 1)
                    snapshot = self.snapshot_builder.force_snapshot(symbol, venue, current_time_us)
                    if snapshot:
                        await self._publish_snapshot(snapshot)
                        logger.info(f"Final snapshot published: {symbol}@{venue}")
                except Exception as e:
                    logger.error(f"Error publishing final snapshot for {book_key}: {e}")
        except Exception as e:
            logger.error(f"Error during final snapshot creation: {e}")
        
        logger.info("Orderbook Curator stopped")

# =============================
# MAIN ENTRY POINT
# =============================

async def main():
    """Main entry point for Orderbook Curator."""
    
    # Example configuration
    config = {
        "snapshot_interval_sec": 1.0,  # 1-second snapshots
        "max_levels": 20,              # Top 20 levels per side
        "pool_size": 4                 # Kafka consumer pool size
    }
    
    curator = OrderbookCurator(config)
    
    try:
        await curator.start()
        
        # Keep running until interrupted
        while curator.running:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await curator.stop()

if __name__ == "__main__":
    asyncio.run(main())
