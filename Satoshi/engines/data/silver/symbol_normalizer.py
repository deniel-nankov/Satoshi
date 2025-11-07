"""
Symbol Normalizer - Gold Layer (Curated Data)

Mission: Unify symbol naming across venues → curated.data.symbols
Independent Kafka consumer operating in parallel with other Gold Layer curators.

Architecture Compliance:
- Gold Layer Component: Cross-venue symbol unification
- Input: clean.* (all topics with venue-specific symbols)
- Output: curated.data.symbols (canonical format + metadata + cross-venue mapping)
- Consumer Group: symbol_normalizer

Data Transformations:
✅ DOES: Cross-venue mapping, metadata enrichment, registry integration
❌ DOES NOT: API formatting (Bronze Layer concern), feature engineering, trading signals

Examples:
- Binance "BTCUSDT" + Kraken "XBTUSDT" → Canonical "BTC/USDT"
- Coinbase "BTC-USD" → Canonical "BTC/USD"
- OKX "BTC-USDT" → Canonical "BTC/USDT"

SLOs/KPIs:
- Mapping accuracy ≥99.9%
- Coverage: 100% of traded symbols
- Latency p95 < 50ms per symbol
- Uptime ≥99.5%

Medallion Architecture: Clean → Gold (Curated)
"""

import asyncio
import logging
import time
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple, Protocol
import json

# Infrastructure imports
from infra.bus.streaming_bus import StreamingBus
from infra.monitoring.prometheus_metrics import get_metrics_collector

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

logger = logging.getLogger(__name__)


# ============================================================================
# TYPE PROTOCOLS
# ============================================================================

class MetricsCollector(Protocol):
    """
    Protocol defining the metrics collector interface.
    Enables proper type checking while maintaining loose coupling.
    """
    
    def increment_counter(self, name: str, value: float = 1, labels: Optional[Dict[str, str]] = None) -> None:
        """Increment a counter metric."""
        ...
    
    def observe_histogram(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Record a histogram observation."""
        ...
    
    def set_gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Set a gauge metric value."""
        ...


# ============================================================================
# SCHEMAS & DATA STRUCTURES
# ============================================================================

class AssetType(Enum):
    """Asset classification."""
    SPOT = "spot"
    PERPETUAL = "perpetual"
    FUTURES = "futures"
    OPTIONS = "options"
    STABLECOIN = "stablecoin"


@dataclass
class SymbolMetadata:
    """Comprehensive symbol metadata."""
    canonical_symbol: str  # e.g., "BTC/USDT"
    base_asset: str  # e.g., "BTC"
    quote_asset: str  # e.g., "USDT"
    asset_type: AssetType
    
    # Trading specifications
    base_decimals: int = 8
    quote_decimals: int = 8
    min_quantity: Optional[float] = None
    max_quantity: Optional[float] = None
    
    # Venue mappings
    venue_symbols: Dict[str, str] = field(default_factory=dict)
    
    # Historical metadata
    first_seen_utc_us: int = 0
    listing_date: Optional[str] = None  # ISO 8601
    
    # Quality metadata
    active_venues: Set[str] = field(default_factory=set)
    last_update_utc_us: int = 0
    
    # Venue Latency Tracking (Gold Layer - COMPLIANT: data quality, NOT trading logic)
    venue_last_seen_utc_us: Dict[str, int] = field(default_factory=dict)  # venue → last update timestamp
    venue_staleness_score: Dict[str, float] = field(default_factory=dict)  # venue → staleness (0.0=fresh, 1.0=stale)
    
    def to_dict(self) -> dict:
        """Serialize for Kafka."""
        return {
            "canonical_symbol": self.canonical_symbol,
            "base_asset": self.base_asset,
            "quote_asset": self.quote_asset,
            "asset_type": self.asset_type.value,
            "base_decimals": self.base_decimals,
            "quote_decimals": self.quote_decimals,
            "min_quantity": self.min_quantity,
            "max_quantity": self.max_quantity,
            "venue_symbols": self.venue_symbols,
            "first_seen_utc_us": self.first_seen_utc_us,
            "listing_date": self.listing_date,
            "active_venues": list(self.active_venues),
            "last_update_utc_us": self.last_update_utc_us,
            "venue_last_seen_utc_us": self.venue_last_seen_utc_us,
            "venue_staleness_score": self.venue_staleness_score
        }


@dataclass
class NormalizationResult:
    """Result of symbol normalization."""
    raw_symbol: str
    venue: str
    canonical_symbol: str
    metadata: SymbolMetadata
    confidence: float  # 0.0-1.0 (mapping certainty)
    normalized_at_utc_us: int
    
    def to_dict(self) -> dict:
        """Serialize for Kafka."""
        return {
            "raw_symbol": self.raw_symbol,
            "venue": self.venue,
            "canonical_symbol": self.canonical_symbol,
            "metadata": self.metadata.to_dict(),
            "confidence": self.confidence,
            "normalized_at_utc_us": self.normalized_at_utc_us
        }


@dataclass
class NormalizerMetrics:
    """Metrics tracking."""
    symbols_processed: int = 0
    new_symbols_discovered: int = 0
    mapping_failures: int = 0
    venue_coverage: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    avg_confidence: float = 1.0
    
    def get_stats(self) -> dict:
        """Get current metrics."""
        return {
            "symbols_processed": self.symbols_processed,
            "new_symbols_discovered": self.new_symbols_discovered,
            "mapping_failures": self.mapping_failures,
            "venue_coverage": dict(self.venue_coverage),
            "avg_confidence": round(self.avg_confidence, 3)
        }


# ============================================================================
# CANONICAL SYMBOL MAPPING REGISTRY
# ============================================================================

class CanonicalSymbolRegistry:
    """
    Centralized registry for symbol normalization.
    Single source of truth for cross-venue symbol mapping.
    
    Future Enhancement: Integrate with PostgresRegistry for persistent storage.
    """
    
    def __init__(self):
        # Canonical symbol → metadata
        self.canonical_metadata: Dict[str, SymbolMetadata] = {}
        
        # Venue-specific symbol → canonical
        # Key: (venue, symbol) → canonical_symbol
        self.venue_to_canonical: Dict[Tuple[str, str], str] = {}
        
        # Reverse mapping: canonical → venue-specific
        # Key: canonical_symbol → {venue: symbol}
        self.canonical_to_venue: Dict[str, Dict[str, str]] = defaultdict(dict)
        
        # Initialize with known mappings
        self._initialize_known_mappings()
    
    def _initialize_known_mappings(self) -> None:
        """Initialize with known symbol mappings across venues."""
        
        # Major cryptocurrency pairs
        major_pairs = [
            # ======== TOP 10 BY MARKET CAP ========
            ("BTC/USD", "BTC", "USD", AssetType.SPOT, {
                "coinbase": "BTC-USD",
                "gemini": "BTCUSD"
            }),
            ("ETH/USD", "ETH", "USD", AssetType.SPOT, {
                "coinbase": "ETH-USD",
                "gemini": "ETHUSD"
            }),
            ("SOL/USD", "SOL", "USD", AssetType.SPOT, {
                "coinbase": "SOL-USD",
                "gemini": "SOLUSD"
            }),
            ("XRP/USD", "XRP", "USD", AssetType.SPOT, {
                "coinbase": "XRP-USD"
            }),
            ("ADA/USD", "ADA", "USD", AssetType.SPOT, {
                "coinbase": "ADA-USD",
                "gemini": "ADAUSD"
            }),
            ("AVAX/USD", "AVAX", "USD", AssetType.SPOT, {
                "coinbase": "AVAX-USD",
                "gemini": "AVAXUSD"
            }),
            ("DOT/USD", "DOT", "USD", AssetType.SPOT, {
                "coinbase": "DOT-USD",
                "gemini": "DOTUSD"
            }),
            ("MATIC/USD", "MATIC", "USD", AssetType.SPOT, {
                "coinbase": "MATIC-USD",
                "gemini": "MATICUSD"
            }),
            ("LINK/USD", "LINK", "USD", AssetType.SPOT, {
                "coinbase": "LINK-USD",
                "gemini": "LINKUSD"
            }),
            ("UNI/USD", "UNI", "USD", AssetType.SPOT, {
                "coinbase": "UNI-USD",
                "gemini": "UNIUSD"
            }),
            
            # ======== ADDITIONAL L1s & POPULAR ALTS ========
            ("ATOM/USD", "ATOM", "USD", AssetType.SPOT, {
                "coinbase": "ATOM-USD",
                "gemini": "ATOMUSD"
            }),
            ("ALGO/USD", "ALGO", "USD", AssetType.SPOT, {
                "coinbase": "ALGO-USD",
                "gemini": "ALGOUSD"
            }),
            ("APT/USD", "APT", "USD", AssetType.SPOT, {
                "coinbase": "APT-USD",
                "gemini": "APTUSD"
            }),
            ("SUI/USD", "SUI", "USD", AssetType.SPOT, {
                "coinbase": "SUI-USD"
            }),
            ("NEAR/USD", "NEAR", "USD", AssetType.SPOT, {
                "coinbase": "NEAR-USD",
                "gemini": "NEARUSD"
            }),
            ("FTM/USD", "FTM", "USD", AssetType.SPOT, {
                "coinbase": "FTM-USD",
                "gemini": "FTMUSD"
            }),
            ("LTC/USD", "LTC", "USD", AssetType.SPOT, {
                "coinbase": "LTC-USD",
                "gemini": "LTCUSD"
            }),
            ("BCH/USD", "BCH", "USD", AssetType.SPOT, {
                "coinbase": "BCH-USD",
                "gemini": "BCHUSD"
            }),
            ("ICP/USD", "ICP", "USD", AssetType.SPOT, {
                "coinbase": "ICP-USD"
            }),
            ("VET/USD", "VET", "USD", AssetType.SPOT, {
                "coinbase": "VET-USD"
            }),
            
            # ======== DEFI TOKENS ========
            ("AAVE/USD", "AAVE", "USD", AssetType.SPOT, {
                "coinbase": "AAVE-USD",
                "gemini": "AAVEUSD"
            }),
            ("MKR/USD", "MKR", "USD", AssetType.SPOT, {
                "coinbase": "MKR-USD",
                "gemini": "MKRUSD"
            }),
            ("SNX/USD", "SNX", "USD", AssetType.SPOT, {
                "coinbase": "SNX-USD"
            }),
            ("CRV/USD", "CRV", "USD", AssetType.SPOT, {
                "coinbase": "CRV-USD"
            }),
            ("COMP/USD", "COMP", "USD", AssetType.SPOT, {
                "coinbase": "COMP-USD",
                "gemini": "COMPUSD"
            }),
            ("SUSHI/USD", "SUSHI", "USD", AssetType.SPOT, {
                "coinbase": "SUSHI-USD"
            }),
            ("YFI/USD", "YFI", "USD", AssetType.SPOT, {
                "coinbase": "YFI-USD"
            }),
            ("1INCH/USD", "1INCH", "USD", AssetType.SPOT, {
                "coinbase": "1INCH-USD"
            }),
            ("BAL/USD", "BAL", "USD", AssetType.SPOT, {
                "coinbase": "BAL-USD"
            }),
            
            # ======== LAYER 2 & SCALING ========
            ("ARB/USD", "ARB", "USD", AssetType.SPOT, {
                "coinbase": "ARB-USD",
                "gemini": "ARBUSD"
            }),
            ("OP/USD", "OP", "USD", AssetType.SPOT, {
                "coinbase": "OP-USD",
                "gemini": "OPUSD"
            }),
            
            # ======== MEME COINS (HIGH VOLUME) ========
            ("DOGE/USD", "DOGE", "USD", AssetType.SPOT, {
                "coinbase": "DOGE-USD",
                "gemini": "DOGEUSD"
            }),
            ("SHIB/USD", "SHIB", "USD", AssetType.SPOT, {
                "coinbase": "SHIB-USD",
                "gemini": "SHIBUSD"
            }),
            ("PEPE/USD", "PEPE", "USD", AssetType.SPOT, {
                "coinbase": "PEPE-USD"
            }),
            
            # ======== WRAPPED ASSETS ========
            ("WBTC/USD", "WBTC", "USD", AssetType.SPOT, {
                "coinbase": "WBTC-USD"
            }),
            
            # ======== CROSS-ASSET PAIRS (BTC DENOMINATED) ========
            ("ETH/BTC", "ETH", "BTC", AssetType.SPOT, {
                "coinbase": "ETH-BTC",
                "gemini": "ETHBTC"
            }),
            ("SOL/BTC", "SOL", "BTC", AssetType.SPOT, {
                "coinbase": "SOL-BTC",
                "gemini": "SOLBTC"
            }),
            ("LINK/BTC", "LINK", "BTC", AssetType.SPOT, {
                "coinbase": "LINK-BTC",
                "gemini": "LINKBTC"
            }),
            ("UNI/BTC", "UNI", "BTC", AssetType.SPOT, {
                "coinbase": "UNI-BTC",
                "gemini": "UNIBTC"
            }),
        ]
        
        # Register all known pairs
        current_time_us = int(time.time() * 1_000_000)
        
        for canonical, base, quote, asset_type, venue_map in major_pairs:
            metadata = SymbolMetadata(
                canonical_symbol=canonical,
                base_asset=base,
                quote_asset=quote,
                asset_type=asset_type,
                venue_symbols=venue_map,
                first_seen_utc_us=current_time_us,
                active_venues=set(venue_map.keys()),
                last_update_utc_us=current_time_us
            )
            
            # Store canonical metadata
            self.canonical_metadata[canonical] = metadata
            
            # Build bidirectional mappings
            for venue, venue_symbol in venue_map.items():
                self.venue_to_canonical[(venue, venue_symbol)] = canonical
                self.canonical_to_venue[canonical][venue] = venue_symbol
        
        logger.info(
            f"✅ Initialized canonical registry with {len(self.canonical_metadata)} symbols "
            f"across {len(set(v for vm in major_pairs for v in vm[4].keys()))} venues (Coinbase + Gemini)"
        )
        logger.info(
            f"   Categories: Top 10 (BTC/ETH/SOL/XRP/ADA/AVAX/DOT/MATIC/LINK/UNI) + "
            f"L1s (ATOM/ALGO/APT/SUI/NEAR/FTM/LTC/BCH/ICP/VET) + "
            f"DeFi (AAVE/MKR/SNX/CRV/COMP/SUSHI/YFI/1INCH/BAL) + "
            f"Layer2 (ARB/OP) + Meme (DOGE/SHIB/PEPE) + BTC pairs (ETH/SOL/LINK/UNI)"
        )
        logger.info(
            f"   Total: {len(self.canonical_metadata)} symbols with full cross-venue mapping"
        )
    
    def get_canonical(self, venue: str, symbol: str) -> Optional[str]:
        """Get canonical symbol for venue-specific symbol."""
        return self.venue_to_canonical.get((venue, symbol))
    
    def get_venue_symbol(self, canonical: str, venue: str) -> Optional[str]:
        """Get venue-specific symbol for canonical symbol."""
        return self.canonical_to_venue.get(canonical, {}).get(venue)
    
    def get_metadata(self, canonical: str) -> Optional[SymbolMetadata]:
        """Get metadata for canonical symbol."""
        return self.canonical_metadata.get(canonical)
    
    def register_new_symbol(
        self, 
        venue: str, 
        symbol: str, 
        canonical: str,
        base: str,
        quote: str,
        asset_type: AssetType = AssetType.SPOT
    ) -> None:
        """Register a newly discovered symbol."""
        current_time_us = int(time.time() * 1_000_000)
        
        # Create or update metadata
        if canonical in self.canonical_metadata:
            metadata = self.canonical_metadata[canonical]
            metadata.venue_symbols[venue] = symbol
            metadata.active_venues.add(venue)
            metadata.last_update_utc_us = current_time_us
            
            # Update venue latency tracking
            metadata.venue_last_seen_utc_us[venue] = current_time_us
            
            # Calculate staleness score for this venue
            # Staleness = time since last update / expected update interval (30s)
            # 0.0 = fresh (< 5s), 1.0 = stale (> 60s)
            time_since_update_sec = (current_time_us - metadata.venue_last_seen_utc_us.get(venue, current_time_us)) / 1_000_000
            if time_since_update_sec < 5:
                staleness = 0.0  # Fresh
            elif time_since_update_sec > 60:
                staleness = 1.0  # Stale
            else:
                # Linear interpolation between 5s (fresh) and 60s (stale)
                staleness = (time_since_update_sec - 5) / 55
            metadata.venue_staleness_score[venue] = round(staleness, 3)
        else:
            metadata = SymbolMetadata(
                canonical_symbol=canonical,
                base_asset=base,
                quote_asset=quote,
                asset_type=asset_type,
                venue_symbols={venue: symbol},
                first_seen_utc_us=current_time_us,
                active_venues={venue},
                last_update_utc_us=current_time_us,
                venue_last_seen_utc_us={venue: current_time_us},
                venue_staleness_score={venue: 0.0}  # New venue, assume fresh
            )
            self.canonical_metadata[canonical] = metadata
        
        # Update mappings
        self.venue_to_canonical[(venue, symbol)] = canonical
        self.canonical_to_venue[canonical][venue] = symbol
        
        logger.info(f"🆕 Registered new symbol: {venue}:{symbol} → {canonical}")
    
    def get_all_canonical_symbols(self) -> List[str]:
        """Get list of all canonical symbols."""
        return list(self.canonical_metadata.keys())
    
    def get_venue_coverage(self) -> Dict[str, int]:
        """Get number of symbols per venue."""
        coverage = defaultdict(int)
        for canonical, metadata in self.canonical_metadata.items():
            for venue in metadata.active_venues:
                coverage[venue] += 1
        return dict(coverage)


# ============================================================================
# SYMBOL NORMALIZER
# ============================================================================

class SymbolNormalizer:
    """
    Institutional Gold Layer: Cross-Venue Symbol Normalization
    
    Architecture:
    - Independent Kafka consumer (consumer group: symbol_normalizer)
    - Subscribes to: clean.* (all topics with symbols)
    - Publishes to: curated.data.symbols
    - Parallel execution with other Gold Layer curators
    
    Responsibilities:
    ✅ DOES: Cross-venue mapping, metadata enrichment, registry integration
    ❌ DOES NOT: API formatting (Bronze concern), feature engineering, trading signals
    
    Quality Guarantees:
    - Bidirectional mapping (venue-specific ↔ canonical)
    - Metadata enrichment from registry
    - Automatic discovery of new symbols
    - Audit trail of all mappings
    """
    
    def __init__(
        self,
        streaming_bus: StreamingBus,
        metrics_collector: Optional[MetricsCollector] = None,
        clickhouse_client = None,
        enable_institutional_controls: bool = True
    ):
        self.bus = streaming_bus
        self.metrics: Optional[MetricsCollector] = metrics_collector or get_metrics_collector()
        
        # Canonical symbol registry
        self.registry = CanonicalSymbolRegistry()
        
        # Metrics tracking
        self.normalizer_metrics = NormalizerMetrics()
        
        # Circuit breaker state
        self.circuit_open = False
        self.consecutive_failures = 0
        self.max_consecutive_failures = 5
        
        # Cache for frequent lookups
        self.normalization_cache: Dict[Tuple[str, str], NormalizationResult] = {}
        self.cache_size_limit = 10000
        
        # Shutdown flag
        self._shutdown = False
        
        # Institutional Controls (optional)
        self.institutional_controls: Optional[InstitutionalControls] = None  # type: ignore
        if enable_institutional_controls and INSTITUTIONAL_CONTROLS_AVAILABLE and InstitutionalControls is not None:
            try:
                # Custom SLA thresholds for symbol normalization
                sla_thresholds = {
                    SLAMetric.LATENCY: 50.0,  # type: ignore  # 50ms P95 latency (very fast for lookups)
                    SLAMetric.COMPLETENESS: 1.0,  # type: ignore  # 100% completeness (every symbol must map)
                    SLAMetric.FRESHNESS: 1.0,  # type: ignore  # 1 second max lag
                    SLAMetric.QUALITY_SCORE: 0.995,  # type: ignore  # 99.5% quality score
                }
                
                self.institutional_controls = InstitutionalControls(
                    component_name="symbol_normalizer",
                    component_version="1.0.0",
                    clickhouse_client=clickhouse_client,
                    sla_thresholds=sla_thresholds
                )
                logger.info("✅ Institutional controls enabled for Symbol Normalizer")
            except Exception as e:
                logger.warning(f"⚠️ Failed to initialize institutional controls: {e}")
                self.institutional_controls = None
        
        logger.info("🏗️ SymbolNormalizer initialized")
    
    async def start(self) -> None:
        """
        Start Symbol Normalizer as independent Kafka consumer.
        Launches background tasks and returns immediately (non-blocking).
        """
        logger.info("🚀 Starting Symbol Normalizer (Gold Layer)")
        
        try:
            # Start message consumer (background task)
            consumer_task = asyncio.create_task(self._consume_messages())
            
            # Start health monitoring (background task)
            health_task = asyncio.create_task(self._health_monitor())
            
            # Start cache cleanup (background task)
            cleanup_task = asyncio.create_task(self._cache_cleanup())
            
            # Store tasks for cleanup (non-blocking - tasks run in background)
            self._background_tasks = [consumer_task, health_task, cleanup_task]
            
            logger.info("✅ Symbol Normalizer background tasks started (consumer, health monitor, cache cleanup)")
            
        except Exception as e:
            logger.error(f"❌ Symbol Normalizer fatal error: {e}", exc_info=True)
            raise
    
    async def _consume_messages(self) -> None:
        """
        Consume clean.* topics and normalize symbols.
        Independent Kafka consumer with auto-commit.
        """
        consumer_group = "symbol_normalizer"
        topics = ["clean.market.trades", "clean.market.orderbook", 
                  "clean.market.funding", "clean.market.options"]
        
        logger.info(f"📥 Starting message consumer (group: {consumer_group})")
        
        # Define message handler
        async def handle_message(topic: str, partition_key: str, 
                                payload: Dict, headers: Dict) -> None:
            if self._shutdown or self.circuit_open:
                return
            
            try:
                # Extract symbol and venue
                symbol = payload.get("symbol")
                venue = payload.get("venue")
                
                if not symbol or not venue:
                    logger.debug(f"⚠️ Message missing symbol or venue: {topic}")
                    return
                
                # Normalize symbol
                result = await self.normalize_symbol(venue, symbol)
                
                if result:
                    # Publish normalized result
                    await self._publish_normalized_symbol(result)
                    
                    # Update metrics
                    self.normalizer_metrics.symbols_processed += 1
                    self.normalizer_metrics.venue_coverage[venue] += 1
                
                # Reset circuit breaker on success
                self.consecutive_failures = 0
                
            except Exception as e:
                logger.error(f"❌ Error processing message: {e}", exc_info=True)
                self.consecutive_failures += 1
                
                if self.consecutive_failures >= self.max_consecutive_failures:
                    self.circuit_open = True
                    logger.error("🔴 Circuit breaker OPEN due to consecutive failures")
                    await self._publish_incident("circuit_breaker_open", str(e))
        
        # Subscribe with worker pool
        await self.bus.subscribe_with_worker_pool(
            consumer_group=consumer_group,
            topics=topics,
            handler=handle_message,
            pool_size=4  # Parallel workers
        )
    
    async def normalize_symbol(
        self, 
        venue: str, 
        symbol: str
    ) -> Optional[NormalizationResult]:
        """
        Normalize venue-specific symbol to canonical format.
        
        Args:
            venue: Exchange/venue name (e.g., "binance", "kraken")
            symbol: Venue-specific symbol (e.g., "BTCUSDT", "XBTUSDT")
        
        Returns:
            NormalizationResult with canonical symbol and metadata
        """
        # Check cache first
        cache_key = (venue, symbol)
        if cache_key in self.normalization_cache:
            return self.normalization_cache[cache_key]
        
        # Lookup in registry
        canonical = self.registry.get_canonical(venue, symbol)
        confidence = 1.0  # Known mapping
        
        # If not found, attempt algorithmic normalization
        if canonical is None:
            canonical, confidence = self._infer_canonical(venue, symbol)
            
            if canonical:
                # Register new symbol
                base, quote = self._parse_canonical(canonical)
                self.registry.register_new_symbol(
                    venue=venue,
                    symbol=symbol,
                    canonical=canonical,
                    base=base,
                    quote=quote
                )
                self.normalizer_metrics.new_symbols_discovered += 1
            else:
                # Mapping failed
                self.normalizer_metrics.mapping_failures += 1
                logger.warning(
                    f"⚠️ Failed to normalize symbol: {venue}:{symbol}"
                )
                return None
        
        # Get metadata
        metadata = self.registry.get_metadata(canonical)
        
        if not metadata:
            logger.error(f"❌ Missing metadata for canonical symbol: {canonical}")
            return None
        
        # Create result
        result = NormalizationResult(
            raw_symbol=symbol,
            venue=venue,
            canonical_symbol=canonical,
            metadata=metadata,
            confidence=confidence,
            normalized_at_utc_us=int(time.time() * 1_000_000)
        )
        
        # Cache result
        if len(self.normalization_cache) < self.cache_size_limit:
            self.normalization_cache[cache_key] = result
        
        return result
    
    def _infer_canonical(self, venue: str, symbol: str) -> Tuple[Optional[str], float]:
        """
        Attempt to infer canonical symbol from venue-specific format.
        
        Returns: (canonical_symbol, confidence_score)
        """
        # Clean symbol
        symbol_clean = symbol.upper().strip()
        
        # Kraken: XBT → BTC conversion
        if venue == "kraken":
            symbol_clean = symbol_clean.replace("XBT", "BTC")
        
        # Try common patterns
        patterns = [
            # BTCUSDT → BTC/USDT
            (r'^([A-Z]{2,10})(USDT|USDC|USD|EUR|GBP|BTC|ETH)$', r'\1/\2'),
            # BTC-USDT → BTC/USDT
            (r'^([A-Z]{2,10})[-_]((USDT|USDC|USD|EUR|GBP|BTC|ETH))$', r'\1/\2'),
            # Already canonical: BTC/USDT
            (r'^([A-Z]{2,10})/([A-Z]{2,10})$', r'\1/\2'),
        ]
        
        for pattern, replacement in patterns:
            match = re.match(pattern, symbol_clean)
            if match:
                canonical = re.sub(pattern, replacement, symbol_clean)
                # Confidence based on pattern complexity
                confidence = 0.8 if "-" in symbol or "_" in symbol else 0.7
                logger.info(
                    f"🔍 Inferred canonical: {venue}:{symbol} → {canonical} "
                    f"(confidence: {confidence:.2f})"
                )
                return canonical, confidence
        
        # Failed to infer
        return None, 0.0
    
    def _parse_canonical(self, canonical: str) -> Tuple[str, str]:
        """Parse canonical symbol into base and quote assets."""
        parts = canonical.split("/")
        if len(parts) == 2:
            return parts[0], parts[1]
        else:
            # Fallback
            return canonical, "UNKNOWN"
    
    async def _publish_normalized_symbol(self, result: NormalizationResult) -> None:
        """Publish normalized symbol to curated.data.symbols with institutional headers."""
        try:
            topic = "curated.data.symbols"
            partition_key = result.canonical_symbol
            payload = result.to_dict()
            
            # Use canonical headers for data lineage and audit compliance
            await self.bus.publish_with_canonical_headers(
                topic=topic,
                partition_key=partition_key,
                payload=payload,
                source_id="symbol_normalizer.001",
                sequence_number=getattr(self, '_sequence_number', 0),
                producer_version="1.0.0"
            )
            
            # Increment sequence number for next message
            if not hasattr(self, '_sequence_number'):
                self._sequence_number = 0
            self._sequence_number += 1
            
            # Metrics (defensive: check for None and method existence)
            if self.metrics is not None:
                try:
                    self.metrics.increment_counter(
                        "symbols_normalized_total",
                        labels={
                            "venue": result.venue,
                            "canonical": result.canonical_symbol
                        }
                    )
                except (AttributeError, TypeError) as e:
                    logger.debug(f"Metrics collector method not available: {e}")
            
            logger.debug(
                f"✅ Published normalized symbol: {result.venue}:{result.raw_symbol} "
                f"→ {result.canonical_symbol} (confidence: {result.confidence:.2f})"
            )
            
        except Exception as e:
            logger.error(f"❌ Failed to publish normalized symbol: {e}", exc_info=True)
            # Metrics (defensive: check for None and method existence)
            if self.metrics is not None:
                try:
                    self.metrics.increment_counter("symbol_publish_failures_total")
                except (AttributeError, TypeError) as e:
                    logger.debug(f"Metrics collector method not available: {e}")
    
    async def _cache_cleanup(self) -> None:
        """Periodic cache cleanup to prevent unbounded growth."""
        while not self._shutdown:
            try:
                await asyncio.sleep(300)  # Every 5 minutes
                
                if len(self.normalization_cache) > self.cache_size_limit * 0.9:
                    # Clear oldest 20% of cache
                    items_to_remove = int(len(self.normalization_cache) * 0.2)
                    keys_to_remove = list(self.normalization_cache.keys())[:items_to_remove]
                    
                    for key in keys_to_remove:
                        del self.normalization_cache[key]
                    
                    logger.info(
                        f"🧹 Cache cleanup: removed {items_to_remove} entries, "
                        f"{len(self.normalization_cache)} remaining"
                    )
            except Exception as e:
                logger.error(f"❌ Cache cleanup error: {e}")
    
    async def _health_monitor(self) -> None:
        """Health monitoring task - logs metrics and checks circuit breaker."""
        logger.info("🏥 Starting health monitor")
        
        while not self._shutdown:
            try:
                await asyncio.sleep(60)  # Every minute
                
                # Log metrics
                stats = self.normalizer_metrics.get_stats()
                registry_coverage = self.registry.get_venue_coverage()
                
                health_report = {
                    "normalizer_metrics": stats,
                    "registry_coverage": registry_coverage,
                    "cache_size": len(self.normalization_cache),
                    "circuit_breaker_open": self.circuit_open,
                    "total_canonical_symbols": len(self.registry.get_all_canonical_symbols())
                }
                
                logger.info(f"📊 Symbol Normalizer Health: {json.dumps(health_report, indent=2)}")
                
                # Auto-reset circuit breaker
                if self.circuit_open:
                    logger.warning("🔄 Attempting circuit breaker reset...")
                    self.circuit_open = False
                    self.consecutive_failures = 0
                
                # Prometheus metrics (defensive: check for None and method existence)
                if self.metrics is not None:
                    try:
                        self.metrics.set_gauge(
                            "symbol_normalizer_cache_size",
                            len(self.normalization_cache)
                        )
                        self.metrics.set_gauge(
                            "symbol_normalizer_circuit_breaker_open",
                            1 if self.circuit_open else 0
                        )
                        self.metrics.set_gauge(
                            "symbol_normalizer_canonical_symbols_total",
                            len(self.registry.get_all_canonical_symbols())
                        )
                    except (AttributeError, TypeError) as e:
                        logger.debug(f"Metrics collector method not available: {e}")
                
            except Exception as e:
                logger.error(f"❌ Health monitor error: {e}", exc_info=True)
    
    async def _publish_incident(self, incident_type: str, details: str) -> None:
        """Publish incident with institutional headers for audit trail."""
        try:
            incident = {
                "type": incident_type,
                "component": "symbol_normalizer",
                "details": details,
                "timestamp_utc_us": int(time.time() * 1_000_000),
                "severity": "error"
            }
            partition_key = "symbol_normalizer"
            
            # Incidents need lineage tracking for regulatory compliance
            if not hasattr(self, '_incident_sequence'):
                self._incident_sequence = 0
            
            await self.bus.publish_with_canonical_headers(
                topic="incidents.symbol_normalizer",
                partition_key=partition_key,
                payload=incident,
                source_id="symbol_normalizer.001",
                sequence_number=self._incident_sequence,
                producer_version="1.0.0"
            )
            
            self._incident_sequence += 1
        except Exception as e:
            logger.error(f"❌ Failed to publish incident: {e}")
    
    async def shutdown(self) -> None:
        """Graceful shutdown."""
        logger.info("🛑 Initiating Symbol Normalizer shutdown...")
        self._shutdown = True
        
        # Persist registry state (future enhancement: PostgresRegistry integration)
        logger.info(
            f"📊 Final stats: {self.normalizer_metrics.symbols_processed} symbols processed, "
            f"{self.normalizer_metrics.new_symbols_discovered} new symbols discovered"
        )
        
        logger.info("✅ Symbol Normalizer shutdown complete")


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

async def main():
    """
    Run Symbol Normalizer as standalone service.
    """
    # Initialize streaming bus
    bus = StreamingBus(config={
        "bootstrap_servers": ["localhost:9092"],
        "client_id": "symbol-normalizer"
    })
    
    # Create normalizer
    normalizer = SymbolNormalizer(
        streaming_bus=bus
    )
    
    try:
        await normalizer.start()
    except KeyboardInterrupt:
        logger.info("⚠️ Received shutdown signal")
    finally:
        await normalizer.shutdown()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    asyncio.run(main())
