"""
Crypto Market Structure Curator - Gold Layer (Data Preparation)

Purpose: Prepare crypto market-wide metrics for consumption by downstream layers.
This is PURE DATA CURATION - no feature engineering, no alpha signals.

Gold Layer Responsibilities:
✅ Historical data tracking (store time series for trend calculation)
✅ Denormalization (calculate alt dominance = 100 - BTC - ETH)
✅ Liquidity ratio calculation (volume/mcap - basic arithmetic)
✅ Time alignment (consistent snapshots)
✅ Data enrichment (add computed fields that are just arithmetic)

NOT Gold Layer (belongs in Feature Layer):
❌ Regime classification (alt season, bull/bear)
❌ Sector rotation detection
❌ Momentum classification
❌ Trading signals
❌ Any predictive or classificatory logic

Input Topics (from Silver Layer):
- clean.crypto.market_metrics (CoinGecko market data - quality validated)

Output Topics:
- curated.data.crypto_market_structure (denormalized, with basic calcs)

Data Flow:
Bronze (raw_data.*) → Silver (clean.*) → Gold (curated.*) → Features (features.*)

Author: Satoshi Data Engineering
Last Updated: November 6, 2025
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from collections import deque

logger = logging.getLogger(__name__)


# =============================
# DATA MODELS
# =============================

@dataclass
class CryptoMarketStructure:
    """
    Denormalized crypto market structure with basic calculations.
    
    This is CURATED DATA - not features. Just denormalized facts and simple arithmetic.
    Feature layer will do regime detection, momentum analysis, and signals.
    """
    timestamp_utc_us: int
    
    # Current state (from CoinGecko)
    total_market_cap_usd: float
    total_volume_24h_usd: float
    btc_dominance_pct: float
    eth_dominance_pct: float
    defi_market_cap_usd: float
    defi_volume_24h_usd: float
    defi_dominance_pct: float
    active_cryptocurrencies: int
    
    # Denormalized fields (simple arithmetic - Gold layer responsibility)
    alt_dominance_pct: float  # 100 - BTC - ETH
    volume_mcap_ratio: float  # Total volume / Total mcap
    defi_volume_mcap_ratio: float  # DeFi volume / DeFi mcap
    
    # Historical values (for Feature layer to calculate trends)
    # These are just stored values, not calculated features
    btc_dominance_24h_ago: Optional[float] = None
    btc_dominance_7d_ago: Optional[float] = None
    market_cap_24h_ago: Optional[float] = None
    market_cap_7d_ago: Optional[float] = None
    
    # Data quality metadata
    data_age_sec: float = 0.0  # How old is this data
    
    def to_dict(self) -> Dict:
        return asdict(self)


# =============================
# CRYPTO MARKET STRUCTURE CURATOR
# =============================

class CryptoMarketStructureCurator:
    """
    Gold layer curator for crypto market-wide metrics.
    
    PURE DATA PREPARATION:
    - Denormalize (add alt dominance)
    - Calculate basic ratios (volume/mcap)
    - Store historical values for trend calculation
    - Time-align and create snapshots
    
    NO FEATURE ENGINEERING:
    - No regime detection
    - No momentum classification
    - No sector rotation signals
    - Just prepared, denormalized data
    """
    
    def __init__(
        self,
        streaming_bus,
        metrics_collector=None,
        enrichment_interval_sec: int = 60,
    ):
        self.bus = streaming_bus
        self.metrics = metrics_collector
        self.enrichment_interval = enrichment_interval_sec
        
        # State management
        self._running = False
        self._tasks: List[asyncio.Task] = []
        
        # Latest metrics
        self._latest_metrics: Optional[Dict] = None
        
        # Historical data (for storing past values - NOT for calculating features)
        # Store (timestamp_us, btc_dom, market_cap) tuples
        self._history: deque = deque(maxlen=10080)  # 7 days at 1-min intervals
        
        # Time windows for historical lookback
        self._window_24h_us = 24 * 3600 * 1_000_000
        self._window_7d_us = 7 * 24 * 3600 * 1_000_000
        
        logger.info("✅ CryptoMarketStructureCurator initialized (data preparation mode)")
    
    async def start(self):
        """Start the curator."""
        logger.info("📊 CryptoMarketStructureCurator starting...")
        self._running = True
        
        self._tasks = [
            asyncio.create_task(self._consume_metrics()),
            asyncio.create_task(self._generate_enriched_data()),
        ]
        
        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            logger.info("Crypto curator tasks cancelled during shutdown")
        except Exception as e:
            logger.error(f"Crypto curator fatal error: {e}")
            raise
    
    async def stop(self):
        """Gracefully stop the curator."""
        logger.info("🛑 Stopping CryptoMarketStructureCurator...")
        self._running = False
        
        for task in self._tasks:
            if not task.done():
                task.cancel()
        
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        
        logger.info("✅ CryptoMarketStructureCurator stopped")
    
    # =============================
    # DATA INGESTION
    # =============================
    
    async def _consume_metrics(self):
        """Consume validated crypto market metrics from Silver layer."""
        while self._running:
            try:
                async for message in self.bus.subscribe('clean.crypto.market_metrics'):
                    if not self._running:
                        break
                    
                    payload = message.get('payload', {})
                    
                    # Store latest
                    self._latest_metrics = payload
                    
                    # Store in history (for providing historical values to Feature layer)
                    timestamp_us = payload.get('timestamp_utc_us', int(time.time() * 1_000_000))
                    btc_dom = payload.get('btc_dominance_pct', 0)
                    market_cap = payload.get('total_market_cap_usd', 0)
                    
                    self._history.append((timestamp_us, btc_dom, market_cap))
                    
                    logger.debug(
                        f"📊 Received market metrics: "
                        f"BTC dominance={btc_dom:.2f}%, "
                        f"MCap=${market_cap/1e9:.1f}B"
                    )
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error consuming market metrics: {e}")
                await asyncio.sleep(5)
    
    # =============================
    # DATA ENRICHMENT (PREPARATION ONLY)
    # =============================
    
    async def _generate_enriched_data(self):
        """Generate enriched market structure (denormalized, with basic calcs)."""
        while self._running:
            try:
                await asyncio.sleep(self.enrichment_interval)
                
                if not self._running:
                    break
                
                if not self._latest_metrics:
                    logger.debug("No market metrics available yet")
                    continue
                
                now_us = int(time.time() * 1_000_000)
                
                # Build enriched structure (pure data preparation)
                structure = self._build_enriched_structure(now_us)
                
                # Publish curated data
                await self.bus.publish(
                    topic='curated.data.crypto_market_structure',
                    partition_key='market_structure',
                    payload=structure.to_dict()
                )
                
                if self.metrics:
                    self.metrics.increment_counter(
                        'crypto_structure_curator_enrichments_total',
                        value=1.0
                    )
                
                logger.info(
                    f"📊 Published crypto structure: "
                    f"BTC dom={structure.btc_dominance_pct:.2f}%, "
                    f"Alt dom={structure.alt_dominance_pct:.2f}%, "
                    f"Vol/MCap={structure.volume_mcap_ratio:.4f}, "
                    f"History size={len(self._history)}"
                )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error generating enriched data: {e}")
                await asyncio.sleep(5)
    
    def _build_enriched_structure(self, timestamp_us: int) -> CryptoMarketStructure:
        """
        Build enriched market structure (PURE DATA CURATION).
        
        What this does:
        - Denormalize (add alt dominance)
        - Calculate basic ratios (simple arithmetic)
        - Look up historical values (for Feature layer)
        - Add metadata
        
        What this does NOT do:
        - Classify regimes (Feature layer)
        - Detect trends or momentum (Feature layer)
        - Generate signals (Feature layer)
        """
        m = self._latest_metrics
        if m is None:
            raise ValueError("No metrics available to build structure")
        
        # Simple arithmetic (Gold layer responsibility)
        alt_dominance = 100.0 - m['btc_dominance_pct'] - m['eth_dominance_pct']
        volume_mcap_ratio = (
            m['total_volume_24h_usd'] / m['total_market_cap_usd']
            if m['total_market_cap_usd'] > 0 else 0
        )
        defi_volume_mcap_ratio = (
            m['defi_volume_24h_usd'] / m['defi_market_cap_usd']
            if m['defi_market_cap_usd'] > 0 else 0
        )
        
        # Calculate data age
        data_timestamp_us = m.get('timestamp_utc_us', timestamp_us)
        data_age_sec = (timestamp_us - data_timestamp_us) / 1_000_000
        
        # Look up historical values (NOT calculating trends - just providing data)
        btc_dom_24h = self._find_historical_value(timestamp_us, self._window_24h_us, field_idx=1)
        btc_dom_7d = self._find_historical_value(timestamp_us, self._window_7d_us, field_idx=1)
        mcap_24h = self._find_historical_value(timestamp_us, self._window_24h_us, field_idx=2)
        mcap_7d = self._find_historical_value(timestamp_us, self._window_7d_us, field_idx=2)
        
        return CryptoMarketStructure(
            timestamp_utc_us=timestamp_us,
            # Current values (from Silver layer)
            total_market_cap_usd=m['total_market_cap_usd'],
            total_volume_24h_usd=m['total_volume_24h_usd'],
            btc_dominance_pct=m['btc_dominance_pct'],
            eth_dominance_pct=m['eth_dominance_pct'],
            defi_market_cap_usd=m['defi_market_cap_usd'],
            defi_volume_24h_usd=m['defi_volume_24h_usd'],
            defi_dominance_pct=m['defi_dominance_pct'],
            active_cryptocurrencies=m['active_cryptocurrencies'],
            # Denormalized fields (simple arithmetic)
            alt_dominance_pct=alt_dominance,
            volume_mcap_ratio=volume_mcap_ratio,
            defi_volume_mcap_ratio=defi_volume_mcap_ratio,
            # Historical values (for Feature layer to calculate trends)
            btc_dominance_24h_ago=btc_dom_24h,
            btc_dominance_7d_ago=btc_dom_7d,
            market_cap_24h_ago=mcap_24h,
            market_cap_7d_ago=mcap_7d,
            # Metadata
            data_age_sec=data_age_sec,
        )
    
    def _find_historical_value(
        self, 
        current_time_us: int, 
        lookback_window_us: int,
        field_idx: int  # 1=btc_dom, 2=market_cap
    ) -> Optional[float]:
        """
        Find historical value at lookback time.
        
        This is data preparation (providing past values), NOT feature engineering.
        Feature layer will calculate trends, changes, momentum from these values.
        """
        if len(self._history) < 2:
            return None
        
        target_time_us = current_time_us - lookback_window_us
        
        # Find closest historical point
        closest = None
        min_diff = float('inf')
        
        for record in self._history:
            timestamp = record[0]
            diff = abs(timestamp - target_time_us)
            if diff < min_diff:
                min_diff = diff
                closest = record
        
        if not closest:
            return None
        
        # Only return if within 20% of window (otherwise too stale)
        tolerance = lookback_window_us * 0.2
        if min_diff > tolerance:
            return None
        
        return closest[field_idx]


# =============================
# MAIN ENTRY POINT (TESTING)
# =============================

async def main():
    """Test crypto curator."""
    
    class MockStreamingBus:
        async def subscribe(self, topic: str):
            while True:
                await asyncio.sleep(10)
                if topic == 'clean.crypto.market_metrics':
                    yield {
                        'payload': {
                            'total_market_cap_usd': 2.5e12,
                            'total_volume_24h_usd': 95e9,
                            'btc_dominance_pct': 52.3,
                            'eth_dominance_pct': 17.8,
                            'defi_market_cap_usd': 85e9,
                            'defi_volume_24h_usd': 12e9,
                            'defi_dominance_pct': 3.4,
                            'timestamp_utc_us': int(time.time() * 1_000_000),
                            'active_cryptocurrencies': 12500,
                            'market_cap_change_24h_pct': 2.1,
                            'volume_change_24h_pct': -5.3,
                        }
                    }
        
        async def publish(self, topic: str, partition_key: str, payload: Dict):
            logger.info(f"📤 Published to {topic}")
            logger.info(
                f"   BTC dom={payload.get('btc_dominance_pct')}%, "
                f"Alt dom={payload.get('alt_dominance_pct')}%, "
                f"Vol/MCap={payload.get('volume_mcap_ratio'):.4f}"
            )
    
    curator = CryptoMarketStructureCurator(
        streaming_bus=MockStreamingBus(),
        enrichment_interval_sec=10
    )
    
    try:
        await asyncio.wait_for(curator.start(), timeout=120)
    except asyncio.TimeoutError:
        logger.info("Test completed")
        await curator.stop()


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())
