"""
Macro/TradFi Curator - Gold Layer (Data Preparation)

Purpose: Prepare macro/traditional finance data for consumption by downstream layers.
This is PURE DATA CURATION - no feature engineering, no alpha signals.

Gold Layer Responsibilities:
✅ Time alignment (join daily FRED data with real-time equity data)
✅ Interpolation (handle missing values for weekends/holidays)
✅ Denormalization (flatten nested structures)
✅ Snapshot creation (unified view at consistent intervals)
✅ Staleness detection (flag old data)
✅ Data enrichment (add metadata, timestamps)

NOT Gold Layer (belongs in Feature Layer):
❌ Yield curve spreads
❌ Risk-on scores
❌ Regime classification
❌ Momentum calculations
❌ Any derived metrics or signals

Input Topics (from Silver Layer):
- clean.tradfi.indices (VIX, SPY, DIA, QQQ - quality validated)
- clean.tradfi.equities (SPY, QQQ, TLT - quality validated)
- clean.macro.economic_indicators (FRED data - quality validated)

Output Topics:
- curated.data.macro_snapshot (time-aligned, denormalized, ready for Features)

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
from collections import defaultdict

logger = logging.getLogger(__name__)


# =============================
# DATA MODELS
# =============================

@dataclass
class MacroSnapshot:
    """
    Unified macro snapshot at a point in time.
    
    This is CURATED DATA - not features. Just time-aligned, denormalized facts.
    Feature layer will calculate spreads, ratios, and signals from this.
    """
    timestamp_utc_us: int
    
    # Equity indices (latest values)
    spy_price: Optional[float] = None
    qqq_price: Optional[float] = None
    dia_price: Optional[float] = None
    vix_price: Optional[float] = None
    tlt_price: Optional[float] = None
    
    # Interest rates (latest FRED values)
    fed_funds_rate: Optional[float] = None  # DFF
    treasury_10y_yield: Optional[float] = None  # DGS10
    treasury_2y_yield: Optional[float] = None  # DGS2
    treasury_5y_yield: Optional[float] = None  # DGS5
    
    # Economic indicators (latest FRED values)
    unemployment_rate: Optional[float] = None  # UNRATE
    cpi_yoy: Optional[float] = None  # CPIAUCSL
    
    # Data quality metadata
    rates_stale: bool = False  # True if FRED data > 3 days old
    equities_stale: bool = False  # True if equity data > 1 hour old
    rates_last_updated_utc_us: Optional[int] = None
    equities_last_updated_utc_us: Optional[int] = None
    
    # Data completeness
    missing_fields: List[str] = None  # List of expected but missing fields
    
    def __post_init__(self):
        if self.missing_fields is None:
            self.missing_fields = []
    
    def to_dict(self) -> Dict:
        return asdict(self)


# =============================
# MACRO/TRADFI CURATOR
# =============================

class MacroTradFiCurator:
    """
    Gold layer curator for macro/traditional finance data.
    
    PURE DATA PREPARATION:
    - Time-align daily FRED data with real-time equity data
    - Interpolate missing values (last valid observation)
    - Create unified snapshots at consistent intervals
    - Flag stale data for quality monitoring
    - Denormalize for easy consumption
    
    NO FEATURE ENGINEERING:
    - No calculated spreads or ratios
    - No regime classification
    - No risk scores
    - Just prepared, clean, time-aligned data
    """
    
    def __init__(
        self,
        streaming_bus,
        metrics_collector=None,
        snapshot_interval_sec: int = 60,
        max_staleness_sec: int = 86400 * 3,  # 3 days for FRED
    ):
        self.bus = streaming_bus
        self.metrics = metrics_collector
        self.snapshot_interval = snapshot_interval_sec
        self.max_staleness = max_staleness_sec
        
        # State management
        self._running = False
        self._tasks: List[asyncio.Task] = []
        
        # Latest values cache (for time alignment)
        self._latest_data = {
            'indices': {},  # symbol -> {price, timestamp_utc_us}
            'equities': {},  # symbol -> {price, timestamp_utc_us}
            'indicators': {},  # series_id -> {value, timestamp_utc_us}
        }
        
        logger.info("✅ MacroTradFiCurator initialized (data preparation mode)")
    
    async def start(self):
        """Start the curator."""
        logger.info("🏛️ MacroTradFiCurator starting...")
        self._running = True
        
        self._tasks = [
            asyncio.create_task(self._consume_indices()),
            asyncio.create_task(self._consume_equities()),
            asyncio.create_task(self._consume_indicators()),
            asyncio.create_task(self._generate_snapshots()),
        ]
        
        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            logger.info("Macro curator tasks cancelled during shutdown")
        except Exception as e:
            logger.error(f"Macro curator fatal error: {e}")
            raise
    
    async def stop(self):
        """Gracefully stop the curator."""
        logger.info("🛑 Stopping MacroTradFiCurator...")
        self._running = False
        
        for task in self._tasks:
            if not task.done():
                task.cancel()
        
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        
        logger.info("✅ MacroTradFiCurator stopped")
    
    # =============================
    # DATA INGESTION
    # =============================
    
    async def _consume_indices(self):
        """Consume validated index data from Silver layer."""
        while self._running:
            try:
                async for message in self.bus.subscribe('clean.tradfi.indices'):
                    if not self._running:
                        break
                    
                    payload = message.get('payload', {})
                    quotes = payload.get('quotes', [])
                    
                    for quote in quotes:
                        symbol = quote.get('symbol')
                        if symbol:
                            self._latest_data['indices'][symbol] = {
                                'price': quote.get('price'),
                                'timestamp_utc_us': quote.get('timestamp_utc_us'),
                            }
                    
                    if quotes:
                        logger.debug(f"📊 Updated {len(quotes)} index quotes")
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error consuming indices: {e}")
                await asyncio.sleep(5)
    
    async def _consume_equities(self):
        """Consume validated equity data from Silver layer."""
        while self._running:
            try:
                async for message in self.bus.subscribe('clean.tradfi.equities'):
                    if not self._running:
                        break
                    
                    payload = message.get('payload', {})
                    quotes = payload.get('quotes', [])
                    
                    for quote in quotes:
                        symbol = quote.get('symbol')
                        if symbol:
                            self._latest_data['equities'][symbol] = {
                                'price': quote.get('price'),
                                'timestamp_utc_us': quote.get('timestamp_utc_us'),
                            }
                    
                    if quotes:
                        logger.debug(f"📈 Updated {len(quotes)} equity quotes")
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error consuming equities: {e}")
                await asyncio.sleep(5)
    
    async def _consume_indicators(self):
        """Consume validated FRED economic indicators from Silver layer."""
        while self._running:
            try:
                async for message in self.bus.subscribe('clean.macro.economic_indicators'):
                    if not self._running:
                        break
                    
                    payload = message.get('payload', {})
                    indicators = payload.get('indicators', [])
                    
                    for indicator in indicators:
                        series_id = indicator.get('series_id')
                        if series_id:
                            self._latest_data['indicators'][series_id] = {
                                'value': indicator.get('value'),
                                'timestamp_utc_us': indicator.get('timestamp_utc_us'),
                            }
                    
                    if indicators:
                        logger.debug(f"📉 Updated {len(indicators)} FRED indicators")
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error consuming indicators: {e}")
                await asyncio.sleep(5)
    
    # =============================
    # SNAPSHOT GENERATION (DATA PREPARATION)
    # =============================
    
    async def _generate_snapshots(self):
        """Generate unified macro snapshots (time-aligned, denormalized)."""
        while self._running:
            try:
                await asyncio.sleep(self.snapshot_interval)
                
                if not self._running:
                    break
                
                now_us = int(time.time() * 1_000_000)
                
                # Build snapshot (pure data preparation)
                snapshot = self._build_snapshot(now_us)
                
                # Publish curated data
                await self.bus.publish(
                    topic='curated.data.macro_snapshot',
                    partition_key='macro',
                    payload=snapshot.to_dict()
                )
                
                if self.metrics:
                    self.metrics.increment_counter(
                        'macro_curator_snapshots_generated_total',
                        value=1.0
                    )
                
                logger.info(
                    f"🏛️ Published macro snapshot: "
                    f"VIX={snapshot.vix_price:.2f if snapshot.vix_price else 'N/A'}, "
                    f"10Y={snapshot.treasury_10y_yield:.2f}% if snapshot.treasury_10y_yield else 'N/A', "
                    f"Stale(rates={snapshot.rates_stale}, equities={snapshot.equities_stale}), "
                    f"Missing={len(snapshot.missing_fields)} fields"
                )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error generating snapshot: {e}")
                await asyncio.sleep(5)
    
    def _build_snapshot(self, timestamp_us: int) -> MacroSnapshot:
        """
        Build unified macro snapshot (PURE DATA CURATION).
        
        What this does:
        - Time-align data from different sources
        - Use last valid observation for missing data (interpolation)
        - Flag stale data
        - Identify missing fields
        
        What this does NOT do:
        - Calculate spreads or ratios (Feature layer)
        - Classify regimes (Feature layer)
        - Generate scores (Feature layer)
        """
        snapshot = MacroSnapshot(timestamp_utc_us=timestamp_us)
        missing = []
        
        # Populate equities (with interpolation = last valid observation)
        snapshot.spy_price = self._get_latest_value('equities', 'SPY', 'price')
        snapshot.qqq_price = self._get_latest_value('equities', 'QQQ', 'price')
        snapshot.tlt_price = self._get_latest_value('equities', 'TLT', 'price')
        
        # Populate indices
        snapshot.dia_price = self._get_latest_value('indices', 'DIA', 'price')
        snapshot.vix_price = self._get_latest_value('indices', 'VIX', 'price')
        
        # Populate FRED indicators
        snapshot.fed_funds_rate = self._get_latest_value('indicators', 'DFF', 'value')
        snapshot.treasury_10y_yield = self._get_latest_value('indicators', 'DGS10', 'value')
        snapshot.treasury_2y_yield = self._get_latest_value('indicators', 'DGS2', 'value')
        snapshot.treasury_5y_yield = self._get_latest_value('indicators', 'DGS5', 'value')
        snapshot.unemployment_rate = self._get_latest_value('indicators', 'UNRATE', 'value')
        snapshot.cpi_yoy = self._get_latest_value('indicators', 'CPIAUCSL', 'value')
        
        # Track missing fields
        expected_fields = {
            'spy_price': snapshot.spy_price,
            'qqq_price': snapshot.qqq_price,
            'vix_price': snapshot.vix_price,
            'treasury_10y_yield': snapshot.treasury_10y_yield,
            'treasury_2y_yield': snapshot.treasury_2y_yield,
            'fed_funds_rate': snapshot.fed_funds_rate,
        }
        missing = [k for k, v in expected_fields.items() if v is None]
        snapshot.missing_fields = missing
        
        # Check staleness
        now_sec = timestamp_us / 1_000_000
        
        # FRED data staleness (daily data, >3 days is stale)
        if self._latest_data['indicators']:
            latest_indicator_ts = max(
                ind['timestamp_utc_us'] 
                for ind in self._latest_data['indicators'].values()
                if ind.get('timestamp_utc_us')
            )
            snapshot.rates_last_updated_utc_us = latest_indicator_ts
            if now_sec - (latest_indicator_ts / 1_000_000) > self.max_staleness:
                snapshot.rates_stale = True
        
        # Equity data staleness (real-time data, >1 hour is stale)
        if self._latest_data['equities']:
            latest_equity_ts = max(
                eq['timestamp_utc_us'] 
                for eq in self._latest_data['equities'].values()
                if eq.get('timestamp_utc_us')
            )
            snapshot.equities_last_updated_utc_us = latest_equity_ts
            if now_sec - (latest_equity_ts / 1_000_000) > 3600:
                snapshot.equities_stale = True
        
        return snapshot
    
    def _get_latest_value(
        self, 
        data_type: str,  # 'indices', 'equities', 'indicators'
        key: str,  # symbol or series_id
        field: str  # 'price' or 'value'
    ) -> Optional[float]:
        """
        Get latest value with last observation carried forward (LOCF interpolation).
        This is data preparation, not feature engineering.
        """
        data = self._latest_data.get(data_type, {}).get(key, {})
        return data.get(field)


# =============================
# MAIN ENTRY POINT (TESTING)
# =============================

async def main():
    """Test macro curator."""
    
    class MockStreamingBus:
        async def subscribe(self, topic: str):
            while True:
                await asyncio.sleep(10)
                if topic == 'clean.tradfi.indices':
                    yield {
                        'payload': {
                            'quotes': [{
                                'symbol': 'VIX',
                                'price': 16.5,
                                'timestamp_utc_us': int(time.time() * 1_000_000),
                            }]
                        }
                    }
        
        async def publish(self, topic: str, partition_key: str, payload: Dict):
            logger.info(f"📤 Published to {topic}")
            logger.info(f"   Snapshot: VIX={payload.get('vix_price')}, Missing={len(payload.get('missing_fields', []))}")
    
    curator = MacroTradFiCurator(
        streaming_bus=MockStreamingBus(),
        snapshot_interval_sec=10
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
