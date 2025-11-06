"""
Momentum Engine - Multi-Horizon Momentum Feature Agent

MODULARITY BOUNDARY: DESCRIPTIVE (Feature Layer)
- Measures price momentum across multiple timeframes
- Detects trend strength, acceleration, and divergence
- Does NOT make trading decisions (PRESCRIPTIVE)
- Momentum is DESCRIPTIVE measurement of market state

Subscribes:  curated.data.ohlcv_1m, curated.data.ohlcv_5m, curated.data.ohlcv_1h
Publishes:   features.momentum
Consumer Group: momentum_engine

Architecture:
- Input: OHLCV bars from Gold Layer (multiple timeframes)
- Output: 15 momentum features per symbol
- Processing: Pure mathematical transformations (no external calls)
- Target: <20ms computation time per update

Feature Categories:
1. Multi-Horizon Momentum (6 features): Price change % at 5m, 15m, 1h, 4h, 1d, 1w
2. Trend Detection (3 features): Direction, strength, confidence
3. Momentum Quality (3 features): Strength score, acceleration, volume confirmation
4. Divergence Detection (2 features): Price-volume divergence, momentum divergence
5. Metadata (1 feature): Confidence score

Institutional Standards:
- Centralized Prometheus metrics
- Graceful degradation (metrics optional)
- Circuit breaker for data quality issues
- Multi-timeframe cache management
- Proper data lineage tracking
"""

import asyncio
import logging
import math
import numpy as np
import time
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
from collections import deque, defaultdict

logger = logging.getLogger(__name__)

# Import centralized Prometheus metrics
try:
    from infra.monitoring.prometheus_metrics import MetricsCollector
    
    # Initialize global metrics collector instance
    _metrics_collector = MetricsCollector()
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    _metrics_collector = None
    logger.warning("MetricsCollector not available - metrics will not be collected")


def _safe_probability(value: float) -> float:
    """Ensure value is finite and in [0, 1] range."""
    if math.isnan(value) or math.isinf(value):
        return 0.0
    return max(0.0, min(1.0, float(value)))


def _safe_positive(value: float, default: float = 0.0) -> float:
    """Ensure value is finite and non-negative."""
    if math.isnan(value) or math.isinf(value):
        return default
    return max(0.0, float(value))


def _safe_finite(value: float, default: float = 0.0) -> float:
    """Ensure value is finite (can be negative)."""
    if math.isnan(value) or math.isinf(value):
        return default
    return float(value)


@dataclass
class MomentumFeatures:
    """
    Multi-horizon momentum features from OHLCV data.
    
    All momentum values are percentage returns (%).
    Strength/confidence metrics are normalized to [0, 1].
    """
    symbol: str
    timestamp: float  # Unix timestamp
    venue: str
    
    # Multi-Horizon Momentum (6 features)
    momentum_5m: float      # 5-minute price change %
    momentum_15m: float     # 15-minute price change %
    momentum_1h: float      # 1-hour price change %
    momentum_4h: float      # 4-hour price change %
    momentum_1d: float      # 1-day price change %
    momentum_1w: float      # 1-week price change %
    
    # Trend Detection (3 features)
    trend_direction: str    # "up", "down", "sideways"
    trend_strength: float   # 0-1 (higher = stronger trend)
    trend_confidence: float # 0-1 (consistency across timeframes)
    
    # Momentum Quality (3 features)
    momentum_strength: float        # 0-1 (alignment across horizons)
    momentum_acceleration: float    # Rate of momentum change (% per hour)
    volume_confirmation: float      # 0-1 (volume supports momentum)
    
    # Divergence Detection (2 features)
    price_volume_divergence: float  # -1 to 1 (negative = bearish divergence)
    momentum_divergence: float      # -1 to 1 (short vs long momentum)
    
    # Metadata (1 feature)
    confidence: float  # 0-1 overall confidence score


class MomentumEngine:
    """
    Computes multi-horizon momentum features from OHLCV data.
    
    Institutional Features:
    - Multi-timeframe analysis (5m to 1w)
    - Trend detection with confidence scoring
    - Momentum quality metrics (strength, acceleration)
    - Volume confirmation
    - Divergence detection
    - Circuit breaker protection
    - Prometheus metrics integration
    
    Performance Targets:
    - Computation time: <20ms per update
    - Memory: ~10MB per symbol (168 hours of data)
    - Latency: <50ms end-to-end
    """
    
    def __init__(self, streaming_bus):
        self.bus = streaming_bus
        self.logger = logging.getLogger(__name__)
        
        # Multi-timeframe price caches
        # Structure: {symbol: {timeframe: deque([bars])}}
        self.price_cache = defaultdict(lambda: defaultdict(deque))
        
        # Cache sizes (number of bars to keep)
        self.cache_sizes = {
            '1m': 10080,   # 1 week of minutes
            '5m': 2016,    # 1 week of 5-minute bars
            '1h': 168,     # 1 week of hours
        }
        
        # Minimum bars required for computations
        self.min_bars = {
            '5m': 1,       # 5 minutes
            '15m': 3,      # 15 minutes  
            '1h': 1,       # 1 hour
            '4h': 4,       # 4 hours
            '1d': 24,      # 1 day
            '1w': 168,     # 1 week
        }
        
        # Circuit breaker state
        self.circuit_open = False
        self.circuit_failures = 0
        self.circuit_failure_threshold = 10
        self.circuit_recovery_delay_sec = 60
        self.last_circuit_open_time = 0
        
        # Performance tracking
        self.computation_times = deque(maxlen=100)
        self.features_computed = 0
        
    async def start(self):
        """
        Start consuming OHLCV data and producing momentum features.
        
        Subscribes to multiple timeframe topics:
        - curated.data.ohlcv_1m (for fine-grained momentum)
        - curated.data.ohlcv_5m (for 5m, 15m momentum)
        - curated.data.ohlcv_1h (for 1h, 4h, 1d, 1w momentum)
        """
        self.logger.info("🚀 MomentumEngine starting...")
        
        try:
            # Subscribe to multiple OHLCV topics concurrently
            tasks = [
                self._consume_ohlcv('curated.data.ohlcv_1m', '1m'),
                self._consume_ohlcv('curated.data.ohlcv_5m', '5m'),
                self._consume_ohlcv('curated.data.ohlcv_1h', '1h'),
            ]
            
            await asyncio.gather(*tasks)
            
        except Exception as e:
            self.logger.error(f"❌ MomentumEngine fatal error: {e}")
            raise
    
    async def _consume_ohlcv(self, topic: str, timeframe: str):
        """
        Consume OHLCV data from a specific timeframe topic.
        
        Args:
            topic: Kafka topic name
            timeframe: Timeframe identifier ('1m', '5m', '1h')
        """
        self.logger.info(f"📊 Subscribing to {topic}")
        
        async for message in self.bus.consume(
            topic=topic,
            consumer_group="momentum_engine",
            value_deserializer=lambda m: m  # Assuming already deserialized
        ):
            try:
                await self._process_ohlcv_bar(message, timeframe)
            except Exception as e:
                self.logger.error(f"Error processing {timeframe} bar: {e}")
                self._increment_circuit_failures()
    
    async def _process_ohlcv_bar(self, bar: Dict, timeframe: str):
        """
        Process a single OHLCV bar and update momentum features.
        
        Args:
            bar: OHLCV bar dictionary
            timeframe: Timeframe of the bar ('1m', '5m', '1h')
        """
        # Check circuit breaker
        if self._is_circuit_open():
            return
        
        start_time = time.time()
        
        try:
            symbol = bar['symbol']
            venue = bar.get('venue', 'unknown')
            
            # Update price cache
            self._update_cache(symbol, bar, timeframe)
            
            # Only compute features on 1h updates (primary trigger)
            # This prevents duplicate computation while maintaining multi-TF data
            if timeframe != '1h':
                return
            
            # Compute momentum features
            features = await self._compute_momentum_features(symbol, venue, bar['timestamp'])
            
            if features:
                # Publish to Kafka
                await self.bus.publish(
                    topic="features.momentum",
                    key=symbol,
                    value=asdict(features)
                )
                
                self.features_computed += 1
                
                # Record metrics
                if METRICS_AVAILABLE:
                    _metrics_collector.increment_counter(
                        "momentum_features_computed",
                        value=1.0,
                        labels={"symbol": symbol, "venue": venue}
                    )
                
                # Log periodic progress
                if self.features_computed % 100 == 0:
                    avg_time = np.mean(self.computation_times) if self.computation_times else 0
                    self.logger.info(
                        f"📈 Momentum features computed: {self.features_computed} "
                        f"(avg: {avg_time*1000:.2f}ms)"
                    )
        
        except Exception as e:
            self.logger.error(f"Error computing momentum for {bar.get('symbol')}: {e}")
            self._increment_circuit_failures()
        
        finally:
            # Track computation time
            elapsed = time.time() - start_time
            self.computation_times.append(elapsed)
            
            if elapsed > 0.020:  # Warn if >20ms
                self.logger.warning(f"Slow momentum computation: {elapsed*1000:.2f}ms")
    
    def _update_cache(self, symbol: str, bar: Dict, timeframe: str):
        """
        Update price cache with new OHLCV bar.
        
        Args:
            symbol: Trading symbol
            bar: OHLCV bar dictionary
            timeframe: Timeframe identifier
        """
        cache = self.price_cache[symbol][timeframe]
        cache.append(bar)
        
        # Trim cache to max size
        max_size = self.cache_sizes.get(timeframe, 1000)
        while len(cache) > max_size:
            cache.popleft()
    
    async def _compute_momentum_features(self, symbol: str, venue: str, 
                                        timestamp: float) -> Optional[MomentumFeatures]:
        """
        Compute all momentum features for a symbol.
        
        Args:
            symbol: Trading symbol
            venue: Exchange venue
            timestamp: Current timestamp
            
        Returns:
            MomentumFeatures object or None if insufficient data
        """
        try:
            # Get cached bars for different timeframes
            bars_1m = list(self.price_cache[symbol]['1m'])
            bars_5m = list(self.price_cache[symbol]['5m'])
            bars_1h = list(self.price_cache[symbol]['1h'])
            
            # Check minimum data requirements
            if len(bars_1h) < self.min_bars['1w']:
                self.logger.debug(f"Insufficient data for {symbol}: need {self.min_bars['1w']} 1h bars, have {len(bars_1h)}")
                return None
            
            # Extract close prices for 1h timeframe
            closes_1h = np.array([bar['close'] for bar in bars_1h])
            volumes_1h = np.array([bar['volume'] for bar in bars_1h])
            
            # 1. Multi-Horizon Momentum
            momentum_5m = self._compute_horizon_momentum(bars_5m, periods=1)  # 1 bar = 5min
            momentum_15m = self._compute_horizon_momentum(bars_5m, periods=3)  # 3 bars = 15min
            momentum_1h = self._compute_horizon_momentum(bars_1h, periods=1)  # 1 bar = 1h
            momentum_4h = self._compute_horizon_momentum(bars_1h, periods=4)  # 4 bars = 4h
            momentum_1d = self._compute_horizon_momentum(bars_1h, periods=24)  # 24 bars = 1d
            momentum_1w = self._compute_horizon_momentum(bars_1h, periods=168)  # 168 bars = 1w
            
            # 2. Trend Detection
            trend_direction, trend_strength, trend_confidence = self._detect_trend(
                closes_1h, 
                [momentum_1h, momentum_4h, momentum_1d, momentum_1w]
            )
            
            # 3. Momentum Quality
            momentum_strength = self._compute_momentum_strength(
                [momentum_5m, momentum_15m, momentum_1h, momentum_4h, momentum_1d, momentum_1w]
            )
            momentum_acceleration = self._compute_momentum_acceleration(closes_1h)
            volume_confirmation = self._compute_volume_confirmation(closes_1h, volumes_1h)
            
            # 4. Divergence Detection
            price_volume_divergence = self._compute_price_volume_divergence(closes_1h, volumes_1h)
            momentum_divergence = self._compute_momentum_divergence(momentum_1h, momentum_1d)
            
            # 5. Overall Confidence
            confidence = self._compute_confidence_score(
                len(bars_1h),
                trend_confidence,
                momentum_strength,
                volume_confirmation
            )
            
            return MomentumFeatures(
                symbol=symbol,
                timestamp=timestamp,
                venue=venue,
                momentum_5m=momentum_5m,
                momentum_15m=momentum_15m,
                momentum_1h=momentum_1h,
                momentum_4h=momentum_4h,
                momentum_1d=momentum_1d,
                momentum_1w=momentum_1w,
                trend_direction=trend_direction,
                trend_strength=trend_strength,
                trend_confidence=trend_confidence,
                momentum_strength=momentum_strength,
                momentum_acceleration=momentum_acceleration,
                volume_confirmation=volume_confirmation,
                price_volume_divergence=price_volume_divergence,
                momentum_divergence=momentum_divergence,
                confidence=confidence
            )
            
        except Exception as e:
            self.logger.error(f"Error in _compute_momentum_features for {symbol}: {e}")
            return None
    
    def _compute_horizon_momentum(self, bars: List[Dict], periods: int) -> float:
        """
        Compute momentum (% return) over specified number of periods.
        
        Args:
            bars: List of OHLCV bars
            periods: Number of periods to look back
            
        Returns:
            Percentage return over the period
        """
        if len(bars) < periods + 1:
            return 0.0
        
        try:
            current_close = bars[-1]['close']
            past_close = bars[-periods-1]['close']
            
            if past_close == 0:
                return 0.0
            
            momentum_pct = ((current_close - past_close) / past_close) * 100
            return _safe_finite(momentum_pct)
            
        except (KeyError, IndexError, TypeError):
            return 0.0
    
    def _detect_trend(self, closes: np.ndarray, momentum_horizons: List[float]) -> Tuple[str, float, float]:
        """
        Detect trend direction, strength, and confidence.
        
        Args:
            closes: Array of close prices
            momentum_horizons: List of momentum values at different horizons
            
        Returns:
            (direction, strength, confidence) tuple
            - direction: "up", "down", "sideways"
            - strength: 0-1 (magnitude of trend)
            - confidence: 0-1 (consistency across timeframes)
        """
        try:
            # Use linear regression slope for trend direction
            x = np.arange(len(closes))
            slope, _ = np.polyfit(x, closes, 1)
            
            # Normalize slope to percentage
            avg_price = np.mean(closes)
            slope_pct = (slope / avg_price) * 100 if avg_price > 0 else 0
            
            # Determine direction
            if slope_pct > 0.1:  # >0.1% per bar
                direction = "up"
            elif slope_pct < -0.1:  # <-0.1% per bar
                direction = "down"
            else:
                direction = "sideways"
            
            # Trend strength (absolute slope magnitude)
            strength = min(abs(slope_pct) / 5.0, 1.0)  # Normalize to [0, 1], 5% = max
            strength = _safe_probability(strength)
            
            # Trend confidence (consistency across horizons)
            if len(momentum_horizons) > 0:
                # Check if all horizons agree on direction
                positive_count = sum(1 for m in momentum_horizons if m > 0)
                negative_count = sum(1 for m in momentum_horizons if m < 0)
                total_count = len(momentum_horizons)
                
                confidence = max(positive_count, negative_count) / total_count
                confidence = _safe_probability(confidence)
            else:
                confidence = 0.0
            
            return direction, strength, confidence
            
        except Exception as e:
            logger.error(f"Error in trend detection: {e}")
            return "sideways", 0.0, 0.0
    
    def _compute_momentum_strength(self, momentum_horizons: List[float]) -> float:
        """
        Compute momentum strength (alignment across horizons).
        
        Args:
            momentum_horizons: List of momentum values at different timeframes
            
        Returns:
            Strength score 0-1 (1 = perfect alignment)
        """
        if len(momentum_horizons) < 2:
            return 0.0
        
        try:
            # Check directional consistency
            positive_count = sum(1 for m in momentum_horizons if m > 0)
            negative_count = sum(1 for m in momentum_horizons if m < 0)
            total_count = len(momentum_horizons)
            
            # Strength is the degree of agreement
            alignment = max(positive_count, negative_count) / total_count
            
            # Weight by magnitude
            avg_magnitude = np.mean([abs(m) for m in momentum_horizons])
            magnitude_score = min(avg_magnitude / 10.0, 1.0)  # Normalize to [0, 1], 10% = max
            
            # Combined strength
            strength = alignment * magnitude_score
            return _safe_probability(float(strength))
            
        except Exception as e:
            logger.error(f"Error computing momentum strength: {e}")
            return 0.0
    
    def _compute_momentum_acceleration(self, closes: np.ndarray) -> float:
        """
        Compute rate of change in momentum (acceleration).
        
        Args:
            closes: Array of close prices
            
        Returns:
            Acceleration (% change per hour)
        """
        if len(closes) < 3:
            return 0.0
        
        try:
            # Compute recent momentum (last 4 hours)
            recent_momentum = ((closes[-1] - closes[-5]) / closes[-5]) * 100 if len(closes) >= 5 else 0
            
            # Compute previous momentum (4 hours before that)
            prev_momentum = ((closes[-5] - closes[-9]) / closes[-9]) * 100 if len(closes) >= 9 else recent_momentum
            
            # Acceleration is change in momentum
            acceleration = recent_momentum - prev_momentum
            return _safe_finite(acceleration)
            
        except Exception as e:
            logger.error(f"Error computing acceleration: {e}")
            return 0.0
    
    def _compute_volume_confirmation(self, closes: np.ndarray, volumes: np.ndarray) -> float:
        """
        Compute volume confirmation score (does volume support price movement?).
        
        Args:
            closes: Array of close prices
            volumes: Array of volumes
            
        Returns:
            Confirmation score 0-1 (1 = strong volume support)
        """
        if len(closes) < 20 or len(volumes) < 20:
            return 0.5  # Neutral if insufficient data
        
        try:
            # Compute price direction (recent 10 bars)
            price_change = closes[-1] - closes[-10]
            
            # Compute volume trend (recent vs previous 10 bars)
            recent_vol = np.mean(volumes[-10:])
            prev_vol = np.mean(volumes[-20:-10])
            
            if prev_vol == 0:
                return 0.5
            
            vol_ratio = recent_vol / prev_vol
            
            # High volume on price move = confirmation
            # Low volume on price move = divergence
            if abs(price_change) > closes[-10] * 0.01:  # Significant price move (>1%)
                if vol_ratio > 1.2:  # Volume increased by 20%
                    confirmation = 0.8
                elif vol_ratio > 1.0:
                    confirmation = 0.6
                else:
                    confirmation = 0.4  # Weak volume on big move
            else:
                confirmation = 0.5  # Neutral for small price moves
            
            return _safe_probability(confirmation)
            
        except Exception as e:
            logger.error(f"Error computing volume confirmation: {e}")
            return 0.5
    
    def _compute_price_volume_divergence(self, closes: np.ndarray, volumes: np.ndarray) -> float:
        """
        Detect price-volume divergence (bearish if price up but volume down).
        
        Args:
            closes: Array of close prices
            volumes: Array of volumes
            
        Returns:
            Divergence score -1 to 1
            - Negative = bearish divergence (price up, volume down)
            - Positive = bullish divergence (price down, volume up)
            - Zero = no divergence
        """
        if len(closes) < 20 or len(volumes) < 20:
            return 0.0
        
        try:
            # Price trend (slope)
            x = np.arange(len(closes[-20:]))
            price_slope, _ = np.polyfit(x, closes[-20:], 1)
            
            # Volume trend (slope)
            vol_slope, _ = np.polyfit(x, volumes[-20:], 1)
            
            # Normalize slopes
            price_direction = 1 if price_slope > 0 else -1
            vol_direction = 1 if vol_slope > 0 else -1
            
            # Divergence occurs when directions differ
            if price_direction != vol_direction:
                # Bearish divergence: price up, volume down
                if price_direction > 0 and vol_direction < 0:
                    divergence = -0.7
                # Bullish divergence: price down, volume up
                elif price_direction < 0 and vol_direction > 0:
                    divergence = 0.7
                else:
                    divergence = 0.0
            else:
                divergence = 0.0  # No divergence
            
            return _safe_finite(divergence, default=0.0)
            
        except Exception as e:
            logger.error(f"Error computing price-volume divergence: {e}")
            return 0.0
    
    def _compute_momentum_divergence(self, short_momentum: float, long_momentum: float) -> float:
        """
        Detect momentum divergence between short and long horizons.
        
        Args:
            short_momentum: Short-term momentum (e.g., 1h)
            long_momentum: Long-term momentum (e.g., 1d)
            
        Returns:
            Divergence score -1 to 1
            - Negative = bearish (short < long, momentum weakening)
            - Positive = bullish (short > long, momentum strengthening)
        """
        try:
            # Compare short vs long momentum
            if abs(long_momentum) < 0.1:  # Avoid division by small numbers
                return 0.0
            
            divergence = (short_momentum - long_momentum) / abs(long_momentum)
            divergence = max(-1.0, min(1.0, divergence))  # Clip to [-1, 1]
            
            return _safe_finite(divergence, default=0.0)
            
        except Exception as e:
            logger.error(f"Error computing momentum divergence: {e}")
            return 0.0
    
    def _compute_confidence_score(self, num_bars: int, trend_confidence: float,
                                  momentum_strength: float, volume_confirmation: float) -> float:
        """
        Compute overall confidence score for the momentum features.
        
        Args:
            num_bars: Number of bars available
            trend_confidence: Trend confidence score
            momentum_strength: Momentum strength score
            volume_confirmation: Volume confirmation score
            
        Returns:
            Overall confidence 0-1
        """
        # Data sufficiency score
        data_score = min(num_bars / self.min_bars['1w'], 1.0)
        
        # Average of quality metrics
        quality_score = np.mean([trend_confidence, momentum_strength, volume_confirmation])
        
        # Combined confidence
        confidence = data_score * quality_score
        return _safe_probability(float(confidence))
    
    # Circuit Breaker Methods
    
    def _is_circuit_open(self) -> bool:
        """Check if circuit breaker is open."""
        if not self.circuit_open:
            return False
        
        # Check if recovery time has passed
        if time.time() - self.last_circuit_open_time > self.circuit_recovery_delay_sec:
            self.logger.info("🔄 Circuit breaker recovering - resetting failures")
            self.circuit_open = False
            self.circuit_failures = 0
            return False
        
        return True
    
    def _increment_circuit_failures(self):
        """Increment failure count and open circuit if threshold reached."""
        self.circuit_failures += 1
        
        if self.circuit_failures >= self.circuit_failure_threshold:
            self.circuit_open = True
            self.last_circuit_open_time = time.time()
            self.logger.error(
                f"🚨 Circuit breaker OPEN after {self.circuit_failures} failures "
                f"(recovery in {self.circuit_recovery_delay_sec}s)"
            )
