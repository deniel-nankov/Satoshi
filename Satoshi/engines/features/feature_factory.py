"""
Feature Factory - Foundation Features

Computes core mathematical features from OHLCV data:
- Returns (multiple horizons)
- Volatility (multiple horizons)
- Microstructure metrics

This is the foundation layer - all other features can build on these.
Target: <5ms computation time per feature vector.
"""

import asyncio
import logging
import math
import numpy as np
import time
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from datetime import datetime
from collections import deque

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


@dataclass
class BaseFeatures:
    """Foundation features computed from OHLCV data"""
    symbol: str
    timestamp: float  # Unix timestamp
    
    # Returns (multiple horizons)
    returns_1m: float
    returns_5m: float
    returns_15m: float
    returns_1h: float
    
    # Volatility (multiple horizons)
    volatility_5m: float
    volatility_15m: float
    volatility_1h: float
    volatility_1d: float
    
    # Risk-Adjusted Returns
    sharpe_ratio_5m: float   # Return / volatility
    sharpe_ratio_1h: float
    sortino_ratio_5m: float  # Return / downside volatility
    sortino_ratio_1h: float
    skewness: float          # Distribution asymmetry
    kurtosis: float          # Tail thickness
    
    # Momentum Signals
    volume_weighted_momentum: float  # Momentum weighted by volume participation
    volume_acceleration: float       # Rate of change in volume
    vwap_deviation: float            # Distance from VWAP (%)
    
    # Microstructure
    bid_ask_spread: float
    orderbook_imbalance: float
    volume_profile: float
    
    # Quality metadata
    confidence: float  # 0-1 confidence score
    data_age_ms: int   # How old is the input data


class FeatureFactory:
    """
    Computes foundation features from curated OHLCV data.
    
    These features are the building blocks for all other features.
    Keep this FAST - target <5ms computation time.
    
    Architecture:
    - Input: curated.data.ohlcv_* topics (1m, 5m, 15m, 1h)
    - Output: features.base topic
    - Processing: Pure mathematical transformations (no external calls)
    """
    
    def __init__(self, streaming_bus):
        self.bus = streaming_bus
        self.logger = logging.getLogger(__name__)
        
        # Cache recent OHLCV for multi-horizon calculations
        # Structure: {symbol: {timeframe: deque([bars])}}
        self.ohlcv_cache = {}
        self.max_cache_bars = {
            '1m': 100,   # Keep 100 minutes
            '5m': 100,   # Keep ~8 hours
            '15m': 100,  # Keep ~25 hours
            '1h': 168,   # Keep 1 week
        }
        
        # Runtime control
        self._running = False
        self._tasks = []
        
        self.logger.info("🏭 FeatureFactory initialized")
        
    async def start(self):
        """Start consuming OHLCV data and producing base features"""
        self.logger.info("🏭 FeatureFactory starting...")
        self._running = True
        
        # Subscribe to all OHLCV timeframes
        topics = [
            "curated.data.ohlcv_1m",
            "curated.data.ohlcv_5m", 
            "curated.data.ohlcv_15m",
            "curated.data.ohlcv_1h",
        ]
        
        # Start consuming each topic in parallel
        self._tasks = [asyncio.create_task(self._consume_ohlcv(topic)) for topic in topics]
        
        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            self.logger.info("FeatureFactory tasks cancelled")
    
    async def stop(self):
        """Gracefully stop feature computation"""
        self.logger.info("🛑 FeatureFactory stopping...")
        self._running = False
        
        # Cancel all running tasks
        for task in self._tasks:
            if not task.done():
                task.cancel()
        
        # Wait for all tasks to complete
        await asyncio.gather(*self._tasks, return_exceptions=True)
        
        # Clear cache to free memory
        self.ohlcv_cache.clear()
        
        self.logger.info("✅ FeatureFactory stopped")
    
    async def _consume_ohlcv(self, topic: str):
        """Consume OHLCV data and compute features"""
        self.logger.info(f"📊 Consuming {topic}...")
        
        async for msg in self.bus.consume(topic):
            # Check if we should stop
            if not self._running:
                break
                
            try:
                # Start timing for metrics
                start_time = time.time()
                
                # Extract timeframe from topic name
                timeframe = topic.split('_')[-1]  # e.g., "1m" from "curated.data.ohlcv_1m"
                
                # Update cache with new bar
                self._update_cache(msg, timeframe)
                
                # Only compute features from 1m bars (most frequent)
                # Other timeframes just update the cache for multi-horizon calculations
                if timeframe == '1m':
                    features = await self._compute_features(msg)
                    
                    if features:
                        # Publish to features.base topic
                        await self.bus.publish(
                            topic="features.base",
                            data=asdict(features)
                        )
                        
                        # Record metrics
                        if METRICS_AVAILABLE and _metrics_collector:
                            computation_time = time.time() - start_time
                            _metrics_collector.observe_histogram(
                                'feature_computation_seconds',
                                computation_time,
                                labels={'symbol': features.symbol, 'feature_type': 'base'}
                            )
                            
                            _metrics_collector.increment_counter(
                                'features_published_total',
                                labels={'symbol': features.symbol, 'feature_type': 'base'}
                            )
                            
                            _metrics_collector.set_gauge(
                                'feature_confidence_score',
                                features.confidence,
                                labels={'symbol': features.symbol}
                            )
                            
                            _metrics_collector.set_gauge(
                                'feature_data_age_milliseconds',
                                features.data_age_ms,
                                labels={'symbol': features.symbol}
                            )
                
            except Exception as e:
                self.logger.error(f"Error computing features from {topic}: {e}", exc_info=True)
                
                # Record error metric
                if METRICS_AVAILABLE and _metrics_collector and 'symbol' in msg:
                    _metrics_collector.increment_counter(
                        'feature_computation_errors_total',
                        labels={'symbol': msg.get('symbol', 'unknown'), 'error_type': type(e).__name__}
                    )
    
    def _update_cache(self, ohlcv_bar: Dict, timeframe: str):
        """Update in-memory cache of recent bars"""
        symbol = ohlcv_bar['symbol']
        
        # Initialize cache structure if needed
        if symbol not in self.ohlcv_cache:
            self.ohlcv_cache[symbol] = {}
        
        if timeframe not in self.ohlcv_cache[symbol]:
            max_len = self.max_cache_bars.get(timeframe, 100)
            self.ohlcv_cache[symbol][timeframe] = deque(maxlen=max_len)
        
        # Add new bar (deque automatically removes oldest if at maxlen)
        self.ohlcv_cache[symbol][timeframe].append(ohlcv_bar)
    
    async def _compute_features(self, current_bar: Dict) -> Optional[BaseFeatures]:
        """Compute all foundation features from OHLCV data"""
        symbol = current_bar['symbol']
        
        # Get cached bars for multi-horizon calculations
        bars_1m = self._get_cached_bars(symbol, "1m", count=60)
        bars_5m = self._get_cached_bars(symbol, "5m", count=12)
        bars_15m = self._get_cached_bars(symbol, "15m", count=4)
        bars_1h = self._get_cached_bars(symbol, "1h", count=24)
        
        # Need minimum data to compute features
        if len(bars_1m) < 5:
            self.logger.debug(f"Insufficient data for {symbol}: only {len(bars_1m)} 1m bars")
            return None
        
        # Compute returns (percent change)
        returns_1m = self._compute_returns(bars_1m, periods=1)
        returns_5m = self._compute_returns(bars_5m, periods=1)
        returns_15m = self._compute_returns(bars_15m, periods=1)
        returns_1h = self._compute_returns(bars_1h, periods=1)
        
        # Compute volatility (standard deviation of returns)
        vol_5m = self._compute_volatility(bars_1m, window=5)
        vol_15m = self._compute_volatility(bars_1m, window=15)
        vol_1h = self._compute_volatility(bars_1m, window=60)
        vol_1d = self._compute_volatility(bars_1h, window=24)
        
        # Compute risk-adjusted returns
        sharpe_5m = self._compute_sharpe_ratio(returns_5m, vol_5m)
        sharpe_1h = self._compute_sharpe_ratio(returns_1h, vol_1h)
        sortino_5m = self._compute_sortino_ratio(bars_1m, window=5, returns=returns_5m)
        sortino_1h = self._compute_sortino_ratio(bars_1m, window=60, returns=returns_1h)
        skewness = self._compute_skewness(bars_1m, window=60)
        kurtosis = self._compute_kurtosis(bars_1m, window=60)
        
        # Compute momentum signals
        vw_momentum = self._compute_volume_weighted_momentum(bars_1m, window=20)
        vol_acceleration = self._compute_volume_acceleration(bars_1m, window=10)
        vwap_dev = self._compute_vwap_deviation(bars_1m, window=20)
        
        # Compute microstructure features
        spread = self._compute_spread(current_bar)
        imbalance = self._compute_imbalance(current_bar)
        volume_profile = self._compute_volume_profile(bars_1m)
        
        # Quality metrics
        confidence = self._compute_confidence(bars_1m)
        data_age = self._compute_data_age(current_bar)
        
        return BaseFeatures(
            symbol=symbol,
            timestamp=current_bar['timestamp'],
            returns_1m=returns_1m,
            returns_5m=returns_5m,
            returns_15m=returns_15m,
            returns_1h=returns_1h,
            volatility_5m=vol_5m,
            volatility_15m=vol_15m,
            volatility_1h=vol_1h,
            volatility_1d=vol_1d,
            sharpe_ratio_5m=sharpe_5m,
            sharpe_ratio_1h=sharpe_1h,
            sortino_ratio_5m=sortino_5m,
            sortino_ratio_1h=sortino_1h,
            skewness=skewness,
            kurtosis=kurtosis,
            volume_weighted_momentum=vw_momentum,
            volume_acceleration=vol_acceleration,
            vwap_deviation=vwap_dev,
            bid_ask_spread=spread,
            orderbook_imbalance=imbalance,
            volume_profile=volume_profile,
            confidence=confidence,
            data_age_ms=data_age
        )
    
    def _get_cached_bars(self, symbol: str, timeframe: str, count: int) -> List:
        """Get recent bars from cache"""
        if symbol not in self.ohlcv_cache:
            return []
        
        if timeframe not in self.ohlcv_cache[symbol]:
            return []
        
        bars = list(self.ohlcv_cache[symbol][timeframe])
        
        # Return last N bars (or all if less than N)
        return bars[-count:] if len(bars) >= count else bars
    
    def _compute_returns(self, bars: List, periods: int = 1) -> float:
        """Compute percentage return over N periods"""
        if len(bars) < periods + 1:
            return 0.0
        
        try:
            current_close = bars[-1]['close']
            past_close = bars[-periods-1]['close']
            
            if past_close == 0:
                return 0.0
            
            return_pct = ((current_close - past_close) / past_close) * 100
            # Returns can be negative, just ensure finite
            if math.isnan(return_pct) or math.isinf(return_pct):
                return 0.0
            return float(return_pct)
        except (KeyError, IndexError, TypeError):
            return 0.0
    
    def _compute_volatility(self, bars: List, window: int) -> float:
        """Compute rolling volatility (annualized standard deviation)."""
        try:
            if len(bars) < window + 1:
                return 0.0
            
            # Compute returns from bars
            returns = []
            for i in range(1, min(len(bars), window + 1)):
                current_close = bars[-i]['close']
                prev_close = bars[-i-1]['close']
                if prev_close > 0:
                    ret = ((current_close - prev_close) / prev_close)
                    returns.append(ret)
            
            if len(returns) < window:
                return 0.0
            
            # Calculate standard deviation
            returns_array = np.array(returns[-window:])
            volatility = float(np.std(returns_array))
            
            # Annualize assuming 1-minute bars (525600 minutes per year)
            # Note: window is lookback period, not bar frequency - always use sqrt(525600)
            annualization_factor = np.sqrt(525600)
            annualized_vol = volatility * annualization_factor
            return _safe_positive(annualized_vol)
        except Exception as e:
            logger.error(f"Error computing volatility: {e}")
            return 0.0
    
    def _compute_sharpe_ratio(self, returns: float, volatility: float) -> float:
        """
        Compute Sharpe Ratio (risk-adjusted return).
        
        Sharpe = Return / Volatility
        Higher is better - shows return per unit of risk taken.
        """
        try:
            if volatility == 0:
                return 0.0
            
            # Returns are in %, volatility is annualized %
            # Return same scale for consistency
            sharpe = returns / volatility if abs(volatility) > 1e-8 else 0.0
            # Sharpe can be negative, just ensure finite
            if math.isnan(sharpe) or math.isinf(sharpe):
                return 0.0
            return float(sharpe)
        except (TypeError, ZeroDivisionError):
            return 0.0
    
    def _compute_sortino_ratio(self, bars: List, window: int, returns: float) -> float:
        """
        Compute Sortino Ratio (downside risk-adjusted return).
        
        Sortino = Return / Downside Volatility
        Only penalizes downside volatility (negative returns).
        Better than Sharpe for asymmetric return distributions.
        """
        try:
            if len(bars) < window + 1:
                return 0.0
            
            # Compute returns from bars
            returns_list = []
            for i in range(1, min(len(bars), window + 1)):
                current_close = bars[-i]['close']
                prev_close = bars[-i-1]['close']
                if prev_close > 0:
                    ret = ((current_close - prev_close) / prev_close)
                    returns_list.append(ret)
            
            if len(returns_list) < window:
                return 0.0
            
            # Calculate downside deviation (only negative returns)
            returns_array = np.array(returns_list[-window:])
            downside_returns = returns_array[returns_array < 0]
            
            if len(downside_returns) == 0:
                return 0.0  # No downside risk
            
            downside_volatility = float(np.std(downside_returns))
            
            # Annualize downside volatility
            annualization_factor = np.sqrt(525600)
            downside_vol_annualized = downside_volatility * annualization_factor
            
            if downside_vol_annualized == 0:
                return 0.0
            
            # Returns are in %, volatility is annualized %
            sortino = returns / downside_vol_annualized if abs(downside_vol_annualized) > 1e-8 else 0.0
            # Sortino can be negative, just ensure finite
            if math.isnan(sortino) or math.isinf(sortino):
                return 0.0
            return float(sortino)
        except Exception as e:
            logger.error(f"Error computing Sortino ratio: {e}")
            return 0.0
    
    def _compute_skewness(self, bars: List, window: int = 60) -> float:
        """
        Compute skewness of return distribution.
        
        Skewness measures asymmetry:
        - Negative: Left tail (crashes)
        - Positive: Right tail (surges)
        - Zero: Symmetric distribution
        
        In crypto, typically negative (crash risk).
        """
        try:
            if len(bars) < window + 1:
                return 0.0
            
            # Compute returns from bars
            returns = []
            for i in range(1, min(len(bars), window + 1)):
                current_close = bars[-i]['close']
                prev_close = bars[-i-1]['close']
                if prev_close > 0:
                    ret = ((current_close - prev_close) / prev_close)
                    returns.append(ret)
            
            if len(returns) < window:
                return 0.0
            
            returns_array = np.array(returns[-window:])
            
            # scipy.stats.skew would be more accurate, but using numpy for consistency
            # Manual skewness calculation: E[(X - μ)³] / σ³
            mean = np.mean(returns_array)
            std = np.std(returns_array)
            
            if std == 0:
                return 0.0
            
            skew = float(np.mean(((returns_array - mean) / std) ** 3))
            # Skewness can be negative, just ensure finite
            if math.isnan(skew) or math.isinf(skew):
                return 0.0
            return skew
        except Exception as e:
            logger.error(f"Error computing skewness: {e}")
            return 0.0
    
    def _compute_kurtosis(self, bars: List, window: int = 60) -> float:
        """
        Compute kurtosis of return distribution.
        
        Kurtosis measures tail thickness:
        - High (>3): Fat tails (extreme events more likely)
        - Normal (≈3): Gaussian distribution
        - Low (<3): Thin tails
        
        Crypto typically has high kurtosis (fat tails).
        Returns excess kurtosis (kurtosis - 3).
        """
        try:
            if len(bars) < window + 1:
                return 0.0
            
            # Compute returns from bars
            returns = []
            for i in range(1, min(len(bars), window + 1)):
                current_close = bars[-i]['close']
                prev_close = bars[-i-1]['close']
                if prev_close > 0:
                    ret = ((current_close - prev_close) / prev_close)
                    returns.append(ret)
            
            if len(returns) < window:
                return 0.0
            
            returns_array = np.array(returns[-window:])
            
            # Manual kurtosis calculation: E[(X - μ)⁴] / σ⁴
            mean = np.mean(returns_array)
            std = np.std(returns_array)
            
            if std == 0:
                return 0.0
            
            # Excess kurtosis (kurtosis - 3, so normal distribution = 0)
            kurt = float(np.mean(((returns_array - mean) / std) ** 4) - 3)
            # Kurtosis can be negative, just ensure finite
            if math.isnan(kurt) or math.isinf(kurt):
                return 0.0
            return kurt
        except Exception as e:
            logger.error(f"Error computing kurtosis: {e}")
            return 0.0
    
    def _compute_volume_weighted_momentum(self, bars: List, window: int = 20) -> float:
        """
        Compute momentum weighted by volume participation.
        
        Traditional momentum: Just price change
        Volume-weighted: Price change scaled by volume intensity
        
        Higher volume moves are more significant.
        """
        try:
            if len(bars) < window + 1:
                return 0.0
            
            recent_bars = bars[-window:]
            
            # Compute price returns and volume weights
            returns = []
            volumes = []
            
            for i in range(1, len(recent_bars)):
                curr = recent_bars[i]
                prev = recent_bars[i-1]
                
                if prev['close'] > 0 and 'volume' in curr:
                    ret = (curr['close'] - prev['close']) / prev['close']
                    vol = curr['volume']
                    returns.append(ret)
                    volumes.append(vol)
            
            if len(returns) == 0:
                return 0.0
            
            returns_array = np.array(returns)
            volumes_array = np.array(volumes)
            
            # Avoid division by zero
            if np.sum(volumes_array) == 0:
                return 0.0
            
            # Volume-weighted momentum
            weights = volumes_array / np.sum(volumes_array)
            vw_momentum = float(np.sum(returns_array * weights))
            
            momentum_pct = vw_momentum * 100  # Convert to percentage
            # Momentum can be negative, just ensure finite
            if math.isnan(momentum_pct) or math.isinf(momentum_pct):
                return 0.0
            return momentum_pct
        except Exception as e:
            logger.error(f"Error computing volume-weighted momentum: {e}")
            return 0.0
    
    def _compute_volume_acceleration(self, bars: List, window: int = 10) -> float:
        """
        Compute rate of change in volume.
        
        Measures if volume is increasing (positive) or decreasing (negative).
        Accelerating volume confirms trend strength.
        """
        try:
            if len(bars) < window * 2:
                return 0.0
            
            # Get recent and previous period volumes
            recent_bars = bars[-window:]
            previous_bars = bars[-window*2:-window]
            
            recent_volumes = [b['volume'] for b in recent_bars if 'volume' in b]
            previous_volumes = [b['volume'] for b in previous_bars if 'volume' in b]
            
            if len(recent_volumes) < window // 2 or len(previous_volumes) < window // 2:
                return 0.0
            
            recent_avg = np.mean(recent_volumes)
            previous_avg = np.mean(previous_volumes)
            
            if previous_avg == 0:
                return 0.0
            
            # Percentage change in average volume
            acceleration = float((recent_avg - previous_avg) / previous_avg * 100)
            # Acceleration can be negative, just ensure finite
            if math.isnan(acceleration) or math.isinf(acceleration):
                return 0.0
            return acceleration
        except Exception as e:
            logger.error(f"Error computing volume acceleration: {e}")
            return 0.0
    
    def _compute_vwap_deviation(self, bars: List, window: int = 20) -> float:
        """
        Compute deviation from Volume-Weighted Average Price (VWAP).
        
        VWAP = Σ(Price × Volume) / Σ(Volume)
        
        Deviation shows if current price is above/below fair value:
        - Positive: Price above VWAP (potentially overbought)
        - Negative: Price below VWAP (potentially oversold)
        """
        try:
            if len(bars) < window:
                return 0.0
            
            recent_bars = bars[-window:]
            
            # Calculate VWAP
            price_volume_sum = 0.0
            volume_sum = 0.0
            
            for bar in recent_bars:
                if 'volume' in bar and bar['volume'] > 0:
                    # Use typical price: (high + low + close) / 3
                    if 'high' in bar and 'low' in bar:
                        typical_price = (bar['high'] + bar['low'] + bar['close']) / 3
                    else:
                        typical_price = bar['close']
                    
                    price_volume_sum += typical_price * bar['volume']
                    volume_sum += bar['volume']
            
            if volume_sum == 0:
                return 0.0
            
            vwap = price_volume_sum / volume_sum
            current_price = bars[-1]['close']
            
            if vwap == 0:
                return 0.0
            
            # Deviation as percentage
            deviation = float((current_price - vwap) / vwap * 100)
            # Deviation can be negative, just ensure finite
            if math.isnan(deviation) or math.isinf(deviation):
                return 0.0
            return deviation
        except Exception as e:
            logger.error(f"Error computing VWAP deviation: {e}")
            return 0.0
    
    def _compute_spread(self, bar: Dict) -> float:
        """Compute bid-ask spread as % of mid"""
        try:
            # Check if bar has bid/ask data
            if 'bid' in bar and 'ask' in bar and bar['bid'] > 0 and bar['ask'] > 0:
                mid = (bar['bid'] + bar['ask']) / 2
                spread = bar['ask'] - bar['bid']
                
                if mid == 0:
                    return 0.0
                
                return (spread / mid) * 100
        except (KeyError, TypeError, ZeroDivisionError):
            pass
        
        return 0.0
    
    def _compute_imbalance(self, bar: Dict) -> float:
        """Compute order book imbalance (-1 to 1)"""
        try:
            # Check if bar has volume data
            if 'bid_volume' in bar and 'ask_volume' in bar:
                bid_vol = bar['bid_volume']
                ask_vol = bar['ask_volume']
                total = bid_vol + ask_vol
                
                if total > 0:
                    return (bid_vol - ask_vol) / total
        except (KeyError, TypeError, ZeroDivisionError):
            pass
        
        return 0.0
    
    def _compute_volume_profile(self, bars: List) -> float:
        """Compute volume trend (recent vs average)"""
        if len(bars) < 10:
            return 1.0
        
        try:
            volumes = [b['volume'] for b in bars if 'volume' in b]
            
            if len(volumes) < 10:
                return 1.0
            
            recent_volume = np.mean(volumes[-5:])
            avg_volume = np.mean(volumes)
            
            if avg_volume == 0:
                return 1.0
            
            return float(recent_volume / avg_volume)
        except (KeyError, TypeError, ValueError):
            return 1.0
    
    def _compute_confidence(self, bars: List) -> float:
        """Compute confidence score based on data quality"""
        if len(bars) < 5:
            return 0.5  # Low confidence with sparse data
        
        # Higher confidence with more data and recent updates
        # Full confidence at 60 bars (1 hour of 1-minute data)
        data_coverage = min(len(bars) / 60, 1.0)
        
        return data_coverage
    
    def _compute_data_age(self, bar: Dict) -> int:
        """Compute age of data in milliseconds"""
        try:
            now = datetime.now().timestamp()
            bar_time = bar['timestamp']
            return int((now - bar_time) * 1000)
        except (KeyError, TypeError):
            return 0
