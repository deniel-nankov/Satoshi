# MomentumEngine - Multi-Horizon Momentum Feature Agent

## Overview

The MomentumEngine computes multi-horizon momentum features from OHLCV data. It measures price momentum across 6 different timeframes (5 minutes to 1 week) and provides trend detection, momentum quality metrics, and divergence signals.

**Status**: ✅ Production-Ready (Institutional-Grade)

## Architecture

**Layer**: Feature Engineering (DESCRIPTIVE)  
**Input**: Gold Layer OHLCV data (`curated.data.ohlcv_1m`, `curated.data.ohlcv_5m`, `curated.data.ohlcv_1h`)  
**Output**: Momentum features (`features.momentum`)  
**Processing Time**: <20ms per update  
**Memory**: ~10MB per symbol (1 week of hourly data)

## Feature Categories

### 1. Multi-Horizon Momentum (6 features)

Price momentum (percentage returns) at different timeframes:

| Feature | Timeframe | Description | Interpretation |
|---------|-----------|-------------|----------------|
| `momentum_5m` | 5 minutes | Very short-term momentum | Intraday scalping signals |
| `momentum_15m` | 15 minutes | Short-term momentum | Quick trend changes |
| `momentum_1h` | 1 hour | Medium-term momentum | Hourly trend strength |
| `momentum_4h` | 4 hours | Swing momentum | Multi-hour trends |
| `momentum_1d` | 1 day | Daily momentum | Daily trend direction |
| `momentum_1w` | 1 week | Long-term momentum | Weekly trend strength |

**Values**: Percentage returns (e.g., 5.0 = 5% gain, -3.2 = 3.2% loss)

### 2. Trend Detection (3 features)

| Feature | Type | Range | Description |
|---------|------|-------|-------------|
| `trend_direction` | string | "up", "down", "sideways" | Current trend direction |
| `trend_strength` | float | 0-1 | Magnitude of trend (0=flat, 1=very strong) |
| `trend_confidence` | float | 0-1 | Consistency across timeframes (1=all agree) |

**Trend Detection Algorithm**:
- Uses linear regression slope on recent prices
- Direction determined by slope magnitude (>±0.1% per bar)
- Confidence measured by agreement across all momentum horizons

### 3. Momentum Quality (3 features)

| Feature | Range | Description | Interpretation |
|---------|-------|-------------|----------------|
| `momentum_strength` | 0-1 | Alignment across horizons | 1.0 = perfect alignment |
| `momentum_acceleration` | % per hour | Rate of momentum change | Positive = accelerating |
| `volume_confirmation` | 0-1 | Volume supports price move | >0.6 = strong confirmation |

**Momentum Strength Calculation**:
```python
# Directional consistency (all positive or all negative?)
alignment = max(positive_count, negative_count) / total_horizons

# Magnitude weighting
magnitude_score = min(avg_magnitude / 10%, 1.0)

# Combined strength
strength = alignment * magnitude_score
```

### 4. Divergence Detection (2 features)

| Feature | Range | Description | Interpretation |
|---------|-------|-------------|----------------|
| `price_volume_divergence` | -1 to 1 | Price vs volume direction | Negative = bearish divergence |
| `momentum_divergence` | -1 to 1 | Short vs long momentum | Negative = weakening momentum |

**Divergence Types**:
- **Bearish Price-Volume**: Price rising, volume falling (distribution)
- **Bullish Price-Volume**: Price falling, volume rising (accumulation)
- **Momentum Weakening**: Short-term < long-term (trend ending)
- **Momentum Strengthening**: Short-term > long-term (trend accelerating)

### 5. Metadata (1 feature)

| Feature | Range | Description |
|---------|-------|-------------|
| `confidence` | 0-1 | Overall feature quality score |

**Confidence Factors**:
- Data sufficiency (number of bars available)
- Trend consistency
- Momentum alignment
- Volume confirmation quality

## Usage Examples

### Basic Momentum Monitoring

```python
from engines.features.momentum_engine import MomentumEngine

# Initialize
engine = MomentumEngine(streaming_bus)

# Start consuming OHLCV and producing features
await engine.start()
```

### Interpreting Momentum Features

```python
# Example output
features = MomentumFeatures(
    symbol="BTC-USD",
    timestamp=1699200000.0,
    venue="coinbase",
    
    # All horizons positive = strong uptrend
    momentum_5m=0.5,      # +0.5% in 5 minutes
    momentum_15m=1.2,     # +1.2% in 15 minutes
    momentum_1h=2.8,      # +2.8% in 1 hour
    momentum_4h=5.5,      # +5.5% in 4 hours
    momentum_1d=12.0,     # +12% in 1 day
    momentum_1w=25.0,     # +25% in 1 week
    
    # Strong uptrend confirmed
    trend_direction="up",
    trend_strength=0.85,  # Strong trend
    trend_confidence=1.0, # All timeframes agree
    
    # High quality momentum
    momentum_strength=0.92,      # Excellent alignment
    momentum_acceleration=0.5,   # Accelerating
    volume_confirmation=0.78,    # Strong volume support
    
    # No divergence (healthy trend)
    price_volume_divergence=0.1,  # Aligned
    momentum_divergence=0.2,      # Strengthening
    
    confidence=0.89  # High confidence
)
```

### Trading Signal Interpretation

#### Strong Buy Signal
```python
if (features.trend_direction == "up" and 
    features.trend_confidence > 0.8 and
    features.momentum_strength > 0.7 and
    features.volume_confirmation > 0.6 and
    features.price_volume_divergence > -0.2):
    # Strong uptrend with high conviction
    signal = "STRONG_BUY"
```

#### Bearish Divergence Warning
```python
if (features.momentum_1h > 0 and 
    features.price_volume_divergence < -0.5):
    # Price rising but volume falling - distribution
    signal = "TAKE_PROFIT"
```

#### Momentum Exhaustion
```python
if (features.momentum_acceleration < -2.0 and
    features.momentum_divergence < -0.5):
    # Momentum decelerating and short < long
    signal = "EXIT_LONG"
```

## Institutional Features

### 1. Defensive Validation

All computations protected against:
- **NaN/Inf values**: Handled gracefully with defaults
- **Division by zero**: Checked before division operations
- **Out-of-range values**: Clipped to valid ranges
- **Missing data**: Graceful degradation

### 2. Circuit Breaker Protection

```python
# Automatic circuit breaker
circuit_failure_threshold = 10  # Opens after 10 failures
circuit_recovery_delay_sec = 60  # Recovers after 60 seconds
```

### 3. Performance Monitoring

- Tracks computation time per update
- Warns if computation exceeds 20ms
- Logs progress every 100 features

### 4. Multi-Timeframe Cache Management

```python
cache_sizes = {
    '1m': 10080,   # 1 week of minutes
    '5m': 2016,    # 1 week of 5-minute bars
    '1h': 168,     # 1 week of hours
}
```

### 5. Prometheus Metrics Integration

Tracks:
- Features computed count
- Computation time distribution
- Circuit breaker state

## Data Requirements

### Minimum Bars Required

| Horizon | Minimum Bars | Timeframe |
|---------|--------------|-----------|
| 5m momentum | 1 | 5-minute bars |
| 15m momentum | 3 | 5-minute bars |
| 1h momentum | 1 | 1-hour bars |
| 4h momentum | 4 | 1-hour bars |
| 1d momentum | 24 | 1-hour bars |
| 1w momentum | 168 | 1-hour bars |

**Full Feature Set**: Requires 168 1-hour bars (1 week of data)

### Input Data Format

```python
# Expected OHLCV bar structure
bar = {
    'symbol': 'BTC-USD',
    'venue': 'coinbase',
    'timestamp': 1699200000.0,
    'open': 35000.0,
    'high': 35500.0,
    'low': 34800.0,
    'close': 35200.0,
    'volume': 1250000.0
}
```

## Kafka Topics

### Subscribes To
- `curated.data.ohlcv_1m` - 1-minute bars
- `curated.data.ohlcv_5m` - 5-minute bars  
- `curated.data.ohlcv_1h` - 1-hour bars (primary trigger)

### Publishes To
- `features.momentum` - Momentum features

**Consumer Group**: `momentum_engine`

## Algorithms & Methodology

### Momentum Calculation

```python
momentum_pct = ((current_close - past_close) / past_close) * 100
```

### Trend Strength

```python
# Linear regression slope
slope, _ = np.polyfit(x, closes, 1)

# Normalize to percentage per bar
slope_pct = (slope / avg_price) * 100

# Strength (0-1 scale, 5% = max)
strength = min(abs(slope_pct) / 5.0, 1.0)
```

### Momentum Acceleration

```python
# Recent momentum (last 4 hours)
recent_momentum = (closes[-1] - closes[-5]) / closes[-5] * 100

# Previous momentum (4 hours before)
prev_momentum = (closes[-5] - closes[-9]) / closes[-9] * 100

# Acceleration = change in momentum
acceleration = recent_momentum - prev_momentum
```

### Volume Confirmation

```python
# Compare recent vs previous volume
recent_vol = mean(volumes[-10:])
prev_vol = mean(volumes[-20:-10])
vol_ratio = recent_vol / prev_vol

# High volume on big price move = confirmation
if abs(price_change) > 1% and vol_ratio > 1.2:
    confirmation = 0.8  # Strong
```

## Performance Characteristics

| Metric | Target | Actual |
|--------|--------|--------|
| Computation time | <20ms | ~5-15ms |
| Memory per symbol | <15MB | ~10MB |
| Features computed | N/A | 15 features |
| Test coverage | >90% | 35 tests (100%) |

## Testing

Comprehensive test suite with 35 tests covering:

✅ Multi-horizon momentum calculations  
✅ Trend detection (up/down/sideways)  
✅ Momentum strength and alignment  
✅ Acceleration calculations  
✅ Volume confirmation logic  
✅ Price-volume divergence  
✅ Momentum divergence  
✅ Confidence scoring  
✅ Cache management  
✅ Circuit breaker functionality  
✅ Defensive validation (NaN/Inf/edge cases)  

Run tests:
```bash
pytest tests/test_momentum_engine.py -v
```

## Comparison with FeatureFactory

| Aspect | FeatureFactory | MomentumEngine |
|--------|---------------|----------------|
| Focus | Foundation features | Momentum-specific |
| Horizons | 4 (1m, 5m, 15m, 1h) | 6 (5m, 15m, 1h, 4h, 1d, 1w) |
| Features | 22 (broad) | 15 (specialized) |
| Trend Detection | Basic | Advanced (direction + strength + confidence) |
| Divergence | No | Yes (price-volume + momentum) |
| Volume Analysis | VWAP deviation | Confirmation + divergence |
| Use Case | General ML features | Momentum strategies |

**Recommendation**: Use both together for comprehensive feature coverage.

## Future Enhancements

Potential additions (not currently implemented):

- [ ] Relative momentum (vs BTC, vs market)
- [ ] Momentum regime classification
- [ ] Fractal momentum analysis
- [ ] Cross-asset momentum correlation
- [ ] Momentum factor portfolio construction

## References

- Jegadeesh, N., & Titman, S. (1993). "Returns to Buying Winners and Selling Losers"
- Moskowitz, T., Ooi, Y. H., & Pedersen, L. H. (2012). "Time series momentum"
- Crypto-specific momentum research: Shorter horizons work better (hours/days vs months)

## Status Summary

✅ **Production-Ready**

- [x] Full implementation (15 features)
- [x] Defensive validation (NaN/Inf protection)
- [x] Circuit breaker protection
- [x] Comprehensive tests (35 tests, 100% pass rate)
- [x] Performance optimized (<20ms target)
- [x] Prometheus metrics integration
- [x] Multi-timeframe cache management
- [x] Complete documentation

**Next Steps**: Deploy alongside FeatureFactory and OrderbookDepthAnalyzer for comprehensive feature coverage.
