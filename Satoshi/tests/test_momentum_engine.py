"""
Test suite for MomentumEngine - multi-horizon momentum features
"""

import pytest
import math
import numpy as np
from engines.features.momentum_engine import (
    MomentumEngine, 
    MomentumFeatures,
    _safe_probability, 
    _safe_positive,
    _safe_finite
)


class TestValidationHelpers:
    """Test defensive validation helpers"""
    
    def test_safe_finite_valid(self):
        """Test valid finite values"""
        assert _safe_finite(0.0) == 0.0
        assert _safe_finite(10.5) == 10.5
        assert _safe_finite(-5.3) == -5.3  # Can be negative
    
    def test_safe_finite_nan(self):
        """Test NaN handling"""
        assert _safe_finite(float('nan')) == 0.0
        assert _safe_finite(float('nan'), default=10.0) == 10.0
    
    def test_safe_finite_inf(self):
        """Test Inf handling"""
        assert _safe_finite(float('inf')) == 0.0
        assert _safe_finite(float('-inf')) == 0.0


class TestMomentumEngine:
    """Test MomentumEngine computations"""
    
    @pytest.fixture
    def mock_bus(self):
        """Mock streaming bus for testing"""
        class MockBus:
            async def subscribe(self, *args, **kwargs):
                pass
            async def publish(self, *args, **kwargs):
                pass
            async def consume(self, *args, **kwargs):
                # Return empty async iterator
                async def empty_iterator():
                    return
                    yield  # Make it a generator
                return empty_iterator()
        return MockBus()
    
    @pytest.fixture
    def engine(self, mock_bus):
        """Create MomentumEngine instance"""
        return MomentumEngine(mock_bus)
    
    def test_init(self, engine):
        """Test engine initialization"""
        assert engine is not None
        assert engine.circuit_open is False
        assert engine.circuit_failures == 0
        assert engine.features_computed == 0
    
    def test_compute_horizon_momentum_positive(self, engine):
        """Test momentum computation with positive return"""
        bars = [
            {'close': 100.0, 'volume': 1000},
            {'close': 105.0, 'volume': 1100}
        ]
        momentum = engine._compute_horizon_momentum(bars, periods=1)
        assert abs(momentum - 5.0) < 0.01  # 5% return
    
    def test_compute_horizon_momentum_negative(self, engine):
        """Test momentum computation with negative return"""
        bars = [
            {'close': 100.0, 'volume': 1000},
            {'close': 95.0, 'volume': 1100}
        ]
        momentum = engine._compute_horizon_momentum(bars, periods=1)
        assert abs(momentum - (-5.0)) < 0.01  # -5% return
    
    def test_compute_horizon_momentum_insufficient_data(self, engine):
        """Test momentum with insufficient bars"""
        bars = [{'close': 100.0}]
        momentum = engine._compute_horizon_momentum(bars, periods=5)
        assert momentum == 0.0
    
    def test_compute_horizon_momentum_zero_price(self, engine):
        """Test momentum with zero price (edge case)"""
        bars = [
            {'close': 0.0, 'volume': 1000},
            {'close': 100.0, 'volume': 1000}
        ]
        momentum = engine._compute_horizon_momentum(bars, periods=1)
        assert momentum == 0.0  # Should handle gracefully
    
    def test_detect_trend_uptrend(self, engine):
        """Test trend detection for uptrend"""
        closes = np.array([100.0, 101.0, 102.5, 104.0, 106.0])
        momentum_horizons = [1.0, 2.0, 3.0, 4.0]  # All positive
        
        direction, strength, confidence = engine._detect_trend(closes, momentum_horizons)
        
        assert direction == "up"
        assert strength > 0.0
        assert confidence == 1.0  # All horizons agree (all positive)
    
    def test_detect_trend_downtrend(self, engine):
        """Test trend detection for downtrend"""
        closes = np.array([100.0, 98.0, 96.0, 94.0, 92.0])
        momentum_horizons = [-1.0, -2.0, -3.0, -4.0]  # All negative
        
        direction, strength, confidence = engine._detect_trend(closes, momentum_horizons)
        
        assert direction == "down"
        assert strength > 0.0
        assert confidence == 1.0  # All horizons agree (all negative)
    
    def test_detect_trend_sideways(self, engine):
        """Test trend detection for sideways movement"""
        closes = np.array([100.0, 100.1, 99.9, 100.0, 100.1])
        momentum_horizons = [0.05, -0.05, 0.0, 0.03]  # Mixed small values
        
        direction, strength, confidence = engine._detect_trend(closes, momentum_horizons)
        
        assert direction == "sideways"
        assert strength < 0.3  # Weak trend
    
    def test_compute_momentum_strength_aligned(self, engine):
        """Test momentum strength with aligned horizons"""
        momentum_horizons = [2.0, 2.5, 3.0, 3.5, 4.0, 4.5]  # All positive
        
        strength = engine._compute_momentum_strength(momentum_horizons)
        
        assert strength > 0.2  # Reasonable alignment (algorithm weights by magnitude)
        assert not math.isnan(strength)
        assert not math.isinf(strength)
    
    def test_compute_momentum_strength_mixed(self, engine):
        """Test momentum strength with mixed directions"""
        momentum_horizons = [2.0, -1.0, 3.0, -0.5, 1.0, -2.0]  # Mixed
        
        strength = engine._compute_momentum_strength(momentum_horizons)
        
        assert strength < 0.6  # Weaker alignment
        assert not math.isnan(strength)
    
    def test_compute_momentum_strength_insufficient_data(self, engine):
        """Test momentum strength with insufficient horizons"""
        momentum_horizons = [2.0]  # Only one horizon
        
        strength = engine._compute_momentum_strength(momentum_horizons)
        
        assert strength == 0.0
    
    def test_compute_momentum_acceleration(self, engine):
        """Test momentum acceleration computation"""
        # Accelerating upward (slope increasing)
        closes = np.array([100, 101, 102, 103, 104, 105, 107, 109, 112, 116])
        
        acceleration = engine._compute_momentum_acceleration(closes)
        
        assert acceleration > 0  # Positive acceleration
        assert not math.isnan(acceleration)
        assert not math.isinf(acceleration)
    
    def test_compute_momentum_acceleration_decelerating(self, engine):
        """Test momentum with deceleration"""
        # Decelerating (was going up fast, now slower)
        closes = np.array([100, 105, 110, 115, 120, 121, 122, 123, 124, 125])
        
        acceleration = engine._compute_momentum_acceleration(closes)
        
        assert acceleration < 0  # Negative acceleration (decelerating)
    
    def test_compute_volume_confirmation_strong(self, engine):
        """Test volume confirmation with strong volume support"""
        closes = np.array([100 + i for i in range(25)])  # Steady uptrend
        volumes = np.array([1000] * 15 + [1500] * 10)  # Volume increased recently
        
        confirmation = engine._compute_volume_confirmation(closes, volumes)
        
        assert confirmation > 0.5  # Strong confirmation
        assert confirmation <= 1.0
    
    def test_compute_volume_confirmation_weak(self, engine):
        """Test volume confirmation with weak volume"""
        closes = np.array([100 + i for i in range(25)])  # Steady uptrend
        volumes = np.array([1500] * 15 + [800] * 10)  # Volume decreased recently
        
        confirmation = engine._compute_volume_confirmation(closes, volumes)
        
        assert confirmation < 0.6  # Weak confirmation
    
    def test_compute_volume_confirmation_insufficient_data(self, engine):
        """Test volume confirmation with insufficient data"""
        closes = np.array([100.0, 101.0])
        volumes = np.array([1000, 1100])
        
        confirmation = engine._compute_volume_confirmation(closes, volumes)
        
        assert confirmation == 0.5  # Neutral default
    
    def test_compute_price_volume_divergence_bearish(self, engine):
        """Test bearish divergence (price up, volume down)"""
        closes = np.linspace(100, 110, 20)  # Price trending up
        volumes = np.linspace(2000, 1000, 20)  # Volume trending down
        
        divergence = engine._compute_price_volume_divergence(closes, volumes)
        
        assert divergence < 0  # Bearish divergence
        assert divergence >= -1.0
    
    def test_compute_price_volume_divergence_bullish(self, engine):
        """Test bullish divergence (price down, volume up)"""
        closes = np.linspace(100, 90, 20)  # Price trending down
        volumes = np.linspace(1000, 2000, 20)  # Volume trending up
        
        divergence = engine._compute_price_volume_divergence(closes, volumes)
        
        assert divergence > 0  # Bullish divergence
        assert divergence <= 1.0
    
    def test_compute_price_volume_divergence_no_divergence(self, engine):
        """Test no divergence (price and volume aligned)"""
        closes = np.linspace(100, 110, 20)  # Price up
        volumes = np.linspace(1000, 1500, 20)  # Volume up
        
        divergence = engine._compute_price_volume_divergence(closes, volumes)
        
        assert abs(divergence) < 0.1  # Minimal divergence
    
    def test_compute_momentum_divergence_bearish(self, engine):
        """Test bearish momentum divergence (short < long)"""
        short_momentum = 1.0  # Weakening
        long_momentum = 5.0  # Was strong
        
        divergence = engine._compute_momentum_divergence(short_momentum, long_momentum)
        
        assert divergence < 0  # Bearish (momentum weakening)
    
    def test_compute_momentum_divergence_bullish(self, engine):
        """Test bullish momentum divergence (short > long)"""
        short_momentum = 5.0  # Strengthening
        long_momentum = 1.0  # Was weak
        
        divergence = engine._compute_momentum_divergence(short_momentum, long_momentum)
        
        assert divergence > 0  # Bullish (momentum strengthening)
    
    def test_compute_momentum_divergence_weak_signal(self, engine):
        """Test momentum divergence with weak long momentum"""
        short_momentum = 0.5
        long_momentum = 0.05  # Very small
        
        divergence = engine._compute_momentum_divergence(short_momentum, long_momentum)
        
        assert divergence == 0.0  # Avoids division by small numbers
    
    def test_compute_confidence_score_high_quality(self, engine):
        """Test confidence score with high quality data"""
        num_bars = 200  # More than minimum
        trend_confidence = 0.9
        momentum_strength = 0.8
        volume_confirmation = 0.85
        
        confidence = engine._compute_confidence_score(
            num_bars, trend_confidence, momentum_strength, volume_confirmation
        )
        
        assert confidence > 0.8  # High confidence
        assert confidence <= 1.0
    
    def test_compute_confidence_score_low_quality(self, engine):
        """Test confidence score with low quality data"""
        num_bars = 50  # Less than ideal
        trend_confidence = 0.3
        momentum_strength = 0.2
        volume_confirmation = 0.4
        
        confidence = engine._compute_confidence_score(
            num_bars, trend_confidence, momentum_strength, volume_confirmation
        )
        
        assert confidence < 0.4  # Low confidence
    
    def test_update_cache(self, engine):
        """Test price cache update"""
        symbol = "BTC-USD"
        bar = {'close': 100.0, 'volume': 1000, 'timestamp': 1234567890}
        timeframe = '1h'
        
        engine._update_cache(symbol, bar, timeframe)
        
        assert symbol in engine.price_cache
        assert timeframe in engine.price_cache[symbol]
        assert len(engine.price_cache[symbol][timeframe]) == 1
        assert engine.price_cache[symbol][timeframe][0] == bar
    
    def test_update_cache_trimming(self, engine):
        """Test cache trimming to max size"""
        symbol = "BTC-USD"
        timeframe = '1h'
        max_size = engine.cache_sizes[timeframe]
        
        # Add more bars than max_size
        for i in range(max_size + 50):
            bar = {'close': 100.0 + i, 'volume': 1000, 'timestamp': i}
            engine._update_cache(symbol, bar, timeframe)
        
        # Cache should be trimmed to max_size
        assert len(engine.price_cache[symbol][timeframe]) == max_size
        # Should keep most recent bars
        assert engine.price_cache[symbol][timeframe][-1]['close'] == 100.0 + max_size + 49
    
    def test_circuit_breaker_opens(self, engine):
        """Test circuit breaker opens after threshold failures"""
        # Increment failures to threshold
        for _ in range(engine.circuit_failure_threshold):
            engine._increment_circuit_failures()
        
        assert engine.circuit_open is True
        assert engine._is_circuit_open() is True
    
    def test_circuit_breaker_recovery(self, engine):
        """Test circuit breaker recovers after delay"""
        import time
        
        # Open circuit
        for _ in range(engine.circuit_failure_threshold):
            engine._increment_circuit_failures()
        
        assert engine.circuit_open is True
        
        # Fast-forward recovery time
        engine.circuit_recovery_delay_sec = 0  # Instant recovery for testing
        
        # Check recovery
        is_open = engine._is_circuit_open()
        assert is_open is False
        assert engine.circuit_failures == 0


class TestDefensiveValidation:
    """Test that all computations handle extreme values"""
    
    @pytest.fixture
    def mock_bus(self):
        """Mock streaming bus for testing"""
        class MockBus:
            async def subscribe(self, *args, **kwargs):
                pass
            async def publish(self, *args, **kwargs):
                pass
            async def consume(self, *args, **kwargs):
                async def empty_iterator():
                    return
                    yield
                return empty_iterator()
        return MockBus()
    
    @pytest.fixture
    def engine(self, mock_bus):
        """Create MomentumEngine instance"""
        return MomentumEngine(mock_bus)
    
    def test_momentum_handles_nan_prices(self, engine):
        """Test momentum with NaN prices"""
        bars = [
            {'close': float('nan'), 'volume': 1000},
            {'close': 100.0, 'volume': 1000}
        ]
        momentum = engine._compute_horizon_momentum(bars, periods=1)
        # Should return 0.0 or finite value, not NaN
        assert not math.isnan(momentum)
    
    def test_trend_detection_handles_constant_prices(self, engine):
        """Test trend detection with constant price"""
        closes = np.array([100.0] * 10)
        momentum_horizons = [0.0, 0.0, 0.0]
        
        direction, strength, confidence = engine._detect_trend(closes, momentum_horizons)
        
        assert direction == "sideways"
        assert not math.isnan(strength)
        assert not math.isnan(confidence)
    
    def test_acceleration_handles_insufficient_data(self, engine):
        """Test acceleration with minimal data"""
        closes = np.array([100.0, 101.0])
        
        acceleration = engine._compute_momentum_acceleration(closes)
        
        assert acceleration == 0.0
    
    def test_divergence_handles_zero_volume(self, engine):
        """Test price-volume divergence with zero volumes"""
        closes = np.array([100 + i for i in range(20)])
        volumes = np.array([0.0] * 20)
        
        divergence = engine._compute_price_volume_divergence(closes, volumes)
        
        assert not math.isnan(divergence)
        assert not math.isinf(divergence)
