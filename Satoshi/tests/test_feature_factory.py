"""
Test suite for FeatureFactory - foundation features from OHLCV data
"""

import pytest
import math
import numpy as np
from engines.features.feature_factory import FeatureFactory, _safe_probability, _safe_positive


class TestValidationHelpers:
    """Test defensive validation helpers"""
    
    def test_safe_probability_valid(self):
        """Test valid probability values"""
        assert _safe_probability(0.0) == 0.0
        assert _safe_probability(0.5) == 0.5
        assert _safe_probability(1.0) == 1.0
    
    def test_safe_probability_out_of_range(self):
        """Test out-of-range clipping"""
        assert _safe_probability(-0.5) == 0.0
        assert _safe_probability(1.5) == 1.0
        assert _safe_probability(-100.0) == 0.0
        assert _safe_probability(100.0) == 1.0
    
    def test_safe_probability_nan(self):
        """Test NaN handling"""
        assert _safe_probability(float('nan')) == 0.0
    
    def test_safe_probability_inf(self):
        """Test Inf handling"""
        assert _safe_probability(float('inf')) == 0.0
        assert _safe_probability(float('-inf')) == 0.0
    
    def test_safe_positive_valid(self):
        """Test valid positive values"""
        assert _safe_positive(0.0) == 0.0
        assert _safe_positive(10.5) == 10.5
        assert _safe_positive(1000.0) == 1000.0
    
    def test_safe_positive_negative(self):
        """Test negative value clipping"""
        assert _safe_positive(-5.0) == 0.0
        assert _safe_positive(-100.0) == 0.0
    
    def test_safe_positive_nan(self):
        """Test NaN handling with default"""
        assert _safe_positive(float('nan')) == 0.0
        assert _safe_positive(float('nan'), default=10.0) == 10.0
    
    def test_safe_positive_inf(self):
        """Test Inf handling"""
        assert _safe_positive(float('inf')) == 0.0
        assert _safe_positive(float('-inf')) == 0.0


class TestFeatureFactoryComputations:
    """Test feature computation methods"""
    
    @pytest.fixture
    def mock_bus(self):
        """Mock streaming bus for testing"""
        class MockBus:
            async def subscribe(self, *args, **kwargs):
                pass
            async def publish(self, *args, **kwargs):
                pass
        return MockBus()
    
    @pytest.fixture
    def factory(self, mock_bus):
        """Create FeatureFactory instance"""
        return FeatureFactory(mock_bus)
    
    def test_compute_returns_positive(self, factory):
        """Test returns computation with positive return"""
        bars = [
            {'close': 100.0},
            {'close': 105.0}
        ]
        returns = factory._compute_returns(bars, periods=1)
        assert abs(returns - 5.0) < 0.01  # 5% return
    
    def test_compute_returns_negative(self, factory):
        """Test returns computation with negative return"""
        bars = [
            {'close': 100.0},
            {'close': 95.0}
        ]
        returns = factory._compute_returns(bars, periods=1)
        assert abs(returns - (-5.0)) < 0.01  # -5% return
    
    def test_compute_returns_zero_price(self, factory):
        """Test returns with zero price (edge case)"""
        bars = [
            {'close': 0.0},
            {'close': 100.0}
        ]
        returns = factory._compute_returns(bars, periods=1)
        assert returns == 0.0  # Should handle gracefully
    
    def test_compute_returns_insufficient_data(self, factory):
        """Test returns with insufficient bars"""
        bars = [{'close': 100.0}]
        returns = factory._compute_returns(bars, periods=1)
        assert returns == 0.0
    
    def test_compute_volatility_valid(self, factory):
        """Test volatility computation"""
        # Create bars with some price variation
        bars = [{'close': 100.0 + i * 0.5} for i in range(30)]
        volatility = factory._compute_volatility(bars, window=20)
        assert volatility > 0.0  # Should have some volatility
        assert not math.isnan(volatility)
        assert not math.isinf(volatility)
    
    def test_compute_volatility_constant_price(self, factory):
        """Test volatility with constant price (no variation)"""
        bars = [{'close': 100.0} for _ in range(30)]
        volatility = factory._compute_volatility(bars, window=20)
        assert volatility == 0.0  # No variation = no volatility
    
    def test_compute_sharpe_ratio_valid(self, factory):
        """Test Sharpe ratio computation"""
        returns = 10.0  # 10% return
        volatility = 5.0  # 5% volatility
        sharpe = factory._compute_sharpe_ratio(returns, volatility)
        assert abs(sharpe - 2.0) < 0.01  # 10/5 = 2.0
    
    def test_compute_sharpe_ratio_zero_volatility(self, factory):
        """Test Sharpe ratio with zero volatility"""
        returns = 10.0
        volatility = 0.0
        sharpe = factory._compute_sharpe_ratio(returns, volatility)
        assert sharpe == 0.0  # Should handle division by zero
    
    def test_compute_sharpe_ratio_handles_nan(self, factory):
        """Test Sharpe ratio handles NaN gracefully"""
        returns = float('nan')
        volatility = 5.0
        sharpe = factory._compute_sharpe_ratio(returns, volatility)
        assert sharpe == 0.0 or not math.isnan(sharpe)  # Should not propagate NaN
    
    def test_compute_skewness_valid(self, factory):
        """Test skewness computation"""
        # Create bars with slight positive skew
        bars = [{'close': 100.0 + np.random.normal(0, 1) + i * 0.1} for i in range(70)]
        skewness = factory._compute_skewness(bars, window=60)
        assert not math.isnan(skewness)
        assert not math.isinf(skewness)
    
    def test_compute_kurtosis_valid(self, factory):
        """Test kurtosis computation"""
        # Create bars with some variation
        bars = [{'close': 100.0 + np.random.normal(0, 1)} for _ in range(70)]
        kurtosis = factory._compute_kurtosis(bars, window=60)
        assert not math.isnan(kurtosis)
        assert not math.isinf(kurtosis)
    
    def test_compute_volume_weighted_momentum(self, factory):
        """Test volume-weighted momentum"""
        bars = [
            {'close': 100.0, 'volume': 1000},
            {'close': 101.0, 'volume': 1500},
            {'close': 102.0, 'volume': 2000},
            {'close': 103.0, 'volume': 1800}
        ]
        momentum = factory._compute_volume_weighted_momentum(bars, window=3)
        assert not math.isnan(momentum)
        assert not math.isinf(momentum)
        assert momentum != 0.0  # Should detect upward momentum
    
    def test_compute_volume_acceleration(self, factory):
        """Test volume acceleration computation"""
        # Increasing volume
        bars = [{'volume': 1000 * (1 + i * 0.1)} for i in range(20)]
        acceleration = factory._compute_volume_acceleration(bars, window=10)
        assert acceleration > 0.0  # Volume is accelerating
        assert not math.isnan(acceleration)
        assert not math.isinf(acceleration)
    
    def test_compute_vwap_deviation(self, factory):
        """Test VWAP deviation computation"""
        bars = [
            {'close': 100.0, 'high': 101.0, 'low': 99.0, 'volume': 1000},
            {'close': 102.0, 'high': 103.0, 'low': 101.0, 'volume': 1500},
            {'close': 104.0, 'high': 105.0, 'low': 103.0, 'volume': 1200}
        ]
        deviation = factory._compute_vwap_deviation(bars, window=3)
        assert not math.isnan(deviation)
        assert not math.isinf(deviation)
    
    def test_compute_vwap_deviation_zero_volume(self, factory):
        """Test VWAP with zero volume"""
        bars = [
            {'close': 100.0, 'high': 101.0, 'low': 99.0, 'volume': 0},
            {'close': 102.0, 'high': 103.0, 'low': 101.0, 'volume': 0}
        ]
        deviation = factory._compute_vwap_deviation(bars, window=2)
        assert deviation == 0.0  # Should handle zero volume gracefully


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
        return MockBus()
    
    @pytest.fixture
    def factory(self, mock_bus):
        """Create FeatureFactory instance"""
        return FeatureFactory(mock_bus)
    
    def test_volatility_protects_against_nan(self, factory):
        """Test volatility handles NaN from numpy"""
        bars = [{'close': float('nan')} for _ in range(30)]
        volatility = factory._compute_volatility(bars, window=20)
        assert volatility == 0.0 or not math.isnan(volatility)
    
    def test_skewness_protects_against_constant_values(self, factory):
        """Test skewness with constant price (zero std)"""
        bars = [{'close': 100.0} for _ in range(70)]
        skewness = factory._compute_skewness(bars, window=60)
        assert skewness == 0.0  # Should handle zero std gracefully
    
    def test_kurtosis_protects_against_constant_values(self, factory):
        """Test kurtosis with constant price (zero std)"""
        bars = [{'close': 100.0} for _ in range(70)]
        kurtosis = factory._compute_kurtosis(bars, window=60)
        assert kurtosis == 0.0  # Should handle zero std gracefully
    
    def test_momentum_handles_zero_volume(self, factory):
        """Test momentum with zero total volume"""
        bars = [
            {'close': 100.0, 'volume': 0},
            {'close': 101.0, 'volume': 0}
        ]
        momentum = factory._compute_volume_weighted_momentum(bars, window=2)
        assert momentum == 0.0  # Should handle zero volume
