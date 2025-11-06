"""
Test validation helpers in orderbook_depth.py
"""

import pytest
import math


def test_safe_probability():
    """Test _safe_probability helper."""
    from engines.features.orderbook_depth import _safe_probability
    
    # Valid probabilities
    assert _safe_probability(0.0) == 0.0
    assert _safe_probability(0.5) == 0.5
    assert _safe_probability(1.0) == 1.0
    
    # Out of range (should clip)
    assert _safe_probability(-0.5) == 0.0
    assert _safe_probability(1.5) == 1.0
    
    # NaN (should return 0.0)
    assert _safe_probability(float('nan')) == 0.0
    
    # Inf (should return 0.0)
    assert _safe_probability(float('inf')) == 0.0
    assert _safe_probability(float('-inf')) == 0.0


def test_safe_imbalance():
    """Test _safe_imbalance helper."""
    from engines.features.orderbook_depth import _safe_imbalance
    
    # Valid imbalances
    assert _safe_imbalance(-1.0) == -1.0
    assert _safe_imbalance(0.0) == 0.0
    assert _safe_imbalance(1.0) == 1.0
    
    # Out of range (should clip)
    assert _safe_imbalance(-2.0) == -1.0
    assert _safe_imbalance(2.0) == 1.0
    
    # NaN (should return 0.0)
    assert _safe_imbalance(float('nan')) == 0.0
    
    # Inf (should return 0.0)
    assert _safe_imbalance(float('inf')) == 0.0
    assert _safe_imbalance(float('-inf')) == 0.0


def test_safe_positive():
    """Test _safe_positive helper."""
    from engines.features.orderbook_depth import _safe_positive
    
    # Valid positive values
    assert _safe_positive(0.0) == 0.0
    assert _safe_positive(10.5) == 10.5
    assert _safe_positive(1000.0) == 1000.0
    
    # Negative (should clip to 0)
    assert _safe_positive(-5.0) == 0.0
    
    # NaN (should return default)
    assert _safe_positive(float('nan')) == 0.0
    assert _safe_positive(float('nan'), default=10.0) == 10.0
    
    # Inf (should return default)
    assert _safe_positive(float('inf')) == 0.0
    assert _safe_positive(float('-inf')) == 0.0
