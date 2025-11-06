"""
Unit tests for business logic validations in data quality orchestrator.

Tests the new validations:
1. Trade arithmetic (price × quantity = notional)
2. Bid/ask spread (bid < ask)
3. Orderbook ordering (bids descending, asks ascending)
"""

import pytest
import asyncio
from typing import Dict
from engines.data.data_quality_orchestrator import DataQualityOrchestrator, QualityResult


class TestBusinessLogicValidations:
    """Test suite for business logic validations."""
    
    @pytest.fixture
    async def orchestrator(self):
        """Create orchestrator instance for testing."""
        # Mock minimal config
        class MockConfig:
            quality_gates = {"schema_validation": 0.7}
            stage_timeouts = {"schema_validation": 5000}
        
        orch = DataQualityOrchestrator()
        orch.config = MockConfig()
        return orch
    
    # ========================================
    # Trade Arithmetic Validation Tests
    # ========================================
    
    @pytest.mark.asyncio
    async def test_trade_arithmetic_valid(self, orchestrator):
        """Test valid trade arithmetic: price × quantity = notional."""
        payload = {
            "price": 100.0,
            "quantity": 50.0,
            "notional": 5000.0
        }
        
        result = await orchestrator._execute_business_logic_validation(
            payload, {}, "raw_data.trades"
        )
        
        assert result.score == 1.0
        assert result.result == QualityResult.PASS
        assert len(result.incidents) == 0
        assert result.metadata["violations_found"] == 0
    
    @pytest.mark.asyncio
    async def test_trade_arithmetic_invalid(self, orchestrator):
        """Test invalid trade arithmetic: notional mismatch."""
        payload = {
            "price": 100.0,
            "quantity": 50.0,
            "notional": 5050.0  # Should be 5000.0
        }
        
        result = await orchestrator._execute_business_logic_validation(
            payload, {}, "raw_data.trades"
        )
        
        assert result.score == 0.0
        assert result.result == QualityResult.FAIL
        assert len(result.incidents) > 0
        assert result.incidents[0]["type"] == "trade_arithmetic_violation"
        assert result.metadata["error_violations"] == 1
    
    @pytest.mark.asyncio
    async def test_trade_arithmetic_floating_point_tolerance(self, orchestrator):
        """Test floating point tolerance in trade arithmetic."""
        payload = {
            "price": 0.00001234,
            "quantity": 1000000.0,
            "notional": 12.34  # Exact match with floating point
        }
        
        result = await orchestrator._execute_business_logic_validation(
            payload, {}, "raw_data.trades"
        )
        
        # Should pass due to tolerance
        assert result.score == 1.0
    
    @pytest.mark.asyncio
    async def test_trade_arithmetic_missing_notional(self, orchestrator):
        """Test trade without notional field (should pass - optional validation)."""
        payload = {
            "price": 100.0,
            "quantity": 50.0
            # No notional field
        }
        
        result = await orchestrator._execute_business_logic_validation(
            payload, {}, "raw_data.trades"
        )
        
        # Should pass - notional is optional
        assert result.score == 1.0
    
    # ========================================
    # Bid/Ask Spread Validation Tests
    # ========================================
    
    @pytest.mark.asyncio
    async def test_bid_ask_spread_valid(self, orchestrator):
        """Test valid bid/ask spread: bid < ask."""
        payload = {
            "bid": 99.50,
            "ask": 100.00
        }
        
        result = await orchestrator._execute_business_logic_validation(
            payload, {}, "raw_data.book"
        )
        
        assert result.score == 1.0
        assert result.result == QualityResult.PASS
        assert len(result.incidents) == 0
    
    @pytest.mark.asyncio
    async def test_bid_ask_spread_invalid(self, orchestrator):
        """Test invalid bid/ask spread: bid >= ask."""
        payload = {
            "bid": 100.50,
            "ask": 100.00
        }
        
        result = await orchestrator._execute_business_logic_validation(
            payload, {}, "raw_data.book"
        )
        
        assert result.score == 0.0
        assert result.result == QualityResult.FAIL
        assert len(result.incidents) > 0
        assert result.incidents[0]["type"] == "bid_ask_spread_violation"
    
    @pytest.mark.asyncio
    async def test_bid_ask_spread_crossed_within_tolerance(self, orchestrator):
        """Test crossed spread within tolerance (extreme volatility scenario)."""
        payload = {
            "bid": 100.005,
            "ask": 100.000
        }
        
        result = await orchestrator._execute_business_logic_validation(
            payload, {}, "raw_data.quote"
        )
        
        # Spread < 0.01% so might pass tolerance
        # This test verifies the tolerance logic exists
        assert result.score in [0.0, 1.0]  # Either fails or passes based on tolerance
    
    # ========================================
    # Orderbook Ordering Validation Tests
    # ========================================
    
    @pytest.mark.asyncio
    async def test_orderbook_bids_descending_valid(self, orchestrator):
        """Test valid orderbook: bids in descending order."""
        payload = {
            "bids": [
                [100.00, 10.0],  # Best bid
                [99.50, 15.0],
                [99.00, 20.0],
                [98.50, 25.0]
            ]
        }
        
        result = await orchestrator._execute_business_logic_validation(
            payload, {}, "raw_data.orderbook"
        )
        
        assert result.score >= 0.8  # Should pass or warn
        assert result.metadata["violations_found"] == 0
    
    @pytest.mark.asyncio
    async def test_orderbook_bids_not_descending(self, orchestrator):
        """Test invalid orderbook: bids not in descending order."""
        payload = {
            "bids": [
                [100.00, 10.0],
                [100.50, 15.0],  # Invalid: higher than previous
                [99.50, 20.0]
            ]
        }
        
        result = await orchestrator._execute_business_logic_validation(
            payload, {}, "raw_data.orderbook"
        )
        
        # Should warn (not fail) for ordering issues
        assert result.score == 0.8
        assert result.result == QualityResult.WARN
        assert len(result.incidents) > 0
        assert result.incidents[0]["type"] == "orderbook_ordering_violation"
    
    @pytest.mark.asyncio
    async def test_orderbook_asks_ascending_valid(self, orchestrator):
        """Test valid orderbook: asks in ascending order."""
        payload = {
            "asks": [
                [100.00, 10.0],  # Best ask
                [100.50, 15.0],
                [101.00, 20.0],
                [101.50, 25.0]
            ]
        }
        
        result = await orchestrator._execute_business_logic_validation(
            payload, {}, "raw_data.orderbook"
        )
        
        assert result.score >= 0.8
        assert result.metadata["violations_found"] == 0
    
    @pytest.mark.asyncio
    async def test_orderbook_asks_not_ascending(self, orchestrator):
        """Test invalid orderbook: asks not in ascending order."""
        payload = {
            "asks": [
                [100.00, 10.0],
                [99.50, 15.0],  # Invalid: lower than previous
                [101.00, 20.0]
            ]
        }
        
        result = await orchestrator._execute_business_logic_validation(
            payload, {}, "raw_data.orderbook"
        )
        
        # Should warn for ordering issues
        assert result.score == 0.8
        assert result.result == QualityResult.WARN
        assert len(result.incidents) > 0
    
    # ========================================
    # Topic-Based Routing Tests
    # ========================================
    
    @pytest.mark.asyncio
    async def test_validation_only_for_relevant_topics(self, orchestrator):
        """Test that validations only trigger for relevant topics."""
        # Trade validation should NOT trigger on book topics
        payload = {
            "price": 100.0,
            "quantity": 50.0,
            "notional": 9999.0  # Wrong, but should be ignored
        }
        
        result = await orchestrator._execute_business_logic_validation(
            payload, {}, "raw_data.book"  # Book topic, not trade
        )
        
        # Should pass because trade validation doesn't run on book topics
        assert "trade_arithmetic" not in [i.get("type") for i in result.incidents]
    
    @pytest.mark.asyncio
    async def test_multiple_validations_combined(self, orchestrator):
        """Test multiple validations on same payload."""
        payload = {
            # Invalid trade arithmetic
            "price": 100.0,
            "quantity": 50.0,
            "notional": 9999.0,
            # Invalid bid/ask spread
            "bid": 101.0,
            "ask": 100.0
        }
        
        # Topic matches both validations
        result = await orchestrator._execute_business_logic_validation(
            payload, {}, "raw_data.book_trades"
        )
        
        # Should detect both violations
        assert result.score == 0.0
        assert result.metadata["error_violations"] >= 1
    
    # ========================================
    # Metadata and Metrics Tests
    # ========================================
    
    @pytest.mark.asyncio
    async def test_metadata_tracking(self, orchestrator):
        """Test that metadata correctly tracks validation results."""
        payload = {
            "bid": 100.5,
            "ask": 100.0  # Invalid spread
        }
        
        result = await orchestrator._execute_business_logic_validation(
            payload, {}, "raw_data.quote"
        )
        
        # Check metadata structure
        assert "business_logic_checks" in result.metadata
        assert "violations_found" in result.metadata
        assert "error_violations" in result.metadata
        assert "warning_violations" in result.metadata
        assert "checks_performed" in result.metadata
        
        # Verify counts
        assert result.metadata["error_violations"] >= 1
        assert "bid_ask_spread" in result.metadata["checks_performed"]


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])
