"""
Comprehensive tests for OnChainBuilder feature extraction agent.

Test Coverage:
- OnChainFeatures dataclass structure (13 features)
- Whale flow detection (>$1M threshold)
- Exchange flow tracking (CEX inflow/outflow)
- Network activity metrics (addresses, tx count, avg size)
- DeFi activity (DEX volume, TVL changes)
- Defensive validation (NaN, Inf, edge cases)
- Circuit breaker functionality
- Cache management (TVL tracking)
- Empty/invalid inputs

Total: 40 tests
"""

import pytest
import time
from decimal import Decimal
from typing import Dict, List

from engines.features.onchain_builder import (
    OnChainBuilder,
    OnChainFeatures,
    _safe_usd_amount,
    _safe_count,
    _safe_percentage,
)


# =============================
# TEST FIXTURES
# =============================

class MockStreamingBus:
    """Mock streaming bus for testing."""
    def __init__(self):
        self.published = []
    
    def publish(self, topic: str, data: dict):
        self.published.append((topic, data))


# =============================
# TEST ONCHAIN FEATURES DATACLASS
# =============================

class TestOnChainFeatures:
    """Test OnChainFeatures dataclass structure and initialization."""
    
    def test_features_structure(self):
        """Test OnChainFeatures has all 13 expected features."""
        features = OnChainFeatures(
            symbol='WETH',
            timestamp=1234567890.0,
            whale_inflow_usd=5_000_000.0,
            whale_outflow_usd=2_000_000.0,
            whale_netflow_usd=3_000_000.0,
            exchange_inflow_usd=10_000_000.0,
            exchange_outflow_usd=8_000_000.0,
            exchange_netflow_usd=2_000_000.0,
            active_addresses=500,
            transaction_count=1000,
            avg_transaction_size_usd=50_000.0,
            defi_volume_usd=25_000_000.0,
            defi_tvl_change_pct=5.5,
        )
        
        # Metadata
        assert features.symbol == 'WETH'
        assert features.timestamp == 1234567890.0
        
        # Whale activity (3)
        assert features.whale_inflow_usd == 5_000_000.0
        assert features.whale_outflow_usd == 2_000_000.0
        assert features.whale_netflow_usd == 3_000_000.0
        
        # Exchange flows (3)
        assert features.exchange_inflow_usd == 10_000_000.0
        assert features.exchange_outflow_usd == 8_000_000.0
        assert features.exchange_netflow_usd == 2_000_000.0
        
        # Network activity (3)
        assert features.active_addresses == 500
        assert features.transaction_count == 1000
        assert features.avg_transaction_size_usd == 50_000.0
        
        # DeFi activity (2)
        assert features.defi_volume_usd == 25_000_000.0
        assert features.defi_tvl_change_pct == 5.5
    
    def test_features_types(self):
        """Test OnChainFeatures field types are correct."""
        features = OnChainFeatures(
            symbol='USDC',
            timestamp=time.time(),
            whale_inflow_usd=0.0,
            whale_outflow_usd=0.0,
            whale_netflow_usd=0.0,
            exchange_inflow_usd=0.0,
            exchange_outflow_usd=0.0,
            exchange_netflow_usd=0.0,
            active_addresses=0,
            transaction_count=0,
            avg_transaction_size_usd=0.0,
            defi_volume_usd=0.0,
            defi_tvl_change_pct=0.0,
        )
        
        assert isinstance(features.symbol, str)
        assert isinstance(features.timestamp, float)
        assert isinstance(features.whale_inflow_usd, float)
        assert isinstance(features.active_addresses, int)
        assert isinstance(features.transaction_count, int)


# =============================
# TEST DEFENSIVE VALIDATION
# =============================

class TestDefensiveValidation:
    """Test inline validation helpers."""
    
    def test_safe_usd_amount_valid(self):
        """Test _safe_usd_amount with valid inputs."""
        assert _safe_usd_amount(100.0) == 100.0
        assert _safe_usd_amount(0.0) == 0.0
        assert _safe_usd_amount(1_000_000.0) == 1_000_000.0
    
    def test_safe_usd_amount_none(self):
        """Test _safe_usd_amount with None."""
        assert _safe_usd_amount(None) == 0.0
    
    def test_safe_usd_amount_nan(self):
        """Test _safe_usd_amount with NaN."""
        assert _safe_usd_amount(float('nan')) == 0.0
    
    def test_safe_usd_amount_inf(self):
        """Test _safe_usd_amount with Inf."""
        assert _safe_usd_amount(float('inf')) == 0.0
        assert _safe_usd_amount(float('-inf')) == 0.0
    
    def test_safe_usd_amount_negative(self):
        """Test _safe_usd_amount clamps negative to zero."""
        assert _safe_usd_amount(-100.0) == 0.0
    
    def test_safe_usd_amount_decimal(self):
        """Test _safe_usd_amount converts Decimal."""
        assert _safe_usd_amount(Decimal('123.45')) == 123.45
    
    def test_safe_count_valid(self):
        """Test _safe_count with valid inputs."""
        assert _safe_count(10) == 10
        assert _safe_count(0) == 0
        assert _safe_count(1000) == 1000
    
    def test_safe_count_none(self):
        """Test _safe_count with None."""
        assert _safe_count(None) == 0
    
    def test_safe_count_negative(self):
        """Test _safe_count clamps negative to zero."""
        assert _safe_count(-5) == 0
    
    def test_safe_count_float_conversion(self):
        """Test _safe_count converts float to int."""
        assert _safe_count(10.9) == 10
    
    def test_safe_percentage_valid(self):
        """Test _safe_percentage with valid inputs."""
        assert _safe_percentage(5.0) == 5.0
        assert _safe_percentage(-10.0) == -10.0
        assert _safe_percentage(0.0) == 0.0
    
    def test_safe_percentage_none(self):
        """Test _safe_percentage with None."""
        assert _safe_percentage(None) == 0.0
    
    def test_safe_percentage_nan(self):
        """Test _safe_percentage with NaN."""
        assert _safe_percentage(float('nan')) == 0.0
    
    def test_safe_percentage_inf(self):
        """Test _safe_percentage with Inf."""
        assert _safe_percentage(float('inf')) == 0.0
    
    def test_safe_percentage_clamped(self):
        """Test _safe_percentage clamps to reasonable range."""
        assert _safe_percentage(2000.0) == 1000.0  # Max +1000%
        assert _safe_percentage(-200.0) == -100.0  # Min -100%


# =============================
# TEST ONCHAIN BUILDER
# =============================

class TestOnChainBuilder:
    """Test OnChainBuilder core functionality."""
    
    @pytest.fixture
    def mock_bus(self):
        """Create mock streaming bus."""
        return MockStreamingBus()
    
    @pytest.fixture
    def builder(self, mock_bus):
        """Create OnChainBuilder instance."""
        return OnChainBuilder(mock_bus)
    
    def test_initialization(self, builder):
        """Test OnChainBuilder initializes correctly."""
        assert builder.streaming_bus is not None
        assert builder._circuit_open is False
        assert builder._consecutive_failures == 0
        assert builder.WHALE_THRESHOLD_USD == 1_000_000.0
    
    def test_empty_events(self, builder):
        """Test build_onchain_features with empty event list."""
        features = builder.build_onchain_features('WETH', [])
        
        assert features is not None
        assert features.symbol == 'WETH'
        assert features.whale_inflow_usd == 0.0
        assert features.exchange_inflow_usd == 0.0
        assert features.active_addresses == 0
        assert features.transaction_count == 0
        assert features.defi_volume_usd == 0.0
    
    def test_invalid_symbol(self, builder):
        """Test build_onchain_features rejects invalid symbol."""
        with pytest.raises(ValueError, match="Invalid symbol"):
            builder.build_onchain_features('', [])
        
        with pytest.raises(ValueError, match="Invalid symbol"):
            builder.build_onchain_features(None, [])
    
    def test_invalid_events(self, builder):
        """Test build_onchain_features rejects None events."""
        with pytest.raises(ValueError, match="Events list cannot be None"):
            builder.build_onchain_features('WETH', None)
    
    def test_whale_detection_above_threshold(self, builder):
        """Test whale detection for transfers >$1M."""
        events = [
            {
                'chain': 'ethereum',
                'event_type': 'erc20_transfer',
                'tx_hash': '0xabc123',
                'from_address': '0x1234567890abcdef',
                'to_address': '0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be',  # Binance
                'value_usd': 5_000_000.0,
                'timestamp_utc_us': 1234567890000000,
            }
        ]
        
        features = builder.build_onchain_features('WETH', events)
        
        assert features.whale_inflow_usd == 5_000_000.0
        assert features.whale_outflow_usd == 0.0
        assert features.whale_netflow_usd == 5_000_000.0
    
    def test_whale_detection_below_threshold(self, builder):
        """Test whale detection ignores transfers <$1M."""
        events = [
            {
                'chain': 'ethereum',
                'event_type': 'erc20_transfer',
                'tx_hash': '0xabc123',
                'from_address': '0x1234567890abcdef',
                'to_address': '0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be',  # Binance
                'value_usd': 500_000.0,  # Below $1M threshold
                'timestamp_utc_us': 1234567890000000,
            }
        ]
        
        features = builder.build_onchain_features('WETH', events)
        
        assert features.whale_inflow_usd == 0.0
        assert features.whale_outflow_usd == 0.0
        assert features.whale_netflow_usd == 0.0
    
    def test_whale_outflow(self, builder):
        """Test whale detection for withdrawals from CEX."""
        events = [
            {
                'chain': 'ethereum',
                'event_type': 'erc20_transfer',
                'tx_hash': '0xabc123',
                'from_address': '0xa910f92acdaf488fa6ef02174fb86208ad7722ba',  # Coinbase
                'to_address': '0x9876543210fedcba',
                'value_usd': 2_000_000.0,
                'timestamp_utc_us': 1234567890000000,
            }
        ]
        
        features = builder.build_onchain_features('WETH', events)
        
        assert features.whale_inflow_usd == 0.0
        assert features.whale_outflow_usd == 2_000_000.0
        assert features.whale_netflow_usd == -2_000_000.0
    
    def test_whale_bidirectional(self, builder):
        """Test whale detection with both inflows and outflows."""
        events = [
            {
                'chain': 'ethereum',
                'from_address': '0xuser1',
                'to_address': '0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be',  # Binance
                'value_usd': 3_000_000.0,
                'timestamp_utc_us': 1234567890000000,
            },
            {
                'chain': 'ethereum',
                'from_address': '0xa910f92acdaf488fa6ef02174fb86208ad7722ba',  # Coinbase
                'to_address': '0xuser2',
                'value_usd': 1_500_000.0,
                'timestamp_utc_us': 1234567890000000,
            }
        ]
        
        features = builder.build_onchain_features('WETH', events)
        
        assert features.whale_inflow_usd == 3_000_000.0
        assert features.whale_outflow_usd == 1_500_000.0
        assert features.whale_netflow_usd == 1_500_000.0
    
    def test_exchange_flows_all_sizes(self, builder):
        """Test exchange flows track all transfers (not just whales)."""
        events = [
            {
                'chain': 'ethereum',
                'from_address': '0xuser1',
                'to_address': '0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be',  # Binance
                'value_usd': 100_000.0,  # Below whale threshold
                'timestamp_utc_us': 1234567890000000,
            },
            {
                'chain': 'ethereum',
                'from_address': '0xuser2',
                'to_address': '0xa910f92acdaf488fa6ef02174fb86208ad7722ba',  # Coinbase
                'value_usd': 50_000.0,
                'timestamp_utc_us': 1234567890000000,
            }
        ]
        
        features = builder.build_onchain_features('USDC', events)
        
        assert features.exchange_inflow_usd == 150_000.0
        assert features.exchange_outflow_usd == 0.0
        assert features.exchange_netflow_usd == 150_000.0
    
    def test_exchange_outflow(self, builder):
        """Test exchange outflow detection."""
        events = [
            {
                'chain': 'ethereum',
                'from_address': '0x77696bb39917c91a0c3908d577d5e322095425ca',  # Coinbase 2
                'to_address': '0xuser1',
                'value_usd': 200_000.0,
                'timestamp_utc_us': 1234567890000000,
            }
        ]
        
        features = builder.build_onchain_features('WETH', events)
        
        assert features.exchange_inflow_usd == 0.0
        assert features.exchange_outflow_usd == 200_000.0
        assert features.exchange_netflow_usd == -200_000.0
    
    def test_network_activity_unique_addresses(self, builder):
        """Test active_addresses counts unique from/to addresses."""
        events = [
            {
                'chain': 'ethereum',
                'from_address': '0xaaa',
                'to_address': '0xbbb',
                'value_usd': 1000.0,
                'timestamp_utc_us': 1234567890000000,
            },
            {
                'chain': 'ethereum',
                'from_address': '0xaaa',  # Same as before
                'to_address': '0xccc',
                'value_usd': 2000.0,
                'timestamp_utc_us': 1234567890000000,
            },
            {
                'chain': 'ethereum',
                'from_address': '0xddd',
                'to_address': '0xbbb',  # Same as before
                'value_usd': 3000.0,
                'timestamp_utc_us': 1234567890000000,
            }
        ]
        
        features = builder.build_onchain_features('USDC', events)
        
        # Unique addresses: 0xaaa, 0xbbb, 0xccc, 0xddd = 4
        assert features.active_addresses == 4
        assert features.transaction_count == 3
    
    def test_network_activity_average_size(self, builder):
        """Test avg_transaction_size_usd calculation."""
        events = [
            {
                'chain': 'ethereum',
                'from_address': '0xaaa',
                'to_address': '0xbbb',
                'value_usd': 100.0,
                'timestamp_utc_us': 1234567890000000,
            },
            {
                'chain': 'ethereum',
                'from_address': '0xccc',
                'to_address': '0xddd',
                'value_usd': 200.0,
                'timestamp_utc_us': 1234567890000000,
            },
            {
                'chain': 'ethereum',
                'from_address': '0xeee',
                'to_address': '0xfff',
                'value_usd': 300.0,
                'timestamp_utc_us': 1234567890000000,
            }
        ]
        
        features = builder.build_onchain_features('WETH', events)
        
        # Average: (100 + 200 + 300) / 3 = 200
        assert features.avg_transaction_size_usd == 200.0
    
    def test_network_activity_zero_division(self, builder):
        """Test avg_transaction_size_usd with no transactions."""
        features = builder.build_onchain_features('WETH', [])
        
        assert features.avg_transaction_size_usd == 0.0
    
    def test_defi_dex_volume(self, builder):
        """Test DEX volume aggregation."""
        events = [
            {
                'chain': 'ethereum',
                'event_type': 'dex_swap',
                'from_address': '0xaaa',
                'to_address': '0xbbb',
                'value_usd': 10_000.0,
                'timestamp_utc_us': 1234567890000000,
            },
            {
                'chain': 'ethereum',
                'event_type': 'dex_swap',
                'from_address': '0xccc',
                'to_address': '0xddd',
                'value_usd': 15_000.0,
                'timestamp_utc_us': 1234567890000000,
            },
            {
                'chain': 'ethereum',
                'event_type': 'erc20_transfer',  # Not a DEX swap
                'from_address': '0xeee',
                'to_address': '0xfff',
                'value_usd': 5_000.0,
                'timestamp_utc_us': 1234567890000000,
            }
        ]
        
        features = builder.build_onchain_features('WETH', events)
        
        # Only dex_swap events: 10,000 + 15,000 = 25,000
        assert features.defi_volume_usd == 25_000.0
    
    def test_defi_tvl_change_staking(self, builder):
        """Test TVL change from staking events."""
        # First call - establish baseline
        events1 = [
            {
                'chain': 'ethereum',
                'event_type': 'lst',
                'value_usd': 1_000_000.0,
                'extra': {'method_name': 'stake'},
                'timestamp_utc_us': 1234567890000000,
            }
        ]
        features1 = builder.build_onchain_features('stETH', events1)
        
        # TVL change should be 0 on first call (no previous TVL)
        assert features1.defi_tvl_change_pct == 0.0
        
        # Second call - additional staking
        # prev_tvl = 1M, tvl_delta = +200k, current_tvl = 1.2M
        # change = ((1.2M - 1M) / 1M) * 100 = 20%
        events2 = [
            {
                'chain': 'ethereum',
                'event_type': 'lst',
                'value_usd': 200_000.0,  # Additional stake
                'extra': {'method_name': 'stake'},
                'timestamp_utc_us': 1234567900000000,
            }
        ]
        features2 = builder.build_onchain_features('stETH', events2)
        
        # TVL change: ((1,200,000 - 1,000,000) / 1,000,000) * 100 = 20%
        assert features2.defi_tvl_change_pct == 20.0
    
    def test_defi_tvl_change_unstaking(self, builder):
        """Test TVL change from unstaking events."""
        # Establish baseline
        events1 = [
            {
                'chain': 'ethereum',
                'event_type': 'lst',
                'value_usd': 1_000_000.0,
                'extra': {'method_name': 'stake'},
                'timestamp_utc_us': 1234567890000000,
            }
        ]
        builder.build_onchain_features('stETH', events1)
        
        # Unstaking reduces TVL
        # prev_tvl = 1M, tvl_delta = -200k, current_tvl = 800k
        # change = ((800k - 1M) / 1M) * 100 = -20%
        events2 = [
            {
                'chain': 'ethereum',
                'event_type': 'lst',
                'value_usd': 200_000.0,
                'extra': {'method_name': 'unstake'},
                'timestamp_utc_us': 1234567900000000,
            }
        ]
        features2 = builder.build_onchain_features('stETH', events2)
        
        # Current TVL: 1,000,000 - 200,000 = 800,000
        # Change: ((800,000 - 1,000,000) / 1,000,000) * 100 = -20%
        assert features2.defi_tvl_change_pct == -20.0
    
    def test_circuit_breaker_opens(self, builder):
        """Test circuit breaker opens after consecutive failures."""
        # Cause 5 consecutive failures
        for i in range(5):
            try:
                builder.build_onchain_features('WETH', {'invalid': 'data'})
            except:
                pass
        
        assert builder._circuit_open is True
        assert builder._consecutive_failures == 5
    
    def test_circuit_breaker_returns_none(self, builder):
        """Test circuit breaker returns None when open."""
        # Open circuit
        builder._circuit_open = True
        
        features = builder.build_onchain_features('WETH', [])
        
        assert features is None
    
    def test_circuit_breaker_resets(self, builder):
        """Test circuit breaker resets after timeout."""
        # Open circuit
        builder._circuit_open = True
        builder._last_success_time = time.time() - 120.0  # 2 minutes ago
        
        # Should reset and process
        features = builder.build_onchain_features('WETH', [])
        
        assert features is not None
        assert builder._circuit_open is False
        assert builder._consecutive_failures == 0
    
    def test_timestamp_extraction(self, builder):
        """Test timestamp extraction from events."""
        events = [
            {
                'chain': 'ethereum',
                'timestamp_utc_us': 1234567890123456,  # Microseconds
                'value_usd': 1000.0,
            }
        ]
        
        features = builder.build_onchain_features('WETH', events)
        
        # Should convert to seconds: 1234567890.123456
        assert abs(features.timestamp - 1234567890.123456) < 0.01
    
    def test_multiple_chains(self, builder):
        """Test handling events from multiple chains."""
        # Add CEX addresses for arbitrum
        builder.CEX_HOT_WALLETS['arbitrum'] = {'0xarbitrum_cex'}
        
        events = [
            {
                'chain': 'ethereum',
                'from_address': '0xuser',
                'to_address': '0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be',  # Binance
                'value_usd': 2_000_000.0,
                'timestamp_utc_us': 1234567890000000,
            },
            {
                'chain': 'arbitrum',
                'from_address': '0xuser',
                'to_address': '0xarbitrum_cex',
                'value_usd': 1_000_000.0,
                'timestamp_utc_us': 1234567890000000,
            }
        ]
        
        features = builder.build_onchain_features('USDC', events)
        
        # Both whales detected (chain-specific CEX mapping)
        assert features.whale_inflow_usd == 3_000_000.0
