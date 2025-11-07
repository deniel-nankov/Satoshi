"""
OnChainBuilder - Extract blockchain-native alpha from on-chain transaction flows

This agent analyzes raw Ethereum blockchain events to detect:
- Whale activity: Large (>$1M USD) transfers to/from exchanges
- Exchange flows: CEX inflow/outflow/netflow (Coinbase, Binance, etc.)
- Network activity: Active addresses, transaction count, average sizes
- DeFi activity: DEX volume, TVL changes from LST/LRT protocols

Input:
  - Kafka topic: raw_data.onchain_events (OnchainFlow events from onchain_collector.py)
  - Data: ERC20 transfers, DEX swaps, CEX hot-wallet movements, bridge transfers
  - Chains: Ethereum, Arbitrum, Polygon, Optimism, Base
  - Tokens: USDT, USDC, DAI, WETH, WBTC, stETH, cbETH, sfrxETH

Output:
  - Kafka topic: features.onchain
  - 13 features per symbol per 1-minute window
  - Features: whale flows (3), exchange flows (3), network (3), DeFi (2), metadata (2)

Performance Target: <50ms per symbol per aggregation window

Architecture:
  - DESCRIPTIVE: Measures on-chain flows, does NOT make trading decisions
  - Defensive: Inline validation helpers protect against data quality issues
  - Circuit Breaker: Automatic recovery from consecutive failures
  - 1-minute aggregation windows with reorg-safe finality tracking

On-Chain Data Format (from onchain_collector.py):
  @dataclass
  class OnchainFlow:
      chain: str
      event_type: str  # 'erc20_transfer', 'dex_swap', 'cex_hot_wallet', 'bridge', 'lst', 'lrt'
      tx_hash: str
      block_number: int
      timestamp_utc_us: int
      from_address: str
      to_address: str
      token: Optional[str] = None
      amount: Optional[Decimal] = None
      value_usd: Optional[Decimal] = None  # <-- KEY: USD value for whale detection
      extra: Optional[Dict[str, Any]] = {}
      finalized: bool = False  # Reorg-safe when True
"""

import time
import math
from dataclasses import dataclass
from typing import List, Dict, Optional, Set
from decimal import Decimal
from collections import defaultdict

# Prometheus metrics (centralized)
try:
    from infra.monitoring.prometheus_metrics import MetricsCollector
    _metrics_collector = MetricsCollector()
    METRICS_AVAILABLE = True
except ImportError:
    _metrics_collector = None
    METRICS_AVAILABLE = False


# =============================
# DATACLASS
# =============================

@dataclass
class OnChainFeatures:
    """
    On-chain features extracted from blockchain transaction flows.
    
    Whale Activity (3 features):
      - whale_inflow_usd: USD value of large (>$1M) transfers TO exchanges
      - whale_outflow_usd: USD value of large (>$1M) transfers FROM exchanges
      - whale_netflow_usd: Net whale flow (inflow - outflow)
    
    Exchange Flows (3 features):
      - exchange_inflow_usd: Total USD value flowing TO CEX hot wallets
      - exchange_outflow_usd: Total USD value flowing FROM CEX hot wallets
      - exchange_netflow_usd: Net exchange flow (inflow - outflow)
    
    Network Activity (3 features):
      - active_addresses: Count of unique addresses (from + to) in window
      - transaction_count: Total number of transactions in window
      - avg_transaction_size_usd: Average USD value per transaction
    
    DeFi Activity (2 features):
      - defi_volume_usd: Total DEX swap volume in USD
      - defi_tvl_change_pct: Percentage change in DeFi TVL (LST/LRT staking)
    
    Metadata (2 features):
      - symbol: Asset symbol (e.g. 'WETH', 'USDC')
      - timestamp: Unix timestamp (seconds) of window start
    
    Total: 13 features
    """
    # Metadata
    symbol: str
    timestamp: float
    
    # Whale Activity (3)
    whale_inflow_usd: float
    whale_outflow_usd: float
    whale_netflow_usd: float
    
    # Exchange Flows (3)
    exchange_inflow_usd: float
    exchange_outflow_usd: float
    exchange_netflow_usd: float
    
    # Network Activity (3)
    active_addresses: int
    transaction_count: int
    avg_transaction_size_usd: float
    
    # DeFi Activity (2)
    defi_volume_usd: float
    defi_tvl_change_pct: float


# =============================
# INLINE VALIDATION HELPERS
# =============================

def _safe_usd_amount(value: Optional[float]) -> float:
    """Ensure USD amounts are finite and non-negative."""
    if value is None:
        return 0.0
    
    # Convert Decimal to float if needed
    if isinstance(value, Decimal):
        try:
            value = float(value)
        except (ValueError, OverflowError):
            return 0.0
    
    # Check for NaN/Inf
    if not isinstance(value, (int, float)):
        return 0.0
    if math.isnan(value) or math.isinf(value):
        return 0.0
    
    # Clamp negative to zero (shouldn't happen, but defensive)
    return max(0.0, value)


def _safe_count(value: Optional[int]) -> int:
    """Ensure counts are non-negative integers."""
    if value is None:
        return 0
    if not isinstance(value, int):
        try:
            value = int(value)
        except (ValueError, TypeError):
            return 0
    return max(0, value)


def _safe_percentage(value: Optional[float]) -> float:
    """Ensure percentages are finite (can be negative for TVL decreases)."""
    if value is None:
        return 0.0
    
    # Convert Decimal to float if needed
    if isinstance(value, Decimal):
        try:
            value = float(value)
        except (ValueError, OverflowError):
            return 0.0
    
    # Check for NaN/Inf
    if not isinstance(value, (int, float)):
        return 0.0
    if math.isnan(value) or math.isinf(value):
        return 0.0
    
    # Clamp to reasonable range (-100% to +1000% TVL change)
    return max(-100.0, min(1000.0, value))


# =============================
# ONCHAIN BUILDER
# =============================

class OnChainBuilder:
    """
    Extract blockchain-native alpha from on-chain transaction flows.
    
    Analyzes raw Ethereum events (ERC20 transfers, DEX swaps, CEX movements) to detect:
    - Whale accumulation/distribution patterns
    - Exchange flow dynamics (supply leaving/entering CEX)
    - Network activity levels (addresses, transactions, sizes)
    - DeFi activity (DEX volume, TVL changes)
    
    Input: OnchainFlow events from raw_data.onchain_events Kafka topic
    Output: OnChainFeatures published to features.onchain Kafka topic
    
    Performance: <50ms per symbol per 1-minute aggregation window
    
    Example usage:
        builder = OnChainBuilder(mock_bus)
        features = builder.build_onchain_features(symbol='WETH', events=raw_events)
        # features.whale_netflow_usd > 5_000_000 => Whales buying aggressively
        # features.exchange_netflow_usd < -10_000_000 => Supply leaving CEX (bullish)
        # features.defi_volume_usd > 50_000_000 => High DEX activity
    """
    
    # Whale threshold (from FEATURE.md specification)
    WHALE_THRESHOLD_USD = 1_000_000.0
    
    # CEX hot wallet addresses (from onchain_collector.py)
    # Note: In production, should import from onchain_collector.TransactionClassifier.CEX_HOT_WALLETS
    CEX_HOT_WALLETS = {
        'ethereum': {
            '0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be',  # Binance
            '0xd551234ae421e3bcba99a0da6d736074f22192ff',  # Binance 2
            '0x28c6c06298d514db089934071355e5743bf21d60',  # Binance 14
            '0x21a31ee1afc51d94c2efccaa2092ad1028285549',  # Binance 15
            '0xa910f92acdaf488fa6ef02174fb86208ad7722ba',  # Coinbase 1
            '0x77696bb39917c91a0c3908d577d5e322095425ca',  # Coinbase 2
            '0x503828976d22510aad0201ac7ec88293211d23da',  # Coinbase 3
            '0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740',  # Coinbase 4
        },
        # Add other chains as needed (arbitrum, polygon, optimism, base)
        'arbitrum': set(),
        'polygon': set(),
        'optimism': set(),
        'base': set(),
    }
    
    def __init__(self, streaming_bus):
        """
        Initialize OnChainBuilder with streaming bus for Kafka I/O.
        
        Args:
            streaming_bus: StreamingBus instance for Kafka topic access
        """
        self.streaming_bus = streaming_bus
        
        # Circuit breaker state
        self._circuit_open = False
        self._consecutive_failures = 0
        self._failure_threshold = 5
        self._last_success_time = time.time()
        self._circuit_reset_time = 60.0  # 1 minute
        
        # Previous TVL cache for percentage change calculation
        self._prev_tvl: Dict[str, float] = {}
        
        # Metrics labels
        self._metrics_labels = {'agent': 'onchain_builder'}
    
    def build_onchain_features(self, symbol: str, events: List[Dict]) -> Optional[OnChainFeatures]:
        """
        Build on-chain features from raw blockchain events.
        
        Args:
            symbol: Asset symbol (e.g. 'WETH', 'USDC')
            events: List of OnchainFlow events (as dicts) from raw_data.onchain_events
        
        Returns:
            OnChainFeatures if successful, None if circuit breaker is open
        
        Raises:
            ValueError: If symbol is empty or events list is invalid
        """
        # Input validation
        if not symbol or not isinstance(symbol, str):
            raise ValueError(f"Invalid symbol: {symbol}")
        if events is None:
            raise ValueError("Events list cannot be None")
        
        # Circuit breaker check
        if self._circuit_open:
            if time.time() - self._last_success_time > self._circuit_reset_time:
                # Try to reset circuit
                self._circuit_open = False
                self._consecutive_failures = 0
            else:
                # Circuit still open
                return None
        
        start_time = time.time()
        if METRICS_AVAILABLE and _metrics_collector is not None:
            _metrics_collector.increment_counter(
                "onchain_agent_runs_total",
                value=1.0,
                labels=self._metrics_labels
            )
        
        try:
            # Convert empty list to valid empty features
            if not events:
                return self._empty_features(symbol)
            
            # Compute features
            whale_flows = self._compute_whale_flows(symbol, events)
            exchange_flows = self._compute_exchange_flows(symbol, events)
            network_activity = self._compute_network_activity(symbol, events)
            defi_activity = self._compute_defi_activity(symbol, events)
            
            # Get timestamp from first event
            timestamp = self._extract_timestamp(events)
            
            # Build features
            features = OnChainFeatures(
                symbol=symbol,
                timestamp=timestamp,
                # Whale activity
                whale_inflow_usd=whale_flows['inflow'],
                whale_outflow_usd=whale_flows['outflow'],
                whale_netflow_usd=whale_flows['netflow'],
                # Exchange flows
                exchange_inflow_usd=exchange_flows['inflow'],
                exchange_outflow_usd=exchange_flows['outflow'],
                exchange_netflow_usd=exchange_flows['netflow'],
                # Network activity
                active_addresses=network_activity['active_addresses'],
                transaction_count=network_activity['transaction_count'],
                avg_transaction_size_usd=network_activity['avg_transaction_size_usd'],
                # DeFi activity
                defi_volume_usd=defi_activity['defi_volume_usd'],
                defi_tvl_change_pct=defi_activity['defi_tvl_change_pct'],
            )
            
            # Success - reset circuit breaker
            self._consecutive_failures = 0
            self._last_success_time = time.time()
            
            # Record metrics
            elapsed_ms = (time.time() - start_time) * 1000
            if METRICS_AVAILABLE and _metrics_collector is not None:
                _metrics_collector.observe_histogram(
                    "onchain_agent_processing_time_ms",
                    value=elapsed_ms,
                    labels=self._metrics_labels
                )
            
            return features
            
        except Exception as e:
            # Handle failure
            self._consecutive_failures += 1
            if METRICS_AVAILABLE and _metrics_collector is not None:
                _metrics_collector.increment_counter(
                    "onchain_agent_errors_total",
                    value=1.0,
                    labels=self._metrics_labels
                )
            
            # Open circuit if too many failures
            if self._consecutive_failures >= self._failure_threshold:
                self._circuit_open = True
            
            # Re-raise for caller to handle
            raise
    
    def _empty_features(self, symbol: str) -> OnChainFeatures:
        """Create zero-initialized features for empty event list."""
        return OnChainFeatures(
            symbol=symbol,
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
    
    def _extract_timestamp(self, events: List[Dict]) -> float:
        """Extract Unix timestamp (seconds) from first event."""
        if not events:
            return time.time()
        
        first_event = events[0]
        # OnchainFlow uses timestamp_utc_us (microseconds)
        timestamp_us = first_event.get('timestamp_utc_us', 0)
        if timestamp_us > 0:
            return timestamp_us / 1_000_000.0  # Convert to seconds
        
        # Fallback to current time
        return time.time()
    
    def _compute_whale_flows(self, symbol: str, events: List[Dict]) -> Dict[str, float]:
        """
        Compute whale activity: Large (>$1M USD) transfers to/from exchanges.
        
        Whale Detection:
        - value_usd > WHALE_THRESHOLD_USD ($1M)
        - Inflow: Transfer TO CEX hot wallet (accumulation by whales)
        - Outflow: Transfer FROM CEX hot wallet (distribution by whales)
        - Netflow: Inflow - Outflow (positive = accumulation, negative = distribution)
        
        Args:
            symbol: Asset symbol
            events: OnchainFlow events
        
        Returns:
            Dict with keys: 'inflow', 'outflow', 'netflow'
        """
        whale_inflow = 0.0
        whale_outflow = 0.0
        
        for event in events:
            # Extract USD value
            value_usd = _safe_usd_amount(event.get('value_usd'))
            
            # Check whale threshold
            if value_usd < self.WHALE_THRESHOLD_USD:
                continue
            
            # Extract addresses
            to_addr = (event.get('to_address') or '').lower().strip()
            from_addr = (event.get('from_address') or '').lower().strip()
            chain = (event.get('chain') or 'ethereum').lower()
            
            # Get CEX addresses for this chain
            cex_addresses = self.CEX_HOT_WALLETS.get(chain, set())
            
            # Check direction
            if to_addr in cex_addresses:
                # Whale transfer TO exchange (deposit)
                whale_inflow += value_usd
            
            if from_addr in cex_addresses:
                # Whale transfer FROM exchange (withdrawal)
                whale_outflow += value_usd
        
        # Apply defensive validation
        whale_inflow = _safe_usd_amount(whale_inflow)
        whale_outflow = _safe_usd_amount(whale_outflow)
        whale_netflow = whale_inflow - whale_outflow  # Can be negative
        
        return {
            'inflow': whale_inflow,
            'outflow': whale_outflow,
            'netflow': whale_netflow,
        }
    
    def _compute_exchange_flows(self, symbol: str, events: List[Dict]) -> Dict[str, float]:
        """
        Compute exchange flows: Total USD value flowing to/from CEX hot wallets.
        
        Exchange Flow Detection (all sizes, not just whales):
        - Inflow: Transfer TO CEX hot wallet (supply entering exchanges)
        - Outflow: Transfer FROM CEX hot wallet (supply leaving exchanges)
        - Netflow: Inflow - Outflow (positive = supply increase, negative = supply decrease)
        
        Trading Signal:
        - Negative netflow (outflow > inflow) => Bullish (supply leaving CEX)
        - Positive netflow (inflow > outflow) => Bearish (supply entering CEX for selling)
        
        Args:
            symbol: Asset symbol
            events: OnchainFlow events
        
        Returns:
            Dict with keys: 'inflow', 'outflow', 'netflow'
        """
        exchange_inflow = 0.0
        exchange_outflow = 0.0
        
        for event in events:
            # Extract USD value (all sizes, not just whales)
            value_usd = _safe_usd_amount(event.get('value_usd'))
            if value_usd == 0.0:
                continue
            
            # Extract addresses
            to_addr = (event.get('to_address') or '').lower().strip()
            from_addr = (event.get('from_address') or '').lower().strip()
            chain = (event.get('chain') or 'ethereum').lower()
            
            # Get CEX addresses for this chain
            cex_addresses = self.CEX_HOT_WALLETS.get(chain, set())
            
            # Check direction
            if to_addr in cex_addresses:
                # Transfer TO exchange (inflow)
                exchange_inflow += value_usd
            
            if from_addr in cex_addresses:
                # Transfer FROM exchange (outflow)
                exchange_outflow += value_usd
        
        # Apply defensive validation
        exchange_inflow = _safe_usd_amount(exchange_inflow)
        exchange_outflow = _safe_usd_amount(exchange_outflow)
        exchange_netflow = exchange_inflow - exchange_outflow  # Can be negative
        
        return {
            'inflow': exchange_inflow,
            'outflow': exchange_outflow,
            'netflow': exchange_netflow,
        }
    
    def _compute_network_activity(self, symbol: str, events: List[Dict]) -> Dict:
        """
        Compute network activity: Active addresses, transaction count, average sizes.
        
        Network Activity Metrics:
        - active_addresses: Count of unique addresses (from + to)
        - transaction_count: Total number of transactions
        - avg_transaction_size_usd: Average USD value per transaction
        
        Trading Signal:
        - High active_addresses + high avg_transaction_size => Institutional activity
        - Low transaction_count => Low liquidity (higher slippage risk)
        
        Args:
            symbol: Asset symbol
            events: OnchainFlow events
        
        Returns:
            Dict with keys: 'active_addresses', 'transaction_count', 'avg_transaction_size_usd'
        """
        unique_addresses: Set[str] = set()
        total_value_usd = 0.0
        tx_count = 0
        
        for event in events:
            # Count transaction
            tx_count += 1
            
            # Track unique addresses
            to_addr = (event.get('to_address') or '').lower().strip()
            from_addr = (event.get('from_address') or '').lower().strip()
            
            if to_addr:
                unique_addresses.add(to_addr)
            if from_addr:
                unique_addresses.add(from_addr)
            
            # Accumulate USD value
            value_usd = _safe_usd_amount(event.get('value_usd'))
            total_value_usd += value_usd
        
        # Compute average
        if tx_count > 0:
            avg_size = total_value_usd / tx_count
        else:
            avg_size = 0.0
        
        # Apply defensive validation
        active_addresses = _safe_count(len(unique_addresses))
        transaction_count = _safe_count(tx_count)
        avg_transaction_size_usd = _safe_usd_amount(avg_size)
        
        return {
            'active_addresses': active_addresses,
            'transaction_count': transaction_count,
            'avg_transaction_size_usd': avg_transaction_size_usd,
        }
    
    def _compute_defi_activity(self, symbol: str, events: List[Dict]) -> Dict[str, float]:
        """
        Compute DeFi activity: DEX volume and TVL changes.
        
        DeFi Activity Metrics:
        - defi_volume_usd: Total DEX swap volume in USD
        - defi_tvl_change_pct: Percentage change in DeFi TVL (LST/LRT staking)
        
        Trading Signal:
        - High DEX volume => High on-chain liquidity and trading activity
        - Positive TVL change => Capital flowing into DeFi (risk-on)
        - Negative TVL change => Capital leaving DeFi (risk-off)
        
        Args:
            symbol: Asset symbol
            events: OnchainFlow events
        
        Returns:
            Dict with keys: 'defi_volume_usd', 'defi_tvl_change_pct'
        """
        dex_volume = 0.0
        tvl_delta = 0.0  # Change in TVL this window
        
        for event in events:
            event_type = event.get('event_type', '').lower()
            value_usd = _safe_usd_amount(event.get('value_usd'))
            
            # DEX volume (sum all dex_swap events)
            if event_type == 'dex_swap':
                dex_volume += value_usd
            
            # TVL tracking (LST/LRT staking/unstaking)
            if event_type in {'lst', 'lrt'}:
                # Get staking direction from method name
                extra = event.get('extra') or {}
                method = extra.get('method_name', '').lower()
                
                # Check unstake BEFORE stake (since 'unstake' contains 'stake')
                if 'unstake' in method or 'redeem' in method:
                    # Unstaking removes from TVL
                    tvl_delta -= value_usd
                elif 'stake' in method or 'mint' in method:
                    # Staking adds to TVL
                    tvl_delta += value_usd
        
        # Get previous TVL and compute new TVL
        prev_tvl = self._prev_tvl.get(symbol, 0.0)
        current_tvl = prev_tvl + tvl_delta
        
        # Compute TVL change percentage
        if prev_tvl > 0:
            tvl_change_pct = ((current_tvl - prev_tvl) / prev_tvl) * 100.0
        else:
            tvl_change_pct = 0.0
        
        # Update cache for next iteration
        self._prev_tvl[symbol] = current_tvl
        
        # Apply defensive validation
        defi_volume_usd = _safe_usd_amount(dex_volume)
        defi_tvl_change_pct = _safe_percentage(tvl_change_pct)
        
        return {
            'defi_volume_usd': defi_volume_usd,
            'defi_tvl_change_pct': defi_tvl_change_pct,
        }
