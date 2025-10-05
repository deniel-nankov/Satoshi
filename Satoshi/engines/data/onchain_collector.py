# =============================
# HELPERS
# =============================

def _safe_decimal(val):
    if val is None or val == '' or val == 'None':
        return None
    try:
        return Decimal(str(val))
    except Exception:
        return None

def _safe_int(val):
    try:
        return int(val)
    except Exception:
        return 0

def _normalize_timestamp(ts, default_now_us):
    # Accepts int (ms/s/us), float, or ISO string
    if ts is None:
        return default_now_us
    if isinstance(ts, int):
        # Heuristic: ns >=1e18, us >=1e15, ms >=1e12, s >=1e9 else
        if ts >= 1_000_000_000_000_000_000:
            return ts // 1000  # ns to us
        elif ts >= 1_000_000_000_000_000:
            return ts  # already us
        elif ts >= 1_000_000_000_000:
            return ts * 1000  # ms to us
        elif ts >= 1_000_000_000:
            return ts * 1_000_000  # s to us
        else:
            return ts * 1_000_000  # s to us (fallback)
    if isinstance(ts, float):
        return int(ts * 1_000_000)
    if isinstance(ts, str):
        try:
            # Try ISO string
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            return int(dt.timestamp() * 1_000_000)
        except Exception:
            try:
                return int(ts)
            except Exception:
                return default_now_us
    return default_now_us
"""
On-Chain Collector Agent

Mission: Coarse mempool stats, ERC20 transfers, DEX swaps, CEX hot-wallet flows, LST/LRT state.

Outputs: raw_data.onchain.{flows,lst_state,bridge,queues}.

SLO: chain→bus lag p95 < 30s (coarse!).
"""

import asyncio
import aiohttp
import logging
from typing import List, Dict, Any, Optional, ClassVar, Set
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime
import time
import hashlib
from collections import defaultdict, deque
import os

# Streaming Bus Integration
from infra.bus.streaming_bus import StreamingBus

logger = logging.getLogger(__name__)

# =============================
import os
# DATA STRUCTURES
# =============================

@dataclass
class OnchainFlow:
    chain: str
    event_type: str  # e.g. 'erc20_transfer', 'dex_swap', 'cex_hot_wallet', 'bridge', 'lst', 'lrt'
    tx_hash: str
    block_number: int
    timestamp_utc_us: int
    from_address: str
    to_address: str
    token: Optional[str] = None
    amount: Optional[Decimal] = None
    value_usd: Optional[Decimal] = None
    extra: Optional[Dict[str, Any]] = field(default_factory=lambda: {})
    capture_timestamp_utc_us: int = 0
    # Reorg safety fields
    finalized: bool = False  # True when confirmed beyond reorg_depth
    reorg_depth: int = 0     # How many blocks deep this transaction is
    block_hash: Optional[str] = None  # Block hash for reorg detection

    def get_hash(self) -> str:
        # Prefer log_index or evt_index if present, else fallback to from:to:token:amount
        idx = None
        extra = self.extra or {}
        if 'log_index' in extra:
            idx = extra['log_index']
        elif 'evt_index' in extra:
            idx = extra['evt_index']
        if idx is not None: 
            h = f"{self.chain}:{self.event_type}:{self.tx_hash}:{idx}" 
        else: 
            # Normalize addresses and quantize amount for dedup 
            from_addr = (self.from_address or '').lower() 
            to_addr = (self.to_address or '').lower()
            
            # Token normalization: lowercase, handle contract_address/pool_address aliases
            token = (self.token or '').lower() if self.token else ''
            if not token:
                token = (extra.get('contract_address') or '').lower()
            if not token:
                token = (extra.get('pool_address') or '').lower()
            
            # Amount normalization with token decimals if available
            if self.amount:
                decimals = extra.get('decimals')
                if decimals is not None:
                    try:
                        scale = Decimal(10) ** (-int(decimals))
                        amt = self.amount.quantize(scale)
                    except (ValueError, TypeError):
                        amt = self.amount.quantize(Decimal('0.000000000000000001'))  # 18 decimals
                else:
                    amt = self.amount.quantize(Decimal('0.000000000000000001'))  # 18 decimals
            else:
                amt = Decimal(0)
            
            h = f"{self.chain}:{self.event_type}:{self.tx_hash}:{from_addr}:{to_addr}:{token}:{amt}" 
        return hashlib.sha256(str(h).encode()).hexdigest()

@dataclass
class LSTState:
    chain: str
    protocol: str
    block_number: int
    timestamp_utc_us: int
    total_supply: Decimal
    total_staked: Decimal
    apr: Optional[Decimal] = None
    extra: Optional[Dict[str, Any]] = field(default_factory=lambda: {})

    def get_hash(self) -> str:
        h = f"{self.chain}:{self.protocol}:{self.block_number}:{self.total_supply}"
        return hashlib.sha256(h.encode()).hexdigest()

# =============================
# TRANSACTION CLASSIFIER
# =============================

class TransactionClassifier:
    """Deterministic classification of onchain events into types."""
    
    # Well-known DEX contract addresses (lowercase)
    DEX_CONTRACTS: ClassVar[Dict[str, Set[str]]] = {
        'ethereum': {
            '0x7a250d5630b4cf539739df2c5dacb4c659f2488d',  # Uniswap V2 Router
            '0xe592427a0aece92de3edee1f18e0157c05861564',  # Uniswap V3 Router
            '0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45',  # Uniswap V3 Router 2
            '0x1111111254eeb25477b68fb85ed929f73a960582',  # 1inch V4
            '0x11111112542d85b3ef69ae05771c2dccff4faa26',  # 1inch V3
            '0xdef1c0ded9bec7f1a1670819833240f027b25eff',  # 0x Protocol
        },
        'arbitrum': {
            '0x1b02da8cb0d097eb8d57a175b88c7d8b47997506',  # SushiSwap
            '0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45',  # Uniswap V3 Router 2
        },
        'polygon': {
            '0xa5e0829caced8ffdd4de3c43696c57f7d7a678ff',  # QuickSwap
            '0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45',  # Uniswap V3 Router 2
        }
    }
    
    # Bridge contract patterns
    BRIDGE_CONTRACTS: ClassVar[Dict[str, Set[str]]] = {
        'ethereum': {
            '0xa0c68c638235ee32657e8f720a23cec1bfc77c77',  # Polygon Bridge
            '0x8315177ab297ba92a06054ce80a67ed4dbd7ed3a',  # Arbitrum Bridge
            '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1',  # Optimism Bridge
        }
    }
    
    # CEX hot wallet patterns (known exchange addresses)
    CEX_HOT_WALLETS: ClassVar[Dict[str, Set[str]]] = {
        'ethereum': {
            '0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be',  # Binance
            '0xd551234ae421e3bcba99a0da6d736074f22192ff',  # Binance 2
            '0x28c6c06298d514db089934071355e5743bf21d60',  # Binance 14
            '0x21a31ee1afc51d94c2efccaa2092ad1028285549',  # Binance 15
            '0xa910f92acdaf488fa6ef02174fb86208ad7722ba',  # Coinbase 1
            '0x77696bb39917c91a0c3908d577d5e322095425ca',  # Coinbase 2
            '0x503828976d22510aad0201ac7ec88293211d23da',  # Coinbase 3
            '0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740',  # Coinbase 4
        }
    }
    
    @classmethod
    def classify_event_type(cls, row: dict, chain: str) -> str:
        """Deterministically classify transaction into event type."""
        # Extract addresses
        to_addr = (row.get('to_address') or '').lower().strip()
        from_addr = (row.get('from_address') or '').lower().strip()
        contract_addr = (row.get('contract_address') or '').lower().strip()
        
        # Check for explicit event type first
        explicit_type = row.get('event_type')
        if explicit_type:
            return explicit_type
        
        # Check for method signature or function name
        method = row.get('method_name', '').lower()
        function_sig = row.get('function_signature', '').lower()
        
        # DEX classification
        dex_addrs = cls.DEX_CONTRACTS.get(chain, set())
        if (to_addr in dex_addrs or 
            contract_addr in dex_addrs or
            'swap' in method or 
            'exchange' in method or
            function_sig.startswith('0x38ed1739') or  # swapExactTokensForTokens
            function_sig.startswith('0x7ff36ab5')):   # swapExactETHForTokens
            return 'dex_swap'
        
        # Bridge classification
        bridge_addrs = cls.BRIDGE_CONTRACTS.get(chain, set())
        if (to_addr in bridge_addrs or 
            contract_addr in bridge_addrs or
            'bridge' in method or
            'deposit' in method and 'withdraw' not in method):
            return 'bridge'
        
        # CEX hot wallet classification
        cex_addrs = cls.CEX_HOT_WALLETS.get(chain, set())
        if to_addr in cex_addrs or from_addr in cex_addrs:
            return 'cex_hot_wallet'
        
        # LST/LRT classification
        if ('stake' in method or 'unstake' in method or 
            'mint' in method or 'redeem' in method or
            row.get('protocol', '').lower() in {'lido', 'rocketpool', 'stakewise', 'frax'}):
            return 'lst' if 'liquid' in row.get('protocol', '').lower() else 'lrt'
        
        # Default to ERC20 transfer
        return 'erc20_transfer'

# =============================
# BLOCKCHAIN VALIDATOR
# =============================

class BlockchainValidator:
    """Lightweight validation for onchain data - source-side sanity checks only."""
    
    @staticmethod
    def validate_address(address: str, chain: str) -> bool:
        """Basic address format validation."""
        if not address or not isinstance(address, str):
            return False
        
        # Ethereum-style addresses (40 hex chars with 0x prefix)
        if chain.lower() in {'ethereum', 'arbitrum', 'polygon', 'optimism', 'base'}:
            if len(address) == 42 and address.startswith('0x'):
                try:
                    int(address[2:], 16)  # Validate hex
                    return True
                except ValueError:
                    return False
        
        # Add other chain address formats as needed
        return True  # Conservative: allow unknown formats
    
    @staticmethod
    def validate_amount(amount: Optional[Decimal], decimals: Optional[int] = None) -> tuple[bool, Optional[str]]:
        """Validate amount is non-negative and reasonable."""
        if amount is None:
            return True, None  # Null amounts are OK
        
        if not isinstance(amount, Decimal):
            return False, "amount_not_decimal"
        
        if amount < 0:
            return False, "negative_amount"
        
        # Check for suspiciously large amounts
        if decimals is not None:
            try:
                max_supply = Decimal(10) ** (decimals + 12)  # 1T tokens max
                if amount > max_supply:
                    return False, "amount_too_large"
            except Exception:
                pass
        
        return True, None
    
    @staticmethod
    def validate_block_number(block_number: int, chain: str, latest_known: Optional[int] = None) -> tuple[bool, Optional[str]]:
        """Validate block number is reasonable."""
        if not isinstance(block_number, int) or block_number < 0:
            return False, "invalid_block_number"
        
        # Chain-specific genesis blocks
        genesis_blocks = {
            'ethereum': 0,
            'arbitrum': 0,
            'polygon': 0,
            'optimism': 0
        }
        
        genesis = genesis_blocks.get(chain.lower(), 0)
        if block_number < genesis:
            return False, "block_before_genesis"
        
        # Check against latest known block
        if latest_known and block_number > latest_known + 100:  # Allow some drift
            return False, "block_too_far_ahead"
        
        return True, None
    
    @classmethod
    def validate_flow(cls, flow_data: dict, chain: str, latest_block: Optional[int] = None) -> tuple[bool, List[str]]:
        """Comprehensive flow validation with suspect flags."""
        issues = []
        
        # Address validation
        for addr_field in ['from_address', 'to_address']:
            addr = flow_data.get(addr_field)
            if addr and not cls.validate_address(addr, chain):
                issues.append(f"invalid_{addr_field}")
        
        # Amount validation
        amount = flow_data.get('amount')
        decimals = flow_data.get('decimals')
        if amount is not None:
            amount_valid, amount_issue = cls.validate_amount(amount, decimals)
            if not amount_valid and amount_issue:
                issues.append(amount_issue)
        
        # Block number validation
        block_num = flow_data.get('block_number')
        if block_num is not None:
            block_valid, block_issue = cls.validate_block_number(block_num, chain, latest_block)
            if not block_valid and block_issue:
                issues.append(block_issue)
        
        # Transaction hash validation (basic)
        tx_hash = flow_data.get('tx_hash')
        if tx_hash and (len(tx_hash) != 66 or not tx_hash.startswith('0x')):
            issues.append("invalid_tx_hash")
        
        return len(issues) == 0, issues

# =============================
# DUPLICATE DETECTOR
# =============================

class DuplicateDetector:
    def __init__(self, window_size: int = 10000):
        self.window_size = window_size
        # For each stream (flows/lst/bridge/queues), keep a dict of key -> (deque, set)
        self.seen: Dict[str, Dict[str, tuple]] = defaultdict(dict)

    def is_duplicate(self, data_type: str, data_hash: str, key: str = "default") -> bool:
        # key can be chain, protocol, etc. for per-stream dedup
        if key not in self.seen[data_type]:
            self.seen[data_type][key] = (deque(), set())
        dq, st = self.seen[data_type][key]
        if data_hash in st:
            return True
        dq.append(data_hash)
        st.add(data_hash)
        while len(dq) > self.window_size:
            evicted = dq.popleft()
            st.discard(evicted)
        return False

# =============================
# DUNE API CLIENT
# =============================

class DuneClient:
    """Minimal Dune API client for query execution and result polling."""
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.dune.com/api/v1"
        self.session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        if self.session is None or self.session.closed:
            connector = aiohttp.TCPConnector(limit_per_host=10)
            timeout = aiohttp.ClientTimeout(total=30)
            headers = {
                'User-Agent': 'OnchainCollector/1.0 (aiohttp)'
            }
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers=headers
            )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()

    async def get_cached_results(self, query_id: int, params: dict = {}) -> Optional[dict]:
        # Only use cached results if params is empty
        if params:
            return None
        headers = {"x-dune-api-key": self.api_key}
        url = f"{self.base_url}/query/{query_id}/results"
        assert self.session is not None, "aiohttp session is not initialized"
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with self.session.get(url, headers=headers, timeout=timeout) as resp:
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", "2"))
                    logger.warning(f"Dune cached results 429, sleeping {retry_after}s")
                    await asyncio.sleep(retry_after)
                    return None
                if resp.status == 200:
                    try:
                        result = await resp.json()
                    except Exception:
                        logger.warning(f"Non-JSON response from Dune (cached): {await resp.text()}")
                        return None
                    if result.get("state") == "QUERY_STATE_COMPLETED":
                        return result["result"]
        except Exception as e:
            logger.warning(f"Dune cached results error: {e}")
        return None

    async def run_query(self, query_id: int, params: dict = {}) -> dict:
        headers = {"x-dune-api-key": self.api_key}
        url = f"{self.base_url}/query/{query_id}/execute"
        # Try cached results first
        cached = await self.get_cached_results(query_id, params)
        if cached:
            return cached
        # Otherwise, execute
        assert self.session is not None, "aiohttp session is not initialized"
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with self.session.post(url, headers=headers, json={"parameters": params}, timeout=timeout) as resp:
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", "2"))
                    logger.warning(f"Dune execute 429, sleeping {retry_after}s")
                    await asyncio.sleep(retry_after)
                    return {"rows": []}
                if resp.status >= 400:
                    body = await resp.text()
                    logger.error({"event":"dune_http_error","status":resp.status,"body":body[:300]})
                    return {"rows": []}
                try:
                    resp.raise_for_status()
                except aiohttp.ClientResponseError as e:
                    raise
                try:
                    data = await resp.json()
                except Exception:
                    logger.warning(f"Non-JSON response from Dune (execute): {await resp.text()}")
                    return {"rows": []}
                execution_id = data["execution_id"]
        except Exception as e:
            logger.warning(f"Dune execute error: {e}")
            return {"rows": []}
        # Poll for result, fast at first, then slower, max 20s
        poll_intervals = [0.5]*4 + [1.0]*8 + [2.0]*4  # ~20s
        total_wait = 0
        for interval in poll_intervals:
            await asyncio.sleep(interval)
            total_wait += interval
            result_url = f"{self.base_url}/execution/{execution_id}/results"
            assert self.session is not None, "aiohttp session is not initialized"
            try:
                timeout = aiohttp.ClientTimeout(total=10)
                async with self.session.get(result_url, headers=headers, timeout=timeout) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("Retry-After", "2"))
                        await asyncio.sleep(retry_after)
                    elif resp.status >= 400:
                        body = await resp.text()
                        logger.error({"event":"dune_http_error","status":resp.status,"body":body[:300]})
                        return {"rows": []}
                    elif resp.status == 200:
                        try:
                            result = await resp.json()
                        except Exception:
                            logger.warning(f"Non-JSON response from Dune (poll): {await resp.text()}")
                            continue
                        if result.get("state") == "QUERY_STATE_COMPLETED":
                            return result["result"]
            except Exception as e:
                logger.warning(f"Dune poll error: {e}")
            if total_wait > 20:
                break
        # If not ready, return empty result for this tick
        return {"rows": []}

# =============================
# MAIN ONCHAIN COLLECTOR AGENT
# =============================

class OnchainCollectorAgent:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.duplicate_detector = DuplicateDetector()
        self.output_queues: Dict[str, asyncio.Queue] = {
            'flows': asyncio.Queue(maxsize=config.get('flows_queue_size', 10000)),
            'lst_state': asyncio.Queue(maxsize=config.get('lst_queue_size', 1000)),
            'bridge': asyncio.Queue(maxsize=config.get('bridge_queue_size', 1000)),
            'queues': asyncio.Queue(maxsize=config.get('queues_queue_size', 1000)),
        }
        
        # Streaming Bus Integration
        streaming_config = self.config.get("streaming_bus", {
            "bootstrap_servers": "localhost:9092",
            "enable_ssl": False,
            "enable_sasl": False
        })
        self.streaming_bus = StreamingBus(streaming_config)
        
        self.running = False
        self.tasks: List[asyncio.Task] = []
        # Per-chain cursor for flows if enabled
        self._flow_cursors: Dict[str, Any] = {}
        
        # Enhanced Chain State Management
        self.chain_configs = self._setup_chain_configs(config)
        self._pending_flows: Dict[str, deque] = defaultdict(lambda: deque())  # chain -> deque of (block_num, flows)
        self._latest_block_numbers: Dict[str, int] = {}  # chain -> latest seen block number
        self._block_hashes: Dict[str, Dict[int, str]] = defaultdict(dict)  # chain -> {block_num: block_hash}
        self._finality_windows: Dict[str, int] = {}  # chain -> confirmed finality depth
        self._cursor_state: Dict[str, Dict[str, Any]] = defaultdict(dict)  # chain -> {cursor_type: value}
        
        # Initialize validators and classifiers
        self.validator = BlockchainValidator()
        self.classifier = TransactionClassifier()

    def _setup_chain_configs(self, config: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Setup chain-specific configurations."""
        chain_configs = {}
        
        # Default configurations per chain
        defaults = {
            'ethereum': {
                'reorg_depth': 12,
                'finality_blocks': 32,  # ETH 2.0 finality
                'block_time_ms': 12000,
                'max_cursor_lag_blocks': 100
            },
            'arbitrum': {
                'reorg_depth': 1,  # Much faster finality
                'finality_blocks': 10,
                'block_time_ms': 250,
                'max_cursor_lag_blocks': 400
            },
            'polygon': {
                'reorg_depth': 256,  # Polygon checkpoint system
                'finality_blocks': 256,
                'block_time_ms': 2000,
                'max_cursor_lag_blocks': 500
            },
            'optimism': {
                'reorg_depth': 1,
                'finality_blocks': 12,
                'block_time_ms': 2000,
                'max_cursor_lag_blocks': 300
            }
        }
        
        # Apply chain-specific overrides
        for chain in config.get('chains', []):
            chain_config = defaults.get(chain.lower(), defaults['ethereum']).copy()
            
            # Override with user config
            chain_overrides = config.get('chain_overrides', {}).get(chain, {})
            chain_config.update(chain_overrides)
            
            # Global reorg_depth override for backward compatibility
            if 'reorg_depth' in config:
                chain_config['reorg_depth'] = config['reorg_depth']
            
            chain_configs[chain] = chain_config
            
        return chain_configs
    
    def _get_chain_config(self, chain: str, key: str, default: Any = None) -> Any:
        """Get chain-specific configuration value."""
        return self.chain_configs.get(chain, {}).get(key, default)

    async def start(self):
        logger.info("Starting Onchain Collector Agent...")
        self.running = True
        # Start all collection tasks
        for chain in self.config.get('chains', []):
            self.tasks.append(asyncio.create_task(self._collect_flows(chain)))
            self.tasks.append(asyncio.create_task(self._collect_lst_state(chain)))
            if self.config.get('bridge_query_id'):
                self.tasks.append(asyncio.create_task(self._collect_bridge(chain)))
            if self.config.get('queues_query_id'):
                self.tasks.append(asyncio.create_task(self._collect_queues(chain)))
            # Start reorg finalization task
            self.tasks.append(asyncio.create_task(self._finalize_reorg_safe_flows(chain)))
        logger.info(f"Started {len(self.tasks)} collection tasks")

    async def stop(self):
        logger.info("Stopping Onchain Collector Agent...")
        self.running = False
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        logger.info("Onchain Collector Agent stopped")

    async def _get_latest_block_number(self, chain: str) -> Optional[int]:
        """Get the latest block number from the chain."""
        try:
            # Use a simple RPC call to get latest block
            rpc_url = self.config.get('rpc_urls', {}).get(chain)
            if not rpc_url:
                logger.warning(f"No RPC URL configured for chain {chain}")
                return None
                
            async with aiohttp.ClientSession() as session:
                payload = {
                    "jsonrpc": "2.0",
                    "method": "eth_blockNumber",
                    "params": [],
                    "id": 1
                }
                async with session.post(rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if 'result' in data:
                            return int(data['result'], 16)  # Convert hex to int
        except Exception as e:
            logger.warning(f"Failed to get latest block for {chain}: {e}")
        return None

    async def _finalize_reorg_safe_flows(self, chain: str):
        """Process pending flows and finalize those beyond reorg depth."""
        interval_sec = self.config.get('reorg_finalization_interval_sec', 30)
        next_tick = time.monotonic()
        
        while self.running:
            try:
                # Get current chain tip
                latest_block = await self._get_latest_block_number(chain)
                if latest_block is not None:
                    self._latest_block_numbers[chain] = latest_block
                    
                    # Process pending flows for this chain
                    pending = self._pending_flows[chain]
                    finalized_flows = []
                    
                    # Get chain-specific reorg depth
                    reorg_depth = self._get_chain_config(chain, 'reorg_depth', 12)
                    
                    # Check all pending flows to see if they're now finalized
                    while pending and pending[0][0] <= latest_block - reorg_depth:
                        block_num, flows_in_block = pending.popleft()
                        
                        # Mark all flows in this block as finalized
                        for flow in flows_in_block:
                            flow.finalized = True
                            flow.reorg_depth = latest_block - flow.block_number
                            finalized_flows.append(flow)
                    
                    # Publish finalized flows
                    for flow in finalized_flows:
                        await self._publish_finalized_flow(flow, chain)
                        
                    if finalized_flows:
                        logger.debug(f"Finalized {len(finalized_flows)} flows for {chain} "
                                   f"(latest_block={latest_block}, reorg_depth={reorg_depth})")
                        
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in reorg finalization for {chain}: {e}")
            
            # Drift-free cadence
            next_tick += interval_sec
            sleep_for = max(0, next_tick - time.monotonic())
            await asyncio.sleep(sleep_for)

    async def _publish_finalized_flow(self, flow: OnchainFlow, chain: str):
        """Publish a finalized flow to the streaming bus with comprehensive headers."""
        try:
            flow_data = {
                "chain": chain,
                "data_type": "flows",
                "timestamp": flow.timestamp_utc_us,
                "capture_timestamp": flow.capture_timestamp_utc_us,
                "block_number": flow.block_number,
                "tx_hash": flow.tx_hash,
                "event_type": flow.event_type,
                "from_address": flow.from_address,
                "to_address": flow.to_address,
                "amount": str(flow.amount) if flow.amount else None,
                "token": flow.token,
                "value_usd": str(flow.value_usd) if flow.value_usd else None,
                "extra": flow.extra,
                # Reorg safety fields
                "finalized": flow.finalized,
                "reorg_depth": flow.reorg_depth,
                "block_hash": flow.block_hash
            }
            
            # Use chain+block as partition key for blockchain ordering
            partition_key = f"{chain}_{flow.block_number or 0}"
            log_index = flow.extra.get('log_index', 0) if flow.extra else 0
            dedupe_key = f"{chain}_{flow.tx_hash}_{log_index}"
            
            # Enhanced headers with validation and classification info
            headers = {
                "data_type": "flows", 
                "chain": chain,
                "event_type": flow.event_type,
                "finalized": str(flow.finalized).lower(),
                "reorg_depth": str(flow.reorg_depth),
                "block_number": str(flow.block_number),
                "collector_version": "enhanced_v1"
            }
            
            # Add validation flags to headers
            if flow.extra:
                if flow.extra.get('suspect_data'):
                    headers["suspect_data"] = "true"
                    headers["validation_issues"] = ','.join(flow.extra.get('validation_issues', []))
                
                if flow.extra.get('log_index') is not None:
                    headers["log_index"] = str(flow.extra['log_index'])
            
            # Add chain-specific metadata
            chain_config = self.chain_configs.get(chain, {})
            if chain_config:
                headers["chain_reorg_depth"] = str(chain_config.get('reorg_depth', 12))
                headers["chain_finality_blocks"] = str(chain_config.get('finality_blocks', 32))
            
            await self.streaming_bus.publish_with_headers(
                topic="raw_data.onchain_events",
                partition_key=partition_key,
                payload=flow_data,
                headers=headers,
                dedupe_key=dedupe_key
            )
            
            # Local queue fallback with proper error handling
            try:
                q = self.output_queues['flows']
                if q.full():
                    try: 
                        q.get_nowait()
                        logger.debug("Dropped old flow from full queue")
                    except asyncio.QueueEmpty: 
                        pass
                q.put_nowait(flow)
                logger.debug("Enqueued flow to local queue")
            except asyncio.QueueFull:
                logger.warning("Failed to enqueue flow - queue full")
            except Exception as e:
                logger.warning(f"Failed to enqueue flow to local queue: {e}")
            
        except Exception as e:
            logger.warning(f"Failed to publish finalized flow to streaming bus: {e}")

    async def _add_flow_to_pending_buffer(self, flow: OnchainFlow, chain: str):
        """Add a flow to the pending buffer for reorg safety with enhanced validation."""
        try:
            # Check if reorg safety is disabled
            if self.config.get('disable_reorg_safety', False):
                # Publish immediately without reorg safety
                flow.finalized = True
                flow.reorg_depth = 0
                await self._publish_finalized_flow(flow, chain)
                return
            
            # Validate block hash consistency for reorg detection
            block_num = flow.block_number
            if flow.block_hash:
                stored_hash = self._block_hashes[chain].get(block_num)
                if stored_hash and stored_hash != flow.block_hash:
                    logger.warning(f"Reorg detected: {chain} block {block_num} "
                                 f"hash changed from {stored_hash[:10]}... to {flow.block_hash[:10]}...")
                    # Remove conflicting flows from pending buffer
                    await self._handle_reorg(chain, block_num)
                
                # Update block hash
                self._block_hashes[chain][block_num] = flow.block_hash
            
            pending = self._pending_flows[chain]
            
            # Find the right place to insert this flow (keep blocks sorted)
            inserted = False
            for i, (existing_block_num, flows_in_block) in enumerate(pending):
                if existing_block_num == block_num:
                    # Add to existing block
                    flows_in_block.append(flow)
                    inserted = True
                    break
                elif existing_block_num > block_num:
                    # Insert new block before this one
                    pending.insert(i, (block_num, [flow]))
                    inserted = True
                    break
            
            if not inserted:
                # Add to the end
                pending.append((block_num, [flow]))
            
            # Update reorg depth for all flows
            latest_block = self._latest_block_numbers.get(chain, block_num)
            flow.reorg_depth = max(0, latest_block - block_num)
            
            # Maintain reasonable buffer size
            max_buffer_blocks = self._get_chain_config(chain, 'max_buffer_blocks', 1000)
            while len(pending) > max_buffer_blocks:
                # Force finalize oldest blocks if buffer too large
                old_block_num, old_flows = pending.popleft()
                logger.warning(f"Force finalizing old block {old_block_num} due to buffer overflow")
                for old_flow in old_flows:
                    old_flow.finalized = True
                    old_flow.reorg_depth = latest_block - old_flow.block_number
                    await self._publish_finalized_flow(old_flow, chain)
            
            logger.debug(f"Added flow to pending buffer: {chain} block {block_num}, "
                        f"pending blocks: {len(pending)}")
            
        except Exception as e:
            logger.warning(f"Failed to add flow to pending buffer: {e}")

    async def _handle_reorg(self, chain: str, reorg_block: int):
        """Handle blockchain reorganization by removing affected flows."""
        try:
            pending = self._pending_flows[chain]
            
            # Remove all flows from reorg_block and higher
            filtered_pending = deque()
            removed_count = 0
            
            for block_num, flows_in_block in pending:
                if block_num < reorg_block:
                    filtered_pending.append((block_num, flows_in_block))
                else:
                    removed_count += len(flows_in_block)
            
            self._pending_flows[chain] = filtered_pending
            
            # Clean up block hashes from reorg_block onwards
            for block_num in list(self._block_hashes[chain].keys()):
                if block_num >= reorg_block:
                    del self._block_hashes[chain][block_num]
            
            logger.info(f"Handled reorg on {chain}: removed {removed_count} flows from block {reorg_block}+")
            
        except Exception as e:
            logger.error(f"Failed to handle reorg on {chain}: {e}")

    async def _collect_flows(self, chain: str):
        interval_sec = self.config.get('flows_interval_sec', 10)
        dune_api_key = self.config.get('dune_api_key') or os.environ.get('DUNE_API_KEY')
        dune_query_id = self.config.get('dune_query_id')
        cursor_enabled = self.config.get('flows_cursor_enabled', False)
        cursor_param = self.config.get('flows_cursor_param', None)  # e.g. 'min_block' or 'min_ts'
        cursor_unit = self.config.get('flows_cursor_unit', 'us')  # 'us', 'ms', or 's'
        if not dune_api_key or not dune_query_id:
            logger.warning("Dune API key or query ID not set; skipping Dune flows collection.")
            await asyncio.sleep(interval_sec)
            return
        async with DuneClient(dune_api_key) as dune:
            next_tick = time.monotonic()
            while self.running:
                try:
                    params = {"chain": chain}
                    # If cursoring is enabled and we have a cursor, add it to params
                    if cursor_enabled and cursor_param:
                        last_cursor = self._flow_cursors.get(chain)
                        if last_cursor is not None:
                            # Convert cursor to correct unit for Dune
                            send_cursor = last_cursor
                            if cursor_param == 'min_ts':
                                if cursor_unit == 'us':
                                    send_cursor = last_cursor
                                elif cursor_unit == 'ms':
                                    send_cursor = last_cursor // 1000
                                elif cursor_unit == 's':
                                    send_cursor = last_cursor // 1_000_000
                            params[cursor_param] = send_cursor
                    result = await dune.run_query(dune_query_id, params=params)
                    now = int(time.time() * 1_000_000)
                    capture_now = int(time.time() * 1_000_000)
                    max_cursor = None
                    for row in result.get("rows", []):
                        from_addr = row.get("from_address", "")
                        to_addr = row.get("to_address", "")
                        try:
                            # Classify event type using our deterministic classifier
                            classified_event_type = self.classifier.classify_event_type(row, chain)
                            
                            # Extract block_hash if available
                            block_hash = row.get("block_hash")
                            
                            # Validate data before creating flow
                            latest_block = self._latest_block_numbers.get(chain)
                            is_valid, validation_issues = self.validator.validate_flow(row, chain, latest_block)
                            
                            flow = OnchainFlow(
                                chain=chain,
                                event_type=classified_event_type,  # Use classified type
                                tx_hash=row["tx_hash"],
                                block_number=_safe_int(row.get("block_number", 0)),
                                timestamp_utc_us=_normalize_timestamp(row.get("timestamp"), now),
                                from_address=from_addr.lower(),
                                to_address=to_addr.lower(),
                                token=row.get("token"),
                                amount=_safe_decimal(row.get("amount")),
                                value_usd=_safe_decimal(row.get("value_usd")),
                                extra={k: v for k, v in row.items() if k not in {"chain","event_type","tx_hash","block_number","timestamp","from_address","to_address","token","amount","value_usd","block_hash"}},
                                capture_timestamp_utc_us=capture_now,
                                # Reorg safety fields
                                finalized=False,
                                reorg_depth=0,
                                block_hash=block_hash
                            )
                            
                            # Add validation metadata to extra if there are issues
                            if not is_valid:
                                flow.extra = flow.extra or {}
                                flow.extra['validation_issues'] = validation_issues
                                flow.extra['suspect_data'] = True
                                logger.debug(f"Flow validation issues for {chain} {flow.tx_hash}: {validation_issues}")
                            
                        except Exception as row_exc:
                            logger.warning(f"Bad row in flows: {row_exc} | {str(row)[:300]}")
                            continue
                            
                        if not self.duplicate_detector.is_duplicate('flows', flow.get_hash(), key=chain):
                            # Add to pending buffer for reorg safety instead of immediate publishing
                            await self._add_flow_to_pending_buffer(flow, chain)
                        # Track max cursor value in this page
                        if cursor_enabled and cursor_param:
                            # Use block_number or timestamp as cursor, depending on param
                            if cursor_param == 'min_block':
                                val = _safe_int(row.get('block_number', 0))
                            elif cursor_param == 'min_ts':
                                val = _normalize_timestamp(row.get('timestamp'), now)
                            else:
                                val = None
                            if val is not None:
                                if max_cursor is None or val > max_cursor:
                                    max_cursor = val
                    # Only advance cursor if we got a successful page
                    if cursor_enabled and cursor_param and max_cursor is not None:
                        inc = 1 if cursor_unit == 'us' else (1_000 if cursor_unit == 'ms' else 1_000_000)
                        self._flow_cursors[chain] = max_cursor + inc
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Error collecting flows from Dune: {e}")
                # Drift-free cadence
                next_tick += interval_sec
                sleep_for = max(0, next_tick - time.monotonic())
                await asyncio.sleep(sleep_for)

    async def _collect_lst_state(self, chain: str):
        interval_sec = self.config.get('lst_interval_sec', 60)
        dune_api_key = self.config.get('dune_api_key') or os.environ.get('DUNE_API_KEY')
        lst_query_id = self.config.get('lst_query_id')
        if not dune_api_key or not lst_query_id:
            logger.warning("Dune API key or LST query ID not set; skipping Dune LST collection.")
            await asyncio.sleep(interval_sec)
            return
        async with DuneClient(dune_api_key) as dune:
            next_tick = time.monotonic()
            while self.running:
                try:
                    params = {"chain": chain}
                    result = await dune.run_query(lst_query_id, params=params)
                    now = int(time.time() * 1_000_000)
                    for row in result.get("rows", []):
                        try:
                            lst = LSTState(
                                chain=chain,
                                protocol=row.get("protocol", "unknown"),
                                block_number=_safe_int(row.get("block_number", 0)),
                                timestamp_utc_us=_normalize_timestamp(row.get("timestamp"), now),
                                total_supply=_safe_decimal(row.get("total_supply")) or Decimal(0),
                                total_staked=_safe_decimal(row.get("total_staked")) or Decimal(0),
                                apr=_safe_decimal(row.get("apr")),
                                extra={k: v for k, v in row.items() if k not in {"chain","protocol","block_number","timestamp","total_supply","total_staked","apr"}}
                            )
                        except Exception as row_exc:
                            logger.warning(f"Bad row in LST: {row_exc} | {str(row)[:300]}")
                            continue
                        if not self.duplicate_detector.is_duplicate('lst_state', lst.get_hash(), key=chain):
                            q = self.output_queues['lst_state']
                            if q.full():
                                try: q.get_nowait()
                                except asyncio.QueueEmpty: pass
                            try: q.put_nowait(lst)
                            except asyncio.QueueFull: pass
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Error collecting LST state from Dune: {e}")
                next_tick += interval_sec
                sleep_for = max(0, next_tick - time.monotonic())
                await asyncio.sleep(sleep_for)
    async def _collect_bridge(self, chain: str):
        interval_sec = self.config.get('bridge_interval_sec', 60)
        dune_api_key = self.config.get('dune_api_key') or os.environ.get('DUNE_API_KEY')
        bridge_query_id = self.config.get('bridge_query_id')
        if not dune_api_key or not bridge_query_id:
            logger.warning("Dune API key or bridge query ID not set; skipping Dune bridge collection.")
            await asyncio.sleep(interval_sec)
            return
        async with DuneClient(dune_api_key) as dune:
            next_tick = time.monotonic()
            while self.running:
                try:
                    params = {"chain": chain}
                    result = await dune.run_query(bridge_query_id, params=params)
                    now = int(time.time() * 1_000_000)
                    capture_now = int(time.time() * 1_000_000)
                    for row in result.get("rows", []):
                        try:
                            bridge_event = OnchainFlow(
                                chain=chain,
                                event_type='bridge',
                                tx_hash=row.get("tx_hash", ""),
                                block_number=_safe_int(row.get("block_number", 0)),
                                timestamp_utc_us=_normalize_timestamp(row.get("timestamp"), now),
                                from_address=row.get("from_address", "").lower(),
                                to_address=row.get("to_address", "").lower(),
                                token=row.get("token"),
                                amount=_safe_decimal(row.get("amount")),
                                value_usd=_safe_decimal(row.get("value_usd")),
                                extra={k: v for k, v in row.items() if k not in {"chain","event_type","tx_hash","block_number","timestamp","from_address","to_address","token","amount","value_usd"}},
                                capture_timestamp_utc_us=capture_now
                            )
                        except Exception as row_exc:
                            logger.warning(f"Bad row in bridge: {row_exc} | {str(row)[:300]}")
                            continue
                        if not self.duplicate_detector.is_duplicate('bridge', bridge_event.get_hash(), key=chain):
                            q = self.output_queues['bridge']
                            if q.full():
                                try: q.get_nowait()
                                except asyncio.QueueEmpty: pass
                            try: q.put_nowait(bridge_event)
                            except asyncio.QueueFull: pass
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Error collecting bridge events from Dune: {e}")
                next_tick += interval_sec
                sleep_for = max(0, next_tick - time.monotonic())
                await asyncio.sleep(sleep_for)

    async def _collect_queues(self, chain: str):
        interval_sec = self.config.get('queues_interval_sec', 60)
        dune_api_key = self.config.get('dune_api_key') or os.environ.get('DUNE_API_KEY')
        queues_query_id = self.config.get('queues_query_id')
        if not dune_api_key or not queues_query_id:
            logger.warning("Dune API key or queues query ID not set; skipping Dune queues collection.")
            await asyncio.sleep(interval_sec)
            return
        async with DuneClient(dune_api_key) as dune:
            next_tick = time.monotonic()
            while self.running:
                try:
                    params = {"chain": chain}
                    result = await dune.run_query(queues_query_id, params=params)
                    now = int(time.time() * 1_000_000)
                    capture_now = int(time.time() * 1_000_000)
                    for row in result.get("rows", []):
                        try:
                            queue_event = OnchainFlow(
                                chain=chain,
                                event_type='queue',
                                tx_hash=row.get("tx_hash", ""),
                                block_number=_safe_int(row.get("block_number", 0)),
                                timestamp_utc_us=_normalize_timestamp(row.get("timestamp"), now),
                                from_address=row.get("from_address", "").lower(),
                                to_address=row.get("to_address", "").lower(),
                                token=row.get("token"),
                                amount=_safe_decimal(row.get("amount")),
                                value_usd=_safe_decimal(row.get("value_usd")),
                                extra={k: v for k, v in row.items() if k not in {"chain","event_type","tx_hash","block_number","timestamp","from_address","to_address","token","amount","value_usd"}},
                                capture_timestamp_utc_us=capture_now
                            )
                        except Exception as row_exc:
                            logger.warning(f"Bad row in queues: {row_exc} | {str(row)[:300]}")
                            continue
                        if not self.duplicate_detector.is_duplicate('queues', queue_event.get_hash(), key=chain):
                            q = self.output_queues['queues']
                            if q.full():
                                try: q.get_nowait()
                                except asyncio.QueueEmpty: pass
                            try: q.put_nowait(queue_event)
                            except asyncio.QueueFull: pass
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Error collecting queue events from Dune: {e}")
                next_tick += interval_sec
                sleep_for = max(0, next_tick - time.monotonic())
                await asyncio.sleep(sleep_for)

    async def get_output_data(self, data_type: str, timeout: float = 1.0) -> Optional[Any]:
        try:
            return await asyncio.wait_for(self.output_queues[data_type].get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

# =============================
# EXAMPLE USAGE
# =============================

async def main():
    config = {
        'chains': ['ethereum', 'arbitrum', 'polygon'],
        'flows_interval_sec': 10,
        'lst_interval_sec': 60,
        
        # Enhanced Chain State Management
        'chain_overrides': {
            'ethereum': {
                'reorg_depth': 12,
                'finality_blocks': 32,
                'max_buffer_blocks': 1000,
                'max_cursor_lag_blocks': 100
            },
            'arbitrum': {
                'reorg_depth': 1,
                'finality_blocks': 10,
                'max_buffer_blocks': 2000,
                'max_cursor_lag_blocks': 400
            },
            'polygon': {
                'reorg_depth': 256,
                'finality_blocks': 256,
                'max_buffer_blocks': 500,
                'max_cursor_lag_blocks': 500
            }
        },
        
        # Reorg safety configuration
        'reorg_finalization_interval_sec': 30,
        'disable_reorg_safety': False,  # Set to True for testing
        
        # Enhanced cursor management
        'flows_cursor_enabled': True,
        'flows_cursor_param': 'min_block',
        'flows_cursor_unit': 'us',
        
        # RPC URLs for getting latest block numbers
        'rpc_urls': {
            'ethereum': 'https://eth-mainnet.g.alchemy.com/v2/YOUR_API_KEY',
            'arbitrum': 'https://arb-mainnet.g.alchemy.com/v2/YOUR_API_KEY',
            'polygon': 'https://polygon-mainnet.g.alchemy.com/v2/YOUR_API_KEY'
        },
        
        # Queue sizes
        'flows_queue_size': 20000,  # Increased for higher throughput
        'lst_queue_size': 2000,
        'bridge_queue_size': 2000,
        'queues_queue_size': 2000,
        
        # Dune API configuration
        'dune_query_id': 1234567,  # Your Dune query ID for flows
        'lst_query_id': 1234568,   # Your Dune query ID for LST state
        'bridge_query_id': 1234569, # Your Dune query ID for bridge events
        'queues_query_id': 1234570  # Your Dune query ID for queue events
    }
    logging.basicConfig(level=logging.INFO)
    agent = OnchainCollectorAgent(config)
    try:
        await agent.start()
        while True:
            flow = await agent.get_output_data('flows', timeout=5.0)
            if flow:
                finalized_status = "✓" if flow.finalized else "⏳"
                validation_status = "⚠️" if (flow.extra and flow.extra.get('suspect_data')) else "✓"
                print(f"Flow: {finalized_status}{validation_status} {flow.chain} {flow.event_type} "
                      f"{flow.amount} {flow.token} (block {flow.block_number}, depth {flow.reorg_depth})")
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        await agent.stop()

if __name__ == "__main__":
    asyncio.run(main())
