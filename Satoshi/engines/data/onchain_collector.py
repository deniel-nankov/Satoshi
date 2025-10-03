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
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime
import time
import hashlib
from collections import defaultdict, deque
import os

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
        return hashlib.md5(str(h).encode()).hexdigest()

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
        return hashlib.md5(h.encode()).hexdigest()

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
        self.running = False
        self.tasks: List[asyncio.Task] = []
        # Per-chain cursor for flows if enabled
        self._flow_cursors: Dict[str, Any] = {}

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
        logger.info(f"Started {len(self.tasks)} collection tasks")

    async def stop(self):
        logger.info("Stopping Onchain Collector Agent...")
        self.running = False
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        logger.info("Onchain Collector Agent stopped")

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
                            flow = OnchainFlow(
                                chain=chain,
                                event_type=row.get("event_type", "erc20_transfer"),
                                tx_hash=row["tx_hash"],
                                block_number=_safe_int(row.get("block_number", 0)),
                                timestamp_utc_us=_normalize_timestamp(row.get("timestamp"), now),
                                from_address=from_addr.lower(),
                                to_address=to_addr.lower(),
                                token=row.get("token"),
                                amount=_safe_decimal(row.get("amount")),
                                value_usd=_safe_decimal(row.get("value_usd")),
                                extra={k: v for k, v in row.items() if k not in {"chain","event_type","tx_hash","block_number","timestamp","from_address","to_address","token","amount","value_usd"}},
                                capture_timestamp_utc_us=capture_now
                            )
                        except Exception as row_exc:
                            logger.warning(f"Bad row in flows: {row_exc} | {str(row)[:300]}")
                            continue
                        if not self.duplicate_detector.is_duplicate('flows', flow.get_hash(), key=chain):
                            q = self.output_queues['flows']
                            if q.full():
                                try: q.get_nowait()
                                except asyncio.QueueEmpty: pass
                            try: q.put_nowait(flow)
                            except asyncio.QueueFull: pass
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
        'chains': ['ethereum', 'arbitrum'],
        'flows_interval_sec': 10,
        'lst_interval_sec': 60
    }
    logging.basicConfig(level=logging.INFO)
    agent = OnchainCollectorAgent(config)
    try:
        await agent.start()
        while True:
            flow = await agent.get_output_data('flows', timeout=5.0)
            if flow:
                print(f"Flow: {flow.chain} {flow.event_type} {flow.amount} {flow.token}")
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        await agent.stop()

if __name__ == "__main__":
    asyncio.run(main())
