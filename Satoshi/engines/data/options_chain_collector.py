"""
Options Chain Collector Agent

Mission: Pull BTC/ETH chains (IV/Greeks) at 15–60s cadence.
Owns: Surface grid completeness, tenor/strike normalization.
Outputs: raw_data.options.surface
"""

import asyncio
import aiohttp
import time
import logging
import random
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Optional, List, Dict, Any, Set
from datetime import datetime, timezone
from enum import Enum
import hashlib
from collections import deque
from dataclasses import dataclass

# =============================
# SCHEMAS & DATA STRUCTURES
# =============================

class OptionType(Enum):
    CALL = "call"
    PUT = "put"

@dataclass
class OptionSurfacePoint:
    symbol: str
    venue: str
    timestamp_utc_us: int  # Capture time
    expiry: int  # UTC microseconds
    strike: Decimal
    option_type: OptionType
    iv: Optional[Decimal]
    delta: Optional[Decimal]
    gamma: Optional[Decimal]
    vega: Optional[Decimal]
    theta: Optional[Decimal]
    rho: Optional[Decimal]
    mark_price: Optional[Decimal]
    underlying_price: Optional[Decimal]
    volume: Optional[Decimal] = None
    open_interest: Optional[Decimal] = None
    venue_timestamp_utc_us: Optional[int] = None  # If available from venue
    def get_hash(self) -> str:
        # Use option_type.value for stable, venue-agnostic identity
        h = f"{self.venue}:{self.symbol}:{self.expiry}:{self.strike}:{self.option_type.value}:{self.iv}:{self.mark_price}:{self.delta}:{self.gamma}:{self.vega}:{self.theta}:{self.rho}:{self.open_interest}:{self.volume}:{self.underlying_price}"
        return hashlib.md5(h.encode()).hexdigest()

# =============================
# DEDUPLICATION & METRICS
# =============================

class DuplicateDetector:
    def __init__(self, window_size: int = 10000):
        self.window_size = window_size
        self.seen_hashes: deque = deque(maxlen=window_size)
        self.seen_set: Set[str] = set()
    def is_duplicate(self, data_hash: str) -> bool:
        if data_hash in self.seen_set:
            return True
        if len(self.seen_hashes) == self.window_size:
            old = self.seen_hashes.popleft()
            self.seen_set.discard(old)
        self.seen_hashes.append(data_hash)
        self.seen_set.add(data_hash)
        return False

class MetricsCollector:
    def __init__(self):
        self.success = 0
        self.errors = 0
        self.duplicates = 0
        self.start_time = time.time()
    def record_success(self):
        self.success += 1
    def record_error(self):
        self.errors += 1
    def record_duplicate(self):
        self.duplicates += 1
    def uptime_pct(self):
        total = self.success + self.errors
        return 100.0 if total == 0 else (self.success / total) * 100

# =============================
# VENUE ADAPTERS (e.g., Deribit)
# =============================

class OptionsVenueAdapter(ABC):
    def __init__(self, venue: str, config: Dict[str, Any]):
        self.venue = venue
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self.logger = logging.getLogger(f"{__name__}.{venue}")
        self.supports_cursor = False  # Default: no cursor support
    async def start(self):
        # User-Agent and connection cap
        headers = {"User-Agent": self.config.get("user_agent", "OptionsChainCollector/1.0")}
        connector = aiohttp.TCPConnector(limit_per_host=self.config.get("conn_limit_per_host", 10))
        timeout = aiohttp.ClientTimeout(total=self.config.get("session_timeout", 15))
        self.session = aiohttp.ClientSession(headers=headers, connector=connector, timeout=timeout)
    async def stop(self):
        if self.session:
            await self.session.close()
    @abstractmethod
    async def fetch_surface(self, symbol: str, since: Optional[int] = None) -> List[OptionSurfacePoint]:
        pass

class DeribitOptionsAdapter(OptionsVenueAdapter):
    BASE_URL = "https://www.deribit.com/api/v2"
    def __init__(self, venue: str, config: Dict[str, Any]):
        super().__init__(venue, config)
        self.supports_cursor = False  # Deribit does not support since/cursor
    async def fetch_surface(self, symbol: str, since: Optional[int] = None) -> List[OptionSurfacePoint]:
        # symbol: "BTC" or "ETH"; since: last processed event time (us)
        if self.session is None:
            raise RuntimeError("aiohttp ClientSession is not initialized. Call 'start()' before 'fetch_surface()'.")
        url = f"{self.BASE_URL}/public/get_book_summary_by_currency"
        params = {"currency": symbol, "kind": "option"}
        timeout = aiohttp.ClientTimeout(total=self.config.get("request_timeout", 7))
        try:
            async with self.session.get(url, params=params, timeout=timeout) as resp:
                # Explicit status handling
                if resp.status in (429, 418):
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after:
                        await asyncio.sleep(float(retry_after))
                    else:
                        await asyncio.sleep(1 + 2 * random.random())
                    return []
                elif not (200 <= resp.status < 300):
                    # Log status and body, don't try to parse as JSON; truncate to 300 chars
                    body = await resp.text()
                    self.logger.error({"event": "surface_http_error", "status": resp.status, "body": body[:300]})
                    return []
                # Optionally soft-throttle if venue weight header present and config says to check
                if self.config.get("check_weight_header", False):
                    weight = resp.headers.get("X-Request-Weight")
                    try:
                        weight_val = int(weight) if weight is not None else 0
                    except Exception:
                        weight_val = 0
                    if weight_val > self.config.get("weight_cap", 100):
                        await asyncio.sleep(1)
                try:
                    data = await resp.json()
                except Exception:
                    body = await resp.text()
                    self.logger.error({"event": "json_parse_error", "body": body[:500]})
                    return []
                now_us = int(time.time() * 1_000_000)
                surface = []
                drop_expired = self.config.get("drop_expired", False)
                now_ts = int(time.time() * 1_000_000)
                for o in data.get("result", []):
                    try:
                        instr_name = o.get("instrument_name", "")
                        instr = instr_name.split("-")
                        if len(instr) != 4:
                            raise ValueError("Malformed instrument_name")
                        # Expiry parsing must be UTC
                        expiry = int(datetime.strptime(instr[1], "%d%b%y").replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
                        if drop_expired and expiry < now_ts:
                            continue
                        strike = Decimal(instr[2])
                        # Option type case-hardening
                        opt_type = instr[3].upper()
                        if opt_type == "C":
                            option_type = OptionType.CALL
                        elif opt_type == "P":
                            option_type = OptionType.PUT
                        else:
                            raise ValueError(f"Unknown option type: {instr[3]}")
                        # Deribit creation_timestamp is ms, convert to us
                        venue_ts = int(o["creation_timestamp"]) * 1000 if "creation_timestamp" in o else None
                        # Use explicit is not None for all numeric fields, with underlying fallback
                        def dec_field(field, fallback=None):
                            v = o.get(field)
                            if v is not None:
                                return Decimal(str(v))
                            if fallback is not None:
                                v2 = o.get(fallback)
                                return Decimal(str(v2)) if v2 is not None else None
                            return None
                        # IV normalization: prefer iv, else mark_iv, bid_iv, ask_iv
                        iv_val = o.get("iv")
                        if iv_val is None:
                            for alt_iv in ("mark_iv", "bid_iv", "ask_iv"):
                                iv_val = o.get(alt_iv)
                                if iv_val is not None:
                                    break
                        iv = Decimal(str(iv_val)) if iv_val is not None else None
                        # Underlying price fallback: use index_price if underlying_price missing
                        underlying_price = dec_field("underlying_price", fallback="index_price")
                        pt = OptionSurfacePoint(
                            symbol,
                            "deribit",
                            now_us,
                            expiry,
                            strike,
                            option_type,
                            iv,
                            dec_field("delta"),
                            dec_field("gamma"),
                            dec_field("vega"),
                            dec_field("theta"),
                            dec_field("rho"),
                            dec_field("mark_price"),
                            underlying_price,
                            dec_field("volume"),
                            dec_field("open_interest"),
                            venue_ts
                        )
                        # For this endpoint, ignore since (creation_timestamp is not an event time)
                        surface.append(pt)
                    except Exception as ex:
                        # Only log instrument_name and error, not full row
                        self.logger.error({"event": "instrument_parse_error", "instrument_name": o.get("instrument_name", ""), "error": str(ex)})
                        continue
                return surface
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger.error({"event": "fetch_surface_error", "error": str(e)})
            return []

# =============================
# MAIN AGENT
# =============================

class OptionsChainCollectorAgent:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.adapters: Dict[str, OptionsVenueAdapter] = {}
        self.duplicate_detector = DuplicateDetector()
        self.metrics = MetricsCollector()
        self.running = False
        self.tasks: List[asyncio.Task] = []
        output_queue_maxsize = self.config.get("output_queue_maxsize", 10000)
        self.output_queue: asyncio.Queue = asyncio.Queue(maxsize=output_queue_maxsize)
        self._setup_adapters()
        self.logger = logging.getLogger(__name__)
        self._last_surface_time: Dict[str, int] = {}  # For inclusive pagination per symbol/venue
        self._dropped_count = 0
        # For per-stream deadman
        self._last_capture_map: Dict[str, float] = {}  # key: f"{venue}:{symbol}", value: last capture time (monotonic)
    def _setup_adapters(self):
        for venue_cfg in self.config.get("venues", []):
            venue = venue_cfg["name"]
            if venue == "deribit":
                self.adapters[venue] = DeribitOptionsAdapter(venue, venue_cfg)
            # Add more venues here
    async def start(self):
        self.running = True
        for adapter in self.adapters.values():
            await adapter.start()
        for venue, adapter in self.adapters.items():
            for symbol in self.config.get("symbols", []):
                task = asyncio.create_task(self._collect_surface(adapter, symbol, venue))
                self.tasks.append(task)
        self.logger.info(f"Started {len(self.tasks)} surface collection tasks")
        # Optionally: start server-time skew probe
        if self.config.get("enable_skew_probe", True):
            for venue, adapter in self.adapters.items():
                task = asyncio.create_task(self._probe_server_time(adapter, venue))
                self.tasks.append(task)
    async def stop(self):
        self.running = False
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        for adapter in self.adapters.values():
            await adapter.stop()
    async def _collect_surface(self, adapter: OptionsVenueAdapter, symbol: str, venue: str):
        interval = self.config.get("surface_interval_sec", 30)
        key = f"{venue}:{symbol}"
        next_ts = time.monotonic()
        last_cursor = None
        warn_drop_threshold = self.config.get("warn_drop_threshold", 100)
        last_warned_drop = 0
        while self.running:
            now_monotonic = time.monotonic()
            # Per-stream deadman: log if now - last_capture > 3×interval
            last_capture = self._last_capture_map.get(key)
            if last_capture is not None and (now_monotonic - last_capture) > (3 * interval):
                self.logger.info({
                    "event": "surface_deadman",
                    "venue": venue,
                    "symbol": symbol,
                    "since_sec": round(now_monotonic - last_capture, 1),
                    "interval_sec": interval
                })
            try:
                surface = await adapter.fetch_surface(symbol, since=last_cursor)
                max_venue_ts = last_cursor
                got_data = False
                for pt in surface:
                    h = pt.get_hash()
                    if self.duplicate_detector.is_duplicate(h):
                        self.metrics.record_duplicate()
                        continue
                    try:
                        if self.output_queue.full():
                            try:
                                self.output_queue.get_nowait()
                                self._dropped_count += 1
                            except asyncio.QueueEmpty:
                                pass
                        self.output_queue.put_nowait(pt)
                        self.metrics.record_success()
                        got_data = True
                        # Log a warning if drops grow fast
                        if self._dropped_count - last_warned_drop >= warn_drop_threshold:
                            self.logger.warning({
                                "event": "surface_output_queue_drops",
                                "venue": venue,
                                "symbol": symbol,
                                "dropped_count": self._dropped_count
                            })
                            last_warned_drop = self._dropped_count
                    except asyncio.QueueFull:
                        self.logger.warning("Surface output queue full, dropping oldest.")
                        self._dropped_count += 1
                    # Track max event time for inclusive pagination
                    if pt.venue_timestamp_utc_us:
                        if max_venue_ts is None or pt.venue_timestamp_utc_us > max_venue_ts:
                            max_venue_ts = pt.venue_timestamp_utc_us
                # Inclusive pagination: bump cursor by +1us after max event time, only if adapter supports it
                if max_venue_ts and getattr(adapter, "supports_cursor", False):
                    last_cursor = max_venue_ts + 1
                # Update last_capture_map if we got any data
                if got_data:
                    self._last_capture_map[key] = now_monotonic
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.metrics.record_error()
                self.logger.error({"event": "surface_collect_error", "venue": venue, "symbol": symbol, "error": str(e)})
            next_ts += interval
            await asyncio.sleep(max(0, next_ts - time.monotonic()))
    async def _probe_server_time(self, adapter: OptionsVenueAdapter, venue: str):
        # Log-only server-time skew probe (once/minute)
        while self.running:
            try:
                url = getattr(adapter, "BASE_URL", None)
                if not url or not getattr(adapter, "session", None):
                    return
                time_url = url + "/public/get_time"
                timeout = aiohttp.ClientTimeout(total=3)
                if adapter.session is not None:
                    async with adapter.session.get(time_url, timeout=timeout) as resp:
                        data = await resp.json()
                        # Deribit /public/get_time result can be int or dict
                        result = data.get("result")
                        if isinstance(result, dict):
                            server_ms = int(result.get("unixtime", 0))
                        else:
                            try:
                                server_ms = int(result)
                            except Exception:
                                server_ms = 0
                        local_ms = int(time.time() * 1000)
                        skew = abs(server_ms - local_ms)
                        if skew > 500:
                            self.logger.info({"event": "server_time_skew", "venue": venue, "skew_ms": skew})
            except Exception:
                pass
            await asyncio.sleep(60)
    async def get_output_surface(self, timeout: float = 1.0) -> Optional[OptionSurfacePoint]:
        try:
            return await asyncio.wait_for(self.output_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

# =============================
# EXAMPLE USAGE
# =============================

async def main():
    config = {
        "venues": [
            {"name": "deribit"}
        ],
        "symbols": ["BTC", "ETH"],
        "surface_interval_sec": 30
    }
    logging.basicConfig(level=logging.INFO)
    agent = OptionsChainCollectorAgent(config)
    try:
        await agent.start()
        while True:
            pt = await agent.get_output_surface(timeout=5.0)
            if pt:
                print(f"{pt.venue} {pt.symbol} {pt.expiry} {pt.strike} {pt.option_type} IV={pt.iv}")
    except KeyboardInterrupt:
        pass
    finally:
        await agent.stop()

if __name__ == "__main__":
    asyncio.run(main())
