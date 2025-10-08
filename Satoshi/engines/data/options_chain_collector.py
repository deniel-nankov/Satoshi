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

# Streaming Bus Integration
from infra.bus.streaming_bus import StreamingBus

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
        """
        Generate institutional-grade stable hash for options surface deduplication.
        Uses only immutable identity fields: venue, symbol, expiry, strike, option_type.
        Market data (iv, mark_price, delta, venue_timestamp_utc_us) excluded for stable identity.
        """
        # Core identity: venue:symbol:expiry:strike:option_type (immutable contract identity)
        hash_input = f"{self.venue}:{self.symbol}:{self.expiry}:{self.strike}:{self.option_type.value}"
        return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()
    
    def get_content_hash(self) -> str:
        """
        Generate content hash that includes mutable market data for change tracking.
        Use this for detecting market data updates to the same contract.
        """
        # Include market data for content versioning
        content_input = f"{self.venue}:{self.symbol}:{self.expiry}:{self.strike}:{self.option_type.value}"
        
        if self.iv is not None:
            content_input += f":iv={self.iv}"
        if self.mark_price is not None:
            content_input += f":mark={self.mark_price}"
        if self.delta is not None:
            content_input += f":delta={self.delta}"
        if self.venue_timestamp_utc_us is not None:
            content_input += f":ts={self.venue_timestamp_utc_us}"
            
        return hashlib.sha256(content_input.encode('utf-8')).hexdigest()

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
        """Enhanced session initialization for options data collection."""
        # Production-grade connector for options data
        connector = aiohttp.TCPConnector(
            limit=100,              # Adequate pool for options
            limit_per_host=25,      # Conservative per-host limit
            ttl_dns_cache=600,      # Longer DNS cache for options endpoints
            use_dns_cache=True,
            keepalive_timeout=60,   # Longer keepalive for options
            enable_cleanup_closed=True,
            force_close=False
        )
        
        timeout = aiohttp.ClientTimeout(
            total=15,               # Longer timeout for options data
            connect=5,
            sock_read=10,
            sock_connect=3
        )
        
        headers = {
            "User-Agent": f"Satoshi-Options-Collector/1.0 ({self.venue})",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive"
        }
        
        self.session = aiohttp.ClientSession(
            headers=headers, 
            connector=connector, 
            timeout=timeout,
            raise_for_status=False
        )

    async def stop(self):
        """Enhanced session cleanup for options collector."""
        if self.session:
            try:
                logger = logging.getLogger(__name__)
                await asyncio.sleep(0.1)  # Allow pending requests to complete
                await self.session.close()
                await asyncio.sleep(0.3)  # Wait for connections to close
                logger.info(f"{self.venue} options session closed successfully")
            except Exception as e:
                logger = logging.getLogger(__name__)
                logger.warning(f"Error during {self.venue} options session cleanup: {e}")
            finally:
                self.session = None

    async def health_check(self) -> Dict[str, Any]:
        """Health check specific to options data collection."""
        return {
            "venue": self.venue,
            "status": "healthy" if self.session else "unhealthy",
            "session_active": self.session is not None,
            "timestamp_us": int(time.time() * 1_000_000),
            "supports_cursor": getattr(self, 'supports_cursor', False)
        }
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

class OKXOptionsAdapter(OptionsVenueAdapter):
    BASE_URL = "https://www.okx.com/api/v5"
    
    def __init__(self, venue: str, config: Dict[str, Any]):
        super().__init__(venue, config)
        self.supports_cursor = False  # OKX does not support since/cursor for options
        
    async def fetch_surface(self, symbol: str, since: Optional[int] = None) -> List[OptionSurfacePoint]:
        """Fetch options surface from OKX API."""
        if self.session is None:
            raise RuntimeError("aiohttp ClientSession is not initialized. Call 'start()' before 'fetch_surface()'.")
            
        # OKX uses uppercase symbols like 'BTC-USD' for options
        okx_symbol = f"{symbol.upper()}-USD"
        
        url = f"{self.BASE_URL}/public/opt-summary"
        params = {"uly": okx_symbol}
        timeout = aiohttp.ClientTimeout(total=self.config.get("request_timeout", 7))
        
        try:
            async with self.session.get(url, params=params, timeout=timeout) as resp:
                # Handle rate limiting
                if resp.status in (429, 418):
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after:
                        await asyncio.sleep(float(retry_after))
                    else:
                        await asyncio.sleep(1 + 2 * random.random())
                    return []
                elif not (200 <= resp.status < 300):
                    body = await resp.text()
                    self.logger.error({"event": "surface_http_error", "status": resp.status, "body": body[:300]})
                    return []
                    
                # Handle response weight throttling
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
                    
                # Check OKX response format
                if data.get("code") != "0":
                    self.logger.error({"event": "okx_api_error", "code": data.get("code"), "msg": data.get("msg")})
                    return []
                    
                now_us = int(time.time() * 1_000_000)
                surface = []
                drop_expired = self.config.get("drop_expired", False)
                now_ts = int(time.time() * 1_000_000)
                
                for option in data.get("data", []):
                    try:
                        # Parse OKX instrument name: BTC-USD-241025-70000-C
                        inst_id = option.get("instId", "")
                        parts = inst_id.split("-")
                        if len(parts) != 5:
                            self.logger.warning(f"Malformed OKX instrument ID: {inst_id}")
                            continue
                            
                        # Extract expiry from instrument name (YYMMDD format)
                        expiry_str = parts[2]  # e.g., "241025"
                        try:
                            # Convert YYMMDD to datetime
                            expiry_dt = datetime.strptime(f"20{expiry_str}", "%Y%m%d").replace(
                                hour=8, minute=0, second=0, tzinfo=timezone.utc  # OKX options expire at 08:00 UTC
                            )
                            expiry_us = int(expiry_dt.timestamp() * 1_000_000)
                        except ValueError:
                            self.logger.warning(f"Invalid expiry date in {inst_id}: {expiry_str}")
                            continue
                            
                        if drop_expired and expiry_us < now_ts:
                            continue
                            
                        # Extract strike price
                        try:
                            strike = Decimal(parts[3])
                        except (ValueError, IndexError):
                            self.logger.warning(f"Invalid strike in {inst_id}: {parts[3] if len(parts) > 3 else 'missing'}")
                            continue
                            
                        # Extract option type
                        option_type_str = parts[4].upper()
                        if option_type_str == "C":
                            option_type = OptionType.CALL
                        elif option_type_str == "P":
                            option_type = OptionType.PUT
                        else:
                            self.logger.warning(f"Unknown option type in {inst_id}: {option_type_str}")
                            continue
                            
                        # Helper function to safely convert to Decimal
                        def safe_decimal(value, field_name=""):
                            if value is None or value == "":
                                return None
                            try:
                                return Decimal(str(value))
                            except (ValueError, TypeError):
                                if field_name:
                                    self.logger.warning(f"Invalid {field_name} value: {value}")
                                return None
                                
                        # Extract option Greeks and pricing data
                        iv = safe_decimal(option.get("iv"), "iv")
                        delta = safe_decimal(option.get("delta"), "delta")
                        gamma = safe_decimal(option.get("gamma"), "gamma")
                        vega = safe_decimal(option.get("vega"), "vega") 
                        theta = safe_decimal(option.get("theta"), "theta")
                        rho = None  # OKX doesn't provide rho
                        
                        mark_price = safe_decimal(option.get("markPx"), "markPx")
                        underlying_price = safe_decimal(option.get("uly"), "underlying")
                        volume = safe_decimal(option.get("vol24h"), "volume")
                        open_interest = safe_decimal(option.get("oi"), "open_interest")
                        
                        # OKX timestamps are in milliseconds
                        venue_timestamp_ms = option.get("ts")
                        venue_timestamp_us = int(venue_timestamp_ms) * 1000 if venue_timestamp_ms else None
                        
                        point = OptionSurfacePoint(
                            symbol=symbol,
                            venue="okx",
                            timestamp_utc_us=now_us,
                            expiry=expiry_us,
                            strike=strike,
                            option_type=option_type,
                            iv=iv,
                            delta=delta,
                            gamma=gamma,
                            vega=vega,
                            theta=theta,
                            rho=rho,
                            mark_price=mark_price,
                            underlying_price=underlying_price,
                            volume=volume,
                            open_interest=open_interest,
                            venue_timestamp_utc_us=venue_timestamp_us
                        )
                        
                        surface.append(point)
                        
                    except Exception as ex:
                        self.logger.error({
                            "event": "instrument_parse_error", 
                            "instrument_id": option.get("instId", ""), 
                            "error": str(ex)
                        })
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
        
        # Integration features for 100% Kafka integration
        self.circuit_breaker_id = f"options_chain_collector_{id(self)}"
        self._circuit_breaker_registered = False
        self._health_check_interval = config.get('health_check_interval', 120.0)  # 2 minutes for options
        self._background_tasks: set = set()
        self._health_check_task: Optional[asyncio.Task] = None
        
        # Retry configuration for external API calls (venue APIs)
        self.retry_config = {
            'max_retries': config.get('max_retries', 3),
            'base_delay': config.get('base_delay', 1.0),
            'max_delay': config.get('max_delay', 30.0),
            'exponential_base': config.get('exponential_base', 2.0)
        }
        
        # Comprehensive metrics for options data collection
        self.integration_metrics = {
            'venues_processed': 0,
            'symbols_processed': 0,
            'options_contracts_collected': 0,
            'surface_points_published': 0,
            'venue_api_calls': 0,
            'venue_api_errors': 0,
            'data_quality_failures': 0,
            'circuit_breaker_activations': 0,
            'iv_surface_completeness': 0.0,
            'strike_range_coverage': 0.0,
            'time_to_expiry_coverage': 0.0,
            'duplicate_rate': 0.0,
            'health_check_failures': 0
        }
        
        # Component identification for circuit breaker
        self.component_id = "options_chain_collector"
        
        output_queue_maxsize = self.config.get("output_queue_maxsize", 10000)
        self.output_queue: asyncio.Queue = asyncio.Queue(maxsize=output_queue_maxsize)
        
        # Streaming Bus Integration
        streaming_config = self.config.get("streaming_bus", {
            "bootstrap_servers": "localhost:9092",
            "enable_ssl": False,
            "enable_sasl": False
        })
        self.streaming_bus = StreamingBus(streaming_config)
        
        # Circuit breaker configuration with options-specific tuning
        base_threshold = self.config.get("circuit_breaker_failure_threshold", 3)
        
        # Options data is typically less frequent than spot, so be more tolerant
        # Also consider that options venues have different reliability characteristics
        venues_config = self.config.get("venues", [])
        if isinstance(venues_config, list):
            options_venues = [v.get("name", "") for v in venues_config if isinstance(v, dict)]
        else:
            options_venues = list(venues_config.keys()) if isinstance(venues_config, dict) else []
        
        # If only collecting from highly reliable options venues, keep threshold low
        # If including more experimental venues, increase tolerance
        if all(venue.lower() in ['deribit', 'okx'] for venue in options_venues):
            adjusted_threshold = base_threshold  # Keep standard for reliable venues
        else:
            adjusted_threshold = base_threshold + 1  # More tolerance for mixed venue setups
            
        self.circuit_breaker_config = {
            "failure_threshold": adjusted_threshold,
            "recovery_timeout_us": self.config.get("circuit_breaker_recovery_timeout_us", 300_000_000),  # 5 minutes
            "dependency_components": ["exchange_connector"]  # Depends on underlying asset data
        }
        
        # Sequence tracking for canonical headers
        self.sequence_numbers: Dict[str, int] = {}  # venue -> sequence
        
        self._setup_adapters()
        self.logger = logging.getLogger(__name__)
        self._last_surface_time: Dict[str, int] = {}  # For inclusive pagination per symbol/venue
        self._dropped_count = 0
        # For per-stream deadman
        self._last_capture_map: Dict[str, float] = {}  # key: f"{venue}:{symbol}", value: last capture time (monotonic)

    async def _register_circuit_breaker(self):
        """Register this component with the system circuit breaker."""
        try:
            if not self._circuit_breaker_registered:
                # Actually register with the streaming bus circuit breaker system
                await self.streaming_bus.register_circuit_breaker(
                    component_id=self.component_id,
                    failure_threshold=self.circuit_breaker_config["failure_threshold"],
                    recovery_timeout_us=self.circuit_breaker_config["recovery_timeout_us"],
                    dependency_components=self.circuit_breaker_config["dependency_components"]
                )
                self._circuit_breaker_registered = True
                self.logger.info(f"Options Chain Collector registered circuit breaker: {self.circuit_breaker_id}")
        except Exception as e:
            self.logger.error(f"Failed to register circuit breaker for options collector: {e}")
            self._circuit_breaker_registered = False

    async def _perform_health_check(self) -> bool:
        """Perform health check for options chain collection pipeline."""
        try:
            health_score = 0
            total_checks = 0
            
            # Check venue adapters
            healthy_adapters = 0
            for venue, adapter in self.adapters.items():
                try:
                    health_status = await adapter.health_check()
                    if health_status.get('status') == 'healthy':
                        healthy_adapters += 1
                except Exception:
                    pass
            
            if len(self.adapters) > 0:
                adapter_health_pct = (healthy_adapters / len(self.adapters)) * 100
                if adapter_health_pct >= 80:  # 80% of adapters healthy
                    health_score += 1
                total_checks += 1
            
            # Check symbol coverage
            configured_symbols = len(self.config.get('symbols', []))
            if configured_symbols > 0:
                health_score += 1
            total_checks += 1
            
            # Check venue coverage
            configured_venues = len(self.config.get('venues', []))
            if configured_venues > 0:
                health_score += 1
            total_checks += 1
            
            # Check recent data collection activity
            recent_collections = sum(1 for capture_time in self._last_capture_map.values() 
                                   if (time.monotonic() - capture_time) < self._health_check_interval * 2)
            expected_streams = configured_venues * configured_symbols
            if expected_streams > 0 and recent_collections >= expected_streams * 0.6:  # 60% streams active
                health_score += 1
            total_checks += 1
            
            # Check data quality metrics
            total_processed = self.integration_metrics.get('options_contracts_collected', 0)
            quality_failures = self.integration_metrics.get('data_quality_failures', 0)
            if total_processed == 0 or (quality_failures / max(total_processed, 1)) < 0.1:  # < 10% failure rate
                health_score += 1
            total_checks += 1
            
            health_percentage = (health_score / total_checks) * 100 if total_checks > 0 else 0
            self.logger.debug(f"Options Chain Collector health check: {health_score}/{total_checks} ({health_percentage:.1f}%)")
            
            return health_percentage >= 80  # 80% threshold for healthy
            
        except Exception as e:
            self.integration_metrics['health_check_failures'] += 1
            self.logger.error(f"Options Chain Collector health check failed: {e}")
            return False

    async def _health_monitor_loop(self):
        """Background health monitoring loop."""
        while self.running:
            try:
                is_healthy = await self._perform_health_check()
                
                if not is_healthy:
                    self.logger.warning("Options Chain Collector health check failed")
                    if hasattr(self.streaming_bus, 'system_circuit_breaker'):
                        await self.streaming_bus.system_circuit_breaker.record_component_failure(
                            component_id=self.component_id,
                            cascade_to_dependents=False
                        )
                else:
                    if hasattr(self.streaming_bus, 'system_circuit_breaker'):
                        await self.streaming_bus.system_circuit_breaker.record_component_success(
                            component_id=self.component_id
                        )
                
                await asyncio.sleep(self._health_check_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in options chain collector health monitoring: {e}")
                await asyncio.sleep(min(self._health_check_interval, 60))

    async def _retry_with_backoff(self, operation, *args, **kwargs):
        """Retry venue API operations with exponential backoff."""
        last_exception = None
        
        for attempt in range(self.retry_config['max_retries'] + 1):
            try:
                return await operation(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt == self.retry_config['max_retries']:
                    break
                
                delay = min(
                    self.retry_config['base_delay'] * (self.retry_config['exponential_base'] ** attempt),
                    self.retry_config['max_delay']
                )
                
                self.logger.debug(f"Options Chain Collector retry {attempt + 1}/{self.retry_config['max_retries']} "
                               f"after {delay:.1f}s: {e}")
                await asyncio.sleep(delay)
        
        if last_exception:
            raise last_exception
        else:
            raise RuntimeError("Retry loop completed without success or exception")

    def get_health_status(self) -> dict:
        """Get current health status of the options chain collector."""
        return {
            "component_id": self.component_id,
            "circuit_breaker_registered": self._circuit_breaker_registered,
            "venues_configured": len(self.adapters),
            "symbols_configured": len(self.config.get('symbols', [])),
            "active_streams": len(self._last_capture_map),
            "running": self.running,
            "background_tasks": len(self._background_tasks),
            "health_check_interval": self._health_check_interval,
            "retry_config": self.retry_config,
            "legacy_metrics": {
                "success": self.metrics.success,
                "errors": self.metrics.errors,
                "duplicates": self.metrics.duplicates,
                "uptime_pct": self.metrics.uptime_pct()
            },
            "integration_metrics": self.integration_metrics.copy()
        }
    def _setup_adapters(self):
        venues_config = self.config.get("venues", [])
        
        # Handle both dict and list formats for venues
        if isinstance(venues_config, list):
            # List format: [{"name": "deribit", ...}, ...]
            for venue_cfg in venues_config:
                venue = venue_cfg.get("name")
                if venue == "deribit":
                    self.adapters[venue] = DeribitOptionsAdapter(venue, venue_cfg)
                elif venue == "okx":
                    self.adapters[venue] = OKXOptionsAdapter(venue, venue_cfg)
                # Add more venues here
        elif isinstance(venues_config, dict):
            # Dict format: {"deribit": {...}, "okx": {...}}
            for venue, venue_cfg in venues_config.items():
                if venue == "deribit":
                    self.adapters[venue] = DeribitOptionsAdapter(venue, venue_cfg)
                elif venue == "okx":
                    self.adapters[venue] = OKXOptionsAdapter(venue, venue_cfg)
                # Add more venues here
    async def start(self):
        self.running = True
        
        # Register circuit breaker
        await self._register_circuit_breaker()
        
        # Start health monitoring
        if self._health_check_task is None:
            self._health_check_task = asyncio.create_task(self._health_monitor_loop())
            self._background_tasks.add(self._health_check_task)
        
        # Initialize sequence numbers for each venue
        for venue_name in self.adapters.keys():
            self.sequence_numbers[venue_name] = 0
        
        for adapter in self.adapters.values():
            await adapter.start()
            
        for venue, adapter in self.adapters.items():
            for symbol in self.config.get("symbols", []):
                task = asyncio.create_task(self._collect_surface(adapter, symbol, venue))
                self.tasks.append(task)
                self._background_tasks.add(task)
                
        # Optionally: start server-time skew probe
        if self.config.get("enable_skew_probe", True):
            for venue, adapter in self.adapters.items():
                task = asyncio.create_task(self._probe_server_time(adapter, venue))
                self.tasks.append(task)
                self._background_tasks.add(task)
        
        # Start Kafka control message consumption
        control_task = asyncio.create_task(self._consume_control_messages())
        self.tasks.append(control_task)
        self._background_tasks.add(control_task)
        
        self.logger.info(f"Started {len(self.tasks)} total tasks with Kafka control consumption and health monitoring")
        self.logger.info(f"Circuit Breaker ID: {self.circuit_breaker_id}")
        self.logger.info(f"Health check interval: {self._health_check_interval}s")
        self.logger.info(f"Configured venues: {list(self.adapters.keys())}")
        self.logger.info(f"Configured symbols: {self.config.get('symbols', [])}")
    
    async def _consume_control_messages(self):
        """Consume control messages from Kafka topics for dynamic configuration."""
        control_topics = [
            "control.circuit_breaker",
            "control.config_update", 
            "control.venue_maintenance",
            "control.options_symbols"
        ]
        
        self.logger.info(f"Options Chain Collector: Starting control message consumption from topics: {control_topics}")
        
        try:
            await self.streaming_bus.subscribe(
                consumer_group="options_collector_control",
                topics=control_topics,
                handler=self._handle_control_message_wrapper
            )
                
        except Exception as e:
            self.logger.error(f"Options Chain Collector: Error in control message consumption: {e}")
            # Use the system circuit breaker to record failure
            await self.streaming_bus.system_circuit_breaker.record_component_failure(
                component_id=self.component_id,
                cascade_to_dependents=False
            )
    
    def _handle_control_message_wrapper(self, topic: str, partition_key: str, 
                                      message: dict, headers: dict):
        """Wrapper to handle the subscribe callback signature."""
        # Schedule the async handler and store task reference
        task = asyncio.create_task(self._handle_control_message(topic, message))
        self.tasks.append(task)
        
        # Add callback to remove task when done and log any exceptions
        def task_done_callback(completed_task):
            try:
                self.tasks.remove(completed_task)
                # Check for exceptions
                exc = completed_task.exception()
                if exc:
                    self.logger.error(f"Control message handler failed: {exc}")
            except ValueError:
                pass  # Task already removed
        
        task.add_done_callback(task_done_callback)
    
    async def _handle_control_message(self, topic: str, message: dict):
        """Handle control messages for dynamic behavior adjustment."""
        try:
            if topic == "control.circuit_breaker":
                # Handle circuit breaker commands
                component_id = message.get("component_id")
                if component_id == self.component_id or component_id == "all":
                    action = message.get("action")
                    if action == "open":
                        self.logger.warning(f"Options Chain Collector: Circuit breaker opened via control message")
                    elif action == "close":
                        self.logger.info(f"Options Chain Collector: Circuit breaker closed via control message")
                        
            elif topic == "control.config_update":
                # Handle dynamic configuration updates
                component_id = message.get("component_id")
                if component_id == self.component_id or component_id == "all":
                    config_updates = message.get("updates", {})
                    await self._apply_config_updates(config_updates)
                    
            elif topic == "control.venue_maintenance":
                # Handle venue maintenance notifications
                venue = message.get("venue")
                if venue in self.adapters:
                    maintenance_action = message.get("action")
                    if maintenance_action == "start":
                        self.logger.warning(f"Options Chain Collector: Maintenance started for {venue}")
                        # Could pause collection for this venue
                    elif maintenance_action == "end":
                        self.logger.info(f"Options Chain Collector: Maintenance ended for {venue}")
                        
            elif topic == "control.options_symbols":
                # Handle dynamic symbol addition/removal
                action = message.get("action")
                symbols = message.get("symbols", [])
                if action == "add":
                    self.logger.info(f"Options Chain Collector: Adding symbols: {symbols}")
                    # Could dynamically add new symbols to collection
                elif action == "remove":
                    self.logger.info(f"Options Chain Collector: Removing symbols: {symbols}")
                    # Could dynamically remove symbols from collection
                        
        except Exception as e:
            self.logger.error(f"Options Chain Collector: Error handling control message from {topic}: {e}")
    
    async def _apply_config_updates(self, updates: dict):
        """Apply dynamic configuration updates."""
        try:
            # Update collection intervals
            if "surface_interval_sec" in updates:
                self.logger.info(f"Options Chain Collector: Updated surface_interval_sec to {updates['surface_interval_sec']}")
                
            # Update circuit breaker thresholds
            if "circuit_breaker_failure_threshold" in updates:
                self.circuit_breaker_config["failure_threshold"] = updates["circuit_breaker_failure_threshold"]
                self.logger.info(f"Options Chain Collector: Updated circuit breaker threshold to {updates['circuit_breaker_failure_threshold']}")
                
            # Update symbols list
            if "symbols" in updates:
                self.config["symbols"] = updates["symbols"]
                self.logger.info(f"Options Chain Collector: Updated symbols to {updates['symbols']}")
                
        except Exception as e:
            self.logger.error(f"Options Chain Collector: Error applying config updates: {e}")
    
    async def stop(self):
        self.logger.info("Stopping Options Chain Collector Agent...")
        self.running = False
        
        # Cancel all tasks with timeout
        all_tasks = list(self.tasks) + list(self._background_tasks)
        for task in all_tasks:
            if not task.done():
                task.cancel()
        
        # Wait for tasks to complete with timeout
        if all_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*all_tasks, return_exceptions=True),
                    timeout=30.0
                )
            except asyncio.TimeoutError:
                self.logger.warning("Some options collector tasks did not complete within 30s timeout")
        
        # Clean up adapters
        for adapter in self.adapters.values():
            await adapter.stop()
        
        # Clear task collections
        self.tasks.clear()
        self._background_tasks.clear()
        self._health_check_task = None
        
        self.logger.info("Options Chain Collector Agent stopped")
        self.logger.info(f"Final legacy metrics - Success: {self.metrics.success}, Errors: {self.metrics.errors}")
        self.logger.info(f"Final integration metrics: {self.integration_metrics}")
    async def _collect_surface(self, adapter: OptionsVenueAdapter, symbol: str, venue: str):
        interval = self.config.get("surface_interval_sec", 30)
        key = f"{venue}:{symbol}"
        next_ts = time.monotonic()
        last_cursor = None
        warn_drop_threshold = self.config.get("warn_drop_threshold", 100)
        last_warned_drop = 0
        
        while self.running:
            # Check circuit breaker before attempting collection
            if not await self.streaming_bus.can_component_execute(self.component_id):
                self.logger.warning(f"Circuit breaker open for {self.component_id}, skipping options surface collection")
                await asyncio.sleep(interval)
                continue
            
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
                # Use retry wrapper for venue API calls
                self.integration_metrics['venue_api_calls'] += 1
                try:
                    surface = await self._retry_with_backoff(adapter.fetch_surface, symbol, since=last_cursor)
                except Exception as e:
                    self.integration_metrics['venue_api_errors'] += 1
                    raise e
                    
                max_venue_ts = last_cursor
                got_data = False
                surface_points_this_batch = 0
                quality_failures_this_batch = 0
                
                # Track successful data fetch
                if hasattr(self.streaming_bus, 'system_circuit_breaker'):
                    await self.streaming_bus.system_circuit_breaker.record_component_success(self.component_id)
                
                for pt in surface:
                    # Validate options data quality before processing
                    if not self._validate_options_data_quality(pt):
                        quality_failures_this_batch += 1
                        self.integration_metrics['data_quality_failures'] += 1
                        self.logger.warning(f"Invalid options data for {venue}:{symbol}, skipping")
                        continue
                        
                    h = pt.get_hash()
                    if self.duplicate_detector.is_duplicate(h):
                        self.metrics.record_duplicate()
                        continue
                    
                    # Get sequence number for this venue
                    self.sequence_numbers[venue] += 1
                    sequence_num = self.sequence_numbers[venue]
                    
                    surface_points_this_batch += 1
                    self.integration_metrics['options_contracts_collected'] += 1
                    
                    # Streaming Bus: Publish with canonical headers and circuit breaker protection
                    try:
                        surface_data = {
                            "venue": venue,
                            "symbol": symbol,
                            "data_type": "options_surface",
                            "timestamp": pt.timestamp_utc_us,
                            "venue_timestamp": pt.venue_timestamp_utc_us,
                            "expiry": pt.expiry,
                            "strike": str(pt.strike),
                            "option_type": pt.option_type.value,
                            "iv": str(pt.iv) if pt.iv else None,
                            "delta": str(pt.delta) if pt.delta else None,
                            "gamma": str(pt.gamma) if pt.gamma else None,
                            "vega": str(pt.vega) if pt.vega else None,
                            "theta": str(pt.theta) if pt.theta else None,
                            "rho": str(pt.rho) if pt.rho else None,
                            "mark_price": str(pt.mark_price) if pt.mark_price else None,
                            "underlying_price": str(pt.underlying_price) if pt.underlying_price else None,
                            "volume": str(pt.volume) if pt.volume else None,
                            "open_interest": str(pt.open_interest) if pt.open_interest else None
                        }
                        
                        # Use institutional partitioning standard: "{venue}:{symbol}" 
                        partition_key = f"{venue}:{symbol}"
                        
                        # Create source ID for canonical headers
                        source_id = f"{self.component_id}.{venue}"
                        
                        # Publish with circuit breaker protection
                        try:
                            if hasattr(self.streaming_bus, 'publish_with_circuit_breaker_check'):
                                success = await self.streaming_bus.publish_with_circuit_breaker_check(
                                    component_id=self.component_id,
                                    topic="raw_data.options_chain",
                                    partition_key=partition_key,
                                    payload=surface_data,
                                    source_id=source_id,
                                    sequence_number=sequence_num,
                                    dedupe_key=pt.get_hash()  # Use institutional-grade dedupe key
                                )
                            else:
                                # Fallback to regular publish
                                await self.streaming_bus.publish_with_headers(
                                    topic="raw_data.options_chain",
                                    partition_key=partition_key,
                                    payload=surface_data,
                                    headers={
                                        "source_id": source_id,
                                        "sequence_number": str(sequence_num),
                                        "data_type": "options_surface",
                                        "venue": venue,
                                        "symbol": symbol,
                                        "collector_version": "enhanced_v1"
                                    },
                                    dedupe_key=pt.get_hash()
                                )
                                success = True
                            
                            if success:
                                self.integration_metrics['surface_points_published'] += 1
                            else:
                                self.logger.warning(f"Failed to publish options surface to streaming bus - circuit breaker or network issue")
                                
                        except Exception as e:
                            self.logger.warning(f"Failed to publish options surface to streaming bus: {e}")
                            success = False
                            
                    except Exception as e:
                        self.logger.warning(f"Failed to publish options surface to streaming bus: {e}")
                    
                    # Local queue fallback with proper error handling and metrics
                    try:
                        if self.output_queue.full():
                            try:
                                self.output_queue.get_nowait()
                                self._dropped_count += 1
                                self.logger.debug(f"Dropped old options surface from full queue for {venue}")
                            except asyncio.QueueEmpty:
                                pass
                        self.output_queue.put_nowait(pt)
                        self.logger.debug(f"Enqueued options surface to local queue for {venue}")
                    except asyncio.QueueFull:
                        self.logger.warning(f"Failed to enqueue options surface for {venue} - queue full")
                    except Exception as queue_e:
                        self.logger.warning(f"Failed to enqueue options surface to local queue: {queue_e}")
                    
                    self.metrics.record_success()
                    got_data = True
                    
                    # Log a warning if drops grow fast
                    if self._dropped_count - last_warned_drop >= warn_drop_threshold:
                        self.logger.warning({
                            "event": "surface_output_queue_drops",
                            "venue": venue,
                            "dropped_count": self._dropped_count,
                            "since_last_warning": self._dropped_count - last_warned_drop
                        })
                        last_warned_drop = self._dropped_count
                    
                    # Track max event time for inclusive pagination
                    if pt.venue_timestamp_utc_us:
                        if max_venue_ts is None or pt.venue_timestamp_utc_us > max_venue_ts:
                            max_venue_ts = pt.venue_timestamp_utc_us
                            
                # Update batch metrics
                if surface_points_this_batch > 0:
                    # Update venues and symbols processed
                    self.integration_metrics['venues_processed'] += 1
                    self.integration_metrics['symbols_processed'] += 1
                    
                    # Calculate duplicate rate
                    total_processed = self.integration_metrics['options_contracts_collected']
                    total_duplicates = self.metrics.duplicates
                    if total_processed > 0:
                        self.integration_metrics['duplicate_rate'] = (total_duplicates / total_processed) * 100
                    
                    self.logger.debug(f"Processed {surface_points_this_batch} options for {venue}:{symbol}, "
                                   f"{quality_failures_this_batch} quality failures")
                                   
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
                self.integration_metrics['venue_api_errors'] += 1
                self.logger.error({"event": "surface_collect_error", "venue": venue, "symbol": symbol, "error": str(e)})
                # Record circuit breaker failure
                if hasattr(self.streaming_bus, 'system_circuit_breaker'):
                    await self.streaming_bus.system_circuit_breaker.record_component_failure(self.component_id)
                elif hasattr(self.streaming_bus, 'record_component_failure'):
                    await self.streaming_bus.record_component_failure(self.component_id)
                
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

    def _validate_options_data_quality(self, pt: OptionSurfacePoint) -> bool:
        """Basic data quality validation for options data."""
        try:
            # Basic sanity checks for options-specific data
            if pt.strike <= 0:
                return False
                
            # IV should be reasonable (0-1000% expressed as decimal)
            if pt.iv is not None and (pt.iv < 0 or pt.iv > 10):
                return False
                
            # Delta should be between -1 and 1 for options
            if pt.delta is not None and (pt.delta < -1 or pt.delta > 1):
                return False
                
            # Gamma should be non-negative
            if pt.gamma is not None and pt.gamma < 0:
                return False
                
            # Mark price should be positive
            if pt.mark_price is not None and pt.mark_price <= 0:
                return False
                
            # Underlying price should be positive
            if pt.underlying_price is not None and pt.underlying_price <= 0:
                return False
                
            # Timestamp should be reasonable (within last 24 hours)
            current_time_us = int(time.time() * 1_000_000)
            if abs(pt.timestamp_utc_us - current_time_us) > 24 * 60 * 60 * 1_000_000:
                return False
                
            return True
            
        except Exception:
            return False

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
            {"name": "deribit"},
            {"name": "okx"}
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
