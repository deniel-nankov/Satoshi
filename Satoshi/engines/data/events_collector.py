"""
Off-Chain Events Collector Agent

Mission: Unlock calendars, governance proposals, exchange status, GitHub releases.

Outputs: raw_data.events.calendar.

SLO: event→bus lag p95 < 5 minutes (real-time for governance, best-effort for others).
"""

import asyncio
import aiohttp
import logging
from typing import Any, Optional, ClassVar
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime, timezone
import time
import hashlib
from collections import defaultdict, deque
import os
import json

# Streaming Bus Integration
from infra.bus.streaming_bus import StreamingBus

logger = logging.getLogger(__name__)

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
            # Try ISO string - force UTC for naive timestamps
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1_000_000)
        except Exception:
            try:
                return int(ts)
            except Exception:
                return default_now_us
    return default_now_us

# =============================
# DATA STRUCTURES
# =============================

@dataclass
class CalendarEvent:
    event_type: str  # e.g. 'governance_proposal', 'token_unlock', 'exchange_maintenance', 'github_release'
    title: str
    description: Optional[str] = None
    start_time_utc_us: int = 0
    end_time_utc_us: Optional[int] = None
    source: str = ""  # e.g. 'snapshot', 'compound', 'binance', 'github'
    source_id: str = ""  # external ID from source
    status: str = "active"  # 'active', 'completed', 'cancelled'
    metadata: Optional[dict[str, Any]] = field(default_factory=lambda: {})
    capture_timestamp_utc_us: int = 0

    def get_hash(self) -> str:
        h = f"{self.event_type}:{self.source}:{self.source_id}:{self.start_time_utc_us}"
        return hashlib.sha256(h.encode()).hexdigest()

# =============================
# EVENT CLASSIFIER
# =============================

class EventClassifier:
    """Deterministic classification and prioritization of off-chain events."""
    
    # Event priority levels (1 = highest priority, 5 = lowest)
    EVENT_PRIORITIES: ClassVar[dict[str, int]] = {
        'governance_proposal': 1,      # Highest priority - real-time
        'exchange_maintenance': 2,     # High priority - trading impact
        'github_release': 3,           # Medium priority
        'token_unlock': 4,             # Medium priority
        'general_announcement': 5      # Lowest priority
    }
    
    # Valid status transitions for governance proposals
    VALID_TRANSITIONS: ClassVar[dict[str, set[str]]] = {
        'pending': {'active', 'cancelled'},
        'active': {'closed', 'succeeded', 'defeated', 'cancelled'},
        'closed': {'succeeded', 'defeated'},
        'succeeded': set(),  # Terminal state
        'defeated': set(),   # Terminal state
        'cancelled': set()   # Terminal state
    }
    
    @classmethod
    def classify_event_priority(cls, event: CalendarEvent) -> int:
        """Classify event priority for processing order."""
        return cls.EVENT_PRIORITIES.get(event.event_type, 5)
    
    @classmethod
    def validate_status_transition(cls, old_status: str, new_status: str, event_type: str) -> bool:
        """Validate status transitions for governance proposals."""
        if event_type != 'governance_proposal':
            return True  # Only validate governance proposal transitions
        
        return new_status in cls.VALID_TRANSITIONS.get(old_status, set())
    
    @classmethod
    def normalize_event_type(cls, raw_type: str, source: str, metadata: dict) -> str:
        """Normalize event types across sources."""
        raw_type = raw_type.lower().strip()
        
        # Governance proposal detection
        if ('proposal' in raw_type or 'governance' in raw_type or 
            source == 'snapshot' or metadata.get('space')):
            return 'governance_proposal'
        
        # Exchange maintenance detection
        if ('maintenance' in raw_type or 'outage' in raw_type or 
            'incident' in raw_type or source in {'binance', 'coinbase', 'gemini', 'kraken'}):
            return 'exchange_maintenance'
        
        # GitHub release detection
        if ('release' in raw_type or 'version' in raw_type or 
            source == 'github' or metadata.get('tag_name')):
            return 'github_release'
        
        # Token unlock detection
        if ('unlock' in raw_type or 'vesting' in raw_type or 
            'airdrop' in raw_type):
            return 'token_unlock'
        
        return raw_type or 'general_announcement'

# =============================
# EVENT VALIDATOR
# =============================

class EventValidator:
    """Lightweight validation for event data - source-side sanity checks only."""
    
    @staticmethod
    def validate_event_timing(event: CalendarEvent) -> tuple[bool, list[str]]:
        """Validate event timing is reasonable."""
        issues = []
        now = int(time.time() * 1_000_000)
        
        # Check if start time is too far in the past (older than 5 years)
        five_years_ago = now - (5 * 365 * 24 * 60 * 60 * 1_000_000)
        if event.start_time_utc_us < five_years_ago:
            issues.append("start_time_too_old")
        
        # Check if start time is too far in the future (more than 10 years)
        ten_years_future = now + (10 * 365 * 24 * 60 * 60 * 1_000_000)
        if event.start_time_utc_us > ten_years_future:
            issues.append("start_time_too_future")
        
        # Check if end time is before start time
        if (event.end_time_utc_us and 
            event.end_time_utc_us < event.start_time_utc_us):
            issues.append("end_before_start")
        
        # Check for zero timestamps (likely parsing error)
        if event.start_time_utc_us == 0:
            issues.append("zero_start_time")
        
        return len(issues) == 0, issues
    
    @staticmethod
    def validate_governance_proposal(event: CalendarEvent) -> tuple[bool, list[str]]:
        """Validate governance proposal specific fields."""
        issues = []
        
        if event.event_type != 'governance_proposal':
            return True, []
        
        # Check for required metadata
        metadata = event.metadata or {}
        if not metadata.get('space'):
            issues.append("missing_governance_space")
        
        if not metadata.get('author'):
            issues.append("missing_proposal_author")
        
        # Check proposal ID format (should be meaningful)
        if not event.source_id or len(event.source_id) < 8:
            issues.append("invalid_proposal_id")
        
        # Check title length (too short might be truncated)
        if len(event.title) < 10:
            issues.append("title_too_short")
        
        return len(issues) == 0, issues
    
    @staticmethod
    def validate_github_release(event: CalendarEvent) -> tuple[bool, list[str]]:
        """Validate GitHub release specific fields."""
        issues = []
        
        if event.event_type != 'github_release':
            return True, []
        
        metadata = event.metadata or {}
        
        # Check for required GitHub metadata
        if not metadata.get('repo'):
            issues.append("missing_repo_name")
        
        if not metadata.get('tag_name'):
            issues.append("missing_tag_name")
        
        # Check for valid release ID
        if not event.source_id:
            issues.append("missing_release_id")
        
        return len(issues) == 0, issues
    
    @classmethod
    def validate_event(cls, event: CalendarEvent) -> tuple[bool, list[str]]:
        """Comprehensive event validation."""
        all_issues = []
        
        # General timing validation
        _, timing_issues = cls.validate_event_timing(event)
        all_issues.extend(timing_issues)
        
        # Event-type specific validation
        if event.event_type == 'governance_proposal':
            _, gov_issues = cls.validate_governance_proposal(event)
            all_issues.extend(gov_issues)
        elif event.event_type == 'github_release':
            _, gh_issues = cls.validate_github_release(event)
            all_issues.extend(gh_issues)
        
        return len(all_issues) == 0, all_issues

# =============================
# EVENT CORRELATOR
# =============================

class EventCorrelator:
    """Cross-source event correlation and deduplication."""
    
    def __init__(self, correlation_window_hours: int = 24):
        self.correlation_window_hours = correlation_window_hours
        self.pending_correlations: dict[str, list[CalendarEvent]] = defaultdict(list)
        self.max_correlations_per_key = 100  # Prevent memory bloat
        self._lock = asyncio.Lock()  # Protect concurrent access to pending_correlations
    
    async def find_related_events(self, event: CalendarEvent) -> list[CalendarEvent]:
        """Find events that might be duplicates or related."""
        async with self._lock:
            related = []
            correlation_key = self._get_correlation_key(event)
            
            # Look for events within correlation window
            time_window = self.correlation_window_hours * 60 * 60 * 1_000_000  # Convert to microseconds
            candidates = self.pending_correlations[correlation_key]
            
            for candidate in candidates:
                time_diff = abs(candidate.start_time_utc_us - event.start_time_utc_us)
                if time_diff < time_window:
                    similarity = self._calculate_similarity(event, candidate)
                    if similarity > 0.7:  # 70% similarity threshold
                        related.append(candidate)
            
            return related
    
    async def add_event_for_correlation(self, event: CalendarEvent):
        """Add event to correlation tracking."""
        async with self._lock:
            correlation_key = self._get_correlation_key(event)
            correlations = self.pending_correlations[correlation_key]
            
            # Add new event
            correlations.append(event)
            
            # Maintain reasonable size
            if len(correlations) > self.max_correlations_per_key:
                # Remove oldest events
                correlations.sort(key=lambda e: e.capture_timestamp_utc_us)
                self.pending_correlations[correlation_key] = correlations[-self.max_correlations_per_key:]
    
    def _get_correlation_key(self, event: CalendarEvent) -> str:
        """Generate correlation key for grouping similar events."""
        # Use event type + normalized title keywords
        title_words = set(event.title.lower().split())
        
        # Extract key words that might indicate same event
        key_words = title_words & {
            'ethereum', 'bitcoin', 'upgrade', 'fork', 'proposal', 'maintenance', 
            'release', 'unlock', 'airdrop', 'hardfork', 'snapshot', 'voting',
            'compound', 'aave', 'uniswap', 'binance', 'coinbase'
        }
        
        # Include event type and up to 3 key words
        sorted_words = sorted(key_words)[:3]
        return f"{event.event_type}:{':'.join(sorted_words)}"
    
    def _calculate_similarity(self, event1: CalendarEvent, event2: CalendarEvent) -> float:
        """Calculate similarity score between two events."""
        # Skip if different sources and types (unlikely to be duplicates)
        if event1.source != event2.source and event1.event_type != event2.event_type:
            return 0.0
        
        # Title similarity (word overlap)
        words1 = set(event1.title.lower().split())
        words2 = set(event2.title.lower().split())
        
        if not words1 or not words2:
            return 0.0
        
        intersection = len(words1 & words2)
        union = len(words1 | words2)
        jaccard_similarity = intersection / union if union > 0 else 0.0
        
        # Boost similarity if same source_id (likely same event)
        if event1.source_id and event1.source_id == event2.source_id:
            jaccard_similarity = min(1.0, jaccard_similarity + 0.3)
        
        return jaccard_similarity

# =============================
# DUPLICATE DETECTOR
# =============================

class DuplicateDetector:
    def __init__(self, window_size: int = 10000):
        self.window_size = window_size
        # For each stream, keep a dict of key -> (deque, set)
        self.seen: dict[str, dict[str, tuple]] = defaultdict(dict)

    def is_duplicate(self, data_type: str, data_hash: str, key: str = "default") -> bool:
        # key can be source, event_type, etc. for per-stream dedup
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
# HTTP CLIENT
# =============================

class _AsyncNullContext:
    """Async no-op context manager used when rate limiting is disabled."""

    async def __aenter__(self):
        return None

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False


class HttpClient:
    """Generic HTTP client for various APIs."""
    def __init__(self, api_key: Optional[str] = None,
                 streaming_bus: Optional[StreamingBus] = None,
                 rate_domain: Optional[str] = None,
                 rate_timeout_sec: float = 5.0):
        self.api_key = api_key
        self.session: aiohttp.ClientSession | None = None
        self._streaming_bus = streaming_bus
        self._rate_domain = rate_domain
        self._rate_timeout = float(rate_timeout_sec)
        self._rate_limit_enabled = self._streaming_bus is not None and self._rate_domain is not None

    async def __aenter__(self):
        if self.session is None or self.session.closed:
            connector = aiohttp.TCPConnector(limit_per_host=5)
            timeout = aiohttp.ClientTimeout(total=30)
            headers = {
                'User-Agent': 'EventsCollector/1.0 (aiohttp)'
            }
            if self.api_key:
                headers['Authorization'] = f'Bearer {self.api_key}'
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers=headers
            )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()

    async def post_json(self, url: str, json_data: dict, headers: dict[str, str] = {}) -> Optional[dict]:
        assert self.session is not None, "aiohttp session is not initialized"
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with self._rate_limit_context():
                async with self.session.post(url, json=json_data, headers=headers, timeout=timeout) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("Retry-After", "5"))
                        logger.warning(f"HTTP 429, sleeping {retry_after}s for {url}")
                        self._record_rate_limit_429()
                        await asyncio.sleep(min(retry_after, self._rate_timeout))
                        return None
                    if resp.status >= 400:
                        body = await resp.text()
                        logger.error({"event":"http_error","url":url,"status":resp.status,"body":body[:300]})
                        return None
                try:
                    return await resp.json()
                except Exception:
                    body = await resp.text()
                    logger.warning(f"Non-JSON response from POST {url}: {body[:300]}")
                    return None
        except Exception as e:
            logger.warning(f"HTTP error for {url}: {e}")
            return None

    async def get_json_with_headers(self, url: str, headers: dict[str, str] = {}) -> tuple[Optional[dict], dict[str, str]]:
        """Returns (json_data, response_headers) for ETag handling."""
        assert self.session is not None, "aiohttp session is not initialized"
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with self._rate_limit_context():
                async with self.session.get(url, headers=headers, timeout=timeout) as resp:
                    resp_headers = dict(resp.headers)
                    if resp.status == 304:
                        # Not Modified - return None data but headers
                        return None, resp_headers
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("Retry-After", "5"))
                        logger.warning(f"HTTP 429, sleeping {retry_after}s for {url}")
                        self._record_rate_limit_429()
                        await asyncio.sleep(min(retry_after, self._rate_timeout))
                        return None, {}
                    if resp.status >= 400:
                        body = await resp.text()
                        logger.error({"event":"http_error","url":url,"status":resp.status,"body":body[:300]})
                        return None, {}
                try:
                    json_data = await resp.json()
                    return json_data, resp_headers
                except Exception:
                    body = await resp.text()
                    logger.warning(f"Non-JSON response from {url}: {body[:300]}")
                    return None, {}
        except Exception as e:
            logger.warning(f"HTTP error for {url}: {e}")
            return None, {}

    async def get_json(self, url: str, headers: dict[str, str] = {}) -> Optional[dict]:
        assert self.session is not None, "aiohttp session is not initialized"
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with self._rate_limit_context():
                async with self.session.get(url, headers=headers, timeout=timeout) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("Retry-After", "5"))
                        logger.warning(f"HTTP 429, sleeping {retry_after}s for {url}")
                        self._record_rate_limit_429()
                        await asyncio.sleep(min(retry_after, self._rate_timeout))
                        return None
                    if resp.status >= 400:
                        body = await resp.text()
                        logger.error({"event":"http_error","url":url,"status":resp.status,"body":body[:300]})
                        return None
                return await resp.json()
        except Exception as e:
            logger.warning(f"HTTP error for {url}: {e}")
            return None

    def _rate_limit_context(self):
        if self._rate_limit_enabled:
            return self._streaming_bus.rate_limit(self._rate_domain, timeout=self._rate_timeout)
        return _AsyncNullContext()

    def _record_rate_limit_429(self) -> None:
        if not self._rate_limit_enabled:
            return
        try:
            self._streaming_bus.record_rate_limit_429(self._rate_domain)
        except Exception as exc:
            logger.debug(f"Failed to record rate limit 429 for domain {self._rate_domain}: {exc}")

# =============================
# MAIN EVENTS COLLECTOR AGENT
# =============================

class EventsCollectorAgent:
    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.duplicate_detector = DuplicateDetector()
        self.output_queues: dict[str, asyncio.Queue] = {
            'calendar': asyncio.Queue(maxsize=config.get('calendar_queue_size', 5000)),
        }
        
        # Streaming Bus Integration
        streaming_config = self.config.get("streaming_bus", {
            "bootstrap_servers": "localhost:9092",
            "enable_ssl": False,
            "enable_sasl": False
        })
        self.streaming_bus = StreamingBus(streaming_config)
        
        # Component identification for circuit breaker
        self.component_id = "events_collector"
        self.circuit_breaker_id = f"events_collector_{id(self)}"
        self._circuit_breaker_registered = False
        
        # Task management for async operations
        self._tasks = set()
        
        # Enhanced Event Processing Components
        self.classifier = EventClassifier()
        self.validator = EventValidator()
        self.correlator = EventCorrelator(config.get('correlation_window_hours', 24))
        
        self.running = False
        self.tasks: list[asyncio.Task] = []
        
        # Enhanced task management for health monitoring
        self._background_tasks: set[asyncio.Task] = set()
        self._health_check_task: Optional[asyncio.Task] = None
        
        # Health monitoring configuration (agent-specific, not Kafka health)
        self._health_check_interval = config.get('health_check_interval', 300.0)  # 5 minutes
        self._last_health_check = time.time()
        
        # Retry configuration for external API calls (not Kafka operations)
        self.retry_config = {
            'max_retries': config.get('max_retries', 3),
            'base_delay_ms': config.get('base_delay_ms', 1000),
            'max_delay_ms': config.get('max_delay_ms', 30000),
            'exponential_base': config.get('exponential_base', 2.0)
        }
        
        # Comprehensive metrics for event processing performance
        self.metrics = {
            'events_processed': 0,
            'events_published': 0,
            'events_failed': 0,
            'events_validated': 0,
            'events_correlated': 0,
            'events_duplicates_filtered': 0,
            'api_calls_made': 0,
            'api_calls_retried': 0,
            'api_calls_failed': 0,
            'sources_healthy': 0,
            'sources_unhealthy': 0,
            'health_checks_performed': 0,
            'processing_errors': 0
        }
        # ETag cache for GitHub API
        self.etag_by_repo: dict[str, str] = {}
        # Enhanced status tracking for event updates with transition validation
        self.last_status: dict[tuple, str] = {}  # (source, source_id) -> status
        
        # Source health tracking
        self.source_health: dict[str, dict[str, Any]] = defaultdict(lambda: {
            'status': 'unknown',
            'last_success': None,
            'consecutive_failures': 0,
            'circuit_breaker_open': False,
            'last_check': None
        })

    async def _publish_event(self, event: CalendarEvent, partition_key: str, headers: dict[str, str]) -> bool:
        """Enhanced event publishing with validation and correlation metadata."""
        try:
            # Validate event data
            is_valid, validation_issues = self.validator.validate_event(event)
            
            # Calculate event priority
            priority = self.classifier.classify_event_priority(event)
            
            # Find related events for correlation
            related_events = await self.correlator.find_related_events(event)
            
            # Add event to correlation tracking
            await self.correlator.add_event_for_correlation(event)
            
            event_data = {
                "source": event.source,
                "event_type": event.event_type,
                "title": event.title,
                "description": event.description[:500] if event.description else None,
                "timestamp": event.start_time_utc_us,
                "end_timestamp": event.end_time_utc_us,
                "capture_timestamp": event.capture_timestamp_utc_us,
                "status": event.status,
                "source_id": event.source_id,
                "metadata": event.metadata,
                # Enhanced fields
                "priority": priority,
                "related_event_count": len(related_events),
                "validation_status": "valid" if is_valid else "suspect"
            }
            
            # Enhanced headers with validation and correlation info
            enhanced_headers = {
                **headers,
                "data_type": "calendar_events",
                "priority": str(priority),
                "collector_version": "enhanced_v1"
            }
            
            # Add validation flags
            if validation_issues:
                enhanced_headers["validation_issues"] = ','.join(validation_issues)
                enhanced_headers["suspect_data"] = "true"
            
            # Add correlation info
            if related_events:
                enhanced_headers["has_related_events"] = "true"
                enhanced_headers["related_count"] = str(len(related_events))
            
            # Add source health info
            source_health = self.source_health[event.source]
            enhanced_headers["source_health"] = source_health['status']
            if source_health['consecutive_failures'] > 0:
                enhanced_headers["source_failures"] = str(source_health['consecutive_failures'])
            
            await self.streaming_bus.publish_with_headers(
                topic="raw_data.offchain_events",
                partition_key=partition_key,
                payload=event_data,
                headers=enhanced_headers
            )
            
            # Update metrics on successful publish
            self.metrics['events_processed'] += 1
            self.metrics['events_published'] += 1
            if is_valid:
                self.metrics['events_validated'] += 1
            if related_events:
                self.metrics['events_correlated'] += 1
            
            # Update source health on successful publish
            self._update_source_health(event.source, success=True)
            
            return True
        except Exception as e:
            logger.warning(f"Failed to publish {event.event_type} event to streaming bus: {e}")
            self.metrics['events_failed'] += 1
            self.metrics['processing_errors'] += 1
            self._update_source_health(event.source, success=False)
            return False
    
    def _update_source_health(self, source: str, success: bool):
        """Update source health tracking."""
        health = self.source_health[source]
        health['last_check'] = time.time()
        
        if success:
            health['status'] = 'healthy'
            health['last_success'] = time.time()
            health['consecutive_failures'] = 0
            health['circuit_breaker_open'] = False
        else:
            health['consecutive_failures'] += 1
            if health['consecutive_failures'] >= 3:
                health['status'] = 'unhealthy'
                health['circuit_breaker_open'] = True
                logger.warning(f"Circuit breaker opened for source {source} after {health['consecutive_failures']} failures")
    
    def _is_source_healthy(self, source: str) -> bool:
        """Check if source is healthy for data collection."""
        return not self.source_health[source]['circuit_breaker_open']

    async def _add_to_queue(self, event: CalendarEvent) -> bool:
        """Helper method to add events to local queue with consistent handling."""
        try:
            q = self.output_queues['calendar']
            if q.full():
                try: 
                    q.get_nowait()
                except asyncio.QueueEmpty: 
                    pass
            q.put_nowait(event)
            return True
        except asyncio.QueueFull:
            return False

    async def _register_circuit_breaker(self):
        """Register circuit breaker with streaming bus."""
        try:
            if not self._circuit_breaker_registered:
                await self.streaming_bus.register_circuit_breaker(
                    component_id=self.circuit_breaker_id,
                    failure_threshold=5,  # Tolerant for event processing
                    recovery_timeout_us=300_000_000,  # 5 minutes
                    dependency_components=[]  # Events collector is typically independent
                )
                self._circuit_breaker_registered = True
                logger.info(f"Registered circuit breaker: {self.circuit_breaker_id}")
        except Exception as e:
            logger.error(f"Failed to register circuit breaker: {e}")
            raise

    async def _perform_health_check(self) -> bool:
        """Perform comprehensive health check of event processing capabilities."""
        try:
            self.metrics['health_checks_performed'] += 1
            
            # Check if we have healthy event sources
            healthy_sources = 0
            total_sources = len(self.source_health)
            
            for source_name, health_info in self.source_health.items():
                if health_info['status'] in ['healthy', 'degraded']:
                    healthy_sources += 1
                elif health_info['consecutive_failures'] > 10:
                    # Source has been failing for too long
                    health_info['circuit_breaker_open'] = True
            
            # Update metrics
            self.metrics['sources_healthy'] = healthy_sources
            self.metrics['sources_unhealthy'] = total_sources - healthy_sources
            
            # Check event processing pipeline health
            recent_events = self.metrics['events_processed'] - getattr(self, '_last_events_count', 0)
            self._last_events_count = self.metrics['events_processed']
            
            # Health is good if we have some healthy sources and processing pipeline works
            pipeline_healthy = (
                self.metrics['processing_errors'] < self.metrics['events_processed'] * 0.1 if self.metrics['events_processed'] > 0 
                else True
            )
            
            # Overall health: at least 30% sources healthy AND pipeline healthy
            is_healthy = (
                (healthy_sources >= total_sources * 0.3 if total_sources > 0 else True) and
                pipeline_healthy
            )
            
            if not is_healthy:
                logger.warning(f"Health check failed: {healthy_sources}/{total_sources} sources healthy, pipeline_healthy={pipeline_healthy}")
            
            self._last_health_check = time.time()
            return is_healthy
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    async def _health_monitor_loop(self):
        """Background health monitoring loop for event processing."""
        while self.running:
            try:
                await self._perform_health_check()
                await asyncio.sleep(self._health_check_interval)
            except asyncio.CancelledError:
                # Task was cancelled, break gracefully
                break
            except Exception as e:
                logger.error(f"Health monitor error: {e}")
                break

    def get_health_status(self) -> dict[str, Any]:
        """Get current health status of the events collector."""
        return {
            'component_id': self.circuit_breaker_id,
            'healthy': time.time() - self._last_health_check < self._health_check_interval * 2,
            'last_health_check': self._last_health_check,
            'circuit_breaker_registered': self._circuit_breaker_registered,
            'running': self.running,
            'sources_monitored': len(self.source_health),
            'metrics': self.metrics.copy(),
            'source_health_summary': {
                source: health['status'] for source, health in self.source_health.items()
            }
        }

    async def _retry_with_backoff(self, operation_func, operation_name: str, *args, **kwargs):
        """Execute external API operation with exponential backoff retry."""
        last_exception = None
        
        for attempt in range(self.retry_config['max_retries'] + 1):
            try:
                self.metrics['api_calls_made'] += 1
                result = await operation_func(*args, **kwargs)
                if attempt > 0:
                    logger.info(f"Retry succeeded for {operation_name} on attempt {attempt + 1}")
                return result
                
            except Exception as e:
                last_exception = e
                self.metrics['api_calls_retried'] += 1
                
                if attempt == self.retry_config['max_retries']:
                    self.metrics['api_calls_failed'] += 1
                    logger.error(f"Retry failed for {operation_name} after {attempt + 1} attempts: {e}")
                    break
                
                # Calculate exponential backoff delay
                delay_ms = min(
                    self.retry_config['base_delay_ms'] * (self.retry_config['exponential_base'] ** attempt),
                    self.retry_config['max_delay_ms']
                )
                
                logger.warning(f"Retry {attempt + 1}/{self.retry_config['max_retries']} for {operation_name} after {delay_ms}ms: {e}")
                await asyncio.sleep(delay_ms / 1000.0)
        
        # Re-raise the last exception if all retries failed
        if last_exception:
            raise last_exception
        else:
            raise RuntimeError(f"All retries failed for {operation_name} with no exception captured")

    async def start(self):
        logger.info("Starting Events Collector Agent...")
        self.running = True
        
        # Register circuit breaker with streaming bus
        await self._register_circuit_breaker()
        
        # Start health monitoring
        self._health_check_task = asyncio.create_task(self._health_monitor_loop())
        self._background_tasks.add(self._health_check_task)
        
        # Start all collection tasks
        if self.config.get('governance_enabled', True):
            task = asyncio.create_task(self._collect_governance())
            self.tasks.append(task)
            self._background_tasks.add(task)
        if self.config.get('token_unlocks_enabled', True):
            task = asyncio.create_task(self._collect_token_unlocks())
            self.tasks.append(task)
            self._background_tasks.add(task)
        if self.config.get('exchange_status_enabled', True):
            task = asyncio.create_task(self._collect_exchange_status())
            self.tasks.append(task)
            self._background_tasks.add(task)
        if self.config.get('github_releases_enabled', True):
            task = asyncio.create_task(self._collect_github_releases())
            self.tasks.append(task)
            self._background_tasks.add(task)
        
        # Start Kafka control message consumption
        control_task = asyncio.create_task(self._consume_control_messages())
        self.tasks.append(control_task)
        self._background_tasks.add(control_task)
        
        logger.info(f"Started {len(self.tasks)} collection tasks with enhanced integration monitoring: {self.circuit_breaker_id}")
    
    async def _consume_control_messages(self):
        """Consume control messages from Kafka topics for dynamic configuration."""
        control_topics = [
            "control.circuit_breaker",
            "control.config_update", 
            "control.event_sources",
            "control.calendar_update"
        ]
        
        logger.info(f"Events Collector: Starting control message consumption from topics: {control_topics}")
        
        try:
            await self.streaming_bus.subscribe(
                consumer_group="events_collector_control",
                topics=control_topics,
                handler=self._handle_control_message_wrapper
            )
                
        except asyncio.CancelledError:
            # Re-raise cancellation errors during shutdown
            raise
        except Exception as e:
            logger.error(f"Events Collector: Error in control message consumption: {e}")
            # Use the system circuit breaker to record failure
            await self.streaming_bus.record_component_failure(
                component_id="events_collector",
                cascade_failure=False,
                reason="events_collector_control_listener_failure",
                severity="medium"
            )
    
    def _handle_control_message_wrapper(self, topic: str, partition_key: str, 
                                      message: dict, headers: dict):
        """Wrapper to handle the subscribe callback signature."""
        # Schedule the async handler and store task reference
        task = asyncio.create_task(self._handle_control_message(topic, message))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
    
    async def _handle_control_message(self, topic: str, message: dict):
        """Handle control messages for dynamic behavior adjustment."""
        try:
            if topic == "control.circuit_breaker":
                # Handle circuit breaker commands
                component_id = message.get("component_id")
                if component_id == "events_collector" or component_id == "all":
                    action = message.get("action")
                    if action == "open":
                        logger.warning(f"Events Collector: Circuit breaker opened via control message")
                    elif action == "close":
                        logger.info(f"Events Collector: Circuit breaker closed via control message")
                        
            elif topic == "control.config_update":
                # Handle dynamic configuration updates
                component_id = message.get("component_id")
                if component_id == "events_collector" or component_id == "all":
                    config_updates = message.get("updates", {})
                    await self._apply_config_updates(config_updates)
                    
            elif topic == "control.event_sources":
                # Handle event source enable/disable
                source_name = message.get("source_name")
                action = message.get("action")
                if source_name and action:
                    if action == "enable":
                        logger.info(f"Events Collector: Enabling {source_name} collection")
                        # Could dynamically enable collection
                    elif action == "disable":
                        logger.warning(f"Events Collector: Disabling {source_name} collection")
                        # Could dynamically disable collection
                        
            elif topic == "control.calendar_update":
                # Handle priority event notifications
                event_type = message.get("event_type")
                urgency = message.get("urgency")
                if urgency == "high":
                    logger.info(f"Events Collector: High priority {event_type} event detected")
                    # Could trigger immediate collection
                        
        except Exception as e:
            logger.error(f"Events Collector: Error handling control message from {topic}: {e}")
    
    async def _apply_config_updates(self, updates: dict):
        """Apply dynamic configuration updates."""
        try:
            # Update collection intervals
            if "collection_interval_sec" in updates:
                logger.info(f"Events Collector: Updated collection_interval_sec to {updates['collection_interval_sec']}")
                
            # Update feature enables
            if "governance_enabled" in updates:
                self.config["governance_enabled"] = updates["governance_enabled"]
                logger.info(f"Events Collector: Updated governance_enabled to {updates['governance_enabled']}")
                
            if "token_unlocks_enabled" in updates:
                self.config["token_unlocks_enabled"] = updates["token_unlocks_enabled"]
                logger.info(f"Events Collector: Updated token_unlocks_enabled to {updates['token_unlocks_enabled']}")
                
            if "github_releases_enabled" in updates:
                self.config["github_releases_enabled"] = updates["github_releases_enabled"]
                logger.info(f"Events Collector: Updated github_releases_enabled to {updates['github_releases_enabled']}")
                
        except Exception as e:
            logger.error(f"Events Collector: Error applying config updates: {e}")

    async def stop(self):
        logger.info("Stopping Events Collector Agent...")
        self.running = False
        
        # Cancel all background tasks with timeout
        for task in self._background_tasks:
            if not task.done():
                task.cancel()
        
        # Cancel all control message tasks
        for task in self._tasks:
            if not task.done():
                task.cancel()
        
        # Cancel all tasks
        for task in self.tasks:
            task.cancel()
        
        # Wait for tasks to complete with timeout
        try:
            await asyncio.wait_for(
                asyncio.gather(*self.tasks, return_exceptions=True),
                timeout=10.0
            )
            logger.info("All event collection tasks completed successfully")
        except asyncio.TimeoutError:
            logger.warning("Some tasks did not complete within timeout")
        except Exception as e:
            logger.error(f"Error during task cleanup: {e}")
        
        # Clear task references
        self._background_tasks.clear()
        self.tasks.clear()
        self._health_check_task = None
        
        # Stop streaming bus
        try:
            await self.streaming_bus.graceful_shutdown()
        except Exception as e:
            logger.error(f"Error stopping streaming bus: {e}")
        
        logger.info(f"Events Collector Agent stopped - Final metrics: {self.metrics}")

    async def _collect_governance(self):
        """Collect governance proposals from Snapshot, Compound, etc."""
        interval_sec = self.config.get('governance_interval_sec', 300)  # 5 min
        next_tick = time.monotonic()
        while self.running:
            try:
                now = int(time.time() * 1_000_000)
                capture_now = int(time.time() * 1_000_000)
                
                # Snapshot governance
                snapshot_spaces = self.config.get('snapshot_spaces', [])
                async with HttpClient(streaming_bus=self.streaming_bus,
                                      rate_domain="events.snapshot") as client:
                    # Parallelize snapshot queries
                    tasks = []
                    for space in snapshot_spaces:
                        query = {
                            "query": """
                            query Proposals($space: String!, $first: Int!) {
                              proposals(
                                first: $first
                                where: { space: $space }
                                orderBy: "created"
                                orderDirection: desc
                              ) {
                                id
                                title
                                body
                                start
                                end
                                state
                                author
                                space { id name }
                              }
                            }
                            """,
                            "variables": {"space": space, "first": 20}
                        }
                        headers = {"Content-Type": "application/json"}
                        tasks.append(client.post_json("https://hub.snapshot.org/graphql", query, headers))
                    
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    for space, result in zip(snapshot_spaces, results):
                        try:
                            if isinstance(result, Exception):
                                logger.error({"event": "snapshot_fetch_error", "space": space, "error": str(result)})
                                continue
                            
                            if not result or not isinstance(result, dict):
                                continue
                                
                            # Guard for GraphQL error payloads
                            if "errors" in result:
                                logger.error({"event": "graphql_errors", "space": space, "errors": str(result['errors'])[:300]})
                                continue
                            
                            # Guard for partial data shapes under outages
                            data_section = result.get("data")
                            if not data_section or not isinstance(data_section, dict):
                                logger.warning({"event": "partial_data", "space": space, "issue": "missing_data_section"})
                                continue
                                
                            proposals = data_section.get("proposals")
                            if proposals is None:
                                logger.warning({"event": "partial_data", "space": space, "issue": "missing_proposals_field"})
                                continue
                                
                            if not isinstance(proposals, list):
                                logger.warning({"event": "partial_data", "space": space, "issue": "proposals_not_list", "type": str(type(proposals))})
                                continue
                            
                            for proposal in proposals:
                                try:
                                    # Normalize event type and status
                                    normalized_event_type = self.classifier.normalize_event_type(
                                        "governance_proposal", "snapshot", proposal
                                    )
                                    
                                    event = CalendarEvent(
                                        event_type=normalized_event_type,
                                        title=proposal["title"],
                                        description=proposal.get("body", "")[:500],  # Truncate
                                        start_time_utc_us=_normalize_timestamp(proposal["start"], now),
                                        end_time_utc_us=_normalize_timestamp(proposal["end"], None),
                                        source="snapshot",
                                        source_id=proposal["id"],
                                        status=proposal["state"],
                                        metadata={
                                            "space": proposal["space"]["name"],
                                            "author": proposal["author"]
                                        },
                                        capture_timestamp_utc_us=capture_now
                                    )
                                    
                                    # Enhanced status update validation
                                    status_key = ("snapshot", proposal["id"])
                                    last_status = self.last_status.get(status_key)
                                    current_status = proposal["state"]
                                    
                                    # Validate status transition for governance proposals
                                    is_valid_transition = True
                                    if last_status and last_status != current_status:
                                        is_valid_transition = self.classifier.validate_status_transition(
                                            last_status, current_status, event.event_type
                                        )
                                        if not is_valid_transition:
                                            logger.warning(f"Invalid status transition for {proposal['id']}: "
                                                         f"{last_status} -> {current_status}")
                                    
                                    is_status_update = last_status and last_status != current_status
                                    
                                    if (is_status_update or 
                                        not self.duplicate_detector.is_duplicate('calendar', event.get_hash(), key=f"snapshot_{space}")):
                                        
                                        # Update status tracking only if transition is valid
                                        if is_valid_transition:
                                            self.last_status[status_key] = current_status
                                        
                                        # Enhanced publishing with validation
                                        partition_key = f"governance_{space}"
                                        headers = {
                                            "data_type": "governance", 
                                            "source": "snapshot", 
                                            "space": space,
                                            "event_type": event.event_type,
                                            "status": event.status
                                        }
                                        
                                        # Add transition validation info
                                        if is_status_update:
                                            headers["status_update"] = "true"
                                            headers["previous_status"] = last_status or "unknown"
                                            headers["valid_transition"] = str(is_valid_transition).lower()
                                        
                                        # Use enhanced publishing method
                                        success = await self._publish_event(event, partition_key, headers)
                                        
                                        if success:
                                            # Local queue fallback with proper error handling
                                            try:
                                                q = self.output_queues['calendar']
                                                if q.full():
                                                    try: 
                                                        q.get_nowait()
                                                        logger.debug("Dropped old governance event from full queue")
                                                    except asyncio.QueueEmpty: 
                                                        pass
                                                q.put_nowait(event)
                                                logger.debug("Enqueued governance event to local queue")
                                            except asyncio.QueueFull:
                                                logger.warning("Failed to enqueue governance event - queue full")
                                            except Exception as queue_e:
                                                logger.warning(f"Failed to enqueue governance event to local queue: {queue_e}")
                                        
                                except Exception as row_exc:
                                    logger.warning(f"Bad governance proposal: {row_exc} | {str(proposal)[:300]}")
                                    continue
                                    
                        except Exception as e:
                            logger.error(f"Error processing governance from {space}: {e}")
                            
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in governance collection: {e}")
            
            # Drift-free cadence
            next_tick += interval_sec
            sleep_for = max(0, next_tick - time.monotonic())
            await asyncio.sleep(sleep_for)

    async def _collect_token_unlocks(self):
        """Collect token unlock events from various sources."""
        interval_sec = self.config.get('token_unlocks_interval_sec', 3600)  # 1 hour
        next_tick = time.monotonic()
        while self.running:
            try:
                now = int(time.time() * 1_000_000)
                capture_now = int(time.time() * 1_000_000)
                
                # Example: Token unlocks from a hypothetical API
                unlock_api_url = self.config.get('token_unlocks_api_url')
                if unlock_api_url:
                    async with HttpClient(streaming_bus=self.streaming_bus,
                                          rate_domain="events.token_unlocks") as client:
                        data = await client.get_json(unlock_api_url)
                        if data and "unlocks" in data:
                            for unlock in data["unlocks"]:
                                try:
                                    event = CalendarEvent(
                                        event_type="token_unlock",
                                        title=f"{unlock.get('token', 'Token')} Unlock",
                                        description=f"Unlock of {unlock.get('amount', 'unknown')} tokens",
                                        start_time_utc_us=_normalize_timestamp(unlock.get("unlock_date"), now),
                                        source="token_unlocks_api",
                                        source_id=unlock.get("id", ""),
                                        status="active",
                                        metadata={
                                            "token": unlock.get("token"),
                                            "amount": unlock.get("amount"),
                                            "recipient": unlock.get("recipient")
                                        },
                                        capture_timestamp_utc_us=capture_now
                                    )
                                    
                                    if not self.duplicate_detector.is_duplicate('calendar', event.get_hash(), key="token_unlocks"):
                                        # Streaming Bus: Publish to raw_data.offchain_events
                                        try:
                                            event_data = {
                                                "source": "token_unlocks",
                                                "event_type": event.event_type,
                                                "timestamp": event.start_time_utc_us,
                                                "capture_timestamp": event.capture_timestamp_utc_us,
                                                "title": event.title,
                                                "description": event.description[:500] if event.description else None,
                                                "status": event.status,
                                                "source_id": event.source_id,
                                                "extra": event.metadata
                                            }
                                            
                                            await self.streaming_bus.publish_with_headers(
                                                topic="raw_data.offchain_events",
                                                partition_key="token_unlocks",
                                                payload=event_data,
                                                headers={"data_type": "token_unlocks", "source": "token_unlocks_api"}
                                            )
                                        except Exception as e:
                                            logger.warning(f"Failed to publish token unlock event to streaming bus: {e}")
                                        
                                        # Local queue fallback with proper error handling
                                        try:
                                            q = self.output_queues['calendar']
                                            if q.full():
                                                try: 
                                                    q.get_nowait()
                                                    logger.debug("Dropped old token unlock event from full queue")
                                                except asyncio.QueueEmpty: 
                                                    pass
                                            q.put_nowait(event)
                                            logger.debug("Enqueued token unlock event to local queue")
                                        except asyncio.QueueFull:
                                            logger.warning("Failed to enqueue token unlock event - queue full")
                                        except Exception as queue_e:
                                            logger.warning(f"Failed to enqueue token unlock event to local queue: {queue_e}")
                                except Exception as row_exc:
                                    logger.warning(f"Bad token unlock: {row_exc} | {str(unlock)[:300]}")
                                    continue
                                    
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in token unlocks collection: {e}")
            
            # Drift-free cadence
            next_tick += interval_sec
            sleep_for = max(0, next_tick - time.monotonic())
            await asyncio.sleep(sleep_for)

    async def _collect_exchange_status(self):
        """Collect exchange maintenance and status events."""
        interval_sec = self.config.get('exchange_status_interval_sec', 600)  # 10 min
        next_tick = time.monotonic()
        while self.running:
            try:
                now = int(time.time() * 1_000_000)
                capture_now = int(time.time() * 1_000_000)
                
                # Exchange status from multiple sources
                exchanges = []
                
                # Coinbase status
                if self.config.get('coinbase_status_enabled', False):
                    exchanges.append(('coinbase', 'https://status.coinbase.com/api/v2/incidents.json'))
                
                # Gemini status
                if self.config.get('gemini_status_enabled', False):
                    exchanges.append(('gemini', 'https://status.gemini.com/api/v2/incidents.json'))
                
                # Add more exchanges as needed
                # if self.config.get('kraken_status_enabled', False):
                #     exchanges.append(('kraken', 'https://status.kraken.com/api/v2/incidents.json'))
                
                if exchanges:
                    async with HttpClient(streaming_bus=self.streaming_bus,
                                          rate_domain="events.exchange_status") as client:
                        # Parallelize exchange status queries
                        tasks = []
                        for exchange, url in exchanges:
                            tasks.append((exchange, client.get_json(url)))
                        
                        results = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)
                        
                        for (exchange, _), result in zip(tasks, results):
                            try:
                                if isinstance(result, Exception):
                                    logger.error(f"Error fetching {exchange} status: {result}")
                                    continue
                                
                                if not result or not isinstance(result, dict):
                                    continue
                                
                                # Process based on exchange type
                                if exchange == 'binance' and "data" in result and "catalogs" in result["data"]:
                                    for article in result["data"]["catalogs"]:
                                        try:
                                            event = CalendarEvent(
                                                event_type="exchange_maintenance",
                                                title=article.get("title", "Binance Update"),
                                                description=article.get("summary", "")[:500],
                                                start_time_utc_us=_normalize_timestamp(article.get("releaseDate"), now),
                                                source="binance",
                                                source_id=str(article.get("id", "")),
                                                status="active",
                                                metadata={
                                                    "exchange": "binance",
                                                    "type": article.get("type")
                                                },
                                                capture_timestamp_utc_us=capture_now
                                            )
                                            
                                            # Check for status updates
                                            status_key = ("binance", str(article.get("id", "")))
                                            last_status = self.last_status.get(status_key)
                                            is_status_update = last_status and last_status != "active"
                                            
                                            if is_status_update or not self.duplicate_detector.is_duplicate('calendar', event.get_hash(), key="binance_status"):
                                                # Update status tracking
                                                self.last_status[status_key] = "active"
                                                
                                                # Streaming Bus: Publish to raw_data.offchain_events
                                                try:
                                                    event_data = {
                                                        "source": "exchange_status",
                                                        "exchange": "binance",
                                                        "event_type": event.event_type,
                                                        "timestamp": event.start_time_utc_us,
                                                        "capture_timestamp": event.capture_timestamp_utc_us,
                                                        "title": event.title,
                                                        "description": event.description[:500] if event.description else None,
                                                        "status": event.status,
                                                        "source_id": event.source_id,
                                                        "extra": event.metadata
                                                    }
                                                    
                                                    await self.streaming_bus.publish_with_headers(
                                                        topic="raw_data.offchain_events",
                                                        partition_key="exchange_binance",
                                                        payload=event_data,
                                                        headers={"data_type": "exchange_status", "exchange": "binance"}
                                                    )
                                                except Exception as e:
                                                    logger.warning(f"Failed to publish binance status event to streaming bus: {e}")
                                                
                                                # Local queue fallback with proper error handling
                                                try:
                                                    q = self.output_queues['calendar']
                                                    if q.full():
                                                        try: 
                                                            q.get_nowait()
                                                            logger.debug("Dropped old exchange status event from full queue")
                                                        except asyncio.QueueEmpty: 
                                                            pass
                                                    q.put_nowait(event)
                                                    logger.debug("Enqueued exchange status event to local queue")
                                                except asyncio.QueueFull:
                                                    logger.warning("Failed to enqueue exchange status event - queue full")
                                                except Exception as queue_e:
                                                    logger.warning(f"Failed to enqueue exchange status event to local queue: {queue_e}")
                                        except Exception as row_exc:
                                            logger.warning(f"Bad {exchange} status: {row_exc} | {str(article)[:300]}")
                                            continue
                                
                                elif exchange == 'coinbase' and "incidents" in result:
                                    for incident in result["incidents"]:
                                        try:
                                            event = CalendarEvent(
                                                event_type="exchange_maintenance",
                                                title=incident.get("name", "Coinbase Incident"),
                                                description=incident.get("body", "")[:500],
                                                start_time_utc_us=_normalize_timestamp(incident.get("created_at"), now),
                                                source="coinbase",
                                                source_id=incident.get("id", ""),
                                                status=incident.get("status", "unknown"),
                                                metadata={
                                                    "exchange": "coinbase",
                                                    "impact": incident.get("impact"),
                                                    "shortlink": incident.get("shortlink")
                                                },
                                                capture_timestamp_utc_us=capture_now
                                            )
                                            
                                            # Check for status updates
                                            status_key = ("coinbase", incident.get("id", ""))
                                            last_status = self.last_status.get(status_key)
                                            current_status = incident.get("status", "unknown")
                                            is_status_update = last_status and last_status != current_status
                                            
                                            if is_status_update or not self.duplicate_detector.is_duplicate('calendar', event.get_hash(), key="coinbase_status"):
                                                # Update status tracking
                                                self.last_status[status_key] = current_status
                                                
                                                # Streaming Bus: Publish to raw_data.offchain_events
                                                try:
                                                    event_data = {
                                                        "source": "exchange_status",
                                                        "exchange": "coinbase",
                                                        "event_type": event.event_type,
                                                        "timestamp": event.start_time_utc_us,
                                                        "capture_timestamp": event.capture_timestamp_utc_us,
                                                        "title": event.title,
                                                        "description": event.description[:500] if event.description else None,
                                                        "status": event.status,
                                                        "source_id": event.source_id,
                                                        "extra": event.metadata
                                                    }
                                                    
                                                    await self.streaming_bus.publish_with_headers(
                                                        topic="raw_data.offchain_events",
                                                        partition_key="exchange_coinbase",
                                                        payload=event_data,
                                                        headers={"data_type": "exchange_status", "exchange": "coinbase"}
                                                    )
                                                except Exception as e:
                                                    logger.warning(f"Failed to publish coinbase status event to streaming bus: {e}")
                                                
                                                # Local queue fallback with proper error handling
                                                try:
                                                    q = self.output_queues['calendar']
                                                    if q.full():
                                                        try: 
                                                            q.get_nowait()
                                                            logger.debug("Dropped old coinbase incident event from full queue")
                                                        except asyncio.QueueEmpty: 
                                                            pass
                                                    q.put_nowait(event)
                                                    logger.debug("Enqueued coinbase incident event to local queue")
                                                except asyncio.QueueFull:
                                                    logger.warning("Failed to enqueue coinbase incident event - queue full")
                                                except Exception as queue_e:
                                                    logger.warning(f"Failed to enqueue coinbase incident event to local queue: {queue_e}")
                                        except Exception as row_exc:
                                            logger.warning(f"Bad {exchange} incident: {row_exc} | {str(incident)[:300]}")
                                            continue
                                
                                elif exchange == 'gemini' and "incidents" in result:
                                    for incident in result["incidents"]:
                                        try:
                                            event = CalendarEvent(
                                                event_type="exchange_maintenance",
                                                title=incident.get("name", "Gemini Incident"),
                                                description=incident.get("body", "")[:500],
                                                start_time_utc_us=_normalize_timestamp(incident.get("created_at"), now),
                                                source="gemini",
                                                source_id=incident.get("id", ""),
                                                status=incident.get("status", "unknown"),
                                                metadata={
                                                    "exchange": "gemini",
                                                    "impact": incident.get("impact"),
                                                    "shortlink": incident.get("shortlink")
                                                },
                                                capture_timestamp_utc_us=capture_now
                                            )
                                            
                                            # Check for status updates
                                            status_key = ("gemini", incident.get("id", ""))
                                            last_status = self.last_status.get(status_key)
                                            current_status = incident.get("status", "unknown")
                                            is_status_update = last_status and last_status != current_status
                                            
                                            if is_status_update or not self.duplicate_detector.is_duplicate('calendar', event.get_hash(), key="gemini_status"):
                                                # Update status tracking
                                                self.last_status[status_key] = current_status
                                                
                                                # Streaming Bus: Publish to raw_data.offchain_events
                                                try:
                                                    event_data = {
                                                        "source": "exchange_status",
                                                        "exchange": "gemini",
                                                        "event_type": event.event_type,
                                                        "timestamp": event.start_time_utc_us,
                                                        "capture_timestamp": event.capture_timestamp_utc_us,
                                                        "title": event.title,
                                                        "description": event.description[:500] if event.description else None,
                                                        "status": event.status,
                                                        "source_id": event.source_id,
                                                        "extra": event.metadata
                                                    }
                                                    
                                                    await self.streaming_bus.publish_with_headers(
                                                        topic="raw_data.offchain_events",
                                                        partition_key="exchange_gemini",
                                                        payload=event_data,
                                                        headers={"data_type": "exchange_status", "exchange": "gemini"}
                                                    )
                                                except Exception as e:
                                                    logger.warning(f"Failed to publish gemini status event to streaming bus: {e}")
                                                
                                                # Local queue fallback with proper error handling
                                                try:
                                                    q = self.output_queues['calendar']
                                                    if q.full():
                                                        try: 
                                                            q.get_nowait()
                                                            logger.debug("Dropped old gemini incident event from full queue")
                                                        except asyncio.QueueEmpty: 
                                                            pass
                                                    q.put_nowait(event)
                                                    logger.debug("Enqueued gemini incident event to local queue")
                                                except asyncio.QueueFull:
                                                    logger.warning("Failed to enqueue gemini incident event - queue full")
                                                except Exception as queue_e:
                                                    logger.warning(f"Failed to enqueue gemini incident event to local queue: {queue_e}")
                                        except Exception as row_exc:
                                            logger.warning(f"Bad {exchange} incident: {row_exc} | {str(incident)[:300]}")
                                            continue
                                            
                            except Exception as e:
                                logger.error(f"Error processing {exchange} status: {e}")
                                    
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in exchange status collection: {e}")
            
            # Drift-free cadence
            next_tick += interval_sec
            sleep_for = max(0, next_tick - time.monotonic())
            await asyncio.sleep(sleep_for)

    async def _collect_github_releases(self):
        """Collect GitHub releases for important repos."""
        interval_sec = self.config.get('github_releases_interval_sec', 1800)  # 30 min
        github_token = self.config.get('github_token') or os.environ.get('GITHUB_TOKEN')
        next_tick = time.monotonic()
        while self.running:
            try:
                now = int(time.time() * 1_000_000)
                capture_now = int(time.time() * 1_000_000)
                
                repos = self.config.get('github_repos', [])  # e.g. ['ethereum/go-ethereum', 'compound-finance/compound-protocol']
                
                async with HttpClient(streaming_bus=self.streaming_bus,
                                      rate_domain="events.github") as client:
                    # Parallelize GitHub repo queries
                    tasks = []
                    for repo in repos:
                        url = f"https://api.github.com/repos/{repo}/releases?per_page=10"
                        headers = {
                            'Accept': 'application/vnd.github+json'
                        }
                        if github_token:
                            headers['Authorization'] = f'token {github_token}'
                        
                        # Add ETag for caching
                        etag = self.etag_by_repo.get(repo)
                        if etag:
                            headers['If-None-Match'] = etag
                            
                        tasks.append((repo, client.get_json_with_headers(url, headers)))
                    
                    # Execute all requests in parallel
                    repo_tasks = [(repo, task) for repo, task in tasks]
                    results = await asyncio.gather(*[task for _, task in repo_tasks], return_exceptions=True)
                    
                    for (repo, _), result in zip(repo_tasks, results):
                        try:
                            if isinstance(result, Exception):
                                logger.error(f"Error fetching GitHub repo {repo}: {result}")
                                continue
                                
                            if not result or not isinstance(result, tuple) or len(result) != 2:
                                continue
                                
                            data, resp_headers = result
                            
                            # Update ETag cache (case-insensitive)
                            etag_value = None
                            for key, value in resp_headers.items():
                                if key.lower() == 'etag':
                                    etag_value = value
                                    break
                            if etag_value:
                                self.etag_by_repo[repo] = etag_value
                            
                            if not data:
                                # Could be 304 Not Modified, which is fine
                                continue
                                
                            if not isinstance(data, list):
                                logger.warning(f"Unexpected GitHub API response format for {repo}: {str(data)[:300]}")
                                continue
                                
                            for release in data:
                                try:
                                    # Check if source is healthy before processing
                                    if not self._is_source_healthy('github'):
                                        logger.debug("Skipping GitHub releases - circuit breaker open")
                                        break
                                    
                                    # Normalize event type
                                    normalized_event_type = self.classifier.normalize_event_type(
                                        "github_release", "github", release
                                    )
                                    
                                    event = CalendarEvent(
                                        event_type=normalized_event_type,
                                        title=f"{repo}: {release.get('name', release.get('tag_name', 'Release'))}",
                                        description=release.get("body", "")[:500],
                                        start_time_utc_us=_normalize_timestamp(release.get("published_at"), now),
                                        source="github",
                                        source_id=str(release.get("id", "")),
                                        status="completed" if not release.get("draft") else "draft",
                                        metadata={
                                            "repo": repo,
                                            "tag_name": release.get("tag_name"),
                                            "prerelease": release.get("prerelease", False),
                                            "author": release.get("author", {}).get("login")
                                        },
                                        capture_timestamp_utc_us=capture_now
                                    )
                                    
                                    # Enhanced status update tracking
                                    status_key = ("github", str(release.get("id", "")))
                                    last_status = self.last_status.get(status_key)
                                    current_status = "completed" if not release.get("draft") else "draft"
                                    is_status_update = last_status and last_status != current_status
                                    
                                    if (is_status_update or 
                                        not self.duplicate_detector.is_duplicate('calendar', event.get_hash(), key=f"github_{repo}")):
                                        
                                        # Update status tracking
                                        self.last_status[status_key] = current_status
                                        
                                        # Enhanced publishing
                                        partition_key = f"github_{repo.replace('/', '_')}"
                                        headers = {
                                            "data_type": "github_releases",
                                            "source": "github", 
                                            "repo": repo,
                                            "event_type": event.event_type,
                                            "status": event.status
                                        }
                                        
                                        # Add release-specific metadata
                                        if release.get("prerelease"):
                                            headers["prerelease"] = "true"
                                        if release.get("draft"):
                                            headers["draft"] = "true"
                                        if release.get("tag_name"):
                                            headers["tag_name"] = release["tag_name"]
                                        
                                        # Use enhanced publishing method
                                        success = await self._publish_event(event, partition_key, headers)
                                        
                                        if success:
                                            # Local queue fallback with proper error handling
                                            try:
                                                q = self.output_queues['calendar']
                                                if q.full():
                                                    try: 
                                                        q.get_nowait()
                                                        logger.debug("Dropped old github release event from full queue")
                                                    except asyncio.QueueEmpty: 
                                                        pass
                                                q.put_nowait(event)
                                                logger.debug("Enqueued github release event to local queue")
                                            except asyncio.QueueFull:
                                                logger.warning("Failed to enqueue github release event - queue full")
                                            except Exception as queue_e:
                                                logger.warning(f"Failed to enqueue github release event to local queue: {queue_e}")
                                        
                                except Exception as row_exc:
                                    logger.warning(f"Bad GitHub release: {row_exc} | {str(release)[:300]}")
                                    continue
                                    
                        except Exception as e:
                            logger.error(f"Error processing GitHub releases from {repo}: {e}")
                            
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in GitHub releases collection: {e}")
            
            # Drift-free cadence
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
        # Collection intervals (enhanced with priority-based processing)
        'governance_interval_sec': 180,  # Faster for high-priority governance
        'token_unlocks_interval_sec': 3600,
        'exchange_status_interval_sec': 300,  # More frequent for trading impact
        'github_releases_interval_sec': 1800,
        
        # Enhanced source configuration
        'snapshot_spaces': ['compound', 'aave.eth', 'uniswap', 'ens.eth', 'gitcoin.eth'],
        'github_repos': [
            'ethereum/go-ethereum', 
            'compound-finance/compound-protocol', 
            'Uniswap/v3-core',
            'aave/aave-protocol-v2'
        ],
        'binance_status_enabled': True,
        'coinbase_status_enabled': True,
        
        # Enhanced queue and correlation settings
        'calendar_queue_size': 10000,  # Increased for enhanced processing
        'correlation_window_hours': 48,  # Extended for better event correlation
        
        # Circuit breaker configuration
        'source_health_check_interval': 300,  # 5 minutes
        'max_consecutive_failures': 3,
        
        # Enhanced validation settings
        'enable_strict_validation': True,
        'log_validation_issues': True,
        
        # GitHub API rate limiting
        'github_token': None,  # Set to your GitHub token for higher rate limits
        
        # Priority processing
        'priority_queue_enabled': True,
        'max_priority_events_per_batch': 50
    }
    
    logging.basicConfig(level=logging.INFO)
    agent = EventsCollectorAgent(config)
    try:
        await agent.start()
        while True:
            event = await agent.get_output_data('calendar', timeout=5.0)
            if event:
                # Enhanced event display with validation and correlation info
                priority_symbols = {1: "🔴", 2: "🟡", 3: "🟢", 4: "🔵", 5: "⚪"}
                priority = agent.classifier.classify_event_priority(event)
                priority_symbol = priority_symbols.get(priority, "⚪")
                
                validation_status = "✓"
                is_valid, _ = agent.validator.validate_event(event)
                if not is_valid:
                    validation_status = "⚠️"
                
                related_events = await agent.correlator.find_related_events(event)
                related_count = len(related_events)
                correlation_info = f" ({related_count} related)" if related_count > 0 else ""
                
                print(f"{priority_symbol}{validation_status} {event.event_type} | {event.title} | "
                      f"{event.source} | {event.status}{correlation_info}")
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        await agent.stop()

if __name__ == "__main__":
    asyncio.run(main())
