#!/usr/bin/env python3
"""
Streaming Bus Infrastructure - Redpanda/Kafka
High-throughput, low-latency message bus for HFT data plane.

Topic Structure:
- raw_data.market.{trades,book,funding,oi}.{venue}.{instrument}
- clean.market.{trades,book,funding,oi}.{venue}.{instrument}
- features.{vector,vol_surface,carry_basis,onchain,events,regime}
- labels.{tb,forward}
- signals.{raw,calibrated}
- intents.trade
- orders.{place,fills}
- incidents.{schema,freshness,anomaly,leakage,risk}
- control.{circuit_breaker,approval_request}
- telemetry.{exec,performance}
"""

import asyncio
import json
import logging
import time
import ssl
import threading
from typing import Dict, List, Optional, Any, Callable, Union, Deque, Set, Tuple
from dataclasses import dataclass, asdict, field
from enum import Enum
from collections import defaultdict, deque
import hashlib
import statistics
import uuid
import struct
import random

# Adaptive rate limiting
from infra.bus.adaptive_rate_limiter import AdaptiveRateLimiterPool

# Import centralized Prometheus metrics
try:
    from infra.monitoring.prometheus_metrics import MetricsCollector
    _metrics_collector = MetricsCollector()
    METRICS_AVAILABLE = True
except ImportError:
    _metrics_collector = None
    METRICS_AVAILABLE = False

# Kafka imports with proper fallbacks
try:
    from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
    from aiokafka.admin import AIOKafkaAdminClient
    from aiokafka.helpers import create_ssl_context
    from kafka.errors import KafkaError
    from kafka.admin.new_topic import NewTopic
    from kafka.admin.config_resource import ConfigResource, ConfigResourceType
    KAFKA_AVAILABLE = True
except ImportError:
    # Create stub classes to avoid "possibly unbound" errors
    KAFKA_AVAILABLE = False
    AIOKafkaProducer = None
    AIOKafkaConsumer = None
    AIOKafkaAdminClient = None
    create_ssl_context = None
    KafkaError = Exception
    NewTopic = None
    ConfigResource = None
    ConfigResourceType = None
    print("⚠️  aiokafka not installed. Install with: pip install aiokafka kafka-python")

# Compression library detection with graceful fallbacks
COMPRESSION_LIBRARIES = {}

try:
    import lz4
    COMPRESSION_LIBRARIES['lz4'] = True
    print("✅ LZ4 compression available")
except ImportError:
    COMPRESSION_LIBRARIES['lz4'] = False
    print("⚠️  LZ4 compression not available - install with: pip install lz4")

try:
    import snappy
    COMPRESSION_LIBRARIES['snappy'] = True  
    print("✅ Snappy compression available")
except ImportError:
    COMPRESSION_LIBRARIES['snappy'] = False
    print("⚠️  Snappy compression not available - install with: pip install python-snappy")

try:
    import zstandard
    COMPRESSION_LIBRARIES['zstd'] = True
    print("✅ ZSTD compression available")
except ImportError:
    COMPRESSION_LIBRARIES['zstd'] = False
    print("⚠️  ZSTD compression not available - install with: pip install zstandard")

try:
    import gzip
    COMPRESSION_LIBRARIES['gzip'] = True
    print("✅ GZIP compression available (built-in)")
except ImportError:
    COMPRESSION_LIBRARIES['gzip'] = False

def get_best_available_compression() -> str:
    """
    Get the best available compression algorithm based on installed libraries.
    Priority: zstd > lz4 > snappy > gzip > none
    """
    if COMPRESSION_LIBRARIES.get('zstd', False):
        return 'zstd'
    elif COMPRESSION_LIBRARIES.get('lz4', False):
        return 'lz4'
    elif COMPRESSION_LIBRARIES.get('snappy', False):
        return 'snappy'
    elif COMPRESSION_LIBRARIES.get('gzip', False):
        return 'gzip'
    else:
        return 'none'

def validate_compression_type(compression_type: str) -> str:
    """
    Validate compression type and fallback to available alternative if needed.
    """
    # Convert to lowercase string
    compression_type = str(compression_type).lower()
    
    if compression_type == 'none':
        return 'none'
        
    # Check if requested compression is available
    if COMPRESSION_LIBRARIES.get(compression_type, False):
        return compression_type
    
    # Fallback to best available
    fallback = get_best_available_compression()
    if fallback != compression_type:
        print(f"⚠️  Compression '{compression_type}' not available, falling back to '{fallback}'")
    
    return fallback

logger = logging.getLogger(__name__)


@dataclass
class RateDomainState:
    """Runtime state for a single rate-limited domain."""
    domain: str
    qps: float
    burst: float
    tokens: float
    last_refill: float
    recent_borrows: Deque[Tuple[float, float]] = field(default_factory=lambda: deque(maxlen=1024))
    wait_events: Deque[float] = field(default_factory=lambda: deque(maxlen=256))
    throttled_events: int = 0
    last_update_ts: float = field(default_factory=lambda: time.time())


# ==========================================================================
# ADAPTIVE RATE LIMITING (AIMD Algorithm)
# ==========================================================================

class AdaptiveRateLimitContext:
    """
    Async context manager for adaptive rate limiting using AIMD algorithm.
    
    Automatically acquires permits before the protected block and records
    success/failure after, enabling the rate limiter to adapt dynamically.
    """
    
    def __init__(self, pool, domain: str, timeout: Optional[float] = None):
        self.pool = pool
        self.domain = domain
        self.timeout = timeout
        self.limiter = None
        self.start_time = None
    
    async def __aenter__(self):
        if not self.pool:
            # Rate limiting disabled
            return self
        
        # Get or create limiter for this domain
        self.limiter = await self.pool.get_limiter(
            domain=self.domain,
            initial_rate=10.0,  # Conservative start
            max_rate=100.0      # Can be tuned per domain
        )
        
        # Acquire permission
        self.start_time = time.time()
        success = await self.limiter.acquire(timeout=self.timeout)
        
        if not success:
            raise asyncio.TimeoutError(
                f"Rate limit timeout for domain '{self.domain}' after {self.timeout}s"
            )
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if not self.limiter or not self.start_time:
            return False
        
        latency_ms = (time.time() - self.start_time) * 1000.0
        
        # Record outcome for adaptive adjustment
        if exc_type is None:
            # Success
            self.limiter.record_success(latency_ms=latency_ms)
        elif exc_type == asyncio.TimeoutError:
            # Timeout
            self.limiter.record_timeout()
        else:
            # Other error
            self.limiter.record_error()
        
        return False

def generate_uuidv7() -> str:
    """
    Generate a UUIDv7 with time-ordered properties for institutional traceability.
    
    UUIDv7 format: 48-bit timestamp + 12-bit version/random + 62-bit random
    Provides time-ordered correlation IDs for cross-system tracing.
    """
    # Get current timestamp in milliseconds
    timestamp_ms = int(time.time() * 1000)
    
    # 48-bit timestamp (6 bytes)
    timestamp_bytes = struct.pack('>Q', timestamp_ms)[-6:]
    
    # 16-bit version (4) + random (12 bits)
    version_random = 0x7000 | (random.getrandbits(12))
    version_bytes = struct.pack('>H', version_random)
    
    # 64-bit random (8 bytes) with variant bits
    random_high = 0x8000 | (random.getrandbits(15))  # Set variant bits 10
    random_low = random.getrandbits(48)
    random_bytes = struct.pack('>HQ', random_high, random_low)[-8:]
    
    # Combine all bytes
    uuid_bytes = timestamp_bytes + version_bytes + random_bytes
    
    # Convert to standard UUID format
    uuid_obj = uuid.UUID(bytes=uuid_bytes)
    return str(uuid_obj)

@dataclass
class CanonicalHeaders:
    """
    Institutional-grade message headers for complete data lineage and traceability.
    Every message MUST carry these headers for audit compliance.
    """
    timestamp_utc_us: int              # Event timestamp in UTC microseconds
    source_id: str                     # Format: component.instance (e.g., "exchange_connector.001")
    sequence_number: int               # Monotonic sequence per source_id
    correlation_id: str               # UUIDv7 for time-ordered request tracing across components
    content_hash: str                 # SHA-256 hash of payload for integrity verification
    partition_semantic: str           # Business semantic for partition validation (e.g., "BTCUSDT")
    ingestion_timestamp_utc_us: int   # Bus ingestion timestamp
    producer_version: str             # Producer component version for compatibility
    schema_version: str               # Data schema version for institutional compliance
    time_alignment_id: Optional[str]  # Optional time alignment identifier for cross-venue synchronization
    
    @classmethod
    def create(cls, source_id: str, sequence_number: int, payload: Dict[str, Any], 
               partition_semantic: str, correlation_id: Optional[str] = None,
               producer_version: str = "1.0.0", schema_version: str = "1.0",
               time_alignment_id: Optional[str] = None) -> 'CanonicalHeaders':
        """Create canonical headers with auto-generated fields."""
        now_us = int(time.time_ns() // 1000)
        content_hash = hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode('utf-8')
        ).hexdigest()
        
        return cls(
            timestamp_utc_us=now_us,
            source_id=source_id,
            sequence_number=sequence_number,
            correlation_id=correlation_id or generate_uuidv7(),
            content_hash=content_hash,
            partition_semantic=partition_semantic,
            ingestion_timestamp_utc_us=now_us,
            producer_version=producer_version,
            schema_version=schema_version,
            time_alignment_id=time_alignment_id
        )
    
    def to_kafka_headers(self) -> List[tuple]:
        """Convert to Kafka header format."""
        headers = [
            ("timestamp_utc_us", str(self.timestamp_utc_us).encode('utf-8')),
            ("source_id", self.source_id.encode('utf-8')),
            ("sequence_number", str(self.sequence_number).encode('utf-8')),
            ("correlation_id", self.correlation_id.encode('utf-8')),
            ("content_hash", self.content_hash.encode('utf-8')),
            ("partition_semantic", self.partition_semantic.encode('utf-8')),
            ("ingestion_timestamp_utc_us", str(self.ingestion_timestamp_utc_us).encode('utf-8')),
            ("producer_version", self.producer_version.encode('utf-8')),
            ("schema_version", self.schema_version.encode('utf-8'))
        ]
        
        # Add optional time_alignment_id if present
        if self.time_alignment_id:
            headers.append(("time_alignment_id", self.time_alignment_id.encode('utf-8')))
            
        return headers
    
    @classmethod
    def from_kafka_headers(cls, headers: Dict[str, bytes]) -> 'CanonicalHeaders':
        """Parse from Kafka headers."""
        return cls(
            timestamp_utc_us=int(headers["timestamp_utc_us"].decode('utf-8')),
            source_id=headers["source_id"].decode('utf-8'),
            sequence_number=int(headers["sequence_number"].decode('utf-8')),
            correlation_id=headers["correlation_id"].decode('utf-8'),
            content_hash=headers["content_hash"].decode('utf-8'),
            partition_semantic=headers["partition_semantic"].decode('utf-8'),
            ingestion_timestamp_utc_us=int(headers["ingestion_timestamp_utc_us"].decode('utf-8')),
            producer_version=headers["producer_version"].decode('utf-8'),
            schema_version=headers.get("schema_version", b"1.0").decode('utf-8'),
            time_alignment_id=headers.get("time_alignment_id", b"").decode('utf-8') or None
        )
    
    def verify_integrity(self, payload: Dict[str, Any]) -> bool:
        """Verify payload matches content hash."""
        expected_hash = hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode('utf-8')
        ).hexdigest()
        return self.content_hash == expected_hash
    
    def verify_partition_semantic(self, partition_key: str) -> bool:
        """Verify partition key matches declared semantic."""
        return self.partition_semantic == partition_key


@dataclass
class MessageEnvelope:
    """
    Complete message envelope with canonical headers and payload.
    Provides institutional-grade message structure.
    """
    headers: CanonicalHeaders
    payload: Dict[str, Any]
    
    def validate(self) -> List[str]:
        """Validate message envelope. Returns list of validation errors."""
        errors = []
        
        # Verify content integrity
        if not self.headers.verify_integrity(self.payload):
            errors.append(f"Content hash mismatch for message {self.headers.correlation_id}")
        
        # Check required header fields
        if not self.headers.source_id:
            errors.append("source_id is required")
        if not self.headers.correlation_id:
            errors.append("correlation_id is required")
        if not self.headers.partition_semantic:
            errors.append("partition_semantic is required")
            
        return errors


class CircuitBreakerState(Enum):
    """Circuit breaker states for component health management."""
    CLOSED = "closed"         # Normal operation
    OPEN = "open"            # Circuit tripped, blocking requests  
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class BreakerIntent:
    """Intent published by agents or systems requesting breaker state changes."""
    component_id: str
    intent: str  # e.g. "trip", "recover", "probe"
    reason: str
    severity: str = "medium"
    requested_by: str = "system"
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp_us: int = field(default_factory=lambda: int(time.time_ns() // 1000))


@dataclass
class CircuitBreakerConfig:
    """Configuration for component circuit breakers."""
    component_id: str                    # Unique component identifier
    failure_threshold: int = 5           # Failures before opening circuit
    recovery_timeout_us: int = 300_000_000  # 5 minutes recovery timeout
    half_open_max_calls: int = 3         # Max calls in half-open state
    success_threshold: int = 2           # Successes needed to close circuit
    dependency_components: Optional[List[str]] = None  # Components this depends on
    
    def __post_init__(self):
        if self.dependency_components is None:
            self.dependency_components = []


@dataclass 
class CircuitBreakerState_Instance:
    """Runtime state for a circuit breaker."""
    config: CircuitBreakerConfig
    state: CircuitBreakerState = CircuitBreakerState.CLOSED
    failure_count: int = 0
    success_count: int = 0  # For half-open state
    last_failure_time_us: Optional[int] = None
    opened_time_us: Optional[int] = None
    half_open_call_count: int = 0
    
    def _update_prometheus_state(self):
        """Update Prometheus gauge with current state."""
        state_value = {
            CircuitBreakerState.CLOSED: 0,
            CircuitBreakerState.HALF_OPEN: 1,
            CircuitBreakerState.OPEN: 2
        }[self.state]
        if METRICS_AVAILABLE and _metrics_collector:
            _metrics_collector.set_gauge(
                'circuit_breaker_state',
                state_value,
                labels={'component': self.config.component_id, 'breaker_type': ''}
            )
    
    def can_execute(self) -> bool:
        """Check if circuit allows execution."""
        now_us = int(time.time_ns() // 1000)
        
        if self.state == CircuitBreakerState.CLOSED:
            return True
        elif self.state == CircuitBreakerState.OPEN:
            # Check if recovery timeout has passed
            if (self.opened_time_us and 
                now_us - self.opened_time_us >= self.config.recovery_timeout_us):
                self.state = CircuitBreakerState.HALF_OPEN
                self._update_prometheus_state()
                self.half_open_call_count = 0
                self.success_count = 0
                return True
            return False
        elif self.state == CircuitBreakerState.HALF_OPEN:
            return self.half_open_call_count < self.config.half_open_max_calls
        
        return False
    
    def record_success(self):
        """Record successful execution."""
        now_us = int(time.time_ns() // 1000)
        
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self.state = CircuitBreakerState.CLOSED
                self._update_prometheus_state()
                self.failure_count = 0
                self.success_count = 0
                self.half_open_call_count = 0
        elif self.state == CircuitBreakerState.CLOSED:
            self.failure_count = max(0, self.failure_count - 1)  # Gradual recovery
    
    def record_failure(self):
        """Record failed execution."""
        now_us = int(time.time_ns() // 1000)
        self.last_failure_time_us = now_us

        if self.state == CircuitBreakerState.CLOSED:
            self.failure_count += 1
            if self.failure_count >= self.config.failure_threshold:
                self.state = CircuitBreakerState.OPEN
                self._update_prometheus_state()
                self.opened_time_us = now_us
        elif self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.OPEN
            self._update_prometheus_state()
            self.opened_time_us = now_us
            self.failure_count += 1

    def record_call_attempt(self):
        """Record call attempt in half-open state."""
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.half_open_call_count += 1

    def force_open(self):
        now_us = int(time.time_ns() // 1000)
        self.state = CircuitBreakerState.OPEN
        self._update_prometheus_state()
        self.opened_time_us = now_us

    def force_close(self):
        self.state = CircuitBreakerState.CLOSED
        self._update_prometheus_state()
        self.failure_count = 0
        self.success_count = 0
        self.half_open_call_count = 0

    def force_half_open(self):
        self.state = CircuitBreakerState.HALF_OPEN
        self._update_prometheus_state()
        self.success_count = 0
        self.half_open_call_count = 0


class SystemCircuitBreakerManager:
    """
    System-wide circuit breaker coordination for dependency-aware failures.
    Implements cascade failure prevention and coordinated recovery.
    """
    
    def __init__(self):
        self.breakers: Dict[str, CircuitBreakerState_Instance] = {}
        self.dependency_graph: Dict[str, List[str]] = {}  # component -> dependencies
        self.dependents_graph: Dict[str, List[str]] = {}  # component -> dependents
        self._lock = asyncio.Lock()
        self._last_intents: Dict[str, BreakerIntent] = {}

    async def register_component(self, config: CircuitBreakerConfig):
        """Register a component with its circuit breaker configuration."""
        async with self._lock:
            self.breakers[config.component_id] = CircuitBreakerState_Instance(config)
            
            # Build dependency graphs
            self.dependency_graph[config.component_id] = config.dependency_components.copy() if config.dependency_components else []
            
            # Update dependents graph
            if config.dependency_components:
                for dependency in config.dependency_components:
                    if dependency not in self.dependents_graph:
                        self.dependents_graph[dependency] = []
                    self.dependents_graph[dependency].append(config.component_id)
    
    async def can_component_execute(self, component_id: str) -> bool:
        """Check if component can execute considering dependencies."""
        async with self._lock:
            if component_id not in self.breakers:
                return True  # Unknown components allowed by default
            
            breaker = self.breakers[component_id]
            
            # Check own circuit breaker
            if not breaker.can_execute():
                return False
            
            # Check dependency circuit breakers  
            for dependency in self.dependency_graph.get(component_id, []):
                if dependency in self.breakers:
                    dep_breaker = self.breakers[dependency]
                    if not dep_breaker.can_execute():
                        logger.warning(f"Component {component_id} blocked due to dependency {dependency} circuit open")
                        return False
            
            return True
    
    async def record_component_success(self, component_id: str,
                                       reason: str = "success",
                                       severity: str = "low") -> Dict[str, Any]:
        """Record successful component execution."""
        async with self._lock:
            if component_id in self.breakers:
                breaker = self.breakers[component_id]
                breaker.record_success()
                intent = BreakerIntent(component_id=component_id,
                                       intent="recover",
                                       reason=reason,
                                       severity=severity,
                                       requested_by="system")
                self._last_intents[component_id] = intent
                return self._export_state(component_id)
        return {}

    async def record_component_failure(self, component_id: str, 
                                     cascade_to_dependents: bool = True,
                                     reason: str = "failure",
                                     severity: str = "high") -> Dict[str, Any]:
        """Record component failure and optionally cascade to dependents."""
        async with self._lock:
            if component_id not in self.breakers:
                return {}
            
            breaker = self.breakers[component_id]
            old_state = breaker.state
            breaker.record_failure()
            intent_type = "trip" if breaker.state == CircuitBreakerState.OPEN else "degrade"
            intent = BreakerIntent(component_id=component_id,
                                   intent=intent_type,
                                   reason=reason,
                                   severity=severity,
                                   requested_by="system")
            self._last_intents[component_id] = intent
            state_snapshot = self._export_state(component_id)
            
            # If circuit just opened, cascade to dependents
            if (cascade_to_dependents and 
                old_state != CircuitBreakerState.OPEN and 
                breaker.state == CircuitBreakerState.OPEN):
                
                await self._cascade_failure_to_dependents(component_id)
            return state_snapshot

    async def _cascade_failure_to_dependents(self, failed_component: str):
        """Cascade failure to dependent components."""
        dependents = self.dependents_graph.get(failed_component, [])
        for dependent in dependents:
            if dependent in self.breakers:
                    dep_breaker = self.breakers[dependent]
                    if dep_breaker.state == CircuitBreakerState.CLOSED:
                        logger.warning(f"Cascading failure from {failed_component} to {dependent}")
                        dep_breaker.state = CircuitBreakerState.OPEN
                        dep_breaker.opened_time_us = int(time.time_ns() // 1000)
                    
                    # Continue cascade
                    await self._cascade_failure_to_dependents(dependent)
    
    async def submit_manual_intent(self, intent: BreakerIntent) -> Dict[str, Any]:
        """Submit a manual breaker intent from agents or orchestrator."""
        async with self._lock:
            breaker = self.breakers.get(intent.component_id)
            if not breaker:
                return {}

            if intent.intent in {"trip", "open"}:
                breaker.force_open()
            elif intent.intent in {"recover", "close"}:
                breaker.force_close()
            elif intent.intent in {"probe", "half_open"}:
                breaker.force_half_open()

            self._last_intents[intent.component_id] = intent
            if breaker.state == CircuitBreakerState.OPEN:
                await self._cascade_failure_to_dependents(intent.component_id)
            return self._export_state(intent.component_id)

    async def get_system_health_status(self) -> Dict[str, Any]:
        """Get overall system health status."""
        async with self._lock:
            status = {
                "healthy_components": [],
                "degraded_components": [],
                "failed_components": [],
                "dependency_violations": [],
                "components": {comp: self._export_state(comp) for comp in self.breakers}
            }
            
            for component_id, breaker in self.breakers.items():
                if breaker.state == CircuitBreakerState.CLOSED:
                    status["healthy_components"].append(component_id)
                elif breaker.state == CircuitBreakerState.HALF_OPEN:
                    status["degraded_components"].append(component_id)
                else:
                    status["failed_components"].append(component_id)
            
            return status
    
    async def get_component_state(self, component_id: str) -> Dict[str, Any]:
        """Return the latest state snapshot for a specific component."""
        async with self._lock:
            return self._export_state(component_id)

    def _export_state(self, component_id: str) -> Dict[str, Any]:
        breaker = self.breakers.get(component_id)
        if not breaker:
            return {}
        intent = self._last_intents.get(component_id)
        return {
            "component_id": component_id,
            "state": breaker.state.value,
            "failure_count": breaker.failure_count,
            "success_count": breaker.success_count,
            "last_failure_time_us": breaker.last_failure_time_us,
            "opened_time_us": breaker.opened_time_us,
            "intent": intent.intent if intent else None,
            "intent_reason": intent.reason if intent else None,
            "intent_severity": intent.severity if intent else None,
            "intent_requested_by": intent.requested_by if intent else None,
            "intent_timestamp_us": intent.timestamp_us if intent else None,
            "metadata": intent.metadata if intent else {}
        }


class TopicType(Enum):
    """Standard topic types for the streaming bus."""
    RAW_DATA = "raw_data"
    CLEAN_DATA = "clean"
    FEATURES = "features"
    LABELS = "labels"
    SIGNALS = "signals"
    INTENTS = "intents"
    ORDERS = "orders"
    INCIDENTS = "incidents"
    CONTROL = "control"
    TELEMETRY = "telemetry"

class CompressionType(Enum):
    """Compression types for different data patterns."""
    NONE = "none"
    GZIP = "gzip"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    ZSTD = "zstd"  # Best for time series

@dataclass
class TopicConfig:
    """Configuration for streaming topics."""
    name: str
    partitions: int
    replication_factor: int
    retention_ms: int
    compression_type: CompressionType
    cleanup_policy: str  # "delete" or "compact"
    min_insync_replicas: int = 1  # Changed from 2 to 1 for single-node Kafka compatibility
    unclean_leader_election: bool = False
    
    # Performance tuning
    segment_ms: Optional[int] = None
    max_message_bytes: int = 1048576  # 1MB default
    
    # Special configs
    enable_idempotence: bool = True
    enable_compaction: bool = False

@dataclass
class TopicMetadata:
    """Extended topic metadata for registry."""
    config: TopicConfig
    data_type: str  # "raw_data", "clean", "features", "labels", "signals", "incidents", etc.
    schema_version: str
    owner_component: str
    description: str
    tags: List[str]
    created_at: int  # timestamp_utc_us
    last_updated: int  # timestamp_utc_us

class TopicRegistry:
    """
    Centralized topic registry for discovery and governance.
    Provides topic metadata, schema tracking, and ownership management.
    """
    
    def __init__(self):
        self.topics: Dict[str, TopicMetadata] = {}
        self.tags_index: Dict[str, List[str]] = defaultdict(list)  # tag -> [topic_names]
        self.owner_index: Dict[str, List[str]] = defaultdict(list)  # owner -> [topic_names]
        self.data_type_index: Dict[str, List[str]] = defaultdict(list)  # data_type -> [topic_names]
        
    def register_topic(self, metadata: TopicMetadata) -> None:
        """Register a topic with metadata."""
        topic_name = metadata.config.name
        self.topics[topic_name] = metadata
        
        # Update indexes
        self.owner_index[metadata.owner_component].append(topic_name)
        self.data_type_index[metadata.data_type].append(topic_name)
        for tag in metadata.tags:
            self.tags_index[tag].append(topic_name)
            
        logger.info(f"Registered topic: {topic_name} (owner: {metadata.owner_component})")
    
    def get_topic_metadata(self, topic_name: str) -> Optional[TopicMetadata]:
        """Get metadata for a specific topic."""
        return self.topics.get(topic_name)
    
    def find_topics_by_owner(self, owner: str) -> List[TopicMetadata]:
        """Find all topics owned by a component."""
        topic_names = self.owner_index.get(owner, [])
        return [self.topics[name] for name in topic_names if name in self.topics]
    
    def find_topics_by_data_type(self, data_type: str) -> List[TopicMetadata]:
        """Find all topics of a specific data type."""
        topic_names = self.data_type_index.get(data_type, [])
        return [self.topics[name] for name in topic_names if name in self.topics]
    
    def find_topics_by_tag(self, tag: str) -> List[TopicMetadata]:
        """Find all topics with a specific tag."""
        topic_names = self.tags_index.get(tag, [])
        return [self.topics[name] for name in topic_names if name in self.topics]
    
    def get_schema_versions(self, topic_name: str) -> List[str]:
        """Get all schema versions for a topic (for now, just current)."""
        metadata = self.topics.get(topic_name)
        return [metadata.schema_version] if metadata else []
    
    def get_topic_lineage(self, topic_name: str) -> Dict[str, List[str]]:
        """Get upstream and downstream topics (simplified implementation)."""
        # This could be enhanced with actual dependency tracking
        metadata = self.topics.get(topic_name)
        if not metadata:
            return {"upstream": [], "downstream": []}
            
        # Simple heuristic based on naming patterns
        upstream = []
        downstream = []
        
        if metadata.data_type == "clean":
            # Clean topics depend on raw_data topics
            raw_equivalent = topic_name.replace("clean.", "raw_data.")
            if raw_equivalent in self.topics:
                upstream.append(raw_equivalent)
        elif metadata.data_type == "features":
            # Features depend on clean topics
            for topic in self.topics:
                if topic.startswith("clean.") and any(tag in metadata.tags for tag in self.topics[topic].tags):
                    upstream.append(topic)
        
        return {"upstream": upstream, "downstream": downstream}
    
    def export_registry(self) -> Dict[str, Any]:
        """Export full registry for external tools (Grafana, etc.)."""
        return {
            "topics": {
                name: {
                    "config": asdict(metadata.config),
                    "data_type": metadata.data_type,
                    "schema_version": metadata.schema_version,
                    "owner_component": metadata.owner_component,
                    "description": metadata.description,
                    "tags": metadata.tags,
                    "created_at": metadata.created_at,
                    "last_updated": metadata.last_updated
                }
                for name, metadata in self.topics.items()
            },
            "summary": {
                "total_topics": len(self.topics),
                "by_data_type": {dt: len(topics) for dt, topics in self.data_type_index.items()},
                "by_owner": {owner: len(topics) for owner, topics in self.owner_index.items()},
                "by_tag": {tag: len(topics) for tag, topics in self.tags_index.items()}
            }
        }

class LatencyTracker:
    """Track latency percentiles with rolling window."""
    
    def __init__(self, window_size: int = 1000):
        self.window_size = window_size
        self.samples: Deque[float] = deque(maxlen=window_size)
        self._lock = threading.Lock()
    
    def record(self, latency_us: float) -> None:
        """Record a latency sample."""
        with self._lock:
            self.samples.append(latency_us)
    
    def get_percentiles(self) -> Dict[str, float]:
        """Get latency percentiles."""
        with self._lock:
            if not self.samples:
                return {"p50": 0.0, "p95": 0.0, "p99": 0.0}
            
            sorted_samples = sorted(self.samples)
            size = len(sorted_samples)
            
            return {
                "p50": sorted_samples[int(size * 0.5)],
                "p95": sorted_samples[int(size * 0.95)],
                "p99": sorted_samples[int(size * 0.99)],
                "count": size
            }

class ProducerPool:
    """Pool of producers grouped by compression type."""
    
    def __init__(self, bootstrap_servers: List[str], client_id: str, security_config: Dict[str, Any]):
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.security_config = security_config
        self.producers: Dict[str, Any] = {}  # Use Any to avoid import issues
        self.transactional_producer: Optional[Any] = None
    
    async def get_producer(self, compression_type: str) -> Any:
        """Get producer for specific compression type."""
        if not KAFKA_AVAILABLE or AIOKafkaProducer is None:
            raise RuntimeError("Kafka not available")
        
        # Validate and potentially fallback compression type
        validated_compression = validate_compression_type(compression_type)
            
        if validated_compression not in self.producers:
            producer_config = {
                "bootstrap_servers": self.bootstrap_servers,
                "client_id": f"{self.client_id}-{validated_compression}",
                "compression_type": validated_compression,
                "max_batch_size": 16384,
                "linger_ms": 1,
                "acks": "all",
                "enable_idempotence": True,
                "value_serializer": lambda v: json.dumps(v).encode('utf-8') if isinstance(v, dict) else v,
                # Metadata refresh settings to fix NotLeaderForPartitionError
                "metadata_max_age_ms": 5000,  # Refresh metadata every 5s (default 300s)
                "request_timeout_ms": 40000,  # 40s timeout
                "retry_backoff_ms": 100,  # Wait 100ms between retries
            }
            
            # Add security config
            producer_config.update(self.security_config)
            
            producer = AIOKafkaProducer(**producer_config)
            await producer.start()
            self.producers[validated_compression] = producer
            
        return self.producers[validated_compression]
    
    async def get_transactional_producer(self) -> Any:
        """Get transactional producer for exactly-once semantics."""
        if not KAFKA_AVAILABLE or AIOKafkaProducer is None:
            raise RuntimeError("Kafka not available")
            
        if self.transactional_producer is None:
            config = {
                "bootstrap_servers": self.bootstrap_servers,
                "client_id": f"{self.client_id}-transactional",
                "transactional_id": f"{self.client_id}-tx",
                "enable_idempotence": True,
                "acks": "all",
                "value_serializer": lambda v: json.dumps(v).encode('utf-8') if isinstance(v, dict) else v,
                # Metadata refresh settings to fix NotLeaderForPartitionError
                "metadata_max_age_ms": 5000,  # Refresh metadata every 5s
                "request_timeout_ms": 40000,  # 40s timeout
                "retry_backoff_ms": 100,  # Wait 100ms between retries
            }
            config.update(self.security_config)
            
            self.transactional_producer = AIOKafkaProducer(**config)
            await self.transactional_producer.start()
            
        return self.transactional_producer
    
    async def shutdown(self) -> None:
        """Shutdown all producers."""
        for producer in self.producers.values():
            if hasattr(producer, 'stop'):
                await producer.stop()
        if self.transactional_producer and hasattr(self.transactional_producer, 'stop'):
            await self.transactional_producer.stop()

class ConsumerWorkerPool:
    """Worker pool for concurrent message processing."""
    
    def __init__(self, pool_size: int = 16, batch_commit_size: int = 100):
        self.pool_size = pool_size
        self.batch_commit_size = batch_commit_size
        self.semaphore = asyncio.Semaphore(pool_size * 2)  # Control backpressure
        self.pending_commits: List[Any] = []
        self.commit_lock = asyncio.Lock()
    
    async def process_message_raw(self, topic: str, partition_key: str, payload: Union[Dict[str, Any], None], 
                                 headers: Dict[str, str], handler: Callable, consumer: Any) -> None:
        """Process a raw transport message with backpressure control."""
        # Skip None payloads (deserialization errors)
        if payload is None:
            logger.warning(f"Skipping message with None payload from {topic}")
            return
            
        async with self.semaphore:
            try:
                result = handler(topic, partition_key, payload, headers)
                if result is None:
                    logger.error(f"Handler {handler.__name__ if hasattr(handler, '__name__') else handler} returned None instead of coroutine!")
                    return
                await result
                
                # Batch commits for performance
                async with self.commit_lock:
                    self.pending_commits.append(True)  # Just count commits
                    if len(self.pending_commits) >= self.batch_commit_size:
                        if hasattr(consumer, 'commit'):
                            await consumer.commit()
                        self.pending_commits.clear()
                        
            except Exception as e:
                import traceback
                logger.error(f"Message processing error: {e}")
                logger.error(f"Handler info: {handler.__name__ if hasattr(handler, '__name__') else type(handler)}")
                logger.error(f"Full traceback:\n{traceback.format_exc()}")
    
    # Legacy method for backward compatibility
    async def process_message(self, msg: Any, handler: Callable, consumer: Any) -> None:
        """Legacy method - use process_message_raw for pure transport."""
        async with self.semaphore:
            try:
                await handler(msg)
                
                async with self.commit_lock:
                    self.pending_commits.append(True)
                    if len(self.pending_commits) >= self.batch_commit_size:
                        if hasattr(consumer, 'commit'):
                            await consumer.commit()
                        self.pending_commits.clear()
                        
            except Exception as e:
                logger.error(f"Message processing error: {e}")
    
    async def final_commit(self, consumer: Any) -> None:
        """Commit any remaining messages."""
        async with self.commit_lock:
            if self.pending_commits and hasattr(consumer, 'commit'):
                await consumer.commit()
                self.pending_commits.clear()

class StreamingBus:
    """
    High-performance streaming bus for HFT data plane.
    Supports both Kafka and Redpanda with zero-copy optimizations.
    """
    
    def __init__(self, config: Dict[str, Any], memory_governor: Optional[Any] = None, 
                 workload_distributor: Optional[Any] = None):
        """
        Initialize streaming bus with configuration.
        
        Args:
            config: Streaming bus configuration
            memory_governor: Optional MemoryGovernor for backpressure control
            workload_distributor: Optional WorkloadDistributor for intelligent partition routing
        """
        self.config = config
        self.bootstrap_servers = config.get("bootstrap_servers", ["localhost:9092"])
        self.client_id = config.get("client_id", "satoshi-streaming-bus")
        self.security_protocol = config.get("security_protocol", "PLAINTEXT")
        
        # Memory governance for backpressure control
        self.memory_governor = memory_governor
        self.backpressure_enabled = config.get("backpressure_enabled", True)
        
        # Workload distribution for hot key detection and load balancing
        self.workload_distributor = workload_distributor
        self.enable_workload_distribution = config.get("enable_workload_distribution", True)
        # Note: Backpressure thresholds are now handled by MemoryGovernor's pressure levels
        # The _check_backpressure method delegates to memory_governor.get_memory_pressure_level()
        # which returns: "none", "low", "medium", "high", "critical"
        self.backpressure_metrics = {
            "messages_dropped": 0,
            "messages_throttled": 0,
            "throttle_wait_time_ms": 0.0
        }
        
        # Security configuration
        self.security_config = self._build_security_config()
        
        # Enhanced connection pools
        self.producer_pool = ProducerPool(self.bootstrap_servers, self.client_id, self.security_config)
        self.consumers: Dict[str, Any] = {}
        self.consumer_pools: Dict[str, ConsumerWorkerPool] = {}
        
        # Admin client for topic management
        self.admin_client: Optional[Any] = None
        
        # Topic management
        self.topic_configs: Dict[str, TopicConfig] = {}
        self.topic_schemas: Dict[str, Dict[str, Any]] = {}
        self.topic_registry = TopicRegistry()  # Centralized topic registry
        
        # Circuit breaker state
        self.circuit_breakers: Dict[str, bool] = {}  # topic -> paused
        self.paused_partitions: set = set()  # Track paused partitions for safe resume
        
        # System-wide circuit breaker coordination
        self.system_circuit_breaker = SystemCircuitBreakerManager()
        
        # Enhanced performance monitoring
        self.latency_tracker = LatencyTracker()
        # Type as Dict[str, Any] to allow both int counters and nested dict metrics
        self.metrics: Dict[str, Any] = {
            "messages_sent": 0,
            "messages_received": 0,
            "bytes_sent": 0,
            "bytes_received": 0,
            "producer_errors": 0,
            "consumer_errors": 0,
            "transactions_committed": 0,
            "transactions_aborted": 0,
            "circuit_breaker_triggers": 0
        }

        # Per-topic metrics with rate tracking
        self.topic_metrics: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "message_count": 0,
            "last_count_time": time.time(),
            "throughput_msg_per_sec": 0.0,  # Computed rate
            "lag_ms": 0,
            "error_count": 0,
            "error_rate": 0.0,
            "last_error_time": 0
        })
        
        # Health monitoring
        self.broker_health: Dict[str, Dict[str, Any]] = {}
        self.health_check_interval = 30.0  # 30 seconds
        self.health_check_task: Optional[asyncio.Task] = None
        self.last_health_check = 0.0

        # Adaptive rate limiting (AIMD algorithm)
        self.enable_adaptive_rate_limiting = config.get("enable_adaptive_rate_limiting", True)
        self.adaptive_rate_limiter_pool: Optional[AdaptiveRateLimiterPool] = None
        if self.enable_adaptive_rate_limiting:
            self.adaptive_rate_limiter_pool = AdaptiveRateLimiterPool()
            logger.info("✅ Adaptive rate limiting (AIMD) enabled - uses intelligent feedback-driven algorithm")
        
        # Consumer lag monitoring  
        self.consumer_lag_thresholds = {
            "warning": 1000,    # 1000 messages
            "critical": 5000    # 5000 messages  
        }
        
        # Standard topic configurations
        self._setup_standard_topics()
        
        # Track startup time for uptime calculation
        self._start_time = time.time()
        
        logger.info(f"Enhanced streaming bus initialized with servers: {self.bootstrap_servers}")

    def rate_limit(self, domain: str, permits: float = 1.0,
                   timeout: Optional[float] = None):
        """
        Return an async context manager that enforces adaptive rate limiting.
        
        Uses AIMD (Additive Increase Multiplicative Decrease) algorithm that adapts
        to downstream pressure and API responses (429s, timeouts, latency).
        
        Usage:
            async with streaming_bus.rate_limit("onchain_rpc"):
                await call_external_api()
        """
        return AdaptiveRateLimitContext(
            pool=self.adaptive_rate_limiter_pool,
            domain=domain,
            timeout=timeout
        )

    def record_rate_limit_429(self, domain: str) -> None:
        """Record an upstream 429/limit response for adaptive rate adjustment."""
        if self.adaptive_rate_limiter_pool and domain in self.adaptive_rate_limiter_pool.limiters:
            limiter = self.adaptive_rate_limiter_pool.limiters[domain]
            limiter.record_429()
            logger.debug(f"Recorded 429 for domain '{domain}', rate will decrease")

    def get_rate_budget_metrics(self) -> Dict[str, Any]:
        """Expose adaptive rate limiter metrics for monitoring."""
        if not self.adaptive_rate_limiter_pool:
            return {"enabled": False, "limiters": []}
        
        return {
            "enabled": True,
            "type": "adaptive_aimd",
            "limiters": self.adaptive_rate_limiter_pool.get_all_metrics()
        }
    
    def get_workload_distributor_metrics(self) -> Dict[str, Any]:
        """Expose WorkloadDistributor metrics for monitoring hot keys and partition balance."""
        if not self.workload_distributor or not self.enable_workload_distribution:
            return {
                "enabled": False,
                "reason": "workload_distributor_not_configured"
            }
        
        return {
            "enabled": True,
            "hot_keys_detected": self.workload_distributor.hot_key_detections,
            "skew_detections": self.workload_distributor.skew_detections,
            "routing_decisions": dict(self.workload_distributor.routing_decisions),
            "active_hot_keys": {
                topic: list(keys.keys()) 
                for topic, keys in self.workload_distributor.hot_keys.items() 
                if keys
            }
        }

    def _build_security_config(self) -> Dict[str, Any]:
        """Build security configuration for producers/consumers."""
        security_config = {}
        
        if not KAFKA_AVAILABLE or create_ssl_context is None:
            return security_config
            
        if self.security_protocol == "SSL":
            # SSL with optional mTLS (client certificate authentication)
            ssl_context = create_ssl_context(
                cafile=self.config.get("ssl_cafile"),
                certfile=self.config.get("ssl_certfile"),  # Client cert for mTLS
                keyfile=self.config.get("ssl_keyfile"),    # Client key for mTLS
                password=self.config.get("ssl_password")
            )
            
            # Enable mTLS if client certificate is provided
            if self.config.get("ssl_certfile") and self.config.get("ssl_keyfile"):
                ssl_context.check_hostname = True
                ssl_context.verify_mode = ssl.CERT_REQUIRED
                logger.info("mTLS enabled for Kafka connection")
            
            security_config.update({
                "security_protocol": "SSL",
                "ssl_context": ssl_context
            })
            
        elif self.security_protocol == "SASL_SSL":
            security_config.update({
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": self.config.get("sasl_mechanism", "PLAIN"),
                "sasl_plain_username": self.config.get("sasl_username"),
                "sasl_plain_password": self.config.get("sasl_password"),
                "ssl_context": create_ssl_context(
                    cafile=self.config.get("ssl_cafile")
                )
            })
        
        # Fail closed: validate required credentials in production
        env = self.config.get("environment", "development")
        if env == "production":
            if self.security_protocol in ["SSL", "SASL_SSL"]:
                if not security_config:
                    raise ValueError("Security configuration required in production environment")
                
                # Validate SSL credentials
                if self.security_protocol == "SSL":
                    if not self.config.get("ssl_cafile"):
                        raise ValueError("SSL CA file required in production")
                        
                # Validate SASL credentials  
                elif self.security_protocol == "SASL_SSL":
                    if not self.config.get("sasl_username") or not self.config.get("sasl_password"):
                        raise ValueError("SASL credentials required in production")
                        
                logger.info(f"Production security validated: {self.security_protocol}")
            else:
                logger.warning("⚠️  PLAINTEXT protocol used in production - security risk!")
        
        return security_config
    
    async def _check_backpressure(self, topic: str) -> Tuple[bool, Optional[str]]:
        """
        Check memory backpressure and determine if message should be published.
        
        Returns:
            Tuple[bool, Optional[str]]: (should_publish, reason_if_blocked)
        """
        if not self.backpressure_enabled or not self.memory_governor:
            return (True, None)
        
        try:
            # Get memory pressure level
            pressure_level = self.memory_governor.get_memory_pressure_level()
            
            # Critical pressure: Drop messages to prevent OOM
            if pressure_level == "critical":
                self.backpressure_metrics["messages_dropped"] += 1
                return (False, f"Memory pressure CRITICAL - dropping message to topic {topic}")
            
            # High pressure: Throttle publishing with adaptive delay
            elif pressure_level == "high":
                self.backpressure_metrics["messages_throttled"] += 1
                
                # Adaptive delay based on memory pressure severity
                # MemoryGovernor provides pressure level; we adapt delay accordingly
                # HIGH pressure (~85-95%) warrants significant but not extreme delay
                throttle_delay_ms = 100  # Adaptive base: 100ms for HIGH (increased from fixed 50ms)
                start_time = time.time()
                await asyncio.sleep(throttle_delay_ms / 1000.0)
                
                elapsed_ms = (time.time() - start_time) * 1000
                self.backpressure_metrics["throttle_wait_time_ms"] += elapsed_ms
                
                logger.warning(f"Memory pressure HIGH - throttled publish to {topic} by {throttle_delay_ms}ms")
                return (True, None)
            
            # Medium pressure: Log warning but allow
            elif pressure_level == "medium":
                if random.random() < 0.01:  # Log 1% of messages to avoid spam
                    logger.warning(f"Memory pressure MEDIUM - monitor for potential throttling (topic={topic})")
                return (True, None)
            
            # Low or no pressure: Normal operation
            else:
                return (True, None)
                
        except Exception as e:
            logger.error(f"Backpressure check failed: {e}, allowing publish")
            return (True, None)
    
    async def _ensure_admin_client(self) -> Any:
        """Get or create admin client."""
        if not KAFKA_AVAILABLE or AIOKafkaAdminClient is None:
            raise RuntimeError("Kafka not available")
            
        if self.admin_client is None:
            admin_config = {
                "bootstrap_servers": self.bootstrap_servers,
                "client_id": f"{self.client_id}-admin"
            }
            admin_config.update(self.security_config)
            
            self.admin_client = AIOKafkaAdminClient(**admin_config)
            await self.admin_client.start()
        
        return self.admin_client
    
    async def create_topics_from_config(self) -> None:
        """Create topics based on registered configurations."""
        if not KAFKA_AVAILABLE or NewTopic is None:
            logger.warning("Kafka not available, skipping topic creation")
            return
        
        admin = await self._ensure_admin_client()
        
        try:
            # Get existing topics
            existing_topics = await admin.list_topics()
            
            # Create missing topics
            topics_to_create = []
            for topic_name, config in self.topic_configs.items():
                if topic_name not in existing_topics:
                    topic_configs = {
                        "retention.ms": str(config.retention_ms),
                        "compression.type": config.compression_type.value,
                        "cleanup.policy": config.cleanup_policy,
                        "min.insync.replicas": str(config.min_insync_replicas),
                        "unclean.leader.election.enable": str(config.unclean_leader_election).lower(),
                        "max.message.bytes": str(config.max_message_bytes)
                    }
                    
                    if config.segment_ms:
                        topic_configs["segment.ms"] = str(config.segment_ms)
                    
                    new_topic = NewTopic(
                        name=topic_name,
                        num_partitions=config.partitions,
                        replication_factor=config.replication_factor,
                        topic_configs=topic_configs
                    )
                    topics_to_create.append(new_topic)
            
            if topics_to_create:
                result = await admin.create_topics(topics_to_create)
                logger.info(f"Created {len(topics_to_create)} topics: {[t.name for t in topics_to_create]}")
            
            # Validate existing topic configurations
            await self._validate_topic_configs(admin, existing_topics)
            
        except Exception as e:
            logger.error(f"Topic creation/validation failed: {e}")
    
    async def _validate_topic_configs(self, admin: Any, existing_topics: List[str]) -> None:
        """Validate that existing topics match expected configurations."""
        if ConfigResource is None or ConfigResourceType is None:
            logger.warning("Kafka config classes not available, skipping validation")
            return
            
        for topic_name in self.topic_configs.keys():
            if topic_name in existing_topics:
                try:
                    # Get current topic config
                    config_resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
                    configs = await admin.describe_configs([config_resource])
                    
                    current_config = configs[config_resource]
                    expected_config = self.topic_configs[topic_name]
                    
                    # Check key configurations
                    mismatches = []
                    if current_config.get("compression.type") != expected_config.compression_type.value:
                        mismatches.append(f"compression.type: {current_config.get('compression.type')} != {expected_config.compression_type.value}")
                    
                    if mismatches:
                        logger.warning(f"Topic {topic_name} config drift: {', '.join(mismatches)}")
                
                except Exception as e:
                    logger.warning(f"Could not validate config for topic {topic_name}: {e}")
    
    async def publish_with_canonical_headers(self, topic: str, partition_key: str, 
                                           payload: Dict[str, Any],
                                           source_id: str, sequence_number: int,
                                           correlation_id: Optional[str] = None,
                                           producer_version: str = "1.0.0",
                                           dedupe_key: Optional[str] = None,
                                           time_alignment_id: Optional[str] = None) -> bool:
        """
        Institutional-grade publish with canonical headers for full traceability.
        
        Args:
            topic: Target topic
            partition_key: Partition routing key (must match business semantic)
            payload: Message payload
            source_id: Component.instance identifier (e.g., "exchange_connector.001")
            sequence_number: Monotonic sequence per source_id
            correlation_id: Optional UUIDv7 correlation ID (auto-generated if None)
            producer_version: Producer component version
            dedupe_key: Optional deduplication key
            time_alignment_id: Optional time alignment identifier for cross-venue synchronization
        """
        # Create canonical headers with validation
        canonical_headers = CanonicalHeaders.create(
            source_id=source_id,
            sequence_number=sequence_number,
            payload=payload,
            partition_semantic=partition_key,
            correlation_id=correlation_id,
            producer_version=producer_version,
            time_alignment_id=time_alignment_id
        )
        
        # Validate message envelope
        envelope = MessageEnvelope(headers=canonical_headers, payload=payload)
        validation_errors = envelope.validate()
        if validation_errors:
            logger.error(f"Message validation failed: {validation_errors}")
            return False
        
        # Verify partition semantic matches partition key
        if not canonical_headers.verify_partition_semantic(partition_key):
            logger.error(f"Partition semantic mismatch: key={partition_key}, semantic={canonical_headers.partition_semantic}")
            return False
        
        # Convert to transport format
        transport_headers = dict(canonical_headers.to_kafka_headers())
        if dedupe_key:
            transport_headers["dedupe_key"] = dedupe_key.encode('utf-8')
        
        # Use existing transport method
        return await self._publish_with_transport_headers(topic, partition_key, payload, transport_headers)

    async def _publish_with_transport_headers(self, topic: str, partition_key: str, 
                                            payload: Dict[str, Any],
                                            transport_headers: Dict[str, bytes]) -> bool:
        """Internal method for actual Kafka publishing with backpressure control."""
        # Check memory backpressure BEFORE attempting to publish
        should_publish, reason = await self._check_backpressure(topic)
        if not should_publish:
            logger.warning(f"Backpressure blocking publish: {reason}")
            self.metrics["producer_errors"] += 1
            return False
        
        max_retries = 3
        base_delay = 0.1  # 100ms base delay
        
        for attempt in range(max_retries + 1):
            try:
                start_time = time.time_ns()
                
                # Get topic config for compression (transport concern)
                topic_config = self.topic_configs.get(topic)
                compression_type = topic_config.compression_type.value if topic_config else "lz4"
                
                # Get appropriate producer
                producer = await self.producer_pool.get_producer(compression_type)
                
                # Convert headers to Kafka format - handle all possible types
                kafka_headers = []
                for k, v in transport_headers.items():
                    if isinstance(v, (bytes, bytearray, memoryview)):
                        kafka_headers.append((k, bytes(v)))
                    else:
                        kafka_headers.append((k, str(v).encode('utf-8')))
                
                # Intelligent partition selection using WorkloadDistributor (hot key detection, load balancing)
                partition = None
                if self.workload_distributor and self.enable_workload_distribution:
                    try:
                        message_size = len(json.dumps(payload).encode('utf-8'))
                        # Get partition count from topic config or default to None (auto-detect)
                        topic_config = self.topic_configs.get(topic)
                        partition_count = topic_config.partitions if topic_config else None
                        
                        partition = self.workload_distributor.get_partition(
                            key=partition_key,
                            topic=topic,
                            message_size=message_size,
                            partition_count=partition_count
                        )
                        logger.debug(f"WorkloadDistributor selected partition {partition} for key '{partition_key}' on topic {topic}")
                    except Exception as e:
                        logger.warning(f"WorkloadDistributor failed, falling back to Kafka default partitioner: {e}")
                        partition = None
                
                # Send message (with explicit partition if WorkloadDistributor provided one)
                await producer.send_and_wait(
                    topic=topic,
                    key=partition_key.encode('utf-8'),
                    value=json.dumps(payload).encode('utf-8'),
                    headers=kafka_headers,
                    partition=partition  # None = Kafka's default hash partitioner
                )
                
                latency_us = (time.time_ns() - start_time) // 1000
                logger.debug(f"Published to {topic} in {latency_us}μs (attempt {attempt + 1})")
                return True
                
            except Exception as e:
                if attempt == max_retries:
                    logger.error(f"Failed to publish to {topic} after {max_retries + 1} attempts: {e}")
                    return False
                
                delay = base_delay * (2 ** attempt)
                logger.warning(f"Publish attempt {attempt + 1} failed, retrying in {delay}s: {e}")
                await asyncio.sleep(delay)
        
        return False

    async def publish_with_headers(self, topic: str, partition_key: str, payload: Dict[str, Any],
                                  headers: Optional[Dict[str, str]] = None, 
                                  dedupe_key: Optional[str] = None) -> bool:
        """
        Legacy transport method: Publish message with headers and optional deduplication.
        
        ⚠️ DEPRECATED: Use publish_with_canonical_headers for institutional compliance.
        
        This method lacks:
        - Data lineage tracking (correlation_id, source_id)
        - Content integrity verification (content_hash)
        - Audit trail (sequence_number, schema_version)
        - Regulatory compliance (immutable metadata)
        
        Args:
            topic: Target topic
            partition_key: Partition routing key
            payload: Message payload (passed through as-is)
            headers: Transport headers (passed through as-is)
            dedupe_key: Optional deduplication key for idempotent publishing
        """
        # WARNING: This method is deprecated. All Gold Layer components migrated to canonical headers.
        # Only use for Bronze/Silver layers or non-institutional data flows.
        # Rate-limit warnings to prevent CPU wake storm (log once per topic per minute)
        if not hasattr(self, '_deprecation_warnings'):
            self._deprecation_warnings = {}
        
        now = time.time()
        if topic not in self._deprecation_warnings or now - self._deprecation_warnings[topic] > 60:
            self._deprecation_warnings[topic] = now
            logger.warning(
                f"⚠️  DEPRECATED API: publish_with_headers() used for topic={topic}. "
                f"Migrate to publish_with_canonical_headers() for institutional compliance. "
                f"Missing: lineage tracking, audit trail, content integrity."
            )
        
        max_retries = 3
        base_delay = 0.1  # 100ms base delay
        
        # Add dedupe_key to headers if provided (for Exchange Connector ring buffer)
        if headers is None:
            headers = {}
        if dedupe_key:
            headers["dedupe_key"] = dedupe_key
        
        for attempt in range(max_retries + 1):
            try:
                start_time = time.time_ns()
                
                # Get topic config for compression (transport concern)
                topic_config = self.topic_configs.get(topic)
                if topic_config and hasattr(topic_config.compression_type, 'value'):
                    compression_type = topic_config.compression_type.value
                elif topic_config:
                    compression_type = str(topic_config.compression_type)
                else:
                    compression_type = "lz4"
                
                # Validate and get appropriate producer (with fallback)
                validated_compression = validate_compression_type(compression_type)
                producer = await self.producer_pool.get_producer(validated_compression)
                
                # Pass through headers without inspection
                kafka_headers = []
                if headers:
                    kafka_headers = [(k, v.encode('utf-8')) for k, v in headers.items()]
                
                # Send message (payload passed through without inspection)
                await producer.send_and_wait(
                    topic=topic,
                    key=partition_key.encode('utf-8'),
                    value=payload,
                    headers=kafka_headers
                )
                
                # Update transport metrics only
                end_time = time.time_ns()
                latency_us = (end_time - start_time) // 1000
                self.latency_tracker.record(latency_us)
                self.metrics["messages_sent"] += 1
                
                # Update topic metrics with rate calculation
                current_time = time.time()
                topic_metric = self.topic_metrics[topic]
                topic_metric["message_count"] += 1
                
                # Calculate rate over last 10 seconds window
                time_window = current_time - topic_metric["last_count_time"]
                if time_window >= 10.0:  # Update rate every 10 seconds
                    topic_metric["throughput_msg_per_sec"] = topic_metric["message_count"] / time_window
                    topic_metric["message_count"] = 0
                    topic_metric["last_count_time"] = current_time
                
                return True
                
            except Exception as e:
                # Check if this is a retryable error
                if attempt < max_retries and self._is_retryable_error(e):
                    # Exponential backoff with jitter
                    delay = base_delay * (2 ** attempt) + (time.time() % 0.1)  # Add jitter
                    logger.warning(f"Retryable error on attempt {attempt + 1}/{max_retries + 1} for topic {topic}: {e}. Retrying in {delay:.2f}s")
                    await asyncio.sleep(delay)
                    continue
                
                # Non-retryable error or max retries exceeded
                # Transport errors only
                self.metrics["producer_errors"] += 1
                
                # Update topic error metrics
                topic_metric = self.topic_metrics[topic]
                topic_metric["error_count"] += 1
                current_time = time.time()
                topic_metric["last_error_time"] = current_time
                
                # Calculate error rate (errors per minute)
                time_window = 60.0  # 1 minute window
                topic_metric["error_rate"] = topic_metric["error_count"] / time_window
                
                logger.error(f"Transport error publishing to {topic} (attempt {attempt + 1}/{max_retries + 1}): {e}")
                
                # Send to DLQ on final failure (but only for message-level errors, not infrastructure failures)
                if attempt == max_retries and not topic.startswith("dlq.") and self._should_dlq_error(e):
                    await self.send_to_dlq(topic, payload, str(e))
                
                return False
        
        # Should never reach here, but satisfy type checker
        return False
    
    def _is_retryable_error(self, error: Exception) -> bool:
        """Check if an error is retryable (transport-level failures)."""
        error_str = str(error).lower()
        
        # Retryable errors (typically network/broker issues)
        retryable_patterns = [
            "connection", "timeout", "network", "unavailable", 
            "broker not available", "retriable", "leader not available",
            "not enough replicas", "coordinator not available"
        ]
        
        # Non-retryable errors (client/config issues)
        non_retryable_patterns = [
            "authentication", "authorization", "invalid", "serialization",
            "record too large", "offset out of range", "unknown topic"
        ]
        
        # Check non-retryable first
        for pattern in non_retryable_patterns:
            if pattern in error_str:
                return False
        
        # Check retryable patterns
        for pattern in retryable_patterns:
            if pattern in error_str:
                return True
        
        # Default: retry unknown errors (conservative approach)
        return True
    
    def _should_dlq_error(self, error: Exception) -> bool:
        """Check if an error should result in DLQ (message-level errors only)."""
        error_str = str(error).lower()
        
        # Infrastructure errors - DON'T DLQ (Kafka down, network issues)
        infrastructure_errors = [
            "kafka not available", "connection", "timeout", "network", 
            "broker not available", "coordinator not available", 
            "leader not available", "not enough replicas"
        ]
        
        # Message-level errors - DO DLQ (bad data, schema issues)
        message_errors = [
            "serialization", "record too large", "invalid message",
            "schema violation", "malformed", "encoding error"
        ]
        
        # Don't DLQ infrastructure failures
        for pattern in infrastructure_errors:
            if pattern in error_str:
                return False
        
        # DO DLQ message-level errors
        for pattern in message_errors:
            if pattern in error_str:
                return True
        
        # Conservative: Don't DLQ unknown errors (avoid unnecessary DLQ churn)
        return False
    
    async def publish_transactional(self, messages: List[Dict[str, Any]]) -> bool:
        """
        Pure transport: Publish multiple messages atomically (no domain logic).
        
        Args:
            messages: List of {topic, partition_key, payload, headers} dicts
        """
        if not messages:
            return True
        
        try:
            producer = await self.producer_pool.get_transactional_producer()
            
            async with producer.transaction():
                for msg in messages:
                    topic = msg["topic"]
                    partition_key = msg["partition_key"]
                    payload = msg["payload"]
                    headers = msg.get("headers", {})
                    
                    # Pass through without inspection
                    kafka_headers = [(k, v.encode('utf-8')) for k, v in headers.items()]
                    
                    await producer.send(
                        topic=topic,
                        key=partition_key.encode('utf-8'),
                        value=payload,
                        headers=kafka_headers
                    )
            
            # Transport metrics only
            self.metrics["transactions_committed"] += 1
            self.metrics["messages_sent"] += len(messages)
            return True
            
        except Exception as e:
            self.metrics["transactions_aborted"] += 1
            logger.error(f"Transaction failed: {e}")
            return False
    
    async def transactional_consume_produce(self, consumer: Any, records: List[Any], 
                                           out_messages: List[Dict[str, Any]]) -> bool:
        """
        Pure transport: Exactly-once from consumer→producer (EOS v2).
        
        Sends consumer offsets in the same transaction as producer messages.
        This provides exactly-once semantics for consume→transform→produce workflows.
        
        Args:
            consumer: AIOKafkaConsumer instance
            records: Consumer records that were processed
            out_messages: List of {topic, partition_key, payload, headers} dicts to send
            
        Returns:
            bool: Success status
        """
        if not KAFKA_AVAILABLE or not out_messages:
            return True
            
        try:
            producer = await self.producer_pool.get_transactional_producer()
            
            async with producer.transaction():
                # Send output messages
                for msg in out_messages:
                    topic = msg["topic"]
                    partition_key = msg["partition_key"]
                    payload = msg["payload"]
                    headers = msg.get("headers", {})
                    
                    # Pure transport: pass through without inspection
                    kafka_headers = [(k, v.encode('utf-8')) for k, v in headers.items()]
                    
                    await producer.send(
                        topic=topic,
                        key=partition_key.encode('utf-8'),
                        value=payload,
                        headers=kafka_headers
                    )
                
                # Send consumer offsets in same transaction (EOS v2)
                if hasattr(consumer, 'position') and hasattr(producer, 'send_offsets_to_transaction'):
                    # Get current positions from processed records
                    offsets = {}
                    for record in records:
                        tp = (record.topic, record.partition)
                        offsets[tp] = record.offset + 1  # Next offset to read
                    
                    # Send offsets to transaction
                    await producer.send_offsets_to_transaction(
                        offsets, 
                        consumer._group_id if hasattr(consumer, '_group_id') else 'default-group'
                    )
            
            # Transport metrics only
            self.metrics["transactions_committed"] += 1
            self.metrics["messages_sent"] += len(out_messages)
            logger.debug(f"EOS transaction: consumed {len(records)}, produced {len(out_messages)}")
            return True
            
        except Exception as e:
            self.metrics["transactions_aborted"] += 1
            logger.error(f"EOS transaction failed: {e}")
            return False
    
    def _setup_standard_topics(self) -> None:
        """Setup standard topic configurations for the trading system."""
        
        # Raw data topics (high throughput, short retention)
        self.register_topic_with_metadata(
            topic_config=TopicConfig(
                name="raw_data.market.trades",
                partitions=16,
                replication_factor=3,
                retention_ms=3600000,  # 1 hour
                compression_type=CompressionType.LZ4,
                cleanup_policy="delete",
                segment_ms=300000  # 5 minutes
            ),
            data_type="raw_data",
            owner_component="exchange_connector",
            description="Real-time trade data from all exchanges",
            tags=["market_data", "high_frequency", "trades"]
        )
        
        self.register_topic_with_metadata(
            topic_config=TopicConfig(
                name="raw_data.market.book",
                partitions=16,
                replication_factor=3,
                retention_ms=1800000,  # 30 minutes
                compression_type=CompressionType.LZ4,
                cleanup_policy="delete"
            ),
            data_type="raw_data",
            owner_component="exchange_connector", 
            description="Order book snapshots from all exchanges",
            tags=["market_data", "high_frequency", "orderbook"]
        )
        
        # Additional raw data topics for complete data ingestion coverage
        self.register_topic_config(TopicConfig(
            name="raw_data.market.funding",
            partitions=8,
            replication_factor=3,
            retention_ms=3600000,  # 1 hour
            compression_type=CompressionType.LZ4,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="raw_data.market.oi",
            partitions=8,
            replication_factor=3,
            retention_ms=3600000,  # 1 hour
            compression_type=CompressionType.LZ4,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="raw_data.onchain.blocks",
            partitions=4,
            replication_factor=3,
            retention_ms=7200000,  # 2 hours
            compression_type=CompressionType.GZIP,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="raw_data.onchain.mempool",
            partitions=6,
            replication_factor=3,
            retention_ms=1800000,  # 30 minutes
            compression_type=CompressionType.LZ4,
            cleanup_policy="delete"
        ))
        
        # Clean data topics (validated, longer retention)
        self.register_topic_config(TopicConfig(
            name="clean.market.trades",
            partitions=16,
            replication_factor=3,
            retention_ms=86400000,  # 24 hours
            compression_type=CompressionType.ZSTD,
            cleanup_policy="delete"
        ))
        
        # Additional clean data topics
        self.register_topic_config(TopicConfig(
            name="clean.market.book",
            partitions=16,
            replication_factor=3,
            retention_ms=86400000,  # 24 hours
            compression_type=CompressionType.ZSTD,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="clean.market.funding",
            partitions=8,
            replication_factor=3,
            retention_ms=604800000,  # 7 days
            compression_type=CompressionType.ZSTD,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="clean.market.oi",
            partitions=8,
            replication_factor=3,
            retention_ms=604800000,  # 7 days
            compression_type=CompressionType.ZSTD,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="clean.market.options",
            partitions=8,
            replication_factor=3,
            retention_ms=604800000,  # 7 days
            compression_type=CompressionType.ZSTD,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="clean.market.onchain",
            partitions=4,
            replication_factor=3,
            retention_ms=604800000,  # 7 days
            compression_type=CompressionType.ZSTD,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="clean.market.events",
            partitions=4,
            replication_factor=3,
            retention_ms=604800000,  # 7 days
            compression_type=CompressionType.GZIP,
            cleanup_policy="delete"
        ))
        
        # Feature topics (medium retention, compaction)
        self.register_topic_config(TopicConfig(
            name="features.vector",
            partitions=8,
            replication_factor=3,
            retention_ms=604800000,  # 7 days
            compression_type=CompressionType.ZSTD,
            cleanup_policy="compact",
            enable_compaction=True
        ))
        
        self.register_topic_config(TopicConfig(
            name="features.vol_surface",
            partitions=4,
            replication_factor=3,
            retention_ms=604800000,  # 7 days
            compression_type=CompressionType.ZSTD,
            cleanup_policy="compact",
            enable_compaction=True
        ))
        
        self.register_topic_config(TopicConfig(
            name="features.carry_basis",
            partitions=4,
            replication_factor=3,
            retention_ms=604800000,  # 7 days
            compression_type=CompressionType.ZSTD,
            cleanup_policy="compact",
            enable_compaction=True
        ))
        
        self.register_topic_config(TopicConfig(
            name="features.onchain",
            partitions=4,
            replication_factor=3,
            retention_ms=604800000,  # 7 days
            compression_type=CompressionType.ZSTD,
            cleanup_policy="compact",
            enable_compaction=True
        ))
        
        self.register_topic_config(TopicConfig(
            name="features.events",
            partitions=4,
            replication_factor=3,
            retention_ms=604800000,  # 7 days
            compression_type=CompressionType.ZSTD,
            cleanup_policy="compact",
            enable_compaction=True
        ))
        
        self.register_topic_config(TopicConfig(
            name="features.costs",
            partitions=4,
            replication_factor=3,
            retention_ms=604800000,  # 7 days
            compression_type=CompressionType.ZSTD,
            cleanup_policy="compact",
            enable_compaction=True
        ))
        
        self.register_topic_config(TopicConfig(
            name="features.regime",
            partitions=2,
            replication_factor=3,
            retention_ms=2592000000,  # 30 days
            compression_type=CompressionType.ZSTD,
            cleanup_policy="compact",
            enable_compaction=True
        ))
        
        # Alpha Signal Feature Topics (Pure Alpha Generation)
        self.register_topic_config(TopicConfig(
            name="features.flow_pressure",
            partitions=12,
            replication_factor=3,
            retention_ms=21600000,  # 6 hours (flow signals decay fast)
            compression_type=CompressionType.LZ4,
            cleanup_policy="delete",
            segment_ms=30000  # 30-second segments for ultra-low latency
        ))
        
        self.register_topic_config(TopicConfig(
            name="features.momentum_exhaustion",
            partitions=8,
            replication_factor=3,
            retention_ms=43200000,  # 12 hours (momentum signals)
            compression_type=CompressionType.LZ4,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="features.liquidity_stress",
            partitions=16,
            replication_factor=3,
            retention_ms=7200000,  # 2 hours (ultra-high frequency)
            compression_type=CompressionType.NONE,  # No compression for speed
            cleanup_policy="delete",
            segment_ms=10000  # 10-second segments
        ))
        
        self.register_topic_config(TopicConfig(
            name="features.onchain_flow",
            partitions=6,
            replication_factor=3,
            retention_ms=86400000,  # 24 hours (onchain signals)
            compression_type=CompressionType.LZ4,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="features.ohlcv_signals",
            partitions=10,
            replication_factor=3,
            retention_ms=172800000,  # 48 hours (OHLCV-based alpha signals)
            compression_type=CompressionType.ZSTD,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="features.spread_analysis",
            partitions=8,
            replication_factor=3,
            retention_ms=21600000,  # 6 hours (spread-based alpha signals)
            compression_type=CompressionType.LZ4,
            cleanup_policy="delete"
        ))
        
        # Label topics
        self.register_topic_config(TopicConfig(
            name="labels.tb",
            partitions=4,
            replication_factor=3,
            retention_ms=2592000000,  # 30 days
            compression_type=CompressionType.ZSTD,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="labels.forward",
            partitions=4,
            replication_factor=3,
            retention_ms=2592000000,  # 30 days
            compression_type=CompressionType.ZSTD,
            cleanup_policy="delete"
        ))
        
        # Signal topics (critical path, minimal retention)
        self.register_topic_config(TopicConfig(
            name="signals.raw",
            partitions=4,
            replication_factor=3,
            retention_ms=3600000,  # 1 hour
            compression_type=CompressionType.LZ4,
            cleanup_policy="delete"
        ))
        
        # Order topics (high frequency, short retention)
        self.register_topic_config(TopicConfig(
            name="orders.place",
            partitions=8,
            replication_factor=3,
            retention_ms=86400000,  # 24 hours
            compression_type=CompressionType.LZ4,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="orders.fills",
            partitions=8,
            replication_factor=3,
            retention_ms=86400000,  # 24 hours
            compression_type=CompressionType.LZ4,
            cleanup_policy="delete"
        ))
        
        # Execution topics
        self.register_topic_config(TopicConfig(
            name="exec.cost_pred",
            partitions=4,
            replication_factor=3,
            retention_ms=86400000,  # 24 hours
            compression_type=CompressionType.ZSTD,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="exec.telemetry",
            partitions=4,
            replication_factor=3,
            retention_ms=604800000,  # 7 days
            compression_type=CompressionType.GZIP,
            cleanup_policy="delete"
        ))
        
        # Trade intents topics
        self.register_topic_config(TopicConfig(
            name="trade_intents.entry",
            partitions=4,
            replication_factor=3,
            retention_ms=86400000,  # 24 hours
            compression_type=CompressionType.LZ4,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="trade_intents.exit",
            partitions=4,
            replication_factor=3,
            retention_ms=86400000,  # 24 hours
            compression_type=CompressionType.LZ4,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="trade_intents.risk_check",
            partitions=2,
            replication_factor=3,
            retention_ms=86400000,  # 24 hours
            compression_type=CompressionType.LZ4,
            cleanup_policy="delete"
        ))
        
        # Incident topics (important for auditing)
        self.register_topic_config(TopicConfig(
            name="incidents.all",
            partitions=4,
            replication_factor=3,
            retention_ms=2592000000,  # 30 days
            compression_type=CompressionType.GZIP,
            cleanup_policy="delete"
        ))
        
        # Control topics (critical, high retention)
        self.register_topic_config(TopicConfig(
            name="control.circuit_breaker",
            partitions=2,
            replication_factor=3,
            retention_ms=604800000,  # 7 days
            compression_type=CompressionType.GZIP,
            cleanup_policy="delete"
        ))
        
        # Centralized breaker coordination topics
        self.register_topic_config(TopicConfig(
            name="control.breaker_intent",
            partitions=2,
            replication_factor=3,
            retention_ms=604800000,  # 7 days
            compression_type=CompressionType.GZIP,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="control.breaker_state",
            partitions=2,
            replication_factor=3,
            retention_ms=604800000,  # 7 days
            compression_type=CompressionType.GZIP,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="control.command_acks",
            partitions=2,
            replication_factor=3,
            retention_ms=604800000,  # 7 days
            compression_type=CompressionType.GZIP,
            cleanup_policy="delete"
        ))
        
        # Additional missing topics for data ingestion layer
        self.register_topic_config(TopicConfig(
            name="clean.pass_fail",
            partitions=4,
            replication_factor=3,
            retention_ms=86400000,  # 24 hours
            compression_type=CompressionType.GZIP,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="raw_data.options.surface",
            partitions=8,
            replication_factor=3,
            retention_ms=604800000,  # 7 days
            compression_type=CompressionType.ZSTD,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="block_watermark",
            partitions=1,  # Single partition for ordering
            replication_factor=3,
            retention_ms=2592000000,  # 30 days (for leakage proofs)
            compression_type=CompressionType.GZIP,
            cleanup_policy="compact",
            enable_compaction=True
        ))
        
        # ========== CURATED/GOLD LAYER TOPICS ==========
        # ❌ REMOVED: Alpha computation topics moved to features.* (leakage prevention)
        # curated.market.ohlcv_1s → features.ohlcv_signals (OHLCV is alpha generation)
        # curated.market.ohlcv_1m → features.ohlcv_signals (momentum indicators)
        # curated.market.spreads → features.spread_analysis (spread signals are alpha)
        
        self.register_topic_config(TopicConfig(
            name="curated.market.depth_metrics",
            partitions=10,
            replication_factor=3,
            retention_ms=14400000,  # 4 hours
            compression_type=CompressionType.LZ4,
            cleanup_policy="delete"
        ))
        
        # Priority 2: Cross-Venue Normalization
        self.register_topic_config(TopicConfig(
            name="curated.venues.normalized_book",
            partitions=16,
            replication_factor=3,
            retention_ms=28800000,  # 8 hours
            compression_type=CompressionType.ZSTD,
            cleanup_policy="delete"
        ))
        
        # ❌ REMOVED: Alpha signal topics moved to features.* (leakage prevention)
        # curated.venues.spread_matrix → features.spread_analysis
        # curated.venues.flow_divergence → features.flow_pressure
        
        # Priority 3: Safe Gold Layer - Business Data Preparation (NO ALPHA)
        self.register_topic_config(TopicConfig(
            name="curated.data.trades_1s",
            partitions=12,
            replication_factor=3,
            retention_ms=86400000,  # 24 hours (performance-optimized trades)
            compression_type=CompressionType.LZ4,
            cleanup_policy="delete",
            segment_ms=30000  # 30-second segments for performance
        ))
        
        self.register_topic_config(TopicConfig(
            name="curated.data.book_snapshots",
            partitions=16,
            replication_factor=3,
            retention_ms=21600000,  # 6 hours (format-standardized books)
            compression_type=CompressionType.LZ4,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="curated.data.indexed_blocks",
            partitions=6,
            replication_factor=3,
            retention_ms=2592000000,  # 30 days (indexed blockchain data)
            compression_type=CompressionType.GZIP,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="curated.data.unified_symbols",
            partitions=4,
            replication_factor=3,
            retention_ms=172800000,  # 48 hours (symbol normalization)
            compression_type=CompressionType.GZIP,
            cleanup_policy="delete"
        ))
        
        # Priority 4: Options & Derivatives
        self.register_topic_config(TopicConfig(
            name="curated.options.implied_vol_surface",
            partitions=6,
            replication_factor=3,
            retention_ms=604800000,  # 7 days
            compression_type=CompressionType.ZSTD,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="curated.options.flow_analysis",
            partitions=8,
            replication_factor=3,
            retention_ms=43200000,  # 12 hours
            compression_type=CompressionType.LZ4,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="curated.carry.basis_signals",
            partitions=4,
            replication_factor=3,
            retention_ms=604800000,  # 7 days
            compression_type=CompressionType.GZIP,
            cleanup_policy="delete"
        ))
        
        # Additional Data Ingestion Layer Topics
        self.register_topic_config(TopicConfig(
            name="incidents.SchemaViolation",
            partitions=6,
            replication_factor=3,
            retention_ms=2592000000,  # 30 days (compliance)
            compression_type=CompressionType.GZIP,
            cleanup_policy="delete"
        ))
        
        # Additional Monitoring & Quality Incident Topics
        self.register_topic_config(TopicConfig(
            name="incidents.Freshness",
            partitions=4,
            replication_factor=3,
            retention_ms=604800000,  # 7 days (operational incidents)
            compression_type=CompressionType.GZIP,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="incidents.Anomaly",
            partitions=6,
            replication_factor=3,
            retention_ms=1209600000,  # 14 days (analysis incidents)
            compression_type=CompressionType.GZIP,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="incidents.Leakage",
            partitions=4,
            replication_factor=3,
            retention_ms=2592000000,  # 30 days (compliance - data leakage is serious)
            compression_type=CompressionType.GZIP,
            cleanup_policy="delete"
        ))
        
        # Raw Data Ingestion Topics (All Collectors)
        self.register_topic_config(TopicConfig(
            name="raw_data.exchange_feed",
            partitions=20,  # High throughput exchange data
            replication_factor=3,
            retention_ms=604800000,  # 7 days
            compression_type=CompressionType.LZ4,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="raw_data.options_chain",
            partitions=12,
            replication_factor=3,
            retention_ms=604800000,  # 7 days
            compression_type=CompressionType.LZ4,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="raw_data.onchain_events",
            partitions=8,
            replication_factor=3,
            retention_ms=2592000000,  # 30 days (blockchain audit)
            compression_type=CompressionType.GZIP,
            cleanup_policy="delete"
        ))
        
        self.register_topic_config(TopicConfig(
            name="raw_data.offchain_events",
            partitions=6,
            replication_factor=3,
            retention_ms=604800000,  # 7 days
            compression_type=CompressionType.GZIP,
            cleanup_policy="delete"
        ))
    
    def register_topic_config(self, topic_config: TopicConfig) -> None:
        """Register a topic configuration."""
        self.topic_configs[topic_config.name] = topic_config
        logger.info(f"Registered topic config: {topic_config.name}")
    
    def register_topic_with_metadata(self, topic_config: TopicConfig, 
                                   data_type: str, owner_component: str,
                                   description: str, tags: Optional[List[str]] = None,
                                   schema_version: str = "1.0") -> None:
        """Register a topic with full metadata in the topic registry."""
        # Register the config first
        self.register_topic_config(topic_config)
        
        # Create metadata and register with topic registry
        now_us = int(time.time_ns() // 1000)
        metadata = TopicMetadata(
            config=topic_config,
            data_type=data_type,
            schema_version=schema_version,
            owner_component=owner_component,
            description=description,
            tags=tags or [],
            created_at=now_us,
            last_updated=now_us
        )
        self.topic_registry.register_topic(metadata)
    
    def get_topic_registry_export(self) -> Dict[str, Any]:
        """Export topic registry for external tools."""
        return self.topic_registry.export_registry()
    
    def validate_data_ingestion_topics(self) -> bool:
        """
        Validate that all required data ingestion layer topics are configured.
        Returns True if all required topics are present, False otherwise.
        """
        required_topics = {
            # Raw Data Topics (used by collectors)
            "raw_data.market.trades",
            "raw_data.market.book",
            "raw_data.market.funding", 
            "raw_data.market.oi",
            "raw_data.onchain.blocks",
            "raw_data.onchain.mempool",
            "raw_data.exchange_feed",
            "raw_data.options_chain", 
            "raw_data.onchain_events",
            "raw_data.offchain_events",
            
            # Clean Data Topics (used by schema validator)
            "clean.pass_fail",
            
            # Incident Topics (used by monitoring agents)
            "incidents.SchemaViolation",
            "incidents.Freshness",
            "incidents.Anomaly", 
            "incidents.Leakage",
            
            # Control Topics (used by circuit breaker)
            "control.circuit_breaker",
            "control.breaker_intent",
            "control.breaker_state",
            "control.command_acks"
        }
        
        configured_topics = set(self.topic_configs.keys())
        missing_topics = required_topics - configured_topics
        
        if missing_topics:
            logger.error(f"Missing required data ingestion topics: {sorted(missing_topics)}")
            return False
        
        logger.info(f"✅ All {len(required_topics)} required data ingestion topics are configured")
        return True
    
    def get_topic_summary(self) -> Dict[str, int]:
        """Get summary of configured topics by category."""
        summary = {
            "raw_data": 0,
            "clean": 0,
            "curated": 0,
            "incidents": 0,
            "control": 0,
            "other": 0
        }
        
        for topic_name in self.topic_configs.keys():
            if topic_name.startswith("raw_data."):
                summary["raw_data"] += 1
            elif topic_name.startswith("clean."):
                summary["clean"] += 1
            elif topic_name.startswith("curated."):
                summary["curated"] += 1
            elif topic_name.startswith("incidents."):
                summary["incidents"] += 1
            elif topic_name.startswith("control."):
                summary["control"] += 1
            else:
                summary["other"] += 1
        
        return summary
    
    async def subscribe_with_worker_pool(self, consumer_group: str, topics: List[str], 
                                        handler: Callable[[str, str, Dict[str, Any], Dict[str, str]], Any],
                                        pool_size: int = 16) -> None:
        """
        Pure transport: Subscribe with worker pool (no domain logic).
        
        Args:
            consumer_group: Consumer group identifier
            topics: List of topics to subscribe to
            handler: Async function(topic, partition_key, payload, headers) - gets raw data
            pool_size: Number of concurrent workers
        """
        if not KAFKA_AVAILABLE or AIOKafkaConsumer is None:
            logger.error("Kafka not available for subscription")
            return
            
        try:
            # Create enhanced consumer
            consumer_config = {
                "bootstrap_servers": self.bootstrap_servers,
                "group_id": consumer_group,
                "client_id": f"{self.client_id}-consumer-{consumer_group}",
                "auto_offset_reset": "latest",  # Only read NEW messages (historical data has schema mismatches)
                "enable_auto_commit": False,
                "fetch_min_bytes": 1,
                "fetch_max_wait_ms": 100,
                "max_poll_records": 500,  # Batch size for performance
                "value_deserializer": lambda m: json.loads(m.decode('utf-8')) if m else None,
                # Coordinator timeout settings (match Kafka broker config)
                "session_timeout_ms": 30000,  # 30s - matches Kafka's group_max_session_timeout_ms range
                "heartbeat_interval_ms": 10000,  # 10s - 1/3 of session_timeout
                "request_timeout_ms": 40000,  # 40s - must be > session_timeout_ms
            }
            consumer_config.update(self.security_config)
            
            consumer = AIOKafkaConsumer(*topics, **consumer_config)
            await consumer.start()
            
            # Register consumer for metrics tracking
            self.consumers[consumer_group] = consumer
            
            # Create worker pool
            worker_pool = ConsumerWorkerPool(pool_size=pool_size)
            self.consumer_pools[consumer_group] = worker_pool
            
            logger.info(f"Transport subscription: {consumer_group} -> {topics} (pool_size={pool_size})")
            
            try:
                while True:
                    # Check circuit breakers (transport control)
                    paused_partitions = []
                    for topic in topics:
                        if self.circuit_breakers.get(topic, False):
                            topic_partitions = consumer.assignment()
                            paused_partitions.extend([tp for tp in topic_partitions if tp.topic == topic])
                    
                    if paused_partitions:
                        consumer.pause(*paused_partitions)
                        self.paused_partitions.update(paused_partitions)
                        logger.info(f"Transport: Paused partitions due to circuit breaker: {paused_partitions}")
                        await asyncio.sleep(1)
                        continue
                    else:
                        # Resume if previously paused and now safe
                        if self.paused_partitions:
                            resume_partitions = [tp for tp in self.paused_partitions if tp in consumer.assignment()]
                            if resume_partitions:
                                consumer.resume(*resume_partitions)
                                self.paused_partitions.clear()
                    
                    # Poll messages (timeout reduced from 100ms to 500ms to prevent macOS CPU throttling)
                    msg_batch = await consumer.getmany(timeout_ms=500, max_records=500)
                    
                    # Process batch concurrently
                    tasks = []
                    for tp, messages in msg_batch.items():
                        for msg in messages:
                            try:
                                # Extract transport data without domain inspection
                                topic = msg.topic
                                partition_key = msg.key.decode('utf-8') if msg.key else ""
                                payload = msg.value  # Pass through as-is
                                raw_headers = msg.headers or []  # Keep as List[Tuple[str, bytes]] to preserve duplicates
                                
                                # Skip messages with None payload (deserialization error)
                                if payload is None:
                                    logger.warning(f"Skipping message with None payload from {topic}")
                                    continue
                                
                                # Convert headers to dict for convenience (collapses duplicates)
                                # Handler gets both formats: Dict for convenience, raw available if needed
                                str_headers = {}
                                for k, v in raw_headers:
                                    if isinstance(v, bytes):
                                        str_headers[k] = v.decode('utf-8')
                                    else:
                                        str_headers[k] = str(v)
                                
                                # Submit to worker pool (pure transport data)
                                task = asyncio.create_task(
                                    worker_pool.process_message_raw(topic, partition_key, payload, str_headers, handler, consumer)
                                )
                                tasks.append(task)
                                
                            except Exception as e:
                                logger.error(f"Transport message parsing error: {e}")
                                self.metrics["consumer_errors"] += 1
                    
                    # Wait for batch completion
                    if tasks:
                        await asyncio.gather(*tasks, return_exceptions=True)
                        self.metrics["messages_received"] += len(tasks)
                    
            except asyncio.CancelledError:
                logger.info(f"Consumer {consumer_group} cancelled")
            except Exception as e:
                logger.error(f"Consumer error: {e}")
            finally:
                # Final commit and cleanup
                await worker_pool.final_commit(consumer)
                await consumer.stop()
                
        except Exception as e:
            logger.error(f"Failed to start transport consumer: {e}")
    
    async def set_circuit_breaker(self, topic: str, paused: bool) -> None:
        """Set circuit breaker state for a topic (transport control only)."""
        self.circuit_breakers[topic] = paused
        if paused:
            self.metrics["circuit_breaker_triggers"] += 1
        logger.info(f"Transport circuit breaker for {topic}: {'PAUSED' if paused else 'ACTIVE'}")
    
    async def get_transport_metrics(self) -> Dict[str, Any]:
        """Get transport performance metrics (no domain KPIs)."""
        latency_stats = self.latency_tracker.get_percentiles()
        
        return {
            **self.metrics,
            "latency_percentiles_us": latency_stats,
            "topic_throughput": dict(self.topic_metrics),  # Only transport throughput
            "circuit_breakers": self.circuit_breakers.copy(),
            "active_consumers": len(self.consumers),
            "active_worker_pools": len(self.consumer_pools)
        }
    
    # Dead Letter Queue for transport errors only
    async def send_to_dlq(self, failed_topic: str, failed_message: Dict[str, Any], 
                         error_reason: str) -> bool:
        """Send to DLQ for message-level errors only (no retries to avoid recursion)."""
        if not KAFKA_AVAILABLE:
            # If Kafka is down, log locally instead of trying DLQ
            logger.error(f"DLQ skipped (Kafka unavailable): {failed_topic} - {error_reason}")
            return False
            
        dlq_topic = f"dlq.transport.{failed_topic}"
        
        dlq_payload = {
            "original_topic": failed_topic,
            "failed_message": failed_message,
            "transport_error": error_reason,
            "timestamp": int(time.time_ns())
        }
        
        dlq_headers = {
            "error_type": "transport",
            "original_topic": failed_topic
        }
        
        try:
            # Simple DLQ send without retries (to avoid recursion)
            topic_config = self.topic_configs.get(dlq_topic)
            compression_type = topic_config.compression_type.value if topic_config else "lz4"
            
            producer = await self.producer_pool.get_producer(compression_type)
            kafka_headers = [(k, v.encode('utf-8')) for k, v in dlq_headers.items()]
            
            await producer.send_and_wait(
                topic=dlq_topic,
                key="error".encode('utf-8'),
                value=dlq_payload,
                headers=kafka_headers
            )
            
            logger.info(f"Sent to DLQ: {failed_topic} -> {dlq_topic}")
            return True
            
        except Exception as dlq_error:
            # DLQ failed - just log, don't recurse
            logger.error(f"DLQ failed for {failed_topic}: {dlq_error}")
            return False
    
    # Legacy compatibility wrappers (simplified)
    async def get_producer(self, producer_id: str = "default") -> Any:
        """Legacy method - use producer pool directly."""
        return await self.producer_pool.get_producer("lz4")
    
    async def get_consumer(self, consumer_group: str, topics: List[str]) -> Any:
        """Legacy method - use subscribe_with_worker_pool instead."""
        if not KAFKA_AVAILABLE or AIOKafkaConsumer is None:
            raise RuntimeError("Kafka not available")
            
        consumer_config = {
            "bootstrap_servers": self.bootstrap_servers,
            "group_id": consumer_group,
            "client_id": f"{self.client_id}-consumer-{consumer_group}",
            "auto_offset_reset": "earliest",  # TEMPORARY: Changed from "latest" to process existing messages
            "enable_auto_commit": False,
            "value_deserializer": lambda m: json.loads(m.decode('utf-8')) if m else None,
            # Coordinator timeout settings (match Kafka broker config)
            "session_timeout_ms": 30000,  # 30s - matches Kafka's group_max_session_timeout_ms range
            "heartbeat_interval_ms": 10000,  # 10s - 1/3 of session_timeout
            "request_timeout_ms": 40000,  # 40s - must be > session_timeout_ms
        }
        consumer_config.update(self.security_config)
        
        consumer = AIOKafkaConsumer(*topics, **consumer_config)
        await consumer.start()
        return consumer
    
    async def publish(self, topic: str, partition_key: str, payload: Dict[str, Any], 
                     headers: Optional[Dict[str, str]] = None) -> bool:
        """
        Simplified publish method using canonical headers for institutional compliance.
        
        For advanced use cases with full header control, use publish_with_canonical_headers.
        """
        return await self.publish_with_canonical_headers(
            topic=topic,
            partition_key=partition_key,
            payload=payload,
            source_id="simple_publisher",
            sequence_number=int(time.time() * 1000000),  # Auto-generated from timestamp
            correlation_id=None,   # Auto-generated
            producer_version="1.0",
            time_alignment_id=None
        )
    
    async def subscribe(self, consumer_group: str, topics: List[str], 
                       handler: Callable[[str, str, Dict[str, Any], Dict[str, str]], None]) -> None:
        """Legacy subscribe method."""
        await self.subscribe_with_worker_pool(consumer_group, topics, handler, pool_size=8)
    
    # Helper method removed - use publish_with_headers() directly
    # Domain-specific incident logic belongs in your agents, not transport
    
    async def start_health_monitoring(self) -> None:
        """Start continuous health monitoring of brokers and consumers."""
        if self.health_check_task is None:
            self.health_check_task = asyncio.create_task(self._health_monitor_loop())
            logger.info("Health monitoring started")
    
    async def stop_health_monitoring(self) -> None:
        """Stop health monitoring."""
        if self.health_check_task:
            self.health_check_task.cancel()
            try:
                await self.health_check_task
            except asyncio.CancelledError:
                pass
            self.health_check_task = None
            logger.info("Health monitoring stopped")
    
    async def _health_monitor_loop(self) -> None:
        """Continuous health monitoring loop."""
        while True:
            try:
                await self._check_broker_health()
                await self._check_consumer_lag()
                await asyncio.sleep(self.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health monitoring error: {e}")
                await asyncio.sleep(self.health_check_interval)
    
    async def _check_broker_health(self) -> None:
        """Check health of all configured brokers."""
        current_time = time.time()
        
        for broker in self.bootstrap_servers:
            try:
                # Simple connectivity check via admin client
                admin = await self._ensure_admin_client()
                metadata = await admin.describe_cluster()
                
                self.broker_health[broker] = {
                    "status": "healthy",
                    "last_check": current_time,
                    "nodes": len(metadata.nodes) if hasattr(metadata, 'nodes') else 0,
                    "controller": getattr(metadata, 'controller', None)
                }
                
            except Exception as e:
                self.broker_health[broker] = {
                    "status": "unhealthy",
                    "last_check": current_time,
                    "error": str(e),
                    "nodes": 0
                }
                logger.warning(f"Broker {broker} health check failed: {e}")
        
        self.last_health_check = current_time

    # System Circuit Breaker Management
    
    async def register_circuit_breaker(self, component_id: str, 
                                     failure_threshold: int = 5,
                                     recovery_timeout_us: int = 300_000_000,
                                     dependency_components: Optional[List[str]] = None) -> None:
        """Register a component with system circuit breaker management."""
        config = CircuitBreakerConfig(
            component_id=component_id,
            failure_threshold=failure_threshold,
            recovery_timeout_us=recovery_timeout_us,
            dependency_components=dependency_components or []
        )
        await self.system_circuit_breaker.register_component(config)
        logger.info(f"Registered circuit breaker for component: {component_id}")
        
        # Broadcast initial state so downstream consumers have a baseline view
        # NOTE: Fire-and-forget to avoid blocking during initialization
        # If Kafka isn't ready yet, this will fail silently and be retried later
        try:
            initial_state = await self.system_circuit_breaker.get_component_state(component_id)
            if initial_state:
                # Use asyncio.create_task for fire-and-forget (don't await)
                asyncio.create_task(self._publish_breaker_state_safe(initial_state, component_id))
        except Exception as exc:
            logger.debug(f"Unable to get initial breaker state for {component_id}: {exc}")
    
    async def _publish_breaker_state_safe(self, state, component_id: str):
        """
        Safely publish breaker state without blocking caller.
        Handles cases where Kafka/producer isn't ready yet.
        """
        try:
            await asyncio.wait_for(self._publish_breaker_state(state), timeout=2.0)
        except asyncio.TimeoutError:
            logger.debug(f"Timeout publishing initial breaker state for {component_id} (Kafka may not be ready)")
        except Exception as exc:
            logger.debug(f"Failed to publish initial breaker state for {component_id}: {exc}")
    
    async def can_component_execute(self, component_id: str) -> bool:
        """Check if component can execute based on circuit breaker state."""
        return await self.system_circuit_breaker.can_component_execute(component_id)
    
    async def record_component_success(self, component_id: str,
                                       reason: str = "success",
                                       severity: str = "low",
                                       metadata: Optional[Dict[str, Any]] = None) -> None:
        """Publish a recovery intent for the specified component and apply locally."""
        intent = BreakerIntent(
            component_id=component_id,
            intent="recover",
            reason=reason,
            severity=severity,
            requested_by="streaming_bus",
            metadata=metadata or {}
        )
        await self.publish_breaker_intent(intent, apply_locally=True)
    
    async def record_component_failure(self, component_id: str, 
                                     cascade_failure: bool = True,
                                     reason: str = "failure",
                                     severity: str = "high") -> None:
        """Publish a breaker intent requesting a trip for the component and apply locally."""
        intent = BreakerIntent(
            component_id=component_id,
            intent="trip",
            reason=reason,
            severity=severity,
            requested_by="streaming_bus",
            metadata={"cascade_failure": cascade_failure}
        )
        await self.publish_breaker_intent(intent, apply_locally=True)
        logger.warning(f"Requested breaker trip for component: {component_id} (reason={reason})")
    
    async def publish_breaker_intent(self, intent: BreakerIntent,
                                     apply_locally: bool = False) -> Optional[Dict[str, Any]]:
        """
        Publish a breaker intent to the shared control topic and optionally apply it locally.
        
        Agents should call this instead of mutating circuit breaker state directly so that
        the orchestrator/bus remains the source of truth for breaker coordination.
        """
        payload = {
            "component_id": intent.component_id,
            "intent": intent.intent,
            "reason": intent.reason,
            "severity": intent.severity,
            "requested_by": intent.requested_by,
            "timestamp_utc_us": intent.timestamp_us,
            "metadata": intent.metadata or {}
        }
        
        headers = {
            "source": "system_circuit_breaker",
            "component_id": intent.component_id,
            "intent": intent.intent
        }
        
        try:
            if not hasattr(self, '_breaker_intent_sequence'):
                self._breaker_intent_sequence = 0
            self._breaker_intent_sequence += 1
            
            await self.publish_with_canonical_headers(
                topic="control.breaker_intent",
                partition_key=intent.component_id,
                payload=payload,
                source_id="system_circuit_breaker.orchestrator",
                sequence_number=self._breaker_intent_sequence,
                correlation_id=f"breaker_{intent.component_id}_{int(time.time() * 1000)}",
                producer_version="1.0.0"
            )
        except Exception as exc:
            logger.warning(f"Failed to publish breaker intent for {intent.component_id}: {exc}")
        
        state_snapshot: Optional[Dict[str, Any]] = None
        if apply_locally:
            try:
                state_snapshot = await self.system_circuit_breaker.submit_manual_intent(intent)
                if state_snapshot:
                    await self._publish_breaker_state(state_snapshot)
            except Exception as exc:
                logger.error(f"Failed to apply breaker intent locally for {intent.component_id}: {exc}")
        
        return state_snapshot
    
    async def apply_breaker_intent(self, intent: BreakerIntent) -> Optional[Dict[str, Any]]:
        """
        Apply a breaker intent without re-publishing it. Intended for the orchestrator
        once a decision has been made on a submitted intent.
        """
        try:
            state_snapshot = await self.system_circuit_breaker.submit_manual_intent(intent)
            if state_snapshot:
                await self._publish_breaker_state(state_snapshot)
            return state_snapshot
        except Exception as exc:
            logger.error(f"Failed to apply breaker intent for {intent.component_id}: {exc}")
            return None
    
    async def _publish_breaker_state(self, state: Dict[str, Any]) -> None:
        """
        Broadcast the current breaker state snapshot so that downstream components can react.
        """
        try:
            if not hasattr(self, '_breaker_state_sequence'):
                self._breaker_state_sequence = 0
            self._breaker_state_sequence += 1
            
            await self.publish_with_canonical_headers(
                topic="control.breaker_state",
                partition_key=state.get("component_id", "unknown"),
                payload=state,
                source_id="system_circuit_breaker.monitor",
                sequence_number=self._breaker_state_sequence,
                correlation_id=f"breaker_state_{state.get('component_id', 'unknown')}_{int(time.time() * 1000)}",
                producer_version="1.0.0"
            )
        except Exception as exc:
            logger.warning(f"Failed to publish breaker state for {state.get('component_id')}: {exc}")
    
    async def get_system_health_status(self) -> Dict[str, Any]:
        """Get comprehensive system health status."""
        circuit_status = await self.system_circuit_breaker.get_system_health_status()
        
        # Add transport-level health
        transport_health = {
            "broker_connections": len(self.producer_pool.producers),
            "active_consumers": len(self.consumers),
            "paused_topics": list(self.circuit_breakers.keys()),
            "metrics": await self.get_transport_metrics()
        }
        
        return {
            "system_circuit_breakers": circuit_status,
            "transport_health": transport_health,
            "timestamp_utc_us": int(time.time_ns() // 1000)
        }
    
    async def publish_with_circuit_breaker_check(self, component_id: str, topic: str, 
                                               partition_key: str, payload: Dict[str, Any],
                                               source_id: str, sequence_number: int,
                                               correlation_id: Optional[str] = None,
                                               producer_version: str = "1.0.0",
                                               dedupe_key: Optional[str] = None,
                                               time_alignment_id: Optional[str] = None) -> bool:
        """
        Publish with circuit breaker protection.
        Returns False if component circuit is open.
        """
        # Check if component can execute
        if not await self.can_component_execute(component_id):
            logger.warning(f"Component {component_id} blocked by circuit breaker")
            return False
        
        try:
            # Attempt publication with canonical headers
            success = await self.publish_with_canonical_headers(
                topic=topic,
                partition_key=partition_key,
                payload=payload,
                source_id=source_id,
                sequence_number=sequence_number,
                correlation_id=correlation_id,
                producer_version=producer_version,
                dedupe_key=dedupe_key,
                time_alignment_id=time_alignment_id
            )
            
            if success:
                await self.record_component_success(component_id)
            else:
                await self.record_component_failure(component_id)
            
            return success
            
        except Exception as e:
            logger.error(f"Publication failed for component {component_id}: {e}")
            await self.record_component_failure(component_id)
            return False

    async def _check_consumer_lag(self) -> None:
        """Check consumer lag for all active consumers."""
        for group_id, consumer in self.consumers.items():
            try:
                if hasattr(consumer, 'assignment') and hasattr(consumer, 'position'):
                    assignment = consumer.assignment()
                    
                    for tp in assignment:
                        try:
                            # Get current position and high water mark
                            position = await consumer.position(tp)
                            
                            # Update topic metrics with lag
                            topic_name = tp.topic
                            # Simple lag approximation (would need broker query for exact)
                            estimated_lag = max(0, self.topic_metrics[topic_name]["message_count"] - position)
                            self.topic_metrics[topic_name]["lag_ms"] = estimated_lag
                            
                            # Update Prometheus metrics
                            if METRICS_AVAILABLE and _metrics_collector:
                                _metrics_collector.set_gauge(
                                    'kafka_consumer_lag',
                                    estimated_lag,
                                    labels={
                                        'consumer_group': group_id,
                                        'topic': topic_name
                                    }
                                )
                            
                            # Check lag thresholds
                            if estimated_lag > self.consumer_lag_thresholds["critical"]:
                                logger.error(f"Critical consumer lag: {group_id} on {topic_name} lag={estimated_lag}")
                            elif estimated_lag > self.consumer_lag_thresholds["warning"]:
                                logger.warning(f"High consumer lag: {group_id} on {topic_name} lag={estimated_lag}")
                                
                        except Exception as e:
                            logger.debug(f"Could not check lag for {tp}: {e}")
                            
            except Exception as e:
                logger.debug(f"Could not check consumer lag for {group_id}: {e}")
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get comprehensive health status."""
        current_time = time.time()
        
        # Overall broker health
        healthy_brokers = sum(1 for h in self.broker_health.values() if h.get("status") == "healthy")
        total_brokers = len(self.bootstrap_servers)
        
        # Consumer lag summary
        high_lag_topics = []
        for topic, metrics in self.topic_metrics.items():
            lag = metrics.get("lag_ms", 0)
            if lag > self.consumer_lag_thresholds["warning"]:
                high_lag_topics.append({"topic": topic, "lag": lag})
        
        return {
            "overall_status": "healthy" if healthy_brokers == total_brokers else "degraded",
            "healthy": healthy_brokers == total_brokers,
            "brokers": {
                "healthy": healthy_brokers,
                "total": total_brokers,
                "details": self.broker_health
            },
            "consumers": {
                "active_count": len(self.consumers),
                "high_lag_topics": high_lag_topics
            },
            "rate_limiting": {
                "adaptive_enabled": self.enable_adaptive_rate_limiting and self.adaptive_rate_limiter_pool is not None,
                "adaptive_metrics": self.get_rate_budget_metrics() if self.adaptive_rate_limiter_pool else None
            },
            "last_health_check": self.last_health_check,
            "uptime_seconds": current_time - getattr(self, '_start_time', current_time)
        }
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get comprehensive transport and backpressure metrics."""
        metrics = self.metrics.copy()
        
        # Add backpressure metrics if enabled
        if self.backpressure_enabled and self.memory_governor:
            metrics["backpressure"] = {
                "enabled": True,
                "messages_dropped": self.backpressure_metrics["messages_dropped"],
                "messages_throttled": self.backpressure_metrics["messages_throttled"],
                "total_throttle_wait_ms": self.backpressure_metrics["throttle_wait_time_ms"],
                "current_pressure_level": self.memory_governor.get_memory_pressure_level(),
                "total_memory_mb": self.memory_governor.get_total_memory_usage_mb()
            }
        else:
            metrics["backpressure"] = {"enabled": False}
        
        # Add workload distribution metrics if enabled
        if self.workload_distributor and self.enable_workload_distribution:
            workload_metrics = self.get_workload_distributor_metrics()
            metrics["workload_distribution"] = workload_metrics
            
            # Push to Prometheus if enabled
            if workload_metrics.get("enabled"):
                try:
                    from infra.monitoring.prometheus_metrics import get_metrics_collector
                    prom_collector = get_metrics_collector()
                    prom_collector.record_workload_distributor_metrics(workload_metrics)
                except Exception as e:
                    logger.debug(f"Could not record workload metrics to Prometheus: {e}")
        else:
            metrics["workload_distribution"] = {"enabled": False}
        
        return metrics
    
    def get_compression_status(self) -> Dict[str, Any]:
        """Get comprehensive compression library status and capabilities."""
        return {
            "available_libraries": COMPRESSION_LIBRARIES.copy(),
            "best_available": get_best_available_compression(),
            "recommendations": {
                "high_frequency": "lz4" if COMPRESSION_LIBRARIES.get('lz4') else "snappy",
                "time_series": "zstd" if COMPRESSION_LIBRARIES.get('zstd') else "lz4", 
                "events": "gzip" if COMPRESSION_LIBRARIES.get('gzip') else "none",
                "general": get_best_available_compression()
            },
            "installation_commands": {
                "lz4": "pip install lz4",
                "snappy": "pip install python-snappy", 
                "zstd": "pip install zstandard",
                "gzip": "built-in (always available)"
            }
        }
    
    async def validate_compression_performance(self) -> Dict[str, Any]:
        """Test compression performance with sample data for optimization guidance."""
        # Sample time series data (typical market data structure)
        sample_data = {
            "timestamp": int(time.time() * 1000000),
            "symbol": "BTCUSDT",
            "side": "buy",
            "price": 45678.91,
            "quantity": 0.12345,
            "venue": "binance",
            "metadata": {
                "latency_us": 150,
                "sequence": 12345,
                "trade_id": "abc123def456"
            }
        }
        
        # Serialize to JSON for testing
        json_data = json.dumps(sample_data).encode('utf-8')
        results = {}
        
        # Test each available compression
        for comp_type, available in COMPRESSION_LIBRARIES.items():
            if not available:
                results[comp_type] = {"available": False, "reason": "library_not_installed"}
                continue
                
            try:
                start_time = time.perf_counter()
                compressed = b''
                decompressed = b''
                
                if comp_type == 'gzip':
                    import gzip
                    compressed = gzip.compress(json_data)
                    decompressed = gzip.decompress(compressed)
                elif comp_type == 'lz4':
                    import lz4.frame
                    compressed = lz4.frame.compress(json_data)
                    decompressed = lz4.frame.decompress(compressed)
                elif comp_type == 'snappy':
                    import snappy
                    compressed = snappy.compress(json_data)
                    decompressed = snappy.decompress(compressed)
                elif comp_type == 'zstd':
                    import zstandard
                    cctx = zstandard.ZstdCompressor()
                    compressed = cctx.compress(json_data)
                    dctx = zstandard.ZstdDecompressor()
                    decompressed = dctx.decompress(compressed)
                else:
                    results[comp_type] = {"available": False, "reason": "unknown_compression_type"}
                    continue
                
                end_time = time.perf_counter()
                
                # Verify round-trip integrity
                if decompressed != json_data:
                    results[comp_type] = {"available": False, "reason": "integrity_check_failed"}
                    continue
                
                results[comp_type] = {
                    "available": True,
                    "original_size": len(json_data),
                    "compressed_size": len(compressed),
                    "compression_ratio": len(json_data) / len(compressed) if len(compressed) > 0 else 0,
                    "time_us": (end_time - start_time) * 1000000,
                    "throughput_mb_per_sec": (len(json_data) / (1024 * 1024)) / (end_time - start_time) if (end_time - start_time) > 0 else 0
                }
                
            except Exception as e:
                results[comp_type] = {"available": False, "reason": f"test_error: {str(e)}"}
        
        # Add recommendations based on results
        recommendations = {}
        if results:
            # Find fastest compression for low-latency scenarios
            fastest = min([r for r in results.values() if r.get("available", False)], 
                         key=lambda x: x.get("time_us", float('inf')), default=None)
            if fastest:
                fastest_name = [k for k, v in results.items() if v == fastest][0]
                recommendations["lowest_latency"] = fastest_name
                
            # Find best compression ratio for storage efficiency
            best_ratio = max([r for r in results.values() if r.get("available", False)], 
                           key=lambda x: x.get("compression_ratio", 0), default=None)
            if best_ratio:
                best_ratio_name = [k for k, v in results.items() if v == best_ratio][0]
                recommendations["best_compression"] = best_ratio_name
        
        return {
            "test_results": results,
            "recommendations": recommendations,
            "summary": {
                "total_libraries_tested": len([r for r in results.values() if r.get("available", False)]),
                "best_available_compression": get_best_available_compression()
            }
        }
    
    async def graceful_shutdown(self) -> None:
        """Enhanced graceful shutdown with final commits and health monitoring cleanup."""
        logger.info("Starting graceful shutdown of streaming bus...")
        
        # Stop adaptive rate limiter pool
        if self.enable_adaptive_rate_limiting and self.adaptive_rate_limiter_pool:
            await self.adaptive_rate_limiter_pool.shutdown()
            logger.info("Adaptive rate limiter pool shutdown complete")
        
        # Stop health monitoring first
        await self.stop_health_monitoring()
        # Final commits for all consumer pools
        for consumer_group, worker_pool in self.consumer_pools.items():
            try:
                logger.info(f"Draining worker pool for {consumer_group}")
                # Allow worker pool to finish pending tasks
                await asyncio.sleep(0.1)  # Brief drain period
                logger.info(f"Worker pool {consumer_group} drained")
            except Exception as e:
                logger.error(f"Error draining worker pool {consumer_group}: {e}")
        
        # Close consumers with final commits
        for consumer_key, consumer in self.consumers.items():
            try:
                if hasattr(consumer, 'commit'):
                    await consumer.commit()  # Final commit
                if hasattr(consumer, 'stop'):
                    await consumer.stop()
                logger.info(f"Gracefully closed consumer: {consumer_key}")
            except Exception as e:
                logger.error(f"Error closing consumer {consumer_key}: {e}")
        
        # Close producer pool
        try:
            await self.producer_pool.shutdown()
            logger.info("Producer pool shutdown complete")
        except Exception as e:
            logger.error(f"Error shutting down producer pool: {e}")
        
        # Close admin client
        if self.admin_client:
            try:
                if hasattr(self.admin_client, 'close'):
                    await self.admin_client.close()
                logger.info("Admin client closed")
            except Exception as e:
                logger.error(f"Error closing admin client: {e}")
        
        self.consumers.clear()
        self.consumer_pools.clear()
        
        logger.info("Enhanced streaming bus shutdown complete")
    
    def __del__(self):
        """Destructor to ensure cleanup if shutdown wasn't called."""
        # Check if there are any unclosed producers
        if hasattr(self, 'producer_pool') and self.producer_pool:
            if hasattr(self.producer_pool, 'producers') and self.producer_pool.producers:
                logger.warning("StreamingBus deleted without proper shutdown - some resources may not be cleaned up")
                # Can't await in __del__, but log the issue
    
    # Alias for backward compatibility
    async def shutdown(self) -> None:
        """Gracefully shutdown all producers and consumers."""
        await self.graceful_shutdown()
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with guaranteed cleanup."""
        await self.graceful_shutdown()
        return False

# Pure transport example usage
async def main():
    """Pure transport example usage of the streaming bus."""
    
    config = {
        "bootstrap_servers": ["localhost:9092"],
        "client_id": "satoshi-transport-test",
        "security_protocol": "PLAINTEXT",  # Use SSL/SASL in production
        "environment": "development"
    }
    
    bus = StreamingBus(config)
    
    # Create topics from configuration (transport concern)
    await bus.create_topics_from_config()
    
    # Pure transport message handler
    async def handle_raw_message(topic: str, partition_key: str, payload: Dict[str, Any], headers: Dict[str, str]):
        print(f"📦 Transport received: topic={topic}, key={partition_key}")
        print(f"   Headers: {headers}")
        print(f"   Payload keys: {list(payload.keys())}")
    
    # Test transactional publishing (pure transport)
    test_messages = []
    for i in range(3):
        message = {
            "topic": "clean.market.trades",
            "partition_key": "BTCUSDT",
            "payload": {
                "symbol": "BTCUSDT",
                "price": 50000 + i * 100,
                "quantity": 1.0,
                "side": "buy"
            },
            "headers": {
                "record_id": f"test_{i}_{int(time.time_ns())}",
                "schema_version": "trade.v2",
                "event_ts": str(int(time.time_ns()))
            }
        }
        test_messages.append(message)
    
    # Publish atomically (transport operation)
    success = await bus.publish_transactional(test_messages)
    print(f"✅ Transport transactional publish: {'SUCCESS' if success else 'FAILED'}")
    
    # Test single message (pure transport)
    single_payload = {
        "symbol": "ETHUSDT",
        "price": 3000,
        "quantity": 5.0,
        "side": "sell"
    }
    
    single_headers = {
        "record_id": f"single_{int(time.time_ns())}",
        "schema_version": "trade.v2",
        "exchange": "binance"
    }
    
    await bus.publish_with_headers("clean.market.trades", "ETHUSDT", single_payload, single_headers)
    print("✅ Published single message via transport")
    
    # Test circuit breaker (transport control)
    await bus.set_circuit_breaker("clean.market.trades", True)
    print("⚠️  Transport circuit breaker activated")
    
    await bus.set_circuit_breaker("clean.market.trades", False)
    print("✅ Transport circuit breaker deactivated")
    
    # Transport incident example (pure transport - no business logic)
    incident_payload = {
        "incident_id": f"incident_{int(time.time_ns())}",
        "class": "Freshness",
        "severity": "warning", 
        "evidence": {"stream": "trades.binance.BTC-PERP", "staleness_ms": 5000},
        "impacted_streams": ["features.momentum"],
        "proposed_action": "CircuitBreak"
    }
    
    await bus.publish_with_headers(
        topic="incidents.all",
        partition_key="Freshness", 
        payload=incident_payload,
        headers={"incident_class": "Freshness", "severity": "warning"}
    )
    print("✅ Published incident via transport")
    
    # Show transport metrics (no domain KPIs)
    metrics = await bus.get_transport_metrics()
    print(f"\n📊 Transport Metrics:")
    print(f"   Messages sent: {metrics['messages_sent']}")
    print(f"   Transactions committed: {metrics['transactions_committed']}")
    print(f"   Latency p95: {metrics['latency_percentiles_us']['p95']:.1f}μs")
    print(f"   Circuit breakers: {metrics['circuit_breakers']}")
    
    # In production, your domain agents would use:
    # await bus.subscribe_with_worker_pool("trade-processor", ["clean.market.trades"], handle_raw_message, pool_size=32)
    
    await bus.graceful_shutdown()
    print("✅ Pure transport graceful shutdown complete")

if __name__ == "__main__":
    if KAFKA_AVAILABLE:
        asyncio.run(main())
    else:
        print("❌ Kafka not available. Install with: pip install aiokafka kafka-python")
