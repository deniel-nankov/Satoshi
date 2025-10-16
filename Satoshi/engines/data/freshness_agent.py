"""
Freshness Agent

Mission: Compute staleness per stream; raise circuit-breaker signals.
Outputs: incidents.Freshness (+ optional CircuitBreakerRequest).
SLO: false-positive <1/week; detection < 2× bar size.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Set, Tuple
from decimal import Decimal
from enum import Enum
from collections import deque, defaultdict
import statistics
import hashlib
from datetime import datetime, timezone, timedelta
from dataclasses import asdict
import json
from threading import Lock

# Streaming Bus Integration
from infra.bus.streaming_bus import StreamingBus, BreakerIntent

logger = logging.getLogger(__name__)


@dataclass
class SLOMetrics:
    """Comprehensive SLO tracking with bulletproof state management."""
    # Detection SLO: < 2× bar size
    detection_target_multiplier: float = 2.0
    detection_violations: int = 0
    detection_total_checks: int = 0
    detection_p95_delay_us: Optional[float] = None
    detection_max_delay_us: int = 0
    
    # False positive SLO: < 1/week  
    false_positive_target_weekly: float = 1.0
    false_positive_count_weekly: int = 0
    false_positive_rolling_window_us: int = 7 * 24 * 60 * 60 * 1_000_000  # 7 days
    false_positive_history: deque = field(default_factory=lambda: deque(maxlen=1000))
    
    # Confidence adjustment tracking
    min_confidence_threshold: float = 0.7
    confidence_adjustment_count: int = 0
    last_confidence_adjustment_us: int = 0
    adjustment_cooldown_us: int = 60 * 60 * 1_000_000  # 1 hour
    
    # Performance metrics
    week_start_us: int = 0
    last_slo_check_us: int = 0
    consecutive_slo_violations: int = 0
    max_consecutive_violations: int = 3
    
    # Thread safety
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)
    
    def __post_init__(self):
        """Initialize computed fields safely."""
        if not hasattr(self, '_lock'):
            self._lock = Lock()
        if self.week_start_us == 0:
            self.week_start_us = self._calculate_week_start_us(int(time.time() * 1_000_000))
    
    def reset_weekly_counters(self, now_us: int) -> None:
        """Reset weekly counters with thread safety."""
        with self._lock:
            current_week_start = self._calculate_week_start_us(now_us)
            if now_us >= self.week_start_us + self.false_positive_rolling_window_us:
                self.week_start_us = current_week_start
                self.false_positive_count_weekly = 0
                # Clean old entries from history
                cutoff_time = now_us - self.false_positive_rolling_window_us
                while self.false_positive_history and self.false_positive_history[0]['timestamp_us'] < cutoff_time:
                    self.false_positive_history.popleft()
    
    def _calculate_week_start_us(self, timestamp_us: int) -> int:
        """Calculate week start (Monday 00:00 UTC) for given timestamp."""
        dt = datetime.fromtimestamp(timestamp_us / 1_000_000, tz=timezone.utc)
        week_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = week_start - timedelta(days=dt.weekday())  # Monday
        return int(week_start.timestamp() * 1_000_000)
    
    def record_detection_attempt(self, staleness_us: int, threshold_us: float, detected_us: int, 
                               bar_size_us: Optional[float] = None) -> bool:
        """Record detection attempt and check SLO compliance."""
        with self._lock:
            self.detection_total_checks += 1
            
            # Calculate detection delay relative to SLO target (bar-based for accurate telemetry)
            if bar_size_us is not None:
                target_detection_time = bar_size_us * self.detection_target_multiplier
            else:
                # Fallback to threshold-based calculation
                target_detection_time = threshold_us * self.detection_target_multiplier
            
            is_violation = detected_us > target_detection_time
            
            if is_violation:
                self.detection_violations += 1
                self.detection_max_delay_us = max(self.detection_max_delay_us, detected_us)
                self.consecutive_slo_violations += 1
            else:
                self.consecutive_slo_violations = 0
            
            # Update P95 using improved exponential quantile estimation for smoother telemetry
            if self.detection_total_checks > 0:
                alpha = min(0.1, 10.0 / self.detection_total_checks)  # Adaptive learning rate
                if self.detection_p95_delay_us is None:
                    self.detection_p95_delay_us = float(detected_us)
                else:
                    # Enhanced quantile tracking with smoother convergence
                    target_quantile = 0.95
                    indicator = 1.0 if detected_us > self.detection_p95_delay_us else 0.0
                    quantile_error = target_quantile - indicator
                    adjustment = alpha * quantile_error * (detected_us - self.detection_p95_delay_us)
                    self.detection_p95_delay_us += adjustment
            
            return is_violation
    
    def record_false_positive(self, timestamp_us: int, stream_name: str, confidence: float) -> None:
        """Record false positive with full context."""
        with self._lock:
            self.false_positive_count_weekly += 1
            self.false_positive_history.append({
                'timestamp_us': timestamp_us,
                'stream_name': stream_name,
                'confidence': confidence,
                'week_number': self._get_week_number(timestamp_us)
            })
    
    def _get_week_number(self, timestamp_us: int) -> int:
        """Get week number for timestamp."""
        dt = datetime.fromtimestamp(timestamp_us / 1_000_000, tz=timezone.utc)
        return dt.isocalendar()[1]
    
    def get_false_positive_rate_weekly(self) -> float:
        """Get current false positive rate per week."""
        with self._lock:
            return self.false_positive_count_weekly
    
    def should_adjust_confidence_threshold(self, now_us: int) -> bool:
        """Determine if confidence threshold needs adjustment."""
        with self._lock:
            # Check cooldown
            if now_us - self.last_confidence_adjustment_us < self.adjustment_cooldown_us:
                return False
            
            # Check for consecutive SLO violations
            if self.consecutive_slo_violations >= self.max_consecutive_violations:
                return True
            
            # Check false positive rate
            fp_rate = self.get_false_positive_rate_weekly()
            if fp_rate > self.false_positive_target_weekly * 1.5:  # 50% over target
                return True
            
            return False
    
    def adjust_confidence_threshold(self, now_us: int) -> float:
        """Adjust confidence threshold based on SLO performance with bounded changes."""
        with self._lock:
            old_threshold = self.min_confidence_threshold
            fp_rate = self.get_false_positive_rate_weekly()
            
            # Conservative adjustment bounds
            max_adjustment = 0.05  # Maximum 5% change per adjustment
            
            # Increase threshold if too many false positives
            if fp_rate > self.false_positive_target_weekly:
                adjustment = min(max_adjustment, (fp_rate - self.false_positive_target_weekly) * 0.02)
                self.min_confidence_threshold = min(0.95, self.min_confidence_threshold + adjustment)
            
            # Decrease threshold if false positives well under control and detection is slow
            elif (fp_rate < self.false_positive_target_weekly * 0.3 and 
                  self.detection_violations > self.detection_total_checks * 0.15):
                self.min_confidence_threshold = max(0.3, self.min_confidence_threshold - max_adjustment)
            
            # Ensure threshold stayed within absolute bounds
            self.min_confidence_threshold = max(0.3, min(0.95, self.min_confidence_threshold))
            
            self.last_confidence_adjustment_us = now_us
            self.confidence_adjustment_count += 1
            
            logger.info(f"Adjusted confidence threshold: {old_threshold:.3f} → {self.min_confidence_threshold:.3f} "
                       f"(FP rate: {fp_rate:.2f}, violations: {self.consecutive_slo_violations})")
            
            return self.min_confidence_threshold
    
    def get_slo_compliance_report(self) -> Dict[str, Any]:
        """Generate comprehensive SLO compliance report."""
        with self._lock:
            detection_slo_compliance = 1.0 - (self.detection_violations / max(1, self.detection_total_checks))
            
            return {
                'detection_slo': {
                    'target_multiplier': self.detection_target_multiplier,
                    'compliance_rate': detection_slo_compliance,
                    'violations': self.detection_violations,
                    'total_checks': self.detection_total_checks,
                    'p95_delay_us': self.detection_p95_delay_us,
                    'max_delay_us': self.detection_max_delay_us,
                    'consecutive_violations': self.consecutive_slo_violations
                },
                'false_positive_slo': {
                    'target_weekly': self.false_positive_target_weekly,
                    'current_weekly': self.false_positive_count_weekly,
                    'compliance': self.false_positive_count_weekly <= self.false_positive_target_weekly,
                    'history_count': len(self.false_positive_history)
                },
                'confidence_adjustment': {
                    'current_threshold': self.min_confidence_threshold,
                    'adjustment_count': self.confidence_adjustment_count,
                    'last_adjustment_us': self.last_confidence_adjustment_us,
                    'cooldown_remaining_us': max(0, self.adjustment_cooldown_us - (int(time.time() * 1_000_000) - self.last_confidence_adjustment_us))
                },
                'overall_health': {
                    'slo_compliant': detection_slo_compliance > 0.95 and self.false_positive_count_weekly <= self.false_positive_target_weekly,
                    'week_start_us': self.week_start_us,
                    'last_check_us': self.last_slo_check_us
                }
            }
    
    def export_metrics(self) -> Dict[str, Any]:
        """Export all metrics for persistence/monitoring."""
        with self._lock:
            # Build dict explicitly to avoid unserializables like _lock and deque
            slo_metrics = {
                'detection_total_checks': self.detection_total_checks,
                'detection_violations': self.detection_violations,
                'detection_max_delay_us': self.detection_max_delay_us,
                'detection_p95_delay_us': self.detection_p95_delay_us,
                'detection_target_multiplier': self.detection_target_multiplier,
                'false_positive_count_weekly': self.false_positive_count_weekly,
                'false_positive_target_weekly': self.false_positive_target_weekly,
                'false_positive_rolling_window_us': self.false_positive_rolling_window_us,
                'min_confidence_threshold': self.min_confidence_threshold,
                'confidence_adjustment_count': self.confidence_adjustment_count,
                'last_confidence_adjustment_us': self.last_confidence_adjustment_us,
                'adjustment_cooldown_us': self.adjustment_cooldown_us,
                'week_start_us': self.week_start_us,
                'last_slo_check_us': self.last_slo_check_us,
                'consecutive_slo_violations': self.consecutive_slo_violations,
                'max_consecutive_violations': self.max_consecutive_violations
            }
            
            return {
                'slo_metrics': slo_metrics,
                'false_positive_history': list(self.false_positive_history),
                'export_timestamp_us': int(time.time() * 1_000_000)
            }


def median_absolute_deviation(data: List[float]) -> float:
    """Calculate Median Absolute Deviation (MAD) for robust statistics."""
    if not data:
        return 0.0
    median = statistics.median(data)
    mad = statistics.median([abs(x - median) for x in data])
    return mad * 1.4826  # Scale factor for normal distribution consistency


def monotonic_time_us() -> int:
    """Get monotonic time in microseconds for lag measurement."""
    return int(time.monotonic() * 1_000_000)


class FreshnessLevel(Enum):
    FRESH = "fresh"
    STALE = "stale" 
    VERY_STALE = "very_stale"
    CRITICAL = "critical"


class CircuitBreakerState(Enum):
    CLOSED = "closed"     # Normal operation
    OPEN = "open"         # Circuit tripped, blocking requests
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class StreamConfig:
    """Configuration for freshness monitoring of a data stream with robust estimation."""
    stream_name: str
    expected_interval_us: int  # Expected time between updates (microseconds)
    
    # Robust threshold estimation
    bar_estimation_window: int = 200  # Rolling window for median calculation
    bar_multiplier: float = 1.5  # k in threshold = bar * k + jitter * m
    jitter_multiplier: float = 2.0  # m in threshold = bar * k + jitter * m  
    min_threshold_us: int = 10_000_000  # Minimum threshold (10 seconds)
    max_threshold_us: int = 3600_000_000  # Maximum threshold (1 hour)
    
    # Hysteresis and consecutive checks
    confirmation_checks: int = 2  # Consecutive checks before raising incident
    confirmation_time_us: Optional[int] = None  # Time window for confirmation (default: 1x bar)
    clear_threshold_ratio: float = 0.5  # Ratio of threshold for clearing (hysteresis)
    clear_confirmation_checks: int = 2  # Consecutive clear checks before clearing
    
    # Detection frequency
    min_check_period_us: int = 10_000_000  # Minimum check period (10 seconds)
    check_period_ratio: float = 0.25  # Check period as ratio of estimated bar
    
    # Circuit breaker
    circuit_breaker_enabled: bool = True
    escalation_threshold_multiplier: float = 2.0  # 2x threshold for circuit breaker
    escalation_time_us: Optional[int] = None  # Time before escalation (default: 1x bar)
    cooldown_time_us: int = 300_000_000  # Cooldown after clearing (5 minutes)
    
    # Cold start and planned maintenance
    min_observations_for_arming: int = 10  # Observations before arming breaker
    planned_pause_windows: List[Tuple[int, int]] = field(default_factory=list)  # (start_us, end_us) tuples in UTC wall-clock microseconds
    
    # Incident deduplication
    incident_dedupe_window_us: int = 900_000_000  # 15 minutes
    
    # Clock and timing
    use_event_time: bool = True  # Use event time if available, otherwise arrival time
    max_event_time_skew_us: int = 300_000_000  # Max allowed event time skew (5 minutes)
    
    # Legacy compatibility
    staleness_threshold_multiplier: float = 2.0
    critical_threshold_multiplier: float = 5.0
    false_positive_tolerance: float = 0.02


@dataclass
class FreshnessIncident:
    """Enhanced freshness incident with comprehensive observability."""
    # Core incident data
    stream_name: str
    incident_type: str  # "staleness_detected", "freshness_restored", "circuit_breaker_triggered"
    level: FreshnessLevel
    timestamp_utc_us: int
    last_update_us: int
    staleness_duration_us: int
    expected_interval_us: int
    threshold_multiplier: float
    confidence: float = 1.0
    
    # Enhanced observability (optional)
    dedupe_key: str = ""
    bar_us: Optional[float] = None  # Estimated bar (median inter-arrival)
    jitter_us: Optional[float] = None  # Estimated jitter (MAD)
    threshold_us: Optional[float] = None  # Applied threshold
    critical_threshold_us: Optional[float] = None  # Critical escalation threshold
    estimator_window_size: int = 0  # Number of samples in estimation window
    checks_period_us: int = 0  # Current check period
    clock_source: str = "arrival_time"  # "event_time" or "arrival_time"
    first_seen_us: int = 0  # When this type of incident first occurred
    max_lag_seen_us: int = 0  # Maximum lag observed for this incident
    occurrence_count: int = 1  # How many times this incident occurred
    consecutive_misses: int = 0  # Number of consecutive stale checks
    
    # Original fields
    metadata: Dict[str, Any] = field(default_factory=dict)
    row_identifier: str = ""
    severity: str = "warning"  # "info", "warning", "error", "critical"
    
    def __post_init__(self):
        # Set severity based on level
        severity_map = {
            FreshnessLevel.STALE: "warning",
            FreshnessLevel.VERY_STALE: "error", 
            FreshnessLevel.CRITICAL: "critical"
        }
        if self.severity == "warning":  # Only override if default
            self.severity = severity_map.get(self.level, "warning")
        
        # Set enhanced fields if not provided
        if not self.first_seen_us:
            self.first_seen_us = self.timestamp_utc_us
        if not self.max_lag_seen_us:
            self.max_lag_seen_us = self.staleness_duration_us
    
    def update_occurrence(self, new_lag_us: int, now_us: int) -> None:
        """Update incident with new occurrence."""
        self.timestamp_utc_us = now_us
        self.occurrence_count += 1
        self.max_lag_seen_us = max(self.max_lag_seen_us, new_lag_us)
        self.staleness_duration_us = new_lag_us


@dataclass
class CircuitBreakerRequest:
    """Request to open/close circuit breaker for a stream."""
    stream_name: str
    action: str  # "open", "close", "half_open"
    reason: str
    timestamp_utc_us: int
    staleness_duration_us: int
    confidence: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StreamStats:
    """Consolidated stream statistics with enhanced capabilities."""
    stream_name: str
    last_update_us: int
    
    # Basic statistics (original fields)
    update_count: int = 0
    recent_intervals: deque = field(default_factory=lambda: deque(maxlen=100))
    avg_interval_us: Optional[float] = None
    std_interval_us: Optional[float] = None
    p95_interval_us: Optional[float] = None
    last_stats_update_us: int = field(default_factory=lambda: int(time.time() * 1_000_000))
    
    # Enhanced timing and updates
    last_event_time_us: int = 0  # Last event timestamp from data
    last_arrival_time_us: int = 0  # Last arrival timestamp (monotonic)
    last_batch_max_event_time_us: int = 0  # Max event time from last batch
    
    # Robust bar estimation
    inter_arrival_times: deque = field(default_factory=lambda: deque(maxlen=200))
    estimated_bar_us: Optional[float] = None
    estimated_jitter_us: Optional[float] = None
    last_bar_update_us: int = 0
    
    # Hysteresis state
    consecutive_stale_checks: int = 0
    consecutive_fresh_checks: int = 0
    last_incident_state: Optional[FreshnessLevel] = None
    incident_confirmed: bool = False
    
    # Circuit breaker state
    circuit_breaker_state: CircuitBreakerState = CircuitBreakerState.CLOSED
    circuit_breaker_opened_at: Optional[int] = None
    circuit_breaker_escalated_at: Optional[int] = None
    circuit_breaker_last_cleared_at: Optional[int] = None
    escalation_start_time_us: Optional[int] = None
    
    # Incident deduplication
    last_incident_dedupe_key: Optional[str] = None
    last_incident_time_us: int = 0
    incident_count_in_window: int = 0
    
    # Cold start state
    observations_count: int = 0
    armed: bool = False
    
    # Performance metrics
    false_positive_count: int = 0
    total_incident_count: int = 0
    max_lag_seen_us: int = 0
    
    def update_intervals(self, new_interval_us: int):
        """Update interval statistics with new data point."""
        self.recent_intervals.append(new_interval_us)
        if len(self.recent_intervals) >= 2:
            intervals = list(self.recent_intervals)
            self.avg_interval_us = statistics.mean(intervals)
            if len(intervals) > 1:
                self.std_interval_us = statistics.stdev(intervals)
                self.p95_interval_us = sorted(intervals)[int(len(intervals) * 0.95)]
    
    def update_bar_estimation(self, new_interval_us: int, max_window: int = 200) -> None:
        """Update robust bar estimation using median and MAD."""
        if new_interval_us <= 0:
            return
            
        self.inter_arrival_times.append(new_interval_us)
        
        if len(self.inter_arrival_times) >= 5:  # Need minimum samples
            intervals = list(self.inter_arrival_times)
            self.estimated_bar_us = statistics.median(intervals)
            self.estimated_jitter_us = median_absolute_deviation(intervals)
            self.last_bar_update_us = monotonic_time_us()
    
    def get_robust_threshold_us(self, config: StreamConfig) -> int:
        """Calculate robust threshold using median + MAD."""
        if self.estimated_bar_us is None or self.estimated_jitter_us is None:
            # Fallback to configured expected interval (bounded by max)
            fallback = int(config.expected_interval_us * config.bar_multiplier)
            return max(config.min_threshold_us, 
                      min(fallback, config.max_threshold_us))
        
        # Robust threshold: bar * k + jitter * m
        threshold = (self.estimated_bar_us * config.bar_multiplier + 
                    self.estimated_jitter_us * config.jitter_multiplier)
        
        # Bound within reasonable limits
        return max(config.min_threshold_us, 
                  min(int(threshold), config.max_threshold_us))
    
    def get_check_period_us(self, config: StreamConfig) -> int:
        """Calculate optimal check period to meet <2x bar detection."""
        if self.estimated_bar_us:
            optimal_period = int(self.estimated_bar_us * config.check_period_ratio)
        else:
            optimal_period = int(config.expected_interval_us * config.check_period_ratio)
        
        return max(config.min_check_period_us, optimal_period)
    
    def is_in_planned_pause(self, timestamp_us: int, config: StreamConfig) -> bool:
        """Check if timestamp falls within a planned pause window with grace period."""
        for start_us, end_us in config.planned_pause_windows:
            # Add 30-second grace period after pause ends to avoid false positives
            grace_period_us = 30_000_000
            if start_us <= timestamp_us <= (end_us + grace_period_us):
                return True
        return False
    
    def generate_dedupe_key(self, level: FreshnessLevel, threshold_us: int) -> str:
        """Generate deterministic deduplication key."""
        key_data = f"{self.stream_name}:{level.value}:{threshold_us}"
        return hashlib.md5(key_data.encode()).hexdigest()[:16]
    
    @property
    def false_positive_rate(self) -> float:
        """Calculate false positive rate."""
        if self.total_incident_count == 0:
            return 0.0
        return self.false_positive_count / self.total_incident_count


class FreshnessAgent:
    """
    Monitors data stream freshness and triggers circuit breakers.
    
    Key Features:
    - Adaptive threshold calculation based on observed patterns
    - False positive rate tracking and mitigation
    - Circuit breaker integration
    - Confidence-based incident reporting
    - SLO-driven tuning (false-positive <1/week, detection <2x bar size)
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.stream_configs: Dict[str, StreamConfig] = {}
        self.stream_stats: Dict[str, StreamStats] = {}
        self.running = False
        
        # Task management with health monitoring
        self._consumer_task: Optional[asyncio.Task] = None
        self._health_check_task: Optional[asyncio.Task] = None
        self._background_tasks: Set[asyncio.Task] = set()
        
        # Health monitoring configuration
        self._health_check_interval = config.get('health_check_interval', 30.0)  # seconds
        self._last_health_check = time.time()
        
        # Retry configuration for streaming operations
        self.retry_config = {
            'max_retries': config.get('max_retries', 3),
            'base_delay_ms': config.get('base_delay_ms', 1000),
            'max_delay_ms': config.get('max_delay_ms', 30000),
            'exponential_base': config.get('exponential_base', 2.0)
        }
        
        # Enhanced metrics tracking
        self.metrics = {
            'streams_monitored': 0,
            'incidents_generated': 0,
            'false_positives_detected': 0,
            'circuit_breakers_triggered': 0,
            'health_checks_performed': 0,
            'health_checks_failed': 0,
            'kafka_operations_retried': 0,
            'kafka_operations_failed': 0,
            'slo_violations_detected': 0,
            'confidence_adjustments_made': 0,
            'messages_processed': 0,
            'processing_errors': 0,
            'total_staleness_checks': 0
        }
        
        # Streaming Bus Integration
        streaming_config = self.config.get("streaming_bus", {
            "bootstrap_servers": "localhost:9092",
            "enable_ssl": False,
            "enable_sasl": False
        })
        self.streaming_bus = StreamingBus(streaming_config)
        
        # Component identification for circuit breaker
        self.component_id = "freshness_agent"
        
        # Generate unique circuit breaker ID for this instance
        self.circuit_breaker_id = f"freshness_agent_{id(self)}"
        self._circuit_breaker_registered = False
        
        # Circuit breaker dependencies - monitors all data collectors
        self.circuit_breaker_dependencies = [
            "exchange_connector",
            "options_chain_collector", 
            "onchain_collector"
        ]
        
        # Global configuration
        self.check_interval_us = config.get('check_interval_us', 30_000_000)  # 30 seconds
        self.stats_retention_hours = config.get('stats_retention_hours', 24)
        self.min_confidence_threshold = config.get('min_confidence_threshold', 0.7)
        
        # SLO tracking with adaptive adjustment
        self.slo_metrics = SLOMetrics()
        self.slo_metrics.min_confidence_threshold = self.min_confidence_threshold
        self.slo_metrics.detection_target_multiplier = config.get('detection_slo_multiplier', 2.0)
        self.slo_metrics.false_positive_target_weekly = config.get('false_positive_slo_weekly', 1.0)
        
        # Circuit breaker configuration
        self.circuit_breaker_timeout_us = config.get('circuit_breaker_timeout_us', 300_000_000)  # 5 minutes
        self.circuit_breaker_half_open_test_period_us = config.get('circuit_breaker_half_open_test_period_us', 60_000_000)  # 1 minute
        
        # Output queues
        self.output_queues = {
            'freshness_incidents': asyncio.Queue(maxsize=1000),
            'circuit_breaker_requests': asyncio.Queue(maxsize=100)
        }
        
        # Incident tracking for false positive analysis
        self.recent_incidents: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        
        # Incident deduplication cache
        self.last_incident_by_key: Dict[str, FreshnessIncident] = {}
    
    async def _register_circuit_breaker(self):
        """Register circuit breaker with streaming bus."""
        try:
            if not self._circuit_breaker_registered:
                await self.streaming_bus.register_circuit_breaker(
                    component_id=self.circuit_breaker_id,
                    failure_threshold=5,  # More tolerant for monitoring component
                    recovery_timeout_us=self.circuit_breaker_timeout_us,
                    dependency_components=self.circuit_breaker_dependencies
                )
                self._circuit_breaker_registered = True
                logger.info(f"Registered circuit breaker: {self.circuit_breaker_id}")
        except Exception as e:
            logger.error(f"Failed to register circuit breaker: {e}")
            raise
    
    def _get_week_start_us(self) -> int:
        """Get the start of the current week in microseconds."""
        now = datetime.now(timezone.utc)
        week_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = week_start - timedelta(days=now.weekday())  # Monday
        return int(week_start.timestamp() * 1_000_000)
    
    async def _perform_health_check(self) -> bool:
        """Perform comprehensive health check of freshness agent."""
        try:
            self.metrics['health_checks_performed'] += 1
            
            # Check streaming bus health
            if not self.streaming_bus or not hasattr(self.streaming_bus, 'producer'):
                self.metrics['health_checks_failed'] += 1
                return False
            
            # Check if we have registered streams
            if not self.stream_configs:
                logger.warning("Health check: No streams registered for monitoring")
                return True  # Not necessarily unhealthy
            
            # Check if we have recent data from monitored streams
            now_us = int(time.time() * 1_000_000)
            stale_streams = 0
            
            for stream_name, stats in self.stream_stats.items():
                if stats.armed:  # Only check armed streams
                    staleness_us = now_us - stats.last_update_us
                    config = self.stream_configs.get(stream_name)
                    if config:
                        threshold_us = self._calculate_staleness_threshold(stream_name, config, stats)
                        if staleness_us > threshold_us * 3:  # Very stale (3x threshold)
                            stale_streams += 1
            
            # Health is good if less than 50% of streams are very stale
            is_healthy = stale_streams < len([s for s in self.stream_stats.values() if s.armed]) * 0.5
            
            if not is_healthy:
                self.metrics['health_checks_failed'] += 1
                logger.warning(f"Health check failed: {stale_streams} very stale streams")
            
            self._last_health_check = time.time()
            return is_healthy
            
        except Exception as e:
            self.metrics['health_checks_failed'] += 1
            logger.error(f"Health check failed: {e}")
            return False
    
    async def _health_monitor_loop(self):
        """Background health monitoring loop."""
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
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get current health status of the freshness agent."""
        return {
            'component_id': self.circuit_breaker_id,
            'healthy': time.time() - self._last_health_check < self._health_check_interval * 2,
            'last_health_check': self._last_health_check,
            'circuit_breaker_registered': self._circuit_breaker_registered,
            'streams_monitored': len(self.stream_configs),
            'streams_armed': len([s for s in self.stream_stats.values() if s.armed]),
            'running': self.running,
            'metrics': self.metrics.copy()
        }
    
    def register_stream(self, stream_config: StreamConfig) -> None:
        """Register a stream for freshness monitoring."""
        self.stream_configs[stream_config.stream_name] = stream_config
        if stream_config.stream_name not in self.stream_stats:
            stats = StreamStats(
                stream_name=stream_config.stream_name,
                last_update_us=int(time.time() * 1_000_000)
            )
            # Set deque window size from config
            stats.inter_arrival_times = deque(maxlen=stream_config.bar_estimation_window)
            self.stream_stats[stream_config.stream_name] = stats
        
        self.metrics['streams_monitored'] = len(self.stream_configs)
        logger.info(f"Registered stream for freshness monitoring: {stream_config.stream_name}")
    
    async def _retry_with_backoff(self, operation_func, operation_name: str, *args, **kwargs):
        """Execute operation with exponential backoff retry."""
        last_exception = None
        
        for attempt in range(self.retry_config['max_retries'] + 1):
            try:
                result = await operation_func(*args, **kwargs)
                if attempt > 0:
                    logger.info(f"Retry succeeded for {operation_name} on attempt {attempt + 1}")
                return result
                
            except Exception as e:
                last_exception = e
                self.metrics['kafka_operations_retried'] += 1
                
                if attempt == self.retry_config['max_retries']:
                    self.metrics['kafka_operations_failed'] += 1
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
    
    def _simple_retry_sync(self, operation_func, operation_name: str, *args, **kwargs):
        """Simplified synchronous retry for non-async operations."""
        last_exception = None
        
        for attempt in range(self.retry_config['max_retries'] + 1):
            try:
                result = operation_func(*args, **kwargs)
                if attempt > 0:
                    logger.info(f"Sync retry succeeded for {operation_name} on attempt {attempt + 1}")
                return result
                
            except Exception as e:
                last_exception = e
                
                if attempt == self.retry_config['max_retries']:
                    logger.error(f"Sync retry failed for {operation_name} after {attempt + 1} attempts: {e}")
                    break
                
                import time
                delay_ms = min(
                    self.retry_config['base_delay_ms'] * (self.retry_config['exponential_base'] ** attempt),
                    self.retry_config['max_delay_ms']
                )
                
                logger.warning(f"Sync retry {attempt + 1}/{self.retry_config['max_retries']} for {operation_name} after {delay_ms}ms: {e}")
                time.sleep(delay_ms / 1000.0)
        
        # Re-raise the last exception if all retries failed
        if last_exception:
            raise last_exception
        else:
            raise RuntimeError(f"All sync retries failed for {operation_name} with no exception captured")
    
    def record_data_update(self, stream_name: str, timestamp_us: int, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Record that a stream received new data."""
        if stream_name not in self.stream_stats:
            logger.warning(f"Recording update for unregistered stream: {stream_name}")
            return
        
        stats = self.stream_stats[stream_name]
        now_us = int(time.time() * 1_000_000)
        now_monotonic_us = monotonic_time_us()
        
        # Calculate interval if this isn't the first update
        if stats.update_count > 0:
            # Use arrival domain for interval stats (monotonic clock)
            interval_us = now_monotonic_us - stats.last_arrival_time_us
            if interval_us > 0:  # Only record positive intervals
                stats.update_intervals(interval_us)
        
        # Clock domain separation for perfect clarity:
        # - last_update_us: wall clock fallback for staleness when arrival time unavailable
        # - last_arrival_time_us: monotonic clock for interval calculations and primary staleness
        # - timestamp_us param: arrival-agnostic (could be event time or wall time)
        stats.last_update_us = now_us  # Wall clock for fallback staleness calculation
        stats.last_arrival_time_us = now_monotonic_us  # Monotonic for intervals and primary staleness
        stats.update_count += 1
        stats.last_stats_update_us = now_us
        
        # Mirror arming logic from enhanced method
        config = self.stream_configs[stream_name]
        stats.observations_count += 1
        
        if (not stats.armed and 
            stats.observations_count >= config.min_observations_for_arming):
            stats.armed = True
            logger.info(f"Armed freshness monitoring for {stream_name}")
        
        # Update circuit breaker state if it was open
        if stats.circuit_breaker_state == CircuitBreakerState.OPEN:
            self._try_circuit_breaker_half_open(stream_name)
        elif stats.circuit_breaker_state == CircuitBreakerState.HALF_OPEN:
            close_request = self._close_circuit_breaker(stream_name, "data_received")
            # Actually enqueue the close request
            asyncio.create_task(self._enqueue_circuit_breaker_request(close_request))
    
    async def check_freshness(self) -> List[FreshnessIncident]:
        """Enhanced freshness check with confirmation, hysteresis, and proper gating."""
        incidents = []
        now_us = monotonic_time_us()
        wall_time_us = int(time.time() * 1_000_000)
        
        # Update metrics
        self.metrics['total_staleness_checks'] += 1
        
        # Update SLO tracking
        self.slo_metrics.reset_weekly_counters(wall_time_us)
        self.slo_metrics.last_slo_check_us = wall_time_us
        
        # Prune stale dedupe cache entries periodically (every ~10 minutes)
        if wall_time_us - getattr(self, '_last_cache_prune_us', 0) > 600_000_000:
            self._prune_dedupe_cache(wall_time_us)
            self._last_cache_prune_us = wall_time_us
        
        # Check if confidence threshold needs adjustment
        if self.slo_metrics.should_adjust_confidence_threshold(wall_time_us):
            new_threshold = self.slo_metrics.adjust_confidence_threshold(wall_time_us)
            self.min_confidence_threshold = new_threshold
            self.metrics['confidence_adjustments_made'] += 1
        
        for stream_name, config in self.stream_configs.items():
            if stream_name not in self.stream_stats:
                continue
                
            stats = self.stream_stats[stream_name]
            
            # Skip if not armed yet
            if not stats.armed:
                continue
            
            # Skip if in planned pause
            if stats.is_in_planned_pause(wall_time_us, config):
                continue
            
            # Choose consistent clock: event time vs arrival time
            if config.use_event_time and stats.last_event_time_us > 0:
                last_time_us = stats.last_event_time_us
                clock_source = "event_time"
                reference_time = wall_time_us  # Compare event time to wall time
            else:
                # Use monotonic arrival time if available, fallback to wall time
                if stats.last_arrival_time_us > 0:
                    last_time_us = stats.last_arrival_time_us
                    reference_time = now_us  # Compare monotonic to monotonic
                else:
                    last_time_us = stats.last_update_us  
                    reference_time = wall_time_us  # Fallback: wall to wall
                clock_source = "arrival_time"
            
            staleness_us = reference_time - last_time_us
            
            # Calculate robust threshold
            threshold_us = self._calculate_staleness_threshold(stream_name, config, stats)
            clear_threshold_us = threshold_us * config.clear_threshold_ratio
            # Align critical threshold with escalation logic (derive from robust threshold)
            critical_threshold_us = threshold_us * config.escalation_threshold_multiplier
            
            # Determine if currently stale
            is_stale = staleness_us > threshold_us
            is_fresh = staleness_us < clear_threshold_us
            
            # Update consecutive counters with hysteresis
            if is_stale:
                stats.consecutive_stale_checks += 1
                stats.consecutive_fresh_checks = 0
            elif is_fresh:
                stats.consecutive_fresh_checks += 1
                stats.consecutive_stale_checks = 0
            # else: in hysteresis zone, don't change counters
            
            # Only confirm incident after required consecutive checks
            should_emit_incident = (stats.consecutive_stale_checks >= config.confirmation_checks and 
                                  not stats.incident_confirmed)
            
            # Only clear incident after required consecutive fresh checks
            should_clear_incident = (stats.consecutive_fresh_checks >= config.clear_confirmation_checks and 
                                   stats.incident_confirmed)
            
            if should_emit_incident:
                # Determine freshness level
                level = self._determine_freshness_level(staleness_us, threshold_us, critical_threshold_us)
                
                # Calculate confidence in the assessment
                confidence = self._calculate_confidence(stream_name, staleness_us, threshold_us, stats)
                
                # Only report incidents if confidence is above threshold
                if level != FreshnessLevel.FRESH and confidence >= self.min_confidence_threshold:
                    # Check false positive budget
                    if self._should_create_incident(stream_name, level, confidence):
                        
                        # Real dedupe over window
                        incident = self._create_freshness_incident(
                            stream_name, level, staleness_us, threshold_us, 
                            config.expected_interval_us, confidence, wall_time_us, last_time_us,
                            critical_threshold_us, clock_source  # Pass clock source directly
                        )
                        
                        # Check for deduplication
                        if self._should_emit_after_dedupe(stats, incident, wall_time_us, config):
                            incidents.append(incident)
                            stats.incident_confirmed = True
                            self.metrics['incidents_generated'] += 1
                            
                            # Track detection SLO compliance
                            target_detection_time = threshold_us * self.slo_metrics.detection_target_multiplier
                            detection_delay = staleness_us
                            bar_size = stats.estimated_bar_us or config.expected_interval_us
                            
                            is_slo_violation = self.slo_metrics.record_detection_attempt(
                                staleness_us, threshold_us, detection_delay, bar_size
                            )
                            
                            if is_slo_violation:
                                self.metrics['slo_violations_detected'] += 1
                                logger.warning(f"Detection SLO violation for {stream_name}: "
                                             f"delay {detection_delay/1_000_000:.1f}s > "
                                             f"target {target_detection_time/1_000_000:.1f}s")
                            
                            # Handle circuit breaker escalation (gated)
                            await self._handle_circuit_breaker_escalation(stream_name, config, stats, 
                                                                        staleness_us, threshold_us, wall_time_us)
            
            # Check escalation even for confirmed incidents (staleness might worsen)
            elif stats.incident_confirmed and is_stale:
                await self._handle_circuit_breaker_escalation(stream_name, config, stats, 
                                                            staleness_us, threshold_us, wall_time_us)
            
            elif should_clear_incident:
                # Clear the incident state
                stats.incident_confirmed = False
                stats.last_incident_dedupe_key = None
                
                # Create freshness_restored incident for audit trail
                restoration_incident = self._create_freshness_incident(
                    stream_name, FreshnessLevel.FRESH, staleness_us, threshold_us,
                    config.expected_interval_us, 1.0, wall_time_us, last_time_us,
                    critical_threshold_us, clock_source
                )
                restoration_incident.incident_type = "freshness_restored"
                incidents.append(restoration_incident)
                
                # Handle circuit breaker cooldown and state transition
                if stats.circuit_breaker_escalated_at:
                    stats.circuit_breaker_last_cleared_at = wall_time_us
                    stats.circuit_breaker_escalated_at = None
                    
                    # Generate close request when breaker can close
                    close_request = CircuitBreakerRequest(
                        stream_name=stream_name,
                        action="close",
                        reason="Incident cleared after confirmation",
                        timestamp_utc_us=wall_time_us,
                        staleness_duration_us=0,  # No longer stale
                        confidence=1.0,
                        metadata={"cleared_at": wall_time_us}
                    )
                    await self._enqueue_circuit_breaker_request(close_request)
                    
                    stats.circuit_breaker_state = CircuitBreakerState.CLOSED  # Fully closed
        
        # Enqueue incidents
        for incident in incidents:
            await self._enqueue_freshness_incident(incident)
        
        return incidents
    
    def _should_emit_after_dedupe(self, stats: StreamStats, incident: FreshnessIncident, now_us: int, config: StreamConfig) -> bool:
        """Check if incident should be emitted after deduplication logic."""
        # If same dedupe key within window, update existing incident instead of emitting new one
        if (stats.last_incident_dedupe_key == incident.dedupe_key and 
            now_us - stats.last_incident_time_us < config.incident_dedupe_window_us):
            
            # Update cached incident with new occurrence data
            if incident.dedupe_key in self.last_incident_by_key:
                cached_incident = self.last_incident_by_key[incident.dedupe_key]
                # Increment occurrence count directly on cached object to stay in sync
                cached_incident.occurrence_count += 1
                cached_incident.max_lag_seen_us = max(cached_incident.max_lag_seen_us, incident.staleness_duration_us)
                cached_incident.consecutive_misses = incident.consecutive_misses  # Persist current miss streak
                cached_incident.timestamp_utc_us = now_us  # Update last seen time
                
            stats.incident_count_in_window += 1
            return False  # Don't emit, just update
        
        # New incident or outside dedupe window
        stats.last_incident_dedupe_key = incident.dedupe_key
        stats.last_incident_time_us = now_us
        stats.incident_count_in_window = 1
        
        # Cache the incident for future deduplication
        self.last_incident_by_key[incident.dedupe_key] = incident
        
        return True  # Emit this incident
    
    async def _handle_circuit_breaker_escalation(self, stream_name: str, config: StreamConfig, stats: StreamStats,
                                                staleness_us: int, threshold_us: float, now_us: int) -> None:
        """Handle circuit breaker escalation with proper gating."""
        if not config.circuit_breaker_enabled:
            return
        
        escalation_threshold = threshold_us * config.escalation_threshold_multiplier
        
        # Check cooldown period
        if (stats.circuit_breaker_last_cleared_at and 
            now_us - stats.circuit_breaker_last_cleared_at < config.cooldown_time_us):
            return  # Still in cooldown
        
        # Check if we should escalate
        if staleness_us > escalation_threshold:
            # Start escalation timer if not already started
            if stats.escalation_start_time_us is None:
                stats.escalation_start_time_us = now_us
                return
            
            # Check if escalation time has elapsed with safety bounds
            escalation_time = config.escalation_time_us or stats.estimated_bar_us or config.expected_interval_us
            escalation_time = max(5_000_000, min(escalation_time, 3600_000_000))  # 5s min, 1h max
            if now_us - stats.escalation_start_time_us >= escalation_time and not stats.circuit_breaker_escalated_at:
                # Escalate to circuit breaker - align request and state
                stats.circuit_breaker_escalated_at = now_us
                stats.circuit_breaker_state = CircuitBreakerState.OPEN  # Set state to match request
                self.metrics['circuit_breakers_triggered'] += 1
                
                # Create circuit breaker request with consistent action casing
                cb_request = CircuitBreakerRequest(
                    stream_name=stream_name,
                    action="open",  # Consistent lowercase action
                    reason=f"Escalated staleness: {staleness_us/1_000_000:.1f}s > {escalation_threshold/1_000_000:.1f}s",
                    timestamp_utc_us=now_us,
                    staleness_duration_us=staleness_us,
                    confidence=1.0,
                    metadata={"escalation_duration_us": now_us - stats.escalation_start_time_us}
                )
                await self._enqueue_circuit_breaker_request(cb_request)
        else:
            # Reset escalation timer if staleness drops
            stats.escalation_start_time_us = None
    
    def _calculate_staleness_threshold(self, stream_name: str, config: StreamConfig, stats: StreamStats) -> float:
        """Calculate robust staleness threshold using median+MAD when available."""
        # Use robust threshold if stats are armed and we have enough observations
        if stats.armed:
            try:
                robust_threshold = stats.get_robust_threshold_us(config)
                return robust_threshold
            except Exception:
                # Fallback if robust calculation fails
                pass
        
        # Fallback to simple multiplier when window too small or not armed
        return config.expected_interval_us * config.staleness_threshold_multiplier
    
    def _determine_freshness_level(self, staleness_us: int, threshold_us: float, critical_threshold_us: float) -> FreshnessLevel:
        """Determine the freshness level based on staleness."""
        if staleness_us <= threshold_us:
            return FreshnessLevel.FRESH
        elif staleness_us <= critical_threshold_us:
            return FreshnessLevel.STALE
        elif staleness_us <= critical_threshold_us * 2:
            return FreshnessLevel.VERY_STALE
        else:
            return FreshnessLevel.CRITICAL
    
    def _calculate_confidence(self, stream_name: str, staleness_us: int, threshold_us: float, stats: Any) -> float:
        """Calculate confidence in the freshness assessment with nuanced quality indicators."""
        base_confidence = 1.0
        
        # Factor 1: Distance from threshold (farther = more confident)
        threshold_ratio = staleness_us / threshold_us if threshold_us > 0 else 1.0
        if threshold_ratio < 1.5:  # Within 50% above threshold
            base_confidence *= 0.6
        elif threshold_ratio > 3.0:  # Very stale
            base_confidence *= 1.1  # Boost confidence for clearly stale data
        
        # Factor 2: Statistical confidence (more data = more confident)
        if hasattr(stats, 'recent_intervals'):
            n_observations = len(stats.recent_intervals)
            if n_observations < 5:
                base_confidence *= 0.7
            elif n_observations > 20:
                base_confidence *= 1.05  # Slight boost for lots of data
        
        # Factor 3: Pattern stability (stable patterns = more confident)
        if hasattr(stats, 'recent_intervals') and len(stats.recent_intervals) >= 3:
            intervals = list(stats.recent_intervals)
            cv = statistics.stdev(intervals) / statistics.mean(intervals) if statistics.mean(intervals) > 0 else float('inf')
            if cv < 0.1:  # Very stable
                base_confidence *= 1.1
            elif cv > 0.5:  # Very unstable
                base_confidence *= 0.8
        
        # Factor 4: Historical accuracy (past false positives reduce confidence)
        if hasattr(stats, 'false_positive_rate') and stats.false_positive_rate > 0.1:
            base_confidence *= (1.0 - min(0.3, stats.false_positive_rate))
        
        # Factor 5: SLO pressure (adapt to current SLO state)
        fp_rate = self.slo_metrics.get_false_positive_rate_weekly()
        if fp_rate >= self.slo_metrics.false_positive_target_weekly * 0.8:  # Approaching limit
            base_confidence *= 0.7
        
        return max(0.1, min(1.0, base_confidence))  # Clamp to [0.1, 1.0]
    
    def _should_create_incident(self, stream_name: str, level: FreshnessLevel, confidence: float) -> bool:
        """Determine if we should create an incident based on SLO constraints."""
        # Always create critical incidents
        if level == FreshnessLevel.CRITICAL:
            return True
        
        # Check false positive SLO compliance
        fp_rate = self.slo_metrics.get_false_positive_rate_weekly()
        if fp_rate >= self.slo_metrics.false_positive_target_weekly:
            # Only create high-confidence incidents when at/over SLO limit
            return confidence > 0.9
        
        return True
    
    def _create_freshness_incident(self, stream_name: str, level: FreshnessLevel, staleness_us: int, 
                                 threshold_us: float, expected_interval_us: int, confidence: float,
                                 now_us: int, last_update_us: int, critical_threshold_us: Optional[float] = None,
                                 clock_source: str = "arrival_time") -> FreshnessIncident:
        """Create an enhanced freshness incident with observability."""
        
        # Calculate adaptive check interval for timely detection
        next_check_interval_us = self._calculate_next_check_interval()
        
        # Use adaptive critical threshold based on robust threshold if available
        adaptive_critical_threshold_us = critical_threshold_us
        if critical_threshold_us is None:
            # Use adaptive calculation based on robust threshold (rides bar/MAD regime)
            adaptive_critical_threshold_us = threshold_us * 2.0  # More responsive than fixed multiplier
        
        # Generate dedupe key
        dedupe_key = hashlib.md5(f"{stream_name}:{level.value}:{int(threshold_us)}".encode()).hexdigest()[:16]
        
        # Get enhanced stats if available
        stats = self.stream_stats.get(stream_name)
        bar_us = stats.estimated_bar_us if stats else None
        jitter_us = stats.estimated_jitter_us if stats else None
        window_size = len(stats.inter_arrival_times) if stats else 0
        consecutive_misses = stats.consecutive_stale_checks if stats else 0
        
        incident = FreshnessIncident(
            stream_name=stream_name,
            incident_type="staleness_detected",
            level=level,
            timestamp_utc_us=now_us,
            last_update_us=last_update_us,
            staleness_duration_us=staleness_us,
            expected_interval_us=expected_interval_us,
            threshold_multiplier=staleness_us / expected_interval_us if expected_interval_us > 0 else 0,
            confidence=confidence,
            
            # Enhanced observability
            dedupe_key=dedupe_key,
            bar_us=bar_us,
            jitter_us=jitter_us,
            threshold_us=threshold_us,
            critical_threshold_us=adaptive_critical_threshold_us,
            estimator_window_size=window_size,
            checks_period_us=next_check_interval_us,  # Use calculated interval
            clock_source=clock_source,  # Use passed clock source
            first_seen_us=now_us,
            max_lag_seen_us=staleness_us,
            consecutive_misses=consecutive_misses,
            
            metadata={
                "threshold_us": threshold_us,
                "staleness_seconds": staleness_us / 1_000_000,
                "expected_interval_seconds": expected_interval_us / 1_000_000,
                "detection_time_us": now_us,
                "bar_seconds": bar_us / 1_000_000 if bar_us else None,
                "jitter_seconds": jitter_us / 1_000_000 if jitter_us else None
            }
        )
        
        # Track incident for false positive analysis
        self.recent_incidents[stream_name].append({
            'timestamp_us': now_us,
            'level': level,
            'confidence': confidence,
            'staleness_us': staleness_us,
            'dedupe_key': dedupe_key
        })
        
        if stream_name in self.stream_stats:
            self.stream_stats[stream_name].total_incident_count += 1
        
        return incident
    
    async def _handle_circuit_breaker(self, stream_name: str, incident: FreshnessIncident, 
                                    stats: StreamStats) -> Optional[CircuitBreakerRequest]:
        """Handle circuit breaker logic for stale streams."""
        if stats.circuit_breaker_state == CircuitBreakerState.CLOSED:
            return self._open_circuit_breaker(stream_name, incident)
        return None
    
    def _open_circuit_breaker(self, stream_name: str, incident: FreshnessIncident) -> CircuitBreakerRequest:
        """Open circuit breaker for a stream."""
        stats = self.stream_stats[stream_name]
        stats.circuit_breaker_state = CircuitBreakerState.OPEN
        stats.circuit_breaker_opened_at = incident.timestamp_utc_us
        
        return CircuitBreakerRequest(
            stream_name=stream_name,
            action="open",
            reason=f"Staleness detected: {incident.level.value}",
            timestamp_utc_us=incident.timestamp_utc_us,
            staleness_duration_us=incident.staleness_duration_us,
            confidence=incident.confidence,
            metadata={
                "incident_level": incident.level.value,
                "threshold_multiplier": incident.threshold_multiplier
            }
        )
    
    def _try_circuit_breaker_half_open(self, stream_name: str) -> None:
        """Try to move circuit breaker to half-open state."""
        stats = self.stream_stats[stream_name]
        now_us = int(time.time() * 1_000_000)
        
        if (stats.circuit_breaker_opened_at and 
            now_us - stats.circuit_breaker_opened_at >= self.circuit_breaker_timeout_us):
            stats.circuit_breaker_state = CircuitBreakerState.HALF_OPEN
            logger.info(f"Circuit breaker for {stream_name} moved to HALF_OPEN")
    
    def _close_circuit_breaker(self, stream_name: str, reason: str) -> CircuitBreakerRequest:
        """Close circuit breaker for a stream."""
        stats = self.stream_stats[stream_name]
        stats.circuit_breaker_state = CircuitBreakerState.CLOSED
        stats.circuit_breaker_opened_at = None
        
        return CircuitBreakerRequest(
            stream_name=stream_name,
            action="close",
            reason=reason,
            timestamp_utc_us=int(time.time() * 1_000_000),
            staleness_duration_us=0,
            confidence=1.0,
            metadata={"recovery_reason": reason}
        )
    
    def mark_false_positive(self, stream_name: str, incident_timestamp_us: int, confidence: float = 1.0) -> None:
        """Mark an incident as a false positive for SLO tracking and learning."""
        # Update stream-level stats
        if stream_name in self.stream_stats:
            stats = self.stream_stats[stream_name]
            if hasattr(stats, 'false_positive_count'):
                stats.false_positive_count += 1
        
        # Update SLO metrics
        self.slo_metrics.record_false_positive(incident_timestamp_us, stream_name, confidence)
        
        logger.info(f"Marked false positive for {stream_name} at {incident_timestamp_us} "
                   f"(confidence: {confidence:.3f})")
    
    def record_data_update_enhanced(self, stream_name: str, event_timestamp_us: int, 
                                  arrival_timestamp_us: Optional[int] = None,
                                  batch_events: Optional[List[Dict[str, Any]]] = None) -> None:
        """Enhanced data update recording with event time support and batch handling."""
        if stream_name not in self.stream_stats:
            logger.warning(f"Recording update for unregistered stream: {stream_name}")
            return
        
        stats = self.stream_stats[stream_name]
        config = self.stream_configs[stream_name]
        now_monotonic_us = monotonic_time_us()
        
        # Handle batch updates - find max event time
        if batch_events:
            max_event_time = max(event.get('timestamp_us', event_timestamp_us) for event in batch_events)
            event_timestamp_us = max_event_time
        
        # Event time sanity check with consistent clock domains
        current_time_us = int(time.time() * 1_000_000)
        if abs(event_timestamp_us - current_time_us) > config.max_event_time_skew_us:
            logger.warning(f"Event time skew detected for {stream_name}: {(event_timestamp_us - current_time_us)/1_000_000:.1f}s")
            # Use wall clock fallback (stay in wall clock domain)
            event_timestamp_us = current_time_us
        
        # Capture previous times BEFORE updating (fix interval calculation)
        prev_event_time = stats.last_event_time_us
        prev_arrival_time = stats.last_arrival_time_us
        prev_update_time = stats.last_update_us
        
        # Update timestamps (ensure clock domain consistency)
        stats.last_event_time_us = event_timestamp_us
        arrival_time_us = arrival_timestamp_us or now_monotonic_us
        stats.last_arrival_time_us = arrival_time_us
        stats.last_batch_max_event_time_us = event_timestamp_us
        
        # Calculate interval for bar estimation (using captured previous times)
        if stats.update_count > 0:
            if config.use_event_time and prev_event_time > 0:
                interval_us = event_timestamp_us - prev_event_time
            elif prev_arrival_time > 0:
                interval_us = arrival_time_us - prev_arrival_time
            else:
                # Fallback to wall time interval
                interval_us = int(time.time() * 1_000_000) - prev_update_time
            
            if interval_us > 0:  # Only record positive intervals
                stats.update_bar_estimation(interval_us)
                # Also update basic interval stats for compatibility
                stats.update_intervals(interval_us)
        
        # Enhanced observation tracking with arming
        stats.observations_count += 1
        stats.update_count += 1  # Direct update instead of calling simple recorder
        
        if (not stats.armed and 
            stats.observations_count >= config.min_observations_for_arming):
            stats.armed = True
            logger.info(f"Armed freshness monitoring for {stream_name}")
        
        # Smart cold-start: arm early if we detect clear regular patterns
        elif (not stats.armed and stats.observations_count >= 3 and 
              len(stats.recent_intervals) >= 2):
            intervals = list(stats.recent_intervals)
            cv = statistics.stdev(intervals) / statistics.mean(intervals) if statistics.mean(intervals) > 0 else float('inf')
            if cv < 0.2:  # Very stable intervals, arm early
                stats.armed = True
                logger.info(f"Early-armed {stream_name} due to stable pattern (CV={cv:.3f})")
        
        # Update last_update_us for backward compatibility (use wall time)
        stats.last_update_us = int(time.time() * 1_000_000)
        
        # Update circuit breaker state if it was open
        if stats.circuit_breaker_state == CircuitBreakerState.OPEN:
            self._try_circuit_breaker_half_open(stream_name)
        elif stats.circuit_breaker_state == CircuitBreakerState.HALF_OPEN:
            close_request = self._close_circuit_breaker(stream_name, "data_received")
            # Actually enqueue the close request
            asyncio.create_task(self._enqueue_circuit_breaker_request(close_request))
        
        # Burst recovery detection: if we get rapid updates after staleness, expedite recovery
        if (stats.incident_confirmed and stats.update_count > 0 and 
            len(stats.recent_intervals) >= 2):
            recent_intervals = list(stats.recent_intervals)[-2:]
            if all(interval < config.expected_interval_us * 0.8 for interval in recent_intervals):
                # Two quick updates in a row - likely recovered, reduce confirmation requirement
                stats.consecutive_fresh_checks = min(stats.consecutive_fresh_checks + 1, 
                                                   config.clear_confirmation_checks - 1)
                logger.debug(f"Burst recovery detected for {stream_name}, expediting clearance")
    
    async def _enqueue_freshness_incident(self, incident: FreshnessIncident) -> None:
        """Enqueue freshness incident to output queue and publish to streaming bus."""
        try:
            # Keep backward compatibility with output queue
            if not self.output_queues['freshness_incidents'].full():
                self.output_queues['freshness_incidents'].put_nowait(incident)
            else:
                logger.warning(f"Freshness incidents queue full, dropping incident for {incident.stream_name}")
                
            # Publish to streaming bus
            await self._publish_freshness_incident(incident)
            
        except Exception as e:
            logger.error(f"Failed to enqueue freshness incident: {e}")
    
    async def _publish_freshness_incident(self, incident: FreshnessIncident) -> None:
        """Publish freshness incident to incidents.Freshness topic."""
        try:
            incident_data = {
                "incident_id": f"freshness_{incident.stream_name}_{incident.timestamp_utc_us}",
                "stream_name": incident.stream_name,
                "level": incident.level.value,
                "staleness_us": incident.staleness_duration_us,
                "staleness_seconds": incident.staleness_duration_us / 1_000_000,
                "threshold_us": incident.threshold_us or 0,
                "confidence": incident.confidence,
                "detected_at": incident.timestamp_utc_us,
                "last_update_us": incident.last_update_us,
                "evidence": {
                    "expected_interval_us": incident.expected_interval_us,
                    "threshold_multiplier": incident.threshold_multiplier,
                    "bar_us": incident.bar_us,
                    "jitter_us": incident.jitter_us,
                    "clock_source": incident.clock_source,
                    "consecutive_misses": incident.consecutive_misses,
                    "occurrence_count": incident.occurrence_count
                },
                "clock_source": incident.clock_source,
                "severity": incident.severity.upper(),
                "description": f"Stream {incident.stream_name} staleness: {incident.staleness_duration_us/1_000_000:.1f}s (threshold: {(incident.threshold_us or 0)/1_000_000:.1f}s)",
                "impacted_streams": [incident.stream_name],  # For circuit breaker integration
                "proposed_action": "CircuitBreak" if incident.level == FreshnessLevel.CRITICAL else "Monitor"
            }
            
            # Use stream name as partition key for locality
            partition_key = f"freshness_{incident.stream_name}"
            
            await self.streaming_bus.publish_with_headers(
                topic="incidents.Freshness",
                partition_key=partition_key,
                payload=incident_data,
                headers={
                    "data_type": "freshness_incident",
                    "stream": incident.stream_name,
                    "level": incident.level.value,
                    "severity": incident.severity.upper()
                },
                dedupe_key=f"freshness_{incident.stream_name}_{incident.level.value}_{incident.timestamp_utc_us}"
            )
            
        except Exception as e:
            logger.error(f"Failed to publish freshness incident to streaming bus: {e}")
    
    async def _enqueue_circuit_breaker_request(self, request: CircuitBreakerRequest) -> None:
        """
        Enhanced circuit breaker request handling with system-wide coordination.
        Integrates with StreamingBus circuit breaker manager for dependency-aware failures.
        """
        try:
            # Map stream names to component IDs for circuit breaker coordination
            component_mapping = {
                "raw_data.exchange_feed": "exchange_connector",
                "raw_data.options_chain": "options_chain_collector",
                "raw_data.onchain": "onchain_collector"
            }
            
            # Extract component from stream name
            component_id = None
            for stream_prefix, comp_id in component_mapping.items():
                if request.stream_name.startswith(stream_prefix):
                    component_id = comp_id
                    break
            
            breaker_intent = None
            action = request.action.lower()
            if component_id:
                metadata = {
                    "stream_name": request.stream_name,
                    "staleness_duration_us": request.staleness_duration_us,
                    "confidence": request.confidence,
                    "raw_metadata": request.metadata
                }
                
                if action == "open":
                    severity = (request.metadata or {}).get("severity", "critical")
                    breaker_intent = BreakerIntent(
                        component_id=component_id,
                        intent="trip",
                        reason=request.reason,
                        severity=severity,
                        requested_by=self.circuit_breaker_id,
                        metadata=metadata
                    )
                elif action == "close":
                    severity = (request.metadata or {}).get("severity", "low")
                    breaker_intent = BreakerIntent(
                        component_id=component_id,
                        intent="recover",
                        reason=request.reason,
                        severity=severity,
                        requested_by=self.circuit_breaker_id,
                        metadata=metadata
                    )
                elif action == "half_open":
                    severity = (request.metadata or {}).get("severity", "medium")
                    breaker_intent = BreakerIntent(
                        component_id=component_id,
                        intent="probe",
                        reason=request.reason,
                        severity=severity,
                        requested_by=self.circuit_breaker_id,
                        metadata=metadata
                    )
            
            if breaker_intent:
                try:
                    await self.streaming_bus.publish_breaker_intent(breaker_intent)
                    logger.debug(f"Published breaker intent {breaker_intent.intent} for {component_id}")
                except Exception as publish_exc:
                    logger.error(f"Failed to publish breaker intent for {component_id}: {publish_exc}")
                    # Fallback to legacy direct mutation for safety
                    await self.streaming_bus.apply_breaker_intent(breaker_intent)
            
            # Keep existing queue-based approach for backwards compatibility
            if not self.output_queues['circuit_breaker_requests'].full():
                self.output_queues['circuit_breaker_requests'].put_nowait(request)
            else:
                logger.warning(f"Circuit breaker queue full, dropping request for {request.stream_name}")
                
        except Exception as e:
            logger.error(f"Failed to enqueue circuit breaker request: {e}")
    
    async def start(self) -> None:
        """Start the freshness monitoring agent with streaming bus consumer."""
        self.running = True
        
        # Register circuit breaker with streaming bus
        await self._register_circuit_breaker()
        
        # Subscribe to raw data topics to monitor freshness
        raw_data_topics = [
            "raw_data.exchange_feed",
            "raw_data.options_chain", 
            "raw_data.onchain_events",
            "raw_data.offchain_events",
            "raw_data.market.trades",
            "raw_data.market.book",
            "raw_data.market.funding",
            "raw_data.market.oi",
            "raw_data.onchain.blocks",
            "raw_data.onchain.mempool"
        ]
        
        # Start consumer to track message timestamps (sync handler for compatibility)
        self._consumer_task = asyncio.create_task(
            self._retry_with_backoff(
                self.streaming_bus.subscribe_with_worker_pool,
                "kafka_consumer_start",
                consumer_group="freshness-monitor",
                topics=raw_data_topics,
                handler=self._handle_data_update_sync,
                pool_size=4  # Lightweight processing
            )
        )
        self._background_tasks.add(self._consumer_task)
        
        # Start health monitoring
        self._health_check_task = asyncio.create_task(self._health_monitor_loop())
        self._background_tasks.add(self._health_check_task)
        
        logger.info(f"Freshness Agent started with circuit breaker: {self.circuit_breaker_id}")
        
        # Start background monitoring task
        monitoring_task = asyncio.create_task(self._monitoring_loop())
        self._background_tasks.add(monitoring_task)
    
    def _handle_data_update_sync(self, topic: str, partition_key: str, payload: Dict[str, Any], headers: Dict[str, str]) -> None:
        """Synchronous handler for incoming data updates to track freshness."""
        try:
            self.metrics['messages_processed'] += 1
            
            # Extract timestamp from payload or use current time
            timestamp_us = payload.get('timestamp_utc_us') or payload.get('timestamp') or int(time.time() * 1_000_000)
            
            # Convert to microseconds if needed
            if isinstance(timestamp_us, str):
                try:
                    timestamp_us = int(timestamp_us)
                except ValueError:
                    timestamp_us = int(time.time() * 1_000_000)
            
            # Handle different timestamp units
            if timestamp_us < 1e12:  # Likely seconds
                timestamp_us = timestamp_us * 1_000_000
            elif timestamp_us < 1e15:  # Likely milliseconds 
                timestamp_us = timestamp_us * 1_000
            
            # Map topic to stream name for freshness tracking
            stream_name = self._map_topic_to_stream(topic, payload, headers)
            if stream_name:
                self.record_data_update(stream_name, timestamp_us)
                
        except Exception as e:
            self.metrics['processing_errors'] += 1
            logger.error(f"Error tracking freshness for {topic}: {e}")
    
    def _map_topic_to_stream(self, topic: str, payload: Dict[str, Any], headers: Dict[str, str]) -> Optional[str]:
        """Map Kafka topic to stream name for freshness tracking."""
        # Base stream name from topic
        if topic == "raw_data.exchange_feed":
            # Use venue from headers for granular tracking
            venue = headers.get("venue") or payload.get("venue", "unknown")
            return f"exchange_feed_{venue}"
        elif topic == "raw_data.options_chain":
            venue = headers.get("venue") or payload.get("venue", "unknown") 
            return f"options_chain_{venue}"
        elif topic == "raw_data.onchain_events":
            chain = headers.get("chain") or payload.get("chain", "unknown")
            return f"onchain_events_{chain}"
        elif topic == "raw_data.market.trades":
            venue = headers.get("venue") or payload.get("venue", "unknown")
            return f"market_trades_{venue}"
        elif topic == "raw_data.market.book":
            venue = headers.get("venue") or payload.get("venue", "unknown")
            return f"market_book_{venue}"
        elif topic == "raw_data.market.funding":
            venue = headers.get("venue") or payload.get("venue", "unknown")
            return f"market_funding_{venue}"
        elif topic == "raw_data.market.oi":
            venue = headers.get("venue") or payload.get("venue", "unknown")
            return f"market_oi_{venue}"
        elif topic == "raw_data.onchain.blocks":
            chain = headers.get("chain") or payload.get("chain", "unknown")
            return f"onchain_blocks_{chain}"
        elif topic == "raw_data.onchain.mempool":
            chain = headers.get("chain") or payload.get("chain", "unknown")
            return f"onchain_mempool_{chain}"
        else:
            # Fallback to topic name
            return topic.replace("raw_data.", "").replace(".", "_")
    
    async def stop(self) -> None:
        """Stop the freshness monitoring agent with graceful task cleanup."""
        self.running = False
        logger.info("Shutting down Freshness Agent...")
        
        # Cancel all background tasks
        for task in self._background_tasks:
            if not task.done():
                task.cancel()
        
        # Wait for tasks to complete with timeout
        if self._background_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._background_tasks, return_exceptions=True),
                    timeout=10.0
                )
                logger.info("All background tasks completed successfully")
            except asyncio.TimeoutError:
                logger.warning("Some background tasks did not complete within timeout")
            except Exception as e:
                logger.error(f"Error during task cleanup: {e}")
        
        # Clear task references
        self._background_tasks.clear()
        self._consumer_task = None
        self._health_check_task = None
        
        # Stop streaming bus
        try:
            await self.streaming_bus.graceful_shutdown()
        except Exception as e:
            logger.error(f"Error stopping streaming bus: {e}")
        
        logger.info(f"Freshness Agent stopped - Final metrics: {self.metrics}")
    
    async def _monitoring_loop(self) -> None:
        """Background monitoring loop with adaptive check intervals and SLO tracking."""
        slo_check_interval = 300  # Check SLO compliance every 5 minutes
        last_slo_check = 0
        
        while self.running:
            try:
                current_time = time.time()
                
                # Regular freshness checking
                incidents = await self.check_freshness()
                if incidents:
                    logger.info(f"Generated {len(incidents)} freshness incidents")
                
                # Periodic SLO compliance reporting
                if current_time - last_slo_check >= slo_check_interval:
                    try:
                        slo_report = self.slo_metrics.get_slo_compliance_report()
                        
                        # Log SLO health
                        if slo_report['overall_health']['slo_compliant']:
                            logger.info("SLO compliance: HEALTHY")
                        else:
                            logger.warning(f"SLO compliance: DEGRADED - "
                                         f"Detection: {slo_report['detection_slo']['compliance_rate']:.1%}, "
                                         f"FP rate: {slo_report['false_positive_slo']['current_weekly']:.1f}")
                        
                        # Check for automatic threshold adjustment
                        now_us = int(time.time() * 1_000_000)
                        if self.slo_metrics.should_adjust_confidence_threshold(now_us):
                            self.slo_metrics.adjust_confidence_threshold(now_us)
                        
                        last_slo_check = current_time
                        
                    except Exception as e:
                        logger.error(f"Error in SLO compliance check: {e}")
                
                # Calculate adaptive check interval for "< 2× bar" detection SLO
                next_check_interval_us = self._calculate_next_check_interval()
                await asyncio.sleep(next_check_interval_us / 1_000_000)  # Convert to seconds
                
            except Exception as e:
                logger.error(f"Error in freshness monitoring loop: {e}")
                await asyncio.sleep(5)  # Brief pause on error
    
    def _calculate_next_check_interval(self) -> int:
        """Calculate optimal next check interval with intelligent adaptation."""
        if not self.stream_stats:
            return self.check_interval_us
        
        min_period_us = self.check_interval_us
        urgency_factor = 1.0
        
        for stream_name, config in self.stream_configs.items():
            if stream_name in self.stream_stats:
                stats = self.stream_stats[stream_name]
                stream_period = stats.get_check_period_us(config)
                min_period_us = min(min_period_us, stream_period)
                
                # Increase urgency if stream is close to stale
                if stats.armed:
                    # Use event-time lag when available, otherwise arrival lag
                    if config.use_event_time and stats.last_event_time_us > 0:
                        now_wall_us = int(time.time() * 1_000_000)
                        current_staleness = now_wall_us - stats.last_event_time_us
                    elif stats.last_arrival_time_us > 0:
                        now_monotonic_us = monotonic_time_us()
                        current_staleness = now_monotonic_us - stats.last_arrival_time_us
                    else:
                        continue  # Skip if no timing data
                    
                    threshold = self._calculate_staleness_threshold(stream_name, config, stats)
                    staleness_ratio = current_staleness / threshold if threshold > 0 else 0
                    if staleness_ratio > 0.9:  # Very close - most severe case first
                        urgency_factor = min(urgency_factor, 0.25)  # Check very frequently
                    elif staleness_ratio > 0.7:  # Getting close to stale
                        urgency_factor = min(urgency_factor, 0.5)  # Check more frequently
        
        # Apply urgency factor but respect minimum bounds
        adaptive_interval = int(min_period_us * urgency_factor)
        return max(adaptive_interval, 1_000_000)  # Never less than 1 second
    
    def _prune_dedupe_cache(self, now_us: int) -> None:
        """Prune stale entries from dedupe cache to prevent memory growth."""
        if not self.last_incident_by_key:
            return
        
        # Find maximum dedupe window across all streams
        max_dedupe_window_us = 0
        for config in self.stream_configs.values():
            max_dedupe_window_us = max(max_dedupe_window_us, config.incident_dedupe_window_us)
        
        if max_dedupe_window_us == 0:
            return  # No dedupe windows configured
        
        # Remove entries older than the longest dedupe window
        cutoff_time_us = now_us - max_dedupe_window_us
        stale_keys = []
        
        for dedupe_key, incident in self.last_incident_by_key.items():
            if incident.timestamp_utc_us < cutoff_time_us:
                stale_keys.append(dedupe_key)
        
        for key in stale_keys:
            del self.last_incident_by_key[key]
        
        if stale_keys:
            logger.debug(f"Pruned {len(stale_keys)} stale dedupe cache entries")
    
    def _calculate_stream_health_score(self, stream_name: str, stats: StreamStats, config: StreamConfig) -> float:
        """Calculate simple health score [0,1] based on freshness and stability."""
        if not stats.armed:
            return 0.5  # Neutral for unarmed streams
        
        score = 1.0
        
        # Factor 1: Current freshness (primary factor)
        # Mirror detection's clock choice for perfect alignment
        if config.use_event_time and stats.last_event_time_us > 0:
            # Use event-time clock when configured (wall clock)
            now_wall_us = int(time.time() * 1_000_000)
            staleness_us = now_wall_us - stats.last_event_time_us
        elif stats.last_arrival_time_us > 0:
            # Use arrival-time clock (monotonic)
            now_monotonic_us = monotonic_time_us()
            staleness_us = now_monotonic_us - stats.last_arrival_time_us
        else:
            # Fallback to wall clock if no timing data
            now_wall_us = int(time.time() * 1_000_000)
            staleness_us = now_wall_us - stats.last_update_us
        
        threshold_us = self._calculate_staleness_threshold(stream_name, config, stats)
        staleness_ratio = staleness_us / threshold_us if threshold_us > 0 else 0
        
        if staleness_ratio < 0.5:
            score *= 1.0  # Excellent
        elif staleness_ratio < 1.0:
            score *= 0.8  # Good
        elif staleness_ratio < 2.0:
            score *= 0.4  # Poor
        else:
            score *= 0.1  # Critical
        
        # Factor 2: Pattern stability
        if len(stats.recent_intervals) >= 3:
            intervals = list(stats.recent_intervals)
            cv = statistics.stdev(intervals) / statistics.mean(intervals) if statistics.mean(intervals) > 0 else 1.0
            if cv < 0.1:
                score *= 1.05  # Bonus for stability
            elif cv > 0.5:
                score *= 0.9   # Penalty for instability
        
        # Factor 3: False positive history
        if stats.false_positive_rate > 0.2:
            score *= 0.85  # Penalty for unreliable detection
        
        return max(0.0, min(1.0, score))
    
    def get_stream_status(self, stream_name: str) -> Optional[Dict[str, Any]]:
        """Get current status for a stream, honoring event-time mode."""
        if stream_name not in self.stream_stats:
            return None
        
        stats = self.stream_stats[stream_name]
        config = self.stream_configs.get(stream_name)
        now_us = int(time.time() * 1_000_000)
        now_monotonic_us = monotonic_time_us()
        
        # Honor event-time mode for lag calculation (same logic as check_freshness)
        if config and config.use_event_time and stats.last_event_time_us > 0:
            staleness_us = now_us - stats.last_event_time_us
            last_timestamp_us = stats.last_event_time_us
            clock_source = "event_time"
        else:
            # Use monotonic arrival time when available, fallback to wall time
            if stats.last_arrival_time_us > 0:
                staleness_us = now_monotonic_us - stats.last_arrival_time_us
                last_timestamp_us = stats.last_arrival_time_us
            else:
                staleness_us = now_us - stats.last_update_us
                last_timestamp_us = stats.last_update_us
            clock_source = "arrival_time"
        
        if config:
            threshold_us = self._calculate_staleness_threshold(stream_name, config, stats)
            # Derive critical threshold from detection threshold for consistency
            critical_us = threshold_us * config.escalation_threshold_multiplier
            level = self._determine_freshness_level(staleness_us, threshold_us, critical_us)
        else:
            threshold_us = None
            level = FreshnessLevel.FRESH
        
        return {
            'stream_name': stream_name,
            'last_update_us': last_timestamp_us,
            'staleness_us': staleness_us,
            'staleness_seconds': staleness_us / 1_000_000,
            'level': level.value,
            'clock_source': clock_source,
            'circuit_breaker_state': stats.circuit_breaker_state.value,
            'update_count': stats.update_count,
            'false_positive_rate': stats.false_positive_rate,
            'avg_interval_us': stats.avg_interval_us,
            'threshold_us': threshold_us,
            'health_score': self._calculate_stream_health_score(stream_name, stats, config) if config else None
        }
    
    def get_weekly_summary(self) -> Dict[str, Any]:
        """Get comprehensive SLO compliance summary."""
        slo_report = self.slo_metrics.get_slo_compliance_report()
        
        return {
            'week_start_us': self.slo_metrics.week_start_us,
            'false_positive_count': self.slo_metrics.false_positive_count_weekly,
            'false_positive_budget': self.slo_metrics.false_positive_target_weekly,
            'budget_remaining': max(0, self.slo_metrics.false_positive_target_weekly - self.slo_metrics.false_positive_count_weekly),
            'total_streams': len(self.stream_configs),
            'circuit_breakers_open': sum(1 for s in self.stream_stats.values() if s.circuit_breaker_state == CircuitBreakerState.OPEN),
            'slo_compliance': slo_report,
            'confidence_threshold': self.min_confidence_threshold
        }
    
    def get_slo_compliance_report(self) -> Dict[str, Any]:
        """Get detailed SLO compliance report."""
        return self.slo_metrics.get_slo_compliance_report()
    
    def export_slo_metrics(self) -> Dict[str, Any]:
        """Export SLO metrics for persistence or external monitoring."""
        return self.slo_metrics.export_metrics()
    
    def import_slo_metrics(self, metrics_data: Dict[str, Any]) -> bool:
        """Import SLO metrics from persistence with validation."""
        try:
            if 'slo_metrics' not in metrics_data:
                logger.error("Invalid SLO metrics data: missing slo_metrics key")
                return False
            
            slo_data = metrics_data['slo_metrics']
            
            # Validate required fields
            required_fields = ['detection_violations', 'detection_total_checks', 'false_positive_count_weekly']
            for field in required_fields:
                if field not in slo_data:
                    logger.error(f"Invalid SLO metrics data: missing {field}")
                    return False
            
            # Import with safety checks
            with self.slo_metrics._lock:
                self.slo_metrics.detection_violations = max(0, slo_data.get('detection_violations', 0))
                self.slo_metrics.detection_total_checks = max(0, slo_data.get('detection_total_checks', 0))
                self.slo_metrics.false_positive_count_weekly = max(0, slo_data.get('false_positive_count_weekly', 0))
                
                if 'detection_p95_delay_us' in slo_data and slo_data['detection_p95_delay_us'] is not None:
                    self.slo_metrics.detection_p95_delay_us = float(slo_data['detection_p95_delay_us'])
                
                self.slo_metrics.detection_max_delay_us = max(0, slo_data.get('detection_max_delay_us', 0))
                self.slo_metrics.min_confidence_threshold = max(0.1, min(0.95, slo_data.get('min_confidence_threshold', 0.7)))
                
                # Import false positive history with validation
                if 'false_positive_history' in metrics_data:
                    self.slo_metrics.false_positive_history.clear()
                    for fp_record in metrics_data['false_positive_history']:
                        if all(key in fp_record for key in ['timestamp_us', 'stream_name', 'confidence']):
                            self.slo_metrics.false_positive_history.append(fp_record)
            
            # Update agent's confidence threshold
            self.min_confidence_threshold = self.slo_metrics.min_confidence_threshold
            
            logger.info(f"Successfully imported SLO metrics: "
                       f"detection_checks={self.slo_metrics.detection_total_checks}, "
                       f"fp_count={self.slo_metrics.false_positive_count_weekly}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to import SLO metrics: {e}")
            return False


async def main():
    """Example usage of the Freshness Agent."""
    # Configure the agent
    config = {
        'check_interval_us': 30_000_000,  # Check every 30 seconds
        'min_confidence_threshold': 0.7
    }
    
    agent = FreshnessAgent(config)
    
    # Register some streams
    streams = [
        StreamConfig(
            stream_name="onchain_flows",
            expected_interval_us=60_000_000,  # 1 minute
            staleness_threshold_multiplier=2.0,
            critical_threshold_multiplier=5.0,
            circuit_breaker_enabled=True
        ),
        StreamConfig(
            stream_name="options_surface",
            expected_interval_us=15_000_000,  # 15 seconds
            staleness_threshold_multiplier=3.0,
            critical_threshold_multiplier=10.0,
            circuit_breaker_enabled=True
        ),
        StreamConfig(
            stream_name="events_calendar",
            expected_interval_us=300_000_000,  # 5 minutes
            staleness_threshold_multiplier=2.0,
            critical_threshold_multiplier=4.0,
            circuit_breaker_enabled=False  # Events are less time-critical
        )
    ]
    
    for stream_config in streams:
        agent.register_stream(stream_config)
    
    # Start the agent
    await agent.start()
    
    # Simulate some data updates
    import random
    now_us = int(time.time() * 1_000_000)
    
    print("🔍 FRESHNESS AGENT DEMO")
    print("=" * 50)
    
    # Record some updates
    for i in range(5):
        for stream in streams:
            # Simulate some jitter in update timing
            jitter = random.randint(-10_000_000, 10_000_000)  # ±10 seconds
            update_time = now_us + (i * stream.expected_interval_us) + jitter
            agent.record_data_update(stream.stream_name, update_time)
            print(f"📊 Recorded update for {stream.stream_name} at {update_time}")
    
    # Check freshness
    incidents = await agent.check_freshness()
    print(f"\n🚨 Generated {len(incidents)} incidents")
    
    for incident in incidents:
        print(f"   - {incident.stream_name}: {incident.level.value} (confidence: {incident.confidence:.2f})")
    
    # Show stream statuses
    print(f"\n📈 STREAM STATUS")
    print("-" * 30)
    for stream_name in agent.stream_configs.keys():
        status = agent.get_stream_status(stream_name)
        if status:
            print(f"{stream_name}:")
            print(f"   Staleness: {status['staleness_seconds']:.1f}s")
            print(f"   Level: {status['level']}")
            print(f"   Circuit Breaker: {status['circuit_breaker_state']}")
    
    # Show weekly summary
    summary = agent.get_weekly_summary()
    print(f"\n📅 WEEKLY SUMMARY")
    print("-" * 20)
    print(f"False Positives: {summary['false_positive_count']}/{summary['false_positive_budget']}")
    print(f"Budget Remaining: {summary['budget_remaining']}")
    print(f"Open Circuit Breakers: {summary['circuit_breakers_open']}")
    
    await agent.stop()


if __name__ == "__main__":
    asyncio.run(main())
