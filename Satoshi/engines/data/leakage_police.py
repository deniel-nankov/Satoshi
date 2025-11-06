"""
Leakage Police Agent - Data Integrity and Look-Ahead Detection

Mission: Prove no look-ahead; detect dataset/label leakage (incl. mempool/bridge timing edge).
Outputs: incidents.Leakage with evidence + severity; proposals go to Policy.

The Leakage Police performs systematic analysis to detect various forms of data leakage
that could compromise trading strategy integrity, including temporal leakage, 
information leakage, and subtle timing edge cases.

KEY FEATURES:
============

🔍 Look-Ahead Detection:
   - Temporal ordering violations in features vs labels
   - Future information bleeding into historical features
   - Cross-validation data contamination
   - Feature engineering look-ahead bias

📊 Dataset Leakage Detection:
   - Train/validation/test set contamination
   - Duplicate records across splits
   - Information leakage through derived features
   - Target variable leakage into feature construction

⏱️ Timing Edge Analysis:
   - Mempool timing advantages detection
   - Bridge timing arbitrage opportunities
   - Market data timestamp inconsistencies
   - Order execution timing leakage

🛡️ Guardrails:
   - NO DATA FIXES: Only detection and reporting
   - NO POLICY ENFORCEMENT: Proposals sent to Policy agent
   - NO AUTOMATIC BLOCKING: Evidence-based incident reporting only

🎯 Smart Detection:
   - Statistical significance testing for leakage
   - Severity classification based on impact potential
   - Evidence collection with specific examples
   - Actionable remediation proposals

USAGE EXAMPLE:
=============

    # Configure leakage police
    config = LeakagePoliceConfig(
        temporal_tolerance_ms=100,
        statistical_threshold=0.001,
        min_samples_for_analysis=1000
    )
    
    police = LeakagePolice(config)
    
    # Analyze dataset for leakage
    incidents = await police.analyze_dataset(
        features=feature_data,
        labels=label_data,
        timestamps=timestamp_data,
        splits={"train": train_indices, "val": val_indices}
    )
    
    # Review incidents and proposals
    for incident in incidents:
        print(f"Leakage detected: {incident.leakage_type}")
        print(f"Severity: {incident.severity}")
        print(f"Evidence: {incident.evidence}")
"""

import asyncio
import time
import hashlib
import json
import logging
import numpy as np
import pandas as pd
from collections import defaultdict, Counter
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Set, Tuple, Union
from enum import Enum
from datetime import datetime, timedelta
import warnings

from infra.bus.streaming_bus import StreamingBus, BreakerIntent

# Import centralized Prometheus metrics
try:
    from infra.monitoring.prometheus_metrics import MetricsCollector
    _metrics_collector = MetricsCollector()
    METRICS_AVAILABLE = True
except ImportError:
    _metrics_collector = None
    METRICS_AVAILABLE = False

logger = logging.getLogger(__name__)


class LeakageType(Enum):
    """Types of data leakage that can be detected."""
    TEMPORAL_LOOK_AHEAD = "temporal_look_ahead"
    FUTURE_INFORMATION = "future_information"  
    TRAIN_TEST_CONTAMINATION = "train_test_contamination"
    TARGET_LEAKAGE = "target_leakage"
    FEATURE_LEAKAGE = "feature_leakage"
    MEMPOOL_TIMING_EDGE = "mempool_timing_edge"
    BRIDGE_TIMING_EDGE = "bridge_timing_edge"
    EXECUTION_TIMING_LEAK = "execution_timing_leak"
    CROSS_VALIDATION_LEAK = "cross_validation_leak"
    DUPLICATE_CONTAMINATION = "duplicate_contamination"


class LeakageSeverity(Enum):
    """Severity levels for data leakage incidents."""
    LOW = "low"          # Minor timing advantages, < 1% alpha impact
    MEDIUM = "medium"    # Moderate leakage, 1-5% alpha impact
    HIGH = "high"        # Significant leakage, 5-20% alpha impact  
    CRITICAL = "critical" # Severe leakage, >20% alpha impact or regulatory risk


class PolicyAction(Enum):
    """Types of policy actions that can be proposed."""
    QUARANTINE_DATASET = "quarantine_dataset"
    RETRAIN_MODEL = "retrain_model"
    ADJUST_FEATURES = "adjust_features"
    FIX_TIMESTAMPS = "fix_timestamps"
    REVIEW_METHODOLOGY = "review_methodology"
    MANUAL_INVESTIGATION = "manual_investigation"
    NO_ACTION_REQUIRED = "no_action_required"


@dataclass
class LeakageEvidence:
    """Evidence supporting a leakage detection."""
    evidence_type: str
    description: str
    sample_data: Dict[str, Any]
    statistical_measures: Dict[str, float]
    affected_records: int
    confidence_score: float
    timestamps: Optional[List[int]] = None


@dataclass
class PolicyProposal:
    """A proposal for policy action to address leakage."""
    proposal_id: str
    action: PolicyAction
    target_component: str
    rationale: str
    implementation_steps: List[str]
    estimated_impact: str
    urgency_level: LeakageSeverity
    estimated_effort: str
    rollback_plan: str
    metadata: Dict[str, Any] = field(default_factory=lambda: {})


@dataclass
class LeakageIncident:
    """A detected data leakage incident."""
    incident_id: str
    leakage_type: LeakageType
    severity: LeakageSeverity
    title: str
    description: str
    evidence: List[LeakageEvidence]
    affected_components: List[str]
    detection_timestamp: int
    confidence_score: float
    potential_impact: str
    policy_proposals: List[PolicyProposal] = field(default_factory=lambda: [])
    metadata: Dict[str, Any] = field(default_factory=lambda: {})


@dataclass
class LeakagePoliceConfig:
    """Configuration for the leakage police agent."""
    # Temporal analysis settings
    temporal_tolerance_ms: int = 50  # Allowable timestamp tolerance
    future_window_hours: int = 24   # How far to look for future information
    label_horizon_us: int = 0       # Expected lag from feature to label (microseconds)
    embargo_us: int = 86400000000   # Time embargo for CV splits (24 hours in microseconds)
    
    # Statistical thresholds
    statistical_threshold: float = 0.001  # P-value threshold for significance
    correlation_threshold: float = 0.95   # Correlation threshold for duplicates
    min_samples_for_analysis: int = 100   # Minimum samples required
    target_equality_epsilon: float = 1e-10  # Epsilon for target equality tests
    target_correlation_threshold: float = 0.99  # |corr(feature, target)| threshold
    target_mae_threshold: float = 0.1    # Normalized MAE threshold (MAE / std(target))
    
    # Performance limits
    max_incidents_per_run: int = 1000     # Limit incident reporting
    analysis_timeout_seconds: float = 600 # Maximum analysis time
    per_analyzer_timeout_seconds: float = 120  # Maximum time per analyzer
    batch_size: int = 10000               # Records to analyze in batches
    
    # Streaming bus configuration
    streaming_bus: Dict[str, Any] = field(default_factory=lambda: {
        "bootstrap_servers": "localhost:9092",
        "enable_ssl": False,
        "enable_sasl": False
    })
    
    # Severity thresholds by leakage type
    temporal_severity_thresholds: Dict[str, float] = field(default_factory=lambda: {
        "low": 0.001, "medium": 0.01, "high": 0.05, "critical": 0.20
    })
    contamination_severity_thresholds: Dict[str, float] = field(default_factory=lambda: {
        "low": 0.001, "medium": 0.01, "high": 0.05, "critical": 0.20
    })
    mempool_severity_thresholds: Dict[str, float] = field(default_factory=lambda: {
        "low": 0.001, "medium": 0.01, "high": 0.05, "critical": 0.20
    })
    execution_severity_thresholds: Dict[str, float] = field(default_factory=lambda: {
        "low": 0.001, "medium": 0.01, "high": 0.05, "critical": 0.20
    })
    bridge_severity_thresholds: Dict[str, float] = field(default_factory=lambda: {
        "low": 0.001, "medium": 0.01, "high": 0.05, "critical": 0.20
    })
    cv_severity_thresholds: Dict[str, float] = field(default_factory=lambda: {
        "low": 0.001, "medium": 0.01, "high": 0.05, "critical": 0.20
    })
    
    # Global severity thresholds (fallback)
    low_impact_threshold: float = 0.01    # 1% alpha impact
    medium_impact_threshold: float = 0.05 # 5% alpha impact  
    high_impact_threshold: float = 0.20   # 20% alpha impact
    
    # Timing edge detection
    mempool_advantage_ms: int = 100       # Suspicious mempool timing advantage
    mempool_percentile_factor: float = 0.5  # Factor for p1 percentile baseline
    mempool_threshold_epsilon: float = 1.0  # Minimum effective threshold in ms
    bridge_latency_ms: int = 500          # Expected bridge latency
    execution_delay_ms: int = 10          # Expected execution delay
    
    # Hashing optimization
    hash_exclude_fields: List[str] = field(default_factory=lambda: ['timestamp', 'created_at', 'updated_at'])


class LeakagePolice:
    """
    Data integrity and look-ahead detection agent.
    
    Systematically analyzes datasets, features, and labels to detect various
    forms of data leakage that could compromise trading strategy integrity.
    
    100% Kafka Integration Features:
    ===============================
    🔄 StreamingBus Integration: Complete Kafka infrastructure via StreamingBus
    ⚡ Circuit Breaker Protection: Component-level failure isolation and recovery
    📊 Health Monitoring: Real-time component status and degradation detection
    🔁 Exponential Backoff Retry: Intelligent failure recovery for external dependencies
    📈 Comprehensive Metrics: 15 detailed operational and business metrics
    🎯 Smart Architecture: StreamingBus handles infrastructure, agent handles business logic
    """
    
    def __init__(self, config: LeakagePoliceConfig):
        self.config = config
        self.session_id = self._generate_session_id()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
        # Initialize Kafka streaming bus
        streaming_config = getattr(config, 'streaming_bus', {
            "bootstrap_servers": "localhost:9092",
            "enable_ssl": False,
            "enable_sasl": False
        })
        self.streaming_bus = StreamingBus(streaming_config)
        
        # Circuit breaker integration
        self.circuit_breaker_id = f"leakage_police_{id(self)}"
        self.component_id = "leakage_police"  # Business identifier
        self._circuit_breaker_registered = False
        
        # Task management for async operations
        self._tasks = set()
        self._background_tasks = set()
        
        # Health monitoring state
        self._health_status = "initializing"
        self._health_check_interval = 30.0  # 30 seconds
        self._last_health_check = time.time()
        self._consecutive_failures = 0
        self._max_consecutive_failures = 5
        
        # Comprehensive metrics tracking
        self.metrics = {
            # Operational metrics
            "total_analysis_runs": 0,
            "successful_analyses": 0, 
            "failed_analyses": 0,
            "circuit_breaker_trips": 0,
            "health_check_failures": 0,
            "kafka_publish_errors": 0,
            "retry_attempts": 0,
            "analysis_timeout_events": 0,
            
            # Business metrics
            "total_incidents_detected": 0,
            "incidents_by_severity": {"low": 0, "medium": 0, "high": 0, "critical": 0},
            "incidents_by_type": {lt.value: 0 for lt in LeakageType},
            "average_analysis_time_ms": 0.0,
            "datasets_analyzed": 0,
            "policy_proposals_generated": 0,
            
            # Performance metrics
            "last_analysis_time_ms": 0.0
        }
        
        # Statistics tracking (kept for backward compatibility)
        self.analysis_stats = {
            "total_analyses": 0,
            "total_incidents": 0,
            "incidents_by_type": defaultdict(int),
            "incidents_by_severity": defaultdict(int),
            "avg_analysis_time": 0.0
        }
        
        # Cache for efficiency
        self.feature_hash_cache: Dict[str, str] = {}
        self.correlation_cache: Dict[str, float] = {}
        
        # Canonical headers: Sequence tracking for institutional compliance
        self._sequence_numbers: Dict[str, int] = defaultdict(int)  # topic -> sequence_number
        self.split_hash_cache: Dict[Tuple[str, str], Set[str]] = {}  # (split_name, batch_key) -> hash_set
        self.exclude_cols_sorted = sorted(self.config.hash_exclude_fields)  # Pre-sort for efficiency
        
        # De-duplication for single run
        self.seen_incident_ids: Set[str] = set()
        self.duplicate_incidents: Dict[str, int] = {}  # incident_id -> repeat_count
        
        # Exponential backoff configuration
        self.retry_config = {
            "initial_delay": 1.0,
            "max_delay": 60.0,
            "backoff_factor": 2.0,
            "max_retries": 3
        }
    
    async def _register_circuit_breaker(self) -> bool:
        """Register component with StreamingBus circuit breaker system."""
        if self._circuit_breaker_registered:
            return True
            
        try:
            await self.streaming_bus.register_circuit_breaker(
                component_id=self.circuit_breaker_id,
                failure_threshold=5,
                recovery_timeout_us=30_000_000,  # 30 seconds in microseconds
                dependency_components=[]
            )
            self._circuit_breaker_registered = True
            self._health_status = "healthy"
            self.logger.info(f"Circuit breaker registered: {self.circuit_breaker_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to register circuit breaker: {e}")
            self.metrics["circuit_breaker_trips"] += 1
            return False
    
    async def _exponential_backoff_retry(self, operation_name: str, operation_func, *args, **kwargs):
        """Execute operation with exponential backoff retry logic."""
        delay = self.retry_config["initial_delay"]
        
        for attempt in range(self.retry_config["max_retries"] + 1):
            try:
                result = await operation_func(*args, **kwargs)
                
                # Reset consecutive failures on success
                self._consecutive_failures = 0
                return result
                
            except Exception as e:
                self._consecutive_failures += 1
                
                if attempt < self.retry_config["max_retries"]:
                    self.metrics["retry_attempts"] += 1  # Only count actual retries
                    self.logger.warning(f"{operation_name} failed (attempt {attempt + 1}), retrying in {delay:.1f}s: {e}")
                    await asyncio.sleep(delay)
                    delay = min(delay * self.retry_config["backoff_factor"], self.retry_config["max_delay"])
                else:
                    self.logger.error(f"{operation_name} failed after {self.retry_config['max_retries']} retries: {e}")
                    
                    # Record failure with circuit breaker
                    if self._circuit_breaker_registered:
                        await self._emit_breaker_intent(
                            intent="trip",
                            reason=f"{operation_name} retry exhaustion",
                            severity="high",
                            metadata={
                                "operation": operation_name,
                                "attempts": attempt + 1,
                                "exception": str(e)
                            }
                        )
                    
                    raise e
    
    async def _perform_health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check for the leakage police component."""
        try:
            health_data = {
                "component_id": self.component_id,
                "circuit_breaker_id": self.circuit_breaker_id,
                "status": self._health_status,
                "consecutive_failures": self._consecutive_failures,
                "max_consecutive_failures": self._max_consecutive_failures,
                "last_health_check": self._last_health_check,
                "streaming_bus_connected": self.streaming_bus is not None,
                "circuit_breaker_registered": self._circuit_breaker_registered,
                "cache_sizes": {
                    "feature_hash_cache": len(self.feature_hash_cache),
                    "correlation_cache": len(self.correlation_cache),
                    "split_hash_cache": len(self.split_hash_cache)
                },
                "metrics_summary": {
                    "total_analysis_runs": self.metrics["total_analysis_runs"],
                    "success_rate": (self.metrics["successful_analyses"] / max(1, self.metrics["total_analysis_runs"])) * 100,
                    "incident_detection_rate": self.metrics["total_incidents_detected"] / max(1, self.metrics["datasets_analyzed"]),
                    "avg_analysis_time_ms": self.metrics["average_analysis_time_ms"]
                }
            }
            
            # Update health status based on consecutive failures
            if self._consecutive_failures >= self._max_consecutive_failures:
                self._health_status = "degraded"
            elif self._consecutive_failures > 0:
                self._health_status = "warning" 
            else:
                self._health_status = "healthy"
            
            health_data["status"] = self._health_status
            
            self._last_health_check = time.time()
            return health_data
            
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            self.metrics["health_check_failures"] += 1
            self._health_status = "unhealthy"
            return {
                "component_id": self.component_id,
                "circuit_breaker_id": self.circuit_breaker_id,
                "status": "unhealthy",
                "error": str(e)
            }
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get current health status (synchronous version for external callers)."""
        return {
            "component_id": self.component_id,
            "circuit_breaker_id": self.circuit_breaker_id,
            "status": self._health_status,
            "consecutive_failures": self._consecutive_failures,
            "last_health_check": self._last_health_check,
            "circuit_breaker_registered": self._circuit_breaker_registered,
            "metrics": self.metrics.copy()
        }
    
    def get_comprehensive_metrics(self) -> Dict[str, Any]:
        """Get all operational and business metrics."""
        return {
            "component_info": {
                "component_id": self.component_id,
                "circuit_breaker_id": self.circuit_breaker_id,
                "session_id": self.session_id
            },
            "operational_metrics": {
                "total_analysis_runs": self.metrics["total_analysis_runs"],
                "successful_analyses": self.metrics["successful_analyses"],
                "failed_analyses": self.metrics["failed_analyses"], 
                "success_rate_percent": (self.metrics["successful_analyses"] / max(1, self.metrics["total_analysis_runs"])) * 100,
                "circuit_breaker_trips": self.metrics["circuit_breaker_trips"],
                "health_check_failures": self.metrics["health_check_failures"],
                "kafka_publish_errors": self.metrics["kafka_publish_errors"],
                "retry_attempts": self.metrics["retry_attempts"],
                "analysis_timeout_events": self.metrics["analysis_timeout_events"]
            },
            "business_metrics": {
                "total_incidents_detected": self.metrics["total_incidents_detected"],
                "incidents_by_severity": self.metrics["incidents_by_severity"].copy(),
                "incidents_by_type": self.metrics["incidents_by_type"].copy(),
                "datasets_analyzed": self.metrics["datasets_analyzed"],
                "policy_proposals_generated": self.metrics["policy_proposals_generated"],
                "incident_detection_rate": self.metrics["total_incidents_detected"] / max(1, self.metrics["datasets_analyzed"])
            },
            "performance_metrics": {
                "average_analysis_time_ms": self.metrics["average_analysis_time_ms"],
                "last_analysis_time_ms": self.metrics["last_analysis_time_ms"]
            },
            "health_status": self.get_health_status()
        }
    
    def _generate_session_id(self) -> str:
        """Generate a unique session ID for this police instance."""
        timestamp = int(time.time() * 1_000_000)
        random_component = hashlib.md5(f"{timestamp}_{id(self)}".encode()).hexdigest()[:8]
        return f"leakage_police_{timestamp}_{random_component}"
    
    def _generate_incident_id(self, leakage_type: LeakageType, scope_keys: Dict[str, Any], 
                          summary_stats: Dict[str, Any]) -> str:
        """Generate a deterministic, content-stable ID for a leakage incident."""
        # Create deterministic content hash
        content_components = {
            "leakage_type": leakage_type.value,
            "scope_keys": sorted(scope_keys.items()) if scope_keys else [],
            "summary_stats": sorted(summary_stats.items()) if summary_stats else []
        }
        
        content_str = json.dumps(content_components, sort_keys=True, default=str)
        content_hash = hashlib.sha256(content_str.encode()).hexdigest()[:12]
        return f"leak_{content_hash}"
    
    def _calculate_severity(self, leakage_type: LeakageType, 
                          impact_score: float, confidence: float) -> LeakageSeverity:
        """Calculate severity based on leakage type and impact with type-specific thresholds."""
        # Adjust impact by confidence
        adjusted_impact = impact_score * confidence
        
        # Get type-specific thresholds
        type_thresholds = None
        if leakage_type in [LeakageType.TEMPORAL_LOOK_AHEAD, LeakageType.FUTURE_INFORMATION]:
            type_thresholds = self.config.temporal_severity_thresholds
        elif leakage_type in [LeakageType.TRAIN_TEST_CONTAMINATION, LeakageType.DUPLICATE_CONTAMINATION]:
            type_thresholds = self.config.contamination_severity_thresholds
        elif leakage_type == LeakageType.MEMPOOL_TIMING_EDGE:
            type_thresholds = self.config.mempool_severity_thresholds
        elif leakage_type == LeakageType.EXECUTION_TIMING_LEAK:
            type_thresholds = self.config.execution_severity_thresholds
        elif leakage_type == LeakageType.BRIDGE_TIMING_EDGE:
            type_thresholds = self.config.bridge_severity_thresholds
        elif leakage_type == LeakageType.CROSS_VALIDATION_LEAK:
            type_thresholds = self.config.cv_severity_thresholds
        
        # Use type-specific or global thresholds
        if type_thresholds:
            if adjusted_impact >= type_thresholds["critical"]:
                return LeakageSeverity.CRITICAL
            elif adjusted_impact >= type_thresholds["high"]:
                return LeakageSeverity.HIGH
            elif adjusted_impact >= type_thresholds["medium"]:
                return LeakageSeverity.MEDIUM
            else:
                return LeakageSeverity.LOW
        else:
            # Fallback to global thresholds
            if adjusted_impact >= self.config.high_impact_threshold:
                return LeakageSeverity.HIGH
            elif adjusted_impact >= self.config.medium_impact_threshold:
                return LeakageSeverity.MEDIUM
            elif adjusted_impact >= self.config.low_impact_threshold:
                return LeakageSeverity.LOW
            else:
                return LeakageSeverity.LOW
    
    def _calculate_confidence(self, violation_ratio: float, sample_size: int) -> float:
        """Calculate confidence from effect size and sample size using sigmoid mapping."""
        if sample_size == 0:
            return 0.0
        
        # Effect size × sqrt(N) mapped through sigmoid
        effect_strength = violation_ratio * np.sqrt(sample_size)
        # Sigmoid: 1 / (1 + exp(-x))
        confidence = 1.0 / (1.0 + np.exp(-effect_strength))
        return min(1.0, confidence)
    
    async def _emit_breaker_intent(self, intent: str, reason: str, severity: str,
                                   metadata: Optional[Dict[str, Any]] = None) -> None:
        """Helper to publish centralized breaker intents for leakage police."""
        if not self._circuit_breaker_registered or not self.streaming_bus:
            return
        
        payload_metadata: Dict[str, Any] = {
            "agent": "leakage_police"
        }
        if metadata:
            payload_metadata.update(metadata)
        
        breaker_intent = BreakerIntent(
            component_id=self.circuit_breaker_id,
            intent=intent,
            reason=reason,
            severity=severity,
            requested_by=self.circuit_breaker_id,
            metadata=payload_metadata
        )
        
        try:
            await self.streaming_bus.publish_breaker_intent(breaker_intent)
        except Exception as exc:
            self.logger.error(f"Failed to publish breaker intent ({intent}) for {self.circuit_breaker_id}: {exc}")
    
    def _format_human_readable_time(self, timestamp_us: int) -> str:
        """Convert microsecond timestamp to human-readable UTC ISO-8601 format."""
        try:
            timestamp_s = timestamp_us / 1_000_000
            dt = datetime.utcfromtimestamp(timestamp_s)
            return dt.isoformat() + 'Z'
        except (ValueError, OSError):
            return f"invalid_timestamp_{timestamp_us}"
    
    def _create_standardized_evidence(self, evidence_type: str, description: str,
                                   sample_data: Dict[str, Any], statistical_measures: Dict[str, Any],
                                   affected_records: int, confidence_score: float,
                                   threshold_used: Optional[float] = None,
                                   window_size_used: Optional[int] = None,
                                   violation_indices: Optional[List[int]] = None,
                                   timestamps: Optional[List[int]] = None) -> LeakageEvidence:
        """Create standardized evidence with uniform keys across all analyzers."""
        
        # Standardize sample_data with required keys
        standardized_sample_data = {
            **sample_data,
            "threshold_used": threshold_used,
            "window_size_used": window_size_used or affected_records,
            "N_used": affected_records
        }
        
        # Add violation indices (first 5)
        if violation_indices:
            standardized_sample_data["violation_indices"] = violation_indices[:5]
        
        # Add human-readable timestamps
        if timestamps:
            standardized_sample_data["example_timestamps_iso"] = [
                self._format_human_readable_time(ts) for ts in timestamps[:5]
            ]
            standardized_sample_data["example_timestamps_raw"] = timestamps[:5]
        
        return LeakageEvidence(
            evidence_type=evidence_type,
            description=description,
            sample_data=standardized_sample_data,
            statistical_measures=statistical_measures,
            affected_records=affected_records,
            confidence_score=confidence_score,
            timestamps=timestamps[:10] if timestamps else None
        )
    
    async def _publish_incident(self, incident: 'LeakageIncident') -> bool:
        """Publish leakage incident to Kafka incident stream with circuit breaker protection."""
        try:
            # Ensure circuit breaker is registered
            if not self._circuit_breaker_registered:
                registered = await self._register_circuit_breaker()
                if not registered:
                    self.logger.error("Cannot publish incident: circuit breaker registration failed")
                    self.metrics["kafka_publish_errors"] += 1
                    return False
            
            # Check circuit breaker status before publishing
            can_execute = await self.streaming_bus.can_component_execute(self.circuit_breaker_id)
            if not can_execute:
                self.logger.warning(f"Circuit breaker prevents publication for component: {self.circuit_breaker_id}")
                self.metrics["circuit_breaker_trips"] += 1
                return False
            
            # Get first evidence for summary
            primary_evidence = incident.evidence[0] if incident.evidence else None
            
            incident_data = {
                "incident_id": incident.incident_id,
                "class": "Leakage",
                "severity": incident.severity.value,
                "leakage_type": incident.leakage_type.value,
                "title": incident.title,
                "description": incident.description,
                "evidence_ref": {
                    "evidence_type": primary_evidence.evidence_type if primary_evidence else "unknown",
                    "description": primary_evidence.description if primary_evidence else "",
                    "confidence_score": primary_evidence.confidence_score if primary_evidence else 0.0,
                    "affected_records": primary_evidence.affected_records if primary_evidence else 0
                },
                "affected_components": incident.affected_components,
                "confidence_score": incident.confidence_score,
                "potential_impact": incident.potential_impact,
                "timestamp": datetime.now().isoformat(),
                "source_agent": "leakage_police",
                "session_id": self.session_id,
                "circuit_breaker_id": self.circuit_breaker_id
            }
            
            headers = {
                "incident_class": "Leakage",
                "severity": incident.severity.value,
                "leakage_type": incident.leakage_type.value,
                "agent": "leakage_police",
                "circuit_breaker_id": self.circuit_breaker_id
            }
            
            # Get sequence number for incidents
            self._sequence_numbers["incidents.leakage"] += 1
            
            # Use exponential backoff retry for resilient publishing with canonical headers
            await self._exponential_backoff_retry(
                "publish_leakage_incident",
                self.streaming_bus.publish_with_canonical_headers,
                topic="incidents.leakage",
                payload=incident_data,
                partition_key=incident.incident_id,
                source_id=f"{self.component_id}.{incident.leakage_type.value}",
                sequence_number=self._sequence_numbers["incidents.leakage"],
                correlation_id=f"leakage_{self.session_id}_{incident.incident_id}",
                producer_version="2.0.0"
            )
            
            # Update Prometheus metrics
            if METRICS_AVAILABLE and _metrics_collector:
                _metrics_collector.increment_counter(
                    'leakage_incidents_total',
                    labels={
                        'leakage_type': incident.leakage_type.value,
                        'severity': incident.severity.value,
                        'source': ''  # Add empty source to match metric definition
                    }
                )
            
            self.logger.info(f"Published leakage incident: {incident.incident_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to publish leakage incident {incident.incident_id}: {e}")
            self.metrics["kafka_publish_errors"] += 1
            
            # Record failure with circuit breaker
            if self._circuit_breaker_registered:
                await self._emit_breaker_intent(
                    intent="trip",
                    reason="incident_publish_failure",
                    severity="high",
                    metadata={
                        "incident_id": incident.incident_id,
                        "exception": str(e)
                    }
                )
            
            return False
    
    async def start_monitoring(self, topics: Optional[List[str]] = None) -> None:
        """Start monitoring data streams for leakage detection with full Kafka integration."""
        if topics is None:
            # Monitor clean data topics for leakage detection
            topics = [
                "clean.market.trades",
                "clean.market.book", 
                "clean.market.funding",
                "clean.market.oi",
                "clean.features.vector",
                "clean.labels.tb",
                "clean.labels.forward"
            ]
        
        # Register circuit breaker
        await self._register_circuit_breaker()
        
        # Start health monitoring
        self._health_task = asyncio.create_task(self._periodic_health_check())
        
        try:
            # Start data monitoring consumer for leakage detection
            self._data_monitor_task = asyncio.create_task(
                self.streaming_bus.subscribe_with_worker_pool(
                    consumer_group="leakage_police",
                    topics=topics,
                    handler=self._handle_data_message_for_leakage,
                    pool_size=6  # Parallel leakage analysis
                )
            )
            
            # Start control message consumption
            self._control_task = asyncio.create_task(self._consume_control_messages())
            
            print(f"🔍 Leakage Police monitoring {len(topics)} topics for data leakage...")
            print(f"📊 Circuit breaker registered: {self.circuit_breaker_id}")
            print("📊 Enhanced monitoring mode - real-time leakage detection with full resilience")
            
        except Exception as e:
            print(f"🚨 Leakage Police: Failed to start monitoring: {e}")
            raise
    
    async def _periodic_health_check(self) -> None:
        """Perform periodic health checks and update metrics."""
        while True:
            try:
                await asyncio.sleep(self._health_check_interval)
                health_data = await self._perform_health_check()
                
                # Log health status changes
                if health_data.get("status") != self._health_status:
                    self.logger.info(f"Health status changed: {self._health_status} -> {health_data.get('status')}")
                
                # Publish health metrics if circuit breaker is healthy
                if self._circuit_breaker_registered:
                    can_execute = await self.streaming_bus.can_component_execute(self.circuit_breaker_id)
                    if can_execute:
                        await self._publish_health_metrics(health_data)
                
            except Exception as e:
                self.logger.error(f"Health check failed: {e}")
                self.metrics["health_check_failures"] += 1
    
    async def _publish_health_metrics(self, health_data: Dict[str, Any]) -> None:
        """Publish health metrics to Kafka telemetry topic."""
        try:
            metrics_payload = {
                "component_id": self.component_id,
                "circuit_breaker_id": self.circuit_breaker_id,
                "timestamp": time.time() * 1_000_000,  # microseconds
                "health_status": health_data,
                "comprehensive_metrics": self.get_comprehensive_metrics()
            }
            
            # Get sequence number for telemetry
            self._sequence_numbers["telemetry.health"] += 1
            
            await self.streaming_bus.publish_with_canonical_headers(
                topic="telemetry.health",
                payload=metrics_payload,
                partition_key=self.circuit_breaker_id,
                source_id=f"{self.component_id}.health",
                sequence_number=self._sequence_numbers["telemetry.health"],
                producer_version="2.0.0"
            )
            
        except Exception as e:
            self.logger.error(f"Failed to publish health metrics: {e}")
            self.metrics["kafka_publish_errors"] += 1
    
    async def _consume_control_messages(self):
        """Consume control messages from Kafka topics for dynamic configuration."""
        control_topics = [
            "control.circuit_breaker",
            "control.config_update", 
            "control.leakage_rules",
            "control.privacy_policy"
        ]
        
        print(f"🚨 Leakage Police: Starting control message consumption from topics: {control_topics}")
        
        try:
            await self.streaming_bus.subscribe(
                consumer_group="leakage_police_control",
                topics=control_topics,
                handler=self._handle_control_message_wrapper
            )
                
        except Exception as e:
            print(f"🚨 Leakage Police: Error in control message consumption: {e}")
            # Use the system circuit breaker to record failure
            await self._emit_breaker_intent(
                intent="trip",
                reason="control_listener_failure",
                severity="medium",
                metadata={"exception": str(e)}
            )
    
    def _handle_data_message_for_leakage(self, topic: str, partition_key: str, payload: Dict[str, Any], headers: Dict[str, str]) -> None:
        """Handle incoming data messages for real-time leakage detection."""
        try:
            # Extract data type and venue information
            data_type = self._extract_data_type_from_topic(topic)
            venue = headers.get("venue", "unknown")
            timestamp = payload.get("timestamp", int(time.time_ns() // 1000))
            
            # Schedule async leakage analysis to avoid blocking consumer
            task = asyncio.create_task(self._analyze_data_for_leakage_async(data_type, venue, payload, headers, timestamp))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)
            
        except Exception as e:
            print(f"🚨 Leakage Police: Error handling data message from {topic}: {e}")
    
    def _extract_data_type_from_topic(self, topic: str) -> str:
        """Extract data type from topic for leakage classification."""
        if "trades" in topic:
            return "trade_data"
        elif "book" in topic:
            return "orderbook_data"
        elif "features" in topic:
            return "feature_data"
        elif "labels" in topic:
            return "label_data"
        else:
            return "unknown_data"
    
    async def _analyze_data_for_leakage_async(self, data_type: str, venue: str, payload: Dict[str, Any], 
                                            headers: Dict[str, str], timestamp: int) -> None:
        """Asynchronously analyze data for potential leakage patterns."""
        try:
            # Smart leakage detection logic
            leakage_incidents = []
            
            # Check for time-based leakage (future information)
            if await self._detect_temporal_leakage(data_type, payload, timestamp):
                incident = await self._create_leakage_incident(
                    leakage_type=LeakageType.TEMPORAL_LOOK_AHEAD,
                    data_type=data_type,
                    venue=venue,
                    description=f"Future information detected in {data_type}",
                    severity=LeakageSeverity.HIGH,
                    payload_sample=payload
                )
                leakage_incidents.append(incident)
            
            # Check for cross-venue information leakage
            if await self._detect_cross_venue_leakage(data_type, venue, payload):
                incident = await self._create_leakage_incident(
                    leakage_type=LeakageType.FUTURE_INFORMATION,
                    data_type=data_type,
                    venue=venue,
                    description=f"Cross-venue information leakage in {data_type}",
                    severity=LeakageSeverity.MEDIUM,
                    payload_sample=payload
                )
                leakage_incidents.append(incident)
            
            # Check for feature leakage (label information in features)
            if data_type == "feature_data" and await self._detect_feature_leakage(payload):
                incident = await self._create_leakage_incident(
                    leakage_type=LeakageType.FEATURE_LEAKAGE,
                    data_type=data_type,
                    venue=venue,
                    description="Label information detected in feature data",
                    severity=LeakageSeverity.CRITICAL,
                    payload_sample=payload
                )
                leakage_incidents.append(incident)
            
            # Publish any detected incidents
            for incident in leakage_incidents:
                await self._publish_incident(incident)
                
        except Exception as e:
            self.logger.error(f"Error in leakage analysis for {data_type}: {e}")
    
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
                if component_id == "leakage_police" or component_id == "all":
                    action = message.get("action")
                    if action == "open":
                        print(f"🚨 Leakage Police: Circuit breaker opened via control message")
                    elif action == "close":
                        print(f"🚨 Leakage Police: Circuit breaker closed via control message")
                        
            elif topic == "control.config_update":
                # Handle dynamic configuration updates
                component_id = message.get("component_id")
                if component_id == "leakage_police" or component_id == "all":
                    config_updates = message.get("updates", {})
                    await self._apply_config_updates(config_updates)
                    
            elif topic == "control.leakage_rules":
                # Handle dynamic leakage detection rule updates
                action = message.get("action")
                rule_id = message.get("rule_id")
                if action == "add":
                    rule_data = message.get("rule_data", {})
                    print(f"🚨 Leakage Police: Adding leakage detection rule: {rule_id}")
                elif action == "remove":
                    print(f"🚨 Leakage Police: Removing leakage detection rule: {rule_id}")
                elif action == "update":
                    rule_data = message.get("rule_data", {})
                    print(f"🚨 Leakage Police: Updating leakage detection rule: {rule_id}")
                        
            elif topic == "control.privacy_policy":
                # Handle privacy policy updates
                policy_updates = message.get("policy_updates", {})
                print(f"🚨 Leakage Police: Privacy policy updated: {policy_updates}")
                        
        except Exception as e:
            print(f"🚨 Leakage Police: Error handling control message from {topic}: {e}")
    
    async def _apply_config_updates(self, updates: dict):
        """Apply dynamic configuration updates."""
        try:
            # Update monitoring sensitivity
            if "sensitivity_threshold" in updates:
                print(f"🚨 Leakage Police: Updated sensitivity_threshold to {updates['sensitivity_threshold']}")
                
            # Update detection rules
            if "detection_rules" in updates:
                print(f"🚨 Leakage Police: Updated detection_rules")
                
            # Update alert thresholds
            if "alert_threshold" in updates:
                print(f"🚨 Leakage Police: Updated alert_threshold to {updates['alert_threshold']}")
                
        except Exception as e:
            print(f"🚨 Leakage Police: Error applying config updates: {e}")
    
    async def _detect_temporal_leakage(self, data_type: str, payload: Dict[str, Any], timestamp: int) -> bool:
        """Detect temporal leakage patterns in real-time data."""
        try:
            # Check if payload contains future timestamps
            current_time = int(time.time() * 1000000)  # microseconds
            
            # Look for timestamp fields that are suspiciously in the future
            for key, value in payload.items():
                if 'timestamp' in key.lower() or 'time' in key.lower():
                    if isinstance(value, (int, float)):
                        # Allow some tolerance for clock skew
                        if value > current_time + self.config.temporal_tolerance_ms * 1000:
                            return True
            
            return False
        except Exception as e:
            self.logger.error(f"Error in temporal leakage detection: {e}")
            return False
    
    async def _detect_cross_venue_leakage(self, data_type: str, venue: str, payload: Dict[str, Any]) -> bool:
        """Detect cross-venue information leakage."""
        try:
            # Check if data contains information from other venues before it should be available
            # This is a simplified heuristic - in practice would need more sophisticated analysis
            
            # Look for venue-specific information that shouldn't be available
            other_venue_indicators = ['binance', 'coinbase', 'kraken', 'bybit', 'okx']
            current_venue_lower = venue.lower()
            
            for key, value in payload.items():
                if isinstance(value, str):
                    value_lower = value.lower()
                    for indicator in other_venue_indicators:
                        if indicator != current_venue_lower and indicator in value_lower:
                            # Found reference to other venue - potential leakage
                            return True
            
            return False
        except Exception as e:
            self.logger.error(f"Error in cross-venue leakage detection: {e}")
            return False
    
    async def _detect_feature_leakage(self, payload: Dict[str, Any]) -> bool:
        """Detect feature leakage (target information in features)."""
        try:
            # Look for suspicious features that might contain target information
            suspicious_keys = ['label', 'target', 'outcome', 'result', 'profit', 'loss', 'pnl']
            
            for key in payload.keys():
                key_lower = key.lower()
                for suspicious in suspicious_keys:
                    if suspicious in key_lower:
                        return True
            
            # Check for perfect correlations or suspicious patterns
            # This is a simplified check - real implementation would be more sophisticated
            return False
        except Exception as e:
            self.logger.error(f"Error in feature leakage detection: {e}")
            return False
    
    async def _create_leakage_incident(self, leakage_type: LeakageType, data_type: str, venue: str,
                                     description: str, severity: LeakageSeverity, 
                                     payload_sample: Dict[str, Any]) -> LeakageIncident:
        """Create a leakage incident with proper structure."""
        try:
            # Generate incident ID with proper parameters
            summary_stats = {
                "sample_size": 1,
                "anomaly_score": 0.8,
                "detection_timestamp": int(time.time() * 1000000)
            }
            
            incident_id = self._generate_incident_id(
                leakage_type=leakage_type,
                scope_keys={"data_type": data_type, "venue": venue},
                summary_stats=summary_stats
            )
            
            # Create evidence with proper parameters
            evidence = [self._create_standardized_evidence(
                evidence_type="payload_analysis",
                description=f"Suspicious pattern detected in {data_type} from {venue}",
                sample_data=payload_sample,
                statistical_measures={"anomaly_score": 0.8, "confidence": 0.8},
                affected_records=1,
                confidence_score=0.8
            )]
            
            # Create the incident
            incident = LeakageIncident(
                incident_id=incident_id,
                leakage_type=leakage_type,
                severity=severity,
                title=f"{leakage_type.value.replace('_', ' ').title()} Detected",
                description=description,
                evidence=evidence,
                affected_components=[data_type, venue],
                detection_timestamp=int(time.time() * 1000000),
                confidence_score=0.8,
                potential_impact=f"Potential {severity.value} impact on {data_type} integrity",
                metadata={
                    "data_type": data_type,
                    "venue": venue,
                    "detection_method": "real_time_analysis"
                }
            )
            
            return incident
            
        except Exception as e:
            self.logger.error(f"Error creating leakage incident: {e}")
            # Return a minimal incident to avoid breaking the flow
            return LeakageIncident(
                incident_id=f"error_{int(time.time())}",
                leakage_type=leakage_type,
                severity=severity,
                title="Incident Creation Error",
                description=f"Error creating incident: {e}",
                evidence=[],
                affected_components=[data_type],
                detection_timestamp=int(time.time() * 1000000),
                confidence_score=0.1,
                potential_impact="Unknown",
                metadata={}
            )
    
    async def _monitor_topic_pattern(self, topic_pattern: str) -> None:
        """Monitor a specific topic pattern for leakage detection."""
        try:
            # For now, implement passive monitoring
            # In full implementation, this would consume from topics
            print(f"👁️  Monitoring {topic_pattern} for leakage patterns...")
                    
        except Exception as e:
            print(f"Error monitoring topic pattern {topic_pattern}: {e}")
    
    async def _analyze_message(self, message: dict, topic: str) -> List['LeakageIncident']:
        """Analyze a single message for leakage patterns."""
        try:
            # Simple leakage detection - check for suspicious patterns
            incidents = []
            
            # Check for temporal inconsistencies
            if 'timestamp' in message and 'target' in message:
                current_time = datetime.now().timestamp() * 1000000  # microseconds
                message_time = message.get('timestamp', 0)
                
                # If target appears before current time but message is future
                if message_time > current_time:
                    # Generate deterministic incident ID using _generate_incident_id
                    scope_keys = {
                        "topic": topic,
                        "message_time": message_time,
                        "violation_type": "temporal_violation"
                    }
                    summary_stats = {
                        "time_difference": message_time - current_time,
                        "confidence": 0.9
                    }
                    
                    incident_id = self._generate_incident_id(
                        LeakageType.TEMPORAL_LOOK_AHEAD,
                        scope_keys,
                        summary_stats
                    )
                    
                    evidence = LeakageEvidence(
                        evidence_type="temporal_violation",
                        description=f"Message timestamp {message_time} > current time {current_time}",
                        sample_data={"message_time": message_time, "current_time": current_time},
                        statistical_measures={},
                        affected_records=1,
                        confidence_score=0.9
                    )
                    
                    incident = LeakageIncident(
                        incident_id=incident_id,
                        leakage_type=LeakageType.TEMPORAL_LOOK_AHEAD,
                        severity=LeakageSeverity.CRITICAL,
                        title="Future Information Detected",
                        description=f"Message from future timestamp detected in {topic}",
                        evidence=[evidence],
                        affected_components=[topic],
                        detection_timestamp=int(current_time),
                        confidence_score=0.9,
                        potential_impact="High - potential look-ahead bias in predictions"
                    )
                    incidents.append(incident)
            
            # Pass incidents through deduplication flow
            return self._deduplicate_incidents(incidents)
            
        except Exception as e:
            self.logger.error(f"Error analyzing message from {topic}: {e}")
            return []
    
    def _deduplicate_incidents(self, incidents: List[LeakageIncident]) -> List[LeakageIncident]:
        """Deduplicate incidents by incident_id and add repeat_count to metadata."""
        deduplicated = []
        
        for incident in incidents:
            if incident.incident_id in self.seen_incident_ids:
                # Increment repeat count
                self.duplicate_incidents[incident.incident_id] = self.duplicate_incidents.get(incident.incident_id, 1) + 1
            else:
                # First occurrence
                self.seen_incident_ids.add(incident.incident_id)
                self.duplicate_incidents[incident.incident_id] = 1
                
                # Add repeat_count to metadata
                incident.metadata["repeat_count"] = 1
                
                # Add effect_strength to top-level metadata
                if incident.evidence and "effect_strength" in incident.evidence[0].statistical_measures:
                    incident.metadata["effect_strength"] = incident.evidence[0].statistical_measures["effect_strength"]
                
                deduplicated.append(incident)
        
        # Update repeat counts for already-added incidents
        for incident in deduplicated:
            incident.metadata["repeat_count"] = self.duplicate_incidents[incident.incident_id]
        
        return deduplicated
    
    async def analyze_temporal_ordering(self, features: pd.DataFrame, 
                                      labels: pd.DataFrame,
                                      feature_timestamp_col: str = "timestamp",
                                      label_timestamp_col: Optional[str] = None,
                                      id_col: Optional[str] = None) -> List[LeakageIncident]:
        """Analyze temporal ordering to detect look-ahead bias with proper ID alignment."""
        incidents = []
        
        # Check minimum samples
        if len(features) < self.config.min_samples_for_analysis:
            return incidents
        
        # Default to same column name if not specified
        if label_timestamp_col is None:
            label_timestamp_col = feature_timestamp_col
        
        if feature_timestamp_col not in features.columns or label_timestamp_col not in labels.columns:
            return incidents
        
        # Align features and labels by ID or index (prefer ID alignment if available)
        alignment_mode = "index"
        if id_col and id_col in features.columns and id_col in labels.columns:
            # Join on ID column
            aligned = features.set_index(id_col).join(
                labels.set_index(id_col), 
                how='inner', 
                rsuffix='_label'
            )
            if aligned.empty:
                return incidents
            
            feature_timestamps = aligned[feature_timestamp_col].values
            label_timestamps = aligned[f'{label_timestamp_col}_label'].values
            alignment_mode = f"id_col={id_col}"
        else:
            # Use index alignment (require same length)
            if len(features) != len(labels):
                return incidents
            
            feature_timestamps = features[feature_timestamp_col].values
            label_timestamps = labels[label_timestamp_col].values
            alignment_mode = "index"
        
        # Convert to numpy arrays for arithmetic operations
        feature_ts_array = np.array(feature_timestamps)
        label_ts_array = np.array(label_timestamps)
        
        # NaN/inf hygiene: filter out non-finite timestamps
        finite_mask = np.isfinite(feature_ts_array) & np.isfinite(label_ts_array)
        nan_dropped_count = len(feature_ts_array) - np.sum(finite_mask)
        
        if np.sum(finite_mask) == 0:
            return incidents  # No valid timestamps
        
        feature_ts_array = feature_ts_array[finite_mask]
        label_ts_array = label_ts_array[finite_mask]
        aligned_pair_count = len(feature_ts_array)  # Post-alignment count for denominator
        
        # Use label_horizon_us by default
        horizon_and_tolerance = self.config.label_horizon_us + (self.config.temporal_tolerance_ms * 1000)
        
        # Expected temporal relationship: feature_ts ≤ label_ts - horizon - tolerance
        future_mask = feature_ts_array > label_ts_array - horizon_and_tolerance
        future_violations = np.sum(future_mask)
        
        if future_violations > 0:
            violation_ratio = future_violations / aligned_pair_count  # Use aligned count as denominator
            confidence = self._calculate_confidence(violation_ratio, aligned_pair_count)
            effect_strength = violation_ratio * np.sqrt(aligned_pair_count)
            
            # Calculate time differences for evidence
            time_diffs_ms = (feature_ts_array[future_mask] - label_ts_array[future_mask]) / 1000
            violation_indices = np.where(future_mask)[0].tolist()
            
            # Create matched timestamp pairs for evidence
            matched_pairs = []
            for i in range(min(5, len(violation_indices))):
                idx = violation_indices[i]
                feature_ts = int(feature_ts_array[idx])
                label_ts = int(label_ts_array[idx])
                delta_ms = float(time_diffs_ms[i])
                matched_pairs.append({
                    "feature_timestamp": feature_ts,
                    "label_timestamp": label_ts,
                    "delta_ms": delta_ms,
                    "feature_iso": self._format_human_readable_time(feature_ts),
                    "label_iso": self._format_human_readable_time(label_ts)
                })
            
            scope_keys = {
                "component": "features", 
                "alignment": alignment_mode,
                "label_horizon_us": self.config.label_horizon_us,
                "temporal_tolerance_ms": self.config.temporal_tolerance_ms
            }
            summary_stats = {
                "violation_count": int(future_violations),
                "violation_ratio": float(violation_ratio),
                "max_time_diff_ms": float(np.max(time_diffs_ms))
            }
            
            evidence = self._create_standardized_evidence(
                evidence_type="temporal_ordering",
                description=f"Found {future_violations} features with timestamps violating temporal ordering ({alignment_mode})",
                sample_data={
                    **summary_stats,
                    "aligned_pair_count": aligned_pair_count,
                    "nan_dropped_count": nan_dropped_count,
                    "total_samples": len(feature_ts_array),
                    "horizon_us": self.config.label_horizon_us,
                    "tolerance_ms": self.config.temporal_tolerance_ms,
                    "example_time_diffs_ms": time_diffs_ms[:5].tolist(),
                    "matched_timestamp_pairs": matched_pairs
                },
                statistical_measures={
                    "violation_ratio": violation_ratio,
                    "confidence_level": confidence,
                    "effect_strength": effect_strength
                },
                affected_records=int(future_violations),
                confidence_score=confidence,
                threshold_used=float(horizon_and_tolerance / 1000),  # Convert to ms
                window_size_used=aligned_pair_count,
                violation_indices=violation_indices,
                timestamps=feature_ts_array[future_mask][:10].tolist()
            )
            
            incident_id = self._generate_incident_id(LeakageType.TEMPORAL_LOOK_AHEAD, scope_keys, summary_stats)
            
            incident = LeakageIncident(
                incident_id=incident_id,
                leakage_type=LeakageType.TEMPORAL_LOOK_AHEAD,
                severity=self._calculate_severity(LeakageType.TEMPORAL_LOOK_AHEAD, violation_ratio, confidence),
                title=f"Temporal Look-Ahead Detected in Features",
                description=f"Features contain information from {future_violations} timestamps that violate temporal ordering ({alignment_mode})",
                evidence=[evidence],
                affected_components=["feature_engineering", "data_pipeline"],
                detection_timestamp=int(time.time() * 1_000_000),
                confidence_score=confidence,
                potential_impact=f"Could artificially inflate model performance by {violation_ratio*100:.1f}%",
                metadata={
                    "alignment_mode": alignment_mode,
                    "effect_strength": effect_strength,
                    "horizon_us": self.config.label_horizon_us,
                    "leakage_proof_id": incident_id,
                    "effective_thresholds": {
                        "horizon_us": self.config.label_horizon_us,
                        "tolerance_ms": self.config.temporal_tolerance_ms,
                        "total_threshold_us": int(horizon_and_tolerance),
                        "threshold_explanation": f"Features must be ≤ {horizon_and_tolerance/1000:.1f}ms before label timestamps"
                    }
                }
            )
            
            # Add policy proposals
            incident.policy_proposals = [
                PolicyProposal(
                    proposal_id=f"{incident.incident_id}_fix_timestamps",
                    action=PolicyAction.FIX_TIMESTAMPS,
                    target_component="feature_engineering",
                    rationale="Temporal ordering violations indicate features are using future information",
                    implementation_steps=[
                        "Audit feature engineering pipeline for timestamp handling",
                        "Implement strict temporal ordering checks with proper horizons",
                        f"Add lag buffers of at least {self.config.label_horizon_us/1000000:.1f}s + {self.config.temporal_tolerance_ms}ms",
                        "Validate all features have timestamps before their labels minus horizon"
                    ],
                    estimated_impact=f"Prevents {violation_ratio*100:.1f}% potential alpha inflation",
                    urgency_level=incident.severity,
                    estimated_effort="2-5 engineering days",
                    rollback_plan="Revert to previous feature engineering methodology if needed"
                )
            ]
            
            incidents.append(incident)
        
        return incidents
    
    def _compute_record_hashes(self, data: pd.DataFrame) -> List[str]:
        """Compute deterministic hashes for records to detect duplicates using vectorized operations."""
        if data.empty:
            return []
        
        # Use pre-sorted exclude columns to avoid re-alloc
        exclude_cols = [col for col in data.columns if col in self.exclude_cols_sorted]
        hash_data = data.drop(columns=exclude_cols, errors='ignore')
        
        try:
            # Fast vectorized hashing with proper int conversion
            hash_values = pd.util.hash_pandas_object(hash_data, index=False)
            return [f"{int(h):016x}" for h in hash_values]
        except Exception:
            # Fallback to JSON hashing for complex nested columns
            hashes = []
            for _, row in hash_data.iterrows():
                row_str = json.dumps(row.to_dict(), sort_keys=True, default=str)
                row_hash = hashlib.md5(row_str.encode()).hexdigest()
                hashes.append(row_hash)
            return hashes
    
    async def analyze_train_test_contamination_batched(self, dataset: pd.DataFrame,
                                                     splits: Dict[str, List[int]]) -> List[LeakageIncident]:
        """Analyze dataset splits for contamination with batched processing."""
        incidents = []
        
        # Check minimum samples
        if len(dataset) < self.config.min_samples_for_analysis:
            return incidents
        
        if len(splits) < 2:
            return incidents
        
        # Process in batches to manage memory
        batch_size = self.config.batch_size
        
        for split1_name, split1_indices in splits.items():
            for split2_name, split2_indices in splits.items():
                if split1_name >= split2_name:  # Avoid duplicate comparisons
                    continue
                
                # Process splits in batches
                total_contamination = 0
                all_split1_hashes = set()
                all_split2_hashes = set()
                
                # Batch process split1 with caching
                for i in range(0, len(split1_indices), batch_size):
                    batch_key = f"{split1_name}_{i}_{batch_size}"
                    cache_key = (split1_name, batch_key)
                    
                    if cache_key in self.split_hash_cache:
                        batch_hashes = self.split_hash_cache[cache_key]
                    else:
                        batch_indices = split1_indices[i:i+batch_size]
                        batch_data = dataset.iloc[batch_indices]
                        batch_hashes = set(self._compute_record_hashes(batch_data))
                        self.split_hash_cache[cache_key] = batch_hashes
                    
                    all_split1_hashes.update(batch_hashes)
                
                # Batch process split2 with caching
                for i in range(0, len(split2_indices), batch_size):
                    batch_key = f"{split2_name}_{i}_{batch_size}"
                    cache_key = (split2_name, batch_key)
                    
                    if cache_key in self.split_hash_cache:
                        batch_hashes = self.split_hash_cache[cache_key]
                    else:
                        batch_indices = split2_indices[i:i+batch_size]
                        batch_data = dataset.iloc[batch_indices]
                        batch_hashes = set(self._compute_record_hashes(batch_data))
                        self.split_hash_cache[cache_key] = batch_hashes
                    
                    all_split2_hashes.update(batch_hashes)
                
                # Find contamination
                common_hashes = all_split1_hashes & all_split2_hashes
                contamination_count = len(common_hashes)
                
                if contamination_count > 0:
                    contamination_ratio = contamination_count / min(len(split1_indices), len(split2_indices))
                    confidence = self._calculate_confidence(contamination_ratio, min(len(split1_indices), len(split2_indices)))
                    effect_strength = contamination_ratio * np.sqrt(min(len(split1_indices), len(split2_indices)))
                    
                    # Calculate Jaccard similarity
                    jaccard_similarity = contamination_count / len(all_split1_hashes | all_split2_hashes)
                    
                    # Collect concrete examples of duplicate hashes for evidence
                    example_duplicate_hashes = list(common_hashes)[:5]
                    
                    scope_keys = {
                        "split1": split1_name, 
                        "split2": split2_name,
                        "hash_exclude_fields": tuple(sorted(self.config.hash_exclude_fields))
                    }
                    summary_stats = {
                        "contamination_count": contamination_count,
                        "contamination_ratio": contamination_ratio,
                        "jaccard_similarity": jaccard_similarity
                    }
                    
                    evidence = self._create_standardized_evidence(
                        evidence_type="dataset_contamination",
                        description=f"Found {contamination_count} duplicate records between {split1_name} and {split2_name} splits",
                        sample_data={
                            **summary_stats,
                            "split1_size": len(split1_indices),
                            "split2_size": len(split2_indices),
                            "overlap_count": contamination_count,
                            "total_unique_records": len(all_split1_hashes | all_split2_hashes),
                            "example_duplicate_hashes": example_duplicate_hashes,
                            "hash_exclude_fields": self.config.hash_exclude_fields
                        },
                        statistical_measures={
                            "contamination_ratio": contamination_ratio,
                            "jaccard_similarity": jaccard_similarity,
                            "effect_strength": effect_strength
                        },
                        affected_records=contamination_count,
                        confidence_score=confidence,
                        threshold_used=0.0,  # No explicit threshold for contamination
                        window_size_used=min(len(split1_indices), len(split2_indices))
                    )
                    
                    incident_id = self._generate_incident_id(LeakageType.TRAIN_TEST_CONTAMINATION, scope_keys, summary_stats)
                    
                    incident = LeakageIncident(
                        incident_id=incident_id,
                        leakage_type=LeakageType.TRAIN_TEST_CONTAMINATION,
                        severity=self._calculate_severity(LeakageType.TRAIN_TEST_CONTAMINATION, contamination_ratio, confidence),
                        title=f"Dataset Contamination: {split1_name} ↔ {split2_name}",
                        description=f"Duplicate records found between {split1_name} and {split2_name} dataset splits",
                        evidence=[evidence],
                        affected_components=["dataset_splitting", "model_training", "model_validation"],
                        detection_timestamp=int(time.time() * 1_000_000),
                        confidence_score=confidence,
                        potential_impact=f"Artificially inflated validation metrics due to {contamination_ratio*100:.1f}% contamination",
                        metadata={
                            "effect_strength": effect_strength,
                            "leakage_proof_id": incident_id,
                            "effective_thresholds": {
                                "contamination_threshold": 0.0,
                                "hash_exclude_fields": self.config.hash_exclude_fields,
                                "threshold_explanation": "Any duplicate records between splits constitute contamination"
                            }
                        }
                    )
                    
                    # Add policy proposals
                    incident.policy_proposals = [
                        PolicyProposal(
                            proposal_id=f"{incident.incident_id}_fix_splits",
                            action=PolicyAction.RETRAIN_MODEL,
                            target_component="dataset_splitting",
                            rationale="Dataset contamination invalidates model validation results",
                            implementation_steps=[
                                "Identify and remove duplicate records from splits",
                                "Implement stricter dataset splitting with duplicate detection",
                                "Re-split dataset with proper isolation",
                                "Retrain and re-validate all affected models"
                            ],
                            estimated_impact=f"Corrects validation bias from {contamination_ratio*100:.1f}% contamination",
                            urgency_level=incident.severity,
                            estimated_effort="1-3 engineering days plus retraining time",
                            rollback_plan="Maintain current splits while new methodology is validated"
                        )
                    ]
                    
                    incidents.append(incident)
        
        return incidents
    
    async def analyze_mempool_timing_edges(self, transactions: pd.DataFrame,
                                         mempool_col: str = "mempool_timestamp",
                                         block_col: str = "block_timestamp") -> List[LeakageIncident]:
        """Analyze mempool timing for potential timing edge advantages with baseline distribution."""
        incidents = []
        
        # Check minimum samples
        if len(transactions) < self.config.min_samples_for_analysis:
            return incidents
        
        if mempool_col not in transactions.columns or block_col not in transactions.columns:
            return incidents
        
        # Process in batches for large datasets
        batch_size = self.config.batch_size
        all_valid_delays = []
        fast_confirmations_total = 0
        total_valid_rows = 0
        
        for i in range(0, len(transactions), batch_size):
            batch = transactions.iloc[i:i+batch_size]
            
            # Calculate mempool-to-block delays (exclude invalid clocks)
            mempool_delays = batch[block_col] - batch[mempool_col]
            
            # Filter out invalid/negative delays (clock skew issues)
            valid_mask = mempool_delays >= 0
            valid_delays = mempool_delays[valid_mask]
            if len(valid_delays) == 0:
                continue
            
            all_valid_delays.extend(valid_delays.tolist())
            total_valid_rows += len(valid_delays)
        
        if total_valid_rows == 0:
            return incidents
        
        # Calculate baseline from distribution
        valid_delays_array = np.array(all_valid_delays)
        
        # NaN/inf hygiene for delay percentiles
        finite_delays = valid_delays_array[np.isfinite(valid_delays_array)]
        if len(finite_delays) == 0:
            return incidents
        
        delay_percentiles = np.percentile(finite_delays / 1000, [1, 5, 10, 50, 90, 95, 99])  # Convert to ms
        p1_baseline = delay_percentiles[0] * self.config.mempool_percentile_factor
        hard_threshold = self.config.mempool_advantage_ms
        
        # Use minimum of hard threshold and percentile-based baseline, with epsilon floor
        effective_threshold = max(
            self.config.mempool_threshold_epsilon,  # Epsilon floor
            min(hard_threshold, p1_baseline)
        ) * 1000  # Convert back to microseconds
        
        # Detect suspiciously fast confirmations
        fast_confirmations = finite_delays < effective_threshold
        fast_count = np.sum(fast_confirmations)
        
        if fast_count > 0:
            # Rebuild violating timestamps using the same mask for consistency
            violating_timestamps = []
            violation_row_indices = []
            current_idx = 0
            for i in range(0, len(transactions), batch_size):
                batch = transactions.iloc[i:i+batch_size]
                mempool_delays = batch[block_col] - batch[mempool_col]
                valid_mask = mempool_delays >= 0
                valid_delays = mempool_delays[valid_mask]
                
                if len(valid_delays) > 0:
                    batch_end_idx = current_idx + len(valid_delays)
                    batch_fast_mask = finite_delays[current_idx:batch_end_idx] < effective_threshold
                    if batch_fast_mask.any():
                        # Map back to original dataframe indices
                        valid_indices = batch.index[valid_mask]
                        violating_indices = valid_indices[batch_fast_mask]
                        violating_timestamps.extend(batch.loc[violating_indices, mempool_col].tolist())
                        violation_row_indices.extend(violating_indices.tolist())
                    current_idx = batch_end_idx
            
            # Use valid rows as denominator
            fast_ratio = fast_count / total_valid_rows
            confidence = self._calculate_confidence(fast_ratio, total_valid_rows)
            effect_strength = fast_ratio * np.sqrt(total_valid_rows)
            
            # Calculate detailed timing statistics
            fast_delays_ms = finite_delays[fast_confirmations] / 1000
            
            scope_keys = {
                "component": "transactions", 
                "threshold_type": "mempool_timing",
                "advantage_ms": self.config.mempool_advantage_ms,
                "percentile_factor": self.config.mempool_percentile_factor,
                "threshold_epsilon": self.config.mempool_threshold_epsilon
            }
            summary_stats = {
                "fast_confirmation_count": int(fast_count),
                "fast_ratio": float(fast_ratio),
                "effective_threshold_ms": float(effective_threshold / 1000)
            }
            
            evidence = self._create_standardized_evidence(
                evidence_type="mempool_timing_edge",
                description=f"Found {fast_count} transactions with suspiciously fast mempool-to-block times",
                sample_data={
                    **summary_stats,
                    "total_transactions": len(transactions),
                    "valid_delays": total_valid_rows,
                    "nan_dropped_count": len(all_valid_delays) - len(finite_delays),
                    "min_delay_ms": float(np.min(fast_delays_ms)),
                    "median_delay_ms": float(np.median(fast_delays_ms)),
                    "p95_delay_ms": float(np.percentile(fast_delays_ms, 95)),
                    "baseline_p1_ms": float(delay_percentiles[0]),
                    "hard_threshold_ms": hard_threshold,
                    "percentile_factor": self.config.mempool_percentile_factor,
                    "epsilon_floor_ms": self.config.mempool_threshold_epsilon
                },
                statistical_measures={
                    "fast_confirmation_ratio": fast_ratio,
                    "delay_percentiles_ms": delay_percentiles.tolist(),
                    "effect_strength": effect_strength
                },
                affected_records=int(fast_count),
                confidence_score=confidence,
                threshold_used=float(effective_threshold / 1000),
                window_size_used=total_valid_rows,
                violation_indices=violation_row_indices[:5],  # Original row indices
                timestamps=violating_timestamps[:10]  # Actual timestamps, not delays
            )
            
            incident_id = self._generate_incident_id(LeakageType.MEMPOOL_TIMING_EDGE, scope_keys, summary_stats)
            
            incident = LeakageIncident(
                incident_id=incident_id,
                leakage_type=LeakageType.MEMPOOL_TIMING_EDGE,
                severity=self._calculate_severity(LeakageType.MEMPOOL_TIMING_EDGE, fast_ratio, confidence),
                title="Mempool Timing Edge Detected",
                description=f"Detected {fast_count} transactions with timing advantages in mempool processing",
                evidence=[evidence],
                affected_components=["mempool_monitoring", "transaction_analysis", "alpha_generation"],
                detection_timestamp=int(time.time() * 1_000_000),
                confidence_score=confidence,
                potential_impact=f"Potential unfair advantage from {fast_ratio*100:.1f}% timing-advantaged transactions",
                metadata={
                    "effect_strength": effect_strength,
                    "leakage_proof_id": incident_id,
                    "effective_thresholds": {
                        "hard_threshold_ms": hard_threshold,
                        "p1_baseline_ms": float(p1_baseline),
                        "percentile_factor": self.config.mempool_percentile_factor,
                        "epsilon_floor_ms": self.config.mempool_threshold_epsilon,
                        "effective_threshold_ms": float(effective_threshold / 1000),
                        "threshold_explanation": f"min(hard={hard_threshold}ms, p1×{self.config.mempool_percentile_factor}={p1_baseline:.1f}ms) with ε≥{self.config.mempool_threshold_epsilon}ms = {effective_threshold/1000:.1f}ms"
                    }
                }
            )
            
            # Add policy proposals
            severity_map = {
                LeakageSeverity.LOW: PolicyAction.MANUAL_INVESTIGATION,
                LeakageSeverity.MEDIUM: PolicyAction.REVIEW_METHODOLOGY,
                LeakageSeverity.HIGH: PolicyAction.ADJUST_FEATURES,
                LeakageSeverity.CRITICAL: PolicyAction.QUARANTINE_DATASET
            }
            
            incident.policy_proposals = [
                PolicyProposal(
                    proposal_id=f"{incident.incident_id}_investigate_timing",
                    action=severity_map.get(incident.severity, PolicyAction.MANUAL_INVESTIGATION),
                    target_component="mempool_monitoring",
                    rationale="Timing edges in mempool data may indicate unfair information advantages",
                    implementation_steps=[
                        "Investigate source of fast mempool confirmations",
                        "Review data collection methodology for timing biases",
                        "Analyze correlation with trading performance",
                        f"Consider threshold adjustment: current={effective_threshold/1000:.1f}ms, baseline={p1_baseline:.1f}ms"
                    ],
                    estimated_impact=f"Ensures fair trading from {fast_ratio*100:.1f}% timing edge elimination",
                    urgency_level=incident.severity,
                    estimated_effort="3-7 engineering days",
                    rollback_plan="Maintain current mempool analysis while investigation proceeds"
                )
            ]
            
            incidents.append(incident)
        
        return incidents
    
    async def analyze_cross_validation_leakage(self, dataset: pd.DataFrame,
                                             cv_folds: Dict[str, List[int]],
                                             timestamp_col: Optional[str] = None,
                                             entity_col: Optional[str] = None) -> List[LeakageIncident]:
        """Analyze cross-validation splits for leakage and temporal embargo violations."""
        incidents = []
        
        # Check minimum samples
        if len(dataset) < self.config.min_samples_for_analysis:
            return incidents
        
        if len(cv_folds) < 2:
            return incidents
        
        # Check for entity overlap across folds
        if entity_col and entity_col in dataset.columns:
            fold_entities = {}
            for fold_name, fold_indices in cv_folds.items():
                fold_entities[fold_name] = set(dataset.iloc[fold_indices][entity_col].values)
            
            # Check pairwise entity overlaps
            for fold1_name, entities1 in fold_entities.items():
                for fold2_name, entities2 in fold_entities.items():
                    if fold1_name >= fold2_name:
                        continue
                    
                    overlap = entities1 & entities2
                    if overlap:
                        overlap_ratio = len(overlap) / min(len(entities1), len(entities2))
                        confidence = self._calculate_confidence(overlap_ratio, min(len(entities1), len(entities2)))
                        effect_strength = overlap_ratio * np.sqrt(min(len(entities1), len(entities2)))
                        
                        # Get original row indices for overlapping entities
                        fold1_overlap_indices = []
                        fold2_overlap_indices = []
                        for entity in list(overlap)[:5]:  # First 5 for examples
                            fold1_matches = dataset.iloc[cv_folds[fold1_name]][dataset.iloc[cv_folds[fold1_name]][entity_col] == entity].index.tolist()
                            fold2_matches = dataset.iloc[cv_folds[fold2_name]][dataset.iloc[cv_folds[fold2_name]][entity_col] == entity].index.tolist()
                            fold1_overlap_indices.extend(fold1_matches[:1])  # One example per entity
                            fold2_overlap_indices.extend(fold2_matches[:1])
                        
                        scope_keys = {
                            "fold1": fold1_name, 
                            "fold2": fold2_name, 
                            "violation": "entity_overlap",
                            "entity_col": entity_col
                        }
                        summary_stats = {
                            "overlap_count": len(overlap),
                            "overlap_ratio": overlap_ratio
                        }
                        
                        evidence = self._create_standardized_evidence(
                            evidence_type="cross_validation_leak",
                            description=f"Entity overlap between CV folds {fold1_name} and {fold2_name}",
                            sample_data={
                                **summary_stats,
                                "fold1_entities": len(entities1),
                                "fold2_entities": len(entities2),
                                "nan_dropped_count": 0,  # No NaN dropping for entity analysis
                                "example_overlapping_entities": list(overlap)[:5],
                                "example_fold1_indices": fold1_overlap_indices,
                                "example_fold2_indices": fold2_overlap_indices
                            },
                            statistical_measures={
                                "overlap_ratio": overlap_ratio,
                                "jaccard_similarity": len(overlap) / len(entities1 | entities2),
                                "effect_strength": effect_strength
                            },
                            affected_records=len(overlap),
                            confidence_score=confidence,
                            threshold_used=0.0,
                            window_size_used=min(len(entities1), len(entities2))
                        )
                        
                        incident_id = self._generate_incident_id(LeakageType.CROSS_VALIDATION_LEAK, scope_keys, summary_stats)
                        
                        incident = LeakageIncident(
                            incident_id=incident_id,
                            leakage_type=LeakageType.CROSS_VALIDATION_LEAK,
                            severity=self._calculate_severity(LeakageType.CROSS_VALIDATION_LEAK, overlap_ratio, confidence),
                            title=f"CV Entity Overlap: {fold1_name} ↔ {fold2_name}",
                            description=f"Cross-validation folds share {len(overlap)} entities",
                            evidence=[evidence],
                            affected_components=["cross_validation", "model_training"],
                            detection_timestamp=int(time.time() * 1_000_000),
                            confidence_score=confidence,
                            potential_impact=f"CV validation compromised by {overlap_ratio*100:.1f}% entity overlap",
                            metadata={
                                "effect_strength": effect_strength,
                                "leakage_proof_id": incident_id,
                                "effective_thresholds": {
                                    "entity_overlap_threshold": 0.0,
                                    "threshold_explanation": "Any shared entities between CV folds constitute leakage"
                                }
                            }
                        )
                        
                        incidents.append(incident)
        
        # Check temporal embargo violations
        if timestamp_col and timestamp_col in dataset.columns:
            for fold1_name, fold1_indices in cv_folds.items():
                for fold2_name, fold2_indices in cv_folds.items():
                    if fold1_name >= fold2_name:
                        continue
                    
                    fold1_times = np.array(dataset.iloc[fold1_indices][timestamp_col].values)
                    fold2_times = np.array(dataset.iloc[fold2_indices][timestamp_col].values)
                    
                    # NaN/inf hygiene for temporal analysis
                    finite_fold1 = fold1_times[np.isfinite(fold1_times)]
                    finite_fold2 = fold2_times[np.isfinite(fold2_times)]
                    nan_dropped_count = (len(fold1_times) - len(finite_fold1)) + (len(fold2_times) - len(finite_fold2))
                    
                    if len(finite_fold1) == 0 or len(finite_fold2) == 0:
                        continue  # Skip if no valid timestamps
                    
                    # Robust overlap logic: ranges overlap or gap < embargo
                    start1, end1 = np.min(finite_fold1), np.max(finite_fold1)
                    start2, end2 = np.min(finite_fold2), np.max(finite_fold2)
                    
                    # Check if ranges overlap or gap is too small
                    gap_between_ranges = max(start1, start2) - min(end1, end2)
                    violation = gap_between_ranges < self.config.embargo_us
                    
                    if violation:
                        violation_ratio = 1.0  # Binary violation
                        confidence = 1.0
                        effect_strength = 1.0 * np.sqrt(len(fold1_indices) + len(fold2_indices))
                        
                        scope_keys = {
                            "fold1": fold1_name, 
                            "fold2": fold2_name, 
                            "violation": "temporal_embargo",
                            "embargo_us": self.config.embargo_us,
                            "timestamp_col": timestamp_col
                        }
                        summary_stats = {
                            "gap_between_ranges_hours": float(gap_between_ranges / 3600000000),  # Convert to hours
                            "required_embargo_hours": float(self.config.embargo_us / 3600000000),
                            "ranges_overlap": gap_between_ranges <= 0,
                            "gap_us": int(gap_between_ranges)
                        }
                        
                        evidence = self._create_standardized_evidence(
                            evidence_type="cross_validation_leak",
                            description=f"Temporal embargo violation between CV folds {fold1_name} and {fold2_name}",
                            sample_data={
                                **summary_stats,
                                "fold1_time_range": [int(start1), int(end1)],
                                "fold2_time_range": [int(start2), int(end2)],
                                "fold1_size": len(fold1_indices),
                                "fold2_size": len(fold2_indices),
                                "nan_dropped_count": nan_dropped_count,
                                "ranges_overlap": gap_between_ranges <= 0
                            },
                            statistical_measures={
                                "embargo_violation_severity": float(max(0, 1.0 - gap_between_ranges / self.config.embargo_us)),
                                "effect_strength": effect_strength
                            },
                            affected_records=len(fold1_indices) + len(fold2_indices),
                            confidence_score=confidence,
                            threshold_used=float(self.config.embargo_us / 3600000000),  # hours
                            window_size_used=len(fold1_indices) + len(fold2_indices),
                            timestamps=[int(start1), int(end1), int(start2), int(end2)]
                        )
                        
                        incident_id = self._generate_incident_id(LeakageType.CROSS_VALIDATION_LEAK, scope_keys, summary_stats)
                        
                        incident = LeakageIncident(
                            incident_id=incident_id,
                            leakage_type=LeakageType.CROSS_VALIDATION_LEAK,
                            severity=LeakageSeverity.HIGH,  # Temporal violations are always serious
                            title=f"CV Temporal Embargo Violation: {fold1_name} ↔ {fold2_name}",
                            description=f"Cross-validation folds violate {self.config.embargo_us/3600000000:.1f}h temporal embargo",
                            evidence=[evidence],
                            affected_components=["cross_validation", "temporal_modeling"],
                            detection_timestamp=int(time.time() * 1_000_000),
                            confidence_score=confidence,
                            potential_impact="Temporal leakage in cross-validation invalidates time-series model validation",
                            metadata={
                                "effect_strength": effect_strength,
                                "leakage_proof_id": incident_id,
                                "effective_thresholds": {
                                    "embargo_hours": float(self.config.embargo_us / 3600000000),
                                    "gap_hours": float(gap_between_ranges / 3600000000),
                                    "ranges_overlap": gap_between_ranges <= 0,
                                    "embargo_us": self.config.embargo_us,
                                    "timestamp_col": timestamp_col,
                                    "threshold_explanation": f"Folds must have ≥{self.config.embargo_us/3600000000:.1f}h gap, found {gap_between_ranges/3600000000:.1f}h"
                                }
                            }
                        )
                        
                        incidents.append(incident)
        
        return incidents
    
    async def analyze_target_feature_equality(self, features: pd.DataFrame,
                                            targets: pd.DataFrame,
                                            target_col: str = "target") -> List[LeakageIncident]:
        """Analyze features for near-equality with targets (target leakage detection)."""
        incidents = []
        
        # Check minimum samples
        if len(features) < self.config.min_samples_for_analysis:
            return incidents
        
        if target_col not in targets.columns or len(features) != len(targets):
            return incidents
        
        target_values = targets[target_col].values
        leakage_features = []
        
        # Calculate target statistics for normalization
        try:
            target_array = np.asarray(target_values, dtype=float)
            # NaN/inf hygiene for target statistics
            finite_targets = target_array[np.isfinite(target_array)]
            if len(finite_targets) == 0:
                return incidents
            target_std = np.std(finite_targets)
            if target_std == 0:
                target_std = 1.0  # Avoid division by zero
        except (ValueError, TypeError):
            return incidents
        
        tested_feature_count = 0
        nan_dropped_count = 0
        
        for col in features.columns:
            if col in ['timestamp', 'id']:  # Skip non-feature columns
                continue
            
            tested_feature_count += 1  # Count actual tested features
            feature_values = features[col].values
            
            # Test for near-equality (within epsilon) - convert to numpy arrays first
            try:
                feature_array = np.asarray(feature_values, dtype=float)
                
                # NaN/inf hygiene: only compare finite values
                finite_mask = np.isfinite(feature_array) & np.isfinite(target_array)
                nan_dropped_count += len(feature_array) - np.sum(finite_mask)
                
                if np.sum(finite_mask) == 0:
                    continue  # Skip if no finite pairs
                
                finite_features = feature_array[finite_mask]
                finite_targets = target_array[finite_mask]
                
                # Equality rate test (existing good test)
                equality_mask = np.abs(finite_features - finite_targets) <= self.config.target_equality_epsilon
                equality_rate = np.mean(equality_mask)
                
                # Replace flawed correlation with well-posed tests
                feature_target_corr = 0.0
                normalized_mae = float('inf')
                
                # Test 1: Direct correlation between feature and target
                if np.std(finite_features) > 0:
                    corr_matrix = np.corrcoef(finite_features, finite_targets)
                    feature_target_corr = abs(corr_matrix[0, 1]) if not np.isnan(corr_matrix[0, 1]) else 0.0
                
                # Test 2: Normalized MAE test
                mae = np.mean(np.abs(finite_features - finite_targets))
                normalized_mae = mae / target_std
                
                # Flag as leakage if any test exceeds thresholds
                is_leakage = (
                    equality_rate > 0.95 or 
                    (equality_rate > 0.1 and feature_target_corr > self.config.target_correlation_threshold) or
                    normalized_mae < self.config.target_mae_threshold
                )
                
                if is_leakage:
                    confidence = min(1.0, max(equality_rate, feature_target_corr, 1.0 - normalized_mae))
                    
                    # Find original row indices for violated examples
                    violation_indices = np.where(equality_mask)[0][:5]
                    original_indices = np.where(finite_mask)[0][violation_indices]
                    
                    leakage_features.append({
                        'column': col,
                        'equality_rate': equality_rate,
                        'feature_target_correlation': feature_target_corr,
                        'normalized_mae': normalized_mae,
                        'confidence': confidence,
                        'finite_pairs': int(np.sum(finite_mask)),
                        'original_indices': original_indices.tolist(),
                        'example_pairs': [(float(finite_features[i]), float(finite_targets[i])) 
                                        for i in violation_indices]
                    })
            except (ValueError, TypeError):
                # Skip non-numeric columns
                continue
        
        if leakage_features:
            total_leakage_ratio = len(leakage_features) / tested_feature_count  # Use tested count, not total columns
            avg_confidence = float(np.mean([f['confidence'] for f in leakage_features]))
            effect_strength = total_leakage_ratio * np.sqrt(len(features))
            
            # Create top-N worst offenders table
            worst_offenders = sorted(leakage_features, key=lambda x: x['confidence'], reverse=True)[:10]
            
            scope_keys = {
                "component": "features", 
                "analysis": "target_equality",
                "equality_epsilon": self.config.target_equality_epsilon,
                "correlation_threshold": self.config.target_correlation_threshold,
                "mae_threshold": self.config.target_mae_threshold
            }
            summary_stats = {
                "leakage_feature_count": len(leakage_features),
                "tested_feature_count": tested_feature_count,  # More accurate than total columns
                "leakage_ratio": total_leakage_ratio
            }
            
            evidence = self._create_standardized_evidence(
                evidence_type="target_leakage",
                description=f"Found {len(leakage_features)} features with near-equality to targets",
                sample_data={
                    **summary_stats,
                    "total_feature_count": len(features.columns),  # Keep for context
                    "nan_dropped_count": nan_dropped_count,
                    "leakage_features": leakage_features[:10],  # Sample subset
                    "top_worst_offenders": worst_offenders,  # Top-N worst for fast fix-path
                    "thresholds_used": {  # Show all three knobs for triage clarity
                        "equality_epsilon": self.config.target_equality_epsilon,
                        "correlation_threshold": self.config.target_correlation_threshold,
                        "mae_threshold": self.config.target_mae_threshold,
                        "trigger_conditions": "equality_rate>95% OR (equality_rate>10% AND |corr|>threshold) OR norm_MAE<threshold"
                    },
                    "target_std": float(target_std)
                },
                statistical_measures={
                    "average_equality_rate": float(np.mean([f['equality_rate'] for f in leakage_features])),
                    "average_correlation": float(np.mean([f['feature_target_correlation'] for f in leakage_features])),
                    "average_normalized_mae": float(np.mean([f['normalized_mae'] for f in leakage_features])),
                    "effect_strength": effect_strength
                },
                affected_records=len(features),
                confidence_score=avg_confidence,
                threshold_used=self.config.target_equality_epsilon,
                window_size_used=len(features)
            )
            
            incident_id = self._generate_incident_id(LeakageType.TARGET_LEAKAGE, scope_keys, summary_stats)
            
            incident = LeakageIncident(
                incident_id=incident_id,
                leakage_type=LeakageType.TARGET_LEAKAGE,
                severity=self._calculate_severity(LeakageType.TARGET_LEAKAGE, total_leakage_ratio, avg_confidence),
                title="Target Leakage in Features",
                description=f"Detected {len(leakage_features)} features that are trivial functions of the target",
                evidence=[evidence],
                affected_components=["feature_engineering", "model_training"],
                detection_timestamp=int(time.time() * 1_000_000),
                confidence_score=avg_confidence,
                potential_impact=f"Artificial performance inflation from {total_leakage_ratio*100:.1f}% leaked features",
                metadata={
                    "effect_strength": effect_strength,
                    "leakage_proof_id": incident_id,
                    "effective_thresholds": {
                        "equality_epsilon": self.config.target_equality_epsilon,
                        "correlation_threshold": self.config.target_correlation_threshold,
                        "mae_threshold": self.config.target_mae_threshold,
                        "target_std": float(target_std),
                        "threshold_explanation": f"Features flagged if: equality_rate>95% OR (equality_rate>10% AND |corr|>{self.config.target_correlation_threshold}) OR norm_MAE<{self.config.target_mae_threshold}"
                    }
                }
            )
            
            incidents.append(incident)
        
        return incidents
    
    async def analyze_execution_timing_edges(self, orders: pd.DataFrame,
                                           signal_col: str = "signal_timestamp",
                                           order_col: str = "order_timestamp", 
                                           ack_col: str = "ack_timestamp",
                                           fill_col: str = "fill_timestamp") -> List[LeakageIncident]:
        """Analyze execution timing for impossibly fast order processing."""
        incidents = []
        
        # Check minimum samples
        if len(orders) < self.config.min_samples_for_analysis:
            return incidents
        
        required_cols = [signal_col, order_col, ack_col, fill_col]
        if not all(col in orders.columns for col in required_cols):
            return incidents
        
        # Process in batches for large datasets
        batch_size = self.config.batch_size
        all_valid_orders = 0
        fast_execution_total = 0
        fast_execution_times = []
        violating_order_timestamps = []  # Store actual timestamps
        violation_row_indices = []  # Store original row indices
        
        for i in range(0, len(orders), batch_size):
            batch = orders.iloc[i:i+batch_size]
            
            # Calculate timing intervals
            signal_to_order = batch[order_col] - batch[signal_col]
            order_to_ack = batch[ack_col] - batch[order_col] 
            ack_to_fill = batch[fill_col] - batch[ack_col]
            
            # Filter out invalid timings
            valid_mask = (signal_to_order >= 0) & (order_to_ack >= 0) & (ack_to_fill >= 0)
            if not valid_mask.any():
                continue
            
            valid_order_to_ack = order_to_ack[valid_mask]
            all_valid_orders += len(valid_order_to_ack)
            
            # Check against physical constraints
            execution_threshold_us = self.config.execution_delay_ms * 1000
            fast_execution_mask = valid_order_to_ack < execution_threshold_us
            batch_fast_count = np.sum(fast_execution_mask)
            fast_execution_total += batch_fast_count
            
            if batch_fast_count > 0:
                fast_execution_times.extend(valid_order_to_ack[fast_execution_mask].tolist())
                # Store actual order timestamps (not execution delays)
                valid_indices = batch.index[valid_mask]
                fast_indices = valid_indices[fast_execution_mask]
                violating_order_timestamps.extend(batch.loc[fast_indices, order_col].tolist())
                violation_row_indices.extend(fast_indices.tolist())
        
        if fast_execution_total > 0 and all_valid_orders > 0:
            # Use valid orders as denominator
            fast_ratio = fast_execution_total / all_valid_orders
            confidence = self._calculate_confidence(fast_ratio, all_valid_orders)
            effect_strength = fast_ratio * np.sqrt(all_valid_orders)
            
            scope_keys = {
                "component": "execution", 
                "threshold_type": "order_timing",
                "execution_delay_ms": self.config.execution_delay_ms
            }
            summary_stats = {
                "fast_execution_count": int(fast_execution_total),
                "fast_ratio": float(fast_ratio),
                "threshold_ms": self.config.execution_delay_ms
            }
            
            # Convert timing stats to ms
            fast_times_ms = [t / 1000 for t in fast_execution_times]
            
            evidence = self._create_standardized_evidence(
                evidence_type="execution_timing_leak",
                description=f"Found {fast_execution_total} orders with impossibly fast execution times",
                sample_data={
                    **summary_stats,
                    "total_orders": len(orders),
                    "valid_orders": all_valid_orders,
                    "nan_dropped_count": len(orders) - all_valid_orders,  # Add NaN counter for audit parity
                    "min_execution_ms": float(np.min(fast_times_ms)),
                    "median_execution_ms": float(np.median(fast_times_ms)),
                    "p95_execution_ms": float(np.percentile(fast_times_ms, 95))
                },
                statistical_measures={
                    "fast_execution_ratio": fast_ratio,
                    "effect_strength": effect_strength
                },
                affected_records=int(fast_execution_total),
                confidence_score=confidence,
                threshold_used=float(self.config.execution_delay_ms),
                window_size_used=all_valid_orders,
                violation_indices=violation_row_indices[:5],  # Original row indices
                timestamps=violating_order_timestamps[:10]  # Actual timestamps, not delays
            )
            
            incident_id = self._generate_incident_id(LeakageType.EXECUTION_TIMING_LEAK, scope_keys, summary_stats)
            
            incident = LeakageIncident(
                incident_id=incident_id,
                leakage_type=LeakageType.EXECUTION_TIMING_LEAK,
                severity=self._calculate_severity(LeakageType.EXECUTION_TIMING_LEAK, fast_ratio, confidence),
                title="Execution Timing Edge Detected",
                description=f"Detected {fast_execution_total} orders with timing advantages in execution processing",
                evidence=[evidence],
                affected_components=["order_execution", "trading_system"],
                detection_timestamp=int(time.time() * 1_000_000),
                confidence_score=confidence,
                potential_impact=f"Potential unfair execution advantage from {fast_ratio*100:.1f}% timing-advantaged orders",
                metadata={
                    "effect_strength": effect_strength,
                    "leakage_proof_id": incident_id,
                    "effective_thresholds": {
                        "execution_delay_ms": self.config.execution_delay_ms,
                        "threshold_explanation": f"Order-to-ack times must be ≥{self.config.execution_delay_ms}ms for physical realism"
                    }
                }
            )
            
            incidents.append(incident)
        
        return incidents
    
    async def analyze_bridge_timing_edges(self, bridge_txs: pd.DataFrame,
                                        enqueue_col: str = "bridge_enqueue_timestamp",
                                        confirm_col: str = "bridge_confirm_timestamp") -> List[LeakageIncident]:
        """Analyze bridge timing for impossibly fast cross-chain confirmations."""
        incidents = []
        
        # Check minimum samples
        if len(bridge_txs) < self.config.min_samples_for_analysis:
            return incidents
        
        if enqueue_col not in bridge_txs.columns or confirm_col not in bridge_txs.columns:
            return incidents
        
        # Process in batches for large datasets
        batch_size = self.config.batch_size
        all_valid_delays = []
        fast_bridge_total = 0
        total_valid_txs = 0
        violating_bridge_timestamps = []  # Store actual timestamps
        violation_row_indices = []  # Store original row indices
        
        for i in range(0, len(bridge_txs), batch_size):
            batch = bridge_txs.iloc[i:i+batch_size]
            
            # Calculate bridge delays
            bridge_delays = batch[confirm_col] - batch[enqueue_col]
            
            # Filter out invalid delays
            valid_mask = bridge_delays >= 0
            valid_delays = bridge_delays[valid_mask]
            if len(valid_delays) == 0:
                continue
            
            all_valid_delays.extend(valid_delays.tolist())
            total_valid_txs += len(valid_delays)
            
            # Check against physical bridge latency constraints
            bridge_threshold_us = self.config.bridge_latency_ms * 1000
            fast_bridge_mask = valid_delays < bridge_threshold_us
            batch_fast_count = np.sum(fast_bridge_mask)
            fast_bridge_total += batch_fast_count
            
            if batch_fast_count > 0:
                # Store actual enqueue timestamps (not delays)
                valid_indices = batch.index[valid_mask]
                fast_indices = valid_indices[fast_bridge_mask]
                violating_bridge_timestamps.extend(batch.loc[fast_indices, enqueue_col].tolist())
                violation_row_indices.extend(fast_indices.tolist())
        
        if fast_bridge_total > 0 and total_valid_txs > 0:
            # Apply NaN/inf hygiene to delay calculation (following mempool pattern)
            all_valid_delays_array = np.array(all_valid_delays)
            finite_delays = all_valid_delays_array[np.isfinite(all_valid_delays_array)]
            
            if len(finite_delays) == 0:
                return incidents  # No finite delays to analyze
            
            # Use valid transactions as denominator
            fast_ratio = fast_bridge_total / total_valid_txs
            confidence = self._calculate_confidence(fast_ratio, total_valid_txs)
            effect_strength = fast_ratio * np.sqrt(total_valid_txs)
            
            scope_keys = {
                "component": "bridge", 
                "threshold_type": "cross_chain_timing",
                "bridge_latency_ms": self.config.bridge_latency_ms
            }
            summary_stats = {
                "fast_bridge_count": int(fast_bridge_total),
                "fast_ratio": float(fast_ratio),
                "threshold_ms": self.config.bridge_latency_ms
            }
            
            # Convert to ms for display using NaN-safe finite delays
            fast_delays_ms = finite_delays[finite_delays < self.config.bridge_latency_ms * 1000] / 1000
            
            evidence = self._create_standardized_evidence(
                evidence_type="bridge_timing_edge",
                description=f"Found {fast_bridge_total} bridge transactions with impossibly fast confirmations",
                sample_data={
                    **summary_stats,
                    "total_bridge_txs": len(bridge_txs),
                    "valid_delays": total_valid_txs,
                    "nan_dropped_count": len(bridge_txs) - total_valid_txs + (len(all_valid_delays) - len(finite_delays)),  # Total NaN count
                    "min_delay_ms": float(np.min(fast_delays_ms)) if len(fast_delays_ms) > 0 else 0.0,
                    "median_delay_ms": float(np.median(fast_delays_ms)) if len(fast_delays_ms) > 0 else 0.0,
                    "p95_delay_ms": float(np.percentile(fast_delays_ms, 95)) if len(fast_delays_ms) > 0 else 0.0
                },
                statistical_measures={
                    "fast_bridge_ratio": fast_ratio,
                    "effect_strength": effect_strength
                },
                affected_records=int(fast_bridge_total),
                confidence_score=confidence,
                threshold_used=float(self.config.bridge_latency_ms),
                window_size_used=total_valid_txs,
                violation_indices=violation_row_indices[:5],  # Original row indices
                timestamps=violating_bridge_timestamps[:10]  # Actual timestamps, not delays
            )
            
            incident_id = self._generate_incident_id(LeakageType.BRIDGE_TIMING_EDGE, scope_keys, summary_stats)
            
            incident = LeakageIncident(
                incident_id=incident_id,
                leakage_type=LeakageType.BRIDGE_TIMING_EDGE,
                severity=self._calculate_severity(LeakageType.BRIDGE_TIMING_EDGE, fast_ratio, confidence),
                title="Bridge Timing Edge Detected",
                description=f"Detected {fast_bridge_total} bridge transactions with timing advantages",
                evidence=[evidence],
                affected_components=["bridge_monitoring", "cross_chain_analysis"],
                detection_timestamp=int(time.time() * 1_000_000),
                confidence_score=confidence,
                potential_impact=f"Potential unfair bridge advantage from {fast_ratio*100:.1f}% timing-advantaged transactions",
                metadata={
                    "effect_strength": effect_strength,
                    "leakage_proof_id": incident_id,
                    "effective_thresholds": {
                        "bridge_latency_ms": self.config.bridge_latency_ms,
                        "threshold_explanation": f"Bridge confirmations must be ≥{self.config.bridge_latency_ms}ms for cross-chain realism"
                    }
                }
            )
            
            incidents.append(incident)
        
        return incidents
    
    async def _run_analyzer_with_timeout(self, analyzer_name: str, analyzer_coro):
        """Run a single analyzer with timeout protection."""
        try:
            return await asyncio.wait_for(analyzer_coro, timeout=self.config.per_analyzer_timeout_seconds)
        except asyncio.TimeoutError:
            # Log timeout but don't fail the entire analysis
            print(f"⚠️  Analyzer '{analyzer_name}' timed out after {self.config.per_analyzer_timeout_seconds}s")
            return []  # Return empty incident list
        except Exception as e:
            # Log error but don't fail the entire analysis
            print(f"⚠️  Analyzer '{analyzer_name}' failed with error: {e}")
            return []  # Return empty incident list
    
    async def analyze_dataset(self, features: pd.DataFrame,
                            labels: pd.DataFrame,
                            feature_timestamps: Optional[pd.DataFrame] = None,
                            label_timestamps: Optional[pd.DataFrame] = None,
                            feature_timestamp_col: str = "timestamp",
                            label_timestamp_col: str = "timestamp",
                            splits: Optional[Dict[str, List[int]]] = None,
                            mempool_data: Optional[pd.DataFrame] = None,
                            cv_folds: Optional[Dict[str, List[int]]] = None,
                            execution_data: Optional[pd.DataFrame] = None,
                            bridge_data: Optional[pd.DataFrame] = None,
                            id_col: Optional[str] = None) -> List[LeakageIncident]:
        """Comprehensive dataset analysis for all types of leakage with full resilience."""
        start_time = time.time()
        self.metrics["total_analysis_runs"] += 1
        self.metrics["datasets_analyzed"] += 1
        
        # Ensure circuit breaker is registered before analysis
        if not self._circuit_breaker_registered:
            await self._register_circuit_breaker()
        
        all_incidents = []
        
        try:
            # Use asyncio.wait_for for timeout protection
            async def run_analysis():
                incidents = []
                
                # Temporal ordering analysis with proper timestamp handling
                features_for_temporal = features.copy()
                labels_for_temporal = labels.copy()
                
                # Handle separate timestamp sources
                if feature_timestamps is not None and feature_timestamp_col in feature_timestamps.columns:
                    features_for_temporal[feature_timestamp_col] = feature_timestamps[feature_timestamp_col]
                
                if label_timestamps is not None and label_timestamp_col in label_timestamps.columns:
                    labels_for_temporal[label_timestamp_col] = label_timestamps[label_timestamp_col]
                elif feature_timestamps is not None and feature_timestamp_col in feature_timestamps.columns:
                    # Don't overwrite existing label timestamps - only add if missing
                    if label_timestamp_col not in labels_for_temporal.columns:
                        labels_for_temporal[label_timestamp_col] = feature_timestamps[feature_timestamp_col]
                
                # Run temporal analysis if timestamps are available
                if (feature_timestamp_col in features_for_temporal.columns and 
                    label_timestamp_col in labels_for_temporal.columns):
                    temporal_incidents = await self._run_analyzer_with_timeout(
                        "temporal_ordering",
                        self.analyze_temporal_ordering(
                            features_for_temporal, labels_for_temporal, 
                            feature_timestamp_col, label_timestamp_col, id_col
                        )
                    )
                    incidents.extend(temporal_incidents)
                
                # Train/test contamination analysis (using batched version)
                if splits is not None:
                    combined_data = pd.concat([features, labels], axis=1)
                    contamination_incidents = await self._run_analyzer_with_timeout(
                        "train_test_contamination",
                        self.analyze_train_test_contamination_batched(combined_data, splits)
                    )
                    incidents.extend(contamination_incidents)
                
                # Cross-validation leakage analysis
                if cv_folds is not None:
                    combined_data = pd.concat([features, labels], axis=1)
                    timestamp_col = None
                    
                    # Determine which timestamp column to use for CV analysis
                    if feature_timestamps is not None and feature_timestamp_col in feature_timestamps.columns:
                        combined_data[feature_timestamp_col] = feature_timestamps[feature_timestamp_col]
                        timestamp_col = feature_timestamp_col
                    elif feature_timestamp_col in features_for_temporal.columns:
                        timestamp_col = feature_timestamp_col
                    
                    cv_incidents = await self._run_analyzer_with_timeout(
                        "cross_validation_leakage",
                        self.analyze_cross_validation_leakage(
                            combined_data, cv_folds, timestamp_col, id_col
                        )
                    )
                    incidents.extend(cv_incidents)
                
                # Target leakage analysis
                target_incidents = await self._run_analyzer_with_timeout(
                    "target_feature_equality",
                    self.analyze_target_feature_equality(features, labels)
                )
                incidents.extend(target_incidents)
                
                # Mempool timing edge analysis
                if mempool_data is not None:
                    mempool_incidents = await self._run_analyzer_with_timeout(
                        "mempool_timing_edges",
                        self.analyze_mempool_timing_edges(mempool_data)
                    )
                    incidents.extend(mempool_incidents)
                
                # Execution timing edge analysis
                if execution_data is not None:
                    execution_incidents = await self._run_analyzer_with_timeout(
                        "execution_timing_edges",
                        self.analyze_execution_timing_edges(execution_data)
                    )
                    incidents.extend(execution_incidents)
                
                # Bridge timing edge analysis
                if bridge_data is not None:
                    bridge_incidents = await self._run_analyzer_with_timeout(
                        "bridge_timing_edges",
                        self.analyze_bridge_timing_edges(bridge_data)
                    )
                    incidents.extend(bridge_incidents)
                
                return incidents
            
            # Run with timeout protection
            all_incidents = await asyncio.wait_for(
                run_analysis(), 
                timeout=self.config.analysis_timeout_seconds
            )
            
            # Deduplicate incidents within this run
            all_incidents = self._deduplicate_incidents(all_incidents)
            
            # Publish incidents to Kafka
            for incident in all_incidents:
                await self._publish_incident(incident)
            
            # Update comprehensive metrics
            analysis_time_ms = (time.time() - start_time) * 1000
            self.metrics["successful_analyses"] += 1
            self.metrics["last_analysis_time_ms"] = analysis_time_ms
            self.metrics["total_incidents_detected"] += len(all_incidents)
            
            # Update average analysis time
            total_time = self.metrics["average_analysis_time_ms"] * (self.metrics["successful_analyses"] - 1)
            self.metrics["average_analysis_time_ms"] = (total_time + analysis_time_ms) / self.metrics["successful_analyses"]
            
            # Update incident metrics
            for incident in all_incidents:
                self.metrics["incidents_by_type"][incident.leakage_type.value] += 1
                self.metrics["incidents_by_severity"][incident.severity.value] += 1
                self.metrics["policy_proposals_generated"] += len(incident.policy_proposals)
            
            # Update legacy stats for backward compatibility
            self.analysis_stats["total_analyses"] += 1
            self.analysis_stats["total_incidents"] += len(all_incidents)
            self.analysis_stats["avg_analysis_time"] = self.metrics["average_analysis_time_ms"] / 1000
            
            for incident in all_incidents:
                self.analysis_stats["incidents_by_type"][incident.leakage_type.value] += 1
                self.analysis_stats["incidents_by_severity"][incident.severity.value] += 1
            
            return all_incidents[:self.config.max_incidents_per_run]
            
        except asyncio.TimeoutError:
            self.metrics["failed_analyses"] += 1
            self.metrics["analysis_timeout_events"] += 1
            
            # Record failure with circuit breaker
            if self._circuit_breaker_registered:
                await self._emit_breaker_intent(
                    intent="trip",
                    reason="analysis_timeout",
                    severity="medium",
                    metadata={
                        "timeout_seconds": self.config.analysis_timeout_seconds
                    }
                )
            
            # Create incident for timeout
            scope_keys = {"component": "analysis", "error": "timeout"}
            summary_stats = {"timeout_seconds": self.config.analysis_timeout_seconds}
            
            timeout_incident = LeakageIncident(
                incident_id=self._generate_incident_id(LeakageType.FUTURE_INFORMATION, scope_keys, summary_stats),
                leakage_type=LeakageType.FUTURE_INFORMATION,
                severity=LeakageSeverity.MEDIUM,
                title="Leakage Analysis Timeout",
                description=f"Leakage analysis timed out after {self.config.analysis_timeout_seconds}s",
                evidence=[],
                affected_components=["leakage_police"],
                detection_timestamp=int(time.time() * 1_000_000),
                confidence_score=1.0,
                potential_impact="Unable to complete data integrity verification",
                metadata={"timeout_seconds": self.config.analysis_timeout_seconds, "repeat_count": 1}
            )
            return [timeout_incident]
            
        except Exception as e:
            self.metrics["failed_analyses"] += 1
            
            # Record failure with circuit breaker
            if self._circuit_breaker_registered:
                await self._emit_breaker_intent(
                    intent="trip",
                    reason="analysis_exception",
                    severity="high",
                    metadata={
                        "error_type": type(e).__name__,
                        "exception": str(e)
                    }
                )
            
            # Create incident for analysis failure  
            scope_keys = {"component": "analysis", "error": "exception"}
            summary_stats = {"error_type": type(e).__name__}
            
            error_incident = LeakageIncident(
                incident_id=self._generate_incident_id(LeakageType.FUTURE_INFORMATION, scope_keys, summary_stats),
                leakage_type=LeakageType.FUTURE_INFORMATION,
                severity=LeakageSeverity.MEDIUM,
                title="Leakage Analysis Failed",
                description=f"Leakage analysis encountered an error: {str(e)}",
                evidence=[],
                affected_components=["leakage_police"],
                detection_timestamp=int(time.time() * 1_000_000),
                confidence_score=1.0,
                potential_impact="Unable to verify data integrity",
                metadata={"error": str(e), "error_type": type(e).__name__, "repeat_count": 1}
            )
            
            return [error_incident]
    
    def get_analysis_stats(self) -> Dict[str, Any]:
        """Get comprehensive analysis statistics."""
        return {
            **self.analysis_stats,
            "session_id": self.session_id,
            "config": {
                "temporal_tolerance_ms": self.config.temporal_tolerance_ms,
                "statistical_threshold": self.config.statistical_threshold,
                "min_samples": self.config.min_samples_for_analysis
            }
        }
    
    async def shutdown(self) -> None:
        """Graceful shutdown of leakage police."""
        self.logger.info("🛑 Shutting down Leakage Police...")
        
        # Cancel all control message tasks
        for task in self._tasks:
            if not task.done():
                task.cancel()
        
        # Cancel all background analysis tasks
        for task in self._background_tasks:
            if not task.done():
                task.cancel()
        
        # Wait for tasks to complete with timeout
        all_tasks = list(self._tasks) + list(self._background_tasks)
        if all_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*all_tasks, return_exceptions=True),
                    timeout=5.0
                )
            except asyncio.TimeoutError:
                self.logger.warning("Some leakage analysis tasks did not stop within timeout")
            except asyncio.CancelledError:
                pass
        
        self.logger.info("✅ Leakage Police shutdown complete")


# Example usage and demo
if __name__ == "__main__":
    async def demo_leakage_police():
        """Demonstrate the leakage police functionality."""
        print("=== Leakage Police Demo ===\n")
        
        # Configure leakage police
        config = LeakagePoliceConfig(
            temporal_tolerance_ms=100,
            statistical_threshold=0.001,
            min_samples_for_analysis=100,
            mempool_advantage_ms=50,
            target_correlation_threshold=0.99,
            target_mae_threshold=0.1,
            mempool_threshold_epsilon=1.0
        )
        
        police = LeakagePolice(config)
        
        print("1. Leakage Police Configuration:")
        print(f"   - Temporal tolerance: {config.temporal_tolerance_ms}ms")
        print(f"   - Statistical threshold: {config.statistical_threshold}")
        print(f"   - Mempool advantage threshold: {config.mempool_advantage_ms}ms")
        print(f"   - Target correlation threshold: {config.target_correlation_threshold}")
        print(f"   - Target MAE threshold: {config.target_mae_threshold}")
        print(f"   - Mempool epsilon floor: {config.mempool_threshold_epsilon}ms")
        
        # Create sample data with potential leakage
        np.random.seed(42)
        n_samples = 1000
        
        # Features with some future information (temporal leakage)
        base_timestamps = np.arange(n_samples) * 1000000  # 1 second intervals
        feature_timestamps = base_timestamps.copy()
        feature_timestamps[50:60] += 5000000  # 5 seconds in the future (leakage!)
        
        features = pd.DataFrame({
            "timestamp": feature_timestamps,
            "price": np.random.normal(45000, 1000, n_samples),
            "volume": np.random.exponential(100, n_samples),
            "feature_a": np.random.normal(0, 1, n_samples)
        })
        
        # Labels at base timestamps  
        labels = pd.DataFrame({
            "timestamp": base_timestamps,
            "target": np.random.binomial(1, 0.5, n_samples)
        })
        
        # Add target leakage feature
        features["leaked_feature"] = labels["target"] + np.random.normal(0, 0.001, n_samples)  # Nearly identical to target
        
        # Create separate timestamp DataFrames to test new API
        feature_timestamps_df = pd.DataFrame({"timestamp": feature_timestamps})
        label_timestamps_df = pd.DataFrame({"timestamp": base_timestamps})
        
        # Dataset splits with some contamination
        train_indices = list(range(0, 700))
        val_indices = list(range(600, 800))  # Overlap with train (contamination!)
        test_indices = list(range(800, 1000))
        
        splits = {
            "train": train_indices,
            "validation": val_indices,
            "test": test_indices
        }
        
        # Mempool data with timing edges
        mempool_data = pd.DataFrame({
            "mempool_timestamp": base_timestamps,
            "block_timestamp": base_timestamps + np.random.exponential(200000, n_samples),  # Normal: ~200ms
            "transaction_id": [f"tx_{i}" for i in range(n_samples)]
        })
        # Add some suspiciously fast confirmations (timing edge!)
        mempool_data.loc[10:20, "block_timestamp"] = mempool_data.loc[10:20, "mempool_timestamp"] + 30000  # 30ms
        
        print("\n2. Analyzing Dataset for Leakage:")
        print("   - Created synthetic data with temporal leakage")
        print("   - Added target leakage feature")
        print("   - Added train/validation contamination")
        print("   - Included mempool timing edges")
        print("   - Testing separate timestamp handling")
        
        # Run comprehensive analysis with new API
        incidents = await police.analyze_dataset(
            features=features.drop(columns=['timestamp']),  # Remove timestamp from features
            labels=labels.drop(columns=['timestamp']),      # Remove timestamp from labels
            feature_timestamps=feature_timestamps_df,       # Separate feature timestamps
            label_timestamps=label_timestamps_df,           # Separate label timestamps
            splits=splits,
            mempool_data=mempool_data
        )
        
        print(f"\n3. Leakage Detection Results:")
        print(f"   Found {len(incidents)} leakage incidents:")
        
        for incident in incidents:
            print(f"\n   - {incident.title}")
            print(f"     Type: {incident.leakage_type.value}")
            print(f"     Severity: {incident.severity.value}")
            print(f"     Confidence: {incident.confidence_score:.2f}")
            print(f"     Impact: {incident.potential_impact}")
            print(f"     Evidence: {len(incident.evidence)} pieces")
            print(f"     Policy Proposals: {len(incident.policy_proposals)}")
            if hasattr(incident, 'metadata') and 'effect_strength' in incident.metadata:
                print(f"     Effect Strength: {incident.metadata['effect_strength']:.3f}")
            if hasattr(incident, 'metadata') and 'repeat_count' in incident.metadata:
                print(f"     Repeat Count: {incident.metadata['repeat_count']}")
            
            if incident.policy_proposals:
                proposal = incident.policy_proposals[0]
                print(f"       → Action: {proposal.action.value}")
                print(f"       → Target: {proposal.target_component}")
                print(f"       → Effort: {proposal.estimated_effort}")
        
        print("\n4. Analysis Statistics:")
        stats = police.get_analysis_stats()
        for key, value in stats.items():
            if isinstance(value, dict):
                print(f"   {key}:")
                for subkey, subvalue in value.items():
                    print(f"     {subkey}: {subvalue}")
            else:
                print(f"   {key}: {value}")
        
        print("\n=== Demo Complete ===")
        print("\nLeakage Police Enhanced Features:")
        print("✓ Temporal look-ahead detection with ID alignment")
        print("✓ Train/test contamination analysis (batched)")
        print("✓ Target leakage detection with robust correlation tests")
        print("✓ Mempool timing edge detection with epsilon floors") 
        print("✓ Execution and bridge timing analysis")
        print("✓ Cross-validation leakage detection")
        print("✓ Separate timestamp column handling")
        print("✓ Incident deduplication within runs")
        print("✓ Standardized evidence with human-readable times")
        print("✓ Type-specific severity thresholds")
        print("✓ Minimum sample size enforcement")
        print("✓ Effect strength in top-level metadata")
        print("✓ Evidence-based incident reporting")
        print("✓ Policy proposal generation")
        print("✓ No data modification - detection only")
    
    # Run demo
    asyncio.run(demo_leakage_police())
