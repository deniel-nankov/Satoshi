"""
Anomaly Detector (Data)

Mission: Fast detection of data glitches (spikes, flatlines, duplicates, discontinuities).
Outputs: incidents.Anomaly (data class).
Don't: no routing, no autoscaling, no alerts; just incidents.
"""

import logging
import time
import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Union, Set, Tuple
from decimal import Decimal
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import hashlib
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class AnomalyType(Enum):
    """Types of data anomalies that can be detected."""
    SPIKE = "spike"
    FLATLINE = "flatline"
    DUPLICATE = "duplicate"
    DISCONTINUITY = "discontinuity"
    OUTLIER = "outlier"
    MISSING_DATA = "missing_data"
    SEQUENCE_GAP = "sequence_gap"
    LEVEL_SHIFT = "level_shift"  # Formerly VALUE_DRIFT


class AnomalySeverity(Enum):
    """Severity levels for anomalies."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class AnomalyIncident:
    """Data class for anomaly incidents."""
    incident_id: str
    table_name: str
    field_name: Optional[str]
    anomaly_type: AnomalyType
    severity: AnomalySeverity
    detected_at: int  # timestamp_us (integer microseconds)
    window_start: Optional[int] = None  # timestamp_us (integer microseconds)
    window_end: Optional[int] = None  # timestamp_us (integer microseconds)
    expected_value: Optional[Union[float, str]] = None
    actual_value: Optional[Union[float, str]] = None
    deviation_magnitude: Optional[float] = None
    confidence_score: float = 0.0  # 0.0 to 1.0
    evidence: Dict[str, Any] = field(default_factory=dict)
    affected_rows: int = 1
    description: str = ""


@dataclass
class DetectionConfig:
    """Configuration for anomaly detection parameters."""
    # Spike detection
    spike_threshold_std: float = 3.0  # Standard deviations for spike detection
    spike_window_size: int = 100  # Window size for rolling statistics
    spike_debounce_count: int = 1  # Consecutive breaches required (1 = no debounce)
    spike_debounce_window: int = 5  # Window size for K/M breach counting
    
    # Flatline detection
    flatline_min_duration: int = 10  # Minimum consecutive identical values
    flatline_tolerance: float = 1e-6  # Absolute tolerance for "identical" floating values
    flatline_relative_tolerance: float = 1e-9  # Relative tolerance for floats (rtol)
    
    # Duplicate detection (improved semantics)
    duplicate_window_size: int = 1000  # Deprecated: use duplicate_window_seconds instead
    duplicate_window_seconds: float = 300.0  # Time window for duplicate detection 
    duplicate_key_fields: List[str] = field(default_factory=list)  # Key fields for duplicate detection
    exclude_timestamp_from_duplicates: bool = True  # Exclude timestamp fields from duplicate hashing by default
    
    # Discontinuity detection
    discontinuity_threshold: float = 5.0  # Threshold for detecting jumps
    discontinuity_min_gap: float = 60.0  # Minimum time gap (seconds) to consider
    
    # Outlier detection
    outlier_threshold_std: float = 2.5  # Standard deviations for outlier detection
    outlier_window_size: int = 200  # Window size for outlier detection
    outlier_debounce_count: int = 1  # Consecutive breaches required (1 = no debounce)
    outlier_debounce_window: int = 5  # Window size for K/M breach counting
    
    # Robust statistics option
    use_robust_stats: bool = False  # Use median+MAD instead of mean+std for spike/outlier detection
    
    # Adaptive threshold enhancement
    enable_adaptive_thresholds: bool = False  # Enable data-driven threshold adaptation
    adaptation_sensitivity: float = 0.1  # Sensitivity for threshold adaptation (0.1 = 10%)
    min_samples_for_adaptation: int = 1000  # Minimum samples before enabling adaptation
    
    # Missing data detection
    expected_interval_seconds: Optional[float] = None  # Expected data interval
    missing_data_tolerance: float = 1.5  # Multiplier for expected interval
    
    # Value drift detection
    level_shift_window_size: int = 500  # Window size for level shift detection
    drift_window_size: Optional[int] = None  # Deprecated: use level_shift_window_size (kept for backward compatibility)
    level_shift_threshold_z: float = 3.0  # Z-score threshold for level shift detection (robust median/MAD)
    
    # MAD≈0 fallback thresholds (explicit, not ad-hoc scaling)
    level_shift_absolute_epsilon: float = 0.01  # Absolute change threshold when MAD≈0
    level_shift_relative_epsilon: float = 0.1   # Relative change threshold when MAD≈0 (10%)
    
    # Discontinuity detection (configurable fields instead of name-based)
    discontinuity_fields: List[str] = field(default_factory=list)  # Specific fields to check for discontinuities
    
    # Sequence gap detection
    sequence_fields: List[str] = field(default_factory=list)  # Integer fields that should be strictly increasing
    sequence_gap_threshold: int = 1  # Maximum allowed gap in sequence (gaps > this trigger incidents)
    
    # Field filtering for performance
    include_fields: List[str] = field(default_factory=list)  # If specified, only analyze these fields
    exclude_fields: List[str] = field(default_factory=list)  # Skip these fields (applied after include_fields)
    exclude_high_cardinality: bool = True  # Skip string fields likely to be high cardinality (IDs, hashes, etc.)
    
    # Incident deduplication
    incident_dedupe_ttl_seconds: float = 30.0  # Time to suppress identical incidents (15-60s recommended)


@dataclass
class FieldStats:
    """Statistics for a field over a rolling window with O(1) updates."""
    # Use bounded deques sized to largest window needed
    values: deque = field(default_factory=lambda: deque(maxlen=1000))  # For flatline detection (all values)
    numeric_values: deque = field(default_factory=lambda: deque(maxlen=1000))  # For numeric stats only
    timestamps: deque = field(default_factory=lambda: deque(maxlen=1000))  # Will be resized in __post_init__
    last_value: Optional[Union[float, str]] = None
    last_timestamp: Optional[int] = None
    consecutive_identical: int = 0
    
    # O(1) rolling statistics (Welford's algorithm)
    mean: float = 0.0
    variance_sum: float = 0.0  # Sum of squared differences from mean
    std: float = 0.0
    min_val: Optional[float] = None
    max_val: Optional[float] = None
    numeric_count: int = 0  # Count of numeric values for proper readiness gating
    
    # Robust statistics (computed periodically, not every update)
    median: float = 0.0
    mad: float = 0.0  # Median Absolute Deviation
    robust_stats_dirty: bool = True  # Flag to recompute robust stats
    robust_update_counter: int = 0  # Counter for periodic robust stats updates
    
    # Rolling variance drift correction
    rolling_updates_count: int = 0  # Counter for periodic exact variance recomputation
    
    # Debounce tracking for spikes/outliers
    spike_breach_history: deque = field(default_factory=lambda: deque(maxlen=20))  # Recent breach flags
    outlier_breach_history: deque = field(default_factory=lambda: deque(maxlen=20))  # Recent breach flags
    
    # Detection config reference (set during initialization)
    detection_config: Optional[object] = None
    
    def __post_init__(self):
        """Resize deques to optimal size based on detection config."""
        if self.detection_config:
            # Calculate maximum window size needed across all detection types
            max_window = max(
                getattr(self.detection_config, 'spike_window_size', 100),
                getattr(self.detection_config, 'outlier_window_size', 200),
                getattr(self.detection_config, 'level_shift_window_size', 500)
            )
            # Add small buffer for edge cases
            optimal_size = max_window + 50
            
            # Recreate deques with optimal size
            self.values = deque(self.values, maxlen=optimal_size)
            self.numeric_values = deque(self.numeric_values, maxlen=optimal_size)
            self.timestamps = deque(self.timestamps, maxlen=optimal_size)
    
    def add_numeric_value(self, value: float) -> None:
        """Add a numeric value using O(1) Welford's algorithm for rolling stats."""
        # Update min/max
        if self.min_val is None or value < self.min_val:
            self.min_val = value
        if self.max_val is None or value > self.max_val:
            self.max_val = value
            
        # Handle window overflow for rolling stats using dedicated numeric_values deque
        old_value = None
        if len(self.numeric_values) == self.numeric_values.maxlen:
            # We're about to evict the oldest numeric value
            old_value = self.numeric_values[0]
        
        # Add new value to both deques (values for flatline, numeric_values for stats)
        self.values.append(value)
        self.numeric_values.append(value)
        
        # Update numeric statistics
        if old_value is not None:
            # Rolling window: remove old, add new (count stays same)
            self._update_rolling_stats(old_value, value)
        else:
            # Growing window: just add new
            self.numeric_count += 1
            self._update_growing_stats(value)
            
        # Mark robust stats as needing update
        self.robust_stats_dirty = True
        self.robust_update_counter += 1
    
    def _update_growing_stats(self, new_value: float) -> None:
        """Update statistics when adding to a growing window (Welford's algorithm)."""
        n = self.numeric_count
        if n == 1:
            self.mean = new_value
            self.variance_sum = 0.0
        else:
            # Welford's online algorithm
            delta = new_value - self.mean
            self.mean += delta / n
            delta2 = new_value - self.mean
            self.variance_sum += delta * delta2
            
        # Update standard deviation
        if n > 1:
            self.std = float(np.sqrt(self.variance_sum / (n - 1)))
        else:
            self.std = 0.0
    
    def _update_rolling_stats(self, old_value: float, new_value: float) -> None:
        """Update statistics when replacing old value with new in rolling window."""
        n = self.numeric_count
        if n <= 1:
            return
            
        # Increment rolling update counter for periodic variance correction
        self.rolling_updates_count += 1
        
        # Every 100 rolling updates, recompute variance exactly to prevent drift
        if self.rolling_updates_count >= 100:
            self._recompute_exact_variance()
            self.rolling_updates_count = 0
            return
            
        # Remove old value's contribution
        old_delta = old_value - self.mean
        self.mean = (self.mean * n - old_value + new_value) / n
        
        # Update variance sum (approximate for rolling window)
        new_delta = new_value - self.mean
        old_delta_new_mean = old_value - self.mean
        
        self.variance_sum = self.variance_sum - old_delta * old_delta_new_mean + new_delta * new_delta
        
        # Ensure variance sum doesn't go negative due to floating point errors
        self.variance_sum = max(0.0, self.variance_sum)
        
        # Update standard deviation
        if n > 1:
            self.std = float(np.sqrt(self.variance_sum / (n - 1)))
        else:
            self.std = 0.0
    
    def _recompute_exact_variance(self) -> None:
        """Recompute variance exactly from current window to correct accumulated drift."""
        if self.numeric_count < 2:
            self.variance_sum = 0.0
            self.std = 0.0
            return
            
        # Get current numeric values from deque
        numeric_values = list(self.numeric_values)
        n = len(numeric_values)
        
        if n < 2:
            self.variance_sum = 0.0
            self.std = 0.0
            return
            
        # Compute exact mean
        exact_mean = sum(numeric_values) / n
        
        # Compute exact variance sum
        exact_variance_sum = sum((x - exact_mean) ** 2 for x in numeric_values)
        
        # Update with exact values
        self.mean = exact_mean
        self.variance_sum = exact_variance_sum
        self.std = float(np.sqrt(exact_variance_sum / (n - 1)))
    
    def update_robust_stats_if_needed(self, force: bool = False) -> None:
        """Update robust statistics periodically (not every value) for efficiency."""
        # Update robust stats every 10 values or when forced
        if not (force or self.robust_stats_dirty and self.robust_update_counter % 10 == 0):
            return
            
        if self.numeric_count < 2:
            return
            
        # Extract numeric values for robust stats (use dedicated numeric_values deque)
        if len(self.numeric_values) >= 2:
            numeric_values = list(self.numeric_values)
            # Robust statistics
            self.median = float(np.median(numeric_values))
            # MAD = median(|x - median(x)|)
            self.mad = float(np.median(np.abs(np.array(numeric_values) - self.median)))
            
        self.robust_stats_dirty = False
    
    def update_stats(self):
        """Legacy method - now delegates to optimized add_numeric_value."""
        # This method is kept for compatibility but is no longer the primary update path
        # The new approach is to call add_numeric_value() directly when adding numeric values
        pass


class DataAnomalyDetector:
    """
    Detects data quality anomalies in streaming data.
    
    Key Features:
    - Spike detection using rolling statistics
    - Flatline detection for stuck values
    - Duplicate detection within windows
    - Discontinuity detection for time series
    - Outlier detection using statistical methods
    - Missing data detection based on expected intervals
    
    Pure Detection Agent - no routing, alerts, or infrastructure concerns.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.detection_config = DetectionConfig(**config.get('detection_params', {}))
        
        # Backward compatibility: if duplicate_window_size is set but duplicate_window_seconds isn't,
        # treat duplicate_window_size as seconds for compatibility
        if (hasattr(self.detection_config, 'duplicate_window_size') and 
            self.detection_config.duplicate_window_size != 1000 and  # Not default value
            self.detection_config.duplicate_window_seconds == 300.0):  # Is default value
            self.detection_config.duplicate_window_seconds = float(self.detection_config.duplicate_window_size)
        
        # Backward compatibility: handle drift_window_size -> level_shift_window_size migration
        if (hasattr(self.detection_config, 'drift_window_size') and 
            self.detection_config.drift_window_size is not None and
            self.detection_config.level_shift_window_size == 500):  # Is default value
            self.detection_config.level_shift_window_size = self.detection_config.drift_window_size
        
        # Field statistics tracking with optimized window sizes
        self.field_stats: Dict[str, Dict[str, FieldStats]] = defaultdict(lambda: defaultdict(self._create_field_stats))
        
        # Duplicate tracking
        self.duplicate_hashes: Dict[str, Dict[str, float]] = defaultdict(dict)  # table -> hash -> timestamp
        
        # Cross-batch tracking for missing data gaps
        self.last_timestamp_by_table: Dict[str, int] = {}
        
        # Incident deduplication tracking: (table, field, type) -> (last_emitted_at, repeat_count)
        self.incident_dedupe_cache: Dict[Tuple[str, str, str], Tuple[float, int]] = {}
        
        # Performance metrics
        self.detections_count = 0
        
        logger.info("Data Anomaly Detector initialized")
    
    def _create_field_stats(self) -> FieldStats:
        """Create a new FieldStats instance with optimal configuration."""
        stats = FieldStats()
        stats.detection_config = self.detection_config
        stats.__post_init__()  # Initialize optimal deque sizes
        return stats
    
    def _should_analyze_field(self, field_name: str, value: Any) -> bool:
        """Determine if a field should be analyzed based on filtering configuration."""
        # Apply include_fields filter first (if specified)
        if self.detection_config.include_fields:
            if field_name not in self.detection_config.include_fields:
                return False
        
        # Apply exclude_fields filter
        if field_name in self.detection_config.exclude_fields:
            return False
            
        # Apply high cardinality filter for string values
        if self.detection_config.exclude_high_cardinality and isinstance(value, str):
            # Skip fields that look like IDs, hashes, or other high-cardinality strings
            field_lower = field_name.lower()
            if any(pattern in field_lower for pattern in ['id', 'hash', 'uuid', 'guid', 'key', 'token', 'address']):
                return False
            # Skip very long strings (likely to be high cardinality)
            if len(value) > 100:
                return False
                
        return True
    
    def _generate_incident_dedupe_key(self, table_name: str, field_name: Optional[str], anomaly_type: AnomalyType) -> Tuple[str, str, str]:
        """Generate a key for incident deduplication."""
        field_key = field_name if field_name is not None else "row_level"
        return (table_name, field_key, anomaly_type.value)
    
    def _should_emit_incident(self, incident: AnomalyIncident) -> bool:
        """Check if incident should be emitted based on deduplication logic."""
        current_time = int(time.time() * 1_000_000)  # microseconds as integer
        dedupe_key = self._generate_incident_dedupe_key(incident.table_name, incident.field_name, incident.anomaly_type)
        
        if dedupe_key in self.incident_dedupe_cache:
            last_emitted_at, repeat_count = self.incident_dedupe_cache[dedupe_key]
            time_since_last = (current_time - last_emitted_at) / 1_000_000  # seconds
            
            if time_since_last < self.detection_config.incident_dedupe_ttl_seconds:
                # Suppress emission but update repeat count
                self.incident_dedupe_cache[dedupe_key] = (last_emitted_at, repeat_count + 1)
                return False
        
        # Emit incident and update cache
        self.incident_dedupe_cache[dedupe_key] = (current_time, 1)
        return True
    
    def _enrich_incident_with_context(self, incident: AnomalyIncident, stats: FieldStats, 
                                    threshold_value: Optional[float] = None, 
                                    method_used: Optional[str] = None,
                                    window_size_used: Optional[int] = None) -> AnomalyIncident:
        """Add consistent context to incident for better triage."""
        # Get repeat count from deduplication cache
        dedupe_key = self._generate_incident_dedupe_key(incident.table_name, incident.field_name, incident.anomaly_type)
        repeat_count = 1
        if dedupe_key in self.incident_dedupe_cache:
            _, repeat_count = self.incident_dedupe_cache[dedupe_key]
        
        # Add consistent context to evidence
        # For numeric anomalies (spike/outlier/level_shift), always use numeric_values count
        is_numeric_anomaly = incident.anomaly_type in [AnomalyType.SPIKE, AnomalyType.OUTLIER, AnomalyType.LEVEL_SHIFT]
        if is_numeric_anomaly:
            actual_window_size = len(stats.numeric_values)
        else:
            actual_window_size = window_size_used or len(stats.values)
        
        context = {
            "window_size_used": actual_window_size,
            "numeric_sample_count": stats.numeric_count,
            "repeat_count": repeat_count,
            "detection_method": method_used or "unknown",
            "threshold_used": threshold_value,
            "field_statistics": {
                "mean": stats.mean,
                "std": stats.std,
                "median": stats.median,
                "mad": stats.mad,
                "min": stats.min_val,
                "max": stats.max_val
            }
        }
        
        # Merge with existing evidence
        incident.evidence.update(context)
        
        return incident
    
    def _prune_dedupe_cache(self):
        """Prune expired entries from incident deduplication cache."""
        current_time = int(time.time() * 1_000_000)  # microseconds as integer
        ttl_us = self.detection_config.incident_dedupe_ttl_seconds * 1_000_000
        
        # Remove expired entries
        expired_keys = [
            key for key, (last_emitted_at, _) in self.incident_dedupe_cache.items()
            if (current_time - last_emitted_at) > ttl_us
        ]
        
        for key in expired_keys:
            del self.incident_dedupe_cache[key]
    
    def analyze_batch(self, table_name: str, rows: List[Dict[str, Any]], 
                     timestamp_field: str = "timestamp_utc_us") -> List[AnomalyIncident]:
        """
        Analyze a batch of data for anomalies.
        
        Args:
            table_name: Name of the table/stream
            rows: List of data rows
            timestamp_field: Field containing timestamp data
            
        Returns:
            List of detected anomaly incidents
        """
        incidents = []
        
        if not rows:
            return incidents
            
        # Sort rows by timestamp if possible
        sorted_rows = self._sort_rows_by_timestamp(rows, timestamp_field)
        
        for row in sorted_rows:
            row_incidents = self.analyze_row(table_name, row, timestamp_field)
            incidents.extend(row_incidents)
            
        # Batch-level anomaly detection
        batch_incidents = self._detect_batch_anomalies(table_name, sorted_rows, timestamp_field)
        incidents.extend(batch_incidents)
        
        # Update cross-batch timestamp tracking for missing data detection
        self._update_cross_batch_tracking(table_name, sorted_rows, timestamp_field)
        
        # Periodic cache pruning (every batch is small overhead)
        self._prune_dedupe_cache()
        
        self.detections_count += len(incidents)
        return incidents
    
    def analyze_row(self, table_name: str, row: Dict[str, Any], 
                   timestamp_field: str = "timestamp_utc_us") -> List[AnomalyIncident]:
        """
        Analyze a single row for anomalies.
        
        Args:
            table_name: Name of the table/stream
            row: Data row to analyze
            timestamp_field: Field containing timestamp data
            
        Returns:
            List of detected anomaly incidents
        """
        incidents = []
        current_time = int(time.time() * 1_000_000)  # microseconds as integer
        
        # Get timestamp from row
        row_timestamp = self._extract_timestamp(row, timestamp_field, current_time)
        
        # Analyze each field in the row
        for field_name, value in row.items():
            if field_name == timestamp_field:
                continue
                
            if value is None:
                continue
                
            # Skip fields based on filtering configuration
            if not self._should_analyze_field(field_name, value):
                continue
                
            # Ensure we have a valid timestamp
            if row_timestamp is None:
                continue
                
            field_incidents = self._analyze_field_value(
                table_name, field_name, value, row_timestamp, row
            )
            incidents.extend(field_incidents)
            
        # Check for duplicates (only if we have a valid timestamp)
        if row_timestamp is not None:
            duplicate_incident = self._check_duplicate(table_name, row, row_timestamp)
            if duplicate_incident:
                incidents.append(duplicate_incident)
            
        return incidents
    
    def _analyze_field_value(self, table_name: str, field_name: str, value: Any, 
                           timestamp: int, full_row: Dict[str, Any]) -> List[AnomalyIncident]:
        """Analyze a single field value for anomalies."""
        incidents = []
        stats = self.field_stats[table_name][field_name]
        
        # Update field statistics
        # For numeric values, add_numeric_value() handles values but we need to sync timestamps
        # For non-numeric values, manually add to values/timestamps for flatline detection
        if isinstance(value, (int, float, Decimal)):
            numeric_value = float(value)
            # Update statistics using O(1) algorithm
            stats.add_numeric_value(numeric_value)
            # Keep timestamps aligned with values for flatline detection
            stats.timestamps.append(timestamp)
        else:
            # Non-numeric values: only add for flatline detection
            stats.values.append(value)
            stats.timestamps.append(timestamp)
        
        # Check for flatlines
        flatline_incident = self._check_flatline(table_name, field_name, value, timestamp, stats)
        if flatline_incident:
            # Enrich with context and check deduplication
            flatline_incident = self._enrich_incident_with_context(
                flatline_incident, stats, method_used="flatline_detection"
            )
            if self._should_emit_incident(flatline_incident):
                incidents.append(flatline_incident)
            
        # Check for numeric anomalies (numeric values only)
        if isinstance(value, (int, float, Decimal)):
            numeric_value = float(value)  # Convert once for all numeric checks
            
            # Guard against NaN/inf values
            if not (math.isfinite(numeric_value)):
                # Treat non-finite values as outliers
                incident_id = self._generate_incident_id(table_name, field_name, "non_finite_value", timestamp)
                non_finite_incident = AnomalyIncident(
                    incident_id=incident_id,
                    table_name=table_name,
                    field_name=field_name,
                    anomaly_type=AnomalyType.OUTLIER,  # Treat as outlier
                    severity=AnomalySeverity.HIGH,  # Non-finite values are concerning
                    detected_at=int(time.time() * 1_000_000),
                    actual_value=str(numeric_value),  # Store as string for display
                    confidence_score=1.0,
                    evidence={
                        "value_type": "nan" if math.isnan(numeric_value) else "inf",
                        "raw_value": str(numeric_value),
                        "anomaly_subtype": "non_finite"
                    },
                    description=f"Non-finite value detected: {numeric_value}"
                )
                
                # Enrich and check deduplication
                non_finite_incident = self._enrich_incident_with_context(
                    non_finite_incident, stats, method_used="non_finite_detection"
                )
                if self._should_emit_incident(non_finite_incident):
                    incidents.append(non_finite_incident)
                
                # Skip further numeric processing for non-finite values
                return incidents
            
            # Update robust statistics periodically for efficiency
            stats.update_robust_stats_if_needed()
            
            # Spike detection
            spike_incident = self._check_spike(table_name, field_name, numeric_value, timestamp, stats)
            if spike_incident:
                # Incident already enriched in _check_spike, just check deduplication
                if self._should_emit_incident(spike_incident):
                    incidents.append(spike_incident)
                
            # Outlier detection
            outlier_incident = self._check_outlier(table_name, field_name, numeric_value, timestamp, stats)
            if outlier_incident:
                # Incident already enriched in _check_outlier, just check deduplication
                if self._should_emit_incident(outlier_incident):
                    incidents.append(outlier_incident)
                
            # Value drift detection
            drift_incident = self._check_level_shift(table_name, field_name, numeric_value, timestamp, stats)
            if drift_incident:
                # Enrich with context and check deduplication
                drift_incident = self._enrich_incident_with_context(
                    drift_incident, stats, method_used="level_shift_detection"
                )
                if self._should_emit_incident(drift_incident):
                    incidents.append(drift_incident)
                
            # Discontinuity detection (for configured fields)
            if field_name in self.detection_config.discontinuity_fields:
                discontinuity_incident = self._check_discontinuity(
                    table_name, field_name, numeric_value, timestamp, stats
                )
                if discontinuity_incident:
                    # Enrich with context and check deduplication
                    discontinuity_incident = self._enrich_incident_with_context(
                        discontinuity_incident, stats, method_used="discontinuity_detection"
                    )
                    if self._should_emit_incident(discontinuity_incident):
                        incidents.append(discontinuity_incident)
                    
            # Sequence gap detection (for configured sequence fields)
            if field_name in self.detection_config.sequence_fields and isinstance(value, int):
                sequence_gap_incident = self._check_sequence_gap(
                    table_name, field_name, value, timestamp, stats
                )
                if sequence_gap_incident:
                    # Enrich with context and check deduplication
                    sequence_gap_incident = self._enrich_incident_with_context(
                        sequence_gap_incident, stats, method_used="sequence_gap_detection"
                    )
                    if self._should_emit_incident(sequence_gap_incident):
                        incidents.append(sequence_gap_incident)
        
        # Update last seen values
        if isinstance(value, Decimal):
            stats.last_value = float(value)
        else:
            stats.last_value = value
        stats.last_timestamp = timestamp
        
        return incidents
    
    def _check_spike(self, table_name: str, field_name: str, value: float, 
                    timestamp: int, stats: FieldStats) -> Optional[AnomalyIncident]:
        """Check for spike anomalies."""
        if stats.numeric_count < self.detection_config.spike_window_size:
            return None
        
        # Choose statistics method and calculate threshold
        if self.detection_config.use_robust_stats:
            # Ensure robust stats are up to date
            stats.update_robust_stats_if_needed(force=True)
            if stats.mad == 0:  # No variation, can't detect spikes
                return None
            # Use robust z-score: |x - median| / (1.4826 * MAD)
            # 1.4826 makes MAD consistent with std for normal distributions
            robust_z_score = abs(value - stats.median) / (1.4826 * stats.mad)
            threshold_value = robust_z_score
            center_value = stats.median
            scale_value = stats.mad
            method_used = "robust_median_mad"
        else:
            if stats.std == 0:  # No variation, can't detect spikes
                return None
            # Use traditional z-score
            z_score = abs(value - stats.mean) / stats.std
            threshold_value = z_score
            center_value = stats.mean
            scale_value = stats.std
            method_used = "traditional_mean_std"
        
        # Calculate adaptive threshold (beautiful mathematical enhancement)
        base_threshold = self.detection_config.spike_threshold_std
        if (self.detection_config.enable_adaptive_thresholds and 
            stats.numeric_count >= self.detection_config.min_samples_for_adaptation):
            # Adaptive threshold based on data volatility and false positive rate
            # Higher volatility (CV) -> slightly higher threshold to reduce noise
            cv = scale_value / abs(center_value) if abs(center_value) > 1e-10 else 0
            volatility_factor = 1.0 + self.detection_config.adaptation_sensitivity * min(cv, 1.0)
            
            # Estimate false positive rate from recent breach history
            recent_breaches = sum(stats.spike_breach_history) if stats.spike_breach_history else 0
            fp_rate = recent_breaches / len(stats.spike_breach_history) if stats.spike_breach_history else 0
            
            # If FP rate is high, increase threshold slightly
            fp_factor = 1.0 + self.detection_config.adaptation_sensitivity * min(fp_rate, 0.5)
            
            adaptive_threshold = base_threshold * volatility_factor * fp_factor
            method_used += "_adaptive"
        else:
            adaptive_threshold = base_threshold
        
        # Check if this value breaches the adaptive threshold
        is_breach = threshold_value > adaptive_threshold
        
        # Update breach history for debouncing
        stats.spike_breach_history.append(is_breach)
        
        # Check debouncing conditions
        consecutive_breaches = 1  # Default for immediate emission
        if self.detection_config.spike_debounce_count <= 1:
            # No debouncing - emit immediately on breach
            should_emit = is_breach
        else:
            # Check for consecutive breaches
            recent_history = list(stats.spike_breach_history)[-self.detection_config.spike_debounce_window:]
            consecutive_breaches = 0
            for breach in reversed(recent_history):
                if breach:
                    consecutive_breaches += 1
                else:
                    break
            should_emit = consecutive_breaches >= self.detection_config.spike_debounce_count
        
        if should_emit:
            incident_id = self._generate_incident_id(table_name, field_name, "spike", timestamp)
            
            incident = AnomalyIncident(
                incident_id=incident_id,
                table_name=table_name,
                field_name=field_name,
                anomaly_type=AnomalyType.SPIKE,
                severity=self._calculate_spike_severity(threshold_value),
                detected_at=int(time.time() * 1_000_000),
                expected_value=center_value,
                actual_value=value,
                deviation_magnitude=threshold_value,
                confidence_score=min(threshold_value / 10.0, 1.0),
                evidence={
                    "threshold_value": threshold_value,
                    "adaptive_threshold": adaptive_threshold,
                    "base_threshold": base_threshold,
                    "center": center_value,
                    "scale": scale_value,
                    "method": method_used,
                    "window_size": len(stats.numeric_values),
                    "numeric_count": stats.numeric_count,
                    "debounce_count": self.detection_config.spike_debounce_count,
                    "consecutive_breaches": consecutive_breaches,
                    "adaptive_enabled": self.detection_config.enable_adaptive_thresholds
                },
                description=f"Spike detected: value {value} deviates {threshold_value:.2f} {method_used.split('_')[0]} deviations from center {center_value:.2f}"
            )
            
            # Enrich with consistent context
            return self._enrich_incident_with_context(
                incident, stats, threshold_value, method_used, self.detection_config.spike_window_size
            )
        
        return None
    
    def _check_flatline(self, table_name: str, field_name: str, value: Any, 
                       timestamp: int, stats: FieldStats) -> Optional[AnomalyIncident]:
        """Check for flatline anomalies."""
        if stats.last_value is None:
            stats.consecutive_identical = 1
            return None
            
        # Check if values are identical (with tolerance for floats)
        if self._values_identical(value, stats.last_value):
            stats.consecutive_identical += 1
        else:
            stats.consecutive_identical = 1
            
        if stats.consecutive_identical >= self.detection_config.flatline_min_duration:
            incident_id = self._generate_incident_id(table_name, field_name, "flatline", timestamp)
            
            # Calculate correct start index for the flatline duration
            start_index = max(0, len(stats.timestamps) - stats.consecutive_identical)
            duration_seconds = (timestamp - stats.timestamps[start_index]) / 1_000_000
            
            incident = AnomalyIncident(
                incident_id=incident_id,
                table_name=table_name,
                field_name=field_name,
                anomaly_type=AnomalyType.FLATLINE,
                severity=self._calculate_flatline_severity(stats.consecutive_identical),
                detected_at=int(time.time() * 1_000_000),
                window_start=stats.timestamps[start_index],
                window_end=timestamp,
                actual_value=value,
                confidence_score=min(stats.consecutive_identical / 50.0, 1.0),
                evidence={
                    "consecutive_count": stats.consecutive_identical,
                    "value": value,
                    "duration_seconds": duration_seconds,
                    "start_index": start_index,
                    "tolerance_used": {
                        "absolute": self.detection_config.flatline_tolerance,
                        "relative": self.detection_config.flatline_relative_tolerance
                    }
                },
                affected_rows=stats.consecutive_identical,
                description=f"Value {value} repeated {stats.consecutive_identical} consecutive times over {duration_seconds:.1f}s"
            )
            
            # Enrich with context and check deduplication
            incident = self._enrich_incident_with_context(
                incident, stats, stats.consecutive_identical, "flatline_detection", 
                self.detection_config.flatline_min_duration
            )
            if self._should_emit_incident(incident):
                return incident
        
        return None
    
    def _check_outlier(self, table_name: str, field_name: str, value: float, 
                      timestamp: int, stats: FieldStats) -> Optional[AnomalyIncident]:
        """Check for outlier anomalies (less strict than spikes)."""
        if stats.numeric_count < self.detection_config.outlier_window_size:
            return None
        
        # Choose statistics method
        if self.detection_config.use_robust_stats:
            # Ensure robust stats are up to date
            stats.update_robust_stats_if_needed(force=True)
            if stats.mad == 0:  # No variation, can't detect outliers
                return None
            # Use robust z-score
            threshold_value = abs(value - stats.median) / (1.4826 * stats.mad)
            center_value = stats.median
            scale_value = stats.mad
            method_used = "robust_median_mad"
        else:
            if stats.std == 0:
                return None
            # Use traditional z-score
            threshold_value = abs(value - stats.mean) / stats.std
            center_value = stats.mean
            scale_value = stats.std
            method_used = "traditional_mean_std"
        
        # Check if this value breaches the threshold
        is_breach = threshold_value > self.detection_config.outlier_threshold_std
        
        # Update breach history for debouncing
        stats.outlier_breach_history.append(is_breach)
        
        # Check debouncing conditions
        consecutive_breaches = 1  # Default for immediate emission
        if self.detection_config.outlier_debounce_count <= 1:
            # No debouncing - emit immediately on breach
            should_emit = is_breach
        else:
            # Check for consecutive breaches
            recent_history = list(stats.outlier_breach_history)[-self.detection_config.outlier_debounce_window:]
            consecutive_breaches = 0
            for breach in reversed(recent_history):
                if breach:
                    consecutive_breaches += 1
                else:
                    break
            should_emit = consecutive_breaches >= self.detection_config.outlier_debounce_count
        
        if should_emit:
            incident_id = self._generate_incident_id(table_name, field_name, "outlier", timestamp)
            
            incident = AnomalyIncident(
                incident_id=incident_id,
                table_name=table_name,
                field_name=field_name,
                anomaly_type=AnomalyType.OUTLIER,
                severity=AnomalySeverity.LOW,  # Outliers are typically low severity
                detected_at=int(time.time() * 1_000_000),
                expected_value=center_value,
                actual_value=value,
                deviation_magnitude=threshold_value,
                confidence_score=min(threshold_value / 5.0, 1.0),
                evidence={
                    "threshold_value": threshold_value,
                    "center": center_value,
                    "scale": scale_value,
                    "method": method_used,
                    "numeric_count": stats.numeric_count,
                    "debounce_count": self.detection_config.outlier_debounce_count,
                    "consecutive_breaches": consecutive_breaches
                },
                description=f"Outlier detected: value {value} (threshold: {threshold_value:.2f} using {method_used.split('_')[0]} method)"
            )
            
            # Enrich with consistent context
            return self._enrich_incident_with_context(
                incident, stats, threshold_value, method_used, self.detection_config.outlier_window_size
            )
        
        return None
    
    def _check_discontinuity(self, table_name: str, field_name: str, value: float, 
                           timestamp: int, stats: FieldStats) -> Optional[AnomalyIncident]:
        """Check for discontinuity anomalies in time series data."""
        if (stats.last_value is None or stats.last_timestamp is None or 
            not isinstance(stats.last_value, (int, float, Decimal))):
            return None
            
        last_numeric = float(stats.last_value)
        time_gap = (timestamp - stats.last_timestamp) / 1_000_000  # seconds
        
        if time_gap < self.detection_config.discontinuity_min_gap:
            return None
            
        # Calculate rate of change
        value_change = abs(value - last_numeric)
        rate_of_change = value_change / time_gap
        
        # Check if rate exceeds threshold
        if rate_of_change > self.detection_config.discontinuity_threshold:
            incident_id = self._generate_incident_id(table_name, field_name, "discontinuity", timestamp)
            
            incident = AnomalyIncident(
                incident_id=incident_id,
                table_name=table_name,
                field_name=field_name,
                anomaly_type=AnomalyType.DISCONTINUITY,
                severity=self._calculate_discontinuity_severity(rate_of_change),
                detected_at=int(time.time() * 1_000_000),
                window_start=stats.last_timestamp,
                window_end=timestamp,
                expected_value=last_numeric,
                actual_value=value,
                deviation_magnitude=rate_of_change,
                confidence_score=min(rate_of_change / 100.0, 1.0),
                evidence={
                    "rate_of_change": rate_of_change,
                    "time_gap_seconds": time_gap,
                    "value_change": value_change,
                    "previous_value": last_numeric
                },
                description=f"Discontinuity: value changed from {last_numeric} to {value} in {time_gap:.1f}s (rate: {rate_of_change:.2f})"
            )
            
            # Enrich with context and check deduplication
            incident = self._enrich_incident_with_context(
                incident, stats, rate_of_change, "discontinuity_detection",
                None  # No specific window size for discontinuity detection
            )
            if self._should_emit_incident(incident):
                return incident
        
        return None
    
    def _check_level_shift(self, table_name: str, field_name: str, value: float, 
                          timestamp: int, stats: FieldStats) -> Optional[AnomalyIncident]:
        """Check for level shift anomalies by comparing older vs newer windows using robust statistics."""
        if stats.numeric_count < self.detection_config.level_shift_window_size:
            return None
            
        # Get numeric values for drift analysis - clip to configured window size
        all_numeric_values = list(stats.numeric_values)
        window_size = self.detection_config.level_shift_window_size
        
        # Use only the last N samples as configured
        if len(all_numeric_values) > window_size:
            numeric_values = all_numeric_values[-window_size:]
        else:
            numeric_values = all_numeric_values
        
        if len(numeric_values) < window_size:
            return None
            
        # Split into older and newer halves
        half_size = len(numeric_values) // 2
        if half_size < 5:  # Need minimum samples in each half
            return None
            
        older_half = numeric_values[:half_size]
        newer_half = numeric_values[half_size:]
        
        # Use robust statistics (medians) instead of means for level shift detection
        older_median = float(np.median(older_half))
        newer_median = float(np.median(newer_half))
        
        # Safe fallback when MAD ≈ 0 (flat baseline) - use explicit thresholds
        older_mad = float(np.median(np.abs(np.array(older_half) - older_median)))
        if older_mad < 1e-10:
            # Flat baseline: use explicit absolute and relative thresholds from config
            abs_change = abs(newer_median - older_median)
            
            # Check absolute threshold first
            absolute_breach = abs_change > self.detection_config.level_shift_absolute_epsilon
            
            # Check relative threshold if baseline is non-zero
            relative_breach = False
            if abs(older_median) > 1e-10:
                relative_change = abs_change / abs(older_median)
                relative_breach = relative_change > self.detection_config.level_shift_relative_epsilon
            
            # Trigger if either threshold is breached
            if absolute_breach or relative_breach:
                # Normalize to comparable scale for severity calculation
                level_shift = abs_change / self.detection_config.level_shift_absolute_epsilon
            else:
                level_shift = 0.0  # No breach
        else:
            # Standard robust z-score calculation
            level_shift = abs(newer_median - older_median) / (1.4826 * older_mad)
        
        if level_shift > self.detection_config.level_shift_threshold_z:
            incident_id = self._generate_incident_id(table_name, field_name, "level_shift", timestamp)
            
            incident = AnomalyIncident(
                incident_id=incident_id,
                table_name=table_name,
                field_name=field_name,
                anomaly_type=AnomalyType.LEVEL_SHIFT,
                severity=self._calculate_level_shift_severity(level_shift),
                detected_at=int(time.time() * 1_000_000),
                expected_value=older_median,
                actual_value=newer_median,
                deviation_magnitude=level_shift,
                confidence_score=min(float(level_shift) / 0.5, 1.0),
                evidence={
                    "older_median": older_median,
                    "newer_median": newer_median,
                    "level_shift": level_shift,
                    "older_mad": older_mad,
                    "window_size": len(numeric_values),
                    "half_size": half_size
                },
                description=f"Level shift detected: {level_shift:.2f} MAD shift from {older_median:.4f} to {newer_median:.4f}"
            )
            
            # Enrich with context and check deduplication
            incident = self._enrich_incident_with_context(
                incident, stats, level_shift, "level_shift_detection", 
                self.detection_config.level_shift_window_size
            )
            if self._should_emit_incident(incident):
                return incident
        
        return None
    
    def _check_sequence_gap(self, table_name: str, field_name: str, value: int, 
                           timestamp: int, stats: FieldStats) -> Optional[AnomalyIncident]:
        """Check for sequence gap anomalies in strictly increasing integer fields."""
        if (stats.last_value is None or 
            not isinstance(stats.last_value, (int, float)) or 
            stats.last_timestamp is None):
            return None
            
        last_int = int(stats.last_value)
        
        # Check for regression (value decreased)
        if value < last_int:
            regression_size = last_int - value
            incident_id = self._generate_incident_id(table_name, field_name, "sequence_regression", timestamp)
            
            incident = AnomalyIncident(
                incident_id=incident_id,
                table_name=table_name,
                field_name=field_name,
                anomaly_type=AnomalyType.SEQUENCE_GAP,  # Reuse SEQUENCE_GAP type
                severity=self._calculate_sequence_gap_severity(regression_size),
                detected_at=int(time.time() * 1_000_000),
                expected_value=last_int + 1,  # Expected to increase
                actual_value=value,
                deviation_magnitude=regression_size,
                confidence_score=1.0,  # High confidence for sequence regressions
                evidence={
                    "previous_value": last_int,
                    "expected_value": last_int + 1,
                    "actual_value": value,
                    "gap_size": -regression_size,  # Negative gap indicates regression
                    "anomaly_subtype": "regression",  # For easier triage
                    "regression_type": "sequence_decreased"
                },
                description=f"Sequence regression: expected ≥{last_int + 1}, got {value} (decreased by {regression_size})"
            )
            
            # Enrich with context and check deduplication
            incident = self._enrich_incident_with_context(
                incident, stats, regression_size, "sequence_gap_detection", 
                None  # No specific window size for sequence detection
            )
            if self._should_emit_incident(incident):
                return incident
        
        # Check for gap (should be strictly increasing by 1)
        expected_next = last_int + 1
        if value > expected_next:
            gap_size = value - expected_next
            incident_id = self._generate_incident_id(table_name, field_name, "sequence_gap", timestamp)
            
            incident = AnomalyIncident(
                incident_id=incident_id,
                table_name=table_name,
                field_name=field_name,
                anomaly_type=AnomalyType.SEQUENCE_GAP,
                severity=self._calculate_sequence_gap_severity(gap_size),
                detected_at=int(time.time() * 1_000_000),
                expected_value=expected_next,
                actual_value=value,
                deviation_magnitude=gap_size,
                confidence_score=1.0,  # High confidence for sequence gaps
                evidence={
                    "previous_value": last_int,
                    "expected_value": expected_next,
                    "actual_value": value,
                    "gap_size": gap_size,
                    "anomaly_subtype": "gap"  # For easier triage
                },
                description=f"Sequence gap: expected {expected_next}, got {value} (gap of {gap_size})"
            )
            
            # Enrich with context and check deduplication
            incident = self._enrich_incident_with_context(
                incident, stats, gap_size, "sequence_gap_detection",
                None  # No specific window size for sequence detection
            )
            if self._should_emit_incident(incident):
                return incident
        
        return None
    
    def _check_duplicate(self, table_name: str, row: Dict[str, Any], 
                        timestamp: int) -> Optional[AnomalyIncident]:
        """Check for duplicate rows."""
        # Generate hash for the row
        row_hash = self._generate_row_hash(row)
        
        # Check if we've seen this hash recently
        if row_hash in self.duplicate_hashes[table_name]:
            last_seen = self.duplicate_hashes[table_name][row_hash]
            time_diff = (timestamp - last_seen) / 1_000_000  # seconds
            
            # If seen recently, it's a duplicate
            if time_diff < self.detection_config.duplicate_window_seconds:
                incident_id = self._generate_incident_id(table_name, "row", "duplicate", timestamp)
                
                incident = AnomalyIncident(
                    incident_id=incident_id,
                    table_name=table_name,
                    field_name=None,
                    anomaly_type=AnomalyType.DUPLICATE,
                    severity=AnomalySeverity.MEDIUM,
                    detected_at=int(time.time() * 1_000_000),
                    confidence_score=1.0,  # High confidence for exact duplicates
                    evidence={
                        "row_hash": row_hash,
                        "last_seen_timestamp": last_seen,
                        "time_diff_seconds": time_diff,
                        "duplicate_row": row,
                        # Add consistent context for batch-level detection
                        "window_size_used": self.detection_config.duplicate_window_seconds,
                        "estimator_type": "duplicate_detection",
                        "numeric_sample_count": 1,
                        "threshold_used": self.detection_config.duplicate_window_seconds
                    },
                    description=f"Duplicate row detected (seen {time_diff:.1f}s ago)"
                )
                
                # Check if we should emit this incident (deduplication)
                if self._should_emit_incident(incident):
                    return incident
        
        # Update hash tracking
        self.duplicate_hashes[table_name][row_hash] = timestamp
        
        # Clean old hashes to prevent memory growth
        self._cleanup_duplicate_hashes(table_name, timestamp)
        
        return None
    
    def _detect_batch_anomalies(self, table_name: str, rows: List[Dict[str, Any]], 
                              timestamp_field: str) -> List[AnomalyIncident]:
        """Detect batch-level anomalies."""
        incidents = []
        
        if len(rows) < 2:
            return incidents
            
        # Check for missing data gaps (including cross-batch gaps)
        missing_data_incidents = self._check_missing_data_gaps(table_name, rows, timestamp_field)
        incidents.extend(missing_data_incidents)
        
        return incidents
    
    def _update_cross_batch_tracking(self, table_name: str, rows: List[Dict[str, Any]], 
                                   timestamp_field: str) -> None:
        """Update cross-batch tracking for missing data detection."""
        if not rows:
            return
            
        # Find the latest timestamp in this batch
        latest_timestamp = None
        for row in reversed(rows):  # Check from end for efficiency
            ts = self._extract_timestamp(row, timestamp_field)
            if ts is not None:
                latest_timestamp = ts
                break
                
        if latest_timestamp is not None:
            self.last_timestamp_by_table[table_name] = latest_timestamp
    
    def _check_missing_data_gaps(self, table_name: str, rows: List[Dict[str, Any]], 
                               timestamp_field: str) -> List[AnomalyIncident]:
        """Check for missing data gaps in time series."""
        incidents = []
        
        if self.detection_config.expected_interval_seconds is None:
            return incidents
            
        timestamps = []
        for row in rows:
            ts = self._extract_timestamp(row, timestamp_field)
            if ts is not None:
                timestamps.append(ts)
                
        timestamps.sort()
        
        expected_interval_us = self.detection_config.expected_interval_seconds * 1_000_000
        max_gap_us = expected_interval_us * self.detection_config.missing_data_tolerance
        
        # Check for cross-batch gap (between last batch and current batch)
        if (table_name in self.last_timestamp_by_table and 
            timestamps and 
            self.last_timestamp_by_table[table_name] > 0):
            
            cross_batch_gap = timestamps[0] - self.last_timestamp_by_table[table_name]
            
            if cross_batch_gap > max_gap_us:
                incident_id = self._generate_incident_id(table_name, timestamp_field, "missing_data_cross_batch", timestamps[0])
                
                incident = AnomalyIncident(
                    incident_id=incident_id,
                    table_name=table_name,
                    field_name=timestamp_field,
                    anomaly_type=AnomalyType.MISSING_DATA,
                    severity=self._calculate_missing_data_severity(cross_batch_gap, expected_interval_us),
                    detected_at=int(time.time() * 1_000_000),
                    window_start=self.last_timestamp_by_table[table_name],
                    window_end=timestamps[0],
                    expected_value=expected_interval_us / 1_000_000,
                    actual_value=cross_batch_gap / 1_000_000,
                    deviation_magnitude=cross_batch_gap / expected_interval_us,
                    confidence_score=min(cross_batch_gap / (expected_interval_us * 10), 1.0),
                    evidence={
                        "gap_seconds": cross_batch_gap / 1_000_000,
                        "expected_interval_seconds": expected_interval_us / 1_000_000,
                        "gap_multiplier": cross_batch_gap / expected_interval_us,
                        "gap_type": "cross_batch",
                        # Add consistent context for batch-level detection
                        "window_size_used": len(timestamps),
                        "estimator_type": "missing_data_detection",
                        "numeric_sample_count": len(timestamps),
                        "threshold_used": self.detection_config.missing_data_tolerance
                    },
                    description=f"Cross-batch missing data gap: {cross_batch_gap / 1_000_000:.1f}s (expected: {expected_interval_us / 1_000_000:.1f}s)"
                )
                
                # Check if we should emit this incident (deduplication)
                if self._should_emit_incident(incident):
                    incidents.append(incident)
        
        # Check for intra-batch gaps
        for i in range(1, len(timestamps)):
            gap = timestamps[i] - timestamps[i-1]
            
            if gap > max_gap_us:
                incident_id = self._generate_incident_id(table_name, timestamp_field, "missing_data", timestamps[i])
                
                incident = AnomalyIncident(
                    incident_id=incident_id,
                    table_name=table_name,
                    field_name=timestamp_field,
                    anomaly_type=AnomalyType.MISSING_DATA,
                    severity=self._calculate_missing_data_severity(gap, expected_interval_us),
                    detected_at=int(time.time() * 1_000_000),
                    window_start=timestamps[i-1],
                    window_end=timestamps[i],
                    expected_value=expected_interval_us / 1_000_000,
                    actual_value=gap / 1_000_000,
                    deviation_magnitude=gap / expected_interval_us,
                    confidence_score=min(gap / (expected_interval_us * 10), 1.0),
                    evidence={
                        "gap_seconds": gap / 1_000_000,
                        "expected_interval_seconds": expected_interval_us / 1_000_000,
                        "gap_multiplier": gap / expected_interval_us,
                        "gap_type": "intra_batch",
                        # Add consistent context for batch-level detection
                        "window_size_used": len(timestamps),
                        "estimator_type": "missing_data_detection", 
                        "numeric_sample_count": len(timestamps),
                        "threshold_used": self.detection_config.missing_data_tolerance
                    },
                    description=f"Missing data gap: {gap / 1_000_000:.1f}s (expected: {expected_interval_us / 1_000_000:.1f}s)"
                )
                
                # Check if we should emit this incident (deduplication)
                if self._should_emit_incident(incident):
                    incidents.append(incident)
        
        return incidents
    
    # Helper methods
    
    def _sort_rows_by_timestamp(self, rows: List[Dict[str, Any]], 
                               timestamp_field: str) -> List[Dict[str, Any]]:
        """Sort rows by timestamp field."""
        try:
            def get_timestamp_for_sort(row):
                ts = self._extract_timestamp(row, timestamp_field, 0)
                return ts if ts is not None else 0
            
            return sorted(rows, key=get_timestamp_for_sort)
        except (KeyError, TypeError, ValueError):
            return rows  # Return unsorted if sorting fails
    
    def _extract_timestamp(self, row: Dict[str, Any], timestamp_field: str, 
                          default: Optional[int] = None) -> Optional[int]:
        """Extract timestamp from row as integer microseconds for precision."""
        try:
            value = row.get(timestamp_field, default)
            if value is None:
                return default
            # Parse as float first for flexibility, then convert to int microseconds
            timestamp_float = float(value)
            # Ensure we have microseconds (convert if needed)
            if timestamp_float < 1e12:  # Likely seconds, convert to microseconds
                timestamp_float *= 1_000_000
            return int(timestamp_float)
        except (ValueError, TypeError):
            return default
    
    def _values_identical(self, val1: Any, val2: Any) -> bool:
        """Check if two values are identical (with tolerance for floats)."""
        # Handle numeric types uniformly (int, float, Decimal)
        val1_is_numeric = isinstance(val1, (int, float, Decimal))
        val2_is_numeric = isinstance(val2, (int, float, Decimal))
        
        if val1_is_numeric and val2_is_numeric:
            # Both numeric: use tolerance-based comparison (treats 1 and 1.0 as identical)
            abs_diff = abs(float(val1) - float(val2))
            tolerance = (self.detection_config.flatline_tolerance + 
                        self.detection_config.flatline_relative_tolerance * abs(float(val2)))
            return abs_diff <= tolerance
        elif val1_is_numeric or val2_is_numeric:
            # One numeric, one not: definitely different
            return False
        else:
            # Both non-numeric: exact equality
            return val1 == val2
    
    def _generate_row_hash(self, row: Dict[str, Any]) -> str:
        """Generate a stable hash for a row using consistent serialization."""
        import json
        
        # Use key fields if specified, otherwise use all fields except timestamp by default
        if self.detection_config.duplicate_key_fields:
            fields_to_hash = self.detection_config.duplicate_key_fields
        elif self.detection_config.exclude_timestamp_from_duplicates:
            # Exclude common timestamp field names from hashing
            timestamp_fields = {'timestamp_utc_us', 'timestamp', 'created_at', 'updated_at', 'time'}
            fields_to_hash = [f for f in sorted(row.keys()) if f not in timestamp_fields]
        else:
            fields_to_hash = sorted(row.keys())
        
        # Build stable dictionary for hashing
        hash_dict = {}
        for field in fields_to_hash:
            if field in row:
                value = row[field]
                # Convert to stable string representation
                if isinstance(value, float):
                    # Use consistent float precision to avoid tiny differences
                    hash_dict[field] = f"{value:.10g}"
                elif isinstance(value, (dict, list)):
                    # For complex objects, use JSON with sorted keys
                    hash_dict[field] = json.dumps(value, sort_keys=True, default=str)
                else:
                    hash_dict[field] = str(value)
        
        # Use JSON with sorted keys for deterministic serialization
        hash_string = json.dumps(hash_dict, sort_keys=True)
        return hashlib.md5(hash_string.encode()).hexdigest()
    
    def _generate_incident_id(self, table_name: str, field_name: str, 
                             anomaly_type: str, timestamp: int) -> str:
        """Generate a unique incident ID."""
        data = f"{table_name}:{field_name}:{anomaly_type}:{timestamp}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]
    
    def _cleanup_duplicate_hashes(self, table_name: str, current_timestamp: int):
        """Clean up old duplicate hashes to prevent memory growth."""
        cutoff_time = current_timestamp - (self.detection_config.duplicate_window_seconds * 1_000_000)
        
        hashes_to_remove = []
        for row_hash, timestamp in self.duplicate_hashes[table_name].items():
            if timestamp < cutoff_time:
                hashes_to_remove.append(row_hash)
        
        for row_hash in hashes_to_remove:
            del self.duplicate_hashes[table_name][row_hash]
    
    # Severity calculation methods
    
    def _calculate_spike_severity(self, z_score: float) -> AnomalySeverity:
        """Calculate severity for spike anomalies."""
        if z_score > 10.0:
            return AnomalySeverity.CRITICAL
        elif z_score > 6.0:
            return AnomalySeverity.HIGH
        elif z_score > 4.0:
            return AnomalySeverity.MEDIUM
        else:
            return AnomalySeverity.LOW
    
    def _calculate_flatline_severity(self, consecutive_count: int) -> AnomalySeverity:
        """Calculate severity for flatline anomalies."""
        if consecutive_count > 100:
            return AnomalySeverity.CRITICAL
        elif consecutive_count > 50:
            return AnomalySeverity.HIGH
        elif consecutive_count > 25:
            return AnomalySeverity.MEDIUM
        else:
            return AnomalySeverity.LOW
    
    def _calculate_discontinuity_severity(self, rate_of_change: float) -> AnomalySeverity:
        """Calculate severity for discontinuity anomalies."""
        if rate_of_change > 100.0:
            return AnomalySeverity.CRITICAL
        elif rate_of_change > 50.0:
            return AnomalySeverity.HIGH
        elif rate_of_change > 20.0:
            return AnomalySeverity.MEDIUM
        else:
            return AnomalySeverity.LOW
    
    def _calculate_missing_data_severity(self, actual_gap: float, expected_gap: float) -> AnomalySeverity:
        """Calculate severity for missing data anomalies."""
        multiplier = actual_gap / expected_gap
        
        if multiplier > 20.0:
            return AnomalySeverity.CRITICAL
        elif multiplier > 10.0:
            return AnomalySeverity.HIGH
        elif multiplier > 5.0:
            return AnomalySeverity.MEDIUM
        else:
            return AnomalySeverity.LOW
    
    def _calculate_level_shift_severity(self, z_score: float) -> AnomalySeverity:
        """Calculate severity for level shift anomalies based on z-score magnitude."""
        if z_score > 10:  # Very extreme shift
            return AnomalySeverity.CRITICAL
        elif z_score > 6:  # Strong shift
            return AnomalySeverity.HIGH
        elif z_score > 3:  # Clear shift
            return AnomalySeverity.MEDIUM
        else:
            return AnomalySeverity.LOW
    
    def _calculate_sequence_gap_severity(self, gap_size: int) -> AnomalySeverity:
        """Calculate severity for sequence gap anomalies."""
        if gap_size > 100:
            return AnomalySeverity.CRITICAL
        elif gap_size > 10:
            return AnomalySeverity.HIGH
        elif gap_size > 1:
            return AnomalySeverity.MEDIUM
        else:
            return AnomalySeverity.LOW
    
    # Performance and monitoring methods
    
    def get_detection_stats(self) -> Dict[str, Any]:
        """Get detection performance statistics."""
        return {
            "total_detections": self.detections_count,
            "tables_monitored": len(self.field_stats),
            "total_fields_tracked": sum(len(fields) for fields in self.field_stats.values()),
            "duplicate_hash_count": sum(len(hashes) for hashes in self.duplicate_hashes.values()),
            "incident_dedupe_cache_size": len(self.incident_dedupe_cache)
        }
    
    def reset_stats(self):
        """Reset detection statistics."""
        self.detections_count = 0
        logger.info("Anomaly detection statistics reset")


# Example usage and demo
if __name__ == "__main__":
    import asyncio
    
    async def demo_anomaly_detection():
        """Demonstrate the anomaly detector functionality."""
        print("=== Data Anomaly Detector Demo ===\n")
        
        # Configure detector
        config = {
            "detection_params": {
                "spike_threshold_std": 3.0,
                "flatline_min_duration": 5,
                "duplicate_window_seconds": 300,  # 5 minutes
                "exclude_timestamp_from_duplicates": True,
                "expected_interval_seconds": 1.0,  # 1 second expected interval
                "discontinuity_threshold": 10.0,
                "discontinuity_fields": ["price", "volume"],  # Configure specific fields for discontinuity
                "sequence_fields": ["block_number", "sequence_id"],  # Configure sequence fields
                "level_shift_threshold_z": 3.0,  # Z-score threshold for level shift detection
                "level_shift_window_size": 20,  # Smaller window for demo
                "level_shift_absolute_epsilon": 0.01,  # Explicit absolute threshold when MAD≈0
                "level_shift_relative_epsilon": 0.1   # Explicit relative threshold when MAD≈0 (10%)
            }
        }
        
        detector = DataAnomalyDetector(config)
        
        # Test data with various anomalies
        print("1. Testing Spike Detection:")
        spike_data = [
            {"timestamp_utc_us": 1000000 * i, "price": 100 + i * 0.1} for i in range(50)
        ]
        spike_data.append({"timestamp_utc_us": 1000000 * 50, "price": 500})  # Spike
        
        incidents = detector.analyze_batch("test_table", spike_data)
        print(f"   Detected {len(incidents)} incidents")
        for incident in incidents:
            print(f"   - {incident.anomaly_type.value}: {incident.description}")
        
        print("\n2. Testing Flatline Detection:")
        flatline_data = [
            {"timestamp_utc_us": 1000000 * (100 + i), "volume": 1000} for i in range(10)
        ]
        
        incidents = detector.analyze_batch("test_table", flatline_data)
        print(f"   Detected {len(incidents)} incidents")
        for incident in incidents:
            print(f"   - {incident.anomaly_type.value}: {incident.description}")
        
        print("\n3. Testing Duplicate Detection:")
        duplicate_data = [
            {"timestamp_utc_us": 1000000 * 200, "id": "test123", "value": 42},
            {"timestamp_utc_us": 1000000 * 201, "id": "test123", "value": 42},  # Duplicate (timestamp excluded)
        ]
        
        incidents = detector.analyze_batch("test_table", duplicate_data)
        print(f"   Detected {len(incidents)} incidents")
        for incident in incidents:
            print(f"   - {incident.anomaly_type.value}: {incident.description}")
        
        print("\n4. Testing Value Drift Detection:")
        # Create data with clear drift: starts around 100, then jumps to around 150
        drift_data = []
        for i in range(30):
            if i < 15:
                value = 100 + (i * 0.1)  # Stable around 100
            else:
                value = 150 + ((i-15) * 0.1)  # Jump to 150
            drift_data.append({"timestamp_utc_us": 1000000 * (300 + i), "price": value})
        
        incidents = detector.analyze_batch("test_drift", drift_data)
        print(f"   Detected {len(incidents)} incidents")
        for incident in incidents:
            if incident.anomaly_type.value == "level_shift":
                print(f"   - LEVEL_SHIFT: {incident.description}")
        
        print("\n5. Testing Sequence Gap Detection:")
        sequence_data = [
            {"timestamp_utc_us": 1000000 * 400, "block_number": 1000},
            {"timestamp_utc_us": 1000000 * 401, "block_number": 1001},
            {"timestamp_utc_us": 1000000 * 402, "block_number": 1005},  # Gap: missing 1002, 1003, 1004
        ]
        
        incidents = detector.analyze_batch("test_sequence", sequence_data)
        print(f"   Detected {len(incidents)} incidents")
        for incident in incidents:
            if incident.anomaly_type.value == "sequence_gap":
                print(f"   - SEQUENCE_GAP: {incident.description}")
        
        print("\n6. Performance Statistics:")
        stats = detector.get_detection_stats()
        for key, value in stats.items():
            print(f"   {key}: {value}")
        
        print("\n=== Demo Complete ===")
    
    # Run demo
    asyncio.run(demo_anomaly_detection())
