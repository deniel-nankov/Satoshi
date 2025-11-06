"""
Schema Validator Agent

Mission: Enforce data contracts (types, nulls, ranges, referential integrity).
Outputs: incidents.SchemaViolation + clean pass/fail summary.
Do/Don't: Do coerce within explicit rules; don't silently reshape.
"""

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Union, Set, Callable, Tuple, Pattern
from decimal import Decimal, ROUND_HALF_EVEN
from enum import Enum
import re
from datetime import datetime, timezone
import difflib
import hashlib
import statistics

# Streaming Bus Integration
from infra.bus.streaming_bus import StreamingBus

# Import centralized Prometheus metrics
try:
    from infra.monitoring.prometheus_metrics import MetricsCollector
    _metrics_collector = MetricsCollector()
    METRICS_AVAILABLE = True
except ImportError:
    _metrics_collector = None
    METRICS_AVAILABLE = False

logger = logging.getLogger(__name__)


class ValidationResult(Enum):
    PASS = "pass"
    FAIL = "fail"
    COERCED = "coerced"


class FieldType(Enum):
    STRING = "string"
    INTEGER = "integer" 
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    TIMESTAMP_US = "timestamp_us"
    ADDRESS = "address"
    HASH = "hash"
    ENUM = "enum"
    LIST = "list"
    DICT = "dict"


@dataclass
class CoercionRule:
    """Explicit rules for data coercion."""
    from_type: type
    to_type: type
    coercer: Callable[[Any], Any]
    description: str


@dataclass
class CrossFieldRule:
    """Rules for validation across multiple fields."""
    rule_type: str  # "conditional_required", "mutual_exclusive", "field_relationship", "temporal_ordering"
    fields: List[str]
    condition: Optional[Callable[[Dict[str, Any]], bool]] = None
    error_message: str = ""
    severity: str = "error"


@dataclass
class FieldSchema:
    """Schema definition for a single field."""
    name: str
    field_type: FieldType
    required: bool = True
    nullable: bool = False
    min_value: Optional[Union[int, Decimal]] = None
    max_value: Optional[Union[int, Decimal]] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None  # regex pattern
    enum_values: Optional[Set[str]] = None
    list_item_type: Optional[FieldType] = None
    dict_value_type: Optional[FieldType] = None
    dict_key_pattern: Optional[str] = None
    coercion_rules: List[CoercionRule] = field(default_factory=list)
    custom_validator: Optional[Callable[[Any], bool]] = None
    allow_enum_case_insensitive: bool = False
    trim_whitespace: bool = False
    decimal_scale: Optional[int] = None  # For decimal precision control
    
    # Enhanced validation rules
    business_hours_only: bool = False  # Validate timestamps are in business hours
    checksum_algorithm: Optional[str] = None  # "luhn", "iban", "isbn"
    semantic_validator: Optional[str] = None  # "email", "url", "phone", "country_code", "currency_code"
    
    # Compiled patterns (set during __post_init__)
    _compiled_pattern: Optional[Pattern[str]] = field(default=None, init=False, repr=False)
    _compiled_dict_key_pattern: Optional[Pattern[str]] = field(default=None, init=False, repr=False)
    
    def __post_init__(self):
        # Precompile regex patterns for performance
        if self.pattern:
            self._compiled_pattern = re.compile(self.pattern)
        
        if self.dict_key_pattern:
            self._compiled_dict_key_pattern = re.compile(self.dict_key_pattern)


@dataclass
class TableSchema:
    """Schema definition for a table/stream."""
    name: str
    fields: List[FieldSchema]
    primary_key: Optional[List[str]] = None
    foreign_keys: Dict[str, str] = field(default_factory=dict)  # field -> ref_table.ref_field
    unique_constraints: List[List[str]] = field(default_factory=list)
    unexpected_field_severity: str = "warning"  # "warning" | "error" | "ignore"
    strict_foreign_keys: bool = True  # Make missing reference data an error
    allow_extra_fields: bool = True  # Whether to allow unexpected fields
    drop_extra_fields: bool = False  # Whether to log dropping (but never actually drop)
    cross_field_rules: List[CrossFieldRule] = field(default_factory=list)
    
    # Batch-level constraints for complete data partitions
    batch_constraints: List[str] = field(default_factory=list)  
    # Supported constraints:
    # "unique_in_batch" - Handled automatically by unique_constraints 
    # "sum_equals_zero" - Amount/balance fields must sum to zero (double-entry bookkeeping)
    # "sequence_continuous" - ID/sequence fields must be continuous with no gaps
    # "monotonic_increasing" - Time/timestamp fields must be in ascending order
    
    # Batch partition control
    is_complete_partition: bool = False  # Only run aggregate checks when True to avoid FPs on partial batches
    
    def __post_init__(self):
        self.field_map = {f.name: f for f in self.fields}


@dataclass
class SchemaViolation:
    """Represents a schema validation violation."""
    table_name: str
    field_name: Optional[str]
    violation_type: str
    expected: str
    actual: str
    row_identifier: str
    severity: str = "error"  # error, warning
    coerced_value: Optional[Any] = None
    expected_hint: Optional[str] = None  # Suggestion for human-readable errors
    reference_info: Optional[str] = None  # Additional context for FK violations
    timestamp_utc_us: int = field(default_factory=lambda: int(time.time() * 1_000_000))


@dataclass
class ValidationSummary:
    """Summary of validation results."""
    table_name: str
    total_rows: int
    passed_rows: int
    failed_rows: int
    coerced_rows: int
    violations: List[SchemaViolation]
    violations_by_type: Dict[str, int] = field(default_factory=dict)
    fields_with_most_violations: List[str] = field(default_factory=list)
    first_error_examples: Dict[str, SchemaViolation] = field(default_factory=dict)
    validation_timestamp_utc_us: int = field(default_factory=lambda: int(time.time() * 1_000_000))
    
    @property
    def pass_rate(self) -> float:
        if self.total_rows == 0:
            return 1.0
        return self.passed_rows / self.total_rows
    
    @property
    def status(self) -> ValidationResult:
        if self.failed_rows > 0:
            return ValidationResult.FAIL
        elif self.coerced_rows > 0:
            return ValidationResult.COERCED
        else:
            return ValidationResult.PASS
    
@dataclass
class ValidationFlags:
    """Row-level validation state indicators."""
    had_pattern_error: bool = False
    had_range_error: bool = False
    had_type_error: bool = False
    had_null_error: bool = False
    had_foreign_key_error: bool = False
    had_unique_constraint_error: bool = False
    had_coercion: bool = False  # Separate flag for successful coercions


class SchemaValidatorAgent:
    """
    Enforces data contracts across streaming data.
    
    Key Features:
    - Type validation with explicit coercion rules
    - Range and pattern validation
    - Referential integrity checks
    - Null/required field validation
    - Unique constraint validation
    - Clear violation reporting
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.schemas: Dict[str, TableSchema] = {}
        self.reference_data: Dict[str, Dict[str, Set[Any]]] = {}  # table -> field -> values
        self.running = False
        self._semantic_patterns: Dict[str, Pattern[str]] = {}  # Will be initialized below
        
        # Data Engineering Enhancement - Safe data curation only
        self.enable_data_curation = config.get('enable_data_curation', True)
        self.curation_config = config.get('curation', {
            'normalize_prices': True,         # Price/size normalization
            'temporal_bucketing': True,       # Time bucket organization  
            'venue_normalization': True,      # Cross-venue name consistency
            'quality_scoring': True           # Data completeness scoring
        })
        
        # Initialize data structures for safe curation
        self.venue_books = {}         # symbol -> venue -> book_data  
        self.normalization_cache = {} # Cached normalized values
        
        # Streaming Bus Integration
        streaming_config = self.config.get("streaming_bus", {
            "bootstrap_servers": "localhost:9092",
            "enable_ssl": False,
            "enable_sasl": False
        })
        self.streaming_bus = StreamingBus(streaming_config)
        
        # Task lifecycle management for consumer tasks
        self._consumer_task: Optional[asyncio.Task] = None
        self._pending_validation_tasks: Set[asyncio.Task] = set()
        
        # Circuit breaker integration for resilience
        self.circuit_breaker_id = f"schema_validator_{id(self)}"
        self._circuit_breaker_registered = False
        
        # Health monitoring
        self._health_check_task: Optional[asyncio.Task] = None
        self._last_health_check = 0.0
        self._health_check_interval = config.get('health_check_interval', 30.0)
        
        # Retry configuration with exponential backoff
        self.retry_config = {
            'max_retries': config.get('max_retries', 3),
            'base_delay': config.get('retry_base_delay', 1.0),
            'max_delay': config.get('retry_max_delay', 60.0),
            'exponential_base': config.get('retry_exponential_base', 2.0)
        }
        
        # Metrics and telemetry
        self.metrics = {
            'validations_processed': 0,
            'validations_passed': 0,
            'validations_failed': 0,
            'schema_violations': 0,
            'circuit_breaker_trips': 0,
            'retry_attempts': 0,
            'health_checks_performed': 0,
            'avg_validation_time_ms': 0.0,
            'last_validation_timestamp': 0
        }
        
        # Strict mode disables all coercion
        self.strict_mode = config.get('strict_mode', False)
        
        # Timestamp validation bounds (microseconds)
        self.min_timestamp_us = config.get('min_timestamp_us', 946684800000000)  # 2000-01-01
        self.max_timestamp_us = config.get('max_timestamp_us', 4102444800000000)  # 2100-01-01
        
        # Precompiled semantic validation patterns for performance
        self._semantic_patterns = {
            'email': re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
            'url': re.compile(r'^https?://[^\s/$.?#].[^\s]*$'),
            'phone': re.compile(r'^\+?[1-9]\d{1,14}$')
        }
        
        # Default coercion rules (only used if strict_mode=False)
        self.default_coercion_rules = self._build_default_coercion_rules()
        
        # Canonical headers: Sequence tracking for institutional compliance
        # Changed from topic-keyed to source_id-keyed for correct monotonic sequencing per source
        self._sequence_numbers: Dict[str, int] = defaultdict(int)  # source_id -> sequence_number
    
    def _build_default_coercion_rules(self) -> List[CoercionRule]:
        """Build default coercion rules for common type conversions."""
        return [
            # Numeric string to integer (improved regex)
            CoercionRule(
                from_type=str, 
                to_type=int,
                coercer=lambda x: int(x.strip()) if re.fullmatch(r'^[+-]?\d+$', x.strip()) else None,
                description="String to integer for valid numeric strings"
            ),
            CoercionRule(
                from_type=str,
                to_type=Decimal,
                coercer=lambda x: self._safe_decimal_coercion(x.strip()) if isinstance(x, str) else None,
                description="String to decimal for valid numeric strings"
            ),
            # Numeric to string
            CoercionRule(
                from_type=int,
                to_type=str,
                coercer=str,
                description="Integer to string"
            ),
            CoercionRule(
                from_type=float,
                to_type=str,
                coercer=str,
                description="Float to string"
            ),
            # Boolean coercions (symmetric)
            CoercionRule(
                from_type=str,
                to_type=bool,
                coercer=lambda x: self._string_to_bool(x) if isinstance(x, str) else None,
                description="String to boolean"
            ),
            CoercionRule(
                from_type=int,
                to_type=bool,
                coercer=lambda x: True if x == 1 else False if x == 0 else None,
                description="Integer to boolean (only 0→False, 1→True)"
            ),
            # Timestamp unit normalization
            CoercionRule(
                from_type=str,
                to_type=int,
                coercer=lambda x: self._timestamp_from_string(x) if isinstance(x, str) else None,
                description="String to timestamp microseconds"
            )
        ]
    
    def _is_decimal_string(self, value: str) -> bool:
        """Check if string can be converted to Decimal."""
        try:
            Decimal(value)
            return True
        except:
            return False
    
    def _safe_decimal_coercion(self, value: str) -> Optional[Decimal]:
        """Safely convert string to Decimal, rejecting NaN/Inf."""
        try:
            d = Decimal(value)
            if d.is_finite():
                return d
            return None
        except:
            return None
    
    def _string_to_bool(self, value: str) -> Optional[bool]:
        """Convert string to boolean with explicit mapping."""
        lower_val = value.lower().strip()
        if lower_val in {'true', '1', 'yes', 'on'}:
            return True
        elif lower_val in {'false', '0', 'no', 'off'}:
            return False
        return None
    
    def _timestamp_from_string(self, value: str) -> Optional[int]:
        """Convert string to timestamp microseconds with unit detection and ISO support."""
        try:
            # Try ISO-8601 format first
            if 'T' in value or '-' in value:
                from datetime import datetime, timezone
                # Common ISO formats
                iso_formats = [
                    '%Y-%m-%dT%H:%M:%S.%fZ',
                    '%Y-%m-%dT%H:%M:%SZ', 
                    '%Y-%m-%dT%H:%M:%S.%f',
                    '%Y-%m-%dT%H:%M:%S',
                    '%Y-%m-%d %H:%M:%S.%f',
                    '%Y-%m-%d %H:%M:%S'
                ]
                
                for fmt in iso_formats:
                    try:
                        dt = datetime.strptime(value.strip(), fmt)
                        # If no timezone info, assume UTC to avoid local time drift
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        return int(dt.timestamp() * 1_000_000)
                    except ValueError:
                        continue
            
            # Try to parse as numeric string
            if not re.fullmatch(r'^[+-]?\d+$', value.strip()):
                return None
            
            num = int(value.strip())
            
            # Unit detection by magnitude
            if num < 1e10:  # Likely seconds
                return num * 1_000_000
            elif num < 1e13:  # Likely milliseconds 
                return num * 1_000
            elif num < 1e16:  # Likely microseconds
                return num
            elif num < 1e19:  # Likely nanoseconds
                return num // 1_000
            else:
                return None  # Out of reasonable range
        except:
            return None
    
    def _generate_row_identifier(self, table_name: str, row: Dict[str, Any], fallback_index: int) -> str:
        """Generate a stable row identifier for traceability across runs."""
        if table_name in self.schemas:
            schema = self.schemas[table_name]
            
            # Try primary key first
            if schema.primary_key:
                pk_values = []
                all_pk_present = True
                for pk_field in schema.primary_key:
                    if pk_field in row and row[pk_field] is not None:
                        pk_values.append(str(row[pk_field]))
                    else:
                        all_pk_present = False
                        break
                
                if all_pk_present:
                    return ":".join(pk_values)
        
        # Fallback to hash of sorted key-value pairs for stability
        try:
            import hashlib
            sorted_items = sorted([(k, str(v)) for k, v in row.items() if v is not None])
            row_string = "|".join([f"{k}={v}" for k, v in sorted_items])
            return hashlib.md5(row_string.encode()).hexdigest()[:16]
        except Exception:
            # Ultimate fallback to index
            return f"row_{fallback_index}"
    
    def _get_levenshtein_candidates(self, value: str, valid_values: Set[str], max_distance: int = 2) -> List[str]:
        """Get close matches using Levenshtein distance, sorted by similarity."""
        candidates_with_scores = []
        for valid_val in valid_values:
            # Calculate similarity score using difflib
            ratio = difflib.SequenceMatcher(None, value.lower(), valid_val.lower()).ratio()
            if ratio >= 0.85:  # Higher threshold to reduce noisy "did you mean" hints
                candidates_with_scores.append((valid_val, ratio))
        
        # Sort by similarity score (descending) and return top 3
        candidates_with_scores.sort(key=lambda x: x[1], reverse=True)
        return [candidate for candidate, score in candidates_with_scores[:3]]
    
    def _validate_checksum(self, value: str, algorithm: str) -> bool:
        """Validate checksums using various algorithms."""
        if algorithm == "luhn":
            return self._luhn_checksum(value)
        elif algorithm == "iban":
            return self._iban_checksum(value)
        elif algorithm == "isbn":
            return self._isbn_checksum(value)
        return False
    
    def _luhn_checksum(self, value: str) -> bool:
        """Validate Luhn checksum (credit cards, etc.)."""
        digits = [int(d) for d in value if d.isdigit()]
        if len(digits) < 2:
            return False
        
        checksum = 0
        for i, digit in enumerate(reversed(digits)):
            if i % 2 == 1:  # Every second digit from the right
                digit *= 2
                if digit > 9:
                    digit -= 9
            checksum += digit
        return checksum % 10 == 0
    
    def _iban_checksum(self, value: str) -> bool:
        """Validate IBAN checksum."""
        # Basic IBAN validation - move first 4 chars to end, convert letters to numbers
        if len(value) < 15:
            return False
        
        rearranged = value[4:] + value[:4]
        numeric = ""
        for char in rearranged:
            if char.isdigit():
                numeric += char
            elif char.isalpha():
                numeric += str(ord(char.upper()) - ord('A') + 10)
        
        try:
            return int(numeric) % 97 == 1
        except ValueError:
            return False
    
    def _isbn_checksum(self, value: str) -> bool:
        """Validate ISBN-10 or ISBN-13 checksum."""
        digits = [d for d in value if d.isdigit() or d.upper() == 'X']
        
        if len(digits) == 10:  # ISBN-10
            checksum = sum((10 - i) * (10 if d == 'X' else int(d)) for i, d in enumerate(digits))
            return checksum % 11 == 0
        elif len(digits) == 13:  # ISBN-13
            checksum = sum((3 if i % 2 else 1) * int(d) for i, d in enumerate(digits))
            return checksum % 10 == 0
        
        return False
    
    def _validate_semantic(self, value: str, semantic_type: str) -> bool:
        """Validate semantic types like email, URL, phone numbers."""
        if semantic_type == "email":
            return self._semantic_patterns['email'].fullmatch(value) is not None
        elif semantic_type == "url":
            return self._semantic_patterns['url'].fullmatch(value) is not None
        elif semantic_type == "phone":
            # International phone number format - strip non-digits first
            clean_phone = re.sub(r'[^\d+]', '', value)
            return self._semantic_patterns['phone'].fullmatch(clean_phone) is not None
        elif semantic_type == "country_code":
            # ISO 3166-1 alpha-2 country codes (simplified check)
            return len(value) == 2 and value.isupper() and value.isalpha()
        elif semantic_type == "currency_code":
            # ISO 4217 currency codes (simplified check)
            return len(value) == 3 and value.isupper() and value.isalpha()
        return False
    
    def _validate_business_hours(self, timestamp_us: int) -> bool:
        """Validate timestamp is within business hours (9 AM - 5 PM UTC, Mon-Fri)."""
        dt = datetime.fromtimestamp(timestamp_us / 1_000_000, tz=timezone.utc)
        return (dt.weekday() < 5 and  # Monday = 0, Friday = 4
                9 <= dt.hour < 17)  # 9 AM to 5 PM
    
    def _validate_temporal_consistency(self, timestamp_us: int, table_name: str, field_name: str) -> List[SchemaViolation]:
        """
        Enterprise-grade temporal consistency validation.
        
        Validates:
        - Clock drift detection
        - Sequence ordering validation
        - Weekend/holiday filtering
        - Time zone consistency
        - Temporal gaps detection
        - Future timestamp prevention
        """
        violations = []
        
        # Convert to datetime for analysis
        try:
            dt = datetime.fromtimestamp(timestamp_us / 1_000_000, tz=timezone.utc)
        except (ValueError, OSError, OverflowError) as e:
            violations.append(SchemaViolation(
                table_name=table_name,
                field_name=field_name,
                violation_type="invalid_timestamp_conversion",
                expected="valid timestamp that can be converted to datetime",
                actual=f"timestamp {timestamp_us} caused error: {str(e)}",
                row_identifier="temporal_validation",
                severity="error"
            ))
            return violations
        
        # 1. Future timestamp prevention (allow small buffer for clock skew)
        current_time_us = int(time.time() * 1_000_000)
        max_future_buffer_us = 300_000_000  # 5 minutes buffer for clock skew
        
        if timestamp_us > current_time_us + max_future_buffer_us:
            future_seconds = (timestamp_us - current_time_us) / 1_000_000
            violations.append(SchemaViolation(
                table_name=table_name,
                field_name=field_name,
                violation_type="future_timestamp_violation",
                expected=f"timestamp within {max_future_buffer_us/1_000_000:.1f}s of current time",
                actual=f"timestamp {future_seconds:.1f}s in the future",
                row_identifier="temporal_validation",
                severity="warning"  # Bronze layer - clock skew is common in distributed systems
            ))
        
        # 2. Clock drift detection (track per table/field)
        drift_key = f"{table_name}.{field_name}"
        if not hasattr(self, '_temporal_drift_tracker'):
            self._temporal_drift_tracker = {}
            self._temporal_sequence_tracker = {}
        
        if drift_key not in self._temporal_drift_tracker:
            self._temporal_drift_tracker[drift_key] = {
                'last_timestamp': timestamp_us,
                'drift_samples': [],
                'max_samples': 100
            }
        else:
            tracker = self._temporal_drift_tracker[drift_key]
            time_diff = timestamp_us - tracker['last_timestamp']
            
            # Track drift samples (time differences)
            tracker['drift_samples'].append(time_diff)
            if len(tracker['drift_samples']) > tracker['max_samples']:
                tracker['drift_samples'].pop(0)
            
            # Detect abnormal clock drift (sudden jumps > 1 hour)
            if abs(time_diff) > 3600_000_000:  # 1 hour in microseconds
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=field_name,
                    violation_type="clock_drift_violation",
                    expected="timestamp progression within reasonable bounds",
                    actual=f"timestamp jumped by {time_diff/1_000_000:.1f}s",
                    row_identifier="temporal_validation",
                    severity="warning"
                ))
            
            # Detect backwards time (expect in streaming Bronze layer)
            if time_diff < -60_000_000:  # Allow 1 minute tolerance for minor reordering
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=field_name,
                    violation_type="temporal_ordering_violation",
                    expected="timestamps in non-decreasing order (±1min tolerance)",
                    actual=f"timestamp went backwards by {abs(time_diff)/1_000_000:.1f}s",
                    row_identifier="temporal_validation",
                    severity="warning"  # Bronze layer - out-of-order is expected in streaming
                ))
            
            tracker['last_timestamp'] = timestamp_us
        
        # 3. Sequence gap detection (for high-frequency data)
        seq_key = f"{table_name}.{field_name}"
        if seq_key not in self._temporal_sequence_tracker:
            self._temporal_sequence_tracker[seq_key] = {
                'expected_interval_us': None,
                'last_timestamp': timestamp_us,
                'interval_samples': [],
                'max_gap_multiplier': 5.0
            }
        else:
            seq_tracker = self._temporal_sequence_tracker[seq_key]
            interval = timestamp_us - seq_tracker['last_timestamp']
            
            if interval > 0:  # Only track positive intervals
                seq_tracker['interval_samples'].append(interval)
                if len(seq_tracker['interval_samples']) > 50:
                    seq_tracker['interval_samples'].pop(0)
                
                # Estimate expected interval from recent samples
                if len(seq_tracker['interval_samples']) >= 10:
                    seq_tracker['expected_interval_us'] = statistics.median(seq_tracker['interval_samples'])
                    
                    # Detect large gaps
                    expected = seq_tracker['expected_interval_us']
                    if interval > expected * seq_tracker['max_gap_multiplier']:
                        gap_ratio = interval / expected
                        violations.append(SchemaViolation(
                            table_name=table_name,
                            field_name=field_name,
                            violation_type="temporal_gap_violation",
                            expected=f"interval ~{expected/1_000_000:.3f}s (±{seq_tracker['max_gap_multiplier']}x)",
                            actual=f"gap of {interval/1_000_000:.3f}s ({gap_ratio:.1f}x expected)",
                            row_identifier="temporal_validation",
                            severity="warning"
                        ))
            
            seq_tracker['last_timestamp'] = timestamp_us
        
        # 4. Weekend/Holiday validation for market data
        if table_name.startswith(('market_', 'trading_', 'exchange_')):
            # Crypto markets are 24/7, but traditional market data should not appear on weekends
            if 'stock' in table_name.lower() or 'equity' in table_name.lower():
                if dt.weekday() >= 5:  # Saturday = 5, Sunday = 6
                    violations.append(SchemaViolation(
                        table_name=table_name,
                        field_name=field_name,
                        violation_type="weekend_market_data_violation",
                        expected="no traditional market data on weekends",
                        actual=f"timestamp on {dt.strftime('%A')} ({dt.isoformat()})",
                        row_identifier="temporal_validation",
                        severity="warning"
                    ))
        
        # 5. Timezone consistency check (all timestamps should be UTC)
        if dt.tzinfo != timezone.utc:
            violations.append(SchemaViolation(
                table_name=table_name,
                field_name=field_name,
                violation_type="timezone_consistency_violation",
                expected="UTC timezone",
                actual=f"timezone: {dt.tzinfo}",
                row_identifier="temporal_validation",
                severity="error"
            ))
        
        # 6. Reasonable timestamp bounds (not too far in past/future)
        min_reasonable_timestamp = datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp() * 1_000_000
        max_reasonable_timestamp = datetime(2030, 1, 1, tzinfo=timezone.utc).timestamp() * 1_000_000
        
        if timestamp_us < min_reasonable_timestamp:
            violations.append(SchemaViolation(
                table_name=table_name,
                field_name=field_name,
                violation_type="timestamp_too_old_violation",
                expected="timestamp after 2020-01-01",
                actual=f"timestamp: {dt.isoformat()}",
                row_identifier="temporal_validation",
                severity="warning"
            ))
        elif timestamp_us > max_reasonable_timestamp:
            violations.append(SchemaViolation(
                table_name=table_name,
                field_name=field_name,
                violation_type="timestamp_too_future_violation",
                expected="timestamp before 2030-01-01",
                actual=f"timestamp: {dt.isoformat()}",
                row_identifier="temporal_validation",
                severity="warning"
            ))
        
        return violations
    
    def _validate_cross_field_rules(self, schema: TableSchema, row: Dict[str, Any], row_id: str) -> List[SchemaViolation]:
        """Validate cross-field rules."""
        violations = []
        
        for rule in schema.cross_field_rules:
            if rule.rule_type == "conditional_required":
                # If condition is met, certain fields become required
                if rule.condition and rule.condition(row):
                    for field_name in rule.fields:
                        if field_name not in row or row[field_name] is None:
                            violations.append(SchemaViolation(
                                table_name=schema.name,
                                field_name=field_name,
                                violation_type="conditional_required_missing",
                                expected=f"required when condition met: {rule.error_message}",
                                actual="null/missing",
                                row_identifier=row_id,
                                severity=rule.severity
                            ))
            
            elif rule.rule_type == "mutual_exclusive":
                # Exactly one of the fields should be set
                set_fields = [f for f in rule.fields if f in row and row[f] is not None]
                if len(set_fields) != 1:
                    violations.append(SchemaViolation(
                        table_name=schema.name,
                        field_name=",".join(rule.fields),
                        violation_type="mutual_exclusivity_violation",
                        expected=f"exactly one of {rule.fields}",
                        actual=f"found {len(set_fields)} set: {set_fields}",
                        row_identifier=row_id,
                        severity=rule.severity
                    ))
            
            elif rule.rule_type == "temporal_ordering":
                # Enhanced temporal ordering validation between timestamp fields
                violations.extend(self._validate_cross_field_temporal_ordering(
                    schema.name, rule, row, row_id
                ))
            
            elif rule.rule_type == "field_relationship":
                # Validate field relationships using custom condition
                if rule.condition and rule.condition(row):
                    violations.append(SchemaViolation(
                        table_name=schema.name,
                        field_name=",".join(rule.fields),
                        violation_type="field_relationship_violation",
                        expected=f"relationship satisfied: {rule.error_message}",
                        actual=f"relationship violated between {rule.fields}",
                        row_identifier=row_id,
                        severity=rule.severity
                    ))
        
        return violations
    
    def _validate_cross_field_temporal_ordering(self, table_name: str, rule: CrossFieldRule, 
                                              row: Dict[str, Any], row_id: str) -> List[SchemaViolation]:
        """
        Enhanced cross-field temporal ordering validation with enterprise-grade checks.
        
        Validates:
        - Strict temporal ordering between multiple timestamp fields
        - Reasonable time gaps between sequential events
        - Context-aware validation rules for different data types
        - Microsecond precision handling
        """
        violations = []
        
        if len(rule.fields) < 2:
            return violations
        
        # Extract and normalize timestamps
        timestamps = []
        for field_name in rule.fields:
            if field_name in row and row[field_name] is not None:
                raw_timestamp = row[field_name]
                
                # Normalize timestamp to microseconds (int)
                try:
                    if isinstance(raw_timestamp, str):
                        # Try parsing ISO format first, then float
                        try:
                            dt = datetime.fromisoformat(raw_timestamp.replace('Z', '+00:00'))
                            # Normalize to UTC
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            else:
                                dt = dt.astimezone(timezone.utc)
                            normalized_ts = int(dt.timestamp() * 1_000_000)
                        except ValueError:
                            normalized_ts = int(float(raw_timestamp) * 1_000_000)
                    elif isinstance(raw_timestamp, (int, float)):
                        # Assume microseconds if > 1e12, else seconds
                        if raw_timestamp > 1e12:
                            normalized_ts = int(raw_timestamp)
                        else:
                            normalized_ts = int(raw_timestamp * 1_000_000)
                    else:
                        continue  # Skip incompatible types
                    
                    timestamps.append((field_name, normalized_ts, raw_timestamp))
                    
                except (ValueError, TypeError, OverflowError):
                    violations.append(SchemaViolation(
                        table_name=table_name,
                        field_name=field_name,
                        violation_type="timestamp_normalization_error",
                        expected="valid timestamp format",
                        actual=f"could not normalize: {raw_timestamp}",
                        row_identifier=row_id,
                        severity="error"
                    ))
                    continue
        
        if len(timestamps) < 2:
            return violations  # Need at least 2 timestamps to validate ordering
        
        # Validate temporal ordering
        for i in range(1, len(timestamps)):
            prev_field, prev_ts, prev_raw = timestamps[i-1]
            curr_field, curr_ts, curr_raw = timestamps[i]
            
            # Basic ordering check
            if curr_ts < prev_ts:
                time_diff_ms = (prev_ts - curr_ts) / 1000
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=f"{prev_field},{curr_field}",
                    violation_type="temporal_ordering_violation",
                    expected=f"{prev_field} <= {curr_field}",
                    actual=f"{prev_field}({prev_ts}) > {curr_field}({curr_ts}) by {time_diff_ms:.1f}ms",
                    row_identifier=row_id,
                    severity=rule.severity
                ))
            
            # Context-aware gap validation
            time_gap_us = curr_ts - prev_ts
            gap_violations = self._validate_temporal_gap_context(
                table_name, prev_field, curr_field, time_gap_us, row_id, rule.severity
            )
            violations.extend(gap_violations)
        
        # Validate simultaneous timestamp constraints
        simultaneous_violations = self._validate_simultaneous_timestamp_constraints(
            table_name, timestamps, row_id, rule.severity
        )
        violations.extend(simultaneous_violations)
        
        return violations
    
    def _validate_temporal_gap_context(self, table_name: str, prev_field: str, curr_field: str,
                                     gap_us: int, row_id: str, severity: str) -> List[SchemaViolation]:
        """Validate temporal gaps based on field context and table type."""
        violations = []
        
        # Define reasonable gaps for different field combinations
        gap_constraints = {
            # Trading/execution sequence validation
            ('signal_timestamp', 'order_timestamp'): (0, 1_000_000),  # 0-1s
            ('order_timestamp', 'ack_timestamp'): (0, 5_000_000),     # 0-5s
            ('ack_timestamp', 'fill_timestamp'): (0, 30_000_000),     # 0-30s
            ('order_timestamp', 'fill_timestamp'): (0, 60_000_000),   # 0-60s
            
            # Market data sequence validation
            ('trade_timestamp', 'book_timestamp'): (-1_000_000, 1_000_000),  # ±1s
            ('quote_timestamp', 'trade_timestamp'): (-5_000_000, 0),         # quote before trade
            
            # Settlement and clearing
            ('trade_timestamp', 'settlement_timestamp'): (0, 259_200_000_000),  # 0-3 days
            ('settlement_timestamp', 'clear_timestamp'): (0, 86_400_000_000),   # 0-1 day
            
            # On-chain validation
            ('block_timestamp', 'tx_timestamp'): (-30_000_000, 30_000_000),  # ±30s block tolerance
            ('tx_timestamp', 'confirmation_timestamp'): (0, 1800_000_000),   # 0-30min confirmation
        }
        
        # Check for exact field name matches
        field_pair = (prev_field, curr_field)
        if field_pair in gap_constraints:
            min_gap, max_gap = gap_constraints[field_pair]
            
            if gap_us < min_gap:
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=f"{prev_field},{curr_field}",
                    violation_type="temporal_gap_too_small",
                    expected=f"gap >= {min_gap/1_000_000:.3f}s",
                    actual=f"gap = {gap_us/1_000_000:.3f}s",
                    row_identifier=row_id,
                    severity=severity
                ))
            elif gap_us > max_gap:
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=f"{prev_field},{curr_field}",
                    violation_type="temporal_gap_too_large",
                    expected=f"gap <= {max_gap/1_000_000:.3f}s",
                    actual=f"gap = {gap_us/1_000_000:.3f}s",
                    row_identifier=row_id,
                    severity=severity
                ))
        
        # Pattern-based validation for common timestamp patterns
        elif prev_field.endswith('_timestamp') and curr_field.endswith('_timestamp'):
            # Generic timestamp pair - check for suspiciously large gaps
            max_reasonable_gap = 86400_000_000  # 1 day
            
            if gap_us > max_reasonable_gap:
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=f"{prev_field},{curr_field}",
                    violation_type="temporal_gap_suspicious",
                    expected=f"reasonable gap < {max_reasonable_gap/1_000_000:.0f}s",
                    actual=f"gap = {gap_us/1_000_000:.1f}s",
                    row_identifier=row_id,
                    severity="warning"
                ))
        
        return violations
    
    def _validate_simultaneous_timestamp_constraints(self, table_name: str, timestamps: List[tuple],
                                                   row_id: str, severity: str) -> List[SchemaViolation]:
        """Validate constraints for timestamps that should be simultaneous or have specific relationships."""
        violations = []
        
        # Check for duplicated timestamps (might indicate clock resolution issues)
        timestamp_values = [ts for _, ts, _ in timestamps]
        unique_timestamps = set(timestamp_values)
        
        if len(unique_timestamps) < len(timestamp_values):
            # Find duplicates
            from collections import Counter
            counts = Counter(timestamp_values)
            duplicates = [ts for ts, count in counts.items() if count > 1]
            
            duplicate_fields = []
            for dup_ts in duplicates:
                fields = [field for field, ts, _ in timestamps if ts == dup_ts]
                duplicate_fields.extend(fields)
            
            violations.append(SchemaViolation(
                table_name=table_name,
                field_name=','.join(duplicate_fields),
                violation_type="duplicate_timestamps",
                expected="unique timestamps for different events",
                actual=f"duplicate timestamp(s): {duplicates}",
                row_identifier=row_id,
                severity="warning"
            ))
        
        # Check for microsecond precision consistency
        precision_issues = []
        for field, ts_us, raw_value in timestamps:
            # Check if timestamp has suspicious precision (e.g., always ends in 000)
            if ts_us % 1000 == 0:  # Millisecond precision
                precision_issues.append((field, 'millisecond'))
            elif ts_us % 1_000_000 == 0:  # Second precision
                precision_issues.append((field, 'second'))
        
        # If we have mixed precision, warn about potential issues
        precisions = set(precision for _, precision in precision_issues)
        if len(precisions) > 1:
            violations.append(SchemaViolation(
                table_name=table_name,
                field_name=','.join([field for field, _ in precision_issues]),
                violation_type="mixed_timestamp_precision",
                expected="consistent timestamp precision across fields",
                actual=f"mixed precisions: {precisions}",
                row_identifier=row_id,
                severity="warning"
            ))
        
        return violations
    
    def reset_temporal_tracking(self, table_name: Optional[str] = None, field_name: Optional[str] = None) -> None:
        """
        Reset temporal tracking state for maintenance or testing.
        
        Args:
            table_name: If specified, reset tracking for specific table only
            field_name: If specified (with table_name), reset tracking for specific field only
        """
        if not hasattr(self, '_temporal_drift_tracker'):
            return
        
        if table_name and field_name:
            # Reset specific field
            key = f"{table_name}.{field_name}"
            self._temporal_drift_tracker.pop(key, None)
            self._temporal_sequence_tracker.pop(key, None)
            logger.info(f"Reset temporal tracking for {key}")
        elif table_name:
            # Reset all fields for table
            keys_to_remove = [k for k in self._temporal_drift_tracker.keys() if k.startswith(f"{table_name}.")]
            for key in keys_to_remove:
                self._temporal_drift_tracker.pop(key, None)
                self._temporal_sequence_tracker.pop(key, None)
            logger.info(f"Reset temporal tracking for table {table_name} ({len(keys_to_remove)} fields)")
        else:
            # Reset all tracking
            field_count = len(self._temporal_drift_tracker)
            self._temporal_drift_tracker.clear()
            self._temporal_sequence_tracker.clear()
            logger.info(f"Reset all temporal tracking ({field_count} fields)")
    
    def get_temporal_tracking_stats(self) -> Dict[str, Any]:
        """Get current temporal tracking statistics for monitoring."""
        if not hasattr(self, '_temporal_drift_tracker'):
            return {}
        
        stats = {
            'tracked_fields': len(self._temporal_drift_tracker),
            'fields': {}
        }
        
        for key, tracker in self._temporal_drift_tracker.items():
            field_stats = {
                'last_timestamp': tracker['last_timestamp'],
                'drift_samples_count': len(tracker['drift_samples']),
            }
            
            if tracker['drift_samples']:
                field_stats.update({
                    'avg_interval_us': statistics.mean(tracker['drift_samples']),
                    'median_interval_us': statistics.median(tracker['drift_samples']),
                    'min_interval_us': min(tracker['drift_samples']),
                    'max_interval_us': max(tracker['drift_samples'])
                })
            
            # Add sequence tracking stats if available
            if hasattr(self, '_temporal_sequence_tracker') and key in self._temporal_sequence_tracker:
                seq_tracker = self._temporal_sequence_tracker[key]
                field_stats['expected_interval_us'] = seq_tracker.get('expected_interval_us')
                field_stats['interval_samples_count'] = len(seq_tracker.get('interval_samples', []))
            
            stats['fields'][key] = field_stats
        
        return stats
    
    def register_schema(self, schema: TableSchema) -> None:
        """Register a table schema for validation."""
        self.schemas[schema.name] = schema
        logger.info(f"Registered schema for table: {schema.name}")
    
    def update_reference_data(self, table_name: str, field_name: str, key_values: Set[Any]) -> None:
        """Update reference data for foreign key validation."""
        if table_name not in self.reference_data:
            self.reference_data[table_name] = {}
        self.reference_data[table_name][field_name] = key_values
        logger.debug(f"Updated reference data for {table_name}.{field_name}: {len(key_values)} keys")
    
    async def validate_row(self, table_name: str, row: Dict[str, Any], row_id: str) -> Tuple[Dict[str, Any], List[SchemaViolation], ValidationFlags]:
        """
        Validate a single row against its schema with metrics tracking.
        
        Returns:
            (cleaned_row, violations, validation_flags)
        """
        start_time = time.time()
        flags = ValidationFlags()
        
        try:
            if table_name not in self.schemas:
                violation = SchemaViolation(
                    table_name=table_name,
                    field_name=None,
                    violation_type="unknown_table",
                    expected="registered table schema",
                    actual=f"table '{table_name}' not registered",
                    row_identifier=row_id,
                    severity="error"
                )
                self._update_validation_metrics((time.time() - start_time) * 1000, False, 1)
                return row, [violation], flags
        
            schema = self.schemas[table_name]
            violations = []
            cleaned_row = {}
        
            # Check all required fields are present
            for field_schema in schema.fields:
                field_name = field_schema.name
                value = row.get(field_name)
                
                # Required field validation
                if field_schema.required and (value is None or value == ""):
                    violations.append(SchemaViolation(
                        table_name=table_name,
                        field_name=field_name,
                        violation_type="required_field_missing",
                        expected="non-null value",
                        actual="null/empty",
                        row_identifier=row_id
                    ))
                    continue
                
                # Nullable field validation
                if value is None:
                    if field_schema.nullable:
                        cleaned_row[field_name] = None
                        continue
                    else:
                        violations.append(SchemaViolation(
                            table_name=table_name,
                            field_name=field_name,
                            violation_type="null_not_allowed",
                            expected="non-null value",
                            actual="null",
                            row_identifier=row_id
                        ))
                        continue
                
                # Validate and potentially coerce the field
                validated_value, field_violations = await self._validate_field(
                    field_schema, value, table_name, row_id
                )
                
                violations.extend(field_violations)
                cleaned_row[field_name] = validated_value
        
            # Check for unexpected fields
            for field_name in row:
                if field_name not in schema.field_map:
                    if not schema.allow_extra_fields:
                        severity = schema.unexpected_field_severity
                        if severity != "ignore":
                            violations.append(SchemaViolation(
                                table_name=table_name,
                                field_name=field_name,
                                violation_type="unexpected_field",
                                expected="field not in schema",
                                actual=f"unexpected field '{field_name}'",
                                row_identifier=row_id,
                                severity=severity
                            ))
                    
                    # Always preserve in cleaned_row (never actually drop fields)
                    cleaned_row[field_name] = row[field_name]
                    
                    # Log dropping intention if configured (but don't actually drop)
                    if schema.drop_extra_fields and schema.allow_extra_fields:
                        # Use consistent severity - always "info" for dropping intentions
                        violations.append(SchemaViolation(
                            table_name=table_name,
                            field_name=field_name,
                            violation_type="field_would_be_dropped",
                            expected="field removal (not performed)",
                                actual=f"would drop '{field_name}' (preserved per no-reshape policy)",
                            row_identifier=row_id,
                            severity="info"
                        ))
            
            # Validate primary key nullability
            if schema.primary_key:
                for pk_field in schema.primary_key:
                    if cleaned_row.get(pk_field) is None:
                        violations.append(SchemaViolation(
                            table_name=table_name,
                            field_name=pk_field,
                            violation_type="primary_key_null",
                            expected="non-null primary key value",
                            actual="null",
                            row_identifier=row_id,
                            severity="error"
                        ))
                        flags.had_null_error = True
            
            # Validate referential integrity
            ref_violations = await self._validate_referential_integrity(
                schema, cleaned_row, table_name, row_id
            )
            violations.extend(ref_violations)
            for v in ref_violations:
                if v.violation_type.endswith("foreign_key_violation"):
                    flags.had_foreign_key_error = True
            
            # Set row-level flags based on violations
            for violation in violations:
                if "pattern" in violation.violation_type:
                    flags.had_pattern_error = True
                elif "range" in violation.violation_type or "minimum" in violation.violation_type or "maximum" in violation.violation_type:
                    flags.had_range_error = True
                elif violation.violation_type == "type_coerced" or violation.violation_type.endswith("_type_coerced"):
                    flags.had_coercion = True  # Include list/dict item coercions as benign coercions
                elif "type" in violation.violation_type and violation.violation_type != "type_coerced" and not violation.violation_type.endswith("_type_coerced"):
                    flags.had_type_error = True  # Only for actual type errors, not coercions
                elif "null" in violation.violation_type:
                    flags.had_null_error = True
            
            # Enhanced cross-field validation
            cross_field_violations = self._validate_cross_field_rules(schema, cleaned_row, row_id)
            violations.extend(cross_field_violations)
            
            # Update metrics - only fail on ERROR severity (institutional Bronze layer)
            validation_time_ms = (time.time() - start_time) * 1000
            error_violations = [v for v in violations if v.severity == "error"]
            passed = len(error_violations) == 0
            self._update_validation_metrics(validation_time_ms, passed, len(violations), table_name)
            
            return cleaned_row, violations, flags
            
        except Exception as e:
            # Handle unexpected validation errors
            logger.error(f"Unexpected error during validation of {table_name}: {e}")
            violation = SchemaViolation(
                table_name=table_name,
                field_name=None,
                violation_type="validation_error",
                expected="successful validation",
                actual=f"validation error: {str(e)}",
                row_identifier=row_id,
                severity="error"
            )
            self._update_validation_metrics((time.time() - start_time) * 1000, False, 1)
            return row, [violation], flags
    
    async def _validate_field(self, field_schema: FieldSchema, value: Any, table_name: str, row_id: str) -> tuple[Any, List[SchemaViolation]]:
        """Validate a single field value."""
        violations = []
        original_value = value
        was_trimmed = False  # Track to avoid double-trim logging
        
        # Type validation with coercion
        coerced_value, type_violation = await self._validate_and_coerce_type(
            field_schema, value, table_name, row_id
        )
        
        if type_violation:
            violations.append(type_violation)
            if type_violation.coerced_value is None:
                return value, violations  # Couldn't coerce, return original
            value = type_violation.coerced_value
            # Check if this was a trim coercion by examining the rule description
            if (type_violation.violation_type == "type_coerced" and 
                isinstance(original_value, str) and isinstance(value, str) and
                original_value.strip() == value):
                was_trimmed = True
        else:
            value = coerced_value
        
        # Range validation for numeric types (after coercion)
        if field_schema.field_type in [FieldType.INTEGER, FieldType.DECIMAL]:
            # Standardize bounds to Decimal for DECIMAL fields to avoid type mismatch
            if field_schema.field_type == FieldType.DECIMAL and isinstance(value, Decimal):
                min_val = Decimal(str(field_schema.min_value)) if field_schema.min_value is not None else None
                max_val = Decimal(str(field_schema.max_value)) if field_schema.max_value is not None else None
            else:
                min_val = field_schema.min_value
                max_val = field_schema.max_value
            
            # Ensure value is comparable to min_val/max_val
            comparable_value = value
            try:
                if field_schema.field_type == FieldType.DECIMAL:
                    if not isinstance(comparable_value, Decimal):
                        comparable_value = Decimal(str(comparable_value))
                elif field_schema.field_type == FieldType.INTEGER:
                    if not isinstance(comparable_value, int):
                        comparable_value = int(comparable_value)
            except Exception:
                # If coercion fails, skip range check (type violation will be reported elsewhere)
                comparable_value = value

            if min_val is not None:
                try:
                    # Ensure types are compatible for comparison
                    if isinstance(comparable_value, (int, float, Decimal)) and isinstance(min_val, (int, float, Decimal)):
                        if comparable_value < min_val:
                            violations.append(SchemaViolation(
                                table_name=table_name,
                                field_name=field_schema.name,
                                violation_type="value_below_minimum",
                                expected=f">= {min_val}",
                                actual=str(value),
                                row_identifier=row_id
                            ))
                except Exception:
                    pass

            if max_val is not None:
                try:
                    # Ensure types are compatible for comparison
                    if isinstance(comparable_value, (int, float, Decimal)) and isinstance(max_val, (int, float, Decimal)):
                        if comparable_value > max_val:
                            violations.append(SchemaViolation(
                                table_name=table_name,
                                field_name=field_schema.name,
                                violation_type="value_above_maximum",
                                expected=f"<= {max_val}",
                                actual=str(value),
                                row_identifier=row_id
                            ))
                except Exception:
                    pass
        
        # Decimal precision validation (disabled in strict mode)
        if (not self.strict_mode and 
            field_schema.field_type == FieldType.DECIMAL and 
            isinstance(value, Decimal) and 
            field_schema.decimal_scale is not None):
            
            quantized = value.quantize(Decimal(10) ** -field_schema.decimal_scale, rounding=ROUND_HALF_EVEN)
            if quantized != value:
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=field_schema.name,
                    violation_type="scale_coerced",
                    expected=f"scale {field_schema.decimal_scale}",
                    actual=f"{value} -> {quantized}",
                    row_identifier=row_id,
                    severity="warning",
                    coerced_value=quantized
                ))
                value = quantized
        
        # Apply whitespace trimming if configured (before pattern/length checks)
        # Skip if already trimmed via coercion rule to avoid double-logging
        if (field_schema.trim_whitespace and isinstance(value, str) and not was_trimmed):
            trimmed = value.strip()
            if trimmed != value and not self.strict_mode:
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=field_schema.name,
                    violation_type="type_coerced",
                    expected="trimmed string",
                    actual=f"'{value}' -> '{trimmed}'",
                    row_identifier=row_id,
                    severity="warning",
                    coerced_value=trimmed
                ))
                value = trimmed
        
        # Length validation for strings
        if field_schema.field_type == FieldType.STRING and isinstance(value, str):
            if field_schema.min_length is not None and len(value) < field_schema.min_length:
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=field_schema.name,
                    violation_type="string_too_short",
                    expected=f"length >= {field_schema.min_length}",
                    actual=f"length {len(value)}",
                    row_identifier=row_id
                ))
            
            if field_schema.max_length is not None and len(value) > field_schema.max_length:
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=field_schema.name,
                    violation_type="string_too_long",
                    expected=f"length <= {field_schema.max_length}",
                    actual=f"length {len(value)}",
                    row_identifier=row_id
                ))
        
        # Pattern validation (use precompiled regex for performance)
        if field_schema._compiled_pattern and isinstance(value, str):
            if not field_schema._compiled_pattern.fullmatch(value):
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=field_schema.name,
                    violation_type="pattern_mismatch",
                    expected=f"pattern: {field_schema.pattern}",
                    actual=f"'{value}'",
                    row_identifier=row_id
                ))
        
        # Enum validation with case handling and suggestions
        if field_schema.enum_values:
            enum_match = value in field_schema.enum_values
            coerced_enum = None
            
            # Try case-insensitive match if allowed and not in strict mode
            if (not enum_match and 
                not self.strict_mode and 
                field_schema.allow_enum_case_insensitive and 
                isinstance(value, str)):
                for enum_val in field_schema.enum_values:
                    if value.lower() == enum_val.lower():
                        coerced_enum = enum_val
                        break
            
            if not enum_match and not coerced_enum:
                # Get suggestions for better error messages
                suggestions = self._get_levenshtein_candidates(str(value), field_schema.enum_values)
                hint = f"did you mean: {', '.join(suggestions)}?" if suggestions else None
                
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=field_schema.name,
                    violation_type="invalid_enum_value",
                    expected=f"one of {sorted(field_schema.enum_values)}",
                    actual=f"'{value}'",
                    row_identifier=row_id,
                    expected_hint=hint
                ))
            elif coerced_enum:
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=field_schema.name,
                    violation_type="type_coerced",
                    expected="exact case match",
                    actual=f"'{value}' -> '{coerced_enum}'",
                    row_identifier=row_id,
                    severity="warning",
                    coerced_value=coerced_enum
                ))
                value = coerced_enum
        
        # List validation
        if field_schema.field_type == FieldType.LIST and isinstance(value, list):
            if field_schema.list_item_type:
                # Create a copy to potentially modify
                modified_list = list(value) if not self.strict_mode else value
                
                for i, item in enumerate(value):
                    item_violations = await self._validate_list_item(
                        field_schema, item, i, table_name, row_id, modified_list
                    )
                    violations.extend(item_violations)
                
                # Use modified list if coercions occurred
                if not self.strict_mode and modified_list != value:
                    value = modified_list
        
        # Dict validation  
        if field_schema.field_type == FieldType.DICT and isinstance(value, dict):
            dict_violations = await self._validate_dict_contents(
                field_schema, value, table_name, row_id
            )
            violations.extend(dict_violations)
            
            # dict_violations may have modified the dictionary if coercions occurred
            # _validate_dict_contents handles the mutation internally
        
        # Enhanced timestamp validation with enterprise-grade temporal consistency
        if field_schema.field_type == FieldType.TIMESTAMP_US and isinstance(value, int):
            # Basic bounds validation
            if value < self.min_timestamp_us or value > self.max_timestamp_us:
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=field_schema.name,
                    violation_type="timestamp_out_of_bounds",
                    expected=f"between {self.min_timestamp_us} and {self.max_timestamp_us}",
                    actual=str(value),
                    row_identifier=row_id
                ))
            
            # Enterprise-grade temporal consistency validation
            temporal_violations = self._validate_temporal_consistency(
                value, table_name, field_schema.name
            )
            violations.extend(temporal_violations)
        
        # Business hours validation for timestamps
        if (field_schema.business_hours_only and 
            field_schema.field_type == FieldType.TIMESTAMP_US and 
            isinstance(value, int)):
            if not self._validate_business_hours(value):
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=field_schema.name,
                    violation_type="business_hours_violation",
                    expected="timestamp within business hours (9 AM - 5 PM UTC, Mon-Fri)",
                    actual=f"timestamp {value}",
                    row_identifier=row_id
                ))
        
        # Checksum validation
        if field_schema.checksum_algorithm and isinstance(value, str):
            if not self._validate_checksum(value, field_schema.checksum_algorithm):
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=field_schema.name,
                    violation_type="checksum_validation_failed",
                    expected=f"valid {field_schema.checksum_algorithm} checksum",
                    actual=f"'{value}'",
                    row_identifier=row_id
                ))
        
        # Semantic validation
        if field_schema.semantic_validator and isinstance(value, str):
            if not self._validate_semantic(value, field_schema.semantic_validator):
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=field_schema.name,
                    violation_type="semantic_validation_failed",
                    expected=f"valid {field_schema.semantic_validator} format",
                    actual=f"'{value}'",
                    row_identifier=row_id
                ))
        
        # Custom validation (with error protection)
        if field_schema.custom_validator:
            try:
                if not field_schema.custom_validator(value):
                    violations.append(SchemaViolation(
                        table_name=table_name,
                        field_name=field_schema.name,
                        violation_type="custom_validation_failed",
                        expected="passes custom validator",
                        actual=f"'{value}'",
                        row_identifier=row_id
                    ))
            except Exception as e:
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=field_schema.name,
                    violation_type="custom_validator_error",
                    expected="custom validator to execute without error",
                    actual=f"validator crashed: {str(e)[:100]}",
                    row_identifier=row_id,
                    severity="error"
                ))
        
        return value, violations
    
    async def _validate_list_item(self, field_schema: FieldSchema, item: Any, index: int, 
                                 table_name: str, row_id: str, modified_list: Optional[list] = None) -> List[SchemaViolation]:
        """Validate individual list items."""
        violations = []
        
        if not field_schema.list_item_type:
            return violations
        
        # Create a temporary field schema for the list item
        item_schema = FieldSchema(
            name=f"{field_schema.name}[{index}]",
            field_type=field_schema.list_item_type,
            required=True,
            nullable=False
        )
        
        # Validate the item type
        coerced_item, item_violations = await self._validate_and_coerce_type(
            item_schema, item, table_name, row_id
        )
        
        if item_violations:
            # Pass through the correct violation type based on whether coercion succeeded
            if item_violations.coerced_value is not None:
                # Coercion succeeded - report as coerced, not mismatch
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=field_schema.name,
                    violation_type="list_item_type_coerced",
                    expected=f"item[{index}] of type {field_schema.list_item_type.value}",
                    actual=f"item[{index}] {item_violations.actual} -> {item_violations.coerced_value}",
                    row_identifier=row_id,
                    severity="warning",
                    coerced_value=item_violations.coerced_value
                ))
            else:
                # Coercion failed - report as mismatch
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=field_schema.name,
                    violation_type="list_item_type_mismatch",
                    expected=f"item[{index}] of type {field_schema.list_item_type.value}",
                    actual=f"item[{index}] {item_violations.actual}",
                    row_identifier=row_id,
                    severity=item_violations.severity
                ))
            
            # Apply coercion to the list if successful and not in strict mode
            if (item_violations.coerced_value is not None and 
                modified_list is not None and 
                not self.strict_mode):
                modified_list[index] = item_violations.coerced_value
        
        return violations
    
    async def _validate_dict_contents(self, field_schema: FieldSchema, value: dict, 
                                    table_name: str, row_id: str) -> List[SchemaViolation]:
        """Validate dictionary contents."""
        violations = []
        
        # Validate keys if pattern is specified
        if field_schema._compiled_dict_key_pattern:
            for key in value.keys():
                if not isinstance(key, str) or not field_schema._compiled_dict_key_pattern.fullmatch(key):
                    violations.append(SchemaViolation(
                        table_name=table_name,
                        field_name=field_schema.name,
                        violation_type="dict_key_pattern_mismatch",
                        expected=f"key matching pattern: {field_schema.dict_key_pattern}",
                        actual=f"key: '{key}'",
                        row_identifier=row_id
                    ))
        
        # Validate values if type is specified
        if field_schema.dict_value_type:
            for key, dict_value in value.items():
                # Create temporary schema for dict value
                value_schema = FieldSchema(
                    name=f"{field_schema.name}[{key}]",
                    field_type=field_schema.dict_value_type,
                    required=True,
                    nullable=False
                )
                
                coerced_value, value_violations = await self._validate_and_coerce_type(
                    value_schema, dict_value, table_name, row_id
                )
                
                if value_violations:
                    # Pass through the correct violation type based on whether coercion succeeded
                    if value_violations.coerced_value is not None:
                        # Coercion succeeded - report as coerced, not mismatch
                        violations.append(SchemaViolation(
                            table_name=table_name,
                            field_name=field_schema.name,
                            violation_type="dict_value_type_coerced",
                            expected=f"value[{key}] of type {field_schema.dict_value_type.value}",
                            actual=f"value[{key}] {value_violations.actual} -> {value_violations.coerced_value}",
                            row_identifier=row_id,
                            severity="warning",
                            coerced_value=value_violations.coerced_value
                        ))
                    else:
                        # Coercion failed - report as mismatch
                        violations.append(SchemaViolation(
                            table_name=table_name,
                            field_name=field_schema.name,
                            violation_type="dict_value_type_mismatch",
                            expected=f"value[{key}] of type {field_schema.dict_value_type.value}",
                            actual=f"value[{key}] {value_violations.actual}",
                            row_identifier=row_id,
                            severity=value_violations.severity
                        ))
                    
                    # Apply coercion to the dict if successful and not in strict mode
                    if (value_violations.coerced_value is not None and 
                        not self.strict_mode):
                        value[key] = value_violations.coerced_value
        
        return violations
    
    async def _validate_and_coerce_type(self, field_schema: FieldSchema, value: Any, table_name: str, row_id: str) -> tuple[Any, Optional[SchemaViolation]]:
        """Validate field type and attempt coercion if needed."""
        target_type_map = {
            FieldType.STRING: str,
            FieldType.INTEGER: int,
            FieldType.DECIMAL: Decimal,
            FieldType.BOOLEAN: bool,
            FieldType.TIMESTAMP_US: int,
            FieldType.ADDRESS: str,
            FieldType.HASH: str,
            FieldType.ENUM: str,
            FieldType.LIST: list,
            FieldType.DICT: dict
        }
        
        target_type = target_type_map.get(field_schema.field_type)
        if not target_type:
            return value, None
        
        # If already correct type
        if isinstance(value, target_type):
            return value, None
        
        # Special handling for Decimal (only if not in strict mode)
        if (field_schema.field_type == FieldType.DECIMAL and 
            isinstance(value, (int, float)) and 
            not self.strict_mode):
            coerced_decimal = Decimal(str(value))
            return coerced_decimal, SchemaViolation(
                table_name=table_name,
                field_name=field_schema.name,
                violation_type="type_coerced",
                expected=target_type.__name__,
                actual=f"{type(value).__name__}: {value}",
                row_identifier=row_id,
                severity="warning",
                coerced_value=coerced_decimal
            )
        
        # Special handling for TIMESTAMP_US to prioritize timestamp parsing over generic str→int
        if (field_schema.field_type == FieldType.TIMESTAMP_US and 
            isinstance(value, str) and 
            not self.strict_mode):
            timestamp_us = self._timestamp_from_string(value)
            if timestamp_us is not None:
                return timestamp_us, SchemaViolation(
                    table_name=table_name,
                    field_name=field_schema.name,
                    violation_type="type_coerced",
                    expected=target_type.__name__,
                    actual=f"{type(value).__name__}: {value}",
                    row_identifier=row_id,
                    severity="warning",
                    coerced_value=timestamp_us
                )
        
        # Try field-specific coercion rules first (if not in strict mode)
        if not self.strict_mode:
            for rule in field_schema.coercion_rules:
                if isinstance(value, rule.from_type):
                    try:
                        coerced = rule.coercer(value)
                        if coerced is not None and isinstance(coerced, target_type):
                            return coerced, SchemaViolation(
                                table_name=table_name,
                                field_name=field_schema.name,
                                violation_type="type_coerced",
                                expected=target_type.__name__,
                                actual=f"{type(value).__name__}: {value}",
                                row_identifier=row_id,
                                severity="warning",
                                coerced_value=coerced
                            )
                    except Exception:
                        continue
            
            # Try default coercion rules
            for rule in self.default_coercion_rules:
                if isinstance(value, rule.from_type) and rule.to_type == target_type:
                    try:
                        coerced = rule.coercer(value)
                        if coerced is not None and isinstance(coerced, target_type):
                            return coerced, SchemaViolation(
                                table_name=table_name,
                                field_name=field_schema.name,
                                violation_type="type_coerced",
                                expected=target_type.__name__,
                                actual=f"{type(value).__name__}: {value}",
                                row_identifier=row_id,
                                severity="warning",
                                coerced_value=coerced
                            )
                    except Exception:
                        continue
        
        # Coercion failed
        return value, SchemaViolation(
            table_name=table_name,
            field_name=field_schema.name,
            violation_type="type_mismatch",
            expected=target_type.__name__,
            actual=f"{type(value).__name__}: {value}",
            row_identifier=row_id
        )
    
    async def _validate_referential_integrity(self, schema: TableSchema, row: Dict[str, Any], table_name: str, row_id: str) -> List[SchemaViolation]:
        """Validate foreign key constraints."""
        violations = []
        
        for field_name, ref_spec in schema.foreign_keys.items():
            if field_name not in row:
                continue
            
            value = row[field_name]
            if value is None:
                continue
            
            # Parse reference specification: "table.field"
            if '.' not in ref_spec:
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=field_name,
                    violation_type="invalid_foreign_key_spec",
                    expected="format: table.field",
                    actual=ref_spec,
                    row_identifier=row_id,
                    severity="error"
                ))
                continue
            
            ref_table, ref_field = ref_spec.split('.', 1)
            
            # Check if reference data is available
            if ref_table not in self.reference_data:
                severity = "error" if schema.strict_foreign_keys else "warning"
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=field_name,
                    violation_type="missing_reference_data",
                    expected=f"reference data for {ref_table}",
                    actual="no reference data loaded",
                    row_identifier=row_id,
                    severity=severity
                ))
                continue
            
            # Check if specific field reference data is available
            if ref_field not in self.reference_data[ref_table]:
                severity = "error" if schema.strict_foreign_keys else "warning"
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=field_name,
                    violation_type="missing_reference_field_data",
                    expected=f"reference data for {ref_table}.{ref_field}",
                    actual=f"no reference data for field {ref_field}",
                    row_identifier=row_id,
                    severity=severity
                ))
                continue
            
            # Validate foreign key
            ref_values = self.reference_data[ref_table][ref_field]
            if value not in ref_values:
                # Get suggestions for better error messages
                if isinstance(value, str):
                    suggestions = self._get_levenshtein_candidates(value, {str(v) for v in ref_values})
                    hint = f"did you mean: {', '.join(suggestions)}?" if suggestions else None
                else:
                    hint = None
                
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=field_name,
                    violation_type="foreign_key_violation",
                    expected=f"value exists in {ref_spec}",
                    actual=f"'{value}' not found in reference table",
                    row_identifier=row_id,
                    expected_hint=hint,
                    reference_info=f"Available values: {len(ref_values)} total"
                ))
        
        return violations
    
    async def validate_batch(self, table_name: str, rows: List[Dict[str, Any]]) -> ValidationSummary:
        """Validate a batch of rows and return summary."""
        violations = []
        passed_count = 0
        failed_count = 0
        coerced_count = 0
        violations_by_type = {}
        field_violation_counts = {}
        first_error_examples = {}
        
        # Track batch data for batch constraints
        cleaned_rows = []
        row_identifiers = []
        
        # Track unique constraints
        constraint_trackers = {}
        if table_name in self.schemas:
            schema = self.schemas[table_name]
            # Initialize constraint trackers as dicts to store tuple->first_row_id
            for constraint in schema.unique_constraints:
                constraint_trackers[tuple(constraint)] = {}
            if schema.primary_key:
                # Only add primary key if it's not already covered by unique constraints
                pk_tuple = tuple(schema.primary_key)
                if pk_tuple not in constraint_trackers:
                    constraint_trackers[pk_tuple] = {}
        
        for i, row in enumerate(rows):
            # Generate stable row identifier
            row_id = self._generate_row_identifier(table_name, row, i)
            
            cleaned_row, row_violations, row_flags = await self.validate_row(table_name, row, row_id)
            
            # Store for batch constraint validation
            cleaned_rows.append(cleaned_row)
            row_identifiers.append(row_id)
            
            # Check unique constraints after row validation
            if table_name in self.schemas:
                constraint_violations = self._check_unique_constraints(
                    self.schemas[table_name], cleaned_row, row_id, constraint_trackers
                )
                row_violations.extend(constraint_violations)
                for v in constraint_violations:
                    row_flags.had_unique_constraint_error = True
            
            # Categorize result
            has_errors = any(v.severity == "error" for v in row_violations)
            has_coercions = any(v.violation_type.endswith("_coerced") for v in row_violations)
            
            if has_errors:
                failed_count += 1
            elif has_coercions:
                coerced_count += 1
            else:
                passed_count += 1
            
            # Aggregate violation statistics
            for violation in row_violations:
                violations_by_type[violation.violation_type] = violations_by_type.get(violation.violation_type, 0) + 1
                
                if violation.field_name:
                    field_violation_counts[violation.field_name] = field_violation_counts.get(violation.field_name, 0) + 1
                
                if violation.violation_type not in first_error_examples:
                    first_error_examples[violation.violation_type] = violation
            
            violations.extend(row_violations)
        
        # Sort fields by violation count
        fields_with_most_violations = sorted(field_violation_counts.keys(), 
                                           key=lambda f: field_violation_counts[f], 
                                           reverse=True)[:10]
        
        # Validate batch-level constraints
        if table_name in self.schemas:
            batch_violations = self._validate_batch_constraints(
                self.schemas[table_name], cleaned_rows, row_identifiers
            )
            violations.extend(batch_violations)
            
            # Update statistics with batch violations
            for violation in batch_violations:
                violations_by_type[violation.violation_type] = violations_by_type.get(violation.violation_type, 0) + 1
                
                if violation.field_name:
                    field_violation_counts[violation.field_name] = field_violation_counts.get(violation.field_name, 0) + 1
                
                if violation.violation_type not in first_error_examples:
                    first_error_examples[violation.violation_type] = violation
        
        summary = ValidationSummary(
            table_name=table_name,
            total_rows=len(rows),
            passed_rows=passed_count,
            failed_rows=failed_count,
            coerced_rows=coerced_count,
            violations=violations,
            violations_by_type=violations_by_type,
            fields_with_most_violations=fields_with_most_violations,
            first_error_examples=first_error_examples
        )
        
        # Update Prometheus metrics
        if METRICS_AVAILABLE and _metrics_collector:
            if failed_count > 0:
                _metrics_collector.increment_counter('schema_validation_total', labels={'table_name': table_name, 'status': 'failed', 'venue': ''})
            else:
                _metrics_collector.increment_counter('schema_validation_total', labels={'table_name': table_name, 'status': 'success', 'venue': ''})
            
            # Record violations by type
            for violation_type, count in violations_by_type.items():
                _metrics_collector.increment_counter('schema_violations_total', value=count, labels={'table_name': table_name, 'violation_type': violation_type, 'venue': ''})
        
        # Streaming Bus: Publish validation summary to clean.pass_fail
        try:
            validation_result = {
                "table_name": table_name,
                "timestamp": int(time.time() * 1_000_000),  # Current time in microseconds
                "total_rows": summary.total_rows,
                "passed_rows": summary.passed_rows,
                "failed_rows": summary.failed_rows,
                "coerced_rows": summary.coerced_rows,
                "pass_rate": summary.passed_rows / summary.total_rows if summary.total_rows > 0 else 0,
                "violations_by_type": summary.violations_by_type,
                "fields_with_most_violations": summary.fields_with_most_violations,
                "result": "PASS" if summary.failed_rows == 0 else "FAIL"
            }
            
            # Use table name as partition key for schema locality
            partition_key = f"schema_validation_{table_name}"
            
            # Get sequence number for validation results (using source_id as key)
            source_id_pass_fail = f"schema_validator.{table_name}"
            self._sequence_numbers[source_id_pass_fail] += 1
            
            await self.streaming_bus.publish_with_canonical_headers(
                topic="clean.pass_fail",
                partition_key=partition_key,
                payload=validation_result,
                source_id=source_id_pass_fail,
                sequence_number=self._sequence_numbers[source_id_pass_fail],
                producer_version="2.0.0"
            )
            
            # Also publish individual violations to incidents.SchemaViolation
            for violation in violations[:100]:  # Limit to first 100 violations
                violation_data = {
                    "table_name": table_name,
                    "timestamp": violation.timestamp_utc_us,
                    "row_id": violation.row_identifier,
                    "field_name": violation.field_name,
                    "violation_type": violation.violation_type,
                    "message": violation.expected_hint or violation.reference_info or f"Expected {violation.expected}",
                    "invalid_value": str(violation.actual or violation.coerced_value)[:1000] if (violation.actual is not None or violation.coerced_value is not None) else None,
                    "expected_type": violation.expected,
                    "severity": violation.severity.upper()
                }
                
                # Get sequence number for incidents (using source_id as key for this topic too)
                source_id_incidents = f"schema_validator.{table_name}.incidents"
                self._sequence_numbers[source_id_incidents] += 1
                
                await self.streaming_bus.publish_with_canonical_headers(
                    topic="incidents.SchemaViolation",
                    partition_key=partition_key,
                    payload=violation_data,
                    source_id=source_id_incidents,
                    sequence_number=self._sequence_numbers[source_id_incidents],
                    correlation_id=f"{table_name}_{violation.row_identifier}",  # Link violations to same row
                    producer_version="2.0.0"
                )
                
        except Exception as e:
            logger.warning(f"Failed to publish validation results to streaming bus: {e}")
        
        return summary
    
    def _check_unique_constraints(self, schema: TableSchema, row: Dict[str, Any], row_id: str, 
                                 constraint_trackers: Dict[Tuple[str, ...], Dict[Tuple, str]]) -> List[SchemaViolation]:
        """Check unique constraints for a row."""
        violations = []
        
        for constraint_fields in constraint_trackers.keys():
            # Build tuple of values for this constraint
            constraint_values = []
            all_present = True
            
            for field in constraint_fields:
                if field not in row or row[field] is None:
                    all_present = False
                    break
                constraint_values.append(row[field])
            
            if not all_present:
                continue
            
            constraint_tuple = tuple(constraint_values)
            
            # Check if we've seen this combination before
            if constraint_tuple in constraint_trackers[constraint_fields]:
                first_row_id = constraint_trackers[constraint_fields][constraint_tuple]
                
                # Determine if this is a primary key or unique constraint violation
                is_primary_key = (schema.primary_key and 
                                tuple(schema.primary_key) == constraint_fields)
                
                violation_type = "primary_key_violation" if is_primary_key else "unique_constraint_violation"
                constraint_name = "primary key" if is_primary_key else "unique constraint"
                
                violations.append(SchemaViolation(
                    table_name=schema.name,
                    field_name=",".join(constraint_fields),
                    violation_type=violation_type,
                    expected=f"unique {constraint_name} combination",
                    actual=f"duplicate: {constraint_tuple}",
                    row_identifier=row_id,
                    severity="error",
                    reference_info=f"first_row_identifier={first_row_id}"
                ))
            else:
                constraint_trackers[constraint_fields][constraint_tuple] = row_id
        
        return violations
    
    def _validate_batch_constraints(self, schema: TableSchema, cleaned_rows: List[Dict[str, Any]], 
                                   row_identifiers: List[str]) -> List[SchemaViolation]:
        """Validate batch-level constraints across all rows in the batch."""
        violations = []
        
        if not schema.batch_constraints:
            return violations
        
        for constraint_type in schema.batch_constraints:
            if constraint_type == "unique_in_batch":
                # This is already handled by the regular unique constraint logic
                continue
                
            elif constraint_type == "sum_equals_zero":
                # Only run on complete partitions to avoid false positives
                if not schema.is_complete_partition:
                    continue
                    
                # Validate that specific numeric fields sum to zero across the batch
                # Only check fields that are likely to represent amounts/balances
                amount_fields = [f for f in schema.fields 
                               if f.field_type in [FieldType.DECIMAL, FieldType.INTEGER] and
                               any(keyword in f.name.lower() for keyword in ['amount', 'balance', 'value', 'debit', 'credit'])]
                
                for field_schema in amount_fields:
                    field_name = field_schema.name
                    total = 0
                    count = 0
                    
                    for i, row in enumerate(cleaned_rows):
                        if field_name in row and row[field_name] is not None:
                            try:
                                total += float(row[field_name])
                                count += 1
                            except (ValueError, TypeError):
                                continue
                    
                    if count > 0 and abs(total) > 0.001:  # Allow for small floating point errors
                        violations.append(SchemaViolation(
                            table_name=schema.name,
                            field_name=field_name,
                            violation_type="batch_sum_not_zero",
                            expected="sum equals zero across batch",
                            actual=f"sum = {total} ({count} values)",
                            row_identifier="batch_constraint",
                            severity="error"
                        ))
            
            elif constraint_type == "sequence_continuous":
                # Only run on complete partitions to avoid false positives
                if not schema.is_complete_partition:
                    continue
                    
                # Validate that ID-like integer sequences are continuous (no gaps)
                id_fields = [f for f in schema.fields 
                           if f.field_type == FieldType.INTEGER and
                           any(keyword in f.name.lower() for keyword in ['id', 'sequence', 'number', 'index'])]
                
                for field_schema in id_fields:
                    field_name = field_schema.name
                    values = []
                    
                    for i, row in enumerate(cleaned_rows):
                        if field_name in row and row[field_name] is not None:
                            try:
                                values.append((int(row[field_name]), row_identifiers[i]))
                            except (ValueError, TypeError):
                                continue
                    
                    if len(values) > 1:
                        values.sort(key=lambda x: x[0])  # Sort by value
                        
                        for i in range(1, len(values)):
                            if values[i][0] != values[i-1][0] + 1:
                                violations.append(SchemaViolation(
                                    table_name=schema.name,
                                    field_name=field_name,
                                    violation_type="batch_sequence_gap",
                                    expected="continuous sequence",
                                    actual=f"gap between {values[i-1][0]} and {values[i][0]}",
                                    row_identifier=values[i][1],
                                    severity="warning"
                                ))
            
            elif constraint_type == "monotonic_increasing":
                # Gate monotonic checks on complete partition to avoid noise on partial slices
                if not schema.is_complete_partition:
                    continue  # Skip monotonic checks on partial data
                    
                # Validate that time-like values are monotonically increasing within the batch
                time_fields = [f for f in schema.fields 
                             if f.field_type in [FieldType.DECIMAL, FieldType.INTEGER, FieldType.TIMESTAMP_US] and
                             any(keyword in f.name.lower() for keyword in ['time', 'timestamp', 'created', 'updated', 'block_number'])]
                
                for field_schema in time_fields:
                    field_name = field_schema.name
                    values = []
                    
                    for i, row in enumerate(cleaned_rows):
                        if field_name in row and row[field_name] is not None:
                            try:
                                # Use int for integer/timestamp fields to avoid precision loss
                                if field_schema.field_type in [FieldType.INTEGER, FieldType.TIMESTAMP_US]:
                                    values.append((int(row[field_name]), row_identifiers[i]))
                                else:
                                    values.append((float(row[field_name]), row_identifiers[i]))
                            except (ValueError, TypeError):
                                continue
                    
                    if len(values) > 1:
                        for i in range(1, len(values)):
                            try:
                                # Ensure comparable types for temporal ordering
                                prev_val = values[i-1][0]
                                curr_val = values[i][0]
                                
                                # Convert to same numeric type if needed
                                if isinstance(prev_val, str) and isinstance(curr_val, (int, float)):
                                    try:
                                        prev_val = float(prev_val)
                                    except ValueError:
                                        continue
                                elif isinstance(curr_val, str) and isinstance(prev_val, (int, float)):
                                    try:
                                        curr_val = float(curr_val)
                                    except ValueError:
                                        continue
                                elif isinstance(prev_val, str) and isinstance(curr_val, str):
                                    # Try to convert both to float
                                    try:
                                        prev_val = float(prev_val)
                                        curr_val = float(curr_val)
                                    except ValueError:
                                        continue
                                
                                # Only compare if both are numeric
                                if isinstance(prev_val, (int, float)) and isinstance(curr_val, (int, float)):
                                    if curr_val < prev_val:
                                        violations.append(SchemaViolation(
                                            table_name=schema.name,
                                            field_name=field_name,
                                            violation_type="batch_not_monotonic",
                                            expected="monotonically increasing values",
                                            actual=f"{prev_val} > {curr_val} at position {i}",
                                            row_identifier=values[i][1],
                                            severity="warning"
                                        ))
                            except (TypeError, ValueError) as e:
                                # Skip comparison if types are incompatible
                                logger.debug(f"Skipping temporal comparison for incompatible types: {e}")
                                continue
        
        return violations
    
    async def _register_circuit_breaker(self):
        """Register circuit breaker with the streaming bus."""
        if not self._circuit_breaker_registered:
            try:
                await self.streaming_bus.register_circuit_breaker(
                    component_id=self.circuit_breaker_id,
                    failure_threshold=self.config.get('circuit_breaker_failure_threshold', 5),
                    recovery_timeout_us=self.config.get('circuit_breaker_recovery_timeout_us', 300_000_000),
                    dependency_components=self.config.get('circuit_breaker_dependencies', [])
                )
                self._circuit_breaker_registered = True
                logger.info(f"Circuit breaker registered for {self.circuit_breaker_id}")
            except Exception as e:
                logger.warning(f"Failed to register circuit breaker: {e}")
    
    async def _perform_health_check(self):
        """Perform health check and update metrics."""
        try:
            # Check streaming bus health via system status
            system_health = await self.streaming_bus.get_system_health_status()
            bus_health = system_health.get('streaming_bus_healthy', True)
            
            # Check schema registry health (verify we have schemas loaded)
            schema_health = len(self.schemas) > 0
            
            # Check task health
            task_health = (
                (self._consumer_task is None or not self._consumer_task.done()) and
                len(self._pending_validation_tasks) < 1000  # Reasonable task backlog
            )
            
            overall_health = bus_health and schema_health and task_health
            
            # Update metrics
            self.metrics['health_checks_performed'] += 1
            self._last_health_check = time.time()
            
            return overall_health
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
    
    async def _health_monitor_loop(self):
        """Background health monitoring loop."""
        while self.running:
            try:
                await self._perform_health_check()
                await asyncio.sleep(self._health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health monitor error: {e}")
                await asyncio.sleep(self._health_check_interval)
    
    async def _retry_with_backoff(self, operation_func, operation_name: str, *args, **kwargs):
        """Execute operation with exponential backoff retry."""
        last_exception = None
        
        for attempt in range(self.retry_config['max_retries'] + 1):
            try:
                # Check circuit breaker
                if self._circuit_breaker_registered:
                    can_execute = await self.streaming_bus.can_component_execute(self.circuit_breaker_id)
                    if not can_execute:
                        self.metrics['circuit_breaker_trips'] += 1
                        raise Exception("Circuit breaker is open")
                
                # Execute operation
                result = await operation_func(*args, **kwargs)
                
                # Record success
                if self._circuit_breaker_registered:
                    await self.streaming_bus.record_component_success(self.circuit_breaker_id)
                
                return result
                
            except Exception as e:
                last_exception = e
                self.metrics['retry_attempts'] += 1
                
                # Record failure
                if self._circuit_breaker_registered:
                    await self.streaming_bus.record_component_failure(self.circuit_breaker_id, cascade_failure=True)
                
                if attempt < self.retry_config['max_retries']:
                    # Calculate backoff delay
                    delay = min(
                        self.retry_config['base_delay'] * (self.retry_config['exponential_base'] ** attempt),
                        self.retry_config['max_delay']
                    )
                    logger.warning(f"Attempt {attempt + 1} failed for {operation_name}: {e}. Retrying in {delay:.1f}s")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"All {self.retry_config['max_retries']} retry attempts failed for {operation_name}")
        
        if last_exception:
            raise last_exception
        else:
            raise Exception(f"Operation {operation_name} failed with no recorded exception")
    
    def _update_validation_metrics(self, validation_time_ms: float, passed: bool, violations_count: int, table_name: str = 'unknown'):
        """Update validation metrics."""
        self.metrics['validations_processed'] += 1
        if passed:
            self.metrics['validations_passed'] += 1
            # Increment Prometheus counter for successful validations
            if METRICS_AVAILABLE and _metrics_collector:
                _metrics_collector.increment_counter('schema_validation_total', labels={'table_name': table_name, 'status': 'success', 'venue': ''})
        else:
            self.metrics['validations_failed'] += 1
            # Increment Prometheus counter for failed validations
            if METRICS_AVAILABLE and _metrics_collector:
                _metrics_collector.increment_counter('schema_validation_total', labels={'table_name': table_name, 'status': 'failed', 'venue': ''})
        
        self.metrics['schema_violations'] += violations_count
        # Increment Prometheus counter for violations
        if violations_count > 0 and METRICS_AVAILABLE and _metrics_collector:
            _metrics_collector.increment_counter('schema_violations_total', value=violations_count, labels={'table_name': table_name, 'violation_type': 'general', 'venue': ''})
        
        self.metrics['last_validation_timestamp'] = time.time()
        
        # Update running average of validation time
        current_avg = self.metrics['avg_validation_time_ms']
        processed = self.metrics['validations_processed']
        self.metrics['avg_validation_time_ms'] = ((current_avg * (processed - 1)) + validation_time_ms) / processed
    
    def _register_data_ingestion_schemas(self):
        """Register schemas for all data ingestion topics consumed by this agent."""
        logger.info("Registering data ingestion schemas...")
        
        # Exchange trades schema (from exchange_connector output)
        exchange_trades_schema = TableSchema(
            name="exchange_trades",
            fields=[
                FieldSchema(
                    name="venue",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=50,
                    trim_whitespace=True
                ),
                FieldSchema(
                    name="symbol",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=20,
                    pattern=r"^[A-Z0-9]{2,}[/-]?[A-Z0-9]{2,}$",
                    trim_whitespace=True
                ),
                FieldSchema(
                    name="data_type",
                    field_type=FieldType.ENUM,
                    required=True,
                    enum_values={"trades"}
                ),
                FieldSchema(
                    name="timestamp",
                    field_type=FieldType.TIMESTAMP_US,
                    required=True
                ),
                FieldSchema(
                    name="capture_timestamp",
                    field_type=FieldType.TIMESTAMP_US,
                    required=True
                ),
                FieldSchema(
                    name="side",
                    field_type=FieldType.ENUM,
                    required=True,
                    enum_values={"buy", "sell"},
                    allow_enum_case_insensitive=True
                ),
                FieldSchema(
                    name="quantity",
                    field_type=FieldType.STRING,  # Decimal as string for precision
                    required=True,
                    pattern=r"^\d+(\.\d+)?([eE][+-]?\d+)?$"  # Positive decimal (including scientific notation)
                ),
                FieldSchema(
                    name="price",
                    field_type=FieldType.STRING,  # Decimal as string for precision
                    required=True,
                    pattern=r"^\d+(\.\d+)?([eE][+-]?\d+)?$"  # Positive decimal (including scientific notation)
                ),
                FieldSchema(
                    name="trade_id",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=100
                )
            ],
            primary_key=["venue", "symbol", "trade_id"],
            foreign_keys={
                "venue": "venues.name",
                "symbol": "symbols.symbol"
            },
            cross_field_rules=[
                CrossFieldRule(
                    rule_type="temporal_ordering",
                    fields=["timestamp", "capture_timestamp"],
                    error_message="timestamp must be <= capture_timestamp",
                    severity="warning"
                )
            ],
            strict_foreign_keys=False  # Don't fail on missing reference data
        )
        self.register_schema(exchange_trades_schema)
        
        # Market trades schema (granular market data)
        market_trades_schema = TableSchema(
            name="market_trades",
            fields=[
                FieldSchema(
                    name="venue",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=50
                ),
                FieldSchema(
                    name="symbol",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=20
                ),
                FieldSchema(
                    name="timestamp",
                    field_type=FieldType.TIMESTAMP_US,
                    required=True
                ),
                FieldSchema(
                    name="price",
                    field_type=FieldType.STRING,
                    required=True,
                    pattern=r"^\d+(\.\d+)?$"
                ),
                FieldSchema(
                    name="quantity",
                    field_type=FieldType.STRING,
                    required=True,
                    pattern=r"^\d+(\.\d+)?([eE][+-]?\d+)?$"  # Positive decimal (including scientific notation)
                ),
                FieldSchema(
                    name="side",
                    field_type=FieldType.ENUM,
                    required=True,
                    enum_values={"buy", "sell"},
                    allow_enum_case_insensitive=True
                ),
                FieldSchema(
                    name="trade_id",
                    field_type=FieldType.STRING,
                    required=True
                )
            ],
            primary_key=["venue", "symbol", "trade_id"],
            foreign_keys={
                "venue": "venues.name",
                "symbol": "symbols.symbol"
            },
            strict_foreign_keys=False
        )
        self.register_schema(market_trades_schema)
        
        # Market book schema
        market_book_schema = TableSchema(
            name="market_book",
            fields=[
                FieldSchema(
                    name="venue",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=50
                ),
                FieldSchema(
                    name="symbol",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=20
                ),
                FieldSchema(
                    name="timestamp",
                    field_type=FieldType.TIMESTAMP_US,
                    required=True
                ),
                FieldSchema(
                    name="bids",
                    field_type=FieldType.LIST,
                    required=True,
                    list_item_type=FieldType.DICT
                ),
                FieldSchema(
                    name="asks",
                    field_type=FieldType.LIST,
                    required=True,
                    list_item_type=FieldType.DICT
                ),
                FieldSchema(
                    name="sequence_number",
                    field_type=FieldType.INTEGER,
                    nullable=True
                )
            ],
            primary_key=["venue", "symbol", "timestamp"],
            strict_foreign_keys=False
        )
        self.register_schema(market_book_schema)
        
        # Market funding schema
        market_funding_schema = TableSchema(
            name="market_funding",
            fields=[
                FieldSchema(
                    name="venue",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=50
                ),
                FieldSchema(
                    name="symbol",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=20
                ),
                FieldSchema(
                    name="timestamp",
                    field_type=FieldType.TIMESTAMP_US,
                    required=True
                ),
                FieldSchema(
                    name="funding_rate",
                    field_type=FieldType.STRING,
                    required=True,
                    pattern=r"^-?\d+(\.\d+)?$"  # Allow negative rates
                ),
                FieldSchema(
                    name="next_funding_time",
                    field_type=FieldType.TIMESTAMP_US,
                    nullable=True
                )
            ],
            primary_key=["venue", "symbol", "timestamp"],
            strict_foreign_keys=False
        )
        self.register_schema(market_funding_schema)
        
        # Market open interest schema
        market_oi_schema = TableSchema(
            name="market_oi",
            fields=[
                FieldSchema(
                    name="venue",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=50
                ),
                FieldSchema(
                    name="symbol",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=20
                ),
                FieldSchema(
                    name="timestamp",
                    field_type=FieldType.TIMESTAMP_US,
                    required=True
                ),
                FieldSchema(
                    name="open_interest",
                    field_type=FieldType.STRING,
                    required=True,
                    pattern=r"^\d+(\.\d+)?$"
                ),
                FieldSchema(
                    name="open_interest_usd",
                    field_type=FieldType.STRING,
                    nullable=True,
                    pattern=r"^\d+(\.\d+)?$"
                )
            ],
            primary_key=["venue", "symbol", "timestamp"],
            strict_foreign_keys=False
        )
        self.register_schema(market_oi_schema)
        
        # Options surface schema
        options_surface_schema = TableSchema(
            name="options_surface",
            fields=[
                FieldSchema(
                    name="venue",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=50
                ),
                FieldSchema(
                    name="underlying",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=20
                ),
                FieldSchema(
                    name="timestamp",
                    field_type=FieldType.TIMESTAMP_US,
                    required=True
                ),
                FieldSchema(
                    name="expiry",
                    field_type=FieldType.TIMESTAMP_US,
                    required=True
                ),
                FieldSchema(
                    name="strike",
                    field_type=FieldType.STRING,
                    required=True,
                    pattern=r"^\d+(\.\d+)?$"
                ),
                FieldSchema(
                    name="option_type",
                    field_type=FieldType.ENUM,
                    required=True,
                    enum_values={"call", "put"},
                    allow_enum_case_insensitive=True
                ),
                FieldSchema(
                    name="implied_volatility",
                    field_type=FieldType.STRING,
                    nullable=True,
                    pattern=r"^\d+(\.\d+)?$"
                ),
                FieldSchema(
                    name="bid",
                    field_type=FieldType.STRING,
                    nullable=True,
                    pattern=r"^\d+(\.\d+)?$"
                ),
                FieldSchema(
                    name="ask",
                    field_type=FieldType.STRING,
                    nullable=True,
                    pattern=r"^\d+(\.\d+)?$"
                )
            ],
            primary_key=["venue", "underlying", "expiry", "strike", "option_type"],
            cross_field_rules=[
                CrossFieldRule(
                    rule_type="temporal_ordering",
                    fields=["timestamp", "expiry"],
                    error_message="timestamp must be <= expiry",
                    severity="error"
                )
            ],
            strict_foreign_keys=False
        )
        self.register_schema(options_surface_schema)
        
        # OnChain flows schema (from onchain_collector)
        onchain_flows_schema = TableSchema(
            name="onchain_flows",
            fields=[
                FieldSchema(
                    name="chain",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=20,
                    enum_values={"ethereum", "bitcoin", "polygon", "arbitrum", "optimism", "base"},
                    allow_enum_case_insensitive=True
                ),
                FieldSchema(
                    name="data_type",
                    field_type=FieldType.STRING,
                    required=False,
                    nullable=True,
                    max_length=20
                ),
                FieldSchema(
                    name="event_type",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=50,
                    enum_values={"erc20_transfer", "dex_swap", "cex_hot_wallet", "bridge", "lst", "lrt"}
                ),
                FieldSchema(
                    name="tx_hash",
                    field_type=FieldType.HASH,
                    required=True,
                    pattern=r"^0x[a-fA-F0-9]{64}$",
                    coercion_rules=[LOWERCASE_HASH_RULE]
                ),
                FieldSchema(
                    name="block_number",
                    field_type=FieldType.INTEGER,
                    required=True,
                    min_value=0
                ),
                FieldSchema(
                    name="timestamp_utc_us",
                    field_type=FieldType.TIMESTAMP_US,
                    required=True
                ),
                FieldSchema(
                    name="timestamp",
                    field_type=FieldType.TIMESTAMP_US,
                    required=False,
                    nullable=True
                ),
                FieldSchema(
                    name="from_address",
                    field_type=FieldType.ADDRESS,
                    required=True,
                    pattern=r"^0x[a-fA-F0-9]{40}$",
                    coercion_rules=[LOWERCASE_ADDRESS_RULE]
                ),
                FieldSchema(
                    name="to_address",
                    field_type=FieldType.ADDRESS,
                    required=True,
                    pattern=r"^0x[a-fA-F0-9]{40}$",
                    coercion_rules=[LOWERCASE_ADDRESS_RULE]
                ),
                FieldSchema(
                    name="token",
                    field_type=FieldType.STRING,
                    nullable=True,
                    max_length=100  # Increased for token addresses
                ),
                FieldSchema(
                    name="amount",
                    field_type=FieldType.STRING,
                    nullable=True,
                    pattern=r"^(\d+(\.\d+)?|\d+\.?\d*[eE][+-]?\d+)$"  # Supports both decimal and scientific notation
                ),
                FieldSchema(
                    name="value_usd",
                    field_type=FieldType.STRING,
                    required=False,  # Bronze layer - calculated in Silver
                    nullable=True,
                    pattern=r"^(\d+(\.\d+)?|\d+\.?\d*[eE][+-]?\d+)$"  # Supports both decimal and scientific notation
                ),
                FieldSchema(
                    name="capture_timestamp",
                    field_type=FieldType.TIMESTAMP_US,
                    required=False,
                    nullable=True
                ),
                FieldSchema(
                    name="finalized",
                    field_type=FieldType.BOOLEAN,
                    required=False,
                    nullable=True
                ),
                FieldSchema(
                    name="reorg_depth",
                    field_type=FieldType.INTEGER,
                    required=False,
                    nullable=True,
                    min_value=0
                ),
                FieldSchema(
                    name="block_hash",
                    field_type=FieldType.HASH,
                    required=False,
                    nullable=True,
                    pattern=r"^0x[a-fA-F0-9]{64}$",
                    coercion_rules=[LOWERCASE_HASH_RULE]
                ),
                FieldSchema(
                    name="extra",
                    field_type=FieldType.DICT,
                    required=False,
                    nullable=True
                )
            ],
            primary_key=["chain", "tx_hash", "block_number"],  # Updated to include block_number for better uniqueness
            foreign_keys={},  # Removed chain FK since we don't have reference data loaded
            batch_constraints=["monotonic_increasing"],  # Block numbers should increase
            strict_foreign_keys=False,
            allow_extra_fields=True,  # Allow metadata fields not explicitly defined
            unexpected_field_severity="info"  # Don't fail validation for extra fields
        )
        self.register_schema(onchain_flows_schema)
        
        # OnChain blocks schema
        onchain_blocks_schema = TableSchema(
            name="onchain_blocks",
            fields=[
                FieldSchema(
                    name="chain",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=20
                ),
                FieldSchema(
                    name="block_number",
                    field_type=FieldType.INTEGER,
                    required=True,
                    min_value=0
                ),
                FieldSchema(
                    name="block_hash",
                    field_type=FieldType.HASH,
                    required=True,
                    pattern=r"^0x[a-fA-F0-9]{64}$",
                    coercion_rules=[LOWERCASE_HASH_RULE]
                ),
                FieldSchema(
                    name="timestamp",
                    field_type=FieldType.TIMESTAMP_US,
                    required=True
                ),
                FieldSchema(
                    name="transaction_count",
                    field_type=FieldType.INTEGER,
                    required=True,
                    min_value=0
                ),
                FieldSchema(
                    name="gas_used",
                    field_type=FieldType.INTEGER,
                    nullable=True,
                    min_value=0
                ),
                FieldSchema(
                    name="gas_limit",
                    field_type=FieldType.INTEGER,
                    nullable=True,
                    min_value=0
                )
            ],
            primary_key=["chain", "block_number"],
            unique_constraints=[["chain", "block_hash"]],
            batch_constraints=["monotonic_increasing"],
            strict_foreign_keys=False
        )
        self.register_schema(onchain_blocks_schema)
        
        # OnChain mempool schema
        onchain_mempool_schema = TableSchema(
            name="onchain_mempool",
            fields=[
                FieldSchema(
                    name="chain",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=20
                ),
                FieldSchema(
                    name="tx_hash",
                    field_type=FieldType.HASH,
                    required=True,
                    pattern=r"^0x[a-fA-F0-9]{64}$",
                    coercion_rules=[LOWERCASE_HASH_RULE]
                ),
                FieldSchema(
                    name="timestamp",
                    field_type=FieldType.TIMESTAMP_US,
                    required=True
                ),
                FieldSchema(
                    name="gas_price",
                    field_type=FieldType.STRING,
                    required=True,
                    pattern=r"^\d+(\.\d+)?$"
                ),
                FieldSchema(
                    name="gas_limit",
                    field_type=FieldType.INTEGER,
                    required=True,
                    min_value=0
                ),
                FieldSchema(
                    name="nonce",
                    field_type=FieldType.INTEGER,
                    required=True,
                    min_value=0
                ),
                FieldSchema(
                    name="from_address",
                    field_type=FieldType.ADDRESS,
                    required=True,
                    pattern=r"^0x[a-fA-F0-9]{40}$",
                    coercion_rules=[LOWERCASE_ADDRESS_RULE]
                ),
                FieldSchema(
                    name="to_address",
                    field_type=FieldType.ADDRESS,
                    nullable=True,
                    pattern=r"^0x[a-fA-F0-9]{40}$",
                    coercion_rules=[LOWERCASE_ADDRESS_RULE]
                )
            ],
            primary_key=["chain", "tx_hash"],
            strict_foreign_keys=False
        )
        self.register_schema(onchain_mempool_schema)
        
        # OffChain events schema
        offchain_events_schema = TableSchema(
            name="offchain_events",
            fields=[
                FieldSchema(
                    name="event_type",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=100,
                    enum_values={"governance_proposal", "token_unlock", "exchange_maintenance", "software_release", "market_news", "regulatory_update"}
                ),
                FieldSchema(
                    name="source",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=50,
                    enum_values={"snapshot", "github", "binance", "coinbase", "twitter", "discord", "telegram", "reddit"}
                ),
                FieldSchema(
                    name="source_id",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=100
                ),
                FieldSchema(
                    name="timestamp",
                    field_type=FieldType.TIMESTAMP_US,
                    required=True
                ),
                FieldSchema(
                    name="title",
                    field_type=FieldType.STRING,
                    required=True,
                    min_length=1,
                    max_length=500,
                    trim_whitespace=True
                ),
                FieldSchema(
                    name="description",
                    field_type=FieldType.STRING,
                    nullable=True,
                    max_length=2000,
                    trim_whitespace=True
                ),
                FieldSchema(
                    name="status",
                    field_type=FieldType.STRING,
                    required=True,
                    enum_values={"active", "closed", "pending", "resolved", "cancelled"},
                    allow_enum_case_insensitive=True
                ),
                FieldSchema(
                    name="impact_score",
                    field_type=FieldType.INTEGER,
                    nullable=True,
                    min_value=1,
                    max_value=10
                )
            ],
            primary_key=["source", "source_id"],
            unique_constraints=[["source", "source_id"]],
            strict_foreign_keys=False
        )
        self.register_schema(offchain_events_schema)
        
        # =============================
        # MACRO/TRADFI DATA SCHEMAS
        # =============================
        
        # TradFi Indices schema (VIX, DXY, SPY, DIA, QQQ)
        tradfi_indices_schema = TableSchema(
            name="tradfi_indices",
            fields=[
                FieldSchema(
                    name="symbol",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=20,
                    trim_whitespace=True
                ),
                FieldSchema(
                    name="price",
                    field_type=FieldType.FLOAT,
                    required=True,
                    min_value=0.0
                ),
                FieldSchema(
                    name="timestamp_utc_us",
                    field_type=FieldType.TIMESTAMP_US,
                    required=True
                ),
                FieldSchema(
                    name="source",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=50,
                    enum_values={"alpha_vantage", "yahoo_finance"}
                ),
                FieldSchema(
                    name="change_pct",
                    field_type=FieldType.FLOAT,
                    nullable=True
                ),
                FieldSchema(
                    name="volume",
                    field_type=FieldType.FLOAT,
                    nullable=True,
                    min_value=0.0
                )
            ],
            primary_key=["symbol", "timestamp_utc_us", "source"],
            strict_foreign_keys=False
        )
        self.register_schema(tradfi_indices_schema)
        
        # TradFi Equities schema (SPY, QQQ, TLT, GLD, USO)
        tradfi_equities_schema = TableSchema(
            name="tradfi_equities",
            fields=[
                FieldSchema(
                    name="symbol",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=20,
                    trim_whitespace=True
                ),
                FieldSchema(
                    name="price",
                    field_type=FieldType.FLOAT,
                    required=True,
                    min_value=0.0
                ),
                FieldSchema(
                    name="timestamp_utc_us",
                    field_type=FieldType.TIMESTAMP_US,
                    required=True
                ),
                FieldSchema(
                    name="source",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=50,
                    enum_values={"alpha_vantage", "yahoo_finance"}
                ),
                FieldSchema(
                    name="change_pct",
                    field_type=FieldType.FLOAT,
                    nullable=True
                ),
                FieldSchema(
                    name="volume",
                    field_type=FieldType.FLOAT,
                    nullable=True,
                    min_value=0.0
                ),
                FieldSchema(
                    name="market_cap",
                    field_type=FieldType.FLOAT,
                    nullable=True,
                    min_value=0.0
                )
            ],
            primary_key=["symbol", "timestamp_utc_us", "source"],
            strict_foreign_keys=False
        )
        self.register_schema(tradfi_equities_schema)
        
        # Macro Economic Indicators schema (FRED data)
        macro_economic_indicators_schema = TableSchema(
            name="macro_economic_indicators",
            fields=[
                FieldSchema(
                    name="indicator_name",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=100,
                    trim_whitespace=True
                ),
                FieldSchema(
                    name="indicator_code",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=50,
                    trim_whitespace=True
                ),
                FieldSchema(
                    name="value",
                    field_type=FieldType.FLOAT,
                    required=True
                ),
                FieldSchema(
                    name="timestamp_utc_us",
                    field_type=FieldType.TIMESTAMP_US,
                    required=True
                ),
                FieldSchema(
                    name="source",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=50,
                    default_value="fred"
                ),
                FieldSchema(
                    name="frequency",
                    field_type=FieldType.STRING,
                    nullable=True,
                    max_length=20,
                    enum_values={"daily", "weekly", "monthly", "quarterly", "annual"}
                ),
                FieldSchema(
                    name="units",
                    field_type=FieldType.STRING,
                    nullable=True,
                    max_length=50
                )
            ],
            primary_key=["indicator_code", "timestamp_utc_us"],
            strict_foreign_keys=False
        )
        self.register_schema(macro_economic_indicators_schema)
        
        # Crypto Market Metrics schema (CoinGecko data)
        crypto_market_metrics_schema = TableSchema(
            name="crypto_market_metrics",
            fields=[
                FieldSchema(
                    name="timestamp_utc_us",
                    field_type=FieldType.TIMESTAMP_US,
                    required=True
                ),
                FieldSchema(
                    name="source",
                    field_type=FieldType.STRING,
                    required=True,
                    max_length=50,
                    default_value="coingecko"
                ),
                FieldSchema(
                    name="total_market_cap_usd",
                    field_type=FieldType.FLOAT,
                    required=True,
                    min_value=0.0
                ),
                FieldSchema(
                    name="total_volume_24h_usd",
                    field_type=FieldType.FLOAT,
                    required=True,
                    min_value=0.0
                ),
                FieldSchema(
                    name="btc_dominance_pct",
                    field_type=FieldType.FLOAT,
                    required=True,
                    min_value=0.0,
                    max_value=100.0
                ),
                FieldSchema(
                    name="eth_dominance_pct",
                    field_type=FieldType.FLOAT,
                    nullable=True,
                    min_value=0.0,
                    max_value=100.0
                ),
                FieldSchema(
                    name="defi_market_cap_usd",
                    field_type=FieldType.FLOAT,
                    nullable=True,
                    min_value=0.0
                ),
                FieldSchema(
                    name="defi_volume_24h_usd",
                    field_type=FieldType.FLOAT,
                    nullable=True,
                    min_value=0.0
                ),
                FieldSchema(
                    name="defi_dominance_pct",
                    field_type=FieldType.FLOAT,
                    nullable=True,
                    min_value=0.0,
                    max_value=100.0
                ),
                FieldSchema(
                    name="active_cryptocurrencies",
                    field_type=FieldType.INTEGER,
                    nullable=True,
                    min_value=0
                )
            ],
            primary_key=["timestamp_utc_us", "source"],
            strict_foreign_keys=False
        )
        self.register_schema(crypto_market_metrics_schema)
        
        logger.info(f"Registered {len(self.schemas)} data ingestion schemas: {list(self.schemas.keys())}")
    
    async def _load_reference_data(self):
        """Load reference data for foreign key validation from configuration."""
        logger.info("Loading reference data for foreign key validation...")
        
        # Load venues from config or use defaults
        venues = set(self.config.get("valid_venues", [
            "binance", "binance_futures", "coinbase", "gemini", "kraken", "okx",
            "bybit", "deribit", "ftx", "kucoin", "huobi", "bitfinex", "gate"
        ]))
        self.update_reference_data("venues", "name", venues)
        
        # Load symbols from config or use defaults
        # NOTE: Symbol normalization happens in Quality Orchestrator BEFORE validation,
        # so we only need to list canonical formats (e.g., BTCUSDT, not BTC-USD)
        symbols = set(self.config.get("valid_symbols", [
            # Spot pairs (canonical format after normalization)
            "BTCUSDT", "ETHUSDT", "ADAUSDT", "SOLUSDT", "DOTUSDT", "LINKUSDT",
            "XRPUSDT", "AVAXUSDT", "MATICUSDT", "UNIUSDT", "ATOMUSDT", "ALGOUSDT",
            "APTUSDT", "SUIUSDT", "NEARUSDT", "FTMUSDT", "LTCUSDT", "BCHUSDT",
            "ICPUSDT", "VETUSDT", "AAVEUSDT", "MKRUSDT", "SNXUSDT", "CRVUSDT",
            "COMPUSDT", "SUSHIUSDT", "YFIUSDT", "1INCHUSDT", "BALUSDT",
            "ARBUSDT", "OPUSDT", "DOGEUSDT", "SHIBUSDT", "PEPEUSDT",
            # Fiat pairs
            "BTCUSD", "ETHUSD", "BTCEUR", "ETHEUR",
            # Stablecoin pairs
            "USDCUSDT", "BUSDUSDT"
        ]))
        self.update_reference_data("symbols", "symbol", symbols)
        
        # Load chains from config or use defaults
        chains = set(self.config.get("valid_chains", [
            "ethereum", "bitcoin", "polygon", "arbitrum", "optimism", "base",
            "avalanche", "bsc", "fantom", "solana", "cosmos", "terra"
        ]))
        self.update_reference_data("chains", "name", chains)
        
        # Load currencies from config or use defaults
        currencies = set(self.config.get("valid_currencies", [
            "USD", "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "CNY", "HKD", "SGD",
            "USDT", "USDC", "BUSD", "DAI", "BTC", "ETH", "BNB", "ADA", "SOL", "DOT"
        ]))
        self.update_reference_data("currencies", "code", currencies)
        
        logger.info(f"Loaded reference data: {len(venues)} venues, {len(symbols)} symbols, {len(chains)} chains, {len(currencies)} currencies")
    
    async def get_health_status(self) -> Dict[str, Any]:
        """Get comprehensive health status."""
        return {
            'component_id': self.circuit_breaker_id,
            'running': self.running,
            'last_health_check': self._last_health_check,
            'schemas_loaded': len(self.schemas),
            'pending_tasks': len(self._pending_validation_tasks),
            'circuit_breaker_registered': self._circuit_breaker_registered,
            'metrics': self.metrics.copy()
        }
    
    async def start(self):
        """Start the schema validator agent with streaming bus consumer."""
        self.running = True
        
        # Register circuit breaker
        await self._register_circuit_breaker()
        
        # Register data ingestion schemas
        self._register_data_ingestion_schemas()
        
        # Load reference data for foreign key validation
        await self._load_reference_data()
        
        # Start health monitoring
        self._health_check_task = asyncio.create_task(self._health_monitor_loop())
        
        # Subscribe to raw data topics for validation (configurable)
        raw_data_topics = self.config.get("validation_topics", [
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
        ])
        
        # Get pool size from config
        pool_size = self.config.get("validation_pool_size", 8)
        
        try:
            # Start consumer as a managed task
            self._consumer_task = asyncio.create_task(
                self.streaming_bus.subscribe_with_worker_pool(
                    consumer_group="schema-validator",
                    topics=raw_data_topics,
                    handler=self._handle_raw_message_sync,
                    pool_size=pool_size  # Process validation in parallel
                )
            )
            
            logger.info(f"Schema Validator Agent started with streaming bus consumer (topics: {len(raw_data_topics)}, pool_size: {pool_size})")
            
        except Exception as e:
            self.running = False
            logger.error(f"Schema Validator startup failed: {e}")
            logger.error(f"Error details - Topics: {raw_data_topics}, Pool size: {pool_size}")
            logger.exception("Full startup error traceback:")
            raise  # Re-raise to prevent silent failure
    
    async def _handle_raw_message_sync(self, topic: str, partition_key: str, payload: Dict[str, Any], headers: Dict[str, str]) -> None:
        """Asynchronous handler for incoming raw data messages for validation."""
        try:
            # Extract table name from topic
            table_name = self._extract_table_name_from_topic(topic)
            if not table_name or table_name not in self.schemas:
                # Skip topics we don't have schemas for
                return
            
            # Generate row identifier from payload and headers
            row_id = headers.get("record_id", f"{topic}_{partition_key}_{int(time.time_ns())}")
            
            # Schedule async validation with proper task management
            validation_task = asyncio.create_task(self._async_validate_and_publish(table_name, payload, row_id, headers))
            self._pending_validation_tasks.add(validation_task)
            
            # Add callback to remove task when done and handle exceptions
            def task_done_callback(completed_task):
                self._pending_validation_tasks.discard(completed_task)
                exc = completed_task.exception()
                if exc:
                    logger.error(f"Validation task failed for {table_name}: {exc}")
            
            validation_task.add_done_callback(task_done_callback)
            
        except Exception as e:
            logger.exception(f"Error validating message from {topic}: {e}")
            
            # Schedule async error publishing with proper task management
            error_task = asyncio.create_task(self._publish_validation_error(topic, payload, str(e), headers))
            self._pending_validation_tasks.add(error_task)
            
            # Add callback to remove error task when done
            def error_task_done_callback(completed_task):
                self._pending_validation_tasks.discard(completed_task)
                exc = completed_task.exception()
                if exc:
                    logger.error(f"Error publishing validation error: {exc}")
            
            error_task.add_done_callback(error_task_done_callback)
    
    async def _async_validate_and_publish(self, table_name: str, payload: Dict[str, Any], row_id: str, headers: Dict[str, str]) -> None:
        """Async helper to validate a row and publish results."""
        try:
            cleaned_row, violations, flags = await self.validate_row(table_name, payload, row_id)
            await self._publish_validation_results(table_name, payload, cleaned_row, violations, flags, headers)
        except Exception:
            logger.exception(f"Error in async validation and publishing for table {table_name}")
    
    def _extract_table_name_from_topic(self, topic: str) -> Optional[str]:
        """Extract logical table name from Kafka topic."""
        # Map Kafka topics to logical table names
        topic_mapping = {
            "raw_data.exchange_feed": "exchange_trades",
            "raw_data.options_chain": "options_surface",
            "raw_data.onchain_events": "onchain_flows",
            "raw_data.offchain_events": "offchain_events",
            "raw_data.market.trades": "market_trades",
            "raw_data.market.book": "market_book",
            "raw_data.market.funding": "market_funding",
            "raw_data.market.oi": "market_oi",
            "raw_data.onchain.blocks": "onchain_blocks",
            "raw_data.onchain.mempool": "onchain_mempool",
            # Macro/TradFi/Crypto metrics
            "raw_data.tradfi.indices": "tradfi_indices",
            "raw_data.tradfi.equities": "tradfi_equities",
            "raw_data.macro.economic_indicators": "macro_economic_indicators",
            "raw_data.crypto.market_metrics": "crypto_market_metrics"
        }
        
        return topic_mapping.get(topic)
    
    def _get_clean_topic_for_table(self, table_name: str) -> Optional[str]:
        """Map table name to appropriate clean data topic."""
        table_to_topic_mapping = {
            # Market data tables
            "exchange_trades": "clean.market.trades",
            "market_trades": "clean.market.trades", 
            "market_book": "clean.market.book",
            "market_funding": "clean.market.funding",
            "market_oi": "clean.market.oi",
            
            # Options data
            "options_surface": "clean.market.options",
            
            # OnChain data
            "onchain_flows": "clean.market.onchain",
            "onchain_blocks": "clean.market.onchain",
            "onchain_mempool": "clean.market.onchain",
            
            # Events data
            "offchain_events": "clean.market.events",
            "calendar_events": "clean.market.events",
            "governance_events": "clean.market.events",
            
            # Macro/TradFi/Crypto metrics
            "tradfi_indices": "clean.tradfi.indices",
            "tradfi_equities": "clean.tradfi.equities",
            "macro_economic_indicators": "clean.macro.economic_indicators",
            "crypto_market_metrics": "clean.crypto.market_metrics"
        }
        
        return table_to_topic_mapping.get(table_name)
    
    async def _publish_validation_results(self, table_name: str, original_payload: Dict[str, Any], 
                                         cleaned_row: Dict[str, Any], violations: List[SchemaViolation], 
                                         flags: ValidationFlags, headers: Dict[str, str]) -> None:
        """Publish validation results to streaming bus."""
        
        # Determine validation status
        has_errors = any(v.severity == "error" for v in violations)
        has_coercions = any(v.violation_type.endswith("_coerced") for v in violations)
        
        if has_errors:
            status = "FAIL"
        elif has_coercions:
            status = "COERCED"
        else:
            status = "PASS"
        
        # Create validation summary
        validation_result = {
            "table_name": table_name,
            "row_id": violations[0].row_identifier if violations else headers.get("record_id", "unknown"),
            "timestamp": int(time.time() * 1_000_000),
            "status": status,
            "total_violations": len(violations),
            "error_violations": len([v for v in violations if v.severity == "error"]),
            "warning_violations": len([v for v in violations if v.severity == "warning"]),
            "coercion_count": len([v for v in violations if v.violation_type.endswith("_coerced")]),
            "validation_flags": {
                "had_pattern_error": flags.had_pattern_error,
                "had_range_error": flags.had_range_error,
                "had_type_error": flags.had_type_error,
                "had_null_error": flags.had_null_error,
                "had_foreign_key_error": flags.had_foreign_key_error,
                "had_unique_constraint_error": flags.had_unique_constraint_error,
                "had_coercion": flags.had_coercion
            }
        }
        
        # **ARCHITECTURAL FIX: Schema Validator does NOT publish to clean topics**
        # 
        # REASON: Violates single responsibility + bypasses quality pipeline
        # 
        # CORRECT FLOW:
        #   Schema Validator → returns validation result to Quality Orchestrator
        #   Quality Orchestrator → runs full 6-stage pipeline → publishes if quality_score >= threshold
        # 
        # Publishing here would mean data reaches clean topics BEFORE:
        #   - Leakage detection
        #   - Anomaly detection  
        #   - Freshness validation
        #   - Cross-source reconciliation
        # 
        # This defeats the purpose of the quality orchestration pipeline.
        # Only Quality Orchestrator should decide what data is "clean enough" to publish.
        # 
        # REMOVED CODE (lines 3189-3220):
        #   - Direct publishing to clean.* topics
        #   - Bypassed orchestrator's quality decision
        # 
        # Schema Validator now ONLY returns validation results.
        # Quality Orchestrator makes the publishing decision.
        
        # Publish to clean.pass_fail topic (validation summary only, not clean data)
        partition_key = f"schema_validation_{table_name}"
        
        # Get sequence number for validation summary
        self._sequence_numbers["clean.pass_fail"] += 1
        
        await self.streaming_bus.publish_with_canonical_headers(
            topic="clean.pass_fail",
            partition_key=partition_key,
            payload=validation_result,
            source_id=f"schema_validator.{table_name}",
            sequence_number=self._sequence_numbers["clean.pass_fail"],
            producer_version="2.0.0"
        )
        
        # Publish individual schema violations to incidents
        for violation in violations[:20]:  # Limit to first 20 violations per message
            if violation.severity == "error":  # Only publish errors as incidents
                violation_data = {
                    "incident_id": f"schema_{violation.table_name}_{violation.row_identifier}_{violation.field_name}_{int(time.time_ns())}",
                    "table_name": violation.table_name,
                    "field_name": violation.field_name,
                    "violation_type": violation.violation_type,
                    "expected": violation.expected,
                    "actual": violation.actual,
                    "row_identifier": violation.row_identifier,
                    "severity": violation.severity.upper(),
                    "timestamp": violation.timestamp_utc_us,
                    "expected_hint": violation.expected_hint,
                    "reference_info": violation.reference_info,
                    "coerced_value": str(violation.coerced_value) if violation.coerced_value is not None else None
                }
                
                # Get sequence number for incidents
                self._sequence_numbers["incidents.SchemaViolation"] += 1
                
                await self.streaming_bus.publish_with_canonical_headers(
                    topic="incidents.SchemaViolation",
                    partition_key=partition_key,
                    payload=violation_data,
                    source_id=f"schema_validator.{table_name}",
                    sequence_number=self._sequence_numbers["incidents.SchemaViolation"],
                    correlation_id=f"{table_name}_{violation.row_identifier}",
                    producer_version="2.0.0",
                    dedupe_key=f"schema_{violation.table_name}_{violation.row_identifier}_{violation.field_name}_{violation.violation_type}"
                )
    
    async def _publish_validation_error(self, topic: str, payload: Dict[str, Any], error_message: str, headers: Dict[str, str]) -> None:
        """Publish validation processing errors as incidents."""
        error_data = {
            "incident_id": f"schema_error_{topic}_{int(time.time_ns())}",
            "table_name": topic,
            "field_name": None,
            "violation_type": "validation_processing_error",
            "expected": "successful schema validation",
            "actual": f"validation failed: {error_message}",
            "row_identifier": headers.get("record_id", "unknown"),
            "severity": "ERROR",
            "timestamp": int(time.time() * 1_000_000),
            "payload_size": len(str(payload)),
            "error_details": error_message[:500]  # Truncate long error messages
        }
        
        # Get sequence number for incidents
        self._sequence_numbers["incidents.SchemaViolation"] += 1
        
        await self.streaming_bus.publish_with_canonical_headers(
            topic="incidents.SchemaViolation",
            partition_key=f"schema_error_{topic}",
            payload=error_data,
            source_id=f"schema_validator.{topic}",
            sequence_number=self._sequence_numbers["incidents.SchemaViolation"],
            producer_version="2.0.0",
            dedupe_key=f"schema_error_{topic}_{headers.get('record_id', int(time.time_ns()))}"
        )
    
    # Gold layer functionality removed - belongs in separate Gold Layer component
    # Schema Validator responsibility: raw_data.* → clean.* only
    
    async def stop(self):
        """Stop the schema validator agent with proper task cleanup."""
        self.running = False
        
        logger.info("Stopping Schema Validator Agent...")
        
        # Cancel the health check task
        if self._health_check_task and not self._health_check_task.done():
            logger.info("Cancelling health check task...")
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                logger.info("Health check task cancelled successfully")
            except Exception as e:
                logger.error(f"Error during health check task cancellation: {e}")
        
        # Cancel the consumer task
        if self._consumer_task and not self._consumer_task.done():
            logger.info("Cancelling consumer task...")
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                logger.info("Consumer task cancelled successfully")
            except Exception as e:
                logger.error(f"Error during consumer task cancellation: {e}")
        
        # Cancel all pending validation tasks
        if self._pending_validation_tasks:
            logger.info(f"Cancelling {len(self._pending_validation_tasks)} pending validation tasks...")
            
            # Cancel all pending tasks
            for task in list(self._pending_validation_tasks):
                if not task.done():
                    task.cancel()
            
            # Wait for cancelled tasks to complete
            if self._pending_validation_tasks:
                try:
                    await asyncio.gather(*self._pending_validation_tasks, return_exceptions=True)
                except Exception as e:
                    logger.error(f"Error during validation task cleanup: {e}")
            
            # Clear the task set
            self._pending_validation_tasks.clear()
        
        # Stop streaming bus
        try:
            await self.streaming_bus.shutdown()
            logger.info("Streaming bus shutdown completed")
        except Exception as e:
            logger.error(f"Error during streaming bus shutdown: {e}")
        
        logger.info("Schema Validator Agent stopped")


# Predefined coercion rules for common use cases
LOWERCASE_ADDRESS_RULE = CoercionRule(
    from_type=str,
    to_type=str,
    coercer=lambda x: x.lower() if isinstance(x, str) and len(x) == 42 and x[:2].lower() == '0x' else None,
    description="Normalize Ethereum addresses to lowercase"
)

LOWERCASE_HASH_RULE = CoercionRule(
    from_type=str,
    to_type=str,
    coercer=lambda x: x.lower() if isinstance(x, str) and len(x) == 66 and x[:2].lower() == '0x' else None,
    description="Normalize transaction hashes to lowercase"
)

TRIM_WHITESPACE_RULE = CoercionRule(
    from_type=str,
    to_type=str,
    coercer=lambda x: x.strip() if isinstance(x, str) else None,
    description="Trim leading and trailing whitespace"
)

DECIMAL_PRECISION_6_RULE = CoercionRule(
    from_type=Decimal,
    to_type=Decimal,
    coercer=lambda x: x.quantize(Decimal('0.000001'), rounding=ROUND_HALF_EVEN) if isinstance(x, Decimal) else None,
    description="Quantize decimal to 6 decimal places"
)


# Enhanced schema definitions showcasing new capabilities
def create_enhanced_onchain_flow_schema() -> TableSchema:
    """Enhanced schema with cross-field validation and data quality rules."""
    return TableSchema(
        name="enhanced_onchain_flows",
        fields=[
            FieldSchema(
                name="hash",
                field_type=FieldType.HASH,
                required=True,
                pattern=r"^0x[a-fA-F0-9]{64}$",
                coercion_rules=[LOWERCASE_HASH_RULE]
            ),
            FieldSchema(
                name="from_address", 
                field_type=FieldType.ADDRESS,
                required=True,
                pattern=r"^0x[a-fA-F0-9]{40}$",
                coercion_rules=[LOWERCASE_ADDRESS_RULE]
            ),
            FieldSchema(
                name="to_address",
                field_type=FieldType.ADDRESS, 
                required=True,
                pattern=r"^0x[a-fA-F0-9]{40}$",
                coercion_rules=[LOWERCASE_ADDRESS_RULE]
            ),
            FieldSchema(
                name="amount",
                field_type=FieldType.DECIMAL,
                required=True,
                min_value=Decimal('0'),
                decimal_scale=6,
                coercion_rules=[DECIMAL_PRECISION_6_RULE]
            ),
            FieldSchema(
                name="token_symbol",
                field_type=FieldType.STRING,
                required=True,
                max_length=20,
                coercion_rules=[TRIM_WHITESPACE_RULE]
            ),
            FieldSchema(
                name="block_number",
                field_type=FieldType.INTEGER,
                required=True,
                min_value=0
            ),
            FieldSchema(
                name="timestamp_utc_us",
                field_type=FieldType.TIMESTAMP_US,
                required=True,
                business_hours_only=True  # Enhanced: business hours validation
            ),
            FieldSchema(
                name="created_at_us",
                field_type=FieldType.TIMESTAMP_US,
                required=True
            ),
            FieldSchema(
                name="gas_fee",
                field_type=FieldType.DECIMAL,
                nullable=True
            ),
            FieldSchema(
                name="is_mainnet",
                field_type=FieldType.BOOLEAN,
                required=True
            ),
            FieldSchema(
                name="exchange_rate",
                field_type=FieldType.DECIMAL,
                nullable=True
            ),
            FieldSchema(
                name="usd_amount",
                field_type=FieldType.DECIMAL,
                nullable=True
            )
        ],
        primary_key=["hash"],
        unique_constraints=[["hash"]],
        # Enhanced cross-field validation rules
        cross_field_rules=[
            CrossFieldRule(
                rule_type="conditional_required",
                fields=["gas_fee"],
                condition=lambda row: row.get("is_mainnet") is True,
                error_message="gas_fee required for mainnet transactions",
                severity="error"
            ),
            CrossFieldRule(
                rule_type="temporal_ordering", 
                fields=["created_at_us", "timestamp_utc_us"],
                error_message="created_at must be <= timestamp",
                severity="error"
            ),
            CrossFieldRule(
                rule_type="field_relationship",
                fields=["amount", "exchange_rate", "usd_amount"],
                condition=lambda row: (
                    all(row.get(f) is not None for f in ["amount", "exchange_rate", "usd_amount"]) and
                    abs(float(row["amount"]) * float(row["exchange_rate"]) - float(row["usd_amount"])) > 0.01
                ),
                error_message="usd_amount should equal amount * exchange_rate",
                severity="warning"
            )
        ],
        batch_constraints=["monotonic_increasing"]  # Block numbers should increase within a batch
    )


def create_enhanced_user_schema() -> TableSchema:
    """Enhanced user schema with semantic validation and checksums."""
    return TableSchema(
        name="enhanced_users",
        fields=[
            FieldSchema(
                name="user_id",
                field_type=FieldType.STRING,
                required=True
            ),
            FieldSchema(
                name="email",
                field_type=FieldType.STRING,
                required=True,
                semantic_validator="email"  # Enhanced: email validation
            ),
            FieldSchema(
                name="phone",
                field_type=FieldType.STRING,
                nullable=True,
                semantic_validator="phone"  # Enhanced: phone validation
            ),
            FieldSchema(
                name="website",
                field_type=FieldType.STRING,
                nullable=True,
                semantic_validator="url"  # Enhanced: URL validation
            ),
            FieldSchema(
                name="country_code",
                field_type=FieldType.STRING,
                required=True,
                semantic_validator="country_code"  # Enhanced: country code validation
            ),
            FieldSchema(
                name="preferred_currency",
                field_type=FieldType.STRING,
                required=True,
                semantic_validator="currency_code"  # Enhanced: currency code validation
            ),
            FieldSchema(
                name="credit_card",
                field_type=FieldType.STRING,
                nullable=True,
                checksum_algorithm="luhn"  # Enhanced: Luhn checksum validation
            ),
            FieldSchema(
                name="bank_account",
                field_type=FieldType.STRING,
                nullable=True,
                checksum_algorithm="iban"  # Enhanced: IBAN validation
            ),
            FieldSchema(
                name="notification_method",
                field_type=FieldType.ENUM,
                required=True,
                enum_values={"email", "sms", "push", "none"}
            )
        ],
        primary_key=["user_id"],
        # Enhanced cross-field validation
        cross_field_rules=[
            CrossFieldRule(
                rule_type="conditional_required",
                fields=["phone"],
                condition=lambda row: row.get("notification_method") == "sms",
                error_message="phone required when notification_method is sms",
                severity="error"
            ),
            CrossFieldRule(
                rule_type="mutual_exclusive",
                fields=["credit_card", "bank_account"],
                error_message="provide either credit card or bank account, not both",
                severity="warning"
            )
        ]
    )


def create_financial_transactions_schema() -> TableSchema:
    """Example schema demonstrating all batch constraints."""
    return TableSchema(
        name="financial_transactions",
        fields=[
            FieldSchema(
                name="transaction_id",
                field_type=FieldType.INTEGER,
                required=True
            ),
            FieldSchema(
                name="amount",
                field_type=FieldType.DECIMAL,
                required=True
            ),
            FieldSchema(
                name="timestamp_utc_us",
                field_type=FieldType.TIMESTAMP_US,
                required=True
            ),
            FieldSchema(
                name="account_id",
                field_type=FieldType.STRING,
                required=True
            )
        ],
        primary_key=["transaction_id"],
        unique_constraints=[["transaction_id"]],
        batch_constraints=["sum_equals_zero", "sequence_continuous", "monotonic_increasing"]
        # sum_equals_zero: Credits and debits must balance in a complete transaction set
        # sequence_continuous: Transaction IDs should be continuous (no gaps)  
        # monotonic_increasing: Timestamps should be in order
    )


def create_calendar_event_schema() -> TableSchema:
    """Example schema for calendar events."""
    return TableSchema(
        name="calendar_events", 
        fields=[
            FieldSchema(
                name="event_type",
                field_type=FieldType.ENUM,
                required=True,
                enum_values={"governance_proposal", "token_unlock", "exchange_maintenance", "software_release"}
            ),
            FieldSchema(
                name="title",
                field_type=FieldType.STRING,
                required=True,
                min_length=1,
                max_length=500
            ),
            FieldSchema(
                name="description",
                field_type=FieldType.STRING,
                nullable=True,
                max_length=2000
            ),
            FieldSchema(
                name="start_time_utc_us",
                field_type=FieldType.TIMESTAMP_US,
                required=True
            ),
            FieldSchema(
                name="source",
                field_type=FieldType.ENUM,
                required=True,
                enum_values={"snapshot", "github", "binance", "coinbase"},
                allow_enum_case_insensitive=True
            ),
            FieldSchema(
                name="source_id",
                field_type=FieldType.STRING,
                required=True
            ),
            FieldSchema(
                name="status",
                field_type=FieldType.ENUM,
                required=True,
                enum_values={"active", "closed", "pending", "resolved"},
                allow_enum_case_insensitive=True
            )
        ],
        primary_key=["source", "source_id"],
        unique_constraints=[["source", "source_id"]]
    )


# Example usage
async def main():
    """Example usage of the Schema Validator Agent."""
    config = {
        'validation_enabled': True,
        'strict_mode': False,  # Allow coercions
        'log_all_violations': True
    }
    
    validator = SchemaValidatorAgent(config)
    
    # Register schemas
    validator.register_schema(create_enhanced_onchain_flow_schema())
    validator.register_schema(create_enhanced_user_schema())
    validator.register_schema(create_calendar_event_schema())
    
    await validator.start()
    
    # Example data validation
    test_flow = {
        "hash": "0X" + "A" * 64,  # Uppercase hash to test lowercasing
        "from_address": "0X" + "B" * 40,  # Uppercase to test address normalization
        "to_address": "0x" + "c" * 40,
        "amount": "1000.123456789",  # String with extra precision
        "token_symbol": "  USDC  ",  # With whitespace
        "block_number": "12345",  # String that can be coerced to int
        "timestamp_utc_us": int(time.time() * 1_000_000),
        "extra_field": "should_be_flagged"  # Unexpected field
    }
    
    cleaned_row, violations, flags = await validator.validate_row("enhanced_onchain_flows", test_flow, "test_1")
    
    print(f"Cleaned row: {cleaned_row}")
    print(f"Violations: {len(violations)}")
    for v in violations:
        print(f"  {v.violation_type}: {v.expected} vs {v.actual}")
        if v.coerced_value is not None:
            print(f"    -> Coerced to: {v.coerced_value}")
    print(f"Validation flags: {flags}")
    
    # Test strict mode
    print("\n--- Testing Strict Mode ---")
    strict_validator = SchemaValidatorAgent({'strict_mode': True})
    strict_validator.register_schema(create_enhanced_onchain_flow_schema())
    await strict_validator.start()
    
    # Test with int amount in strict mode (should fail type validation)
    strict_test = dict(test_flow)
    strict_test["amount"] = 1000  # Integer instead of Decimal, should fail in strict mode
    
    strict_cleaned, strict_violations, strict_flags = await strict_validator.validate_row("enhanced_onchain_flows", strict_test, "test_strict")
    print(f"Strict mode violations: {len(strict_violations)}")
    for v in strict_violations:
        print(f"  {v.violation_type}: {v.expected} vs {v.actual}")
        if v.field_name == "amount":
            print(f"    -> Integer amount rejected (no coercion to Decimal)")
    
    # Test timestamp coercion priority
    print("\n--- Testing Timestamp Coercion Priority ---")
    timestamp_test = dict(test_flow)
    timestamp_test["timestamp_utc_us"] = "1700000000"  # Numeric string (seconds)
    
    timestamp_cleaned, timestamp_violations, _ = await validator.validate_row("enhanced_onchain_flows", timestamp_test, "test_timestamp")
    print(f"Timestamp coercion result: {timestamp_cleaned.get('timestamp_utc_us')}")
    for v in timestamp_violations:
        if v.field_name == "timestamp_utc_us" and v.violation_type == "type_coerced":
            print(f"  Coerced timestamp: {v.actual} -> {v.coerced_value}")
    
    # Test list/dict coercion propagation
    print("\n--- Testing List/Dict Coercion Propagation ---")
    
    # Create a schema with list and dict fields for testing
    test_schema = TableSchema(
        name="test_collections",
        fields=[
            FieldSchema(
                name="numbers",
                field_type=FieldType.LIST,
                list_item_type=FieldType.INTEGER
            ),
            FieldSchema(
                name="metadata",
                field_type=FieldType.DICT,
                dict_value_type=FieldType.DECIMAL
            )
        ]
    )
    validator.register_schema(test_schema)
    
    collection_test = {
        "numbers": ["1", "2", "3"],  # String numbers that can be coerced
        "metadata": {"price": "123.45", "volume": "67.89"}  # String decimals
    }
    
    collection_cleaned, collection_violations, _ = await validator.validate_row("test_collections", collection_test, "test_collections")
    print(f"List coercion: {collection_test['numbers']} -> {collection_cleaned['numbers']}")
    print(f"Dict coercion: {collection_test['metadata']} -> {collection_cleaned['metadata']}")
    print(f"Collection violations: {len(collection_violations)}")
    
    # Test primary key vs unique constraint violations
    print("\n--- Testing Primary Key vs Unique Constraint Violations ---")
    
    # Create duplicate primary key scenario
    duplicate_flow = dict(test_flow)
    duplicate_flow["hash"] = cleaned_row["hash"]  # Same hash (primary key)
    duplicate_flow["from_address"] = "0x" + "d" * 40  # Different from address
    
    batch_summary = await validator.validate_batch("enhanced_onchain_flows", [test_flow, duplicate_flow])
    print(f"Batch violations: {len(batch_summary.violations)}")
    for v in batch_summary.violations:
        if v.violation_type in ["primary_key_violation", "unique_constraint_violation"]:
            print(f"  {v.violation_type}: {v.expected} - {v.actual}")
    
    await validator.stop()
    await strict_validator.stop()


if __name__ == "__main__":
    asyncio.run(main())
