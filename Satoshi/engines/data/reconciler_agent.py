"""
Reconciler Agent - Deterministic Cross-Venue Data Reconciliation

Mission: Deterministic diffs across venues/sources; propose fixes.
Outputs: recon.report + ApprovalRequest (no direct writes).

The reconciler performs systematic comparisons between different data sources,
identifies discrepancies, and generates approval requests for remediation actions.
It never performs direct writes - all changes go through approval workflows.

KEY FEATURES:
============

🔍 Deterministic Comparisons:
   - Configurable tolerance levels for numeric, timestamp, and string fields
   - Priority-based conflict resolution (authoritative source wins)
   - Stable hashing for consistent discrepancy identification
   - Field-specific comparison rules and normalization

📊 Comprehensive Discrepancy Detection:
   - Missing/Extra records across sources
   - Value mismatches with severity classification
   - Timestamp drift detection and tolerance
   - Schema differences and reference violations
   - Ordering discrepancies

🔐 Approval Workflow (No Direct Writes):
   - All changes require explicit approval
   - Confidence scoring based on source priorities
   - Automatic rollback plan generation
   - Expiry times for approval requests
   - Manual review escalation for complex cases

📈 Production-Ready Operations:
   - Batch processing with configurable limits
   - Performance monitoring and statistics
   - Report retention and caching
   - Read-only source protection
   - Timeout protection and error handling

🛡️ Guardrails:
   - NO WRITES EVER: Only produces ApprovalRequests, never executes
   - NO SCHEMA ENFORCEMENT: Treats schema as cross-source discrepancies only
   - NO ALERTING/ROUTING: Report is the product, no external notifications

🎯 Smart Fix Proposals:
   - Priority-based authoritative source selection
   - Action type determination (UPDATE/INSERT/DELETE/MERGE)
   - Impact assessment and rollback planning
   - Dual approval requirements for critical changes
   - Auto-approval options for low-severity fixes

USAGE EXAMPLE:
=============

    # Configure reconciler
    config = ReconcilerConfig(
        float_tolerance=1e-6,
        timestamp_tolerance_seconds=2.0,
        auto_approve_low_severity=False
    )
    reconciler = ReconcilerAgent(config)
    
    # Register sources with priorities
    reconciler.register_data_source(DataSource(
        name="primary_db", priority=3, key_fields=["trade_id"]
    ))
    reconciler.register_data_source(DataSource(
        name="backup_db", priority=2, key_fields=["trade_id"]
    ))
    
    # Run reconciliation
    report = await reconciler.reconcile_sources(["primary_db", "backup_db"])
    
    # Review discrepancies and approval requests
    print(f"Found {len(report.discrepancies)} discrepancies")
    print(f"Generated {len(report.approval_requests)} approval requests")
"""

import asyncio
import time
import hashlib
import json
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Set, Tuple, Union, Callable
from enum import Enum
import numpy as np

from infra.bus.streaming_bus import StreamingBus


class DiscrepancyType(Enum):
    """Types of data discrepancies that can be detected."""
    MISSING_RECORD = "missing_record"
    EXTRA_RECORD = "extra_record"
    VALUE_MISMATCH = "value_mismatch"
    TIMESTAMP_DRIFT = "timestamp_drift"
    SCHEMA_DIFFERENCE = "schema_difference"
    REFERENCE_VIOLATION = "reference_violation"
    ORDERING_DIFFERENCE = "ordering_difference"


class ReconciliationSeverity(Enum):
    """Severity levels for reconciliation discrepancies."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ApprovalAction(Enum):
    """Types of actions that can be requested for approval."""
    INSERT_RECORD = "insert_record"
    UPDATE_RECORD = "update_record"
    DELETE_RECORD = "delete_record"
    MERGE_RECORDS = "merge_records"
    SYNC_TIMESTAMP = "sync_timestamp"
    SCHEMA_MIGRATION = "schema_migration"
    MANUAL_REVIEW = "manual_review"


@dataclass
class DataSource:
    """Configuration for a data source to reconcile."""
    name: str
    connection_params: Dict[str, Any]
    priority: int = 1  # Higher number = higher priority (authoritative source)
    read_only: bool = False
    query_template: Optional[str] = None
    key_fields: List[str] = field(default_factory=list)  # Fields that uniquely identify records
    timestamp_field: Optional[str] = None
    tolerance_config: Dict[str, Any] = field(default_factory=dict)
    
    # Field mapping and normalization for comparison hygiene
    field_map: Dict[str, str] = field(default_factory=dict)  # source_field -> canonical_field
    field_normalizers: Dict[str, Callable[[Any], Any]] = field(default_factory=dict)  # canonical_field -> normalizer function
    
    # Ordering detection (opt-in)
    ordering_key: Optional[str] = None  # Field that should maintain order (e.g., "sequence_id", "trade_id")


@dataclass
class Discrepancy:
    """A specific discrepancy found between data sources."""
    discrepancy_id: str
    discrepancy_type: DiscrepancyType
    severity: ReconciliationSeverity
    source_a: str
    source_b: str
    key_values: Dict[str, Any]  # Values of key fields that identify the record
    field_name: Optional[str] = None  # Specific field with discrepancy (for value mismatches)
    value_a: Optional[Any] = None
    value_b: Optional[Any] = None
    detected_at: int = field(default_factory=lambda: int(time.time() * 1_000_000))
    context: Dict[str, Any] = field(default_factory=dict)
    confidence_score: float = 1.0  # 0.0 to 1.0


@dataclass
class ApprovalRequest:
    """A request for approval to fix a discrepancy."""
    request_id: str
    action: ApprovalAction
    target_source: str
    discrepancy_id: str
    description: str
    proposed_changes: Dict[str, Any]
    impact_assessment: str
    rollback_plan: str
    confidence_score: float
    created_at: int = field(default_factory=lambda: int(time.time() * 1_000_000))
    expires_at: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ReconciliationReport:
    """Comprehensive report of reconciliation results."""
    report_id: str
    session_id: str
    started_at: int
    completed_at: Optional[int] = None
    sources_compared: List[str] = field(default_factory=list)
    total_records_compared: int = 0
    discrepancies: List[Discrepancy] = field(default_factory=list)
    approval_requests: List[ApprovalRequest] = field(default_factory=list)
    summary_stats: Dict[str, Any] = field(default_factory=dict)
    performance_metrics: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ReconcilerConfig:
    """Configuration for the reconciler agent."""
    # Comparison settings
    float_tolerance: float = 1e-6  # Absolute tolerance for float comparisons
    float_relative_tolerance: float = 1e-9  # Relative tolerance for float comparisons
    timestamp_tolerance_seconds: float = 1.0  # Allowable timestamp drift
    
    # Batch processing
    batch_size: int = 1000  # Records to process in each batch
    max_concurrent_sources: int = 3  # Maximum parallel source queries
    
    # Performance limits
    max_discrepancies_per_run: int = 10000  # Limit to prevent runaway reporting
    timeout_seconds: float = 300.0  # Maximum time for reconciliation run
    
    # Approval workflow
    auto_approve_low_severity: bool = False  # FLAG ONLY: Never execute auto-approval, only metadata
    require_dual_approval_critical: bool = True  # Require two approvals for critical fixes
    approval_expiry_hours: float = 24.0  # Hours before approval requests expire
    
    # Report retention
    report_retention_days: int = 90  # Days to retain reconciliation reports
    
    # Field-specific tolerances
    field_tolerances: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    # Streaming bus configuration
    streaming_bus: Optional[Dict[str, Any]] = field(default_factory=lambda: {
        "bootstrap_servers": "localhost:9092",
        "enable_ssl": False,
        "enable_sasl": False
    })
    
    # Severity policy overrides for operational tuning
    severity_overrides: Dict[str, str] = field(default_factory=dict)  # field_name -> {low|medium|high|critical}


class ReconcilerAgent:
    """
    Deterministic cross-venue data reconciliation agent.
    
    Performs systematic comparisons between data sources, identifies discrepancies,
    and generates approval requests for remediation. Never performs direct writes.
    """
    
    def __init__(self, config: ReconcilerConfig):
        self.config = config
        self.data_sources: Dict[str, DataSource] = {}
        self.session_id = self._generate_session_id()
        
        # Circuit Breaker Integration - Unique ID for this component
        self.circuit_breaker_id = f"reconciler_agent_{id(self)}"
        
        # Streaming Bus Integration  
        streaming_config = config.streaming_bus or {
            "bootstrap_servers": "localhost:9092",
            "enable_ssl": False,
            "enable_sasl": False
        }
        self.streaming_bus = StreamingBus(streaming_config)
        
        # Health monitoring
        self.health_check_interval = 30.0  # seconds
        self.last_successful_reconciliation = time.time()
        self.consecutive_failures = 0
        self.max_consecutive_failures = 3
        
        # Concurrency control
        self.fetch_semaphore = asyncio.Semaphore(config.max_concurrent_sources)
        
        # Enhanced statistics tracking with comprehensive metrics
        self.reconciliation_stats = {
            "total_runs": 0,
            "total_discrepancies": 0,
            "total_approval_requests": 0,
            "avg_processing_time": 0.0,
            "sources_registered": 0,
            "successful_reconciliations": 0,
            "failed_reconciliations": 0,
            "total_records_processed": 0,
            "avg_discrepancy_rate": 0.0,
            "circuit_breaker_trips": 0,
            "approval_requests_generated": 0,
            "high_severity_discrepancies": 0,
            "critical_severity_discrepancies": 0,
            "auto_approval_eligible": 0,
            "manual_review_required": 0,
            "source_comparison_pairs": 0,
            "cache_hit_rate": 0.0
        }
        
        # Cache for deduplication and idempotency
        self.discrepancy_cache: Dict[str, Discrepancy] = {}
        self.approval_cache: Dict[str, Tuple[int, str]] = {}  # discrepancy_id -> (last_request_time, request_signature)
        self.recent_reports: deque = deque(maxlen=100)
        
        # Task management for control message handlers
        self._pending_tasks: set = set()
        self._shutdown_event = asyncio.Event()
        self._monitoring_task: Optional[asyncio.Task] = None
        
    def register_data_source(self, source: DataSource) -> None:
        """Register a data source for reconciliation."""
        self.data_sources[source.name] = source
        self.reconciliation_stats["sources_registered"] = len(self.data_sources)
        
    def _generate_session_id(self) -> str:
        """Generate a unique session ID for this reconciler instance."""
        timestamp = int(time.time() * 1_000_000)
        return f"recon_session_{timestamp}_{hash(id(self)) & 0xFFFF:04x}"
    
    def _generate_discrepancy_id(self, source_a: str, source_b: str, 
                                key_values: Dict[str, Any], field_name: Optional[str] = None) -> str:
        """Generate a deterministic, canonical ID for a discrepancy."""
        # Canonicalize source ordering for symmetric IDs (A,B) == (B,A)
        sources = sorted([source_a, source_b])
        
        # Create stable, sorted key tuple
        sorted_key_items = sorted(key_values.items())
        
        # Create stable hash from canonical components
        components = [
            sources[0], sources[1], 
            json.dumps(sorted_key_items, sort_keys=True, separators=(',', ':'))  # Compact, deterministic
        ]
        if field_name:
            components.append(field_name)
        content = "|".join(components)
        hash_hex = hashlib.sha256(content.encode()).hexdigest()[:16]
        return f"disc_{hash_hex}"
    
    def _generate_approval_request_id(self, discrepancy_id: str, action: ApprovalAction) -> str:
        """Generate a unique ID for an approval request."""
        timestamp = int(time.time() * 1_000_000)
        content = f"{discrepancy_id}_{action.value}_{timestamp}"
        hash_hex = hashlib.sha256(content.encode()).hexdigest()[:12]
        return f"approval_{hash_hex}"
    
    def _calculate_severity(self, discrepancy_type: DiscrepancyType, 
                          context: Dict[str, Any]) -> ReconciliationSeverity:
        """Calculate severity based on discrepancy type and context with policy overrides."""
        # Check for field-specific severity overrides first
        field_name = context.get("field_name", "")
        if field_name and field_name in self.config.severity_overrides:
            override_level = self.config.severity_overrides[field_name].lower()
            severity_map = {
                "low": ReconciliationSeverity.LOW,
                "medium": ReconciliationSeverity.MEDIUM,
                "high": ReconciliationSeverity.HIGH,
                "critical": ReconciliationSeverity.CRITICAL
            }
            return severity_map.get(override_level, ReconciliationSeverity.MEDIUM)
        
        # Default severity calculation logic
        # Critical: Data integrity violations
        if discrepancy_type in [DiscrepancyType.REFERENCE_VIOLATION, DiscrepancyType.SCHEMA_DIFFERENCE]:
            return ReconciliationSeverity.CRITICAL
            
        # High: Missing or extra records in high-priority sources
        if discrepancy_type in [DiscrepancyType.MISSING_RECORD, DiscrepancyType.EXTRA_RECORD]:
            if context.get("affects_high_priority_source", False):
                return ReconciliationSeverity.HIGH
            return ReconciliationSeverity.MEDIUM
            
        # Medium: Value mismatches on important fields
        if discrepancy_type == DiscrepancyType.VALUE_MISMATCH:
            if any(keyword in field_name.lower() for keyword in ["price", "amount", "balance", "quantity"]):
                return ReconciliationSeverity.HIGH
            return ReconciliationSeverity.MEDIUM
            
        # Low: Timestamp drifts and ordering differences
        if discrepancy_type in [DiscrepancyType.TIMESTAMP_DRIFT, DiscrepancyType.ORDERING_DIFFERENCE]:
            return ReconciliationSeverity.LOW
            
        return ReconciliationSeverity.MEDIUM
    
    def _compare_values(self, value_a: Any, value_b: Any, field_name: str) -> bool:
        """Compare two values with appropriate tolerance including ULP guard."""
        # Handle None values
        if value_a is None and value_b is None:
            return True
        if value_a is None or value_b is None:
            return False
            
        # Use field-specific tolerance if configured
        field_config = self.config.field_tolerances.get(field_name, {})
        
        # Numeric comparisons with ULP support
        if isinstance(value_a, (int, float)) and isinstance(value_b, (int, float)):
            # Handle Decimal for money fields when configured
            use_decimal = field_config.get("use_decimal", False)
            if use_decimal:
                try:
                    from decimal import Decimal
                    dec_a = Decimal(str(value_a))
                    dec_b = Decimal(str(value_b))
                    abs_tol = field_config.get("absolute_tolerance", self.config.float_tolerance)
                    return abs(dec_a - dec_b) <= Decimal(str(abs_tol))
                except:
                    # Fall back to float comparison if Decimal fails
                    pass
            
            abs_tol = field_config.get("absolute_tolerance", self.config.float_tolerance)
            rel_tol = field_config.get("relative_tolerance", self.config.float_relative_tolerance)
            
            # ULP (Units in Last Place) guard for tiny float jitters
            ulp_tolerance = field_config.get("ulp_tolerance", 2)  # Default 2 ULP
            if ulp_tolerance > 0:
                # Simple ULP check: if values are very close in magnitude
                if abs(value_a) > 0 and abs(value_b) > 0:
                    # Use relative difference as ULP approximation
                    rel_diff = abs(value_a - value_b) / max(abs(value_a), abs(value_b))
                    if rel_diff < ulp_tolerance * 2.22e-16:  # Machine epsilon * ULP tolerance
                        return True
            
            # Apply min epsilon to prevent micro-jitter false positives
            min_epsilon = field_config.get("min_epsilon", 1e-15)
            if abs(value_a - value_b) <= min_epsilon:
                return True
                
            # Standard absolute/relative tolerance check
            return abs(value_a - value_b) <= abs_tol or abs(value_a - value_b) <= rel_tol * max(abs(value_a), abs(value_b))
        
        # String comparisons (case-sensitive by default)
        if isinstance(value_a, str) and isinstance(value_b, str):
            case_sensitive = field_config.get("case_sensitive", True)
            if not case_sensitive:
                return value_a.lower() == value_b.lower()
            return value_a == value_b
            
        # Exact comparison for other types
        return value_a == value_b
    
    def _normalize_record(self, record: Dict[str, Any], source: DataSource) -> Dict[str, Any]:
        """Normalize a record for comparison with field mapping and normalization."""
        normalized = {}
        
        for key, value in record.items():
            # Apply field mapping FIRST (source field -> canonical field)
            canonical_key = source.field_map.get(key, key)
            
            # Handle None values after mapping
            if value is None:
                normalized[canonical_key] = None
                continue
            
            # Normalize timestamps with improved units detection
            if canonical_key == source.timestamp_field and isinstance(value, (int, float)):
                normalized[canonical_key] = self._normalize_timestamp(value)
            else:
                normalized[canonical_key] = value
            
            # Apply field-specific normalizers for comparison hygiene
            if canonical_key in source.field_normalizers:
                try:
                    normalized[canonical_key] = source.field_normalizers[canonical_key](normalized[canonical_key])
                except Exception:
                    # Keep original value if normalizer fails
                    pass
                
        return normalized
    
    def _normalize_timestamp(self, value: Union[int, float]) -> int:
        """Units-aware timestamp normalization using digit bands - always convert to microseconds.
        
        Digit bands for correct unit identification:
        <1e11 → seconds → ×1e6
        <1e14 → milliseconds → ×1e3  
        <1e17 → microseconds → as-is
        ≥1e17 → nanoseconds → ÷1e3
        """
        if value < 1e11:  # Seconds (e.g., 1728000000)
            return int(value * 1_000_000)
        elif value < 1e14:  # Milliseconds (e.g., 1728000000000)
            return int(value * 1_000)
        elif value < 1e17:  # Microseconds (e.g., 1728000000000000)
            return int(value)
        else:  # Nanoseconds (e.g., 1728000000000000000)
            return int(value / 1_000)
    
    def _extract_key(self, record: Dict[str, Any], key_fields: List[str]) -> Dict[str, Any]:
        """Extract key values from a record."""
        return {field: record.get(field) for field in key_fields}
    
    async def _fetch_data(self, source: DataSource, query_params: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Fetch data from a source with timeout and concurrency control."""
        async with self.fetch_semaphore:
            try:
                # This would be implemented with actual database/API connectors
                # For now, return empty list as placeholder
                await asyncio.wait_for(
                    asyncio.sleep(0.001),  # Simulate fetch
                    timeout=self.config.timeout_seconds
                )
                return [], None  # (data, error)
            except asyncio.TimeoutError:
                error_msg = f"Timeout fetching from {source.name} ({self.config.timeout_seconds}s)"
                return [], error_msg
            except Exception as e:
                error_msg = f"Error fetching from {source.name}: {str(e)}"
                return [], error_msg
    
    def compare_records(self, record_a: Dict[str, Any], record_b: Dict[str, Any],
                       source_a: DataSource, source_b: DataSource) -> List[Discrepancy]:
        """Compare two records and return any discrepancies found."""
        discrepancies = []
        
        # Normalize records
        norm_a = self._normalize_record(record_a, source_a)
        norm_b = self._normalize_record(record_b, source_b)
        
        # Get common fields
        fields_a = set(norm_a.keys())
        fields_b = set(norm_b.keys())
        common_fields = fields_a.intersection(fields_b)
        
        # Extract key for discrepancy identification
        key_values = self._extract_key(norm_a, source_a.key_fields)
        
        # Check for schema differences with enhanced clarity
        if fields_a != fields_b:
            missing_in_b = fields_a - fields_b
            missing_in_a = fields_b - fields_a
            
            if missing_in_a or missing_in_b:
                context = {
                    "missing_in_a": list(missing_in_a),
                    "missing_in_b": list(missing_in_b),
                    "field_count_a": len(fields_a),
                    "field_count_b": len(fields_b),
                    "field_name": "schema"  # Consistent field name for schema differences
                }
                
                discrepancy_id = self._generate_discrepancy_id(source_a.name, source_b.name, key_values, "schema")
                severity = self._calculate_severity(DiscrepancyType.SCHEMA_DIFFERENCE, context)
                
                discrepancies.append(Discrepancy(
                    discrepancy_id=discrepancy_id,
                    discrepancy_type=DiscrepancyType.SCHEMA_DIFFERENCE,
                    severity=severity,
                    source_a=source_a.name,
                    source_b=source_b.name,
                    key_values=key_values,
                    field_name="schema",  # Consistent field name
                    context=context
                ))
        
        # Compare common fields
        for field_name in common_fields:
            value_a = norm_a[field_name]
            value_b = norm_b[field_name]
            
            if not self._compare_values(value_a, value_b, field_name):
                # Handle timestamp drift specially
                if field_name == source_a.timestamp_field:
                    if isinstance(value_a, (int, float)) and isinstance(value_b, (int, float)):
                        drift_seconds = abs(value_a - value_b) / 1_000_000  # Convert from microseconds
                        if drift_seconds <= self.config.timestamp_tolerance_seconds:
                            continue  # Within tolerance
                            
                        discrepancy_type = DiscrepancyType.TIMESTAMP_DRIFT
                        context = {"drift_seconds": drift_seconds}
                    else:
                        discrepancy_type = DiscrepancyType.VALUE_MISMATCH
                        context = {"field_name": field_name}
                else:
                    discrepancy_type = DiscrepancyType.VALUE_MISMATCH
                    context = {"field_name": field_name}
                
                discrepancy_id = self._generate_discrepancy_id(source_a.name, source_b.name, key_values, field_name)
                severity = self._calculate_severity(discrepancy_type, context)
                
                discrepancies.append(Discrepancy(
                    discrepancy_id=discrepancy_id,
                    discrepancy_type=discrepancy_type,
                    severity=severity,
                    source_a=source_a.name,
                    source_b=source_b.name,
                    key_values=key_values,
                    field_name=field_name,
                    value_a=value_a,
                    value_b=value_b,
                    context=context
                ))
        
        return discrepancies
    
    def _propose_fix(self, discrepancy: Discrepancy) -> Optional[ApprovalRequest]:
        """Propose a fix for a discrepancy through an approval request."""
        source_a = self.data_sources.get(discrepancy.source_a)
        source_b = self.data_sources.get(discrepancy.source_b)
        
        if not source_a or not source_b:
            return None
        
        # Determine authoritative source based on priority
        if source_a.priority > source_b.priority:
            authoritative_source = source_a
            target_source = source_b
            authoritative_value = discrepancy.value_a
        elif source_b.priority > source_a.priority:
            authoritative_source = source_b
            target_source = source_a
            authoritative_value = discrepancy.value_b
        else:
            # Equal priority - require manual review
            request_id = self._generate_approval_request_id(discrepancy.discrepancy_id, ApprovalAction.MANUAL_REVIEW)
            return ApprovalRequest(
                request_id=request_id,
                action=ApprovalAction.MANUAL_REVIEW,
                target_source="manual",
                discrepancy_id=discrepancy.discrepancy_id,
                description=f"Manual review required: Equal priority sources have conflicting values",
                proposed_changes={},
                impact_assessment="Requires human decision on which source is correct",
                rollback_plan="No automated rollback available for manual decisions",
                confidence_score=0.5
            )
        
        # Enhanced read-only safety checks
        if target_source.read_only:
            # Single read-only target - no fix possible
            return None
            
        # Both sources read-only - require manual review with explicit note
        if source_a.read_only and source_b.read_only:
            request_id = self._generate_approval_request_id(discrepancy.discrepancy_id, ApprovalAction.MANUAL_REVIEW)
            return ApprovalRequest(
                request_id=request_id,
                action=ApprovalAction.MANUAL_REVIEW,
                target_source="manual",
                discrepancy_id=discrepancy.discrepancy_id,
                description=f"Manual review required: Both sources are read-only ({source_a.name}, {source_b.name})",
                proposed_changes={},
                impact_assessment="No writable target available - both sources are read-only. Manual intervention required.",
                rollback_plan="No automated rollback available for read-only sources",
                confidence_score=0.5,
                metadata={"both_readonly": True, "readonly_sources": [source_a.name, source_b.name]}
            )
        
        # Determine action based on discrepancy type
        if discrepancy.discrepancy_type == DiscrepancyType.VALUE_MISMATCH:
            action = ApprovalAction.UPDATE_RECORD
            proposed_changes = {
                "table": target_source.name,
                "key_values": discrepancy.key_values,
                "field_updates": {discrepancy.field_name: authoritative_value}
            }
            description = f"Update {discrepancy.field_name} in {target_source.name} from {self._format_value_for_display(discrepancy.value_b if target_source == source_b else discrepancy.value_a)} to {self._format_value_for_display(authoritative_value)}"
            
        elif discrepancy.discrepancy_type == DiscrepancyType.TIMESTAMP_DRIFT:
            action = ApprovalAction.SYNC_TIMESTAMP
            proposed_changes = {
                "table": target_source.name,
                "key_values": discrepancy.key_values,
                "timestamp_field": target_source.timestamp_field,
                "new_timestamp": authoritative_value
            }
            description = f"Sync timestamp in {target_source.name} to match {authoritative_source.name}"
            
        elif discrepancy.discrepancy_type == DiscrepancyType.MISSING_RECORD:
            # Authoritative has record, target lacks it -> INSERT
            action = ApprovalAction.INSERT_RECORD
            # Get the full record from authoritative source context
            proposed_changes = {
                "table": target_source.name,
                "key_values": discrepancy.key_values,
                "action": "insert"
            }
            description = f"Insert missing record in {target_source.name} to match {authoritative_source.name}"
            
        elif discrepancy.discrepancy_type == DiscrepancyType.EXTRA_RECORD:
            # Target has record, authoritative lacks it -> DELETE (or MERGE if near-duplicate detected)
            # For now, default to DELETE - MERGE detection would require additional logic
            action = ApprovalAction.DELETE_RECORD
            proposed_changes = {
                "table": target_source.name,
                "key_values": discrepancy.key_values,
                "action": "delete"
            }
            description = f"Delete extra record from {target_source.name} (not in authoritative {authoritative_source.name})"
            
        else:
            # Default to manual review for complex cases
            action = ApprovalAction.MANUAL_REVIEW
            proposed_changes = {}
            description = f"Manual review required for {discrepancy.discrepancy_type.value}"
        
        request_id = self._generate_approval_request_id(discrepancy.discrepancy_id, action)
        
        # Enhanced confidence calculation with explainability
        priority_diff = abs(source_a.priority - source_b.priority)
        base_confidence = min(0.5 + (priority_diff * 0.1), 1.0)
        
        # Add signal strength for VALUE_MISMATCH and TIMESTAMP_DRIFT
        signal_strength = 1.0
        confidence_factors = {
            "priority_difference": priority_diff,
            "base_confidence": base_confidence
        }
        
        if discrepancy.discrepancy_type == DiscrepancyType.VALUE_MISMATCH and discrepancy.field_name:
            # Calculate signal strength based on relative delta vs tolerance
            if isinstance(discrepancy.value_a, (int, float)) and isinstance(discrepancy.value_b, (int, float)):
                field_config = self.config.field_tolerances.get(discrepancy.field_name, {})
                abs_tol = field_config.get("absolute_tolerance", self.config.float_tolerance)
                rel_tol = field_config.get("relative_tolerance", self.config.float_relative_tolerance)
                
                actual_diff = abs(discrepancy.value_a - discrepancy.value_b)
                max_val = max(abs(discrepancy.value_a), abs(discrepancy.value_b))
                
                # Signal strength = how much the difference exceeds tolerance
                abs_multiple = actual_diff / abs_tol if abs_tol > 0 else 1
                rel_multiple = (actual_diff / max_val) / rel_tol if rel_tol > 0 and max_val > 0 else 1
                signal_strength = min(max(abs_multiple, rel_multiple) / 10.0, 2.0)  # Cap at 2x
                
                confidence_factors["signal_strength"] = signal_strength
                confidence_factors["absolute_multiple"] = abs_multiple
                confidence_factors["relative_multiple"] = rel_multiple
                
        elif discrepancy.discrepancy_type == DiscrepancyType.TIMESTAMP_DRIFT:
            # Signal strength based on drift magnitude vs tolerance
            drift_seconds = discrepancy.context.get("drift_seconds", 0)
            tolerance_seconds = self.config.timestamp_tolerance_seconds
            if tolerance_seconds > 0:
                signal_strength = min(drift_seconds / tolerance_seconds, 5.0)  # Cap at 5x
                confidence_factors["signal_strength"] = signal_strength
                confidence_factors["drift_multiple"] = drift_seconds / tolerance_seconds
        
        # Calculate final confidence
        confidence = base_confidence * min(signal_strength, 1.5)  # Boost for strong signals
        
        # Adjust confidence based on discrepancy severity
        if discrepancy.severity == ReconciliationSeverity.CRITICAL:
            confidence = confidence * 0.8  # Lower confidence for critical issues
            confidence_factors["severity_adjustment"] = 0.8
        
        # Clamp to valid range
        confidence = max(0.1, min(confidence, 1.0))
        
        # Set expiry time
        expires_at = int(time.time() * 1_000_000) + int(self.config.approval_expiry_hours * 3600 * 1_000_000)
        
        return ApprovalRequest(
            request_id=request_id,
            action=action,
            target_source=target_source.name,
            discrepancy_id=discrepancy.discrepancy_id,
            description=description,
            proposed_changes=proposed_changes,
            impact_assessment=f"Will update 1 record in {target_source.name}. Severity: {discrepancy.severity.value}",
            rollback_plan=f"Can rollback by updating {discrepancy.field_name} back to original value",
            confidence_score=confidence,
            expires_at=expires_at,
            metadata={
                "authoritative_source": authoritative_source.name,
                "priority_difference": priority_diff,
                "discrepancy_type": discrepancy.discrepancy_type.value,
                "confidence_factors": confidence_factors,
                "auto_approve_eligible": (discrepancy.severity == ReconciliationSeverity.LOW and 
                                        self.config.auto_approve_low_severity),
                "dual_approval_required": (discrepancy.severity == ReconciliationSeverity.CRITICAL and 
                                         self.config.require_dual_approval_critical)
            }
        )
    
    def _generate_approval_signature(self, request: ApprovalRequest) -> str:
        """Generate a signature for approval request deduplication."""
        signature_components = [
            request.action.value,
            request.target_source,
            request.discrepancy_id,
            json.dumps(request.proposed_changes, sort_keys=True, separators=(',', ':'))  # Compact, deterministic
        ]
        content = "|".join(signature_components)
        return hashlib.sha256(content.encode()).hexdigest()[:12]
    
    def _should_emit_approval_request(self, request: ApprovalRequest) -> bool:
        """Check if approval request should be emitted (not duplicate within TTL)."""
        current_time = int(time.time() * 1_000_000)
        ttl_us = self.config.approval_expiry_hours * 3600 * 1_000_000
        
        # Check if we have a cached request for this discrepancy
        if request.discrepancy_id in self.approval_cache:
            last_time, last_signature = self.approval_cache[request.discrepancy_id]
            
            # If within TTL and same signature, suppress duplicate
            if (current_time - last_time) < ttl_us:
                current_signature = self._generate_approval_signature(request)
                if current_signature == last_signature:
                    return False  # Duplicate request, suppress
        
        return True  # Emit request
    
    def _cache_approval_request(self, request: ApprovalRequest) -> None:
        """Cache approval request signature for deduplication."""
        current_time = int(time.time() * 1_000_000)
        signature = self._generate_approval_signature(request)
        self.approval_cache[request.discrepancy_id] = (current_time, signature)
    
    def _prune_approval_cache(self) -> None:
        """Prune approval cache entries older than TTL to keep memory flat."""
        current_time = int(time.time() * 1_000_000)
        ttl_microseconds = int(self.config.approval_expiry_hours * 3600 * 1_000_000)
        cutoff_time = current_time - ttl_microseconds
        
        # Remove expired entries
        expired_keys = [
            discrepancy_id for discrepancy_id, (timestamp, _) in self.approval_cache.items()
            if timestamp < cutoff_time
        ]
        for key in expired_keys:
            del self.approval_cache[key]
    
    def _format_value_for_display(self, value: Any) -> str:
        """Format values deterministically for human-friendly descriptions."""
        if value is None:
            return "null"
        elif isinstance(value, float):
            return f"{value:.6f}"  # Fixed precision for deterministic display
        elif isinstance(value, int) and value > 1e12:  # Likely microsecond timestamp
            # Convert to ISO-8601 format
            timestamp_seconds = value / 1_000_000
            import datetime
            dt = datetime.datetime.fromtimestamp(timestamp_seconds, tz=datetime.timezone.utc)
            return dt.isoformat()
        else:
            return str(value)
    
    def _reconcile_in_memory(self, sources: List[DataSource], source_data: Dict[str, List[Dict[str, Any]]]) -> Tuple[List[Discrepancy], List[ApprovalRequest], int]:
        """Reconcile data in memory for smaller datasets."""
        # Build record maps keyed by unique identifiers
        record_maps = {}
        total_records = 0
        for source in sources:
            record_map = {}
            for record in source_data[source.name]:
                # Normalize first, then extract key from normalized record
                normalized_record = self._normalize_record(record, source)
                key = tuple(self._extract_key(normalized_record, source.key_fields).values())
                record_map[key] = record  # Store original record, keyed by normalized key
                total_records += 1
            record_maps[source.name] = record_map
            
        discrepancies = self._compare_source_pairs(sources, record_maps)
        return discrepancies, [], total_records
    
    async def _reconcile_chunked(self, sources: List[DataSource], source_data: Dict[str, List[Dict[str, Any]]], 
                               report: ReconciliationReport) -> Tuple[List[Discrepancy], List[ApprovalRequest], int]:
        """Reconcile data in chunks for memory efficiency."""
        discrepancies = []
        total_records_processed = 0
        
        # Sort data by keys for merge-join processing (using normalized keys)
        sorted_data = {}
        for source in sources:
            data = source_data[source.name]
            # Sort using normalized keys for consistent ordering across sources
            sorted_data[source.name] = sorted(data, key=lambda x: tuple(
                self._extract_key(self._normalize_record(x, source), source.key_fields).values()
            ))
        
        # Process in chunks
        batch_size = self.config.batch_size
        max_records = max(len(data) for data in sorted_data.values())
        
        for offset in range(0, max_records, batch_size):
            # Extract chunk from each source
            chunk_maps = {}
            chunk_records = 0
            for source in sources:
                data = sorted_data[source.name]
                chunk = data[offset:offset + batch_size]
                chunk_map = {}
                for record in chunk:
                    # Use normalized key for consistent alignment across sources
                    normalized_record = self._normalize_record(record, source)
                    key = tuple(self._extract_key(normalized_record, source.key_fields).values())
                    chunk_map[key] = record  # Store original record, keyed by normalized key
                    chunk_records += 1
                chunk_maps[source.name] = chunk_map
            
            # Compare this chunk
            chunk_discrepancies = self._compare_source_pairs(sources, chunk_maps)
            discrepancies.extend(chunk_discrepancies)
            total_records_processed += chunk_records
            
            # Update report incrementally
            report.summary_stats["total_discrepancies"] = len(discrepancies)
            
            # Early termination on max discrepancies
            if len(discrepancies) >= self.config.max_discrepancies_per_run:
                report.metadata["early_termination"] = True
                report.metadata["reason"] = "max_discrepancies_reached"
                break
        
        return discrepancies, [], total_records_processed
    
    def _compare_source_pairs(self, sources: List[DataSource], record_maps: Dict[str, Dict]) -> List[Discrepancy]:
        """Compare all source pairs for discrepancies."""
        discrepancies = []
        
        for i, source_a in enumerate(sources):
            for source_b in sources[i + 1:]:
                map_a = record_maps[source_a.name]
                map_b = record_maps[source_b.name]
                
                # Find all unique keys
                keys_a = set(map_a.keys())
                keys_b = set(map_b.keys())
                all_keys = keys_a.union(keys_b)
                
                for key in all_keys:
                    record_a = map_a.get(key)
                    record_b = map_b.get(key)
                    
                    if record_a is None and record_b is not None:
                        # Missing in source A - use normalized record for canonical key evidence
                        normalized_b = self._normalize_record(record_b, source_b)
                        key_values = self._extract_key(normalized_b, source_b.key_fields)
                        
                        # If source A has higher priority, it's missing from authoritative (MISSING)
                        # If source B has higher priority, it's extra in lower priority (EXTRA)
                        if source_a.priority >= source_b.priority:
                            discrepancy_type = DiscrepancyType.MISSING_RECORD
                            discrepancy_id = self._generate_discrepancy_id(source_a.name, source_b.name, key_values, "missing")
                            context = {
                                "missing_from": source_a.name, 
                                "authoritative_source": source_a.name,
                                "affects_high_priority_source": True  # Missing from authoritative
                            }
                        else:
                            discrepancy_type = DiscrepancyType.EXTRA_RECORD
                            discrepancy_id = self._generate_discrepancy_id(source_a.name, source_b.name, key_values, "extra")
                            context = {
                                "extra_in": source_b.name, 
                                "authoritative_source": source_a.name,
                                "affects_high_priority_source": False  # Extra in low-priority
                            }
                        
                        discrepancies.append(Discrepancy(
                            discrepancy_id=discrepancy_id,
                            discrepancy_type=discrepancy_type,
                            severity=self._calculate_severity(discrepancy_type, context),
                            source_a=source_a.name,
                            source_b=source_b.name,
                            key_values=key_values,
                            context=context
                        ))
                        
                    elif record_b is None and record_a is not None:
                        # Missing in source B - use normalized record for canonical key evidence
                        normalized_a = self._normalize_record(record_a, source_a)
                        key_values = self._extract_key(normalized_a, source_a.key_fields)
                        
                        # If source B has higher priority, it's missing from authoritative (MISSING)
                        # If source A has higher priority, it's extra in lower priority (EXTRA)
                        if source_b.priority >= source_a.priority:
                            discrepancy_type = DiscrepancyType.MISSING_RECORD
                            discrepancy_id = self._generate_discrepancy_id(source_a.name, source_b.name, key_values, "missing")
                            context = {
                                "missing_from": source_b.name, 
                                "authoritative_source": source_b.name,
                                "affects_high_priority_source": True  # Missing from authoritative
                            }
                        else:
                            discrepancy_type = DiscrepancyType.EXTRA_RECORD
                            discrepancy_id = self._generate_discrepancy_id(source_a.name, source_b.name, key_values, "extra")
                            context = {
                                "extra_in": source_a.name, 
                                "authoritative_source": source_b.name,
                                "affects_high_priority_source": False  # Extra in low-priority
                            }
                        
                        discrepancies.append(Discrepancy(
                            discrepancy_id=discrepancy_id,
                            discrepancy_type=discrepancy_type,
                            severity=self._calculate_severity(discrepancy_type, context),
                            source_a=source_a.name,
                            source_b=source_b.name,
                            key_values=key_values,
                            context=context
                        ))
                        
                    elif record_a is not None and record_b is not None:
                        # Compare records
                        record_discrepancies = self.compare_records(record_a, record_b, source_a, source_b)
                        discrepancies.extend(record_discrepancies)
                        
                    # Stop if we hit the discrepancy limit
                    if len(discrepancies) >= self.config.max_discrepancies_per_run:
                        break
                
                if len(discrepancies) >= self.config.max_discrepancies_per_run:
                    break
            
            if len(discrepancies) >= self.config.max_discrepancies_per_run:
                break
        
        # Check for ordering differences when sources have ordering keys
        ordering_sources = [(s, record_maps[s.name]) for s in sources if s.ordering_key]
        if len(ordering_sources) >= 2:
            ordering_discrepancies = self._detect_ordering_differences(ordering_sources)
            discrepancies.extend(ordering_discrepancies)
        
        return discrepancies
    
    def _detect_ordering_differences(self, ordering_sources: List[Tuple[DataSource, Dict]]) -> List[Discrepancy]:
        """Detect ordering differences between sources that guarantee order."""
        discrepancies = []
        
        for i, (source_a, map_a) in enumerate(ordering_sources):
            for source_b, map_b in ordering_sources[i + 1:]:
                # Get common records
                keys_a = set(map_a.keys())
                keys_b = set(map_b.keys()) 
                common_keys = keys_a.intersection(keys_b)
                
                if len(common_keys) < 2:
                    continue  # Need at least 2 records to compare order
                
                # Extract ordering values for common records
                ordering_a = []
                ordering_b = []
                
                for key in common_keys:
                    record_a = map_a[key]
                    record_b = map_b[key]
                    
                    # Normalize records and get ordering field value from normalized records
                    normalized_a = self._normalize_record(record_a, source_a)
                    normalized_b = self._normalize_record(record_b, source_b)
                    
                    if source_a.ordering_key and source_b.ordering_key:
                        # Use canonical field names after normalization
                        value_a = normalized_a.get(source_a.ordering_key)
                        value_b = normalized_b.get(source_b.ordering_key)
                        
                        if value_a is not None and value_b is not None:
                            ordering_a.append((value_a, key))
                            ordering_b.append((value_b, key))
                
                # Sort by ordering values
                ordering_a.sort(key=lambda x: x[0])
                ordering_b.sort(key=lambda x: x[0])
                
                # Compare relative order
                order_a = [key for _, key in ordering_a]
                order_b = [key for _, key in ordering_b]
                
                if order_a != order_b:
                    # Find divergence points
                    divergences = []
                    for idx, (key_a, key_b) in enumerate(zip(order_a, order_b)):
                        if key_a != key_b:
                            divergences.append({
                                "position": idx,
                                "key_a": key_a,
                                "key_b": key_b,
                                "ordering_value_a": ordering_a[idx][0],
                                "ordering_value_b": ordering_b[idx][0]
                            })
                    
                    # Create discrepancy for ordering difference
                    context = {
                        "ordering_field_a": source_a.ordering_key,
                        "ordering_field_b": source_b.ordering_key,
                        "common_records": len(common_keys),
                        "divergences": divergences[:5],  # Limit to first 5 divergences
                        "total_divergences": len(divergences)
                    }
                    
                    discrepancy_id = self._generate_discrepancy_id(
                        source_a.name, source_b.name, 
                        {"ordering_comparison": f"{source_a.ordering_key}_{source_b.ordering_key}"}, 
                        "ordering"
                    )
                    
                    discrepancies.append(Discrepancy(
                        discrepancy_id=discrepancy_id,
                        discrepancy_type=DiscrepancyType.ORDERING_DIFFERENCE,
                        severity=self._calculate_severity(DiscrepancyType.ORDERING_DIFFERENCE, context),
                        source_a=source_a.name,
                        source_b=source_b.name,
                        key_values={"ordering_comparison": f"{source_a.ordering_key}_{source_b.ordering_key}"},
                        field_name="ordering",
                        context=context
                    ))
        
        return discrepancies
    
    async def reconcile_sources(self, source_names: List[str], 
                              query_params: Optional[Dict[str, Any]] = None) -> ReconciliationReport:
        """
        Perform reconciliation between specified data sources with circuit breaker protection.
        
        Args:
            source_names: List of source names to reconcile
            query_params: Optional query parameters for data fetching
            
        Returns:
            ReconciliationReport with discrepancies and approval requests
        """
        # Check circuit breaker before starting
        can_execute = True
        if hasattr(self.streaming_bus, 'can_component_execute'):
            try:
                can_execute = await self.streaming_bus.can_component_execute(self.circuit_breaker_id)
            except Exception as e:
                print(f"🔄 Reconciler Agent: Circuit breaker check failed: {e}")
        
        if not can_execute:
            raise RuntimeError(f"Reconciler Agent circuit breaker is open - component {self.circuit_breaker_id} cannot execute")
        
        start_time = int(time.time() * 1_000_000)
        
        # Prune expired approval cache entries to keep memory flat
        self._prune_approval_cache()
        
        # Generate deterministic report ID using stable hash
        sorted_sources = ",".join(sorted(source_names))
        content = f"{sorted_sources}_{start_time}"
        report_hash = hashlib.sha256(content.encode()).hexdigest()[:12]
        report_id = f"recon_report_{report_hash}"
        
        # Validate sources
        missing_sources = [name for name in source_names if name not in self.data_sources]
        if missing_sources:
            raise ValueError(f"Unknown data sources: {missing_sources}")
        
        sources = [self.data_sources[name] for name in source_names]
        
        # Initialize report
        report = ReconciliationReport(
            report_id=report_id,
            session_id=self.session_id,
            started_at=start_time,
            sources_compared=source_names
        )
        
        try:
            # Fetch data from all sources with error tracking
            source_data = {}
            fetch_errors = {}
            for source in sources:
                data, error = await self._fetch_data_with_retry(source, query_params)
                source_data[source.name] = data
                if error:
                    fetch_errors[source.name] = error
            
            # Record fetch errors in metadata
            if fetch_errors:
                report.metadata["fetch_errors"] = fetch_errors
                
            # Process data in chunks for memory efficiency
            discrepancies = []
            approval_requests = []
            total_records_processed = 0
            
            # Use chunked reconciliation for large datasets
            if any(len(data) > self.config.batch_size for data in source_data.values()):
                discrepancies, approval_requests, total_records_processed = await self._reconcile_chunked(
                    sources, source_data, report
                )
            else:
                # Small datasets - process in memory
                discrepancies, approval_requests, total_records_processed = self._reconcile_in_memory(
                    sources, source_data
                )
            # Generate approval requests for discrepancies with deduplication
            for discrepancy in discrepancies:
                approval_request = self._propose_fix(discrepancy)
                if approval_request and self._should_emit_approval_request(approval_request):
                    approval_requests.append(approval_request)
                    # Cache this request to prevent duplicates
                    self._cache_approval_request(approval_request)
            
            # Calculate summary statistics with enhanced metrics
            total_records = total_records_processed
            severity_counts = defaultdict(int)
            for discrepancy in discrepancies:
                severity_counts[discrepancy.severity.value] += 1
                # Track high/critical severity counts
                if discrepancy.severity == ReconciliationSeverity.HIGH:
                    self.reconciliation_stats["high_severity_discrepancies"] += 1
                elif discrepancy.severity == ReconciliationSeverity.CRITICAL:
                    self.reconciliation_stats["critical_severity_discrepancies"] += 1
            
            # Track approval request types
            for approval_request in approval_requests:
                self.reconciliation_stats["approval_requests_generated"] += 1
                if approval_request.metadata.get("auto_approve_eligible", False):
                    self.reconciliation_stats["auto_approval_eligible"] += 1
                if approval_request.action == ApprovalAction.MANUAL_REVIEW:
                    self.reconciliation_stats["manual_review_required"] += 1
            
            # Update report with enhanced metadata for reproducibility
            report.completed_at = int(time.time() * 1_000_000)
            report.total_records_compared = total_records
            report.discrepancies = discrepancies
            report.approval_requests = approval_requests
            
            # Calculate reconciliation window (min/max timestamps per source)
            reconciliation_window = {}
            sources_version = {}
            for source in sources:
                data = source_data[source.name]
                if data and source.timestamp_field:
                    timestamps = []
                    for record in data:
                        ts_value = record.get(source.timestamp_field)
                        if isinstance(ts_value, (int, float)):
                            # Normalize to microseconds
                            normalized_ts = self._normalize_timestamp(ts_value)
                            timestamps.append(normalized_ts)
                    
                    if timestamps:
                        reconciliation_window[source.name] = {
                            "min_timestamp": min(timestamps),
                            "max_timestamp": max(timestamps),
                            "min_timestamp_iso": self._format_value_for_display(min(timestamps)),
                            "max_timestamp_iso": self._format_value_for_display(max(timestamps))
                        }
                
                # Track source version/snapshot info if available in query params
                if query_params:
                    source_version = query_params.get(f"{source.name}_version") or query_params.get("version")
                    if source_version:
                        sources_version[source.name] = source_version
            
            # Enhanced metadata for reproducibility
            report.metadata.update({
                "reconciliation_window": reconciliation_window,
                "sources_version": sources_version,
                "config_snapshot": {
                    "float_tolerance": self.config.float_tolerance,
                    "timestamp_tolerance_seconds": self.config.timestamp_tolerance_seconds,
                    "batch_size": self.config.batch_size,
                    "max_discrepancies_per_run": self.config.max_discrepancies_per_run
                },
                "circuit_breaker_id": self.circuit_breaker_id
            })
            
            report.summary_stats = {
                "total_discrepancies": len(discrepancies),
                "total_approval_requests": len(approval_requests),
                "severity_breakdown": dict(severity_counts),
                "discrepancy_rate": len(discrepancies) / max(total_records, 1),
                "sources_in_sync": len(discrepancies) == 0
            }
            
            processing_time_seconds = (report.completed_at - report.started_at) / 1_000_000
            report.performance_metrics = {
                "processing_time_seconds": processing_time_seconds,
                "records_per_second": total_records / max(processing_time_seconds, 0.001),
                "discrepancies_per_second": len(discrepancies) / max(processing_time_seconds, 0.001),
                "source_comparison_pairs": len(sources) * (len(sources) - 1) // 2
            }
            
            # Update comprehensive statistics
            self.reconciliation_stats["total_runs"] += 1
            self.reconciliation_stats["successful_reconciliations"] += 1
            self.reconciliation_stats["total_discrepancies"] += len(discrepancies)
            self.reconciliation_stats["total_approval_requests"] += len(approval_requests)
            self.reconciliation_stats["total_records_processed"] += total_records
            self.reconciliation_stats["source_comparison_pairs"] += report.performance_metrics["source_comparison_pairs"]
            
            # Update running averages
            if self.reconciliation_stats["total_runs"] > 0:
                self.reconciliation_stats["avg_processing_time"] = (
                    (self.reconciliation_stats["avg_processing_time"] * (self.reconciliation_stats["total_runs"] - 1) + 
                     processing_time_seconds) / self.reconciliation_stats["total_runs"]
                )
            
            # Record success with circuit breaker
            if hasattr(self.streaming_bus, 'record_component_success'):
                try:
                    await self.streaming_bus.record_component_success(self.circuit_breaker_id)
                    self.consecutive_failures = 0
                    self.last_successful_reconciliation = time.time()
                except Exception as cb_error:
                    print(f"🔄 Reconciler Agent: Failed to record circuit breaker success: {cb_error}")
            
            # Cache report
            self.recent_reports.append(report)
            
            return report
            
        except Exception as e:
            # Update report with error information
            report.completed_at = int(time.time() * 1_000_000)
            report.metadata["error"] = str(e)
            report.metadata["error_type"] = type(e).__name__
            
            # Update failure statistics
            self.reconciliation_stats["failed_reconciliations"] += 1
            self.consecutive_failures += 1
            
            # Record failure with circuit breaker
            if hasattr(self.streaming_bus, 'record_component_failure'):
                try:
                    await self.streaming_bus.record_component_failure(self.circuit_breaker_id)
                except Exception as cb_error:
                    print(f"🔄 Reconciler Agent: Failed to record circuit breaker failure: {cb_error}")
            
            raise
    
    async def _fetch_data_with_retry(self, source: DataSource, query_params: Optional[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Fetch data from a source with exponential backoff retry logic and circuit breaker integration."""
        max_retries = 3
        base_delay = 1.0
        
        for attempt in range(max_retries):
            try:
                # Check circuit breaker before each attempt
                can_execute = True
                if hasattr(self.streaming_bus, 'can_component_execute'):
                    try:
                        can_execute = await self.streaming_bus.can_component_execute(self.circuit_breaker_id)
                    except Exception:
                        pass  # Continue on circuit breaker check failure
                
                if not can_execute:
                    return [], f"Circuit breaker open for component {self.circuit_breaker_id}"
                
                # Attempt to fetch data
                data, error = await self._fetch_data(source, query_params)
                return data, error  # Success, return immediately
                
            except Exception as e:
                if attempt == max_retries - 1:  # Last attempt
                    return [], f"Failed after {max_retries} attempts: {str(e)}"
                else:
                    # Exponential backoff with jitter
                    delay = base_delay * (2 ** attempt) + (0.1 * attempt)
                    await asyncio.sleep(delay)
                    print(f"🔄 Reconciler Agent: Retry {attempt + 1}/{max_retries} for {source.name} after {delay:.2f}s: {e}")
        
        # This should never be reached, but just in case
        return [], "Unexpected end of retry loop"
    
    def get_reconciliation_stats(self) -> Dict[str, Any]:
        """Get comprehensive reconciliation statistics."""
        return {
            **self.reconciliation_stats,
            "session_id": self.session_id,
            "sources_registered": len(self.data_sources),
            "recent_reports_count": len(self.recent_reports),
            "cache_size": len(self.discrepancy_cache)
        }
    
    def get_recent_reports(self, limit: int = 10) -> List[ReconciliationReport]:
        """Get recent reconciliation reports."""
        return list(self.recent_reports)[-limit:]
    
    async def start(self):
        """Start the reconciler agent with Kafka control consumption and health monitoring."""
        print("🔄 Starting Reconciler Agent...")
        
        # Register with circuit breaker system
        try:
            if hasattr(self.streaming_bus, 'register_circuit_breaker'):
                await self.streaming_bus.register_circuit_breaker(
                    component_id=self.circuit_breaker_id,
                    failure_threshold=self.max_consecutive_failures
                )
                print(f"🔄 Reconciler Agent: Registered circuit breaker with ID: {self.circuit_breaker_id}")
        except Exception as e:
            print(f"🔄 Reconciler Agent: Warning - Could not register circuit breaker: {e}")
        
        # Start health monitoring
        self._monitoring_task = asyncio.create_task(self._health_monitoring_loop())
        
        # Start Kafka control message consumption
        control_task = asyncio.create_task(self._consume_control_messages())
        
        # Start clean data consumption for real-time reconciliation
        data_task = asyncio.create_task(self._consume_clean_data())
        
        print("🔄 Reconciler Agent started with control and data consumption")
    
    async def _health_monitoring_loop(self):
        """Health monitoring loop with circuit breaker integration."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.health_check_interval)
                
                if self._shutdown_event.is_set():
                    break
                
                # Check if circuit breaker allows execution
                can_execute = True
                if hasattr(self.streaming_bus, 'can_component_execute'):
                    try:
                        can_execute = await self.streaming_bus.can_component_execute(self.circuit_breaker_id)
                    except Exception as e:
                        print(f"🔄 Reconciler Agent: Circuit breaker check failed: {e}")
                
                if not can_execute:
                    print(f"🔄 Reconciler Agent: Circuit breaker open, skipping health check")
                    continue
                
                # Perform health check
                await self._perform_health_check()
                
                # Update metrics
                await self._update_health_metrics()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"🔄 Reconciler Agent: Health monitoring error: {e}")
                self.consecutive_failures += 1
                
                # Trip circuit breaker on repeated failures
                if (self.consecutive_failures >= self.max_consecutive_failures and 
                    hasattr(self.streaming_bus, 'record_component_failure')):
                    try:
                        await self.streaming_bus.record_component_failure(
                            component_id=self.circuit_breaker_id
                        )
                        self.reconciliation_stats["circuit_breaker_trips"] += 1
                        print(f"🔄 Reconciler Agent: Circuit breaker tripped after {self.consecutive_failures} failures")
                    except Exception as cb_error:
                        print(f"🔄 Reconciler Agent: Failed to record circuit breaker failure: {cb_error}")
    
    async def _perform_health_check(self):
        """Perform health check on registered data sources."""
        try:
            health_check_start = time.time()
            
            # Check connectivity to registered data sources
            healthy_sources = 0
            total_sources = len(self.data_sources)
            
            for source_name, source in self.data_sources.items():
                try:
                    # Simplified health check - in production would check actual connectivity
                    if source.connection_params:
                        healthy_sources += 1
                except Exception as source_error:
                    print(f"🔄 Reconciler Agent: Health check failed for source {source_name}: {source_error}")
            
            # Consider healthy if at least half of sources are accessible
            health_ratio = healthy_sources / max(total_sources, 1)
            is_healthy = health_ratio >= 0.5 or total_sources == 0  # Healthy if no sources registered yet
            
            if is_healthy:
                self.consecutive_failures = 0
                self.last_successful_reconciliation = time.time()
                
                # Record success with circuit breaker
                if hasattr(self.streaming_bus, 'record_component_success'):
                    await self.streaming_bus.record_component_success(self.circuit_breaker_id)
            else:
                self.consecutive_failures += 1
                print(f"🔄 Reconciler Agent: Health check failed - {healthy_sources}/{total_sources} sources healthy")
            
            health_check_duration = time.time() - health_check_start
            print(f"🔄 Reconciler Agent: Health check completed in {health_check_duration:.3f}s - {healthy_sources}/{total_sources} sources healthy")
            
        except Exception as e:
            print(f"🔄 Reconciler Agent: Health check error: {e}")
            self.consecutive_failures += 1
    
    async def _update_health_metrics(self):
        """Update health and performance metrics."""
        try:
            current_time = time.time()
            
            # Calculate cache hit rate
            total_cache_access = (self.reconciliation_stats.get("total_discrepancies", 0) + 
                                len(self.discrepancy_cache))
            cache_hits = len(self.discrepancy_cache)
            self.reconciliation_stats["cache_hit_rate"] = (
                cache_hits / max(total_cache_access, 1)
            ) * 100.0
            
            # Update averages
            total_runs = self.reconciliation_stats["total_runs"]
            if total_runs > 0:
                self.reconciliation_stats["avg_discrepancy_rate"] = (
                    self.reconciliation_stats["total_discrepancies"] / 
                    max(self.reconciliation_stats["total_records_processed"], 1)
                ) * 100.0
            
            # Publish metrics to Kafka (optional)
            if hasattr(self.streaming_bus, 'publish'):
                try:
                    metrics_message = {
                        "component_id": self.circuit_breaker_id,
                        "timestamp": int(current_time * 1_000_000),
                        "metrics": {
                            "consecutive_failures": self.consecutive_failures,
                            "last_successful_reconciliation": self.last_successful_reconciliation,
                            "cache_hit_rate": self.reconciliation_stats["cache_hit_rate"],
                            "avg_discrepancy_rate": self.reconciliation_stats["avg_discrepancy_rate"],
                            "sources_registered": len(self.data_sources),
                            "pending_tasks": len(self._pending_tasks)
                        }
                    }
                    
                    # Use fire-and-forget publishing to avoid blocking health monitoring
                    asyncio.create_task(self.streaming_bus.publish(
                        topic="metrics.reconciler",
                        partition_key=self.circuit_breaker_id,
                        payload=metrics_message
                    ))
                except Exception as pub_error:
                    # Don't fail health monitoring due to metrics publishing issues
                    pass
                    
        except Exception as e:
            print(f"🔄 Reconciler Agent: Metrics update error: {e}")
    
    async def _consume_clean_data(self):
        """Consume clean data streams for real-time reconciliation monitoring."""
        clean_data_topics = [
            "clean.market.trades",
            "clean.market.book", 
            "clean.market.funding",
            "clean.market.oi",
            "clean.market.onchain",
            "clean.market.options",
            "clean.market.events"
        ]
        
        print(f"🔄 Reconciler Agent: Starting clean data consumption from topics: {clean_data_topics}")
        
        try:
            # Use worker pool for parallel reconciliation processing
            await self.streaming_bus.subscribe_with_worker_pool(
                consumer_group="reconciler_data_monitor",
                topics=clean_data_topics,
                handler=self._handle_clean_data_message_smart,
                pool_size=8  # Parallel reconciliation checks
            )
                
        except Exception as e:
            print(f"🔄 Reconciler Agent: Error in clean data consumption: {e}")
            # Record failure with circuit breaker
            await self.streaming_bus.record_component_failure(
                component_id="reconciler_agent",
                cascade_failure=False,
                reason="reconciler_clean_data_subscription_failure",
                severity="medium"
            )
    
    def _handle_clean_data_wrapper(self, topic: str, partition_key: str, 
                                 message: dict, headers: dict):
        """Wrapper to handle clean data messages."""
        # Schedule the async handler
        asyncio.create_task(self._handle_clean_data_message(topic, message, headers))
    
    def _handle_clean_data_message_smart(self, topic: str, partition_key: str, payload: Dict[str, Any], headers: Dict[str, str]) -> None:
        """Smart handler for clean data messages with real-time reconciliation triggers."""
        try:
            # Extract reconciliation metadata
            table_name = payload.get("table_name") or self._extract_table_from_topic(topic)
            venue = headers.get("venue", "unknown")
            timestamp = payload.get("timestamp", int(time.time_ns() // 1000))
            
            # Schedule async reconciliation analysis
            asyncio.create_task(self._process_reconciliation_trigger_async(table_name, venue, payload, headers, timestamp))
            
        except Exception as e:
            print(f"🔄 Reconciler Agent: Error handling clean data from {topic}: {e}")
    
    def _extract_table_from_topic(self, topic: str) -> str:
        """Extract table name from topic for reconciliation grouping."""
        if "trades" in topic:
            return "market_trades"
        elif "book" in topic:
            return "market_book"
        elif "funding" in topic:
            return "market_funding"
        elif "options" in topic:
            return "options_surface"
        elif "onchain" in topic:
            return "onchain_flows"
        else:
            return "unknown_table"
    
    async def _process_reconciliation_trigger_async(self, table_name: str, venue: str, payload: Dict[str, Any], 
                                                  headers: Dict[str, str], timestamp: int) -> None:
        """Asynchronously process reconciliation triggers with smart pattern detection."""
        try:
            # Track data arrival by venue and table
            data_key = f"{table_name}_{venue}"
            instrument_id = payload.get("instrument_id") or payload.get("symbol", "unknown")
            
            # Smart reconciliation logic
            if table_name in ["market_trades", "options_surface", "market_book"]:
                # Check if we have recent data from multiple venues for same instrument
                if instrument_id and instrument_id != "unknown":
                    await self._check_cross_venue_reconciliation(table_name, instrument_id, venue, payload, timestamp)
            
            # Check for temporal reconciliation opportunities
            if table_name == "market_trades":
                await self._check_temporal_patterns(venue, payload, timestamp)
            
            # Check for data completeness 
            await self._check_data_completeness(table_name, venue, payload, timestamp)
            
            print(f"🔄 Reconciler Agent: Processed {table_name} from {venue} (instrument: {instrument_id})")
            
        except Exception as e:
            print(f"🔄 Reconciler Agent: Error in reconciliation processing for {table_name}: {e}")
    
    async def _check_temporal_patterns(self, venue: str, payload: Dict[str, Any], timestamp: int) -> None:
        """Check for temporal reconciliation patterns."""
        try:
            # Simple temporal analysis - check for data ordering issues
            trade_timestamp = payload.get("timestamp", timestamp)
            time_diff = timestamp - trade_timestamp
            
            if time_diff > 300_000_000:  # More than 5 minutes delay
                print(f"🔄 Reconciler Agent: Detected temporal anomaly for {venue} - {time_diff/1_000_000:.1f}s delay")
                
        except Exception as e:
            print(f"🔄 Reconciler Agent: Error in temporal pattern check: {e}")
    
    async def _check_data_completeness(self, table_name: str, venue: str, payload: Dict[str, Any], timestamp: int) -> None:
        """Check for data completeness reconciliation opportunities."""
        try:
            # Track data completeness metrics
            required_fields = self._get_required_fields_for_table(table_name)
            missing_fields = [field for field in required_fields if field not in payload]
            
            if missing_fields:
                print(f"🔄 Reconciler Agent: Data completeness issue for {table_name} from {venue} - missing: {missing_fields}")
                
        except Exception as e:
            print(f"🔄 Reconciler Agent: Error in data completeness check: {e}")
    
    def _get_required_fields_for_table(self, table_name: str) -> List[str]:
        """Get required fields for a data table."""
        field_mapping = {
            "market_trades": ["price", "volume", "timestamp", "side"],
            "market_book": ["bids", "asks", "timestamp"],
            "options_surface": ["strike", "expiry", "iv", "timestamp"],
            "market_funding": ["rate", "timestamp"],
            "onchain_flows": ["block_height", "timestamp"]
        }
        return field_mapping.get(table_name, [])
    
    async def _check_cross_venue_reconciliation(self, table_name: str, instrument_id: str, venue: str, 
                                              payload: Dict[str, Any], timestamp: int) -> None:
        """Check for cross-venue reconciliation opportunities."""
        try:
            # Store recent data point for cross-venue comparison
            reconciliation_key = f"{table_name}_{instrument_id}"
            
            # Simple heuristic: if we haven't seen this data combination recently, store it
            # In production, this would trigger sophisticated reconciliation logic
            if reconciliation_key not in getattr(self, '_recent_data_cache', {}):
                if not hasattr(self, '_recent_data_cache'):
                    self._recent_data_cache = {}
                
                self._recent_data_cache[reconciliation_key] = {
                    'venue': venue,
                    'timestamp': timestamp,
                    'payload_sample': payload
                }
                
                print(f"🔄 Reconciler Agent: Cached {reconciliation_key} from {venue} for cross-venue reconciliation")
                
        except Exception as e:
            print(f"🔄 Reconciler Agent: Error in cross-venue reconciliation check: {e}")
    
    async def _handle_clean_data_message(self, topic: str, message: dict, headers: dict):
        """Handle clean data messages for potential reconciliation triggers."""
        try:
            table_name = message.get("table_name")
            venue = headers.get("venue", "unknown")
            
            # Track data arrival for reconciliation scheduling
            data_key = f"{table_name}_{venue}"
            
            # Simple heuristic: if we have data from multiple venues for same table,
            # we might want to trigger reconciliation
            if table_name in ["exchange_trades", "market_trades", "options_surface"]:
                print(f"🔄 Reconciler Agent: Received {table_name} data from {venue}")
                # Could implement smart reconciliation triggers here
                # For now, just log the data flow
                
        except Exception as e:
            print(f"🔄 Reconciler Agent: Error handling clean data from {topic}: {e}")
    
    async def _consume_control_messages(self):
        """Consume control messages from Kafka topics for dynamic configuration."""
        control_topics = [
            "control.circuit_breaker",
            "control.config_update", 
            "control.reconciliation_schedule",
            "control.data_sources"
        ]
        
        print(f"🔄 Reconciler Agent: Starting control message consumption from topics: {control_topics}")
        
        try:
            await self.streaming_bus.subscribe(
                consumer_group="reconciler_agent_control",
                topics=control_topics,
                handler=self._handle_control_message_wrapper
            )
                
        except Exception as e:
            print(f"🔄 Reconciler Agent: Error in control message consumption: {e}")
            # Use the system circuit breaker to record failure
            await self.streaming_bus.record_component_failure(
                component_id="reconciler_agent",
                cascade_failure=False,
                reason="reconciler_control_listener_failure",
                severity="medium"
            )
    
    def _handle_control_message_wrapper(self, topic: str, partition_key: str, 
                                      message: dict, headers: dict):
        """Wrapper to handle the subscribe callback signature."""
        # Schedule the async handler and store task reference
        task = asyncio.create_task(self._handle_control_message(topic, message))
        self._pending_tasks.add(task)
        
        # Add callback to remove task when done
        def task_done_callback(completed_task):
            self._pending_tasks.discard(completed_task)
            # Check for exceptions
            exc = completed_task.exception()
            if exc:
                print(f"🔄 Reconciler Agent: Control message handler failed: {exc}")
        
        task.add_done_callback(task_done_callback)
    
    async def _handle_control_message(self, topic: str, message: dict):
        """Handle control messages for dynamic behavior adjustment."""
        try:
            if topic == "control.circuit_breaker":
                # Handle circuit breaker commands
                component_id = message.get("component_id")
                if component_id == "reconciler_agent" or component_id == "all":
                    action = message.get("action")
                    if action == "open":
                        print(f"🔄 Reconciler Agent: Circuit breaker opened via control message")
                        await self.streaming_bus.record_component_failure(
                            component_id="reconciler_agent",
                            cascade_failure=False,
                            reason="reconciler_control_open_request",
                            severity="medium"
                        )
                    elif action == "close":
                        print("🔄 Reconciler Agent: Circuit breaker closed via control message")
                        await self.streaming_bus.record_component_success(
                            component_id="reconciler_agent",
                            reason="reconciler_control_close_request",
                            severity="low"
                        )
                        
            elif topic == "control.config_update":
                # Handle dynamic configuration updates
                component_id = message.get("component_id")
                if component_id == "reconciler_agent" or component_id == "all":
                    config_updates = message.get("updates", {})
                    await self._apply_config_updates(config_updates)
                    
            elif topic == "control.reconciliation_schedule":
                # Handle scheduled reconciliation requests
                source_names = message.get("source_names", [])
                priority = message.get("priority", "normal")
                if source_names:
                    print(f"🔄 Reconciler Agent: Scheduled reconciliation for sources: {source_names} (priority: {priority})")
                    try:
                        # Trigger immediate reconciliation
                        report = await self.reconcile_sources(source_names)
                        print(f"🔄 Reconciler Agent: Reconciliation completed. Found {len(report.discrepancies)} discrepancies")
                    except Exception as recon_error:
                        print(f"🔄 Reconciler Agent: Reconciliation failed: {recon_error}")
                        
            elif topic == "control.data_sources":
                # Handle data source configuration changes
                action = message.get("action")
                source_config = message.get("source_config", {})
                try:
                    if action == "add":
                        source_name = source_config.get('name')
                        if source_name:
                            print(f"🔄 Reconciler Agent: Adding data source: {source_name}")
                            # Create and register new data source
                            # DataSource is already defined in this file
                            new_source = DataSource(
                                name=source_name,
                                connection_params=source_config.get('connection_params', {}),
                                priority=source_config.get('priority', 1),
                                key_fields=source_config.get('key_fields', ['id'])
                            )
                            self.register_data_source(new_source)
                    elif action == "remove":
                        source_name = message.get("source_name")
                        if source_name and source_name in self.data_sources:
                            print(f"🔄 Reconciler Agent: Removing data source: {source_name}")
                            del self.data_sources[source_name]
                            self.reconciliation_stats["sources_registered"] = len(self.data_sources)
                    elif action == "update":
                        source_name = source_config.get('name')
                        if source_name and source_name in self.data_sources:
                            print(f"🔄 Reconciler Agent: Updating data source: {source_name}")
                            # Update existing data source properties
                            existing_source = self.data_sources[source_name]
                            if 'priority' in source_config:
                                existing_source.priority = source_config['priority']
                            if 'key_fields' in source_config:
                                existing_source.key_fields = source_config['key_fields']
                except Exception as ds_error:
                    print(f"🔄 Reconciler Agent: Data source operation failed: {ds_error}")
                        
        except Exception as e:
            print(f"🔄 Reconciler Agent: Error handling control message from {topic}: {e}")
    
    async def _apply_config_updates(self, updates: dict):
        """Apply dynamic configuration updates."""
        try:
            # Update tolerance settings
            if "float_tolerance" in updates:
                self.config.float_tolerance = updates["float_tolerance"]
                print(f"🔄 Reconciler Agent: Updated float_tolerance to {updates['float_tolerance']}")
                
            if "timestamp_tolerance_seconds" in updates:
                self.config.timestamp_tolerance_seconds = updates["timestamp_tolerance_seconds"]
                print(f"🔄 Reconciler Agent: Updated timestamp_tolerance_seconds to {updates['timestamp_tolerance_seconds']}")
                
            # Update batch processing
            if "batch_size" in updates:
                self.config.batch_size = updates["batch_size"]
                print(f"🔄 Reconciler Agent: Updated batch_size to {updates['batch_size']}")
                
            if "max_concurrent_sources" in updates:
                self.config.max_concurrent_sources = updates["max_concurrent_sources"]
                # Update the semaphore
                self.fetch_semaphore = asyncio.Semaphore(self.config.max_concurrent_sources)
                print(f"🔄 Reconciler Agent: Updated max_concurrent_sources to {updates['max_concurrent_sources']}")
                
        except Exception as e:
            print(f"🔄 Reconciler Agent: Error applying config updates: {e}")
    
    async def stop(self):
        """Stop the reconciler agent and clean up pending tasks."""
        print("🔄 Stopping Reconciler Agent...")
        
        # Signal shutdown
        self._shutdown_event.set()
        
        # Stop health monitoring
        if self._monitoring_task and not self._monitoring_task.done():
            print("🔄 Reconciler Agent: Stopping health monitoring...")
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print(f"🔄 Reconciler Agent: Error stopping health monitoring: {e}")
        
        # Cancel all pending control message handler tasks
        if hasattr(self, '_pending_tasks'):
            print(f"🔄 Reconciler Agent: Cancelling {len(self._pending_tasks)} pending tasks")
            
            # Cancel all pending tasks
            for task in list(self._pending_tasks):
                if not task.done():
                    task.cancel()
            
            # Wait for cancelled tasks to complete
            if self._pending_tasks:
                try:
                    await asyncio.gather(*self._pending_tasks, return_exceptions=True)
                except Exception as e:
                    print(f"🔄 Reconciler Agent: Error during task cleanup: {e}")
            
            # Clear the task set
            self._pending_tasks.clear()
        
        # Publish final metrics before shutdown
        if hasattr(self.streaming_bus, 'publish'):
            try:
                final_metrics = {
                    "component_id": self.circuit_breaker_id,
                    "timestamp": int(time.time() * 1_000_000),
                    "event": "shutdown",
                    "final_stats": self.reconciliation_stats
                }
                
                await self.streaming_bus.publish(
                    topic="metrics.reconciler",
                    partition_key=self.circuit_breaker_id,
                    payload=final_metrics
                )
                print("🔄 Reconciler Agent: Published final metrics")
            except Exception as e:
                print(f"🔄 Reconciler Agent: Failed to publish final metrics: {e}")
        
        # Close streaming bus connections
        if hasattr(self, 'streaming_bus'):
            try:
                await self.streaming_bus.shutdown()
                print("🔄 Reconciler Agent: Streaming bus shutdown")
            except Exception as e:
                print(f"🔄 Reconciler Agent: Error shutting down streaming bus: {e}")
        
        print("🔄 Reconciler Agent stopped")
        print(f"🔄 Final Statistics: {self.reconciliation_stats}")


# Example usage and demo
if __name__ == "__main__":
    async def demo_reconciler():
        """Demonstrate the reconciler agent functionality."""
        print("=== Reconciler Agent Demo ===\n")
        
        # Configure reconciler
        config = ReconcilerConfig(
            float_tolerance=1e-6,
            timestamp_tolerance_seconds=2.0,
            batch_size=500,
            auto_approve_low_severity=False,
            approval_expiry_hours=12.0
        )
        
        reconciler = ReconcilerAgent(config)
        
        # Register data sources with enhanced configuration
        source_primary = DataSource(
            name="primary_db",
            connection_params={"host": "primary.db", "database": "trading"},
            priority=3,  # Highest priority (authoritative)
            key_fields=["trade_id"],
            timestamp_field="timestamp_utc_us",
            field_map={"ts": "timestamp_utc_us", "px": "price"},  # Field mapping
            field_normalizers={"symbol": lambda x: x.upper().strip()}  # Normalization
        )
        
        source_backup = DataSource(
            name="backup_db", 
            connection_params={"host": "backup.db", "database": "trading"},
            priority=2,
            key_fields=["trade_id"],
            timestamp_field="timestamp_utc_us"
        )
        
        source_archive = DataSource(
            name="archive_db",
            connection_params={"host": "archive.db", "database": "trading_archive"},
            priority=1,
            read_only=True,  # Archive is read-only
            key_fields=["trade_id"],
            timestamp_field="timestamp_utc_us"
        )
        
        reconciler.register_data_source(source_primary)
        reconciler.register_data_source(source_backup)
        reconciler.register_data_source(source_archive)
        
        print("1. Registered Data Sources:")
        for name, source in reconciler.data_sources.items():
            print(f"   - {name} (priority: {source.priority}, read_only: {source.read_only})")
        
        print("\n2. Demo Record Comparison:")
        # Simulate record comparison
        record_a = {
            "trade_id": "TXN_001",
            "symbol": "BTC/USD",
            "price": 45000.00,
            "quantity": 1.5,
            "timestamp_utc_us": 1635724800000000,
            "status": "completed"
        }
        
        record_b = {
            "trade_id": "TXN_001", 
            "symbol": "BTC/USD",
            "price": 45000.01,  # Slight price difference
            "quantity": 1.5,
            "timestamp_utc_us": 1635724801000000,  # 1 second timestamp drift
            "status": "completed"
        }
        
        discrepancies = reconciler.compare_records(record_a, record_b, source_primary, source_backup)
        print(f"   Found {len(discrepancies)} discrepancies:")
        for disc in discrepancies:
            print(f"   - {disc.discrepancy_type.value}: {disc.field_name} ({disc.severity.value})")
            if disc.field_name:
                print(f"     Primary: {disc.value_a}, Backup: {disc.value_b}")
        
        print("\n3. Approval Request Generation:")
        for discrepancy in discrepancies:
            approval_request = reconciler._propose_fix(discrepancy)
            if approval_request:
                print(f"   - Action: {approval_request.action.value}")
                print(f"     Target: {approval_request.target_source}")
                print(f"     Description: {approval_request.description}")
                print(f"     Confidence: {approval_request.confidence_score:.2f}")
        
        print("\n4. Reconciliation Statistics:")
        stats = reconciler.get_reconciliation_stats()
        for key, value in stats.items():
            print(f"   {key}: {value}")
        
        print("\n=== Demo Complete ===")
        print("\nReconciler Agent Features:")
        print("✓ Deterministic cross-venue data comparison")
        print("✓ Configurable tolerance levels and priority-based conflict resolution")
        print("✓ Approval workflow for all proposed changes (no direct writes)")
        print("✓ Comprehensive reporting with severity classification")
        print("✓ Performance monitoring and statistics tracking")
        print("✓ Read-only source protection and manual review escalation")
    
    # Run demo
    asyncio.run(demo_reconciler())
