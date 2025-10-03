"""
Schema Validator Agent

Mission: Enforce data contracts (types, nulls, ranges, referential integrity).
Outputs: incidents.SchemaViolation + clean pass/fail summary.
Do/Don't: Do coerce within explicit rules; don't silently reshape.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Union, Set, Callable, Tuple, Pattern
from decimal import Decimal, ROUND_HALF_EVEN
from enum import Enum
import re
from datetime import datetime, timezone
import difflib
import hashlib

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
                # Validate temporal ordering between timestamp fields
                if len(rule.fields) >= 2:
                    timestamps = []
                    for field_name in rule.fields:
                        if field_name in row and row[field_name] is not None:
                            timestamps.append((field_name, row[field_name]))
                    
                    # Check if timestamps are in order
                    for i in range(1, len(timestamps)):
                        if timestamps[i][1] < timestamps[i-1][1]:
                            violations.append(SchemaViolation(
                                table_name=schema.name,
                                field_name=f"{timestamps[i-1][0]},{timestamps[i][0]}",
                                violation_type="temporal_ordering_violation",
                                expected=f"{timestamps[i-1][0]} <= {timestamps[i][0]}",
                                actual=f"{timestamps[i-1][1]} > {timestamps[i][1]}",
                                row_identifier=row_id,
                                severity=rule.severity
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
        Validate a single row against its schema.
        
        Returns:
            (cleaned_row, violations, validation_flags)
        """
        flags = ValidationFlags()
        
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
        
        return cleaned_row, violations, flags
    
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
        
        # Timestamp validation with bounds
        if field_schema.field_type == FieldType.TIMESTAMP_US and isinstance(value, int):
            if value < self.min_timestamp_us or value > self.max_timestamp_us:
                violations.append(SchemaViolation(
                    table_name=table_name,
                    field_name=field_schema.name,
                    violation_type="timestamp_out_of_bounds",
                    expected=f"between {self.min_timestamp_us} and {self.max_timestamp_us}",
                    actual=str(value),
                    row_identifier=row_id
                ))
        
        
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
                            if values[i][0] < values[i-1][0]:
                                violations.append(SchemaViolation(
                                    table_name=schema.name,
                                    field_name=field_name,
                                    violation_type="batch_not_monotonic",
                                    expected="monotonically increasing values",
                                    actual=f"{values[i-1][0]} > {values[i][0]} at position {i}",
                                    row_identifier=values[i][1],
                                    severity="warning"
                                ))
        
        return violations
    
    async def start(self):
        """Start the schema validator agent."""
        self.running = True
        logger.info("Schema Validator Agent started")
    
    async def stop(self):
        """Stop the schema validator agent."""
        self.running = False
        logger.info("Schema Validator Agent stopped")


# Predefined coercion rules for common use cases
LOWERCASE_ADDRESS_RULE = CoercionRule(
    from_type=str,
    to_type=str,
    coercer=lambda x: x.lower() if isinstance(x, str) and x.startswith('0x') and len(x) == 42 else None,
    description="Normalize Ethereum addresses to lowercase"
)

LOWERCASE_HASH_RULE = CoercionRule(
    from_type=str,
    to_type=str,
    coercer=lambda x: x.lower() if isinstance(x, str) and x.startswith('0x') and len(x) == 66 else None,
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
