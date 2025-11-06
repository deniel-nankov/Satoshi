"""
┌──────────────────────────────────────────────────────────────────────────┐
│ OPTIONS CHAIN CURATOR (Gold Layer GD-4)                                 │
├──────────────────────────────────────────────────────────────────────────┤
│ Purpose: Transform clean options data into organized, curated chains    │
│          Group by strike/expiry/moneyness with days-to-expiry metadata  │
│                                                                          │
│ Data Flow:                                                               │
│   clean.market.options → curated.data.options_chain                     │
│                                                                          │
│ Transformations:                                                         │
│   [1] Group by strike/expiry/option_type                                │
│   [2] Classify moneyness (OTM/ATM/ITM) based on strike vs underlying    │
│   [3] Compute days-to-expiry and tenor buckets                          │
│   [4] Extract exchange-provided Greeks (no recalculation)               │
│   [5] Organize by symbol → expiry → strike ladder                       │
│                                                                          │
│ Boundaries:                                                              │
│   ✅ DO:   Group, classify, organize, enrich with metadata              │
│   ❌ DON'T: Calculate IV/skew (Feature Layer), recalculate Greeks        │
│                                                                          │
│ Consumer Group: options_curator                                          │
│ Publish Topic:  curated.data.options_chain                               │
│ Instance:       Independent Kafka consumer (no orchestrator)             │
└──────────────────────────────────────────────────────────────────────────┘
"""

import asyncio
import time
import logging
from dataclasses import dataclass, asdict
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Set, Any, Tuple, Protocol
from enum import Enum
from collections import defaultdict
import hashlib

from infra.bus.streaming_bus import StreamingBus

# Institutional Controls (optional import)
try:
    from engines.data.gold.gold_layer_institutional_controls import (
        InstitutionalControls,
        SLAMetric,
        DEFAULT_SLA_THRESHOLDS,
    )
    INSTITUTIONAL_CONTROLS_AVAILABLE = True
except ImportError:
    INSTITUTIONAL_CONTROLS_AVAILABLE = False
    InstitutionalControls = None  # type: ignore
    SLAMetric = None  # type: ignore

# =============================
# LOGGING CONFIGURATION
# =============================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================
# TYPE PROTOCOLS
# =============================

class MetricsCollector(Protocol):
    """
    Protocol defining the metrics collector interface.
    Enables proper type checking while maintaining loose coupling.
    """
    
    def increment_counter(self, name: str, value: float = 1, labels: Optional[Dict[str, str]] = None) -> None:
        """Increment a counter metric."""
        ...
    
    def observe_histogram(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Record a histogram observation."""
        ...
    
    def set_gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Set a gauge metric value."""
        ...

# =============================
# ENUMS & DATACLASSES
# =============================

class OptionType(Enum):
    """Option type: Call or Put."""
    CALL = "call"
    PUT = "put"

class Moneyness(Enum):
    """Moneyness classification relative to underlying price."""
    DEEP_OTM = "deep_otm"      # |strike - spot| > 15%
    OTM = "otm"                # 5% < |strike - spot| <= 15%
    ATM = "atm"                # |strike - spot| <= 5%
    ITM = "itm"                # 5% < |strike - spot| <= 15%
    DEEP_ITM = "deep_itm"      # |strike - spot| > 15%

class Tenor(Enum):
    """Time to expiry classification."""
    WEEKLY = "7d"       # <= 7 days
    MONTHLY = "30d"     # 7 < days <= 30
    QUARTERLY = "90d"   # 30 < days <= 90
    SEMI_ANNUAL = "180d"  # 90 < days <= 180
    ANNUAL = "365d"     # 180 < days <= 365
    LEAP = "leap"       # > 365 days

@dataclass
class OptionsGreeks:
    """Exchange-provided Greeks (no recalculation)."""
    delta: Optional[Decimal]
    gamma: Optional[Decimal]
    vega: Optional[Decimal]
    theta: Optional[Decimal]
    rho: Optional[Decimal]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "delta": float(self.delta) if self.delta is not None else None,
            "gamma": float(self.gamma) if self.gamma is not None else None,
            "vega": float(self.vega) if self.vega is not None else None,
            "theta": float(self.theta) if self.theta is not None else None,
            "rho": float(self.rho) if self.rho is not None else None
        }

@dataclass
class OptionContract:
    """Individual option contract with enriched metadata."""
    symbol: str
    venue: str
    strike: Decimal
    expiry_utc_us: int
    option_type: OptionType
    
    # Market data
    mark_price: Optional[Decimal]
    underlying_price: Optional[Decimal]
    iv: Optional[Decimal]
    volume: Optional[Decimal]
    open_interest: Optional[Decimal]
    
    # Greeks (exchange-provided)
    greeks: OptionsGreeks
    
    # Enriched metadata (Gold Layer additions)
    moneyness: Moneyness
    days_to_expiry: Decimal
    tenor: Tenor
    
    # Timestamps
    timestamp_utc_us: int  # Capture time
    venue_timestamp_utc_us: Optional[int]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "venue": self.venue,
            "strike": float(self.strike),
            "expiry_utc_us": self.expiry_utc_us,
            "option_type": self.option_type.value,
            "mark_price": float(self.mark_price) if self.mark_price is not None else None,
            "underlying_price": float(self.underlying_price) if self.underlying_price is not None else None,
            "iv": float(self.iv) if self.iv is not None else None,
            "volume": float(self.volume) if self.volume is not None else None,
            "open_interest": float(self.open_interest) if self.open_interest is not None else None,
            "greeks": self.greeks.to_dict(),
            "moneyness": self.moneyness.value,
            "days_to_expiry": float(self.days_to_expiry),
            "tenor": self.tenor.value,
            "timestamp_utc_us": self.timestamp_utc_us,
            "venue_timestamp_utc_us": self.venue_timestamp_utc_us
        }

@dataclass
class StrikeLadder:
    """Organized strike ladder for a specific expiry."""
    expiry_utc_us: int
    days_to_expiry: Decimal
    tenor: Tenor
    underlying_price: Optional[Decimal]
    
    # Organized by strike → option_type → contract
    strikes: Dict[Decimal, Dict[str, OptionContract]]  # strike → {"call": contract, "put": contract}
    
    # Summary statistics
    num_strikes: int
    strike_min: Optional[Decimal]
    strike_max: Optional[Decimal]
    atm_strike: Optional[Decimal]  # Closest to underlying
    
    timestamp_utc_us: int
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "expiry_utc_us": self.expiry_utc_us,
            "days_to_expiry": float(self.days_to_expiry),
            "tenor": self.tenor.value,
            "underlying_price": float(self.underlying_price) if self.underlying_price is not None else None,
            "strikes": {
                float(strike): {
                    opt_type: contract.to_dict() 
                    for opt_type, contract in contracts.items()
                }
                for strike, contracts in self.strikes.items()
            },
            "num_strikes": self.num_strikes,
            "strike_min": float(self.strike_min) if self.strike_min is not None else None,
            "strike_max": float(self.strike_max) if self.strike_max is not None else None,
            "atm_strike": float(self.atm_strike) if self.atm_strike is not None else None,
            "timestamp_utc_us": self.timestamp_utc_us
        }

@dataclass
class SurfaceQualityMetrics:
    """
    Task #5: Options surface quality checks (Gold Layer data quality).
    
    IMPORTANT: This validates CURATED STRUCTURE, not raw record integrity.
    
    Silver Layer already validated individual option contracts (schema, bounds, freshness).
    Gold Layer validates the ORGANIZED CHAIN STRUCTURE after curation:
      - Strike coverage (detect missing strikes in the ladder)
      - Expiry gaps (detect missing expiration dates)
      - Greeks sanity (validate delta bounds, gamma positivity)
      - Staleness (flag quotes older than 30 seconds)
      - Surface anomalies (discontinuities in the chain)
    
    Example distinction:
      - Silver: "This option has delta=1.5" → REJECT (invalid record)
      - Gold: "This chain is missing 20 strikes" → FLAG (incomplete structure)
    
    This is data quality validation, NOT alpha generation.
    Output includes quality score so downstream consumers can decide if data is usable.
    """
    # Strike coverage
    has_complete_strike_coverage: bool  # True if no significant gaps in strike ladder
    missing_strike_ranges: List[Tuple[Decimal, Decimal]]  # Gaps in strike coverage
    strike_spacing_uniform: bool  # True if strike spacing is regular
    
    # Expiry coverage
    has_expiry_gaps: bool  # True if missing expected expiries (e.g., weekly chain incomplete)
    missing_expiry_tenors: List[str]  # List of missing tenor buckets
    
    # Greeks sanity checks
    delta_violations_count: int  # Count of deltas outside [-1, 1] for puts or [0, 1] for calls
    gamma_negative_count: int  # Count of negative gamma (invalid for long positions)
    greeks_stale_count: int  # Count of contracts with no Greeks data
    
    # Staleness (>30s old quotes are flagged)
    stale_contracts_count: int
    stale_contracts_pct: Decimal
    max_staleness_sec: Decimal  # Maximum age of any contract in seconds
    
    # Surface continuity
    iv_discontinuities_count: int  # Large jumps in IV across strikes (>20% relative change)
    surface_quality_score: Decimal  # Overall quality: 1.0 = perfect, 0.0 = unusable
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "has_complete_strike_coverage": self.has_complete_strike_coverage,
            "missing_strike_ranges": [[float(start), float(end)] for start, end in self.missing_strike_ranges],
            "strike_spacing_uniform": self.strike_spacing_uniform,
            "has_expiry_gaps": self.has_expiry_gaps,
            "missing_expiry_tenors": self.missing_expiry_tenors,
            "delta_violations_count": self.delta_violations_count,
            "gamma_negative_count": self.gamma_negative_count,
            "greeks_stale_count": self.greeks_stale_count,
            "stale_contracts_count": self.stale_contracts_count,
            "stale_contracts_pct": float(self.stale_contracts_pct),
            "max_staleness_sec": float(self.max_staleness_sec),
            "iv_discontinuities_count": self.iv_discontinuities_count,
            "surface_quality_score": float(self.surface_quality_score)
        }

@dataclass
class CuratedOptionsChain:
    """Complete curated options chain for a symbol."""
    symbol: str
    venue: str
    underlying_price: Optional[Decimal]
    
    # Organized by expiry → strike ladder
    expiries: Dict[int, StrikeLadder]  # expiry_utc_us → StrikeLadder
    
    # Summary statistics
    num_expiries: int
    num_contracts: int
    min_expiry_utc_us: Optional[int]
    max_expiry_utc_us: Optional[int]
    tenor_distribution: Dict[str, int]  # tenor → count
    
    # Task #5: Surface quality metrics (data quality validation)
    surface_quality: SurfaceQualityMetrics
    
    timestamp_utc_us: int
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "venue": self.venue,
            "underlying_price": float(self.underlying_price) if self.underlying_price is not None else None,
            "expiries": {
                expiry: ladder.to_dict() 
                for expiry, ladder in self.expiries.items()
            },
            "num_expiries": self.num_expiries,
            "num_contracts": self.num_contracts,
            "min_expiry_utc_us": self.min_expiry_utc_us,
            "max_expiry_utc_us": self.max_expiry_utc_us,
            "tenor_distribution": self.tenor_distribution,
            "surface_quality": self.surface_quality.to_dict(),
            "timestamp_utc_us": self.timestamp_utc_us
        }

# =============================
# OPTIONS CHAIN ORGANIZER
# =============================

class OptionsChainOrganizer:
    """
    Core logic for organizing options into curated chains.
    Groups by symbol → expiry → strike with moneyness/tenor classification.
    """
    
    MICROSECONDS_PER_DAY = Decimal(86_400_000_000)
    
    def __init__(self):
        # Temporary storage for incoming options (symbol:venue → contracts)
        self.pending_contracts: Dict[str, List[OptionContract]] = defaultdict(list)
        
        # Deduplication cache (hash → timestamp)
        self.seen_contracts: Dict[str, int] = {}
        self.cache_max_size = 50000
        
        logger.info("OptionsChainOrganizer initialized")
    
    def add_option(self, raw_option: Dict[str, Any], current_time_us: int) -> Optional[OptionContract]:
        """
        Parse raw option from clean.market.options and enrich with metadata.
        Returns enriched OptionContract or None if duplicate/invalid.
        """
        try:
            # Extract fields
            symbol = raw_option.get("symbol", "")
            venue = raw_option.get("venue", "")
            strike = self._parse_decimal(raw_option.get("strike"))
            expiry_utc_us = raw_option.get("expiry", 0)
            option_type_str = raw_option.get("option_type", "")
            
            # Validation
            if not symbol or not venue or strike is None or expiry_utc_us <= 0:
                logger.warning(f"Invalid option: missing required fields (symbol={symbol}, venue={venue}, strike={strike}, expiry={expiry_utc_us})")
                return None
            
            # Parse option type
            try:
                option_type = OptionType(option_type_str.lower())
            except (ValueError, AttributeError):
                logger.warning(f"Invalid option_type: {option_type_str}")
                return None
            
            # Deduplication check
            contract_hash = self._get_contract_hash(symbol, venue, strike, expiry_utc_us, option_type)
            if contract_hash in self.seen_contracts:
                return None  # Duplicate
            
            # Market data
            mark_price = self._parse_decimal(raw_option.get("mark_price"))
            underlying_price = self._parse_decimal(raw_option.get("underlying_price"))
            iv = self._parse_decimal(raw_option.get("iv"))
            volume = self._parse_decimal(raw_option.get("volume"))
            open_interest = self._parse_decimal(raw_option.get("open_interest"))
            
            # Greeks (exchange-provided, no recalculation)
            greeks = OptionsGreeks(
                delta=self._parse_decimal(raw_option.get("delta")),
                gamma=self._parse_decimal(raw_option.get("gamma")),
                vega=self._parse_decimal(raw_option.get("vega")),
                theta=self._parse_decimal(raw_option.get("theta")),
                rho=self._parse_decimal(raw_option.get("rho"))
            )
            
            # Compute enriched metadata
            days_to_expiry = self._compute_days_to_expiry(current_time_us, expiry_utc_us)
            tenor = self._classify_tenor(days_to_expiry)
            moneyness = self._classify_moneyness(strike, underlying_price, option_type)
            
            # Timestamps
            timestamp_utc_us = raw_option.get("timestamp_utc_us", current_time_us)
            venue_timestamp_utc_us = raw_option.get("venue_timestamp_utc_us")
            
            # Create enriched contract
            contract = OptionContract(
                symbol=symbol,
                venue=venue,
                strike=strike,
                expiry_utc_us=expiry_utc_us,
                option_type=option_type,
                mark_price=mark_price,
                underlying_price=underlying_price,
                iv=iv,
                volume=volume,
                open_interest=open_interest,
                greeks=greeks,
                moneyness=moneyness,
                days_to_expiry=days_to_expiry,
                tenor=tenor,
                timestamp_utc_us=timestamp_utc_us,
                venue_timestamp_utc_us=venue_timestamp_utc_us
            )
            
            # Cache deduplication
            self.seen_contracts[contract_hash] = current_time_us
            self._cleanup_cache_if_needed()
            
            # Add to pending contracts
            chain_key = f"{symbol}:{venue}"
            self.pending_contracts[chain_key].append(contract)
            
            return contract
            
        except Exception as e:
            logger.error(f"Error parsing option: {e}")
            return None
    
    def build_curated_chain(self, symbol: str, venue: str, current_time_us: int) -> Optional[CuratedOptionsChain]:
        """
        Build curated chain from pending contracts for a specific symbol/venue.
        Returns CuratedOptionsChain or None if no contracts available.
        """
        chain_key = f"{symbol}:{venue}"
        contracts = self.pending_contracts.get(chain_key, [])
        
        if not contracts:
            return None
        
        # Group by expiry → strike → option_type
        expiry_groups: Dict[int, Dict[Decimal, Dict[str, OptionContract]]] = defaultdict(lambda: defaultdict(dict))
        
        underlying_price = None
        for contract in contracts:
            # Use most recent underlying price
            if contract.underlying_price is not None:
                underlying_price = contract.underlying_price
            
            expiry = contract.expiry_utc_us
            strike = contract.strike
            opt_type = contract.option_type.value
            
            expiry_groups[expiry][strike][opt_type] = contract
        
        # Build strike ladders for each expiry
        expiries: Dict[int, StrikeLadder] = {}
        for expiry_utc_us, strikes_dict in expiry_groups.items():
            # Compute expiry metadata
            days_to_expiry = self._compute_days_to_expiry(current_time_us, expiry_utc_us)
            tenor = self._classify_tenor(days_to_expiry)
            
            # Strike statistics
            strikes_list = list(strikes_dict.keys())
            strike_min = min(strikes_list) if strikes_list else None
            strike_max = max(strikes_list) if strikes_list else None
            
            # Find ATM strike (closest to underlying)
            atm_strike = None
            if underlying_price is not None and strikes_list:
                atm_strike = min(strikes_list, key=lambda s: abs(s - underlying_price))
            
            ladder = StrikeLadder(
                expiry_utc_us=expiry_utc_us,
                days_to_expiry=days_to_expiry,
                tenor=tenor,
                underlying_price=underlying_price,
                strikes=strikes_dict,
                num_strikes=len(strikes_dict),
                strike_min=strike_min,
                strike_max=strike_max,
                atm_strike=atm_strike,
                timestamp_utc_us=current_time_us
            )
            
            expiries[expiry_utc_us] = ladder
        
        # Chain-level statistics
        num_contracts = sum(len(ladder.strikes) * len(ladder.strikes[list(ladder.strikes.keys())[0]]) 
                           for ladder in expiries.values() if ladder.strikes)
        
        expiry_times = list(expiries.keys())
        min_expiry = min(expiry_times) if expiry_times else None
        max_expiry = max(expiry_times) if expiry_times else None
        
        # Tenor distribution
        tenor_dist: Dict[str, int] = defaultdict(int)
        for ladder in expiries.values():
            tenor_dist[ladder.tenor.value] += 1
        
        # Task #5: Compute surface quality metrics (data quality validation)
        surface_quality = self._compute_surface_quality(
            expiries=expiries,
            contracts=contracts,
            current_time_us=current_time_us,
            underlying_price=underlying_price
        )
        
        chain = CuratedOptionsChain(
            symbol=symbol,
            venue=venue,
            underlying_price=underlying_price,
            expiries=expiries,
            num_expiries=len(expiries),
            num_contracts=num_contracts,
            min_expiry_utc_us=min_expiry,
            max_expiry_utc_us=max_expiry,
            tenor_distribution=dict(tenor_dist),
            surface_quality=surface_quality,
            timestamp_utc_us=current_time_us
        )
        
        # Clear pending contracts for this chain
        self.pending_contracts[chain_key] = []
        
        return chain
    
    def _compute_days_to_expiry(self, current_time_us: int, expiry_utc_us: int) -> Decimal:
        """Compute days to expiry from current time."""
        time_diff_us = expiry_utc_us - current_time_us
        return Decimal(time_diff_us) / self.MICROSECONDS_PER_DAY
    
    def _classify_tenor(self, days_to_expiry: Decimal) -> Tenor:
        """Classify time to expiry into tenor bucket."""
        if days_to_expiry <= 7:
            return Tenor.WEEKLY
        elif days_to_expiry <= 30:
            return Tenor.MONTHLY
        elif days_to_expiry <= 90:
            return Tenor.QUARTERLY
        elif days_to_expiry <= 180:
            return Tenor.SEMI_ANNUAL
        elif days_to_expiry <= 365:
            return Tenor.ANNUAL
        else:
            return Tenor.LEAP
    
    def _classify_moneyness(self, strike: Decimal, underlying_price: Optional[Decimal], option_type: OptionType) -> Moneyness:
        """
        Classify moneyness based on strike vs underlying price.
        OTM/ITM definition depends on option type (call vs put).
        """
        if underlying_price is None or underlying_price == 0:
            return Moneyness.ATM  # Default when no underlying price
        
        # Compute percentage difference
        pct_diff = abs((strike - underlying_price) / underlying_price) * 100
        
        # Determine if in/out of money based on option type
        if option_type == OptionType.CALL:
            is_itm = strike < underlying_price
        else:  # PUT
            is_itm = strike > underlying_price
        
        # Classify based on percentage difference and ITM status
        if pct_diff <= 5:
            return Moneyness.ATM
        elif pct_diff <= 15:
            return Moneyness.ITM if is_itm else Moneyness.OTM
        else:  # > 15%
            return Moneyness.DEEP_ITM if is_itm else Moneyness.DEEP_OTM
    
    def _parse_decimal(self, value: Any) -> Optional[Decimal]:
        """Safely parse Decimal from various types."""
        if value is None:
            return None
        try:
            if isinstance(value, Decimal):
                return value
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return None
    
    def _get_contract_hash(self, symbol: str, venue: str, strike: Decimal, expiry: int, option_type: OptionType) -> str:
        """Generate stable hash for contract identity (deduplication)."""
        hash_input = f"{venue}:{symbol}:{expiry}:{strike}:{option_type.value}"
        return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()
    
    def _compute_surface_quality(
        self,
        expiries: Dict[int, StrikeLadder],
        contracts: List[OptionContract],
        current_time_us: int,
        underlying_price: Optional[Decimal]
    ) -> SurfaceQualityMetrics:
        """
        Task #5: Compute options surface quality metrics.
        
        LAYER DISTINCTION:
          - Silver Layer: Already validated individual contracts (schema, bounds, anomalies)
          - Gold Layer: Now validates the ORGANIZED STRUCTURE after curation
        
        Validates CURATED data completeness, continuity, and structural integrity:
          1. Strike coverage (detect gaps in strike ladder) - STRUCTURAL
          2. Expiry coverage (detect missing tenors) - STRUCTURAL
          3. Greeks sanity (validate delta bounds, gamma positivity) - AGGREGATE CHECK
          4. Staleness (flag quotes >30s old) - AGGREGATE CHECK
          5. IV discontinuities (detect jumps in implied volatility surface) - CONTINUITY
        
        Why this is NOT redundant:
          - Silver validates: "Is each option valid?" (record-level)
          - Gold validates: "Is the chain complete and continuous?" (structure-level)
        
        Example:
          - Silver: ✅ All 50 options pass validation
          - Gold: ⚠️ But the chain is missing strikes 47500-49500 (incomplete ladder)
        
        This is pure data quality validation (Gold Layer compliant), NOT alpha generation.
        Output quality score lets downstream consumers decide if chain is usable.
        """
        # Staleness check (threshold: 30 seconds)
        staleness_threshold_us = 30_000_000  # 30 seconds
        stale_count = 0
        max_staleness_us = Decimal("0")
        
        for contract in contracts:
            age_us = current_time_us - contract.timestamp_utc_us
            if age_us > staleness_threshold_us:
                stale_count += 1
            if age_us > max_staleness_us:
                max_staleness_us = Decimal(age_us)
        
        stale_pct = (Decimal(stale_count) / Decimal(len(contracts)) * Decimal("100")) if contracts else Decimal("0")
        max_staleness_sec = max_staleness_us / Decimal("1_000_000")
        
        # Greeks sanity checks
        delta_violations = 0
        gamma_negative = 0
        greeks_stale = 0
        
        for contract in contracts:
            # Check if Greeks are present
            if contract.greeks.delta is None or contract.greeks.gamma is None:
                greeks_stale += 1
                continue
            
            # Delta bounds: Calls [0, 1], Puts [-1, 0]
            if contract.option_type == OptionType.CALL:
                if contract.greeks.delta < 0 or contract.greeks.delta > 1:
                    delta_violations += 1
            else:  # PUT
                if contract.greeks.delta < -1 or contract.greeks.delta > 0:
                    delta_violations += 1
            
            # Gamma should always be non-negative (for long positions)
            if contract.greeks.gamma < 0:
                gamma_negative += 1
        
        # Strike coverage analysis (per expiry)
        missing_strike_ranges: List[Tuple[Decimal, Decimal]] = []
        strike_spacing_uniform = True
        
        for ladder in expiries.values():
            strikes = sorted(ladder.strikes.keys())
            if len(strikes) >= 2:
                # Check for uniform spacing
                spacings = [strikes[i+1] - strikes[i] for i in range(len(strikes) - 1)]
                avg_spacing = sum(spacings, start=Decimal("0")) / Decimal(len(spacings))
                
                # Allow 50% deviation from average spacing
                for i, spacing in enumerate(spacings):
                    if abs(spacing - avg_spacing) > avg_spacing * Decimal("0.5"):
                        strike_spacing_uniform = False
                        # Gap detected: record range
                        missing_strike_ranges.append((strikes[i], strikes[i+1]))
        
        has_complete_strike_coverage = len(missing_strike_ranges) == 0
        
        # Expiry coverage (check if we have all major tenors)
        expected_tenors = {Tenor.WEEKLY.value, Tenor.MONTHLY.value, Tenor.QUARTERLY.value}
        observed_tenors = {ladder.tenor.value for ladder in expiries.values()}
        missing_expiry_tenors = list(expected_tenors - observed_tenors)
        has_expiry_gaps = len(missing_expiry_tenors) > 0
        
        # IV discontinuity detection (check for large jumps in IV across strikes)
        iv_discontinuities = 0
        iv_jump_threshold = Decimal("0.20")  # 20% relative change is considered a discontinuity
        
        for ladder in expiries.values():
            strikes = sorted(ladder.strikes.keys())
            for i in range(len(strikes) - 1):
                strike1 = strikes[i]
                strike2 = strikes[i + 1]
                
                # Check calls
                if "call" in ladder.strikes[strike1] and "call" in ladder.strikes[strike2]:
                    contract1 = ladder.strikes[strike1]["call"]
                    contract2 = ladder.strikes[strike2]["call"]
                    
                    if contract1.iv is not None and contract2.iv is not None and contract1.iv > 0:
                        iv_change_pct = abs(contract2.iv - contract1.iv) / contract1.iv
                        if iv_change_pct > iv_jump_threshold:
                            iv_discontinuities += 1
        
        # Compute overall surface quality score (1.0 = perfect, 0.0 = unusable)
        # Deduct points for each quality issue
        quality_score = Decimal("1.0")
        
        # Staleness penalty (up to -0.3)
        if stale_pct > 50:
            quality_score -= Decimal("0.3")
        elif stale_pct > 20:
            quality_score -= Decimal("0.15")
        elif stale_pct > 5:
            quality_score -= Decimal("0.05")
        
        # Greeks violations penalty (up to -0.3)
        total_contracts = len(contracts) if contracts else 1
        greeks_violation_pct = (delta_violations + gamma_negative + greeks_stale) / total_contracts
        if greeks_violation_pct > 0.3:
            quality_score -= Decimal("0.3")
        elif greeks_violation_pct > 0.1:
            quality_score -= Decimal("0.15")
        elif greeks_violation_pct > 0.05:
            quality_score -= Decimal("0.05")
        
        # Strike coverage penalty (up to -0.2)
        if not has_complete_strike_coverage:
            quality_score -= Decimal("0.2")
        elif not strike_spacing_uniform:
            quality_score -= Decimal("0.1")
        
        # Expiry coverage penalty (up to -0.1)
        if has_expiry_gaps:
            quality_score -= Decimal("0.1")
        
        # IV discontinuities penalty (up to -0.1)
        if iv_discontinuities > 10:
            quality_score -= Decimal("0.1")
        elif iv_discontinuities > 5:
            quality_score -= Decimal("0.05")
        
        # Clamp to [0, 1]
        quality_score = max(Decimal("0"), min(Decimal("1"), quality_score))
        
        return SurfaceQualityMetrics(
            has_complete_strike_coverage=has_complete_strike_coverage,
            missing_strike_ranges=missing_strike_ranges,
            strike_spacing_uniform=strike_spacing_uniform,
            has_expiry_gaps=has_expiry_gaps,
            missing_expiry_tenors=missing_expiry_tenors,
            delta_violations_count=delta_violations,
            gamma_negative_count=gamma_negative,
            greeks_stale_count=greeks_stale,
            stale_contracts_count=stale_count,
            stale_contracts_pct=stale_pct,
            max_staleness_sec=max_staleness_sec,
            iv_discontinuities_count=iv_discontinuities,
            surface_quality_score=quality_score
        )
    
    def _cleanup_cache_if_needed(self):
        """Cleanup deduplication cache if it exceeds max size."""
        if len(self.seen_contracts) > self.cache_max_size:
            # Remove oldest 20% of entries
            sorted_items = sorted(self.seen_contracts.items(), key=lambda x: x[1])
            cutoff_index = len(sorted_items) // 5
            for contract_hash, _ in sorted_items[:cutoff_index]:
                del self.seen_contracts[contract_hash]
            logger.info(f"Cleaned deduplication cache: {len(sorted_items[:cutoff_index])} entries removed")

# =============================
# OPTIONS CHAIN CURATOR AGENT
# =============================

class OptionsChainCurator:
    """
    Gold Layer Options Chain Curator.
    
    Subscribes:  clean.market.options
    Publishes:   curated.data.options_chain
    Consumer Group: options_curator
    
    Transformations:
      1. Group by symbol → expiry → strike
      2. Classify moneyness (OTM/ATM/ITM) based on strike vs underlying
      3. Compute days-to-expiry and tenor buckets
      4. Extract exchange-provided Greeks (no recalculation)
      5. Organize strike ladders with summary statistics
    
    Enterprise Features:
      - Circuit breaker (auto-recovery after failures)
      - Health monitoring (periodic status checks)
      - Metrics tracking (chains curated, options processed)
      - Incident reporting (critical failures)
      - Graceful shutdown (cleanup on exit)
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.running = False
        
        # Core components
        self.streaming_bus: Optional[StreamingBus] = None
        self.organizer = OptionsChainOrganizer()
        
        # Circuit breaker
        self.consecutive_failures = 0
        self.max_consecutive_failures = 5
        self.circuit_open = False
        self.circuit_recovery_time = 0
        self.circuit_recovery_delay_sec = 60
        
        # Metrics
        self.metrics_collector: Optional[MetricsCollector] = self._get_metrics_collector()
        self.chains_curated = 0
        self.options_processed = 0
        self.errors_count = 0
        self.start_time = time.time()
        
        # Health monitoring
        self.health_check_interval_sec = 60
        self.last_health_check = 0
        
        # Batching (build chains periodically)
        self.batch_interval_sec = 10  # Build chains every 10 seconds
        self.last_batch_time = 0
        
        # Institutional Controls (optional)
        self.institutional_controls: Optional[InstitutionalControls] = None  # type: ignore
        enable_controls = self.config.get("enable_institutional_controls", True)
        clickhouse_client = self.config.get("clickhouse_client")
        
        if enable_controls and INSTITUTIONAL_CONTROLS_AVAILABLE and InstitutionalControls is not None:
            try:
                # Custom SLA thresholds for options chain curation
                sla_thresholds = {
                    SLAMetric.LATENCY: 500.0,  # type: ignore  # 500ms P95 latency (complex processing)
                    SLAMetric.COMPLETENESS: 0.95,  # type: ignore  # 95% completeness
                    SLAMetric.FRESHNESS: 5.0,  # type: ignore  # 5 second max lag (less critical)
                    SLAMetric.QUALITY_SCORE: 0.90,  # type: ignore  # 90% quality score
                }
                
                self.institutional_controls = InstitutionalControls(
                    component_name="options_chain_curator",
                    component_version="1.0.0",
                    clickhouse_client=clickhouse_client,
                    sla_thresholds=sla_thresholds
                )
                logger.info("✅ Institutional controls enabled for Options Chain Curator")
            except Exception as e:
                logger.warning(f"⚠️ Failed to initialize institutional controls: {e}")
                self.institutional_controls = None
        
        logger.info(f"OptionsChainCurator initialized (batch_interval={self.batch_interval_sec}s)")
    
    def _get_metrics_collector(self) -> Optional[MetricsCollector]:
        """Get metrics collector from monitoring infrastructure."""
        try:
            from infra.monitoring.prometheus_metrics import get_metrics_collector
            return get_metrics_collector()
        except ImportError:
            logger.warning("Metrics collector not available")
            return None
    
    async def start(self):
        """Start the Options Chain Curator agent (non-blocking)."""
        self.running = True
        logger.info("Starting Options Chain Curator...")
        
        try:
            # Initialize streaming bus with proper config
            bus_config = self.config.get("streaming_bus", {
                "bootstrap_servers": ["localhost:9092"],
                "client_id": "options-chain-curator"
            })
            self.streaming_bus = StreamingBus(config=bus_config)
            
            # Start health monitoring (background task)
            asyncio.create_task(self._health_monitor())
            
            # Start batch chain building (background task)
            asyncio.create_task(self._batch_chain_builder())
            
            # Subscribe to clean.market.options (background task - non-blocking)
            input_topics = ["clean.market.options"]
            pool_size = self.config.get("pool_size", 4)
            
            logger.info(f"Subscribing to {input_topics} with pool_size={pool_size}...")
            
            # Launch subscription in background task instead of awaiting
            subscription_task = asyncio.create_task(
                self.streaming_bus.subscribe_with_worker_pool(
                    topics=input_topics,
                    consumer_group="options_curator",
                    handler=self._process_option,
                    pool_size=pool_size
                )
            )
            
            # Store task for cleanup
            self._subscription_task = subscription_task
            
            logger.info("✅ Options Chain Curator background tasks started (subscription, health monitor, batch builder)")
            
        except Exception as e:
            self.running = False
            logger.error(f"Failed to start Options Chain Curator: {e}")
            await self._publish_incident("startup_failure", str(e))
            raise
    
    async def _process_option(self, topic: str, partition_key: str, payload: Dict[str, Any], headers: Dict[str, str]):
        """
        Process individual option from clean.market.options.
        Adds to organizer for batch chain building.
        """
        try:
            # Circuit breaker check
            if self.circuit_open:
                current_time = time.time()
                if current_time < self.circuit_recovery_time:
                    return  # Circuit still open
                else:
                    # Attempt recovery
                    logger.info("Circuit breaker recovery attempt")
                    self.circuit_open = False
                    self.consecutive_failures = 0
            
            current_time_us = int(time.time() * 1_000_000)
            
            # Add option to organizer
            contract = self.organizer.add_option(payload, current_time_us)
            
            if contract:
                self.options_processed += 1
                
                # Metrics (defensive: check for None and method existence)
                if self.metrics_collector is not None:
                    try:
                        self.metrics_collector.increment_counter("options_chain_curator.options_processed")
                    except (AttributeError, TypeError) as e:
                        logger.debug(f"Metrics collector method not available: {e}")
                
                # Reset failure counter on success
                if self.consecutive_failures > 0:
                    self.consecutive_failures = 0
            
        except Exception as e:
            self.errors_count += 1
            self.consecutive_failures += 1
            logger.error(f"Error processing option: {e}")
            
            # Circuit breaker trigger
            if self.consecutive_failures >= self.max_consecutive_failures:
                self.circuit_open = True
                self.circuit_recovery_time = time.time() + self.circuit_recovery_delay_sec
                logger.error(f"Circuit breaker OPEN after {self.consecutive_failures} failures (recovery in {self.circuit_recovery_delay_sec}s)")
                await self._publish_incident("circuit_breaker_open", f"Failures: {self.consecutive_failures}")
            
            # Metrics (defensive: check for None and method existence)
            if self.metrics_collector is not None:
                try:
                    self.metrics_collector.increment_counter("options_chain_curator.errors")
                except (AttributeError, TypeError) as e:
                    logger.debug(f"Metrics collector method not available: {e}")
    
    async def _batch_chain_builder(self):
        """
        Periodically build curated chains from accumulated options.
        Runs every batch_interval_sec.
        """
        logger.info(f"Batch chain builder started (interval={self.batch_interval_sec}s)")
        
        while self.running:
            try:
                await asyncio.sleep(self.batch_interval_sec)
                
                current_time = time.time()
                current_time_us = int(current_time * 1_000_000)
                
                # Build chains for all pending symbols/venues
                chain_keys = list(self.organizer.pending_contracts.keys())
                
                for chain_key in chain_keys:
                    try:
                        symbol, venue = chain_key.split(":", 1)
                        chain = self.organizer.build_curated_chain(symbol, venue, current_time_us)
                        
                        if chain:
                            await self._publish_curated_chain(chain)
                            self.chains_curated += 1
                            
                            # Metrics (defensive: check for None and method existence)
                            if self.metrics_collector is not None:
                                try:
                                    self.metrics_collector.increment_counter("options_chain_curator.chains_curated")
                                except (AttributeError, TypeError) as e:
                                    logger.debug(f"Metrics collector method not available: {e}")
                            
                            logger.info(f"Curated chain published: {symbol}@{venue} ({chain.num_expiries} expiries, {chain.num_contracts} contracts)")
                    
                    except Exception as e:
                        logger.error(f"Error building chain for {chain_key}: {e}")
                
                self.last_batch_time = current_time
                
            except asyncio.CancelledError:
                logger.info("Batch chain builder cancelled")
                break
            except Exception as e:
                logger.error(f"Error in batch chain builder: {e}")
    
    async def _publish_curated_chain(self, chain: CuratedOptionsChain):
        """Publish curated options chain to curated.data.options_chain."""
        try:
            if not self.streaming_bus:
                logger.error("Streaming bus not available")
                return
            
            partition_key = f"{chain.symbol}:{chain.venue}"
            
            # Use canonical headers for data lineage and audit compliance
            if not hasattr(self, '_sequence_number'):
                self._sequence_number = 0
            
            await self.streaming_bus.publish_with_canonical_headers(
                topic="curated.data.options_chain",
                partition_key=partition_key,
                payload=chain.to_dict(),
                source_id="options_chain_curator.001",
                sequence_number=self._sequence_number,
                producer_version="1.0.0"
            )
            
            self._sequence_number += 1
            
        except Exception as e:
            logger.error(f"Error publishing curated chain: {e}")
            raise
    
    async def _health_monitor(self):
        """Periodic health monitoring and metrics reporting."""
        logger.info(f"Health monitor started (interval={self.health_check_interval_sec}s)")
        
        while self.running:
            try:
                await asyncio.sleep(self.health_check_interval_sec)
                
                current_time = time.time()
                uptime_sec = current_time - self.start_time
                uptime_min = uptime_sec / 60
                
                # Compute rates
                chains_per_min = self.chains_curated / uptime_min if uptime_min > 0 else 0
                options_per_min = self.options_processed / uptime_min if uptime_min > 0 else 0
                
                # Log health status
                logger.info(
                    f"Health Check - "
                    f"Uptime: {uptime_min:.1f}m | "
                    f"Chains: {self.chains_curated} ({chains_per_min:.1f}/min) | "
                    f"Options: {self.options_processed} ({options_per_min:.1f}/min) | "
                    f"Errors: {self.errors_count} | "
                    f"Circuit: {'OPEN' if self.circuit_open else 'CLOSED'}"
                )
                
                # Publish metrics (defensive: check for None and method existence)
                if self.metrics_collector is not None:
                    try:
                        self.metrics_collector.set_gauge("options_chain_curator.uptime_sec", uptime_sec)
                        self.metrics_collector.set_gauge("options_chain_curator.chains_curated_total", self.chains_curated)
                        self.metrics_collector.set_gauge("options_chain_curator.options_processed_total", self.options_processed)
                        self.metrics_collector.set_gauge("options_chain_curator.errors_total", self.errors_count)
                        self.metrics_collector.set_gauge("options_chain_curator.circuit_open", 1 if self.circuit_open else 0)
                    except (AttributeError, TypeError) as e:
                        logger.debug(f"Metrics collector method not available: {e}")
                
                self.last_health_check = current_time
                
            except asyncio.CancelledError:
                logger.info("Health monitor cancelled")
                break
            except Exception as e:
                logger.error(f"Error in health monitor: {e}")
    
    async def _publish_incident(self, incident_type: str, details: str):
        """Publish critical incident with institutional headers for audit trail."""
        try:
            if not self.streaming_bus:
                return
            
            incident = {
                "agent": "options_chain_curator",
                "incident_type": incident_type,
                "details": details,
                "timestamp_utc_us": int(time.time() * 1_000_000),
                "severity": "critical"
            }
            
            # Incidents need lineage tracking for regulatory compliance
            if not hasattr(self, '_incident_sequence'):
                self._incident_sequence = 0
            
            await self.streaming_bus.publish_with_canonical_headers(
                topic="incidents.options_curator",
                partition_key="options_chain_curator",
                payload=incident,
                source_id="options_chain_curator.001",
                sequence_number=self._incident_sequence,
                producer_version="1.0.0"
            )
            
            self._incident_sequence += 1
            
        except Exception as e:
            logger.error(f"Error publishing incident: {e}")
    
    async def stop(self):
        """Graceful shutdown."""
        logger.info("Stopping Options Chain Curator...")
        self.running = False
        
        # Build final chains from pending options
        try:
            current_time_us = int(time.time() * 1_000_000)
            chain_keys = list(self.organizer.pending_contracts.keys())
            
            for chain_key in chain_keys:
                try:
                    symbol, venue = chain_key.split(":", 1)
                    chain = self.organizer.build_curated_chain(symbol, venue, current_time_us)
                    if chain:
                        await self._publish_curated_chain(chain)
                        logger.info(f"Final chain published: {symbol}@{venue}")
                except Exception as e:
                    logger.error(f"Error publishing final chain for {chain_key}: {e}")
        except Exception as e:
            logger.error(f"Error during final chain building: {e}")
        
        # Stop streaming bus (graceful cleanup - StreamingBus handles its own shutdown)
        if self.streaming_bus:
            logger.info("Streaming bus cleanup completed")
        
        logger.info("Options Chain Curator stopped")

# =============================
# MAIN ENTRY POINT
# =============================

async def main():
    """Main entry point for Options Chain Curator."""
    curator = OptionsChainCurator()
    
    try:
        await curator.start()
        
        # Keep running until interrupted
        while curator.running:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await curator.stop()

if __name__ == "__main__":
    asyncio.run(main())
