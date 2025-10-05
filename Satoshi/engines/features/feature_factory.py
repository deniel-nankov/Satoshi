"""
Feature Factory - Trusted feature engineering with leakage-proof lineage tracking.

This module provides a comprehensive feature engineering system that converts raw signals
into well-defined, robust features and labels while maintaining strict domain boundaries
and complete provenance tracking.

Mission: Build robust features (returns/RV/jumps; microstructure; funding & basis term 
structures; vol factors; wallet-cohort flows) with impeccable lineage and parity.

Author: Satoshi HFT System
Date: October 2025
"""

import asyncio
import hashlib
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import warnings

import numpy as np
import pandas as pd
from scipy import stats
from scipy.signal import butter, filtfilt

# Import existing Leakage Police from data layer (proper domain owner)
from ..data.leakage_police import LeakagePolice, LeakagePoliceConfig


class FeatureType(Enum):
    """Enumeration of supported feature types."""
    RETURNS = "returns"
    REALIZED_VOLATILITY = "realized_volatility"
    JUMPS = "jumps"
    MICROSTRUCTURE = "microstructure"
    FUNDING_BASIS = "funding_basis"
    TERM_STRUCTURE = "term_structure"
    VOL_FACTORS = "vol_factors"
    WALLET_FLOWS = "wallet_flows"
    CROSS_ASSET = "cross_asset"
    TECHNICAL = "technical"
    # Hidden Alpha Feature Types for Non-Arbitrage Strategies
    REGIME_SIGNALS = "regime_signals"
    INFORMATION_DECAY = "information_decay"
    FLOW_PRESSURE = "flow_pressure"
    SENTIMENT_STRUCTURE = "sentiment_structure"
    LIQUIDITY_GRADIENTS = "liquidity_gradients"
    VOLATILITY_SURFACE = "volatility_surface"
    CORRELATION_DYNAMICS = "correlation_dynamics"
    MARKET_EFFICIENCY = "market_efficiency"


class FeatureUnits(Enum):
    """Standard units for features."""
    BASIS_POINTS = "bps"
    PERCENTAGE = "pct"
    DOLLARS = "usd"
    SATOSHIS = "sats"
    SHARES = "shares"
    RATIO = "ratio"
    COUNT = "count"
    SECONDS = "seconds"
    MICROSECONDS = "us"
    STANDARDIZED = "z_score"
    LOG_RETURNS = "log_ret"
    ANNUALIZED_VOLATILITY = "annualized_vol"  # For volatility features
    # Information Theory Units for Alpha Discovery
    ENTROPY = "entropy"
    MUTUAL_INFO = "mutual_info"
    INFORMATION_RATIO = "info_ratio"
    PREDICTIVE_POWER = "pred_power"
    ALPHA_DECAY = "alpha_decay"
    REGIME_PROBABILITY = "regime_prob"


class DriftStatus(Enum):
    """Feature drift monitoring status."""
    STABLE = "stable"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


@dataclass
class FeatureProvenance:
    """Complete lineage tracking for feature engineering."""
    feature_id: str
    source_datasets: List[str]
    transformation_pipeline: List[str]
    parameters: Dict[str, Any]
    creation_timestamp: int
    data_version: str
    algorithm_version: str
    validation_checksum: str
    dependency_graph: Dict[str, List[str]]
    code_hash: str = "feature_factory_v1.0"  # File/module version hash
    git_sha: str = "unknown"  # Git commit hash for reproducibility
    data_watermark: Optional[int] = None  # Latest data timestamp used
    late_data_dropped: int = 0  # Count of late/out-of-order data dropped


@dataclass
class FeatureVector:
    """Standardized feature output with metadata."""
    feature_id: str
    feature_type: FeatureType
    values: np.ndarray
    timestamps: np.ndarray
    window_size: int
    horizon: int
    units: FeatureUnits
    provenance: FeatureProvenance
    leakage_proof_id: str
    quality_score: float
    drift_status: DriftStatus
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FeatureDriftMetrics:
    """Drift monitoring metrics for features."""
    feature_id: str
    current_mean: float
    current_std: float
    baseline_mean: float
    baseline_std: float
    drift_score: float
    p_value: float
    last_updated: int
    sample_size: int
    drift_threshold: float = 0.05


@dataclass
class FeatureFactoryConfig:
    """Configuration for Feature Factory operations."""
    # Performance settings
    batch_size: int = 10000
    max_memory_usage_gb: float = 8.0
    parallel_workers: int = 4
    feature_cache_size: int = 1000
    
    # Quality thresholds
    min_feature_sla: float = 0.99
    max_drift_threshold: float = 0.05
    min_quality_score: float = 0.85
    outlier_threshold: float = 5.0
    
    # Window and horizon defaults
    default_lookback_window: int = 252  # Trading days
    default_prediction_horizon: int = 1  # Days
    max_window_size: int = 5000
    min_window_size: int = 10
    
    # Feature engineering parameters
    volatility_scaling: float = np.sqrt(252)  # Annualization
    jump_threshold: float = 3.0  # Standard deviations
    microstructure_window: int = 100  # Ticks
    funding_rate_horizon: int = 8  # Hours
    
    # Drift monitoring
    drift_check_frequency: int = 3600  # Seconds
    drift_baseline_window: int = 30  # Days
    drift_alert_threshold: float = 0.01
    
    # Data validation
    max_missing_ratio: float = 0.1
    min_data_points: int = 100
    timestamp_tolerance_us: int = 1000000  # 1 second
    
    # Leakage Police service configuration
    leakage_police_config: Dict[str, Any] = field(default_factory=lambda: {
        'timestamp_tolerance_us': 1000000,  # 1 second
        'lookbehind_buffer_us': 100000,     # 100ms processing buffer
        'min_confidence_threshold': 0.95    # 95% confidence required
    })


class FeatureFactory:
    """
    Trusted feature engineering factory with leakage-proof lineage tracking.
    
    Converts raw signals into robust, well-defined features and labels while maintaining
    strict domain boundaries and complete provenance tracking.
    """
    
    def __init__(self, config: FeatureFactoryConfig):
        self.config = config
        self.session_id = self._generate_session_id()
        
        # Feature tracking
        self.feature_registry: Dict[str, FeatureVector] = {}
        self.provenance_store: Dict[str, FeatureProvenance] = {}
        self.drift_tracker: Dict[str, FeatureDriftMetrics] = {}
        
        # Performance monitoring
        self.feature_stats = {
            "total_features_created": 0,
            "total_processing_time": 0.0,
            "sla_violations": 0,
            "drift_alerts": 0,
            "quality_failures": 0
        }
        
        # Caching for efficiency
        self.feature_cache: Dict[str, FeatureVector] = {}
        self.computation_cache: Dict[str, Any] = {}
        
        # Leakage prevention
        self.leakage_checksums: Set[str] = set()
        self.temporal_boundaries: Dict[str, int] = {}
        
    def _generate_session_id(self) -> str:
        """Generate unique session ID for this factory instance."""
        timestamp = int(time.time() * 1_000_000)
        random_component = hashlib.md5(f"{timestamp}_{id(self)}".encode()).hexdigest()[:8]
        return f"feature_factory_{timestamp}_{random_component}"
    
    def _generate_feature_id(self, feature_type: FeatureType, parameters: Dict[str, Any]) -> str:
        """Generate deterministic feature ID based on type and parameters."""
        param_str = json.dumps(parameters, sort_keys=True, default=str)
        param_hash = hashlib.sha256(param_str.encode()).hexdigest()[:12]
        return f"{feature_type.value}_{param_hash}"
    
    async def _validate_with_leakage_police(self, timestamps: np.ndarray, window_size: int, 
                                           horizon: int, feature_data: Optional[np.ndarray] = None,
                                           parameters: Optional[Dict[str, Any]] = None) -> str:
        """
        Delegate temporal integrity validation to existing Leakage Police (Agent #9).
        
        This method properly delegates leakage detection to the existing implementation
        in /Satoshi/engines/data/leakage_police.py instead of duplicating logic.
        
        Args:
            timestamps: Array of timestamps to validate
            window_size: Window size for feature computation
            horizon: Prediction horizon
            feature_data: Optional feature data for alignment validation
            parameters: Optional computation parameters
            
        Returns:
            str: Leakage proof ID from existing Leakage Police
            
        Raises:
            ValueError: If validation fails
        """
        # Create LeakagePolice instance with our config
        leakage_config = LeakagePoliceConfig(
            temporal_tolerance_ms=self.config.timestamp_tolerance_us // 1000,  # Convert μs to ms
            label_horizon_us=horizon,
            embargo_us=self.config.leakage_police_config.get('embargo_us', 86400000000),
            min_samples_for_analysis=max(10, len(timestamps)),
            **self.config.leakage_police_config
        )
        
        leakage_police = LeakagePolice(leakage_config)
        
        # Create minimal DataFrames for analysis
        feature_df = pd.DataFrame({
            'timestamp': timestamps,
            'feature_data': feature_data.flatten()[:len(timestamps)] if feature_data is not None else np.zeros(len(timestamps))
        })
        
        label_df = pd.DataFrame({
            'timestamp': timestamps + horizon,  # Labels are horizon ahead
            'target': np.zeros(len(timestamps))  # Dummy target for validation
        })
        
        # Request temporal ordering analysis
        incidents = await leakage_police.analyze_temporal_ordering(
            features=feature_df,
            labels=label_df,
            feature_timestamp_col='timestamp',
            label_timestamp_col='timestamp'
        )
        
        # Check if any critical incidents were found
        for incident in incidents:
            if incident.severity.value in ['high', 'critical']:
                raise ValueError(f"Leakage Police validation failed: {incident.description} "
                               f"(severity: {incident.severity.value}, confidence: {incident.confidence_score:.3f})")
        
        # Generate leakage proof ID from successful validation
        if incidents:
            # Use incident ID as proof (validation detected minor issues but not critical)
            return f"LP_validated_{incidents[0].incident_id}"
        else:
            # Clean validation - generate proof ID
            content = {
                "timestamps_hash": hashlib.sha256(timestamps.tobytes()).hexdigest()[:16],
                "window_size": window_size,
                "horizon": horizon,
                "parameters": sorted(parameters.items()) if parameters else [],
                "validation_timestamp": int(time.time() * 1000000)
            }
            content_str = json.dumps(content, sort_keys=True, default=str)
            return f"LP_clean_{hashlib.sha256(content_str.encode()).hexdigest()[:16]}"
    
    def _generate_leakage_proof_id_from_validation(
        self,
        validation_result: Dict[str, Any],
        timestamps: np.ndarray,
        window_size: int,
        horizon: int,
        parameters: Optional[Dict[str, Any]] = None
    ) -> str:
        """Generate leakage proof ID from existing Leakage Police validation result."""
        if validation_result.get("proof_id"):
            return validation_result["proof_id"]
        
        # Generate proof ID from validation context
        content = {
            "validation_status": validation_result.get("status", "unknown"),
            "timestamps_hash": hashlib.sha256(timestamps.tobytes()).hexdigest()[:16],
            "window_size": window_size,
            "horizon": horizon,
            "parameters": sorted(parameters.items()) if parameters else [],
            "validation_timestamp": int(time.time() * 1000000)
        }
        content_str = json.dumps(content, sort_keys=True, default=str)
        return f"LP_valid_{hashlib.sha256(content_str.encode()).hexdigest()[:16]}"
    
    async def _validate_with_existing_leakage_police(
        self, 
        timestamps: np.ndarray, 
        window_size: int, 
        horizon: int,
        feature_data: Optional[np.ndarray] = None,
        parameters: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, str]:
        """
        Validate temporal integrity using existing Leakage Police agent.
        
        This method delegates all validation to the existing LeakagePolice 
        implementation in the data layer, ensuring proper domain separation.
        
        Args:
            timestamps: Array of timestamps for validation
            window_size: Window size for temporal analysis  
            horizon: Forward-looking horizon
            feature_data: Optional feature data for validation
            parameters: Optional parameters for validation context
            
        Returns:
            Tuple[bool, str]: (is_valid, leakage_proof_id)
            
        Raises:
            ValueError: If validation fails or detects leakage
        """
        try:
            # Create configuration for existing Leakage Police
            config = LeakagePoliceConfig(
                temporal_tolerance_ms=50,
                future_window_hours=24,
                label_horizon_us=horizon * 1000000,  # Convert to microseconds
                statistical_threshold=0.001
            )
            
            # Initialize existing Leakage Police agent
            leakage_police = LeakagePolice(config)
            
            # Convert numpy arrays to pandas for existing interface
            features_df = pd.DataFrame({
                'timestamp': timestamps,
                'feature_window_size': [window_size] * len(timestamps)
            })
            
            # Create simple labels df for temporal validation
            labels_df = pd.DataFrame({
                'timestamp': timestamps + horizon * 1000000,  # Future timestamps
                'label': np.ones(len(timestamps))  # Dummy labels for validation
            })
            
            # Use existing implementation for temporal ordering analysis
            incidents = await leakage_police.analyze_temporal_ordering(
                features=features_df,
                labels=labels_df,
                feature_timestamp_col='timestamp',
                label_timestamp_col='timestamp'
            )
            
            # Check for critical incidents
            critical_incidents = [inc for inc in incidents if inc.severity.value in ['high', 'critical']]
            
            if critical_incidents:
                incident_msg = "; ".join([f"{inc.description} (severity: {inc.severity.value})" 
                                        for inc in critical_incidents[:3]])
                raise ValueError(f"Critical leakage detected: {incident_msg}")
            
            # Generate leakage proof ID using validation context
            proof_id = self._generate_leakage_proof_id_from_validation(
                {"status": "valid", "incidents": len(incidents)}, 
                timestamps, window_size, horizon, parameters
            )
            
            return True, proof_id
            
        except Exception as e:
            # Use print instead of self.logger since logger might not exist
            print(f"Leakage Police validation failed: {e}")
            raise ValueError(f"Validation error: {e}")
    
    def _validate_temporal_integrity_sync(self, timestamps: np.ndarray, window_size: int, 
                                         horizon: int, feature_data: Optional[np.ndarray] = None,
                                         parameters: Optional[Dict[str, Any]] = None) -> str:
        """
        Synchronous wrapper for existing Leakage Police validation.
        
        Returns:
            str: Leakage proof ID from existing Leakage Police
            
        Raises:
            ValueError: If validation fails
        """
        # Run async validation in sync context
        loop = None
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        if loop.is_running():
            # If we're already in an async context, create a new event loop in a thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(
                    asyncio.run,
                    self._validate_with_leakage_police(timestamps, window_size, horizon, feature_data, parameters)
                )
                return future.result()
        else:
            # Direct async execution
            return loop.run_until_complete(
                self._validate_with_leakage_police(timestamps, window_size, horizon, feature_data, parameters)
            )
    
    def _compute_quality_score(self, feature_data: np.ndarray, 
                             timestamps: np.ndarray) -> float:
        """Compute feature quality score based on multiple criteria."""
        scores = []
        
        # Completeness score
        completeness = 1.0 - (np.isnan(feature_data).sum() / len(feature_data))
        scores.append(completeness)
        
        # Stability score (avoid extreme outliers)
        if len(feature_data) > 1:
            z_scores = np.abs(stats.zscore(feature_data, nan_policy='omit'))
            stability = 1.0 - (np.sum(z_scores > self.config.outlier_threshold) / len(z_scores))
            scores.append(stability)
        else:
            scores.append(0.5)  # Neutral score for insufficient data
        
        # Temporal consistency score
        if len(timestamps) > 1:
            time_diffs = np.diff(timestamps)
            cv_time = np.std(time_diffs) / np.mean(time_diffs) if np.mean(time_diffs) > 0 else 1.0
            temporal_consistency = max(0.0, 1.0 - cv_time)
            scores.append(temporal_consistency)
        else:
            scores.append(0.5)
        
        # Information content score (robust coefficient of variation)
        feature_std = np.std(feature_data)
        feature_mean = np.abs(np.mean(feature_data))
        
        if feature_std > 1e-10:
            if feature_mean > 1e-10:
                # Traditional coefficient of variation when mean is not near zero
                cv = feature_std / feature_mean
                info_score = min(1.0, cv / 2.0)  # Normalize CV to [0,1] range
            else:
                # Entropy-like measure when mean ≈ 0 (use variability directly)
                # Normalize by data range to get scale-invariant measure
                data_range = np.max(feature_data) - np.min(feature_data)
                if data_range > 1e-10:
                    info_score = min(1.0, feature_std / data_range)
                else:
                    info_score = 0.1  # Constant data
            scores.append(info_score)
        else:
            scores.append(0.1)  # Low score for constant features
        
        return float(np.mean(scores))
    
    async def compute_returns_features(self, price_data: pd.DataFrame,
                                     window_size: int = 20,
                                     horizon: int = 1,
                                     price_col: str = "close",
                                     timestamp_col: str = "timestamp") -> FeatureVector:
        """Compute various return-based features with multiple horizons."""
        if len(price_data) < window_size + horizon:
            raise ValueError(f"Insufficient data: need {window_size + horizon}, got {len(price_data)}")
        
        # Sort input data by timestamp for deterministic results
        price_data_sorted = price_data.sort_values(timestamp_col).reset_index(drop=True)
        
        prices = np.array(price_data_sorted[price_col].values, dtype=float)
        timestamps = np.array(price_data_sorted[timestamp_col].values)
        
        # Validate temporal integrity with existing Leakage Police agent
        parameters = {'feature_type': 'returns', 'window_size': window_size, 'horizon': horizon}
        leakage_proof_id = self._validate_temporal_integrity_sync(timestamps, window_size, horizon, prices, parameters)
        
        # Compute log returns
        log_prices = np.log(np.array(prices, dtype=float))
        returns = np.diff(log_prices)
        
        # Feature engineering with proper timestamp alignment
        features = []
        feature_timestamps = []
        
        for i in range(window_size, len(returns) - horizon + 1):
            window_returns = returns[i-window_size:i]
            
            # Multiple return features
            feature_vector = np.array([
                np.mean(window_returns),  # Mean return
                np.std(window_returns),   # Volatility
                stats.skew(window_returns),  # Skewness
                stats.kurtosis(window_returns),  # Kurtosis
                np.percentile(window_returns, 5),  # VaR 5%
                np.percentile(window_returns, 95),  # VaR 95%
                np.sum(window_returns > 0) / len(window_returns),  # Win rate
                np.max(np.maximum.accumulate(window_returns.cumsum()) - window_returns.cumsum()),  # Max drawdown
            ])
            
            features.append(feature_vector)
            # Use timestamp at end of window (returns[i-1] uses prices[i-1] to prices[i])
            # This ensures feature represents "information available at time t"
            feature_timestamps.append(timestamps[i])
        
        features_array = np.array(features)
        timestamps_array = np.array(feature_timestamps)
        
        # Create provenance
        parameters = {
            "window_size": window_size,
            "horizon": horizon,
            "price_column": price_col,
            "feature_count": features_array.shape[1]
        }
        
        feature_id = self._generate_feature_id(FeatureType.RETURNS, parameters)
        # Use leakage proof ID from validation above (already generated with existing Leakage Police)
        
        provenance = FeatureProvenance(
            feature_id=feature_id,
            source_datasets=[f"price_data_{price_col}"],
            transformation_pipeline=["log_transform", "rolling_window", "statistical_moments"],
            parameters=parameters,
            creation_timestamp=int(time.time() * 1_000_000),
            data_version="1.0",
            algorithm_version="1.0",
            validation_checksum=leakage_proof_id,
            dependency_graph={"price_data": [price_col, timestamp_col]}
        )
        
        quality_score = self._compute_quality_score(features_array.flatten(), timestamps_array)
        
        return FeatureVector(
            feature_id=feature_id,
            feature_type=FeatureType.RETURNS,
            values=features_array,
            timestamps=timestamps_array,
            window_size=window_size,
            horizon=horizon,
            units=FeatureUnits.LOG_RETURNS,
            provenance=provenance,
            leakage_proof_id=leakage_proof_id,
            quality_score=quality_score,
            drift_status=DriftStatus.UNKNOWN,
            metadata={
                "feature_names": ["mean_return", "volatility", "skewness", "kurtosis", 
                                "var_5pct", "var_95pct", "win_rate", "max_drawdown"],
                "computation_time": time.time(),
                "log_base": "natural_log",  # Document base for parity
                "return_calculation": "ln(P_t / P_{t-1})"
            }
        )
    
    async def compute_realized_volatility_features(self, price_data: pd.DataFrame,
                                                 window_size: int = 252,
                                                 horizon: int = 1,
                                                 price_col: str = "close",
                                                 timestamp_col: str = "timestamp") -> FeatureVector:
        """Compute realized volatility features with multiple estimators."""
        if len(price_data) < window_size + horizon:
            raise ValueError(f"Insufficient data: need {window_size + horizon}, got {len(price_data)}")
        
        prices = price_data[price_col].values
        timestamps = price_data[timestamp_col].values
        
        # Validate temporal integrity with existing Leakage Police agent
        timestamps_array = np.array(timestamps)
        prices_array = np.array(prices)
        parameters = {'feature_type': 'realized_volatility', 'window_size': window_size, 'horizon': horizon}
        leakage_proof_id = self._validate_temporal_integrity_sync(timestamps_array, window_size, horizon, 
                                                                 prices_array, parameters)
        
        # Compute log returns
        log_prices = np.log(prices_array)
        returns = np.diff(log_prices)
        
        # Feature engineering
        features = []
        feature_timestamps = []
        
        for i in range(window_size, len(returns) - horizon + 1):
            window_returns = returns[i-window_size:i]
            
            # Multiple volatility estimators
            rv_simple = np.std(window_returns) * self.config.volatility_scaling
            rv_parkinson = self._compute_parkinson_volatility(window_returns) if len(window_returns) > 1 else rv_simple
            rv_garch = self._compute_ewma_volatility(window_returns, alpha=0.94)
            rv_realized = np.sqrt(np.sum(window_returns**2)) * self.config.volatility_scaling
            
            # Volatility of volatility
            if len(window_returns) >= 20:
                rolling_vols = [np.std(window_returns[j:j+20]) for j in range(len(window_returns)-20+1)]
                vol_of_vol = np.std(rolling_vols) * self.config.volatility_scaling
            else:
                vol_of_vol = 0.0
            
            feature_vector = np.array([
                rv_simple,
                rv_parkinson,
                rv_garch,
                rv_realized,
                vol_of_vol,
                rv_simple / rv_realized if rv_realized > 0 else 1.0,  # Efficiency ratio
            ])
            
            features.append(feature_vector)
            feature_timestamps.append(timestamps[i])
        
        features_array = np.array(features)
        timestamps_array = np.array(feature_timestamps)
        
        # Create provenance
        parameters = {
            "window_size": window_size,
            "horizon": horizon,
            "volatility_scaling": self.config.volatility_scaling,
            "estimators": ["simple", "parkinson", "ewma", "realized", "vol_of_vol"]
        }
        
        feature_id = self._generate_feature_id(FeatureType.REALIZED_VOLATILITY, parameters)
        # Use leakage proof ID from validation above (already generated with existing Leakage Police)
        
        provenance = FeatureProvenance(
            feature_id=feature_id,
            source_datasets=[f"price_data_{price_col}"],
            transformation_pipeline=["log_transform", "volatility_estimators", "scaling"],
            parameters=parameters,
            creation_timestamp=int(time.time() * 1_000_000),
            data_version="1.0",
            algorithm_version="1.0",
            validation_checksum=leakage_proof_id,
            dependency_graph={"price_data": [price_col, timestamp_col]}
        )
        
        quality_score = self._compute_quality_score(features_array.flatten(), timestamps_array)
        
        return FeatureVector(
            feature_id=feature_id,
            feature_type=FeatureType.REALIZED_VOLATILITY,
            values=features_array,
            timestamps=timestamps_array,
            window_size=window_size,
            horizon=horizon,
            units=FeatureUnits.ANNUALIZED_VOLATILITY,
            provenance=provenance,
            leakage_proof_id=leakage_proof_id,
            quality_score=quality_score,
            drift_status=DriftStatus.UNKNOWN,
            metadata={
                "feature_names": ["rv_simple", "rv_parkinson", "rv_ewma", "rv_realized", "vol_of_vol", "efficiency_ratio"],
                "computation_time": time.time(),
                "volatility_base": "natural_log_returns",
                "annualization_factor": self.config.volatility_scaling
            }
        )
    
    def _compute_parkinson_volatility(self, returns: np.ndarray) -> float:
        """Compute Parkinson volatility estimator."""
        if len(returns) < 2:
            return np.std(returns) if len(returns) > 0 else 0.0
        
        # Simplified Parkinson for returns data
        return np.std(returns) * np.sqrt(2 * np.log(2)) * self.config.volatility_scaling
    
    def _compute_ewma_volatility(self, returns: np.ndarray, alpha: float = 0.94) -> float:
        """
        Compute EWMA volatility estimator using RiskMetrics-style zero-mean variance.
        
        Uses exponentially weighted moving average of squared returns (assumes zero mean).
        This is the RiskMetrics convention: EWMA_var = (1-α) * Σ(α^i * r_{t-i}^2)
        
        Args:
            returns: Array of returns
            alpha: Decay factor (default 0.94 follows RiskMetrics)
            
        Returns:
            Annualized volatility
        """
        if len(returns) == 0:
            return 0.0
        
        weights = np.array([(1-alpha) * (alpha**i) for i in range(len(returns))])
        weights = weights[::-1]  # Reverse for proper weighting
        weights /= weights.sum()
        
        ewma_var = np.sum(weights * returns**2)  # Zero-mean variance
        return float(np.sqrt(ewma_var) * self.config.volatility_scaling)
    
    async def compute_jump_features(self, price_data: pd.DataFrame,
                                  window_size: int = 100,
                                  horizon: int = 1,
                                  price_col: str = "close",
                                  timestamp_col: str = "timestamp") -> FeatureVector:
        """Detect and characterize price jumps."""
        if len(price_data) < window_size + horizon:
            raise ValueError(f"Insufficient data: need {window_size + horizon}, got {len(price_data)}")
        
        prices = price_data[price_col].values
        timestamps = price_data[timestamp_col].values
        
        # Validate temporal integrity with existing Leakage Police agent
        timestamps_array = np.array(timestamps)
        prices_array = np.array(prices)
        parameters = {'feature_type': 'jumps', 'window_size': window_size, 'horizon': horizon}
        leakage_proof_id = self._validate_temporal_integrity_sync(timestamps_array, window_size, horizon, 
                                                                 prices_array, parameters)
        
        # Compute returns
        log_prices = np.log(prices_array)
        returns = np.diff(log_prices)
        
        # Feature engineering
        features = []
        feature_timestamps = []
        
        for i in range(window_size, len(returns) - horizon + 1):
            window_returns = returns[i-window_size:i]
            current_return = returns[i] if i < len(returns) else 0.0
            
            # Jump detection using rolling statistics
            rolling_mean = np.mean(window_returns)
            rolling_std = np.std(window_returns)
            
            # Z-score based jump detection
            z_score = (current_return - rolling_mean) / rolling_std if rolling_std > 0 else 0.0
            is_jump = abs(z_score) > self.config.jump_threshold
            
            # Jump characteristics
            jump_magnitude = abs(current_return - rolling_mean)
            jump_direction = np.sign(current_return - rolling_mean)
            
            # Historical jump statistics
            historical_jumps = np.abs(window_returns - rolling_mean) > (self.config.jump_threshold * rolling_std)
            jump_frequency = np.sum(historical_jumps) / len(window_returns)
            
            # Jump intensity (Lee-Mykland test approximation)
            if rolling_std > 0:
                jump_intensity = np.sum(np.abs(window_returns - rolling_mean) / rolling_std > self.config.jump_threshold)
            else:
                jump_intensity = 0.0
            
            feature_vector = np.array([
                float(is_jump),
                jump_magnitude,
                jump_direction,
                jump_frequency,
                jump_intensity,
                abs(z_score),
                np.percentile(np.abs(window_returns - rolling_mean), 95) / rolling_std if rolling_std > 0 else 0.0
            ])
            
            features.append(feature_vector)
            feature_timestamps.append(timestamps[i])
        
        features_array = np.array(features)
        timestamps_array = np.array(feature_timestamps)
        
        # Create provenance
        parameters = {
            "window_size": window_size,
            "horizon": horizon,
            "jump_threshold": self.config.jump_threshold,
            "detection_method": "z_score"
        }
        
        feature_id = self._generate_feature_id(FeatureType.JUMPS, parameters)
        # Use leakage proof ID from validation above (already generated with existing Leakage Police)
        
        provenance = FeatureProvenance(
            feature_id=feature_id,
            source_datasets=[f"price_data_{price_col}"],
            transformation_pipeline=["log_transform", "rolling_statistics", "jump_detection"],
            parameters=parameters,
            creation_timestamp=int(time.time() * 1_000_000),
            data_version="1.0",
            algorithm_version="1.0",
            validation_checksum=leakage_proof_id,
            dependency_graph={"price_data": [price_col, timestamp_col]}
        )
        
        quality_score = self._compute_quality_score(features_array.flatten(), timestamps_array)
        
        return FeatureVector(
            feature_id=feature_id,
            feature_type=FeatureType.JUMPS,
            values=features_array,
            timestamps=timestamps_array,
            window_size=window_size,
            horizon=horizon,
            units=FeatureUnits.STANDARDIZED,
            provenance=provenance,
            leakage_proof_id=leakage_proof_id,
            quality_score=quality_score,
            drift_status=DriftStatus.UNKNOWN,
            metadata={
                "feature_names": ["is_jump", "jump_magnitude", "jump_direction", "jump_frequency", 
                                "jump_intensity", "z_score", "extreme_percentile"],
                "computation_time": time.time()
            }
        )
    
    async def compute_microstructure_features(self, orderbook_data: pd.DataFrame,
                                            trades_data: pd.DataFrame,
                                            window_size: int = 100,
                                            horizon: int = 1,
                                            timestamp_col: str = "timestamp") -> FeatureVector:
        """Compute microstructure features from orderbook and trades data."""
        if len(orderbook_data) < window_size + horizon or len(trades_data) < window_size:
            raise ValueError("Insufficient data for microstructure analysis")
        
        # Align timestamps between orderbook and trades
        common_timestamps = np.intersect1d(orderbook_data[timestamp_col], trades_data[timestamp_col])
        if len(common_timestamps) < window_size + horizon:
            raise ValueError("Insufficient aligned timestamps")
        
        # Validate temporal integrity with existing Leakage Police agent
        timestamps_array = np.array(common_timestamps)
        parameters = {'feature_type': 'microstructure', 'window_size': window_size, 'horizon': horizon}
        leakage_proof_id = self._validate_temporal_integrity_sync(timestamps_array, window_size, horizon, 
                                                                 None, parameters)
        
        # Feature engineering
        features = []
        feature_timestamps = []
        
        for i in range(window_size, len(common_timestamps) - horizon + 1):
            window_ts = common_timestamps[i-window_size:i]
            
            # Get window data with proper timestamp sorting
            ob_window = orderbook_data[orderbook_data[timestamp_col].isin(window_ts)].sort_values(timestamp_col)
            trades_window = trades_data[trades_data[timestamp_col].isin(window_ts)].sort_values(timestamp_col)
            
            if len(ob_window) == 0 or len(trades_window) == 0:
                continue
            
            # Bid-ask spread features
            spreads = ob_window['ask_price'] - ob_window['bid_price']
            spread_mean = np.mean(spreads)
            spread_std = np.std(spreads)
            relative_spread = spread_mean / np.mean(ob_window['mid_price']) if 'mid_price' in ob_window.columns else 0.0
            
            # Order flow imbalance
            if 'bid_size' in ob_window.columns and 'ask_size' in ob_window.columns:
                bid_sizes = np.array(ob_window['bid_size'].values, dtype=float)
                ask_sizes = np.array(ob_window['ask_size'].values, dtype=float)
                
                # Prevent division by zero in imbalance calculation
                denominator = bid_sizes + ask_sizes
                valid_mask = denominator != 0
                
                if np.any(valid_mask):
                    imbalance = np.zeros_like(denominator, dtype=float)
                    imbalance[valid_mask] = (bid_sizes[valid_mask] - ask_sizes[valid_mask]) / denominator[valid_mask]
                    imbalance_mean = np.mean(imbalance)
                    imbalance_std = np.std(imbalance)
                else:
                    imbalance_mean = imbalance_std = 0.0
            else:
                imbalance_mean = imbalance_std = 0.0
            
            # Trade characteristics
            if 'volume' in trades_window.columns:
                volume_mean = np.mean(trades_window['volume'])
                volume_std = np.std(trades_window['volume'])
                trade_count = len(trades_window)
            else:
                volume_mean = volume_std = trade_count = 0.0
            
            # Price impact proxy
            if 'price' in trades_window.columns and len(trades_window) > 1:
                price_changes = np.diff(np.array(trades_window['price'].values, dtype=float))
                price_impact = np.std(price_changes) if len(price_changes) > 0 else 0.0
            else:
                price_impact = 0.0
            
            feature_vector = np.array([
                spread_mean,
                spread_std,
                relative_spread,
                imbalance_mean,
                imbalance_std,
                volume_mean,
                volume_std,
                float(trade_count),
                price_impact
            ])
            
            features.append(feature_vector)
            feature_timestamps.append(common_timestamps[i])
        
        if not features:
            raise ValueError("No valid microstructure features could be computed")
        
        features_array = np.array(features)
        timestamps_array = np.array(feature_timestamps)
        
        # Create provenance
        parameters = {
            "window_size": window_size,
            "horizon": horizon,
            "orderbook_features": ["spread", "imbalance"],
            "trade_features": ["volume", "count", "impact"]
        }
        
        feature_id = self._generate_feature_id(FeatureType.MICROSTRUCTURE, parameters)
        # Use leakage proof ID from validation above (already generated with existing Leakage Police)
        
        provenance = FeatureProvenance(
            feature_id=feature_id,
            source_datasets=["orderbook_data", "trades_data"],
            transformation_pipeline=["timestamp_alignment", "spread_calculation", "imbalance_calculation"],
            parameters=parameters,
            creation_timestamp=int(time.time() * 1_000_000),
            data_version="1.0",
            algorithm_version="1.0",
            validation_checksum=leakage_proof_id,
            dependency_graph={"orderbook": ["bid_price", "ask_price", "bid_size", "ask_size"], 
                            "trades": ["price", "volume", timestamp_col]}
        )
        
        quality_score = self._compute_quality_score(features_array.flatten(), timestamps_array)
        
        return FeatureVector(
            feature_id=feature_id,
            feature_type=FeatureType.MICROSTRUCTURE,
            values=features_array,
            timestamps=timestamps_array,
            window_size=window_size,
            horizon=horizon,
            units=FeatureUnits.RATIO,
            provenance=provenance,
            leakage_proof_id=leakage_proof_id,
            quality_score=quality_score,
            drift_status=DriftStatus.UNKNOWN,
            metadata={
                "feature_names": ["spread_mean", "spread_std", "relative_spread", "imbalance_mean", 
                                "imbalance_std", "volume_mean", "volume_std", "trade_count", "price_impact"],
                "computation_time": time.time()
            }
        )
    
    async def compute_funding_basis_features(self, spot_data: pd.DataFrame,
                                           futures_data: pd.DataFrame,
                                           funding_data: Optional[pd.DataFrame] = None,
                                           window_size: int = 24,  # Hours
                                           horizon: int = 8,  # Hours
                                           timestamp_col: str = "timestamp") -> FeatureVector:
        """Compute funding and basis features for crypto derivatives."""
        # Align spot and futures data
        common_timestamps = np.intersect1d(spot_data[timestamp_col], futures_data[timestamp_col])
        if len(common_timestamps) < window_size + horizon:
            raise ValueError("Insufficient aligned data for funding/basis analysis")
        
        # Validate temporal integrity with existing Leakage Police agent
        timestamps_array = np.array(common_timestamps)
        parameters = {'feature_type': 'funding_basis', 'window_size': window_size, 'horizon': horizon}
        leakage_proof_id = self._validate_temporal_integrity_sync(timestamps_array, window_size, horizon, 
                                                                 None, parameters)
        
        features = []
        feature_timestamps = []
        
        for i in range(window_size, len(common_timestamps) - horizon + 1):
            window_ts = common_timestamps[i-window_size:i]
            
            # Get window data
            spot_window = spot_data[spot_data[timestamp_col].isin(window_ts)]
            futures_window = futures_data[futures_data[timestamp_col].isin(window_ts)]
            
            if len(spot_window) == 0 or len(futures_window) == 0:
                continue
            
            # Basis calculation with proper timestamp alignment
            if len(spot_window) > 0 and len(futures_window) > 0:
                # Explicit timestamp merge (inner join) for proper alignment
                spot_ts_data = spot_window.set_index(timestamp_col)['price'] if 'price' in spot_window.columns else pd.Series(dtype=float)
                futures_ts_data = futures_window.set_index(timestamp_col)['price'] if 'price' in futures_window.columns else pd.Series(dtype=float)
                
                # Inner join on timestamps for exact alignment
                aligned_data = pd.DataFrame({
                    'spot': spot_ts_data,
                    'futures': futures_ts_data
                }).dropna()
                
                if len(aligned_data) > 0:
                    spot_prices = np.array(aligned_data['spot'].values, dtype=float)
                    futures_prices = np.array(aligned_data['futures'].values, dtype=float)
                    
                    basis = futures_prices - spot_prices
                    basis_pct = basis / spot_prices * 100  # Basis in percentage
                    
                    basis_mean = np.mean(basis_pct)
                    basis_std = np.std(basis_pct)
                    basis_trend = np.polyfit(range(len(basis_pct)), basis_pct, 1)[0] if len(basis_pct) > 1 else 0.0
                else:
                    basis_mean = basis_std = basis_trend = 0.0
                    basis_pct = np.array([])
            else:
                basis_mean = basis_std = basis_trend = 0.0
                basis_pct = np.array([])  # Initialize for later use
            
            # Funding rate features
            if funding_data is not None:
                funding_window = funding_data[funding_data[timestamp_col].isin(window_ts)]
                if 'funding_rate' in funding_window.columns and len(funding_window) > 0:
                    funding_rates = np.array(funding_window['funding_rate'].values, dtype=float)
                    funding_mean = np.mean(funding_rates)
                    funding_std = np.std(funding_rates)
                    funding_trend = np.polyfit(range(len(funding_rates)), funding_rates, 1)[0] if len(funding_rates) > 1 else 0.0
                else:
                    funding_mean = funding_std = funding_trend = 0.0
            else:
                funding_mean = funding_std = funding_trend = 0.0
            
            # Term structure features (simplified)
            if len(basis_pct) > self.config.funding_rate_horizon:
                short_term_basis = np.mean(basis_pct[:self.config.funding_rate_horizon])
                long_term_basis = np.mean(basis_pct[-self.config.funding_rate_horizon:])
                term_slope = long_term_basis - short_term_basis
            else:
                term_slope = 0.0
            
            feature_vector = np.array([
                basis_mean,
                basis_std,
                basis_trend,
                funding_mean,
                funding_std,
                funding_trend,
                term_slope,
                abs(basis_mean) / (basis_std if basis_std > 0 else 1.0)  # Basis signal-to-noise
            ])
            
            features.append(feature_vector)
            feature_timestamps.append(common_timestamps[i])
        
        if not features:
            raise ValueError("No valid funding/basis features could be computed")
        
        features_array = np.array(features)
        timestamps_array = np.array(feature_timestamps)
        
        # Create provenance
        parameters = {
            "window_size": window_size,
            "horizon": horizon,
            "funding_horizon": self.config.funding_rate_horizon,
            "has_funding_data": funding_data is not None
        }
        
        feature_id = self._generate_feature_id(FeatureType.FUNDING_BASIS, parameters)
        # Use leakage proof ID from validation above (already generated with existing Leakage Police)
        
        provenance = FeatureProvenance(
            feature_id=feature_id,
            source_datasets=["spot_data", "futures_data"] + (["funding_data"] if funding_data is not None else []),
            transformation_pipeline=["price_alignment", "basis_calculation", "funding_analysis"],
            parameters=parameters,
            creation_timestamp=int(time.time() * 1_000_000),
            data_version="1.0",
            algorithm_version="1.0",
            validation_checksum=leakage_proof_id,
            dependency_graph={"spot": ["price"], "futures": ["price"], "funding": ["funding_rate"]}
        )
        
        quality_score = self._compute_quality_score(features_array.flatten(), timestamps_array)
        
        return FeatureVector(
            feature_id=feature_id,
            feature_type=FeatureType.FUNDING_BASIS,
            values=features_array,
            timestamps=timestamps_array,
            window_size=window_size,
            horizon=horizon,
            units=FeatureUnits.BASIS_POINTS,
            provenance=provenance,
            leakage_proof_id=leakage_proof_id,
            quality_score=quality_score,
            drift_status=DriftStatus.UNKNOWN,
            metadata={
                "feature_names": ["basis_mean", "basis_std", "basis_trend", "funding_mean", 
                                "funding_std", "funding_trend", "term_slope", "basis_snr"],
                "computation_time": time.time()
            }
        )
    
    async def compute_wallet_flow_features(self, wallet_data: pd.DataFrame,
                                         cohort_col: str = "cohort",
                                         flow_col: str = "net_flow",
                                         balance_col: str = "balance",
                                         window_size: int = 168,  # Hours (1 week)
                                         horizon: int = 24,  # Hours
                                         timestamp_col: str = "timestamp") -> FeatureVector:
        """Compute wallet cohort flow features for on-chain analysis."""
        if len(wallet_data) < window_size + horizon:
            raise ValueError(f"Insufficient data: need {window_size + horizon}, got {len(wallet_data)}")
        
        # Get unique cohorts
        cohorts = wallet_data[cohort_col].unique()
        timestamps = sorted(wallet_data[timestamp_col].unique())
        
        # Validate temporal integrity with existing Leakage Police agent
        timestamps_array = np.array(timestamps)
        parameters = {'feature_type': 'wallet_flows', 'window_size': window_size, 'horizon': horizon}
        leakage_proof_id = self._validate_temporal_integrity_sync(timestamps_array, window_size, horizon, 
                                                                 None, parameters)
        
        if len(timestamps) < window_size + horizon:
            raise ValueError("Insufficient timestamps for analysis")
        
        features = []
        feature_timestamps = []
        
        for i in range(window_size, len(timestamps) - horizon + 1):
            window_ts = timestamps[i-window_size:i]
            window_data = wallet_data[wallet_data[timestamp_col].isin(window_ts)]
            
            if len(window_data) == 0:
                continue
            
            cohort_features = []
            
            # Analyze each cohort
            for cohort in cohorts:
                cohort_data = window_data[window_data[cohort_col] == cohort]
                
                if len(cohort_data) == 0:
                    # Fill with zeros for missing cohort data
                    cohort_features.extend([0.0, 0.0, 0.0, 0.0, 0.0])
                    continue
                
                # Flow analysis
                flows = cohort_data[flow_col].values if flow_col in cohort_data.columns else np.array([])
                balances = cohort_data[balance_col].values if balance_col in cohort_data.columns else np.array([])
                
                # Flow statistics
                flow_mean = np.mean(flows) if len(flows) > 0 else 0.0
                flow_std = np.std(flows) if len(flows) > 0 else 0.0
                flow_trend = np.polyfit(range(len(flows)), flows, 1)[0] if len(flows) > 1 else 0.0
                
                # Balance dynamics
                balance_change = (balances[-1] - balances[0]) / balances[0] if len(balances) > 1 and balances[0] != 0 else 0.0
                
                # Flow concentration
                flow_concentration = np.std(flows) / np.abs(np.mean(flows)) if len(flows) > 0 and np.mean(flows) != 0 else 0.0
                
                cohort_features.extend([flow_mean, flow_std, flow_trend, balance_change, flow_concentration])
            
            # Aggregate cross-cohort features
            if len(cohort_features) > 0:
                # Reshape to analyze cross-cohort dynamics
                cohort_matrix = np.array(cohort_features).reshape(len(cohorts), -1)
                
                # Cross-cohort correlation (guard for small cohort counts)
                if len(cohorts) >= 2 and cohort_matrix.shape[1] >= 2:
                    flow_balance_corr = np.corrcoef(cohort_matrix[:, 0], cohort_matrix[:, 3])[0, 1]
                    if np.isnan(flow_balance_corr):
                        flow_balance_corr = 0.0
                else:
                    flow_balance_corr = 0.0
                
                cross_cohort_features = [
                    np.mean(cohort_matrix[:, 0]),  # Average flow across cohorts
                    np.std(cohort_matrix[:, 0]),   # Flow diversity across cohorts
                    flow_balance_corr,  # Flow vs balance correlation (guarded)
                ]
                
                feature_vector = np.array(cohort_features + cross_cohort_features)
            else:
                feature_vector = np.zeros(len(cohorts) * 5 + 3)  # Default structure
            
            features.append(feature_vector)
            feature_timestamps.append(timestamps[i])
        
        if not features:
            raise ValueError("No valid wallet flow features could be computed")
        
        features_array = np.array(features)
        timestamps_array = np.array(feature_timestamps)
        
        # Create provenance
        parameters = {
            "window_size": window_size,
            "horizon": horizon,
            "cohort_count": len(cohorts),
            "cohorts": list(cohorts)
        }
        
        feature_id = self._generate_feature_id(FeatureType.WALLET_FLOWS, parameters)
        # Use leakage proof ID from validation above (already generated with existing Leakage Police)
        
        provenance = FeatureProvenance(
            feature_id=feature_id,
            source_datasets=["wallet_data"],
            transformation_pipeline=["cohort_grouping", "flow_analysis", "cross_cohort_correlation"],
            parameters=parameters,
            creation_timestamp=int(time.time() * 1_000_000),
            data_version="1.0",
            algorithm_version="1.0",
            validation_checksum=leakage_proof_id,
            dependency_graph={"wallet_data": [cohort_col, flow_col, balance_col, timestamp_col]}
        )
        
        quality_score = self._compute_quality_score(features_array.flatten(), timestamps_array)
        
        # Generate feature names dynamically
        feature_names = []
        for cohort in cohorts:
            feature_names.extend([f"{cohort}_flow_mean", f"{cohort}_flow_std", f"{cohort}_flow_trend", 
                                f"{cohort}_balance_change", f"{cohort}_flow_concentration"])
        feature_names.extend(["cross_cohort_flow_mean", "cross_cohort_flow_std", "flow_balance_corr"])
        
        return FeatureVector(
            feature_id=feature_id,
            feature_type=FeatureType.WALLET_FLOWS,
            values=features_array,
            timestamps=timestamps_array,
            window_size=window_size,
            horizon=horizon,
            units=FeatureUnits.RATIO,
            provenance=provenance,
            leakage_proof_id=leakage_proof_id,
            quality_score=quality_score,
            drift_status=DriftStatus.UNKNOWN,
            metadata={
                "feature_names": feature_names,
                "computation_time": time.time(),
                "cohort_count": len(cohorts)
            }
        )
    
    async def monitor_feature_drift(self, feature_vector: FeatureVector,
                                  baseline_data: Optional[np.ndarray] = None) -> FeatureDriftMetrics:
        """Monitor feature drift using statistical tests."""
        current_data = feature_vector.values.flatten()
        
        if baseline_data is None:
            # Use first 30 days as baseline if available
            baseline_window = min(self.config.drift_baseline_window, len(current_data) // 2)
            if baseline_window < self.config.min_data_points:
                return FeatureDriftMetrics(
                    feature_id=feature_vector.feature_id,
                    current_mean=float(np.mean(current_data)),
                    current_std=float(np.std(current_data)),
                    baseline_mean=float(np.mean(current_data)),
                    baseline_std=float(np.std(current_data)),
                    drift_score=0.0,
                    p_value=1.0,
                    last_updated=int(time.time() * 1_000_000),
                    sample_size=len(current_data)
                )
            
            baseline_data = current_data[:baseline_window]
            current_data = current_data[baseline_window:]
        
        # Statistical drift detection
        current_mean = np.mean(current_data)
        current_std = np.std(current_data)
        baseline_mean = np.mean(baseline_data)
        baseline_std = np.std(baseline_data)
        
        # Kolmogorov-Smirnov test for distribution drift
        try:
            _, p_value = stats.ks_2samp(baseline_data, current_data)
        except Exception:
            p_value = 1.0
        
        # Drift score based on standardized difference
        if baseline_std > 0:
            drift_score = abs(current_mean - baseline_mean) / baseline_std
        else:
            drift_score = 0.0
        
        return FeatureDriftMetrics(
            feature_id=feature_vector.feature_id,
            current_mean=float(current_mean),
            current_std=float(current_std),
            baseline_mean=float(baseline_mean),
            baseline_std=float(baseline_std),
            drift_score=float(drift_score),
            p_value=float(p_value),
            last_updated=int(time.time() * 1_000_000),
            sample_size=len(current_data),
            drift_threshold=self.config.max_drift_threshold
        )
    
    def update_drift_status(self, feature_vector: FeatureVector, 
                          drift_metrics: FeatureDriftMetrics) -> DriftStatus:
        """Update drift status based on metrics."""
        if drift_metrics.p_value < self.config.drift_alert_threshold:
            if drift_metrics.drift_score > 2.0:
                return DriftStatus.CRITICAL
            elif drift_metrics.drift_score > 1.0:
                return DriftStatus.WARNING
            else:
                return DriftStatus.STABLE
        else:
            return DriftStatus.STABLE
    
    async def create_feature_batch(self, raw_data: Dict[str, pd.DataFrame],
                                 feature_configs: List[Dict[str, Any]]) -> List[FeatureVector]:
        """Create multiple features in batch for efficiency."""
        feature_vectors = []
        
        # Process features in parallel where possible
        tasks = []
        
        for config in feature_configs:
            feature_type = FeatureType(config.get("type", "returns"))
            
            if feature_type == FeatureType.RETURNS:
                task = self.compute_returns_features(
                    raw_data["price_data"],
                    window_size=config.get("window_size", 20),
                    horizon=config.get("horizon", 1),
                    price_col=config.get("price_col", "close")
                )
            elif feature_type == FeatureType.REALIZED_VOLATILITY:
                task = self.compute_realized_volatility_features(
                    raw_data["price_data"],
                    window_size=config.get("window_size", 252),
                    horizon=config.get("horizon", 1),
                    price_col=config.get("price_col", "close")
                )
            elif feature_type == FeatureType.JUMPS:
                task = self.compute_jump_features(
                    raw_data["price_data"],
                    window_size=config.get("window_size", 100),
                    horizon=config.get("horizon", 1),
                    price_col=config.get("price_col", "close")
                )
            elif feature_type == FeatureType.MICROSTRUCTURE:
                if "orderbook_data" in raw_data and "trades_data" in raw_data:
                    task = self.compute_microstructure_features(
                        raw_data["orderbook_data"],
                        raw_data["trades_data"],
                        window_size=config.get("window_size", 100),
                        horizon=config.get("horizon", 1)
                    )
                else:
                    continue
            elif feature_type == FeatureType.FUNDING_BASIS:
                if "spot_data" in raw_data and "futures_data" in raw_data:
                    task = self.compute_funding_basis_features(
                        raw_data["spot_data"],
                        raw_data["futures_data"],
                        funding_data=raw_data.get("funding_data"),
                        window_size=config.get("window_size", 24),
                        horizon=config.get("horizon", 8)
                    )
                else:
                    continue
            elif feature_type == FeatureType.WALLET_FLOWS:
                if "wallet_data" in raw_data:
                    task = self.compute_wallet_flow_features(
                        raw_data["wallet_data"],
                        window_size=config.get("window_size", 168),
                        horizon=config.get("horizon", 24)
                    )
                else:
                    continue
            elif feature_type == FeatureType.REGIME_SIGNALS:
                if "market_data" in raw_data:
                    task = self.compute_regime_signal_features(
                        raw_data["market_data"],
                        window_size=config.get("window_size", 504),
                        horizon=config.get("horizon", 1),
                        price_col=config.get("price_col", "close"),
                        volume_col=config.get("volume_col", "volume")
                    )
                else:
                    continue
            elif feature_type == FeatureType.INFORMATION_DECAY:
                if "price_data" in raw_data:
                    task = self.compute_information_decay_features(
                        raw_data["price_data"],
                        order_flow_data=raw_data.get("order_flow_data"),
                        window_size=config.get("window_size", 252),
                        horizon=config.get("horizon", 1),
                        price_col=config.get("price_col", "close")
                    )
                else:
                    continue
            else:
                continue
            
            tasks.append(task)
        
        # Execute tasks with limited concurrency
        semaphore = asyncio.Semaphore(self.config.parallel_workers)
        
        async def process_with_semaphore(task):
            async with semaphore:
                try:
                    return await task
                except Exception as e:
                    print(f"⚠️  Feature computation failed: {e}")
                    return None
        
        results = await asyncio.gather(*[process_with_semaphore(task) for task in tasks], return_exceptions=True)
        
        # Filter successful results
        for result in results:
            if isinstance(result, FeatureVector):
                feature_vectors.append(result)
                
                # Store in registry
                self.feature_registry[result.feature_id] = result
                self.provenance_store[result.feature_id] = result.provenance
                
                # Update statistics
                self.feature_stats["total_features_created"] += 1
                
                # Track SLA violations
                if result.quality_score < self.config.min_quality_score:
                    self.feature_stats["quality_failures"] += 1
                    self.feature_stats["sla_violations"] += 1  # Track actual SLA violations
        
        return feature_vectors
    
    async def compute_regime_signal_features(self, market_data: pd.DataFrame,
                                           window_size: int = 504,  # 2 years of daily data
                                           horizon: int = 1,
                                           price_col: str = "close",
                                           volume_col: str = "volume",
                                           timestamp_col: str = "timestamp") -> FeatureVector:
        """Compute regime detection features for hidden alpha discovery."""
        if len(market_data) < window_size + horizon:
            raise ValueError(f"Insufficient data: need {window_size + horizon}, got {len(market_data)}")
        
        prices = np.array(market_data[price_col].values, dtype=float)
        volumes = np.array(market_data[volume_col].values, dtype=float) if volume_col in market_data.columns else np.ones_like(prices)
        timestamps = np.array(market_data[timestamp_col].values)
        
        # Validate temporal integrity with existing Leakage Police agent
        parameters = {'feature_type': 'regime_signal', 'window_size': window_size, 'horizon': horizon}
        leakage_proof_id = self._validate_temporal_integrity_sync(timestamps, window_size, horizon, 
                                                                 prices, parameters)
        
        # Compute log returns and volatility
        log_prices = np.log(prices)
        returns = np.diff(log_prices)
        
        features = []
        feature_timestamps = []
        
        for i in range(window_size, len(returns) - horizon + 1):
            window_returns = returns[i-window_size:i]
            window_volumes = volumes[i-window_size:i]
            
            # Regime detection through multiple lenses
            
            # 1. Volatility regime clustering (Hidden Alpha: Vol Regime Persistence)
            short_vol = np.std(window_returns[-63:])  # 3 months
            long_vol = np.std(window_returns)  # Full window
            vol_regime_signal = (short_vol - long_vol) / long_vol if long_vol > 0 else 0.0
            
            # 2. Return distribution regime (Hidden Alpha: Tail Risk Asymmetry)
            skewness = stats.skew(window_returns)
            kurtosis = stats.kurtosis(window_returns)
            tail_regime = skewness * np.sqrt(abs(kurtosis - 3))  # Tail risk interaction
            
            # 3. Correlation structure regime (Hidden Alpha: Multi-Asset Correlation Decay)
            if len(window_returns) >= 126:  # 6 months minimum
                early_half = window_returns[:len(window_returns)//2]
                late_half = window_returns[len(window_returns)//2:]
                correlation_stability = np.corrcoef(early_half[:len(late_half)], late_half)[0, 1] if len(late_half) > 1 else 0.0
            else:
                correlation_stability = 0.0
            
            # 4. Volume-Price regime (Hidden Alpha: Smart Money vs Retail Flow)
            price_volume_correlation = np.corrcoef(window_returns, np.diff(np.log(window_volumes + 1)))[0, 1] if len(window_volumes) > 1 else 0.0
            
            # 5. Information decay regime (Hidden Alpha: Alpha Half-Life Detection)
            autocorr_1 = np.corrcoef(window_returns[:-1], window_returns[1:])[0, 1] if len(window_returns) > 1 else 0.0
            autocorr_5 = np.corrcoef(window_returns[:-5], window_returns[5:])[0, 1] if len(window_returns) > 5 else 0.0
            information_decay = abs(autocorr_1) - abs(autocorr_5)  # Speed of mean reversion
            
            # 6. Momentum-Reversal regime (Hidden Alpha: Momentum Exhaustion Signals)
            short_momentum = np.mean(window_returns[-21:])  # 1 month
            long_momentum = np.mean(window_returns)  # Full window
            momentum_regime = short_momentum / (long_momentum + 1e-8)  # Relative momentum strength
            
            feature_vector = np.array([
                vol_regime_signal,
                tail_regime,
                correlation_stability,
                price_volume_correlation,
                information_decay,
                momentum_regime,
                short_vol / (long_vol + 1e-8),  # Vol regime ratio
                abs(skewness) + abs(kurtosis - 3),  # Distributional stress
            ])
            
            features.append(feature_vector)
            feature_timestamps.append(timestamps[i])
        
        features_array = np.array(features)
        timestamps_array = np.array(feature_timestamps)
        
        # Create provenance
        parameters = {
            "window_size": window_size,
            "horizon": horizon,
            "regime_types": ["volatility", "distribution", "correlation", "volume_price", "information_decay", "momentum"],
            "alpha_focus": "hidden_regime_persistence"
        }
        
        feature_id = self._generate_feature_id(FeatureType.REGIME_SIGNALS, parameters)
        # Use leakage proof ID from validation above (already generated with existing Leakage Police)
        
        provenance = FeatureProvenance(
            feature_id=feature_id,
            source_datasets=[f"market_data_{price_col}_{volume_col}"],
            transformation_pipeline=["log_transform", "regime_detection", "correlation_analysis", "distribution_analysis"],
            parameters=parameters,
            creation_timestamp=int(time.time() * 1_000_000),
            data_version="1.0",
            algorithm_version="1.0",
            validation_checksum=leakage_proof_id,
            dependency_graph={"market_data": [price_col, volume_col, timestamp_col]}
        )
        
        quality_score = self._compute_quality_score(features_array.flatten(), timestamps_array)
        
        return FeatureVector(
            feature_id=feature_id,
            feature_type=FeatureType.REGIME_SIGNALS,
            values=features_array,
            timestamps=timestamps_array,
            window_size=window_size,
            horizon=horizon,
            units=FeatureUnits.REGIME_PROBABILITY,
            provenance=provenance,
            leakage_proof_id=leakage_proof_id,
            quality_score=quality_score,
            drift_status=DriftStatus.UNKNOWN,
            metadata={
                "feature_names": ["vol_regime_signal", "tail_regime", "correlation_stability", "price_volume_corr", 
                                "information_decay", "momentum_regime", "vol_regime_ratio", "distributional_stress"],
                "computation_time": time.time(),
                "alpha_strategy": "hidden_regime_detection"
            }
        )
    
    async def compute_information_decay_features(self, price_data: pd.DataFrame,
                                               order_flow_data: Optional[pd.DataFrame] = None,
                                               window_size: int = 252,
                                               horizon: int = 1,
                                               price_col: str = "close",
                                               timestamp_col: str = "timestamp") -> FeatureVector:
        """Compute information decay features to capture alpha half-life dynamics."""
        if len(price_data) < window_size + horizon:
            raise ValueError(f"Insufficient data: need {window_size + horizon}, got {len(price_data)}")
        
        prices = np.array(price_data[price_col].values, dtype=float)
        timestamps = np.array(price_data[timestamp_col].values)
        
        # Validate temporal integrity with existing Leakage Police agent
        parameters = {'feature_type': 'information_decay', 'window_size': window_size, 'horizon': horizon}
        leakage_proof_id = self._validate_temporal_integrity_sync(timestamps, window_size, horizon, 
                                                                 prices, parameters)
        
        log_prices = np.log(prices)
        returns = np.diff(log_prices)
        
        features = []
        feature_timestamps = []
        
        for i in range(window_size, len(returns) - horizon + 1):
            window_returns = returns[i-window_size:i]
            
            # Information theory features for alpha discovery
            
            # 1. Return predictability decay (Hidden Alpha: Predictive Signal Half-Life)
            predictability_lags = []
            for lag in [1, 2, 3, 5, 10, 21]:  # Various horizons
                if len(window_returns) > lag:
                    lagged_corr = np.corrcoef(window_returns[:-lag], window_returns[lag:])[0, 1]
                    predictability_lags.append(abs(lagged_corr) if not np.isnan(lagged_corr) else 0.0)
                else:
                    predictability_lags.append(0.0)
            
            # Decay rate (alpha half-life proxy)
            if len(predictability_lags) >= 2:
                decay_rate = -np.log(max(predictability_lags[1] / (predictability_lags[0] + 1e-8), 1e-8))
            else:
                decay_rate = 0.0
            
            # 2. Information content evolution (Hidden Alpha: Signal Strength Decay)
            rolling_entropy = []
            window_size_entropy = 63  # 3 months
            for j in range(window_size_entropy, len(window_returns), 21):  # Monthly steps
                sub_window = window_returns[j-window_size_entropy:j]
                # Discretize returns for entropy calculation
                bins = np.percentile(sub_window, [20, 40, 60, 80])
                digitized = np.digitize(sub_window, bins)
                _, counts = np.unique(digitized, return_counts=True)
                probs = counts / len(digitized)
                entropy = -np.sum(probs * np.log(probs + 1e-8))
                rolling_entropy.append(entropy)
            
            entropy_trend = 0.0
            if len(rolling_entropy) > 1:
                entropy_trend = np.polyfit(range(len(rolling_entropy)), rolling_entropy, 1)[0]
            
            # 3. Market efficiency evolution (Hidden Alpha: Inefficiency Persistence)
            hurst_exponent = self._compute_hurst_exponent(window_returns[-126:]) if len(window_returns) >= 126 else 0.5
            efficiency_score = abs(hurst_exponent - 0.5)  # Deviation from random walk
            
            # 4. Volume-return information coupling (Hidden Alpha: Smart Money Detection)
            if order_flow_data is not None:
                # Use order flow if available for more sophisticated analysis
                flow_info_score = 0.5  # Placeholder for advanced order flow analysis
            else:
                # Use price-based proxy
                recent_vol = np.std(window_returns[-21:])
                historical_vol = np.std(window_returns[:-21]) if len(window_returns) > 21 else recent_vol
                flow_info_score = recent_vol / (historical_vol + 1e-8) - 1.0
            
            # 5. Cross-scale information transfer (Hidden Alpha: Multi-Timeframe Consistency)
            if len(window_returns) >= 252:
                daily_info = np.std(window_returns[-21:])   # Daily scale
                weekly_info = np.std(window_returns[-63:])   # Weekly scale  
                monthly_info = np.std(window_returns[-252:]) # Monthly scale
                scale_consistency = 1.0 - np.std([daily_info, weekly_info, monthly_info]) / (np.mean([daily_info, weekly_info, monthly_info]) + 1e-8)
            else:
                scale_consistency = 0.0
            
            feature_vector = np.array([
                decay_rate,
                predictability_lags[0],  # 1-day predictability
                predictability_lags[2],  # 3-day predictability  
                predictability_lags[5],  # 1-week predictability
                entropy_trend,
                efficiency_score,
                flow_info_score,
                scale_consistency,
                np.mean(predictability_lags),  # Average predictability
                np.std(predictability_lags),   # Predictability stability
            ])
            
            features.append(feature_vector)
            feature_timestamps.append(timestamps[i])
        
        features_array = np.array(features)
        timestamps_array = np.array(feature_timestamps)
        
        # Create provenance
        parameters = {
            "window_size": window_size,
            "horizon": horizon,
            "decay_lags": [1, 2, 3, 5, 10, 21],
            "alpha_focus": "information_half_life"
        }
        
        feature_id = self._generate_feature_id(FeatureType.INFORMATION_DECAY, parameters)
        # Use leakage proof ID from validation above (already generated with existing Leakage Police)
        
        provenance = FeatureProvenance(
            feature_id=feature_id,
            source_datasets=[f"price_data_{price_col}"] + (["order_flow_data"] if order_flow_data is not None else []),
            transformation_pipeline=["log_transform", "autocorrelation_analysis", "entropy_calculation", "hurst_analysis"],
            parameters=parameters,
            creation_timestamp=int(time.time() * 1_000_000),
            data_version="1.0",
            algorithm_version="1.0",
            validation_checksum=leakage_proof_id,
            dependency_graph={"price_data": [price_col, timestamp_col]}
        )
        
        quality_score = self._compute_quality_score(features_array.flatten(), timestamps_array)
        
        return FeatureVector(
            feature_id=feature_id,
            feature_type=FeatureType.INFORMATION_DECAY,
            values=features_array,
            timestamps=timestamps_array,
            window_size=window_size,
            horizon=horizon,
            units=FeatureUnits.ALPHA_DECAY,
            provenance=provenance,
            leakage_proof_id=leakage_proof_id,
            quality_score=quality_score,
            drift_status=DriftStatus.UNKNOWN,
            metadata={
                "feature_names": ["decay_rate", "pred_1d", "pred_3d", "pred_1w", "entropy_trend", 
                                "efficiency_score", "flow_info_score", "scale_consistency", "avg_predictability", "pred_stability"],
                "computation_time": time.time(),
                "alpha_strategy": "information_decay_analysis"
            }
        )
    
    def _compute_hurst_exponent(self, returns: np.ndarray, max_lag: int = 20) -> float:
        """Compute Hurst exponent for market efficiency analysis."""
        if len(returns) < max_lag * 2:
            return 0.5  # Random walk default
        
        # R/S analysis for Hurst exponent
        lags = range(2, min(max_lag, len(returns) // 4))
        rs_values = []
        
        for lag in lags:
            # Split into sub-periods
            n_periods = len(returns) // lag
            if n_periods < 2:
                continue
                
            rs_period = []
            for i in range(n_periods):
                period_returns = returns[i*lag:(i+1)*lag]
                if len(period_returns) < 2:
                    continue
                    
                # Cumulative deviance from mean
                cumdev = np.cumsum(period_returns - np.mean(period_returns))
                R = np.max(cumdev) - np.min(cumdev)  # Range
                S = np.std(period_returns)  # Standard deviation
                
                if S > 0:
                    rs_period.append(R / S)
            
            if rs_period:
                rs_values.append((lag, np.mean(rs_period)))
        
        if len(rs_values) < 3:
            return 0.5
        
        # Linear regression in log space
        lags_log = [np.log(rs[0]) for rs in rs_values]
        rs_log = [np.log(rs[1]) for rs in rs_values if rs[1] > 0]
        
        if len(lags_log) != len(rs_log) or len(rs_log) < 3:
            return 0.5
        
        hurst = np.polyfit(lags_log, rs_log, 1)[0]
        return max(0.0, min(1.0, hurst))  # Clamp to valid range
    
    def validate_feature_sla(self) -> bool:
        """Validate that feature SLA requirements are met."""
        if self.feature_stats["total_features_created"] == 0:
            return True
        
        success_rate = 1.0 - (self.feature_stats["sla_violations"] / self.feature_stats["total_features_created"])
        return success_rate >= self.config.min_feature_sla
    
    def get_feature_lineage(self, feature_id: str) -> Optional[FeatureProvenance]:
        """Retrieve complete lineage for a feature."""
        return self.provenance_store.get(feature_id)
    
    def get_factory_statistics(self) -> Dict[str, Any]:
        """Get comprehensive factory performance statistics."""
        # Prevent zero-division in cache utilization calculation
        if self.config.feature_cache_size <= 0:
            cache_utilization = 0.0
        else:
            cache_utilization = len(self.feature_cache) / self.config.feature_cache_size
        
        return {
            **self.feature_stats,
            "session_id": self.session_id,
            "feature_registry_size": len(self.feature_registry),
            "drift_tracker_size": len(self.drift_tracker),
            "cache_utilization": cache_utilization,
            "sla_compliance": self.validate_feature_sla(),
            "avg_quality_score": np.mean([fv.quality_score for fv in self.feature_registry.values()]) if self.feature_registry else 0.0
        }
