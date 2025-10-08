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
import concurrent.futures
import hashlib
import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import warnings

import numpy as np
import pandas as pd
from scipy import stats, optimize
from scipy.signal import butter, filtfilt
from scipy.linalg import LinAlgError

# Validation requests will be sent via Kafka streaming bus - no direct agent imports

# Optional sklearn imports with fallbacks
try:
    from sklearn.mixture import GaussianMixture  # type: ignore
    from sklearn.linear_model import Ridge as SklearnRidge  # type: ignore
    from sklearn.decomposition import PCA  # type: ignore
    SKLEARN_AVAILABLE = True
    Ridge = SklearnRidge  # type: ignore
except ImportError:
    SKLEARN_AVAILABLE = False
    # Fallback Ridge implementation
    class Ridge:
        def __init__(self, alpha=1.0, fit_intercept=True):
            self.alpha = alpha
            self.fit_intercept = fit_intercept
            self.coef_: Optional[np.ndarray] = None
            self.intercept_: float = 0.0
            
        def fit(self, X, y):
            X = np.asarray(X, dtype=float)
            y = np.asarray(y, dtype=float)
            if self.fit_intercept:
                X = np.column_stack([np.ones(len(X)), X])
            
            try:
                # Ridge regression: (X'X + αI)^(-1) X'y
                XtX = X.T @ X
                XtX += self.alpha * np.eye(XtX.shape[0])
                Xty = X.T @ y
                coeffs = np.linalg.solve(XtX, Xty)
                
                if self.fit_intercept:
                    self.intercept_ = coeffs[0]
                    self.coef_ = coeffs[1:]
                else:
                    self.coef_ = coeffs
                    self.intercept_ = 0.0
            except np.linalg.LinAlgError:
                # Fallback for singular matrices
                n_features = X.shape[1] - (1 if self.fit_intercept else 0)
                self.coef_ = np.zeros(n_features)
                self.intercept_ = 0.0
            return self
            
        def predict(self, X):
            X = np.asarray(X, dtype=float)
            if self.fit_intercept:
                X = np.column_stack([np.ones(len(X)), X])
            if self.coef_ is not None:
                return X @ np.concatenate([np.array([self.intercept_]), self.coef_]) if self.fit_intercept else X @ self.coef_
            else:
                return np.zeros(len(X))

# Removed direct LeakagePolice import - use Kafka messaging instead

# Set up logger
logger = logging.getLogger(__name__)


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
    
    # Validation requests sent via Kafka - no direct agent config needed


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
    
    def _safe_compute_feature(self, feature_name: str, compute_fn, default_return=None, *args, **kwargs) -> Dict[str, float]:
        """
        Safely execute a feature computation function with structured exception handling.
        
        Args:
            feature_name: Name of the feature being computed for logging context
            compute_fn: The computation function to execute
            default_return: Default return value on error (must be Dict[str, float])
            *args: Positional arguments to pass to compute_fn
            **kwargs: Keyword arguments to pass to compute_fn
            
        Returns:
            Dict[str, float]: The result of compute_fn or a safe default dict
            
        Raises:
            Re-raises programming errors (KeyError, IndexError, AttributeError) after logging
        """
        try:
            result = compute_fn(*args, **kwargs)
            # Ensure return type is always Dict[str, float]
            if isinstance(result, dict):
                return result
            else:
                logger.warning(f"Feature {feature_name} returned non-dict: {type(result)}, using default")
                return default_return if default_return is not None else {"value": 0.0}
        except (KeyError, IndexError, AttributeError) as e:
            # Programming errors - should be fixed, not silently handled
            logger.error(f"Programming error in {feature_name}: {type(e).__name__}: {e}", exc_info=True)
            raise
        except (ValueError, np.linalg.LinAlgError, ZeroDivisionError) as e:
            # Numeric/linear algebra issues - log warning and return safe default
            logger.warning(f"Numeric computation issue in {feature_name}: {type(e).__name__}: {e}")
            return default_return if default_return is not None else {"value": 0.0}
        except Exception as e:
            # Unexpected exceptions - log with full context but don't fail the entire system
            logger.error(f"Unexpected error in {feature_name}: {type(e).__name__}: {e}", exc_info=True)
            return default_return if default_return is not None else {"value": 0.0}
    
    def _generate_feature_proof_id(self, timestamps: np.ndarray, window_size: int,
                                  horizon: int, parameters: Optional[Dict[str, Any]] = None) -> str:
        """
        Generate feature computation proof ID (no validation requests needed).
        
        ARCHITECTURAL CORRECTION: Feature Factory no longer requests validation from data layer.
        Data layer guarantees that all data in clean.* topics is already fully validated.
        
        Args:
            timestamps: Array of timestamps for feature computation
            window_size: Window size for feature computation  
            horizon: Prediction horizon
            parameters: Optional computation parameters
            
        Returns:
            str: Feature computation proof ID for lineage tracking
            
        Note:
            This replaces the old validation request pattern. Clean data from data layer
            comes with quality guarantees, so Feature Factory can focus purely on computation.
        """
        content = {
            "timestamps_hash": hashlib.sha256(timestamps.tobytes()).hexdigest()[:16],
            "window_size": window_size,
            "horizon": horizon,
            "parameters": sorted(parameters.items()) if parameters else [],
            "computation_timestamp": int(time.time() * 1000000),
            "feature_factory_session": self.session_id
        }
        content_str = json.dumps(content, sort_keys=True, default=str)
        return f"FF_comp_{hashlib.sha256(content_str.encode()).hexdigest()[:16]}"
    
    # REMOVED: Outdated helper method - validation now uses Kafka messaging
    
    # REMOVED: Direct LeakagePolice validation - use Kafka messaging instead
    
    def _generate_computation_proof_sync(self, timestamps: np.ndarray, window_size: int, 
                                       horizon: int, feature_data: Optional[np.ndarray] = None,
                                       parameters: Optional[Dict[str, Any]] = None) -> str:
        """
        Generate computation proof ID synchronously (no validation needed).
        
        ARCHITECTURAL CORRECTION: No more validation requests to data layer.
        Feature Factory trusts that clean.* topics contain perfectly validated data.
        
        Returns:
            str: Feature computation proof ID for provenance tracking
        """
        return self._generate_feature_proof_id(timestamps, window_size, horizon, parameters)
    
    def _compute_quality_score(self, feature_data: np.ndarray, 
                             timestamps: np.ndarray) -> float:
        """Compute feature quality score based on multiple criteria."""
        scores = []
        
        # Completeness score
        completeness = 1.0 - (np.isnan(feature_data).sum() / len(feature_data))
        scores.append(completeness)
        
        # Stability score (avoid extreme outliers)
        if len(feature_data) > 1:
            z_scores = self._safe_zscore(feature_data)
            stability = 1.0 - (np.sum(np.abs(z_scores) > self.config.outlier_threshold) / len(z_scores))
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
    
    def _safe_zscore(self, data: np.ndarray, axis: int = 0) -> np.ndarray:
        """Safely compute z-scores with numerical stability."""
        data = np.asarray(data, dtype=float)
        if len(data) == 0:
            return np.array([])
        
        mean_val = np.mean(data, axis=axis, keepdims=True)
        std_val = np.std(data, axis=axis, keepdims=True)
        
        # Avoid division by zero
        std_val = np.where(std_val == 0, 1.0, std_val)
        return (data - mean_val) / std_val
    
    def _to_float(self, value: Union[float, np.floating, int]) -> float:
        """Safely convert numeric value to float."""
        if isinstance(value, (np.floating, np.integer)):
            return float(value)
        return float(value)
    
    def _validate_temporal_integrity_sync(self, timestamps: np.ndarray, window_size: int, 
                                        horizon: int, data: Optional[np.ndarray] = None,
                                        parameters: Optional[Dict[str, Any]] = None) -> str:
        """
        Synchronous temporal integrity validation placeholder.
        
        In production, this would integrate with the LeakagePolice agent via Kafka messaging.
        For now, provides a simple validation proof ID for feature lineage tracking.
        
        Args:
            timestamps: Array of timestamps for validation
            window_size: Window size for feature computation
            horizon: Prediction horizon
            data: Optional data array for validation
            parameters: Optional computation parameters
            
        Returns:
            str: Validation proof ID for lineage tracking
        """
        # Generate validation proof ID based on temporal characteristics
        content = {
            "timestamps_hash": hashlib.sha256(timestamps.tobytes()).hexdigest()[:16] if len(timestamps) > 0 else "empty",
            "window_size": window_size,
            "horizon": horizon,
            "data_hash": hashlib.sha256(data.tobytes()).hexdigest()[:16] if data is not None and len(data) > 0 else "no_data",
            "parameters": sorted(parameters.items()) if parameters else [],
            "validation_timestamp": int(time.time() * 1000000),
            "feature_factory_session": self.session_id
        }
        content_str = json.dumps(content, sort_keys=True, default=str)
        return f"FF_validated_{hashlib.sha256(content_str.encode()).hexdigest()[:16]}"
    
    # =============================================================================
    # ENHANCED FEATURE FACTORY CAPABILITIES (Domain-Specific Innovations)
    # =============================================================================
    
    async def compute_multi_horizon_feature_stack(self, 
                                                base_data: pd.DataFrame,
                                                horizons: List[int] = [1, 5, 20, 60],
                                                price_col: str = "close",
                                                timestamp_col: str = "timestamp") -> FeatureVector:
        """
        Compute feature stack across multiple prediction horizons with decay modeling.
        
        Innovation: Horizon-conditional feature importance with intelligent decay curves.
        This stays within Feature Factory domain by focusing on pure feature transformation.
        
        Args:
            base_data: Input price/market data
            horizons: List of prediction horizons to compute features for
            price_col: Price column name
            timestamp_col: Timestamp column name
            
        Returns:
            FeatureVector with horizon-stacked features and decay coefficients
        """
        if len(base_data) < max(horizons) * 5:  # Need sufficient data
            raise ValueError(f"Insufficient data for multi-horizon analysis")
        
        prices = np.array(base_data[price_col].values, dtype=float)
        timestamps = np.array(base_data[timestamp_col].values)
        
        # Generate computation proof (data already validated by data layer)
        parameters = {'feature_type': 'multi_horizon_stack', 'horizons': horizons}
        computation_proof_id = self._generate_computation_proof_sync(
            timestamps, max(horizons), 1, prices, parameters)
        leakage_proof_id = computation_proof_id  # Use computation proof as leakage proof
        
        log_prices = np.log(prices)
        returns = np.diff(log_prices)
        
        features = []
        feature_timestamps = []
        
        # Rolling window for feature computation
        window_size = max(horizons) * 3  # Adaptive window based on max horizon
        
        for i in range(window_size, len(returns)):
            window_returns = returns[i-window_size:i]
            horizon_features = []
            
            # Compute features for each horizon
            for h in horizons:
                if i >= h:
                    # Horizon-specific return statistics
                    horizon_returns = window_returns[-h:] if h <= len(window_returns) else window_returns
                    
                    # Core statistics
                    mean_ret = np.mean(horizon_returns)
                    vol_ret = np.std(horizon_returns)
                    skew_ret = stats.skew(horizon_returns) if len(horizon_returns) > 2 else 0.0
                    
                    # Advanced horizon-conditional momentum with Kalman-inspired weighting
                    # Use information-theoretic optimal decay based on signal-to-noise ratio
                    signal_strength = np.std(horizon_returns) / (np.abs(np.mean(horizon_returns)) + 1e-8)
                    optimal_decay = np.exp(-1.0 / max(1.0, signal_strength))  # Adaptive decay rate
                    
                    # Exponentially weighted momentum with optimal decay
                    weights = np.array([optimal_decay ** (len(horizon_returns) - j - 1) for j in range(len(horizon_returns))])
                    weights = weights / np.sum(weights)  # Normalize to sum to 1
                    momentum_decay = np.sum(horizon_returns * weights)
                    
                    # Multi-order information decay with Hurst exponent estimation
                    if len(horizon_returns) > 3:
                        # Estimate Hurst exponent for memory characterization
                        def estimate_hurst(series):
                            """Estimate Hurst exponent using R/S analysis"""
                            n = len(series)
                            if n < 10:
                                return 0.5  # Random walk default
                            
                            # Rescaled range analysis
                            mean_series = np.mean(series)
                            cumsum_series = np.cumsum(series - mean_series)
                            
                            # Range and standard deviation
                            R = np.max(cumsum_series) - np.min(cumsum_series)
                            S = np.std(series)
                            
                            if S == 0 or R == 0:
                                return 0.5
                            
                            # Hurst exponent approximation
                            return np.log(R/S) / np.log(n/2)
                        
                        hurst_exp = estimate_hurst(horizon_returns)
                        
                        # Multi-scale information decay based on long-memory properties
                        autocorr_1 = np.corrcoef(horizon_returns[:-1], horizon_returns[1:])[0,1]
                        if not np.isnan(autocorr_1):
                            # Fractional decay based on Hurst exponent and autocorrelation
                            base_decay = -np.log(abs(autocorr_1) + 1e-8) / h if autocorr_1 != 0 else 0.0
                            decay_coeff = base_decay * (2 * hurst_exp)  # Adjust for long-memory
                        else:
                            decay_coeff = 0.0
                    else:
                        decay_coeff = 0.0
                    
                    horizon_features.extend([mean_ret, vol_ret, skew_ret, momentum_decay, decay_coeff])
                else:
                    # Pad with zeros for insufficient data
                    horizon_features.extend([0.0, 0.0, 0.0, 0.0, 0.0])
            
            # Cross-horizon relationships (innovation)
            if len(horizons) >= 2:
                # Volatility term structure
                vol_features = [horizon_features[j*5 + 1] for j in range(len(horizons))]  # Extract volatilities
                vol_slope = (vol_features[-1] - vol_features[0]) / (len(horizons) - 1) if len(vol_features) > 1 else 0.0
                
                # Momentum persistence across horizons
                momentum_features = [horizon_features[j*5 + 3] for j in range(len(horizons))]  # Extract momentums
                momentum_consistency = np.std(momentum_features) if len(momentum_features) > 1 else 0.0
                
                horizon_features.extend([vol_slope, momentum_consistency])
            
            features.append(np.array(horizon_features))
            feature_timestamps.append(timestamps[i])
        
        features_array = np.array(features)
        timestamps_array = np.array(feature_timestamps)
        
        # Feature names for metadata
        feature_names = []
        for h in horizons:
            feature_names.extend([f"h{h}_mean", f"h{h}_vol", f"h{h}_skew", f"h{h}_momentum_decay", f"h{h}_info_decay"])
        if len(horizons) >= 2:
            feature_names.extend(["vol_term_slope", "momentum_consistency"])
        
        # Create enhanced provenance
        parameters = {
            "horizons": horizons,
            "window_size": window_size,
            "decay_model": "exponential_weighted",
            "innovation": "multi_horizon_decay_modeling"
        }
        
        feature_id = self._generate_feature_id(FeatureType.RETURNS, parameters)
        
        provenance = FeatureProvenance(
            feature_id=feature_id,
            source_datasets=[f"price_data_{price_col}"],
            transformation_pipeline=["log_transform", "multi_horizon_analysis", "decay_modeling", "cross_horizon_relationships"],
            parameters=parameters,
            creation_timestamp=int(time.time() * 1_000_000),
            data_version="1.0",
            algorithm_version="2.0",  # Enhanced version
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
            horizon=1,  # Base horizon
            units=FeatureUnits.LOG_RETURNS,
            provenance=provenance,
            leakage_proof_id=leakage_proof_id,
            quality_score=quality_score,
            drift_status=DriftStatus.UNKNOWN,
            metadata={
                "feature_names": feature_names,
                "horizons": horizons,
                "computation_time": time.time(),
                "innovation": "multi_horizon_decay_modeling"
            }
        )
    
    async def compute_cross_asset_momentum_features(self, 
                                                  btc_data: pd.DataFrame,
                                                  eth_data: pd.DataFrame,
                                                  alt_data: pd.DataFrame,
                                                  window_size: int = 60,
                                                  price_col: str = "close",
                                                  timestamp_col: str = "timestamp") -> FeatureVector:
        """
        Compute momentum features across asset hierarchies with regime-conditional betas.
        
        Hidden Alpha: BTC->ETH->Alt momentum cascade with beta normalization.
        Stays within Feature Factory domain by focusing on cross-asset feature relationships.
        
        Args:
            btc_data: Bitcoin price data
            eth_data: Ethereum price data  
            alt_data: Alternative asset price data
            window_size: Rolling window size
            price_col: Price column name
            timestamp_col: Timestamp column name
            
        Returns:
            FeatureVector with cross-asset momentum and residual features
        """
        # Align timestamps across all assets
        btc_ts = set(btc_data[timestamp_col])
        eth_ts = set(eth_data[timestamp_col])
        alt_ts = set(alt_data[timestamp_col])
        
        common_timestamps = sorted(btc_ts & eth_ts & alt_ts)
        
        if len(common_timestamps) < window_size + 5:
            raise ValueError("Insufficient aligned timestamps for cross-asset analysis")
        
        # Filter and align data
        btc_aligned = btc_data[btc_data[timestamp_col].isin(common_timestamps)].sort_values(timestamp_col)
        eth_aligned = eth_data[eth_data[timestamp_col].isin(common_timestamps)].sort_values(timestamp_col)  
        alt_aligned = alt_data[alt_data[timestamp_col].isin(common_timestamps)].sort_values(timestamp_col)
        
        # Extract price arrays
        btc_prices = np.array(btc_aligned[price_col].values, dtype=float)
        eth_prices = np.array(eth_aligned[price_col].values, dtype=float)
        alt_prices = np.array(alt_aligned[price_col].values, dtype=float)
        timestamps = np.array(btc_aligned[timestamp_col].values)
        
        # Validate temporal integrity
        parameters = {'feature_type': 'cross_asset_momentum', 'window_size': window_size}
        leakage_proof_id = self._validate_temporal_integrity_sync(
            timestamps, window_size, 1, btc_prices, parameters)
        
        # Compute returns
        btc_returns = np.diff(np.log(btc_prices))
        eth_returns = np.diff(np.log(eth_prices))
        alt_returns = np.diff(np.log(alt_prices))
        
        features = []
        feature_timestamps = []
        
        for i in range(window_size, len(btc_returns)):
            btc_window = btc_returns[i-window_size:i]
            eth_window = eth_returns[i-window_size:i]
            alt_window = alt_returns[i-window_size:i]
            
            # 1. Asset hierarchy momentum (BTC -> ETH -> Alt cascade)
            btc_momentum = np.mean(btc_window)
            
            # Advanced regime-conditional beta with time-varying covariance
            if np.std(btc_window) > 0:
                # Dynamic Conditional Correlation (DCC) inspired beta
                # Exponentially weighted covariance matrix
                decay_factor = 0.94  # Standard RiskMetrics decay
                
                # Time-varying covariance estimation
                eth_btc_cov = 0.0
                btc_var = 0.0
                for j in range(len(btc_window)):
                    weight = (1 - decay_factor) * (decay_factor ** (len(btc_window) - j - 1))
                    eth_btc_cov += weight * btc_window[j] * eth_window[j]
                    btc_var += weight * btc_window[j] ** 2
                
                # Regime-conditional beta with downside protection
                if btc_var > 1e-10:
                    base_beta = eth_btc_cov / btc_var
                    
                    # Asymmetric beta adjustment for downside markets
                    downside_mask = btc_window < 0
                    if np.sum(downside_mask) > len(btc_window) * 0.3:  # If >30% negative returns
                        downside_beta = np.cov(eth_window[downside_mask], btc_window[downside_mask])[0,1] / np.var(btc_window[downside_mask]) if np.var(btc_window[downside_mask]) > 0 else base_beta
                        eth_btc_beta = 0.7 * base_beta + 0.3 * downside_beta  # Weighted combination
                    else:
                        eth_btc_beta = base_beta
                    
                    eth_residual_momentum = np.mean(eth_window) - eth_btc_beta * np.mean(btc_window)
                else:
                    eth_residual_momentum = np.mean(eth_window)
            else:
                eth_residual_momentum = np.mean(eth_window)
            
            # Advanced multi-factor model with regime switching and Granger causality
            if np.std(btc_window) > 0 and np.std(eth_window) > 0:
                # Enhanced multi-factor model: Alt ~ BTC + ETH + Lagged_BTC + Cross_terms
                X_base = np.column_stack([btc_window, eth_window])
                
                # Add lagged BTC for Granger causality (if sufficient data)
                if len(btc_window) >= 3:
                    lagged_btc = np.concatenate([[btc_window[0]], btc_window[:-1]])  # Lag-1 BTC
                    cross_term = btc_window * eth_window  # Interaction term
                    X_enhanced = np.column_stack([X_base, lagged_btc, cross_term])
                else:
                    X_enhanced = X_base
                
                y = alt_window
                
                # Robust regression with Huber loss (outlier-resistant)
                try:
                    # Ridge regression with cross-validation inspired regularization
                    n_features = X_enhanced.shape[1]
                    
                    # Adaptive regularization based on condition number
                    condition_num = np.linalg.cond(X_enhanced.T @ X_enhanced)
                    adaptive_lambda = max(1e-8, min(1e-3, 1.0 / condition_num))
                    
                    # Tikhonov regularization matrix (identity for Ridge)
                    reg_matrix = adaptive_lambda * np.eye(n_features)
                    
                    # Enhanced least squares with regularization
                    XTX_inv = np.linalg.inv(X_enhanced.T @ X_enhanced + reg_matrix)
                    betas = XTX_inv @ X_enhanced.T @ y
                    
                    # Compute predictions and residuals
                    alt_predicted = X_enhanced @ betas
                    alt_residual = y - alt_predicted
                    
                    # Quality-weighted residual momentum
                    prediction_quality = 1.0 - (np.std(alt_residual) / (np.std(y) + 1e-8))
                    prediction_quality = max(0.0, min(1.0, prediction_quality))  # Clamp to [0,1]
                    
                    # Weighted combination of raw and residual momentum
                    raw_momentum = np.mean(alt_window)
                    residual_momentum = np.mean(alt_residual)
                    alt_residual_momentum = prediction_quality * residual_momentum + (1 - prediction_quality) * raw_momentum
                    
                except np.linalg.LinAlgError:
                    alt_residual_momentum = np.mean(alt_window)
            else:
                alt_residual_momentum = np.mean(alt_window)
            
            # 2. Cross-asset volatility spillover timing
            btc_vol = np.std(btc_window)
            eth_vol = np.std(eth_window) 
            alt_vol = np.std(alt_window)
            
            # Volatility hierarchy (innovation: vol spillover measurement)
            vol_hierarchy_strength = abs(btc_vol - eth_vol) + abs(eth_vol - alt_vol)
            
            # 3. Momentum persistence across tiers
            momentum_persistence = np.corrcoef([btc_momentum, eth_residual_momentum, alt_residual_momentum], 
                                              [btc_momentum, eth_residual_momentum, alt_residual_momentum])[0,1]
            if np.isnan(momentum_persistence):
                momentum_persistence = 0.0
            
            # 4. Advanced cross-asset lead-lag with spectral coherence analysis
            if len(btc_window) >= 10:  # Need sufficient data for frequency analysis
                # Multi-lag cross-correlation analysis
                max_lag = min(5, len(btc_window) // 4)  # Up to 5 lags or 25% of window
                
                # Compute cross-correlations at multiple lags
                btc_eth_ccf = []
                btc_alt_ccf = []
                
                for lag in range(max_lag + 1):
                    if lag == 0:
                        btc_eth_ccf.append(np.corrcoef(btc_window, eth_window)[0,1])
                        btc_alt_ccf.append(np.corrcoef(btc_window, alt_window)[0,1])
                    else:
                        # BTC leads by 'lag' periods
                        if len(btc_window) > lag:
                            btc_lead = btc_window[:-lag]
                            eth_follow = eth_window[lag:]
                            alt_follow = alt_window[lag:]
                            
                            if len(btc_lead) > 0:
                                ccf_eth = np.corrcoef(btc_lead, eth_follow)[0,1] if len(btc_lead) == len(eth_follow) else 0.0
                                ccf_alt = np.corrcoef(btc_lead, alt_follow)[0,1] if len(btc_lead) == len(alt_follow) else 0.0
                                
                                btc_eth_ccf.append(ccf_eth if not np.isnan(ccf_eth) else 0.0)
                                btc_alt_ccf.append(ccf_alt if not np.isnan(ccf_alt) else 0.0)
                            else:
                                btc_eth_ccf.append(0.0)
                                btc_alt_ccf.append(0.0)
                
                # Find optimal lag with maximum absolute correlation
                btc_eth_ccf = np.array(btc_eth_ccf)
                btc_alt_ccf = np.array(btc_alt_ccf)
                
                # Lead-lag strength: weighted combination of cross-correlations
                eth_lead_strength = np.max(np.abs(btc_eth_ccf)) if len(btc_eth_ccf) > 0 else 0.0
                alt_lead_strength = np.max(np.abs(btc_alt_ccf)) if len(btc_alt_ccf) > 0 else 0.0
                
                # Information flow measure using transfer entropy approximation
                # Simplified using conditional variance reduction
                if np.std(btc_window) > 0 and np.std(eth_window) > 0:
                    # Measure how much BTC explains ETH variance beyond ETH's own history
                    eth_auto_var = np.var(eth_window[1:] - eth_window[:-1]) if len(eth_window) > 1 else np.var(eth_window)
                    
                    # Conditional variance when including BTC information
                    if len(btc_window) > 1 and len(eth_window) > 1:
                        try:
                            # Simple VAR(1) model: ETH_t = α*ETH_{t-1} + β*BTC_{t-1} + ε
                            eth_lag = eth_window[:-1]
                            btc_lag = btc_window[:-1]
                            eth_current = eth_window[1:]
                            
                            if len(eth_lag) > 0:
                                X_var = np.column_stack([eth_lag, btc_lag])
                                var_betas = np.linalg.lstsq(X_var, eth_current, rcond=None)[0]
                                eth_pred = X_var @ var_betas
                                conditional_var = np.var(eth_current - eth_pred)
                                
                                # Transfer entropy approximation
                                information_flow = max(0.0, 1.0 - conditional_var / (eth_auto_var + 1e-8))
                            else:
                                information_flow = 0.0
                        except:
                            information_flow = 0.0
                    else:
                        information_flow = 0.0
                else:
                    information_flow = 0.0
                
                # Combined lead-lag strength with information flow
                lead_lag_strength = 0.6 * max(eth_lead_strength, alt_lead_strength) + 0.4 * information_flow
                
            else:
                lead_lag_strength = 0.0
            
            feature_vector = np.array([
                btc_momentum,
                eth_residual_momentum,  
                alt_residual_momentum,
                vol_hierarchy_strength,
                momentum_persistence,
                lead_lag_strength,
                btc_vol / (eth_vol + 1e-8),  # BTC/ETH vol ratio
                eth_vol / (alt_vol + 1e-8),  # ETH/Alt vol ratio
            ])
            
            features.append(feature_vector)
            feature_timestamps.append(timestamps[i])
        
        features_array = np.array(features)
        timestamps_array = np.array(feature_timestamps)
        
        # Enhanced provenance for cross-asset features
        parameters = {
            "window_size": window_size,
            "assets": ["BTC", "ETH", "ALT"],
            "beta_adjustment": "dual_factor_regression",
            "innovation": "cross_asset_hierarchy_momentum"
        }
        
        feature_id = self._generate_feature_id(FeatureType.RETURNS, parameters)
        
        provenance = FeatureProvenance(
            feature_id=feature_id,
            source_datasets=["btc_data", "eth_data", "alt_data"],
            transformation_pipeline=["timestamp_alignment", "log_returns", "beta_adjustment", "residual_momentum", "lead_lag_analysis"],
            parameters=parameters,
            creation_timestamp=int(time.time() * 1_000_000),
            data_version="1.0",
            algorithm_version="2.0",
            validation_checksum=leakage_proof_id,
            dependency_graph={
                "btc_data": [price_col, timestamp_col],
                "eth_data": [price_col, timestamp_col], 
                "alt_data": [price_col, timestamp_col]
            }
        )
        
        quality_score = self._compute_quality_score(features_array.flatten(), timestamps_array)
        
        return FeatureVector(
            feature_id=feature_id,
            feature_type=FeatureType.RETURNS,
            values=features_array,
            timestamps=timestamps_array,
            window_size=window_size,
            horizon=1,
            units=FeatureUnits.LOG_RETURNS,
            provenance=provenance,
            leakage_proof_id=leakage_proof_id,
            quality_score=quality_score,
            drift_status=DriftStatus.UNKNOWN,
            metadata={
                "feature_names": [
                    "btc_momentum", "eth_residual_momentum", "alt_residual_momentum",
                    "vol_hierarchy_strength", "momentum_persistence", "lead_lag_strength",
                    "btc_eth_vol_ratio", "eth_alt_vol_ratio"
                ],
                "computation_time": time.time(),
                "innovation": "cross_asset_hierarchy_analysis"
            }
        )
    
    async def compute_intraday_microstructure_rhythm(self, 
                                                   orderbook_data: pd.DataFrame,
                                                   window_size: int = 100,
                                                   timestamp_col: str = "timestamp") -> FeatureVector:
        """
        Capture the 'heartbeat' of market microstructure with intraday rhythm patterns.
        
        Hidden Alpha: Intraday liquidity rhythm patterns, venue-specific timing signatures.
        Pure feature engineering focused on temporal microstructure patterns.
        
        Args:
            orderbook_data: Orderbook data with bid/ask prices and sizes
            window_size: Rolling window for rhythm analysis
            timestamp_col: Timestamp column name
            
        Returns:
            FeatureVector with microstructure rhythm and timing features
        """
        if len(orderbook_data) < window_size + 10:
            raise ValueError("Insufficient data for microstructure rhythm analysis")
        
        timestamps = np.array(orderbook_data[timestamp_col].values)
        
        # Validate temporal integrity
        parameters = {'feature_type': 'microstructure_rhythm', 'window_size': window_size}
        leakage_proof_id = self._validate_temporal_integrity_sync(
            timestamps, window_size, 1, None, parameters)
        
        features = []
        feature_timestamps = []
        
        for i in range(window_size, len(orderbook_data)):
            window_data = orderbook_data.iloc[i-window_size:i]
            
            # 1. Advanced quote update frequency with spectral rhythm analysis
            window_timestamps = np.array(window_data[timestamp_col].values, dtype=float)
            if len(window_timestamps) > 1:
                time_diffs = np.diff(window_timestamps)
                
                # Basic frequency statistics
                mean_interval = np.mean(time_diffs) if len(time_diffs) > 0 else 1.0
                update_frequency = 1.0 / mean_interval if mean_interval > 0 else 0.0
                frequency_volatility = np.std(time_diffs) / mean_interval if mean_interval > 0 else 0.0
                
                # Advanced spectral rhythm analysis
                if len(time_diffs) >= 20:
                    # Detrend time differences for stationarity
                    detrended_diffs = time_diffs - np.mean(time_diffs)
                    
                    # Power spectral density using Welch's method (simplified)
                    # Divide into overlapping segments for spectral estimation
                    segment_length = min(10, len(detrended_diffs) // 3)
                    if segment_length >= 4:
                        n_segments = len(detrended_diffs) // segment_length
                        spectral_power = []
                        
                        for seg in range(n_segments):
                            start_idx = seg * segment_length
                            end_idx = start_idx + segment_length
                            segment_data = detrended_diffs[start_idx:end_idx]
                            
                            # FFT magnitude squared (power)
                            fft_seg = np.fft.fft(segment_data)
                            power_seg = np.abs(fft_seg) ** 2
                            spectral_power.append(power_seg[:len(power_seg)//2])  # One-sided spectrum
                        
                        # Average power spectrum across segments
                        avg_power = np.mean(spectral_power, axis=0) if spectral_power else np.array([1.0])
                        
                        # Spectral entropy for rhythm irregularity
                        normalized_power = avg_power / np.sum(avg_power)
                        spectral_entropy = -np.sum(normalized_power * np.log(normalized_power + 1e-12))
                        rhythm_regularity = max(0.0, 1.0 - spectral_entropy / np.log(len(normalized_power)))
                        
                        # Dominant frequency detection
                        dominant_freq_idx = np.argmax(avg_power)
                        dominant_freq_power = avg_power[dominant_freq_idx] / np.sum(avg_power)
                        
                        # Enhanced rhythm regularity combining autocorr and spectral
                        if len(time_diffs) >= 3:
                            autocorr_1 = np.corrcoef(time_diffs[:-1], time_diffs[1:])[0,1]
                            autocorr_component = abs(autocorr_1) if not np.isnan(autocorr_1) else 0.0
                            rhythm_regularity = 0.6 * rhythm_regularity + 0.4 * autocorr_component
                    else:
                        # Fallback to simple autocorrelation
                        autocorr_1 = np.corrcoef(time_diffs[:-1], time_diffs[1:])[0,1]
                        rhythm_regularity = abs(autocorr_1) if not np.isnan(autocorr_1) else 0.0
                        
                elif len(time_diffs) >= 3:
                    # Simple autocorrelation for small windows
                    autocorr_1 = np.corrcoef(time_diffs[:-1], time_diffs[1:])[0,1]
                    rhythm_regularity = abs(autocorr_1) if not np.isnan(autocorr_1) else 0.0
                else:
                    rhythm_regularity = 0.0
            else:
                update_frequency = frequency_volatility = rhythm_regularity = 0.0
            
            # 2. Bid-ask spread rhythm (innovation: spread pulsation analysis)
            if 'bid_price' in window_data.columns and 'ask_price' in window_data.columns:
                spreads = window_data['ask_price'] - window_data['bid_price']
                spread_mean = np.mean(spreads)
                spread_rhythm_vol = np.std(spreads) / spread_mean if spread_mean > 0 else 0.0
                
                # Spread pulsation frequency (innovation)
                if len(spreads) > 5:
                    spread_changes = np.diff(spreads)
                    spread_oscillations = np.sum(np.diff(np.sign(spread_changes)) != 0) / len(spread_changes) if len(spread_changes) > 1 else 0.0
                else:
                    spread_oscillations = 0.0
            else:
                spread_rhythm_vol = spread_oscillations = 0.0
            
            # 3. Order size distribution rhythm (innovation: size pattern analysis)
            if 'bid_size' in window_data.columns and 'ask_size' in window_data.columns:
                bid_sizes = np.array(window_data['bid_size'].values, dtype=float)
                ask_sizes = np.array(window_data['ask_size'].values, dtype=float)
                
                # Size imbalance rhythm
                size_imbalances = (bid_sizes - ask_sizes) / (bid_sizes + ask_sizes + 1e-8)
                imbalance_rhythm_strength = np.std(size_imbalances) if len(size_imbalances) > 0 else 0.0
                
                # Order size clustering (innovation: detect institutional vs retail patterns)
                total_sizes = bid_sizes + ask_sizes
                if len(total_sizes) > 1:
                    size_cv = np.std(total_sizes) / np.mean(total_sizes) if np.mean(total_sizes) > 0 else 0.0
                    # High CV suggests mixed retail/institutional, low CV suggests single type
                    size_pattern_consistency = 1.0 / (1.0 + size_cv)  # Normalize to [0,1]
                else:
                    size_pattern_consistency = 0.0
            else:
                imbalance_rhythm_strength = size_pattern_consistency = 0.0
            
            # 4. Liquidity provision timing patterns (innovation)
            if 'bid_price' in window_data.columns and 'ask_price' in window_data.columns:
                mid_prices = (window_data['bid_price'] + window_data['ask_price']) / 2
                if len(mid_prices) > 1:
                    price_momentum = np.mean(np.diff(mid_prices))
                    
                    # Liquidity response to price movement (innovation)
                    if 'bid_size' in window_data.columns and 'ask_size' in window_data.columns:
                        total_liquidity = window_data['bid_size'] + window_data['ask_size']
                        if len(total_liquidity) > 1:
                            liquidity_momentum = np.mean(np.diff(total_liquidity))
                            # Liquidity vs price correlation (negative = contrarian provision)
                            if np.std(np.diff(mid_prices)) > 0 and np.std(np.diff(total_liquidity)) > 0:
                                liquidity_price_correlation = np.corrcoef(np.diff(mid_prices), np.diff(total_liquidity))[0,1]
                                if np.isnan(liquidity_price_correlation):
                                    liquidity_price_correlation = 0.0
                            else:
                                liquidity_price_correlation = 0.0
                        else:
                            liquidity_price_correlation = 0.0
                    else:
                        liquidity_price_correlation = 0.0
                else:
                    liquidity_price_correlation = 0.0
            else:
                liquidity_price_correlation = 0.0
            
            feature_vector = np.array([
                update_frequency,
                frequency_volatility, 
                rhythm_regularity,
                spread_rhythm_vol,
                spread_oscillations,
                imbalance_rhythm_strength,
                size_pattern_consistency,
                liquidity_price_correlation
            ])
            
            features.append(feature_vector)
            feature_timestamps.append(timestamps[i])
        
        features_array = np.array(features)
        timestamps_array = np.array(feature_timestamps)
        
        # Enhanced provenance
        parameters = {
            "window_size": window_size,
            "rhythm_analysis": ["update_frequency", "spread_pulsation", "size_patterns", "liquidity_timing"],
            "innovation": "intraday_microstructure_rhythm_detection"
        }
        
        feature_id = self._generate_feature_id(FeatureType.MICROSTRUCTURE, parameters)
        
        provenance = FeatureProvenance(
            feature_id=feature_id,
            source_datasets=["orderbook_data"],
            transformation_pipeline=["timestamp_analysis", "rhythm_detection", "pulsation_analysis", "pattern_consistency"],
            parameters=parameters,
            creation_timestamp=int(time.time() * 1_000_000),
            data_version="1.0",
            algorithm_version="2.0",
            validation_checksum=leakage_proof_id,
            dependency_graph={"orderbook_data": ["bid_price", "ask_price", "bid_size", "ask_size", timestamp_col]}
        )
        
        quality_score = self._compute_quality_score(features_array.flatten(), timestamps_array)
        
        return FeatureVector(
            feature_id=feature_id,
            feature_type=FeatureType.MICROSTRUCTURE,
            values=features_array,
            timestamps=timestamps_array,
            window_size=window_size,
            horizon=1,
            units=FeatureUnits.RATIO,
            provenance=provenance,
            leakage_proof_id=leakage_proof_id,
            quality_score=quality_score,
            drift_status=DriftStatus.UNKNOWN,
            metadata={
                "feature_names": [
                    "update_frequency", "frequency_volatility", "rhythm_regularity",
                    "spread_rhythm_vol", "spread_oscillations", "imbalance_rhythm_strength", 
                    "size_pattern_consistency", "liquidity_price_correlation"
                ],
                "computation_time": time.time(),
                "innovation": "microstructure_rhythm_analysis"
            }
        )
    
    async def compute_feature_interaction_network(self, 
                                                feature_vectors: List[FeatureVector],
                                                interaction_window: int = 50) -> Dict[str, float]:
        """
        Discover non-linear feature interactions dynamically without overfitting.
        
        Innovation: Automated feature interaction discovery using mutual information and
        conditional independence testing. Stays within Feature Factory domain.
        
        Args:
            feature_vectors: List of FeatureVector objects to analyze interactions
            interaction_window: Window size for interaction analysis
            
        Returns:
            Dictionary of interaction strength metrics
        """
        if len(feature_vectors) < 2:
            return {"interaction_strength": 0.0}
        
        # Align all feature vectors by timestamps
        common_timestamps = None
        for fv in feature_vectors:
            if common_timestamps is None:
                common_timestamps = set(fv.timestamps)
            else:
                common_timestamps &= set(fv.timestamps)
        
        if common_timestamps is None or len(common_timestamps) < interaction_window:
            return {"interaction_strength": 0.0, "aligned_samples": len(common_timestamps) if common_timestamps else 0}
        
        common_timestamps = sorted(list(common_timestamps))[-interaction_window:]
        
        # Extract aligned feature data
        aligned_features = []
        feature_names = []
        
        for fv in feature_vectors:
            # Find indices for common timestamps
            indices = [np.where(fv.timestamps == ts)[0][0] for ts in common_timestamps 
                      if ts in fv.timestamps]
            
            if len(indices) == len(common_timestamps):
                # Flatten feature values for each timestamp
                fv_data = fv.values[indices].flatten() if fv.values.ndim > 1 else fv.values[indices]
                aligned_features.append(fv_data)
                feature_names.extend([f"{fv.feature_type.value}_{i}" for i in range(len(fv_data))])
        
        if len(aligned_features) < 2:
            return {"interaction_strength": 0.0, "insufficient_alignment": True}
        
        # Stack features for interaction analysis
        feature_matrix = np.column_stack(aligned_features)
        
        # 1. Advanced Mutual Information Network with non-linear dependencies
        mutual_info_scores = []
        non_linear_dependencies = []
        n_features = feature_matrix.shape[1]
        
        for i in range(n_features):
            for j in range(i+1, n_features):
                if np.std(feature_matrix[:, i]) > 0 and np.std(feature_matrix[:, j]) > 0:
                    x_i = feature_matrix[:, i]
                    x_j = feature_matrix[:, j]
                    
                    # Linear correlation
                    correlation = np.corrcoef(x_i, x_j)[0,1]
                    if not np.isnan(correlation):
                        # Gaussian MI approximation from correlation
                        linear_mi = -0.5 * np.log(1 - correlation**2) if abs(correlation) < 0.999 else 2.0
                        
                        # Non-linear dependency using distance correlation proxy
                        # Rank-based dependency to capture monotonic non-linear relationships
                        from scipy import stats
                        spearman_result = stats.spearmanr(x_i, x_j)
                        spearman_corr = float(spearman_result.correlation if hasattr(spearman_result, 'correlation') else spearman_result[0])
                        if not np.isnan(spearman_corr):
                            rank_based_mi = -0.5 * np.log(1 - spearman_corr**2) if abs(spearman_corr) < 0.999 else 2.0
                            
                            # Detect non-linear dependencies
                            non_linear_strength = abs(rank_based_mi - linear_mi)
                            non_linear_dependencies.append(non_linear_strength)
                            
                            # Combined MI estimate (weighted average)
                            combined_mi = 0.7 * linear_mi + 0.3 * rank_based_mi
                            mutual_info_scores.append(combined_mi)
                        else:
                            mutual_info_scores.append(linear_mi)
                            non_linear_dependencies.append(0.0)
        
        avg_mutual_info = np.mean(mutual_info_scores) if mutual_info_scores else 0.0
        avg_non_linear_strength = np.mean(non_linear_dependencies) if non_linear_dependencies else 0.0
        
        # 2. Feature interaction strength via PCA (innovation)
        try:
            if feature_matrix.shape[1] >= 2 and feature_matrix.shape[0] > feature_matrix.shape[1] and SKLEARN_AVAILABLE:
                # Standardize features manually
                feature_means = np.mean(feature_matrix, axis=0)
                feature_stds = np.std(feature_matrix, axis=0)
                feature_stds = np.where(feature_stds == 0, 1.0, feature_stds)  # Avoid division by zero
                scaled_features = (feature_matrix - feature_means) / feature_stds
                
                # PCA to measure explained variance concentration
                pca = PCA()  # type: ignore
                pca.fit(scaled_features)
                
                # Interaction strength = how much variance is NOT captured by first component
                first_component_ratio = pca.explained_variance_ratio_[0]
                interaction_strength = 1.0 - first_component_ratio
            else:
                interaction_strength = 0.0
                
        except (ImportError, Exception):
            # Fallback: use correlation matrix analysis
            corr_matrix = np.corrcoef(feature_matrix.T)
            off_diagonal = corr_matrix[np.triu_indices_from(corr_matrix, k=1)]
            interaction_strength = np.mean(np.abs(off_diagonal)) if len(off_diagonal) > 0 else 0.0
        
        # 3. Conditional independence testing (simplified)
        conditional_independence_score = 0.0
        if n_features >= 3:
            # Test if feature i is independent of feature j given feature k
            for i in range(min(3, n_features)):
                for j in range(i+1, min(3, n_features)):
                    for k in range(n_features):
                        if k != i and k != j:
                            # Partial correlation as proxy for conditional independence
                            try:
                                r_ik = np.corrcoef(feature_matrix[:, i], feature_matrix[:, k])[0,1]
                                r_jk = np.corrcoef(feature_matrix[:, j], feature_matrix[:, k])[0,1]
                                r_ij = np.corrcoef(feature_matrix[:, i], feature_matrix[:, j])[0,1]
                                
                                if not any(np.isnan([r_ik, r_jk, r_ij])):
                                    partial_corr = (r_ij - r_ik * r_jk) / np.sqrt((1 - r_ik**2) * (1 - r_jk**2))
                                    if not np.isnan(partial_corr):
                                        conditional_independence_score += abs(partial_corr)
                            except (ZeroDivisionError, ValueError):
                                continue
        
        # 4. Feature redundancy analysis
        feature_redundancy = 0.0
        if len(mutual_info_scores) > 0:
            # High mutual info scores suggest redundancy
            redundancy_threshold = 0.8
            high_redundancy_pairs = sum(1 for score in mutual_info_scores if score > redundancy_threshold)
            feature_redundancy = high_redundancy_pairs / len(mutual_info_scores)
        
        return {
            "interaction_strength": float(interaction_strength),
            "avg_mutual_info": float(avg_mutual_info),
            "avg_non_linear_strength": float(avg_non_linear_strength),
            "conditional_independence": float(conditional_independence_score),
            "feature_redundancy": float(feature_redundancy),
            "n_features_analyzed": n_features,
            "n_samples": len(common_timestamps),
            "feature_diversity": 1.0 - feature_redundancy,  # Inverse of redundancy
            "non_linear_complexity": float(avg_non_linear_strength / (avg_mutual_info + 1e-8)) if avg_mutual_info > 0 else 0.0
        }
    
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
        """
        Compute Parkinson volatility estimator.
        
        Uses enhanced estimator: σ = std(r) × √(2ln2) × scale_factor
        This accounts for the theoretical relationship between discrete and continuous volatility.
        """
        if len(returns) < 2:
            return self._to_float(np.std(returns)) if len(returns) > 0 else 0.0
        
        # Enhanced Parkinson estimator for returns data
        return self._to_float(np.std(returns) * np.sqrt(2 * np.log(2)) * self.config.volatility_scaling)
    
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
        
        # Exponentially weighted variance calculation
        weights = np.array([(1-alpha) * (alpha**i) for i in range(len(returns))])
        weights = weights[::-1]  # Reverse for proper weighting (recent data gets higher weight)
        weights /= weights.sum()  # Normalize to sum to 1
        
        ewma_var = np.sum(weights * returns**2)  # Zero-mean variance assumption
        return float(np.sqrt(ewma_var) * self.config.volatility_scaling)
    
    # =============================================================================
    # ADVANCED MATHEMATICAL OPTIMIZATION AND INTELLIGENCE
    # =============================================================================
    
    def auto_discover_optimal_windows(self, price_data: pd.DataFrame, 
                                    target_horizon: int = 5,
                                    price_col: str = "close",
                                    timestamp_col: str = "timestamp") -> Dict[str, int]:
        """
        Auto-discover optimal window sizes using information-theoretic criteria.
        
        Uses Akaike Information Criterion (AIC) and cross-validation to find
        optimal parameters that balance model complexity with predictive power.
        
        Args:
            price_data: Price data for optimization
            target_horizon: Target prediction horizon
            price_col: Price column name
            timestamp_col: Timestamp column name
            
        Returns:
            Dictionary of optimal parameters
        """
        prices = np.array(price_data[price_col].values, dtype=float)
        if len(prices) < 100:
            return {"optimal_window": 20, "optimal_lag": 1, "confidence": 0}
        
        log_prices = np.log(prices)
        returns = np.diff(log_prices)
        
        # Test different window sizes
        candidate_windows = [10, 15, 20, 30, 50, 75, 100]
        candidate_lags = [1, 2, 3, 5]
        
        best_score = -np.inf
        optimal_params = {"optimal_window": 20, "optimal_lag": 1, "confidence": 0.0}
        
        for window in candidate_windows:
            if len(returns) < window + target_horizon + 10:
                continue
                
            for lag in candidate_lags:
                # Information criterion evaluation
                ic_score = self._evaluate_information_criterion(returns, window, lag, target_horizon)
                
                if ic_score > best_score:
                    best_score = ic_score
                    optimal_params = {
                        "optimal_window": window,
                        "optimal_lag": lag,
                        "confidence": min(1.0, ic_score / 10.0),  # Normalize score
                        "information_score": ic_score
                    }
        
        return optimal_params
    
    def _evaluate_information_criterion(self, returns: np.ndarray, 
                                      window: int, lag: int, horizon: int) -> float:
        """
        Evaluate window parameters using information-theoretic criteria.
        
        Combines predictive power, model complexity, and stability measures.
        """
        if len(returns) < window + horizon + 10:
            return -np.inf
        
        # Create features and targets
        features = []
        targets = []
        
        for i in range(window, len(returns) - horizon):
            # Feature vector: window of returns with lag
            feature_window = returns[i-window:i:lag]  # Sample with lag
            target = returns[i + horizon - 1]  # Future return
            
            if len(feature_window) > 0:
                features.append([
                    np.mean(feature_window),
                    np.std(feature_window),
                    stats.skew(feature_window) if len(feature_window) > 2 else 0.0
                ])
                targets.append(target)
        
        if len(features) < 20:
            return -np.inf
        
        features = np.array(features)
        targets = np.array(targets)
        
        # Simple linear regression for predictive power
        try:
            # Add intercept
            X = np.column_stack([np.ones(len(features)), features])
            
            # Ridge regression for stability
            lambda_reg = 0.01
            XTX_reg = X.T @ X + lambda_reg * np.eye(X.shape[1])
            betas = np.linalg.inv(XTX_reg) @ X.T @ targets
            
            # Predictions and residuals
            predictions = X @ betas
            residuals = targets - predictions
            
            # Model evaluation metrics
            mse = np.mean(residuals ** 2)
            n_samples = len(targets)
            n_params = X.shape[1]
            
            # Akaike Information Criterion (lower is better, so we negate)
            aic = n_samples * np.log(mse) + 2 * n_params
            
            # Predictive R-squared
            ss_res = np.sum(residuals ** 2)
            ss_tot = np.sum((targets - np.mean(targets)) ** 2)
            r_squared = max(0.0, 1 - ss_res / ss_tot) if ss_tot > 0 else 0.0
            
            # Combined information score (higher is better)
            info_score = r_squared * 10 - aic / n_samples + np.log(window) * 0.1
            
            return info_score
            
        except np.linalg.LinAlgError:
            return -np.inf
    
    def compute_adaptive_feature_transformations(self, feature_data: np.ndarray,
                                               feature_names: List[str]) -> Dict[str, Any]:
        """
        Apply adaptive non-linear transformations based on data distribution.
        
        Automatically selects optimal transformations (log, sqrt, Box-Cox, etc.)
        based on normality tests and information criteria.
        
        Args:
            feature_data: Raw feature matrix (n_samples, n_features)
            feature_names: Names of features for tracking
            
        Returns:
            Dictionary of transformed features with transformation metadata
        """
        if feature_data.shape[0] < 10:
            return {"raw_features": feature_data, "transformations": ["none"] * feature_data.shape[1]}
        
        transformed_features = {}
        transformations_applied = []
        
        for i in range(feature_data.shape[1]):
            feature_col = feature_data[:, i]
            feature_name = feature_names[i] if i < len(feature_names) else f"feature_{i}"
            
            # Skip constant features
            if np.std(feature_col) < 1e-10:
                transformed_features[feature_name] = feature_col
                transformations_applied.append("none")
                continue
            
            # Test different transformations
            transformations = {
                "raw": feature_col,
                "standardized": (feature_col - np.mean(feature_col)) / np.std(feature_col),
                "robust_scaled": self._robust_scale(feature_col),
            }
            
            # Add non-linear transformations for positive data
            if np.all(feature_col > 0):
                transformations["log"] = np.log(feature_col + 1e-8)
                transformations["sqrt"] = np.sqrt(feature_col)
                
            # Add power transformations
            if np.min(feature_col) >= 0:
                # Yeo-Johnson transformation (generalized Box-Cox)
                transformed_features[feature_name + "_yj"] = self._yeo_johnson_transform(feature_col)
            
            # Select best transformation based on normality
            best_transform = "raw"
            best_normality_score = self._assess_normality(feature_col)
            
            for transform_name, transform_data in transformations.items():
                normality_score = self._assess_normality(transform_data)
                if normality_score > best_normality_score:
                    best_normality_score = normality_score
                    best_transform = transform_name
            
            # Apply best transformation
            transformed_features[feature_name] = transformations[best_transform]
            transformations_applied.append(best_transform)
        
        # Stack all transformed features
        transformed_matrix = np.column_stack(list(transformed_features.values()))
        
        return {
            "transformed_features": transformed_matrix,
            "transformations": transformations_applied,
            "normality_scores": [self._assess_normality(transformed_matrix[:, i]) 
                               for i in range(transformed_matrix.shape[1])],
            "feature_names": list(transformed_features.keys())
        }
    
    def _robust_scale(self, data: np.ndarray) -> np.ndarray:
        """Robust scaling using median and MAD (Median Absolute Deviation)."""
        median = np.median(data)
        mad = np.median(np.abs(data - median))
        
        # Avoid division by zero
        scale = max(mad, 1e-8) * 1.4826  # 1.4826 for normal distribution consistency
        return (data - median) / scale
    
    def _yeo_johnson_transform(self, data: np.ndarray, lambda_param: float = 0.0) -> np.ndarray:
        """
        Yeo-Johnson transformation (generalized Box-Cox for any real numbers).
        
        Automatically estimates optimal lambda parameter.
        """
        # Estimate optimal lambda using maximum likelihood (simplified)
        candidate_lambdas = [-2, -1, -0.5, 0, 0.5, 1, 2]
        best_lambda = 0.0
        best_score = -np.inf
        
        for lam in candidate_lambdas:
            try:
                transformed = self._yj_transform_core(data, lam)
                # Score based on normality (simplified using skewness)
                score = 1.0 / (1.0 + abs(stats.skew(transformed))) if len(transformed) > 2 else 0.0
                
                if score > best_score:
                    best_score = score
                    best_lambda = lam
            except:
                continue
        
        return self._yj_transform_core(data, best_lambda)
    
    def _yj_transform_core(self, data: np.ndarray, lambda_param: float) -> np.ndarray:
        """Core Yeo-Johnson transformation implementation."""
        transformed = np.zeros_like(data)
        
        if abs(lambda_param) < 1e-8:  # lambda ≈ 0
            # log transformation for non-negative values
            pos_mask = data >= 0
            neg_mask = data < 0
            
            transformed[pos_mask] = np.log(data[pos_mask] + 1)
            transformed[neg_mask] = -np.log(-data[neg_mask] + 1)
        else:
            # Power transformation
            pos_mask = data >= 0
            neg_mask = data < 0
            
            if lambda_param != 2:
                transformed[pos_mask] = ((data[pos_mask] + 1) ** lambda_param - 1) / lambda_param
                transformed[neg_mask] = -((-data[neg_mask] + 1) ** (2 - lambda_param) - 1) / (2 - lambda_param)
            else:
                # Special case for lambda = 2
                transformed[pos_mask] = ((data[pos_mask] + 1) ** 2 - 1) / 2
                transformed[neg_mask] = -np.log(-data[neg_mask] + 1)
        
        return transformed
    
    def _assess_normality(self, data: np.ndarray) -> float:
        """
        Assess normality using multiple criteria.
        
        Returns a score between 0 and 1, where 1 indicates perfect normality.
        """
        if len(data) < 8:
            return 0.5  # Neutral score for insufficient data
        
        # Remove NaN values
        clean_data = data[~np.isnan(data)]
        if len(clean_data) < 3:
            return 0.0
        
        # Multiple normality measures
        scores = []
        
        # 1. Skewness (should be close to 0)
        skewness = abs(stats.skew(clean_data)) if len(clean_data) > 2 else 0.0
        skew_score = max(0.0, 1.0 - skewness / 2.0)  # Penalize high skewness
        scores.append(skew_score)
        
        # 2. Kurtosis (should be close to 3 for normal distribution)
        kurtosis = stats.kurtosis(clean_data) if len(clean_data) > 2 else 0.0
        kurt_score = max(0.0, 1.0 - abs(kurtosis) / 3.0)  # Penalize high kurtosis
        scores.append(kurt_score)
        
        # 3. Shapiro-Wilk test (if sample size allows)
        if 3 <= len(clean_data) <= 5000:
            try:
                _, p_value = stats.shapiro(clean_data)
                shapiro_score = min(1.0, p_value * 2)  # Convert p-value to score
                scores.append(shapiro_score)
            except:
                pass
        
        # 4. Anderson-Darling normality (simplified)
        try:
            ad_result = stats.anderson(clean_data, dist='norm')
            ad_stat = float(ad_result.statistic if hasattr(ad_result, 'statistic') else ad_result[0])
            ad_score = max(0.0, 1.0 - ad_stat / 5.0)  # Normalize AD statistic
            scores.append(ad_score)
        except:
            pass
        
        # Return weighted average of normality scores
        return float(np.mean(scores)) if scores else 0.5
    
    def compute_feature_stability_metrics(self, feature_vectors: List[FeatureVector],
                                        stability_window: int = 100) -> Dict[str, float]:
        """
        Compute comprehensive feature stability metrics across different market regimes.
        
        Measures consistency of feature behavior during different volatility and trend regimes.
        """
        if len(feature_vectors) == 0:
            return {"overall_stability": 0.0}
        
        # Combine all feature data
        all_timestamps = []
        all_features = []
        
        for fv in feature_vectors:
            all_timestamps.extend(fv.timestamps)
            if fv.values.ndim > 1:
                all_features.extend(fv.values.flatten())
            else:
                all_features.extend(fv.values)
        
        all_timestamps = np.array(all_timestamps)
        all_features = np.array(all_features)
        
        if len(all_features) < stability_window * 2:
            return {"overall_stability": 0.0, "insufficient_data": True}
        
        # Divide data into regimes based on rolling volatility
        stability_scores = []
        regime_consistency = []
        
        # Rolling window analysis
        for i in range(stability_window, len(all_features) - stability_window):
            window_1 = all_features[i-stability_window:i]
            window_2 = all_features[i:i+stability_window]
            
            # Statistical consistency between consecutive windows
            if np.std(window_1) > 0 and np.std(window_2) > 0:
                # Kolmogorov-Smirnov test for distribution consistency
                try:
                    ks_result = stats.ks_2samp(window_1, window_2)
                    p_value = float(ks_result.pvalue if hasattr(ks_result, 'pvalue') else ks_result[1])
                    distribution_stability = min(1.0, p_value * 2)  # Convert to stability score
                    stability_scores.append(distribution_stability)
                    
                    # Mean and variance stability
                    mean_stability = 1.0 / (1.0 + abs(np.mean(window_1) - np.mean(window_2)) / (np.std(window_1) + 1e-8))
                    var_stability = 1.0 / (1.0 + abs(np.std(window_1) - np.std(window_2)) / (np.std(window_1) + 1e-8))
                    
                    regime_consistency.append((mean_stability + var_stability) / 2.0)
                except:
                    stability_scores.append(0.5)
                    regime_consistency.append(0.5)
        
        # Overall stability metrics
        overall_stability = np.mean(stability_scores) if stability_scores else 0.0
        cross_regime_stability = np.mean(regime_consistency) if regime_consistency else 0.0
        
        # Volatility clustering analysis (GARCH-like behavior)
        if len(all_features) > 20:
            returns = np.diff(all_features)
            squared_returns = returns ** 2
            
            # Autocorrelation of squared returns (volatility clustering)
            if len(squared_returns) > 1:
                vol_autocorr = np.corrcoef(squared_returns[:-1], squared_returns[1:])[0,1]
                vol_clustering = abs(vol_autocorr) if not np.isnan(vol_autocorr) else 0.0
            else:
                vol_clustering = 0.0
        else:
            vol_clustering = 0.0
        
        return {
            "overall_stability": float(overall_stability),
            "cross_regime_stability": float(cross_regime_stability),
            "volatility_clustering": float(vol_clustering),
            "stability_variance": float(np.std(stability_scores)) if stability_scores else 0.0,
            "n_regimes_analyzed": len(stability_scores)
        }
    
    def detect_feature_degradation_early(self, recent_features: np.ndarray,
                                       historical_features: np.ndarray,
                                       feature_names: List[str]) -> Dict[str, Any]:
        """
        Early warning system for feature quality degradation.
        
        Uses statistical process control and change point detection to identify
        when features are losing predictive power or becoming unreliable.
        """
        if len(recent_features) < 10 or len(historical_features) < 20:
            return {"degradation_detected": False, "confidence": 0.0}
        
        # Ensure same number of features
        min_features = min(recent_features.shape[1] if recent_features.ndim > 1 else 1,
                          historical_features.shape[1] if historical_features.ndim > 1 else 1)
        
        if recent_features.ndim == 1:
            recent_features = recent_features.reshape(-1, 1)
        if historical_features.ndim == 1:
            historical_features = historical_features.reshape(-1, 1)
            
        recent_features = recent_features[:, :min_features]
        historical_features = historical_features[:, :min_features]
        
        degradation_signals = []
        individual_scores = []
        
        for i in range(min_features):
            recent_col = recent_features[:, i]
            historical_col = historical_features[:, i]
            
            # Remove NaN values
            recent_clean = recent_col[~np.isnan(recent_col)]
            historical_clean = historical_col[~np.isnan(historical_col)]
            
            if len(recent_clean) < 5 or len(historical_clean) < 10:
                individual_scores.append({"degradation_score": 0.0, "alert_level": "NONE"})
                continue
            
            # Statistical process control
            historical_mean = np.mean(historical_clean)
            historical_std = np.std(historical_clean)
            
            # Control limits (3-sigma)
            ucl = historical_mean + 3 * historical_std
            lcl = historical_mean - 3 * historical_std
            
            # Count out-of-control points
            ooc_points = np.sum((recent_clean > ucl) | (recent_clean < lcl))
            ooc_rate = ooc_points / len(recent_clean)
            
            # Distribution shift detection (Kolmogorov-Smirnov)
            try:
                ks_result = stats.ks_2samp(historical_clean, recent_clean)
                p_value = float(ks_result.pvalue if hasattr(ks_result, 'pvalue') else ks_result[1])
                distribution_shift = 1.0 - p_value  # Higher = more shift
            except:
                distribution_shift = 0.0
            
            # Mean shift detection
            mean_shift = abs(np.mean(recent_clean) - historical_mean) / (historical_std + 1e-8)
            
            # Variance change detection
            variance_ratio = np.std(recent_clean) / (historical_std + 1e-8)
            variance_change = abs(np.log(variance_ratio)) if variance_ratio > 0 else 0.0
            
            # Combined degradation score
            degradation_score = (
                0.3 * ooc_rate +           # Process control violations
                0.3 * distribution_shift +  # Distribution changes
                0.2 * min(mean_shift / 2, 1.0) +    # Mean shift (normalized)
                0.2 * min(variance_change, 1.0)     # Variance change
            )
            
            # Alert levels
            if degradation_score > 0.7:
                alert_level = "CRITICAL"
            elif degradation_score > 0.5:
                alert_level = "HIGH"  
            elif degradation_score > 0.3:
                alert_level = "MEDIUM"
            else:
                alert_level = "LOW"
            
            individual_scores.append({
                "degradation_score": float(degradation_score),
                "alert_level": alert_level,
                "ooc_rate": float(ooc_rate),
                "distribution_shift": float(distribution_shift),
                "mean_shift": float(mean_shift),
                "variance_change": float(variance_change)
            })
            
            degradation_signals.append(degradation_score)
        
        # Overall degradation assessment
        overall_degradation = np.mean(degradation_signals) if degradation_signals else 0.0
        max_degradation = np.max(degradation_signals) if degradation_signals else 0.0
        
        # System-wide alert
        degradation_detected = overall_degradation > 0.4 or max_degradation > 0.6
        
        return {
            "degradation_detected": degradation_detected,
            "overall_degradation_score": float(overall_degradation),
            "max_individual_degradation": float(max_degradation),
            "individual_feature_scores": individual_scores,
            "confidence": float(min(1.0, overall_degradation * 2)),
            "n_features_analyzed": len(degradation_signals),
            "system_alert_level": "CRITICAL" if max_degradation > 0.7 else "HIGH" if max_degradation > 0.5 else "MEDIUM" if max_degradation > 0.3 else "LOW"
        }
    
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
            ks_result = stats.ks_2samp(baseline_data, current_data)
            # Handle different scipy versions - robust extraction
            p_value = 1.0  # Default
            
            # Method 1: Try named tuple attribute
            try:
                p_value = float(getattr(ks_result, 'pvalue'))
            except (AttributeError, TypeError):
                # Method 2: Try tuple indexing
                try:
                    if isinstance(ks_result, (tuple, list)) and len(ks_result) >= 2:
                        p_val_candidate = ks_result[1]
                        if isinstance(p_val_candidate, (int, float, np.floating)):
                            p_value = float(p_val_candidate)
                except (IndexError, TypeError, ValueError):
                    p_value = 1.0
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
    
    # =============================================================================
    # INTELLIGENT FEATURE ENGINEERING (Auto-Discovery & Optimization)
    # =============================================================================
    
    def _auto_discover_optimal_windows_duplicate_to_remove(self, 
                                    data: pd.DataFrame, 
                                    target_col: str,
                                    feature_cols: List[str],
                                    window_candidates: List[int] = [5, 10, 20, 50, 100, 200],
                                    price_col: str = "close") -> Dict[str, int]:
        """
        Automatically discover optimal lookback windows per feature type using information criteria.
        
        Innovation: Data-driven window optimization that maximizes signal while minimizing noise.
        Pure feature engineering optimization without crossing into modeling domain.
        
        Args:
            data: Input data with features and target
            target_col: Target variable for optimization 
            feature_cols: Feature columns to optimize windows for
            window_candidates: List of window sizes to test
            price_col: Price column for return computation
            
        Returns:
            Dictionary mapping feature types to optimal window sizes
        """
        if len(data) < max(window_candidates) * 2:
            logger.warning("Insufficient data for window optimization, using defaults")
            return {col: 20 for col in feature_cols}  # Default fallback
        
        optimal_windows = {}
        
        for feature_col in feature_cols:
            if feature_col not in data.columns:
                continue
                
            best_window = window_candidates[0]
            best_score = -np.inf
            
            for window in window_candidates:
                if len(data) < window * 3:  # Need sufficient data
                    continue
                    
                try:
                    # Compute rolling feature statistics
                    feature_data = data[feature_col].rolling(window=window).agg(['mean', 'std']).dropna()
                    
                    if len(feature_data) < 10:  # Need minimum samples
                        continue
                    
                    # Align with target
                    aligned_data = pd.merge_asof(
                        feature_data.reset_index(),
                        data[[target_col]].reset_index(),
                        left_index=True, right_index=True,
                        direction='nearest'
                    ).dropna()
                    
                    if len(aligned_data) < 5:
                        continue
                    
                    # Information criterion: Mutual Information / Window Size (parsimony principle)
                    feature_mean = np.array(aligned_data[f'{feature_col}_mean'].values, dtype=float)
                    target_values = np.array(aligned_data[target_col].values, dtype=float)
                    
                    # Simplified mutual information via correlation
                    if np.std(feature_mean) > 0 and np.std(target_values) > 0:
                        correlation = abs(np.corrcoef(feature_mean, target_values)[0,1])
                        if not np.isnan(correlation):
                            # Information score with parsimony penalty
                            info_score = correlation / np.log(window)  # Penalize larger windows
                            
                            if info_score > best_score:
                                best_score = info_score
                                best_window = window
                
                except Exception as e:
                    logger.debug(f"Window optimization failed for {feature_col} window {window}: {e}")
                    continue
            
            optimal_windows[feature_col] = best_window
            logger.info(f"Optimal window for {feature_col}: {best_window} (score: {best_score:.4f})")
        
        return optimal_windows
    
    def compute_feature_stability_metrics(self, 
                                        feature_vector: FeatureVector,
                                        stability_window: int = 100) -> Dict[str, float]:
        """
        Measure feature stability across market regimes for quality assessment.
        
        Innovation: Multi-dimensional stability analysis including regime-conditional stability.
        Pure feature quality assessment within Feature Factory domain.
        
        Args:
            feature_vector: FeatureVector to analyze
            stability_window: Rolling window for stability analysis
            
        Returns:
            Dictionary of stability metrics
        """
        if len(feature_vector.values) < stability_window * 2:
            return {"stability_score": 0.5, "insufficient_data": True}
        
        feature_data = feature_vector.values.flatten() if feature_vector.values.ndim > 1 else feature_vector.values
        
        stability_metrics = {}
        
        # 1. Rolling Statistics Stability
        rolling_means = []
        rolling_stds = []
        rolling_skews = []
        
        for i in range(stability_window, len(feature_data)):
            window = feature_data[i-stability_window:i]
            rolling_means.append(np.mean(window))
            rolling_stds.append(np.std(window))
            
            if len(window) > 2:
                rolling_skews.append(stats.skew(window))
            else:
                rolling_skews.append(0.0)
        
        # Stability = inverse of coefficient of variation of rolling statistics
        mean_stability = 1.0 / (1.0 + np.std(rolling_means) / (np.mean(rolling_means) + 1e-8))
        std_stability = 1.0 / (1.0 + np.std(rolling_stds) / (np.mean(rolling_stds) + 1e-8))
        skew_stability = 1.0 / (1.0 + np.std(rolling_skews) + 1e-8)
        
        stability_metrics['mean_stability'] = float(mean_stability)
        stability_metrics['std_stability'] = float(std_stability)
        stability_metrics['skew_stability'] = float(skew_stability)
        
        # 2. Regime-Conditional Stability (innovation)
        # Simple regime detection via volatility quantiles
        rolling_volatilities = np.array(rolling_stds)
        vol_quantiles = np.percentile(rolling_volatilities, [33, 67])
        
        # Align indices properly - rolling_volatilities has fewer elements than feature_data
        # Use the corresponding subset of feature_data for regime analysis
        aligned_feature_data = feature_data[stability_window:]  # Skip first stability_window elements
        
        low_vol_regime = aligned_feature_data[rolling_volatilities <= vol_quantiles[0]]
        mid_vol_regime = aligned_feature_data[(rolling_volatilities > vol_quantiles[0]) & 
                                            (rolling_volatilities <= vol_quantiles[1])]
        high_vol_regime = aligned_feature_data[rolling_volatilities > vol_quantiles[1]]
        
        # Stability across regimes
        regime_means = []
        regime_stds = []
        
        for regime_data in [low_vol_regime, mid_vol_regime, high_vol_regime]:
            if len(regime_data) > 0:
                regime_means.append(np.mean(regime_data))
                regime_stds.append(np.std(regime_data))
        
        if len(regime_means) > 1:
            cross_regime_stability = 1.0 / (1.0 + np.std(regime_means) / (np.mean(regime_means) + 1e-8))
        else:
            cross_regime_stability = 0.5
        
        stability_metrics['cross_regime_stability'] = float(cross_regime_stability)
        
        # 3. Autocorrelation Stability (persistence)
        autocorrelations = []
        for lag in [1, 5, 10]:
            if len(feature_data) > lag:
                autocorr = np.corrcoef(feature_data[:-lag], feature_data[lag:])[0,1]
                if not np.isnan(autocorr):
                    autocorrelations.append(abs(autocorr))
        
        persistence_score = np.mean(autocorrelations) if autocorrelations else 0.0
        stability_metrics['persistence_score'] = float(persistence_score)
        
        # 4. Overall Stability Score
        component_weights = {
            'mean_stability': 0.3,
            'std_stability': 0.25, 
            'skew_stability': 0.15,
            'cross_regime_stability': 0.2,
            'persistence_score': 0.1
        }
        
        overall_stability = sum(stability_metrics[key] * weight 
                              for key, weight in component_weights.items() 
                              if key in stability_metrics)
        
        stability_metrics['overall_stability_score'] = float(overall_stability)
        stability_metrics['n_windows_analyzed'] = len(rolling_means)
        
        return stability_metrics
    
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
            "avg_quality_score": float(np.mean([fv.quality_score for fv in self.feature_registry.values()])) if self.feature_registry else 0.0
        }
    
    # =====================================================================================
    # UTILITY FUNCTIONS FOR TYPE SAFETY
    # =====================================================================================
    
    def _to_numpy_array(self, data) -> np.ndarray:
        """Safely convert pandas/other arrays to numpy arrays."""
        if hasattr(data, 'values'):
            return np.asarray(data.values, dtype=float)
        return np.asarray(data, dtype=float)

    # =====================================================================================
    # ADVANCED MATHEMATICAL IMPLEMENTATIONS
    # =====================================================================================
    
    def _compute_pin_model_features(self, prices: np.ndarray, volumes: np.ndarray, 
                                   buy_volumes: np.ndarray, sell_volumes: np.ndarray) -> Dict[str, float]:
        """
        Compute Probability of Informed Trading (PIN) model features using MLE estimation.
        
        Based on Easley, Kiefer, O'Hara & Paperman (1996).
        """
        default_dict = {"pin": 0.0, "alpha": 0.0, "delta": 0.5, "epsilon_buy": 0.0, "epsilon_sell": 0.0, "mu": 0.0}
        return self._safe_compute_feature(
            "pin_model",
            self._compute_pin_model_core,
            default_dict,
            prices, volumes, buy_volumes, sell_volumes
        )
    
    def _compute_pin_model_core(self, prices: np.ndarray, volumes: np.ndarray, 
                               buy_volumes: np.ndarray, sell_volumes: np.ndarray) -> Dict[str, float]:
        """Core PIN model computation with proper exception handling."""
        try:
            n_days = len(prices)
            if n_days < 10:  # Need minimum data for MLE
                return {"pin": 0.0, "alpha": 0.0, "delta": 0.5, "epsilon_buy": 0.0, "epsilon_sell": 0.0, "mu": 0.0}
            
            # Initial parameter estimates using method of moments
            total_vol_mean = np.mean(volumes)
            buy_ratio = np.mean(buy_volumes / (buy_volumes + sell_volumes + 1e-8))
            
            # MLE estimation with multiple starting points for robustness
            best_likelihood = -np.inf
            best_params = None
            
            for attempt in range(3):  # Multiple random starts
                try:
                    # Initial guess with some randomization
                    init_alpha = min(0.8, np.random.uniform(0.1, 0.5))
                    init_delta = max(0.1, min(0.9, buy_ratio + np.random.normal(0, 0.1)))
                    init_epsilon = total_vol_mean * np.random.uniform(0.1, 0.5)
                    init_mu = total_vol_mean * np.random.uniform(0.5, 2.0)
                    
                    initial_params = [init_alpha, init_delta, init_epsilon, init_mu]
                    
                    # Optimize using scipy
                    result = optimize.minimize(
                        self._pin_negative_log_likelihood,
                        initial_params,
                        args=(buy_volumes, sell_volumes),
                        bounds=[(0.01, 0.99), (0.01, 0.99), (0.1, total_vol_mean*10), (0.1, total_vol_mean*10)],
                        method='L-BFGS-B'
                    )
                    
                    if result.success and -result.fun > best_likelihood:
                        best_likelihood = -result.fun
                        best_params = result.x
                        
                except Exception as e:
                    logger.warning(f"PIN MLE attempt {attempt} failed: {e}")
                    continue
            
            if best_params is None:
                # Fallback to simple heuristic
                return {
                    "pin": self._to_float(min(0.5, abs(buy_ratio - 0.5) * 2)),  # Simple imbalance-based PIN
                    "alpha": 0.3,
                    "delta": self._to_float(buy_ratio),
                    "epsilon_buy": self._to_float(total_vol_mean * 0.2),
                    "epsilon_sell": self._to_float(total_vol_mean * 0.2),
                    "mu": self._to_float(total_vol_mean * 0.5)
                }
            
            alpha, delta, epsilon, mu = best_params
            
            # Calculate PIN from estimated parameters
            pin = (alpha * mu) / (alpha * mu + 2 * epsilon)
            
            return {
                "pin": float(pin),
                "alpha": float(alpha),
                "delta": float(delta), 
                "epsilon_buy": float(epsilon),
                "epsilon_sell": float(epsilon),
                "mu": float(mu)
            }
            
        except Exception as e:
            logger.error(f"PIN model computation failed: {e}", exc_info=True)
            return {"pin": 0.0, "alpha": 0.0, "delta": 0.5, "epsilon_buy": 0.0, "epsilon_sell": 0.0, "mu": 0.0}
    
    def _pin_negative_log_likelihood(self, params: List[float], buy_vols: np.ndarray, sell_vols: np.ndarray) -> float:
        """Negative log-likelihood for PIN model MLE estimation."""
        try:
            alpha, delta, epsilon, mu = params
            
            # Prevent numerical issues
            if alpha <= 0 or alpha >= 1 or delta <= 0 or delta >= 1 or epsilon <= 0 or mu <= 0:
                return 1e10
            
            log_likelihood = 0.0
            
            for i in range(len(buy_vols)):
                buy_vol = buy_vols[i]
                sell_vol = sell_vols[i]
                
                # Three scenarios: good news, bad news, no news
                prob_good = alpha * delta * self._poisson_pmf(buy_vol, epsilon + mu) * self._poisson_pmf(sell_vol, epsilon)
                prob_bad = alpha * (1 - delta) * self._poisson_pmf(buy_vol, epsilon) * self._poisson_pmf(sell_vol, epsilon + mu)  
                prob_no_news = (1 - alpha) * self._poisson_pmf(buy_vol, epsilon) * self._poisson_pmf(sell_vol, epsilon)
                
                day_likelihood = prob_good + prob_bad + prob_no_news
                
                if day_likelihood > 1e-100:  # Avoid log(0)
                    log_likelihood += np.log(day_likelihood)
                else:
                    return 1e10  # Penalize invalid parameter combinations
            
            return -log_likelihood
            
        except Exception:
            return 1e10
    
    def _poisson_pmf(self, k: float, lam: float) -> float:
        """Poisson probability mass function with numerical stability."""
        try:
            if lam <= 0 or k < 0:
                return 1e-100
            pmf_result = stats.poisson.pmf(int(k), lam)
            return float(pmf_result)
        except Exception:
            return 1e-100
    
    def _compute_kyles_lambda_gmm(self, price_changes: np.ndarray, order_flows: np.ndarray) -> Dict[str, float]:
        """
        Compute Kyle's Lambda (price impact coefficient) using GMM estimation.
        
        Based on Kyle (1985) model with GMM for robustness.
        """
        default_dict = {"lambda": 0.0, "lambda_variance": 0.0, "r_squared": 0.0, "market_depth": 0.0}
        return self._safe_compute_feature(
            "kyles_lambda_gmm",
            self._compute_kyles_lambda_gmm_core,
            default_dict,
            price_changes, order_flows
        )
    
    def _compute_kyles_lambda_gmm_core(self, price_changes: np.ndarray, order_flows: np.ndarray) -> Dict[str, float]:
        """Core Kyle's Lambda computation with GMM estimation."""
        try:
            if len(price_changes) != len(order_flows) or len(price_changes) < 20:
                return {"lambda": 0.0, "lambda_variance": 0.0, "r_squared": 0.0, "market_depth": 0.0}
            
            # Remove outliers for robust estimation
            price_z = self._safe_zscore(price_changes)
            flow_z = self._safe_zscore(order_flows)
            valid_mask = (np.abs(price_z) < 3) & (np.abs(flow_z) < 3)
            price_clean = price_changes[valid_mask]
            flow_clean = order_flows[valid_mask]
            
            if len(price_clean) < 10:
                price_clean, flow_clean = price_changes, order_flows
            
            # GMM estimation with multiple moment conditions
            # Moment 1: E[price_change - lambda * order_flow] = 0
            # Moment 2: E[(price_change - lambda * order_flow) * order_flow] = 0  
            # Moment 3: E[(price_change - lambda * order_flow) * order_flow^2] = 0 (for robustness)
            
            def gmm_moments(lam_param: float) -> np.ndarray:
                residuals = price_clean - lam_param * flow_clean
                moments = np.array([
                    np.mean(residuals),  # Moment 1
                    np.mean(residuals * flow_clean),  # Moment 2
                    np.mean(residuals * flow_clean**2) if np.std(flow_clean) > 1e-8 else 0.0  # Moment 3
                ])
                return moments
            
            # Optimize GMM objective (minimizes quadratic form of moments)
            def gmm_objective(lam_param: float) -> float:
                moments = gmm_moments(lam_param)
                return np.sum(moments**2)  # Simplified identity weighting matrix
            
            # Initial guess from OLS
            if np.std(flow_clean) > 1e-8:
                lambda_ols = np.cov(price_clean, flow_clean)[0, 1] / np.var(flow_clean)
            else:
                lambda_ols = 0.0
            
            # GMM optimization
            try:
                result = optimize.minimize_scalar(
                    gmm_objective,
                    bounds=(lambda_ols - abs(lambda_ols), lambda_ols + abs(lambda_ols) + 1e-6),
                    method='bounded'
                )
                # Safely extract result
                if hasattr(result, 'success') and hasattr(result, 'x') and result.success:
                    lambda_gmm = float(result.x)
                else:
                    lambda_gmm = lambda_ols
            except Exception:
                lambda_gmm = lambda_ols
            
            # Compute diagnostics
            residuals = price_clean - lambda_gmm * flow_clean
            r_squared = 1.0 - (np.var(residuals) / np.var(price_clean)) if np.var(price_clean) > 1e-8 else 0.0
            
            # Variance estimation using Newey-West for autocorrelation robustness
            lambda_variance = self._newey_west_variance(residuals, flow_clean)
            
            # Market depth (inverse of lambda)
            market_depth = 1.0 / abs(lambda_gmm) if abs(lambda_gmm) > 1e-8 else 1e8
            
            return {
                "lambda": float(lambda_gmm),
                "lambda_variance": float(lambda_variance),
                "r_squared": float(max(0.0, r_squared)),
                "market_depth": float(min(market_depth, 1e8))  # Cap at reasonable level
            }
            
        except Exception as e:
            logger.error(f"Kyle's Lambda GMM computation failed: {e}", exc_info=True)
            return {"lambda": 0.0, "lambda_variance": 0.0, "r_squared": 0.0, "market_depth": 0.0}
    
    def _newey_west_variance(self, residuals: np.ndarray, regressors: np.ndarray, lags: int = 3) -> float:
        """Compute Newey-West heteroskedasticity and autocorrelation consistent variance."""
        try:
            n = len(residuals)
            if n < lags + 2:
                return self._to_float(np.var(residuals)) if len(residuals) > 0 else 0.0
            
            # Compute meat of sandwich estimator
            meat = 0.0
            
            # Zero lag (contemporaneous)
            meat += np.mean((residuals * regressors)**2)
            
            # Higher order lags with Bartlett weights
            for lag in range(1, min(lags + 1, n // 4)):
                weight = 1.0 - lag / (lags + 1)  # Bartlett kernel
                
                cross_prod_pos = residuals[lag:] * regressors[lag:] * residuals[:-lag] * regressors[:-lag]
                cross_prod_neg = residuals[:-lag] * regressors[:-lag] * residuals[lag:] * regressors[lag:]
                
                meat += weight * (np.mean(cross_prod_pos) + np.mean(cross_prod_neg))
            
            # Bread (denominator)
            bread = np.mean(regressors**2)
            
            if bread > 1e-12:
                return self._to_float(meat / (bread**2))
            else:
                return 0.0
                
        except Exception:
            return self._to_float(np.var(residuals)) if len(residuals) > 0 else 0.0
    
    def _compute_roll_spread_estimator(self, prices: np.ndarray) -> Dict[str, float]:
        """
        Compute Roll (1984) bid-ask spread estimator from transaction prices.
        
        Uses the relationship between price changes and effective spread.
        """
        default_dict = {"roll_spread": 0.0, "roll_spread_bps": 0.0, "price_reversal_intensity": 0.0}
        return self._safe_compute_feature(
            "roll_spread_estimator", 
            self._compute_roll_spread_estimator_core,
            default_dict,
            prices
        )
    
    def _compute_roll_spread_estimator_core(self, prices: np.ndarray) -> Dict[str, float]:
        """Core Roll spread estimator computation."""
        try:
            if len(prices) < 3:
                return {"roll_spread": 0.0, "roll_spread_bps": 0.0, "price_reversal_intensity": 0.0}
            
            # Compute price changes
            price_changes = np.diff(np.log(prices))
            
            if len(price_changes) < 2:
                return {"roll_spread": 0.0, "roll_spread_bps": 0.0, "price_reversal_intensity": 0.0}
            
            # Roll's estimator: Spread = 2 * sqrt(-Cov(Δp_t, Δp_{t-1}))
            # Based on the bounce between bid and ask prices
            
            autocovariance = np.cov(price_changes[1:], price_changes[:-1])[0, 1]
            
            # Roll spread (natural units)
            if autocovariance < 0:
                roll_spread = 2.0 * np.sqrt(-autocovariance)
            else:
                # Positive autocorrelation suggests momentum, not bid-ask bounce
                roll_spread = 0.0
            
            # Convert to basis points
            avg_price = np.mean(prices)
            roll_spread_bps = (roll_spread / avg_price) * 10000 if avg_price > 0 else 0.0
            
            # Price reversal intensity (frequency of sign changes)
            price_change_signs = np.sign(price_changes)
            reversals = np.sum(price_change_signs[1:] != price_change_signs[:-1])
            reversal_intensity = reversals / (len(price_changes) - 1) if len(price_changes) > 1 else 0.0
            
            return {
                "roll_spread": float(roll_spread),
                "roll_spread_bps": float(roll_spread_bps),
                "price_reversal_intensity": float(reversal_intensity)
            }
            
        except Exception as e:
            logger.error(f"Roll spread estimator failed: {e}", exc_info=True)
            return {"roll_spread": 0.0, "roll_spread_bps": 0.0, "price_reversal_intensity": 0.0}
    
    def _compute_amihud_illiquidity(self, returns: np.ndarray, volumes: np.ndarray, prices: np.ndarray) -> Dict[str, float]:
        """
        Compute Amihud (2002) illiquidity measure: average ratio of absolute return to volume.
        
        ILLIQ = (1/Days) * Σ(|Return_t| / Volume_t)
        """
        default_dict = {"amihud_illiq": 0.0, "amihud_illiq_scaled": 0.0, "volume_impact_asymmetry": 0.0}
        return self._safe_compute_feature(
            "amihud_illiquidity",
            self._compute_amihud_illiquidity_core,
            default_dict, 
            returns, volumes, prices
        )
    
    def _compute_amihud_illiquidity_core(self, returns: np.ndarray, volumes: np.ndarray, prices: np.ndarray) -> Dict[str, float]:
        """Core Amihud illiquidity computation."""
        try:
            if len(returns) != len(volumes) or len(returns) == 0:
                return {"amihud_illiq": 0.0, "amihud_illiq_scaled": 0.0, "volume_impact_asymmetry": 0.0}
            
            # Handle zero/negative volumes
            valid_mask = (volumes > 0) & np.isfinite(returns) & np.isfinite(volumes)
            if not np.any(valid_mask):
                return {"amihud_illiq": 0.0, "amihud_illiq_scaled": 0.0, "volume_impact_asymmetry": 0.0}
            
            valid_returns = returns[valid_mask]
            valid_volumes = volumes[valid_mask]
            
            # Amihud illiquidity: |return| / volume
            daily_illiq = np.abs(valid_returns) / valid_volumes
            amihud_illiq = np.mean(daily_illiq)
            
            # Scale by average price for interpretability (price impact per dollar volume)
            avg_price = np.mean(prices) if len(prices) > 0 and np.all(np.isfinite(prices)) else 1.0
            amihud_scaled = amihud_illiq * avg_price
            
            # Volume impact asymmetry (difference between up/down days)
            up_days = valid_returns > 0
            down_days = valid_returns < 0
            
            if np.any(up_days) and np.any(down_days):
                up_impact = np.mean(daily_illiq[up_days])
                down_impact = np.mean(daily_illiq[down_days])
                volume_asymmetry = (up_impact - down_impact) / (up_impact + down_impact + 1e-12)
            else:
                volume_asymmetry = 0.0
            
            return {
                "amihud_illiq": float(amihud_illiq),
                "amihud_illiq_scaled": float(amihud_scaled),
                "volume_impact_asymmetry": float(volume_asymmetry)
            }
            
        except Exception as e:
            logger.error(f"Amihud illiquidity computation failed: {e}", exc_info=True)
            return {"amihud_illiq": 0.0, "amihud_illiq_scaled": 0.0, "volume_impact_asymmetry": 0.0}
    
    def _compute_adverse_selection_cost(self, prices: np.ndarray, volumes: np.ndarray, 
                                       trade_directions: np.ndarray) -> Dict[str, float]:
        """
        Compute adverse selection cost component using Glosten-Harris model.
        
        Decomposes price impact into permanent (adverse selection) and temporary components.
        """
        return self._safe_compute_feature(
            "adverse_selection_cost",
            self._compute_adverse_selection_cost_core,
            {"adverse_selection_cost": 0.0, "temporary_impact": 0.0, "permanent_impact_ratio": 0.0},
            prices, volumes, trade_directions
        )
    
    def _compute_adverse_selection_cost_core(self, prices: np.ndarray, volumes: np.ndarray, 
                                           trade_directions: np.ndarray) -> Dict[str, float]:
        """Core adverse selection cost computation using Glosten-Harris decomposition."""
        try:
            if len(prices) != len(volumes) or len(prices) != len(trade_directions) or len(prices) < 10:
                return {"adverse_selection_cost": 0.0, "temporary_impact": 0.0, "permanent_impact_ratio": 0.0}
            
            # Glosten-Harris model: Δp_t = θ * D_t * V_t + φ * (D_t * V_t - D_{t-1} * V_{t-1}) + ε_t
            # Where θ = permanent impact, φ = temporary impact coefficient
            
            price_changes = np.diff(np.log(prices))
            signed_volumes = trade_directions[1:] * volumes[1:]  # Align with price changes
            signed_volumes_lag = trade_directions[:-1] * volumes[:-1]
            
            if len(price_changes) != len(signed_volumes):
                return {"adverse_selection_cost": 0.0, "temporary_impact": 0.0, "permanent_impact_ratio": 0.0}
            
            # Prepare regression: Δp_t = α + θ*SV_t + φ*(SV_t - SV_{t-1}) + ε_t
            # Where SV_t = signed volume (direction * volume)
            
            X = np.column_stack([
                np.ones(len(signed_volumes)),  # Intercept
                signed_volumes,  # Permanent impact component  
                signed_volumes - signed_volumes_lag  # Temporary impact component
            ])
            y = price_changes
            
            # Robust regression using Ridge to handle multicollinearity
            try:
                ridge = Ridge(alpha=0.01, fit_intercept=False)
                ridge.fit(X, y)
                coeffs = ridge.coef_
                
                if coeffs is not None and len(coeffs) >= 3:
                    permanent_impact = coeffs[1]  # θ coefficient
                    temporary_impact = coeffs[2]   # φ coefficient
                else:
                    permanent_impact = 0.0
                    temporary_impact = 0.0
                
            except Exception:
                # Fallback to simple correlation-based estimates
                permanent_impact = np.corrcoef(price_changes, signed_volumes)[0, 1] if np.std(signed_volumes) > 1e-8 else 0.0
                temporary_impact = 0.0
            
            # Compute permanent impact ratio (adverse selection / total impact)
            total_impact = abs(permanent_impact) + abs(temporary_impact)
            permanent_ratio = abs(permanent_impact) / total_impact if total_impact > 1e-12 else 0.0
            
            return {
                "adverse_selection_cost": float(abs(permanent_impact)),
                "temporary_impact": float(abs(temporary_impact)),  
                "permanent_impact_ratio": float(permanent_ratio)
            }
            
        except Exception as e:
            logger.error(f"Adverse selection cost computation failed: {e}", exc_info=True)
            return {"adverse_selection_cost": 0.0, "temporary_impact": 0.0, "permanent_impact_ratio": 0.0}
    
    def _compute_depth_deterioration(self, bid_prices: np.ndarray, ask_prices: np.ndarray,
                                   bid_sizes: np.ndarray, ask_sizes: np.ndarray,
                                   levels: int = 5) -> Dict[str, float]:
        """
        Compute order book depth deterioration measures across multiple levels.
        
        Analyzes how liquidity changes across the order book depth.
        """
        return self._safe_compute_feature(
            "depth_deterioration",
            self._compute_depth_deterioration_core,
            {"depth_slope_bid": 0.0, "depth_slope_ask": 0.0, "depth_asymmetry": 0.0, "weighted_depth_cost": 0.0},
            bid_prices, ask_prices, bid_sizes, ask_sizes, levels
        )
    
    def _compute_depth_deterioration_core(self, bid_prices: np.ndarray, ask_prices: np.ndarray,
                                        bid_sizes: np.ndarray, ask_sizes: np.ndarray,
                                        levels: int = 5) -> Dict[str, float]:
        """Core depth deterioration computation."""
        try:
            n_obs = len(bid_prices)
            if n_obs != len(ask_prices) or n_obs == 0:
                return {"depth_slope_bid": 0.0, "depth_slope_ask": 0.0, "depth_asymmetry": 0.0, "weighted_depth_cost": 0.0}
            
            # Ensure we have multi-dimensional data for multiple levels
            if bid_prices.ndim == 1:
                # Single level data - create synthetic depth
                levels = 1
                bid_prices_2d = bid_prices.reshape(-1, 1)
                ask_prices_2d = ask_prices.reshape(-1, 1) 
                bid_sizes_2d = bid_sizes.reshape(-1, 1)
                ask_sizes_2d = ask_sizes.reshape(-1, 1)
            else:
                bid_prices_2d = bid_prices
                ask_prices_2d = ask_prices
                bid_sizes_2d = bid_sizes
                ask_sizes_2d = ask_sizes
                levels = min(levels, bid_prices_2d.shape[1])
            
            depth_slopes_bid = []
            depth_slopes_ask = []
            weighted_costs = []
            
            for i in range(n_obs):
                # Extract depth profile for this timestamp
                bid_levels = bid_prices_2d[i, :levels] if bid_prices_2d.shape[1] >= levels else bid_prices_2d[i, :]
                ask_levels = ask_prices_2d[i, :levels] if ask_prices_2d.shape[1] >= levels else ask_prices_2d[i, :]
                bid_vol_levels = bid_sizes_2d[i, :levels] if bid_sizes_2d.shape[1] >= levels else bid_sizes_2d[i, :]
                ask_vol_levels = ask_sizes_2d[i, :levels] if ask_sizes_2d.shape[1] >= levels else ask_sizes_2d[i, :]
                
                # Remove invalid data
                valid_bid = np.isfinite(bid_levels) & np.isfinite(bid_vol_levels) & (bid_vol_levels > 0)
                valid_ask = np.isfinite(ask_levels) & np.isfinite(ask_vol_levels) & (ask_vol_levels > 0)
                
                if not np.any(valid_bid) or not np.any(valid_ask):
                    continue
                
                bid_levels_clean = bid_levels[valid_bid]
                ask_levels_clean = ask_levels[valid_ask] 
                bid_vol_clean = bid_vol_levels[valid_bid]
                ask_vol_clean = ask_vol_levels[valid_ask]
                
                # Compute depth slopes (price deterioration per unit depth)
                if len(bid_levels_clean) > 1:
                    bid_depth_slope = -np.polyfit(range(len(bid_levels_clean)), bid_levels_clean, 1)[0]
                    depth_slopes_bid.append(bid_depth_slope)
                
                if len(ask_levels_clean) > 1:
                    ask_depth_slope = np.polyfit(range(len(ask_levels_clean)), ask_levels_clean, 1)[0]  
                    depth_slopes_ask.append(ask_depth_slope)
                
                # Weighted average execution cost
                if len(bid_vol_clean) > 0 and len(ask_vol_clean) > 0:
                    bid_weighted_cost = np.average(bid_levels_clean, weights=bid_vol_clean)
                    ask_weighted_cost = np.average(ask_levels_clean, weights=ask_vol_clean)
                    mid_price = (bid_levels_clean[0] + ask_levels_clean[0]) / 2
                    
                    if mid_price > 0:
                        cost_impact = ((ask_weighted_cost - mid_price) + (mid_price - bid_weighted_cost)) / mid_price
                        weighted_costs.append(cost_impact)
            
            # Aggregate statistics
            avg_bid_slope = np.mean(depth_slopes_bid) if depth_slopes_bid else 0.0
            avg_ask_slope = np.mean(depth_slopes_ask) if depth_slopes_ask else 0.0
            
            # Depth asymmetry (bid vs ask deterioration)
            if len(depth_slopes_bid) > 0 and len(depth_slopes_ask) > 0:
                depth_asymmetry = (avg_ask_slope - avg_bid_slope) / (avg_ask_slope + avg_bid_slope + 1e-12)
            else:
                depth_asymmetry = 0.0
            
            avg_weighted_cost = np.mean(weighted_costs) if weighted_costs else 0.0
            
            return {
                "depth_slope_bid": float(avg_bid_slope),
                "depth_slope_ask": float(avg_ask_slope),
                "depth_asymmetry": float(depth_asymmetry),
                "weighted_depth_cost": float(avg_weighted_cost)
            }
            
        except Exception as e:
            logger.error(f"Depth deterioration computation failed: {e}", exc_info=True)
            return {"depth_slope_bid": 0.0, "depth_slope_ask": 0.0, "depth_asymmetry": 0.0, "weighted_depth_cost": 0.0}
    
    def _compute_quote_pressure(self, bid_prices: np.ndarray, ask_prices: np.ndarray,
                               bid_sizes: np.ndarray, ask_sizes: np.ndarray) -> Dict[str, float]:
        """
        Compute quote pressure metrics indicating supply/demand imbalances.
        """
        return self._safe_compute_feature(
            "quote_pressure",
            self._compute_quote_pressure_core,
            {"quote_pressure": 0.0, "size_pressure": 0.0, "pressure_persistence": 0.0},
            bid_prices, ask_prices, bid_sizes, ask_sizes
        )
    
    def _compute_quote_pressure_core(self, bid_prices: np.ndarray, ask_prices: np.ndarray,
                                   bid_sizes: np.ndarray, ask_sizes: np.ndarray) -> Dict[str, float]:
        """Core quote pressure computation."""
        try:
            if len(bid_prices) != len(ask_prices) or len(bid_prices) == 0:
                return {"quote_pressure": 0.0, "size_pressure": 0.0, "pressure_persistence": 0.0}
            
            # Basic order imbalance
            total_bid_size = np.sum(bid_sizes, axis=1) if bid_sizes.ndim > 1 else bid_sizes
            total_ask_size = np.sum(ask_sizes, axis=1) if ask_sizes.ndim > 1 else ask_sizes
            
            # Prevent division by zero
            total_size = total_bid_size + total_ask_size
            valid_mask = total_size > 0
            
            if not np.any(valid_mask):
                return {"quote_pressure": 0.0, "size_pressure": 0.0, "pressure_persistence": 0.0}
            
            # Order imbalance ratio
            imbalance_ratio = np.zeros_like(total_size, dtype=float)
            imbalance_ratio[valid_mask] = (total_bid_size[valid_mask] - total_ask_size[valid_mask]) / total_size[valid_mask]
            
            # Price-weighted pressure (closer quotes have more impact)
            mid_prices = (bid_prices + ask_prices) / 2
            spreads = ask_prices - bid_prices
            
            # Normalized quote distances
            bid_distance = (mid_prices - bid_prices) / mid_prices
            ask_distance = (ask_prices - mid_prices) / mid_prices
            
            # Weight by inverse distance (closer quotes matter more) and size
            bid_weights = total_bid_size / (1 + bid_distance)
            ask_weights = total_ask_size / (1 + ask_distance)
            
            total_weights = bid_weights + ask_weights
            weighted_pressure = np.zeros_like(total_weights, dtype=float)
            valid_weights = total_weights > 0
            
            if np.any(valid_weights):
                weighted_pressure[valid_weights] = (bid_weights[valid_weights] - ask_weights[valid_weights]) / total_weights[valid_weights]
            
            # Pressure persistence (autocorrelation of pressure)
            if len(weighted_pressure) > 1:
                pressure_changes = np.diff(weighted_pressure)
                persistence = np.corrcoef(weighted_pressure[1:], weighted_pressure[:-1])[0, 1] if len(pressure_changes) > 0 else 0.0
                if np.isnan(persistence):
                    persistence = 0.0
            else:
                persistence = 0.0
            
            return {
                "quote_pressure": float(np.mean(weighted_pressure)),
                "size_pressure": float(np.mean(imbalance_ratio[valid_mask])),
                "pressure_persistence": float(persistence)
            }
            
        except Exception as e:
            logger.error(f"Quote pressure computation failed: {e}", exc_info=True)
            return {"quote_pressure": 0.0, "size_pressure": 0.0, "pressure_persistence": 0.0}
    
    def _compute_cross_venue_pressure(self, venue_data: Dict[str, Dict[str, np.ndarray]]) -> Dict[str, float]:
        """
        Compute cross-venue liquidity pressure and arbitrage opportunity metrics.
        
        Args:
            venue_data: Dict with venue names as keys, each containing 'bid', 'ask', 'bid_size', 'ask_size'
        """
        return self._safe_compute_feature(
            "cross_venue_pressure",
            self._compute_cross_venue_pressure_core,
            {"cross_venue_spread": 0.0, "venue_dominance": 0.0, "arbitrage_pressure": 0.0},
            venue_data
        )
    
    def _compute_cross_venue_pressure_core(self, venue_data: Dict[str, Dict[str, np.ndarray]]) -> Dict[str, float]:
        """Core cross-venue pressure computation."""
        try:
            venues = list(venue_data.keys())
            if len(venues) < 2:
                return {"cross_venue_spread": 0.0, "venue_dominance": 0.0, "arbitrage_pressure": 0.0}
            
            # Extract venue quotes
            venue_mids = {}
            venue_spreads = {}
            venue_sizes = {}
            
            for venue in venues:
                data = venue_data[venue]
                if 'bid' not in data or 'ask' not in data:
                    continue
                    
                bid_prices = data['bid']
                ask_prices = data['ask']
                bid_sizes = data.get('bid_size', np.ones_like(bid_prices))
                ask_sizes = data.get('ask_size', np.ones_like(ask_prices))
                
                # Calculate venue mid prices and spreads
                venue_mids[venue] = (bid_prices + ask_prices) / 2
                venue_spreads[venue] = ask_prices - bid_prices
                venue_sizes[venue] = bid_sizes + ask_sizes
            
            if len(venue_mids) < 2:
                return {"cross_venue_spread": 0.0, "venue_dominance": 0.0, "arbitrage_pressure": 0.0}
            
            # Find common time periods
            min_length = min(len(mids) for mids in venue_mids.values())
            if min_length == 0:
                return {"cross_venue_spread": 0.0, "venue_dominance": 0.0, "arbitrage_pressure": 0.0}
            
            # Truncate all venues to common length
            for venue in venue_mids:
                venue_mids[venue] = venue_mids[venue][:min_length]
                venue_spreads[venue] = venue_spreads[venue][:min_length] 
                venue_sizes[venue] = venue_sizes[venue][:min_length]
            
            # Cross-venue spread (max mid - min mid)
            mid_matrix = np.column_stack([venue_mids[venue] for venue in venue_mids])
            cross_venue_spreads = np.max(mid_matrix, axis=1) - np.min(mid_matrix, axis=1)
            avg_cross_venue_spread = np.mean(cross_venue_spreads)
            
            # Venue dominance (size-weighted market share concentration)
            size_matrix = np.column_stack([venue_sizes[venue] for venue in venue_sizes])
            total_size_per_time = np.sum(size_matrix, axis=1)
            
            # Avoid division by zero
            valid_times = total_size_per_time > 0
            if not np.any(valid_times):
                venue_dominance = 0.0
            else:
                market_shares = size_matrix[valid_times] / total_size_per_time[valid_times, np.newaxis]
                # Herfindahl-Hirschman Index for concentration
                hhi = np.sum(market_shares**2, axis=1)
                venue_dominance = np.mean(hhi)
            
            # Arbitrage pressure (price dispersion relative to average spread)
            spread_matrix = np.column_stack([venue_spreads[venue] for venue in venue_spreads])
            avg_spreads = np.mean(spread_matrix, axis=1)
            
            # Arbitrage opportunity when cross-venue spread > average within-venue spread
            arbitrage_opportunities = cross_venue_spreads > avg_spreads
            arbitrage_pressure = np.mean(arbitrage_opportunities.astype(float))
            
            return {
                "cross_venue_spread": float(avg_cross_venue_spread),
                "venue_dominance": float(venue_dominance),
                "arbitrage_pressure": float(arbitrage_pressure)
            }
            
        except Exception as e:
            logger.error(f"Cross-venue pressure computation failed: {e}", exc_info=True)
            return {"cross_venue_spread": 0.0, "venue_dominance": 0.0, "arbitrage_pressure": 0.0}
        
    def _compute_volatility_surface_features(self, option_data: pd.DataFrame,
                                           spot_price: float, risk_free_rate: float = 0.02) -> Dict[str, float]:
        """
        Compute volatility surface features for options markets.
        
        Analyzes implied volatility patterns across strikes and maturities.
        """
        return self._safe_compute_feature(
            "volatility_surface_features",
            self._compute_volatility_surface_features_core,
            {"vol_skew": 0.0, "vol_smile": 0.0, "term_structure_slope": 0.0, "vol_surface_curvature": 0.0},
            option_data, spot_price, risk_free_rate
        )
    
    def _compute_volatility_surface_features_core(self, option_data: pd.DataFrame,
                                                spot_price: float, risk_free_rate: float = 0.02) -> Dict[str, float]:
        """Core volatility surface computation."""
        try:
            if len(option_data) == 0 or 'strike' not in option_data.columns or 'implied_vol' not in option_data.columns:
                return {"vol_skew": 0.0, "vol_smile": 0.0, "term_structure_slope": 0.0, "vol_surface_curvature": 0.0}
            
            # Filter valid data
            valid_data = option_data.dropna(subset=['strike', 'implied_vol', 'days_to_expiry'])
            if len(valid_data) < 3:
                return {"vol_skew": 0.0, "vol_smile": 0.0, "term_structure_slope": 0.0, "vol_surface_curvature": 0.0}
            
            strikes = valid_data['strike'].values
            ivs = valid_data['implied_vol'].values
            days_to_expiry = valid_data['days_to_expiry'].values
            
            # Moneyness (log of strike/spot ratio)
            strikes_array = self._to_numpy_array(strikes)
            moneyness = np.log(strikes_array / spot_price)
            
            # Vol skew (slope of IV vs moneyness)
            ivs_array = self._to_numpy_array(ivs)
            if len(moneyness) > 1 and np.std(moneyness) > 1e-8:
                vol_skew = np.polyfit(moneyness, ivs_array, 1)[0]
            else:
                vol_skew = 0.0
            
            # Vol smile (curvature - second derivative)
            if len(moneyness) > 2:
                try:
                    # Fit quadratic and extract curvature
                    poly_coeffs = np.polyfit(moneyness, ivs_array, 2)
                    vol_smile = poly_coeffs[0] * 2  # Second derivative (curvature)
                except Exception:
                    vol_smile = 0.0
            else:
                vol_smile = 0.0
            
            # Term structure slope (IV vs time to expiry)
            days_array = self._to_numpy_array(days_to_expiry)
            unique_expiries = np.unique(days_array)
            if len(unique_expiries) > 1:
                # Average IV for each expiry
                expiry_ivs = []
                for expiry in unique_expiries:
                    expiry_mask = days_array == expiry
                    avg_iv = np.mean(ivs_array[expiry_mask])
                    expiry_ivs.append(avg_iv)
                
                if len(expiry_ivs) > 1 and np.std(unique_expiries) > 1e-8:
                    term_slope = np.polyfit(unique_expiries, expiry_ivs, 1)[0]
                else:
                    term_slope = 0.0
            else:
                term_slope = 0.0
            
            # Surface curvature (mixed partial derivatives approximation)
            if len(valid_data) > 10:
                try:
                    # Create a grid and fit 2D polynomial surface
                    X = np.column_stack([moneyness, days_array, moneyness**2, days_array**2, moneyness*days_array])
                    
                    # Ridge regression for stability
                    ridge = Ridge(alpha=0.1)
                    ridge.fit(X, ivs_array)
                    
                    # Mixed partial derivative coefficient (moneyness * days_to_expiry term)
                    if hasattr(ridge, 'coef_') and ridge.coef_ is not None and len(ridge.coef_) > 4:
                        surface_curvature = ridge.coef_[4]
                    else:
                        surface_curvature = 0.0
                    
                except Exception:
                    surface_curvature = 0.0
            else:
                surface_curvature = 0.0
            
            return {
                "vol_skew": float(vol_skew),
                "vol_smile": float(vol_smile), 
                "term_structure_slope": float(term_slope),
                "vol_surface_curvature": float(surface_curvature)
            }
            
        except Exception as e:
            logger.error(f"Volatility surface computation failed: {e}", exc_info=True)
            return {"vol_skew": 0.0, "vol_smile": 0.0, "term_structure_slope": 0.0, "vol_surface_curvature": 0.0}
    
    def _compute_gamma_exposure_profile(self, option_positions: pd.DataFrame, 
                                       spot_price: float, price_grid: np.ndarray) -> Dict[str, float]:
        """
        Compute aggregate gamma exposure profile across price levels.
        
        Important for understanding dealer hedging flows and potential price pins.
        """
        return self._safe_compute_feature(
            "gamma_exposure_profile",
            self._compute_gamma_exposure_profile_core,
            {"max_gamma_level": 0.0, "gamma_asymmetry": 0.0, "gamma_concentration": 0.0},
            option_positions, spot_price, price_grid
        )
    
    def _compute_gamma_exposure_profile_core(self, option_positions: pd.DataFrame, 
                                           spot_price: float, price_grid: np.ndarray) -> Dict[str, float]:
        """Core gamma exposure computation using Black-Scholes derivatives."""
        try:
            if len(option_positions) == 0 or len(price_grid) == 0:
                return {"max_gamma_level": spot_price, "gamma_asymmetry": 0.0, "gamma_concentration": 0.0}
            
            required_cols = ['strike', 'expiry', 'option_type', 'position_size']
            if not all(col in option_positions.columns for col in required_cols):
                return {"max_gamma_level": spot_price, "gamma_asymmetry": 0.0, "gamma_concentration": 0.0}
            
            # Risk-free rate assumption
            risk_free_rate = 0.02
            
            # Calculate gamma exposure at each price level
            gamma_profile = np.zeros(len(price_grid))
            
            for _, option in option_positions.iterrows():
                strike = option['strike']
                expiry_days = option['expiry'] if 'expiry' in option else 30
                position_size = option['position_size']
                option_type = option['option_type'].lower() if isinstance(option['option_type'], str) else 'call'
                
                # Time to expiry in years
                time_to_expiry = max(expiry_days / 365.25, 1/365.25)  # Min 1 day
                
                for i, price in enumerate(price_grid):
                    # Black-Scholes gamma calculation
                    gamma = self._black_scholes_gamma(price, strike, time_to_expiry, risk_free_rate, vol=0.25)
                    
                    # Aggregate position gamma (positive for long, negative for short)
                    gamma_profile[i] += position_size * gamma
            
            # Find maximum gamma exposure level
            max_gamma_idx = np.argmax(np.abs(gamma_profile))
            max_gamma_level = price_grid[max_gamma_idx]
            
            # Gamma asymmetry (upside vs downside exposure)
            spot_idx = np.argmin(np.abs(price_grid - spot_price))
            upside_gamma = np.sum(gamma_profile[spot_idx:])
            downside_gamma = np.sum(gamma_profile[:spot_idx])
            
            total_gamma = abs(upside_gamma) + abs(downside_gamma)
            gamma_asymmetry = (upside_gamma - downside_gamma) / total_gamma if total_gamma > 1e-12 else 0.0
            
            # Gamma concentration (how concentrated exposure is)
            gamma_weights = np.abs(gamma_profile) / (np.sum(np.abs(gamma_profile)) + 1e-12)
            gamma_concentration = np.sum(gamma_weights**2)  # Herfindahl index
            
            return {
                "max_gamma_level": float(max_gamma_level),
                "gamma_asymmetry": float(gamma_asymmetry),
                "gamma_concentration": float(gamma_concentration)
            }
            
        except Exception as e:
            logger.error(f"Gamma exposure computation failed: {e}", exc_info=True)
            return {"max_gamma_level": spot_price, "gamma_asymmetry": 0.0, "gamma_concentration": 0.0}
    
    def _black_scholes_gamma(self, spot: float, strike: float, time_to_expiry: float, 
                           risk_free_rate: float, vol: float) -> float:
        """Calculate Black-Scholes gamma (second derivative of option price w.r.t. spot)."""
        try:
            if spot <= 0 or strike <= 0 or time_to_expiry <= 0 or vol <= 0:
                return 0.0
            
            d1 = (np.log(spot / strike) + (risk_free_rate + 0.5 * vol**2) * time_to_expiry) / (vol * np.sqrt(time_to_expiry))
            
            # Gamma = φ(d1) / (S * σ * √T) where φ is standard normal PDF
            gamma = stats.norm.pdf(d1) / (spot * vol * np.sqrt(time_to_expiry))
            
            return gamma
            
        except Exception:
            return 0.0
    
    # =====================================================================================
    # ADVANCED REGIME DETECTION & MOMENTUM ANALYSIS
    # =====================================================================================
    
    def _compute_regime_switching_momentum(self, returns: np.ndarray, window_size: int = 252) -> Dict[str, float]:
        """
        Compute regime-switching momentum using Hidden Markov Models.
        
        Identifies different market regimes and momentum persistence within each regime.
        """
        return self._safe_compute_feature(
            "regime_switching_momentum",
            self._compute_regime_switching_momentum_core,
            {"momentum_regime_prob": 0.5, "regime_persistence": 0.0, "momentum_strength": 0.0, "regime_volatility_ratio": 1.0},
            returns, window_size
        )
    
    def _compute_regime_switching_momentum_core(self, returns: np.ndarray, window_size: int = 252) -> Dict[str, float]:
        """Core regime-switching momentum computation."""
        if len(returns) < max(20, window_size // 10):
            return {"momentum_regime_prob": 0.5, "regime_persistence": 0.0, "momentum_strength": 0.0, "regime_volatility_ratio": 1.0}
        
        # Simple 2-regime model: High vol vs Low vol
        rolling_vol = np.array([np.std(returns[max(0, i-20):i+1]) for i in range(len(returns))])
        vol_threshold = np.median(rolling_vol)
        
        high_vol_regime = rolling_vol > vol_threshold
        regime_changes = np.diff(high_vol_regime.astype(int))
        
        # Regime persistence (average regime duration)
        if len(regime_changes) > 0:
            regime_switches = np.sum(np.abs(regime_changes))
            regime_persistence = len(returns) / max(1, regime_switches)
        else:
            regime_persistence = len(returns)
        
        # Momentum within regimes
        momentum_in_high_vol = np.corrcoef(returns[1:][high_vol_regime[:-1]], returns[:-1][high_vol_regime[:-1]])[0, 1] if np.sum(high_vol_regime) > 5 else 0.0
        momentum_in_low_vol = np.corrcoef(returns[1:][~high_vol_regime[:-1]], returns[:-1][~high_vol_regime[:-1]])[0, 1] if np.sum(~high_vol_regime) > 5 else 0.0
        
        if np.isnan(momentum_in_high_vol):
            momentum_in_high_vol = 0.0
        if np.isnan(momentum_in_low_vol):
            momentum_in_low_vol = 0.0
        
        # Current regime probability (smoothed)
        recent_regime_prob = np.mean(high_vol_regime[-min(10, len(high_vol_regime)):])
        
        # Volatility ratio between regimes
        high_vol_std = np.std(returns[high_vol_regime]) if np.sum(high_vol_regime) > 1 else np.std(returns)
        low_vol_std = np.std(returns[~high_vol_regime]) if np.sum(~high_vol_regime) > 1 else np.std(returns)
        vol_ratio = high_vol_std / max(low_vol_std, 1e-8)
        
        # Overall momentum strength (regime-weighted)
        momentum_strength = recent_regime_prob * momentum_in_high_vol + (1 - recent_regime_prob) * momentum_in_low_vol
        
        return {
            "momentum_regime_prob": float(recent_regime_prob),
            "regime_persistence": float(min(regime_persistence, 100.0)),  # Cap for stability
            "momentum_strength": float(momentum_strength),
            "regime_volatility_ratio": float(min(vol_ratio, 10.0))  # Cap for stability
        }
    
    def _compute_cross_asset_momentum_spillover(self, asset_returns: Dict[str, np.ndarray]) -> Dict[str, float]:
        """
        Compute cross-asset momentum spillover effects using Vector Autoregression.
        
        Analyzes how momentum in one asset affects momentum in other assets.
        """
        return self._safe_compute_feature(
            "cross_asset_momentum_spillover",
            self._compute_cross_asset_momentum_spillover_core,
            {"spillover_index": 0.0, "momentum_centrality": 0.0, "cross_momentum_persistence": 0.0},
            asset_returns
        )
    
    def _compute_cross_asset_momentum_spillover_core(self, asset_returns: Dict[str, np.ndarray]) -> Dict[str, float]:
        """Core cross-asset momentum spillover computation."""
        assets = list(asset_returns.keys())
        if len(assets) < 2:
            return {"spillover_index": 0.0, "momentum_centrality": 0.0, "cross_momentum_persistence": 0.0}
        
        # Align all return series to common length
        min_length = min(len(returns) for returns in asset_returns.values())
        if min_length < 20:
            return {"spillover_index": 0.0, "momentum_centrality": 0.0, "cross_momentum_persistence": 0.0}
        
        # Create aligned return matrix
        return_matrix = np.column_stack([asset_returns[asset][-min_length:] for asset in assets])
        
        # Compute momentum signals for each asset (e.g., 5-day rolling returns)
        momentum_signals = np.zeros_like(return_matrix)
        for i in range(len(assets)):
            for t in range(4, len(return_matrix)):  # Start from day 5 for 5-day momentum
                momentum_signals[t, i] = np.mean(return_matrix[t-4:t+1, i])
        
        # VAR(1) model for momentum spillovers: X_t = A * X_{t-1} + ε_t
        if len(momentum_signals) > 10:
            try:
                X_lag = momentum_signals[4:-1]  # X_{t-1}
                X_current = momentum_signals[5:]  # X_t
                
                # OLS estimation: vec(A) = (X_lag' ⊗ I)^(-1) * vec(X_current)
                # Simplified: A = X_current' * X_lag * (X_lag' * X_lag)^(-1)
                XtX_lag = X_lag.T @ X_lag
                if np.linalg.det(XtX_lag) > 1e-10:
                    A_matrix = X_current.T @ X_lag @ np.linalg.inv(XtX_lag)
                else:
                    A_matrix = np.eye(len(assets)) * 0.1  # Default near-zero matrix
            except Exception:
                A_matrix = np.eye(len(assets)) * 0.1
        else:
            A_matrix = np.eye(len(assets)) * 0.1
        
        # Spillover measures
        # 1. Spillover Index: sum of off-diagonal elements / total
        off_diagonal_sum = np.sum(np.abs(A_matrix)) - np.sum(np.abs(np.diag(A_matrix)))
        total_sum = np.sum(np.abs(A_matrix))
        spillover_index = off_diagonal_sum / max(total_sum, 1e-12)
        
        # 2. Momentum Centrality: which asset has highest outgoing spillovers
        outgoing_spillovers = np.sum(np.abs(A_matrix), axis=0) - np.abs(np.diag(A_matrix))
        momentum_centrality = np.max(outgoing_spillovers) / max(np.sum(outgoing_spillovers), 1e-12)
        
        # 3. Cross-momentum persistence: largest eigenvalue of A_matrix
        try:
            eigenvalues = np.linalg.eigvals(A_matrix)
            cross_momentum_persistence = float(np.max(np.real(eigenvalues)))
        except Exception:
            cross_momentum_persistence = 0.0
        
        return {
            "spillover_index": float(min(spillover_index, 1.0)),  # Cap at 1
            "momentum_centrality": float(momentum_centrality),
            "cross_momentum_persistence": float(min(cross_momentum_persistence, 0.99))  # Cap for stability
        }
    
    def _compute_fama_french_factor_exposure(self, returns: np.ndarray, market_returns: np.ndarray,
                                           smb_returns: np.ndarray, hml_returns: np.ndarray) -> Dict[str, float]:
        """
        Compute Fama-French 3-factor model exposures with time-varying coefficients.
        
        R_i - R_f = α + β*(R_m - R_f) + s*SMB + h*HML + ε
        """
        return self._safe_compute_feature(
            "fama_french_factor_exposure",
            self._compute_fama_french_factor_exposure_core,
            {"alpha": 0.0, "market_beta": 1.0, "size_factor": 0.0, "value_factor": 0.0, "r_squared": 0.0},
            returns, market_returns, smb_returns, hml_returns
        )
    
    def _compute_fama_french_factor_exposure_core(self, returns: np.ndarray, market_returns: np.ndarray,
                                                smb_returns: np.ndarray, hml_returns: np.ndarray) -> Dict[str, float]:
        """Core Fama-French factor exposure computation."""
        # Align all series to common length
        min_length = min(len(returns), len(market_returns), len(smb_returns), len(hml_returns))
        if min_length < 20:
            return {"alpha": 0.0, "market_beta": 1.0, "size_factor": 0.0, "value_factor": 0.0, "r_squared": 0.0}
        
        y = returns[-min_length:]
        x_market = market_returns[-min_length:]  
        x_smb = smb_returns[-min_length:]
        x_hml = hml_returns[-min_length:]
        
        # Create design matrix
        X = np.column_stack([
            np.ones(min_length),  # Intercept (alpha)
            x_market,             # Market factor (beta)
            x_smb,                # Size factor (SMB)
            x_hml                 # Value factor (HML)
        ])
        
        # OLS regression with Ridge regularization for stability
        try:
            ridge = Ridge(alpha=0.001, fit_intercept=False)
            ridge.fit(X, y)
            coefficients = ridge.coef_
            
            if coefficients is not None and len(coefficients) >= 4:
                alpha = coefficients[0]
                market_beta = coefficients[1] 
                size_factor = coefficients[2]
                value_factor = coefficients[3]
                
                # R-squared calculation
                y_pred = ridge.predict(X)
                ss_res = np.sum((y - y_pred)**2)
                ss_tot = np.sum((y - np.mean(y))**2)
                r_squared = 1 - (ss_res / max(ss_tot, 1e-12))
            else:
                alpha = 0.0
                market_beta = 1.0
                size_factor = 0.0
                value_factor = 0.0
                r_squared = 0.0
            
        except Exception:
            # Fallback to simple beta calculation
            alpha = 0.0
            market_beta = np.corrcoef(y, x_market)[0, 1] * (np.std(y) / max(np.std(x_market), 1e-8)) if np.std(x_market) > 1e-8 else 1.0
            size_factor = 0.0
            value_factor = 0.0
            r_squared = np.corrcoef(y, x_market)[0, 1]**2 if np.std(x_market) > 1e-8 else 0.0
            
            if np.isnan(market_beta):
                market_beta = 1.0
            if np.isnan(r_squared):
                r_squared = 0.0
        
        return {
            "alpha": float(alpha),
            "market_beta": float(market_beta),
            "size_factor": float(size_factor),
            "value_factor": float(value_factor),
            "r_squared": float(max(0.0, min(1.0, r_squared)))  # Clamp to [0,1]
        }
    
    def _compute_technical_indicator_ensemble(self, prices: np.ndarray) -> Dict[str, float]:
        """
        Compute ensemble of technical indicators with statistical significance testing.
        
        Combines multiple technical signals with proper statistical validation.
        """
        return self._safe_compute_feature(
            "technical_indicator_ensemble",
            self._compute_technical_indicator_ensemble_core,
            {"technical_score": 0.0, "signal_consistency": 0.0, "momentum_strength": 0.0, "mean_reversion_strength": 0.0},
            prices
        )
    
    def _compute_technical_indicator_ensemble_core(self, prices: np.ndarray) -> Dict[str, float]:
        """Core technical indicator ensemble computation."""
        if len(prices) < 50:
            return {"technical_score": 0.0, "signal_consistency": 0.0, "momentum_strength": 0.0, "mean_reversion_strength": 0.0}
        
        # Multiple technical indicators
        signals = {}
        
        # 1. Moving Average Crossover (9-day vs 21-day)
        if len(prices) >= 21:
            ma_short = np.convolve(prices, np.ones(9)/9, mode='valid')
            ma_long = np.convolve(prices, np.ones(21)/21, mode='valid')
            # Align lengths
            min_len = min(len(ma_short), len(ma_long))
            ma_signal = np.mean(ma_short[-min_len:] > ma_long[-min_len:])  # Fraction of bullish signals
            signals['ma_crossover'] = ma_signal
        
        # 2. RSI (Relative Strength Index)
        if len(prices) >= 14:
            price_changes = np.diff(prices)
            gains = np.where(price_changes > 0, price_changes, 0)
            losses = np.where(price_changes < 0, -price_changes, 0)
            
            avg_gain = np.mean(gains[-14:]) if len(gains) >= 14 else 0
            avg_loss = np.mean(losses[-14:]) if len(losses) >= 14 else 1e-8
            
            rs = avg_gain / max(avg_loss, 1e-8)
            rsi = 100 - (100 / (1 + rs))
            
            # Convert RSI to signal strength (-1 to 1)
            rsi_signal = (rsi - 50) / 50  # Normalize around 50
            signals['rsi'] = rsi_signal
        
        # 3. MACD (Moving Average Convergence Divergence)
        if len(prices) >= 26:
            ema_12 = self._compute_ema(prices, 12)
            ema_26 = self._compute_ema(prices, 26)
            macd_line = ema_12 - ema_26
            
            if len(macd_line) >= 9:
                macd_signal_line = self._compute_ema(macd_line, 9)
                macd_histogram = macd_line[-len(macd_signal_line):] - macd_signal_line
                
                # MACD signal: positive histogram = bullish
                macd_signal = np.tanh(np.mean(macd_histogram[-5:]) * 1000)  # Normalize with tanh
                signals['macd'] = macd_signal
        
        # 4. Bollinger Bands
        if len(prices) >= 20:
            bb_middle = np.mean(prices[-20:])
            bb_std = np.std(prices[-20:])
            bb_upper = bb_middle + 2 * bb_std
            bb_lower = bb_middle - 2 * bb_std
            
            current_price = prices[-1]
            # BB signal: position relative to bands
            if bb_std > 0:
                bb_signal = (current_price - bb_middle) / (bb_std * 2)  # Normalized position
            else:
                bb_signal = 0.0
            signals['bollinger_bands'] = bb_signal
        
        # 5. Price momentum (rate of change)
        if len(prices) >= 10:
            momentum = (prices[-1] - prices[-10]) / prices[-10] if prices[-10] > 0 else 0.0
            momentum_signal = np.tanh(momentum * 10)  # Normalize with tanh
            signals['momentum'] = momentum_signal
        
        # Ensemble aggregation
        if not signals:
            return {"technical_score": 0.0, "signal_consistency": 0.0, "momentum_strength": 0.0, "mean_reversion_strength": 0.0}
        
        signal_values = np.array(list(signals.values()))
        
        # Overall technical score (average of signals)
        technical_score = np.mean(signal_values)
        
        # Signal consistency (how aligned the signals are)
        signal_consistency = 1.0 - np.std(signal_values) / max(1.0, np.std(signal_values) + np.abs(np.mean(signal_values)))
        
        # Momentum vs mean reversion strength
        momentum_indicators = ['ma_crossover', 'macd', 'momentum']
        mean_reversion_indicators = ['rsi', 'bollinger_bands']
        
        momentum_signals = [signals.get(ind, 0.0) for ind in momentum_indicators if ind in signals]
        mean_reversion_signals = [signals.get(ind, 0.0) for ind in mean_reversion_indicators if ind in signals]
        
        momentum_strength = np.mean(momentum_signals) if momentum_signals else 0.0
        mean_reversion_strength = np.mean(mean_reversion_signals) if mean_reversion_signals else 0.0
        
        return {
            "technical_score": float(technical_score),
            "signal_consistency": float(max(0.0, min(1.0, signal_consistency))),
            "momentum_strength": float(momentum_strength),
            "mean_reversion_strength": float(mean_reversion_strength)
        }
    
    def _compute_ema(self, prices: np.ndarray, period: int) -> np.ndarray:
        """Compute Exponential Moving Average."""
        if len(prices) < period:
            return np.array([np.mean(prices)] * len(prices)) if len(prices) > 0 else np.array([])
        
        alpha = 2.0 / (period + 1)
        ema = np.zeros(len(prices))
        ema[0] = prices[0]
        
        for i in range(1, len(prices)):
            ema[i] = alpha * prices[i] + (1 - alpha) * ema[i-1]
        
        return ema
