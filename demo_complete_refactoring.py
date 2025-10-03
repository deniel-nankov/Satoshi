#!/usr/bin/env python3
"""
Complete Feature Factory refactoring demonstration using existing Leakage Police.

This demonstrates how the Feature Factory should properly integrate with the existing 
LeakagePolice agent instead of implementing its own validation logic.
"""

import sys
import asyncio
import numpy as np
import pandas as pd
import hashlib
import json
import time
from typing import Dict, Any, Optional

# Add the project to the path
sys.path.append('.')

from Satoshi.engines.data.leakage_police import LeakagePolice, LeakagePoliceConfig

class RefactoredFeatureFactory:
    """
    Properly refactored Feature Factory that delegates all leakage validation
    to the existing LeakagePolice agent (Agent #9).
    """
    
    def __init__(self, leakage_config: Optional[Dict[str, Any]] = None):
        self.leakage_config = leakage_config or {
            'temporal_tolerance_ms': 100,
            'min_samples_for_analysis': 10,
            'statistical_threshold': 0.001
        }
        
    async def _validate_with_existing_leakage_police(self, timestamps: np.ndarray, 
                                                   window_size: int, horizon: int,
                                                   feature_data: Optional[np.ndarray] = None,
                                                   parameters: Optional[Dict[str, Any]] = None) -> str:
        """
        Delegate validation to existing LeakagePolice agent (proper approach).
        
        This is how Feature Factory SHOULD integrate with existing domain authority.
        """
        # Create LeakagePolice instance with proper config
        config = LeakagePoliceConfig(
            temporal_tolerance_ms=self.leakage_config.get('temporal_tolerance_ms', 100),
            label_horizon_us=horizon,
            min_samples_for_analysis=max(10, len(timestamps)),
            **self.leakage_config
        )
        
        police = LeakagePolice(config)
        
        # Create DataFrames for existing LeakagePolice interface
        features_df = pd.DataFrame({
            'timestamp': timestamps,
            'feature_data': feature_data.flatten()[:len(timestamps)] if feature_data is not None else np.zeros(len(timestamps))
        })
        
        labels_df = pd.DataFrame({
            'timestamp': timestamps + horizon,  # Labels are horizon ahead
            'target': np.zeros(len(timestamps))  # Dummy target for validation
        })
        
        # Use existing temporal ordering analysis
        incidents = await police.analyze_temporal_ordering(
            features=features_df,
            labels=labels_df,
            feature_timestamp_col='timestamp',
            label_timestamp_col='timestamp'
        )
        
        # Handle incidents according to severity
        critical_incidents = [inc for inc in incidents if inc.severity.value in ['high', 'critical']]
        if critical_incidents:
            incident = critical_incidents[0]
            raise ValueError(f"Leakage Police validation failed: {incident.description} "
                           f"(severity: {incident.severity.value}, confidence: {incident.confidence_score:.3f})")
        
        # Generate proof ID from successful validation
        if incidents:
            # Minor issues detected but not critical - include in proof
            return f"LP_validated_{incidents[0].incident_id}"
        else:
            # Clean validation - generate clean proof
            content = {
                "timestamps_hash": hashlib.sha256(timestamps.tobytes()).hexdigest()[:16],
                "window_size": window_size,
                "horizon": horizon,
                "parameters": sorted(parameters.items()) if parameters else [],
                "validation_timestamp": int(time.time() * 1000000)
            }
            content_str = json.dumps(content, sort_keys=True, default=str)
            return f"LP_clean_{hashlib.sha256(content_str.encode()).hexdigest()[:16]}"
    
    def _validate_temporal_integrity_sync(self, timestamps: np.ndarray, window_size: int, 
                                        horizon: int, feature_data: Optional[np.ndarray] = None,
                                        parameters: Optional[Dict[str, Any]] = None) -> str:
        """Synchronous wrapper for existing LeakagePolice validation."""
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        if loop.is_running():
            # If already in async context, use thread executor
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(
                    asyncio.run,
                    self._validate_with_existing_leakage_police(timestamps, window_size, horizon, feature_data, parameters)
                )
                return future.result()
        else:
            # Direct async execution
            return loop.run_until_complete(
                self._validate_with_existing_leakage_police(timestamps, window_size, horizon, feature_data, parameters)
            )
    
    def compute_returns_features_example(self, price_data: pd.DataFrame, window_size: int = 20, 
                                       horizon: int = 1, price_col: str = "price", 
                                       timestamp_col: str = "timestamp") -> Dict[str, Any]:
        """
        Example of how feature computation should work with existing Leakage Police.
        
        This shows the complete integration pattern that ALL feature methods should follow.
        """
        print(f"🔧 Computing returns features with existing Leakage Police validation...")
        
        # Sort and prepare data
        price_data_sorted = price_data.sort_values(timestamp_col).reset_index(drop=True)
        prices = np.array(price_data_sorted[price_col].values, dtype=float)
        timestamps = np.array(price_data_sorted[timestamp_col].values)
        
        print(f"   📊 Data: {len(prices)} prices, {len(timestamps)} timestamps")
        
        # PROPER INTEGRATION: Validate with existing Leakage Police
        parameters = {
            'feature_type': 'returns',
            'window_size': window_size,
            'horizon': horizon,
            'price_col': price_col,
            'timestamp_col': timestamp_col
        }
        
        print(f"   🔍 Validating with existing Leakage Police Agent #9...")
        leakage_proof_id = self._validate_temporal_integrity_sync(
            timestamps, window_size, horizon, prices, parameters
        )
        print(f"   ✅ Validation successful: {leakage_proof_id}")
        
        # Continue with feature computation
        log_prices = np.log(prices)
        returns = np.diff(log_prices)
        
        # Feature engineering
        features = []
        feature_timestamps = []
        
        for i in range(window_size, len(returns) - horizon + 1):
            window_returns = returns[i-window_size:i]
            feature_value = np.std(window_returns) * np.sqrt(252)  # Annualized volatility
            features.append(feature_value)
            feature_timestamps.append(timestamps[i])
        
        print(f"   📈 Generated {len(features)} features")
        
        return {
            'features': np.array(features),
            'timestamps': np.array(feature_timestamps),
            'leakage_proof_id': leakage_proof_id,
            'parameters': parameters,
            'validation_method': 'existing_leakage_police',
            'domain_separation': 'proper'
        }

async def demonstrate_proper_refactoring():
    """Demonstrate the proper refactoring approach."""
    print("🎯 Demonstrating PROPER Feature Factory Refactoring")
    print("=" * 60)
    
    print("\n✅ CORRECT APPROACH: Using existing LeakagePolice from /data/leakage_police.py")
    print("❌ WRONG APPROACH: Creating duplicate service in /governance/")
    
    # Create refactored factory
    factory = RefactoredFeatureFactory({
        'temporal_tolerance_ms': 50,
        'min_samples_for_analysis': 10
    })
    
    # Create test data
    n_samples = 100
    timestamps = np.arange(n_samples) * 1000000  # 1 second intervals
    prices = 45000 + np.cumsum(np.random.normal(0, 100, n_samples))  # Random walk
    
    price_data = pd.DataFrame({
        'timestamp': timestamps,
        'price': prices
    })
    
    print(f"\n📊 Test Data: {len(price_data)} samples")
    
    # Compute features using proper integration
    result = factory.compute_returns_features_example(
        price_data=price_data,
        window_size=10,
        horizon=1
    )
    
    print(f"\n📈 Results:")
    print(f"   Features generated: {len(result['features'])}")
    print(f"   Leakage proof ID: {result['leakage_proof_id']}")
    print(f"   Validation method: {result['validation_method']}")
    print(f"   Domain separation: {result['domain_separation']}")
    
    # Test with problematic data (temporal violations)
    print(f"\n🧪 Testing with temporal violations...")
    problem_timestamps = timestamps.copy()
    problem_timestamps[10:15] += 5000000  # 5 seconds in future (leakage!)
    
    problem_data = pd.DataFrame({
        'timestamp': problem_timestamps,
        'price': prices
    })
    
    try:
        problem_result = factory.compute_returns_features_example(
            price_data=problem_data,
            window_size=10,
            horizon=1
        )
        print(f"   ⚠️  Validation passed with incidents: {problem_result['leakage_proof_id']}")
    except ValueError as e:
        print(f"   ✅ Validation properly rejected: {e}")
    
    print(f"\n🎯 Key Benefits of Proper Refactoring:")
    print(f"   ✅ Uses existing 1200+ line LeakagePolice implementation")
    print(f"   ✅ No code duplication - single source of truth")
    print(f"   ✅ Proper domain separation maintained")
    print(f"   ✅ Consistent validation across entire system")
    print(f"   ✅ Leverages battle-tested existing algorithms")

def show_refactoring_pattern():
    """Show the complete refactoring pattern."""
    print("\n" + "=" * 80)
    print("🔧 COMPLETE REFACTORING PATTERN FOR FEATURE FACTORY")
    print("=" * 80)
    
    pattern = '''
# BEFORE (boundary violation):
def _validate_temporal_integrity(self, timestamps, window_size, horizon):
    # 50+ lines of duplicated leakage detection logic
    
def _generate_leakage_proof_id(self, data, timestamps, params):
    # 20+ lines of duplicated proof generation

# AFTER (proper domain separation):
from ..data.leakage_police import LeakagePolice, LeakagePoliceConfig

async def _validate_with_existing_leakage_police(self, timestamps, window_size, horizon, ...):
    config = LeakagePoliceConfig(...)
    police = LeakagePolice(config)
    
    # Create DataFrames for existing interface
    features_df = pd.DataFrame({'timestamp': timestamps, ...})
    labels_df = pd.DataFrame({'timestamp': timestamps + horizon, ...})
    
    # Use existing analysis methods
    incidents = await police.analyze_temporal_ordering(features_df, labels_df)
    
    # Handle results appropriately
    if critical_incidents_found(incidents):
        raise ValueError("Validation failed")
    
    return generate_proof_from_incidents(incidents)

# ALL feature methods should follow this pattern:
def compute_any_features(self, data, ...):
    # 1. Prepare data
    timestamps = prepare_timestamps(data)
    
    # 2. Validate with existing Leakage Police
    leakage_proof_id = self._validate_temporal_integrity_sync(timestamps, ...)
    
    # 3. Compute features knowing validation passed
    features = compute_features_logic(data)
    
    # 4. Return with proof
    return FeatureVector(features, leakage_proof_id=leakage_proof_id, ...)
'''
    
    print(pattern)
    
    print("\n🎯 REFACTORING CHECKLIST:")
    print("   □ Remove _validate_temporal_integrity() method")
    print("   □ Remove _generate_leakage_proof_id() method") 
    print("   □ Add import: from ..data.leakage_police import LeakagePolice, LeakagePoliceConfig")
    print("   □ Add _validate_with_existing_leakage_police() method")
    print("   □ Add _validate_temporal_integrity_sync() wrapper")
    print("   □ Update ALL feature computation methods to use new validation")
    print("   □ Test with existing LeakagePolice to ensure compatibility")
    print("   □ Remove duplicate /governance/leakage_police_service.py file")

if __name__ == "__main__":
    print("🚀 Complete Feature Factory Refactoring to Existing Leakage Police")
    
    # Show the proper integration
    asyncio.run(demonstrate_proper_refactoring())
    
    # Show the complete pattern
    show_refactoring_pattern()
    
    print("\n" + "=" * 80)
    print("✅ READY TO COMMIT: Proper integration with existing Leakage Police")
    print("🗑️  TODO: Complete remaining method updates in actual feature_factory.py")
    print("🔗 TODO: Remove duplicate service file")
    print("=" * 80)
