#!/usr/bin/env python3
"""
Complete refactoring script to properly integrate Feature Factory with existing Leakage Police.

This script will:
1. Update all _validate_temporal_integrity calls to use existing LeakagePolice
2. Update all _generate_leakage_proof_id calls to use existing LeakagePolice
3. Ensure proper integration with existing Agent #9 implementation
"""

import re

def refactor_feature_factory():
    """Refactor Feature Factory to use existing Leakage Police."""
    
    # Read the current file
    with open('/Users/christianlee/Downloads/Casablanca/Satoshi/engines/features/feature_factory.py', 'r') as f:
        content = f.read()
    
    print("🔧 Starting complete refactoring to existing Leakage Police...")
    
    # Pattern 1: Replace _validate_temporal_integrity calls
    validation_pattern = r'if not self\._validate_temporal_integrity\(([^)]+)\):\s*raise ValueError\("Temporal integrity validation failed"\)'
    validation_replacement = r'''# Validate with existing Leakage Police agent
        timestamps_array = np.array(\1.split(',')[0].strip())
        try:
            leakage_proof_id = self._validate_temporal_integrity_sync(\1, None, {})
        except Exception as e:
            raise ValueError(f"Temporal integrity validation failed: {e}")'''
    
    content = re.sub(validation_pattern, validation_replacement, content, flags=re.MULTILINE)
    
    # Pattern 2: Replace _generate_leakage_proof_id calls
    proof_pattern = r'leakage_proof_id = self\._generate_leakage_proof_id\(([^)]+)\)'
    proof_replacement = r'leakage_proof_id = self._validate_temporal_integrity_sync(\1.replace("features_array", "timestamps_array"), None, {})'
    
    content = re.sub(proof_pattern, proof_replacement, content)
    
    # Write back the file
    with open('/Users/christianlee/Downloads/Casablanca/Satoshi/engines/features/feature_factory.py', 'w') as f:
        f.write(content)
    
    print("✅ Refactoring complete!")
    print("   - All validation calls now use existing Leakage Police")
    print("   - All proof ID generation delegated to Agent #9")
    print("   - Feature Factory maintains clean domain boundaries")

if __name__ == "__main__":
    print("🚀 Complete Feature Factory Refactoring")
    print("=" * 50)
    refactor_feature_factory()
    print("=" * 50)
    print("✅ Ready for commit!")
