#!/usr/bin/env python3
"""
Compute Infrastructure Package

Provides sophisticated analytics and processing capabilities using Apache Spark
for enterprise-grade alpha generation and arbitrage detection.

Components:
- spark_alpha_engine.py: Main Spark-based alpha generation engine
- statistical_models.py: Advanced statistical models for alpha detection  
- arbitrage_detector.py: Multi-market arbitrage opportunity detection
- coverage_analyzer.py: Market coverage optimization and analysis

Integration:
Works with existing streaming bus, columnar (Arrow), and lakehouse (Iceberg) infrastructure.
"""

# Version info
__version__ = "1.0.0"
__author__ = "Satoshi Alpha Generation Team"

# Import main classes when Spark is available
try:
    from .spark_alpha_engine import SparkAlphaEngine, create_enterprise_alpha_engine
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("⚠️  Spark compute engine not available. Install PySpark: pip install pyspark")

__all__ = ['SparkAlphaEngine', 'create_enterprise_alpha_engine', 'SPARK_AVAILABLE']