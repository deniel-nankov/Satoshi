#!/usr/bin/env python3
"""Comprehensive health check for ClickHouse TSDB."""

import sys
import os
import time
import asyncio
from datetime import datetime, timezone

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from clickhouse_tsdb import QualityMonitoringTSDB, TSDBConfig
except ImportError as e:
    print(f"❌ Failed to import ClickHouse TSDB: {e}")
    sys.exit(1)

async def run_health_check():
    """Run comprehensive health check."""
    print("🏥 Running ClickHouse TSDB Health Check...")
    print("=" * 50)
    
    config = TSDBConfig(
        host="localhost",
        port=8123,
        database="satoshi_tsdb",
        username="default",
        password=""
    )
    
    tsdb = QualityMonitoringTSDB(config)
    checks = []
    
    try:
        # Check 1: Connection
        if tsdb.client:
            checks.append(("✅", "ClickHouse connection established"))
            
            # Check 2: Database exists
            try:
                result = tsdb.client.query(f"EXISTS DATABASE {config.database}")
                if result.result_rows[0][0] == 1:
                    checks.append(("✅", f"Database '{config.database}' exists"))
                else:
                    checks.append(("❌", f"Database '{config.database}' not found"))
            except Exception as e:
                checks.append(("❌", f"Database check failed: {e}"))
            
            # Check 3: Tables exist
            try:
                tables = tsdb.client.query("SHOW TABLES").result_rows
                table_names = [table[0] for table in tables]
                expected_tables = [
                    'incidents', 'quality_agent_metrics', 'pipeline_sla',
                    'incident_patterns', 'quality_alerts', 'execution_telemetry',
                    'performance_metrics', 'stream_health', 'pnl_tracking'
                ]
                
                missing_tables = [t for t in expected_tables if t not in table_names]
                
                if not missing_tables:
                    checks.append(("✅", f"All {len(expected_tables)} tables present"))
                else:
                    checks.append(("❌", f"Missing tables: {missing_tables}"))
                    
            except Exception as e:
                checks.append(("❌", f"Table check failed: {e}"))
            
            # Check 4: Write performance test
            try:
                start_time = time.time()
                test_incident = {
                    'incident_id': f'HEALTH_CHECK_{int(time.time())}',
                    'class': 'Anomaly',
                    'severity': 'info',
                    'source_agent': 'health_check',
                    'source_topic': 'health.check',
                    'correlation_id': 'health-test',
                    'impacted_streams': ['health_stream'],
                    'proposed_action': 'health_check',
                    'evidence_ref': {'test': True, 'timestamp': start_time}
                }
                
                success = tsdb.insert_incident(test_incident)
                write_time_ms = (time.time() - start_time) * 1000
                
                if success:
                    checks.append(("✅", f"Write test passed ({write_time_ms:.1f}ms)"))
                else:
                    checks.append(("❌", "Write test failed"))
                    
            except Exception as e:
                checks.append(("❌", f"Write test failed: {e}"))
            
            # Check 5: Read performance test
            try:
                start_time = time.time()
                result = tsdb.client.query("SELECT COUNT(*) FROM incidents WHERE source_agent = 'health_check'")
                count = result.result_rows[0][0]
                read_time_ms = (time.time() - start_time) * 1000
                
                checks.append(("✅", f"Read test passed ({read_time_ms:.1f}ms, {count} records)"))
                
            except Exception as e:
                checks.append(("❌", f"Read test failed: {e}"))
            
            # Check 6: Dashboard functionality
            try:
                dashboard = tsdb.get_quality_pipeline_dashboard()
                if dashboard and 'pipeline_health' in dashboard:
                    checks.append(("✅", "Dashboard generation working"))
                else:
                    checks.append(("❌", "Dashboard generation failed"))
                    
            except Exception as e:
                checks.append(("❌", f"Dashboard test failed: {e}"))
            
            # Check 7: System metrics
            try:
                metrics = tsdb.get_comprehensive_metrics()
                if metrics:
                    checks.append(("✅", f"System metrics available ({len(metrics)} metrics)"))
                else:
                    checks.append(("❌", "System metrics unavailable"))
                    
            except Exception as e:
                checks.append(("❌", f"Metrics test failed: {e}"))
            
        else:
            checks.append(("❌", "ClickHouse connection failed"))
        
    except Exception as e:
        checks.append(("❌", f"Health check error: {e}"))
    
    finally:
        if tsdb:
            await tsdb.close()
    
    # Print results
    print("\n📊 Health Check Results:")
    print("-" * 30)
    
    passed = 0
    failed = 0
    
    for status, check in checks:
        print(f"{status} {check}")
        if status == "✅":
            passed += 1
        else:
            failed += 1
    
    # Summary
    print("-" * 30)
    print(f"📈 Summary: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("\n🎉 All health checks passed - TSDB is fully operational!")
        return True
    else:
        print(f"\n⚠️  {failed} health check(s) failed - investigate issues")
        return False

def main():
    """Run health check."""
    success = asyncio.run(run_health_check())
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
