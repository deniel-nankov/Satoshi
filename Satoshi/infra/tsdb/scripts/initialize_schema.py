#!/usr/bin/env python3
"""Initialize ClickHouse TSDB schema."""

import sys
import os
import time
import traceback

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from clickhouse_tsdb import QualityMonitoringTSDB, TSDBConfig
except ImportError as e:
    print(f"❌ Failed to import ClickHouse TSDB: {e}")
    print("Make sure you're running from the correct directory and dependencies are installed")
    sys.exit(1)

def wait_for_clickhouse(config, max_retries=30, delay=2):
    """Wait for ClickHouse to be ready."""
    print("⏳ Waiting for ClickHouse to be ready...")
    
    for i in range(max_retries):
        try:
            import clickhouse_connect
            client = clickhouse_connect.get_client(
                host=config.host,
                port=config.port,
                username=config.username,
                password=config.password
            )
            client.query("SELECT 1")
            client.close()
            print("✅ ClickHouse is ready!")
            return True
        except Exception as e:
            if i == max_retries - 1:
                print(f"❌ ClickHouse not ready after {max_retries} retries: {e}")
                return False
            print(f"   Attempt {i+1}/{max_retries} - ClickHouse not ready yet, waiting {delay}s...")
            time.sleep(delay)
    
    return False

def main():
    """Initialize TSDB schema."""
    print("🏗️ Initializing ClickHouse TSDB schema...")
    
    config = TSDBConfig(
        host="localhost",
        port=8123,
        database="satoshi_tsdb",
        username="default",
        password=""
    )
    
    # Wait for ClickHouse to be ready
    if not wait_for_clickhouse(config):
        sys.exit(1)
    
    try:
        # Initialize TSDB (creates schema automatically)
        print("📊 Creating TSDB instance and schema...")
        tsdb = QualityMonitoringTSDB(config)
        
        if tsdb.client:
            print("✅ Schema initialization completed successfully")
            
            # Test basic functionality
            print("🧪 Testing basic functionality...")
            
            # Test incident insertion
            test_incident = {
                'incident_id': 'SCHEMA_INIT_TEST',
                'class': 'Anomaly',
                'severity': 'info',
                'source_agent': 'schema_initializer',
                'source_topic': 'init.test',
                'correlation_id': 'test-correlation',
                'impacted_streams': ['test_stream'],
                'proposed_action': 'test_action',
                'evidence_ref': {'test': True}
            }
            
            success = tsdb.insert_incident(test_incident)
            if success:
                print("✅ Test incident inserted successfully")
            else:
                print("⚠️  Test incident insertion failed")
            
            # Test dashboard generation
            try:
                dashboard = tsdb.get_quality_pipeline_dashboard()
                if dashboard:
                    print("✅ Dashboard generation test passed")
                else:
                    print("⚠️  Dashboard generation test failed")
            except Exception as e:
                print(f"⚠️  Dashboard test failed: {e}")
            
            # Show table list
            try:
                tables = tsdb.client.query("SHOW TABLES").result_rows
                print(f"📊 Created {len(tables)} tables:")
                for table in tables:
                    print(f"   - {table[0]}")
            except Exception as e:
                print(f"⚠️  Failed to list tables: {e}")
            
            print("🎉 TSDB initialization completed successfully!")
            
        else:
            print("❌ Failed to connect to ClickHouse")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Schema initialization failed: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
