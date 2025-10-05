#!/usr/bin/env python3
"""
Topic Configuration Validator

Validates that all required topics for data ingestion layer are properly configured.
Auto-creates missing topics and provides proper exit codes for production deployment.
"""

import sys
import os
import asyncio
from pathlib import Path

# Add the bus module directory to path
sys.path.insert(0, str(Path(__file__).parent))

from streaming_bus import StreamingBus

async def main():
    print("🔍 Validating Data Ingestion Layer Topic Configuration...")
    
    config = {
        "bootstrap_servers": "localhost:9092",
        "enable_ssl": False,
        "enable_sasl": False
    }
    
    bus = StreamingBus(config)
    exit_code = 0
    
    try:
        # Check if all required topics are configured
        print("\n📋 Checking Required Topics:")
        validation_result = bus.validate_data_ingestion_topics()
        
        if validation_result:
            print("✅ All required topics are configured!")
        else:
            print("❌ Some required topics are missing!")
            
            # Attempt to auto-create missing topics
            print("\n🔧 Auto-creating missing topics...")
            try:
                await bus.create_topics_from_config()
                print("✅ Successfully created missing topics!")
                
                # Verify creation
                validation_result_after = bus.validate_data_ingestion_topics()
                if validation_result_after:
                    print("✅ Topic validation now passes!")
                else:
                    print("❌ Topic creation failed - some topics still missing!")
                    exit_code = 1
                    
            except Exception as e:
                print(f"❌ Failed to create topics: {e}")
                print("💡 Ensure Kafka is running and accessible")
                exit_code = 1
        
        # Show topic summary
        print("\n📊 Topic Configuration Summary:")
        summary = bus.get_topic_summary()
        total_topics = sum(summary.values())
        
        print(f"Total Topics: {total_topics}")
        print(f"  • Raw Data Topics: {summary['raw_data']}")
        print(f"  • Clean Data Topics: {summary['clean']}")
        print(f"  • Incident Topics: {summary['incidents']}")
        print(f"  • Control Topics: {summary['control']}")
        print(f"  • Other Topics: {summary['other']}")
        
        # Show detailed topic list
        print("\n📝 Configured Topics by Category:")
        
        all_topics = sorted(bus.topic_configs.keys())
        
        raw_data_topics = [t for t in all_topics if t.startswith("raw_data.")]
        clean_topics = [t for t in all_topics if t.startswith("clean.")]
        incident_topics = [t for t in all_topics if t.startswith("incidents.")]
        control_topics = [t for t in all_topics if t.startswith("control.")]
        other_topics = [t for t in all_topics if not any(t.startswith(prefix) for prefix in ["raw_data.", "clean.", "incidents.", "control."])]
        
        if raw_data_topics:
            print(f"\n🔥 Raw Data Topics ({len(raw_data_topics)}):")
            for topic in raw_data_topics:
                config = bus.topic_configs[topic]
                retention_days = config.retention_ms // 86400000 if config.retention_ms >= 86400000 else 0
                retention_hours = (config.retention_ms // 3600000) if config.retention_ms < 86400000 else 0
                retention_str = f"{retention_days}d" if retention_days > 0 else f"{retention_hours}h"
                print(f"  • {topic} ({config.partitions}p, {retention_str} retention)")
        
        if clean_topics:
            print(f"\n✨ Clean Data Topics ({len(clean_topics)}):")
            for topic in clean_topics:
                config = bus.topic_configs[topic]
                retention_days = config.retention_ms // 86400000 if config.retention_ms >= 86400000 else 0
                retention_hours = (config.retention_ms // 3600000) if config.retention_ms < 86400000 else 0
                retention_str = f"{retention_days}d" if retention_days > 0 else f"{retention_hours}h"
                print(f"  • {topic} ({config.partitions}p, {retention_str} retention)")
        
        if incident_topics:
            print(f"\n🚨 Incident Topics ({len(incident_topics)}):")
            for topic in incident_topics:
                config = bus.topic_configs[topic]
                retention_days = config.retention_ms // 86400000
                print(f"  • {topic} ({config.partitions}p, {retention_days}d retention)")
        
        if control_topics:
            print(f"\n⚙️  Control Topics ({len(control_topics)}):")
            for topic in control_topics:
                config = bus.topic_configs[topic]
                retention_days = config.retention_ms // 86400000
                print(f"  • {topic} ({config.partitions}p, {retention_days}d retention)")
        
        if other_topics:
            print(f"\n📦 Other Topics ({len(other_topics)}):")
            for topic in other_topics:
                config = bus.topic_configs[topic]
                retention_days = config.retention_ms // 86400000
                print(f"  • {topic} ({config.partitions}p, {retention_days}d retention)")
        
        print(f"\n🎉 Topic configuration validation complete!")
        
        if exit_code == 0:
            print(f"💡 Start Kafka with: ./start-kafka-dev.sh")
            print(f"🚀 Test agents with: python test_data_ingestion_wiring.py")
        else:
            print(f"⚠️  Please check Kafka connectivity and retry")
            
    except Exception as e:
        print(f"❌ Validation failed with error: {e}")
        exit_code = 1
    
    finally:
        # Graceful shutdown
        try:
            await bus.graceful_shutdown()
        except Exception:
            pass  # Ignore shutdown errors
    
    return exit_code

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
