#!/usr/bin/env python3
"""
📊 Satoshi System Status Dashboard
Real-time monitoring of data engineering infrastructure
"""

import json
import subprocess
import time
from datetime import datetime

def get_topic_list():
    """Get list of Kafka topics"""
    try:
        result = subprocess.run([
            'docker', 'exec', 'kafka', 
            'kafka-topics', '--bootstrap-server', 'localhost:9092', '--list'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            topics = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
            return topics
        return []
    except (FileNotFoundError, OSError, subprocess.SubprocessError, subprocess.TimeoutExpired) as e:
        print(f"Warning: Could not get topic list: {e}")
        return []

def get_container_status():
    """Get Docker container status"""
    try:
        result = subprocess.run(['docker', 'ps', '--format', 'json'], 
                              capture_output=True, text=True)
        
        containers = []
        for line in result.stdout.strip().split('\n'):
            if line.strip():
                try:
                    container = json.loads(line)
                    containers.append(container)
                except (json.JSONDecodeError, ValueError) as e:
                    print(f"Warning: Could not parse container JSON: {e}")
                    continue
        return containers
    except (OSError, subprocess.SubprocessError) as e:
        print(f"Warning: Could not get container status: {e}")
        return []
    except:
        return []

def print_status():
    """Print current system status"""
    print("\n" + "="*60)
    print(f"🏛️  SATOSHI DATA ENGINEERING SYSTEM STATUS")
    print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # Container Status
    print("\n🐳 INFRASTRUCTURE STATUS:")
    containers = get_container_status()
    
    kafka_running = any('kafka' in c.get('Names', '') and 'kafka-ui' not in c.get('Names', '') 
                       for c in containers)
    zk_running = any('zookeeper' in c.get('Names', '') for c in containers)
    ui_running = any('kafka-ui' in c.get('Names', '') for c in containers)
    
    print(f"   {'✅' if kafka_running else '❌'} Kafka Broker")
    print(f"   {'✅' if zk_running else '❌'} ZooKeeper")
    print(f"   {'✅' if ui_running else '❌'} Kafka UI (http://localhost:8080)")
    
    # Topic Status
    topics = get_topic_list()
    print(f"\n📊 DATA TOPICS ({len(topics)} active):")
    
    if topics:
        raw_topics = [t for t in topics if t.startswith('raw_data')]
        clean_topics = [t for t in topics if t.startswith('clean')]
        incident_topics = [t for t in topics if t.startswith('incidents')]
        control_topics = [t for t in topics if t.startswith('control')]
        
        print(f"   📥 Raw Data Topics: {len(raw_topics)}")
        print(f"   ✨ Clean Topics: {len(clean_topics)}")
        print(f"   🚨 Incident Topics: {len(incident_topics)}")
        print(f"   🎛️  Control Topics: {len(control_topics)}")
        
        if len(topics) >= 10:
            print("   🎯 Status: READY FOR PRODUCTION")
        else:
            print("   ⚠️  Status: INCOMPLETE SETUP")
    else:
        print("   ❌ No topics found - run topic creation")
    
    # Service URLs
    print(f"\n🌐 MONITORING INTERFACES:")
    print(f"   📊 Kafka UI: http://localhost:8080")
    print(f"   📈 Grafana Dashboard: infra/monitoring/grafana-dashboard.json")
    print(f"   🔗 Kafka Bootstrap: localhost:9092")
    
    # Quick Commands
    print(f"\n🚀 QUICK COMMANDS:")
    print(f"   Demo: python3 tests/integration/final_production_demo.py")
    print(f"   Topics: ./create_topics.sh")
    print(f"   Runner: python3 run_system.py")
    
    print("="*60)

if __name__ == "__main__":
    print_status()