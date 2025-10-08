#!/usr/bin/env python3
"""
🚀 Satoshi Data Engineering System - Production Runner
Comprehensive execution and monitoring for institutional data infrastructure
"""

import subprocess
import sys
import os
import time
import webbrowser
from pathlib import Path

def print_banner():
    print("""
🏛️  SATOSHI DATA ENGINEERING SYSTEM
══════════════════════════════════════════════════════════════
    Institutional-Grade Data Infrastructure
    📊 44 Topics | 🛡️  5 Quality Agents | ⚡ <1ms Latency
══════════════════════════════════════════════════════════════
""")

def check_docker():
    """Check if Docker is running"""
    try:
        result = subprocess.run(['docker', '--version'], capture_output=True, text=True)
        return result.returncode == 0
    except FileNotFoundError:
        return False

def start_infrastructure():
    """Start the Kafka infrastructure"""
    print("🚀 Starting Kafka Infrastructure...")
    
    # Stop any existing containers
    subprocess.run(['docker-compose', '-f', 'infra/bus/docker-compose.kafka.yml', 'down'], 
                  capture_output=True)
    
    # Start fresh
    result = subprocess.run(['docker-compose', '-f', 'infra/bus/docker-compose.kafka.yml', 'up', '-d'])
    
    if result.returncode == 0:
        print("✅ Kafka infrastructure started successfully")
        print("⏳ Waiting for services to initialize...")
        time.sleep(15)  # Give services time to start
        
        # Create topics
        print("📊 Creating data engineering topics...")
        
        # Validate script exists and is executable
        script_path = './create_topics.sh'
        if not os.path.exists(script_path):
            print(f"❌ Error: Script {script_path} not found")
            return False
        
        if not os.access(script_path, os.X_OK):
            print(f"❌ Error: Script {script_path} is not executable")
            return False
        
        try:
            result = subprocess.run([script_path], check=True, capture_output=True, text=True)
            print("✅ All 11 data engineering topics created")
            return True
        except FileNotFoundError as e:
            print(f"❌ Error: Could not find script: {e}")
            return False
        except subprocess.CalledProcessError as e:
            print(f"❌ Error: Script failed with exit code {e.returncode}")
            if e.stderr:
                print(f"Error output: {e.stderr}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error running script: {e}")
            return False
    else:
        print("❌ Failed to start Kafka infrastructure")
        return False

def run_data_engineering_demo():
    """Run the comprehensive data engineering demonstration"""
    print("\n🎯 Running Production Data Engineering Demo...")
    result = subprocess.run(['python3', 'tests/integration/final_production_demo.py'])
    return result.returncode == 0

def open_monitoring_dashboards():
    """Open monitoring interfaces"""
    print("\n📊 Opening Monitoring Dashboards...")
    
    # Kafka UI
    print("🌐 Opening Kafka UI at http://localhost:8080")
    webbrowser.open('http://localhost:8080')
    
    print("📊 Grafana dashboard available at:")
    print("   File: infra/monitoring/grafana-dashboard.json")
    print("   Import this JSON file into your Grafana instance")

def show_system_status():
    """Show current system status"""
    print("\n📈 System Status:")
    
    # Check containers
    result = subprocess.run(['docker', 'ps', '--filter', 'name=kafka'], 
                          capture_output=True, text=True)
    
    if 'kafka' in result.stdout:
        print("✅ Kafka: Running")
    else:
        print("❌ Kafka: Stopped")
    
    if 'zookeeper' in result.stdout:
        print("✅ ZooKeeper: Running")
    else:
        print("❌ ZooKeeper: Stopped")
    
    if 'kafka-ui' in result.stdout:
        print("✅ Kafka UI: Running (http://localhost:8080)")
    else:
        print("❌ Kafka UI: Stopped")

def show_available_tests():
    """Show available integration tests"""
    print("\n🧪 Available Tests:")
    print("1. tests/integration/final_production_demo.py - Complete A+ system demo")
    print("2. tests/integration/comprehensive_data_layer_test.py - Full layer validation")
    print("3. tests/integration/compression_validation_test.py - Compression testing")
    print("4. tests/integration/agent_functionality_test.py - Quality agent testing")

def main():
    print_banner()
    
    if not check_docker():
        print("❌ Docker is not available. Please install Docker and try again.")
        sys.exit(1)
    
    while True:
        print("\n🎮 Choose an action:")
        print("1. 🚀 Start Complete System (Infrastructure + Demo)")
        print("2. 🏗️  Start Infrastructure Only")
        print("3. 🎯 Run Data Engineering Demo")
        print("4. 📊 Open Monitoring Dashboards")
        print("5. 📈 Show System Status")
        print("6. 🧪 Show Available Tests")
        print("7. 🛑 Stop All Services")
        print("8. ❌ Exit")
        
        choice = input("\nEnter your choice (1-8): ").strip()
        
        if choice == '1':
            if start_infrastructure():
                run_data_engineering_demo()
                open_monitoring_dashboards()
        
        elif choice == '2':
            start_infrastructure()
        
        elif choice == '3':
            run_data_engineering_demo()
        
        elif choice == '4':
            open_monitoring_dashboards()
        
        elif choice == '5':
            show_system_status()
        
        elif choice == '6':
            show_available_tests()
        
        elif choice == '7':
            print("🛑 Stopping all services...")
            subprocess.run(['docker-compose', '-f', 'infra/bus/docker-compose.kafka.yml', 'down'])
            print("✅ All services stopped")
        
        elif choice == '8':
            print("👋 Goodbye!")
            break
        
        else:
            print("❌ Invalid choice. Please try again.")

if __name__ == "__main__":
    main()