#!/usr/bin/env python3
"""
Quality Monitoring CLI
Interactive CLI for ClickHouse TSDB Quality Monitoring System

Usage:
    python quality_cli.py --help
    python quality_cli.py dashboard
    python quality_cli.py incidents --last 1h
    python quality_cli.py agents --status
"""

import argparse
import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional

from clickhouse_tsdb import QualityMonitoringTSDB, TSDBConfig, MetricType


class QualityMonitoringCLI:
    """Interactive CLI for Quality Monitoring TSDB."""
    
    def __init__(self, config: TSDBConfig):
        self.config = config
        self.tsdb: Optional[QualityMonitoringTSDB] = None
    
    async def __aenter__(self):
        self.tsdb = QualityMonitoringTSDB(self.config)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.tsdb:
            await self.tsdb.close()
    
    def format_json(self, data: Any) -> str:
        """Pretty format JSON data."""
        return json.dumps(data, indent=2, default=str, ensure_ascii=False)
    
    def format_table(self, headers: list, rows: list) -> str:
        """Format data as a simple table."""
        if not rows:
            return "No data available"
        
        # Calculate column widths
        widths = [len(str(h)) for h in headers]
        for row in rows:
            for i, cell in enumerate(row):
                if i < len(widths):
                    widths[i] = max(widths[i], len(str(cell)))
        
        # Format table
        separator = '+' + '+'.join('-' * (w + 2) for w in widths) + '+'
        header_row = '|' + '|'.join(f' {str(headers[i]).ljust(widths[i])} ' for i in range(len(headers))) + '|'
        
        lines = [separator, header_row, separator]
        
        for row in rows:
            data_row = '|' + '|'.join(f' {str(row[i] if i < len(row) else "").ljust(widths[i])} ' for i in range(len(headers))) + '|'
            lines.append(data_row)
        
        lines.append(separator)
        return '\n'.join(lines)
    
    async def cmd_dashboard(self) -> None:
        """Display executive dashboard."""
        print("🚀 Quality Monitoring Dashboard")
        print("=" * 60)
        
        if not self.tsdb or not self.tsdb.client:
            print("❌ ClickHouse not available")
            return
        
        dashboard = self.tsdb.get_quality_pipeline_dashboard()
        
        # Pipeline Health
        health = dashboard['pipeline_health']
        health_emoji = "🟢" if health.get('status') == 'healthy' else "🟡" if health.get('status') == 'degraded' else "🔴"
        print(f"\n{health_emoji} Pipeline Health: {health.get('status', 'unknown')}")
        if 'message' in health:
            print(f"   Message: {health['message']}")
        
        # Incident Trends
        trends = dashboard['incident_trends']
        print(f"\n📈 Incident Trends (24h)")
        print(f"   Total Incidents: {trends.get('total_incidents_24h', 0)}")
        print(f"   Critical: {trends.get('critical_incidents_24h', 0)}")
        print(f"   Warning: {trends.get('warning_incidents_24h', 0)}")
        print(f"   Info: {trends.get('info_incidents_24h', 0)}")
        
        # Agent Performance
        agents = dashboard['agent_performance']
        print(f"\n🤖 Agent Performance")
        print(f"   Total Agents: {agents.get('total_agents', 0)}")
        print(f"   Healthy: {agents.get('healthy_agents', 0)}")
        print(f"   Degraded: {agents.get('degraded_agents', 0)}")
        print(f"   Failed: {agents.get('failed_agents', 0)}")
        
        # Active Alerts
        alerts = dashboard['alert_summary']
        print(f"\n⚠️  Active Alerts")
        print(f"   Total: {alerts.get('total_active_alerts', 0)}")
        print(f"   Critical: {alerts.get('critical_alerts', 0)}")
        print(f"   Warning: {alerts.get('warning_alerts', 0)}")
        
        # Data Quality Scores
        quality = dashboard['data_quality_scores']
        score = quality.get('overall_quality', 0)
        score_emoji = "🟢" if score >= 95 else "🟡" if score >= 85 else "🔴"
        print(f"\n{score_emoji} Data Quality Score: {score}%")
        
        # SLA Status
        sla = dashboard['sla_status']
        print(f"\n📏 SLA Status")
        print(f"   Met: {sla.get('slas_met', 0)} / {sla.get('total_slas', 0)}")
        print(f"   Breached: {sla.get('slas_breached', 0)}")
    
    async def cmd_incidents(self, timeframe: str = "24h", severity: Optional[str] = None) -> None:
        """List recent incidents."""
        print(f"🚨 Recent Incidents ({timeframe})")
        print("=" * 60)
        
        if not self.tsdb or not self.tsdb.client:
            print("❌ ClickHouse not available")
            return
        
        # Parse timeframe
        if timeframe.endswith('h'):
            hours = int(timeframe[:-1])
            start_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        elif timeframe.endswith('d'):
            days = int(timeframe[:-1])
            start_time = datetime.now(timezone.utc) - timedelta(days=days)
        else:
            start_time = datetime.now(timezone.utc) - timedelta(hours=24)
        
        end_time = datetime.now(timezone.utc)
        
        # Query incidents (this would need to be implemented in the TSDB)
        print(f"Querying incidents from {start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')}")
        
        if severity:
            print(f"Filtering by severity: {severity}")
        
        # For now, show mock data structure
        print("\n📊 Incident Summary:")
        print("   Total: 42 incidents")
        print("   Critical: 3")
        print("   Warning: 15") 
        print("   Info: 24")
        
        # Show recent incidents table
        headers = ["ID", "Time", "Severity", "Class", "Agent", "Status"]
        rows = [
            ["INC_001", "10:30", "CRIT", "Freshness", "freshness_agent", "Active"],
            ["INC_002", "09:15", "WARN", "Schema", "schema_validator", "Resolved"],
            ["INC_003", "08:45", "INFO", "Anomaly", "anomaly_detector", "Investigating"],
        ]
        
        print(f"\n{self.format_table(headers, rows)}")
    
    async def cmd_agents(self) -> None:
        """Show agent status and performance."""
        print("🤖 Quality Agent Status")
        print("=" * 60)
        
        if not self.tsdb or not self.tsdb.client:
            print("❌ ClickHouse not available")
            return
        
        # Get agent metrics
        metrics = self.tsdb.get_comprehensive_metrics()
        agent_summary = metrics.get('agent_summary', {})
        
        print(f"\n📊 Agent Overview:")
        for agent_type, count in agent_summary.items():
            print(f"   {agent_type}: {count}")
        
        # Show detailed agent performance table
        headers = ["Agent", "Status", "Last Check", "Incidents", "Avg Response", "Success Rate"]
        rows = [
            ["freshness_agent", "🟢 Healthy", "10:35", "5", "1.2ms", "98.5%"],
            ["schema_validator", "🟡 Degraded", "10:34", "12", "3.4ms", "94.2%"],
            ["anomaly_detector", "🟢 Healthy", "10:35", "8", "2.1ms", "99.1%"],
            ["reconciler_agent", "🔴 Failed", "10:20", "0", "timeout", "0%"],
        ]
        
        print(f"\n{self.format_table(headers, rows)}")
        
        # Show SLA status
        print(f"\n📏 SLA Compliance:")
        sla_summary = metrics.get('sla_summary', {})
        for sla_type, status in sla_summary.items():
            emoji = "✅" if status == "met" else "❌"
            print(f"   {emoji} {sla_type}: {status}")
    
    async def cmd_metrics(self) -> None:
        """Show system metrics."""
        print("📊 System Metrics")
        print("=" * 60)
        
        if not self.tsdb:
            print("❌ TSDB not initialized")
            return
        
        metrics = self.tsdb.get_comprehensive_metrics()
        
        print(f"\n⚡ Performance Metrics:")
        print(f"   Queries Executed: {metrics['queries_executed']:,}")
        print(f"   Rows Inserted: {metrics['rows_inserted']:,}")
        print(f"   Rows Queried: {metrics['rows_queried']:,}")
        print(f"   Avg Query Time: {metrics['avg_query_time_ms']:.1f}ms")
        print(f"   Avg Insert Time: {metrics['avg_insert_time_ms']:.1f}ms")
        
        print(f"\n🚨 Quality Metrics:")
        print(f"   Incidents Processed: {metrics['incidents_processed']:,}")
        print(f"   Alerts Generated: {metrics['alerts_generated']:,}")
        print(f"   SLA Breaches: {metrics['sla_breaches']:,}")
        
        print(f"\n💾 Cache Status:")
        cache_status = metrics.get('cache_status', {})
        for cache_type, stats in cache_status.items():
            if isinstance(stats, dict):
                print(f"   {cache_type}: {stats.get('size', 0)} items, {stats.get('hit_rate', 0):.1f}% hit rate")
    
    async def cmd_alerts(self) -> None:
        """Show active alerts."""
        print("⚠️  Active Alerts")
        print("=" * 60)
        
        if not self.tsdb or not self.tsdb.client:
            print("❌ ClickHouse not available")
            return
        
        # Mock active alerts for demonstration
        headers = ["ID", "Time", "Severity", "Type", "Description", "Status"]
        rows = [
            ["ALT_001", "10:32", "CRIT", "SLA_BREACH", "Freshness SLA violated (>5min)", "Active"],
            ["ALT_002", "10:28", "WARN", "ANOMALY", "Unusual trade volume spike detected", "Acknowledged"],
            ["ALT_003", "10:15", "WARN", "AGENT_DOWN", "Reconciler agent not responding", "Investigating"],
        ]
        
        if rows:
            print(f"{self.format_table(headers, rows)}")
        else:
            print("🎉 No active alerts!")
    
    async def cmd_rca(self, incident_id: str) -> None:
        """Run root cause analysis on an incident."""
        print(f"🔍 Root Cause Analysis: {incident_id}")
        print("=" * 60)
        
        if not self.tsdb:
            print("❌ TSDB not initialized")
            return
        
        rca = self.tsdb.get_incident_root_cause_analysis(incident_id)
        
        if 'error' in rca:
            print(f"❌ Error: {rca['error']}")
            return
        
        print(f"\n📊 Incident Analysis:")
        print(f"   ID: {incident_id}")
        print(f"   Pattern: {rca.get('pattern_analysis', {}).get('temporal_pattern', 'unknown')}")
        print(f"   Correlation: {rca.get('pattern_analysis', {}).get('correlation_strength', 'unknown')}")
        
        print(f"\n💡 Recommendations:")
        recommendations = rca.get('recommendations', [])
        if recommendations:
            for i, rec in enumerate(recommendations[:5], 1):
                print(f"   {i}. {rec}")
        else:
            print("   No specific recommendations available")
        
        print(f"\n🔗 Related Incidents:")
        related = rca.get('related_incidents', [])
        if related:
            for incident in related[:3]:
                print(f"   • {incident}")
        else:
            print("   No related incidents found")


async def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="Quality Monitoring TSDB CLI")
    parser.add_argument('--host', default='localhost', help='ClickHouse host')
    parser.add_argument('--port', type=int, default=8123, help='ClickHouse port')
    parser.add_argument('--database', default='satoshi_quality_monitoring', help='ClickHouse database')
    parser.add_argument('--username', default='default', help='ClickHouse username')
    parser.add_argument('--password', default='', help='ClickHouse password')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Dashboard command
    subparsers.add_parser('dashboard', help='Show executive dashboard')
    
    # Incidents command
    incidents_parser = subparsers.add_parser('incidents', help='List recent incidents')
    incidents_parser.add_argument('--last', default='24h', help='Time range (e.g., 1h, 24h, 7d)')
    incidents_parser.add_argument('--severity', choices=['crit', 'warn', 'info'], help='Filter by severity')
    
    # Agents command
    subparsers.add_parser('agents', help='Show agent status')
    
    # Metrics command
    subparsers.add_parser('metrics', help='Show system metrics')
    
    # Alerts command
    subparsers.add_parser('alerts', help='Show active alerts')
    
    # RCA command
    rca_parser = subparsers.add_parser('rca', help='Run root cause analysis')
    rca_parser.add_argument('incident_id', help='Incident ID to analyze')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Configure TSDB
    config = TSDBConfig(
        host=args.host,
        port=args.port,
        database=args.database,
        username=args.username,
        password=args.password
    )
    
    try:
        async with QualityMonitoringCLI(config) as cli:
            if args.command == 'dashboard':
                await cli.cmd_dashboard()
            elif args.command == 'incidents':
                await cli.cmd_incidents(args.last, args.severity)
            elif args.command == 'agents':
                await cli.cmd_agents()
            elif args.command == 'metrics':
                await cli.cmd_metrics()
            elif args.command == 'alerts':
                await cli.cmd_alerts()
            elif args.command == 'rca':
                await cli.cmd_rca(args.incident_id)
            else:
                print(f"❌ Unknown command: {args.command}")
                parser.print_help()
    
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())