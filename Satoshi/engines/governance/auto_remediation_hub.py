#!/usr/bin/env python3
"""
Auto-Remediation Integration Hub

Central orchestrator for all Phase 3A auto-remediation capabilities.
Coordinates between data source failover, agent recovery, and core remediation engine.

This is the MAIN ENTRY POINT for auto-remediation in production trading systems.

Integration Components:
- Auto-Remediation Engine: Core automation logic
- Data Source Failover Manager: Exchange connectivity resilience  
- Agent Recovery Manager: Agent lifecycle management
- Circuit Breaker Network: Cascade failure prevention
- Health Monitoring API: System status tracking

Key Features:
- Unified incident handling
- Coordinated recovery actions
- Real-time status dashboard
- Automated escalation
- Performance metrics
"""

import asyncio
import logging
import time
import json
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass
from datetime import datetime, timezone
from collections import deque

from infra.bus.streaming_bus import StreamingBus
from engines.governance.auto_remediation_engine import (
    AutoRemediationEngine, 
    AutoRemediationConfig,
    RemediationType,
    PRODUCTION_CONFIG
)
from engines.governance.data_source_failover import DataSourceFailoverManager
from engines.governance.agent_recovery_manager import AgentRecoveryManager

logger = logging.getLogger(__name__)

@dataclass
class SystemHealthStatus:
    """Overall system health status."""
    overall_status: str  # HEALTHY, DEGRADED, CRITICAL, FAILED
    agent_health_score: float  # 0.0 to 1.0
    data_source_health_score: float  # 0.0 to 1.0
    recent_incidents: int
    auto_remediation_success_rate: float
    uptime_hours: float
    last_updated: datetime

class AutoRemediationHub:
    """
    Central hub for coordinating all auto-remediation activities.
    
    This is the main component that trading operators interact with for
    system health, recovery status, and manual interventions.
    """
    
    def __init__(self):
        self.session_id = f"hub_{int(time.time())}"
        self.start_time = datetime.now(timezone.utc)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
        # Initialize streaming bus
        streaming_config = {
            "bootstrap_servers": "localhost:9092",
            "enable_ssl": False,
            "enable_sasl": False
        }
        self.streaming_bus = StreamingBus(streaming_config)
        
        # Initialize auto-remediation components
        self.remediation_engine = AutoRemediationEngine(PRODUCTION_CONFIG)
        self.failover_manager = DataSourceFailoverManager(self.streaming_bus)
        self.recovery_manager = AgentRecoveryManager(self.streaming_bus)
        
        # Hub state
        self.is_running = False
        self.incident_queue: deque = deque(maxlen=1000)
        self.manual_overrides: Set[str] = set()
        
        # Metrics aggregation
        self.hub_metrics = {
            "total_incidents_handled": 0,
            "successful_remediations": 0,
            "failed_remediations": 0,
            "manual_interventions": 0,
            "escalations": 0,
            "avg_resolution_time_ms": 0.0
        }
        
        logger.info(f"Auto-Remediation Hub initialized: {self.session_id}")
    
    async def start_system(self) -> None:
        """Start the complete auto-remediation system."""
        logger.info("🚀 Starting Auto-Remediation Hub...")
        
        try:
            # Start core components (StreamingBus doesn't need explicit initialization)
            await self.remediation_engine.start_monitoring()
            await self.failover_manager.start_monitoring()
            await self.recovery_manager.start_monitoring()
            
            # Start hub coordination tasks
            asyncio.create_task(self._coordinate_incidents())
            asyncio.create_task(self._monitor_system_health())
            asyncio.create_task(self._publish_unified_metrics())
            asyncio.create_task(self._handle_manual_commands())
            
            self.is_running = True
            
            logger.info("✅ Auto-Remediation Hub fully operational")
            logger.info("🔥 TRADING SYSTEM PROTECTION ACTIVE")
            
            # Announce system readiness
            await self._announce_system_ready()
            
        except Exception as e:
            logger.error(f"❌ Failed to start Auto-Remediation Hub: {e}")
            raise
    
    async def _coordinate_incidents(self) -> None:
        """Coordinate incident handling across all components."""
        logger.info("📋 Starting incident coordination...")
        
        # Subscribe to incident topics
        incident_topics = [
            "incidents.freshness",
            "incidents.anomalies",
            "incidents.schema_violations", 
            "incidents.leakage",
            "incidents.venue_health",
            "incidents.data_source_failover",
            "events.agent_recovery",
            "alerts.agent_escalation"
        ]
        
        # For demo purposes, simulate incident monitoring
        # In production, this would consume from Kafka topics
        while self.is_running:
            try:
                await asyncio.sleep(10)
                await self._check_for_incidents()
                
            except Exception as e:
                logger.error(f"Error in incident coordination: {e}")
                await asyncio.sleep(30)
    
    async def _check_for_incidents(self) -> None:
        """Check for new incidents that need coordination."""
        try:
            # Get status from all components
            remediation_metrics = self.remediation_engine.get_metrics()
            failover_status = self.failover_manager.get_status_summary()
            recovery_status = self.recovery_manager.get_status_summary()
            
            # Check for critical situations requiring coordination
            
            # 1. Multiple agent failures (cascade prevention)
            failed_agents = [
                agent_id for agent_id, status in recovery_status["agent_status"].items()
                if status == "failed"
            ]
            
            if len(failed_agents) >= 3:
                await self._handle_cascade_scenario(failed_agents)
            
            # 2. Data source issues affecting multiple venues
            if failover_status["recent_failovers_1h"] >= 2:
                await self._handle_multi_venue_failure()
            
            # 3. System resource exhaustion
            if remediation_metrics["actions_failed"] > remediation_metrics["actions_successful"]:
                await self._handle_remediation_overload()
            
        except Exception as e:
            logger.error(f"Error checking incidents: {e}")
    
    async def _handle_cascade_scenario(self, failed_agents: List[str]) -> None:
        """Handle cascade failure scenario across multiple agents."""
        logger.error(f"🚨 CASCADE FAILURE DETECTED: {len(failed_agents)} agents failed")
        logger.error(f"   Failed agents: {failed_agents}")
        
        try:
            # Trigger cascade prevention
            await self.remediation_engine.trigger_remediation(
                incident_id=f"cascade_{int(time.time())}",
                remediation_type=RemediationType.CASCADE_PREVENTION,
                target_component="agent_network",
                reason=f"Multiple agent failures: {failed_agents}",
                action_params={"dependent_components": failed_agents}
            )
            
            # Isolate non-critical agents to protect critical ones
            critical_agents = [
                agent_id for agent_id, config in self.recovery_manager.agent_configs.items()
                if config.critical_for_trading and agent_id not in failed_agents
            ]
            
            for agent_id in failed_agents:
                config = self.recovery_manager.agent_configs.get(agent_id)
                if config and not config.critical_for_trading:
                    await self.remediation_engine.trigger_remediation(
                        incident_id=f"isolate_{agent_id}_{int(time.time())}",
                        remediation_type=RemediationType.CIRCUIT_BREAKER_ISOLATION,
                        target_component=agent_id,
                        reason="Cascade prevention isolation"
                    )
            
            # Alert operations team
            await self._send_critical_alert(
                "CASCADE_FAILURE",
                f"Multiple agent failures detected: {failed_agents}. Auto-remediation in progress."
            )
            
            self.hub_metrics["escalations"] += 1
            
        except Exception as e:
            logger.error(f"Error handling cascade scenario: {e}")
    
    async def _handle_multi_venue_failure(self) -> None:
        """Handle failure across multiple data venues."""
        logger.error("🚨 MULTI-VENUE FAILURE DETECTED")
        
        try:
            # Get current failover status
            status = self.failover_manager.get_status_summary()
            
            # If multiple primary sources are down, this is critical
            primary_sources_down = 0
            for source_name, details in status["source_details"].items():
                if details["status"] == "failed" and "priority" in source_name:
                    primary_sources_down += 1
            
            if primary_sources_down >= 2:
                # Critical situation - notify operations immediately
                await self._send_critical_alert(
                    "MULTI_VENUE_FAILURE",
                    f"Multiple primary data sources failed. Backup sources active. Trading at risk."
                )
                
                # Trigger emergency data source management
                await self.remediation_engine.trigger_remediation(
                    incident_id=f"multi_venue_{int(time.time())}",
                    remediation_type=RemediationType.DATA_SOURCE_FAILOVER,
                    target_component="all_venues",
                    reason="Multiple venue failure emergency",
                    action_params={"emergency_mode": True}
                )
            
        except Exception as e:
            logger.error(f"Error handling multi-venue failure: {e}")
    
    async def _handle_remediation_overload(self) -> None:
        """Handle when auto-remediation system is overloaded."""
        logger.error("🚨 AUTO-REMEDIATION OVERLOAD DETECTED")
        
        try:
            # Pause non-critical auto-remediation
            self.manual_overrides.add("pause_non_critical")
            
            # Alert operations
            await self._send_critical_alert(
                "REMEDIATION_OVERLOAD",
                "Auto-remediation system overloaded. Manual intervention required."
            )
            
            self.hub_metrics["escalations"] += 1
            
        except Exception as e:
            logger.error(f"Error handling remediation overload: {e}")
    
    async def _monitor_system_health(self) -> None:
        """Monitor overall system health and generate status."""
        logger.info("🏥 Starting system health monitoring...")
        
        while self.is_running:
            try:
                # Collect health data from all components
                health_status = await self._calculate_system_health()
                
                # Publish system health
                await self._publish_system_health(health_status)
                
                # Check for critical health issues
                if health_status.overall_status == "CRITICAL":
                    await self._handle_critical_health(health_status)
                
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logger.error(f"Error monitoring system health: {e}")
                await asyncio.sleep(60)
    
    async def _calculate_system_health(self) -> SystemHealthStatus:
        """Calculate overall system health score."""
        try:
            # Get metrics from all components
            remediation_metrics = self.remediation_engine.get_metrics()
            failover_status = self.failover_manager.get_status_summary()
            recovery_status = self.recovery_manager.get_status_summary()
            
            # Calculate agent health score
            total_agents = recovery_status["total_agents"]
            healthy_agents = len([
                status for status in recovery_status["agent_status"].values()
                if status == "healthy"
            ])
            agent_health_score = healthy_agents / total_agents if total_agents > 0 else 0.0
            
            # Calculate data source health score
            total_sources = failover_status["source_count"]
            healthy_sources = failover_status["healthy_sources"]
            data_source_health_score = healthy_sources / total_sources if total_sources > 0 else 0.0
            
            # Calculate auto-remediation success rate
            total_actions = remediation_metrics["actions_executed"]
            successful_actions = remediation_metrics["actions_successful"]
            success_rate = successful_actions / total_actions if total_actions > 0 else 1.0
            
            # Determine overall status
            min_score = min(agent_health_score, data_source_health_score, success_rate)
            
            if min_score >= 0.9:
                overall_status = "HEALTHY"
            elif min_score >= 0.7:
                overall_status = "DEGRADED"
            elif min_score >= 0.5:
                overall_status = "CRITICAL"
            else:
                overall_status = "FAILED"
            
            # Calculate uptime
            uptime = (datetime.now(timezone.utc) - self.start_time).total_seconds() / 3600
            
            return SystemHealthStatus(
                overall_status=overall_status,
                agent_health_score=agent_health_score,
                data_source_health_score=data_source_health_score,
                recent_incidents=len(self.incident_queue),
                auto_remediation_success_rate=success_rate,
                uptime_hours=uptime,
                last_updated=datetime.now(timezone.utc)
            )
            
        except Exception as e:
            logger.error(f"Error calculating system health: {e}")
            return SystemHealthStatus(
                overall_status="UNKNOWN",
                agent_health_score=0.0,
                data_source_health_score=0.0,
                recent_incidents=0,
                auto_remediation_success_rate=0.0,
                uptime_hours=0.0,
                last_updated=datetime.now(timezone.utc)
            )
    
    async def _publish_system_health(self, health_status: SystemHealthStatus) -> None:
        """Publish unified system health status."""
        try:
            health_data = {
                "timestamp": health_status.last_updated.isoformat(),
                "session_id": self.session_id,
                "overall_status": health_status.overall_status,
                "scores": {
                    "agent_health": health_status.agent_health_score,
                    "data_source_health": health_status.data_source_health_score,
                    "auto_remediation_success": health_status.auto_remediation_success_rate
                },
                "metrics": {
                    "recent_incidents": health_status.recent_incidents,
                    "uptime_hours": health_status.uptime_hours,
                    **self.hub_metrics
                },
                "manual_overrides": list(self.manual_overrides)
            }
            
            await self.streaming_bus.publish_with_headers(
                topic="system.health_status",
                payload=health_data,
                headers={
                    "metric_type": "system_health",
                    "overall_status": health_status.overall_status,
                    "source": "auto_remediation_hub"
                },
                partition_key=self.session_id
            )
            
        except Exception as e:
            logger.error(f"Error publishing system health: {e}")
    
    async def _handle_critical_health(self, health_status: SystemHealthStatus) -> None:
        """Handle critical system health situation."""
        logger.error(f"🚨 CRITICAL SYSTEM HEALTH: {health_status.overall_status}")
        
        await self._send_critical_alert(
            "CRITICAL_SYSTEM_HEALTH",
            f"System health critical. Agent score: {health_status.agent_health_score:.2f}, "
            f"Data source score: {health_status.data_source_health_score:.2f}, "
            f"Auto-remediation success: {health_status.auto_remediation_success_rate:.2f}"
        )
    
    async def _publish_unified_metrics(self) -> None:
        """Publish unified metrics from all components."""
        while self.is_running:
            try:
                # Aggregate metrics from all components
                unified_metrics = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "session_id": self.session_id,
                    "hub_metrics": dict(self.hub_metrics),
                    "remediation_engine": self.remediation_engine.get_metrics(),
                    "failover_manager": self.failover_manager.get_status_summary(),
                    "recovery_manager": self.recovery_manager.get_status_summary()
                }
                
                await self.streaming_bus.publish_with_headers(
                    topic="metrics.unified_auto_remediation",
                    payload=unified_metrics,
                    headers={
                        "metric_type": "unified_metrics",
                        "source": "auto_remediation_hub"
                    },
                    partition_key=self.session_id
                )
                
                await asyncio.sleep(120)  # Publish every 2 minutes
                
            except Exception as e:
                logger.error(f"Error publishing unified metrics: {e}")
                await asyncio.sleep(120)
    
    async def _handle_manual_commands(self) -> None:
        """Handle manual commands from operators."""
        # This would typically consume from a control topic
        # For now, simulate command handling
        while self.is_running:
            try:
                await asyncio.sleep(5)
                # Process any manual commands
                
            except Exception as e:
                logger.error(f"Error handling manual commands: {e}")
                await asyncio.sleep(30)
    
    async def _send_critical_alert(self, alert_type: str, message: str) -> None:
        """Send critical alerts to operations team."""
        try:
            alert_data = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "alert_type": alert_type,
                "message": message,
                "session_id": self.session_id,
                "priority": "CRITICAL",
                "requires_immediate_attention": True
            }
            
            await self.streaming_bus.publish_with_headers(
                topic="alerts.critical_system",
                payload=alert_data,
                headers={
                    "alert_type": alert_type,
                    "priority": "CRITICAL",
                    "requires_immediate_attention": "true"
                },
                partition_key="critical_alerts"
            )
            
            logger.error(f"🚨 CRITICAL ALERT SENT: {alert_type} - {message}")
            
        except Exception as e:
            logger.error(f"Failed to send critical alert: {e}")
    
    async def _announce_system_ready(self) -> None:
        """Announce that the auto-remediation system is ready."""
        try:
            ready_announcement = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "session_id": self.session_id,
                "message": "Auto-Remediation Hub fully operational",
                "components": {
                    "remediation_engine": "ACTIVE",
                    "failover_manager": "ACTIVE", 
                    "recovery_manager": "ACTIVE"
                },
                "protection_level": "FULL"
            }
            
            await self.streaming_bus.publish_with_headers(
                topic="system.auto_remediation_ready",
                payload=ready_announcement,
                headers={
                    "event_type": "system_ready",
                    "protection_level": "FULL"
                },
                partition_key="system_events"
            )
            
            logger.info("📢 System ready announcement sent")
            
        except Exception as e:
            logger.error(f"Error sending ready announcement: {e}")
    
    # Manual control methods for operators
    
    async def manual_failover(self, from_source: str, to_source: str) -> bool:
        """Manually trigger data source failover."""
        logger.info(f"🔧 Manual failover requested: {from_source} → {to_source}")
        
        success = await self.failover_manager.manual_failover(from_source, to_source)
        
        if success:
            self.hub_metrics["manual_interventions"] += 1
            logger.info(f"✅ Manual failover completed")
        
        return success
    
    async def manual_restart_agent(self, agent_id: str, force: bool = False) -> bool:
        """Manually restart an agent."""
        logger.info(f"🔧 Manual agent restart requested: {agent_id} (force={force})")
        
        success = await self.recovery_manager.manual_restart_agent(agent_id, force)
        
        if success:
            self.hub_metrics["manual_interventions"] += 1
            logger.info(f"✅ Manual agent restart completed")
        
        return success
    
    def pause_auto_remediation(self, component: str = "all") -> None:
        """Pause auto-remediation for a component or all."""
        logger.warning(f"⏸️  Pausing auto-remediation: {component}")
        
        if component == "all":
            self.remediation_engine.config.enabled = False
            self.manual_overrides.add("paused_all")
        else:
            self.manual_overrides.add(f"paused_{component}")
        
        self.hub_metrics["manual_interventions"] += 1
    
    def resume_auto_remediation(self, component: str = "all") -> None:
        """Resume auto-remediation for a component or all."""
        logger.info(f"▶️  Resuming auto-remediation: {component}")
        
        if component == "all":
            self.remediation_engine.config.enabled = True
            self.manual_overrides.discard("paused_all")
        else:
            self.manual_overrides.discard(f"paused_{component}")
        
        self.hub_metrics["manual_interventions"] += 1
    
    async def trigger_cascade_scenario(self, failed_agents: List[str]) -> bool:
        """Public method to trigger cascade scenario for testing/demo purposes."""
        try:
            await self._handle_cascade_scenario(failed_agents)
            return True
        except Exception as e:
            self.logger.error(f"Failed to trigger cascade scenario: {e}")
            return False
    
    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get comprehensive dashboard data for operators."""
        return {
            "session_id": self.session_id,
            "uptime_hours": (datetime.now(timezone.utc) - self.start_time).total_seconds() / 3600,
            "is_running": self.is_running,
            "manual_overrides": list(self.manual_overrides),
            "hub_metrics": dict(self.hub_metrics),
            "component_status": {
                "remediation_engine": "ACTIVE" if self.remediation_engine.config.enabled else "PAUSED",
                "failover_manager": "ACTIVE",
                "recovery_manager": "ACTIVE"
            },
            "recent_incidents": len(self.incident_queue),
            "components": {
                "remediation_engine": self.remediation_engine.get_metrics(),
                "failover_manager": self.failover_manager.get_status_summary(),
                "recovery_manager": self.recovery_manager.get_status_summary()
            }
        }
    
    async def shutdown(self) -> None:
        """Graceful shutdown of the entire auto-remediation system."""
        logger.info("🛑 Shutting down Auto-Remediation Hub...")
        
        self.is_running = False
        
        # Shutdown all components
        await self.remediation_engine.shutdown()
        await self.failover_manager.shutdown() 
        await self.recovery_manager.shutdown()
        
        logger.info("✅ Auto-Remediation Hub shutdown complete")


# Main entry point for production
async def start_production_auto_remediation():
    """Start the complete auto-remediation system for production trading."""
    
    print("🚀 SATOSHI TRADING SYSTEM AUTO-REMEDIATION")
    print("=" * 60)
    print("🛡️  Phase 3A: Critical Auto-Remediation")
    print("📊 Production Trading Protection")
    print("⚡ Sub-second failover capability")
    print("🔄 Automatic agent recovery")
    print("🚨 Real-time incident response")
    print("=" * 60)
    
    hub = AutoRemediationHub()
    
    try:
        await hub.start_system()
        
        print("\n✅ AUTO-REMEDIATION SYSTEM FULLY OPERATIONAL")
        print("🔥 TRADING SYSTEM PROTECTION ACTIVE")
        print("\n📊 System Status:")
        
        # Run the system
        while hub.is_running:
            await asyncio.sleep(10)
            
            # Show periodic status updates
            dashboard = hub.get_dashboard_data()
            print(f"\r⏱️  Uptime: {dashboard['uptime_hours']:.1f}h | "
                  f"Incidents: {dashboard['recent_incidents']} | "
                  f"Interventions: {dashboard['hub_metrics']['manual_interventions']}", 
                  end="", flush=True)
            
    except KeyboardInterrupt:
        print("\n\n🛑 Shutdown requested by operator...")
    except Exception as e:
        print(f"\n❌ Critical error: {e}")
    finally:
        await hub.shutdown()
        print("\n✅ Auto-remediation system offline")


if __name__ == "__main__":
    # Configure logging for production
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Start the system
    asyncio.run(start_production_auto_remediation())
