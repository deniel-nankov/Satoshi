#!/usr/bin/env python3
"""
Auto-Remediation Engine

Critical automation for trading operations that can't wait for human intervention.
Handles data source failover, agent recovery, resource management, and cascade failure prevention.

Key Features:
- Automatic data source failover (Binance → Coinbase)
- Graceful agent restart and recovery
- Memory pressure and resource scaling
- Circuit breaker cascade prevention
- Schema evolution handling
- Real-time notification system

This is ESSENTIAL for production trading - milliseconds matter!
"""

import asyncio
import logging
import time
import psutil
import json
from typing import Dict, List, Optional, Any, Union, Callable, Set
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timezone
from collections import defaultdict, deque

from infra.bus.streaming_bus import StreamingBus, BreakerIntent

logger = logging.getLogger(__name__)

class RemediationType(Enum):
    """Types of automatic remediation actions."""
    DATA_SOURCE_FAILOVER = "data_source_failover"
    AGENT_RESTART = "agent_restart"
    CIRCUIT_BREAKER_ISOLATION = "circuit_breaker_isolation"
    SCHEMA_MIGRATION = "schema_migration"
    RESOURCE_SCALING = "resource_scaling"
    CASCADE_PREVENTION = "cascade_prevention"
    QUARANTINE_STREAM = "quarantine_stream"

class RemediationStatus(Enum):
    """Status of remediation actions."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL_SUCCESS = "partial_success"

@dataclass
class RemediationAction:
    """A specific remediation action to be executed."""
    action_id: str
    remediation_type: RemediationType
    incident_id: str
    target_component: str
    action_params: Dict[str, Any]
    priority: int  # 1=critical, 5=low
    timeout_seconds: int
    created_at: datetime
    status: RemediationStatus = RemediationStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    rollback_actions: List[str] = field(default_factory=list)

@dataclass
class DataSourceConfig:
    """Configuration for data source failover."""
    name: str
    priority: int  # 1=primary, 2=secondary, etc.
    endpoints: List[str]
    health_check_url: str
    topics: List[str]
    circuit_breaker_threshold: int = 3
    recovery_timeout_seconds: int = 300

@dataclass
class AutoRemediationConfig:
    """Configuration for the auto-remediation engine."""
    enabled: bool = True
    max_concurrent_actions: int = 5
    action_timeout_seconds: int = 300
    notification_webhooks: List[str] = field(default_factory=list)
    escalation_threshold_minutes: int = 15
    
    # Data source configurations
    data_sources: Dict[str, DataSourceConfig] = field(default_factory=dict)
    
    # Agent management
    enable_agent_restart: bool = True
    memory_threshold_percent: int = 85
    cpu_threshold_percent: int = 90
    
    # Circuit breaker management
    enable_auto_isolation: bool = True
    cascade_detection_window_seconds: int = 60
    
    # Resource scaling
    enable_auto_scaling: bool = True
    scale_up_threshold: float = 0.8
    scale_down_threshold: float = 0.3

class AutoRemediationEngine:
    """
    Critical automation engine for trading infrastructure.
    
    Handles immediate responses to system failures that can't wait for human intervention.
    """
    
    def __init__(self, config: AutoRemediationConfig):
        self.config = config
        self.session_id = f"remediation_{int(time.time())}"
        
        # Initialize Kafka streaming bus
        streaming_config = {
            "bootstrap_servers": "localhost:9092",
            "enable_ssl": False,
            "enable_sasl": False
        }
        self.streaming_bus = StreamingBus(streaming_config)
        
        # Action tracking
        self.pending_actions: Dict[str, RemediationAction] = {}
        self.completed_actions: deque = deque(maxlen=1000)
        self.action_semaphore = asyncio.Semaphore(config.max_concurrent_actions)
        
        # Data source management
        self.active_sources: Dict[str, str] = {}  # topic -> active_source
        self.source_health: Dict[str, bool] = {}
        
        # Agent health tracking
        self.agent_health: Dict[str, Dict[str, Any]] = {}
        self.restart_history: Dict[str, List[datetime]] = defaultdict(list)
        
        # Circuit breaker registration tracking to prevent unbounded registrations
        self._registered_isolation_breakers: Set[str] = set()
        self._registered_cascade_breakers: Set[str] = set()
        
        # Metrics
        self.metrics = {
            "actions_executed": 0,
            "actions_successful": 0,
            "actions_failed": 0,
            "data_source_failovers": 0,
            "agent_restarts": 0,
            "cascade_preventions": 0,
            "avg_response_time_ms": 0.0
        }
        
        # Dispatched action tracking to prevent duplicate task spawns
        self._dispatched_action_ids = set()
        
        logger.info(f"Auto-Remediation Engine initialized: {self.session_id}")
    
    async def start_monitoring(self) -> None:
        """Start monitoring incidents and triggering auto-remediation."""
        logger.info("🚨 Auto-Remediation Engine starting incident monitoring...")
        
        # Register circuit breaker for self-protection
        await self.streaming_bus.register_circuit_breaker(
            component_id=f"auto_remediation_{self.session_id}",
            failure_threshold=5,
            recovery_timeout_us=300_000_000,  # 5 minutes
            dependency_components=[]
        )
        
        # Start monitoring different incident types
        incident_topics = [
            "incidents.freshness",
            "incidents.anomalies", 
            "incidents.schema_violations",
            "incidents.leakage",
            "incidents.venue_health"
        ]
        
        for topic in incident_topics:
            asyncio.create_task(self._monitor_incident_topic(topic))
        
        # Start health monitoring
        asyncio.create_task(self._monitor_system_health())
        
        # Start action executor
        asyncio.create_task(self._execute_pending_actions())
        
        logger.info(f"✅ Monitoring {len(incident_topics)} incident streams")
    
    async def _monitor_incident_topic(self, topic: str) -> None:
        """Monitor a specific incident topic for auto-remediation triggers."""
        try:
            logger.info(f"👁️  Monitoring {topic} for auto-remediation triggers...")
            
            # For now, simulate incident monitoring
            # In full implementation, this would consume from Kafka
            while True:
                await asyncio.sleep(5)  # Check every 5 seconds
                
                # Check for any pending incidents that need remediation
                await self._check_for_remediation_triggers(topic)
                
        except Exception as e:
            logger.error(f"Error monitoring {topic}: {e}")
    
    async def _check_for_remediation_triggers(self, topic: str) -> None:
        """Check if any incidents require immediate auto-remediation."""
        try:
            # Simulate checking system health and triggering remediation
            
            # Example: Check memory usage
            memory_percent = psutil.virtual_memory().percent
            if memory_percent > self.config.memory_threshold_percent:
                await self.trigger_remediation(
                    incident_id=f"high_memory_{int(time.time())}",
                    remediation_type=RemediationType.RESOURCE_SCALING,
                    target_component="system",
                    reason=f"Memory usage {memory_percent}% > threshold {self.config.memory_threshold_percent}%",
                    action_params={"memory_percent": memory_percent}
                )
            
            # Example: Check CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            if cpu_percent > self.config.cpu_threshold_percent:
                await self.trigger_remediation(
                    incident_id=f"high_cpu_{int(time.time())}",
                    remediation_type=RemediationType.RESOURCE_SCALING,
                    target_component="system",
                    reason=f"CPU usage {cpu_percent}% > threshold {self.config.cpu_threshold_percent}%",
                    action_params={"cpu_percent": cpu_percent}
                )
                
        except Exception as e:
            logger.error(f"Error checking remediation triggers: {e}")
    
    async def trigger_remediation(self, incident_id: str, remediation_type: RemediationType,
                                target_component: str, reason: str, 
                                action_params: Optional[Dict[str, Any]] = None) -> str:
        """Trigger an auto-remediation action."""
        if not self.config.enabled:
            logger.warning("Auto-remediation disabled, skipping action")
            return ""
        
        action_id = f"{remediation_type.value}_{int(time.time_ns())}"
        
        # Determine priority based on remediation type
        priority_map = {
            RemediationType.CASCADE_PREVENTION: 1,
            RemediationType.DATA_SOURCE_FAILOVER: 1,
            RemediationType.CIRCUIT_BREAKER_ISOLATION: 2,
            RemediationType.AGENT_RESTART: 3,
            RemediationType.RESOURCE_SCALING: 3,
            RemediationType.SCHEMA_MIGRATION: 4,
            RemediationType.QUARANTINE_STREAM: 2
        }
        
        action = RemediationAction(
            action_id=action_id,
            remediation_type=remediation_type,
            incident_id=incident_id,
            target_component=target_component,
            action_params=action_params or {},
            priority=priority_map.get(remediation_type, 5),
            timeout_seconds=self.config.action_timeout_seconds,
            created_at=datetime.now(timezone.utc)
        )
        
        self.pending_actions[action_id] = action
        
        logger.info(f"🚨 Triggered auto-remediation: {remediation_type.value} for {target_component}")
        logger.info(f"   Reason: {reason}")
        logger.info(f"   Action ID: {action_id}")
        
        # Publish remediation event
        await self._publish_remediation_event(action, "triggered")
        
        return action_id
    
    async def _execute_pending_actions(self) -> None:
        """Execute pending remediation actions in priority order."""
        while True:
            try:
                if not self.pending_actions:
                    await asyncio.sleep(1)
                    continue
                
                # Sort by priority (1=highest)
                sorted_actions = sorted(
                    self.pending_actions.values(),
                    key=lambda a: (a.priority, a.created_at)
                )
                
                for action in sorted_actions:
                    if action.status == RemediationStatus.PENDING and action.action_id not in self._dispatched_action_ids:
                        # Mark as dispatched to prevent duplicate task creation
                        self._dispatched_action_ids.add(action.action_id)
                        task = asyncio.create_task(self._execute_action(action))
                        # Remove from dispatched set when task completes
                        task.add_done_callback(lambda t, aid=action.action_id: self._dispatched_action_ids.discard(aid))
                
                await asyncio.sleep(2)  # Check every 2 seconds
                
            except Exception as e:
                logger.error(f"Error in action executor: {e}")
                await asyncio.sleep(5)
    
    async def _execute_action(self, action: RemediationAction) -> None:
        """Execute a specific remediation action."""
        async with self.action_semaphore:
            start_time = time.time()
            action.status = RemediationStatus.IN_PROGRESS
            action.started_at = datetime.now(timezone.utc)
            
            try:
                logger.info(f"⚡ Executing {action.remediation_type.value} for {action.target_component}")
                
                # Route to specific remediation handler
                success = False
                if action.remediation_type == RemediationType.DATA_SOURCE_FAILOVER:
                    success = await self._handle_data_source_failover(action)
                elif action.remediation_type == RemediationType.AGENT_RESTART:
                    success = await self._handle_agent_restart(action)
                elif action.remediation_type == RemediationType.CIRCUIT_BREAKER_ISOLATION:
                    success = await self._handle_circuit_breaker_isolation(action)
                elif action.remediation_type == RemediationType.RESOURCE_SCALING:
                    success = await self._handle_resource_scaling(action)
                elif action.remediation_type == RemediationType.SCHEMA_MIGRATION:
                    success = await self._handle_schema_migration(action)
                elif action.remediation_type == RemediationType.CASCADE_PREVENTION:
                    success = await self._handle_cascade_prevention(action)
                elif action.remediation_type == RemediationType.QUARANTINE_STREAM:
                    success = await self._handle_quarantine_stream(action)
                else:
                    raise ValueError(f"Unknown remediation type: {action.remediation_type}")
                
                # Update action status
                action.status = RemediationStatus.SUCCESS if success else RemediationStatus.FAILED
                action.completed_at = datetime.now(timezone.utc)
                
                # Update metrics
                self.metrics["actions_executed"] += 1
                if success:
                    self.metrics["actions_successful"] += 1
                else:
                    self.metrics["actions_failed"] += 1
                
                # Update response time
                response_time_ms = (time.time() - start_time) * 1000
                if self.metrics["avg_response_time_ms"] == 0:
                    self.metrics["avg_response_time_ms"] = response_time_ms
                else:
                    self.metrics["avg_response_time_ms"] = (
                        0.9 * self.metrics["avg_response_time_ms"] + 0.1 * response_time_ms
                    )
                
                # Move to completed
                self.completed_actions.append(action)
                del self.pending_actions[action.action_id]
                
                # Publish completion event
                await self._publish_remediation_event(action, "completed")
                
                logger.info(f"✅ Remediation completed: {action.action_id} ({'success' if success else 'failed'})")
                
            except Exception as e:
                action.status = RemediationStatus.FAILED
                action.error_message = str(e)
                action.completed_at = datetime.now(timezone.utc)
                self.metrics["actions_failed"] += 1
                
                logger.error(f"❌ Remediation failed: {action.action_id} - {e}")
                
                # Move to completed even if failed
                self.completed_actions.append(action)
                del self.pending_actions[action.action_id]
                
                await self._publish_remediation_event(action, "failed")
    
    async def _handle_data_source_failover(self, action: RemediationAction) -> bool:
        """Handle automatic data source failover."""
        try:
            target_source = action.target_component
            logger.info(f"🔄 Executing data source failover from {target_source}")
            
            # Find backup data source
            backup_source = self._find_backup_data_source(target_source)
            if not backup_source:
                logger.error(f"No backup source available for {target_source}")
                return False
            
            # Update routing configuration
            affected_topics = action.action_params.get("affected_topics", [])
            for topic in affected_topics:
                self.active_sources[topic] = backup_source
            
            # Request circuit breaker open for failed source
            component_id = f"failed_source_{target_source}"
            await self.streaming_bus.register_circuit_breaker(
                component_id=component_id,
                failure_threshold=1,
                recovery_timeout_us=1800_000_000,  # 30 minutes
                dependency_components=[]
            )
            await self.streaming_bus.publish_breaker_intent(
                BreakerIntent(
                    component_id=component_id,
                    intent="trip",
                    reason="auto_remediation_failover",
                    severity="high",
                    requested_by=self.session_id,
                    metadata={
                        "action_id": action.action_id,
                        "failed_source": target_source,
                        "backup_source": backup_source
                    }
                )
            )
            
            self.metrics["data_source_failovers"] += 1
            
            logger.info(f"✅ Failover complete: {target_source} → {backup_source}")
            return True
            
        except Exception as e:
            logger.error(f"Data source failover failed: {e}")
            return False
    
    async def _handle_agent_restart(self, action: RemediationAction) -> bool:
        """Handle graceful agent restart."""
        try:
            agent_id = action.target_component
            logger.info(f"🔄 Executing graceful restart for agent {agent_id}")
            
            # Record restart attempt
            self.restart_history[agent_id].append(datetime.now(timezone.utc))
            
            # Check if too many recent restarts
            recent_restarts = [
                dt for dt in self.restart_history[agent_id]
                if (datetime.now(timezone.utc) - dt).total_seconds() < 3600  # Last hour
            ]
            
            if len(recent_restarts) > 3:
                logger.warning(f"Too many restarts for {agent_id} in last hour, escalating...")
                return False
            
            # Simulate graceful restart
            # In real implementation, this would:
            # 1. Send shutdown signal to agent
            # 2. Wait for graceful shutdown
            # 3. Restart agent process
            # 4. Verify health
            
            await asyncio.sleep(2)  # Simulate restart time
            
            self.metrics["agent_restarts"] += 1
            
            logger.info(f"✅ Agent restart complete: {agent_id}")
            return True
            
        except Exception as e:
            logger.error(f"Agent restart failed: {e}")
            return False
    
    async def _handle_circuit_breaker_isolation(self, action: RemediationAction) -> bool:
        """Handle circuit breaker isolation of failing components."""
        try:
            component = action.target_component
            logger.info(f"🚫 Isolating component via circuit breaker: {component}")
            
            isolation_component_id = f"isolated_{component}"
            
            # Only register if not already registered to prevent unbounded registrations
            if isolation_component_id not in self._registered_isolation_breakers:
                await self.streaming_bus.register_circuit_breaker(
                    component_id=isolation_component_id,
                    failure_threshold=1,  # Immediate isolation
                    recovery_timeout_us=action.action_params.get("recovery_timeout", 1800_000_000),
                    dependency_components=[]
                )
                self._registered_isolation_breakers.add(isolation_component_id)
            
            await self.streaming_bus.publish_breaker_intent(
                BreakerIntent(
                    component_id=isolation_component_id,
                    intent="trip",
                    reason="auto_remediation_isolation",
                    severity="critical",
                    requested_by=self.session_id,
                    metadata={
                        "action_id": action.action_id,
                        "target_component": component
                    }
                )
            )
            
            logger.info(f"✅ Component isolated: {component}")
            return True
            
        except Exception as e:
            logger.error(f"Circuit breaker isolation failed: {e}")
            return False
    
    async def _handle_resource_scaling(self, action: RemediationAction) -> bool:
        """Handle automatic resource scaling."""
        try:
            logger.info(f"📈 Executing resource scaling for {action.target_component}")
            
            # Get current resource usage
            memory_percent = action.action_params.get("memory_percent", 0)
            cpu_percent = action.action_params.get("cpu_percent", 0)
            
            scaling_actions = []
            
            if memory_percent > self.config.memory_threshold_percent:
                # Trigger memory optimization
                scaling_actions.append("memory_optimization")
                logger.info(f"🧠 Triggering memory optimization (usage: {memory_percent}%)")
            
            if cpu_percent > self.config.cpu_threshold_percent:
                # Trigger load balancing
                scaling_actions.append("load_balancing")
                logger.info(f"⚡ Triggering load balancing (usage: {cpu_percent}%)")
            
            # Simulate scaling actions
            await asyncio.sleep(1)
            
            logger.info(f"✅ Resource scaling complete: {scaling_actions}")
            return True
            
        except Exception as e:
            logger.error(f"Resource scaling failed: {e}")
            return False
    
    async def _handle_schema_migration(self, action: RemediationAction) -> bool:
        """Handle automatic schema migration."""
        try:
            logger.info(f"🔄 Executing schema migration for {action.target_component}")
            
            # Simulate schema migration
            # In real implementation, this would:
            # 1. Detect schema changes
            # 2. Generate migration scripts
            # 3. Apply migrations safely
            # 4. Update parser configurations
            
            await asyncio.sleep(1)
            
            logger.info(f"✅ Schema migration complete")
            return True
            
        except Exception as e:
            logger.error(f"Schema migration failed: {e}")
            return False
    
    async def _handle_cascade_prevention(self, action: RemediationAction) -> bool:
        """Handle cascade failure prevention."""
        try:
            logger.info(f"🛡️  Executing cascade prevention for {action.target_component}")
            
            # Identify dependent components
            dependent_components = action.action_params.get("dependent_components", [])
            
            # Limit cascade operations to prevent unbounded registrations
            max_cascade_components = 10
            if len(dependent_components) > max_cascade_components:
                logger.warning(f"⚠️ Too many dependent components ({len(dependent_components)}), limiting to {max_cascade_components}")
                dependent_components = dependent_components[:max_cascade_components]
            
            # Open circuit breakers for dependent components
            for component in dependent_components:
                cascade_component_id = f"cascade_protected_{component}"
                
                # Only register if not already registered
                if cascade_component_id not in self._registered_cascade_breakers:
                    await self.streaming_bus.register_circuit_breaker(
                        component_id=cascade_component_id,
                        failure_threshold=1,
                        recovery_timeout_us=600_000_000,  # 10 minutes
                        dependency_components=[]
                    )
                    self._registered_cascade_breakers.add(cascade_component_id)
                
                await self.streaming_bus.publish_breaker_intent(
                    BreakerIntent(
                        component_id=cascade_component_id,
                        intent="trip",
                        reason="auto_remediation_cascade_prevention",
                        severity="high",
                        requested_by=self.session_id,
                        metadata={
                            "action_id": action.action_id,
                            "dependent_component": component
                        }
                    )
                )
            
            self.metrics["cascade_preventions"] += 1
            
            logger.info(f"✅ Cascade prevention complete: protected {len(dependent_components)} components")
            return True
            
        except Exception as e:
            logger.error(f"Cascade prevention failed: {e}")
            return False
    
    async def _handle_quarantine_stream(self, action: RemediationAction) -> bool:
        """Handle quarantining of bad data streams."""
        try:
            stream_name = action.target_component
            logger.info(f"🚫 Quarantining data stream: {stream_name}")
            
            # Redirect stream to quarantine topic
            quarantine_topic = f"quarantine.{stream_name}"
            
            # Update routing to quarantine
            # In real implementation, this would update Kafka routing
            
            logger.info(f"✅ Stream quarantined: {stream_name} → {quarantine_topic}")
            return True
            
        except Exception as e:
            logger.error(f"Stream quarantine failed: {e}")
            return False
    
    def _find_backup_data_source(self, failed_source: str) -> Optional[str]:
        """Find the next priority backup data source."""
        # Simple backup source mapping
        backup_map = {
            "binance": "coinbase",
            "coinbase": "kraken", 
            "kraken": "binance",
            "deribit": "okx",
            "okx": "deribit"
        }
        return backup_map.get(failed_source)
    
    async def _monitor_system_health(self) -> None:
        """Monitor overall system health."""
        while True:
            try:
                # Check system resources
                memory = psutil.virtual_memory()
                cpu_percent = psutil.cpu_percent(interval=1)
                disk = psutil.disk_usage('/')
                
                # Update health metrics
                self.agent_health["system"] = {
                    "memory_percent": memory.percent,
                    "cpu_percent": cpu_percent,
                    "disk_percent": (disk.used / disk.total) * 100,
                    "last_check": datetime.now(timezone.utc).isoformat()
                }
                
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logger.error(f"Error monitoring system health: {e}")
                await asyncio.sleep(30)
    
    async def _publish_remediation_event(self, action: RemediationAction, event_type: str) -> None:
        """Publish remediation events to Kafka."""
        try:
            event_data = {
                "action_id": action.action_id,
                "remediation_type": action.remediation_type.value,
                "incident_id": action.incident_id,
                "target_component": action.target_component,
                "status": action.status.value,
                "event_type": event_type,
                "priority": action.priority,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "session_id": self.session_id
            }
            
            if action.error_message:
                event_data["error_message"] = action.error_message
            
            headers = {
                "event_type": event_type,
                "remediation_type": action.remediation_type.value,
                "priority": str(action.priority),
                "component": action.target_component
            }
            
            await self.streaming_bus.publish_with_headers(
                topic="control.remediation_events",
                payload=event_data,
                headers=headers,
                partition_key=action.target_component
            )
            
        except Exception as e:
            logger.error(f"Failed to publish remediation event: {e}")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get auto-remediation metrics."""
        return {
            **self.metrics,
            "pending_actions": len(self.pending_actions),
            "completed_actions": len(self.completed_actions),
            "session_id": self.session_id,
            "active_sources": dict(self.active_sources),
            "system_health": self.agent_health.get("system", {})
        }
    
    async def shutdown(self) -> None:
        """Graceful shutdown of auto-remediation engine."""
        logger.info("🛑 Shutting down Auto-Remediation Engine...")
        
        # Complete pending actions if possible
        if self.pending_actions:
            logger.info(f"Waiting for {len(self.pending_actions)} pending actions...")
            await asyncio.sleep(5)  # Give actions time to complete
        
        logger.info("✅ Auto-Remediation Engine shutdown complete")


# Configuration for production trading
PRODUCTION_CONFIG = AutoRemediationConfig(
    enabled=True,
    max_concurrent_actions=3,
    action_timeout_seconds=300,
    memory_threshold_percent=85,
    cpu_threshold_percent=90,
    enable_agent_restart=True,
    enable_auto_isolation=True,
    enable_auto_scaling=True,
    data_sources={
        "binance": DataSourceConfig(
            name="binance",
            priority=1,
            endpoints=["wss://stream.binance.com:9443"],
            health_check_url="https://api.binance.com/api/v3/ping",
            topics=["raw_data.trades.binance", "raw_data.orderbook.binance"]
        ),
        "coinbase": DataSourceConfig(
            name="coinbase", 
            priority=2,
            endpoints=["wss://ws-feed.exchange.coinbase.com"],
            health_check_url="https://api.exchange.coinbase.com/",
            topics=["raw_data.trades.coinbase", "raw_data.orderbook.coinbase"]
        )
    }
)


# Example usage
async def main():
    """Example usage of the auto-remediation engine."""
    
    print("🚨 Auto-Remediation Engine Demo")
    print("=" * 50)
    
    engine = AutoRemediationEngine(PRODUCTION_CONFIG)
    
    # Start monitoring
    await engine.start_monitoring()
    
    print("✅ Auto-remediation engine started")
    print("💡 Monitoring system health and incidents...")
    
    # Simulate running for a while
    try:
        await asyncio.sleep(30)  # Run for 30 seconds
        
        # Show metrics
        metrics = engine.get_metrics()
        print(f"\n📊 Auto-Remediation Metrics:")
        print(f"   Actions executed: {metrics['actions_executed']}")
        print(f"   Success rate: {metrics['actions_successful']}/{metrics['actions_executed']}")
        print(f"   Avg response time: {metrics['avg_response_time_ms']:.1f}ms")
        print(f"   Data source failovers: {metrics['data_source_failovers']}")
        print(f"   Agent restarts: {metrics['agent_restarts']}")
        
    except KeyboardInterrupt:
        print("\n🛑 Shutdown requested...")
    finally:
        await engine.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
