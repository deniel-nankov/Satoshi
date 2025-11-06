#!/usr/bin/env python3
"""
Agent Recovery Manager

Critical automation for agent lifecycle management in trading systems.
Handles graceful restarts, health monitoring, resource management, and cascade failure preve            \"recovery_timeout_seconds\": 120,\n            \"escalation_threshold_failures\": 5,

Agent Monitoring:
- Exchange Connector: Critical data ingestion
- Events Collector: Market event processing  
- OnChain Collector: Blockchain data monitoring
- Options Chain Collector: Derivatives data
- Anomaly Detector: ML-based detection
- Schema Validator: Data integrity
- Freshness Agent: Data quality
- Leakage Police: Information security
- Reconciler Agent: Data consistency

Key Features:
- Graceful agent shutdown/restart
- Memory/CPU threshold monitoring
- Dependency chain management
- Health check automation
- Resource scaling triggers
- Incident escalation
"""

import asyncio
import logging
import time
import psutil
import signal
import os
import json
import subprocess
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque

from infra.bus.streaming_bus import StreamingBus

logger = logging.getLogger(__name__)

class AgentStatus(Enum):
    """Status of individual agents."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    RESTARTING = "restarting"
    FAILED = "failed"
    STOPPED = "stopped"
    STARTING = "starting"

class RecoveryAction(Enum):
    """Types of recovery actions."""
    GRACEFUL_RESTART = "graceful_restart"
    FORCE_RESTART = "force_restart"
    SCALE_RESOURCES = "scale_resources"
    ISOLATE_AGENT = "isolate_agent"
    ESCALATE_HUMAN = "escalate_human"
    DEPENDENCY_RESTART = "dependency_restart"

@dataclass
class AgentHealthMetrics:
    """Real-time health metrics for an agent."""
    agent_id: str
    status: AgentStatus
    last_heartbeat: datetime
    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    memory_mb: float = 0.0
    uptime_seconds: float = 0.0
    messages_processed: int = 0
    errors_count: int = 0
    last_error: Optional[str] = None
    circuit_breaker_open: bool = False
    dependencies_healthy: bool = True
    restart_count: int = 0
    last_restart: Optional[datetime] = None

@dataclass
class AgentConfig:
    """Configuration for an agent."""
    agent_id: str
    module_path: str
    process_id: Optional[int] = None
    dependencies: List[str] = field(default_factory=list)
    max_memory_mb: int = 1024
    max_cpu_percent: float = 80.0
    restart_timeout_seconds: int = 60
    max_restarts_per_hour: int = 3
    critical_for_trading: bool = True
    auto_restart_enabled: bool = True

@dataclass
class RecoveryEvent:
    """Records an agent recovery event."""
    timestamp: datetime
    agent_id: str
    action: RecoveryAction
    reason: str
    success: bool
    duration_ms: float
    error_message: Optional[str] = None
    triggered_by: str = "auto"

class AgentRecoveryManager:
    """
    Manages automatic recovery and health monitoring for all trading agents.
    
    Ensures continuous operation by monitoring agent health and automatically
    handling failures, restarts, and resource issues.
    """
    
    def __init__(self, streaming_bus: StreamingBus):
        self.streaming_bus = streaming_bus
        self.session_id = f"recovery_{int(time.time())}"
        
        # Agent configurations
        self.agent_configs = {
            "exchange_connector": AgentConfig(
                agent_id="exchange_connector",
                module_path="engines.data.bronze.exchange_connector",
                dependencies=[],
                max_memory_mb=2048,
                max_cpu_percent=85.0,
                critical_for_trading=True
            ),
            "events_collector": AgentConfig(
                agent_id="events_collector", 
                module_path="engines.data.bronze.events_collector",
                dependencies=["exchange_connector"],
                max_memory_mb=1024,
                max_cpu_percent=70.0,
                critical_for_trading=True
            ),
            "onchain_collector": AgentConfig(
                agent_id="onchain_collector",
                module_path="engines.data.bronze.onchain_collector", 
                dependencies=[],
                max_memory_mb=1536,
                max_cpu_percent=75.0,
                critical_for_trading=True
            ),
            "options_chain_collector": AgentConfig(
                agent_id="options_chain_collector",
                module_path="engines.data.bronze.options_chain_collector",
                dependencies=["exchange_connector"],
                max_memory_mb=1024,
                max_cpu_percent=80.0,
                critical_for_trading=True
            ),
            "anomaly_detector": AgentConfig(
                agent_id="anomaly_detector",
                module_path="engines.data.silver.anomaly_detector",
                dependencies=["exchange_connector", "events_collector"],
                max_memory_mb=2048,
                max_cpu_percent=90.0,
                critical_for_trading=False
            ),
            "schema_validator": AgentConfig(
                agent_id="schema_validator",
                module_path="engines.data.silver.schema_validator",
                dependencies=["exchange_connector"],
                max_memory_mb=512,
                max_cpu_percent=60.0,
                critical_for_trading=True
            ),
            "freshness_agent": AgentConfig(
                agent_id="freshness_agent",
                module_path="engines.data.silver.freshness_agent",
                dependencies=["exchange_connector"],
                max_memory_mb=512,
                max_cpu_percent=50.0,
                critical_for_trading=True
            ),
            "leakage_police": AgentConfig(
                agent_id="leakage_police",
                module_path="engines.data.silver.leakage_police",
                dependencies=["exchange_connector", "events_collector"],
                max_memory_mb=1024,
                max_cpu_percent=70.0,
                critical_for_trading=False
            ),
            "reconciler_agent": AgentConfig(
                agent_id="reconciler_agent",
                module_path="engines.data.silver.reconciler_agent",
                dependencies=["exchange_connector", "events_collector"],
                max_memory_mb=1024,
                max_cpu_percent=75.0,
                critical_for_trading=True
            )
        }
        
        # Health tracking
        self.agent_health: Dict[str, AgentHealthMetrics] = {}
        for agent_id in self.agent_configs.keys():
            self.agent_health[agent_id] = AgentHealthMetrics(
                agent_id=agent_id,
                status=AgentStatus.STOPPED,
                last_heartbeat=datetime.now(timezone.utc)
            )
        
        # Recovery tracking
        self.recovery_history: deque = deque(maxlen=500)
        self.restart_counts: Dict[str, List[datetime]] = defaultdict(list)
        self.escalated_agents: Set[str] = set()
        
        # Configuration
        self.config = {
            "health_check_interval_seconds": 15,
            "heartbeat_timeout_seconds": 45,
            "recovery_timeout_seconds": 120,
            "escalation_threshold_failures": 5,
            "resource_check_interval_seconds": 30,
            "dependency_check_enabled": True,
            "cascade_prevention_enabled": True
        }
        
        # Sequence tracking for canonical headers
        self._sequence_numbers: Dict[str, int] = defaultdict(int)
        
        # Task management
        self._tasks = []
        self._running = False
        
        logger.info(f"Agent Recovery Manager initialized: {self.session_id}")
        logger.info(f"Managing {len(self.agent_configs)} agents")
    
    async def start_monitoring(self) -> None:
        """Start monitoring all agents and handling recovery."""
        logger.info("🚨 Starting agent recovery monitoring...")
        
        # Register circuit breaker for self-protection
        await self.streaming_bus.register_circuit_breaker(
            component_id=f"recovery_manager_{self.session_id}",
            failure_threshold=3,
            recovery_timeout_us=300_000_000,  # 5 minutes
            dependency_components=[]
        )
        
        # Start monitoring tasks and store handles
        self._running = True
        self._tasks = [
            asyncio.create_task(self._monitor_agent_health()),
            asyncio.create_task(self._monitor_resource_usage()),
            asyncio.create_task(self._monitor_dependencies()),
            asyncio.create_task(self._handle_recovery_queue()),
            asyncio.create_task(self._publish_health_metrics())
        ]
        
        logger.info("✅ Agent recovery monitoring started")
    
    async def _monitor_agent_health(self) -> None:
        """Monitor health of all agents via heartbeat and status checks."""
        while self._running:
            try:
                logger.debug("🔍 Checking agent health...")
                
                for agent_id, config in self.agent_configs.items():
                    metrics = self.agent_health[agent_id]
                    
                    # Skip if agent is in restarting state
                    if metrics.status == AgentStatus.RESTARTING:
                        continue
                    
                    # Check heartbeat timeout
                    time_since_heartbeat = (
                        datetime.now(timezone.utc) - metrics.last_heartbeat
                    ).total_seconds()
                    
                    if time_since_heartbeat > self.config["heartbeat_timeout_seconds"]:
                        if metrics.status != AgentStatus.FAILED:
                            logger.warning(f"🔴 Agent heartbeat timeout: {agent_id} ({time_since_heartbeat:.1f}s)")
                            await self._handle_agent_failure(agent_id, "heartbeat_timeout")
                    
                    # Check process status if we have a PID
                    if config.process_id:
                        is_running = await self._check_process_running(config.process_id)
                        if not is_running and metrics.status != AgentStatus.FAILED:
                            logger.warning(f"🔴 Agent process not running: {agent_id} (PID {config.process_id})")
                            await self._handle_agent_failure(agent_id, "process_died")
                
                await asyncio.sleep(self.config["health_check_interval_seconds"])
                
            except Exception as e:
                logger.error(f"Error in health monitoring: {e}")
                await asyncio.sleep(30)
    
    async def _monitor_resource_usage(self) -> None:
        """Monitor CPU and memory usage of agents."""
        while True:
            try:
                for agent_id, config in self.agent_configs.items():
                    metrics = self.agent_health[agent_id]
                    
                    if config.process_id:
                        try:
                            process = psutil.Process(config.process_id)
                            
                            # Get CPU and memory usage
                            cpu_percent = process.cpu_percent()
                            memory_info = process.memory_info()
                            memory_mb = memory_info.rss / 1024 / 1024
                            memory_percent = process.memory_percent()
                            
                            # Update metrics
                            metrics.cpu_percent = cpu_percent
                            metrics.memory_mb = memory_mb
                            metrics.memory_percent = memory_percent
                            
                            # Check thresholds
                            if memory_mb > config.max_memory_mb:
                                logger.warning(f"🔴 High memory usage: {agent_id} ({memory_mb:.1f}MB > {config.max_memory_mb}MB)")
                                await self._handle_resource_issue(agent_id, "high_memory", {"memory_mb": memory_mb})
                            
                            if cpu_percent > config.max_cpu_percent:
                                logger.warning(f"🔴 High CPU usage: {agent_id} ({cpu_percent:.1f}% > {config.max_cpu_percent}%)")
                                await self._handle_resource_issue(agent_id, "high_cpu", {"cpu_percent": cpu_percent})
                            
                        except psutil.NoSuchProcess:
                            # Process doesn't exist
                            if metrics.status != AgentStatus.FAILED:
                                logger.warning(f"🔴 Agent process not found: {agent_id}")
                                await self._handle_agent_failure(agent_id, "process_not_found")
                        except Exception as e:
                            logger.warning(f"Error checking resources for {agent_id}: {e}")
                
                await asyncio.sleep(self.config["resource_check_interval_seconds"])
                
            except Exception as e:
                logger.error(f"Error monitoring resource usage: {e}")
                await asyncio.sleep(60)
    
    async def _monitor_dependencies(self) -> None:
        """Monitor agent dependencies and handle cascade failures."""
        if not self.config["dependency_check_enabled"]:
            return
        
        while True:
            try:
                for agent_id, config in self.agent_configs.items():
                    metrics = self.agent_health[agent_id]
                    
                    # Check if dependencies are healthy
                    dependencies_healthy = True
                    unhealthy_deps = []
                    
                    for dep_id in config.dependencies:
                        if dep_id in self.agent_health:
                            dep_status = self.agent_health[dep_id].status
                            if dep_status in [AgentStatus.FAILED, AgentStatus.UNHEALTHY]:
                                dependencies_healthy = False
                                unhealthy_deps.append(dep_id)
                    
                    metrics.dependencies_healthy = dependencies_healthy
                    
                    # Handle dependency failures
                    if not dependencies_healthy and metrics.status == AgentStatus.HEALTHY:
                        logger.warning(f"🔴 Dependencies unhealthy for {agent_id}: {unhealthy_deps}")
                        
                        if self.config["cascade_prevention_enabled"]:
                            # Gracefully degrade agent instead of failing
                            metrics.status = AgentStatus.DEGRADED
                            await self._publish_dependency_event(agent_id, unhealthy_deps)
                
                await asyncio.sleep(30)  # Check dependencies every 30 seconds
                
            except Exception as e:
                logger.error(f"Error monitoring dependencies: {e}")
                await asyncio.sleep(60)
    
    async def _handle_agent_failure(self, agent_id: str, reason: str) -> None:
        """Handle agent failure and trigger appropriate recovery."""
        try:
            metrics = self.agent_health[agent_id]
            config = self.agent_configs[agent_id]
            
            logger.error(f"🚨 Agent failure detected: {agent_id} - {reason}")
            
            # Update status
            metrics.status = AgentStatus.FAILED
            
            # Check if we should escalate to human
            if agent_id in self.escalated_agents:
                logger.error(f"❌ Agent {agent_id} already escalated, skipping auto-recovery")
                return
            
            # Check restart frequency
            recent_restarts = self._count_recent_restarts(agent_id)
            if recent_restarts >= config.max_restarts_per_hour:
                logger.error(f"❌ Too many restarts for {agent_id} ({recent_restarts}/hour), escalating to human")
                await self._escalate_to_human(agent_id, "too_many_restarts")
                return
            
            # Determine recovery action
            if not config.auto_restart_enabled:
                logger.warning(f"⚠️  Auto-restart disabled for {agent_id}")
                await self._escalate_to_human(agent_id, "auto_restart_disabled") 
                return
            
            # Try graceful restart first
            recovery_action = RecoveryAction.GRACEFUL_RESTART
            if reason in ["process_died", "process_not_found"]:
                recovery_action = RecoveryAction.FORCE_RESTART
            
            await self._execute_recovery(agent_id, recovery_action, reason)
            
        except Exception as e:
            logger.error(f"Error handling agent failure: {e}")
    
    async def _handle_resource_issue(self, agent_id: str, issue_type: str, details: Dict[str, Any]) -> None:
        """Handle resource-related issues (high CPU/memory)."""
        try:
            logger.warning(f"⚠️  Resource issue for {agent_id}: {issue_type}")
            
            config = self.agent_configs[agent_id]
            
            # For non-critical agents, try resource scaling first
            if not config.critical_for_trading:
                await self._execute_recovery(agent_id, RecoveryAction.SCALE_RESOURCES, issue_type, details)
            else:
                # For critical agents, restart immediately
                await self._execute_recovery(agent_id, RecoveryAction.GRACEFUL_RESTART, issue_type, details)
            
        except Exception as e:
            logger.error(f"Error handling resource issue: {e}")
    
    async def _execute_recovery(self, agent_id: str, action: RecoveryAction, reason: str, 
                               details: Optional[Dict[str, Any]] = None) -> None:
        """Execute a recovery action for an agent."""
        start_time = time.time()
        
        try:
            logger.info(f"⚡ Executing recovery for {agent_id}: {action.value}")
            
            metrics = self.agent_health[agent_id]
            config = self.agent_configs[agent_id]
            
            # Update status
            if action in [RecoveryAction.GRACEFUL_RESTART, RecoveryAction.FORCE_RESTART]:
                metrics.status = AgentStatus.RESTARTING
            
            success = False
            error_message = None
            
            try:
                if action == RecoveryAction.GRACEFUL_RESTART:
                    success = await self._graceful_restart_agent(agent_id)
                elif action == RecoveryAction.FORCE_RESTART:
                    success = await self._force_restart_agent(agent_id)
                elif action == RecoveryAction.SCALE_RESOURCES:
                    success = await self._scale_agent_resources(agent_id, details or {})
                elif action == RecoveryAction.ISOLATE_AGENT:
                    success = await self._isolate_agent(agent_id)
                elif action == RecoveryAction.DEPENDENCY_RESTART:
                    success = await self._restart_dependencies(agent_id)
                else:
                    raise ValueError(f"Unknown recovery action: {action}")
                
                if success:
                    logger.info(f"✅ Recovery successful for {agent_id}: {action.value}")
                    if action in [RecoveryAction.GRACEFUL_RESTART, RecoveryAction.FORCE_RESTART]:
                        metrics.status = AgentStatus.STARTING
                        metrics.restart_count += 1
                        metrics.last_restart = datetime.now(timezone.utc)
                        self.restart_counts[agent_id].append(datetime.now(timezone.utc))
                else:
                    logger.error(f"❌ Recovery failed for {agent_id}: {action.value}")
                    error_message = "Recovery action failed"
                    
                    # Try escalation
                    await self._escalate_to_human(agent_id, f"recovery_failed_{action.value}")
                
            except Exception as e:
                error_message = str(e)
                logger.error(f"❌ Recovery exception for {agent_id}: {e}")
                success = False
            
            # Record recovery event
            duration_ms = (time.time() - start_time) * 1000
            
            recovery_event = RecoveryEvent(
                timestamp=datetime.now(timezone.utc),
                agent_id=agent_id,
                action=action,
                reason=reason,
                success=success,
                duration_ms=duration_ms,
                error_message=error_message
            )
            
            self.recovery_history.append(recovery_event)
            await self._publish_recovery_event(recovery_event)
            
        except Exception as e:
            logger.error(f"Error executing recovery: {e}")
    
    async def _graceful_restart_agent(self, agent_id: str) -> bool:
        """Perform graceful restart of an agent."""
        try:
            config = self.agent_configs[agent_id]
            
            # Send graceful shutdown signal if process exists
            if config.process_id:
                try:
                    os.kill(config.process_id, signal.SIGTERM)
                    
                    # Wait for graceful shutdown
                    for _ in range(30):  # Wait up to 30 seconds
                        if not await self._check_process_running(config.process_id):
                            break
                        await asyncio.sleep(1)
                    
                    # Force kill if still running
                    if await self._check_process_running(config.process_id):
                        os.kill(config.process_id, signal.SIGKILL)
                        await asyncio.sleep(2)
                        
                except ProcessLookupError:
                    pass  # Process already dead
            
            # Start the agent
            new_pid = await self._start_agent_process(agent_id)
            if new_pid:
                config.process_id = new_pid
                logger.info(f"✅ Agent restarted: {agent_id} (PID {new_pid})")
                return True
            else:
                logger.error(f"❌ Failed to start agent: {agent_id}")
                return False
            
        except Exception as e:
            logger.error(f"Graceful restart failed for {agent_id}: {e}")
            return False
    
    async def _force_restart_agent(self, agent_id: str) -> bool:
        """Force restart an agent immediately."""
        try:
            config = self.agent_configs[agent_id]
            
            # Force kill existing process
            if config.process_id:
                try:
                    os.kill(config.process_id, signal.SIGKILL)
                    await asyncio.sleep(2)
                except ProcessLookupError:
                    pass
            
            # Start the agent
            new_pid = await self._start_agent_process(agent_id)
            if new_pid:
                config.process_id = new_pid
                logger.info(f"✅ Agent force restarted: {agent_id} (PID {new_pid})")
                return True
            else:
                return False
            
        except Exception as e:
            logger.error(f"Force restart failed for {agent_id}: {e}")
            return False
    
    async def _start_agent_process(self, agent_id: str) -> Optional[int]:
        """Start an agent process and return the PID."""
        try:
            config = self.agent_configs[agent_id]
            
            # Construct command to start the agent
            # This would typically run the agent's main module
            cmd = [
                "python", "-m", config.module_path,
                "--agent-id", agent_id,
                "--recovery-session", self.session_id
            ]
            
            # For demo purposes, we'll simulate starting the process
            # In real implementation, this would actually start the process
            logger.info(f"🚀 Starting agent process: {' '.join(cmd)}")
            
            # Simulate process start
            process = await asyncio.create_subprocess_exec(
                "sleep", "1000",  # Placeholder process
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            if process.pid:
                logger.info(f"✅ Agent process started: {agent_id} (PID {process.pid})")
                return process.pid
            else:
                return None
            
        except Exception as e:
            logger.error(f"Failed to start agent process for {agent_id}: {e}")
            return None
    
    async def _scale_agent_resources(self, agent_id: str, details: Dict[str, Any]) -> bool:
        """Scale agent resources (placeholder for resource management)."""
        try:
            logger.info(f"📈 Scaling resources for {agent_id}")
            
            # In real implementation, this might:
            # - Increase memory limits
            # - Adjust thread pools
            # - Modify buffer sizes
            # - Trigger garbage collection
            
            # Simulate resource scaling
            await asyncio.sleep(2)
            
            logger.info(f"✅ Resource scaling completed for {agent_id}")
            return True
            
        except Exception as e:
            logger.error(f"Resource scaling failed for {agent_id}: {e}")
            return False
    
    async def _isolate_agent(self, agent_id: str) -> bool:
        """Isolate an agent using circuit breakers."""
        try:
            logger.info(f"🚫 Isolating agent: {agent_id}")
            
            # Open circuit breaker for the agent
            await self.streaming_bus.register_circuit_breaker(
                component_id=f"isolated_agent_{agent_id}",
                failure_threshold=1,
                recovery_timeout_us=1800_000_000,  # 30 minutes
                dependency_components=[]
            )
            
            # Update agent status
            self.agent_health[agent_id].circuit_breaker_open = True
            
            logger.info(f"✅ Agent isolated: {agent_id}")
            return True
            
        except Exception as e:
            logger.error(f"Agent isolation failed for {agent_id}: {e}")
            return False
    
    async def _restart_dependencies(self, agent_id: str) -> bool:
        """Restart dependencies of a failed agent."""
        try:
            config = self.agent_configs[agent_id]
            
            logger.info(f"🔄 Restarting dependencies for {agent_id}: {config.dependencies}")
            
            success_count = 0
            for dep_id in config.dependencies:
                if await self._graceful_restart_agent(dep_id):
                    success_count += 1
            
            return success_count == len(config.dependencies)
            
        except Exception as e:
            logger.error(f"Dependency restart failed for {agent_id}: {e}")
            return False
    
    async def _escalate_to_human(self, agent_id: str, reason: str) -> None:
        """Escalate agent issues to human operators."""
        try:
            self.escalated_agents.add(agent_id)
            
            escalation_data = {
                "agent_id": agent_id,
                "reason": reason,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "session_id": self.session_id,
                "recent_failures": len([
                    event for event in self.recovery_history
                    if (event.agent_id == agent_id and 
                        (datetime.now(timezone.utc) - event.timestamp).total_seconds() < 3600)
                ]),
                "agent_config": {
                    "critical_for_trading": self.agent_configs[agent_id].critical_for_trading,
                    "dependencies": self.agent_configs[agent_id].dependencies
                }
            }
            
            topic = "alerts.agent_escalation"
            self._sequence_numbers[topic] += 1
            await self.streaming_bus.publish_with_canonical_headers(
                topic=topic,
                partition_key=agent_id,
                payload=escalation_data,
                source_id=f"agent_recovery_manager.escalation.{agent_id}",
                sequence_number=self._sequence_numbers[topic],
                correlation_id=f"escalation_{agent_id}_{self.session_id}",
                producer_version="2.0.0"
            )
            
            logger.error(f"🚨 ESCALATED TO HUMAN: {agent_id} - {reason}")
            
        except Exception as e:
            logger.error(f"Failed to escalate {agent_id}: {e}")
    
    async def _check_process_running(self, pid: int) -> bool:
        """Check if a process is running."""
        try:
            psutil.Process(pid)
            return True
        except psutil.NoSuchProcess:
            return False
    
    def _count_recent_restarts(self, agent_id: str) -> int:
        """Count recent restarts for an agent."""
        now = datetime.now(timezone.utc)
        hour_ago = now - timedelta(hours=1)
        
        return len([
            restart_time for restart_time in self.restart_counts[agent_id]
            if restart_time > hour_ago
        ])
    
    async def _handle_recovery_queue(self) -> None:
        """Handle queued recovery actions."""
        # This would manage a queue of recovery actions
        # For now, we'll just run a placeholder loop
        while True:
            try:
                await asyncio.sleep(5)
                # Process any queued recovery actions
            except Exception as e:
                logger.error(f"Error in recovery queue handler: {e}")
                await asyncio.sleep(30)
    
    async def _publish_recovery_event(self, event: RecoveryEvent) -> None:
        """Publish recovery events for monitoring."""
        try:
            event_data = {
                "timestamp": event.timestamp.isoformat(),
                "agent_id": event.agent_id,
                "action": event.action.value,
                "reason": event.reason,
                "success": event.success,
                "duration_ms": event.duration_ms,
                "triggered_by": event.triggered_by,
                "session_id": self.session_id
            }
            
            if event.error_message:
                event_data["error_message"] = event.error_message
            
            topic = "events.agent_recovery"
            self._sequence_numbers[topic] += 1
            await self.streaming_bus.publish_with_canonical_headers(
                topic=topic,
                partition_key=event.agent_id,
                payload=event_data,
                source_id=f"agent_recovery_manager.recovery.{event.agent_id}",
                sequence_number=self._sequence_numbers[topic],
                correlation_id=f"recovery_{event.agent_id}_{int(event.timestamp.timestamp())}",
                producer_version="2.0.0"
            )
            
        except Exception as e:
            logger.error(f"Failed to publish recovery event: {e}")
    
    async def _publish_dependency_event(self, agent_id: str, unhealthy_deps: List[str]) -> None:
        """Publish dependency failure events."""
        try:
            event_data = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "agent_id": agent_id,
                "unhealthy_dependencies": unhealthy_deps,
                "session_id": self.session_id
            }
            
            topic = "events.dependency_failure"
            self._sequence_numbers[topic] += 1
            await self.streaming_bus.publish_with_canonical_headers(
                topic=topic,
                partition_key=agent_id,
                payload=event_data,
                source_id=f"agent_recovery_manager.dependency.{agent_id}",
                sequence_number=self._sequence_numbers[topic],
                correlation_id=f"dependency_{agent_id}_{self.session_id}",
                producer_version="2.0.0"
            )
            
        except Exception as e:
            logger.error(f"Failed to publish dependency event: {e}")
    
    async def _publish_health_metrics(self) -> None:
        """Publish agent health metrics."""
        while True:
            try:
                health_data = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "session_id": self.session_id,
                    "agent_health": {
                        agent_id: {
                            "status": metrics.status.value,
                            "cpu_percent": metrics.cpu_percent,
                            "memory_mb": metrics.memory_mb,
                            "memory_percent": metrics.memory_percent,
                            "uptime_seconds": metrics.uptime_seconds,
                            "messages_processed": metrics.messages_processed,
                            "errors_count": metrics.errors_count,
                            "restart_count": metrics.restart_count,
                            "dependencies_healthy": metrics.dependencies_healthy,
                            "circuit_breaker_open": metrics.circuit_breaker_open,
                            "last_heartbeat_age_seconds": (
                                datetime.now(timezone.utc) - metrics.last_heartbeat
                            ).total_seconds()
                        }
                        for agent_id, metrics in self.agent_health.items()
                    },
                    "summary": {
                        "total_agents": len(self.agent_configs),
                        "healthy_agents": len([
                            m for m in self.agent_health.values() 
                            if m.status == AgentStatus.HEALTHY
                        ]),
                        "failed_agents": len([
                            m for m in self.agent_health.values() 
                            if m.status == AgentStatus.FAILED
                        ]),
                        "escalated_agents": len(self.escalated_agents),
                        "total_recoveries": len(self.recovery_history)
                    }
                }
                
                topic = "metrics.agent_health"
                self._sequence_numbers[topic] += 1
                await self.streaming_bus.publish_with_canonical_headers(
                    topic=topic,
                    partition_key=self.session_id,
                    payload=health_data,
                    source_id="agent_recovery_manager.health_metrics",
                    sequence_number=self._sequence_numbers[topic],
                    correlation_id=f"health_{self.session_id}",
                    producer_version="2.0.0"
                )
                
                await asyncio.sleep(60)  # Publish every minute
                
            except Exception as e:
                logger.error(f"Error publishing health metrics: {e}")
                await asyncio.sleep(60)
    
    async def manual_restart_agent(self, agent_id: str, force: bool = False) -> bool:
        """Manually restart an agent."""
        if agent_id not in self.agent_configs:
            logger.error(f"Unknown agent: {agent_id}")
            return False
        
        action = RecoveryAction.FORCE_RESTART if force else RecoveryAction.GRACEFUL_RESTART
        await self._execute_recovery(agent_id, action, "manual_restart")
        return True
    
    def clear_escalation(self, agent_id: str) -> bool:
        """Clear escalation status for an agent."""
        if agent_id in self.escalated_agents:
            self.escalated_agents.remove(agent_id)
            logger.info(f"✅ Escalation cleared for {agent_id}")
            return True
        return False
    
    async def trigger_agent_failure(self, agent_id: str, reason: str) -> bool:
        """Public method to trigger agent failure for testing/demo purposes."""
        try:
            await self._handle_agent_failure(agent_id, reason)
            return True
        except Exception as e:
            logger.error(f"Failed to trigger agent failure: {e}")
            return False
    
    def get_status_summary(self) -> Dict[str, Any]:
        """Get comprehensive status summary."""
        return {
            "session_id": self.session_id,
            "total_agents": len(self.agent_configs),
            "agent_status": {
                agent_id: metrics.status.value
                for agent_id, metrics in self.agent_health.items()
            },
            "escalated_agents": list(self.escalated_agents),
            "recent_recoveries": len([
                event for event in self.recovery_history
                if (datetime.now(timezone.utc) - event.timestamp).total_seconds() < 3600
            ]),
            "restart_counts": {
                agent_id: len(restarts)
                for agent_id, restarts in self.restart_counts.items()
            },
            "critical_agents_status": {
                agent_id: self.agent_health[agent_id].status.value
                for agent_id, config in self.agent_configs.items()
                if config.critical_for_trading
            }
        }
    
    async def shutdown(self) -> None:
        """Graceful shutdown of recovery manager."""
        logger.info("🛑 Shutting down Agent Recovery Manager...")
        
        # Stop background tasks
        self._running = False
        
        # Cancel and await all tasks
        for task in self._tasks:
            task.cancel()
        
        # Wait for tasks to complete with timeout
        if self._tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=5.0
                )
            except asyncio.TimeoutError:
                logger.warning("Some monitoring tasks did not stop within timeout")
            except asyncio.CancelledError:
                pass
        
        # Stop monitoring all agents gracefully
        for agent_id, config in self.agent_configs.items():
            if config.process_id:
                try:
                    os.kill(config.process_id, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        
        logger.info("✅ Agent Recovery Manager shutdown complete")


# Example usage
async def main():
    """Example usage of the agent recovery manager."""
    
    print("🚨 Agent Recovery Manager Demo")
    print("=" * 50)
    
    streaming_config = {
        "bootstrap_servers": "localhost:9092",
        "enable_ssl": False,
        "enable_sasl": False
    }
    streaming_bus = StreamingBus(streaming_config)
    
    recovery_manager = AgentRecoveryManager(streaming_bus)
    await recovery_manager.start_monitoring()
    
    print(f"✅ Monitoring {len(recovery_manager.agent_configs)} agents")
    
    # Simulate some agent activity
    await asyncio.sleep(20)
    
    # Show status
    status = recovery_manager.get_status_summary()
    print(f"\n📊 Agent Status Summary:")
    print(f"   Total agents: {status['total_agents']}")
    print(f"   Escalated agents: {len(status['escalated_agents'])}")
    print(f"   Recent recoveries: {status['recent_recoveries']}")
    
    await recovery_manager.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
