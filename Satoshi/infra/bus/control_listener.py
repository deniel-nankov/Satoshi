#!/usr/bin/env python3
"""
Control Listener Microservice
Listens to control.circuit_breaker topic and applies circuit breaker commands to the streaming bus.

This is a tiny consumer that bridges control messages to the transport layer,
maintaining the pure transport design of the streaming bus.
"""

import asyncio
import json
import logging
from typing import Dict, Any, Optional
from streaming_bus import StreamingBus

logger = logging.getLogger(__name__)

class ControlListener:
    """
    Microservice that listens to control topics and applies transport-level controls.
    
    This service maintains the separation between control plane and transport layer
    by consuming control messages and invoking appropriate transport methods.
    """
    
    def __init__(self, bus: StreamingBus, consumer_group: str = "control-listener"):
        """
        Initialize control listener.
        
        Args:
            bus: StreamingBus instance to control
            consumer_group: Consumer group for the control listener
        """
        self.bus = bus
        self.consumer_group = consumer_group
        self.running = False
        
        logger.info(f"Control listener initialized for group: {consumer_group}")
    
    async def start(self) -> None:
        """Start the control listener service."""
        if self.running:
            logger.warning("Control listener already running")
            return
            
        self.running = True
        logger.info("Starting control listener service...")
        
        # Subscribe to control topics
        def sync_handler(topic: str, partition_key: str, payload: Dict[str, Any], headers: Dict[str, str]) -> None:
            asyncio.create_task(self._handle_control_message(topic, partition_key, payload, headers))

        await self.bus.subscribe_with_worker_pool(
            consumer_group=self.consumer_group,
            topics=["control.circuit_breaker"],
            handler=sync_handler,
            pool_size=2  # Small pool since control messages are infrequent
        )
    
    async def stop(self) -> None:
        """Stop the control listener service."""
        self.running = False
        logger.info("Stopping control listener service...")
        
        # The streaming bus will handle consumer cleanup
        await self.bus.graceful_shutdown()
    
    async def _handle_control_message(self, topic: str, partition_key: str, 
                                     payload: Dict[str, Any], headers: Dict[str, str]) -> None:
        """
        Handle control messages from the control plane.
        
        Args:
            topic: Control topic name
            partition_key: Message partition key
            payload: Control message payload
            headers: Message headers
        """
        try:
            if topic == "control.circuit_breaker":
                await self._handle_circuit_breaker_command(payload, headers)
            else:
                logger.warning(f"Unknown control topic: {topic}")
                
        except Exception as e:
            logger.error(f"Error handling control message from {topic}: {e}")
    
    async def _handle_circuit_breaker_command(self, payload: Dict[str, Any], 
                                            headers: Dict[str, str]) -> None:
        """
        Handle circuit breaker control commands.
        
        Expected payload format:
        {
            "command": "set_circuit_breaker",
            "target_topic": "clean.market.trades", 
            "paused": true,
            "reason": "High error rate detected",
            "issued_by": "risk_monitor",
            "timestamp": "2025-10-03T10:30:00Z"
        }
        """
        try:
            command = payload.get("command")
            target_topic = payload.get("target_topic")
            paused = payload.get("paused", False)
            reason = payload.get("reason", "No reason provided")
            issued_by = payload.get("issued_by", "unknown")
            
            if command != "set_circuit_breaker":
                logger.warning(f"Unknown circuit breaker command: {command}")
                return
                
            if not target_topic:
                logger.error("Circuit breaker command missing target_topic")
                return
            
            # Apply circuit breaker to transport layer
            await self.bus.set_circuit_breaker(target_topic, paused)
            
            status = "ACTIVATED" if paused else "DEACTIVATED"
            logger.info(f"Circuit breaker {status} for topic '{target_topic}' - Reason: {reason} (by {issued_by})")
            
            # Optional: Publish acknowledgment back to control plane
            ack_payload = {
                "command_ack": "circuit_breaker_applied",
                "target_topic": target_topic,
                "paused": paused,
                "applied_at": headers.get("timestamp"),
                "processed_by": self.consumer_group
            }
            
            await self.bus.publish_with_headers(
                topic="control.command_acks",
                partition_key=target_topic,
                payload=ack_payload,
                headers={"source": "control_listener", "ack_type": "circuit_breaker"}
            )
            
        except Exception as e:
            logger.error(f"Error processing circuit breaker command: {e}")

# Example usage and integration
async def main():
    """Example of running the control listener service."""
    
    # Configure streaming bus
    bus_config = {
        "bootstrap_servers": ["localhost:9092"],
        "client_id": "control-listener-service",
        "security_protocol": "PLAINTEXT",
        "environment": "development"
    }
    
    # Create streaming bus and control listener
    bus = StreamingBus(bus_config)
    control_listener = ControlListener(bus, consumer_group="satoshi-control-listener")
    
    try:
        # Create topics if needed
        await bus.create_topics_from_config()
        
        # Start control listener
        await control_listener.start()
        
        logger.info("Control listener service running. Press Ctrl+C to stop.")
        
        # Keep running until interrupted
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Shutdown signal received")
    except Exception as e:
        logger.error(f"Control listener service error: {e}")
    finally:
        await control_listener.stop()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    asyncio.run(main())
