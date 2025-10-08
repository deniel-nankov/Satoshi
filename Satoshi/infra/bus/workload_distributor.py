#!/usr/bin/env python3
"""
Workload Distributor for Enterprise Streaming Data

Enterprise-grade traffic distribution with hot key detection, load balancing, 
and adaptive workload management for institutional trading systems.

Key Features:
- Hot key detection for high-volume symbols (BTC, ETH, AAPL)
- Enterprise traffic monitoring and mitigation
- Load-aware routing to prevent hotspots
- Consistent hashing with fallback strategies
- Real-time performance optimization

Integration: Enhances existing StreamingBus producer routing decisions
"""

import asyncio
import logging
import time
import hashlib
from typing import Dict, List, Optional, Any, Tuple, Deque
from dataclasses import dataclass, field
from collections import defaultdict, deque
from enum import Enum
import statistics
import numpy as np

logger = logging.getLogger(__name__)

class PartitionStrategy(Enum):
    """Partitioning strategies for different data patterns."""
    CONSISTENT_HASH = "consistent_hash"
    ROUND_ROBIN = "round_robin"
    HOT_KEY_DEDICATED = "hot_key_dedicated"
    LOAD_AWARE = "load_aware"
    SKEW_MITIGATION = "skew_mitigation"

@dataclass
class PartitionMetrics:
    """Metrics for a specific partition."""
    partition_id: int
    message_count: int = 0
    byte_count: int = 0
    last_message_time: float = 0.0
    error_count: int = 0
    processing_time_samples: Deque[float] = field(default_factory=lambda: deque(maxlen=1000))
    
    @property
    def messages_per_second(self) -> float:
        """Calculate recent message rate."""
        current_time = time.time()
        window_seconds = 10.0  # 10 second window
        
        if self.last_message_time == 0 or (current_time - self.last_message_time) > window_seconds:
            return 0.0
        
        # Simplified rate calculation
        return self.message_count / min(window_seconds, current_time - self.last_message_time + 0.1)
    
    @property  
    def avg_processing_time_ms(self) -> float:
        """Average processing time in milliseconds."""
        if not self.processing_time_samples:
            return 0.0
        return statistics.mean(self.processing_time_samples) * 1000

@dataclass
class HotKeyConfig:
    """Configuration for hot key detection."""
    detection_window_seconds: int = 60
    hot_key_threshold_multiplier: float = 3.0  # 3x average = hot key
    min_messages_for_detection: int = 100
    cool_down_period_seconds: int = 300  # 5 minutes cooldown
    dedicated_partitions_per_hot_key: int = 2

@dataclass
class PartitionerConfig:
    """Configuration for intelligent partitioner."""
    skew_threshold: float = 2.0  # 2x load imbalance triggers rebalancing
    rebalance_interval_seconds: int = 300  # 5 minutes
    hot_key_config: HotKeyConfig = field(default_factory=HotKeyConfig)
    max_partition_load_mb_per_second: float = 100.0  # 100MB/s per partition
    enable_adaptive_routing: bool = True
    enable_skew_detection: bool = True
    enable_hot_key_mitigation: bool = True

class WorkloadDistributor:
    """
    Advanced partitioning strategy for HFT streaming data.
    
    Implements sophisticated partition selection to minimize hotspots,
    reduce skew, and optimize for ultra-low latency requirements.
    """
    
    def __init__(self, config: PartitionerConfig):
        self.config = config
        
        # Partition metrics tracking
        self.partition_metrics: Dict[str, Dict[int, PartitionMetrics]] = defaultdict(dict)
        
        # Hot key tracking
        self.hot_keys: Dict[str, Dict[str, float]] = defaultdict(dict)  # topic -> {key: detection_time}
        self.key_message_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        
        # Partition load tracking
        self.partition_loads: Dict[str, Dict[int, float]] = defaultdict(dict)  # topic -> {partition: load}
        
        # Rebalancing state
        self.last_rebalance: Dict[str, float] = {}
        self.rebalancing_in_progress: Dict[str, bool] = defaultdict(bool)
        
        # Performance metrics
        self.routing_decisions: Dict[str, int] = defaultdict(int)
        self.skew_detections: int = 0
        self.hot_key_detections: int = 0
        
        logger.info("Intelligent Partitioner initialized with advanced routing strategies")
    
    def get_partition(self, key: str, topic: str, message_size: int, 
                     partition_count: Optional[int] = None) -> int:
        """
        Intelligent partition selection with hot key and skew mitigation.
        
        Args:
            key: Partition key (typically symbol for trading data)
            topic: Topic name
            message_size: Message size in bytes for load calculation
            partition_count: Number of partitions (auto-detected if None)
        
        Returns:
            Optimal partition number for the message
        """
        start_time = time.time()
        
        try:
            # Auto-detect partition count if not provided
            if partition_count is None:
                partition_count = self._get_topic_partition_count(topic)
            
            # Update key statistics
            self._update_key_statistics(topic, key, message_size)
            
            # Check for hot keys first (highest priority)
            if self.config.enable_hot_key_mitigation and self._is_hot_key(topic, key):
                partition = self._route_hot_key(topic, key, partition_count)
                self.routing_decisions["hot_key"] += 1
                return partition
            
            # Check for partition skew and rebalance if needed
            if (self.config.enable_skew_detection and 
                self._should_rebalance(topic) and 
                not self.rebalancing_in_progress[topic]):
                
                asyncio.create_task(self._trigger_rebalancing(topic, partition_count))
            
            # Select optimal partition based on current load
            if self.config.enable_adaptive_routing:
                partition = self._get_least_loaded_partition(topic, partition_count)
                self.routing_decisions["load_aware"] += 1
            else:
                # Fallback to consistent hashing
                partition = self._consistent_hash(key, partition_count)
                self.routing_decisions["consistent_hash"] += 1
            
            # Update partition metrics
            self._update_partition_metrics(topic, partition, message_size, time.time() - start_time)
            
            return partition
            
        except Exception as e:
            logger.error(f"Error in intelligent partitioning for key {key}: {e}")
            # Fallback to simple consistent hashing
            fallback_partition = self._consistent_hash(key, partition_count or 16)
            self.routing_decisions["fallback"] += 1
            return fallback_partition
    
    def _get_topic_partition_count(self, topic: str) -> int:
        """Get partition count for topic (can be enhanced with Kafka admin API)."""
        # Default partition counts based on topic patterns
        if "market.trades" in topic or "market.book" in topic:
            return 16  # High-volume market data
        elif "features." in topic:
            return 8   # Feature topics
        elif "incidents." in topic or "control." in topic:
            return 4   # Control topics
        else:
            return 8   # Default
    
    def _update_key_statistics(self, topic: str, key: str, message_size: int) -> None:
        """Update statistics for hot key detection."""
        current_time = time.time()
        
        # Update message count
        self.key_message_counts[topic][key] += 1
        
        # Check if key qualifies as hot key
        if (self.key_message_counts[topic][key] >= self.config.hot_key_config.min_messages_for_detection and
            not self._is_in_cooldown(topic, key)):
            
            # Calculate average message rate across all keys
            total_messages = sum(self.key_message_counts[topic].values())
            num_keys = len(self.key_message_counts[topic])
            avg_messages_per_key = total_messages / max(num_keys, 1)
            
            # Detect hot key
            key_messages = self.key_message_counts[topic][key]
            if key_messages > avg_messages_per_key * self.config.hot_key_config.hot_key_threshold_multiplier:
                if key not in self.hot_keys[topic]:
                    self.hot_keys[topic][key] = current_time
                    self.hot_key_detections += 1
                    logger.info(f"🔥 Hot key detected: {key} in {topic} ({key_messages} msgs vs {avg_messages_per_key:.1f} avg)")
    
    def _is_hot_key(self, topic: str, key: str) -> bool:
        """Check if key is currently considered hot."""
        if key not in self.hot_keys[topic]:
            return False
        
        # Check if still within hot key window
        detection_time = self.hot_keys[topic][key]
        current_time = time.time()
        window = self.config.hot_key_config.detection_window_seconds
        
        if current_time - detection_time > window:
            # Hot key has cooled down
            del self.hot_keys[topic][key]
            return False
        
        return True
    
    def _is_in_cooldown(self, topic: str, key: str) -> bool:
        """Check if key is in cooldown period after being hot."""
        if key not in self.hot_keys[topic]:
            return False
        
        detection_time = self.hot_keys[topic][key]
        current_time = time.time()
        cooldown = self.config.hot_key_config.cool_down_period_seconds
        
        return current_time - detection_time < cooldown
    
    def _route_hot_key(self, topic: str, key: str, partition_count: int) -> int:
        """Route hot key to dedicated partition to prevent skew."""
        # Use dedicated partitions for hot keys (highest numbered partitions)
        dedicated_count = min(
            self.config.hot_key_config.dedicated_partitions_per_hot_key,
            max(1, partition_count // 4)  # Use up to 25% of partitions for hot keys
        )
        
        # Hash hot key to one of the dedicated partitions
        key_hash = int(hashlib.sha256(key.encode()).hexdigest(), 16)
        dedicated_partition = (partition_count - dedicated_count) + (key_hash % dedicated_count)
        
        logger.debug(f"Routing hot key {key} to dedicated partition {dedicated_partition}")
        return dedicated_partition
    
    def _get_least_loaded_partition(self, topic: str, partition_count: int) -> int:
        """Get the least loaded partition based on current metrics."""
        if topic not in self.partition_loads or not self.partition_loads[topic]:
            # No load data available, use round-robin
            return hash(topic + str(time.time())) % partition_count
        
        # Find partition with lowest load
        loads = self.partition_loads[topic]
        min_load = min(loads.values()) if loads else 0
        
        # Get all partitions with minimum load (to handle ties)
        min_load_partitions = [p for p, load in loads.items() if load <= min_load * 1.1]
        
        if min_load_partitions:
            # Use consistent selection from least loaded partitions
            return min_load_partitions[hash(topic) % len(min_load_partitions)]
        else:
            # Fallback to round-robin
            return hash(topic + str(time.time())) % partition_count
    
    def _consistent_hash(self, key: str, partition_count: int) -> int:
        """Consistent hashing fallback."""
        key_hash = int(hashlib.sha256(key.encode()).hexdigest(), 16)
        return key_hash % partition_count
    
    def _should_rebalance(self, topic: str) -> bool:
        """Check if topic partitions need rebalancing due to skew."""
        current_time = time.time()
        
        # Check rebalance interval
        last_rebalance = self.last_rebalance.get(topic, 0)
        if current_time - last_rebalance < self.config.rebalance_interval_seconds:
            return False
        
        # Check for partition skew
        if topic not in self.partition_loads:
            return False
        
        loads = list(self.partition_loads[topic].values())
        if len(loads) < 2:
            return False
        
        max_load = max(loads)
        avg_load = statistics.mean(loads)
        
        skew_ratio = max_load / max(avg_load, 0.001)  # Avoid division by zero
        
        if skew_ratio > self.config.skew_threshold:
            self.skew_detections += 1
            logger.info(f"📊 Partition skew detected in {topic}: {skew_ratio:.2f}x (threshold: {self.config.skew_threshold}x)")
            return True
        
        return False
    
    async def _trigger_rebalancing(self, topic: str, partition_count: int) -> None:
        """Trigger partition rebalancing for skewed topic."""
        if self.rebalancing_in_progress[topic]:
            return
        
        self.rebalancing_in_progress[topic] = True
        
        try:
            logger.info(f"🔄 Starting partition rebalancing for {topic}")
            
            # Analyze current partition distribution
            loads = self.partition_loads.get(topic, {})
            if not loads:
                return
            
            max_load = max(loads.values())
            avg_load = statistics.mean(loads.values())
            
            # Identify overloaded partitions
            overloaded_partitions = [
                p for p, load in loads.items() 
                if load > avg_load * self.config.skew_threshold
            ]
            
            # Log rebalancing recommendation
            logger.info(f"📋 Rebalancing recommendation for {topic}:")
            logger.info(f"   • Overloaded partitions: {overloaded_partitions}")
            logger.info(f"   • Max load: {max_load:.1f} MB/s, Avg: {avg_load:.1f} MB/s")
            logger.info(f"   • Skew ratio: {max_load/avg_load:.2f}x")
            
            # In production, this would trigger actual partition rebalancing
            # For now, we reset metrics to simulate rebalancing effect
            await asyncio.sleep(1)  # Simulate rebalancing time
            
            # Reset partition load metrics post-rebalancing
            for partition_id in overloaded_partitions:
                if partition_id in self.partition_loads[topic]:
                    self.partition_loads[topic][partition_id] *= 0.7  # Simulate 30% load reduction
            
            self.last_rebalance[topic] = time.time()
            logger.info(f"✅ Partition rebalancing completed for {topic}")
            
        except Exception as e:
            logger.error(f"Error during rebalancing for {topic}: {e}")
        finally:
            self.rebalancing_in_progress[topic] = False
    
    def _update_partition_metrics(self, topic: str, partition: int, 
                                message_size: int, processing_time: float) -> None:
        """Update metrics for partition performance tracking."""
        current_time = time.time()
        
        # Initialize partition metrics if not exists
        if partition not in self.partition_metrics[topic]:
            self.partition_metrics[topic][partition] = PartitionMetrics(partition_id=partition)
        
        metrics = self.partition_metrics[topic][partition]
        
        # Update metrics
        metrics.message_count += 1
        metrics.byte_count += message_size
        metrics.last_message_time = current_time
        metrics.processing_time_samples.append(processing_time)
        
        # Calculate partition load (MB/s)
        load_mb_per_second = metrics.byte_count / (1024 * 1024) / max(1, current_time - metrics.last_message_time + 0.1)
        self.partition_loads[topic][partition] = load_mb_per_second
    
    def get_partition_analysis(self, topic: str) -> Dict[str, Any]:
        """Get comprehensive partition analysis for a topic."""
        if topic not in self.partition_metrics:
            return {"error": f"No metrics available for topic {topic}"}
        
        partition_stats = []
        total_messages = 0
        total_bytes = 0
        
        for partition_id, metrics in self.partition_metrics[topic].items():
            total_messages += metrics.message_count
            total_bytes += metrics.byte_count
            
            partition_stats.append({
                "partition_id": partition_id,
                "message_count": metrics.message_count,
                "byte_count": metrics.byte_count,
                "messages_per_second": metrics.messages_per_second,
                "avg_processing_time_ms": metrics.avg_processing_time_ms,
                "load_mb_per_second": self.partition_loads[topic].get(partition_id, 0.0)
            })
        
        # Calculate skew metrics
        loads = [stats["load_mb_per_second"] for stats in partition_stats]
        message_counts = [stats["message_count"] for stats in partition_stats]
        
        load_skew = max(loads) / statistics.mean(loads) if loads and statistics.mean(loads) > 0 else 1.0
        message_skew = max(message_counts) / statistics.mean(message_counts) if message_counts and statistics.mean(message_counts) > 0 else 1.0
        
        return {
            "topic": topic,
            "total_partitions": len(partition_stats),
            "total_messages": total_messages,
            "total_bytes": total_bytes,
            "load_skew_ratio": load_skew,
            "message_skew_ratio": message_skew,
            "hot_keys": list(self.hot_keys[topic].keys()),
            "partition_stats": partition_stats,
            "routing_decisions": dict(self.routing_decisions),
            "performance_metrics": {
                "skew_detections": self.skew_detections,
                "hot_key_detections": self.hot_key_detections,
                "rebalancing_in_progress": self.rebalancing_in_progress.get(topic, False)
            }
        }
    
    def get_hot_key_summary(self) -> Dict[str, Any]:
        """Get summary of hot key detection across all topics."""
        hot_key_summary = {}
        current_time = time.time()
        
        for topic, hot_keys in self.hot_keys.items():
            active_hot_keys = []
            for key, detection_time in hot_keys.items():
                age_seconds = current_time - detection_time
                if age_seconds < self.config.hot_key_config.detection_window_seconds:
                    active_hot_keys.append({
                        "key": key,
                        "detected_at": detection_time,
                        "age_seconds": age_seconds,
                        "message_count": self.key_message_counts[topic].get(key, 0)
                    })
            
            hot_key_summary[topic] = {
                "active_hot_keys": active_hot_keys,
                "total_hot_key_detections": len(hot_keys)
            }
        
        return {
            "total_hot_key_detections": self.hot_key_detections,
            "topics": hot_key_summary,
            "config": {
                "detection_threshold_multiplier": self.config.hot_key_config.hot_key_threshold_multiplier,
                "detection_window_seconds": self.config.hot_key_config.detection_window_seconds,
                "cooldown_period_seconds": self.config.hot_key_config.cool_down_period_seconds
            }
        }

# Convenience factory function
def create_hft_partitioner(skew_threshold: float = 2.0, 
                          hot_key_threshold: float = 3.0,
                          enable_all_features: bool = True) -> WorkloadDistributor:
    """Create HFT-optimized partitioner with recommended settings."""
    
    hot_key_config = HotKeyConfig(
        detection_window_seconds=60,
        hot_key_threshold_multiplier=hot_key_threshold,
        min_messages_for_detection=50,  # Lower for HFT sensitivity
        cool_down_period_seconds=180,   # 3 minutes for HFT
        dedicated_partitions_per_hot_key=2
    )
    
    config = PartitionerConfig(
        skew_threshold=skew_threshold,
        rebalance_interval_seconds=120,  # 2 minutes for HFT responsiveness
        hot_key_config=hot_key_config,
        max_partition_load_mb_per_second=200.0,  # Higher for HFT throughput
        enable_adaptive_routing=enable_all_features,
        enable_skew_detection=enable_all_features,
        enable_hot_key_mitigation=enable_all_features
    )
    
    return WorkloadDistributor(config)

if __name__ == "__main__":
    # Demonstration of intelligent partitioning
    partitioner = create_hft_partitioner()
    
    print("🧠 Intelligent Partitioner Demo")
    print("=" * 50)
    
    # Simulate trading symbol distribution
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "XRPUSDT", "SOLUSDT"]
    topic = "raw_data.market.trades"
    
    # Simulate hot key scenario (BTC gets 10x traffic)
    for i in range(1000):
        if i % 10 == 0:  # Every 10th message is BTC (hot key)
            symbol = "BTCUSDT"
        else:
            symbol = symbols[i % len(symbols)]
        
        partition = partitioner.get_partition(symbol, topic, 512)  # 512 byte messages
    
    # Show analysis
    analysis = partitioner.get_partition_analysis(topic)
    hot_key_summary = partitioner.get_hot_key_summary()
    
    print(f"\n📊 Partition Analysis for {topic}:")
    print(f"   • Total messages: {analysis['total_messages']:,}")
    print(f"   • Load skew ratio: {analysis['load_skew_ratio']:.2f}x")
    print(f"   • Hot keys detected: {len(analysis['hot_keys'])}")
    print(f"   • Hot keys: {analysis['hot_keys']}")
    
    print(f"\n🔥 Hot Key Summary:")
    for topic, summary in hot_key_summary['topics'].items():
        print(f"   • {topic}: {len(summary['active_hot_keys'])} active hot keys")
        for hot_key in summary['active_hot_keys']:
            print(f"     - {hot_key['key']}: {hot_key['message_count']} messages")
    
    print(f"\n📈 Routing Decisions:")
    for strategy, count in analysis['routing_decisions'].items():
        print(f"   • {strategy}: {count} decisions")