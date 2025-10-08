#!/usr/bin/env python3
"""
Memory Governor for Enterprise Streaming Systems

Enterprise-grade memory allocation and state management with watermarking, 
automatic cleanup, and resource-bounded operations for institutional trading systems.

Key Features:
- Watermark-based event time processing
- Enterprise memory allocation and cleanup policies
- Memory-bounded state stores with intelligent eviction
- Late data handling and quarantine policies
- State checkpointing and recovery
- Performance monitoring and optimization

Integration: Provides enterprise-grade memory management for streaming processing
"""

import asyncio
import logging
import time
import pickle
import gzip
from typing import Dict, List, Optional, Any, Callable, Union, Tuple
from dataclasses import dataclass, field
from collections import defaultdict, OrderedDict
from enum import Enum
import threading
from pathlib import Path
import json

logger = logging.getLogger(__name__)

class StateStoreType(Enum):
    """Types of state stores for different use cases."""
    MEMORY = "memory"
    PERSISTENT = "persistent"
    WINDOWED = "windowed"
    SESSION = "session"

class EvictionPolicy(Enum):
    """State eviction policies for memory management."""
    LRU = "lru"              # Least Recently Used
    TTL = "ttl"              # Time To Live
    WATERMARK = "watermark"  # Based on watermark progression
    SIZE_BASED = "size_based" # Based on memory usage

class LateDataPolicy(Enum):
    """Policies for handling late-arriving data."""
    DROP = "drop"              # Drop late data
    QUARANTINE = "quarantine"  # Send to quarantine topic
    PROCESS = "process"        # Process despite being late
    GRACE_PERIOD = "grace_period" # Allow within grace period

@dataclass
class StateConfig:
    """Configuration for state management."""
    # Watermark configuration
    watermark_delay_ms: int = 300_000  # 5 minutes default
    max_out_of_order_delay_ms: int = 60_000  # 1 minute max out-of-order
    
    # State retention
    state_ttl_ms: int = 3_600_000  # 1 hour default TTL
    cleanup_interval_ms: int = 60_000  # 1 minute cleanup interval
    
    # Memory management
    max_memory_mb: int = 1024  # 1GB max memory per state store
    eviction_policy: EvictionPolicy = EvictionPolicy.WATERMARK
    eviction_threshold: float = 0.8  # Start eviction at 80% memory usage
    
    # Late data handling
    late_data_policy: LateDataPolicy = LateDataPolicy.QUARANTINE
    late_data_grace_period_ms: int = 30_000  # 30 seconds grace period
    
    # Checkpointing
    enable_checkpointing: bool = True
    checkpoint_interval_ms: int = 300_000  # 5 minutes
    checkpoint_directory: str = "/tmp/state_checkpoints"
    
    # Performance
    batch_cleanup_size: int = 1000  # Cleanup in batches
    enable_compression: bool = True  # Compress checkpoints

@dataclass
class StateEntry:
    """Individual state entry with metadata."""
    key: str
    value: Any
    created_at: int  # timestamp in milliseconds
    last_accessed: int  # timestamp in milliseconds
    access_count: int = 0
    size_bytes: int = 0
    
    def __post_init__(self):
        if self.size_bytes == 0:
            self.size_bytes = self._calculate_size()
    
    def _calculate_size(self) -> int:
        """Estimate memory size of the state entry."""
        try:
            return len(pickle.dumps(self.value))
        except:
            return len(str(self.value).encode('utf-8'))

class StateStore:
    """
    Memory-bounded state store with intelligent eviction and cleanup.
    """
    
    def __init__(self, name: str, config: StateConfig, store_type: StateStoreType = StateStoreType.MEMORY):
        self.name = name
        self.config = config
        self.store_type = store_type
        
        # State storage
        self.state: OrderedDict[str, StateEntry] = OrderedDict()
        self.watermark: int = 0
        
        # Memory tracking
        self.current_memory_bytes = 0
        self.max_memory_bytes = config.max_memory_mb * 1024 * 1024
        
        # Metrics
        self.metrics = {
            "total_entries": 0,
            "evictions": 0,
            "late_data_dropped": 0,
            "late_data_quarantined": 0,
            "cleanup_operations": 0,
            "memory_pressure_events": 0,
            "checkpoints_created": 0
        }
        
        # Cleanup tracking
        self.last_cleanup_time = time.time() * 1000
        self.last_checkpoint_time = time.time() * 1000
        
        # Thread safety
        self._lock = threading.RLock()
        
        logger.info(f"State store '{name}' initialized with {config.max_memory_mb}MB limit")
    
    def put(self, key: str, value: Any, event_time: Optional[int] = None) -> bool:
        """
        Put a value in the state store with watermark and memory checks.
        
        Args:
            key: State key
            value: State value
            event_time: Event timestamp in milliseconds (uses current time if None)
        
        Returns:
            bool: True if value was stored, False if rejected (late data, memory pressure, etc.)
        """
        current_time_ms = int(time.time() * 1000)
        event_time = event_time or current_time_ms
        
        with self._lock:
            # Check if data is too late based on watermark
            if self.watermark > 0 and event_time < (self.watermark - self.config.watermark_delay_ms):
                return self._handle_late_data(key, value, event_time)
            
            # Check memory pressure before adding
            entry = StateEntry(
                key=key,
                value=value,
                created_at=event_time,
                last_accessed=current_time_ms
            )
            
            # Memory pressure check
            if self._would_exceed_memory_limit(entry.size_bytes):
                if not self._evict_entries_for_space(entry.size_bytes):
                    self.metrics["memory_pressure_events"] += 1
                    logger.warning(f"Cannot store key '{key}' - memory limit exceeded and eviction failed")
                    return False
            
            # Store the entry
            old_entry = self.state.get(key)
            if old_entry:
                self.current_memory_bytes -= old_entry.size_bytes
            
            self.state[key] = entry
            self.current_memory_bytes += entry.size_bytes
            self.metrics["total_entries"] = len(self.state)
            
            # Move to end for LRU ordering
            self.state.move_to_end(key)
            
            return True
    
    def get(self, key: str) -> Optional[Any]:
        """Get a value from the state store, updating access time."""
        with self._lock:
            entry = self.state.get(key)
            if entry:
                entry.last_accessed = int(time.time() * 1000)
                entry.access_count += 1
                # Move to end for LRU ordering
                self.state.move_to_end(key)
                return entry.value
            return None
    
    def contains(self, key: str) -> bool:
        """Check if key exists in state store."""
        with self._lock:
            return key in self.state
    
    def remove(self, key: str) -> bool:
        """Remove a key from the state store."""
        with self._lock:
            entry = self.state.pop(key, None)
            if entry:
                self.current_memory_bytes -= entry.size_bytes
                self.metrics["total_entries"] = len(self.state)
                return True
            return False
    
    def update_watermark(self, new_watermark: int) -> None:
        """Update the watermark and trigger cleanup of expired state."""
        with self._lock:
            if new_watermark > self.watermark:
                old_watermark = self.watermark
                self.watermark = new_watermark
                logger.debug(f"Watermark updated from {old_watermark} to {new_watermark}")
                
                # Trigger cleanup based on new watermark
                asyncio.create_task(self._cleanup_expired_state())
    
    def _handle_late_data(self, key: str, value: Any, event_time: int) -> bool:
        """Handle late-arriving data based on configured policy."""
        policy = self.config.late_data_policy
        
        if policy == LateDataPolicy.DROP:
            self.metrics["late_data_dropped"] += 1
            logger.debug(f"Dropped late data for key '{key}' (event_time: {event_time}, watermark: {self.watermark})")
            return False
            
        elif policy == LateDataPolicy.QUARANTINE:
            self.metrics["late_data_quarantined"] += 1
            # In production, this would publish to quarantine topic
            logger.info(f"Quarantined late data for key '{key}' (event_time: {event_time}, watermark: {self.watermark})")
            return False
            
        elif policy == LateDataPolicy.GRACE_PERIOD:
            grace_cutoff = self.watermark - self.config.late_data_grace_period_ms
            if event_time >= grace_cutoff:
                # Within grace period, process normally
                return True
            else:
                self.metrics["late_data_dropped"] += 1
                return False
                
        elif policy == LateDataPolicy.PROCESS:
            # Process late data despite watermark
            return True
        
        return False
    
    def _would_exceed_memory_limit(self, additional_bytes: int) -> bool:
        """Check if adding data would exceed memory limit."""
        return (self.current_memory_bytes + additional_bytes) > self.max_memory_bytes
    
    def _evict_entries_for_space(self, required_bytes: int) -> bool:
        """Evict entries to make space for new data."""
        if self.config.eviction_policy == EvictionPolicy.LRU:
            return self._evict_lru_entries(required_bytes)
        elif self.config.eviction_policy == EvictionPolicy.WATERMARK:
            return self._evict_watermark_expired_entries(required_bytes)
        elif self.config.eviction_policy == EvictionPolicy.TTL:
            return self._evict_ttl_expired_entries(required_bytes)
        else:
            return False
    
    def _evict_lru_entries(self, required_bytes: int) -> bool:
        """Evict least recently used entries."""
        freed_bytes = 0
        evicted_count = 0
        
        # OrderedDict maintains insertion/access order - iterate from beginning (oldest)
        keys_to_evict = []
        for key, entry in self.state.items():
            keys_to_evict.append(key)
            freed_bytes += entry.size_bytes
            evicted_count += 1
            
            if freed_bytes >= required_bytes:
                break
        
        # Remove evicted entries
        for key in keys_to_evict:
            entry = self.state.pop(key, None)
            if entry:
                self.current_memory_bytes -= entry.size_bytes
        
        self.metrics["evictions"] += evicted_count
        self.metrics["total_entries"] = len(self.state)
        
        if evicted_count > 0:
            logger.info(f"Evicted {evicted_count} LRU entries, freed {freed_bytes} bytes")
        
        return freed_bytes >= required_bytes
    
    def _evict_watermark_expired_entries(self, required_bytes: int) -> bool:
        """Evict entries that are older than watermark."""
        current_time = int(time.time() * 1000)
        cutoff_time = self.watermark - self.config.state_ttl_ms
        
        freed_bytes = 0
        evicted_count = 0
        keys_to_evict = []
        
        for key, entry in self.state.items():
            if entry.created_at < cutoff_time:
                keys_to_evict.append(key)
                freed_bytes += entry.size_bytes
                evicted_count += 1
                
                if freed_bytes >= required_bytes:
                    break
        
        # Remove expired entries
        for key in keys_to_evict:
            entry = self.state.pop(key, None)
            if entry:
                self.current_memory_bytes -= entry.size_bytes
        
        self.metrics["evictions"] += evicted_count
        self.metrics["total_entries"] = len(self.state)
        
        if evicted_count > 0:
            logger.info(f"Evicted {evicted_count} watermark-expired entries, freed {freed_bytes} bytes")
        
        return freed_bytes >= required_bytes
    
    def _evict_ttl_expired_entries(self, required_bytes: int) -> bool:
        """Evict entries that have exceeded TTL."""
        current_time = int(time.time() * 1000)
        cutoff_time = current_time - self.config.state_ttl_ms
        
        freed_bytes = 0
        evicted_count = 0
        keys_to_evict = []
        
        for key, entry in self.state.items():
            if entry.last_accessed < cutoff_time:
                keys_to_evict.append(key)
                freed_bytes += entry.size_bytes
                evicted_count += 1
                
                if freed_bytes >= required_bytes:
                    break
        
        # Remove TTL-expired entries
        for key in keys_to_evict:
            entry = self.state.pop(key, None)
            if entry:
                self.current_memory_bytes -= entry.size_bytes
        
        self.metrics["evictions"] += evicted_count
        self.metrics["total_entries"] = len(self.state)
        
        if evicted_count > 0:
            logger.info(f"Evicted {evicted_count} TTL-expired entries, freed {freed_bytes} bytes")
        
        return freed_bytes >= required_bytes
    
    async def _cleanup_expired_state(self) -> None:
        """Asynchronous cleanup of expired state entries."""
        current_time = int(time.time() * 1000)
        
        # Check cleanup interval
        if (current_time - self.last_cleanup_time) < self.config.cleanup_interval_ms:
            return
        
        with self._lock:
            self.last_cleanup_time = current_time
            cutoff_time = self.watermark - self.config.state_ttl_ms
            
            keys_to_remove = []
            for key, entry in self.state.items():
                if entry.created_at < cutoff_time:
                    keys_to_remove.append(key)
                
                # Batch cleanup to avoid holding lock too long
                if len(keys_to_remove) >= self.config.batch_cleanup_size:
                    break
            
            # Remove expired entries
            cleaned_bytes = 0
            for key in keys_to_remove:
                entry = self.state.pop(key, None)
                if entry:
                    cleaned_bytes += entry.size_bytes
                    self.current_memory_bytes -= entry.size_bytes
            
            if keys_to_remove:
                self.metrics["cleanup_operations"] += 1
                self.metrics["total_entries"] = len(self.state)
                logger.info(f"Cleaned up {len(keys_to_remove)} expired entries, freed {cleaned_bytes} bytes")
    
    async def create_checkpoint(self) -> Optional[str]:
        """Create a checkpoint of current state."""
        if not self.config.enable_checkpointing:
            return None
        
        current_time = int(time.time() * 1000)
        
        # Check checkpoint interval
        if (current_time - self.last_checkpoint_time) < self.config.checkpoint_interval_ms:
            return None
        
        checkpoint_path = None
        
        try:
            # Create checkpoint directory
            checkpoint_dir = Path(self.config.checkpoint_directory)
            checkpoint_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate checkpoint filename
            checkpoint_filename = f"checkpoint_{self.name}_{current_time}.pkl"
            if self.config.enable_compression:
                checkpoint_filename += ".gz"
            
            checkpoint_path = checkpoint_dir / checkpoint_filename
            
            # Serialize state
            with self._lock:
                checkpoint_data = {
                    "watermark": self.watermark,
                    "state": dict(self.state),
                    "metrics": self.metrics.copy(),
                    "timestamp": current_time
                }
                
                # Write checkpoint
                if self.config.enable_compression:
                    with gzip.open(checkpoint_path, 'wb') as f:
                        pickle.dump(checkpoint_data, f)
                else:
                    with open(checkpoint_path, 'wb') as f:
                        pickle.dump(checkpoint_data, f)
                
                self.last_checkpoint_time = current_time
                self.metrics["checkpoints_created"] += 1
                
                logger.info(f"Created checkpoint: {checkpoint_path} ({len(self.state)} entries)")
                
            return str(checkpoint_path)
            
        except Exception as e:
            logger.error(f"Failed to create checkpoint: {e}")
            return None
    
    async def restore_from_checkpoint(self, checkpoint_path: str) -> bool:
        """Restore state from checkpoint."""
        try:
            path = Path(checkpoint_path)
            if not path.exists():
                logger.error(f"Checkpoint file not found: {checkpoint_path}")
                return False
            
            # Load checkpoint data
            if checkpoint_path.endswith('.gz'):
                with gzip.open(checkpoint_path, 'rb') as f:
                    checkpoint_data = pickle.load(f)
            else:
                with open(checkpoint_path, 'rb') as f:
                    checkpoint_data = pickle.load(f)
            
            with self._lock:
                # Restore state
                self.watermark = checkpoint_data["watermark"]
                self.state = OrderedDict(checkpoint_data["state"])
                
                # Recalculate memory usage
                self.current_memory_bytes = sum(entry.size_bytes for entry in self.state.values())
                
                # Restore metrics (merge with current)
                for key, value in checkpoint_data["metrics"].items():
                    self.metrics[key] = value
                
                self.metrics["total_entries"] = len(self.state)
                
                logger.info(f"Restored state from checkpoint: {checkpoint_path} ({len(self.state)} entries, {self.current_memory_bytes} bytes)")
                
            return True
            
        except Exception as e:
            logger.error(f"Failed to restore from checkpoint {checkpoint_path}: {e}")
            return False
    
    def get_state_summary(self) -> Dict[str, Any]:
        """Get comprehensive state store summary."""
        with self._lock:
            memory_usage_percent = (self.current_memory_bytes / self.max_memory_bytes) * 100
            
            return {
                "name": self.name,
                "store_type": self.store_type.value,
                "watermark": self.watermark,
                "entry_count": len(self.state),
                "memory_usage": {
                    "current_bytes": self.current_memory_bytes,
                    "current_mb": self.current_memory_bytes / (1024 * 1024),
                    "max_mb": self.config.max_memory_mb,
                    "usage_percent": memory_usage_percent
                },
                "config": {
                    "watermark_delay_ms": self.config.watermark_delay_ms,
                    "state_ttl_ms": self.config.state_ttl_ms,
                    "eviction_policy": self.config.eviction_policy.value,
                    "late_data_policy": self.config.late_data_policy.value
                },
                "metrics": self.metrics.copy(),
                "health": {
                    "memory_pressure": "high" if memory_usage_percent > 80 else "normal",
                    "last_cleanup": self.last_cleanup_time,
                    "last_checkpoint": self.last_checkpoint_time
                }
            }

class MemoryGovernor:
    """
    Manager for multiple state stores with coordinated watermarking and cleanup.
    
    Provides production-grade state management for streaming applications
    with memory bounds, watermarking, and automatic cleanup.
    """
    
    def __init__(self, global_config: StateConfig = None):
        self.global_config = global_config or StateConfig()
        
        # State stores registry
        self.state_stores: Dict[str, StateStore] = {}
        
        # Global watermark tracking
        self.global_watermark: int = 0
        self.topic_watermarks: Dict[str, int] = {}
        
        # Cleanup and monitoring tasks
        self.cleanup_task: Optional[asyncio.Task] = None
        self.monitoring_task: Optional[asyncio.Task] = None
        self.checkpoint_task: Optional[asyncio.Task] = None
        
        # Thread safety
        self._lock = threading.RLock()
        
        logger.info("Bounded State Manager initialized")
    
    def create_state_store(self, name: str, config: StateConfig = None, 
                          store_type: StateStoreType = StateStoreType.MEMORY) -> StateStore:
        """Create a new state store with the given configuration."""
        effective_config = config or self.global_config
        
        with self._lock:
            if name in self.state_stores:
                raise ValueError(f"State store '{name}' already exists")
            
            state_store = StateStore(name, effective_config, store_type)
            self.state_stores[name] = state_store
            
            logger.info(f"Created state store '{name}' with type {store_type.value}")
            return state_store
    
    def get_state_store(self, name: str) -> Optional[StateStore]:
        """Get an existing state store."""
        return self.state_stores.get(name)
    
    def remove_state_store(self, name: str) -> bool:
        """Remove a state store."""
        with self._lock:
            store = self.state_stores.pop(name, None)
            if store:
                logger.info(f"Removed state store '{name}'")
                return True
            return False
    
    async def process_message(self, topic: str, message: Dict[str, Any], 
                            state_store_name: str = None) -> bool:
        """
        Process a message with proper watermarking and state management.
        
        Args:
            topic: Topic name for watermark tracking
            message: Message to process
            state_store_name: Optional specific state store to use
        
        Returns:
            bool: True if message was processed, False if rejected (late, etc.)
        """
        event_time = message.get('timestamp', int(time.time() * 1000))
        
        # Update topic watermark
        await self._update_topic_watermark(topic, event_time)
        
        # If specific state store requested, use it
        if state_store_name:
            store = self.get_state_store(state_store_name)
            if not store:
                logger.error(f"State store '{state_store_name}' not found")
                return False
            
            # Update store watermark
            store.update_watermark(self.topic_watermarks.get(topic, 0))
            return True
        
        # Update all state stores with new watermark
        current_watermark = self.topic_watermarks.get(topic, 0)
        with self._lock:
            for store in self.state_stores.values():
                store.update_watermark(current_watermark)
        
        return True
    
    async def _update_topic_watermark(self, topic: str, event_time: int) -> None:
        """Update watermark for a topic."""
        # Simple watermark strategy: event_time - watermark_delay
        watermark_delay = self.global_config.watermark_delay_ms
        new_watermark = event_time - watermark_delay
        
        # Only advance watermark (never go backwards)
        current_watermark = self.topic_watermarks.get(topic, 0)
        if new_watermark > current_watermark:
            self.topic_watermarks[topic] = new_watermark
            
            # Update global watermark (minimum across all topics)
            if self.topic_watermarks:
                self.global_watermark = min(self.topic_watermarks.values())
    
    async def start_background_tasks(self) -> None:
        """Start background tasks for cleanup, monitoring, and checkpointing."""
        if self.cleanup_task is None:
            self.cleanup_task = asyncio.create_task(self._cleanup_loop())
        
        if self.monitoring_task is None:
            self.monitoring_task = asyncio.create_task(self._monitoring_loop())
        
        if self.global_config.enable_checkpointing and self.checkpoint_task is None:
            self.checkpoint_task = asyncio.create_task(self._checkpoint_loop())
        
        logger.info("Started background tasks for state management")
    
    async def stop_background_tasks(self) -> None:
        """Stop all background tasks."""
        tasks = [self.cleanup_task, self.monitoring_task, self.checkpoint_task]
        
        for task in tasks:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        self.cleanup_task = None
        self.monitoring_task = None 
        self.checkpoint_task = None
        
        logger.info("Stopped background tasks")
    
    async def _cleanup_loop(self) -> None:
        """Background cleanup loop."""
        while True:
            try:
                # Trigger cleanup on all state stores
                cleanup_tasks = []
                for store in self.state_stores.values():
                    cleanup_tasks.append(store._cleanup_expired_state())
                
                if cleanup_tasks:
                    await asyncio.gather(*cleanup_tasks, return_exceptions=True)
                
                # Wait for next cleanup interval
                await asyncio.sleep(self.global_config.cleanup_interval_ms / 1000.0)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
                await asyncio.sleep(5)  # Wait before retrying
    
    async def _monitoring_loop(self) -> None:
        """Background monitoring loop."""
        while True:
            try:
                # Check memory pressure across all stores
                total_memory_mb = 0
                high_pressure_stores = []
                
                for name, store in self.state_stores.items():
                    memory_mb = store.current_memory_bytes / (1024 * 1024)
                    total_memory_mb += memory_mb
                    
                    usage_percent = (store.current_memory_bytes / store.max_memory_bytes) * 100
                    if usage_percent > 80:  # High pressure threshold
                        high_pressure_stores.append((name, usage_percent))
                
                # Log memory status
                if high_pressure_stores:
                    logger.warning(f"High memory pressure detected in stores: {high_pressure_stores}")
                
                logger.debug(f"State manager memory usage: {total_memory_mb:.1f}MB across {len(self.state_stores)} stores")
                
                # Wait for next monitoring interval
                await asyncio.sleep(30)  # Monitor every 30 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(5)
    
    async def _checkpoint_loop(self) -> None:
        """Background checkpointing loop."""
        while True:
            try:
                # Create checkpoints for all stores
                checkpoint_tasks = []
                for store in self.state_stores.values():
                    checkpoint_tasks.append(store.create_checkpoint())
                
                if checkpoint_tasks:
                    results = await asyncio.gather(*checkpoint_tasks, return_exceptions=True)
                    
                    # Log checkpoint results
                    successful_checkpoints = [r for r in results if isinstance(r, str)]
                    if successful_checkpoints:
                        logger.info(f"Created {len(successful_checkpoints)} checkpoints")
                
                # Wait for next checkpoint interval
                await asyncio.sleep(self.global_config.checkpoint_interval_ms / 1000.0)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in checkpoint loop: {e}")
                await asyncio.sleep(30)  # Wait before retrying
    
    def get_system_summary(self) -> Dict[str, Any]:
        """Get comprehensive system summary."""
        with self._lock:
            total_entries = sum(len(store.state) for store in self.state_stores.values())
            total_memory_bytes = sum(store.current_memory_bytes for store in self.state_stores.values())
            
            store_summaries = {}
            for name, store in self.state_stores.items():
                store_summaries[name] = store.get_state_summary()
            
            return {
                "total_stores": len(self.state_stores),
                "total_entries": total_entries,
                "total_memory_mb": total_memory_bytes / (1024 * 1024),
                "global_watermark": self.global_watermark,
                "topic_watermarks": self.topic_watermarks.copy(),
                "stores": store_summaries,
                "background_tasks": {
                    "cleanup_running": self.cleanup_task is not None and not self.cleanup_task.done(),
                    "monitoring_running": self.monitoring_task is not None and not self.monitoring_task.done(),
                    "checkpoint_running": self.checkpoint_task is not None and not self.checkpoint_task.done()
                }
            }

# Convenience functions
def create_hft_state_config(memory_mb: int = 512, 
                           watermark_delay_ms: int = 60_000) -> StateConfig:
    """Create HFT-optimized state configuration."""
    return StateConfig(
        watermark_delay_ms=watermark_delay_ms,     # 1 minute for HFT
        max_out_of_order_delay_ms=30_000,         # 30 seconds max out-of-order
        state_ttl_ms=1_800_000,                   # 30 minutes TTL for HFT
        cleanup_interval_ms=30_000,               # 30 seconds cleanup
        max_memory_mb=memory_mb,
        eviction_policy=EvictionPolicy.WATERMARK,
        late_data_policy=LateDataPolicy.QUARANTINE,
        late_data_grace_period_ms=15_000,         # 15 seconds grace
        enable_checkpointing=True,
        checkpoint_interval_ms=180_000,           # 3 minutes checkpoints
        batch_cleanup_size=500                    # Smaller batches for low latency
    )

if __name__ == "__main__":
    # Demonstration
    import asyncio
    
    async def demo_bounded_state_management():
        print("🗄️  Bounded State Manager Demo")
        print("=" * 50)
        
        # Create HFT-optimized configuration
        config = create_hft_state_config(memory_mb=128)  # 128MB for demo
        
        # Create state manager
        state_manager = MemoryGovernor(config)
        
        # Create different types of state stores
        market_data_store = state_manager.create_state_store("market_data", config)
        position_store = state_manager.create_state_store("positions", config)
        
        # Start background tasks
        await state_manager.start_background_tasks()
        
        print(f"\n📊 Created State Stores:")
        print(f"   • market_data: {config.max_memory_mb}MB limit")
        print(f"   • positions: {config.max_memory_mb}MB limit")
        
        # Simulate market data processing
        print(f"\n💾 Simulating Market Data Processing...")
        
        current_time = int(time.time() * 1000)
        symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
        
        # Add market data entries
        for i in range(1000):
            symbol = symbols[i % len(symbols)]
            price_data = {
                "symbol": symbol,
                "price": 50000 + (i * 10),
                "volume": 1000 + i,
                "timestamp": current_time + (i * 1000)
            }
            
            key = f"{symbol}_{current_time + (i * 1000)}"
            market_data_store.put(key, price_data, current_time + (i * 1000))
            
            # Simulate watermark progression
            if i % 100 == 0:
                await state_manager.process_message("market_data_topic", {
                    "timestamp": current_time + (i * 1000)
                })
        
        # Add position data
        positions = ["long_btc", "short_eth", "neutral_bnb"]
        for i, position in enumerate(positions):
            position_data = {
                "symbol": symbols[i],
                "size": 1000 * (i + 1),
                "entry_price": 50000 + (i * 1000),
                "unrealized_pnl": (i - 1) * 100
            }
            position_store.put(position, position_data)
        
        # Show system summary
        summary = state_manager.get_system_summary()
        
        print(f"\n📈 System Summary:")
        print(f"   • Total Stores: {summary['total_stores']}")
        print(f"   • Total Entries: {summary['total_entries']:,}")
        print(f"   • Total Memory: {summary['total_memory_mb']:.1f}MB")
        print(f"   • Global Watermark: {summary['global_watermark']}")
        
        print(f"\n🗄️  Store Details:")
        for name, store_info in summary['stores'].items():
            memory_info = store_info['memory_usage']
            print(f"   • {name}:")
            print(f"     - Entries: {store_info['entry_count']:,}")
            print(f"     - Memory: {memory_info['current_mb']:.1f}MB ({memory_info['usage_percent']:.1f}%)")
            print(f"     - Watermark: {store_info['watermark']}")
            print(f"     - Evictions: {store_info['metrics']['evictions']}")
        
        # Test memory pressure and eviction
        print(f"\n🔄 Testing Memory Pressure and Eviction...")
        
        # Fill up memory to trigger eviction
        large_data = {"large_payload": "x" * 10000}  # 10KB payload
        eviction_triggered = False
        
        for i in range(100):
            success = market_data_store.put(f"large_entry_{i}", large_data)
            if not success:
                eviction_triggered = True
                break
        
        final_summary = state_manager.get_system_summary()
        market_store_info = final_summary['stores']['market_data']
        
        print(f"   • Memory pressure handling:")
        print(f"     - Final entries: {market_store_info['entry_count']:,}")
        print(f"     - Final memory: {market_store_info['memory_usage']['current_mb']:.1f}MB")
        print(f"     - Evictions triggered: {market_store_info['metrics']['evictions']}")
        print(f"     - Memory pressure events: {market_store_info['metrics']['memory_pressure_events']}")
        
        # Test late data handling
        print(f"\n⏰ Testing Late Data Handling...")
        
        # Send data that's older than watermark
        old_timestamp = current_time - 600_000  # 10 minutes old
        late_data_success = market_data_store.put("late_data", {"test": "late"}, old_timestamp)
        
        print(f"   • Late data result: {'Accepted' if late_data_success else 'Rejected (as expected)'}")
        print(f"   • Late data quarantined: {market_store_info['metrics']['late_data_quarantined']}")
        
        # Clean shutdown
        await state_manager.stop_background_tasks()
        
        print(f"\n✅ Demo completed successfully!")
    
    asyncio.run(demo_bounded_state_management())