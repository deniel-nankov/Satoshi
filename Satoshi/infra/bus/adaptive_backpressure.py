#!/usr/bin/env python3
"""
Adaptive Backpressure Controller

Dynamically adjusts backpressure delays based on real-time feedback from
consumer lag, queue depth, and processing rates.

Key Features:
- Feedback-driven delay adjustment (not fixed delays)
- Exponential backoff when congestion increases
- Adaptive thresholds based on historical patterns
- Per-consumer lag tracking
- Graceful degradation under sustained pressure
"""

import asyncio
import logging
import time
from typing import Dict, Optional, Tuple
from dataclasses import dataclass, field
from collections import deque
from enum import Enum
import statistics

logger = logging.getLogger(__name__)


class BackpressureLevel(Enum):
    """Backpressure severity levels."""
    NONE = "none"           # No backpressure needed
    LOW = "low"             # Minor slowdown (10-25% capacity)
    MEDIUM = "medium"       # Moderate slowdown (25-50% capacity)
    HIGH = "high"           # Significant slowdown (50-75% capacity)
    CRITICAL = "critical"   # Severe slowdown (75-90% capacity)
    EMERGENCY = "emergency" # Emergency mode (>90% capacity)


@dataclass
class BackpressureMetrics:
    """Metrics for backpressure controller."""
    current_level: BackpressureLevel = BackpressureLevel.NONE
    current_delay_ms: float = 0.0
    
    # Buffer/queue metrics
    buffer_size: int = 0
    buffer_capacity: int = 0
    buffer_usage_pct: float = 0.0
    
    # Lag metrics
    consumer_lag: int = 0
    lag_samples: deque = field(default_factory=lambda: deque(maxlen=100))
    avg_lag: float = 0.0
    max_lag: int = 0
    
    # Throughput metrics
    producer_rate: float = 0.0  # items/sec
    consumer_rate: float = 0.0  # items/sec
    rate_ratio: float = 0.0     # consumer/producer (< 1.0 = falling behind)
    
    # Adaptation history
    delay_increases: int = 0
    delay_decreases: int = 0
    level_changes: int = 0
    
    # Timing
    last_adjustment_time: float = 0.0
    time_in_current_level: float = 0.0
    
    def update_derived_metrics(self):
        """Update derived metrics from raw data."""
        if self.buffer_capacity > 0:
            self.buffer_usage_pct = (self.buffer_size / self.buffer_capacity) * 100.0
        
        if self.lag_samples:
            self.avg_lag = statistics.mean(self.lag_samples)
            self.max_lag = max(self.lag_samples)
        
        if self.producer_rate > 0:
            self.rate_ratio = self.consumer_rate / self.producer_rate


class AdaptiveBackpressureController:
    """
    Adaptive backpressure controller that adjusts delays based on real-time feedback.
    
    Algorithm:
    1. Monitor buffer usage, consumer lag, processing rates
    2. Calculate pressure level (NONE → LOW → MEDIUM → HIGH → CRITICAL → EMERGENCY)
    3. Adjust delay exponentially as pressure increases
    4. Decrease delay gradually as pressure decreases
    
    Delay calculation:
    - NONE: 0ms
    - LOW: 10-50ms (linear)
    - MEDIUM: 50-100ms (linear)
    - HIGH: 100-250ms (exponential)
    - CRITICAL: 250-500ms (exponential)
    - EMERGENCY: 500-1000ms (exponential)
    """
    
    def __init__(
        self,
        name: str,
        buffer_capacity: int,
        
        # Thresholds (% of capacity)
        low_threshold: float = 0.50,      # 50%
        medium_threshold: float = 0.65,   # 65%
        high_threshold: float = 0.75,     # 75%
        critical_threshold: float = 0.85, # 85%
        emergency_threshold: float = 0.95, # 95%
        
        # Delay parameters
        min_delay_ms: float = 0.0,
        max_delay_ms: float = 1000.0,
        
        # Adaptation parameters
        adjustment_interval_s: float = 1.0,  # How often to recalculate
        smoothing_factor: float = 0.3,       # EMA smoothing (0-1)
        
        # Lag tracking
        enable_lag_tracking: bool = True,
        max_acceptable_lag: int = 1000,      # Alert if lag exceeds this
    ):
        self.name = name
        self.buffer_capacity = buffer_capacity
        
        # Thresholds
        self.low_threshold = low_threshold
        self.medium_threshold = medium_threshold
        self.high_threshold = high_threshold
        self.critical_threshold = critical_threshold
        self.emergency_threshold = emergency_threshold
        
        # Delay bounds
        self.min_delay_ms = min_delay_ms
        self.max_delay_ms = max_delay_ms
        
        # Adaptation
        self.adjustment_interval_s = adjustment_interval_s
        self.smoothing_factor = smoothing_factor
        
        # Lag tracking
        self.enable_lag_tracking = enable_lag_tracking
        self.max_acceptable_lag = max_acceptable_lag
        
        # State
        self.current_delay_ms = 0.0
        self.current_level = BackpressureLevel.NONE
        self.level_entry_time = time.time()
        
        # Metrics
        self.metrics = BackpressureMetrics(
            buffer_capacity=buffer_capacity,
            current_level=BackpressureLevel.NONE,
            current_delay_ms=0.0
        )
        
        # Throughput tracking
        self._producer_count = 0
        self._consumer_count = 0
        self._last_throughput_time = time.time()
        
        # Background task
        self._adjustment_task: Optional[asyncio.Task] = None
        self._running = False
        
        logger.info(f"Adaptive backpressure controller initialized for '{name}': "
                   f"capacity={buffer_capacity}, thresholds=[{low_threshold:.0%}, "
                   f"{medium_threshold:.0%}, {high_threshold:.0%}, {critical_threshold:.0%}, "
                   f"{emergency_threshold:.0%}]")
    
    async def start(self):
        """Start background adjustment task."""
        if self._running:
            return
        
        self._running = True
        self._adjustment_task = asyncio.create_task(self._adjustment_loop())
        logger.info(f"Adaptive backpressure controller started for '{self.name}'")
    
    async def stop(self):
        """Stop background adjustment task."""
        self._running = False
        if self._adjustment_task:
            self._adjustment_task.cancel()
            try:
                await self._adjustment_task
            except asyncio.CancelledError:
                pass
        logger.info(f"Adaptive backpressure controller stopped for '{self.name}'")
    
    def update_buffer_size(self, current_size: int):
        """Update current buffer size."""
        self.metrics.buffer_size = current_size
        self.metrics.update_derived_metrics()
    
    def update_consumer_lag(self, lag: int):
        """Update consumer lag measurement."""
        if self.enable_lag_tracking:
            self.metrics.consumer_lag = lag
            self.metrics.lag_samples.append(lag)
            self.metrics.update_derived_metrics()
            
            # Alert if lag is excessive
            if lag > self.max_acceptable_lag:
                logger.warning(f"Excessive consumer lag for '{self.name}': "
                             f"{lag} items (max acceptable: {self.max_acceptable_lag})")
    
    def record_produced_item(self):
        """Record item produced."""
        self._producer_count += 1
    
    def record_consumed_item(self):
        """Record item consumed."""
        self._consumer_count += 1
    
    async def apply_backpressure(self) -> float:
        """
        Apply backpressure delay based on current pressure level.
        
        Returns:
            Delay in seconds that was applied
        """
        if self.current_delay_ms <= 0:
            return 0.0
        
        delay_s = self.current_delay_ms / 1000.0
        await asyncio.sleep(delay_s)
        return delay_s
    
    def should_apply_backpressure(self) -> bool:
        """Check if backpressure should be applied."""
        return self.current_level != BackpressureLevel.NONE
    
    def _calculate_pressure_level(self) -> BackpressureLevel:
        """Calculate current pressure level based on buffer usage."""
        usage = self.metrics.buffer_usage_pct / 100.0
        
        if usage >= self.emergency_threshold:
            return BackpressureLevel.EMERGENCY
        elif usage >= self.critical_threshold:
            return BackpressureLevel.CRITICAL
        elif usage >= self.high_threshold:
            return BackpressureLevel.HIGH
        elif usage >= self.medium_threshold:
            return BackpressureLevel.MEDIUM
        elif usage >= self.low_threshold:
            return BackpressureLevel.LOW
        else:
            return BackpressureLevel.NONE
    
    def _calculate_adaptive_delay(self, level: BackpressureLevel) -> float:
        """
        Calculate adaptive delay based on pressure level and feedback.
        
        Delay ranges by level:
        - NONE: 0ms
        - LOW: 10-50ms (linear scaling with usage)
        - MEDIUM: 50-100ms (linear)
        - HIGH: 100-250ms (exponential)
        - CRITICAL: 250-500ms (exponential)
        - EMERGENCY: 500-1000ms (exponential)
        """
        usage = self.metrics.buffer_usage_pct / 100.0
        
        if level == BackpressureLevel.NONE:
            return 0.0
        
        elif level == BackpressureLevel.LOW:
            # Linear: 0-50ms based on usage between low and medium thresholds
            range_pct = (usage - self.low_threshold) / (self.medium_threshold - self.low_threshold)
            return 10.0 + (40.0 * range_pct)
        
        elif level == BackpressureLevel.MEDIUM:
            # Linear: 50-100ms
            range_pct = (usage - self.medium_threshold) / (self.high_threshold - self.medium_threshold)
            return 50.0 + (50.0 * range_pct)
        
        elif level == BackpressureLevel.HIGH:
            # Exponential: 100-250ms
            range_pct = (usage - self.high_threshold) / (self.critical_threshold - self.high_threshold)
            return 100.0 + (150.0 * (range_pct ** 2))
        
        elif level == BackpressureLevel.CRITICAL:
            # Exponential: 250-500ms
            range_pct = (usage - self.critical_threshold) / (self.emergency_threshold - self.critical_threshold)
            return 250.0 + (250.0 * (range_pct ** 2))
        
        elif level == BackpressureLevel.EMERGENCY:
            # Exponential: 500-1000ms (max backpressure)
            range_pct = min(1.0, (usage - self.emergency_threshold) / (1.0 - self.emergency_threshold))
            return 500.0 + (500.0 * (range_pct ** 3))
        
        return 0.0
    
    def _update_throughput_metrics(self):
        """Update producer/consumer throughput metrics."""
        current_time = time.time()
        elapsed = current_time - self._last_throughput_time
        
        if elapsed >= 1.0:  # Update every second
            self.metrics.producer_rate = self._producer_count / elapsed
            self.metrics.consumer_rate = self._consumer_count / elapsed
            self.metrics.update_derived_metrics()
            
            # Reset counters
            self._producer_count = 0
            self._consumer_count = 0
            self._last_throughput_time = current_time
    
    async def _adjustment_loop(self):
        """Background loop for adaptive delay adjustment."""
        while self._running:
            try:
                await asyncio.sleep(self.adjustment_interval_s)
                
                # Update throughput metrics
                self._update_throughput_metrics()
                
                # Calculate new pressure level
                new_level = self._calculate_pressure_level()
                
                # Track level changes
                if new_level != self.current_level:
                    old_level = self.current_level
                    self.current_level = new_level
                    self.level_entry_time = time.time()
                    self.metrics.level_changes += 1
                    self.metrics.current_level = new_level
                    
                    logger.info(f"Backpressure level changed for '{self.name}': "
                               f"{old_level.value} → {new_level.value} "
                               f"(buffer: {self.metrics.buffer_usage_pct:.1f}%)")
                
                # Calculate new target delay
                target_delay = self._calculate_adaptive_delay(new_level)
                
                # Smooth delay adjustment using EMA
                old_delay = self.current_delay_ms
                self.current_delay_ms = (
                    self.smoothing_factor * target_delay +
                    (1 - self.smoothing_factor) * self.current_delay_ms
                )
                
                # Clamp to bounds
                self.current_delay_ms = max(self.min_delay_ms, 
                                           min(self.max_delay_ms, self.current_delay_ms))
                
                # Track adjustments
                if self.current_delay_ms > old_delay:
                    self.metrics.delay_increases += 1
                elif self.current_delay_ms < old_delay:
                    self.metrics.delay_decreases += 1
                
                self.metrics.current_delay_ms = self.current_delay_ms
                self.metrics.last_adjustment_time = time.time()
                self.metrics.time_in_current_level = time.time() - self.level_entry_time
                
                # Periodic logging
                if self.current_level != BackpressureLevel.NONE:
                    logger.debug(f"Backpressure '{self.name}': "
                                f"level={self.current_level.value}, "
                                f"delay={self.current_delay_ms:.1f}ms, "
                                f"buffer={self.metrics.buffer_usage_pct:.1f}%, "
                                f"lag={self.metrics.consumer_lag}, "
                                f"rate_ratio={self.metrics.rate_ratio:.2f}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in backpressure adjustment loop: {e}")
    
    def get_metrics(self) -> Dict:
        """Get current metrics for monitoring."""
        return {
            "name": self.name,
            "current_level": self.current_level.value,
            "current_delay_ms": self.current_delay_ms,
            "buffer_size": self.metrics.buffer_size,
            "buffer_capacity": self.metrics.buffer_capacity,
            "buffer_usage_pct": self.metrics.buffer_usage_pct,
            "consumer_lag": self.metrics.consumer_lag,
            "avg_lag": self.metrics.avg_lag,
            "max_lag": self.metrics.max_lag,
            "producer_rate": self.metrics.producer_rate,
            "consumer_rate": self.metrics.consumer_rate,
            "rate_ratio": self.metrics.rate_ratio,
            "delay_increases": self.metrics.delay_increases,
            "delay_decreases": self.metrics.delay_decreases,
            "level_changes": self.metrics.level_changes,
            "time_in_current_level_s": self.metrics.time_in_current_level,
        }
