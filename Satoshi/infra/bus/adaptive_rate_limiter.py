#!/usr/bin/env python3
"""
Adaptive Rate Limiter with AIMD Algorithm

Implements Additive Increase Multiplicative Decrease (AIMD) congestion control
for intelligent rate limiting that adapts to downstream pressure and API responses.

Key Features:
- AIMD algorithm: Slow increase, fast decrease on congestion
- Adaptive thresholds based on historical patterns
- Feedback-driven rate adjustment (429s, timeouts, latency)
- Circuit breaker integration
- Per-domain rate budgets with priority queuing
"""

import asyncio
import logging
import time
from typing import Dict, Optional, List, Tuple
from dataclasses import dataclass, field
from collections import deque
from enum import Enum
import statistics

logger = logging.getLogger(__name__)


class RateLimitStatus(Enum):
    """Rate limiter operational status."""
    NORMAL = "normal"           # Operating at target rate
    INCREASING = "increasing"   # Gradually increasing rate
    DECREASING = "decreasing"   # Rapidly decreasing rate
    CIRCUIT_OPEN = "circuit_open"  # Circuit breaker triggered


@dataclass
class RateLimitMetrics:
    """Metrics for rate limiter performance and adaptation."""
    current_rate: float = 0.0           # Current requests per second
    target_rate: float = 0.0             # Target rate (may be lower than max)
    max_rate: float = 0.0                # Maximum configured rate
    
    # Feedback signals
    total_requests: int = 0
    successful_requests: int = 0
    rate_limited_429s: int = 0
    timeouts: int = 0
    errors: int = 0
    
    # Adaptation history
    rate_increases: int = 0
    rate_decreases: int = 0
    last_adjustment_time: float = 0.0
    
    # Latency tracking
    latency_samples: deque = field(default_factory=lambda: deque(maxlen=100))
    p50_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    
    def update_latency_percentiles(self):
        """Update latency percentiles from samples."""
        if not self.latency_samples:
            return
        
        sorted_samples = sorted(self.latency_samples)
        n = len(sorted_samples)
        
        self.p50_latency_ms = sorted_samples[int(n * 0.5)]
        self.p95_latency_ms = sorted_samples[int(n * 0.95)]
        self.p99_latency_ms = sorted_samples[int(n * 0.99)]


@dataclass
class AdaptiveThreshold:
    """Adaptive threshold that learns from historical patterns."""
    name: str
    initial_value: float
    current_value: float
    min_value: float
    max_value: float
    
    # Historical samples for learning
    samples: deque = field(default_factory=lambda: deque(maxlen=1000))
    
    # Adaptation parameters
    learning_rate: float = 0.1      # How fast to adapt (0-1)
    sensitivity: float = 1.5         # Multiplier for threshold (median * sensitivity)
    
    def update(self, sample: float):
        """Update threshold based on new sample."""
        self.samples.append(sample)
        
        if len(self.samples) < 10:
            # Not enough data yet
            return
        
        # Calculate adaptive threshold: median * sensitivity + MAD * 2.0
        median = statistics.median(self.samples)
        mad = statistics.median([abs(x - median) for x in self.samples])
        
        adaptive_value = median * self.sensitivity + mad * 2.0
        
        # Smooth update with learning rate
        new_value = (1 - self.learning_rate) * self.current_value + self.learning_rate * adaptive_value
        
        # Clamp to bounds
        self.current_value = max(self.min_value, min(self.max_value, new_value))
    
    def is_exceeded(self, value: float) -> bool:
        """Check if value exceeds adaptive threshold."""
        return value > self.current_value


class AdaptiveRateLimiter:
    """
    Adaptive rate limiter using AIMD (Additive Increase Multiplicative Decrease).
    
    Algorithm:
    - Additive Increase: Gradually increase rate when stable (e.g., +1 req/s every 10s)
    - Multiplicative Decrease: Rapidly decrease rate on congestion (e.g., rate *= 0.5)
    
    Feedback signals:
    - 429 responses: Immediate multiplicative decrease
    - Timeouts: Gradual decrease
    - High latency: Gradual decrease
    - Successful requests: Gradual increase
    """
    
    def __init__(
        self,
        domain: str,
        initial_rate: float = 10.0,      # requests per second
        max_rate: float = 100.0,
        min_rate: float = 1.0,
        
        # AIMD parameters
        additive_increase: float = 1.0,  # req/s to add per adjustment period
        multiplicative_decrease: float = 0.5,  # factor to multiply on congestion
        adjustment_interval_s: float = 10.0,   # how often to adjust rate
        
        # Circuit breaker
        failure_threshold: int = 5,      # consecutive failures before circuit opens
        recovery_timeout_s: float = 60.0,  # seconds to wait before trying recovery
        
        # Adaptive thresholds
        enable_adaptive_thresholds: bool = True,
    ):
        self.domain = domain
        self.max_rate = max_rate
        self.min_rate = min_rate
        
        # AIMD parameters
        self.additive_increase = additive_increase
        self.multiplicative_decrease = multiplicative_decrease
        self.adjustment_interval_s = adjustment_interval_s
        
        # Current state
        self.current_rate = initial_rate
        self.target_rate = initial_rate
        self.status = RateLimitStatus.NORMAL
        
        # Circuit breaker
        self.failure_threshold = failure_threshold
        self.recovery_timeout_s = recovery_timeout_s
        self.consecutive_failures = 0
        self.circuit_open_time: Optional[float] = None
        
        # Token bucket for rate limiting
        self.tokens = initial_rate
        self.last_refill_time = time.time()
        self._lock = asyncio.Lock()
        
        # Metrics
        self.metrics = RateLimitMetrics(
            current_rate=initial_rate,
            target_rate=initial_rate,
            max_rate=max_rate
        )
        
        # Adaptive thresholds
        self.enable_adaptive_thresholds = enable_adaptive_thresholds
        self.latency_threshold = AdaptiveThreshold(
            name="latency_ms",
            initial_value=500.0,  # 500ms initial threshold
            current_value=500.0,
            min_value=100.0,
            max_value=5000.0,
            learning_rate=0.1,
            sensitivity=2.0  # Alert if latency > median * 2
        )
        
        # Background adjustment task
        self._adjustment_task: Optional[asyncio.Task] = None
        self._running = False
        
        logger.info(f"Adaptive rate limiter initialized for domain '{domain}': "
                   f"rate={initial_rate:.1f} req/s, max={max_rate:.1f}, AIMD enabled")
    
    async def start(self):
        """Start background rate adjustment task."""
        if self._running:
            return
        
        self._running = True
        self._adjustment_task = asyncio.create_task(self._adjustment_loop())
        logger.info(f"Adaptive rate limiter started for domain '{self.domain}'")
    
    async def stop(self):
        """Stop background rate adjustment task."""
        self._running = False
        if self._adjustment_task:
            self._adjustment_task.cancel()
            try:
                await self._adjustment_task
            except asyncio.CancelledError:
                pass
        logger.info(f"Adaptive rate limiter stopped for domain '{self.domain}'")
    
    async def acquire(self, timeout: Optional[float] = None) -> bool:
        """
        Acquire permission to make a request.
        
        Returns:
            True if permission granted, False if circuit breaker open or timeout
        """
        start_time = time.time()
        
        # Check circuit breaker
        if self.status == RateLimitStatus.CIRCUIT_OPEN:
            if not self._should_attempt_recovery():
                return False
            # Try recovery
            logger.info(f"Attempting circuit breaker recovery for domain '{self.domain}'")
            self.status = RateLimitStatus.NORMAL
            self.consecutive_failures = 0
        
        async with self._lock:
            # Refill tokens (continuous refill)
            await self._refill_tokens()
            
            # Wait for token if needed
            while self.tokens < 1.0:
                if timeout and (time.time() - start_time) >= timeout:
                    return False
                
                # Wait a bit and refill
                await asyncio.sleep(0.01)  # 10ms granularity
                await self._refill_tokens()
            
            # Consume token
            self.tokens -= 1.0
            self.metrics.total_requests += 1
            
            return True
    
    async def _refill_tokens(self):
        """Refill tokens based on current rate (continuous refill)."""
        current_time = time.time()
        time_elapsed = current_time - self.last_refill_time
        
        # Continuous refill: add tokens proportional to time elapsed
        tokens_to_add = self.current_rate * time_elapsed
        self.tokens = min(self.current_rate, self.tokens + tokens_to_add)
        
        self.last_refill_time = current_time
    
    def record_success(self, latency_ms: float):
        """Record successful request with latency."""
        self.metrics.successful_requests += 1
        self.metrics.latency_samples.append(latency_ms)
        self.consecutive_failures = 0
        
        # Update adaptive latency threshold
        if self.enable_adaptive_thresholds:
            self.latency_threshold.update(latency_ms)
    
    def record_rate_limit_429(self):
        """Record 429 rate limit response - triggers immediate decrease."""
        self.metrics.rate_limited_429s += 1
        self.consecutive_failures += 1
        
        # Immediate multiplicative decrease
        self._decrease_rate_multiplicative()
        
        # Check circuit breaker
        if self.consecutive_failures >= self.failure_threshold:
            self._open_circuit_breaker()
        
        logger.warning(f"Rate limit 429 on domain '{self.domain}': "
                      f"decreased rate to {self.current_rate:.1f} req/s")
    
    def record_429(self):
        """Alias for record_rate_limit_429() for backwards compatibility."""
        self.record_rate_limit_429()
    
    def record_timeout(self):
        """Record timeout - triggers gradual decrease."""
        self.metrics.timeouts += 1
        self.consecutive_failures += 1
        
        # Gradual decrease (less aggressive than 429)
        self._decrease_rate_gradual()
        
        # Check circuit breaker
        if self.consecutive_failures >= self.failure_threshold:
            self._open_circuit_breaker()
        
        logger.warning(f"Timeout on domain '{self.domain}': "
                      f"decreased rate to {self.current_rate:.1f} req/s")
    
    def record_error(self):
        """Record generic error."""
        self.metrics.errors += 1
        self.consecutive_failures += 1
        
        # Check circuit breaker
        if self.consecutive_failures >= self.failure_threshold:
            self._open_circuit_breaker()
    
    def _decrease_rate_multiplicative(self):
        """Multiplicative decrease (AIMD)."""
        old_rate = self.current_rate
        self.current_rate = max(self.min_rate, self.current_rate * self.multiplicative_decrease)
        self.target_rate = self.current_rate
        self.tokens = min(self.tokens, self.current_rate)  # Adjust token bucket
        
        self.metrics.rate_decreases += 1
        self.metrics.last_adjustment_time = time.time()
        self.status = RateLimitStatus.DECREASING
        
        logger.info(f"Multiplicative decrease for '{self.domain}': "
                   f"{old_rate:.1f} → {self.current_rate:.1f} req/s")
    
    def _decrease_rate_gradual(self):
        """Gradual decrease (less aggressive than multiplicative)."""
        old_rate = self.current_rate
        decrease_factor = 0.8  # 20% decrease
        self.current_rate = max(self.min_rate, self.current_rate * decrease_factor)
        self.target_rate = self.current_rate
        self.tokens = min(self.tokens, self.current_rate)
        
        self.metrics.rate_decreases += 1
        self.metrics.last_adjustment_time = time.time()
        self.status = RateLimitStatus.DECREASING
        
        logger.debug(f"Gradual decrease for '{self.domain}': "
                    f"{old_rate:.1f} → {self.current_rate:.1f} req/s")
    
    def _increase_rate_additive(self):
        """Additive increase (AIMD)."""
        if self.current_rate >= self.max_rate:
            return
        
        old_rate = self.current_rate
        self.current_rate = min(self.max_rate, self.current_rate + self.additive_increase)
        self.target_rate = self.current_rate
        
        self.metrics.rate_increases += 1
        self.metrics.last_adjustment_time = time.time()
        self.status = RateLimitStatus.INCREASING
        
        logger.debug(f"Additive increase for '{self.domain}': "
                    f"{old_rate:.1f} → {self.current_rate:.1f} req/s")
    
    def _open_circuit_breaker(self):
        """Open circuit breaker after repeated failures."""
        self.status = RateLimitStatus.CIRCUIT_OPEN
        self.circuit_open_time = time.time()
        
        logger.error(f"Circuit breaker OPENED for domain '{self.domain}' "
                    f"after {self.consecutive_failures} consecutive failures")
    
    def _should_attempt_recovery(self) -> bool:
        """Check if enough time has passed to attempt recovery."""
        if self.circuit_open_time is None:
            return False
        
        time_open = time.time() - self.circuit_open_time
        return time_open >= self.recovery_timeout_s
    
    async def _adjustment_loop(self):
        """Background loop for AIMD rate adjustments."""
        while self._running:
            try:
                await asyncio.sleep(self.adjustment_interval_s)
                
                # Skip adjustment if circuit breaker is open
                if self.status == RateLimitStatus.CIRCUIT_OPEN:
                    continue
                
                # Calculate success rate
                total = self.metrics.total_requests
                success = self.metrics.successful_requests
                success_rate = success / total if total > 0 else 0.0
                
                # Update latency percentiles
                self.metrics.update_latency_percentiles()
                
                # Decision logic for AIMD
                if success_rate >= 0.95 and self.metrics.rate_limited_429s == 0:
                    # High success rate, no 429s: increase rate (additive)
                    if self.enable_adaptive_thresholds:
                        # Check if latency is healthy
                        if self.metrics.p95_latency_ms > 0 and \
                           not self.latency_threshold.is_exceeded(self.metrics.p95_latency_ms):
                            self._increase_rate_additive()
                    else:
                        self._increase_rate_additive()
                
                elif success_rate < 0.90:
                    # Low success rate: decrease rate (gradual)
                    self._decrease_rate_gradual()
                
                # Update metrics
                self.metrics.current_rate = self.current_rate
                self.metrics.target_rate = self.target_rate
                
                # Log periodic status
                if total > 0 and total % 100 == 0:
                    logger.info(f"Rate limiter '{self.domain}': "
                               f"rate={self.current_rate:.1f} req/s, "
                               f"success_rate={success_rate:.2%}, "
                               f"p95_latency={self.metrics.p95_latency_ms:.0f}ms, "
                               f"429s={self.metrics.rate_limited_429s}, "
                               f"status={self.status.value}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in rate limiter adjustment loop: {e}")
    
    def get_metrics(self) -> Dict:
        """Get current metrics for monitoring."""
        return {
            "domain": self.domain,
            "current_rate": self.current_rate,
            "target_rate": self.target_rate,
            "max_rate": self.max_rate,
            "status": self.status.value,
            "total_requests": self.metrics.total_requests,
            "successful_requests": self.metrics.successful_requests,
            "success_rate": self.metrics.successful_requests / self.metrics.total_requests 
                           if self.metrics.total_requests > 0 else 0.0,
            "rate_limited_429s": self.metrics.rate_limited_429s,
            "timeouts": self.metrics.timeouts,
            "errors": self.metrics.errors,
            "rate_increases": self.metrics.rate_increases,
            "rate_decreases": self.metrics.rate_decreases,
            "p50_latency_ms": self.metrics.p50_latency_ms,
            "p95_latency_ms": self.metrics.p95_latency_ms,
            "p99_latency_ms": self.metrics.p99_latency_ms,
            "consecutive_failures": self.consecutive_failures,
            "circuit_breaker_open": self.status == RateLimitStatus.CIRCUIT_OPEN,
            "adaptive_latency_threshold_ms": self.latency_threshold.current_value if self.enable_adaptive_thresholds else None,
        }


class AdaptiveRateLimiterPool:
    """Pool of adaptive rate limiters for different domains."""
    
    def __init__(self):
        self.limiters: Dict[str, AdaptiveRateLimiter] = {}
        self._lock = asyncio.Lock()
    
    async def get_limiter(
        self,
        domain: str,
        initial_rate: float = 10.0,
        max_rate: float = 100.0,
        **kwargs
    ) -> AdaptiveRateLimiter:
        """Get or create adaptive rate limiter for domain."""
        async with self._lock:
            if domain not in self.limiters:
                limiter = AdaptiveRateLimiter(
                    domain=domain,
                    initial_rate=initial_rate,
                    max_rate=max_rate,
                    **kwargs
                )
                await limiter.start()
                self.limiters[domain] = limiter
                logger.info(f"Created adaptive rate limiter for domain '{domain}'")
            
            return self.limiters[domain]
    
    async def shutdown(self):
        """Shutdown all rate limiters."""
        async with self._lock:
            for limiter in self.limiters.values():
                await limiter.stop()
            self.limiters.clear()
            logger.info("All adaptive rate limiters stopped")
    
    def get_all_metrics(self) -> List[Dict]:
        """Get metrics for all rate limiters."""
        return [limiter.get_metrics() for limiter in self.limiters.values()]
