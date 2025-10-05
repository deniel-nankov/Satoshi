#!/usr/bin/env python3
"""
Prometheus Metrics Collector

Institutional-grade metrics collection for HFT infrastructure.
Provides real-time performance, latency, and business metrics.

Metrics Categories:
- System: CPU, memory, network, disk I/O
- Application: Request rates, error rates, latencies
- Business: Trade counts, PnL, risk exposure
- Infrastructure: Kafka lag, database connections, circuit breakers
"""

import time
import psutil
import asyncio
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from collections import defaultdict, deque
from threading import Lock
import logging

logger = logging.getLogger(__name__)

# =============================
# METRIC DEFINITIONS
# =============================

@dataclass
class MetricSample:
    """Individual metric sample with timestamp."""
    name: str
    value: float
    timestamp: float
    labels: Dict[str, str] = field(default_factory=dict)
    help_text: str = ""

@dataclass
class TimeSeries:
    """Time series data for a metric."""
    name: str
    help_text: str
    metric_type: str  # counter, gauge, histogram, summary
    samples: deque = field(default_factory=lambda: deque(maxlen=1000))
    labels: Dict[str, str] = field(default_factory=dict)

class MetricsCollector:
    """
    High-performance metrics collector optimized for HFT workloads.
    
    Features:
    - Sub-millisecond metric recording
    - Lock-free counters for hot paths
    - Automatic system metrics collection
    - Prometheus exposition format
    """
    
    def __init__(self, collection_interval: float = 1.0):
        self.collection_interval = collection_interval
        self.metrics: Dict[str, TimeSeries] = {}
        self.counters: Dict[str, float] = defaultdict(float)
        self.gauges: Dict[str, float] = {}
        self.histograms: Dict[str, List[float]] = defaultdict(list)
        
        # Thread safety
        self._lock = Lock()
        
        # System metrics
        self._last_cpu_times = psutil.cpu_times()
        self._last_network_io = psutil.net_io_counters()
        self._last_disk_io = psutil.disk_io_counters()
        
        # Collection task
        self._collection_task: Optional[asyncio.Task] = None
        self._running = False
        
        # Initialize core metrics
        self._initialize_core_metrics()
    
    def _initialize_core_metrics(self) -> None:
        """Initialize essential system and application metrics."""
        # System metrics
        self.register_gauge("system_cpu_percent", "CPU utilization percentage")
        self.register_gauge("system_memory_percent", "Memory utilization percentage") 
        self.register_gauge("system_disk_percent", "Disk utilization percentage")
        self.register_counter("system_network_bytes_sent", "Network bytes sent")
        self.register_counter("system_network_bytes_recv", "Network bytes received")
        
        # Application metrics
        self.register_counter("http_requests_total", "Total HTTP requests")
        self.register_counter("http_request_errors_total", "Total HTTP request errors")
        self.register_histogram("http_request_duration_seconds", "HTTP request duration")
        
        # Trading metrics
        self.register_counter("trades_executed_total", "Total trades executed")
        self.register_counter("orders_placed_total", "Total orders placed")
        self.register_gauge("portfolio_value_usd", "Current portfolio value in USD")
        self.register_gauge("risk_exposure_percent", "Current risk exposure percentage")
        
        # Infrastructure metrics  
        self.register_gauge("kafka_consumer_lag", "Kafka consumer lag")
        self.register_counter("kafka_messages_consumed", "Kafka messages consumed")
        self.register_counter("kafka_messages_produced", "Kafka messages produced")
        self.register_gauge("circuit_breaker_state", "Circuit breaker state (0=closed, 1=open)")
        
    def register_counter(self, name: str, help_text: str, labels: Dict[str, str] = None) -> None:
        """Register a counter metric."""
        with self._lock:
            self.metrics[name] = TimeSeries(
                name=name,
                help_text=help_text,
                metric_type="counter",
                labels=labels or {}
            )
    
    def register_gauge(self, name: str, help_text: str, labels: Dict[str, str] = None) -> None:
        """Register a gauge metric."""
        with self._lock:
            self.metrics[name] = TimeSeries(
                name=name,
                help_text=help_text,
                metric_type="gauge",
                labels=labels or {}
            )
    
    def register_histogram(self, name: str, help_text: str, 
                         buckets: List[float] = None, labels: Dict[str, str] = None) -> None:
        """Register a histogram metric."""
        if buckets is None:
            # HFT-optimized latency buckets (microseconds to seconds)
            buckets = [0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 
                      0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]
        
        with self._lock:
            self.metrics[name] = TimeSeries(
                name=name,
                help_text=help_text,
                metric_type="histogram",
                labels=labels or {}
            )
            # Store bucket configuration
            self.metrics[name].buckets = buckets
    
    def increment_counter(self, name: str, value: float = 1.0, labels: Dict[str, str] = None) -> None:
        """Increment a counter metric (thread-safe, lock-free for performance)."""
        key = f"{name}_{hash(frozenset(labels.items()) if labels else frozenset())}"
        self.counters[key] += value
        
        # Record sample for time series
        sample = MetricSample(
            name=name,
            value=self.counters[key],
            timestamp=time.time(),
            labels=labels or {}
        )
        
        if name in self.metrics:
            self.metrics[name].samples.append(sample)
    
    def set_gauge(self, name: str, value: float, labels: Dict[str, str] = None) -> None:
        """Set a gauge metric value."""
        key = f"{name}_{hash(frozenset(labels.items()) if labels else frozenset())}"
        self.gauges[key] = value
        
        sample = MetricSample(
            name=name,
            value=value,
            timestamp=time.time(),
            labels=labels or {}
        )
        
        if name in self.metrics:
            self.metrics[name].samples.append(sample)
    
    def observe_histogram(self, name: str, value: float, labels: Dict[str, str] = None) -> None:
        """Add an observation to a histogram metric."""
        key = f"{name}_{hash(frozenset(labels.items()) if labels else frozenset())}"
        self.histograms[key].append(value)
        
        # Keep only recent observations to prevent memory growth
        if len(self.histograms[key]) > 1000:
            self.histograms[key] = self.histograms[key][-1000:]
        
        sample = MetricSample(
            name=name,
            value=value,
            timestamp=time.time(),
            labels=labels or {}
        )
        
        if name in self.metrics:
            self.metrics[name].samples.append(sample)
    
    def time_function(self, metric_name: str = "function_duration_seconds"):
        """Decorator to time function execution."""
        def decorator(func: Callable) -> Callable:
            if asyncio.iscoroutinefunction(func):
                async def async_wrapper(*args, **kwargs):
                    start_time = time.time()
                    try:
                        result = await func(*args, **kwargs)
                        return result
                    finally:
                        duration = time.time() - start_time
                        self.observe_histogram(metric_name, duration, 
                                             {"function": func.__name__})
                return async_wrapper
            else:
                def sync_wrapper(*args, **kwargs):
                    start_time = time.time()
                    try:
                        result = func(*args, **kwargs)
                        return result
                    finally:
                        duration = time.time() - start_time
                        self.observe_histogram(metric_name, duration,
                                             {"function": func.__name__})
                return sync_wrapper
        return decorator
    
    async def collect_system_metrics(self) -> None:
        """Collect system-level metrics."""
        try:
            # CPU metrics
            cpu_percent = psutil.cpu_percent(interval=None)
            self.set_gauge("system_cpu_percent", cpu_percent)
            
            # Memory metrics
            memory = psutil.virtual_memory()
            self.set_gauge("system_memory_percent", memory.percent)
            self.set_gauge("system_memory_available_bytes", memory.available)
            
            # Disk metrics
            disk = psutil.disk_usage('/')
            disk_percent = (disk.used / disk.total) * 100
            self.set_gauge("system_disk_percent", disk_percent)
            
            # Network metrics
            net_io = psutil.net_io_counters()
            if self._last_network_io:
                bytes_sent_delta = net_io.bytes_sent - self._last_network_io.bytes_sent
                bytes_recv_delta = net_io.bytes_recv - self._last_network_io.bytes_recv
                self.increment_counter("system_network_bytes_sent", bytes_sent_delta)
                self.increment_counter("system_network_bytes_recv", bytes_recv_delta)
            self._last_network_io = net_io
            
        except Exception as e:
            logger.error(f"Failed to collect system metrics: {e}")
    
    def generate_prometheus_output(self) -> str:
        """Generate Prometheus exposition format output."""
        output_lines = []
        
        # Process counters
        for key, value in self.counters.items():
            if '_' in key:
                name = key.rsplit('_', 1)[0]
                if name in self.metrics:
                    metric = self.metrics[name]
                    output_lines.append(f"# HELP {name} {metric.help_text}")
                    output_lines.append(f"# TYPE {name} counter")
                    output_lines.append(f"{name} {value}")
        
        # Process gauges
        for key, value in self.gauges.items():
            if '_' in key:
                name = key.rsplit('_', 1)[0]
                if name in self.metrics:
                    metric = self.metrics[name]
                    output_lines.append(f"# HELP {name} {metric.help_text}")
                    output_lines.append(f"# TYPE {name} gauge")
                    output_lines.append(f"{name} {value}")
        
        # Process histograms
        for key, observations in self.histograms.items():
            if '_' in key and observations:
                name = key.rsplit('_', 1)[0]
                if name in self.metrics:
                    metric = self.metrics[name]
                    output_lines.append(f"# HELP {name} {metric.help_text}")
                    output_lines.append(f"# TYPE {name} histogram")
                    
                    # Generate histogram buckets
                    buckets = getattr(metric, 'buckets', [0.001, 0.01, 0.1, 1.0, 10.0])
                    total_count = len(observations)
                    
                    for bucket in buckets:
                        count = sum(1 for obs in observations if obs <= bucket)
                        output_lines.append(f"{name}_bucket{{le=\"{bucket}\"}} {count}")
                    
                    output_lines.append(f"{name}_bucket{{le=\"+Inf\"}} {total_count}")
                    output_lines.append(f"{name}_count {total_count}")
                    
                    if observations:
                        total_sum = sum(observations)
                        output_lines.append(f"{name}_sum {total_sum}")
        
        return '\n'.join(output_lines) + '\n'
    
    async def start_collection(self) -> None:
        """Start automatic metrics collection."""
        if self._running:
            return
            
        self._running = True
        self._collection_task = asyncio.create_task(self._collection_loop())
        logger.info(f"Started metrics collection with {self.collection_interval}s interval")
    
    async def stop_collection(self) -> None:
        """Stop automatic metrics collection."""
        self._running = False
        if self._collection_task:
            self._collection_task.cancel()
            try:
                await self._collection_task
            except asyncio.CancelledError:
                pass
        logger.info("Stopped metrics collection")
    
    async def _collection_loop(self) -> None:
        """Main collection loop."""
        while self._running:
            try:
                await self.collect_system_metrics()
                await asyncio.sleep(self.collection_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in metrics collection loop: {e}")
                await asyncio.sleep(self.collection_interval)
    
    def get_metric_summary(self) -> Dict[str, Any]:
        """Get summary of all metrics for debugging."""
        return {
            "total_metrics": len(self.metrics),
            "counters": len(self.counters),
            "gauges": len(self.gauges),
            "histograms": len(self.histograms),
            "collection_interval": self.collection_interval,
            "running": self._running
        }

# =============================
# CONVENIENCE DECORATORS
# =============================

# Global metrics collector instance
_global_collector: Optional[MetricsCollector] = None

def get_metrics_collector() -> MetricsCollector:
    """Get or create global metrics collector."""
    global _global_collector
    if _global_collector is None:
        _global_collector = MetricsCollector()
    return _global_collector

def timed(metric_name: str = "function_duration_seconds"):
    """Convenience decorator for timing functions."""
    collector = get_metrics_collector()
    return collector.time_function(metric_name)

def count_calls(metric_name: str = "function_calls_total"):
    """Convenience decorator for counting function calls."""
    def decorator(func: Callable) -> Callable:
        collector = get_metrics_collector()
        
        if asyncio.iscoroutinefunction(func):
            async def async_wrapper(*args, **kwargs):
                collector.increment_counter(metric_name, labels={"function": func.__name__})
                return await func(*args, **kwargs)
            return async_wrapper
        else:
            def sync_wrapper(*args, **kwargs):
                collector.increment_counter(metric_name, labels={"function": func.__name__})
                return func(*args, **kwargs)
            return sync_wrapper
    return decorator

# =============================
# EXAMPLE USAGE
# =============================

if __name__ == "__main__":
    import asyncio
    
    async def example_usage():
        collector = MetricsCollector(collection_interval=0.1)
        await collector.start_collection()
        
        # Simulate some metrics
        for i in range(10):
            collector.increment_counter("test_counter", 1.0)
            collector.set_gauge("test_gauge", i * 10)
            collector.observe_histogram("test_histogram", i * 0.001)
            await asyncio.sleep(0.1)
        
        # Generate output
        print("Prometheus Output:")
        print(collector.generate_prometheus_output())
        
        await collector.stop_collection()
    
    asyncio.run(example_usage())
