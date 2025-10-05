#!/usr/bin/env python3
"""
Satoshi Health & Metrics API

Provides real-time health monitoring, metrics, and operational insights
for the institutional HFT data infrastructure.

Endpoints:
- /health - System health status
- /metrics - Prometheus metrics
- /status - Detailed component status
- /circuits - Circuit breaker states
"""

import asyncio
import time
import os
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import logging

from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
import uvicorn

# Import internal components
try:
    from infra.bus.streaming_bus import StreamingBus
    from infra.monitoring.prometheus_metrics import MetricsCollector
except ImportError as e:
    logging.warning(f"Import warning: {e}")
    StreamingBus = None
    MetricsCollector = None

logger = logging.getLogger(__name__)

# Application constants
APP_VERSION = "0.1.0"

# =============================
# PYDANTIC MODELS
# =============================

class HealthStatus(BaseModel):
    """Overall system health status."""
    status: str  # "healthy", "degraded", "unhealthy"
    timestamp: datetime
    uptime_seconds: float
    version: str = APP_VERSION
    components: Dict[str, Any]

class ComponentHealth(BaseModel):
    """Individual component health."""
    name: str
    status: str
    last_check: datetime
    latency_ms: Optional[float] = None
    error_rate: Optional[float] = None
    details: Dict[str, Any] = {}

class CircuitBreakerStatus(BaseModel):
    """Circuit breaker state."""
    component: str
    state: str  # "closed", "open", "half_open"
    failure_count: int
    last_failure: Optional[datetime] = None
    next_attempt: Optional[datetime] = None

# =============================
# FASTAPI APP SETUP
# =============================

app = FastAPI(
    title="Satoshi Health & Metrics API",
    description="Real-time health monitoring and metrics for HFT infrastructure",
    version=APP_VERSION,
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware configuration
cors_origins = os.getenv("CORS_ALLOWED_ORIGINS", "*").split(",")
cors_credentials = cors_origins != ["*"]  # Only allow credentials with explicit origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=cors_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global state
_start_time = time.time()

# =============================
# AUTHENTICATION & AUTHORIZATION
# =============================

async def get_current_service_token(request: Request) -> str:
    """
    Basic service token authentication for administrative endpoints.
    In production, replace with proper OAuth/JWT validation.
    """
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail="Missing or invalid authorization header",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    token = auth_header.split(" ", 1)[1]
    # In production, validate against proper token store/service
    if token != "dev-admin-token":  # Simple dev token for demo
        raise HTTPException(
            status_code=401,
            detail="Invalid service token",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    return token

async def verify_admin_permissions(token: str = Depends(get_current_service_token)) -> bool:
    """Verify admin permissions for sensitive operations."""
    # In production, check token scope/permissions
    # For demo, all valid tokens have admin access
    return True

# =============================
# DEPENDENCY INJECTION
# =============================

async def get_streaming_bus(request: Request) -> Optional[StreamingBus]:
    """Get streaming bus instance from app state."""
    return getattr(request.app.state, 'streaming_bus', None)

async def get_metrics_collector(request: Request) -> Optional[MetricsCollector]:
    """Get metrics collector instance from app state."""
    return getattr(request.app.state, 'metrics_collector', None)

# =============================
# HEALTH CHECK ENDPOINTS
# =============================

@app.get("/health", response_model=HealthStatus)
async def health_check(
    streaming_bus: Optional[StreamingBus] = Depends(get_streaming_bus),
    metrics: Optional[MetricsCollector] = Depends(get_metrics_collector)
) -> HealthStatus:
    """
    Comprehensive system health check.
    
    Returns overall system status with component-level health details.
    Used by load balancers and monitoring systems.
    """
    current_time = datetime.now(timezone.utc)
    uptime = time.time() - _start_time
    
    components = {}
    overall_status = "healthy"
    
    # Check streaming bus health
    if streaming_bus:
        try:
            bus_health = streaming_bus.get_health_status()
            components["streaming_bus"] = ComponentHealth(
                name="streaming_bus",
                status="healthy" if bus_health.get("healthy", False) else "unhealthy",
                last_check=current_time,
                latency_ms=bus_health.get("avg_latency_ms"),
                error_rate=bus_health.get("error_rate"),
                details=bus_health
            )
            if not bus_health.get("healthy", False):
                overall_status = "degraded"
        except Exception as e:
            components["streaming_bus"] = ComponentHealth(
                name="streaming_bus",
                status="unhealthy",
                last_check=current_time,
                details={"error": str(e)}
            )
            overall_status = "unhealthy"
    else:
        components["streaming_bus"] = ComponentHealth(
            name="streaming_bus",
            status="not_configured",
            last_check=current_time,
            details={"message": "Streaming bus not initialized"}
        )
    
    # Check metrics collector
    if metrics:
        components["metrics"] = ComponentHealth(
            name="metrics_collector",
            status="healthy",
            last_check=current_time,
            details={"collectors_active": True}
        )
    else:
        components["metrics"] = ComponentHealth(
            name="metrics_collector",
            status="not_configured",
            last_check=current_time,
            details={"message": "Metrics collector not initialized"}
        )
    
    return HealthStatus(
        status=overall_status,
        timestamp=current_time,
        uptime_seconds=uptime,
        components={name: comp.dict() for name, comp in components.items()}
    )

@app.get("/health/live")
async def liveness_probe() -> Dict[str, str]:
    """
    Kubernetes liveness probe endpoint.
    Simple check that the service is running.
    """
    return {"status": "alive", "timestamp": datetime.now(timezone.utc).isoformat()}

@app.get("/health/ready")
async def readiness_probe(
    streaming_bus: Optional[StreamingBus] = Depends(get_streaming_bus)
) -> Dict[str, str]:
    """
    Kubernetes readiness probe endpoint.
    Checks if service is ready to accept traffic.
    """
    # Check critical dependencies
    if streaming_bus:
        try:
            health = streaming_bus.get_health_status()
            if not health.get("healthy", False):
                raise HTTPException(status_code=503, detail="Streaming bus not ready")
        except Exception as e:
            raise HTTPException(status_code=503, detail=f"Dependency check failed: {e}") from e
    
    return {"status": "ready", "timestamp": datetime.now(timezone.utc).isoformat()}

# =============================
# METRICS ENDPOINTS
# =============================

@app.get("/metrics", response_class=PlainTextResponse)
async def prometheus_metrics(
    metrics: Optional[MetricsCollector] = Depends(get_metrics_collector)
) -> str:
    """
    Prometheus metrics endpoint.
    Returns metrics in Prometheus exposition format.
    """
    if not metrics:
        # Return basic metrics even without collector
        uptime = time.time() - _start_time
        return f"""# HELP satoshi_uptime_seconds Total uptime in seconds
# TYPE satoshi_uptime_seconds counter
satoshi_uptime_seconds {uptime}

# HELP satoshi_build_info Build information
# TYPE satoshi_build_info gauge
satoshi_build_info{{version="{APP_VERSION}"}} 1
"""
    
    try:
        return metrics.generate_prometheus_output()
    except Exception as e:
        logger.error(f"Failed to generate metrics: {e}")
        raise HTTPException(status_code=500, detail="Metrics generation failed") from e

@app.get("/status")
async def detailed_status(
    streaming_bus: Optional[StreamingBus] = Depends(get_streaming_bus)
) -> Dict[str, Any]:
    """
    Detailed system status for operations dashboard.
    Includes performance metrics, error rates, and operational data.
    """
    status = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "uptime_seconds": time.time() - _start_time,
        "version": APP_VERSION,
        "environment": "development",  # Configure via environment
        "components": {}
    }
    
    # Detailed streaming bus status
    if streaming_bus:
        try:
            bus_status = streaming_bus.get_health_status()
            status["components"]["streaming_bus"] = {
                "healthy": bus_status.get("healthy", False),
                "brokers": bus_status.get("brokers", {}),
                "consumers": bus_status.get("consumers", {}),
                "producers": bus_status.get("producers", {}),
                "circuit_breakers": bus_status.get("circuit_breakers", {}),
                "topic_metrics": getattr(streaming_bus, 'topic_metrics', {})
            }
        except Exception as e:
            status["components"]["streaming_bus"] = {"error": str(e)}
    
    return status

# =============================
# CIRCUIT BREAKER ENDPOINTS
# =============================

@app.get("/circuits", response_model=List[CircuitBreakerStatus])
async def circuit_breaker_status(
    streaming_bus: Optional[StreamingBus] = Depends(get_streaming_bus)
) -> List[CircuitBreakerStatus]:
    """Get status of all circuit breakers."""
    circuits = []
    
    if streaming_bus and hasattr(streaming_bus, 'circuit_breakers'):
        for component_id, breaker in streaming_bus.circuit_breakers.items():
            circuits.append(CircuitBreakerStatus(
                component=component_id,
                state=breaker.state,
                failure_count=breaker.failure_count,
                last_failure=breaker.last_failure_time,
                next_attempt=breaker.next_attempt_time
            ))
    
    return circuits

@app.post("/circuits/{component_id}/reset")
async def reset_circuit_breaker(
    component_id: str,
    _admin_auth: bool = Depends(verify_admin_permissions),
    streaming_bus: Optional[StreamingBus] = Depends(get_streaming_bus)
) -> Dict[str, str]:
    """Reset a specific circuit breaker. Requires admin authentication."""
    if not streaming_bus:
        raise HTTPException(status_code=503, detail="Streaming bus not available")
    
    try:
        if hasattr(streaming_bus, 'reset_circuit_breaker'):
            await streaming_bus.reset_circuit_breaker(component_id)
            return {"status": "reset", "component": component_id}
        else:
            raise HTTPException(status_code=501, detail="Circuit breaker reset not implemented")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Reset failed: {e}") from e

# =============================
# STARTUP/SHUTDOWN EVENTS
# =============================

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    logger.info("Starting Satoshi Health & Metrics API")
    global _start_time
    _start_time = time.time()
    
    # Initialize components in app state
    app.state.streaming_bus = None  # Would be initialized in production
    app.state.metrics_collector = None  # Would be initialized in production

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    logger.info("Shutting down Satoshi Health & Metrics API")
    
    # Cleanup app state components
    if hasattr(app.state, 'streaming_bus') and app.state.streaming_bus:
        # Would cleanup streaming bus
        pass
    if hasattr(app.state, 'metrics_collector') and app.state.metrics_collector:
        # Would cleanup metrics collector
        pass

# =============================
# DEVELOPMENT SERVER
# =============================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        port=8000,
        reload=True,
        access_log=True
    )
