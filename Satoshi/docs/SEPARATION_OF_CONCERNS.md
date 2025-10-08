# Separation of Concerns - Data Quality Architecture

## 🎯 **Clear Responsibility Matrix**

| **Component** | **SHOULD DO** | **SHOULD NOT DO** |
|---------------|---------------|-------------------|
| **StreamingBus** | Infrastructure, Transport, Connectivity | Business Logic, Quality Rules |
| **Data Quality Orchestrator** | Business Logic, Quality Pipeline | Infrastructure, Message Transport |
| **Quality Agents** | Domain-Specific Validation | Cross-Domain Coordination |

---

## 📡 **StreamingBus - Infrastructure Layer**

### **✅ RESPONSIBILITIES**
- **Message Transport**: Produce/consume messages via Kafka/Redpanda  
- **Connection Management**: Producer/consumer pools, connection pooling
- **Infrastructure Resilience**: Infrastructure circuit breakers, retries, timeouts
- **Topic Management**: Create topics, manage partitions, schema registry
- **Network Layer**: SSL/TLS, authentication, authorization
- **Infrastructure Monitoring**: Broker health, consumer lag, throughput metrics

### **❌ SHOULD NOT DO**
- Quality validation logic
- Business rule enforcement  
- Quality scoring or thresholds
- Domain-specific error handling
- Business metrics (quality scores, incidents)

### **🔧 INTERFACE CONTRACT**
```python
class StreamingBusInterface:
    # Core Transport
    async def publish(topic: str, key: str, payload: Dict) -> bool
    async def consume(consumer_group: str, topics: List[str]) -> AsyncIterator[Message]
    
    # Infrastructure Resilience  
    async def check_infrastructure_health() -> Dict[str, Any]
    def is_infrastructure_circuit_breaker_open(topic: str) -> bool
    
    # Management
    async def create_topics(topic_configs: List[TopicConfig]) -> bool
```

---

## 🎯 **Data Quality Orchestrator - Business Logic Layer**

### **✅ RESPONSIBILITIES**
- **Quality Pipeline Coordination**: Orchestrate 5-stage validation pipeline
- **Business Rule Enforcement**: Quality gates, thresholds, scoring algorithms  
- **Quality Circuit Breaking**: Business-level failure handling (not infrastructure)
- **Pipeline State Management**: STRICT/RESILIENT/DEGRADED/EMERGENCY modes
- **Quality Metrics**: Business KPIs (quality scores, incident rates, SLA compliance)
- **Incident Management**: Quality violations, remediation workflows

### **❌ SHOULD NOT DO**
- Infrastructure connectivity (delegate to StreamingBus)
- Low-level message serialization/deserialization
- Topic creation or partition management
- Network-level error handling
- Infrastructure monitoring (broker health, etc.)

### **🔧 INTERFACE CONTRACT**  
```python
class DataQualityOrchestratorInterface:
    # Pipeline Coordination
    async def orchestrate_quality_pipeline(message: Message) -> PipelineResult
    def register_quality_agents(agents: Dict[str, QualityAgent]) -> None
    
    # Business Logic
    def calculate_quality_score(stage_results: List[StageResult]) -> float
    def should_quality_circuit_breaker_block() -> bool
    def determine_pipeline_mode(context: QualityContext) -> PipelineMode
    
    # Quality Management
    async def publish_quality_incidents(incidents: List[Incident]) -> None
    async def get_quality_health() -> QualityHealthStatus
```

---

## 🔍 **Quality Agents - Domain Expertise Layer**

### **✅ RESPONSIBILITIES**
- **Domain-Specific Validation**: Each agent handles one quality domain
- **Validation Logic**: Schema rules, leakage detection, anomaly algorithms
- **Domain Metrics**: Agent-specific performance and accuracy metrics
- **Domain State**: Maintain validation models, thresholds, historical data

### **❌ SHOULD NOT DO**
- Cross-domain coordination (orchestrator's job)
- Infrastructure concerns (StreamingBus's job)  
- Quality scoring across multiple domains
- Pipeline state management

---

## 🤝 **Proper Integration Patterns**

### **Pattern 1: Infrastructure Dependency Injection**
```python
# Data Quality Orchestrator receives StreamingBus as dependency
class DataQualityOrchestrator:
    def __init__(self, streaming_bus: StreamingBusInterface):
        self.streaming_bus = streaming_bus  # Uses, doesn't implement
```

### **Pattern 2: Layered Error Handling**
```python
# Infrastructure errors bubble up, business errors are handled locally
try:
    result = await self.streaming_bus.publish(topic, key, payload)
except InfrastructureError as e:
    # Let infrastructure handle this - don't try to fix network issues
    raise
except QualityValidationError as e:
    # Handle business logic errors locally
    self._handle_quality_failure(e)
```

### **Pattern 3: Separate Circuit Breakers with Coordination**
```python
async def should_process_message(self) -> bool:
    # Check infrastructure first
    if self.streaming_bus.is_infrastructure_circuit_breaker_open():
        return False  # Can't process due to infrastructure
    
    # Check business logic second  
    if self._should_quality_circuit_breaker_block():
        return False  # Can't process due to quality failures
        
    return True
```

### **Pattern 4: Metrics Separation**
```python
# Infrastructure metrics (StreamingBus)
{
    "messages_per_second": 10000,
    "average_latency_ms": 5.2,
    "connection_pool_utilization": 0.75,
    "broker_availability": 0.999
}

# Business metrics (Quality Orchestrator)  
{
    "quality_score_average": 0.967,
    "pipeline_sla_compliance": 0.995,  
    "incidents_per_hour": 2.3,
    "degraded_mode_percentage": 0.05
}
```

---

## 🔄 **Proper Message Flow**

```
┌─────────────────┐    ┌──────────────────────┐    ┌─────────────────┐
│   StreamingBus  │ -> │ DataQualityOrchest. │ -> │  Quality Agents │
│ (Infrastructure)│    │   (Business Logic)   │    │ (Domain Expert) │
└─────────────────┘    └──────────────────────┘    └─────────────────┘

1. StreamingBus: "Here's a message from raw_data.trades"
2. Orchestrator: "Route this through 5-stage quality pipeline"  
3. Agents: "Validate schema/leakage/anomaly/freshness/reconciliation"
4. Orchestrator: "Calculate quality score, decide on clean.* publication"
5. StreamingBus: "Publish to clean.trades with quality metadata"
```

---

## ⚖️ **Decision Framework: "Who Should Handle This?"**

| **Scenario** | **Handler** | **Rationale** |
|--------------|-------------|---------------|
| Kafka broker is down | StreamingBus | Infrastructure failure |
| Message format is invalid | Quality Agent (Schema) | Domain-specific validation |  
| Quality score < threshold | Orchestrator | Business rule enforcement |
| Network timeout on publish | StreamingBus | Infrastructure resilience |
| Cross-validation fails | Orchestrator | Cross-domain business logic |
| SSL certificate expired | StreamingBus | Infrastructure security |
| Incident remediation | Orchestrator | Business process management |

---

## 🎖️ **Benefits of This Separation**

### **✅ TESTABILITY**
- Mock StreamingBus for orchestrator unit tests
- Mock Quality Agents for orchestrator integration tests
- Test infrastructure independently of business logic

### **✅ SCALABILITY**  
- Scale infrastructure (more brokers) independently of business logic
- Scale quality processing (more orchestrator instances) independently  
- Add new quality agents without touching infrastructure

### **✅ MAINTAINABILITY**
- Infrastructure changes don't affect business logic
- Business rule changes don't affect infrastructure
- Clear ownership boundaries for different teams

### **✅ RELIABILITY**
- Infrastructure failures don't break business logic  
- Business logic failures don't break infrastructure
- Independent circuit breakers and recovery strategies

---

## 🚀 **Implementation Guidelines**

1. **Use Dependency Injection**: Orchestrator receives StreamingBus interface
2. **Define Clear Interfaces**: Each layer exposes well-defined contracts
3. **Separate Error Domains**: Infrastructure vs Business error handling  
4. **Independent Circuit Breakers**: Different failure modes, different recovery
5. **Layer-Specific Metrics**: Infrastructure vs Business KPIs
6. **Clean Message Contracts**: Clear data structures between layers

This architecture ensures **each component does exactly what it should do** and **nothing it shouldn't**! 🎯