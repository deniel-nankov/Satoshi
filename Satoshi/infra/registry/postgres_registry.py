#!/usr/bin/env python3
"""
Registry Database Infrastructure - PostgreSQL
Metadata registry for configurations, feature specs, model registry, and experiment tracking.

Key Features:
- Feature specification registry with versioning
- Model artifact registry with lineage tracking
- Experiment configuration and results
- Agent configuration management
- Schema evolution tracking
- ACID compliance for critical metadata
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict
from enum import Enum
import json
import uuid
from datetime import datetime, timezone

# PostgreSQL async client
try:
    import asyncpg
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False
    print("⚠️  asyncpg not installed. Install with: pip install asyncpg")

# SQLAlchemy for ORM (optional)
try:
    from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, DateTime, Text, Boolean, Float
    from sqlalchemy.dialects.postgresql import UUID, JSONB
    from sqlalchemy.orm import sessionmaker
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False
    print("⚠️  SQLAlchemy not installed. Install with: pip install sqlalchemy")

import pandas as pd

logger = logging.getLogger(__name__)

class RegistryType(Enum):
    """Types of registry entries."""
    FEATURE_SPEC = "feature_spec"
    MODEL_ARTIFACT = "model_artifact"
    EXPERIMENT_CONFIG = "experiment_config"
    AGENT_CONFIG = "agent_config"
    SCHEMA_VERSION = "schema_version"
    DEPLOYMENT_CONFIG = "deployment_config"

@dataclass
class RegistryConfig:
    """Configuration for PostgreSQL registry."""
    host: str = "localhost"
    port: int = 5432
    database: str = "satoshi_registry"
    username: str = "postgres"
    password: str = ""
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30

@dataclass
class FeatureSpec:
    """Feature specification for the registry."""
    name: str
    version: str
    feature_type: str
    description: str
    schema: Dict[str, Any]
    dependencies: List[str]
    owner: str
    tags: Dict[str, str]
    created_at: datetime
    deprecated: bool = False

@dataclass 
class ModelArtifact:
    """Model artifact for the registry."""
    model_id: str
    name: str
    version: str
    model_type: str
    framework: str
    artifact_path: str
    metrics: Dict[str, float]
    hyperparameters: Dict[str, Any]
    training_data_hash: str
    feature_dependencies: List[str]
    owner: str
    tags: Dict[str, str]
    created_at: datetime
    deployed: bool = False

@dataclass
class ExperimentConfig:
    """Experiment configuration for the registry."""
    experiment_id: str
    name: str
    description: str
    strategy: str
    start_date: datetime
    end_date: datetime
    parameters: Dict[str, Any]
    feature_specs: List[str]
    model_versions: List[str]
    status: str
    owner: str
    created_at: datetime

class PostgreSQLRegistry:
    """
    PostgreSQL-based metadata registry for the trading system.
    Manages feature specs, model artifacts, experiments, and configurations.
    """
    
    def __init__(self, config: RegistryConfig):
        """Initialize PostgreSQL registry."""
        self.config = config
        self.connection_string = (
            f"postgresql://{config.username}:{config.password}@"
            f"{config.host}:{config.port}/{config.database}"
        )
        
        self.pool = None
        self.engine = None
        
        # Performance metrics
        self.metrics = {
            "connections_created": 0,
            "queries_executed": 0,
            "records_inserted": 0,
            "records_updated": 0,
            "avg_query_time_ms": 0
        }
        
        logger.info(f"Registry initialized for {config.host}:{config.port}/{config.database}")
    
    async def initialize(self) -> bool:
        """Initialize database connection and schema."""
        if not POSTGRES_AVAILABLE:
            logger.error("PostgreSQL client not available")
            return False
        
        try:
            # Create connection pool
            self.pool = await asyncpg.create_pool(
                self.connection_string,
                min_size=5,
                max_size=self.config.pool_size,
                command_timeout=self.config.pool_timeout
            )
            
            # Initialize schema
            await self._initialize_schema()
            
            logger.info("PostgreSQL registry initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL registry: {e}")
            return False
    
    async def _initialize_schema(self) -> None:
        """Initialize the registry database schema."""
        async with self.pool.acquire() as conn:
            # Feature specifications table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS feature_specs (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    name VARCHAR(255) NOT NULL,
                    version VARCHAR(50) NOT NULL,
                    feature_type VARCHAR(100) NOT NULL,
                    description TEXT,
                    schema JSONB NOT NULL,
                    dependencies JSONB,
                    owner VARCHAR(255) NOT NULL,
                    tags JSONB,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    deprecated BOOLEAN DEFAULT FALSE,
                    UNIQUE(name, version)
                )
            """)
            
            # Model artifacts table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS model_artifacts (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    model_id VARCHAR(255) UNIQUE NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    version VARCHAR(50) NOT NULL,
                    model_type VARCHAR(100) NOT NULL,
                    framework VARCHAR(100) NOT NULL,
                    artifact_path TEXT NOT NULL,
                    metrics JSONB,
                    hyperparameters JSONB,
                    training_data_hash VARCHAR(64),
                    feature_dependencies JSONB,
                    owner VARCHAR(255) NOT NULL,
                    tags JSONB,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    deployed BOOLEAN DEFAULT FALSE,
                    deployed_at TIMESTAMP WITH TIME ZONE
                )
            """)
            
            # Experiment configurations table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS experiment_configs (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    experiment_id VARCHAR(255) UNIQUE NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    description TEXT,
                    strategy VARCHAR(255) NOT NULL,
                    start_date TIMESTAMP WITH TIME ZONE NOT NULL,
                    end_date TIMESTAMP WITH TIME ZONE NOT NULL,
                    parameters JSONB,
                    feature_specs JSONB,
                    model_versions JSONB,
                    status VARCHAR(50) DEFAULT 'planned',
                    results JSONB,
                    owner VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)
            
            # Agent configurations table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS agent_configs (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    agent_name VARCHAR(255) NOT NULL,
                    version VARCHAR(50) NOT NULL,
                    config JSONB NOT NULL,
                    environment VARCHAR(50) NOT NULL,
                    active BOOLEAN DEFAULT TRUE,
                    owner VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    UNIQUE(agent_name, version, environment)
                )
            """)
            
            # Schema versions table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS schema_versions (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    schema_name VARCHAR(255) NOT NULL,
                    version VARCHAR(50) NOT NULL,
                    schema_definition JSONB NOT NULL,
                    migration_script TEXT,
                    backward_compatible BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    UNIQUE(schema_name, version)
                )
            """)
            
            # Create indexes for performance
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_feature_specs_name ON feature_specs(name)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_feature_specs_type ON feature_specs(feature_type)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_model_artifacts_name ON model_artifacts(name)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_model_artifacts_type ON model_artifacts(model_type)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_experiments_strategy ON experiment_configs(strategy)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_experiments_status ON experiment_configs(status)")
            
            logger.info("Registry schema initialized")
    
    async def register_feature_spec(self, spec: FeatureSpec) -> bool:
        """Register a feature specification."""
        if not self.pool:
            return False
        
        try:
            start_time = time.time()
            
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO feature_specs 
                    (name, version, feature_type, description, schema, dependencies, owner, tags, created_at, deprecated)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (name, version) DO UPDATE SET
                    description = EXCLUDED.description,
                    schema = EXCLUDED.schema,
                    dependencies = EXCLUDED.dependencies,
                    tags = EXCLUDED.tags,
                    updated_at = NOW(),
                    deprecated = EXCLUDED.deprecated
                """, 
                spec.name, spec.version, spec.feature_type, spec.description,
                json.dumps(spec.schema), json.dumps(spec.dependencies),
                spec.owner, json.dumps(spec.tags), spec.created_at, spec.deprecated)
            
            self._update_metrics(time.time() - start_time, "insert")
            logger.info(f"Registered feature spec: {spec.name} v{spec.version}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register feature spec: {e}")
            return False
    
    async def register_model_artifact(self, artifact: ModelArtifact) -> bool:
        """Register a model artifact."""
        if not self.pool:
            return False
        
        try:
            start_time = time.time()
            
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO model_artifacts 
                    (model_id, name, version, model_type, framework, artifact_path, 
                     metrics, hyperparameters, training_data_hash, feature_dependencies, 
                     owner, tags, created_at, deployed)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                    ON CONFLICT (model_id) DO UPDATE SET
                    artifact_path = EXCLUDED.artifact_path,
                    metrics = EXCLUDED.metrics,
                    hyperparameters = EXCLUDED.hyperparameters,
                    tags = EXCLUDED.tags,
                    updated_at = NOW(),
                    deployed = EXCLUDED.deployed
                """,
                artifact.model_id, artifact.name, artifact.version, artifact.model_type,
                artifact.framework, artifact.artifact_path, json.dumps(artifact.metrics),
                json.dumps(artifact.hyperparameters), artifact.training_data_hash,
                json.dumps(artifact.feature_dependencies), artifact.owner,
                json.dumps(artifact.tags), artifact.created_at, artifact.deployed)
            
            self._update_metrics(time.time() - start_time, "insert")
            logger.info(f"Registered model artifact: {artifact.model_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register model artifact: {e}")
            return False
    
    async def register_experiment(self, experiment: ExperimentConfig) -> bool:
        """Register an experiment configuration."""
        if not self.pool:
            return False
        
        try:
            start_time = time.time()
            
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO experiment_configs 
                    (experiment_id, name, description, strategy, start_date, end_date,
                     parameters, feature_specs, model_versions, status, owner, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (experiment_id) DO UPDATE SET
                    description = EXCLUDED.description,
                    parameters = EXCLUDED.parameters,
                    status = EXCLUDED.status,
                    updated_at = NOW()
                """,
                experiment.experiment_id, experiment.name, experiment.description,
                experiment.strategy, experiment.start_date, experiment.end_date,
                json.dumps(experiment.parameters), json.dumps(experiment.feature_specs),
                json.dumps(experiment.model_versions), experiment.status,
                experiment.owner, experiment.created_at)
            
            self._update_metrics(time.time() - start_time, "insert")
            logger.info(f"Registered experiment: {experiment.experiment_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register experiment: {e}")
            return False
    
    async def get_feature_spec(self, name: str, version: str = "latest") -> Optional[FeatureSpec]:
        """Get a feature specification by name and version."""
        if not self.pool:
            return None
        
        try:
            start_time = time.time()
            
            async with self.pool.acquire() as conn:
                if version == "latest":
                    query = """
                        SELECT * FROM feature_specs 
                        WHERE name = $1 AND deprecated = FALSE 
                        ORDER BY created_at DESC LIMIT 1
                    """
                    row = await conn.fetchrow(query, name)
                else:
                    query = """
                        SELECT * FROM feature_specs 
                        WHERE name = $1 AND version = $2
                    """
                    row = await conn.fetchrow(query, name, version)
                
                if row:
                    spec = FeatureSpec(
                        name=row['name'],
                        version=row['version'],
                        feature_type=row['feature_type'],
                        description=row['description'],
                        schema=json.loads(row['schema']),
                        dependencies=json.loads(row['dependencies']) if row['dependencies'] else [],
                        owner=row['owner'],
                        tags=json.loads(row['tags']) if row['tags'] else {},
                        created_at=row['created_at'],
                        deprecated=row['deprecated']
                    )
                    
                    self._update_metrics(time.time() - start_time, "query")
                    return spec
                
                return None
                
        except Exception as e:
            logger.error(f"Failed to get feature spec: {e}")
            return None
    
    async def get_model_artifact(self, model_id: str) -> Optional[ModelArtifact]:
        """Get a model artifact by ID."""
        if not self.pool:
            return None
        
        try:
            start_time = time.time()
            
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM model_artifacts WHERE model_id = $1", model_id
                )
                
                if row:
                    artifact = ModelArtifact(
                        model_id=row['model_id'],
                        name=row['name'],
                        version=row['version'],
                        model_type=row['model_type'],
                        framework=row['framework'],
                        artifact_path=row['artifact_path'],
                        metrics=json.loads(row['metrics']) if row['metrics'] else {},
                        hyperparameters=json.loads(row['hyperparameters']) if row['hyperparameters'] else {},
                        training_data_hash=row['training_data_hash'],
                        feature_dependencies=json.loads(row['feature_dependencies']) if row['feature_dependencies'] else [],
                        owner=row['owner'],
                        tags=json.loads(row['tags']) if row['tags'] else {},
                        created_at=row['created_at'],
                        deployed=row['deployed']
                    )
                    
                    self._update_metrics(time.time() - start_time, "query")
                    return artifact
                
                return None
                
        except Exception as e:
            logger.error(f"Failed to get model artifact: {e}")
            return None
    
    async def list_active_experiments(self, strategy: Optional[str] = None) -> List[ExperimentConfig]:
        """List active experiments, optionally filtered by strategy."""
        if not self.pool:
            return []
        
        try:
            start_time = time.time()
            
            async with self.pool.acquire() as conn:
                if strategy:
                    query = """
                        SELECT * FROM experiment_configs 
                        WHERE status IN ('running', 'planned') AND strategy = $1
                        ORDER BY created_at DESC
                    """
                    rows = await conn.fetch(query, strategy)
                else:
                    query = """
                        SELECT * FROM experiment_configs 
                        WHERE status IN ('running', 'planned')
                        ORDER BY created_at DESC
                    """
                    rows = await conn.fetch(query)
                
                experiments = []
                for row in rows:
                    experiment = ExperimentConfig(
                        experiment_id=row['experiment_id'],
                        name=row['name'],
                        description=row['description'],
                        strategy=row['strategy'],
                        start_date=row['start_date'],
                        end_date=row['end_date'],
                        parameters=json.loads(row['parameters']) if row['parameters'] else {},
                        feature_specs=json.loads(row['feature_specs']) if row['feature_specs'] else [],
                        model_versions=json.loads(row['model_versions']) if row['model_versions'] else [],
                        status=row['status'],
                        owner=row['owner'],
                        created_at=row['created_at']
                    )
                    experiments.append(experiment)
                
                self._update_metrics(time.time() - start_time, "query")
                return experiments
                
        except Exception as e:
            logger.error(f"Failed to list active experiments: {e}")
            return []
    
    async def update_model_deployment_status(self, model_id: str, deployed: bool) -> bool:
        """Update model deployment status."""
        if not self.pool:
            return False
        
        try:
            start_time = time.time()
            
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    UPDATE model_artifacts 
                    SET deployed = $1, deployed_at = $2, updated_at = NOW()
                    WHERE model_id = $3
                """, deployed, datetime.now(timezone.utc) if deployed else None, model_id)
            
            self._update_metrics(time.time() - start_time, "update")
            logger.info(f"Updated deployment status for model {model_id}: {deployed}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update model deployment status: {e}")
            return False
    
    def _update_metrics(self, elapsed_time: float, operation_type: str) -> None:
        """Update performance metrics."""
        elapsed_ms = elapsed_time * 1000
        
        self.metrics["queries_executed"] += 1
        
        if operation_type == "insert":
            self.metrics["records_inserted"] += 1
        elif operation_type == "update":
            self.metrics["records_updated"] += 1
        
        # Update average query time
        if self.metrics["avg_query_time_ms"] == 0:
            self.metrics["avg_query_time_ms"] = elapsed_ms
        else:
            self.metrics["avg_query_time_ms"] = (
                0.9 * self.metrics["avg_query_time_ms"] + 0.1 * elapsed_ms
            )
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get registry performance metrics."""
        return self.metrics.copy()
    
    async def close(self) -> None:
        """Close database connections."""
        if self.pool:
            await self.pool.close()
            logger.info("PostgreSQL registry connections closed")

# Example usage
async def main():
    """Example usage of the PostgreSQL registry."""
    
    print("📚 PostgreSQL Registry Demo")
    print("=" * 50)
    
    config = RegistryConfig(
        host="localhost",
        port=5432,
        database="satoshi_registry",
        username="satoshi",
        password="secure_password"
    )
    
    registry = PostgreSQLRegistry(config)
    
    if not await registry.initialize():
        print("❌ PostgreSQL not available")
        return
    
    # Register a feature spec
    feature_spec = FeatureSpec(
        name="momentum_features",
        version="1.0.0",
        feature_type="momentum",
        description="Price momentum and trend features",
        schema={
            "fields": ["price_change_1h", "price_change_4h", "price_change_24h"],
            "types": ["float64", "float64", "float64"]
        },
        dependencies=["clean.market.trades"],
        owner="feature_factory",
        tags={"category": "technical", "frequency": "1min"},
        created_at=datetime.now(timezone.utc)
    )
    
    success = await registry.register_feature_spec(feature_spec)
    print(f"✅ Registered feature spec" if success else "❌ Failed to register feature spec")
    
    # Register a model artifact
    model_artifact = ModelArtifact(
        model_id="momentum_model_v1",
        name="Momentum Strategy Model",
        version="1.0.0",
        model_type="gradient_boosting",
        framework="xgboost",
        artifact_path="/models/momentum_v1.pkl",
        metrics={"accuracy": 0.85, "sharpe": 2.3, "max_drawdown": 0.12},
        hyperparameters={"n_estimators": 100, "max_depth": 6, "learning_rate": 0.1},
        training_data_hash="abc123def456",
        feature_dependencies=["momentum_features:1.0.0"],
        owner="research_team",
        tags={"strategy": "momentum", "asset_class": "crypto"},
        created_at=datetime.now(timezone.utc)
    )
    
    success = await registry.register_model_artifact(model_artifact)
    print(f"✅ Registered model artifact" if success else "❌ Failed to register model artifact")
    
    # Register an experiment
    experiment = ExperimentConfig(
        experiment_id="momentum_backtest_001",
        name="Momentum Strategy Backtest",
        description="Testing momentum strategy on crypto markets",
        strategy="momentum",
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2024, 12, 31, tzinfo=timezone.utc),
        parameters={"lookback_hours": 24, "rebalance_freq": "4h", "risk_target": 0.10},
        feature_specs=["momentum_features:1.0.0"],
        model_versions=["momentum_model_v1"],
        status="planned",
        owner="research_team",
        created_at=datetime.now(timezone.utc)
    )
    
    success = await registry.register_experiment(experiment)
    print(f"✅ Registered experiment" if success else "❌ Failed to register experiment")
    
    # Retrieve feature spec
    retrieved_spec = await registry.get_feature_spec("momentum_features", "1.0.0")
    if retrieved_spec:
        print(f"✅ Retrieved feature spec: {retrieved_spec.name} v{retrieved_spec.version}")
        print(f"   Fields: {retrieved_spec.schema['fields']}")
    
    # List active experiments
    active_experiments = await registry.list_active_experiments()
    print(f"✅ Found {len(active_experiments)} active experiments")
    
    # Show metrics
    metrics = registry.get_metrics()
    print(f"\n📊 Registry Metrics:")
    print(f"   Queries executed: {metrics['queries_executed']}")
    print(f"   Records inserted: {metrics['records_inserted']}")
    print(f"   Avg query time: {metrics['avg_query_time_ms']:.1f}ms")
    
    await registry.close()

if __name__ == "__main__":
    if POSTGRES_AVAILABLE:
        asyncio.run(main())
    else:
        print("❌ PostgreSQL not available. Install with: pip install asyncpg")
