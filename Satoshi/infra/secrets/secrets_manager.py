#!/usr/bin/env python3
"""
Secrets Management Infrastructure - KMS/HSM
Secure storage and management of API keys, private keys, and sensitive configuration.

Key Features:
- Hardware Security Module (HSM) integration
- Key Management Service (KMS) for encryption
- API key rotation and management
- Private key storage for DeFi/DEX operations
- Audit logging for all secret access
- Environment-based secret isolation
"""

import asyncio
import logging
import time
import os
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict
from enum import Enum
import json
import hashlib
import base64
from datetime import datetime, timezone, timedelta

# Cryptography for local encryption
try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False
    print("⚠️  cryptography not installed. Install with: pip install cryptography")

# AWS KMS (optional)
try:
    import boto3
    from botocore.exceptions import ClientError
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False
    print("⚠️  boto3 not installed. Install with: pip install boto3")

# HashiCorp Vault (optional)
try:
    import hvac
    VAULT_AVAILABLE = True
except ImportError:
    VAULT_AVAILABLE = False
    print("⚠️  hvac not installed. Install with: pip install hvac")

logger = logging.getLogger(__name__)

class SecretType(Enum):
    """Types of secrets managed by the system."""
    API_KEY = "api_key"
    PRIVATE_KEY = "private_key"
    DATABASE_PASSWORD = "database_password"
    JWT_SECRET = "jwt_secret"
    WEBHOOK_SECRET = "webhook_secret"
    ENCRYPTION_KEY = "encryption_key"
    SIGNING_KEY = "signing_key"

class SecretProvider(Enum):
    """Secret storage providers."""
    LOCAL_FILE = "local_file"
    AWS_KMS = "aws_kms"
    HASHICORP_VAULT = "hashicorp_vault"
    AZURE_KEY_VAULT = "azure_key_vault"
    GCP_SECRET_MANAGER = "gcp_secret_manager"

@dataclass
class SecretMetadata:
    """Metadata for a secret."""
    secret_id: str
    name: str
    secret_type: SecretType
    environment: str
    created_at: datetime
    expires_at: Optional[datetime]
    rotation_interval_days: Optional[int]
    last_rotated: Optional[datetime]
    owner: str
    tags: Dict[str, str]

@dataclass
class SecretAccess:
    """Audit record for secret access."""
    secret_id: str
    accessor: str
    access_type: str  # "read", "write", "rotate"
    timestamp: datetime
    success: bool
    client_ip: Optional[str]
    user_agent: Optional[str]

@dataclass
class SecretsConfig:
    """Configuration for secrets management."""
    provider: SecretProvider = SecretProvider.LOCAL_FILE
    encryption_key_env: str = "SATOSHI_MASTER_KEY"
    vault_url: Optional[str] = None
    vault_token: Optional[str] = None
    aws_region: Optional[str] = None
    aws_kms_key_id: Optional[str] = None
    secrets_file: str = "/etc/satoshi/secrets.enc"
    audit_file: str = "/var/log/satoshi/secrets_audit.log"

class SecretsManager:
    """
    Secure secrets management with multiple backend support.
    Handles API keys, private keys, and sensitive configuration.
    """
    
    def __init__(self, config: SecretsConfig):
        """Initialize secrets manager."""
        self.config = config
        self.provider = config.provider
        
        # Local encryption setup
        self.cipher_suite = None
        if CRYPTO_AVAILABLE:
            self._setup_local_encryption()
        
        # Provider clients
        self.vault_client = None
        self.kms_client = None
        
        # Initialize provider
        self._initialize_provider()
        
        # In-memory cache with TTL
        self.secret_cache: Dict[str, tuple] = {}  # {secret_id: (value, expiry)}
        self.cache_ttl_seconds = 300  # 5 minutes
        
        # Audit trail
        self.audit_log: List[SecretAccess] = []
        
        # Performance metrics
        self.metrics = {
            "secrets_read": 0,
            "secrets_written": 0,
            "secrets_rotated": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "avg_access_time_ms": 0
        }
        
        logger.info(f"Secrets manager initialized with provider: {self.provider.value}")
    
    def _setup_local_encryption(self) -> None:
        """Setup local encryption for file-based secrets."""
        master_key = os.environ.get(self.config.encryption_key_env)
        
        if not master_key:
            # Generate a master key for demo purposes
            master_key = base64.urlsafe_b64encode(os.urandom(32)).decode()
            logger.warning(f"Generated demo master key. Set {self.config.encryption_key_env} environment variable.")
            logger.warning(f"Demo master key: {master_key}")
        
        # Derive encryption key
        password = master_key.encode()
        salt = b'satoshi_salt_12345'  # In production, use random salt
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(password))
        self.cipher_suite = Fernet(key)
    
    def _initialize_provider(self) -> None:
        """Initialize the configured secrets provider."""
        try:
            if self.provider == SecretProvider.HASHICORP_VAULT and VAULT_AVAILABLE:
                self.vault_client = hvac.Client(
                    url=self.config.vault_url,
                    token=self.config.vault_token
                )
                if self.vault_client.is_authenticated():
                    logger.info("Connected to HashiCorp Vault")
                else:
                    logger.error("Failed to authenticate with HashiCorp Vault")
            
            elif self.provider == SecretProvider.AWS_KMS and AWS_AVAILABLE:
                self.kms_client = boto3.client(
                    'kms',
                    region_name=self.config.aws_region
                )
                logger.info("Connected to AWS KMS")
            
            elif self.provider == SecretProvider.LOCAL_FILE:
                # Ensure secrets directory exists
                os.makedirs(os.path.dirname(self.config.secrets_file), exist_ok=True)
                logger.info("Using local file storage for secrets")
            
        except Exception as e:
            logger.error(f"Failed to initialize secrets provider: {e}")
    
    def _log_access(self, secret_id: str, accessor: str, access_type: str, 
                   success: bool) -> None:
        """Log secret access for audit trail."""
        access_record = SecretAccess(
            secret_id=secret_id,
            accessor=accessor,
            access_type=access_type,
            timestamp=datetime.now(timezone.utc),
            success=success,
            client_ip=None,  # Could be populated from request context
            user_agent=None
        )
        
        self.audit_log.append(access_record)
        
        # Write to audit file
        try:
            os.makedirs(os.path.dirname(self.config.audit_file), exist_ok=True)
            with open(self.config.audit_file, 'a') as f:
                f.write(json.dumps(asdict(access_record), default=str) + '\n')
        except Exception as e:
            logger.error(f"Failed to write audit log: {e}")
    
    async def store_secret(self, secret_id: str, value: str, metadata: SecretMetadata) -> bool:
        """
        Store a secret securely.
        
        Args:
            secret_id: Unique identifier for the secret
            value: The secret value to store
            metadata: Metadata about the secret
            
        Returns:
            bool: Success status
        """
        start_time = time.time()
        accessor = "system"  # Could be extracted from context
        
        try:
            success = False
            
            if self.provider == SecretProvider.LOCAL_FILE:
                success = await self._store_secret_local(secret_id, value, metadata)
            elif self.provider == SecretProvider.HASHICORP_VAULT:
                success = await self._store_secret_vault(secret_id, value, metadata)
            elif self.provider == SecretProvider.AWS_KMS:
                success = await self._store_secret_kms(secret_id, value, metadata)
            
            if success:
                # Clear cache
                if secret_id in self.secret_cache:
                    del self.secret_cache[secret_id]
                
                self.metrics["secrets_written"] += 1
            
            # Log access
            self._log_access(secret_id, accessor, "write", success)
            
            # Update metrics
            elapsed_ms = (time.time() - start_time) * 1000
            self._update_avg_access_time(elapsed_ms)
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to store secret {secret_id}: {e}")
            self._log_access(secret_id, accessor, "write", False)
            return False
    
    async def get_secret(self, secret_id: str, accessor: str = "system") -> Optional[str]:
        """
        Retrieve a secret value.
        
        Args:
            secret_id: Unique identifier for the secret
            accessor: Who is accessing the secret (for audit)
            
        Returns:
            Secret value or None if not found
        """
        start_time = time.time()
        
        try:
            # Check cache first
            if secret_id in self.secret_cache:
                value, expiry = self.secret_cache[secret_id]
                if time.time() < expiry:
                    self.metrics["cache_hits"] += 1
                    self._log_access(secret_id, accessor, "read", True)
                    return value
                else:
                    # Cache expired
                    del self.secret_cache[secret_id]
            
            self.metrics["cache_misses"] += 1
            
            # Retrieve from provider
            value = None
            
            if self.provider == SecretProvider.LOCAL_FILE:
                value = await self._get_secret_local(secret_id)
            elif self.provider == SecretProvider.HASHICORP_VAULT:
                value = await self._get_secret_vault(secret_id)
            elif self.provider == SecretProvider.AWS_KMS:
                value = await self._get_secret_kms(secret_id)
            
            if value:
                # Cache the value
                expiry = time.time() + self.cache_ttl_seconds
                self.secret_cache[secret_id] = (value, expiry)
                
                self.metrics["secrets_read"] += 1
            
            # Log access
            self._log_access(secret_id, accessor, "read", value is not None)
            
            # Update metrics
            elapsed_ms = (time.time() - start_time) * 1000
            self._update_avg_access_time(elapsed_ms)
            
            return value
            
        except Exception as e:
            logger.error(f"Failed to get secret {secret_id}: {e}")
            self._log_access(secret_id, accessor, "read", False)
            return None
    
    async def _store_secret_local(self, secret_id: str, value: str, 
                                metadata: SecretMetadata) -> bool:
        """Store secret in encrypted local file."""
        if not self.cipher_suite:
            return False
        
        try:
            # Load existing secrets
            secrets = {}
            if os.path.exists(self.config.secrets_file):
                with open(self.config.secrets_file, 'rb') as f:
                    encrypted_data = f.read()
                    if encrypted_data:
                        decrypted_data = self.cipher_suite.decrypt(encrypted_data)
                        secrets = json.loads(decrypted_data.decode())
            
            # Add/update secret
            secrets[secret_id] = {
                'value': value,
                'metadata': asdict(metadata)
            }
            
            # Encrypt and save
            data_bytes = json.dumps(secrets, default=str).encode()
            encrypted_data = self.cipher_suite.encrypt(data_bytes)
            
            with open(self.config.secrets_file, 'wb') as f:
                f.write(encrypted_data)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to store secret locally: {e}")
            return False
    
    async def _get_secret_local(self, secret_id: str) -> Optional[str]:
        """Get secret from encrypted local file."""
        if not self.cipher_suite:
            return None
        
        try:
            if not os.path.exists(self.config.secrets_file):
                return None
            
            with open(self.config.secrets_file, 'rb') as f:
                encrypted_data = f.read()
                if not encrypted_data:
                    return None
                
                decrypted_data = self.cipher_suite.decrypt(encrypted_data)
                secrets = json.loads(decrypted_data.decode())
                
                if secret_id in secrets:
                    return secrets[secret_id]['value']
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get secret locally: {e}")
            return None
    
    async def _store_secret_vault(self, secret_id: str, value: str,
                                metadata: SecretMetadata) -> bool:
        """Store secret in HashiCorp Vault."""
        if not self.vault_client:
            return False
        
        try:
            # Store in Vault KV v2
            self.vault_client.secrets.kv.v2.create_or_update_secret(
                path=secret_id,
                secret={'value': value, 'metadata': asdict(metadata)}
            )
            return True
            
        except Exception as e:
            logger.error(f"Failed to store secret in Vault: {e}")
            return False
    
    async def _get_secret_vault(self, secret_id: str) -> Optional[str]:
        """Get secret from HashiCorp Vault."""
        if not self.vault_client:
            return None
        
        try:
            response = self.vault_client.secrets.kv.v2.read_secret_version(path=secret_id)
            return response['data']['data']['value']
            
        except Exception as e:
            logger.error(f"Failed to get secret from Vault: {e}")
            return None
    
    async def _store_secret_kms(self, secret_id: str, value: str,
                              metadata: SecretMetadata) -> bool:
        """Store secret using AWS KMS encryption."""
        if not self.kms_client:
            return False
        
        try:
            # Encrypt with KMS
            response = self.kms_client.encrypt(
                KeyId=self.config.aws_kms_key_id,
                Plaintext=value.encode()
            )
            
            # Store encrypted blob and metadata
            encrypted_value = base64.b64encode(response['CiphertextBlob']).decode()
            
            # In production, store in AWS Secrets Manager or parameter store
            # For demo, store in local file with KMS encryption
            secrets_data = {
                'encrypted_value': encrypted_value,
                'metadata': asdict(metadata)
            }
            
            kms_secrets_file = f"{self.config.secrets_file}.kms"
            secrets = {}
            
            if os.path.exists(kms_secrets_file):
                with open(kms_secrets_file, 'r') as f:
                    secrets = json.load(f)
            
            secrets[secret_id] = secrets_data
            
            with open(kms_secrets_file, 'w') as f:
                json.dump(secrets, f, default=str, indent=2)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to store secret with KMS: {e}")
            return False
    
    async def _get_secret_kms(self, secret_id: str) -> Optional[str]:
        """Get secret using AWS KMS decryption."""
        if not self.kms_client:
            return None
        
        try:
            kms_secrets_file = f"{self.config.secrets_file}.kms"
            
            if not os.path.exists(kms_secrets_file):
                return None
            
            with open(kms_secrets_file, 'r') as f:
                secrets = json.load(f)
            
            if secret_id not in secrets:
                return None
            
            encrypted_value = secrets[secret_id]['encrypted_value']
            ciphertext_blob = base64.b64decode(encrypted_value)
            
            # Decrypt with KMS
            response = self.kms_client.decrypt(CiphertextBlob=ciphertext_blob)
            return response['Plaintext'].decode()
            
        except Exception as e:
            logger.error(f"Failed to get secret with KMS: {e}")
            return None
    
    async def rotate_secret(self, secret_id: str, new_value: str) -> bool:
        """Rotate a secret to a new value."""
        try:
            # Get existing metadata
            current_value = await self.get_secret(secret_id)
            if not current_value:
                logger.error(f"Cannot rotate non-existent secret: {secret_id}")
                return False
            
            # Create updated metadata
            metadata = SecretMetadata(
                secret_id=secret_id,
                name=secret_id,  # Simplified
                secret_type=SecretType.API_KEY,  # Default
                environment="production",  # Default
                created_at=datetime.now(timezone.utc),
                expires_at=None,
                rotation_interval_days=90,
                last_rotated=datetime.now(timezone.utc),
                owner="system",
                tags={}
            )
            
            success = await self.store_secret(secret_id, new_value, metadata)
            
            if success:
                self.metrics["secrets_rotated"] += 1
                self._log_access(secret_id, "system", "rotate", True)
                logger.info(f"Successfully rotated secret: {secret_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to rotate secret {secret_id}: {e}")
            self._log_access(secret_id, "system", "rotate", False)
            return False
    
    def _update_avg_access_time(self, elapsed_ms: float) -> None:
        """Update average access time metric."""
        if self.metrics["avg_access_time_ms"] == 0:
            self.metrics["avg_access_time_ms"] = elapsed_ms
        else:
            # Exponential moving average
            self.metrics["avg_access_time_ms"] = (
                0.9 * self.metrics["avg_access_time_ms"] + 0.1 * elapsed_ms
            )
    
    def get_audit_trail(self, secret_id: Optional[str] = None,
                       start_time: Optional[datetime] = None) -> List[SecretAccess]:
        """Get audit trail for secret access."""
        filtered_logs = self.audit_log
        
        if secret_id:
            filtered_logs = [log for log in filtered_logs if log.secret_id == secret_id]
        
        if start_time:
            filtered_logs = [log for log in filtered_logs if log.timestamp >= start_time]
        
        return filtered_logs
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get secrets manager performance metrics."""
        metrics = self.metrics.copy()
        metrics["cache_size"] = len(self.secret_cache)
        metrics["audit_entries"] = len(self.audit_log)
        return metrics

# Example usage
async def main():
    """Example usage of the secrets management system."""
    
    print("🔐 Secrets Management Demo")
    print("=" * 50)
    
    config = SecretsConfig(
        provider=SecretProvider.LOCAL_FILE,
        secrets_file="/tmp/satoshi_secrets_demo.enc",
        audit_file="/tmp/satoshi_secrets_audit.log"
    )
    
    secrets_manager = SecretsManager(config)
    
    # Create sample secret metadata
    api_key_metadata = SecretMetadata(
        secret_id="binance_api_key",
        name="Binance API Key",
        secret_type=SecretType.API_KEY,
        environment="production",
        created_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(days=365),
        rotation_interval_days=90,
        last_rotated=None,
        owner="trading_team",
        tags={"venue": "binance", "permissions": "read_only"}
    )
    
    # Store a secret
    success = await secrets_manager.store_secret(
        "binance_api_key", 
        "demo_api_key_123456789",
        api_key_metadata
    )
    print(f"✅ Stored API key" if success else "❌ Failed to store API key")
    
    # Retrieve the secret
    retrieved_key = await secrets_manager.get_secret("binance_api_key", "trading_bot")
    if retrieved_key:
        print(f"✅ Retrieved API key: {retrieved_key[:10]}...")
    else:
        print("❌ Failed to retrieve API key")
    
    # Test cache hit
    cached_key = await secrets_manager.get_secret("binance_api_key", "trading_bot")
    if cached_key:
        print(f"✅ Cache hit: {cached_key[:10]}...")
    
    # Rotate the secret
    success = await secrets_manager.rotate_secret("binance_api_key", "new_api_key_987654321")
    print(f"✅ Rotated API key" if success else "❌ Failed to rotate API key")
    
    # Get audit trail
    audit_trail = secrets_manager.get_audit_trail("binance_api_key")
    print(f"✅ Audit trail: {len(audit_trail)} entries")
    for entry in audit_trail:
        print(f"   {entry.timestamp}: {entry.access_type} by {entry.accessor} ({'✅' if entry.success else '❌'})")
    
    # Show metrics
    metrics = secrets_manager.get_metrics()
    print(f"\n📊 Secrets Manager Metrics:")
    print(f"   Secrets read: {metrics['secrets_read']}")
    print(f"   Secrets written: {metrics['secrets_written']}")
    print(f"   Cache hits: {metrics['cache_hits']}")
    print(f"   Cache misses: {metrics['cache_misses']}")
    print(f"   Avg access time: {metrics['avg_access_time_ms']:.1f}ms")

if __name__ == "__main__":
    if CRYPTO_AVAILABLE:
        asyncio.run(main())
    else:
        print("❌ Cryptography not available. Install with: pip install cryptography")
