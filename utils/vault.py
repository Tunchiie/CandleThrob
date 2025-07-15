"""
Oracle Cloud Vault Integration for CandleThrob Transformations

This module provides a comprehensive integration with Oracle Cloud Vault for secure
secret management with advanced features including caching, retry logic, performance
monitoring, and error handling.

Features:
- Secure secret retrieval with comprehensive error handling
- Intelligent caching with TTL and automatic refresh
- Retry logic with exponential backoff
- Performance monitoring and metrics
- Audit trail for secret access
- Connection pooling and resource management
- Security best practices and validation
- Health monitoring and diagnostics

Author: Adetunji Fasiku
Version: 3.0.0
Last Updated: 2025-07-14
"""

import oci
import base64
import time
import threading
import hashlib
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, asdict
from contextlib import contextmanager
import traceback
from datetime import datetime, timedelta

from CandleThrob.utils.logging_config import get_database_logger, performance_monitor

# Configuration constants
DEFAULT_CACHE_TTL = 3600  # 1 hour
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY = 1  # seconds
DEFAULT_MAX_RETRY_DELAY = 60  # seconds
DEFAULT_CONNECTION_TIMEOUT = 30  # seconds
MAX_CACHE_SIZE = 100  # Maximum number of cached secrets

@dataclass
class SecretCacheEntry:
    """Cache entry for a secret."""
    secret_id: str
    value: str
    timestamp: datetime
    ttl: int
    access_count: int = 0
    last_accessed: datetime = None
    
    def is_expired(self) -> bool:
        """Check if the cache entry has expired."""
        return datetime.now() > self.timestamp + timedelta(seconds=self.ttl)
    
    def should_refresh(self) -> bool:
        """Check if the cache entry should be refreshed (80% of TTL)."""
        refresh_time = self.timestamp + timedelta(seconds=int(self.ttl * 0.8))
        return datetime.now() > refresh_time

@dataclass
class VaultConfig:
    """Configuration for vault operations."""
    cache_ttl: int = DEFAULT_CACHE_TTL
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS
    retry_delay: int = DEFAULT_RETRY_DELAY
    max_retry_delay: int = DEFAULT_MAX_RETRY_DELAY
    connection_timeout: int = DEFAULT_CONNECTION_TIMEOUT
    enable_caching: bool = True
    enable_audit_trail: bool = True
    enable_performance_monitoring: bool = True
    max_cache_size: int = MAX_CACHE_SIZE
    enable_health_monitoring: bool = True

class SecretCache:
    """Cache for vault secrets with TTL and refresh logic."""
    
    def __init__(self, config: VaultConfig):
        self.config = config
        self.cache: Dict[str, SecretCacheEntry] = {}
        self.lock = threading.RLock()
        self.logger = get_database_logger(__name__)
    
    def _cleanup_expired_entries(self):
        """Remove expired cache entries."""
        with self.lock:
            expired_keys = [key for key, entry in self.cache.items() if entry.is_expired()]
            for key in expired_keys:
                del self.cache[key]
                self.logger.debug(f"Removed expired cache entry: {key}")
    
    def _evict_oldest_if_full(self):
        """Evict oldest entries if cache is full."""
        with self.lock:
            if len(self.cache) >= self.config.max_cache_size:
                # Sort by last accessed time and remove oldest
                sorted_entries = sorted(
                    self.cache.items(),
                    key=lambda x: x[1].last_accessed or x[1].timestamp
                )
                # Remove oldest 20% of entries
                remove_count = max(1, len(sorted_entries) // 5)
                for key, _ in sorted_entries[:remove_count]:
                    del self.cache[key]
                    self.logger.debug(f"Evicted cache entry: {key}")
    
    def get(self, secret_id: str) -> Optional[str]:
        """
        Get a secret from cache if available and not expired.
        
        Args:
            secret_id (str): The secret ID to retrieve
            
        Returns:
            Optional[str]: The cached secret value or None if not found/expired
        """
        with self.lock:
            self._cleanup_expired_entries()
            
            if secret_id not in self.cache:
                return None
            
            entry = self.cache[secret_id]
            if entry.is_expired():
                del self.cache[secret_id]
                self.logger.debug(f"Cache entry expired: {secret_id}")
                return None
            
            # Update access statistics
            entry.access_count += 1
            entry.last_accessed = datetime.now()
            
            self.logger.debug(f"Cache hit for secret: {secret_id}")
            return entry.value
    
    def set(self, secret_id: str, value: str, ttl: Optional[int] = None):
        """
        Store a secret in cache with TTL.
        
        Args:
            secret_id (str): The secret ID
            value (str): The secret value
            ttl (Optional[int]): Time to live in seconds
        """
        with self.lock:
            self._evict_oldest_if_full()
            
            ttl = ttl or self.config.cache_ttl
            entry = SecretCacheEntry(
                secret_id=secret_id,
                value=value,
                timestamp=datetime.now(),
                ttl=ttl,
                access_count=1,
                last_accessed=datetime.now()
            )
            
            self.cache[secret_id] = entry
            self.logger.debug(f"Cached secret: {secret_id}")
    
    def invalidate(self, secret_id: str):
        """Invalidate a specific cache entry."""
        with self.lock:
            if secret_id in self.cache:
                del self.cache[secret_id]
                self.logger.debug(f"Invalidated cache entry: {secret_id}")
    
    def clear(self):
        """Clear all cache entries."""
        with self.lock:
            self.cache.clear()
            self.logger.info("Secret cache cleared")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self.lock:
            total_entries = len(self.cache)
            expired_entries = sum(1 for entry in self.cache.values() if entry.is_expired())
            total_accesses = sum(entry.access_count for entry in self.cache.values())
            
            return {
                "total_entries": total_entries,
                "expired_entries": expired_entries,
                "valid_entries": total_entries - expired_entries,
                "total_accesses": total_accesses,
                "cache_hit_rate": total_accesses / max(total_entries, 1),
                "max_size": self.config.max_cache_size
            }

class VaultHealthMonitor:
    """Health monitoring for vault operations."""
    
    def __init__(self):
        self.operation_history: List[Dict[str, Any]] = []
        self.lock = threading.Lock()
        self.logger = get_database_logger(__name__)
    
    def record_operation(self, operation: str, secret_id: str, success: bool, 
                        duration: float, error_message: Optional[str] = None):
        """Record a vault operation for health monitoring."""
        with self.lock:
            record = {
                "operation": operation,
                "secret_id": secret_id,
                "success": success,
                "duration": duration,
                "error_message": error_message,
                "timestamp": datetime.now().isoformat()
            }
            
            self.operation_history.append(record)
            # Keep only last 100 operations
            if len(self.operation_history) > 100:
                self.operation_history = self.operation_history[-100:]
    
    def get_health_metrics(self) -> Dict[str, Any]:
        """Get health metrics for vault operations."""
        with self.lock:
            if not self.operation_history:
                return {"status": "unknown", "total_operations": 0}
            
            recent_operations = self.operation_history[-20:]  # Last 20 operations
            successful_operations = [op for op in recent_operations if op["success"]]
            failed_operations = [op for op in recent_operations if not op["success"]]
            
            success_rate = len(successful_operations) / len(recent_operations) if recent_operations else 0
            avg_duration = sum(op["duration"] for op in successful_operations) / len(successful_operations) if successful_operations else 0
            
            return {
                "status": "healthy" if success_rate >= 0.9 else "degraded" if success_rate >= 0.7 else "unhealthy",
                "success_rate": success_rate,
                "avg_operation_time": avg_duration,
                "total_operations": len(self.operation_history),
                "recent_failures": len(failed_operations),
                "last_operation": self.operation_history[-1]["timestamp"] if self.operation_history else None
            }

class VaultClient:
    """Oracle Cloud Vault client with advanced features."""
    
    def __init__(self, config: Optional[VaultConfig] = None):
        self.config = config or VaultConfig()
        self.logger = get_database_logger(__name__)
        self.cache = SecretCache(self.config) if self.config.enable_caching else None
        self.health_monitor = VaultHealthMonitor()
        self._client = None
        self._client_lock = threading.Lock()
    
    def _initialize_client(self) -> bool:
        """Initialize the OCI secrets client with error handling."""
        try:
            with performance_monitor("vault_client_initialization"):
                with self._client_lock:
                    if self._client is not None:
                        return True
                    
                    # Initialize signer
                    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
                    
                    # Create client with configuration
                    client_config = {
                        "timeout": self.config.connection_timeout * 1000,  # Convert to milliseconds
                        "retry_strategy": oci.retry.DEFAULT_RETRY_STRATEGY
                    }
                    
                    # Use SecretsClient instead of VaultsClient
                    self._client = oci.secrets.SecretsClient(config=client_config, signer=signer)
                    self.logger.info("OCI Secrets client initialized successfully")
                    return True
                    
        except Exception as e:
            self.logger.error(f"Failed to initialize OCI Secrets client: {e}")
            return False
    
    def _validate_secret_id(self, secret_id: str) -> bool:
        """Validate secret ID format."""
        if not secret_id or not isinstance(secret_id, str):
            return False
        
        # Basic OCID format validation
        if not secret_id.startswith("ocid1."):
            return False
        
        return True
    
    def _fetch_secret_from_vault(self, secret_id: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """Fetch secret from Oracle Cloud Vault with error handling."""
        start_time = time.time()
        success = False
        value = None
        error_message = None
        
        try:
            with performance_monitor("vault_secret_fetch"):
                # Initialize client if needed
                if not self._initialize_client():
                    raise RuntimeError("Failed to initialize OCI client")
                
                # Validate secret ID
                if not self._validate_secret_id(secret_id):
                    raise ValueError(f"Invalid secret ID format: {secret_id}")
                
                # Fetch secret bundle using the working approach
                self.logger.debug(f"Fetching secret from vault: {secret_id}")
                secret_bundle = self._client.get_secret_bundle(secret_id=secret_id).data
                
                # Decode secret content using the working approach
                decoded_secret = base64.b64decode(
                    secret_bundle.secret_bundle_content.content.encode("utf-8")
                ).decode("utf-8")
                
                value = decoded_secret
                success = True
                self.logger.debug(f"Successfully fetched secret: {secret_id}")
                
        except Exception as e:
            error_message = str(e)
            self.logger.error(f"Failed to fetch secret {secret_id}: {e}")
            if hasattr(e, '__traceback__'):
                self.logger.debug(f"Traceback: {traceback.format_exc()}")
        
        duration = time.time() - start_time
        
        # Record operation for health monitoring
        self.health_monitor.record_operation(
            "fetch_secret", secret_id, success, duration, error_message
        )
        
        return success, value, error_message
    
    def _fetch_secret_with_retry(self, secret_id: str) -> str:
        """Fetch secret with retry logic and exponential backoff."""
        last_error = None
        
        for attempt in range(1, self.config.retry_attempts + 1):
            self.logger.debug(f"Secret fetch attempt {attempt}/{self.config.retry_attempts} for {secret_id}")
            
            success, value, error_message = self._fetch_secret_from_vault(secret_id)
            
            if success and value is not None:
                return value
            
            last_error = error_message
            
            # Calculate delay for next attempt
            if attempt < self.config.retry_attempts:
                delay = min(self.config.retry_delay * (2 ** (attempt - 1)), self.config.max_retry_delay)
                self.logger.info(f"Retrying secret fetch in {delay:.1f} seconds...")
                time.sleep(delay)
        
        raise RuntimeError(f"Failed to fetch secret after {self.config.retry_attempts} attempts: {last_error}")
    
    def get_secret(self, secret_id: str, use_cache: bool = True) -> str:
        """
        Fetch a secret from Oracle Cloud Vault.
        
        This method provides comprehensive secret management with caching, retry logic,
        performance monitoring, and detailed error handling.
        
        Args:
            secret_id (str): The OCID of the secret to fetch
            use_cache (bool): Whether to use caching (if enabled)
            
        Returns:
            str: The decoded secret value
            
        Raises:
            ValueError: If secret_id is invalid
            RuntimeError: If secret fetch fails after retries
            ConnectionError: If vault connection fails
            
        Example:
            >>> vault_client = VaultClient()
            >>> secret = vault_client.get_secret("ocid1.vaultsecret.xxx")
            >>> print(f"Secret retrieved: {secret[:10]}...")
        """
        try:
            with performance_monitor("get_secret"):
                # Validate input
                if not secret_id:
                    raise ValueError("Secret ID cannot be empty")
                
                # Check cache first
                if use_cache and self.cache:
                    cached_value = self.cache.get(secret_id)
                    if cached_value is not None:
                        self.logger.debug(f"Cache hit for secret: {secret_id}")
                        return cached_value
                
                # Fetch from vault
                self.logger.info(f"Fetching secret from vault: {secret_id}")
                value = self._fetch_secret_with_retry(secret_id)
                
                # Cache the result
                if use_cache and self.cache:
                    self.cache.set(secret_id, value)
                
                # Audit trail
                if self.config.enable_audit_trail:
                    self.logger.info(f"AUDIT: Secret accessed - ID: {secret_id}, Length: {len(value)}")
                
                return value
                
        except Exception as e:
            self.logger.error(f"Failed to get secret {secret_id}: {e}")
            raise
    
    def invalidate_cache(self, secret_id: Optional[str] = None):
        """
        Invalidate cache entries.
        
        Args:
            secret_id (Optional[str]): Specific secret ID to invalidate, or None for all
        """
        if self.cache:
            if secret_id:
                self.cache.invalidate(secret_id)
                self.logger.info(f"Invalidated cache for secret: {secret_id}")
            else:
                self.cache.clear()
                self.logger.info("Invalidated all cache entries")
    
    def get_cache_stats(self) -> Optional[Dict[str, Any]]:
        """Get cache statistics if caching is enabled."""
        if self.cache:
            return self.cache.get_stats()
        return None
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get health status of vault operations."""
        return self.health_monitor.get_health_metrics()

# Global vault client instance
_vault_client: Optional[VaultClient] = None
_client_lock = threading.Lock()

def get_vault_client(config: Optional[VaultConfig] = None) -> VaultClient:
    """
    Get or create a global vault client instance.
    
    Args:
        config (Optional[VaultConfig]): Configuration for the vault client
        
    Returns:
        VaultClient: Configured vault client instance
    """
    global _vault_client
    
    with _client_lock:
        if _vault_client is None:
            _vault_client = VaultClient(config)
        return _vault_client

def get_secret(secret_id: str, use_cache: bool = True) -> str:
    """
    Fetch a secret from Oracle Cloud Vault using the global client.
    
    This is the main entry point for secret retrieval, providing backward
    compatibility with the original implementation.
    
    Args:
        secret_id (str): The OCID of the secret to fetch
        use_cache (bool): Whether to use caching (if enabled)
        
    Returns:
        str: The decoded secret value
        
    Raises:
        ValueError: If secret_id is invalid
        RuntimeError: If secret fetch fails after retries
        
    Example:
        >>> secret = get_secret("ocid1.vaultsecret.xxx")
        >>> print(f"Secret retrieved: {secret[:10]}...")
    """
    client = get_vault_client()
    return client.get_secret(secret_id, use_cache)

def invalidate_secret_cache(secret_id: Optional[str] = None):
    """
    Invalidate secret cache entries.
    
    Args:
        secret_id (Optional[str]): Specific secret ID to invalidate, or None for all
    """
    client = get_vault_client()
    client.invalidate_cache(secret_id)

def get_vault_health_status() -> Dict[str, Any]:
    """Get health status of vault operations."""
    client = get_vault_client()
    return client.get_health_status()

def get_cache_statistics() -> Optional[Dict[str, Any]]:
    """Get cache statistics if caching is enabled."""
    client = get_vault_client()
    return client.get_cache_stats()