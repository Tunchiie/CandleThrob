"""
Utility modules for CandleThrob

This module provides specialized utilities and integrations for external services.
Common functionality has been moved to the core module to reduce duplication.

Modules:
- connections: Centralized connection management
- gcs: Google Cloud Storage operations
- vault: OCI Vault integration
- oracle_conn: Oracle database connectivity
- models: Data models and schemas
"""

from .connections import (
    DatabaseConnectionManager,
    StorageConnectionManager, 
    APIConnectionManager,
    ConnectionRegistry,
    create_connection_registry
)

# Legacy imports for backward compatibility
from . import models
from . import oracle_conn  
from . import gcs
from . import vault

# Re-export core components for backward compatibility
from ..core.config import get_config
from ..core.logging import get_logger, get_ingestion_logger, get_transform_logger
from ..core.validators import UniversalDataValidator, create_validator
from ..core.monitors import UniversalPerformanceMonitor, HealthMonitor, ResourceMonitor

__all__ = [
    # Connection management
    "DatabaseConnectionManager",
    "StorageConnectionManager",
    "APIConnectionManager", 
    "ConnectionRegistry",
    "create_connection_registry",
    
    # Legacy modules
    "models",
    "oracle_conn", 
    "gcs",
    "vault",
    
    # Core components (backward compatibility)
    "get_config",
    "get_logger",
    "get_ingestion_logger",
    "get_transform_logger",
    "UniversalDataValidator",
    "create_validator",
    "UniversalPerformanceMonitor",
    "HealthMonitor",
    "ResourceMonitor",
]
