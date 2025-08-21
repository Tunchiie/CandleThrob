"""
Core module for CandleThrob - shared components and base classes.

This module provides centralized, reusable components that eliminate
code duplication across the CandleThrob system.

Modules:
- config: Centralized configuration management
- base: Base classes and utilities
- logging: Unified logging system
- validators: Consolidated data validation
- monitors: Performance and health monitoring
"""

from .config import get_config, CandleThrobConfig
from .logging import CandleThrobLogger, get_logger, get_ingestion_logger, get_transform_logger
from .validators import UniversalDataValidator, create_validator
from .monitors import UniversalPerformanceMonitor, HealthMonitor, ResourceMonitor
from .base import BaseProcessor, BaseDataValidator, BaseRateLimiter

__all__ = [
    # Configuration
    "get_config",
    "CandleThrobConfig",
    
    # Logging
    "CandleThrobLogger",
    "get_logger",
    "get_ingestion_logger", 
    "get_transform_logger",
    
    # Validation
    "UniversalDataValidator",
    "create_validator",
    
    # Monitoring
    "UniversalPerformanceMonitor",
    "HealthMonitor",
    "ResourceMonitor",
    
    # Base classes
    "BaseProcessor",
    "BaseDataValidator", 
    "BaseRateLimiter",
]