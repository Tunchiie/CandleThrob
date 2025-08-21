"""
Centralized Configuration Management for CandleThrob
===================================================

This module provides a unified configuration system for all CandleThrob components.
All configuration classes inherit from a common base to ensure consistency.
"""

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from pathlib import Path


@dataclass
class BaseConfig(ABC):
    """Base configuration class for all CandleThrob components."""
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        self.validate()
    
    @abstractmethod
    def validate(self) -> None:
        """Validate configuration parameters. Override in subclasses."""
        pass
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}
    
    @classmethod
    def from_env(cls, prefix: str = "") -> 'BaseConfig':
        """Create configuration from environment variables."""
        kwargs = {}
        for field_name in cls.__dataclass_fields__:
            env_name = f"{prefix}{field_name.upper()}" if prefix else field_name.upper()
            if env_name in os.environ:
                field_type = cls.__dataclass_fields__[field_name].type
                value = os.environ[env_name]
                
                # Type conversion
                if field_type == bool:
                    kwargs[field_name] = value.lower() in ('true', '1', 'yes', 'on')
                elif field_type == int:
                    kwargs[field_name] = int(value)
                elif field_type == float:
                    kwargs[field_name] = float(value)
                else:
                    kwargs[field_name] = value
        
        return cls(**kwargs)


@dataclass
class DatabaseConfig(BaseConfig):
    """Database configuration settings."""
    
    host: str = "localhost"
    port: int = 1521
    service_name: str = "XEPDB1"
    username: str = ""
    password: str = ""
    pool_size: int = 10
    pool_timeout: int = 30
    connection_timeout: int = 60
    
    def validate(self) -> None:
        """Validate database configuration."""
        if not self.username:
            raise ValueError("Database username is required")
        if not self.password:
            raise ValueError("Database password is required")
        if self.port <= 0 or self.port > 65535:
            raise ValueError("Port must be between 1 and 65535")
        if self.pool_size <= 0:
            raise ValueError("Pool size must be positive")


@dataclass
class StorageConfig(BaseConfig):
    """Storage configuration for GCS and local storage."""
    
    gcs_bucket: str = "candlethrob-wandata"
    gcs_project_id: str = ""
    gcs_credentials_path: str = ""
    local_data_dir: str = "./data"
    cache_size_mb: int = 1024
    cache_ttl_seconds: int = 3600
    
    def validate(self) -> None:
        """Validate storage configuration."""
        if not self.gcs_bucket:
            raise ValueError("GCS bucket name is required")
        
        # Create local data directory if it doesn't exist
        Path(self.local_data_dir).mkdir(parents=True, exist_ok=True)


@dataclass
class APIConfig(BaseConfig):
    """API configuration for external data sources."""
    
    polygon_api_key: str = ""
    fred_api_key: str = ""
    max_requests_per_minute: int = 5
    max_requests_per_hour: int = 100
    request_timeout: int = 30
    max_retries: int = 3
    retry_delay_base: float = 1.0
    
    def validate(self) -> None:
        """Validate API configuration."""
        if not self.polygon_api_key:
            raise ValueError("Polygon API key is required")
        if not self.fred_api_key:
            raise ValueError("FRED API key is required")
        if self.max_requests_per_minute <= 0:
            raise ValueError("Max requests per minute must be positive")
        if self.max_retries < 0:
            raise ValueError("Max retries must be non-negative")


@dataclass
class ProcessingConfig(BaseConfig):
    """Processing configuration for data ingestion and transformation."""
    
    batch_size: int = 25
    parallel_processing: bool = True
    max_workers: int = 4
    chunk_size: int = 1000
    memory_limit_mb: float = 2048.0
    processing_timeout: float = 3600.0
    enable_monitoring: bool = True
    enable_audit_trail: bool = True
    
    def validate(self) -> None:
        """Validate processing configuration."""
        if self.batch_size <= 0:
            raise ValueError("Batch size must be positive")
        if self.max_workers <= 0:
            raise ValueError("Max workers must be positive")
        if self.memory_limit_mb <= 0:
            raise ValueError("Memory limit must be positive")


@dataclass
class QualityConfig(BaseConfig):
    """Data quality configuration."""
    
    min_quality_score: float = 0.8
    max_null_ratio: float = 0.1
    max_outlier_ratio: float = 0.05
    enable_validation: bool = True
    enable_quality_gates: bool = True
    validation_sample_size: int = 1000
    
    def validate(self) -> None:
        """Validate quality configuration."""
        if not 0.0 <= self.min_quality_score <= 1.0:
            raise ValueError("Min quality score must be between 0.0 and 1.0")
        if not 0.0 <= self.max_null_ratio <= 1.0:
            raise ValueError("Max null ratio must be between 0.0 and 1.0")
        if not 0.0 <= self.max_outlier_ratio <= 1.0:
            raise ValueError("Max outlier ratio must be between 0.0 and 1.0")


@dataclass
class LoggingConfig(BaseConfig):
    """Logging configuration."""
    
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_dir: str = "./logs"
    max_file_size_mb: int = 100
    backup_count: int = 5
    enable_console: bool = True
    enable_structured: bool = True
    
    def validate(self) -> None:
        """Validate logging configuration."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.level.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of: {valid_levels}")
        
        # Create log directory if it doesn't exist
        Path(self.log_dir).mkdir(parents=True, exist_ok=True)


@dataclass
class CandleThrobConfig(BaseConfig):
    """Main configuration class that combines all component configurations."""
    
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    api: APIConfig = field(default_factory=APIConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    quality: QualityConfig = field(default_factory=QualityConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    
    # Global settings
    environment: str = "development"
    debug: bool = False
    
    def validate(self) -> None:
        """Validate all configuration components."""
        self.database.validate()
        self.storage.validate()
        self.api.validate()
        self.processing.validate()
        self.quality.validate()
        self.logging.validate()
        
        if self.environment not in ["development", "staging", "production"]:
            raise ValueError("Environment must be one of: development, staging, production")
    
    @classmethod
    def from_env(cls, prefix: str = "CANDLETHROB_") -> 'CandleThrobConfig':
        """Create configuration from environment variables."""
        config = cls()
        
        # Load component configurations
        config.database = DatabaseConfig.from_env(f"{prefix}DB_")
        config.storage = StorageConfig.from_env(f"{prefix}STORAGE_")
        config.api = APIConfig.from_env(f"{prefix}API_")
        config.processing = ProcessingConfig.from_env(f"{prefix}PROCESSING_")
        config.quality = QualityConfig.from_env(f"{prefix}QUALITY_")
        config.logging = LoggingConfig.from_env(f"{prefix}LOGGING_")
        
        # Load global settings
        if f"{prefix}ENVIRONMENT" in os.environ:
            config.environment = os.environ[f"{prefix}ENVIRONMENT"]
        if f"{prefix}DEBUG" in os.environ:
            config.debug = os.environ[f"{prefix}DEBUG"].lower() in ('true', '1', 'yes', 'on')
        
        return config


# Default configuration instance (lazy initialization)
default_config = None

# Environment-based configuration
def get_config() -> CandleThrobConfig:
    """Get configuration instance, preferring environment variables."""
    global default_config
    try:
        return CandleThrobConfig.from_env()
    except Exception:
        if default_config is None:
            # Create a minimal default config when environment isn't available
            default_config = CandleThrobConfig(
                db_username="default",
                db_password="default", 
                db_dsn="default",
                polygon_api_key="default"
            )
        return default_config