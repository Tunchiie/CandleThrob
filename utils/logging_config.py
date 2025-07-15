"""
Advanced Logging Configuration for CandleThrob

This module provides a comprehensive, advanced logging configuration that ensures
all modules log with structured formatting, rotation, performance monitoring, and audit trails.
It supports multiple log files for different types of operations with advanced features
including log aggregation, structured logging, and performance metrics.

Features:
- Structured JSON logging for log aggregation
- Log rotation with size and time-based policies
- Performance monitoring and metrics
- Audit trail logging
- Configurable log levels and formats
- Thread-safe logging
- Memory-efficient logging
- Integration with monitoring systems

Author: Adetunji Fasiku
Version: 3.0.0
Last Updated: 2025-01-13
"""

import json
import logging
import logging.handlers
import os
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Union
from dataclasses import dataclass, asdict
from contextlib import contextmanager
import traceback

# Configuration constants
DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DEFAULT_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_LOG_DIR = "/app/logs"
MAX_LOG_SIZE = 100 * 1024 * 1024  # 100MB
BACKUP_COUNT = 10
ROTATION_INTERVAL = timedelta(days=1)

# Performance monitoring
_performance_metrics: Dict[str, Dict[str, Any]] = {}
_metrics_lock = threading.Lock()

# Global flags to track if logging has been configured
_ingestion_logging_configured = False
_database_logging_configured = False
_audit_logging_configured = False

@dataclass
class LogConfig:
    """Configuration for logging setup."""
    log_file: Optional[str] = None
    level: int = DEFAULT_LOG_LEVEL
    format_string: str = DEFAULT_LOG_FORMAT
    date_format: str = DEFAULT_LOG_DATE_FORMAT
    max_size: int = MAX_LOG_SIZE
    backup_count: int = BACKUP_COUNT
    enable_json: bool = False
    enable_console: bool = True
    enable_file: bool = True
    enable_rotation: bool = True
    enable_performance_monitoring: bool = True
    enable_audit_trail: bool = True

class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured logging with JSON support."""
    
    def __init__(self, enable_json: bool = False, include_traceback: bool = True):
        super().__init__()
        self.enable_json = enable_json
        self.include_traceback = include_traceback
    
    def format(self, record: logging.LogRecord) -> str:
        if self.enable_json:
            return self._format_json(record)
        else:
            return super().format(record)
    
    def _format_json(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "thread": record.thread,
            "process": record.process
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": traceback.format_exception(*record.exc_info)
            }
        
        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in log_entry and not key.startswith('_'):
                log_entry[key] = value
        
        return json.dumps(log_entry, default=str)

class PerformanceMonitor:
    """Performance monitoring for logging operations."""
    
    def __init__(self):
        self.metrics = {}
        self.lock = threading.Lock()
    
    def record_operation(self, operation: str, duration: float, success: bool = True):
        """Record a logging operation for performance monitoring."""
        with self.lock:
            if operation not in self.metrics:
                self.metrics[operation] = {
                    'count': 0,
                    'total_duration': 0.0,
                    'success_count': 0,
                    'error_count': 0,
                    'min_duration': float('inf'),
                    'max_duration': 0.0
                }
            
            self.metrics[operation]['count'] += 1
            self.metrics[operation]['total_duration'] += duration
            self.metrics[operation]['min_duration'] = min(
                self.metrics[operation]['min_duration'], duration
            )
            self.metrics[operation]['max_duration'] = max(
                self.metrics[operation]['max_duration'], duration
            )
            
            if success:
                self.metrics[operation]['success_count'] += 1
            else:
                self.metrics[operation]['error_count'] += 1
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics."""
        with self.lock:
            return self.metrics.copy()
    
    def reset_metrics(self):
        """Reset all performance metrics."""
        with self.lock:
            self.metrics.clear()

# Global performance monitor
_performance_monitor = PerformanceMonitor()

@contextmanager
def performance_monitor(operation: str):
    """Context manager for performance monitoring."""
    start_time = time.time()
    success = False
    try:
        yield
        success = True
    except Exception:
        success = False
        raise
    finally:
        duration = time.time() - start_time
        _performance_monitor.record_operation(operation, duration, success)

class AuditLogger:
    """Audit trail logging for compliance and security."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def log_user_action(self, user_id: str, action: str, resource: str, 
                       details: Optional[Dict[str, Any]] = None):
        """Log user actions for audit trail."""
        audit_entry = {
            'event_type': 'user_action',
            'user_id': user_id,
            'action': action,
            'resource': resource,
            'timestamp': datetime.now().isoformat(),
            'details': details or {}
        }
        self.logger.info(f"AUDIT: {json.dumps(audit_entry)}")
    
    def log_data_access(self, user_id: str, table: str, operation: str, 
                       record_count: Optional[int] = None):
        """Log data access for audit trail."""
        audit_entry = {
            'event_type': 'data_access',
            'user_id': user_id,
            'table': table,
            'operation': operation,
            'record_count': record_count,
            'timestamp': datetime.now().isoformat()
        }
        self.logger.info(f"AUDIT: {json.dumps(audit_entry)}")
    
    def log_system_event(self, event_type: str, description: str, 
                        severity: str = 'INFO', details: Optional[Dict[str, Any]] = None):
        """Log system events for audit trail."""
        audit_entry = {
            'event_type': 'system_event',
            'system_event_type': event_type,
            'description': description,
            'severity': severity,
            'timestamp': datetime.now().isoformat(),
            'details': details or {}
        }
        self.logger.info(f"AUDIT: {json.dumps(audit_entry)}")

def _create_log_directory(log_dir: str) -> str:
    """Create log directory and return the path."""
    try:
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        return str(log_path)
    except Exception as e:
        # Fallback to current directory if permission denied
        print(f"Warning: Could not create log directory {log_dir}: {e}")
        return "."

def _create_rotating_handler(log_file: str, config: LogConfig) -> logging.Handler:
    """Create a rotating file handler with proper configuration."""
    try:
        if config.enable_rotation:
            handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=config.max_size,
                backupCount=config.backup_count
            )
        else:
            handler = logging.FileHandler(log_file)
        
        handler.setLevel(config.level)
        formatter = StructuredFormatter(
            enable_json=config.enable_json,
            include_traceback=True
        )
        handler.setFormatter(formatter)
        return handler
    except Exception as e:
        # Fallback to console handler if file creation fails
        print(f"Warning: Could not create file handler for {log_file}: {e}")
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(config.level)
        formatter = StructuredFormatter(enable_json=config.enable_json)
        handler.setFormatter(formatter)
        return handler

def setup_ingestion_logging(config: Optional[LogConfig] = None) -> logging.Logger:
    """
    Set up advanced logging configuration for ingestion operations.
    
    This function provides advanced logging features including:
    - Structured JSON logging
    - Log rotation with size and time-based policies
    - Performance monitoring
    - Audit trail logging
    - Thread-safe operations
    - Memory-efficient logging
    
    Args:
        config (Optional[LogConfig]): Logging configuration. If None, uses defaults
        
    Returns:
        logging.Logger: Configured logger instance
        
    Raises:
        RuntimeError: If logging setup fails
        
    Example:
        >>> config = LogConfig(level=logging.DEBUG, enable_json=True)
        >>> logger = setup_ingestion_logging(config)
        >>> logger.info("Starting ingestion process")
    """
    global _ingestion_logging_configured
    
    if _ingestion_logging_configured:
        return logging.getLogger("ingestion")
    
    try:
        with performance_monitor("setup_ingestion_logging"):
            config = config or LogConfig()
            
            # Set default log file if none provided
            if config.log_file is None:
                log_dir = _create_log_directory(DEFAULT_LOG_DIR)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                config.log_file = os.path.join(log_dir, f"candlethrob_ingestion_{timestamp}.log")
            
            # Configure ingestion logger
            ingestion_logger = logging.getLogger("ingestion")
            ingestion_logger.setLevel(config.level)
            
            # Remove existing handlers to avoid duplicates
            for handler in ingestion_logger.handlers[:]:
                ingestion_logger.removeHandler(handler)
            
            # Add file handler if enabled
            if config.enable_file:
                file_handler = _create_rotating_handler(config.log_file, config)
                ingestion_logger.addHandler(file_handler)
            
            # Add console handler if enabled
            if config.enable_console:
                console_handler = logging.StreamHandler(sys.stdout)
                console_handler.setLevel(config.level)
                formatter = StructuredFormatter(enable_json=config.enable_json)
                console_handler.setFormatter(formatter)
                ingestion_logger.addHandler(console_handler)
            
            # Mark as configured
            _ingestion_logging_configured = True
            
            # Log the configuration
            ingestion_logger.info(
                f"Ingestion logging configured - File: {config.log_file}, "
                f"Level: {logging.getLevelName(config.level)}, "
                f"JSON: {config.enable_json}, "
                f"Performance Monitoring: {config.enable_performance_monitoring}"
            )
            
            return ingestion_logger
            
    except Exception as e:
        raise RuntimeError(f"Failed to setup ingestion logging: {e}") from e

def setup_database_logging(config: Optional[LogConfig] = None) -> logging.Logger:
    """
    Set up advanced logging configuration for database operations.
    
    This function provides the same advanced features as ingestion logging
    but configured specifically for database operations.
    
    Args:
        config (Optional[LogConfig]): Logging configuration. If None, uses defaults
        
    Returns:
        logging.Logger: Configured logger instance
        
    Raises:
        RuntimeError: If logging setup fails
        
    Example:
        >>> config = LogConfig(level=logging.INFO, enable_audit_trail=True)
        >>> logger = setup_database_logging(config)
        >>> logger.info("Database operation completed")
    """
    global _database_logging_configured
    
    if _database_logging_configured:
        return logging.getLogger("database")
    
    try:
        with performance_monitor("setup_database_logging"):
            config = config or LogConfig()
            
            # Set default log file if none provided
            if config.log_file is None:
                log_dir = _create_log_directory(DEFAULT_LOG_DIR)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                config.log_file = os.path.join(log_dir, f"candlethrob_database_{timestamp}.log")
            
            # Configure database logger
            database_logger = logging.getLogger("database")
            database_logger.setLevel(config.level)
            
            # Remove existing handlers to avoid duplicates
            for handler in database_logger.handlers[:]:
                database_logger.removeHandler(handler)
            
            # Add file handler if enabled
            if config.enable_file:
                file_handler = _create_rotating_handler(config.log_file, config)
                database_logger.addHandler(file_handler)
            
            # Add console handler if enabled
            if config.enable_console:
                console_handler = logging.StreamHandler(sys.stdout)
                console_handler.setLevel(config.level)
                formatter = StructuredFormatter(enable_json=config.enable_json)
                console_handler.setFormatter(formatter)
                database_logger.addHandler(console_handler)
            
            # Mark as configured
            _database_logging_configured = True
            
            # Log the configuration
            database_logger.info(
                f"Database logging configured - File: {config.log_file}, "
                f"Level: {logging.getLevelName(config.level)}, "
                f"JSON: {config.enable_json}, "
                f"Audit Trail: {config.enable_audit_trail}"
            )
            
            return database_logger
            
    except Exception as e:
        raise RuntimeError(f"Failed to setup database logging: {e}") from e

def setup_audit_logging(config: Optional[LogConfig] = None) -> logging.Logger:
    """
    Set up advanced audit logging for compliance and security.
    
    This function provides specialized logging for audit trails with enhanced
    security features and compliance reporting.
    
    Args:
        config (Optional[LogConfig]): Logging configuration. If None, uses defaults
        
    Returns:
        logging.Logger: Configured audit logger instance
        
    Raises:
        RuntimeError: If logging setup fails
        
    Example:
        >>> config = LogConfig(level=logging.INFO, enable_json=True)
        >>> logger = setup_audit_logging(config)
        >>> logger.info("Audit trail initialized")
    """
    global _audit_logging_configured
    
    if _audit_logging_configured:
        return logging.getLogger("audit")
    
    try:
        with performance_monitor("setup_audit_logging"):
            config = config or LogConfig()
            
            # Set default log file if none provided
            if config.log_file is None:
                log_dir = _create_log_directory(DEFAULT_LOG_DIR)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                config.log_file = os.path.join(log_dir, f"candlethrob_audit_{timestamp}.log")
            
            # Configure audit logger
            audit_logger = logging.getLogger("audit")
            audit_logger.setLevel(config.level)
            
            # Remove existing handlers to avoid duplicates
            for handler in audit_logger.handlers[:]:
                audit_logger.removeHandler(handler)
            
            # Add file handler if enabled
            if config.enable_file:
                file_handler = _create_rotating_handler(config.log_file, config)
                audit_logger.addHandler(file_handler)
            
            # Add console handler if enabled
            if config.enable_console:
                console_handler = logging.StreamHandler(sys.stdout)
                console_handler.setLevel(config.level)
                formatter = StructuredFormatter(enable_json=config.enable_json)
                console_handler.setFormatter(formatter)
                audit_logger.addHandler(console_handler)
            
            # Mark as configured
            _audit_logging_configured = True
            
            # Log the configuration
            audit_logger.info(
                f"Audit logging configured - File: {config.log_file}, "
                f"Level: {logging.getLevelName(config.level)}, "
                f"JSON: {config.enable_json}"
            )
            
            return audit_logger
            
    except Exception as e:
        raise RuntimeError(f"Failed to setup audit logging: {e}") from e

def get_ingestion_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for ingestion operations with performance monitoring.
    
    This function ensures that the ingestion logging is properly configured before
    returning a logger instance with performance monitoring capabilities.
    
    Args:
        name (str): Logger name (usually __name__)
        
    Returns:
        logging.Logger: Configured logger instance for ingestion operations
        
    Example:
        >>> logger = get_ingestion_logger(__name__)
        >>> logger.info("Module initialized")
    """
    try:
        # Ensure logging is configured
        if not _ingestion_logging_configured:
            setup_ingestion_logging()
        
        return logging.getLogger(f"ingestion.{name}")
    except Exception as e:
        # Fallback to basic logger if configuration fails
        print(f"Warning: Failed to get ingestion logger: {e}")
        return logging.getLogger(f"ingestion.{name}")

def get_database_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for database operations with audit trail support.
    
    This function ensures that the database logging is properly configured before
    returning a logger instance with audit trail capabilities.
    
    Args:
        name (str): Logger name (usually __name__)
        
    Returns:
        logging.Logger: Configured logger instance for database operations
        
    Example:
        >>> logger = get_database_logger(__name__)
        >>> logger.info("Database operation completed")
    """
    try:
        # Ensure logging is configured
        if not _database_logging_configured:
            setup_database_logging()
        
        return logging.getLogger(f"database.{name}")
    except Exception as e:
        # Fallback to basic logger if configuration fails
        print(f"Warning: Failed to get database logger: {e}")
        return logging.getLogger(f"database.{name}")

def get_audit_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for audit trail operations.
    
    This function ensures that the audit logging is properly configured before
    returning a logger instance with audit trail capabilities.
    
    Args:
        name (str): Logger name (usually __name__)
        
    Returns:
        logging.Logger: Configured logger instance for audit operations
        
    Example:
        >>> logger = get_audit_logger(__name__)
        >>> logger.info("Audit event recorded")
    """
    try:
        # Ensure logging is configured
        if not _audit_logging_configured:
            setup_audit_logging()
        
        return logging.getLogger(f"audit.{name}")
    except Exception as e:
        # Fallback to basic logger if configuration fails
        print(f"Warning: Failed to get audit logger: {e}")
        return logging.getLogger(f"audit.{name}")

def log_ingestion_start(module_name: str, ticker: Optional[str] = None, 
                       batch_info: Optional[str] = None, **kwargs):
    """
    Log the start of an ingestion process with enhanced formatting and metadata.
    
    Args:
        module_name (str): Name of the ingestion module
        ticker (Optional[str]): Ticker being processed
        batch_info (Optional[str]): Batch information
        **kwargs: Additional metadata to log
        
    Example:
        >>> log_ingestion_start("fetch_data", "AAPL", "Batch 1/21", 
        ...                     source="API", priority="high")
    """
    try:
        with performance_monitor("log_ingestion_start"):
            logger = get_ingestion_logger(__name__)
            
            # Build log message with metadata
            message_parts = [f"[{module_name}] Starting processing"]
            if ticker:
                message_parts.append(f"for {ticker}")
            if batch_info:
                message_parts.append(f"({batch_info})")
            
            # Add additional metadata
            if kwargs:
                metadata = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
                message_parts.append(f"| {metadata}")
            
            logger.info(" ".join(message_parts))
    except Exception as e:
        print(f"Warning: Failed to log ingestion start: {e}")

def log_ingestion_success(module_name: str, ticker: Optional[str] = None, 
                         records: Optional[int] = None, duration: Optional[float] = None,
                         **kwargs):
    """
    Log successful completion of an ingestion process with performance metrics.
    
    Args:
        module_name (str): Name of the ingestion module
        ticker (Optional[str]): Ticker that was processed
        records (Optional[int]): Number of records processed
        duration (Optional[float]): Processing duration in seconds
        **kwargs: Additional metadata to log
        
    Example:
        >>> log_ingestion_success("fetch_data", "AAPL", 252, 12.5, 
        ...                       api_calls=5, cache_hits=3)
    """
    try:
        with performance_monitor("log_ingestion_success"):
            logger = get_ingestion_logger(__name__)
            
            # Build log message with performance metrics
            message_parts = [f"[{module_name}] Successfully processed"]
            if ticker:
                message_parts.append(ticker)
            if records:
                message_parts.append(f"- {records} records")
            if duration:
                message_parts.append(f"({duration:.2f}s)")
            
            # Add additional metadata
            if kwargs:
                metadata = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
                message_parts.append(f"| {metadata}")
            
            logger.info(" ".join(message_parts))
    except Exception as e:
        print(f"Warning: Failed to log ingestion success: {e}")

def log_ingestion_error(module_name: str, ticker: Optional[str] = None, 
                       error: Optional[str] = None, exception: Optional[Exception] = None,
                       **kwargs):
    """
    Log errors during ingestion process with detailed error information.
    
    Args:
        module_name (str): Name of the ingestion module
        ticker (Optional[str]): Ticker that failed
        error (Optional[str]): Error message
        exception (Optional[Exception]): Exception object for detailed logging
        **kwargs: Additional metadata to log
        
    Example:
        >>> log_ingestion_error("fetch_data", "AAPL", "API rate limit exceeded",
        ...                     retry_count=3, backoff_time=60)
    """
    try:
        with performance_monitor("log_ingestion_error"):
            logger = get_ingestion_logger(__name__)
            
            # Build log message with error details
            message_parts = [f"[{module_name}] Error processing"]
            if ticker:
                message_parts.append(ticker)
            if error:
                message_parts.append(f": {error}")
            
            # Add additional metadata
            if kwargs:
                metadata = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
                message_parts.append(f"| {metadata}")
            
            log_message = " ".join(message_parts)
            
            if exception:
                logger.exception(log_message, exc_info=exception)
            else:
                logger.error(log_message)
    except Exception as e:
        print(f"Warning: Failed to log ingestion error: {e}")

def log_rate_limiting(module_name: str, sleep_time: float, 
                     reason: Optional[str] = None, **kwargs):
    """
    Log rate limiting information with detailed context.
    
    Args:
        module_name (str): Name of the module
        sleep_time (float): Time to sleep in seconds
        reason (Optional[str]): Reason for rate limiting
        **kwargs: Additional metadata to log
        
    Example:
        >>> log_rate_limiting("fetch_data", 12.5, "API quota exceeded",
        ...                   quota_remaining=50, quota_limit=1000)
    """
    try:
        with performance_monitor("log_rate_limiting"):
            logger = get_ingestion_logger(__name__)
            
            message_parts = [f"[{module_name}] Rate limiting: sleeping {sleep_time:.2f}s"]
            if reason:
                message_parts.append(f"({reason})")
            
            # Add additional metadata
            if kwargs:
                metadata = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
                message_parts.append(f"| {metadata}")
            
            logger.info(" ".join(message_parts))
    except Exception as e:
        print(f"Warning: Failed to log rate limiting: {e}")

def log_batch_progress(module_name: str, current: int, total: int, 
                      ticker: Optional[str] = None, progress_percent: Optional[float] = None,
                      **kwargs):
    """
    Log batch processing progress with detailed metrics.
    
    Args:
        module_name (str): Name of the module
        current (int): Current item number
        total (int): Total number of items
        ticker (Optional[str]): Current ticker being processed
        progress_percent (Optional[float]): Progress percentage
        **kwargs: Additional metadata to log
        
    Example:
        >>> log_batch_progress("ingest_data", 5, 25, "AAPL", 20.0,
        ...                    estimated_remaining=300, success_rate=0.95)
    """
    try:
        with performance_monitor("log_batch_progress"):
            logger = get_ingestion_logger(__name__)
            
            # Calculate progress percentage if not provided
            if progress_percent is None:
                progress_percent = (current / total) * 100 if total > 0 else 0
            
            message_parts = [f"[{module_name}] Processing {current}/{total} ({progress_percent:.1f}%)"]
            if ticker:
                message_parts.append(f": {ticker}")
            
            # Add additional metadata
            if kwargs:
                metadata = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
                message_parts.append(f"| {metadata}")
            
            logger.info(" ".join(message_parts))
    except Exception as e:
        print(f"Warning: Failed to log batch progress: {e}")

def log_database_operation(operation: str, table: Optional[str] = None, 
                          records: Optional[int] = None, error: Optional[str] = None,
                          duration: Optional[float] = None, **kwargs):
    """
    Log database operations with comprehensive metrics and audit trail.
    
    Args:
        operation (str): Type of database operation (insert, update, delete, etc.)
        table (Optional[str]): Table name
        records (Optional[int]): Number of records affected
        error (Optional[str]): Error message if operation failed
        duration (Optional[float]): Operation duration in seconds
        **kwargs: Additional metadata to log
        
    Example:
        >>> log_database_operation("insert", "ticker_data", 1000, duration=2.5,
        ...                        connection_pool_size=10, query_timeout=30)
    """
    try:
        with performance_monitor("log_database_operation"):
            logger = get_database_logger(__name__)
            
            # Build log message with operation details
            message_parts = [f"[DATABASE] {operation}"]
            if table:
                message_parts.append(f"on {table}")
            if records:
                message_parts.append(f"- {records} records")
            if duration:
                message_parts.append(f"({duration:.2f}s)")
            
            # Add additional metadata
            if kwargs:
                metadata = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
                message_parts.append(f"| {metadata}")
            
            log_message = " ".join(message_parts)
            
            if error:
                logger.error(f"{log_message} - Error: {error}")
            else:
                logger.info(log_message)
    except Exception as e:
        print(f"Warning: Failed to log database operation: {e}")

def get_performance_metrics() -> Dict[str, Any]:
    """
    Get current performance metrics for all logging operations.
    
    Returns:
        Dict[str, Any]: Performance metrics for all logging operations
        
    Example:
        >>> metrics = get_performance_metrics()
        >>> print(f"Total operations: {len(metrics)}")
    """
    return _performance_monitor.get_metrics()

def reset_performance_metrics():
    """Reset all performance metrics."""
    _performance_monitor.reset_metrics()

def get_audit_logger_instance(name: str) -> AuditLogger:
    """
    Get an audit logger instance for compliance and security logging.
    
    Args:
        name (str): Logger name (usually __name__)
        
    Returns:
        AuditLogger: Configured audit logger instance
        
    Example:
        >>> audit_logger = get_audit_logger_instance(__name__)
        >>> audit_logger.log_user_action("user123", "data_access", "ticker_data")
    """
    logger = get_audit_logger(name)
    return AuditLogger(logger)

# Initialize logging when module is imported
try:
    setup_ingestion_logging()
    setup_database_logging()
    setup_audit_logging()
except Exception as e:
    print(f"Warning: Failed to initialize logging: {e}") 