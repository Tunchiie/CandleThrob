"""
Centralized Logging System for CandleThrob
==========================================

This module provides a unified logging system that consolidates all logging
functionality across the CandleThrob system, replacing duplicate logging configs.
"""

import os
import sys
import json
import logging
import logging.handlers
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path

from .config import LoggingConfig
from .base import BaseAuditLogger


class StructuredFormatter(logging.Formatter):
    """JSON formatter for structured logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON."""
        log_data = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in {"name", "msg", "args", "levelname", "levelno", "pathname",
                          "filename", "module", "exc_info", "exc_text", "stack_info",
                          "lineno", "funcName", "created", "msecs", "relativeCreated",
                          "thread", "threadName", "processName", "process", "message"}:
                log_data["extra"] = log_data.get("extra", {})
                log_data["extra"][key] = value
        
        return json.dumps(log_data)


class CandleThrobLogger:
    """Centralized logger for CandleThrob system."""
    
    _instances: Dict[str, logging.Logger] = {}
    _config: Optional[LoggingConfig] = None
    _initialized = False
    
    @classmethod
    def initialize(cls, config: LoggingConfig) -> None:
        """Initialize the logging system."""
        cls._config = config
        cls._setup_root_logger()
        cls._initialized = True
    
    @classmethod
    def _setup_root_logger(cls) -> None:
        """Setup root logger configuration."""
        if not cls._config:
            cls._config = LoggingConfig()
        
        # Create log directory
        Path(cls._config.log_dir).mkdir(parents=True, exist_ok=True)
        
        # Clear existing handlers
        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        
        # Set log level
        root_logger.setLevel(getattr(logging, cls._config.level.upper()))
        
        # Console handler
        if cls._config.enable_console:
            console_handler = logging.StreamHandler(sys.stdout)
            if cls._config.enable_structured:
                console_handler.setFormatter(StructuredFormatter())
            else:
                console_handler.setFormatter(logging.Formatter(cls._config.format))
            root_logger.addHandler(console_handler)
        
        # File handler
        log_file = Path(cls._config.log_dir) / "candlethrob.log"
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=cls._config.max_file_size_mb * 1024 * 1024,
            backupCount=cls._config.backup_count
        )
        if cls._config.enable_structured:
            file_handler.setFormatter(StructuredFormatter())
        else:
            file_handler.setFormatter(logging.Formatter(cls._config.format))
        root_logger.addHandler(file_handler)
    
    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """Get logger instance for specified name."""
        if not cls._initialized:
            cls.initialize(LoggingConfig())
        
        if name not in cls._instances:
            logger = logging.getLogger(name)
            cls._instances[name] = logger
        
        return cls._instances[name]
    
    @classmethod
    def get_component_logger(cls, component: str, subcomponent: str = "") -> logging.Logger:
        """Get logger for a specific component."""
        name = f"candlethrob.{component}"
        if subcomponent:
            name += f".{subcomponent}"
        return cls.get_logger(name)


class IngestionLogger(BaseAuditLogger):
    """Specialized logger for ingestion operations."""
    
    def __init__(self, component_name: str):
        super().__init__(f"ingestion.{component_name}")
        self.logger = CandleThrobLogger.get_component_logger("ingestion", component_name)
    
    def log_start(self, operation: str, ticker: str, **kwargs) -> None:
        """Log start of ingestion operation."""
        details = {"ticker": ticker, **kwargs}
        self.log_operation(f"{operation}_start", details)
        self.logger.info(f"[{operation}] Starting processing for {ticker}", extra=details)
    
    def log_success(self, operation: str, ticker: str, record_count: int, **kwargs) -> None:
        """Log successful ingestion."""
        details = {"ticker": ticker, "record_count": record_count, **kwargs}
        self.log_operation(f"{operation}_success", details)
        self.logger.info(f"[{operation}] Successfully processed {ticker} - {record_count} records", extra=details)
    
    def log_error(self, operation: str, ticker: str, error: str, **kwargs) -> None:
        """Log ingestion error."""
        details = {"ticker": ticker, "error": error, **kwargs}
        self.log_operation(f"{operation}_error", details)
        self.logger.error(f"[{operation}] Error processing {ticker}: {error}", extra=details)
    
    def log_rate_limiting(self, operation: str, delay_seconds: float, **kwargs) -> None:
        """Log rate limiting event."""
        details = {"delay_seconds": delay_seconds, **kwargs}
        self.log_operation(f"{operation}_rate_limit", details)
        self.logger.info(f"[{operation}] Rate limiting - waiting {delay_seconds:.2f} seconds", extra=details)
    
    def log_batch_progress(self, operation: str, current: int, total: int, ticker: str, **kwargs) -> None:
        """Log batch processing progress."""
        details = {"current": current, "total": total, "ticker": ticker, "progress": current/total, **kwargs}
        self.log_operation(f"{operation}_progress", details)
        self.logger.info(f"[{operation}] Progress: {current}/{total} ({current/total:.1%}) - Processing {ticker}", extra=details)


class TransformLogger(BaseAuditLogger):
    """Specialized logger for transformation operations."""
    
    def __init__(self, component_name: str):
        super().__init__(f"transform.{component_name}")
        self.logger = CandleThrobLogger.get_component_logger("transform", component_name)
    
    def log_transform_start(self, ticker: str, indicators: list, **kwargs) -> None:
        """Log start of transformation."""
        details = {"ticker": ticker, "indicators": indicators, **kwargs}
        self.log_operation("transform_start", details)
        self.logger.info(f"Starting transformation for {ticker} with indicators: {indicators}", extra=details)
    
    def log_transform_success(self, ticker: str, record_count: int, indicators_added: int, **kwargs) -> None:
        """Log successful transformation."""
        details = {"ticker": ticker, "record_count": record_count, "indicators_added": indicators_added, **kwargs}
        self.log_operation("transform_success", details)
        self.logger.info(f"Successfully transformed {ticker} - {record_count} records, {indicators_added} indicators added", extra=details)
    
    def log_transform_error(self, ticker: str, error: str, **kwargs) -> None:
        """Log transformation error."""
        details = {"ticker": ticker, "error": error, **kwargs}
        self.log_operation("transform_error", details)
        self.logger.error(f"Error transforming {ticker}: {error}", extra=details)


# Convenience functions for backward compatibility
def get_ingestion_logger(name: str) -> IngestionLogger:
    """Get ingestion logger instance."""
    return IngestionLogger(name)

def get_transform_logger(name: str) -> TransformLogger:
    """Get transform logger instance."""
    return TransformLogger(name)

def get_logger(name: str) -> logging.Logger:
    """Get standard logger instance."""
    return CandleThrobLogger.get_logger(name)

# Legacy function aliases for backward compatibility
def log_ingestion_start(operation: str, ticker: str, **kwargs) -> None:
    """Legacy function for logging ingestion start."""
    logger = get_ingestion_logger(operation)
    logger.log_start(operation, ticker, **kwargs)

def log_ingestion_success(operation: str, ticker: str, record_count: int, **kwargs) -> None:
    """Legacy function for logging ingestion success."""
    logger = get_ingestion_logger(operation)
    logger.log_success(operation, ticker, record_count, **kwargs)

def log_ingestion_error(operation: str, ticker: str, error: str, **kwargs) -> None:
    """Legacy function for logging ingestion error."""
    logger = get_ingestion_logger(operation)
    logger.log_error(operation, ticker, error, **kwargs)

def log_rate_limiting(operation: str, delay_seconds: float, **kwargs) -> None:
    """Legacy function for logging rate limiting."""
    logger = get_ingestion_logger(operation)
    logger.log_rate_limiting(operation, delay_seconds, **kwargs)

def log_batch_progress(operation: str, current: int, total: int, ticker: str, **kwargs) -> None:
    """Legacy function for logging batch progress."""
    logger = get_ingestion_logger(operation)
    logger.log_batch_progress(operation, current, total, ticker, **kwargs)