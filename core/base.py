"""
Base Classes for CandleThrob Components
======================================

This module provides base classes that are shared across the CandleThrob system
to ensure consistency and reduce code duplication.
"""

import time
import psutil
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional, List, ContextManager
from contextlib import contextmanager

from .config import BaseConfig


@dataclass
class OperationMetrics:
    """Metrics for monitoring operations."""
    
    operation_name: str
    start_time: float
    end_time: Optional[float] = None
    duration: Optional[float] = None
    memory_usage_mb: Optional[float] = None
    success: bool = True
    error_message: Optional[str] = None
    additional_data: Dict[str, Any] = field(default_factory=dict)
    
    def finish(self, success: bool = True, error_message: Optional[str] = None) -> None:
        """Mark operation as finished."""
        self.end_time = time.time()
        self.duration = self.end_time - self.start_time
        self.memory_usage_mb = psutil.Process().memory_info().rss / 1024 / 1024
        self.success = success
        self.error_message = error_message


class BasePerformanceMonitor:
    """Base performance monitoring class."""
    
    def __init__(self):
        self.metrics: Dict[str, OperationMetrics] = {}
        self._lock = threading.Lock()
    
    @contextmanager
    def monitor_operation(self, operation_name: str) -> ContextManager[OperationMetrics]:
        """Context manager for monitoring operations."""
        metrics = OperationMetrics(
            operation_name=operation_name,
            start_time=time.time()
        )
        
        try:
            yield metrics
            metrics.finish(success=True)
        except Exception as e:
            metrics.finish(success=False, error_message=str(e))
            raise
        finally:
            with self._lock:
                self.metrics[operation_name] = metrics
    
    def get_metrics(self, operation_name: Optional[str] = None) -> Dict[str, Any]:
        """Get performance metrics."""
        with self._lock:
            if operation_name:
                return self.metrics.get(operation_name, {})
            return {name: vars(metrics) for name, metrics in self.metrics.items()}
    
    def clear_metrics(self) -> None:
        """Clear all metrics."""
        with self._lock:
            self.metrics.clear()


@dataclass
class ValidationResult:
    """Result of data validation."""
    
    is_valid: bool
    quality_score: float
    issues: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class BaseDataValidator(ABC):
    """Base class for data validators."""
    
    def __init__(self, config: BaseConfig):
        self.config = config
    
    @abstractmethod
    def validate(self, data: Any) -> ValidationResult:
        """Validate data and return validation result."""
        pass
    
    def _check_null_ratio(self, data, max_ratio: float = 0.1) -> ValidationResult:
        """Check for excessive null values."""
        if hasattr(data, 'isnull'):
            null_ratio = data.isnull().sum().sum() / (len(data) * len(data.columns))
            if null_ratio > max_ratio:
                return ValidationResult(
                    is_valid=False,
                    quality_score=1.0 - null_ratio,
                    issues=[f"High null ratio: {null_ratio:.2%} > {max_ratio:.2%}"]
                )
        return ValidationResult(is_valid=True, quality_score=1.0)


class BaseRateLimiter:
    """Base rate limiter with exponential backoff."""
    
    def __init__(self, calls_per_minute: int = 5):
        self.calls_per_minute = calls_per_minute
        self.seconds_between_calls = 60.0 / calls_per_minute
        self.last_call_time = 0.0
        self.consecutive_failures = 0
        self.adaptive_delay = self.seconds_between_calls
        self._lock = threading.Lock()
    
    def wait_if_needed(self) -> None:
        """Wait if necessary to respect rate limits."""
        with self._lock:
            current_time = time.time()
            time_since_last_call = current_time - self.last_call_time
            
            effective_delay = self.adaptive_delay if self.consecutive_failures > 0 else self.seconds_between_calls
            
            if time_since_last_call < effective_delay:
                sleep_time = effective_delay - time_since_last_call
                time.sleep(sleep_time)
            
            self.last_call_time = time.time()
    
    def record_failure(self) -> None:
        """Record a rate limit failure."""
        with self._lock:
            self.consecutive_failures += 1
            self.adaptive_delay = min(self.adaptive_delay * 1.5, 60.0)
    
    def record_success(self) -> None:
        """Record a successful call."""
        with self._lock:
            self.consecutive_failures = 0
            self.adaptive_delay = self.seconds_between_calls


class BaseProcessor(ABC):
    """Base class for data processors."""
    
    def __init__(self, config: BaseConfig):
        self.config = config
        self.performance_monitor = BasePerformanceMonitor()
        self.validator = self._create_validator()
        self.rate_limiter = self._create_rate_limiter()
    
    @abstractmethod
    def _create_validator(self) -> BaseDataValidator:
        """Create validator instance. Override in subclasses."""
        pass
    
    @abstractmethod
    def _create_rate_limiter(self) -> BaseRateLimiter:
        """Create rate limiter instance. Override in subclasses."""
        pass
    
    @abstractmethod
    def process(self, *args, **kwargs) -> Any:
        """Main processing method. Override in subclasses."""
        pass
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics."""
        return self.performance_monitor.get_metrics()


class BaseAuditLogger:
    """Base audit logging functionality."""
    
    def __init__(self, logger_name: str):
        self.logger_name = logger_name
        self.audit_entries: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
    
    def log_operation(self, operation: str, details: Dict[str, Any]) -> None:
        """Log an operation for audit purposes."""
        entry = {
            "timestamp": datetime.now().isoformat(),
            "operation": operation,
            "details": details,
            "logger": self.logger_name
        }
        
        with self._lock:
            self.audit_entries.append(entry)
    
    def get_audit_trail(self, operation: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get audit trail entries."""
        with self._lock:
            if operation:
                return [entry for entry in self.audit_entries if entry["operation"] == operation]
            return self.audit_entries.copy()
    
    def clear_audit_trail(self) -> None:
        """Clear audit trail."""
        with self._lock:
            self.audit_entries.clear()


# Utility functions for common operations
def retry_on_failure(max_retries: int = 3, delay_seconds: float = 1.0):
    """Decorator for retrying operations with exponential backoff."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = delay_seconds * (2 ** attempt)
                        time.sleep(delay)
            
            raise last_exception
        return wrapper
    return decorator


def log_operation_metrics(func):
    """Decorator to automatically log operation metrics."""
    def wrapper(self, *args, **kwargs):
        if hasattr(self, 'performance_monitor'):
            with self.performance_monitor.monitor_operation(func.__name__):
                return func(self, *args, **kwargs)
        return func(self, *args, **kwargs)
    return wrapper