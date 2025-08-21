"""
Centralized Monitoring Components for CandleThrob
=================================================

This module consolidates all monitoring functionality across the system,
replacing duplicate monitor classes with unified implementations.
"""

import time
import psutil
import threading
from typing import Dict, Any, Optional, List, ContextManager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from contextlib import contextmanager

from .base import BasePerformanceMonitor, OperationMetrics


@dataclass
class SystemMetrics:
    """System-wide metrics collection."""
    
    timestamp: datetime = field(default_factory=datetime.now)
    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    memory_used_mb: float = 0.0
    disk_usage_percent: float = 0.0
    network_io: Dict[str, int] = field(default_factory=dict)
    process_count: int = 0
    
    def update(self):
        """Update all system metrics."""
        self.timestamp = datetime.now()
        self.cpu_percent = psutil.cpu_percent()
        memory = psutil.virtual_memory()
        self.memory_percent = memory.percent
        self.memory_used_mb = memory.used / 1024 / 1024
        
        try:
            disk = psutil.disk_usage('/')
            self.disk_usage_percent = (disk.used / disk.total) * 100
        except:
            self.disk_usage_percent = 0.0
        
        self.process_count = len(psutil.pids())


@dataclass
class HealthMetrics:
    """Health monitoring metrics."""
    
    service_name: str
    status: str = "unknown"  # healthy, degraded, unhealthy, unknown
    last_check: datetime = field(default_factory=datetime.now)
    response_time_ms: float = 0.0
    error_count: int = 0
    success_count: int = 0
    uptime_seconds: float = 0.0
    custom_metrics: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        total = self.success_count + self.error_count
        return (self.success_count / total) if total > 0 else 0.0
    
    @property
    def is_healthy(self) -> bool:
        """Check if service is healthy based on metrics."""
        return (
            self.status == "healthy" and
            self.success_rate >= 0.95 and
            self.response_time_ms < 5000
        )


class UniversalPerformanceMonitor(BasePerformanceMonitor):
    """Universal performance monitor that can track various operation types."""
    
    def __init__(self, monitor_name: str):
        super().__init__()
        self.monitor_name = monitor_name
        self.system_metrics = SystemMetrics()
        self.health_metrics = HealthMetrics(service_name=monitor_name)
        self.start_time = time.time()
        self._metric_history: List[Dict[str, Any]] = []
        self._max_history = 1000  # Keep last 1000 metrics
    
    def record_operation(self, operation_name: str, duration: float, 
                        success: bool, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Record an operation with its metrics."""
        metrics = OperationMetrics(
            operation_name=operation_name,
            start_time=time.time() - duration,
            duration=duration,
            success=success,
            additional_data=metadata or {}
        )
        metrics.finish(success)
        
        with self._lock:
            self.metrics[operation_name] = metrics
            
            # Update health metrics
            if success:
                self.health_metrics.success_count += 1
            else:
                self.health_metrics.error_count += 1
            
            self.health_metrics.response_time_ms = duration * 1000
            self.health_metrics.last_check = datetime.now()
            
            # Update status based on recent performance
            if self.health_metrics.success_rate >= 0.95:
                self.health_metrics.status = "healthy"
            elif self.health_metrics.success_rate >= 0.8:
                self.health_metrics.status = "degraded"
            else:
                self.health_metrics.status = "unhealthy"
    
    def get_system_metrics(self) -> SystemMetrics:
        """Get current system metrics."""
        self.system_metrics.update()
        return self.system_metrics
    
    def get_health_status(self) -> HealthMetrics:
        """Get current health status."""
        self.health_metrics.uptime_seconds = time.time() - self.start_time
        return self.health_metrics
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary."""
        with self._lock:
            total_operations = len(self.metrics)
            successful_operations = sum(1 for m in self.metrics.values() if m.success)
            total_duration = sum(m.duration or 0 for m in self.metrics.values())
            avg_duration = total_duration / total_operations if total_operations > 0 else 0
            
            return {
                "monitor_name": self.monitor_name,
                "total_operations": total_operations,
                "successful_operations": successful_operations,
                "failed_operations": total_operations - successful_operations,
                "success_rate": successful_operations / total_operations if total_operations > 0 else 0,
                "average_duration": avg_duration,
                "total_duration": total_duration,
                "uptime_seconds": time.time() - self.start_time,
                "system_metrics": self.get_system_metrics(),
                "health_metrics": self.get_health_status()
            }
    
    def export_metrics(self, format: str = "dict") -> Any:
        """Export metrics in various formats."""
        summary = self.get_performance_summary()
        
        if format == "dict":
            return summary
        elif format == "json":
            import json
            return json.dumps(summary, default=str, indent=2)
        elif format == "prometheus":
            return self._to_prometheus_format(summary)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def _to_prometheus_format(self, summary: Dict[str, Any]) -> str:
        """Convert metrics to Prometheus format."""
        lines = []
        prefix = f"candlethrob_{self.monitor_name.lower()}"
        
        lines.append(f'# HELP {prefix}_operations_total Total number of operations')
        lines.append(f'# TYPE {prefix}_operations_total counter')
        lines.append(f'{prefix}_operations_total {summary["total_operations"]}')
        
        lines.append(f'# HELP {prefix}_success_rate Success rate of operations')
        lines.append(f'# TYPE {prefix}_success_rate gauge')
        lines.append(f'{prefix}_success_rate {summary["success_rate"]}')
        
        lines.append(f'# HELP {prefix}_avg_duration_seconds Average operation duration')
        lines.append(f'# TYPE {prefix}_avg_duration_seconds gauge')
        lines.append(f'{prefix}_avg_duration_seconds {summary["average_duration"]}')
        
        return '\n'.join(lines)


class HealthMonitor:
    """Health monitoring for services and components."""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.health_metrics = HealthMetrics(service_name=service_name)
        self.checks: Dict[str, callable] = {}
        self._lock = threading.Lock()
    
    def register_health_check(self, check_name: str, check_function: callable) -> None:
        """Register a health check function."""
        with self._lock:
            self.checks[check_name] = check_function
    
    def run_health_checks(self) -> Dict[str, Any]:
        """Run all registered health checks."""
        results = {
            "service_name": self.service_name,
            "overall_status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "checks": {}
        }
        
        with self._lock:
            for check_name, check_function in self.checks.items():
                try:
                    start_time = time.time()
                    check_result = check_function()
                    duration = time.time() - start_time
                    
                    results["checks"][check_name] = {
                        "status": "passed" if check_result else "failed",
                        "duration_ms": duration * 1000,
                        "details": getattr(check_function, 'details', None)
                    }
                    
                    if not check_result:
                        results["overall_status"] = "unhealthy"
                        
                except Exception as e:
                    results["checks"][check_name] = {
                        "status": "error",
                        "error": str(e),
                        "duration_ms": 0
                    }
                    results["overall_status"] = "unhealthy"
        
        # Update health metrics
        self.health_metrics.status = results["overall_status"]
        self.health_metrics.last_check = datetime.now()
        
        return results
    
    def get_status(self) -> str:
        """Get current health status."""
        return self.health_metrics.status


class ResourceMonitor:
    """Monitor system resources and set alerts."""
    
    def __init__(self, alert_thresholds: Optional[Dict[str, float]] = None):
        self.alert_thresholds = alert_thresholds or {
            "cpu_percent": 80.0,
            "memory_percent": 85.0,
            "disk_percent": 90.0
        }
        self.alerts: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
    
    def check_resources(self) -> Dict[str, Any]:
        """Check current resource usage and generate alerts."""
        system_metrics = SystemMetrics()
        system_metrics.update()
        
        alerts = []
        
        # Check CPU
        if system_metrics.cpu_percent > self.alert_thresholds["cpu_percent"]:
            alerts.append({
                "type": "cpu_high",
                "value": system_metrics.cpu_percent,
                "threshold": self.alert_thresholds["cpu_percent"],
                "severity": "warning" if system_metrics.cpu_percent < 95 else "critical"
            })
        
        # Check Memory
        if system_metrics.memory_percent > self.alert_thresholds["memory_percent"]:
            alerts.append({
                "type": "memory_high",
                "value": system_metrics.memory_percent,
                "threshold": self.alert_thresholds["memory_percent"],
                "severity": "warning" if system_metrics.memory_percent < 95 else "critical"
            })
        
        # Check Disk
        if system_metrics.disk_usage_percent > self.alert_thresholds["disk_percent"]:
            alerts.append({
                "type": "disk_high",
                "value": system_metrics.disk_usage_percent,
                "threshold": self.alert_thresholds["disk_percent"],
                "severity": "warning" if system_metrics.disk_usage_percent < 98 else "critical"
            })
        
        with self._lock:
            # Add new alerts
            for alert in alerts:
                alert["timestamp"] = datetime.now().isoformat()
                self.alerts.append(alert)
            
            # Keep only recent alerts (last 100)
            self.alerts = self.alerts[-100:]
        
        return {
            "system_metrics": system_metrics,
            "current_alerts": alerts,
            "alert_count": len(alerts)
        }
    
    def get_alerts(self, severity: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get current alerts, optionally filtered by severity."""
        with self._lock:
            if severity:
                return [alert for alert in self.alerts if alert.get("severity") == severity]
            return self.alerts.copy()


# Factory functions for creating monitors
def create_performance_monitor(name: str) -> UniversalPerformanceMonitor:
    """Create a performance monitor instance."""
    return UniversalPerformanceMonitor(name)

def create_health_monitor(service_name: str) -> HealthMonitor:
    """Create a health monitor instance."""
    return HealthMonitor(service_name)

def create_resource_monitor(thresholds: Optional[Dict[str, float]] = None) -> ResourceMonitor:
    """Create a resource monitor instance."""
    return ResourceMonitor(thresholds)


# Backward compatibility aliases
PerformanceMonitor = UniversalPerformanceMonitor
GCSHealthMonitor = HealthMonitor
VaultHealthMonitor = HealthMonitor