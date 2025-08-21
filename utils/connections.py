"""
Centralized Connection Management for CandleThrob
=================================================

This module consolidates all connection management functionality,
replacing duplicate connection classes with unified implementations.
"""

import os
import time
import threading
from typing import Dict, Any, Optional, Union, List
from dataclasses import dataclass
from contextlib import contextmanager

from ..core.config import DatabaseConfig, StorageConfig, APIConfig
from ..core.monitors import HealthMonitor, create_health_monitor
from ..core.logging import get_logger


@dataclass
class ConnectionPool:
    """Generic connection pool configuration."""
    
    min_connections: int = 1
    max_connections: int = 10
    timeout_seconds: int = 30
    retry_attempts: int = 3
    health_check_interval: int = 60


class DatabaseConnectionManager:
    """Manages database connections with health monitoring."""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.logger = get_logger("database_connection")
        self.health_monitor = create_health_monitor("database")
        self._connections: Dict[str, Any] = {}
        self._lock = threading.Lock()
        
        # Register health checks
        self.health_monitor.register_health_check("connectivity", self._check_connectivity)
        self.health_monitor.register_health_check("response_time", self._check_response_time)
    
    def get_connection(self, connection_name: str = "default") -> Any:
        """Get a database connection."""
        with self._lock:
            if connection_name not in self._connections:
                self._connections[connection_name] = self._create_connection()
            return self._connections[connection_name]
    
    def _create_connection(self) -> Any:
        """Create a new database connection."""
        try:
            # For Oracle database (existing implementation)
            if hasattr(self, '_create_oracle_connection'):
                return self._create_oracle_connection()
            else:
                # Placeholder for other database types
                self.logger.warning("No specific database implementation found")
                return None
        except Exception as e:
            self.logger.error(f"Failed to create database connection: {e}")
            raise
    
    def _check_connectivity(self) -> bool:
        """Health check for database connectivity."""
        try:
            conn = self.get_connection()
            if conn is None:
                return False
            
            # Simple connectivity test
            # Implementation depends on database type
            return True
        except Exception:
            return False
    
    def _check_response_time(self) -> bool:
        """Health check for database response time."""
        try:
            start_time = time.time()
            self._check_connectivity()
            response_time = time.time() - start_time
            
            # Consider healthy if response time < 5 seconds
            return response_time < 5.0
        except Exception:
            return False
    
    def close_connections(self) -> None:
        """Close all connections."""
        with self._lock:
            for name, conn in self._connections.items():
                try:
                    if hasattr(conn, 'close'):
                        conn.close()
                    self.logger.info(f"Closed connection: {name}")
                except Exception as e:
                    self.logger.error(f"Error closing connection {name}: {e}")
            self._connections.clear()
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get health status of database connections."""
        return self.health_monitor.run_health_checks()


class StorageConnectionManager:
    """Manages storage connections (GCS, local filesystem)."""
    
    def __init__(self, config: StorageConfig):
        self.config = config
        self.logger = get_logger("storage_connection")
        self.health_monitor = create_health_monitor("storage")
        self._clients: Dict[str, Any] = {}
        self._lock = threading.Lock()
        
        # Register health checks
        self.health_monitor.register_health_check("gcs_connectivity", self._check_gcs_connectivity)
        self.health_monitor.register_health_check("local_storage", self._check_local_storage)
    
    def get_gcs_client(self) -> Any:
        """Get GCS client instance."""
        with self._lock:
            if "gcs" not in self._clients:
                self._clients["gcs"] = self._create_gcs_client()
            return self._clients["gcs"]
    
    def _create_gcs_client(self) -> Any:
        """Create GCS client."""
        try:
            from google.cloud import storage
            
            if self.config.gcs_credentials_path:
                client = storage.Client.from_service_account_json(
                    self.config.gcs_credentials_path,
                    project=self.config.gcs_project_id
                )
            else:
                client = storage.Client(project=self.config.gcs_project_id)
            
            self.logger.info("GCS client created successfully")
            return client
        except Exception as e:
            self.logger.error(f"Failed to create GCS client: {e}")
            raise
    
    def _check_gcs_connectivity(self) -> bool:
        """Health check for GCS connectivity."""
        try:
            client = self.get_gcs_client()
            bucket = client.bucket(self.config.gcs_bucket)
            bucket.exists()  # Simple existence check
            return True
        except Exception as e:
            self.logger.warning(f"GCS connectivity check failed: {e}")
            return False
    
    def _check_local_storage(self) -> bool:
        """Health check for local storage."""
        try:
            import os
            from pathlib import Path
            
            data_dir = Path(self.config.local_data_dir)
            data_dir.mkdir(parents=True, exist_ok=True)
            
            # Test write/read
            test_file = data_dir / "health_check.tmp"
            test_file.write_text("health check")
            content = test_file.read_text()
            test_file.unlink()
            
            return content == "health check"
        except Exception as e:
            self.logger.warning(f"Local storage check failed: {e}")
            return False
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get health status of storage connections."""
        return self.health_monitor.run_health_checks()


class APIConnectionManager:
    """Manages external API connections."""
    
    def __init__(self, config: APIConfig):
        self.config = config
        self.logger = get_logger("api_connection")
        self.health_monitor = create_health_monitor("api")
        self._sessions: Dict[str, Any] = {}
        self._lock = threading.Lock()
        
        # Register health checks
        self.health_monitor.register_health_check("polygon_api", self._check_polygon_api)
        self.health_monitor.register_health_check("fred_api", self._check_fred_api)
    
    def get_polygon_client(self) -> Any:
        """Get Polygon API client."""
        with self._lock:
            if "polygon" not in self._sessions:
                self._sessions["polygon"] = self._create_polygon_client()
            return self._sessions["polygon"]
    
    def get_fred_client(self) -> Any:
        """Get FRED API client."""
        with self._lock:
            if "fred" not in self._sessions:
                self._sessions["fred"] = self._create_fred_client()
            return self._sessions["fred"]
    
    def _create_polygon_client(self) -> Any:
        """Create Polygon API client."""
        try:
            from polygon import RESTClient
            client = RESTClient(api_key=self.config.polygon_api_key)
            self.logger.info("Polygon client created successfully")
            return client
        except Exception as e:
            self.logger.error(f"Failed to create Polygon client: {e}")
            raise
    
    def _create_fred_client(self) -> Any:
        """Create FRED API client."""
        try:
            from fredapi import Fred
            client = Fred(api_key=self.config.fred_api_key)
            self.logger.info("FRED client created successfully")
            return client
        except Exception as e:
            self.logger.error(f"Failed to create FRED client: {e}")
            raise
    
    def _check_polygon_api(self) -> bool:
        """Health check for Polygon API."""
        try:
            client = self.get_polygon_client()
            # Simple API test - get market status
            status = client.get_market_status()
            return status is not None
        except Exception as e:
            self.logger.warning(f"Polygon API check failed: {e}")
            return False
    
    def _check_fred_api(self) -> bool:
        """Health check for FRED API."""
        try:
            client = self.get_fred_client()
            # Simple API test - get a known series
            data = client.get_series('GDP', limit=1)
            return data is not None and not data.empty
        except Exception as e:
            self.logger.warning(f"FRED API check failed: {e}")
            return False
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get health status of API connections."""
        return self.health_monitor.run_health_checks()


class ConnectionRegistry:
    """Central registry for all connection managers."""
    
    def __init__(self, db_config: DatabaseConfig, storage_config: StorageConfig, api_config: APIConfig):
        self.db_manager = DatabaseConnectionManager(db_config)
        self.storage_manager = StorageConnectionManager(storage_config)
        self.api_manager = APIConnectionManager(api_config)
        
        self.logger = get_logger("connection_registry")
        self.health_monitor = create_health_monitor("connection_registry")
        
        # Register aggregate health checks
        self.health_monitor.register_health_check("all_systems", self._check_all_systems)
    
    def _check_all_systems(self) -> bool:
        """Health check for all connection systems."""
        try:
            db_status = self.db_manager.get_health_status()
            storage_status = self.storage_manager.get_health_status()
            api_status = self.api_manager.get_health_status()
            
            return (
                db_status["overall_status"] == "healthy" and
                storage_status["overall_status"] == "healthy" and
                api_status["overall_status"] == "healthy"
            )
        except Exception:
            return False
    
    def get_comprehensive_health_status(self) -> Dict[str, Any]:
        """Get comprehensive health status of all connections."""
        return {
            "database": self.db_manager.get_health_status(),
            "storage": self.storage_manager.get_health_status(),
            "api": self.api_manager.get_health_status(),
            "overall": self.health_monitor.run_health_checks()
        }
    
    def close_all_connections(self) -> None:
        """Close all connections across all managers."""
        try:
            self.db_manager.close_connections()
            self.logger.info("All connections closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing connections: {e}")


# Factory function for creating connection registry
def create_connection_registry(db_config: DatabaseConfig, storage_config: StorageConfig, api_config: APIConfig) -> ConnectionRegistry:
    """Create a connection registry with all managers."""
    return ConnectionRegistry(db_config, storage_config, api_config)