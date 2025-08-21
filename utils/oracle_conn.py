"""
Oracle Database Connection Manager for CandleThrob

This module provides a robust Oracle database connection manager with advanced features
including connection pooling, retry logic, performance monitoring, and comprehensive
error handling.

Author: Adetunji Fasiku
Version: 2.0.0
"""

import os
import time
import logging
import threading
from contextlib import contextmanager
from typing import Optional, Dict, Any, Tuple
from dataclasses import dataclass
from functools import wraps
from CandleThrob.core.monitors import UniversalPerformanceMonitor as PerformanceMonitor

try:
    import oracledb
    from sqlalchemy import create_engine, event
    from sqlalchemy.pool import QueuePool
except ImportError as e:
    print(f"Required packages not installed: {e}")
    print("Please install: pip install oracledb sqlalchemy")

from CandleThrob.core.logging import get_ingestion_logger, get_transform_logger, log_ingestion_start, log_ingestion_success, log_ingestion_error, log_rate_limiting, log_batch_progress

logger = get_database_logger(__name__)

@dataclass
class OracleConfig:
    """Configuration for Oracle database connections."""
    user: str = os.getenv('ORACLE_USER', 'ADMIN')
    password: str = os.getenv('ORACLE_PASSWORD', '')
    dsn: str = os.getenv('ORACLE_DSN', 'candlethroblake_high')
    config_dir: str = os.getenv('TNS_ADMIN', '/opt/oracle/instantclient_23_8/network/admin')
    max_connections: int = 5
    connection_timeout: int = 30
    enable_query_logging: bool = False
    enable_performance_monitoring: bool = True

class ConnectionPool:
    """Manages Oracle database connection pool."""
    
    def __init__(self):
        self.engine = None
        self.connection_count = 0
        self._lock = threading.Lock()


def retry_on_failure(max_retries: int = 3, delay_seconds: float = 1.0):
    """Decorator for retrying database operations on failure."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay_seconds}s...")
                        time.sleep(delay_seconds)
                    else:
                        logger.error(f"All {max_retries} attempts failed. Last error: {e}")
            raise last_exception
        return wrapper
    return decorator

class OracleDB:
    """
    Oracle Database Connection Manager with advanced features.
    
    Provides connection pooling, retry logic, performance monitoring,
    and comprehensive error handling for Oracle database operations.
    """
    
    def __init__(self, config: Optional[OracleConfig] = None):
        """
        Initialize Oracle database manager.
        
        Args:
            config: Optional OracleConfig instance. If None, uses default configuration.
        """
        self.config = config or OracleConfig()
        self.connection_pool = ConnectionPool()
        self.performance_monitor = PerformanceMonitor("default")
        
        # Initialize connection pool
        try:
            connection_string = self._build_connection_string()
            self.initialize_pool(connection_string)
            logger.info("Oracle database manager initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Oracle database manager: {e}")
            raise
    
    def _build_connection_string(self) -> str:
        """Build SQLAlchemy connection string for Oracle."""
        return f"oracle+oracledb://{self.config.user}:{self.config.password}@{self.config.dsn}"
    
    def initialize_pool(self, connection_string: str):
        """Initialize the connection pool."""
        try:
            self.connection_pool.engine = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=self.config.max_connections,
                max_overflow=0,
                pool_timeout=self.config.connection_timeout,
                pool_pre_ping=True,
                pool_recycle=3600,  # Recycle connections every hour
                echo=self.config.enable_query_logging
            )
            
            # Add event listeners for monitoring
            if self.config.enable_performance_monitoring:
                event.listen(self.connection_pool.engine, 'before_cursor_execute', self._before_cursor_execute)
                event.listen(self.connection_pool.engine, 'after_cursor_execute', self._after_cursor_execute)
            
            logger.info(f"Connection pool initialized with {self.config.max_connections} connections")
            
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            raise
    
    def _before_cursor_execute(self, conn, cursor, statement, parameters, context, executemany):
        """Event listener for query start."""
        context._query_start_time = time.time()
    
    def _after_cursor_execute(self, conn, cursor, statement, parameters, context, executemany):
        """Event listener for query completion."""
        total_time = time.time() - context._query_start_time
        self.performance_monitor.record_query_time(total_time)
        
        if total_time > 5.0:  # Log slow queries
            logger.warning(f"Slow query detected: {total_time:.2f}s - {statement[:100]}...")
    
    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def get_sqlalchemy_engine(self):
        """
        Get SQLAlchemy engine for pandas operations.
        
        Returns:
            Engine: SQLAlchemy engine
        """
        try:
            if self.connection_pool.engine:
                return self.connection_pool.engine
            else:
                # Fallback to direct engine creation
                connection_string = self._build_connection_string()
                engine = create_engine(connection_string)
                return engine
                
        except Exception as e:
            logger.error(f"Failed to get SQLAlchemy engine: {e}")
            raise
    
    @contextmanager
    def establish_connection(self):
        """
        Create and manage database connection.
        
        Yields:
            Connection: Oracle database connection
        """
        conn = None
        
        try:
            logger.info("Establishing Oracle DB connection")
            
            # Use the existing get_sqlalchemy_engine method
            engine = self.get_sqlalchemy_engine()
            conn = engine.connect()
            
            with self.performance_monitor._lock:
                self.connection_pool.connection_count += 1
            
            yield conn
            
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
        finally:
            if conn:
                try:
                    conn.close()
                    with self.performance_monitor._lock:
                        self.connection_pool.connection_count -= 1
                    logger.debug("Database connection closed")
                except Exception as e:
                    logger.error(f"Error closing connection: {e}")
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """
        Get connection pool statistics.
        
        Returns:
            Dict containing connection statistics
        """
        try:
            engine = self.connection_pool.engine
            if engine and hasattr(engine.pool, 'size'):
                return {
                    "pool_size": engine.pool.size(),
                    "checked_in": engine.pool.checkedin(),
                    "checked_out": engine.pool.checkedout(),
                    "overflow": engine.pool.overflow(),
                    "invalid": engine.pool.invalid(),
                    "active_connections": self.connection_pool.connection_count
                }
            else:
                return {"status": "Pool not initialized"}
        except Exception as e:
            logger.error(f"Error getting connection stats: {e}")
            return {"error": str(e)}
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """
        Get performance statistics.
        
        Returns:
            Dict containing performance metrics
        """
        try:
            with self.performance_monitor._lock:
                query_times = self.performance_monitor.query_times.copy()
            
            if not query_times:
                return {"status": "No queries recorded"}
            
            return {
                "total_queries": len(query_times),
                "avg_query_time": sum(query_times) / len(query_times),
                "min_query_time": min(query_times),
                "max_query_time": max(query_times),
                "slow_queries": len([t for t in query_times if t > 5.0])
            }
        except Exception as e:
            logger.error(f"Error getting performance stats: {e}")
            return {"error": str(e)}

# Convenience function for backward compatibility
def get_oracle_connection():
    """Get Oracle database connection (legacy function)."""
    db = OracleDB()
    return db.establish_connection()
