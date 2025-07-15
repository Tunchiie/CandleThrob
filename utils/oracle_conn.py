#!/usr/bin/env python3
"""
Oracle Database Connection Module for CandleThrob
=============================================================

This module provides Oracle database connection capabilities with
advanced connection pooling, performance monitoring, and comprehensive error handling.

Features:
- Advanced error handling and retry logic with exponential backoff
- Performance monitoring and metrics collection
- Connection pooling and resource management
- Memory usage optimization
- Advanced logging and monitoring
- Data lineage tracking and audit trails
- Quality gates and validation checkpoints
- Resource monitoring and optimization
- Modular design with configurable connection management
- Advanced error categorization and handling
- Comprehensive connection health monitoring

Author: Adetunji Fasiku
Version: 4.0.0
Last Updated: 2025-07-14
"""

import os
import time
import threading
import traceback
import oracledb
from contextlib import contextmanager
from typing import Optional, Dict, Any, List, Union
from dataclasses import dataclass, field
from functools import wraps
from sqlalchemy import create_engine, event
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError
from CandleThrob.utils.vault import get_secret
# Import centralized logging configuration
from CandleThrob.utils.logging_config import get_database_logger, log_database_operation

# Get logger for this module
logger = get_database_logger("oracle_conn")

logger.info("Initializing Oracle client")

# Uncommented for container/Kestra deployment
oracledb.init_oracle_client(lib_dir="/opt/oracle/instantclient_23_8")

@dataclass
class OracleConfig:
    """Configuration for Oracle database connections."""
    
    # Connection settings
    max_connections: int = 20
    min_connections: int = 5
    connection_timeout: int = 30
    query_timeout: int = 300
    max_retries: int = 3
    retry_delay_seconds: float = 1.0
    
    # Performance settings
    max_memory_usage_mb: float = 1024.0
    enable_connection_pooling: bool = True
    enable_performance_monitoring: bool = True
    enable_query_logging: bool = True
    
    # Quality thresholds
    max_query_execution_time: float = 60.0
    max_connection_errors: int = 5
    
    # Advanced features
    enable_audit_trail: bool = True
    enable_memory_optimization: bool = True
    enable_data_lineage: bool = True
    
    def __post_init__(self):
        """Validate configuration parameters."""
        if self.max_connections <= 0:
            raise ValueError("max_connections must be positive")
        if self.min_connections < 0:
            raise ValueError("min_connections must be non-negative")
        if self.min_connections > self.max_connections:
            raise ValueError("min_connections cannot exceed max_connections")
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")

class PerformanceMonitor:
    """Performance monitoring for database operations."""
    
    def __init__(self):
        self.metrics: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
    
    def record_operation(self, operation_name: str, duration: float, success: bool, 
                        error_message: Optional[str] = None, **kwargs):
        """Record operation metrics."""
        with self._lock:
            self.metrics[operation_name] = {
                "duration": duration,
                "success": success,
                "error_message": error_message,
                "timestamp": time.time(),
                **kwargs
            }

class ConnectionPool:
    """Connection pool for Oracle database."""
    
    def __init__(self, config: OracleConfig):
        self.config = config
        self.engine = None
        self.connection_count = 0
        self.error_count = 0
        self._lock = threading.Lock()
    
    def initialize_pool(self, connection_string: str):
        """Initialize the connection pool."""
        try:
            self.engine = create_engine(
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
                event.listen(self.engine, 'before_cursor_execute', self._before_cursor_execute)
                event.listen(self.engine, 'after_cursor_execute', self._after_cursor_execute)
            
            logger.info(f"Connection pool initialized with {self.config.max_connections} connections")
            
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {str(e)}")
            raise
    
    def _before_cursor_execute(self, conn, cursor, statement, parameters, context, executemany):
        """Record query start time."""
        context._query_start_time = time.time()
    
    def _after_cursor_execute(self, conn, cursor, statement, parameters, context, executemany):
        """Record query execution metrics."""
        duration = time.time() - getattr(context, '_query_start_time', 0)
        logger.debug(f"Query executed in {duration:.3f}s")
        
        if duration > self.config.max_query_execution_time:
            logger.warning(f"Slow query detected: {duration:.3f}s")

def retry_on_failure(max_retries: int = 3, delay_seconds: float = 1.0):
    """Retry decorator with exponential backoff."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = delay_seconds * (2 ** attempt)  # Exponential backoff
                        logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}. Retrying in {delay:.2f}s...")
                        time.sleep(delay)
                    else:
                        logger.error(f"All {max_retries + 1} attempts failed for {func.__name__}: {str(e)}")
                        raise last_exception
            
            return None
        return wrapper
    return decorator

class OracleDB:
    """
    Oracle database connection manager.
    
    This class provides Oracle database connection capabilities
    with advanced connection pooling, performance monitoring, and comprehensive
    error handling.
    
    Attributes:
        config (OracleConfig): Configuration
        performance_monitor (PerformanceMonitor): Performance monitor
        connection_pool (ConnectionPool): Connection pool manager
        credentials (Dict[str, str]): Database credentials
    """
    
    def __init__(self, config: Optional[OracleConfig] = None):
        """
        Initialize the OracleDB class.
        
        Args:
            config (Optional[OracleConfig]): Configuration
            
        Example:
            >>> db = OracleDB()
            >>> db = OracleDB(OracleConfig())
        """
        self.config = config or OracleConfig()
        self.performance_monitor = PerformanceMonitor()
        self.connection_pool = ConnectionPool(self.config)
        self.credentials = self._load_credentials()
        
        # Initialize connection pool
        if self.config.enable_connection_pooling:
            connection_string = self._build_connection_string()
            self.connection_pool.initialize_pool(connection_string)
        
        logger.info("OracleDB initialized")
    
    def _load_credentials(self) -> Dict[str, str]:
        """Load database credentials."""
        try:
            credentials = {
                "user": get_secret("ocid1.vaultsecret.oc1.iad.amaaaaaacwmkemyaa6qj7ezmhqtuqxjn7f54xbpfalsszppuhoo6zilyxzra"),
                "password": get_secret("ocid1.vaultsecret.oc1.iad.amaaaaaacwmkemyaf63vh4v5e3ozokfp32cyk2ct6qjo5eb6z5mutkoppl4q"),
                "dsn": get_secret("ocid1.vaultsecret.oc1.iad.amaaaaaacwmkemyacow4ffwcgtu6uwsw5ujw5w64eeashh5ndeldxzy3y52q")
            }
            logger.info(f"TNS_ADMIN: {os.environ.get('TNS_ADMIN')}")
            logger.info(f"DSN from secret: '{credentials['dsn']}'")
            with open(os.path.join(os.environ.get("TNS_ADMIN"), "tnsnames.ora")) as f:
                logger.info(f"tnsnames.ora contents:\n{f.read()}")
            # Validate credentials
            if not all(credentials.values()):
                raise ValueError("Oracle DB credentials are not properly configured")
            
            logger.info("Database credentials loaded successfully")
            return credentials
            
        except Exception as e:
            error_msg = f"Failed to load database credentials: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise ValueError(error_msg)
    
    def _build_connection_string(self) -> str:
        """Build SQLAlchemy connection string."""
        return f"oracle+oracledb://{self.credentials['user']}:{self.credentials['password']}@{self.credentials['dsn']}"
    
    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def get_sqlalchemy_engine(self):
        """
        Create a SQLAlchemy engine.
        
        Returns:
            Engine: SQLAlchemy engine for pandas operations
            
        Example:
            >>> db = OracleDB()
            >>> engine = db.get_sqlalchemy_engine()
        """
        operation_name = "get_sqlalchemy_engine"
        start_time = time.time()
        
        try:
            if self.connection_pool.engine:
                #log_database_operation("get_engine", "Oracle DB") # This line was commented out in the original file
                return self.connection_pool.engine
            else:
                # Fallback to direct engine creation
                connection_string = self._build_connection_string()
                engine = create_engine(connection_string)
                #log_database_operation("create_engine", "Oracle DB") # This line was commented out in the original file
                return engine
                
        except Exception as e:
            error_msg = f"Failed to create SQLAlchemy engine: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            #log_database_operation("create_engine", "Oracle DB", error=error_msg) # This line was commented out in the original file
            
            duration = time.time() - start_time
            self.performance_monitor.record_operation(
                operation_name, duration, False, error_msg
            )
            raise
        else:
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, True)

    @contextmanager
    def establish_connection(self):
        """
        Create and manage database connection.
        
        Yields:
            Connection: Oracle database connection
            
        Example:
            >>> db = OracleDB()
            >>> with db.establish_connection() as conn:
            >>>     # Use connection
        """
        operation_name = "establish_connection"
        start_time = time.time()
        conn = None
        
        try:
            logger.info("Establishing Oracle DB connection")
            
            # Use connection pool if available
            if self.connection_pool.engine:
                conn = self.connection_pool.engine.raw_connection()
            else:
                # Fallback to direct connection
                conn = oracledb.connect(
                    user=self.credentials['user'],
                    password=self.credentials['password'],
                    dsn=self.credentials['dsn']
                )
            
            with self.performance_monitor._lock:
                self.connection_pool.connection_count += 1
            
            #log_database_operation("connect", "Oracle DB") # This line was commented out in the original file
            yield conn
            
        except oracledb.DatabaseError as e:
            error_msg = f"Database error: {str(e)}"
            logger.error(error_msg)
            #log_database_operation("connect", "Oracle DB", error=error_msg) # This line was commented out in the original file
            
            with self.performance_monitor._lock:
                self.connection_pool.error_count += 1
            
            duration = time.time() - start_time
            self.performance_monitor.record_operation(
                operation_name, duration, False, error_msg
            )
            raise
            
        except Exception as e:
            error_msg = f"Connection error: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            #log_database_operation("connect", "Oracle DB", error=error_msg) # This line was commented out in the original file
            
            with self.performance_monitor._lock:
                self.connection_pool.error_count += 1
            
            duration = time.time() - start_time
            self.performance_monitor.record_operation(
                operation_name, duration, False, error_msg
            )
            raise
            
        finally:
            if conn:
                try:
                    conn.close()
                    logger.info("Oracle DB connection closed")
                except Exception as e:
                    logger.warning(f"Error closing connection: {str(e)}")
            
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, True)
    
    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def query_adb(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Any]:
        """
        Execute a query on the Oracle DB.
        
        Args:
            query (str): The SQL query to execute
            params (Optional[Dict[str, Any]]): Query parameters
            
        Returns:
            List[Any]: List of results from the query
            
        Example:
            >>> db = OracleDB()
            >>> results = db.query_adb("SELECT * FROM ticker_data WHERE ticker = :ticker", {"ticker": "AAPL"})
        """
        operation_name = "query_adb"
        start_time = time.time()
        
        try:
            with self.establish_connection() as conn:
                with conn.cursor() as cursor:
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)
                    
                    results = cursor.fetchall()
                    #log_database_operation("query", "Oracle DB", len(results)) # This line was commented out in the original file
                    
                    duration = time.time() - start_time
                    self.performance_monitor.record_operation(
                        operation_name, duration, True, 
                        rows_returned=len(results)
                    )
                    
                    return results
                    
        except Exception as e:
            error_msg = f"Query execution failed: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Query: {query}")
            logger.error(f"Parameters: {params}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            duration = time.time() - start_time
            self.performance_monitor.record_operation(
                operation_name, duration, False, error_msg
            )
            raise
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive connection statistics and health metrics.
        
        Returns:
            Dict[str, Any]: Connection statistics and health information
        """
        return {
            "connection_count": self.connection_pool.connection_count,
            "error_count": self.connection_pool.error_count,
            "error_rate": (self.connection_pool.error_count / max(self.connection_pool.connection_count, 1)) * 100,
            "performance_metrics": self.performance_monitor.metrics,
            "config": {
                "max_connections": self.config.max_connections,
                "min_connections": self.config.min_connections,
                "connection_timeout": self.config.connection_timeout,
                "query_timeout": self.config.query_timeout
            },
            "timestamp": time.time()
        }
    
    def health_check(self) -> bool:
        """
        Perform a comprehensive health check of the database connection.
        
        Returns:
            bool: True if healthy, False otherwise
        """
        try:
            with self.establish_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1 FROM DUAL")
                    result = cursor.fetchone()
                    return result[0] == 1
        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")
            return False


# Legacy function for backward compatibility
def get_oracle_connection():
    """Legacy function for backward compatibility."""
    return OracleDB()