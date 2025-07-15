"""
Database Connection Testing Utility for CandleThrob

This module provides a comprehensive database connection testing utility that validates
Oracle database connectivity with advanced error handling, performance monitoring,
and detailed diagnostics. It supports connection pooling, retry logic, and
comprehensive health checks including table existence verification.

Features:
- Comprehensive connection testing with detailed diagnostics
- Connection pooling and performance monitoring
- Retry logic with exponential backoff
- Detailed error reporting and logging
- Configuration management and validation
- Health check endpoints for monitoring
- Connection metrics and performance tracking
- Security best practices for credential handling
- Table existence and structure verification
- CandleThrob-specific table validation

Author: Adetunji Fasiku
Version: 2.1.0
Last Updated: 2025-01-13
"""

import os
import sys
import time
import threading
from typing import Dict, Any, Optional, Tuple, List
from dataclasses import dataclass, asdict
from contextlib import contextmanager
import traceback
from pathlib import Path

# Add the project root to Python path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    import oracledb
except ImportError:
    print("❌ Error: oracledb module not found. Please install it with: pip install oracledb")
    sys.exit(1)

# Try to import CandleThrob modules with fallback
try:
    from CandleThrob.utils.vault import get_secret
    from CandleThrob.utils.logging_config import get_database_logger, performance_monitor
except ImportError:
    # Fallback for when running directly
    try:
        from CandleThrob.utils.vault import get_secret
        from CandleThrob.utils.logging_config import get_database_logger, performance_monitor
    except ImportError:
        print("❌ Error: Could not import CandleThrob modules. Make sure you're running from the correct directory.")
        sys.exit(1)

# Configuration constants
DEFAULT_CONNECTION_TIMEOUT = 30  # seconds
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY = 1  # seconds
DEFAULT_MAX_RETRY_DELAY = 60  # seconds
DEFAULT_HEALTH_CHECK_INTERVAL = 300  # seconds (5 minutes)

# Expected CandleThrob tables
CANDLETHROB_TABLES = [
    'TICKER_DATA',
    'MACRO_DATA', 
    'TRANSFORMED_TICKERS',
    'TRANSFORMED_MACRO_DATA'
]

@dataclass
class TableInfo:
    """Information about a database table."""
    table_name: str
    column_count: int
    row_count: Optional[int] = None
    created_date: Optional[str] = None
    last_updated: Optional[str] = None
    columns: Optional[List[Dict[str, Any]]] = None

@dataclass
class TableCheckResult:
    """Result of a table existence check."""
    table_name: str
    exists: bool
    structure_valid: bool
    table_info: Optional[TableInfo] = None
    error_message: Optional[str] = None

@dataclass
class ConnectionConfig:
    """Configuration for database connection testing."""
    user: str
    password: str
    dsn: str
    config_dir: Optional[str] = None
    wallet_location: Optional[str] = None
    wallet_password: Optional[str] = None
    connection_timeout: int = DEFAULT_CONNECTION_TIMEOUT
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS
    retry_delay: int = DEFAULT_RETRY_DELAY
    max_retry_delay: int = DEFAULT_MAX_RETRY_DELAY
    enable_pooling: bool = True
    pool_min_size: int = 1
    pool_max_size: int = 10
    pool_increment: int = 1
    enable_health_monitoring: bool = True
    health_check_interval: int = DEFAULT_HEALTH_CHECK_INTERVAL

@dataclass
class ConnectionResult:
    """Result of a database connection test."""
    success: bool
    duration: float
    error_message: Optional[str] = None
    error_type: Optional[str] = None
    connection_info: Optional[Dict[str, Any]] = None
    performance_metrics: Optional[Dict[str, Any]] = None
    timestamp: Optional[str] = None

class DatabaseHealthMonitor:
    """Health monitoring for database connections."""
    
    def __init__(self):
        self.connection_history: List[ConnectionResult] = []
        self.lock = threading.Lock()
        # Create a simple logger if the main one fails
        try:
            self.logger = get_database_logger(__name__)
        except:
            # Fallback logger
            import logging
            self.logger = logging.getLogger(__name__)
            self.logger.setLevel(logging.INFO)
            if not self.logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)
    
    def record_connection_attempt(self, result: ConnectionResult):
        """Record a connection attempt for health monitoring."""
        with self.lock:
            self.connection_history.append(result)
            # Keep only last 100 attempts
            if len(self.connection_history) > 100:
                self.connection_history = self.connection_history[-100:]
    
    def get_health_metrics(self) -> Dict[str, Any]:
        """Get health metrics for the database connection."""
        with self.lock:
            if not self.connection_history:
                return {"status": "unknown", "total_attempts": 0}
            
            recent_results = self.connection_history[-10:]  # Last 10 attempts
            successful_attempts = [r for r in recent_results if r.success]
            failed_attempts = [r for r in recent_results if not r.success]
            
            success_rate = len(successful_attempts) / len(recent_results) if recent_results else 0
            avg_duration = sum(r.duration for r in successful_attempts) / len(successful_attempts) if successful_attempts else 0
            
            return {
                "status": "healthy" if success_rate >= 0.8 else "degraded" if success_rate >= 0.5 else "unhealthy",
                "success_rate": success_rate,
                "avg_connection_time": avg_duration,
                "total_attempts": len(self.connection_history),
                "recent_failures": len(failed_attempts),
                "last_attempt": self.connection_history[-1].timestamp if self.connection_history else None
            }
    
    def get_connection_history(self, limit: int = 20) -> List[ConnectionResult]:
        """Get recent connection history."""
        with self.lock:
            return self.connection_history[-limit:] if self.connection_history else []

class DatabaseConnectionTester:
    """Database connection testing utility."""
    
    def __init__(self, config: ConnectionConfig):
        self.config = config
        # Create a simple logger if the main one fails
        try:
            self.logger = get_database_logger(__name__)
        except:
            # Fallback logger
            import logging
            self.logger = logging.getLogger(__name__)
            self.logger.setLevel(logging.INFO)
            if not self.logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)
        
        self.health_monitor = DatabaseHealthMonitor()
        self._connection_pool = None
        self._pool_lock = threading.Lock()
    
    def _initialize_oracle_client(self) -> bool:
        """Initialize Oracle client with error handling."""
        try:
            # Check if Oracle client is already initialized
            if hasattr(oracledb, '_oracle_client_initialized'):
                self.logger.info("Oracle client already initialized")
                return True
            
            # Try to initialize Oracle client
            lib_dir = "/opt/oracle/instantclient"
            if os.path.exists(lib_dir):
                oracledb.init_oracle_client(lib_dir=lib_dir)
                self.logger.info(f"Oracle client initialized from {lib_dir}")
                return True
            else:
                self.logger.warning(f"Oracle client library directory not found: {lib_dir}")
                # Try without explicit lib_dir
                oracledb.init_oracle_client()
                self.logger.info("Oracle client initialized with default settings")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to initialize Oracle client: {e}")
            return False
    
    def _validate_config(self) -> Tuple[bool, Optional[str]]:
        """Validate connection configuration."""
        try:
            # Check required fields
            if not self.config.user:
                return False, "Database user is required"
            if not self.config.password:
                return False, "Database password is required"
            if not self.config.dsn:
                return False, "Database DSN is required"
            
            # Check wallet configuration if provided
            if self.config.wallet_location and not os.path.exists(self.config.wallet_location):
                return False, f"Wallet location does not exist: {self.config.wallet_location}"
            
            if self.config.config_dir and not os.path.exists(self.config.config_dir):
                return False, f"Config directory does not exist: {self.config.config_dir}"
            
            return True, None
            
        except Exception as e:
            return False, f"Configuration validation failed: {e}"
    
    def _create_connection_params(self) -> Dict[str, Any]:
        """Create connection parameters with proper error handling."""
        params = {
            "user": self.config.user,
            "password": self.config.password,
            "dsn": self.config.dsn
        }
        
        # Add optional parameters
        if self.config.config_dir:
            params["config_dir"] = self.config.config_dir
        if self.config.wallet_location:
            params["wallet_location"] = self.config.wallet_location
        if self.config.wallet_password:
            params["wallet_password"] = self.config.wallet_password
        
        return params
    
    def _test_basic_connection(self) -> ConnectionResult:
        """Test basic database connection."""
        start_time = time.time()
        success = False
        error_message = None
        error_type = None
        connection_info = None
        
        try:
            # Initialize Oracle client
            if not self._initialize_oracle_client():
                raise RuntimeError("Failed to initialize Oracle client")
            
            # Validate configuration
            is_valid, validation_error = self._validate_config()
            if not is_valid:
                raise ValueError(validation_error)
            
            # Create connection parameters
            conn_params = self._create_connection_params()
            
            # Test connection
            self.logger.info("Testing database connection...")
            self.logger.debug(f"Connection parameters: user={self.config.user}, dsn={self.config.dsn}")
            
            conn = oracledb.connect(**conn_params)
            
            # Get connection information
            connection_info = {
                "version": conn.version,
                "server_version": getattr(conn, 'server_version', 'Unknown'),
                "autocommit": conn.autocommit,
                "encoding": getattr(conn, 'encoding', 'Unknown'),
                "nencoding": getattr(conn, 'nencoding', 'Unknown')
            }
            
            # Test basic query
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            result = cursor.fetchone()
            cursor.close()
            
            if result and result[0] == 1:
                success = True
                self.logger.info("Database connection test successful")
            else:
                raise RuntimeError("Basic query test failed")
            
            conn.close()
            
        except Exception as e:
            success = False
            error_message = str(e)
            error_type = type(e).__name__
            self.logger.error(f"Database connection test failed: {error_message}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
        
        duration = time.time() - start_time
        
        result = ConnectionResult(
            success=success,
            duration=duration,
            error_message=error_message,
            error_type=error_type,
            connection_info=connection_info,
            timestamp=time.strftime("%Y-%m-%d %H:%M:%S")
        )
        
        self.health_monitor.record_connection_attempt(result)
        return result
    
    def _test_connection_with_retry(self) -> ConnectionResult:
        """Test connection with retry logic."""
        last_result = None
        
        for attempt in range(self.config.retry_attempts + 1):
            self.logger.info(f"Connection attempt {attempt + 1}/{self.config.retry_attempts + 1}")
            
            result = self._test_basic_connection()
            last_result = result
            
            if result.success:
                return result
            
            if attempt < self.config.retry_attempts:
                delay = min(self.config.retry_delay * (2 ** attempt), self.config.max_retry_delay)
                self.logger.warning(f"Connection failed, retrying in {delay} seconds...")
                time.sleep(delay)
        
        return last_result
    
    def test_connection(self, use_retry: bool = True) -> ConnectionResult:
        """Test database connection with optional retry logic."""
        self.logger.info("Starting database connection test")
        
        if use_retry:
            return self._test_connection_with_retry()
        else:
            return self._test_basic_connection()
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get comprehensive health status including table checks."""
        health_metrics = self.health_monitor.get_health_metrics()
        
        # Test current connection
        connection_result = self.test_connection(use_retry=False)
        
        # Check tables if connection is successful
        table_status = {}
        if connection_result.success:
            table_status = self.check_candlethrob_tables()
        
        return {
            "connection_health": health_metrics,
            "current_connection": {
                "success": connection_result.success,
                "duration": connection_result.duration,
                "error": connection_result.error_message
            },
            "table_status": table_status,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    
    def get_connection_history(self, limit: int = 20) -> List[ConnectionResult]:
        """Get recent connection history."""
        return self.health_monitor.get_connection_history(limit)
    
    def check_candlethrob_tables(self) -> Dict[str, Any]:
        """Check for CandleThrob-specific tables and their structure."""
        self.logger.info("Checking CandleThrob tables...")
        
        try:
            conn_params = self._create_connection_params()
            conn = oracledb.connect(**conn_params)
            cursor = conn.cursor()
            
            # Get all user tables
            cursor.execute("""
                SELECT table_name 
                FROM user_tables 
                ORDER BY table_name
            """)
            
            all_tables = [row[0] for row in cursor.fetchall()]
            
            # Check each expected table
            table_results = {}
            for expected_table in CANDLETHROB_TABLES:
                result = self._check_single_table(cursor, expected_table, all_tables)
                table_results[expected_table] = result
            
            cursor.close()
            conn.close()
            
            # Summary
            existing_tables = [name for name, result in table_results.items() if result.exists]
            missing_tables = [name for name, result in table_results.items() if not result.exists]
            
            summary = {
                "total_expected": len(CANDLETHROB_TABLES),
                "existing_tables": existing_tables,
                "missing_tables": missing_tables,
                "table_details": table_results
            }
            
            self.logger.info(f"Table check completed: {len(existing_tables)}/{len(CANDLETHROB_TABLES)} tables exist")
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Table check failed: {str(e)}")
            return {
                "error": str(e),
                "total_expected": len(CANDLETHROB_TABLES),
                "existing_tables": [],
                "missing_tables": CANDLETHROB_TABLES
            }
    
    def _check_single_table(self, cursor, table_name: str, all_tables: List[str]) -> TableCheckResult:
        """Check a single table for existence and structure."""
        try:
            exists = table_name in all_tables
            
            if not exists:
                return TableCheckResult(
                    table_name=table_name,
                    exists=False,
                    structure_valid=False,
                    error_message="Table does not exist"
                )
            
            # Get table structure
            cursor.execute(f"""
                SELECT column_name, data_type, nullable, data_length, data_precision, data_scale
                FROM user_tab_columns 
                WHERE table_name = '{table_name}'
                ORDER BY column_id
            """)
            
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "name": row[0],
                    "type": row[1],
                    "nullable": row[2] == "Y",
                    "length": row[3],
                    "precision": row[4],
                    "scale": row[5]
                })
            
            # Get table statistics
            cursor.execute(f"""
                SELECT num_rows, last_analyzed
                FROM user_tables 
                WHERE table_name = '{table_name}'
            """)
            
            stats = cursor.fetchone()
            row_count = stats[0] if stats and stats[0] else None
            last_analyzed = stats[1].strftime("%Y-%m-%d %H:%M:%S") if stats and stats[1] else None
            
            table_info = TableInfo(
                table_name=table_name,
                column_count=len(columns),
                row_count=row_count,
                last_updated=last_analyzed,
                columns=columns
            )
            
            # Validate structure based on table type
            structure_valid = self._validate_table_structure(table_name, columns)
            
            return TableCheckResult(
                table_name=table_name,
                exists=True,
                structure_valid=structure_valid,
                table_info=table_info
            )
            
        except Exception as e:
            return TableCheckResult(
                table_name=table_name,
                exists=False,
                structure_valid=False,
                error_message=str(e)
            )
    
    def _validate_table_structure(self, table_name: str, columns: List[Dict[str, Any]]) -> bool:
        """Validate table structure based on expected schema."""
        column_names = [col["name"] for col in columns]
        
        # Basic validation for each table type
        if table_name == "TICKER_DATA":
            required_columns = ["TICKER", "TRADE_DATE", "OPEN_PRICE", "HIGH_PRICE", "LOW_PRICE", "CLOSE_PRICE", "VOLUME"]
            return all(col in column_names for col in required_columns)
        
        elif table_name == "MACRO_DATA":
            required_columns = ["TRADE_DATE", "SERIES_ID", "VALUE"]
            return all(col in column_names for col in required_columns)
        
        elif table_name == "TRANSFORMED_TICKERS":
            # Should have many technical indicator columns
            return len(columns) > 50  # Basic check for transformed table
            
        elif table_name == "TRANSFORMED_MACRO_DATA":
            required_columns = ["TRANS_DATE", "SERIES_ID", "VALUE", "NORMALIZED_VALUE"]
            return all(col in column_names for col in required_columns)
        
        return True  # Unknown table, assume valid

def load_connection_config_from_vault() -> ConnectionConfig:
    """Load database connection configuration from vault."""
    try:
        user = get_secret("ocid1.vaultsecret.oc1.iad.amaaaaaacwmkemyaa6qj7ezmhqtuqxjn7f54xbpfalsszppuhoo6zilyxzra")
        password = get_secret("ocid1.vaultsecret.oc1.iad.amaaaaaacwmkemyaf63vh4v5e3ozokfp32cyk2ct6qjo5eb6z5mutkoppl4q")
        dsn = get_secret("ocid1.vaultsecret.oc1.iad.amaaaaaacwmkemyacow4ffwcgtu6uwsw5ujw5w64eeashh5ndeldxzy3y52q")
        
        if not all([user, password, dsn]):
            raise ValueError("Missing required database credentials from vault")
        
        return ConnectionConfig(
            user=user,
            password=password,
            dsn=dsn,
            config_dir="/opt/oracle/instantclient/network/admin" if os.path.exists("/opt/oracle/instantclient/network/admin") else None
        )
        
    except Exception as e:
        raise ValueError(f"Failed to load database configuration: {e}")

def main():
    """Main function for database connection testing."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Database connection testing utility")
    parser.add_argument("--check-tables", action="store_true", help="Check CandleThrob tables")
    parser.add_argument("--health-check", action="store_true", help="Perform comprehensive health check")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    try:
        # Load configuration
        config = load_connection_config_from_vault()
        tester = DatabaseConnectionTester(config)
        
        if args.health_check:
            print("🔍 Performing comprehensive health check...")
            health_status = tester.get_health_status()
            
            print(f"\n📊 Health Status:")
            print(f"  Connection Status: {health_status['connection_health']['status']}")
            print(f"  Success Rate: {health_status['connection_health']['success_rate']:.2%}")
            print(f"  Current Connection: {'✅ Success' if health_status['current_connection']['success'] else '❌ Failed'}")
            
            if health_status['current_connection']['success']:
                print(f"\n📋 Table Status:")
                table_status = health_status['table_status']
                print(f"  Expected Tables: {table_status['total_expected']}")
                print(f"  Existing Tables: {len(table_status['existing_tables'])}")
                print(f"  Missing Tables: {len(table_status['missing_tables'])}")
                
                for table_name in CANDLETHROB_TABLES:
                    if table_name in table_status['table_details']:
                        result = table_status['table_details'][table_name]
                        status = "✅ EXISTS" if result.exists else "❌ MISSING"
                        print(f"    {table_name}: {status}")
                        
                        if result.exists and result.table_info:
                            info = result.table_info
                            print(f"      Columns: {info.column_count}, Rows: {info.row_count or 'Unknown'}")
            
        elif args.check_tables:
            print("🔍 Checking CandleThrob tables...")
            table_status = tester.check_candlethrob_tables()
            
            print(f"\n📋 Table Check Results:")
            print(f"  Expected Tables: {table_status['total_expected']}")
            print(f"  Existing Tables: {len(table_status['existing_tables'])}")
            print(f"  Missing Tables: {len(table_status['missing_tables'])}")
            
            for table_name in CANDLETHROB_TABLES:
                if table_name in table_status['table_details']:
                    result = table_status['table_details'][table_name]
                    status = "✅ EXISTS" if result.exists else "❌ MISSING"
                    print(f"    {table_name}: {status}")
                    
                    if result.exists and result.table_info and args.verbose:
                        info = result.table_info
                        print(f"      Columns: {info.column_count}, Rows: {info.row_count or 'Unknown'}")
                        if info.columns:
                            print(f"      Sample columns: {[col['name'] for col in info.columns[:5]]}")
        
        else:
            # Basic connection test
            print("🔍 Testing database connection...")
            result = tester.test_connection()
            
            if result.success:
                print("✅ Database connection successful!")
                print(f"   Duration: {result.duration:.3f}s")
                if result.connection_info:
                    print(f"   Version: {result.connection_info.get('version', 'Unknown')}")
            else:
                print("❌ Database connection failed!")
                print(f"   Error: {result.error_message}")
                sys.exit(1)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()