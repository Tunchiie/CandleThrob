# oracledb.py Documentation

## Overview
The `oracledb.py` module provides Oracle database connectivity and session management for the CandleThrob financial data pipeline. It offers a clean, object-oriented interface for establishing, managing, and testing Oracle database connections using SQLAlchemy ORM with the cx_Oracle driver.

## Architecture

### Design Philosophy
- **Enterprise Integration**: Designed for Oracle database environments
- **Session Management**: Proper connection lifecycle management
- **Error Handling**: Robust error handling and reporting
- **Configuration**: Environment-based configuration management
- **Testing**: Built-in connection testing capabilities

### Database Connectivity
- **Driver**: cx_Oracle for Oracle database connectivity
- **ORM**: SQLAlchemy for object-relational mapping
- **Session Management**: Explicit session creation and cleanup
- **Connection Pooling**: Leverages SQLAlchemy's connection pooling

## Class: OracleDB

### Purpose
The `OracleDB` class encapsulates Oracle database connectivity, providing methods for session management, connection testing, and graceful cleanup.

### Constructor
```python
def __init__(self):
    """
    Initialize the OracleDB class.
    This class provides methods to create and manage a SQLAlchemy session for Oracle DB.
    """
    self.session = None
```

**Initialization:**
- Sets session to None initially
- No immediate connection establishment
- Lazy connection pattern for resource efficiency

### Core Methods

#### get_oracledb_session() -> None
**Purpose:** Establishes connection to Oracle database and creates SQLAlchemy session.

**Environment Variables Required:**
```python
ORACLE_USER = os.getenv('ORACLE_USER')           # Database username
ORACLE_PASSWORD = os.getenv('ORACLE_PASSWORD')   # Database password  
ORACLE_HOST = os.getenv('ORACLE_HOST')           # Database host/IP
ORACLE_PORT = os.getenv('ORACLE_PORT', '1521')  # Database port (default: 1521)
ORACLE_SERVICE_NAME = os.getenv('ORACLE_SERVICE_NAME')  # Service name
```

**Connection String Format:**
```python
DATABASE_URL = f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE_NAME}'
```

**Process Flow:**
1. **Environment Validation**: Check all required environment variables
2. **Connection String Construction**: Build Oracle connection URL
3. **Engine Creation**: Create SQLAlchemy engine with echo=True for debugging
4. **Session Factory**: Create sessionmaker bound to engine
5. **Session Instantiation**: Create and store session instance

**Error Handling:**
```python
if not all([ORACLE_USER, ORACLE_PASSWORD, ORACLE_HOST, ORACLE_SERVICE_NAME]):
    raise ValueError("Missing required Oracle DB environment variables.")
```

**Configuration Options:**
- **Echo Mode**: SQL logging enabled by default for debugging
- **Default Port**: 1521 (standard Oracle port)
- **Service Name**: Oracle service identifier for connection routing

#### close_oracledb_session() -> None
**Purpose:** Properly closes the Oracle database session and releases resources.

```python
def close_oracledb_session(self):
    """
    Close the SQLAlchemy session for Oracle DB.
    """
    if self.session:
        self.session.close()
        print("Oracle DB session closed.")
    else:
        print("No session to close.")
```

**Features:**
- **Safe Closure**: Checks for existing session before closing
- **Resource Cleanup**: Properly releases database connections
- **Status Feedback**: Provides confirmation messages
- **Idempotent**: Safe to call multiple times

**Best Practices:**
- Always call in finally blocks or context managers
- Essential for connection pool management
- Prevents connection leaks in long-running applications

#### test_connection() -> None
**Purpose:** Tests Oracle database connectivity and validates configuration.

```python
def test_connection(self):
    """
    Test the connection to the Oracle database.
    """
    try:
        self.session.execute("SELECT 1 FROM dual")
        print("Connection to Oracle DB successful.")
    except Exception as e:
        print(f"Error connecting to Oracle DB: {e}")
    finally:
        self.close_oracledb_session()
```

**Test Query:**
- **`SELECT 1 FROM dual`**: Standard Oracle connectivity test
- **dual**: Oracle's dummy table for testing
- **Minimal Overhead**: Lightweight query for connection validation

**Error Scenarios:**
- Network connectivity issues
- Authentication failures
- Database service unavailability
- Configuration errors

## Environment Configuration

### Required Environment Variables

#### ORACLE_USER
- **Description**: Database username for authentication
- **Example**: `export ORACLE_USER="candlethrob_user"`
- **Security**: Store securely, never hardcode

#### ORACLE_PASSWORD
- **Description**: Database password for authentication
- **Example**: `export ORACLE_PASSWORD="secure_password"`
- **Security**: Use secrets management in production

#### ORACLE_HOST
- **Description**: Oracle database server hostname or IP address
- **Example**: `export ORACLE_HOST="oracle.company.com"`
- **Network**: Ensure network connectivity from application

#### ORACLE_PORT
- **Description**: Oracle database port (optional, defaults to 1521)
- **Example**: `export ORACLE_PORT="1521"`
- **Default**: Standard Oracle port 1521

#### ORACLE_SERVICE_NAME
- **Description**: Oracle service name for connection routing
- **Example**: `export ORACLE_SERVICE_NAME="ORCL"`
- **Configuration**: Must match Oracle database configuration

### Configuration Examples

#### Development Environment
```bash
export ORACLE_USER="dev_user"
export ORACLE_PASSWORD="dev_password"
export ORACLE_HOST="localhost"
export ORACLE_PORT="1521"
export ORACLE_SERVICE_NAME="XEPDB1"
```

#### Production Environment
```bash
export ORACLE_USER="prod_candlethrob"
export ORACLE_PASSWORD="${ORACLE_PASSWORD_SECRET}"
export ORACLE_HOST="oracle-prod.internal.company.com"
export ORACLE_PORT="1521"
export ORACLE_SERVICE_NAME="CANDLETHROBPROD"
```

#### Docker Environment
```yaml
# docker-compose.yml
environment:
  - ORACLE_USER=candlethrob_user
  - ORACLE_PASSWORD_FILE=/run/secrets/oracle_password
  - ORACLE_HOST=oracle-db
  - ORACLE_SERVICE_NAME=CANDLETHROB
```

## Usage Examples

### Basic Connection Management
```python
from utils.oracledb import OracleDB

# Initialize Oracle DB connection
oracle_db = OracleDB()

try:
    # Establish connection
    oracle_db.get_oracledb_session()
    
    # Use session for database operations
    result = oracle_db.session.execute("SELECT SYSDATE FROM dual")
    current_time = result.fetchone()[0]
    print(f"Oracle server time: {current_time}")
    
finally:
    # Always close session
    oracle_db.close_oracledb_session()
```

### Connection Testing
```python
from utils.oracledb import OracleDB

# Test Oracle connectivity
oracle_db = OracleDB()
oracle_db.get_oracledb_session()
oracle_db.test_connection()  # Automatically closes session
```

### Integration with Models
```python
from utils.oracledb import OracleDB
from utils.models import TickerData, Base

# Initialize database
oracle_db = OracleDB()
oracle_db.get_oracledb_session()

try:
    # Create tables if they don't exist
    Base.metadata.create_all(oracle_db.session.bind)
    
    # Insert ticker data
    ticker_data = TickerData(
        date=datetime.date.today(),
        ticker='AAPL',
        open=150.0,
        high=155.0,
        low=149.0,
        close=154.0,
        volume=1000000,
        year=2023,
        month=1,
        weekday=1
    )
    
    oracle_db.session.add(ticker_data)
    oracle_db.session.commit()
    
finally:
    oracle_db.close_oracledb_session()
```

### Context Manager Pattern
```python
from contextlib import contextmanager
from utils.oracledb import OracleDB

@contextmanager
def oracle_session():
    """Context manager for Oracle database sessions."""
    oracle_db = OracleDB()
    oracle_db.get_oracledb_session()
    try:
        yield oracle_db.session
    finally:
        oracle_db.close_oracledb_session()

# Usage with context manager
with oracle_session() as session:
    result = session.execute("SELECT COUNT(*) FROM raw_ticker_data")
    count = result.fetchone()[0]
    print(f"Total ticker records: {count}")
```

## Integration Points

### Database Schema
- **Tables**: Works with models defined in `utils/models.py`
- **Schema Creation**: Supports SQLAlchemy table creation
- **Migrations**: Compatible with Alembic for schema migrations
- **Constraints**: Supports Oracle-specific constraints and features

### Application Integration
- **Data Pipeline**: Used by ingestion modules for data persistence
- **Analytics**: Provides data access for analysis modules
- **API Layer**: Supports web application database needs
- **Batch Processing**: Enables large-scale data processing jobs

### Transaction Management
```python
# Explicit transaction control
oracle_db.get_oracledb_session()
try:
    oracle_db.session.begin()
    
    # Multiple database operations
    oracle_db.session.add(ticker_data_1)
    oracle_db.session.add(ticker_data_2)
    
    # Commit all changes
    oracle_db.session.commit()
    
except Exception as e:
    # Rollback on error
    oracle_db.session.rollback()
    raise e
    
finally:
    oracle_db.close_oracledb_session()
```

## Performance Considerations

### Connection Pooling
- **SQLAlchemy Pooling**: Automatic connection pool management
- **Pool Size**: Configurable through engine parameters
- **Connection Reuse**: Efficient resource utilization
- **Timeout Handling**: Automatic cleanup of stale connections

### Query Optimization
- **Echo Mode**: SQL logging for performance analysis
- **Prepared Statements**: Automatic parameterization via SQLAlchemy
- **Batch Operations**: Support for bulk inserts and updates
- **Index Utilization**: Optimized queries through proper ORM usage

### Resource Management
```python
# Engine configuration for production
engine = create_engine(
    DATABASE_URL,
    echo=False,  # Disable in production
    pool_size=10,  # Connection pool size
    max_overflow=20,  # Additional connections
    pool_pre_ping=True,  # Validate connections
    pool_recycle=3600  # Recycle connections hourly
)
```

## Security Considerations

### Authentication
- **Environment Variables**: Never hardcode credentials
- **Secrets Management**: Use secure secret storage in production
- **Principle of Least Privilege**: Minimal required database permissions
- **Connection Encryption**: Use encrypted connections when possible

### Database Permissions
**Recommended Oracle Permissions:**
```sql
-- Create user
CREATE USER candlethrob_user IDENTIFIED BY secure_password;

-- Grant necessary privileges
GRANT CONNECT TO candlethrob_user;
GRANT RESOURCE TO candlethrob_user;
GRANT CREATE TABLE TO candlethrob_user;
GRANT CREATE INDEX TO candlethrob_user;

-- Grant specific table access
GRANT SELECT, INSERT, UPDATE, DELETE ON raw_ticker_data TO candlethrob_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON raw_macro_data TO candlethrob_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON transformed_ticker_data TO candlethrob_user;
```

### Network Security
- **VPN/Private Networks**: Secure network connectivity
- **Firewall Rules**: Restrict database access to authorized IPs
- **SSL/TLS**: Encrypted data transmission
- **Certificate Validation**: Verify database server certificates

## Error Handling and Debugging

### Common Connection Issues

#### Missing Environment Variables
```
ValueError: Missing required Oracle DB environment variables.
```
**Solutions:**
- Verify all required environment variables are set
- Check variable names for typos
- Ensure variables are available in application environment

#### cx_Oracle Installation Issues
```
ModuleNotFoundError: No module named 'cx_Oracle'
```
**Solutions:**
```bash
# Install cx_Oracle
pip install cx_Oracle

# Or use python-oracledb (newer Oracle driver)
pip install oracledb
```

#### Oracle Client Library Issues
```
DPI-1047: Cannot locate a 64-bit Oracle Client library
```
**Solutions:**
- Install Oracle Instant Client
- Set ORACLE_HOME environment variable
- Add Oracle client to system PATH

#### Network Connectivity
```
ORA-12170: TNS:Connect timeout occurred
```
**Solutions:**
- Verify network connectivity to Oracle server
- Check firewall settings
- Validate Oracle listener configuration
- Test with Oracle tools (SQL*Plus, SQL Developer)

### Debug Commands
```python
# Test individual components
import os
print("Environment variables:")
for var in ['ORACLE_USER', 'ORACLE_HOST', 'ORACLE_SERVICE_NAME']:
    print(f"{var}: {os.getenv(var, 'NOT SET')}")

# Test cx_Oracle driver
try:
    import cx_Oracle
    print(f"cx_Oracle version: {cx_Oracle.version}")
except ImportError as e:
    print(f"cx_Oracle import error: {e}")

# Test connection string construction
from utils.oracledb import OracleDB
oracle_db = OracleDB()
# Add debug prints in get_oracledb_session method
```

## Best Practices

### Development
1. **Environment Separation**: Use different credentials for dev/test/prod
2. **Connection Testing**: Always test connections before deployment
3. **Error Handling**: Implement comprehensive error handling
4. **Logging**: Use appropriate logging levels for different environments

### Production
1. **Connection Pooling**: Configure appropriate pool sizes
2. **Monitoring**: Monitor connection usage and performance
3. **Backup Strategy**: Regular database backups
4. **Security Auditing**: Regular security reviews and updates

### Code Quality
1. **Resource Cleanup**: Always close sessions properly
2. **Exception Handling**: Handle database-specific exceptions
3. **Testing**: Unit tests for database operations
4. **Documentation**: Keep connection documentation current

## Troubleshooting

### Diagnostic Steps
1. **Environment Check**: Verify all environment variables
2. **Network Test**: Test connectivity to Oracle host/port
3. **Oracle Client**: Verify Oracle client installation
4. **Database Status**: Check Oracle database service status
5. **Permissions**: Validate user permissions and grants

### Common Solutions
```python
# Connection timeout issues
engine = create_engine(
    DATABASE_URL,
    connect_args={
        'timeout': 30,  # Connection timeout
        'retry_count': 3  # Retry attempts
    }
)

# Character encoding issues
engine = create_engine(
    DATABASE_URL,
    connect_args={
        'encoding': 'UTF-8',
        'nencoding': 'UTF-8'
    }
)
```

## Future Enhancements

### Potential Improvements
1. **Async Support**: Add asyncio support for concurrent operations
2. **Health Checks**: Implement comprehensive health monitoring
3. **Metrics Collection**: Add performance metrics and monitoring
4. **Configuration Management**: Enhanced configuration management
5. **High Availability**: Support for Oracle RAC and failover
6. **Modern Drivers**: Migration to python-oracledb driver
