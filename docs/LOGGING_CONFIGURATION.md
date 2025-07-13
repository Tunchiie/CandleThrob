# Logging Configuration - CandleThrob

## Overview

CandleThrob uses a centralized logging configuration that separates ingestion operations from database operations. This ensures better organization, debugging, and monitoring of different types of operations.

## Logging Architecture

### Separate Log Files

The system uses two separate log files:

1. **Ingestion Logs**: `candlethrob_ingestion_YYYYMMDD_HHMMSS.log`
   - API calls and data fetching operations
   - Rate limiting and retry logic
   - Batch processing progress
   - Data validation and cleaning

2. **Database Logs**: `candlethrob_database_YYYYMMDD_HHMMSS.log`
   - Database connection management
   - Table creation and schema operations
   - Bulk insert operations
   - Query execution and results

### Log File Locations

```
/app/logs/
├── candlethrob_ingestion_20250713_143022.log  # Ingestion operations
└── candlethrob_database_20250713_143022.log   # Database operations
```

## Usage

### For Ingestion Operations

```python
from CandleThrob.utils.logging_config import (
    get_ingestion_logger,
    log_ingestion_start,
    log_ingestion_success,
    log_ingestion_error,
    log_rate_limiting,
    log_batch_progress
)

# Get logger for ingestion operations
logger = get_ingestion_logger(__name__)

# Log ingestion events
log_ingestion_start("fetch_data", "AAPL", "Batch 1/21")
log_ingestion_success("fetch_data", "AAPL", 252)
log_ingestion_error("fetch_data", "INVALID", "API error")
log_rate_limiting("fetch_data", 12.5)
log_batch_progress("ingest_data", 5, 25, "AAPL")
```

### For Database Operations

```python
from CandleThrob.utils.logging_config import (
    get_database_logger,
    log_database_operation
)

# Get logger for database operations
logger = get_database_logger(__name__)

# Log database events
log_database_operation("create_table", "ticker_data")
log_database_operation("insert", "ticker_data", 1000)
log_database_operation("query", "ticker_data", 50)
log_database_operation("create_table", "macro_data", error="Table already exists")
```

## Log Format

Both log files use the same consistent format:

```
2025-07-13 14:30:22 - ingestion.fetch_data - INFO - [fetch_data] Starting processing for AAPL (Batch 1/21)
2025-07-13 14:30:23 - ingestion.fetch_data - INFO - [fetch_data] Successfully processed AAPL - 252 records
2025-07-13 14:30:24 - database.models - INFO - [DATABASE] insert completed on ticker_data - 1000 records
2025-07-13 14:30:25 - database.oracle_conn - INFO - [DATABASE] connect completed on Oracle DB
```

## Module-Specific Logging

### Ingestion Modules

The following modules use ingestion logging:

- `ingestion/fetch_data.py` - API data fetching
- `ingestion/ingest_data.py` - Batch processing
- `ingestion/enrich_data.py` - Data transformation
- `ingestion/update_macro.py` - Macro data updates

### Database Modules

The following modules use database logging:

- `utils/models.py` - Database models and operations
- `utils/oracle_conn.py` - Database connections
- `utils/db_ping.py` - Database health checks

## Configuration

### Automatic Configuration

Logging is automatically configured when the `logging_config.py` module is imported:

```python
# Initialize logging when module is imported
setup_ingestion_logging()
setup_database_logging()
```

### Manual Configuration

You can manually configure logging with custom settings:

```python
from CandleThrob.utils.logging_config import setup_ingestion_logging, setup_database_logging

# Custom ingestion logging
setup_ingestion_logging("/custom/path/ingestion.log", logging.DEBUG)

# Custom database logging
setup_database_logging("/custom/path/database.log", logging.INFO)
```

## Log Levels

### Ingestion Logs
- **INFO**: Normal operation messages
- **WARNING**: Rate limiting, retries, data validation issues
- **ERROR**: API failures, data processing errors

### Database Logs
- **INFO**: Successful operations, connection management
- **WARNING**: Connection issues, performance warnings
- **ERROR**: Database errors, transaction failures

## Monitoring

### Real-Time Monitoring

You can monitor logs in real-time:

```bash
# Monitor ingestion logs
tail -f /app/logs/candlethrob_ingestion_*.log

# Monitor database logs
tail -f /app/logs/candlethrob_database_*.log

# Monitor both logs
tail -f /app/logs/candlethrob_*.log
```

### Log Analysis

Common patterns to look for:

**Ingestion Logs:**
- `[fetch_data] Starting processing for` - API calls starting
- `[fetch_data] Successfully processed` - Successful data fetching
- `[fetch_data] Error processing` - API or data errors
- `Rate limiting: sleeping` - Rate limit compliance

**Database Logs:**
- `[DATABASE] insert completed` - Successful bulk inserts
- `[DATABASE] create_table` - Table creation/verification
- `[DATABASE] connect completed` - Database connections
- `[DATABASE] * failed` - Database errors

## Troubleshooting

### Common Issues

1. **No Log Files Created**
   - Check that `/app/logs/` directory exists and is writable
   - Verify that logging configuration is imported

2. **Logs Not Appearing**
   - Ensure the correct logger is being used (ingestion vs database)
   - Check log level settings
   - Verify file permissions

3. **Duplicate Log Entries**
   - Check for multiple logging configuration calls
   - Ensure proper logger hierarchy

### Debug Mode

Enable debug logging for troubleshooting:

```python
import logging
from CandleThrob.utils.logging_config import setup_ingestion_logging, setup_database_logging

# Enable debug logging
setup_ingestion_logging(level=logging.DEBUG)
setup_database_logging(level=logging.DEBUG)
```

## Integration with Kestra

The logging configuration works seamlessly with Kestra workflows:

```yaml
- id: process_all_batches
  type: io.kestra.plugin.docker.Run
  containerImage: candlethrob:latest
  command: ["sh", "-c", "cd /app && PYTHONPATH=/app python3 /app/CandleThrob/ingestion/ingest_data.py"]
  volumes:
    - "/tmp/kestra-wd:/app"
    - "/home/ubuntu/stockbot/logs:/app/logs"  # Mount logs directory
```

## Performance Considerations

### Log File Rotation

Consider implementing log rotation for production:

```python
from logging.handlers import RotatingFileHandler

# Rotate logs when they reach 100MB, keep 5 backup files
handler = RotatingFileHandler(
    "/app/logs/candlethrob_ingestion.log",
    maxBytes=100*1024*1024,  # 100MB
    backupCount=5
)
```

### Log Level Optimization

For production, use appropriate log levels:

- **Development**: `logging.DEBUG` for detailed debugging
- **Production**: `logging.INFO` for normal operations
- **High Volume**: `logging.WARNING` to reduce log volume

## Best Practices

1. **Use Structured Logging**: Use the provided logging functions for consistent formatting
2. **Include Context**: Always include relevant context (ticker, batch info, etc.)
3. **Error Handling**: Log errors with sufficient detail for debugging
4. **Performance Monitoring**: Use logs to monitor processing times and success rates
5. **Security**: Avoid logging sensitive information (credentials, personal data)

## Example Output

### Ingestion Log Example
```
2025-07-13 14:30:22 - ingestion.fetch_data - INFO - [fetch_data] Starting processing for AAPL
2025-07-13 14:30:23 - ingestion.fetch_data - INFO - Fetching Polygon.io daily OHLCV data for AAPL
2025-07-13 14:30:24 - ingestion.fetch_data - INFO - [fetch_data] Successfully processed AAPL - 252 records
2025-07-13 14:30:25 - ingestion.ingest_data - INFO - [ingest_data] Rate limiting: sleeping 12.00 seconds
2025-07-13 14:30:37 - ingestion.ingest_data - INFO - [ingest_data] Processing 1/25: MMM
```

### Database Log Example
```
2025-07-13 14:30:22 - database.oracle_conn - INFO - [DATABASE] create_engine completed on Oracle DB
2025-07-13 14:30:23 - database.oracle_conn - INFO - [DATABASE] connect completed on Oracle DB
2025-07-13 14:30:24 - database.models - INFO - [DATABASE] create_table completed on ticker_data
2025-07-13 14:30:25 - database.models - INFO - [DATABASE] insert completed on ticker_data - 252 records
2025-07-13 14:30:26 - database.oracle_conn - INFO - Oracle DB connection closed
```

---

**Last Updated**: July 13, 2025  
**Version**: 2.0.0  
**Author**: CandleThrob Team 