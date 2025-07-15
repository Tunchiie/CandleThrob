# monitor_ingestion.py Documentation

## Overview

The `monitor_ingestion.py` module provides monitoring capabilities for the CandleThrob data ingestion pipeline. It offers comprehensive performance monitoring, data quality validation, alerting, and audit trail functionality to ensure reliable and high-quality data ingestion operations.

## Features

### Core Capabilities
- **Performance Monitoring**: Real-time performance metrics and resource tracking
- **Data Quality Validation**: Multi-layer data quality assessment and scoring
- **Alert System**: Automated alerting for critical issues and anomalies
- **Audit Trail**: Complete operation history and data lineage tracking
- **Health Checks**: Comprehensive system health and data freshness monitoring
- **Resource Monitoring**: Memory, CPU, and database connection monitoring
- **Error Handling**: Advanced error categorization and recovery
- **Modular Design**: Testable, configurable monitoring components

## Architecture

### Design Philosophy
- **Advanced **: Production-ready with comprehensive monitoring
- **Modular**: Testable components with clear separation of concerns
- **Configurable**: Flexible configuration for different environments
- **Reliable**: Robust error handling and retry mechanisms
- **Observable**: Extensive logging and metrics collection

### Monitoring Flow
```
Data Ingestion → Performance Monitoring → Quality Validation → 
Health Checks → Alert Generation → Audit Trail → Reporting
```

## Core Classes

### AuditTrail

**Purpose**: audit trail for monitoring actions and data lineage.

```python
class AuditTrail:
    def __init__(self):
        self.entries: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
```

**Features**:
- **Thread-Safe**: Lock-protected audit entry management
- **Comprehensive**: Complete operation history tracking
- **Structured**: Standardized audit entry format
- **Queryable**: Easy access to audit history and summaries

**Methods**:
- `log_event(event_type, details, success)`: Log monitoring events
- `get_audit_summary()`: Generate audit trail summary

### IngestionMonitor

**Purpose**: Main monitoring orchestrator with comprehensive health checks.

```python
class IngestionMonitor:
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.db = OracleDB()
        self.engine = self.db.get_sqlalchemy_engine()
        self.audit_trail = AuditTrail()
        self.config = config or {
            "data_freshness_hours": 24,
            "success_rate_threshold": 0.9,
            "batch_failure_threshold": 3,
            "data_gap_threshold": 5
        }
```

**Configuration Options**:
- `data_freshness_hours`: Maximum age of data before considered stale
- `success_rate_threshold`: Minimum success rate for batch operations
- `batch_failure_threshold`: Maximum consecutive batch failures
- `data_gap_threshold`: Maximum days of missing data

## Monitoring Functions

### check_data_freshness()

**Purpose**: Verify data freshness and identify stale data issues.

**Features**:
- **Ticker Data**: Check latest ticker data timestamp
- **Macro Data**: Check latest macroeconomic data timestamp
- **Age Calculation**: Calculate hours since last update
- **Freshness Validation**: Compare against configurable thresholds
- **Audit Logging**: Complete audit trail of freshness checks

**Returns**:
```python
{
    "ticker_data": {
        "latest_date": datetime,
        "hours_old": float,
        "is_fresh": bool
    },
    "macro_data": {
        "latest_date": datetime,
        "hours_old": float,
        "is_fresh": bool
    },
    "check_timestamp": str
}
```

### check_batch_performance()

**Purpose**: Analyze batch processing performance from recent runs.

**Features**:
- **Batch Results**: Parse recent batch result files
- **Performance Metrics**: Calculate success rates and timing
- **Trend Analysis**: Identify performance trends and anomalies
- **Threshold Monitoring**: Alert on performance degradation
- **Historical Data**: Track performance over time

**Returns**:
```python
{
    "total_batches": int,
    "successful_batches": int,
    "failed_batches": int,
    "success_rate": float,
    "average_duration": float,
    "performance_trend": str,
    "alerts": List[str]
}
```

### check_data_quality()

**Purpose**: Validate data quality and identify quality issues.

**Features**:
- **Schema Validation**: Verify data structure compliance
- **Data Completeness**: Check for missing values and gaps
- **Data Integrity**: Validate OHLCV relationships
- **Quality Scoring**: Calculate overall data quality score
- **Issue Identification**: Flag specific quality problems

**Returns**:
```python
{
    "overall_quality_score": float,
    "ticker_quality": {
        "completeness": float,
        "integrity": float,
        "freshness": float
    },
    "macro_quality": {
        "completeness": float,
        "integrity": float,
        "freshness": float
    },
    "quality_issues": List[str],
    "recommendations": List[str]
}
```

## Utility Functions

### retry_on_failure()

**Purpose**: retry decorator with exponential backoff.

```python
def retry_on_failure(max_retries: int = 3, delay_seconds: float = 1.0):
    """retry decorator with exponential backoff."""
```

**Features**:
- **Exponential Backoff**: Increasing delays between retries
- **Configurable**: Adjustable retry count and delay
- **Error Logging**: Comprehensive retry attempt logging
- **Exception Handling**: Proper exception propagation

### monitor_performance()

**Purpose**: Decorator for performance and resource monitoring.

```python
def monitor_performance(operation: str):
    """Decorator for performance and resource monitoring."""
```

**Features**:
- **Timing**: Operation duration tracking
- **Memory Usage**: Memory consumption monitoring
- **Success Tracking**: Operation success/failure logging
- **Structured Logging**: JSON-formatted performance metrics

## Alert System

### Alert Types

1. **Data Freshness Alerts**
   - Stale ticker data
   - Outdated macro data
   - Missing recent updates

2. **Performance Alerts**
   - Low success rates
   - Slow processing times
   - High failure counts

3. **Quality Alerts**
   - Poor data quality scores
   - Data integrity issues
   - Schema violations

4. **System Alerts**
   - Database connection issues
   - Memory usage problems
   - Resource exhaustion

### Alert Generation

```python
def generate_alert(self, alert_type: str, message: str, details: Dict[str, Any]) -> None:
    """Generate and send alerts for critical issues."""
```

**Features**:
- **Alert Categorization**: Different handling for different alert types
- **Escalation**: Automatic escalation for critical issues
- **Notification**: Email and logging notifications
- **Audit Trail**: Complete alert history tracking

## Configuration

### Environment Variables
```bash
# Monitoring Configuration
DATA_FRESHNESS_HOURS=24
SUCCESS_RATE_THRESHOLD=0.9
BATCH_FAILURE_THRESHOLD=3
DATA_GAP_THRESHOLD=5

# Alert Configuration
ALERT_EMAIL_ENABLED=true
ALERT_EMAIL_RECIPIENTS=admin@company.com
ALERT_LOG_LEVEL=WARNING

# Performance Configuration
MONITORING_INTERVAL_SECONDS=300
MAX_MEMORY_USAGE_MB=2048
MAX_CPU_USAGE_PERCENT=80
```

### Custom Configuration
```python
config = {
    "data_freshness_hours": 24,
    "success_rate_threshold": 0.9,
    "batch_failure_threshold": 3,
    "data_gap_threshold": 5,
    "alert_email_enabled": True,
    "alert_email_recipients": ["admin@company.com"],
    "monitoring_interval_seconds": 300
}

monitor = IngestionMonitor(config)
```

## Usage Examples

### Basic Monitoring
```python
from CandleThrob.ingestion.monitor_ingestion import IngestionMonitor

# Initialize monitor with default configuration
monitor = IngestionMonitor()

# Run comprehensive monitoring checks
results = monitor.run_monitoring_checks()
print(f"Monitoring results: {results}")
```

### Custom Configuration
```python
from CandleThrob.ingestion.monitor_ingestion import IngestionMonitor

# Custom configuration
config = {
    "data_freshness_hours": 12,  # More strict freshness requirement
    "success_rate_threshold": 0.95,  # Higher success rate requirement
    "batch_failure_threshold": 2,  # Lower failure tolerance
    "alert_email_enabled": True,
    "alert_email_recipients": ["data-team@company.com"]
}

monitor = IngestionMonitor(config)
results = monitor.run_monitoring_checks()
```

### Individual Health Checks
```python
from CandleThrob.ingestion.monitor_ingestion import IngestionMonitor

monitor = IngestionMonitor()

# Check data freshness
freshness = monitor.check_data_freshness()
print(f"Data freshness: {freshness}")

# Check batch performance
performance = monitor.check_batch_performance()
print(f"Batch performance: {performance}")

# Check data quality
quality = monitor.check_data_quality()
print(f"Data quality: {quality}")
```

### Scheduled Monitoring
```python
import time
from CandleThrob.ingestion.monitor_ingestion import IngestionMonitor

monitor = IngestionMonitor()

# Run monitoring every 5 minutes
while True:
    try:
        results = monitor.run_monitoring_checks()
        print(f"Monitoring check completed: {results}")
    except Exception as e:
        print(f"Monitoring check failed: {e}")
    
    time.sleep(300)  # Wait 5 minutes
```

## Integration

### Kestra Workflow Integration
```yaml
- id: monitor_ingestion
  type: io.kestra.plugin.docker.Run
  containerImage: candlethrob:latest
  command: ["sh", "-c", "cd /app && PYTHONPATH=/app python3 /app/CandleThrob/ingestion/monitor_ingestion.py"]
  env:
    DATA_FRESHNESS_HOURS: "24"
    SUCCESS_RATE_THRESHOLD: "0.9"
    ALERT_EMAIL_ENABLED: "true"
  timeout: PT10M
```

### Database Integration
- **Oracle Database**: Direct connection for data freshness checks
- **SQL Queries**: Optimized queries for performance monitoring
- **Transaction Management**: Proper connection handling
- **Error Recovery**: Graceful handling of database issues

## Troubleshooting

### Common Issues

1. **Database Connection Errors**
   - Verify Oracle wallet configuration
   - Check database credentials and permissions
   - Ensure network connectivity

2. **Performance Issues**
   - Monitor memory and CPU usage
   - Check database query performance
   - Review monitoring interval settings

3. **Alert Configuration**
   - Verify email server settings
   - Check alert threshold configurations
   - Review alert recipient lists

4. **Data Quality Issues**
   - Review data quality thresholds
   - Check data source reliability
   - Validate data processing pipeline

### Debug Mode
```python
import logging
logging.getLogger('CandleThrob.ingestion.monitor_ingestion').setLevel(logging.DEBUG)

# Run with detailed logging
monitor = IngestionMonitor()
results = monitor.run_monitoring_checks()
```

## Best Practices

### Production Deployment
1. **Monitoring Schedule**: Regular monitoring intervals
2. **Alert Configuration**: Appropriate alert thresholds
3. **Performance Optimization**: Efficient monitoring queries
4. **Error Handling**: Comprehensive error recovery
5. **Audit Trail**: Complete operation history

### Development Guidelines
1. **Testing**: Comprehensive unit and integration testing
2. **Documentation**: Keep monitoring documentation current
3. **Configuration**: Use environment variables for configuration
4. **Logging**: Structured logging with appropriate levels
5. **Security**: Secure handling of credentials and alerts

### Monitoring Strategy
1. **Proactive Monitoring**: Identify issues before they become critical
2. **Trend Analysis**: Track performance trends over time
3. **Alert Escalation**: Appropriate escalation for different issues
4. **Data Quality**: Continuous data quality assessment
5. **Performance Optimization**: Regular performance review and optimization

---

**Author**: Adetunji Fasiku 