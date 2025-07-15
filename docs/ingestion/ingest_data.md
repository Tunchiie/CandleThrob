# ingest_data.py Documentation

## Overview

The `ingest_data.py` module provides data ingestion capabilities for the CandleThrob financial data pipeline. It processes all S&P 500 and ETF tickers in a single execution with advanced features including stateful batch processing, advanced rate limiting, comprehensive error handling, and incremental data loading.

## Features

### Core Capabilities
- **Complete Processing**: All 523 tickers (503 S&P 500 + 20 ETFs) in one execution
- **Stateful Batching**: Automatic batch progression with state management
- **Advanced Rate Limiting**: Polygon.io API compliance with adaptive backoff
- **Incremental Loading**: Only fetches new data after last available date
- **Comprehensive Error Handling**: Retry logic with exponential backoff
- **Oracle Integration**: Bulk database operations with transaction management
- **Performance Monitoring**: Real-time metrics and resource tracking
- **Data Quality Validation**: Multi-layer validation with quality scoring
- **Memory Optimization**: Efficient memory management for large datasets
- **Audit Trail**: Complete data lineage and operation tracking

## Architecture

### Design Philosophy
- **Advanced **: Production-ready with comprehensive error handling
- **Scalable**: Handles large datasets with optimized memory usage
- **Reliable**: Robust error recovery and data validation
- **Monitorable**: Extensive logging and performance metrics
- **Configurable**: Flexible configuration for different environments

### Pipeline Flow
```
S&P 500 + ETF Tickers → Batch Processing → Rate Limiting → Data Fetching → 
Quality Validation → Database Storage → Progress Tracking → Completion
```

## Core Classes

### AdvancedBatchConfig

**Purpose**: configuration management for batch processing parameters.

```python
@dataclass
class AdvancedBatchConfig:
    batch_size: int = 25
    rate_limit_seconds: int = 12
    max_retries: int = 3
    retry_delay_seconds: int = 60
    max_processing_time_seconds: float = 7200.0  # 2 hours
    max_memory_usage_mb: float = 2048.0
    parallel_processing: bool = True
    max_workers: int = 4
    min_data_quality_score: float = 0.8
    enable_audit_trail: bool = True
    enable_performance_monitoring: bool = True
    enable_memory_optimization: bool = True
    enable_data_lineage: bool = True
```

**Key Features**:
- **Validation**: Automatic parameter validation in `__post_init__`
- **Flexibility**: Configurable for different environments
- **Safety**: Built-in limits and constraints
- **Monitoring**: Performance and quality thresholds

### PerformanceMonitor

**Purpose**: performance monitoring for ingestion operations.

```python
class PerformanceMonitor:
    def __init__(self):
        self.metrics: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
    
    @contextmanager
    def monitor_operation(self, operation_name: str):
        """Context manager for operation monitoring."""
```

**Features**:
- **Thread-Safe**: Lock-protected metrics collection
- **Comprehensive**: Duration, memory usage, success/failure tracking
- **Context Manager**: Easy integration with existing code
- **Real-Time**: Immediate metric availability

### RateLimiter

**Purpose**: Advanced rate limiter for Polygon.io API calls with adaptive features.

```python
class RateLimiter:
    def __init__(self, calls_per_minute: int = 5):
        self.calls_per_minute = calls_per_minute
        self.seconds_between_calls = 60.0 / calls_per_minute
        self.last_call_time = 0.0
        self.consecutive_failures = 0
        self.adaptive_delay = self.seconds_between_calls
        self._lock = threading.Lock()
```

**Advanced Features**:
- **Token Bucket Algorithm**: Efficient rate limiting implementation
- **Adaptive Backoff**: Increases delay on consecutive failures
- **Thread-Safe**: Lock-protected for concurrent operations
- **Exponential Backoff**: Intelligent retry strategy
- **Failure Recovery**: Automatic delay adjustment

### DataQualityValidator

**Purpose**: Multi-layer data quality validation with scoring.

```python
class DataQualityValidator:
    @staticmethod
    def validate_ticker_data(df: pd.DataFrame) -> Tuple[bool, float, List[str]]:
        """Validate ticker data quality and return score."""
```

**Validation Criteria**:
- **Schema Compliance**: Required columns and data types
- **Data Integrity**: OHLCV relationship validation
- **Completeness**: Missing value detection
- **Range Validation**: Reasonable price and volume ranges
- **Quality Scoring**: 0.0-1.0 quality score

### CompleteDataIngestion

**Purpose**: Main ingestion orchestrator with advanced features.

```python
class CompleteDataIngestion:
    def __init__(self, config: BatchConfig):
        self.config = config
        self.rate_limiter = RateLimiter()
        self.performance_monitor = PerformanceMonitor()
        self.data_quality_validator = DataQualityValidator()
        self.oracle_db = OracleDB()
        self.logger = get_ingestion_logger(__name__)
```

**Core Methods**:

#### update_ticker_data(ticker: str) -> bool
**Purpose**: Process single ticker with comprehensive error handling.

**Features**:
- **Retry Logic**: Automatic retry with exponential backoff
- **Rate Limiting**: API compliance with adaptive delays
- **Quality Validation**: Data quality scoring and validation
- **Error Recovery**: Graceful handling of various failure types
- **Performance Monitoring**: Operation timing and resource tracking

#### process_batch(tickers: List[str]) -> Dict[str, Any]
**Purpose**: Process batch of tickers with parallel processing.

**Features**:
- **Parallel Processing**: Multi-threaded execution
- **Progress Tracking**: Real-time batch progress monitoring
- **Memory Management**: Efficient memory usage optimization
- **Error Isolation**: Individual ticker failures don't stop batch
- **Comprehensive Reporting**: Detailed batch results and metrics

## Utility Functions

### clean_ticker(ticker: str) -> str
**Purpose**: Standardize ticker symbol formatting.

**Features**:
- **Character Cleaning**: Removes unwanted characters
- **Case Normalization**: Converts to uppercase
- **Format Validation**: Ensures proper ticker format
- **Error Handling**: Graceful handling of invalid inputs

### get_sp500_tickers() -> List[str]
**Purpose**: Fetch current S&P 500 component tickers.

**Features**:
- **Wikipedia Integration**: Real-time ticker list updates
- **Data Cleaning**: Automatic ticker symbol standardization
- **Error Handling**: Fallback mechanisms for API failures
- **Caching**: Efficient data retrieval

### get_etf_tickers() -> List[str]
**Purpose**: Retrieve curated list of ETF tickers.

**Features**:
- **Curated List**: Hand-picked ETF selection
- **Diversification**: Various ETF categories
- **Quality Control**: Pre-validated ticker symbols
- **Maintainability**: Easy to update and extend

## Configuration

### Environment Variables
```bash
# Database Configuration
TNS_ADMIN=/opt/oracle/instantclient_23_8/network/admin
ORA_PYTHON_DRIVER_TYPE=thick

# API Configuration
POLYGON_API_KEY=your_polygon_api_key
FRED_API_KEY=your_fred_api_key

# Processing Configuration
BATCH_SIZE=25
POLYGON_RATE_LIMIT=12
MAX_RETRIES=3
RETRY_DELAY=60
MAX_PROCESSING_TIME=7200
MAX_MEMORY_USAGE=2048
PARALLEL_PROCESSING=true
MAX_WORKERS=4
MIN_DATA_QUALITY_SCORE=0.8
```

### Performance Configuration
```python
config = AdvancedBatchConfig(
    batch_size=25,
    rate_limit_seconds=12,
    max_retries=3,
    retry_delay_seconds=60,
    max_processing_time_seconds=7200.0,
    max_memory_usage_mb=2048.0,
    parallel_processing=True,
    max_workers=4,
    min_data_quality_score=0.8,
    enable_audit_trail=True,
    enable_performance_monitoring=True
)
```

## Usage Examples

### Basic Usage
```python
from CandleThrob.ingestion.ingest_data import main

# Run complete ingestion with default configuration
main()
```

### Custom Configuration
```python
from CandleThrob.ingestion.ingest_data import AdvancedCompleteDataIngestion, AdvancedBatchConfig

# Create custom configuration
config = AdvancedBatchConfig(
    batch_size=50,
    rate_limit_seconds=10,
    max_retries=5,
    parallel_processing=True,
    max_workers=8
)

# Initialize and run
ingestion = CompleteDataIngestion(config)
results = ingestion.process_all_tickers()
```

### Performance Monitoring
```python
from CandleThrob.ingestion.ingest_data import CompleteDataIngestion, BatchConfig

config = BatchConfig(enable_performance_monitoring=True)
ingestion = CompleteDataIngestion(config)

# Process with monitoring
with ingestion.performance_monitor.monitor_operation("complete_ingestion"):
    results = ingestion.process_all_tickers()

# Access metrics
metrics = ingestion.performance_monitor.metrics
print(f"Processing time: {metrics['complete_ingestion']['duration']:.2f} seconds")
print(f"Memory usage: {metrics['complete_ingestion']['memory_usage_mb']:.2f} MB")
```

## Error Handling

### Retry Decorator
```python
@retry_on_failure(max_retries=3, delay_seconds=60)
def update_ticker_data(self, ticker: str) -> bool:
    """Process single ticker with automatic retry."""
```

**Features**:
- **Exponential Backoff**: Increasing delays between retries
- **Configurable**: Adjustable retry count and delay
- **Error Categorization**: Different handling for different error types
- **Logging**: Comprehensive retry attempt logging

### Error Categories
1. **Network Errors**: Connection timeouts, DNS failures
2. **API Errors**: Rate limiting, authentication failures
3. **Data Errors**: Invalid responses, parsing failures
4. **Database Errors**: Connection issues, constraint violations
5. **System Errors**: Memory issues, resource exhaustion

## Performance Optimization

### Memory Management
- **Batch Processing**: Sequential processing to manage memory
- **Data Cleanup**: Automatic cleanup of temporary data
- **Resource Monitoring**: Real-time memory usage tracking
- **Optimization**: Memory-efficient data structures

### Parallel Processing
- **Thread Pool**: Configurable worker thread pool
- **Error Isolation**: Individual failures don't affect others
- **Resource Control**: Memory and CPU usage limits
- **Progress Tracking**: Real-time parallel operation monitoring

### Database Optimization
- **Bulk Operations**: 1000-record chunks for efficient inserts
- **Connection Pooling**: Reused database connections
- **Transaction Management**: Proper commit/rollback handling
- **Incremental Loading**: Only processes new data

## Monitoring and Logging

### Log Levels
- **INFO**: Normal operation progress
- **WARNING**: Non-critical issues and retries
- **ERROR**: Operation failures and exceptions
- **DEBUG**: Detailed operation information

### Metrics Collection
- **Processing Time**: Operation duration tracking
- **Memory Usage**: Real-time memory consumption
- **Success Rates**: Operation success/failure ratios
- **Quality Scores**: Data quality assessment metrics

### Audit Trail
- **Operation Logging**: Complete operation history
- **Data Lineage**: Source to destination tracking
- **Quality Metrics**: Data quality assessment history
- **Performance History**: Historical performance data

## Integration

### Kestra Workflow
```yaml
- id: process_all_batches
  type: io.kestra.plugin.docker.Run
  containerImage: candlethrob:latest
  command: ["sh", "-c", "cd /app && PYTHONPATH=/app python3 /app/CandleThrob/ingestion/ingest_data.py"]
  env:
    BATCH_SIZE: "25"
    POLYGON_RATE_LIMIT: "12"
    MAX_RETRIES: "3"
    PARALLEL_PROCESSING: "true"
    ENABLE_PERFORMANCE_MONITORING: "true"
  volumes:
    - "/tmp/kestra-wd:/app"
    - "/home/ubuntu/stockbot/Wallet_candleThroblake:/opt/oracle/instantclient_23_8/network/admin:ro"
  timeout: PT4H
```

### Database Integration
- **Oracle Database**: Primary storage with optimized schema
- **SQLAlchemy**: ORM for bulk operations
- **Incremental Loading**: Only processes new data
- **Transaction Management**: ACID compliance

## Troubleshooting

### Common Issues

1. **Rate Limit Errors**
   - Check `POLYGON_RATE_LIMIT` environment variable
   - Verify API key validity
   - Monitor adaptive delay adjustments

2. **Memory Issues**
   - Reduce batch size if needed
   - Enable memory optimization
   - Monitor memory usage metrics

3. **Database Connection Errors**
   - Verify Oracle wallet configuration
   - Check database credentials
   - Ensure network connectivity

4. **Performance Issues**
   - Adjust parallel processing settings
   - Monitor performance metrics
   - Optimize batch size and worker count

### Debug Mode
```python
import logging
logging.getLogger('CandleThrob.ingestion').setLevel(logging.DEBUG)

# Run with detailed logging
main()
```

## Best Practices

### Production Deployment
1. **Environment Configuration**: Use environment variables for configuration
2. **Monitoring**: Enable performance monitoring and audit trails
3. **Error Handling**: Implement comprehensive error handling
4. **Resource Management**: Monitor memory and CPU usage
5. **Quality Gates**: Set appropriate data quality thresholds

### Development Guidelines
1. **Testing**: Comprehensive unit and integration testing
2. **Documentation**: Keep documentation current with code changes
3. **Code Quality**: Follow best practices and best practices
4. **Performance**: Regular performance optimization and monitoring
5. **Security**: Secure handling of API keys and credentials

---

**Author**: Adetunji Fasiku 