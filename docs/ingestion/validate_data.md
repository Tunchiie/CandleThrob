# validate_data.py Documentation

## Overview

The `validate_data.py` module provides data validation capabilities for the CandleThrob financial data pipeline. It offers comprehensive data quality assessment, completeness checks, integrity validation, and detailed reporting with advanced features including performance monitoring, audit trails, and quality scoring.

## Features

### Core Capabilities
- **Comprehensive Data Quality Validation**: Multi-layer data quality assessment and scoring
- **Performance Monitoring**: Real-time performance metrics and resource tracking
- **Advanced Error Handling**: Retry logic with exponential backoff
- **Data Lineage Tracking**: Complete audit trails and data lineage
- **Quality Gates**: Configurable validation checkpoints and thresholds
- **Resource Monitoring**: Memory usage optimization and monitoring
- **Advanced Logging**: Structured logging with detailed validation reports
- **Modular Design**: Configurable validation rules and testable components
- **Parallel Processing**: Efficient validation of large datasets
- **Advanced Error Categorization**: Detailed error classification and handling

## Architecture

### Design Philosophy
- **Advanced **: Production-ready with comprehensive validation
- **Quality-First**: Data quality as the primary concern
- **Configurable**: Flexible validation rules and thresholds
- **Observable**: Extensive logging and metrics collection
- **Reliable**: Robust error handling and recovery mechanisms

### Validation Flow
```
Data Input → Schema Validation → Quality Checks → Integrity Validation → 
Gap Analysis → Quality Scoring → Report Generation → Audit Trail
```

## Core Classes

### AdvancedValidationConfig

**Purpose**: configuration management for data validation parameters.

```python
@dataclass
class ValidationConfig:
    # Performance settings
    max_validation_time_seconds: float = 300.0
    max_memory_usage_mb: float = 1024.0
    parallel_processing: bool = True
    max_workers: int = 4
    
    # Quality thresholds
    min_data_quality_score: float = 0.8
    max_null_ratio: float = 0.1
    max_gap_days: int = 5
    min_recent_data_days: int = 7
    
    # Advanced features
    enable_audit_trail: bool = True
    enable_performance_monitoring: bool = True
    enable_memory_optimization: bool = True
    enable_data_lineage: bool = True
```

**Key Features**:
- **Validation**: Automatic parameter validation in `__post_init__`
- **Flexibility**: Configurable for different environments and requirements
- **Safety**: Built-in limits and constraints for resource usage
- **Monitoring**: Performance and quality thresholds for alerts

### PerformanceMonitor

**Purpose**: performance monitoring for validation operations.

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
- **Real-Time**: Immediate metric availability for monitoring

### DataQualityScorer

**Purpose**: Advanced data quality scoring system with comprehensive assessment.

```python
class DataQualityScorer:
    @staticmethod
    def calculate_ticker_quality_score(validation_results: Dict[str, Any]) -> Tuple[float, List[str]]:
        """Calculate comprehensive quality score for ticker data."""
    
    @staticmethod
    def calculate_macro_quality_score(validation_results: Dict[str, Any]) -> Tuple[float, List[str]]:
        """Calculate comprehensive quality score for macro data."""
```

**Scoring Criteria**:
- **Data Completeness**: Null value ratios and missing data detection
- **Data Integrity**: OHLCV relationship validation and constraint checking
- **Data Freshness**: Recent data availability and update frequency
- **Data Gaps**: Missing time periods and continuity assessment
- **Date Range**: Historical data coverage and span analysis

### AdvancedDataValidator

**Purpose**: Main validation orchestrator with comprehensive validation capabilities.

```python
class AdvancedDataValidator:
    def __init__(self, config: Optional[AdvancedValidationConfig] = None):
        self.config = config or AdvancedValidationConfig()
        self.oracle_db = OracleDB()
        self.performance_monitor = PerformanceMonitor()
        self.data_quality_scorer = DataQualityScorer()
        self.logger = get_ingestion_logger(__name__)
```

## Validation Functions

### validate_ticker_data()

**Purpose**: Comprehensive validation of ticker data quality and integrity.

**Validation Checks**:
- **Schema Compliance**: Required columns and data types
- **Data Completeness**: Null value detection and ratios
- **Data Integrity**: OHLCV relationship validation
- **Data Freshness**: Recent data availability
- **Data Gaps**: Missing time periods and continuity
- **Date Range**: Historical coverage analysis
- **Quality Scoring**: Overall quality assessment

**Returns**:
```python
{
    "validation_status": str,
    "total_records": int,
    "date_range": {
        "min_date": str,
        "max_date": str,
        "days_span": int
    },
    "data_quality": {
        "null_checks": Dict[str, int],
        "recent_data_tickers": int,
        "data_gaps": List[str],
        "integrity_issues": List[str]
    },
    "quality_score": float,
    "quality_issues": List[str],
    "recommendations": List[str]
}
```

### validate_macro_data()

**Purpose**: Comprehensive validation of macroeconomic data quality.

**Validation Checks**:
- **Series Completeness**: All expected economic indicators
- **Data Freshness**: Recent data availability
- **Update Frequency**: Regular data updates
- **Data Quality**: Value ranges and consistency
- **Coverage Analysis**: Time series completeness

**Returns**:
```python
{
    "validation_status": str,
    "total_series": int,
    "active_series": int,
    "data_quality": {
        "series_coverage": Dict[str, bool],
        "recent_updates": Dict[str, str],
        "data_gaps": List[str]
    },
    "quality_score": float,
    "quality_issues": List[str],
    "recommendations": List[str]
}
```

### check_batch_results()

**Purpose**: Validate batch processing results and performance.

**Validation Checks**:
- **Batch Success Rates**: Overall batch processing success
- **Processing Times**: Performance analysis
- **Error Patterns**: Common failure modes
- **Data Volume**: Expected vs actual data volumes
- **Quality Metrics**: Batch-level quality assessment

**Returns**:
```python
{
    "batch_validation": {
        "total_batches": int,
        "successful_batches": int,
        "failed_batches": int,
        "success_rate": float,
        "average_duration": float
    },
    "performance_metrics": {
        "processing_times": List[float],
        "memory_usage": List[float],
        "error_counts": Dict[str, int]
    },
    "quality_assessment": {
        "overall_quality": float,
        "quality_trends": str,
        "recommendations": List[str]
    }
}
```

### generate_validation_report()

**Purpose**: Generate comprehensive validation report with recommendations.

**Report Components**:
- **Executive Summary**: High-level validation status
- **Detailed Findings**: Specific issues and metrics
- **Quality Scores**: Numerical quality assessments
- **Recommendations**: Actionable improvement suggestions
- **Performance Metrics**: Resource usage and timing
- **Audit Trail**: Complete validation history

**Returns**:
```python
{
    "report_timestamp": str,
    "executive_summary": {
        "overall_status": str,
        "critical_issues": int,
        "quality_score": float
    },
    "detailed_findings": {
        "ticker_validation": Dict[str, Any],
        "macro_validation": Dict[str, Any],
        "batch_validation": Dict[str, Any]
    },
    "quality_scores": {
        "ticker_quality": float,
        "macro_quality": float,
        "overall_quality": float
    },
    "recommendations": List[str],
    "performance_metrics": Dict[str, Any],
    "audit_trail": List[Dict[str, Any]]
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

## Configuration

### Environment Variables
```bash
# Validation Configuration
MAX_VALIDATION_TIME_SECONDS=300
MAX_MEMORY_USAGE_MB=1024
PARALLEL_PROCESSING=true
MAX_WORKERS=4

# Quality Thresholds
MIN_DATA_QUALITY_SCORE=0.8
MAX_NULL_RATIO=0.1
MAX_GAP_DAYS=5
MIN_RECENT_DATA_DAYS=7

# Advanced Features
ENABLE_AUDIT_TRAIL=true
ENABLE_PERFORMANCE_MONITORING=true
ENABLE_MEMORY_OPTIMIZATION=true
ENABLE_DATA_LINEAGE=true
```

### Custom Configuration
```python
from CandleThrob.ingestion.validate_data import ValidationConfig, DataValidator

# Custom configuration
config = ValidationConfig(
    max_validation_time_seconds=600,  # 10 minutes
    max_memory_usage_mb=2048,  # 2GB
    parallel_processing=True,
    max_workers=8,
    min_data_quality_score=0.9,  # Higher quality requirement
    max_null_ratio=0.05,  # Lower null tolerance
    max_gap_days=3,  # Stricter gap tolerance
    enable_audit_trail=True,
    enable_performance_monitoring=True
)

validator = DataValidator(config)
```

## Usage Examples

### Basic Validation
```python
from CandleThrob.ingestion.validate_data import main

# Run comprehensive validation with default configuration
main()
```

### Custom Validation
```python
from CandleThrob.ingestion.validate_data import DataValidator, ValidationConfig

# Create custom configuration
config = ValidationConfig(
    min_data_quality_score=0.9,
    max_null_ratio=0.05,
    max_gap_days=3,
    parallel_processing=True,
    max_workers=8
)

# Initialize and run validation
validator = DataValidator(config)
report = validator.generate_validation_report()
print(f"Validation report: {report}")
```

### Individual Validation Checks
```python
from CandleThrob.ingestion.validate_data import DataValidator

validator = DataValidator()

# Validate ticker data
ticker_results = validator.validate_ticker_data()
print(f"Ticker validation: {ticker_results}")

# Validate macro data
macro_results = validator.validate_macro_data()
print(f"Macro validation: {macro_results}")

# Check batch results
batch_results = validator.check_batch_results()
print(f"Batch validation: {batch_results}")
```

### Performance Monitoring
```python
from CandleThrob.ingestion.validate_data import AdvancedDataValidator

validator = AdvancedDataValidator()

# Run validation with performance monitoring
with validator.performance_monitor.monitor_operation("comprehensive_validation"):
    report = validator.generate_validation_report()

# Access performance metrics
metrics = validator.get_performance_metrics()
print(f"Validation performance: {metrics}")
```

## Quality Scoring

### Ticker Data Quality Score

**Scoring Components**:
- **Data Completeness** (30%): Null value ratios and missing data
- **Data Integrity** (25%): OHLCV relationship validation
- **Data Freshness** (25%): Recent data availability
- **Data Continuity** (20%): Gap analysis and time series completeness

**Score Calculation**:
```python
score = 1.0
issues = []

# Completeness check
if null_ratio > 0.1:
    score -= 0.2
    issues.append(f"High null ratio: {null_ratio:.2%}")

# Integrity check
if integrity_violations > 0:
    score -= 0.25
    issues.append(f"Integrity violations: {integrity_violations}")

# Freshness check
if not recent_data:
    score -= 0.25
    issues.append("No recent data available")

# Continuity check
if gap_count > 5:
    score -= 0.2
    issues.append(f"Multiple data gaps: {gap_count}")

return max(0.0, score), issues
```

### Macro Data Quality Score

**Scoring Components**:
- **Series Coverage** (40%): All expected economic indicators
- **Data Freshness** (30%): Recent updates and availability
- **Update Frequency** (20%): Regular data updates
- **Data Quality** (10%): Value ranges and consistency

## Integration

### Kestra Workflow Integration
```yaml
- id: validate_data
  type: io.kestra.plugin.docker.Run
  containerImage: candlethrob:latest
  command: ["sh", "-c", "cd /app && PYTHONPATH=/app python3 /app/CandleThrob/ingestion/validate_data.py"]
  env:
    MIN_DATA_QUALITY_SCORE: "0.8"
    MAX_NULL_RATIO: "0.1"
    ENABLE_AUDIT_TRAIL: "true"
    ENABLE_PERFORMANCE_MONITORING: "true"
  timeout: PT10M
```

### Database Integration
- **Oracle Database**: Direct connection for data validation
- **SQL Queries**: Optimized queries for validation checks
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
   - Review validation time limits

3. **Quality Threshold Issues**
   - Review quality score thresholds
   - Check data source reliability
   - Validate data processing pipeline

4. **Memory Issues**
   - Reduce parallel processing workers
   - Enable memory optimization
   - Monitor memory usage metrics

### Debug Mode
```python
import logging
logging.getLogger('CandleThrob.ingestion.validate_data').setLevel(logging.DEBUG)

# Run with detailed logging
validator = AdvancedDataValidator()
report = validator.generate_validation_report()
```

## Best Practices

### Production Deployment
1. **Regular Validation**: Scheduled validation runs
2. **Quality Gates**: Appropriate quality thresholds
3. **Performance Monitoring**: Efficient validation queries
4. **Error Handling**: Comprehensive error recovery
5. **Audit Trail**: Complete validation history

### Development Guidelines
1. **Testing**: Comprehensive unit and integration testing
2. **Documentation**: Keep validation documentation current
3. **Configuration**: Use environment variables for configuration
4. **Logging**: Structured logging with appropriate levels
5. **Security**: Secure handling of database credentials

### Validation Strategy
1. **Proactive Validation**: Identify issues before they become critical
2. **Quality Metrics**: Track quality trends over time
3. **Alert Thresholds**: Appropriate alerts for different issues
4. **Data Quality**: Continuous data quality assessment
5. **Performance Optimization**: Regular performance review and optimization

---

**Author**: Adetunji Fasiku 