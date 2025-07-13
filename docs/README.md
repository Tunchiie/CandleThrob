# CandleThrob Documentation

Welcome to the comprehensive documentation for CandleThrob, an industry-grade financial data pipeline with Oracle Database storage and 113+ technical indicators.

## Overview

CandleThrob is a production-ready financial data pipeline that processes S&P 500 and ETF data with enterprise-grade features including incremental loading, rate limiting, error handling, and comprehensive monitoring.

## Documentation Structure

### Core Modules
- **[Ingestion Module](./ingestion/)** - Data ingestion with Oracle DB storage and incremental loading
- **[Utils Module](./utils/)** - Database models, Oracle DB utilities, and helper functions
- **[Tests](./tests/)** - Comprehensive test suite documentation

### Schema Documentation
- **[Database Schemas](./schemas.md)** - Complete database schemas including 113+ technical indicators
- **[Models Documentation](./utils/models.md)** - SQLAlchemy ORM models for Oracle Database
- **[Oracle DB Utilities](./utils/oracledb.md)** - Database connection and management

### Technical Documentation
- **[Complete Data Ingestion](./ingestion/ingest_data_complete.md)** - Industry-grade data ingestion pipeline
- **[Data Fetching](./ingestion/fetch_data.md)** - API integration for Polygon.io and FRED
- **[Technical Enrichment](./ingestion/enrich_data.md)** - 113+ technical indicators calculation

## Quick Start

### 1. Database Setup
```python
from CandleThrob.utils.oracle_conn import OracleDB

# Test database connection
db = OracleDB()
with db.establish_connection() as conn:
    print("Database connection successful!")
```

### 2. Data Ingestion
```python
from CandleThrob.ingestion.ingest_data_complete import main

# Process all 523 tickers (503 S&P 500 + 20 ETFs)
main()
```

### 3. Technical Analysis
```python
from CandleThrob.ingestion.enrich_data import TechnicalEnrichment

# Calculate 113+ technical indicators
enrichment = TechnicalEnrichment()
enrichment.calculate_all_indicators()
```

## Architecture

```
CandleThrob Enterprise Pipeline:
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Polygon.io    │───▶│   Raw Ingestion  │───▶│   Oracle DB     │
│   (OHLCV Data)  │    │   (ticker_data)  │    │ (ticker_data)   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│     FRED API    │───▶│  Macro Ingestion │───▶│   Oracle DB     │
│  (Economic Data)│    │   (macro_data)   │    │  (macro_data)   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Oracle DB     │◀───│   Technical      │◀───│   113+ Tech     │
│(transformed_    │    │   Enrichment     │    │   Indicators    │
│ tickers)        │    │ (enrich_data.py) │    │   + Patterns    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Key Features

### Enterprise-Grade Data Ingestion
- **Complete Processing**: All 523 tickers (503 S&P 500 + 20 ETFs) in one execution
- **Stateful Batching**: Automatic batch progression with state management
- **Rate Limiting**: Polygon.io API compliance (5 calls/minute)
- **Incremental Loading**: Only fetches new data after last available date
- **Error Handling**: Comprehensive retry logic and error recovery
- **Oracle Integration**: Bulk database operations with transaction management

### Technical Indicators (113+ indicators)
- **Returns**: 1d, 3d, 7d, 30d, 90d, 365d
- **Momentum**: RSI, MACD, Stochastics, CCI, Williams %R, ROC, MOM, TRIX
- **Moving Averages**: SMA/EMA series (10, 20, 50, 100, 200)
- **Volume**: OBV, A/D Line, MFI, Chaikin indicators, VWAP, VPT, ADX
- **Volatility**: ATR, Bollinger Bands, Donchian Channels, Ulcer Index
- **Price**: Midprice, Median, Typical, Weighted Close, Average Price
- **Cyclical**: Hilbert Transform family indicators
- **Statistical**: Standard Deviation, Variance, Beta, Z-Score
- **Patterns**: 54 candlestick pattern recognition indicators

### Database Features
- **Incremental Loading**: Only insert new data after last available date
- **Bulk Operations**: Optimized 1000-record chunks for high performance
- **Error Recovery**: Transaction rollback and connection management
- **Oracle Optimization**: Indexes and constraints for time series data
- **ACID Compliance**: Full transaction support with rollback capabilities

## Performance Metrics

### Data Processing
- **Total Tickers**: 523 (503 S&P 500 + 20 ETFs)
- **Batch Size**: 25 tickers per batch
- **Total Batches**: 21 batches
- **Rate Limit**: 12 seconds between API calls
- **Estimated Duration**: 3-4 hours for complete run

### Database Performance
- **Bulk Operations**: 1000-record chunks
- **Connection Pooling**: Reuses database connections
- **Memory Management**: Processes batches sequentially
- **Incremental Loading**: Only processes new data

## Monitoring & Logging

### Real-Time Monitoring
```python
# Check batch state
from CandleThrob.ingestion.check_batch_state import main
main()

# Monitor database
from CandleThrob.utils.db_ping import test_connection
test_connection()
```

### Log Output
```
2025-07-13 22:31:13,345 - INFO - Starting complete ingestion - processing all tickers in batches of 25
2025-07-13 22:31:13,540 - INFO - Total tickers available: 523
2025-07-13 22:31:13,540 - INFO - Processing 21 batches of 25 tickers each
2025-07-13 22:31:13,541 - INFO - Processing batch 1/21: tickers 0-24 (25 tickers)
2025-07-13 22:31:13,541 - INFO - Processing ticker 1/25: MMM
```

## Environment Configuration

### Required Environment Variables
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
```

### Oracle Database Setup
1. **Wallet Configuration**: Mount Oracle wallet to `/opt/oracle/instantclient_23_8/network/admin`
2. **Connection String**: Configure `tnsnames.ora` with database aliases
3. **Credentials**: Store in vault for secure access

## Integration

### Kestra Workflow
```yaml
- id: process_all_batches
  type: io.kestra.plugin.docker.Run
  containerImage: candlethrob:latest
  command: ["sh", "-c", "cd /app && PYTHONPATH=/app python3 /app/CandleThrob/ingestion/ingest_data_complete.py"]
  env:
    BATCH_SIZE: "25"
    POLYGON_RATE_LIMIT: "12"
    TNS_ADMIN: "/opt/oracle/instantclient_23_8/network/admin"
    ORA_PYTHON_DRIVER_TYPE: "thick"
  volumes:
    - "/tmp/kestra-wd:/app"
    - "/home/ubuntu/stockbot/Wallet_candleThroblake:/opt/oracle/instantclient_23_8/network/admin:ro"
  timeout: PT4H
```

## Dependencies

### Core Dependencies
- **Python 3.8+**: Modern Python with type hints
- **Oracle Database**: cx_Oracle driver with wallet authentication
- **TA-Lib**: Technical analysis library for indicators
- **SQLAlchemy**: ORM for database operations
- **pandas**: Data manipulation and analysis
- **numpy**: Numerical computing

### API Dependencies
- **Polygon.io**: Primary OHLCV data source
- **FRED API**: Macroeconomic indicators
- **requests**: HTTP client for API calls

### Development Dependencies
- **pytest**: Testing framework
- **black**: Code formatting
- **mypy**: Type checking
- **flake8**: Linting

## Contributing

When contributing to CandleThrob:

1. **Code Quality**: Follow industry standards with comprehensive type hints
2. **Documentation**: Add detailed docstrings for all functions and classes
3. **Testing**: Include unit tests for new functionality
4. **Error Handling**: Implement comprehensive error handling and retry logic
5. **Performance**: Optimize for large-scale data processing
6. **Security**: Use vault for sensitive configuration

### Development Guidelines
- **Type Hints**: All functions must include type annotations
- **Docstrings**: Follow Google/NumPy docstring format
- **Error Handling**: Use try/except with specific exception types
- **Logging**: Use structured logging with appropriate levels
- **Testing**: Maintain >90% code coverage

## Version History

- **v2.0.0** (2025-07-13): Complete rewrite with industry-grade practices
- **v1.0.0**: Initial implementation

## Support

For technical support or questions:
1. Check the troubleshooting documentation
2. Review the comprehensive API documentation
3. Examine the test suite for usage examples
4. Contact the development team

---

**Last Updated**: July 13, 2025  
**Version**: 2.0.0  
**Author**: CandleThrob Team
