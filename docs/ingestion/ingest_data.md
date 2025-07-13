# Complete Data Ingestion Module

## Overview

The `ingest_data_complete.py` module provides a comprehensive data ingestion system that processes all S&P 500 and ETF tickers in a single execution. This module implements industry-grade data pipeline practices including stateful batch processing, rate limiting, error handling, and incremental data loading.

## Features

- **Complete Processing**: Processes all 523 tickers (503 S&P 500 + 20 ETFs) in one execution
- **Stateful Batching**: Automatic batch progression with state management
- **Rate Limiting**: Polygon.io API compliance (5 calls/minute)
- **Incremental Loading**: Only fetches new data after last available date
- **Error Handling**: Comprehensive retry logic and error recovery
- **Oracle Integration**: Bulk database operations with transaction management
- **Progress Monitoring**: Real-time logging and result tracking

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   S&P 500 +     │───▶│   Batch Config   │───▶│   Rate Limiter  │
│   ETF Tickers   │    │   (25/batch)     │    │   (5/min)       │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │                        │
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Oracle DB     │◀───│   Bulk Insert    │◀───│   Polygon.io    │
│   (ticker_data) │    │   (1000/chunk)   │    │   (OHLCV)       │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Usage

### Basic Usage

```python
from CandleThrob.ingestion.ingest_data_complete import main

# Run complete ingestion
main()
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BATCH_SIZE` | 25 | Number of tickers per batch |
| `POLYGON_RATE_LIMIT` | 12 | API calls per minute |

### Configuration

```python
from CandleThrob.ingestion.ingest_data_complete import BatchConfig, CompleteDataIngestion

config = BatchConfig(
    batch_size=25,
    rate_limit_seconds=12,
    max_retries=3,
    retry_delay_seconds=60
)

ingestion = CompleteDataIngestion(config)
```

## Classes

### BatchConfig

Configuration class for batch processing parameters.

**Attributes:**
- `batch_size` (int): Number of tickers to process per batch
- `rate_limit_seconds` (int): Seconds between API calls
- `max_retries` (int): Maximum retry attempts
- `retry_delay_seconds` (int): Delay between retries

### RateLimiter

Rate limiter for Polygon.io API calls using token bucket algorithm.

**Methods:**
- `wait_if_needed()`: Ensures API rate limit compliance

### CompleteDataIngestion

Main ingestion class with comprehensive error handling.

**Methods:**
- `update_ticker_data(ticker)`: Process single ticker with retry logic
- `process_batch(tickers)`: Process batch of tickers with progress tracking

## Data Flow

1. **Ticker Collection**: Fetches S&P 500 from Wikipedia + curated ETF list
2. **Batch Division**: Splits 523 tickers into 21 batches of 25 each
3. **Incremental Check**: For each ticker, checks last available date
4. **Data Fetching**: Retrieves new data from Polygon.io with rate limiting
5. **Bulk Insert**: Saves data to Oracle DB using optimized bulk operations
6. **Progress Tracking**: Maintains comprehensive results and timing
7. **Completion Signal**: Creates flag file when all batches complete

## Error Handling

### Retry Logic
- **Max Retries**: 3 attempts per ticker
- **Delay**: 60 seconds between retries
- **Exponential Backoff**: Implemented via decorator

### Rate Limiting
- **API Calls**: 5 calls per minute (12-second intervals)
- **Token Bucket**: Prevents rate limit violations
- **Sleep Calculation**: Dynamic timing based on last call

### Database Errors
- **Connection Management**: Automatic connection cleanup
- **Transaction Rollback**: On bulk insert failures
- **Incremental Recovery**: Continues from last successful ticker

## Performance

### Expected Timeline
- **Total Tickers**: 523 (503 S&P 500 + 20 ETFs)
- **Batch Size**: 25 tickers per batch
- **Total Batches**: 21 batches
- **Rate Limit**: 12 seconds between API calls
- **Estimated Duration**: 3-4 hours for complete run

### Optimization Features
- **Bulk Operations**: 1000-record chunks for database inserts
- **Incremental Loading**: Only fetches new data
- **Connection Pooling**: Reuses database connections
- **Memory Management**: Processes batches sequentially

## Monitoring

### Log Output
```
2025-07-13 22:31:13,345 - INFO - Starting complete ingestion - processing all tickers in batches of 25
2025-07-13 22:31:13,540 - INFO - Total tickers available: 523
2025-07-13 22:31:13,540 - INFO - Processing 21 batches of 25 tickers each
2025-07-13 22:31:13,541 - INFO - Processing batch 1/21: tickers 0-24 (25 tickers)
2025-07-13 22:31:13,541 - INFO - Processing ticker 1/25: MMM
```

### Result Files
- **`/app/data/complete_ingestion_results.json`**: Comprehensive results
- **`/app/data/batch_cycle_complete.flag`**: Completion signal

### Result Structure
```json
{
  "total_batches": 21,
  "total_tickers": 523,
  "successful_tickers": 520,
  "failed_tickers": 3,
  "failed_tickers_list": ["TICKER1", "TICKER2", "TICKER3"],
  "batch_results": [...],
  "start_time": "2025-07-13T22:31:13",
  "end_time": "2025-07-13T02:31:13",
  "duration_seconds": 14400.0
}
```

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
   - Monitor API usage in Polygon.io dashboard

2. **Database Connection Errors**
   - Verify Oracle wallet configuration
   - Check database credentials in vault
   - Ensure network connectivity

3. **Memory Issues**
   - Reduce batch size if needed
   - Monitor system resources
   - Check for memory leaks in long-running processes

### Debug Mode
```python
import logging
logging.getLogger().setLevel(logging.DEBUG)
```

## Dependencies

- **pandas**: Data manipulation and analysis
- **requests**: HTTP client for API calls
- **polygon**: Polygon.io API client
- **oracledb**: Oracle Database driver
- **sqlalchemy**: Database ORM
- **logging**: Standard logging framework

## Version History

- **v2.0.0** (2025-07-13): Complete rewrite with industry-grade practices
- **v1.0.0**: Initial implementation

## Contributing

When modifying this module:
1. Maintain comprehensive error handling
2. Add type hints for all functions
3. Update documentation for any changes
4. Test with various batch sizes
5. Verify rate limiting compliance
6. Update version number and changelog 