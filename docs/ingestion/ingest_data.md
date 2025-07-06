# ingest_data.py Documentation

## Overview
The `ingest_data.py` module orchestrates the complete data ingestion pipeline for the CandleThrob financial data system. It serves as the main entry point for batch processing of stock and ETF data using Polygon.io as the primary data source, with Oracle database storage and intelligent incremental loading for CI/CD environments.

## Core Functions

### ingest_ticker_data(tickers: List[str], start_date: Optional[str] = None, end_date: Optional[str] = None)
**Purpose:** Ingests ticker data with automatic incremental loading and bulk insert optimization.

**Parameters:**
- `tickers` (List[str]): List of stock ticker symbols to ingest
- `start_date` (Optional[str]): Start date for data ingestion (format: 'YYYY-MM-DD')
- `end_date` (Optional[str]): End date for data ingestion (format: 'YYYY-MM-DD')

**Process Flow:**
1. **Initialize Database**: Create ticker_data table if not exists
2. **Check Existing Data**: Query Oracle DB for last available date per ticker
3. **Determine Date Range**: 
   - If data exists: Start from last available date + 1 day
   - If no data: Start from 2000-01-01 or provided start_date
4. **Fetch New Data**: Uses Polygon.io via DataIngestion class
5. **Incremental Insert**: Only inserts new data using bulk operations
6. **Store to Oracle DB**: Direct database insert with 1000-record chunks

**Incremental Loading Features:**
- Automatic detection of existing data per ticker
- Only fetches and inserts new data after last available date
- Handles missing data gracefully with gap detection
- Bulk insert optimization for high performance

**Error Handling:**
- Database connection error recovery
- API rate limiting and retry logic
- Data validation before insertion
- Transaction rollback on errors

### ingest_macro_data(series_ids: Optional[List[str]] = None, start_date: Optional[str] = None)
**Purpose:** Ingests macroeconomic indicators from FRED API with incremental loading.

**Parameters:**
- `series_ids` (Optional[List[str]]): List of FRED series IDs to ingest
- `start_date` (Optional[str]): Start date for data ingestion

**Data Sources:**
- Federal Reserve Economic Data (FRED) API
- 11+ key economic indicators including GDP, UNRATE, FEDFUNDS, etc.

**Process:**
1. Initialize Oracle DB connection
2. Create macro_data table if not exists
3. Check last available date per series
4. Fetch only new data from FRED API
5. Bulk insert with incremental loading
6. Handle data normalization and validation

### enrich_and_store_data(df: pd.DataFrame, ticker: str)
**Purpose:** Enriches raw OHLCV data with 113+ technical indicators and stores to Oracle DB.

**Parameters:**
- `df` (pd.DataFrame): Raw OHLCV data from Polygon.io
- `ticker` (str): Stock ticker symbol

**Process Flow:**
1. **Technical Enrichment**: Apply TechnicalIndicators.calculate_all_indicators()
2. **Data Validation**: Ensure all required columns are present
3. **Incremental Check**: Query existing data in transformed_tickers table
4. **Bulk Insert**: Store enriched data with 113+ indicators

**Indicators Added:**
- 6 return calculations (1d, 3d, 7d, 30d, 90d, 365d)
- 21 momentum indicators (RSI, MACD, Stochastics, etc.)
- 9 volume indicators (OBV, MFI, VWAP, etc.)
- 9 volatility indicators (ATR, Bollinger Bands, etc.)
- 5 price transformation indicators
- 5 cyclical indicators (Hilbert Transform)
- 4 statistical indicators
- 54 candlestick pattern recognition indicators

### run_full_ingestion(batch_size: int = 10)
**Purpose:** Orchestrates complete data pipeline for all S&P 500 stocks and ETFs.

**Parameters:**
- `batch_size` (int): Number of tickers to process concurrently (default: 10)

**Process:**
1. **Initialize Database**: Create all required tables (ticker_data, macro_data, transformed_tickers)
2. **Fetch Ticker Lists**: Get current S&P 500 and major ETF lists
3. **Batch Processing**: Process tickers in batches to manage API rate limits
4. **Raw Data Ingestion**: Ingest OHLCV data with incremental loading
5. **Technical Enrichment**: Calculate and store all 113+ technical indicators
6. **Macro Data Ingestion**: Fetch and store FRED economic indicators
7. **Error Recovery**: Continue processing even if individual tickers fail

**Performance Optimizations:**
- Concurrent processing within API rate limits
- Bulk database operations with 1000-record chunks
- Incremental loading to avoid duplicate processing
- Connection pooling for database efficiency

### clean_ticker(ticker: str) -> str
**Purpose:** Standardizes ticker symbol formatting for database consistency.

**Parameters:**
- `ticker` (str): Raw ticker symbol

**Returns:**
- `str`: Cleaned ticker (uppercase, alphanumeric with hyphens)

**Cleaning Rules:**
- Converts to uppercase
- Removes non-alphanumeric characters except hyphens
- Strips leading/trailing hyphens
- Validates against common ticker patterns

### get_sp500_tickers() -> List[str]
**Purpose:** Retrieves current S&P 500 ticker list from Wikipedia with caching.

**Returns:**
- `List[str]`: Cleaned S&P 500 ticker symbols

**Data Source:**
- Wikipedia: "List of S&P 500 companies"
- Real-time updates when companies are added/removed
- Local caching to reduce API calls

**Processing:**
- Parses HTML table using pandas
- Extracts Symbol column
- Applies ticker cleaning and validation
- Handles ticker changes and special cases

### get_etf_tickers() -> List[str]
**Purpose:** Returns predefined list of major ETF tickers for broad market coverage.

**Returns:**
- `List[str]`: 20+ major ETF ticker symbols

**ETF Selection Criteria:**
- High trading volume and liquidity
- Major market segments representation
- Popular investment vehicles
- Sector and geographic diversification

**Included ETFs:**
```python
['SPY', 'QQQ', 'DIA', 'IWM', 'VTI', 'TLT', 'GLD',
 'XLF', 'XLE', 'XLK', 'XLV', 'ARKK', 'VXX',
 'TQQQ', 'SQQQ', 'SOXL', 'XLI', 'XLY', 'XLP', 'SLV']
## Database Integration

### Oracle Database Architecture
The ingestion system uses Oracle Database for persistent storage with the following optimizations:

**Connection Management:**
- SQLAlchemy ORM with Oracle cx_Oracle driver
- Connection pooling for concurrent operations
- Automatic reconnection on connection failures
- Transaction management with rollback on errors

**Table Structure:**
```sql
-- Raw ticker data storage
ticker_data (id, ticker, date, open_price, high_price, low_price, close_price, volume, created_at, updated_at)

-- Macroeconomic indicators
macro_data (id, date, series_id, value, created_at, updated_at)

-- Enriched data with 113+ technical indicators
transformed_tickers (id, ticker, date, [113+ indicator columns], created_at, updated_at)

-- Enriched macro data
transformed_macro_data (id, date, series_id, value, normalized_value, moving_avg_30, year_over_year_change, created_at, updated_at)
```

**Incremental Loading Strategy:**
- Query existing max(date) per ticker before fetching new data
- Only fetch and insert data newer than last available date
- Handles data gaps and missing periods gracefully
- Bulk insert optimization with 1000-record chunks

### Performance Optimization

**Bulk Insert Operations:**
```python
# Example: Efficient bulk insert using pandas to_sql
df_clean.to_sql(
    name='ticker_data',
    con=session.bind,
    if_exists='append',
    index=False,
    method='multi',
    chunksize=1000
)
```

**Index Strategy:**
```sql
-- Primary performance indexes
CREATE INDEX idx_ticker_data_ticker_date ON ticker_data(ticker, date);
CREATE INDEX idx_ticker_data_date ON ticker_data(date);
CREATE INDEX idx_macro_data_series_date ON macro_data(series_id, date);
CREATE INDEX idx_transformed_ticker_date ON transformed_tickers(ticker, date);
```

## Main Function

### main()
**Purpose:** Orchestrates the complete batch processing workflow with Oracle DB storage and technical indicator enrichment.

**Environment Variables:**
- `BATCH_NUMBER` (int): Current batch index for CI/CD (default: 0)
- `BATCH_SIZE` (int): Tickers per batch (default: 10)
- `ENABLE_ENRICHMENT` (bool): Enable technical indicator calculation (default: True)

**Processing Architecture:**

#### Data Pipeline Flow
```python
1. Raw Data Ingestion → ticker_data table
2. Technical Enrichment → Calculate 113+ indicators
3. Enriched Storage → transformed_tickers table
4. Macro Data Ingestion → macro_data table
5. Macro Enrichment → transformed_macro_data table
```

#### Batch Processing Logic
```python
Total Tickers: 520+ (500+ S&P 500 + 20+ ETFs)
Batch Size: 10 tickers (optimized for API rate limits)
Processing Time: ~2-3 minutes per ticker (including enrichment)
Total Batches: 52+ (batches 0-51)
```

#### Enhanced Processing Flow
1. **Database Initialization**: Create all tables if not exist
2. **Ticker Collection**: Fetch current S&P 500 and ETF lists
3. **Batch Validation**: Validate batch parameters
4. **Per-Ticker Processing**:
   - Check existing data in Oracle DB
   - Fetch only new raw data from Polygon.io
   - Store raw data to ticker_data table
   - Calculate 113+ technical indicators
   - Store enriched data to transformed_tickers table
   - Apply 10-second rate limiting between tickers
5. **Macro Data Processing**: Update economic indicators
6. **Error Recovery**: Continue processing on individual ticker failures

#### Rate Limiting & API Management
- **10 seconds** between Polygon.io API calls
- Complies with free tier limits (5 calls/minute)
- Retry logic for transient API failures
- Exponential backoff on rate limit errors

#### Batch Distribution with Enrichment
| Batch | Ticker Range | Count | Raw + Enrichment Time* |
|-------|--------------|-------|------------------------|
| 0     | 0-9          | 10    | ~20-30 minutes         |
| 1     | 10-19        | 10    | ~20-30 minutes         |
| ...   | ...          | ...   | ...                    |
| 52    | 520-529      | 10    | ~20-30 minutes         |

*Approximate time including rate limiting

## CI/CD Integration

### GitHub Actions Compatibility
**Workflow Configuration:**
- Designed for GitHub Actions scheduled runs
- Each batch runs as separate workflow job
- Optimized for 2-7 AM UTC processing window

**Environment Variables Setup:**
```yaml
env:
  BATCH_NUMBER: ${{ matrix.batch }}
  BATCH_SIZE: 25
  POLYGON_API_KEY: ${{ secrets.POLYGON_API_KEY }}
  FRED_API_KEY: ${{ secrets.FRED_API_KEY }}
```

**Benefits:**
- **Parallel Processing**: Multiple batches can run simultaneously
- **Fault Tolerance**: Individual batch failures don't affect others
- **Resource Management**: Distributes API load across time
- **Monitoring**: Each batch provides separate logs

### Error Recovery
**Batch-Level Recovery:**
- Failed batches can be re-run independently
- No impact on successfully processed batches
- Detailed logging for debugging

**Ticker-Level Recovery:**
- Individual ticker failures logged but don't stop batch
- Missing data handled gracefully in downstream processing
- Retry logic for transient failures

## Storage Architecture

### Google Cloud Storage Integration
**Bucket Structure:**
```
candlethrob-candata/
├── raw/
│   ├── tickers/           # S&P 500 individual stock files
│   │   ├── AAPL.parquet
│   │   ├── GOOGL.parquet
│   │   └── ...
│   ├── etfs/              # ETF individual files
│   │   ├── SPY.parquet
│   │   ├── QQQ.parquet
│   │   └── ...
│   └── macros/            # Macroeconomic data
│       └── macro_data.parquet
```

**File Format:**
- **Parquet**: Efficient columnar storage
- **Compression**: Optimized for financial time series
- **Schema**: Consistent across all ticker files

### Data Consistency
**Update Strategy:**
- Incremental updates from last available date
- Handles gaps in historical data
- Maintains chronological order
- Prevents duplicate records

## Dependencies

### Python Packages
```python
import sys
import os
import re
import pandas as pd
import logging
import time
from datetime import datetime
from tqdm import tqdm
```

### Internal Modules
```python
from ingestion.fetch_data import DataIngestion
from utils.gcs import upload_to_gcs, load_from_gcs, blob_exists
```

### External APIs
- **Polygon.io**: OHLCV data source
- **FRED**: Macroeconomic data source
- **Wikipedia**: S&P 500 ticker list

## Usage Examples

### Manual Execution
```bash
# Set environment variables
export BATCH_NUMBER=0
export BATCH_SIZE=25
export POLYGON_API_KEY="your_key"
export FRED_API_KEY="your_key"

# Run ingestion
python -m ingestion.ingest_data
```

### Specific Ticker Update
```python
from ingestion.ingest_data import update_ticker_data

# Update single ticker
update_ticker_data("AAPL", path="raw/tickers")

# Update ETF
update_ticker_data("SPY", path="raw/etfs")
```

### Macro Data Only
```python
from ingestion.ingest_data import update_macro_data

# Update macroeconomic indicators
update_macro_data("raw/macros/macro_data.parquet")
```

## Performance Considerations

### Batch Size Optimization
**Current Setting: 25 tickers/batch**
- Balances processing time vs. resource usage
- Fits within GitHub Actions time limits
- Accommodates API rate limiting

**Adjustable Parameters:**
- Increase batch size for faster processing (if API limits allow)
- Decrease for more granular error recovery

### Memory Management
- Processes one ticker at a time within batch
- Uploads data immediately after processing
- Minimal memory footprint per operation

### Network Resilience
- Implements exponential backoff for retries
- Handles transient network failures
- Logs all API interactions for debugging

## Monitoring and Logging

### Log Levels
- **INFO**: Normal processing updates
- **WARNING**: Recoverable issues (missing data, etc.)
- **ERROR**: Processing failures requiring attention

### Key Metrics Logged
- Batch processing statistics
- Individual ticker success/failure rates
- API response times and errors
- Data quality metrics

### Log File Location
```
ingestion/debug.log
```

## Security Considerations

### API Key Management
- Stored as environment variables only
- Never logged or exposed in code
- Separate keys for different environments

### GCS Access
- Uses service account authentication
- Principle of least privilege
- Encrypted data transmission

## Best Practices

### Operational
1. **Monitor batch completion rates**
2. **Set up alerts for consecutive failures**
3. **Regular review of API usage patterns**
4. **Backup critical configuration**

### Development
1. **Test with small batch sizes first**
2. **Validate data quality after changes**
3. **Use staging environment for testing**
4. **Document any parameter changes**

## Troubleshooting

### Common Issues
1. **API Key Errors**: Verify environment variables
2. **Rate Limiting**: Check delay between requests
3. **GCS Access**: Validate service account permissions
4. **Batch Overflow**: Verify ticker count calculations

### Debug Commands
```bash
# Check environment variables
echo $BATCH_NUMBER $BATCH_SIZE

# Test API connectivity
python -c "import os; print(bool(os.getenv('POLYGON_API_KEY')))"

# Validate GCS access
python -c "from utils.gcs import blob_exists; print(blob_exists('candlethrob-candata', 'test'))"
```
