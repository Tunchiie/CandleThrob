# transform_data.py Documentation

## Overview
The `transform_data.py` module is the data transformation engine of the CandleThrob financial data pipeline. It processes raw OHLCV data from cloud storage, calculates technical indicators, and saves the enriched datasets back to cloud storage. This module serves as the bridge between raw data ingestion and analytics-ready datasets.

## Architecture

### Design Philosophy
- **Separation of Concerns**: Dedicated transformation step after data ingestion
- **Cloud-Native**: Reads from and writes to Google Cloud Storage
- **Fallback Strategy**: Works with or without TA-Lib installation
- **Batch Processing**: Processes all S&P 500 stocks and ETFs systematically
- **Error Resilience**: Individual ticker failures don't stop overall processing

### Pipeline Position
```
Raw Data (GCS) → Transform Data → Processed Data (GCS) → Analytics
```

**Scheduling:**
- Runs at 8:30 AM UTC via GitHub Actions
- Executes after ingestion completes (2:00-7:00 AM UTC)
- Processes all available ticker data

## Class: TechnicalTransformer

### Purpose
The `TechnicalTransformer` class encapsulates all data transformation logic, providing a clean interface for processing financial time series data with technical indicators.

### Constructor
```python
def __init__(self):
    self.logger = logger
```

**Initialization:**
- Sets up logging instance
- No external dependencies required
- Lightweight initialization for batch processing

### Core Methods

#### load_ticker_data(ticker, path="raw/tickers") -> Optional[pd.DataFrame]
**Purpose:** Loads individual ticker data from Google Cloud Storage.

**Parameters:**
- `ticker` (str): Stock ticker symbol (e.g., "AAPL")
- `path` (str): GCS blob path prefix (default: "raw/tickers")

**Returns:**
- `pd.DataFrame`: Loaded OHLCV data or None if not found

**Process:**
1. **Blob Existence Check**: Verifies file exists in GCS
2. **Data Loading**: Uses utils/gcs.py to fetch parquet data
3. **Validation**: Checks data completeness
4. **Logging**: Records success/failure statistics

**Error Handling:**
- Missing files: Returns None with warning log
- Corrupted data: Logs error and returns None
- Network issues: Retries handled by GCS utility

**Storage Paths:**
```
GCS Structure:
├── raw/tickers/{ticker}.parquet    # S&P 500 stocks
├── raw/etfs/{ticker}.parquet       # ETF data
├── processed/tickers/{ticker}.parquet  # Transformed stocks
└── processed/etfs/{ticker}.parquet     # Transformed ETFs
```

#### calculate_basic_indicators(df) -> pd.DataFrame
**Purpose:** Calculates essential technical indicators using only pandas/numpy (no TA-Lib dependency).

**Calculated Indicators:**

**Moving Averages:**
```python
for period in [10, 20, 50, 100, 200]:
    df[f'SMA_{period}'] = df['Close'].rolling(window=period).mean()
```

**RSI (Relative Strength Index):**
```python
delta = df['Close'].diff()
gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
rs = gain / loss
df['RSI'] = 100 - (100 / (1 + rs))
```

**Bollinger Bands:**
```python
sma_20 = df['Close'].rolling(window=20).mean()
std_20 = df['Close'].rolling(window=20).std()
df['BB_Upper'] = sma_20 + (std_20 * 2)
df['BB_Lower'] = sma_20 - (std_20 * 2)
```

**Volume Indicators:**
```python
df['Volume_SMA_20'] = df['Volume'].rolling(window=20).mean()
df['Volume_Ratio'] = df['Volume'] / df['Volume_SMA_20']
```

**Benefits:**
- **No External Dependencies**: Works without TA-Lib
- **Core Indicators**: Covers most essential technical analysis needs
- **Fast Computation**: Optimized pandas operations
- **Fallback Safety**: Ensures pipeline never fails completely

#### calculate_talib_indicators(df) -> pd.DataFrame
**Purpose:** Calculates advanced technical indicators using TA-Lib library.

**Key TA-Lib Indicators:**

**Momentum Indicators:**
- **RSI_TALIB**: TA-Lib RSI (more precise than basic version)
- **MACD/Signal/Histogram**: Complete MACD analysis
- **ATR**: Average True Range for volatility

**Volume Analysis:**
- **OBV**: On-Balance Volume

**Pattern Recognition:**
- **DOJI**: Doji candlestick pattern
- **HAMMER**: Hammer reversal pattern

**Error Handling:**
```python
try:
    # TA-Lib calculations
except Exception as e:
    self.logger.error(f"Error calculating TA-Lib indicators: {e}")
```

**Availability Check:**
```python
if not TALIB_AVAILABLE or df.empty:
    return df
```

**Benefits:**
- **Professional-Grade**: Professional indicator calculations
- **Extensive Library**: Access to 150+ indicators
- **Pattern Recognition**: Advanced candlestick pattern detection
- **Performance**: C-compiled functions for speed

#### transform_ticker(ticker, source_path, dest_path) -> bool
**Purpose:** Complete transformation pipeline for a single ticker.

**Parameters:**
- `ticker` (str): Stock symbol to transform
- `source_path` (str): Source GCS path (e.g., "raw/tickers")
- `dest_path` (str): Destination GCS path (e.g., "processed/tickers")

**Returns:**
- `bool`: True if transformation successful, False otherwise

**Process Flow:**
1. **Data Loading**: Load raw OHLCV data from GCS
2. **Validation**: Check for required columns and data quality
3. **Data Preparation**: Sort by date and ensure proper formatting
4. **Basic Indicators**: Calculate pandas-based indicators
5. **Advanced Indicators**: Add TA-Lib indicators if available
6. **Data Saving**: Upload transformed data to processed path

**Required Columns:**
```python
required_cols = ['Open', 'High', 'Low', 'Close', 'Volume', 'Date']
```

**Data Preparation:**
```python
df['Date'] = pd.to_datetime(df['Date'])
df = df.sort_values('Date').reset_index(drop=True)
```

**Success Criteria:**
- All required columns present
- Data successfully loaded and transformed
- Indicators calculated without errors
- Data successfully saved to destination

## Utility Functions

### get_sp500_tickers() -> list
**Purpose:** Fetches current S&P 500 component tickers from Wikipedia.

**Data Source:**
- URL: "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
- Method: pandas.read_html() for table parsing
- Column: "Symbol" contains ticker symbols

**Processing:**
```python
tickers = pd.read_html("...")[0]["Symbol"].tolist()
return [re.sub(r"[^A-Z0-9\-]", "-", ticker.strip().upper()).strip("-") 
        for ticker in tickers]
```

**Benefits:**
- **Real-Time Updates**: Always current with index changes
- **Automatic Cleaning**: Standardizes ticker format
- **Error Handling**: Returns empty list on network failures

### get_etf_tickers() -> list
**Purpose:** Returns predefined list of important ETF tickers.

**ETF Selection:**
```python
etf_tickers = ['SPY', 'QQQ', 'DIA', 'IWM', 'VTI', 'TLT', 'GLD',
               'XLF', 'XLE', 'XLK', 'XLV', 'ARKK', 'VXX',
               'TQQQ', 'SQQQ', 'SOXL', 'XLI', 'XLY', 'XLP', 'SLV']
```

**Categories Covered:**
- **Broad Market**: SPY, QQQ, DIA, IWM, VTI
- **Sector**: XLF, XLE, XLK, XLV, XLI, XLY, XLP
- **Commodities**: GLD, SLV, TLT
- **Volatility**: VXX
- **Leveraged**: TQQQ, SQQQ, SOXL
- **Thematic**: ARKK

## Main Function

### main()
**Purpose:** Orchestrates the complete transformation process for all tickers.

**Process Overview:**
1. **Initialization**: Create TechnicalTransformer instance
2. **Ticker Collection**: Fetch S&P 500 and ETF lists
3. **Batch Processing**: Transform all tickers systematically
4. **Statistics Tracking**: Count successful/failed transformations
5. **Summary Reporting**: Log final statistics and status

**Processing Flow:**
```python
# Process S&P 500 stocks
for ticker in sp500_tickers:
    transformer.transform_ticker(ticker, "raw/tickers", "processed/tickers")

# Process ETFs  
for ticker in etf_tickers:
    transformer.transform_ticker(ticker, "raw/etfs", "processed/etfs")
```

**Statistics Tracking:**
- Total tickers processed
- Successful transformations
- Failed transformations
- TA-Lib availability status

**Logging Examples:**
```
INFO - Processing S&P 500 ticker 1/503: AAPL
INFO - Loaded 5000 records for AAPL
INFO - Calculated TA-Lib indicators for 5000 records
INFO - Saved transformed data for AAPL to processed/tickers/AAPL.parquet
```

## Error Handling Strategy

### Graceful Degradation
**TA-Lib Fallback:**
```python
if TALIB_AVAILABLE:
    df = self.calculate_talib_indicators(df)
else:
    self.logger.warning("Using basic indicators only")
```

**Benefits:**
- Pipeline never fails completely
- Basic indicators still provide value
- Clear logging of capability limitations

### Individual Ticker Isolation
```python
if transformer.transform_ticker(ticker, source, dest):
    successful_transforms += 1
else:
    failed_transforms += 1
```

**Benefits:**
- One bad ticker doesn't stop entire batch
- Failed tickers clearly identified in logs
- Partial success still provides value

### Comprehensive Logging
```python
logging.basicConfig(
    filename="ingestion/transform_debug.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
```

**Log Levels:**
- **INFO**: Normal processing updates
- **WARNING**: Non-critical issues (missing TA-Lib, etc.)
- **ERROR**: Processing failures requiring attention

## Integration Points

### Upstream Dependencies
- **Raw Data**: Requires completed ingestion from ingest_data.py
- **GCS Storage**: Depends on utils/gcs.py for data access
- **Network Access**: Needs internet for S&P 500 ticker list

### Downstream Consumers
- **Analytics Modules**: Use processed data for analysis
- **Dashboard**: Displays transformed data and indicators
- **Machine Learning**: Uses enriched features for model training

### CI/CD Integration
**GitHub Actions Workflow:**
- Scheduled run at 8:30 AM UTC
- Runs after ingestion completes
- Environment variables for configuration
- Artifact logging for monitoring

**Workflow Configuration:**
```yaml
- name: Transform Data
  run: python -m ingestion.transform_data
  env:
    GOOGLE_APPLICATION_CREDENTIALS: ${{ secrets.GCP_SA_KEY }}
```

## Performance Characteristics

### Processing Time
**Estimated Times:**
- S&P 500 stocks (503 tickers): ~15-20 minutes
- ETFs (20 tickers): ~1-2 minutes
- Total processing: ~20-25 minutes

**Factors Affecting Performance:**
- TA-Lib availability (faster with TA-Lib)
- GCS network speed
- Amount of historical data per ticker
- GitHub Actions runner performance

### Memory Usage
**Memory Optimization:**
- Processes one ticker at a time
- Immediate garbage collection after processing
- No large datasets held in memory
- Suitable for GitHub Actions environment

### Storage Efficiency
**Output Format:**
- Parquet format for efficient storage
- Columnar compression for time series data
- Metadata preservation for data lineage

## Configuration Options

### Environment Variables
```bash
# Optional: Custom GCS credentials
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Logging level (INFO, WARNING, ERROR)
LOG_LEVEL=INFO
```

### Processing Options
```python
# Custom source/destination paths
transformer.transform_ticker("AAPL", "custom/source", "custom/dest")

# Custom indicator parameters (would require code modification)
talib.RSI(df['Close'], timeperiod=21)  # Custom RSI period
```

## Usage Examples

### Manual Execution
```bash
# Run complete transformation
python -m ingestion.transform_data

# Check logs
tail -f ingestion/transform_debug.log
```

### Programmatic Usage
```python
from ingestion.transform_data import TechnicalTransformer

# Initialize transformer
transformer = TechnicalTransformer()

# Transform specific ticker
success = transformer.transform_ticker("AAPL", "raw/tickers", "processed/tickers")

# Load and check transformed data
df = transformer.load_ticker_data("AAPL", "processed/tickers")
print(f"Columns: {df.columns.tolist()}")
```

### Custom Indicator Development
```python
def calculate_custom_indicators(self, df):
    """Add custom indicators to the transformation."""
    # Williams %R
    high_14 = df['High'].rolling(window=14).max()
    low_14 = df['Low'].rolling(window=14).min()
    df['WillR'] = -100 * (high_14 - df['Close']) / (high_14 - low_14)
    
    return df
```

## Output Data Schema

### Base Columns (from raw data)
- Date, Ticker, Open, High, Low, Close, Volume

### Basic Indicators (always present)
- SMA_10, SMA_20, SMA_50, SMA_100, SMA_200
- RSI, BB_Upper, BB_Lower
- Volume_SMA_20, Volume_Ratio

### TA-Lib Indicators (when available)
- RSI_TALIB, MACD, MACD_Signal, MACD_Hist
- ATR, OBV
- DOJI, HAMMER (candlestick patterns)

### Total Columns
- **Without TA-Lib**: ~15 columns
- **With TA-Lib**: ~20 columns (basic transformation)
- **Full TA-Lib**: 129+ columns (using enrich_data.py)

## Best Practices

### Operational
1. **Monitor Logs**: Check transform_debug.log for errors
2. **Validate Output**: Spot-check transformed data quality
3. **Resource Monitoring**: Watch GitHub Actions usage
4. **Backup Strategy**: Ensure GCS data is backed up

### Development
1. **Test Locally**: Validate changes with small datasets
2. **Environment Consistency**: Match production TA-Lib setup
3. **Error Handling**: Add robust error handling for new indicators
4. **Documentation**: Update docs when adding new features

### Data Quality
1. **Input Validation**: Verify raw data quality before transformation
2. **Output Validation**: Check indicator calculations for sanity
3. **Historical Consistency**: Ensure transformed data is time-consistent
4. **Missing Data**: Handle gaps in time series appropriately

## Troubleshooting

### Common Issues

#### TA-Lib Import Errors
```
❌ Warning: TA-Lib not available: No module named 'talib'
```
**Solutions:**
- Install TA-Lib C library first
- Use pip install TA-Lib with appropriate binaries
- Check system-specific installation instructions

#### GCS Access Errors
```
ERROR - Error loading data for AAPL: 403 Forbidden
```
**Solutions:**
- Verify GOOGLE_APPLICATION_CREDENTIALS
- Check GCS bucket permissions
- Validate service account access

#### Memory Issues
```
ERROR - Memory error during transformation
```
**Solutions:**
- Process smaller batches
- Increase GitHub Actions runner memory
- Optimize data types in calculations

### Debug Commands
```python
# Check TA-Lib availability
import talib
print("TA-Lib version:", talib.__version__)

# Test GCS connectivity
from utils.gcs import blob_exists
print(blob_exists("candlethrob-candata", "raw/tickers/AAPL.parquet"))

# Validate data structure
df = transformer.load_ticker_data("AAPL")
print(f"Shape: {df.shape}")
print(f"Columns: {df.columns.tolist()}")
```

## Future Enhancements

### Potential Improvements
1. **Parallel Processing**: Process multiple tickers simultaneously
2. **Delta Processing**: Only transform updated data
3. **Custom Indicators**: Add more domain-specific indicators
4. **Data Validation**: Implement comprehensive data quality checks
5. **Caching**: Cache intermediate calculations for performance

### Scalability Considerations
1. **Batch Size**: Optimize for larger ticker universes
2. **Cloud Resources**: Consider more powerful compute instances
3. **Storage Optimization**: Implement data partitioning strategies
4. **Monitoring**: Add detailed performance metrics

**Author**: Adetunji Fasiku
