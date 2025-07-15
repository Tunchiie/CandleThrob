# fetch_data.py Documentation

## Overview
The `fetch_data.py` module is the core data retrieval component of the CandleThrob financial data pipeline. It provides a unified interface for fetching OHLCV (Open, High, Low, Close, Volume) stock data from Polygon.io and macroeconomic data from the Federal Reserve Economic Data (FRED) API.

## Class: DataIngestion

### Purpose
The `DataIngestion` class serves as the primary interface for retrieving financial data from external APIs. It has been refactored to use Polygon.io as the sole source for OHLCV data, ensuring consistency and quality in the financial data pipeline.

### Constructor
```python
def __init__(self, start_date=None, end_date=None)
```

**Parameters:**
- `start_date` (str, optional): Start date in 'YYYY-MM-DD' format. Defaults to 50 years before end_date
- `end_date` (str, optional): End date in 'YYYY-MM-DD' format. Defaults to today's date

**Attributes:**
- `ticker_df` (pd.DataFrame): Stores historical stock data for all tickers
- `macro_df` (pd.DataFrame): Stores macroeconomic data
- `start_date` (datetime): Parsed start date
- `end_date` (datetime): Parsed end date

**Raises:**
- `ValueError`: If start_date is after end_date

### Methods

#### clean_ticker(ticker: str) -> str
**Static method** for cleaning ticker symbols.

**Purpose:** Removes unwanted characters and formats ticker symbols consistently.

**Parameters:**
- `ticker` (str): Raw ticker symbol

**Returns:**
- `str`: Cleaned ticker symbol (uppercase, alphanumeric with hyphens only)

**Example:**
```python
clean_ticker("AAPL.") # Returns "AAPL"
clean_ticker("brk-b") # Returns "BRK-B"
```

#### ingest_ticker_data(ticker: str) -> pd.DataFrame
**Main method** for fetching individual ticker data.

**Purpose:** Retrieves OHLCV data for a single ticker using Polygon.io API exclusively.

**Parameters:**
- `ticker` (str): Stock ticker symbol

**Returns:**
- `pd.DataFrame`: DataFrame with columns:
  - Date (datetime): Trading date
  - Open (float): Opening price
  - High (float): Highest price
  - Low (float): Lowest price
  - Close (float): Closing price
  - Volume (int): Trading volume
  - Ticker (str): Stock symbol
  - Year (int): Year extracted from date
  - Month (int): Month extracted from date
  - Weekday (int): Weekday extracted from date

**Error Handling:**
- Returns empty DataFrame on API errors
- Logs warnings for empty tickers
- Handles network timeouts and API rate limits

#### _fetch_polygon_data(ticker: str) -> pd.DataFrame
**Private method** for Polygon.io API interaction.

**Purpose:** Handles the low-level communication with Polygon.io aggregates endpoint.

**Parameters:**
- `ticker` (str): Stock ticker symbol

**Returns:**
- `pd.DataFrame`: OHLCV data or empty DataFrame on failure

**API Configuration:**
- Endpoint: `/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{to_date}`
- Parameters:
  - `adjusted`: true (split/dividend adjusted)
  - `sort`: asc (chronological order)
  - `limit`: 50000 (maximum data points)
  - `timeout`: 30 seconds

**Error Handling:**
- `requests.RequestException`: Network/HTTP errors
- `KeyError`: Missing data fields in response
- `ValueError`/`TypeError`: Data parsing errors
- Logs all errors with ticker context

#### ingest_tickers(tickers: list) -> None
**Batch processing method** for multiple tickers.

**Purpose:** Fetches OHLCV data for multiple tickers with rate limiting and progress tracking.

**Parameters:**
- `tickers` (list): List of ticker symbols

**Side Effects:**
- Updates `self.ticker_df` with combined data
- Implements 12-second delays between API calls for rate limiting
- Shows progress bar using tqdm

**Rate Limiting:**
- 5 calls per minute (12 seconds between requests)
- Complies with Polygon.io free tier limits

**Data Processing:**
- Concatenates all ticker data
- Sorts by date
- Converts Date column to datetime
- Logs summary statistics

#### fetch_fred_data() -> None
**Macroeconomic data method** for FRED API integration.

**Purpose:** Retrieves key economic indicators from the Federal Reserve Economic Data API.

**Data Sources:**
- FEDFUNDS: Federal Funds Rate
- CPIAUCSL: Consumer Price Index
- UNRATE: Unemployment Rate
- GDP: Gross Domestic Product
- GS10: 10-Year Treasury Rate
- USREC: Recession Indicator
- UMCSENT: Consumer Sentiment
- HOUST: Housing Starts
- RSAFS: Retail Sales
- INDPRO: Industrial Production Index
- M2SL: M2 Money Supply

**Processing:**
- Resamples data to daily frequency
- Forward fills missing values
- Backward fills initial missing values
- Merges with existing macro data if present

**Requirements:**
- FRED_API_KEY environment variable must be set
- Active internet connection

**Error Handling:**
- Raises `RuntimeError` if API key missing
- Raises `ValueError` if no data returned
- Logs individual series fetch errors

## Dependencies

### Required Packages
- `pandas`: Data manipulation and analysis
- `requests`: HTTP client for API calls
- `fredapi`: FRED API client
- `tqdm`: Progress bar display
- `logging`: Error and info logging
- `datetime`: Date/time handling

### Environment Variables
- `POLYGON_API_KEY`: Required for stock data access
- `FRED_API_KEY`: Required for macroeconomic data access

## Usage Examples

### Basic Usage
```python
from ingestion.fetch_data import DataIngestion

# Initialize with date range
data = DataIngestion(start_date="2020-01-01", end_date="2023-12-31")

# Fetch single ticker
aapl_data = data.ingest_ticker_data("AAPL")

# Fetch multiple tickers
tickers = ["AAPL", "GOOGL", "MSFT"]
data.ingest_tickers(tickers)

# Fetch macroeconomic data
data.fetch_fred_data()
```

### Error Handling Example
```python
try:
    data = DataIngestion(start_date="2023-01-01", end_date="2022-01-01")
except ValueError as e:
    print(f"Date error: {e}")

# Check for empty results
ticker_data = data.ingest_ticker_data("INVALID")
if ticker_data.empty:
    print("No data retrieved")
```

## Performance Considerations

### Rate Limiting
- Polygon.io free tier: 5 calls per minute
- Implementation uses 12-second delays
- Suitable for batch processing outside market hours

### Memory Usage
- Data accumulated in memory during batch processing
- Consider processing large ticker lists in chunks
- DataFrame operations optimized for performance

### Error Resilience
- Individual ticker failures don't stop batch processing
- Comprehensive logging for debugging
- Graceful degradation when services unavailable

## Integration Points

### With Other Modules
- Used by `ingest_data.py` for data collection
- Provides data to `enrich_data.py` for indicator calculation
- Integrates with `utils/gcs.py` for cloud storage

### CI/CD Integration
- Compatible with GitHub Actions environment
- Handles environment variable configuration
- Logs suitable for CI/CD monitoring

## Best Practices

### Configuration
1. Set environment variables in secure location
2. Use appropriate date ranges for data needs
3. Monitor API usage to avoid rate limits

### Error Handling
1. Always check for empty DataFrames
2. Implement retry logic for critical operations
3. Monitor logs for API issues

### Performance
1. Use batch processing for multiple tickers
2. Implement appropriate delays for rate limiting
3. Consider caching for frequently accessed data

## Changelog

### Recent Updates
- **Polygon.io Only**: Removed yfinance and Alpha Vantage dependencies
- **Enhanced Error Handling**: Added comprehensive try/catch blocks
- **Rate Limiting**: Implemented proper delays for API compliance
- **Logging**: Enhanced logging for better debugging
- **Data Quality**: Improved data validation and cleaning

**Author**: Adetunji Fasiku
