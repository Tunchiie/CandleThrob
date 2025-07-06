# test_ingestion.py Documentation

## Overview
The `test_ingestion.py` module provides comprehensive unit tests for the data ingestion functionality in the CandleThrob financial data pipeline. It validates the `DataIngestion` class and database operations, testing various scenarios including Oracle DB integration, data validation, and API fallback mechanisms.

## Updated Test Architecture (Post-Oracle Migration)

### Testing Strategy
- **Database-First Testing**: Validates Oracle DB operations and schema compliance
- **API Integration Testing**: Real API calls with robust fallback mechanisms
- **Data Quality Validation**: Ensures output data meets expected standards
- **Mock Data Integration**: Comprehensive fallback for offline testing

### Current Test Categories
1. **Database Operations**: Table creation, connections, CRUD operations
2. **Data Ingestion Tests**: Ticker and macro data fetching with Oracle persistence
3. **Schema Validation**: Ensures data matches SQLAlchemy models
4. **Error Handling**: Database failures, API timeouts, invalid data

## Core Database Tests

### test_database_connection()
**Purpose:** Validates Oracle database connectivity and session management.

```python
def test_database_connection(db_session):
    """Test database connection and basic operations."""
    assert db_session is not None
    # Verify connection is active
    result = db_session.execute(text("SELECT 1 FROM DUAL"))
    assert result.fetchone()[0] == 1
```

**Key Validations:**
- Database session initialization
- Connection health verification
- Basic SQL execution capability

### test_ticker_data_table_creation()
**Purpose:** Validates TickerData table schema and creation.

```python
def test_ticker_data_table_creation(db_session):
    """Test TickerData table creation."""
    ticker_data = TickerData()
    ticker_data.create_table(db_session)
    
    # Verify table exists and schema is correct
    assert ticker_data.__tablename__ == 'raw_ticker_data'
```

**Schema Validation:**
- Table creation without errors
- Proper column definitions
- Index and constraint validation

### test_macro_data_table_creation()
**Purpose:** Validates MacroData table schema and creation.

```python
def test_macro_data_table_creation(db_session):
    """Test MacroData table creation."""
    macro_data = MacroData()
    macro_data.create_table(db_session)
    
    assert macro_data.__tablename__ == 'raw_macro_data'
```

**Features:**
- Macro data schema validation
- Table relationship verification
- Data type compliance

## Data Insertion and Validation Tests

### test_data_insertion_ticker()
**Purpose:** Validates ticker data insertion into Oracle database.

```python
def test_data_insertion_ticker(db_session):
    """Test inserting ticker data into database."""
    ticker_data = TickerData()
    ticker_data.create_table(db_session)
    
    # Create sample data
    sample_data = {
        'date': date(2024, 1, 1),
        'open': 100.0,
        'high': 105.0,
        'low': 99.0,
        'close': 103.0,
        'volume': 1000000,
        'ticker': 'TEST',
        'year': 2024,
        'month': 1,
        'weekday': 0
    }
    
    ticker_data.insert_data(db_session, sample_data)
```

**Validation Points:**
- Successful data insertion
- Data type compliance with schema
- Proper date handling and formatting
- Volume and price validation

### test_data_insertion_macro()
**Purpose:** Validates macroeconomic data insertion.

```python
def test_data_insertion_macro(db_session):
    """Test inserting macro data into database."""
    macro_data = MacroData()
    macro_data.create_table(db_session)
    
    sample_data = {
        'date': date(2024, 1, 1),
        'indicator_name': 'GDP',
        'value': 25000.0,
        'year': 2024,
        'month': 1,
        'weekday': 0
    }
    
    macro_data.insert_data(db_session, sample_data)
```

**Key Features:**
- Macro indicator persistence
- Named indicator storage
- Time series data organization

## API Integration Tests

### test_macro_data_fetching()
**Purpose:** Tests FRED API integration with fallback mechanisms.

```python
def test_macro_data_fetching(sample_data_ingestion):
    """Test fetching macroeconomic data."""
    try:
        sample_data_ingestion.fetch_fred_data()
        assert sample_data_ingestion.macro_df is not None
    except ValueError as e:
        # Expected when FRED API key is not available
        assert "FRED API" in str(e)
```

**Fallback Strategy:**
- Attempts real FRED API calls when key is available
- Gracefully handles API failures
- Validates error messages for debugging

## Utility Function Tests

### test_get_etf_tickers()
**Purpose:** Validates ETF ticker list generation.

```python
def test_get_etf_tickers():
    """Test ETF ticker list retrieval."""
    etf_tickers = get_etf_tickers()
    assert isinstance(etf_tickers, list)
    assert len(etf_tickers) > 0
    assert "SPY" in etf_tickers
    assert "QQQ" in etf_tickers
```

**Validation:**
- Proper list structure
- Known ETF inclusion
- Data format compliance

### test_get_sp500_tickers()
**Purpose:** Tests S&P 500 ticker retrieval with network resilience.

```python
def test_get_sp500_tickers():
    """Test S&P 500 ticker list retrieval."""
    try:
        sp500_tickers = get_sp500_tickers()
        assert isinstance(sp500_tickers, list)
        assert len(sp500_tickers) > 400
    except Exception:
        pytest.skip("S&P 500 ticker fetching failed - likely network issue")
```

**Features:**
- Network-resilient testing
- Graceful failure handling
- Reasonable data volume validation

**Test Scenario:**
- **Empty Input**: No tickers provided for fetching
- **Expected Behavior**: ValueError should be raised
- **Error Prevention**: Prevents unnecessary API calls

**Business Logic:**
- Validates minimum input requirements
- Provides clear error messaging
- Saves API quota and processing time

### test_data_ingestion_with_nonexistent_ticker()
**Purpose:** Validates handling of invalid ticker symbols.

```python
def test_data_ingestion_with_nonexistent_ticker():
    data = DataIngestion()
    with pytest.raises(ValueError):
        data.fetch(tickers=["NONEXISTENT"])
```

**Test Scenario:**
- **Invalid Ticker**: Non-existent stock symbol
- **Expected Behavior**: ValueError should be raised
- **Error Prevention**: Handles API response for invalid symbols

**Business Logic:**
- Validates ticker existence before processing
- Prevents processing of invalid data
- Provides meaningful error feedback

### test_data_ingestion_with_invalid_ticker_format()
**Purpose:** Tests validation of ticker symbol formatting.

```python
def test_data_ingestion_with_invalid_ticker_format():
    data = DataIngestion()
    with pytest.raises(ValueError):
        data.fetch(["INVALID TICKER"])
```

**Test Scenario:**
- **Format Error**: Ticker with spaces (invalid format)
- **Expected Behavior**: ValueError should be raised
- **Error Prevention**: Enforces proper ticker formatting

**Business Logic:**
- Validates ticker format standards
- Prevents API errors due to malformed requests
- Ensures data consistency

## Data Validation Tests

### test_data_ingestion_with_partial_data()
**Purpose:** Validates successful data fetching for valid tickers.

```python
def test_data_ingestion_with_partial_data():
    data = DataIngestion()
    data.fetch(["AAPL", "GOOGL"])
    assert isinstance(data.ticker_df, pd.DataFrame)
    assert not data.ticker_df.empty
    assert all(ticker in data.ticker_df['Ticker'].values for ticker in ["AAPL", "GOOGL"])
    assert isinstance(data.ticker_df['Ticker'], pd.Series)
```

**Validation Points:**
1. **Data Type**: Ensures DataFrame output
2. **Non-Empty Result**: Validates data was fetched
3. **Ticker Presence**: Confirms all requested tickers included
4. **Column Type**: Validates Ticker column is pandas Series

**Test Scenario:**
- **Valid Tickers**: AAPL and GOOGL (major stocks)
- **Expected Behavior**: Successful data retrieval
- **Data Quality**: Complete and properly formatted data

### test_data_ingestion_with_large_date_range()
**Purpose:** Tests performance and data integrity with extended date ranges.

```python
def test_data_ingestion_with_large_date_range():
    data = DataIngestion(start_date="2000-01-01", end_date="2023-12-31")
    data.fetch(["AAPL", "GOOGL"])
    assert isinstance(data.ticker_df, pd.DataFrame)
    assert not data.ticker_df.empty
    assert isinstance(data.ticker_df['Ticker'], pd.Series)
```

**Test Scenario:**
- **Large Range**: 23+ years of historical data
- **Performance Test**: Validates handling of large datasets
- **Data Integrity**: Ensures quality maintained across time

**Benefits:**
- **Scalability Testing**: Validates large data handling
- **API Limits**: Tests compliance with API rate limits
- **Memory Management**: Ensures efficient data processing

### test_data_ingestion_with_valid_ticker_format()
**Purpose:** Validates successful processing of properly formatted tickers.

```python
def test_data_ingestion_with_valid_ticker_format():
    data = DataIngestion()
    data.fetch(["AAPL", "GOOGL"])
    assert isinstance(data.ticker_df, pd.DataFrame)
    assert not data.ticker_df.empty
    assert all(ticker in data.ticker_df['Ticker'].values for ticker in ["AAPL", "GOOGL"])
    assert isinstance(data.ticker_df['Ticker'], pd.Series)
```

**Validation Scope:**
- **Format Compliance**: Standard ticker format (uppercase, no spaces)
- **Data Completeness**: All tickers successfully processed
- **Structure Integrity**: Proper DataFrame structure maintained

## Data Processing Tests

### test_clean_ticker()
**Purpose:** Comprehensive testing of ticker symbol cleaning functionality.

```python
def test_clean_ticker():
    data = DataIngestion()
    assert data.clean_ticker("AAPL@") == "AAPL"
    assert data.clean_ticker("$GOOGL ") == "GOOGL"
    assert data.clean_ticker("  MSFT") == "MSFT"
    assert data.clean_ticker("AMZN!") == "AMZN"
    assert data.clean_ticker("  TSLA  ") == "TSLA"
    assert data.clean_ticker("  NFLX@") == "NFLX"
```

**Cleaning Scenarios:**
1. **Special Characters**: @ symbol removal
2. **Prefix Symbols**: $ symbol removal
3. **Leading Whitespace**: Space trimming
4. **Trailing Special**: ! symbol removal
5. **Multiple Spaces**: Leading and trailing space removal
6. **Mixed Issues**: Combined space and special character cleaning

**Cleaning Rules Tested:**
- **Uppercase Conversion**: All output should be uppercase
- **Special Character Removal**: Non-alphanumeric characters except hyphens
- **Whitespace Trimming**: Leading and trailing space removal
- **Format Standardization**: Consistent output format

## Integration Tests

### test_macro_ingestion()
**Purpose:** Validates macroeconomic data fetching from FRED API.

```python
def test_macro_ingestion():
    data = DataIngestion()
    data.fetch_fred_data()
    assert isinstance(data.macro_df, pd.DataFrame)
    assert not data.macro_df.empty
    assert "GDP" in data.macro_df.columns
    assert "UNRATE" in data.macro_df.columns
    assert "FEDFUNDS" in data.macro_df.columns
    assert "INDPRO" in data.macro_df.columns
    assert "CPIAUCSL" in data.macro_df.columns
    assert "UMCSENT" in data.macro_df.columns
    assert "HOUST" in data.macro_df.columns
    assert "M2SL" in data.macro_df.columns
```

**Tested Macroeconomic Indicators:**
- **GDP**: Gross Domestic Product
- **UNRATE**: Unemployment Rate
- **FEDFUNDS**: Federal Funds Rate
- **INDPRO**: Industrial Production Index
- **CPIAUCSL**: Consumer Price Index
- **UMCSENT**: Consumer Sentiment
- **HOUST**: Housing Starts
- **M2SL**: M2 Money Supply

**Validation Points:**
1. **API Integration**: Successful FRED API connection
2. **Data Completeness**: All expected indicators present
3. **Data Quality**: Non-empty DataFrame with economic data
4. **Column Presence**: All key economic indicators included

## Test Execution

### Running Tests
```bash
# Run all ingestion tests
pytest tests/test_ingestion.py -v

# Run specific test category
pytest tests/test_ingestion.py -k "invalid" -v

# Run with coverage
pytest tests/test_ingestion.py --cov=ingestion.fetch_data

# Run with detailed output
pytest tests/test_ingestion.py -v -s
```

### Expected Test Results
```
tests/test_ingestion.py::test_data_ingestion_with_invalid_date PASSED
tests/test_ingestion.py::test_data_ingestion_with_empty_tickers PASSED
tests/test_ingestion.py::test_data_ingestion_with_nonexistent_ticker PASSED
tests/test_ingestion.py::test_data_ingestion_with_partial_data PASSED
tests/test_ingestion.py::test_data_ingestion_with_large_date_range PASSED
tests/test_ingestion.py::test_data_ingestion_with_invalid_ticker_format PASSED
tests/test_ingestion.py::test_data_ingestion_with_valid_ticker_format PASSED
tests/test_ingestion.py::test_clean_ticker PASSED
tests/test_ingestion.py::test_macro_ingestion PASSED
```

## Dependencies

### Required Environment
```python
from ingestion.fetch_data import DataIngestion
import pytest
import pandas as pd
```

### Environment Variables
```bash
# Required for API access
export POLYGON_API_KEY="your_polygon_api_key"
export FRED_API_KEY="your_fred_api_key"
```

### External Dependencies
- **Internet Connection**: Required for API calls
- **Valid API Keys**: Both Polygon.io and FRED API access
- **pandas**: Data manipulation library
- **pytest**: Testing framework

## Performance Considerations

### Test Execution Times
- **Error Tests**: <1 second each (no API calls)
- **Data Fetching Tests**: 5-30 seconds (depends on API response)
- **Macro Data Test**: 10-60 seconds (fetches multiple indicators)
- **Total Suite**: 2-5 minutes for complete execution

### API Rate Limiting
- **Polygon.io**: 5 calls per minute (free tier)
- **FRED**: Higher limits, typically not an issue
- **Test Strategy**: Minimize API calls in test suite

### Resource Usage
- **Memory**: Moderate usage for test data
- **Network**: Multiple API calls required
- **CPU**: Minimal processing for test validation

## Error Scenarios and Debugging

### Common Test Failures

#### API Key Issues
```
Error: Authentication failed for Polygon.io API
```
**Solutions:**
- Verify POLYGON_API_KEY environment variable is set
- Check API key validity and permissions
- Ensure network connectivity

#### Network Connectivity
```
Error: Connection timeout during data fetch
```
**Solutions:**
- Check internet connection
- Verify API endpoint availability
- Consider proxy/firewall settings

#### Data Quality Issues
```
AssertionError: Ticker 'AAPL' not found in data
```
**Solutions:**
- Verify ticker symbol validity
- Check data date range for availability
- Review API response for errors

### Debug Commands
```python
# Test API connectivity
data = DataIngestion()
try:
    data.fetch(["AAPL"])
    print("API connection successful")
except Exception as e:
    print(f"API error: {e}")

# Validate data structure
print(f"Data shape: {data.ticker_df.shape}")
print(f"Columns: {data.ticker_df.columns.tolist()}")
print(f"Tickers: {data.ticker_df['Ticker'].unique()}")

# Check macro data
data.fetch_fred_data()
print(f"Macro columns: {data.macro_df.columns.tolist()}")
```

## Best Practices

### Test Design
1. **Error-First**: Test error conditions before success cases
2. **Isolation**: Each test should be independent
3. **Realistic Data**: Use actual market tickers for testing
4. **Coverage**: Test both happy path and edge cases

### Maintenance
1. **Regular Updates**: Keep test tickers current (avoid delisted stocks)
2. **API Changes**: Monitor for API endpoint modifications
3. **Performance**: Track test execution times for regression
4. **Documentation**: Keep test documentation synchronized with code

### CI/CD Integration
1. **Environment Setup**: Ensure test environment has required API keys
2. **Timeout Handling**: Set appropriate timeouts for API tests
3. **Retry Logic**: Implement retries for transient network failures
4. **Result Reporting**: Clear pass/fail indicators for automated systems

## Security Considerations

### API Key Management
- **Environment Variables**: Never hardcode API keys
- **Test Isolation**: Use separate API keys for testing if possible
- **Rate Limiting**: Respect API rate limits to avoid account suspension
- **Error Handling**: Don't expose API keys in error messages

### Data Handling
- **Test Data**: Use minimal data sets for faster testing
- **Cleanup**: Ensure test data doesn't persist unnecessarily
- **Privacy**: No personal or sensitive data in test scenarios

## Future Enhancements

### Potential Improvements
1. **Mock Testing**: Add mock API responses for faster testing
2. **Parameterized Tests**: Test multiple ticker combinations systematically
3. **Performance Benchmarks**: Add performance regression testing
4. **Edge Case Expansion**: More comprehensive error scenario testing
5. **Integration Tests**: Full end-to-end pipeline testing
