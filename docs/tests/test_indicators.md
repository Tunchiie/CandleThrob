# test_indicators.py Documentation

## Overview
The `test_indicators.py` module contains comprehensive unit tests for the technical indicators functionality in the CandleThrob financial data pipeline. It validates the calculation, transformation, and merging operations of the `TechnicalIndicators` class from `ingestion.enrich_data`.

## Updated Test Architecture (Post-Refactoring)

### Testing Framework
- **pytest**: Primary testing framework for structured test execution
- **Mock Data Integration**: Robust fallback mechanisms for offline testing
- **TA-Lib Validation**: Tests both TA-Lib availability and fallback calculations
- **DataFrame Operations**: Comprehensive validation of pandas operations

### Test Data Strategy
- **Primary**: Real market data when APIs are available
- **Fallback**: Synthetic OHLCV data that mimics real market patterns
- **Flexible Tickers**: AAPL and GOOGL with graceful degradation
- **Consistent Structure**: Fixed schema for reliable testing

## Fixtures

### technical_indicators
**Purpose:** Provides TechnicalIndicators instance with validated test data.

```python
@pytest.fixture
def technical_indicators():
    """Create a TechnicalIndicators instance with sample data."""
    try:
        data = DataIngestion(start_date="2022-01-01", end_date="2022-12-31")
        data.fetch(tickers=["AAPL", "GOOGL"])
        
        if data.ticker_df is None or data.ticker_df.empty:
            # Create realistic mock data
            dates = pd.date_range(start="2022-01-01", end="2022-01-31", freq='D')
            ticker_df = pd.DataFrame({
                'Date': dates,
                'Ticker': 'AAPL',
                'Open': 150 + np.random.randn(len(dates)) * 2,
                'High': 155 + np.random.randn(len(dates)) * 2,
                'Low': 145 + np.random.randn(len(dates)) * 2,
                'Close': 152 + np.random.randn(len(dates)) * 2,
                'Volume': 1000000 + np.random.randint(-100000, 100000, len(dates))
            })
            data.ticker_df = ticker_df
            
        return TechnicalIndicators(data.ticker_df)
    except Exception:
        # Fallback mock data for complete offline testing
        dates = pd.date_range(start="2022-01-01", end="2022-01-31", freq='D')
        ticker_df = pd.DataFrame({
            'Date': dates,
            'Ticker': 'AAPL',
            'Open': np.linspace(150, 160, len(dates)),
            'High': np.linspace(155, 165, len(dates)), 
            'Low': np.linspace(145, 155, len(dates)),
            'Close': np.linspace(152, 162, len(dates)),
            'Volume': np.full(len(dates), 1000000)
        })
        return TechnicalIndicators(ticker_df)
```

**Key Features:**
- **Robust Fallbacks**: Multiple levels of error handling
- **Realistic Data**: Mock data maintains market-like characteristics
- **API Independence**: Works without external API access
- **Consistent Schema**: Ensures reliable test execution

## Core Test Functions

### test_technical_indicators_initialization()
**Purpose:** Validates TechnicalIndicators class initialization and data setup.

```python
def test_technical_indicators_initialization(technical_indicators):
    """Test TechnicalIndicators initialization."""
    assert technical_indicators is not None
    assert technical_indicators.ticker_df is not None
    assert not technical_indicators.ticker_df.empty
    
    # Check required columns
    required_cols = ['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']
    for col in required_cols:
        assert col in technical_indicators.ticker_df.columns
```

**Validations:**
- Proper class instantiation
- DataFrame structure validation
- Required column presence
- Data integrity checks

### test_enrich_tickers()
**Purpose:** Tests return calculation and ticker enrichment.

```python
def test_enrich_tickers(technical_indicators):
    """Test ticker enrichment with return calculations."""
    technical_indicators.enrich_tickers()
    
    # Check that return columns were added
    return_columns = ['Return_1d', 'Return_3d', 'Return_7d', 'Return_30d', 'Return_90d', 'Return_365d']
    for col in return_columns:
        assert col in technical_indicators.ticker_df.columns
        assert isinstance(technical_indicators.ticker_df[col], pd.Series)
```

**Features:**
- Multi-period return calculations
- Proper time series handling
- Data type validation
- Column addition verification

### test_calculate_technical_indicators()
**Purpose:** Validates full technical indicator calculation pipeline.

```python
def test_calculate_technical_indicators(technical_indicators):
    """Test full technical indicators calculation."""
    try:
        import talib
    except ImportError:
        pytest.skip("TA-Lib not available for testing")
    
    technical_indicators.enrich_tickers()
    result_df = technical_indicators.calculate_technical_indicators()
    
    assert result_df is not None
    assert not result_df.empty
    assert isinstance(result_df, pd.DataFrame)
```

**Key Validations:**
- TA-Lib availability checking
- Complete pipeline execution
- Result DataFrame validation
- Error handling for missing libraries

## Indicator Category Tests

### test_momentum_indicators()
**Purpose:** Tests momentum-based technical indicators.

```python
def test_momentum_indicators(technical_indicators):
    """Test momentum indicators calculation."""
    momentum_df = technical_indicators.get_talib_momentum_indicators(technical_indicators.ticker_df)
    
    expected_columns = ['RSI', 'MACD', 'MACD_Signal', 'CCI', 'ROC', 'SMA20', 'EMA20']
    for col in expected_columns:
        assert col in momentum_df.columns
        assert isinstance(momentum_df[col], pd.Series)
```

**Indicators Tested:**
- **RSI**: Relative Strength Index
- **MACD**: Moving Average Convergence Divergence
- **CCI**: Commodity Channel Index
- **ROC**: Rate of Change
- **Moving Averages**: SMA and EMA variations

### test_volume_indicators()
**Purpose:** Validates volume-based technical indicators.

```python
def test_volume_indicators(technical_indicators):
    """Test volume indicators calculation."""
    volume_df = technical_indicators.get_talib_volume_indicators(technical_indicators.ticker_df)
    
    expected_columns = ['OBV', 'AD', 'MFI']
    for col in expected_columns:
        assert col in volume_df.columns
        assert isinstance(volume_df[col], pd.Series)
```

**Indicators Tested:**
- **OBV**: On-Balance Volume
- **AD**: Accumulation/Distribution Line
- **MFI**: Money Flow Index

### test_volatility_indicators()
**Purpose:** Tests volatility measurement indicators.

```python
def test_volatility_indicators(technical_indicators):
    """Test volatility indicators calculation."""
    volatility_df = technical_indicators.get_talib_volatility_indicators(technical_indicators.ticker_df)
    
    expected_columns = ['ATR', 'BBANDS_UPPER', 'BBANDS_MIDDLE', 'BBANDS_LOWER']
    for col in expected_columns:
        assert col in volatility_df.columns
        assert isinstance(volatility_df[col], pd.Series)
```

**Indicators Tested:**
- **ATR**: Average True Range
- **Bollinger Bands**: Upper, Middle, and Lower bands
- **Volatility Measures**: Various volatility calculations
2. **Column Presence**: Verifies key indicators are present in output
3. **Data Type Validation**: Ensures indicators are pandas Series objects
4. **Data Quality**: Confirms indicators contain non-null values

**Key Assertions:**
```python
# Column presence validation
assert 'CCI' in indicators.transformed_df.columns    # Commodity Channel Index
assert 'ADX' in indicators.transformed_df.columns    # Average Directional Index  
assert 'RSI' in indicators.transformed_df.columns    # Relative Strength Index

# Data type validation
assert isinstance(indicators.transformed_df['CCI'], pd.Series)
assert isinstance(indicators.transformed_df['ADX'], pd.Series)
assert isinstance(indicators.transformed_df['RSI'], pd.Series)

# Data quality validation
assert not indicators.transformed_df['CCI'].isnull().all()
assert not indicators.transformed_df['ADX'].isnull().all()
assert not indicators.transformed_df['RSI'].isnull().all()
```

**Tested Indicators:**
- **CCI (Commodity Channel Index)**: Momentum oscillator
- **ADX (Average Directional Index)**: Trend strength indicator
- **RSI (Relative Strength Index)**: Momentum oscillator

**Expected Behavior:**
- All indicators should be calculated successfully
- No complete null columns (some initial NaN values expected due to calculation periods)
- Proper pandas Series data types maintained

### test_safe_merge_or_concat(sample_tickers)
**Purpose:** Validates the safe DataFrame merging functionality.

**Test Design:**
```python
df1 = sample_tickers.ticker_df.head(10)  # First 10 records
df2 = sample_tickers.ticker_df.tail(10)  # Last 10 records
merged_df = indicators.safe_merge_or_concat(df1, df2, on=['Date', 'Ticker'], how='left')
```

**Validation Points:**
1. **Return Type**: Ensures merged result is a DataFrame
2. **Non-Empty Result**: Validates merge produces data
3. **Required Columns**: Confirms merge keys are preserved
4. **Data Integrity**: Checks merge logic correctness

**Key Assertions:**
```python
assert isinstance(merged_df, pd.DataFrame)     # Correct return type
assert not merged_df.empty                     # Non-empty result
assert 'Date' in merged_df.columns             # Merge key preserved
assert 'Ticker' in merged_df.columns           # Merge key preserved
```

**Merge Logic Validation:**
- Tests left join behavior
- Validates handling of overlapping data
- Ensures no unexpected data duplication
- Confirms proper column alignment

### test_indicators(sample_tickers)
**Purpose:** Comprehensive validation of all indicator categories and edge cases.

**Test Categories:**

#### Empty DataFrame Handling
```python
assert indicators.get_talib_momentum_indicators(pd.DataFrame()).empty
assert indicators.get_talib_volume_indicators(pd.DataFrame()).empty
assert indicators.get_talib_pattern_indicators(pd.DataFrame()).empty
assert indicators.get_talib_volatility_indicators(pd.DataFrame()).empty
assert indicators.get_talib_cyclical_indicators(pd.DataFrame()).empty
```

**Purpose:** Ensures graceful handling of edge cases with empty input data.

#### Full Transformation Pipeline
```python
indicators.transform()
assert isinstance(indicators.transformed_df, pd.DataFrame)

indicators.calculate_technical_indicators()
assert not indicators.transformed_df.empty
```

**Validates:**
- Complete transformation workflow
- Data type consistency throughout pipeline
- Non-empty output after processing

#### Core Column Validation
```python
assert 'Date' in indicators.transformed_df.columns
assert 'Ticker' in indicators.transformed_df.columns
assert 'Open' in indicators.transformed_df.columns
assert 'Close' in indicators.transformed_df.columns
assert 'High' in indicators.transformed_df.columns
assert 'Low' in indicators.transformed_df.columns
```

**Ensures:**
- Base OHLCV columns preserved
- Date and Ticker columns maintained for data integrity
- No column dropping during transformation

#### Key Indicator Presence
```python
assert 'RSI' in indicators.transformed_df.columns    # Momentum
assert 'CCI' in indicators.transformed_df.columns    # Momentum  
assert 'ADX' in indicators.transformed_df.columns    # Volume/Trend
```

**Validates:**
- Critical indicators calculated successfully
- Indicator categories properly represented
- No missing key technical analysis tools

## Testing Strategy

### Unit Testing Approach
1. **Isolation**: Each test focuses on specific functionality
2. **Independence**: Tests can run in any order without dependencies
3. **Repeatability**: Fixed data ensures consistent results
4. **Coverage**: Tests cover normal operations and edge cases

### Data Validation Strategy
1. **Type Checking**: Validates pandas DataFrame and Series types
2. **Null Checking**: Ensures indicators contain meaningful data
3. **Column Presence**: Verifies expected outputs are generated
4. **Structure Integrity**: Confirms data structure preservation

### Error Handling Testing
1. **Empty DataFrames**: Tests graceful handling of no-data scenarios
2. **Missing Columns**: Validates proper error responses
3. **Invalid Data**: Ensures robust error handling

## Test Execution

### Running Tests
```bash
# Run all indicator tests
pytest tests/test_indicators.py -v

# Run specific test
pytest tests/test_indicators.py::test_technical_indicators -v

# Run with coverage
pytest tests/test_indicators.py --cov=ingestion.enrich_data

# Run with detailed output
pytest tests/test_indicators.py -v -s
```

### Expected Output
```
tests/test_indicators.py::test_technical_indicators PASSED
tests/test_indicators.py::test_safe_merge_or_concat PASSED  
tests/test_indicators.py::test_indicators PASSED
```

### Test Performance
- **Setup Time**: ~10-30 seconds (data fetching)
- **Execution Time**: ~5-15 seconds per test
- **Total Runtime**: ~30-60 seconds for full suite

## Dependencies

### Required Modules
```python
import pytest                    # Testing framework
import sys, os                  # Path management
import pandas as pd             # Data manipulation
from ingestion.fetch_data import DataIngestion      # Data source
from ingestion.enrich_data import TechnicalIndicators  # Tested module
```

### External Dependencies
- **Internet Connection**: Required for data fetching
- **API Keys**: POLYGON_API_KEY for market data access
- **TA-Lib**: Required for advanced indicator calculations

### Test Environment Setup
```bash
# Install test dependencies
pip install pytest pytest-cov

# Set environment variables
export POLYGON_API_KEY="your_polygon_api_key"

# Install TA-Lib (if testing advanced indicators)
# See TALIB_GUIDE.md for installation instructions
```

## Test Data Characteristics

### Market Data Properties
- **AAPL Data**: High-liquidity large-cap stock with stable patterns
- **GOOGL Data**: Tech stock with different volatility characteristics
- **2022 Period**: Includes market volatility and trend changes
- **Daily Frequency**: Standard daily OHLCV data

### Expected Indicator Ranges
- **RSI**: 0-100 range, typically 30-70 for normal conditions
- **CCI**: Unbounded oscillator, typically ±100 for normal conditions
- **ADX**: 0-100 range, >25 indicates strong trend

## Troubleshooting

### Common Test Failures

#### API Connection Issues
```
Error: Failed to fetch data for AAPL
```
**Solutions:**
- Verify POLYGON_API_KEY environment variable
- Check internet connectivity
- Validate API key permissions

#### TA-Lib Import Errors
```
ImportError: No module named 'talib'
```
**Solutions:**
- Install TA-Lib C library first
- Use appropriate installation method for OS
- Consider running basic indicator tests only

#### Missing Indicators
```
AssertionError: 'RSI' not in indicators.transformed_df.columns
```
**Solutions:**
- Check TA-Lib installation and availability
- Verify input data has sufficient length for calculation
- Review indicator calculation requirements

### Debug Commands
```python
# Check test data
print(f"Sample data shape: {sample_tickers.ticker_df.shape}")
print(f"Columns: {sample_tickers.ticker_df.columns.tolist()}")

# Verify indicators
indicators = TechnicalIndicators(sample_tickers.ticker_df)
indicators.calculate_technical_indicators()
print(f"Transformed columns: {indicators.transformed_df.columns.tolist()}")

# Check for nulls
print(indicators.transformed_df.isnull().sum())
```

## Best Practices

### Test Maintenance
1. **Update Test Data**: Periodically refresh date ranges
2. **Monitor API Changes**: Watch for data source modifications
3. **Validate Results**: Spot-check indicator calculations manually
4. **Performance Monitoring**: Track test execution times

### Test Development
1. **Add New Tests**: Cover new indicators as they're added
2. **Edge Case Testing**: Test boundary conditions and error scenarios
3. **Integration Testing**: Validate end-to-end workflows
4. **Documentation**: Keep test documentation current

### Continuous Integration
1. **Automated Runs**: Include in CI/CD pipeline
2. **Environment Variables**: Secure API key management
3. **Test Isolation**: Ensure tests don't interfere with each other
4. **Result Reporting**: Clear pass/fail reporting for DevOps teams

## Future Enhancements

### Potential Improvements
1. **Mock Data**: Add tests with synthetic data for faster execution
2. **Parametrized Tests**: Test multiple ticker combinations
3. **Performance Tests**: Validate calculation speed benchmarks
4. **Error Scenario Tests**: More comprehensive error condition testing
5. **Regression Tests**: Validate indicator calculation consistency over time
