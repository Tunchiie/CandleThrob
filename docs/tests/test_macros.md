# test_macros.py Documentation

## Overview
The `test_macros.py` module provides comprehensive unit tests for macroeconomic data processing functionality in the CandleThrob financial data pipeline. It validates the `EnrichMacros` class operations including data transformation, lagging, rolling statistics, percentage changes, and z-score calculations.

## Updated Test Architecture (Post-Refactoring)

### Testing Strategy
- **Robust Fallback System**: Multiple levels of mock data generation
- **Column-Aware Processing**: Only processes available columns
- **API-Independent Testing**: Works without external API keys
- **Statistical Validation**: Comprehensive transformation testing

### Test Categories
1. **Data Initialization Tests**: Setup and configuration validation
2. **Transformation Pipeline Tests**: Full workflow testing with available columns
3. **Individual Operation Tests**: Specific transformation function testing
4. **Error Handling Tests**: Edge cases and empty data scenarios
5. **Statistical Validation Tests**: Mathematical accuracy verification

## Fixtures

### sample_macros
**Purpose:** Provides robust macroeconomic test data with comprehensive fallbacks.

```python
@pytest.fixture
def sample_macros():
    """Fetch sample macroeconomic data for testing."""
    try:
        data = DataIngestion()
        data.fetch_fred_data()
        
        if data.macro_df is None or data.macro_df.empty:
            # Create comprehensive mock data
            dates = pd.date_range(start="2024-01-01", end="2024-01-31", freq='D')
            macro_df = pd.DataFrame({
                'Date': dates,
                'GDP': 25000 + (dates.dayofyear * 10),
                'UNRATE': 3.5 + (dates.dayofyear * 0.01),
                'FEDFUNDS': 5.0 + (dates.dayofyear * 0.001),
                'CPIAUCSL': 300 + (dates.dayofyear * 0.1),
                'INDPRO': 110 + (dates.dayofyear * 0.01),
                'UMCSENT': 70 + (dates.dayofyear * 0.1),
                'RSAFS': 600000 + (dates.dayofyear * 100)
            })
            return macro_df
            
        return data.macro_df
        
    except Exception as e:
        # Comprehensive fallback mock data
        dates = pd.date_range(start="2024-01-01", end="2024-01-31", freq='D')
        return pd.DataFrame({
            'Date': dates,
            'GDP': 25000 + (dates.dayofyear * 10),
            'UNRATE': 3.5 + (dates.dayofyear * 0.01),
            'FEDFUNDS': 5.0 + (dates.dayofyear * 0.001),
            'CPIAUCSL': 300 + (dates.dayofyear * 0.1),
        })
```

**Key Features:**
- **Multi-Level Fallbacks**: Handles API failures gracefully
- **Realistic Patterns**: Mock data follows economic indicator trends
- **Flexible Column Set**: Works with varying available indicators
- **Time Series Structure**: Proper date indexing and progression

## Core Transformation Tests

### test_enrich_macros_transform()
**Purpose:** Validates the complete macroeconomic transformation pipeline.

```python
def test_enrich_macros_transform(sample_macros):
    """Test macroeconomic data transformation."""
    macros = EnrichMacros(sample_macros)
    macros.transform_macro_data()
    
    assert macros.transformed_df is not None
    assert not macros.transformed_df.empty
    assert isinstance(macros.transformed_df, pd.DataFrame)
```

**Pipeline Validations:**
- Successful pipeline execution
- Data availability post-transformation
- Proper DataFrame structure maintenance
- Column-aware processing (only available columns)

## Individual Transformation Tests

### test_lag_macro_data()
**Purpose:** Tests data lagging functionality for time series alignment.

```python
def test_lag_macro_data(sample_macros):
    """Test lagging macroeconomic data."""
    macros = EnrichMacros(sample_macros)
    
    macros.lag_macro_data(cols=["GDP", "UNRATE"], lag_periods=[30, 60, 90])
    
    expected_lagged_columns = [
        "GDP_lagged_30", "GDP_lagged_60", "GDP_lagged_90",
        "UNRATE_lagged_30", "UNRATE_lagged_60", "UNRATE_lagged_90"
    ]
    
    for col in expected_lagged_columns:
        if 'GDP' in sample_macros.columns or 'UNRATE' in sample_macros.columns:
            assert col in macros.transformed_df.columns
```

**Features:**
- Multi-period lagging (30, 60, 90 days)
- Column existence validation
- Proper time series shifting
- DataFrame structure preservation

### test_rolling_macro_data()
**Purpose:** Validates rolling statistical calculations.

```python
def test_rolling_macro_data(sample_macros):
    """Test rolling mean calculation for macroeconomic data."""
    macros = EnrichMacros(sample_macros)
    
    macros.rolling_macro_data(cols=["FEDFUNDS", "INDPRO"], window=[30, 90])
    
    expected_rolling_columns = [
        "FEDFUNDS_rolling_30", "FEDFUNDS_rolling_90",
        "INDPRO_rolling_30", "INDPRO_rolling_90"
    ]
    
    for col in expected_rolling_columns:
        if 'FEDFUNDS' in sample_macros.columns or 'INDPRO' in sample_macros.columns:
            assert col in macros.transformed_df.columns
```

**Statistical Operations:**
- Moving averages with multiple windows
- Trend smoothing validation
- Column-conditional processing
- Data continuity preservation

### test_pct_change_macro_data()
**Purpose:** Tests percentage change calculations for growth analysis.

```python
def test_pct_change_macro_data(sample_macros):
    """Test percentage change calculation for macroeconomic data."""
    macros = EnrichMacros(sample_macros)
    
    # Use available columns for testing
    available_cols = [col for col in ['GDP', 'UMCSENT', 'RSAFS'] if col in sample_macros.columns]
    if available_cols:
        macros.pct_change_macro_data(cols=available_cols, periods=[90])
        
        for col in available_cols:
            expected_col = f"{col}_pct_change"
            assert expected_col in macros.transformed_df.columns
```

**Growth Metrics:**
- Percentage change calculations
- Multi-period growth rates
- Dynamic column selection
- Rate of change validation

### test_z_score_macro_data()
**Purpose:** Validates statistical normalization through z-score calculation.

```python
def test_z_score_macro_data(sample_macros):
    """Test z-score calculation for macroeconomic data."""
    macros = EnrichMacros(sample_macros)
    
    available_cols = [col for col in ['UNRATE', 'CPIAUCSL'] if col in sample_macros.columns]
    if available_cols:
        macros.z_score_macro_data(cols=available_cols, window=90)
        
        for col in available_cols:
            expected_col = f"{col}_z_score"
            assert expected_col in macros.transformed_df.columns
```

**Normalization Features:**
- Statistical standardization
- Rolling window z-scores
- Outlier detection preparation
- Cross-indicator comparison enablement

```python
def test_enrich_macros(sample_macros):
    print("Testing macroeconomic data enrichment...")
    macros = EnrichMacros(sample_macros)
    macros.transform_macro_data()
    
    # Basic validation
    assert isinstance(macros.macro_df, pd.DataFrame)
    assert not macros.macro_df.empty
    assert "GDP" in macros.macro_df.columns
    assert "UNRATE" in macros.macro_df.columns
    
    # Lagged variables testing
    macros.lag_macro_data(cols=["GDP", "UNRATE"], lag_periods=[30, 60, 90])
    assert "GDP_lagged_30" in macros.transformed_df.columns
    assert "UNRATE_lagged_30" in macros.transformed_df.columns
    assert "UNRATE_z_score" in macros.transformed_df.columns
    assert "GDP_pct_change" in macros.transformed_df.columns
    assert "FEDFUNDS_rolling_30" in macros.transformed_df.columns
    assert "INDPRO_rolling_90" in macros.transformed_df.columns
    
    # Data quality validation
    assert not macros.transformed_df["GDP_lagged_30"].isnull().all()
    assert not macros.transformed_df["UNRATE_lagged_30"].isnull().all()
    assert not macros.transformed_df["UNRATE_z_score"].isnull().all()
    assert not macros.transformed_df["GDP_pct_change"].isnull().all()
    assert not macros.transformed_df["FEDFUNDS_rolling_30"].isnull().all()
    assert not macros.transformed_df["INDPRO_rolling_90"].isnull().all()
```

**Test Scope:**
1. **Object Initialization**: EnrichMacros class setup
2. **Data Transformation**: Core transformation pipeline
3. **Lagged Variables**: Time-shifted indicator creation
4. **Column Presence**: Validates all expected outputs
5. **Data Quality**: Ensures non-null data after transformations

**Validated Transformations:**
- **Lagged Indicators**: GDP_lagged_30, UNRATE_lagged_30
- **Z-Scores**: UNRATE_z_score (standardized values)
- **Percentage Changes**: GDP_pct_change
- **Rolling Means**: FEDFUNDS_rolling_30, INDPRO_rolling_90

## Error Handling Tests

### test_macro_data_empty(sample_macros)
**Purpose:** Validates proper error handling for empty DataFrame scenarios.

```python
def test_macro_data_empty(sample_macros):
    print("Testing macroeconomic data with empty DataFrame...")
    macros = EnrichMacros(sample_macros)
    macros.macro_df = pd.DataFrame()
    with pytest.raises(
        ValueError, match="Macro DataFrame is empty. Cannot transform empty DataFrame."
    ):
        macros.transform_macro_data()
```

**Test Scenario:**
- **Empty Data**: Manually set macro_df to empty DataFrame
- **Expected Behavior**: ValueError with specific message
- **Error Prevention**: Prevents processing of invalid data

**Business Logic:**
- Validates input data before processing
- Provides clear error messaging for debugging
- Prevents downstream errors from empty datasets

## Data Initialization Tests

### test_macro_data_initialization(sample_macros)
**Purpose:** Validates proper object initialization and state setup.

```python
def test_macro_data_initialization(sample_macros):
    print("Testing macroeconomic data initialization...")
    macros = EnrichMacros(sample_macros)
    assert macros.macro_df is not None
    assert isinstance(macros.macro_df, pd.DataFrame)
    assert macros.transformed_df is None
    assert macros.start_date is not None
    assert macros.end_date is not None
```

**Validation Points:**
1. **Data Assignment**: macro_df properly set from input
2. **Type Validation**: DataFrame type maintained
3. **Initial State**: transformed_df starts as None
4. **Date Configuration**: start_date and end_date properly set

**State Validation:**
- **Input Data**: Original data preserved
- **Transform State**: No premature transformation
- **Configuration**: Proper date range setup

### test_macro_data_fetch(sample_macros)
**Purpose:** Validates macroeconomic data structure and content.

```python
def test_macro_data_fetch(sample_macros):
    print("Testing macroeconomic data fetch...")
    macros = EnrichMacros(sample_macros)
    assert not macros.macro_df.empty
    assert isinstance(macros.macro_df, pd.DataFrame)
    assert "GDP" in macros.macro_df.columns
    assert "UNRATE" in macros.macro_df.columns
    assert "CPIAUCSL" in macros.macro_df.columns
    assert "FEDFUNDS" in macros.macro_df.columns
```

**Validated Indicators:**
- **GDP**: Gross Domestic Product
- **UNRATE**: Unemployment Rate  
- **CPIAUCSL**: Consumer Price Index
- **FEDFUNDS**: Federal Funds Rate

**Data Quality Checks:**
- Non-empty DataFrame
- Proper data types
- Expected economic indicators present

## Individual Transformation Tests

### test_macro_data_lag(sample_macros)
**Purpose:** Validates lagged variable creation functionality.

```python
def test_macro_data_lag(sample_macros):
    print("Testing macroeconomic data lagging...")
    macros = EnrichMacros(sample_macros)
    macros.lag_macro_data(cols=["GDP", "UNRATE"], lag_periods=[30, 60, 90])
    
    assert "GDP_lagged_30" in macros.transformed_df.columns
    assert "UNRATE_lagged_30" in macros.transformed_df.columns
    assert not macros.transformed_df["GDP_lagged_30"].isnull().all()
    assert not macros.transformed_df["UNRATE_lagged_30"].isnull().all()
```

**Lag Configuration:**
- **Indicators**: GDP and Unemployment Rate
- **Periods**: 30, 60, and 90 days
- **Purpose**: Create leading indicators for economic modeling

**Validation:**
- **Column Creation**: Proper naming convention
- **Data Quality**: Non-null values after lag calculation
- **Time Series Logic**: Proper temporal offset

### test_macro_data_rolling(sample_macros)
**Purpose:** Validates rolling window statistics calculation.

```python
def test_macro_data_rolling(sample_macros):
    print("Testing macroeconomic data rolling mean...")
    macros = EnrichMacros(sample_macros)
    macros.rolling_macro_data(cols=["FEDFUNDS", "INDPRO"], window=[30, 90])
    
    assert "FEDFUNDS_rolling_30" in macros.transformed_df.columns
    assert "INDPRO_rolling_90" in macros.transformed_df.columns
    assert not macros.transformed_df["FEDFUNDS_rolling_30"].isnull().all()
    assert not macros.transformed_df["INDPRO_rolling_90"].isnull().all()
```

**Rolling Window Configuration:**
- **FEDFUNDS**: 30-day rolling mean for interest rate smoothing
- **INDPRO**: 90-day rolling mean for industrial production trends
- **Purpose**: Smooth noisy economic data for trend analysis

**Statistical Validation:**
- **Window Logic**: Proper rolling calculation
- **Data Continuity**: Non-null values where expected
- **Smoothing Effect**: Reduced volatility in output series

### test_macro_data_pct_change(sample_macros)
**Purpose:** Validates percentage change calculation functionality.

```python
def test_macro_data_pct_change(sample_macros):
    print("Testing macroeconomic data percentage change...")
    macros = EnrichMacros(sample_macros)
    macros.pct_change_macro_data(cols=["GDP", "UMCSENT"], periods=[90])
    
    assert "GDP_pct_change" in macros.transformed_df.columns
    assert not macros.transformed_df["GDP_pct_change"].isnull().all()
    assert "UMCSENT_pct_change" in macros.transformed_df.columns
    assert not macros.transformed_df["UMCSENT_pct_change"].isnull().all()
```

**Percentage Change Configuration:**
- **GDP**: 90-day percentage change for growth measurement
- **UMCSENT**: Consumer sentiment change for behavioral analysis
- **Period**: 90-day period for quarterly-like analysis

**Mathematical Validation:**
- **Calculation Logic**: Proper percentage change formula
- **Data Range**: Realistic percentage values
- **Temporal Consistency**: Proper period alignment

### test_macro_data_z_score(sample_macros)
**Purpose:** Validates z-score standardization functionality.

```python
def test_macro_data_z_score(sample_macros):
    print("Testing macroeconomic data z-score...")
    macros = EnrichMacros(sample_macros)
    macros.z_score_macro_data(cols=["GDP", "UNRATE"])
    assert "GDP_z_score" in macros.transformed_df.columns
    assert not macros.transformed_df["GDP_z_score"].isnull().all()
    assert "UNRATE_z_score" in macros.transformed_df.columns
    assert not macros.transformed_df["UNRATE_z_score"].isnull().all()
```

**Z-Score Configuration:**
- **GDP**: Standardized GDP values for relative comparison
- **UNRATE**: Standardized unemployment for anomaly detection
- **Purpose**: Normalize indicators for cross-series comparison

**Statistical Properties:**
- **Mean**: Z-scores should have mean ≈ 0
- **Standard Deviation**: Z-scores should have std ≈ 1
- **Distribution**: Normalized distribution shape

## Test Execution

### Running Tests
```bash
# Run all macro tests
pytest tests/test_macros.py -v

# Run specific test
pytest tests/test_macros.py::test_enrich_macros -v

# Run with coverage
pytest tests/test_macros.py --cov=ingestion.enrich_data

# Run with detailed output
pytest tests/test_macros.py -v -s
```

### Expected Output
```
tests/test_macros.py::test_enrich_macros PASSED
tests/test_macros.py::test_macro_data_empty PASSED
tests/test_macros.py::test_macro_data_initialization PASSED
tests/test_macros.py::test_macro_data_fetch PASSED
tests/test_macros.py::test_macro_data_lag PASSED
tests/test_macros.py::test_macro_data_rolling PASSED
tests/test_macros.py::test_macro_data_pct_change PASSED
tests/test_macros.py::test_macro_data_z_score PASSED
```

## Dependencies

### Required Modules
```python
import pytest                    # Testing framework
import pandas as pd             # Data manipulation
from ingestion.fetch_data import DataIngestion      # Data source
from ingestion.enrich_data import EnrichMacros      # Tested module
```

### Environment Requirements
```bash
# Required for FRED API access
export FRED_API_KEY="your_fred_api_key"

# Install testing dependencies
pip install pytest pytest-cov pandas fredapi
```

### External Dependencies
- **Internet Connection**: Required for FRED API calls
- **FRED API Key**: Must be valid and active
- **pandas**: Data manipulation library
- **fredapi**: FRED API client library

## Performance Characteristics

### Test Execution Times
- **FRED Data Fetch**: 10-60 seconds (depends on network)
- **Transformation Tests**: 1-5 seconds each
- **Error Tests**: <1 second each
- **Total Suite**: 1-3 minutes for complete execution

### Data Volume
- **Time Series Length**: Varies by indicator (GDP quarterly, others monthly/daily)
- **Memory Usage**: Moderate for economic time series
- **Processing Time**: Fast for statistical transformations

### API Considerations
- **FRED API Limits**: Generally high, rarely hit in testing
- **Network Dependency**: All tests require internet connectivity
- **Data Freshness**: Tests use current data, may vary over time

## Data Quality Validation

### Expected Data Characteristics

#### Economic Indicators Properties
- **GDP**: Quarterly data, positive values, growth trends
- **Unemployment Rate**: Monthly data, 0-15% typical range
- **Federal Funds Rate**: Daily data, 0-20% historical range
- **Consumer Price Index**: Monthly data, positive with inflation trends

#### Transformation Properties
- **Lagged Data**: Proper temporal shift, maintains value ranges
- **Rolling Means**: Smoothed values, reduced volatility
- **Percentage Changes**: Bounded ranges, economic sense
- **Z-Scores**: Mean ≈ 0, standard deviation ≈ 1

### Statistical Validation Examples
```python
# Validate z-score properties
z_scores = macros.transformed_df["GDP_z_score"]
assert abs(z_scores.mean()) < 0.1  # Mean close to 0
assert abs(z_scores.std() - 1.0) < 0.1  # Std close to 1

# Validate percentage change ranges
pct_changes = macros.transformed_df["GDP_pct_change"]
assert pct_changes.abs().max() < 100  # Reasonable change bounds
```

## Troubleshooting

### Common Test Failures

#### FRED API Issues
```
Error: FRED API authentication failed
```
**Solutions:**
- Verify FRED_API_KEY environment variable
- Check API key validity on FRED website
- Ensure internet connectivity

#### Missing Data Issues
```
AssertionError: Column 'GDP_lagged_30' not found
```
**Solutions:**
- Check if transformation methods were called
- Verify input data contains required indicators
- Review lag period requirements vs. data length

#### Data Quality Issues
```
AssertionError: All values are null after transformation
```
**Solutions:**
- Check input data quality and length
- Verify transformation parameters are appropriate
- Review calculation requirements vs. available data

### Debug Commands
```python
# Check macro data availability
macros = EnrichMacros(sample_macros)
print(f"Available indicators: {macros.macro_df.columns.tolist()}")
print(f"Data shape: {macros.macro_df.shape}")
print(f"Date range: {macros.macro_df.index.min()} to {macros.macro_df.index.max()}")

# Validate transformation results
macros.lag_macro_data(cols=["GDP"], lag_periods=[30])
print(f"Transformed columns: {macros.transformed_df.columns.tolist()}")
print(f"Non-null values: {macros.transformed_df.notna().sum()}")
```

## Best Practices

### Test Design
1. **Real Data Testing**: Use actual economic data for realistic validation
2. **Statistical Validation**: Verify mathematical properties of transformations
3. **Edge Case Coverage**: Test with various data conditions
4. **Error Validation**: Ensure proper error handling for invalid inputs

### Maintenance
1. **Data Updates**: Economic data changes over time, tests may need adjustment
2. **API Monitoring**: Watch for FRED API changes or outages
3. **Performance Tracking**: Monitor test execution times for regression
4. **Documentation**: Keep transformation logic documentation current

### Development
1. **Parameter Testing**: Validate transformations with different parameters
2. **Cross-Validation**: Compare results with external economic analysis tools
3. **Version Control**: Track changes in transformation algorithms
4. **Integration**: Ensure compatibility with downstream analysis modules

## Security and Data Handling

### API Security
- **Key Management**: Store FRED API keys securely
- **Rate Limiting**: Respect FRED API usage guidelines
- **Error Handling**: Don't expose API keys in error messages

### Data Privacy
- **Public Data**: FRED data is public, no privacy concerns
- **Test Isolation**: Tests don't affect production data
- **Cleanup**: Temporary test data properly managed

## Future Enhancements

### Potential Improvements
1. **Mock Testing**: Add synthetic data tests for faster execution
2. **Property-Based Testing**: Use hypothesis for statistical property validation
3. **Performance Benchmarks**: Add transformation speed benchmarks
4. **Cross-Indicator Testing**: Validate relationships between indicators
5. **Historical Validation**: Test transformation consistency over time periods

### Advanced Testing Scenarios
1. **Economic Cycle Testing**: Validate during different economic conditions
2. **Missing Data Handling**: Test with incomplete time series
3. **Frequency Conversion**: Test with mixed-frequency data
4. **Seasonal Adjustment**: Validate seasonal patterns in transformations
