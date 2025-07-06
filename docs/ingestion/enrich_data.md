# enrich_data.py Documentation

## Overview
The `enrich_data.py` module is responsible for calculating technical indicators and enriching raw OHLCV data with advanced analytics. It serves as the core technical analysis engine of the CandleThrob financial data pipeline, providing over 129 different technical indicators using the TA-Lib library with robust fallback mechanisms.

## Class: TechnicalIndicators

### Purpose
The `TechnicalIndicators` class transforms raw stock price data into a comprehensive dataset enriched with:
- Basic return calculations (1d, 3d, 7d, 30d, 90d, 365d)
- 129+ technical indicators across 7 categories
- Robust error handling with TA-Lib availability checks
- Memory-efficient processing for large datasets

### Constructor
```python
def __init__(self, ticker_df: pd.DataFrame)
```

**Parameters:**
- `ticker_df` (pd.DataFrame): Raw OHLCV data with required columns:
  - Date (datetime): Trading dates
  - Open, High, Low, Close, Volume (numeric): Price and volume data
  - Ticker (str): Stock symbol

**Initialization Process:**
1. **Date Conversion**: Converts Date column to UTC datetime
2. **Data Filtering**: Filters data to years >= 2000 for consistency
3. **Data Copying**: Creates defensive copy to prevent external modifications
4. **Attribute Setup**: Initializes transformed_df as None

### TA-Lib Integration

#### Import Strategy
```python
try:
    import talib
    TALIB_AVAILABLE = True
except ImportError as e:
    print(f"Warning: TA-Lib not available: {e}")
    TALIB_AVAILABLE = False
```

**Benefits:**
- **Graceful Degradation**: Continues operation without TA-Lib
- **Clear Error Messages**: Provides installation guidance
- **Runtime Detection**: Checks availability at module load
- **Flexible Deployment**: Works in environments where TA-Lib installation fails

### Core Methods

#### enrich_tickers()
**Purpose:** Calculates basic return metrics for multiple time horizons.

**Return Calculations:**
```python
for i in [1, 3, 7, 30, 90, 365]:
    self.ticker_df[f"Return_{i}d"] = self.ticker_df.groupby("Ticker")["Close"]\
        .pct_change(periods=i).fillna(0)
```

**Features:**
- **Grouped Calculation**: Computes returns separately for each ticker
- **Multiple Timeframes**: 1-day to 1-year returns
- **Zero Fill**: Handles NaN values for edge cases
- **Percentage Changes**: Standard return calculation methodology

**Output Columns:**
- `Return_1d`: Daily return percentage
- `Return_3d`: 3-day return percentage  
- `Return_7d`: Weekly return percentage
- `Return_30d`: Monthly return percentage
- `Return_90d`: Quarterly return percentage
- `Return_365d`: Annual return percentage

#### calculate_technical_indicators()
**Purpose:** Main entry point for technical indicator calculation.

**Prerequisites:**
- TA-Lib must be available
- DataFrame must contain required OHLCV columns
- Data must be properly formatted

**Process:**
1. **Availability Check**: Verifies TA-Lib installation
2. **Column Validation**: Ensures required columns exist
3. **Transform Execution**: Calls internal transform() method
4. **Return Processing**: Returns enriched DataFrame

**Error Handling:**
- `ImportError`: If TA-Lib unavailable
- `ValueError`: If required columns missing

#### transform()
**Purpose:** Core transformation engine that calculates all technical indicators.

**Processing Pipeline:**
1. **Data Type Conversion**: Ensures numeric types for calculations
2. **Date Standardization**: Converts to UTC datetime
3. **Ticker Iteration**: Processes each ticker individually with progress tracking
4. **Indicator Calculation**: Computes 7 categories of indicators
5. **Data Merging**: Safely merges all indicator datasets
6. **Final Sorting**: Orders by Date and Ticker

**Memory Management:**
- Processes one ticker at a time to manage memory
- Uses progress bars (tqdm) for long-running operations
- Implements safe merging to prevent data corruption

### Technical Indicator Categories

#### 1. Momentum Indicators (21 columns)
**Method:** `get_talib_momentum_indicators(df)`

**Oscillators:**
- **RSI**: Relative Strength Index (14-period)
- **CCI**: Commodity Channel Index (14-period)
- **WILLR**: Williams %R (14-period)
- **Stoch_K/D**: Stochastic oscillator components

**MACD Family:**
- **MACD**: Moving Average Convergence Divergence
- **MACD_Signal**: MACD signal line
- **MACD_Hist**: MACD histogram

**Moving Averages:**
- **Simple MA**: SMA10, SMA20, SMA50, SMA100, SMA200
- **Exponential MA**: EMA10, EMA20, EMA50, EMA100, EMA200

**Rate of Change:**
- **ROC**: Rate of Change (10-period)
- **MOM**: Momentum (10-period)
- **TRIX**: Triple Smoothed Moving Average (30-period)

#### 2. Volume Indicators (10 columns)
**Method:** `get_talib_volume_indicators(df)`

**Core Volume Indicators:**
- **OBV**: On-Balance Volume
- **AD**: Accumulation/Distribution Line
- **MFI**: Money Flow Index (14-period)
- **ADOSC**: Chaikin A/D Oscillator

**Custom Volume Indicators:**
- **CMF**: Chaikin Money Flow (20-period)
- **VWAP**: Volume Weighted Average Price
- **VPT**: Volume Price Trend
- **ADX**: Average Directional Index (14-period)
- **RVOL**: Relative Volume (20-period rolling average)

#### 3. Volatility Indicators (10 columns)
**Method:** `get_talib_volatility_indicators(df)`

**Standard Volatility:**
- **ATR**: Average True Range (14-period)
- **NATR**: Normalized Average True Range (14-period)
- **TRANGE**: True Range

**Bollinger Bands:**
- **BBANDS_UPPER**: Upper Bollinger Band
- **BBANDS_MIDDLE**: Middle Bollinger Band (SMA20)
- **BBANDS_LOWER**: Lower Bollinger Band

**Channel Indicators:**
- **DONCH_UPPER**: Donchian Channel Upper (20-period)
- **DONCH_LOWER**: Donchian Channel Lower (20-period)

**Risk Metrics:**
- **ULCER_INDEX**: Ulcer Index (14-period)

#### 4. Pattern Recognition (61 columns)
**Method:** `get_talib_pattern_indicators(df)`

**Candlestick Patterns:** Complete set of TA-Lib candlestick patterns including:
- Basic patterns: DOJI, HAMMER, HANGING_MAN
- Reversal patterns: ENGULFING, HARAMI, PIERCING
- Continuation patterns: 3_WHITE_SOLDIERS, 3_BLACK_CROWS
- Complex patterns: MORNING_STAR, EVENING_STAR, ABANDONED_BABY

**Pattern Output:**
- Integer values: +100 (bullish), -100 (bearish), 0 (no pattern)
- Each pattern has specific recognition criteria
- Suitable for rule-based trading systems

#### 5. Price Transform Indicators
**Method:** `get_talib_price_indicators(df)`

**Price Transforms:**
- **AVGPRICE**: Average Price
- **MEDPRICE**: Median Price  
- **TYPPRICE**: Typical Price
- **WCLPRICE**: Weighted Close Price

#### 6. Cyclical Indicators
**Method:** `get_talib_cyclical_indicators(df)`

**Cycle Analysis:**
- **HT_DCPERIOD**: Hilbert Transform - Dominant Cycle Period
- **HT_DCPHASE**: Hilbert Transform - Dominant Cycle Phase
- **HT_PHASOR**: Hilbert Transform - Phasor Components
- **HT_SINE**: Hilbert Transform - Sine Wave
- **HT_TRENDMODE**: Hilbert Transform - Trend vs Cycle Mode

#### 7. Statistical Indicators
**Method:** `get_talib_statistical_indicators(df)`

**Statistical Functions:**
- **BETA**: Beta coefficient vs market
- **CORREL**: Pearson Correlation Coefficient
- **LINEARREG**: Linear Regression
- **STDDEV**: Standard Deviation
- **TSF**: Time Series Forecast
- **VAR**: Variance

### Custom Indicator Functions

#### chaikin_money_flow(df, period=20)
**Purpose:** Calculates Chaikin Money Flow indicator.

**Formula:**
```python
ad = talib.AD(df['High'], df['Low'], df['Close'], df['Volume'])
cmf = ad.rolling(window=period).sum() / df['Volume'].rolling(window=period).sum()
```

**Interpretation:**
- Values above 0: Accumulation (buying pressure)
- Values below 0: Distribution (selling pressure)
- Range: Typically -1.0 to +1.0

#### donchian_channel(df, period=20)
**Purpose:** Calculates Donchian Channel boundaries.

**Formula:**
```python
donch_upper = df['High'].rolling(window=period).max()
donch_lower = df['Low'].rolling(window=period).min()
```

**Usage:**
- **Breakout Strategy**: Price above upper = bullish breakout
- **Support/Resistance**: Channel boundaries act as dynamic levels
- **Volatility Measurement**: Channel width indicates volatility

#### ulcer_index(close, period=14)
**Purpose:** Measures downside risk and volatility.

**Formula:**
```python
ulcer_index = ((close.rolling(window=period).max() - close) / 
               close.rolling(window=period).max()) ** 2
```

**Benefits:**
- **Risk Assessment**: Focuses on downside volatility
- **Portfolio Management**: Alternative to standard deviation
- **Drawdown Analysis**: Quantifies maximum loss periods

### Data Safety and Merging

#### safe_merge_or_concat(df1, df2, on, how='left')
**Purpose:** Safely merges DataFrames with comprehensive error handling.

**Safety Checks:**
1. **Type Validation**: Ensures both inputs are DataFrames
2. **Empty DataFrame Handling**: Returns non-empty DataFrame
3. **Overlap Detection**: Identifies conflicting columns
4. **Ticker Validation**: Prevents data duplication

**Merge Strategy:**
- Uses pandas merge with specified join type
- Handles overlapping columns gracefully
- Logs warnings for potential data issues
- Falls back to first DataFrame on errors

**Error Recovery:**
- Returns valid DataFrame even on merge failures
- Provides detailed logging for debugging
- Maintains data integrity throughout process

## Error Handling Strategy

### TA-Lib Availability
```python
if not TALIB_AVAILABLE:
    logger.error("TA-Lib is not available. Returning empty DataFrame.")
    return pd.DataFrame(columns=[...])
```

**Fallback Behavior:**
- Returns empty DataFrames with correct column structure
- Logs clear error messages
- Allows pipeline to continue without indicators
- Provides installation guidance

### Data Validation
```python
if df.empty:
    logger.warning("Input DataFrame is empty. Returning an empty DataFrame.")
    return pd.DataFrame(columns=[...])
```

**Validation Checks:**
- Empty DataFrame detection
- Required column presence
- Data type validation
- Date format consistency

### Exception Handling
```python
try:
    # Indicator calculation
except Exception as e:
    logger.error(f"Error calculating indicators: {e}")
    return df1  # Return original data
```

**Recovery Strategy:**
- Catches all calculation errors
- Returns partial results when possible
- Logs detailed error information
- Maintains data pipeline stability

## Performance Optimizations

### Memory Management
- **Single Ticker Processing**: Processes one ticker at a time
- **Immediate Type Conversion**: Converts to optimal data types
- **Garbage Collection**: Explicit cleanup of temporary variables
- **Streaming Processing**: Avoids loading entire dataset in memory

### Computational Efficiency
- **Vectorized Operations**: Uses pandas/numpy vectorization
- **TA-Lib Optimization**: Leverages C-compiled TA-Lib functions
- **Progress Tracking**: tqdm for long-running operations
- **Early Exit**: Stops on critical errors to save time

### Data Type Optimization
```python
momentum_df = momentum_df.astype({
    'RSI': 'double', 'MACD': 'double', 'MACD_Signal': 'double',
    # ... all columns to double precision
})
```

**Benefits:**
- **Consistent Precision**: All indicators use double precision
- **Memory Efficiency**: Appropriate data types for each column
- **Calculation Accuracy**: Prevents precision loss in computations

## Integration Points

### Upstream Dependencies
- **Raw OHLCV Data**: From fetch_data.py and ingest_data.py
- **Data Storage**: From utils/gcs.py for cloud data access
- **TA-Lib Library**: For technical indicator calculations

### Downstream Usage
- **transform_data.py**: Main consumer of enriched data
- **Analysis Modules**: Uses enriched data for modeling
- **Dashboard**: Displays calculated indicators

### CI/CD Integration
- **Environment Flexibility**: Works with/without TA-Lib
- **Resource Management**: Optimized for GitHub Actions limits
- **Error Resilience**: Continues processing despite individual failures

## Configuration Options

### TA-Lib Parameters
Most indicators use standard parameters, but can be customized:

```python
# Example parameter modifications
momentum_rsi = talib.RSI(df['Close'], timeperiod=14)  # Default
momentum_rsi = talib.RSI(df['Close'], timeperiod=21)  # Custom
```

### Processing Options
```python
# Filter data by year
self.ticker_df = ticker_df[ticker_df["Date"].dt.year >= 2000].copy()

# Custom date ranges can be implemented
```

## Usage Examples

### Basic Usage
```python
from ingestion.enrich_data import TechnicalIndicators
import pandas as pd

# Load raw OHLCV data
raw_data = pd.read_parquet("stock_data.parquet")

# Initialize technical indicators
tech_indicators = TechnicalIndicators(raw_data)

# Calculate returns
tech_indicators.enrich_tickers()

# Calculate technical indicators (requires TA-Lib)
enriched_data = tech_indicators.calculate_technical_indicators()
```

### Error Handling Example
```python
try:
    tech_indicators = TechnicalIndicators(raw_data)
    enriched_data = tech_indicators.calculate_technical_indicators()
except ImportError:
    print("TA-Lib not available, using basic returns only")
    tech_indicators.enrich_tickers()
    enriched_data = tech_indicators.ticker_df
```

### Custom Indicator Usage
```python
# Calculate individual custom indicators
cmf = tech_indicators.chaikin_money_flow(stock_data, period=20)
upper, lower = tech_indicators.donchian_channel(stock_data, period=20)
ulcer = tech_indicators.ulcer_index(stock_data['Close'], period=14)
```

## Best Practices

### Data Quality
1. **Validate Input Data**: Ensure OHLCV columns present and numeric
2. **Check Date Ranges**: Verify sufficient historical data for indicators
3. **Handle Missing Values**: Use appropriate fill strategies
4. **Monitor Outliers**: Identify and handle extreme values

### Performance
1. **Batch Processing**: Process multiple tickers efficiently
2. **Memory Monitoring**: Watch memory usage for large datasets
3. **Progress Tracking**: Use tqdm for long operations
4. **Error Logging**: Implement comprehensive logging

### Maintenance
1. **Parameter Testing**: Validate indicator parameters for your use case
2. **Version Control**: Track TA-Lib version compatibility
3. **Documentation**: Keep indicator documentation current
4. **Testing**: Implement unit tests for custom indicators

## Troubleshooting

### Common Issues

#### TA-Lib Installation
```bash
# Linux/Ubuntu
sudo apt-get install ta-lib
pip install TA-Lib

# macOS with Homebrew
brew install ta-lib
pip install TA-Lib

# Windows
# Download ta-lib binaries from official source
pip install TA-Lib
```

#### Memory Issues
- Reduce batch size for large datasets
- Process tickers individually
- Use data type optimization
- Implement garbage collection

#### Calculation Errors
- Verify input data quality
- Check for sufficient historical data
- Validate date formats
- Monitor for extreme values

### Debug Commands
```python
# Check TA-Lib availability
print(f"TA-Lib available: {TALIB_AVAILABLE}")

# Validate data structure
print(f"Required columns: {['Open', 'High', 'Low', 'Close', 'Volume']}")
print(f"Available columns: {df.columns.tolist()}")

# Check data types
print(df.dtypes)

# Validate date range
print(f"Date range: {df['Date'].min()} to {df['Date'].max()}")
```

## Output Schema

The final enriched dataset contains 129+ columns:
- **7 Base Columns**: Date, Ticker, Open, High, Low, Close, Volume
- **6 Return Columns**: Return_1d through Return_365d
- **116+ Technical Indicators**: Across 7 categories

**Total Output**: Comprehensive financial dataset suitable for:
- Machine learning model training
- Quantitative analysis
- Trading strategy development
- Risk management systems
- Financial research
