# Data Schemas Documentation

## Overview

This document provides a comprehensive overview of all data schemas used throughout the CandleThrob project. It covers DataFrame schemas for the data processing pipeline, Oracle database schemas for persistence, and the relationships between different data structures.

**Author**: Adetunji Fasiku  
**Version**: 3.0.0  
**Last Updated**: 2025-07-14

## Table of Contents

1. [DataFrame Schemas](#dataframe-schemas)
   - [Raw OHLCV Data](#raw-ohlcv-data)
   - [Enriched Technical Indicators](#enriched-technical-indicators)
   - [Macroeconomic Data](#macroeconomic-data)
   - [Transformed Data](#transformed-data)
2. [Oracle Database Schemas](#oracle-database-schemas)
   - [TickerData Table](#tickerdata-table)
   - [MacroData Table](#macrodata-table)
   - [TransformedTickers Table](#transformedtickers-table)
   - [TransformedMacroData Table](#transformedmacrodata-table)
3. [Advanced Features](#advanced-features)
4. [Data Flow and Relationships](#data-flow-and-relationships)
5. [Schema Evolution](#schema-evolution)
6. [Validation Rules](#validation-rules)

## DataFrame Schemas

### Raw OHLCV Data

**Source**: `fetch_data.py` - Output from Polygon API  
**Description**: Basic stock price and volume data from Polygon.io API

| Column | Type | Description | Required | Constraints |
|--------|------|-------------|----------|-------------|
| timestamp | int64 | Unix timestamp (milliseconds) | Yes | Must be valid timestamp |
| open | float64 | Opening price | Yes | > 0 |
| high | float64 | Highest price | Yes | >= open |
| low | float64 | Lowest price | Yes | <= open, > 0 |
| close | float64 | Closing price | Yes | > 0 |
| volume | float64 | Trading volume | Yes | >= 0 |
| vwap | float64 | Volume-weighted average price | No | Can be null |
| transactions | int64 | Number of transactions | No | Can be null |
| ticker | object (string) | Stock symbol | Yes | 1-5 characters, uppercase |
| trade_date | object (string) | Formatted date (YYYY-MM-DD) | Yes | Must be valid date string |

**Example**:
```python
{
    'timestamp': 1704067200000,
    'open': 130.28,
    'high': 130.90,
    'low': 124.17,
    'close': 125.07,
    'volume': 112117471.0,
    'vwap': 127.45,
    'transactions': 1234567,
    'ticker': 'AAPL',
    'trade_date': '2024-01-03'
}
```

**Data Quality Validation**:
- Removes rows with null values in critical columns (`open`, `high`, `low`, `close`, `volume`)
- Ensures positive values for all price and volume columns
- Sorts by timestamp in ascending order
- Validates OHLC relationships (high >= low, high >= open, high >= close, etc.)

### Raw Macroeconomic Data

**Source**: `fetch_data.py` - FRED API output  
**Description**: Macroeconomic indicators from Federal Reserve Economic Data

| Column | Type | Description | FRED Series |
|--------|------|-------------|-------------|
| date | datetime64[ns] | Observation date | Index |
| series_id | object (string) | FRED series identifier | Yes | One of: FEDFUNDS, CPIAUCSL, UNRATE, GDP, GS10 |
| value | float64 | Series value | Yes | Can be null for missing data |

**Available Series**:
- `FEDFUNDS`: Federal Funds Rate
- `CPIAUCSL`: Consumer Price Index
- `UNRATE`: Unemployment Rate
- `GDP`: Gross Domestic Product
- `GS10`: 10-Year Treasury Rate

**Example**:
```python
{
    'date': '2024-01-03 00:00:00',
    'series_id': 'FEDFUNDS',
    'value': 5.33
}
```

**Data Quality Validation**:
- Validates data quality using OHLCV validation framework
- Removes series with excessive null values
- Ensures proper date formatting and ordering

### Enriched Technical Indicators

**Source**: `enrich_data.py` - TechnicalIndicators class output  
**Description**: OHLCV data enriched with technical indicators and return calculations

#### Core Columns (from Raw OHLCV)
All columns from Raw OHLCV Data schema, plus:

#### Return Calculations
| Column | Type | Description |
|--------|------|-------------|
| Return_1d | float64 | 1-day price return |
| Return_3d | float64 | 3-day price return |
| Return_7d | float64 | 7-day price return |
| Return_30d | float64 | 30-day price return |
| Return_90d | float64 | 90-day price return |
| Return_365d | float64 | 365-day price return |

#### Momentum Indicators
| Column | Type | Description | TA-Lib Function |
|--------|------|-------------|-----------------|
| RSI | float64 | Relative Strength Index (14-period) | `talib.RSI()` |
| MACD | float64 | MACD line | `talib.MACD()` |
| MACD_Signal | float64 | MACD signal line | `talib.MACD()` |
| MACD_Hist | float64 | MACD histogram | `talib.MACD()` |
| Stoch_K | float64 | Stochastic %K | `talib.STOCH()` |
| Stoch_D | float64 | Stochastic %D | `talib.STOCH()` |
| CCI | float64 | Commodity Channel Index | `talib.CCI()` |
| ROC | float64 | Rate of Change (10-period) | `talib.ROC()` |
| MOM | float64 | Momentum (10-period) | `talib.MOM()` |
| TRIX | float64 | Triple Exponential Average | `talib.TRIX()` |
| WILLR | float64 | Williams %R | `talib.WILLR()` |

#### Moving Averages
| Column | Type | Description |
|--------|------|-------------|
| SMA10 | float64 | 10-period Simple Moving Average |
| SMA20 | float64 | 20-period Simple Moving Average |
| SMA50 | float64 | 50-period Simple Moving Average |
| SMA100 | float64 | 100-period Simple Moving Average |
| SMA200 | float64 | 200-period Simple Moving Average |
| EMA10 | float64 | 10-period Exponential Moving Average |
| EMA20 | float64 | 20-period Exponential Moving Average |
| EMA50 | float64 | 50-period Exponential Moving Average |
| EMA100 | float64 | 100-period Exponential Moving Average |
| EMA200 | float64 | 200-period Exponential Moving Average |

#### Volume Indicators
| Column | Type | Description | TA-Lib Function |
|--------|------|-------------|-----------------|
| OBV | float64 | On-Balance Volume | `talib.OBV()` |
| AD | float64 | Accumulation/Distribution Line | `talib.AD()` |
| MFI | float64 | Money Flow Index | `talib.MFI()` |
| ADOSC | float64 | Chaikin A/D Oscillator | `talib.ADOSC()` |
| CMF | float64 | Chaikin Money Flow (custom) | Custom function |
| VWAP | float64 | Volume Weighted Average Price | Custom calculation |
| VPT | float64 | Volume Price Trend | Custom calculation |
| ADX | float64 | Average Directional Index | `talib.ADX()` |
| RVOL | float64 | Relative Volume (20-period) | Custom calculation |

#### Volatility Indicators
| Column | Type | Description | TA-Lib Function |
|--------|------|-------------|-----------------|
| ATR | float64 | Average True Range | `talib.ATR()` |
| NATR | float64 | Normalized Average True Range | `talib.NATR()` |
| TRANGE | float64 | True Range | `talib.TRANGE()` |
| BBANDS_UPPER | float64 | Bollinger Bands Upper | `talib.BBANDS()` |
| BBANDS_MIDDLE | float64 | Bollinger Bands Middle | `talib.BBANDS()` |
| BBANDS_LOWER | float64 | Bollinger Bands Lower | `talib.BBANDS()` |
| ULCER_INDEX | float64 | Ulcer Index | Custom function |
| DONCH_UPPER | float64 | Donchian Channel Upper | Custom function |
| DONCH_LOWER | float64 | Donchian Channel Lower | Custom function |

#### Price Indicators
| Column | Type | Description | TA-Lib Function |
|--------|------|-------------|-----------------|
| Midprice | float64 | Midpoint Price | `talib.MIDPRICE()` |
| Medprice | float64 | Median Price | `talib.MEDPRICE()` |
| Typprice | float64 | Typical Price | `talib.TYPPRICE()` |
| Wclprice | float64 | Weighted Close Price | `talib.WCLPRICE()` |
| Avgprice | float64 | Average Price | `talib.AVGPRICE()` |

#### Cyclical Indicators
| Column | Type | Description | TA-Lib Function |
|--------|------|-------------|-----------------|
| HT_TRENDLINE | float64 | Hilbert Transform - Instantaneous Trendline | `talib.HT_TRENDLINE()` |
| HT_SINE | float64 | Hilbert Transform - SineWave | `talib.HT_SINE()` |
| HT_SINE_LEAD | float64 | Hilbert Transform - SineWave Lead | `talib.HT_SINE()` |
| HT_DCPERIOD | float64 | Hilbert Transform - Dominant Cycle Period | `talib.HT_DCPERIOD()` |
| HT_DCPHASE | float64 | Hilbert Transform - Dominant Cycle Phase | `talib.HT_DCPHASE()` |

#### Statistical Indicators
| Column | Type | Description | TA-Lib Function |
|--------|------|-------------|-----------------|
| STDDEV | float64 | Standard Deviation (20-period) | `talib.STDDEV()` |
| VAR | float64 | Variance (20-period) | `talib.VAR()` |
| BETA_VS_SP500 | float64 | Beta vs S&P 500 | `talib.BETA()` |
| ZSCORE_PRICE_NORMALIZED | float64 | Z-Score of normalized price | Custom calculation |

#### Candlestick Pattern Recognition (61 patterns)
All pattern columns are of type `float64` and return integer values:
- 100: Bullish pattern
- 0: No pattern
- -100: Bearish pattern

**Selected Pattern Columns**:
- CDL2CROWS, CDL3BLACKCROWS, CDL3INSIDE, CDL3LINESTRIKE
- CDLDOJI, CDLHAMMER, CDLHANGINGMAN, CDLENGULFING
- CDLMORNINGSTAR, CDLEVENINGSTAR, CDLSHOOTINGSTAR
- And 50+ additional candlestick patterns

### Enriched Macroeconomic Data

**Source**: `enrich_data.py` - EnrichMacros class output

##### Lagged Variables (30, 60, 90 day lags)
- GDP_lagged_30, GDP_lagged_60, GDP_lagged_90
- UNRATE_lagged_30, UNRATE_lagged_60, UNRATE_lagged_90
- UMCSENT_lagged_30, UMCSENT_lagged_60, UMCSENT_lagged_90
- CPIAUCSL_lagged_30, CPIAUCSL_lagged_60, CPIAUCSL_lagged_90

##### Rolling Averages (30, 90 day windows)
- FEDFUNDS_rolling_30, FEDFUNDS_rolling_90
- INDPRO_rolling_30, INDPRO_rolling_90

##### Percentage Changes (90 day periods)
- GDP_pct_change
- UMCSENT_pct_change
- RSAFS_pct_change

##### Z-Scores (90 day rolling window)
- UNRATE_z_score
- CPIAUCSL_z_score

### Transformed Data

**Source**: `transform_data.py` - Final processed dataset  
**Description**: Merged ticker and macro data with additional features

This schema combines all columns from:
1. Enriched Technical Indicators DataFrame
2. Enriched Macroeconomic Data DataFrame

Additional columns may include:
- Feature engineering outputs
- Target variables for ML models
- Cross-sectional rankings
- Sector/industry classifications (if available)

## Oracle Database Schemas

### TickerData Table

**Source**: `utils/models.py` - TickerData class
**Purpose**: Persistent storage of stock price data with advanced features

```sql
CREATE TABLE ticker_data (
    id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    ticker VARCHAR2(10) NOT NULL,
    trade_date DATE NOT NULL,
    open_price NUMBER(15,4) NOT NULL,
    high_price NUMBER(15,4) NOT NULL,
    low_price NUMBER(15,4) NOT NULL,
    close_price NUMBER(15,4) NOT NULL,
    volume NUMBER NOT NULL,
    vwap NUMBER(15,4),
    num_transactions NUMBER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Indexes**:
- `idx_ticker_data_ticker` on `ticker`
- `idx_ticker_data_date` on `trade_date`
- `idx_ticker_data_ticker_date` on `(ticker, trade_date)`

**Advanced Features**:
- Bulk insert operations with configurable batch sizes
- Data validation and quality scoring
- Performance monitoring and metrics collection
- Retry logic with exponential backoff
- Memory usage optimization
- Comprehensive error handling and logging

### MacroData Table

**Source**: `utils/models.py` - MacroData class
**Purpose**: Persistent storage of macroeconomic indicators with advanced features

```sql
CREATE TABLE macro_data (
    id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    trade_date DATE NOT NULL,
    series_id VARCHAR2(100) NOT NULL,
    value NUMBER(15,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Indexes**:
- `idx_macro_data_series` on `series_id`
- `idx_macro_data_date` on `trade_date`

**Key Features**:
- Incremental loading support
- Data quality validation
- Performance monitoring
- Bulk operations optimization
- Comprehensive error handling

### TransformedTickers Table

**Source**: `utils/models.py` - AdvancedTransformedTickers class  
**Purpose**: Storage of processed data with comprehensive technical indicators

```sql
CREATE TABLE transformed_tickers (
    id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    ticker VARCHAR2(10) NOT NULL,
    trans_date DATE NOT NULL,
    
    -- OHLCV Data
    open_price BINARY_DOUBLE,
    high_price BINARY_DOUBLE,
    low_price BINARY_DOUBLE,
    close_price BINARY_DOUBLE,
    volume NUMBER,
    vwap NUMBER(10,4),
    num_transactions NUMBER,
    
    -- Return Calculations (6 indicators)
    return_1d BINARY_DOUBLE,
    return_3d BINARY_DOUBLE,
    return_7d BINARY_DOUBLE,
    return_30d BINARY_DOUBLE,
    return_90d BINARY_DOUBLE,
    return_365d BINARY_DOUBLE,
    
    -- Momentum Indicators (21 indicators)
    rsi BINARY_DOUBLE,
    macd BINARY_DOUBLE,
    macd_signal BINARY_DOUBLE,
    macd_hist BINARY_DOUBLE,
    stoch_k BINARY_DOUBLE,
    stoch_d BINARY_DOUBLE,
    cci BINARY_DOUBLE,
    roc BINARY_DOUBLE,
    mom BINARY_DOUBLE,
    trix BINARY_DOUBLE,
    willr BINARY_DOUBLE,
    sma10 BINARY_DOUBLE,
    sma20 BINARY_DOUBLE,
    sma50 BINARY_DOUBLE,
    sma100 BINARY_DOUBLE,
    sma200 BINARY_DOUBLE,
    ema10 BINARY_DOUBLE,
    ema20 BINARY_DOUBLE,
    ema50 BINARY_DOUBLE,
    ema100 BINARY_DOUBLE,
    ema200 BINARY_DOUBLE,
    
    -- Volume Indicators (9 indicators)
    obv BINARY_DOUBLE,
    ad BINARY_DOUBLE,
    mfi BINARY_DOUBLE,
    adosc BINARY_DOUBLE,
    cmf BINARY_DOUBLE,
    vwap BINARY_DOUBLE,
    vpt BINARY_DOUBLE,
    adx BINARY_DOUBLE,
    rvol BINARY_DOUBLE,
    
    -- Volatility Indicators (9 indicators)
    atr BINARY_DOUBLE,
    natr BINARY_DOUBLE,
    trange BINARY_DOUBLE,
    bbands_upper BINARY_DOUBLE,
    bbands_middle BINARY_DOUBLE,
    bbands_lower BINARY_DOUBLE,
    ulcer_index BINARY_DOUBLE,
    donch_upper BINARY_DOUBLE,
    donch_lower BINARY_DOUBLE,
    
    -- Price Indicators (5 indicators)
    midprice BINARY_DOUBLE,
    medprice BINARY_DOUBLE,
    typprice BINARY_DOUBLE,
    wclprice BINARY_DOUBLE,
    avgprice BINARY_DOUBLE,
    
    -- Cyclical Indicators (5 indicators)
    ht_trendline BINARY_DOUBLE,
    ht_sine BINARY_DOUBLE,
    ht_sine_lead BINARY_DOUBLE,
    ht_dcperiod BINARY_DOUBLE,
    ht_dcphase BINARY_DOUBLE,
    
    -- Statistical Indicators (4 indicators)
    stddev BINARY_DOUBLE,
    var BINARY_DOUBLE,
    beta_vs_sp500 BINARY_DOUBLE,
    zscore_price_normalized BINARY_DOUBLE,
    
    -- Candlestick Pattern Recognition (54 patterns)
    cdl2crows BINARY_DOUBLE,
    cdl3blackcrows BINARY_DOUBLE,
    cdl3inside BINARY_DOUBLE,
    cdl3linestrike BINARY_DOUBLE,
    cdl3outside BINARY_DOUBLE,
    cdl3starsinsouth BINARY_DOUBLE,
    cdl3whitesoldiers BINARY_DOUBLE,
    cdlabandonedbaby BINARY_DOUBLE,
    cdlbelthold BINARY_DOUBLE,
    cdlbreakaway BINARY_DOUBLE,
    cdlclosingmarubozu BINARY_DOUBLE,
    cdlconcealbabyswall BINARY_DOUBLE,
    cdlcounterattack BINARY_DOUBLE,
    cdldarkcloudcover BINARY_DOUBLE,
    cdldoji BINARY_DOUBLE,
    cdldojistar BINARY_DOUBLE,
    cdlengulfing BINARY_DOUBLE,
    cdleveningstar BINARY_DOUBLE,
    cdlgravestonedoji BINARY_DOUBLE,
    cdlhammer BINARY_DOUBLE,
    cdlhangingman BINARY_DOUBLE,
    cdlharami BINARY_DOUBLE,
    cdlharamicross BINARY_DOUBLE,
    cdlhighwave BINARY_DOUBLE,
    cdlhikkake BINARY_DOUBLE,
    cdlhikkakemod BINARY_DOUBLE,
    cdlhomingpigeon BINARY_DOUBLE,
    cdlidentical3crows BINARY_DOUBLE,
    cdlinneck BINARY_DOUBLE,
    cdlinvertedhammer BINARY_DOUBLE,
    cdlladderbottom BINARY_DOUBLE,
    cdllongleggeddoji BINARY_DOUBLE,
    cdllongline BINARY_DOUBLE,
    cdlmarubozu BINARY_DOUBLE,
    cdlmatchinglow BINARY_DOUBLE,
    cdlmathold BINARY_DOUBLE,
    cdlmorningdojistar BINARY_DOUBLE,
    cdlmorningstar BINARY_DOUBLE,
    cdlonneck BINARY_DOUBLE,
    cdlpiercing BINARY_DOUBLE,
    cdlrickshawman BINARY_DOUBLE,
    cdlrisefall3methods BINARY_DOUBLE,
    cdlseparatinglines BINARY_DOUBLE,
    cdlshootingstar BINARY_DOUBLE,
    cdlshortline BINARY_DOUBLE,
    cdlspinningtop BINARY_DOUBLE,
    cdlstalledpattern BINARY_DOUBLE,
    cdlsticksandwich BINARY_DOUBLE,
    cdltakuri BINARY_DOUBLE,
    cdltasukigap BINARY_DOUBLE,
    cdlthrusting BINARY_DOUBLE,
    cdltristar BINARY_DOUBLE,
    cdlunique3river BINARY_DOUBLE,
    cdlxsidegap3methods BINARY_DOUBLE,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Indexes**:
- `idx_transformed_tickers_ticker` on `ticker`
- `idx_transformed_tickers_date` on `trans_date`
- `idx_transformed_tickers_ticker_date` on `(ticker, trans_date)`

**Total Columns**: 113+ technical indicators and candlestick patterns
- **Base columns**: 8 (id, ticker, trans_date, OHLCV, metadata)
- **Technical indicators**: 59 (returns, momentum, volume, volatility, price, cyclical, statistical)
- **Candlestick patterns**: 54 pattern recognition indicators

### TransformedMacroData Table

**Source**: `utils/models.py` - AdvancedTransformedMacroData class  
**Purpose**: Storage of transformed macroeconomic data with enrichments

```sql
CREATE TABLE transformed_macro_data (
    id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    trans_date DATE NOT NULL,
    series_id VARCHAR2(100) NOT NULL,
    value BINARY_DOUBLE,
    normalized_value BINARY_DOUBLE,
    moving_avg_30 BINARY_DOUBLE,
    year_over_year_change BINARY_DOUBLE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Indexes**:
- `idx_transformed_macro_data_series` on `series_id`
- `idx_transformed_macro_data_date` on `trans_date`

## Advanced Features

### Performance Monitoring
- Real-time operation tracking
- Duration and success rate metrics
- Memory usage optimization
- Bulk operation performance analysis

### Data Validation
- Comprehensive DataFrame validation
- Data quality scoring (0.0-1.0 scale)
- Type checking and null validation
- Price consistency validation

### Error Handling
- Retry logic with exponential backoff
- Comprehensive error categorization
- Detailed logging and audit trails
- Graceful failure handling

### Configuration Management
- Configurable batch sizes
- Memory usage limits
- Data quality thresholds
- Performance monitoring settings

## Data Flow and Relationships

### Pipeline Flow

```
1. Raw Data Ingestion
   ├── Polygon API → Raw OHLCV DataFrame
   └── FRED API → Raw Macro DataFrame

2. Data Enrichment
   ├── Raw OHLCV → Technical Indicators DataFrame (113+ indicators)
   └── Raw Macro → Enriched Macro DataFrame

3. Data Transformation
   └── Technical Indicators + Enriched Macro → Transformed DataFrame

4. Data Persistence (Oracle DB)
   ├── Raw OHLCV → TickerData Table
   ├── Raw Macro → MacroData Table
   ├── Enriched Technical Data → TransformedTickers Table
   └── Enriched Macro Data → TransformedMacroData Table
```

### Schema Relationships

#### DataFrame to Database Mapping

| DataFrame Schema | Database Table | Column Mapping |
|------------------|----------------|----------------|
| Raw OHLCV Data | TickerData | `timestamp` → N/A (not stored)<br>`open` → `open_price`<br>`high` → `high_price`<br>`low` → `low_price`<br>`close` → `close_price`<br>`volume` → `volume`<br>`vwap` → `vwap`<br>`transactions` → `num_transactions`<br>`ticker` → `ticker`<br>`trade_date` → `trade_date` |
| Raw Macroeconomic Data | MacroData | `date` → `trade_date`<br>`series_id` → `series_id`<br>`value` → `value` |
| Enriched Technical Indicators | TransformedTickers | All technical indicators mapped with snake_case naming<br>`ticker` → `ticker`<br>`trade_date` → `trans_date` |
| Enriched Macro Data | TransformedMacroData | `date` → `trans_date`<br>`series_id` → `series_id`<br>`value` → `value`<br>+ enriched columns |

#### Key Relationships

1. **Date-based Joins**: All schemas use date fields as primary join keys
2. **Ticker Grouping**: Stock data is grouped by `ticker` symbol
3. **Time Series Nature**: All data is time-ordered and can be analyzed as time series
4. **Hierarchical Structure**: Raw → Enriched → Transformed represents increasing levels of processing
5. **Column Naming Convention**: 
   - DataFrame: camelCase (e.g., `open`, `high`, `close`)
   - Database: snake_case (e.g., `open_price`, `high_price`, `close_price`)

## Schema Evolution

### Version History

#### v1.0 (Initial)
- Basic OHLCV data structure
- Limited technical indicators
- Simple macro data integration

#### v2.0 (Previous)
- Comprehensive technical indicators (100+ columns)
- Advanced macro data transformations
- Candlestick pattern recognition
- Enhanced database schemas with proper indexing

#### v3.0 (Current - Professional Grade)
- Oracle database optimization
- Advanced error handling and retry logic
- Performance monitoring and metrics collection
- Advanced data validation and quality scoring
- Memory optimization and bulk operations
- Comprehensive audit trails and logging

#### Future Considerations
- Additional data sources (news sentiment, options data)
- Real-time streaming schemas
- Partitioning strategies for large datasets
- Data quality and lineage tracking
- Machine learning model integration

## Validation Rules

### Data Quality Constraints

#### Price Data Validation
```python
# Price consistency checks
assert df['High'] >= df['Open'], "High must be >= Open"
assert df['High'] >= df['Close'], "High must be >= Close"
assert df['Low'] <= df['Open'], "Low must be <= Open"
assert df['Low'] <= df['Close'], "Low must be <= Close"
assert df['Volume'] >= 0, "Volume must be non-negative"
```

#### Date Validation
```python
# Date consistency
assert df['Date'].is_monotonic_increasing, "Dates must be in ascending order"
assert df['Date'].dt.tz is not None, "Dates must be timezone-aware"
```

#### Technical Indicator Validation
```python
# RSI bounds
assert (df['RSI'] >= 0).all() and (df['RSI'] <= 100).all(), "RSI must be between 0-100"

# Bollinger Bands ordering
assert (df['BBANDS_UPPER'] >= df['BBANDS_MIDDLE']).all(), "Upper band >= Middle"
assert (df['BBANDS_MIDDLE'] >= df['BBANDS_LOWER']).all(), "Middle >= Lower band"
```

### Missing Data Handling

#### Acceptable Missing Values
- Technical indicators: NaN for initial periods (insufficient history)
- Macro data: NaN for non-trading days or data delays
- Pattern recognition: 0 for "no pattern detected"

#### Required Fields
- Date, Ticker, OHLCV: No missing values allowed
- Primary technical indicators (RSI, MACD, etc.): Must be calculated when sufficient data exists

### Performance Considerations

#### Indexing Strategy
```sql
-- Recommended database indexes
CREATE INDEX idx_ticker_date ON ticker_data(ticker, trade_date);
CREATE INDEX idx_date_ticker ON ticker_data(trade_date, ticker);
CREATE INDEX idx_macro_date_series ON macro_data(trade_date, series_id);
```

#### Memory Optimization
- Use appropriate dtypes (float32 vs float64)
- Categorical encoding for repeated strings
- Chunked processing for large datasets

## Usage Examples

### Loading Schemas in Code

```python
# Load raw OHLCV data
from ingestion.fetch_data import DataIngestion
di = DataIngestion()
raw_data = di.fetch_ticker_data(['AAPL', 'GOOGL'])

# Enrich with technical indicators
from ingestion.enrich_data import TechnicalIndicators
ti = TechnicalIndicators(raw_data)
ti.enrich_tickers()
enriched_data = ti.calculate_technical_indicators()

# Access specific schema columns
momentum_cols = [col for col in enriched_data.columns if col.startswith(('RSI', 'MACD', 'Stoch'))]
volume_cols = [col for col in enriched_data.columns if col.startswith(('OBV', 'AD', 'MFI'))]
```

### Advanced Model Usage

```python
# Initialize advanced model
from utils.models import TickerData, ModelConfig

config = ModelConfig(
    max_insert_batch_size=10000,
    enable_data_validation=True,
    min_data_quality_score=0.8
)

model = TickerData(config)

# Create table and insert data
with db.establish_connection() as conn:
    with conn.cursor() as cursor:
        model.create_table(cursor)
        model.insert_data(conn, ticker_df)

# Get performance metrics
metrics = model.get_performance_metrics()
print(f"Data quality score: {metrics['quality_score']}")
```

### Schema Inspection

```python
# Check schema compliance
def validate_ohlcv_schema(df):
    required_cols = ['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Validate dtypes
    assert pd.api.types.is_datetime64_any_dtype(df['Date']), "Date must be datetime"
    assert pd.api.types.is_numeric_dtype(df['Open']), "Open must be numeric"
    # ... additional validations
```

This schema documentation serves as the authoritative reference for all data structures used in the CandleThrob project, ensuring consistency across the entire data pipeline.

**Author**: Adetunji Fasiku
