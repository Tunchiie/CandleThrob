# Data Schemas Documentation

## Overview

This document provides a comprehensive overview of all data schemas used throughout the CandleThrob project. It covers DataFrame schemas for the data processing pipeline, database schemas for persistence, and the relationships between different data structures.

## Table of Contents

1. [DataFrame Schemas](#dataframe-schemas)
   - [Raw OHLCV Data](#raw-ohlcv-data)
   - [Enriched Technical Indicators](#enriched-technical-indicators)
   - [Macroeconomic Data](#macroeconomic-data)
   - [Transformed Data](#transformed-data)
2. [Database Schemas](#database-schemas)
   - [TickerData Table](#tickerdata-table)
   - [MacroData Table](#macrodata-table)
   - [TransformedTickers Table](#transformedtickers-table)
   - [TransformedMacroData Table](#transformedmacrodata-table)
3. [Data Flow and Relationships](#data-flow-and-relationships)
4. [Schema Evolution](#schema-evolution)
5. [Validation Rules](#validation-rules)

## DataFrame Schemas

### Raw OHLCV Data

**Source**: `fetch_data.py` - Output from Polygon API
**Description**: Basic stock price and volume data

| Column | Type | Description | Required | Constraints |
|--------|------|-------------|----------|-------------|
| Date | datetime64[ns, UTC] | Trading date | Yes | Must be valid date, timezone-aware |
| Ticker | object (string) | Stock symbol | Yes | 1-5 characters, uppercase |
| Open | float64 | Opening price | Yes | > 0 |
| High | float64 | Highest price | Yes | >= Open |
| Low | float64 | Lowest price | Yes | <= Open, > 0 |
| Close | float64 | Closing price | Yes | > 0 |
| Volume | float64 | Trading volume | Yes | >= 0 |

**Example**:
```python
{
    'Date': '2023-01-03 00:00:00+00:00',
    'Ticker': 'AAPL',
    'Open': 130.28,
    'High': 130.90,
    'Low': 124.17,
    'Close': 125.07,
    'Volume': 112117471.0
}
```

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

### Macroeconomic Data

**Source**: `fetch_data.py` - FRED API output
**Description**: Macroeconomic indicators from Federal Reserve Economic Data

#### Raw Macro Data
| Column | Type | Description | FRED Series |
|--------|------|-------------|-------------|
| Date | datetime64[ns] | Observation date | Index |
| GDP | float64 | Gross Domestic Product | GDP |
| UNRATE | float64 | Unemployment Rate | UNRATE |
| FEDFUNDS | float64 | Federal Funds Rate | FEDFUNDS |
| CPIAUCSL | float64 | Consumer Price Index | CPIAUCSL |
| INDPRO | float64 | Industrial Production Index | INDPRO |
| UMCSENT | float64 | Consumer Sentiment | UMCSENT |
| RSAFS | float64 | Retail Sales | RSAFS |

#### Enriched Macro Data
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

## Database Schemas

### TickerData Table

**Source**: `utils/models.py` - SQLAlchemy model
**Purpose**: Persistent storage of stock price data

```python
class TickerData(Base):
    __tablename__ = "ticker_data"
    
    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String(10), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    open_price = Column(Float, nullable=False)
    high_price = Column(Float, nullable=False)
    low_price = Column(Float, nullable=False)
    close_price = Column(Float, nullable=False)
    volume = Column(BigInteger, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
```

**Constraints**:
- Primary key: `id`
- Composite unique constraint: `(ticker, date)`
- Indexes on: `ticker`, `date`, `id`
- Database: Oracle DB optimized for financial time series data
- Supports bulk inserts and incremental loading
- Foreign key relationships: None

### MacroData Table

**Source**: `utils/models.py` - SQLAlchemy model
**Purpose**: Persistent storage of macroeconomic indicators

```python
class MacroData(Base):
    __tablename__ = "macro_data"
    
    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, nullable=False, index=True)
    series_id = Column(String(100), nullable=False, index=True)
    value = Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
```

**Constraints**:
- Primary key: `id`
- Composite unique constraint: `(date, series_id)`
- Indexes on: `date`, `series_id`, `id`
- Database: Oracle DB with support for incremental loading
- Supports bulk inserts for large datasets
- Foreign key relationships: None

### TransformedTickers Table

**Source**: `utils/models.py` - SQLAlchemy model
**Purpose**: Storage of processed data with comprehensive technical indicators

```python
class TransformedTickers(Base):
    __tablename__ = "transformed_tickers"
    
    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String(10), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    
    # OHLCV Data
    open_price = Column(Float)
    high_price = Column(Float)
    low_price = Column(Float)
    close_price = Column(Float)
    volume = Column(BigInteger)
    
    # Return Calculations (6 indicators)
    return_1d = Column(Float)
    return_3d = Column(Float)
    return_7d = Column(Float)
    return_30d = Column(Float)
    return_90d = Column(Float)
    return_365d = Column(Float)
    
    # Momentum Indicators (21 indicators)
    rsi = Column(Float)
    macd = Column(Float)
    macd_signal = Column(Float)
    macd_hist = Column(Float)
    stoch_k = Column(Float)
    stoch_d = Column(Float)
    cci = Column(Float)
    roc = Column(Float)
    mom = Column(Float)
    trix = Column(Float)
    willr = Column(Float)
    sma10 = Column(Float)
    sma20 = Column(Float)
    sma50 = Column(Float)
    sma100 = Column(Float)
    sma200 = Column(Float)
    ema10 = Column(Float)
    ema20 = Column(Float)
    ema50 = Column(Float)
    ema100 = Column(Float)
    ema200 = Column(Float)
    
    # Volume Indicators (9 indicators)
    obv = Column(Float)
    ad = Column(Float)
    mfi = Column(Float)
    adosc = Column(Float)
    cmf = Column(Float)
    vwap = Column(Float)
    vpt = Column(Float)
    adx = Column(Float)
    rvol = Column(Float)
    
    # Volatility Indicators (9 indicators)
    atr = Column(Float)
    natr = Column(Float)
    trange = Column(Float)
    bbands_upper = Column(Float)
    bbands_middle = Column(Float)
    bbands_lower = Column(Float)
    ulcer_index = Column(Float)
    donch_upper = Column(Float)
    donch_lower = Column(Float)
    
    # Price Indicators (5 indicators)
    midprice = Column(Float)
    medprice = Column(Float)
    typprice = Column(Float)
    wclprice = Column(Float)
    avgprice = Column(Float)
    
    # Cyclical Indicators (5 indicators)
    ht_trendline = Column(Float)
    ht_sine = Column(Float)
    ht_sine_lead = Column(Float)
    ht_dcperiod = Column(Float)
    ht_dcphase = Column(Float)
    
    # Statistical Indicators (4 indicators)
    stddev = Column(Float)
    var = Column(Float)
    beta_vs_sp500 = Column(Float)
    zscore_price_normalized = Column(Float)
    
    # Candlestick Pattern Recognition (54 patterns)
    # All patterns return: 100 = Bullish, 0 = No pattern, -100 = Bearish
    cdl2crows = Column(Float)
    cdl3blackcrows = Column(Float)
    cdl3inside = Column(Float)
    cdl3linestrike = Column(Float)
    cdl3outside = Column(Float)
    cdl3starsinsouth = Column(Float)
    cdl3whitesoldiers = Column(Float)
    cdlabandonedbaby = Column(Float)
    cdlbelthold = Column(Float)
    cdlbreakaway = Column(Float)
    cdlclosingmarubozu = Column(Float)
    cdlconcealbabyswall = Column(Float)
    cdlcounterattack = Column(Float)
    cdldarkcloudcover = Column(Float)
    cdldoji = Column(Float)
    cdldojistar = Column(Float)
    cdlengulfing = Column(Float)
    cdleveningstar = Column(Float)
    cdlgravestonedoji = Column(Float)
    cdlhammer = Column(Float)
    cdlhangingman = Column(Float)
    cdlharami = Column(Float)
    cdlharamicross = Column(Float)
    cdlhighwave = Column(Float)
    cdlhikkake = Column(Float)
    cdlhikkakemod = Column(Float)
    cdlhomingpigeon = Column(Float)
    cdlidentical3crows = Column(Float)
    cdlinneck = Column(Float)
    cdlinvertedhammer = Column(Float)
    cdlladderbottom = Column(Float)
    cdllongleggeddoji = Column(Float)
    cdllongline = Column(Float)
    cdlmarubozu = Column(Float)
    cdlmatchinglow = Column(Float)
    cdlmathold = Column(Float)
    cdlmorningdojistar = Column(Float)
    cdlmorningstar = Column(Float)
    cdlonneck = Column(Float)
    cdlpiercing = Column(Float)
    cdlrickshawman = Column(Float)
    cdlrisefall3methods = Column(Float)
    cdlseparatinglines = Column(Float)
    cdlshootingstar = Column(Float)
    cdlshortline = Column(Float)
    cdlspinningtop = Column(Float)
    cdlstalledpattern = Column(Float)
    cdlsticksandwich = Column(Float)
    cdltakuri = Column(Float)
    cdltasukigap = Column(Float)
    cdlthrusting = Column(Float)
    cdltristar = Column(Float)
    cdlunique3river = Column(Float)
    cdlxsidegap3methods = Column(Float)
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
```

**Total Columns**: 113+ technical indicators and candlestick patterns
- **Base columns**: 8 (id, ticker, date, OHLCV, metadata)
- **Technical indicators**: 59 (returns, momentum, volume, volatility, price, cyclical, statistical)
- **Candlestick patterns**: 54 pattern recognition indicators

**Constraints**:
- Primary key: `id`
- Composite unique constraint: `(ticker, date)`
- Indexes on: `ticker`, `date`, `id`
- Foreign key relationships: None

### TransformedMacroData Table

**Source**: `utils/models.py` - SQLAlchemy model
**Purpose**: Storage of transformed macroeconomic data with enrichments

```python
class TransformedMacroData(Base):
    __tablename__ = "transformed_macro_data"
    
    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, nullable=False, index=True)
    series_id = Column(String(100), nullable=False, index=True)
    value = Column(Float, nullable=True)
    normalized_value = Column(Float)
    moving_avg_30 = Column(Float)
    year_over_year_change = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
```

**Constraints**:
- Primary key: `id`
- Composite unique constraint: `(date, series_id)`
- Indexes on: `date`, `series_id`, `id`
- Database: Oracle DB with support for incremental macro data loading
- Supports bulk inserts for enriched macro indicators
- Foreign key relationships: None

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

| DataFrame Schema | Database Table | Mapping Notes |
|------------------|----------------|---------------|
| Raw OHLCV Data | TickerData | Direct 1:1 mapping with column renaming |
| Macroeconomic Data | MacroData | Pivoted: one row per (date, series_id) |
| Enriched Technical Indicators | TransformedTickers | 113+ technical and pattern indicators |
| Enriched Macro Data | TransformedMacroData | Normalized values with statistical features |

#### Key Relationships

1. **Date-based Joins**: All schemas use `Date` as a primary join key
2. **Ticker Grouping**: Stock data is grouped by `Ticker` symbol
3. **Time Series Nature**: All data is time-ordered and can be analyzed as time series
4. **Hierarchical Structure**: Raw → Enriched → Transformed represents increasing levels of processing

## Schema Evolution

### Version History

#### v1.0 (Initial)
- Basic OHLCV data structure
- Limited technical indicators
- Simple macro data integration

#### v2.0 (Current)
- Comprehensive technical indicators (100+ columns)
- Advanced macro data transformations
- Candlestick pattern recognition
- Enhanced database schemas with proper indexing

#### Future Considerations
- Additional data sources (news sentiment, options data)
- Real-time streaming schemas
- Partitioning strategies for large datasets
- Data quality and lineage tracking

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
CREATE INDEX idx_ticker_date ON ticker_data(ticker, date);
CREATE INDEX idx_date_ticker ON ticker_data(date, ticker);
CREATE INDEX idx_macro_date_series ON macro_data(date, series_id);
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
