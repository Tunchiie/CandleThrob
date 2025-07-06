# models.py Documentation

## Overview
The `models.py` module defines SQLAlchemy ORM (Object-Relational Mapping) models for the CandleThrob financial data pipeline. It provides comprehensive database schema definitions for raw data storage, transformed data storage, and complete technical indicator schemas using Oracle database. This module serves as the data layer abstraction for database operations with support for bulk inserts and incremental loading.

## Architecture

### Design Philosophy
- **Oracle-First**: Optimized for Oracle Database with SQLAlchemy ORM
- **Incremental Loading**: Built-in support for efficient data updates
- **Bulk Operations**: High-performance bulk insert capabilities
- **Schema-Complete**: Comprehensive coverage of all technical indicators
- **Type Safety**: Strong typing with SQLAlchemy column definitions
- **Scalability**: Designed for large-scale financial data storage

### Model Categories
1. **Raw Data Models**: Storage of ingested OHLCV and macro data
2. **Transformed Data Models**: Storage of enriched data with 113+ technical indicators
3. **Incremental Loading**: Methods for efficient data updates
4. **Bulk Processing**: Optimized for high-volume financial data

## Model Classes

### TickerData (Raw Ticker Data)
**Purpose:** Stores raw OHLCV data as ingested from Polygon.io API with Oracle DB optimization.

```python
class TickerData(Base):
    """Model for storing ticker data (OHLCV) with bulk insert and incremental loading support."""
    __tablename__ = 'ticker_data'
```

**Table Schema:**
```sql
CREATE TABLE ticker_data (
    id INTEGER PRIMARY KEY,
    ticker VARCHAR2(10) NOT NULL,
    date DATE NOT NULL,
    open_price NUMBER NOT NULL,
    high_price NUMBER NOT NULL,
    low_price NUMBER NOT NULL,
    close_price NUMBER NOT NULL,
    volume NUMBER(19,0) NOT NULL,
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    updated_at TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_ticker_data_ticker_date ON ticker_data(ticker, date);
CREATE INDEX idx_ticker_data_date ON ticker_data(date);
```

**Column Definitions:**
- `id` (Integer, Primary Key): Auto-incrementing unique identifier
- `ticker` (String(10), Not Null): Stock symbol (e.g., 'AAPL', 'GOOGL')
- `date` (Date, Not Null): Trading date
- `open_price` (Float, Not Null): Opening price
- `high_price` (Float, Not Null): Highest price of the day
- `low_price` (Float, Not Null): Lowest price of the day
- `close_price` (Float, Not Null): Closing price
- `volume` (BigInteger, Not Null): Trading volume
- `created_at` (DateTime): Record creation timestamp
- `updated_at` (DateTime): Last update timestamp

**Key Methods:**
```python
def create_table(self, session: Session):
    """Create the table if it doesn't exist."""

def data_exists(self, session: Session, ticker: Optional[str] = None) -> bool:
    """Check if data exists in the table."""

def get_last_date(self, session: Session, ticker: Optional[str] = None) -> Optional[date_type]:
    """Get the last date for which data exists."""

def insert_data(self, session: Session, df: pd.DataFrame):
    """Insert data using bulk operations with incremental loading."""
```

**Incremental Loading Features:**
- Automatically detects existing data
- Only inserts new records after last available date
- Handles column mapping from DataFrame to database schema
- Bulk insert optimization with 1000-record chunks

### MacroData (Macroeconomic Data)
**Purpose:** Stores macroeconomic indicators from FRED API in normalized format.

```python
class MacroData(Base):
    """
    SQLAlchemy model for macroeconomic data.
    """
    __tablename__ = 'raw_macro_data'
```

**Table Schema:**
```sql
CREATE TABLE raw_macro_data (
### MacroData (Macroeconomic Data)
**Purpose:** Stores macroeconomic indicators from FRED API in normalized format with incremental loading.

```python
class MacroData(Base):
    """Model for storing macroeconomic data with bulk insert and incremental loading support."""
    __tablename__ = 'macro_data'
```

**Table Schema:**
```sql
CREATE TABLE macro_data (
    id INTEGER PRIMARY KEY,
    date DATE NOT NULL,
    series_id VARCHAR2(100) NOT NULL,
    value NUMBER,
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
    updated_at TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- Indexes for time series queries
CREATE INDEX idx_macro_data_date_series ON macro_data(date, series_id);
CREATE INDEX idx_macro_data_series_date ON macro_data(series_id, date);
```

**Column Definitions:**
- `id` (Integer, Primary Key): Auto-incrementing unique identifier
- `date` (Date, Not Null): Observation date
- `series_id` (String(100), Not Null): Economic indicator code (e.g., "GDP", "UNRATE")
- `value` (Float, Nullable): Indicator value (nullable for missing data)
- `created_at` (DateTime): Record creation timestamp
- `updated_at` (DateTime): Last update timestamp

**Supported Economic Indicators:**
- GDP: Gross Domestic Product
- UNRATE: Unemployment Rate
- FEDFUNDS: Federal Funds Rate
- CPIAUCSL: Consumer Price Index
- INDPRO: Industrial Production Index
- UMCSENT: Consumer Sentiment
- RSAFS: Retail Sales
- And other FRED series

**Incremental Loading:**
- Detects latest data by series_id
- Appends only new observations
- Handles missing values gracefully
- Optimized for FRED API data structure

### TransformedTickers (Enriched Technical Data)
**Purpose:** Stores comprehensive transformed data with 113+ technical indicators and candlestick patterns.

```python
class TransformedTickers(Base):
    """Model for storing processed data with technical indicators."""
    __tablename__ = 'transformed_tickers'
```

**Schema Categories:**

#### Base Columns (8 columns)
```python
id = Column(Integer, primary_key=True, index=True)
ticker = Column(String(10), nullable=False, index=True)
date = Column(Date, nullable=False, index=True)

# OHLCV data
open_price = Column(Float)
high_price = Column(Float)
low_price = Column(Float)
close_price = Column(Float)
volume = Column(BigInteger)
```

#### Return Indicators (6 columns)
```python
return_1d = Column(Float)    # 1-day return
return_3d = Column(Float)    # 3-day return
return_7d = Column(Float)    # Weekly return
return_30d = Column(Float)   # Monthly return
return_90d = Column(Float)   # Quarterly return
return_365d = Column(Float)  # Annual return
```

#### Momentum Indicators (21 columns)
```python
# Core Oscillators
rsi = Column(Float)          # Relative Strength Index (14-period)
cci = Column(Float)          # Commodity Channel Index (14-period)
willr = Column(Float)        # Williams %R (14-period)
stoch_k = Column(Float)      # Stochastic %K (14,3,3)
stoch_d = Column(Float)      # Stochastic %D (14,3,3)

# MACD Family
macd = Column(Float)         # MACD Line (12,26,9)
macd_signal = Column(Float)  # MACD Signal Line
macd_hist = Column(Float)    # MACD Histogram

# Rate of Change
roc = Column(Float)          # Rate of Change (10-period)
mom = Column(Float)          # Momentum (10-period)
trix = Column(Float)         # TRIX (14-period)

# Simple Moving Averages
sma10 = Column(Float)        # 10-period SMA
sma20 = Column(Float)        # 20-period SMA
sma50 = Column(Float)        # 50-period SMA
sma100 = Column(Float)       # 100-period SMA
sma200 = Column(Float)       # 200-period SMA

# Exponential Moving Averages
ema10 = Column(Float)        # 10-period EMA
ema20 = Column(Float)        # 20-period EMA
ema50 = Column(Float)        # 50-period EMA
ema100 = Column(Float)       # 100-period EMA
ema200 = Column(Float)       # 200-period EMA
```

#### Volume Indicators (9 columns)
```python
obv = Column(Float)          # On-Balance Volume
ad = Column(Float)           # Accumulation/Distribution Line
mfi = Column(Float)          # Money Flow Index (14-period)
adosc = Column(Float)        # Chaikin A/D Oscillator (3,10)
cmf = Column(Float)          # Chaikin Money Flow (20-period)
vpt = Column(Float)          # Volume Price Trend
vwap = Column(Float)         # Volume Weighted Average Price
adx = Column(Float)          # Average Directional Index (14-period)
rvol = Column(Float)         # Relative Volume (20-period average)
```

#### Volatility Indicators (9 columns)
```python
atr = Column(Float)          # Average True Range (14-period)
natr = Column(Float)         # Normalized ATR (14-period)
trange = Column(Float)       # True Range

# Bollinger Bands (20-period, 2 std dev)
bbands_upper = Column(Float)  # Upper Band
bbands_middle = Column(Float) # Middle Band (SMA20)
bbands_lower = Column(Float)  # Lower Band

# Channel Indicators
donch_upper = Column(Float)   # Donchian Upper (20-period)
donch_lower = Column(Float)   # Donchian Lower (20-period)
ulcer_index = Column(Float)   # Ulcer Index (14-period)
```

#### Price Transformation Indicators (5 columns)
```python
midprice = Column(Float)     # Midpoint Price (14-period)
medprice = Column(Float)     # Median Price (High + Low) / 2
typprice = Column(Float)     # Typical Price (High + Low + Close) / 3
wclprice = Column(Float)     # Weighted Close Price (High + Low + 2*Close) / 4
avgprice = Column(Float)     # Average Price (Open + High + Low + Close) / 4
```

#### Cyclical Indicators (5 columns)
```python
ht_trendline = Column(Float) # Hilbert Transform Instantaneous Trendline
ht_sine = Column(Float)      # Hilbert Transform Sine Wave
ht_sine_lead = Column(Float) # Hilbert Transform Sine Wave Lead
ht_dcperiod = Column(Float)  # Hilbert Transform Dominant Cycle Period
ht_dcphase = Column(Float)   # Hilbert Transform Dominant Cycle Phase
```

#### Statistical Indicators (4 columns)
```python
stddev = Column(Float)                # Standard Deviation (20-period)
var = Column(Float)                   # Variance (20-period)
beta_vs_sp500 = Column(Float)         # Beta vs S&P 500 (20-period)
zscore_price_normalized = Column(Float) # Z-Score of price (20-period rolling)
```

#### Candlestick Pattern Recognition (54 patterns)
Complete set of TA-Lib candlestick pattern recognition indicators. Each pattern returns:
- **100**: Bullish pattern detected
- **0**: No pattern detected
- **-100**: Bearish pattern detected

**Selected Key Patterns:**
```python
cdldoji = Column(Float)            # Doji
cdlhammer = Column(Float)          # Hammer
cdlhangingman = Column(Float)      # Hanging Man
cdlinvertedhammer = Column(Float)  # Inverted Hammer
cdlharami = Column(Float)          # Harami Pattern
cdlharamicross = Column(Float)     # Harami Cross Pattern
cdlengulfing = Column(Float)       # Engulfing Pattern
cdlmorningstar = Column(Float)     # Morning Star
cdleveningstar = Column(Float)     # Evening Star
cdlmorningdojistar = Column(Float) # Morning Doji Star
cdldojistar = Column(Float)        # Doji Star
cdlshootingstar = Column(Float)    # Shooting Star
cdl3whitesoldiers = Column(Float)  # Three Advancing White Soldiers
cdl3blackcrows = Column(Float)     # Three Black Crows
cdl3inside = Column(Float)         # Three Inside Up/Down
cdl3outside = Column(Float)        # Three Outside Up/Down
cdl3linestrike = Column(Float)     # Three-Line Strike
cdl3starsinsouth = Column(Float)   # Three Stars In The South
cdlabandonedbaby = Column(Float)   # Abandoned Baby
cdlbreakaway = Column(Float)       # Breakaway
cdlladderbottom = Column(Float)    # Ladder Bottom
cdlunique3river = Column(Float)    # Unique 3 River
cdltristar = Column(Float)         # Tristar Pattern
# ... and 31 additional patterns
```

**Total Schema Size:**
- **Base Columns**: 8 (id, ticker, date, OHLCV, metadata)
- **Technical Indicators**: 59 (returns, momentum, volume, volatility, price, cyclical, statistical)
- **Candlestick Patterns**: 54 pattern recognition indicators
- **Total**: 113+ comprehensive technical analysis indicators

### TransformedMacroData (Enriched Macro Data)
**Purpose:** Stores transformed macroeconomic data with statistical enrichments.

```python
class TransformedMacroData(Base):
    """Model for storing transformed macroeconomic data."""
    __tablename__ = 'transformed_macro_data'
```

**Column Definitions:**
```python
id = Column(Integer, primary_key=True, index=True)
date = Column(Date, nullable=False, index=True)
series_id = Column(String(100), nullable=False, index=True)
value = Column(Float, nullable=True)           # Original value
normalized_value = Column(Float)               # Z-score normalized
moving_avg_30 = Column(Float)                  # 30-day moving average
year_over_year_change = Column(Float)          # YoY percentage change
created_at = Column(DateTime, default=datetime.utcnow)
updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
```

## Database Integration

### Oracle Database Configuration
```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from utils.models import Base, TickerData, MacroData, TransformedTickers

# Oracle connection string
engine = create_engine('oracle+cx_oracle://username:password@host:port/service_name')

# Create all tables
Base.metadata.create_all(engine)

# Create session
Session = sessionmaker(bind=engine)
session = Session()
```

### Usage Examples

#### Bulk Insert with Incremental Loading
```python
# Example: Insert ticker data with automatic incremental loading
from utils.models import TickerData
import pandas as pd

# Sample DataFrame
df = pd.DataFrame({
    'ticker': ['AAPL'] * 100,
    'date': pd.date_range('2023-01-01', periods=100),
    'open': [150.0] * 100,
    'high': [155.0] * 100,
    'low': [149.0] * 100,
    'close': [154.0] * 100,
    'volume': [1000000] * 100
})

# Insert with automatic incremental loading
ticker_model = TickerData()
with Session() as session:
    ticker_model.insert_data(session, df)
    # Only new data (after existing last_date) will be inserted
```

#### Querying Technical Indicators
```python
# Query specific technical indicators
from utils.models import TransformedTickers

with Session() as session:
    # Get RSI and MACD for AAPL in 2023
    query = session.query(
        TransformedTickers.date,
        TransformedTickers.rsi,
        TransformedTickers.macd,
        TransformedTickers.close_price
    ).filter(
        TransformedTickers.ticker == 'AAPL',
        TransformedTickers.date >= '2023-01-01'
    ).order_by(TransformedTickers.date)
    
    results = query.all()
```

## Performance Optimization

### Indexing Strategy
```sql
-- Primary performance indexes
CREATE INDEX idx_ticker_data_ticker_date ON ticker_data(ticker, date);
CREATE INDEX idx_macro_data_series_date ON macro_data(series_id, date);
CREATE INDEX idx_transformed_ticker_date ON transformed_tickers(ticker, date);

-- Query optimization indexes
CREATE INDEX idx_ticker_date_only ON ticker_data(date);
CREATE INDEX idx_transformed_rsi ON transformed_tickers(rsi) WHERE rsi IS NOT NULL;
CREATE INDEX idx_transformed_volume ON transformed_tickers(volume) WHERE volume > 0;
```

### Bulk Insert Performance
- **Chunk Size**: Optimized for 1000-record batches
- **pandas.to_sql()**: Leverages DataFrame bulk operations
- **Incremental Loading**: Minimizes duplicate processing
- **Oracle Optimization**: Uses Oracle-specific bulk insert features

## Data Types and Constraints

### Oracle-Specific Data Types
- **NUMBER**: All financial data (prices, volumes, indicators)
- **VARCHAR2**: String data with explicit length limits
- **DATE**: Trading dates and timestamps
- **TIMESTAMP**: Audit trail timestamps

### Validation and Constraints
```python
# Recommended data validation
def validate_ticker_data(df):
    """Validate OHLCV data before insertion."""
    assert df['high_price'].ge(df['open_price']).all(), "High >= Open"
    assert df['high_price'].ge(df['close_price']).all(), "High >= Close"
    assert df['low_price'].le(df['open_price']).all(), "Low <= Open"
    assert df['low_price'].le(df['close_price']).all(), "Low <= Close"
    assert df['volume'].ge(0).all(), "Volume >= 0"

def validate_technical_indicators(df):
    """Validate technical indicator ranges."""
    if 'rsi' in df.columns:
        assert df['rsi'].between(0, 100, inclusive='both').all(), "RSI in 0-100 range"
    if 'stoch_k' in df.columns:
        assert df['stoch_k'].between(0, 100, inclusive='both').all(), "Stoch %K in 0-100 range"
```

## Integration Points

### Data Pipeline Integration
- **ingestion/fetch_data.py**: Provides raw data for TickerData and MacroData
- **ingestion/enrich_data.py**: Generates 113+ indicators for TransformedTickers
- **ingestion/ingest_data.py**: Orchestrates bulk loading with incremental updates
- **utils/oracledb.py**: Database connection management

### Downstream Consumers
- **analysis/**: Statistical analysis using technical indicators
- **dashboard/**: Real-time visualization of indicators and patterns
- **bot/**: Trading algorithms using pattern recognition
- **ML pipelines**: Feature engineering using comprehensive indicator set

## Best Practices

### Schema Design
1. **Normalization**: Appropriate level for time series financial data
2. **Indexing**: Optimized for date-range and ticker-based queries
3. **Data Types**: Oracle NUMBER type for precise financial calculations
4. **Incremental Loading**: Built-in support for efficient updates

### Development Workflow
1. **Schema Migrations**: Use Alembic for Oracle schema versioning
2. **Testing**: Validate against sample datasets before production
3. **Documentation**: Keep technical indicator documentation current
4. **Performance**: Regular query optimization and index maintenance

### Production Operations
1. **Monitoring**: Track insert performance and data volumes
2. **Backup**: Regular Oracle database backups with point-in-time recovery
3. **Archival**: Strategy for historical data retention
4. **Security**: Oracle-specific security and encryption features

## Technical Indicator Reference

### Complete Indicator List (113+ indicators)
The TransformedTickers model includes comprehensive coverage of:

**Returns (6)**: 1d, 3d, 7d, 30d, 90d, 365d
**Momentum (21)**: RSI, MACD family, Stochastics, CCI, Williams %R, ROC, MOM, TRIX, SMA/EMA series
**Volume (9)**: OBV, A/D Line, MFI, Chaikin indicators, VWAP, VPT, ADX, Relative Volume
**Volatility (9)**: ATR family, Bollinger Bands, Donchian Channels, Ulcer Index
**Price (5)**: Midprice, Median, Typical, Weighted Close, Average Price
**Cyclical (5)**: Hilbert Transform family indicators
**Statistical (4)**: Standard Deviation, Variance, Beta, Z-Score
**Patterns (54)**: Complete TA-Lib candlestick pattern recognition suite

This comprehensive indicator set enables:
- **Technical Analysis**: Full spectrum of technical analysis indicators
- **Pattern Recognition**: Complete candlestick pattern detection
- **Risk Management**: Volatility and statistical measures
- **Market Sentiment**: Volume and momentum indicators
- **Machine Learning**: Rich feature set for ML algorithms

## Future Enhancements

### Planned Improvements
1. **Real-time Streaming**: Models for live market data ingestion
2. **Alternative Data**: Schema extensions for news, sentiment, options data
3. **ML Features**: Additional derived features for machine learning
4. **Performance**: Oracle partitioning for very large datasets
5. **Audit Trail**: Enhanced data lineage and change tracking
