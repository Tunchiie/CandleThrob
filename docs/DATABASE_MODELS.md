# CandleThrob Database Models Documentation

## Overview
The `models.py` module defines SQLAlchemy ORM (Object-Relational Mapping) models for the CandleThrob financial data pipeline. It provides comprehensive database schema definitions for raw data storage, transformed data storage, and complete technical indicator schemas using Oracle database.

## Core Models

### TickerData
- **Purpose**: Raw OHLCV ticker data storage
- **Schema**: Oracle NUMBER(10,4) precision for price fields
- **Features**: Bulk insert, incremental loading, price validation

### MacroData  
- **Purpose**: Economic indicators and market data
- **Schema**: Flexible value storage with metadata
- **Features**: Time series data, multiple data sources

### TransformedTickerData
- **Purpose**: Technical indicators and enriched data
- **Schema**: 98+ technical indicators from TA-Lib
- **Features**: Advanced analytics, pattern recognition

## Key Features

### Price Validation
- Automatic capping to fit Oracle NUMBER(10,4) precision
- Values capped at 999,999.9999 maximum
- Automatic rounding to 4 decimal places
- Warning logs for capped values

### Bulk Operations
- Optimized batch processing
- Transaction management
- Error handling and rollback
- Performance monitoring

### Data Quality
- Comprehensive validation
- Duplicate detection
- Data integrity checks
- Quality scoring

## Usage Examples

### Basic Data Insertion
```python
from CandleThrob.utils.models import TickerData

ticker_model = TickerData()
ticker_model.insert_data(engine, dataframe)
```

### Price Validation
```python
from CandleThrob.utils.models import validate_and_cap_price_values

# Validate individual values
safe_price = validate_and_cap_price_values(2267744.534164718)
# Result: 999999.9999

# Validate entire DataFrame
validated_df = validate_price_dataframe(df)
```

### Technical Indicators
```python
from CandleThrob.utils.models import TransformedTickerData

transform_model = TransformedTickerData()
transform_model.insert_data(engine, enriched_dataframe)
```

## Integration Points

- **Ingestion Pipeline**: Raw data storage and validation
- **Transformation Pipeline**: Technical indicator calculation
- **Analytics**: Data retrieval for analysis
- **Monitoring**: Performance and quality metrics

For detailed API documentation, see the inline docstrings in `CandleThrob/utils/models.py`.
