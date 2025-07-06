# CandleThrob Documentation

Welcome to the CandleThrob documentation! This directory contains comprehensive documentation for all modules and components of the CandleThrob financial data pipeline with Oracle Database storage and 113+ technical indicators.

## Documentation Structure

### Core Modules
- [Ingestion Module](./ingestion/) - Data ingestion with Oracle DB storage and incremental loading
- [Utils Module](./utils/) - Database models, Oracle DB utilities, and helper functions
- [Tests](./tests/) - Comprehensive test suite documentation

### Schema Documentation
- [Database Schemas](./schemas.md) - Complete database schemas including 113+ technical indicators
- [Models Documentation](./utils/models.md) - SQLAlchemy ORM models for Oracle Database
- [Oracle DB Utilities](./utils/oracledb.md) - Database connection and management

### Technical Documentation
- [Ingestion Pipeline](./ingestion/ingest_data.md) - Complete data ingestion with Oracle DB storage
- [Technical Enrichment](./ingestion/enrich_data.md) - 113+ technical indicators calculation
- [Data Fetching](./ingestion/fetch_data.md) - API integration for Polygon.io and FRED

## Quick Start

1. **Database Setup**: Configure Oracle Database connection in [oracledb.py](./utils/oracledb.md)
2. **Data Ingestion**: Use [ingest_data.py](./ingestion/ingest_data.md) for complete pipeline with incremental loading
3. **Technical Analysis**: Review [enrich_data.py](./ingestion/enrich_data.md) for 113+ technical indicators
4. **Data Models**: Check [models.py](./utils/models.md) for database schema and ORM usage

## Dependencies

- Python 3.8+
- Oracle Database (cx_Oracle driver)
- TA-Lib (for technical indicators)
- SQLAlchemy ORM
- Polygon.io API key
- FRED API key
- pandas, numpy, logging

## Architecture Overview

```
CandleThrob Pipeline (Oracle DB):
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Polygon.io    │───▶│   Raw Ingestion  │───▶│   Oracle DB     │
│   (OHLCV Data)  │    │   (ticker_data)  │    │ (ticker_data)   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│     FRED API    │───▶│  Macro Ingestion │───▶│   Oracle DB     │
│  (Economic Data)│    │   (macro_data)   │    │  (macro_data)   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Oracle DB     │◀───│   Technical      │◀───│   113+ Tech     │
│(transformed_    │    │   Enrichment     │    │   Indicators    │
│ tickers)        │    │ (enrich_data.py) │    │   + Patterns    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Key Features

### Technical Indicators (113+ indicators)
- **Returns**: 1d, 3d, 7d, 30d, 90d, 365d
- **Momentum**: RSI, MACD, Stochastics, CCI, Williams %R, ROC, MOM, TRIX
- **Moving Averages**: SMA/EMA series (10, 20, 50, 100, 200)
- **Volume**: OBV, A/D Line, MFI, Chaikin indicators, VWAP, VPT, ADX
- **Volatility**: ATR, Bollinger Bands, Donchian Channels, Ulcer Index
- **Price**: Midprice, Median, Typical, Weighted Close, Average Price
- **Cyclical**: Hilbert Transform family indicators
- **Statistical**: Standard Deviation, Variance, Beta, Z-Score
- **Patterns**: 54 candlestick pattern recognition indicators

### Database Features
- **Incremental Loading**: Only insert new data after last available date
- **Bulk Operations**: Optimized 1000-record chunks for high performance
- **Error Recovery**: Transaction rollback and connection management
- **Oracle Optimization**: Indexes and constraints for time series data

## Contributing

When adding new features or modules, please:
1. Add comprehensive documentation to the appropriate directory
2. Include code examples with Oracle DB integration
3. Update schema documentation for any model changes
4. Add tests with database validation
5. Update the main README.md with links to new documentation
4. Follow the established documentation format

Last updated: July 6, 2025
