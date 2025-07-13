# CandleThrob Documentation Index

## Overview
This directory contains comprehensive documentation for all modules in the CandleThrob financial data pipeline. Each document provides detailed information about module functionality, architecture, usage examples, and best practices.

## Core Documentation

### [schemas.md](schemas.md)
**Purpose**: Comprehensive data schema documentation
**Description**: Complete reference for all DataFrame and database schemas used throughout the CandleThrob project, including:
- Raw OHLCV data structures
- Enriched technical indicators schemas  
- Macroeconomic data schemas
- Database table definitions
- Schema relationships and data flow
- Validation rules and constraints
export ORACLE_USER="username"
export ORACLE_PASSWORD="password"
export ORACLE_HOST="hostname"  
export ORACLE_SERVICE_NAME="service"
```

### Test Categories
- **Unit Tests**: Individual function validation
- **Integration Tests**: End-to-end pipeline testing
- **Database Tests**: Oracle DB operations and schema validation
- **API Tests**: External service integration with fallbacks
 schema documentation
**Description**: Complete reference for all DataFrame and database schemas used throughout the CandleThrob project, including:
- Raw OHLCV data structures
- Enriched technical indicators schemas  
- Macroeconomic data schemas
- Database table definitions
- Schema relationships and data flow
- Validation rules and constraints

## Documentation Structure

### Ingestion Module Documentation
The ingestion directory contains the core data collection and processing modules.

#### [fetch_data.md](ingestion/fetch_data.md)
**Module**: `ingestion/fetch_data.py`
**Purpose**: Core data retrieval from external APIs
**Key Features**:
- Polygon.io integration for OHLCV data
- FRED API integration for macroeconomic data
- Rate limiting and error handling
- Data cleaning and validation

**Classes & Functions**:
- `DataIngestion`: Main data fetching class
- `ingest_ticker_data()`: Individual ticker data retrieval
- `fetch_fred_data()`: Macroeconomic data fetching
- `clean_ticker()`: Ticker symbol standardization

#### [ingest_data.md](ingestion/ingest_data.md)
**Module**: `ingestion/ingest_data.py`
**Purpose**: Orchestrates data ingestion pipeline with batch processing
**Key Features**:
- Batch processing for CI/CD environments
- Cloud storage integration
- S&P 500 and ETF ticker management
- Incremental data updates

**Functions**:
- `main()`: Primary batch processing orchestrator
- `update_ticker_data()`: Individual ticker updates
- `update_macro_data()`: Macro data updates
- `get_sp500_tickers()`: S&P 500 ticker list retrieval

#### [enrich_data.md](ingestion/enrich_data.md)
**Module**: `ingestion/enrich_data.py`
**Purpose**: Advanced technical indicator calculation engine
**Key Features**:
- 129+ technical indicators across 7 categories
- TA-Lib integration with fallback mechanisms
- Memory-efficient processing
- Comprehensive error handling

**Classes & Methods**:
- `TechnicalIndicators`: Main indicator calculation class
- Momentum, Volume, Volatility, Pattern indicators
- Custom indicators: Chaikin Money Flow, Donchian Channels
- Safe data merging and transformation

#### [transform_data.md](ingestion/transform_data.md)
**Module**: `ingestion/transform_data.py`
**Purpose**: Data transformation pipeline for processed datasets
**Key Features**:
- Cloud-native design for GCS integration
- Basic and advanced indicator calculations
- Batch processing of all tickers
- Fallback strategies for TA-Lib unavailability

**Classes & Functions**:
- `TechnicalTransformer`: Main transformation engine
- `calculate_basic_indicators()`: Pandas-based indicators
- `calculate_talib_indicators()`: TA-Lib advanced indicators
- `transform_ticker()`: Complete ticker transformation

### Tests Module Documentation
Comprehensive unit test suites for all major components.

#### [test_indicators.md](tests/test_indicators.md)
**Module**: `tests/test_indicators.py`
**Purpose**: Technical indicators functionality testing
**Key Features**:
- Real market data testing (AAPL, GOOGL)
- Comprehensive indicator validation
- Edge case handling tests
- Data merging safety tests

**Test Functions**:
- `test_technical_indicators()`: Core indicator calculation
- `test_safe_merge_or_concat()`: Data merging validation
- `test_indicators()`: Complete transformation pipeline

#### [test_ingestion.md](tests/test_ingestion.md)
**Module**: `tests/test_ingestion.py`
**Purpose**: Data ingestion functionality testing
**Key Features**:
- Error condition testing
- API integration validation
- Data cleaning verification
- Input validation testing

**Test Functions**:
- Error handling tests for invalid inputs
- Data fetching validation with real APIs
- Ticker cleaning functionality tests
- Macro data integration tests

#### [test_macros.md](tests/test_macros.md)
**Module**: `tests/test_macros.py`
**Purpose**: Macroeconomic data processing testing
**Key Features**:
- FRED API integration testing
- Economic indicator transformation validation
- Statistical operation verification
- Error handling for edge cases

**Test Functions**:
- `test_enrich_macros()`: Complete macro enrichment pipeline
- Individual transformation tests (lag, rolling, z-score)
- Data initialization and error handling tests

### Utils Module Documentation
Core utility modules for database operations and data management.

#### [gcs.md](utils/gcs.md)
**Module**: `utils/gcs.py`
**Purpose**: Google Cloud Storage interface
**Key Features**:
- Pandas DataFrame cloud storage
- Parquet format optimization
- Batch operations support
- Memory-efficient streaming

**Functions**:
- `upload_to_gcs()`: DataFrame cloud upload
- `load_from_gcs()`: DataFrame cloud download
- `blob_exists()`: File existence checking
- `load_all_gcs_data()`: Batch data loading

#### [models.md](utils/models.md)
**Module**: `utils/models.py`
**Purpose**: SQLAlchemy ORM models for database schema
**Key Features**:
- Comprehensive database schema definitions
- 150+ column support for technical indicators
- Normalized macro data storage
- Enterprise-grade data modeling

**Models**:
- `TickerData`: Raw OHLCV data storage
- `MacroData`: Macroeconomic indicators storage
- `TransformedTickers`: Complete enriched data schema

#### [oracledb.md](utils/oracledb.md)
**Module**: `utils/oracledb.py`
**Purpose**: Oracle database connectivity and session management
**Key Features**:
- Enterprise Oracle integration
- Session lifecycle management
- Connection testing and validation
- Environment-based configuration

**Classes & Methods**:
- `OracleDB`: Main database connection class
- `establish_connection()`: Session establishment
- `close_oracledb_session()`: Resource cleanup
- `test_connection()`: Connectivity validation

## Documentation Standards

### Document Structure
Each documentation file follows a consistent structure:

1. **Overview**: Purpose and high-level functionality
2. **Architecture**: Design philosophy and integration points
3. **Classes/Functions**: Detailed API documentation
4. **Usage Examples**: Practical implementation examples
5. **Configuration**: Environment and setup requirements
6. **Performance**: Optimization and scalability considerations
7. **Error Handling**: Common issues and troubleshooting
8. **Best Practices**: Recommended usage patterns
9. **Integration**: How modules work together
10. **Future Enhancements**: Potential improvements

### Code Examples
All documentation includes:
- **Practical Examples**: Real-world usage scenarios
- **Error Handling**: Proper exception management
- **Configuration**: Environment setup examples
- **Integration**: Cross-module usage patterns

### Technical Depth
Documentation provides:
- **Implementation Details**: Internal workings and algorithms
- **Performance Characteristics**: Speed and memory considerations
- **Security Considerations**: Best practices for production
- **Troubleshooting Guides**: Common issues and solutions

## Quick Reference

### Module Dependencies
```
fetch_data.py
├── External APIs: Polygon.io, FRED
├── Dependencies: requests, pandas, fredapi
└── Output: Raw OHLCV and macro data

ingest_data.py  
├── Imports: fetch_data.py, utils/gcs.py
├── Dependencies: pandas, tqdm
└── Output: Cloud-stored raw data

enrich_data.py
├── Imports: fetch_data.py
├── Dependencies: pandas, talib (optional)
└── Output: Enriched data with 129+ indicators

transform_data.py
├── Imports: utils/gcs.py, talib (optional)
├── Dependencies: pandas
└── Output: Processed data in cloud storage

utils/gcs.py
├── Dependencies: google-cloud-storage, pandas
└── Purpose: Cloud storage interface

utils/models.py
├── Dependencies: sqlalchemy
└── Purpose: Database schema definitions

utils/oracledb.py
├── Dependencies: sqlalchemy, cx_oracle
└── Purpose: Oracle database connectivity
```

### Key Environment Variables
```bash
# API Access
POLYGON_API_KEY="your_polygon_key"
FRED_API_KEY="your_fred_key"

# Google Cloud Storage
GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

# Oracle Database (optional)
ORACLE_USER="username"
ORACLE_PASSWORD="password"
ORACLE_HOST="hostname"
ORACLE_SERVICE_NAME="service"

# Batch Processing
BATCH_NUMBER="0"
BATCH_SIZE="25"
```

### Data Flow Overview
```
1. fetch_data.py → Raw API data retrieval
2. ingest_data.py → Batch processing and cloud storage
3. transform_data.py → Load, transform, save processed data
4. enrich_data.py → Advanced indicator calculations
5. utils/gcs.py → Cloud storage operations throughout
6. utils/models.py → Database schema for persistence
7. utils/oracledb.py → Enterprise database integration
```

## Getting Started

### For Developers
1. **Start with**: [fetch_data.md](ingestion/fetch_data.md) for understanding data sources
2. **Then read**: [ingest_data.md](ingestion/ingest_data.md) for pipeline orchestration
3. **Deep dive**: [enrich_data.md](ingestion/enrich_data.md) for technical analysis
4. **Integration**: [gcs.md](utils/gcs.md) for cloud storage patterns

### For Data Scientists
1. **Begin with**: [models.md](utils/models.md) for data schema understanding
2. **Then explore**: [enrich_data.md](ingestion/enrich_data.md) for available indicators
3. **Data access**: [gcs.md](utils/gcs.md) for loading datasets
4. **Testing**: [test_indicators.md](tests/test_indicators.md) for validation patterns

### For DevOps Engineers
1. **Infrastructure**: [gcs.md](utils/gcs.md) for cloud storage setup
2. **Database**: [oracledb.md](utils/oracledb.md) for enterprise integration
3. **Pipeline**: [ingest_data.md](ingestion/ingest_data.md) for CI/CD configuration
4. **Monitoring**: Error handling sections across all documents

### For QA Engineers
1. **Test Strategy**: [test_ingestion.md](tests/test_ingestion.md) for API testing
2. **Data Quality**: [test_indicators.md](tests/test_indicators.md) for calculation validation
3. **Integration**: [test_macros.md](tests/test_macros.md) for macro data testing
4. **Performance**: Performance sections across all documents

## Test Suite Documentation

### [test_ingestion.md](tests/test_ingestion.md)
**Module**: `tests/test_ingestion.py`
**Purpose**: Comprehensive testing of data ingestion pipeline
**Coverage**:
- Oracle DB integration and table operations
- Data fetching and API validation
- Error handling and edge cases
- Database persistence and retrieval

**Key Test Areas**:
- Database connection and table creation
- Ticker and macro data insertion
- Data validation and integrity checks
- Mock data generation for testing

### [test_indicators.md](tests/test_indicators.md)
**Module**: `tests/test_indicators.py`
**Purpose**: Technical indicator calculation validation
**Coverage**:
- Technical analysis indicators (RSI, MACD, Bollinger Bands, etc.)
- Volume and volatility indicators
- Price action and candlestick patterns
- Statistical measures and transformations

**Key Test Areas**:
- TA-Lib indicator calculations
- DataFrame merging and concatenation
- Edge case handling (empty data, NaN values)
- Performance validation for large datasets

### [test_macros.md](tests/test_macros.md)  
**Module**: `tests/test_macros.py`
**Purpose**: Macroeconomic data transformation testing
**Coverage**:
- FRED API data integration
- Data lagging and rolling calculations
- Percentage changes and z-score normalization
- Empty DataFrame handling

**Key Test Areas**:
- Macro data enrichment pipeline
- Time series transformations
- Statistical calculations validation
- Data persistence and file I/O

## Maintenance

### Keeping Documentation Current
- **Code Changes**: Update docs when modifying functionality
- **API Changes**: Update integration docs for external API changes
- **Performance Updates**: Refresh performance characteristics
- **Best Practices**: Evolve recommendations based on experience

### Documentation Contribution
1. **Follow Standards**: Use consistent structure and formatting
2. **Include Examples**: Provide practical, working code examples
3. **Error Scenarios**: Document common failure modes and solutions
4. **Cross-Reference**: Link related functionality across modules

### Version Control
- **Sync with Code**: Keep documentation in sync with code versions
- **Change Tracking**: Document breaking changes and migration paths
- **Release Notes**: Update docs with new features and improvements
