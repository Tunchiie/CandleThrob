# CandleThrob Test Suite Documentation

## Overview
The CandleThrob test suite provides comprehensive validation of the financial data pipeline, ensuring data quality, API integration, and calculation accuracy across all modules.

## Test Architecture

### Testing Strategy
- **Unit Testing**: Individual function and method validation
- **Integration Testing**: End-to-end pipeline validation  
- **Data Quality Testing**: Output validation and edge case handling
- **Mock Testing**: Fallback scenarios and API failure handling

### Test Framework
- **Primary Framework**: pytest
- **Fixtures**: Reusable test data and setup
- **Mocking**: API responses and external dependencies
- **Coverage**: Core functionality and error conditions

## Test Modules

### 1. test_ingestion.py
**Purpose**: Validates data ingestion pipeline and database operations

**Key Features**:
- Oracle DB connection and table creation
- Data insertion and retrieval validation
- API integration testing (with fallbacks)
- Mock data generation for offline testing

**Test Categories**:
- Database operations (connection, table creation, CRUD)
- Data fetching (ticker data, macro data)
- Error handling (invalid inputs, connection failures)
- Data validation (schema compliance, data types)

### 2. test_indicators.py  
**Purpose**: Validates technical indicator calculations and data transformations

**Key Features**:
- TA-Lib integration validation
- DataFrame operations and merging
- Technical analysis accuracy
- Performance benchmarking

**Test Categories**:
- Momentum indicators (RSI, MACD, Stochastic)
- Volume indicators (OBV, MFI, A/D Line)
- Volatility indicators (ATR, Bollinger Bands)
- Statistical functions (rolling means, standard deviation)

### 3. test_macros.py
**Purpose**: Validates macroeconomic data processing and transformations

**Key Features**:
- FRED API integration
- Time series transformations
- Statistical calculations
- Data enrichment pipeline

**Test Categories**:
- Data lagging and shifting
- Rolling calculations and windows
- Percentage changes and growth rates
- Z-score normalization and standardization

## Test Data Management

### Fixtures
```python
@pytest.fixture
def sample_data_ingestion():
    """Provides DataIngestion instance with mock data."""

@pytest.fixture  
def sample_macros():
    """Provides sample macroeconomic data for testing."""

@pytest.fixture
def technical_indicators():
    """Provides TechnicalIndicators instance with sample data."""
```

### Mock Data Strategy
- **Primary**: Real API data when available
- **Fallback**: Synthetic data that mimics real market patterns
- **Edge Cases**: Empty datasets, invalid data, extreme values
- **Performance**: Large datasets for scalability testing

## Database Testing

### Oracle DB Integration
```python
def test_database_connection(db_session):
    """Test Oracle database connectivity."""

def test_ticker_data_table_creation(db_session):
    """Test table creation and schema validation."""

def test_data_insertion_ticker(db_session):
    """Test data insertion and retrieval."""
```

### SQLite Fallback
- Local SQLite databases for CI/CD environments
- Same schema validation as Oracle
- Consistent behavior across environments

## Running Tests

### Full Test Suite
```bash
# Run all tests
pytest tests/

# Run with coverage
pytest --cov=ingestion --cov=utils tests/

# Run specific test module
pytest tests/test_ingestion.py
```

### Individual Test Categories
```bash
# Database tests only
pytest tests/test_ingestion.py::test_database_connection

# Technical indicators only  
pytest tests/test_indicators.py

# Macro data tests only
pytest tests/test_macros.py
```

### Continuous Integration
```bash
# CI-friendly test run (no external APIs)
pytest tests/ --tb=short -v

# Performance testing
pytest tests/ --benchmark-only
```

## Test Environment Setup

### Required Environment Variables
```bash
# Optional: FRED API key for real data testing
FRED_API_KEY="your_fred_api_key"

# Optional: Oracle DB credentials for full DB testing
ORACLE_USER="username"
ORACLE_PASSWORD="password"  
ORACLE_HOST="hostname"
ORACLE_SERVICE_NAME="service"
```

### Dependencies
```bash
# Install test dependencies
pip install pytest pytest-cov pandas sqlalchemy

# Optional: TA-Lib for technical indicators
pip install TA-Lib

# Optional: Oracle client for DB testing
pip install oracledb
```

## Test Data Validation

### Data Quality Checks
- **Schema Compliance**: Ensure output matches expected schema
- **Data Types**: Validate numeric, date, and string types
- **Range Validation**: Check for reasonable value ranges
- **Null Handling**: Validate NaN/null value handling

### Statistical Validation
- **Technical Indicators**: Compare with known benchmark calculations
- **Macro Transformations**: Validate statistical properties
- **Time Series**: Ensure proper time alignment and ordering

## Error Scenarios

### Network Failures
- API timeout handling
- Connection retry logic
- Graceful degradation to mock data

### Data Quality Issues
- Empty API responses
- Malformed data handling
- Missing or incomplete datasets

### Database Errors
- Connection failures
- Transaction rollback testing
- Schema migration validation

## Performance Testing

### Benchmark Targets
- **Data Fetching**: < 30 seconds for 1000 tickers
- **Indicator Calculation**: < 10 seconds for 1M data points
- **Database Operations**: < 5 seconds for 10K insertions

### Memory Management
- Large dataset handling
- Memory leak detection
- Garbage collection validation

## Maintenance Guidelines

### Test Updates
- Update tests when API schemas change
- Refresh mock data periodically
- Validate test accuracy against real market data

### Documentation
- Keep test documentation in sync with code
- Document new test scenarios and edge cases
- Update performance benchmarks regularly

### Quality Assurance
- Review test coverage regularly
- Add tests for new features
- Remove obsolete or redundant tests

## Troubleshooting

### Common Issues
1. **API Rate Limits**: Use mock data or reduce test frequency
2. **Database Connectivity**: Verify credentials and network access
3. **TA-Lib Installation**: Follow platform-specific installation guides
4. **Memory Issues**: Reduce dataset sizes for testing

### Debug Mode
```bash
# Run tests with detailed output
pytest tests/ -v -s

# Debug specific test failure
pytest tests/test_ingestion.py::test_specific_function -v -s --pdb
```

### Log Analysis
- Check test logs for API response details
- Validate data transformation steps
- Monitor performance metrics and bottlenecks

**Author**: Adetunji Fasiku
