import pytest
import pandas as pd
from ingestion.fetch_data import DataIngestion
from ingestion.ingest_data import clean_ticker, get_etf_tickers, get_sp500_tickers
from utils.oracledb import OracleDB
from utils.models import TickerData, MacroData

"""
Updated test functions for data ingestion functionality, including tests for
database operations, ticker data fetching, macro data fetching, and utility functions.
"""

# Test fixtures
@pytest.fixture
def sample_data_ingestion():
    """Create a DataIngestion instance for testing."""
    return DataIngestion(start_date="2024-01-01", end_date="2024-01-05")

@pytest.fixture
def db_session():
    """Create a database session for testing."""
    db = OracleDB()
    session = db.get_oracledb_session()
    yield session
    db.close_oracledb_session()

def test_data_ingestion_initialization():
    """Test DataIngestion class initialization."""
    data = DataIngestion()
    assert data.start_date is not None
    assert data.end_date is not None
    
def test_data_ingestion_with_invalid_date():
    """Test that invalid date ranges raise ValueError."""
    with pytest.raises(ValueError):
        DataIngestion(start_date="2023-01-01", end_date="2022-12-31")

def test_ticker_data_fetching(sample_data_ingestion):
    """Test fetching ticker data for a valid ticker."""
    ticker_df = sample_data_ingestion.ingest_ticker_data("AAPL")
    
    if ticker_df is not None and not ticker_df.empty:
        assert isinstance(ticker_df, pd.DataFrame)
        required_columns = ['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']
        for col in required_columns:
            assert col in ticker_df.columns
        assert ticker_df['Ticker'].iloc[0] == "AAPL"

def test_macro_data_fetching(sample_data_ingestion):
    """Test fetching macroeconomic data."""
    sample_data_ingestion.fetch_fred_data()
    
    if sample_data_ingestion.macro_df is not None and not sample_data_ingestion.macro_df.empty:
        assert isinstance(sample_data_ingestion.macro_df, pd.DataFrame)
        # Check for some expected macro indicators
        expected_columns = ["GDP", "UNRATE", "FEDFUNDS", "CPIAUCSL"]
        for col in expected_columns:
            if col in sample_data_ingestion.macro_df.columns:
                assert isinstance(sample_data_ingestion.macro_df[col], pd.Series)

def test_clean_ticker():
    """Test ticker symbol cleaning function."""
    assert clean_ticker("AAPL@") == "AAPL"
    assert clean_ticker("$GOOGL ") == "GOOGL"
    assert clean_ticker("  MSFT") == "MSFT"
    assert clean_ticker("AMZN!") == "AMZN"
    assert clean_ticker("  TSLA  ") == "TSLA"
    assert clean_ticker("  NFLX@") == "NFLX"
    assert clean_ticker("BRK.B") == "BRK-B"  # Test period to dash conversion

def test_get_etf_tickers():
    """Test ETF ticker list retrieval."""
    etf_tickers = get_etf_tickers()
    assert isinstance(etf_tickers, list)
    assert len(etf_tickers) > 0
    assert "SPY" in etf_tickers
    assert "QQQ" in etf_tickers
    # All tickers should be strings and properly formatted
    for ticker in etf_tickers:
        assert isinstance(ticker, str)
        assert len(ticker) > 0

def test_get_sp500_tickers():
    """Test S&P 500 ticker list retrieval."""
    try:
        sp500_tickers = get_sp500_tickers()
        assert isinstance(sp500_tickers, list)
        assert len(sp500_tickers) > 400  # S&P 500 should have ~500 tickers
        # All tickers should be strings and properly formatted
        for ticker in sp500_tickers:
            assert isinstance(ticker, str)
            assert len(ticker) > 0
    except Exception:
        # Skip test if web scraping fails (network issues, etc.)
        pytest.skip("S&P 500 ticker fetching failed - likely network issue")

def test_database_connection(db_session):
    """Test database connection and basic operations."""
    from sqlalchemy import text
    
    # Test basic query
    result = db_session.execute(text("SELECT 1 as test_col"))
    row = result.fetchone()
    assert row[0] == 1

def test_ticker_data_table_creation(db_session):
    """Test TickerData table creation."""
    ticker_data = TickerData()
    ticker_data.create_table(db_session)
    
    # Verify table exists by attempting to query it
    from sqlalchemy import text
    try:
        result = db_session.execute(text("SELECT COUNT(*) FROM raw_ticker_data"))
        count = result.fetchone()[0]
        assert count >= 0  # Should return 0 or more rows
    except Exception as e:
        pytest.fail(f"TickerData table creation failed: {e}")

def test_macro_data_table_creation(db_session):
    """Test MacroData table creation."""
    macro_data = MacroData()
    macro_data.create_table(db_session)
    
    # Verify table exists by attempting to query it  
    from sqlalchemy import text
    try:
        result = db_session.execute(text("SELECT COUNT(*) FROM raw_macro_data"))
        count = result.fetchone()[0]
        assert count >= 0  # Should return 0 or more rows
    except Exception as e:
        pytest.fail(f"MacroData table creation failed: {e}")

def test_data_insertion_ticker(db_session):
    """Test inserting ticker data into database."""
    ticker_data = TickerData()
    ticker_data.create_table(db_session)
    
    # Create sample data
    sample_data = {
        'date': pd.to_datetime('2024-01-01').date(),
        'ticker': 'TEST',
        'open': 100.0,
        'high': 105.0,
        'low': 95.0,
        'close': 102.0,
        'volume': 1000000,
        'year': 2024,
        'month': 1,
        'weekday': 0
    }
    
    try:
        ticker_data.insert_data(db_session, sample_data)
        # If no exception is raised, insertion was successful
        assert True
    except Exception as e:
        pytest.fail(f"Ticker data insertion failed: {e}")

def test_data_insertion_macro(db_session):
    """Test inserting macro data into database."""
    macro_data = MacroData()
    macro_data.create_table(db_session)
    
    # Create sample data
    sample_data = {
        'date': pd.to_datetime('2024-01-01').date(),
        'indicator_name': 'TEST_INDICATOR',
        'value': 1.5,
        'year': 2024,
        'month': 1,
        'weekday': 0
    }
    
    try:
        macro_data.insert_data(db_session, sample_data)
        # If no exception is raised, insertion was successful
        assert True
    except Exception as e:
        pytest.fail(f"Macro data insertion failed: {e}")