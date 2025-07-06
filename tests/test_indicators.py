import pytest
from ingestion.fetch_data import DataIngestion
from ingestion.enrich_data import TechnicalIndicators
import pandas as pd

"""
Updated test functions for testing technical indicators calculation, safe merge or
concat operation, and various momentum indicators calculation using the TechnicalIndicators class.
"""

@pytest.fixture
def sample_ticker_data():
    """
    Create sample ticker data for testing technical indicators.
    Uses a smaller date range to speed up tests.
    """
    print("Setting up sample ticker data for testing...")
    data = DataIngestion(start_date="2024-01-01", end_date="2024-01-31")
    ticker_df = data.ingest_ticker_data("AAPL")
    
    if ticker_df is None or ticker_df.empty:
        # Create mock data if API fails
        dates = pd.date_range(start="2024-01-01", end="2024-01-31", freq='D')
        ticker_df = pd.DataFrame({
            'Date': dates,
            'Ticker': 'AAPL',
            'Open': 150.0 + (dates.dayofyear * 0.1),
            'High': 155.0 + (dates.dayofyear * 0.1),
            'Low': 145.0 + (dates.dayofyear * 0.1),
            'Close': 152.0 + (dates.dayofyear * 0.1),
            'Volume': 1000000
        })
    
    return ticker_df

@pytest.fixture
def technical_indicators(sample_ticker_data):
    """Create TechnicalIndicators instance with sample data."""
    return TechnicalIndicators(sample_ticker_data)

def test_technical_indicators_initialization(sample_ticker_data):
    """Test TechnicalIndicators class initialization."""
    indicators = TechnicalIndicators(sample_ticker_data)
    assert indicators.ticker_df is not None
    assert not indicators.ticker_df.empty
    assert isinstance(indicators.ticker_df, pd.DataFrame)
    
    # Check required columns
    required_columns = ['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']
    for col in required_columns:
        assert col in indicators.ticker_df.columns

def test_enrich_tickers(technical_indicators):
    """Test ticker enrichment with return calculations."""
    technical_indicators.enrich_tickers()
    
    # Check that return columns were added
    return_columns = ['Return_1d', 'Return_3d', 'Return_7d', 'Return_30d', 'Return_90d', 'Return_365d']
    for col in return_columns:
        assert col in technical_indicators.ticker_df.columns
        assert isinstance(technical_indicators.ticker_df[col], pd.Series)

def test_calculate_technical_indicators(technical_indicators):
    """Test full technical indicators calculation."""
    # Skip if TA-Lib is not available
    try:
        import talib
    except ImportError:
        pytest.skip("TA-Lib not available for testing")
    
    # Add return calculations first
    technical_indicators.enrich_tickers()
    
    # Calculate technical indicators
    result_df = technical_indicators.calculate_technical_indicators()
    
    assert result_df is not None
    assert not result_df.empty
    assert isinstance(result_df, pd.DataFrame)

def test_momentum_indicators(technical_indicators):
    """Test momentum indicators calculation."""
    try:
        import talib
    except ImportError:
        pytest.skip("TA-Lib not available for testing")
    
    momentum_df = technical_indicators.get_talib_momentum_indicators(technical_indicators.ticker_df)
    
    assert isinstance(momentum_df, pd.DataFrame)
    
    # Check for key momentum indicators
    expected_columns = ['RSI', 'MACD', 'MACD_Signal', 'CCI', 'ROC', 'SMA20', 'EMA20']
    for col in expected_columns:
        assert col in momentum_df.columns
        assert isinstance(momentum_df[col], pd.Series)

def test_volume_indicators(technical_indicators):
    """Test volume indicators calculation."""
    try:
        import talib
    except ImportError:
        pytest.skip("TA-Lib not available for testing")
    
    volume_df = technical_indicators.get_talib_volume_indicators(technical_indicators.ticker_df)
    
    assert isinstance(volume_df, pd.DataFrame)
    
    # Check for key volume indicators
    expected_columns = ['OBV', 'AD', 'MFI', 'VWAP', 'ADX']
    for col in expected_columns:
        assert col in volume_df.columns
        assert isinstance(volume_df[col], pd.Series)

def test_volatility_indicators(technical_indicators):
    """Test volatility indicators calculation."""
    try:
        import talib
    except ImportError:
        pytest.skip("TA-Lib not available for testing")
    
    volatility_df = technical_indicators.get_talib_volatility_indicators(technical_indicators.ticker_df)
    
    assert isinstance(volatility_df, pd.DataFrame)
    
    # Check for key volatility indicators
    expected_columns = ['ATR', 'BBANDS_UPPER', 'BBANDS_MIDDLE', 'BBANDS_LOWER']
    for col in expected_columns:
        assert col in volatility_df.columns
        assert isinstance(volatility_df[col], pd.Series)

def test_safe_merge_or_concat(technical_indicators):
    """Test safe merge or concat functionality."""
    # Create a DataFrame with different tickers to avoid overlap error
    df1 = technical_indicators.ticker_df.head(5).copy()
    
    # Create a second DataFrame with different data and same Date/Ticker structure
    df2 = df1[['Date', 'Ticker']].copy()
    df2['test_column'] = 1.0
    
    merged_df = technical_indicators.safe_merge_or_concat(df1, df2, on=['Date', 'Ticker'], how='left')
    
    assert isinstance(merged_df, pd.DataFrame)
    assert not merged_df.empty
    assert 'test_column' in merged_df.columns

def test_safe_merge_empty_dataframes(technical_indicators):
    """Test safe merge with empty DataFrames."""
    df1 = technical_indicators.ticker_df.copy()
    df2 = pd.DataFrame()
    
    result = technical_indicators.safe_merge_or_concat(df1, df2, on=['Date', 'Ticker'], how='left')
    
    # Should return the non-empty DataFrame
    assert isinstance(result, pd.DataFrame)
    assert len(result) == len(df1)

def test_chaikin_money_flow(technical_indicators):
    """Test Chaikin Money Flow calculation."""
    cmf = technical_indicators.chaikin_money_flow(technical_indicators.ticker_df, period=20)
    
    assert isinstance(cmf, pd.Series)
    assert len(cmf) == len(technical_indicators.ticker_df)

def test_donchian_channel(technical_indicators):
    """Test Donchian Channel calculation."""
    upper, lower = technical_indicators.donchian_channel(technical_indicators.ticker_df, period=20)
    
    assert isinstance(upper, pd.Series)
    assert isinstance(lower, pd.Series)
    assert len(upper) == len(technical_indicators.ticker_df)
    assert len(lower) == len(technical_indicators.ticker_df)
    
    # Upper channel should be >= lower channel
    valid_indices = ~(upper.isna() | lower.isna())
    if valid_indices.any():
        assert (upper[valid_indices] >= lower[valid_indices]).all()

def test_ulcer_index(technical_indicators):
    """Test Ulcer Index calculation."""
    close_prices = technical_indicators.ticker_df['Close']
    ulcer = technical_indicators.ulcer_index(close_prices, period=14)
    
    assert isinstance(ulcer, pd.Series)
    assert len(ulcer) == len(close_prices)

def test_persist_transformed_data(technical_indicators, tmp_path):
    """Test saving transformed data to file."""
    try:
        import talib
    except ImportError:
        pytest.skip("TA-Lib not available for testing")
    
    # Calculate indicators first
    technical_indicators.enrich_tickers()
    technical_indicators.calculate_technical_indicators()
    
    # Test file saving
    test_file = tmp_path / "test_transformed_data.parquet"
    technical_indicators.persist_transformed_data(str(test_file))
    
    # Check that file was created
    assert test_file.exists()
    
    # Try to read the file back
    loaded_df = pd.read_parquet(str(test_file))
    assert isinstance(loaded_df, pd.DataFrame)
    assert not loaded_df.empty