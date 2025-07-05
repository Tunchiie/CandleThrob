import pytest
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ingestion.fetch_data import DataIngestion
from ingestion.enrich_tickers import TechnicalIndicators
import pandas as pd

"""
    The code contains pytest test functions for testing technical indicators calculation, safe merge or
    concat operation, and various momentum indicators calculation using the TechnicalIndicators class.
    
    :param sample_tickers: The `sample_tickers` fixture is a pytest fixture that sets up sample tickers
    for testing purposes. It fetches data for tickers "AAPL" and "GOOGL" within a specified date range
    using the `DataIngestion` class and returns the fetched data
"""
@pytest.fixture
def sample_tickers():
    print("Setting up sample tickers for testing...")
    data = DataIngestion(tickers=["AAPL", "GOOGL"], start_date="2022-01-01", end_date="2022-12-31")
    data.fetch()
    return data

def test_technical_indicators(sample_tickers):
    print("Testing technical indicators calculation...")
    indicators = TechnicalIndicators(sample_tickers.ticker_df)
    indicators.calculate_technical_indicators()

    assert 'CCI' in indicators.transformed_df.columns
    assert 'ADX' in indicators.transformed_df.columns
    assert 'RSI' in indicators.transformed_df.columns

    assert isinstance(indicators.transformed_df['CCI'], pd.Series)
    assert isinstance(indicators.transformed_df['ADX'], pd.Series)
    assert isinstance(indicators.transformed_df['RSI'], pd.Series)

    assert not indicators.transformed_df['CCI'].isnull().all()
    assert not indicators.transformed_df['ADX'].isnull().all()
    assert not indicators.transformed_df['RSI'].isnull().all()
    
def test_safe_merge_or_concat(sample_tickers):
    print("Testing safe merge or concat...")
    indicators = TechnicalIndicators(sample_tickers.ticker_df)
    
    df1 = sample_tickers.ticker_df.head(10)
    df2 = sample_tickers.ticker_df.tail(10)
    
    merged_df = indicators.safe_merge_or_concat(df1, df2, on=['Date', 'Ticker'], how='left')
    
    assert isinstance(merged_df, pd.DataFrame)
    assert not merged_df.empty
    assert 'Date' in merged_df.columns
    assert 'Ticker' in merged_df.columns
    assert len(merged_df) != len(df1) + len(df2) - len(set(df1['Ticker']).intersection(set(df2['Ticker'])))
    
def test_indicators(sample_tickers):
    print("Testing momentum indicators calculation...")
    indicators = TechnicalIndicators(sample_tickers.ticker_df)
    assert indicators.get_talib_momentum_indicators(pd.DataFrame()).empty
    assert indicators.get_talib_volume_indicators(pd.DataFrame()).empty
    assert indicators.get_talib_pattern_indicators(pd.DataFrame()).empty
    assert indicators.get_talib_volatility_indicators(pd.DataFrame()).empty
    assert indicators.get_talib_cyclical_indicators(pd.DataFrame()).empty
    assert isinstance(indicators.transformed_df, pd.DataFrame)
    # Check if indicators were calculated
    indicators.calculate_technical_indicators()
    assert not indicators.transformed_df.empty
    assert 'Date' in indicators.transformed_df.columns
    assert 'Ticker' in indicators.transformed_df.columns
    assert 'Open' in indicators.transformed_df.columns
    assert 'Close' in indicators.transformed_df.columns
    assert 'High' in indicators.transformed_df.columns
    assert 'Low' in indicators.transformed_df.columns
    assert 'RSI' in indicators.transformed_df.columns
    assert 'CCI' in indicators.transformed_df.columns
    assert 'ADX' in indicators.transformed_df.columns