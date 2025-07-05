from ingestion.fetch_data import DataIngestion
import pytest
import pandas as pd

"""
    The code contains multiple test functions for data ingestion functionality, including tests for
    handling invalid dates, empty tickers, nonexistent tickers, partial data, large date ranges, invalid
    ticker formats, and cleaning ticker symbols.
"""
    
def test_data_ingestion_with_invalid_date():
    with pytest.raises(ValueError):
        data = DataIngestion(start_date="2023-01-01", end_date="2022-12-31")

def test_data_ingestion_with_empty_tickers():
    data = DataIngestion()
    with pytest.raises(ValueError):
        data.fetch(tickers=[])
        
def test_data_ingestion_with_nonexistent_ticker():
    data = DataIngestion()
    with pytest.raises(ValueError):
        data.fetch(tickers=[])

def test_data_ingestion_with_nonexistent_ticker():
    data = DataIngestion()
    with pytest.raises(ValueError):
        data.fetch(tickers=["NONEXISTENT"])
        
def test_data_ingestion_with_partial_data():
    data = DataIngestion()
    data.fetch(["AAPL", "GOOGL"])
    assert isinstance(data.ticker_df, pd.DataFrame)
    assert not data.ticker_df.empty
    assert all(ticker in data.ticker_df['Ticker'].values for ticker in ["AAPL", "GOOGL"])
    assert isinstance(data.ticker_df['Ticker'], pd.Series)

def test_data_ingestion_with_large_date_range():
    data = DataIngestion(start_date="2000-01-01", end_date="2023-12-31")
    data.fetch(["AAPL", "GOOGL"])
    assert isinstance(data.ticker_df, pd.DataFrame)
    assert not data.ticker_df.empty
    assert isinstance(data.ticker_df['Ticker'], pd.Series)
    
def test_data_ingestion_with_invalid_ticker_format():
    data = DataIngestion()
    with pytest.raises(ValueError):
        data.fetch(["INVALID TICKER"])

def test_data_ingestion_with_valid_ticker_format():
    data = DataIngestion()
    data.fetch(["AAPL", "GOOGL"])
    assert isinstance(data.ticker_df, pd.DataFrame)
    assert not data.ticker_df.empty
    assert all(ticker in data.ticker_df['Ticker'].values for ticker in ["AAPL", "GOOGL"])
    assert isinstance(data.ticker_df['Ticker'], pd.Series)


def test_clean_ticker():
    data = DataIngestion()
    assert data.clean_ticker("AAPL@") == "AAPL"
    assert data.clean_ticker("$GOOGL ") == "GOOGL"
    assert data.clean_ticker("  MSFT") == "MSFT"
    assert data.clean_ticker("AMZN!") == "AMZN"
    assert data.clean_ticker("  TSLA  ") == "TSLA"
    assert data.clean_ticker("  NFLX@") == "NFLX"
    
def test_macro_ingestion():
    data = DataIngestion()
    data.fetch_fred_data()
    assert isinstance(data.macro_df, pd.DataFrame)
    assert not data.macro_df.empty
    assert "GDP" in data.macro_df.columns
    assert "UNRATE" in data.macro_df.columns
    assert "FEDFUNDS" in data.macro_df.columns
    assert "INDPRO" in data.macro_df.columns
    assert "CPIAUCSL" in data.macro_df.columns
    assert "UMCSENT" in data.macro_df.columns
    assert "HOUST" in data.macro_df.columns
    assert "M2SL" in data.macro_df.columns