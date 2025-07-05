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
        data = DataIngestion(tickers=["AAPL"], start_date="2023-01-01", end_date="2022-12-31")

def test_data_ingestion_with_empty_tickers():
    data = DataIngestion(tickers=[])
    with pytest.raises(ValueError):
        data.fetch()
        
def test_data_ingestion_with_nonexistent_ticker():
    data = DataIngestion(tickers=["NONEXISTENT"])
    with pytest.raises(ValueError):
        data.fetch()
        
def test_data_ingestion_with_partial_data():
    data = DataIngestion(tickers=["AAPL", "GOOGL"])
    data.fetch()
    assert isinstance(data.ticker_df, pd.DataFrame)
    assert not data.ticker_df.empty
    assert all(ticker in data.ticker_df['Ticker'].values for ticker in ["AAPL", "GOOGL"])
    assert isinstance(data.ticker_df['Ticker'], pd.Series)

def test_data_ingestion_with_large_date_range():
    data = DataIngestion(tickers=["AAPL", "GOOGL"], start_date="2000-01-01", end_date="2023-12-31")
    data.fetch()
    assert isinstance(data.ticker_df, pd.DataFrame)
    assert not data.ticker_df.empty
    assert isinstance(data.ticker_df['Ticker'], pd.Series)
    
def test_data_ingestion_with_invalid_ticker_format():
    data = DataIngestion(tickers=["INVALID TICKER"])
    with pytest.raises(ValueError):
        data.fetch()

def test_data_ingestion_with_valid_ticker_format():
    data = DataIngestion(tickers=["AAPL", "GOOGL"])
    data.fetch()
    assert isinstance(data.ticker_df, pd.DataFrame)
    assert not data.ticker_df.empty
    assert all(ticker in data.ticker_df['Ticker'].values for ticker in ["AAPL", "GOOGL"])
    assert isinstance(data.ticker_df['Ticker'], pd.Series)


def test_clean_ticker():
    data = DataIngestion(tickers=["AAPL", "GOOGL"], start_date="2022-01-01", end_date="2022-12-31")
    assert data.clean_ticker("AAPL@") == "AAPL"
    assert data.clean_ticker("$GOOGL ") == "GOOGL"
    assert data.clean_ticker("  MSFT") == "MSFT"
    assert data.clean_ticker("AMZN!") == "AMZN"
    assert data.clean_ticker("  TSLA  ") == "TSLA"
    assert data.clean_ticker("  NFLX@") == "NFLX"