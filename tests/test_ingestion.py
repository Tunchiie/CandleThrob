from ingestion.fetch_data import DataIngestion
import pytest
import pandas as pd
    
def test_data_ingestion_with_invalid_date():
    with pytest.raises(ValueError):
        data = DataIngestion(start_date="2023-01-01", end_date="2022-12-31")

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
    assert isinstance(data.macro_df, pd.DataFrame)
    assert not data.macro_df.empty

def test_data_ingestion_with_large_date_range():
    data = DataIngestion(tickers=["AAPL", "GOOGL"], start_date="2000-01-01", end_date="2023-12-31")
    data.fetch()
    assert isinstance(data.ticker_df, pd.DataFrame)
    assert not data.ticker_df.empty
    assert isinstance(data.ticker_df['Ticker'], pd.Series)
    assert isinstance(data.macro_df, pd.DataFrame)
    assert not data.macro_df.empty
    
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
    assert isinstance(data.macro_df, pd.DataFrame)
    assert not data.macro_df.empty
    
