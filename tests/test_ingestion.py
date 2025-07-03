from ingestion.fetch_data import DataIngestion
import pytest
import pandas as pd

@pytest.fixture
def test_data_ingestion_with_valid_data(tickers):
    data = DataIngestion()
    data.fetch()
    assert isinstance(data.ticker_df, pd.DataFrame)
    assert not data.ticker_df.empty
    assert all(ticker in data.ticker_df['ticker'].values for ticker in data.all_tickers)
    assert isinstance(data.ticker_df['ticker'], pd.Series)
    assert isinstance(data.macro_df, pd.DataFrame)
    assert not data.macro_df.empty