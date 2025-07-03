import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ingestion.fetch_data import DataIngestion
from ingestion.trans_data import TechnicalIndicators
import pytest
import pandas as pd


if __name__ == "__main__":
    try:
        print("Instantiating...")
        data = DataIngestion("2022-01-01", "2022-12-31")
        print("Fetching...")
        data.fetch()
        print("Success:", data.ticker_df.shape, data.macro_df.shape)
    except Exception as e:
        print("Caught error:", e)
        
        
# @pytest.fixture
# def sample_tickers():
    
#     try:
#         data = DataIngestion("2022-01-01", "2022-12-31")
#         data.fetch()
#     except Exception as e:
#         print(f"Error fetching data: {e}")
#     return data

# def test_technical_indicators(sample_tickers):
#     print("Testing technical indicators calculation...")
#     assert True
    # indicators = TechnicalIndicators(sample_tickers.ticker_df, sample_tickers.macro_df)
    # indicators.calculate_technical_indicators()

    # assert 'CCI' in indicators.ticker_df.columns
    # assert 'ADX' in indicators.ticker_df.columns
    # assert 'RSI' in indicators.ticker_df.columns

    # assert isinstance(indicators.ticker_df['CCI'], pd.Series)
    # assert isinstance(indicators.ticker_df['ADX'], pd.Series)
    # assert isinstance(indicators.ticker_df['RSI'], pd.Series)

    # assert not indicators.ticker_df['CCI'].isnull().all()
    # assert not indicators.ticker_df['ADX'].isnull().all()
    # assert not indicators.ticker_df['RSI'].isnull().all()
    
    # assert 'GDP' in indicators.ticker_df.columns
    # assert 'INFLATION' in indicators.ticker_df.columns

    # assert not indicators.ticker_df['GDP'].isnull().all()
    # assert not indicators.ticker_df['INFLATION'].isnull().all()