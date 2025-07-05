import sys
import os
import re
import pandas as pd
import logging
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ingestion.fetch_data import DataIngestion
from ingestion.enrich_tickers import TechnicalIndicators
from ingestion.enrich_macros import EnrichMacros
from utils.gcs import upload_to_gcs, load_from_gcs, blob_exists


logging.basicConfig(
    filename="ingestion/debug.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def update_ticker_data(ticker: str, path: str="tickers"):
    """
    Update the ticker data by fetching new data from the source.

    Args:
        ticker (str): The stock ticker symbol.
        path (str): The directory path where the ticker data is stored."""
    filepath = f"{path}/{ticker}.parquet"
    df = None
    if blob_exists(bucket_name="candlethrob-candata", blob_name=f"{path}/{ticker}.parquet"):
        logger.info("Loading existing ticker data from %s", filepath)
        df = load_from_gcs(
            bucket_name="candlethrob-candata",
            source_blob_name=f"{path}/{ticker}.parquet",
        )
        if df.empty:
            logger.warning("No existing data found for %s, fetching new data.", ticker)
            start = "2020-01-01"
        else:
            # Get the last date in the existing data to determine the start date for new data
            start = df["Date"].max().strftime("%Y-%m-%d")
            end = datetime.now().strftime("%Y-%m-%d")

    logger.info("Updating ticker data for %s from %s to %s", ticker, start, end)
    data = DataIngestion(tickers=[ticker], start_date=start, end_date=end)
    data.fetch()
    indicators = TechnicalIndicators(ticker_df=data.ticker_df)
    indicators.calculate_technical_indicators()
    if df is not None and not df.empty:
        indicators.transformed_df = pd.concat([df, indicators.transformed_df], ignore_index=True)
    else:
        logger.info("No existing data found for %s, using new data only.", ticker)
        if indicators.transformed_df.empty:
            logger.warning("No data was fetched for %s. Please check the ticker symbol or your internet connection.", ticker)
            return
        
    logger.info("Saving transformed ticker data to %s", filepath)
    upload_to_gcs(
        data=indicators.transformed_df,
        bucket_name="candlethrob-candata",
        destination_blob_name=f"{path}/{ticker}.parquet",
    )
    
def update_macro_data(filepath: str="data/macros/macro_data.parquet"):
    """
    Update the macroeconomic data by fetching new data from the FRED API.
    This function will fetch the latest macroeconomic indicators and save them to a CSV file.
    """
    logger.info("Updating macroeconomic data...")
    enrich_macros = EnrichMacros()
    enrich_macros.transform_macro_data()
    logger.info("Saving transformed macroeconomic data to %s", filepath)
        
    if enrich_macros.macro_df.empty:
        logger.warning("No macroeconomic data was fetched. Please check your internet connection or the FRED API.")
        return
    else:
        logger.info("Saving raw macroeconomic data to %s", filepath)
    upload_to_gcs(
        data=enrich_macros.macro_df,
        bucket_name="candlethrob-candata",
        destination_blob_name="macros/macro_data.parquet",
    )

    logger.info("Macro data updated successfully.")

def clean_ticker(ticker:str):
        """
        Clean the ticker symbol by removing unwanted characters and formatting it.
        Args:
            ticker (str): The ticker symbol to clean.
        Returns:
            str: The cleaned ticker symbol."""
        return re.sub(r"[^A-Z0-9\-]", "-", ticker.strip().upper()).strip("-")
    
def get_sp500_tickers():
        """ 
        Get the list of S&P 500 tickers.
        Returns:
            list: A list of S&P 500 ticker symbols.
        """
        tickers = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]["Symbol"].tolist()
        return [clean_ticker(ticker) for ticker in tickers]
    
def get_etf_tickers():
    """ 
    Get the list of ETF tickers.
    Returns:
        list: A list of ETF ticker symbols.
    """
    etf_tickers = ['SPY', 'QQQ', 'DIA', 'IWM', 'VTI', 'TLT', 'GLD',
                    'XLF', 'XLE', 'XLK', 'XLV', 'ARKK', 'VXX',
                    'TQQQ', 'SQQQ', 'SOXL', 'XLI', 'XLY', 'XLP', 'SLV']
    return [clean_ticker(ticker) for ticker in etf_tickers]


def main():
    """
    Main function to run the data ingestion and enrichment process.
    This function will update the ticker data for S&P 500 and ETFs, and also update the macroeconomic data.
    """
    logger.info("Starting data ingestion and enrichment process...")
    # Update ticker data for S&P 500 and ETFs
    for sp500_ticker in get_sp500_tickers():
        update_ticker_data(sp500_ticker)
    for etf_ticker in get_etf_tickers():
        update_ticker_data(etf_ticker, path="etfs")
    update_macro_data(filepath="data/macros/macro_data.parquet")
    logger.info("Data ingestion and enrichment completed successfully.")
    

if __name__ == "__main__":
    main()
    logger.info("Script executed successfully.")