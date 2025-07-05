import sys
import os
import re
import pandas as pd
import logging
import time
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ingestion.fetch_data import DataIngestion
from utils.gcs import upload_to_gcs, load_from_gcs, blob_exists


logging.basicConfig(
    filename="ingestion/debug.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def update_ticker_data(ticker: str, path: str="raw/tickers"):
    """
    Update the ticker data by fetching new data from Polygon.io.

    Args:
        ticker (str): The stock ticker symbol.
        path (str): The directory path where the ticker data is stored.
    """
    filepath = f"{path}/{ticker}.parquet"
    df = None
    if blob_exists(bucket_name="candlethrob-candata", blob_name=f"{path}/{ticker}.parquet"):
        logger.info("Loading existing ticker data from %s", filepath)
        df = load_from_gcs(
            bucket_name="candlethrob-candata",
            source_blob_name=f"{path}/{ticker}.parquet",
        )
    
    if df is None or df.empty:
        logger.warning("No existing data found for %s, fetching new data.", ticker)
        start = "2000-01-01"
        end = datetime.now().strftime("%Y-%m-%d")
    else:
        # Get the last date in the existing data to determine the start date for new data
        start = df["Date"].max().strftime("%Y-%m-%d")
        end = datetime.now().strftime("%Y-%m-%d")

    logger.info("Updating ticker data for %s from %s to %s using Polygon.io", ticker, start, end)
    data = DataIngestion(start_date=start, end_date=end)
    ticker_df = data.ingest_ticker_data(ticker)
    
    if df is not None and not df.empty and ticker_df is not None and not ticker_df.empty:
        ticker_df = pd.concat([df, ticker_df], ignore_index=True)
    else:
        logger.info("No existing data found for %s, using new data only.", ticker)
        if ticker_df is None or ticker_df.empty:
            logger.warning("No data was fetched for %s. Please check the ticker symbol or Polygon.io API.", ticker)
            return
    
    logger.info("Saving ticker data to %s", filepath)
    upload_to_gcs(
        data=ticker_df,
        bucket_name="candlethrob-candata",
        destination_blob_name=f"{path}/{ticker}.parquet",
    )
    
def update_macro_data(filepath: str="raw/macros/macro_data.parquet"):
    """
    Update the macroeconomic data by fetching new data from the FRED API.
    This function will fetch the latest macroeconomic indicators and save them to a CSV file.
    """
    logger.info("Updating macroeconomic data...")
    data = DataIngestion()
    data.fetch_fred_data()
    logger.info("Saving transformed macroeconomic data to %s", filepath)

    if data.macro_df is None or data.macro_df.empty:
        logger.warning("No macroeconomic data was fetched. Please check your internet connection or the FRED API.")
        return
    else:
        logger.info("Saving raw macroeconomic data to %s", filepath)
    upload_to_gcs(
        data=data.macro_df,
        bucket_name="candlethrob-candata",
        destination_blob_name=filepath,
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
    Main function to run the data ingestion process using Polygon.io only.
    This function will update ticker data in batches starting at 9 PM EST.
    """
    
    logger.info("Starting Polygon.io-only data ingestion process...")
    
    batch_num = int(os.getenv("BATCH_NUMBER", "0"))
    batch_size = int(os.getenv("BATCH_SIZE", "25"))  # 25 tickers per batch
    
    # Get all tickers
    sp500_tickers = get_sp500_tickers()
    etf_tickers = get_etf_tickers()
    all_tickers = sp500_tickers + etf_tickers
    
    logger.info("Total tickers: %d, Batch size: %d, Batch number: %d", 
                len(all_tickers), batch_size, batch_num)
    
    # Calculate batch range
    start_idx = batch_num * batch_size
    end_idx = min((batch_num + 1) * batch_size, len(all_tickers))
    
    if start_idx >= len(all_tickers):
        logger.info("Batch %d is beyond available tickers. Only updating macro data.", batch_num)
        update_macro_data(filepath="raw/macros/macro_data.parquet")
        return
    
    batch_tickers = all_tickers[start_idx:end_idx]
    logger.info("Processing batch %d: tickers %d-%d (%d tickers)", 
                batch_num, start_idx, end_idx-1, len(batch_tickers))
    
    for i, ticker in enumerate(batch_tickers):
        logger.info("Processing ticker %d/%d: %s", i+1, len(batch_tickers), ticker)
        
        if ticker in sp500_tickers:
            update_ticker_data(ticker, path="raw/tickers")
        else:
            update_ticker_data(ticker, path="raw/etfs")
        
        # Rate limiting for Polygon.io (12 seconds between requests)
        if i < len(batch_tickers) - 1:  
            logger.info("Rate limiting: sleeping 12 seconds...")
            time.sleep(12)
    
    total_batches = (len(all_tickers) + batch_size - 1) // batch_size  # Ceiling division
    if batch_num == 0 or batch_num == total_batches - 1:
        logger.info("Updating macro data (batch %d of %d)", batch_num, total_batches-1)
        update_macro_data(filepath="raw/macros/macro_data.parquet")
    
    logger.info("Batch %d completed successfully. Processed %d tickers.", batch_num, len(batch_tickers))
    

if __name__ == "__main__":
    main()
    logger.info("Script executed successfully.")