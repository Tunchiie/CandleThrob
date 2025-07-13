import sys
import os
import re
import pandas as pd
import logging
import time
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from CandleThrob.ingestion.fetch_data import DataIngestion
from CandleThrob.utils.models import TickerData, MacroData
from CandleThrob.utils.oracle_conn import OracleDB


logging.basicConfig(
    filename="apps/logs/ingest_debug.log", 
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DB = OracleDB()
ENGINE = DB.get_sqlalchemy_engine()

def update_ticker_data(ticker: str):
    """
    Update the ticker data by fetching new data from Polygon.io and storing directly to Oracle DB.
    Uses incremental loading - only fetches data after the last date in the database.

    Args:
        ticker (str): The stock ticker symbol.
    """
    logger.info("Fetching ticker data for %s using Polygon.io", ticker)
    
    with DB.establish_connection() as conn:
        with conn.cursor() as cursor:
            TickerData().create_table(cursor)
        
        # Check if data exists and get the last date
        ticker_model = TickerData()
        if ticker_model.data_exists(conn, ticker):
            last_date = ticker_model.get_last_date(conn, ticker)
            if last_date:
                # Start from the day after the last date
                from datetime import timedelta
                start = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
                logger.info("Incremental update for %s: last date in DB is %s, fetching from %s", 
                           ticker, last_date, start)
            else:
                start = "2000-01-01"
                logger.info("No valid date found for %s, fetching from %s", ticker, start)
        else:
            start = "2000-01-01"
            logger.info("No existing data for %s, fetching from %s", ticker, start)
    
        end = datetime.now().strftime("%Y-%m-%d")
        
        # Skip if start date is today or later (no new data to fetch)
        if start >= end:
            logger.info("No new data to fetch for %s (last date: %s)", ticker, start)
            return

        logger.info("Updating ticker data for %s from %s to %s using Polygon.io", ticker, start, end)
        data = DataIngestion(start_date=start, end_date=end)
        ticker_df = data.ingest_ticker_data(ticker)
        
        if ticker_df is None or ticker_df.empty:
            logger.warning("No new data was fetched for %s from %s to %s", ticker, start, end)
            return
        
        logger.info("Saving %d records for %s to Oracle DB using bulk insert", len(ticker_df), ticker)
        ticker_model.insert_data(ENGINE, ticker_df)
        logger.info("Successfully saved %d records for %s", len(ticker_df), ticker)


def update_macro_data():
    """
    Update the macroeconomic data by fetching new data from the FRED API and storing to Oracle DB.
    Uses incremental loading - only fetches data after the last date in the database.
    """
    logger.info("Updating macroeconomic data...")
    
    with DB.establish_connection() as conn:
        with conn.cursor() as cursor:
            MacroData().create_table(cursor)
        
        # Check if macro data exists and get the last date
        macro_model = MacroData()
        if macro_model.data_exists(conn):
            last_date = macro_model.get_last_date(conn)
            if last_date:
                # Start from the day after the last date
                from datetime import timedelta
                start_date = last_date + timedelta(days=1)
                logger.info("Incremental update for macro data: last date in DB is %s, fetching from %s", 
                           last_date, start_date)
            else:
                start_date = None
                logger.info("No valid date found for macro data, fetching all available data")
        else:
            start_date = None
            logger.info("No existing macro data, fetching all available data")
        
        # If we have a start date, use it; otherwise use default range
        if start_date:
            end_date = datetime.now()
            # Skip if start date is today or later (no new data to fetch)
            if start_date.date() >= end_date.date():
                logger.info("No new macro data to fetch (last date: %s)", start_date.date())
                return
            data = DataIngestion(start_date=start_date.strftime("%Y-%m-%d"), end_date=end_date.strftime("%Y-%m-%d"))
        else:
            data = DataIngestion()
        
        data.fetch_fred_data()

        if data.macro_df is None or data.macro_df.empty:
            logger.warning("No new macroeconomic data was fetched.")
            return
        
        logger.info("Saving %d macro records to Oracle DB using bulk insert", len(data.macro_df))
        macro_model.insert_data(conn, data.macro_df)
        logger.info("Successfully saved %d macro records", len(data.macro_df))

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

def test_ingest_data():
    """
    Test the data ingestion process by querying the ticker data from the Oracle database.
    This function will print statistics about the data to verify that the ingestion process
    has successfully updated the database.
    """
    from sqlalchemy import func
    logger.info("Testing data ingestion by querying data from Oracle DB...")
    
    with DB.establish_connection() as conn:
        # Check ticker data
        ticker_model = TickerData()
        if ticker_model.data_exists(conn):
            last_date = ticker_model.get_last_date(conn)
            
            # Count total records
            ticker_count = conn.query(func.count(TickerData.id)).scalar()
            ticker_symbol_count = conn.query(func.count(func.distinct(TickerData.ticker))).scalar()
            
            logger.info("Ticker data - Total records: %d, Unique tickers: %d, Last date: %s", 
                       ticker_count, ticker_symbol_count, last_date)
        else:
            logger.info("No ticker data found in database")
        
        # Check macro data
        macro_model = MacroData()
        if macro_model.data_exists(conn):
            last_date = macro_model.get_last_date(conn)
            
            # Count total records
            macro_count = conn.query(func.count(MacroData.id)).scalar()
            indicator_count = conn.query(func.count(func.distinct(MacroData.series_id))).scalar()
            
            logger.info("Macro data - Total records: %d, Unique indicators: %d, Last date: %s", 
                       macro_count, indicator_count, last_date)
        else:
            logger.info("No macro data found in database")
    


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
        update_macro_data()
        return
    
    batch_tickers = all_tickers[start_idx:end_idx]
    logger.info("Processing batch %d: tickers %d-%d (%d tickers)", 
                batch_num, start_idx, end_idx-1, len(batch_tickers))
    
    for i, ticker in enumerate(batch_tickers):
        logger.info("Processing ticker %d/%d: %s", i+1, len(batch_tickers), ticker)
        
        if ticker in sp500_tickers:
            update_ticker_data(ticker)
        else:
            update_ticker_data(ticker)
        
        # Rate limiting for Polygon.io (12 seconds between requests)
        if i < len(batch_tickers) - 1:  
            logger.info("Rate limiting: sleeping 12 seconds...")
            time.sleep(12)
    
    total_batches = (len(all_tickers) + batch_size - 1) // batch_size  # Ceiling division
    if batch_num == 0 or batch_num == total_batches - 1:
        logger.info("Updating macro data (batch %d of %d)", batch_num, total_batches-1)
        update_macro_data()
    
    logger.info("Batch %d completed successfully. Processed %d tickers.", batch_num, len(batch_tickers))
    test_ingest_data()
    logger.info("Oracle DB conn will close automatically via context manager.")
    
    

if __name__ == "__main__":
    main()
    print("Data ingestion process completed successfully.")
    logger.info("Script executed successfully.")