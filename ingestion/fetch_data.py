import logging
import os
import re
import time
import yfinance as yf
import pandas as pd
from datetime import datetime
from tqdm import tqdm

logging.basicConfig(
    filename="debug.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class DataIngestion:
    """
    Class for ingesting financial data from various sources.
    """
    def __init__(self, tickers=None, start_date=None, end_date=None):
        """ 
        Initialize the DataIngestion class.
        Args:
            start_date (date): The start date for fetching historical data. 
            Defaults to 50 years before the end date.
            end_date (date): The end date for fetching historical data. Defaults to today.
        Attributes:
            sp50_tickers (list): List of S&P 500 ticker symbols.
            etf_tickers (list): List of ETF ticker symbols.
            all_tickers (list): Combined list of S&P 500 and ETF ticker symbols.
            ticker_df (pd.DataFrame): DataFrame to store historical stock data for all tickers.
            macro_df (pd.DataFrame): DataFrame to store macroeconomic data.
        Raises:
            ValueError: If the start date is after the end date.
        """

        self.all_tickers = [self.clean_ticker(ticker) for ticker in tickers]
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d") if start_date else (datetime.now() - pd.DateOffset(years=50))
        self.ticker_df = None
        self.macro_df = None
        if self.start_date > self.end_date:
            raise ValueError("Start date cannot be after end date. Please check the dates provided.")
        logger.info("DataIngestion initialized with start date: %s and end date: %s", self.start_date, self.end_date)
    
    def fetch(self):
        """ 
        Fetch the list of tickers from the S&P 500 and ETFs, clean them, and store them in a list.
        This method downloads historical stock data for each ticker using yfinance, enriches the data with additional information,
        and fetches economic data from the FRED API.
        Returns:
            None: The method updates the ticker DataFrame in place with historical stock data and economic indicators.
        """
        logger.info("Starting data ingestion process...")
        logger.info("Fetching data from %s to %s", self.start_date, self.end_date)
        self.ingest_tickers()
        self.enrich_data()
        logger.info("Data ingestion process completed.")
    
    @staticmethod
    def clean_ticker(ticker:str):
        """
        Clean the ticker symbol by removing unwanted characters and formatting it.
        Args:
            ticker (str): The ticker symbol to clean.
        Returns:
            str: The cleaned ticker symbol."""
        return re.sub(r"[^A-Z0-9\-]", "-", ticker.strip().upper()).strip("-")
    
    def ingest_ticker_data(self, ticker) -> pd.DataFrame:
        """ 
        Ingest historical stock data for a given ticker using yfinance.
        Args:
            ticker (str): The ticker symbol to fetch data for.
        Returns:
            pd.DataFrame: A DataFrame containing historical stock data for the ticker.
        """
        logger.info("Fetching data for %s from %s to %s", ticker, self.start_date, self.end_date)
        retries = 3
        if not ticker or ticker.strip() == "":
            logger.warning("Ticker is empty. Skipping.")
            return None
        
        while retries > 0:
            try:
                print(f"Fetching {ticker} from {self.start_date} to {self.end_date}")
                print(f"As strings: {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}")

                price_history = yf.download(ticker, start=self.start_date.strftime("%Y-%m-%d"), end=self.end_date.strftime("%Y-%m-%d"), group_by='Ticker', auto_adjust=True)
                if price_history.empty:
                    logger.warning("No data found for %s. Skipping.", ticker)
                    return None
                
                price_history = price_history.stack(level="Ticker", future_stack=True).reset_index(level="Ticker")
                price_history['Ticker'] = ticker
                price_history['Year'] = price_history.index.year
                price_history['Month'] = price_history.index.month
                price_history['Weekday'] = price_history.index.weekday
                price_history.columns = [col for col in price_history.columns]
                price_history.columns.name = None
                price_history.reset_index(inplace=True)
                logger.info("Successfully fetched data for %s.", ticker)
                return price_history
            except Exception as e:
                logger.error("Error fetching data for %s: %s", ticker, e)
                time.sleep(5 * retries)  # Wait before retrying
                retries -= 1
        logger.error("Failed to fetch data for %s after multiple retries.", ticker)
        return None

    def ingest_tickers(self):
        """ 
        Fetch the list of tickers from the S&P 500 and ETFs, clean them, and store them in a list.
        Returns:
            None: The method updates the ticker DataFrame in place with historical stock data for all tickers.
        Raises:
            ValueError: If no stock data was fetched.
        """
        
        stock_data = []
        # Download stock data from yfinance
        tq = tqdm(self.all_tickers, desc="Downloading")
        for _, ticker in enumerate(tq):
            ticker_df = self.ingest_ticker_data(ticker)
            if ticker_df is not None and not ticker_df.empty:
                stock_data.append(ticker_df)
                
        if stock_data:
            stock_data = [df for df in stock_data if df is not None and not df.empty]
            self.ticker_df = pd.concat(stock_data, ignore_index=True)
            self.ticker_df.sort_index(inplace=True)
            self.ticker_df['Date'] = pd.to_datetime(self.ticker_df['Date'])
            self.ticker_df = self.ticker_df.copy()
            logger.info("Fetched data for %d tickers.", len(self.ticker_df['Ticker'].unique()))
        else:
            logger.error("No stock data was fetched. Please check your internet connection or ticker symbols.")
            raise ValueError("No stock data was fetched. Please check your internet connection or ticker symbols.")
        
    def enrich_data(self):
        """ 
        Enrich the ticker data with additional information such as market cap, sector, and industry.
        This method fetches data from yfinance and updates the ticker DataFrame with market cap, sector, and industry information.
        It also calculates returns for various periods (1, 3, 7, 30, 90, and 365 days).
        Returns:
            None: The method updates the ticker DataFrame in place with additional columns for market cap,
        """
        # Fetch market cap, sector, and industry data from yfinance
        logger.info("Enriching data with market cap, sector, and industry information...")
        ticker_info = yf.Tickers(self.all_tickers).tickers
        metadata = []
        
        for ticker in tqdm(ticker_info, desc="Enriching Data"):
            info = ticker_info[ticker].info
            metadata.append({
                "Ticker": ticker,
                "Market Cap": info.get("marketCap", None),
                "Sector": info.get("sector", None),
                "Industry": info.get("industry", None)
            })
            
        metadata_df = pd.DataFrame(metadata)
        if metadata_df.empty:
            logger.error("No metadata was fetched. Please check your internet connection or ticker symbols.")
            raise ValueError("No metadata was fetched. Please check your internet connection or ticker symbols.")

        if self.ticker_df is None:
            raise ValueError("Ticker DataFrame is empty. Please run ingest_tickers() first.")
        
        self.ticker_df = self.ticker_df.merge(metadata_df, on="Ticker", how="left")
        self.ticker_df["Market Cap"] = self.ticker_df["Market Cap"].fillna(0).astype(float)
        self.ticker_df["Sector"] = self.ticker_df["Sector"].fillna("Unknown")
        self.ticker_df["Industry"] = self.ticker_df["Industry"].fillna("Unknown")

        for i in [1,3,7,30,90,365]:
            self.ticker_df[f"Return_{i}d"] = self.ticker_df.groupby("Ticker")["Close"]\
                .pct_change(periods=i).fillna(0)
        self.ticker_df["Date"] = pd.to_datetime(self.ticker_df["Date"])
