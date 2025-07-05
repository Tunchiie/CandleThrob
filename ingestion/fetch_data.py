import logging
import time
import re
import os
import time
import yfinance as yf
import pandas as pd
from fredapi import Fred
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
    def __init__(self, start_date=None, end_date=None):
        """ 
        Initialize the DataIngestion class.
        Args:
            start_date (date): The start date for fetching historical data. 
            Defaults to 50 years before the end date.
            end_date (date): The end date for fetching historical data. Defaults to today.
        Attributes:
            ticker_df (pd.DataFrame): DataFrame to store historical stock data for all tickers.
            macro_df (pd.DataFrame): DataFrame to store macroeconomic data.
        Raises:
            ValueError: If the start date is after the end date.
        """

        self.end_date = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d") if start_date else (datetime.now() - pd.DateOffset(years=50))
        self.ticker_df = None
        self.macro_df = None
        if self.start_date > self.end_date:
            raise ValueError("Start date cannot be after end date. Please check the dates provided.")
        logger.info("DataIngestion initialized with start date: %s and end date: %s", self.start_date, self.end_date)
    
    def fetch(self, ticker=None):
        """ 
        Fetch the list of tickers from the S&P 500 and ETFs, clean them, and store them in a list.
        This method downloads historical stock data for each ticker using yfinance, enriches the data with additional information,
        and fetches economic data from the FRED API.
        Returns:
            None: The method updates the ticker DataFrame in place with historical stock data and economic indicators.
        """
        logger.info("Starting data ingestion process...")
        logger.info("Fetching data from %s to %s", self.start_date, self.end_date)
        self.ingest_ticker_data(ticker)
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

    def ingest_tickers(self, tickers=None):
        """ 
        Fetch the list of tickers from the S&P 500 and ETFs, clean them, and store them in a list.
        Returns:
            None: The method updates the ticker DataFrame in place with historical stock data for all tickers.
        """
        
        stock_data = []
        # Download stock data from yfinance
        tq = tqdm(tickers, desc="Downloading")
        for _, ticker in enumerate(tq):
            ticker_df = self.ingest_ticker_data(ticker)
            if ticker_df is not None and not ticker_df.empty:
                stock_data.append(ticker_df)
            time.sleep(1.5)
                
        if stock_data:
            stock_data = [df for df in stock_data if df is not None and not df.empty]
            self.ticker_df = pd.concat(stock_data, ignore_index=True)
            self.ticker_df.sort_index(inplace=True)
            self.ticker_df['Date'] = pd.to_datetime(self.ticker_df['Date'])
            self.ticker_df = self.ticker_df.copy()
            logger.info("Fetched data for %d tickers.", len(self.ticker_df['Ticker'].unique()))
        else:
            logger.error("No stock data was fetched for %s. Please check your internet connection or ticker symbols.", tickers)
            return None

    def fetch_fred_data(self):
        """ 
        Fetch economic data from the Federal Reserve Economic Data (FRED) API.
        This method retrieves various economic indicators such as GDP, unemployment rate, inflation rate, and interest rates.
        It stores the data in a DataFrame and merges it with the ticker DataFrame based on the date.
        Returns:
            None: The method updates the ticker DataFrame in place with additional columns for economic indicators.
        Raises:
            Exception: If the FRED API key is not set in the environment variables.
        """
        
        if not os.getenv("FRED_API_KEY"):
            raise RuntimeError("FRED API key is not set. Please set the FRED_API_KEY environment variable.")

        fred_api = Fred(os.getenv("FRED_API_KEY"))
        fred_series = {
            "FEDFUNDS": "Federal_Funds_Rate",
            "CPIAUCSL": "Consumer_Price_Index",
            "UNRATE": "Unemployment_Rate",
            "GDP": "Gross_Domestic_Product",
            "GS10": "10-Year_Treasury_Rate",
            "USREC": "Recession_Indicator",
            "UMCSENT": "Consumer_Sentiment",
            "HOUST": "Housing_Starts",
            "RSAFS": "Retail_Sales",
            "INDPRO": "Industrial_Production_Index",
            "M2SL": "M2_Money_Supply",
        }

        fred_data = {}
        for code, name in fred_series.items():
            try:
                logger.info("Fetching FRED data for %s (%s) from %s to %s", name, code, self.start_date, self.end_date)
                fred_data[code] = fred_api.get_series(code, start_date=self.start_date.strftime("%Y-%m-%d"), end_date=self.end_date.strftime("%Y-%m-%d"))
            except Exception as e:
                logger.error("Error fetching FRED data for %s (%s): %s", name, code, e)
        fred_df = pd.DataFrame(fred_data)
        if fred_df.empty:
            raise ValueError("No data returned from FRED API. Check your API key and internet connection.")

        fred_df.index = pd.to_datetime(fred_df.index)
        fred_df = fred_df.resample('D').mean().ffill().bfill()
        fred_df.reset_index(names='Date', inplace=True)
        fred_df['Date'] = fred_df['Date'].dt.normalize()

        if self.macro_df is not None and not self.macro_df.empty:
            self.macro_df = self.macro_df.merge(fred_df, on='Date', how='left')
        else:
            self.macro_df = fred_df        
        logger.info("FRED data fetched and stored successfully.")
