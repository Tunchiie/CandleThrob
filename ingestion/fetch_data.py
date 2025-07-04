import yfinance as yf
import pandas as pd
from fredapi import Fred
import re
from datetime import date
from tqdm import tqdm
import os
import time
import logging

logging.basicConfig(
    filename="debug.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class DataIngestion:

    def __init__(self, tickers=None, macro=None, start_date=None, end_date=None):
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
        
        self.all_tickers = tickers
        self.end_date = end_date or date.today()
        self.start_date = start_date or date(year=self.end_date.year-50, month=self.end_date.month, day=self.end_date.day)
        self.ticker_df = None
        self.macro_df = None
        if self.start_date > self.end_date:
            raise ValueError("Start date cannot be after end date. Please check the dates provided.")
        
    # def get_sp500_tickers(self):
    #     """ 
    #     Get the list of S&P 500 tickers.
    #     Returns:
    #         list: A list of S&P 500 ticker symbols.
    #     """
    #     tickers = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]["Symbol"].tolist()[:5]
    #     return [self.clean_ticker(ticker) for ticker in tickers]
    
    # def get_etf_tickers(self):
    #     """ 
    #     Get the list of ETF tickers.
    #     Returns:
    #         list: A list of ETF ticker symbols.
    #     """
    #     etf_tickers = ['SPY', 'QQQ', 'DIA', 'IWM', 'VTI', 'TLT', 'GLD',
    #                    'XLF', 'XLE', 'XLK', 'XLV', 'ARKK', 'VXX',
    #                    'TQQQ', 'SQQQ', 'SOXL', 'XLI', 'XLY', 'XLP', 'SLV']
    #     return [self.clean_ticker(ticker) for ticker in etf_tickers]
    
    def fetch(self):
        """ 
        Fetch the list of tickers from the S&P 500 and ETFs, clean them, and store them in a list.
        This method downloads historical stock data for each ticker using yfinance, enriches the data with additional information,
        and fetches economic data from the FRED API.
        Returns:
            None: The method updates the ticker DataFrame in place with historical stock data and economic indicators.
        """
        logger.info("Starting data ingestion process...")
        logger.info(f"Fetching data from %s to %s", self.start_date, self.end_date)
        self.ingest_tickers()
        self.enrich_data()
        self.ingest_fred_data()
        logger.info("Data ingestion process completed.")
        self.save_data()
    
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
        logger.info(f"Fetching data for %s from %s to %s", ticker, self.start_date, self.end_date)
        retries = 3
        if not ticker or ticker.strip() == "":
            logger.warning("Ticker is empty. Skipping.")
            return None
        
        while retries > 0:
            try:
                price_history = yf.download(ticker, start=self.start_date, end=self.end_date, group_by='Ticker', auto_adjust=True)
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
            self.ticker_df.reset_index(names=["Date"], inplace=True)
            self.ticker_df['Date'] = pd.to_datetime(self.ticker_df['Date']).dt.normalize()
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
        self.ticker_df["Date"] = pd.to_datetime(self.ticker_df["Date"]).dt.normalize()

    def ingest_fred_data(self):
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
            "FEDFUNDS": "Federal Funds Rate",
            "CPIAUCSL": "Consumer Price Index",
            "UNRATE": "Unemployment Rate",
            "GDP": "Gross Domestic Product",
            "GS10": "10-Year Treasury Rate",
            "USREC": "Recession Indicator",
            "UMCSENT": "Consumer Sentiment",
            "HOUST": "Housing Starts",
            "RSAFS": "Retail Sales",
            "INDPRO": "Industrial Production Index",
            "M2SL": "M2 Money Supply",
        }

        fred_data = {}
        for code, name in fred_series.items():
            try:
                logger.info(f"Fetching FRED data for {name} ({code}) from {self.start_date} to {self.end_date}")
                fred_data[name] = fred_api.get_series(code, start_date=self.start_date, end_date=self.end_date)
            except Exception as e:
                logger.error(f"Error fetching FRED data for {name} ({code}): {e}")

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
            
        self.macro_df.to_csv("storage/macro_data.csv", index=False)
        
        logger.info("FRED data fetched and stored successfully.")

    
    def save_data(self):
        """ 
        Save the ticker DataFrame to a parquet file.
        Returns:
            None: The method saves the ticker DataFrame to a parquet file at the specified path.
        """
        
        if self.ticker_df is None or self.ticker_df.empty:
            raise ValueError("Ticker DataFrame is empty. Please run fetch() first.")
        if not os.path.exists("./storage"):
            os.makedirs("./storage")
        logger.info("Saving data to ./storage/ticker_data.parquet")
        self.ticker_df.to_parquet("./storage/ticker_data.parquet")
        print("Data saved to ./storage/ticker_data.parquet")
