import logging
import time
import re
import pandas as pd
from fredapi import Fred
from datetime import datetime, timedelta
from tqdm import tqdm
from CandleThrob.utils.vault import get_secret
from polygon import RESTClient


logging.basicConfig(
    filename="ingest_debug.log", 
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
        Ingest historical stock data for a given ticker using Polygon.io API only.
        Args:
            ticker (str): The ticker symbol to fetch data for.
        Returns:
            pd.DataFrame: The DataFrame containing OHLCV data.
        """
        logger.info("Fetching Polygon.io data for %s from %s to %s", ticker, self.start_date, self.end_date)
        
        if not ticker or ticker.strip() == "":
            logger.warning("Ticker is empty. Skipping.")
            return pd.DataFrame()
        
        # Use Polygon.io as the sole data source for consistency and quality
        price_history = self._fetch_polygon_data(ticker)
        
        if price_history is None or price_history.empty:
            logger.error("Failed to fetch data for %s from Polygon.io", ticker)
            return pd.DataFrame()
            
        return price_history

    def _fetch_polygon_data(self, ticker):
        """
        Fetch OHLCV data using Polygon.io API.
        Args:
            ticker (str): The ticker symbol to fetch data for.
        Returns:
            pd.DataFrame: A DataFrame containing historical OHLCV data for the ticker.
        Raises:
            requests.RequestException: If there is an issue with the API request.
            KeyError: If the expected data structure is not found in the response.
            ValueError: If the data cannot be parsed correctly.
            TypeError: If there is a type mismatch in the data. 
        """
        try:
            api_key = get_secret("ocid1.vaultsecret.oc1.iad.amaaaaaacwmkemyaca7fcwmhm3y6pjqoe5arj47tcvpvidiniodv2p7d2xcq")
            if not api_key:
                logger.error("Polygon.io API key not found. Please set POLYGON_API_KEY environment variable.")
                return pd.DataFrame()
            
            # Format dates for Polygon API
            from_date = self.start_date.strftime("%Y-%m-%d")
            to_date = self.end_date.strftime("%Y-%m-%d")
            
            # Polygon.io aggregates endpoint
            client = RESTClient(api_key)

            two_years = datetime.utcnow() - timedelta(days=365 * 2)
            if self.start_date < two_years:
                logger.info("Fetching Polygon.io OHLCV data for %s", ticker)
                trades = client.get_aggs(ticker,multiplier=1,timespan="day", from_=from_date, to=to_date, adjusted=True)
            else:
                logger.info("Fetching Polygon.io OHLCV data for %s with 1 minute granularity", ticker)
                trades = client.get_aggs(ticker, multiplier=1, timespan="minute", from_=from_date, to=to_date, adjusted=True)
            data = pd.DataFrame(trades)
            data["ticker"] = ticker
            data["trade_date"] = pd.to_datetime(data["timestamp"], unit='ms')
            return data
    
        except (KeyError, ValueError, TypeError) as e:
            logger.error("Polygon.io data parsing error for %s: %s", ticker, str(e))
        except Exception as e:
            logger.error("Unexpected error fetching Polygon.io data for %s: %s", ticker, str(e))
        
        return pd.DataFrame()

    def ingest_tickers(self, tickers=None):
        """ 
        Fetch historical OHLCV stock data for a list of tickers using Polygon.io API only.
        Args:
            tickers (list): A list of ticker symbols to fetch data for.
        Returns:
            None: The method updates the ticker DataFrame in place with historical stock data for all tickers.
        Raises:
            ValueError: If no tickers are provided.
        """
        
        if not tickers:
            logger.error("No tickers provided")
            return None
            
        stock_data = []
        
        tq = tqdm(tickers, desc="Downloading OHLCV from Polygon.io")
        
        for i, ticker in enumerate(tq):
            tq.set_postfix({"ticker": ticker})
            
            ticker_df = self.ingest_ticker_data(ticker)
            
            if ticker_df is not None and not ticker_df.empty:
                stock_data.append(ticker_df)
            
            # Rate limiting: 5 calls per minute = 12 seconds between calls
            if i < len(tickers) - 1:
                rate_limit_delay = 12  # seconds between Polygon API calls
                logger.info("Rate limiting: waiting %d seconds after Polygon.io call", rate_limit_delay)
                time.sleep(rate_limit_delay)
                
        if stock_data:
            stock_data = [df for df in stock_data if df is not None and not df.empty]
            self.ticker_df = pd.concat(stock_data, ignore_index=True)
            self.ticker_df.sort_index(inplace=True)
            self.ticker_df['date'] = pd.to_datetime(self.ticker_df['date'])
            self.ticker_df = self.ticker_df.copy()
            logger.info("Fetched data for %d tickers using Polygon.io API.",
                       len(self.ticker_df['ticker'].unique()))
        else:
            logger.error("No stock data was fetched for %s. Please check your internet connection or Polygon.io API key.", tickers)
            return None

    def fetch(self, tickers=None):
        """
        Backward compatibility method for ingest_tickers.
        
        Args:
            tickers (list): A list of ticker symbols to fetch data for.
        """
        return self.ingest_tickers(tickers)

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
        
        if not get_secret("ocid1.vaultsecret.oc1.iad.amaaaaaacwmkemyawwc75zpuccl4qzs5qewxaauzwjfpxmsjmhu5umoij2oa"):
            raise RuntimeError("FRED API key is not set. Please set the FRED_API_KEY environment variable.")

        fred_api = Fred(get_secret("ocid1.vaultsecret.oc1.iad.amaaaaaacwmkemyawwc75zpuccl4qzs5qewxaauzwjfpxmsjmhu5umoij2oa"))
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
        fred_df.reset_index(names='date', inplace=True)
        fred_df['trade_date'] = fred_df['date'].dt.normalize()
        
        # Transform to long format for MacroData model
        melted_df = pd.melt(fred_df, id_vars=['trade_date'], var_name='series_id', value_name='value')
        melted_df = melted_df.dropna(subset=['value'])  # Remove rows with NaN values
        
        # Don't add year, month, weekday - they're not in the current models

        if self.macro_df is not None and not self.macro_df.empty:
            self.macro_df = pd.concat([self.macro_df, melted_df], ignore_index=True)
        else:
            self.macro_df = melted_df        
        logger.info("FRED data fetched and stored successfully.")
