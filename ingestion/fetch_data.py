import logging
import time
import re
import os
import requests
import pandas as pd
from fredapi import Fred
from datetime import datetime, timedelta
from tqdm import tqdm

logging.basicConfig(
    filename="ingestion/debug.log",
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
        Ingest historical stock data for a given ticker using Polygon.io API.
        Args:
            ticker (str): The ticker symbol to fetch data for.
        Returns:
            pd.DataFrame: A DataFrame containing historical stock data for the ticker.
        """
        logger.info("Fetching data for %s from %s to %s", ticker, self.start_date, self.end_date)
        
        if not ticker or ticker.strip() == "":
            logger.warning("Ticker is empty. Skipping.")
            return pd.DataFrame()
        
        # Use Polygon.io as primary data source
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
            api_key = os.getenv("POLYGON_API_KEY")
            if not api_key:
                logger.error("Polygon.io API key not found. Please set POLYGON_API_KEY environment variable.")
                return pd.DataFrame()
            
            # Format dates for Polygon API
            from_date = self.start_date.strftime("%Y-%m-%d")
            to_date = self.end_date.strftime("%Y-%m-%d")
            
            # Polygon.io aggregates endpoint
            url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{to_date}"
            params = {
                "adjusted": "true",
                "sort": "asc",
                "limit": 50000,  # Max limit for free tier
                "apikey": api_key
            }
            
            logger.info("Fetching Polygon.io OHLCV data for %s", ticker)
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data.get("status") != "OK":
                logger.warning("Polygon.io API error for %s: %s", ticker, data.get("message", "Unknown error"))
                return pd.DataFrame()
            
            if "results" not in data or not data["results"]:
                logger.warning("No Polygon.io data found for %s", ticker)
                return pd.DataFrame()
            
            # Convert to DataFrame
            df_data = []
            for bar in data["results"]:
                # Polygon returns timestamp in milliseconds
                date_obj = datetime.fromtimestamp(bar["t"] / 1000)
                
                df_data.append({
                    "Date": date_obj.date(),
                    "Open": float(bar["o"]),
                    "High": float(bar["h"]),
                    "Low": float(bar["l"]),
                    "Close": float(bar["c"]),
                    "Volume": int(bar["v"]),
                    "Ticker": ticker,
                    "Year": date_obj.year,
                    "Month": date_obj.month,
                    "Weekday": date_obj.weekday()
                })
            
            if df_data:
                df = pd.DataFrame(df_data)
                df['Date'] = pd.to_datetime(df['Date'])
                df.sort_values("Date", inplace=True)
                df.reset_index(drop=True, inplace=True)
                logger.info("Successfully fetched Polygon.io OHLCV data for %s (%d days)", ticker, len(df))
                return df
                
        except requests.RequestException as e:
            logger.error("Polygon.io request error for %s: %s", ticker, str(e))
        except (KeyError, ValueError, TypeError) as e:
            logger.error("Polygon.io data parsing error for %s: %s", ticker, str(e))
        except Exception as e:
            logger.error("Unexpected error fetching Polygon.io data for %s: %s", ticker, str(e))
        
        return pd.DataFrame()

    def ingest_tickers(self, tickers=None):
        """ 
        Fetch historical OHLCV stock data for a list of tickers using Polygon.io.
        Handles rate limiting (5 calls per minute for free tier).
        Args:
            tickers (list): A list of ticker symbols to fetch data for. If None,
        Returns:
            None: The method updates the ticker DataFrame in place with historical stock data for all tickers.
        Raises:
            ValueError: If no tickers are provided.
            RuntimeError: If the Polygon.io API key is not set in the environment variables.
        """
        
        if not tickers:
            logger.error("No tickers provided")
            return None
            
        stock_data = []
        # Rate limiting: 5 calls per minute = 12 seconds between calls
        rate_limit_delay = 12  # seconds between API calls
        
        tq = tqdm(tickers, desc="Downloading OHLCV from Polygon.io")
        for i, ticker in enumerate(tq):
            ticker_df = self.ingest_ticker_data(ticker)
            if ticker_df is not None and not ticker_df.empty:
                stock_data.append(ticker_df)
            
            # Rate limiting: wait between API calls (except for the last one)
            if i < len(tickers) - 1:
                logger.info("Rate limiting: waiting %d seconds before next API call", rate_limit_delay)
                time.sleep(rate_limit_delay)
                
        if stock_data:
            stock_data = [df for df in stock_data if df is not None and not df.empty]
            self.ticker_df = pd.concat(stock_data, ignore_index=True)
            self.ticker_df.sort_index(inplace=True)
            self.ticker_df['Date'] = pd.to_datetime(self.ticker_df['Date'])
            self.ticker_df = self.ticker_df.copy()
            logger.info("Fetched data for %d tickers.", len(self.ticker_df['Ticker'].unique()))
        else:
            logger.error("No stock data was fetched for %s. Please check your internet connection or API key.", tickers)
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
