"""
Data Ingestion Module for CandleThrob

This module provides comprehensive data ingestion capabilities for financial data
from multiple sources including Polygon.io (OHLCV data) and FRED (macroeconomic data).
It includes rate limiting, error handling, and data validation.

Features:
- Polygon.io API integration for OHLCV data
- FRED API integration for macroeconomic indicators
- Rate limiting and error handling
- Data validation and cleaning
- Incremental data loading support

Author: CandleThrob Team
Version: 2.0.0
Last Updated: 2025-07-13
"""

import time
import re
import pandas as pd
from fredapi import Fred
from datetime import datetime, timedelta
from tqdm import tqdm
from typing import Optional, List, Dict, Any, Union
from CandleThrob.utils.vault import get_secret
from CandleThrob.utils.logging_config import get_ingestion_logger, log_ingestion_start, log_ingestion_success, log_ingestion_error, log_rate_limiting
from polygon import RESTClient

# Get logger for this module
logger = get_ingestion_logger(__name__)

class DataIngestion:
    """
    Comprehensive data ingestion class for financial data from multiple sources.
    
    This class provides methods to ingest historical stock data from Polygon.io
    and macroeconomic data from FRED. It includes rate limiting, error handling,
    and data validation to ensure data quality and API compliance.
    
    Attributes:
        start_date (datetime): Start date for data fetching
        end_date (datetime): End date for data fetching
        ticker_df (Optional[pd.DataFrame]): DataFrame for stock data
        macro_df (Optional[pd.DataFrame]): DataFrame for macroeconomic data
    """
    
    def __init__(self, start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        Initialize the DataIngestion class.
        
        Args:
            start_date (Optional[str]): Start date in 'YYYY-MM-DD' format.
                Defaults to 50 years before end_date.
            end_date (Optional[str]): End date in 'YYYY-MM-DD' format.
                Defaults to current date.
                
        Raises:
            ValueError: If start_date is after end_date.
            
        Example:
            >>> ingestion = DataIngestion("2020-01-01", "2025-07-13")
            >>> ingestion = DataIngestion()  # Uses defaults
        """
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d") if start_date else (datetime.now() - pd.DateOffset(years=50))
        self.ticker_df: Optional[pd.DataFrame] = None
        self.macro_df: Optional[pd.DataFrame] = None
        
        if self.start_date > self.end_date:
            raise ValueError("Start date cannot be after end date. Please check the dates provided.")
            
        logger.info("DataIngestion initialized with start date: %s and end date: %s", 
                   self.start_date, self.end_date)
    
    @staticmethod
    def clean_ticker(ticker: str) -> str:
        """
        Clean ticker symbol by removing unwanted characters and standardizing format.
        
        This method removes special characters, converts to uppercase, and ensures
        consistent ticker format for API calls and database storage.
        
        Args:
            ticker (str): The ticker symbol to clean
            
        Returns:
            str: The cleaned ticker symbol
            
        Example:
            >>> DataIngestion.clean_ticker("AAPL.O")
            'AAPL'
            >>> DataIngestion.clean_ticker("msft")
            'MSFT'
            >>> DataIngestion.clean_ticker("BRK-B")
            'BRK-B'
        """
        return re.sub(r"[^A-Z0-9\-]", "-", ticker.strip().upper()).strip("-")
    
    def ingest_ticker_data(self, ticker: str) -> pd.DataFrame:
        """
        Ingest historical stock data for a given ticker using Polygon.io API.
        
        This method fetches OHLCV (Open, High, Low, Close, Volume) data from
        Polygon.io with automatic rate limiting and error handling. It supports
        both daily and minute-level data based on the date range.
        
        Args:
            ticker (str): The ticker symbol to fetch data for
            
        Returns:
            pd.DataFrame: DataFrame containing OHLCV data with columns:
                - timestamp: Unix timestamp
                - open: Opening price
                - high: Highest price
                - low: Lowest price
                - close: Closing price
                - volume: Trading volume
                - vwap: Volume-weighted average price
                - transactions: Number of transactions
                - ticker: Ticker symbol
                - trade_date: Formatted date
                
        Example:
            >>> ingestion = DataIngestion("2024-01-01", "2024-12-31")
            >>> df = ingestion.ingest_ticker_data("AAPL")
            >>> print(df.head())
        """
        log_ingestion_start("fetch_data", ticker)
        
        if not ticker or ticker.strip() == "":
            logger.warning("Ticker is empty. Skipping.")
            return pd.DataFrame()
        
        # Use Polygon.io as the primary data source for consistency and quality
        price_history = self._fetch_polygon_data(ticker)
        
        if price_history is None or price_history.empty:
            log_ingestion_error("fetch_data", ticker, "No data fetched from Polygon.io")
            return pd.DataFrame()
        
        log_ingestion_success("fetch_data", ticker, len(price_history))
        return price_history

    def _fetch_polygon_data(self, ticker: str) -> pd.DataFrame:
        """
        Fetch OHLCV data using Polygon.io API with intelligent granularity selection.
        
        This method automatically selects the appropriate data granularity based on
        the date range to optimize API usage and data quality.
        
        Args:
            ticker (str): The ticker symbol to fetch data for
            
        Returns:
            pd.DataFrame: DataFrame containing historical OHLCV data
            
        Raises:
            requests.RequestException: If there is an issue with the API request
            KeyError: If the expected data structure is not found in the response
            ValueError: If the data cannot be parsed correctly
            TypeError: If there is a type mismatch in the data
        """
        try:
            api_key = get_secret("ocid1.vaultsecret.oc1.iad.amaaaaaacwmkemyaca7fcwmhm3y6pjqoe5arj47tcvpvidiniodv2p7d2xcq")
            if not api_key:
                log_ingestion_error("fetch_data", ticker, "Polygon.io API key not found")
                return pd.DataFrame()
            
            # Format dates for Polygon API
            from_date = self.start_date.strftime("%Y-%m-%d")
            to_date = self.end_date.strftime("%Y-%m-%d")
            
            # Initialize Polygon.io client
            client = RESTClient(api_key)

            # Intelligent granularity selection based on date range
            two_years = datetime.utcnow() - timedelta(days=365 * 2)
            if self.start_date < two_years:
                logger.info("Fetching Polygon.io daily OHLCV data for %s", ticker)
                trades = client.get_aggs(
                    ticker, 
                    multiplier=1, 
                    timespan="day", 
                    from_=from_date, 
                    to=to_date, 
                    adjusted=True
                )
            else:
                logger.info("Fetching Polygon.io minute-level OHLCV data for %s", ticker)
                trades = client.get_aggs(
                    ticker, 
                    multiplier=1, 
                    timespan="minute", 
                    from_=from_date, 
                    to=to_date, 
                    adjusted=True
                )
                
            # Convert to DataFrame and add metadata
            data = pd.DataFrame(trades)
            data["ticker"] = ticker
            data["trade_date"] = pd.to_datetime(data["timestamp"], unit='ms')
            
            return data
    
        except (KeyError, ValueError, TypeError) as e:
            log_ingestion_error("fetch_data", ticker, f"Data parsing error: {str(e)}")
        except Exception as e:
            log_ingestion_error("fetch_data", ticker, f"Unexpected error: {str(e)}")
        
        return pd.DataFrame()

    def ingest_tickers(self, tickers: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        Fetch historical OHLCV stock data for a list of tickers using Polygon.io API.
        
        This method processes multiple tickers with rate limiting and progress
        tracking. It aggregates all ticker data into a single DataFrame for
        efficient processing and storage.
        
        Args:
            tickers (Optional[List[str]]): List of ticker symbols to fetch data for.
                If None, returns None.
                
        Returns:
            Optional[pd.DataFrame]: DataFrame containing all ticker data, or None if failed
            
        Raises:
            ValueError: If no tickers are provided
            
        Example:
            >>> ingestion = DataIngestion("2024-01-01", "2024-12-31")
            >>> df = ingestion.ingest_tickers(["AAPL", "MSFT", "GOOGL"])
            >>> print(f"Fetched data for {len(df['ticker'].unique())} tickers")
        """
        if not tickers:
            logger.error("No tickers provided")
            return None
            
        stock_data = []
        
        # Progress bar for user feedback
        tq = tqdm(tickers, desc="Downloading OHLCV from Polygon.io")
        
        for i, ticker in enumerate(tq):
            tq.set_postfix({"ticker": ticker})
            
            ticker_df = self.ingest_ticker_data(ticker)
            
            if ticker_df is not None and not ticker_df.empty:
                stock_data.append(ticker_df)
            
            # Rate limiting: 5 calls per minute = 12 seconds between calls
            if i < len(tickers) - 1:
                rate_limit_delay = 12  # seconds between Polygon API calls
                log_rate_limiting("fetch_data", rate_limit_delay)
                time.sleep(rate_limit_delay)
                
        if stock_data:
            # Filter out empty DataFrames and concatenate
            stock_data = [df for df in stock_data if df is not None and not df.empty]
            self.ticker_df = pd.concat(stock_data, ignore_index=True)
            self.ticker_df.sort_index(inplace=True)
            self.ticker_df['date'] = pd.to_datetime(self.ticker_df['date'])
            self.ticker_df = self.ticker_df.copy()
            
            logger.info("Fetched data for %d tickers using Polygon.io API.",
                       len(self.ticker_df['ticker'].unique()))
            return self.ticker_df
        else:
            log_ingestion_error("fetch_data", None, "No stock data was fetched")
            return None

    def fetch(self, tickers: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        Backward compatibility method for ingest_tickers.
        
        This method provides backward compatibility for existing code that uses
        the 'fetch' method name instead of 'ingest_tickers'.
        
        Args:
            tickers (Optional[List[str]]): List of ticker symbols to fetch data for
            
        Returns:
            Optional[pd.DataFrame]: DataFrame containing all ticker data, or None if failed
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
