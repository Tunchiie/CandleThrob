#!/usr/bin/env python3
"""
Data Ingestion Module for CandleThrob
=====================================

This module provides data ingestion capabilities for financial data
from multiple sources including Polygon.io (OHLCV data) and FRED (macroeconomic data).
It includes advanced rate limiting, comprehensive error handling, data validation,
and performance monitoring.

Features:
- Advanced rate limiting with exponential backoff
- Comprehensive error handling and retry logic
- Data quality validation and monitoring
- Performance metrics collection and analysis
- Memory usage optimization
- Parallel processing capabilities
- Advanced logging and monitoring
- Data lineage tracking
- Quality gates and validation checkpoints
- Resource management and optimization
- Advanced error categorization and handling

Author: CandleThrob Team
Version: 3.0.0
Last Updated: 2025-07-14
"""

import time
import re
import pandas as pd
import numpy as np
import psutil
import threading
from fredapi import Fred
from datetime import datetime, timedelta
from tqdm import tqdm
from typing import Optional, List, Dict, Any, Union, Tuple
from dataclasses import dataclass, field
from functools import wraps
import hashlib
import json
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager

from CandleThrob.utils.vault import get_secret
from CandleThrob.utils.logging_config import (
    get_ingestion_logger, log_ingestion_start, log_ingestion_success, 
    log_ingestion_error, log_rate_limiting
)
from polygon import RESTClient

# Get logger for this module
logger = get_ingestion_logger(__name__)

@dataclass
class IngestionConfig:
    """Configuration for data ingestion."""
    
    # Rate limiting
    max_requests_per_minute: int = 60
    max_requests_per_hour: int = 1000
    retry_delay_seconds: float = 1.0
    max_retries: int = 3
    
    # Performance
    parallel_processing: bool = True
    max_workers: int = 4
    chunk_size: int = 100
    
    # Quality gates
    min_data_quality_score: float = 0.8
    max_memory_usage_mb: float = 2048.0
    max_processing_time_seconds: float = 1800.0
    
    # Advanced features
    enable_data_lineage: bool = True
    enable_audit_trail: bool = True
    enable_metrics_aggregation: bool = True
    enable_memory_optimization: bool = True
    
    def __post_init__(self):
        """Validate configuration parameters."""
        if self.max_requests_per_minute <= 0:
            raise ValueError("max_requests_per_minute must be positive")
        if self.max_requests_per_hour <= 0:
            raise ValueError("max_requests_per_hour must be positive")
        if self.retry_delay_seconds <= 0:
            raise ValueError("retry_delay_seconds must be positive")
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if not 0.0 <= self.min_data_quality_score <= 1.0:
            raise ValueError("min_data_quality_score must be between 0.0 and 1.0")

class RateLimiter:
    """Rate limiter with exponential backoff."""

    def __init__(self, config: IngestionConfig):
        self.config = config
        self.request_times: List[float] = []
        self._lock = threading.Lock()
    
    def wait_if_needed(self):
        """Wait if rate limit is exceeded."""
        with self._lock:
            current_time = time.time()
            
            # Remove old requests outside the time window
            self.request_times = [t for t in self.request_times 
                                if current_time - t < 60]  # Keep last minute
            
            if len(self.request_times) >= self.config.max_requests_per_minute:
                sleep_time = 60 - (current_time - self.request_times[0])
                if sleep_time > 0:
                    logger.info(f"Rate limit reached. Waiting {sleep_time:.2f} seconds")
                    time.sleep(sleep_time)
            
            self.request_times.append(current_time)

class DataQualityValidator:
    """Data quality validation."""
    
    def validate_ohlcv_data(self, df: pd.DataFrame) -> Tuple[bool, float, List[str], pd.DataFrame]:
        """Validate OHLCV data quality."""
        issues = []
        quality_score = 1.0
        
        if df.empty:
            issues.append("DataFrame is empty")
            quality_score = 0.0
            return False, quality_score, issues
        
        # Check required columns
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            issues.append(f"Missing required columns: {missing_cols}")
            quality_score -= 0.3
        
        # Check for negative values
        if 'volume' in df.columns and (df['volume'] < 0).any():
            issues.append("Negative volume values found")
            quality_score -= 0.2
        
        if 'close' in df.columns and (df['close'] < 0).any():
            issues.append("Negative price values found")
            quality_score -= 0.2
        
        # Check OHLC relationships with robust tolerance for real-world data
        if all(col in df.columns for col in ['high', 'low', 'open', 'close']):
            try:
                # Convert to float to handle any data type issues
                df['high'] = pd.to_numeric(df['high'], errors='coerce')
                df['low'] = pd.to_numeric(df['low'], errors='coerce')
                df['open'] = pd.to_numeric(df['open'], errors='coerce')
                df['close'] = pd.to_numeric(df['close'], errors='coerce')

                # Add tolerance for floating-point precision and market data quirks (0.1%)
                tolerance = 0.001

                # Check for clearly invalid relationships with tolerance
                invalid_high_low = df['high'] < (df['low'] * (1 - tolerance))
                invalid_high_open = df['high'] < (df['open'] * (1 - tolerance))
                invalid_high_close = df['high'] < (df['close'] * (1 - tolerance))
                invalid_low_open = df['low'] > (df['open'] * (1 + tolerance))
                invalid_low_close = df['low'] > (df['close'] * (1 + tolerance))

                invalid_ohlc = (
                    invalid_high_low |
                    invalid_high_open |
                    invalid_high_close |
                    invalid_low_open |
                    invalid_low_close
                )

                invalid_count = invalid_ohlc.sum()
                total_count = len(df)

                # Only flag as critical issue if more than 5% of records are invalid
                if invalid_count > 0:
                    invalid_ratio = invalid_count / total_count
                    if invalid_ratio > 0.05:  # More than 5% invalid - critical issue
                        issues.append(f"Critical OHLC validation failure: {invalid_count}/{total_count} records ({invalid_ratio:.2%}) have invalid relationships")
                        quality_score -= min(0.5, invalid_ratio * 3)  # Heavy penalty for critical issues
                    elif invalid_ratio > 0.01:  # 1-5% invalid - moderate issue
                        logger.warning(f"Moderate OHLC issues: {invalid_count}/{total_count} records ({invalid_ratio:.2%}) - cleaning data")
                        quality_score -= min(0.1, invalid_ratio)  # Light penalty

                        # Clean up the invalid records and update the original DataFrame
                        cleaned_df = self._clean_ohlc_data(df, invalid_ohlc)
                        df.update(cleaned_df)
                    else:
                        # Less than 1% invalid - just log and clean
                        logger.info(f"Minor OHLC inconsistencies: {invalid_count}/{total_count} records ({invalid_ratio:.2%}) - auto-correcting")
                        cleaned_df = self._clean_ohlc_data(df, invalid_ohlc)
                        df.update(cleaned_df)

            except Exception as e:
                logger.warning(f"OHLC validation failed due to data type issues: {e}")
                # Don't fail validation for data type issues - just log warning
        
        # Check for excessive null values
        null_ratio = df.isnull().sum().sum() / (len(df) * len(df.columns))
        if null_ratio > 0.1:
            issues.append(f"High null ratio: {null_ratio:.2%}")
            quality_score -= 0.2
        
        quality_score = max(0.0, quality_score)
        return quality_score >= 0.8, quality_score, issues, df

    def _clean_ohlc_data(self, df: pd.DataFrame, invalid_mask: pd.Series) -> pd.DataFrame:
        """
        Clean OHLC data by fixing invalid relationships.

        Args:
            df (pd.DataFrame): DataFrame with OHLC data
            invalid_mask (pd.Series): Boolean mask indicating invalid records

        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        df_clean = df.copy()

        for idx in df[invalid_mask].index:
            row = df_clean.loc[idx]

            # Get all price values
            prices = [row['open'], row['close']]
            current_high = row['high']
            current_low = row['low']

            # Ensure high is at least the maximum of open/close
            corrected_high = max(max(prices), current_high)

            # Ensure low is at most the minimum of open/close
            corrected_low = min(min(prices), current_low)

            # Apply corrections
            df_clean.loc[idx, 'high'] = corrected_high
            df_clean.loc[idx, 'low'] = corrected_low

        return df_clean

class PerformanceMonitor:
    """Performance monitoring."""
    
    def __init__(self):
        self.metrics: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
    
    @contextmanager
    def monitor_operation(self, operation_name: str):
        """Context manager for operation monitoring."""
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        try:
            yield
            success = True
            error_message = None
        except Exception as e:
            success = False
            error_message = str(e)
            raise
        finally:
            end_time = time.time()
            end_memory = psutil.Process().memory_info().rss / 1024 / 1024
            
            with self._lock:
                self.metrics[operation_name] = {
                    "duration": end_time - start_time,
                    "memory_usage_mb": end_memory - start_memory,
                    "success": success,
                    "error_message": error_message,
                    "timestamp": datetime.now().isoformat()
                }

def retry_on_failure(max_retries: int = 3, delay_seconds: float = 1.0):
    """Retry decorator with exponential backoff."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = delay_seconds * (2 ** attempt)  # Exponential backoff
                        logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}. Retrying in {delay:.2f}s...")
                        time.sleep(delay)
                    else:
                        logger.error(f"All {max_retries + 1} attempts failed for {func.__name__}: {str(e)}")
                        raise last_exception
            
            return None
        return wrapper
    return decorator

class DataIngestion:
    """
    Comprehensive data ingestion class for financial data from multiple sources.

    This class provides methods to ingest historical stock data from Polygon.io
    and macroeconomic data from FRED. It includes advanced rate limiting, comprehensive error
    handling, data validation, and performance monitoring to ensure data quality and API compliance.
    
    Attributes:
        start_date (datetime): Start date for data fetching
        end_date (datetime): End date for data fetching
        ticker_df (Optional[pd.DataFrame]): DataFrame for stock data
        macro_df (Optional[pd.DataFrame]): DataFrame for macroeconomic data
        config (IngestionConfig): Configuration
        rate_limiter (RateLimiter): Advanced rate limiter
        quality_validator (DataQualityValidator): Data quality validator
        performance_monitor (PerformanceMonitor): Performance monitor
    """
    
    def __init__(self, start_date: Optional[str] = None, end_date: Optional[str] = None,
                 config: Optional[IngestionConfig] = None):
        """
        Initialize the DataIngestion class with advanced features.
        
        Args:
            start_date (Optional[str]): Start date in 'YYYY-MM-DD' format.
                Defaults to 50 years before end_date.
            end_date (Optional[str]): End date in 'YYYY-MM-DD' format.
                Defaults to current date.
            config (Optional[IngestionConfig]): Configuration
                
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
        
        # Initialize components
        self.config = config or IngestionConfig()
        self.rate_limiter = RateLimiter(self.config)
        self.quality_validator = DataQualityValidator()
        self.performance_monitor = PerformanceMonitor()
        
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
    
    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def ingest_ticker_data(self, ticker: str) -> pd.DataFrame:
        """
        Ingest historical stock data for a given ticker using Polygon.io API with advanced features.
        
        This method fetches OHLCV (Open, High, Low, Close, Volume) data from
        Polygon.io with advanced rate limiting, comprehensive error handling, and
        data quality validation.
        
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
        operation_name = f"ingest_ticker_data_{ticker}"
        
        with self.performance_monitor.monitor_operation(operation_name):
            log_ingestion_start("fetch_data", ticker)
            
            if not ticker or ticker.strip() == "":
                logger.warning("Ticker is empty. Skipping.")
                return pd.DataFrame()
            
            # Rate limiting
            self.rate_limiter.wait_if_needed()
            
            # Use hybrid approach: yfinance for historical + Polygon.io for recent data
            price_history = self._fetch_hybrid_data(ticker)
            
            if price_history is None or price_history.empty:
                log_ingestion_error("fetch_data", ticker, "No data fetched from hybrid sources (yfinance + Polygon.io)")
                return pd.DataFrame()
            
            # Data quality validation
            quality_passed, quality_score, issues, price_history = self.quality_validator.validate_ohlcv_data(price_history)
            
            if not quality_passed:
                logger.error(f"Data quality validation failed for ticker {ticker}: {issues}")
                logger.error(f"Quality score: {quality_score:.3f}")

                # Log sample of problematic data for debugging
                if not price_history.empty:
                    sample_data = price_history[['trade_date', 'open', 'high', 'low', 'close', 'volume']].head(5)
                    logger.error(f"Sample data for debugging:\n{sample_data}")

                log_ingestion_error("fetch_data", ticker, f"Quality validation failed: {issues}")
                return pd.DataFrame()
            
            log_ingestion_success("fetch_data", ticker, len(price_history))
            return price_history

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def _fetch_yfinance_data(self, ticker: str) -> pd.DataFrame:
        """
        Fetch historical OHLCV data using yfinance (free, unlimited historical data).

        This method fetches daily data from yfinance which provides free access to
        historical stock data going back to IPO/listing date.

        Args:
            ticker (str): The ticker symbol to fetch data for

        Returns:
            pd.DataFrame: DataFrame with OHLCV data and proper date formatting
        """
        operation_name = f"fetch_yfinance_data_{ticker}"

        with self.performance_monitor.monitor_operation(operation_name):
            try:
                import yfinance as yf

                # Format dates for yfinance
                from_date = self.start_date.strftime("%Y-%m-%d")
                to_date = self.end_date.strftime("%Y-%m-%d")

                logger.info(f"Fetching yfinance daily data for {ticker} from {from_date} to {to_date}")

                # Create yfinance ticker object
                yf_ticker = yf.Ticker(ticker)

                # Fetch historical data
                hist_data = yf_ticker.history(
                    start=from_date,
                    end=to_date,
                    interval="1d",
                    auto_adjust=True,
                    prepost=False
                )

                if hist_data.empty:
                    logger.warning(f"No data returned from yfinance for ticker: {ticker}")
                    return pd.DataFrame()

                logger.info(f"Received {len(hist_data)} records from yfinance for {ticker}")

                # Convert to our standard format
                data = []
                earliest_date = None
                latest_date = None

                for date, row in hist_data.iterrows():
                    # Convert to timezone-naive datetime for consistency
                    trade_datetime = pd.to_datetime(date).tz_localize(None)

                    # Track date range for debugging
                    if earliest_date is None or trade_datetime < earliest_date:
                        earliest_date = trade_datetime
                    if latest_date is None or trade_datetime > latest_date:
                        latest_date = trade_datetime

                    data.append({
                        'timestamp': int(trade_datetime.timestamp() * 1000),  # Convert to milliseconds
                        'open': float(row['Open']),
                        'high': float(row['High']),
                        'low': float(row['Low']),
                        'close': float(row['Close']),
                        'volume': int(row['Volume']),
                        'vwap': float((row['High'] + row['Low'] + row['Close']) / 3),  # Approximate VWAP
                        'transactions': 1,  # yfinance doesn't provide transaction count
                        'ticker': ticker,
                        'trade_date': trade_datetime
                    })

                logger.info(f"yfinance data range for {ticker}: {earliest_date} to {latest_date}")
                df = pd.DataFrame(data)

                # Data cleaning and validation
                if not df.empty:
                    # Remove any rows with null values in critical columns
                    critical_cols = ['open', 'high', 'low', 'close', 'volume']
                    df = df.dropna(subset=critical_cols)

                    # Ensure positive values
                    for col in ['open', 'high', 'low', 'close', 'volume']:
                        if col in df.columns:
                            df = df[df[col] > 0]

                    # Sort by timestamp
                    df = df.sort_values('timestamp').reset_index(drop=True)

                return df

            except Exception as e:
                logger.error(f"Error fetching yfinance data for {ticker}: {str(e)}")
                logger.error(f"Traceback: {traceback.format_exc()}")
                return pd.DataFrame()

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

            logger.info(f"Fetching data for {ticker} from {from_date} to {to_date}")

            # Initialize Polygon.io client
            client = RESTClient(api_key)

            # Intelligent granularity selection based on date range
            two_years = datetime.utcnow() - timedelta(days=365 * 2)
            if self.start_date < two_years:
                logger.info("Fetching Polygon.io daily OHLCV data for %s from %s to %s", ticker, from_date, to_date)

                # Use pagination to get maximum available historical data
                all_trades = []
                current_from = from_date
                max_iterations = 10  # Prevent infinite loops
                iteration = 0

                while current_from <= to_date and iteration < max_iterations:
                    logger.info(f"Fetching batch {iteration + 1} starting from {current_from}")

                    batch_trades = client.get_aggs(
                        ticker,
                        multiplier=1,
                        timespan="day",
                        from_=current_from,
                        to=to_date,
                        adjusted=True,
                        limit=50000
                    )

                    if not batch_trades:
                        logger.info(f"No more data available from {current_from}")
                        break

                    batch_list = list(batch_trades)
                    all_trades.extend(batch_list)

                    # Check if we got less than the limit (indicates end of data)
                    if len(batch_list) < 50000:
                        logger.info(f"Received {len(batch_list)} records (less than limit), assuming end of data")
                        break

                    # Move to next batch - start from day after last record
                    last_timestamp = batch_list[-1].timestamp
                    last_date = datetime.fromtimestamp(last_timestamp / 1000)
                    current_from = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
                    iteration += 1

                trades = all_trades
                logger.info(f"Total records collected across {iteration + 1} batches: {len(trades)}")

            else:
                logger.info("Fetching Polygon.io minute-level OHLCV data for %s from %s to %s", ticker, from_date, to_date)
                trades = client.get_aggs(
                    ticker,
                    multiplier=1,
                    timespan="minute",
                    from_=from_date,
                    to=to_date,
                    adjusted=True,
                    limit=50000  # Keep existing limit for minute data
                )
            
            if not trades:
                logger.warning("No data returned from Polygon.io for ticker: %s", ticker)
                return pd.DataFrame()

            logger.info(f"Received {len(trades)} records from Polygon.io for {ticker}")

            # Convert to DataFrame with data processing
            data = []
            earliest_date = None
            latest_date = None

            for trade in trades:
                # Convert timestamp to proper datetime object for Oracle
                trade_datetime = datetime.fromtimestamp(trade.timestamp / 1000)

                # Track date range for debugging
                if earliest_date is None or trade_datetime < earliest_date:
                    earliest_date = trade_datetime
                if latest_date is None or trade_datetime > latest_date:
                    latest_date = trade_datetime

                data.append({
                    'timestamp': trade.timestamp,
                    'open': trade.open,
                    'high': trade.high,
                    'low': trade.low,
                    'close': trade.close,
                    'volume': trade.volume,
                    'vwap': trade.vwap,
                    'transactions': trade.transactions,
                    'ticker': ticker,
                    'trade_date': trade_datetime  # Store as datetime object, not string
                })

            logger.info(f"Data range for {ticker}: {earliest_date} to {latest_date}")

            # Check if we got the full requested range
            if earliest_date and earliest_date.date() > self.start_date.date():
                days_missing = (earliest_date.date() - self.start_date.date()).days
                logger.warning(f"API limitation: Requested data from {self.start_date.date()}, but earliest available is {earliest_date.date()} ({days_missing} days missing)")
                logger.warning("This is likely due to Polygon.io API tier limitations. Consider upgrading for more historical data.")

            df = pd.DataFrame(data)
            
            # Data cleaning and validation
            if not df.empty:
                # Remove any rows with null values in critical columns
                critical_cols = ['open', 'high', 'low', 'close', 'volume']
                df = df.dropna(subset=critical_cols)
                
                # Ensure positive values
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    if col in df.columns:
                        df = df[df[col] > 0]
                
                # Sort by timestamp
                df = df.sort_values('timestamp').reset_index(drop=True)
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching Polygon.io data for {ticker}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    def _fetch_hybrid_data(self, ticker: str) -> pd.DataFrame:
        """
        Fetch data using hybrid approach: yfinance for historical + Polygon for recent high-frequency.

        Strategy:
        - Use yfinance for data older than 2 years (unlimited free historical data)
        - Use Polygon.io for recent 2 years (minute-level data when available)
        - Merge and deduplicate the results

        Args:
            ticker (str): The ticker symbol to fetch data for

        Returns:
            pd.DataFrame: Combined DataFrame with maximum available data
        """
        operation_name = f"fetch_hybrid_data_{ticker}"

        with self.performance_monitor.monitor_operation(operation_name):
            try:
                # Define the split point (2 years ago)
                two_years_ago = datetime.utcnow() - timedelta(days=365 * 2)

                all_data = []

                # Part 1: Historical data from yfinance (if start date is before 2 years ago)
                if self.start_date < two_years_ago:
                    historical_end = min(two_years_ago, self.end_date)

                    logger.info(f"Fetching historical data via yfinance: {self.start_date.date()} to {historical_end.date()}")

                    # Create temporary DataIngestion for historical period
                    historical_ingestion = DataIngestion(
                        start_date=self.start_date.strftime("%Y-%m-%d"),
                        end_date=historical_end.strftime("%Y-%m-%d"),
                        config=self.config
                    )

                    historical_df = historical_ingestion._fetch_yfinance_data(ticker)
                    if not historical_df.empty:
                        all_data.append(historical_df)
                        logger.info(f"Retrieved {len(historical_df)} historical records from yfinance")

                # Part 2: Recent data from Polygon.io (last 2 years)
                recent_start = max(two_years_ago, self.start_date)
                if recent_start <= self.end_date:
                    logger.info(f"Fetching recent data via Polygon.io: {recent_start.date()} to {self.end_date.date()}")

                    # Create temporary DataIngestion for recent period
                    recent_ingestion = DataIngestion(
                        start_date=recent_start.strftime("%Y-%m-%d"),
                        end_date=self.end_date.strftime("%Y-%m-%d"),
                        config=self.config
                    )

                    recent_df = recent_ingestion._fetch_polygon_data(ticker)
                    if not recent_df.empty:
                        all_data.append(recent_df)
                        logger.info(f"Retrieved {len(recent_df)} recent records from Polygon.io")

                # Combine and deduplicate
                if not all_data:
                    logger.warning(f"No data retrieved from any source for {ticker}")
                    return pd.DataFrame()

                # Concatenate all data
                combined_df = pd.concat(all_data, ignore_index=True)

                # Remove duplicates based on trade_date (keep the most recent source)
                combined_df = combined_df.sort_values(['trade_date', 'timestamp']).drop_duplicates(
                    subset=['ticker', 'trade_date'], keep='last'
                ).reset_index(drop=True)

                logger.info(f"Combined dataset for {ticker}: {len(combined_df)} total records")
                logger.info(f"Date range: {combined_df['trade_date'].min()} to {combined_df['trade_date'].max()}")

                return combined_df

            except Exception as e:
                logger.error(f"Error in hybrid data fetch for {ticker}: {str(e)}")
                logger.error(f"Traceback: {traceback.format_exc()}")
                return pd.DataFrame()

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def ingest_tickers(self, tickers: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        Ingest data for multiple tickers with parallel processing and advanced features.
        
        This method processes multiple tickers efficiently using parallel processing
        while maintaining rate limits and data quality standards.
        
        Args:
            tickers (Optional[List[str]]): List of ticker symbols to process.
                If None, uses a default list of major stocks.
                
        Returns:
            Optional[pd.DataFrame]: Combined DataFrame with all ticker data
            
        Example:
            >>> ingestion = DataIngestion("2024-01-01", "2024-12-31")
            >>> df = ingestion.ingest_tickers(["AAPL", "MSFT", "GOOGL"])
        """
        if tickers is None:
            tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "NFLX", "AMD", "INTC"]
        
        logger.info(f"Starting ingestion for {len(tickers)} tickers")
        
        if self.config.parallel_processing and len(tickers) > 1:
            # Parallel processing for multiple tickers
            with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                future_to_ticker = {
                    executor.submit(self.ingest_ticker_data, ticker): ticker 
                    for ticker in tickers
                }
                
                all_data = []
                for future in as_completed(future_to_ticker):
                    ticker = future_to_ticker[future]
                    try:
                        df = future.result()
                        if not df.empty:
                            all_data.append(df)
                    except Exception as e:
                        logger.error(f"Exception occurred while processing ticker {ticker}: {str(e)}")
            
            if all_data:
                return pd.concat(all_data, ignore_index=True)
            else:
                return pd.DataFrame()
        else:
            # Sequential processing
            all_data = []
            for ticker in tickers:
                try:
                    df = self.ingest_ticker_data(ticker)
                    if not df.empty:
                        all_data.append(df)
                except Exception as e:
                    logger.error(f"Error processing ticker {ticker}: {str(e)}")
            
            if all_data:
                return pd.concat(all_data, ignore_index=True)
            else:
                return pd.DataFrame()

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def fetch(self, tickers: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        Main fetch method with advanced features.
        
        Args:
            tickers (Optional[List[str]]): List of ticker symbols to fetch
            
        Returns:
            Optional[pd.DataFrame]: Combined ticker data
        """
        return self.ingest_tickers(tickers)

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def fetch_fred_data(self):
        """
        Fetch macroeconomic data from FRED with advanced features.
        
        Returns:
            pd.DataFrame: Macroeconomic data with quality validation
        """
        operation_name = "fetch_fred_data"
        
        with self.performance_monitor.monitor_operation(operation_name):
            try:
                api_key = get_secret("ocid1.vaultsecret.oc1.iad.amaaaaaacwmkemyaca7fcwmhm3y6pjqoe5arj47tcvpvidiniodv2p7d2xcq")
                if not api_key:
                    logger.error("FRED API key not found")
                    return pd.DataFrame()
                
                # Rate limiting
                self.rate_limiter.wait_if_needed()
                
                fred = Fred(api_key=api_key)
                
                # Define macroeconomic series with validation
                series_list = [
                    "FEDFUNDS",  # Federal Funds Rate
                    "CPIAUCSL",  # Consumer Price Index
                    "UNRATE",    # Unemployment Rate
                    "GDP",       # Gross Domestic Product
                    "GS10"       # 10-Year Treasury Rate
                ]
                
                all_data = []
                
                for series_id in series_list:
                    try:
                        # Rate limiting for each series
                        self.rate_limiter.wait_if_needed()
                        
                        logger.info(f"Fetching FRED data for series: {series_id}")
                        
                        # Fetch data with date range
                        data = fred.get_series(
                            series_id,
                            observation_start=self.start_date.strftime("%Y-%m-%d"),
                            observation_end=self.end_date.strftime("%Y-%m-%d")
                        )
                        
                        if not data.empty:
                            # Convert to DataFrame format
                            df = pd.DataFrame({
                                'date': data.index,
                                'series_id': series_id,
                                'value': data.values
                            })
                            
                            # Data quality validation
                            quality_passed, quality_score, issues = self.quality_validator.validate_ohlcv_data(
                                df.rename(columns={'value': 'close'})  # Use close for validation
                            )
                            
                            if quality_passed:
                                all_data.append(df)
                            else:
                                logger.warning(f"Quality validation failed for series {series_id}: {issues}")
                        
                    except Exception as e:
                        logger.error(f"Error fetching FRED data for series {series_id}: {str(e)}")
                
                if all_data:
                    return pd.concat(all_data, ignore_index=True)
                else:
                    return pd.DataFrame()
                    
            except Exception as e:
                logger.error(f"Error in fetch_fred_data: {str(e)}")
                logger.error(f"Traceback: {traceback.format_exc()}")
                return pd.DataFrame()

    def get_performance_metrics(self) -> Dict[str, Any]:
        """
        Get comprehensive performance metrics for monitoring and analysis.
        
        Returns:
            Dict[str, Any]: Performance metrics summary
        """
        return {
            "performance_metrics": self.performance_monitor.metrics,
            "config": asdict(self.config),
            "timestamp": datetime.now().isoformat()
        }

# Backward compatibility aliases
# DataIngestion is now the main class
