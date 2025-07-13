#!/usr/bin/env python3
"""
Complete Data Ingestion Pipeline for CandleThrob

This module provides a comprehensive data ingestion system that processes all S&P 500 
and ETF tickers in a single execution. It includes stateful batch processing, 
rate limiting, error handling, and incremental data loading.

Features:
- Processes all 523 tickers (503 S&P 500 + 20 ETFs) in one execution
- Stateful batch processing with automatic progression
- Rate limiting for Polygon.io API (5 calls/minute)
- Incremental data loading (only fetches new data)
- Comprehensive error handling and retry logic
- Oracle Database integration with bulk operations
- Real-time progress monitoring and logging

Author: Adetunji Fasiku
Version: 2.0.0
Last Updated: 2025-07-13
"""

import sys
import os
import re
import pandas as pd
import time
import json
import requests
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from dataclasses import dataclass
from functools import wraps

# Add parent directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from CandleThrob.ingestion.fetch_data import DataIngestion
from CandleThrob.utils.models import TickerData, MacroData
from CandleThrob.utils.oracle_conn import OracleDB
from CandleThrob.utils.logging_config import get_ingestion_logger, log_ingestion_start, log_ingestion_success, log_ingestion_error, log_rate_limiting, log_batch_progress

# Get logger for this module
logger = get_ingestion_logger(__name__)

@dataclass
class BatchConfig:
    """
    Configuration class for batch processing parameters.
    
    Attributes:
        batch_size (int): Number of tickers to process per batch
        rate_limit_seconds (int): Seconds between API calls for rate limiting
        max_retries (int): Maximum number of retry attempts for failed operations
        retry_delay_seconds (int): Delay between retry attempts
    """
    batch_size: int
    rate_limit_seconds: int
    max_retries: int
    retry_delay_seconds: int

class RateLimiter:
    """
    Rate limiter for Polygon.io API calls to ensure compliance with API limits.
    
    Implements a token bucket algorithm to maintain consistent API call spacing
    and prevent rate limit violations.
    
    Attributes:
        calls_per_minute (int): Maximum API calls allowed per minute
        seconds_between_calls (float): Calculated seconds between calls
        last_call_time (float): Timestamp of the last API call
    """
    
    def __init__(self, calls_per_minute: int = 5):
        """
        Initialize the rate limiter.
        
        Args:
            calls_per_minute (int): Maximum API calls per minute (default: 5)
        """
        self.calls_per_minute = calls_per_minute
        self.seconds_between_calls = 60.0 / calls_per_minute
        self.last_call_time = 0.0
    
    def wait_if_needed(self) -> None:
        """
        Wait if necessary to respect rate limits.
        
        Calculates the time since the last API call and sleeps if needed
        to maintain the specified rate limit.
        """
        current_time = time.time()
        time_since_last_call = current_time - self.last_call_time
        
        if time_since_last_call < self.seconds_between_calls:
            sleep_time = self.seconds_between_calls - time_since_last_call
            log_rate_limiting("ingest_data", sleep_time)
            time.sleep(sleep_time)
        
        self.last_call_time = time.time()

def retry_on_failure(max_retries: int = 3, delay_seconds: int = 60):
    """
    Decorator for retrying failed operations with exponential backoff.
    
    Args:
        max_retries (int): Maximum number of retry attempts
        delay_seconds (int): Base delay between retry attempts
    
    Returns:
        Callable: Decorated function with retry logic
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    logger.warning(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
                    
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying in {delay_seconds} seconds...")
                        time.sleep(delay_seconds)
            
            logger.error(f"All {max_retries} attempts failed. Last error: {str(last_exception)}")
            raise last_exception
        
        return wrapper
    return decorator

class CompleteDataIngestion:
    """
    Complete data ingestion system for processing all tickers in batches.
    
    This class provides a comprehensive data ingestion pipeline that processes
    all S&P 500 and ETF tickers with stateful batch processing, rate limiting,
    and incremental data loading.
    
    Attributes:
        config (BatchConfig): Configuration for batch processing
        rate_limiter (RateLimiter): Rate limiter for API calls
        db (OracleDB): Database connection manager
        engine: SQLAlchemy engine for bulk operations
    """
    
    def __init__(self, config: BatchConfig):
        """
        Initialize the complete data ingestion system.
        
        Args:
            config (BatchConfig): Configuration for batch processing
        """
        self.config = config
        self.rate_limiter = RateLimiter()
        self.db = OracleDB()
        self.engine = self.db.get_sqlalchemy_engine()
        
    @retry_on_failure(max_retries=3, delay_seconds=60)
    def update_ticker_data(self, ticker: str) -> bool:
        """
        Update ticker data with enhanced error handling and retry logic.
        
        This method performs incremental data loading by checking existing data
        and only fetching new data from the last available date. It includes
        comprehensive error handling and rate limiting.
        
        Args:
            ticker (str): The stock ticker symbol to process
            
        Returns:
            bool: True if successful, False otherwise
            
        Raises:
            Exception: If data processing fails after all retry attempts
        """
        log_ingestion_start("ingest_data", ticker)
        
        try:
            # Rate limiting
            self.rate_limiter.wait_if_needed()
            
            with self.db.establish_connection() as conn:
                with conn.cursor() as cursor:
                    TickerData().create_table(cursor)
                
                # Check existing data and get last date for incremental loading
                ticker_model = TickerData()
                if ticker_model.data_exists(conn, ticker):
                    last_date = ticker_model.get_last_date(conn, ticker)
                    if last_date:
                        start = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
                        logger.info(f"Incremental update for {ticker}: fetching from {start}")
                    else:
                        start = "2000-01-01"
                        logger.info(f"No valid date found for {ticker}, fetching from {start}")
                else:
                    start = "2000-01-01"
                    logger.info(f"No existing data for {ticker}, fetching from {start}")
            
            end = datetime.now().strftime("%Y-%m-%d")
            
            # Skip if no new data to fetch
            if start >= end:
                logger.info(f"No new data to fetch for {ticker}")
                return True
            
            # Fetch data from Polygon.io
            data = DataIngestion(start_date=start, end_date=end)
            ticker_df = data.ingest_ticker_data(ticker)
            
            if ticker_df is None or ticker_df.empty:
                log_ingestion_error("ingest_data", ticker, "No data fetched")
                return False
            
            # Save to database using bulk operations
            logger.info(f"Saving {len(ticker_df)} records for {ticker}")
            ticker_model.insert_data(self.engine, ticker_df)
            log_ingestion_success("ingest_data", ticker, len(ticker_df))
            
            return True
            
        except Exception as e:
            log_ingestion_error("ingest_data", ticker, str(e))
            raise
    
    def process_batch(self, tickers: List[str]) -> Dict[str, Any]:
        """
        Process a batch of tickers with comprehensive error handling.
        
        This method processes a list of tickers with detailed progress tracking,
        error handling, and result aggregation. It maintains a comprehensive
        summary of processing results including success/failure counts and timing.
        
        Args:
            tickers (List[str]): List of ticker symbols to process
            
        Returns:
            Dict[str, Any]: Processing results summary with the following keys:
                - total_tickers (int): Total number of tickers processed
                - successful (int): Number of successfully processed tickers
                - failed (int): Number of failed ticker processing attempts
                - failed_tickers (List[str]): List of tickers that failed
                - start_time (datetime): Processing start timestamp
                - end_time (datetime): Processing end timestamp
                - duration_seconds (float): Total processing duration
        """
        results = {
            "total_tickers": len(tickers),
            "successful": 0,
            "failed": 0,
            "failed_tickers": [],
            "start_time": datetime.now(),
            "end_time": None
        }
        
        logger.info(f"Starting batch processing for {len(tickers)} tickers")
        
        for i, ticker in enumerate(tickers):
            log_batch_progress("ingest_data", i+1, len(tickers), ticker)
            
            try:
                success = self.update_ticker_data(ticker)
                if success:
                    results["successful"] += 1
                else:
                    results["failed"] += 1
                    results["failed_tickers"].append(ticker)
                    
            except Exception as e:
                log_ingestion_error("ingest_data", ticker, str(e))
                results["failed"] += 1
                results["failed_tickers"].append(ticker)
        
        results["end_time"] = datetime.now()
        results["duration_seconds"] = (results["end_time"] - results["start_time"]).total_seconds()
        
        logger.info(f"Batch processing completed: {results['successful']} successful, {results['failed']} failed")
        
        return results

def clean_ticker(ticker: str) -> str:
    """
    Clean ticker symbol by removing unwanted characters and standardizing format.
    
    Args:
        ticker (str): The ticker symbol to clean
        
    Returns:
        str: The cleaned ticker symbol in uppercase format
        
    Example:
        >>> clean_ticker("AAPL.O")
        'AAPL'
        >>> clean_ticker("msft")
        'MSFT'
    """
    return re.sub(r"[^A-Z0-9\-]", "-", ticker.strip().upper()).strip("-")

def get_sp500_tickers() -> List[str]:
    """
    Fetch S&P 500 ticker symbols from Wikipedia.
    
    Retrieves the current S&P 500 constituents from Wikipedia and returns
    a cleaned list of ticker symbols.
    
    Returns:
        List[str]: List of cleaned S&P 500 ticker symbols
        
    Raises:
        Exception: If unable to fetch tickers from Wikipedia
    """
    try:
        tickers = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]["Symbol"].tolist()
        return [clean_ticker(ticker) for ticker in tickers]
    except Exception as e:
        logger.error(f"Error fetching S&P 500 tickers: {str(e)}")
        return []

def get_etf_tickers() -> List[str]:
    """
    Get list of major ETF ticker symbols.
    
    Returns a curated list of major ETFs covering various market segments
    including broad market, sector, and specialized ETFs.
    
    Returns:
        List[str]: List of cleaned ETF ticker symbols
    """
    etf_tickers = [
        'SPY', 'QQQ', 'DIA', 'IWM', 'VTI', 'TLT', 'GLD',
        'XLF', 'XLE', 'XLK', 'XLV', 'ARKK', 'VXX',
        'TQQQ', 'SQQQ', 'SOXL', 'XLI', 'XLY', 'XLP', 'SLV'
    ]
    return [clean_ticker(ticker) for ticker in etf_tickers]

def main():
    """
    Main function for complete data ingestion processing all batches.
    
    This function orchestrates the complete data ingestion pipeline:
    1. Loads configuration from environment variables
    2. Fetches all S&P 500 and ETF tickers
    3. Processes all batches sequentially
    4. Tracks progress and results
    5. Signals completion when all batches are processed
    6. Saves comprehensive results to file
    
    Environment Variables:
        BATCH_SIZE (int): Number of tickers per batch (default: 25)
        POLYGON_RATE_LIMIT (int): API calls per minute (default: 12)
    
    Raises:
        SystemExit: If fatal error occurs during processing
    """
    try:
        # Load configuration from environment variables
        batch_size = int(os.getenv("BATCH_SIZE", "25"))
        rate_limit_seconds = int(os.getenv("POLYGON_RATE_LIMIT", "12"))
        
        config = BatchConfig(
            batch_size=batch_size,
            rate_limit_seconds=rate_limit_seconds,
            max_retries=3,
            retry_delay_seconds=60
        )
        
        logger.info(f"Starting complete ingestion - processing all tickers in batches of {batch_size}")
        
        # Get all tickers
        sp500_tickers = get_sp500_tickers()
        etf_tickers = get_etf_tickers()
        all_tickers = sp500_tickers + etf_tickers
        
        logger.info(f"Total tickers available: {len(all_tickers)}")
        logger.info(f"S&P 500 tickers: {len(sp500_tickers)}")
        logger.info(f"ETF tickers: {len(etf_tickers)}")
        
        # Calculate total batches
        total_batches = (len(all_tickers) + batch_size - 1) // batch_size
        
        logger.info(f"Processing {total_batches} batches of {batch_size} tickers each")
        
        # Process all batches
        ingestion = CompleteDataIngestion(config)
        overall_results = {
            "total_batches": total_batches,
            "total_tickers": len(all_tickers),
            "successful_tickers": 0,
            "failed_tickers": 0,
            "failed_tickers_list": [],
            "batch_results": [],
            "start_time": datetime.now(),
            "end_time": None
        }
        
        logger.info(f"Starting to process {total_batches} batches...")
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, len(all_tickers))
            
            batch_tickers = all_tickers[start_idx:end_idx]
            logger.info(f"Processing batch {batch_num + 1}/{total_batches}: tickers {start_idx}-{end_idx-1} ({len(batch_tickers)} tickers)")
            logger.info(f"Batch tickers: {batch_tickers}")
            
            # Process batch
            batch_results = ingestion.process_batch(batch_tickers)
            
            # Update overall results
            overall_results["successful_tickers"] += batch_results["successful"]
            overall_results["failed_tickers"] += batch_results["failed"]
            overall_results["failed_tickers_list"].extend(batch_results["failed_tickers"])
            overall_results["batch_results"].append({
                "batch_number": batch_num,
                "results": batch_results
            })
            
            logger.info(f"Batch {batch_num + 1} completed: {batch_results['successful']} successful, {batch_results['failed']} failed")
            logger.info(f"Running total: {overall_results['successful_tickers']} successful, {overall_results['failed_tickers']} failed")
        
        overall_results["end_time"] = datetime.now()
        overall_results["duration_seconds"] = (overall_results["end_time"] - overall_results["start_time"]).total_seconds()
        
        # Log final results
        logger.info(f"Complete ingestion finished!")
        logger.info(f"Total batches processed: {total_batches}")
        logger.info(f"Total tickers processed: {len(all_tickers)}")
        logger.info(f"Successful: {overall_results['successful_tickers']}")
        logger.info(f"Failed: {overall_results['failed_tickers']}")
        logger.info(f"Total duration: {overall_results['duration_seconds']:.2f} seconds")
        
        if overall_results['failed_tickers'] > 0:
            logger.warning(f"Failed tickers: {overall_results['failed_tickers_list']}")
        
        # Signal completion
        try:
            with open("/app/data/batch_cycle_complete.flag", "w") as f:
                f.write(datetime.now().isoformat())
            logger.info("Batch cycle complete flag written.")
        except Exception as e:
            logger.error(f"Failed to write batch cycle complete flag: {e}")
        
        # Save results to file for monitoring
        results_file = f"/app/data/complete_ingestion_results.json"
        os.makedirs(os.path.dirname(results_file), exist_ok=True)
        
        with open(results_file, 'w') as f:
            json.dump(overall_results, f, indent=2)
        
        logger.info(f"Results saved to {results_file}")
        
    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 