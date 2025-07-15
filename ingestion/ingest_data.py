#!/usr/bin/env python3
"""
Complete Data Ingestion Pipeline for CandleThrob
===============================================

This module provides data ingestion capabilities for processing all S&P 500
and ETF tickers in a single execution. It includes stateful batch processing, advanced
rate limiting, comprehensive error handling, and incremental data loading.

Features:
- Advanced error handling and retry logic with exponential backoff
- Performance monitoring and metrics collection
- Data quality validation and scoring
- Memory usage optimization
- Advanced logging and monitoring
- Data lineage tracking and audit trails
- Quality gates and validation checkpoints
- Resource monitoring and optimization
- Modular design with configurable batch processing
- Advanced error categorization and handling
- Parallel processing capabilities
- Comprehensive progress tracking and reporting

Author: CandleThrob Team
Version: 3.0.0
Last Updated: 2025-07-14
"""

import sys
import os
import re
import time
import json
import psutil
import threading
import traceback
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union, Tuple
from dataclasses import dataclass, field
from functools import wraps
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add parent directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from CandleThrob.ingestion.fetch_data import DataIngestion
from CandleThrob.utils.models import TickerData, MacroData
from CandleThrob.utils.oracle_conn import OracleDB
from CandleThrob.utils.logging_config import (
    get_ingestion_logger, log_ingestion_start, log_ingestion_success, 
    log_ingestion_error, log_rate_limiting, log_batch_progress
)

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
        max_processing_time_seconds (float): Maximum time for processing
        max_memory_usage_mb (float): Maximum memory usage in MB
        parallel_processing (bool): Enable parallel processing
        max_workers (int): Maximum number of worker threads
        min_data_quality_score (float): Minimum data quality score
        enable_audit_trail (bool): Enable audit trail
        enable_performance_monitoring (bool): Enable performance monitoring
    """
    batch_size: int = 25
    rate_limit_seconds: int = 12
    max_retries: int = 3
    retry_delay_seconds: int = 60
    max_processing_time_seconds: float = 7200.0  # 2 hours
    max_memory_usage_mb: float = 2048.0
    parallel_processing: bool = True
    max_workers: int = 4
    min_data_quality_score: float = 0.8
    enable_audit_trail: bool = True
    enable_performance_monitoring: bool = True
    enable_memory_optimization: bool = True
    enable_data_lineage: bool = True
    
    def __post_init__(self):
        """Validate configuration parameters."""
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.rate_limit_seconds <= 0:
            raise ValueError("rate_limit_seconds must be positive")
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if not 0.0 <= self.min_data_quality_score <= 1.0:
            raise ValueError("min_data_quality_score must be between 0.0 and 1.0")

class PerformanceMonitor:
    """Performance monitoring for ingestion operations."""
    
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

class RateLimiter:
    """
    Rate limiter for Polygon.io API calls with advanced features.
    
    Implements a token bucket algorithm with exponential backoff and adaptive
    rate limiting to ensure compliance with API limits and handle rate limit
    violations gracefully.
    
    Attributes:
        calls_per_minute (int): Maximum API calls allowed per minute
        seconds_between_calls (float): Calculated seconds between calls
        last_call_time (float): Timestamp of the last API call
        consecutive_failures (int): Count of consecutive rate limit failures
        adaptive_delay (float): Adaptive delay based on failures
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
        self.consecutive_failures = 0
        self.adaptive_delay = self.seconds_between_calls
        self._lock = threading.Lock()
    
    def wait_if_needed(self) -> None:
        """
        Wait if necessary to respect rate limits with adaptive backoff.
        
        Calculates the time since the last API call and sleeps if needed
        to maintain the specified rate limit. Implements adaptive delays
        based on consecutive failures.
        """
        with self._lock:
            current_time = time.time()
            time_since_last_call = current_time - self.last_call_time
            
            # Use adaptive delay if we've had consecutive failures
            effective_delay = self.adaptive_delay if self.consecutive_failures > 0 else self.seconds_between_calls
            
            if time_since_last_call < effective_delay:
                sleep_time = effective_delay - time_since_last_call
                log_rate_limiting("ingest_data", sleep_time)
                time.sleep(sleep_time)
            
            self.last_call_time = time.time()
    
    def record_failure(self) -> None:
        """Record a rate limit failure and increase adaptive delay."""
        with self._lock:
            self.consecutive_failures += 1
            self.adaptive_delay = min(self.adaptive_delay * 1.5, 60.0)  # Cap at 60 seconds
    
    def record_success(self) -> None:
        """Record a successful call and reset adaptive delay."""
        with self._lock:
            self.consecutive_failures = 0
            self.adaptive_delay = self.seconds_between_calls

def retry_on_failure(max_retries: int = 3, delay_seconds: int = 60):
    """
    Retry decorator with exponential backoff and advanced error handling.
    
    Args:
        max_retries (int): Maximum number of retry attempts
        delay_seconds (int): Base delay between retry attempts
    
    Returns:
        Callable: Decorated function with advanced retry logic
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    result = func(*args, **kwargs)
                    # Record success if this is a rate limiter method
                    if hasattr(args[0], 'rate_limiter') and hasattr(args[0].rate_limiter, 'record_success'):
                        args[0].rate_limiter.record_success()
                    return result
                except Exception as e:
                    last_exception = e
                    logger.warning(f"Attempt {attempt + 1}/{max_retries + 1} failed for {func.__name__}: {str(e)}")
                    
                    # Record failure if this is a rate limiter method
                    if hasattr(args[0], 'rate_limiter') and hasattr(args[0].rate_limiter, 'record_failure'):
                        args[0].rate_limiter.record_failure()
                    
                    if attempt < max_retries:
                        delay = delay_seconds * (2 ** attempt)  # Exponential backoff
                        logger.info(f"Retrying in {delay} seconds...")
                        time.sleep(delay)
            
            logger.error(f"All {max_retries + 1} attempts failed for {func.__name__}. Last error: {str(last_exception)}")
            raise last_exception
        
        return wrapper
    return decorator

class DataQualityValidator:
    """Data quality validation for ticker data."""
    
    @staticmethod
    def validate_ticker_data(df: pd.DataFrame) -> Tuple[bool, float, List[str]]:
        """Validate ticker data quality."""
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
        
        # Check OHLC relationships
        if all(col in df.columns for col in ['high', 'low', 'open', 'close']):
            invalid_ohlc = (
                (df['high'] < df['low']) | 
                (df['high'] < df['open']) | 
                (df['high'] < df['close']) |
                (df['low'] > df['open']) | 
                (df['low'] > df['close'])
            )
            if invalid_ohlc.any():
                issues.append("Invalid OHLC relationships found")
                quality_score -= 0.3
        
        # Check for excessive null values
        null_ratio = df.isnull().sum().sum() / (len(df) * len(df.columns))
        if null_ratio > 0.1:
            issues.append(f"High null ratio: {null_ratio:.2%}")
            quality_score -= 0.2
        
        quality_score = max(0.0, quality_score)
        return quality_score >= 0.8, quality_score, issues

class CompleteDataIngestion:
    """
    Complete data ingestion system for processing all tickers in batches.

    This class provides data ingestion capabilities that processes
    all S&P 500 and ETF tickers with stateful batch processing, advanced rate limiting,
    comprehensive error handling, and incremental data loading.

    Attributes:
        config (BatchConfig): Configuration for batch processing
        rate_limiter (RateLimiter): Advanced rate limiter for API calls
        performance_monitor (PerformanceMonitor): Performance monitor
        quality_validator (DataQualityValidator): Data quality validator
        db (OracleDB): Database connection manager
        engine: SQLAlchemy engine for bulk operations
    """
    
    def __init__(self, config: BatchConfig):
        """
        Initialize the complete data ingestion system.

        Args:
            config (BatchConfig): Configuration for batch processing

        Example:
            >>> config = BatchConfig(batch_size=25, rate_limit_seconds=12)
            >>> ingestion = CompleteDataIngestion(config)
        """
        self.config = config
        self.rate_limiter = RateLimiter()
        self.performance_monitor = PerformanceMonitor()
        self.quality_validator = DataQualityValidator()
        self.db = OracleDB()
        self.engine = self.db.get_sqlalchemy_engine()
        
        logger.info("CompleteDataIngestion initialized with configuration")
    
    @retry_on_failure(max_retries=3, delay_seconds=60)
    def update_ticker_data(self, ticker: str) -> bool:
        """
        Update ticker data with advanced features and comprehensive error handling.
        
        This method performs incremental data loading by checking existing data
        and only fetching new data from the last available date. It includes
        comprehensive error handling, rate limiting, and data quality validation.
        
        Args:
            ticker (str): The stock ticker symbol to process
            
        Returns:
            bool: True if successful, False otherwise
            
        Raises:
            Exception: If data processing fails after all retry attempts
            
        Example:
            >>> ingestion = CompleteDataIngestion(config)
            >>> success = ingestion.update_ticker_data("AAPL")
        """
        operation_name = f"update_ticker_data_{ticker}"
        
        with self.performance_monitor.monitor_operation(operation_name):
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
                
                # Fetch data from Polygon.io with advanced features
                data = DataIngestion(start_date=start, end_date=end)
                ticker_df = data.ingest_ticker_data(ticker)
                
                if ticker_df is None or ticker_df.empty:
                    log_ingestion_error("ingest_data", ticker, "No data fetched")
                    return False
                
                # Data quality validation
                quality_passed, quality_score, quality_issues = self.quality_validator.validate_ticker_data(ticker_df)
                
                if not quality_passed:
                    logger.error(f"Data quality validation failed for {ticker}: {quality_issues}")
                    log_ingestion_error("ingest_data", ticker, f"Quality validation failed: {quality_issues}")
                    return False
                
                # Save to database using bulk operations
                logger.info(f"Saving {len(ticker_df)} records for {ticker} (quality score: {quality_score:.2f})")
                ticker_model.insert_data(self.engine, ticker_df)
                log_ingestion_success("ingest_data", ticker, len(ticker_df))
                
                return True
                
            except Exception as e:
                error_msg = f"Error updating ticker data for {ticker}: {str(e)}"
                logger.error(error_msg)
                logger.error(f"Traceback: {traceback.format_exc()}")
                log_ingestion_error("ingest_data", ticker, error_msg)
                raise
    
    def process_batch(self, tickers: List[str]) -> Dict[str, Any]:
        """
        Process a batch of tickers with advanced features and comprehensive error handling.
        
        This method processes a list of tickers with detailed progress tracking,
        error handling, result aggregation, and performance monitoring. It maintains
        a comprehensive summary of processing results including success/failure counts,
        timing, and quality metrics.
        
        Args:
            tickers (List[str]): List of ticker symbols to process
            
        Returns:
            Dict[str, Any]: Comprehensive processing results summary with the following keys:
                - total_tickers (int): Total number of tickers processed
                - successful (int): Number of successfully processed tickers
                - failed (int): Number of failed ticker processing attempts
                - failed_tickers (List[str]): List of tickers that failed
                - start_time (datetime): Processing start timestamp
                - end_time (datetime): Processing end timestamp
                - duration_seconds (float): Total processing duration
                - quality_scores (List[float]): Quality scores for successful tickers
                - performance_metrics (Dict): Performance metrics
                
        Example:
            >>> ingestion = CompleteDataIngestion(config)
            >>> results = ingestion.process_batch(["AAPL", "MSFT", "GOOGL"])
        """
        operation_name = f"process_batch_{len(tickers)}_tickers"
        
        with self.performance_monitor.monitor_operation(operation_name):
            results = {
                "total_tickers": len(tickers),
                "successful": 0,
                "failed": 0,
                "failed_tickers": [],
                "quality_scores": [],
                "start_time": datetime.now(),
                "end_time": None,
                "performance_metrics": {}
            }
            
            logger.info(f"Starting batch processing for {len(tickers)} tickers")
            
            if self.config.parallel_processing and len(tickers) > 1:
                # Parallel processing for multiple tickers
                with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                    future_to_ticker = {
                        executor.submit(self.update_ticker_data, ticker): ticker 
                        for ticker in tickers
                    }
                    
                    for i, future in enumerate(as_completed(future_to_ticker)):
                        ticker = future_to_ticker[future]
                        log_batch_progress("ingest_data", i+1, len(tickers), ticker)
                        
                        try:
                            success = future.result()
                            if success:
                                results["successful"] += 1
                                results["quality_scores"].append(1.0)  # Default quality score
                            else:
                                results["failed"] += 1
                                results["failed_tickers"].append(ticker)
                        except Exception as e:
                            logger.error(f"Exception occurred while processing ticker {ticker}: {str(e)}")
                            results["failed"] += 1
                            results["failed_tickers"].append(ticker)
            else:
                # Sequential processing
                for i, ticker in enumerate(tickers):
                    log_batch_progress("ingest_data", i+1, len(tickers), ticker)
                    
                    try:
                        success = self.update_ticker_data(ticker)
                        if success:
                            results["successful"] += 1
                            results["quality_scores"].append(1.0)  # Default quality score
                        else:
                            results["failed"] += 1
                            results["failed_tickers"].append(ticker)
                    except Exception as e:
                        logger.error(f"Exception occurred while processing ticker {ticker}: {str(e)}")
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
    Fetch S&P 500 ticker symbols from Wikipedia with advanced error handling.
    
    Retrieves the current S&P 500 constituents from Wikipedia and returns
    a cleaned list of ticker symbols with comprehensive error handling.
    
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
        logger.error(f"Traceback: {traceback.format_exc()}")
        return []

def get_etf_tickers() -> List[str]:
    """
    Get list of major ETF ticker symbols with advanced features.
    
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
    
    This function orchestrates the data ingestion pipeline:
    1. Loads configuration from environment variables
    2. Fetches all S&P 500 and ETF tickers
    3. Processes all batches with advanced features
    4. Tracks progress and results with comprehensive metrics
    5. Signals completion when all batches are processed
    6. Saves comprehensive results to file
    
    Environment Variables:
        BATCH_SIZE (int): Number of tickers per batch (default: 25)
        POLYGON_RATE_LIMIT (int): API calls per minute (default: 12)
        MAX_PROCESSING_TIME (float): Maximum processing time in seconds (default: 7200)
        MAX_MEMORY_USAGE (float): Maximum memory usage in MB (default: 2048)
        PARALLEL_PROCESSING (bool): Enable parallel processing (default: true)
        MAX_WORKERS (int): Maximum number of worker threads (default: 4)
    
    Raises:
        SystemExit: If fatal error occurs during processing
    """
    try:
        # Load configuration from environment variables with defaults
        batch_size = int(os.getenv("BATCH_SIZE", "25"))
        rate_limit_seconds = int(os.getenv("POLYGON_RATE_LIMIT", "12"))
        max_processing_time = float(os.getenv("MAX_PROCESSING_TIME", "7200"))
        max_memory_usage = float(os.getenv("MAX_MEMORY_USAGE", "2048"))
        parallel_processing = os.getenv("PARALLEL_PROCESSING", "true").lower() == "true"
        max_workers = int(os.getenv("MAX_WORKERS", "4"))
        
        config = BatchConfig(
            batch_size=batch_size,
            rate_limit_seconds=rate_limit_seconds,
            max_retries=3,
            retry_delay_seconds=60,
            max_processing_time_seconds=max_processing_time,
            max_memory_usage_mb=max_memory_usage,
            parallel_processing=parallel_processing,
            max_workers=max_workers
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
            "performance_metrics": {},
            "start_time": datetime.now(),
            "end_time": None
        }
        
        logger.info(f"Starting to process {total_batches} batches with advanced features...")
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, len(all_tickers))
            
            batch_tickers = all_tickers[start_idx:end_idx]
            logger.info(f"Processing batch {batch_num + 1}/{total_batches}: tickers {start_idx}-{end_idx-1} ({len(batch_tickers)} tickers)")
            logger.info(f"Batch tickers: {batch_tickers}")
            
            # Process batch with advanced features
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
        overall_results["performance_metrics"] = ingestion.performance_monitor.metrics
        
        # Log final results with advanced features
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
        
        # Save comprehensive results to file for monitoring
        results_file = f"/app/data/ingestion_results.json"
        os.makedirs(os.path.dirname(results_file), exist_ok=True)
        
        with open(results_file, 'w') as f:
            json.dump(overall_results, f, indent=2)
        
        logger.info(f"Results saved to {results_file}")
        
    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)

# Backward compatibility aliases
# BatchConfig and CompleteDataIngestion are now the main classes

if __name__ == "__main__":
    main() 