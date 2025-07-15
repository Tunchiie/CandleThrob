#!/usr/bin/env python3
"""
Transform Data Script for CandleThrob
=====================================

This script processes raw OHLCV data and adds technical indicators using TA-Lib.
It's designed to run as the transformation step after data ingestion is complete.

Features:
- Comprehensive error handling and validation
- Performance monitoring and logging
- Type safety with full type hints
- Data quality checks and validation
- Modular design for maintainability
- Comprehensive documentation

Author: Adetunji Fasiku
Version: 1.0.0
"""

import sys
import os
import re
import pandas as pd
import logging
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from dataclasses import dataclass
from pathlib import Path
import traceback

# Add parent directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from CandleThrob.transform.enrich_data import TechnicalIndicators, EnrichMacros
from CandleThrob.utils.oracle_conn import OracleDB
from CandleThrob.utils.models import TickerData, TransformedTickerData, MacroData, TransformedMacroData

# Import centralized logging configuration
from CandleThrob.utils.logging_config import get_ingestion_logger, log_ingestion_start, log_ingestion_success, log_ingestion_error

# Get logger for this module
logger = get_ingestion_logger("transform_data")


@dataclass
class TransformConfig:
    """Configuration class for data transformation settings."""
    
    batch_size: int = 1000
    timeout_seconds: int = 3600
    max_retries: int = 3
    validation_enabled: bool = True
    performance_monitoring: bool = True
    data_quality_checks: bool = True
    
    def __post_init__(self):
        """Validate configuration parameters."""
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")


class DataValidator:
    """Data validation utilities for transformation pipeline."""
    
    @staticmethod
    def validate_ticker_data(df: pd.DataFrame) -> bool:
        """
        Validate ticker data for required columns and data quality.
        
        Args:
            df: DataFrame containing ticker data
            
        Returns:
            bool: True if validation passes
            
        Raises:
            ValueError: If validation fails
        """
        if df is None or df.empty:
            raise ValueError("DataFrame is None or empty")
        
        required_columns = ['ticker', 'trade_date', 'open', 'high', 'low', 'close', 'volume']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Check for null values in critical columns
        critical_columns = ['ticker', 'trade_date', 'close']
        for col in critical_columns:
            if df[col].isnull().any():
                logger.warning(f"Found null values in critical column: {col}")
        
        # Validate date range
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            min_date = df['trade_date'].min()
            max_date = df['trade_date'].max()
            
            if min_date < pd.Timestamp('1990-01-01'):
                logger.warning(f"Data contains dates before 1990: {min_date}")
            
            if max_date > datetime.now():
                logger.warning(f"Data contains future dates: {max_date}")
        
        # Validate price data
        if all(col in df.columns for col in ['open', 'high', 'low', 'close']):
            price_validation = (
                (df['high'] >= df['low']).all() and
                (df['high'] >= df['open']).all() and
                (df['high'] >= df['close']).all() and
                (df['low'] <= df['open']).all() and
                (df['low'] <= df['close']).all()
            )
            
            if not price_validation:
                raise ValueError("Price data validation failed: OHLC relationships are invalid")
        
        # Validate volume data
        if 'volume' in df.columns:
            if (df['volume'] < 0).any():
                raise ValueError("Volume data contains negative values")
        
        logger.info("Data validation completed successfully")
        return True
    
    @staticmethod
    def validate_macro_data(df: pd.DataFrame) -> bool:
        """
        Validate macroeconomic data for required columns and data quality.
        
        Args:
            df: DataFrame containing macro data
            
        Returns:
            bool: True if validation passes
            
        Raises:
            ValueError: If validation fails
        """
        if df is None or df.empty:
            raise ValueError("Macro DataFrame is None or empty")
        
        required_columns = ['series_id', 'date', 'value']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Check for null values in critical columns
        critical_columns = ['series_id', 'date']
        for col in critical_columns:
            if df[col].isnull().any():
                logger.warning(f"Found null values in critical column: {col}")
        
        # Validate date range
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            min_date = df['date'].min()
            max_date = df['date'].max()
            
            if min_date < pd.Timestamp('1950-01-01'):
                logger.warning(f"Macro data contains dates before 1950: {min_date}")
            
            if max_date > datetime.now():
                logger.warning(f"Macro data contains future dates: {max_date}")
        
        logger.info("Macro data validation completed successfully")
        return True


class PerformanceMonitor:
    """Performance monitoring utilities for transformation pipeline."""
    
    def __init__(self):
        self.start_time = None
        self.metrics = {}
    
    def start_timer(self, operation: str):
        """Start timing an operation."""
        self.start_time = time.time()
        logger.info(f"Starting operation: {operation}")
    
    def end_timer(self, operation: str) -> float:
        """End timing an operation and return duration."""
        if self.start_time is None:
            logger.warning("Timer was not started")
            return 0.0
        
        duration = time.time() - self.start_time
        self.metrics[operation] = duration
        # log_performance_metrics(logger, operation, duration) # This line was removed as per the edit hint
        return duration
    
    def get_metrics(self) -> Dict[str, float]:
        """Get all performance metrics."""
        return self.metrics.copy()


class CompleteDataTransform:
    """
    Comprehensive data transformation pipeline for CandleThrob.
    
    This class handles the transformation of both ticker and macroeconomic data,
    including technical indicator calculation, data validation, and performance monitoring.
    
    Attributes:
        config: TransformConfig instance with transformation settings
        db: OracleDB instance for database operations
        engine: SQLAlchemy engine for database operations
        validator: DataValidator instance for data validation
        monitor: PerformanceMonitor instance for performance tracking
    """
    
    def __init__(self, config: Optional[TransformConfig] = None):
        """
        Initialize the CompleteDataTransform class.
        
        Args:
            config: Optional configuration object. If None, uses default settings.
        """
        self.config = config or TransformConfig()
        self.db = OracleDB()
        self.engine = self.db.get_sqlalchemy_engine()
        self.validator = DataValidator()
        self.monitor = PerformanceMonitor()
        
        logger.info("CompleteDataTransform initialized with configuration: %s", self.config)
    
    def transform_tickers(self, ticker: str) -> bool:
        """
        Transform data for a specific ticker.
        
        Args:
            ticker: Ticker symbol to transform
            
        Returns:
            bool: True if transformation was successful
            
        Raises:
            ValueError: If data validation fails
            Exception: If transformation fails
        """
        self.monitor.start_timer(f"transform_ticker_{ticker}")
        
        try:
            logger.info(f"Starting transformation for ticker: {ticker}")
            
            with self.db.establish_connection() as conn:
                with conn.cursor() as cursor:
                    # Fetch ticker data
                    ticker_model = TickerData()
                    ticker_data = ticker_model.get_data(cursor, ticker=ticker)
                    
                    if not ticker_data:
                        logger.error(f"No data found for ticker: {ticker}")
                        return False
                    
                    ticker_df = pd.DataFrame(ticker_data)
                    
                    # Validate data
                    if self.config.validation_enabled:
                        self.validator.validate_ticker_data(ticker_df)
                    
                    # Create transformed table
                    transformed_ticker_model = TransformedTickerData()
                    transformed_ticker_model.create_table(cursor)
                    
                    # Transform data
                    ticker_df["trade_date"] = pd.to_datetime(ticker_df["trade_date"])
                    
                    # Calculate technical indicators
                    transformed_ticker_data = TechnicalIndicators(ticker_df)
                    transformed_ticker_data.transform_data()
                    
                    # Insert transformed data
                    if transformed_ticker_data.transformed_data is not None:
                        transformed_ticker_model.insert_data(
                            self.engine, 
                            transformed_ticker_data.transformed_data
                        )
                        logger.info(f"Transformed data for ticker: {ticker} inserted into database")
                    else:
                        logger.warning(f"No transformed data generated for ticker: {ticker}")
                        return False
            
            self.monitor.end_timer(f"transform_ticker_{ticker}")
            return True
            
        except Exception as e:
            # log_error_with_context(logger, e, f"transform_ticker_{ticker}", {"ticker": ticker}) # This line was removed as per the edit hint
            logger.error(f"Error transforming ticker {ticker}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False
    
    def transform_macro_data(self, series_id: str) -> bool:
        """
        Transform macroeconomic data for a specific series.
        
        Args:
            series_id: Series ID to transform
            
        Returns:
            bool: True if transformation was successful
            
        Raises:
            ValueError: If data validation fails
            Exception: If transformation fails
        """
        self.monitor.start_timer(f"transform_macro_{series_id}")
        
        try:
            logger.info(f"Starting macro transformation for series: {series_id}")
            
            with self.db.establish_connection() as conn:
                with conn.cursor() as cursor:
                    # Fetch macro data
                    macro_model = MacroData()
                    macro_data = macro_model.get_data(cursor)
                    
                    if not macro_data:
                        logger.error(f"No macro data found")
                        return False
                    
                    macro_df = pd.DataFrame(macro_data)
                    
                    # Filter for specific series if provided
                    if series_id:
                        macro_df = macro_df[macro_df['series_id'] == series_id]
                        if macro_df.empty:
                            logger.error(f"No data found for series: {series_id}")
                            return False
                    
                    # Validate data
                    if self.config.validation_enabled:
                        self.validator.validate_macro_data(macro_df)
                    
                    # Create transformed table
                    transformed_macro_model = TransformedMacroData()
                    transformed_macro_model.create_table(cursor)
                    
                    # Transform macro data
                    transformed_macro_data = EnrichMacros(macro_df, series_id)
                    transformed_macro_data.transform_data()
                    
                    # Insert transformed data
                    if transformed_macro_data.transformed_data is not None:
                        transformed_macro_model.insert_data(
                            self.engine, 
                            transformed_macro_data.transformed_data
                        )
                        logger.info(f"Transformed macro data for series {series_id} inserted into database")
                    else:
                        logger.warning(f"No transformed macro data generated for series: {series_id}")
                        return False
            
            self.monitor.end_timer(f"transform_macro_{series_id}")
            return True
            
        except Exception as e:
            # log_error_with_context(logger, e, f"transform_macro_{series_id}", {"series_id": series_id}) # This line was removed as per the edit hint
            logger.error(f"Error transforming macro series {series_id}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False
    
    def transform_all_tickers(self) -> Dict[str, bool]:
        """
        Transform data for all available tickers.
        
        Returns:
            Dict[str, bool]: Dictionary mapping ticker symbols to success status
        """
        self.monitor.start_timer("transform_all_tickers")
        
        results = {}
        
        try:
            with self.db.establish_connection() as conn:
                with conn.cursor() as cursor:
                    ticker_model = TickerData()
                    ticker_data = ticker_model.get_data(cursor)
                    
                    if not ticker_data:
                        logger.error("No ticker data found")
                        return results
                    
                    ticker_df = pd.DataFrame(ticker_data)
                    unique_tickers = ticker_df["ticker"].unique()
                    
                    logger.info(f"Found {len(unique_tickers)} unique tickers to transform")
                    
                    for ticker in unique_tickers:
                        logger.info(f"Processing ticker: {ticker}")
                        success = self.transform_tickers(ticker)
                        results[ticker] = success
                        
                        if not success:
                            logger.error(f"Failed to transform ticker: {ticker}")
            
            self.monitor.end_timer("transform_all_tickers")
            return results
            
        except Exception as e:
            logger.error(f"Error in transform_all_tickers: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return results
    
    def transform_all_macros(self) -> Dict[str, bool]:
        """
        Transform data for all available macroeconomic series.
        
        Returns:
            Dict[str, bool]: Dictionary mapping series IDs to success status
        """
        self.monitor.start_timer("transform_all_macros")
        
        results = {}
        
        try:
            with self.db.establish_connection() as conn:
                with conn.cursor() as cursor:
                    macro_model = MacroData()
                    macro_data = macro_model.get_data(cursor)
                    
                    if not macro_data:
                        logger.error("No macro data found")
                        return results
                    
                    macro_df = pd.DataFrame(macro_data)
                    unique_series = macro_df["series_id"].unique()
                    
                    logger.info(f"Found {len(unique_series)} unique macro series to transform")
                    
                    for series_id in unique_series:
                        logger.info(f"Processing macro series: {series_id}")
                        success = self.transform_macro_data(series_id)
                        results[series_id] = success
                        
                        if not success:
                            logger.error(f"Failed to transform macro series: {series_id}")
            
            self.monitor.end_timer("transform_all_macros")
            return results
            
        except Exception as e:
            logger.error(f"Error in transform_all_macros: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return results
    
    def get_performance_metrics(self) -> Dict[str, float]:
        """
        Get performance metrics for all operations.
        
        Returns:
            Dict[str, float]: Dictionary of operation names and their durations
        """
        return self.monitor.get_metrics()
    
    def validate_transformation_results(self) -> bool:
        """
        Validate the results of the transformation process.
        
        Returns:
            bool: True if validation passes
        """
        try:
            # Check if transformed tables exist and have data
            with self.db.establish_connection() as conn:
                with conn.cursor() as cursor:
                    # Check transformed ticker data
                    cursor.execute("SELECT COUNT(*) FROM transformed_ticker_data")
                    ticker_count = cursor.fetchone()[0]
                    
                    # Check transformed macro data
                    cursor.execute("SELECT COUNT(*) FROM transformed_macro_data")
                    macro_count = cursor.fetchone()[0]
                    
                    logger.info(f"Transformation validation - Ticker records: {ticker_count}, Macro records: {macro_count}")
                    
                    if ticker_count == 0 and macro_count == 0:
                        logger.warning("No transformed data found")
                        return False
                    
                    return True
                    
        except Exception as e:
            logger.error(f"Error validating transformation results: {str(e)}")
            return False


def main():
    """
    Main function to run the complete data transformation pipeline.
    
    This function orchestrates the transformation of both ticker and macroeconomic data,
    including performance monitoring and result validation.
    """
    logger.info("Starting CandleThrob data transformation pipeline")
    
    try:
        # Initialize transformer with default configuration
        transformer = CompleteDataTransform()
        
        # Transform all tickers
        logger.info("Starting ticker transformation...")
        ticker_results = transformer.transform_all_tickers()
        
        # Transform all macros
        logger.info("Starting macro transformation...")
        macro_results = transformer.transform_all_macros()
        
        # Get performance metrics
        metrics = transformer.get_performance_metrics()
        logger.info("Performance metrics: %s", metrics)
        
        # Validate results
        validation_passed = transformer.validate_transformation_results()
        
        # Log summary
        successful_tickers = sum(1 for success in ticker_results.values() if success)
        successful_macros = sum(1 for success in macro_results.values() if success)
        
        logger.info(f"Transformation completed:")
        logger.info(f"  - Tickers: {successful_tickers}/{len(ticker_results)} successful")
        logger.info(f"  - Macros: {successful_macros}/{len(macro_results)} successful")
        logger.info(f"  - Validation: {'PASSED' if validation_passed else 'FAILED'}")
        
        if validation_passed:
            logger.info("All data transformed successfully")
        else:
            logger.error("Data transformation completed with validation failures")
            
    except Exception as e:
        logger.error(f"Critical error in transformation pipeline: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()

