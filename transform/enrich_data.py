#!/usr/bin/env python3
"""
Technical Indicators and Data Enrichment Module for CandleThrob
==============================================================

This module provides comprehensive technical indicator calculation and data enrichment
capabilities for financial data analysis. It includes TA-Lib integration, custom
indicators, and macroeconomic data processing.

Features:
- Comprehensive technical indicator calculation using TA-Lib
- Custom indicator implementations for advanced analysis
- Macroeconomic data enrichment and transformation
- Data validation and quality checks
- Performance monitoring and optimization
- Advanced error handling and logging

Author: CandleThrob Team
Version: 1.0.0
"""

import pandas as pd
import os
import logging
import time
import numpy as np
from typing import Optional, List, Dict, Any, Union, Tuple
from dataclasses import dataclass
from pathlib import Path
import traceback
from tqdm import tqdm
from datetime import datetime, timedelta
from CandleThrob.ingestion.fetch_data import DataIngestion

# Import centralized logging configuration
#from CandleThrob.transform.logging_config import get_transform_logger, log_performance_metrics, log_data_quality_metrics, log_error_with_context

# Get logger for this module
#logger = get_transform_logger("enrich_data")
logger = logging.getLogger(__name__)

# Try to import talib, with fallback if not available
try:
    import talib
    TALIB_AVAILABLE = True
    logger.info("TA-Lib successfully imported")
except ImportError as e:
    logger.warning(f"TA-Lib not available: {e}")
    logger.warning("Please install TA-Lib C library first, then pip install TA-Lib")
    TALIB_AVAILABLE = False


@dataclass
class IndicatorConfig:
    """Configuration for technical indicator calculation."""
    
    # Momentum indicators
    rsi_period: int = 14
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    stoch_k_period: int = 5
    stoch_d_period: int = 3
    cci_period: int = 14
    roc_period: int = 10
    mom_period: int = 10
    trix_period: int = 30
    willr_period: int = 14
    
    # Moving averages
    sma_periods: List[int] = None
    ema_periods: List[int] = None
    
    # Volume indicators
    mfi_period: int = 14
    adosc_fast: int = 3
    adosc_slow: int = 10
    cmf_period: int = 20
    
    # Volatility indicators
    atr_period: int = 14
    bbands_period: int = 20
    bbands_dev: float = 2.0
    donchian_period: int = 20
    ulcer_period: int = 14
    
    # Pattern indicators
    pattern_sensitivity: float = 1.0
    
    # Statistical indicators
    stddev_period: int = 20
    var_period: int = 20
    beta_period: int = 20
    zscore_window: int = 20
    
    def __post_init__(self):
        """Validate configuration parameters."""
        if self.sma_periods is None:
            self.sma_periods = [10, 20, 50, 100, 200]
        if self.ema_periods is None:
            self.ema_periods = [10, 20, 50, 100, 200]
        
        # Validate all periods are positive
        for period in [self.rsi_period, self.macd_fast, self.macd_slow, 
                      self.macd_signal, self.stoch_k_period, self.stoch_d_period,
                      self.cci_period, self.roc_period, self.mom_period, 
                      self.trix_period, self.willr_period, self.mfi_period,
                      self.adosc_fast, self.adosc_slow, self.cmf_period,
                      self.atr_period, self.bbands_period, self.donchian_period,
                      self.ulcer_period, self.stddev_period, self.var_period,
                      self.beta_period, self.zscore_window]:
            if period <= 0:
                raise ValueError(f"Period must be positive, got: {period}")
        
        # Validate moving average periods
        for period in self.sma_periods + self.ema_periods:
            if period <= 0:
                raise ValueError(f"Moving average period must be positive, got: {period}")
        
        if self.bbands_dev <= 0:
            raise ValueError("Bollinger Bands deviation must be positive")
        
        if self.pattern_sensitivity <= 0:
            raise ValueError("Pattern sensitivity must be positive")


class DataQualityChecker:
    """Data quality validation utilities."""
    
    @staticmethod
    def validate_ohlcv_data(df: pd.DataFrame) -> bool:
        """
        Validate OHLCV data for quality and consistency.
        
        Args:
            df: DataFrame containing OHLCV data
            
        Returns:
            bool: True if validation passes
            
        Raises:
            ValueError: If validation fails
        """
        if df is None or df.empty:
            raise ValueError("DataFrame is None or empty")
        
        required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required OHLCV columns: {missing_columns}")
        
        # Check for null values
        for col in required_columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                logger.warning(f"Found {null_count} null values in column {col}")
        
        # Validate price relationships
        price_validation = (
            (df['High'] >= df['Low']).all() and
            (df['High'] >= df['Open']).all() and
            (df['High'] >= df['Close']).all() and
            (df['Low'] <= df['Open']).all() and
            (df['Low'] <= df['Close']).all()
        )
        
        if not price_validation:
            raise ValueError("Price data validation failed: OHLC relationships are invalid")
        
        # Check for extreme values
        for col in ['Open', 'High', 'Low', 'Close']:
            if (df[col] <= 0).any():
                raise ValueError(f"Found non-positive values in {col}")
        
        if (df['Volume'] < 0).any():
            raise ValueError("Found negative volume values")
        
        # Check for reasonable price ranges
        price_cols = ['Open', 'High', 'Low', 'Close']
        for col in price_cols:
            mean_price = df[col].mean()
            if mean_price > 0:
                # Check for values that are more than 1000x the mean (likely errors)
                extreme_threshold = mean_price * 1000
                extreme_count = (df[col] > extreme_threshold).sum()
                if extreme_count > 0:
                    logger.warning(f"Found {extreme_count} extreme values in {col}")
        
        logger.info("OHLCV data validation completed successfully")
        return True
    
    @staticmethod
    def validate_macro_data(df: pd.DataFrame) -> bool:
        """
        Validate macroeconomic data for quality and consistency.
        
        Args:
            df: DataFrame containing macro data
            
        Returns:
            bool: True if validation passes
            
        Raises:
            ValueError: If validation fails
        """
        if df is None or df.empty:
            raise ValueError("Macro DataFrame is None or empty")
        
        required_columns = ['Date', 'value']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required macro columns: {missing_columns}")
        
        # Check for null values
        for col in required_columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                logger.warning(f"Found {null_count} null values in column {col}")
        
        # Validate date column
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            if df['Date'].isnull().any():
                raise ValueError("Found null values in Date column")
        
        # Check for reasonable value ranges
        if 'value' in df.columns:
            if df['value'].isnull().all():
                raise ValueError("All values in 'value' column are null")
            
            # Check for extreme outliers
            q1 = df['value'].quantile(0.01)
            q3 = df['value'].quantile(0.99)
            iqr = q3 - q1
            lower_bound = q1 - 3 * iqr
            upper_bound = q3 + 3 * iqr
            
            outliers = ((df['value'] < lower_bound) | (df['value'] > upper_bound)).sum()
            if outliers > 0:
                logger.warning(f"Found {outliers} potential outliers in macro data")
        
        logger.info("Macro data validation completed successfully")
        return True

class TechnicalIndicators:
    """
    Comprehensive technical indicator calculation class for financial data analysis.
    
    This class provides extensive technical indicator calculation capabilities using TA-Lib,
    including momentum, volume, pattern, volatility, price, cyclical, and statistical indicators.
    It includes data validation, quality checks, and performance monitoring.
    
    Attributes:
        ticker_df: DataFrame containing stock data with OHLCV columns
        config: IndicatorConfig instance with calculation parameters
        transformed_df: DataFrame containing calculated indicators
        quality_checker: DataQualityChecker instance for data validation
        performance_metrics: Dictionary tracking calculation performance
    """

    def __init__(self, ticker_df: pd.DataFrame, config: Optional[IndicatorConfig] = None):
        """
        Initialize the TechnicalIndicators class.

        Args:
            ticker_df: DataFrame containing stock data with OHLCV columns
            config: Optional configuration object. If None, uses default settings.
            
        Raises:
            ValueError: If data validation fails
        """
        self.config = config or IndicatorConfig()
        self.quality_checker = DataQualityChecker()
        self.performance_metrics = {}
        
        # Validate and prepare data
        if ticker_df is None or ticker_df.empty:
            raise ValueError("Ticker DataFrame is None or empty")
        
        # Standardize column names
        column_mapping = {
            'trade_date': 'Date',
            'open': 'Open',
            'high': 'High', 
            'low': 'Low',
            'close': 'Close',
            'volume': 'Volume'
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in ticker_df.columns and new_col not in ticker_df.columns:
                ticker_df = ticker_df.rename(columns={old_col: new_col})
        
        # Validate OHLCV data
        self.quality_checker.validate_ohlcv_data(ticker_df)
        
        # Prepare data
        ticker_df["Date"] = pd.to_datetime(ticker_df["Date"], utc=True)
        self.ticker_df = ticker_df[ticker_df["Date"].dt.year >= 2000].copy()
        self.transformed_df = None
        
        logger.info(f"TechnicalIndicators initialized with {len(self.ticker_df)} records")
        logger.info(f"Date range: {self.ticker_df['Date'].min()} to {self.ticker_df['Date'].max()}")
        
    def enrich_tickers(self):
        """ 
        Enrich the ticker data with basic return calculations.
        Note: Market cap, sector, and industry enrichment has been removed for Polygon-only strategy.
        This method calculates returns for various periods (1, 3, 7, 30, 90, and 365 days).
        Returns:
            None: The method updates the ticker DataFrame in place with additional return columns.
        """
        logger.info("Enriching data with return calculations...")
        
        if self.ticker_df is None or self.ticker_df.empty:
            raise ValueError("Ticker DataFrame is empty. Please run ingest_tickers() first.")
        
        # Calculate returns for various periods
        for i in [1, 3, 7, 30, 90, 365]:
            self.ticker_df[f"Return_{i}d"] = self.ticker_df.groupby("Ticker")["Close"]\
                .pct_change(periods=i).fillna(0)
        
        self.ticker_df["Date"] = pd.to_datetime(self.ticker_df["Date"])
        logger.info("Return calculations completed successfully.")

    def calculate_technical_indicators(self):
        """
        Calculate technical indicators for the stock data in the DataFrame.

        Returns:
            pd.DataFrame: DataFrame with technical indicators added.
        """
        if not TALIB_AVAILABLE:
            logger.error("TA-Lib is not available. Cannot calculate technical indicators.")
            raise ImportError("TA-Lib is required for technical indicators but is not installed.")
            
        logger.info("Calculating technical indicators...")
        # Ensure the DataFrame has the necessary columns
        required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in self.ticker_df.columns for col in required_columns):
            raise ValueError(f"DataFrame must contain columns: {required_columns}")
        self.transform()

        logger.info("Technical indicators calculated successfully.")
        return self.ticker_df
    
    def _calculate_momentum_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate momentum indicators for a given DataFrame."""
        return self.get_talib_momentum_indicators(df)
    
    def _calculate_volume_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate volume indicators for a given DataFrame."""
        return self.get_talib_volume_indicators(df)
    
    def _calculate_pattern_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate pattern indicators for a given DataFrame."""
        return self.get_talib_pattern_indicators(df)
    
    def _calculate_volatility_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate volatility indicators for a given DataFrame."""
        return self.get_talib_volatility_indicators(df)
    
    def _calculate_price_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate price indicators for a given DataFrame."""
        return self.get_talib_price_indicators(df)
    
    def _calculate_cyclical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate cyclical indicators for a given DataFrame."""
        return self.get_talib_cyclical_indicators(df)
    
    def _calculate_statistical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate statistical indicators for a given DataFrame."""
        return self.get_talib_statistical_indicators(df)
    
    def _merge_indicators(self, momentum_df: pd.DataFrame, volume_df: pd.DataFrame,
                         pattern_df: pd.DataFrame, volatility_df: pd.DataFrame,
                         price_df: pd.DataFrame, cyclical_df: pd.DataFrame,
                         statistical_df: pd.DataFrame, ticker: str) -> None:
        """
        Merge all calculated indicators into the main transformed DataFrame.
        
        Args:
            momentum_df: DataFrame with momentum indicators
            volume_df: DataFrame with volume indicators
            pattern_df: DataFrame with pattern indicators
            volatility_df: DataFrame with volatility indicators
            price_df: DataFrame with price indicators
            cyclical_df: DataFrame with cyclical indicators
            statistical_df: DataFrame with statistical indicators
            ticker: Ticker symbol being processed
        """
        try:
            # Start with base data
            if self.transformed_df is None:
                self.transformed_df = self.ticker_df.copy()
            
            # Merge each indicator category
            indicator_dfs = [
                ('momentum', momentum_df),
                ('volume', volume_df),
                ('pattern', pattern_df),
                ('volatility', volatility_df),
                ('price', price_df),
                ('cyclical', cyclical_df),
                ('statistical', statistical_df)
            ]
            
            for indicator_type, indicator_df in indicator_dfs:
                if indicator_df is not None and not indicator_df.empty:
                    self.transformed_df = self.safe_merge_or_concat(
                        self.transformed_df, indicator_df, on=['Date', 'Ticker'], how='left'
                    )
                    logger.debug(f"{indicator_type.capitalize()} indicators merged for ticker {ticker}")
                else:
                    logger.warning(f"No {indicator_type} indicators calculated for ticker {ticker}")
                    
        except Exception as e:
            logger.error(f"Error merging indicators for ticker {ticker}: {str(e)}")
            raise
    
    def _validate_transformed_data(self) -> None:
        """
        Validate the final transformed dataset for quality and completeness.
        
        Raises:
            ValueError: If validation fails
        """
        if self.transformed_df is None or self.transformed_df.empty:
            raise ValueError("Transformed DataFrame is None or empty")
        
        # Check for required columns
        required_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        missing_columns = [col for col in required_columns if col not in self.transformed_df.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns in transformed data: {missing_columns}")
        
        # Check for null values in critical columns
        critical_columns = ['Date', 'Close']
        for col in critical_columns:
            null_count = self.transformed_df[col].isnull().sum()
            if null_count > 0:
                logger.warning(f"Found {null_count} null values in critical column: {col}")
        
        # Check for reasonable number of indicator columns
        indicator_columns = [col for col in self.transformed_df.columns 
                           if col not in required_columns + ['Ticker']]
        
        if len(indicator_columns) < 10:
            logger.warning(f"Only {len(indicator_columns)} indicator columns found, expected more")
        
        log_data_quality_metrics(logger, "transformed_data_validation", len(self.transformed_df), len(self.transformed_df.columns))
    
    def get_performance_metrics(self) -> Dict[str, float]:
        """
        Get performance metrics for all calculations.
        
        Returns:
            Dict[str, float]: Dictionary of operation names and their durations
        """
        return self.performance_metrics.copy()
    
    def transform(self) -> pd.DataFrame:
        """
        Transform the ticker DataFrame to calculate comprehensive technical indicators.
        
        This method calculates momentum, volume, pattern, volatility, price, cyclical, 
        and statistical indicators using TA-Lib with performance monitoring and error handling.
        
        Returns:
            pd.DataFrame: DataFrame with all calculated technical indicators
            
        Raises:
            ValueError: If data validation fails
            ImportError: If TA-Lib is not available
            Exception: If calculation fails
        """
        start_time = time.time()
        
        try:
            logger.info("Starting comprehensive technical indicator calculation")
            
            # Validate TA-Lib availability
            if not TALIB_AVAILABLE:
                raise ImportError("TA-Lib is required for technical indicator calculation but is not installed")
            
            # Prepare data
            self.ticker_df["Volume"] = self.ticker_df["Volume"] * 1.0
            self.ticker_df["Date"] = pd.to_datetime(self.ticker_df["Date"], utc=True)
            
            # Ensure proper data types
            for key in ['Open', 'High', 'Low', 'Close', 'Volume']:
                if key in self.ticker_df.columns:
                    self.ticker_df.loc[:, key] = self.ticker_df[key].astype('double')
            
            # Suppress pandas warnings for chained assignment
            pd.options.mode.chained_assignment = None
            
            # Validate data quality before processing
            self.quality_checker.validate_ohlcv_data(self.ticker_df)
            
            # Process each ticker
            tickers = self.ticker_df['Ticker'].unique() if 'Ticker' in self.ticker_df.columns else ['DEFAULT']
            logger.info(f"Processing {len(tickers)} tickers for technical indicators")
            
            for ticker in tqdm(tickers, desc="Calculating indicators"):
                ticker_start_time = time.time()
                
                try:
                    # Filter data for current ticker
                    if 'Ticker' in self.ticker_df.columns:
                        current_ticker_df = self.ticker_df[self.ticker_df['Ticker'] == ticker].copy()
                    else:
                        current_ticker_df = self.ticker_df.copy()
                    
                    if current_ticker_df.empty:
                        logger.warning(f"No data found for ticker: {ticker}")
                        continue
                    
                    # Calculate all indicator categories
                    momentum_indicators = self._calculate_momentum_indicators(current_ticker_df)
                    volume_indicators = self._calculate_volume_indicators(current_ticker_df)
                    pattern_indicators = self._calculate_pattern_indicators(current_ticker_df)
                    volatility_indicators = self._calculate_volatility_indicators(current_ticker_df)
                    price_indicators = self._calculate_price_indicators(current_ticker_df)
                    cyclical_indicators = self._calculate_cyclical_indicators(current_ticker_df)
                    statistical_indicators = self._calculate_statistical_indicators(current_ticker_df)
                    
                    # Merge all indicators
                    self._merge_indicators(
                        momentum_indicators, volume_indicators, pattern_indicators,
                        volatility_indicators, price_indicators, cyclical_indicators,
                        statistical_indicators, ticker
                    )
                    
                    # Track performance
                    ticker_duration = time.time() - ticker_start_time
                    self.performance_metrics[f"ticker_{ticker}"] = ticker_duration
                    log_performance_metrics(logger, f"process_ticker_{ticker}", ticker_duration)
                    
                except Exception as e:
                    log_error_with_context(logger, e, f"process_ticker_{ticker}", {"ticker": ticker})
                    continue
            
            # Final processing
            if self.transformed_df is not None:
                self.transformed_df = self.transformed_df.sort_values(by=['Date', 'Ticker']).reset_index(drop=True)
                
                # Validate final results
                self._validate_transformed_data()
                
                total_duration = time.time() - start_time
                self.performance_metrics["total_transform"] = total_duration
                
                log_performance_metrics(logger, "technical_indicator_calculation", total_duration, {
                    "final_records": len(self.transformed_df),
                    "final_columns": len(self.transformed_df.columns)
                })
                
                return self.transformed_df
            else:
                raise ValueError("No transformed data generated")
                
        except Exception as e:
            log_error_with_context(logger, e, "transform_method")
            raise

    def safe_merge_or_concat(self, df1: pd.DataFrame, df2: pd.DataFrame, on: list, how: str = 'left') -> pd.DataFrame:
        """
        Safely merge or concatenate two DataFrames, handling empty DataFrames.
        Args:
            df1 (pd.DataFrame): First DataFrame.
            df2 (pd.DataFrame): Second DataFrame.
            on (list): List of columns to merge on.
            how (str): Type of merge to perform ('left', 'right', 'inner', 'outer').
        Returns:    
            pd.DataFrame: Merged DataFrame.
        """
        if not isinstance(df1, pd.DataFrame) or not isinstance(df2, pd.DataFrame):
            logger.error("Both inputs must be pandas DataFrames. Returning the first DataFrame.")
            return df1 if isinstance(df1, pd.DataFrame) else df2 if isinstance(df2, pd.DataFrame) else pd.DataFrame()
        
        if df1.empty or df2.empty:
            logger.warning("One of the DataFrames is empty. Returning the non-empty DataFrame.")
            return df1 if not df1.empty else df2 if not df2.empty else pd.DataFrame()
        
        try:
            overlap = set(df1.columns).intersection(set(df2.columns)) - set(on)
            
            if overlap:
                # check if data is not being duplicated based on Ticker
                df1_tickers = set(df1['Ticker']) if 'Ticker' in df1.columns else set()
                df2_tickers = set(df2['Ticker']) if 'Ticker' in df2.columns else set()
                logger.info("Overlapping Tickers found: %s", df1_tickers.intersection(df2_tickers))
                if df1_tickers == df2_tickers:
                    logger.warning("Overlapping Tickers found in both DataFrames. Merging may lead to unexpected results.")
                    raise ValueError("Overlapping Tickers found in both DataFrames. Please check the data.")

                merged_df = pd.merge(df1, df2, on=on, how=how)
                logger.info(f"DataFrames merged successfully on %s with method %s.", on, how)
                return merged_df
            else:
                logger.info("No overlapping columns found except for the merge keys. Merging DataFrames.")
                merged_df = pd.merge(df1, df2, on=on, how=how)
                logger.info("DataFrames merged successfully.")
                return merged_df
        except Exception as e:
            logger.error(f"Error merging DataFrames: %s", e)
            return df1
    
    def get_talib_momentum_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate momentum indicators using TA-Lib with configurable parameters.
        
        Args:
            df: DataFrame containing stock data with OHLCV columns
            
        Returns:
            pd.DataFrame: DataFrame with momentum indicators added
            
        Raises:
            ImportError: If TA-Lib is not available
            ValueError: If DataFrame is empty or invalid
        """
        if not TALIB_AVAILABLE:
            logger.error("TA-Lib is not available. Returning empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'RSI', 'MACD', 'MACD_Signal', 'MACD_Hist', 
                                         'Stoch_K', 'Stoch_D', 'CCI', 'ROC', 
                                         'MOM', 'TRIX', 'WILLR'])
            
        if df.empty:
            logger.warning("Input DataFrame is empty. Returning an empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'RSI', 'MACD', 'MACD_Signal', 'MACD_Hist', 
                                         'Stoch_K', 'Stoch_D', 'CCI', 'ROC', 
                                         'MOM', 'TRIX', 'WILLR'])
            
        logger.info("Calculating momentum indicators with configurable parameters...")
        
        try:
            # Calculate indicators using configuration parameters
            momentum_rsi = talib.RSI(df['Close'], timeperiod=self.config.rsi_period)
            momentum_macd, momentum_macdsignal, momentum_macdhist = talib.MACD(
                df['Close'], 
                fastperiod=self.config.macd_fast, 
                slowperiod=self.config.macd_slow, 
                signalperiod=self.config.macd_signal
            )
            momentum_stoch_k, momentum_stoch_d = talib.STOCH(
                df['High'], df['Low'], df['Close'], 
                fastk_period=self.config.stoch_k_period, 
                slowk_period=self.config.stoch_k_period, 
                slowk_matype=0, 
                slowd_period=self.config.stoch_d_period, 
                slowd_matype=0
            )
            momentum_cci = talib.CCI(df['High'], df['Low'], df['Close'], timeperiod=self.config.cci_period)
            momentum_roc = talib.ROC(df['Close'], timeperiod=self.config.roc_period)
            momentum_mom = talib.MOM(df['Close'], timeperiod=self.config.mom_period)
            momentum_trix = talib.TRIX(df['Close'], timeperiod=self.config.trix_period)
            momentum_willr = talib.WILLR(df['High'], df['Low'], df['Close'], timeperiod=self.config.willr_period)
            
            # Calculate moving averages using configurable periods
            momentum_indicators = {}
            for period in self.config.sma_periods:
                momentum_indicators[f'SMA{period}'] = talib.SMA(df['Close'], timeperiod=period)
            
            for period in self.config.ema_periods:
                momentum_indicators[f'EMA{period}'] = talib.EMA(df['Close'], timeperiod=period)
        

                    # Create DataFrame with all momentum indicators
            momentum_data = {
                'Date': df['Date'],
                'Ticker': df['Ticker'],
                'RSI': momentum_rsi,
                'MACD': momentum_macd,
                'MACD_Signal': momentum_macdsignal,
                'MACD_Hist': momentum_macdhist,
                'Stoch_K': momentum_stoch_k,
                'Stoch_D': momentum_stoch_d,
                'CCI': momentum_cci,
                'ROC': momentum_roc,
                'MOM': momentum_mom,
                'TRIX': momentum_trix,
                'WILLR': momentum_willr
            }
            
            # Add configurable moving averages
            momentum_data.update(momentum_indicators)
            
            momentum_df = pd.DataFrame(momentum_data)
            
            # Ensure proper data types
            numeric_columns = [col for col in momentum_df.columns if col not in ['Date', 'Ticker']]
            for col in numeric_columns:
                momentum_df[col] = momentum_df[col].astype('double')
            
            momentum_df['Date'] = pd.to_datetime(momentum_df['Date'], utc=True)
            
            logger.info(f"Momentum indicators calculated successfully with {len(momentum_indicators)} moving averages")
            return momentum_df
            
        except Exception as e:
            logger.error(f"Error calculating momentum indicators: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Return empty DataFrame with expected columns
            return pd.DataFrame(columns=['Date', 'Ticker', 'RSI', 'MACD', 'MACD_Signal', 'MACD_Hist', 
                                         'Stoch_K', 'Stoch_D', 'CCI', 'ROC', 
                                         'MOM', 'TRIX', 'WILLR'])
    
    def get_talib_volume_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate volume indicators using TA-Lib with configurable parameters.
        
        Args:
            df: DataFrame containing stock data with OHLCV columns
            
        Returns:
            pd.DataFrame: DataFrame with volume indicators added
            
        Raises:
            ImportError: If TA-Lib is not available
            ValueError: If DataFrame is empty or invalid
        """
        if not TALIB_AVAILABLE:
            logger.error("TA-Lib is not available. Returning empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'OBV', 'AD', 'MFI', 'ADOSC', 'CMF', 'VWAP', 'VPT', 'ADX', 'RVOL'])
            
        if df.empty:
            logger.warning("Input DataFrame is empty. Returning an empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'OBV', 'AD', 'ADOSC'])
        
        logger.info("Calculating volume indicators with configurable parameters...")

        try:
            # Calculate indicators using configuration parameters
            volume_obv = talib.OBV(df['Close'], df['Volume'])
            volume_ad = talib.AD(df['High'], df['Low'], df['Close'], df['Volume'])
            volume_mfi = talib.MFI(df['High'], df['Low'], df['Close'], df['Volume'], timeperiod=self.config.mfi_period)
            volume_adosc = talib.ADOSC(df['High'], df['Low'], df['Close'], df['Volume'], 
                                      fastperiod=self.config.adosc_fast, slowperiod=self.config.adosc_slow)
            volume_cmf = self.chaikin_money_flow(df, period=self.config.cmf_period)
            
            # Calculate VWAP (Volume Weighted Average Price)
            volume_vwap = (df['Close'] * df['Volume']).cumsum() / df['Volume'].cumsum()
            volume_vwap = volume_vwap.fillna(0)
            volume_vwap = volume_vwap.astype('double')
            
            # Calculate VPT (Volume Price Trend)
            volume_vpt = (df['Close'].pct_change() * df['Volume']).fillna(0).cumsum()
            
            # Calculate ADX (Average Directional Index)
            volume_adx = talib.ADX(df['High'], df['Low'], df['Close'], timeperiod=14)
            
            # Calculate RVOL (Relative Volume)
            volume_rvol = df['Volume'] / df['Volume'].rolling(window=20).mean()
            volume_df = pd.DataFrame({
                'Date': df['Date'],
                'Ticker': df['Ticker'],
                'OBV': volume_obv,
                'AD': volume_ad,
                'MFI': volume_mfi,
                'ADOSC': volume_adosc,
                'CMF': volume_cmf,
                'VWAP': volume_vwap,
                'VPT': volume_vpt,
                'ADX': volume_adx,
                'RVOL': volume_rvol
            })
            
            # Handle missing values and ensure proper data types
            volume_df = volume_df.fillna(0)
            numeric_columns = [col for col in volume_df.columns if col not in ['Date', 'Ticker']]
            for col in numeric_columns:
                volume_df[col] = volume_df[col].astype('double')
            
            volume_df['Date'] = pd.to_datetime(volume_df['Date'], utc=True)
            
            logger.info("Volume indicators calculated successfully with configurable parameters.")
            return volume_df
            
        except Exception as e:
            logger.error(f"Error calculating volume indicators: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Return empty DataFrame with expected columns
            return pd.DataFrame(columns=['Date', 'Ticker', 'OBV', 'AD', 'MFI', 'ADOSC', 'CMF', 'VWAP', 'VPT', 'ADX', 'RVOL'])
    
    def chaikin_money_flow(self, df: pd.DataFrame, period: int = 20) -> pd.Series:
        """
        Calculate Chaikin Money Flow (CMF) indicator.
        Args:
            df (pd.DataFrame): DataFrame containing stock data with columns 'High', 'Low', 'Close', 'Volume'.
            period (int): Period for the CMF calculation.
        Returns:
            pd.Series: Series with CMF values.
        """
        if df.empty:
            logger.warning("Input DataFrame is empty. Returning an empty Series.")
            return pd.Series(dtype=float)

        logger.info("Calculating Chaikin Money Flow (CMF) indicator...")
        
        ad = talib.AD(df['High'], df['Low'], df['Close'], df['Volume'])
        cmf = ad.rolling(window=period).sum() / df['Volume'].rolling(window=period).sum()
        
        logger.info("Chaikin Money Flow (CMF) indicator calculated successfully.")
        return cmf
    
    def donchian_channel(self, df: pd.DataFrame, period: int = 20) -> pd.Series:
        """
        Calculate Donchian Channel indicators.
        Args:
            df (pd.DataFrame): DataFrame containing stock data with columns 'High', 'Low', 'Close'.
            period (int): Period for the Donchian Channel.
        Returns:
            pd.Series: Series with Donchian Channel indicators.
        """
        if df.empty:
            logger.warning("Input DataFrame is empty. Returning an empty Series.")
            return pd.Series(dtype=float)

        logger.info("Calculating Donchian Channel indicators...")
        
        donch_upper = df['High'].rolling(window=period).max()
        donch_lower = df['Low'].rolling(window=period).min()
        

        logger.info("Donchian Channel indicators calculated successfully.")
        return donch_upper, donch_lower
    
    def ulcer_index(self, close: pd.Series, period: int = 14) -> pd.Series:
        """
        Calculate Ulcer Index.
        Args:
            close (pd.Series): Series of closing prices.
            period (int): Period for the Ulcer Index calculation.
        Returns:
            pd.Series: Series with Ulcer Index values.
        """
        if close.empty:
            logger.warning("Input Series is empty. Returning an empty Series.")
            return pd.Series(dtype=float)

        logger.info("Calculating Ulcer Index...")
        
        ulcer_index = ((close.rolling(window=period).max() - close) / close.rolling(window=period).max()) ** 2
        
        logger.info("Ulcer Index calculated successfully.")
        return ulcer_index
    
    
    def get_talib_volatility_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate volatility indicators using TA-Lib with configurable parameters.
        
        Args:
            df: DataFrame containing stock data with OHLCV columns
            
        Returns:
            pd.DataFrame: DataFrame with volatility indicators added
            
        Raises:
            ImportError: If TA-Lib is not available
            ValueError: If DataFrame is empty or invalid
        """
        if not TALIB_AVAILABLE:
            logger.error("TA-Lib is not available. Returning empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'ATR', 'NATR', 'TRANGE', 'BBANDS_UPPER', 'BBANDS_MIDDLE', 'BBANDS_LOWER', 'ULCER_INDEX', 'DONCH_UPPER', 'DONCH_LOWER'])
            
        if df.empty:
            logger.warning("Input DataFrame is empty. Returning an empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'ATR', 'NATR', 'TRANGE'])
        
        logger.info("Calculating volatility indicators with configurable parameters...")

        try:
            # Calculate indicators using configuration parameters
            volatility_atr = talib.ATR(df['High'], df['Low'], df['Close'], timeperiod=self.config.atr_period)
            volatility_bbands_upper, volatility_bbands_middle, volatility_bbands_lower = talib.BBANDS(
                df['Close'], 
                timeperiod=self.config.bbands_period, 
                nbdevup=self.config.bbands_dev, 
                nbdevdn=self.config.bbands_dev, 
                matype=0
            )
            volatility_donch_upper, volatility_donch_lower = self.donchian_channel(df, period=self.config.donchian_period)
            volatility_natr = talib.NATR(df['High'], df['Low'], df['Close'], timeperiod=self.config.atr_period)
            volatility_trange = talib.TRANGE(df['High'], df['Low'], df['Close'])
            volatility_ulcer_index = self.ulcer_index(df['Close'], period=self.config.ulcer_period)

            volatility_df = pd.DataFrame({
                'Date': df['Date'],
                'Ticker': df['Ticker'],
                'ATR': volatility_atr,
                'NATR': volatility_natr,
                'TRANGE': volatility_trange,
                'BBANDS_UPPER': volatility_bbands_upper,
                'BBANDS_MIDDLE': volatility_bbands_middle,
                'BBANDS_LOWER': volatility_bbands_lower,
                'ULCER_INDEX': volatility_ulcer_index,
                'DONCH_UPPER': volatility_donch_upper,
                'DONCH_LOWER': volatility_donch_lower
            })
            
            # Ensure proper data types
            numeric_columns = [col for col in volatility_df.columns if col not in ['Date', 'Ticker']]
            for col in numeric_columns:
                volatility_df[col] = volatility_df[col].astype('double')
            
            volatility_df["Date"] = pd.to_datetime(volatility_df["Date"], utc=True)

            logger.info("Volatility indicators calculated successfully with configurable parameters.")
            return volatility_df
            
        except Exception as e:
            logger.error(f"Error calculating volatility indicators: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Return empty DataFrame with expected columns
            return pd.DataFrame(columns=['Date', 'Ticker', 'ATR', 'NATR', 'TRANGE', 'BBANDS_UPPER', 'BBANDS_MIDDLE', 'BBANDS_LOWER', 'ULCER_INDEX', 'DONCH_UPPER', 'DONCH_LOWER'])
    def get_talib_pattern_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate pattern indicators using TA-Lib with configurable sensitivity.
        
        Args:
            df: DataFrame containing stock data with OHLCV columns
            
        Returns:
            pd.DataFrame: DataFrame with pattern indicators added
            
        Raises:
            ImportError: If TA-Lib is not available
            ValueError: If DataFrame is empty or invalid
        """
        if not TALIB_AVAILABLE:
            logger.error("TA-Lib is not available. Returning empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'CDLDOJI', 'CDLHAMMER', 'CDLHANGINGMAN'])
            
        if df.empty:
            logger.warning("Input DataFrame is empty. Returning an empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'CDLDOJI', 'CDLHAMMER', 'CDLHANGINGMAN'])
        
        logger.info("Calculating pattern indicators with configurable sensitivity...")
        
        try:
            # Calculate indicators
            pattern_cd12crows = talib.CDL2CROWS(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cd3blackcrows = talib.CDL3BLACKCROWS(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cd3inside = talib.CDL3INSIDE(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cd3linestrike = talib.CDL3LINESTRIKE(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cd3outside = talib.CDL3OUTSIDE(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cd3starsinsouth = talib.CDL3STARSINSOUTH(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cd3whitesoldiers = talib.CDL3WHITESOLDIERS(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlabandonedbaby = talib.CDLABANDONEDBABY(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlbelthold = talib.CDLBELTHOLD(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlbreakaway = talib.CDLBREAKAWAY(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlclosingmarubozu = talib.CDLCLOSINGMARUBOZU(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlconcealbabyswall = talib.CDLCONCEALBABYSWALL(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlcounterattack = talib.CDLCOUNTERATTACK(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdldarkcloudcover = talib.CDLDARKCLOUDCOVER(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdldoji = talib.CDLDOJI(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdldojistar = talib.CDLDOJISTAR(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlengulfing = talib.CDLENGULFING(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdleveningstar = talib.CDLEVENINGSTAR(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlgravestonedoji = talib.CDLGRAVESTONEDOJI(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlhammer = talib.CDLHAMMER(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlhangingman = talib.CDLHANGINGMAN(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlharami = talib.CDLHARAMI(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlharamicross = talib.CDLHARAMICROSS(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlhighwave = talib.CDLHIGHWAVE(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlhikkake = talib.CDLHIKKAKE(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlhikkakemod = talib.CDLHIKKAKEMOD(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlhomingpigeon = talib.CDLHOMINGPIGEON(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlidentical3crows = talib.CDLIDENTICAL3CROWS(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlinneck = talib.CDLINNECK(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlinvertedhammer = talib.CDLINVERTEDHAMMER(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlladderbottom = talib.CDLLADDERBOTTOM(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdllongleggeddoji = talib.CDLLONGLEGGEDDOJI(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdllongline = talib.CDLLONGLINE(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlmarubozu = talib.CDLMARUBOZU(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlmatchinglow = talib.CDLMATCHINGLOW(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlmathold = talib.CDLMATHOLD(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlmorningdojistar = talib.CDLMORNINGDOJISTAR(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlmorningstar = talib.CDLMORNINGSTAR(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlonneck = talib.CDLONNECK(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlpiercing = talib.CDLPIERCING(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlrickshawman = talib.CDLRICKSHAWMAN(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlrisefall3methods = talib.CDLRISEFALL3METHODS(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlseparatinglines = talib.CDLSEPARATINGLINES(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlshootingstar = talib.CDLSHOOTINGSTAR(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlshortline = talib.CDLSHORTLINE(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlspinningtop = talib.CDLSPINNINGTOP(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlstalledpattern = talib.CDLSTALLEDPATTERN(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlsticksandwich = talib.CDLSTICKSANDWICH(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdltakuri = talib.CDLTAKURI(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdltasukigap = talib.CDLTASUKIGAP(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlthrusting = talib.CDLTHRUSTING(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdltristar = talib.CDLTRISTAR(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlunique3river = talib.CDLUNIQUE3RIVER(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_cdlxsidegap3methods = talib.CDLXSIDEGAP3METHODS(df['Open'], df['High'], df['Low'], df['Close'])
            pattern_df = pd.DataFrame({
                'Date': df['Date'],
                'Ticker': df['Ticker'],
                'CDL2CROWS': pattern_cd12crows,
                'CDL3BLACKCROWS': pattern_cd3blackcrows,
                'CDL3INSIDE': pattern_cd3inside,
                'CDL3LINESTRIKE': pattern_cd3linestrike,
                'CDL3OUTSIDE': pattern_cd3outside,
                'CDL3STARSINSOUTH': pattern_cd3starsinsouth,
                'CDL3WHITESOLDIERS': pattern_cd3whitesoldiers,
                'CDLABANDONEDBABY': pattern_cdlabandonedbaby,
                'CDLBELTHOLD': pattern_cdlbelthold,
                'CDLBREAKAWAY': pattern_cdlbreakaway,
                'CDLCLOSINGMARUBOZU': pattern_cdlclosingmarubozu,
                'CDLCONCEALBABYSWALL': pattern_cdlconcealbabyswall,
                'CDLCOUNTERATTACK': pattern_cdlcounterattack,
                'CDLDARKCLOUDCOVER': pattern_cdldarkcloudcover,
                'CDLDOJI': pattern_cdldoji,
                'CDLDOJISTAR': pattern_cdldojistar,
                'CDLENGULFING': pattern_cdlengulfing,
                'CDLEVENINGSTAR': pattern_cdleveningstar,
                'CDLGRAVESTONEDOJI': pattern_cdlgravestonedoji,
                'CDLHAMMER': pattern_cdlhammer,
                'CDLHANGINGMAN': pattern_cdlhangingman,
                'CDLHARAMI': pattern_cdlharami,
                'CDLHARAMICROSS': pattern_cdlharamicross,
                'CDLHIGHWAVE': pattern_cdlhighwave,
                'CDLHIKKAKE': pattern_cdlhikkake,
                'CDLHIKKAKEMOD': pattern_cdlhikkakemod,
                'CDLHOMINGPIGEON': pattern_cdlhomingpigeon,
                'CDLIDENTICAL3CROWS': pattern_cdlidentical3crows,
                'CDLINNECK': pattern_cdlinneck,
                'CDLINVERTEDHAMMER': pattern_cdlinvertedhammer,
                'CDLLADDERBOTTOM': pattern_cdlladderbottom,
                'CDLLONGLEGGEDDOJI': pattern_cdllongleggeddoji,
                'CDLLONGLINE': pattern_cdllongline,
                'CDLMARUBOZU': pattern_cdlmarubozu,
                'CDLMATCHINGLOW': pattern_cdlmatchinglow,
                'CDLMATHOLD': pattern_cdlmathold,
                'CDLMORNINGDOJISTAR': pattern_cdlmorningdojistar,
                'CDLMORNINGSTAR': pattern_cdlmorningstar,
                'CDLONNECK': pattern_cdlonneck,
                'CDLPIERCING': pattern_cdlpiercing,
                'CDLRICKSHAWMAN': pattern_cdlrickshawman,
                'CDLRISEFALL3METHODS': pattern_cdlrisefall3methods,
                'CDLSEPARATINGLINES': pattern_cdlseparatinglines,
                'CDLSHOOTINGSTAR': pattern_cdlshootingstar,
                'CDLSHORTLINE': pattern_cdlshortline,
                'CDLSPINNINGTOP': pattern_cdlspinningtop,
                'CDLSTALLEDPATTERN': pattern_cdlstalledpattern,
                'CDLSTICKSANDWICH': pattern_cdlsticksandwich,
                'CDLTAKURI': pattern_cdltakuri,
                'CDLTASUKIGAP': pattern_cdltasukigap,
                'CDLTHRUSTING': pattern_cdlthrusting,
                'CDLTRISTAR': pattern_cdltristar,
                'CDLUNIQUE3RIVER': pattern_cdlunique3river,
                'CDLXSIDEGAP3METHODS': pattern_cdlxsidegap3methods
            })

            # Apply pattern sensitivity if configured
            if hasattr(self.config, 'pattern_sensitivity') and self.config.pattern_sensitivity != 1.0:
                pattern_columns = [col for col in pattern_df.columns if col not in ['Date', 'Ticker']]
                for col in pattern_columns:
                    pattern_df[col] = pattern_df[col] * self.config.pattern_sensitivity
            
            # Ensure proper data types
            numeric_columns = [col for col in pattern_df.columns if col not in ['Date', 'Ticker']]
            for col in numeric_columns:
                pattern_df[col] = pattern_df[col].astype('double')
            
            pattern_df["Date"] = pd.to_datetime(pattern_df["Date"], utc=True)
            
            logger.info(f"Pattern indicators calculated successfully with {len(numeric_columns)} patterns.")
            return pattern_df
            
        except Exception as e:
            logger.error(f"Error calculating pattern indicators: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Return empty DataFrame with expected columns
            return pd.DataFrame(columns=['Date', 'Ticker', 'CDLDOJI', 'CDLHAMMER', 'CDLHANGINGMAN'])
    
    def get_talib_price_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate price indicators.
        Args:
            df (pd.DataFrame): DataFrame containing stock data with columns 'Open', 'High', 'Low', 'Close'.
        Returns:
            pd.DataFrame: DataFrame with price indicators added.
        """
        if not TALIB_AVAILABLE:
            logger.error("TA-Lib is not available. Returning empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'Midprice', 'Medprice', 'Typprice', 'Wclprice', 'Avgprice'])
            
        if df.empty:
            logger.warning("Input DataFrame is empty. Returning an empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'Price_Change', 'Price_Change_Percent'])
        
        logger.info("Calculating price indicators...")
        

        midprice_indicator = talib.MIDPRICE(df['High'], df['Low'], timeperiod=14)
        medprice_indicator = talib.MEDPRICE(df['High'], df['Low'])
        typprice_indicator = talib.TYPPRICE(df['High'], df['Low'], df['Close'])
        wclprice_indicator = talib.WCLPRICE(df['High'], df['Low'], df['Close'])
        avgprice_indicator = talib.AVGPRICE(df['Open'], df['High'], df['Low'], df['Close'])

        price_indicators = pd.DataFrame({
            'Date': df['Date'],
            'Ticker': df['Ticker'],
            'Midprice': midprice_indicator,
            'Medprice': medprice_indicator,
            'Typprice': typprice_indicator,
            'Wclprice': wclprice_indicator,
            'Avgprice': avgprice_indicator
        })

        price_indicators = price_indicators.astype({'Midprice': 'double', 'Medprice': 'double',
                                                     'Typprice': 'double', 'Wclprice': 'double',
                                                     'Avgprice': 'double'})
        price_indicators["Date"] = pd.to_datetime(price_indicators["Date"], utc=True)

        logger.info("Price indicators calculated successfully.")
        return price_indicators
    
    def get_talib_cyclical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate cycle indicators using TA-Lib.
        Args:
            df (pd.DataFrame): DataFrame containing stock data with columns like 'Open', 'High', 'Low', 'Close', etc.
        Returns:
            pd.DataFrame: DataFrame with cycle indicators added.
        """
        if not TALIB_AVAILABLE:
            logger.error("TA-Lib is not available. Returning empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'HT_TRENDLINE', 'HT_SINE', 'HT_SINE_LEAD', 'HT_DCPERIOD', 'HT_DCPHASE'])
            
        if df.empty:
            logger.warning("Input DataFrame is empty. Returning an empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'HT_TRENDLINE', 'HT_SINE', 'HT_DCPERIOD', 'HT_DCPHASE'])
        
        logger.info("Calculating cycle indicators...")

        ht_trendline = talib.HT_TRENDLINE(df['Close'])
        ht_sine, ht_sine_lead, = talib.HT_SINE(df['Close'])
        ht_dcperiod = talib.HT_DCPERIOD(df['Close'])
        ht_dcphase = talib.HT_DCPHASE(df['Close'])

        cycle_df = pd.DataFrame({
            'Date': df['Date'],
            'Ticker': df['Ticker'],
            'HT_TRENDLINE': ht_trendline,
            'HT_SINE': ht_sine,
            'HT_SINE_LEAD': ht_sine_lead,
            'HT_DCPERIOD': ht_dcperiod,
            'HT_DCPHASE': ht_dcphase
        })

        cycle_df = cycle_df.astype({'HT_TRENDLINE': 'double', 'HT_SINE': 'double',
                                     'HT_SINE_LEAD': 'double',
                                     'HT_DCPERIOD': 'double', 'HT_DCPHASE': 'double'})
        cycle_df["Date"] = pd.to_datetime(cycle_df["Date"], utc=True)

        logger.info("Cycle indicators calculated successfully.")
        return cycle_df
    
    def get_talib_statistical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate statistical indicators using TA-Lib with configurable parameters.
        
        Args:
            df: DataFrame containing stock data with OHLCV columns
            
        Returns:
            pd.DataFrame: DataFrame with statistical indicators added
            
        Raises:
            ImportError: If TA-Lib is not available
            ValueError: If DataFrame is empty or invalid
        """
        if not TALIB_AVAILABLE:
            logger.error("TA-Lib is not available. Returning empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'STDDEV', 'VAR', 'BETA_VS_SP500', 'ZSCORE_PRICE_NORMALIZED'])
            
        if df.empty:
            logger.warning("Input DataFrame is empty. Returning an empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'STDDEV', 'VAR'])
        
        logger.info("Calculating statistical indicators with configurable parameters...")
        
        try:
            # Calculate indicators using configuration parameters
            stddev_indicator = talib.STDDEV(df['Close'], timeperiod=self.config.stddev_period, nbdev=1)
            var_indicator = talib.VAR(df['Close'], timeperiod=self.config.var_period, nbdev=1)
            beta_vs_sp500 = talib.BETA(df['Close'], df['Close'].rolling(window=self.config.beta_period).mean(), timeperiod=self.config.beta_period)
            zscore_price_normalized = (df['Close'] - df['Close'].rolling(window=self.config.zscore_window).mean()) / df['Close'].rolling(window=self.config.zscore_window).std()

            statistical_df = pd.DataFrame({
                'Date': df['Date'],
                'Ticker': df['Ticker'],
                'STDDEV': stddev_indicator,
                'VAR': var_indicator,
                'BETA_VS_SP500': beta_vs_sp500,
                'ZSCORE_PRICE_NORMALIZED': zscore_price_normalized
            })

            # Ensure proper data types
            numeric_columns = [col for col in statistical_df.columns if col not in ['Date', 'Ticker']]
            for col in numeric_columns:
                statistical_df[col] = statistical_df[col].astype('double')
            
            statistical_df["Date"] = pd.to_datetime(statistical_df["Date"], utc=True)

            logger.info("Statistical indicators calculated successfully with configurable parameters.")
            return statistical_df
            
        except Exception as e:
            logger.error(f"Error calculating statistical indicators: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Return empty DataFrame with expected columns
            return pd.DataFrame(columns=['Date', 'Ticker', 'STDDEV', 'VAR', 'BETA_VS_SP500', 'ZSCORE_PRICE_NORMALIZED'])

    def persist_transformed_data(self, file_path: str) -> None:
        """
        Save DataFrame to a Parquet file.
        Args:
            df (pd.DataFrame): DataFrame to save.
            file_path (str): Path to the Parquet file.
        """
        if self.transformed_df is None or self.transformed_df.empty:
            logger.warning("DataFrame is empty. No data to save.")
            return
        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path))
        try:
            self.transformed_df.to_parquet(file_path, index=False)
            logger.info(f"DataFrame saved to {file_path} successfully.")
        except Exception as e:
            logger.error(f"Error saving DataFrame to Parquet: %s", e)
            
class EnrichMacros:
    """
    Comprehensive macroeconomic data enrichment and transformation class.
    
    This class provides extensive macroeconomic data processing capabilities including
    lagging, rolling calculations, percentage changes, and z-score normalization.
    It includes data validation, quality checks, and performance monitoring.
    
    Attributes:
        macro_df: DataFrame containing macroeconomic data
        config: Configuration for transformation parameters
        transformed_df: DataFrame containing transformed macro data
        quality_checker: DataQualityChecker instance for data validation
        performance_metrics: Dictionary tracking processing performance
    """
    
    def __init__(self, macro_df: pd.DataFrame = None, series_id: str = None, 
                 config: Optional[Dict[str, Any]] = None):
        """
        Initialize the EnrichMacros class with macroeconomic data.
        
        Args:
            macro_df: DataFrame containing macroeconomic data
            series_id: Optional series ID to filter data
            config: Optional configuration dictionary for transformation parameters
            
        Raises:
            ValueError: If data validation fails
        """
        self.config = config or {
            'lag_periods': [30, 60, 90],
            'rolling_windows': [3, 6, 12],
            'pct_change_periods': [1, 2, 3],
            'zscore_window': 90,
            'min_year': 2000
        }
        
        self.quality_checker = DataQualityChecker()
        self.performance_metrics = {}
        self.series_id = series_id
        
        logger.info("Initializing EnrichMacros with macroeconomic data")
        
        # Validate and prepare data
        if macro_df is not None and not macro_df.empty:
            # Standardize column names
            column_mapping = {
                'date': 'Date',
                'value': 'value',
                'series_id': 'series_id'
            }
            
            for old_col, new_col in column_mapping.items():
                if old_col in macro_df.columns and new_col not in macro_df.columns:
                    macro_df = macro_df.rename(columns={old_col: new_col})
            
            # Filter by series if specified
            if series_id and 'series_id' in macro_df.columns:
                macro_df = macro_df[macro_df['series_id'] == series_id]
                logger.info(f"Filtered data for series: {series_id}")
            
            # Validate macro data
            self.quality_checker.validate_macro_data(macro_df)
            
            # Filter by date range
            if 'Date' in macro_df.columns:
                macro_df['Date'] = pd.to_datetime(macro_df['Date'])
                min_year = self.config.get('min_year', 2000)
                macro_df = macro_df[macro_df['Date'].dt.year >= min_year]
            
            self.macro_df = macro_df.copy()
            logger.info(f"EnrichMacros initialized with {len(self.macro_df)} records")
            
            if not self.macro_df.empty:
                logger.info(f"Date range: {self.macro_df['Date'].min()} to {self.macro_df['Date'].max()}")
        else:
            self.macro_df = pd.DataFrame()
            logger.warning("No macro data provided, initializing with empty DataFrame")
        
        self.transformed_df = None
        

    def transform_macro_data(self) -> pd.DataFrame:
        """
        Transform macroeconomic data with comprehensive enrichment and validation.
        
        This method applies lagging, rolling calculations, percentage changes, and
        z-score normalization to macroeconomic data with performance monitoring.
        
        Returns:
            pd.DataFrame: DataFrame with transformed macroeconomic data
            
        Raises:
            ValueError: If data validation fails or DataFrame is empty
            Exception: If transformation fails
        """
        start_time = time.time()
        
        try:
            logger.info("Starting macroeconomic data transformation")
            
            if self.macro_df.empty:
                raise ValueError("Macro DataFrame is empty. Cannot transform empty DataFrame.")
            
            # Validate data quality
            self.quality_checker.validate_macro_data(self.macro_df)
            
            # Sort by date and prepare base DataFrame
            self.macro_df = self.macro_df.sort_values('Date').reset_index(drop=True)
            self.transformed_df = self.macro_df.copy()
            
            # Get available columns for transformation
            available_cols = self.macro_df.columns.tolist()
            numeric_cols = self.macro_df.select_dtypes(include=[np.number]).columns.tolist()
            
            logger.info(f"Available columns for transformation: {available_cols}")
            logger.info(f"Numeric columns for calculations: {numeric_cols}")
            
            # Apply transformations based on data availability
            transformations_applied = []
            
            # 1. Lag transformations
            lag_periods = self.config.get('lag_periods', [30, 60, 90])
            lag_cols = [col for col in ['GDP', 'UNRATE', 'UMCSENT', 'CPIAUCSL', 'FEDFUNDS', 'INDPRO'] 
                       if col in available_cols]
            if lag_cols:
                self.lag_macro_data(lag_cols, lag_periods=lag_periods)
                transformations_applied.append(f"lag_{len(lag_cols)}_cols")
                logger.info(f"Applied lag transformations to {len(lag_cols)} columns")
            
            # 2. Rolling mean transformations
            rolling_windows = self.config.get('rolling_windows', [3, 6, 12])
            rolling_cols = [col for col in ['FEDFUNDS', 'INDPRO', 'GDP', 'UNRATE'] 
                          if col in available_cols]
            if rolling_cols:
                self.rolling_macro_data(rolling_cols, window=rolling_windows)
                transformations_applied.append(f"rolling_{len(rolling_cols)}_cols")
                logger.info(f"Applied rolling mean transformations to {len(rolling_cols)} columns")
            
            # 3. Percentage change transformations
            pct_periods = self.config.get('pct_change_periods', [1, 2, 3])
            pct_cols = [col for col in ['GDP', 'UMCSENT', 'RSAFS', 'INDPRO'] 
                       if col in available_cols]
            if pct_cols:
                self.pct_change_macro_data(pct_cols, periods=pct_periods)
                transformations_applied.append(f"pct_change_{len(pct_cols)}_cols")
                logger.info(f"Applied percentage change transformations to {len(pct_cols)} columns")
            
            # 4. Z-score transformations
            zscore_window = self.config.get('zscore_window', 90)
            zscore_cols = [col for col in ['UNRATE', 'CPIAUCSL', 'UMCSENT'] 
                          if col in available_cols]
            if zscore_cols:
                self.z_score_macro_data(zscore_cols, window=zscore_window)
                transformations_applied.append(f"zscore_{len(zscore_cols)}_cols")
                logger.info(f"Applied z-score transformations to {len(zscore_cols)} columns")
            
            # Validate final results
            self._validate_transformed_macro_data()
            
            # Track performance
            total_duration = time.time() - start_time
            self.performance_metrics["total_macro_transform"] = total_duration
            self.performance_metrics["transformations_applied"] = transformations_applied
            
            log_performance_metrics(logger, "macro_data_transformation", total_duration, {
                "transformations_applied": transformations_applied,
                "final_records": len(self.transformed_df),
                "final_columns": len(self.transformed_df.columns)
            })
            
            return self.transformed_df
            
        except Exception as e:
            log_error_with_context(logger, e, "macro_data_transformation")
            raise

    def lag_macro_data(self, cols: list, lag_periods: list = [30, 60, 90]):
        """
        Lag the macroeconomic data by a specified number of periods.
        This is useful for aligning macroeconomic indicators with stock data.
        
        Args:
            lag_periods (list): List of periods to lag the data.
        """
        if self.macro_df is None or cols is None:
            logger.error("Macro DataFrame or columns to lag are not initialized.")
            return

        for col in cols:
            if col not in self.macro_df.columns:
                logger.warning("Column %s not found in macro DataFrame. Skipping.", col)
                continue
            for period in lag_periods:
                if period <= 0:
                    raise ValueError("Lag period must be a positive integer.")
                lagged_df = self.macro_df[col].shift(period)
                lagged_df.name = f"{col}_lagged_{period}"
                self.transformed_df = pd.concat([self.transformed_df, lagged_df], axis=1)
        logger.info("Macro data lagged by %s periods.", lag_periods)

    def rolling_macro_data(self, cols: list = [], window: list = [3, 6, 12]):
        """
        Apply rolling mean to the macroeconomic data.
        
        Args:
            window (list): The sizes of the moving window.
        """
        if self.macro_df is None:
            logger.error("Macro DataFrame is not initialized.")
            return
        for col in cols:
            if col not in self.macro_df.columns:
                logger.warning("Column %s not found in macro DataFrame. Skipping rolling mean calculation for this column.", col)
                continue
            for w in window:
                if w <= 0:
                    raise ValueError("Window size must be a positive integer.")
                rolling_df = self.macro_df[col].rolling(window=w).mean()
                rolling_df.name = f"{col}_rolling_{w}"
                self.transformed_df = pd.concat([self.transformed_df, rolling_df], axis=1)
        logger.info("Applied rolling mean with window size %s to macro data.", window)

    def pct_change_macro_data(self, cols: list = [], periods: list = [1, 2, 3]):
        """
        Calculate percentage change for the macroeconomic data.
        This is useful for understanding the relative change in macroeconomic indicators.
        Args:
            periods (list): List of periods for which to calculate percentage change.
        """
        if self.macro_df is None:
            logger.error("Macro DataFrame is not initialized.")
            return
        # Go through each column and calculate percentage change
        for col in cols:
            if col not in self.macro_df.columns:
                logger.warning("Column %s not found in macro DataFrame. Skipping percentage change calculation for this column.", col)
                continue
            for period in periods:
                if period <= 0:
                    raise ValueError("Period must be a positive integer.")
                pct_change_df = self.macro_df[col].pct_change(periods=period)
                pct_change_df.name = f"{col}_pct_change"
                self.transformed_df = pd.concat([self.transformed_df, pct_change_df], axis=1)
        logger.info("Calculated percentage change with periods %s for macro data.", periods)

    def z_score_macro_data(self, cols: list = [], window: int = 90):
        """
        Calculate the z-score for the macroeconomic data.
        This is useful for standardizing the macroeconomic indicators.
        """
        if self.macro_df is None:
            logger.error("Macro DataFrame is not initialized.")
            return
        
        if window <= 0:
            raise ValueError("Window size must be a positive integer.")
        for col in cols:
            if col not in self.macro_df.columns:
                logger.warning("Column %s not found in macro DataFrame. Skipping z-score calculation for this column.", col)
                continue
            roll = self.macro_df[col].rolling(window=window)
            z_score_df = (self.macro_df[col] - roll.mean()) / roll.std()
            z_score_df.name = f"{col}_z_score"
            self.transformed_df = pd.concat([self.transformed_df, z_score_df], axis=1)
        logger.info("Calculated z-score for macro data.")
        
    def persist_transformed_data(self, path: str = "data/macro_data.parquet"):
        """
        Save the transformed macroeconomic data to a parquet file.
        
        Args:
            path (str): The file path where the transformed data will be saved.
        """
        if self.transformed_df is None:
            logger.error("Transformed DataFrame is empty. Cannot save to file.")
            return
        
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        self.transformed_df.to_parquet(path, index=False)
        logger.info("Transformed macroeconomic data saved to %s", path)
    
    def _validate_transformed_macro_data(self) -> None:
        """
        Validate the final transformed macroeconomic dataset for quality and completeness.
        
        Raises:
            ValueError: If validation fails
        """
        if self.transformed_df is None or self.transformed_df.empty:
            raise ValueError("Transformed macro DataFrame is None or empty")
        
        # Check for required columns
        required_columns = ['Date']
        missing_columns = [col for col in required_columns if col not in self.transformed_df.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns in transformed macro data: {missing_columns}")
        
        # Check for null values in critical columns
        critical_columns = ['Date']
        for col in critical_columns:
            null_count = self.transformed_df[col].isnull().sum()
            if null_count > 0:
                logger.warning(f"Found {null_count} null values in critical column: {col}")
        
        # Check for reasonable number of transformed columns
        original_cols = set(self.macro_df.columns)
        transformed_cols = set(self.transformed_df.columns)
        new_cols = transformed_cols - original_cols
        
        if len(new_cols) < 5:
            logger.warning(f"Only {len(new_cols)} new columns created, expected more transformations")
        
        # Check for date consistency
        if 'Date' in self.transformed_df.columns:
            date_range = self.transformed_df['Date'].max() - self.transformed_df['Date'].min()
            if date_range.days < 30:
                logger.warning(f"Transformed data covers only {date_range.days} days")
        
        log_data_quality_metrics(logger, "transformed_macro_data_validation", len(self.transformed_df), len(self.transformed_df.columns))
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """
        Get performance metrics for all macro data processing operations.
        
        Returns:
            Dict[str, Any]: Dictionary of operation names and their metrics
        """
        return self.performance_metrics.copy()
        