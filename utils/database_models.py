#!/usr/bin/env python3
"""
 SQLAlchemy Models for CandleThrob Financial Data Pipeline
======================================================================

This module contains -level database models for storing ticker data and 
macroeconomic data using Oracle database with SQLAlchemy ORM. Supports bulk inserts, 
incremental loading, and comprehensive data validation.

 Features:
- Advanced error handling and retry logic with exponential backoff
- Performance monitoring and metrics collection
- Data quality validation and scoring
- Memory usage optimization
- Advanced logging and monitoring
- Data lineage tracking and audit trails
- Quality gates and validation checkpoints
- Resource monitoring and optimization
- Modular design with configurable model management
- Advanced error categorization and handling
- Comprehensive data validation and type checking
- -grade bulk operations and optimization

Author: Adetunji Fasiku
Version: 3.0.0
Last Updated: 2025-07-14
"""

import os
import sys
import time
import psutil
import threading
import traceback
import pandas as pd
import numpy as np
from datetime import datetime, date as date_type
from typing import Optional, Dict, Any, List, Union, Tuple
from dataclasses import dataclass, field
from functools import wraps
from contextlib import contextmanager
from CandleThrob.core.monitors import UniversalPerformanceMonitor as PerformanceMonitor

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from CandleThrob.utils.oracle_conn import OracleDB
from CandleThrob.core.logging import get_ingestion_logger, get_transform_logger, log_ingestion_start, log_ingestion_success, log_ingestion_error, log_rate_limiting, log_batch_progress

# Get logger for this module
logger = get_database_logger(__name__)


def validate_and_cap_price_values(value: float, max_value: float = 999999.9999, decimal_places: int = 4) -> float:
    """
    Validate and cap price values to fit Oracle NUMBER(10,4) precision.

    Args:
        value (float): The price value to validate
        max_value (float): Maximum allowed value (default: 999999.9999 for NUMBER(10,4))
        decimal_places (int): Number of decimal places to round to (default: 4)

    Returns:
        float: Capped and rounded value that fits database precision

    Example:
        >>> validate_and_cap_price_values(2267744.534164718)
        999999.9999
        >>> validate_and_cap_price_values(123.456789)
        123.4568
    """
    if pd.isna(value) or value is None:
        return value

    # Cap the value to maximum allowed
    capped_value = min(float(value), max_value)

    # Round to specified decimal places
    rounded_value = round(capped_value, decimal_places)

    return rounded_value


def validate_price_dataframe(df: pd.DataFrame, price_columns: List[str] = None) -> pd.DataFrame:
    """
    Validate and cap all price columns in a DataFrame to fit Oracle NUMBER(10,4) precision.

    Args:
        df (pd.DataFrame): DataFrame containing price data
        price_columns (List[str]): List of price column names to validate

    Returns:
        pd.DataFrame: DataFrame with validated and capped price values

    Example:
        >>> df = validate_price_dataframe(ticker_df, ['open_price', 'high_price', 'low_price', 'close_price'])
    """
    if price_columns is None:
        price_columns = ['open_price', 'high_price', 'low_price', 'close_price', 'vwap']

    df_clean = df.copy()
    max_price_value = 999999.9999

    for col in price_columns:
        if col in df_clean.columns:
            # Count values that will be capped
            original_count = len(df_clean[df_clean[col] > max_price_value])
            if original_count > 0:
                logger.warning(f"Capping {original_count} {col} values that exceed {max_price_value}")

            # Apply validation and capping
            df_clean[col] = df_clean[col].apply(lambda x: validate_and_cap_price_values(x))

    return df_clean


class OracleDataTypeConverter:
    """
    Utility class for converting DataFrame data types to be compatible with Oracle database.
    This class provides methods to handle common data type conversion issues when working with Oracle.
    """

    @staticmethod
    def convert_datatypes_for_oracle(df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert DataFrame data types to be compatible with Oracle database.

        This method handles:
        - NaN, inf, -inf values (converts to None)
        - Pandas Timestamp objects (converts to Python datetime)
        - Numeric types (ensures proper Python native types)
        - Object columns that might contain timestamps

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with Oracle-compatible data types
        """
        import numpy as np
        from datetime import datetime

        df_converted = df.copy()

        for col in df_converted.columns:
            # Handle numeric columns
            if df_converted[col].dtype in ['float64', 'float32', 'int64', 'int32']:
                # Replace inf/-inf with None using a safer approach
                try:
                    df_converted[col] = df_converted[col].replace([np.inf, -np.inf], None)
                except (ValueError, TypeError):
                    # Handle cases where replace might fail
                    pass

                # Replace NaN with None
                df_converted[col] = df_converted[col].where(pd.notna(df_converted[col]), None)

                # Convert to Python native types for better Oracle compatibility
                if df_converted[col].dtype in ['float64', 'float32']:
                    df_converted[col] = df_converted[col].astype('object')
                    # Convert non-None values to Python float using a safer approach
                    def safe_float_convert(val):
                        if val is None or pd.isna(val):
                            return None
                        try:
                            return float(val)
                        except (ValueError, TypeError, OverflowError):
                            return None

                    df_converted[col] = df_converted[col].apply(safe_float_convert)

            # Handle datetime columns
            elif df_converted[col].dtype == 'datetime64[ns]' or 'datetime' in str(df_converted[col].dtype):
                # Convert pandas timestamps to Python datetime objects
                df_converted[col] = df_converted[col].apply(
                    lambda x: x.to_pydatetime().replace(tzinfo=None) if pd.notna(x) and hasattr(x, 'to_pydatetime') else None
                )

            # Handle object columns that might contain timestamps
            elif df_converted[col].dtype == 'object':
                # Check if this column contains datetime objects
                sample_vals = df_converted[col].dropna()
                if not sample_vals.empty:
                    sample_val = sample_vals.iloc[0]
                    if hasattr(sample_val, 'to_pydatetime'):
                        # Convert pandas timestamps to Python datetime objects
                        df_converted[col] = df_converted[col].apply(
                            lambda x: x.to_pydatetime().replace(tzinfo=None) if pd.notna(x) and hasattr(x, 'to_pydatetime') else x
                        )
                    elif isinstance(sample_val, pd.Timestamp):
                        # Handle pandas Timestamp objects
                        df_converted[col] = df_converted[col].apply(
                            lambda x: x.to_pydatetime().replace(tzinfo=None) if isinstance(x, pd.Timestamp) else x
                        )

        logger.debug(f"Converted DataFrame data types for Oracle compatibility - Shape: {df_converted.shape}")
        return df_converted

    @staticmethod
    def add_audit_timestamps(df: pd.DataFrame) -> pd.DataFrame:
        """
        Add created_at and updated_at timestamp columns with Oracle-compatible datetime objects.

        Note: This method is deprecated since database tables have DEFAULT CURRENT_TIMESTAMP.
        Use remove_audit_timestamps() instead to exclude these columns from INSERT statements.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with audit timestamp columns added
        """
        from datetime import datetime, timezone

        df_with_timestamps = df.copy()

        # Create timezone-aware datetime and then remove timezone for Oracle compatibility
        current_time = datetime.now(timezone.utc).replace(tzinfo=None)

        df_with_timestamps['created_at'] = current_time
        df_with_timestamps['updated_at'] = current_time

        return df_with_timestamps

    @staticmethod
    def remove_audit_timestamps(df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove created_at and updated_at columns from DataFrame since they have database defaults.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame without audit timestamp columns
        """
        df_clean = df.copy()

        # Remove timestamp columns that have database defaults
        timestamp_columns = ['created_at', 'updated_at']
        existing_timestamp_cols = [col for col in timestamp_columns if col in df_clean.columns]

        if existing_timestamp_cols:
            df_clean = df_clean.drop(columns=existing_timestamp_cols)
            logger.debug(f"Removed timestamp columns with database defaults: {existing_timestamp_cols}")

        return df_clean

    @staticmethod
    def apply_schema_precision(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Apply schema-specific numeric precision constraints to DataFrame columns.

        This method ensures all numeric values are properly rounded according to the
        database schema constraints to prevent Oracle precision errors.

        Args:
            df: Input DataFrame
            table_name: Name of the target table to get precision constraints

        Returns:
            DataFrame with properly rounded numeric values
        """
        import numpy as np

        df_precision = df.copy()

        # Define schema precision constraints for each table
        schema_constraints = {
            'TICKERS': {
                'open_price': {'type': 'NUMBER', 'precision': 10, 'scale': 4},
                'high_price': {'type': 'NUMBER', 'precision': 10, 'scale': 4},
                'low_price': {'type': 'NUMBER', 'precision': 10, 'scale': 4},
                'close_price': {'type': 'NUMBER', 'precision': 10, 'scale': 4},
                'vwap': {'type': 'NUMBER', 'precision': 10, 'scale': 4},
                'volume': {'type': 'NUMBER', 'precision': None, 'scale': 0},
                'num_transactions': {'type': 'NUMBER', 'precision': None, 'scale': 0},
                'marketcap': {'type': 'NUMBER', 'precision': 20, 'scale': 2}
            },
            'MACRO_DATA': {
                'value': {'type': 'NUMBER', 'precision': 15, 'scale': 6}
            },
            'TRANSFORMED_TICKERS': {
                # All technical indicators use BINARY_DOUBLE (no specific precision limit)
                # But we'll apply reasonable rounding for consistency
                'open_price': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'high_price': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'low_price': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'close_price': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'volume': {'type': 'NUMBER', 'precision': None, 'scale': 0},
                'marketcap': {'type': 'NUMBER', 'precision': 20, 'scale': 2},
                # Technical indicators - reasonable precision for financial calculations
                'rsi': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 6},
                'macd': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'macd_signal': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'macd_hist': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'stoch_k': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 6},
                'stoch_d': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 6},
                'cci': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 6},
                'roc': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'mom': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'trix': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'willr': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 6},
                # Moving averages
                'sma10': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'sma20': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'sma50': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'sma100': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'sma200': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'ema10': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'ema20': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'ema50': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'ema100': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'ema200': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                # Volume indicators
                'obv': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 2},
                'ad': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 2},
                'mfi': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 6},
                'adosc': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 2},
                'cmf': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'vwap': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'vpt': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 2},
                'adx': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 6},
                'rvol': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 6},
                # Volatility indicators
                'atr': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'natr': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 6},
                'trange': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                # Bollinger Bands
                'bbands_upper': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'bbands_middle': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'bbands_lower': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                # Other indicators
                'ulcer_index': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'donch_upper': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'donch_lower': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                # Price indicators
                'midprice': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'medprice': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'typprice': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'wclprice': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'avgprice': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                # Returns
                'return_1d': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'return_3d': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'return_7d': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'return_30d': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'return_90d': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'return_365d': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                # Hilbert Transform
                'ht_trendline': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'ht_sine': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'ht_sine_lead': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'ht_dcperiod': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 6},
                'ht_dcphase': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 6},
                # Statistical indicators
                'stddev': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'var': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'beta_vs_sp500': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'zscore_price_normalized': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8}
                # Note: Candlestick patterns (cdl*) are typically 0, 100, or -100, so scale=0 is appropriate
            },
            'TRANSFORMED_MACRO_DATA': {
                'value': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8},
                'transformed_value': {'type': 'BINARY_DOUBLE', 'precision': None, 'scale': 8}
            }
        }

        # Get constraints for the specified table
        table_constraints = schema_constraints.get(table_name.upper(), {})

        if not table_constraints:
            logger.debug(f"No precision constraints defined for table {table_name}")
            return df_precision

        # Apply precision constraints to each column
        precision_applied = 0
        for col in df_precision.columns:
            if col in table_constraints and df_precision[col].dtype in ['float64', 'float32', 'int64', 'int32', 'object']:
                constraint = table_constraints[col]
                scale = constraint['scale']

                if scale is not None:
                    # Use proper decimal rounding to avoid floating-point precision issues
                    from decimal import Decimal, ROUND_HALF_UP

                    def round_decimal(value):
                        # Handle all possible NaN/None cases
                        if value is None:
                            return None
                        if pd.isna(value):
                            return None
                        if isinstance(value, str) and value.lower() in ['nan', 'none', 'null', '']:
                            return None

                        try:
                            # Convert to float first to check if it's finite
                            float_val = float(value)
                            import math
                            if not math.isfinite(float_val):
                                return None
                            if math.isnan(float_val):
                                return None

                            # Convert to Decimal for precise rounding
                            decimal_val = Decimal(str(float_val))
                            rounded_decimal = decimal_val.quantize(
                                Decimal('0.' + '0' * scale),
                                rounding=ROUND_HALF_UP
                            )
                            result = float(rounded_decimal)

                            # Final check to ensure result is not NaN
                            if math.isnan(result):
                                return None

                            return result
                        except (ValueError, TypeError, OverflowError):
                            return None

                    # Apply decimal rounding to the column
                    df_precision[col] = df_precision[col].apply(round_decimal)
                    precision_applied += 1

                    logger.debug(f"Applied precision constraint to {col}: scale={scale}")

        if precision_applied > 0:
            logger.info(f"Applied precision constraints to {precision_applied} columns for table {table_name}")

        return df_precision

    @staticmethod
    def final_nan_cleanup(df: pd.DataFrame) -> pd.DataFrame:
        """
        Final cleanup to ensure no NaN values remain in the DataFrame.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with all NaN values replaced with None
        """
        df_clean = df.copy()

        # Replace all possible NaN representations with None
        import math

        def clean_value(val):
            if val is None:
                return None
            if pd.isna(val):
                return None
            if isinstance(val, str):
                if val.lower() in ['nan', 'none', 'null', '']:
                    return None
                # Check if string representation of nan
                if val == 'nan':
                    return None
            try:
                # Convert to float to check for NaN/inf
                if isinstance(val, (int, float)):
                    if math.isnan(val):
                        return None
                    if not math.isfinite(val):
                        return None
                # Check if it's numpy nan
                import numpy as np
                if isinstance(val, np.floating) and np.isnan(val):
                    return None
                # Check string representation
                if str(val).lower() == 'nan':
                    return None
                return val
            except (ValueError, TypeError, OverflowError):
                # If we can't process it, check string representation
                if str(val).lower() == 'nan':
                    return None
                return val

        # Apply cleanup to all columns
        for col in df_clean.columns:
            if df_clean[col].dtype in ['float64', 'float32', 'object']:
                df_clean[col] = df_clean[col].apply(clean_value)

        logger.debug("Applied final NaN cleanup to DataFrame")
        return df_clean

    @staticmethod
    def clean_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
        """
        Specialized cleanup for technical indicator columns that often contain NaN values.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with technical indicator NaN values replaced with None
        """
        df_clean = df.copy()

        # Define technical indicator columns that commonly have NaN values
        technical_columns = [
            'RSI', 'MACD', 'MACD_Signal', 'MACD_Hist', 'Stoch_K', 'Stoch_D', 'CCI', 'ROC', 'MOM', 'TRIX', 'WILLR',
            'SMA10', 'SMA20', 'SMA50', 'SMA100', 'SMA200', 'EMA10', 'EMA20', 'EMA50', 'EMA100', 'EMA200',
            'ATR', 'NATR', 'TRANGE', 'BBANDS_UPPER', 'BBANDS_MIDDLE', 'BBANDS_LOWER', 'ULCER_INDEX',
            'DONCH_UPPER', 'DONCH_LOWER', 'Midprice', 'HT_TRENDLINE', 'HT_SINE', 'HT_SINE_LEAD',
            'HT_DCPERIOD', 'HT_DCPHASE', 'STDDEV', 'VAR', 'BETA_VS_SP500', 'ZSCORE_PRICE_NORMALIZED',
            'rsi', 'macd', 'macd_signal', 'macd_hist', 'stoch_k', 'stoch_d', 'cci', 'roc', 'mom', 'trix', 'willr',
            'sma10', 'sma20', 'sma50', 'sma100', 'sma200', 'ema10', 'ema20', 'ema50', 'ema100', 'ema200',
            'atr', 'natr', 'trange', 'bbands_upper', 'bbands_middle', 'bbands_lower', 'ulcer_index',
            'donch_upper', 'donch_lower', 'midprice', 'ht_trendline', 'ht_sine', 'ht_sine_lead',
            'ht_dcperiod', 'ht_dcphase', 'stddev', 'var', 'beta_vs_sp500', 'zscore_price_normalized'
        ]

        import numpy as np
        import math

        def aggressive_nan_clean(val):
            # Multiple checks for different types of NaN representations
            if val is None:
                return None
            if pd.isna(val):
                return None
            try:
                # Check for numpy NaN
                if isinstance(val, np.floating) and np.isnan(val):
                    return None
                # Check for Python float NaN
                if isinstance(val, float) and math.isnan(val):
                    return None
                # Check string representation
                val_str = str(val).lower()
                if val_str in ['nan', 'none', 'null', '']:
                    return None
                # Try to convert to float and check
                float_val = float(val)
                if math.isnan(float_val) or not math.isfinite(float_val):
                    return None
                return val
            except (ValueError, TypeError, OverflowError):
                # If conversion fails, check if it's a string representation of NaN
                if str(val).lower() == 'nan':
                    return None
                return val

        # Apply aggressive cleaning to technical indicator columns
        cleaned_count = 0
        for col in technical_columns:
            if col in df_clean.columns:
                original_nan_count = df_clean[col].apply(lambda x: str(x).lower() == 'nan').sum()
                if original_nan_count > 0:
                    df_clean[col] = df_clean[col].apply(aggressive_nan_clean)
                    cleaned_count += original_nan_count
                    logger.debug(f"Cleaned {original_nan_count} NaN values from technical indicator column: {col}")

        if cleaned_count > 0:
            logger.info(f"Cleaned {cleaned_count} NaN values from technical indicator columns")

        return df_clean

    @staticmethod
    def auto_clean_numerical_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Automatically detect and clean ALL numerical columns of NaN values.
        This method identifies numerical columns by dtype and applies aggressive NaN cleanup.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with ALL NaN values in numerical columns replaced with None
        """
        df_clean = df.copy()
        import numpy as np
        import math

        # Define numerical dtypes to clean
        numerical_dtypes = ['float64', 'float32', 'int64', 'int32', 'object']

        def nuclear_clean(val):
            """Nuclear option: if it's not clearly a valid number, make it None"""
            if val is None:
                return None

            # Handle string cases
            if isinstance(val, str):
                val_lower = val.lower().strip()
                if val_lower in ['nan', 'none', 'null', '', 'inf', '-inf', 'infinity', '-infinity']:
                    return None
                # Try to convert string to float
                try:
                    float_val = float(val)
                    if not math.isfinite(float_val):
                        return None
                    return float_val
                except (ValueError, TypeError, OverflowError):
                    return None

            # Handle pandas/numpy NaN
            try:
                if pd.isna(val):
                    return None
                if isinstance(val, np.floating) and np.isnan(val):
                    return None
                if isinstance(val, (int, float)):
                    if math.isnan(val):
                        return None
                    if not math.isfinite(val):
                        return None
                    return val
                # If we can't determine what it is, try to convert to float
                float_val = float(val)
                if not math.isfinite(float_val):
                    return None
                return float_val
            except (ValueError, TypeError, OverflowError, AttributeError):
                # If all else fails, check string representation
                if str(val).lower().strip() in ['nan', 'none', 'null', '']:
                    return None
                return None  # When in doubt, make it None

        def is_likely_numerical_column(col_name, col_data):
            """Determine if a column is likely numerical based on name and content"""
            # Skip obvious categorical columns
            categorical_indicators = ['ticker', 'date', 'series_id', 'transformation_type',
                                    'transformation_params', 'units', 'title', 'symbol']

            col_lower = col_name.lower()
            if any(indicator in col_lower for indicator in categorical_indicators):
                return False

            # Check if column dtype suggests it's numerical
            if str(col_data.dtype) in numerical_dtypes:
                # Sample a few values to see if they look numerical
                sample_vals = col_data.dropna().head(5)
                if len(sample_vals) == 0:
                    return True  # Empty column, treat as numerical

                numerical_count = 0
                for val in sample_vals:
                    try:
                        float(val)
                        numerical_count += 1
                    except (ValueError, TypeError):
                        pass

                # If most sampled values are numerical, treat column as numerical
                return numerical_count >= len(sample_vals) * 0.5

            return False

        # Apply nuclear cleaning to automatically detected numerical columns
        total_cleaned = 0
        numerical_columns_found = []

        for col in df_clean.columns:
            if is_likely_numerical_column(col, df_clean[col]):
                numerical_columns_found.append(col)

                # Count problematic values before cleaning
                before_count = 0
                for val in df_clean[col]:
                    val_str = str(val).lower().strip()
                    if val_str == 'nan' or pd.isna(val):
                        before_count += 1

                if before_count > 0:
                    df_clean[col] = df_clean[col].apply(nuclear_clean)
                    total_cleaned += before_count
                    logger.info(f"Auto-cleaned {before_count} problematic values from numerical column: {col}")

        if total_cleaned > 0:
            logger.warning(f"AUTO NUMERICAL CLEANUP: Cleaned {total_cleaned} total problematic values across {len(numerical_columns_found)} numerical columns")
            logger.debug(f"Numerical columns processed: {numerical_columns_found}")

        return df_clean

@dataclass
class ModelConfig:
    """ configuration for database models."""
    
    # Performance settings
    max_insert_batch_size: int = 10000
    max_memory_usage_mb: float = 1024.0
    max_retries: int = 3
    retry_delay_seconds: float = 1.0
    
    # Data validation settings
    enable_data_validation: bool = True
    enable_type_checking: bool = True
    enable_null_validation: bool = True
    min_data_quality_score: float = 0.8
    
    # Advanced features
    enable_audit_trail: bool = True
    enable_performance_monitoring: bool = True
    enable_memory_optimization: bool = True
    enable_data_lineage: bool = True
    
    def __post_init__(self):
        """Validate configuration parameters."""
        if self.max_insert_batch_size <= 0:
            raise ValueError("max_insert_batch_size must be positive")
        if self.max_memory_usage_mb <= 0:
            raise ValueError("max_memory_usage_mb must be positive")
        if not 0.0 <= self.min_data_quality_score <= 1.0:
            raise ValueError("min_data_quality_score must be between 0.0 and 1.0")
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")


class DataValidator:
    """ data validation for database models."""
    
    @staticmethod
    def validate_dataframe(df: pd.DataFrame, required_columns: List[str], 
                          numeric_columns: List[str] = None) -> Tuple[bool, float, List[str]]:
        """Validate DataFrame quality and structure."""
        issues = []
        quality_score = 1.0
        
        if df is None or df.empty:
            issues.append("DataFrame is empty or None")
            quality_score = 0.0
            return False, quality_score, issues
        
        # Check required columns
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            issues.append(f"Missing required columns: {missing_cols}")
            quality_score -= 0.3
        
        # Check for null values in required columns
        if numeric_columns:
            for col in numeric_columns:
                if col in df.columns:
                    null_count = df[col].isnull().sum()
                    if null_count > 0:
                        issues.append(f"Null values found in {col}: {null_count}")
                        quality_score -= 0.1
        
        # Check data types
        if numeric_columns:
            for col in numeric_columns:
                if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
                    issues.append(f"Non-numeric data type in {col}")
                    quality_score -= 0.2
        
        # Check for negative values in price columns
        price_columns = ['open', 'high', 'low', 'close', 'open_price', 'high_price', 'low_price', 'close_price']
        for col in price_columns:
            if col in df.columns:
                negative_count = (df[col] < 0).sum()
                if negative_count > 0:
                    issues.append(f"Negative values found in {col}: {negative_count}")
                    quality_score -= 0.2
        
        quality_score = max(0.0, quality_score)
        return quality_score >= 0.8, quality_score, issues

def retry_on_failure(max_retries: int = 3, delay_seconds: float = 1.0):
    """ retry decorator with exponential backoff."""
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

class TickerData:
    """
     model for storing ticker data (OHLCV) with  features.
    
    This class provides -level capabilities for storing ticker data with
    advanced bulk insert operations, incremental loading, and comprehensive data validation.
    
    Attributes:
        config (ModelConfig):  configuration
        performance_monitor (PerformanceMonitor): Performance monitor
        data_validator (DataValidator): Data validator
        __tablename__ (str): Database table name
    """
    
    __tablename__ = 'TICKER_DATA'
    
    def __init__(self, config: Optional[ModelConfig] = None):
        """
        Initialize the TickerData class with  features.
        
        Args:
            config (Optional[ModelConfig]):  configuration
            
        Example:
            >>> model = TickerData()
            >>> model = TickerData(ModelConfig())
        """
        self.config = config or ModelConfig()
        self.performance_monitor = PerformanceMonitor("default")
        self.data_validator = DataValidator()
        
        logger.info("TickerData initialized with  configuration")
    
    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def create_table(self, conn):
        """
        Create the table with  features and comprehensive error handling.

        Args:
            conn: SQLAlchemy database connection for table creation

        Example:
            >>> model = TickerData()
            >>> with db.establish_connection() as conn:
            >>>     model.create_table(conn)
        """
        operation_name = "create_table_ticker_data"
        start_time = time.time()
        
        try:
            from sqlalchemy import text
            conn.execute(text(f"""
                BEGIN
                    EXECUTE IMMEDIATE 'CREATE TABLE {self.__tablename__} (
                        id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                        ticker VARCHAR2(10) NOT NULL,
                        trade_date DATE NOT NULL,
                        open_price NUMBER(10,4) NOT NULL,
                        high_price NUMBER(10,4) NOT NULL,
                        low_price NUMBER(10,4) NOT NULL,
                        close_price NUMBER(10,4) NOT NULL,
                        volume NUMBER NOT NULL,
                        vwap NUMBER(10,4),
                        num_transactions NUMBER,
                        marketcap NUMBER(20,2),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -955 THEN  -- Table already exists
                            RAISE;
                        END IF;
                END;
            """))
            
            # Create indexes for performance
            conn.execute(text(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{self.__tablename__}_ticker ON {self.__tablename__} (ticker)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN  -- Index already exists
                        RAISE;
                    END IF;
            END;
            """))

            conn.execute(text(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{self.__tablename__}_date ON {self.__tablename__} (trade_date)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN  -- Index already exists
                        RAISE;
                    END IF;
            END;
            """))

            conn.execute(text(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{self.__tablename__}_ticker_date ON {self.__tablename__} (ticker, trade_date)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN  -- Index already exists
                        RAISE;
                    END IF;
            END;
            """))
            
            log_database_operation("create_table", self.__tablename__)
            logger.info(f"Table {self.__tablename__} created/verified successfully")
            
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, True)
            
        except Exception as e:
            error_msg = f"Error creating table {self.__tablename__}: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            log_database_operation("create_table", self.__tablename__, error=error_msg)
            
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, False, error_msg)
            raise

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def data_exists(self, conn, ticker: Optional[str] = None) -> bool:
        """
        Check if data exists in the table with error handling.

        Args:
            conn: SQLAlchemy database connection
            ticker (Optional[str]): Specific ticker to check

        Returns:
            bool: True if data exists, False otherwise

        Example:
            >>> model = TickerData()
            >>> exists = model.data_exists(conn, "AAPL")
        """
        operation_name = "data_exists_ticker_data"
        start_time = time.time()
        
        try:
            from sqlalchemy import text
            if ticker:
                result_proxy = conn.execute(text(f"SELECT 1 FROM {self.__tablename__} WHERE ticker = :ticker"), {"ticker": ticker})
                result = result_proxy.fetchone() is not None
            else:
                result_proxy = conn.execute(text(f"SELECT 1 FROM {self.__tablename__}"))
                result = result_proxy.fetchone() is not None
            
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, True, data_exists=result)
            return result
            
        except Exception as e:
            error_msg = f"Error checking data existence: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, False, error_msg)
            return False

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def get_last_date(self, conn, ticker: Optional[str] = None) -> Optional[date_type]:
        """
        Get the last date for which data exists with  error handling.

        Args:
            conn: SQLAlchemy database connection
            ticker (Optional[str]): Specific ticker to check

        Returns:
            Optional[date_type]: Last date or None if no data exists

        Example:
            >>> model = TickerData()
            >>> last_date = model.get_last_date(conn, "AAPL")
        """
        operation_name = "get_last_date_ticker_data"
        start_time = time.time()
        
        try:
            from sqlalchemy import text
            if ticker:
                result_proxy = conn.execute(text(f"SELECT MAX(trade_date) FROM {self.__tablename__} WHERE ticker = :ticker"), {"ticker": ticker})
            else:
                result_proxy = conn.execute(text(f"SELECT MAX(trade_date) FROM {self.__tablename__}"))

            result = result_proxy.fetchone()
            last_date = result[0] if result else None
            
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, True, last_date=last_date)
            return last_date
            
        except Exception as e:
            error_msg = f"Error getting last date: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, False, error_msg)
            return None

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def insert_data(self, conn, df: pd.DataFrame):
        """
        Insert data using bulk operations with  features and comprehensive validation.
        
        Args:
            conn: Database connection
            df (pd.DataFrame): DataFrame to insert
            
        Example:
            >>> model = TickerData()
            >>> model.insert_data(conn, ticker_df)
        """
        operation_name = "insert_data_ticker_data"
        start_time = time.time()
        
        try:
            if df is None or df.empty:
                logger.warning("No data to insert")
                return
            
            # Data validation
            required_columns = ['ticker', 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'marketcap']
            numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'marketcap']
            
            quality_score = None
            if self.config.enable_data_validation:
                validation_passed, quality_score, validation_issues = self.data_validator.validate_dataframe(
                    df, required_columns, numeric_columns
                )

                if not validation_passed:
                    error_msg = f"Data validation failed: {validation_issues}"
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                logger.info(f"Data validation passed with quality score: {quality_score:.2f}")
            
            # Data preparation
            df_clean = df.copy()

            # Convert trade_date to proper datetime object for Oracle
            if 'trade_date' in df_clean.columns:
                # Handle both string and datetime formats
                if df_clean['trade_date'].dtype == 'object':
                    df_clean['trade_date'] = pd.to_datetime(df_clean['trade_date'])
                elif df_clean['trade_date'].dtype == 'int64':
                    # Handle timestamp format
                    df_clean['trade_date'] = pd.to_datetime(df_clean['trade_date'], unit='ms')

            # Column mapping
            column_mapping = {
                'open': 'open_price',
                'high': 'high_price',
                'low': 'low_price',
                'close': 'close_price',
                'transactions': 'num_transactions',
                'marketcap': 'marketcap'
            }
            df_clean = df_clean.rename(columns=column_mapping)

            # Note: Price precision is now handled by apply_schema_precision method

            # Ensure required columns
            required_cols = ['ticker', 'trade_date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'vwap', 'num_transactions', 'marketcap']
            df_clean = df_clean[required_cols]

            # Remove timestamp columns (they have database defaults) and convert data types for Oracle compatibility
            df_clean = OracleDataTypeConverter.remove_audit_timestamps(df_clean)
            df_clean = OracleDataTypeConverter.convert_datatypes_for_oracle(df_clean)
            df_clean = OracleDataTypeConverter.apply_schema_precision(df_clean, 'TICKERS')
            df_clean = OracleDataTypeConverter.final_nan_cleanup(df_clean)
            # AUTO NUMERICAL CLEANUP: Automatically detect and clean numerical columns
            df_clean = OracleDataTypeConverter.auto_clean_numerical_columns(df_clean)
            
            # Bulk insert using pandas to_sql with SQLAlchemy
            current_data = df_clean.to_dict(orient='records')
            cols = df_clean.columns.tolist()
            placeholders = ', '.join([':' + col for col in cols])
            insert_statement = f"""
            INSERT INTO {self.__tablename__} ({', '.join(cols)})
            VALUES ({placeholders})
            """
            
            from sqlalchemy import text
            
            # Insert in chunks for better performance
            chunk_size = self.config.max_insert_batch_size
            total_inserted = 0
            
            for start in range(0, len(df_clean), chunk_size):
                chunk = df_clean.iloc[start:start+chunk_size]
                current_data = chunk.to_dict(orient='records')
                
                with conn.begin() as transaction:
                    transaction.execute(text(insert_statement), current_data)
                
                total_inserted += len(chunk)
                logger.debug(f"Inserted chunk {start//chunk_size + 1}: {len(chunk)} records")

            log_database_operation("insert", self.__tablename__, total_inserted)
            logger.info(f"Successfully inserted {total_inserted} records into {self.__tablename__}")
            
            duration = time.time() - start_time
            self.performance_monitor.record_operation(
                operation_name, duration, True, 
                records_inserted=total_inserted,
                quality_score=quality_score if self.config.enable_data_validation else None
            )
            
        except Exception as e:
            error_msg = f"Error inserting data into {self.__tablename__}: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            log_database_operation("insert", self.__tablename__, error=error_msg)
            
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, False, error_msg)
            raise
    
    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def get_data(self, conn, ticker: Optional[str] = None) -> pd.DataFrame:
        """
        Get data from the table with  error handling.
        
        Args:
            cursor: Database cursor
            ticker (Optional[str]): Specific ticker to retrieve
            
        Returns:
            pd.DataFrame: Retrieved data
            
        Example:
            >>> model = TickerData()
            >>> data = model.get_data(cursor, "AAPL")
        """
        operation_name = "get_data_ticker_data"
        start_time = time.time()
        
        try:
            from sqlalchemy import text
            if ticker:
                result_proxy = conn.execute(text(f"SELECT * FROM {self.__tablename__} WHERE ticker = :ticker"), {"ticker": ticker})
            else:
                result_proxy = conn.execute(text(f"SELECT * FROM {self.__tablename__}"))

            results = result_proxy.fetchall()
            df = pd.DataFrame(results, columns=result_proxy.keys())
            
            duration = time.time() - start_time
            self.performance_monitor.record_operation(
                operation_name, duration, True, 
                rows_retrieved=len(df)
            )
            
            return df
            
        except Exception as e:
            error_msg = f"Error getting ticker data: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, False, error_msg)
            raise
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """
        Get comprehensive performance metrics for monitoring and analysis.
        
        Returns:
            Dict[str, Any]: Performance metrics summary
        """
        return {
            "performance_metrics": self.performance_monitor.metrics,
            "config": {
                "max_insert_batch_size": self.config.max_insert_batch_size,
                "max_memory_usage_mb": self.config.max_memory_usage_mb,
                "enable_data_validation": self.config.enable_data_validation,
                "min_data_quality_score": self.config.min_data_quality_score
            },
            "timestamp": time.time()
        }

# Backward compatibility
TickerData = TickerData

class MacroData:
    """
    Model for storing macroeconomic data with  features.
    
    This class provides -level capabilities for storing macroeconomic data with
    advanced bulk insert operatio   ns, incremental loading, and comprehensive data validation.
    
    Attributes:
        config (ModelConfig):  configuration
        performance_monitor (PerformanceMonitor): Performance monitor
        data_validator (DataValidator): Data validator
        __tablename__ (str): Database table name
    """
    __tablename__ = 'MACRO_DATA'

    def __init__(self, config: Optional[ModelConfig] = None):
        self.config = config or ModelConfig()
        self.performance_monitor = PerformanceMonitor("default")
        self.data_validator = DataValidator()
        logger.info("MacroData initialized with  configuration")

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def create_table(self, conn):
        operation_name = "create_table_macro_data"
        start_time = time.time()
        try:
            from sqlalchemy import text
            conn.execute(text(f"""
                BEGIN
                    EXECUTE IMMEDIATE 'CREATE TABLE {self.__tablename__} (
                        id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                        trade_date DATE NOT NULL,
                        series_id VARCHAR2(100) NOT NULL,
                        value NUMBER(15,6),
                        units VARCHAR2(150),
                        title VARCHAR2(255),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -955 THEN  -- Table already exists
                            RAISE;
                        END IF;
                END;
            """))
            # Create indexes for performance
            conn.execute(text(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{self.__tablename__}_series ON {self.__tablename__} (series_id)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN  -- Index already exists
                        RAISE;
                    END IF;
            END;
            """))
            conn.execute(text(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{self.__tablename__}_date ON {self.__tablename__} (trade_date)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN  -- Index already exists
                        RAISE;
                    END IF;
            END;
            """))
            log_database_operation("create_table", self.__tablename__)
            logger.info(f"Table {self.__tablename__} created/verified successfully")
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, True)
        except Exception as e:
            error_msg = f"Error creating table {self.__tablename__}: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            log_database_operation("create_table", self.__tablename__, error=error_msg)
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, False, error_msg)
            raise

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def data_exists(self, conn, series_id: Optional[str] = None) -> bool:
        operation_name = "data_exists_macro_data"
        start_time = time.time()
        try:
            from sqlalchemy import text
            if series_id:
                result_proxy = conn.execute(text(f"SELECT 1 FROM {self.__tablename__} WHERE series_id = :series_id AND ROWNUM = 1"), {"series_id": series_id})
                result = result_proxy.fetchone() is not None
            else:
                result_proxy = conn.execute(text(f"SELECT 1 FROM {self.__tablename__} WHERE ROWNUM = 1"))
                result = result_proxy.fetchone() is not None
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, True, data_exists=result)
            return result
        except Exception as e:
            error_msg = f"Error checking macro data existence: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, False, error_msg)
            return False

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def get_last_date(self, conn, series_id: Optional[str] = None) -> Optional[date_type]:
        operation_name = "get_last_date_macro_data"
        start_time = time.time()
        try:
            from sqlalchemy import text
            if series_id:
                result_proxy = conn.execute(text(f"SELECT MAX(trade_date) FROM {self.__tablename__} WHERE series_id = :series_id"), {"series_id": series_id})
            else:
                result_proxy = conn.execute(text(f"SELECT MAX(trade_date) FROM {self.__tablename__}"))
            result = result_proxy.fetchone()
            last_date = result[0] if result else None
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, True, last_date=last_date)
            return last_date
        except Exception as e:
            error_msg = f"Error getting last macro date: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, False, error_msg)
            return None

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def insert_data(self, conn, df: pd.DataFrame):
        operation_name = "insert_data_macro_data"
        start_time = time.time()
        try:
            if df is None or df.empty:
                logger.warning("No macro data to insert")
                return
            # Data validation
            required_columns = ['date', 'series_id', 'value', 'units', 'title']
            numeric_columns = ['value']
            quality_score = None
            if self.config.enable_data_validation:
                validation_passed, quality_score, validation_issues = self.data_validator.validate_dataframe(
                    df, required_columns, numeric_columns
                )
                if not validation_passed:
                    error_msg = f"Macro data validation failed: {validation_issues}"
                    logger.error(error_msg)
                    raise ValueError(error_msg)
                logger.info(f"Macro data validation passed with quality score: {quality_score:.2f}")
            df_clean = df.copy()
            # Handle trade_date column
            if 'trade_date' not in df_clean.columns and 'date' in df_clean.columns:
                df_clean['trade_date'] = pd.to_datetime(df_clean['date']).dt.date
            elif 'trade_date' in df_clean.columns:
                df_clean['trade_date'] = pd.to_datetime(df_clean['trade_date']).dt.date
            else:
                logger.warning("No date or trade_date column found in DataFrame")
                return
            # Remove timestamp columns (they have database defaults) and convert data types for Oracle compatibility
            df_clean = OracleDataTypeConverter.remove_audit_timestamps(df_clean)
            df_clean = OracleDataTypeConverter.convert_datatypes_for_oracle(df_clean)
            df_clean = OracleDataTypeConverter.apply_schema_precision(df_clean, 'MACRO_DATA')
            df_clean = OracleDataTypeConverter.final_nan_cleanup(df_clean)
            # AUTO NUMERICAL CLEANUP: Automatically detect and clean numerical columns
            df_clean = OracleDataTypeConverter.auto_clean_numerical_columns(df_clean)
            # Ensure columns are in the correct order for the table (excluding timestamp columns with defaults)
            expected_columns = ['trade_date', 'series_id', 'value', 'units', 'title']
            df_clean = df_clean[expected_columns]
            # Prepare for bulk insert
            cols = df_clean.columns.tolist()
            placeholders = ', '.join([':' + col for col in cols])
            insert_statement = f"""
            INSERT INTO {self.__tablename__} ({', '.join(cols)})
            VALUES ({placeholders})
            """
            from sqlalchemy import text
            chunk_size = self.config.max_insert_batch_size
            total_inserted = 0
            for start in range(0, len(df_clean), chunk_size):
                chunk = df_clean.iloc[start:start+chunk_size]
                current_data = chunk.to_dict(orient='records')
                with conn.begin() as transaction:
                    transaction.execute(text(insert_statement), current_data)
                total_inserted += len(chunk)
            log_database_operation("insert", self.__tablename__, total_inserted)
            logger.info(f"Successfully inserted {total_inserted} macro records into {self.__tablename__}")
            duration = time.time() - start_time
            self.performance_monitor.record_operation(
                operation_name, duration, True, 
                records_inserted=total_inserted,
                quality_score=quality_score if self.config.enable_data_validation else None
            )
        except Exception as e:
            error_msg = f"Error inserting macro data into {self.__tablename__}: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            log_database_operation("insert", self.__tablename__, error=error_msg)
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, False, error_msg)
            raise

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def get_data(self, conn, series_id: Optional[str] = None) -> pd.DataFrame:
        operation_name = "get_data_macro_data"
        start_time = time.time()
        try:
            from sqlalchemy import text
            if series_id:
                result_proxy = conn.execute(text(f"SELECT * FROM {self.__tablename__} WHERE series_id = :series_id"), {"series_id": series_id})
            else:
                result_proxy = conn.execute(text(f"SELECT * FROM {self.__tablename__}"))
            results = result_proxy.fetchall()
            df = pd.DataFrame(results, columns=result_proxy.keys())
            duration = time.time() - start_time
            self.performance_monitor.record_operation(
                operation_name, duration, True, 
                rows_retrieved=len(df)
            )
            return df
        except Exception as e:
            error_msg = f"Error getting macro data: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, False, error_msg)
            raise
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        return {
            "performance_metrics": self.performance_monitor.metrics,
            "config": {
                "max_insert_batch_size": self.config.max_insert_batch_size,
                "max_memory_usage_mb": self.config.max_memory_usage_mb,
                "enable_data_validation": self.config.enable_data_validation,
                "min_data_quality_score": self.config.min_data_quality_score
            },
            "timestamp": time.time()
        }

# Backward compatibility
MacroData = MacroData

class TransformedTickerData:
    """
     model for storing processed ticker data with technical indicators.
    
    This class provides -level capabilities for storing transformed ticker data with
    advanced bulk insert operations, incremental loading, and comprehensive data validation.
    
    Attributes:
        config (ModelConfig):  configuration
        performance_monitor (PerformanceMonitor): Performance monitor
        data_validator (DataValidator): Data validator
        __tablename__ (str): Database table name
    """
    __tablename__ = 'TRANSFORMED_TICKERS'

    def __init__(self, config: Optional[ModelConfig] = None):
        self.config = config or ModelConfig()
        self.performance_monitor = PerformanceMonitor("default")
        self.data_validator = DataValidator()
        logger.info("TransformedTickers initialized with  configuration")

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def create_table(self, conn):
        operation_name = "create_table_transformed_tickers"
        start_time = time.time()
        try:
            from sqlalchemy import text
            conn.execute(text(f"""
                BEGIN
                    EXECUTE IMMEDIATE 'CREATE TABLE {self.__tablename__} (
                        id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                        ticker VARCHAR2(10) NOT NULL,
                        trade_date DATE NOT NULL,
                        open_price BINARY_DOUBLE,
                        high_price BINARY_DOUBLE,
                        low_price BINARY_DOUBLE,
                        close_price BINARY_DOUBLE,
                        volume NUMBER,
                        num_transactions NUMBER,
                        marketcap NUMBER(20, 2),
                        rsi BINARY_DOUBLE,
                        macd BINARY_DOUBLE,
                        macd_signal BINARY_DOUBLE,
                        macd_hist BINARY_DOUBLE,
                        stoch_k BINARY_DOUBLE,
                        stoch_d BINARY_DOUBLE,
                        cci BINARY_DOUBLE,
                        roc BINARY_DOUBLE,
                        mom BINARY_DOUBLE,
                        trix BINARY_DOUBLE,
                        willr BINARY_DOUBLE,
                        sma10 BINARY_DOUBLE,
                        sma20 BINARY_DOUBLE,
                        sma50 BINARY_DOUBLE,
                        sma100 BINARY_DOUBLE,
                        sma200 BINARY_DOUBLE,
                        ema10 BINARY_DOUBLE,
                        ema20 BINARY_DOUBLE,
                        ema50 BINARY_DOUBLE,
                        ema100 BINARY_DOUBLE,
                        ema200 BINARY_DOUBLE,
                        obv BINARY_DOUBLE,
                        ad BINARY_DOUBLE,
                        mfi BINARY_DOUBLE,
                        adosc BINARY_DOUBLE,
                        cmf BINARY_DOUBLE,
                        vwap BINARY_DOUBLE,
                        vpt BINARY_DOUBLE,
                        adx BINARY_DOUBLE,
                        rvol BINARY_DOUBLE,
                        atr BINARY_DOUBLE,
                        natr BINARY_DOUBLE,
                        trange BINARY_DOUBLE,
                        bbands_upper BINARY_DOUBLE,
                        bbands_middle BINARY_DOUBLE,
                        bbands_lower BINARY_DOUBLE,
                        ulcer_index BINARY_DOUBLE,
                        donch_upper BINARY_DOUBLE,
                        donch_lower BINARY_DOUBLE,
                        midprice BINARY_DOUBLE,
                        medprice BINARY_DOUBLE,
                        typprice BINARY_DOUBLE,
                        wclprice BINARY_DOUBLE,
                        avgprice BINARY_DOUBLE,
                        return_1d BINARY_DOUBLE,
                        return_3d BINARY_DOUBLE,
                        return_7d BINARY_DOUBLE,
                        return_30d BINARY_DOUBLE,
                        return_90d BINARY_DOUBLE,
                        return_365d BINARY_DOUBLE,
                        ht_trendline BINARY_DOUBLE,
                        ht_sine BINARY_DOUBLE,
                        ht_sine_lead BINARY_DOUBLE,
                        ht_dcperiod BINARY_DOUBLE,
                        ht_dcphase BINARY_DOUBLE,
                        stddev BINARY_DOUBLE,
                        var BINARY_DOUBLE,
                        beta_vs_sp500 BINARY_DOUBLE,
                        zscore_price_normalized BINARY_DOUBLE,
                        cdl2crows BINARY_DOUBLE,
                        cdl3blackcrows BINARY_DOUBLE,
                        cdl3inside BINARY_DOUBLE,
                        cdl3linestrike BINARY_DOUBLE,
                        cdl3outside BINARY_DOUBLE,
                        cdl3starsinsouth BINARY_DOUBLE,
                        cdl3whitesoldiers BINARY_DOUBLE,
                        cdlabandonedbaby BINARY_DOUBLE,
                        cdlbelthold BINARY_DOUBLE,
                        cdlbreakaway BINARY_DOUBLE,
                        cdlclosingmarubozu BINARY_DOUBLE,
                        cdlconcealbabyswall BINARY_DOUBLE,
                        cdlcounterattack BINARY_DOUBLE,
                        cdldarkcloudcover BINARY_DOUBLE,
                        cdldoji BINARY_DOUBLE,
                        cdldojistar BINARY_DOUBLE,
                        cdlengulfing BINARY_DOUBLE,
                        cdleveningstar BINARY_DOUBLE,
                        cdlgravestonedoji BINARY_DOUBLE,
                        cdlhammer BINARY_DOUBLE,
                        cdlhangingman BINARY_DOUBLE,
                        cdlharami BINARY_DOUBLE,
                        cdlharamicross BINARY_DOUBLE,
                        cdlhighwave BINARY_DOUBLE,
                        cdlhikkake BINARY_DOUBLE,
                        cdlhikkakemod BINARY_DOUBLE,
                        cdlhomingpigeon BINARY_DOUBLE,
                        cdlidentical3crows BINARY_DOUBLE,
                        cdlinneck BINARY_DOUBLE,
                        cdlinvertedhammer BINARY_DOUBLE,
                        cdlladderbottom BINARY_DOUBLE,
                        cdllongleggeddoji BINARY_DOUBLE,
                        cdllongline BINARY_DOUBLE,
                        cdlmarubozu BINARY_DOUBLE,
                        cdlmatchinglow BINARY_DOUBLE,
                        cdlmathold BINARY_DOUBLE,
                        cdlmorningdojistar BINARY_DOUBLE,
                        cdlmorningstar BINARY_DOUBLE,
                        cdlonneck BINARY_DOUBLE,
                        cdlpiercing BINARY_DOUBLE,
                        cdlrickshawman BINARY_DOUBLE,
                        cdlrisefall3methods BINARY_DOUBLE,
                        cdlseparatinglines BINARY_DOUBLE,
                        cdlshootingstar BINARY_DOUBLE,
                        cdlshortline BINARY_DOUBLE,
                        cdlspinningtop BINARY_DOUBLE,
                        cdlstalledpattern BINARY_DOUBLE,
                        cdlsticksandwich BINARY_DOUBLE,
                        cdltakuri BINARY_DOUBLE,
                        cdltasukigap BINARY_DOUBLE,
                        cdlthrusting BINARY_DOUBLE,
                        cdltristar BINARY_DOUBLE,
                        cdlunique3river BINARY_DOUBLE,
                        cdlxsidegap3methods BINARY_DOUBLE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -955 THEN  -- Table already exists
                            RAISE;
                        END IF;
                END;
            """))
            # Create indexes for performance
            conn.execute(text(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{self.__tablename__}_ticker ON {self.__tablename__} (ticker)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN  -- Index already exists
                        RAISE;
                    END IF;
            END;
            """))
            conn.execute(text(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{self.__tablename__}_date ON {self.__tablename__} (trade_date)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN  -- Index already exists
                        RAISE;
                    END IF;
            END;
            """))
            conn.execute(text(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{self.__tablename__}_ticker_date ON {self.__tablename__} (ticker, trade_date)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN  -- Index already exists
                        RAISE;
                    END IF;
            END;
            """))
            log_database_operation("create_table", self.__tablename__)
            logger.info(f"Table {self.__tablename__} created/verified successfully")
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, True)
        except Exception as e:
            error_msg = f"Error creating table {self.__tablename__}: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            log_database_operation("create_table", self.__tablename__, error=error_msg)
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, False, error_msg)
            raise

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def data_exists(self, conn, ticker: Optional[str] = None) -> bool:
        operation_name = "data_exists_transformed_tickers"
        start_time = time.time()
        try:
            from sqlalchemy import text
            if ticker:
                result_proxy = conn.execute(text(f"SELECT 1 FROM {self.__tablename__} WHERE ticker = :ticker"), {"ticker": ticker})
                result = result_proxy.fetchone() is not None
            else:
                result_proxy = conn.execute(text(f"SELECT 1 FROM {self.__tablename__}"))
                result = result_proxy.fetchone() is not None
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, True, data_exists=result)
            return result
        except Exception as e:
            error_msg = f"Error checking transformed data existence: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, False, error_msg)
            return False

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def get_last_date(self, conn, ticker: Optional[str] = None) -> Optional[date_type]:
        operation_name = "get_last_date_transformed_tickers"
        start_time = time.time()
        try:
            from sqlalchemy import text
            if ticker:
                result_proxy = conn.execute(text(f"SELECT MAX(trade_date) FROM {self.__tablename__} WHERE ticker = :ticker"), {"ticker": ticker})
                result = result_proxy.fetchone()
                last_date = result[0] if result else None
            else:
                last_date = None
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, True, last_date=last_date)
            return last_date
        except Exception as e:
            error_msg = f"Error getting last transformed date: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, False, error_msg)
            return None

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def insert_data(self, conn, df: pd.DataFrame):
        operation_name = "insert_data_transformed_tickers"
        start_time = time.time()
        try:
            if df is None or df.empty:
                logger.warning("No transformed data to insert")
                return
            # Data validation
            required_columns = ['ticker', 'trade_date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']
            numeric_columns = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
            quality_score = None
            if self.config.enable_data_validation:
                validation_passed, quality_score, validation_issues = self.data_validator.validate_dataframe(
                    df, required_columns, numeric_columns
                )
                if not validation_passed:
                    error_msg = f"Transformed ticker data validation failed: {validation_issues}"
                    logger.error(error_msg)
                    raise ValueError(error_msg)
                logger.info(f"Transformed ticker data validation passed with quality score: {quality_score:.2f}")
            df_clean = df.copy()
            # Handle trade_date column
            if 'trade_date' in df_clean.columns:
                df_clean['trade_date'] = pd.to_datetime(df_clean['trade_date']).dt.date
            elif 'date' in df_clean.columns:
                df_clean['trade_date'] = pd.to_datetime(df_clean['date']).dt.date
            else:
                logger.warning("No date or trade_date column found in DataFrame")
                return
            # Remove timestamp columns (they have database defaults) and convert data types for Oracle compatibility
            df_clean = OracleDataTypeConverter.remove_audit_timestamps(df_clean)
            df_clean = OracleDataTypeConverter.convert_datatypes_for_oracle(df_clean)
            df_clean = OracleDataTypeConverter.apply_schema_precision(df_clean, 'TRANSFORMED_TICKERS')
            df_clean = OracleDataTypeConverter.clean_technical_indicators(df_clean)
            df_clean = OracleDataTypeConverter.final_nan_cleanup(df_clean)
            # AUTO NUMERICAL CLEANUP: Automatically detect and clean numerical columns
            df_clean = OracleDataTypeConverter.auto_clean_numerical_columns(df_clean)
            df_clean = df_clean.astype(object).where(pd.notnull(df_clean), None)


            # Import text here
            from sqlalchemy import text
            
            # Clear existing data for this ticker to avoid conflicts
            if 'ticker' in df_clean.columns:
                ticker_value = df_clean['ticker'].iloc[0]
                delete_statement = f"DELETE FROM {self.__tablename__} WHERE ticker = :ticker_val"
                with conn.begin() as transaction:
                    transaction.execute(text(delete_statement), {'ticker_val': ticker_value})
                    logger.info(f"Cleared existing data for ticker: {ticker_value}")

            # Prepare for bulk insert
            cols = df_clean.columns.tolist()
            placeholders = ', '.join([':' + col for col in cols])
            insert_statement = f"""
            INSERT INTO {self.__tablename__} ({', '.join(cols)})
            VALUES ({placeholders})
            """
            from sqlalchemy import text
            chunk_size = self.config.max_insert_batch_size
            total_inserted = 0
            for start in range(0, len(df_clean), chunk_size):
                chunk = df_clean.iloc[start:start+chunk_size]
                current_data = chunk.to_dict(orient='records')
                with conn.begin() as transaction:
                    transaction.execute(text(insert_statement), current_data)
                total_inserted += len(chunk)
            logger.info(f"Successfully inserted {total_inserted} transformed ticker records into {self.__tablename__}")
            duration = time.time() - start_time
            self.performance_monitor.record_operation(
                operation_name, duration, True, 
                records_inserted=total_inserted,
                quality_score=quality_score if self.config.enable_data_validation else None
            )
        except Exception as e:
            error_msg = f"Error inserting transformed data into {self.__tablename__}: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            duration = time.time() - start_time
            log_database_operation("insert", self.__tablename__, error=error_msg)
            self.performance_monitor.record_operation(operation_name, duration, False, error_msg)
            raise

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def get_data(self, conn, ticker: Optional[str] = None) -> pd.DataFrame:
        operation_name = "get_data_transformed_tickers"
        start_time = time.time()
        try:
            from sqlalchemy import text
            if ticker:
                result_proxy = conn.execute(text(f"SELECT * FROM {self.__tablename__} WHERE ticker = :ticker"), {"ticker": ticker})
            else:
                result_proxy = conn.execute(text(f"SELECT * FROM {self.__tablename__}"))
            results = result_proxy.fetchall()
            df = pd.DataFrame(results, columns=result_proxy.keys())
            duration = time.time() - start_time
            self.performance_monitor.record_operation(
                operation_name, duration, True, 
                rows_retrieved=len(df)
            )
            return df
        except Exception as e:
            error_msg = f"Error getting transformed data: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, False, error_msg)
            raise
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        return {
            "performance_metrics": self.performance_monitor.metrics,
            "config": {
                "max_insert_batch_size": self.config.max_insert_batch_size,
                "max_memory_usage_mb": self.config.max_memory_usage_mb,
                "enable_data_validation": self.config.enable_data_validation,
                "min_data_quality_score": self.config.min_data_quality_score
            },
            "timestamp": time.time()
        }

# Backward compatibility
TransformedTickers = TransformedTickerData

class TransformedMacroData:
    """
     model for storing transformed macroeconomic data.
    
    This class provides -level capabilities for storing transformed macroeconomic data with
    advanced bulk insert operations, incremental loading, and comprehensive data validation.
    
    Attributes:
        config (ModelConfig):  configuration
        performance_monitor (PerformanceMonitor): Performance monitor
        data_validator (DataValidator): Data validator
        __tablename__ (str): Database table name
    """
    __tablename__ = 'TRANSFORMED_MACRO_DATA'

    def __init__(self, config: Optional[ModelConfig] = None):
        self.config = config or ModelConfig()
        self.performance_monitor = PerformanceMonitor("default")
        self.data_validator = DataValidator()
        logger.info("TransformedMacroData initialized with  configuration")

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def create_table(self, conn):
        operation_name = "create_table_transformed_macro_data"
        start_time = time.time()
        try:
            from sqlalchemy import text
            conn.execute(text(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE TABLE {self.__tablename__} (
                    id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    trade_date DATE NOT NULL,
                    series_id VARCHAR2(100) NOT NULL,
                    value BINARY_DOUBLE,
                    transformation_type VARCHAR2(50) NOT NULL,
                    transformation_params VARCHAR2(200),
                    transformed_value BINARY_DOUBLE,
                    units VARCHAR2(150),
                    title VARCHAR2(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT uk_macro_transform UNIQUE (trade_date, series_id, transformation_type, transformation_params)
                )';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN  -- Table already exists
                        RAISE;
                    END IF;
                END;
            """))
            # Create indexes for performance
            conn.execute(text(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{self.__tablename__}_series ON {self.__tablename__} (series_id)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN  -- Index already exists
                        RAISE;
                    END IF;
                END;
            """))
            conn.execute(text(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{self.__tablename__}_date ON {self.__tablename__} (trade_date)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN  -- Index already exists
                        RAISE;
                    END IF;
                END;
            """))
            conn.execute(text(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{self.__tablename__}_transform ON {self.__tablename__} (transformation_type)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN  -- Index already exists
                        RAISE;
                    END IF;
                END;
            """))
            log_database_operation("create_table", self.__tablename__)
            logger.info(f"Table {self.__tablename__} created/verified successfully")
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, True)
        except Exception as e:
            error_msg = f"Error creating table {self.__tablename__}: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            log_database_operation("create_table", self.__tablename__, error=error_msg)
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, False, error_msg)
            raise

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def data_exists(self, conn, series_id: Optional[str] = None) -> bool:
        operation_name = "data_exists_transformed_macro_data"
        start_time = time.time()
        try:
            from sqlalchemy import text
            if series_id:
                result_proxy = conn.execute(text(f"SELECT 1 FROM {self.__tablename__} WHERE series_id = :series_id AND ROWNUM = 1"), {"series_id": series_id})
                result = result_proxy.fetchone() is not None
            else:
                result_proxy = conn.execute(text(f"SELECT 1 FROM {self.__tablename__} WHERE ROWNUM = 1"))
                result = result_proxy.fetchone() is not None
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, True, data_exists=result)
            return result
        except Exception as e:
            error_msg = f"Error checking transformed macro data existence: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, False, error_msg)
            return False

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def get_last_date(self, conn, series_id: Optional[str] = None) -> Optional[date_type]:
        operation_name = "get_last_date_transformed_macro_data"
        start_time = time.time()
        try:
            from sqlalchemy import text
            if series_id:
                result_proxy = conn.execute(text(f"SELECT MAX(trade_date) FROM {self.__tablename__} WHERE series_id = :series_id"), {"series_id": series_id})
                result = result_proxy.fetchone()
                last_date = result[0] if result else None
            else:
                last_date = None
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, True, last_date=last_date)
            return last_date
        except Exception as e:
            error_msg = f"Error getting last transformed macro date: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, False, error_msg)
            return None

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def insert_data(self, conn, df: pd.DataFrame):
        operation_name = "insert_data_transformed_macro_data"
        start_time = time.time()
        try:
            if df is None or df.empty:
                logger.warning("No transformed macro data to insert")
                return
            # Data validation
            required_columns = ['trade_date', 'series_id', 'value']
            numeric_columns = ['value']
            quality_score = None
            if self.config.enable_data_validation:
                validation_passed, quality_score, validation_issues = self.data_validator.validate_dataframe(
                    df, required_columns, numeric_columns
                )
                if not validation_passed:
                    error_msg = f"Transformed macro data validation failed: {validation_issues}"
                    logger.error(error_msg)
                    raise ValueError(error_msg)
                logger.info(f"Transformed macro data validation passed with quality score: {quality_score:.2f}")
            df_clean = df.copy()
            # Handle trade_date column
            if 'trade_date' in df_clean.columns:
                df_clean['trade_date'] = pd.to_datetime(df_clean['trade_date']).dt.date
            elif 'date' in df_clean.columns:
                df_clean['trade_date'] = pd.to_datetime(df_clean['date']).dt.date
            else:
                logger.warning("No date or trade_date column found in DataFrame")
                return
            # Remove timestamp columns (they have database defaults)
            df_clean = OracleDataTypeConverter.remove_audit_timestamps(df_clean)

            # Import text here
            from sqlalchemy import text

            # Convert data types for Oracle compatibility and apply schema precision constraints
            df_clean = OracleDataTypeConverter.convert_datatypes_for_oracle(df_clean)
            df_clean = OracleDataTypeConverter.apply_schema_precision(df_clean, 'TRANSFORMED_MACRO_DATA')
            df_clean = OracleDataTypeConverter.final_nan_cleanup(df_clean)
            # AUTO NUMERICAL CLEANUP: Automatically detect and clean numerical columns
            df_clean = OracleDataTypeConverter.auto_clean_numerical_columns(df_clean)
            df_clean = df_clean.astype(object).where(pd.notnull(df_clean), None)
            
            print(df_clean.isna().sum())    # check which columns still have NaNs
            print(df_clean.dtypes)          # confirm numeric types
            print(df_clean.head(3).to_dict())  # sanity check row content

            # Clear existing data for this series to avoid conflicts
            if 'series_id' in df_clean.columns:
                series_value = df_clean['series_id'].iloc[0]
                delete_statement = f"DELETE FROM {self.__tablename__} WHERE series_id = :series_val"
                with conn.begin() as transaction:
                    transaction.execute(text(delete_statement), {'series_val': series_value})
                    logger.info(f"Cleared existing data for series: {series_value}")

            # Prepare for bulk insert
            cols = df_clean.columns.tolist()
            placeholders = ', '.join([':' + col for col in cols])
            insert_statement = f"""
            INSERT INTO {self.__tablename__} ({', '.join(cols)})
            VALUES ({placeholders})
            """
            chunk_size = self.config.max_insert_batch_size
            total_inserted = 0
            for start in range(0, len(df_clean), chunk_size):
                chunk = df_clean.iloc[start:start+chunk_size]
                current_data = chunk.to_dict(orient='records')
                with conn.begin() as transaction:
                    transaction.execute(text(insert_statement), current_data)
                total_inserted += len(chunk)
            logger.info(f"Successfully inserted {total_inserted} transformed macro records into {self.__tablename__}")
            duration = time.time() - start_time
            self.performance_monitor.record_operation(
                operation_name, duration, True, 
                records_inserted=total_inserted,
                quality_score=quality_score if self.config.enable_data_validation else None
            )
        except Exception as e:
            error_msg = f"Error inserting transformed macro data into {self.__tablename__}: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            duration = time.time() - start_time
            log_database_operation("insert", self.__tablename__, error=error_msg)
            self.performance_monitor.record_operation(operation_name, duration, False, error_msg)
            raise

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def get_data(self, conn, series_id: Optional[str] = None) -> pd.DataFrame:
        operation_name = "get_data_transformed_macro_data"
        start_time = time.time()
        try:
            from sqlalchemy import text
            if series_id:
                result_proxy = conn.execute(text(f"SELECT * FROM {self.__tablename__} WHERE series_id = :series_id"), {"series_id": series_id})
            else:
                result_proxy = conn.execute(text(f"SELECT * FROM {self.__tablename__}"))
            results = result_proxy.fetchall()
            df = pd.DataFrame(results, columns=result_proxy.keys())
            duration = time.time() - start_time
            self.performance_monitor.record_operation(
                operation_name, duration, True, 
                rows_retrieved=len(df)
            )
            return df
        except Exception as e:
            error_msg = f"Error getting transformed macro data: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            duration = time.time() - start_time
            self.performance_monitor.record_operation(operation_name, duration, False, error_msg)
            raise
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        return {
            "performance_metrics": self.performance_monitor.metrics,
            "config": {
                "max_insert_batch_size": self.config.max_insert_batch_size,
                "max_memory_usage_mb": self.config.max_memory_usage_mb,
                "enable_data_validation": self.config.enable_data_validation,
                "min_data_quality_score": self.config.min_data_quality_score
            },
            "timestamp": time.time()
        }



# Backward compatibility
TransformedMacroData = TransformedMacroData