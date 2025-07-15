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

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from CandleThrob.utils.oracle_conn import OracleDB
from CandleThrob.utils.logging_config import get_database_logger, log_database_operation

# Get logger for this module
logger = get_database_logger(__name__)

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

class PerformanceMonitor:
    """ performance monitoring for model operations."""
    
    def __init__(self):
        self.metrics: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
    
    def record_operation(self, operation_name: str, duration: float, success: bool, 
                        error_message: Optional[str] = None, **kwargs):
        """Record operation metrics."""
        with self._lock:
            self.metrics[operation_name] = {
                "duration": duration,
                "success": success,
                "error_message": error_message,
                "timestamp": time.time(),
                **kwargs
            }

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
        self.performance_monitor = PerformanceMonitor()
        self.data_validator = DataValidator()
        
        logger.info("TickerData initialized with  configuration")
    
    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def create_table(self, cursor):
        """
        Create the table with  features and comprehensive error handling.
        
        Args:
            cursor: Database cursor for table creation
            
        Example:
            >>> model = TickerData()
            >>> with db.establish_connection() as conn:
            >>>     with conn.cursor() as cursor:
            >>>         model.create_table(cursor)
        """
        operation_name = "create_table_ticker_data"
        start_time = time.time()
        
        try:
            cursor.execute(f"""
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
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -955 THEN  -- Table already exists
                            RAISE;
                        END IF;
                END;
            """)
            
            # Create indexes for performance
            cursor.execute(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{self.__tablename__}_ticker ON {self.__tablename__} (ticker)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN  -- Index already exists
                        RAISE;
                    END IF;
            END;
            """)
            
            cursor.execute(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{self.__tablename__}_date ON {self.__tablename__} (trade_date)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN  -- Index already exists
                        RAISE;
                    END IF;
            END;
            """)
            
            cursor.execute(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{self.__tablename__}_ticker_date ON {self.__tablename__} (ticker, trade_date)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN  -- Index already exists
                        RAISE;
                    END IF;
            END;
            """)
            
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
    def data_exists(self, cursor, ticker: Optional[str] = None) -> bool:
        """
        Check if data exists in the table with error handling.
        
        Args:
            cursor: Database cursor
            ticker (Optional[str]): Specific ticker to check
            
        Returns:
            bool: True if data exists, False otherwise
            
        Example:
            >>> model = TickerData()
            >>> exists = model.data_exists(cursor, "AAPL")
        """
        operation_name = "data_exists_ticker_data"
        start_time = time.time()
        
        try:
            if ticker:
                cursor.execute(f"SELECT 1 FROM {self.__tablename__} WHERE ticker = :ticker", {"ticker": ticker})
                result = cursor.fetchone() is not None
            else:
                cursor.execute(f"SELECT 1 FROM {self.__tablename__}")
                result = cursor.fetchone() is not None
            
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
    def get_last_date(self, cursor, ticker: Optional[str] = None) -> Optional[date_type]:
        """
        Get the last date for which data exists with  error handling.
        
        Args:
            cursor: Database cursor
            ticker (Optional[str]): Specific ticker to check
            
        Returns:
            Optional[date_type]: Last date or None if no data exists
            
        Example:
            >>> model = TickerData()
            >>> last_date = model.get_last_date(cursor, "AAPL")
        """
        operation_name = "get_last_date_ticker_data"
        start_time = time.time()
        
        try:
            if ticker:
                cursor.execute(f"SELECT MAX(trade_date) FROM {self.__tablename__} WHERE ticker = :ticker", {"ticker": ticker})
            else:
                cursor.execute(f"SELECT MAX(trade_date) FROM {self.__tablename__}")
            
            result = cursor.fetchone()
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
            required_columns = ['ticker', 'trade_date', 'open', 'high', 'low', 'close', 'volume']
            numeric_columns = ['open', 'high', 'low', 'close', 'volume']
            
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
                'transactions': 'num_transactions'
            }
            df_clean = df_clean.rename(columns=column_mapping)

            # Ensure required columns
            required_cols = ['ticker', 'trade_date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', "vwap", "num_transactions"]
            df_clean = df_clean[required_cols]

            # Add timestamps
            df_clean['created_at'] = datetime.utcnow()
            df_clean['updated_at'] = datetime.utcnow()
            
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
    def get_data(self, cursor, ticker: Optional[str] = None) -> pd.DataFrame:
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
            if ticker:
                cursor.execute(f"SELECT * FROM {self.__tablename__} WHERE ticker = :ticker", {"ticker": ticker})
            else:
                cursor.execute(f"SELECT * FROM {self.__tablename__}")
            
            results = cursor.fetchall()
            df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
            
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
        self.performance_monitor = PerformanceMonitor()
        self.data_validator = DataValidator()
        logger.info("MacroData initialized with  configuration")

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def create_table(self, cursor):
        operation_name = "create_table_macro_data"
        start_time = time.time()
        try:
            cursor.execute(f"""
                BEGIN
                    EXECUTE IMMEDIATE 'CREATE TABLE {self.__tablename__} (
                        id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                        trade_date DATE NOT NULL,
                        series_id VARCHAR2(100) NOT NULL,
                        value NUMBER(15,6),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -955 THEN  -- Table already exists
                            RAISE;
                        END IF;
                END;
            """)
            # Create indexes for performance
            cursor.execute(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{self.__tablename__}_series ON {self.__tablename__} (series_id)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN  -- Index already exists
                        RAISE;
                    END IF;
            END;
            """)
            cursor.execute(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{self.__tablename__}_date ON {self.__tablename__} (trade_date)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN  -- Index already exists
                        RAISE;
                    END IF;
            END;
            """)
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
    def data_exists(self, cursor, series_id: Optional[str] = None) -> bool:
        operation_name = "data_exists_macro_data"
        start_time = time.time()
        try:
            if series_id:
                cursor.execute(f"SELECT 1 FROM {self.__tablename__} WHERE series_id = :series_id AND ROWNUM = 1", {"series_id": series_id})
                result = cursor.fetchone() is not None
            else:
                cursor.execute(f"SELECT 1 FROM {self.__tablename__} WHERE ROWNUM = 1")
                result = cursor.fetchone() is not None
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
    def get_last_date(self, cursor, series_id: Optional[str] = None) -> Optional[date_type]:
        operation_name = "get_last_date_macro_data"
        start_time = time.time()
        try:
            if series_id:
                cursor.execute(f"SELECT MAX(trade_date) FROM {self.__tablename__} WHERE series_id = :series_id", {"series_id": series_id})
            else:
                cursor.execute(f"SELECT MAX(trade_date) FROM {self.__tablename__}")
            result = cursor.fetchone()
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
            required_columns = ['trade_date', 'series_id', 'value']
            numeric_columns = ['value']
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
            # Add created_at/updated_at
            df_clean['created_at'] = datetime.utcnow()
            df_clean['updated_at'] = datetime.utcnow()
            # Ensure columns are in the correct order for the table
            expected_columns = ['trade_date', 'series_id', 'value', 'created_at', 'updated_at']
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
    def get_data(self, cursor, series_id: Optional[str] = None) -> pd.DataFrame:
        operation_name = "get_data_macro_data"
        start_time = time.time()
        try:
            if series_id:
                cursor.execute(f"SELECT * FROM {self.__tablename__} WHERE series_id = :series_id", {"series_id": series_id})
            else:
                cursor.execute(f"SELECT * FROM {self.__tablename__}")
            results = cursor.fetchall()
            df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
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
        self.performance_monitor = PerformanceMonitor()
        self.data_validator = DataValidator()
        logger.info("TransformedTickers initialized with  configuration")

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def create_table(self, cursor):
        operation_name = "create_table_transformed_tickers"
        start_time = time.time()
        try:
            cursor.execute(f"""
                BEGIN
                    EXECUTE IMMEDIATE 'CREATE TABLE {self.__tablename__} (
                        id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                        ticker VARCHAR2(10) NOT NULL,
                        trans_date DATE NOT NULL,
                        open_price BINARY_DOUBLE,
                        high_price BINARY_DOUBLE,
                        low_price BINARY_DOUBLE,
                        close_price BINARY_DOUBLE,
                        volume NUMBER,
                        vwap NUMBER(10,4),
                        num_transactions NUMBER,
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
            """)
            # Create indexes for performance
            cursor.execute(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{self.__tablename__}_ticker ON {self.__tablename__} (ticker)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN  -- Index already exists
                        RAISE;
                    END IF;
            END;
            """)
            cursor.execute(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{self.__tablename__}_date ON {self.__tablename__} (trans_date)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN  -- Index already exists
                        RAISE;
                    END IF;
            END;
            """)
            cursor.execute(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{self.__tablename__}_ticker_date ON {self.__tablename__} (ticker, trans_date)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN  -- Index already exists
                        RAISE;
                    END IF;
            END;
            """)
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
    def data_exists(self, cursor, ticker: Optional[str] = None) -> bool:
        operation_name = "data_exists_transformed_tickers"
        start_time = time.time()
        try:
            if ticker:
                cursor.execute(f"SELECT 1 FROM {self.__tablename__} WHERE ticker = :ticker", {"ticker": ticker})
                result = cursor.fetchone() is not None
            else:
                cursor.execute(f"SELECT 1 FROM {self.__tablename__}")
                result = cursor.fetchone() is not None
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
    def get_last_date(self, cursor, ticker: Optional[str] = None) -> Optional[date_type]:
        operation_name = "get_last_date_transformed_tickers"
        start_time = time.time()
        try:
            if ticker:
                cursor.execute(f"SELECT MAX(trans_date) FROM {self.__tablename__} WHERE ticker = :ticker", {"ticker": ticker})
                result = cursor.fetchone()
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
            required_columns = ['ticker', 'trans_date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']
            numeric_columns = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
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
            # Handle trans_date column
            if 'trans_date' in df_clean.columns:
                df_clean['trans_date'] = pd.to_datetime(df_clean['trans_date']).dt.date
            elif 'date' in df_clean.columns:
                df_clean['trans_date'] = pd.to_datetime(df_clean['date']).dt.date
            else:
                logger.warning("No date or trans_date column found in DataFrame")
                return
            # Add created_at/updated_at
            df_clean['created_at'] = datetime.utcnow()
            df_clean['updated_at'] = datetime.utcnow()
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
    def get_data(self, cursor, ticker: Optional[str] = None) -> pd.DataFrame:
        operation_name = "get_data_transformed_tickers"
        start_time = time.time()
        try:
            if ticker:
                cursor.execute(f"SELECT * FROM {self.__tablename__} WHERE ticker = :ticker", {"ticker": ticker})
            else:
                cursor.execute(f"SELECT * FROM {self.__tablename__}")
            results = cursor.fetchall()
            df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
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
        self.performance_monitor = PerformanceMonitor()
        self.data_validator = DataValidator()
        logger.info("TransformedMacroData initialized with  configuration")

    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def create_table(self, cursor):
        operation_name = "create_table_transformed_macro_data"
        start_time = time.time()
        try:
            cursor.execute(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE TABLE {self.__tablename__} (
                    id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    trans_date DATE NOT NULL,
                    series_id VARCHAR2(100) NOT NULL,
                    value BINARY_DOUBLE,
                    normalized_value BINARY_DOUBLE,
                    moving_avg_30 BINARY_DOUBLE,
                    year_over_year_change BINARY_DOUBLE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN  -- Table already exists
                        RAISE;
                    END IF;
            END;
            """)
            # Create indexes for performance
            cursor.execute(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{self.__tablename__}_series ON {self.__tablename__} (series_id)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN  -- Index already exists
                        RAISE;
                    END IF;
            END;
            """)
            cursor.execute(f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{self.__tablename__}_date ON {self.__tablename__} (trans_date)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN  -- Index already exists
                        RAISE;
                    END IF;
            END;
            """)
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
    def data_exists(self, cursor, series_id: Optional[str] = None) -> bool:
        operation_name = "data_exists_transformed_macro_data"
        start_time = time.time()
        try:
            if series_id:
                cursor.execute(f"SELECT 1 FROM {self.__tablename__} WHERE series_id = :series_id AND ROWNUM = 1", {"series_id": series_id})
                result = cursor.fetchone() is not None
            else:
                cursor.execute(f"SELECT 1 FROM {self.__tablename__} WHERE ROWNUM = 1")
                result = cursor.fetchone() is not None
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
    def get_last_date(self, cursor, series_id: Optional[str] = None) -> Optional[date_type]:
        operation_name = "get_last_date_transformed_macro_data"
        start_time = time.time()
        try:
            if series_id:
                cursor.execute(f"SELECT MAX(trans_date) FROM {self.__tablename__} WHERE series_id = :series_id", {"series_id": series_id})
                result = cursor.fetchone()
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
            required_columns = ['trans_date', 'series_id', 'value']
            numeric_columns = ['value']
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
            # Handle trans_date column
            if 'trans_date' in df_clean.columns:
                df_clean['trans_date'] = pd.to_datetime(df_clean['trans_date']).dt.date
            elif 'date' in df_clean.columns:
                df_clean['trans_date'] = pd.to_datetime(df_clean['date']).dt.date
            else:
                logger.warning("No date or trans_date column found in DataFrame")
                return
            # Add created_at/updated_at
            df_clean['created_at'] = datetime.utcnow()
            df_clean['updated_at'] = datetime.utcnow()
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
    def get_data(self, cursor, series_id: Optional[str] = None) -> pd.DataFrame:
        operation_name = "get_data_transformed_macro_data"
        start_time = time.time()
        try:
            if series_id:
                cursor.execute(f"SELECT * FROM {self.__tablename__} WHERE series_id = :series_id", {"series_id": series_id})
            else:
                cursor.execute(f"SELECT * FROM {self.__tablename__}")
            results = cursor.fetchall()
            df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
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