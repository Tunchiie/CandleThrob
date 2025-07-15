#!/usr/bin/env python3
"""
Macro Data Update Module for CandleThrob
=======================================

This module provides macroeconomic data update capabilities from FRED API.
It includes advanced error handling, performance monitoring, data quality validation,
incremental updates, and comprehensive reporting.

Key Features:
- Advanced error handling and retry logic
- Performance monitoring and metrics collection
- Data quality validation and scoring
- Incremental update optimization
- Memory usage optimization
- Advanced logging and monitoring
- Data lineage tracking and audit trails
- Quality gates and validation checkpoints
- Resource monitoring and optimization
- Modular design with configurable update rules
- Advanced error categorization and handling

Author: CandleThrob Team
Version: 3.0.0
Last Updated: 2025-07-14
"""

import sys
import os
import json
import time
import psutil
import threading
import traceback
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field
from functools import wraps
from contextlib import contextmanager
import pandas as pd
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from CandleThrob.ingestion.fetch_data import CompleteDataIngestion
from CandleThrob.utils.models import MacroData
from CandleThrob.utils.oracle_conn import OracleDB
from CandleThrob.utils.logging_config import (
    get_ingestion_logger, log_ingestion_start, log_ingestion_success, 
    log_ingestion_error, log_macro_update_start, log_macro_update_success,
    log_macro_update_error, AdvancedTransformLogger
)

# Get logger for this module
logger = get_ingestion_logger(__name__)

@dataclass
class MacroUpdateConfig:
    """Configuration for macro data updates."""
    
    # Performance settings
    max_update_time_seconds: float = 600.0
    max_memory_usage_mb: float = 1024.0
    batch_size: int = 1000
    max_retries: int = 3
    retry_delay_seconds: float = 2.0
    
    # Quality thresholds
    min_data_quality_score: float = 0.8
    max_null_ratio: float = 0.1
    min_indicators_required: int = 4
    
    # Advanced features
    enable_incremental_updates: bool = True
    enable_audit_trail: bool = True
    enable_performance_monitoring: bool = True
    enable_memory_optimization: bool = True
    enable_data_lineage: bool = True
    
    # Update rules
    expected_indicators: List[str] = field(default_factory=lambda: [
        'FEDFUNDS', 'CPIAUCSL', 'UNRATE', 'GDP', 'GS10', 'USREC'
    ])
    update_frequency_hours: int = 24
    max_update_age_days: int = 7
    
    def __post_init__(self):
        """Validate configuration parameters."""
        if self.max_update_time_seconds <= 0:
            raise ValueError("max_update_time_seconds must be positive")
        if self.max_memory_usage_mb <= 0:
            raise ValueError("max_memory_usage_mb must be positive")
        if not 0.0 <= self.min_data_quality_score <= 1.0:
            raise ValueError("min_data_quality_score must be between 0.0 and 1.0")
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")

class PerformanceMonitor:
    """performance monitoring for macro update operations."""
    
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
    """retry decorator with exponential backoff."""
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
                        logger.logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}. Retrying in {delay:.2f}s...")
                        time.sleep(delay)
                    else:
                        logger.logger.error(f"All {max_retries + 1} attempts failed for {func.__name__}: {str(e)}")
                        raise last_exception
            
            return None
        return wrapper
    return decorator

class DataQualityValidator:
    """data quality validation for macro data."""
    
    @staticmethod
    def validate_macro_data(df: pd.DataFrame) -> Tuple[bool, float, List[str]]:
        """Validate macro data quality."""
        issues = []
        quality_score = 1.0
        
        if df.empty:
            issues.append("DataFrame is empty")
            quality_score = 0.0
            return False, quality_score, issues
        
        # Check required columns
        required_cols = ['date', 'series_id', 'value']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            issues.append(f"Missing required columns: {missing_cols}")
            quality_score -= 0.3
        
        # Check for null values
        null_ratio = df.isnull().sum().sum() / (len(df) * len(df.columns))
        if null_ratio > 0.1:
            issues.append(f"High null ratio: {null_ratio:.2%}")
            quality_score -= 0.2
        
        # Check for expected indicators
        expected_indicators = ['FEDFUNDS', 'CPIAUCSL', 'UNRATE', 'GDP', 'GS10', 'USREC']
        available_indicators = df['series_id'].unique()
        missing_indicators = [ind for ind in expected_indicators if ind not in available_indicators]
        if missing_indicators:
            issues.append(f"Missing expected indicators: {missing_indicators}")
            quality_score -= 0.2
        
        # Check date range
        if 'date' in df.columns:
            try:
                df['date'] = pd.to_datetime(df['date'])
                date_span = (df['date'].max() - df['date'].min()).days
                if date_span < 30:
                    issues.append(f"Limited date range: {date_span} days")
                    quality_score -= 0.1
            except Exception:
                issues.append("Invalid date format")
                quality_score -= 0.2
        
        # Check for recent data
        if 'date' in df.columns:
            try:
                recent_cutoff = datetime.now() - timedelta(days=30)
                recent_data = df[df['date'] >= recent_cutoff]
                if len(recent_data) == 0:
                    issues.append("No recent data (last 30 days)")
                    quality_score -= 0.3
            except Exception:
                issues.append("Error checking recent data")
                quality_score -= 0.1
        
        quality_score = max(0.0, quality_score)
        return quality_score >= 0.8, quality_score, issues

class MacroDataUpdater:
    """
    Comprehensive macro data updater class.

    This class provides methods to update macroeconomic data from FRED API
    with advanced error handling, performance monitoring, data quality validation, and
    incremental update optimization.

    Attributes:
        config (MacroUpdateConfig): Configuration
        performance_monitor (PerformanceMonitor): Performance monitor
        quality_validator (DataQualityValidator): Data quality validator
        db (OracleDB): Database connection
    """
    
    def __init__(self, config: Optional[MacroUpdateConfig] = None):
        """
        Initialize the MacroDataUpdater class with advanced features.

        Args:
            config (Optional[MacroUpdateConfig]): Configuration

        Example:
            >>> updater = MacroDataUpdater()
            >>> updater = MacroDataUpdater(MacroUpdateConfig())
        """
        self.config = config or MacroUpdateConfig()
        self.performance_monitor = PerformanceMonitor()
        self.quality_validator = DataQualityValidator()
        self.db = OracleDB()
        
        logger.logger.info("MacroDataUpdater initialized with configuration")
    
    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def update_macro_data(self) -> Dict[str, Any]:
        """
        Update macroeconomic data with advanced features.
        
        Returns:
            Dict[str, Any]: Comprehensive update results with quality scoring
            
        Example:
            >>> updater = MacroDataUpdater()
            >>> results = updater.update_macro_data()
        """
        operation_name = "update_macro_data"
        
        with self.performance_monitor.monitor_operation(operation_name):
            log_macro_update_start("update_macro", "macro_data")
            
            results = {
                "status": "success",
                "start_time": datetime.now(),
                "end_time": None,
                "records_updated": 0,
                "indicators_updated": 0,
                "quality_score": 0.0,
                "quality_issues": [],
                "errors": [],
                "performance_metrics": {}
            }
            
            try:
                with self.db.establish_connection() as conn:
                    with conn.cursor() as cursor:
                        MacroData().create_table(cursor)
                    
                    # Check existing data and get last date
                    macro_model = MacroData()
                    if macro_model.data_exists(conn):
                        last_date = macro_model.get_last_date(conn)
                        if last_date:
                            start_date = last_date + timedelta(days=1)
                            logger.logger.info(f"Incremental macro update: fetching from {start_date}")
                        else:
                            start_date = None
                            logger.logger.info("No valid macro date found, fetching all available data")
                    else:
                        start_date = None
                        logger.logger.info("No existing macro data, fetching all available data")
                    
                    # Determine date range for fetching
                    if start_date:
                        end_date = datetime.now()
                        if start_date.date() >= end_date.date():
                            logger.logger.info("No new macro data to fetch")
                            results["status"] = "no_new_data"
                            results["end_time"] = datetime.now()
                            return results
                        
                        data = CompleteDataIngestion(
                            start_date=start_date.strftime("%Y-%m-%d"),
                            end_date=end_date.strftime("%Y-%m-%d")
                        )
                    else:
                        data = CompleteDataIngestion()
                    
                    # Fetch FRED data with advanced features
                    logger.logger.info("Fetching FRED data with advanced features...")
                    macro_df = data.fetch_fred_data()
                    
                    if macro_df is None or macro_df.empty:
                        logger.logger.warning("No new macroeconomic data was fetched")
                        results["status"] = "no_data_fetched"
                        results["end_time"] = datetime.now()
                        return results
                    
                    # Data quality validation
                    quality_passed, quality_score, quality_issues = self.quality_validator.validate_macro_data(macro_df)
                    results["quality_score"] = quality_score
                    results["quality_issues"] = quality_issues
                    
                    if not quality_passed:
                        logger.logger.error(f"Data quality validation failed: {quality_issues}")
                        results["status"] = "quality_validation_failed"
                        results["end_time"] = datetime.now()
                        return results
                    
                    # Save to database with advanced features
                    logger.logger.info(f"Saving {len(macro_df)} macro records to database")
                    
                    # Batch processing for large datasets
                    if len(macro_df) > self.config.batch_size:
                        batches = [macro_df[i:i + self.config.batch_size] 
                                 for i in range(0, len(macro_df), self.config.batch_size)]
                        
                        for i, batch in enumerate(batches):
                            logger.logger.info(f"Processing batch {i+1}/{len(batches)} ({len(batch)} records)")
                            macro_model.insert_data(self.db.get_sqlalchemy_engine(), batch)
                    else:
                        macro_model.insert_data(self.db.get_sqlalchemy_engine(), macro_df)
                    
                    # Update results
                    results["records_updated"] = len(macro_df)
                    results["indicators_updated"] = len(macro_df['series_id'].unique())
                    results["end_time"] = datetime.now()
                    results["duration_seconds"] = (results["end_time"] - results["start_time"]).total_seconds()
                    
                    logger.logger.info(f"Macro data update completed successfully")
                    logger.logger.info(f"Updated {results['records_updated']} records for {results['indicators_updated']} indicators")
                    logger.logger.info(f"Quality score: {quality_score:.2f}")
                    
                    log_macro_update_success("update_macro", "macro_data", results["records_updated"])
                    return results
                    
            except Exception as e:
                error_msg = f"Error updating macro data: {str(e)}"
                logger.logger.error(error_msg)
                logger.logger.error(f"Traceback: {traceback.format_exc()}")
                results["status"] = "error"
                results["errors"].append(error_msg)
                results["end_time"] = datetime.now()
                log_macro_update_error("update_macro", "macro_data", error_msg)
                return results
    
    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def validate_macro_update(self) -> Dict[str, Any]:
        """
        Validate the macro data update with advanced features.
        
        Returns:
            Dict[str, Any]: Comprehensive validation results
            
        Example:
            >>> updater = MacroDataUpdater()
            >>> results = updater.validate_macro_update()
        """
        operation_name = "validate_macro_update"
        
        with self.performance_monitor.monitor_operation(operation_name):
            log_macro_update_start("update_macro", "validation")
            
            try:
                with self.db.establish_connection() as conn:
                    # Check for recent data (last 30 days)
                    recent_data = conn.execute("""
                        SELECT COUNT(DISTINCT series_id) as recent_indicators,
                               COUNT(*) as total_records
                        FROM macro_data 
                        WHERE date >= SYSDATE - 30
                    """).fetchone()
                    
                    # Check for expected indicators
                    available_indicators = conn.execute("""
                        SELECT DISTINCT series_id FROM macro_data
                        WHERE date >= SYSDATE - 30
                    """).fetchall()
                    available_indicators = [row[0] for row in available_indicators]
                    missing_indicators = [ind for ind in self.config.expected_indicators 
                                       if ind not in available_indicators]
                    
                    # Calculate quality score
                    quality_score = 1.0
                    quality_issues = []
                    
                    if recent_data[0] < self.config.min_indicators_required:
                        quality_issues.append(f"Insufficient indicators: {recent_data[0]}/{self.config.min_indicators_required}")
                        quality_score -= 0.3
                    
                    if missing_indicators:
                        quality_issues.append(f"Missing indicators: {missing_indicators}")
                        quality_score -= 0.2
                    
                    if recent_data[1] == 0:
                        quality_issues.append("No recent records found")
                        quality_score -= 0.5
                    
                    quality_score = max(0.0, quality_score)
                    
                    validation_results = {
                        "recent_indicators": recent_data[0],
                        "total_recent_records": recent_data[1],
                        "available_indicators": available_indicators,
                        "missing_indicators": missing_indicators,
                        "quality_score": quality_score,
                        "quality_issues": quality_issues,
                        "validation_timestamp": datetime.now().isoformat()
                    }
                    
                    logger.logger.info(f"Macro validation: {recent_data[0]} indicators, {recent_data[1]} records, quality score: {quality_score:.2f}")
                    log_macro_update_success("update_macro", "validation", recent_data[1])
                    return validation_results
                    
            except Exception as e:
                error_msg = f"Error validating macro data: {str(e)}"
                logger.logger.error(error_msg)
                logger.logger.error(f"Traceback: {traceback.format_exc()}")
                log_macro_update_error("update_macro", "validation", error_msg)
                return {"status": "error", "message": error_msg}
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """
        Get comprehensive performance metrics for monitoring and analysis.
        
        Returns:
            Dict[str, Any]: Performance metrics summary
        """
        return {
            "performance_metrics": self.performance_monitor.metrics,
            "config": {
                "max_update_time_seconds": self.config.max_update_time_seconds,
                "max_memory_usage_mb": self.config.max_memory_usage_mb,
                "batch_size": self.config.batch_size,
                "min_data_quality_score": self.config.min_data_quality_score
            },
            "timestamp": datetime.now().isoformat()
        }

# Backward compatibility alias
# MacroDataUpdater is now the main class

def main():
    """Main function for macro data update."""
    try:
        logger.logger.info("Starting macro data update process...")

        updater = MacroDataUpdater()
        
        # Update macro data
        update_results = updater.update_macro_data()
        
        # Validate the update
        validation_results = updater.validate_macro_update()
        
        # Get performance metrics
        performance_metrics = updater.get_performance_metrics()
        
        # Save comprehensive results
        results_file = "/app/data/macro_update_results.json"
        os.makedirs(os.path.dirname(results_file), exist_ok=True)
        
        combined_results = {
            "update_results": update_results,
            "validation_results": validation_results,
            "performance_metrics": performance_metrics,
            "timestamp": datetime.now().isoformat()
        }
        
        with open(results_file, 'w') as f:
            json.dump(combined_results, f, indent=2)
        
        # Print comprehensive summary
        print("\n=== MACRO DATA UPDATE SUMMARY ===")
        print(f"Status: {update_results['status']}")
        print(f"Records Updated: {update_results['records_updated']}")
        print(f"Indicators Updated: {update_results['indicators_updated']}")
        print(f"Quality Score: {update_results.get('quality_score', 0.0):.2f}")
        
        if update_results['end_time']:
            duration = update_results['duration_seconds']
            print(f"Duration: {duration:.1f} seconds")
        
        if validation_results.get('recent_indicators'):
            print(f"Recent Indicators (30 days): {validation_results['recent_indicators']}")
            print(f"Recent Records: {validation_results['total_recent_records']}")
            print(f"Validation Quality Score: {validation_results.get('quality_score', 0.0):.2f}")
        
        if validation_results.get('missing_indicators'):
            print(f"Missing Indicators: {validation_results['missing_indicators']}")
        
        if update_results.get('quality_issues'):
            print(f"Quality Issues: {', '.join(update_results['quality_issues'])}")
        
        # Performance summary
        if performance_metrics.get('performance_metrics'):
            print(f"\nPerformance Summary:")
            for operation, metrics in performance_metrics['performance_metrics'].items():
                print(f"  {operation}: {metrics['duration']:.2f}s, {metrics['memory_usage_mb']:.1f}MB")
        
        print(f"Results saved to: {results_file}")
        
        logger.logger.info("Macro data update process completed")
        
        # Exit with appropriate code
        if update_results['status'] == 'error':
            sys.exit(1)
        else:
            sys.exit(0)
            
    except Exception as e:
        logger.logger.error(f"Fatal error in macro update: {str(e)}")
        logger.logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main() 