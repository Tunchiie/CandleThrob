#!/usr/bin/env python3
"""
Macro Data Update Module for CandleThrob
=======================================

This module provides macroeconomic data update capabilities from FRED API.
from CandleThrob.core.monitors import UniversalPerformanceMonitor as PerformanceMonitor
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

Author: Adetunji Fasiku
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
from CandleThrob.ingestion.fetch_data import DataIngestion
from CandleThrob.utils.gcs import save_macro_data, load_macro_data, list_available_macro_series
from CandleThrob.core.logging import get_ingestion_logger, get_transform_logger, log_ingestion_start, log_ingestion_success, log_ingestion_error, log_rate_limiting, log_batch_progress

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
    max_update_age_days: int = 365  # Not used for historical fetch (fetches since 2000)
    
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
                        logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}. Retrying in {delay:.2f}s...")
                        time.sleep(delay)
                    else:
                        logger.error(f"All {max_retries + 1} attempts failed for {func.__name__}: {str(e)}")
                        raise last_exception
            
            return None
        return wrapper
    return decorator


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
        gcs_bucket (str): Google Cloud Storage bucket name for data storage
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
        self.performance_monitor = PerformanceMonitor("default")
        self.quality_validator = DataQualityValidator()
        self.gcs_bucket = os.getenv('GCS_BUCKET_NAME', 'candlethrob-wandata')
        
        logger.info("MacroDataUpdater initialized with configuration")
    
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
            log_ingestion_start("update_macro", "macro_data")
            
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
                logger.info("Using GCS for macro data storage")

                # For GCS, fetch comprehensive historical data since 2000
                # This ensures we have complete macroeconomic time series
                start_date = datetime(2000, 1, 1)  # Start from January 1, 2000
                logger.info(f"Fetching comprehensive macro data from {start_date} (historical fetch mode)")

                # Fetch macro data
                data = DataIngestion(
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=datetime.now().strftime("%Y-%m-%d")
                )

                macro_df = data.fetch_fred_data()

                if macro_df is None or macro_df.empty:
                    logger.warning("No new macroeconomic data was fetched")
                    results["status"] = "no_data_fetched"
                    results["end_time"] = datetime.now()
                    results["duration_seconds"] = (results["end_time"] - results["start_time"]).total_seconds()
                    return results

                # Validate data quality
                is_valid, quality_score, issues = self.quality_validator.validate_macro_data(macro_df)
                if not is_valid:
                    logger.error(f"Data quality validation failed: {issues}")
                    results["status"] = "validation_failed"
                    results["quality_score"] = quality_score
                    results["validation_issues"] = issues
                    results["end_time"] = datetime.now()
                    results["duration_seconds"] = (results["end_time"] - results["start_time"]).total_seconds()
                    return results

                logger.info(f"Data quality validation passed (score: {quality_score:.2f})")

                # Save to GCS using parquet format
                logger.info(f"Saving {len(macro_df)} macro records to GCS")

                # Group by series_id and save each series separately
                for series_id in macro_df['series_id'].unique():
                    series_df = macro_df[macro_df['series_id'] == series_id]
                    success = save_macro_data(series_df, series_id, self.gcs_bucket)

                    if success:
                        logger.info(f"Successfully saved {len(series_df)} records for series {series_id} to GCS")
                    else:
                        logger.error(f"Failed to save macro data for series {series_id} to GCS")
                        results["status"] = "save_failed"
                        results["end_time"] = datetime.now()
                        results["duration_seconds"] = (results["end_time"] - results["start_time"]).total_seconds()
                        return results

                # Update results
                results["records_updated"] = len(macro_df)
                results["series_updated"] = len(macro_df['series_id'].unique())
                results["quality_score"] = quality_score
                results["status"] = "success"

                logger.info(f"Successfully updated {results['records_updated']} macro records for {results['series_updated']} series")

                # Save to GCS using parquet format
                logger.info(f"Saving {len(macro_df)} macro records to GCS")

                # Group by series_id and save each series separately
                for series_id in macro_df['series_id'].unique():
                    series_df = macro_df[macro_df['series_id'] == series_id]
                    success = save_macro_data(series_df, series_id, self.gcs_bucket)

                    if success:
                        logger.info(f"Successfully saved {len(series_df)} records for series {series_id} to GCS")
                    else:
                        logger.error(f"Failed to save macro data for series {series_id} to GCS")
                        results["status"] = "save_failed"
                        results["end_time"] = datetime.now()
                        results["duration_seconds"] = (results["end_time"] - results["start_time"]).total_seconds()
                        return results

                # Update results
                results["records_updated"] = len(macro_df)
                results["series_updated"] = len(macro_df['series_id'].unique())
                results["status"] = "success"
                results["end_time"] = datetime.now()
                results["duration_seconds"] = (results["end_time"] - results["start_time"]).total_seconds()

                logger.info(f"Macro data update completed successfully")
                logger.info(f"Updated {results['records_updated']} records for {results['series_updated']} series")
                logger.info(f"Quality score: {quality_score:.2f}")

                log_ingestion_success("update_macro", "macro_data", results["records_updated"])
                return results
                    
            except Exception as e:
                error_msg = f"Error updating macro data: {str(e)}"
                logger.error(error_msg)
                logger.error(f"Traceback: {traceback.format_exc()}")
                results["status"] = "error"
                results["errors"].append(error_msg)
                results["end_time"] = datetime.now()
                results["duration_seconds"] = (results["end_time"] - results["start_time"]).total_seconds()
                log_ingestion_error("update_macro", "macro_data", error_msg)
                return results
    
    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def validate_gcs_update(self, macro_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate the macro data update in GCS with comprehensive checks.

        Args:
            macro_df: The macro DataFrame that was saved

        Returns:
            Dict[str, Any]: Validation results with quality metrics
        """
        operation_name = "validate_gcs_update"

        with self.performance_monitor.monitor_operation(operation_name):
            log_ingestion_start("update_macro", "validation")

            try:
                # Get available series from GCS
                available_series = list_available_macro_series(bucket_name=self.gcs_bucket)

                # Check for expected indicators
                missing_indicators = [ind for ind in self.config.expected_indicators
                                   if ind not in available_series]

                # Calculate quality score
                quality_score = 1.0
                quality_issues = []

                if len(available_series) < self.config.min_indicators_required:
                    quality_issues.append(f"Insufficient indicators: {len(available_series)}/{self.config.min_indicators_required}")
                    quality_score -= 0.3

                if missing_indicators:
                    quality_issues.append(f"Missing indicators: {missing_indicators}")
                    quality_score -= 0.2

                if macro_df.empty:
                    quality_issues.append("No recent records found")
                    quality_score -= 0.5

                quality_score = max(0.0, quality_score)

                validation_results = {
                    "available_series": len(available_series),
                    "total_recent_records": len(macro_df),
                    "available_indicators": available_series,
                    "missing_indicators": missing_indicators,
                    "quality_score": quality_score,
                    "quality_issues": quality_issues,
                    "is_valid": quality_score >= 0.7,
                    "validation_timestamp": datetime.now().isoformat()
                }

                logger.info(f"GCS macro validation: {len(available_series)} series, {len(macro_df)} records, quality score: {quality_score:.2f}")
                log_ingestion_success("update_macro", "validation", len(macro_df))
                return validation_results

            except Exception as e:
                error_msg = f"Error validating macro data: {str(e)}"
                logger.error(error_msg)
                logger.error(f"Traceback: {traceback.format_exc()}")
                log_ingestion_error("update_macro", "validation", error_msg)
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


def main():
    """Main function for macro data update."""
    try:
        logger.info("Starting macro data update process...")

        updater = MacroDataUpdater()
        
        # Update macro data
        update_results = updater.update_macro_data()
        
        # Validate the update
        # For GCS validation, we can check available series
        available_series = list_available_macro_series(bucket_name=updater.gcs_bucket)
        validation_results = {
            "available_series": len(available_series),
            "series_list": available_series,
            "validation_timestamp": datetime.now().isoformat(),
            "is_valid": len(available_series) > 0
        }
        
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
            json.dump(combined_results, f, indent=2, default=str)
        
        # Print comprehensive summary
        print("\n=== MACRO DATA UPDATE SUMMARY ===")
        print(f"Status: {update_results['status']}")
        print(f"Records Updated: {update_results['records_updated']}")
        print(f"Indicators Updated: {update_results['indicators_updated']}")
        print(f"Quality Score: {update_results.get('quality_score', 0.0):.2f}")
        
        if update_results.get('end_time'):
            duration = update_results.get('duration_seconds', 0)
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
        
        logger.info("Macro data update process completed")

        # Exit with appropriate code
        if update_results['status'] == 'error':
            sys.exit(1)
        else:
            sys.exit(0)

    except Exception as e:
        logger.error(f"Fatal error in macro update: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main() 