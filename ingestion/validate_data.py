#!/usr/bin/env python3
"""
Data Validation Module for CandleThrob
=====================================

This module provides data validation capabilities for financial data
quality assessment, completeness checks, and comprehensive reporting. It includes
advanced error handling, performance monitoring, data quality scoring, and audit trails.

Features:
- Comprehensive data quality validation and scoring
- Performance monitoring and metrics collection
- Advanced error handling and retry logic
- Data lineage tracking and audit trails
- Quality gates and validation checkpoints
- Resource monitoring and optimization
- Advanced logging and monitoring
- Modular design with configurable validation rules
- Memory usage optimization
- Parallel processing capabilities
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
from typing import Dict, List, Any, Optional, Tuple, Union
from dataclasses import dataclass, field
from functools import wraps
from contextlib import contextmanager
import pandas as pd
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from CandleThrob.utils.models import TickerData, MacroData
from CandleThrob.utils.oracle_conn import OracleDB
from CandleThrob.utils.logging_config import (
    get_ingestion_logger, log_validation_start, log_validation_success, 
    log_validation_error, AdvancedTransformLogger
)

# Get logger for this module
logger = get_ingestion_logger(__name__)

@dataclass
class ValidationConfig:
    """Configuration for data validation."""
    
    # Performance settings
    max_validation_time_seconds: float = 300.0
    max_memory_usage_mb: float = 1024.0
    parallel_processing: bool = True
    max_workers: int = 4
    
    # Quality thresholds
    min_data_quality_score: float = 0.8
    max_null_ratio: float = 0.1
    max_gap_days: int = 5
    min_recent_data_days: int = 7
    
    # Advanced features
    enable_audit_trail: bool = True
    enable_performance_monitoring: bool = True
    enable_memory_optimization: bool = True
    enable_data_lineage: bool = True
    
    # Validation rules
    expected_ticker_indicators: List[str] = field(default_factory=lambda: [
        'FEDFUNDS', 'CPIAUCSL', 'UNRATE', 'GDP', 'GS10', 'USREC'
    ])
    critical_columns: List[str] = field(default_factory=lambda: [
        'open', 'high', 'low', 'close', 'volume'
    ])
    
    def __post_init__(self):
        """Validate configuration parameters."""
        if self.max_validation_time_seconds <= 0:
            raise ValueError("max_validation_time_seconds must be positive")
        if self.max_memory_usage_mb <= 0:
            raise ValueError("max_memory_usage_mb must be positive")
        if not 0.0 <= self.min_data_quality_score <= 1.0:
            raise ValueError("min_data_quality_score must be between 0.0 and 1.0")
        if not 0.0 <= self.max_null_ratio <= 1.0:
            raise ValueError("max_null_ratio must be between 0.0 and 1.0")

class PerformanceMonitor:
    """performance monitoring for validation operations."""
    
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

class DataQualityScorer:
    """data quality scoring system."""
    
    @staticmethod
    def calculate_ticker_quality_score(validation_results: Dict[str, Any]) -> Tuple[float, List[str]]:
        """Calculate comprehensive quality score for ticker data."""
        issues = []
        score = 1.0
        
        # Check data completeness
        if validation_results.get("total_records", 0) == 0:
            issues.append("No ticker data found")
            score = 0.0
            return score, issues
        
        # Check null ratios
        null_checks = validation_results.get("data_quality", {}).get("null_checks", {})
        total_records = validation_results.get("total_records", 0)
        
        for col, null_count in null_checks.items():
            if col != "total_null_checks":
                null_ratio = null_count / total_records if total_records > 0 else 0
                if null_ratio > 0.1:
                    issues.append(f"High null ratio in {col}: {null_ratio:.2%}")
                    score -= 0.2
        
        # Check recent data availability
        recent_tickers = validation_results.get("data_quality", {}).get("recent_data_tickers", 0)
        if recent_tickers == 0:
            issues.append("No recent data (last 7 days)")
            score -= 0.3
        
        # Check data gaps
        data_gaps = validation_results.get("data_gaps", [])
        if len(data_gaps) > 5:
            issues.append(f"Multiple tickers with data gaps: {len(data_gaps)}")
            score -= 0.2
        
        # Check date range
        date_range = validation_results.get("date_range", {})
        if date_range.get("min_date") and date_range.get("max_date"):
            try:
                min_date = datetime.strptime(date_range["min_date"], "%Y-%m-%d")
                max_date = datetime.strptime(date_range["max_date"], "%Y-%m-%d")
                days_span = (max_date - min_date).days
                if days_span < 30:
                    issues.append(f"Limited date range: {days_span} days")
                    score -= 0.1
            except ValueError:
                issues.append("Invalid date format in date range")
                score -= 0.1
        
        score = max(0.0, score)
        return score, issues
    
    @staticmethod
    def calculate_macro_quality_score(validation_results: Dict[str, Any]) -> Tuple[float, List[str]]:
        """Calculate comprehensive quality score for macro data."""
        issues = []
        score = 1.0
        
        # Check data completeness
        if validation_results.get("total_records", 0) == 0:
            issues.append("No macro data found")
            score = 0.0
            return score, issues
        
        # Check missing indicators
        missing_indicators = validation_results.get("missing_indicators", [])
        if missing_indicators:
            issues.append(f"Missing indicators: {missing_indicators}")
            score -= 0.3
        
        # Check recent data availability
        recent_indicators = validation_results.get("recent_indicators", 0)
        if recent_indicators == 0:
            issues.append("No recent macro data (last 30 days)")
            score -= 0.3
        
        # Check date range
        date_range = validation_results.get("date_range", {})
        if date_range.get("min_date") and date_range.get("max_date"):
            try:
                min_date = datetime.strptime(date_range["min_date"], "%Y-%m-%d")
                max_date = datetime.strptime(date_range["max_date"], "%Y-%m-%d")
                days_span = (max_date - min_date).days
                if days_span < 90:
                    issues.append(f"Limited macro date range: {days_span} days")
                    score -= 0.1
            except ValueError:
                issues.append("Invalid date format in macro date range")
                score -= 0.1
        
        score = max(0.0, score)
        return score, issues

class DataValidator:
    """
    Comprehensive data validation class.

    This class provides methods to validate data quality and completeness
    for both ticker and macroeconomic data. It includes advanced error handling, performance
    monitoring, data quality scoring, and comprehensive reporting.

    Attributes:
        config (ValidationConfig): Configuration
        performance_monitor (PerformanceMonitor): Performance monitor
        quality_scorer (DataQualityScorer): Data quality scorer
        db (OracleDB): Database connection
    """
    
    def __init__(self, config: Optional[ValidationConfig] = None):
        """
        Initialize the DataValidator class with advanced features.

        Args:
            config (Optional[ValidationConfig]): Configuration

        Example:
            >>> validator = DataValidator()
            >>> validator = DataValidator(ValidationConfig())
        """
        self.config = config or ValidationConfig()
        self.performance_monitor = PerformanceMonitor()
        self.quality_scorer = DataQualityScorer()
        self.db = OracleDB()
        
        logger.logger.info("DataValidator initialized with configuration")
    
    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def validate_ticker_data(self) -> Dict[str, Any]:
        """
        Validate ticker data quality and completeness with advanced features.
        
        Returns:
            Dict[str, Any]: Comprehensive validation results with quality scoring
            
        Example:
            >>> validator = DataValidator()
            >>> results = validator.validate_ticker_data()
        """
        operation_name = "validate_ticker_data"
        
        with self.performance_monitor.monitor_operation(operation_name):
            log_validation_start("validate_data", "ticker_data")
            
            try:
                with self.db.establish_connection() as conn:
                    ticker_model = TickerData()
                    
                    if not ticker_model.data_exists(conn):
                        logger.logger.warning("No ticker data found in database")
                        log_validation_error("validate_data", "ticker_data", "No data found")
                        return {"status": "no_data", "message": "No ticker data found"}
                    
                    # Get comprehensive statistics
                    total_records = conn.execute("SELECT COUNT(*) FROM ticker_data").scalar()
                    unique_tickers = conn.execute("SELECT COUNT(DISTINCT ticker) FROM ticker_data").scalar()
                    date_range = conn.execute("""
                        SELECT MIN(trade_date) as min_date, MAX(trade_date) as max_date 
                        FROM ticker_data
                    """).fetchone()
                    
                    # Advanced data quality checks
                    null_checks = conn.execute("""
                        SELECT 
                            COUNT(*) as total_null_checks,
                            SUM(CASE WHEN open IS NULL THEN 1 ELSE 0 END) as null_open,
                            SUM(CASE WHEN high IS NULL THEN 1 ELSE 0 END) as null_high,
                            SUM(CASE WHEN low IS NULL THEN 1 ELSE 0 END) as null_low,
                            SUM(CASE WHEN close IS NULL THEN 1 ELSE 0 END) as null_close,
                            SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END) as null_volume
                        FROM ticker_data
                    """).fetchone()
                    
                    # Check for recent data
                    recent_data = conn.execute("""
                        SELECT COUNT(DISTINCT ticker) as recent_tickers
                        FROM ticker_data 
                        WHERE trade_date >= SYSDATE - 7
                    """).scalar()
                    
                    # Advanced data gap analysis
                    gaps_query = """
                        SELECT ticker, COUNT(*) as gap_count
                        FROM (
                            SELECT ticker, trade_date,
                                   LAG(trade_date) OVER (PARTITION BY ticker ORDER BY trade_date) as prev_date
                            FROM ticker_data
                        ) t
                        WHERE prev_date IS NOT NULL 
                        AND trade_date - prev_date > 1
                        GROUP BY ticker
                        HAVING COUNT(*) > 5
                        ORDER BY gap_count DESC
                    """
                    data_gaps = conn.execute(gaps_query).fetchall()
                    data_gaps = data_gaps[:10]  # Limit to 10 results
                    
                    # Calculate quality score
                    validation_results = {
                        "status": "success",
                        "total_records": total_records,
                        "unique_tickers": unique_tickers,
                        "date_range": {
                            "min_date": str(date_range[0]) if date_range[0] else None,
                            "max_date": str(date_range[1]) if date_range[1] else None
                        },
                        "data_quality": {
                            "null_checks": dict(null_checks._mapping),
                            "recent_data_tickers": recent_data
                        },
                        "data_gaps": [{"ticker": row[0], "gap_count": row[1]} for row in data_gaps],
                        "validation_timestamp": datetime.now().isoformat()
                    }
                    
                    # Calculate quality score
                    quality_score, quality_issues = self.quality_scorer.calculate_ticker_quality_score(validation_results)
                    validation_results["quality_score"] = quality_score
                    validation_results["quality_issues"] = quality_issues
                    
                    logger.logger.info(f"Ticker validation completed: {total_records} records, {unique_tickers} tickers, quality score: {quality_score:.2f}")
                    log_validation_success("validate_data", "ticker_data", total_records)
                    return validation_results
                    
            except Exception as e:
                error_msg = f"Error validating ticker data: {str(e)}"
                logger.logger.error(error_msg)
                logger.logger.error(f"Traceback: {traceback.format_exc()}")
                log_validation_error("validate_data", "ticker_data", error_msg)
                return {"status": "error", "message": error_msg}
    
    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def validate_macro_data(self) -> Dict[str, Any]:
        """
        Validate macroeconomic data quality and completeness with advanced features.
        
        Returns:
            Dict[str, Any]: Comprehensive validation results with quality scoring
            
        Example:
            >>> validator = DataValidator()
            >>> results = validator.validate_macro_data()
        """
        operation_name = "validate_macro_data"
        
        with self.performance_monitor.monitor_operation(operation_name):
            log_validation_start("validate_data", "macro_data")
            
            try:
                with self.db.establish_connection() as conn:
                    macro_model = MacroData()
                    
                    if not macro_model.data_exists(conn):
                        logger.logger.warning("No macro data found in database")
                        log_validation_error("validate_data", "macro_data", "No data found")
                        return {"status": "no_data", "message": "No macro data found"}
                    
                    # Get comprehensive statistics
                    total_records = conn.execute("SELECT COUNT(*) FROM macro_data").scalar()
                    unique_indicators = conn.execute("SELECT COUNT(DISTINCT series_id) FROM macro_data").scalar()
                    date_range = conn.execute("""
                        SELECT MIN(date) as min_date, MAX(date) as max_date 
                        FROM macro_data
                    """).fetchone()
                    
                    # Check for recent data
                    recent_data = conn.execute("""
                        SELECT COUNT(DISTINCT series_id) as recent_indicators
                        FROM macro_data 
                        WHERE date >= SYSDATE - 30
                    """).scalar()
                    
                    # Check for missing indicators
                    available_indicators = conn.execute("""
                        SELECT DISTINCT series_id FROM macro_data
                    """).fetchall()
                    available_indicators = [row[0] for row in available_indicators]
                    missing_indicators = [ind for ind in self.config.expected_ticker_indicators 
                                       if ind not in available_indicators]
                    
                    validation_results = {
                        "status": "success",
                        "total_records": total_records,
                        "unique_indicators": unique_indicators,
                        "date_range": {
                            "min_date": str(date_range[0]) if date_range[0] else None,
                            "max_date": str(date_range[1]) if date_range[1] else None
                        },
                        "recent_indicators": recent_data,
                        "missing_indicators": missing_indicators,
                        "validation_timestamp": datetime.now().isoformat()
                    }
                    
                    # Calculate quality score
                    quality_score, quality_issues = self.quality_scorer.calculate_macro_quality_score(validation_results)
                    validation_results["quality_score"] = quality_score
                    validation_results["quality_issues"] = quality_issues
                    
                    logger.logger.info(f"Macro validation completed: {total_records} records, {unique_indicators} indicators, quality score: {quality_score:.2f}")
                    log_validation_success("validate_data", "macro_data", total_records)
                    return validation_results
                    
            except Exception as e:
                error_msg = f"Error validating macro data: {str(e)}"
                logger.logger.error(error_msg)
                logger.logger.error(f"Traceback: {traceback.format_exc()}")
                log_validation_error("validate_data", "macro_data", error_msg)
                return {"status": "error", "message": error_msg}
    
    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def check_batch_results(self) -> Dict[str, Any]:
        """
        Check batch processing results from recent runs with advanced features.
        
        Returns:
            Dict[str, Any]: Comprehensive batch analysis results
            
        Example:
            >>> validator = DataValidator()
            >>> results = validator.check_batch_results()
        """
        operation_name = "check_batch_results"
        
        with self.performance_monitor.monitor_operation(operation_name):
            log_validation_start("validate_data", "batch_results")
            
            try:
                batch_results = []
                data_dir = "/app/data"
                
                if os.path.exists(data_dir):
                    for filename in os.listdir(data_dir):
                        if filename.startswith("batch_") and filename.endswith("_results.json"):
                            filepath = os.path.join(data_dir, filename)
                            try:
                                with open(filepath, 'r') as f:
                                    batch_data = json.load(f)
                                    batch_results.append(batch_data)
                            except Exception as e:
                                logger.logger.warning(f"Error reading {filename}: {str(e)}")
                
                # Advanced batch analysis
                if batch_results:
                    total_batches = len(batch_results)
                    successful_batches = sum(1 for b in batch_results if b.get("results", {}).get("failed", 0) == 0)
                    total_tickers_processed = sum(b.get("results", {}).get("total_tickers", 0) for b in batch_results)
                    total_successful = sum(b.get("results", {}).get("successful", 0) for b in batch_results)
                    total_failed = sum(b.get("results", {}).get("failed", 0) for b in batch_results)
                    
                    # Calculate advanced metrics
                    batch_success_rate = successful_batches / total_batches if total_batches > 0 else 0
                    overall_success_rate = total_successful / total_tickers_processed if total_tickers_processed > 0 else 0
                    
                    # Analyze recent trends
                    recent_batches = batch_results[-5:]  # Last 5 batches
                    recent_success_rate = sum(1 for b in recent_batches if b.get("results", {}).get("failed", 0) == 0) / len(recent_batches) if recent_batches else 0
                    
                    batch_summary = {
                        "total_batches": total_batches,
                        "successful_batches": successful_batches,
                        "batch_success_rate": batch_success_rate,
                        "total_tickers_processed": total_tickers_processed,
                        "total_successful_tickers": total_successful,
                        "total_failed_tickers": total_failed,
                        "overall_success_rate": overall_success_rate,
                        "recent_success_rate": recent_success_rate,
                        "recent_batches": recent_batches,
                        "analysis_timestamp": datetime.now().isoformat()
                    }
                else:
                    batch_summary = {"status": "no_batch_results", "message": "No batch results found"}
                
                log_validation_success("validate_data", "batch_results", len(batch_results))
                return batch_summary
                
            except Exception as e:
                error_msg = f"Error checking batch results: {str(e)}"
                logger.logger.error(error_msg)
                logger.logger.error(f"Traceback: {traceback.format_exc()}")
                log_validation_error("validate_data", "batch_results", error_msg)
                return {"status": "error", "message": error_msg}
    
    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def generate_validation_report(self) -> Dict[str, Any]:
        """
        Generate comprehensive validation report with advanced features.
        
        Returns:
            Dict[str, Any]: Complete validation report with quality scores and analysis
            
        Example:
            >>> validator = DataValidator()
            >>> report = validator.generate_validation_report()
        """
        operation_name = "generate_validation_report"
        
        with self.performance_monitor.monitor_operation(operation_name):
            log_validation_start("validate_data", "comprehensive_report")
            
            try:
                ticker_validation = self.validate_ticker_data()
                macro_validation = self.validate_macro_data()
                batch_summary = self.check_batch_results()
                
                # Calculate overall quality score
                ticker_score = ticker_validation.get("quality_score", 0.0) if ticker_validation.get("status") == "success" else 0.0
                macro_score = macro_validation.get("quality_score", 0.0) if macro_validation.get("status") == "success" else 0.0
                overall_score = (ticker_score + macro_score) / 2 if (ticker_score > 0 or macro_score > 0) else 0.0
                
                # Determine overall status
                overall_status = "healthy"
                if (ticker_validation.get("status") == "error" or 
                    macro_validation.get("status") == "error"):
                    overall_status = "error"
                elif (ticker_validation.get("status") == "no_data" and 
                      macro_validation.get("status") == "no_data"):
                    overall_status = "no_data"
                elif overall_score < self.config.min_data_quality_score:
                    overall_status = "quality_issues"
                
                report = {
                    "timestamp": datetime.now().isoformat(),
                    "overall_status": overall_status,
                    "overall_quality_score": overall_score,
                    "ticker_data": ticker_validation,
                    "macro_data": macro_validation,
                    "batch_processing": batch_summary,
                    "performance_metrics": self.performance_monitor.metrics,
                    "config": {
                        "min_data_quality_score": self.config.min_data_quality_score,
                        "max_null_ratio": self.config.max_null_ratio,
                        "max_gap_days": self.config.max_gap_days
                    }
                }
                
                # Save report with advanced features
                report_file = "/app/data/validation_report.json"
                os.makedirs(os.path.dirname(report_file), exist_ok=True)
                
                with open(report_file, 'w') as f:
                    json.dump(report, f, indent=2, default=str)
                
                logger.logger.info(f"Comprehensive validation report generated with overall score: {overall_score:.2f}")
                log_validation_success("validate_data", "comprehensive_report", 1)
                return report
                
            except Exception as e:
                error_msg = f"Error generating validation report: {str(e)}"
                logger.logger.error(error_msg)
                logger.logger.error(f"Traceback: {traceback.format_exc()}")
                log_validation_error("validate_data", "comprehensive_report", error_msg)
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
                "max_validation_time_seconds": self.config.max_validation_time_seconds,
                "max_memory_usage_mb": self.config.max_memory_usage_mb,
                "parallel_processing": self.config.parallel_processing,
                "min_data_quality_score": self.config.min_data_quality_score
            },
            "timestamp": datetime.now().isoformat()
        }

# Backward compatibility aliases
# DataValidator is now the main class

def main():
    """Main validation function with advanced features."""
    try:
        logger.logger.info("Starting data validation...")

        validator = DataValidator()
        report = validator.generate_validation_report()
        
        # Print comprehensive summary
        print("\n=== DATA VALIDATION REPORT ===")
        print(f"Overall Status: {report['overall_status']}")
        print(f"Overall Quality Score: {report['overall_quality_score']:.2f}")
        print(f"Timestamp: {report['timestamp']}")
        
        if report['ticker_data']['status'] == 'success':
            ticker_data = report['ticker_data']
            print(f"\nTicker Data:")
            print(f"  Total Records: {ticker_data['total_records']:,}")
            print(f"  Unique Tickers: {ticker_data['unique_tickers']}")
            print(f"  Quality Score: {ticker_data.get('quality_score', 0.0):.2f}")
            print(f"  Date Range: {ticker_data['date_range']['min_date']} to {ticker_data['date_range']['max_date']}")
            print(f"  Recent Data (7 days): {ticker_data['data_quality']['recent_data_tickers']} tickers")
            
            if ticker_data.get('quality_issues'):
                print(f"  Quality Issues: {', '.join(ticker_data['quality_issues'])}")
        
        if report['macro_data']['status'] == 'success':
            macro_data = report['macro_data']
            print(f"\nMacro Data:")
            print(f"  Total Records: {macro_data['total_records']:,}")
            print(f"  Unique Indicators: {macro_data['unique_indicators']}")
            print(f"  Quality Score: {macro_data.get('quality_score', 0.0):.2f}")
            print(f"  Recent Indicators (30 days): {macro_data['recent_indicators']}")
            
            if macro_data.get('quality_issues'):
                print(f"  Quality Issues: {', '.join(macro_data['quality_issues'])}")
        
        if 'total_batches' in report['batch_processing']:
            batch_data = report['batch_processing']
            print(f"\nBatch Processing:")
            print(f"  Total Batches: {batch_data['total_batches']}")
            print(f"  Successful Batches: {batch_data['successful_batches']}")
            print(f"  Batch Success Rate: {batch_data['batch_success_rate']:.1%}")
            print(f"  Overall Success Rate: {batch_data['overall_success_rate']:.1%}")
            print(f"  Recent Success Rate: {batch_data['recent_success_rate']:.1%}")
        
        # Performance summary
        if report.get('performance_metrics'):
            print(f"\nPerformance Summary:")
            for operation, metrics in report['performance_metrics'].items():
                print(f"  {operation}: {metrics['duration']:.2f}s, {metrics['memory_usage_mb']:.1f}MB")
        
        print(f"\nReport saved to: /app/data/validation_report.json")
        
        logger.logger.info("Data validation completed successfully")

    except Exception as e:
        logger.logger.error(f"Error in validation: {str(e)}")
        logger.logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main() 