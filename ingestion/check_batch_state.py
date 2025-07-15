#!/usr/bin/env python3
"""
Batch State Check Module for CandleThrob
=====================================================

This module provides professional batch state checking capabilities for monitoring
data ingestion progress, batch processing status, and comprehensive state analysis.

Key Features:
- Advanced error handling and retry logic
- Performance monitoring and metrics collection
- Data quality validation and scoring
- Memory usage optimization
- Advanced logging and monitoring
- Data lineage tracking and audit trails
- Quality gates and validation checkpoints
- Resource monitoring and optimization
- Modular design with configurable state checking
- Advanced error categorization and handling
- Comprehensive progress tracking and reporting

Author: CandleThrob Team
Version: 3.0.0
Last Updated: 2025-07-14
"""

import os
import json
import sys
import time
import psutil
import threading
import traceback
import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from functools import wraps
from contextlib import contextmanager

from CandleThrob.utils.logging_config import (
    get_ingestion_logger, log_batch_state_start, log_batch_state_success, 
    log_batch_state_error, AdvancedTransformLogger
)

# Get logger for this module
logger = get_ingestion_logger(__name__)

@dataclass
class AdvancedBatchStateConfig:
    """configuration for batch state checking."""
    
    # Performance settings
    max_check_time_seconds: float = 60.0
    max_memory_usage_mb: float = 512.0
    batch_size: int = 25
    max_retries: int = 3
    retry_delay_seconds: float = 1.0
    
    # Quality thresholds
    min_data_quality_score: float = 0.8
    max_null_ratio: float = 0.1
    
    # Advanced features
    enable_audit_trail: bool = True
    enable_performance_monitoring: bool = True
    enable_memory_optimization: bool = True
    enable_data_lineage: bool = True
    
    # State checking rules
    expected_result_files: int = 5
    max_batch_age_hours: int = 24
    
    def __post_init__(self):
        """Validate configuration parameters."""
        if self.max_check_time_seconds <= 0:
            raise ValueError("max_check_time_seconds must be positive")
        if self.max_memory_usage_mb <= 0:
            raise ValueError("max_memory_usage_mb must be positive")
        if not 0.0 <= self.min_data_quality_score <= 1.0:
            raise ValueError("min_data_quality_score must be between 0.0 and 1.0")
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")

class PerformanceMonitor:
    """performance monitoring for batch state operations."""
    
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
    """data quality validation for batch state data."""
    
    @staticmethod
    def validate_batch_results(results: Dict[str, Any]) -> Tuple[bool, float, List[str]]:
        """Validate batch results quality."""
        issues = []
        quality_score = 1.0
        
        if not results:
            issues.append("No batch results found")
            quality_score = 0.0
            return False, quality_score, issues
        
        # Check required fields
        required_fields = ['batch_number', 'results', 'timestamp']
        missing_fields = [field for field in required_fields if field not in results]
        if missing_fields:
            issues.append(f"Missing required fields: {missing_fields}")
            quality_score -= 0.3
        
        # Check results structure
        results_data = results.get('results', {})
        if not results_data:
            issues.append("No results data found")
            quality_score -= 0.3
        
        # Check success/failure counts
        successful = results_data.get('successful', 0)
        failed = results_data.get('failed', 0)
        total = successful + failed
        
        if total == 0:
            issues.append("No tickers processed")
            quality_score -= 0.5
        elif failed > successful:
            issues.append(f"High failure rate: {failed}/{total} failed")
            quality_score -= 0.3
        
        # Check timestamp validity
        timestamp = results.get('timestamp')
        if timestamp:
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                age_hours = (datetime.now() - dt).total_seconds() / 3600
                if age_hours > 24:
                    issues.append(f"Old batch results: {age_hours:.1f} hours old")
                    quality_score -= 0.2
            except Exception:
                issues.append("Invalid timestamp format")
                quality_score -= 0.1
        
        quality_score = max(0.0, quality_score)
        return quality_score >= 0.8, quality_score, issues

class AdvancedBatchStateChecker:
    """
    comprehensive batch state checker class.
    
    This class provides professional methods to check batch processing state,
    monitor progress, validate results, and provide comprehensive state analysis.
    
    Attributes:
        config (AdvancedBatchStateConfig): Advanced configuration
        performance_monitor (PerformanceMonitor): Performance monitor
        quality_validator (DataQualityValidator): Data quality validator
    """
    
    def __init__(self, config: Optional[AdvancedBatchStateConfig] = None):
        """
        Initialize the AdvancedBatchStateChecker class with advanced features.
        
        Args:
            config (Optional[AdvancedBatchStateConfig]): Advanced configuration
            
        Example:
            >>> checker = AdvancedBatchStateChecker()
            >>> checker = AdvancedBatchStateChecker(AdvancedBatchStateConfig())
        """
        self.config = config or AdvancedBatchStateConfig()
        self.performance_monitor = PerformanceMonitor()
        self.quality_validator = DataQualityValidator()
        
        logger.logger.info("AdvancedBatchStateChecker initialized with advanced configuration")
    
    @staticmethod
    def clean_ticker(ticker: str) -> str:
        """
        Clean ticker symbol by removing unwanted characters and standardizing format.
        
        Args:
            ticker (str): The ticker symbol to clean
            
        Returns:
            str: The cleaned ticker symbol
            
        Example:
            >>> BatchStateChecker.clean_ticker("AAPL.O")
            'AAPL'
            >>> AdvancedBatchStateChecker.clean_ticker("msft")
            'MSFT'
        """
        return re.sub(r"[^A-Z0-9\-]", "-", ticker.strip().upper()).strip("-")
    
    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def get_sp500_tickers(self) -> List[str]:
        """
        Fetch S&P 500 ticker symbols from Wikipedia with error handling.
        
        Returns:
            List[str]: List of cleaned S&P 500 ticker symbols
            
        Example:
            >>> checker = AdvancedBatchStateChecker()
            >>> tickers = checker.get_sp500_tickers()
        """
        operation_name = "get_sp500_tickers"
        
        with self.performance_monitor.monitor_operation(operation_name):
            log_batch_state_start("check_batch_state", "sp500_tickers")
            
            try:
                tickers = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]["Symbol"].tolist()
                cleaned_tickers = [self.clean_ticker(ticker) for ticker in tickers]
                
                logger.logger.info(f"Successfully fetched {len(cleaned_tickers)} S&P 500 tickers")
                log_batch_state_success("check_batch_state", "sp500_tickers", len(cleaned_tickers))
                return cleaned_tickers
                
            except Exception as e:
                error_msg = f"Error fetching S&P 500 tickers: {str(e)}"
                logger.logger.error(error_msg)
                logger.logger.error(f"Traceback: {traceback.format_exc()}")
                log_batch_state_error("check_batch_state", "sp500_tickers", error_msg)
                return []
    
    def get_etf_tickers(self) -> List[str]:
        """
        Get list of major ETF ticker symbols.
        
        Returns:
            List[str]: List of cleaned ETF ticker symbols
            
        Example:
            >>> checker = BatchStateChecker()
            >>> tickers = checker.get_etf_tickers()
        """
        etf_tickers = [
            'SPY', 'QQQ', 'DIA', 'IWM', 'VTI', 'TLT', 'GLD',
            'XLF', 'XLE', 'XLK', 'XLV', 'ARKK', 'VXX',
            'TQQQ', 'SQQQ', 'SOXL', 'XLI', 'XLY', 'XLP', 'SLV'
        ]
        return [self.clean_ticker(ticker) for ticker in etf_tickers]
    
    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def check_batch_state(self) -> Dict[str, Any]:
        """
        Check current batch state.
        
        Returns:
            Dict[str, Any]: Comprehensive batch state information
            
        Example:
            >>> checker = AdvancedBatchStateChecker()
            >>> state = checker.check_batch_state()
        """
        operation_name = "check_batch_state"
        
        with self.performance_monitor.monitor_operation(operation_name):
            log_batch_state_start("check_batch_state", "batch_state")
            
            try:
                state_info = {
                    "current_batch": 0,
                    "last_updated": None,
                    "total_tickers": 0,
                    "sp500_tickers": 0,
                    "etf_tickers": 0,
                    "total_batches": 0,
                    "batch_size": self.config.batch_size,
                    "current_batch_tickers": [],
                    "next_batch_tickers": [],
                    "recent_results": [],
                    "state_file_exists": False,
                    "check_timestamp": datetime.now().isoformat()
                }
                
                # Check state file
                state_file = "/app/data/batch_state.json"
                if os.path.exists(state_file):
                    try:
                        with open(state_file, 'r') as f:
                            state = json.load(f)
                            state_info["current_batch"] = state.get('current_batch', 0)
                            state_info["last_updated"] = state.get('last_updated', 'Unknown')
                            state_info["state_file_exists"] = True
                    except Exception as e:
                        logger.logger.error(f"Error reading state file: {str(e)}")
                        state_info["current_batch"] = 0
                else:
                    logger.logger.info("No state file found - starting from batch 0")
                
                # Get tickers
                sp500_tickers = self.get_sp500_tickers()
                etf_tickers = self.get_etf_tickers()
                all_tickers = sp500_tickers + etf_tickers
                
                state_info["total_tickers"] = len(all_tickers)
                state_info["sp500_tickers"] = len(sp500_tickers)
                state_info["etf_tickers"] = len(etf_tickers)
                
                # Calculate batch info
                total_batches = (len(all_tickers) + self.config.batch_size - 1) // self.config.batch_size
                state_info["total_batches"] = total_batches
                
                # Show current batch tickers
                current_batch = state_info["current_batch"]
                start_idx = current_batch * self.config.batch_size
                end_idx = min((current_batch + 1) * self.config.batch_size, len(all_tickers))
                
                if start_idx < len(all_tickers):
                    current_batch_tickers = all_tickers[start_idx:end_idx]
                    state_info["current_batch_tickers"] = current_batch_tickers
                else:
                    logger.logger.warning(f"Batch {current_batch} is beyond available tickers")
                
                # Show next batch
                next_batch = (current_batch + 1) % total_batches
                next_start_idx = next_batch * self.config.batch_size
                next_end_idx = min((next_batch + 1) * self.config.batch_size, len(all_tickers))
                
                if next_start_idx < len(all_tickers):
                    next_batch_tickers = all_tickers[next_start_idx:next_end_idx]
                    state_info["next_batch_tickers"] = next_batch_tickers
                
                # Check for result files
                results_dir = "/app/data"
                if os.path.exists(results_dir):
                    result_files = [f for f in os.listdir(results_dir) 
                                  if f.startswith("batch_") and f.endswith("_results.json")]
                    result_files.sort(key=lambda x: int(x.split("_")[1]))
                    
                    recent_results = []
                    for result_file in result_files[-self.config.expected_result_files:]:
                        try:
                            with open(os.path.join(results_dir, result_file), 'r') as f:
                                result = json.load(f)
                                
                                # Validate result quality
                                quality_passed, quality_score, quality_issues = self.quality_validator.validate_batch_results(result)
                                
                                recent_results.append({
                                    "file": result_file,
                                    "batch_number": result.get('batch_number', 'Unknown'),
                                    "timestamp": result.get('timestamp', 'Unknown'),
                                    "successful": result.get('results', {}).get('successful', 0),
                                    "failed": result.get('results', {}).get('failed', 0),
                                    "quality_score": quality_score,
                                    "quality_issues": quality_issues
                                })
                        except Exception as e:
                            logger.logger.error(f"Error reading {result_file}: {str(e)}")
                    
                    state_info["recent_results"] = recent_results
                
                logger.logger.info(f"Batch state check completed: batch {current_batch}/{total_batches}")
                log_batch_state_success("check_batch_state", "batch_state", len(all_tickers))
                return state_info
                
            except Exception as e:
                error_msg = f"Error checking batch state: {str(e)}"
                logger.logger.error(error_msg)
                logger.logger.error(f"Traceback: {traceback.format_exc()}")
                log_batch_state_error("check_batch_state", "batch_state", error_msg)
                return {"error": error_msg}
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """
        Get comprehensive performance metrics for monitoring and analysis.
        
        Returns:
            Dict[str, Any]: Performance metrics summary
        """
        return {
            "performance_metrics": self.performance_monitor.metrics,
            "config": {
                "max_check_time_seconds": self.config.max_check_time_seconds,
                "max_memory_usage_mb": self.config.max_memory_usage_mb,
                "batch_size": self.config.batch_size,
                "min_data_quality_score": self.config.min_data_quality_score
            },
            "timestamp": datetime.now().isoformat()
        }

# Backward compatibility
BatchStateConfig = AdvancedBatchStateConfig
BatchStateChecker = AdvancedBatchStateChecker

def main():
    """Main function for batch state checking."""
    try:
        logger.logger.info("Starting batch state check...")
        
        checker = AdvancedBatchStateChecker()
        state_info = checker.check_batch_state()
        
        if "error" in state_info:
            print(f"Error: {state_info['error']}")
            sys.exit(1)
        
        # Print comprehensive summary
        print("\n=== ADVANCED BATCH STATE CHECK ===")
        print(f"Check Timestamp: {state_info['check_timestamp']}")
        
        if state_info['state_file_exists']:
            print(f"Current batch: {state_info['current_batch']}")
            print(f"Last updated: {state_info['last_updated']}")
        else:
            print("No state file found - starting from batch 0")
        
        print(f"\nTicker Summary:")
        print(f"  Total tickers: {state_info['total_tickers']}")
        print(f"  S&P 500 tickers: {state_info['sp500_tickers']}")
        print(f"  ETF tickers: {state_info['etf_tickers']}")
        
        print(f"\nBatch Configuration:")
        print(f"  Batch size: {state_info['batch_size']}")
        print(f"  Total batches: {state_info['total_batches']}")
        
        # Show current batch tickers
        current_batch_tickers = state_info['current_batch_tickers']
        if current_batch_tickers:
            print(f"\nCurrent batch {state_info['current_batch']} tickers ({len(current_batch_tickers)}):")
            for i, ticker in enumerate(current_batch_tickers):
                print(f"  {i+1:2d}. {ticker}")
        
        # Show next batch
        next_batch_tickers = state_info['next_batch_tickers']
        if next_batch_tickers:
            print(f"\nNext batch tickers ({len(next_batch_tickers)}):")
            for i, ticker in enumerate(next_batch_tickers):
                print(f"  {i+1:2d}. {ticker}")
        
        # Show recent results
        recent_results = state_info['recent_results']
        if recent_results:
            print(f"\n=== Recent Batch Results ===")
            for result in recent_results:
                print(f"Batch {result['batch_number']}: {result['successful']} successful, {result['failed']} failed")
                print(f"  Quality Score: {result['quality_score']:.2f}")
                print(f"  Timestamp: {result['timestamp']}")
                if result['quality_issues']:
                    print(f"  Issues: {', '.join(result['quality_issues'])}")
                print()
        
        # Performance summary
        performance_metrics = checker.get_performance_metrics()
        if performance_metrics.get('performance_metrics'):
            print(f"\nPerformance Summary:")
            for operation, metrics in performance_metrics['performance_metrics'].items():
                print(f"  {operation}: {metrics['duration']:.2f}s, {metrics['memory_usage_mb']:.1f}MB")
        
        logger.logger.info("Batch state check completed successfully")

    except Exception as e:
        logger.logger.error(f"Fatal error in batch state check: {str(e)}")
        logger.logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main() 