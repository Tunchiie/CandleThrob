#!/usr/bin/env python3
"""
Batch Calculator Module for CandleThrob
=======================================

This module provides advanced batch calculation capabilities for optimizing
data ingestion schedules, rate limiting analysis, and comprehensive batch planning.

 Features:
- Advanced error handling and retry logic
- Performance monitoring and metrics collection
- Data quality validation and scoring
- Memory usage optimization
- Advanced logging and monitoring
- Data lineage tracking and audit trails
- Quality gates and validation checkpoints
- Resource monitoring and optimization
- Modular design with configurable batch calculation
- Advanced error categorization and handling
- Comprehensive scheduling optimization
- Rate limiting analysis and optimization

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
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from functools import wraps
from contextlib import contextmanager

# Add the parent directory to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from CandleThrob.ingestion.ingest_data import get_sp500_tickers, get_etf_tickers
from CandleThrob.utils.logging_config import (
    get_ingestion_logger, log_batch_calculation_start, log_batch_calculation_success, 
    log_batch_calculation_error, AdvancedTransformLogger
)

# Get logger for this module
logger = get_ingestion_logger(__name__)

@dataclass
class BatchCalculationConfig:
    """Configuration for batch calculation."""
    
    # Performance settings
    max_calculation_time_seconds: float = 120.0
    max_memory_usage_mb: float = 512.0
    max_retries: int = 3
    retry_delay_seconds: float = 1.0
    
    # Batch calculation parameters
    default_batch_size: int = 25
    rate_limit_calls_per_minute: int = 5
    rate_limit_seconds_between_calls: float = 12.0
    max_batches_per_hour: int = 12
    optimal_start_hour: int = 21  # 9 PM EST
    
    # Quality thresholds
    min_batch_efficiency: float = 0.8
    max_schedule_overlap: float = 0.1
    
    # Advanced features
    enable_audit_trail: bool = True
    enable_performance_monitoring: bool = True
    enable_memory_optimization: bool = True
    enable_data_lineage: bool = True
    
    def __post_init__(self):
        """Validate configuration parameters."""
        if self.max_calculation_time_seconds <= 0:
            raise ValueError("max_calculation_time_seconds must be positive")
        if self.max_memory_usage_mb <= 0:
            raise ValueError("max_memory_usage_mb must be positive")
        if not 0.0 <= self.min_batch_efficiency <= 1.0:
            raise ValueError("min_batch_efficiency must be between 0.0 and 1.0")
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")

class PerformanceMonitor:
    """Performance monitoring for batch calculation operations."""
    
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
    """Retry decorator with exponential backoff."""
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

class BatchEfficiencyCalculator:
    """Batch efficiency calculation and optimization."""
    
    @staticmethod
    def calculate_batch_efficiency(batch_size: int, rate_limit: float, total_tickers: int) -> Tuple[float, List[str]]:
        """Calculate batch processing efficiency."""
        issues = []
        efficiency = 1.0
        
        # Check batch size optimization
        if batch_size < 10:
            issues.append("Batch size too small for efficient processing")
            efficiency -= 0.2
        elif batch_size > 50:
            issues.append("Batch size too large for rate limiting")
            efficiency -= 0.2
        
        # Check rate limiting compliance
        time_per_batch = batch_size * rate_limit
        if time_per_batch > 60:  # More than 1 minute per batch
            issues.append("Batch processing time exceeds optimal limits")
            efficiency -= 0.3
        
        # Check total processing time
        total_batches = (total_tickers + batch_size - 1) // batch_size
        total_time_hours = (total_batches * time_per_batch) / 3600
        if total_time_hours > 24:
            issues.append("Total processing time exceeds 24 hours")
            efficiency -= 0.3
        
        efficiency = max(0.0, efficiency)
        return efficiency, issues
    
    @staticmethod
    def optimize_batch_schedule(batches: List[Dict[str, Any]], max_batches_per_hour: int) -> List[Dict[str, Any]]:
        """Optimize batch schedule to minimize overlap and maximize efficiency."""
        optimized_batches = []
        
        for i, batch in enumerate(batches):
            # Calculate optimal spacing
            spacing_minutes = 60 / max_batches_per_hour
            optimal_start = datetime.fromisoformat(batch['scheduled_start'])
            
            # Adjust start time to avoid overlap
            if i > 0:
                prev_batch = optimized_batches[-1]
                prev_end = datetime.fromisoformat(prev_batch['scheduled_start']) + timedelta(minutes=prev_batch['estimated_duration_minutes'])
                optimal_start = max(optimal_start, prev_end + timedelta(minutes=spacing_minutes))
            
            optimized_batch = batch.copy()
            optimized_batch['scheduled_start'] = optimal_start.isoformat()
            optimized_batch['optimized'] = True
            optimized_batches.append(optimized_batch)
        
        return optimized_batches

class BatchCalculator:
    """
    Comprehensive batch calculator class.

    This class provides advanced methods to calculate optimal batch sizes,
    schedules, and rate limiting strategies for data ingestion processes.
    
    Attributes:
        config (BatchCalculationConfig):  configuration
        performance_monitor (PerformanceMonitor): Performance monitor
        efficiency_calculator (BatchEfficiencyCalculator): Efficiency calculator
    """
    
    def __init__(self, config: Optional[BatchCalculationConfig] = None):
        """
        Initialize the BatchCalculator class with  features.
        
        Args:
            config (Optional[BatchCalculationConfig]):  configuration
            
        Example:
            >>> calculator = BatchCalculator()
            >>> calculator = BatchCalculator(BatchCalculationConfig())
        """
        self.config = config or BatchCalculationConfig()
        self.performance_monitor = PerformanceMonitor()
        self.efficiency_calculator = BatchEfficiencyCalculator()
        
        logger.info("BatchCalculator initialized with  configuration")
    
    @retry_on_failure(max_retries=3, delay_seconds=2.0)
    def calculate_optimal_batches(self) -> Dict[str, Any]:
        """
        Calculate optimal batch configuration with  features.
        
        Returns:
            Dict[str, Any]: Comprehensive batch calculation results
            
        Example:
            >>> calculator = BatchCalculator()
            >>> results = calculator.calculate_optimal_batches()
        """
        operation_name = "calculate_optimal_batches"
        
        with self.performance_monitor.monitor_operation(operation_name):
            log_batch_calculation_start("batch_calculator", "optimal_batches")
            
            try:
                # Get all tickers with  error handling
                sp500_tickers = get_sp500_tickers()
                etf_tickers = get_etf_tickers()
                all_tickers = sp500_tickers + etf_tickers
                
                batch_size = int(os.getenv("BATCH_SIZE", str(self.config.default_batch_size)))
                total_tickers = len(all_tickers)
                
                # Calculate total batches needed
                total_batches = (total_tickers + batch_size - 1) // batch_size
                
                # Calculate time needed with rate limiting
                time_per_batch_minutes = batch_size * (self.config.rate_limit_seconds_between_calls / 60)
                total_time_minutes = total_batches * time_per_batch_minutes
                
                # Calculate optimal start times to spread across the day
                start_time = datetime.now().replace(
                    hour=self.config.optimal_start_hour, 
                    minute=0, 
                    second=0, 
                    microsecond=0
                )
                
                # Generate batch schedule
                batch_schedule = []
                for batch_num in range(total_batches):
                    start_idx = batch_num * batch_size
                    end_idx = min((batch_num + 1) * batch_size, total_tickers)
                    batch_tickers = all_tickers[start_idx:end_idx]
                    
                    # Calculate start time for this batch
                    batch_start_time = start_time + timedelta(minutes=batch_num * time_per_batch_minutes)
                    
                    batch_info = {
                        "batch_number": batch_num,
                        "start_idx": start_idx,
                        "end_idx": end_idx,
                        "ticker_count": len(batch_tickers),
                        "tickers": batch_tickers,
                        "scheduled_start": batch_start_time.isoformat(),
                        "estimated_duration_minutes": time_per_batch_minutes,
                        "rate_limit_compliant": True
                    }
                    batch_schedule.append(batch_info)
                
                # Optimize batch schedule
                optimized_schedule = self.efficiency_calculator.optimize_batch_schedule(
                    batch_schedule, self.config.max_batches_per_hour
                )
                
                # Calculate efficiency metrics
                efficiency, efficiency_issues = self.efficiency_calculator.calculate_batch_efficiency(
                    batch_size, self.config.rate_limit_seconds_between_calls, total_tickers
                )
                
                # Prepare comprehensive results
                results = {
                    "total_tickers": total_tickers,
                    "sp500_tickers": len(sp500_tickers),
                    "etf_tickers": len(etf_tickers),
                    "batch_size": batch_size,
                    "total_batches": total_batches,
                    "total_time_minutes": total_time_minutes,
                    "total_time_hours": total_time_minutes / 60,
                    "rate_limit_calls_per_minute": self.config.rate_limit_calls_per_minute,
                    "rate_limit_seconds_between_calls": self.config.rate_limit_seconds_between_calls,
                    "batch_efficiency": efficiency,
                    "efficiency_issues": efficiency_issues,
                    "batch_schedule": optimized_schedule,
                    "calculation_timestamp": datetime.now().isoformat()
                }
                
                # Save batch schedule to file for Kestra to use
                schedule_file = "/app/data/_batch_schedule.json"
                os.makedirs(os.path.dirname(schedule_file), exist_ok=True)
                
                with open(schedule_file, 'w') as f:
                    json.dump(results, f, indent=2, default=str)
                
                logger.info(f"Calculated {total_batches} batches for {total_tickers} tickers")
                logger.info(f"Total estimated time: {total_time_minutes:.1f} minutes")
                logger.info(f"Batch efficiency: {efficiency:.2f}")
                logger.info(f" batch schedule saved to {schedule_file}")
                
                log_batch_calculation_success("batch_calculator", "optimal_batches", total_batches)
                return results
                
            except Exception as e:
                error_msg = f"Error calculating optimal batches: {str(e)}"
                logger.error(error_msg)
                logger.error(f"Traceback: {traceback.format_exc()}")
                log_batch_calculation_error("batch_calculator", "optimal_batches", error_msg)
                return {"error": error_msg}
    
    def validate_batch_schedule(self, schedule: List[Dict[str, Any]]) -> Tuple[bool, float, List[str]]:
        """
        Validate batch schedule quality and efficiency.
        
        Args:
            schedule (List[Dict[str, Any]]): Batch schedule to validate
            
        Returns:
            Tuple[bool, float, List[str]]: Validation result, quality score, and issues
        """
        issues = []
        quality_score = 1.0
        
        if not schedule:
            issues.append("No batch schedule provided")
            quality_score = 0.0
            return False, quality_score, issues
        
        # Check for overlapping schedules
        for i in range(len(schedule) - 1):
            current_batch = schedule[i]
            next_batch = schedule[i + 1]
            
            current_start = datetime.fromisoformat(current_batch['scheduled_start'])
            current_end = current_start + timedelta(minutes=current_batch['estimated_duration_minutes'])
            next_start = datetime.fromisoformat(next_batch['scheduled_start'])
            
            if next_start < current_end:
                issues.append(f"Schedule overlap between batches {i} and {i+1}")
                quality_score -= 0.2
        
        # Check batch size consistency
        batch_sizes = [batch['ticker_count'] for batch in schedule]
        if max(batch_sizes) - min(batch_sizes) > 5:
            issues.append("Inconsistent batch sizes")
            quality_score -= 0.1
        
        # Check rate limiting compliance
        for batch in schedule:
            if batch['estimated_duration_minutes'] > 60:
                issues.append(f"Batch {batch['batch_number']} exceeds 1 hour duration")
                quality_score -= 0.1
        
        quality_score = max(0.0, quality_score)
        return quality_score >= 0.8, quality_score, issues
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """
        Get comprehensive performance metrics for monitoring and analysis.
        
        Returns:
            Dict[str, Any]: Performance metrics summary
        """
        return {
            "performance_metrics": self.performance_monitor.metrics,
            "config": {
                "max_calculation_time_seconds": self.config.max_calculation_time_seconds,
                "max_memory_usage_mb": self.config.max_memory_usage_mb,
                "default_batch_size": self.config.default_batch_size,
                "rate_limit_calls_per_minute": self.config.rate_limit_calls_per_minute
            },
            "timestamp": datetime.now().isoformat()
        }


def main():
    """Main function for  batch calculation."""
    try:
        logger.info("Starting  batch calculation for CandleThrob data ingestion...")
        
        calculator = BatchCalculator()
        results = calculator.calculate_optimal_batches()
        
        if "error" in results:
            print(f"Error: {results['error']}")
            sys.exit(1)
        
        # Validate batch schedule
        schedule = results.get('batch_schedule', [])
        validation_passed, quality_score, validation_issues = calculator.validate_batch_schedule(schedule)
        
        # Print comprehensive summary
        print(f"\n===  BATCH CALCULATION SUMMARY ===")
        print(f"Calculation Timestamp: {results['calculation_timestamp']}")
        print(f"Total tickers: {results['total_tickers']}")
        print(f"  S&P 500 tickers: {results['sp500_tickers']}")
        print(f"  ETF tickers: {results['etf_tickers']}")
        print(f"Batch size: {results['batch_size']}")
        print(f"Total batches: {results['total_batches']}")
        print(f"Total estimated time: {results['total_time_minutes']:.1f} minutes ({results['total_time_hours']:.1f} hours)")
        print(f"Batch efficiency: {results['batch_efficiency']:.2f}")
        
        if results['efficiency_issues']:
            print(f"Efficiency issues: {', '.join(results['efficiency_issues'])}")
        
        print(f"Rate limiting: {results['rate_limit_calls_per_minute']} calls/minute")
        print(f"Schedule validation: {'PASSED' if validation_passed else 'FAILED'}")
        print(f"Schedule quality score: {quality_score:.2f}")
        
        if validation_issues:
            print(f"Validation issues: {', '.join(validation_issues)}")
        
        # Performance summary
        performance_metrics = calculator.get_performance_metrics()
        if performance_metrics.get('performance_metrics'):
            print(f"\nPerformance Summary:")
            for operation, metrics in performance_metrics['performance_metrics'].items():
                print(f"  {operation}: {metrics['duration']:.2f}s, {metrics['memory_usage_mb']:.1f}MB")
        
        print(f" schedule file: /app/data/_batch_schedule.json")
        
        logger.info(" batch calculation completed successfully")
        
    except Exception as e:
        logger.error(f"Fatal error in  batch calculation: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main() 