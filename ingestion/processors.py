"""
Data Processing Components for Ingestion Module
===============================================

This module contains refactored processing components extracted from the
monolithic ingest_data.py file for better maintainability and testability.
"""

import time
import pandas as pd
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass

from ..core.base import BaseProcessor, BaseDataValidator, ValidationResult, log_operation_metrics
from ..core.config import ProcessingConfig, QualityConfig
from ..core.logging import get_ingestion_logger
from .fetch_data import DataIngestion


@dataclass
class IngestionResult:
    """Result of data ingestion operation."""
    
    success: bool
    ticker: str
    record_count: int = 0
    quality_score: float = 0.0
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class TickerDataValidator(BaseDataValidator):
    """Validator for ticker data quality."""
    
    def __init__(self, config: QualityConfig):
        super().__init__(config)
        self.config = config
    
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        """Validate ticker data quality."""
        issues = []
        warnings = []
        quality_score = 1.0
        
        if df.empty:
            return ValidationResult(
                is_valid=False,
                quality_score=0.0,
                issues=["DataFrame is empty"]
            )
        
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
        
        # Check OHLC relationships with tolerance
        if all(col in df.columns for col in ['high', 'low', 'open', 'close']):
            try:
                # Convert to numeric with error handling
                for col in ['high', 'low', 'open', 'close']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Add tolerance for floating-point precision (0.1%)
                tolerance = 0.001
                
                invalid_relationships = (
                    (df['high'] < (df['low'] * (1 - tolerance))) |
                    (df['high'] < (df['open'] * (1 - tolerance))) |
                    (df['high'] < (df['close'] * (1 - tolerance))) |
                    (df['low'] > (df['open'] * (1 + tolerance))) |
                    (df['low'] > (df['close'] * (1 + tolerance)))
                )
                
                invalid_count = invalid_relationships.sum()
                total_count = len(df)
                
                if invalid_count > 0:
                    invalid_ratio = invalid_count / total_count
                    if invalid_ratio > 0.05:  # More than 5% invalid
                        issues.append(f"Critical OHLC validation failure: {invalid_count}/{total_count} records ({invalid_ratio:.2%}) have invalid relationships")
                        quality_score -= min(0.5, invalid_ratio * 3)
                    else:
                        warnings.append(f"Minor OHLC inconsistencies: {invalid_count}/{total_count} records ({invalid_ratio:.2%})")
                        quality_score -= min(0.1, invalid_ratio)
                        
            except Exception as e:
                warnings.append(f"OHLC validation failed due to data type issues: {e}")
        
        # Check null ratio
        null_check = self._check_null_ratio(df, self.config.max_null_ratio)
        if not null_check.is_valid:
            issues.extend(null_check.issues)
            quality_score = min(quality_score, null_check.quality_score)
        
        quality_score = max(0.0, quality_score)
        is_valid = quality_score >= self.config.min_quality_score
        
        return ValidationResult(
            is_valid=is_valid,
            quality_score=quality_score,
            issues=issues,
            warnings=warnings
        )


class TickerProcessor(BaseProcessor):
    """Processor for individual ticker data."""
    
    def __init__(self, config: ProcessingConfig, quality_config: QualityConfig):
        self.processing_config = config
        self.quality_config = quality_config
        super().__init__(config)
        self.logger = get_ingestion_logger("ticker_processor")
        self.gcs_bucket = "candlethrob-wandata"  # TODO: Move to config
    
    def _create_validator(self) -> TickerDataValidator:
        """Create validator instance."""
        return TickerDataValidator(self.quality_config)
    
    def _create_rate_limiter(self):
        """Create rate limiter instance."""
        from ..core.base import BaseRateLimiter
        return BaseRateLimiter(calls_per_minute=5)  # TODO: Move to config
    
    @log_operation_metrics
    def process(self, ticker: str, start_date: str = "2000-01-01", end_date: Optional[str] = None) -> IngestionResult:
        """Process a single ticker."""
        self.logger.log_start("process_ticker", ticker)
        
        try:
            # Rate limiting
            self.rate_limiter.wait_if_needed()
            
            # Set end date if not provided
            if end_date is None:
                from datetime import datetime
                end_date = datetime.now().strftime("%Y-%m-%d")
            
            # Skip if no new data to fetch
            if start_date >= end_date:
                self.logger.logger.info(f"No new data to fetch for {ticker}")
                return IngestionResult(
                    success=True,
                    ticker=ticker,
                    record_count=0,
                    quality_score=1.0
                )
            
            # Fetch data
            data_ingestion = DataIngestion(start_date=start_date, end_date=end_date)
            ticker_df = data_ingestion.ingest_ticker_data(ticker)
            
            if ticker_df is None or ticker_df.empty:
                self.logger.log_error("process_ticker", ticker, "No data fetched")
                return IngestionResult(
                    success=False,
                    ticker=ticker,
                    error_message="No data fetched"
                )
            
            # Validate data quality
            validation_result = self.validator.validate(ticker_df)
            
            if not validation_result.is_valid:
                error_msg = f"Quality validation failed: {validation_result.issues}"
                self.logger.log_error("process_ticker", ticker, error_msg)
                return IngestionResult(
                    success=False,
                    ticker=ticker,
                    error_message=error_msg,
                    quality_score=validation_result.quality_score
                )
            
            # Save to GCS
            from ..utils.gcs import save_ticker_data
            self.logger.logger.info(f"Saving {len(ticker_df)} records for {ticker} to GCS (quality score: {validation_result.quality_score:.2f})")
            success = save_ticker_data(ticker_df, ticker, self.gcs_bucket)
            
            if success:
                self.logger.log_success("process_ticker", ticker, len(ticker_df))
                return IngestionResult(
                    success=True,
                    ticker=ticker,
                    record_count=len(ticker_df),
                    quality_score=validation_result.quality_score
                )
            else:
                error_msg = f"Failed to save ticker data for {ticker} to GCS"
                self.logger.log_error("process_ticker", ticker, error_msg)
                return IngestionResult(
                    success=False,
                    ticker=ticker,
                    error_message=error_msg
                )
                
        except Exception as e:
            error_msg = f"Error processing ticker {ticker}: {str(e)}"
            self.logger.log_error("process_ticker", ticker, error_msg)
            return IngestionResult(
                success=False,
                ticker=ticker,
                error_message=error_msg
            )


class BatchProcessor(BaseProcessor):
    """Processor for batches of tickers."""
    
    def __init__(self, config: ProcessingConfig, quality_config: QualityConfig):
        self.processing_config = config
        self.quality_config = quality_config
        super().__init__(config)
        self.logger = get_ingestion_logger("batch_processor")
        self.ticker_processor = TickerProcessor(config, quality_config)
    
    def _create_validator(self):
        """Create validator instance."""
        return TickerDataValidator(self.quality_config)
    
    def _create_rate_limiter(self):
        """Create rate limiter instance."""
        from ..core.base import BaseRateLimiter
        return BaseRateLimiter(calls_per_minute=5)
    
    @log_operation_metrics
    def process(self, tickers: List[str]) -> Dict[str, Any]:
        """Process a batch of tickers."""
        results = {
            "total_tickers": len(tickers),
            "successful": 0,
            "failed": 0,
            "failed_tickers": [],
            "results": [],
            "start_time": time.time(),
            "end_time": None,
        }
        
        self.logger.logger.info(f"Starting batch processing for {len(tickers)} tickers")
        
        if self.processing_config.parallel_processing and len(tickers) > 1:
            results.update(self._process_parallel(tickers))
        else:
            results.update(self._process_sequential(tickers))
        
        results["end_time"] = time.time()
        results["duration_seconds"] = results["end_time"] - results["start_time"]
        
        self.logger.logger.info(f"Batch processing completed: {results['successful']} successful, {results['failed']} failed")
        return results
    
    def _process_sequential(self, tickers: List[str]) -> Dict[str, Any]:
        """Process tickers sequentially."""
        results = {"successful": 0, "failed": 0, "failed_tickers": [], "results": []}
        
        for i, ticker in enumerate(tickers):
            self.logger.log_batch_progress("batch_process", i+1, len(tickers), ticker)
            
            result = self.ticker_processor.process(ticker)
            results["results"].append(result)
            
            if result.success:
                results["successful"] += 1
            else:
                results["failed"] += 1
                results["failed_tickers"].append(ticker)
        
        return results
    
    def _process_parallel(self, tickers: List[str]) -> Dict[str, Any]:
        """Process tickers in parallel."""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        results = {"successful": 0, "failed": 0, "failed_tickers": [], "results": []}
        
        with ThreadPoolExecutor(max_workers=self.processing_config.max_workers) as executor:
            future_to_ticker = {
                executor.submit(self.ticker_processor.process, ticker): ticker
                for ticker in tickers
            }
            
            for i, future in enumerate(as_completed(future_to_ticker)):
                ticker = future_to_ticker[future]
                self.logger.log_batch_progress("batch_process", i+1, len(tickers), ticker)
                
                try:
                    result = future.result()
                    results["results"].append(result)
                    
                    if result.success:
                        results["successful"] += 1
                    else:
                        results["failed"] += 1
                        results["failed_tickers"].append(ticker)
                        
                except Exception as e:
                    self.logger.logger.error(f"Exception occurred while processing ticker {ticker}: {str(e)}")
                    results["failed"] += 1
                    results["failed_tickers"].append(ticker)
                    results["results"].append(IngestionResult(
                        success=False,
                        ticker=ticker,
                        error_message=str(e)
                    ))
        
        return results