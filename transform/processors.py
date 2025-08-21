"""
Data Processing Components for Transform Module
===============================================

This module contains refactored processing components for data transformation,
using the centralized core components for consistency and maintainability.
"""

import pandas as pd
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass

from ..core.base import BaseProcessor, BaseDataValidator, ValidationResult, log_operation_metrics
from ..core.config import ProcessingConfig, QualityConfig
from ..core.logging import get_transform_logger
from .enrich_data import TechnicalIndicators, EnrichMacros


@dataclass
class TransformResult:
    """Result of data transformation operation."""
    
    success: bool
    ticker: str
    record_count: int = 0
    indicators_added: int = 0
    quality_score: float = 0.0
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class TransformDataValidator(BaseDataValidator):
    """Validator for transformation data quality."""
    
    def __init__(self, config: QualityConfig):
        super().__init__(config)
        self.config = config
    
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        """Validate transformation data quality."""
        issues = []
        warnings = []
        quality_score = 1.0
        
        if df.empty:
            return ValidationResult(
                is_valid=False,
                quality_score=0.0,
                issues=["DataFrame is empty"]
            )
        
        # Check required columns for transformation
        required_cols = ['ticker', 'trade_date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            issues.append(f"Missing required columns: {missing_cols}")
            quality_score -= 0.3
        
        # Check for null values in critical columns
        critical_columns = ['ticker', 'trade_date', 'close_price']
        for col in critical_columns:
            if col in df.columns and df[col].isnull().any():
                issues.append(f"Null values found in critical column: {col}")
                quality_score -= 0.2
        
        # Check data types
        expected_numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
        for col in expected_numeric_cols:
            if col in df.columns:
                try:
                    pd.to_numeric(df[col], errors='raise')
                except (ValueError, TypeError):
                    warnings.append(f"Non-numeric values in {col}")
                    quality_score -= 0.1
        
        # Check for reasonable date range
        if 'trade_date' in df.columns:
            try:
                dates = pd.to_datetime(df['trade_date'])
                min_date = dates.min()
                max_date = dates.max()
                
                # Check if dates are within reasonable range (not too old or future)
                from datetime import datetime, timedelta
                min_reasonable = datetime(1990, 1, 1)
                max_reasonable = datetime.now() + timedelta(days=1)
                
                if min_date < min_reasonable or max_date > max_reasonable:
                    warnings.append(f"Date range outside reasonable bounds: {min_date} to {max_date}")
                    quality_score -= 0.1
                    
            except Exception as e:
                issues.append(f"Invalid date format in trade_date column: {e}")
                quality_score -= 0.2
        
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


class TickerTransformProcessor(BaseProcessor):
    """Processor for individual ticker transformation."""
    
    def __init__(self, config: ProcessingConfig, quality_config: QualityConfig):
        self.processing_config = config
        self.quality_config = quality_config
        super().__init__(config)
        self.logger = get_transform_logger("ticker_transform")
        self.technical_indicators = TechnicalIndicators()
        self.gcs_bucket = "candlethrob-wandata"  # TODO: Move to config
    
    def _create_validator(self) -> TransformDataValidator:
        """Create validator instance."""
        return TransformDataValidator(self.quality_config)
    
    def _create_rate_limiter(self):
        """Create rate limiter instance."""
        from ..core.base import BaseRateLimiter
        return BaseRateLimiter(calls_per_minute=60)  # Transform operations can be faster
    
    @log_operation_metrics
    def process(self, ticker: str) -> TransformResult:
        """Transform a single ticker's data."""
        self.logger.log_transform_start(ticker, ["RSI", "MACD", "Bollinger Bands", "Moving Averages"])
        
        try:
            # Load raw ticker data from GCS
            from ..utils.gcs import load_ticker_data
            ticker_df = load_ticker_data(ticker, self.gcs_bucket)
            
            if ticker_df is None or ticker_df.empty:
                error_msg = f"No raw data found for {ticker}"
                self.logger.log_transform_error(ticker, error_msg)
                return TransformResult(
                    success=False,
                    ticker=ticker,
                    error_message=error_msg
                )
            
            # Validate input data
            validation_result = self.validator.validate(ticker_df)
            if not validation_result.is_valid:
                error_msg = f"Input data validation failed: {validation_result.issues}"
                self.logger.log_transform_error(ticker, error_msg)
                return TransformResult(
                    success=False,
                    ticker=ticker,
                    error_message=error_msg,
                    quality_score=validation_result.quality_score
                )
            
            # Log any warnings
            if validation_result.warnings:
                for warning in validation_result.warnings:
                    self.logger.logger.warning(f"Data validation warning for {ticker}: {warning}")
            
            # Apply technical indicators
            self.logger.logger.info(f"Applying technical indicators to {len(ticker_df)} records for {ticker}")
            
            try:
                transformed_df = self.technical_indicators.add_all_indicators(ticker_df)
            except Exception as e:
                error_msg = f"Failed to apply technical indicators: {str(e)}"
                self.logger.log_transform_error(ticker, error_msg)
                return TransformResult(
                    success=False,
                    ticker=ticker,
                    error_message=error_msg
                )
            
            # Count new indicators added
            original_cols = set(ticker_df.columns)
            new_cols = set(transformed_df.columns) - original_cols
            indicators_added = len(new_cols)
            
            # Save transformed data to GCS
            from ..utils.gcs import save_transformed_ticker_data
            self.logger.logger.info(f"Saving transformed data for {ticker} with {indicators_added} new indicators")
            success = save_transformed_ticker_data(transformed_df, ticker, self.gcs_bucket)
            
            if success:
                self.logger.log_transform_success(ticker, len(transformed_df), indicators_added)
                return TransformResult(
                    success=True,
                    ticker=ticker,
                    record_count=len(transformed_df),
                    indicators_added=indicators_added,
                    quality_score=validation_result.quality_score,
                    metadata={"new_indicators": list(new_cols)}
                )
            else:
                error_msg = f"Failed to save transformed data for {ticker}"
                self.logger.log_transform_error(ticker, error_msg)
                return TransformResult(
                    success=False,
                    ticker=ticker,
                    error_message=error_msg
                )
                
        except Exception as e:
            error_msg = f"Error transforming ticker {ticker}: {str(e)}"
            self.logger.log_transform_error(ticker, error_msg)
            return TransformResult(
                success=False,
                ticker=ticker,
                error_message=error_msg
            )


class MacroTransformProcessor(BaseProcessor):
    """Processor for macro data transformation."""
    
    def __init__(self, config: ProcessingConfig, quality_config: QualityConfig):
        self.processing_config = config
        self.quality_config = quality_config
        super().__init__(config)
        self.logger = get_transform_logger("macro_transform")
        self.macro_enricher = EnrichMacros()
        self.gcs_bucket = "candlethrob-wandata"
    
    def _create_validator(self) -> TransformDataValidator:
        """Create validator instance."""
        return TransformDataValidator(self.quality_config)
    
    def _create_rate_limiter(self):
        """Create rate limiter instance."""
        from ..core.base import BaseRateLimiter
        return BaseRateLimiter(calls_per_minute=60)
    
    @log_operation_metrics
    def process(self, series_id: str) -> TransformResult:
        """Transform macro economic data."""
        self.logger.logger.info(f"Starting macro data transformation for {series_id}")
        
        try:
            # Load raw macro data from GCS
            from ..utils.gcs import load_macro_data
            macro_df = load_macro_data(series_id, self.gcs_bucket)
            
            if macro_df is None or macro_df.empty:
                error_msg = f"No raw macro data found for {series_id}"
                self.logger.logger.error(error_msg)
                return TransformResult(
                    success=False,
                    ticker=series_id,
                    error_message=error_msg
                )
            
            # Apply macro enrichment
            self.logger.logger.info(f"Applying macro enrichment to {len(macro_df)} records for {series_id}")
            
            try:
                transformed_df = self.macro_enricher.enrich_macro_data(macro_df)
            except Exception as e:
                error_msg = f"Failed to enrich macro data: {str(e)}"
                self.logger.logger.error(error_msg)
                return TransformResult(
                    success=False,
                    ticker=series_id,
                    error_message=error_msg
                )
            
            # Count new features added
            original_cols = set(macro_df.columns)
            new_cols = set(transformed_df.columns) - original_cols
            features_added = len(new_cols)
            
            # Save transformed macro data to GCS
            from ..utils.gcs import save_transformed_macro_data
            self.logger.logger.info(f"Saving transformed macro data for {series_id} with {features_added} new features")
            success = save_transformed_macro_data(transformed_df, series_id, self.gcs_bucket)
            
            if success:
                self.logger.logger.info(f"Successfully transformed macro data for {series_id}")
                return TransformResult(
                    success=True,
                    ticker=series_id,
                    record_count=len(transformed_df),
                    indicators_added=features_added,
                    quality_score=1.0,  # Default for macro data
                    metadata={"new_features": list(new_cols)}
                )
            else:
                error_msg = f"Failed to save transformed macro data for {series_id}"
                self.logger.logger.error(error_msg)
                return TransformResult(
                    success=False,
                    ticker=series_id,
                    error_message=error_msg
                )
                
        except Exception as e:
            error_msg = f"Error transforming macro data {series_id}: {str(e)}"
            self.logger.logger.error(error_msg)
            return TransformResult(
                success=False,
                ticker=series_id,
                error_message=error_msg
            )


class BatchTransformProcessor(BaseProcessor):
    """Processor for batches of transformations."""
    
    def __init__(self, config: ProcessingConfig, quality_config: QualityConfig):
        self.processing_config = config
        self.quality_config = quality_config
        super().__init__(config)
        self.logger = get_transform_logger("batch_transform")
        self.ticker_processor = TickerTransformProcessor(config, quality_config)
        self.macro_processor = MacroTransformProcessor(config, quality_config)
    
    def _create_validator(self):
        """Create validator instance."""
        return TransformDataValidator(self.quality_config)
    
    def _create_rate_limiter(self):
        """Create rate limiter instance."""
        from ..core.base import BaseRateLimiter
        return BaseRateLimiter(calls_per_minute=60)
    
    @log_operation_metrics
    def process_tickers(self, tickers: List[str]) -> Dict[str, Any]:
        """Process a batch of tickers."""
        results = {
            "total_tickers": len(tickers),
            "successful": 0,
            "failed": 0,
            "failed_tickers": [],
            "results": [],
            "start_time": None,
            "end_time": None,
        }
        
        import time
        start_time = time.time()
        results["start_time"] = start_time
        
        self.logger.logger.info(f"Starting batch transformation for {len(tickers)} tickers")
        
        if self.processing_config.parallel_processing and len(tickers) > 1:
            results.update(self._process_tickers_parallel(tickers))
        else:
            results.update(self._process_tickers_sequential(tickers))
        
        end_time = time.time()
        results["end_time"] = end_time
        results["duration_seconds"] = end_time - start_time
        
        self.logger.logger.info(f"Batch transformation completed: {results['successful']} successful, {results['failed']} failed")
        return results
    
    def _process_tickers_sequential(self, tickers: List[str]) -> Dict[str, Any]:
        """Process tickers sequentially."""
        results = {"successful": 0, "failed": 0, "failed_tickers": [], "results": []}
        
        for i, ticker in enumerate(tickers):
            self.logger.logger.info(f"Processing ticker {i+1}/{len(tickers)}: {ticker}")
            
            result = self.ticker_processor.process(ticker)
            results["results"].append(result)
            
            if result.success:
                results["successful"] += 1
            else:
                results["failed"] += 1
                results["failed_tickers"].append(ticker)
        
        return results
    
    def _process_tickers_parallel(self, tickers: List[str]) -> Dict[str, Any]:
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
                self.logger.logger.info(f"Processing ticker {i+1}/{len(tickers)}: {ticker}")
                
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
                    results["results"].append(TransformResult(
                        success=False,
                        ticker=ticker,
                        error_message=str(e)
                    ))
        
        return results