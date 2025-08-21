"""
Transform Orchestrator for CandleThrob
======================================

This module provides the main orchestration logic for data transformation,
replacing the monolithic transform_data.py with a more modular approach.
"""

import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

from ..core.config import get_config, CandleThrobConfig
from ..core.logging import get_transform_logger, CandleThrobLogger
from .processors import BatchTransformProcessor, TransformResult


class TransformOrchestrator:
    """Main orchestrator for data transformation operations."""
    
    def __init__(self, config: Optional[CandleThrobConfig] = None):
        """Initialize the transform orchestrator."""
        self.config = config or get_config()
        self.logger = get_transform_logger("orchestrator")
        self.batch_processor = BatchTransformProcessor(
            self.config.processing,
            self.config.quality
        )
        
        # Initialize logging system
        CandleThrobLogger.initialize(self.config.logging)
        
        self.logger.logger.info("TransformOrchestrator initialized")
    
    def run_complete_transformation(self) -> Dict[str, Any]:
        """Run complete data transformation for all available tickers."""
        self.logger.logger.info("Starting complete data transformation")
        
        # Get all available tickers from GCS
        available_tickers = self._get_available_tickers()
        
        if not available_tickers:
            error_msg = "No tickers available for transformation"
            self.logger.logger.error(error_msg)
            return {"error": error_msg}
        
        self.logger.logger.info(f"Found {len(available_tickers)} tickers for transformation")
        
        # Calculate batches
        batch_size = self.config.processing.batch_size
        total_batches = (len(available_tickers) + batch_size - 1) // batch_size
        
        self.logger.logger.info(f"Processing {total_batches} batches of {batch_size} tickers each")
        
        # Process all batches
        overall_results = self._process_all_batches(available_tickers, batch_size, total_batches)
        
        # Transform macro data
        macro_results = self._transform_macro_data()
        overall_results["macro_results"] = macro_results
        
        # Save results and signal completion
        self._finalize_transformation(overall_results)
        
        return overall_results
    
    def _get_available_tickers(self) -> List[str]:
        """Get list of available tickers from GCS."""
        try:
            from ..utils.gcs import list_available_tickers
            tickers = list_available_tickers(bucket_name=self.config.storage.gcs_bucket)
            self.logger.logger.info(f"Found {len(tickers)} available tickers in GCS")
            return tickers
        except Exception as e:
            self.logger.logger.error(f"Failed to get available tickers: {e}")
            return []
    
    def _process_all_batches(self, tickers: List[str], batch_size: int, total_batches: int) -> Dict[str, Any]:
        """Process all batches of tickers."""
        start_time = datetime.now()
        overall_results = {
            "total_batches": total_batches,
            "total_tickers": len(tickers),
            "successful_tickers": 0,
            "failed_tickers": 0,
            "failed_tickers_list": [],
            "batch_results": [],
            "start_time": start_time.isoformat(),
            "end_time": None
        }
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, len(tickers))
            
            batch_tickers = tickers[start_idx:end_idx]
            self.logger.logger.info(f"Processing batch {batch_num + 1}/{total_batches}: tickers {start_idx}-{end_idx-1} ({len(batch_tickers)} tickers)")
            self.logger.logger.info(f"Batch tickers: {batch_tickers}")
            
            # Process batch
            batch_results = self.batch_processor.process_tickers(batch_tickers)
            
            # Update overall results
            overall_results["successful_tickers"] += batch_results["successful"]
            overall_results["failed_tickers"] += batch_results["failed"]
            overall_results["failed_tickers_list"].extend(batch_results["failed_tickers"])
            overall_results["batch_results"].append({
                "batch_number": batch_num,
                "results": batch_results
            })
            
            self.logger.logger.info(f"Batch {batch_num + 1} completed: {batch_results['successful']} successful, {batch_results['failed']} failed")
            self.logger.logger.info(f"Running total: {overall_results['successful_tickers']} successful, {overall_results['failed_tickers']} failed")
        
        end_time = datetime.now()
        overall_results["end_time"] = end_time.isoformat()
        overall_results["duration_seconds"] = (end_time - start_time).total_seconds()
        
        return overall_results
    
    def _transform_macro_data(self) -> Dict[str, Any]:
        """Transform macro economic data."""
        self.logger.logger.info("Starting macro data transformation")
        
        try:
            from ..utils.gcs import list_available_macro_series
            available_series = list_available_macro_series(bucket_name=self.config.storage.gcs_bucket)
            
            if not available_series:
                self.logger.logger.warning("No macro data series available for transformation")
                return {"total_series": 0, "successful": 0, "failed": 0}
            
            self.logger.logger.info(f"Found {len(available_series)} macro data series for transformation")
            
            results = {"total_series": len(available_series), "successful": 0, "failed": 0, "failed_series": []}
            
            for series_id in available_series:
                try:
                    result = self.batch_processor.macro_processor.process(series_id)
                    if result.success:
                        results["successful"] += 1
                    else:
                        results["failed"] += 1
                        results["failed_series"].append(series_id)
                except Exception as e:
                    self.logger.logger.error(f"Error transforming macro series {series_id}: {e}")
                    results["failed"] += 1
                    results["failed_series"].append(series_id)
            
            self.logger.logger.info(f"Macro transformation completed: {results['successful']} successful, {results['failed']} failed")
            return results
            
        except Exception as e:
            self.logger.logger.error(f"Failed to transform macro data: {e}")
            return {"error": str(e)}
    
    def _finalize_transformation(self, results: Dict[str, Any]) -> None:
        """Finalize transformation process and save results."""
        # Log final results
        self.logger.logger.info("Complete transformation finished!")
        self.logger.logger.info(f"Total batches processed: {results['total_batches']}")
        self.logger.logger.info(f"Total tickers processed: {results['total_tickers']}")
        self.logger.logger.info(f"Successful: {results['successful_tickers']}")
        self.logger.logger.info(f"Failed: {results['failed_tickers']}")
        self.logger.logger.info(f"Total duration: {results['duration_seconds']:.2f} seconds")
        
        if results['failed_tickers'] > 0:
            self.logger.logger.warning(f"Failed tickers: {results['failed_tickers_list']}")
        
        # Log macro results
        macro_results = results.get("macro_results", {})
        if "error" not in macro_results:
            self.logger.logger.info(f"Macro data: {macro_results.get('successful', 0)} successful, {macro_results.get('failed', 0)} failed")
        
        # Create data directory and save results
        data_dir = Path(self.config.storage.local_data_dir)
        data_dir.mkdir(parents=True, exist_ok=True)
        
        # Signal completion
        try:
            completion_flag = data_dir / "transform_cycle_complete.flag"
            with open(completion_flag, "w") as f:
                f.write(datetime.now().isoformat())
            self.logger.logger.info(f"Transform cycle complete flag written to: {completion_flag}")
        except Exception as e:
            self.logger.logger.error(f"Failed to write transform cycle complete flag: {e}")
        
        # Save comprehensive results
        results_file = data_dir / "transform_results.json"
        try:
            with open(results_file, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            self.logger.logger.info(f"Results saved to {results_file}")
        except Exception as e:
            self.logger.logger.error(f"Failed to save results: {e}")
    
    def run_single_ticker(self, ticker: str) -> TransformResult:
        """Run transformation for a single ticker."""
        self.logger.logger.info(f"Starting single ticker transformation for {ticker}")
        
        # Use ticker processor directly
        result = self.batch_processor.ticker_processor.process(ticker)
        
        self.logger.logger.info(f"Single ticker transformation completed for {ticker}: {'success' if result.success else 'failed'}")
        return result
    
    def run_batch(self, tickers: List[str]) -> Dict[str, Any]:
        """Run transformation for a specific batch of tickers."""
        self.logger.logger.info(f"Starting batch transformation for {len(tickers)} tickers")
        
        results = self.batch_processor.process_tickers(tickers)
        
        self.logger.logger.info(f"Batch transformation completed: {results['successful']} successful, {results['failed']} failed")
        return results
    
    def run_ticker_validation(self) -> Dict[str, Any]:
        """Run validation on transformed ticker data."""
        self.logger.logger.info("Starting transformed data validation")
        
        validation_results = {
            "total_tickers": 0,
            "valid_tickers": 0,
            "invalid_tickers": 0,
            "validation_issues": [],
            "timestamp": datetime.now().isoformat()
        }
        
        try:
            # Get available transformed tickers
            from ..utils.gcs import list_available_tickers
            tickers = list_available_tickers(bucket_name=self.config.storage.gcs_bucket)
            validation_results["total_tickers"] = len(tickers)
            
            # Sample validation on a subset of tickers
            sample_size = min(100, len(tickers))
            sample_tickers = tickers[:sample_size]
            
            self.logger.logger.info(f"Validating sample of {sample_size} tickers")
            
            for ticker in sample_tickers:
                try:
                    from ..utils.gcs import load_ticker_data
                    df = load_ticker_data(ticker, self.config.storage.gcs_bucket)
                    
                    if df is not None and not df.empty:
                        validation_result = self.batch_processor.validator.validate(df)
                        if validation_result.is_valid:
                            validation_results["valid_tickers"] += 1
                        else:
                            validation_results["invalid_tickers"] += 1
                            validation_results["validation_issues"].append({
                                "ticker": ticker,
                                "issues": validation_result.issues,
                                "quality_score": validation_result.quality_score
                            })
                    else:
                        validation_results["invalid_tickers"] += 1
                        validation_results["validation_issues"].append({
                            "ticker": ticker,
                            "issues": ["No data found"],
                            "quality_score": 0.0
                        })
                        
                except Exception as e:
                    validation_results["invalid_tickers"] += 1
                    validation_results["validation_issues"].append({
                        "ticker": ticker,
                        "issues": [f"Validation error: {str(e)}"],
                        "quality_score": 0.0
                    })
            
            self.logger.logger.info(f"Validation completed: {validation_results['valid_tickers']} valid, {validation_results['invalid_tickers']} invalid")
            return validation_results
            
        except Exception as e:
            self.logger.logger.error(f"Failed to run validation: {e}")
            return {"error": str(e)}


def main():
    """Main function for command-line execution."""
    try:
        # Initialize configuration from environment
        config = get_config()
        
        # Create orchestrator and run complete transformation
        orchestrator = TransformOrchestrator(config)
        results = orchestrator.run_complete_transformation()
        
        # Exit with appropriate code
        if results.get("error") or results.get("failed_tickers", 0) > 0:
            exit(1)
        else:
            exit(0)
            
    except Exception as e:
        logger = get_transform_logger("main")
        logger.logger.error(f"Fatal error in main: {str(e)}")
        import traceback
        logger.logger.error(f"Traceback: {traceback.format_exc()}")
        exit(1)


if __name__ == "__main__":
    main()