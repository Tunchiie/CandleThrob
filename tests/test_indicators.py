"""
Comprehensive test suite for technical indicators and data transformation.

This module provides tests for:
- Technical indicator calculations
- Data transformation and enrichment
- Statistical validation of indicators
- Performance benchmarking
- Data quality validation
- Edge cases and error conditions
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any, List

from CandleThrob.ingestion.fetch_data import DataIngestion
from CandleThrob.ingestion.enrich_data import TechnicalIndicators


class TestTechnicalIndicatorsUnit:
    """Unit tests for technical indicators functionality."""
    
    @pytest.mark.unit
    def test_technical_indicators_initialization(self, sample_ticker_data):
        """Test TechnicalIndicators class initialization with various data types."""
        # Test with valid data
        indicators = TechnicalIndicators(sample_ticker_data)
        assert indicators.ticker_df is not None
        assert not indicators.ticker_df.empty
        assert isinstance(indicators.ticker_df, pd.DataFrame)
        
        # Check required columns
        required_columns = ["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]
        for col in required_columns:
            assert col in indicators.ticker_df.columns
        
        # Test with empty DataFrame
        empty_df = pd.DataFrame()
        with pytest.raises(ValueError):
            TechnicalIndicators(empty_df)
    
    @pytest.mark.unit
    def test_enrich_tickers_functionality(self, technical_indicators_instance):
        """Test ticker enrichment with return calculations."""
        # Test before enrichment
        original_columns = set(technical_indicators_instance.ticker_df.columns)
        
        # Perform enrichment
        technical_indicators_instance.enrich_tickers()
        
        # Check that return columns were added
        return_columns = ["Return_1d", "Return_3d", "Return_7d", "Return_30d", "Return_90d", "Return_365d"]
        for col in return_columns:
            assert col in technical_indicators_instance.ticker_df.columns
            assert isinstance(technical_indicators_instance.ticker_df[col], pd.Series)
        
        # Verify new columns were added
        new_columns = set(technical_indicators_instance.ticker_df.columns) - original_columns
        assert len(new_columns) >= len(return_columns)
        
        # Validate return calculations
        for col in return_columns:
            returns = technical_indicators_instance.ticker_df[col].dropna()
            if len(returns) > 0:
                # Returns should be reasonable (not extreme outliers)
                assert returns.abs().max() < 1.0  # No returns > 100%
    
    @pytest.mark.unit
    def test_safe_merge_or_concat_functionality(self, technical_indicators_instance):
        """Test safe merge or concat functionality with various scenarios."""
        # Create test DataFrames
        df1 = technical_indicators_instance.ticker_df.head(5).copy()
        df2 = df1[["Date", "Ticker"]].copy()
        df2["test_column"] = 1.0
        
        # Test successful merge
        merged_df = technical_indicators_instance.safe_merge_or_concat(
            df1, df2, on=["Date", "Ticker"], how="left"
        )
        
        assert isinstance(merged_df, pd.DataFrame)
        assert not merged_df.empty
        assert "test_column" in merged_df.columns
        assert len(merged_df) == len(df1)
        
        # Test with empty DataFrame
        empty_df = pd.DataFrame()
        result = technical_indicators_instance.safe_merge_or_concat(df1, empty_df, on=["Date", "Ticker"])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(df1)
        
        # Test with None DataFrame
        result = technical_indicators_instance.safe_merge_or_concat(df1, None, on=["Date", "Ticker"])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(df1)


class TestTechnicalIndicatorsCalculation:
    """Tests for technical indicator calculations."""
    
    @pytest.mark.unit
    def test_momentum_indicators_calculation(self, technical_indicators_instance):
        """Test momentum indicators calculation and validation."""
        try:
            import talib
        except ImportError:
            pytest.skip("TA-Lib not available for testing")
        
        # Calculate momentum indicators
        momentum_df = technical_indicators_instance.get_talib_momentum_indicators(
            technical_indicators_instance.ticker_df
        )
        
        # Validate structure
        assert isinstance(momentum_df, pd.DataFrame)
        assert not momentum_df.empty
        
        # Check for key momentum indicators
        expected_columns = ["RSI", "MACD", "MACD_Signal", "CCI", "ROC", "SMA20", "EMA20"]
        for col in expected_columns:
            if col in momentum_df.columns:
                assert isinstance(momentum_df[col], pd.Series)
                # Validate indicator ranges
                if col == "RSI":
                    valid_rsi = momentum_df[col].dropna()
                    if len(valid_rsi) > 0:
                        assert (valid_rsi >= 0).all() and (valid_rsi <= 100).all()
    
    @pytest.mark.unit
    def test_volume_indicators_calculation(self, technical_indicators_instance):
        """Test volume indicators calculation and validation."""
        try:
            import talib
        except ImportError:
            pytest.skip("TA-Lib not available for testing")
        
        # Calculate volume indicators
        volume_df = technical_indicators_instance.get_talib_volume_indicators(
            technical_indicators_instance.ticker_df
        )
        
        # Validate structure
        assert isinstance(volume_df, pd.DataFrame)
        assert not volume_df.empty
        
        # Check for key volume indicators
        expected_columns = ["OBV", "AD", "MFI", "VWAP", "ADX"]
        for col in expected_columns:
            if col in volume_df.columns:
                assert isinstance(volume_df[col], pd.Series)
                # Validate indicator properties
                if col == "MFI":
                    valid_mfi = volume_df[col].dropna()
                    if len(valid_mfi) > 0:
                        assert (valid_mfi >= 0).all() and (valid_mfi <= 100).all()
    
    @pytest.mark.unit
    def test_volatility_indicators_calculation(self, technical_indicators_instance):
        """Test volatility indicators calculation and validation."""
        try:
            import talib
        except ImportError:
            pytest.skip("TA-Lib not available for testing")
        
        # Calculate volatility indicators
        volatility_df = technical_indicators_instance.get_talib_volatility_indicators(
            technical_indicators_instance.ticker_df
        )
        
        # Validate structure
        assert isinstance(volatility_df, pd.DataFrame)
        assert not volatility_df.empty
        
        # Check for key volatility indicators
        expected_columns = ["ATR", "BBANDS_UPPER", "BBANDS_MIDDLE", "BBANDS_LOWER"]
        for col in expected_columns:
            if col in volatility_df.columns:
                assert isinstance(volatility_df[col], pd.Series)
        
        # Validate Bollinger Bands relationships
        if all(col in volatility_df.columns for col in ["BBANDS_UPPER", "BBANDS_MIDDLE", "BBANDS_LOWER"]):
            valid_bb = volatility_df[["BBANDS_UPPER", "BBANDS_MIDDLE", "BBANDS_LOWER"]].dropna()
            if len(valid_bb) > 0:
                assert (valid_bb["BBANDS_UPPER"] >= valid_bb["BBANDS_MIDDLE"]).all()
                assert (valid_bb["BBANDS_MIDDLE"] >= valid_bb["BBANDS_LOWER"]).all()
    
    @pytest.mark.unit
    def test_custom_indicators_calculation(self, technical_indicators_instance):
        """Test custom technical indicators calculation."""
        # Test Chaikin Money Flow
        cmf = technical_indicators_instance.chaikin_money_flow(
            technical_indicators_instance.ticker_df, period=20
        )
        
        assert isinstance(cmf, pd.Series)
        assert len(cmf) == len(technical_indicators_instance.ticker_df)
        
        # Test Donchian Channel
        upper, lower = technical_indicators_instance.donchian_channel(
            technical_indicators_instance.ticker_df, period=20
        )
        
        assert isinstance(upper, pd.Series)
        assert isinstance(lower, pd.Series)
        assert len(upper) == len(technical_indicators_instance.ticker_df)
        assert len(lower) == len(technical_indicators_instance.ticker_df)
        
        # Validate channel relationships
        valid_indices = ~(upper.isna() | lower.isna())
        if valid_indices.any():
            assert (upper[valid_indices] >= lower[valid_indices]).all()
        
        # Test Ulcer Index
        close_prices = technical_indicators_instance.ticker_df["Close"]
        ulcer = technical_indicators_instance.ulcer_index(close_prices, period=14)
        
        assert isinstance(ulcer, pd.Series)
        assert len(ulcer) == len(close_prices)
        
        # Ulcer index should be non-negative
        valid_ulcer = ulcer.dropna()
        if len(valid_ulcer) > 0:
            assert (valid_ulcer >= 0).all()


class TestDataQualityValidation:
    """Data quality validation tests for technical indicators."""
    
    @pytest.mark.data_quality
    def test_indicator_data_quality(self, technical_indicators_instance, data_quality_validator):
        """Test data quality validation for technical indicators."""
        # Enrich data first
        technical_indicators_instance.enrich_tickers()
        
        # Calculate technical indicators
        try:
            import talib
            result_df = technical_indicators_instance.calculate_technical_indicators()
            
            # Validate indicator data quality
            validation_results = data_quality_validator.validate_ohlcv_data(result_df)
            
            assert validation_results["is_valid"] is True
            assert len(validation_results["errors"]) == 0
            
            # Check for reasonable indicator values
            indicator_columns = [col for col in result_df.columns if col not in 
                               ["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]]
            
            for col in indicator_columns:
                if col in result_df.columns:
                    values = result_df[col].dropna()
                    if len(values) > 0:
                        # Check for extreme outliers
                        q99 = values.quantile(0.99)
                        q01 = values.quantile(0.01)
                        iqr = values.quantile(0.75) - values.quantile(0.25)
                        
                        # No extreme outliers (beyond 3 IQR)
                        assert not ((values > q99 + 3 * iqr) | (values < q01 - 3 * iqr)).any()
                        
        except ImportError:
            pytest.skip("TA-Lib not available for testing")
    
    @pytest.mark.data_quality
    def test_indicator_consistency(self, technical_indicators_instance):
        """Test consistency of technical indicators across different periods."""
        # Test with different lookback periods
        periods = [10, 20, 50]
        
        for period in periods:
            # Calculate SMA with different periods
            sma = technical_indicators_instance.ticker_df["Close"].rolling(window=period).mean()
            
            # Validate SMA properties
            assert isinstance(sma, pd.Series)
            assert len(sma) == len(technical_indicators_instance.ticker_df)
            
            # SMA should be reasonable relative to price
            valid_sma = sma.dropna()
            if len(valid_sma) > 0:
                close_prices = technical_indicators_instance.ticker_df["Close"].iloc[-len(valid_sma):]
                # SMA should be within reasonable range of prices
                assert (valid_sma > 0).all()
                assert (valid_sma < close_prices * 2).all()  # SMA shouldn't be 2x price


class TestStatisticalValidation:
    """Statistical validation tests for technical indicators."""
    
    @pytest.mark.statistical
    def test_indicator_statistical_properties(self, technical_indicators_instance, statistical_validator):
        """Test statistical properties of technical indicators."""
        # Enrich data first
        technical_indicators_instance.enrich_tickers()
        
        # Test return series properties
        returns = technical_indicators_instance.ticker_df["Return_1d"].dropna()
        
        if len(returns) > 10:
            # Test stationarity of returns
            stationarity_results = statistical_validator.validate_stationarity(returns)
            assert "is_stationary" in stationarity_results
            
            # Test normality of returns
            normality_results = statistical_validator.validate_normality(returns)
            assert "is_normal" in normality_results
            
            # Test autocorrelation of returns
            autocorr_results = statistical_validator.calculate_autocorrelation(returns)
            assert "autocorr" in autocorr_results
    
    @pytest.mark.statistical
    def test_indicator_correlation_analysis(self, technical_indicators_instance):
        """Test correlation analysis between different indicators."""
        # Enrich data first
        technical_indicators_instance.enrich_tickers()
        
        # Calculate some basic indicators
        close_prices = technical_indicators_instance.ticker_df["Close"]
        sma_20 = close_prices.rolling(window=20).mean()
        sma_50 = close_prices.rolling(window=50).mean()
        
        # Calculate correlation
        correlation = sma_20.corr(sma_50)
        
        # Correlation should be reasonable
        assert not pd.isna(correlation)
        assert -1 <= correlation <= 1
        
        # SMA20 and SMA50 should be positively correlated
        if not pd.isna(correlation):
            assert correlation > 0.5  # Strong positive correlation expected


class TestPerformanceBenchmarking:
    """Performance benchmarking tests for technical indicators."""
    
    @pytest.mark.performance
    def test_indicator_calculation_performance(self, technical_indicators_instance, performance_benchmark):
        """Benchmark technical indicator calculation performance."""
        performance_benchmark.start()
        
        # Enrich data
        technical_indicators_instance.enrich_tickers()
        
        # Calculate technical indicators
        try:
            import talib
            result_df = technical_indicators_instance.calculate_technical_indicators()
            
            performance_benchmark.end()
            metrics = performance_benchmark.get_metrics()
            
            # Validate performance metrics
            assert "duration" in metrics
            assert metrics["duration"] > 0
            assert metrics["duration"] < 5.0  # Should complete within 5 seconds
            
            # Validate results
            assert result_df is not None
            assert not result_df.empty
            
        except ImportError:
            pytest.skip("TA-Lib not available for testing")
    
    @pytest.mark.performance
    def test_bulk_indicator_calculation_performance(self, sample_ticker_data, performance_benchmark):
        """Benchmark bulk indicator calculation performance."""
        # Create multiple ticker datasets
        tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
        datasets = []
        
        for ticker in tickers:
            df = sample_ticker_data.copy()
            df["Ticker"] = ticker
            datasets.append(df)
        
        performance_benchmark.start()
        
        # Process all datasets
        results = []
        for df in datasets:
            indicators = TechnicalIndicators(df)
            indicators.enrich_tickers()
            
            try:
                import talib
                result = indicators.calculate_technical_indicators()
                results.append(result)
            except ImportError:
                results.append(df)  # Fallback to original data
        
        performance_benchmark.end()
        metrics = performance_benchmark.get_metrics()
        
        # Validate performance metrics
        assert "duration" in metrics
        assert metrics["duration"] > 0
        assert metrics["duration"] < 15.0  # Should complete within 15 seconds
        
        # Validate all datasets were processed
        assert len(results) == len(datasets)
        assert all(df is not None and not df.empty for df in results)


class TestIntegrationTests:
    """Integration tests for technical indicators with real data scenarios."""
    
    @pytest.mark.integration
    def test_end_to_end_indicator_pipeline(self, technical_indicators_instance):
        """Test complete technical indicator calculation pipeline."""
        # Step 1: Enrich data
        technical_indicators_instance.enrich_tickers()
        
        # Verify enrichment worked
        return_columns = ["Return_1d", "Return_3d", "Return_7d", "Return_30d", "Return_90d", "Return_365d"]
        for col in return_columns:
            assert col in technical_indicators_instance.ticker_df.columns
        
        # Step 2: Calculate technical indicators
        try:
            import talib
            result_df = technical_indicators_instance.calculate_technical_indicators()
            
            # Verify indicators were calculated
            assert result_df is not None
            assert not result_df.empty
            
            # Check that original data is preserved
            original_columns = ["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]
            for col in original_columns:
                assert col in result_df.columns
            
            # Check that new indicators were added
            indicator_columns = [col for col in result_df.columns if col not in original_columns + return_columns]
            assert len(indicator_columns) > 0
            
        except ImportError:
            pytest.skip("TA-Lib not available for testing")
    
    @pytest.mark.integration
    def test_data_persistence_integration(self, technical_indicators_instance, test_data_dir):
        """Test data persistence integration."""
        # Enrich and calculate indicators
        technical_indicators_instance.enrich_tickers()
        
        try:
            import talib
            result_df = technical_indicators_instance.calculate_technical_indicators()
            
            # Test file saving
            test_file = f"{test_data_dir}/test_transformed_data.parquet"
            technical_indicators_instance.persist_transformed_data(test_file)
            
            # Verify file was created and can be read
            import os
            assert os.path.exists(test_file)
            
            # Try to read the file back
            loaded_df = pd.read_parquet(test_file)
            assert isinstance(loaded_df, pd.DataFrame)
            assert not loaded_df.empty
            
            # Verify data integrity
            assert len(loaded_df) == len(result_df)
            assert set(loaded_df.columns) == set(result_df.columns)
            
        except ImportError:
            pytest.skip("TA-Lib not available for testing")


class TestErrorHandling:
    """Error handling and edge case tests for technical indicators."""
    
    @pytest.mark.unit
    def test_invalid_data_handling(self):
        """Test handling of invalid data in technical indicators."""
        # Test with insufficient data
        insufficient_data = pd.DataFrame({
            "Date": pd.date_range("2024-01-01", periods=5),
            "Ticker": "TEST",
            "Open": [100] * 5,
            "High": [110] * 5,
            "Low": [90] * 5,
            "Close": [105] * 5,
            "Volume": [1000] * 5
        })
        
        indicators = TechnicalIndicators(insufficient_data)
        
        # Should handle insufficient data gracefully
        indicators.enrich_tickers()
        
        # Returns should have NaN values for longer periods
        assert indicators.ticker_df["Return_365d"].isna().all()
    
    @pytest.mark.unit
    def test_missing_data_handling(self, technical_indicators_instance):
        """Test handling of missing data in technical indicators."""
        # Introduce missing data
        df_with_missing = technical_indicators_instance.ticker_df.copy()
        df_with_missing.loc[5:10, "Close"] = np.nan
        
        indicators = TechnicalIndicators(df_with_missing)
        
        # Should handle missing data gracefully
        indicators.enrich_tickers()
        
        # Check that calculations still work
        assert "Return_1d" in indicators.ticker_df.columns
        
        # Missing data should result in NaN values
        assert indicators.ticker_df["Return_1d"].isna().any()


class TestRegressionTests:
    """Regression tests to ensure consistent indicator behavior."""
    
    @pytest.mark.regression
    def test_indicator_calculation_consistency(self, technical_indicators_instance):
        """Regression test for indicator calculation consistency."""
        # Calculate indicators twice
        technical_indicators_instance.enrich_tickers()
        
        df1 = technical_indicators_instance.ticker_df.copy()
        
        # Reset and recalculate
        technical_indicators_instance.enrich_tickers()
        df2 = technical_indicators_instance.ticker_df.copy()
        
        # Results should be identical
        pd.testing.assert_frame_equal(df1, df2)
    
    @pytest.mark.regression
    def test_indicator_value_ranges(self, technical_indicators_instance):
        """Regression test for indicator value ranges."""
        technical_indicators_instance.enrich_tickers()
        
        # Test return value ranges
        for col in ["Return_1d", "Return_3d", "Return_7d"]:
            if col in technical_indicators_instance.ticker_df.columns:
                returns = technical_indicators_instance.ticker_df[col].dropna()
                if len(returns) > 0:
                    # Returns should be reasonable
                    assert returns.abs().max() < 1.0  # No returns > 100%


class TestSmokeTests:
    """Smoke tests for critical technical indicator functionality."""
    
    @pytest.mark.smoke
    def test_basic_indicator_smoke(self, technical_indicators_instance):
        """Smoke test for basic technical indicator functionality."""
        # Test that basic enrichment works
        technical_indicators_instance.enrich_tickers()
        
        # Check that returns were calculated
        assert "Return_1d" in technical_indicators_instance.ticker_df.columns
        
        # Test that safe merge works
        df1 = technical_indicators_instance.ticker_df.head(3)
        df2 = technical_indicators_instance.ticker_df.tail(3)
        
        merged = technical_indicators_instance.safe_merge_or_concat(df1, df2, on=["Date", "Ticker"])
        assert isinstance(merged, pd.DataFrame)
    
    @pytest.mark.smoke
    def test_custom_indicator_smoke(self, technical_indicators_instance):
        """Smoke test for custom technical indicators."""
        # Test Chaikin Money Flow
        cmf = technical_indicators_instance.chaikin_money_flow(
            technical_indicators_instance.ticker_df, period=10
        )
        assert isinstance(cmf, pd.Series)
        
        # Test Donchian Channel
        upper, lower = technical_indicators_instance.donchian_channel(
            technical_indicators_instance.ticker_df, period=10
        )
        assert isinstance(upper, pd.Series)
        assert isinstance(lower, pd.Series)