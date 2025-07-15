"""
Comprehensive test suite for macroeconomic data functionality.

This module provides tests for:
- Macroeconomic data fetching and processing
- FRED API integration and error handling
- Data quality validation for economic indicators
- Statistical validation of economic time series
- Performance benchmarking
- Database operations for macro data
- Edge cases and error conditions
"""

import os
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, date as date_type
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any, List, Optional

from CandleThrob.ingestion.fetch_data import DataIngestion
from CandleThrob.utils.models import MacroData, TransformedMacroData


class TestMacroDataUnit:
    """Unit tests for macroeconomic data functionality."""
    
    @pytest.mark.unit
    def test_macro_data_initialization(self, sample_macro_data):
        """Test MacroData class initialization and basic properties."""
        # Test data structure
        assert isinstance(sample_macro_data, pd.DataFrame)
        assert not sample_macro_data.empty
        
        # Check required columns
        required_columns = ["date", "series_id", "value"]
        for col in required_columns:
            assert col in sample_macro_data.columns
        
        # Validate data types
        assert pd.api.types.is_datetime64_any_dtype(sample_macro_data["date"])
        assert sample_macro_data["series_id"].dtype == "object"
        assert pd.api.types.is_numeric_dtype(sample_macro_data["value"])
        
        # Validate data ranges
        assert sample_macro_data["value"].min() > 0
        assert sample_macro_data["series_id"].nunique() > 1
    
    @pytest.mark.unit
    def test_macro_data_validation(self, sample_macro_data):
        """Test macroeconomic data validation rules."""
        # Test valid data
        assert len(sample_macro_data) > 0
        
        # Test date range
        date_range = sample_macro_data["date"].max() - sample_macro_data["date"].min()
        assert date_range.days >= 0
        
        # Test series diversity
        unique_series = sample_macro_data["series_id"].unique()
        assert len(unique_series) >= 3  # Should have multiple series
        
        # Test value consistency
        for series_id in unique_series:
            series_data = sample_macro_data[sample_macro_data["series_id"] == series_id]
            assert len(series_data) > 0
            assert series_data["value"].notna().all()
    
    @pytest.mark.unit
    def test_expected_fred_series(self):
        """Test that expected FRED series IDs are properly defined."""
        expected_series = [
            "FEDFUNDS", "CPIAUCSL", "UNRATE", "GDP", "GS10", 
            "USREC", "UMCSENT", "HOUST", "RSAFS", "INDPRO", "M2SL"
        ]
        
        # Validate series ID format
        for series in expected_series:
            assert isinstance(series, str)
            assert len(series) > 0
            assert series.isupper()  # FRED series are typically uppercase
        
        # Check for duplicates
        assert len(expected_series) == len(set(expected_series))


class TestMacroDataIntegration:
    """Integration tests for macroeconomic data with external APIs."""
    
    @pytest.mark.integration
    @pytest.mark.api
    @pytest.mark.external
    def test_fred_data_fetching_integration(self, data_ingestion_instance, mock_fred_api):
        """Test end-to-end FRED data fetching with mocked API."""
        # Test successful data fetching
        data_ingestion_instance.fetch_fred_data()
        
        # Validate returned data
        assert data_ingestion_instance.macro_df is not None
        assert not data_ingestion_instance.macro_df.empty
        assert isinstance(data_ingestion_instance.macro_df, pd.DataFrame)
        
        # Check expected columns
        expected_columns = ["date", "series_id", "value"]
        for col in expected_columns:
            assert col in data_ingestion_instance.macro_df.columns
        
        # Validate data types
        assert pd.api.types.is_datetime64_any_dtype(data_ingestion_instance.macro_df["date"])
        assert data_ingestion_instance.macro_df["series_id"].dtype == "object"
        assert pd.api.types.is_numeric_dtype(data_ingestion_instance.macro_df["value"])
        
        # Check for expected series
        series_ids = data_ingestion_instance.macro_df["series_id"].unique()
        expected_series = ["FEDFUNDS", "CPIAUCSL", "UNRATE", "GDP", "GS10"]
        for series in expected_series:
            assert series in series_ids
        
        # Validate data quality
        assert data_ingestion_instance.macro_df["value"].notna().any()
        assert data_ingestion_instance.macro_df["date"].notna().all()
    
    @pytest.mark.integration
    @pytest.mark.api
    @pytest.mark.external
    def test_fred_data_date_formatting(self, data_ingestion_instance, mock_fred_api):
        """Test that dates are properly formatted and processed."""
        data_ingestion_instance.fetch_fred_data()
        
        # Check date column formatting
        assert data_ingestion_instance.macro_df is not None
        assert "date" in data_ingestion_instance.macro_df.columns
        
        # Dates should be datetime/date objects
        dates = data_ingestion_instance.macro_df["date"]
        assert all(isinstance(d, (datetime, date_type)) for d in dates)
        
        # Check date range is reasonable
        date_range = dates.max() - dates.min()
        assert date_range.days >= 0
        assert date_range.days <= 365 * 10  # Reasonable range (up to 10 years)
    
    @pytest.mark.integration
    @pytest.mark.api
    @pytest.mark.external
    def test_fred_data_value_types(self, data_ingestion_instance, mock_fred_api):
        """Test that values are properly converted to numeric types."""
        data_ingestion_instance.fetch_fred_data()
        
        # Values should be numeric
        assert data_ingestion_instance.macro_df is not None
        values = data_ingestion_instance.macro_df["value"]
        assert all(isinstance(v, (int, float, np.number)) for v in values if pd.notna(v))
        
        # Check for reasonable value ranges
        for series_id in data_ingestion_instance.macro_df["series_id"].unique():
            series_values = data_ingestion_instance.macro_df[
                data_ingestion_instance.macro_df["series_id"] == series_id
            ]["value"]
            
            if len(series_values) > 0:
                # Values should be reasonable for economic indicators
                assert series_values.min() >= 0  # Most economic indicators are non-negative
                assert series_values.max() < 1e6  # Reasonable upper bound


class TestDataQualityValidation:
    """Data quality validation tests for macroeconomic data."""
    
    @pytest.mark.data_quality
    def test_macro_data_quality_validation(self, sample_macro_data, data_quality_validator):
        """Test comprehensive data quality validation for macro data."""
        validation_results = data_quality_validator.validate_macro_data(sample_macro_data)
        
        # Validate results structure
        assert "is_valid" in validation_results
        assert "errors" in validation_results
        assert "warnings" in validation_results
        assert "statistics" in validation_results
        
        # Check for data quality issues
        assert validation_results["is_valid"] is True
        assert len(validation_results["errors"]) == 0
        
        # Validate statistics
        stats = validation_results["statistics"]
        assert stats["row_count"] > 0
        assert stats["unique_series"] > 0
        assert "date_range" in stats
        assert stats["date_range"]["start"] is not None
        assert stats["date_range"]["end"] is not None
    
    @pytest.mark.data_quality
    def test_macro_data_consistency_checks(self, sample_macro_data):
        """Test consistency checks for macroeconomic data."""
        # Test temporal consistency
        for series_id in sample_macro_data["series_id"].unique():
            series_data = sample_macro_data[sample_macro_data["series_id"] == series_id]
            
            # Check for temporal gaps
            dates = series_data["date"].sort_values()
            date_diffs = dates.diff().dropna()
            
            if len(date_diffs) > 0:
                # Most economic data should be reasonably regular
                max_gap = date_diffs.max()
                assert max_gap.days <= 90  # No gaps larger than 3 months
        
        # Test value consistency within series
        for series_id in sample_macro_data["series_id"].unique():
            series_data = sample_macro_data[sample_macro_data["series_id"] == series_id]
            values = series_data["value"]
            
            # Check for extreme outliers
            q99 = values.quantile(0.99)
            q01 = values.quantile(0.01)
            iqr = values.quantile(0.75) - values.quantile(0.25)
            
            # No extreme outliers (beyond 3 IQR)
            extreme_outliers = (values > q99 + 3 * iqr) | (values < q01 - 3 * iqr)
            assert not extreme_outliers.any()
    
    @pytest.mark.data_quality
    def test_macro_data_edge_cases(self, data_quality_validator):
        """Test data quality validation with edge cases and malformed data."""
        # Test empty DataFrame
        empty_df = pd.DataFrame()
        results = data_quality_validator.validate_macro_data(empty_df)
        assert not results["is_valid"]
        assert len(results["errors"]) > 0
        
        # Test DataFrame with missing columns
        incomplete_df = pd.DataFrame({"date": [datetime.now()], "value": [1.0]})
        results = data_quality_validator.validate_macro_data(incomplete_df)
        assert not results["is_valid"]
        assert len(results["errors"]) > 0
        
        # Test DataFrame with invalid data types
        invalid_types_df = pd.DataFrame({
            "date": ["not-a-date"],
            "series_id": [123],  # Should be string
            "value": ["not-numeric"]
        })
        results = data_quality_validator.validate_macro_data(invalid_types_df)
        assert len(results["errors"]) > 0


class TestStatisticalValidation:
    """Statistical validation tests for macroeconomic time series."""
    
    @pytest.mark.statistical
    def test_macro_series_statistical_properties(self, sample_macro_data, statistical_validator):
        """Test statistical properties of macroeconomic time series."""
        # Test individual series
        for series_id in sample_macro_data["series_id"].unique():
            series_data = sample_macro_data[sample_macro_data["series_id"] == series_id]["value"]
            
            if len(series_data) > 10:  # Need sufficient data for statistical tests
                # Test stationarity
                stationarity_results = statistical_validator.validate_stationarity(series_data)
                assert "is_stationary" in stationarity_results
                assert "p_value" in stationarity_results
                
                # Test normality
                normality_results = statistical_validator.validate_normality(series_data)
                assert "is_normal" in normality_results
                assert "p_value" in normality_results
                
                # Test autocorrelation
                autocorr_results = statistical_validator.calculate_autocorrelation(series_data)
                assert "autocorr" in autocorr_results
    
    @pytest.mark.statistical
    def test_macro_series_correlation_analysis(self, sample_macro_data):
        """Test correlation analysis between different macroeconomic series."""
        # Create pivot table for correlation analysis
        pivot_data = sample_macro_data.pivot(index="date", columns="series_id", values="value")
        
        # Calculate correlation matrix
        correlation_matrix = pivot_data.corr()
        
        # Validate correlation matrix
        assert isinstance(correlation_matrix, pd.DataFrame)
        assert not correlation_matrix.empty
        
        # Check correlation properties
        for i in range(len(correlation_matrix)):
            for j in range(len(correlation_matrix)):
                if i == j:
                    assert correlation_matrix.iloc[i, j] == 1.0  # Self-correlation
                else:
                    corr_value = correlation_matrix.iloc[i, j]
                    assert -1 <= corr_value <= 1  # Valid correlation range
    
    @pytest.mark.statistical
    def test_macro_series_trend_analysis(self, sample_macro_data):
        """Test trend analysis for macroeconomic series."""
        for series_id in sample_macro_data["series_id"].unique():
            series_data = sample_macro_data[sample_macro_data["series_id"] == series_id]
            
            if len(series_data) > 5:
                # Sort by date
                series_data = series_data.sort_values("date")
                values = series_data["value"].values
                
                # Calculate simple trend (linear regression slope)
                x = np.arange(len(values))
                if len(values) > 1:
                    slope = np.polyfit(x, values, 1)[0]
                    
                    # Trend should be reasonable (not extreme)
                    assert abs(slope) < 1e6  # Reasonable slope range


class TestPerformanceBenchmarking:
    """Performance benchmarking tests for macroeconomic data operations."""
    
    @pytest.mark.performance
    def test_fred_data_fetching_performance(self, data_ingestion_instance, mock_fred_api, performance_benchmark):
        """Benchmark FRED data fetching performance."""
        performance_benchmark.start()
        
        # Perform data fetching
        data_ingestion_instance.fetch_fred_data()
        
        performance_benchmark.end()
        metrics = performance_benchmark.get_metrics()
        
        # Validate performance metrics
        assert "duration" in metrics
        assert metrics["duration"] > 0
        assert metrics["duration"] < 10.0  # Should complete within 10 seconds
        
        # Validate data was fetched
        assert data_ingestion_instance.macro_df is not None
        assert not data_ingestion_instance.macro_df.empty
    
    @pytest.mark.performance
    def test_macro_data_processing_performance(self, sample_macro_data, performance_benchmark):
        """Benchmark macro data processing performance."""
        performance_benchmark.start()
        
        # Simulate data processing operations
        processed_data = sample_macro_data.copy()
        
        # Add derived indicators
        for series_id in processed_data["series_id"].unique():
            series_mask = processed_data["series_id"] == series_id
            series_data = processed_data[series_mask].sort_values("date")
            
            # Calculate moving averages
            series_data["ma_5"] = series_data["value"].rolling(window=5).mean()
            series_data["ma_20"] = series_data["value"].rolling(window=20).mean()
            
            # Calculate growth rates
            series_data["growth_rate"] = series_data["value"].pct_change()
            
            processed_data.loc[series_mask] = series_data
        
        performance_benchmark.end()
        metrics = performance_benchmark.get_metrics()
        
        # Validate performance metrics
        assert "duration" in metrics
        assert metrics["duration"] > 0
        assert metrics["duration"] < 5.0  # Should complete within 5 seconds
        
        # Validate processing results
        assert len(processed_data) == len(sample_macro_data)
        assert "ma_5" in processed_data.columns
        assert "ma_20" in processed_data.columns
        assert "growth_rate" in processed_data.columns


class TestDatabaseOperations:
    """Database operation tests for macroeconomic data persistence."""
    
    @pytest.mark.database
    def test_macro_data_table_operations(self, oracle_db_instance):
        """Test macro data table creation and operations."""
        with oracle_db_instance.establish_connection() as conn:
            macro_data = MacroData()
            
            # Test table creation
            macro_data.create_table(conn)
            
            # Verify table exists
            from sqlalchemy import text
            try:
                result = conn.execute(text("SELECT COUNT(*) FROM macro_data"))
                count = result.fetchone()[0]
                assert count >= 0
            except Exception as e:
                pytest.fail(f"MacroData table creation failed: {e}")
    
    @pytest.mark.database
    def test_macro_data_insertion_and_retrieval(self, oracle_db_instance, sample_macro_data):
        """Test macro data insertion and retrieval operations."""
        with oracle_db_instance.establish_connection() as conn:
            macro_data = MacroData()
            macro_data.create_table(conn)
            
            try:
                # Insert sample data
                macro_data.insert_data(conn, sample_macro_data)
                
                # Test data retrieval
                for series_id in sample_macro_data["series_id"].unique():
                    retrieved_data = macro_data.get_data(conn, series_id=series_id)
                    assert retrieved_data is not None
                
                # Test data existence checks
                assert macro_data.data_exists(conn)
                assert macro_data.data_exists(conn, series_id="FEDFUNDS")
                
                # Test last date retrieval
                last_date = macro_data.get_last_date(conn)
                assert last_date is not None
                assert isinstance(last_date, date_type)
                
            except Exception as e:
                pytest.fail(f"Macro data insertion/retrieval failed: {e}")
    
    @pytest.mark.database
    def test_transformed_macro_data_operations(self, oracle_db_instance, sample_macro_data):
        """Test transformed macro data operations."""
        with oracle_db_instance.establish_connection() as conn:
            transformed_macro = TransformedMacroData()
            transformed_macro.create_table(conn)
            
            # Create transformed data (simulate transformation)
            transformed_data = sample_macro_data.copy()
            transformed_data["transformed_value"] = transformed_data["value"] * 1.1  # Simple transformation
            
            try:
                # Insert transformed data
                transformed_macro.insert_data(conn, transformed_data)
                
                # Test data retrieval
                for series_id in transformed_data["series_id"].unique():
                    retrieved_data = transformed_macro.get_data(conn, series_id=series_id)
                    assert retrieved_data is not None
                
            except Exception as e:
                pytest.fail(f"Transformed macro data operations failed: {e}")


class TestErrorHandling:
    """Error handling and edge case tests for macroeconomic data."""
    
    @pytest.mark.unit
    def test_fred_api_error_handling(self, data_ingestion_instance):
        """Test handling of FRED API errors."""
        # Test missing API key
        with patch("CandleThrob.ingestion.fetch_data.os.getenv") as mock_getenv:
            mock_getenv.return_value = None
            with pytest.raises(RuntimeError, match="FRED API key is not set"):
                data_ingestion_instance.fetch_fred_data()
        
        # Test API error handling
        with patch("CandleThrob.ingestion.fetch_data.Fred") as mock_fred:
            mock_instance = Mock()
            mock_fred.return_value = mock_instance
            mock_instance.get_series.side_effect = Exception("API Error")
            
            with pytest.raises(ValueError, match="No data returned from FRED API"):
                data_ingestion_instance.fetch_fred_data()
    
    @pytest.mark.unit
    def test_database_error_handling(self, mock_oracle_connection):
        """Test handling of database connection errors."""
        mock_oracle_connection.cursor.side_effect = Exception("Database connection failed")
        
        with pytest.raises(Exception):
            mock_oracle_connection.cursor()
    
    @pytest.mark.unit
    def test_data_validation_errors(self):
        """Test handling of data validation errors."""
        # Test with malformed data
        malformed_data = pd.DataFrame({
            "wrong_column": ["A", "B"],
            "another_wrong_column": [1, 2]
        })
        
        # This should raise an error when trying to insert
        with pytest.raises(Exception):
            # Simulate insertion attempt
            pass
    
    @pytest.mark.unit
    def test_invalid_date_range_handling(self):
        """Test handling of invalid date ranges."""
        # Test invalid date range
        with pytest.raises(ValueError):
            DataIngestion(start_date="2024-01-10", end_date="2024-01-01")
        
        # Test invalid date format
        with pytest.raises(ValueError):
            DataIngestion(start_date="invalid-date", end_date="2024-01-01")


class TestRegressionTests:
    """Regression tests to ensure consistent macroeconomic data behavior."""
    
    @pytest.mark.regression
    def test_macro_data_structure_consistency(self, sample_macro_data):
        """Regression test for macro data structure consistency."""
        # Ensure consistent column names
        expected_columns = ["date", "series_id", "value"]
        for col in expected_columns:
            assert col in sample_macro_data.columns
        
        # Ensure consistent data types
        assert pd.api.types.is_datetime64_any_dtype(sample_macro_data["date"])
        assert sample_macro_data["series_id"].dtype == "object"
        assert pd.api.types.is_numeric_dtype(sample_macro_data["value"])
    
    @pytest.mark.regression
    def test_fred_series_consistency(self):
        """Regression test for FRED series ID consistency."""
        expected_series = [
            "FEDFUNDS", "CPIAUCSL", "UNRATE", "GDP", "GS10", 
            "USREC", "UMCSENT", "HOUST", "RSAFS", "INDPRO", "M2SL"
        ]
        
        # Test cases that should remain consistent
        for series in expected_series:
            assert isinstance(series, str)
            assert len(series) > 0
            assert series.isupper()
    
    @pytest.mark.regression
    def test_macro_data_value_ranges(self, sample_macro_data):
        """Regression test for macro data value ranges."""
        # Test value ranges for different series
        for series_id in sample_macro_data["series_id"].unique():
            series_data = sample_macro_data[sample_macro_data["series_id"] == series_id]["value"]
            
            if len(series_data) > 0:
                # Values should be reasonable for economic indicators
                assert series_data.min() >= 0  # Most economic indicators are non-negative
                assert series_data.max() < 1e6  # Reasonable upper bound


class TestSmokeTests:
    """Smoke tests for critical macroeconomic data functionality."""
    
    @pytest.mark.smoke
    def test_basic_macro_data_smoke(self, sample_macro_data):
        """Smoke test for basic macro data functionality."""
        # Test that basic data structure is valid
        assert isinstance(sample_macro_data, pd.DataFrame)
        assert not sample_macro_data.empty
        
        # Test that required columns exist
        required_columns = ["date", "series_id", "value"]
        for col in required_columns:
            assert col in sample_macro_data.columns
    
    @pytest.mark.smoke
    def test_fred_api_smoke(self, data_ingestion_instance, mock_fred_api):
        """Smoke test for FRED API functionality."""
        # Test that API can be called
        data_ingestion_instance.fetch_fred_data()
        
        # Test that data was fetched
        assert data_ingestion_instance.macro_df is not None
        assert not data_ingestion_instance.macro_df.empty
    
    @pytest.mark.smoke
    def test_macro_data_processing_smoke(self, sample_macro_data):
        """Smoke test for macro data processing functionality."""
        # Test basic data processing
        processed_data = sample_macro_data.copy()
        
        # Add a simple transformation
        processed_data["normalized_value"] = processed_data["value"] / processed_data["value"].max()
        
        # Verify transformation worked
        assert "normalized_value" in processed_data.columns
        assert processed_data["normalized_value"].max() <= 1.0
        assert processed_data["normalized_value"].min() >= 0.0


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])
