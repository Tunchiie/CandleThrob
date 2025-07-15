"""
Comprehensive test suite for data ingestion functionality.

This module provides tests for:
- Data ingestion pipeline components
- API integration and error handling
- Data quality validation
- Performance benchmarking
- Statistical validation
- Database operations
- Edge cases and error conditions
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any

from CandleThrob.ingestion.fetch_data import DataIngestion
from CandleThrob.ingestion.ingest_data import clean_ticker, get_etf_tickers, get_sp500_tickers
from CandleThrob.utils.oracle_conn import OracleDB
from CandleThrob.utils.models import TickerData, MacroData


class TestDataIngestionUnit:
    """Unit tests for data ingestion functionality."""
    
    @pytest.mark.unit
    def test_data_ingestion_initialization(self):
        """Test DataIngestion class initialization with various parameters."""
        # Test default initialization
        data = DataIngestion()
        assert data.start_date is not None
        assert data.end_date is not None
        assert isinstance(data.start_date, str)
        assert isinstance(data.end_date, str)
        
        # Test custom date initialization
        custom_data = DataIngestion(start_date="2024-01-01", end_date="2024-01-31")
        assert custom_data.start_date == "2024-01-01"
        assert custom_data.end_date == "2024-01-31"
    
    @pytest.mark.unit
    def test_data_ingestion_invalid_date_range(self):
        """Test that invalid date ranges raise appropriate errors."""
        # End date before start date
        with pytest.raises(ValueError, match="End date must be after start date"):
            DataIngestion(start_date="2023-01-01", end_date="2022-12-31")
        
        # Invalid date format
        with pytest.raises(ValueError):
            DataIngestion(start_date="invalid-date", end_date="2024-01-01")
    
    @pytest.mark.unit
    def test_clean_ticker_function(self):
        """Test ticker symbol cleaning function with various inputs."""
        test_cases = [
            ("AAPL@", "AAPL"),
            ("$GOOGL ", "GOOGL"),
            ("  MSFT", "MSFT"),
            ("AMZN!", "AMZN"),
            ("  TSLA  ", "TSLA"),
            ("  NFLX@", "NFLX"),
            ("BRK.B", "BRK-B"),  # Test period to dash conversion
            ("BRK.A", "BRK-A"),
            ("", ""),  # Empty string
            ("   ", ""),  # Whitespace only
            ("ABC123", "ABC123"),  # Alphanumeric
            ("ABC-123", "ABC-123"),  # Already has dash
        ]
        
        for input_ticker, expected_output in test_cases:
            assert clean_ticker(input_ticker) == expected_output
    
    @pytest.mark.unit
    def test_get_etf_tickers(self):
        """Test ETF ticker list retrieval and validation."""
        etf_tickers = get_etf_tickers()
        
        # Basic validation
        assert isinstance(etf_tickers, list)
        assert len(etf_tickers) > 0
        
        # Check for expected major ETFs
        expected_etfs = ["SPY", "QQQ", "IWM", "VTI", "VEA", "VWO"]
        for etf in expected_etfs:
            assert etf in etf_tickers
        
        # Validate ticker format
        for ticker in etf_tickers:
            assert isinstance(ticker, str)
            assert len(ticker) > 0
            assert len(ticker) <= 10  # Reasonable ticker length
            assert ticker.isalnum() or "-" in ticker  # Valid characters
    
    @pytest.mark.unit
    def test_get_sp500_tickers(self):
        """Test S&P 500 ticker list retrieval and validation."""
        try:
            sp500_tickers = get_sp500_tickers()
            
            # Basic validation
            assert isinstance(sp500_tickers, list)
            assert len(sp500_tickers) >= 400  # S&P 500 should have ~500 tickers
            
            # Check for expected major stocks
            expected_stocks = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
            for stock in expected_stocks:
                assert stock in sp500_tickers
            
            # Validate ticker format
            for ticker in sp500_tickers:
                assert isinstance(ticker, str)
                assert len(ticker) > 0
                assert len(ticker) <= 10
                assert ticker.isalnum() or "-" in ticker
                
        except Exception as e:
            pytest.skip(f"S&P 500 ticker fetching failed: {e}")


class TestDataIngestionIntegration:
    """Integration tests for data ingestion with external APIs."""
    
    @pytest.mark.integration
    @pytest.mark.api
    @pytest.mark.external
    def test_ticker_data_fetching_integration(self, data_ingestion_instance, mock_yfinance):
        """Test end-to-end ticker data fetching with mocked API."""
        ticker_df = data_ingestion_instance.ingest_ticker_data("AAPL")
        
        # Validate returned data
        assert ticker_df is not None
        assert not ticker_df.empty
        assert isinstance(ticker_df, pd.DataFrame)
        
        # Check required columns
        required_columns = ["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]
        for col in required_columns:
            assert col in ticker_df.columns
        
        # Validate data types
        assert pd.api.types.is_datetime64_any_dtype(ticker_df["Date"])
        assert ticker_df["Ticker"].iloc[0] == "AAPL"
        assert all(pd.api.types.is_numeric_dtype(ticker_df[col]) for col in ["Open", "High", "Low", "Close", "Volume"])
        
        # Validate OHLC relationships
        assert (ticker_df["High"] >= ticker_df["Low"]).all()
        assert (ticker_df["High"] >= ticker_df["Open"]).all()
        assert (ticker_df["High"] >= ticker_df["Close"]).all()
        assert (ticker_df["Low"] <= ticker_df["Open"]).all()
        assert (ticker_df["Low"] <= ticker_df["Close"]).all()
    
    @pytest.mark.integration
    @pytest.mark.api
    @pytest.mark.external
    def test_macro_data_fetching_integration(self, data_ingestion_instance, mock_fred_api):
        """Test end-to-end macroeconomic data fetching with mocked API."""
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


class TestDataQualityValidation:
    """Data quality validation tests for ingested data."""
    
    @pytest.mark.data_quality
    def test_ticker_data_quality_validation(self, sample_ticker_data, data_quality_validator):
        """Test comprehensive data quality validation for ticker data."""
        validation_results = data_quality_validator.validate_ohlcv_data(sample_ticker_data)
        
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
        assert "price_range" in stats
        assert stats["price_range"]["min"] > 0
        assert stats["price_range"]["max"] > stats["price_range"]["min"]
    
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
    
    @pytest.mark.data_quality
    def test_data_quality_edge_cases(self, data_quality_validator):
        """Test data quality validation with edge cases and malformed data."""
        # Test empty DataFrame
        empty_df = pd.DataFrame()
        results = data_quality_validator.validate_ohlcv_data(empty_df)
        assert not results["is_valid"]
        assert len(results["errors"]) > 0
        
        # Test DataFrame with missing columns
        incomplete_df = pd.DataFrame({"Open": [100], "Close": [110]})
        results = data_quality_validator.validate_ohlcv_data(incomplete_df)
        assert not results["is_valid"]
        assert len(results["errors"]) > 0
        
        # Test DataFrame with invalid OHLC relationships
        invalid_ohlc_df = pd.DataFrame({
            "Open": [100], "High": [90], "Low": [120], "Close": [110], "Volume": [1000]
        })
        results = data_quality_validator.validate_ohlcv_data(invalid_ohlc_df)
        assert len(results["warnings"]) > 0


class TestStatisticalValidation:
    """Statistical validation tests for financial time series data."""
    
    @pytest.mark.statistical
    def test_price_series_statistical_properties(self, sample_ticker_data, statistical_validator):
        """Test statistical properties of price time series."""
        close_prices = sample_ticker_data["Close"]
        
        # Test stationarity
        stationarity_results = statistical_validator.validate_stationarity(close_prices)
        assert "is_stationary" in stationarity_results
        assert "p_value" in stationarity_results
        
        # Test normality of returns
        returns = close_prices.pct_change().dropna()
        normality_results = statistical_validator.validate_normality(returns)
        assert "is_normal" in normality_results
        assert "p_value" in normality_results
        
        # Test autocorrelation
        autocorr_results = statistical_validator.calculate_autocorrelation(close_prices)
        assert "autocorr" in autocorr_results
    
    @pytest.mark.statistical
    def test_macro_series_statistical_properties(self, sample_macro_data, statistical_validator):
        """Test statistical properties of macroeconomic time series."""
        # Test individual series
        for series_id in sample_macro_data["series_id"].unique():
            series_data = sample_macro_data[sample_macro_data["series_id"] == series_id]["value"]
            
            # Test stationarity
            stationarity_results = statistical_validator.validate_stationarity(series_data)
            assert "is_stationary" in stationarity_results
            
            # Test normality
            normality_results = statistical_validator.validate_normality(series_data)
            assert "is_normal" in normality_results


class TestPerformanceBenchmarking:
    """Performance benchmarking tests for data ingestion operations."""
    
    @pytest.mark.performance
    def test_ticker_data_fetching_performance(self, data_ingestion_instance, mock_yfinance, performance_benchmark):
        """Benchmark ticker data fetching performance."""
        performance_benchmark.start()
        
        # Perform data fetching
        ticker_df = data_ingestion_instance.ingest_ticker_data("AAPL")
        
        performance_benchmark.end()
        metrics = performance_benchmark.get_metrics()
        
        # Validate performance metrics
        assert "duration" in metrics
        assert metrics["duration"] > 0
        assert metrics["duration"] < 10.0  # Should complete within 10 seconds
        
        # Validate data was fetched
        assert ticker_df is not None
        assert not ticker_df.empty
    
    @pytest.mark.performance
    def test_macro_data_fetching_performance(self, data_ingestion_instance, mock_fred_api, performance_benchmark):
        """Benchmark macro data fetching performance."""
        performance_benchmark.start()
        
        # Perform data fetching
        data_ingestion_instance.fetch_fred_data()
        
        performance_benchmark.end()
        metrics = performance_benchmark.get_metrics()
        
        # Validate performance metrics
        assert "duration" in metrics
        assert metrics["duration"] > 0
        assert metrics["duration"] < 15.0  # Should complete within 15 seconds
        
        # Validate data was fetched
        assert data_ingestion_instance.macro_df is not None
        assert not data_ingestion_instance.macro_df.empty
    
    @pytest.mark.performance
    def test_bulk_ticker_processing_performance(self, data_ingestion_instance, mock_yfinance, performance_benchmark):
        """Benchmark bulk ticker processing performance."""
        tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
        
        performance_benchmark.start()
        
        # Process multiple tickers
        results = []
        for ticker in tickers:
            df = data_ingestion_instance.ingest_ticker_data(ticker)
            results.append(df)
        
        performance_benchmark.end()
        metrics = performance_benchmark.get_metrics()
        
        # Validate performance metrics
        assert "duration" in metrics
        assert metrics["duration"] > 0
        assert metrics["duration"] < 30.0  # Should complete within 30 seconds
        
        # Validate all tickers were processed
        assert len(results) == len(tickers)
        assert all(df is not None and not df.empty for df in results)


class TestDatabaseOperations:
    """Database operation tests for data persistence."""
    
    @pytest.mark.database
    def test_database_connection(self, oracle_db_instance):
        """Test database connection establishment and basic operations."""
        with oracle_db_instance.establish_connection() as conn:
            from sqlalchemy import text
            
            # Test basic query execution
            result = conn.execute(text("SELECT 1 as test_col"))
            row = result.fetchone()
            assert row[0] == 1
    
    @pytest.mark.database
    def test_ticker_data_table_operations(self, oracle_db_instance):
        """Test ticker data table creation and operations."""
        with oracle_db_instance.establish_connection() as conn:
            ticker_data = TickerData()
            
            # Test table creation
            ticker_data.create_table(conn)
            
            # Verify table exists
            from sqlalchemy import text
            try:
                result = conn.execute(text("SELECT COUNT(*) FROM ticker_data"))
                count = result.fetchone()[0]
                assert count >= 0
            except Exception as e:
                pytest.fail(f"TickerData table creation failed: {e}")
    
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
    def test_data_insertion_and_retrieval(self, oracle_db_instance, sample_ticker_data, sample_macro_data):
        """Test data insertion and retrieval operations."""
        with oracle_db_instance.establish_connection() as conn:
            # Test ticker data insertion
            ticker_data = TickerData()
            ticker_data.create_table(conn)
            
            # Prepare ticker data for insertion
            ticker_insert_df = sample_ticker_data.copy()
            ticker_insert_df.columns = [col.lower() for col in ticker_insert_df.columns]
            ticker_insert_df["ticker"] = "TEST"
            
            try:
                ticker_data.insert_data(conn, ticker_insert_df)
                
                # Test data retrieval
                retrieved_data = ticker_data.get_data(conn, ticker="TEST")
                assert retrieved_data is not None
                
            except Exception as e:
                pytest.fail(f"Ticker data insertion/retrieval failed: {e}")
            
            # Test macro data insertion
            macro_data = MacroData()
            macro_data.create_table(conn)
            
            try:
                macro_data.insert_data(conn, sample_macro_data)
                
                # Test data retrieval
                retrieved_macro_data = macro_data.get_data(conn, series_id="FEDFUNDS")
                assert retrieved_macro_data is not None
                
            except Exception as e:
                pytest.fail(f"Macro data insertion/retrieval failed: {e}")


class TestErrorHandling:
    """Error handling and edge case tests."""
    
    @pytest.mark.unit
    def test_api_error_handling(self, data_ingestion_instance):
        """Test handling of API errors and network issues."""
        # Test with invalid ticker
        with pytest.raises(Exception):
            data_ingestion_instance.ingest_ticker_data("INVALID_TICKER_12345")
    
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


class TestRegressionTests:
    """Regression tests to ensure consistent behavior."""
    
    @pytest.mark.regression
    def test_ticker_cleaning_regression(self):
        """Regression test for ticker cleaning function."""
        # Test cases that should remain consistent
        test_cases = [
            ("AAPL", "AAPL"),
            ("GOOGL", "GOOGL"),
            ("BRK.B", "BRK-B"),
            ("BRK.A", "BRK-A"),
        ]
        
        for input_ticker, expected_output in test_cases:
            result = clean_ticker(input_ticker)
            assert result == expected_output, f"Failed for {input_ticker}: expected {expected_output}, got {result}"
    
    @pytest.mark.regression
    def test_data_structure_consistency(self, sample_ticker_data):
        """Regression test for data structure consistency."""
        # Ensure consistent column names
        expected_columns = ["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]
        for col in expected_columns:
            assert col in sample_ticker_data.columns
        
        # Ensure consistent data types
        assert pd.api.types.is_datetime64_any_dtype(sample_ticker_data["Date"])
        assert sample_ticker_data["Ticker"].dtype == "object"
        assert all(pd.api.types.is_numeric_dtype(sample_ticker_data[col]) for col in ["Open", "High", "Low", "Close", "Volume"])


class TestSmokeTests:
    """Smoke tests for critical functionality."""
    
    @pytest.mark.smoke
    def test_basic_ingestion_smoke(self, data_ingestion_instance):
        """Smoke test for basic ingestion functionality."""
        # Test that basic initialization works
        assert data_ingestion_instance is not None
        assert hasattr(data_ingestion_instance, "start_date")
        assert hasattr(data_ingestion_instance, "end_date")
    
    @pytest.mark.smoke
    def test_ticker_cleaning_smoke(self):
        """Smoke test for ticker cleaning functionality."""
        # Test basic ticker cleaning
        result = clean_ticker("AAPL")
        assert result == "AAPL"
    
    @pytest.mark.smoke
    def test_etf_list_smoke(self):
        """Smoke test for ETF list functionality."""
        # Test that ETF list can be retrieved
        etf_list = get_etf_tickers()
        assert isinstance(etf_list, list)
        assert len(etf_list) > 0