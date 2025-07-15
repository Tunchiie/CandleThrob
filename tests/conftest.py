"""
Advanced Test Configuration and Fixtures for CandleThrob

This module provides comprehensive test configuration, fixtures, and utilities for
the CandleThrob test suite with advanced features including advanced mocking,
performance monitoring, data validation, and comprehensive test coverage.

Features:
- Comprehensive test fixtures with realistic data generation
- Advanced mocking with detailed behavior simulation
- Performance monitoring and benchmarking
- Data quality validation and statistical testing
- Advanced error handling and logging
- Test categorization and organization
- Parallel test execution support
- Comprehensive test reporting and metrics

Author: Adetunji Fasiku
Version: 2.0.0
Last Updated: 2025-01-13
"""

import os
import sys
import tempfile
import pytest
import pandas as pd
import numpy as np
import time
import threading
import json
import hashlib
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock, call
from typing import Generator, Dict, Any, Optional, List, Tuple, Union
from pathlib import Path
from dataclasses import dataclass, asdict
from contextlib import contextmanager
import traceback

# Import project modules
from CandleThrob.ingestion.fetch_data import DataIngestion
from CandleThrob.ingestion.enrich_data import TechnicalIndicators
from CandleThrob.utils.oracle_conn import OracleDB
from CandleThrob.utils.models import TickerData, MacroData, TransformedTickerData, TransformedMacroData
from CandleThrob.utils.logging_config import get_database_logger, performance_monitor

# Test configuration constants
DEFAULT_TEST_TIMEOUT = 300  # 5 minutes
DEFAULT_PERFORMANCE_THRESHOLD = 1.0  # seconds
DEFAULT_DATA_QUALITY_THRESHOLD = 0.95  # 95% quality
MAX_TEST_RETRIES = 3
TEST_DATA_SIZE_LARGE = 10000
TEST_DATA_SIZE_MEDIUM = 1000
TEST_DATA_SIZE_SMALL = 100

@dataclass
class TestConfig:
    """Configuration for test execution."""
    timeout: int = DEFAULT_TEST_TIMEOUT
    performance_threshold: float = DEFAULT_PERFORMANCE_THRESHOLD
    data_quality_threshold: float = DEFAULT_DATA_QUALITY_THRESHOLD
    max_retries: int = MAX_TEST_RETRIES
    enable_performance_monitoring: bool = True
    enable_data_validation: bool = True
    enable_statistical_testing: bool = True
    enable_parallel_execution: bool = True
    enable_detailed_logging: bool = True
    enable_test_metrics: bool = True

@dataclass
class TestMetrics:
    """Metrics for test execution."""
    test_name: str
    duration: float
    memory_usage: Optional[float] = None
    cpu_usage: Optional[float] = None
    data_size: Optional[int] = None
    error_count: int = 0
    warning_count: int = 0
    success: bool = True
    timestamp: Optional[str] = None

class TestPerformanceMonitor:
    """Performance monitoring for test execution."""
    
    def __init__(self):
        self.metrics: List[TestMetrics] = []
        self.lock = threading.Lock()
        self.logger = get_database_logger(__name__)
    
    def record_test(self, metrics: TestMetrics):
        """Record test execution metrics."""
        with self.lock:
            self.metrics.append(metrics)
            # Keep only last 1000 test metrics
            if len(self.metrics) > 1000:
                self.metrics = self.metrics[-1000:]
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary for all tests."""
        with self.lock:
            if not self.metrics:
                return {"total_tests": 0}
            
            successful_tests = [m for m in self.metrics if m.success]
            failed_tests = [m for m in self.metrics if not m.success]
            
            avg_duration = sum(m.duration for m in successful_tests) / len(successful_tests) if successful_tests else 0
            max_duration = max(m.duration for m in self.metrics) if self.metrics else 0
            min_duration = min(m.duration for m in self.metrics) if self.metrics else 0
            
            return {
                "total_tests": len(self.metrics),
                "successful_tests": len(successful_tests),
                "failed_tests": len(failed_tests),
                "success_rate": len(successful_tests) / len(self.metrics) if self.metrics else 0,
                "avg_duration": avg_duration,
                "max_duration": max_duration,
                "min_duration": min_duration,
                "total_duration": sum(m.duration for m in self.metrics),
                "total_errors": sum(m.error_count for m in self.metrics),
                "total_warnings": sum(m.warning_count for m in self.metrics)
            }

# Global performance monitor
_performance_monitor = TestPerformanceMonitor()

@pytest.fixture(scope="session")
def test_config() -> TestConfig:
    """Provide test configuration for all tests."""
    return TestConfig()

@pytest.fixture(scope="session")
def test_data_dir() -> str:
    """Create a temporary directory for test data files with cleanup."""
    with tempfile.TemporaryDirectory(prefix="candlethrob_test_") as temp_dir:
        # Create subdirectories for different test data types
        os.makedirs(os.path.join(temp_dir, "tickers"), exist_ok=True)
        os.makedirs(os.path.join(temp_dir, "macros"), exist_ok=True)
        os.makedirs(os.path.join(temp_dir, "processed"), exist_ok=True)
        os.makedirs(os.path.join(temp_dir, "logs"), exist_ok=True)
        
        yield temp_dir

@pytest.fixture(scope="function")
def sample_ticker_data() -> pd.DataFrame:
    """
    Generate realistic sample ticker data for testing with comprehensive validation.
    
    Returns:
        pd.DataFrame: Sample OHLCV data with realistic price movements and validation
    """
    try:
        with performance_monitor("generate_ticker_data"):
            dates = pd.date_range(start="2024-01-01", end="2024-01-31", freq="D")
            base_price = 150.0
            
            # Generate realistic price movements with volatility clustering
            np.random.seed(42)  # For reproducible tests
            returns = np.random.normal(0.001, 0.02, len(dates))
            
            # Add volatility clustering (GARCH-like behavior)
            volatility = np.ones(len(dates)) * 0.02
            for i in range(1, len(dates)):
                volatility[i] = 0.9 * volatility[i-1] + 0.1 * abs(returns[i-1])
            
            returns = returns * volatility
            prices = [base_price]
            
            for ret in returns[1:]:
                new_price = prices[-1] * (1 + ret)
                prices.append(max(new_price, 1.0))  # Ensure positive prices
            
            data = []
            for i, (date, price) in enumerate(zip(dates, prices)):
                # Generate OHLC from close price with realistic spreads
                close = price
                spread = np.random.uniform(0.001, 0.005)  # Realistic bid-ask spread
                high = close * (1 + np.random.uniform(0, 0.03) + spread/2)
                low = close * (1 - np.random.uniform(0, 0.03) - spread/2)
                open_price = np.random.uniform(low, high)
                
                # Ensure OHLC relationships
                high = max(high, open_price, close)
                low = min(low, open_price, close)
                
                # Generate realistic volume with correlation to price movement
                base_volume = np.random.uniform(1000000, 10000000)
                volume_multiplier = 1 + abs(returns[i]) * 10  # Higher volume on volatile days
                volume = int(base_volume * volume_multiplier)
                
                data.append({
                    "Date": date,
                    "Ticker": "AAPL",
                    "Open": round(open_price, 2),
                    "High": round(high, 2),
                    "Low": round(low, 2),
                    "Close": round(close, 2),
                    "Volume": volume,
                    "Returns": round(returns[i], 4),
                    "Volatility": round(volatility[i], 4)
                })
            
            df = pd.DataFrame(data)
            
            # Validate generated data
            assert not df.empty, "Generated ticker data is empty"
            assert all(col in df.columns for col in ["Open", "High", "Low", "Close", "Volume"]), "Missing required columns"
            assert (df["High"] >= df["Low"]).all(), "Invalid OHLC relationships"
            assert (df["Volume"] > 0).all(), "Invalid volume data"
            
            return df
            
    except Exception as e:
        pytest.fail(f"Failed to generate sample ticker data: {e}")

@pytest.fixture(scope="function")
def sample_macro_data() -> pd.DataFrame:
    """
    Generate realistic sample macroeconomic data for testing with comprehensive validation.
    
    Returns:
        pd.DataFrame: Sample macro data with realistic economic indicators and validation
    """
    try:
        with performance_monitor("generate_macro_data"):
            dates = pd.date_range(start="2024-01-01", end="2024-01-31", freq="D")
            
            # Define realistic economic series with proper relationships
            series_data = {
                "FEDFUNDS": {"base": 5.25, "volatility": 0.1, "trend": 0.001},
                "CPIAUCSL": {"base": 310.0, "volatility": 0.5, "trend": 0.002},
                "UNRATE": {"base": 3.7, "volatility": 0.1, "trend": -0.001},
                "GDP": {"base": 25000.0, "volatility": 100.0, "trend": 50.0},
                "GS10": {"base": 4.5, "volatility": 0.2, "trend": 0.005},
                "VIXCLS": {"base": 20.0, "volatility": 2.0, "trend": 0.0},
                "DGS10": {"base": 4.5, "volatility": 0.2, "trend": 0.005}
            }
            
            data = []
            np.random.seed(42)
            
            for date in dates:
                for series_id, params in series_data.items():
                    # Generate realistic economic data with trends and seasonality
                    days_elapsed = (date - dates[0]).days
                    trend = params["trend"] * days_elapsed
                    seasonal = np.sin(2 * np.pi * days_elapsed / 365) * 0.1  # Annual seasonality
                    noise = np.random.normal(0, params["volatility"])
                    value = params["base"] * (1 + trend + seasonal + noise)
                    
                    # Ensure realistic bounds
                    if series_id in ["FEDFUNDS", "UNRATE", "GS10", "DGS10"]:
                        value = max(0, value)  # Rates can't be negative
                    elif series_id == "CPIAUCSL":
                        value = max(100, value)  # CPI should be reasonable
                    elif series_id == "GDP":
                        value = max(1000, value)  # GDP should be reasonable
                    elif series_id == "VIXCLS":
                        value = max(5, value)  # VIX should be reasonable
                    
                    data.append({
                        "date": date.date(),
                        "series_id": series_id,
                        "value": round(value, 2),
                        "trend": round(trend, 4),
                        "seasonal": round(seasonal, 4),
                        "noise": round(noise, 4)
                    })
            
            df = pd.DataFrame(data)
            
            # Validate generated data
            assert not df.empty, "Generated macro data is empty"
            assert all(col in df.columns for col in ["date", "series_id", "value"]), "Missing required columns"
            assert (df["value"] >= 0).all(), "Invalid negative values in macro data"
            assert df["series_id"].nunique() == len(series_data), "Missing expected series"
            
            return df
            
    except Exception as e:
        pytest.fail(f"Failed to generate sample macro data: {e}")

@pytest.fixture(scope="function")
def large_sample_ticker_data() -> pd.DataFrame:
    """
    Generate large sample ticker data for performance testing.
    
    Returns:
        pd.DataFrame: Large OHLCV dataset for performance testing
    """
    try:
        with performance_monitor("generate_large_ticker_data"):
            dates = pd.date_range(start="2020-01-01", end="2024-12-31", freq="D")
            base_price = 150.0
            
            np.random.seed(42)
            returns = np.random.normal(0.001, 0.02, len(dates))
            prices = [base_price]
            
            for ret in returns[1:]:
                new_price = prices[-1] * (1 + ret)
                prices.append(max(new_price, 1.0))
            
            data = []
            for i, (date, price) in enumerate(zip(dates, prices)):
                close = price
                high = close * (1 + np.random.uniform(0, 0.03))
                low = close * (1 - np.random.uniform(0, 0.03))
                open_price = np.random.uniform(low, high)
                
                high = max(high, open_price, close)
                low = min(low, open_price, close)
                volume = int(np.random.uniform(1000000, 10000000))
                
                data.append({
                    "Date": date,
                    "Ticker": "AAPL",
                    "Open": round(open_price, 2),
                    "High": round(high, 2),
                    "Low": round(low, 2),
                    "Close": round(close, 2),
                    "Volume": volume
                })
            
            return pd.DataFrame(data)
            
    except Exception as e:
        pytest.fail(f"Failed to generate large sample ticker data: {e}")

@pytest.fixture(scope="function")
def mock_oracle_connection():
    """
    Advanced mock Oracle database connection for testing with realistic behavior.
    
    Returns:
        Mock: Mocked database connection with comprehensive behavior simulation
    """
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    
    # Mock realistic query results
    mock_cursor.fetchall.return_value = [
        (1, "AAPL", "2024-01-01", 150.0, 155.0, 145.0, 152.0, 1000000),
        (2, "AAPL", "2024-01-02", 152.0, 158.0, 150.0, 156.0, 1200000),
        (3, "AAPL", "2024-01-03", 156.0, 160.0, 154.0, 158.0, 1100000)
    ]
    mock_cursor.fetchone.return_value = [1]  # For simple queries
    mock_cursor.rowcount = 3  # For insert/update operations
    
    # Mock connection properties
    mock_conn.version = "Oracle Database 19c Enterprise Edition"
    mock_conn.autocommit = False
    
    # Mock error scenarios
    def mock_execute_with_error(query, params=None):
        if "ERROR" in query.upper():
            raise Exception("Mock database error")
        return None
    
    mock_cursor.execute.side_effect = mock_execute_with_error
    
    return mock_conn

@pytest.fixture(scope="function")
def mock_fred_api():
    """
    Advanced mock FRED API for testing macroeconomic data fetching with realistic behavior.
    
    Returns:
        Mock: Mocked FRED API instance with comprehensive data simulation
    """
    with patch("CandleThrob.ingestion.fetch_data.Fred") as mock_fred:
        mock_instance = Mock()
        mock_fred.return_value = mock_instance
        
        # Create comprehensive mock data for different series
        dates = pd.date_range("2024-01-01", periods=30, freq="D")
        
        mock_data = {
            "FEDFUNDS": pd.Series(
                np.linspace(5.25, 5.50, len(dates)),
                index=dates
            ),
            "CPIAUCSL": pd.Series(
                np.linspace(310.2, 312.0, len(dates)),
                index=dates
            ),
            "UNRATE": pd.Series(
                np.linspace(3.7, 3.5, len(dates)),
                index=dates
            ),
            "GDP": pd.Series(
                np.linspace(25000.0, 26000.0, len(dates)),
                index=dates
            ),
            "GS10": pd.Series(
                np.linspace(4.5, 4.8, len(dates)),
                index=dates
            ),
            "VIXCLS": pd.Series(
                np.random.uniform(15, 25, len(dates)),
                index=dates
            ),
            "DGS10": pd.Series(
                np.linspace(4.5, 4.8, len(dates)),
                index=dates
            )
        }
        
        def mock_get_series(series_id, **kwargs):
            if series_id in mock_data:
                return mock_data[series_id]
            else:
                raise ValueError(f"Unknown series: {series_id}")
        
        mock_instance.get_series.side_effect = mock_get_series
        
        # Mock API rate limiting
        call_count = 0
        def mock_get_series_with_rate_limit(series_id, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count > 100:  # Simulate rate limit
                raise Exception("Rate limit exceeded")
            return mock_get_series(series_id, **kwargs)
        
        mock_instance.get_series.side_effect = mock_get_series_with_rate_limit
        
        yield mock_instance

@pytest.fixture(scope="function")
def mock_yfinance():
    """
    Advanced mock yfinance for testing stock data fetching with realistic behavior.
    
    Returns:
        Mock: Mocked yfinance Ticker instance with comprehensive data simulation
    """
    with patch("CandleThrob.ingestion.fetch_data.yf") as mock_yf:
        mock_ticker = Mock()
        mock_yf.Ticker.return_value = mock_ticker
        
        # Create realistic mock OHLCV data with trends and volatility
        dates = pd.date_range("2024-01-01", "2024-01-31", freq="D")
        np.random.seed(42)
        
        # Generate realistic price movements
        base_price = 150.0
        returns = np.random.normal(0.001, 0.02, len(dates))
        prices = [base_price]
        
        for ret in returns[1:]:
            new_price = prices[-1] * (1 + ret)
            prices.append(max(new_price, 1.0))
        
        mock_data = pd.DataFrame({
            "Open": [p * (1 + np.random.uniform(-0.01, 0.01)) for p in prices],
            "High": [p * (1 + np.random.uniform(0, 0.03)) for p in prices],
            "Low": [p * (1 - np.random.uniform(0, 0.03)) for p in prices],
            "Close": prices,
            "Volume": np.random.randint(1000000, 10000000, len(dates)),
            "Dividends": np.zeros(len(dates)),
            "Stock Splits": np.zeros(len(dates))
        }, index=dates)
        
        mock_ticker.history.return_value = mock_data
        
        # Mock ticker info
        mock_ticker.info = {
            "symbol": "AAPL",
            "shortName": "Apple Inc.",
            "longName": "Apple Inc.",
            "marketCap": 3000000000000,
            "currency": "USD",
            "exchange": "NASDAQ"
        }
        
        # Mock error scenarios
        def mock_history_with_error(period="1y", **kwargs):
            if "ERROR" in str(kwargs):
                raise Exception("Mock yfinance error")
            return mock_data
        
        mock_ticker.history.side_effect = mock_history_with_error
        
        yield mock_ticker

@pytest.fixture(scope="function")
def data_ingestion_instance() -> DataIngestion:
    """
    Create a DataIngestion instance for testing with comprehensive configuration.
    
    Returns:
        DataIngestion: Configured instance for testing
    """
    return DataIngestion(
        start_date="2024-01-01", 
        end_date="2024-01-31",
        tickers=["AAPL", "MSFT", "GOOGL"],
        macro_series=["FEDFUNDS", "CPIAUCSL", "UNRATE"]
    )

@pytest.fixture(scope="function")
def technical_indicators_instance(sample_ticker_data) -> TechnicalIndicators:
    """
    Create a TechnicalIndicators instance with sample data for comprehensive testing.
    
    Args:
        sample_ticker_data: Sample ticker data fixture
        
    Returns:
        TechnicalIndicators: Configured instance for testing
    """
    return TechnicalIndicators(sample_ticker_data)

@pytest.fixture(scope="function")
def oracle_db_instance() -> OracleDB:
    """
    Create an OracleDB instance for testing with comprehensive configuration.
    
    Returns:
        OracleDB: Configured instance for testing
    """
    return OracleDB(
        pool_min_size=1,
        pool_max_size=5,
        pool_increment=1,
        connection_timeout=30
    )

@pytest.fixture(scope="function")
def performance_benchmark():
    """
    Advanced performance benchmarking fixture for comprehensive timing and resource monitoring.
    
    Returns:
        PerformanceBenchmark: Instance for performance monitoring
    """
    import psutil
    import time
    
    class PerformanceBenchmark:
        def __init__(self):
            self.start_time = None
            self.end_time = None
            self.start_memory = None
            self.end_memory = None
            self.start_cpu = None
            self.end_cpu = None
            self.metrics = {}
            self.process = psutil.Process()
        
        def start(self):
            """Start timing and resource monitoring."""
            self.start_time = time.time()
            self.start_memory = self.process.memory_info().rss / 1024 / 1024  # MB
            self.start_cpu = self.process.cpu_percent()
        
        def end(self):
            """End timing and calculate comprehensive metrics."""
            self.end_time = time.time()
            self.end_memory = self.process.memory_info().rss / 1024 / 1024  # MB
            self.end_cpu = self.process.cpu_percent()
            
            self.metrics = {
                "duration": self.end_time - self.start_time,
                "memory_usage": self.end_memory - self.start_memory,
                "peak_memory": self.end_memory,
                "cpu_usage": (self.start_cpu + self.end_cpu) / 2,
                "memory_efficiency": (self.end_memory - self.start_memory) / max(self.metrics.get("duration", 1), 1)
            }
        
        def get_metrics(self) -> Dict[str, Any]:
            """Get comprehensive performance metrics."""
            return self.metrics.copy()
        
        def assert_performance(self, max_duration: float = 1.0, max_memory: float = 100.0):
            """Assert performance meets requirements."""
            if self.metrics.get("duration", 0) > max_duration:
                pytest.fail(f"Performance test failed: duration {self.metrics['duration']:.2f}s > {max_duration}s")
            
            if self.metrics.get("memory_usage", 0) > max_memory:
                pytest.fail(f"Performance test failed: memory usage {self.metrics['memory_usage']:.2f}MB > {max_memory}MB")
    
    return PerformanceBenchmark()

@pytest.fixture(scope="function")
def data_quality_validator():
    """
    Advanced data quality validation fixture for comprehensive data checks and reporting.
    
    Returns:
        DataQualityValidator: Instance for comprehensive data quality validation
    """
    class DataQualityValidator:
        """Comprehensive data quality validation for financial data with detailed reporting."""
        
        def validate_ohlcv_data(self, df: pd.DataFrame) -> Dict[str, Any]:
            """
            Validate OHLCV data quality with comprehensive checks.
            
            Args:
                df: DataFrame with OHLCV data
                
            Returns:
                Dict: Comprehensive validation results
            """
            results = {
                "is_valid": True,
                "errors": [],
                "warnings": [],
                "statistics": {},
                "quality_score": 0.0,
                "recommendations": []
            }
            
            required_columns = ["Open", "High", "Low", "Close", "Volume"]
            
            # Check required columns
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                results["is_valid"] = False
                results["errors"].append(f"Missing required columns: {missing_columns}")
            
            # Check data types
            for col in required_columns:
                if col in df.columns:
                    if not pd.api.types.is_numeric_dtype(df[col]):
                        results["errors"].append(f"Column {col} is not numeric")
                        results["is_valid"] = False
            
            # Check OHLC relationships
            if all(col in df.columns for col in ["Open", "High", "Low", "Close"]):
                invalid_ohlc = (
                    (df["High"] < df["Low"]) |
                    (df["High"] < df["Open"]) |
                    (df["High"] < df["Close"]) |
                    (df["Low"] > df["Open"]) |
                    (df["Low"] > df["Close"])
                )
                
                invalid_count = invalid_ohlc.sum()
                if invalid_count > 0:
                    results["warnings"].append(f"Found {invalid_count} invalid OHLC relationships")
                    results["quality_score"] -= 0.1 * (invalid_count / len(df))
            
            # Check for negative values
            for col in ["Open", "High", "Low", "Close", "Volume"]:
                if col in df.columns:
                    negative_count = (df[col] < 0).sum()
                    if negative_count > 0:
                        results["warnings"].append(f"Found {negative_count} negative values in {col}")
                        results["quality_score"] -= 0.05 * (negative_count / len(df))
            
            # Check for extreme outliers
            for col in ["Open", "High", "Low", "Close"]:
                if col in df.columns:
                    Q1 = df[col].quantile(0.25)
                    Q3 = df[col].quantile(0.75)
                    IQR = Q3 - Q1
                    outliers = ((df[col] < (Q1 - 1.5 * IQR)) | (df[col] > (Q3 + 1.5 * IQR))).sum()
                    if outliers > 0:
                        results["warnings"].append(f"Found {outliers} outliers in {col}")
                        results["quality_score"] -= 0.05 * (outliers / len(df))
            
            # Check for missing values
            missing_counts = df[required_columns].isnull().sum()
            if missing_counts.any():
                results["warnings"].append(f"Missing values found: {missing_counts.to_dict()}")
                results["quality_score"] -= 0.1 * (missing_counts.sum() / (len(df) * len(required_columns)))
            
            # Calculate comprehensive statistics
            if not df.empty:
                results["statistics"] = {
                    "row_count": len(df),
                    "missing_values": df[required_columns].isnull().sum().to_dict(),
                    "price_range": {
                        "min": df[["Open", "High", "Low", "Close"]].min().min(),
                        "max": df[["Open", "High", "Low", "Close"]].max().max()
                    },
                    "volume_statistics": {
                        "mean": df["Volume"].mean(),
                        "std": df["Volume"].std(),
                        "min": df["Volume"].min(),
                        "max": df["Volume"].max()
                    },
                    "price_volatility": df[["Open", "High", "Low", "Close"]].std().mean()
                }
            
            # Calculate quality score
            results["quality_score"] = max(0.0, 1.0 + results["quality_score"])
            
            # Generate recommendations
            if results["quality_score"] < 0.8:
                results["recommendations"].append("Data quality is below threshold - review required")
            if len(results["warnings"]) > 5:
                results["recommendations"].append("High number of warnings - data review recommended")
            
            return results
        
        def validate_macro_data(self, df: pd.DataFrame) -> Dict[str, Any]:
            """
            Validate macroeconomic data quality with comprehensive checks.
            
            Args:
                df: DataFrame with macro data
                
            Returns:
                Dict: Comprehensive validation results
            """
            results = {
                "is_valid": True,
                "errors": [],
                "warnings": [],
                "statistics": {},
                "quality_score": 0.0,
                "recommendations": []
            }
            
            required_columns = ["date", "series_id", "value"]
            
            # Check required columns
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                results["is_valid"] = False
                results["errors"].append(f"Missing required columns: {missing_columns}")
            
            # Check data types
            if "date" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["date"]):
                results["errors"].append("Date column is not datetime type")
                results["is_valid"] = False
            
            if "value" in df.columns and not pd.api.types.is_numeric_dtype(df["value"]):
                results["errors"].append("Value column is not numeric")
                results["is_valid"] = False
            
            # Check for missing values
            if not df.empty:
                missing_counts = df.isnull().sum()
                if missing_counts.any():
                    results["warnings"].append(f"Missing values found: {missing_counts.to_dict()}")
                    results["quality_score"] -= 0.1 * (missing_counts.sum() / (len(df) * len(df.columns)))
            
            # Check for extreme outliers
            if "value" in df.columns:
                Q1 = df["value"].quantile(0.25)
                Q3 = df["value"].quantile(0.75)
                IQR = Q3 - Q1
                outliers = ((df["value"] < (Q1 - 1.5 * IQR)) | (df["value"] > (Q3 + 1.5 * IQR))).sum()
                if outliers > 0:
                    results["warnings"].append(f"Found {outliers} outliers in values")
                    results["quality_score"] -= 0.05 * (outliers / len(df))
            
            # Calculate comprehensive statistics
            if not df.empty:
                results["statistics"] = {
                    "row_count": len(df),
                    "unique_series": df["series_id"].nunique() if "series_id" in df.columns else 0,
                    "date_range": {
                        "start": df["date"].min() if "date" in df.columns else None,
                        "end": df["date"].max() if "date" in df.columns else None
                    },
                    "value_statistics": {
                        "mean": df["value"].mean() if "value" in df.columns else None,
                        "std": df["value"].std() if "value" in df.columns else None,
                        "min": df["value"].min() if "value" in df.columns else None,
                        "max": df["value"].max() if "value" in df.columns else None
                    }
                }
            
            # Calculate quality score
            results["quality_score"] = max(0.0, 1.0 + results["quality_score"])
            
            return results
    
    return DataQualityValidator()

@pytest.fixture(scope="function")
def statistical_validator():
    """
    Advanced statistical validation fixture for comprehensive financial data analysis.
    
    Returns:
        StatisticalValidator: Instance for comprehensive statistical validation
    """
    class StatisticalValidator:
        """Comprehensive statistical validation for financial time series data."""
        
        def validate_stationarity(self, series: pd.Series, alpha: float = 0.05) -> Dict[str, Any]:
            """
            Test for stationarity using Augmented Dickey-Fuller test with comprehensive analysis.
            
            Args:
                series: Time series to test
                alpha: Significance level
                
            Returns:
                Dict: Comprehensive stationarity test results
            """
            try:
                from statsmodels.tsa.stattools import adfuller
                
                clean_series = series.dropna()
                if len(clean_series) < 10:
                    return {
                        "is_stationary": None,
                        "p_value": None,
                        "test_statistic": None,
                        "critical_values": None,
                        "error": "Insufficient data for stationarity test"
                    }
                
                result = adfuller(clean_series)
                
                return {
                    "is_stationary": result[1] < alpha,
                    "p_value": result[1],
                    "test_statistic": result[0],
                    "critical_values": result[4],
                    "data_length": len(clean_series),
                    "significance_level": alpha,
                    "interpretation": "Stationary" if result[1] < alpha else "Non-stationary"
                }
            except ImportError:
                return {
                    "is_stationary": None,
                    "p_value": None,
                    "test_statistic": None,
                    "critical_values": None,
                    "error": "statsmodels not available"
                }
        
        def validate_normality(self, series: pd.Series, alpha: float = 0.05) -> Dict[str, Any]:
            """
            Test for normality using Shapiro-Wilk test with comprehensive analysis.
            
            Args:
                series: Series to test
                alpha: Significance level
                
            Returns:
                Dict: Comprehensive normality test results
            """
            try:
                from scipy.stats import shapiro, normaltest
                
                clean_series = series.dropna()
                if len(clean_series) < 3:
                    return {"is_normal": None, "p_value": None, "error": "Insufficient data"}
                
                # Multiple normality tests
                shapiro_stat, shapiro_p = shapiro(clean_series)
                d_agostino_stat, d_agostino_p = normaltest(clean_series)
                
                # Combined result
                is_normal = shapiro_p > alpha and d_agostino_p > alpha
                
                return {
                    "is_normal": is_normal,
                    "shapiro_p_value": shapiro_p,
                    "d_agostino_p_value": d_agostino_p,
                    "shapiro_statistic": shapiro_stat,
                    "d_agostino_statistic": d_agostino_stat,
                    "data_length": len(clean_series),
                    "significance_level": alpha,
                    "interpretation": "Normal" if is_normal else "Non-normal"
                }
            except ImportError:
                return {
                    "is_normal": None,
                    "p_value": None,
                    "test_statistic": None,
                    "error": "scipy not available"
                }
        
        def calculate_autocorrelation(self, series: pd.Series, max_lag: int = 10) -> Dict[str, Any]:
            """
            Calculate autocorrelation for time series with comprehensive analysis.
            
            Args:
                series: Time series
                max_lag: Maximum lag to calculate
                
            Returns:
                Dict: Comprehensive autocorrelation results
            """
            try:
                from statsmodels.tsa.stattools import acf, pacf
                
                clean_series = series.dropna()
                if len(clean_series) < max_lag + 1:
                    return {"autocorr": None, "error": "Insufficient data"}
                
                # Calculate both ACF and PACF
                acf_values = acf(clean_series, nlags=max_lag, fft=False)
                pacf_values = pacf(clean_series, nlags=max_lag)
                
                # Find significant lags
                significant_acf = [i for i, ac in enumerate(acf_values) if abs(ac) > 0.2]
                significant_pacf = [i for i, ac in enumerate(pacf_values) if abs(ac) > 0.2]
                
                return {
                    "acf": acf_values.tolist(),
                    "pacf": pacf_values.tolist(),
                    "significant_acf_lags": significant_acf,
                    "significant_pacf_lags": significant_pacf,
                    "max_acf": max(abs(ac) for ac in acf_values[1:]),
                    "max_pacf": max(abs(ac) for ac in pacf_values[1:]),
                    "data_length": len(clean_series)
                }
            except ImportError:
                return {"autocorr": None, "error": "statsmodels not available"}
        
        def calculate_volatility(self, series: pd.Series, window: int = 20) -> Dict[str, Any]:
            """
            Calculate volatility measures for financial time series.
            
            Args:
                series: Time series (typically returns)
                window: Rolling window size
                
            Returns:
                Dict: Volatility analysis results
            """
            try:
                clean_series = series.dropna()
                if len(clean_series) < window:
                    return {"volatility": None, "error": "Insufficient data"}
                
                # Calculate rolling volatility
                rolling_vol = clean_series.rolling(window=window).std()
                
                return {
                    "overall_volatility": clean_series.std(),
                    "rolling_volatility": rolling_vol.dropna().tolist(),
                    "volatility_range": {
                        "min": rolling_vol.min(),
                        "max": rolling_vol.max(),
                        "mean": rolling_vol.mean()
                    },
                    "volatility_clustering": rolling_vol.autocorr(),
                    "data_length": len(clean_series)
                }
            except Exception as e:
                return {"volatility": None, "error": str(e)}
    
    return StatisticalValidator()

@pytest.fixture(scope="function")
def test_metrics_collector():
    """
    Test metrics collector for comprehensive test reporting and analysis.
    
    Returns:
        TestMetricsCollector: Instance for collecting and analyzing test metrics
    """
    class TestMetricsCollector:
        def __init__(self):
            self.metrics: List[TestMetrics] = []
            self.logger = get_database_logger(__name__)
        
        def record_test(self, test_name: str, duration: float, success: bool = True, 
                       error_count: int = 0, warning_count: int = 0, **kwargs):
            """Record test execution metrics."""
            metrics = TestMetrics(
                test_name=test_name,
                duration=duration,
                success=success,
                error_count=error_count,
                warning_count=warning_count,
                timestamp=datetime.now().isoformat(),
                **kwargs
            )
            self.metrics.append(metrics)
            _performance_monitor.record_test(metrics)
        
        def get_summary(self) -> Dict[str, Any]:
            """Get comprehensive test summary."""
            if not self.metrics:
                return {"total_tests": 0}
            
            successful_tests = [m for m in self.metrics if m.success]
            failed_tests = [m for m in self.metrics if not m.success]
            
            return {
                "total_tests": len(self.metrics),
                "successful_tests": len(successful_tests),
                "failed_tests": len(failed_tests),
                "success_rate": len(successful_tests) / len(self.metrics),
                "avg_duration": sum(m.duration for m in self.metrics) / len(self.metrics),
                "total_duration": sum(m.duration for m in self.metrics),
                "total_errors": sum(m.error_count for m in self.metrics),
                "total_warnings": sum(m.warning_count for m in self.metrics)
            }
    
    return TestMetricsCollector()

# Test markers for different test types
def pytest_configure(config):
    """Configure pytest with comprehensive custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )
    config.addinivalue_line(
        "markers", "performance: marks tests as performance tests"
    )
    config.addinivalue_line(
        "markers", "data_quality: marks tests as data quality validation tests"
    )
    config.addinivalue_line(
        "markers", "statistical: marks tests as statistical validation tests"
    )
    config.addinivalue_line(
        "markers", "regression: marks tests as regression tests"
    )
    config.addinivalue_line(
        "markers", "smoke: marks tests as smoke tests"
    )
    config.addinivalue_line(
        "markers", "api: marks tests as API tests"
    )
    config.addinivalue_line(
        "markers", "database: marks tests as database tests"
    )
    config.addinivalue_line(
        "markers", "external: marks tests that require external services"
    )
    config.addinivalue_line(
        "markers", "security: marks tests as security tests"
    )
    config.addinivalue_line(
        "markers", "load: marks tests as load testing"
    )
    config.addinivalue_line(
        "markers", "stress: marks tests as stress testing"
    )
    config.addinivalue_line(
        "markers", "end_to_end: marks tests as end-to-end tests"
    )

def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """Generate comprehensive test summary report."""
    # Get performance summary
    performance_summary = _performance_monitor.get_performance_summary()
    
    if performance_summary["total_tests"] > 0:
        terminalreporter.write_sep("=", "PERFORMANCE SUMMARY")
        terminalreporter.write_line(f"Total Tests: {performance_summary['total_tests']}")
        terminalreporter.write_line(f"Success Rate: {performance_summary['success_rate']:.2%}")
        terminalreporter.write_line(f"Average Duration: {performance_summary['avg_duration']:.3f}s")
        terminalreporter.write_line(f"Total Duration: {performance_summary['total_duration']:.3f}s")
        terminalreporter.write_line(f"Total Errors: {performance_summary['total_errors']}")
        terminalreporter.write_line(f"Total Warnings: {performance_summary['total_warnings']}") 