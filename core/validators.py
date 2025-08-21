"""
Centralized Data Validators for CandleThrob
==========================================

This module consolidates all data validation functionality across the system,
replacing duplicate validator classes with unified implementations.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Tuple, Union
from datetime import datetime, timedelta
from dataclasses import dataclass

from .base import BaseDataValidator, ValidationResult
from .config import QualityConfig


@dataclass
class ValidationMetrics:
    """Metrics collected during validation."""
    
    total_records: int = 0
    null_records: int = 0
    invalid_records: int = 0
    outlier_records: int = 0
    quality_score: float = 0.0
    validation_time: float = 0.0
    checks_performed: List[str] = None
    
    def __post_init__(self):
        if self.checks_performed is None:
            self.checks_performed = []


class UniversalDataValidator(BaseDataValidator):
    """Universal data validator that can handle multiple data types."""
    
    def __init__(self, config: QualityConfig):
        super().__init__(config)
        self.config = config
    
    def validate(self, data: Any, data_type: str = "ticker") -> ValidationResult:
        """
        Validate data based on type.
        
        Args:
            data: Data to validate (typically pandas DataFrame)
            data_type: Type of data ('ticker', 'macro', 'transformed', 'generic')
        
        Returns:
            ValidationResult with comprehensive validation details
        """
        if data_type == "ticker":
            return self.validate_ticker_data(data)
        elif data_type == "macro":
            return self.validate_macro_data(data)
        elif data_type == "transformed":
            return self.validate_transformed_data(data)
        else:
            return self.validate_generic_data(data)
    
    def validate_ticker_data(self, df: pd.DataFrame) -> ValidationResult:
        """Validate ticker OHLCV data."""
        issues = []
        warnings = []
        quality_score = 1.0
        metrics = ValidationMetrics(total_records=len(df) if df is not None else 0)
        
        if df is None or df.empty:
            return ValidationResult(
                is_valid=False,
                quality_score=0.0,
                issues=["DataFrame is None or empty"],
                metadata={"metrics": metrics}
            )
        
        # Check required columns
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            issues.append(f"Missing required columns: {missing_cols}")
            quality_score -= 0.3
        
        # Check OHLC relationships with robust error handling
        if all(col in df.columns for col in required_cols[:4]):
            ohlc_issues = self._validate_ohlc_relationships(df)
            if ohlc_issues["critical_count"] > 0:
                issues.extend(ohlc_issues["issues"])
                quality_score -= ohlc_issues["quality_penalty"]
            if ohlc_issues["warning_count"] > 0:
                warnings.extend(ohlc_issues["warnings"])
        
        # Check for negative values
        negative_issues = self._check_negative_values(df)
        if negative_issues:
            issues.extend(negative_issues)
            quality_score -= 0.2
        
        # Check null values
        null_check = self._check_null_ratio(df, self.config.max_null_ratio)
        if not null_check.is_valid:
            issues.extend(null_check.issues)
            quality_score = min(quality_score, null_check.quality_score)
        
        # Check for outliers
        outlier_check = self._check_outliers(df)
        if outlier_check["outlier_ratio"] > self.config.max_outlier_ratio:
            warnings.append(f"High outlier ratio: {outlier_check['outlier_ratio']:.2%}")
            quality_score -= 0.1
        
        # Update metrics
        metrics.null_records = df.isnull().sum().sum()
        metrics.quality_score = quality_score
        metrics.checks_performed = ["required_columns", "ohlc_relationships", "negative_values", "null_ratio", "outliers"]
        
        quality_score = max(0.0, quality_score)
        is_valid = quality_score >= self.config.min_quality_score
        
        return ValidationResult(
            is_valid=is_valid,
            quality_score=quality_score,
            issues=issues,
            warnings=warnings,
            metadata={"metrics": metrics}
        )
    
    def validate_macro_data(self, df: pd.DataFrame) -> ValidationResult:
        """Validate macro economic data."""
        issues = []
        warnings = []
        quality_score = 1.0
        
        if df is None or df.empty:
            return ValidationResult(
                is_valid=False,
                quality_score=0.0,
                issues=["DataFrame is None or empty"]
            )
        
        # Check for required date column
        date_cols = ['date', 'DATE', 'Date', 'trade_date']
        date_col = None
        for col in date_cols:
            if col in df.columns:
                date_col = col
                break
        
        if date_col is None:
            issues.append("No date column found")
            quality_score -= 0.3
        else:
            # Validate date format and range
            try:
                dates = pd.to_datetime(df[date_col])
                min_date = dates.min()
                max_date = dates.max()
                
                # Check reasonable date range
                min_reasonable = datetime(1900, 1, 1)
                max_reasonable = datetime.now() + timedelta(days=365)
                
                if min_date < min_reasonable or max_date > max_reasonable:
                    warnings.append(f"Date range may be unreasonable: {min_date} to {max_date}")
                    
            except Exception as e:
                issues.append(f"Invalid date format: {e}")
                quality_score -= 0.2
        
        # Check for value columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) == 0:
            issues.append("No numeric value columns found")
            quality_score -= 0.3
        
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
    
    def validate_transformed_data(self, df: pd.DataFrame) -> ValidationResult:
        """Validate transformed/enriched data."""
        # Start with ticker validation
        result = self.validate_ticker_data(df)
        
        # Additional checks for transformed data
        if result.is_valid:
            # Check for technical indicators
            indicator_cols = [col for col in df.columns if any(ind in col.lower() for ind in 
                            ['rsi', 'macd', 'sma', 'ema', 'bb_', 'stoch', 'adx', 'cci'])]
            
            if len(indicator_cols) == 0:
                result.warnings.append("No technical indicators found in transformed data")
                result.quality_score -= 0.1
            else:
                result.metadata["indicators_found"] = len(indicator_cols)
                result.metadata["indicator_columns"] = indicator_cols
        
        return result
    
    def validate_generic_data(self, df: pd.DataFrame) -> ValidationResult:
        """Validate generic data with basic checks."""
        issues = []
        warnings = []
        quality_score = 1.0
        
        if df is None or df.empty:
            return ValidationResult(
                is_valid=False,
                quality_score=0.0,
                issues=["DataFrame is None or empty"]
            )
        
        # Basic checks
        null_check = self._check_null_ratio(df, self.config.max_null_ratio)
        if not null_check.is_valid:
            issues.extend(null_check.issues)
            quality_score = min(quality_score, null_check.quality_score)
        
        # Check for completely empty columns
        empty_cols = [col for col in df.columns if df[col].isnull().all()]
        if empty_cols:
            warnings.append(f"Completely empty columns found: {empty_cols}")
            quality_score -= 0.1
        
        quality_score = max(0.0, quality_score)
        is_valid = quality_score >= self.config.min_quality_score
        
        return ValidationResult(
            is_valid=is_valid,
            quality_score=quality_score,
            issues=issues,
            warnings=warnings
        )
    
    def _validate_ohlc_relationships(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Validate OHLC price relationships with detailed reporting."""
        result = {
            "issues": [],
            "warnings": [],
            "critical_count": 0,
            "warning_count": 0,
            "quality_penalty": 0.0
        }
        
        try:
            # Convert to numeric with error handling
            for col in ['high', 'low', 'open', 'close']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Add tolerance for floating-point precision (0.1%)
            tolerance = 0.001
            
            # Check relationships
            invalid_high_low = df['high'] < (df['low'] * (1 - tolerance))
            invalid_high_open = df['high'] < (df['open'] * (1 - tolerance))
            invalid_high_close = df['high'] < (df['close'] * (1 - tolerance))
            invalid_low_open = df['low'] > (df['open'] * (1 + tolerance))
            invalid_low_close = df['low'] > (df['close'] * (1 + tolerance))
            
            invalid_relationships = (
                invalid_high_low | invalid_high_open | invalid_high_close |
                invalid_low_open | invalid_low_close
            )
            
            invalid_count = invalid_relationships.sum()
            total_count = len(df)
            
            if invalid_count > 0:
                invalid_ratio = invalid_count / total_count
                if invalid_ratio > 0.05:  # More than 5% invalid
                    result["issues"].append(f"Critical OHLC validation failure: {invalid_count}/{total_count} records ({invalid_ratio:.2%}) have invalid relationships")
                    result["critical_count"] = invalid_count
                    result["quality_penalty"] = min(0.5, invalid_ratio * 3)
                else:
                    result["warnings"].append(f"Minor OHLC inconsistencies: {invalid_count}/{total_count} records ({invalid_ratio:.2%})")
                    result["warning_count"] = invalid_count
                    result["quality_penalty"] = min(0.1, invalid_ratio)
                    
        except Exception as e:
            result["warnings"].append(f"OHLC validation failed due to data type issues: {e}")
        
        return result
    
    def _check_negative_values(self, df: pd.DataFrame) -> List[str]:
        """Check for negative values in price and volume columns."""
        issues = []
        
        if 'volume' in df.columns and (df['volume'] < 0).any():
            issues.append("Negative volume values found")
        
        price_cols = ['open', 'high', 'low', 'close', 'open_price', 'high_price', 'low_price', 'close_price']
        for col in price_cols:
            if col in df.columns and (df[col] < 0).any():
                issues.append(f"Negative price values found in {col}")
        
        return issues
    
    def _check_outliers(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check for outliers in numeric columns."""
        result = {"outlier_ratio": 0.0, "outlier_columns": []}
        
        try:
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            total_outliers = 0
            total_values = 0
            
            for col in numeric_cols:
                if col in df.columns:
                    Q1 = df[col].quantile(0.25)
                    Q3 = df[col].quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - 1.5 * IQR
                    upper_bound = Q3 + 1.5 * IQR
                    
                    outliers = ((df[col] < lower_bound) | (df[col] > upper_bound)).sum()
                    if outliers > 0:
                        result["outlier_columns"].append({"column": col, "count": outliers})
                        total_outliers += outliers
                    
                    total_values += len(df[col].dropna())
            
            if total_values > 0:
                result["outlier_ratio"] = total_outliers / total_values
                
        except Exception:
            # Silently handle outlier detection errors
            pass
        
        return result


# Factory function for creating validators
def create_validator(data_type: str, config: QualityConfig) -> UniversalDataValidator:
    """
    Factory function to create appropriate validator.
    
    Args:
        data_type: Type of data to validate
        config: Quality configuration
    
    Returns:
        Configured validator instance
    """
    return UniversalDataValidator(config)


# Backward compatibility aliases
TickerDataValidator = UniversalDataValidator
MacroDataValidator = UniversalDataValidator
TransformDataValidator = UniversalDataValidator