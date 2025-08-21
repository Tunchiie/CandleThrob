"""
Unified Data Models for CandleThrob
===================================

This module provides centralized, unified data models that replace
scattered model definitions across the system.
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
from typing import Optional, Dict, Any, List, Union, Tuple
from dataclasses import dataclass, field
from enum import Enum


class DataType(Enum):
    """Enumeration of data types in the system."""
    TICKER_RAW = "ticker_raw"
    TICKER_TRANSFORMED = "ticker_transformed"
    MACRO_RAW = "macro_raw"
    MACRO_TRANSFORMED = "macro_transformed"
    VALIDATION_RESULT = "validation_result"
    PROCESSING_RESULT = "processing_result"


class DataStatus(Enum):
    """Enumeration of data processing statuses."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    VALIDATION_FAILED = "validation_failed"
    RETRYING = "retrying"


@dataclass
class BaseDataModel:
    """Base class for all data models."""
    
    id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    status: DataStatus = DataStatus.PENDING
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary."""
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, datetime):
                result[key] = value.isoformat()
            elif isinstance(value, Enum):
                result[key] = value.value
            else:
                result[key] = value
        return result
    
    def update_timestamp(self) -> None:
        """Update the updated_at timestamp."""
        self.updated_at = datetime.now()


@dataclass
class TickerData(BaseDataModel):
    """Model for raw ticker OHLCV data."""
    
    ticker: Optional[str] = None
    trade_date: Optional[date] = None
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    close_price: Optional[float] = None
    volume: Optional[int] = None
    adjusted_close: Optional[float] = None
    dividend_amount: Optional[float] = None
    split_coefficient: Optional[float] = None
    
    # Data quality metrics
    data_quality_score: Optional[float] = None
    validation_issues: List[str] = field(default_factory=list)
    
    def validate(self) -> Tuple[bool, List[str]]:
        """Validate ticker data."""
        issues = []
        
        if not self.ticker:
            issues.append("Ticker symbol is required")
        
        if not self.trade_date:
            issues.append("Trade date is required")
        
        if self.open_price is None or self.open_price < 0:
            issues.append("Invalid open price")
        
        if self.high_price is None or self.high_price < 0:
            issues.append("Invalid high price")
        
        if self.low_price is None or self.low_price < 0:
            issues.append("Invalid low price")
        
        if self.close_price is None or self.close_price < 0:
            issues.append("Invalid close price")
        
        if self.volume is None or self.volume < 0:
            issues.append("Invalid volume")
        
        # OHLC relationship validation
        if all(x is not None for x in [self.open_price, self.high_price, self.low_price, self.close_price]):
            if self.high_price < max(self.open_price, self.close_price):
                issues.append("High price is less than open/close price")
            if self.low_price > min(self.open_price, self.close_price):
                issues.append("Low price is greater than open/close price")
        
        self.validation_issues = issues
        return len(issues) == 0, issues
    
    @classmethod
    def from_dataframe_row(cls, row: pd.Series) -> 'TickerData':
        """Create TickerData from a pandas DataFrame row."""
        return cls(
            ticker=row.get('ticker'),
            trade_date=pd.to_datetime(row.get('trade_date')).date() if row.get('trade_date') else None,
            open_price=float(row.get('open', 0)) if pd.notna(row.get('open')) else None,
            high_price=float(row.get('high', 0)) if pd.notna(row.get('high')) else None,
            low_price=float(row.get('low', 0)) if pd.notna(row.get('low')) else None,
            close_price=float(row.get('close', 0)) if pd.notna(row.get('close')) else None,
            volume=int(row.get('volume', 0)) if pd.notna(row.get('volume')) else None,
            adjusted_close=float(row.get('adjusted_close', 0)) if pd.notna(row.get('adjusted_close')) else None,
        )


@dataclass
class TransformedTickerData(TickerData):
    """Model for transformed ticker data with technical indicators."""
    
    # Moving averages
    sma_20: Optional[float] = None
    sma_50: Optional[float] = None
    sma_200: Optional[float] = None
    ema_12: Optional[float] = None
    ema_26: Optional[float] = None
    
    # Technical indicators
    rsi_14: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_histogram: Optional[float] = None
    
    # Bollinger Bands
    bb_upper: Optional[float] = None
    bb_middle: Optional[float] = None
    bb_lower: Optional[float] = None
    bb_width: Optional[float] = None
    bb_percent: Optional[float] = None
    
    # Volume indicators
    volume_sma_20: Optional[float] = None
    volume_ratio: Optional[float] = None
    
    # Volatility
    atr_14: Optional[float] = None
    volatility_20d: Optional[float] = None
    
    # Price changes
    price_change_1d: Optional[float] = None
    price_change_5d: Optional[float] = None
    price_change_20d: Optional[float] = None
    
    # Transformation metadata
    indicators_applied: List[str] = field(default_factory=list)
    transformation_timestamp: Optional[datetime] = None
    
    def __post_init__(self):
        """Post-initialization processing."""
        if self.transformation_timestamp is None:
            self.transformation_timestamp = datetime.now()


@dataclass
class MacroData(BaseDataModel):
    """Model for macroeconomic data."""
    
    series_id: Optional[str] = None
    series_name: Optional[str] = None
    date: Optional[date] = None
    value: Optional[float] = None
    unit: Optional[str] = None
    frequency: Optional[str] = None
    source: Optional[str] = None
    
    # Data quality metrics
    data_quality_score: Optional[float] = None
    validation_issues: List[str] = field(default_factory=list)
    
    def validate(self) -> Tuple[bool, List[str]]:
        """Validate macro data."""
        issues = []
        
        if not self.series_id:
            issues.append("Series ID is required")
        
        if not self.date:
            issues.append("Date is required")
        
        if self.value is None:
            issues.append("Value is required")
        
        self.validation_issues = issues
        return len(issues) == 0, issues
    
    @classmethod
    def from_dataframe_row(cls, row: pd.Series, series_id: str) -> 'MacroData':
        """Create MacroData from a pandas DataFrame row."""
        return cls(
            series_id=series_id,
            date=pd.to_datetime(row.name).date() if hasattr(row, 'name') else None,
            value=float(row.iloc[0]) if len(row) > 0 and pd.notna(row.iloc[0]) else None,
        )


@dataclass
class TransformedMacroData(MacroData):
    """Model for transformed macroeconomic data."""
    
    # Trend indicators
    sma_12: Optional[float] = None
    sma_24: Optional[float] = None
    trend_direction: Optional[str] = None
    
    # Change metrics
    mom_1: Optional[float] = None  # 1-period momentum
    mom_3: Optional[float] = None  # 3-period momentum
    mom_12: Optional[float] = None  # 12-period momentum
    
    # Volatility
    volatility_12: Optional[float] = None
    
    # Seasonal adjustments
    seasonal_factor: Optional[float] = None
    seasonally_adjusted_value: Optional[float] = None
    
    # Transformation metadata
    indicators_applied: List[str] = field(default_factory=list)
    transformation_timestamp: Optional[datetime] = None
    
    def __post_init__(self):
        """Post-initialization processing."""
        if self.transformation_timestamp is None:
            self.transformation_timestamp = datetime.now()


@dataclass
class ValidationResult(BaseDataModel):
    """Model for data validation results."""
    
    data_type: DataType
    data_id: Optional[str] = None
    is_valid: bool = False
    quality_score: float = 0.0
    issues: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    # Validation metrics
    total_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    null_records: int = 0
    
    # Performance metrics
    validation_duration_ms: float = 0.0
    validation_timestamp: datetime = field(default_factory=datetime.now)
    
    @property
    def validation_passed(self) -> bool:
        """Check if validation passed."""
        return self.is_valid and len(self.issues) == 0
    
    @property
    def validation_rate(self) -> float:
        """Calculate validation success rate."""
        return self.valid_records / self.total_records if self.total_records > 0 else 0.0


@dataclass
class ProcessingResult(BaseDataModel):
    """Model for processing operation results."""
    
    operation_type: str
    data_type: DataType
    input_count: int = 0
    output_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    
    # Performance metrics
    processing_duration_ms: float = 0.0
    processing_timestamp: datetime = field(default_factory=datetime.now)
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    
    # Error tracking
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    @property
    def success_rate(self) -> float:
        """Calculate processing success rate."""
        total = self.success_count + self.failure_count
        return self.success_count / total if total > 0 else 0.0
    
    @property
    def processing_efficiency(self) -> float:
        """Calculate processing efficiency (records per second)."""
        duration_seconds = self.processing_duration_ms / 1000
        return self.output_count / duration_seconds if duration_seconds > 0 else 0.0


@dataclass
class BatchMetadata(BaseDataModel):
    """Model for batch processing metadata."""
    
    batch_id: str
    batch_type: str
    total_items: int = 0
    processed_items: int = 0
    successful_items: int = 0
    failed_items: int = 0
    
    start_timestamp: Optional[datetime] = None
    end_timestamp: Optional[datetime] = None
    
    # Configuration
    batch_size: int = 0
    parallel_processing: bool = False
    max_workers: int = 1
    
    # Results
    results: List[ProcessingResult] = field(default_factory=list)
    
    @property
    def is_completed(self) -> bool:
        """Check if batch is completed."""
        return self.processed_items >= self.total_items
    
    @property
    def success_rate(self) -> float:
        """Calculate batch success rate."""
        return self.successful_items / self.total_items if self.total_items > 0 else 0.0
    
    @property
    def duration_seconds(self) -> float:
        """Calculate batch duration in seconds."""
        if self.start_timestamp and self.end_timestamp:
            return (self.end_timestamp - self.start_timestamp).total_seconds()
        return 0.0


# Factory functions for creating models
def create_ticker_data(ticker: str, trade_date: date, **kwargs) -> TickerData:
    """Create a TickerData instance."""
    return TickerData(ticker=ticker, trade_date=trade_date, **kwargs)

def create_macro_data(series_id: str, date: date, value: float, **kwargs) -> MacroData:
    """Create a MacroData instance."""
    return MacroData(series_id=series_id, date=date, value=value, **kwargs)

def create_validation_result(data_type: DataType, **kwargs) -> ValidationResult:
    """Create a ValidationResult instance."""
    return ValidationResult(data_type=data_type, **kwargs)

def create_processing_result(operation_type: str, data_type: DataType, **kwargs) -> ProcessingResult:
    """Create a ProcessingResult instance."""
    return ProcessingResult(operation_type=operation_type, data_type=data_type, **kwargs)

def create_batch_metadata(batch_id: str, batch_type: str, **kwargs) -> BatchMetadata:
    """Create a BatchMetadata instance."""
    return BatchMetadata(batch_id=batch_id, batch_type=batch_type, **kwargs)


# Utility functions for DataFrame conversions
def dataframe_to_ticker_data(df: pd.DataFrame) -> List[TickerData]:
    """Convert DataFrame to list of TickerData instances."""
    return [TickerData.from_dataframe_row(row) for _, row in df.iterrows()]

def ticker_data_to_dataframe(ticker_data_list: List[TickerData]) -> pd.DataFrame:
    """Convert list of TickerData instances to DataFrame."""
    return pd.DataFrame([data.to_dict() for data in ticker_data_list])

def dataframe_to_macro_data(df: pd.DataFrame, series_id: str) -> List[MacroData]:
    """Convert DataFrame to list of MacroData instances."""
    result = []
    for idx, row in df.iterrows():
        if isinstance(idx, (datetime, date)):
            macro_data = MacroData(
                series_id=series_id,
                date=idx.date() if isinstance(idx, datetime) else idx,
                value=float(row.iloc[0]) if len(row) > 0 and pd.notna(row.iloc[0]) else None
            )
            result.append(macro_data)
    return result

def macro_data_to_dataframe(macro_data_list: List[MacroData]) -> pd.DataFrame:
    """Convert list of MacroData instances to DataFrame."""
    return pd.DataFrame([data.to_dict() for data in macro_data_list])