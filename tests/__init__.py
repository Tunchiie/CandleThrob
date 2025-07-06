"""
Tests package for CandleThrob data pipeline.
Contains unit tests for ingestion, indicators, and macroeconomic data processing.
"""

# Import test modules for easier access
from . import test_ingestion
from . import test_indicators
from . import test_macros

__all__ = ['test_ingestion', 'test_indicators', 'test_macros']