"""
Ingestion package for CandleThrob data pipeline.
Contains modules for fetching, ingesting, transforming, and enriching financial data.
"""

# Import ingestion modules for easier access
from . import fetch_data
from . import ingest_data
from . import transform_data
from . import enrich_data

__all__ = ['fetch_data', 'ingest_data', 'transform_data', 'enrich_data']