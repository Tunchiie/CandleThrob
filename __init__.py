"""
CandleThrob - Financial Data Ingestion and Analysis Pipeline
============================================================

A comprehensive financial data pipeline for ingesting, transforming, and analyzing
market data from various sources including Polygon.io and FRED.

Key Features:
- Real-time and historical market data ingestion
- Oracle Autonomous Database integration
- OCI Vault secrets management
- Kestra workflow orchestration
- Advanced data transformation and enrichment

Modules:
- ingestion: Data ingestion from external APIs
- transform: Data transformation and enrichment
- utils: Utility functions and database connections
"""

__version__ = "2.0.0"
__author__ = "Adetunji Fasiku"
__description__ = "Financial Data Ingestion and Analysis Pipeline"

# Package metadata
__all__ = [
    "ingestion",
    "transform", 
    "utils",
]
