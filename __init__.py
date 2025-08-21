"""
CandleThrob - Financial Data Ingestion and Analysis Pipeline
============================================================

A comprehensive financial data pipeline for ingesting, transforming, and analyzing
market data from various sources including Polygon.io and FRED.

Key Features:
- Real-time and historical market data ingestion
- Oracle Autonomous Database integration
- OCI Vault secrets management
- Apache Airflow workflow orchestration
- Advanced data transformation and enrichment

Modules:
- core: Shared components and base classes
- ingestion: Data ingestion from external APIs
- transform: Data transformation and enrichment
- utils: Utility functions and database connections
"""

__version__ = "2.1.0"
__author__ = "Adetunji Fasiku"
__description__ = "Financial Data Ingestion and Analysis Pipeline"

# Package metadata
__all__ = [
    "core",
    "ingestion",
    "transform", 
    "utils",
]
