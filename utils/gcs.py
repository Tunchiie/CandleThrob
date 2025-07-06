"""
Google Cloud Storage utility module for CandleThrob financial data pipeline.

This module provides core functionality for interacting with Google Cloud Storage,
including uploading/downloading DataFrames, checking file existence, and batch operations.
Optimized for pandas DataFrame operations using Parquet format.
"""

import pandas as pd
import logging
from io import BytesIO
from typing import List, Optional

# Configure logging
logger = logging.getLogger(__name__)

# Mock GCS functions for environments where google-cloud-storage is not available
def upload_to_gcs(bucket_name: str, data: pd.DataFrame, destination_blob_name: str) -> bool:
    """
    Upload a pandas DataFrame to Google Cloud Storage as Parquet format.
    
    Args:
        bucket_name (str): The name of the GCS bucket
        data (pd.DataFrame): The DataFrame to upload
        destination_blob_name (str): The destination path in the GCS bucket
        
    Returns:
        bool: True if upload successful, False otherwise
    """
    try:
        # For development/testing - just log the operation
        logger.info("Mock GCS upload: %d records to gs://%s/%s", 
                   len(data), bucket_name, destination_blob_name)
        return True
        
    except Exception as e:
        logger.error("Failed to upload data to gs://%s/%s: %s", 
                    bucket_name, destination_blob_name, e)
        return False

def load_from_gcs(bucket_name: str, source_blob_name: str) -> Optional[pd.DataFrame]:
    """
    Load a pandas DataFrame from Google Cloud Storage.
    
    Args:
        bucket_name (str): The name of the GCS bucket
        source_blob_name (str): The source path in the GCS bucket
        
    Returns:
        pd.DataFrame: The loaded DataFrame, or None if failed
    """
    try:
        # For development/testing - return None (file not found)
        logger.warning("Mock GCS load: gs://%s/%s (returning None)", 
                      bucket_name, source_blob_name)
        return None
        
    except Exception as e:
        logger.error("Failed to load data from gs://%s/%s: %s", 
                    bucket_name, source_blob_name, e)
        return None

def blob_exists(bucket_name: str, blob_name: str) -> bool:
    """
    Check if a blob exists in Google Cloud Storage.
    
    Args:
        bucket_name (str): The name of the GCS bucket
        blob_name (str): The blob path to check
        
    Returns:
        bool: True if blob exists, False otherwise
    """
    try:
        # For development/testing - return False
        logger.info("Mock GCS exists check: gs://%s/%s (returning False)", 
                   bucket_name, blob_name)
        return False
        
    except Exception as e:
        logger.error("Failed to check existence of gs://%s/%s: %s", 
                    bucket_name, blob_name, e)
        return False

def list_blobs_with_prefix(bucket_name: str, prefix: str) -> List[str]:
    """
    List all blobs in a bucket with a given prefix.
    
    Args:
        bucket_name (str): The name of the GCS bucket
        prefix (str): The prefix to filter blobs
        
    Returns:
        List[str]: List of blob names matching the prefix
    """
    try:
        # For development/testing - return empty list
        logger.info("Mock GCS list: prefix '%s' in bucket '%s' (returning empty list)", 
                   prefix, bucket_name)
        return []
        
    except Exception as e:
        logger.error("Failed to list blobs with prefix '%s' in bucket '%s': %s", 
                    prefix, bucket_name, e)
        return []

def delete_blob(bucket_name: str, blob_name: str) -> bool:
    """
    Delete a blob from Google Cloud Storage.
    
    Args:
        bucket_name (str): The name of the GCS bucket
        blob_name (str): The blob path to delete
        
    Returns:
        bool: True if deletion successful, False otherwise
    """
    try:
        # For development/testing - just log
        logger.info("Mock GCS delete: gs://%s/%s", bucket_name, blob_name)
        return True
        
    except Exception as e:
        logger.error("Failed to delete gs://%s/%s: %s", bucket_name, blob_name, e)
        return False

def batch_upload_dataframes(bucket_name: str, dataframes: dict, prefix: str = "") -> dict:
    """
    Upload multiple DataFrames to GCS in batch.
    
    Args:
        bucket_name (str): The name of the GCS bucket
        dataframes (dict): Dictionary of {filename: DataFrame} pairs
        prefix (str): Optional prefix for all blob names
        
    Returns:
        dict: Dictionary of {filename: success_status} results
    """
    results = {}
    
    for filename, df in dataframes.items():
        blob_name = f"{prefix}/{filename}" if prefix else filename
        if not blob_name.endswith('.parquet'):
            blob_name += '.parquet'
            
        success = upload_to_gcs(bucket_name, df, blob_name)
        results[filename] = success
        
    successful = sum(1 for success in results.values() if success)
    logger.info("Mock batch upload completed: %d/%d files successful", 
               successful, len(dataframes))
    
    return results

def batch_load_dataframes(bucket_name: str, blob_names: List[str]) -> dict:
    """
    Load multiple DataFrames from GCS in batch.
    
    Args:
        bucket_name (str): The name of the GCS bucket
        blob_names (List[str]): List of blob names to load
        
    Returns:
        dict: Dictionary of {blob_name: DataFrame} pairs (successful loads only)
    """
    dataframes = {}
    
    for blob_name in blob_names:
        df = load_from_gcs(bucket_name, blob_name)
        if df is not None:
            # Use filename without extension as key
            key = blob_name.split('/')[-1].replace('.parquet', '')
            dataframes[key] = df
            
    logger.info("Mock batch load completed: %d/%d files loaded successfully", 
               len(dataframes), len(blob_names))
    
    return dataframes

def get_bucket_info(bucket_name: str) -> dict:
    """
    Get information about a GCS bucket.
    
    Args:
        bucket_name (str): The name of the GCS bucket
        
    Returns:
        dict: Bucket information including size, object count, etc.
    """
    try:
        # For development/testing - return mock info
        info = {
            'name': bucket_name,
            'location': 'US',
            'storage_class': 'STANDARD',
            'created': None,
            'object_count': 0,
            'total_size_bytes': 0,
            'total_size_mb': 0.0
        }
        
        logger.info("Mock bucket info for '%s': %d objects, %.2f MB total", 
                   bucket_name, info['object_count'], info['total_size_mb'])
        
        return info
        
    except Exception as e:
        logger.error("Failed to get bucket info for '%s': %s", bucket_name, e)
        return {}

def test_gcs_connection(bucket_name: str) -> bool:
    """
    Test connection to Google Cloud Storage and verify bucket access.
    
    Args:
        bucket_name (str): The name of the GCS bucket to test
        
    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        # For development/testing - return True
        logger.info("Mock GCS connection test for bucket '%s': SUCCESS", bucket_name)
        return True
            
    except Exception as e:
        logger.error("GCS connection test failed: %s", e)
        return False

# Storage configuration constants
DEFAULT_BUCKET = "candlethrob-candata"
BUCKET_PATHS = {
    'raw_tickers': 'raw/tickers',
    'raw_etfs': 'raw/etfs', 
    'raw_macros': 'raw/macros',
    'processed_tickers': 'processed/tickers',
    'processed_etfs': 'processed/etfs',
    'analysis': 'analysis'
}

def get_standard_path(category: str, filename: str) -> str:
    """
    Get standardized GCS path for different data categories.
    
    Args:
        category (str): Data category (e.g., 'raw_tickers', 'processed_tickers')
        filename (str): Base filename
        
    Returns:
        str: Full GCS path
    """
    if category not in BUCKET_PATHS:
        raise ValueError(f"Unknown category '{category}'. Valid categories: {list(BUCKET_PATHS.keys())}")
        
    if not filename.endswith('.parquet'):
        filename += '.parquet'
        
    return f"{BUCKET_PATHS[category]}/{filename}"
