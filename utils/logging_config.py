"""
Centralized Logging Configuration for CandleThrob

This module provides a centralized logging configuration that ensures all modules
log to appropriate files with consistent formatting. It supports multiple log files
for different types of operations (ingestion vs database).

Author: CandleThrob Team
Version: 2.0.0
Last Updated: 2025-07-13
"""

import logging
import os
from datetime import datetime
from typing import Optional

# Global flags to track if logging has been configured
_ingestion_logging_configured = False
_database_logging_configured = False

def setup_ingestion_logging(log_file: Optional[str] = None, level: int = logging.INFO) -> logging.Logger:
    """
    Set up logging configuration for ingestion operations.
    
    This function ensures that all ingestion modules use the same logging configuration
    and log to the same file. It prevents conflicts between different modules
    trying to configure logging independently.
    
    Args:
        log_file (Optional[str]): Path to log file. If None, uses default location
        level (int): Logging level (default: logging.INFO)
        
    Returns:
        logging.Logger: Configured logger instance
        
    Example:
        >>> logger = setup_ingestion_logging("/app/logs/ingestion.log")
        >>> logger.info("Starting ingestion process")
    """
    global _ingestion_logging_configured
    
    # Only configure logging once
    if _ingestion_logging_configured:
        return logging.getLogger("ingestion")
    
    # Set default log file if none provided
    if log_file is None:
        # Create logs directory if it doesn't exist
        logs_dir = "/app/logs"
        os.makedirs(logs_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(logs_dir, f"candlethrob_ingestion_{timestamp}.log")
    
    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Create file handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    
    # Configure ingestion logger
    ingestion_logger = logging.getLogger("ingestion")
    ingestion_logger.setLevel(level)
    
    # Remove existing handlers to avoid duplicates
    for handler in ingestion_logger.handlers[:]:
        ingestion_logger.removeHandler(handler)
    
    # Add our handlers
    ingestion_logger.addHandler(file_handler)
    ingestion_logger.addHandler(console_handler)
    
    # Mark as configured
    _ingestion_logging_configured = True
    
    # Log the configuration
    ingestion_logger.info(f"Ingestion logging configured - File: {log_file}, Level: {logging.getLevelName(level)}")
    
    return ingestion_logger

def setup_database_logging(log_file: Optional[str] = None, level: int = logging.INFO) -> logging.Logger:
    """
    Set up logging configuration for database operations.
    
    This function ensures that all database modules use the same logging configuration
    and log to a separate file from ingestion operations.
    
    Args:
        log_file (Optional[str]): Path to log file. If None, uses default location
        level (int): Logging level (default: logging.INFO)
        
    Returns:
        logging.Logger: Configured logger instance
        
    Example:
        >>> logger = setup_database_logging("/app/logs/database.log")
        >>> logger.info("Database operation completed")
    """
    global _database_logging_configured
    
    # Only configure logging once
    if _database_logging_configured:
        return logging.getLogger("database")
    
    # Set default log file if none provided
    if log_file is None:
        # Create logs directory if it doesn't exist
        logs_dir = "/app/logs"
        os.makedirs(logs_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(logs_dir, f"candlethrob_database_{timestamp}.log")
    
    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Create file handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    
    # Configure database logger
    database_logger = logging.getLogger("database")
    database_logger.setLevel(level)
    
    # Remove existing handlers to avoid duplicates
    for handler in database_logger.handlers[:]:
        database_logger.removeHandler(handler)
    
    # Add our handlers
    database_logger.addHandler(file_handler)
    database_logger.addHandler(console_handler)
    
    # Mark as configured
    _database_logging_configured = True
    
    # Log the configuration
    database_logger.info(f"Database logging configured - File: {log_file}, Level: {logging.getLevelName(level)}")
    
    return database_logger

def get_ingestion_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for ingestion operations.
    
    This function ensures that the ingestion logging is properly configured before
    returning a logger instance.
    
    Args:
        name (str): Logger name (usually __name__)
        
    Returns:
        logging.Logger: Configured logger instance for ingestion operations
        
    Example:
        >>> logger = get_ingestion_logger(__name__)
        >>> logger.info("Module initialized")
    """
    # Ensure logging is configured
    if not _ingestion_logging_configured:
        setup_ingestion_logging()
    
    return logging.getLogger(f"ingestion.{name}")

def get_database_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for database operations.
    
    This function ensures that the database logging is properly configured before
    returning a logger instance.
    
    Args:
        name (str): Logger name (usually __name__)
        
    Returns:
        logging.Logger: Configured logger instance for database operations
        
    Example:
        >>> logger = get_database_logger(__name__)
        >>> logger.info("Database operation completed")
    """
    # Ensure logging is configured
    if not _database_logging_configured:
        setup_database_logging()
    
    return logging.getLogger(f"database.{name}")

def log_ingestion_start(module_name: str, ticker: str = None, batch_info: str = None):
    """
    Log the start of an ingestion process with consistent formatting.
    
    Args:
        module_name (str): Name of the ingestion module
        ticker (str, optional): Ticker being processed
        batch_info (str, optional): Batch information
        
    Example:
        >>> log_ingestion_start("fetch_data", "AAPL", "Batch 1/21")
    """
    logger = get_ingestion_logger(__name__)
    
    if ticker and batch_info:
        logger.info(f"[{module_name}] Starting processing for {ticker} ({batch_info})")
    elif ticker:
        logger.info(f"[{module_name}] Starting processing for {ticker}")
    else:
        logger.info(f"[{module_name}] Starting ingestion process")

def log_ingestion_success(module_name: str, ticker: str = None, records: int = None):
    """
    Log successful completion of an ingestion process.
    
    Args:
        module_name (str): Name of the ingestion module
        ticker (str, optional): Ticker that was processed
        records (int, optional): Number of records processed
        
    Example:
        >>> log_ingestion_success("fetch_data", "AAPL", 252)
    """
    logger = get_ingestion_logger(__name__)
    
    if ticker and records:
        logger.info(f"[{module_name}] Successfully processed {ticker} - {records} records")
    elif ticker:
        logger.info(f"[{module_name}] Successfully processed {ticker}")
    else:
        logger.info(f"[{module_name}] Ingestion process completed successfully")

def log_ingestion_error(module_name: str, ticker: str = None, error: str = None):
    """
    Log errors during ingestion process.
    
    Args:
        module_name (str): Name of the ingestion module
        ticker (str, optional): Ticker that failed
        error (str, optional): Error message
        
    Example:
        >>> log_ingestion_error("fetch_data", "AAPL", "API rate limit exceeded")
    """
    logger = get_ingestion_logger(__name__)
    
    if ticker and error:
        logger.error(f"[{module_name}] Error processing {ticker}: {error}")
    elif ticker:
        logger.error(f"[{module_name}] Error processing {ticker}")
    else:
        logger.error(f"[{module_name}] Ingestion process failed")

def log_rate_limiting(module_name: str, sleep_time: float):
    """
    Log rate limiting information.
    
    Args:
        module_name (str): Name of the module
        sleep_time (float): Time to sleep in seconds
        
    Example:
        >>> log_rate_limiting("fetch_data", 12.5)
    """
    logger = get_ingestion_logger(__name__)
    logger.info(f"[{module_name}] Rate limiting: sleeping {sleep_time:.2f} seconds")

def log_batch_progress(module_name: str, current: int, total: int, ticker: str = None):
    """
    Log batch processing progress.
    
    Args:
        module_name (str): Name of the module
        current (int): Current item number
        total (int): Total number of items
        ticker (str, optional): Current ticker being processed
        
    Example:
        >>> log_batch_progress("ingest_data", 5, 25, "AAPL")
    """
    logger = get_ingestion_logger(__name__)
    
    if ticker:
        logger.info(f"[{module_name}] Processing {current}/{total}: {ticker}")
    else:
        logger.info(f"[{module_name}] Processing {current}/{total}")

def log_database_operation(operation: str, table: str = None, records: int = None, error: str = None):
    """
    Log database operations with consistent formatting.
    
    Args:
        operation (str): Type of database operation (insert, update, delete, etc.)
        table (str, optional): Table name
        records (int, optional): Number of records affected
        error (str, optional): Error message if operation failed
        
    Example:
        >>> log_database_operation("insert", "ticker_data", 1000)
        >>> log_database_operation("create_table", "macro_data", error="Table already exists")
    """
    logger = get_database_logger(__name__)
    
    if error:
        logger.error(f"[DATABASE] {operation} failed on {table}: {error}")
    elif table and records:
        logger.info(f"[DATABASE] {operation} completed on {table} - {records} records")
    elif table:
        logger.info(f"[DATABASE] {operation} completed on {table}")
    else:
        logger.info(f"[DATABASE] {operation} completed")

# Initialize logging when module is imported
setup_ingestion_logging()
setup_database_logging() 