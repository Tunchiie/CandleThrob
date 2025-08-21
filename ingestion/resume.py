"""
Resume Functionality for Data Ingestion
=======================================

This module provides resume functionality to avoid reprocessing
already completed tickers during ingestion runs.
"""

import os
import re
from datetime import datetime
from typing import List, Set
from pathlib import Path

from ..core.logging import get_ingestion_logger


class ResumeManager:
    """Manages resume functionality for ingestion operations."""
    
    def __init__(self, log_dir: str = "/app/logs"):
        """Initialize resume manager."""
        self.log_dir = Path(log_dir)
        self.logger = get_ingestion_logger("resume_manager")
    
    def get_processed_tickers(self) -> List[str]:
        """
        Check which tickers have been processed today by examining log files.
        This enables resume functionality without changing the core algorithm.
        
        Returns:
            List of ticker symbols that have been processed today
        """
        processed_tickers: Set[str] = set()
        
        try:
            today_str = datetime.now().strftime('%Y%m%d')
            
            if not self.log_dir.exists():
                self.logger.logger.info(f"Log directory {self.log_dir} does not exist")
                return []
            
            # Search through today's log files
            log_files = list(self.log_dir.glob(f'*ingestion_{today_str}*'))
            
            if not log_files:
                self.logger.logger.info(f"No ingestion log files found for {today_str}")
                return []
            
            self.logger.logger.info(f"Checking {len(log_files)} log files for processed tickers")
            
            for log_file in log_files:
                try:
                    with open(log_file, 'r') as f:
                        content = f.read()
                        
                    # Look for successful processing messages
                    # Pattern matches: "[ingest_data] Successfully processed AAPL - 1234 records"
                    pattern = r'\[.*\] Successfully processed (\w+) - \d+ records'
                    matches = re.findall(pattern, content)
                    processed_tickers.update(matches)
                    
                    # Also look for structured log format
                    # Pattern matches JSON logs with successful operations
                    json_pattern = r'"ticker":\s*"(\w+)".*"success":\s*true'
                    json_matches = re.findall(json_pattern, content)
                    processed_tickers.update(json_matches)
                    
                except Exception as e:
                    self.logger.logger.warning(f"Error reading log file {log_file}: {e}")
                    continue
            
            # Convert to sorted list
            processed_list = sorted(list(processed_tickers))
            
            self.logger.logger.info(f"Found {len(processed_list)} already processed tickers from logs")
            if processed_list:
                self.logger.logger.info(f"Sample processed tickers: {processed_list[:10]}...")
            
            return processed_list
            
        except Exception as e:
            self.logger.logger.warning(f"Could not check processed tickers: {e}")
            self.logger.logger.info("Continuing with full processing...")
            return []
    
    def mark_ticker_processed(self, ticker: str) -> None:
        """
        Mark a ticker as processed (for testing or manual marking).
        
        Args:
            ticker: The ticker symbol to mark as processed
        """
        try:
            # Create a simple marker file in the log directory
            today_str = datetime.now().strftime('%Y%m%d')
            marker_file = self.log_dir / f"processed_tickers_{today_str}.txt"
            
            # Ensure log directory exists
            self.log_dir.mkdir(parents=True, exist_ok=True)
            
            # Append ticker to marker file
            with open(marker_file, 'a') as f:
                f.write(f"{ticker}\n")
                
            self.logger.logger.info(f"Marked ticker {ticker} as processed in {marker_file}")
            
        except Exception as e:
            self.logger.logger.error(f"Failed to mark ticker {ticker} as processed: {e}")
    
    def clear_processed_markers(self, date: str = None) -> None:
        """
        Clear processed ticker markers for a specific date.
        
        Args:
            date: Date string in YYYYMMDD format. If None, uses today.
        """
        try:
            if date is None:
                date = datetime.now().strftime('%Y%m%d')
            
            # Remove marker files for the specified date
            marker_files = list(self.log_dir.glob(f"processed_tickers_{date}*"))
            
            for marker_file in marker_files:
                marker_file.unlink()
                self.logger.logger.info(f"Removed processed marker file: {marker_file}")
            
            self.logger.logger.info(f"Cleared processed markers for date {date}")
            
        except Exception as e:
            self.logger.logger.error(f"Failed to clear processed markers: {e}")
    
    def get_processing_statistics(self) -> dict:
        """
        Get statistics about processing history.
        
        Returns:
            Dictionary with processing statistics
        """
        try:
            stats = {
                "dates_with_processing": [],
                "total_processed_tickers": 0,
                "unique_tickers": set(),
                "processing_dates": {}
            }
            
            if not self.log_dir.exists():
                return stats
            
            # Look for all ingestion log files
            log_files = list(self.log_dir.glob('*ingestion_*'))
            
            for log_file in log_files:
                try:
                    # Extract date from filename
                    match = re.search(r'ingestion_(\d{8})', log_file.name)
                    if not match:
                        continue
                    
                    date = match.group(1)
                    if date not in stats["processing_dates"]:
                        stats["processing_dates"][date] = set()
                    
                    with open(log_file, 'r') as f:
                        content = f.read()
                    
                    # Find processed tickers
                    pattern = r'\[.*\] Successfully processed (\w+) - \d+ records'
                    matches = re.findall(pattern, content)
                    
                    for ticker in matches:
                        stats["processing_dates"][date].add(ticker)
                        stats["unique_tickers"].add(ticker)
                    
                except Exception as e:
                    self.logger.logger.warning(f"Error processing log file {log_file}: {e}")
                    continue
            
            # Convert sets to lists and calculate totals
            stats["dates_with_processing"] = sorted(stats["processing_dates"].keys())
            stats["total_processed_tickers"] = len(stats["unique_tickers"])
            stats["unique_tickers"] = sorted(list(stats["unique_tickers"]))
            
            # Convert date sets to counts
            date_counts = {}
            for date, tickers in stats["processing_dates"].items():
                date_counts[date] = len(tickers)
            stats["processing_dates"] = date_counts
            
            return stats
            
        except Exception as e:
            self.logger.logger.error(f"Failed to get processing statistics: {e}")
            return {}