#!/usr/bin/env python3
"""
Batch Calculator for CandleThrob Data Ingestion
Calculates optimal batch sizes and schedules for Polygon.io API rate limiting
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime, timedelta
import json

# Add the parent directory to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from CandleThrob.ingestion.ingest_data import get_sp500_tickers, get_etf_tickers

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def calculate_optimal_batches():
    """
    Calculate optimal batch configuration for Polygon.io rate limiting.
    
    Polygon.io allows 5 calls per minute = 300 calls per hour
    With 520 tickers (S&P 500 + ETFs), we need to spread this over time.
    """
    
    # Get all tickers
    sp500_tickers = get_sp500_tickers()
    etf_tickers = get_etf_tickers()
    all_tickers = sp500_tickers + etf_tickers
    
    batch_size = int(os.getenv("BATCH_SIZE", "25"))
    total_tickers = len(all_tickers)
    
    # Calculate total batches needed
    total_batches = (total_tickers + batch_size - 1) // batch_size
    
    # Calculate time needed with rate limiting
    # 5 calls per minute = 12 seconds between calls
    # Each batch takes: batch_size * 12 seconds = batch_size * 0.2 minutes
    time_per_batch_minutes = batch_size * 0.2
    total_time_minutes = total_batches * time_per_batch_minutes
    
    # Calculate optimal start times to spread across the day
    start_time = datetime.now().replace(hour=21, minute=0, second=0, microsecond=0)  # 9 PM EST
    
    batch_schedule = []
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min((batch_num + 1) * batch_size, total_tickers)
        batch_tickers = all_tickers[start_idx:end_idx]
        
        # Calculate start time for this batch
        batch_start_time = start_time + timedelta(minutes=batch_num * time_per_batch_minutes)
        
        batch_info = {
            "batch_number": batch_num,
            "start_idx": start_idx,
            "end_idx": end_idx,
            "ticker_count": len(batch_tickers),
            "tickers": batch_tickers,
            "scheduled_start": batch_start_time.isoformat(),
            "estimated_duration_minutes": time_per_batch_minutes
        }
        batch_schedule.append(batch_info)
    
    # Save batch schedule to file for Kestra to use
    schedule_file = "/app/data/batch_schedule.json"
    os.makedirs(os.path.dirname(schedule_file), exist_ok=True)
    
    with open(schedule_file, 'w') as f:
        json.dump({
            "total_tickers": total_tickers,
            "batch_size": batch_size,
            "total_batches": total_batches,
            "total_time_minutes": total_time_minutes,
            "rate_limit_calls_per_minute": 5,
            "rate_limit_seconds_between_calls": 12,
            "batch_schedule": batch_schedule
        }, f, indent=2)
    
    logger.info(f"Calculated {total_batches} batches for {total_tickers} tickers")
    logger.info(f"Total estimated time: {total_time_minutes:.1f} minutes")
    logger.info(f"Batch schedule saved to {schedule_file}")
    
    return batch_schedule

def main():
    """Main function to calculate and save batch schedule."""
    try:
        logger.info("Starting batch calculation for CandleThrob data ingestion...")
        
        batch_schedule = calculate_optimal_batches()
        
        # Print summary
        print(f"\n=== BATCH CALCULATION SUMMARY ===")
        print(f"Total tickers: {len(get_sp500_tickers()) + len(get_etf_tickers())}")
        print(f"Batch size: {os.getenv('BATCH_SIZE', '25')}")
        print(f"Total batches: {len(batch_schedule)}")
        print(f"Estimated total time: {len(batch_schedule) * 25 * 0.2:.1f} minutes")
        print(f"Schedule file: /app/data/batch_schedule.json")
        
        logger.info("Batch calculation completed successfully")
        
    except Exception as e:
        logger.error(f"Error in batch calculation: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 