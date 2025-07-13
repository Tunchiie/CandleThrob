#!/usr/bin/env python3
"""
Check batch state and ticker processing status
"""

import os
import json
import sys
import pandas as pd
import re
from datetime import datetime

def clean_ticker(ticker: str) -> str:
    """Clean ticker symbol"""
    return re.sub(r"[^A-Z0-9\-]", "-", ticker.strip().upper()).strip("-")

def get_sp500_tickers():
    """Get S&P 500 tickers"""
    try:
        tickers = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]["Symbol"].tolist()
        return [clean_ticker(ticker) for ticker in tickers]
    except Exception as e:
        print(f"Error fetching S&P 500 tickers: {str(e)}")
        return []

def get_etf_tickers():
    """Get ETF tickers"""
    etf_tickers = ['SPY', 'QQQ', 'DIA', 'IWM', 'VTI', 'TLT', 'GLD',
                    'XLF', 'XLE', 'XLK', 'XLV', 'ARKK', 'VXX',
                    'TQQQ', 'SQQQ', 'SOXL', 'XLI', 'XLY', 'XLP', 'SLV']
    return [clean_ticker(ticker) for ticker in etf_tickers]

def main():
    """Check current batch state and ticker status"""
    print("=== Batch State Check ===\n")
    
    # Check state file
    state_file = "/app/data/batch_state.json"
    if os.path.exists(state_file):
        with open(state_file, 'r') as f:
            state = json.load(f)
            current_batch = state.get('current_batch', 0)
            last_updated = state.get('last_updated', 'Unknown')
            print(f"Current batch: {current_batch}")
            print(f"Last updated: {last_updated}")
    else:
        print("No state file found - starting from batch 0")
        current_batch = 0
    
    # Get tickers
    sp500_tickers = get_sp500_tickers()
    etf_tickers = get_etf_tickers()
    all_tickers = sp500_tickers + etf_tickers
    
    print(f"\nTotal tickers: {len(all_tickers)}")
    print(f"S&P 500 tickers: {len(sp500_tickers)}")
    print(f"ETF tickers: {len(etf_tickers)}")
    
    # Calculate batch info
    batch_size = 25
    total_batches = (len(all_tickers) + batch_size - 1) // batch_size
    
    print(f"\nBatch configuration:")
    print(f"Batch size: {batch_size}")
    print(f"Total batches: {total_batches}")
    
    # Show current batch tickers
    start_idx = current_batch * batch_size
    end_idx = min((current_batch + 1) * batch_size, len(all_tickers))
    
    if start_idx < len(all_tickers):
        current_batch_tickers = all_tickers[start_idx:end_idx]
        print(f"\nCurrent batch {current_batch} tickers ({len(current_batch_tickers)}):")
        for i, ticker in enumerate(current_batch_tickers):
            print(f"  {i+1:2d}. {ticker}")
    else:
        print(f"\nBatch {current_batch} is beyond available tickers")
    
    # Show next batch
    next_batch = (current_batch + 1) % total_batches
    next_start_idx = next_batch * batch_size
    next_end_idx = min((next_batch + 1) * batch_size, len(all_tickers))
    
    if next_start_idx < len(all_tickers):
        next_batch_tickers = all_tickers[next_start_idx:next_end_idx]
        print(f"\nNext batch {next_batch} tickers ({len(next_batch_tickers)}):")
        for i, ticker in enumerate(next_batch_tickers):
            print(f"  {i+1:2d}. {ticker}")
    
    # Check for result files
    print(f"\n=== Recent Batch Results ===")
    results_dir = "/app/data"
    if os.path.exists(results_dir):
        result_files = [f for f in os.listdir(results_dir) if f.startswith("batch_") and f.endswith("_results.json")]
        result_files.sort(key=lambda x: int(x.split("_")[1]))
        
        for result_file in result_files[-5:]:  # Show last 5 results
            try:
                with open(os.path.join(results_dir, result_file), 'r') as f:
                    result = json.load(f)
                    batch_num = result.get('batch_number', 'Unknown')
                    timestamp = result.get('timestamp', 'Unknown')
                    results = result.get('results', {})
                    successful = results.get('successful', 0)
                    failed = results.get('failed', 0)
                    print(f"Batch {batch_num}: {successful} successful, {failed} failed ({timestamp})")
            except Exception as e:
                print(f"Error reading {result_file}: {e}")

if __name__ == "__main__":
    main() 