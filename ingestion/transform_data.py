#!/usr/bin/env python3
"""
Transform Data Script for CandleThrob
=====================================

This script processes raw OHLCV data and adds technical indicators using TA-Lib.
It's designed to run as the transformation step after data ingestion is complete.
"""

import sys
import os
import re
import pandas as pd
import logging
import time
from datetime import datetime
from typing import Optional

# Add parent directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import GCS utilities
from utils.gcs import upload_to_gcs, load_from_gcs, blob_exists

# Try to import TA-Lib with fallback
try:
    import talib
    TALIB_AVAILABLE = True
    print("✅ TA-Lib imported successfully")
except ImportError as e:
    print(f"❌ Warning: TA-Lib not available: {e}")
    TALIB_AVAILABLE = False

# Configure logging
logging.basicConfig(
    filename="ingestion/transform_debug.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class TechnicalTransformer:
    """Transforms raw OHLCV data by adding technical indicators."""
    
    def __init__(self):
        self.logger = logger
        
    def load_ticker_data(self, ticker: str, path: str = "raw/tickers") -> Optional[pd.DataFrame]:
        """Load ticker data from GCS."""
        blob_name = f"{path}/{ticker}.parquet"
        
        if not blob_exists(bucket_name="candlethrob-candata", blob_name=blob_name):
            self.logger.warning(f"No data found for {ticker} at {blob_name}")
            return None
            
        try:
            df = load_from_gcs(
                bucket_name="candlethrob-candata",
                source_blob_name=blob_name
            )
            self.logger.info(f"Loaded {len(df)} records for {ticker}")
            return df
        except Exception as e:
            self.logger.error(f"Error loading data for {ticker}: {e}")
            return None
    
    def calculate_basic_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate basic technical indicators without TA-Lib."""
        if df.empty:
            return df
            
        df = df.copy()
        
        # Simple Moving Averages
        for period in [10, 20, 50, 100, 200]:
            df[f'SMA_{period}'] = df['Close'].rolling(window=period).mean()
            
        # RSI (simplified calculation)
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))
        
        # Bollinger Bands
        sma_20 = df['Close'].rolling(window=20).mean()
        std_20 = df['Close'].rolling(window=20).std()
        df['BB_Upper'] = sma_20 + (std_20 * 2)
        df['BB_Lower'] = sma_20 - (std_20 * 2)
        
        # Volume indicators
        df['Volume_SMA_20'] = df['Volume'].rolling(window=20).mean()
        df['Volume_Ratio'] = df['Volume'] / df['Volume_SMA_20']
        
        return df
        
    def calculate_talib_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate technical indicators using TA-Lib."""
        if not TALIB_AVAILABLE or df.empty:
            return df
            
        df = df.copy()
        
        try:
            # Key TA-Lib indicators
            df['RSI_TALIB'] = talib.RSI(df['Close'], timeperiod=14)
            df['MACD'], df['MACD_Signal'], df['MACD_Hist'] = talib.MACD(df['Close'])
            df['ATR'] = talib.ATR(df['High'], df['Low'], df['Close'], timeperiod=14)
            df['OBV'] = talib.OBV(df['Close'], df['Volume'])
            
            # Key candlestick patterns
            df['DOJI'] = talib.CDLDOJI(df['Open'], df['High'], df['Low'], df['Close'])
            df['HAMMER'] = talib.CDLHAMMER(df['Open'], df['High'], df['Low'], df['Close'])
            
            self.logger.info(f"Calculated TA-Lib indicators for {len(df)} records")
            
        except Exception as e:
            self.logger.error(f"Error calculating TA-Lib indicators: {e}")
            
        return df
        
    def transform_ticker(self, ticker: str, source_path: str = "raw/tickers", 
                        dest_path: str = "processed/tickers") -> bool:
        """Transform a single ticker's data."""
        self.logger.info(f"Transforming {ticker}...")
        
        # Load raw data
        df = self.load_ticker_data(ticker, source_path)
        if df is None or df.empty:
            self.logger.warning(f"No data to transform for {ticker}")
            return False
            
        # Ensure required columns exist
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume', 'Date']
        if not all(col in df.columns for col in required_cols):
            self.logger.error(f"Missing required columns for {ticker}: {required_cols}")
            return False
            
        # Sort by date
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values('Date').reset_index(drop=True)
        
        # Calculate indicators
        df = self.calculate_basic_indicators(df)
        
        if TALIB_AVAILABLE:
            df = self.calculate_talib_indicators(df)
        else:
            self.logger.warning(f"TA-Lib not available for {ticker}, using basic indicators only")
            
        # Save transformed data
        try:
            dest_blob = f"{dest_path}/{ticker}.parquet"
            upload_to_gcs(
                data=df,
                bucket_name="candlethrob-candata", 
                destination_blob_name=dest_blob
            )
            self.logger.info(f"Saved transformed data for {ticker} to {dest_blob}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving transformed data for {ticker}: {e}")
            return False

def get_sp500_tickers():
    """Get S&P 500 ticker list."""
    try:
        tickers = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]["Symbol"].tolist()
        return [re.sub(r"[^A-Z0-9\-]", "-", ticker.strip().upper()).strip("-") for ticker in tickers]
    except Exception as e:
        logger.error(f"Error fetching S&P 500 tickers: {e}")
        return []

def get_etf_tickers():
    """Get ETF ticker list."""
    etf_tickers = ['SPY', 'QQQ', 'DIA', 'IWM', 'VTI', 'TLT', 'GLD',
                   'XLF', 'XLE', 'XLK', 'XLV', 'ARKK', 'VXX',
                   'TQQQ', 'SQQQ', 'SOXL', 'XLI', 'XLY', 'XLP', 'SLV']
    return [re.sub(r"[^A-Z0-9\-]", "-", ticker.strip().upper()).strip("-") for ticker in etf_tickers]

def main():
    """Main transformation function."""
    start_time = time.time()
    logger.info("STARTING DATA TRANSFORMATION at %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    transformer = TechnicalTransformer()
    
    # Get all tickers
    sp500_tickers = get_sp500_tickers()
    etf_tickers = get_etf_tickers()
    
    logger.info(f"Found {len(sp500_tickers)} S&P 500 tickers")
    logger.info(f"Found {len(etf_tickers)} ETF tickers")
    
    successful_transforms = 0
    failed_transforms = 0
    
    # Transform S&P 500 tickers
    for i, ticker in enumerate(sp500_tickers, 1):
        logger.info(f"Processing S&P 500 ticker {i}/{len(sp500_tickers)}: {ticker}")
        
        if transformer.transform_ticker(ticker, "raw/tickers", "processed/tickers"):
            successful_transforms += 1
        else:
            failed_transforms += 1
    
    # Transform ETF tickers  
    for i, ticker in enumerate(etf_tickers, 1):
        logger.info(f"Processing ETF ticker {i}/{len(etf_tickers)}: {ticker}")
        
        if transformer.transform_ticker(ticker, "raw/etfs", "processed/etfs"):
            successful_transforms += 1
        else:
            failed_transforms += 1
    
    # Summary
    total_tickers = len(sp500_tickers) + len(etf_tickers)
    logger.info(f"Total tickers processed: {total_tickers}")
    logger.info(f"Successful transformations: {successful_transforms}")
    logger.info(f"Failed transformations: {failed_transforms}")
    
    if TALIB_AVAILABLE:
        logger.info("✅ TA-Lib indicators were calculated")
    else:
        logger.info("⚠️  Only basic indicators calculated (TA-Lib not available)")

    elapsed_time = time.time() - start_time
    logger.info(f"Data transformation completed in {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    main()