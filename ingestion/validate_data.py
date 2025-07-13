#!/usr/bin/env python3
"""
Data Validation Script for CandleThrob
Validates ingested data quality and completeness
"""

import sys
import os
import logging
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from CandleThrob.utils.models import TickerData, MacroData
from CandleThrob.utils.oracle_conn import OracleDB

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/app/logs/validation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataValidator:
    """Validates data quality and completeness"""
    
    def __init__(self):
        self.db = OracleDB()
        self.engine = self.db.get_sqlalchemy_engine()
    
    def validate_ticker_data(self) -> Dict[str, Any]:
        """Validate ticker data quality and completeness"""
        logger.info("Validating ticker data...")
        
        try:
            with self.db.establish_connection() as conn:
                ticker_model = TickerData()
                
                if not ticker_model.data_exists(conn):
                    logger.warning("No ticker data found in database")
                    return {"status": "no_data", "message": "No ticker data found"}
                
                # Get basic statistics
                total_records = conn.execute("SELECT COUNT(*) FROM ticker_data").scalar()
                unique_tickers = conn.execute("SELECT COUNT(DISTINCT ticker) FROM ticker_data").scalar()
                date_range = conn.execute("""
                    SELECT MIN(trade_date) as min_date, MAX(trade_date) as max_date 
                    FROM ticker_data
                """).fetchone()
                
                # Check for data quality issues
                null_checks = conn.execute("""
                    SELECT 
                        COUNT(*) as total_null_checks,
                        SUM(CASE WHEN open IS NULL THEN 1 ELSE 0 END) as null_open,
                        SUM(CASE WHEN high IS NULL THEN 1 ELSE 0 END) as null_high,
                        SUM(CASE WHEN low IS NULL THEN 1 ELSE 0 END) as null_low,
                        SUM(CASE WHEN close IS NULL THEN 1 ELSE 0 END) as null_close,
                        SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END) as null_volume
                    FROM ticker_data
                """).fetchone()
                
                # Check for recent data (last 7 days)
                recent_data = conn.execute("""
                    SELECT COUNT(DISTINCT ticker) as recent_tickers
                    FROM ticker_data 
                    WHERE trade_date >= SYSDATE - 7
                """).scalar()
                
                # Check for data gaps
                gaps_query = """
                    SELECT ticker, COUNT(*) as gap_count
                    FROM (
                        SELECT ticker, trade_date,
                               LAG(trade_date) OVER (PARTITION BY ticker ORDER BY trade_date) as prev_date
                        FROM ticker_data
                    ) t
                    WHERE prev_date IS NOT NULL 
                    AND trade_date - prev_date > 1
                    GROUP BY ticker
                    HAVING COUNT(*) > 5
                    ORDER BY gap_count DESC
                """
                data_gaps = conn.execute(gaps_query).fetchall()
                # Limit to 10 results in Python since Oracle doesn't have LIMIT
                data_gaps = data_gaps[:10]
                
                validation_results = {
                    "status": "success",
                    "total_records": total_records,
                    "unique_tickers": unique_tickers,
                    "date_range": {
                        "min_date": str(date_range[0]) if date_range[0] else None,
                        "max_date": str(date_range[1]) if date_range[1] else None
                    },
                    "data_quality": {
                        "null_checks": dict(null_checks._mapping),
                        "recent_data_tickers": recent_data
                    },
                    "data_gaps": [{"ticker": row[0], "gap_count": row[1]} for row in data_gaps],
                    "validation_timestamp": datetime.now().isoformat()
                }
                
                logger.info(f"Ticker validation completed: {total_records} records, {unique_tickers} tickers")
                return validation_results
                
        except Exception as e:
            logger.error(f"Error validating ticker data: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def validate_macro_data(self) -> Dict[str, Any]:
        """Validate macroeconomic data quality and completeness"""
        logger.info("Validating macro data...")
        
        try:
            with self.db.establish_connection() as conn:
                macro_model = MacroData()
                
                if not macro_model.data_exists(conn):
                    logger.warning("No macro data found in database")
                    return {"status": "no_data", "message": "No macro data found"}
                
                # Get basic statistics
                total_records = conn.execute("SELECT COUNT(*) FROM macro_data").scalar()
                unique_indicators = conn.execute("SELECT COUNT(DISTINCT series_id) FROM macro_data").scalar()
                date_range = conn.execute("""
                    SELECT MIN(date) as min_date, MAX(date) as max_date 
                    FROM macro_data
                """).fetchone()
                
                # Check for recent data
                recent_data = conn.execute("""
                    SELECT COUNT(DISTINCT series_id) as recent_indicators
                    FROM macro_data 
                    WHERE date >= SYSDATE - 30
                """).scalar()
                
                # Check for missing indicators
                expected_indicators = ['FEDFUNDS', 'CPIAUCSL', 'UNRATE', 'GDP', 'GS10', 'USREC']
                available_indicators = conn.execute("""
                    SELECT DISTINCT series_id FROM macro_data
                """).fetchall()
                available_indicators = [row[0] for row in available_indicators]
                missing_indicators = [ind for ind in expected_indicators if ind not in available_indicators]
                
                validation_results = {
                    "status": "success",
                    "total_records": total_records,
                    "unique_indicators": unique_indicators,
                    "date_range": {
                        "min_date": str(date_range[0]) if date_range[0] else None,
                        "max_date": str(date_range[1]) if date_range[1] else None
                    },
                    "recent_indicators": recent_data,
                    "missing_indicators": missing_indicators,
                    "validation_timestamp": datetime.now().isoformat()
                }
                
                logger.info(f"Macro validation completed: {total_records} records, {unique_indicators} indicators")
                return validation_results
                
        except Exception as e:
            logger.error(f"Error validating macro data: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def check_batch_results(self) -> Dict[str, Any]:
        """Check batch processing results from recent runs"""
        logger.info("Checking batch processing results...")
        
        try:
            batch_results = []
            data_dir = "/app/data"
            
            if os.path.exists(data_dir):
                for filename in os.listdir(data_dir):
                    if filename.startswith("batch_") and filename.endswith("_results.json"):
                        filepath = os.path.join(data_dir, filename)
                        try:
                            with open(filepath, 'r') as f:
                                batch_data = json.load(f)
                                batch_results.append(batch_data)
                        except Exception as e:
                            logger.warning(f"Error reading {filename}: {str(e)}")
            
            # Analyze batch results
            if batch_results:
                total_batches = len(batch_results)
                successful_batches = sum(1 for b in batch_results if b.get("results", {}).get("failed", 0) == 0)
                total_tickers_processed = sum(b.get("results", {}).get("total_tickers", 0) for b in batch_results)
                total_successful = sum(b.get("results", {}).get("successful", 0) for b in batch_results)
                total_failed = sum(b.get("results", {}).get("failed", 0) for b in batch_results)
                
                batch_summary = {
                    "total_batches": total_batches,
                    "successful_batches": successful_batches,
                    "batch_success_rate": successful_batches / total_batches if total_batches > 0 else 0,
                    "total_tickers_processed": total_tickers_processed,
                    "total_successful_tickers": total_successful,
                    "total_failed_tickers": total_failed,
                    "overall_success_rate": total_successful / total_tickers_processed if total_tickers_processed > 0 else 0,
                    "recent_batches": batch_results[-5:]  # Last 5 batches
                }
            else:
                batch_summary = {"status": "no_batch_results", "message": "No batch results found"}
            
            return batch_summary
            
        except Exception as e:
            logger.error(f"Error checking batch results: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def generate_validation_report(self) -> Dict[str, Any]:
        """Generate comprehensive validation report"""
        logger.info("Generating validation report...")
        
        ticker_validation = self.validate_ticker_data()
        macro_validation = self.validate_macro_data()
        batch_summary = self.check_batch_results()
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "ticker_data": ticker_validation,
            "macro_data": macro_validation,
            "batch_processing": batch_summary,
            "overall_status": "healthy"
        }
        
        # Determine overall status
        if (ticker_validation.get("status") == "error" or 
            macro_validation.get("status") == "error"):
            report["overall_status"] = "error"
        elif (ticker_validation.get("status") == "no_data" and 
              macro_validation.get("status") == "no_data"):
            report["overall_status"] = "no_data"
        
        # Save report
        report_file = "/app/data/validation_report.json"
        os.makedirs(os.path.dirname(report_file), exist_ok=True)
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Validation report saved to {report_file}")
        return report

def main():
    """Main validation function"""
    try:
        logger.info("Starting data validation...")
        
        validator = DataValidator()
        report = validator.generate_validation_report()
        
        # Print summary
        print("\n=== DATA VALIDATION REPORT ===")
        print(f"Overall Status: {report['overall_status']}")
        print(f"Timestamp: {report['timestamp']}")
        
        if report['ticker_data']['status'] == 'success':
            ticker_data = report['ticker_data']
            print(f"\nTicker Data:")
            print(f"  Total Records: {ticker_data['total_records']:,}")
            print(f"  Unique Tickers: {ticker_data['unique_tickers']}")
            print(f"  Date Range: {ticker_data['date_range']['min_date']} to {ticker_data['date_range']['max_date']}")
            print(f"  Recent Data (7 days): {ticker_data['data_quality']['recent_data_tickers']} tickers")
        
        if report['macro_data']['status'] == 'success':
            macro_data = report['macro_data']
            print(f"\nMacro Data:")
            print(f"  Total Records: {macro_data['total_records']:,}")
            print(f"  Unique Indicators: {macro_data['unique_indicators']}")
            print(f"  Recent Indicators (30 days): {macro_data['recent_indicators']}")
        
        if 'total_batches' in report['batch_processing']:
            batch_data = report['batch_processing']
            print(f"\nBatch Processing:")
            print(f"  Total Batches: {batch_data['total_batches']}")
            print(f"  Successful Batches: {batch_data['successful_batches']}")
            print(f"  Batch Success Rate: {batch_data['batch_success_rate']:.1%}")
            print(f"  Overall Success Rate: {batch_data['overall_success_rate']:.1%}")
        
        print(f"\nReport saved to: /app/data/validation_report.json")
        
        logger.info("Data validation completed successfully")
        
    except Exception as e:
        logger.error(f"Error in validation: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 