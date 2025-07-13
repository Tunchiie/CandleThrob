#!/usr/bin/env python3
"""
Macro Data Update Script for CandleThrob
Dedicated script for updating macroeconomic data from FRED API
"""

import sys
import os
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, Any

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from CandleThrob.ingestion.fetch_data import DataIngestion
from CandleThrob.utils.models import MacroData
from CandleThrob.utils.oracle_conn import OracleDB

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/app/logs/macro_update.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MacroDataUpdater:
    """Handles macroeconomic data updates with enhanced error handling"""
    
    def __init__(self):
        self.db = OracleDB()
        self.engine = self.db.get_sqlalchemy_engine()
    
    def update_macro_data(self) -> Dict[str, Any]:
        """
        Update macroeconomic data with comprehensive error handling and logging
        
        Returns:
            Dict[str, Any]: Update results summary
        """
        logger.info("Starting macroeconomic data update...")
        
        results = {
            "status": "success",
            "start_time": datetime.now(),
            "end_time": None,
            "records_updated": 0,
            "indicators_updated": 0,
            "errors": []
        }
        
        try:
            with self.db.establish_connection() as conn:
                with conn.cursor() as cursor:
                    MacroData().create_table(cursor)
                
                # Check existing data and get last date
                macro_model = MacroData()
                if macro_model.data_exists(conn):
                    last_date = macro_model.get_last_date(conn)
                    if last_date:
                        start_date = last_date + timedelta(days=1)
                        logger.info(f"Incremental macro update: fetching from {start_date}")
                    else:
                        start_date = None
                        logger.info("No valid macro date found, fetching all available data")
                else:
                    start_date = None
                    logger.info("No existing macro data, fetching all available data")
                
                # Determine date range for fetching
                if start_date:
                    end_date = datetime.now()
                    if start_date.date() >= end_date.date():
                        logger.info("No new macro data to fetch")
                        results["status"] = "no_new_data"
                        results["end_time"] = datetime.now()
                        return results
                    
                    data = DataIngestion(
                        start_date=start_date.strftime("%Y-%m-%d"),
                        end_date=end_date.strftime("%Y-%m-%d")
                    )
                else:
                    data = DataIngestion()
                
                # Fetch FRED data
                logger.info("Fetching FRED data...")
                data.fetch_fred_data()
                
                if data.macro_df is None or data.macro_df.empty:
                    logger.warning("No new macroeconomic data was fetched")
                    results["status"] = "no_data_fetched"
                    results["end_time"] = datetime.now()
                    return results
                
                # Save to database
                logger.info(f"Saving {len(data.macro_df)} macro records to database")
                macro_model.insert_data(conn, data.macro_df)
                
                # Update results
                results["records_updated"] = len(data.macro_df)
                results["indicators_updated"] = len(data.macro_df['series_id'].unique())
                results["end_time"] = datetime.now()
                results["duration_seconds"] = (results["end_time"] - results["start_time"]).total_seconds()
                
                logger.info(f"Macro data update completed successfully")
                logger.info(f"Updated {results['records_updated']} records for {results['indicators_updated']} indicators")
                
                return results
                
        except Exception as e:
            logger.error(f"Error updating macro data: {str(e)}")
            results["status"] = "error"
            results["errors"].append(str(e))
            results["end_time"] = datetime.now()
            return results
    
    def validate_macro_update(self) -> Dict[str, Any]:
        """Validate the macro data update by checking recent data"""
        logger.info("Validating macro data update...")
        
        try:
            with self.db.establish_connection() as conn:
                # Check for recent data (last 30 days)
                recent_data = conn.execute("""
                    SELECT COUNT(DISTINCT series_id) as recent_indicators,
                           COUNT(*) as total_records
                    FROM macro_data 
                    WHERE date >= SYSDATE - 30
                """).fetchone()
                
                # Check for expected indicators
                expected_indicators = ['FEDFUNDS', 'CPIAUCSL', 'UNRATE', 'GDP', 'GS10', 'USREC']
                available_indicators = conn.execute("""
                    SELECT DISTINCT series_id FROM macro_data
                    WHERE date >= SYSDATE - 30
                """).fetchall()
                available_indicators = [row[0] for row in available_indicators]
                
                validation_results = {
                    "recent_indicators": recent_data[0],
                    "total_recent_records": recent_data[1],
                    "available_indicators": available_indicators,
                    "missing_indicators": [ind for ind in expected_indicators if ind not in available_indicators],
                    "validation_timestamp": datetime.now().isoformat()
                }
                
                logger.info(f"Macro validation: {recent_data[0]} indicators, {recent_data[1]} records")
                return validation_results
                
        except Exception as e:
            logger.error(f"Error validating macro data: {str(e)}")
            return {"status": "error", "message": str(e)}

def main():
    """Main function for macro data update"""
    try:
        logger.info("Starting macro data update process...")
        
        updater = MacroDataUpdater()
        
        # Update macro data
        update_results = updater.update_macro_data()
        
        # Validate the update
        validation_results = updater.validate_macro_update()
        
        # Save results
        results_file = "/app/data/macro_update_results.json"
        os.makedirs(os.path.dirname(results_file), exist_ok=True)
        
        combined_results = {
            "update_results": update_results,
            "validation_results": validation_results,
            "timestamp": datetime.now().isoformat()
        }
        
        with open(results_file, 'w') as f:
            json.dump(combined_results, f, indent=2)
        
        # Print summary
        print("\n=== MACRO DATA UPDATE SUMMARY ===")
        print(f"Status: {update_results['status']}")
        print(f"Records Updated: {update_results['records_updated']}")
        print(f"Indicators Updated: {update_results['indicators_updated']}")
        
        if update_results['end_time']:
            duration = update_results['duration_seconds']
            print(f"Duration: {duration:.1f} seconds")
        
        if validation_results.get('recent_indicators'):
            print(f"Recent Indicators (30 days): {validation_results['recent_indicators']}")
            print(f"Recent Records: {validation_results['total_recent_records']}")
        
        if validation_results.get('missing_indicators'):
            print(f"Missing Indicators: {validation_results['missing_indicators']}")
        
        print(f"Results saved to: {results_file}")
        
        logger.info("Macro data update process completed")
        
        # Exit with appropriate code
        if update_results['status'] == 'error':
            sys.exit(1)
        else:
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"Fatal error in macro update: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 