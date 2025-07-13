#!/usr/bin/env python3
"""
Oracle Compatibility Test Script for CandleThrob
Tests all SQL queries to ensure they work with Oracle Autonomous Database
"""

import sys
import os
import logging
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from CandleThrob.utils.oracle_conn import OracleDB
from CandleThrob.utils.models import TickerData, MacroData

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class OracleCompatibilityTester:
    """Test Oracle compatibility of all SQL queries"""
    
    def __init__(self):
        self.db = OracleDB()
    
    def test_basic_queries(self):
        """Test basic Oracle-compatible queries"""
        logger.info("Testing basic Oracle queries...")
        
        try:
            with self.db.establish_connection() as conn:
                # Test 1: Basic SELECT with SYSDATE
                result = conn.execute("SELECT SYSDATE FROM DUAL").scalar()
                logger.info(f"SYSDATE test: {result}")
                
                # Test 2: Date arithmetic
                result = conn.execute("SELECT SYSDATE - 7 FROM DUAL").scalar()
                logger.info(f"Date arithmetic test: {result}")
                
                # Test 3: ROWNUM (Oracle-specific)
                result = conn.execute("SELECT ROWNUM FROM DUAL WHERE ROWNUM <= 1").scalar()
                logger.info(f"ROWNUM test: {result}")
                
                logger.info("✅ Basic Oracle queries passed")
                return True
                
        except Exception as e:
            logger.error(f"❌ Basic Oracle queries failed: {str(e)}")
            return False
    
    def test_table_creation(self):
        """Test table creation with Oracle syntax"""
        logger.info("Testing table creation...")
        
        try:
            with self.db.establish_connection() as conn:
                # Test TickerData table creation
                ticker_model = TickerData()
                with conn.cursor() as cursor:
                    ticker_model.create_table(cursor)
                
                # Test MacroData table creation
                macro_model = MacroData()
                with conn.cursor() as cursor:
                    macro_model.create_table(cursor)
                
                logger.info("✅ Table creation tests passed")
                return True
                
        except Exception as e:
            logger.error(f"❌ Table creation tests failed: {str(e)}")
            return False
    
    def test_data_queries(self):
        """Test data querying with Oracle syntax"""
        logger.info("Testing data queries...")
        
        try:
            with self.db.establish_connection() as conn:
                # Test 1: COUNT with ROWNUM
                result = conn.execute("""
                    SELECT COUNT(*) FROM ticker_data WHERE ROWNUM <= 1
                """).scalar()
                logger.info(f"COUNT with ROWNUM: {result}")
                
                # Test 2: Date comparison with SYSDATE
                result = conn.execute("""
                    SELECT COUNT(*) FROM ticker_data 
                    WHERE trade_date >= SYSDATE - 7
                """).scalar()
                logger.info(f"Date comparison with SYSDATE: {result}")
                
                # Test 3: Window functions (Oracle supports these)
                result = conn.execute("""
                    SELECT ticker, trade_date,
                           LAG(trade_date) OVER (PARTITION BY ticker ORDER BY trade_date) as prev_date
                    FROM ticker_data
                    WHERE ROWNUM <= 5
                """).fetchall()
                logger.info(f"Window function test: {len(result)} rows")
                
                # Test 4: CASE statements
                result = conn.execute("""
                    SELECT 
                        SUM(CASE WHEN open_price IS NULL THEN 1 ELSE 0 END) as null_open
                    FROM ticker_data
                    WHERE ROWNUM <= 1
                """).scalar()
                logger.info(f"CASE statement test: {result}")
                
                logger.info("✅ Data query tests passed")
                return True
                
        except Exception as e:
            logger.error(f"❌ Data query tests failed: {str(e)}")
            return False
    
    def test_validation_queries(self):
        """Test validation queries used in monitoring"""
        logger.info("Testing validation queries...")
        
        try:
            with self.db.establish_connection() as conn:
                # Test 1: Data freshness query
                result = conn.execute("""
                    SELECT MAX(trade_date) FROM ticker_data
                """).scalar()
                logger.info(f"Data freshness query: {result}")
                
                # Test 2: Recent data query
                result = conn.execute("""
                    SELECT COUNT(DISTINCT ticker) as recent_tickers
                    FROM ticker_data 
                    WHERE trade_date >= SYSDATE - 7
                """).scalar()
                logger.info(f"Recent data query: {result}")
                
                # Test 3: Data gaps query
                result = conn.execute("""
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
                """).fetchall()
                logger.info(f"Data gaps query: {len(result)} results")
                
                # Test 4: Null value checks
                result = conn.execute("""
                    SELECT 
                        SUM(CASE WHEN open_price IS NULL THEN 1 ELSE 0 END) as null_open,
                        SUM(CASE WHEN high_price IS NULL THEN 1 ELSE 0 END) as null_high,
                        SUM(CASE WHEN low_price IS NULL THEN 1 ELSE 0 END) as null_low,
                        SUM(CASE WHEN close_price IS NULL THEN 1 ELSE 0 END) as null_close,
                        SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END) as null_volume
                    FROM ticker_data
                """).fetchone()
                logger.info(f"Null value checks: {result}")
                
                logger.info("✅ Validation query tests passed")
                return True
                
        except Exception as e:
            logger.error(f"❌ Validation query tests failed: {str(e)}")
            return False
    
    def test_macro_queries(self):
        """Test macro data queries"""
        logger.info("Testing macro data queries...")
        
        try:
            with self.db.establish_connection() as conn:
                # Test 1: Macro data freshness
                result = conn.execute("""
                    SELECT MAX(trade_date) FROM macro_data
                """).scalar()
                logger.info(f"Macro data freshness: {result}")
                
                # Test 2: Recent macro indicators
                result = conn.execute("""
                    SELECT COUNT(DISTINCT series_id) as recent_indicators,
                           COUNT(*) as total_records
                    FROM macro_data 
                    WHERE trade_date >= SYSDATE - 30
                """).fetchone()
                logger.info(f"Recent macro indicators: {result}")
                
                # Test 3: Available indicators
                result = conn.execute("""
                    SELECT DISTINCT series_id FROM macro_data
                    WHERE trade_date >= SYSDATE - 30
                """).fetchall()
                logger.info(f"Available indicators: {len(result)}")
                
                logger.info("✅ Macro query tests passed")
                return True
                
        except Exception as e:
            logger.error(f"❌ Macro query tests failed: {str(e)}")
            return False
    
    def run_all_tests(self):
        """Run all Oracle compatibility tests"""
        logger.info("Starting Oracle compatibility tests...")
        
        tests = [
            ("Basic Queries", self.test_basic_queries),
            ("Table Creation", self.test_table_creation),
            ("Data Queries", self.test_data_queries),
            ("Validation Queries", self.test_validation_queries),
            ("Macro Queries", self.test_macro_queries)
        ]
        
        results = {}
        for test_name, test_func in tests:
            logger.info(f"\n--- Running {test_name} Test ---")
            try:
                results[test_name] = test_func()
            except Exception as e:
                logger.error(f"❌ {test_name} test failed with exception: {str(e)}")
                results[test_name] = False
        
        # Print summary
        print("\n=== ORACLE COMPATIBILITY TEST RESULTS ===")
        passed = sum(1 for result in results.values() if result)
        total = len(results)
        
        for test_name, result in results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"{test_name}: {status}")
        
        print(f"\nOverall: {passed}/{total} tests passed")
        
        if passed == total:
            logger.info("🎉 All Oracle compatibility tests passed!")
            return True
        else:
            logger.error(f"❌ {total - passed} tests failed")
            return False

def main():
    """Main function to run Oracle compatibility tests"""
    try:
        tester = OracleCompatibilityTester()
        success = tester.run_all_tests()
        
        if success:
            print("\n✅ All Oracle compatibility tests passed!")
            sys.exit(0)
        else:
            print("\n❌ Some Oracle compatibility tests failed!")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error in Oracle compatibility testing: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 