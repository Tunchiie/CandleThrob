#!/usr/bin/env python3
"""
Ingestion Monitoring Script for CandleThrob
Monitors data ingestion performance and sends alerts for issues
"""

import sys
import os
import logging
import json
import smtplib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from CandleThrob.utils.models import TickerData, MacroData
from CandleThrob.utils.oracle_conn import OracleDB

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/app/logs/monitoring.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class IngestionMonitor:
    """Monitors data ingestion performance and health"""
    
    def __init__(self):
        self.db = OracleDB()
        self.engine = self.db.get_sqlalchemy_engine()
        self.alert_thresholds = {
            "data_freshness_hours": 24,  # Alert if data is older than 24 hours
            "success_rate_threshold": 0.9,  # Alert if success rate < 90%
            "batch_failure_threshold": 3,  # Alert if more than 3 consecutive batch failures
            "data_gap_threshold": 5  # Alert if ticker has more than 5 days of missing data
        }
    
    def check_data_freshness(self) -> Dict[str, Any]:
        """Check if data is fresh and up-to-date"""
        logger.info("Checking data freshness...")
        
        try:
            with self.db.establish_connection() as conn:
                # Check ticker data freshness
                latest_ticker_date = conn.execute("""
                    SELECT MAX(trade_date) FROM ticker_data
                """).scalar()
                
                # Check macro data freshness
                latest_macro_date = conn.execute("""
                    SELECT MAX(date) FROM macro_data
                """).scalar()
                
                current_time = datetime.now()
                
                ticker_freshness = {
                    "latest_date": latest_ticker_date,
                    "hours_old": None,
                    "is_fresh": True
                }
                
                macro_freshness = {
                    "latest_date": latest_macro_date,
                    "hours_old": None,
                    "is_fresh": True
                }
                
                if latest_ticker_date:
                    ticker_freshness["hours_old"] = (current_time - latest_ticker_date).total_seconds() / 3600
                    ticker_freshness["is_fresh"] = ticker_freshness["hours_old"] <= self.alert_thresholds["data_freshness_hours"]
                
                if latest_macro_date:
                    macro_freshness["hours_old"] = (current_time - latest_macro_date).total_seconds() / 3600
                    macro_freshness["is_fresh"] = macro_freshness["hours_old"] <= self.alert_thresholds["data_freshness_hours"]
                
                return {
                    "ticker_data": ticker_freshness,
                    "macro_data": macro_freshness,
                    "check_timestamp": current_time.isoformat()
                }
                
        except Exception as e:
            logger.error(f"Error checking data freshness: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def check_batch_performance(self) -> Dict[str, Any]:
        """Check batch processing performance from recent runs"""
        logger.info("Checking batch processing performance...")
        
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
                                # Only consider recent batches (last 24 hours)
                                batch_time = datetime.fromisoformat(batch_data.get("timestamp", "2000-01-01"))
                                if batch_time > datetime.now() - timedelta(hours=24):
                                    batch_results.append(batch_data)
                        except Exception as e:
                            logger.warning(f"Error reading {filename}: {str(e)}")
            
            if not batch_results:
                return {
                    "status": "no_recent_batches",
                    "message": "No batch results found in last 24 hours"
                }
            
            # Analyze performance
            total_batches = len(batch_results)
            successful_batches = sum(1 for b in batch_results if b.get("results", {}).get("failed", 0) == 0)
            total_tickers = sum(b.get("results", {}).get("total_tickers", 0) for b in batch_results)
            total_successful = sum(b.get("results", {}).get("successful", 0) for b in batch_results)
            
            success_rate = successful_batches / total_batches if total_batches > 0 else 0
            ticker_success_rate = total_successful / total_tickers if total_tickers > 0 else 0
            
            # Check for consecutive failures
            recent_batches = sorted(batch_results, key=lambda x: x.get("timestamp", ""))[-5:]
            consecutive_failures = 0
            for batch in reversed(recent_batches):
                if batch.get("results", {}).get("failed", 0) > 0:
                    consecutive_failures += 1
                else:
                    break
            
            performance_metrics = {
                "total_batches": total_batches,
                "successful_batches": successful_batches,
                "batch_success_rate": success_rate,
                "total_tickers_processed": total_tickers,
                "successful_tickers": total_successful,
                "ticker_success_rate": ticker_success_rate,
                "consecutive_failures": consecutive_failures,
                "is_healthy": (success_rate >= self.alert_thresholds["success_rate_threshold"] and 
                              consecutive_failures < self.alert_thresholds["batch_failure_threshold"])
            }
            
            return performance_metrics
            
        except Exception as e:
            logger.error(f"Error checking batch performance: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def check_data_quality(self) -> Dict[str, Any]:
        """Check data quality issues"""
        logger.info("Checking data quality...")
        
        try:
            with self.db.establish_connection() as conn:
                # Check for data gaps in ticker data
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
                    HAVING COUNT(*) > %s
                    ORDER BY gap_count DESC
                """
                
                data_gaps = conn.execute(gaps_query, (self.alert_thresholds["data_gap_threshold"],)).fetchall()
                
                # Check for null values
                null_counts = conn.execute("""
                    SELECT 
                        SUM(CASE WHEN open IS NULL THEN 1 ELSE 0 END) as null_open,
                        SUM(CASE WHEN high IS NULL THEN 1 ELSE 0 END) as null_high,
                        SUM(CASE WHEN low IS NULL THEN 1 ELSE 0 END) as null_low,
                        SUM(CASE WHEN close IS NULL THEN 1 ELSE 0 END) as null_close,
                        SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END) as null_volume
                    FROM ticker_data
                """).fetchone()
                
                quality_issues = {
                    "data_gaps": [{"ticker": row[0], "gap_count": row[1]} for row in data_gaps],
                    "null_values": dict(null_counts._mapping),
                    "has_issues": len(data_gaps) > 0 or any(null_counts)
                }
                
                return quality_issues
                
        except Exception as e:
            logger.error(f"Error checking data quality: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def generate_alert(self, alert_type: str, message: str, details: Dict[str, Any]) -> None:
        """Generate and send alert (placeholder for email/Slack integration)"""
        alert = {
            "type": alert_type,
            "message": message,
            "details": details,
            "timestamp": datetime.now().isoformat()
        }
        
        # Save alert to file
        alert_file = f"/app/data/alerts/alert_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        os.makedirs(os.path.dirname(alert_file), exist_ok=True)
        
        with open(alert_file, 'w') as f:
            json.dump(alert, f, indent=2)
        
        logger.warning(f"ALERT: {alert_type} - {message}")
        print(f"\n🚨 ALERT: {alert_type}")
        print(f"Message: {message}")
        print(f"Details: {details}")
        print(f"Alert saved to: {alert_file}")
    
    def run_monitoring_checks(self) -> Dict[str, Any]:
        """Run all monitoring checks and generate alerts if needed"""
        logger.info("Running comprehensive monitoring checks...")
        
        results = {
            "timestamp": datetime.now().isoformat(),
            "checks": {},
            "alerts": []
        }
        
        # Check data freshness
        freshness = self.check_data_freshness()
        results["checks"]["freshness"] = freshness
        
        if not freshness.get("ticker_data", {}).get("is_fresh", True):
            self.generate_alert(
                "DATA_FRESHNESS",
                "Ticker data is not fresh",
                {"ticker_data": freshness["ticker_data"]}
            )
            results["alerts"].append("DATA_FRESHNESS")
        
        if not freshness.get("macro_data", {}).get("is_fresh", True):
            self.generate_alert(
                "DATA_FRESHNESS",
                "Macro data is not fresh",
                {"macro_data": freshness["macro_data"]}
            )
            results["alerts"].append("DATA_FRESHNESS")
        
        # Check batch performance
        performance = self.check_batch_performance()
        results["checks"]["performance"] = performance
        
        if not performance.get("is_healthy", True):
            self.generate_alert(
                "BATCH_PERFORMANCE",
                "Batch processing performance is below threshold",
                performance
            )
            results["alerts"].append("BATCH_PERFORMANCE")
        
        # Check data quality
        quality = self.check_data_quality()
        results["checks"]["quality"] = quality
        
        if quality.get("has_issues", False):
            self.generate_alert(
                "DATA_QUALITY",
                "Data quality issues detected",
                quality
            )
            results["alerts"].append("DATA_QUALITY")
        
        # Save monitoring report
        report_file = "/app/data/monitoring_report.json"
        os.makedirs(os.path.dirname(report_file), exist_ok=True)
        
        with open(report_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Monitoring completed. {len(results['alerts'])} alerts generated.")
        return results

def main():
    """Main monitoring function"""
    try:
        logger.info("Starting ingestion monitoring...")
        
        monitor = IngestionMonitor()
        results = monitor.run_monitoring_checks()
        
        # Print summary
        print("\n=== INGESTION MONITORING SUMMARY ===")
        print(f"Timestamp: {results['timestamp']}")
        print(f"Alerts Generated: {len(results['alerts'])}")
        
        if results['alerts']:
            print(f"Alert Types: {', '.join(results['alerts'])}")
        
        # Print check results
        if 'freshness' in results['checks']:
            freshness = results['checks']['freshness']
            print(f"\nData Freshness:")
            if 'ticker_data' in freshness:
                ticker = freshness['ticker_data']
                print(f"  Ticker Data: {'✅ Fresh' if ticker.get('is_fresh') else '❌ Stale'}")
                if ticker.get('hours_old'):
                    print(f"    Age: {ticker['hours_old']:.1f} hours")
            
            if 'macro_data' in freshness:
                macro = freshness['macro_data']
                print(f"  Macro Data: {'✅ Fresh' if macro.get('is_fresh') else '❌ Stale'}")
                if macro.get('hours_old'):
                    print(f"    Age: {macro['hours_old']:.1f} hours")
        
        if 'performance' in results['checks']:
            perf = results['checks']['performance']
            if 'batch_success_rate' in perf:
                print(f"\nBatch Performance:")
                print(f"  Success Rate: {perf['batch_success_rate']:.1%}")
                print(f"  Consecutive Failures: {perf.get('consecutive_failures', 0)}")
                print(f"  Status: {'✅ Healthy' if perf.get('is_healthy') else '❌ Issues'}")
        
        print(f"\nReport saved to: /app/data/monitoring_report.json")
        
        logger.info("Monitoring completed successfully")
        
        # Exit with error code if alerts were generated
        if results['alerts']:
            sys.exit(1)
        else:
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"Error in monitoring: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 