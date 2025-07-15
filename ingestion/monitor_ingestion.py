#!/usr/bin/env python3
"""
Ingestion Monitoring Script for CandleThrob
=========================================================
Monitors data ingestion performance, data quality, and sends alerts for issues.

Key Features:
- Centralized, structured logging
- Performance and resource monitoring
- Advanced error handling and retry logic
- Modular, testable functions with type hints
- Parameterized configuration and thresholds
- Audit trail and metrics aggregation
- Data quality validation and alerting
- Security best practices

Author: CandleThrob Team
Version: 2.0.0
"""

import sys
import os
import json
import smtplib
import time
import psutil
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from functools import wraps
import traceback

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from CandleThrob.utils.models import TickerData, MacroData
from CandleThrob.utils.oracle_conn import OracleDB
from CandleThrob.utils.logging_config import get_ingestion_logger, log_ingestion_error

# Centralized, structured logger
logger = get_ingestion_logger("monitor_ingestion")

def retry_on_failure(max_retries: int = 3, delay_seconds: float = 1.0):
    """retry decorator with exponential backoff."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = delay_seconds * (2 ** attempt)
                        logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}. Retrying in {delay:.2f}s...")
                        time.sleep(delay)
                    else:
                        logger.error(f"All {max_retries + 1} attempts failed for {func.__name__}: {str(e)}")
                        raise last_exception
            return None
        return wrapper
    return decorator

class AuditTrail:
    """audit trail for monitoring actions."""
    def __init__(self):
        self.entries: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
    def log_event(self, event_type: str, details: Dict[str, Any], success: bool = True):
        with self._lock:
            self.entries.append({
                "event_type": event_type,
                "details": details,
                "success": success,
                "timestamp": datetime.now().isoformat()
            })
    def get_audit_summary(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "total_events": len(self.entries),
                "successful_events": sum(1 for e in self.entries if e["success"]),
                "failed_events": sum(1 for e in self.entries if not e["success"]),
                "events": self.entries,
                "timestamp": datetime.now().isoformat()
            }

def monitor_performance(operation: str):
    """Decorator for performance and resource monitoring."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            start_memory = psutil.Process().memory_info().rss / 1024 / 1024
            try:
                result = func(*args, **kwargs)
                success = True
                error_message = None
            except Exception as e:
                success = False
                error_message = str(e)
                raise
            finally:
                end_time = time.time()
                end_memory = psutil.Process().memory_info().rss / 1024 / 1024
                logger.info(json.dumps({
                    "operation": operation,
                    "duration": end_time - start_time,
                    "memory_mb": end_memory - start_memory,
                    "success": success,
                    "error": error_message,
                    "timestamp": datetime.now().isoformat()
                }))
            return result
        return wrapper
    return decorator

class IngestionMonitor:
    """
    monitor for data ingestion performance and health.
    Features:
    - Centralized logging and audit trail
    - Performance/resource monitoring
    - Data quality validation
    - Modular, testable methods
    """
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.db = OracleDB()
        self.engine = self.db.get_sqlalchemy_engine()
        self.audit_trail = AuditTrail()
        self.config = config or {
            "data_freshness_hours": 24,
            "success_rate_threshold": 0.9,
            "batch_failure_threshold": 3,
            "data_gap_threshold": 5
        }

    @monitor_performance("check_data_freshness")
    @retry_on_failure(max_retries=2, delay_seconds=2.0)
    def check_data_freshness(self) -> Dict[str, Any]:
        """Check if data is fresh and up-to-date."""
        logger.info("Checking data freshness...")
        try:
            with self.db.establish_connection() as conn:
                latest_ticker_date = conn.execute("SELECT MAX(trade_date) FROM ticker_data").scalar()
                latest_macro_date = conn.execute("SELECT MAX(date) FROM macro_data").scalar()
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
                    ticker_freshness["is_fresh"] = ticker_freshness["hours_old"] <= self.config["data_freshness_hours"]
                if latest_macro_date:
                    macro_freshness["hours_old"] = (current_time - latest_macro_date).total_seconds() / 3600
                    macro_freshness["is_fresh"] = macro_freshness["hours_old"] <= self.config["data_freshness_hours"]
                result = {
                    "ticker_data": ticker_freshness,
                    "macro_data": macro_freshness,
                    "check_timestamp": current_time.isoformat()
                }
                self.audit_trail.log_event("check_data_freshness", result, success=True)
                return result
        except Exception as e:
            logger.error(f"Error checking data freshness: {str(e)}")
            self.audit_trail.log_event("check_data_freshness", {"error": str(e)}, success=False)
            return {"status": "error", "message": str(e)}

    @monitor_performance("check_batch_performance")
    @retry_on_failure(max_retries=2, delay_seconds=2.0)
    def check_batch_performance(self) -> Dict[str, Any]:
        """Check batch processing performance from recent runs."""
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
                                batch_time = datetime.fromisoformat(batch_data.get("timestamp", "2000-01-01"))
                                if batch_time > datetime.now() - timedelta(hours=24):
                                    batch_results.append(batch_data)
                        except Exception as e:
                            logger.warning(f"Error reading {filename}: {str(e)}")
            if not batch_results:
                result = {
                    "status": "no_recent_batches",
                    "message": "No batch results found in last 24 hours"
                }
                self.audit_trail.log_event("check_batch_performance", result, success=True)
                return result
            total_batches = len(batch_results)
            successful_batches = sum(1 for b in batch_results if b.get("results", {}).get("failed", 0) == 0)
            total_tickers = sum(b.get("results", {}).get("total_tickers", 0) for b in batch_results)
            total_successful = sum(b.get("results", {}).get("successful", 0) for b in batch_results)
            success_rate = successful_batches / total_batches if total_batches > 0 else 0
            ticker_success_rate = total_successful / total_tickers if total_tickers > 0 else 0
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
                "is_healthy": (success_rate >= self.config["success_rate_threshold"] and \
                              consecutive_failures < self.config["batch_failure_threshold"])
            }
            self.audit_trail.log_event("check_batch_performance", performance_metrics, success=True)
            return performance_metrics
        except Exception as e:
            logger.error(f"Error checking batch performance: {str(e)}")
            self.audit_trail.log_event("check_batch_performance", {"error": str(e)}, success=False)
            return {"status": "error", "message": str(e)}

    @monitor_performance("check_data_quality")
    @retry_on_failure(max_retries=2, delay_seconds=2.0)
    def check_data_quality(self) -> Dict[str, Any]:
        """Check data quality issues."""
        logger.info("Checking data quality...")
        try:
            with self.db.establish_connection() as conn:
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
                    HAVING COUNT(*) > :gap_threshold
                    ORDER BY gap_count DESC
                """
                data_gaps = conn.execute(gaps_query, {"gap_threshold": self.config["data_gap_threshold"]}).fetchall()
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
                self.audit_trail.log_event("check_data_quality", quality_issues, success=True)
                return quality_issues
        except Exception as e:
            logger.error(f"Error checking data quality: {str(e)}")
            self.audit_trail.log_event("check_data_quality", {"error": str(e)}, success=False)
            return {"status": "error", "message": str(e)}

    def generate_alert(self, alert_type: str, message: str, details: Dict[str, Any]) -> None:
        """Send alert (placeholder for integration with email/SMS/Slack)."""
        logger.warning(f"ALERT [{alert_type}]: {message} | Details: {details}")
        self.audit_trail.log_event("generate_alert", {"type": alert_type, "message": message, "details": details}, success=True)

    def run_monitoring_checks(self) -> Dict[str, Any]:
        """Run all monitoring checks and aggregate results."""
        logger.info("Running all monitoring checks...")
        results = {}
        freshness = self.check_data_freshness()
        results["data_freshness"] = freshness
        if not freshness.get("ticker_data", {}).get("is_fresh", True):
            self.generate_alert("data_freshness", "Ticker data is stale", freshness)
        if not freshness.get("macro_data", {}).get("is_fresh", True):
            self.generate_alert("data_freshness", "Macro data is stale", freshness)
        batch_perf = self.check_batch_performance()
        results["batch_performance"] = batch_perf
        if not batch_perf.get("is_healthy", True):
            self.generate_alert("batch_performance", "Batch performance is unhealthy", batch_perf)
        quality = self.check_data_quality()
        results["data_quality"] = quality
        if quality.get("has_issues", False):
            self.generate_alert("data_quality", "Data quality issues detected", quality)
        results["audit_trail"] = self.audit_trail.get_audit_summary()
        logger.info(json.dumps({"monitoring_summary": results, "timestamp": datetime.now().isoformat()}))
        return results

def main():
    """Entry point for running the monitoring script."""
    logger.info("Starting ingestion monitoring...")
    monitor = IngestionMonitor()
    results = monitor.run_monitoring_checks()
    print(json.dumps(results, indent=2, default=str))

if __name__ == "__main__":
    main() 