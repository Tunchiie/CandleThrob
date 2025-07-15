# ⏰ Kestra Timeout Configuration Guide

## 🎯 **OVERVIEW**

Kestra tasks have configurable timeout limits to prevent runaway processes. This guide explains timeout configuration and troubleshooting for CandleThrob flows.

## ⚠️ **COMMON TIMEOUT ERROR**

```
ERROR 2025-07-15T16:18:20.855965Z Timeout after 00:01:00.000
```

This indicates a task exceeded its 1-minute timeout limit.

## 📊 **CURRENT TIMEOUT SETTINGS**

### **Ingestion Flow Timeouts**

| Task | Timeout | Purpose | Rationale |
|------|---------|---------|-----------|
| `run_complete_ingestion` | **PT4H** | Main ingestion process | 500+ tickers with rate limiting |
| `check_ingestion_status` | **PT5M** | Status verification | File system checks + validation |
| `update_macro_data` | **PT30M** | Macro data update | FRED API calls + processing |
| `validate_data_quality` | **PT15M** | Data validation | Quality checks across all data |
| `cleanup_and_summary` | **PT2M** | Cleanup operations | File operations + logging |

### **Transformation Flow Timeouts**

| Task | Timeout | Purpose | Rationale |
|------|---------|---------|-----------|
| `transform_*_ticker_and_macro_data` | **PT2H** | Main transformation | 98+ indicators across 500+ tickers |
| `validate_*_transformation_results` | **PT10M** | Result validation | Quality checks on transformed data |
| `get_*_performance_metrics` | **PT5M** | Performance metrics | Statistical calculations |
| `generate_*_transformation_summary` | **PT2M** | Summary generation | Report creation + file operations |

## 🔧 **TIMEOUT FORMAT**

Kestra uses ISO 8601 duration format:

```yaml
timeout: PT2H30M15S  # 2 hours, 30 minutes, 15 seconds
timeout: PT4H        # 4 hours
timeout: PT30M       # 30 minutes
timeout: PT5M        # 5 minutes
timeout: PT2M        # 2 minutes
```

## 🚨 **TROUBLESHOOTING TIMEOUTS**

### **1. Identify the Timing Out Task**

Check Kestra UI execution logs:
1. Go to Executions tab
2. Click on failed execution
3. Look for the task with timeout error
4. Check task duration vs configured timeout

### **2. Common Timeout Causes**

#### **Database Connection Issues**
```yaml
# Symptoms: Quick timeout on database tasks
# Solution: Check Oracle connection and wallet configuration
env:
  TNS_ADMIN: "/opt/oracle/instantclient_23_8/network/admin"
  ORA_PYTHON_DRIVER_TYPE: "thick"
```

#### **Rate Limiting Delays**
```yaml
# Symptoms: Ingestion tasks timing out
# Solution: Adjust rate limiting or increase timeout
env:
  POLYGON_RATE_LIMIT: "5"  # 5 requests per minute
timeout: PT6H  # Increase for large datasets
```

#### **Large Dataset Processing**
```yaml
# Symptoms: Transformation tasks timing out
# Solution: Increase batch size or timeout
env:
  BATCH_SIZE: "50"  # Reduce for memory constraints
timeout: PT4H  # Increase for large datasets
```

### **3. Recommended Timeout Adjustments**

#### **For Development (Smaller Datasets)**
```yaml
# Ingestion
run_complete_ingestion: PT2H
check_ingestion_status: PT5M
update_macro_data: PT15M
validate_data_quality: PT10M

# Transformation
transform_dev_ticker_and_macro_data: PT1H
validate_dev_transformation_results: PT5M
```

#### **For Production (Full Datasets)**
```yaml
# Ingestion
run_complete_ingestion: PT6H
check_ingestion_status: PT5M
update_macro_data: PT45M
validate_data_quality: PT20M

# Transformation
transform_prod_ticker_and_macro_data: PT4H
validate_prod_transformation_results: PT15M
```

## 🔄 **RETRY CONFIGURATION**

Combine timeouts with retry logic:

```yaml
timeout: PT30M
retry:
  type: constant
  maxAttempt: 3
  interval: PT5M
```

This provides:
- 30-minute timeout per attempt
- 3 total attempts
- 5-minute wait between retries
- Total possible runtime: 1.5 hours

## 📊 **MONITORING RECOMMENDATIONS**

### **Task Duration Tracking**
```yaml
# Add performance logging to tasks
command: 
  - "sh"
  - "-c"
  - |
    echo "Task started at: $(date)"
    start_time=$(date +%s)
    
    # Your actual task command here
    python -m CandleThrob.ingestion.ingest_data
    
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    echo "Task completed in: ${duration} seconds"
```

### **Timeout Alerts**
Set up monitoring for:
- Tasks approaching timeout limits
- Frequent timeout failures
- Performance degradation trends

## 🚀 **OPTIMIZATION STRATEGIES**

### **1. Parallel Processing**
```yaml
# Split large tasks into parallel subtasks
- id: process_batch_1
  timeout: PT1H
- id: process_batch_2
  timeout: PT1H
- id: process_batch_3
  timeout: PT1H
```

### **2. Incremental Processing**
```yaml
# Process data in smaller increments
env:
  BATCH_SIZE: "25"  # Smaller batches
  INCREMENTAL_MODE: "true"
timeout: PT30M  # Shorter timeout per batch
```

### **3. Caching Strategies**
```yaml
# Use caching to reduce processing time
env:
  ENABLE_CACHE: "true"
  CACHE_TTL: "3600"  # 1 hour cache
timeout: PT15M  # Reduced timeout with caching
```

## ✅ **BEST PRACTICES**

1. **Set Realistic Timeouts**: Based on actual task duration + buffer
2. **Use Retry Logic**: For transient failures
3. **Monitor Performance**: Track task duration trends
4. **Optimize Code**: Reduce processing time where possible
5. **Parallel Processing**: Split large tasks when feasible
6. **Incremental Updates**: Process data in manageable chunks
7. **Caching**: Avoid redundant processing
8. **Resource Monitoring**: Ensure adequate CPU/memory

## 🔧 **IMMEDIATE ACTION**

Your timeout has been increased from PT1M to PT5M for status check tasks. If you continue to see timeouts:

1. **Check which specific task is timing out** in Kestra UI
2. **Review task logs** for performance bottlenecks
3. **Increase timeout** for the specific failing task
4. **Consider optimization** if timeouts persist

---

**Last Updated:** 2025-07-15  
**Status:** ✅ Status check timeouts increased to 5 minutes
