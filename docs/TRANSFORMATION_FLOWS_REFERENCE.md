# 🔧 CandleThrob Transformation Flows Reference

## 📊 **OVERVIEW**

The CandleThrob pipeline includes multiple transformation flows designed for different environments and use cases. Each flow processes both **ticker data** (OHLCV) and **macro data** (economic indicators) with comprehensive technical analysis.

## 🎯 **AVAILABLE TRANSFORMATION FLOWS**

### **1. Development Flow: `transform_data_dev`**

**Purpose:** Testing and development with detailed logging
**Location:** `CandleThrob/flows/transform_data_dev.yml` & `flows/transform_data_dev.yml`

```yaml
Container Image: candlethrob:dev
Environment: Development (embedded wallet)
Batch Size: 25 (smaller for testing)
Log Level: DEBUG
Trigger: Manual only
Container Names: candlethrob_dev_transform_{{ execution.id }}
```

**Key Features:**
- ✅ Embedded Oracle wallet (no external mounting)
- ✅ Detailed DEBUG logging for troubleshooting
- ✅ Smaller batch sizes for faster testing
- ✅ Manual trigger only (no automatic execution)
- ✅ Development-optimized configuration

### **2. Production Flow: `transform_data_production`**

**Purpose:** Production environment with optimized performance
**Location:** `CandleThrob/flows/transform_data.yml`

```yaml
Container Image: candlethrob:latest
Environment: Production (mounted wallet)
Batch Size: 100 (optimized for production)
Log Level: INFO
Trigger: Automatic after ingestion
Container Names: candlethrob_prod_transform_{{ execution.id }}
```

**Key Features:**
- ✅ External Oracle wallet mounting for security
- ✅ Production-level INFO logging
- ✅ Large batch sizes for efficiency
- ✅ Automatic trigger after ingestion completion
- ✅ Production-optimized configuration

### **3. Latest Flow: `transform_data_latest`**

**Purpose:** Latest configuration with balanced settings
**Location:** `flows/transform_data.yml`

```yaml
Container Image: candlethrob:latest
Environment: Latest (mounted wallet)
Batch Size: 50 (balanced)
Log Level: INFO
Trigger: Configurable
Container Names: candlethrob_latest_transform_{{ execution.id }}
```

## 📈 **DATA PROCESSING SCOPE**

### **Ticker Data Processing:**
- **Input:** Raw OHLCV data from ingestion
- **Processing:** 98+ technical indicators via TA-Lib
- **Output:** Enriched ticker data with technical analysis
- **Indicators Include:**
  - Moving averages (SMA, EMA, WMA)
  - Momentum indicators (RSI, MACD, Stochastic)
  - Volatility indicators (Bollinger Bands, ATR)
  - Volume indicators (OBV, Volume SMA)
  - Pattern recognition indicators

### **Macro Data Processing:**
- **Input:** Economic indicators and market data
- **Processing:** Statistical analysis and trend calculation
- **Output:** Processed macroeconomic indicators
- **Data Types:**
  - Interest rates
  - Inflation metrics
  - Employment data
  - GDP indicators
  - Market sentiment data

## 🔧 **TASK STRUCTURE**

Each transformation flow includes these standardized tasks:

1. **Start Task:** `start_[env]_transformation`
   - Logs the beginning of transformation
   - Environment-specific messaging

2. **Validation Task:** `validate_[env]_ingestion_completion`
   - Validates ingestion completion
   - Ensures data availability

3. **Main Transform Task:** `transform_[env]_ticker_and_macro_data`
   - Core transformation processing
   - Handles both ticker and macro data
   - Environment-specific configuration

4. **Validation Task:** `validate_[env]_transformation_results`
   - Validates transformation output
   - Ensures data quality

5. **Metrics Task:** `get_[env]_performance_metrics`
   - Collects performance metrics
   - Monitors execution statistics

6. **Summary Task:** `generate_[env]_transformation_summary`
   - Generates execution summary
   - Provides transformation overview

7. **End Task:** `end_[env]_transformation`
   - Logs completion
   - Final status confirmation

## 🚀 **USAGE EXAMPLES**

### **Development Testing:**
```bash
# Manual execution in Kestra UI
Flow: transform_data_dev
Namespace: candlethrob.transform
Trigger: Manual execution
```

### **Production Deployment:**
```bash
# Automatic execution after ingestion
Flow: transform_data_production
Namespace: candlethrob.transform
Trigger: After ingest_data_daily completion
```

## 📊 **MONITORING & DEBUGGING**

### **Container Logs:**
```bash
# Development
docker logs candlethrob_dev_transform_[execution_id]

# Production
docker logs candlethrob_prod_transform_[execution_id]
```

### **Performance Metrics:**
- Transformation duration
- Records processed
- Indicators calculated
- Error rates
- Memory usage

## 🔧 **CONFIGURATION OPTIONS**

### **Environment Variables:**
```yaml
BATCH_SIZE: Processing batch size
LOG_LEVEL: Logging verbosity
PYTHONPATH: Python module path
TNS_ADMIN: Oracle client configuration
ORA_PYTHON_DRIVER_TYPE: Oracle driver type
```

### **Volume Mounts:**
```yaml
# Production (external wallet)
- "/home/ubuntu/stockbot/Wallet_candleThroblake:/opt/oracle/instantclient_23_8/network/admin:ro"

# Development (embedded wallet)
# No external mounts required
```

## ✅ **BEST PRACTICES**

1. **Use Development Flow** for testing and debugging
2. **Use Production Flow** for live data processing
3. **Monitor container logs** for troubleshooting
4. **Check performance metrics** for optimization
5. **Validate transformation results** before proceeding
6. **Use predictable container names** for monitoring

---

**Last Updated:** 2025-07-15
**Version:** 3.0.0
**Author:** CandleThrob Development Team
