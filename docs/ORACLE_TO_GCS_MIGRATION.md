# Oracle to Google Cloud Storage Migration Guide

This guide explains how to migrate CandleThrob from Oracle Database to Google Cloud Storage (GCS) for improved scalability, cost-effectiveness, and flexibility.

## Overview

### Why Migrate to GCS?

1. **Cost Efficiency** - No database licensing fees or compute costs when idle
2. **Scalability** - Virtually unlimited storage capacity
3. **Flexibility** - Store data in multiple formats (Parquet, CSV, JSON)
4. **Performance** - Fast parallel reads/writes with proper partitioning
5. **Integration** - Native integration with Google Cloud ML and analytics tools
6. **Backup & Recovery** - Built-in versioning and geo-redundancy

### Architecture Changes

**Before (Oracle):**
```
Data Sources → CandleThrob → Oracle Database → Analysis Tools
```

**After (GCS):**
```
Data Sources → CandleThrob → Google Cloud Storage → Analysis Tools
                                     ↓
                              (Parquet Files)
```

## Prerequisites

### 1. Google Cloud Setup

1. **Create GCS Bucket:**
   ```bash
   gsutil mb gs://candlethrob-candata
   ```

2. **Set up Service Account:**
   ```bash
   # Create service account
   gcloud iam service-accounts create candlethrob-sa \
     --description="CandleThrob service account" \
     --display-name="CandleThrob"

   # Grant storage permissions
   gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
     --member="serviceAccount:candlethrob-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/storage.admin"

   # Create and download key
   gcloud iam service-accounts keys create ~/.gcp/service-account.json \
     --iam-account=candlethrob-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com
   ```

3. **Configure Local Environment:**
   ```bash
   # Create GCP credentials directory
   mkdir -p ~/.gcp
   
   # Set environment variable
   export GOOGLE_APPLICATION_CREDENTIALS="~/.gcp/service-account.json"
   ```

### 2. Update CandleThrob Configuration

The migration includes:
- ✅ **GCS Client Library** - Added `google-cloud-storage` to requirements
- ✅ **GCS Data Models** - New models in `CandleThrob/utils/gcs_models.py`
- ✅ **Migration Script** - Automated migration in `CandleThrob/scripts/migrate_oracle_to_gcs.py`
- ✅ **Docker Configuration** - Updated with GCS credentials mounting

## Migration Process

### Phase 1: Parallel Operation (Recommended)

Run both Oracle and GCS in parallel to ensure data integrity:

1. **Start with Mock Mode (Testing):**
   ```python
   from CandleThrob.utils.gcs import GCSConfig
   config = GCSConfig(enable_mock_mode=True)
   ```

2. **Enable Real GCS:**
   ```python
   config = GCSConfig(enable_mock_mode=False)
   ```

3. **Test GCS Connection:**
   ```bash
   docker compose run --rm candlethrob python -c "
   from CandleThrob.utils.gcs import test_gcs_connection
   print('GCS Connection:', test_gcs_connection('candlethrob-candata'))
   "
   ```

### Phase 2: Data Migration

1. **Run Migration Script:**
   ```bash
   # Test with limited data first
   docker compose run --rm candlethrob python CandleThrob/scripts/migrate_oracle_to_gcs.py \
     --table TICKERS --limit 100

   # Migrate all ticker data
   docker compose run --rm candlethrob python CandleThrob/scripts/migrate_oracle_to_gcs.py \
     --table TICKERS

   # Migrate macro data
   docker compose run --rm candlethrob python CandleThrob/scripts/migrate_oracle_to_gcs.py \
     --table MACRO_DATA

   # Migrate transformed data
   docker compose run --rm candlethrob python CandleThrob/scripts/migrate_oracle_to_gcs.py \
     --table TRANSFORMED_TICKERS

   # Or migrate everything at once
   docker compose run --rm candlethrob python CandleThrob/scripts/migrate_oracle_to_gcs.py \
     --all-tables
   ```

2. **Verify Migration:**
   ```bash
   # Check bucket contents
   gsutil ls -r gs://candlethrob-candata/

   # Check specific data
   docker compose run --rm candlethrob python -c "
   from CandleThrob.utils.gcs_models import load_ticker_data
   df = load_ticker_data('AAPL')
   print(f'Loaded {len(df)} AAPL records' if df is not None else 'No data found')
   "
   ```

### Phase 3: Update Application Code

1. **Update Ingestion Scripts:**
   ```python
   # Old Oracle approach
   from CandleThrob.utils.models import TickerData
   ticker_model = TickerData()
   ticker_model.insert_data(conn, df)

   # New GCS approach
   from CandleThrob.utils.gcs_models import save_ticker_data
   save_ticker_data(df, ticker='AAPL')
   ```

2. **Update Analysis Scripts:**
   ```python
   # Old Oracle approach
   from CandleThrob.utils.models import TickerData
   with db.establish_connection() as conn:
       df = ticker_model.get_data(conn, ticker='AAPL')

   # New GCS approach
   from CandleThrob.utils.gcs_models import load_ticker_data
   df = load_ticker_data('AAPL')
   ```

## GCS Data Organization

### Folder Structure
```
candlethrob-candata/
├── raw/
│   ├── tickers/
│   │   ├── AAPL_20250123.parquet
│   │   ├── GOOGL_20250123.parquet
│   │   └── ...
│   ├── etfs/
│   │   ├── SPY_20250123.parquet
│   │   └── ...
│   └── macros/
│       ├── FEDFUNDS_20250123.parquet
│       ├── GDP_20250123.parquet
│       └── ...
├── processed/
│   ├── tickers/
│   │   ├── AAPL_20250123.parquet  # With technical indicators
│   │   └── ...
│   └── etfs/
│       └── ...
└── analysis/
    ├── backtests/
    ├── models/
    └── reports/
```

### File Naming Convention
- **Raw Data:** `{symbol}_{YYYYMMDD}.parquet`
- **Processed Data:** `{symbol}_{YYYYMMDD}.parquet`
- **Analysis:** `{analysis_type}_{YYYYMMDD}.parquet`

## Performance Optimization

### 1. Parquet Format Benefits
- **Compression:** 80-90% size reduction vs CSV
- **Columnar Storage:** Fast analytical queries
- **Schema Evolution:** Add columns without breaking existing data
- **Type Safety:** Preserves data types

### 2. Partitioning Strategy
```python
# Date-based partitioning
path = f"raw/tickers/year={year}/month={month}/day={day}/{ticker}.parquet"

# Symbol-based partitioning
path = f"raw/tickers/{ticker}/year={year}/{ticker}_{date}.parquet"
```

### 3. Batch Operations
```python
# Upload multiple files at once
from CandleThrob.utils.gcs import batch_upload_dataframes

dataframes = {
    'AAPL': aapl_df,
    'GOOGL': googl_df,
    'MSFT': msft_df
}

results = batch_upload_dataframes('candlethrob-candata', dataframes, 'raw/tickers')
```

## Cost Optimization

### 1. Storage Classes
- **Standard:** Frequently accessed data (recent data)
- **Nearline:** Monthly access (older data)
- **Coldline:** Quarterly access (historical data)
- **Archive:** Yearly access (long-term storage)

### 2. Lifecycle Policies
```json
{
  "rule": [
    {
      "action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
      "condition": {"age": 30}
    },
    {
      "action": {"type": "SetStorageClass", "storageClass": "COLDLINE"},
      "condition": {"age": 90}
    }
  ]
}
```

## Monitoring and Maintenance

### 1. Health Monitoring
```python
from CandleThrob.utils.gcs import get_gcs_health_status, get_gcs_cache_stats

# Check GCS operation health
health = get_gcs_health_status()
print(f"GCS Health: {health['status']}")
print(f"Success Rate: {health['success_rate']:.2%}")

# Check cache performance
cache_stats = get_gcs_cache_stats()
if cache_stats:
    print(f"Cache Hit Rate: {cache_stats['cache_hit_rate']:.2%}")
```

### 2. Data Validation
```python
# Verify data integrity after migration
from CandleThrob.utils.gcs_models import GCSTickerData

gcs_model = GCSTickerData()
available_tickers = gcs_model.list_available_tickers()
print(f"Available tickers in GCS: {len(available_tickers)}")
```

## Rollback Plan

If issues arise, you can rollback to Oracle:

1. **Keep Oracle Running:** Don't decommission Oracle immediately
2. **Dual Write:** Write to both Oracle and GCS during transition
3. **Validation Period:** Run parallel for 30 days minimum
4. **Gradual Cutover:** Move services one by one

## Next Steps

1. **✅ Set up GCS bucket and credentials**
2. **✅ Test GCS connection**
3. **✅ Run migration script with limited data**
4. **✅ Verify migrated data**
5. **✅ Update application code**
6. **✅ Run parallel for validation period**
7. **✅ Complete cutover**
8. **✅ Decommission Oracle (after validation)**

## Support

For issues during migration:
1. Check logs: `docker compose logs candlethrob`
2. Verify GCS permissions: `gsutil ls gs://candlethrob-candata`
3. Test connection: Use the test functions in `gcs.py`
4. Review migration logs in the migration script output
