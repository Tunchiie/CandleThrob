# TA-Lib Installation and Usage Guide

## The Issue: Module 'talib' has no 'RSI' member

This error occurs because **TA-Lib requires both a C library and Python wrapper** to be installed correctly.

## Root Cause

The error `Module 'talib' has no 'RSI' member` typically means:

1. **TA-Lib C library is missing** - The underlying C library isn't installed
2. **Incorrect Python package** - You might have `ta-lib` instead of `TA-Lib`
3. **Import issues** - The module isn't loading properly

## Solution Implemented

### 1. **Added TA-Lib to requirements.txt**
```
TA-Lib==0.4.32
```

### 2. **Enhanced GitHub Actions Workflow**
The `.github/workflows/transform.yml` already includes TA-Lib installation:

```yaml
- name: Install Ta-Lib and Dependencies
  run: |
    sudo apt-get update
    sudo apt-get install -y build-essential wget libtool autoconf automake pkg-config python3-dev libffi-dev
    wget https://github.com/ta-lib/ta-lib/releases/download/v0.6.4/ta-lib_0.6.4_amd64.deb
    sudo dpkg -i ta-lib_0.6.4_amd64.deb
    sudo apt-get install -y -f
    pip install ta-lib pandas
    pip install -r requirements.txt
```

### 3. **Added Fallback Logic**
The code now includes proper error handling:

```python
# Try to import TA-Lib with fallback
try:
    import talib
    TALIB_AVAILABLE = True
    print("✅ TA-Lib imported successfully")
except ImportError as e:
    print(f"❌ Warning: TA-Lib not available: {e}")
    TALIB_AVAILABLE = False
```

### 4. **Graceful Degradation**
When TA-Lib isn't available, the system falls back to basic indicators:

- **Basic indicators**: RSI, SMA, EMA, Bollinger Bands (calculated manually)
- **TA-Lib indicators**: Full suite of 150+ technical indicators and candlestick patterns

## Installation Instructions

### For Development (Local)

**Ubuntu/Debian:**
```bash
sudo apt-get install libta-lib-dev
pip install TA-Lib
```

**macOS:**
```bash
brew install ta-lib
pip install TA-Lib
```

**Windows:**
```bash
# Download wheel from: https://www.lfd.uci.edu/~gohlke/pythonlibs/#ta-lib
pip install TA_Lib‑0.4.32‑cp311‑cp311‑win_amd64.whl
```

### For Production (GitHub Actions)
Already handled in the workflow files - no additional setup needed.

## File Changes Made

### 1. **requirements.txt**
- ✅ Added `TA-Lib==0.4.32`
- ✅ Removed `yfinance==0.2.48` (Polygon-only strategy)

### 2. **ingestion/transform_data.py**
- ✅ Complete rewrite with fallback logic
- ✅ Basic indicators when TA-Lib unavailable
- ✅ Full TA-Lib indicators when available
- ✅ Proper error handling and logging

### 3. **ingestion/enrich_data.py**
- ✅ Added TA-Lib availability checks
- ✅ Removed yfinance dependencies
- ✅ Simplified for Polygon-only strategy

## Pipeline Flow

### Ingestion (9:00 PM - 2:00 AM EST)
1. **Polygon.io OHLCV data** → `raw/tickers/` and `raw/etfs/`
2. **FRED macro data** → `raw/macros/`

### Transformation (3:30 AM EST)
1. **Load raw data** from GCS
2. **Calculate basic indicators** (always available)
3. **Calculate TA-Lib indicators** (if available)
4. **Save processed data** → `processed/tickers/` and `processed/etfs/`

## Benefits of This Approach

✅ **Resilient**: Works with or without TA-Lib  
✅ **Comprehensive**: 150+ indicators when TA-Lib is available  
✅ **Fallback**: Basic indicators always calculated  
✅ **Production Ready**: Handles GitHub Actions environment  
✅ **Clean Separation**: Ingestion and transformation are separate  

## Monitoring

Check the transformation logs to see which indicators were calculated:
- `✅ TA-Lib indicators were calculated` - Full indicator suite
- `⚠️ Only basic indicators calculated` - TA-Lib not available

The pipeline will continue to work in both scenarios, ensuring reliability even if TA-Lib installation fails.
