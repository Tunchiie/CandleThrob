# TA-Lib Installation Guide for CandleThrob

This comprehensive guide covers proper installation of TA-Lib (Technical Analysis Library) for the CandleThrob financial data pipeline.

## What is TA-Lib?

TA-Lib is a technical analysis library that provides 113+ financial indicators and candlestick pattern recognition functions. It consists of:

1. **C/C++ Core Library** - High-performance mathematical calculations
2. **Python Wrapper** - Python bindings for the core library

Both components must be installed correctly for TA-Lib to function.

## Common Installation Issues

### "Module 'talib' has no 'RSI' member"
This error typically indicates:
- TA-Lib C library is missing or corrupted
- Wrong Python package installed (`ta-lib` vs `TA-Lib`)
- Incomplete installation or import conflicts

### "No module named 'talib'"
This means the Python wrapper isn't installed or isn't in your Python path.

## Platform-Specific Installation

### Ubuntu/Debian (Recommended for Production)

**Method 1: Using pre-built package (Fastest)**
```bash
# Install dependencies
sudo apt-get update
sudo apt-get install -y build-essential wget

# Download and install TA-Lib C library
wget https://github.com/ta-lib/ta-lib/releases/download/v0.6.4/ta-lib_0.6.4_amd64.deb
sudo dpkg -i ta-lib_0.6.4_amd64.deb
sudo apt-get install -y -f

# Install Python wrapper
pip install TA-Lib==0.4.32
```

**Method 2: Building from source**
```bash
# Install build dependencies
sudo apt-get install -y build-essential libtool autoconf automake pkg-config python3-dev libffi-dev

# Download and build TA-Lib C library
wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
tar -xzf ta-lib-0.4.0-src.tar.gz
cd ta-lib/
./configure --prefix=/usr/local
make
sudo make install

# Update library path
sudo ldconfig

# Install Python wrapper
pip install TA-Lib==0.4.32
```

### macOS

**Using Homebrew (Recommended)**
```bash
# Install TA-Lib C library
brew install ta-lib

# Install Python wrapper
pip install TA-Lib==0.4.32
```

**Building from source**
```bash
# Install Xcode command line tools if not already installed
xcode-select --install

# Download and build TA-Lib
wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
tar -xzf ta-lib-0.4.0-src.tar.gz
cd ta-lib/
./configure --prefix=/usr/local
make
sudo make install

# Install Python wrapper
pip install TA-Lib==0.4.32
```

### Windows

**Method 1: Pre-compiled wheels (Easiest)**
```bash
# Download appropriate wheel from: https://www.lfd.uci.edu/~gohlke/pythonlibs/#ta-lib
# Example for Python 3.11, 64-bit:
pip install TA_Lib‑0.4.32‑cp311‑cp311‑win_amd64.whl
```

**Method 2: Using conda**
```bash
conda install -c conda-forge ta-lib
```

**Method 3: Building from source (Advanced)**
```bash
# Requires Visual Studio Build Tools
# Download TA-Lib C library source and follow Windows build instructions
# Then: pip install TA-Lib==0.4.32
```

## Docker Installation

For containerized environments:

```dockerfile
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install TA-Lib C library
RUN wget https://github.com/ta-lib/ta-lib/releases/download/v0.6.4/ta-lib_0.6.4_amd64.deb \
    && dpkg -i ta-lib_0.6.4_amd64.deb \
    && apt-get install -y -f \
    && rm ta-lib_0.6.4_amd64.deb

# Install Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt
```

## GitHub Actions CI/CD

For automated deployments and testing:

```yaml
name: Install TA-Lib
runs-on: ubuntu-latest

steps:
- name: Install TA-Lib Dependencies
  run: |
    sudo apt-get update
    sudo apt-get install -y build-essential wget libtool autoconf automake pkg-config python3-dev libffi-dev

- name: Install TA-Lib C Library
  run: |
    wget https://github.com/ta-lib/ta-lib/releases/download/v0.6.4/ta-lib_0.6.4_amd64.deb
    sudo dpkg -i ta-lib_0.6.4_amd64.deb
    sudo apt-get install -y -f

- name: Install Python Dependencies
  run: |
    pip install TA-Lib==0.4.32
    pip install -r requirements.txt
```

## Pipeline Flow

### Ingestion (9:00 PM - 2:00 AM EST)
1. **Polygon.io OHLCV data** → Oracle DB `ticker_data` table
2. **FRED macro data** → Oracle DB `macro_data` table

### Transformation (3:30 AM EST)
1. **Load raw data** from Oracle DB
2. **Calculate basic indicators** (always available)
3. **Calculate TA-Lib indicators** (if available)
4. **Save processed data** → Oracle DB `transformed_tickers` table

## Benefits of This Approach

✅ **Resilient**: Works with or without TA-Lib  
✅ **Comprehensive**: 113+ technical and candlestick pattern indicators when TA-Lib is available  
✅ **Fallback**: Basic indicators always calculated  
✅ **Production Ready**: Handles GitHub Actions environment  
✅ **Clean Separation**: Ingestion and transformation are separate  
✅ **Oracle DB Integration**: All data stored in centralized database with incremental loading

## Monitoring

Check the transformation logs to see which indicators were calculated:
- `✅ TA-Lib indicators were calculated` - Full indicator suite (113+ indicators)
- `⚠️ Only basic indicators calculated` - TA-Lib not available

The pipeline will continue to work in both scenarios, ensuring reliability even if TA-Lib installation fails.

## Technical Indicators Included

When TA-Lib is available, the system calculates:

### Momentum Indicators
- RSI, MACD, CCI, ROC, Williams %R, Rate of Change Ratio, etc.

### Volume Indicators  
- OBV, AD, Chaikin A/D Oscillator, MFI, VWAP, etc.

### Volatility Indicators
- ATR, Bollinger Bands, Standard Deviation, etc.

### Price Transform Indicators
- Typical Price, Weighted Close Price, Median Price, etc.

### Candlestick Pattern Recognition
- All major candlestick patterns (Doji, Hammer, Shooting Star, etc.)
- 61 different candlestick pattern indicators

### Custom Indicators
- Chaikin Money Flow
- Donchian Channels  
- Ulcer Index
- And many more...

All indicators are stored in the `transformed_tickers` table for further analysis and model building.

## Verification and Testing

### Test Installation
```python
# Test script to verify TA-Lib installation
import sys

try:
    import talib
    import numpy as np
    
    print("✅ TA-Lib imported successfully")
    print(f"✅ TA-Lib version: {talib.__version__}")
    
    # Test basic functionality
    test_data = np.random.randn(100).cumsum() + 100
    rsi = talib.RSI(test_data, timeperiod=14)
    
    if not np.isnan(rsi[-1]):
        print("✅ TA-Lib functions working correctly")
        print(f"✅ Sample RSI value: {rsi[-1]:.2f}")
    else:
        print("❌ TA-Lib functions not working properly")
        
except ImportError as e:
    print(f"❌ TA-Lib import failed: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ TA-Lib test failed: {e}")
    sys.exit(1)
```

### Available Functions
TA-Lib provides 158+ functions across categories:

**Momentum Indicators (30 functions)**
- RSI, MACD, CCI, ROC, Williams %R, Stochastic, etc.

**Volume Indicators (3 functions)**  
- OBV, AD, Chaikin A/D Oscillator

**Volatility Indicators (3 functions)**
- ATR, NATR, True Range

**Price Transform (4 functions)**
- Typical Price, Weighted Close Price, Median Price, Average Price

**Overlap Studies (17 functions)**
- Moving Averages (SMA, EMA, WMA, etc.), Bollinger Bands, KAMA, etc.

**Candlestick Patterns (61 functions)**
- Doji, Hammer, Shooting Star, Engulfing, etc.

**Cycle Indicators (5 functions)**
- Hilbert Transform functions

**Pattern Recognition (61 functions)**
- All major candlestick patterns

## Troubleshooting

### "ImportError: No module named '_ta_lib'"
```bash
# The C library isn't properly installed
# Reinstall both components:
sudo apt-get remove ta-lib* 
# Follow installation steps above again
```

### "Cannot import name 'MA_Type' from 'talib'"
```bash
# Version mismatch between C library and Python wrapper
pip uninstall TA-Lib
pip install --no-cache-dir TA-Lib==0.4.32
```

### "Library not loaded" (macOS)
```bash
# Add library path to environment
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
# Or permanently in ~/.bashrc or ~/.zshrc
```

### Windows compilation errors
- Use pre-compiled wheels instead of building from source
- Ensure Visual Studio Build Tools are installed if building from source
- Use conda-forge channel as alternative: `conda install -c conda-forge ta-lib`

### Permission errors during installation
```bash
# Use --user flag to install in user directory
pip install --user TA-Lib==0.4.32

# Or create virtual environment
python -m venv ta_lib_env
source ta_lib_env/bin/activate  # Linux/macOS
# ta_lib_env\Scripts\activate  # Windows
pip install TA-Lib==0.4.32
```

## Best Practices

### 1. **Pin Versions**
Always specify exact versions in `requirements.txt`:
```
TA-Lib==0.4.32
numpy>=1.21.0,<2.0.0
pandas>=1.3.0,<3.0.0
```

### 2. **Error Handling**
Implement graceful fallbacks in your code:
```python
try:
    import talib
    TALIB_AVAILABLE = True
except ImportError:
    TALIB_AVAILABLE = False
    # Implement manual calculations or skip advanced indicators
```

### 3. **Testing**
Include TA-Lib tests in your CI/CD pipeline:
```python
def test_talib_availability():
    try:
        import talib
        # Test basic functionality
        assert hasattr(talib, 'RSI')
        assert hasattr(talib, 'MACD')
    except ImportError:
        pytest.skip("TA-Lib not available")
```

### 4. **Performance**
- TA-Lib functions expect numpy arrays, not pandas Series
- Convert data types appropriately: `data.astype(np.float64)`
- Use vectorized operations when possible

### 5. **Data Preparation**
```python
# Proper data format for TA-Lib
import numpy as np
import pandas as pd

# Convert pandas Series to numpy array
close_prices = df['close'].astype(np.float64).values
high_prices = df['high'].astype(np.float64).values
low_prices = df['low'].astype(np.float64).values
volume = df['volume'].astype(np.float64).values

# Calculate indicators
rsi = talib.RSI(close_prices, timeperiod=14)
macd, macd_signal, macd_hist = talib.MACD(close_prices)
```

## Integration with CandleThrob

The CandleThrob pipeline uses TA-Lib in `ingestion/enrich_data.py`:

```python
# Check TA-Lib availability
try:
    import talib
    TALIB_AVAILABLE = True
    logger.info("✅ TA-Lib loaded successfully")
except ImportError as e:
    TALIB_AVAILABLE = False
    logger.warning(f"❌ TA-Lib not available: {e}")

# Calculate indicators with fallback
if TALIB_AVAILABLE:
    # Use full TA-Lib indicator suite (113+ indicators)
    indicators_df = calculate_talib_indicators(ticker_df)
else:
    # Use basic manual calculations
    indicators_df = calculate_basic_indicators(ticker_df)
```

### Data Flow in CandleThrob
1. **Raw Data Ingestion** → `fetch_data.py` fetches from Polygon.io/FRED
2. **Storage** → Oracle DB `ticker_data` and `macro_data` tables
3. **Enhancement** → `enrich_data.py` calculates technical indicators
4. **Persistence** → Oracle DB `transformed_tickers` table

### Technical Indicators in CandleThrob
When TA-Lib is available, CandleThrob calculates 113+ indicators:

**Momentum (30 indicators)**
- RSI, MACD, CCI, ROC, Stochastic, Williams %R, etc.

**Overlap Studies (17 indicators)**
- SMA, EMA, Bollinger Bands, KAMA, MAMA, etc.

**Volume (3 indicators)**
- OBV, AD, Chaikin A/D Oscillator

**Volatility (3 indicators)**
- ATR, NATR, True Range

**Price Transform (4 indicators)**
- AVGPRICE, MEDPRICE, TYPPRICE, WCLPRICE

**Candlestick Patterns (61 indicators)**
- All major patterns: Doji, Hammer, Engulfing, etc.

**Custom Indicators**
- Chaikin Money Flow
- Donchian Channels
- Ulcer Index

### Database Schema
All indicators are stored in `transformed_tickers` with:
- Base OHLCV data
- Return calculations (1d, 3d, 7d, 30d, 90d, 365d)
- All TA-Lib indicators as separate columns
- Metadata (created_at, updated_at)

This provides a comprehensive dataset for:
- Machine learning model training
- Backtesting trading strategies
- Technical analysis research
- Quantitative finance applications

### Fallback Strategy
If TA-Lib is unavailable, CandleThrob calculates basic indicators manually:
- Simple Moving Average (SMA)
- Exponential Moving Average (EMA)
- Relative Strength Index (RSI) 
- Bollinger Bands
- Basic return metrics

This ensures the pipeline remains functional even without TA-Lib installation.
