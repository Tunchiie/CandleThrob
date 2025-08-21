# CandleThrob Jupyter Lab Setup

This guide explains how to use Jupyter Lab with CandleThrob for interactive data analysis and machine learning.

## Quick Start

### 1. Start Jupyter Lab
```bash
# Start Jupyter Lab service
docker compose --profile dev up jupyter

# Or start in detached mode
docker compose --profile dev up -d jupyter
```

### 2. Access Jupyter Lab
- Open your browser and go to: **http://localhost:8888**
- No token required (development mode)

### 3. Directory Structure
```
CandleThrob/
├── notebooks/          # Existing Jupyter notebooks
│   ├── 01_INITIAL_EDA.ipynb
│   ├── 02_VENV_ORACLE_ACCESS.ipynb
│   ├── 03_REALTIME_DEMO.ipynb
│   ├── Init_EDA.ipynb
│   └── 04_SCIKIT_LEARN_ANALYSIS.ipynb  # Auto-created
├── analysis/           # Your analysis work
└── experiments/        # ML experiments and prototypes
```

## Available Libraries

### Core Data Science Stack
- **pandas** - Data manipulation and analysis
- **numpy** - Numerical computing
- **scikit-learn** - Machine learning library
- **matplotlib** - Plotting and visualization
- **seaborn** - Statistical data visualization
- **plotly** - Interactive visualizations

### Financial Data Libraries
- **yfinance** - Yahoo Finance data
- **fredapi** - Federal Reserve Economic Data
- **polygon-api-client** - Polygon.io market data
- **TA-Lib** - Technical analysis indicators

### CandleThrob Integration
- **CandleThrob.utils.models** - Database models
- **CandleThrob.utils.database** - Database connections
- **CandleThrob.transform** - Data transformation utilities

## Example Usage

### Basic Setup in Notebook
```python
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import sys
sys.path.append('/app')

# Import CandleThrob modules
from CandleThrob.utils.models import TickerData, TransformedTickerData
from CandleThrob.utils.database import DatabaseManager
```

### Load Data from Database
```python
# Connect to database and load ticker data
db = DatabaseManager()
with db.establish_connection() as conn:
    ticker_model = TransformedTickerData()
    df = ticker_model.get_data(conn, ticker='AAPL')
    print(f"Loaded {len(df)} records for AAPL")
```

### Machine Learning Example
```python
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score

# Prepare features and target
features = ['rsi', 'macd', 'sma20', 'volume']
X = df[features].dropna()
y = df['close_price'].loc[X.index]

# Train model
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
model = RandomForestRegressor()
model.fit(X_train, y_train)

# Evaluate
predictions = model.predict(X_test)
print(f"R² Score: {r2_score(y_test, predictions):.4f}")
```

## Tips

### 1. Live Code Reloading
The CandleThrob source code is mounted as a volume, so changes to Python files are immediately available in Jupyter.

### 2. Database Access
All Oracle database connections work the same as in the main application. The wallet and credentials are automatically configured.

### 3. Saving Work
- Notebooks are automatically saved in `CandleThrob/notebooks/`
- Save analysis scripts in `CandleThrob/analysis/`
- Save ML experiments in `CandleThrob/experiments/`

### 4. Stopping Jupyter
```bash
# Stop the Jupyter service
docker compose --profile dev down jupyter

# Or stop all services
docker compose down
```

## Troubleshooting

### Port Already in Use
If port 8888 is already in use, modify the port mapping in `docker-compose.yml`:
```yaml
ports:
  - "8889:8888"  # Use port 8889 instead
```

### Oracle Connection Issues
Ensure the wallet files are properly mounted and the database is accessible. Check the logs:
```bash
docker compose logs jupyter
```

### Package Installation
To install additional packages, add them to `CandleThrob/requirements.txt` and rebuild:
```bash
docker build -f Dockerfile.dev -t candlethrob:dev .
```
