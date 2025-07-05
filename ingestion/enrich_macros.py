import logging
import os
import pandas as pd
from fredapi import Fred
from datetime import datetime

logging.basicConfig(
    filename="debug.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class EnrichMacros:
    """
    Class to enrich macroeconomic data for analysis.
    """
    def __init__(self):
        """
        Initialize the EnrichMacros class with macroeconomic data.
        This class fetches macroeconomic indicators from the FRED API and transforms them for further analysis.
        """
        logger.info("Initializing EnrichMacros with macroeconomic data.")
        self.macro_df = None
        self.end_date = datetime.now()
        self.start_date = (datetime.now() - pd.DateOffset(years=50))
        self.fetch()
        self.transformed_df = None
        

    def fetch(self):
        """ 
        Fetch economic data from the Federal Reserve Economic Data (FRED) API.
        This method retrieves various economic indicators such as GDP, unemployment rate, inflation rate, and interest rates.
        It stores the data in a DataFrame and merges it with the ticker DataFrame based on the date.
        Returns:
            None: The method updates the ticker DataFrame in place with additional columns for economic indicators.
        Raises:
            Exception: If the FRED API key is not set in the environment variables.
        """
        
        if not os.getenv("FRED_API_KEY"):
            raise RuntimeError("FRED API key is not set. Please set the FRED_API_KEY environment variable.")

        fred_api = Fred(os.getenv("FRED_API_KEY"))
        fred_series = {
            "FEDFUNDS": "Federal_Funds_Rate",
            "CPIAUCSL": "Consumer_Price_Index",
            "UNRATE": "Unemployment_Rate",
            "GDP": "Gross_Domestic_Product",
            "GS10": "10-Year_Treasury_Rate",
            "USREC": "Recession_Indicator",
            "UMCSENT": "Consumer_Sentiment",
            "HOUST": "Housing_Starts",
            "RSAFS": "Retail_Sales",
            "INDPRO": "Industrial_Production_Index",
            "M2SL": "M2_Money_Supply",
        }

        fred_data = {}
        for code, name in fred_series.items():
            try:
                logger.info("Fetching FRED data for %s (%s) from %s to %s", name, code, self.start_date, self.end_date)
                fred_data[code] = fred_api.get_series(code, start_date=self.start_date.strftime("%Y-%m-%d"), end_date=self.end_date.strftime("%Y-%m-%d"))
            except Exception as e:
                logger.error("Error fetching FRED data for %s (%s): %s", name, code, e)
        fred_df = pd.DataFrame(fred_data)
        if fred_df.empty:
            raise ValueError("No data returned from FRED API. Check your API key and internet connection.")

        fred_df.index = pd.to_datetime(fred_df.index)
        fred_df = fred_df.resample('D').mean().ffill().bfill()
        fred_df.reset_index(names='Date', inplace=True)
        fred_df['Date'] = fred_df['Date'].dt.normalize()

        if self.macro_df is not None and not self.macro_df.empty:
            self.macro_df = self.macro_df.merge(fred_df, on='Date', how='left')
        else:
            self.macro_df = fred_df        
        logger.info("FRED data fetched and stored successfully.")


    def transform_macro_data(self):
        """
        Transform macroeconomic data to a format suitable for merging with ticker data.
        This method will ensure that the macroeconomic data is in a consistent format.
        """
        if self.macro_df.empty:
            raise ValueError("Macro DataFrame is empty. Cannot transform empty DataFrame.")

        # Sort by date
        self.macro_df.sort_index(inplace=True)
        self.transformed_df = self.macro_df.copy()
        # Rename columns to match FRED series names
        self.lag_macro_data(['GDP', 'UNRATE', 'UMCSENT', 'CPIAUCSL'], lag_periods=[30, 60, 90])
        self.rolling_macro_data(['FEDFUNDS', 'INDPRO'], window=[30, 90])
        self.pct_change_macro_data(['GDP', 'UMCSENT', 'RSAFS'], periods=[90])
        self.z_score_macro_data(['UNRATE', 'CPIAUCSL'], window=90)
        logger.info("Transformed macroeconomic data to a consistent format.")

    def lag_macro_data(self, cols: list, lag_periods: list = [30, 60, 90]):
        """
        Lag the macroeconomic data by a specified number of periods.
        This is useful for aligning macroeconomic indicators with stock data.
        
        Args:
            lag_periods (list): List of periods to lag the data.
        """
        if self.macro_df is None or cols is None:
            logger.error("Macro DataFrame or columns to lag are not initialized.")
            return

        for col in cols:
            for period in lag_periods:
                if period <= 0:
                    raise ValueError("Lag period must be a positive integer.")
                lagged_df = self.macro_df[col].shift(period)
                lagged_df.name = f"{col}_lagged_{period}"
                self.transformed_df = pd.concat([self.transformed_df, lagged_df], axis=1)
        logger.info("Macro data lagged by %s periods.", lag_periods)

    def rolling_macro_data(self, cols: list = [], window: list = [3, 6, 12]):
        """
        Apply rolling mean to the macroeconomic data.
        
        Args:
            window (list): The sizes of the moving window.
        """
        if self.macro_df is None:
            logger.error("Macro DataFrame is not initialized.")
            return
        for col in cols:
            if col not in self.macro_df.columns:
                logger.warning("Column %s not found in macro DataFrame. Skipping rolling mean calculation for this column.", col)
                continue
            for w in window:
                if w <= 0:
                    raise ValueError("Window size must be a positive integer.")
                rolling_df = self.macro_df[col].rolling(window=w).mean()
                rolling_df.name = f"{col}_rolling_{w}"
                self.transformed_df = pd.concat([self.transformed_df, rolling_df], axis=1)
        logger.info("Applied rolling mean with window size %s to macro data.", window)

    def pct_change_macro_data(self, cols: list = [], periods: list = [1, 2, 3]):
        """
        Calculate percentage change for the macroeconomic data.
        This is useful for understanding the relative change in macroeconomic indicators.
        Args:
            periods (list): List of periods for which to calculate percentage change.
        """
        if self.macro_df is None:
            logger.error("Macro DataFrame is not initialized.")
            return
        # Go through each column and calculate percentage change
        for col in cols:
            if col not in self.macro_df.columns:
                logger.warning("Column %s not found in macro DataFrame. Skipping percentage change calculation for this column.", col)
                continue
            for period in periods:
                if period <= 0:
                    raise ValueError("Period must be a positive integer.")
                pct_change_df = self.macro_df[col].pct_change(periods=period)
                pct_change_df.name = f"{col}_pct_change"
                self.transformed_df = pd.concat([self.transformed_df, pct_change_df], axis=1)
        logger.info("Calculated percentage change with periods %s for macro data.", periods)

    def z_score_macro_data(self, cols: list = [], window: int = 90):
        """
        Calculate the z-score for the macroeconomic data.
        This is useful for standardizing the macroeconomic indicators.
        """
        if self.macro_df is None:
            logger.error("Macro DataFrame is not initialized.")
            return
        
        if window <= 0:
            raise ValueError("Window size must be a positive integer.")
        for col in cols:
            if col not in self.macro_df.columns:
                logger.warning("Column %s not found in macro DataFrame. Skipping z-score calculation for this column.", col)
                continue
            roll = self.macro_df[col].rolling(window=window)
            z_score_df = (self.macro_df[col] - roll.mean()) / roll.std()
            z_score_df.name = f"{col}_z_score"
            self.transformed_df = pd.concat([self.transformed_df, z_score_df], axis=1)
        logger.info("Calculated z-score for macro data.")
        
    def persist_transformed_data(self, path: str = "data/macro_data.parquet"):
        """
        Save the transformed macroeconomic data to a parquet file.
        
        Args:
            path (str): The file path where the transformed data will be saved.
        """
        if self.transformed_df is None:
            logger.error("Transformed DataFrame is empty. Cannot save to file.")
            return
        
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        self.transformed_df.to_parquet(path, index=False)
        logger.info("Transformed macroeconomic data saved to %s", path)
        