import pandas as pd
import os
import logging
from tqdm import tqdm
from datetime import datetime
from ingestion.fetch_data import DataIngestion


# Try to import talib, with fallback if not available
try:
    import talib
    TALIB_AVAILABLE = True
except ImportError as e:
    print(f"Warning: TA-Lib not available: {e}")
    print("Please install TA-Lib C library first, then pip install TA-Lib")
    TALIB_AVAILABLE = False

logging.basicConfig(
    filename="ingestion/enrich_debug.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class TechnicalIndicators:
    """
    A class to calculate technical indicators for stock data.
    This class uses TA-Lib to compute various indicators such as momentum, volume, pattern, volatility, price, cyclical, and statistical indicators.
    It also merges macroeconomic indicators from FRED into the stock data DataFrame.
    Attributes:
        ticker_df (pd.DataFrame): DataFrame containing stock data with columns like 'Open', 'High', 'Low', 'Close', 'Volume', and 'Ticker'.
        macro_df (pd.DataFrame): DataFrame containing macroeconomic indicators.
    """

    def __init__(self, ticker_df: pd.DataFrame):
        """
        Initialize the TechnicalIndicators class.

        Args:
            ticker_df (pd.DataFrame): DataFrame containing stock data.
            macro_df (pd.DataFrame): DataFrame containing macroeconomic indicators.
        """
        ticker_df["Date"] = pd.to_datetime(ticker_df["Date"], utc=True)
        self.ticker_df = ticker_df[ticker_df["Date"].dt.year >= 2000].copy()
        self.transformed_df = None
        
    def enrich_tickers(self):
        """ 
        Enrich the ticker data with basic return calculations.
        Note: Market cap, sector, and industry enrichment has been removed for Polygon-only strategy.
        This method calculates returns for various periods (1, 3, 7, 30, 90, and 365 days).
        Returns:
            None: The method updates the ticker DataFrame in place with additional return columns.
        """
        logger.info("Enriching data with return calculations...")
        
        if self.ticker_df is None or self.ticker_df.empty:
            raise ValueError("Ticker DataFrame is empty. Please run ingest_tickers() first.")
        
        # Calculate returns for various periods
        for i in [1, 3, 7, 30, 90, 365]:
            self.ticker_df[f"Return_{i}d"] = self.ticker_df.groupby("Ticker")["Close"]\
                .pct_change(periods=i).fillna(0)
        
        self.ticker_df["Date"] = pd.to_datetime(self.ticker_df["Date"])
        logger.info("Return calculations completed successfully.")

    def calculate_technical_indicators(self):
        """
        Calculate technical indicators for the stock data in the DataFrame.

        Returns:
            pd.DataFrame: DataFrame with technical indicators added.
        """
        if not TALIB_AVAILABLE:
            logger.error("TA-Lib is not available. Cannot calculate technical indicators.")
            raise ImportError("TA-Lib is required for technical indicators but is not installed.")
            
        logger.info("Calculating technical indicators...")
        # Ensure the DataFrame has the necessary columns
        required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in self.ticker_df.columns for col in required_columns):
            raise ValueError(f"DataFrame must contain columns: {required_columns}")
        self.transform()

        logger.info("Technical indicators calculated successfully.")
        return self.ticker_df
    
    def transform(self):
        """
        Transform the ticker DataFrame to calculate technical indicators using TA-Lib.
        This method calculates various indicators such as momentum, volume, pattern, volatility, price, cyclical, and statistical indicators.
        
        Returns:
            None: The method updates the ticker_df in place with the calculated indicators.
        """
        
        
        self.ticker_df["Volume"] = self.ticker_df["Volume"]*1.0
        self.ticker_df["Date"] = pd.to_datetime(self.ticker_df["Date"], utc=True)
        
        for key in ['Open', 'High', 'Low', 'Close', 'Volume']:
            self.ticker_df.loc[:,key] = self.ticker_df[key].astype('double')
            
        pd.options.mode.chained_assignment = None
        
        
        tickers = tqdm(self.ticker_df['Ticker'].unique(), desc="Calculating indicators")
        for ticker in tickers:
            current_ticker_df = self.ticker_df[self.ticker_df['Ticker'] == ticker]
            df_current_ticker_momentum_indicators = self.get_talib_momentum_indicators(current_ticker_df)
            df_current_ticker_volume_indicators = self.get_talib_volume_indicators(current_ticker_df)
            df_current_ticker_pattern_indicators = self.get_talib_pattern_indicators(current_ticker_df)
            df_current_ticker_volatility_indicators = self.get_talib_volatility_indicators(current_ticker_df)
            df_current_ticker_price_indicators = self.get_talib_price_indicators(current_ticker_df)
            df_current_ticker_cyclical_indicators = self.get_talib_cyclical_indicators(current_ticker_df)
            df_current_ticker_statistical_indicators = self.get_talib_statistical_indicators(current_ticker_df)
            
            
            current_ticker_df["Date"] = pd.to_datetime(current_ticker_df["Date"], utc=True)
            df_current_ticker_momentum_indicators["Date"] = pd.to_datetime(df_current_ticker_momentum_indicators["Date"], utc=True)
            df_current_ticker_volume_indicators["Date"] = pd.to_datetime(df_current_ticker_volume_indicators["Date"], utc=True)
            df_current_ticker_pattern_indicators["Date"] = pd.to_datetime(df_current_ticker_pattern_indicators["Date"], utc=True)
            df_current_ticker_volatility_indicators["Date"] = pd.to_datetime(df_current_ticker_volatility_indicators["Date"], utc=True)
            df_current_ticker_price_indicators["Date"] = pd.to_datetime(df_current_ticker_price_indicators["Date"], utc=True)
            df_current_ticker_cyclical_indicators["Date"] = pd.to_datetime(df_current_ticker_cyclical_indicators["Date"], utc=True)
            df_current_ticker_statistical_indicators["Date"] = pd.to_datetime(df_current_ticker_statistical_indicators["Date"], utc=True)
            
            self.transformed_df = self.safe_merge_or_concat(self.ticker_df, df_current_ticker_momentum_indicators, on=['Date', 'Ticker'], how='left')
            logger.info(f"Momentum Indicators for ticker %s calculated and merged successfully.", ticker)
            self.transformed_df = self.safe_merge_or_concat(self.transformed_df, df_current_ticker_volume_indicators, on=['Date', 'Ticker'], how='left')
            logger.info(f"Volume Indicators for ticker %s calculated and merged successfully.", ticker)
            self.transformed_df = self.safe_merge_or_concat(self.transformed_df, df_current_ticker_pattern_indicators, on=['Date', 'Ticker'], how='left')
            logger.info(f"Pattern Indicators for ticker %s calculated and merged successfully.", ticker)
            self.transformed_df = self.safe_merge_or_concat(self.transformed_df, df_current_ticker_volatility_indicators, on=['Date', 'Ticker'], how='left')
            logger.info(f"Volatility Indicators for ticker %s calculated and merged successfully.", ticker)
            self.transformed_df = self.safe_merge_or_concat(self.transformed_df, df_current_ticker_price_indicators, on=['Date', 'Ticker'], how='left')
            logger.info(f"Price Indicators for ticker %s calculated and merged successfully.", ticker)
            self.transformed_df = self.safe_merge_or_concat(self.transformed_df, df_current_ticker_cyclical_indicators, on=['Date', 'Ticker'], how='left')
            logger.info(f"Cyclical Indicators for ticker %s calculated and merged successfully.", ticker)
            self.transformed_df = self.safe_merge_or_concat(self.transformed_df, df_current_ticker_statistical_indicators, on=['Date', 'Ticker'], how='left')
            logger.info(f"Statistical Indicators for ticker %s calculated and merged successfully.", ticker)
            self.transformed_df = self.transformed_df.sort_values(by=['Date', 'Ticker']).reset_index(drop=True)
            logger.info(f"Indicators for ticker %s calculated and merged successfully.", ticker)

    def safe_merge_or_concat(self, df1: pd.DataFrame, df2: pd.DataFrame, on: list, how: str = 'left') -> pd.DataFrame:
        """
        Safely merge or concatenate two DataFrames, handling empty DataFrames.
        Args:
            df1 (pd.DataFrame): First DataFrame.
            df2 (pd.DataFrame): Second DataFrame.
            on (list): List of columns to merge on.
            how (str): Type of merge to perform ('left', 'right', 'inner', 'outer').
        Returns:    
            pd.DataFrame: Merged DataFrame.
        """
        if not isinstance(df1, pd.DataFrame) or not isinstance(df2, pd.DataFrame):
            logger.error("Both inputs must be pandas DataFrames. Returning the first DataFrame.")
            return df1 if isinstance(df1, pd.DataFrame) else df2 if isinstance(df2, pd.DataFrame) else pd.DataFrame()
        
        if df1.empty or df2.empty:
            logger.warning("One of the DataFrames is empty. Returning the non-empty DataFrame.")
            return df1 if not df1.empty else df2 if not df2.empty else pd.DataFrame()
        
        try:
            overlap = set(df1.columns).intersection(set(df2.columns)) - set(on)
            
            if overlap:
                # check if data is not being duplicated based on Ticker
                df1_tickers = set(df1['Ticker']) if 'Ticker' in df1.columns else set()
                df2_tickers = set(df2['Ticker']) if 'Ticker' in df2.columns else set()
                logger.info("Overlapping Tickers found: %s", df1_tickers.intersection(df2_tickers))
                if df1_tickers == df2_tickers:
                    logger.warning("Overlapping Tickers found in both DataFrames. Merging may lead to unexpected results.")
                    raise ValueError("Overlapping Tickers found in both DataFrames. Please check the data.")

                merged_df = pd.merge(df1, df2, on=on, how=how)
                logger.info(f"DataFrames merged successfully on %s with method %s.", on, how)
                return merged_df
            else:
                logger.info("No overlapping columns found except for the merge keys. Merging DataFrames.")
                merged_df = pd.merge(df1, df2, on=on, how=how)
                logger.info("DataFrames merged successfully.")
                return merged_df
        except Exception as e:
            logger.error(f"Error merging DataFrames: %s", e)
            return df1
    
    def get_talib_momentum_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate momentum indicators using TA-Lib.
        Args:
            df (pd.DataFrame): DataFrame containing stock data with columns like 'Open', 'High', 'Low', 'Close', etc.
        Returns:
            pd.DataFrame: DataFrame with momentum indicators added.
        """
        if not TALIB_AVAILABLE:
            logger.error("TA-Lib is not available. Returning empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'RSI', 'MACD', 'MACD_Signal', 'MACD_Hist', 
                                         'Stoch_K', 'Stoch_D', 'CCI', 'ROC', 
                                         'MOM', 'TRIX', 'WILLR'])
            
        if df.empty:
            logger.warning("Input DataFrame is empty. Returning an empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'RSI', 'MACD', 'MACD_Signal', 'MACD_Hist', 
                                         'Stoch_K', 'Stoch_D', 'CCI', 'ROC', 
                                         'MOM', 'TRIX', 'WILLR'])
            
        logger.info("Calculating TA-Lib indicators...")
        

        # Calculate indicators
        momentum_rsi = talib.RSI(df['Close'], timeperiod=14)
        momentum_macd, momentum_macdsignal, momentum_macdhist = talib.MACD(df['Close'], fastperiod=12, slowperiod=26, signalperiod=9)
        momentum_stoch_k, momentum_stoch_d = talib.STOCH(df['High'], df['Low'], df['Close'], fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)
        momentum_cci = talib.CCI(df['High'], df['Low'], df['Close'], timeperiod=14)
        momentum_roc = talib.ROC(df['Close'], timeperiod=10)
        momentum_mom = talib.MOM(df['Close'], timeperiod=10)
        momentum_trix = talib.TRIX(df['Close'], timeperiod=30)
        momentum_willr = talib.WILLR(df['High'], df['Low'], df['Close'], timeperiod=14)
        momentum_sma10 = talib.SMA(df['Close'], timeperiod=10)
        momentum_sma20 = talib.SMA(df['Close'], timeperiod=20)
        momentum_sma50 = talib.SMA(df['Close'], timeperiod=50)
        momentum_sma100 = talib.SMA(df['Close'], timeperiod=100)
        momentum_sma200 = talib.SMA(df['Close'], timeperiod=200)
        momentum_ema10 = talib.EMA(df['Close'], timeperiod=10)
        momentum_ema20 = talib.EMA(df['Close'], timeperiod=20)  
        momentum_ema50 = talib.EMA(df['Close'], timeperiod=50)
        momentum_ema100 = talib.EMA(df['Close'], timeperiod=100)
        momentum_ema200 = talib.EMA(df['Close'], timeperiod=200)
        

        momentum_df = pd.DataFrame({
            'Date': df['Date'],
            'Ticker': df['Ticker'],
            'RSI': momentum_rsi,
            'MACD': momentum_macd,
            'MACD_Signal': momentum_macdsignal,
            'MACD_Hist': momentum_macdhist,
            'Stoch_K': momentum_stoch_k,
            'Stoch_D': momentum_stoch_d,
            'CCI': momentum_cci,
            'ROC': momentum_roc,
            'MOM': momentum_mom,
            'TRIX': momentum_trix,
            'WILLR': momentum_willr,
            "SMA10": momentum_sma10,
            "SMA20": momentum_sma20,
            "SMA50": momentum_sma50,
            "SMA100": momentum_sma100,
            "SMA200": momentum_sma200,
            "EMA10": momentum_ema10,
            "EMA20": momentum_ema20,
            "EMA50": momentum_ema50,
            "EMA100": momentum_ema100,
            "EMA200": momentum_ema200
        })
        momentum_df = momentum_df.astype({'RSI': 'double', 'MACD': 'double', 'MACD_Signal': 'double',
                                           'MACD_Hist': 'double', 'Stoch_K': 'double', 
                                           'Stoch_D': 'double', 'CCI': 'double',
                                             'ROC': 'double',
                                             'MOM': 'double', 'TRIX': 'double',
                                             'WILLR': 'double', 'SMA10': 'double',
                                             'SMA20': 'double', 'SMA50': 'double',
                                             'SMA100': 'double', 'SMA200': 'double',
                                             'EMA10': 'double', 'EMA20': 'double',
                                             'EMA50': 'double', 'EMA100': 'double',
                                             'EMA200': 'double'})
        momentum_df['Date'] = pd.to_datetime(momentum_df['Date'], utc=True)
        logger.info("TA-Lib indicators calculated successfully.")
        return momentum_df
    
    def get_talib_volume_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate volume indicators using TA-Lib.
        Args:
            df (pd.DataFrame): DataFrame containing stock data with columns like 'Open', 'High', 'Low', 'Close', etc.
        Returns:
            pd.DataFrame: DataFrame with volume indicators added.
        """
        if not TALIB_AVAILABLE:
            logger.error("TA-Lib is not available. Returning empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'OBV', 'AD', 'MFI', 'ADOSC', 'CMF', 'VWAP', 'VPT', 'ADX', 'RVOL'])
            
        if df.empty:
            logger.warning("Input DataFrame is empty. Returning an empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'OBV', 'AD', 'ADOSC'])
        
        logger.info("Calculating volume indicators...")

        # Calculate indicators
        volume_obv = talib.OBV(df['Close'], df['Volume'])
        volume_ad = talib.AD(df['High'], df['Low'], df['Close'], df['Volume'])
        volume_mfi = talib.MFI(df['High'], df['Low'], df['Close'], df['Volume'], timeperiod=14)
        volume_adosc = talib.ADOSC(df['High'], df['Low'], df['Close'], df['Volume'], fastperiod=3, slowperiod=10)
        volume_cmf = self.chaikin_money_flow(df, period=20)
        volume_vwap = (df['Close'] * df['Volume']).cumsum() / df['Volume'].cumsum()
        volume_vwap = volume_vwap.fillna(0)
        volume_vwap = volume_vwap.astype('double')
        volume_vpt = (df['Close'].pct_change() * df['Volume']).fillna(0).cumsum()
        volume_adx = talib.ADX(df['High'], df['Low'], df['Close'], timeperiod=14)
        volume_rvol = df['Volume'] / df['Volume'].rolling(window=20).mean()
        volume_df = pd.DataFrame({
            'Date': df['Date'],
            'Ticker': df['Ticker'],
            'OBV': volume_obv,
            'AD': volume_ad,
            'MFI': volume_mfi,
            'ADOSC': volume_adosc,
            'CMF': volume_cmf,
            'VWAP': volume_vwap,
            'VPT': volume_vpt,
            'ADX': volume_adx,
            'RVOL': volume_rvol
        })
        volume_df = volume_df.fillna(0)
        volume_df = volume_df.astype({'OBV': 'double', 'AD': 'double', 'MFI': 'double', 
                                       'ADOSC': 'double', 'CMF': 'double', 
                                       'VWAP': 'double', 'VPT': 'double',
                                        'ADX': 'double', 'RVOL': 'double'})
        volume_df['Date'] = pd.to_datetime(volume_df['Date'], utc=True)
        logger.info("Volume indicators calculated successfully.")
        return volume_df
    
    def chaikin_money_flow(self, df: pd.DataFrame, period: int = 20) -> pd.Series:
        """
        Calculate Chaikin Money Flow (CMF) indicator.
        Args:
            df (pd.DataFrame): DataFrame containing stock data with columns 'High', 'Low', 'Close', 'Volume'.
            period (int): Period for the CMF calculation.
        Returns:
            pd.Series: Series with CMF values.
        """
        if df.empty:
            logger.warning("Input DataFrame is empty. Returning an empty Series.")
            return pd.Series(dtype=float)

        logger.info("Calculating Chaikin Money Flow (CMF) indicator...")
        
        ad = talib.AD(df['High'], df['Low'], df['Close'], df['Volume'])
        cmf = ad.rolling(window=period).sum() / df['Volume'].rolling(window=period).sum()
        
        logger.info("Chaikin Money Flow (CMF) indicator calculated successfully.")
        return cmf
    
    def donchian_channel(self, df: pd.DataFrame, period: int = 20) -> pd.Series:
        """
        Calculate Donchian Channel indicators.
        Args:
            df (pd.DataFrame): DataFrame containing stock data with columns 'High', 'Low', 'Close'.
            period (int): Period for the Donchian Channel.
        Returns:
            pd.Series: Series with Donchian Channel indicators.
        """
        if df.empty:
            logger.warning("Input DataFrame is empty. Returning an empty Series.")
            return pd.Series(dtype=float)

        logger.info("Calculating Donchian Channel indicators...")
        
        donch_upper = df['High'].rolling(window=period).max()
        donch_lower = df['Low'].rolling(window=period).min()
        

        logger.info("Donchian Channel indicators calculated successfully.")
        return donch_upper, donch_lower
    
    def ulcer_index(self, close: pd.Series, period: int = 14) -> pd.Series:
        """
        Calculate Ulcer Index.
        Args:
            close (pd.Series): Series of closing prices.
            period (int): Period for the Ulcer Index calculation.
        Returns:
            pd.Series: Series with Ulcer Index values.
        """
        if close.empty:
            logger.warning("Input Series is empty. Returning an empty Series.")
            return pd.Series(dtype=float)

        logger.info("Calculating Ulcer Index...")
        
        ulcer_index = ((close.rolling(window=period).max() - close) / close.rolling(window=period).max()) ** 2
        
        logger.info("Ulcer Index calculated successfully.")
        return ulcer_index
    
    
    def get_talib_volatility_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate volatility indicators using TA-Lib.
        Args:
            df (pd.DataFrame): DataFrame containing stock data with columns like 'Open', 'High', 'Low', 'Close', etc.
        Returns:
            pd.DataFrame: DataFrame with volatility indicators added.
        """
        if not TALIB_AVAILABLE:
            logger.error("TA-Lib is not available. Returning empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'ATR', 'NATR', 'TRANGE', 'BBANDS_UPPER', 'BBANDS_MIDDLE', 'BBANDS_LOWER', 'ULCER_INDEX', 'DONCH_UPPER', 'DONCH_LOWER'])
            
        if df.empty:
            logger.warning("Input DataFrame is empty. Returning an empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'ATR', 'NATR', 'TRANGE'])
        
        logger.info("Calculating volatility indicators...")

        volatility_atr = talib.ATR(df['High'], df['Low'], df['Close'], timeperiod=14)
        volatility_bbands_upper, volatility_bbands_middle, volatility_bbands_lower = talib.BBANDS(df['Close'], timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
        volatility_donch_upper, volatility_donch_lower = self.donchian_channel(df, period=20)
        volatility_natr = talib.NATR(df['High'], df['Low'], df['Close'], timeperiod=14)
        volatility_trange = talib.TRANGE(df['High'], df['Low'], df['Close'])
        volatility_ulcer_index = self.ulcer_index(df['Close'], period=14)

        volatility_df = pd.DataFrame({
            'Date': df['Date'],
            'Ticker': df['Ticker'],
            'ATR': volatility_atr,
            'NATR': volatility_natr,
            'TRANGE': volatility_trange,
            'BBANDS_UPPER': volatility_bbands_upper,
            'BBANDS_MIDDLE': volatility_bbands_middle,
            'BBANDS_LOWER': volatility_bbands_lower,
            'ULCER_INDEX': volatility_ulcer_index,
            'DONCH_UPPER': volatility_donch_upper,
            'DONCH_LOWER': volatility_donch_lower
        })
        volatility_df = volatility_df.astype({'ATR': 'double', 'NATR': 'double', 'TRANGE': 'double',
                                               'BBANDS_UPPER': 'double', 'BBANDS_MIDDLE': 'double',
                                               'BBANDS_LOWER': 'double', 'ULCER_INDEX': 'double',
                                               'DONCH_UPPER': 'double', 'DONCH_LOWER': 'double'})
        volatility_df["Date"] = pd.to_datetime(volatility_df["Date"], utc=True)

        logger.info("Volatility indicators calculated successfully.")
        return volatility_df
    def get_talib_pattern_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate pattern indicators using TA-Lib.
        Args:
            df (pd.DataFrame): DataFrame containing stock data with columns like 'Open', 'High', 'Low', 'Close', etc.
        Returns:
            pd.DataFrame: DataFrame with pattern indicators added.
        """
        if not TALIB_AVAILABLE:
            logger.error("TA-Lib is not available. Returning empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'CDLDOJI', 'CDLHAMMER', 'CDLHANGINGMAN'])
            
        if df.empty:
            logger.warning("Input DataFrame is empty. Returning an empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'CDLDOJI', 'CDLHAMMER', 'CDLHANGINGMAN'])
        
        logger.info("Calculating pattern indicators...")

        # Calculate indicators
        pattern_cd12crows = talib.CDL2CROWS(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cd3blackcrows = talib.CDL3BLACKCROWS(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cd3inside = talib.CDL3INSIDE(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cd3linestrike = talib.CDL3LINESTRIKE(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cd3outside = talib.CDL3OUTSIDE(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cd3starsinsouth = talib.CDL3STARSINSOUTH(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cd3whitesoldiers = talib.CDL3WHITESOLDIERS(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlabandonedbaby = talib.CDLABANDONEDBABY(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlbelthold = talib.CDLBELTHOLD(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlbreakaway = talib.CDLBREAKAWAY(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlclosingmarubozu = talib.CDLCLOSINGMARUBOZU(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlconcealbabyswall = talib.CDLCONCEALBABYSWALL(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlcounterattack = talib.CDLCOUNTERATTACK(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdldarkcloudcover = talib.CDLDARKCLOUDCOVER(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdldoji = talib.CDLDOJI(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdldojistar = talib.CDLDOJISTAR(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlengulfing = talib.CDLENGULFING(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdleveningstar = talib.CDLEVENINGSTAR(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlgravestonedoji = talib.CDLGRAVESTONEDOJI(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlhammer = talib.CDLHAMMER(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlhangingman = talib.CDLHANGINGMAN(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlharami = talib.CDLHARAMI(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlharamicross = talib.CDLHARAMICROSS(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlhighwave = talib.CDLHIGHWAVE(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlhikkake = talib.CDLHIKKAKE(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlhikkakemod = talib.CDLHIKKAKEMOD(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlhomingpigeon = talib.CDLHOMINGPIGEON(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlidentical3crows = talib.CDLIDENTICAL3CROWS(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlinneck = talib.CDLINNECK(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlinvertedhammer = talib.CDLINVERTEDHAMMER(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlladderbottom = talib.CDLLADDERBOTTOM(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdllongleggeddoji = talib.CDLLONGLEGGEDDOJI(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdllongline = talib.CDLLONGLINE(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlmarubozu = talib.CDLMARUBOZU(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlmatchinglow = talib.CDLMATCHINGLOW(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlmathold = talib.CDLMATHOLD(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlmorningdojistar = talib.CDLMORNINGDOJISTAR(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlmorningstar = talib.CDLMORNINGSTAR(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlonneck = talib.CDLONNECK(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlpiercing = talib.CDLPIERCING(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlrickshawman = talib.CDLRICKSHAWMAN(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlrisefall3methods = talib.CDLRISEFALL3METHODS(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlseparatinglines = talib.CDLSEPARATINGLINES(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlshootingstar = talib.CDLSHOOTINGSTAR(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlshortline = talib.CDLSHORTLINE(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlspinningtop = talib.CDLSPINNINGTOP(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlstalledpattern = talib.CDLSTALLEDPATTERN(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlsticksandwich = talib.CDLSTICKSANDWICH(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdltakuri = talib.CDLTAKURI(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdltasukigap = talib.CDLTASUKIGAP(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlthrusting = talib.CDLTHRUSTING(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdltristar = talib.CDLTRISTAR(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlunique3river = talib.CDLUNIQUE3RIVER(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlxsidegap3methods = talib.CDLXSIDEGAP3METHODS(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_df = pd.DataFrame({
            'Date': df['Date'],
            'Ticker': df['Ticker'],
            'CDL2CROWS': pattern_cd12crows,
            'CDL3BLACKCROWS': pattern_cd3blackcrows,
            'CDL3INSIDE': pattern_cd3inside,
            'CDL3LINESTRIKE': pattern_cd3linestrike,
            'CDL3OUTSIDE': pattern_cd3outside,
            'CDL3STARSINSOUTH': pattern_cd3starsinsouth,
            'CDL3WHITESOLDIERS': pattern_cd3whitesoldiers,
            'CDLABANDONEDBABY': pattern_cdlabandonedbaby,
            'CDLBELTHOLD': pattern_cdlbelthold,
            'CDLBREAKAWAY': pattern_cdlbreakaway,
            'CDLCLOSINGMARUBOZU': pattern_cdlclosingmarubozu,
            'CDLCONCEALBABYSWALL': pattern_cdlconcealbabyswall,
            'CDLCOUNTERATTACK': pattern_cdlcounterattack,
            'CDLDARKCLOUDCOVER': pattern_cdldarkcloudcover,
            'CDLDOJI': pattern_cdldoji,
            'CDLDOJISTAR': pattern_cdldojistar,
            'CDLENGULFING': pattern_cdlengulfing,
            'CDLEVENINGSTAR': pattern_cdleveningstar,
            'CDLGRAVESTONEDOJI': pattern_cdlgravestonedoji,
            'CDLHAMMER': pattern_cdlhammer,
            'CDLHANGINGMAN': pattern_cdlhangingman,
            'CDLHARAMI': pattern_cdlharami,
            'CDLHARAMICROSS': pattern_cdlharamicross,
            'CDLHIGHWAVE': pattern_cdlhighwave,
            'CDLHIKKAKE': pattern_cdlhikkake,
            'CDLHIKKAKEMOD': pattern_cdlhikkakemod,
            'CDLHOMINGPIGEON': pattern_cdlhomingpigeon,
            'CDLIDENTICAL3CROWS': pattern_cdlidentical3crows,
            'CDLINNECK': pattern_cdlinneck,
            'CDLINVERTEDHAMMER': pattern_cdlinvertedhammer,
            'CDLLADDERBOTTOM': pattern_cdlladderbottom,
            'CDLLONGLEGGEDDOJI': pattern_cdllongleggeddoji,
            'CDLLONGLINE': pattern_cdllongline,
            'CDLMARUBOZU': pattern_cdlmarubozu,
            'CDLMATCHINGLOW': pattern_cdlmatchinglow,
            'CDLMATHOLD': pattern_cdlmathold,
            'CDLMORNINGDOJISTAR': pattern_cdlmorningdojistar,
            'CDLMORNINGSTAR': pattern_cdlmorningstar,
            'CDLONNECK': pattern_cdlonneck,
            'CDLPIERCING': pattern_cdlpiercing,
            'CDLRICKSHAWMAN': pattern_cdlrickshawman,
            'CDLRISEFALL3METHODS': pattern_cdlrisefall3methods,
            'CDLSEPARATINGLINES': pattern_cdlseparatinglines,
            'CDLSHOOTINGSTAR': pattern_cdlshootingstar,
            'CDLSHORTLINE': pattern_cdlshortline,
            'CDLSPINNINGTOP': pattern_cdlspinningtop,
            'CDLSTALLEDPATTERN': pattern_cdlstalledpattern,
            'CDLSTICKSANDWICH': pattern_cdlsticksandwich,
            'CDLTAKURI': pattern_cdltakuri,
            'CDLTASUKIGAP': pattern_cdltasukigap,
            'CDLTHRUSTING': pattern_cdlthrusting,
            'CDLTRISTAR': pattern_cdltristar,
            'CDLUNIQUE3RIVER': pattern_cdlunique3river,
            'CDLXSIDEGAP3METHODS': pattern_cdlxsidegap3methods
        })

        pattern_df = pattern_df.astype({
            'CDL2CROWS': 'double', 'CDL3BLACKCROWS': 'double', 'CDL3INSIDE': 'double',
            'CDL3LINESTRIKE': 'double', 'CDL3OUTSIDE': 'double', 'CDL3STARSINSOUTH': 'double',
            'CDL3WHITESOLDIERS': 'double', 'CDLABANDONEDBABY': 'double',
            'CDLBELTHOLD': 'double', 'CDLBREAKAWAY': 'double', 'CDLCLOSINGMARUBOZU': 'double',
            'CDLCONCEALBABYSWALL': 'double', 'CDLCOUNTERATTACK': 'double', 'CDLDARKCLOUDCOVER': 'double',
            'CDLDOJI': 'double', 'CDLDOJISTAR': 'double', 'CDLENGULFING': 'double', 'CDLEVENINGSTAR': 'double',
            'CDLGRAVESTONEDOJI': 'double', 'CDLHAMMER': 'double', 'CDLHANGINGMAN': 'double', 'CDLHARAMI': 'double',
            'CDLHARAMICROSS': 'double', 'CDLHIGHWAVE': 'double', 'CDLHIKKAKE': 'double', 'CDLHIKKAKEMOD': 'double',
            'CDLHOMINGPIGEON': 'double', 'CDLIDENTICAL3CROWS': 'double', 'CDLINNECK': 'double',
            'CDLINVERTEDHAMMER': 'double', 'CDLLADDERBOTTOM': 'double', 'CDLLONGLEGGEDDOJI': 'double',
            'CDLLONGLINE': 'double', 'CDLMARUBOZU': 'double', 'CDLMATCHINGLOW': 'double', 'CDLMATHOLD': 'double',
            'CDLMORNINGDOJISTAR': 'double', 'CDLMORNINGSTAR': 'double', 'CDLONNECK': 'double', 'CDLPIERCING': 'double',
            'CDLRICKSHAWMAN': 'double', 'CDLRISEFALL3METHODS': 'double', 'CDLSEPARATINGLINES': 'double',
            'CDLSHOOTINGSTAR': 'double', 'CDLSHORTLINE': 'double', 'CDLSPINNINGTOP': 'double',
            'CDLSTALLEDPATTERN': 'double', 'CDLSTICKSANDWICH': 'double', 'CDLTAKURI': 'double',
            'CDLTASUKIGAP': 'double', 'CDLTHRUSTING': 'double', 'CDLTRISTAR': 'double',
            'CDLUNIQUE3RIVER': 'double', 'CDLXSIDEGAP3METHODS': 'double'
        })
        pattern_df["Date"] = pd.to_datetime(pattern_df["Date"], utc=True)
            
        logger.info("Pattern indicators calculated successfully.")
        return pattern_df
    
    def get_talib_price_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate price indicators.
        Args:
            df (pd.DataFrame): DataFrame containing stock data with columns 'Open', 'High', 'Low', 'Close'.
        Returns:
            pd.DataFrame: DataFrame with price indicators added.
        """
        if not TALIB_AVAILABLE:
            logger.error("TA-Lib is not available. Returning empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'Midprice', 'Medprice', 'Typprice', 'Wclprice', 'Avgprice'])
            
        if df.empty:
            logger.warning("Input DataFrame is empty. Returning an empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'Price_Change', 'Price_Change_Percent'])
        
        logger.info("Calculating price indicators...")
        

        midprice_indicator = talib.MIDPRICE(df['High'], df['Low'], timeperiod=14)
        medprice_indicator = talib.MEDPRICE(df['High'], df['Low'])
        typprice_indicator = talib.TYPPRICE(df['High'], df['Low'], df['Close'])
        wclprice_indicator = talib.WCLPRICE(df['High'], df['Low'], df['Close'])
        avgprice_indicator = talib.AVGPRICE(df['Open'], df['High'], df['Low'], df['Close'])

        price_indicators = pd.DataFrame({
            'Date': df['Date'],
            'Ticker': df['Ticker'],
            'Midprice': midprice_indicator,
            'Medprice': medprice_indicator,
            'Typprice': typprice_indicator,
            'Wclprice': wclprice_indicator,
            'Avgprice': avgprice_indicator
        })

        price_indicators = price_indicators.astype({'Midprice': 'double', 'Medprice': 'double',
                                                     'Typprice': 'double', 'Wclprice': 'double',
                                                     'Avgprice': 'double'})
        price_indicators["Date"] = pd.to_datetime(price_indicators["Date"], utc=True)

        logger.info("Price indicators calculated successfully.")
        return price_indicators
    
    def get_talib_cyclical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate cycle indicators using TA-Lib.
        Args:
            df (pd.DataFrame): DataFrame containing stock data with columns like 'Open', 'High', 'Low', 'Close', etc.
        Returns:
            pd.DataFrame: DataFrame with cycle indicators added.
        """
        if not TALIB_AVAILABLE:
            logger.error("TA-Lib is not available. Returning empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'HT_TRENDLINE', 'HT_SINE', 'HT_SINE_LEAD', 'HT_DCPERIOD', 'HT_DCPHASE'])
            
        if df.empty:
            logger.warning("Input DataFrame is empty. Returning an empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'HT_TRENDLINE', 'HT_SINE', 'HT_DCPERIOD', 'HT_DCPHASE'])
        
        logger.info("Calculating cycle indicators...")

        ht_trendline = talib.HT_TRENDLINE(df['Close'])
        ht_sine, ht_sine_lead, = talib.HT_SINE(df['Close'])
        ht_dcperiod = talib.HT_DCPERIOD(df['Close'])
        ht_dcphase = talib.HT_DCPHASE(df['Close'])

        cycle_df = pd.DataFrame({
            'Date': df['Date'],
            'Ticker': df['Ticker'],
            'HT_TRENDLINE': ht_trendline,
            'HT_SINE': ht_sine,
            'HT_SINE_LEAD': ht_sine_lead,
            'HT_DCPERIOD': ht_dcperiod,
            'HT_DCPHASE': ht_dcphase
        })

        cycle_df = cycle_df.astype({'HT_TRENDLINE': 'double', 'HT_SINE': 'double',
                                     'HT_SINE_LEAD': 'double',
                                     'HT_DCPERIOD': 'double', 'HT_DCPHASE': 'double'})
        cycle_df["Date"] = pd.to_datetime(cycle_df["Date"], utc=True)

        logger.info("Cycle indicators calculated successfully.")
        return cycle_df
    
    def get_talib_statistical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate statistical indicators using TA-Lib.
        Args:
            df (pd.DataFrame): DataFrame containing stock data with columns like 'Open', 'High', 'Low', 'Close', etc.
        Returns:
            pd.DataFrame: DataFrame with statistical indicators added.
        """
        if not TALIB_AVAILABLE:
            logger.error("TA-Lib is not available. Returning empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'STDDEV', 'VAR', 'BETA_VS_SP500', 'ZSCORE_PRICE_NORMALIZED'])
            
        if df.empty:
            logger.warning("Input DataFrame is empty. Returning an empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'STDDEV', 'VAR'])
        
        logger.info("Calculating statistical indicators...")
        

        stddev_indicator = talib.STDDEV(df['Close'], timeperiod=20, nbdev=1)
        var_indicator = talib.VAR(df['Close'], timeperiod=20, nbdev=1)
        beta_vs_sp500 = talib.BETA(df['Close'], df['Close'].rolling(window=20).mean(), timeperiod=20)
        zscore_price_normalized = (df['Close'] - df['Close'].rolling(window=20).mean()) / df['Close'].rolling(window=20).std()

        statistical_df = pd.DataFrame({
            'Date': df['Date'],
            'Ticker': df['Ticker'],
            'STDDEV': stddev_indicator,
            'VAR': var_indicator,
            'BETA_VS_SP500': beta_vs_sp500,
            'ZSCORE_PRICE_NORMALIZED': zscore_price_normalized
        })

        statistical_df = statistical_df.astype({'STDDEV': 'double', 'VAR': 'double'})
        statistical_df["Date"] = pd.to_datetime(statistical_df["Date"], utc=True)

        logger.info("Statistical indicators calculated successfully.")
        return statistical_df

    def persist_transformed_data(self, file_path: str) -> None:
        """
        Save DataFrame to a Parquet file.
        Args:
            df (pd.DataFrame): DataFrame to save.
            file_path (str): Path to the Parquet file.
        """
        if self.transformed_df is None or self.transformed_df.empty:
            logger.warning("DataFrame is empty. No data to save.")
            return
        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path))
        try:
            self.transformed_df.to_parquet(file_path, index=False)
            logger.info(f"DataFrame saved to {file_path} successfully.")
        except Exception as e:
            logger.error(f"Error saving DataFrame to Parquet: %s", e)
            
class EnrichMacros:
    """
    Class to enrich macroeconomic data for analysis.
    """
    def __init__(self, macro_df: pd.DataFrame = None):
        """
        Initialize the EnrichMacros class with macroeconomic data.
        This class fetches macroeconomic indicators from the FRED API and transforms them for further analysis.
        """
        logger.info("Initializing EnrichMacros with macroeconomic data.")
        if macro_df is not None and not macro_df.empty and 'Date' in macro_df.columns:
            self.macro_df = macro_df[macro_df['Date'].dt.year >= 2000]
        else:
            self.macro_df = pd.DataFrame()
        self.end_date = datetime.now()
        self.start_date = (datetime.now() - pd.DateOffset(years=50))
        self.transformed_df = None
        

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
        
        # Only apply transformations to columns that exist
        available_cols = self.macro_df.columns.tolist()
        
        # Lag available columns
        lag_cols = [col for col in ['GDP', 'UNRATE', 'UMCSENT', 'CPIAUCSL'] if col in available_cols]
        if lag_cols:
            self.lag_macro_data(lag_cols, lag_periods=[30, 60, 90])
        
        # Rolling mean for available columns  
        rolling_cols = [col for col in ['FEDFUNDS', 'INDPRO'] if col in available_cols]
        if rolling_cols:
            self.rolling_macro_data(rolling_cols, window=[30, 90])
        
        # Percentage change for available columns
        pct_cols = [col for col in ['GDP', 'UMCSENT', 'RSAFS'] if col in available_cols]
        if pct_cols:
            self.pct_change_macro_data(pct_cols, periods=[90])
        
        # Z-score for available columns
        zscore_cols = [col for col in ['UNRATE', 'CPIAUCSL'] if col in available_cols]
        if zscore_cols:
            self.z_score_macro_data(zscore_cols, window=90)
        
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
            if col not in self.macro_df.columns:
                logger.warning("Column %s not found in macro DataFrame. Skipping.", col)
                continue
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
        