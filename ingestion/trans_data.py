import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import talib
import pandas as pd
import os
from ingestion.fetch_data import DataIngestion
import logging
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TechnicalIndicators(DataIngestion):
    """
    A class to calculate technical indicators for stock data.

    Attributes:
        ticker_df (pd.DataFrame): DataFrame containing stock data with columns like 'Open', 'High', 'Low', 'Close', etc.
    """

    def __init__(self, ticker_df: pd.DataFrame, macro_df: pd.DataFrame):
        super().__init__(ticker_df)
        self.ticker_df = ticker_df
        self.macro_df = macro_df

    def calculate_technical_indicators(self):
        """
        Calculate technical indicators for the stock data in the DataFrame.

        Returns:
            pd.DataFrame: DataFrame with technical indicators added.
        """
        logger.info("Calculating technical indicators...")
        # Ensure the DataFrame has the necessary columns
        required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in self.ticker_df.columns for col in required_columns):
            raise ValueError(f"DataFrame must contain columns: {required_columns}")
        self.transform()
        self.merge_macro_indicators()

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
            
            
            current_ticker_df = current_ticker_df.merge(df_current_ticker_momentum_indicators, on=['Date', 'Ticker'], how='left')
            current_ticker_df = current_ticker_df.merge(df_current_ticker_volume_indicators, on=['Date', 'Ticker'], how='left')
            current_ticker_df = current_ticker_df.merge(df_current_ticker_pattern_indicators, on=['Date', 'Ticker'], how='left')
            current_ticker_df = current_ticker_df.merge(df_current_ticker_volatility_indicators, on=['Date', 'Ticker'], how='left')
            current_ticker_df = current_ticker_df.merge(df_current_ticker_price_indicators, on=['Date', 'Ticker'], how='left') 
            self.ticker_df.update(current_ticker_df)
        
    
    def get_talib_momentum_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate momentum indicators using TA-Lib.
        Args:
            df (pd.DataFrame): DataFrame containing stock data with columns like 'Open', 'High', 'Low', 'Close', etc.
        Returns:
            pd.DataFrame: DataFrame with momentum indicators added.
        """
        if df.empty:
            logger.warning("Input DataFrame is empty. Returning an empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'RSI', 'MACD', 'MACD_Signal', 'MACD_Hist', 
                                         'Stoch_K', 'Stoch_D', 'CCI', 'ADX', 'ROC', 
                                         'MOM', 'MACD_Oscillator', 'TRIX', 'WILLR', 'MMACDFix'])
            
        logger.info("Calculating TA-Lib indicators...")
        

        # Calculate indicators
        momentum_rsi = talib.RSI(df['Close'], timeperiod=14)
        momentum_macd, momentum_macdsignal, momentum_macdhist = talib.MACD(df['Close'], fastperiod=12, slowperiod=26, signalperiod=9)
        momentum_stoch_k, momentum_stoch_d = talib.STOCH(df['High'], df['Low'], df['Close'], fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)
        momentum_cci = talib.CCI(df['High'], df['Low'], df['Close'], timeperiod=14)
        momentum_adx = talib.ADX(df['High'], df['Low'], df['Close'], timeperiod=14)
        momentum_roc = talib.ROC(df['Close'], timeperiod=10)
        momentum_mom = talib.MOM(df['Close'], timeperiod=10)
        momentum_macd_oscillator = talib.MACDEXT(df['Close'], fastperiod=12, slowperiod=26, signalperiod=9, fastmatype=0, slowmatype=0, signalmatype=0)
        momentum_trix = talib.TRIX(df['Close'], timeperiod=30)
        momentum_willr = talib.WILLR(df['High'], df['Low'], df['Close'], timeperiod=14)
        momentum_mmacdfix = talib.MACDFIX(df['Close'], fastperiod=12, slowperiod=26, signalperiod=9)
                
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
            'MACD_Oscillator': momentum_macd_oscillator,
            'TRIX': momentum_trix,
            'WILLR': momentum_willr,
            'MMACDFix': momentum_mmacdfix
        })
        momentum_df = momentum_df.astype({'RSI': 'double', 'MACD': 'double', 'MACD_Signal': 'double',
                                           'MACD_Hist': 'double', 'Stoch_K': 'double', 
                                           'Stoch_D': 'double', 'CCI': 'double',
                                             'ADX': 'double', 'ROC': 'double',
                                             'MOM': 'double', 'MACD_Oscillator': 'double',
                                             'TRIX': 'double', 'WILLR': 'double',
                                                'MMACDFix': 'double'})
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
        if df.empty:
            logger.warning("Input DataFrame is empty. Returning an empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'OBV', 'AD', 'ADOSC'])
        
        logger.info("Calculating volume indicators...")

        # Calculate indicators
        volume_obv = talib.OBV(df['Close'], df['Volume'])
        volume_ad = talib.AD(df['High'], df['Low'], df['Close'], df['Volume'])
        volume_mfi = talib.MFI(df['High'], df['Low'], df['Close'], df['Volume'], timeperiod=14)
        volume_adosc = talib.ADOSC(df['High'], df['Low'], df['Close'], df['Volume'], fastperiod=3, slowperiod=10)
        volume_cmf = talib.CMF(df['High'], df['Low'], df['Close'], df['Volume'], timeperiod=20)
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
        return donchian_upper, donch_lower
    
    
    def get_talib_volatility_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate volatility indicators using TA-Lib.
        Args:
            df (pd.DataFrame): DataFrame containing stock data with columns like 'Open', 'High', 'Low', 'Close', etc.
        Returns:
            pd.DataFrame: DataFrame with volatility indicators added.
        """
        if df.empty:
            logger.warning("Input DataFrame is empty. Returning an empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'ATR', 'NATR', 'TRANGE'])
        
        logger.info("Calculating volatility indicators...")
            
        volatility_bbands_upper, volatility_bbands_middle, volatility_bbands_lower = talib.BBANDS(df['Close'], timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
        volatility_chv = talib.CHLV(df['High'], df['Low'], df['Close'])
        volatility_donch_upper, volatility_donch_lower = self.donchian_channel(df, period=20)
        volatility_ulcer_index = self.ulcer_index(df, period=14)
        volatility_natr = talib.NATR(df['High'], df['Low'], df['Close'], timeperiod=14)
        volatility_trange = talib.TRANGE(df['High'], df['Low'], df['Close'])
        volatility_bbands_upper, volatility_bbands_middle, volatility_bbands_lower = talib.BBANDS(df['Close'], timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
        volatility_chv = talib.CHLV(df['High'], df['Low'], df['Close'])
        volatility_donch_upper, volatility_donch_lower = self.donchian_channel(df, period=20).iloc[:, 1:].T
        volatility_ulcer_index = self.ulcer_index(df, period=14)

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
            'CHV': volatility_chv,
            'DONCH_UPPER': volatility_donch_upper,
            'DONCH_LOWER': volatility_donch_lower
        })
        volatility_df = volatility_df.astype({'ATR': 'double', 'NATR': 'double', 'TRANGE': 'double',
                                               'BBANDS_UPPER': 'double', 'BBANDS_MIDDLE': 'double',
                                               'BBANDS_LOWER': 'double', 'ULCER_INDEX': 'double',
                                               'CHV': 'double',
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
        pattern_cdladvancingwhite = talib.CDLADVANCINGWHITE(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlbelthold = talib.CDLBELTHOLD(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlbreakaway = talib.CDLBREAKAWAY(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlclosingmarubozu = talib.CDLCLOSINGMARUBOZU(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlconcealbabyswall = talib.CDLCONCEALBABYSWALL(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlcounterattack = talib.CDLCOUNTERATTACK(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdldarkcloudcover = talib.CDLDARKCLOUDCOVER(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdldoji = talib.CDLDOJI(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdldojistar = talib.CDLDOJISTAR(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlengulfing = talib.CDLENGULFING(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdleverystar = talib.CDLEVERYSTAR(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlgravestonedoji = talib.CDLGRAVESTONEDOJI(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlhammer = talib.CDLHAMMER(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlhangingman = talib.CDLHANGINGMAN(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlharami = talib.CDLHARAMI(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlharamicross = talib.CDLHARAMICROSS(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlhighwave = talib.CDLHIGHWAVE(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlhikkake = talib.CDLHIKKAKE(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlhikkake2 = talib.CDLHIKKAKE2(df['Open'], df['High'], df['Low'], df['Close'])
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
        pattern_cdlstickysession = talib.CDLSTICKYSESSION(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdltakuri = talib.CDLTAKURI(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdltasukigap = talib.CDLTASUKIGAP(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdltasukigap2 = talib.CDLTASUKIGAP2(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlthrusting = talib.CDLTHRUSTING(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdltristar = talib.CDLTRISTAR(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlunique3river = talib.CDLUNIQUE3RIVER(df['Open'], df['High'], df['Low'], df['Close'])
        pattern_cdlupdowngap = talib.CDLUPDOWNGAP(df['Open'], df['High'], df['Low'], df['Close'])
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
            'CDLADVANCINGWHITE': pattern_cdladvancingwhite,
            'CDLBELTHOLD': pattern_cdlbelthold,
            'CDLBREAKAWAY': pattern_cdlbreakaway,
            'CDLCLOSINGMARUBOZU': pattern_cdlclosingmarubozu,
            'CDLCONCEALBABYSWALL': pattern_cdlconcealbabyswall,
            'CDLCOUNTERATTACK': pattern_cdlcounterattack,
            'CDLDARKCLOUDCOVER': pattern_cdldarkcloudcover,
            'CDLDOJI': pattern_cdldoji,
            'CDLDOJISTAR': pattern_cdldojistar,
            'CDLENGULFING': pattern_cdlengulfing,
            'CDLEVERYSTAR': pattern_cdleverystar,
            'CDLGRAVESTONEDOJI': pattern_cdlgravestonedoji,
            'CDLHAMMER': pattern_cdlhammer,
            'CDLHANGINGMAN': pattern_cdlhangingman,
            'CDLHARAMI': pattern_cdlharami,
            'CDLHARAMICROSS': pattern_cdlharamicross,
            'CDLHIGHWAVE': pattern_cdlhighwave,
            'CDLHIKKAKE': pattern_cdlhikkake,
            'CDLHIKKAKE2': pattern_cdlhikkake2,
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
            'CDLSTICKYSESSION': pattern_cdlstickysession,
            'CDLTAKURI': pattern_cdltakuri,
            'CDLTASUKIGAP': pattern_cdltasukigap,
            'CDLTASUKIGAP2': pattern_cdltasukigap2,
            'CDLTHRUSTING': pattern_cdlthrusting,
            'CDLTRISTAR': pattern_cdltristar,
            'CDLUNIQUE3RIVER': pattern_cdlunique3river,
            'CDLUPDOWNGAP': pattern_cdlupdowngap,
            'CDLXSIDEGAP3METHODS': pattern_cdlxsidegap3methods
        })

        pattern_df = pattern_df.astype({
            'CDL2CROWS': 'double', 'CDL3BLACKCROWS': 'double', 'CDL3INSIDE': 'double',
            'CDL3LINESTRIKE': 'double', 'CDL3OUTSIDE': 'double', 'CDL3STARSINSOUTH': 'double',
            'CDL3WHITESOLDIERS': 'double', 'CDLABANDONEDBABY': 'double', 'CDLADVANCINGWHITE': 'double',
            'CDLBELTHOLD': 'double', 'CDLBREAKAWAY': 'double', 'CDLCLOSINGMARUBOZU': 'double',
            'CDLCONCEALBABYSWALL': 'double', 'CDLCOUNTERATTACK': 'double', 'CDLDARKCLOUDCOVER': 'double',
            'CDLDOJI': 'double', 'CDLDOJISTAR': 'double', 'CDLENGULFING': 'double', 'CDLEVERYSTAR': 'double',
            'CDLGRAVESTONEDOJI': 'double', 'CDLHAMMER': 'double', 'CDLHANGINGMAN': 'double', 'CDLHARAMI': 'double',
            'CDLHARAMICROSS': 'double', 'CDLHIGHWAVE': 'double', 'CDLHIKKAKE': 'double', 'CDLHIKKAKE2': 'double',
            'CDLHOMINGPIGEON': 'double', 'CDLIDENTICAL3CROWS': 'double', 'CDLINNECK': 'double',
            'CDLINVERTEDHAMMER': 'double', 'CDLLADDERBOTTOM': 'double', 'CDLLONGLEGGEDDOJI': 'double',
            'CDLLONGLINE': 'double', 'CDLMARUBOZU': 'double', 'CDLMATCHINGLOW': 'double', 'CDLMATHOLD': 'double',
            'CDLMORNINGDOJISTAR': 'double', 'CDLMORNINGSTAR': 'double', 'CDLONNECK': 'double', 'CDLPIERCING': 'double',
            'CDLRICKSHAWMAN': 'double', 'CDLRISEFALL3METHODS': 'double', 'CDLSEPARATINGLINES': 'double',
            'CDLSHOOTINGSTAR': 'double', 'CDLSHORTLINE': 'double', 'CDLSPINNINGTOP': 'double',
            'CDLSTALLEDPATTERN': 'double', 'CDLSTICKYSESSION': 'double', 'CDLTAKURI': 'double',
            'CDLTASUKIGAP': 'double', 'CDLTASUKIGAP2': 'double', 'CDLTHRUSTING': 'double', 'CDLTRISTAR': 'double',
            'CDLUNIQUE3RIVER': 'double', 'CDLUPDOWNGAP': 'double', 'CDLXSIDEGAP3METHODS': 'double'
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
        if df.empty:
            logger.warning("Input DataFrame is empty. Returning an empty DataFrame.")
            return pd.DataFrame(columns=['Date', 'Ticker', 'HT_TRENDLINE', 'HT_SINE', 'HT_DCPERIOD', 'HT_DCPHASE'])
        
        logger.info("Calculating cycle indicators...")

        ht_trendline = talib.HT_TRENDLINE(df['Close'])
        ht_sine, ht_sine_lead, ht_sine_phase = talib.HT_SINE(df['Close'])
        ht_dcperiod = talib.HT_DCPERIOD(df['Close'])
        ht_dcphase = talib.HT_DCPHASE(df['Close'])

        cycle_df = pd.DataFrame({
            'Date': df['Date'],
            'Ticker': df['Ticker'],
            'HT_TRENDLINE': ht_trendline,
            'HT_SINE': ht_sine,
            'HT_SINE_LEAD': ht_sine_lead,
            'HT_SINE_PHASE': ht_sine_phase,
            'HT_DCPERIOD': ht_dcperiod,
            'HT_DCPHASE': ht_dcphase
        })

        cycle_df = cycle_df.astype({'HT_TRENDLINE': 'double', 'HT_SINE': 'double',
                                     'HT_SINE_LEAD': 'double', 'HT_SINE_PHASE': 'double',
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
    
    def merge_macro_indicators(self):
        """
        Merge macroeconomic indicators into the ticker DataFrame.
        This method assumes that the macro_df DataFrame has been populated with macroeconomic indicators
        and that both DataFrames have a 'Date' column for merging.
        If the macro_df is empty, it will skip the merge and log a warning.
        Returns:
            None
        """
        logger.info("Merging macroeconomic indicators...")
        
        if self.macro_df.empty:
            logger.warning("Macro DataFrame is empty. Skipping merge.")
            return
        
        self.ticker_df = self.ticker_df.merge(self.macro_df, on='Date', how='left')
        self.ticker_df['Date'] = pd.to_datetime(self.ticker_df['Date'], utc=True)
        logger.info("Macro indicators merged successfully.")