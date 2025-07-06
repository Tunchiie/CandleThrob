"""
SQLAlchemy models for CandleThrob financial data pipeline.

This module contains the database models for storing ticker data and macroeconomic data
using Oracle database with SQLAlchemy ORM. Supports bulk inserts and incremental loading.
"""

import pandas as pd
import logging
from datetime import datetime
from datetime import date as date_type
from typing import Optional
from sqlalchemy import Column, Integer, String, DateTime, Float, Date, func, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

Base = declarative_base()
logger = logging.getLogger(__name__)

class TickerData(Base):
    """Model for storing ticker data (OHLCV) with bulk insert and incremental loading support."""
    
    __tablename__ = 'ticker_data'
    
    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String(10), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    open_price = Column(Float, nullable=False)
    high_price = Column(Float, nullable=False)
    low_price = Column(Float, nullable=False)
    close_price = Column(Float, nullable=False)
    volume = Column(BigInteger, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def create_table(self, session: Session):
        """Create the table if it doesn't exist."""
        try:
            Base.metadata.create_all(session.bind)
            logger.info("TickerData table created/verified successfully")
        except Exception as e:
            logger.error("Error creating TickerData table: %s", e)
            raise
    
    def data_exists(self, session: Session, ticker: Optional[str] = None) -> bool:
        """Check if data exists in the table."""
        try:
            query = session.query(TickerData)
            if ticker:
                query = query.filter(TickerData.ticker == ticker)
            return query.first() is not None
        except Exception as e:
            logger.error("Error checking data existence: %s", e)
            return False
    
    def get_last_date(self, session: Session, ticker: Optional[str] = None) -> Optional[date_type]:
        """Get the last date for which data exists."""
        try:
            query = session.query(func.max(TickerData.date))
            if ticker:
                query = query.filter(TickerData.ticker == ticker)
            result = query.scalar()
            return result
        except Exception as e:
            logger.error("Error getting last date: %s", e)
            return None
    
    def insert_data(self, session: Session, df: pd.DataFrame):
        """Insert data using bulk operations with incremental loading."""
        if df is None or df.empty:
            logger.warning("No data to insert")
            return
        
        try:
            # Prepare data for insertion
            df_clean = df.copy()
            df_clean['date'] = pd.to_datetime(df_clean['date']).dt.date
            
            # Rename columns to match database schema
            column_mapping = {
                'open': 'open_price',
                'high': 'high_price', 
                'low': 'low_price',
                'close': 'close_price'
            }
            df_clean = df_clean.rename(columns=column_mapping)
            
            # Select only required columns
            required_cols = ['ticker', 'date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']
            df_clean = df_clean[required_cols]
            
            # Remove duplicates and existing data
            ticker = df_clean['ticker'].iloc[0] if 'ticker' in df_clean.columns else None
            if ticker and self.data_exists(session, ticker):
                last_date = self.get_last_date(session, ticker)
                if last_date:
                    df_clean = df_clean[df_clean['date'] > last_date]
            
            if df_clean.empty:
                logger.info("No new data to insert after filtering duplicates")
                return
            
            # Add created_at timestamp
            df_clean['created_at'] = datetime.utcnow()
            
            # Bulk insert using pandas to_sql
            df_clean.to_sql(
                name='ticker_data',
                con=session.bind,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            
            session.commit()
            logger.info("Successfully inserted %d ticker records", len(df_clean))
            
        except Exception as e:
            session.rollback()
            logger.error("Error inserting ticker data: %s", e)
            raise


class MacroData(Base):
    """Model for storing macroeconomic data with bulk insert and incremental loading support."""
    
    __tablename__ = 'macro_data'
    
    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, nullable=False, index=True)
    series_id = Column(String(100), nullable=False, index=True)
    value = Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def create_table(self, session: Session):
        """Create the table if it doesn't exist."""
        try:
            Base.metadata.create_all(session.bind)
            logger.info("MacroData table created/verified successfully")
        except Exception as e:
            logger.error("Error creating MacroData table: %s", e)
            raise
    
    def data_exists(self, session: Session, series_id: Optional[str] = None) -> bool:
        """Check if data exists in the table."""
        try:
            query = session.query(MacroData)
            if series_id:
                query = query.filter(MacroData.series_id == series_id)
            return query.first() is not None
        except Exception as e:
            logger.error("Error checking macro data existence: %s", e)
            return False
    
    def get_last_date(self, session: Session, series_id: Optional[str] = None) -> Optional[date_type]:
        """Get the last date for which data exists."""
        try:
            query = session.query(func.max(MacroData.date))
            if series_id:
                query = query.filter(MacroData.series_id == series_id)
            result = query.scalar()
            return result
        except Exception as e:
            logger.error("Error getting last macro date: %s", e)
            return None
    
    def insert_data(self, session: Session, df: pd.DataFrame):
        """Insert macroeconomic data using bulk operations with incremental loading."""
        if df is None or df.empty:
            logger.warning("No macro data to insert")
            return
        
        try:
            # Prepare data for insertion
            df_clean = df.copy()
            df_clean['date'] = pd.to_datetime(df_clean['date']).dt.date
            
            # Remove existing data (incremental loading)
            if self.data_exists(session):
                last_date = self.get_last_date(session)
                if last_date:
                    df_clean = df_clean[df_clean['date'] > last_date]
            
            if df_clean.empty:
                logger.info("No new macro data to insert after filtering")
                return
            
            # Add created_at timestamp
            df_clean['created_at'] = datetime.utcnow()
            
            # Bulk insert using pandas to_sql
            df_clean.to_sql(
                name='macro_data',
                con=session.bind,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            
            session.commit()
            logger.info("Successfully inserted %d macro records", len(df_clean))
            
        except Exception as e:
            session.rollback()
            logger.error("Error inserting macro data: %s", e)
            raise


class TransformedTickers(Base):
    """Model for storing processed data with technical indicators."""
    
    __tablename__ = 'transformed_tickers'
    
    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String(10), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    
    # OHLCV Data
    open_price = Column(Float)
    high_price = Column(Float)
    low_price = Column(Float)
    close_price = Column(Float)
    volume = Column(BigInteger)
    
    # Momentum Indicators
    rsi = Column(Float)
    macd = Column(Float)
    macd_signal = Column(Float)
    macd_hist = Column(Float)
    stoch_k = Column(Float)
    stoch_d = Column(Float)
    cci = Column(Float)
    roc = Column(Float)
    mom = Column(Float)
    trix = Column(Float)
    willr = Column(Float)
    sma10 = Column(Float)
    sma20 = Column(Float)
    sma50 = Column(Float)
    sma100 = Column(Float)
    sma200 = Column(Float)
    ema10 = Column(Float)
    ema20 = Column(Float)
    ema50 = Column(Float)
    ema100 = Column(Float)
    ema200 = Column(Float)
    
    # Volume indicators
    obv = Column(Float)
    ad = Column(Float)
    mfi = Column(Float)
    adosc = Column(Float)
    cmf = Column(Float)
    vwap = Column(Float)
    vpt = Column(Float)
    adx = Column(Float)
    rvol = Column(Float)
    
    # Volatility indicators
    atr = Column(Float)
    natr = Column(Float)
    trange = Column(Float)
    bbands_upper = Column(Float)
    bbands_middle = Column(Float)
    bbands_lower = Column(Float)
    ulcer_index = Column(Float)
    donch_upper = Column(Float)
    donch_lower = Column(Float)
    
    # Price indicators
    midprice = Column(Float)
    medprice = Column(Float)
    typprice = Column(Float)
    wclprice = Column(Float)
    avgprice = Column(Float)
    
    # Return calculations
    return_1d = Column(Float)
    return_3d = Column(Float)
    return_7d = Column(Float)
    return_30d = Column(Float)
    return_90d = Column(Float)
    return_365d = Column(Float)
    
    # Cyclical indicators
    ht_trendline = Column(Float)
    ht_sine = Column(Float)
    ht_sine_lead = Column(Float)
    ht_dcperiod = Column(Float)
    ht_dcphase = Column(Float)
    
    # Statistical indicators
    stddev = Column(Float)
    var = Column(Float)
    beta_vs_sp500 = Column(Float)
    zscore_price_normalized = Column(Float)
    
    # Candlestick Pattern Recognition (complete set from enrich_data.py)
    # Pattern values: 100 = Bullish, 0 = No pattern, -100 = Bearish
    cdl2crows = Column(Float)  # Two Crows
    cdl3blackcrows = Column(Float)  # Three Black Crows
    cdl3inside = Column(Float)  # Three Inside Up/Down
    cdl3linestrike = Column(Float)  # Three-Line Strike
    cdl3outside = Column(Float)  # Three Outside Up/Down
    cdl3starsinsouth = Column(Float)  # Three Stars In The South
    cdl3whitesoldiers = Column(Float)  # Three Advancing White Soldiers
    cdlabandonedbaby = Column(Float)  # Abandoned Baby
    cdlbelthold = Column(Float)  # Belt-hold
    cdlbreakaway = Column(Float)  # Breakaway
    cdlclosingmarubozu = Column(Float)  # Closing Marubozu
    cdlconcealbabyswall = Column(Float)  # Concealing Baby Swallow
    cdlcounterattack = Column(Float)  # Counterattack
    cdldarkcloudcover = Column(Float)  # Dark Cloud Cover
    cdldoji = Column(Float)  # Doji
    cdldojistar = Column(Float)  # Doji Star
    cdlengulfing = Column(Float)  # Engulfing Pattern
    cdleveningstar = Column(Float)  # Evening Star
    cdlgravestonedoji = Column(Float)  # Gravestone Doji
    cdlhammer = Column(Float)  # Hammer
    cdlhangingman = Column(Float)  # Hanging Man
    cdlharami = Column(Float)  # Harami Pattern
    cdlharamicross = Column(Float)  # Harami Cross Pattern
    cdlhighwave = Column(Float)  # High-Wave Candle
    cdlhikkake = Column(Float)  # Hikkake Pattern
    cdlhikkakemod = Column(Float)  # Modified Hikkake Pattern
    cdlhomingpigeon = Column(Float)  # Homing Pigeon
    cdlidentical3crows = Column(Float)  # Identical Three Crows
    cdlinneck = Column(Float)  # In-Neck Pattern
    cdlinvertedhammer = Column(Float)  # Inverted Hammer
    cdlladderbottom = Column(Float)  # Ladder Bottom
    cdllongleggeddoji = Column(Float)  # Long Legged Doji
    cdllongline = Column(Float)  # Long Line Candle
    cdlmarubozu = Column(Float)  # Marubozu
    cdlmatchinglow = Column(Float)  # Matching Low
    cdlmathold = Column(Float)  # Mat Hold
    cdlmorningdojistar = Column(Float)  # Morning Doji Star
    cdlmorningstar = Column(Float)  # Morning Star
    cdlonneck = Column(Float)  # On-Neck Pattern
    cdlpiercing = Column(Float)  # Piercing Pattern
    cdlrickshawman = Column(Float)  # Rickshaw Man
    cdlrisefall3methods = Column(Float)  # Rising/Falling Three Methods
    cdlseparatinglines = Column(Float)  # Separating Lines
    cdlshootingstar = Column(Float)  # Shooting Star
    cdlshortline = Column(Float)  # Short Line Candle
    cdlspinningtop = Column(Float)  # Spinning Top
    cdlstalledpattern = Column(Float)  # Stalled Pattern
    cdlsticksandwich = Column(Float)  # Stick Sandwich
    cdltakuri = Column(Float)  # Takuri (Dragonfly Doji with very long lower shadow)
    cdltasukigap = Column(Float)  # Tasuki Gap
    cdlthrusting = Column(Float)  # Thrusting Pattern
    cdltristar = Column(Float)  # Tristar Pattern
    cdlunique3river = Column(Float)  # Unique 3 River
    cdlxsidegap3methods = Column(Float)  # Upside/Downside Gap Three Methods
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def create_table(self, session: Session):
        """Create the table if it doesn't exist."""
        try:
            Base.metadata.create_all(session.bind)
            logger.info("TransformedTickers table created/verified successfully")
        except Exception as e:
            logger.error("Error creating TransformedTickers table: %s", e)
            raise
    
    def data_exists(self, session: Session, ticker: Optional[str] = None) -> bool:
        """Check if data exists in the table."""
        try:
            query = session.query(TransformedTickers)
            if ticker:
                query = query.filter(TransformedTickers.ticker == ticker)
            return query.first() is not None
        except Exception as e:
            logger.error("Error checking transformed data existence: %s", e)
            return False
    
    def get_last_date(self, session: Session, ticker: Optional[str] = None) -> Optional[date_type]:
        """Get the last date for which data exists."""
        try:
            query = session.query(func.max(TransformedTickers.date))
            if ticker:
                query = query.filter(TransformedTickers.ticker == ticker)
            result = query.scalar()
            return result
        except Exception as e:
            logger.error("Error getting last transformed date: %s", e)
            return None
    
    def insert_data(self, session: Session, df: pd.DataFrame):
        """Insert transformed data using bulk operations with incremental loading."""
        if df is None or df.empty:
            logger.warning("No transformed data to insert")
            return
        
        try:
            # Prepare data for insertion
            df_clean = df.copy()
            df_clean['date'] = pd.to_datetime(df_clean['date']).dt.date
            
            # Remove existing data (incremental loading)
            ticker = df_clean['ticker'].iloc[0] if 'ticker' in df_clean.columns else None
            if ticker and self.data_exists(session, ticker):
                last_date = self.get_last_date(session, ticker)
                if last_date:
                    df_clean = df_clean[df_clean['date'] > last_date]
            
            if df_clean.empty:
                logger.info("No new transformed data to insert after filtering")
                return
            
            # Add created_at timestamp
            df_clean['created_at'] = datetime.utcnow()
            
            # Bulk insert using pandas to_sql
            df_clean.to_sql(
                name='transformed_tickers',
                con=session.bind,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            
            session.commit()
            logger.info("Successfully inserted %d transformed records", len(df_clean))
            
        except Exception as e:
            session.rollback()
            logger.error("Error inserting transformed data: %s", e)
            raise


class TransformedMacroData(Base):
    """Model for storing transformed macroeconomic data."""
    
    __tablename__ = 'transformed_macro_data'
    
    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, nullable=False, index=True)
    series_id = Column(String(100), nullable=False, index=True)
    value = Column(Float, nullable=True)
    normalized_value = Column(Float)
    moving_avg_30 = Column(Float)
    year_over_year_change = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def create_table(self, session: Session):
        """Create the table if it doesn't exist."""
        try:
            Base.metadata.create_all(session.bind)
            logger.info("TransformedMacroData table created/verified successfully")
        except Exception as e:
            logger.error("Error creating TransformedMacroData table: %s", e)
            raise
    
    def data_exists(self, session: Session, series_id: Optional[str] = None) -> bool:
        """Check if data exists in the table."""
        try:
            query = session.query(TransformedMacroData)
            if series_id:
                query = query.filter(TransformedMacroData.series_id == series_id)
            return query.first() is not None
        except Exception as e:
            logger.error("Error checking transformed macro data existence: %s", e)
            return False
    
    def get_last_date(self, session: Session, series_id: Optional[str] = None) -> Optional[date_type]:
        """Get the last date for which data exists."""
        try:
            query = session.query(func.max(TransformedMacroData.date))
            if series_id:
                query = query.filter(TransformedMacroData.series_id == series_id)
            result = query.scalar()
            return result
        except Exception as e:
            logger.error("Error getting last transformed macro date: %s", e)
            return None
    
    def insert_data(self, session: Session, df: pd.DataFrame):
        """Insert transformed macro data using bulk operations with incremental loading."""
        if df is None or df.empty:
            logger.warning("No transformed macro data to insert")
            return
        
        try:
            # Prepare data for insertion
            df_clean = df.copy()
            df_clean['date'] = pd.to_datetime(df_clean['date']).dt.date
            
            # Remove existing data (incremental loading)
            if self.data_exists(session):
                last_date = self.get_last_date(session)
                if last_date:
                    df_clean = df_clean[df_clean['date'] > last_date]
            
            if df_clean.empty:
                logger.info("No new transformed macro data to insert after filtering")
                return
            
            # Add created_at timestamp
            df_clean['created_at'] = datetime.utcnow()
            
            # Bulk insert using pandas to_sql
            df_clean.to_sql(
                name='transformed_macro_data',
                con=session.bind,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            
            session.commit()
            logger.info("Successfully inserted %d transformed macro records", len(df_clean))
            
        except Exception as e:
            session.rollback()
            logger.error("Error inserting transformed macro data: %s", e)
            raise
