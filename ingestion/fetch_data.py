import yfinance as yf
import pandas_datareader as pdr
import pandas as pd
from fredapi import fred
import re
from datetime import date
from tqdm import tqdm
import os

class DataIngestion:

    def __init__(self):
        """ Initialize the DataIngestion class.
        """
        self.sp50_tickers = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]["Symbol"].tolist()
        self.sp50_tickers = [self.clean_ticker(ticker) for ticker in self.sp50_tickers]
        self.etf_tickers = etfs = ['SPY', 'QQQ', 'DIA', 'IWM', 'VTI', 'TLT', 'GLD',
                                    'XLF', 'XLE', 'XLK', 'XLV', 'ARKK', 'VXX',
                                    'TQQQ', 'SQQQ', 'SOXL', 'XLI', 'XLY', 'XLP', 'SLV']
        self.etf_tickers = [self.clean_ticker(ticker) for ticker in self.etf_tickers]
        self.all_tickers = self.sp50_tickers + self.etf_tickers
        self.end_date = date.today()
        self.start_date = date(year=self.end_date.year-50, month=self.end_date.month, day=self.end_date.day)
        self.ticker_df = None
    
    def fetch(self):
        """ Fetch the list of tickers from the S&P 500 and ETFs, clean them, and store them in a list.
        This method downloads historical stock data for each ticker using yfinance, enriches the data with additional information,
        and fetches economic data from the FRED API.
        Returns:
            None: The method updates the ticker DataFrame in place with historical stock data and economic indicators.
        """
        self.fetch_tickers()
        self.enrich_data()
        self.fetch_fred_data()
    
    def clean_ticker(self, ticker:str):
        """ Clean the ticker symbol by removing unwanted characters and formatting it.
        Args:
            ticker (str): The ticker symbol to clean.
        Returns:
            str: The cleaned ticker symbol."""
        return re.sub(r"[^A-Z0-9\-]", "-", ticker.strip().upper()).strip("-")
    
    def fetch_tickers(self):
        """ Fetch the list of tickers from the S&P 500 and ETFs, clean them, and store them in a list.
        Returns:
            list: A list of cleaned ticker symbols.
        """

        # Download stock data from yfinance
        for ticker in tqdm(self.all_tickers, desc="Downloading"):

            price_history = yf.download(tickers=ticker, start=self.start_date, interval="1d")

            if price_history.empty:
                continue

            price_history["Ticker"] = ticker
            price_history["Year"] = price_history.index.year
            price_history["Month"] = price_history.index.month
            price_history["Weekday"] = price_history.index.weekday
            price_history["Date"] = price_history.index.date

            if self.ticker_df:
                self.ticker_df = pd.concat([self.ticker_df, price_history])
            else:
                self.ticker_df = price_history
        self.ticker_df.reset_index(inplace=True)
        self.ticker_df.rename(columns={"index": "Date"}, inplace=True)
        self.ticker_df = self.ticker_df[["Date", "Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume", "Year", "Month", "Weekday"]]
        self.ticker_df["Date"] = pd.to_datetime(self.ticker_df["Date"])
        self.ticker_df.set_index("Date", inplace=True)
        self.ticker_df.sort_index(inplace=True)
    
    def enrich_data(self):
        """ Enrich the ticker data with additional information such as market cap, sector, and industry.
        This method fetches data from yfinance and updates the ticker DataFrame with market cap, sector, and industry information.
        It also calculates returns for various periods (1, 3, 7, 30, 90, and 365 days).
        Returns:
            None: The method updates the ticker DataFrame in place with additional columns for market cap,
        """
        # Fetch market cap, sector, and industry data from yfinance
        ticker_info = yf.Tickers(self.all_tickers).tickers
        for ticker in tqdm(ticker_info, desc="Enriching Data"):
            info = ticker_info[ticker].info
            if 'marketCap' in info:
                self.ticker_df.loc[self.ticker_df['Ticker'] == ticker, 'Market Cap'] = info['marketCap']
            if 'sector' in info:
                self.ticker_df.loc[self.ticker_df['Ticker'] == ticker, 'Sector'] = info['sector']
            if 'industry' in info:
                self.ticker_df.loc[self.ticker_df['Ticker'] == ticker, 'Industry'] = info['industry']
        self.ticker_df['Market Cap'] = self.ticker_df['Market Cap'].fillna(0)
        self.ticker_df['Sector'] = self.ticker_df['Sector'].fillna('Unknown')
        self.ticker_df['Industry'] = self.ticker_df['Industry'].fillna('Unknown')

        for i in [1,3,7,30,90,365]:
            self.ticker_df[f"Return_{i}d"] = self.ticker_df.groupby("Ticker")["Adj Close"].pct_change(periods=i).fillna(0)

    def fetch_fred_data(self):
        """ Fetch economic data from the Federal Reserve Economic Data (FRED) API.
        This method retrieves various economic indicators such as GDP, unemployment rate, inflation rate, and interest rates.
        It stores the data in a DataFrame and merges it with the ticker DataFrame based on the date.
        Returns:
            None: The method updates the ticker DataFrame in place with additional columns for economic indicators.
        """
        fred_api = fred(os.getenv("FRED_API_KEY"))

        fred_series = [
                            "FEDFUNDS",     # Federal Funds Rate
                            "CPIAUCSL",     # Consumer Price Index (CPI)
                            "UNRATE",       # Unemployment Rate
                            "GDP",          # Gross Domestic Product
                            "GS10",         # 10-Year Treasury Rate
                            "USREC",        # Recession Indicator
                            "UMCSENT",      # Consumer Sentiment
                            "HOUST",        # Housing Starts
                            "RSAFS",        # Retail Sales
                            "INDPRO",       # Industrial Production Index
                            "M2SL"          # M2 Money Supply
        ]
        fred_data = {series: fred_api.get_series(series, start_date=self.start_date, end_date=self.end_date) for series in fred_series}
        fred_df = pd.DataFrame(fred_data)
        for i in [1,3,7,30,90,365]:
            fred_df[f"Return_{i}d"] = fred_df.groupby("Ticker")["Adj Close"].pct_change(periods=i).fillna(0)
        fred_df.reset_index(inplace=True)
        fred_df.rename(columns={"index": "Date"}, inplace=True)
        fred_df["Date"] = pd.to_datetime(fred_df["Date"])
        fred_df.set_index("Date", inplace=True)
        fred_df.sort_index(inplace=True)
        self.ticker_df = self.ticker_df.merge(fred_df, on="Date", how="left")
        self.ticker_df.fillna(method='ffill', inplace=True)
        self.ticker_df.fillna(method='bfill', inplace=True)
        self.ticker_df = self.ticker_df.sort_index()
    
    def save_data(self):
        """ Save the ticker DataFrame to a CSV file.
        Returns:
            None: The method saves the ticker DataFrame to a CSV file at the specified path.
        """
        self.ticker_df.to_csv("storage/ticker_data.csv")
        print(f"Data saved to storage/ticker_data.csv")
