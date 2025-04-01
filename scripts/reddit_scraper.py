import praw
import os
import pandas as pd
import sys

sys.path.append("../scripts")
from db_utils import insert_dataframe
from textblob import TextBlob
from datetime import datetime, timezone
from dotenv import load_dotenv

path = "../CandleThrob/"

load_dotenv(path + "data/var.env")

tickers = [
    "AAPL",  # Apple Inc.,
    "MSFT",  # Microsoft Corporation,
    "GOOGL",  # Alphabet Inc. Class A
    "AMZN",  # Amazon.com Inc.,
    "NVDA",  # NVidia Corporation,
]


reddit = praw.Reddit(
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET"),
    user_agent=os.getenv("USER_AGENT"),
    username=os.getenv("USER"),
    password=os.getenv("PASSWORD"),
)

posts = []


for ticker in tickers:
    for submission in reddit.subreddit("wallstreetbets").search(ticker, limit=300):
        text = submission.title + " " + submission.selftext
        blob = TextBlob(text)

        posts.append(
            {
                "stock": ticker,
                "date": datetime.fromtimestamp(
                    submission.created_utc, tz=timezone.utc
                ).strftime("%Y-%m-%d"),
                "source_type": "social",
                "content": text,
                "polarity": blob.sentiment.polarity,
                "subjectivity": blob.sentiment.subjectivity,
            }
        )


reddit_df = pd.DataFrame(posts)
reddit_df["date"] = pd.to_datetime(reddit_df["date"]).dt.date
insert_dataframe(reddit_df, "sentiment_scores")
