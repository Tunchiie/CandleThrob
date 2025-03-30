import praw
import os
import pandas as pd
import sys

sys.path.append("../scripts")
from db_utils import insert_dataframe
from textblob import TextBlob
from datetime import datetime, timezone
from dotenv import load_dotenv

# loading variables from .env file
path = "c:/Users/tunchiie/Documents/Data Projects/CandleThrob/"

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


# Fetch posts from Reddit for each ticker
for ticker in tickers:
    # Search for posts related to the stock ticker on Reddit
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
insert_dataframe(reddit_df, "sentiment_scores")

reddit_df.head()
